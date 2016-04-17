//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_executor.cpp
//
// Identification: src/backend/executor/delete_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/delete_executor.h"
#include "backend/executor/executor_context.h"

#include "backend/common/value.h"
#include "backend/planner/delete_plan.h"
#include "backend/catalog/manager.h"
#include "backend/expression/container_tuple.h"
#include "backend/common/logger.h"
#include "backend/executor/logical_tile.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/tuple.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/index/index.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for delete executor.
 * @param node Delete node corresponding to this executor.
 */
DeleteExecutor::DeleteExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool DeleteExecutor::DInit() {
  assert(children_.size() == 1);
  assert(executor_context_);

  assert(target_table_ == nullptr);

  // Delete tuples in logical tile
  LOG_TRACE("Delete executor :: 1 child ");

  // Grab data from plan node.
  const planner::DeletePlan &node = GetPlanNode<planner::DeletePlan>();
  target_table_ = node.GetTable();
  assert(target_table_);

  return true;
}

/**
 * @brief Delete the table tuples using the position list in the logical tile.
 *
 * If truncate is on, then it will truncate the table itself.
 * @return true on success, false otherwise.
 */
bool DeleteExecutor::DExecute() {
  assert(target_table_);

  // Retrieve next tile.
  if (!children_[0]->Execute()) {
    return false;
  }

  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());

  auto &pos_lists = source_tile.get()->GetPositionLists();
  storage::Tile *tile = source_tile->GetBaseTile(0);
  storage::TileGroup *tile_group = tile->GetTileGroup();
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();

  auto tile_group_id = tile_group->GetTileGroupId();
  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  LOG_TRACE("Source tile : %p Tuples : %lu ", source_tile.get(),
            source_tile->GetTupleCount());

  LOG_TRACE("Transaction ID: %lu",
            executor_context_->GetTransaction()->GetTransactionId());

  // Check the foreign key constraints for possible cascading action
  oid_t referedFKNum = target_table_->GetReferedForeignKeyCount();

  for (oid_t i = 0; i < referedFKNum; ++i) {
    auto foreign_key = target_table_->GetRefferedForeignKey(i);

    switch (foreign_key->GetDeleteAction()) {
      case FOREIGNKEY_ACTION_NOACTION:
      case FOREIGNKEY_ACTION_RESTRICT: {
        bool isReferenced = false;
        oid_t database_oid = bridge::Bridge::GetCurrentDatabaseOid();
        // get source table
        auto &manager = catalog::Manager::GetInstance();
        auto source_table = manager.GetTableWithOid(database_oid, foreign_key->GetSrcTableOid());
        assert(source_table);
        // get the index associated with the referencing keys
        index::Index* fk_index = source_table->GetIndexWithOid(foreign_key->GetSrcIndexOid());

        for (oid_t visible_tuple_id : *source_tile) {
          expression::ContainerTuple<LogicalTile> cur_tuple(source_tile.get(),
                                                            visible_tuple_id);

          // Build referening key from this tuple to be used
          // to search the index
          std::unique_ptr<catalog::Schema>
              foreign_key_schema(catalog::Schema::CopySchema(source_table->GetSchema(),
                                                             foreign_key->GetFKColumnOffsets()));
          std::unique_ptr<storage::Tuple> key(new storage::Tuple(foreign_key_schema.get(), true));

          for (oid_t offset : foreign_key->GetFKColumnOffsets()) {
            key->SetValue(offset, cur_tuple.GetValue(offset), fk_index->GetPool());
          }

          LOG_INFO("Check restrict foreign key: %s", key->GetInfo().c_str());
          // search this key in the source table's index
          auto locations = fk_index->ScanKey(key.get());

          auto &transaction_manager =
              concurrency::TransactionManagerFactory::GetInstance();
          // if visible key doesn't exist in the refered column
          bool visible_key_exist = false;
          if (locations.size() > 0) {
            for(unsigned long i = 0; i < locations.size(); i++) {
              auto tile_group_header = catalog::Manager::GetInstance()
                  .GetTileGroup(locations[i].block)->GetHeader();
              auto tuple_id = locations[i].offset;
              if (transaction_manager.IsVisible(tile_group_header, tuple_id)) {
                visible_key_exist = true;
                break;
              }
            }
          }

          if (visible_key_exist) {
            isReferenced = true;
            break;
          }
        }
        if (isReferenced) {
          transaction_manager.SetTransactionResult(RESULT_FAILURE);
          LOG_WARN("ForeignKey constraint violated: RESTRICT - "
                       "Deleted tuple appears in referencing table %s",
                   source_table->GetName().c_str());
          return false;
        }
      }
        break;

      case FOREIGNKEY_ACTION_CASCADE:
      case FOREIGNKEY_ACTION_SETNULL:
      case FOREIGNKEY_ACTION_SETDEFAULT:
        break;
      default:
        LOG_ERROR("Invalid logging_type :: %d", foreign_key->GetDeleteAction());
        exit(EXIT_FAILURE);
    }

  }

  // Delete each tuple
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    LOG_TRACE("Visible Tuple id : %lu, Physical Tuple id : %lu ",
              visible_tuple_id, physical_tuple_id);

    if (transaction_manager.IsOwner(tile_group_header, physical_tuple_id) ==
        true) {
      // if the thread is the owner of the tuple, then directly update in place.

      transaction_manager.PerformDelete(tile_group_id, physical_tuple_id);

    } else if (transaction_manager.IsOwnable(tile_group_header,
                                             physical_tuple_id) == true) {
      // if the tuple is not owned by any transaction and is visible to current
      // transaction.

      if (transaction_manager.AcquireOwnership(tile_group_header, tile_group_id,
                                               physical_tuple_id) == false) {
        transaction_manager.SetTransactionResult(RESULT_FAILURE);
        return false;
      }
      // if it is the latest version and not locked by other threads, then
      // insert a new version.
      storage::Tuple *new_tuple =
          new storage::Tuple(target_table_->GetSchema(), true);

      // Make a copy of the original tuple and allocate a new tuple
      expression::ContainerTuple<storage::TileGroup> old_tuple(
          tile_group, physical_tuple_id);

      // finally insert updated tuple into the table
      ItemPointer location = target_table_->InsertEmptyVersion(new_tuple);

      if (location.block == INVALID_OID) {
        delete new_tuple;
        new_tuple = nullptr;
        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
        transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
        return false;
      }

      auto res = transaction_manager.PerformDelete(tile_group_id,
                                                   physical_tuple_id, location);
      if (!res) {
        transaction_manager.SetTransactionResult(RESULT_FAILURE);
        return res;
      }

      executor_context_->num_processed += 1;  // deleted one

      delete new_tuple;
      new_tuple = nullptr;
    } else {
      // transaction should be aborted as we cannot update the latest version.
      LOG_TRACE("Fail to update tuple. Set txn failure.");
      transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
      return false;
    }
  }

  return true;
}

}  // namespace executor
}  // namespace peloton
