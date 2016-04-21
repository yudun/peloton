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

#include "backend/planner/seq_scan_plan.h"
#include "backend/expression/expression_util.h"
#include "backend/executor/delete_executor.h"
#include "backend/executor/executor_context.h"

#include "backend/common/value.h"
#include "backend/planner/delete_plan.h"
#include "backend/catalog/manager.h"
#include "backend/common/logger.h"
#include "backend/executor/logical_tile.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/tuple.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/index/index.h"
#include "executors.h"

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

  // Check all the foreign key constraints referencing this table for possible cascading action
  auto res = CheckDeleteForeiKeyConstraints(source_tile.get());
  if (!res) {
    transaction_manager.SetTransactionResult(RESULT_FAILURE);
    return res;
  }

  // Delete each tuple
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    LOG_INFO("delete tuple in table %s", target_table_->GetName().c_str());
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
    LOG_INFO("delete single tuple sccessful");
  }

  LOG_INFO("delete sccessful");
  return true;
}

/**
 * @brief Check the foreign key constraints for deletion.
 *  It will perform proper action according to the DeleteAction type of each foreign key constraints
 * @param source_tile the logical tile which contain all the tuple to be deleted
 * @return true if all the foreign key constraints' action are succeefully perform for this deletion
 */
bool DeleteExecutor::CheckDeleteForeiKeyConstraints(LogicalTile * source_tile) {
  // get the number of foreign key constraints that reference this table
  oid_t referedFKNum = target_table_->GetReferedForeignKeyCount();

  if (referedFKNum > 0) {
    // Get the physical base tuple list
    // so that we can check all columns to ensure different foreign key constraints
    std::vector<storage::Tuple> base_tuple_list = GetBaseTupleListFromSourceTile(source_tile);

    for (oid_t i = 0; i < referedFKNum; ++i) {
      // get the current foreign key constraints to be checked
      auto foreign_key = target_table_->GetRefferedForeignKey(i);

      // get the source table
      oid_t database_oid = bridge::Bridge::GetCurrentDatabaseOid();
      auto &manager = catalog::Manager::GetInstance();
      auto source_table = manager.GetTableWithOid(database_oid, foreign_key->GetSrcTableOid());
      assert(source_table);

      // decide what kind of delete action to take
      switch (foreign_key->GetDeleteAction()) {
        case FOREIGNKEY_ACTION_NOACTION:
        case FOREIGNKEY_ACTION_RESTRICT: {
          // whether this tuple is referenced in the referencing column
          bool isReferenced = false;
          // get the index associated with the referencing keys
          index::Index *fk_index = source_table->GetIndexWithOid(foreign_key->GetSrcIndexOid());

          for (storage::Tuple cur_tuple : base_tuple_list) {
            // check each base tuple to see whether it violate the restrict foreign key constraint
            bool res = IsDeletedTupleReferencedBySourceTable(source_table, fk_index, cur_tuple,
                                                             foreign_key->GetFKColumnOffsets());

            // if visible key exist in the referencing column
            if (res) {
              isReferenced = true;
              break;
            }
          }

          if (isReferenced) {
            LOG_WARN("ForeignKey constraint violated: RESTRICT - Deleted tuple appears in referencing table %s",
                     source_table->GetName().c_str());
            return false;
          }
        }
          break;

        case FOREIGNKEY_ACTION_CASCADE: {
          for (storage::Tuple cur_tuple : base_tuple_list) {
            // cascading delete associated tuples in the source table
            bool res = DeleteReferencingTupleOnCascading(source_table, cur_tuple,
                                                         foreign_key->GetFKColumnOffsets());

            if (!res)
              return false;
          }
        }
          break;

        case FOREIGNKEY_ACTION_SETNULL: {

          // populate the direct_map_column_offsets that doesn't contain foreign keys
          std::vector<oid_t> direct_map_column_offsets;
          auto fk_column_offsets = foreign_key->GetFKColumnOffsets();
          oid_t source_col_num = source_table->GetSchema()->GetColumnCount();
          for (oid_t i = 0; i < source_col_num; i++) {
            // if this offset is not a foreign key, we add it to the direct_map list
            if (std::find(fk_column_offsets.begin(), fk_column_offsets.end(), i)
                == fk_column_offsets.end()) {
              direct_map_column_offsets.push_back(i);
            }
          }

          for (storage::Tuple cur_tuple : base_tuple_list) {
            // cascading delete associated tuples in the source table
            bool res = SetNullReferencingTupleOnCascading(source_table, cur_tuple,
                                                          foreign_key->GetFKColumnOffsets(),
                                                          direct_map_column_offsets);

            if (!res)
              return false;
          }
        }
          break;
        case FOREIGNKEY_ACTION_SETDEFAULT:
          break;
        default:
        LOG_ERROR("Invalid logging_type :: %d", foreign_key->GetDeleteAction());
          exit(EXIT_FAILURE);
      }

    }
  }

  return true;
}

/**
 * @brief Get all the physical base tuples given a Logical tile
 * @param source_tile the soruce logical tile
 * @return the physical base tuples associdated with the given source tile as a vector
 */
std::vector<storage::Tuple> DeleteExecutor::GetBaseTupleListFromSourceTile(LogicalTile* source_tile) {
  auto &pos_lists = source_tile->GetPositionLists();
  storage::Tile *tile = source_tile->GetBaseTile(0);
  std::vector<storage::Tuple> base_tuple_list;

  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];
    base_tuple_list.emplace_back(tile->GetSchema(), tile->GetTupleLocation(physical_tuple_id));
  }

  return base_tuple_list;
}

/**
 * @brief Create a CmpEq predicate to be used in seq_scan_executor given
 *  a tuple "cur_tuple" and a column_offset list.
 *  The predicate will check whether a given tuple's values in the column_offset list
 *  equals to corresponding value in "cur_tuple"
 *
 *  FIXME: we need to support creating a "AND" predicate for multiple columns
 * @param cur_tuple
 *        column_offsets These 2 are used togetehr to provide constant value in the predicate
 * @return the predicate
 */
expression::ComparisonExpression<expression::CmpEq> *DeleteExecutor::MakePredicate(
    storage::Tuple& cur_tuple,
    std::vector<oid_t>& column_offsets) {
  auto tup_val_exp = new expression::TupleValueExpression(0, column_offsets[0]);

  auto const_val_exp = new expression::ConstantValueExpression(
      cur_tuple.GetValue(column_offsets[0]));

  auto predicate = new expression::ComparisonExpression<expression::CmpEq>(
      EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_exp, const_val_exp);

  return predicate;
}

/**
 * @brief Check whether a tuple to be deleted has corresponding referencing tuple in the source table
 * @param source_table the source table
 *        fk_index the foreign key's index in the source table,
 *        cur_tuple the tuple to be delete
 *        column_offsets foreign key's column offsets
 * @return true if the given tuple is referenced by a visible tuple in the source table
 */
bool DeleteExecutor::IsDeletedTupleReferencedBySourceTable(storage::DataTable* source_table,
                                                           index::Index* fk_index,
                                                           storage::Tuple& cur_tuple,
                                                           std::vector<oid_t >&& column_offsets) {
  // Build referening key from this tuple to be used
  // to search the index
  std::unique_ptr<catalog::Schema>
      foreign_key_schema(catalog::Schema::CopySchema(source_table->GetSchema(),
                                                     column_offsets));
  std::unique_ptr<storage::Tuple> key(new storage::Tuple(foreign_key_schema.get(), true));

  for (oid_t offset : column_offsets) {
    key->SetValue(offset, cur_tuple.GetValue(offset), fk_index->GetPool());
  }

  LOG_INFO("Check restrict foreign key: %s", key->GetInfo().c_str());
  // search this key in the source table's index
  auto locations = fk_index->ScanKey(key.get());

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  return transaction_manager.VisibleTupleExist(locations);
}

/**
 * @brief Delete all the tuples in the referencing table in a cascade manner
 * @param table the source table
 *        cur_tuple the tuple to be delete
 *        column_offsets foreign key's column offsets
 * @return true if cascade delete success
 */
bool DeleteExecutor::DeleteReferencingTupleOnCascading(storage::DataTable* table,
                                                       storage::Tuple& cur_tuple,
                                                       std::vector<oid_t >&& column_offsets) {

  LOG_INFO("Cascading delete foreign key offset %lu in table %s",
           column_offsets[0], table->GetName().c_str());

  // Delete
  planner::DeletePlan delete_node(table, false);
  executor::DeleteExecutor delete_executor(&delete_node, executor_context_);

  auto predicate = MakePredicate(cur_tuple, column_offsets);

  // Scan
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(table, predicate, column_offsets));
  executor::SeqScanExecutor seq_scan_executor(seq_scan_node.get(),
                                              executor_context_);

  delete_node.AddChild(std::move(seq_scan_node));
  delete_executor.AddChild(&seq_scan_executor);

  delete_executor.Init();

  return delete_executor.Execute();
}

/**
 * @brief Set all the tuples' corresponding foreign key as null in the referencing table in a cascade manner
 * @param table the source table
 *        cur_tuple the tuple to be delete
 *        column_offsets foreign key's column offsets
 *        direct_map_column_offsets those column offsets who are not foreign key
 * @return true if cascade set null success
 */
bool DeleteExecutor::SetNullReferencingTupleOnCascading(storage::DataTable* table,
                                                        storage::Tuple& cur_tuple,
                                        std::vector<oid_t>&& column_offsets,
                                        std::vector<oid_t>& direct_map_column_offsets) {
  LOG_INFO("Cascading set null foreign key offset %lu in table %s",
           column_offsets[0], table->GetName().c_str());

  Value null_val = ValueFactory::GetNullValue();

  // ProjectInfo
  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  // set those columns who need to be updated as null
  for (oid_t offset : column_offsets) {
    target_list.emplace_back(
        offset, expression::ExpressionUtil::ConstantValueFactory(null_val));
  }
  // set the direct map columns, those columns doesn't need to be update
  for (oid_t offset : direct_map_column_offsets) {
    direct_map_list.emplace_back(offset, std::pair<oid_t, oid_t>(0, offset));
  }

  // Update plan
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan update_node(table, std::move(project_info));

  executor::UpdateExecutor update_executor(&update_node, executor_context_);

  // Predicate
  auto predicate = MakePredicate(cur_tuple, column_offsets);

  // Seq scan
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(table, predicate, column_offsets));
  executor::SeqScanExecutor seq_scan_executor(seq_scan_node.get(),
                                              executor_context_);

  update_node.AddChild(std::move(seq_scan_node));
  update_executor.AddChild(&seq_scan_executor);

  update_executor.Init();
  return update_executor.Execute();

}


}  // namespace executor
}  // namespace peloton
