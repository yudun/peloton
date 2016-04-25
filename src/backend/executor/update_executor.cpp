//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_executor.cpp
//
// Identification: src/backend/executor/update_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/update_executor.h"
#include "backend/planner/update_plan.h"
#include "backend/common/logger.h"
#include "backend/catalog/manager.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/executor_context.h"
#include "backend/expression/container_tuple.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/tile.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for update executor.
 * @param node Update node corresponding to this executor.
 */
UpdateExecutor::UpdateExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DInit() {
  assert(children_.size() == 1);
  assert(target_table_ == nullptr);
  assert(project_info_ == nullptr);

  // Grab settings from node
  const planner::UpdatePlan &node = GetPlanNode<planner::UpdatePlan>();
  target_table_ = node.GetTable();
  project_info_ = node.GetProjectInfo();

  assert(target_table_);
  assert(project_info_);

  return true;
}

/**
 * @brief updates a set of columns
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DExecute() {
  assert(children_.size() == 1);
  assert(executor_context_);

  // We are scanning over a logical tile.
  LOG_TRACE("Update executor :: 1 child ");

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

  // Update tuples in given table
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    LOG_INFO("update tuple in table %s", target_table_->GetName().c_str());

    ItemPointer old_location(tile_group_id, physical_tuple_id);

    LOG_TRACE("Visible Tuple id : %u, Physical Tuple id : %u ",
              visible_tuple_id, physical_tuple_id);

    if (transaction_manager.IsOwner(tile_group_header, physical_tuple_id) ==
        true) {
      // if the thread is the owner of the tuple, then directly update in place.
      std::unique_ptr<storage::Tuple> new_tuple(new storage::Tuple(target_table_->GetSchema(), true));
      // Make a copy of the original tuple and allocate a new tuple
      expression::ContainerTuple<storage::TileGroup> old_tuple(
          tile_group, physical_tuple_id);
      // Execute the projections
      project_info_->Evaluate(new_tuple.get(), &old_tuple, nullptr,
                              executor_context_);

      LOG_INFO("inplace update %s", new_tuple->GetInfo().c_str());

      {
        // Because this is a inplace update, we check all the "non-referenced" constraints
        // here rather than in "InsertVersion" function.
        // These constraints include not null, primary, unique, check, and
        // if updated tuple satisfies the foreign key constraint for this table
        auto res = CheckUpdateNonReferencedConstraints(tile, physical_tuple_id, new_tuple.get());
        if (!res) {
          transaction_manager.SetTransactionResult(RESULT_FAILURE);
          return res;
        }

        // Check all the foreign key constraints referencing this tuple
        // and perform possible cascading action
        res = CheckUpdateForeignKeyConstraints(tile, physical_tuple_id, new_tuple.get());
        if (!res) {
          transaction_manager.SetTransactionResult(RESULT_FAILURE);
          return res;
        }
      }

      // Perform inplace update physically
      tile_group->CopyTuple(new_tuple.get(), physical_tuple_id);

      transaction_manager.PerformUpdate(old_location);
    } else if (transaction_manager.IsOwnable(tile_group_header,
                                             physical_tuple_id) == true) {
      // if the tuple is not owned by any transaction and is visible to current
      // transaction.

      if (transaction_manager.AcquireOwnership(tile_group_header, tile_group_id,
                                               physical_tuple_id) == false) {
        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
        transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
        return false;
      }
      // if it is the latest version and not locked by other threads, then
      // insert a new version.
      std::unique_ptr<storage::Tuple> new_tuple(new storage::Tuple(target_table_->GetSchema(), true));

      // Make a copy of the original tuple and allocate a new tuple
      expression::ContainerTuple<storage::TileGroup> old_tuple(
          tile_group, physical_tuple_id);
      // Execute the projections
      project_info_->Evaluate(new_tuple.get(), &old_tuple, nullptr,
                              executor_context_);

      LOG_INFO("insert version update %s", new_tuple->GetInfo().c_str());

      // finally insert updated tuple into the table
      ItemPointer new_location = target_table_->InsertVersion(new_tuple.get());

      // FIXME: PerformUpdate() will not be executed if the insertion failed,
      // There is a write lock, acquired, but since it is not in the write set,
      // the acquired lock can't be released when the txn is aborted.
      if (new_location.IsNull() == true) {
        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
        transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
        return false;
      }

      {
        // Check all the foreign key constraints referencing this tuple
        // and perform possible cascading action
        auto res = CheckUpdateForeignKeyConstraints(tile, physical_tuple_id, new_tuple.get());
        if (!res) {
          transaction_manager.SetTransactionResult(RESULT_FAILURE);
          return res;
        }
      }

      LOG_INFO("perform update old location: %u, %u", old_location.block, old_location.offset);
      LOG_INFO("perform update new location: %u, %u", new_location.block, new_location.offset);
      transaction_manager.PerformUpdate(old_location, new_location);

      executor_context_->num_processed += 1;  // updated one
    } else {
      // transaction should be aborted as we cannot update the latest version.
      LOG_TRACE("Fail to update tuple. Set txn failure.");
      transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
      return false;
    }
  }
  return true;
}

/**
 * @brief Check all the non-referenced key constraint for a updated tuple.
 * @param old_physical_tuple_id the physical id for the old tuple
 *        new_tuple the updated new tuple
 * @return true if all the non foreign key constraints are satistfied for this new tuple
 */
bool UpdateExecutor::CheckUpdateNonReferencedConstraints(__attribute__((unused)) storage::Tile *tile,
                                                         __attribute__((unused)) oid_t old_physical_tuple_id,
                                                         __attribute__((unused)) storage::Tuple* new_tuple) {
  // TODO
  return true;
}

/**
 * @brief Check the foreign key constraints for update.
 *  It will perform proper action according to the UpdateAction type of each foreign key constraints
 * @param old_physical_tuple_id the physical id for the old tuple
 *        new_tuple the updated new tuple
 * @return true if all the foreign key constraints' action are succeefully perform for this update
 */
bool UpdateExecutor::CheckUpdateForeignKeyConstraints(storage::Tile *tile,
                                                      oid_t old_physical_tuple_id,
                                                      storage::Tuple* new_tuple) {
// get the number of foreign key constraints that reference this table
  oid_t referedFKNum = target_table_->GetReferedForeignKeyCount();

  if (referedFKNum > 0) {
    // Get the physical base tuple list
    // so that we can check all columns to ensure different foreign key constraints
    std::unique_ptr<storage::Tuple> old_tuple(new storage::Tuple(tile->GetSchema(),
                                              tile->GetTupleLocation(old_physical_tuple_id)));

    for (oid_t i = 0; i < referedFKNum; ++i) {
      // get the current foreign key constraints to be checked
      auto foreign_key = target_table_->GetRefferedForeignKey(i);
      // if any foreign key constraint is violated, return false
      if (!foreign_key->CheckUpdateConstraints(executor_context_, old_tuple.get(), new_tuple))
        return false;
    }
  }

  return true;
}

}  // namespace executor
}  // namespace peloton
