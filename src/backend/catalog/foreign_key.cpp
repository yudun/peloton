//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// foreign_key.cpp
//
// Identification: src/backend/catalog/foreign_key.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <backend/planner/delete_plan.h>
#include <backend/executor/executors.h>
#include <backend/expression/expression_util.h>
#include "backend/storage/tuple.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/bridge/ddl/bridge.h"
#include "backend/common/types.h"
#include "manager.h"
#include "backend/common/logger.h"
#include "backend/executor/executor_context.h"
#include "backend/expression/comparison_expression.h"
#include "backend/index/index.h"
#include "backend/storage/data_table.h"
#include "backend/catalog/foreign_key.h"


namespace peloton {
namespace catalog {

/**
 * @brief Check this foreign key constraint for deletion.
 *  It will perform proper action according to the DeleteAction type of this foreign key constraint
 * @param executor_context the context of this deletion
 *        tuples all the tuples to be deleted
 * @return true if all the foreign key constraints' action are succeefully perform for this deletion
 */
bool ForeignKey::CheckDeleteConstraints(executor::ExecutorContext *executor_context,
                                        std::vector<storage::Tuple>& tuples){

  // get the source table
  oid_t database_oid = bridge::Bridge::GetCurrentDatabaseOid();
  auto &manager = catalog::Manager::GetInstance();
  auto source_table = manager.GetTableWithOid(database_oid, src_table_id);
  assert(source_table);

  // decide what kind of delete action to take
  switch (fk_delete_action) {
    case FOREIGNKEY_ACTION_NOACTION:
    case FOREIGNKEY_ACTION_RESTRICT: {

      // whether this tuple is referenced in the referencing column
      bool isReferenced = false;
      // get the index associated with the referencing keys
      index::Index *fk_index = source_table->GetIndexWithOid(fk_index_id);

      for (storage::Tuple cur_tuple : tuples) {
        // check each base tuple to see whether it violate the restrict foreign key constraint
        bool res = IsDeletedTupleReferencedBySourceTable(source_table, fk_index, cur_tuple);

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
      for (storage::Tuple cur_tuple : tuples) {
        // cascading delete associated tuples in the source table
        bool res = DeleteReferencingTupleOnCascading(executor_context,
                                                     source_table, cur_tuple);

        if (!res)
          return false;
      }
    }
      break;

    case FOREIGNKEY_ACTION_SETNULL: {

      // populate the direct_map_column_offsets that doesn't contain foreign keys
      std::vector<oid_t> direct_map_column_offsets;

      oid_t source_col_num = source_table->GetSchema()->GetColumnCount();
      for (oid_t i = 0; i < source_col_num; i++) {
        // if this offset is not a foreign key, we add it to the direct_map list
        if (std::find(fk_column_offsets.begin(), fk_column_offsets.end(), i)
            == fk_column_offsets.end()) {
          direct_map_column_offsets.push_back(i);
        }
      }

      for (storage::Tuple cur_tuple : tuples) {
        // cascading delete associated tuples in the source table
        bool res = SetNullReferencingTupleOnCascading(executor_context,
                                                      source_table, cur_tuple,
                                                      direct_map_column_offsets);

        if (!res)
          return false;
      }
    }
      break;
    case FOREIGNKEY_ACTION_SETDEFAULT:
      break;
    default:
    LOG_ERROR("Invalid logging_type :: %d", fk_delete_action);
      exit(EXIT_FAILURE);
  }

  return true;
}

/**
 * @brief Check whether the tuple's foreign key exists in the referred table
 * @param sink_table the referred table
 *        tuple the inserted tupe to be checked for the foreign key constraint
 * @returns True if the tuple exists in the referred table
 */
bool ForeignKey::IsTupleInSinkTable(storage::DataTable* sink_table, const storage::Tuple* tuple) {

  int ref_table_index_count = sink_table->GetIndexCount();

  for (int index_itr = ref_table_index_count - 1; index_itr >= 0; --index_itr) {
    auto index = sink_table->GetIndex(index_itr);

    // Get the index in the refered table corresponding with
    // this foreign key constraint
    if (index->GetOid() == pk_index_id) {
      LOG_INFO("BEGIN CHECKING REFERENCED TABLE");
      LOG_INFO("CHECK COLUMN OFFSET = %u", pk_column_offsets[0]);

      std::unique_ptr<catalog::Schema> referenced_key_schema(
          catalog::Schema::CopySchema(sink_table->GetSchema(), pk_column_offsets));
      std::unique_ptr<storage::Tuple> key(new storage::Tuple(referenced_key_schema.get(), true));

      key->SetFromTuple(tuple, fk_column_offsets, index->GetPool());

      // if every column in key is null, we skip the foreign key insert check for it
      // because it may be a result of insert version
      if (key->IsEveryColumnNull()) {
        LOG_INFO("EVERY COLUMN IN KEY IS NULL!");
        break;
      }

      auto locations = index->ScanKey(key.get());

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

      if (!visible_key_exist)
        return false;

      break;
    }
  }

  return true;
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
expression::ComparisonExpression<expression::CmpEq> *ForeignKey::MakePredicate(
    storage::Tuple& cur_tuple) {
  auto tup_val_exp = new expression::TupleValueExpression(0, fk_column_offsets[0]);

  auto const_val_exp = new expression::ConstantValueExpression(
      cur_tuple.GetValue(pk_column_offsets[0]));

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
bool ForeignKey::IsDeletedTupleReferencedBySourceTable(storage::DataTable* source_table,
                                                           index::Index* fk_index,
                                                           storage::Tuple& cur_tuple) {
  // Build referening key from this tuple to be used
  // to search the index
  std::unique_ptr<catalog::Schema>
      foreign_key_schema(catalog::Schema::CopySchema(source_table->GetSchema(),
                                                     fk_column_offsets));
  std::unique_ptr<storage::Tuple> key(new storage::Tuple(foreign_key_schema.get(), true));

  key->SetFromTuple(&cur_tuple, pk_column_offsets, fk_index->GetPool());

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
bool ForeignKey::DeleteReferencingTupleOnCascading(executor::ExecutorContext *executor_context,
                                                   storage::DataTable* source_table,
                                                   storage::Tuple& cur_tuple) {

  LOG_INFO("Cascading delete foreign key offset %u in table %s",
           fk_column_offsets[0], source_table->GetName().c_str());

  // Delete
  planner::DeletePlan delete_node(source_table, false);
  executor::DeleteExecutor delete_executor(&delete_node, executor_context);

  auto predicate = MakePredicate(cur_tuple);

  // Scan
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(source_table, predicate, fk_column_offsets));
  executor::SeqScanExecutor seq_scan_executor(seq_scan_node.get(),
                                              executor_context);

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
bool ForeignKey::SetNullReferencingTupleOnCascading(executor::ExecutorContext *executor_context,
                                                    storage::DataTable* source_table,
                                                    storage::Tuple& cur_tuple,
                                                    std::vector<oid_t>& direct_map_column_offsets) {
  LOG_INFO("Cascading set null foreign key offset %u in table %s",
           fk_column_offsets[0], source_table->GetName().c_str());

  Value null_val = ValueFactory::GetNullValue();

  // ProjectInfo
  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  // set those columns who need to be updated as null
  for (oid_t offset : fk_column_offsets) {
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
  planner::UpdatePlan update_node(source_table, std::move(project_info));

  executor::UpdateExecutor update_executor(&update_node, executor_context);

  // Predicate
  auto predicate = MakePredicate(cur_tuple);

  // Seq scan
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(source_table, predicate, fk_column_offsets));
  executor::SeqScanExecutor seq_scan_executor(seq_scan_node.get(),
                                              executor_context);

  update_node.AddChild(std::move(seq_scan_node));
  update_executor.AddChild(&seq_scan_executor);

  update_executor.Init();
  return update_executor.Execute();

}

}  // End catalog namespace
}  // End peloton namespace