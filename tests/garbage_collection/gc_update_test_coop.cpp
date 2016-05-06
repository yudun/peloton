//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_update_test_coop.cpp
//
// Identification: tests/executor/gc_update_test_coop.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <atomic>

#include "harness.h"

#include "backend/catalog/schema.h"
#include "backend/common/value_factory.h"
#include "backend/common/types.h"
#include "backend/common/pool.h"

#include "backend/executor/executor_context.h"
#include "backend/executor/delete_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/abstract_expression.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/table_factory.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"

#include "backend/planner/delete_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/planner/update_plan.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// GC Tests
//===--------------------------------------------------------------------===//

class GCUpdateTestCoop : public PelotonTest {};

std::atomic<int> tuple_id;
std::atomic<int> delete_tuple_id;
enum GCType type = GC_TYPE_COOPERATIVE;

void InsertTuple(storage::DataTable *table, VarlenPool *pool) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  for (oid_t tuple_itr = 0; tuple_itr < 10; tuple_itr++) {
    auto tuple = ExecutorTestsUtil::GetTuple(table, ++tuple_id, pool);

    planner::InsertPlan node(table, std::move(tuple));
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }

  txn_manager.CommitTransaction();
}

void UpdateTuple(storage::DataTable *table) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Update
  std::vector<oid_t> update_column_ids = {2};
  std::vector<Value> values;
  Value update_val = ValueFactory::GetDoubleValue(23.5);

  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;
  target_list.emplace_back(
      2, expression::ExpressionUtil::ConstantValueFactory(update_val));
  LOG_INFO("%lu", target_list.at(0).first);
  direct_map_list.emplace_back(0, std::pair<oid_t, oid_t>(0, 0));
  direct_map_list.emplace_back(1, std::pair<oid_t, oid_t>(0, 1));
  direct_map_list.emplace_back(3, std::pair<oid_t, oid_t>(0, 3));

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan update_node(table, std::move(project_info));

  executor::UpdateExecutor update_executor(&update_node, context.get());

  // Predicate

  // WHERE ATTR_0 < 70
  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(0, 0);
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(
          ValueFactory::GetIntegerValue(70));
  auto predicate = new expression::ComparisonExpression<expression::CmpLt>(
      EXPRESSION_TYPE_COMPARE_LESSTHAN, tup_val_exp, const_val_exp);

  // Seq scan
  std::vector<oid_t> column_ids = {0};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(table, predicate, column_ids));
  executor::SeqScanExecutor seq_scan_executor(seq_scan_node.get(),
                                              context.get());

  // Parent-Child relationship
  update_node.AddChild(std::move(seq_scan_node));
  update_executor.AddChild(&seq_scan_executor);

  EXPECT_TRUE(update_executor.Init());
  while (update_executor.Execute())
    ;

  txn_manager.CommitTransaction();
}

int SeqScanCount(storage::DataTable *table,
                 const std::vector<oid_t> &column_ids,
                 expression::AbstractExpression *predicate) {

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  planner::SeqScanPlan seq_scan_node(table, predicate, column_ids);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  EXPECT_TRUE(seq_scan_executor.Init());
  auto tuple_cnt = 0;

  while (seq_scan_executor.Execute()) {
    std::unique_ptr<executor::LogicalTile> result_logical_tile(
        seq_scan_executor.GetOutput());
    tuple_cnt += result_logical_tile->GetTupleCount();
  }

  txn_manager.CommitTransaction();

  return tuple_cnt;
}

TEST_F(GCUpdateTestCoop, UpdateTestCoop) {

  peloton::gc::GCManagerFactory::Configure(type);
  peloton::gc::GCManagerFactory::GetInstance().StartGC();

  auto *table = ExecutorTestsUtil::CreateTable(1024);
  auto &manager = catalog::Manager::GetInstance();
  storage::Database db(DEFAULT_DB_ID);
  manager.AddDatabase(&db);
  db.AddTable(table);
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  auto before_insert = catalog::Manager::GetInstance().GetMemoryFootprint();
  LaunchParallelTest(1, InsertTuple, table, testing_pool);
  auto after_insert = catalog::Manager::GetInstance().GetMemoryFootprint();
  LaunchParallelTest(1, UpdateTuple, table);

  EXPECT_GT(after_insert, before_insert);
  // Seq scan to check number
  std::vector<oid_t> column_ids = {0};
  auto tuple_cnt = SeqScanCount(table, column_ids, nullptr);
  EXPECT_EQ(tuple_cnt, 10);

  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(0, 2);
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(
          ValueFactory::GetDoubleValue(23.5));

  auto predicate = new expression::ComparisonExpression<expression::CmpEq>(
      EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_exp, const_val_exp);

  tuple_cnt = SeqScanCount(table, column_ids, predicate);
  auto after_update = catalog::Manager::GetInstance().GetMemoryFootprint();
  EXPECT_EQ(after_insert, after_update);

  EXPECT_EQ(tuple_cnt, 6);

  tuple_id = 0;

}

}  // namespace test
}  // namespace peloton
