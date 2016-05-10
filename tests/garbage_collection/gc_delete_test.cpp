//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_update_test_epoch.cpp
//
// Identification: tests/executor/gc_update_test_epoch.cpp
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

class GCDeleteTest : public PelotonTest {};

std::atomic<int> tuple_id;
std::atomic<int> delete_tuple_id;

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

void DeleteTuple(storage::DataTable *table) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  std::vector<storage::Tuple *> tuples;

  // Delete
  planner::DeletePlan delete_node(table, false);
  executor::DeleteExecutor delete_executor(&delete_node, context.get());

  // Predicate

  // WHERE ATTR_0 > 60
  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(
          ValueFactory::GetIntegerValue(60));
  auto predicate = new expression::ComparisonExpression<expression::CmpGt>(
      EXPRESSION_TYPE_COMPARE_GREATERTHAN, tup_val_exp, const_val_exp);

  // Seq scan
  std::vector<oid_t> column_ids = {0};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(table, predicate, column_ids));
  executor::SeqScanExecutor seq_scan_executor(seq_scan_node.get(),
                                              context.get());

  // Parent-Child relationship
  delete_node.AddChild(std::move(seq_scan_node));
  delete_executor.AddChild(&seq_scan_executor);

  EXPECT_TRUE(delete_executor.Init());
  EXPECT_TRUE(delete_executor.Execute());
  // EXPECT_TRUE(delete_executor.Execute());

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

TEST_F(GCDeleteTest, DeleteTest) {

  auto *table = ExecutorTestsUtil::CreateTable(1024);
  auto &manager = catalog::Manager::GetInstance();
  storage::Database db(DEFAULT_DB_ID);
  manager.AddDatabase(&db);
  db.AddTable(table);
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  enum GCType type = GC_TYPE_VACUUM;
  peloton::gc::GCManagerFactory::Configure(type);
  peloton::gc::GCManagerFactory::GetInstance().StartGC();


  auto before_insert = catalog::Manager::GetInstance().GetMemoryFootprint();
  LaunchParallelTest(1, InsertTuple, table, testing_pool);
  auto after_insert = catalog::Manager::GetInstance().GetMemoryFootprint();
  EXPECT_GT(after_insert, before_insert);
  LaunchParallelTest(1, DeleteTuple, table);
  std::this_thread::sleep_for(std::chrono::seconds(10));
  auto after_delete = catalog::Manager::GetInstance().GetMemoryFootprint();
  EXPECT_EQ(after_insert, after_delete);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  // Seq scan
  std::vector<oid_t> column_ids = {0};
  planner::SeqScanPlan seq_scan_node(table, nullptr, column_ids);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());
  EXPECT_TRUE(seq_scan_executor.Init());

  auto tuple_cnt = 0;
  while (seq_scan_executor.Execute()) {
    std::unique_ptr<executor::LogicalTile> result_logical_tile(
        seq_scan_executor.GetOutput());
    tuple_cnt += result_logical_tile->GetTupleCount();
  }
  txn_manager.CommitTransaction();
  EXPECT_EQ(tuple_cnt, 6);

  tuple_id = 0;
  peloton::gc::GCManagerFactory::GetInstance().StopGC();

  
  type = GC_TYPE_VACUUM;
  peloton::gc::GCManagerFactory::Configure(type);
  peloton::gc::GCManagerFactory::GetInstance().SetGCType(type);
  peloton::gc::GCManagerFactory::GetInstance().StartGC();

  before_insert = catalog::Manager::GetInstance().GetMemoryFootprint();
  LaunchParallelTest(1, InsertTuple, table, testing_pool);
  after_insert = catalog::Manager::GetInstance().GetMemoryFootprint();
  LaunchParallelTest(1, DeleteTuple, table);
  std::this_thread::sleep_for(std::chrono::seconds(10));
  after_delete = catalog::Manager::GetInstance().GetMemoryFootprint();
  EXPECT_EQ(after_insert, after_delete);
  tuple_id = 0;

  type = GC_TYPE_EPOCH;
  peloton::gc::GCManagerFactory::Configure(type);
  peloton::gc::GCManagerFactory::GetInstance().SetGCType(type);
  peloton::gc::GCManagerFactory::GetInstance().StartGC();

  before_insert = catalog::Manager::GetInstance().GetMemoryFootprint();
  LaunchParallelTest(1, InsertTuple, table, testing_pool);
  after_insert = catalog::Manager::GetInstance().GetMemoryFootprint();
  LaunchParallelTest(1, DeleteTuple, table);

  column_ids = {0};
  SeqScanCount(table, column_ids, nullptr);

  after_delete = catalog::Manager::GetInstance().GetMemoryFootprint();
  EXPECT_EQ(after_insert, after_delete);
  tuple_id = 0;

}

}  // namespace test
}  // namespace peloton
