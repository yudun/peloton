//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_stress_test_coop.cpp
//
// Identification: tests/executor/gc_stress_test_coop.cpp
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

class GCTests : public PelotonTest {};

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
      new expression::TupleValueExpression(0, 0);
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

TEST_F(GCTests, StressTests) {

  peloton::gc::GCManagerFactory::Configure(type);
  peloton::gc::GCManagerFactory::GetInstance().StartGC();

  auto *table = ExecutorTestsUtil::CreateTable(1024);
  auto &manager = catalog::Manager::GetInstance();
  storage::Database db(DEFAULT_DB_ID);
  manager.AddDatabase(&db);
  db.AddTable(table);
  // We are going to insert a tile group into a table in this test

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  // Pass through insert executor.
  auto null_tuple = ExecutorTestsUtil::GetNullTuple(table, testing_pool);

  planner::InsertPlan node(table, std::move(null_tuple));
  executor::InsertExecutor executor(&node, context.get());

  try {
    executor.Execute();
  } catch (ConstraintException &ce) {
    LOG_ERROR("%s", ce.what());
  }

  auto non_empty_tuple = ExecutorTestsUtil::GetTuple(table, ++tuple_id, testing_pool);
  planner::InsertPlan node2(table, std::move(non_empty_tuple));
  executor::InsertExecutor executor2(&node2, context.get());
  executor2.Execute();

  try {
    executor2.Execute();
  } catch (ConstraintException &ce) {
    LOG_ERROR("%s", ce.what());
  }

  txn_manager.CommitTransaction();
  auto before_insert = catalog::Manager::GetInstance().GetMemoryFootprint();
  LaunchParallelTest(1, InsertTuple, table, testing_pool);
  LOG_TRACE("%s", table->GetInfo().c_str());
  auto after_insert = catalog::Manager::GetInstance().GetMemoryFootprint();

  LOG_INFO("---------------------------------------------");

  // LaunchParallelTest(1, UpdateTuple, table);
  // LOG_TRACE(table->GetInfo().c_str());

  LOG_INFO("---------------------------------------------");

  LaunchParallelTest(1, DeleteTuple, table);
  auto after_delete = catalog::Manager::GetInstance().GetMemoryFootprint();
  EXPECT_GT(after_insert, before_insert);
  EXPECT_EQ(after_insert, after_delete);
  LOG_TRACE("%s",table->GetInfo().c_str());
  // PRIMARY KEY
  std::vector<catalog::Column> columns;

  columns.push_back(ExecutorTestsUtil::GetColumnInfo(0));
  catalog::Schema *key_schema = new catalog::Schema(columns);
  storage::Tuple *key1 = new storage::Tuple(key_schema, true);
  storage::Tuple *key2 = new storage::Tuple(key_schema, true);

  key1->SetValue(0, ValueFactory::GetIntegerValue(10), nullptr);
  key2->SetValue(0, ValueFactory::GetIntegerValue(100), nullptr);

  delete key1;
  delete key2;
  delete key_schema;

  // SEC KEY
  columns.clear();
  columns.push_back(ExecutorTestsUtil::GetColumnInfo(0));
  columns.push_back(ExecutorTestsUtil::GetColumnInfo(1));
  key_schema = new catalog::Schema(columns);

  storage::Tuple *key3 = new storage::Tuple(key_schema, true);
  storage::Tuple *key4 = new storage::Tuple(key_schema, true);

  key3->SetValue(0, ValueFactory::GetIntegerValue(10), nullptr);
  key3->SetValue(1, ValueFactory::GetIntegerValue(11), nullptr);
  key4->SetValue(0, ValueFactory::GetIntegerValue(100), nullptr);
  key4->SetValue(1, ValueFactory::GetIntegerValue(101), nullptr);

  delete key3;
  delete key4;
  delete key_schema;

  tuple_id = 0;
}

}  // namespace test
}  // namespace peloton
