//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_tests_util.cpp
//
// Identification: tests/concurrency/transaction_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_tests_util.h"
#include "backend/executor/executor_context.h"
#include "backend/executor/delete_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/expression/expression_util.h"
#include "backend/storage/tile.h"
#include "executor/mock_executor.h"
#include "backend/planner/delete_plan.h"
#include "backend/planner/insert_plan.h"

namespace peloton {
namespace executor {
class ExecutorContext;
}
namespace planner {
class InsertPlan;
class ProjectInfo;
}
namespace test {

storage::DataTable *TransactionTestsUtil::CreateCombinedPrimaryKeyTable() {
  auto id_column = catalog::Column(VALUE_TYPE_INTEGER,
                                   GetTypeSize(VALUE_TYPE_INTEGER), "id", true);
  id_column.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
                                              "not_null"));
  auto value_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "value", true);
  value_column.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
                                              "not_null"));

  // Create the table
  catalog::Schema *table_schema =
      new catalog::Schema({id_column, value_column});
  auto table_name = "TEST_TABLE";
  size_t tuples_per_tilegroup = 100;
  auto table = storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name, tuples_per_tilegroup,
      true, false);

  // Create index on the (id, value) column
  std::vector<oid_t> key_attrs = {0, 1};
  auto tuple_schema = table->GetSchema();
  bool unique = false;
  auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  auto index_metadata = new index::IndexMetadata(
      "primary_btree_index", 1234, INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);

  table->AddIndex(pkey_index);

  // Insert tuple
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  for (int i = 0; i < 10; i++) {
    ExecuteInsert(txn, table, i, i);
  }
  txn_manager.CommitTransaction();

  return table;
}


storage::DataTable *TransactionTestsUtil::CreatePrimaryKeyUniqueKeyTable() {
  auto id_column = catalog::Column(VALUE_TYPE_INTEGER,
                                   GetTypeSize(VALUE_TYPE_INTEGER), "id", true);
  id_column.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
                                              "not_null"));
  auto value_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "value", true);

  // Create the table
  catalog::Schema *table_schema =
      new catalog::Schema({id_column, value_column});
  auto table_name = "TEST_TABLE";
  size_t tuples_per_tilegroup = 100;
  auto table = storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name, tuples_per_tilegroup,
      true, false);

  // Create primary index on the id column
  std::vector<oid_t> key_attrs = {0};
  auto tuple_schema = table->GetSchema();
  bool unique = false;
  auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  auto index_metadata = new index::IndexMetadata(
      "primary_btree_index", 1234, INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);

  table->AddIndex(pkey_index);

  // Create unique index on the value column
  std::vector<oid_t> key_attrs2 = {1};
  auto tuple_schema2 = table->GetSchema();
  bool unique2 = false;
  auto key_schema2 = catalog::Schema::CopySchema(tuple_schema2, key_attrs2);
  key_schema2->SetIndexedColumns(key_attrs2);
  auto index_metadata2 = new index::IndexMetadata(
      "unique_btree_index", 1235, INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_UNIQUE, tuple_schema2, key_schema2, unique2);

  index::Index *ukey_index = index::IndexFactory::GetInstance(index_metadata2);

  table->AddIndex(ukey_index);

  // Insert tuple
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  for (int i = 0; i < 10; i++) {
    ExecuteInsert(txn, table, i, i);
  }
  txn_manager.CommitTransaction();

  return table;
}

storage::DataTable *TransactionTestsUtil::CreateTable(int num_key,
                                                      std::string table_name,
                                                      oid_t database_id,
                                                      oid_t relation_id,
                                                      oid_t index_oid,
                                                      bool need_primary_index,
                                                      bool need_secondary_index,
                                                      oid_t secondary_index_oid,
                                                      int inserted_value,
                                                      bool need_third_column) {
  auto id_column = catalog::Column(VALUE_TYPE_INTEGER,
                                   GetTypeSize(VALUE_TYPE_INTEGER), "id", true);
  auto value_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "value", true);

  auto value2_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "value2", true);

  catalog::Schema *table_schema = nullptr;
  // Create the table
  if (need_third_column) {
    table_schema = new catalog::Schema({id_column, value_column, value2_column});
  }
  else {
    table_schema = new catalog::Schema({id_column, value_column});
  }

  size_t tuples_per_tilegroup = 100;
  auto table = storage::TableFactory::GetDataTable(
      database_id, relation_id, table_schema, table_name, tuples_per_tilegroup,
      true, false);

  // Create index on the id column
  std::vector<oid_t> key_attrs = {0};
  auto tuple_schema = table->GetSchema();
  bool unique = false;
  auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  auto index_metadata = new index::IndexMetadata(
      "primary_btree_index", index_oid, INDEX_TYPE_BTREE,
      need_primary_index ? INDEX_CONSTRAINT_TYPE_PRIMARY_KEY : INDEX_CONSTRAINT_TYPE_DEFAULT,
      tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);

  table->AddIndex(pkey_index);

  if (need_secondary_index) {
    std::vector<oid_t> key_attrs = {1};
    auto tuple_schema = table->GetSchema();
    bool unique = false;
    auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    auto index_metadata = new index::IndexMetadata(
        "secondary_btree_index", secondary_index_oid, INDEX_TYPE_BTREE,
        INDEX_CONSTRAINT_TYPE_DEFAULT,
        tuple_schema, key_schema, unique);

    index::Index *sec_key_index = index::IndexFactory::GetInstance(index_metadata);

    table->AddIndex(sec_key_index);
  }

  if (need_third_column) {
    std::vector<oid_t> key_attrs = {2};
    auto tuple_schema = table->GetSchema();
    bool unique = false;
    auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    auto index_metadata = new index::IndexMetadata(
        "secondary_btree_index2", secondary_index_oid+1, INDEX_TYPE_BTREE,
        INDEX_CONSTRAINT_TYPE_DEFAULT,
        tuple_schema, key_schema, unique);

    index::Index *sec_key_index2 = index::IndexFactory::GetInstance(index_metadata);

    table->AddIndex(sec_key_index2);
  }

  // add this table to current database
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(database_id);
  if (db != nullptr) {
    db->AddTable(table);
  }

  // Insert tuple
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  for (int i = 0; i < num_key; i++) {
    // if we choose to have different value for different rows
    if (inserted_value == -1)
      ExecuteInsert(txn, table, i, i, need_third_column ? i : -1);
    else
      ExecuteInsert(txn, table, i, inserted_value, need_third_column ? inserted_value : -1);
  }
  txn_manager.CommitTransaction();

  return table;
}

std::unique_ptr<const planner::ProjectInfo>
TransactionTestsUtil::MakeProjectInfoFromTuple(const storage::Tuple *tuple) {
  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  for (oid_t col_id = START_OID; col_id < tuple->GetColumnCount(); col_id++) {
    auto value = tuple->GetValue(col_id);
    auto expression = expression::ExpressionUtil::ConstantValueFactory(value);
    target_list.emplace_back(col_id, expression);
  }

  return std::unique_ptr<const planner::ProjectInfo>(new planner::ProjectInfo(
      std::move(target_list), std::move(direct_map_list)));
}

bool TransactionTestsUtil::ExecuteInsert(concurrency::Transaction *transaction,
                                         storage::DataTable *table, int id,
                                         int value, int value2) {
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(transaction));

  // Make tuple
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(table->GetSchema(), true));
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  tuple->SetValue(0, ValueFactory::GetIntegerValue(id), testing_pool);
  tuple->SetValue(1, ValueFactory::GetIntegerValue(value), testing_pool);
  if (value2 != -1) {
    tuple->SetValue(2, ValueFactory::GetIntegerValue(value2), testing_pool);
  }
  std::unique_ptr<const planner::ProjectInfo> project_info{
      MakeProjectInfoFromTuple(tuple.get())};

  // Insert
  planner::InsertPlan node(table, std::move(project_info));
  executor::InsertExecutor executor(&node, context.get());
  return executor.Execute();
}

expression::ComparisonExpression<expression::CmpEq> *
TransactionTestsUtil::MakePredicate(int id) {
  auto tup_val_exp = new expression::TupleValueExpression(0, 0);
  auto const_val_exp = new expression::ConstantValueExpression(
      ValueFactory::GetIntegerValue(id));
  auto predicate = new expression::ComparisonExpression<expression::CmpEq>(
      EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_exp, const_val_exp);

  return predicate;
}

bool TransactionTestsUtil::ExecuteRead(concurrency::Transaction *transaction,
                                       storage::DataTable *table, int id,
                                       int &result, bool read_val2) {
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(transaction));

  // Predicate, WHERE `id`=id
  auto predicate = MakePredicate(id);

  // Seq scan
  std::vector<oid_t> column_ids;
  if(read_val2)
    column_ids = {0, 2};
  else
    column_ids = {0, 1};

  planner::SeqScanPlan seq_scan_node(table, predicate, column_ids);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  EXPECT_TRUE(seq_scan_executor.Init());
  if (seq_scan_executor.Execute() == false) {
    result = -1;
    return false;
  }

  std::unique_ptr<executor::LogicalTile> result_tile(
      seq_scan_executor.GetOutput());

  // Read nothing
  if (result_tile->GetTupleCount() == 0)
    result = -1;
  else {
    EXPECT_EQ(1, result_tile->GetTupleCount());
    Value raw_result = result_tile->GetValue(0, 1);
    if (raw_result.IsNull())
      result = INT32_NULL;
    else
      result = raw_result.GetIntegerForTestsOnly();
  }

  return true;
}
bool TransactionTestsUtil::ExecuteDelete(concurrency::Transaction *transaction,
                                         storage::DataTable *table, int id) {
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(transaction));

  // Delete
  planner::DeletePlan delete_node(table, false);
  executor::DeleteExecutor delete_executor(&delete_node, context.get());

  auto predicate = MakePredicate(id);

  // Scan
  std::vector<oid_t> column_ids = {0};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(table, predicate, column_ids));
  executor::SeqScanExecutor seq_scan_executor(seq_scan_node.get(),
                                              context.get());

  delete_node.AddChild(std::move(seq_scan_node));
  delete_executor.AddChild(&seq_scan_executor);

  EXPECT_TRUE(delete_executor.Init());

  return delete_executor.Execute();
}
bool TransactionTestsUtil::ExecuteUpdate(concurrency::Transaction *transaction,
                                         storage::DataTable *table, int id,
                                         int value, bool update_val2) {
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(transaction));

  Value update_val = ValueFactory::GetIntegerValue(value);

  // ProjectInfo
  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;
  if (update_val2) {
    target_list.emplace_back(
        2, expression::ExpressionUtil::ConstantValueFactory(update_val));
    direct_map_list.emplace_back(0, std::pair<oid_t, oid_t>(0, 0));
    direct_map_list.emplace_back(1, std::pair<oid_t, oid_t>(0, 1));
  }
  else {
    target_list.emplace_back(
        1, expression::ExpressionUtil::ConstantValueFactory(update_val));
    direct_map_list.emplace_back(0, std::pair<oid_t, oid_t>(0, 0));
    if (table->GetSchema()->GetColumnCount() == 3)
      direct_map_list.emplace_back(2, std::pair<oid_t, oid_t>(0, 2));
  }

  // Update plan
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan update_node(table, std::move(project_info));

  executor::UpdateExecutor update_executor(&update_node, context.get());

  // Predicate
  auto predicate = MakePredicate(id);

  // Seq scan
  std::vector<oid_t> column_ids = {0};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(table, predicate, column_ids));
  executor::SeqScanExecutor seq_scan_executor(seq_scan_node.get(),
                                              context.get());

  update_node.AddChild(std::move(seq_scan_node));
  update_executor.AddChild(&seq_scan_executor);

  EXPECT_TRUE(update_executor.Init());
  return update_executor.Execute();
}

bool TransactionTestsUtil::ExecuteUpdateByValue(concurrency::Transaction *txn,
                                                storage::DataTable *table,
                                                int old_value,
                                                int new_value,
                                                bool update_val2) {
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  Value update_val = ValueFactory::GetIntegerValue(new_value);

  // ProjectInfo
  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;
  if (update_val2) {
    target_list.emplace_back(
        2, expression::ExpressionUtil::ConstantValueFactory(update_val));
    direct_map_list.emplace_back(0, std::pair<oid_t, oid_t>(0, 0));
    direct_map_list.emplace_back(1, std::pair<oid_t, oid_t>(0, 1));
  }
  else {
    target_list.emplace_back(
        1, expression::ExpressionUtil::ConstantValueFactory(update_val));
    direct_map_list.emplace_back(0, std::pair<oid_t, oid_t>(0, 0));
    if (table->GetSchema()->GetColumnCount() == 3)
      direct_map_list.emplace_back(2, std::pair<oid_t, oid_t>(0, 2));
  }
  // Update plan
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan update_node(table, std::move(project_info));

  executor::UpdateExecutor update_executor(&update_node, context.get());

  // Predicate
  auto tup_val_exp = new expression::TupleValueExpression(0, update_val2 ? (unsigned)2 : (unsigned)1);
  auto const_val_exp = new expression::ConstantValueExpression(
      ValueFactory::GetIntegerValue(old_value));
  auto predicate = new expression::ComparisonExpression<expression::CmpEq>(
      EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_exp, const_val_exp);

  // Seq scan
  std::vector<oid_t> column_ids = {0, update_val2 ? (unsigned)2 : (unsigned)1};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(table, predicate, column_ids));
  executor::SeqScanExecutor seq_scan_executor(seq_scan_node.get(),
                                              context.get());

  update_node.AddChild(std::move(seq_scan_node));
  update_executor.AddChild(&seq_scan_executor);

  EXPECT_TRUE(update_executor.Init());
  return update_executor.Execute();
}

bool TransactionTestsUtil::ExecuteScan(concurrency::Transaction *transaction,
                                       std::vector<int> &results,
                                       storage::DataTable *table, int id) {
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(transaction));

  // Predicate, WHERE `id`>=id1
  auto tup_val_exp = new expression::TupleValueExpression(0, 0);
  auto const_val_exp = new expression::ConstantValueExpression(
      ValueFactory::GetIntegerValue(id));

  auto predicate = new expression::ComparisonExpression<expression::CmpGte>(
      EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO, tup_val_exp, const_val_exp);

  // Seq scan
  std::vector<oid_t> column_ids = {0, 1};
  planner::SeqScanPlan seq_scan_node(table, predicate, column_ids);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  EXPECT_TRUE(seq_scan_executor.Init());
  if (seq_scan_executor.Execute() == false) return false;

  std::unique_ptr<executor::LogicalTile> result_tile(
      seq_scan_executor.GetOutput());

  for (size_t i = 0; i < result_tile->GetTupleCount(); i++) {
    results.push_back(result_tile->GetValue(i, 1).GetIntegerForTestsOnly());
  }
  return true;
}
}
}
