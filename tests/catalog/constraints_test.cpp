//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// catalog_test.cpp
//
// Identification: tests/catalog/constraints_test.cpp
//
// Copyright (c) 2016, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"
#include "harness.h"

#include "backend/catalog/schema.h"
#include "backend/common/value.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/abstract_executor.h"
#include "backend/storage/tile_group_factory.h"
#include "backend/storage/tuple.h"
#include "backend/storage/table_factory.h"
#include "backend/index/index_factory.h"

#include "concurrency/transaction_tests_util.h"
#include "catalog/constraints_tests_util.h"


namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Constraints Tests
//===--------------------------------------------------------------------===//

class ConstraintsTests : public PelotonTest {};

TEST_F(ConstraintsTests, NOTNULLTest) {
  // First, generate the table with index
  // this table has 15 rows:
  //  int(primary)  int   double  var(22) (unique)
  //  0             1     2       "3"
  //  10            11    12      "13"
  //  20            21    22      "23"
  //  .....
  //  140           141   142     "143"

//  storage::DataTable * data_table = ConstraintsTestsUtil::CreateAndPopulateTable();
  std::unique_ptr<storage::DataTable> data_table(
      ConstraintsTestsUtil::CreateAndPopulateTable());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // begin this transaction
  auto txn = txn_manager.BeginTransaction();


  bool hasException = false;
  try {
    // Test1: insert a tuple with column 1 = null
    ConstraintsTestsUtil::ExecuteInsert(txn, data_table.get(),
      ValueFactory::GetNullValue(),
      ValueFactory::GetIntegerValue(ConstraintsTestsUtil::PopulatedValue(15, 1)),
      ValueFactory::GetIntegerValue(ConstraintsTestsUtil::PopulatedValue(15, 2)),
      ValueFactory::GetStringValue(std::to_string(ConstraintsTestsUtil::PopulatedValue(
                                            15, 3))) );

  } catch (ConstraintException e){
    hasException = true;
  }
  EXPECT_TRUE(hasException);


  // Test2: insert a legal tuple
  hasException = false;
  try {
    ConstraintsTestsUtil::ExecuteInsert(txn, data_table.get(),
      ValueFactory::GetIntegerValue(ConstraintsTestsUtil::PopulatedValue(15, 0)),
      ValueFactory::GetIntegerValue(ConstraintsTestsUtil::PopulatedValue(15, 1)),
      ValueFactory::GetIntegerValue(ConstraintsTestsUtil::PopulatedValue(15, 2)),
      ValueFactory::GetStringValue(std::to_string(ConstraintsTestsUtil::PopulatedValue(
          15, 3))) );
  } catch (ConstraintException e){
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // commit this transaction
  txn_manager.CommitTransaction();

}

TEST_F(ConstraintsTests, SingleThreadedUniqueKeyTest) {
  // First, generate the table with index
  // this table has 15 rows:
  //  int(primary)  int   double  var(22)
  //  0             1     2       "3"
  //  10            11    12      "13"
  //  20            21    22      "23"
  //  .....
  //  140           141   142     "143"

  std::unique_ptr<storage::DataTable> data_table(
      ConstraintsTestsUtil::CreateAndPopulateTable());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // begin this transaction
  auto txn = txn_manager.BeginTransaction();

//  const catalog::Schema *schema = data_table->GetSchema();


  // Test1: insert a tuple with column 1 = 0, a illegal primary key
  bool hasException = false;
  try {
    ConstraintsTestsUtil::ExecuteInsert(txn, data_table.get(),
      ValueFactory::GetIntegerValue(ConstraintsTestsUtil::PopulatedValue(0, 0)),
      ValueFactory::GetIntegerValue(ConstraintsTestsUtil::PopulatedValue(15, 1)),
      ValueFactory::GetIntegerValue(ConstraintsTestsUtil::PopulatedValue(15, 2)),
      ValueFactory::GetStringValue(std::to_string(ConstraintsTestsUtil::PopulatedValue(
          15, 3))) );

  } catch (ConstraintException e){
    hasException = true;
  }
  EXPECT_TRUE(hasException);

  // commit this transaction
  txn_manager.CommitTransaction();
}

}  // End test namespace
}  // End peloton namespace
