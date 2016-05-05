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
#include "backend/bridge/ddl/bridge.h"
#include "backend/bridge/ddl/ddl_index.h"

#include "catalog/constraints_tests_util.h"
#include "concurrency/transaction_tests_util.h"

#define NOTNULL_TEST
#define PRIMARY_UNIQUEKEY_TEST
#define FOREIGHN_KEY_INSERT_TEST
#define FOREIGHN_KEY_RESTRICT_DELETE_TEST
#define FOREIGHN_KEY_CASCADE_DELETE_TEST
#define FOREIGHN_KEY_SETNULL_DELETE_TEST
#define FOREIGHN_KEY_RESTRICT_UPDATE_TEST
#define FOREIGHN_KEY_CASCADE_UPDATE_TEST
#define FOREIGHN_KEY_SETNULL_UPDATE_TEST
#define DROPSETNOTNULL_TEST
#define DROPUNIQUE_TEST
#define SETUNIQUE_TEST

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Constraints Tests
//===--------------------------------------------------------------------===//

class ConstraintsTests : public PelotonTest {};

#ifdef NOTNULL_TEST
TEST_F(ConstraintsTests, NOTNULLTest) {
  // First, generate the table with index
  // this table has 15 rows:
  //  int(primary)  int   double  var(22) (unique)
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

  // Test1: insert a tuple with column 1 = null
  bool hasException = false;
  try {
    ConstraintsTestsUtil::ExecuteInsert(
        txn, data_table.get(), ValueFactory::GetNullValue(),
        ValueFactory::GetIntegerValue(
            ConstraintsTestsUtil::PopulatedValue(15, 1)),
        ValueFactory::GetIntegerValue(
            ConstraintsTestsUtil::PopulatedValue(15, 2)),
        ValueFactory::GetStringValue(
            std::to_string(ConstraintsTestsUtil::PopulatedValue(15, 3))));

  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_TRUE(hasException);

  // Test2: insert a legal tuple
  hasException = false;
  try {
    ConstraintsTestsUtil::ExecuteInsert(
        txn, data_table.get(), ValueFactory::GetIntegerValue(
                                   ConstraintsTestsUtil::PopulatedValue(15, 0)),
        ValueFactory::GetIntegerValue(
            ConstraintsTestsUtil::PopulatedValue(15, 1)),
        ValueFactory::GetIntegerValue(
            ConstraintsTestsUtil::PopulatedValue(15, 2)),
        ValueFactory::GetStringValue(
            std::to_string(ConstraintsTestsUtil::PopulatedValue(15, 3))));
  } catch (ConstraintException e) {
    hasException = true;
  }
  EXPECT_FALSE(hasException);

  // commit this transaction
  txn_manager.CommitTransaction();
}
#endif

#ifdef PRIMARY_UNIQUEKEY_TEST
TEST_F(ConstraintsTests, CombinedPrimaryKeyTest) {
  // First, generate the table with index
  // this table has 10 rows:
  //  int(primary)  int(primary)
  //  0             0
  //  1             1
  //  2             2
  //  .....
  //  9             9

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  {
    std::unique_ptr<storage::DataTable> data_table(
        TransactionTestsUtil::CreateCombinedPrimaryKeyTable());
    // Test1: insert 2 tuple with duplicated primary key
    // txn1: insert (0, 1) -- success
    // txn0 commit
    // txn1: insert (1, 1) -- fail
    // txn1 commit
    TransactionScheduler scheduler(2, data_table.get(), &txn_manager);
    scheduler.Txn(0).Insert(0, 1);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Insert(1, 1);
    scheduler.Txn(1).Commit();

    scheduler.Run();

    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_ABORTED == scheduler.schedules[1].txn_result);
  }
}

TEST_F(ConstraintsTests, MultiTransactionUniqueConstraintsTest) {
  // First, generate the table with index
  // this table has 10 rows:
  //  int(primary)  int(unique)
  //  0             0
  //  1             1
  //  2             2
  //  .....
  //  9             9

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  {
    std::unique_ptr<storage::DataTable> data_table(
        TransactionTestsUtil::CreatePrimaryKeyUniqueKeyTable());
    // Test1: insert 2 tuple with duplicated primary key
    TransactionScheduler scheduler(2, data_table.get(), &txn_manager);
    scheduler.Txn(0).Insert(10, 10);
    scheduler.Txn(1).Insert(10, 11);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Run();

    EXPECT_TRUE((RESULT_SUCCESS == scheduler.schedules[0].txn_result &&
                 RESULT_ABORTED == scheduler.schedules[1].txn_result) ||
                (RESULT_SUCCESS == scheduler.schedules[1].txn_result &&
                 RESULT_ABORTED == scheduler.schedules[0].txn_result));
  }

  {
    std::unique_ptr<storage::DataTable> data_table(
        TransactionTestsUtil::CreatePrimaryKeyUniqueKeyTable());
    // Test2: update a tuple to be a illegal unique key
    // txn1: update (1, 1) -> (1,11) -- success
    // txn0: update (0, 0) -> (0,1) -- fail
    // txn1 commit
    // txn0 commit
    TransactionScheduler scheduler(3, data_table.get(), &txn_manager);
    scheduler.Txn(1).Update(1, 11);
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

    scheduler.Txn(2).Read(0);
    scheduler.Txn(2).Read(1);
    scheduler.Txn(2).Commit();

    scheduler.Run();

    EXPECT_TRUE(RESULT_ABORTED == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[2].txn_result);
    EXPECT_TRUE(0 == scheduler.schedules[2].results[0]);
    EXPECT_TRUE(11 == scheduler.schedules[2].results[1]);
  }

  {
    std::unique_ptr<storage::DataTable> data_table(
        TransactionTestsUtil::CreatePrimaryKeyUniqueKeyTable());
    // Test3: update a tuple to be a legal primary key
    // txn1: update (1, 1) -> (1,11) -- success
    // txn1 commit
    // txn0: update (0, 0) -> (0,1) -- success
    // txn0 commit
    TransactionScheduler scheduler(2, data_table.get(), &txn_manager);
    scheduler.Txn(1).Update(1, 11);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(0).Commit();

    scheduler.Run();

    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
  }
}
#endif

TEST_F(ConstraintsTests, ForeignKeyTest) {

  // create new db
  auto &manager = catalog::Manager::GetInstance();
  oid_t current_db_oid = bridge::Bridge::GetCurrentDatabaseOid();
  auto newdb = new storage::Database(current_db_oid);
  manager.AddDatabase(newdb);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

#ifdef FOREIGHN_KEY_INSERT_TEST
  //     TABLE A -- src table          TABLE B -- sink table
  // int(primary)       int(ref B)       int(primary)  int
  //    0                 1               0             0
  //    1                 1               1             0
  //    2                 1               2             0
  //                                      .....
  //                                      9             0
  LOG_INFO("BEGIN FOREIGN KEY INSERT TEST-----------------------------------");
  {
    auto table_A =
        TransactionTestsUtil::CreateTable(3, "tableA", 0, 1000, 1000, true, true, 1010, 1);
    auto table_B =
        TransactionTestsUtil::CreateTable(10, "tableB", 0, 1001, 1001, true);

    // add the foreign key constraints for table_A
    auto foreign_key = new catalog::ForeignKey(
        1000, 1001,
        table_B->GetIndexIdWithColumnOffsets({0}),
        table_A->GetIndexIdWithColumnOffsets({1}),
        {"id"}, {0}, {"value"}, {1}, FOREIGNKEY_ACTION_NOACTION,
        FOREIGNKEY_ACTION_NOACTION, "THIS_IS_FOREIGN_CONSTRAINT");
    table_A->AddForeignKey(foreign_key);


    // Test1: insert 2 tuple, one of which doesn't follow foreign key constraint
    // txn0 insert (10,10) --> fail
    // txn1 insert (11, 9) --> success
    // txn0 commit
    // txn1 commit
    TransactionScheduler scheduler(2, table_A, &txn_manager);
    scheduler.Txn(0).Insert(10, 10);
    scheduler.Txn(1).Insert(11, 9);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Run();

    EXPECT_TRUE(RESULT_ABORTED == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
  }
#endif

#ifdef FOREIGHN_KEY_RESTRICT_DELETE_TEST
  //     TABLE C -- src table          TABLE D -- sink table
  // int(primary)       int(ref B)       int(primary)  int
  //    0                 1               0             0
  //    1                 1               1             0
  //    2                 1               2             0
  //                                      .....
  //                                      9             0
  LOG_INFO("BEGIN FOREIGN KEY RESTRICT_DELETE TEST-----------------------------------");
  {
    auto table_C =
        TransactionTestsUtil::CreateTable(3, "tableC", 0, 2000, 2000, true, true, 2010, 1);
    auto table_D =
        TransactionTestsUtil::CreateTable(10, "tableD", 0, 2001, 2001, true);

    // add the foreign key constraints for table_C
    auto foreign_key = new catalog::ForeignKey(
        2000, 2001,
        table_D->GetIndexIdWithColumnOffsets({0}),
        table_C->GetIndexIdWithColumnOffsets({1}),
        {"id"}, {0}, {"value"}, {1}, FOREIGNKEY_ACTION_NOACTION,
        FOREIGNKEY_ACTION_RESTRICT, "THIS_IS_FOREIGN_CONSTRAINT");
    table_C->AddForeignKey(foreign_key);
    // Test2: insert 2 tuple, one of which doesn't follow foreign key
    // constraint's restrict/noaction action
    // txn0 delete (0, tableD) --> success
    // txn1 delete (1, tableD) --> fail
    // txn0 commit
    // txn1 commit
    // txn2 read (1, tableC) --> still can read 1
    // txn2 read (1, tableD) --> still can read 0
    // txn2 read (0, tableD) --> can't read 0
    // txn2 commit
    TransactionScheduler scheduler(3, table_D, &txn_manager);
    scheduler.Txn(0).Delete(0, table_D);
    scheduler.Txn(1).Delete(1, table_D);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Txn(2).Read(1, table_C);
    scheduler.Txn(2).Read(1, table_D);
    scheduler.Txn(2).Read(0, table_D);
    scheduler.Txn(2).Commit();

    scheduler.Run();

    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_ABORTED == scheduler.schedules[1].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[2].txn_result);
    EXPECT_EQ(1, scheduler.schedules[2].results[0]);
    EXPECT_EQ(0, scheduler.schedules[2].results[1]);
    EXPECT_EQ(-1, scheduler.schedules[2].results[2]);
  }
#endif

#ifdef FOREIGHN_KEY_CASCADE_DELETE_TEST
  //     TABLE E -- src table          TABLE F -- sink table
  // int(primary)       int(ref B)       int(primary)  int
  //    0                 1               0             0
  //    1                 1               1             0
  //    2                 1               2             0
  //                                      .....
  //                                      9             0
  LOG_INFO("BEGIN FOREIGN KEY CASCADE_DELETE TEST-----------------------------------");
  {
    auto table_E =
        TransactionTestsUtil::CreateTable(3, "tableE", 0, 3000, 3000, true, true, 3010, 1);
    auto table_F =
        TransactionTestsUtil::CreateTable(10, "tableF", 0, 3001, 3001, true);

    // add the foreign key constraints for table_E
    auto foreign_key = new catalog::ForeignKey(
        3000, 3001,
        table_F->GetIndexIdWithColumnOffsets({0}),
        table_E->GetIndexIdWithColumnOffsets({1}),
        {"id"}, {0}, {"value"}, {1}, FOREIGNKEY_ACTION_NOACTION,
        FOREIGNKEY_ACTION_CASCADE, "THIS_IS_FOREIGN_CONSTRAINT");
    table_E->AddForeignKey(foreign_key);
    // Test3: cascading delete tuples
    // constraint's restrict/noaction action
    // txn1 read (0, tableE) --> 1
    // txn1 read (1, tableE) --> 1
    // txn1 read (2, tableE) --> 1
    // txn1 read (1, tableF) --> 0
    // txn0 delete (1, tableF) --> cascade delete, 1 in tableE will also be deleted
    // txn0 commit
    // txn1 commit
    // txn2 read (0, tableE) --> empty
    // txn2 read (1, tableE) --> empty
    // txn2 read (2, tableE) --> empty
    // txn2 read (1, tableF) --> empty
    // txn2 commit
    TransactionScheduler scheduler(3, table_F, &txn_manager);
    scheduler.Txn(1).Read(0, table_E);
    scheduler.Txn(1).Read(1, table_E);
    scheduler.Txn(1).Read(2, table_E);
    scheduler.Txn(1).Read(1, table_F);
    scheduler.Txn(0).Delete(1, table_F);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Txn(2).Read(0, table_E);
    scheduler.Txn(2).Read(1, table_E);
    scheduler.Txn(2).Read(2, table_E);
    scheduler.Txn(2).Read(1, table_F);
    scheduler.Txn(2).Commit();

    scheduler.Run();

    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[2].txn_result);
    EXPECT_EQ(1, scheduler.schedules[1].results[0]);
    EXPECT_EQ(1, scheduler.schedules[1].results[1]);
    EXPECT_EQ(1, scheduler.schedules[1].results[2]);
    EXPECT_EQ(0, scheduler.schedules[1].results[3]);
    EXPECT_EQ(-1, scheduler.schedules[2].results[0]);
    EXPECT_EQ(-1, scheduler.schedules[2].results[1]);
    EXPECT_EQ(-1, scheduler.schedules[2].results[2]);
    EXPECT_EQ(-1, scheduler.schedules[2].results[3]);
  }
#endif

#ifdef FOREIGHN_KEY_SETNULL_DELETE_TEST
  LOG_INFO("BEGIN FOREIGN KEY SETNULL_DELETE TEST-----------------------------------");
  //     TABLE G -- src table          TABLE H -- sink table
  // int(primary)   int(ref B)           int(primary)  int
  //    0                 1               0             0
  //    1                 1               1             0
  //    2                 1               2             0
  //                                      .....
  //                                      9             0

  {
    auto table_G =
        TransactionTestsUtil::CreateTable(3, "tableG", 0, 4000, 4000, true, true, 4010, 1);
    auto table_H =
        TransactionTestsUtil::CreateTable(10, "tableH", 0, 4001, 4001, true);

    // add the foreign key constraints for table_G
    auto foreign_key = new catalog::ForeignKey(
        4000, 4001,
        table_H->GetIndexIdWithColumnOffsets({0}),
        table_G->GetIndexIdWithColumnOffsets({1}),
        {"id"}, {0}, {"value"}, {1}, FOREIGNKEY_ACTION_NOACTION,
        FOREIGNKEY_ACTION_SETNULL, "THIS_IS_FOREIGN_CONSTRAINT");
    table_G->AddForeignKey(foreign_key);
    // Test4: delete tuple and cascading set null
    // constraint's restrict/noaction action
    // txn1 read (0, tableG) --> 1
    // txn1 read (1, tableG) --> 1
    // txn1 read (2, tableG) --> 1
    // txn1 read (1, tableH) --> 0
    // txn0 delete (1, tableH) --> cascade delete, 1 in tableG will also be deleted
    // txn0 commit
    // txn1 commit
    // txn2 read (0, tableG) --> NULL
    // txn2 read (1, tableG) --> NULL
    // txn2 read (2, tableG) --> NULL
    // txn2 read (1, tableH) --> empty
    // txn2 commit
    TransactionScheduler scheduler(3, table_H, &txn_manager);
    scheduler.Txn(1).Read(0, table_G);
    scheduler.Txn(1).Read(1, table_G);
    scheduler.Txn(1).Read(2, table_G);
    scheduler.Txn(1).Read(1, table_H);
    scheduler.Txn(0).Delete(1, table_H);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Txn(2).Read(0, table_G);
    scheduler.Txn(2).Read(1, table_G);
    scheduler.Txn(2).Read(2, table_G);
    scheduler.Txn(2).Read(1, table_H);
    scheduler.Txn(2).Commit();

    scheduler.Run();

    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[2].txn_result);
    EXPECT_EQ(1, scheduler.schedules[1].results[0]);
    EXPECT_EQ(1, scheduler.schedules[1].results[1]);
    EXPECT_EQ(1, scheduler.schedules[1].results[2]);
    EXPECT_EQ(0, scheduler.schedules[1].results[3]);
    EXPECT_EQ(INT32_NULL, scheduler.schedules[2].results[0]);
    EXPECT_EQ(INT32_NULL, scheduler.schedules[2].results[1]);
    EXPECT_EQ(INT32_NULL, scheduler.schedules[2].results[2]);
    EXPECT_EQ(-1, scheduler.schedules[2].results[3]);
  }
#endif

#ifdef FOREIGHN_KEY_RESTRICT_UPDATE_TEST
  LOG_INFO("BEGIN FOREIGN KEY RESTRICT_UPDATE TEST-----------------------------------");
  //     TABLE I -- src table          TABLE J -- sink table
  // int(primary)   int(ref J v1)        int(primary)    int   int
  //    0                 1               0               0     0
  //    1                 1               1               1     1
  //    2                 1               2               2     2
  //                                      .....
  //                                      9               9     9

  {
    auto table_I =
        TransactionTestsUtil::CreateTable(3, "tableI", 0, 5000, 5000, true, true, 5010, 1);
    auto table_J =
        TransactionTestsUtil::CreateTable(10, "tableJ", 0, 5001, 5001, true, true, 5020, -1, true);

    // add the foreign key constraints for table_G
    auto foreign_key = new catalog::ForeignKey(
        5000, 5001,
        table_J->GetIndexIdWithColumnOffsets({1}),
        table_I->GetIndexIdWithColumnOffsets({1}),
        {"value"}, {1}, {"value"}, {1}, FOREIGNKEY_ACTION_RESTRICT,
        FOREIGNKEY_ACTION_NOACTION, "THIS_IS_FOREIGN_CONSTRAINT");
    table_I->AddForeignKey(foreign_key);
    // Test4: delete tuple and cascading set null
    // constraint's restrict/noaction action
    // txn1 read (0, tableI) --> 1
    // txn1 read (1, tableI) --> 1
    // txn1 read (2, tableI) --> 1
    // txn1 read (1, tableJ) --> 1
    // txn1 read (1, tableJ, read_val2) --> 1
    // txn1 commit
    // txn0 update (1, 10, tableJ, update_val2) --> update successfully
    // txn0 read (0, tableI) --> 1
    // txn0 read (1, tableI) --> 1
    // txn0 read (2, tableI) --> 1
    // txn0 read (1, tableJ) --> 1
    // txn0 read (1, tableJ, read_val2) --> 10
    // txn0 commit
    // txn2 update (1, 11, tableJ) --> update resctrict, fail
    // txn2 commit
    // txn3 read (0, tableI) --> 1
    // txn3 read (1, tableI) --> 1
    // txn3 read (2, tableI) --> 1
    // txn3 read (1, tableJ) --> 1
    // txn3 read (1, tableJ, read_val2) --> 10
    // txn3 commit
    TransactionScheduler scheduler(4, table_J, &txn_manager);
    scheduler.Txn(1).Read(0, table_I);
    scheduler.Txn(1).Read(1, table_I);
    scheduler.Txn(1).Read(2, table_I);
    scheduler.Txn(1).Read(1, table_J);
    scheduler.Txn(1).Read(1, table_J, true);
    scheduler.Txn(1).Commit();

    scheduler.Txn(0).Update(1, 10, table_J, true);
    scheduler.Txn(0).Read(0, table_I);
    scheduler.Txn(0).Read(1, table_I);
    scheduler.Txn(0).Read(2, table_I);
    scheduler.Txn(0).Read(1, table_J);
    scheduler.Txn(0).Read(1, table_J, true);
    scheduler.Txn(0).Commit();

    scheduler.Txn(2).Update(1, 11, table_J);
    scheduler.Txn(2).Commit();

    scheduler.Txn(3).Read(0, table_I);
    scheduler.Txn(3).Read(1, table_I);
    scheduler.Txn(3).Read(2, table_I);
    scheduler.Txn(3).Read(1, table_J);
    scheduler.Txn(3).Read(1, table_J, true);
    scheduler.Txn(3).Commit();
    scheduler.Run();

    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
    EXPECT_TRUE(RESULT_ABORTED == scheduler.schedules[2].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[3].txn_result);
    EXPECT_EQ(1, scheduler.schedules[0].results[0]);
    EXPECT_EQ(1, scheduler.schedules[0].results[1]);
    EXPECT_EQ(1, scheduler.schedules[0].results[2]);
    EXPECT_EQ(1, scheduler.schedules[0].results[3]);
    EXPECT_EQ(10, scheduler.schedules[0].results[4]);

    EXPECT_EQ(1, scheduler.schedules[1].results[0]);
    EXPECT_EQ(1, scheduler.schedules[1].results[1]);
    EXPECT_EQ(1, scheduler.schedules[1].results[2]);
    EXPECT_EQ(1, scheduler.schedules[1].results[3]);
    EXPECT_EQ(1, scheduler.schedules[1].results[4]);

    EXPECT_EQ(1, scheduler.schedules[3].results[0]);
    EXPECT_EQ(1, scheduler.schedules[3].results[1]);
    EXPECT_EQ(1, scheduler.schedules[3].results[2]);
    EXPECT_EQ(1, scheduler.schedules[3].results[3]);
    EXPECT_EQ(10, scheduler.schedules[3].results[4]);
  }
#endif

#ifdef FOREIGHN_KEY_CASCADE_UPDATE_TEST
  LOG_INFO("BEGIN FOREIGN KEY CASCADE_UPDATE TEST-----------------------------------");
  //     TABLE K -- src table          TABLE L -- sink table
  // int(primary)   int(ref L v1)        int(primary)    int   int
  //    0                 1               0               0     0
  //    1                 1               1               1     1
  //    2                 1               2               2     2
  //                                      .....
  //                                      9               9     9

  {
    auto table_K =
        TransactionTestsUtil::CreateTable(3, "tableK", 0, 6000, 6000, true, true, 6010, 1);
    auto table_L =
        TransactionTestsUtil::CreateTable(10, "tableL", 0, 6001, 6001, true, true, 6020, -1, true);

    // add the foreign key constraints for table_K
    auto foreign_key = new catalog::ForeignKey(
        6000, 6001,
        table_L->GetIndexIdWithColumnOffsets({1}),
        table_K->GetIndexIdWithColumnOffsets({1}),
        {"value"}, {1}, {"value"}, {1}, FOREIGNKEY_ACTION_CASCADE,
        FOREIGNKEY_ACTION_NOACTION, "THIS_IS_FOREIGN_CONSTRAINT");
    table_K->AddForeignKey(foreign_key);
    // Test4: delete tuple and cascading set null
    // constraint's restrict/noaction action
    // txn1 read (0, tableK) --> 1
    // txn1 read (1, tableK) --> 1
    // txn1 read (2, tableK) --> 1
    // txn1 read (1, tableL) --> 1
    // txn1 read (1, tableL, read_val2) --> 1
    // txn1 commit
    // txn0 update (1, 10, tableL, update_val2) --> update successfully
    // txn0 read (0, tableK) --> 1
    // txn0 read (1, tableK) --> 1
    // txn0 read (2, tableK) --> 1
    // txn0 read (1, tableL) --> 1
    // txn0 read (1, tableL, read_val2) --> 10
    // txn0 commit
    // txn2 update (1, 11, tableL) --> cascad update values in tableK
    // txn2 commit
    // txn3 read (0, tableK) --> 11
    // txn3 read (1, tableK) --> 11
    // txn3 read (2, tableK) --> 11
    // txn3 read (1, tableL) --> 11
    // txn3 read (1, tableL, read_val2) --> 10
    // txn3 commit
    TransactionScheduler scheduler(4, table_L, &txn_manager);
    scheduler.Txn(1).Read(0, table_K);
    scheduler.Txn(1).Read(1, table_K);
    scheduler.Txn(1).Read(2, table_K);
    scheduler.Txn(1).Read(1, table_L);
    scheduler.Txn(1).Read(1, table_L, true);
    scheduler.Txn(1).Commit();

    scheduler.Txn(0).Update(1, 10, table_L, true);
    scheduler.Txn(0).Read(0, table_K);
    scheduler.Txn(0).Read(1, table_K);
    scheduler.Txn(0).Read(2, table_K);
    scheduler.Txn(0).Read(1, table_L);
    scheduler.Txn(0).Read(1, table_L, true);
    scheduler.Txn(0).Commit();

    scheduler.Txn(2).Update(1, 11, table_L);
    scheduler.Txn(2).Commit();

    scheduler.Txn(3).Read(0, table_K);
    scheduler.Txn(3).Read(1, table_K);
    scheduler.Txn(3).Read(2, table_K);
    scheduler.Txn(3).Read(1, table_L);
    scheduler.Txn(3).Read(1, table_L, true);
    scheduler.Txn(3).Commit();
    scheduler.Run();

    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[2].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[3].txn_result);
    EXPECT_EQ(1, scheduler.schedules[0].results[0]);
    EXPECT_EQ(1, scheduler.schedules[0].results[1]);
    EXPECT_EQ(1, scheduler.schedules[0].results[2]);
    EXPECT_EQ(1, scheduler.schedules[0].results[3]);
    EXPECT_EQ(10, scheduler.schedules[0].results[4]);

    EXPECT_EQ(1, scheduler.schedules[1].results[0]);
    EXPECT_EQ(1, scheduler.schedules[1].results[1]);
    EXPECT_EQ(1, scheduler.schedules[1].results[2]);
    EXPECT_EQ(1, scheduler.schedules[1].results[3]);
    EXPECT_EQ(1, scheduler.schedules[1].results[4]);

    EXPECT_EQ(11, scheduler.schedules[3].results[0]);
    EXPECT_EQ(11, scheduler.schedules[3].results[1]);
    EXPECT_EQ(11, scheduler.schedules[3].results[2]);
    EXPECT_EQ(11, scheduler.schedules[3].results[3]);
    EXPECT_EQ(10, scheduler.schedules[3].results[4]);
  }
#endif

#ifdef FOREIGHN_KEY_SETNULL_UPDATE_TEST
  LOG_INFO("BEGIN FOREIGN KEY KEY_SETNULL_UPDATE TEST-----------------------------------");
  //     TABLE M -- src table          TABLE N -- sink table
  // int(primary)   int(ref N v1)        int(primary)    int   int
  //    0                 1               0               0     0
  //    1                 1               1               1     1
  //    2                 1               2               2     2
  //                                      .....
  //                                      9               9     9

  {
    auto table_M =
        TransactionTestsUtil::CreateTable(3, "tableM", 0, 7000, 7000, true, true, 7010, 1);
    auto table_N =
        TransactionTestsUtil::CreateTable(10, "tableN", 0, 7001, 7001, true, true, 7020, -1, true);

    // add the foreign key constraints for table_K
    auto foreign_key = new catalog::ForeignKey(
        7000, 7001,
        table_N->GetIndexIdWithColumnOffsets({1}),
        table_M->GetIndexIdWithColumnOffsets({1}),
        {"value"}, {1}, {"value"}, {1}, FOREIGNKEY_ACTION_SETNULL,
        FOREIGNKEY_ACTION_NOACTION, "THIS_IS_FOREIGN_CONSTRAINT");
    table_M->AddForeignKey(foreign_key);
    // Test4: delete tuple and cascading set null
    // constraint's restrict/noaction action
    // txn1 read (0, tableM) --> 1
    // txn1 read (1, tableM) --> 1
    // txn1 read (2, tableM) --> 1
    // txn1 read (1, tableN) --> 1
    // txn1 read (1, tableN, read_val2) --> 1
    // txn1 commit
    // txn0 update (1, 10, tableN, update_val2) --> update successfully
    // txn0 read (0, tableM) --> 1
    // txn0 read (1, tableM) --> 1
    // txn0 read (2, tableM) --> 1
    // txn0 read (1, tableN) --> 1
    // txn0 read (1, tableN, read_val2) --> 10
    // txn0 commit
    // txn2 update (1, 11, tableL) --> cascad set values as null in tableM
    // txn2 commit
    // txn3 read (0, tableM) --> null
    // txn3 read (1, tableM) --> null
    // txn3 read (2, tableM) --> null
    // txn3 read (1, tableN) --> 11
    // txn3 read (1, tableN, read_val2) --> 10
    // txn3 commit
    TransactionScheduler scheduler(4, table_N, &txn_manager);
    scheduler.Txn(1).Read(0, table_M);
    scheduler.Txn(1).Read(1, table_M);
    scheduler.Txn(1).Read(2, table_M);
    scheduler.Txn(1).Read(1, table_N);
    scheduler.Txn(1).Read(1, table_N, true);
    scheduler.Txn(1).Commit();

    scheduler.Txn(0).Update(1, 10, table_N, true);
    scheduler.Txn(0).Read(0, table_M);
    scheduler.Txn(0).Read(1, table_M);
    scheduler.Txn(0).Read(2, table_M);
    scheduler.Txn(0).Read(1, table_N);
    scheduler.Txn(0).Read(1, table_N, true);
    scheduler.Txn(0).Commit();

    scheduler.Txn(2).Update(1, 11, table_N);
    scheduler.Txn(2).Commit();

    scheduler.Txn(3).Read(0, table_M);
    scheduler.Txn(3).Read(1, table_M);
    scheduler.Txn(3).Read(2, table_M);
    scheduler.Txn(3).Read(1, table_N);
    scheduler.Txn(3).Read(1, table_N, true);
    scheduler.Txn(3).Commit();
    scheduler.Run();

    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[2].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[3].txn_result);
    EXPECT_EQ(1, scheduler.schedules[0].results[0]);
    EXPECT_EQ(1, scheduler.schedules[0].results[1]);
    EXPECT_EQ(1, scheduler.schedules[0].results[2]);
    EXPECT_EQ(1, scheduler.schedules[0].results[3]);
    EXPECT_EQ(10, scheduler.schedules[0].results[4]);

    EXPECT_EQ(1, scheduler.schedules[1].results[0]);
    EXPECT_EQ(1, scheduler.schedules[1].results[1]);
    EXPECT_EQ(1, scheduler.schedules[1].results[2]);
    EXPECT_EQ(1, scheduler.schedules[1].results[3]);
    EXPECT_EQ(1, scheduler.schedules[1].results[4]);

    EXPECT_EQ(INT32_NULL, scheduler.schedules[3].results[0]);
    EXPECT_EQ(INT32_NULL, scheduler.schedules[3].results[1]);
    EXPECT_EQ(INT32_NULL, scheduler.schedules[3].results[2]);
    EXPECT_EQ(11, scheduler.schedules[3].results[3]);
    EXPECT_EQ(10, scheduler.schedules[3].results[4]);
  }
#endif

  // remember to drop this database from the manager, this will also indirectly delete all tables in this database
  manager.DropDatabaseWithOid(current_db_oid);
}


#ifdef DROPSETNOTNULL_TEST
        TEST_F(ConstraintsTests, DROPSETNOTNULLTest) {

            std::unique_ptr<storage::DataTable> data_table(
                    ConstraintsTestsUtil::CreateAndPopulateTable());

            auto &txn_manager1 = concurrency::TransactionManagerFactory::GetInstance();

            // begin this transaction
            auto txn1 = txn_manager1.BeginTransaction();

            // Test1: insert a tuple with column 1 = null
            // should fail
            bool hasException = false;
            try {
                ConstraintsTestsUtil::ExecuteInsert(
                        txn1, data_table.get(),
                        ValueFactory::GetIntegerValue(
                                ConstraintsTestsUtil::PopulatedValue(15, 0)),
                        ValueFactory::GetNullValue(),
                        ValueFactory::GetIntegerValue(
                                ConstraintsTestsUtil::PopulatedValue(15, 2)),
                        ValueFactory::GetStringValue(
                                std::to_string(ConstraintsTestsUtil::PopulatedValue(15, 3))));

            } catch (ConstraintException e) {
                hasException = true;
            }
            EXPECT_TRUE(hasException);
            txn_manager1.CommitTransaction();

            // DROP column2 NOT NULL
            data_table->GetSchema()->DropNotNull(
                    catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,"COL_B"));

            // Test2: insert a tuple with column 2 = null
            // should success
            auto& txn_manager2 = concurrency::TransactionManagerFactory::GetInstance();
            auto txn2 = txn_manager2.BeginTransaction();
            hasException = false;
            try {
                ConstraintsTestsUtil::ExecuteInsert(
                        txn2, data_table.get(),
                        ValueFactory::GetIntegerValue(
                                ConstraintsTestsUtil::PopulatedValue(15, 0)),
                        ValueFactory::GetNullValue(),
                        ValueFactory::GetIntegerValue(
                                ConstraintsTestsUtil::PopulatedValue(15, 2)),
                        ValueFactory::GetStringValue(
                                std::to_string(ConstraintsTestsUtil::PopulatedValue(15, 3))));

            } catch (ConstraintException e) {
                hasException = true;
            }
            EXPECT_TRUE(!hasException);
            txn_manager2.CommitTransaction();

            // Set not null on column2
            data_table->GetSchema()->SetNotNull(
                    catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,"COL_B"));

            // Test 3 insert a tuple with column 2 = null
            // should succeed
            auto& txn_manager3 = concurrency::TransactionManagerFactory::GetInstance();
            auto txn3 = txn_manager3.BeginTransaction();

            hasException = false;
            try {
                ConstraintsTestsUtil::ExecuteInsert(
                        txn3, data_table.get(),
                        ValueFactory::GetIntegerValue(
                                ConstraintsTestsUtil::PopulatedValue(16, 0)),
                        ValueFactory::GetNullValue(),
                        ValueFactory::GetIntegerValue(
                                ConstraintsTestsUtil::PopulatedValue(16, 2)),
                        ValueFactory::GetStringValue(
                                std::to_string(ConstraintsTestsUtil::PopulatedValue(16, 3))));

            } catch (ConstraintException e) {
                hasException = true;
            }
            EXPECT_TRUE(hasException);
            // commit this transactio
            txn_manager3.CommitTransaction();
        }
#endif

#ifdef SETUNIQUE_TEST
TEST_F(ConstraintsTests, SetUniqueTest) {
    
    storage::DataTable * data_table =
        TransactionTestsUtil::CreateTable(2, "test_table", 0, 1000, 1000, false, false);
    auto &manager = catalog::Manager::GetInstance();
    oid_t current_db_oid = bridge::Bridge::GetCurrentDatabaseOid();
    storage::Database* newdb = new storage::Database(current_db_oid);
    manager.AddDatabase(newdb);
    
    newdb->AddTable(data_table);
    
    // add an unique constraint to column 2
    std::vector<std::string> column_name = {"value"};
    bridge::IndexInfo my_index_info("value_unique", data_table->GetIndexCount(), "test_table",
               INDEX_TYPE_BTREE,  INDEX_CONSTRAINT_TYPE_UNIQUE,
                true, column_name);
    peloton::bridge::DDLIndex::CreateIndex(my_index_info);
    auto &txn_manager1 = concurrency::TransactionManagerFactory::GetInstance();
            
    // Test1: insert a tuple with duplicated column 2
    // should fail
    TransactionScheduler scheduler(2, data_table, &txn_manager1);
    scheduler.Txn(0).Insert(2, 1);
    scheduler.Txn(1).Insert(3, 1);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();
    scheduler.Run();
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_ABORTED == scheduler.schedules[1].txn_result);
    manager.DropDatabaseWithOid(current_db_oid);

}
#endif

#ifdef DROPUNIQUE_TEST
TEST_F(ConstraintsTests, DropUniqueTest){

    storage::DataTable * data_table =
        TransactionTestsUtil::CreateTable(2, "test_table", 0, 1000, 1000, false, false);
    auto &manager = catalog::Manager::GetInstance();
    oid_t current_db_oid = bridge::Bridge::GetCurrentDatabaseOid();
    auto newdb = new storage::Database(current_db_oid);
    manager.AddDatabase(newdb);
    
    newdb->AddTable(data_table);
    
    // add an unique constraint to column 2
    std::vector<std::string> column_name = {"value"};
    bridge::IndexInfo my_index_info("value_unique", data_table->GetIndexCount(), "test_table",
               INDEX_TYPE_BTREE,  INDEX_CONSTRAINT_TYPE_UNIQUE,
                true, column_name);
    peloton::bridge::DDLIndex::CreateIndex(my_index_info);
    auto &txn_manager1 = concurrency::TransactionManagerFactory::GetInstance();
    char const* name = "value_unique";
    oid_t offset =  data_table->GetSchema()->DropConstraint(name); 
    data_table->DropIndexWithOid(offset);     
    // Test1: insert a tuple with duplicated column 2
    // should fail
    TransactionScheduler scheduler(2, data_table, &txn_manager1);
    scheduler.Txn(0).Insert(2, 1);
    scheduler.Txn(1).Insert(3, 1);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();
    scheduler.Run();
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
            
}
#endif


}  // End test namespace
}  // End peloton namespace

