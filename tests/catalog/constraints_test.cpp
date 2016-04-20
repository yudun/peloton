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

#include "catalog/constraints_tests_util.h"
#include "concurrency/transaction_tests_util.h"

#define NOTNULL_TEST
#define PRIMARY_UNIQUEKEY_TEST
#define FOREIGHN_KEY_INSERT_TEST
#define FOREIGHN_KEY_RESTRICT_DELETE_TEST
#define FOREIGHN_KEY_CASCADE_DELETE_TEST
#define FOREIGHN_KEY_SETNULL_DELETE_TEST

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
    // Test2: update a tuple to be a illegal primary key
    // txn1: update (1, 1) -> (1,11) -- success
    // txn0: update (0, 0) -> (0,1) -- fail
    // txn1 commit
    // txn0 commit
    TransactionScheduler scheduler(2, data_table.get(), &txn_manager);
    scheduler.Txn(1).Update(1, 11);
    scheduler.Txn(0).Update(0, 1);
    scheduler.Txn(1).Commit();
    scheduler.Txn(0).Commit();

    scheduler.Run();

    EXPECT_TRUE(RESULT_ABORTED == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
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
  // First, initial 2 tables like following
  //     TABLE A -- src table          TABLE B -- sink table
  // int(primary, ref B)  int            int(primary)  int
  //    0                 0               0             0
  //    1                 0               1             0
  //    2                 0               2             0
  //                                      .....
  //                                      9             0

  // create new db
  auto &manager = catalog::Manager::GetInstance();
  oid_t current_db_oid = bridge::Bridge::GetCurrentDatabaseOid();
  auto newdb = new storage::Database(current_db_oid);
  manager.AddDatabase(newdb);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

#ifdef FOREIGHN_KEY_INSERT_TEST
  LOG_INFO("BEGIN FOREIGN KEY INSERT TEST-----------------------------------");
  {
    auto table_A =
        TransactionTestsUtil::CreateTable(3, "tableA", 0, 1000, 1000, true);
    auto table_B =
        TransactionTestsUtil::CreateTable(10, "tableB", 0, 1001, 1001, true);

    // add the foreign key constraints for table_A
    auto foreign_key = new catalog::ForeignKey(
        1000, 1001,
        table_B->GetIndexIdWithColumnOffsets({0}),
        table_A->GetIndexIdWithColumnOffsets({0}),
        {"id"}, {0}, {"id"}, {0}, FOREIGNKEY_ACTION_NOACTION,
        FOREIGNKEY_ACTION_NOACTION, "THIS_IS_FOREIGN_CONSTRAINT");
    table_A->AddForeignKey(foreign_key);


    // Test1: insert 2 tuple, one of which doesn't follow foreign key constraint
    // txn0 insert (10,10) --> fail
    // txn1 insert (9,10) --> success
    // txn0 commit
    // txn1 commit
    TransactionScheduler scheduler(2, table_A, &txn_manager);
    scheduler.Txn(0).Insert(10, 10);
    scheduler.Txn(1).Insert(9, 10);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Run();

    EXPECT_TRUE(RESULT_ABORTED == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
  }
#endif

#ifdef FOREIGHN_KEY_RESTRICT_DELETE_TEST
  LOG_INFO("BEGIN FOREIGN KEY RESTRICT_DELETE TEST-----------------------------------");
  {
    auto table_C =
        TransactionTestsUtil::CreateTable(3, "tableC", 0, 2000, 2000, true);
    auto table_D =
        TransactionTestsUtil::CreateTable(10, "tableD", 0, 2001, 2001, true);

    // add the foreign key constraints for table_C
    auto foreign_key = new catalog::ForeignKey(
        2000, 2001,
        table_D->GetIndexIdWithColumnOffsets({0}),
        table_C->GetIndexIdWithColumnOffsets({0}),
        {"id"}, {0}, {"id"}, {0}, FOREIGNKEY_ACTION_NOACTION,
        FOREIGNKEY_ACTION_NOACTION, "THIS_IS_FOREIGN_CONSTRAINT");
    table_C->AddForeignKey(foreign_key);
    // Test2: insert 2 tuple, one of which doesn't follow foreign key
    // constraint's restrict/noaction action
    // txn0 delete (9, tableD) --> success
    // txn1 delete (2, tableD) --> fail
    // txn0 commit
    // txn1 commit
    // txn2 read (2, tableC) --> still can read 0
    // txn2 read (2, tableD) --> still can read 0
    // txn2 read (9, tableD) --> can't read 9
    // txn2 commit
    TransactionScheduler scheduler(3, table_D, &txn_manager);
    scheduler.Txn(0).Delete(9, table_D);
    scheduler.Txn(1).Delete(2, table_D);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Txn(2).Read(2, table_C);
    scheduler.Txn(2).Read(2, table_D);
    scheduler.Txn(2).Read(9, table_D);
    scheduler.Txn(2).Commit();

    scheduler.Run();

    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_ABORTED == scheduler.schedules[1].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[2].txn_result);
    EXPECT_EQ(0, scheduler.schedules[2].results[0]);
    EXPECT_EQ(0, scheduler.schedules[2].results[1]);
    EXPECT_EQ(-1, scheduler.schedules[2].results[2]);
  }
#endif

#ifdef FOREIGHN_KEY_CASCADE_DELETE_TEST
  LOG_INFO("BEGIN FOREIGN KEY CASCADE_DELETE TEST-----------------------------------");
  {
    auto table_E =
        TransactionTestsUtil::CreateTable(3, "tableE", 0, 3000, 3000, true);
    auto table_F =
        TransactionTestsUtil::CreateTable(10, "tableF", 0, 3001, 3001, true);

    // add the foreign key constraints for table_E
    auto foreign_key = new catalog::ForeignKey(
        3000, 3001,
        table_F->GetIndexIdWithColumnOffsets({0}),
        table_E->GetIndexIdWithColumnOffsets({0}),
        {"id"}, {0}, {"id"}, {0}, FOREIGNKEY_ACTION_NOACTION,
        FOREIGNKEY_ACTION_CASCADE, "THIS_IS_FOREIGN_CONSTRAINT");
    table_E->AddForeignKey(foreign_key);
    // Test3: cascading delete tuples
    // constraint's restrict/noaction action
    // txn1 read (1, tableE) --> 0
    // txn1 read (1, tableF) --> 0
    // txn0 delete (1, tableF) --> cascade delete, 1 in tableE will also be deleted
    // txn0 commit
    // txn1 commit
    // txn2 read (1, tableE) --> empty
    // txn2 read (1, tableF) --> empty
    // txn2 commit
    TransactionScheduler scheduler(3, table_F, &txn_manager);
    scheduler.Txn(1).Read(1, table_E);
    scheduler.Txn(1).Read(1, table_F);
    scheduler.Txn(0).Delete(1, table_F);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Txn(2).Read(1, table_E);
    scheduler.Txn(2).Read(1, table_F);
    scheduler.Txn(2).Commit();

    scheduler.Run();

    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[2].txn_result);
    EXPECT_EQ(0, scheduler.schedules[1].results[0]);
    EXPECT_EQ(0, scheduler.schedules[1].results[1]);
    EXPECT_EQ(-1, scheduler.schedules[2].results[0]);
    EXPECT_EQ(-1, scheduler.schedules[2].results[1]);
  }
#endif

#ifdef FOREIGHN_KEY_SETNULL_DELETE_TEST
  LOG_INFO("BEGIN FOREIGN KEY SETNULL_DELETE TEST-----------------------------------");
  {
    auto table_G =
        TransactionTestsUtil::CreateTable(3, "tableG", 0, 4000, 4000, true, true, 4010);
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
    // txn1 read (0, tableG) --> 0
    // txn2 read (1, tableG) --> 0
    // txn2 read (2, tableG) --> 0
    // txn1 read (0, tableH) --> 0
    // txn0 delete (0, tableH) --> cascade delete, 1 in tableG will also be deleted
    // txn0 commit
    // txn1 commit
    // txn2 read (0, tableG) --> NULL
    // txn2 read (1, tableG) --> NULL
    // txn2 read (2, tableG) --> NULL
    // txn2 read (0, tableH) --> empty
    // txn2 commit
    TransactionScheduler scheduler(3, table_H, &txn_manager);
    scheduler.Txn(1).Read(0, table_G);
    scheduler.Txn(1).Read(1, table_G);
    scheduler.Txn(1).Read(2, table_G);
    scheduler.Txn(1).Read(0, table_H);
    scheduler.Txn(0).Delete(0, table_H);
    scheduler.Txn(0).Commit();
    scheduler.Txn(1).Commit();

    scheduler.Txn(2).Read(0, table_G);
    scheduler.Txn(2).Read(1, table_G);
    scheduler.Txn(2).Read(2, table_G);
    scheduler.Txn(2).Read(0, table_H);
    scheduler.Txn(2).Commit();

    scheduler.Run();

    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[0].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[1].txn_result);
    EXPECT_TRUE(RESULT_SUCCESS == scheduler.schedules[2].txn_result);
    EXPECT_EQ(0, scheduler.schedules[1].results[0]);
    EXPECT_EQ(0, scheduler.schedules[1].results[1]);
    EXPECT_EQ(0, scheduler.schedules[1].results[2]);
    EXPECT_EQ(0, scheduler.schedules[1].results[3]);
    EXPECT_EQ(INT32_NULL, scheduler.schedules[2].results[0]);
    EXPECT_EQ(INT32_NULL, scheduler.schedules[2].results[1]);
    EXPECT_EQ(INT32_NULL, scheduler.schedules[2].results[2]);
    EXPECT_EQ(-1, scheduler.schedules[2].results[3]);
  }
#endif


  // remember to drop this database from the manager, this will also indirectly delete all tables in this database
  manager.DropDatabaseWithOid(current_db_oid);
}


}  // End test namespace
}  // End peloton namespace
