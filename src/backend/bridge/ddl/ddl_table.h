//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ddl_table.h
//
// Identification: src/backend/bridge/ddl/ddl_table.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/catalog/schema.h"
#include "backend/catalog/foreign_key.h"
#include "backend/bridge/ddl/ddl_index.h"
#include "backend/storage/data_table.h"

#include "postgres.h"
#include "c.h"
#include "nodes/parsenodes.h"
#include "postmaster/peloton.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL TABLE
//===--------------------------------------------------------------------===//

class DDLTable {
 public:
  DDLTable(const DDLTable &) = delete;
  DDLTable &operator=(const DDLTable &) = delete;
  DDLTable(DDLTable &&) = delete;
  DDLTable &operator=(DDLTable &&) = delete;

  static bool ExecCreateStmt(Node *parsetree,
                             std::vector<Node *> &parsetree_stack);

  static bool ExecAlterTableStmt(Node *parsetree,
                                 std::vector<Node *> &parsetree_stack);

  static bool ExecDropStmt(Node *parsertree);

  static bool CreateTableCheck(Oid relation_oid, std::string table_name,
                          std::vector<catalog::Column> column_infos,
                          catalog::Schema *schema = NULL,
                          CreateStmt *Cstmt = NULL);

  static bool CreateTable(Oid relation_oid, std::string table_name,
                          std::vector<catalog::Column> column_infos,
                          catalog::Schema *schema = NULL);

  static bool AlterTable(Oid relation_oid, AlterTableStmt *Astmt);

  static bool DropTable(Oid table_oid);

  // Set reference tables to the table based on given relation oid
  static bool SetReferenceTables(std::vector<catalog::ForeignKey*> &foreign_keys,
                                 oid_t relation_oid);

 private:

  // Functions for alter table statements
  static bool DropNotNull(Oid relation_oid, char *connname);
  static bool SetNotNull(Oid relation_oid, char *conname);
  static bool CheckNullExist(storage::DataTable* targetTable, std::string column_name);
  // Add foreign key constraint
  static bool AddConstraint(Oid relation_oid, Constraint *constraint, char* name);
  // Dynamically add unique and primary constraints
  static bool AddIndex(IndexStmt * Istmt);
  // Dynamically drop constraint
  static bool DropConstraint(Oid relation_oid, char* conname);
};

}  // namespace bridge
}  // namespace peloton
