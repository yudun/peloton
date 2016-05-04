//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ddl_table.cpp
//
// Identification: src/backend/bridge/ddl/ddl_table.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <iostream>
#include <vector>
#include <thread>

#include "backend/bridge/ddl/ddl.h"
#include "backend/bridge/ddl/ddl_table.h"
#include "backend/bridge/ddl/ddl_database.h"
#include "backend/bridge/ddl/ddl_utils.h"
#include "backend/common/logger.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/database.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/executor/executors.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "commands/dbcommands.h"
#include "nodes/pg_list.h"
#include "parser/parse_utilcmd.h"
namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Table DDL
//===--------------------------------------------------------------------===//

/**
 * @brief Execute the create stmt.
 * @param the parse tree
 * @param query string
 * @return true if we handled it correctly, false otherwise
 */

bool DDLTable::ExecCreateStmt(Node *parsetree,
                     std::vector<Node *> &parsetree_stack) {
        List *stmts = ((CreateStmt *)parsetree)->stmts;

        /* ... and do it */
        ListCell *l;
        foreach (l, stmts) {
          Node *stmt = (Node *)lfirst(l);
          if (IsA(stmt, CreateStmt)) {
            CreateStmt *Cstmt = (CreateStmt *)stmt;
            List *schema = (List *)(Cstmt->tableElts);

            // Relation name and oid
            char *relation_name = Cstmt->relation->relname;
            Oid relation_oid = ((CreateStmt *)parsetree)->relation_id;

            assert(relation_oid);

            std::vector<catalog::Column> column_infos;

            //===--------------------------------------------------------------------===//
            // CreateStmt --> ColumnInfo --> CreateTable
            //===--------------------------------------------------------------------===//
            if (schema != NULL) {
              DDLUtils::ParsingCreateStmt(Cstmt, column_infos);

              DDLTable::CreateTable(relation_oid, relation_name, column_infos);
            }
          }
        }

  //===--------------------------------------------------------------------===//
  // Rerun query
  //===--------------------------------------------------------------------===//
  {
    std::lock_guard<std::mutex> lock(parsetree_stack_mutex);
    for (auto parsetree : parsetree_stack) {
      DDL::ProcessUtility(parsetree);
      pfree(parsetree);
    }
    parsetree_stack.clear();
  }

  return true;
}
/**
 * @brief Execute the alter stmt.
 * @param the parsetree
 * @param the parsetree_stack store parsetree if the table is not created yet
 * @return true if we handled it correctly, false otherwise
 */
bool DDLTable::ExecAlterTableStmt(Node *parsetree,
                                  std::vector<Node *> &parsetree_stack) {
  AlterTableStmt *atstmt = (AlterTableStmt *)parsetree;

  Oid relation_oid = atstmt->relation_id;
  List *stmts = atstmt->stmts;

  // If table has not been created yet, store it into the parsetree stack
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db =
      manager.GetDatabaseWithOid(Bridge::GetCurrentDatabaseOid());
  if (nullptr == db->GetTableWithOid(relation_oid)) {
    {
      std::lock_guard<std::mutex> lock(parsetree_stack_mutex);
      parsetree_stack.push_back(parsetree);
    }
    return true;
  }

  ListCell *l;
  foreach (l, stmts) {
    Node *stmt = (Node *)lfirst(l);
    if (IsA(stmt, AlterTableStmt)) {
      DDLTable::AlterTable(relation_oid, (AlterTableStmt *)stmt);
    }
  }
  return true;
}

/**
 * @brief Execute the drop stmt.
 * @param the parse tree
 * @return true if we handled it correctly, false otherwise
 */
bool DDLTable::ExecDropStmt(Node *parsetree) {
  DropStmt *drop = (DropStmt *)parsetree;
  // TODO drop->behavior;   /* RESTRICT or CASCADE behavior */

  ListCell *cell;
  foreach (cell, drop->objects) {
    List *names = ((List *)lfirst(cell));

    switch (drop->removeType) {
      case OBJECT_TABLE: {
        char *table_name = strVal(linitial(names));

        auto &manager = catalog::Manager::GetInstance();
        storage::Database *db =
            manager.GetDatabaseWithOid(Bridge::GetCurrentDatabaseOid());
        auto table = db->GetTableWithName(table_name);

        // skip if no table
        if (table == nullptr) break;

        Oid table_oid = table->GetOid();
        DDLTable::DropTable(table_oid);
      } break;

      default: {
        LOG_WARN("Unsupported drop object %d ", drop->removeType);
      } break;
    }
  }
  return true;
}

/**
 * @brief Create table.
 * @param table_name Table name
 * @param column_infos Information about the columns
 * @param schema Schema for the table
 * @return true if we created a table, false otherwise
 */
bool DDLTable::CreateTable(Oid relation_oid, std::string table_name,
                           std::vector<catalog::Column> column_infos,
                           catalog::Schema *schema) {
  assert(!table_name.empty());

  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  // if (database_oid == INVALID_OID || relation_oid == INVALID_OID) return
  // false;
  /* Oid and oit_t have different range */
  if (database_oid == InvalidOid || relation_oid == InvalidOid) return false;

  // Get db oid
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(database_oid);

  // Construct our schema from vector of ColumnInfo
  if (schema == NULL) schema = new catalog::Schema(column_infos);

  // Build a table from schema
  bool own_schema = true;
  bool adapt_table = true;
  storage::DataTable *table = storage::TableFactory::GetDataTable(
      database_oid, relation_oid, schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  if (table != nullptr) {
    LOG_INFO("Created table(%u)%s in database(%u) ", relation_oid,
             table_name.c_str(), database_oid);

    db->AddTable(table);
    return true;
  }
  return false;
}

/**
 * @brief AlterTable with given AlterTableStmt
 * @param relation_oid relation oid
 * @param Astmt AlterTableStmt
 * @return true if we alter the table successfully, false otherwise
 */
bool DDLTable::AlterTable(Oid relation_oid, AlterTableStmt *Astmt) {
  ListCell *lcmd;
  foreach (lcmd, Astmt->cmds) {
    AlterTableCmd *cmd = (AlterTableCmd *)lfirst(lcmd);
    switch (cmd->subtype) {
      // case AT_AddColumn:  /* add column */
      // case AT_DropColumn:  /* drop column */
      
      case AT_AddIndex:{
        IndexStmt *Istmt = (IndexStmt *)cmd->def;
         bool status = AddIndex(Istmt);
          if (status == false) {
             LOG_WARN("Failed to add an index");
          }
        break;
      }
      case AT_DropConstraint:{
        bool status = DropConstraint( relation_oid, cmd->name);
        if (status == false) {
           LOG_WARN("Failed to add constraint");
        }
        break;
      }
      case AT_AddConstraint: 
      {
         LOG_INFO("ADD CONSTRAIN");
        bool status = AddConstraint(relation_oid, (Constraint *)cmd->def, cmd->name);

        if (status == false) {
          LOG_WARN("Failed to add constraint");
        }
        break;
      }
      case AT_DropNotNull:
      {
          LOG_INFO("ALTER TABLE === DROP NOT NULL ");
          bool status = DropNotNull(relation_oid, cmd->name);
          if (status == false) {
             LOG_WARN("Failed to add constraint");
          }
          break;
      }
       case AT_SetNotNull:{
          bool status = DDLTable::SetNotNull(relation_oid, cmd->name);
	  if (status == false) {
          	LOG_WARN("Failed to add constraint");
          	return false;
          }
          break;
      }
      default:
        break;
    }
  }

  LOG_INFO("Altered the table (%u)", relation_oid);
  return true;
}

/**
 * @brief Drop table.
 * @param table_oid Table id.
 * @return true if we dropped the table, false otherwise
 */
// FIXME :: Dependencies btw indexes and tables
bool DDLTable::DropTable(Oid table_oid) {
  oid_t database_oid = Bridge::GetCurrentDatabaseOid();

  if (database_oid == InvalidOid || table_oid == InvalidOid) {
    LOG_WARN("Could not drop table :: db oid : %u table oid : %u",
             database_oid, table_oid);
    return false;
  }

  // Get db with current database oid
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(database_oid);

  db->DropTableWithOid(table_oid);

  LOG_INFO("Dropped table with oid : %u", table_oid);

  return true;
}

/**
 * @brief Add new constraint to the table
 * @param relation_oid relation oid
 * @param constraint constraint
 * @return true if we add the constraint, false otherwise
 */
bool DDLTable::AddConstraint(Oid relation_oid, Constraint *constraint, char* name) {
  ConstraintType contype = PostgresConstraintTypeToPelotonConstraintType(
      (PostgresConstraintType)constraint->contype);
  std::vector<catalog::ForeignKey*> foreign_keys;
  std::string conname;

  if (constraint->conname != NULL) {
    conname = constraint->conname;
  } else {
    conname = "";
  }

  switch (contype) {
    case CONSTRAINT_TYPE_FOREIGN: {
      oid_t database_oid = Bridge::GetCurrentDatabaseOid();
      assert(database_oid);

      auto &manager = catalog::Manager::GetInstance();
      storage::Database *db = manager.GetDatabaseWithOid(database_oid);

      // ForeignKey Table and PrimaryKey Table
      auto fk_table = manager.GetTableWithOid(database_oid, relation_oid);
      auto pk_table = db->GetTableWithName(constraint->pktable->relname);
      oid_t PrimaryKeyTableId = pk_table->GetOid();

      // Each table column names and offsets
      std::vector<std::string> pk_column_names;
      std::vector<std::string> fk_column_names;
      std::vector<oid_t> pk_column_offsets, fk_column_offsets;

      ListCell *column;
      if (constraint->pk_attrs != NULL && constraint->pk_attrs->length > 0) {
        foreach (column, constraint->pk_attrs) {
          char *attname = strVal(lfirst(column));
          pk_column_names.push_back(attname);
          oid_t offset = pk_table->GetSchema()->GetColumnOffsetByName(attname);
          pk_column_offsets.push_back(offset);
        }
      }

      if (constraint->fk_attrs != NULL && constraint->fk_attrs->length > 0) {
        foreach (column, constraint->fk_attrs) {
          char *attname = strVal(lfirst(column));
          fk_column_names.push_back(attname);
          oid_t offset = fk_table->GetSchema()->GetColumnOffsetByName(attname);
          fk_column_offsets.push_back(offset);
        }
      }

      LOG_INFO("srcid=%u sinkid=%u, pkidx=%u fkidx=%u, pkcol=%s pkoff=%u,"
                   "fkcol=%s fkoff=%u, up_type=%c, deltype=%c, %s",
               relation_oid, PrimaryKeyTableId,
               pk_table->GetIndexIdWithColumnOffsets(pk_column_offsets),
               fk_table->GetIndexIdWithColumnOffsets(pk_column_offsets),
               pk_column_names[0].c_str(), pk_column_offsets[0],
               fk_column_names[0].c_str(), fk_column_offsets[0],
               constraint->fk_upd_action,
               constraint->fk_del_action, conname.c_str());

      catalog::ForeignKey *foreign_key = new catalog::ForeignKey(
          relation_oid, PrimaryKeyTableId,
          pk_table->GetIndexIdWithColumnOffsets(pk_column_offsets),
          fk_table->GetIndexIdWithColumnOffsets(pk_column_offsets),
          pk_column_names, pk_column_offsets,
          fk_column_names, fk_column_offsets,
          CharToForeignKeyActionType(constraint->fk_upd_action),
          CharToForeignKeyActionType(constraint->fk_del_action), conname);

      foreign_keys.push_back(foreign_key);
      break;
    }
    case CONSTRAINT_TYPE_UNIQUE:{
      LOG_INFO("ADD CONSTRAIN UNIQUE, NAME = %s",name);
      break;

    }
    default:
      LOG_WARN("Unrecognized constraint type %d", (int)contype);
      break;
  }

  // FIXME :
  bool status = SetReferenceTables(foreign_keys, relation_oid);
  if (status == false) {
    LOG_WARN("Failed to set reference tables");
  }

  return true;
}

/**
 * @brief Drop Not Null Constraint on a table
 * @param Oid relation_oid
 * @param Constraint *constraint
 * @return true if drop not null successfully, false otherwise
 */
bool DDLTable::DropNotNull(Oid relation_oid, char *conname){

  oid_t database_oid = Bridge::GetCurrentDatabaseOid();
  assert(database_oid);
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(database_oid);
  storage::DataTable* targetTable = db->GetTableWithOid(relation_oid);
  catalog::Schema* targetSchema = targetTable->GetSchema();
  
  std::string column_name;
  if (conname != NULL) {
    column_name = std::string(conname);
  } else {
    column_name = "";
  }  
  catalog::Constraint tmp_constraint = catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,column_name);
  bool status = targetSchema->DropNotNull( tmp_constraint );
  return status;

}

/**
 * @brief Set Not Null Constraint on a table
 * @param Oid relation_oid
 * @param Constraint *constraint
 * @return true if set not null successfully, false otherwise
 */

bool DDLTable::SetNotNull(Oid relation_oid, char* conname){
   
    oid_t database_oid = Bridge::GetCurrentDatabaseOid();
    assert(database_oid);
    auto &manager = catalog::Manager::GetInstance();
    storage::Database *db = manager.GetDatabaseWithOid(database_oid);
    storage::DataTable* targetTable = db->GetTableWithOid(relation_oid);

    std::string constrain_name;
    if (conname != NULL) {
      constrain_name = std::string(conname);
    } else {
      constrain_name = "";
    }

    catalog::Schema* targetSchema = targetTable->GetSchema();
    catalog::Constraint tmp_constraint = catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
                                                           constrain_name);
    if( targetSchema->ExistConstrain(tmp_constraint) )
       return true;
    bool ExistNull = DDLTable::CheckNullExist(targetTable, constrain_name);
    if( ExistNull ){
       throw ConstraintException("NULL ALREADY EXISTED IN COLUMN "+constrain_name);
       return false;
    }
    bool status = targetSchema->SetNotNull( tmp_constraint );
    return status;
}

/**
* @brief Check if column contains NULL
* @param storage::DataTable* targetTable
* @param std::string column_name
* @return true if there is at least one NULL value, false otherwise
*/
bool DDLTable::CheckNullExist( storage::DataTable* targetTable, std::string column_name ){

    LOG_INFO("=== CHECK NULL EXIST FOR %s ===", column_name.c_str() );

  // prepare for seq scan
  catalog::Schema* targetSchema = targetTable->GetSchema();
  std::vector<oid_t> column_ids;
  oid_t column_count = targetSchema->GetColumnCount();
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++){
    std:: string current_name = targetSchema->GetColumn(column_itr).column_name;
    if( current_name.compare(column_name)==0 ){
      column_ids.push_back(column_itr);
      break;
    }
  }

  planner::SeqScanPlan seq_scan_node(targetTable, nullptr, column_ids);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
          new executor::ExecutorContext(txn));

  executor::SeqScanExecutor executor(&seq_scan_node, context.get());
  if(executor.Init() == false){
    LOG_WARN("ERROR INIT EXECUTOR");
  }

  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
  while ( executor.Execute() ) {
    result_tiles.emplace_back( executor.GetOutput());
  }

  auto result_iter = result_tiles.begin();
  for( ; result_iter != result_tiles.end(); result_iter++){
    size_t tuple_count = (*result_iter)->GetTupleCount();
    for(size_t tuple_iter = 0; tuple_iter < tuple_count; tuple_iter++ ){
      if((*result_iter)->GetValue(tuple_iter, 0).IsNull()){
        return true;
      }
    }
  }
  return false;
}

/**
 * @brief Add an index
 * @param Oid relation_oid
 * @param IndexStmt *Istmt, index statement
 * @return true if we successfull add this index, false otherwise
 */
bool DDLTable::AddIndex( IndexStmt *Istmt) {

  IndexInfo * idx = DDLIndex::ConstructIndexInfoByParsingIndexStmt(Istmt);
  LOG_INFO("add index id = %u", idx->GetOid());
  IndexInfo my_index_info(idx->GetIndexName(), idx->GetOid(),idx->GetTableName(),
                            idx->GetMethodType(),  INDEX_CONSTRAINT_TYPE_UNIQUE,
                                    Istmt->unique, idx->GetKeyColumnNames());
  bool status = DDLIndex::CreateIndex(my_index_info);
  LOG_INFO("add index success = %d", status);
  return status;
}

/**
 * @brief Drop a constraint 
 * @param Oid relation_oid
 * @param Oid relation_oid,  char* conname
 * @return true if we successfull drop this constraint, false otherwise
 */

bool DDLTable::DropConstraint(Oid relation_oid,  char* conname ){
   
  // TODO: only unique implemented
  oid_t database_oid = Bridge::GetCurrentDatabaseOid();
  assert(database_oid);
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(database_oid);
  storage::DataTable* targetTable = db->GetTableWithOid(relation_oid);
  catalog::Schema* targetSchema = targetTable->GetSchema();

  oid_t offset = targetSchema->DropConstraint( conname );
  LOG_INFO("unique index offset = %u", offset);
  if( offset >= targetTable->GetIndexCount() )
    return false;
  targetTable->DropIndexWithOid(offset);
  return true;

}

/**
 * @brief Set Reference Tables
 * @param reference table names
 * @param relation_oid relation oid
 * @return true if we set the reference tables, false otherwise
 */
bool DDLTable::SetReferenceTables(
    std::vector<catalog::ForeignKey*> &foreign_keys, oid_t relation_oid) {
  assert(relation_oid);
  oid_t database_oid = Bridge::GetCurrentDatabaseOid();
  assert(database_oid);

  storage::DataTable *current_table =
      (storage::DataTable *)catalog::Manager::GetInstance().GetTableWithOid(
          database_oid, relation_oid);

  for (auto foreign_key : foreign_keys) {
    current_table->AddForeignKey(foreign_key);
  }

  return true;
}

}  // namespace bridge
}  // namespace peloton

