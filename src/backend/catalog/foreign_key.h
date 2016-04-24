//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// foreign_key.h
//
// Identification: src/backend/catalog/foreign_key.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>
#include <iostream>

#include "backend/common/types.h"
#include "backend/expression/comparison_expression.h"

namespace peloton {

namespace storage{
class DataTable;
class Tuple;
}

namespace index{
class Index;
}

namespace executor {
class ExecutorContext;
}


namespace catalog {

//===--------------------------------------------------------------------===//
// Foreign Key Class
//===--------------------------------------------------------------------===//

// Stores info about foreign key constraints, like the sink table id etc.
class ForeignKey {
 public:
  ForeignKey(oid_t src_table_id, oid_t sink_table_id,
             oid_t pk_index_id, oid_t fk_index_id,
             std::vector<std::string> pk_column_names,
             std::vector<oid_t> pk_column_offsets,
             std::vector<std::string> fk_column_names,
             std::vector<oid_t> fk_column_offsets,
             ForeignKeyActionType fk_update_action,
             ForeignKeyActionType fk_delete_action, std::string constraint_name)

      : src_table_id(src_table_id),
        sink_table_id(sink_table_id),
        pk_index_id(pk_index_id),
        fk_index_id(fk_index_id),
        pk_column_names(pk_column_names),
        pk_column_offsets(pk_column_offsets),
        fk_column_names(fk_column_names),
        fk_column_offsets(fk_column_offsets),
        fk_update_action(fk_update_action),
        fk_delete_action(fk_delete_action),
        fk_name(constraint_name) {}

  oid_t GetSrcTableOid() const { return src_table_id; }
  oid_t GetSinkTableOid() const { return sink_table_id; }
  oid_t GetSrcIndexOid() const { return fk_index_id; }
  oid_t GetSinkIndexOid() const { return pk_index_id; }

  std::vector<std::string> GetPKColumnNames() const { return pk_column_names; }
  std::vector<oid_t> GetPKColumnOffsets() const { return pk_column_offsets; }
  std::vector<std::string> GetFKColumnNames() const { return fk_column_names; }
  std::vector<oid_t> GetFKColumnOffsets() const { return fk_column_offsets; }

  ForeignKeyActionType GetUpdateAction() const { return fk_update_action; }

  ForeignKeyActionType GetDeleteAction() const { return fk_delete_action; }

  std::string &GetConstraintName() { return fk_name; }

 public:
  bool CheckDeleteConstraints(executor::ExecutorContext *executor_context,
                              std::vector<storage::Tuple>& tuples);

  bool IsTupleInSinkTable(storage::DataTable* sink_table, const storage::Tuple* tuple);

 private:
  expression::ComparisonExpression<expression::CmpEq> *MakePredicate(
      storage::Tuple& cur_tuple);

  bool IsDeletedTupleReferencedBySourceTable(storage::DataTable* source_table,
                                             index::Index* fk_index,
                                             storage::Tuple& cur_tuple);

  bool DeleteReferencingTupleOnCascading(executor::ExecutorContext *executor_context,
                                         storage::DataTable* source_table,
                                         storage::Tuple& cur_tuple);

  bool SetNullReferencingTupleOnCascading(executor::ExecutorContext *executor_context,
                                          storage::DataTable* source_table,
                                          storage::Tuple& cur_tuple,
                                          std::vector<oid_t>& direct_map_column_offsets);
 private:
  oid_t src_table_id = INVALID_OID;
  oid_t sink_table_id = INVALID_OID;
  oid_t pk_index_id = INVALID_OID;
  oid_t fk_index_id = INVALID_OID;

  // Columns in the reference table (sink)
  std::vector<std::string> pk_column_names;
  std::vector<oid_t> pk_column_offsets;

  // Columns in the current table (source)
  // Can be a single column or multiple columns depending on the constraint
  std::vector<std::string> fk_column_names;
  std::vector<oid_t> fk_column_offsets;

  // What to do when foreign key is updated or deleted ?
  ForeignKeyActionType fk_update_action;
  ForeignKeyActionType fk_delete_action;

  std::string fk_name;
};

}  // End catalog namespace
}  // End peloton namespace
