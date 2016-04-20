//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_executor.h
//
// Identification: src/backend/executor/delete_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/executor/abstract_executor.h"
#include "backend/expression/container_tuple.h"

#include <vector>
#include "backend/storage/data_table.h"
#include "backend/expression/comparison_expression.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace executor {

class DeleteExecutor : public AbstractExecutor {
 public:
  DeleteExecutor(const DeleteExecutor &) = delete;
  DeleteExecutor &operator=(const DeleteExecutor &) = delete;
  DeleteExecutor(DeleteExecutor &&) = delete;
  DeleteExecutor &operator=(DeleteExecutor &&) = delete;

  DeleteExecutor(const planner::AbstractPlan *node,
                 ExecutorContext *executor_context);

  ~DeleteExecutor() {}

 protected:
  bool DInit();

  bool DExecute();

 private:
  storage::DataTable *target_table_ = nullptr;

  bool CheckDeleteForeiKeyConstraints(LogicalTile * source_tile);

  std::vector<storage::Tuple> GetBaseTupleListFromSourceTile(LogicalTile* source_tile);

  expression::ComparisonExpression<expression::CmpEq> *MakePredicate(
      storage::Tuple& cur_tuple,
      std::vector<oid_t>& column_offsets);

  bool IsDeletedTupleReferencedBySourceTable(storage::DataTable* source_table,
                                             index::Index* fk_index,
                                             storage::Tuple& cur_tuple,
                                             std::vector<oid_t>&& column_offsets);

  bool DeleteReferencingTupleOnCascading(storage::DataTable* table,
                                         storage::Tuple& cur_tuple,
                                         std::vector<oid_t>&& column_offsets);

  bool SetNullReferencingTupleOnCascading(storage::DataTable* table,
                                          storage::Tuple& cur_tuple,
                                         std::vector<oid_t>&& column_offsets,
                                         std::vector<oid_t>& direct_map_column_offsets);

};

}  // namespace executor
}  // namespace peloton
