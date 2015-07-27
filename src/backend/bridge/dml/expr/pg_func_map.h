/*-------------------------------------------------------------------------
 *
 * expr_func_table.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/dml/expr/expr_func_table.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <unordered_map>

namespace peloton {
namespace bridge {

/**
 * @brief Mapping PG Function Id to Peloton Expression Type.
 */
std::unordered_map<Oid, ExpressionType> pg_func_map({

    {63, EXPRESSION_TYPE_COMPARE_EQ},
    {65, EXPRESSION_TYPE_COMPARE_EQ},
    {67, EXPRESSION_TYPE_COMPARE_EQ},
    {158, EXPRESSION_TYPE_COMPARE_EQ},
    {159, EXPRESSION_TYPE_COMPARE_EQ},

    {84, EXPRESSION_TYPE_COMPARE_NE},
    {144, EXPRESSION_TYPE_COMPARE_NE},
    {145, EXPRESSION_TYPE_COMPARE_NE},
    {157, EXPRESSION_TYPE_COMPARE_NE},
    {164, EXPRESSION_TYPE_COMPARE_NE},
    {165, EXPRESSION_TYPE_COMPARE_NE},

    {56, EXPRESSION_TYPE_COMPARE_LT},
    {64, EXPRESSION_TYPE_COMPARE_LT},
    {66, EXPRESSION_TYPE_COMPARE_LT},
    {160, EXPRESSION_TYPE_COMPARE_LT},
    {161, EXPRESSION_TYPE_COMPARE_LT},
    {1246, EXPRESSION_TYPE_COMPARE_LT},

    {57, EXPRESSION_TYPE_COMPARE_GT},
    {73, EXPRESSION_TYPE_COMPARE_GT},
    {146, EXPRESSION_TYPE_COMPARE_GT},
    {147, EXPRESSION_TYPE_COMPARE_GT},
    {162, EXPRESSION_TYPE_COMPARE_GT},
    {163, EXPRESSION_TYPE_COMPARE_GT},

    {74, EXPRESSION_TYPE_COMPARE_GTE},
    {150, EXPRESSION_TYPE_COMPARE_GTE},
    {151, EXPRESSION_TYPE_COMPARE_GTE},
    {168, EXPRESSION_TYPE_COMPARE_GTE},
    {169, EXPRESSION_TYPE_COMPARE_GTE},
    {1692, EXPRESSION_TYPE_COMPARE_GTE},

    {72, EXPRESSION_TYPE_COMPARE_LTE},
    {148, EXPRESSION_TYPE_COMPARE_LTE},
    {149, EXPRESSION_TYPE_COMPARE_LTE},
    {166, EXPRESSION_TYPE_COMPARE_LTE},
    {167, EXPRESSION_TYPE_COMPARE_LTE},
    {1691, EXPRESSION_TYPE_COMPARE_LTE},

    {176, EXPRESSION_TYPE_OPERATOR_PLUS},
    {177, EXPRESSION_TYPE_OPERATOR_PLUS},
    {178, EXPRESSION_TYPE_OPERATOR_PLUS},
    {179, EXPRESSION_TYPE_OPERATOR_PLUS},

    {180, EXPRESSION_TYPE_OPERATOR_MINUS},
    {181, EXPRESSION_TYPE_OPERATOR_MINUS},
    {182, EXPRESSION_TYPE_OPERATOR_MINUS},
    {183, EXPRESSION_TYPE_OPERATOR_MINUS},

    {141, EXPRESSION_TYPE_OPERATOR_MULTIPLY},
    {152, EXPRESSION_TYPE_OPERATOR_MULTIPLY},
    {170, EXPRESSION_TYPE_OPERATOR_MULTIPLY},
    {171, EXPRESSION_TYPE_OPERATOR_MULTIPLY},

    {153, EXPRESSION_TYPE_OPERATOR_DIVIDE},
    {154, EXPRESSION_TYPE_OPERATOR_DIVIDE},
    {172, EXPRESSION_TYPE_OPERATOR_DIVIDE},
    {173, EXPRESSION_TYPE_OPERATOR_DIVIDE}

});

}  // namespace bridge
}  // namespace peloton
