/**
 * Copyright (c) 2021 OceanBase
 * OceanBase Database Proxy(ODP) is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_COMMON_EXPRESSION_OB_I_SQL_EXPRESSION_
#define OCEANBASE_COMMON_EXPRESSION_OB_I_SQL_EXPRESSION_

#include "common/ob_object.h"
#include "common/ob_row.h"
namespace oceanbase
{

namespace sql
{
class ObPhysicalPlanCtx;
class ObSQLSessionInfo;
class ObExecContext;
}
namespace share
{
class ObAutoincrementService;
}

namespace common
{
class ObNewRowIterator;
class ObIAllocator;
struct ObExprCtx
{
  ObExprCtx()
      : phy_plan_ctx_(NULL),
        my_session_(NULL),
        exec_ctx_(NULL),
        calc_buf_(NULL),
        subplan_iters_(NULL),
        cast_mode_(0),
        err_col_idx_(0),
        tz_offset_(0)
  {}
  ObExprCtx(sql::ObPhysicalPlanCtx *phy_plan_ctx,
            sql::ObSQLSessionInfo *my_session,
            sql::ObExecContext *exec_ctx,
            ObIAllocator *calc_buf,
            ObIArray<ObNewRowIterator*> *subplan_iters = NULL)
      : phy_plan_ctx_(phy_plan_ctx),
        my_session_(my_session),
        exec_ctx_(exec_ctx),
        calc_buf_(calc_buf),
        subplan_iters_(subplan_iters),
        cast_mode_(0),
        err_col_idx_(0),
        tz_offset_(0)

  {}
  sql::ObPhysicalPlanCtx *phy_plan_ctx_;
  sql::ObSQLSessionInfo *my_session_;
  sql::ObExecContext *exec_ctx_;
  ObIAllocator *calc_buf_;
  //When an expression involves sub-query calculation, sub_row_iters_ stores the row iterators iterated from all sub-queries in the expression
  ObIArray<ObNewRowIterator*> *subplan_iters_;
  uint64_t cast_mode_;
  int64_t err_col_idx_;
  int64_t tz_offset_;
};

struct ObExprTypeCtx
{
  ObExprTypeCtx()
      : my_session_(NULL)
  {}
  sql::ObSQLSessionInfo *my_session_;
};

class ObISqlExpression
{
public:

  virtual int calc(ObExprCtx &expr_ctx, const common::ObNewRow &row, common::ObObj &result) const = 0;
  virtual int calc(ObExprCtx &expr_ctx, const common::ObNewRow &row1, const common::ObNewRow &row2,
                   common::ObObj &result) const = 0;
  /// Print expression
  virtual int64_t to_string(char *buf, const int64_t buf_len) const = 0;
};
}
}
#endif /* OCEANBASE_COMMON_EXPRESSION_OB_I_SQL_EXPRESSION_ */

