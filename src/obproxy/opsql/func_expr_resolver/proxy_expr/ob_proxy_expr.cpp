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

#define USING_LOG_PREFIX PROXY

#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr.h"
#include "common/ob_obj_cast.h"
#include "common/ob_obj_type.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "lib/time/ob_time_utility.h"
#include "proxy/route/obproxy_expr_calculator.h"
#include "utils/ob_proxy_utils.h"
#include "common/expression/ob_expr_regexp_context.h"
#include "common/expression/ob_expr_util.h"
#include "lib/utility/utility.h"
#include "lib/number/ob_number_format_models.h"
#include <time.h>

namespace oceanbase
{
using namespace common;
namespace obproxy
{
using namespace obutils;
using namespace dbconfig;
namespace opsql
{

template <ObObjTypeClass obj_type_class, ObObjType obj_type>
int get_obj_for_calc(ObIAllocator *allocator, ObObj &left, ObObj &right)
{
  int ret = OB_SUCCESS;
  ObCastCtx cast_ctx(allocator, NULL, CM_NULL_ON_WARN, CS_TYPE_UTF8MB4_GENERAL_CI);
  if (obj_type_class != left.get_type_class()) {
    if (OB_FAIL(ObObjCasterV2::to_type(obj_type, cast_ctx, left, left))) {
      LOG_WDIAG("failed to cast obj", K(ret));
    }
  }

  if (OB_SUCC(ret) && obj_type_class != right.get_type_class()) {
    if (OB_FAIL(ObObjCasterV2::to_type(obj_type, cast_ctx, right, right))) {
      LOG_WDIAG("failed to cast obj", K(ret));
    }
  }

  return ret;
}

static void get_proxy_expr_result_tree_str(ObProxyExpr *root, const int level, char* buf, int& pos, int length)
{
  if (NULL == root || NULL == buf || length < 0) {
    return;
  }
  for (int i = 0 ; i < level * 2; i++) {
    pos += snprintf(buf + pos, length - pos, "-");
  }
  pos += snprintf (buf + pos, length - pos, " type:%s, index:%ld, has_agg:%d, is_func_expr:%d, addr:%p\n",
                   get_expr_type_name(root->type_),
                   root->index_,
                   root->has_agg_,
                   root->is_func_expr_,
                   root);

  if (root->is_func_expr()) {
    ObProxyFuncExpr* func_expr = dynamic_cast<ObProxyFuncExpr*>(root);
    for (int i = 0; i < func_expr->get_param_array().count(); i++) {
      get_proxy_expr_result_tree_str(func_expr->get_param_array().at(i), level + 1, buf, pos, length - pos);
    }
  } else if (OB_PROXY_EXPR_TYPE_FUNC_GROUP == root->get_expr_type()
             || OB_PROXY_EXPR_TYPE_FUNC_ORDER == root->get_expr_type()) {
    ObProxyGroupItem* group_expr = dynamic_cast<ObProxyGroupItem*>(root);
    get_proxy_expr_result_tree_str(group_expr->get_expr(), level + 1, buf, pos, length - pos);
  }
}

void ObProxyExpr::print_proxy_expr(ObProxyExpr *root)
{
  if (OB_UNLIKELY(IS_DEBUG_ENABLED())) {
    char buf[256 * 1024];
    int pos = 0;
    get_proxy_expr_result_tree_str(root, 0, buf, pos, 256 * 1024);
    ObString tree_str(16  * 1024, buf);
    LOG_DEBUG("proxy_expr is \n", K(tree_str));
  }
}

int64_t ObProxyExpr::to_string(char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type));
  J_COMMA();
  J_KV(K_(index));
  J_COMMA();
  J_KV(K_(has_agg));
  J_OBJ_END();
  return pos;
}

int ObProxyExpr::calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                   common::ObIArray<ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (-1 != index_ && ObProxyExprCalcItem::FROM_OBJ_ARRAY == calc_item.source_) {
    int64_t len = calc_item.obj_array_->count();
    if (index_ >= len
        || OB_FAIL(result_obj_array.push_back(*calc_item.obj_array_->at(index_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("calc expr failed", K(ret), K(index_), K(calc_item.obj_array_->count()));
    }
  }

  return ret;
}

int ObProxyExpr::calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                      common::ObIArray<common::ObObj*> &result_obj_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (result_obj_array.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("cant calc, the last obj is result", K(ret));
  } else {
    ObSEArray<ObObj, 4> tmp_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < result_obj_array.count() - 1; i++) {
      ObObj* tmp_obj = result_obj_array.at(i);
      if (OB_FAIL(tmp_array.push_back(*tmp_obj))) {
        LOG_WDIAG("tmp array push back failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(calc(ctx, calc_item, tmp_array))) {
         LOG_WDIAG("calc failed", K(ret));
      } else if (tmp_array.count() != result_obj_array.count()) {
        LOG_WDIAG("not get result", K(ret));
      } else {
        int64_t len = tmp_array.count();
        ObObj *tmp_obj = result_obj_array.at(len - 1);
        *tmp_obj = tmp_array.at(len - 1);
      }
    }
  }

  return ret;
}

int ObProxyExpr::to_sql_string(ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(to_column_string(sql_string))) {
    LOG_WDIAG("fail to column string", K(ret));
  } else if (!alias_name_.empty()) {
    if (OB_FAIL(sql_string.append(" AS "))){
      LOG_WDIAG("append failed", K(ret));
    } else {
      ret = sql_string.append_fmt("%.*s", alias_name_.length(), alias_name_.ptr());
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WDIAG("fail to sql_string", K_(alias_name), K(ret));
  }

  return ret;
}

int ObProxyExprConst::to_column_string(ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (obj_.get_type() == ObIntType) {
    ret = sql_string.append_fmt("%ld", obj_.get_int());
  } else {
    ObString str = obj_.get_string();
    ret = sql_string.append_fmt("'%.*s'", str.length(), str.ptr());
  }
  return ret;
}

int ObProxyExprConst::calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                           common::ObIArray<ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  int64_t len = result_obj_array.count();
  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("cacl expr failed", K(ret));
  } else if (result_obj_array.count() == len) {
    if (OB_FAIL(result_obj_array.push_back(obj_))) {
      LOG_WDIAG("push back obj failed", K(ret));
    }
  }
  ObProxyExpr::print_proxy_expr(this);

  return ret;
}

int ObProxyExprColumn::to_column_string(ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;

  if (!table_name_.empty()) {
    if (OB_FAIL(sql_string.append_fmt("%.*s.", table_name_.length(), table_name_.ptr()))) {
      LOG_WDIAG("fail to append table name", K(table_name_), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql_string.append_fmt("%.*s", column_name_.length(), column_name_.ptr()))) {
      LOG_WDIAG("fail to append table name", K(column_name_), K(ret));
    }
  }

  return ret;
}

int ObProxyExprColumn::calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                            common::ObIArray<ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  int64_t len = result_obj_array.count();
  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("cacl expr failed", K(ret));
  } else if (result_obj_array.count() == len && ObProxyExprCalcItem::FROM_SQL_FIELD == calc_item.source_) {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < calc_item.sql_result_->field_num_; i++) {
      SqlField* field = calc_item.sql_result_->fields_.at(i);
      if (0 == field->column_name_.config_string_.case_compare(column_name_)) {
        found = true;
        common::ObSEArray<SqlColumnValue, 3> &column_values = field->column_values_;
        for (int64_t j = 0; OB_SUCC(ret) && j < column_values.count(); j++) {
          ObObj result_obj;
          SqlColumnValue &sql_column_value = column_values.at(j);
          if (TOKEN_INT_VAL == sql_column_value.value_type_) {
            int64_t value = sql_column_value.column_int_value_;
            result_obj.set_int(value);
            if (OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WDIAG("push back obj failed", K(ret), K(result_obj));
            }
          } else if (TOKEN_STR_VAL == sql_column_value.value_type_) {
            ObString value = sql_column_value.column_value_.config_string_;
            result_obj.set_varchar(value);
            result_obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
            if (OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WDIAG("push back obj failed", K(ret), K(result_obj));
            }
          } else {
            ret = OB_EXPR_CALC_ERROR;
            LOG_WDIAG("sql_column_value value type invalid", K(sql_column_value.value_type_));
          }
        }
      }
    }

    if (!found) {
      ret = OB_EXPR_COLUMN_NOT_EXIST;
    }

    LOG_DEBUG("proxy expr column", K(ret), K(column_name_), K(found), K(result_obj_array));
  }

  ObProxyExpr::print_proxy_expr(this);

  return ret;
}

int ObProxyGroupItem::calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                           ObIArray<ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null", KP_(expr), K(ret));
  } else if (OB_FAIL(expr_->calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("fail to calc expr", K(ret));
  }
  return ret;
}

int ObProxyGroupItem::calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                           ObIArray<ObObj*> &result_obj_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null", KP_(expr), K(ret));
  } else if (OB_FAIL(expr_->calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("fail to calc expr", K(ret));
  }
  return ret;
}

int ObProxyGroupItem::to_column_string(ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null", K(ret));
  } else if (OB_FAIL(expr_->to_column_string(sql_string))) {
    LOG_WDIAG("to column string failed", K(ret));
  }

  return ret;
}

int ObProxyOrderItem::to_sql_string(ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null", K(ret));
  } else if (OB_FAIL(expr_->to_column_string(sql_string))) {
    LOG_WDIAG("to sql_string failed", K(ret));
  } else if (order_direction_ == NULLS_FIRST_ASC){
    if (OB_FAIL(sql_string.append(" ASC"))) {
      LOG_WDIAG("fail to append", K(ret));
    }
  } else if (OB_FAIL(sql_string.append(" DESC"))) {
    LOG_WDIAG("fail to append", K(ret));
  }
  return ret;
}

ObProxyFuncExpr::~ObProxyFuncExpr()
{
  for (int64_t i = 0; i < param_array_.count(); i++) {
    ObProxyExpr *expr = param_array_.at(i);
    expr->~ObProxyExpr();
  }
}

int ObProxyFuncExpr::calc_param_expr(const ObProxyExprCtx &ctx,
                                     const ObProxyExprCalcItem &calc_item,
                                     common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> &param_result,
                                     int &cnt)
{
  int ret = OB_SUCCESS;
  cnt = 1;

  param_result.reset();
  for (int64_t i = 0;  OB_SUCC(ret) && i < param_array_.count(); i++) {
    ObProxyExpr *expr = param_array_.at(i);
    ObSEArray<common::ObObj, 4> array;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("expr is null, unexpected", K(ret));
    } else if (OB_FAIL(expr->calc(ctx, calc_item, array))) {
      LOG_WDIAG("expr cal failed", K(ret));
    } else if (OB_FAIL(param_result.push_back(array))) {
      LOG_WDIAG("param result push failed", K(ret));
    } else if (array.count() > 1) {
      cnt = static_cast<int>(array.count());
      if (cnt != 1 && cnt != array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("calc param expr failed, two columns have more than one value", K(ret));
      }
    } else if (array.count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("calc param expr failed, no param result", K(ret));
    }
  }

  return ret;
}

int ObProxyFuncExpr::get_int_obj_with_default_charset(const common::ObObj &src, common::ObObj &dst, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (!src.is_int()) {
    ObCollationType collation = ObCharset::get_default_collation(ObCharset::get_default_charset());
    ObCastCtx cast_ctx(&allocator, NULL, CM_NULL_ON_WARN, collation);
    if (OB_FAIL(ObObjCasterV2::to_type(ObIntType, collation, cast_ctx, src, dst))) {
      LOG_WDIAG("cast obj to varchar obj fail", K(ret));
    }
  } else {
    dst = src;
  } 
  return ret; 
}

int ObProxyFuncExpr::get_int_obj(const ObObj &src, ObObj &dst, const ObProxyExprCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret));
  } else if (!src.is_int()) {
    ObCollationType collation = get_collation(ctx);
    ObCastCtx cast_ctx(ctx.allocator_, NULL, CM_NULL_ON_WARN, collation);
    if (OB_FAIL(ObObjCasterV2::to_type(ObIntType, collation, cast_ctx, src, dst))) {
      LOG_WDIAG("cast obj to varchar obj fail", K(ret));
    }
  } else {
    dst = src;
  } 

  return ret;
}

int ObProxyFuncExpr::get_varchar_obj(const common::ObObj &src, common::ObObj &dst, const ObProxyExprCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret));
  } else if (!src.is_varchar()) {
    ObCollationType collation = get_collation(ctx);
    ObCastCtx cast_ctx(ctx.allocator_, NULL, CM_NULL_ON_WARN, collation);
    if (src.is_null()) {
      dst.set_varchar("");
    } else if (OB_FAIL(ObObjCasterV2::to_type(ObVarcharType, collation, cast_ctx, src, dst))) {
      LOG_WDIAG("cast obj to varchar obj fail", K(ret));
    }
  } else if (src.is_varchar()) {
    dst = src;
  }

  return ret;
}

int ObProxyFuncExpr::get_time_obj(const common::ObObj &src, common::ObObj &dst, const ObProxyExprCtx &ctx, const common::ObObjType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.client_session_info_) || OB_ISNULL(ctx.client_session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret));
  } else if (src.get_type_class() != ObOTimestampTC) {
    ObCollationType collation = get_collation(ctx);
    ObTimeZoneInfo tz_info;
    ObDataTypeCastParams dtc_params;
    ObCastCtx cast_ctx;
    if (OB_FAIL(proxy::ObExprCalcTool::build_tz_info_for_all_type(ctx.client_session_info_, tz_info))) {
      LOG_WDIAG("fail to build time zone info with ctx session", K(ret));
    } else if (FALSE_IT(dtc_params.tz_info_ = &tz_info)) {
    } else if (OB_FAIL(proxy::ObExprCalcTool::build_dtc_params (
      ctx.client_session_info_, type, dtc_params))) {
      LOG_WDIAG("fail to build dtc params", K(ret));
    } else if (FALSE_IT(cast_ctx = ObCastCtx(ctx.allocator_, &dtc_params, CM_NULL_ON_WARN, collation))) {
    } else if (OB_FAIL(ObObjCasterV2::to_type(type, collation, cast_ctx, src, dst))) {
      LOG_WDIAG("cast obj to varchar obj fail", K(ret));
    }
  } else {
    dst = src;
  }
  return ret; 
}

ObCollationType ObProxyFuncExpr::get_collation(const ObProxyExprCtx &ctx)
{
  ObCollationType collation =
      (ctx.client_session_info_ != NULL) 
          ? static_cast<ObCollationType>(ctx.client_session_info_->get_collation_connection())
          : ObCharset::get_default_collation(ObCharset::get_default_charset());
  return collation;
}


int ObProxyFuncExpr::check_varchar_empty(const common::ObObj& result)
{
  int ret = OB_SUCCESS;
  ObString str;
  if (!result.is_varchar()) {
    ret = OB_EXPR_CALC_ERROR;
    LOG_WDIAG("check varchar empty failed, result is not varchar", K(ret), K(result));
  } else if (OB_FAIL(result.get_varchar(str))) {
    LOG_WDIAG("get varchar failed", K(ret));
  } else if (str.empty()) {
    ret = OB_EXPR_CALC_ERROR;
    LOG_WDIAG("str is empty", K(ret));
  }

  return ret;
}

int ObProxyExprHash::calc(const ObProxyExprCtx &ctx,
                          const ObProxyExprCalcItem &calc_item,
                          common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() >2 || param_array_.count() < 1)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("hash should have one or two param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj result_obj;
          ObObj obj1;
          int64_t index = -1;
          int64_t num = ctx.sharding_physical_size_;

          ObObj &test_load_obj = param_result.at(0);
          if (testload_need_handle_special_char(ctx.test_load_type_)
              && test_load_obj.is_varchar()) {
            ObString str = test_load_obj.get_varchar();
            ObShardRule::handle_special_char(str.ptr(), str.length());
            test_load_obj.set_varchar(str);
            LOG_DEBUG("test load type", K(test_load_obj));
          }

          if (OB_FAIL(get_int_obj(test_load_obj, obj1, ctx))) {
            LOG_WDIAG("get int obj failed", K(ret), K(test_load_obj));
          } else if (OB_FAIL(obj1.get_int(index))) {
            LOG_WDIAG("get int failed", K(ret), K(obj1));
          }

          if (OB_SUCC(ret) && param_array_.count() == 2) {
            ObObj obj2;
            if (OB_FAIL(get_int_obj(param_result.at(1), obj2, ctx))) {
              LOG_WDIAG("get int obj failed", K(ret));
            } else if (OB_FAIL(obj2.get_int(num))) {
              LOG_WDIAG("get int failed", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            if (!ctx.is_elastic_index_) {
              if (0 == num) {
                ret = OB_EXPR_CALC_ERROR;
                LOG_WDIAG("num is 0", K(ret));
              } else {
                index = index % num;
              }
            }
            if (OB_SUCC(ret)) {
              result_obj.set_int(index);
              if (OB_FAIL(result_obj_array.push_back(result_obj))) {
                LOG_WDIAG("result obj array push back failed", K(ret));
              }
            }
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);

  return ret;
}

int ObProxyExprSubStr::calc(const ObProxyExprCtx &ctx,
                            const ObProxyExprCalcItem &calc_item,
                            common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(2 != param_array_.count() && 3 != param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("substr has no param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj first_obj;
          ObObj result_obj;
          ObString value_str;
          int64_t start_pos = 0;
          int64_t substr_len = -1;
          ObCollationType collation = get_collation(ctx);
          // substr retrun NULL on out of bounds params
          bool is_invalid_params = false;
          bool is_contains_null_params = proxy::ObExprCalcTool::is_contains_null_params(param_result);

          if (is_contains_null_params) {
            // do nothing
          } else {
            if (OB_FAIL(get_varchar_obj(param_result.at(0), first_obj, ctx))) {
              LOG_WDIAG("get varchar obj failed", K(ret));
            } else if (OB_FAIL(first_obj.get_varchar(value_str))) {
              LOG_WDIAG("get varchar failed", K(ret), K(first_obj));
            } else if (value_str.empty()) {
              is_invalid_params = true;
            }

            if (OB_SUCC(ret)) {
              ObObj obj;
              if (OB_FAIL(get_int_obj(param_result.at(1), obj, ctx))) {
                LOG_WDIAG("get int obj failed", K(ret));
              } else if (OB_FAIL(obj.get_int(start_pos))) {
                LOG_WDIAG("get int failed", K(ret));
              } else if (start_pos == 0) {
                if (ctx.is_oracle_mode) {
                  // 1 and 0 treated as thr first character in oracle
                  start_pos = 1;
                } else {
                  is_invalid_params = true;
                }
              }
            }

            if (OB_SUCC(ret) && 3 == param_result.count()) {
              ObObj obj;
              if (OB_FAIL(get_int_obj(param_result.at(2), obj, ctx))) {
                LOG_WDIAG("get int obj failed", K(ret));
              } else if (OB_FAIL(obj.get_int(substr_len))) {
                LOG_WDIAG("get int failed", K(ret));
              } else if (substr_len <= 0) {
                is_invalid_params = true;
                LOG_DEBUG("substr function param 3 is less than 1", K(ret));
              }
            }

            if (OB_SUCC(ret) && !is_invalid_params) {
              int64_t mb_len = static_cast<int64_t>(ObCharset::strlen_char(collation, value_str.ptr(), value_str.length()));
              if (start_pos < 0) {
                start_pos = mb_len + start_pos + 1;
              }
              if (-1 == substr_len || start_pos + substr_len - 1 > mb_len) {
                substr_len = mb_len - start_pos + 1;
              }
              if (start_pos <= 0 || start_pos > mb_len || substr_len <= 0 || substr_len > mb_len || start_pos + substr_len - 1 > mb_len) {
                is_invalid_params = true;
                LOG_DEBUG("invalid substr params", K(start_pos), K(substr_len), K(mb_len), K(value_str), K(ret));
              }

              start_pos --;
              start_pos = static_cast<int64_t>(ObCharset::charpos(collation, value_str.ptr(), value_str.length(), start_pos));
              substr_len = static_cast<int64_t>(ObCharset::charpos(collation, value_str.ptr() + start_pos, (start_pos == 0) ? value_str.length(): value_str.length() - start_pos, substr_len));
            }
          }

          if (OB_SUCC(ret)) {
            if (is_contains_null_params) {
              result_obj.set_null();
            } else if(is_invalid_params) {
              if (ctx.is_oracle_mode) {
                result_obj.set_null();
              } else {
                result_obj.set_varchar(ObString());
              }
            } else {
              ObString result_str;
              result_str.assign_ptr(value_str.ptr() + start_pos, static_cast<int32_t>(substr_len));
              result_obj.set_varchar(result_str);
              result_obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WDIAG("result obj array push back failed", K(ret));
            }
          }

        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);

  return ret;
}

/*
 * concat('a', 'ab', 'abc')
 * in mysql mode, if param contains NULL, return NULL
 */
int ObProxyExprConcat::calc(const ObProxyExprCtx &ctx,
                            const ObProxyExprCalcItem &calc_item,
                            common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(0 >= param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("concat has no param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj result_obj;
          ObIAllocator *allocator = ctx.allocator_;
          ObString result_str;
          bool is_null_exist = false;
          int total_len = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < param_result.count(); i++) {
            ObObj tmp_obj;
            ObString tmp_str;
            if (param_result.at(i).is_null()) {
              // do nothing
              is_null_exist = true;
            }
            if (OB_FAIL(get_varchar_obj(param_result.at(i), tmp_obj, ctx))) {
              LOG_WDIAG("get varchar obj failed", K(ret), K(param_result.at(i)));
            } else if (OB_FAIL(tmp_obj.get_varchar(tmp_str))) {
                LOG_WDIAG("get varchar failed", K(ret), K(tmp_obj));
            } else {
              total_len += tmp_str.length();
            }
          }
          if (OB_SUCC(ret) && total_len > 0) {
            char *buf = NULL;
            if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(total_len)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WDIAG("alloc memory failed", K(total_len));
            } else {
              int cur_len = 0;
              for (int64_t i = 0; OB_SUCC(ret) && i < param_result.count() && cur_len < total_len; i++) {
                ObObj tmp_obj;
                ObString tmp_str;
                if (OB_FAIL(get_varchar_obj(param_result.at(i), tmp_obj, ctx))) {
                  LOG_WDIAG("get varchar obj failed", K(ret), K(param_result.at(i)));
                } else {
                  if (OB_FAIL(tmp_obj.get_varchar(tmp_str))) {
                    LOG_WDIAG("get varchar failed", K(ret), K(tmp_obj));
                  } else if (tmp_str.length() == 0) {
                    // do nothing
                  } else if (cur_len + tmp_str.length() <= total_len){
                    MEMCPY(buf + cur_len, tmp_str.ptr(), tmp_str.length());
                    cur_len += tmp_str.length();
                  } else {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WDIAG("unexpected str len", "str_len", tmp_str.length(), K(total_len), K(cur_len), K(ret));
                  }
                }
              }
              if (OB_SUCC(ret)) {
                result_str.assign(buf, total_len);
              }
            }
          }

          if (OB_SUCC(ret)) {
            result_obj.set_varchar(result_str);
            if ((!ctx.is_oracle_mode && is_null_exist) || (ctx.is_oracle_mode && result_str.empty())) {
              result_obj.set_null();
            }
            if (OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WDIAG("result obj array push back failed", K(ret));
            }
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);

  return ret;
}

int ObProxyExprToInt::calc(const ObProxyExprCtx &ctx,
                           const ObProxyExprCalcItem &calc_item,
                           common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (1 != param_array_.count()) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("toint should have one param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param result failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);
        if (OB_SUCC(ret)) {
          ObObj result_obj;
          ObObj &obj = param_result.at(0);
          if (OB_FAIL(get_int_obj(obj, result_obj, ctx))) {
            LOG_WDIAG("get int obj failed", K(ret));
          } else if (OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);

  return ret;
}

int ObProxyExprDiv::calc(const ObProxyExprCtx &ctx,
                         const ObProxyExprCalcItem &calc_item,
                         common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (-1 != index_ && ObProxyExprCalcItem::FROM_OBJ_ARRAY == calc_item.source_ && !has_agg_) {
    if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
      LOG_WDIAG("calc expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && len == result_obj_array.count()) {
    if (2 != param_array_.count()) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("div should have two param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param result failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj obj1 = param_result.at(0);
          ObObj obj2 = param_result.at(1);
          ObObj result_obj;
          if (obj1.is_null() || obj2.is_null()) {
            result_obj.set_null();
          } else if (ObDoubleTC == obj1.get_type_class() || ObDoubleTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObDoubleTC, ObDoubleType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get double obj failed", K(obj1), K(obj2), K(ret));
            } else if (fabs(obj2.get_double()) < DBL_EPSILON) {
              result_obj.set_null();
            } else {
              double obj1_d = obj1.get_double();
              double obj2_d = obj2.get_double();
              result_obj.set_double(obj1_d / obj2_d);
            }
          } else if (ObFloatTC == obj1.get_type_class() || ObFloatTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObFloatTC, ObFloatType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get float obj failed", K(obj1), K(obj2), K(ret));
            } else if (fabs(obj2.get_float()) < FLT_EPSILON) {
              result_obj.set_null();
            } else {
              float obj1_f = obj1.get_float();
              float obj2_f = obj2.get_float();
              result_obj.set_float(obj1_f / obj2_f);
            }
          } else if (ObIntTC != obj1.get_type_class() || ObIntTC != obj2.get_type_class()) {
            number::ObNumber res_nmb;
            if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get number obj failed", K(ret), K(obj1), K(obj2));
            } else if (OB_UNLIKELY(obj2.get_number().is_zero())) {
              result_obj.set_null();
            } else if (OB_FAIL(obj1.get_number().div(obj2.get_number(), res_nmb, *ctx.allocator_))) {
              LOG_WDIAG("failed to div numbers", K(ret), K(obj1), K(obj2));
            } else {
              if (ctx.scale_ >= 0 || accuracy_.get_scale() >= 0) {
                int64_t scale = ctx.scale_ >= 0 ? ctx.scale_ : accuracy_.get_scale();
                if (OB_FAIL(res_nmb.round(scale))) {
                  LOG_WDIAG("failed to round result number", K(res_nmb),
                           K(scale), K(accuracy_), K(ctx.scale_), K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                result_obj.set_number(res_nmb);
              }
            }
          } else {
            if (OB_FAIL(get_int_obj(param_result.at(0), obj1, ctx))) {
              LOG_WDIAG("get int obj failed", K(ret));
            } else if (OB_FAIL(get_int_obj(param_result.at(1), obj2, ctx))) {
              LOG_WDIAG("get int obj failed", K(ret));
            } else {
              int64_t num1 = obj1.get_int();
              int64_t num2 = obj2.get_int();
              if (0 == num2) {
                LOG_INFO("div func and num2 is 0, set NULL", K(ret));
                result_obj.set_null();
              } else {
                result_obj.set_int(num1 / num2);
              }
            }
          }

          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);

  return ret;
}

int ObProxyExprAdd::calc(const ObProxyExprCtx &ctx,
                         const ObProxyExprCalcItem &calc_item,
                         common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (-1 != index_ && ObProxyExprCalcItem::FROM_OBJ_ARRAY == calc_item.source_ && !has_agg_) {
    if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
      LOG_WDIAG("calc expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && len == result_obj_array.count()) {
    if (2 != param_array_.count()) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("add should have two param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param result failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj obj1 = param_result.at(0);
          ObObj obj2 = param_result.at(1);
          ObObj result_obj;
          if (obj1.is_null() || obj2.is_null()) {
            result_obj.set_null();
          } else if (ObDoubleTC == obj1.get_type_class() || ObDoubleTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObDoubleTC, ObDoubleType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get double obj failed", K(obj1), K(obj2), K(ret));
            } else {
              double obj1_d = obj1.get_double();
              double obj2_d = obj2.get_double();
              result_obj.set_double(obj1_d + obj2_d);
            }
          } else if (ObFloatTC == obj1.get_type_class() || ObFloatTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObFloatTC, ObFloatType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get float obj failed", K(obj1), K(obj2), K(ret));
            } else {
              float obj1_f = obj1.get_float();
              float obj2_f = obj2.get_float();
              result_obj.set_float(obj1_f + obj2_f);
            }
          } else if (ObIntTC != obj1.get_type_class() || ObIntTC != obj2.get_type_class()) {
            number::ObNumber res_nmb;
            if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get number obj failed", K(ret), K(obj1), K(obj2));
            } else if (OB_FAIL(obj1.get_number().add(obj2.get_number(), res_nmb, *ctx.allocator_))) {
              LOG_WDIAG("failed to add numbers", K(ret), K(obj1), K(obj2));
            } else {
              if (ctx.scale_ >= 0 || accuracy_.get_scale() >= 0) {
                int64_t scale = ctx.scale_ >= 0 ? ctx.scale_ : accuracy_.get_scale();
                if (OB_FAIL(res_nmb.round(scale))) {
                  LOG_WDIAG("failed to round result number", K(res_nmb),
                           K(scale), K(accuracy_), K(ctx.scale_), K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                result_obj.set_number(res_nmb);
              }
            }
          } else {
            if (OB_FAIL(get_int_obj(param_result.at(0), obj1, ctx))) {
              LOG_WDIAG("get int obj failed", K(ret));
            } else if (OB_FAIL(get_int_obj(param_result.at(1), obj2, ctx))) {
              LOG_WDIAG("get int obj failed", K(ret));
            } else {
              int64_t num1 = obj1.get_int();
              int64_t num2 = obj2.get_int();
              result_obj.set_int(num1 + num2);
            }
          }

          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);

  return ret;
}

int ObProxyExprSub::calc(const ObProxyExprCtx &ctx,
                         const ObProxyExprCalcItem &calc_item,
                         common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (-1 != index_ && ObProxyExprCalcItem::FROM_OBJ_ARRAY == calc_item.source_ && !has_agg_) {
    if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
      LOG_WDIAG("calc expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && len == result_obj_array.count()) {
    if (2 != param_array_.count()) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("sub should have two param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param result failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj obj1 = param_result.at(0);
          ObObj obj2 = param_result.at(1);
          ObObj result_obj;
          if (obj1.is_null() || obj2.is_null()) {
            result_obj.set_null();
          } else if (ObDoubleTC == obj1.get_type_class() || ObDoubleTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObDoubleTC, ObDoubleType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get double obj failed", K(obj1), K(obj2), K(ret));
            } else {
              double obj1_d = obj1.get_double();
              double obj2_d = obj2.get_double();
              result_obj.set_double(obj1_d - obj2_d);
            }
          } else if (ObFloatTC == obj1.get_type_class() || ObFloatTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObFloatTC, ObFloatType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get float obj failed", K(obj1), K(obj2), K(ret));
            } else {
              float obj1_f = obj1.get_float();
              float obj2_f = obj2.get_float();
              result_obj.set_float(obj1_f - obj2_f);
            }
          } else if (ObIntTC != obj1.get_type_class() || ObIntTC != obj2.get_type_class()) {
            number::ObNumber res_nmb;
            if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get number obj failed", K(ret), K(obj1), K(obj2));
            } else if (OB_FAIL(obj1.get_number().sub(obj2.get_number(), res_nmb, *ctx.allocator_))) {
              LOG_WDIAG("failed to sub numbers", K(ret), K(obj1), K(obj2));
            } else {
              if (ctx.scale_ >= 0 || accuracy_.get_scale() >= 0) {
                int64_t scale = ctx.scale_ >= 0 ? ctx.scale_ : accuracy_.get_scale();
                if (OB_FAIL(res_nmb.round(scale))) {
                  LOG_WDIAG("failed to round result number", K(res_nmb),
                           K(scale), K(accuracy_), K(ctx.scale_), K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                result_obj.set_number(res_nmb);
              }
            }
          } else {
            if (OB_FAIL(get_int_obj(param_result.at(0), obj1, ctx))) {
              LOG_WDIAG("get int obj failed", K(ret));
            } else if (OB_FAIL(get_int_obj(param_result.at(1), obj2, ctx))) {
              LOG_WDIAG("get int obj failed", K(ret));
            } else {
              int64_t num1 = obj1.get_int();
              int64_t num2 = obj2.get_int();
              result_obj.set_int(num1 - num2);
            }
          }

          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);

  return ret;
}

int ObProxyExprMul::calc(const ObProxyExprCtx &ctx,
                         const ObProxyExprCalcItem &calc_item,
                         common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (-1 != index_ && ObProxyExprCalcItem::FROM_OBJ_ARRAY == calc_item.source_ && !has_agg_) {
    if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
      LOG_WDIAG("calc expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && len == result_obj_array.count()) {
    if (2 != param_array_.count()) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("mul should have two param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param result failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj obj1 = param_result.at(0);
          ObObj obj2 = param_result.at(1);
          ObObj result_obj;
          if (obj1.is_null() || obj2.is_null()) {
            result_obj.set_null();
          } else if (ObDoubleTC == obj1.get_type_class() || ObDoubleTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObDoubleTC, ObDoubleType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get double obj failed", K(obj1), K(obj2), K(ret));
            } else {
              double obj1_d = obj1.get_double();
              double obj2_d = obj2.get_double();
              result_obj.set_double(obj1_d * obj2_d);
            }
          } else if (ObFloatTC == obj1.get_type_class() || ObFloatTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObFloatTC, ObFloatType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get float obj failed", K(obj1), K(obj2), K(ret));
            } else {
              float obj1_f = obj1.get_float();
              float obj2_f = obj2.get_float();
              result_obj.set_float(obj1_f * obj2_f);
            }
          } else if (ObIntTC != obj1.get_type_class() || ObIntTC != obj2.get_type_class()) {
            number::ObNumber res_nmb;
            if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get number obj failed", K(ret), K(obj1), K(obj2));
            } else if (OB_FAIL(obj1.get_number().mul(obj2.get_number(), res_nmb, *ctx.allocator_))) {
              LOG_WDIAG("failed to mul numbers", K(ret), K(obj1), K(obj2));
            } else {
              if (ctx.scale_ >= 0 || accuracy_.get_scale() >= 0) {
                int64_t scale = ctx.scale_ >= 0 ? ctx.scale_ : accuracy_.get_scale();
                if (OB_FAIL(res_nmb.round(scale))) {
                  LOG_WDIAG("failed to round result number", K(res_nmb),
                           K(scale), K(accuracy_), K(ctx.scale_), K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                result_obj.set_number(res_nmb);
              }
            }
          } else {
            if (OB_FAIL(get_int_obj(param_result.at(0), obj1, ctx))) {
              LOG_WDIAG("get int obj failed", K(ret));
            } else if (OB_FAIL(get_int_obj(param_result.at(1), obj2, ctx))) {
              LOG_WDIAG("get int obj failed", K(ret));
            } else {
              int64_t num1 = obj1.get_int();
              int64_t num2 = obj2.get_int();
              result_obj.set_int(num1 * num2);
            }
          }

          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);

  return ret;
}

int ObProxyExprTestLoad::calc(const ObProxyExprCtx &ctx,
                              const ObProxyExprCalcItem &calc_item,
                              common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (1 != param_array_.count()) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("testload should have one param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param result failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj tmp_obj;
          ObObj result_obj;
          if (OB_FAIL(get_varchar_obj(param_result.at(0), tmp_obj, ctx))) {
            ret = OB_EXPR_CALC_ERROR;
            LOG_WDIAG("testload param is not varchar", K(ret), K(param_result.at(0)));
          } else {
            result_obj = tmp_obj;
            ObString str = result_obj.get_varchar();
            ObShardRule::handle_special_char(str.ptr(), str.length());
            result_obj.set_varchar(str);

            if (OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WDIAG("result obj array push back failed", K(ret));
            }
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);

  return ret;
}

int ObProxyExprSplit::calc(const ObProxyExprCtx &ctx,
                           const ObProxyExprCalcItem &calc_item,
                           common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(3 != param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("split not enough param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("split calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObString value;
          ObObj first_obj;
          ObString symbol;
          int64_t index = -1;

          if (OB_FAIL(get_varchar_obj(param_result.at(0), first_obj, ctx))) {
            LOG_WDIAG("get varchar obj failed", K(ret));
          } else if (OB_FAIL(first_obj.get_varchar(value))) {
            LOG_WDIAG("get varchar failed", K(ret), K(first_obj));
          } else if (value.empty()) {
            ret = OB_EXPR_CALC_ERROR;
            LOG_WDIAG("split first parm is emtpy", K(ret));
          }

          if (OB_SUCC(ret)) {
            ObObj obj;
            if (OB_FAIL(get_varchar_obj(param_result.at(1), obj, ctx))) {
              LOG_WDIAG("get varchar obj failed", K(ret));
            } else if (OB_FAIL(obj.get_varchar(symbol))) {
              LOG_WDIAG("get varchar failed", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            ObObj obj;
            if (OB_FAIL(get_int_obj(param_result.at(2), obj, ctx))) {
              LOG_WDIAG("get int obj failed", K(ret));
            } else if (OB_FAIL(obj.get_int(index))) {
              LOG_WDIAG("get int failed", K(ret));
            } else if (index <= 0) {
              ret = OB_EXPR_CALC_ERROR;
              LOG_WDIAG("split function param 3 is less than 1", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            ObSEArray<ObString, 4> tmp_result;
            char *start = value.ptr();
            char *end = value.ptr() + value.length();
            char *pos = start;

            while (OB_SUCC(ret) && start < end) {
              bool found = true;
              char *tmp = start;
              for (int64_t i = 0; i < symbol.length() && tmp < end; i++) {
                char c1 = *(char*)(symbol.ptr() + i);
                char c2 = *(char*)(tmp);
                tmp++;
                if (c1 != c2) {
                  found = false;
                  break;
                }
              }

              if (found) {
                ObString str;
                str.assign_ptr(pos, static_cast<ObString::obstr_size_t>(start - pos));
                start = tmp;
                pos = start;
                if (OB_FAIL(tmp_result.push_back(str))) {
                  LOG_WDIAG("tmp result push back str failed", K(ret));
                }
              } else {
                start++;
              }
            }

            if (OB_SUCC(ret)) {
              ObString str;
              str.assign_ptr(pos, static_cast<ObString::obstr_size_t>(end - pos));
              if (OB_FAIL(tmp_result.push_back(str))) {
                LOG_WDIAG("tmp result push back str failed", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              ObObj result_obj;
              if (index <= tmp_result.count()) {
                ObString result_str = tmp_result.at(index - 1);
                result_obj.set_varchar(result_str);
                result_obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

                if (OB_FAIL(check_varchar_empty(result_obj))) {
                  LOG_WDIAG("check varchar emtpy failed", K(ret));
                } else if (OB_FAIL(result_obj_array.push_back(result_obj))) {
                  LOG_WDIAG("result obj array push back failed", K(ret));
                }
              } else {
                ret = OB_EXPR_CALC_ERROR;
                LOG_WDIAG("param 3 is large than result count", K(ret), K(index), K(tmp_result.count()));
              }
            }
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  LOG_DEBUG("proxy expr split", K(ret));

  return ret;
}

int ObProxyExprAvg::calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                   common::ObIArray<ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  if (ObProxyExprCalcItem::FROM_OBJ_ARRAY == calc_item.source_) {
    if (OB_NOT_NULL(sum_expr_) && OB_NOT_NULL(count_expr_)) {
      common::ObSEArray<common::ObObj, 4> param_result;
      if (sum_expr_->calc(ctx, calc_item, param_result)) {
        LOG_WDIAG("fail to calc sum", K(ret));
      } else if (count_expr_->calc(ctx, calc_item, param_result)) {
        LOG_WDIAG("fail to calc count", K(ret));
      } else if (OB_UNLIKELY(2 != param_result.count())) {
        ret = OB_EXPR_CALC_ERROR;
        LOG_WDIAG("avg not enough param", "count", param_result.count(), K(ret));
      } else {
        ObObj obj1 = param_result.at(0);
        ObObj obj2 = param_result.at(1);
        ObObj result_obj;
        if (ObDoubleTC == obj1.get_type_class() || ObDoubleTC == obj2.get_type_class()) {
          if (OB_FAIL((get_obj_for_calc<ObDoubleTC, ObDoubleType>(ctx.allocator_, obj1, obj2)))) {
            LOG_WDIAG("get double obj failed", K(obj1), K(obj2), K(ret));
          } else if (fabs(obj2.get_double()) < DBL_EPSILON) {
            result_obj.set_null();
          } else {
            double obj1_d = obj1.get_double();
            double obj2_d = obj2.get_double();
            result_obj.set_double(obj1_d / obj2_d);
          }
        } else if (ObFloatTC == obj1.get_type_class() || ObFloatTC == obj2.get_type_class()) {
          if (OB_FAIL((get_obj_for_calc<ObFloatTC, ObFloatType>(ctx.allocator_, obj1, obj2)))) {
            LOG_WDIAG("get float obj failed", K(obj1), K(obj2), K(ret));
          } else if (fabs(obj2.get_float() < FLT_EPSILON)) {
            result_obj.set_null();
          } else {
            float obj1_f = obj1.get_float();
            float obj2_f = obj2.get_float();
            result_obj.set_float(obj1_f / obj2_f);
          }
        } else if (ObIntTC != obj1.get_type_class() || ObIntTC != obj2.get_type_class()) {
          number::ObNumber res_nmb;
          if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(ctx.allocator_, obj1, obj2)))) {
            LOG_WDIAG("get number obj failed", K(ret), K(obj1), K(obj2));
          } else if (OB_UNLIKELY(obj2.get_number().is_zero())) {
            result_obj.set_null();
          } else if (OB_FAIL(obj1.get_number().div(obj2.get_number(), res_nmb, *ctx.allocator_))) {
            LOG_WDIAG("failed to div numbers", K(ret), K(obj1), K(obj2));
          } else {
            if (ctx.scale_ >= 0 || accuracy_.get_scale() >= 0) {
              int64_t scale = ctx.scale_ >= 0 ? ctx.scale_ : accuracy_.get_scale();
              if (OB_FAIL(res_nmb.round(scale))) {
                LOG_WDIAG("failed to round result number", K(res_nmb),
                         K(scale), K(accuracy_), K(ctx.scale_), K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              result_obj.set_number(res_nmb);
            }
          }
        } else {
          if (OB_FAIL(get_int_obj(param_result.at(0), obj1, ctx))) {
            LOG_WDIAG("get int obj failed", K(ret));
          } else if (OB_FAIL(get_int_obj(param_result.at(1), obj2, ctx))) {
            LOG_WDIAG("get int obj failed", K(ret));
          } else {
            int64_t num1 = obj1.get_int();
            int64_t num2 = obj2.get_int();
            if (0 == num2) {
              ret = OB_EXPR_CALC_ERROR;
              LOG_WDIAG("div failed, num2 is 0", K(ret));
            } else {
              result_obj.set_int(num1 / num2);
            }
          }
        }

        if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
          LOG_WDIAG("result obj array push back failed", K(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("can't calc avg expr", KP(sum_expr_), KP(count_expr_), K(ret));
    }
  }

  ObProxyExpr::print_proxy_expr(this);

  return ret;
}

/*
 * for to_date and to_timestamp, only support at least one param, at most two params 
 */
int ObProxyExprToTimeHandler::calc(const ObProxyExprCtx &ctx,
                                   const ObProxyExprCalcItem &calc_item,
                                   common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() < 1)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("to_date should have at least one param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        ObObjTypeClass type = param_result.at(0).get_type_class();
        if (OB_SUCC(ret) && type != ObStringTC && type != ObIntTC &&
            type != ObNumberTC && type != ObDateTimeTC && type != ObOTimestampTC) {
          ret = OB_EXPR_CALC_ERROR;
          LOG_WDIAG("unexpected the first param type of to_date", K(type));
        }

        ObTimeZoneInfo tz_info;
        ObDataTypeCastParams dtc_params;
        if (OB_SUCC(ret)) {
          ObObj second_obj;
          ObString nls_format;

          if (2 == param_result.count()) {
            if(OB_FAIL(get_varchar_obj(param_result.at(1), second_obj, ctx))){
              LOG_WDIAG("get varchar obj failed", K(ret));
            } else if (OB_FAIL(second_obj.get_varchar(nls_format))) {
              LOG_WDIAG("get varchar failed", K(ret));
            } else {
              if (!nls_format.empty()) {
                dtc_params.set_nls_format_by_type(target_type_, nls_format);
              } else {
                if (OB_FAIL(proxy::ObExprCalcTool::build_dtc_params(
                    ctx.client_session_info_, target_type_, dtc_params))) {
                  LOG_WDIAG("fail to build dtc params", K(ret), K(target_type_));
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(proxy::ObExprCalcTool::build_tz_info(ctx.client_session_info_, param_result.at(0).get_type() ,tz_info))) {
            LOG_WDIAG("fail to build time zone info with ctx session", K(ret));
          } else {
            dtc_params.tz_info_ = &tz_info;
          }
        }
        ObCollationType collation = get_collation(ctx);
        ObCastCtx cast_ctx(ctx.allocator_, &dtc_params, CM_NULL_ON_WARN, collation);
        param_result.at(0).set_collation_type(collation);
        if (OB_SUCC(ret) && OB_FAIL(ObObjCasterV2::to_type(target_type_, collation, cast_ctx,
                                                           param_result.at(0), param_result.at(0)))) {
          LOG_WDIAG("fail to cast obj", K(ret), K(target_type_), K(collation));
        }
        if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(param_result.at(0)))) {
          LOG_WDIAG("result obj array push back failed", K(ret));
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

int ObProxyExprNvl::calc(const ObProxyExprCtx &ctx,
                         const ObProxyExprCalcItem &calc_item,
                         common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() != 2)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("nvl should have two param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        ObObj result_obj = param_result.at(0);
        ObObj second_obj = param_result.at(1);
        if (OB_SUCC(ret)) {
          // null expr or empty varchar treated as null
          if (result_obj.is_null()) {
            result_obj = second_obj;
          } else if (result_obj.is_string_type()) {
            ObString str;
            if (OB_FAIL(result_obj.get_string(str))) {
              LOG_WDIAG("get varchar of param failed", K(ret));
            } else if (str.empty()) {
              result_obj = second_obj;
            }
          }
        }

        if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
          LOG_WDIAG("result obj array push back failed", K(ret));
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

/*
 * only support datetime format.
 */
int ObProxyExprToChar::calc(const ObProxyExprCtx &ctx,
                            const ObProxyExprCalcItem &calc_item,
                            common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() < 1) || OB_UNLIKELY(param_array_.count() > 2)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("to_char should have one or two param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);
        if (OB_SUCC(ret)) {
          ObObjType target_type = ObVarcharType;
          ObObj result_obj = param_result.at(0);
          ObObj second_obj;
          ObString nls_format;
          ObTimeZoneInfo tz_info;
          ObDataTypeCastParams dtc_params = ObDataTypeCastParams();

          // to_char(datetime) return NULL when nls_format parmas is '' 
          bool is_empty_format = false;

          ObCollationType collation = get_collation(ctx);
          dtc_params.tz_info_ = &tz_info;

          if (ObDateTimeTC != param_result.at(0).get_type_class() && ObOTimestampTC != param_result.at(0).get_type_class()) {
            ret = OB_EXPR_CALC_ERROR;
            LOG_WDIAG("unsupported first param type of to_char", K(param_result.at(0).get_type_class()));
          } else if (2 == param_result.count()) {
            if (OB_FAIL(get_varchar_obj(param_result.at(1), second_obj, ctx))) {
              LOG_WDIAG("get varchar obj failed", K(ret));
            } else if (OB_FAIL(second_obj.get_varchar(nls_format))) {
              LOG_WDIAG("get varchar failed", K(ret));
            } else {
              if (!nls_format.empty()) {
                // ObObjCasterV2 only support ObOTimestampTC to format by nls_format, here convert ObDateTimeTC to ObTimestampTZType
                dtc_params.set_nls_format_by_type(ObTimestampTZType, nls_format);
                ObCastCtx cast_ctx_otimestamp(ctx.allocator_, &dtc_params, CM_NULL_ON_WARN, collation);
                if (OB_FAIL(ObObjCasterV2::to_type(ObTimestampTZType, collation, cast_ctx_otimestamp, result_obj, result_obj))) {
                  LOG_WDIAG("cast DateTime to ObTimestampTZTType failed", K(ret));
                }
              } else {
                is_empty_format = true;
              }
            }
          }

          if (OB_SUCC(ret)) {
            ObCastCtx cast_ctx(ctx.allocator_, &dtc_params, CM_NULL_ON_WARN, collation);
            if (param_result.count() == 1) {
              // do nothing
            } else if (is_empty_format) {
              result_obj.set_null();
            } else if (OB_FAIL(ObObjCasterV2::to_type(target_type, collation, cast_ctx, result_obj, result_obj))) {
              LOG_WDIAG("fail to cast obj to timestamp", K(ret), K(result_obj), K(target_type), K(collation));
            }
          }

          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

int ObProxyExprSysdate::calc(const ObProxyExprCtx &ctx,
                             const ObProxyExprCalcItem &calc_item,
                             common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() > 0)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("sysdate should have no param", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        if (OB_SUCC(ret)) {
          ObObjType target_type = ObDateTimeType;

          int64 now = common::ObTimeUtility::current_time();
          // calc timezone offset to correct the time zone info
          int64 gm_time_buf = (int64)((time_t)now - mktime(gmtime((time_t *)&now))) * 1000000;

          ObObj result_obj;
          result_obj.set_timestamp(now + gm_time_buf);
          ObTimeZoneInfo tz_info;
          ObCollationType collation = get_collation(ctx);
          if (OB_SUCC(ret)) {
            ObCastCtx cast_ctx(ctx.allocator_, NULL, CM_NULL_ON_WARN, collation);
            if (OB_FAIL(ObObjCasterV2::to_type(target_type, collation, cast_ctx, result_obj, result_obj))) {
              LOG_WDIAG("fail to cast obj to timestamp", K(ret), K(result_obj), K(target_type), K(collation));
            } else if (OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WDIAG("result obj array push back failed", K(ret));
            }
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

/*
 * refer to the implementation of oracle
 * mod calculate: MOD(n1, n2) = n1 - n2 * FLOOR(n1/n2)
 *                when ( n2 = 0 ), return n1
 */
int ObProxyExprMod::calc(const ObProxyExprCtx &ctx,
                         const ObProxyExprCalcItem &calc_item,
                         common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (-1 != index_ && ObProxyExprCalcItem::FROM_OBJ_ARRAY == calc_item.source_ && !has_agg_) {
    if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
      LOG_WDIAG("calc expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && len == result_obj_array.count()) {
    if (2 != param_array_.count()) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("mod should have two param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(
        calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param result failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj obj1 = param_result.at(0);
          ObObj obj2 = param_result.at(1);
          ObObj result_obj;

          if (ObDoubleTC == obj1.get_type_class() ||
              ObDoubleTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObDoubleTC, ObDoubleType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get double obj failed", K(obj1), K(obj2), K(ret));
            } else if (fabs(obj2.get_double()) < DBL_EPSILON) {
              result_obj.set_double(obj1.get_double());
            } else {
              double obj1_d = obj1.get_double();
              double obj2_d = obj2.get_double();
              result_obj.set_double(obj1_d - obj2_d * floor(obj1_d / obj1_d));
            }
          } else if (ObFloatTC == obj1.get_type_class() || ObFloatTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObFloatTC, ObFloatType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get float obj failed", K(obj1), K(obj2), K(ret));
            } else if (fabs(obj2.get_float()) < FLT_EPSILON) {
              result_obj.set_float(obj1.get_float());
            } else {
              float obj1_f = obj1.get_float();
              float obj2_f = obj2.get_float();
              result_obj.set_float(obj1_f - obj2_f * floor(obj1_f / obj1_f));
            }
          } else {
            number::ObNumber res_nmb;
            if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WDIAG("get number obj failed", K(ret), K(obj1), K(obj2));
            } else if (obj2.get_number().is_zero()) {
              result_obj.set_number(obj1.get_number());
            } else if (OB_FAIL(obj1.get_number().rem(obj2.get_number(), res_nmb, *ctx.allocator_))) {
              LOG_WDIAG("failed to mod numbers", K(ret), K(obj1), K(obj2));
            } else if (OB_SUCC(ret)) {
              result_obj.set_number(res_nmb);
            }
          }

          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

/*
  refer to the implementation of mysql
  if param is null return 1 else 0
*/
int ObProxyExprIsnull::calc(const ObProxyExprCtx &ctx,
                            const ObProxyExprCalcItem &calc_item,
                            common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() != 1)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("isnull should have one param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj result_obj;
          ObObj obj = param_result.at(0);
            //mysql: only null expr treated as null
          if (obj.is_null()) {
            result_obj.set_int(1);
          } else{
            result_obj.set_int(0);
          }
          if (OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

int ObProxyExprFloor::calc(const ObProxyExprCtx &ctx,
                           const ObProxyExprCalcItem &calc_item,
                           common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() != 1)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("ceil should have one param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj result_obj;
          ObObj obj = param_result.at(0);
          if(obj.is_null()) {
            result_obj.set_null();
          } else if (ObIntTC != obj.get_type_class()) {
            number::ObNumber res_nmb;
            if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(ctx.allocator_, obj, obj)))) {
              LOG_WDIAG("get number obj failed", K(obj), K(ret));
            } else if (OB_FAIL(res_nmb.from(obj.get_number(), *ctx.allocator_))) {
              LOG_WDIAG("get number from obj_number failed", K(ret), K(obj));
            } else if (OB_FAIL(res_nmb.floor(0))) {
              LOG_WDIAG("calc floor for number failed", K(ret), K(res_nmb));
            } else {
              result_obj.set_number(res_nmb);
            }
          } else if (OB_FAIL(get_int_obj(param_result.at(0), obj, ctx))) {
            LOG_WDIAG("get int obj failed", K(ret));
          } else {
            result_obj.set_int(obj.get_int());
          }
          
          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

int ObProxyExprCeil::calc(const ObProxyExprCtx &ctx,
                         const ObProxyExprCalcItem &calc_item,
                         common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() != 1)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("floor should have one param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj result_obj;
          ObObj obj = param_result.at(0);
          if(obj.is_null()) {
            result_obj.set_null();
          } else if (ObIntTC != obj.get_type_class()) {
            number::ObNumber res_nmb;
            if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(ctx.allocator_, obj, obj)))) {
              LOG_WDIAG("get number obj failed", K(obj), K(ret));   
            } else if (OB_FAIL(res_nmb.from(obj.get_number(), *ctx.allocator_))) {
              LOG_WDIAG("get number from obj_number failed", K(ret), K(obj));
            } else if (OB_FAIL(res_nmb.ceil(0))) {
              LOG_WDIAG("calc floor for number failed", K(ret), K(res_nmb));
            } else {
              result_obj.set_number(res_nmb);
            }
          } else if (OB_FAIL(get_int_obj(param_result.at(0), obj, ctx))) {
            LOG_WDIAG("get int obj failed", K(ret));
          } else {
            result_obj.set_int(obj.get_int());
          }
          
          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

/*
  only support round(number,d) in oracle and mysql
  ROUND(n, integer) = 
              FLOOR(n * POWER(10, integer) + 0.5) * POWER(10, -integer)
*/
int ObProxyExprRound::calc(const ObProxyExprCtx &ctx,
                         const ObProxyExprCalcItem &calc_item,
                         common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() != 1) && OB_UNLIKELY(param_array_.count() != 2)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("round should have one/two param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj result_obj;
          ObObj obj = param_result.at(0);
          int64_t scale = 0;

          if (2 == param_result.count()) {
            ObObj obj_tmp;
            if (OB_FAIL(get_int_obj(param_result.at(1), obj_tmp, ctx))) {
              LOG_WDIAG("get int obj failed", K(ret));
            } else if (OB_FAIL(obj_tmp.get_int(scale))) {
              LOG_WDIAG("get int failed", K(ret), K(scale));
            }
          }

          if (OB_SUCC(ret)) {
            if(obj.is_null()) {
              result_obj.set_null();
            } else if (ObIntTC != obj.get_type_class()) {
              number::ObNumber res_nmb;
              if (ctx.is_oracle_mode) {
                scale = scale < 0 ? max(number::ObNumber::MIN_SCALE, scale) 
                                  : min(number::ObNumber::MAX_SCALE, scale);
              } else {
                scale = scale < 0 ? max((-1) * OB_MAX_DECIMAL_SCALE, scale) 
                                  : min(OB_MAX_DECIMAL_SCALE, scale);
              }
              if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(ctx.allocator_, obj, obj)))) {
                LOG_WDIAG("get number obj failed", K(ret), K(obj));
              } else if (OB_FAIL(res_nmb.from(obj.get_number(), *ctx.allocator_))) {
                LOG_WDIAG("get number from obj_number failed", K(ret), K(obj));
              } else if (OB_FAIL(res_nmb.round(scale))) {
                LOG_WDIAG("calc round of number failed", K(ret), K(res_nmb), K(scale));
              } else {
                result_obj.set_number(res_nmb);
              }
            } else {
              if (OB_FAIL(get_int_obj(param_result.at(0), obj, ctx))) {
                LOG_WDIAG("get int obj failed", K(ret));
              } else {
                int64_t val = obj.get_int();
                int64_t res = 0;
                bool sign = val < 0;
                if (sign) {
                  val = -val;
                }
                if (scale >= 0) {
                  res = val;
                } else {
                  /*
                    in ob_expr_parser.l:
                      limit the length of int_num from 1 to 17 in case of int64_t out of bound, 
                      17 -> [ ( length of 2^64 ) - 2 ]
                  */
                  if (abs(scale) >= 17) {
                    res = 0;
                  } else {
                    int64_t div = static_cast<int64_t>(pow(10, abs(scale)));
                    int64_t tmp = val / div * div;
                    res = (val - tmp) >= (div / 2) ? tmp + div : tmp;
                  }
                }
                res = sign ? -res : res;
                LOG_DEBUG("round int:", K(val), K(scale), K(res));
                result_obj.set_int(res);
              }
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

/*
  only support truncate in mysql and trunc(number) in oracle
*/
int ObProxyExprTruncate::calc(const ObProxyExprCtx &ctx,
                         const ObProxyExprCalcItem &calc_item,
                         common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() != 1) && OB_UNLIKELY(param_array_.count() != 2)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("trunc/truncate should have one/two param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj result_obj;
          ObObj obj = param_result.at(0);
          int64_t scale = 0;

          if (2 == param_result.count()) {
            ObObj obj_tmp;
            if (OB_FAIL(get_int_obj(param_result.at(1), obj_tmp, ctx))) {
              LOG_WDIAG("get int obj failed", K(ret));
            } else if (OB_FAIL(obj_tmp.get_int(scale))) {
              LOG_WDIAG("get int failed", K(ret), K(scale));
            }
          }

          if(obj.is_null()) {
            result_obj.set_null();
          } else if (ObIntTC != obj.get_type_class()) {
            number::ObNumber res_nmb;
            if (ctx.is_oracle_mode) {
              scale = scale < 0 ? max((-1) * number::ObNumber::MAX_PRECISION, scale) 
                                : min(number::ObNumber::MAX_SCALE, scale);
            } else {
              scale = scale < 0 ? max((-1) * OB_MAX_DECIMAL_PRECISION, scale) 
                                : min(OB_MAX_DECIMAL_SCALE, scale);
            }
            if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(ctx.allocator_, obj, obj)))) {
              LOG_WDIAG("get number obj failed", K(obj), K(ret));
            } else if (OB_FAIL(res_nmb.from(obj.get_number(), *ctx.allocator_))) {
              LOG_WDIAG("get number from obj_number failed", K(ret), K(obj));
            } else if (OB_FAIL(res_nmb.trunc(scale))) {
              LOG_WDIAG("trunc number failed", K(ret), K(res_nmb), K(scale));
            } else {
              result_obj.set_number(res_nmb);
            }
          } else {
            if (OB_FAIL(get_int_obj(param_result.at(0), obj, ctx))) {
              LOG_WDIAG("get int obj failed", K(ret));
            } else {
              int64_t res = 0;
              int64_t val = obj.get_int();
              if (scale >= 0) {
                res = val;
              } else {
                /*
                  the reason for using the number 17 is the same as round() 
                */
                if (abs(scale) > 17) {
                  res = 0;
                } else {
                  const int64_t div = static_cast<int64_t>(pow(10, abs(scale)));
                  res = (val / div) * div;
                }
              }
              result_obj.set_int(res);
            }
          }
          
          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

int ObProxyExprAbs::calc(const ObProxyExprCtx &ctx,
                         const ObProxyExprCalcItem &calc_item,
                         common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();
  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() != 1)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("abs should have one param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj result_obj;
          ObObj obj = param_result.at(0);

          if(obj.is_null()) {
            result_obj.set_null();
          } else if (ObIntTC != obj.get_type_class()) {
            number::ObNumber res_nmb;
            if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(ctx.allocator_, obj, obj)))) {
              LOG_WDIAG("get number obj failed", K(ret), K(obj));
            } else {
              if (obj.get_number().is_negative()) {
                if (OB_FAIL(obj.get_number().negate(res_nmb, *ctx.allocator_))) {
                  LOG_WDIAG("calc abs number failed", K(ret), K(obj));
                } else {
                  result_obj.set_number(res_nmb);
                }
              } else {
                result_obj = obj;
              }
            } 
          } else {
            if (OB_FAIL(get_int_obj(param_result.at(0), obj, ctx))) {
              LOG_WDIAG("get int obj failed", K(ret));
            } else {
              int64_t val = obj.get_int();
              result_obj.set_int(abs(val));
            }
          }
          
          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

/*
 * for oracle return type is timestamp with time zone
 */
int ObProxyExprSystimestamp::calc(const ObProxyExprCtx &ctx,
                                   const ObProxyExprCalcItem &calc_item,
                                   common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();
  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() > 1)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("systimestamp should have no param", K(ret));
    } else {
      int i = 0;
      do {
        int64_t now = common::ObTimeUtility::current_time();
        // calc timezone offset to correct the time zone info
        int32_t offset_sec = (int32_t)((time_t)now - mktime(gmtime((time_t *)&now)));
        ObObj result_obj;
        ObTimeConverter::trunc_datetime(OB_MAX_TIMESTAMP_TZ_PRECISION, now);
        result_obj.set_timestamp(now + offset_sec);
        if (OB_FAIL(get_time_obj(result_obj, result_obj, ctx, ObTimestampTZType))) {
          LOG_WDIAG("fail to convert oracle timestamp with timezone", K(ret));
        } else if (OB_FAIL(result_obj_array.push_back(result_obj))) {
          LOG_WDIAG("result obj array push back failed", K(ret));
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}
/*
 * oracle returns the current date in the session time zone, type is datetime
 * mysql returns the current date, type is date
 */
int ObProxyExprCurrentdate::calc(const ObProxyExprCtx &ctx,
                                 const ObProxyExprCalcItem &calc_item,
                                 common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();
  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() > 1)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("current_date should have no param", K(ret));
    } else {
      int i = 0;
      do {
        int64_t now = common::ObTimeUtility::current_time();
        // calc timezone offset to correct the time zone info
        ObObj result_obj;
        ObTimeZoneInfo tz_info;
        ObCollationType collation = get_collation(ctx);
        ObTimeConverter::trunc_datetime(OB_MAX_DATE_PRECISION, now);
        result_obj.set_timestamp(now);
        if (!ctx.is_oracle_mode) {
          if (OB_FAIL(get_time_obj(result_obj, result_obj, ctx, ObDateType))) {
            LOG_WDIAG("fail to cast obj to date", K(ret), K(result_obj), K(collation));
          }
        } else {
          if (OB_FAIL(get_time_obj(result_obj, result_obj, ctx, ObDateTimeType))){
            LOG_WDIAG("fail to get otimestamp obj", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
          LOG_WDIAG("result obj array push back failed", K(ret));
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}
/*
 * only for mysql: return the current time, type is time
 */
int ObProxyExprCurrenttime::calc(const ObProxyExprCtx &ctx,
                                 const ObProxyExprCalcItem &calc_item,
                                 common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();
  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() > 1)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("current_time should have no param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {

        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);
        if (OB_SUCC(ret)) {
          int16_t scale = OB_MAX_DATETIME_PRECISION;
          if (param_result.count() == 1) {
            ObObj scale_obj;
            scale_obj = param_result.at(0);
            // scale only support int
            if (scale_obj.get_type_class() == ObIntTC) {
              scale = scale_obj.get_smallint();
            }
          }

          int64_t now = common::ObTimeUtility::current_time();
          // calc timezone offset to correct the time zone info
          ObObj result_obj;
          ObTimeConverter::trunc_datetime(scale, now);
          result_obj.set_timestamp(now);
          if (OB_FAIL(get_time_obj(result_obj, result_obj, ctx, ObTimeType))) {
            LOG_WDIAG("fail to convert oracle timestamp with timezone", K(ret));
          } else if (OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}
/*
 * mysql : return the current date and time, type is datetime
 * oracle: return the current date and time in the session time zone, 
 *         type is timestamp with time zone
 */
int ObProxyExprCurrenttimestamp::calc(const ObProxyExprCtx &ctx,
                                 const ObProxyExprCalcItem &calc_item,
                                 common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();
  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() > 1)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("current_timestamp should have no param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    }  else {
      int i = 0;
      do {

        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);
        if (OB_SUCC(ret)) {
          int16_t scale = OB_MAX_DATETIME_PRECISION;
          if (param_result_array.count() == 1) {
            ObObj scale_obj;
            scale_obj = param_result.at(0);
            // scale only support int
            if (scale_obj.get_type_class() == ObIntTC) {
              scale = scale_obj.get_smallint();
            }
          } else if (ctx.is_oracle_mode) {
            scale = OB_MAX_TIMESTAMP_TZ_PRECISION;
          }

          int64_t now = common::ObTimeUtility::current_time();
          // calc timezone offset to correct the time zone info
          ObObj result_obj;
          ObTimeZoneInfo tz_info;
          ObDataTypeCastParams dtc_params;
          ObTimeConverter::trunc_datetime(scale, now);
          result_obj.set_timestamp(now);
          if (ctx.is_oracle_mode) {
            if (OB_FAIL(get_time_obj(result_obj, result_obj, ctx, ObTimestampTZType))) {
              LOG_WDIAG("fail to convert oracle timestamp with timezone", K(ret));
            }
          } else {
            if (OB_FAIL(get_time_obj(result_obj, result_obj, ctx, ObDateTimeType))) {
              LOG_WDIAG("fail to convert oracle timestamp with timezone", K(ret));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

/*
 * support oracle and mysql ltrim()
 * func trim don't need to consider multi-byte collation
 */
int ObProxyExprLtrim::calc(const ObProxyExprCtx &ctx, 
                           const ObProxyExprCalcItem &calc_item,
                           common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (ctx.is_oracle_mode && OB_UNLIKELY(1 != param_array_.count() && 2 != param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("ltrim have wrong param number", K(ret), K(param_array_.count()));
    } else if (!ctx.is_oracle_mode && OB_UNLIKELY(1 != param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("ltrim have wrong param number", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("ltrim calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObString value_str;
          ObString result_str;
          ObString pattern_str = " ";
          ObObj value_obj;
          ObObj result_obj;
          ObCollationType collation = get_collation(ctx);
          bool is_invalid_params = false;

          if (OB_FAIL(get_varchar_obj(param_result.at(0), value_obj, ctx))) {
            LOG_WDIAG("get varchar obj failed", K(ret));
          } else if (OB_FAIL(value_obj.get_varchar(value_str))) {
            LOG_WDIAG("get varchar failed", K(ret), K(value_obj));
          } else if (value_str.empty()) {
            is_invalid_params = true;
            LOG_DEBUG("rtrim first param is emtpy", K(ret));
          }
          if (OB_SUCC(ret) && 2 == param_result.count()){
            ObObj obj;
            if (OB_FAIL(get_varchar_obj(param_result.at(1), obj, ctx))) {
              LOG_WDIAG("get varchar obj failed", K(ret));
            } else if (OB_FAIL(obj.get_varchar(pattern_str))) {
              LOG_WDIAG("get varchar failed", K(ret), K(obj));
            } else if (pattern_str.empty()) {
              is_invalid_params = true;
              LOG_DEBUG("for oracle rtrim second param is emtpy", K(ret));
            }
          }
          int32_t value_str_len = value_str.length();
          int32_t pattern_str_len = pattern_str.length();
          int32_t start = 0;
          for (int32_t i = 0; OB_SUCC(ret) && i <= value_str_len - pattern_str_len; i += pattern_str_len) {
            if (0 == MEMCMP(value_str.ptr() + i, pattern_str.ptr(), pattern_str_len)) {
              start += pattern_str_len;
            } else {
              break;
            }
          }//end for
          if (value_str_len - start < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpected ltrim calculator", K(ret), K(value_str_len), K(start));
          }
          if (OB_SUCC(ret)) {
            if (is_invalid_params || (value_str_len - start == 0)) {
              if (ctx.is_oracle_mode) {
                result_obj.set_null();
              } else {
                result_obj.set_varchar(ObString());
              }
            } else {
              result_str.assign_ptr(value_str.ptr() + start, static_cast<int32_t>(value_str_len - start));
              result_obj.set_varchar(result_str);
              result_obj.set_collation_type(collation);
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  LOG_DEBUG("proxy expr ltrim", K(ret));
  return ret;
}

/*
 * support oracle and mysql rtrim
 * func trim don't need to consider multi-byte collation
 */
int ObProxyExprRtrim::calc(const ObProxyExprCtx &ctx, 
                           const ObProxyExprCalcItem &calc_item,
                           common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (ctx.is_oracle_mode && OB_UNLIKELY(1 != param_array_.count() && 2 != param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("rtrim have wrong param number", K(ret), K(param_array_.count()));
    } else if (!ctx.is_oracle_mode && OB_UNLIKELY(1 != param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("rtrim have wrong param number", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("rtrim calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObString value_str;
          ObString result_str;
          ObString pattern_str = " ";
          ObObj value_obj;
          ObObj result_obj;
          ObCollationType collation = get_collation(ctx);
          bool is_invalid_params = false;

          if (OB_FAIL(get_varchar_obj(param_result.at(0), value_obj, ctx))) {
            LOG_WDIAG("get varchar obj failed", K(ret));
          } else if (OB_FAIL(value_obj.get_varchar(value_str))) {
            LOG_WDIAG("get varchar failed", K(ret), K(value_obj));
          } else if (value_str.empty()) {
            is_invalid_params = true;
            LOG_DEBUG("rtrim first param is emtpy", K(ret));
          }
          if (OB_SUCC(ret) && 2 == param_result.count()){
            ObObj obj;
            if (OB_FAIL(get_varchar_obj(param_result.at(1), obj, ctx))) {
              LOG_WDIAG("get varchar obj failed", K(ret));
            } else if (OB_FAIL(obj.get_varchar(pattern_str))) {
              LOG_WDIAG("get varchar failed", K(ret), K(obj));
            } else if (pattern_str.empty()) {
              is_invalid_params = true;
              LOG_DEBUG("for oracle rtrim second param is emtpy", K(ret));
            }
          }
          int32_t value_str_len = value_str.length();
          int32_t pattern_str_len = pattern_str.length();
          int32_t end = value_str_len;
          for (int32_t i = value_str_len - pattern_str_len; OB_SUCC(ret) && i >= 0; i -= pattern_str_len) {
            if (0 == MEMCMP(value_str.ptr() + i, pattern_str.ptr(), pattern_str_len)) {
              end -= pattern_str_len;
            } else {
              break;
            }
          }
          if (end < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpected ltrim calculator", K(ret), K(value_str_len), K(end));
          }
          if (OB_SUCC(ret)) {
            if (is_invalid_params || end == 0) {
              if (ctx.is_oracle_mode) {
                result_obj.set_null();
              } else {
                result_obj.set_varchar(ObString());
              }
            } else {
              result_str.assign_ptr(value_str.ptr(), end);
              result_obj.set_varchar(result_str);
              result_obj.set_collation_type(collation);
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  LOG_DEBUG("proxy expr rtrim", K(ret));
  return ret;
}

/*
* support mysql and oracle trim
* in expr_parser
* trim(' a ')
* trim(TRAILING 'a' from 'aba')
* trim(TRAILING from 'aba')
* trim(BOTH 'a' from 'aba')
* trim(BOTH from 'aba')
* trim(TAILING 'a' from 'aba')
* trim(TAILING from 'aba')
* support oracle and mysql ltrim()
* func trim don't need to consider multi-byte collation
*/
int ObProxyExprTrim::calc(const ObProxyExprCtx &ctx, 
                           const ObProxyExprCalcItem &calc_item,
                           common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(0 >= param_array_.count() || 4 <= param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("trim param count is wrong", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("trim calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);
        if (OB_SUCC(ret)) {
          ObObj result_obj;
          ObObj src_obj;
          ObString result_str;
          ObString src;
          ObString remove_str;
          char buf[] =" ";
          int64_t start = 0;
          int64_t end = 0;
          int64_t mode = 0;
          // if param is null return null
          bool is_invalid_params = false;
          remove_str.assign_ptr(buf, 1);

          if (OB_FAIL(get_varchar_obj(param_result.at(param_result.count() - 1), src_obj, ctx))) {
            LOG_WDIAG("get varchar obj failed", K(ret));
          } else if (OB_FAIL(src_obj.get_varchar(src))) {
            LOG_WDIAG("get varchar failed", K(ret), K(src_obj));
          } else if (src.empty()) {
            is_invalid_params = true;
            LOG_WDIAG("trim src param is emtpy", K(ret));
          }

          if (OB_SUCC(ret)) {
            if (3 == param_result.count()) {
              ObObj obj1;
              ObObj obj2;
              if (OB_FAIL(get_int_obj(param_result.at(0), obj1, ctx))) {
                  LOG_WDIAG("get int obj failed", K(ret));
              } else if (OB_FAIL(obj1.get_int(mode))) {
                  LOG_WDIAG("get int failed", K(ret), K(obj1));
              } else if (OB_FAIL(get_varchar_obj(param_result.at(1), obj2, ctx))) {
                LOG_WDIAG("get varchar obj failed", K(ret));
              } else if (OB_FAIL(obj2.get_varchar(remove_str))) {
                LOG_WDIAG("get varchar failed", K(ret), K(obj2));
              }
            } else if (2 == param_result.count()) {
              ObObj obj;
              if (OB_FAIL(get_int_obj(param_result.at(0), obj, ctx))) {
                LOG_WDIAG("get int obj failed", K(ret));
              } else if (OB_FAIL(obj.get_int(mode))) {
                LOG_WDIAG("get int failed", K(ret), K(obj));
              } else if (0 > mode || 3 <= mode){
                LOG_WDIAG("trim mode is wrong", K(ret), K(mode));
              }
            }
          }

          ObCollationType collation = get_collation(ctx);
          if (OB_SUCC(ret)) {
            ObCastCtx cast_ctx(ctx.allocator_, NULL, CM_NULL_ON_WARN, collation);
            if (ctx.is_oracle_mode && !remove_str.empty()) {
              size_t byte_index = ObCharset::strlen_char(collation, remove_str.ptr(), remove_str.length());
              if (1 < byte_index) {
                ret =  OB_EXPR_CALC_ERROR;
                LOG_WDIAG("for orcale trim_character length is not 1", K(ret), K(remove_str));
              }
            }
            if (OB_SUCC(ret)) {
              end = src.length();
              if (OB_SUCC(ret) && !remove_str.empty()) {
                end = src.length();
                int64_t src_len = src.length();
                int64_t remove_len = remove_str.length();
                if (0 == mode || 1 == mode) {
                  for (int64_t i = 0; i <= src_len - remove_len; i += remove_len) {
                    if (0 == MEMCMP(src.ptr() + i, remove_str.ptr(), remove_len)) {
                      start += remove_len;
                    } else {
                      break;
                    }
                  }
                }
                if (0 == mode || 2 == mode) {
                  for (int64_t i = src_len - remove_len; i >= 0; i -= remove_len) {
                    if (0 == MEMCMP(src.ptr() + i, remove_str.ptr(), remove_len)) {
                      end -= remove_len;
                    } else {
                      break;
                    }
                  }
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (is_invalid_params || end <= start || (end - start == 0)) {
              if (ctx.is_oracle_mode) {
                result_obj.set_null();
              } else {
                result_obj.set_varchar(ObString());
              }
            } else {
              result_str.assign_ptr(src.ptr() + start, static_cast<int32_t>(end - start));
              result_obj.set_varchar(result_str);
              result_obj.set_collation_type(collation);
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  LOG_DEBUG("proxy expr trim", K(ret));
  return ret;
}

int ObProxyExprReplace::calc(const ObProxyExprCtx &ctx,
                            const ObProxyExprCalcItem &calc_item,
                            common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(!ctx.is_oracle_mode && 3 != param_array_.count()) || OB_UNLIKELY(2 != param_array_.count() && 3 != param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("replace has wrong param number", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);

        if (OB_SUCC(ret)) {
          ObObj result_obj;
          ObObj obj;

          ObString value_str;
          ObString from_str;
          ObString to_str;
          ObString result_str;
          ObCollationType collation = get_collation(ctx);
          if (OB_FAIL(get_varchar_obj(param_result.at(0), obj, ctx))) {
            LOG_WDIAG("get varchar obj failed", K(ret));
          } else if (OB_FAIL(obj.get_varchar(value_str))) {
            LOG_WDIAG("get varchar failed", K(ret), K(obj));
          } else if (OB_FAIL(get_varchar_obj(param_result.at(1), obj, ctx))) {
            LOG_WDIAG("get varchar obj failed", K(ret));
          } else if (OB_FAIL(obj.get_varchar(from_str))) {
            LOG_WDIAG("get varchar failed", K(ret), K(obj));
          } else if (value_str.empty() || from_str.empty() || OB_UNLIKELY(value_str.length() < from_str.length())) {
            // do nothing
            result_str = value_str;
          } else {
            if (3 == param_result.count()) {
              if (OB_FAIL(get_varchar_obj(param_result.at(2), obj, ctx))) {
                LOG_WDIAG("get varchar obj failed", K(ret));
              } else if (OB_FAIL(obj.get_varchar(to_str))) {
                LOG_WDIAG("get varchar failed", K(ret), K(obj));
              }
            } 

            if (OB_SUCC(ret)) {
              ObCastCtx cast_ctx(ctx.allocator_, NULL, CM_NULL_ON_WARN, collation);
              if (OB_UNLIKELY(from_str == to_str)){
                result_str = value_str;
              } else {
                int64_t length_origin = value_str.length();
                int64_t length_from = from_str.length();
                int64_t length_to = to_str.length();
                int64_t length_res = 0;

                int64_t cnt = 0;
                // record ptr offset of each matched text  
                ObSEArray<int64_t, 4> locations(common::ObModIds::OB_SQL_EXPR_REPLACE, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
                ObString mb;
                int32_t wc;
                ObStringScanner scanner(value_str, collation, ObStringScanner::IGNORE_INVALID_CHARACTER);
                while (OB_SUCC(ret) && scanner.get_remain_str().length() >= from_str.length()) {
                  if (0 == MEMCMP(scanner.get_remain_str().ptr(), from_str.ptr(), from_str.length())) {
                    ret = locations.push_back(scanner.get_remain_str().ptr() - value_str.ptr());
                    scanner.forward_bytes(from_str.length());
                  } else if (OB_FAIL(scanner.next_character(mb, wc))) {
                    LOG_WDIAG("get next character failed", K(ret));
                  }
                }

                if (OB_SUCC(ret)) {
                  cnt = locations.count();
                  length_res = length_origin + (length_to - length_from) *cnt;
                  if (0 == cnt || length_res == 0) {
                    // do nothing
                    result_str = value_str;
                  } else {
                    char *buf = NULL;
                    if (OB_ISNULL(buf = ((static_cast<char*>(ctx.allocator_->alloc(length_res)))))) {
                      ret = OB_ALLOCATE_MEMORY_FAILED;
                      LOG_WDIAG("alloc failed", K(ret), K(length_res));
                    } else {
                      int64_t pos = 0;
                      int64_t value_str_pos = 0;
                      for (int i = 0; OB_SUCC(ret) && i < locations.count(); i++) {
                        MEMCPY(buf + pos, value_str.ptr() + value_str_pos, locations.at(i) - value_str_pos);
                        pos += locations.at(i) - value_str_pos;
                        value_str_pos = locations.at(i);
                        MEMCPY(buf + pos, to_str.ptr(), to_str.length());
                        pos += to_str.length();
                        value_str_pos += from_str.length();
                      }
                      if (OB_SUCC(ret) && value_str_pos < value_str.length()) {
                        MEMCPY(buf + pos, value_str.ptr() + value_str_pos, value_str.length() - value_str_pos);
                      }
                      result_str.assign_ptr(buf, static_cast<int32_t>(length_res));
                    }
                  }
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (result_str.empty() && ctx.is_oracle_mode) {
              result_obj.set_null();
            } else {
              result_obj.set_varchar(result_str);
              result_obj.set_collation_type(collation);
            }
            if (OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WDIAG("result obj array push back failed", K(ret));
            }
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  LOG_DEBUG("proxy expr replace", K(ret));
  return ret;
}

int ObProxyExprLength::calc(const ObProxyExprCtx &ctx,
                            const ObProxyExprCalcItem &calc_item,
                            common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(1 != param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("length should have only one param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);
        if (OB_SUCC(ret)) {
          ObString value;
          ObObj first_obj;
          ObObj result_obj;
          if (OB_FAIL(get_varchar_obj(param_result.at(0), first_obj, ctx))) {
            LOG_WDIAG("get varchar obj failed", K(ret));
          } else if (OB_FAIL(first_obj.get_varchar(value))) {
            LOG_WDIAG("get varchar failed", K(ret), K(first_obj));
          } 
          ObCollationType collation = get_collation(ctx);
          ObCastCtx cast_ctx(ctx.allocator_, NULL, CM_NULL_ON_WARN, collation);
          if (value.empty()) {
            result_obj.set_int(0);
          } else if (ctx.is_oracle_mode) {
            int64_t len = ObCharset::strlen_char(collation, value.ptr(), static_cast<int64_t>(value.length()));
            result_obj.set_int(len);
          } else {
            result_obj.set_int(static_cast<int64_t>(value.length()));
          }
          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

int ObProxyExprLower::calc(const ObProxyExprCtx &ctx,
                           const ObProxyExprCalcItem &calc_item,
                           common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(1 != param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("lower/lcase should have only one param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);
        if (OB_SUCC(ret)) {
          ObString value;
          ObObj first_obj;
          ObObj result_obj;
          if (OB_FAIL(get_varchar_obj(param_result.at(0), first_obj, ctx))) {
            LOG_WDIAG("get varchar obj failed", K(ret));
          } else if (OB_FAIL(first_obj.get_varchar(value))) {
            LOG_WDIAG("get varchar failed", K(ret), K(first_obj));
          } else if (value.empty()) {
            // do nothing
          } else {
            ObCollationType collation = get_collation(ctx);
            ObCastCtx cast_ctx(ctx.allocator_, NULL, CM_NULL_ON_WARN, collation);
            if (0 == ObCharset::casedn(collation, value)) {
              ret = OB_EXPR_CALC_ERROR;
              LOG_WDIAG("lower failed in charset::casedn", K(value), K(collation));
            } else {
              result_obj.set_varchar(value);
              result_obj.set_collation_type(collation);
            }
          }

          if (OB_SUCC(ret)) {
            if (ctx.is_oracle_mode && value.empty()) {
              result_obj.set_null();
            }
            if OB_FAIL(result_obj_array.push_back(result_obj)) {
              LOG_WDIAG("result obj array push back failed", K(ret));
            }
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

int ObProxyExprUpper::calc(const ObProxyExprCtx &ctx,
                           const ObProxyExprCalcItem &calc_item,
                           common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(1 != param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("upper/Ucase should have only one param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);
        if (OB_SUCC(ret)) {
          ObString value;
          ObObj first_obj;
          ObObj result_obj;
          if (OB_FAIL(get_varchar_obj(param_result.at(0), first_obj, ctx))) {
            LOG_WDIAG("get varchar obj failed", K(ret));
          } else if (OB_FAIL(first_obj.get_varchar(value))) {
            LOG_WDIAG("get varchar failed", K(ret), K(first_obj));
          } else if (value.empty()) {
              // do nothing
          } else {
            ObCollationType collation = get_collation(ctx);
            ObCastCtx cast_ctx(ctx.allocator_, NULL, CM_NULL_ON_WARN, collation);
            size_t size = ObCharset::caseup(collation, value.ptr(), value.length(), value.ptr(), value.length());
            if (0 == size) {
              ret = OB_EXPR_CALC_ERROR;
              LOG_WDIAG("upper failed in charset::caseup", K(value), K(collation));
            } else {
              value.set_length(static_cast<int32_t>(size));
              result_obj.set_varchar(value);
              result_obj.set_collation_type(collation);
            }
          }
          if (OB_SUCC(ret)) {
            if (ctx.is_oracle_mode && value.empty()) {
              result_obj.set_null();
            }
            if (OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WDIAG("result obj array push back failed", K(ret));
            }
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

/*
 * TO_NUMBER(expr [, fmt [, 'nlsparam' ] ])
 * oceanbase not support nlsparam
 */
int ObProxyExprToNumber::calc(const ObProxyExprCtx &ctx,
                              const ObProxyExprCalcItem &calc_item,
                              common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() < 1) || OB_UNLIKELY(param_array_.count() > 3)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("to_number has a minimum of two parameters and a maximum of 3 parameters ", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        LOCATE_PARAM_RESULT(param_result_array, param_result, i);
        if (OB_SUCC(ret)) {
          ObObjType target_type = ObNumberType;
          ObObj expr_obj;
          ObObj format_obj;
          ObString format = "";
          ObObj result_obj;
          common::number::ObNumber res_number;
          
          if (param_result.count() == 1) {
            if (ObNumberTC == param_result.at(0).get_type_class()) {
              result_obj = param_result.at(0);
            } else {
              ObCollationType collation = get_collation(ctx);
              ObCastCtx cast_ctx(ctx.allocator_, NULL, CM_NULL_ON_WARN, CS_TYPE_UTF8MB4_GENERAL_CI);
              if (OB_FAIL(ObObjCasterV2::to_type(target_type, collation, cast_ctx, param_result.at(0), result_obj))) {
                LOG_WDIAG("failed to cast obj", K(ret));
              }            
            }
          } else if (param_result.count() >= 2) {
            if (OB_FAIL(get_varchar_obj(param_result.at(0), expr_obj, ctx))) {
              LOG_WDIAG("get varchar failed", K(ret));
            } else if (OB_FAIL(get_varchar_obj(param_result.at(1), format_obj, ctx))) {
              LOG_WDIAG("get varchar failed", K(ret));
            } else {
              format = format_obj.get_string();
              ObNFMToNumber nfm;
              ObObj nls_currency;
              ObObj nls_iso_currency;
              ObObj nls_dual_currency;
              ObObj nls_numeric_characters;
              if (OB_ISNULL(ctx.client_session_info_)) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WDIAG("fail to calc to_number for null session info", K(ret));
              } else if (OB_FAIL(ctx.client_session_info_->get_sys_variable_value(sql::OB_SV_NLS_CURRENCY, nls_currency))) {
                LOG_WDIAG("fail to get nls_currency", K(ret));
              } else if (OB_FAIL(ctx.client_session_info_->get_sys_variable_value(sql::OB_SV_NLS_ISO_CURRENCY, nls_iso_currency))) {
                LOG_WDIAG("fail to get nls_dual_currency", K(ret));
              } else if (OB_FAIL(ctx.client_session_info_->get_sys_variable_value(sql::OB_SV_NLS_DUAL_CURRENCY, nls_dual_currency))) {
                LOG_WDIAG("fail to get nls_dual_currency", K(ret));
              } else if (OB_FAIL(ctx.client_session_info_->get_sys_variable_value(sql::OB_SV_NLS_NUMERIC_CHARACTERS, nls_numeric_characters))) {
                LOG_WDIAG("fail to get to get nls_numeric_characters");
              } else if (OB_FAIL(nfm.convert_char_to_num(
                            nls_currency.get_string(),
                            nls_iso_currency.get_string(),
                            nls_dual_currency.get_string(),
                            nls_numeric_characters.get_string(),
                            expr_obj.get_string(), format, *ctx.allocator_,
                            res_number))) {
                LOG_WDIAG("fail to convert string to number", K(ret));
              }
              result_obj.set_number(res_number);
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WDIAG("result obj array push back failed", K(ret));
          }
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;
}

int ObProxyExprNULL::calc(const ObProxyExprCtx &ctx,
                          const ObProxyExprCalcItem &calc_item,
                          common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WDIAG("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() > 1)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("to_number has a minimum of two parameters and a maximum of 3 parameters ", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WDIAG("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        ObObj result_obj;
        result_obj.set_null();
        if (OB_FAIL(result_obj_array.push_back(result_obj))) {
          LOG_WDIAG("result obj array push back failed", K(ret));
        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);
  return ret;

}


} // end opsql
} // end obproxy
} // end oceanbase
