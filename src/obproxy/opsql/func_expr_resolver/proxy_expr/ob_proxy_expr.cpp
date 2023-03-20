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
#include "lib/utility/utility.h"
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
      LOG_WARN("failed to cast obj", K(ret));
    }
  }

  if (OB_SUCC(ret) && obj_type_class != right.get_type_class()) {
    if (OB_FAIL(ObObjCasterV2::to_type(obj_type, cast_ctx, right, right))) {
      LOG_WARN("failed to cast obj", K(ret));
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
  UNUSED(ctx);
  if (-1 != index_ && ObProxyExprCalcItem::FROM_OBJ_ARRAY == calc_item.source_) {
    int64_t len = calc_item.obj_array_->count();
    if (index_ >= len
        || OB_FAIL(result_obj_array.push_back(*calc_item.obj_array_->at(index_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("calc expr failed", K(ret), K(index_), K(calc_item.obj_array_->count()));
    }
  }

  return ret;
}

int ObProxyExpr::calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                      common::ObIArray<common::ObObj*> &result_obj_array)
{
  int ret = OB_SUCCESS;

  if (result_obj_array.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cant calc, the last obj is result", K(ret));
  } else {
    ObSEArray<ObObj, 4> tmp_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < result_obj_array.count() - 1; i++) {
      ObObj* tmp_obj = result_obj_array.at(i);
      if (OB_FAIL(tmp_array.push_back(*tmp_obj))) {
        LOG_WARN("tmp array push back failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(calc(ctx, calc_item, tmp_array))) {
         LOG_WARN("calc failed", K(ret));
      } else if (tmp_array.count() != result_obj_array.count()) {
        LOG_WARN("not get result", K(ret));
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
    LOG_WARN("fail to column string", K(ret));
  } else if (!alias_name_.empty()) {
    if (OB_FAIL(sql_string.append(" AS "))){
      LOG_WARN("append failed", K(ret));
    } else {
      ret = sql_string.append_fmt("%.*s", alias_name_.length(), alias_name_.ptr());
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to sql_string", K_(alias_name), K(ret));
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
    LOG_WARN("cacl expr failed", K(ret));
  } else if (result_obj_array.count() == len) {
    if (OB_FAIL(result_obj_array.push_back(obj_))) {
      LOG_WARN("push back obj failed", K(ret));
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
      LOG_WARN("fail to append table name", K(table_name_), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql_string.append_fmt("%.*s", column_name_.length(), column_name_.ptr()))) {
      LOG_WARN("fail to append table name", K(column_name_), K(ret));
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
    LOG_WARN("cacl expr failed", K(ret));
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
              LOG_WARN("push back obj failed", K(ret), K(result_obj));
            }
          } else if (TOKEN_STR_VAL == sql_column_value.value_type_) {
            ObString value = sql_column_value.column_value_.config_string_;
            result_obj.set_varchar(value);
            result_obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
            if (OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WARN("push back obj failed", K(ret), K(result_obj));
            }
          } else {
            ret = OB_EXPR_CALC_ERROR;
            LOG_WARN("sql_column_value value type invalid", K(sql_column_value.value_type_));
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
    LOG_WARN("unexpected null", KP_(expr), K(ret));
  } else if (OB_FAIL(expr_->calc(ctx, calc_item, result_obj_array))) {
    LOG_WARN("fail to calc expr", K(ret));
  }
  return ret;
}

int ObProxyGroupItem::calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                           ObIArray<ObObj*> &result_obj_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KP_(expr), K(ret));
  } else if (OB_FAIL(expr_->calc(ctx, calc_item, result_obj_array))) {
    LOG_WARN("fail to calc expr", K(ret));
  }
  return ret;
}

int ObProxyGroupItem::to_column_string(ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(expr_->to_column_string(sql_string))) {
    LOG_WARN("to column string failed", K(ret));
  }

  return ret;
}

int ObProxyOrderItem::to_sql_string(ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(expr_->to_column_string(sql_string))) {
    LOG_WARN("to sql_string failed", K(ret));
  } else if (order_direction_ == NULLS_FIRST_ASC){
    if (OB_FAIL(sql_string.append(" ASC"))) {
      LOG_WARN("fail to append", K(ret));
    }
  } else if (OB_FAIL(sql_string.append(" DESC"))) {
    LOG_WARN("fail to append", K(ret));
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
      LOG_WARN("expr is null, unexpected", K(ret));
    } else if (OB_FAIL(expr->calc(ctx, calc_item, array))) {
      LOG_WARN("expr cal failed", K(ret));
    } else if (OB_FAIL(param_result.push_back(array))) {
      LOG_WARN("param result push failed", K(ret));
    } else if (array.count() > 1) {
      cnt = static_cast<int>(array.count());
      if (cnt != 1 && cnt != array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("calc param expr failed, two columns have more than one value", K(ret));
      }
    }
  }

  return ret;
}

int ObProxyFuncExpr::get_int_obj(const ObObj &src, ObObj &dst, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (!src.is_int()) {
    ObCollationType collation = ObCharset::get_default_collation(ObCharset::get_default_charset());
    ObCastCtx cast_ctx(&allocator, NULL, CM_NULL_ON_WARN, collation);
    if (OB_FAIL(ObObjCasterV2::to_type(ObIntType, collation, cast_ctx, src, dst))) {
      LOG_WARN("cast obj to varchar obj fail", K(ret));
    }
  } else {
    dst = src;
  } 

  return ret;
}

int ObProxyFuncExpr::get_varchar_obj(const common::ObObj &src, common::ObObj &dst, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (!src.is_varchar()) {
    ObCollationType collation = ObCharset::get_default_collation(ObCharset::get_default_charset());
    ObCastCtx cast_ctx(&allocator, NULL, CM_NULL_ON_WARN, collation);
    if (OB_FAIL(ObObjCasterV2::to_type(ObVarcharType, collation, cast_ctx, src, dst))) {
      LOG_WARN("cast obj to varchar obj fail", K(ret));
    }
  } else if (src.is_varchar()) {
    dst = src;
  }

  return ret;
}

int ObProxyFuncExpr::check_varchar_empty(const common::ObObj& result)
{
  int ret = OB_SUCCESS;
  ObString str;
  if (!result.is_varchar()) {
    ret = OB_EXPR_CALC_ERROR;
    LOG_WARN("check varchar empty failed, result is not varchar", K(ret), K(result));
  } else if (OB_FAIL(result.get_varchar(str))) {
    LOG_WARN("get varchar failed", K(ret));
  } else if (str.empty()) {
    ret = OB_EXPR_CALC_ERROR;
    LOG_WARN("str is empty", K(ret));
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
    LOG_WARN("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() >2 || param_array_.count() < 1)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("hash should have one or two param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WARN("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        for (int64_t j = 0; OB_SUCC(ret) && j < param_result_array.count(); j++) {
          ObObj tmp_obj;
          if (param_result_array.at(j).count() == 1) {
            tmp_obj = param_result_array.at(j).at(0);
          } else {
            tmp_obj = param_result_array.at(j).at(i);
          }
          if (OB_FAIL(param_result.push_back(tmp_obj))) {
            LOG_WARN("push back obj failed", K(ret), K(i), K(j));
          }
        }

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

          if (OB_FAIL(get_int_obj(test_load_obj, obj1, *ctx.allocator_))) {
            LOG_WARN("get int obj failed", K(ret), K(test_load_obj));
          } else if (OB_FAIL(obj1.get_int(index))) {
            LOG_WARN("get int failed", K(ret), K(obj1));
          }

          if (OB_SUCC(ret) && param_array_.count() == 2) {
            ObObj obj2;
            if (OB_FAIL(get_int_obj(param_result.at(1), obj2, *ctx.allocator_))) {
              LOG_WARN("get int obj failed", K(ret));
            } else if (OB_FAIL(obj2.get_int(num))) {
              LOG_WARN("get int failed", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            if (!ctx.is_elastic_index_) {
              if (0 == num) {
                ret = OB_EXPR_CALC_ERROR;
                LOG_WARN("num is 0", K(ret));
              } else {
                index = index % num;
              }
            }
            if (OB_SUCC(ret)) {
              result_obj.set_int(index);
              if (OB_FAIL(result_obj_array.push_back(result_obj))) {
                LOG_WARN("result obj array push back failed", K(ret));
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
    LOG_WARN("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(2 != param_array_.count() && 3 != param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("substr has no param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WARN("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        for (int64_t j = 0; OB_SUCC(ret) && j < param_result_array.count(); j++) {
          ObObj tmp_obj;
          if (param_result_array.at(j).count() == 1) {
            tmp_obj = param_result_array.at(j).at(0);
          } else {
            tmp_obj = param_result_array.at(j).at(i);
          }
          if (OB_FAIL(param_result.push_back(tmp_obj))) {
            LOG_WARN("push back obj failed", K(ret), K(i), K(j));
          }
        }

        if (OB_SUCC(ret)) {
          ObObj result_obj;
          ObString value;
          int64_t value_length = -1;
          int64_t start_pos = 0;
          int64_t substr_len = -1;
          ObObj first_obj;

          // substr retrun NULL on out of bounds params
          bool is_invalid_params = false;

          if (OB_FAIL(get_varchar_obj(param_result.at(0), first_obj, *ctx.allocator_))) {
            LOG_WARN("get varchar obj failed", K(ret));
          } else if (OB_FAIL(first_obj.get_varchar(value))) {
            LOG_WARN("get varchar failed", K(ret), K(first_obj));
          } else if (value.empty()) {
            ret = OB_EXPR_CALC_ERROR;
            LOG_WARN("substr first parm is emtpy", K(ret));
          }

          if (OB_SUCC(ret)) {
            ObObj obj;
            if (OB_FAIL(get_int_obj(param_result.at(1), obj, *ctx.allocator_))) {
              LOG_WARN("get int obj failed", K(ret));
            } else if (OB_FAIL(obj.get_int(start_pos))) {
              LOG_WARN("get int failed", K(ret));
            }
          }
          if (start_pos == 0) {
            if (ctx.is_oracle_mode) {
              // 1 and 0 treated as thr first character in oracle
              start_pos = 1;
            } else {
              is_invalid_params = true;
            }
          }
          if (OB_SUCC(ret) && 3 == param_result.count()) {
            ObObj obj;
            if (OB_FAIL(get_int_obj(param_result.at(2), obj, *ctx.allocator_))) {
              LOG_WARN("get int obj failed", K(ret));
            } else if (OB_FAIL(obj.get_int(substr_len))) {
              LOG_WARN("get int failed", K(ret));
            } else if (substr_len <= 0) {
              is_invalid_params = true;
              LOG_DEBUG("substr function param 3 is less than 1", K(ret));
            }
          }

          if (OB_SUCC(ret) && !is_invalid_params) {
            value_length = value.length();
            if (start_pos < 0) {
              start_pos = value_length + start_pos + 1;
            }

            if (-1 == substr_len || start_pos + substr_len - 1 > value_length) {
              substr_len = value_length - start_pos + 1;
            }

            if (start_pos <= 0 || start_pos > value_length || substr_len <= 0 || substr_len > value_length || start_pos + substr_len - 1 > value_length) {
              is_invalid_params = true;
              LOG_DEBUG("invalid substr params", K(start_pos), K(substr_len), K(value_length), K(value), K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            if(is_invalid_params) {
              if (ctx.is_oracle_mode) {
                result_obj.set_null();
              } else {
                result_obj.set_varchar(ObString());
              }
            } else {
              ObString result_str;
              result_str.assign_ptr(value.ptr() + start_pos - 1, static_cast<int32_t>(substr_len));
              result_obj.set_varchar(result_str);
              result_obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
              if (OB_FAIL(check_varchar_empty(result_obj))) {
                LOG_WARN("check varchar emtpy failed", K(ret));
              } 
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WARN("result obj array push back failed", K(ret));
            }
          }

        }
      } while (OB_SUCC(ret) && ++i < cnt);
    }
  }
  ObProxyExpr::print_proxy_expr(this);

  return ret;
}

int ObProxyExprConcat::calc(const ObProxyExprCtx &ctx,
                            const ObProxyExprCalcItem &calc_item,
                            common::ObIArray<common::ObObj> &result_obj_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> param_result_array;
  int cnt = 0;
  int64_t len = result_obj_array.count();

  if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
    LOG_WARN("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(0 >= param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("concat has no param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WARN("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        for (int64_t j = 0; OB_SUCC(ret) && j < param_result_array.count(); j++) {
          ObObj tmp_obj;
          if (param_result_array.at(j).count() == 1) {
            tmp_obj = param_result_array.at(j).at(0);
          } else {
            tmp_obj = param_result_array.at(j).at(i);
          }
          if (OB_FAIL(param_result.push_back(tmp_obj))) {
            LOG_WARN("push back obj failed", K(ret), K(i), K(j));
          }
        }

        if (OB_SUCC(ret)) {
          ObObj result_obj;
          ObObj first_obj = param_result.at(0);
          ObIAllocator *allocator = ctx.allocator_;

          if (OB_FAIL(get_varchar_obj(param_result.at(0), first_obj, *allocator)))
            {
              LOG_WARN("get varchar obj failed", K(ret));
            }

          for (int64_t i = 1; OB_SUCC(ret) && i < param_result.count(); i++) {
            ObObj tmp_obj;
            ObString str1;
            ObString str2;
            if (OB_FAIL(get_varchar_obj(param_result.at(i), tmp_obj, *ctx.allocator_))) {
              LOG_WARN("get varchar obj failed", K(ret), K(param_result.at(i)));
            } else {
              char *buf = NULL;
              if (OB_FAIL(first_obj.get_varchar(str1))) {
                LOG_WARN("get varchar failed", K(ret), K(first_obj));
              } else if (OB_FAIL(tmp_obj.get_varchar(str2))) {
                LOG_WARN("get varchar failed", K(ret), K(tmp_obj));
              } else if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(str1.length() + str2.length())))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("alloc memory failed", K(str1), K(str2), K(ret));
              } else {
                MEMCPY(buf, str1.ptr(), str1.length());
                MEMCPY(buf + str1.length(), str2.ptr(), str2.length());
                ObString result_str(str1.length() + str2.length(), buf);
                first_obj.set_varchar(result_str);
                first_obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
              }
            }
          }

          if (OB_SUCC(ret)) {
            if (first_obj.is_varchar()) {
              result_obj = first_obj;
            } else {
              ret = OB_EXPR_CALC_ERROR;
              LOG_WARN("invalid type", K(ret), K(first_obj));
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(check_varchar_empty(result_obj))) {
              LOG_WARN("check varchar empty failed", K(ret));
            } else if (OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WARN("result obj array push back failed", K(ret));
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
    LOG_WARN("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (1 != param_array_.count()) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("toint should have one param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WARN("calc param result failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        for (int64_t j = 0; OB_SUCC(ret) && j < param_result_array.count(); j++) {
          ObObj tmp_obj;
          if (param_result_array.at(j).count() == 1) {
            tmp_obj = param_result_array.at(j).at(0);
          } else {
            tmp_obj = param_result_array.at(j).at(i);
          }
          if (OB_FAIL(param_result.push_back(tmp_obj))) {
            LOG_WARN("push back obj failed", K(ret), K(i), K(j));
          }
        }
        if (OB_SUCC(ret)) {
          ObObj result_obj;
          ObObj &obj = param_result.at(0);
          if (OB_FAIL(get_int_obj(obj, result_obj, *ctx.allocator_))) {
            LOG_WARN("get int obj failed", K(ret));
          } else if (OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WARN("result obj array push back failed", K(ret));
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
      LOG_WARN("calc expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && len == result_obj_array.count()) {
    if (2 != param_array_.count()) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("div should have two param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WARN("calc param result failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        for (int64_t j = 0; OB_SUCC(ret) && j < param_result_array.count(); j++) {
          ObObj tmp_obj;
          if (param_result_array.at(j).count() == 1) {
            tmp_obj = param_result_array.at(j).at(0);
          } else {
            tmp_obj = param_result_array.at(j).at(i);
          }
          if (OB_FAIL(param_result.push_back(tmp_obj))) {
            LOG_WARN("push back obj failed", K(ret), K(i), K(j));
          }
        }

        if (OB_SUCC(ret)) {
          ObObj obj1 = param_result.at(0);
          ObObj obj2 = param_result.at(1);
          ObObj result_obj;
          if (ObDoubleTC == obj1.get_type_class() || ObDoubleTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObDoubleTC, ObDoubleType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WARN("get double obj failed", K(obj1), K(obj2), K(ret));
            } else if (fabs(obj2.get_double()) < DBL_EPSILON) {
              result_obj.set_null();
            } else {
              double obj1_d = obj1.get_double();
              double obj2_d = obj2.get_double();
              result_obj.set_double(obj1_d / obj2_d);
            }
          } else if (ObFloatTC == obj1.get_type_class() || ObFloatTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObFloatTC, ObFloatType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WARN("get float obj failed", K(obj1), K(obj2), K(ret));
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
              LOG_WARN("get number obj failed", K(ret), K(obj1), K(obj2));
            } else if (OB_UNLIKELY(obj2.get_number().is_zero())) {
              result_obj.set_null();
            } else if (OB_FAIL(obj1.get_number().div(obj2.get_number(), res_nmb, *ctx.allocator_))) {
              LOG_WARN("failed to div numbers", K(ret), K(obj1), K(obj2));
            } else {
              if (ctx.scale_ >= 0 || accuracy_.get_scale() >= 0) {
                int64_t scale = ctx.scale_ >= 0 ? ctx.scale_ : accuracy_.get_scale();
                if (OB_FAIL(res_nmb.round(scale))) {
                  LOG_WARN("failed to round result number", K(res_nmb),
                           K(scale), K(accuracy_), K(ctx.scale_), K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                result_obj.set_number(res_nmb);
              }
            }
          } else {
            if (OB_FAIL(get_int_obj(param_result.at(0), obj1, *ctx.allocator_))) {
              LOG_WARN("get int obj failed", K(ret));
            } else if (OB_FAIL(get_int_obj(param_result.at(1), obj2, *ctx.allocator_))) {
              LOG_WARN("get int obj failed", K(ret));
            } else {
              int64_t num1 = obj1.get_int();
              int64_t num2 = obj2.get_int();
              if (0 == num2) {
                ret = OB_EXPR_CALC_ERROR;
                LOG_WARN("div failed, num2 is 0", K(ret));
              } else {
                result_obj.set_int(num1 / num2);
              }
            }
          }

          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WARN("result obj array push back failed", K(ret));
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
      LOG_WARN("calc expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && len == result_obj_array.count()) {
    if (2 != param_array_.count()) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("div should have two param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WARN("calc param result failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        for (int64_t j = 0; OB_SUCC(ret) && j < param_result_array.count(); j++) {
          ObObj tmp_obj;
          if (param_result_array.at(j).count() == 1) {
            tmp_obj = param_result_array.at(j).at(0);
          } else {
            tmp_obj = param_result_array.at(j).at(i);
          }
          if (OB_FAIL(param_result.push_back(tmp_obj))) {
            LOG_WARN("push back obj failed", K(ret), K(i), K(j));
          }
        }

        if (OB_SUCC(ret)) {
          ObObj obj1 = param_result.at(0);
          ObObj obj2 = param_result.at(1);
          ObObj result_obj;
          if (ObDoubleTC == obj1.get_type_class() || ObDoubleTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObDoubleTC, ObDoubleType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WARN("get double obj failed", K(obj1), K(obj2), K(ret));
            } else {
              double obj1_d = obj1.get_double();
              double obj2_d = obj2.get_double();
              result_obj.set_double(obj1_d + obj2_d);
            }
          } else if (ObFloatTC == obj1.get_type_class() || ObFloatTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObFloatTC, ObFloatType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WARN("get float obj failed", K(obj1), K(obj2), K(ret));
            } else {
              float obj1_f = obj1.get_float();
              float obj2_f = obj2.get_float();
              result_obj.set_float(obj1_f + obj2_f);
            }
          } else if (ObIntTC != obj1.get_type_class() || ObIntTC != obj2.get_type_class()) {
            number::ObNumber res_nmb;
            if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WARN("get number obj failed", K(ret), K(obj1), K(obj2));
            } else if (OB_FAIL(obj1.get_number().add(obj2.get_number(), res_nmb, *ctx.allocator_))) {
              LOG_WARN("failed to div numbers", K(ret), K(obj1), K(obj2));
            } else {
              if (ctx.scale_ >= 0 || accuracy_.get_scale() >= 0) {
                int64_t scale = ctx.scale_ >= 0 ? ctx.scale_ : accuracy_.get_scale();
                if (OB_FAIL(res_nmb.round(scale))) {
                  LOG_WARN("failed to round result number", K(res_nmb),
                           K(scale), K(accuracy_), K(ctx.scale_), K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                result_obj.set_number(res_nmb);
              }
            }
          } else {
            if (OB_FAIL(get_int_obj(param_result.at(0), obj1, *ctx.allocator_))) {
              LOG_WARN("get int obj failed", K(ret));
            } else if (OB_FAIL(get_int_obj(param_result.at(1), obj2, *ctx.allocator_))) {
              LOG_WARN("get int obj failed", K(ret));
            } else {
              int64_t num1 = obj1.get_int();
              int64_t num2 = obj2.get_int();
              if (0 == num2) {
                ret = OB_EXPR_CALC_ERROR;
                LOG_WARN("div failed, num2 is 0", K(ret));
              } else {
                result_obj.set_int(num1 + num2);
              }
            }
          }

          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WARN("result obj array push back failed", K(ret));
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
      LOG_WARN("calc expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && len == result_obj_array.count()) {
    if (2 != param_array_.count()) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("div should have two param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WARN("calc param result failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        for (int64_t j = 0; OB_SUCC(ret) && j < param_result_array.count(); j++) {
          ObObj tmp_obj;
          if (param_result_array.at(j).count() == 1) {
            tmp_obj = param_result_array.at(j).at(0);
          } else {
            tmp_obj = param_result_array.at(j).at(i);
          }
          if (OB_FAIL(param_result.push_back(tmp_obj))) {
            LOG_WARN("push back obj failed", K(ret), K(i), K(j));
          }
        }

        if (OB_SUCC(ret)) {
          ObObj obj1 = param_result.at(0);
          ObObj obj2 = param_result.at(1);
          ObObj result_obj;
          if (ObDoubleTC == obj1.get_type_class() || ObDoubleTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObDoubleTC, ObDoubleType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WARN("get double obj failed", K(obj1), K(obj2), K(ret));
            } else {
              double obj1_d = obj1.get_double();
              double obj2_d = obj2.get_double();
              result_obj.set_double(obj1_d - obj2_d);
            }
          } else if (ObFloatTC == obj1.get_type_class() || ObFloatTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObFloatTC, ObFloatType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WARN("get float obj failed", K(obj1), K(obj2), K(ret));
            } else {
              float obj1_f = obj1.get_float();
              float obj2_f = obj2.get_float();
              result_obj.set_float(obj1_f - obj2_f);
            }
          } else if (ObIntTC != obj1.get_type_class() || ObIntTC != obj2.get_type_class()) {
            number::ObNumber res_nmb;
            if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WARN("get number obj failed", K(ret), K(obj1), K(obj2));
            } else if (OB_FAIL(obj1.get_number().sub(obj2.get_number(), res_nmb, *ctx.allocator_))) {
              LOG_WARN("failed to div numbers", K(ret), K(obj1), K(obj2));
            } else {
              if (ctx.scale_ >= 0 || accuracy_.get_scale() >= 0) {
                int64_t scale = ctx.scale_ >= 0 ? ctx.scale_ : accuracy_.get_scale();
                if (OB_FAIL(res_nmb.round(scale))) {
                  LOG_WARN("failed to round result number", K(res_nmb),
                           K(scale), K(accuracy_), K(ctx.scale_), K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                result_obj.set_number(res_nmb);
              }
            }
          } else {
            if (OB_FAIL(get_int_obj(param_result.at(0), obj1, *ctx.allocator_))) {
              LOG_WARN("get int obj failed", K(ret));
            } else if (OB_FAIL(get_int_obj(param_result.at(1), obj2, *ctx.allocator_))) {
              LOG_WARN("get int obj failed", K(ret));
            } else {
              int64_t num1 = obj1.get_int();
              int64_t num2 = obj2.get_int();
              if (0 == num2) {
                ret = OB_EXPR_CALC_ERROR;
                LOG_WARN("div failed, num2 is 0", K(ret));
              } else {
                result_obj.set_int(num1 - num2);
              }
            }
          }

          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WARN("result obj array push back failed", K(ret));
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
      LOG_WARN("calc expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && len == result_obj_array.count()) {
    if (2 != param_array_.count()) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("div should have two param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WARN("calc param result failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        for (int64_t j = 0; OB_SUCC(ret) && j < param_result_array.count(); j++) {
          ObObj tmp_obj;
          if (param_result_array.at(j).count() == 1) {
            tmp_obj = param_result_array.at(j).at(0);
          } else {
            tmp_obj = param_result_array.at(j).at(i);
          }
          if (OB_FAIL(param_result.push_back(tmp_obj))) {
            LOG_WARN("push back obj failed", K(ret), K(i), K(j));
          }
        }

        if (OB_SUCC(ret)) {
          ObObj obj1 = param_result.at(0);
          ObObj obj2 = param_result.at(1);
          ObObj result_obj;
          if (ObDoubleTC == obj1.get_type_class() || ObDoubleTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObDoubleTC, ObDoubleType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WARN("get double obj failed", K(obj1), K(obj2), K(ret));
            } else {
              double obj1_d = obj1.get_double();
              double obj2_d = obj2.get_double();
              result_obj.set_double(obj1_d * obj2_d);
            }
          } else if (ObFloatTC == obj1.get_type_class() || ObFloatTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObFloatTC, ObFloatType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WARN("get float obj failed", K(obj1), K(obj2), K(ret));
            } else {
              float obj1_f = obj1.get_float();
              float obj2_f = obj2.get_float();
              result_obj.set_float(obj1_f * obj2_f);
            }
          } else if (ObIntTC != obj1.get_type_class() || ObIntTC != obj2.get_type_class()) {
            number::ObNumber res_nmb;
            if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WARN("get number obj failed", K(ret), K(obj1), K(obj2));
            } else if (OB_FAIL(obj1.get_number().mul(obj2.get_number(), res_nmb, *ctx.allocator_))) {
              LOG_WARN("failed to div numbers", K(ret), K(obj1), K(obj2));
            } else {
              if (ctx.scale_ >= 0 || accuracy_.get_scale() >= 0) {
                int64_t scale = ctx.scale_ >= 0 ? ctx.scale_ : accuracy_.get_scale();
                if (OB_FAIL(res_nmb.round(scale))) {
                  LOG_WARN("failed to round result number", K(res_nmb),
                           K(scale), K(accuracy_), K(ctx.scale_), K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                result_obj.set_number(res_nmb);
              }
            }
          } else {
            if (OB_FAIL(get_int_obj(param_result.at(0), obj1, *ctx.allocator_))) {
              LOG_WARN("get int obj failed", K(ret));
            } else if (OB_FAIL(get_int_obj(param_result.at(1), obj2, *ctx.allocator_))) {
              LOG_WARN("get int obj failed", K(ret));
            } else {
              int64_t num1 = obj1.get_int();
              int64_t num2 = obj2.get_int();
              if (0 == num2) {
                ret = OB_EXPR_CALC_ERROR;
                LOG_WARN("div failed, num2 is 0", K(ret));
              } else {
                result_obj.set_int(num1 * num2);
              }
            }
          }

          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WARN("result obj array push back failed", K(ret));
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
    LOG_WARN("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (1 != param_array_.count()) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("testload should have one param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WARN("calc param result failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        for (int64_t j = 0; OB_SUCC(ret) && j < param_result_array.count(); j++) {
          ObObj tmp_obj;
          if (param_result_array.at(j).count() == 1) {
            tmp_obj = param_result_array.at(j).at(0);
          } else {
            tmp_obj = param_result_array.at(j).at(i);
          }
          if (OB_FAIL(param_result.push_back(tmp_obj))) {
            LOG_WARN("push back obj failed", K(ret), K(i), K(j));
          }
        }

        if (OB_SUCC(ret)) {
          ObObj tmp_obj;
          ObObj result_obj;
          if (OB_FAIL(get_varchar_obj(param_result.at(0), tmp_obj, *ctx.allocator_))) {
            ret = OB_EXPR_CALC_ERROR;
            LOG_WARN("testload param is not varchar", K(ret), K(param_result.at(0)));
          } else {
            result_obj = tmp_obj;
            ObString str = result_obj.get_varchar();
            ObShardRule::handle_special_char(str.ptr(), str.length());
            result_obj.set_varchar(str);

            if (OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WARN("result obj array push back failed", K(ret));
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
    LOG_WARN("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(3 != param_array_.count())) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("split not enough param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WARN("split calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        for (int64_t j = 0; OB_SUCC(ret) && j < param_result_array.count(); j++) {
          ObObj tmp_obj;
          if (param_result_array.at(j).count() == 1) {
            tmp_obj = param_result_array.at(j).at(0);
          } else {
            tmp_obj = param_result_array.at(j).at(i);
          }
          if (OB_FAIL(param_result.push_back(tmp_obj))) {
            LOG_WARN("push back obj failed", K(ret), K(i), K(j));
          }
        }

        if (OB_SUCC(ret)) {
          ObString value;
          ObObj first_obj;
          ObString symbol;
          int64_t index = -1;

          if (OB_FAIL(get_varchar_obj(param_result.at(0), first_obj, *ctx.allocator_))) {
            LOG_WARN("get varchar obj failed", K(ret));
          } else if (OB_FAIL(first_obj.get_varchar(value))) {
            LOG_WARN("get varchar failed", K(ret), K(first_obj));
          } else if (value.empty()) {
            ret = OB_EXPR_CALC_ERROR;
            LOG_WARN("split first parm is emtpy", K(ret));
          }

          if (OB_SUCC(ret)) {
            ObObj obj;
            if (OB_FAIL(get_varchar_obj(param_result.at(1), obj, *ctx.allocator_))) {
              LOG_WARN("get varchar obj failed", K(ret));
            } else if (OB_FAIL(obj.get_varchar(symbol))) {
              LOG_WARN("get varchar failed", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            ObObj obj;
            if (OB_FAIL(get_int_obj(param_result.at(2), obj, *ctx.allocator_))) {
              LOG_WARN("get int obj failed", K(ret));
            } else if (OB_FAIL(obj.get_int(index))) {
              LOG_WARN("get int failed", K(ret));
            } else if (index <= 0) {
              ret = OB_EXPR_CALC_ERROR;
              LOG_WARN("split function param 3 is less than 1", K(ret));
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
                  LOG_WARN("tmp result push back str failed", K(ret));
                }
              } else {
                start++;
              }
            }

            if (OB_SUCC(ret)) {
              ObString str;
              str.assign_ptr(pos, static_cast<ObString::obstr_size_t>(end - pos));
              if (OB_FAIL(tmp_result.push_back(str))) {
                LOG_WARN("tmp result push back str failed", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              ObObj result_obj;
              if (index <= tmp_result.count()) {
                ObString result_str = tmp_result.at(index - 1);
                result_obj.set_varchar(result_str);
                result_obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

                if (OB_FAIL(check_varchar_empty(result_obj))) {
                  LOG_WARN("check varchar emtpy failed", K(ret));
                }
              } else {
                ret = OB_EXPR_CALC_ERROR;
                LOG_WARN("param 3 is large than result count", K(ret), K(index), K(tmp_result.count()));
              }

              if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
                LOG_WARN("result obj array push back failed", K(ret));
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
        LOG_WARN("fail to calc sum", K(ret));
      } else if (count_expr_->calc(ctx, calc_item, param_result)) {
        LOG_WARN("fail to calc count", K(ret));
      } else if (OB_UNLIKELY(2 != param_result.count())) {
        ret = OB_EXPR_CALC_ERROR;
        LOG_WARN("avg not enough param", "count", param_result.count(), K(ret));
      } else {
        ObObj obj1 = param_result.at(0);
        ObObj obj2 = param_result.at(1);
        ObObj result_obj;
        if (ObDoubleTC == obj1.get_type_class() || ObDoubleTC == obj2.get_type_class()) {
          if (OB_FAIL((get_obj_for_calc<ObDoubleTC, ObDoubleType>(ctx.allocator_, obj1, obj2)))) {
            LOG_WARN("get double obj failed", K(obj1), K(obj2), K(ret));
          } else if (fabs(obj2.get_double()) < DBL_EPSILON) {
            result_obj.set_null();
          } else {
            double obj1_d = obj1.get_double();
            double obj2_d = obj2.get_double();
            result_obj.set_double(obj1_d / obj2_d);
          }
        } else if (ObFloatTC == obj1.get_type_class() || ObFloatTC == obj2.get_type_class()) {
          if (OB_FAIL((get_obj_for_calc<ObFloatTC, ObFloatType>(ctx.allocator_, obj1, obj2)))) {
            LOG_WARN("get float obj failed", K(obj1), K(obj2), K(ret));
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
            LOG_WARN("get number obj failed", K(ret), K(obj1), K(obj2));
          } else if (OB_UNLIKELY(obj2.get_number().is_zero())) {
            result_obj.set_null();
          } else if (OB_FAIL(obj1.get_number().div(obj2.get_number(), res_nmb, *ctx.allocator_))) {
            LOG_WARN("failed to div numbers", K(ret), K(obj1), K(obj2));
          } else {
            if (ctx.scale_ >= 0 || accuracy_.get_scale() >= 0) {
              int64_t scale = ctx.scale_ >= 0 ? ctx.scale_ : accuracy_.get_scale();
              if (OB_FAIL(res_nmb.round(scale))) {
                LOG_WARN("failed to round result number", K(res_nmb),
                         K(scale), K(accuracy_), K(ctx.scale_), K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              result_obj.set_number(res_nmb);
            }
          }
        } else {
          if (OB_FAIL(get_int_obj(param_result.at(0), obj1, *ctx.allocator_))) {
            LOG_WARN("get int obj failed", K(ret));
          } else if (OB_FAIL(get_int_obj(param_result.at(1), obj2, *ctx.allocator_))) {
            LOG_WARN("get int obj failed", K(ret));
          } else {
            int64_t num1 = obj1.get_int();
            int64_t num2 = obj2.get_int();
            if (0 == num2) {
              ret = OB_EXPR_CALC_ERROR;
              LOG_WARN("div failed, num2 is 0", K(ret));
            } else {
              result_obj.set_int(num1 / num2);
            }
          }
        }

        if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
          LOG_WARN("result obj array push back failed", K(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can't calc avg expr", KP(sum_expr_), KP(count_expr_), K(ret));
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
    LOG_WARN("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() < 1)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("to_date should have at least one param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WARN("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        for (int64_t j = 0; OB_SUCC(ret) && j < param_result_array.count(); j++) {
          ObObj tmp_obj;
          if (param_result_array.at(j).count() == 1) {
            tmp_obj = param_result_array.at(j).at(0);
          } else {
            tmp_obj = param_result_array.at(j).at(i);
          }
          if (OB_FAIL(param_result.push_back(tmp_obj))) {
            LOG_WARN("push back obj failed", K(ret), K(i), K(j));
          }
        }

        ObObjTypeClass type = param_result.at(0).get_type_class();
        if (OB_SUCC(ret) && type != ObStringTC && type != ObIntTC && type != ObNumberTC) {
          ret = OB_EXPR_CALC_ERROR;
          LOG_WARN("unexpected the first param type of to_date", K(type));
        }

        ObTimeZoneInfo tz_info;
        ObDataTypeCastParams dtc_params;
        if (OB_SUCC(ret)) {
          ObObj second_obj;
          ObString nls_format;

          if (2 == param_result.count()) {
            if(OB_FAIL(get_varchar_obj(param_result.at(1), second_obj, *ctx.allocator_))){
              LOG_WARN("get varchar obj failed", K(ret));
            } else if (OB_FAIL(second_obj.get_varchar(nls_format))) {
              LOG_WARN("get varchar failded", K(ret));
            } else {
              if (!nls_format.empty()) {
                dtc_params.tz_info_ = &tz_info;
                dtc_params.set_nls_format_by_type(target_type_, nls_format);
              } else {
                dtc_params.tz_info_ = &tz_info;
                if (OB_FAIL(proxy::ObExprCalcTool::build_dtc_params(
                    ctx.client_session_info_, target_type_, dtc_params))) {
                  LOG_WARN("fail to build dtc params", K(ret), K(target_type_));
                }
              }
            }
          }
        }

        ObCollationType collation = ObCharset::get_default_collation(ObCharset::get_default_charset());
        ObCastCtx cast_ctx(ctx.allocator_, &dtc_params, CM_NULL_ON_WARN, collation);
        param_result.at(0).set_collation_type(collation);
        if (OB_SUCC(ret) && OB_FAIL(ObObjCasterV2::to_type(target_type_, collation, cast_ctx,
                                                           param_result.at(0), param_result.at(0)))) {
          LOG_WARN("fail to cast obj", K(ret), K(target_type_), K(collation));
        }
        if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(param_result.at(0)))) {
          LOG_WARN("result obj array push back failed", K(ret));
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
    LOG_WARN("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() != 2)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("nvl should have two param", K(ret));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WARN("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        for (int64_t j = 0; OB_SUCC(ret) && j < param_result_array.count(); j++) {
          ObObj tmp_obj;
          if (param_result_array.at(j).count() == 1) {
            tmp_obj = param_result_array.at(j).at(0);
          } else {
            tmp_obj = param_result_array.at(j).at(i);
          }
          if (OB_FAIL(param_result.push_back(tmp_obj))) {
            LOG_WARN("push back obj failed", K(ret), K(i), K(j));
          }
        }

        ObObj result_obj = param_result.at(0);
        ObObj second_obj = param_result.at(1);
        if (OB_SUCC(ret)) {
          // null expr or empty varchar treated as null
          if (result_obj.is_null()) {
            result_obj = second_obj;
          } else if (result_obj.is_string_type()) {
            ObString str;
            if (OB_FAIL(result_obj.get_string(str))) {
              LOG_WARN("get varchar of param failed", K(ret));
            } else if (str.empty()) {
              result_obj = second_obj;
            }
          }
        }

        if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
          LOG_WARN("result obj array push back failed", K(ret));
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
    LOG_WARN("calc expr failed", K(ret), K(param_array_.count()));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() < 1) || OB_UNLIKELY(param_array_.count() > 2)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("to_char should have one or two param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WARN("calc param expr failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        for (int64_t j = 0; OB_SUCC(ret) && j < param_result_array.count(); j++) {
          ObObj tmp_obj;
          if (param_result_array.at(j).count() == 1) {
            tmp_obj = param_result_array.at(j).at(0);
          } else {
            tmp_obj = param_result_array.at(j).at(i);
          }
          if (OB_FAIL(param_result.push_back(tmp_obj))) {
            LOG_WARN("push back obj failed", K(ret), K(i), K(j));
          }
        }
        if (OB_SUCC(ret)) {
          ObObjType target_type = ObVarcharType;
          ObObj result_obj = param_result.at(0);
          ObObj second_obj;
          ObString nls_format;
          ObTimeZoneInfo tz_info;
          ObDataTypeCastParams dtc_params = ObDataTypeCastParams();

          // to_char(datetime) return NULL when nls_format parmas is '' 
          bool is_empty_format = false;

          ObCollationType collation = ObCharset::get_default_collation(ObCharset::get_default_charset());
          dtc_params.tz_info_ = &tz_info;

          if (ObDateTimeTC != param_result.at(0).get_type_class() && ObOTimestampTC != param_result.at(0).get_type_class()) {
            ret = OB_EXPR_CALC_ERROR;
            LOG_WARN("unsupported first param type of to_char", K(param_result.at(0).get_type_class()));
          } else if (2 == param_result.count()) {
            if (OB_FAIL(get_varchar_obj(param_result.at(1), second_obj, *ctx.allocator_))) {
              LOG_WARN("get varchar obj failed", K(ret));
            } else if (OB_FAIL(second_obj.get_varchar(nls_format))) {
              LOG_WARN("get varchar failed", K(ret));
            } else {
              if (!nls_format.empty()) {
                // ObObjCasterV2 only support ObOTimestampTC to format by nls_format, here convert ObDateTimeTC to ObTimestampTZType
                dtc_params.set_nls_format_by_type(ObTimestampTZType, nls_format);
                ObCastCtx cast_ctx_otimestamp(ctx.allocator_, &dtc_params, CM_NULL_ON_WARN, collation);
                if (OB_FAIL(ObObjCasterV2::to_type(ObTimestampTZType, collation, cast_ctx_otimestamp,
                                                   result_obj, result_obj))) {
                  LOG_WARN("cast DateTime to ObTimestampTZTType failed", K(ret));
                }
              } else {
                is_empty_format = true;
              }
            }
          }

          if (OB_SUCC(ret)) {
            ObCastCtx cast_ctx(ctx.allocator_, &dtc_params, CM_NULL_ON_WARN, collation);
            if (param_array_.count() == 1) {
              // do nothing
            } else if (is_empty_format) {
              result_obj.set_null();
            } else if (OB_FAIL(ObObjCasterV2::to_type(target_type, collation, cast_ctx, result_obj, result_obj))) {
              LOG_WARN("fail to cast obj to timestamp", K(ret), K(result_obj), K(target_type), K(collation));
            }
          }

          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WARN("result obj array push back failed", K(ret));
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
    LOG_WARN("calc expr failed", K(ret));
  } else if (len == result_obj_array.count()) {
    if (OB_UNLIKELY(param_array_.count() > 0)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("sysdate should have no param", K(ret));
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
          ObCollationType collation = ObCharset::get_default_collation(ObCharset::get_default_charset());
          if (OB_SUCC(ret)) {
            ObCastCtx cast_ctx(ctx.allocator_, NULL, CM_NULL_ON_WARN, collation);
            if (OB_FAIL(ObObjCasterV2::to_type(target_type, collation, cast_ctx, result_obj, result_obj))) {
              LOG_WARN("fail to cast obj to timestamp", K(ret), K(result_obj), K(target_type), K(collation));
            } else if (OB_FAIL(result_obj_array.push_back(result_obj))) {
              LOG_WARN("result obj array push back failed", K(ret));
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

  if (-1 != index_ &&
      ObProxyExprCalcItem::FROM_OBJ_ARRAY == calc_item.source_ && !has_agg_) {
    if (OB_FAIL(ObProxyExpr::calc(ctx, calc_item, result_obj_array))) {
      LOG_WARN("calc expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && len == result_obj_array.count()) {
    if (2 != param_array_.count()) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("mod should have two param", K(ret), K(param_array_.count()));
    } else if (OB_FAIL(
        calc_param_expr(ctx, calc_item, param_result_array, cnt))) {
      LOG_WARN("calc param result failed", K(ret));
    } else {
      int i = 0;
      do {
        common::ObSEArray<common::ObObj, 4> param_result;
        for (int64_t j = 0; OB_SUCC(ret) && j < param_result_array.count(); j++) {
          ObObj tmp_obj;
          if (param_result_array.at(j).count() == 1) {
            tmp_obj = param_result_array.at(j).at(0);
          } else {
            tmp_obj = param_result_array.at(j).at(i);
          }
          if (OB_FAIL(param_result.push_back(tmp_obj))) {
            LOG_WARN("push back obj failed", K(ret), K(i), K(j));
          }
        }

        if (OB_SUCC(ret)) {
          ObObj obj1 = param_result.at(0);
          ObObj obj2 = param_result.at(1);
          ObObj result_obj;

          if (ObDoubleTC == obj1.get_type_class() ||
              ObDoubleTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObDoubleTC, ObDoubleType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WARN("get double obj failed", K(obj1), K(obj2), K(ret));
            } else if (fabs(obj2.get_double()) < DBL_EPSILON) {
              result_obj.set_double(obj1.get_double());
            } else {
              double obj1_d = obj1.get_double();
              double obj2_d = obj2.get_double();
              result_obj.set_double(obj1_d - obj2_d * floor(obj1_d / obj1_d));
            }
          } else if (ObFloatTC == obj1.get_type_class() || ObFloatTC == obj2.get_type_class()) {
            if (OB_FAIL((get_obj_for_calc<ObFloatTC, ObFloatType>(ctx.allocator_, obj1, obj2)))) {
              LOG_WARN("get float obj failed", K(obj1), K(obj2), K(ret));
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
              LOG_WARN("get number obj failed", K(ret), K(obj1), K(obj2));
            } else if (obj2.get_number().is_zero()) {
              result_obj.set_number(obj1.get_number());
            } else if (OB_FAIL(obj1.get_number().rem(obj2.get_number(), res_nmb, *ctx.allocator_))) {
              LOG_WARN("failed to mod numbers", K(ret), K(obj1), K(obj2));
            } else if (OB_SUCC(ret)) {
              result_obj.set_number(res_nmb);
            }
          }

          if (OB_SUCC(ret) && OB_FAIL(result_obj_array.push_back(result_obj))) {
            LOG_WARN("result obj array push back failed", K(ret));
          }
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
