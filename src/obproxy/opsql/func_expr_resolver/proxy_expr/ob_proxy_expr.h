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

#ifndef OB_FUNC_EXPR_PROXY_EXPR_H
#define OB_FUNC_EXPR_PROXY_EXPR_H

#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr_type.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace obproxy
{

namespace opsql
{

class ObProxyExprCtx
{
public:
  explicit ObProxyExprCtx(const int64_t physical_size, dbconfig::ObTestLoadType type,
                          bool is_elastic_index, common::ObIAllocator* allocator)
      : sharding_physical_size_(physical_size), test_load_type_(type), is_elastic_index_(is_elastic_index),
        allocator_(allocator), scale_(-1) {}
  ~ObProxyExprCtx() {}

  void set_sharding_physical_size(const int64_t physical_size) { sharding_physical_size_ = physical_size; }
  void set_test_load_type(const dbconfig::ObTestLoadType type) { test_load_type_ = type; }
  void set_is_elastic_index(const bool is_elastic_index) { is_elastic_index_ = is_elastic_index; }
  void set_scale(const int64_t scale) { scale_ = scale; };

public:
  int64_t sharding_physical_size_;
  dbconfig::ObTestLoadType test_load_type_;
  bool is_elastic_index_;
  common::ObIAllocator *allocator_;
  int64_t scale_;
};

struct ObProxyExprCalcItem {
  enum ObProxyExprCalcItemSource {
    FROM_SQL_FIELD,
    FROM_OBJ_ARRAY,
    FROM_INVALID
  };

  ObProxyExprCalcItem(obutils::SqlFieldResult *sql_result) :
      sql_result_(sql_result), obj_array_(NULL), source_(FROM_SQL_FIELD) {}

  ObProxyExprCalcItem(common::ObIArray<common::ObObj*> *obj_array) :
      sql_result_(NULL), obj_array_(obj_array), source_(FROM_OBJ_ARRAY) {}

  obutils::SqlFieldResult *sql_result_;
  common::ObIArray<common::ObObj*> *obj_array_;
  ObProxyExprCalcItemSource source_;
};

// Attention!! all var in expr NOT modify after init
class ObProxyExpr
{
public:
  explicit ObProxyExpr() : type_(OB_PROXY_EXPR_TYPE_NONE), index_(-1), has_agg_(0),
                           has_alias_(0), is_func_expr_(0), reserved_(0) {}

  ~ObProxyExpr() {}
  void set_expr_type(const ObProxyExprType type) { type_ = type; }
  ObProxyExprType get_expr_type() const { return type_; }
  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                   common::ObIArray<common::ObObj> &result_obj_array);
  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                   common::ObIArray<common::ObObj*> &result_obj_array);

  bool has_agg() const { return has_agg_ == 1; }
  bool has_alias() const { return has_alias_ == 1; }
  void set_index(int64_t index) { index_ = index; }
  int64_t get_index() const { return index_; }
  bool is_func_expr() const { return is_func_expr_ == 1; }
  bool is_star_expr();

  int64_t to_string(char *buf, int64_t buf_len) const;

  virtual int to_sql_string(common::ObSqlString& sql_string)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(sql_string);
    return ret;
  }
  static  void print_proxy_expr(ObProxyExpr *root);

  bool is_agg()
  {
    return OB_PROXY_EXPR_TYPE_FUNC_SUM == type_
    || OB_PROXY_EXPR_TYPE_FUNC_COUNT == type_
    || OB_PROXY_EXPR_TYPE_FUNC_MAX == type_
    || OB_PROXY_EXPR_TYPE_FUNC_MIN == type_
    || OB_PROXY_EXPR_TYPE_FUNC_AVG == type_;
  }

  bool is_alias();
public:
  ObProxyExprType type_;
  int64_t index_;
  struct {
    uint16_t has_agg_ : 1;// whethere hava agg func, 1 means true
    uint16_t has_alias_ : 1; // whether hava alias, 1 means true
    uint16_t is_func_expr_ : 1;
    uint16_t reserved_ : 13;
  };
};

class ObProxyExprConst : public ObProxyExpr
{
public:
  explicit ObProxyExprConst() : obj_() {}
  virtual ~ObProxyExprConst() {}
  common::ObObj& get_object() { return obj_; }
  void set_object(const common::ObObj& obj) { obj_ = obj; }
  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);

protected:
  common::ObObj obj_;
};

// add for save sharding const column
class ObProxyExprShardingConst : public ObProxyExprConst
{
public:
  explicit ObProxyExprShardingConst() : expr_(NULL), is_column_(false) , is_alias_(false) {}
  virtual ~ObProxyExprShardingConst() {}
  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);
  virtual int to_sql_string(common::ObSqlString& sql_string);
  void set_is_column(bool is_column)
  {
    is_column_ = is_column;
  }
  void set_is_alias(bool is_alias)
  {
    is_alias_ = is_alias;
  }
  ObProxyExpr* expr_;
public:
  bool is_column_;
  bool is_alias_;
};

class ObProxyExprColumn : public ObProxyExpr
{
public:
  explicit ObProxyExprColumn() : column_name_() {}
  ~ObProxyExprColumn() {}

  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);
  void set_column_name(char *buf, const int32_t length) { column_name_.assign(buf, length); }
private:
  common::ObString column_name_;
};

enum ObProxyOrderDirection
{
  NULLS_FIRST_ASC = 0,   // Forward, NULLs first
  NULLS_LAST_ASC, // Forward, NULLs last
  NULLS_FIRST_DESC, // Backward, NULLs first
  NULLS_LAST_DESC,   // Backward, NULLs last
  UNORDERED, // not ordered
  MAX_DIR, // invalid
};

class ObProxyOrderItem : public ObProxyExpr
{
public:
  explicit ObProxyOrderItem() : order_direction_(NULLS_FIRST_ASC) {}
  virtual ~ObProxyOrderItem() {}
  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
    common::ObIArray<common::ObObj*> &result_obj_array)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(expr_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      ret = expr_->calc(ctx, calc_item, result_obj_array);
    }
    return ret;
  }
  virtual int to_sql_string(common::ObSqlString& sql_string);
public:
  ObProxyExpr* expr_;
  ObProxyOrderDirection order_direction_;
};

class ObProxyFuncExpr : public ObProxyExpr
{
public:
  explicit ObProxyFuncExpr() : param_array_(common::ObModIds::OB_PROXY_SHARDING_EXPR, common::OB_MALLOC_NORMAL_BLOCK_SIZE) { is_func_expr_ = true; }
  virtual ~ObProxyFuncExpr() {}

  int add_param_expr(ObProxyExpr* expr) { return param_array_.push_back(expr); }
  int calc_param_expr(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                      common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> &param_result,
                      int &cnt);

  common::ObSEArray<ObProxyExpr*, 4>& get_param_array() { return param_array_; }
  void set_param_array(common::ObSEArray<ObProxyExpr*, 4>& param_array) { param_array_ = param_array; }

public:
  static int get_int_obj(const common::ObObj &src, common::ObObj &dst);
  static int get_varchar_obj(const common::ObObj &src, common::ObObj &dst, common::ObIAllocator &allocator);
  int check_varchar_empty(const common::ObObj& result);

protected:
  common::ObSEArray<ObProxyExpr*, 4> param_array_;
};

class ObProxyShardingAliasExpr : public ObProxyFuncExpr
{
public:
  explicit ObProxyShardingAliasExpr() {}
  virtual ~ObProxyShardingAliasExpr() {}

  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                   common::ObIArray<common::ObObj> &result_obj_array);
  virtual int to_sql_string(common::ObSqlString& sql_string);

};

class ObProxyExprHash : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprHash() {}
  ~ObProxyExprHash() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprSubStr : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprSubStr() {}
  ~ObProxyExprSubStr() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprConcat : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprConcat() {}
  ~ObProxyExprConcat() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprToInt : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprToInt() {}
  ~ObProxyExprToInt() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprDiv : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprDiv() {}
  ~ObProxyExprDiv() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);
  virtual int to_sql_string(common::ObSqlString& sql_string);
};

class ObProxyExprAdd : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprAdd() {}
  ~ObProxyExprAdd() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);

  virtual int to_sql_string(common::ObSqlString& sql_string);
};

class ObProxyExprSub : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprSub() {}
  ~ObProxyExprSub() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);

  virtual int to_sql_string(common::ObSqlString& sql_string);
};

class ObProxyExprMul : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprMul() {}
  ~ObProxyExprMul() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);
  virtual int to_sql_string(common::ObSqlString& sql_string);
};

class ObProxyExprTestLoad : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprTestLoad() {}
  ~ObProxyExprTestLoad() {}

  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                   common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprSum : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprSum() {}
  ~ObProxyExprSum() {}
  virtual int to_sql_string(common::ObSqlString& sql_string);
};

class ObProxyExprCount : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprCount() {}
  ~ObProxyExprCount() {}
  virtual int to_sql_string(common::ObSqlString& sql_string);
};

class ObProxyExprAvg : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprAvg() : sum_index_(-1), count_index_(-1) {}
  ~ObProxyExprAvg() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                   common::ObIArray<common::ObObj> &result_obj_array);
  virtual int to_sql_string(common::ObSqlString& sql_string);
  void set_sum_index(int64_t index) { sum_index_ = index; }
  void set_count_index(int64_t index) { count_index_ = index; }
private:
  int64_t sum_index_;
  int64_t count_index_;
};

class ObProxyExprMax : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprMax() {}
  ~ObProxyExprMax() {}
  virtual int to_sql_string(common::ObSqlString& sql_string);
};

class ObProxyExprMin : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprMin() {}
  ~ObProxyExprMin() {}
  virtual int to_sql_string(common::ObSqlString& sql_string);
};

class ObProxyExprSplit : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprSplit() {}
  ~ObProxyExprSplit() {}

    virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                     common::ObIArray<common::ObObj> &result_obj_array);
};

} // end opsql
} // end obproxy
} // end oceanbase

#endif
