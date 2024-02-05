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

#include "dbconfig/ob_proxy_db_config_info.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/ob_define.h"
#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr_type.h"
#include "proxy/mysqllib/ob_proxy_session_info.h"

namespace oceanbase
{
namespace obproxy
{

namespace opsql
{

static const int MAX_REPLACE_COUNT = 256;

enum ExprStrCaseOperation
{
  NONE = 0,
  STR_UPPER,
  STR_LOWER 
};

#define LOCATE_PARAM_RESULT(param_array, param_result, index)             \
  do {                                                                    \
    for (int64_t j = 0; OB_SUCC(ret) && j < param_array.count(); j++) {   \
      ObObj tmp_obj;                                                      \
      common::ObSEArray<common::ObObj, 4> param_item;                     \
      if (OB_FAIL(param_array.at(j, param_item))) {                       \
        LOG_WDIAG("fail to get param array", K(ret));                     \
      } else if (param_item.count() == 1) {                               \
        if (OB_FAIL(param_item.at(0, tmp_obj))) {                         \
          LOG_WDIAG("fail to get param", K(ret));                         \
        }                                                                 \
      } else {                                                            \
        if (OB_FAIL(param_item.at(index, tmp_obj))) {                     \
          LOG_WDIAG("fail to get param", K(ret));                         \
        }                                                                 \
      }                                                                   \
      if (OB_SUCC(ret) && OB_FAIL(param_result.push_back(tmp_obj))) {     \
        LOG_WDIAG("push back obj failed", K(ret), K(i), K(j));            \
      }                                                                   \
    }                                                                     \
  } while (0)

class ObProxyExprCtx
{
public:
  enum INT_CAST_MODE {
    NORMAL_CAST_MODE = 0,
    FLOOR_CAST_MODE,
    CEIL_CAST_MODE
  };
  explicit ObProxyExprCtx(const int64_t physical_size, dbconfig::ObTestLoadType type,
                          bool is_elastic_index, common::ObIAllocator *allocator,
                          INT_CAST_MODE int_cast_mode = NORMAL_CAST_MODE)
      : is_elastic_index_(is_elastic_index), is_oracle_mode(false),
        test_load_type_(type), int_cast_mode_(int_cast_mode),
        sharding_physical_size_(physical_size), scale_(-1), allocator_(allocator), client_session_info_(NULL) {}
  explicit ObProxyExprCtx(const int64_t physical_size,
                          dbconfig::ObTestLoadType type, bool is_elastic_index,
                          common::ObIAllocator *allocator,
                          proxy::ObClientSessionInfo *client_session_info,
                          INT_CAST_MODE int_cast_mode = NORMAL_CAST_MODE)
      : is_elastic_index_(is_elastic_index), is_oracle_mode(false),
        test_load_type_(type), int_cast_mode_(int_cast_mode),
        sharding_physical_size_(physical_size), scale_(-1), allocator_(allocator), client_session_info_(client_session_info) {}
  ~ObProxyExprCtx() {}

  void set_sharding_physical_size(const int64_t physical_size) { sharding_physical_size_ = physical_size; }
  void set_test_load_type(const dbconfig::ObTestLoadType type) { test_load_type_ = type; }
  void set_is_elastic_index(const bool is_elastic_index) { is_elastic_index_ = is_elastic_index; }
  void set_scale(const int64_t scale) { scale_ = scale; };
  void set_client_session_info(proxy::ObClientSessionInfo *client_session_info)
  {
    client_session_info_ = client_session_info;
  }

public:
  bool is_elastic_index_;
  bool is_oracle_mode;
  dbconfig::ObTestLoadType test_load_type_;
  INT_CAST_MODE int_cast_mode_;
  int64_t sharding_physical_size_;
  int64_t scale_;
  common::ObIAllocator *allocator_;
  proxy::ObClientSessionInfo *client_session_info_;

};

struct ObProxyExprCalcItem {
  enum ObProxyExprCalcItemSource {
    FROM_SQL_FIELD,
    FROM_OBJ_ARRAY,
    FROM_INVALID
  };

  ObProxyExprCalcItem() : sql_result_(NULL), obj_array_(NULL), source_(FROM_SQL_FIELD) {}

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
  explicit ObProxyExpr() : type_(OB_PROXY_EXPR_TYPE_NONE), index_(-1), accuracy_(), has_agg_(0),
                           is_func_expr_(0), reserved_(0), alias_name_() {}

  ~ObProxyExpr() {}
  void set_expr_type(const ObProxyExprType type) { type_ = type; }
  ObProxyExprType get_expr_type() const { return type_; }
  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                   common::ObIArray<common::ObObj> &result_obj_array);
  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                   common::ObIArray<common::ObObj*> &result_obj_array);

  bool has_agg() const { return has_agg_ == 1; }
  void set_index(int64_t index) { index_ = index; }
  int64_t get_index() const { return index_; }
  void set_accuracy(common::ObAccuracy accuracy) { accuracy_ = accuracy; }
  common::ObAccuracy get_accuracy() const { return accuracy_; }
  bool is_func_expr() const { return is_func_expr_ == 1; }
  bool is_star_expr() { return OB_PROXY_EXPR_TYPE_STAR == type_; }

  int64_t to_string(char *buf, int64_t buf_len) const;

  virtual int to_sql_string(common::ObSqlString& sql_string);
  virtual int to_column_string(common::ObSqlString& sql_string) {
    return sql_string.append(expr_name_);
  }
  static  void print_proxy_expr(ObProxyExpr *root);
  void set_alias_name(const char *buf, const int64_t length) {
    alias_name_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(length));
  }
  void set_alias_name(const ObString &alias_name) { alias_name_ = alias_name; }
  ObString& get_alias_name() { return alias_name_; }
  void set_expr_name(const char *buf, const int64_t length) {
    expr_name_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(length));
  }
  ObString& get_expr_name() { return expr_name_; }

  bool is_agg()
  {
    return OB_PROXY_EXPR_TYPE_FUNC_SUM == type_
    || OB_PROXY_EXPR_TYPE_FUNC_COUNT == type_
    || OB_PROXY_EXPR_TYPE_FUNC_MAX == type_
    || OB_PROXY_EXPR_TYPE_FUNC_MIN == type_
    || OB_PROXY_EXPR_TYPE_FUNC_AVG == type_;
  }

public:
  ObProxyExprType type_;
  int64_t index_;
  common::ObAccuracy accuracy_;
  struct {
    uint16_t has_agg_ : 1;// whethere hava agg func, 1 means true
    uint16_t is_func_expr_ : 1;
    uint16_t reserved_ : 14;
  };
  common::ObString expr_name_;
  common::ObString alias_name_;
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
  virtual int to_column_string(common::ObSqlString& sql_string);

protected:
  common::ObObj obj_;
};

// add for save sharding const column
class ObProxyExprShardingConst : public ObProxyExpr
{
public:
  explicit ObProxyExprShardingConst() {}
  virtual ~ObProxyExprShardingConst() {}
};

class ObProxyExprTable : public ObProxyExpr
{
public:
  explicit ObProxyExprTable() : database_name_(), table_name_() {}
  ~ObProxyExprTable() {}

  void set_database_name(const char *buf, const int32_t length) { database_name_.assign_ptr(buf, length); }
  ObString& get_database_name() { return database_name_; }
  void set_table_name(const char *buf, const int32_t length) { table_name_.assign_ptr(buf, length); }
  ObString& get_table_name() { return table_name_; }
private:
  common::ObString database_name_;
  common::ObString table_name_;
};

class ObProxyExprColumn : public ObProxyExpr
{
public:
  explicit ObProxyExprColumn() : real_table_name_(), table_name_(), column_name_() {}
  ~ObProxyExprColumn() {}

  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);
  virtual int to_column_string(common::ObSqlString& sql_string);
  void set_column_name(const char *buf, const int32_t length) { column_name_.assign_ptr(buf, length); }
  ObString &get_column_name() { return column_name_; }
  void set_table_name(const char *buf, const int32_t length) { table_name_.assign_ptr(buf, length); }
  ObString &get_table_name() { return table_name_; }
  void set_real_table_name(const ObString &real_table_name) { real_table_name_ = real_table_name; }
  ObString &get_real_table_name() { return real_table_name_; }
private:
  common::ObString real_table_name_;
  common::ObString table_name_;
  common::ObString column_name_;
};

class ObProxyExprStar : public ObProxyExpr
{
public:
  explicit ObProxyExprStar() : table_name_() {}
  ~ObProxyExprStar() {}

  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                   common::ObIArray<common::ObObj> &result_obj_array) {
    UNUSED(ctx);
    UNUSED(calc_item);
    UNUSED(result_obj_array);
    return OB_SUCCESS;
  };
  void set_table_name(const char *buf, const int32_t length) { table_name_.assign_ptr(buf, length); }
  ObString &get_table_name() { return table_name_; }
private:
  common::ObString table_name_;
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

class ObProxyGroupItem : public ObProxyExpr
{
public:
  explicit ObProxyGroupItem() : expr_(NULL) {}
  virtual ~ObProxyGroupItem() {
    if (NULL != expr_) {
      expr_->~ObProxyExpr();
    }
  }
  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                   common::ObIArray<common::ObObj> &result_obj_array);
  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                   common::ObIArray<common::ObObj*> &result_obj_array);
  virtual int to_column_string(common::ObSqlString& sql_string);
  void set_expr(ObProxyExpr* expr) { expr_ = expr; }
  ObProxyExpr* get_expr() { return expr_; }
public:
  ObProxyExpr* expr_;
};

class ObProxyOrderItem : public ObProxyGroupItem
{
public:
  explicit ObProxyOrderItem() : order_direction_(NULLS_FIRST_ASC) {}
  virtual ~ObProxyOrderItem() {}
  virtual int to_sql_string(common::ObSqlString& sql_string);
public:
  ObProxyOrderDirection order_direction_;
};

class ObProxyFuncExpr : public ObProxyExpr
{
public:
  explicit ObProxyFuncExpr() : param_array_(common::ObModIds::OB_PROXY_SHARDING_EXPR, common::OB_MALLOC_NORMAL_BLOCK_SIZE) { is_func_expr_ = true; }
  virtual ~ObProxyFuncExpr();

  int add_param_expr(ObProxyExpr* expr) { return param_array_.push_back(expr); }
  int64_t get_param_conut() { return param_array_.count(); }

  int calc_param_expr(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                      common::ObSEArray<common::ObSEArray<common::ObObj, 4>, 4> &param_result,
                      int &cnt);

  common::ObSEArray<ObProxyExpr*, 4>& get_param_array() { return param_array_; }
  void set_param_array(common::ObSEArray<ObProxyExpr*, 4>& param_array) { param_array_ = param_array; }

public:
  static int get_int_obj(const common::ObObj &src, common::ObObj &dst, const ObProxyExprCtx &expr_ctx);
  static int get_varchar_obj(const common::ObObj &src, common::ObObj &dst, const ObProxyExprCtx &ctx);
  // convert datetime to oracle timestamp
  static int get_time_obj(const common::ObObj &src, common::ObObj &dst, const ObProxyExprCtx &ctx, const ObObjType &type);
  static ObCollationType get_collation(const ObProxyExprCtx &ctx);
  int check_varchar_empty(const common::ObObj& result);
  static int get_case_multiply_num(const ExprStrCaseOperation operation, const ObCollationType cs_type, int32_t &multiply);
  static int string_case_operate(const ExprStrCaseOperation operation, const ObProxyExprCtx &ctx,
                                 const ObCollationType collation, char *src,
                                 int64_t src_len, char *&dst, size_t &out_len);

protected:
  common::ObSEArray<ObProxyExpr*, 4> param_array_;
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
};

class ObProxyExprAdd : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprAdd() {}
  ~ObProxyExprAdd() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprSub : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprSub() {}
  ~ObProxyExprSub() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprMul : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprMul() {}
  ~ObProxyExprMul() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
            common::ObIArray<common::ObObj> &result_obj_array);
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
};

class ObProxyExprCount : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprCount() {}
  ~ObProxyExprCount() {}
};

class ObProxyExprAvg : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprAvg() : sum_expr_(NULL), count_expr_(NULL) {}
  ~ObProxyExprAvg() {
    if (NULL != sum_expr_) {
      sum_expr_->~ObProxyExprSum();
    }

    if (NULL != count_expr_) {
      count_expr_->~ObProxyExprCount();
    }
  }
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                   common::ObIArray<common::ObObj> &result_obj_array);
  void set_sum_expr(ObProxyExprSum* sum_expr) { sum_expr_ = sum_expr; }
  ObProxyExprSum *get_sum_expr() { return sum_expr_; }
  void set_count_expr(ObProxyExprCount *count_expr) { count_expr_ = count_expr; }
  ObProxyExprCount *get_count_expr() { return count_expr_; }
private:
  ObProxyExprSum *sum_expr_;
  ObProxyExprCount *count_expr_;
};

class ObProxyExprMax : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprMax() {}
  ~ObProxyExprMax() {}
};

class ObProxyExprMin : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprMin() {}
  ~ObProxyExprMin() {}
};

class ObProxyExprSplit : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprSplit() {}
  ~ObProxyExprSplit() {}

  virtual int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
                   common::ObIArray<common::ObObj> &result_obj_array);
};

// include to_date and to_timestamp
class ObProxyExprToTimeHandler : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprToTimeHandler() {}
  explicit ObProxyExprToTimeHandler(ObObjType &target_type) : target_type_(target_type) {}
  ~ObProxyExprToTimeHandler() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
  void set_target_type(ObObjType target_type) { target_type_ = target_type; }
private:
  ObObjType target_type_;
};

class ObProxyExprNvl : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprNvl() {}
  ~ObProxyExprNvl() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprToChar : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprToChar() {}
  ~ObProxyExprToChar() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprSysdate : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprSysdate() {}
  ~ObProxyExprSysdate() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprMod : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprMod() {}
  ~ObProxyExprMod() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprIsnull : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprIsnull() {}
  ~ObProxyExprIsnull() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprCeil : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprCeil() {}
  ~ObProxyExprCeil() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprFloor : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprFloor() {}
  ~ObProxyExprFloor() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprRound : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprRound() {}
  ~ObProxyExprRound() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprTruncate : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprTruncate() {}
  ~ObProxyExprTruncate() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprAbs : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprAbs() {}
  ~ObProxyExprAbs() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprSystimestamp : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprSystimestamp() {}
  ~ObProxyExprSystimestamp() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprCurrentdate : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprCurrentdate() {}
  ~ObProxyExprCurrentdate() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprCurrenttime : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprCurrenttime() {}
  ~ObProxyExprCurrenttime() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprCurrenttimestamp : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprCurrenttimestamp() {}
  ~ObProxyExprCurrenttimestamp() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprLtrim : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprLtrim() {}
  ~ObProxyExprLtrim() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprRtrim : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprRtrim() {}
  ~ObProxyExprRtrim() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprTrim : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprTrim() {}
  ~ObProxyExprTrim() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprReplace : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprReplace() {}
  ~ObProxyExprReplace() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprLength : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprLength() {}
  ~ObProxyExprLength() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprLower : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprLower() {}
  ~ObProxyExprLower() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprUpper : public ObProxyFuncExpr
{
public:
  explicit ObProxyExprUpper() {}
  ~ObProxyExprUpper() {}
  int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

class ObProxyExprToNumber : public ObProxyFuncExpr
{
  public:
    explicit ObProxyExprToNumber() {}
    ~ObProxyExprToNumber() {}
    int calc(const ObProxyExprCtx &ctx, const ObProxyExprCalcItem &calc_item,
           common::ObIArray<common::ObObj> &result_obj_array);
};

} // end opsql
} // end obproxy
} // end oceanbase

#endif
