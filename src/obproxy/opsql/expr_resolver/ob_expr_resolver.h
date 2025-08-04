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

#ifndef OBEXPR_RESOLVER_H
#define OBEXPR_RESOLVER_H
#include "common/ob_object.h"
#include "common/ob_range2.h"
#include "lib/container/ob_se_array.h"
#include "opsql/expr_parser/ob_expr_parse_result.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
struct ObNewRange;
}
namespace obproxy
{

namespace obkv
{
enum class ObTableEntityType;
}
namespace obutils
{
struct SqlFieldResult;
struct SqlColumnValue;
class ObSqlParseResult;
}

namespace proxy
{
class ObProxyPartInfo;
class ObProxyMysqlRequest;
class ObPsIdEntry;
class ObTextPsEntry;
class ObClientSessionInfo;
class ObRouteDiagnosis;
}
namespace opsql
{
class ObPartkeyFuncInfo;
struct ObExprResolverContext
{
  ObExprResolverContext() : part_info_(NULL), client_request_(NULL),
                            ps_id_entry_(NULL), text_ps_entry_(NULL), client_info_(NULL),
                            sql_field_result_(NULL) {}
  // parse result
  proxy::ObProxyPartInfo *part_info_;
  proxy::ObProxyMysqlRequest *client_request_;
  // proxy::ObPsEntry *ps_entry_;
  proxy::ObPsIdEntry *ps_id_entry_;
  proxy::ObTextPsEntry *text_ps_entry_;
  proxy::ObClientSessionInfo *client_info_;
  obutils::SqlFieldResult *sql_field_result_;
  ObExprParseResult *parse_result_;
  bool is_insert_stmt_;
};

class ObExprResolverResult
{
public:
  ObExprResolverResult() : ranges_() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;

  common::ObNewRange ranges_[OBPROXY_MAX_PART_LEVEL];
};

// use for old partition id calc process
// will be deprecated
class ObExprResolver
{
public:
  explicit ObExprResolver(common::ObIAllocator &allocator) : allocator_(allocator), route_diagnosis_(NULL) {}
  // will not be inherited, do not set to virtual

  int calc_generated_key_value_for_obkv(common::ObObj &obj, const ObProxyPartKey &part_key, const obkv::ObTableEntityType entity_type, common::ObArenaAllocator &allocator);
  common::ObIAllocator &allocator_;
  proxy::ObRouteDiagnosis *route_diagnosis_;

  DISALLOW_COPY_AND_ASSIGN(ObExprResolver);
};

// use for new partition id calc process.
// Every code change about partition should consider potential influence to the function of ObExprResolverV2.
// Need use ObExprResolverV2 when you want to calc partition id now.
class ObExprResolverV2
{
public:
  explicit ObExprResolverV2(bool is_oracle_mode,
                            proxy::ObProxyMysqlRequest& client_request,
                            proxy::ObClientSessionInfo& client_info,
                            proxy::ObProxyPartInfo& part_info,
                            common::ObIAllocator& allocator) : is_inited_(false), is_oracle_mode_(is_oracle_mode),
                            client_request_(client_request), client_info_(client_info), part_info_(part_info),
                            part_key_num_(0), relation_num_(0), relation_obj_arr_(), relation_type_arr_(),
                            rowid_obj_arr_(), first_part_func_result_(), sub_part_func_result_(),
                            left_bound_arr_(), equal_obj_arr_(), right_bound_arr_(),
                            left_bound_flag_arr_(), right_bound_flag_arr_(), ranges_(),
                            route_diagnosis_(NULL), allocator_(allocator) {}
  // will not be inherited, do not set to virtual
  ~ObExprResolverV2() { set_route_diagnosis(NULL); }
  int64_t init(int64_t part_key_num);

  int64_t do_relation_obj_resolve(const ObExprParseResult& expr_parse_result);

  int64_t do_part_key_obj_prepare(const ObProxyRelationInfo& all_relation_info,
                                  bool is_empty_column_insert_stmt);

  int64_t do_equal_relation_ratiocination(const ObProxyRelationInfo& all_relation_info,
                                          const ObProxyPartKeyInfo& part_key_info,
                                          const common::ObString& origin_table_name,
                                          const common::ObString& alias_table_name);

  int64_t do_relation_simplication();
  int64_t do_rowid_calc(const ObProxyPartKeyInfo& part_key_info,
                        int64_t& partition_id);
  int64_t do_default_value_set(const ObProxyPartKeyInfo& part_key_info);
  int64_t do_generated_key_calc(const ObProxyPartKeyInfo& part_key_info);
  int64_t do_same_part_key_set(const ObProxyPartKeyInfo& part_key_info);
  int64_t do_part_key_func_calc(const ObProxyPartKeyInfo& part_key_info,
                                proxy::ObProxyPartInfo& part_info);

  int64_t do_generate_range(const ObProxyPartKeyInfo& part_key_info,
                            proxy::ObProxyPartInfo& part_info);

  int64_t calc_part_key_func_level(const ObProxyPartKeyInfo& part_key_info,
                                   common::ObIArray<common::ObObj>& func_result_arr,
                                   ObProxyPartKeyLevel part_key_func_level);

  int preprocess_range(common::ObNewRange& range,
                       common::ObIArray<common::ObBorderFlag>& border_flags);

  common::ObIArray<common::ObObj>& get_equal_array() { return equal_obj_arr_; }
  common::ObNewRange& get_first_part_range() { return ranges_[0]; }
  common::ObNewRange& get_sub_part_range() { return ranges_[1]; }

  int calc_partition_id_with_rowid(const common::ObObj& src_obj,
                                   int64_t &partition_id,
                                   common::ObIArray<common::ObObj>& result_obj_arr);
  int calc_partition_id_with_rowid_str(const char *str,
                                       const int64_t str_len,
                                       int64_t &partition_id,
                                       common::ObIArray<common::ObObj>& result_obj,
                                       int32_t &state,
                                       int16_t &version);

  int64_t to_string(char *buf, const int64_t buf_len) const;
  int calc_generated_key_value_for_obkv(common::ObObj &obj, const ObProxyPartKey &part_key, const obkv::ObTableEntityType entity_type, common::ObArenaAllocator &allocator);
  void set_route_diagnosis(proxy::ObRouteDiagnosis *route_diagnosis);

  int resolve_token_list_const_obj(ObProxyRelationExpr& relation,
                                   proxy::ObProxyPartInfo& part_info,
                                   proxy::ObProxyMysqlRequest& client_request,
                                   proxy::ObClientSessionInfo& client_info,
                                   proxy::ObPsIdEntry* ps_id_entry,
                                   common::ObObj& target_obj);

  int parse_and_resolve_default_value(const ObProxyParseString& default_value,
                                      const proxy::ObClientSessionInfo& client_session_info,
                                      common::ObObj& target_obj,
                                      bool is_oracle_mode);
  static int calc_generated_key_value(common::ObObj &obj,
                                      const ObProxyPartKey &part_key,
                                      const bool is_oracle_mode);

  int add_obj_to_sql_field(obutils::SqlFieldResult &sql_field_result,
                           const common::ObObj &target_obj,
                           const common::ObString &col_name);

  int cal_part_key_func(ObPartkeyFuncInfo &func_info,
                        const obutils::SqlFieldResult& sql_field_result,
                        const proxy::ObClientSessionInfo& client_session_info,
                        common::ObObj& target_obj,
                        bool is_oracle_mode);
  int place_obj_to_range(common::ObNewRange& range,
                         common::ObIArray<common::ObBorderFlag>& border_flags,
                         int64_t idx_in_part_columns,
                         const common::ObObj& start_obj,
                         const common::ObObj& end_obj,
                         bool is_start_inclusive,
                         bool is_end_inclusive);

private:
  struct PartitionColumn {
    inline bool operator==(const PartitionColumn& other) const {
      return (0 == column_name_.case_compare(other.column_name_))
              && (0 == table_name_.case_compare(other.table_name_));
    }

    inline bool operator!=(const PartitionColumn& other) const {
      return !(*this == other);
    }

    inline void assgin(const ObProxyTokenNode& column_node) {
      const ObProxyParseString& parse_table_name = column_node.table_name_;
      const ObProxyParseString& parse_column_name_ = column_node.column_name_;
      table_name_.assign_ptr(parse_table_name.str_, parse_table_name.str_len_);
      column_name_.assign_ptr(parse_column_name_.str_, parse_column_name_.str_len_);
    }

    inline uint64_t hash(uint64_t seed = 0) const
    {
      seed = table_name_.hash(seed);
      seed = column_name_.hash(seed);
      return seed;
    }

    inline int to_string(char *buf, const int64_t buf_len) const {
      int64_t pos = 0;
      J_OBJ_START();
      J_KV(K_(table_name), K_(column_name));
      J_OBJ_END();
      return pos;
    }

    common::ObString table_name_;
    common::ObString column_name_;
  };

private:
  int calc_token_func_obj(ObProxyTokenNode *token,
                          proxy::ObClientSessionInfo *client_session_info,
                          common::ObObj &target_obj,
                          obutils::SqlFieldResult *sql_field_result,
                          const bool is_oracle_mode,
                          ObProxyExprType &type);
  int calc_token_hex_obj(ObProxyTokenNode *token, common::ObObj &target_obj);
  int get_obj_with_param(common::ObObj &target_obj,
                         proxy::ObProxyMysqlRequest *client_request,
                         proxy::ObClientSessionInfo *client_info,
                         proxy::ObProxyPartInfo *part_info,
                         proxy::ObPsIdEntry *ps_entry,
                         const int64_t param_index);
  int convert_token_node_to_param_node(ObProxyTokenNode *token,
                                       ObProxyParamNode *&param);
  int recursive_convert_func_token(ObProxyTokenNode *token,
                                   ObProxyParamNode *param);

  static int convert_obj_to_sql_column_value(const common::ObObj &src_obj,
                                             obutils::SqlColumnValue &dest_val);

  ObProxyExprType get_expr_token_func_type(common::ObString *func);

private:
  static const ObProxyExprType DEFAULT_EXPR_TYPE = ObProxyExprType::OB_PROXY_EXPR_TYPE_NONE;
  bool is_inited_;
  bool is_oracle_mode_;
  proxy::ObProxyMysqlRequest& client_request_;
  proxy::ObClientSessionInfo& client_info_;
  proxy::ObProxyPartInfo& part_info_;

  int64_t part_key_num_;
  int64_t relation_num_;
  // objs from relation
  common::ObSEArray<common::ObObj, OBPROXY_MAX_PART_KEY_NUM> relation_obj_arr_;
  common::ObSEArray<ObProxyFunctionType, OBPROXY_MAX_PART_KEY_NUM> relation_type_arr_;

  common::ObSEArray<common::ObObj, 1> rowid_obj_arr_;

  common::ObSEArray<common::ObObj, 1> first_part_func_result_;
  common::ObSEArray<common::ObObj, 1> sub_part_func_result_;
  // objs to generate range
  common::ObSEArray<common::ObObj, OBPROXY_MAX_PART_KEY_NUM> left_bound_arr_;
  common::ObSEArray<common::ObObj, OBPROXY_MAX_PART_KEY_NUM> equal_obj_arr_;
  common::ObSEArray<common::ObObj, OBPROXY_MAX_PART_KEY_NUM> right_bound_arr_;
  common::ObSEArray<bool, OBPROXY_MAX_PART_KEY_NUM> left_bound_flag_arr_;
  common::ObSEArray<bool, OBPROXY_MAX_PART_KEY_NUM> right_bound_flag_arr_;

  common::ObNewRange ranges_[OBPROXY_MAX_PART_LEVEL];
  proxy::ObRouteDiagnosis *route_diagnosis_;
  common::ObIAllocator &allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObExprResolverV2);
};

class ObFuncExprTool
{
public:
  static int calc_int_value_from_func_parser(ObProxyParamNode *param_node, int64_t &int_value);
  static int calc_str_value_from_func_parser(ObProxyParamNode *param_node, common::ObString &str_value);
};

class ObPartkeyFuncInfo
{
public:
  ObPartkeyFuncInfo(): func_params_(NULL), type_(F_NONE) {}
  virtual ~ObPartkeyFuncInfo() { }

  void set_func_params(ObProxyParamNode *val) { func_params_ = val; }
  void set_type(const ObProxyFunctionType type) { type_ = type; }
public:
  ObProxyParamNode *func_params_;
  ObProxyFunctionType type_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPartkeyFuncInfo);
};

} // end of namespace opsql
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBEXPR_RESOLVER_H
