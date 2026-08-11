/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_EXPR_CALCULATOR_H
#define OBPROXY_EXPR_CALCULATOR_H
#include "opsql/expr_parser/ob_expr_parse_result.h"
#include "lib/charset/ob_charset.h"
#include "common/ob_obj_type.h"
#include "lib/container/ob_iarray.h"
#include "common/ob_object.h"
#include "lib/container/ob_se_array.h"
#include "obkv/table/ob_table.h"
#include "obkv/table/ob_table_rpc_request.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObArenaAllocator;
class ObString;
class ObTimeZoneInfo;
class ObDataTypeCastParams;
class ObRowkey;
}
namespace obproxy
{
namespace obkv
{
class ObTableOperation;
class ObTableQuery;
}
namespace opsql
{
class ObExprResolverResult;
}
namespace obutils
{
class ObSqlParseResult;
}
namespace proxy
{
class ObProxyMysqlRequest;
class ObProxyPartInfo;
class ObClientSessionInfo;
class ObPsIdEntry;
class ObTextPsEntry;
class ObServerRoute;
class ObRpcReq;
class ObRouteDiagnosis;

class ObProxyExprCalculator
{
public:
  ObProxyExprCalculator() : route_diagnosis_(NULL) {}
  ~ObProxyExprCalculator() { set_route_diagnosis(NULL); }
  int calculate_partition_id(common::ObArenaAllocator &allocator,
                             const common::ObString &req_sql,
                             const obutils::ObSqlParseResult &parse_result,
                             ObProxyMysqlRequest &client_request,
                             ObClientSessionInfo &client_info,
                             ObServerRoute &route,
                             ObProxyPartInfo &part_info,
                             int64_t &partition_id);
  int calc_part_id_by_random_choose_from_exist(ObProxyPartInfo &part_info,
                                               int64_t &first_part_id,
                                               int64_t &sub_part_id,
                                               int64_t &phy_part_id,
                                               int64_t &first_part_index,
                                               int64_t &sub_part_index);

  int calculate_partition_id_for_rpc(common::ObArenaAllocator &allocator,
                                     ObRpcReq &ob_rpc_req,
                                     // ObRpcClientSessionInfo &client_info,
                                     ObProxyPartInfo &part_info,
                                     int64_t &partition_id);

  void set_route_diagnosis(ObRouteDiagnosis *route_diagnosis);
private:
  // do parse -> do resolve -> do partition id calc
  int do_expr_parse(const common::ObString &req_sql,
                    const obutils::ObSqlParseResult &parse_result,
                    common::ObIAllocator &allocator,
                    ObExprParseResult &expr_result,
                    common::ObCollationType connection_collation);
  int do_expr_resolve(ObExprParseResult &expr_result,
                      const ObProxyMysqlRequest &client_request,
                      ObClientSessionInfo *client_info,
                      ObPsIdEntry *ps_id_entry,
                      ObTextPsEntry *text_ps_entry,
                      ObProxyPartInfo &part_info,
                      common::ObIAllocator &allocator,
                      opsql::ObExprResolverResult &resolve_result,
                      const obutils::ObSqlParseResult &sql_parse_result,
                      int64_t &partition_id);
  int do_partition_id_calc(opsql::ObExprResolverV2 &expr_resolver,
                           ObClientSessionInfo &client_info,
                           ObProxyPartInfo &part_info,
                           const obutils::ObSqlParseResult &parse_result,
                           common::ObIAllocator &allocator,
                           int64_t &first_part_id,
                           int64_t &sub_part_id,
                           int64_t &partition_id,
                           int64_t &first_part_index,
                           int64_t &sub_part_index);

  int calculate_partition_id_for_obkv(common::ObArenaAllocator &allocator,
                                      ObRpcReq &client_request,
                                      ObProxyPartInfo &part_info,
                                      int64_t &partition_id);

  int calculate_partition_id_for_redis(common::ObArenaAllocator &allocator,
                                       ObRpcReq &client_request,
                                       ObProxyPartInfo &part_info,
                                       int64_t &partition_id);

  int handle_hint_route_info(const obutils::ObSqlParseResult& parse_result,
                             const ObProxyPartKeyInfo& part_info,
                             ObIArray<ObObj>& equal_obj_arr);
  int do_expr_parse_diagnosis(ObExprParseResult &expr_result);

  struct PartitionColumn{
    inline bool operator==(const PartitionColumn& other) const {
      return (column_name_ == other.column_name_)
             && (table_name_ == other.table_name_);
    }

    inline bool operator!=(const PartitionColumn& other) const {
      return !(*this == other);
    }

    void assgin(const ObProxyTokenNode& column_node) {
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

    int to_string(char *buf, const int64_t buf_len) const {
      int64_t pos = 0;
      J_OBJ_START();
      J_KV(K_(table_name), K_(column_name));
      J_OBJ_END();
      return pos;
    }

    ObString table_name_;
    ObString column_name_;
  };

  ObRouteDiagnosis *route_diagnosis_;
};


class ObRpcExprCalcTool 
{
public:
  static int calculate_partition_id_with_rowkey(common::ObArenaAllocator &allocator,
                                                obkv::ROWKEY_VALUE_PARAM &rowkey_value,
                                                obkv::ROWKEY_COLUMN_PARAM &rowkey_columns,
                                                ObProxyPartInfo &part_info,
                                                int64_t &partition_id);
  static int do_eval_rowkey_index(ObProxyPartInfo &proxy_part_info,
                                  ObProxyPartKeyLevel level,
                                  const ObString &src_name,
                                  int &src_key_idx,
                                  const common::ObIArray<common::ObString> &rowkey_columns_name,
                                  common::ObIArray<int64_t> &rowkey_index,
                                  int64_t &idx_in_rowid);
  static int eval_rowkey_index(ObProxyPartInfo &proxy_part_info,
                               ObProxyPartKeyLevel level,
                               const common::ObIArray<common::ObString> &rowkey_columns_name,
                               common::ObIArray<int64_t> &rowkey_index,
                               common::ObIArray<int64_t> &part_info_index,
                               int64_t &idx_in_rowid);

      // eval part key from rowkey, stored in eval_rowkey
  static int eval_rowkey_values(ObProxyPartInfo &proxy_part_info,
                                const ObRowkey &rowkey,
                                common::ObArenaAllocator &allocator,
                                common::ObIArray<int64_t> &rowkey_index,
                                common::ObIArray<int64_t> &part_info_index,
                                ObRowkey &eval_part_rowkey,
                                const obkv::ObTableEntityType entity_type);
  static int do_partition_id_calc_for_obkv(opsql::ObExprResolverResult &resolve_result,
                                           // ObRpcClientSessionInfo &client_info,
                                           ObProxyPartInfo &part_info,
                                           common::ObIAllocator &allocator,
                                           common::ObIArray<int64_t> &partition_ids);
  static void trim_part_key_name(const ObString &part_key_name, ObString &trim_name);
};

class ObExprCalcTool {
public:
  static int build_dtc_params_with_tz_info(ObClientSessionInfo *session_info,
                                           common::ObObjType obj_type,
                                           common::ObTimeZoneInfo &tz_info,
                                           common::ObDataTypeCastParams &dtc_params);
  static int build_tz_info(ObClientSessionInfo *session_info,
                           common::ObObjType obj_type,
                           common::ObTimeZoneInfo &tz_info);

  static int build_tz_info_for_all_type(ObClientSessionInfo *session_info,
                                        common::ObTimeZoneInfo &tz_info);
  static int build_dtc_params(ObClientSessionInfo *session_info,
                              common::ObObjType obj_type,
                              common::ObDataTypeCastParams &dtc_params);
  static bool is_contains_null_params(common::ObSEArray<common::ObObj, 4> &param_result);
};


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_EXPR_CALCULATOR_H */
