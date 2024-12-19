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

#ifndef OBPROXY_RPC_REQ_ANALYZER_H
#define OBPROXY_RPC_REQ_ANALYZER_H
#include "rpc/obrpc/ob_rpc_packet.h"


namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObArenaAllocator;
class ObString;
}
namespace obproxy
{
namespace obutils
{
class ObClusterResource;
class ObCachedVariables;
struct SqlFieldResult;
}
namespace obkv
{
class ObRpcPacketMeta;
}
namespace event
{
class ObIOBufferReader;
}
namespace proxy
{
class ObRpcReq;
class ObRpcReqCtx;
class ObTableEntry;

enum ObRpcReqAnalyzeNewStatus
{
  RPC_ANALYZE_NEW_CAN_NOT_PASS_WHITE_LIST_ERROR = -3,
  RPC_ANALYZE_NEW_OBPARSE_ERROR = -2,
  RPC_ANALYZE_NEW_ERROR = -1,
  RPC_ANALYZE_NEW_DONE = 0,
  RPC_ANALYZE_NEW_OK = 1,
  RPC_ANALYZE_NEW_CONT = 2
};

struct ObProxyRpcReqAnalyzeCtx
{
  ObProxyRpcReqAnalyzeCtx() { reset(); }
  ~ObProxyRpcReqAnalyzeCtx() { }
  ObProxyRpcReqAnalyzeCtx(const ObProxyRpcReqAnalyzeCtx &other) {
    if (&other != this) {
      status_ = other.status_;
      analyze_pos_ = other.analyze_pos_;
      cluster_version_ = other.cluster_version_;

      vip_tenant_name_ = other.vip_tenant_name_;
      vip_cluster_name_ = other.vip_cluster_name_;
      has_tenant_username_ = other.has_tenant_username_;
      has_cluster_username_ = other.has_cluster_username_;

      is_response_ = other.is_response_;
      need_retry_ = other.need_retry_;
      need_rewrite_ = other.need_rewrite_;
      need_reroute_ = other.need_reroute_;
      dirty_table_entry_ = other.dirty_table_entry_;
      dirty_partition_entry_ = other.dirty_partition_entry_;

      is_inner_request_ = other.is_inner_request_;
    }
  }

  void reset() { memset(this, 0, sizeof(ObProxyRpcReqAnalyzeCtx)); }

  TO_STRING_KV(K_(status), K_(analyze_pos), K_(cluster_version),
               K_(vip_tenant_name), K_(vip_cluster_name),
               K_(is_response), K_(need_retry), K_(need_rewrite),
               K_(need_reroute), K_(dirty_table_entry), K_(dirty_partition_entry),
               K_(is_inner_request));

  // common
  ObRpcReqAnalyzeNewStatus status_;
  int64_t analyze_pos_;
  int64_t cluster_version_;

  // for request
  common::ObString vip_tenant_name_;
  common::ObString vip_cluster_name_;
  bool has_tenant_username_;
  bool has_cluster_username_;

  // for response
  bool is_response_;
  bool need_retry_;
  bool need_rewrite_;
  bool need_reroute_;
  bool dirty_table_entry_;
  bool dirty_partition_entry_;

  // inner request && response
  bool is_inner_request_;
};

class ObProxyRpcReqAnalyzer
{
public:
static const int64_t RPC_NET_HEADER = 16;
static const int64_t OB_RPC_ANALYZE_MORE_BUFF_LEN = 32;
static const int64_t OB_RPD_AUTH_REQEUST_BUF_LEN = 128;
static const int64_t OB_RPC_HEADER_PCODE_AND_HLEN_LEN = 8;
static const uint32_t OB_RPC_BIG_PACKET_LEN = 1048576;  // equal to (1024 * 1024) aka 1M
static const char FORMAL_USER_TENANT_SEPARATOR = '@';
static const char FORMAL_TENANT_CLUSTER_SEPARATOR = '#';
static const char CLUSTER_ID_SEPARATOR = ':';

/*
 * Currently, for rewriting kv requests, serialization of partition id and table id is required:
 * For 3.x, it is required that the partition ID and table ID passed in by the driver are both -1, and the serialization length is 10, which is convenient for ODP rewriting.
 * For 4.x, the table id defaults to 8-byte serialization, only the table id is required
*/
static const int64_t PARTITION_ID_MAX_LEN = 10;
static const int64_t TABLE_ID_MAX_LEN = 10;
static const int64_t TABLET_ID_LEN = 8;
public:
  static int  analyze_rpc_packet_meta(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req);
  static int  analyze_rpc_request(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req);
  static int  analyze_rpc_response(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req);
  static int  handle_server_failed(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req);
  static int  handle_rpc_response(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req);
  static int  handle_query_async_response(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req);
  static int  handle_login_response(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req);
  static bool required_async_analyze(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req);

  static int get_parse_allocator(common::ObArenaAllocator *&allocator);

  static int do_parse_full_user_name(ObRpcReqCtx &rpc_ctx,
                                     const common::ObString &user_name,
                                     const char separator,
                                     ObProxyRpcReqAnalyzeCtx &ctx);
  static int do_parse_full_redis_auth_info(ObRpcReqCtx &rpc_ctx,
                                               const common::ObString &full_name,
                                               const common::ObString &user_name,
                                               const char separator,
                                               ObProxyRpcReqAnalyzeCtx &ctx,
                                               bool is_cloud_user);
  static int do_parse_auth_result(ObRpcReqCtx &rpc_ctx,
                                  const char ut_separator,
                                  const char tc_separator,
                                  const char cluster_id_separator,
                                  const common::ObString &user,
                                  const common::ObString &tenant,
                                  const common::ObString &cluster,
                                  const common::ObString &cluster_id_str);

  static int handle_obkv_request_rewrite(ObRpcReq &ob_rpc_req);
  static int handle_obkv_response_rewrite(ObRpcReq &ob_rpc_req);
  static int handle_obkv_login_rewrite(ObRpcReq &ob_rpc_req);
  static int handle_obkv_execute_rewrite(ObRpcReq &ob_rpc_req);
  static bool obkv_execute_could_rewrite(int64_t partition_id_len, int64_t table_id_len, int64_t cluster_version);

  static int handle_obkv_serialize_request(ObRpcReq &ob_rpc_req);

  static int reset_obkv_request_before_send(ObRpcReq &ob_rpc_req);

  static int handle_obkv_serialize_response(ObRpcReq &ob_rpc_req);

  static int build_error_response(ObRpcReq &ob_rpc_req, int err_code);
  static int get_rpc_request_size(const obrpc::ObRpcPacketCode pcode, int64_t &size);
  static int get_rpc_response_size(const obrpc::ObRpcPacketCode pcode, int64_t &size);

  static int build_get_partition_response(ObRpcReq &ob_rpc_req, ObTableEntry &table_entry);

  static int build_empty_query_response(ObRpcReq &ob_rpc_req);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_RPC_REQUEST_ANALYZER_H
