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
#ifndef OBPROXY_RPC_REQ_H
#define OBPROXY_RPC_REQ_H

#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_ls_id.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "obkv/table/ob_rpc_struct.h"
#include "proxy/rpc_optimize/ob_rpc_req_trace.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_time_stat.h"
#include "proxy/route/ob_ldc_struct.h"
#include "proxy/route/ob_ldc_location.h"
#include "proxy/mysql/ob_mysql_sm_time_stat.h"
#include "utils/ob_proxy_lib.h"
#include "lib/list/ob_list.h"
#include "obkv/table/ob_table.h"

namespace oceanbase
{
namespace obrpc
{
enum ObRpcPacketCode;
}
namespace common
{
class ObIAllocator;
class ObAddr;
class ObString;
}
namespace obproxy
{
namespace obkv
{
class ObRpcRequest;
class ObRpcResponse;
}
namespace event
{
class ObContinuation;
class ObIOBufferReader;
}
namespace net
{
class ObNetVConnection;
}
namespace proxy
{
// class ObRpcReqTraceId;
class ObRpcReqBaseInfo;
class ObRpcOBKVInfo;
class ObRpcClientNetHandler;
class ObRpcServerNetHandler;
class ObRpcRequestSM;
class ObTableEntry;
class ObLDCLocation;
class ObTableQueryAsyncEntry;
class ObTableGroupEntry;
class ObIndexEntry;
class ObRpcReqCtx;

enum ObRpcReqMagic
{
  RPC_REQ_MAGIC_ALIVE = 0xFEED0000,
  RPC_REQ_SM_MAGIC_DEAD = 0xFEEDDEAD
};

struct ObConnectionAttributes
{
  enum ObServerStateType
  {
    STATE_UNDEFINED = 0,
    ACTIVE_TIMEOUT,
    CONNECTION_ALIVE,
    CONNECTION_CLOSED,
    CONNECTION_ERROR,
    CONNECT_ERROR,
    RESPONSE_ERROR,
    INACTIVE_TIMEOUT,
    ANALYZE_ERROR,
    CMD_COMPLETE,
    TRANSACTION_COMPLETE,
    DEAD_CONGESTED,
    ALIVE_CONGESTED,
    FORCE_BL_REFUSED,
    INTERNAL_ERROR
  };

  enum ObAbortStateType
  {
    ABORT_UNDEFINED = 0,
    DIDNOT_ABORT,
    MAYBE_ABORTED,
    ABORTED
  };

  ObConnectionAttributes()
      : addr_(),
        sql_addr_(),
        obproxy_addr_(),
        state_(STATE_UNDEFINED),
        abort_(ABORT_UNDEFINED)
  {
  }

  ~ObConnectionAttributes() { }

  // in these function the parameter and return value is in host-order EXPECT FOR sockaddr
  void set_addr(const uint32_t ipv4, const uint16_t port) { net::ops_ip_copy(addr_, ipv4, port); }
  void set_addr(const sockaddr &sa) { net::ops_ip_copy(addr_, sa); }
  void set_addr(const net::ObIpEndpoint &ip_point) { net::ops_ip_copy(addr_, ip_point.sa_); }
  void set_sql_addr(const uint32_t ipv4, const uint16_t port) { net::ops_ip_copy(sql_addr_, ipv4, port); }
  void set_sql_addr(const sockaddr &sa) { net::ops_ip_copy(sql_addr_, sa); }
  void set_obproxy_addr(const sockaddr &sa) { net::ops_ip_copy(obproxy_addr_, sa); }

  uint32_t get_ipv4() { return net::ops_ip4_addr_host_order(addr_.sa_); }
  uint16_t get_port() { return net::ops_ip_port_host_order(addr_); }

  net::ObIpEndpoint addr_;    // use function below to get/set ip and port
  net::ObIpEndpoint sql_addr_;    // sql service addr used to congestion info
  net::ObIpEndpoint obproxy_addr_;

  ObServerStateType state_;
  ObAbortStateType abort_;
  void reset()
  {
    state_ = STATE_UNDEFINED;
    abort_ = ABORT_UNDEFINED;
    addr_.reset();
    sql_addr_.reset();
    obproxy_addr_.reset();
  }

  TO_STRING_KV(K_(addr), K_(sql_addr), K_(obproxy_addr),
               K_(state), K_(abort));

private:
  DISALLOW_COPY_AND_ASSIGN(ObConnectionAttributes);
};
class ObRpcReq;

class ObConfigVarStr
{
public:
  ObConfigVarStr() : buf_(), dynamic_buf_(NULL), use_dynamic_buf_(false),
                     var_str_buf_len_(OB_MAX_CONFIG_SECTION_LEN)
  {}

  ~ObConfigVarStr() {
    reset();
  }

  void reset() {
    if (OB_UNLIKELY(OB_NOT_NULL(dynamic_buf_))) {
      op_fixed_mem_free(dynamic_buf_, var_str_buf_len_);
    }
    use_dynamic_buf_ = false;
    var_str_buf_len_ = OB_MAX_CONFIG_SECTION_LEN;
  }

  void check_and_extend_str(uint32_t str_len) {
    if (OB_UNLIKELY(str_len >= var_str_buf_len_)) {
      if (OB_NOT_NULL(dynamic_buf_)) {
        op_fixed_mem_free(dynamic_buf_, var_str_buf_len_);
        var_str_buf_len_ = 0;
      }
      dynamic_buf_ = (char *)op_fixed_mem_alloc(str_len + 1);
      var_str_buf_len_ = str_len + 1;
      use_dynamic_buf_ = true;
    }
  }

  bool is_use_dynamic_buf() const { return use_dynamic_buf_; }

  void mem_reset() { memset(get_config_str(), 0, var_str_buf_len_); }

  char *get_config_str() const { return  (!use_dynamic_buf_) ? (char *)buf_ : dynamic_buf_; }

  uint32_t get_config_buf_len() const { return var_str_buf_len_; }

  void deep_copy(const ObConfigVarStr &item) {
    char *buf = NULL;
    if (OB_LIKELY(item.get_config_buf_len() > OB_MAX_CONFIG_SECTION_LEN)) {
      use_dynamic_buf_ = true;
      check_and_extend_str(item.get_config_buf_len());
      buf = dynamic_buf_;
    } else {
      use_dynamic_buf_ = false;
      buf = buf_;
      var_str_buf_len_ = OB_MAX_CONFIG_SECTION_LEN;
    }
    MEMCPY(buf, item.get_config_str(), item.get_config_buf_len());
  }

  TO_STRING_KV(K_(buf), K_(use_dynamic_buf));

private:
  char buf_[OB_MAX_CONFIG_SECTION_LEN]; //128, more not more than 128
  char *dynamic_buf_; //use it when big than 128
  bool use_dynamic_buf_; //not more than 4096
  uint32_t var_str_buf_len_;
};

class ObRpcRequestConfigInfo
{
public:
  ObRpcRequestConfigInfo () : config_version_(0), proxy_route_policy_(),
    enable_cloud_full_username_(false),
    rpc_support_key_partition_shard_request_(false),
    rpc_enable_force_srv_black_list_(false),
    rpc_enable_direct_expire_route_entry_(false),
    rpc_enable_reroute_(false),
    rpc_enable_congestion_(false), rpc_enable_global_index_(false),
    rpc_enable_retry_request_info_log_(false), rpc_request_max_retries_(0),
    rpc_request_timeout_(0), rpc_request_timeout_delta_(0),
    rpc_request_retry_waiting_time_(0)
  {}
  ObRpcRequestConfigInfo(const ObRpcRequestConfigInfo& config_info) {
    MEMCPY(this, &config_info, sizeof(ObRpcRequestConfigInfo));
  }
  ~ObRpcRequestConfigInfo() {}

  void deep_copy(const ObRpcRequestConfigInfo &config_info) {
    MEMCPY(this, &config_info, sizeof(ObRpcRequestConfigInfo));
    if (OB_UNLIKELY(config_info.proxy_route_policy_.is_use_dynamic_buf())) {
      MEMSET(&proxy_route_policy_, 0, sizeof(ObConfigVarStr));
      proxy_route_policy_.deep_copy(config_info.proxy_route_policy_);
    }
  }

  bool is_init() const { return 0 != config_version_; }

  TO_STRING_KV(K_(config_version), K_(proxy_route_policy), K_(enable_cloud_full_username), K_(rpc_support_key_partition_shard_request),
               K_(rpc_enable_force_srv_black_list), K_(rpc_enable_direct_expire_route_entry),
               K_(rpc_enable_reroute), K_(rpc_enable_congestion), K_(rpc_enable_global_index), K_(rpc_enable_retry_request_info_log),
               K_(rpc_request_max_retries), K_(rpc_request_timeout), K_(rpc_request_timeout_delta), K_(rpc_request_retry_waiting_time));

public:
  uint64_t config_version_;
  //for common config
  ObConfigVarStr proxy_route_policy_;
  // ObConfigVarStr proxy_idc_name_; //not used now
  bool enable_cloud_full_username_;

  //for rpc config
  bool rpc_support_key_partition_shard_request_;
  bool rpc_enable_force_srv_black_list_;
  bool rpc_enable_direct_expire_route_entry_;
  bool rpc_enable_reroute_;
  bool rpc_enable_congestion_;
  bool rpc_enable_global_index_;
  bool rpc_enable_retry_request_info_log_;
  int64_t rpc_request_max_retries_;
  int64_t rpc_request_timeout_;
  int64_t rpc_request_timeout_delta_;
  int64_t rpc_request_retry_waiting_time_;
};


class ObRpcOBKVInfo
{
  static const int SCHEMA_LENGTH = 100;
public:
  enum OBKVInfoFlags {
    HBASE_FLAG                   = 33,
    EMPTY_QUERY_RESULT_FLAG      = 32,
    DIRECT_LOAD_FLAG             = 31,
    GLOBAL_INDEX_ROUTE_FLAG      = 30,
    QUERY_WITH_INDEX             = 29,
    NEED_GLOBAL_INDEX_RETRY_FLAG = 28,
    NEED_RETRY_FLAG              = 27,
    RESP_REROUTE_INFO_FLAG       = 26,
    RESP_COMPLETED_FLAG          = 25,
    ERROR_FLAG                   = 24,
    INTERNAL_FLAG                = 23,
    SINGLE_FLAG                  = 22,
    STREAM_QUERY_FLAG            = 21,          // 对应is_stream_query_flag, 后续可能考虑优化一下名字
    QUERY_FLAG                   = 20,
    BATCH_FALG                   = 19,
    AUTH_FLAG                    = 18,
    READ_WEAK_FLAG               = 17,
    SHARD_FLAG                   = 16,
    // The first 16 flags are the flag bits in the RPC header
    RESP_FLAG                    = 15,
    STREAM_FLAG                  = 14,
    STREAM_LAST_FLAG             = 13,
    DISABLE_DEBUGSYNC_FLAG       = 12,
    CONTEXT_FLAG                 = 11,
    UNNEED_RESPONSE_FLAG         = 10,
    BAD_ROUTING_FLAG             = 9,
    ENABLE_RATELIMIT_FLAG        = 8,
    BACKGROUND_FLOW_FLAG         = 7
  };
public:
  ObRpcOBKVInfo() :request_id_(0), server_request_id_(0), is_first_direct_load_request_(false), is_inner_request_(false),
                   cluster_name_(), tenant_name_(), user_name_(), table_name_(), database_name_(), full_username_(),
                   cluster_id_(0), tenant_id_(0), table_id_(0), partition_id_(common::OB_INVALID_INDEX), client_info_(),
                   server_info_(), route_policy_(1), cs_read_consistency_(0), is_proxy_route_policy_set_(false),
                   is_read_consistency_set_(false), proxy_route_policy_(MAX_PROXY_ROUTE_POLICY), pcode_(obrpc::OB_INVALID_RPC_CODE),
                   flags_(0), rpc_origin_error_code_(0), rpc_request_retry_last_begin_(0), rpc_request_retry_times_(0),
                   rpc_request_reroute_moved_times_(0), query_async_entry_(NULL), rpc_ctx_(NULL), data_table_id_(OB_INVALID_ID),
                   index_name_(), index_entry_(NULL), index_table_name_(), need_add_into_cache_(false), tablegroup_entry_(NULL), dummy_ldc_(), dummy_entry_(NULL),
                   is_set_rpc_trace_id_(false), rpc_trace_id_(), credential_(), inner_req_retries_(0)
                   { index_table_name_buf_[0] = '\0'; }
  ~ObRpcOBKVInfo() {}

  void reset();
  void set_rpc_trace_id(const uint64_t id, const uint64_t ipport);
  void set_rpc_trace_sub_index(const uint64_t sub_index) { rpc_trace_id_.set_rpc_sub_index_id(sub_index, inner_req_retries_); }

  bool need_parse_response_fully() const;

  void retry_reset() {
    set_error_resp(false);
    set_resp_completed(false);
    set_resp_reroute_info(false);
    set_need_retry(false);
    set_need_retry_with_global_index(false);
    set_definitely_single(false);

    partition_id_ = OB_INVALID_INDEX;
    table_id_ = 0;
    ls_id_ = common::ObLSID::INVALID_LS_ID; 
  }

  // common::ObString get_req_trace_id(); //打印trace id使用
  const ObRpcReqTraceId &get_req_trace_id() { return rpc_trace_id_; }
  ObRpcReqTraceId &get_req_trace_id_no_const() { return rpc_trace_id_; }
  // get_rowkey_info();
  void set_partition_id(int64_t partition_id) { partition_id_ = partition_id; }
  void set_table_id(int64_t table_id) { table_id_ = table_id; }
  void set_ls_id(int64_t ls_id) { ls_id_ = ls_id; }

  int64_t get_partition_id() const { return partition_id_; }
  int64_t get_table_id() const { return table_id_; }
  int64_t get_ls_id() const { return ls_id_; }

  void set_flag(int offset, bool flag) {
    if (flag) {
      flags_ |= static_cast<uint64_t>(1) << offset;
    } else {
      flags_ &= ~(static_cast<uint64_t>(1) << offset);
    }
  }

  bool get_flag(int offset) const {
    return flags_ & static_cast<uint64_t>(1) << offset;
  }

  /* flag for request */
  void set_auth(const bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::AUTH_FLAG), flag); }
  void set_definitely_single(const bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::SINGLE_FLAG), flag); }
  void set_batch(const bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::BATCH_FALG), flag); }
  void set_shard(const bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::SHARD_FLAG), flag); }
  void set_stream(const bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::STREAM_FLAG), flag);}
  void set_stream_query(const bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::STREAM_QUERY_FLAG), flag); }
  void set_internal_req(const bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::INTERNAL_FLAG), flag); }
  void set_read_weak(bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::READ_WEAK_FLAG), flag); }
  void set_need_retry(bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::NEED_RETRY_FLAG), flag); }
  void set_need_retry_with_global_index(bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::NEED_GLOBAL_INDEX_RETRY_FLAG), flag); }
  void set_direct_load_req(const bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::DIRECT_LOAD_FLAG), flag);}
  void set_empty_query_result(const bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::EMPTY_QUERY_RESULT_FLAG), flag);}

  /* flag for response */
  void set_resp(bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::RESP_FLAG), flag); }
  void set_bad_routing(bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::BAD_ROUTING_FLAG), flag); }
  void set_error_resp(bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::ERROR_FLAG), flag); }
  void set_resp_completed(bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::RESP_COMPLETED_FLAG), flag); }
  void set_resp_reroute_info(bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::RESP_REROUTE_INFO_FLAG), flag); }
  void set_global_index_route(bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::GLOBAL_INDEX_ROUTE_FLAG), flag); }
  void set_query_with_index(bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::QUERY_WITH_INDEX), flag); }
  void set_hbase_request(bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::HBASE_FLAG), flag); }
  void set_first_direct_load_request(bool flag) { is_first_direct_load_request_ = flag; }
  void set_pcode(obrpc::ObRpcPacketCode pcode) { pcode_ = pcode; }

  /* flag for request */
  bool is_auth() const { return get_flag(static_cast<int>(OBKVInfoFlags::AUTH_FLAG)); }
  bool is_definitely_single() const { return get_flag(static_cast<int>(OBKVInfoFlags::SINGLE_FLAG)); }
  bool is_batch() const { return get_flag(static_cast<int>(OBKVInfoFlags::BATCH_FALG)); }
  bool is_shard() const { return get_flag(static_cast<int>(OBKVInfoFlags::SHARD_FLAG)); }
  bool is_stream() const { return get_flag(static_cast<int>(OBKVInfoFlags::STREAM_FLAG));}
  bool is_stream_query() const { return get_flag(static_cast<int>(OBKVInfoFlags::STREAM_QUERY_FLAG)); }
  bool is_internal_req() const { return get_flag(static_cast<int>(OBKVInfoFlags::INTERNAL_FLAG)); }
  bool is_read_weak() const { return get_flag(static_cast<int>(OBKVInfoFlags::READ_WEAK_FLAG)); }
  bool is_need_retry() const { return get_flag(static_cast<int>(OBKVInfoFlags::NEED_RETRY_FLAG)); }
  bool is_need_retry_with_global_index() const { return get_flag(static_cast<int>(OBKVInfoFlags::NEED_GLOBAL_INDEX_RETRY_FLAG)); }
  bool is_direct_load_req() const { return get_flag(static_cast<int>(OBKVInfoFlags::DIRECT_LOAD_FLAG)); }
  bool is_empty_query_result() const { return get_flag(static_cast<int>(OBKVInfoFlags::EMPTY_QUERY_RESULT_FLAG)); }
  bool is_hbase_request() const { return get_flag(static_cast<int>(OBKVInfoFlags::HBASE_FLAG)); }
  bool is_first_direct_load_request() const { return is_first_direct_load_request_; }

  bool is_need_retry_with_query_async() const;

  bool is_hbase_empty_family() const;

  /* flag for response */
  bool is_resp() const { return get_flag(static_cast<int>(OBKVInfoFlags::RESP_FLAG)); }
  bool is_bad_routing() const { return get_flag(static_cast<int>(OBKVInfoFlags::BAD_ROUTING_FLAG)); }
  bool is_error() const { return get_flag(static_cast<int>(OBKVInfoFlags::ERROR_FLAG)); }
  bool is_resp_completed() const { return get_flag(static_cast<int>(OBKVInfoFlags::RESP_COMPLETED_FLAG)); }
  bool is_respo_reroute_info() const { return get_flag(static_cast<int>(OBKVInfoFlags::RESP_REROUTE_INFO_FLAG)); }
  bool is_global_index_route() const { return get_flag(static_cast<int>(OBKVInfoFlags::GLOBAL_INDEX_ROUTE_FLAG)); }
  bool is_query_with_index() const { return get_flag(static_cast<int>(OBKVInfoFlags::QUERY_WITH_INDEX)); }
  bool is_async_query_request() const { return obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC == pcode_; }
  bool is_inner_req_retrying() const { return is_inner_request_ && inner_req_retries_ > 0; }

  int32_t get_error_code() const { return rpc_origin_error_code_; }
  obrpc::ObRpcPacketCode get_pcode() const { return pcode_; }
  inline bool is_request_has_retried() const { return rpc_request_retry_times_ > 0 || rpc_request_reroute_moved_times_ > 0; }

  void set_meta_flag(uint16_t meta_flag) { flags_ = ((flags_ & (~0xFFFFULL)) | static_cast<uint64_t>(meta_flag)); }
  //TODO add one response flag need clear it in reset_odp_resp_flag()
  void reset_odp_resp_flag() { set_bad_routing(false); set_error_resp(false); set_resp_completed(false); set_resp_reroute_info(false); set_need_retry(false); }
  // const common::ObString &get_server_trace_id() { return server_trace_id_; }

  int generate_index_table_name();
  void set_route_entry_dirty();

  bool is_rpc_req_can_retry() const
  {
    bool bret = false;
    bool route_error_retry = is_need_retry();
    bool global_index_retry = is_need_retry_with_global_index();
    bool query_async_retry = is_need_retry_with_query_async();
    uint32_t sub_req_retry_limit = obutils::get_global_proxy_config().rpc_sub_req_max_retries;
    if (!is_inner_request_) {
      bret = (route_error_retry || global_index_retry || query_async_retry);
    } else {
      bret = (inner_req_retries_ < sub_req_retry_limit && pcode_ == obrpc::OB_TABLE_API_LS_EXECUTE && route_error_retry);
    }
    return bret;
  }

  int set_dummy_entry(ObTableEntry *dummy_entry);

  TO_STRING_KV(K_(pcode), K_(request_id), K_(cluster_name), K_(tenant_name), K_(user_name), K_(table_name),
               K_(database_name), K_(cluster_id), K_(tenant_id), K_(table_id), K_(partition_id), K_(flags),
               K_(rpc_origin_error_code), KP_(query_async_entry), KP_(rpc_ctx), K_(index_table_name),
               KP_(index_entry), KP_(dummy_entry), K_(inner_req_retries), K_(rpc_trace_id));

public:
  uint32_t request_id_;
  uint32_t server_request_id_;          //init by pkt header, trace request

  bool is_first_direct_load_request_; //direct load request need
  bool is_inner_request_;

  common::ObString cluster_name_;
  common::ObString tenant_name_;
  common::ObString user_name_;
  common::ObString table_name_;
  common::ObString database_name_;
  common::ObString full_username_;

  int64_t cluster_id_;
  int64_t tenant_id_;
  int64_t table_id_;
  int64_t partition_id_;
  int64_t ls_id_;

  ObConnectionAttributes client_info_;
  ObConnectionAttributes server_info_;

  // ob route policy
  int64_t route_policy_;
  int64_t cs_read_consistency_;
  //when user set proxy_route_policy, set it true;
  bool is_proxy_route_policy_set_;
  bool is_read_consistency_set_;
  ObProxyRoutePolicyEnum proxy_route_policy_;

  obrpc::ObRpcPacketCode pcode_;
  /* store more info only used in OBKV mode */
  uint64_t flags_;
  int32_t rpc_origin_error_code_;

  int64_t rpc_request_retry_last_begin_;
  int64_t rpc_request_retry_times_;
  int64_t rpc_request_reroute_moved_times_;

  ObTableQueryAsyncEntry *query_async_entry_;
  ObRpcReqCtx *rpc_ctx_;

  // used by global index route
  uint64_t data_table_id_;
  ObString index_name_;
  ObIndexEntry *index_entry_;
  ObString index_table_name_;
  char index_table_name_buf_[OB_MAX_INDEX_TABLE_NAME_LENGTH];
  bool need_add_into_cache_;

  // used by hbase tablegroup
  ObTableGroupEntry *tablegroup_entry_;

  // dummy_entry and dummy ldc
  ObLDCLocation dummy_ldc_;
  ObTableEntry *dummy_entry_;

  bool is_set_rpc_trace_id_;
  ObRpcReqTraceId rpc_trace_id_;

  obkv::ObTableApiCredential credential_;

  uint32_t inner_req_retries_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcOBKVInfo);
};


class ObRpcReq  //根据网络中数据进行初始化
{
public:
  enum ClientNetState
  {
    RPC_REQ_CLIENT_INIT = 0,
    RPC_REQ_CLIENT_REQUEST_READ,
    RPC_REQ_CLIENT_REQUEST_HANDLING,
    RPC_REQ_CLIENT_RESPONSE_HANDLING,
    RPC_REQ_CLIENT_RESPONSE_SEND,
    RPC_REQ_CLIENT_INNER_REQUEST,
    RPC_REQ_CLIENT_DONE,
    RPC_REQ_CLIENT_INNER_REQUEST_DONE,
    RPC_REQ_CLIENT_DESTROY,
    RPC_REQ_CLIENT_CANCLED
  };

  enum ServerNetState
  {
    RPC_REQ_SERVER_INIT = 0,
    RPC_REQ_SERVER_ENTRY_LOOKUP,
    RPC_REQ_SERVER_ENTRY_WARTING,
    RPC_REQ_SERVER_ENTRY_LOOKUP_DONE,
    RPC_REQ_SERVER_REQUST_SENDING,
    RPC_REQ_SERVER_REQUST_SENDED,
    RPC_REQ_SERVER_RESPONSE_READING,
    RPC_REQ_SERVER_RESPONSE_READED,
    RPC_REQ_SERVER_SHARDING_REQUEST_HANDLING,
    RPC_REQ_SERVER_DONE,
    RPC_REQ_SERVER_SHARDING_REQUEST_DONE,
    RPC_REQ_SERVER_DESTROY,
    RPC_REQ_SERVER_CANCLED
  };

  enum RpcReqSmState
  {
    RPC_REQ_SM_INIT = 0,
    RPC_REQ_SM_REQUEST_ANALYZE,
    RPC_REQ_SM_CLUSTER_BUILD,
    RPC_REQ_SM_PARTITION_LOOKUP,
    RPC_REQ_SM_ADDR_SEARCH,
    RPC_REQ_SM_SHARDING_HANDLE,
    RPC_REQ_SM_SERVER_HANDLE,
    RPC_REQ_SM_RESPONE_RETURN,
    RPC_REQ_SM_REQUEST_DONE,
    RPC_REQ_SM_INNER_ERROR,
    RPC_REQ_SM_TIMEOUT,
    RPC_REQ_SM_INNER_REQUEST_CLEANUP,
    RPC_REQ_SM_REQUEST_CLEANUP,
    RPC_REQ_SM_DESTROYED
  };

  enum RpcReqCleanModule
  {
    RPC_REQ_CLEAN_MODULE_CLIENT_NET = 0,
    RPC_REQ_CLEAN_MODULE_SERVER_NET,
    RPC_REQ_CLEAN_MODULE_REQUEST_SM,
    RPC_REQ_CLEAN_MODULE_MAX
  };

  enum RpcReqCongestStatus
  {
    STATE_COMMON = 0,
    DEAD_CONGESTED,
    ALIVE_CONGESTED,
    DETECT_CONGESTED,
    SERVER_CONNECT_ERROR
  };

  struct ObRpcReqCleanupParams
  {
    ObRpcReqCleanupParams(ClientNetState cstate, ServerNetState sstate,
                          RpcReqSmState rstate, RpcReqCleanModule module)
      :cnet_state_(cstate), snet_state_(sstate), sm_state_(rstate), rpc_req_clean_module_(module) {}
    ObRpcReqCleanupParams(ClientNetState cstate)
      :cnet_state_(cstate), snet_state_(RPC_REQ_SERVER_INIT), sm_state_(RPC_REQ_SM_INIT), rpc_req_clean_module_(RPC_REQ_CLEAN_MODULE_CLIENT_NET) {}
    ObRpcReqCleanupParams(ServerNetState sstate)
      :cnet_state_(RPC_REQ_CLIENT_INIT), snet_state_(sstate), sm_state_(RPC_REQ_SM_INIT), rpc_req_clean_module_(RPC_REQ_CLEAN_MODULE_SERVER_NET) {}
    ObRpcReqCleanupParams(RpcReqSmState rstate)
      :cnet_state_(RPC_REQ_CLIENT_INIT), snet_state_(RPC_REQ_SERVER_INIT), sm_state_(rstate), rpc_req_clean_module_(RPC_REQ_CLEAN_MODULE_REQUEST_SM) {}

    ~ObRpcReqCleanupParams() {}

    ClientNetState cnet_state_;
    ServerNetState snet_state_;
    RpcReqSmState sm_state_;
    RpcReqCleanModule rpc_req_clean_module_;
  };

  #define RPC_REQ_CNET_ENTER_STATE(req, state)    \
            (req) ==  NULL ? UNUSED(state) : (req)->set_cnet_state(state);

  #define RPC_REQ_SNET_ENTER_STATE(req, state)    \
            (req) ==  NULL ? UNUSED(state) : (req)->set_snet_state(state);

  #define RPC_REQ_SM_ENTER_STATE(state)     \
            rpc_req_ == NULL ? UNUSED(state) : rpc_req_->set_sm_state(state);
public:
  ObRpcReq();
  ~ObRpcReq() {}

  int init(obkv::ObProxyRpcType rpc_type, ObRpcRequestSM *sm, ObRpcClientNetHandler *client_net,
          int64_t request_len, int64_t cluster_version, int64_t origin_channel_id, int64_t client_channel_id,
          int64_t cs_id, net::ObNetVConnection *rpc_net_vc, int64_t trace_id1, int64_t trace_id2);

  static inline ObRpcReq* allocate();
  static void instantiate_func(ObRpcReq &prototype, ObRpcReq &new_instance);

  static void make_scatter_list(ObRpcReq &prototype);

  // void reset();  // clear all buffer and state
  void cleanup(const ObRpcReqCleanupParams &params);
  void destroy(); //release this object
  void finish(); // update stat and clean request sm
  void inner_request_cleanup();   //Called when the subtask ends to clean up all subtask data

  char    *get_request_buf() { return request_buf_; }
  char    *get_request_inner_buf() { return request_inner_buf_; }
  char    *get_response_inner_buf() { return response_inner_buf_; }
  char    *get_response_buf() { return response_buf_; }
  int64_t  get_request_buf_len() const { return request_buf_len_; }
  int64_t  get_request_inner_buf_len() const { return request_inner_buf_len_; }
  int64_t  get_response_inner_buf_len() const { return response_inner_buf_len_; }
  int64_t  get_response_buf_len() const { return response_buf_len_; }
  int64_t  get_request_len() const { return request_len_; }
  int64_t  get_response_len() const { return response_len_; }
  // int64_t  get_base_info_len() const { return req_info_len_; }
  bool     is_response() const { return is_response_; }
  bool     is_server_addr_set() const { return is_server_addr_set_; }
  bool     is_use_request_inner_buf() const { return is_use_request_inner_buf_; }
  bool     is_use_response_inner_buf() const { return is_use_response_inner_buf_; }
  bool     is_server_failed() const { return is_server_failed_; }
  int      get_rpc_req_error_code() const { return obkv_info_.rpc_origin_error_code_; }
  obkv::ObProxyRpcType get_rpc_type() const { return rpc_type_; }
  ObRpcOBKVInfo &get_obkv_info() { return obkv_info_; }
  ObRpcRequestConfigInfo &get_rpc_request_config_info() { return config_info_; }
  ObConnectionAttributes &get_server_addr() { return server_add_; }
  ObRpcReqCtx *get_rpc_ctx() { return obkv_info_.rpc_ctx_; }
  void get_cur_server_addr(common::ObAddr &addr) {
    if (server_add_.addr_.is_valid()) {
      if (server_add_.addr_.is_ip4()) {
        addr.set_ipv4_addr(server_add_.get_ipv4(), server_add_.get_port());
      } else {
        //TODO need update ipv6
      }
    }
  }

  void retry_reset() {
    // Retrying will no longer reset the error code, but retain the error code.
    is_response_ = false;
    is_server_failed_ = false;
    is_server_addr_set_ = false;
    server_add_.reset();
    server_entry_send_retry_times_ = 0;

    obkv_info_.retry_reset();
  }

  bool is_valid() const { return !is_invalid(); }
  bool is_invalid() const { return obkv_info_.is_inner_request_ && (OB_ISNULL(root_rpc_req_) || root_rpc_req_->canceled()); }

  void  set_request_buf(char *req_buf) { request_buf_ = req_buf; }
  void  set_request_buf_len(int64_t req_buf_len) { request_buf_len_ = req_buf_len; }
  void  set_request_len(int64_t req_len) { request_len_ = req_len; }

  void  set_response_buf(char *res_buf) { response_buf_ = res_buf; }
  void  set_response_buf_len(int64_t res_buf_len) { response_buf_len_ = res_buf_len; }
  void  set_response_len(int64_t res_len) { response_len_ = res_len; }
  void  set_rpc_type(obkv::ObProxyRpcType type) { rpc_type_ = type; }
  void  set_response(bool is_response) { is_response_ = is_response; }
  void  set_origin_channel_id(uint32_t channel_id) { origin_channel_id_ = channel_id; }
  void  set_client_channel_id(uint32_t channel_id) { c_channel_id_ = channel_id; }
  void  set_server_channel_id(uint32_t channel_id) { s_channel_id_ = channel_id; }
  void  set_inner_request(bool flag) { obkv_info_.is_inner_request_ = flag; }
  void  set_use_request_inner_buf(bool flag) { is_use_request_inner_buf_ = flag; }
  void  set_use_response_inner_buf(bool flag) { is_use_response_inner_buf_ = flag; }
  void  set_rpc_req_error_code(int error_code) { obkv_info_.rpc_origin_error_code_ = error_code; }
  void  set_server_failed(bool is_failed) {
    is_server_failed_  = is_failed;
    if(is_server_failed_) {
      congest_status_ = SERVER_CONNECT_ERROR;
    } 
  }
  void  set_server_addr_set(bool flag) { is_server_addr_set_ = flag; }
  void  set_server_entry_send_retry_times (int64_t server_entry_send_retry_times) {
    server_entry_send_retry_times_ = server_entry_send_retry_times;
  }
  void  set_need_terminal_client_net(bool terminal) { is_need_terminal_client_net_ = terminal; }
  bool  is_need_terminal_client_net() { return is_need_terminal_client_net_; }

  uint32_t get_origin_channel_id() { return origin_channel_id_; }
  uint32_t get_client_channel_id() { return c_channel_id_; }
  uint32_t get_server_channel_id() { return s_channel_id_; }
  int64_t get_server_entry_send_retry_times() { return server_entry_send_retry_times_; }

  int alloc_request_buf(uint64_t len);
  int alloc_request_inner_buf(uint64_t len);
  int alloc_response_buf(uint64_t len);
  int alloc_response_inner_buf(uint64_t len);
  int free_request_buf();
  int free_request_inner_buf();
  int free_response_buf();
  int free_response_inner_buf();

  int alloc_inner_request_allocator();
  int free_inner_request_allocator();
  common::ObIAllocator *get_inner_request_allocator() { return inner_request_allocator_; }

  bool canceled() const { return is_canceled_; }
  bool is_inner_request() const { return obkv_info_.is_inner_request_; }
  bool could_release_request();
  bool could_cleanup_inner_request();
  void server_net_cancel_request();
  void client_net_cancel_request();
  void cancel_request();
  void inc_req_buf_repeat_times() { req_buf_repeat_times_++; }

  void set_cont_index(int64_t cont_index) { cont_index_ = cont_index; }
  void set_cluster_version(int64_t cluster_version) { cluster_version_ = cluster_version; }
  void set_cnet_state(ClientNetState state) { cnet_state_ = state; }
  void set_snet_state(ServerNetState state) { snet_state_ = state; }
  void set_sm_state(RpcReqSmState state) { sm_state_ = state; }
  void set_clean_module(RpcReqCleanModule module) { rpc_req_clean_module_ = module; }
  ObString &get_full_username() { return obkv_info_.full_username_; }
  bool has_error_resp() const { return obkv_info_.is_error(); }
  bool has_resultset_resp() const { return rpc_response_ != NULL; }

  ObRpcClientNetHandler *get_cnet_sm() { return reinterpret_cast<ObRpcClientNetHandler *>(cnet_sm_); }
  ObRpcServerNetHandler *get_snet_sm() { return reinterpret_cast<ObRpcServerNetHandler *>(snet_sm_); }
  ObRpcRequestSM *get_request_sm() { return reinterpret_cast<ObRpcRequestSM *>(sm_); }
  ClientNetState &get_cnet_state() { return cnet_state_; }
  ServerNetState &get_snet_state() { return snet_state_; }
  RpcReqSmState &get_sm_state() { return sm_state_; }
  RpcReqCleanModule &get_clean_module() { return rpc_req_clean_module_; }

  int64_t get_cont_index() {return cont_index_; }
  int64_t get_cluster_version() { return cluster_version_; }
  obkv::ObRpcRequest *get_rpc_request() { return rpc_request_; }
  obkv::ObRpcResponse *get_rpc_response() { return rpc_response_; }
  char* alloc_rpc_response(int64_t buf_size);
  int free_rpc_request();
  int free_rpc_response();
  void set_rpc_request(obkv::ObRpcRequest *rpc_request) { rpc_request_ = rpc_request; }
  void set_rpc_response(obkv::ObRpcResponse *rpc_response) { rpc_response_ = rpc_response; }
  void set_rpc_request_len(int64_t len) { rpc_request_len_ = len; }
  void set_rpc_response_len(int64_t len) { rpc_response_len_ = len; }
  int32_t get_error_code()  { return obkv_info_.get_error_code(); }

  const ObRpcReqTraceId &get_trace_id() { return obkv_info_.get_req_trace_id(); }

  void set_client_net_timeout_us(int64_t timeout_us) { client_net_timeout_us_ = timeout_us; }
  int64_t get_client_net_timeout_us() const { return client_net_timeout_us_; }
  void set_server_net_timeout_us(int64_t timeout_us) { server_net_timeout_us_ = timeout_us; }
  int64_t get_server_net_timeout_us() const { return server_net_timeout_us_; }

  void set_root_rpc_req(ObRpcReq *rpc_req) { root_rpc_req_ = rpc_req; }
  ObRpcReq *get_root_rpc_req() { return root_rpc_req_; }
  int alloc_sub_rpc_req_array(int64_t sub_rpc_req_size);
  int free_sub_rpc_req_array();

  int set_sub_rpc_req(const ObIArray<ObRpcReq *> &rpc_reqs);
  int64_t &get_current_sub_rpc_req_count() { return current_sub_rpc_req_count_; }
  int clean_sub_rpc_req();
  int64_t get_sub_rpc_req_array_size() const { return sub_rpc_req_array_size_; }
  int sub_rpc_req_init(ObRpcReq *root_rpc_req, ObRpcRequestSM *sm, obkv::ObRpcRequest *rpc_request, int64_t rpc_request_len,
                               int64_t cont_index, int64_t partition_id, int64_t ls_id  = 0);
  bool is_sub_req_inited() const { return is_sub_req_inited_; }
  void set_sub_req_inited(const bool is_sub_req_inited) { is_sub_req_inited_ = is_sub_req_inited; }

  DECLARE_TO_STRING;

public:
  ObRpcReqMagic magic_;

  event::ObContinuation *sm_;  //指向请求的处理状态逻辑（ObRpcRequestSM *）
  event::ObContinuation *cnet_sm_;  //
  event::ObContinuation *snet_sm_;  //
  void *execute_plan_; //used by shading or inner task
  ObRpcClientMilestones client_timestamp_;
  ObRpcServerMilestones server_timestamp_;
  int64_t server_entry_send_retry_times_;
  int64_t cs_id_;
  int64_t ss_id_;
  RpcReqCongestStatus congest_status_;

  static const int64_t MAX_SCATTER_LEN;

private:
  char *request_buf_;                   //byte data buffer received or to be send
  char *request_inner_buf_;             //byte data buffer which need re-serialize(only used in)
  char *response_buf_;                  //byte data buffer which is for response
  char *response_inner_buf_;            //byte data buffer which is for need re-serialize(only used in)

  int64_t request_buf_len_;             //buffer length for request_buf_
  int64_t request_inner_buf_len_;       //buffer length for request_inner_buf_
  int64_t response_buf_len_;            //buffer lenght for response_buf_;
  int64_t response_inner_buf_len_;      //buffer lenght for response_buf_;
  int64_t req_buf_repeat_times_;        //req_buf used times, need to release more than MAX_REPEATE_TIMES
  int64_t request_len_;                 //request bytes' length
  int64_t response_len_;                //response bytes' length

  uint32_t origin_channel_id_;   //origin channel id for request
  uint32_t c_channel_id_;        //client channel id which init by client_net_handler(Key)
  uint32_t s_channel_id_;        //channel id which send to observer
  int64_t retry_times_;          //retry times for the request
  int64_t inner_req_retry_times_;
  int64_t cont_index_;
  int64_t cluster_version_;

  bool is_need_terminal_client_net_;
  bool is_response_;           //是否结果
  bool is_canceled_;
  bool is_finish_;
  bool is_server_addr_set_;
  bool is_use_request_inner_buf_;
  bool is_use_response_inner_buf_;
  bool is_server_failed_;
  obkv::ObProxyRpcType rpc_type_;
  ObConnectionAttributes server_add_;
  event::ObEThread *created_thread_;
  ClientNetState cnet_state_;
  ServerNetState snet_state_;
  RpcReqSmState sm_state_;
  RpcReqCleanModule rpc_req_clean_module_;

  common::ObIAllocator *inner_request_allocator_;  // for inner request, alloc res_buf and response
  int64_t inner_request_allocator_len_;
  obkv::ObRpcRequest *rpc_request_;  //初始化具体的请求对象，内存使用ObRpcReq中 request_buf_;
  obkv::ObRpcResponse *rpc_response_;
  int64_t rpc_request_len_;
  int64_t rpc_response_len_;
  int64_t client_net_timeout_us_;
  int64_t server_net_timeout_us_;

  ObRpcReq *root_rpc_req_;         // 子任务需要维护root rpc req指针
  ObRpcReq **sub_rpc_req_array_;         // 父请求维护所有子请求
  int64_t sub_rpc_req_array_size_;
  int64_t current_sub_rpc_req_count_;

  ObRpcOBKVInfo obkv_info_;
  ObRpcRequestConfigInfo config_info_;
  bool is_sub_req_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObRpcReq);

  friend class ObRpcRequestSM;
};

typedef common::ObList<ObRpcReq*> ObRpcReqList;

inline ObRpcReq* ObRpcReq::allocate()
{
  ObRpcReq *rpc_req = op_thread_alloc_init(ObRpcReq, event::get_rpc_req_allocator(), ObRpcReq::instantiate_func);
  PROXY_LOG(DEBUG, "ObRpcReq::allocate", KP(rpc_req));
  return rpc_req;
}

inline int ObRpcReq::alloc_sub_rpc_req_array(int64_t sub_rpc_req_array_size)
{
  int ret = common::OB_SUCCESS;
  int64_t alloc_size = sizeof(ObRpcReq *) * sub_rpc_req_array_size;
  void *buf = NULL;

  if (OB_NOT_NULL(sub_rpc_req_array_)) {
    if (OB_FAIL(free_sub_rpc_req_array())) {
      PROXY_LOG(EDIAG, "fail to call free_sub_rpc_req_array", K(ret));
    }
  }

  if (OB_ISNULL(buf = op_fixed_mem_alloc(alloc_size))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(EDIAG, "fail to alloc mem", K(alloc_size), K(ret));
  } else {
    sub_rpc_req_array_ = reinterpret_cast<ObRpcReq **>(buf);
    sub_rpc_req_array_size_ = sub_rpc_req_array_size;
  }

  return ret;
}

inline int ObRpcReq::free_sub_rpc_req_array()
{
  int ret = common::OB_SUCCESS;

  if (OB_NOT_NULL(sub_rpc_req_array_)) {
    if (sub_rpc_req_array_size_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(EDIAG, "sub_rpc_req_array_size_ must > 0", K_(sub_rpc_req_array_size), K_(sub_rpc_req_array), K(ret));
    } else {
      int64_t free_size = sizeof(ObRpcReq *) * sub_rpc_req_array_size_;
      op_fixed_mem_free(sub_rpc_req_array_, free_size);
      sub_rpc_req_array_ = NULL;
      sub_rpc_req_array_size_ = 0;
    }
  }

  return ret;
}

inline int ObRpcReq::set_sub_rpc_req(const ObIArray<ObRpcReq *> &rpc_reqs)
{
  int ret = common::OB_SUCCESS;

  if (OB_ISNULL(sub_rpc_req_array_) || sub_rpc_req_array_size_ != rpc_reqs.count()) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_LOG(EDIAG, "sub_rpc_req_array_size_ must equal to rpc_reqs.count", K_(current_sub_rpc_req_count), K_(sub_rpc_req_array_size), K(rpc_reqs.count()), K(ret));
  } else {
    current_sub_rpc_req_count_ = sub_rpc_req_array_size_;
    for (int64_t i = 0; i < sub_rpc_req_array_size_; ++i) {
      sub_rpc_req_array_[i] = rpc_reqs.at(i);
    }
    PROXY_LOG(DEBUG, "set_sub_rpc_req success", K_(current_sub_rpc_req_count), K_(sub_rpc_req_array_size), K(this));
  }

  return ret;
}

inline int ObRpcReq::alloc_inner_request_allocator()
{
  int ret = common::OB_SUCCESS;
  int64_t alloc_size = sizeof(common::ObArenaAllocator);
  void *buf = NULL;

  if (OB_NOT_NULL(inner_request_allocator_)) {
    if (OB_FAIL(free_inner_request_allocator())) {
      PROXY_LOG(EDIAG, "free inner request allocator error", K(ret));
    }
  }

  if (OB_ISNULL(buf = op_fixed_mem_alloc(alloc_size))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(EDIAG, "fail to alloc mem", K(alloc_size), K(ret));
  } else {
    inner_request_allocator_ = new (buf) common::ObArenaAllocator;
    inner_request_allocator_len_ = alloc_size;
  }

  return ret;
}

inline int ObRpcReq::free_inner_request_allocator()
{
  int ret = common::OB_SUCCESS;

  if (OB_NOT_NULL(inner_request_allocator_)) {
    if (inner_request_allocator_len_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(EDIAG, "inner_request_allocator_len_ must > 0", K_(inner_request_allocator_len), K_(inner_request_allocator), K(ret));
    } else {
      inner_request_allocator_->~ObIAllocator();  // free all memory
      op_fixed_mem_free(inner_request_allocator_, inner_request_allocator_len_);
      inner_request_allocator_ = NULL;
      inner_request_allocator_len_ = 0;
    }
  }

  return ret;
}

inline char* ObRpcReq::alloc_rpc_response(int64_t buf_size)
{
  char* buf = NULL;
  ObRpcReqTraceId &rpc_trace_id = obkv_info_.rpc_trace_id_;
  if (obkv_info_.is_inner_request_ && OB_ISNULL(inner_request_allocator_)) {
    PROXY_LOG(WDIAG, "inner request allocator is NULL", K(rpc_trace_id));
  } else if (obkv_info_.is_inner_request_ && OB_NOT_NULL(inner_request_allocator_)) {
    buf = (char*)inner_request_allocator_->alloc(buf_size);
  } else {
    buf = (char*)op_fixed_mem_alloc(buf_size);
  }
  return buf;
}

inline int ObRpcReq::free_rpc_request()
{
  int ret = common::OB_SUCCESS;

  if (OB_NOT_NULL(rpc_request_)) {
    if (rpc_request_len_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(EDIAG, "rpc_request_len_ must > 0", K_(rpc_request_len), K_(rpc_request), K(ret));
    } else {
      rpc_request_->~ObRpcRequest();  // free all memory
      op_fixed_mem_free(rpc_request_, rpc_request_len_);
      rpc_request_ = NULL;
      rpc_request_len_ = 0;
    }
  }

  return ret;
}

inline int ObRpcReq::free_rpc_response()
{
  int ret = common::OB_SUCCESS;

  if (OB_NOT_NULL(rpc_response_)) {
    if (rpc_response_len_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(EDIAG, "rpc_response_len_ must > 0", K_(rpc_response_len), K_(rpc_response), K(ret));
    } else {
      rpc_response_->~ObRpcResponse();
      if (obkv_info_.is_inner_request_ && OB_NOT_NULL(inner_request_allocator_)) {
        // do nothing, just reset inner_request_allocator_
      } else {
        op_fixed_mem_free(rpc_response_, rpc_response_len_);
      }
      rpc_response_ = NULL;
      rpc_response_len_ = 0;
    }
  }

  return ret;
}

inline int ObRpcReq::alloc_request_buf(uint64_t len)
{
  int ret = common::OB_SUCCESS;
  // free buf if has alloc
  if (OB_UNLIKELY(NULL != request_buf_)) {
    if (OB_FAIL(free_request_buf())) {
      PROXY_LOG(EDIAG, "free request buf error", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(len));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(EDIAG, "fail to alloc mem", K(len), K(ret));
    } else {
      request_buf_ = buf;
      request_buf_len_ = len;
    }
  }
  return ret;
}

inline int ObRpcReq::free_request_buf()
{
  int ret = common::OB_SUCCESS;
  if (NULL != request_buf_) {
    if (request_buf_len_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(EDIAG, "request_buf_len_ must > 0", K_(request_buf_len), K_(request_buf), K(ret));
    } else {
      op_fixed_mem_free(request_buf_, request_buf_len_);
      request_buf_ = NULL;
      request_buf_len_ = 0;
    }
  }
  return ret;
}

inline int ObRpcReq::alloc_request_inner_buf(uint64_t len)
{
  int ret = common::OB_SUCCESS;
  // free buf if has alloc
  if (OB_UNLIKELY(NULL != request_inner_buf_)) {
    if (OB_FAIL(free_request_inner_buf())) {
      PROXY_LOG(EDIAG, "free request inner buf error", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(len));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(EDIAG, "fail to alloc mem", K(len), K(ret));
    } else {
      request_inner_buf_ = buf;
      request_inner_buf_len_ = len;
    }
  }
  return ret;
}

inline int ObRpcReq::alloc_response_inner_buf(uint64_t len)
{
  int ret = common::OB_SUCCESS;
  // free buf if has alloc
  if (OB_UNLIKELY(NULL != response_inner_buf_)) {
    if (OB_FAIL(free_response_inner_buf())) {
      PROXY_LOG(EDIAG, "free response inner buf error", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(len));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(EDIAG, "fail to alloc mem", K(len), K(ret));
    } else {
      response_inner_buf_ = buf;
      response_inner_buf_len_ = len;
    }
  }
  return ret;
}

inline int ObRpcReq::free_request_inner_buf()
{
  int ret = common::OB_SUCCESS;
  if (NULL != request_inner_buf_) {
    if (request_inner_buf_len_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(EDIAG, "request_buf_len_ must > 0", K_(request_inner_buf_len), K_(request_inner_buf), K(ret));
    } else {
      op_fixed_mem_free(request_inner_buf_, request_inner_buf_len_);
      request_inner_buf_ = NULL;
      request_inner_buf_len_ = 0;
    }
  }
  return ret;
}

inline int ObRpcReq::free_response_inner_buf()
{
  int ret = common::OB_SUCCESS;
  if (NULL != response_inner_buf_) {
    if (response_inner_buf_len_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(EDIAG, "response_inner_buf_len_ must > 0", K_(response_inner_buf_len), K_(response_inner_buf), K(ret));
    } else {
      op_fixed_mem_free(response_inner_buf_, response_inner_buf_len_);
      response_inner_buf_ = NULL;
      response_inner_buf_len_ = 0;
    }
  }
  return ret;
}

inline int ObRpcReq::alloc_response_buf(uint64_t len)
{
  int ret = common::OB_SUCCESS;
  // free buf if has alloc
  if (OB_UNLIKELY(NULL != response_buf_)) {
    if (OB_FAIL(free_response_buf())) {
      PROXY_LOG(EDIAG, "free response buf error", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    char *buf = NULL;

    if (obkv_info_.is_inner_request_) {
      if (OB_ISNULL(inner_request_allocator_)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_LOG(WDIAG, "inner request but inner_request_allocator is NULL", K(ret), K(len));
      } else if (OB_ISNULL(buf = reinterpret_cast<char *>(inner_request_allocator_->alloc(len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_LOG(WDIAG, "analyze_obrpc_req alloc memory failed by inner_request_allocator", K(ret), K(len));
      }
    } else {
      buf = reinterpret_cast<char *>(op_fixed_mem_alloc(len));
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(NULL == buf)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        PROXY_LOG(EDIAG, "fail to alloc mem", K(len), K(ret));
      } else {
        response_buf_ = buf;
        response_buf_len_ = len;
      }
    }
  }
  return ret;
}

inline int ObRpcReq::free_response_buf()
{
  int ret = common::OB_SUCCESS;
  if (NULL != response_buf_) {
    if (response_buf_len_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(EDIAG, "response_buf_len_ must > 0", K_(response_buf_len), K_(response_buf), K(ret));
    } else {
      if (obkv_info_.is_inner_request_ && OB_NOT_NULL(inner_request_allocator_)) {
        // do nothing, just reset inner_request_allocator_
      } else {
        op_fixed_mem_free(response_buf_, response_buf_len_);
      }
      response_buf_ = NULL;
      response_buf_len_ = 0;
    }
  }
  return ret;
}

inline void ObRpcReq::server_net_cancel_request()
{
  if (snet_state_ < RPC_REQ_SERVER_CANCLED) {
    // If it is a sub-request, the state will not be changed to prevent it from being cleaned up in advance. The subsequent cleanup will lock the state before changing the state.
    if (!is_inner_request()) {
      snet_state_ = RPC_REQ_SERVER_CANCLED;
    }
  }
  snet_sm_ = NULL; //not to schedule client_net_handler any more
  cancel_request();
}

void ObRpcReq::client_net_cancel_request()
{
  if (cnet_state_ < RPC_REQ_CLIENT_CANCLED) {
    // If it is a sub-request, the state will not be changed to prevent it from being cleaned up in advance. The subsequent cleanup will lock the state before changing the state.
    if (!is_inner_request()) {
      cnet_state_ = RPC_REQ_CLIENT_CANCLED;
    }
  }
  cnet_sm_ = NULL; //not to schedule client_net_handler any more

  if (0 != sub_rpc_req_array_size_) {
    PROXY_LOG(DEBUG, "ObRpcReq::client_net_cancel_request cancel all sub_rpc_req", K_(sub_rpc_req_array_size));
    for (int i = 0; i < sub_rpc_req_array_size_; ++i) {
      // only clean_sub_rpc_req set cnet_state of sub request
      sub_rpc_req_array_[i]->cancel_request();
    }
  }

  cancel_request();
}

inline void ObRpcReq::cancel_request()
{
  is_canceled_ = true;
}

inline bool ObRpcReq::could_cleanup_inner_request()
{
  bool ret = !((cnet_state_ > ObRpcReq::ClientNetState::RPC_REQ_CLIENT_INIT && cnet_state_ < ObRpcReq::ClientNetState::RPC_REQ_CLIENT_DONE) /* client net may need check request */
               || (snet_state_ > ObRpcReq::ServerNetState::RPC_REQ_SERVER_INIT && snet_state_ < ObRpcReq::ServerNetState::RPC_REQ_SERVER_DONE) /* server net may need check request */
               || (sm_state_ > ObRpcReq::RpcReqSmState::RPC_REQ_SM_INIT && sm_state_ < ObRpcReq::RpcReqSmState::RPC_REQ_SM_INNER_REQUEST_CLEANUP)); /* rpc_request_sm may need check request */
  return ret;
}

inline bool ObRpcReq::could_release_request()
{
  bool ret = !((cnet_state_ > RPC_REQ_CLIENT_INIT && cnet_state_ < RPC_REQ_CLIENT_DONE)  /* client net may need check request */
               || (snet_state_ > RPC_REQ_SERVER_INIT && snet_state_ < RPC_REQ_SERVER_DONE) /* server net may need check request */
               || (sm_state_ > RPC_REQ_SM_INIT && sm_state_ < RPC_REQ_SM_REQUEST_DONE)); /* rpc_request_sm may need check request */
  return ret;
}

// 1. not error
// 2. shard request
// 3. async query request
// 3. login request
inline bool ObRpcOBKVInfo::need_parse_response_fully() const
{
  return !is_error() 
    && (is_inner_request_ || pcode_ == obrpc::ObRpcPacketCode::OB_TABLE_API_EXECUTE_QUERY_SYNC
                          || pcode_ == obrpc::ObRpcPacketCode::OB_TABLE_API_LOGIN);
}

inline common::ObString get_rpc_type_string(const obkv::ObProxyRpcType type)
{
  const char *str = "";
  switch (type)
  {
  case obkv::OBPROXY_RPC_OBRPC:
    str = "OB_RPC";
    break;
  case obkv::OBPROXY_RPC_HBASE:
    str = "OB_HBASE";
    break;
  default:
    str = "UNKNOWN";
    break;
  }
  return common::ObString::make_string(str);
}
}
}
}
#endif
