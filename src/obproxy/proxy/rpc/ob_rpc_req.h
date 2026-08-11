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
#include "proxy/rpc/ob_rpc_req_trace.h"
#include "proxy/rpc/rpclib/ob_rpc_time_stat.h"
#include "proxy/rpc/rpclib/ob_rpc_throttle.h"
#include "proxy/route/ob_ldc_struct.h"
#include "proxy/route/ob_ldc_location.h"
#include "proxy/mysql/ob_mysql_sm_time_stat.h"
#include "utils/ob_proxy_lib.h"
#include "lib/list/ob_list.h"
#include "obkv/table/ob_table.h"
#include "stat/ob_rpc_req_stats.h"
#include "proxy/rpc/ob_shared_request_buf.h"

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
namespace engine
{
class ObProxyRpcReqSplitCont;
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
class ObTabletLsEntry;
class ObRpcRedisInfo;

typedef common::ObSEArray<common::ObString, COMMON_REDIS_ARGS_COUNT> ARR_ARGS;

const int64_t MAX_VERSION_LEN = 64;

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

  uint16_t get_obproxy_port() {return net::ops_ip_port_host_order(obproxy_addr_);}

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
  ObRpcRequestConfigInfo () : config_version_(0), rpc_proxy_route_policy_(),
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
    if (OB_UNLIKELY(config_info.rpc_proxy_route_policy_.is_use_dynamic_buf())) {
      MEMSET(&rpc_proxy_route_policy_, 0, sizeof(ObConfigVarStr));
      rpc_proxy_route_policy_.deep_copy(config_info.rpc_proxy_route_policy_);
    }
  }

  bool is_init() const { return 0 != config_version_; }

  TO_STRING_KV(K_(config_version), K_(rpc_proxy_route_policy), K_(enable_cloud_full_username), K_(rpc_support_key_partition_shard_request),
               K_(rpc_enable_force_srv_black_list), K_(rpc_enable_direct_expire_route_entry),
               K_(rpc_enable_reroute), K_(rpc_enable_congestion), K_(rpc_enable_global_index), K_(rpc_enable_retry_request_info_log),
               K_(rpc_request_max_retries), K_(rpc_request_timeout), K_(rpc_request_timeout_delta), K_(rpc_request_retry_waiting_time),
               K_(rpc_redis_default_database_name), K_(rpc_redis_default_user_name));

public:
  uint64_t config_version_;
  //for common config
  ObConfigVarStr rpc_proxy_route_policy_;
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
  char rpc_redis_default_database_name_[OB_MAX_DATABASE_NAME_LENGTH];
  char rpc_redis_default_user_name_[OB_PROXY_FULL_USER_NAME_MAX_LEN];
};


struct ObRpcRetryInfo
{
public:
  ObRpcRetryInfo() : is_server_failed_(false), is_shard_request_retry_(false), is_in_congestion_retry_(false),
                     is_rpc_ls_entry_need_retry_(false), is_need_retry_with_global_index_(false),
                     is_in_reroute_retry_(false),
                     has_get_newest_table_entry_in_retry_(false), is_sub_rpc_request_retry_reach_limit_(false),
                     rpc_origin_error_code_(0), retry_status_() {}
  ~ObRpcRetryInfo() {}

  void set_server_failed(bool flag) { is_server_failed_ = flag; }
  void set_need_retry_with_global_index(bool flag) { is_need_retry_with_global_index_ = flag; }
  void set_in_congestion_retry(bool flag) { is_in_congestion_retry_ = flag; }


  bool is_need_retry_with_global_index() const { return  is_need_retry_with_global_index_; }
  bool is_in_congestion_retry() const { return  is_in_congestion_retry_; }
  bool is_server_failed() const { return  is_server_failed_; }

  bool is_need_retry() const { return is_rpc_ls_entry_need_retry_ || is_need_retry_with_global_index_; }
  bool need_retry_wait_for_newest_table_entry() const {
    bool bret = true;
    uint32_t retry_limit = obutils::get_global_proxy_config().rpc_sub_req_max_retries;
    if (retry_status_.rpc_request_route_calc_retry_times_ < retry_limit) {
      bret = false;
    }
    return bret;
  }

  void reset() {
    retry_reset();
    retry_status_.reset();
    is_shard_request_retry_ = false;
    has_get_newest_table_entry_in_retry_ = false;
    rpc_origin_error_code_ = 0;
  }

  void retry_reset() {
    is_server_failed_ = false;
    is_rpc_ls_entry_need_retry_ = false;
    is_in_congestion_retry_ = false;
    is_need_retry_with_global_index_ = false;
    is_in_reroute_retry_ = false;
  }

  TO_STRING_KV(K_(is_server_failed), K_(is_rpc_ls_entry_need_retry), K_(is_in_congestion_retry), K_(is_need_retry_with_global_index),
               K_(has_get_newest_table_entry_in_retry), K_(rpc_origin_error_code), K_(retry_status));

public:
  struct RetryStatus {
    uint32_t inner_req_retries_;
    int64_t rpc_request_retry_last_begin_;
    int64_t rpc_request_retry_times_;
    int64_t rpc_request_reroute_moved_times_;
    int64_t rpc_request_route_calc_retry_times_;
    int64_t rpc_request_route_calc_fail_retry_times_;

    RetryStatus() : inner_req_retries_(0), rpc_request_retry_last_begin_(0), rpc_request_retry_times_(0), rpc_request_reroute_moved_times_(0), rpc_request_route_calc_retry_times_(0), rpc_request_route_calc_fail_retry_times_(0) {}

    void reset() {
      inner_req_retries_ = 0;
      rpc_request_retry_last_begin_ = 0;
      rpc_request_retry_times_ = 0;
      rpc_request_reroute_moved_times_ = 0;
      rpc_request_route_calc_retry_times_ = 0;
      rpc_request_route_calc_fail_retry_times_ = 0;
    }

    TO_STRING_KV(K_(inner_req_retries), K_(rpc_request_retry_last_begin), K_(rpc_request_retry_times), K_(rpc_request_reroute_moved_times),
                 K_(rpc_request_route_calc_retry_times), K_(rpc_request_route_calc_fail_retry_times));
  };
  bool is_server_failed_;
  bool is_shard_request_retry_;
  bool is_in_congestion_retry_;
  bool is_rpc_ls_entry_need_retry_;
  bool is_need_retry_with_global_index_;
  bool is_in_reroute_retry_;
  bool has_get_newest_table_entry_in_retry_;
  bool is_sub_rpc_request_retry_reach_limit_;
  int32_t rpc_origin_error_code_;
  RetryStatus retry_status_;
};

class ObRpcOBKVInfo
{
  static const int SCHEMA_LENGTH = 100;
public:
  enum OBKVInfoFlags {
    META_FLAG                    = 35,
    NON_PARTITION_TABLE_FLAG     = 34,
    HBASE_FLAG                   = 33,
    EMPTY_QUERY_RESULT_FLAG      = 32,
    DIRECT_LOAD_FLAG             = 31,
    GLOBAL_INDEX_ROUTE_FLAG      = 30,
    QUERY_WITH_INDEX             = 29,
    NEED_GLOBAL_INDEX_RETRY_FLAG = 28,         // will not use this flag, maybe will remove it in the future
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
    BACKGROUND_FLOW_FLAG         = 7,
    // IS_KV_REQUEST_FLAG = KV_ROUTE_META_ERROR_FLAG, odp do not concern IS_KV_REQUEST_FLAG
    KV_ROUTE_META_ERROR_FLAG     = 5
  };
public:
  ObRpcOBKVInfo() :request_id_(0), server_request_id_(0), is_first_direct_load_request_(false), is_inner_request_(false),
                   is_internal_rpc_request_(false), is_internal_rpc_request_has_done_(false), is_rpc_request_with_partition_id_(false),
                   is_table_group_request_(false), is_server_support_distributed_execute_(false), is_single_partition_table_(false),
                   cluster_name_(), tenant_name_(), user_name_(), table_name_(), database_name_(), full_username_(), tablegroup_new_table_name_(),
                   cluster_id_(0), tenant_id_(0), table_id_(0), partition_id_(common::OB_INVALID_INDEX), ls_id_(common::ObLSID::INVALID_LS_ID), client_info_(),
                   server_info_(), route_policy_(1), cs_read_consistency_(0), is_proxy_route_policy_set_(false),
                   is_read_consistency_set_(false), proxy_route_policy_(MAX_PROXY_ROUTE_POLICY), pcode_(obrpc::OB_INVALID_RPC_CODE),
                   flags_(0),
                   query_async_entry_(NULL), rpc_ctx_(NULL), data_table_id_(OB_INVALID_ID),
                   index_name_(), index_entry_(NULL), index_table_name_(), need_add_index_entry_into_cache_(false), tablegroup_entry_(NULL),
                   tablet_ls_entry_(NULL), dummy_ldc_(), dummy_entry_(NULL), retry_info_(),
                   is_set_rpc_trace_id_(false), rpc_trace_id_(), credential_(), is_rpc_req_stat_recorded_(false)
                   { index_table_name_buf_[0] = '\0'; }
  ~ObRpcOBKVInfo() {}

  void reset();
  void set_rpc_trace_id(const uint64_t id, const uint64_t ipport);
  void set_rpc_trace_sub_index(const uint64_t sub_index) { rpc_trace_id_.set_rpc_sub_index_id(sub_index, retry_info_.retry_status_.inner_req_retries_); }

  bool need_parse_response_fully() const;

  void retry_reset(bool clean_flag) {
    set_error_resp(false);
    set_resp_completed(false);
    set_resp_reroute_info(false);
    set_need_retry(false);
    set_definitely_single(false);
    set_non_partition_table(false);
    retry_info_.retry_reset();

    if (clean_flag) {
      partition_id_ = OB_INVALID_INDEX;
      table_id_ = 0;
      ls_id_ = common::ObLSID::INVALID_LS_ID;
    } //do nothing
  }

  // common::ObString get_req_trace_id(); //打印trace id使用
  const ObRpcReqTraceId &get_req_trace_id() const { return rpc_trace_id_; }
  ObRpcReqTraceId &get_req_trace_id() { return rpc_trace_id_; }
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
  void set_need_retry_with_global_index(bool flag) { retry_info_.set_need_retry_with_global_index(flag); }
  void set_direct_load_req(const bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::DIRECT_LOAD_FLAG), flag);}
  void set_empty_query_result(const bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::EMPTY_QUERY_RESULT_FLAG), flag);}
  void set_non_partition_table(const bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::NON_PARTITION_TABLE_FLAG), flag);}
  void set_meta(const bool flag) { set_flag(static_cast<int>(OBKVInfoFlags::META_FLAG), flag);}

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
  void set_table_group_request(bool flag) { is_table_group_request_ = flag; }
  void set_pcode(obrpc::ObRpcPacketCode pcode) { pcode_ = pcode; }
  void set_rpc_ls_entry_need_retry(bool flag) { retry_info_.is_rpc_ls_entry_need_retry_ = flag; }

  /* flag for request */
  bool is_auth() const { return get_flag(static_cast<int>(OBKVInfoFlags::AUTH_FLAG)); }
  bool is_meta() const { return get_flag(static_cast<int>(OBKVInfoFlags::META_FLAG)); }
  bool is_definitely_single() const { return get_flag(static_cast<int>(OBKVInfoFlags::SINGLE_FLAG)); }
  bool is_batch() const { return get_flag(static_cast<int>(OBKVInfoFlags::BATCH_FALG)); }
  bool is_shard() const { return get_flag(static_cast<int>(OBKVInfoFlags::SHARD_FLAG)); }
  bool is_stream() const { return get_flag(static_cast<int>(OBKVInfoFlags::STREAM_FLAG));}
  bool is_stream_query() const { return get_flag(static_cast<int>(OBKVInfoFlags::STREAM_QUERY_FLAG)); }
  bool is_internal_req() const { return get_flag(static_cast<int>(OBKVInfoFlags::INTERNAL_FLAG)); }
  bool is_read_weak() const { return get_flag(static_cast<int>(OBKVInfoFlags::READ_WEAK_FLAG)); }
  bool is_need_retry() const { return get_flag(static_cast<int>(OBKVInfoFlags::NEED_RETRY_FLAG)); }
  bool is_need_retry_with_global_index() const { return retry_info_.is_need_retry_with_global_index(); }
  bool is_direct_load_req() const { return get_flag(static_cast<int>(OBKVInfoFlags::DIRECT_LOAD_FLAG)); }
  bool is_empty_query_result() const { return get_flag(static_cast<int>(OBKVInfoFlags::EMPTY_QUERY_RESULT_FLAG)); }
  bool is_hbase_request() const { return get_flag(static_cast<int>(OBKVInfoFlags::HBASE_FLAG)); }
  bool is_non_partition_table() const { return get_flag(static_cast<int>(OBKVInfoFlags::NON_PARTITION_TABLE_FLAG)); }
  bool is_first_direct_load_request() const { return is_first_direct_load_request_; }
  bool is_table_group_request() const { return is_table_group_request_; }

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
  bool is_need_refresh_table_entry() const { return get_flag(static_cast<int>(OBKVInfoFlags::KV_ROUTE_META_ERROR_FLAG)); }
  bool is_async_query_request() const { return obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC == pcode_; }
  bool is_lsop_request() const { return obrpc::OB_TABLE_API_LS_EXECUTE == pcode_; }
  bool is_query_request() const { return obrpc::OB_TABLE_API_EXECUTE_QUERY == pcode_ || obrpc::OB_TABLE_API_QUERY_AND_MUTATE == pcode_ || obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC == pcode_; }
  bool is_inner_req_retrying() const { return is_inner_request_ && retry_info_.retry_status_.inner_req_retries_ > 0; }
  bool is_internal_get_partition_request() const { return obrpc::OB_GET_PARTITIONS == pcode_; }
  // only lsop need get ls_id
  // 1.lsop请求且是分区表
  // 2.不是子请求或者子请求重试中
  // 3.只有hbase请求且开启分布式能力才不需要获取ls_id
  bool is_need_ls_id() const { return is_lsop_request() && !is_non_partition_table() && (!is_inner_request_|| is_inner_req_retrying()) && !(is_hbase_request() && is_server_support_distributed_execute_); } //TODO need add other condition for next
  bool is_rpc_ls_entry_need_retry() const { return retry_info_.is_rpc_ls_entry_need_retry_; }

  // will not be used any more
  bool     is_not_master_error() const {  //used by sub request to retry(just for route error);
    return get_error_code() == OB_NOT_MASTER
            || get_error_code() == OB_TABLET_NOT_EXIST
            || get_error_code() == OB_LS_NOT_EXIST
            || get_error_code() == OB_PARTITION_NOT_EXIST
            || get_error_code() == OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST
            ;
  }

  bool is_need_refresh_table_entry_error() const {
    return get_error_code() == OB_SCHEMA_ERROR
            || get_error_code() == OB_TABLE_NOT_EXIST
            || get_error_code() == OB_TABLET_NOT_EXIST
            || get_error_code() == OB_LS_NOT_EXIST
            || get_error_code() == OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST;
  }

  int32_t get_error_code() const { return retry_info_.rpc_origin_error_code_; }
  obrpc::ObRpcPacketCode get_pcode() const { return pcode_; }
  inline bool is_request_has_retried() const { return retry_info_.retry_status_.rpc_request_retry_times_ > 0 || retry_info_.retry_status_.rpc_request_reroute_moved_times_ > 0; }

  void set_meta_flag(uint16_t meta_flag) { flags_ = ((flags_ & (~0xFFFFULL)) | static_cast<uint64_t>(meta_flag)); }
  //TODO add one response flag need clear it in reset_odp_resp_flag(), just set for response flag inited by response from observer:
  //  1.not to update flag which read from observer meta (<16)
  //  2.not to update flag which used by request
  void reset_odp_resp_flag() { set_error_resp(false); set_resp_completed(false); set_resp_reroute_info(false); set_need_retry(false); }
  // const common::ObString &get_server_trace_id() { return server_trace_id_; }

  int generate_index_table_name();
  int init_and_set_tablegroup_table_new_name(const ObString &table_name);
  void free_table_group_table_new_name();
  void set_route_entry_dirty();
  void set_tablet_ls_entry_dirty();

  bool is_rpc_req_can_retry() const
  {
    bool bret = false;
    bool route_error_retry = is_need_retry() || is_need_retry_with_query_async();
    uint32_t sub_req_retry_limit = obutils::get_global_proxy_config().rpc_sub_req_max_retries;
    if (!is_inner_request_) {
      bret = (route_error_retry || retry_info_.is_need_retry());
    } else {
      bret = (retry_info_.retry_status_.inner_req_retries_ < sub_req_retry_limit
          && (route_error_retry || retry_info_.is_rpc_ls_entry_need_retry_)
          && (pcode_ == obrpc::OB_TABLE_API_LS_EXECUTE
            || ((!is_need_refresh_table_entry_error() || 0 == get_error_code()))));
    }
    return bret;
  }

  int set_dummy_entry(ObTableEntry *dummy_entry);

  TO_STRING_KV(K_(pcode), K_(request_id), K_(cluster_name), K_(tenant_name), K_(user_name), K_(table_name),
               K_(database_name), K_(cluster_id), K_(tenant_id), K_(table_id), K_(partition_id), K_(flags),
               K_(retry_info), KP_(query_async_entry), KP_(rpc_ctx), K_(index_table_name),
               KP_(index_entry), KP_(dummy_entry), K_(rpc_trace_id));

public:
  uint32_t request_id_;
  uint32_t server_request_id_;          //init by pkt header, trace request

  bool is_first_direct_load_request_; //direct load request need
  bool is_inner_request_;             // 内部拆分的子请求
  bool is_internal_rpc_request_;       // obproxy收到的rpc request，该flag表示obproxy内部执行完返回
  bool is_internal_rpc_request_has_done_; //
  bool is_rpc_request_with_partition_id_;
  bool is_table_group_request_;         //only used for hbase column family group
  bool is_server_support_distributed_execute_;
  bool is_single_partition_table_;

  common::ObString cluster_name_;
  common::ObString tenant_name_;
  common::ObString user_name_;
  common::ObString table_name_;
  common::ObString database_name_;
  common::ObString full_username_;
  common::ObString tablegroup_new_table_name_;

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

  ObTableQueryAsyncEntry *query_async_entry_;
  ObRpcReqCtx *rpc_ctx_;

  // used by global index route
  uint64_t data_table_id_;
  ObString index_name_;
  ObIndexEntry *index_entry_;
  ObString index_table_name_;
  char index_table_name_buf_[OB_MAX_INDEX_TABLE_NAME_LENGTH];
  bool need_add_index_entry_into_cache_;

  // used by hbase tablegroup
  ObTableGroupEntry *tablegroup_entry_;
  char tablegroup_new_table_name_buf_[OB_MAX_TABLE_NAME_LENGTH];
  //used by LSOP which less than observer-4.3.5.bp2
  ObTabletLsEntry *tablet_ls_entry_;

  // dummy_entry and dummy ldc
  ObLDCLocation dummy_ldc_;
  ObTableEntry *dummy_entry_;

  // used by retry
  ObRpcRetryInfo retry_info_;
  bool is_set_rpc_trace_id_;
  ObRpcReqTraceId rpc_trace_id_;

  obkv::ObTableApiCredential credential_;
  bool is_rpc_req_stat_recorded_;
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
    RPC_REQ_CLIENT_INNER_REQUEST,
    RPC_REQ_CLIENT_REQUEST_HANDLING,
    RPC_REQ_CLIENT_RESPONSE_HANDLING,
    RPC_REQ_CLIENT_RESPONSE_SEND,
    RPC_REQ_CLIENT_DONE,
    RPC_REQ_CLIENT_INNER_REQUEST_DONE,
    REDIS_REQ_CLIENT_DONE,
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
    RPC_REQ_SERVER_SHARDING_REQUEST_HANDLING_IDEL,
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

  enum RpcReqSplitContEvent
  {
    INNER_REQUEST_DONE = 0,
    INNER_REQUEST_ERROR,
    SPLIT_CONT_SELF_TRY_FREE
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
  void reset(); //only reset not release this object
  void finish(); // update stat and clean request sm
  void inner_request_cleanup();   //Called when the subtask ends to clean up all subtask data
  void server_handle_request_failed();   // server rpc handle request failed

  char    *get_request_buf() const {
     return is_use_request_buf_for_serialize_ ? request_buf_for_serialize_ : shared_request_buf_ ? shared_request_buf_->get_buf() : nullptr;
  }
  char    *get_request_buf_for_serialize() const { return request_buf_for_serialize_; }
  char    *get_request_inner_buf() { return request_inner_buf_; }
  char    *get_response_inner_buf() { return response_inner_buf_; }
  char    *get_response_buf() { return response_buf_; }
  int64_t  get_request_buf_len() const {
     return is_use_request_buf_for_serialize_ ? request_buf_for_serialize_len_ : shared_request_buf_ ? shared_request_buf_->get_len() : 0;
  }
  int64_t  get_request_buf_for_serialize_len() const { return request_buf_for_serialize_len_; }
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
  bool     is_server_failed() const { return obkv_info_.retry_info_.is_server_failed(); }
  bool     is_use_request_buf_for_serialize() const { return is_use_request_buf_for_serialize_; }

  int32_t      get_rpc_req_error_code() const { return obkv_info_.retry_info_.rpc_origin_error_code_; }
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
  bool is_in_server_entry_sending() {
     ServerNetState &sstate = get_snet_state() ;
     return sstate > ObRpcReq::ServerNetState::RPC_REQ_SERVER_INIT && sstate < RPC_REQ_SERVER_REQUST_SENDED;
  }

  bool is_has_server_sended() {
    ServerNetState &sstate = get_snet_state() ;
    return sstate >= ObRpcReq::ServerNetState::RPC_REQ_SERVER_REQUST_SENDING;
 }

  void retry_reset() {
    // Retrying will no longer reset the error code, but retain the error code.
    is_response_ = false;
    is_server_addr_set_ = false;
    server_add_.reset();
    server_entry_send_retry_times_ = 0;

    // OB_TABLE_API_BATCH_EXECUTE  and QUERY could not change partition info
    // LSOP could not change if has not send to server
    bool could_not_clean_flag = is_inner_request() && (get_obkv_info().get_pcode() != obrpc::OB_TABLE_API_LS_EXECUTE
                                  || ((get_obkv_info().get_pcode() == obrpc::OB_TABLE_API_LS_EXECUTE) && !is_has_server_sended()));
    obkv_info_.retry_reset(!could_not_clean_flag);
    snet_state_ = RPC_REQ_SERVER_INIT; //reclean
  }

  bool is_could_send_next_node_retry() {
    // bool is_in_congestion_retry = obkv_info_.retry_info_.is_in_congestion_retry();
    bool is_require_reroute = obkv_info_.is_bad_routing() && (obkv_info_.get_error_code() == 0 || obkv_info_.get_error_code() == OB_NOT_MASTER);
    return is_require_reroute;
  }

  bool is_valid() const { return !is_invalid(); }
  bool is_invalid() const { return obkv_info_.is_inner_request_ && (OB_ISNULL(root_rpc_req_) || root_rpc_req_->canceled()); }

  void set_request_buf(char *req_buf) { // for test case
    shared_request_buf_->set_buf(req_buf);
  }
  void set_request_buf_len(int64_t req_buf_len) { // for test case
    shared_request_buf_->set_len(req_buf_len);
  }
  void  set_request_len(int64_t req_len) { request_len_ = req_len; }
  void  set_request_buf_for_serialize_len(int64_t req_buf_for_serialize_len) { request_buf_for_serialize_len_ = req_buf_for_serialize_len; }

  void  set_response_buf(char *res_buf) { response_buf_ = res_buf; }
  void  set_response_buf_len(int64_t res_buf_len) { response_buf_len_ = res_buf_len; }
  void  set_response_len(int64_t res_len) { response_len_ = res_len; }
  void  set_rpc_type(obkv::ObProxyRpcType type) { rpc_type_ = type; }
  void  set_response(bool is_response) { is_response_ = is_response; }
  void  set_origin_channel_id(uint32_t channel_id) { origin_channel_id_ = channel_id; }
  void  set_client_channel_id(uint32_t channel_id) { c_channel_id_ = channel_id; }
  void  set_server_channel_id(uint32_t channel_id) { s_channel_id_ = channel_id; }
  void  set_inner_request(bool flag) { obkv_info_.is_inner_request_ = flag; }
  void  set_internal_rpc_request(bool flag) { obkv_info_.is_internal_rpc_request_ = flag; }
  void  set_internal_rpc_request_has_done(bool flag) { obkv_info_.is_internal_rpc_request_has_done_ = flag; }
  void  set_rpc_request_with_partition_id(bool flag) { obkv_info_.is_rpc_request_with_partition_id_ = flag; }
  void  set_use_request_inner_buf(bool flag) { is_use_request_inner_buf_ = flag; }
  void  set_use_response_inner_buf(bool flag) { is_use_response_inner_buf_ = flag; }
  void  set_rpc_req_error_code(int error_code) { obkv_info_.retry_info_.rpc_origin_error_code_ = error_code; }
  void  set_use_request_buf_for_serialize(bool flag) { is_use_request_buf_for_serialize_ = flag; }
  void  set_server_failed(bool is_failed) {
    obkv_info_.retry_info_.set_server_failed(is_failed);
    if(is_failed) {
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

  ObRpcRedisInfo *get_redis_info() const {
    return rpc_type_ == obkv::ObProxyRpcType::OBPROXY_RPC_REDIS ? reinterpret_cast<ObRpcRedisInfo *>(request_info_) : NULL;
  }

  char *get_login_info() { return login_info_; }

  int rewrite_login_info(ObString &server_version);
  int free_login_info();
  int init_rpc_redis_info();
  int free_rpc_redis_info();
  int realloc_request_buf(uint64_t len);
  int alloc_request_buf(uint64_t len);
  int alloc_request_buf_for_serialize(uint64_t len);
  int alloc_request_inner_buf(uint64_t len);
  int alloc_response_buf(uint64_t len);
  int alloc_response_inner_buf(uint64_t len);
  int free_request_buf();
  int free_request_buf_for_serialize();
  int free_request_inner_buf();
  int free_response_buf();
  int free_response_inner_buf();

  int share_request_buf_with(ObRpcReq* child_req);
  ObSharedRequestBuf* get_shared_request_buf() { return shared_request_buf_; }

  int alloc_inner_request_allocator();
  int free_inner_request_allocator();
  common::ObIAllocator *get_inner_request_allocator() { return inner_request_allocator_; }

  void free_split_cont();
  bool canceled() const { return is_canceled_; }
  // obproxy侧执行的请求、不用发到observer
  bool is_internal_rpc_request() const { return obkv_info_.is_internal_rpc_request_; }
  bool is_internal_rpc_request_has_done() const { return obkv_info_.is_internal_rpc_request_has_done_; }
  bool is_rpc_request_with_partition_id() const { return obkv_info_.is_rpc_request_with_partition_id_; }
  // 内部子请求
  bool is_inner_request() const { return obkv_info_.is_inner_request_; }
  bool could_release_request();
  bool could_cleanup_inner_request();
  void server_net_cancel_request();
  void client_net_cancel_request();
  void cancel_request();
  void inc_req_buf_repeat_times() { req_buf_repeat_times_++; }
  //to cleanup the request while server is handing but not return response, directly
  void inform_server_net_cleanup_timeout_request();

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

  int alloc_rpc_request_for_redis(obkv::ObRpcPacketCode pcode);
  int alloc_rpc_request();
  int alloc_rpc_response();
  int free_rpc_request();
  int free_rpc_response();
  void set_rpc_request(obkv::ObRpcRequest *rpc_request) { rpc_request_ = rpc_request; }
  void set_rpc_response(obkv::ObRpcResponse *rpc_response) { rpc_response_ = rpc_response; }
  void set_rpc_request_len(int64_t len) { rpc_request_len_ = len; }
  void set_rpc_response_len(int64_t len) { rpc_response_len_ = len; }
  int32_t get_error_code()  { return obkv_info_.get_error_code(); }

  const ObRpcReqTraceId &get_trace_id() const { return obkv_info_.get_req_trace_id(); }
  ObRpcReqTraceId &get_trace_id() { return obkv_info_.get_req_trace_id(); }

  void set_client_net_timeout_us(int64_t timeout_us) { client_net_timeout_us_ = timeout_us; }
  int64_t get_client_net_timeout_us() const { return client_net_timeout_us_; }
  void set_server_net_timeout_us(int64_t timeout_us) { server_net_timeout_us_ = timeout_us; }
  int64_t get_server_net_timeout_us() const { return server_net_timeout_us_; }
  void set_inner_request_timeout_us(int64_t timeout_us) { inner_request_timeout_us_ = timeout_us; }
  int64_t get_inner_request_timeout_us() const { return inner_request_timeout_us_; }

  void set_root_rpc_req(ObRpcReq *rpc_req) { root_rpc_req_ = rpc_req; }
  ObRpcReq *get_root_rpc_req() { return root_rpc_req_; }
  int64_t &get_current_sub_rpc_req_count() { return current_sub_rpc_req_count_; }

  int sub_rpc_req_init(ObRpcReq *root_rpc_req, ObRpcRequestSM *sm, obkv::ObRpcRequest *rpc_request, int64_t rpc_request_len,
                               int64_t cont_index, int64_t partition_id, common::ObIAllocator *allocator, int64_t ls_id  = 0);
  bool is_sub_req_inited() const { return is_sub_req_inited_; }
  bool is_sub_req_callback() const { return is_sub_req_callback_; }
  bool is_split_cont_canceled() const { return is_split_cont_canceled_; }
  void set_split_cont_canceled(bool is_split_cont_canceled) { is_split_cont_canceled_ = is_split_cont_canceled; }
  void set_sub_req_inited(const bool is_sub_req_inited) { is_sub_req_inited_ = is_sub_req_inited; }
  void set_sub_req_callback(const bool is_sub_req_callback) { is_sub_req_callback_ = is_sub_req_callback; }
  void set_received_cancel_from_split_cont(bool is_received_cancel_from_split_cont) { is_received_cancel_from_split_cont_ = is_received_cancel_from_split_cont; }
  bool is_received_cancel_from_split_cont() const { return is_received_cancel_from_split_cont_; }

  DECLARE_TO_STRING;

public:
  ObRpcReqMagic magic_;

  event::ObContinuation *sm_;  //指向请求的处理状态逻辑（ObRpcRequestSM *）
  event::ObContinuation *cnet_sm_;  //
  event::ObContinuation *snet_sm_;  //
  engine::ObProxyRpcReqSplitCont *split_cont_;
  ObRpcClientMilestones client_timestamp_;
  ObRpcServerMilestones server_timestamp_;
  int64_t server_entry_send_retry_times_;
  int64_t cs_id_;
  int64_t ss_id_;
  RpcReqCongestStatus congest_status_;

  static const int64_t MAX_SCATTER_LEN;

private:
  ObSharedRequestBuf *shared_request_buf_;  //byte data buffer received or to be send (with ref count)
  char *request_buf_for_serialize_;     //byte data buffer which used for serialize(only used inner)
  char *request_inner_buf_;             //byte data buffer which need re-serialize(only used in)
  char *response_buf_;                  //byte data buffer which is for response
  char *response_inner_buf_;            //byte data buffer which is for need re-serialize(only used in)

  // request_buf_len_ 字段已移除，长度信息存储在shared_request_buf_中
  int64_t request_buf_for_serialize_len_; //buffer length for request_buf_for_serialize_
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

  bool is_use_request_buf_for_serialize_;
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
  int64_t inner_request_timeout_us_;

  ObRpcReq *root_rpc_req_;         // 子任务需要维护root rpc req指针
  int64_t current_sub_rpc_req_count_;

  ObRpcOBKVInfo obkv_info_;
  ObRpcRequestConfigInfo config_info_;
  bool is_sub_req_inited_;
  bool is_sub_req_callback_;
  bool is_split_cont_canceled_;
  bool is_received_cancel_from_split_cont_;
  void *request_info_;
  char *login_info_;

  DISALLOW_COPY_AND_ASSIGN(ObRpcReq);

  friend class ObRpcRequestSM;
};

typedef common::ObList<ObRpcReq*> ObRpcReqList;

inline ObRpcReq* ObRpcReq::allocate()
{
  ObRpcReq *rpc_req = op_thread_alloc_init(ObRpcReq, event::get_rpc_req_allocator(), ObRpcReq::instantiate_func);
  obkv::get_global_rpc_throttle().push_index();
  obkv::get_global_rpc_throttle().update_holding_resource(sizeof(ObRpcReq));
  RPC_REQ_INCREMENT_DYN_STAT(event::this_ethread(), CURRENTLY_HANDLING_RPC_REQ);
  PROXY_LOG(DEBUG, "ObRpcReq::allocate", KP(rpc_req));
  return rpc_req;
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
    obkv::get_global_rpc_throttle().update_holding_resource(alloc_size);
    inner_request_allocator_ = new (buf) common::ObArenaAllocator;
    inner_request_allocator_len_ = alloc_size;
  }

  return ret;
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
      obkv::get_global_rpc_throttle().update_holding_resource(-rpc_response_len_);
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
      if (obkv_info_.is_inner_request_) {
        // clear in ~SplitCont()
        if (is_sub_req_callback_ && !is_received_cancel_from_split_cont_) {
          // do nothing
        } else {
          rpc_response_->~ObRpcResponse();
        }
      } else {
        rpc_response_->~ObRpcResponse();
        op_fixed_mem_free(rpc_response_, rpc_response_len_);
        obkv::get_global_rpc_throttle().update_holding_resource(-rpc_response_len_);
      }
      rpc_response_ = NULL;
      rpc_response_len_ = 0;
    }
  }

  return ret;
}

inline int ObRpcReq::alloc_request_buf(uint64_t len)
{
  PROXY_LOG(DEBUG, "alloc request buf", K(len));
  int ret = common::OB_SUCCESS;
  // free buf if has alloc
  if (OB_UNLIKELY(NULL != shared_request_buf_)) {
    if (OB_FAIL(free_request_buf())) {
      PROXY_LOG(EDIAG, "free request buf error", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t alloc_size = sizeof(ObSharedRequestBuf);
    void* tmp_buf = ob_malloc(alloc_size, common::ObModIds::OB_RPC_SHARED_REQUEST_BUF);
    if (OB_ISNULL(tmp_buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(EDIAG, "fail to alloc mem", K(alloc_size), K(ret));
    } else {
      shared_request_buf_ = new (tmp_buf) ObSharedRequestBuf();
      if (OB_ISNULL(shared_request_buf_)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        PROXY_LOG(EDIAG, "fail to alloc mem for shared request buf", K(alloc_size), K(ret));
      } else if (OB_FAIL(shared_request_buf_->init(len))) {
        PROXY_LOG(EDIAG, "fail to init shared request buf", K(alloc_size), K(ret));
        // will free in free_request_buf() and can not use op_fixed_mem_free
        // op_fixed_mem_free(tmp_buf, alloc_size);
        // obkv::get_global_rpc_throttle().update_holding_resource(-alloc_size);
      } else {
        shared_request_buf_->inc_ref();
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(tmp_buf) && OB_ISNULL(shared_request_buf_)) {
      ob_free(tmp_buf);
      tmp_buf = NULL;
      obkv::get_global_rpc_throttle().update_holding_resource(-alloc_size);
    }
  }
  return ret;
}

// used by inner request and redis request(for serialize)
inline int ObRpcReq::realloc_request_buf(uint64_t len)
{
  PROXY_LOG(DEBUG, "realloc request buf", K(len));
  int ret = common::OB_SUCCESS;
  // free buf if has alloc
  if (OB_UNLIKELY(NULL == shared_request_buf_)) {
    ret = alloc_request_buf_for_serialize(len);
  } else if (len == 0) {
    //to free
    ret = free_request_buf_for_serialize();
  } else {
    // request_buf_for_serialize_ use for inner request encode, shared_request_buf_ use for inner request ref count
    request_buf_for_serialize_ = reinterpret_cast<char *>(op_fixed_mem_alloc(len));
    if (OB_UNLIKELY(NULL == request_buf_for_serialize_)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(EDIAG, "fail to alloc mem", K(len), K(ret));
    } else {
      MEMSET(request_buf_for_serialize_, '\0', len);
      uint64_t request_buf_len = get_request_buf_len();
      uint64_t data_len = len > request_buf_len ? request_buf_len : len;
      PROXY_LOG(DEBUG, "copy len", K(data_len), K(len), K(request_buf_len));
      MEMCPY(request_buf_for_serialize_, get_request_buf(), data_len);
      request_buf_for_serialize_len_ = len;
    }
  }
  return ret;
}

inline int ObRpcReq::free_request_buf()
{
  int ret = common::OB_SUCCESS;
  if (NULL != shared_request_buf_) {
    int64_t old_ref = shared_request_buf_->get_ref_count();
    PROXY_LOG(DEBUG, "free request buf", K(old_ref), KPC(this));
    shared_request_buf_->dec_ref();
    if (1 == old_ref) {
      ob_free(shared_request_buf_);
      obkv::get_global_rpc_throttle().update_holding_resource(-sizeof(ObSharedRequestBuf));
    }
    shared_request_buf_ = NULL;
  }
  return ret;
}

inline int ObRpcReq::alloc_request_buf_for_serialize(uint64_t len)
{
  PROXY_LOG(DEBUG, "alloc request buf for serialize", K(len));
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(NULL != request_buf_for_serialize_)) {
    if (OB_FAIL(free_request_buf_for_serialize())) {
      PROXY_LOG(EDIAG, "free request buf for serialize error", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(len));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(EDIAG, "fail to alloc mem", K(len), K(ret));
    } else {
      MEMSET(buf, '\0', len);
      obkv::get_global_rpc_throttle().update_holding_resource(len);
      request_buf_for_serialize_ = buf;
      request_buf_for_serialize_len_ = len;
    }
  }
  return ret;
}


inline int ObRpcReq::free_request_buf_for_serialize()
{
  PROXY_LOG(DEBUG, "free request buf for serialize", K(request_buf_for_serialize_len_));
  int ret = common::OB_SUCCESS;
  if (NULL != request_buf_for_serialize_) {
    if (request_buf_for_serialize_len_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(EDIAG, "request_buf_for_serialize_len_ must > 0", K_(request_buf_for_serialize_len), K_(request_buf_for_serialize), K(ret));
    } else {
      op_fixed_mem_free(request_buf_for_serialize_, request_buf_for_serialize_len_);
      obkv::get_global_rpc_throttle().update_holding_resource(-request_buf_for_serialize_len_);
      request_buf_for_serialize_ = NULL;
      request_buf_for_serialize_len_ = 0;
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
      MEMSET(buf, '\0', len);
      obkv::get_global_rpc_throttle().update_holding_resource(len);
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
      obkv::get_global_rpc_throttle().update_holding_resource(len);
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
      obkv::get_global_rpc_throttle().update_holding_resource(-request_inner_buf_len_);
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
      obkv::get_global_rpc_throttle().update_holding_resource(-response_inner_buf_len_);
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
        obkv::get_global_rpc_throttle().update_holding_resource(len);
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
      if (obkv_info_.is_inner_request_) {
        // do nothing, just reset inner_request_allocator_
      } else {
        op_fixed_mem_free(response_buf_, response_buf_len_);
        obkv::get_global_rpc_throttle().update_holding_resource(-response_buf_len_);
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
  bool need_parse_response_fully = !is_error()
                                    && (is_inner_request_ || pcode_ == obrpc::ObRpcPacketCode::OB_TABLE_API_LOGIN
                                                          || pcode_ == obrpc::ObRpcPacketCode::OB_REDIS_EXECUTE_V2);
  bool need_parse_response_fully_for_async_query = pcode_ == obrpc::ObRpcPacketCode::OB_TABLE_API_EXECUTE_QUERY_SYNC && (!is_error() || (is_server_support_distributed_execute_ && 0 == retry_info_.rpc_origin_error_code_));
  return need_parse_response_fully || need_parse_response_fully_for_async_query;
}

inline common::ObString get_rpc_type_string(const obkv::ObProxyRpcType type)
{
  const char *str = "";
  switch (type)
  {
  case obkv::OBPROXY_RPC_OBRPC:
    str = "OB_RPC";
    break;
  case obkv::OBPROXY_RPC_REDIS:
    str = "OB_REDIS";
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

inline int ObRpcReq::init_rpc_redis_info()
{

  int ret = common::OB_SUCCESS;
  int64_t len = sizeof(ObRpcRedisInfo);
  ObRpcRedisInfo *redis_info = NULL;
  ARR_ARGS *redis_arr_args = NULL;

  if (rpc_type_ != obkv::OBPROXY_RPC_REDIS) {
    rpc_type_ = obkv::OBPROXY_RPC_REDIS;
  }

  char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(sizeof(ObRpcRedisInfo)));
  char *xbuf = reinterpret_cast<char *>(op_fixed_mem_alloc(sizeof(ARR_ARGS)));

  if (OB_UNLIKELY(NULL == buf || NULL == xbuf)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(EDIAG, "fail to alloc mem for rpc_redis_info", K(len), K(buf), K(xbuf), K(ret));
  } else if (OB_ISNULL(redis_info = new (buf) ObRpcRedisInfo())) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_LOG(EDIAG, "fail to init rpc_redis_info", K(len), K(ret), K(buf));
  } else if (OB_FAIL(redis_info->alloc_request_buf(OB_RPC_REDIS_DEFAULT_BUF_SIZE))) {
    PROXY_LOG(WDIAG, "fail to init rpc_redis_info request buf", K(len), K(ret), K(buf));
  } else if (OB_ISNULL(redis_arr_args = new (xbuf) ARR_ARGS(common::ObModIds::OB_RPC_TABLE_REDIS, sizeof(ObString) * COMMON_REDIS_ARGS_COUNT))) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_LOG(EDIAG, "fail to init redis_arr_args", K(len), K(ret), K(xbuf));
  } else {
    redis_info->set_redis_args(redis_arr_args);
    request_info_ = redis_info;
    PROXY_LOG(DEBUG, "succ to init rpc_redis_info request buf", K(redis_info), K(ret), K(buf), K_(rpc_type));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(redis_info)) {
      if (OB_NOT_NULL(redis_arr_args)) {
        redis_arr_args->reset();
        redis_info->set_redis_args(NULL);
      }
      redis_info->~ObRpcRedisInfo();
      redis_info = NULL;
    }
    if (OB_NOT_NULL(buf)) {
      op_fixed_mem_free(buf, sizeof(ObRpcRedisInfo));
      buf = NULL;
    }
    if (OB_NOT_NULL(xbuf)) {
      op_fixed_mem_free(xbuf, sizeof(ARR_ARGS));
      xbuf = NULL;
    }
  }
  return ret;
}

inline int ObRpcReq::free_rpc_redis_info()
{
  int ret = common::OB_SUCCESS;
  ObRpcRedisInfo *redis_info = NULL;
  ARR_ARGS *redis_args_arr = NULL;
  if (OB_NOT_NULL(redis_info = get_redis_info())) {
    if (OB_NOT_NULL(redis_args_arr = redis_info->get_redis_args())) {
      redis_args_arr->reset();
      op_fixed_mem_free(redis_args_arr, sizeof(ARR_ARGS));
      redis_info->set_redis_args(NULL);
    }
    redis_info->~ObRpcRedisInfo();
    op_fixed_mem_free(redis_info, sizeof(ObRpcRedisInfo));
    request_info_ = NULL;
  }
  return ret;
}

inline int ObRpcReq::rewrite_login_info(ObString &server_version)
{
  int ret = common::OB_SUCCESS;
  login_info_ = reinterpret_cast<char *>(op_fixed_mem_alloc(MAX_VERSION_LEN));
  if (OB_UNLIKELY(NULL == login_info_)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(EDIAG, "fail to alloc mem for login_info", K(MAX_VERSION_LEN), K(ret));
  } else {
    int64_t len = 0;
    len = snprintf(login_info_, MAX_VERSION_LEN, "%s + Obproxy %s", server_version.ptr(), PACKAGE_VERSION);
    if (len < 0) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_LOG(WDIAG, "fail to snprintf version_info", K(ret));
    } else {
      login_info_[len] = '\0';
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(login_info_)) {
      op_fixed_mem_free(login_info_, MAX_VERSION_LEN);
      login_info_ = NULL;
    }
  }
  return ret;
}

inline int ObRpcReq::free_login_info()
{
  int ret = common::OB_SUCCESS;
  if (OB_NOT_NULL(login_info_)) {
    op_fixed_mem_free(login_info_, MAX_VERSION_LEN);
    login_info_ = NULL;
  }
  return ret;
}

inline int ObRpcReq::share_request_buf_with(ObRpcReq* child_req)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(child_req)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(EDIAG, "invalid child request", K(ret));
  } else if (OB_ISNULL(shared_request_buf_)) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_LOG(EDIAG, "no shared request buf to share", K(ret));
  } else {
    PROXY_LOG(DEBUG, "share request buf with child", K(shared_request_buf_->get_ref_count()), KPC(child_req));
    child_req->shared_request_buf_ = shared_request_buf_;
    child_req->shared_request_buf_->inc_ref();
    PROXY_LOG(DEBUG, "after share request buf with child", K(shared_request_buf_->get_ref_count()));
  }
  return ret;
}

}
}
}
#endif
