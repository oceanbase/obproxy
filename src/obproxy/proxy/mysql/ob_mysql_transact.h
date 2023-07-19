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

#ifndef OBPROXY_MYSQL_TRANSACT_H
#define OBPROXY_MYSQL_TRANSACT_H

#include "iocore/net/ob_net.h"
#include "utils/ob_proxy_utils.h"
#include "stat/ob_mysql_stats.h"
#include "obutils/ob_congestion_entry.h"
#include "proxy/mysqllib/ob_proxy_mysql_request.h"
#include "proxy/mysqllib/ob_proxy_auth_parser.h"
#include "proxy/mysqllib/ob_mysql_response.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "proxy/mysqllib/ob_mysql_config_processor.h"
#include "proxy/route/ob_table_cache.h"
#include "proxy/route/ob_server_route.h"
#include "proxy/route/ob_tenant_server.h"
#include "proxy/mysql/ob_mysql_proxy_port.h"
#include "cmd/ob_show_sqlaudit_handler.h"
#include "proxy/api/ob_transform.h"
#include "proxy/mysqllib/ob_proxy_ob20_request.h"
#include "lib/oblog/ob_simple_trace.h"
#include "obutils/ob_read_stale_processor.h"
namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{
class ObShardConnector;
}
namespace proxy
{

#define TRANSACT_RETURN(n, r) \
s.next_action_ = n; \
s.transact_return_point = r; \
int64_t stack_start = event::self_ethread().stack_start_; \
_PROXY_TXN_LOG(DEBUG, "sm_id=%u, stack_size=%ld, next_action=%s, return=%s", \
               s.sm_->sm_id_, stack_start - reinterpret_cast<int64_t>(&stack_start), #n, #r); \

class ObMysqlSM;
class ObMysqlClientSession;
class ObClientSessionInfo;
class ObServerSessionInfo;
class ObSqlauditRecordQueue;

enum
{
  MYSQL_UNDEFINED_CL = -1
};

class ObMysqlTransact
{
public:
  enum ObAbortStateType
  {
    ABORT_UNDEFINED = 0,
    DIDNOT_ABORT,
    MAYBE_ABORTED,
    ABORTED
  };
  enum ObPLLookupState 
  {
    NEED_PL_LOOKUP = 0,
    USE_LAST_SERVER_SESSION,
    USE_COORDINATOR_SESSION
  };

  enum ObMysqlTransactMagic
  {
    MYSQL_TRANSACT_MAGIC_ALIVE = 0x00001234,
    MYSQL_TRANSACT_MAGIC_DEAD = 0xDEAD1234
  };

  // server session retry connect status
  enum ObSSRetryStatus
  {
    FOUND_EXISTING_ADDR,
    NOT_FOUND_EXISTING_ADDR,
    NO_NEED_RETRY
  };

  static common::ObString get_retry_status_string(const ObSSRetryStatus status);
  static common::ObString get_pl_lookup_state_string(const ObPLLookupState state);


  // Please do not forget to fix ObServerState
  // (ob_api.h) in case of any modifications in
  // ObServerStateType
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
    DETECT_CONGESTED,
    INTERNAL_ERROR
  };

  enum ObServerRespErrorType
  {
    // init status
    MIN_RESP_ERROR = 0,

    // first login related
    LOGIN_SERVER_INIT_ERROR,
    LOGIN_SERVER_STOPPING_ERROR,
    LOGIN_TENANT_NOT_IN_SERVER_ERROR,
    LOGIN_SESSION_ENTRY_EXIST_ERROR,
    LOGIN_CLUSTER_NOT_MATCH_ERROR,
    LOGIN_CONNECT_ERROR,
    // handshake related
    HANDSHAKE_COMMON_ERROR,
    // saved login related
    SAVED_LOGIN_COMMON_ERROR,
    // sync session var related
    RESET_SESSION_VARS_COMMON_ERROR,
    // sync start trans related
    START_TRANS_COMMON_ERROR,
    // sync xa start related
    START_XA_START_ERROR,
    // sync database related
    SYNC_DATABASE_COMMON_ERROR,
    // sync com_stmt_prepare
    SYNC_PREPARE_COMMON_ERROR,
    // sync text ps prepare
    SYNC_TEXT_PS_PREPARE_COMMON_ERROR,
    // packet checksum error
    ORA_FATAL_ERROR,
    // request related
    REQUEST_TENANT_NOT_IN_SERVER_ERROR,
    REQUEST_SERVER_INIT_ERROR,
    REQUEST_SERVER_STOPPING_ERROR,
    REQUEST_CONNECT_ERROR,
    REQUEST_READ_ONLY_ERROR,
    REQUEST_REROUTE_ERROR,
    STANDBY_WEAK_READONLY_ERROR,
    TRANS_FREE_ROUTE_NOT_SUPPORTED_ERROR,
    // attention!! add error type between MIN_RESP_ERROR and MAX_RESP_ERROR
    MAX_RESP_ERROR
  };

  enum ObSourceType
  {
    SOURCE_NONE = 0,
    SOURCE_OBSERVER,
    SOURCE_TRANSFORM,
    SOURCE_INTERNAL             // generated from text buffer
  };

  // ObMysqlTransact fills a ObStateMachineActionType
  // to tell the state machine what to do next.
  enum ObStateMachineActionType
  {
    SM_ACTION_UNDEFINED = 0,

    SM_ACTION_SERVER_ADDR_LOOKUP,
    SM_ACTION_PARTITION_LOCATION_LOOKUP,
    SM_ACTION_CONGESTION_CONTROL_LOOKUP,
    SM_ACTION_INTERNAL_REQUEST,

    SM_ACTION_OBSERVER_OPEN,

    SM_ACTION_INTERNAL_NOOP,
    SM_ACTION_SEND_ERROR_NOOP,

    SM_ACTION_SERVER_READ,
    SM_ACTION_TRANSFORM_READ,

    SM_ACTION_API_SM_START,
    SM_ACTION_API_READ_REQUEST,
    SM_ACTION_API_OBSERVER_PL,
    SM_ACTION_API_SEND_REQUEST,
    SM_ACTION_API_READ_RESPONSE,
    SM_ACTION_API_SEND_RESPONSE,
    SM_ACTION_API_CMD_COMPLETE,
    SM_ACTION_API_SM_SHUTDOWN,

    SM_ACTION_BINLOG_LOCATION_LOOKUP,
  };

  enum ObAttachDummyEntryType
  {
    NO_TABLE_ENTRY_FOUND_ATTACH_TYPE = 0,
  };

  struct ObStatRecord
  {
    uint16_t index_;
    int64_t increment_;
  };

  struct ObStatBlock
  {
    ObStatBlock() : next_(NULL), next_insert_(0)
    {
      memset(&stats_, 0, sizeof(stats_));
    };

    ~ObStatBlock() { }

    void reset()
    {
      next_ = NULL;
      next_insert_ = 0;
    };

    static const int64_t STAT_BLOCK_ENTRIES = 28;
    ObStatRecord stats_[STAT_BLOCK_ENTRIES];
    ObStatBlock *next_;
    uint16_t next_insert_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObStatBlock);
  };

  struct ObConnectionAttributes
  {
    ObConnectionAttributes()
        : addr_(),
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
    void set_obproxy_addr(const sockaddr &sa) { net::ops_ip_copy(obproxy_addr_, sa); }

    uint32_t get_ipv4() { return net::ops_ip4_addr_host_order(addr_.sa_); }
    uint16_t get_port() { return net::ops_ip_port_host_order(addr_); }

    net::ObIpEndpoint addr_;    // use function below to get/set ip and port
    net::ObIpEndpoint obproxy_addr_;

    ObServerStateType state_;
    ObAbortStateType abort_;
    void reset()
    {
      state_ = STATE_UNDEFINED;
      abort_ = ABORT_UNDEFINED;
      addr_.reset();
      obproxy_addr_.reset();
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(ObConnectionAttributes);
  };

  enum ObServerSendActionType
  {
    SERVER_SEND_NONE = 0,
    SERVER_SEND_HANDSHAKE,
    SERVER_SEND_LOGIN,
    SERVER_SEND_SAVED_LOGIN,
    SERVER_SEND_ALL_SESSION_VARS,
    SERVER_SEND_USE_DATABASE,
    SERVER_SEND_SESSION_VARS,
    SERVER_SEND_SESSION_USER_VARS,
    SERVER_SEND_START_TRANS,
    SERVER_SEND_XA_START,
    SERVER_SEND_REQUEST,
    SERVER_SEND_PREPARE,
    SERVER_SEND_SSL_REQUEST,
    SERVER_SEND_TEXT_PS_PREPARE 
  };

  struct ObCurrentInfo
  {
    ObCurrentInfo()
        : state_(STATE_UNDEFINED),
          error_type_(MIN_RESP_ERROR),
          send_action_(SERVER_SEND_NONE),
          attempts_(1)
    { }
    ~ObCurrentInfo() { }

    void reset()
    {
      state_ = STATE_UNDEFINED;
      error_type_ = MIN_RESP_ERROR;
      send_action_ = SERVER_SEND_NONE;
      attempts_ = 1;
    }

    ObServerStateType state_;
    ObServerRespErrorType error_type_;
    ObServerSendActionType send_action_;
    int32_t attempts_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObCurrentInfo);
  };

  struct ObTransState;
  struct ObPartitionLookupInfo
  {
  public:
    enum ObForceRenewState
    {
      NO_NEED_RENEW = 0,
      PREPARE_RENEW,
      RENEW_DONE
    };

    ObPartitionLookupInfo()
        : lookup_success_(false), cached_dummy_entry_renew_state_(NO_NEED_RENEW),
          pl_attempts_(0), force_renew_state_(NO_NEED_RENEW), te_name_(), route_(),
          is_need_force_flush_(false)
    {
    }
    ~ObPartitionLookupInfo() {}

    void pl_update_if_necessary(const ObTransState &s);
    void pl_update_for_reroute(const ObTransState &s);
    void renew_last_valid_time() { route_.renew_last_valid_time(); }
    bool need_update_entry() const { return route_.need_update_entry(); }
    bool need_update_entry_by_partition_hit();
    bool is_partition_table() const { return route_.is_partition_table(); }
    bool is_all_iterate_once() const { return route_.is_all_iterate_once(); }
    void reset_cursor() { route_.reset_cursor(); }
    bool is_leader_existent() const { return route_.is_leader_existent(); }
    bool is_leader_server() const { return route_.is_leader_server(); }
    bool is_target_location_server() const { return route_.is_target_location_server(); }
    bool is_server_from_rslist() const { return route_.is_server_from_rslist(); }
    bool is_no_route_info_found() const { return route_.is_no_route_info_found(); }
    int64_t get_last_valid_time_us() const { return route_.get_last_valid_time_us(); }
    int64_t replica_size() const { return route_.replica_size(); }
    int64_t to_string(char *buf, const int64_t buf_len) const;
    common::ObConsistencyLevel get_consistency_level() const { return route_.get_consistency_level(); }
    bool is_strong_read() const { return route_.is_strong_read(); }
    bool is_weak_read() const { return route_.is_weak_read(); }
    ObTableEntry *get_dummy_entry() { return route_.get_dummy_entry(); }
    void set_route_info(ObMysqlRouteResult &result) { route_.set_route_info(result); }
    bool set_target_dirty() { return route_.set_target_dirty(); }
    bool set_dirty_all(bool is_need_force_flush = false) { return route_.set_dirty_all(is_need_force_flush); }
    bool set_delay_update() { return route_.set_delay_update(); }
    bool need_update_dummy_entry() const { return route_.need_update_dummy_entry(); }
    void set_need_force_flush(bool is_need_force_flush) { is_need_force_flush_ = is_need_force_flush; }
    bool is_need_force_flush() { return is_need_force_flush_; }
    bool is_remote_readonly() { return route_.is_remote_readonly(); }


    int get_next_avail_replica(const bool is_force_retry,
                               int32_t &attempt_count,
                               bool &found_leader_force_congested,
                               obutils::ObReadStaleParam &read_stale_param,
                               bool &is_all_stale,
                               const ObProxyReplicaLocation *&replica);
    const ObProxyReplicaLocation *get_next_avail_replica()
    {
      return route_.get_next_avail_replica();
    }
    const ObProxyReplicaLocation *get_leader_replica_from_remote()
    {
      return route_.get_leader_replica_from_remote();
    }
    void reset_consistency();
    void reset_pl();
    void reset();

    // if pl has force renewed, don't renew twice
    void set_force_renew()
    {
      if (NO_NEED_RENEW == force_renew_state_) {
        force_renew_state_ = PREPARE_RENEW;
      }
    }
    bool is_force_renew() const { return PREPARE_RENEW == force_renew_state_; }
    void set_force_renew_done() { force_renew_state_ = RENEW_DONE; }

    // if cached dummy entry renewed, don't renew twice
    void set_cached_dummy_force_renew()
    {
      if (NO_NEED_RENEW == cached_dummy_entry_renew_state_) {
        cached_dummy_entry_renew_state_ = PREPARE_RENEW;
      }
    }
    bool is_cached_dummy_force_renew() const { return PREPARE_RENEW == cached_dummy_entry_renew_state_; }
    bool is_cached_dummy_avail_force_renew() const { return NO_NEED_RENEW == cached_dummy_entry_renew_state_; }
    void set_cached_dummy_force_renew_done() { cached_dummy_entry_renew_state_ = RENEW_DONE; }

  public:
    bool lookup_success_;
    ObForceRenewState cached_dummy_entry_renew_state_;
    int64_t pl_attempts_;
    ObForceRenewState force_renew_state_;
    ObTableEntryName te_name_;
    ObServerRoute route_;
    bool is_need_force_flush_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObPartitionLookupInfo);
  };

  struct ObTransactInfo
  {
    ObTransactInfo()
        : client_request_(),
          server_response_(),
          request_content_length_(MYSQL_UNDEFINED_CL),
          transform_request_cl_(MYSQL_UNDEFINED_CL),
          transform_response_cl_(MYSQL_UNDEFINED_CL),
          sql_cmd_(obmysql::OB_MYSQL_COM_END)
    { }
    ~ObTransactInfo() { }
    void reset()
    {
      client_request_.reuse();
      server_response_.reset();
      sql_cmd_ = obmysql::OB_MYSQL_COM_END;
    }

    common::ObString get_print_sql(const int64_t sql_len = PRINT_SQL_LEN)
    {
      common::ObString ret;
      switch (sql_cmd_) {
        case obmysql::OB_MYSQL_COM_HANDSHAKE:
          ret = common::ObString::make_string("OB_MYSQL_COM_HANDSHAKE");
          break;
        case obmysql::OB_MYSQL_COM_LOGIN:
          ret = common::ObString::make_string("OB_MYSQL_COM_LOGIN");
          break;
        default:
          ret = client_request_.get_print_sql(sql_len);
          break;
      }
      return ret;
    }

    ObProxyMysqlRequest client_request_; // for mysql packets except login packet
    ObMysqlResp server_response_;        // for server response
    int64_t request_content_length_;
    int64_t transform_request_cl_;
    int64_t transform_response_cl_;
    obmysql::ObMySQLCmd sql_cmd_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObTransactInfo);
  };

  struct ObTransState
  {
    // Constructor
    ObTransState()
        : magic_(MYSQL_TRANSACT_MAGIC_ALIVE),
          sm_(NULL),
          mysql_config_params_(NULL),
          is_rerouted_(false),
          pl_lookup_state_ (NEED_PL_LOOKUP),
          is_auth_request_(false),
          is_trans_first_request_(false),
          is_proxysys_tenant_(false),
          is_hold_start_trans_(false),
          is_hold_xa_start_(false),
          send_reqeust_direct_(false),
          source_(SOURCE_NONE),
          pre_transform_source_(SOURCE_NONE),
          next_action_(SM_ACTION_UNDEFINED),
          api_next_action_(SM_ACTION_UNDEFINED),
          transact_return_point(NULL),
          internal_buffer_(NULL),
          internal_reader_(NULL),
          reroute_info_(),
          pll_info_(),
          mysql_errcode_(0),
          mysql_errmsg_(NULL),
          inner_errcode_(0),
          inner_errmsg_(NULL),
          first_stats_(),
          current_stats_(NULL),
          api_txn_active_timeout_value_(-1),
          api_txn_connect_timeout_value_(-1),
          api_txn_no_activity_timeout_value_(-1),
          congestion_entry_(NULL),
          congestion_entry_not_exist_count_(0),
          need_congestion_lookup_(true),
          congestion_lookup_success_(false),
          force_retry_congested_(false),
          is_congestion_entry_updated_(false),
          api_mysql_sm_shutdown_(false),
          api_server_addr_set_(false),
          need_retry_(true),
          sqlaudit_record_queue_(NULL),
          trace_log_(),
          is_proxy_protocol_v2_request_(true),
          use_cmnt_target_db_server_(false),
          use_conf_target_db_server_(false)
    {
      memset(user_args_, 0, sizeof(user_args_));
    }

    ~ObTransState() { }

    // Methods
    int init(ObMysqlSM *sm)
    {
      int ret = common::OB_SUCCESS;
      if (OB_ISNULL(sm)) {
        ret = common::OB_INVALID_ARGUMENT;
        PROXY_TXN_LOG(WARN, "invalid argument", K(sm), K(ret));
      } else {
        sm_ = sm;
        pl_lookup_state_ = NEED_PL_LOOKUP;
        current_stats_ = &first_stats_;
        trans_info_.reset();
      }
      return ret;
    }

    bool need_sqlaudit()
    {
      return ((mysql_config_params_->sqlaudit_mem_limited_ > 0) && (NULL != sqlaudit_record_queue_));
    }

    void refresh_mysql_config();
    int get_config_item(const common::ObString& cluster_name,
                        const common::ObString &tenant_name,
                        const obutils::ObVipAddr &addr,
                        const int64_t global_version = 0);
    void record_transaction_stats()
    {
      // Loop over our transaction stat blocks and record the stats
      // in the thread local arrays
      ObStatBlock *b = &first_stats_;
      event::ObEThread *ethread = event::this_ethread();
      ObRecRawStat *rsb_start =
          reinterpret_cast<ObRecRawStat *>(reinterpret_cast<char *>(ethread) + mysql_rsb->ethr_stat_offset_);

      while (NULL != b) {
        for (int64_t i = 0; i < b->next_insert_ && i < ObStatBlock::STAT_BLOCK_ENTRIES; ++i) {
          (rsb_start + b->stats_[i].index_)->sum_ += b->stats_[i].increment_;
          (rsb_start + b->stats_[i].index_)->count_ += 1;
        }
        b = b->next_;
      }
    }

    void record_transaction_stats(int64_t *stats, const int64_t stats_size)
    {
      // Loop over our transaction stat blocks and record the stats
      // in the stats arrays
      ObStatBlock *b = &first_stats_;

      while (NULL != b) {
        for (int64_t i = 0; i < b->next_insert_ && i < ObStatBlock::STAT_BLOCK_ENTRIES; ++i) {
          if (b->stats_[i].index_ < stats_size) {
            stats[b->stats_[i].index_] += b->stats_[i].increment_;
          }
        }
        b = b->next_;
      }
    }

    void update_transaction_stats()
    {
      record_transaction_stats();
      current_stats_ = &first_stats_;
      current_stats_->reset();
      arena_.reuse();
    }

    void set_alive_failed()
    {
      if (OB_LIKELY(NULL != congestion_entry_)
          && !is_congestion_entry_updated_) {
        congestion_entry_->set_alive_failed_at(event::get_hrtime());
        is_congestion_entry_updated_ = true;
      }
    }

    int alloc_internal_buffer(const int64_t buffer_block_size)
    {
      int ret = common::OB_SUCCESS;
      if (OB_UNLIKELY(NULL == internal_buffer_)) {
        if (OB_ISNULL(internal_buffer_ = event::new_empty_miobuffer(buffer_block_size))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          PROXY_TXN_LOG(ERROR, "failed to new miobuffer", K(ret));
        } else if (OB_ISNULL(internal_reader_ = internal_buffer_->alloc_reader())) {
          free_internal_buffer();
          ret = common::OB_INNER_STAT_ERROR;
          PROXY_TXN_LOG(WARN, "failed to allocate reader", K(ret));
        }
      } else if (OB_ISNULL(internal_reader_)) {
        free_internal_buffer();
        ret = common::OB_INNER_STAT_ERROR;
        PROXY_TXN_LOG(WARN, "internal reader should not be NULL",
                      K_(internal_buffer), K_(internal_reader), K(ret));
      } else if (NULL != cache_block_) {
        internal_buffer_->append_block_internal(cache_block_);
        cache_block_ = NULL;
      }

      if ((NULL != internal_buffer_) && (NULL != mysql_config_params_)) {
        internal_buffer_->water_mark_ = mysql_config_params_->default_buffer_water_mark_;
      }

      return ret;
    }

    void reset_internal_buffer()
    {
      if (OB_LIKELY(NULL != internal_buffer_)) {
        internal_buffer_->dealloc_all_readers();
        cache_block_ = internal_buffer_->writer_;
        if (NULL != cache_block_) {
          // only reserved the first block
          cache_block_->next_ = NULL;
          cache_block_->reset();
        }
        internal_buffer_->writer_ = NULL;
        internal_reader_ = internal_buffer_->alloc_reader();
        if (OB_ISNULL(internal_reader_)) {
          PROXY_TXN_LOG(WARN, "failed to allocate reader");
        }
      }
    }

    void free_internal_buffer()
    {
      if (OB_LIKELY(NULL != internal_buffer_)) {
        free_miobuffer(internal_buffer_);
        internal_buffer_ = NULL;
        internal_reader_ = NULL;
        cache_block_ = NULL;
      }
    }

    void reset()
    {
      update_transaction_stats();
      reset_internal_buffer();
      trans_info_.request_content_length_ = MYSQL_UNDEFINED_CL; // disable tunnel client request
      send_reqeust_direct_ = false;
      is_rerouted_ = false;
      reroute_info_.reset();
      use_cmnt_target_db_server_ = false;
      use_conf_target_db_server_ = false;

      if (CMD_COMPLETE == current_.state_) {
        if (!is_hold_start_trans_ && !is_hold_xa_start_) {
          is_trans_first_request_ = false;
        }

        if (obmysql::OB_MYSQL_COM_LOGIN == trans_info_.sql_cmd_) {
          is_auth_request_ = false;
        }
        pll_info_.reset_consistency();
      } else if (TRANSACTION_COMPLETE == current_.state_) {
        trace_log_.reset();
        is_trans_first_request_ = true;
        is_auth_request_ = false;
        reset_congestion_entry();
        mysql_errmsg_ = NULL;
        inner_errcode_ = 0;
        inner_errmsg_ = NULL;

        if (NULL != sqlaudit_record_queue_) {
          sqlaudit_record_queue_->refcount_dec();
          sqlaudit_record_queue_ = NULL;
        }
        is_proxy_protocol_v2_request_ = false;

        server_info_.reset();
        pre_server_info_.reset();
        current_.reset();
        trans_info_.reset();
        pll_info_.reset();
        // needn't reset trans_info, we will reset it when using client request and server response
        // trans_info_.reset();
      } else { /* do nothing */ }
    }

    void reset_congestion_entry()
    {
      if (NULL != congestion_entry_) {
        // if this trans succ, just set alive this server;
        if (!is_congestion_entry_updated_) {
          congestion_entry_->set_alive_congested_free();
        }
        congestion_entry_->dec_ref();
        congestion_entry_ = NULL;
      }
      is_congestion_entry_updated_ = false;
      congestion_entry_not_exist_count_ = 0;
      need_congestion_lookup_ = true;
      congestion_lookup_success_ = false;
      force_retry_congested_ = false;
    }

    void destroy()
    {
      record_transaction_stats();
      magic_ = MYSQL_TRANSACT_MAGIC_DEAD;
      trans_info_.client_request_.reset();
      trans_info_.server_response_.reset();
      free_internal_buffer();
      if (NULL != mysql_config_params_) {
        mysql_config_params_->dec_ref();
        mysql_config_params_ = NULL;
      }
      if (NULL != congestion_entry_) {
        congestion_entry_->dec_ref();
        congestion_entry_ = NULL;
      }
      congestion_entry_not_exist_count_ = 0;
      reroute_info_.reset();
      pll_info_.reset();
      if (NULL != sqlaudit_record_queue_) {
        sqlaudit_record_queue_->refcount_dec();
        sqlaudit_record_queue_ = NULL;
      }
      is_proxy_protocol_v2_request_ = false;
      arena_.reset();
    }

    static bool is_for_update_sql(common::ObString src_sql);
    common::ObConsistencyLevel get_trans_consistency_level(ObClientSessionInfo &cs_info);
    common::ObConsistencyLevel get_read_write_consistency_level(ObClientSessionInfo &session_info);
    bool is_request_readonly_zone_support(ObClientSessionInfo &cs_info);
    ObRoutePolicyEnum get_route_policy(ObMysqlClientSession &cs, const bool need_use_dup_replica);
    void get_route_policy(ObProxyRoutePolicyEnum policy, ObRoutePolicyEnum& ret_policy);
    bool is_need_pl_lookup() { return pl_lookup_state_ == NEED_PL_LOOKUP; }

    event::ObFixedArenaAllocator<1024> arena_;

    ObMysqlTransactMagic magic_;
    ObMysqlSM *sm_;

    ObMysqlConfigParams *mysql_config_params_;
    ObConnectionAttributes client_info_;
    ObConnectionAttributes server_info_;
    // fail-fast probe use
    ObConnectionAttributes pre_server_info_;

    ObCurrentInfo current_;
    ObTransactInfo trans_info_;

    bool is_rerouted_;
    // determin if do pl lookup 
    ObPLLookupState pl_lookup_state_;
    bool is_auth_request_;
    bool is_trans_first_request_;
    bool is_proxysys_tenant_;
    bool is_hold_start_trans_; // indicate whether hold begin(start transaction)
    bool is_hold_xa_start_;
    bool send_reqeust_direct_; // when send sync all session variables, we can send user request directly

    ObSourceType source_;
    ObSourceType pre_transform_source_;

    ObStateMachineActionType next_action_;
    ObStateMachineActionType api_next_action_;
    void (*transact_return_point)(ObMysqlTransact::ObTransState &s);

    event::ObMIOBuffer *internal_buffer_;
    event::ObIOBufferReader *internal_reader_;
    common::ObPtr<event::ObIOBufferBlock> cache_block_;

    ObProxyRerouteInfo reroute_info_;
    ObPartitionLookupInfo pll_info_;

    // used to building error packet, which will be sent to client
    int mysql_errcode_;
    const char *mysql_errmsg_;

    int inner_errcode_;
    const char *inner_errmsg_;

    ObStatBlock first_stats_;
    ObStatBlock *current_stats_;

    void *user_args_[MYSQL_SSN_TXN_MAX_USER_ARG];
    int32_t api_txn_active_timeout_value_;
    int32_t api_txn_connect_timeout_value_;
    int32_t api_txn_no_activity_timeout_value_;

    // congestion control
    obutils::ObCongestionEntry *congestion_entry_;
    int32_t congestion_entry_not_exist_count_;
    bool need_congestion_lookup_;
    bool congestion_lookup_success_;
    bool force_retry_congested_;

    bool is_congestion_entry_updated_;
    bool api_mysql_sm_shutdown_;
    bool api_server_addr_set_;
    bool need_retry_;

    ObSqlauditRecordQueue *sqlaudit_record_queue_;
    common::ObSimpleTrace<4096> trace_log_;
    bool is_proxy_protocol_v2_request_;
    // whether to use target db server from sql comment or multi level config
    bool use_cmnt_target_db_server_;
    bool use_conf_target_db_server_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObTransState);
  }; // End of State struct.

  static int return_last_bound_server_session(ObMysqlClientSession *client_session);
  static void modify_request(ObTransState &s);
  static bool is_sequence_request(ObTransState &s);
  static void handle_mysql_request(ObTransState &s);
  static int set_server_ip_by_shard_conn(ObTransState &s, dbconfig::ObShardConnector* shard_conn);
  static void handle_oceanbase_request(ObTransState &s);
  static void handle_ps_close_reset(ObTransState &s);
  static void handle_fetch_request(ObTransState &s);
  static void handle_target_db_not_allow(ObTransState &s);
  static void handle_request(ObTransState &s);
  static int build_normal_login_request(ObTransState &s, event::ObIOBufferReader *&reader,
                                        int64_t &request_len);
  static int build_server_request(ObTransState &s, event::ObIOBufferReader *&reader,
                                  int64_t &request_len);

  static int build_oceanbase_user_request(ObTransState &s, event::ObIOBufferReader *client_buffer_reader,
                                          event::ObIOBufferReader *&reader, int64_t &request_len);
  static int build_oceanbase_ob20_user_request(ObTransState &s, event::ObMIOBuffer &write_buffer,
                                               event::ObIOBufferReader &request_buffer_reader,
                                               int64_t client_request_len,
                                               uint8_t &compress_seq);
  static int build_user_request(ObTransState &s, event::ObIOBufferReader *client_buffer_reader,
                                event::ObIOBufferReader *&reader, int64_t &request_len);
  static int rewrite_stmt_id(ObTransState &s, event::ObIOBufferReader *client_buffer_reader);

  static void start_access_control(ObTransState &s);
  static void bad_request(ObTransState &s);
  static void lookup_skip_open_server(ObTransState &s);

  static bool need_disable_merge_status_check(ObTransState &s);
  static void acquire_cached_server_session(ObTransState &s);

  static void handle_error_jump(ObTransState &s);
  static void handle_internal_request(ObTransState &s);
  static void handle_binlog_request(ObTransState &s);
  static void handle_server_addr_lookup(ObTransState &s);
  static void get_region_name_and_server_info(ObTransState &s,
                                              common::ObIArray<obutils::ObServerStateSimpleInfo> &simple_servers_info,
                                              common::ObIArray<common::ObString> &region_names);
  static void handle_pl_lookup(ObTransState &s);
  static void handle_bl_lookup(ObTransState &s);
  static void modify_pl_lookup(ObTransState &s);
  static void handle_congestion_control_lookup(ObTransState &s);
  static void handle_congestion_entry_not_exist(ObTransState &s);
  static void handle_response(ObTransState &s);
  static void handle_transform_ready(ObTransState &s);
  static void handle_response_from_server(ObTransState &s);
  static void handle_oceanbase_retry_server_connection(ObTransState &s);
  static void handle_retry_server_connection(ObTransState &s);
  static ObSSRetryStatus retry_server_connection_not_open(ObTransState &s);
  static void handle_retry_last_time(ObTransState &s);
  static int attach_cached_dummy_entry(ObTransState &s, const ObAttachDummyEntryType type);
  static void handle_server_connection_break(ObTransState &s);
  static void handle_on_forward_server_response(ObTransState &s);
  static void handle_api_error_jump(ObTransState &s);
  static void handle_pl_update(ObTransState &s);

  static void handle_server_failed(ObTransState &s);
  static void handle_oceanbase_server_resp_error(ObTransState &s, obmysql::ObMySQLCmd request_cmd, obmysql::ObMySQLCmd current_cmd);
  static void handle_server_resp_error(ObTransState &s);

  static bool is_dbmesh_pool_user(ObTransState &s);
  static bool is_internal_request(ObTransState &s);
  static bool is_binlog_request(const ObTransState &s);
  static bool is_single_shard_db_table(ObTransState &s);
  static bool can_direct_ok_for_login(ObTransState &s);
  static bool is_in_trans(ObTransState &s);
  static bool is_user_trans_complete(ObTransState &s);
  static bool is_large_request(ObTransState &s) { return s.trans_info_.client_request_.is_large_request(); }
  static bool is_bad_route_request(ObTransState &s);
  static bool is_session_memory_overflow(ObTransState &s);
  static bool need_use_dup_replica(const common::ObConsistencyLevel level, ObTransState &s);
  static ObPLLookupState need_pl_lookup(ObTransState &s);
  static bool need_use_coordinator_session(ObTransState &s);
  static bool is_db_reset(ObTransState &s);
  static bool need_server_session_lookup(ObTransState &s);
  static int64_t get_max_connect_attempts_from_replica(const int64_t replica_size);
  static int64_t get_max_connect_attempts(ObTransState &s);

  static int build_table_entry_request_packet(ObTransState &s, event::ObIOBufferReader *&reader);
  static void handle_resultset_resp(ObTransState &s, bool &is_user_request);
  static int fetch_table_entry_info(ObTransState &s);

  static ObClientSessionInfo &get_client_session_info(ObTransState &s);
  static ObServerSessionInfo &get_server_session_info(ObTransState &s);
  static void consume_response_packet(ObTransState &s);
  static int build_no_privilege_message(ObTransState &trans_state, ObMysqlClientSession &client_session,
                                        const common::ObString &database);

  static void handle_handshake_pkt(ObTransState &s);
  static int handle_oceanbase_handshake_pkt(ObTransState &s, uint32_t conn_id,
                                             ObAddr &client_addr);
  static void handle_error_resp(ObTransState &s, bool &is_user_request);
  static void handle_ok_resp(ObTransState &s, bool &is_user_request);
  static void handle_db_reset(ObTransState &s);
  static void handle_first_response_packet(ObTransState &s);

  static ObHRTime get_based_hrtime(ObTransState &s);

  // Utility Methods
  static void setup_plugin_request_intercept(ObTransState &s);
  static void update_sql_cmd(ObTransState &s);
  // check if the global_vars_version is changed, called when receive saved login responce
  static int check_global_vars_version(ObTransState &s, const obmysql::ObStringKV &str_kv);
  static void handle_user_request_succ(ObTransState &s, bool &is_user_request);
  static int handle_user_set_request_succ(ObTransState &s);
  static int handle_normal_user_request_succ(ObTransState &s);
  static void handle_saved_login_succ(ObTransState &s);
  static int handle_oceanbase_saved_login_succ(ObTransState &s);
  static void handle_use_db_succ(ObTransState &s);
  static void handle_prepare_succ(ObTransState &s);
  static int do_handle_prepare_response(ObTransState &s, event::ObIOBufferReader *&buf_reader);
  static int do_handle_prepare_succ(ObTransState &s, uint32_t server_ps_id);
  static void handle_execute_succ(ObTransState &s);
  static int do_handle_execute_succ(ObTransState &s);
  static void handle_prepare_execute_succ(ObTransState &s);
  static void handle_xa_start_sync_succ(ObTransState &s);
  static int do_handle_prepare_execute_xa_succ(event::ObIOBufferReader &buf_reader);
  static void handle_text_ps_prepare_succ(ObTransState &s);
  static int handle_text_ps_drop_succ(ObTransState &s, bool &is_user_request);
  static int handle_change_user_request_succ(ObTransState &s);
  static int handle_reset_connection_request_succ(ObTransState &s);
  static int clear_session_related_source(ObTransState &s);
  static int handle_ps_reset_succ(ObTransState &s, bool &is_user_request);

  static int build_error_packet(ObTransState &s, ObMysqlClientSession *client_session);

  static void handle_new_config_acquired(ObTransState &s);

  // the stat functions
  static bool need_refresh_trace_stats(ObTransState &s);
  static void update_trace_stat(ObTransState &s);
  static void update_stat(ObTransState &s, const int64_t stat, int64_t increment);
  static void histogram_request_size(ObTransState &s, int64_t size);
  static void histogram_response_size(ObTransState &s, int64_t size);
  static void client_connection_speed(ObTransState &s, ObHRTime transfer_time, int64_t nbytes);
  static void server_connection_speed(ObTransState &s, ObHRTime transfer_time, int64_t nbytes);
  static void client_result_stat(ObTransState &s);
  static int add_new_stat_block(ObTransState &s);
  static void update_sync_session_stat(ObTransState &s);

  // get partition location info from sql parse result
  static int extract_partition_info(ObTransState &s);

  static bool is_in_auth_process(ObTransState &s);
  static bool is_in_internal_send_process(ObTransState &s);

  // get debug name
  static const char *get_action_name(ObMysqlTransact::ObStateMachineActionType e);
  static const char *get_server_state_name(ObMysqlTransact::ObServerStateType state);
  static const char *get_send_action_name(ObMysqlTransact::ObServerSendActionType type);
  static const char *get_server_resp_error_name(ObMysqlTransact::ObServerRespErrorType type);

  static void get_ip_port_from_addr(const int64_t addr, uint32_t &ip, uint16_t &port);
  static void check_safe_read_snapshot(ObTransState &s);

  static bool is_need_reroute(ObMysqlTransact::ObTransState &s);
  static bool is_need_use_sql_table_cache(ObMysqlTransact::ObTransState &s);
  static bool handle_set_trans_internal_routing(ObMysqlTransact::ObTransState &s, bool server_transaction_routing_flag);
  static bool is_sql_able_to_route_participant_in_trans(obutils::ObSqlParseResult& base_sql_parse_result, obmysql::ObMySQLCmd  sql_cmd);
  static bool is_trans_specified(ObTransState &s);
  static bool has_dependent_func(ObTransState &s);
  static void record_trans_state(ObTransState &s, bool is_in_trans);
  static bool is_addr_logonly(const net::ObIpEndpoint &addr, const ObTenantServer *ts);
  static void build_read_stale_param(const ObTransState &s, obutils::ObReadStaleParam &param);
  static void set_route_leader_replica(ObTransState &s, const ObProxyReplicaLocation *&replica);
};

inline bool ObMysqlTransact::is_need_use_sql_table_cache(ObMysqlTransact::ObTransState &s)
{
  obutils::ObSqlParseResult &parse_result = s.trans_info_.client_request_.get_parse_result();
  return s.mysql_config_params_->enable_index_route_
         && is_need_reroute(s)
         && !parse_result.is_write_stmt()
         && s.mysql_config_params_->is_standard_routing_mode();
}

typedef void (*TransactEntryFunc)(ObMysqlTransact::ObTransState &s);

inline bool ObMysqlTransact::is_user_trans_complete(ObTransState &s)
{
  return (!s.is_trans_first_request_
          && !s.is_auth_request_
          && !ObMysqlTransact::is_in_trans(s));
}

inline bool ObMysqlTransact::is_bad_route_request(ObTransState &s)
{
  bool bret = false;
  if (s.mysql_config_params_->enable_bad_route_reject_ && (s.is_hold_start_trans_ || s.is_hold_xa_start_)) {
    obutils::ObSqlParseResult &parse_result = s.trans_info_.client_request_.get_parse_result();
    const ObString &table_name = parse_result.get_table_name();
    bret = table_name.empty();
  }

  return bret;
}

inline bool ObMysqlTransact::need_server_session_lookup(ObTransState &s)
{
  return s.trans_info_.client_request_.is_kill_query();
}

inline bool ObMysqlTransact::is_db_reset(ObTransState &s)
{
  bool bret = false;
  // we treat this cases as db reset:
  // 1. drop db by this session, the resp is ok packet and contains db_reset in result
  // 2. drop db by other session, the resp is error packet and contains db_reset in result
  // we do NOT treat this cases as db reset:
  // 1. in auth request
  // 2. the db is empty originally, then we use db failed, observer will reset default database
  //    in this case, the db is not changed in fact
  if (s.trans_info_.server_response_.get_analyze_result().is_server_db_reset()
      && (!is_in_auth_process(s))
      && (!(obmysql::OB_MYSQL_COM_INIT_DB == s.trans_info_.client_request_.get_packet_meta().cmd_
            && s.trans_info_.server_response_.get_analyze_result().is_error_resp()))) {
    bret = true;
  }
  return bret;
}

inline int64_t ObMysqlTransact::get_max_connect_attempts_from_replica(const int64_t replica_size)
{
  return (replica_size * 2 + 1);
}


inline bool ObMysqlTransact::is_in_auth_process(ObTransState &s)
{
  // 1. send handshake
  // 2. send saved login
  // 3. send login
  return (SERVER_SEND_SAVED_LOGIN == s.current_.send_action_
          || SERVER_SEND_HANDSHAKE == s.current_.send_action_
          || SERVER_SEND_LOGIN == s.current_.send_action_);
}

inline bool ObMysqlTransact::is_in_internal_send_process(ObTransState &s)
{
  // all send actions is in internal send process except SERVER_SEND_REQUEST
  return (SERVER_SEND_NONE != s.current_.send_action_
          && SERVER_SEND_REQUEST != s.current_.send_action_);
}

inline void ObMysqlTransact::update_stat(
    ObTransState &s, const int64_t stat, const int64_t increment)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(s.current_stats_->next_insert_ >= ObStatBlock::STAT_BLOCK_ENTRIES)) {
    // This a rare operation and we want to avoid the
    // code bloat of inlining it everywhere so
    // it's a function call
    if (OB_FAIL(add_new_stat_block(s))) {
      PROXY_TXN_LOG(WARN, "failed to add new stat block", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    uint16_t *next_insert = &s.current_stats_->next_insert_;

    s.current_stats_->stats_[*next_insert].index_ = static_cast<uint16_t>(stat);
    s.current_stats_->stats_[*next_insert].increment_ = increment;
    ++(*next_insert);
  }
    // ignore error
}

inline int64_t milestone_diff(const ObHRTime start, const ObHRTime end)
{
  return (start > 0 && end > start) ? (end - start) : 0;
}

inline void ObMysqlTransact::ObPartitionLookupInfo::reset_consistency()
{
  route_.set_consistency_level(common::INVALID_CONSISTENCY);
}

inline void ObMysqlTransact::ObPartitionLookupInfo::reset_pl()
{
  route_.reset();
  lookup_success_ = false;
}

inline void ObMysqlTransact::ObPartitionLookupInfo::reset()
{
  route_.reset();
  lookup_success_ = false;
  pl_attempts_= 0;
  force_renew_state_ = NO_NEED_RENEW;
  cached_dummy_entry_renew_state_ = NO_NEED_RENEW;
  te_name_.reset();
  is_need_force_flush_ = false;
}

inline void ObMysqlTransact::update_sql_cmd(ObTransState &s)
{
  switch (s.current_.send_action_) {
    case SERVER_SEND_HANDSHAKE:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_HANDSHAKE;
      break;

    case SERVER_SEND_SSL_REQUEST:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_LOGIN;
      break;

    case SERVER_SEND_LOGIN:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_LOGIN;
      break;

    case SERVER_SEND_SAVED_LOGIN:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_LOGIN;
      break;

    case SERVER_SEND_USE_DATABASE:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_INIT_DB;
      break;

    case SERVER_SEND_ALL_SESSION_VARS:
    case SERVER_SEND_SESSION_VARS:
    case SERVER_SEND_SESSION_USER_VARS:
    case SERVER_SEND_START_TRANS:
    case SERVER_SEND_TEXT_PS_PREPARE:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_QUERY;
      break;
    
    case SERVER_SEND_XA_START:
      PROXY_TXN_LOG(DEBUG, "[ObMysqlTransact::update_sql_cmd] set s.trans_info_.sql_cmd OB_MYSQL_COM_STMT_PREPARE_EXECUTE for sync xa start");
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE;
      break;

    case SERVER_SEND_PREPARE:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_STMT_PREPARE;
      break;

    case SERVER_SEND_REQUEST:
      if (OB_UNLIKELY(s.is_auth_request_)) {
        s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_LOGIN;
      } else {
        s.trans_info_.sql_cmd_ = s.trans_info_.client_request_.get_packet_meta().cmd_;
      }
      break;

    case SERVER_SEND_NONE:
    default:
      PROXY_TXN_LOG(ERROR, "Unknown server send next action", K(s.current_.send_action_));
      break;
  }

  PROXY_TXN_LOG(DEBUG, "[ObMysqlTransact::update_sql_cmd]",
                "send_action", ObMysqlTransact::get_send_action_name(s.current_.send_action_),
                "sql_cmd", ObProxyParserUtils::get_sql_cmd_name(s.trans_info_.sql_cmd_));
}

inline common::ObString ObMysqlTransact::get_retry_status_string(const ObSSRetryStatus status)
{
  const char *str = "";
  switch (status) {
    case FOUND_EXISTING_ADDR:
      str = "FOUND_EXISTING_ADDR";
      break;
    case NOT_FOUND_EXISTING_ADDR:
      str = "NOT_FOUND_EXISTING_ADDR";
      break;
    case NO_NEED_RETRY:
      str = "NO_NEED_RETRY";
      break;
    default:
      str = "UNKNOWN";
  }
  return common::ObString::make_string(str);
}

inline common::ObString ObMysqlTransact::get_pl_lookup_state_string(const ObPLLookupState state)
{
  const char *str = "";
  switch (state) {
    case NEED_PL_LOOKUP:
      str = "NEED_PL_LOOKUP";
      break;
    case USE_LAST_SERVER_SESSION:
      str = "USE_LAST_SERVER_SESSION";
      break;
    case USE_COORDINATOR_SESSION:
      str = "USE_COORDINATOR_SESSION";
      break;
    default:
      str = "UNKNOWN";
  }
  return common::ObString::make_string(str);
}

inline void ObMysqlTransact::get_ip_port_from_addr(const int64_t addr, uint32_t &ip, uint16_t &port)
{
  static const int64_t IP_MASK = 0x00000000ffffffffL;
  static const int64_t PORT_MASK = 0xffffffff00000000L;
  ip = static_cast<uint32_t>(addr & IP_MASK);
  port = static_cast<uint16_t>((addr & PORT_MASK) >> 32);
}

inline bool ObMysqlTransact::is_addr_logonly(const net::ObIpEndpoint &addr, const ObTenantServer *ts)
{
  bool ret = false;
  if (OB_ISNULL(ts)) {
    ret = false;
  } else {
    int64_t cnt = ts->server_count_;
    ObAddr tmp_addr;
    tmp_addr.reset();
    tmp_addr.set_sockaddr(addr.sa_);
    for (int64_t i = 0; i < cnt; ++i) {
      if (tmp_addr == ts->get_replica_location(i)->server_
          && common::ObReplicaTypeCheck::is_logonly_replica(
              ts->get_replica_location(i)->get_replica_type())) {
        ret = true;
        break;
      }
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_TRANSACT_H
