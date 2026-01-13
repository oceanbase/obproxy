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

#include "lib/encrypt/ob_encrypted_helper.h"
#include "proxy/mysql/ob_mysql_client_session.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "proxy/mysql/ob_mysql_debug_names.h"
#include "proxy/api/ob_plugin.h"
#include "proxy/client/ob_client_vc.h"
#include "obutils/ob_resource_pool_processor.h"
#include "obutils/ob_config_server_processor.h"
#include "prometheus/ob_sql_prometheus.h"
#include "dbconfig/ob_proxy_pb_utils.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"
#include "omt/ob_conn_table_processor.h"
#include "omt/ob_white_list_table_processor.h"
#include "obutils/ob_connection_diagnosis_trace.h"
#include "omt/ob_ssl_config_table_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::prometheus;
using namespace oceanbase::obproxy::dbconfig;
using namespace oceanbase::obproxy::omt;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define STATE_ENTER(state_name, event, vio) do { \
  PROXY_CS_LOG(DEBUG, "ENTER STATE "#state_name"", "event", ObMysqlDebugNames::get_event_name(event), K_(cs_id)); \
} while(0)

#define MYSQL_SSN_INCREMENT_DYN_STAT(x)     \
  session_stats_.stats_[x] += 1;            \
  MYSQL_INCREMENT_DYN_STAT(x)

static uint32_t g_max_local_seq_v1 = 0;
static uint32_t g_max_local_seq_v2 = 0;

// v1 版本，thread_id 和 max_local_seq 共用 22 位
// v2 版本，thread_id 和 max_local_seq 共用 17 位
// 以 max_local_seq 为 16 位为例，cs_id 使用范围：0x0001 ~ 0xFFFF
uint32_t get_g_max_local_seq_v1() {
  if (OB_UNLIKELY(0 == g_max_local_seq_v1)) {
    ATOMIC_CAS(&g_max_local_seq_v1, 0, (1 << (22 - get_thread_id_bits())) - 1);
  }
  return g_max_local_seq_v1;
}

uint32_t get_g_max_local_seq_v2() {
  if (OB_UNLIKELY(0 == g_max_local_seq_v2)) {
    ATOMIC_CAS(&g_max_local_seq_v2, 0, (1 << (17 - get_thread_id_bits())) - 1);
  }
  return g_max_local_seq_v2;
}

enum ObClientSessionMagic
{
  MYSQL_CS_MAGIC_ALIVE = 0x0123F00D,
  MYSQL_CS_MAGIC_DEAD = 0xDEADF00D
};

// We have debugging list that we can use to find stuck
// client sessions
#ifdef USE_MYSQL_DEBUG_LISTS
DLL<ObMysqlClientSession> g_debug_cs_list;
ObMutex g_debug_cs_list_mutex;
#endif

ObMysqlClientSession::ObMysqlClientSession()
    : active_(false), test_server_addr_(),
      session_states_{}, compressed_seq_(0),
      cluster_resource_(NULL), dummy_entry_(NULL), is_need_update_dummy_entry_(false),
      dummy_ldc_(), dummy_entry_valid_time_ns_(0), server_state_version_(0),
      inner_request_param_(NULL), is_request_transferring_(false), timeout_event_(OB_TIMEOUT_UNKNOWN_EVENT),
      timeout_record_(0), conn_record_(), tcp_init_cwnd_set_(false), half_close_(false),
      conn_decrease_(false), conn_prometheus_decrease_(false), vip_connection_decrease_(false),
      magic_(MYSQL_CS_MAGIC_DEAD), create_thread_(NULL), is_local_connection_(false),
      client_vc_(NULL), in_list_stat_(LIST_INIT), current_tid_(-1),
      cs_id_(0), proxy_sessid_(0), last_ss_(NULL), cur_ss_(NULL), lii_ss_(NULL), second_last_ss_(NULL),
      lock_ss_(NULL), closed_key_ss_(NULL), sharding_txn_ss_addr_(), trans_coordinator_ss_addr_(), read_buffer_(NULL),
      buffer_reader_(NULL), mysql_sm_(NULL), read_state_(MCS_INIT), ka_vio_(NULL),
      last_ss_keep_alive_vio_(NULL), trace_stats_(NULL), select_plan_(NULL),
      ps_id_(0), cursor_id_(CURSOR_ID_START),
      cs_id_version_(CLIENT_SESSION_ID_V1), connected_time_(0)
{
  SET_HANDLER(&ObMysqlClientSession::main_handler);
  memset(&session_states_, 0, sizeof(session_states_));
  connection_pool_mode_ = ObConnectionPoolMode::NONE;
  set_need_send_trace_info(true);
  set_first_handle_ps_close_reset_request(true);
  set_first_send_ps_close_reset_request(true);
}

void ObMysqlClientSession::destroy()
{
  PROXY_CS_LOG(INFO, "client session destroy", K_(cs_id), K_(proxy_sessid), KP_(client_vc));

  if (OB_UNLIKELY(NULL != client_vc_)
      || OB_UNLIKELY(NULL != last_ss_)
      || OB_ISNULL(read_buffer_)) {
    PROXY_CS_LOG(WDIAG, "invalid client session", K(client_vc_), K(last_ss_), K(read_buffer_));
  }
  is_local_connection_ = false;

  if (NULL != dummy_entry_) {
    dummy_entry_->dec_ref();
    dummy_entry_ = NULL;
  }

  dummy_ldc_.reset();
  dummy_entry_valid_time_ns_ = 0;
  server_state_version_ = 0;
  inner_request_param_ = NULL;

  if (NULL != cluster_resource_) {
    PROXY_CS_LOG(DEBUG, "client session cluster resource will dec ref", K_(cluster_resource), KPC_(cluster_resource));
    get_global_resource_pool_processor().release_cluster_resource(cluster_resource_);
    cluster_resource_ = NULL;
  }

  magic_ = MYSQL_CS_MAGIC_DEAD;
  if (OB_LIKELY(NULL != read_buffer_)) {
    free_miobuffer(read_buffer_);
    read_buffer_ = NULL;
  }
  buffer_reader_ = NULL;

  set_sharding_select_log_plan(NULL);

#ifdef USE_MYSQL_DEBUG_LISTS
  mutex_acquire(&g_debug_cs_list_mutex);
  g_debug_cs_list.remove(this);
  mutex_release(&g_debug_cs_list_mutex);
#endif

  // 需要放在 session_info_ destroy 之前, 因为要用到里面的数据
  if (conn_prometheus_decrease_) {
    SESSION_PROMETHEUS_STAT(session_info_, PROMETHEUS_CURRENT_SESSION, true, -1);
    conn_prometheus_decrease_ = false;
  }
  if (vip_connection_decrease_) {
    decrease_used_connections();
    vip_connection_decrease_ = false;
  }

  test_server_addr_.reset();
  session_info_.destroy();
  // destroy ps_cache after destroy session, because client session info hold ps entry pointer
  if (NULL != trace_stats_) {
    trace_stats_->destory();
    trace_stats_ = NULL;
  }

  if (conn_decrease_) {
    MYSQL_DECREMENT_DYN_STAT(CURRENT_CLIENT_CONNECTIONS);
    conn_decrease_ = false;
  }

  set_need_send_trace_info(true);
  set_already_send_trace_info(false);
  set_first_handle_ps_close_reset_request(true);
  set_first_send_ps_close_reset_request(true);
  set_in_trans_for_close_request(false);
  set_last_request_in_trans(false);
  set_trans_internal_routing(false);
  set_need_return_second_last_server_session(false);
  set_first_dml_sql_got(false);
  set_proxy_enable_trans_internal_routing(false);
  compressed_seq_ = 0;
  lock_ss_ = NULL;
  closed_key_ss_ = NULL;
  sharding_txn_ss_addr_.reset();
  trans_coordinator_ss_addr_.reset();
  schema_key_.reset();
  ObProxyClientSession::cleanup();
  create_thread_ = NULL;
  set_using_ldg(false);
  set_using_service_name(false);
  set_standby_read_write_split(false);
  session_states_.is_in_trans_internal_routing_ = false;
  cs_id_version_ = CLIENT_SESSION_ID_V1;
  op_reclaim_free(this);
}

inline void ObMysqlClientSession::decrease_used_connections()
{
  ObString cluster_name;
  ObString tenant_name;
  ObString ip_name;

  if (is_need_convert_vip_to_tname() && is_vip_lookup_success()) {
    cluster_name = get_vip_cluster_name();
    tenant_name  = get_vip_tenant_name();
    session_info_.get_vip_addr_name(ip_name);
  } else {
    session_info_.get_cluster_name(cluster_name);
    session_info_.get_tenant_name(tenant_name);
  }

  get_global_conn_table_processor().dec_conn(
    cluster_name, tenant_name, ip_name);
}

int ObMysqlClientSession::ssn_hook_append(ObMysqlHookID id, ObContInternal *cont)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cont)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WDIAG, "invalid argument", K(cont), K(ret));
  } else if (OB_FAIL(ObProxyClientSession::ssn_hook_append(id, cont))) {
    PROXY_CS_LOG(WDIAG, "fail to append ssh hook", K(id), K(ret));
  } else if (NULL != mysql_sm_) {
    mysql_sm_->hooks_set_ = true;
  }
  return ret;
}

int ObMysqlClientSession::ssn_hook_prepend(ObMysqlHookID id, ObContInternal *cont)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cont)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WDIAG, "invalid argument", K(cont), K(ret));
  } else if (OB_FAIL(ObProxyClientSession::ssn_hook_prepend(id, cont))) {
    PROXY_CS_LOG(WDIAG, "fail to prepend ssn hook", K(id), K(ret));
  } else if (NULL != mysql_sm_) {
    mysql_sm_->hooks_set_ = true;
  }
  return ret;
}

int ObMysqlClientSession::new_transaction(const bool is_new_conn/* = false*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL != mysql_sm_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WDIAG, "sm should be null when start a new transaction", K(mysql_sm_), K(ret));
  } else {
    // Defensive programming, make sure nothing persists across
    // connection re-use
    half_close_ = false;

    read_state_ = MCS_ACTIVE_READER;
    if (OB_ISNULL(mysql_sm_ = ObMysqlSM::allocate())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PROXY_CS_LOG(EDIAG, "fail to alloc memory for sm", K(ret));
    } else if (OB_FAIL(mysql_sm_->init())) {
      PROXY_CS_LOG(WDIAG, "fail to init sm", K_(cs_id), K(ret));
    } else {
      PROXY_CS_LOG(INFO, "Starting new transaction using sm", K_(cs_id),
                   K(get_transact_count()), "sm_id", mysql_sm_->sm_id_);

      client_vc_->remove_from_keep_alive_lru();
      if (OB_FAIL(mysql_sm_->attach_client_session(*this, *buffer_reader_, is_new_conn))) {
        // if fail to attach client session, mysql sm will handle the failure itself, so we just print a warning log and
        // need not to return error ret to the caller
        PROXY_CS_LOG(WDIAG, "fail to attach client session", K_(cs_id), K(ret));
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_FAIL(ret)) {
    PROXY_CS_LOG(WDIAG, "fail to start new transaction, will close client session", K_(cs_id), K(ret));
    do_io_close();
  }
  return ret;
}

int ObMysqlClientSession::new_connection(
    ObNetVConnection *new_vc, ObMIOBuffer *iobuf,
    ObIOBufferReader *reader,
    ObShardConnector *shard_conn,
    ObShardProp *shard_prop)
{
  session_info_.set_shard_connector(shard_conn);
  session_info_.set_shard_prop(shard_prop);
  return new_connection(new_vc, iobuf, reader);
}

int ObMysqlClientSession::new_connection(
    ObNetVConnection *new_vc, ObMIOBuffer *iobuf,
    ObIOBufferReader *reader, ObClusterResource *cluster_resource)
{
  if (NULL != cluster_resource) {
    cluster_resource->inc_ref();
    cluster_resource_ = cluster_resource;
    cluster_resource = NULL;
  }
  return new_connection(new_vc, iobuf, reader);
}

int ObMysqlClientSession::new_connection(
    ObNetVConnection *new_vc, ObMIOBuffer *iobuf, ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(new_vc) || OB_UNLIKELY(NULL != client_vc_)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WDIAG, "invalid client connection", K(new_vc), K(client_vc_), K(ret));
  } else {
    create_thread_ = this_ethread();
    client_vc_ = new_vc;
    magic_ = MYSQL_CS_MAGIC_ALIVE;
    mutex_ = new_vc->mutex_;
    session_manager_.set_mutex(mutex_);
    session_manager_sharding_.set_mutex(mutex_);
    set_cs_id_version(static_cast<ObClientSessionIDVersion>(get_global_proxy_config().client_session_id_version.get_value()));
    set_connected_time(ObTimeUtility::current_time());
    MUTEX_TRY_LOCK(lock, mutex_, this_ethread());
    if (OB_LIKELY(lock.is_locked())) {
      current_tid_ = GETTID();
      hooks_on_ = true;

      MYSQL_INCREMENT_DYN_STAT(CURRENT_CLIENT_CONNECTIONS);
      conn_decrease_ = true;
      if (is_proxy_mysql_client()) {
        MYSQL_INCREMENT_DYN_STAT(TOTAL_INTERNAL_CLIENT_CONNECTIONS);
      } else {
        MYSQL_INCREMENT_DYN_STAT(TOTAL_CLIENT_CONNECTIONS);
      }

      switch (new_vc->get_remote_addr().sa_family) {
        case AF_INET:
          MYSQL_INCREMENT_DYN_STAT(TOTAL_CLIENT_CONNECTIONS_IPV4);
          break;
        case AF_INET6:
          MYSQL_INCREMENT_DYN_STAT(TOTAL_CLIENT_CONNECTIONS_IPV6);
          break;
        default:
          break;
      }

#ifdef USE_MYSQL_DEBUG_LISTS
      if (OB_SUCCESS == mutex_acquire(&g_debug_cs_list_mutex)) {
        g_debug_cs_list.push(this);
        if (OB_SUCCESS != mutex_release(&g_debug_cs_list_mutex)) {
          PROXY_CS_LOG(EDIAG, "fail to release mutex", K_(cs_id));
        }
      }
#endif

      if (NULL != iobuf) {
        read_buffer_ = iobuf;
      } else if (OB_ISNULL(read_buffer_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_CS_LOG(EDIAG, "fail to alloc memory for read_buffer", K(ret));
      }

      if (OB_SUCC(ret)) {
        if (NULL != reader) {
          buffer_reader_ = reader;
        } else if (OB_ISNULL(buffer_reader_ = read_buffer_->alloc_reader())) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_CS_LOG(EDIAG, "fail to alloc buffer reader", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        /**
         * we cache the request in the read io buffer, so the water mark of
         * read io buffer must be larger than the reqeust packet size. we set
         * MYSQL_NET_META_LENGTH as the default water mark, when we read the
         * header of request, we reset the water mark.
         */
        read_buffer_->water_mark_ = MYSQL_NET_META_LENGTH;
        // start listen event on client vc
        if (OB_ISNULL(ka_vio_ = client_vc_->do_io_read(this, INT64_MAX, read_buffer_))) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_CS_LOG(WDIAG, "fail to start listen event on client vc, ka_vio is null", K(ret));
        } else if (OB_FAIL(add_to_list())) {
          PROXY_CS_LOG(WDIAG, "fail to add cs to list", K_(cs_id), K(ret));
        } else if (OB_FAIL(session_info_.init())) {
          PROXY_CS_LOG(WDIAG, "fail to init session_info", K_(cs_id), K(ret));
        } else if (OB_FAIL(get_vip_addr())) {
          PROXY_CS_LOG(WDIAG, "get vip addr failed", K(ret));
        } else {
          const ObAddr &client_addr = get_real_client_addr();
          session_info_.set_client_host(client_addr);
          set_local_connection();
          PROXY_CS_LOG(INFO, "client session born", K_(cs_id), K_(proxy_sessid), K_(is_local_connection), K_(client_vc),
                       "client_fd", client_vc_->get_conn_fd(), K(client_addr), "is_proxy_client", is_proxy_mysql_client());

          // 1. first convert vip to tenant info, if needed.
          // 这里需要注意，在阿里云上这里可以获取成功，但 aws 走了
          // ppv2接口所以预期获取失败，处理主要为了兼容 SSL 的能力集合，
          // aws 和阿里云行为有差异
          if (is_need_convert_vip_to_tname()) {
            if (OB_FAIL(fetch_tenant_by_vip())) {
              PROXY_CS_LOG(WDIAG, "fail to fetch tenant by vip", K(ret));
              ret = OB_SUCCESS;
            } else if (is_vip_lookup_success()) {
              ObString user_name;
              if (!get_global_white_list_table_processor().can_ip_pass(
                ct_info_.vip_tenant_.cluster_name_, ct_info_.vip_tenant_.tenant_name_,
                user_name, client_vc_->get_real_client_addr())) {
                ret = OB_ERR_CAN_NOT_PASS_WHITELIST;
                PROXY_CS_LOG(DEBUG, "can not pass white_list", K(ct_info_.vip_tenant_.cluster_name_),
                             K(ct_info_.vip_tenant_.tenant_name_), K(client_addr), K(ret));
                OBPROXY_ERROR_LOG(ERROR, "can not pass white_list of obproxy", "cluster_name",
                                  ct_info_.vip_tenant_.cluster_name_, "tenant_name", ct_info_.vip_tenant_.tenant_name_,
                                  K(user_name), K(client_addr), K_(cs_id), K(ret));
                OBPROXY_DIAGNOSIS_LOG(WDIAG, "[LOGIN]", "trace_type", "PROXY_INTERNAL_TRACE",
                                      "error_msg", "obproxy disconnect because can not pass white list",
                                      "cluster_name", ct_info_.vip_tenant_.cluster_name_, "tenant_name",
                                      ct_info_.vip_tenant_.tenant_name_, K(client_addr), K_(cs_id),
                                      "login_result", "failed");
              }
            }
          }
#ifdef BUILD_OPENSOURCE
          // set weak read flag global
          if (obutils::get_global_proxy_config().enable_force_request_follower) {
            session_info_.set_is_request_follower_user(true);
          }
#endif
          // 2. handle_new_connection no matter convert vip to tenant result.
          if (OB_SUCC(ret)) {
            handle_new_connection();
          }
        }
      } // end if (OB_SUCC(ret))
    } else {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CS_LOG(WDIAG, "fail to try lock thread mutex, will close connection", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    PROXY_CS_LOG(WDIAG, "fail to do new connection, do_io_close itself", K_(cs_id), K(ret));
    do_io_close();
  }
  return ret;
}

int ObMysqlClientSession::fetch_tenant_by_vip()
{
  int ret = OB_SUCCESS;
  ct_info_.lookup_success_ = false;
  ObVipAddr addr = ct_info_.vip_tenant_.vip_addr_;
  ObConfigItem tenant_item, cluster_item;
  bool found = false;
  if (OB_FAIL(get_global_config_processor().get_proxy_config_with_level(
    addr, "", "", "proxy_tenant_name", tenant_item, "LEVEL_VIP", found))) {
    PROXY_CS_LOG(WDIAG, "get proxy tenant name config failed", K(addr), K(ret));
  }

  if (OB_SUCC(ret) && found) {
    if (OB_FAIL(get_global_config_processor().get_proxy_config_with_level(
      addr, "", "", "rootservice_cluster_name", cluster_item, "LEVEL_VIP", found))) {
      PROXY_CS_LOG(WDIAG, "get cluster name config failed", K(addr), K(ret));
    }
  }

  if (OB_SUCC(ret) && found) {
    if (OB_FAIL(ct_info_.vip_tenant_.set_tenant_cluster(tenant_item.str(), cluster_item.str()))) {
      PROXY_CS_LOG(WDIAG, "set tenant and cluster name failed", K(tenant_item), K(cluster_item), K(ret));
    } else {
      session_info_.set_vip_addr_name(addr.addr_);
      ct_info_.lookup_success_ = true;
      PROXY_CS_LOG(DEBUG, "succ to get conn info", "vip_tenant", ct_info_.vip_tenant_);
    }
  }
  return ret;
}

int ObMysqlClientSession::get_vip_addr()
{
  int ret = OB_SUCCESS;
  int64_t vid;
  vid = static_cast<int64_t>(client_vc_->get_virtual_vid());
  ct_info_.vip_tenant_.vip_addr_.set(client_vc_->get_virtual_addr(), vid);

  // TODO oushen, get client ip, slb ip from kernal

  return ret;
}

void ObMysqlClientSession::handle_new_connection()
{
  // Use a local pointer to the mutex as when we return from do_api_callout,
  // the ClientSession may have already been deallocated.
  ObEThread &ethread = self_ethread();
  ObPtr<ObProxyMutex> lmutex = mutex_;
  int ret = OB_SUCCESS;
  {
    MUTEX_LOCK(lock, lmutex, &ethread);
    if (OB_FAIL(do_api_callout(OB_MYSQL_SSN_START_HOOK))) {
      PROXY_CS_LOG(WDIAG, "fail to start hook, will close client session", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    do_io_close();
  }
}


int ObMysqlClientSession::acquire_client_session_id(const ObClientSessionIDVersion version)
{
  int ret = OB_SUCCESS;
  switch (version)
  {
  case CLIENT_SESSION_ID_V1:
    if (OB_FAIL(acquire_client_session_id_v1())) {
      PROXY_CS_LOG(WDIAG, "fail to acquire normal client session id", K(ret));
    }
    break;
  case CLIENT_SESSION_ID_V2:
    if (OB_FAIL(acquire_client_session_id_v2())) {
      PROXY_CS_LOG(WDIAG, "fail to acquire unique client session id", K(ret));
    }
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WDIAG, "unexpected client session version", K(ret), K(version));
    break;
  }
  return ret;
}


//if proxy use client service mode, conn_id is equal to cs_id
//otherwise, conn_id extract from observer's handshake packet
//
//connection id from obproxy
//|----1----|-----8-----|------1------|----------22-----------|
//|  MARKS  |  PROXY_ID | UPGRADE_VER |---1~N-----|----22-N---|
//|    0    |   1~255   |     0/1     | THREAD_ID | LOCAL_SEQ |
// N: bits of thread id hold
//
//connection id extract from observer's handshake packet
//|----1----|-----15-----|----16-----|
//|    1    |  SERVER_ID | LOCAL_SEQ |
// original client session id , depends on 8bit proxy_id, only sync with client
int ObMysqlClientSession::acquire_client_session_id_v1()
{
  static __thread uint32_t next_cs_id = 0;
  static __thread uint32_t thread_init_cs_id = 0;
  uint32_t max_local_seq = get_g_max_local_seq_v1();

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == next_cs_id) && OB_FAIL(get_thread_init_cs_id(CLIENT_SESSION_ID_V1, thread_init_cs_id, max_local_seq))) {
    PROXY_CS_LOG(WDIAG, "fail to get thread init cs id", K(next_cs_id), K(ret));
  }

  if (OB_SUCC(ret)) {
    uint32_t cs_id = ++next_cs_id;
    if ((cs_id & max_local_seq) == 0) {
      cs_id ++;
    }
    cs_id &= max_local_seq;         // set local seq
    cs_id |= thread_init_cs_id;     // set full cs id

    cs_id_ = cs_id;
  }
  return ret;
}

// connection id from obproxy, allocated from thread
//|----1----|-----13------|------1------|----------17-----------|
//|  FLAG   |   PROXY_ID  | UPGRADE_VER |---1~N-----|----17-N---|
//|    0    |    0-8191   |     0/1     | THREAD_ID | LOCAL_SEQ |
//|---------------------thread init cs id-----------------------|
//
// global unique client session id in multi obproxy, depends on 13bits proxy_id, sync with client and observer
int ObMysqlClientSession::acquire_client_session_id_v2()
{
  static __thread uint32_t next_cs_id = 0;
  static __thread uint32_t thread_init_cs_id = 0;
  static __thread uint32_t proxy_id = 0;
  uint32_t max_local_seq = get_g_max_local_seq_v2();

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == next_cs_id || proxy_id != static_cast<uint32_t>(get_global_proxy_config().proxy_id))) {
    proxy_id = static_cast<uint32_t>(get_global_proxy_config().proxy_id);
    if(OB_FAIL(get_thread_init_cs_id(CLIENT_SESSION_ID_V2, thread_init_cs_id, max_local_seq))) {
      PROXY_CS_LOG(WDIAG, "fail to get thread init cs_id", K(next_cs_id), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    uint32_t cs_id = ++next_cs_id;
    if ((cs_id & max_local_seq) == 0) {
      cs_id ++;
    }
    cs_id &= max_local_seq;
    cs_id |= thread_init_cs_id;
    cs_id_ = cs_id;
  }
  return ret;
}

int64_t ObMysqlClientSession::get_max_local_seq(const ObClientSessionIDVersion version) const
{
  int64_t max_local_seq = 0;
  switch (version) {
    case CLIENT_SESSION_ID_V1: {
      max_local_seq = get_g_max_local_seq_v1();
      break;
    }
    case CLIENT_SESSION_ID_V2: {
      max_local_seq = get_g_max_local_seq_v2();
      break;
    }
    default: {
      LOG_EDIAG("unexpected client session version", K(version));
      break;
    }
  }

  return max_local_seq;
}

int ObMysqlClientSession::get_thread_init_cs_id(const ObClientSessionIDVersion version, uint32_t &thread_init_cs_id,
    uint32_t &max_local_seq, const int64_t thread_id/*-1*/)
{
  int ret = OB_SUCCESS;
  uint32_t tmp_thread_id = 0;
  if (thread_id < 0 || thread_id >= g_event_processor.thread_count_for_type_[ET_NET]) {// use curr ethread
    ObEThread &ethread = self_ethread();
    tmp_thread_id = static_cast<uint32_t>(ethread.id_);
  } else {// use assigned ethread
    tmp_thread_id = static_cast<uint32_t>(thread_id);
  }
  const uint32_t upgrade_ver  = static_cast<uint32_t>(0x1 & get_global_hot_upgrade_info().upgrade_version_); //only use the tail bits
  const uint32_t proxy_id = static_cast<uint32_t>(get_global_proxy_config().proxy_id);

  switch (version) {
    case CLIENT_SESSION_ID_V1: {
      const uint32_t proxy_head_bits      = 9;//MARKS + PROXY_ID
      const uint32_t upgrade_ver_bits     = 1;
      const uint32_t thread_id_bits       = get_thread_id_bits();

      const uint32_t proxy_id_offset      = 32 - proxy_head_bits;
      const uint32_t upgrade_ver_offset   = proxy_id_offset - upgrade_ver_bits;
      const uint32_t thread_id_offset     = upgrade_ver_offset - thread_id_bits;

      const uint32_t upgrade_ver   = static_cast<uint32_t>(0x1 & get_global_hot_upgrade_info().upgrade_version_); //only use the tail bits

      max_local_seq = get_g_max_local_seq_v1();
      thread_init_cs_id = 0;

      if (OB_SUCC(ret)) {
        thread_init_cs_id |= (proxy_id << proxy_id_offset);        // set proxy id
        thread_init_cs_id |= (upgrade_ver << upgrade_ver_offset);  // set upgrade version
        thread_init_cs_id |= (tmp_thread_id << thread_id_offset);  // set thread id
      }
      break;
    }
    case CLIENT_SESSION_ID_V2: {
      // calc bits
      const uint32_t proxy_id_bits =  14; // MARKS + PROXY_ID
      const uint32_t upgrade_ver_bits = 1;
      const uint32_t thread_id_bits = get_thread_id_bits();

      // calc offset
      const uint32_t proxy_id_offset = 32 - proxy_id_bits;
      const uint32_t upgrade_ver_offset = proxy_id_offset - upgrade_ver_bits;
      const uint32_t thread_id_offset = upgrade_ver_offset - thread_id_bits;

      // set flag/proxy_id/upgrade_ver/thread_id_offset for cs_id

      thread_init_cs_id = 0;
      max_local_seq = get_g_max_local_seq_v2();

      thread_init_cs_id |= (proxy_id << proxy_id_offset);
      thread_init_cs_id |= (upgrade_ver << upgrade_ver_offset);
      thread_init_cs_id |= (tmp_thread_id << thread_id_offset);
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("unexpected client session version", K(ret), K(version));
      break;
  }

  return ret;
}

int ObMysqlClientSession::async_disconnect_by_internal_reason(int64_t async_disconnect_code)
{
  int ret = OB_SUCCESS;

  ObUnixNetVConnection* client_vc = NULL;
  if (OB_ISNULL(mysql_sm_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(EDIAG, "fail to find mysql_sm, maybe client session is released", K(this), K(ret));
  } else if (OB_FALSE_IT(mysql_sm_->set_kill_this_after_cmd_done(async_disconnect_code))) {
    // nothing
  } else if (OB_UNLIKELY(is_request_transferring_)) {
    PROXY_CS_LOG(WDIAG, "mysql sm is running, will close connection after cmd done");
  } else if (OB_ISNULL(client_vc = static_cast<ObUnixNetVConnection*>(client_vc_))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(EDIAG, "invalid client vc", K(client_vc_), K(ret));
  } else {
    PROXY_CS_LOG(WDIAG, "mysql sm is idle, stop it and close connection");
    ObVIO* client_vio = &(client_vc->read_.vio_);
    mysql_sm_->handle_event(VC_EVENT_EOS, client_vio);
  }

  return ret;
}

int ObMysqlClientSession::add_to_list()
{
  int ret = OB_SUCCESS;
  ObMysqlClientSessionMap &cs_map = get_client_session_map(*mutex_->thread_holding_);
  ObClientSessionIDList &cs_id_list = get_client_session_id_list(*mutex_->thread_holding_);
  // to constrain cycle upper bound, avoid potential dead-cyle
  const int64_t MAX_TRY_TIMES = get_max_local_seq(cs_id_version_);
  if (OB_FAIL(acquire_client_session_id(cs_id_version_))) {
    PROXY_CS_LOG(WDIAG, "fail to acquire client session_id", K_(cs_id), K(ret));
  } else if (OB_UNLIKELY(LIST_ADDED == in_list_stat_)) {
    ret = OB_ENTRY_EXIST;
    PROXY_CS_LOG(WDIAG, "cs had already in the list, it should not happened", K_(cs_id), K(ret));
  } else if (OB_ISNULL(mutex_->thread_holding_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WDIAG, "mutex_->thread_holding_ is null, it should not happened", K_(cs_id), K(ret));
  } else if (is_proxy_mysql_client()) {
    //if it is is proxy mysql client, we do follow things:
    //1. do not add to list
    //2. set cs_id top flag to 1
    //3. use observer session id in handshake as conn id
    //4. do not saved login for inner client vc
    if (cs_id_version_ == CLIENT_SESSION_ID_V1) {
      // CLIENT_SESSION_ID_V1, set top flag to avoid duplicate cs_id
      cs_id_ |= 0x80000000;// set top bit to 1
    } else if (cs_id_version_ == CLIENT_SESSION_ID_V2) {
      // CLIENT_SESSION_ID_V2, record cs id to avoid duplicate cs_id
      bool is_exist = false;
      bool exist_available_cs_id = (get_max_local_seq(cs_id_version_) - cs_id_list.size()) > 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < MAX_TRY_TIMES && exist_available_cs_id; ++i) {
        if (OB_FAIL(cs_id_list.is_cs_id_exist(cs_id_, is_exist))) {
          PROXY_CS_LOG(WDIAG, "fail to check if cs_id exist", K(cs_id_), K(ret));
          break;
        } else if (!is_exist) {
          if (OB_FAIL(cs_id_list.record_cs_id(cs_id_))) {
            PROXY_CS_LOG(WDIAG, "fail to record cs id for proxy mysql client", K(ret));
          }
          break;
        } else if (OB_FAIL(acquire_client_session_id(cs_id_version_))) {
          PROXY_CS_LOG(WDIAG, "fail to acquire client session_id", K(ret));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (is_exist) {
          PROXY_CS_LOG(EDIAG, "there is no enough cs id, close this connect", K_(cs_id), K(is_proxy_mysql_client()), K(client_vc_), K(ret));
          ret = OB_SESSION_ENTRY_EXIST;
          OBPROXY_ERROR_LOG(ERROR, "there is no enough cs id, close this connect", "is_proxy_mysql_client", is_proxy_mysql_client(),
                            "cs_id_version", cs_id_version_, "cluster_name", ct_info_.vip_tenant_.cluster_name_,
                            "tenant_name", ct_info_.vip_tenant_.tenant_name_, "client_addr", get_real_client_addr(), K_(cs_id), K(ret));
          OBPROXY_DIAGNOSIS_LOG(INFO, "[LOGIN]", "trace_type", "PROXY_INTERNAL_TRACE",
                                "error_msg", "obproxy disconnect because the cs_id has been used up", K_(cs_id),
                                "is_proxy_mysql_client", is_proxy_mysql_client(),
                                "cluster_name", ct_info_.vip_tenant_.cluster_name_, "tenant_name",
                                ct_info_.vip_tenant_.tenant_name_, "client_addr", get_real_client_addr(),
                                "cs_map size", cs_map.size(), "cs id list size", cs_id_list.size(), K(MAX_TRY_TIMES),
                                "login_result", "failed");
          cs_id_ = 0;
        } else {
          PROXY_CS_LOG(DEBUG, "acquire cs id succ", K_(cs_id), K(is_proxy_mysql_client()), K(MAX_TRY_TIMES));
        }
      }
    }
  } else {
    bool exist_available_cs_id = (get_max_local_seq(cs_id_version_) - cs_id_list.size()) > 0;
    for (int64_t i = 0; OB_SUCC(ret) && LIST_ADDED != in_list_stat_
                        && i < MAX_TRY_TIMES && exist_available_cs_id; ++i) {
      int tmp_ret = OB_SUCCESS;
      bool is_exist = false;
      if (OB_FAIL(cs_id_list.is_cs_id_exist(cs_id_, is_exist))) {
        PROXY_CS_LOG(WDIAG, "fail to check if cs_id exist", K_(cs_id), K(ret));
      } else if (!is_exist) {
        if (OB_SUCCESS != (tmp_ret = cs_map.set(*this))) {
          if (OB_LIKELY(OB_HASH_EXIST == tmp_ret)) {
            PROXY_CS_LOG(INFO, "repeat cs id, retry to acquire another one", K_(cs_id), K(client_vc_), K(ret));
            if (OB_FAIL(acquire_client_session_id(cs_id_version_))) {
              PROXY_CS_LOG(WDIAG, "fail to acquire client session_id", K_(cs_id), K(ret));
            }
          } else {
            ret = tmp_ret;
            PROXY_CS_LOG(WDIAG, "fail to set cs into cs_map", K_(cs_id), K(client_vc_), K(ret));
          }
        } else {
          if (OB_FAIL(cs_id_list.record_cs_id(cs_id_))) {
            PROXY_CS_LOG(WDIAG, "fail to record cs_id and thread_id map", K_(cs_id), "thread_id", self_ethread().id_, K(ret));
          } else {
            in_list_stat_ = LIST_ADDED;
          }
        }
      } else if (OB_FAIL(acquire_client_session_id(cs_id_version_))) {
        PROXY_CS_LOG(WDIAG, "fail to acquire client session_id", K_(cs_id), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (LIST_ADDED != in_list_stat_) {
        PROXY_CS_LOG(EDIAG, "there is no enough cs id, close this connect", K_(cs_id), K(is_proxy_mysql_client()), K(client_vc_), K(MAX_TRY_TIMES), "cs id list size", cs_id_list.size(), K(ret));
        ret = OB_SESSION_ENTRY_EXIST;
        OBPROXY_ERROR_LOG(ERROR, "there is no enough cs id, close this connect", "is_proxy_mysql_client", is_proxy_mysql_client(),
                          "cs_id_version", cs_id_version_, "cluster_name", ct_info_.vip_tenant_.cluster_name_,
                          "tenant_name", ct_info_.vip_tenant_.tenant_name_, "client_addr", get_real_client_addr(), K_(cs_id), K(ret));
        OBPROXY_DIAGNOSIS_LOG(INFO, "[LOGIN]", "trace_type", "PROXY_INTERNAL_TRACE",
                              "error_msg", "obproxy disconnect because the cs_id has been used up", K_(cs_id),
                              "is_proxy_mysql_client", is_proxy_mysql_client(),
                              "cluster_name", ct_info_.vip_tenant_.cluster_name_, "tenant_name",
                              ct_info_.vip_tenant_.tenant_name_, "client_addr", get_real_client_addr(),
                              "cs_map size", cs_map.size(), "cs id list size", cs_id_list.size(), K(MAX_TRY_TIMES),
                              "login_result", "failed");
        cs_id_ = 0;
      } else {
        PROXY_CS_LOG(DEBUG, "acquire cs id succ", K_(cs_id), K(is_proxy_mysql_client()), K(MAX_TRY_TIMES));
      }
    }
  }
  return ret;
}

int ObMysqlClientSession::create_scramble()
{
  int ret = OB_SUCCESS;
  ObMysqlRandom &random = get_random_seed(*mutex_->thread_holding_);
  ret = session_info_.create_scramble(random);
  PROXY_CS_LOG(DEBUG, "create scramble", K_(cs_id), "scramble_string", session_info_.get_scramble_string(), K(ret));
  return ret;
}

uint64_t ObMysqlClientSession::get_next_proxy_sessid()
{
  // TODO:考虑是否支持IPv6
  static uint64_t next_proxy_sessid = 1;
  const ObAddr &addr = get_global_hot_upgrade_info().local_addr_;
  int64_t ipv4 = static_cast<int64_t>(addr.get_ipv4());
  int64_t port = static_cast<int64_t>(addr.get_port());
  port &= 0xFFFF;

  uint64_t ret = ATOMIC_FAA((&next_proxy_sessid), 1);
  ret &= 0xFFFF;
  ret |= (port << 16);
  ret |= (ipv4 << 32);
  return ret;
}

ObVIO *ObMysqlClientSession::do_io_read(
    ObContinuation *c, const int64_t nbytes, ObMIOBuffer *buf)
{
  return client_vc_->do_io_read(c, nbytes, buf);
}

common::ObString &ObMysqlClientSession::get_login_packet()
{
  return session_info_.get_login_req().get_auth_request();
}

ObVIO *ObMysqlClientSession::do_io_write(
    ObContinuation *c, const int64_t nbytes, ObIOBufferReader *buf)
{
  // conditionally set the tcp initial congestion window
  // before our first write.
  if (!tcp_init_cwnd_set_) {
    tcp_init_cwnd_set_ = true;
    set_tcp_init_cwnd();
  }
  return client_vc_->do_io_write(c, nbytes, buf);
}

void ObMysqlClientSession::set_tcp_init_cwnd()
{
  int32_t desired_tcp_init_cwnd = static_cast<int32_t>(mysql_sm_->trans_state_.mysql_config_params_->server_tcp_init_cwnd_);

  if (0 != desired_tcp_init_cwnd) {
    if (0 != client_vc_->set_tcp_init_cwnd(desired_tcp_init_cwnd)) {
      PROXY_CS_LOG(WDIAG, "set_tcp_init_cwnd failed", K(desired_tcp_init_cwnd));
    }
  }
}

void ObMysqlClientSession::do_io_shutdown(const ShutdownHowToType howto)
{
  client_vc_->do_io_shutdown(howto);
}

int ObMysqlClientSession::setup_reset_conn_buffer(ObMysqlSM *sm)
{
  int ret = OB_SUCCESS;
  if (sm != NULL) {
    if (OB_FAIL(session_manager_.setup_reset_conn_buffer(sm))) {
      PROXY_SS_LOG(WDIAG, "fail to setup_reset_conn_buffer", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    PROXY_SS_LOG(WDIAG, "unexpected null sm", K(ret));
  }

  return ret;
}

void ObMysqlClientSession::do_io_close(const int alerrno)
{
  if (IS_DEBUG_ENABLED()) {
    PROXY_SS_LOG(DEBUG, "client session do_io_close", K(*this), KP(client_vc_), KP(this));
  } else {
    PROXY_SS_LOG(INFO, "client session do_io_close", KP(client_vc_), KP(this),
    "is_proxy_client", session_states_.is_proxy_mysql_client_,
    "cs_id", cs_id_,
    "proxy_sessid", proxy_sessid_,
    "cluster", session_info_.get_priv_info().cluster_name_,
    "tenant", session_info_.get_priv_info().tenant_name_,
    "user", session_info_.get_priv_info().user_name_,
    "mysql_capability", session_info_.get_login_req().get_hsr_result().response_.get_capability_flags().capability_,
    "last_server_sessid", session_info_.get_last_server_sess_id(),
    "last_server_addr", session_info_.get_last_server_addr(),
    "dummy_ldc", dummy_ldc_);
  }
  int ret = OB_SUCCESS;
  // Prevent double closing
  if (MCS_CLOSED != read_state_) {
    if (NULL != mysql_sm_) {
      mysql_sm_->set_detect_server_info(mysql_sm_->trans_state_.server_info_.addr_, -1, 0);
      if (mysql_sm_->connection_diagnosis_trace_ != NULL
          && ObConnectionDiagnosisTrace::is_enable_diagnosis_log(get_global_proxy_config().connection_diagnosis_option)) {
        // in case of client session killed before sm killed, build basic connection diagnosis_info here
        mysql_sm_->build_basic_connection_diagnosis_info();
      }
    }
    if (MCS_ACTIVE_READER == read_state_) {
      if (LIST_ADDED == in_list_stat_ || is_proxy_mysql_client()) {
        MYSQL_DECREMENT_DYN_STAT(CURRENT_CLIENT_TRANSACTIONS);
      }
      if (active_) {
        active_ = false;
        MYSQL_DECREMENT_DYN_STAT(CURRENT_ACTIVE_CLIENT_CONNECTIONS);
      }
    }

    // If we have an attached server session, release
    // it back to our shared pool
    if (OB_FAIL(release_last_server_session())) {
      PROXY_CS_LOG(WDIAG, "fail to release last server session", K(ret));
    } else if (OB_FAIL(release_second_last_server_session())) {
      PROXY_CS_LOG(WDIAG, "fail to release before last server session", K(ret));
    }

    // if pool session then release all session to global session pool
    // else close all session
    session_manager_.purge_keepalives();
    // close all server sessions in server session manager
    session_manager_sharding_.purge_keepalives();

    if (half_close_ && NULL != mysql_sm_) {
      read_state_ = MCS_HALF_CLOSED;
      PROXY_CS_LOG(DEBUG, "session half close", K_(cs_id));

      // We want the client to know that that we're finished writing. The
      // write shutdown accomplishes this. Unfortunately, the IO Core
      // semantics don't stop us from getting events on the write side of
      // the connection like timeouts so we need to zero out the write of
      // the continuation with the do_io_write() call
      client_vc_->do_io_shutdown(IO_SHUTDOWN_WRITE);

      if (OB_ISNULL(ka_vio_ = client_vc_->do_io_read(this, INT64_MAX, read_buffer_))) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CS_LOG(WDIAG, "ka_vio_ is null", K(ret));
      }

      // Drain any data read.
      // If the buffer is full and the client writes again, we will not receive a
      // READ_READY event.
      if (OB_FAIL(buffer_reader_->consume(buffer_reader_->read_avail()))) {
        PROXY_CS_LOG(WDIAG, "fail to consume ", K(ret));
      }
    }

    if (!half_close_ && NULL != client_vc_) {
      client_vc_->do_io_close(alerrno);
      PROXY_CS_LOG(DEBUG, "session closed, session stats", K(session_stats_), K_(cs_id));
      client_vc_ = NULL;
      MYSQL_SUM_DYN_STAT(TRANSACTIONS_PER_CLIENT_CON, get_transact_count());
      MYSQL_DECREMENT_DYN_STAT(CURRENT_CLIENT_CONNECTIONS);
      conn_decrease_ = false;
    }

    if (!half_close_ && LIST_ADDED == in_list_stat_) {
      // proxy_mysql_client is not in map, no need to erase
      if (this_ethread() != create_thread_) {
        PROXY_CS_LOG(DEBUG, "current thread is not create thread, should schedule",
                     "current thread", this_ethread(), "create thread", create_thread_, K_(cs_id));
        CLIENT_SESSION_SET_DEFAULT_HANDLER(&ObMysqlClientSession::handle_other_event);
        if (OB_ISNULL(create_thread_->schedule_imm(this, CLIENT_SESSION_ERASE_FROM_MAP_EVENT))) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_CS_LOG(WDIAG, "fail to schedule switch thread", K_(cs_id), K(ret));
        }
      } else {
        ObMysqlClientSessionMap &cs_map = get_client_session_map(*mutex_->thread_holding_);
        if (OB_FAIL(cs_map.erase(cs_id_))) {
          PROXY_CS_LOG(WDIAG, "current client session is not in table, no need to erase", K_(cs_id), K(ret));
        } else {
          in_list_stat_ = LIST_REMOVED;
        }
      }
    }

    if (cs_id_ != 0 && NULL != mutex_ && NULL != mutex_->thread_holding_) {
      ObClientSessionIDList &cs_id_list = get_client_session_id_list(*mutex_->thread_holding_);
      if (OB_FAIL(cs_id_list.erase_cs_id(cs_id_))) {
        PROXY_CS_LOG(WDIAG, "fail to record cs_id and thread_id map", K_(cs_id), "thread_id", self_ethread().id_, K(ret));
      }
    }

    // in 2 situations we will delete cluster (cluster rslist and resource)
    // 1. all servers of table entry which comes from rslist are not in congestion list
    // 2. fail to verify cluster name in login step, user cluster and server cluster are not the same
    //
    // if we only delete resource, loacl cluster rslist still exist, so we must delete both of them
    if (OB_UNLIKELY(is_need_delete_cluster())) {
      if (OB_FAIL(handle_delete_cluster())) {
        PROXY_CS_LOG(WDIAG, "fail to handle delete cluster", K(ret));
      }
      set_need_delete_cluster(false);
    }

    if ((this_ethread() == create_thread_)
        || is_proxy_mysql_client() // if proxy mysql client, direct destroy, no need scheudle to create thread
        ) {
      read_state_ = MCS_CLOSED;
      if (OB_FAIL(do_api_callout(OB_MYSQL_SSN_CLOSE_HOOK))) {
        PROXY_CS_LOG(WDIAG, "fail to close hook, destroy itself", K(ret));
        destroy();
      }
    }
  }
}

int ObMysqlClientSession::handle_other_event(int event, void *data)
{
  UNUSED(data);
  switch (event) {
    case CLIENT_SESSION_ERASE_FROM_MAP_EVENT: {
      do_io_close();
      break;
    }
    default:
      PROXY_CS_LOG(WDIAG, "unknown event", K(event));
      break;
  }

  return VC_EVENT_NONE;
}

inline int ObMysqlClientSession::handle_delete_cluster()
{
  // in 2 situations we will delete cluster (cluster rslist and resource)
  // 1. all servers of table entry which comes from rslist are not in congestion list
  // 2. fail to verify cluster name in login step, user cluster and server cluster are not the same
  // if we only delete resource, loacl cluster rslist still exist, so we must delete both of them
  int ret = OB_SUCCESS;
  const ObString &name = session_info_.get_login_req().get_hsr_result().cluster_name_;
  const int64_t cr_id = session_info_.get_login_req().get_hsr_result().cluster_id_;
  //1. delete cluster rslist in json
  ObConfigServerProcessor &csp = get_global_config_server_processor();
  if (OB_FAIL(csp.delete_rslist(name, cr_id))) {
    PROXY_CS_LOG(WDIAG, "fail to delete cluster rslist", K(name), K(cr_id), K(ret));
  }

  //2. delete cluster resource in resource_pool
  ObResourcePoolProcessor &rpp = get_global_resource_pool_processor();
  if (name == OB_META_DB_CLUSTER_NAME) {
    const bool ignore_cluster_not_exist = true;
    if (OB_FAIL(rpp.rebuild_metadb(ignore_cluster_not_exist))) {
      PROXY_CS_LOG(WDIAG, "fail to rebuild metadb cluster resource", K(ret));
    }
  } else {
    if (OB_FAIL(rpp.delete_cluster_resource(name, cr_id))) {
      PROXY_CS_LOG(WDIAG, "fail to delete cluster resource", K(name), K(cr_id), K(ret));
    }
  }
  return ret;
}

int ObMysqlClientSession::state_server_keep_alive(int event, void *data)
{
  STATE_ENTER(&ObMysqlClientSession::state_server_keep_alive, event, data);

  int64_t ret = OB_SUCCESS;
  int64_t async_disconnect_code = OB_SUCCESS;
  if (OB_LIKELY(data == last_ss_keep_alive_vio_) && OB_LIKELY(NULL != last_ss_)) {
    switch (event) {
        // fallthrough
      case VC_EVENT_ERROR:
      case VC_EVENT_READ_READY:
      case VC_EVENT_EOS:
        // The server session closed or something is amiss
      case VC_EVENT_DETECT_SERVER_DEAD:
        // find server dead

      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_INACTIVITY_TIMEOUT:
        // Timeout - close it
        if (last_ss_->get_session_info().is_key_session()) {
          async_disconnect_code = last_ss_->get_session_info().get_key_session_code();
          PROXY_CS_LOG(WDIAG, "client session closed because of the bound key server session close");
        } else if (OB_MYSQL_COM_STMT_SEND_LONG_DATA == mysql_sm_->trans_state_.trans_info_.sql_cmd_
                   || OB_MYSQL_COM_STMT_SEND_PIECE_DATA == mysql_sm_->trans_state_.trans_info_.sql_cmd_) {
          async_disconnect_code = OB_PROXY_SEND_LONG_DATA_PIECES_ERROR;
          PROXY_CS_LOG(WDIAG, "client session closed because of send long data/pieces server session close");
        }
        if (OB_UNLIKELY(OB_SUCCESS != async_disconnect_code)) {
          set_closed_key_server_session(last_ss_);
          if (OB_FAIL(async_disconnect_by_internal_reason(async_disconnect_code))) {
            PROXY_CS_LOG(WDIAG, "fail to close client session", "client session", this, KPC(last_ss_), K(ret));
          }
        } else {
          close_last_server_session();
        }
        break;

      case VC_EVENT_READ_COMPLETE:
      default:
        // These events are bogus
        PROXY_CS_LOG(WDIAG, "invalid event", K(event));
        break;
    }
  }

  return VC_EVENT_NONE;
}

int ObMysqlClientSession::state_keep_alive(int event, void *data)
{
  int ret = OB_SUCCESS;
  STATE_ENTER(&ObMysqlClientSession::state_keep_alive, event, data);
  if (OB_LIKELY(data == ka_vio_)) {
    switch (event) {
      case VC_EVENT_READ_READY: {
        // handle half closed
        if (MCS_HALF_CLOSED == read_state_) {
          if (OB_FAIL(buffer_reader_->consume(buffer_reader_->read_avail()))) {
            PROXY_CS_LOG(WDIAG, "fail to consume ", K(ret));
          }
        } else {
          // only after obproxy has sent handshake packet to client, client will send data to obproxy;
          // so in that case, client session must has been inited completed.
          // New transaction, need to spawn of new sm to process request
          if (OB_FAIL(new_transaction())) {
            PROXY_CS_LOG(WDIAG, "fail to start new transaction", K(ret));
          }
        }
        break;
      }
      case VC_EVENT_EOS: {
        PROXY_CS_LOG(WDIAG, "client session received VC_EVENT_EOS event",
                     K_(cs_id), K_(read_state));
        if (MCS_HALF_CLOSED == read_state_) {
          half_close_ = false;
          do_io_close();
        } else {
          // If there is data in the buffer, start a new
          // transaction, otherwise the client gave up
          if (buffer_reader_->read_avail() > 0) {
            if (OB_FAIL(new_transaction())) {
              PROXY_CS_LOG(WDIAG, "fail to start new transaction", K(ret));
            }
          } else {
            do_io_close();
          }
        }
        break;
      }
      // fallthrough
      case VC_EVENT_ERROR:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_INACTIVITY_TIMEOUT:
      case VC_EVENT_DETECT_SERVER_DEAD: {
        if (MCS_HALF_CLOSED == read_state_) {
          half_close_ = false;
        }
        // Keep-alive timed out
        if (VC_EVENT_INACTIVITY_TIMEOUT == event) {
          ObIpEndpoint client_ip;
          if (NULL != client_vc_) {
            if (OB_UNLIKELY(!ops_ip_copy(client_ip, client_vc_->get_remote_addr()))) {
              PROXY_CS_LOG(WDIAG, "fail to ops_ip_copy client_ip", K(client_vc_));
            }
          }

          PROXY_CS_LOG(WDIAG, "client connection is idle over wait_timeout, now we will close it.",
                       "wait_timeout(s)", hrtime_to_sec(session_info_.get_wait_timeout()),
                       K_(cs_id),
                       K(client_ip),
                       "event", ObMysqlDebugNames::get_event_name(event));
        }

        do_io_close();
        break;
      }
      case VC_EVENT_READ_COMPLETE:
      default:
        // These events are bogus
      PROXY_CS_LOG(WDIAG, "invalid event", K(event));
      break;
    }
  }
  return VC_EVENT_NONE;
}
void ObMysqlClientSession::close_last_server_session()
{
  PROXY_CS_LOG(INFO, "close last server session", KPC(last_ss_));
  ObMysqlServerSession *to_be_close = last_ss_;
  close_and_destroy_session(to_be_close);
  set_last_ss_keep_alive_vio(NULL);
}

int ObMysqlClientSession::swap_mutex(void *data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WDIAG, "mutex data cannot be null here", K(ret));
  } else if (mutex_ != data) {
    ObProxyMutex *mutex = reinterpret_cast<ObProxyMutex *>(data);
    // swap client session mutex
    if (IS_DEBUG_ENABLED()) {
      ObHSRResult &hsr = get_session_info().get_login_req().get_hsr_result();
      LOG_DEBUG("mysql client swap mutex", "old_mutex", mutex_.ptr_,
                "new_mutex", mutex, K(hsr.full_name_));
    }
    mutex_ = mutex;

    //swap server session mutex
    if (NULL != last_ss_) {
      last_ss_->do_io_read(this, INT64_MAX, last_ss_->read_buffer_);
      last_ss_->do_io_write(this, 0, NULL);
    }

    if (NULL != mysql_sm_ && OB_FAIL(mysql_sm_->swap_mutex(mutex))) {
      PROXY_CS_LOG(WDIAG, "failed to swap mysql sm mutex", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_LIKELY(0 == session_manager_.get_svr_session_count()
                    && 0 == session_manager_sharding_.get_svr_session_count())) {
        session_manager_.set_mutex(mutex);
        session_manager_sharding_.set_mutex(mutex);
      } else {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CS_LOG(WDIAG, "server session pool is not empty", K(ret));
      }
    }
  }
  return ret;
}

void ObMysqlClientSession::reenable(ObVIO *vio)
{
  client_vc_->reenable(vio);
}

int ObMysqlClientSession::attach_last_to_second_last_session()
{
  int ret = OB_SUCCESS;
  if (last_ss_ != NULL && second_last_ss_ == NULL) {
    last_ss_->do_io_read(this, 0, NULL);
    set_second_last_server_session(last_ss_);
    set_last_server_session(NULL);
    set_last_ss_keep_alive_vio(NULL);
    set_need_return_second_last_server_session(true);
  } else if (second_last_ss_ != NULL) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(EDIAG, "must release last bound ss before call this");
  }

  return ret;
}

int ObMysqlClientSession::attach_second_last_to_last_session()
{
  int ret = OB_SUCCESS;
  if (second_last_ss_ != NULL && last_ss_ == NULL) {
    second_last_ss_->state_ = KEEP_ALIVE_CLIENT_SLAVE;
    set_last_server_session(second_last_ss_);
    set_last_ss_keep_alive_vio(NULL);
    set_second_last_server_session(NULL);
    set_need_return_second_last_server_session(false);
    PROXY_CS_LOG(DEBUG, "attaching last bound as bound", K_(cs_id), "ss_id", last_ss_->ss_id_);
    last_ss_->get_reader()->mbuf_->water_mark_ = MYSQL_NET_META_LENGTH;
    if (active_) {
      active_ = false;
      MYSQL_DECREMENT_DYN_STAT(CURRENT_ACTIVE_CLIENT_CONNECTIONS);
    }
    // Since this our slave, issue an IO to detect a close and
    // have it call the client session back.  This IO also prevent
    // the server net connection from calling back a dead sm
    if (OB_LIKELY(ka_vio_ != (last_ss_keep_alive_vio_ = last_ss_->do_io_read(this, INT64_MAX, last_ss_->read_buffer_)))) {
      // Transfer control of the write side as well
      last_ss_->do_io_write(this, 0, NULL);
      last_ss_->set_inactivity_timeout(session_info_.get_wait_timeout(), obutils::OB_SERVER_WAIT_TIMEOUT);
      #ifdef ERRSIM
      int tmp_ret = OB_SUCCESS;
      if ((tmp_ret = OB_E(EventTable::EN_SERVER_TRX_TIMEOUT) OB_SUCCESS) != OB_SUCCESS) {
        set_inactivity_timeout(1, obutils::OB_SERVER_TRX_TIMEOUT);
      }
      #endif
    }
  } else if (last_ss_ != NULL) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(EDIAG, "must release last_ss_ before call this", KP(last_ss_), K(*last_ss_));
  }

  return ret;
}

int ObMysqlClientSession::release_second_last_server_session()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(release_server_session(second_last_ss_))) {
    PROXY_CS_LOG(WDIAG, "fail to release last bound ss", KP(second_last_ss_));
  } else {
    second_last_ss_= NULL;
  }

  return ret;
}

int ObMysqlClientSession::release_last_server_session()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(release_server_session(last_ss_))) {
    PROXY_CS_LOG(WDIAG, "fail to release bound ss", KP(last_ss_));
  } else {
    set_last_server_session(NULL);
    set_last_ss_keep_alive_vio(NULL);
  }

  return ret;
}

int ObMysqlClientSession::attach_last_server_session(ObMysqlServerSession *session)
{
  int ret = OB_SUCCESS;
  if (NULL != session) {
    if (OB_UNLIKELY(NULL != last_ss_) || OB_UNLIKELY(session != cur_ss_)
        || OB_UNLIKELY(0 != session->get_reader()->read_avail())
        || OB_UNLIKELY(session->get_netvc() == client_vc_)) {
      ret = OB_INVALID_ARGUMENT;
      PROXY_CS_LOG(WDIAG, "invalid server session", K(last_ss_), K(session), K(cur_ss_),
                   K(session->get_reader()->read_avail()), K(session->get_netvc()), K(client_vc_));
    } else {
      session->state_ = KEEP_ALIVE_CLIENT_SLAVE;
      last_ss_ = session;
      //reset cur_session_
      cur_ss_ = NULL;
      PROXY_CS_LOG(DEBUG, "attaching server last_ss_ as slave", K_(cs_id), "ss_id", last_ss_->ss_id_);
      // reset server read buffer water mark
      last_ss_->get_reader()->mbuf_->water_mark_ = MYSQL_NET_META_LENGTH;
      // handling potential keep-alive here
      if (active_) {
        active_ = false;
        MYSQL_DECREMENT_DYN_STAT(CURRENT_ACTIVE_CLIENT_CONNECTIONS);
      }
      // Since this our slave, issue an IO to detect a close and
      // have it call the client session back.  This IO also prevent
      // the server net connection from calling back a dead sm
      if (OB_LIKELY(ka_vio_ != (last_ss_keep_alive_vio_ = last_ss_->do_io_read(this, INT64_MAX, last_ss_->read_buffer_)))) {
        // Transfer control of the write side as well
        last_ss_->do_io_write(this, 0, NULL);
        last_ss_->set_inactivity_timeout(session_info_.get_wait_timeout(), obutils::OB_SERVER_WAIT_TIMEOUT);
        #ifdef ERRSIM
        int tmp_ret = OB_SUCCESS;
        if ((tmp_ret = OB_E(EventTable::EN_SERVER_TRX_TIMEOUT) OB_SUCCESS) != OB_SUCCESS) {
          set_inactivity_timeout(1, obutils::OB_SERVER_TRX_TIMEOUT);
        }
        #endif
      }
    }
  } else {
    last_ss_ = NULL;
    last_ss_keep_alive_vio_ = NULL;
  }
  return ret;
}

int ObMysqlClientSession::release_server_session(ObMysqlServerSession *session) {
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(session)) {
    dbconfig::ObShardConnector *shard_conn = session->get_session_info().get_shard_connector();
    // for sql
    if (NULL == shard_conn) {
      if (OB_FAIL(session_manager_.release_server_session(session))) {
        PROXY_CS_LOG(WDIAG, "fail to release server session to session manager", K(ret), K(*session));
      }
    // for sharding
    } else {
      if (OB_FAIL(session_manager_sharding_.release_session(shard_conn->shard_name_.config_string_, *session))) {
        PROXY_CS_LOG(WDIAG, "fail to release server session to session manager",
                            K(ret), K(*session), K(shard_conn->shard_name_.config_string_));
      }
    }
    session = NULL;
  }

  return ret;
}

int ObMysqlClientSession::main_handler(int event, void *data)
{
  int event_ret = VC_EVENT_CONT;
  PROXY_CS_LOG(DEBUG, "[ObMysqlClientSession::main_handler]",
            "event_name", ObMysqlDebugNames::get_event_name(event),
            "read_state", get_read_state_str(), K(data));

  if (OB_LIKELY(MYSQL_CS_MAGIC_ALIVE == magic_)) {
    if (NULL != data && data == ka_vio_) { // from client vc
      event_ret = state_keep_alive(event, data);
    } else if (NULL != data && data == last_ss_keep_alive_vio_) { // from server vc
      event_ret = state_server_keep_alive(event, data);
    } else if (CLIENT_VC_SWAP_MUTEX_EVENT == event) { // from proxy client vc
      int ret = OB_SUCCESS;
      if (OB_FAIL(swap_mutex(data))) {
        PROXY_CS_LOG(WDIAG, "fail to swap mutex", KP(data), K(ret));
      }
    } else if (is_proxy_mysql_client() && CLIENT_VC_DISCONNECT_LAST_USED_SS_EVENT == event) {
      close_last_server_session();
    } else {
      event_ret = (this->*cs_default_handler_)(event, data); // others
    }
  } else {
    PROXY_CS_LOG(WDIAG, "unexpected magic, expected MYSQL_CS_MAGIC_ALIVE", K(magic_));
  }

  return event_ret;
}

void ObMysqlClientSession::handle_transaction_complete(ObIOBufferReader *r, bool &close_cs)
{
  int ret = OB_SUCCESS;
  close_cs = false;
  set_is_waiting_trans_first_request(true);
  if (OB_LIKELY(MCS_ACTIVE_READER == read_state_) && OB_LIKELY(NULL != mysql_sm_)) {
    PROXY_CS_LOG(DEBUG, "client session handle transaction complete",
                 K_(cs_id), K(mysql_sm_->sm_id_), KPC_(cluster_resource));
    if (OB_UNLIKELY(get_global_proxy_config().is_metadb_used() && mysql_sm_->trans_state_.mysql_config_params_->enable_report_session_stats_)) {
      update_session_stats();
    }

    // Make sure that the state machine is returning correct buffer reader
    if (OB_UNLIKELY(r != buffer_reader_)) {
      PROXY_CS_LOG(WDIAG, "buffer reader mismatch, will close client session",
                   K(r), K(buffer_reader_));
      close_cs = true;
      COLLECT_INTERNAL_DIAGNOSIS(mysql_sm_->connection_diagnosis_trace_, OB_PROXY_INTERNAL_TRACE, OB_PROXY_INTERNAL_ERROR, "buffer reader mismatch, will disconnect");
    } else if (OB_UNLIKELY(!get_global_hot_upgrade_info().need_conn_accept_) && OB_UNLIKELY(need_close())) {
      close_cs = true;
      PROXY_CS_LOG(INFO, "receive exit cmd, obproxy will exit, now close client session",
                   K(*this));
    // 这里只判断非 sharding 的情况. sharding 模式 use db 可能会切换集群, 会置 NULL
    } else if (OB_LIKELY(!session_info_.is_sharding_user() && session_info_.is_oceanbase_server())) {
      if ((OB_ISNULL(cluster_resource_) || OB_UNLIKELY(cluster_resource_->is_deleting())) && !is_proxysys_tenant()) {
        if (NULL != cluster_resource_) {
          PROXY_CS_LOG(INFO, "the cluster resource is deleting, client session will close",
                       KPC_(cluster_resource), K_(cluster_resource), K_(session_states_.is_proxy_mysql_client));
        }
        COLLECT_INTERNAL_DIAGNOSIS(mysql_sm_->connection_diagnosis_trace_, OB_PROXY_INTERNAL_TRACE, OB_PROXY_CLUSTER_RESOURCE_EXPIRED, "cluster resource is deleting, will disconnect");
        close_cs = true;
      }
    }
    if (OB_LIKELY(!close_cs)) {
      if (OB_UNLIKELY(get_global_performance_params().enable_stat_)) {
        MYSQL_DECREMENT_DYN_STAT(CURRENT_CLIENT_TRANSACTIONS);
        if (OB_UNLIKELY(NULL != trace_stats_) && trace_stats_->is_trace_stats_used()) {
          PROXY_CS_LOG(DEBUG, "current trace stats", KPC_(trace_stats), K_(cs_id));
          //mark_trace_stats_need_reuse() will not remove history stats, just denote trace_stats need reset next time
          trace_stats_->mark_trace_stats_need_reuse();
        }
      }

      // Clean up the write VIO in case of inactivity timeout
      do_io_write(NULL, 0, NULL);

      if (session_info_.is_oceanbase_server()
          && OB_LIKELY(NULL != cluster_resource_)
          && get_global_resource_pool_processor().get_default_cluster_resource() != cluster_resource_
          && OB_FAIL(session_info_.revalidate_sys_var_set(cluster_resource_->sys_var_set_processor_))) {
        PROXY_CS_LOG(WDIAG, "fail to check sys variable", K_(cs_id), K(ret));
      }
    }
  }
}

int ObMysqlClientSession::release(ObIOBufferReader *r)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(MCS_ACTIVE_READER == read_state_) && OB_LIKELY(NULL != mysql_sm_)) {
    // handling potential keep-alive here
    if (OB_LIKELY(active_)) {
      active_ = false;
      MYSQL_DECREMENT_DYN_STAT(CURRENT_ACTIVE_CLIENT_CONNECTIONS);
    }

    bool close_cs = false;
    handle_transaction_complete(r, close_cs);

    if (OB_LIKELY(!close_cs)) {
      mysql_sm_ = NULL;

      // reset client read buffer water mark
      buffer_reader_->mbuf_->water_mark_ = MYSQL_NET_META_LENGTH;

      // Check to see there is remaining data in the
      // buffer. If there is, spin up a new state
      // machine to process it. Otherwise, issue an
      // IO to wait for new data
      if (buffer_reader_->read_avail() > 0) {
        PROXY_CS_LOG(DEBUG, "data already in buffer, starting new transaction", K_(cs_id));
        if (OB_FAIL(new_transaction())) {
          PROXY_CS_LOG(WDIAG, "fail to start new transaction", K(ret));
        }
      } else {
        read_state_ = MCS_KEEP_ALIVE;
        ka_vio_ = do_io_read(this, INT64_MAX, read_buffer_);
        if (OB_LIKELY(last_ss_keep_alive_vio_ != ka_vio_)) {
          client_vc_->add_to_keep_alive_lru();
          set_wait_timeout();
        }
      }
    } else {
      mysql_sm_ = NULL;
      do_io_close();
    }
  }
  return ret;
}

int ObMysqlClientSession::init_session_pool_info()
{
  int ret = OB_SUCCESS;
  if (!schema_key_.init_ && mysql_sm_ != NULL) {
    ObServerSessionMatchRules rules;
    rules.enable_oceanbase_20_protocol_ = mysql_sm_->get_server_protocol() == ObProxyProtocol::PROTOCOL_OCEANBASE_20;
    rules.enable_oceanbase_20_compress_ = (mysql_sm_->compression_algorithm_.level_ != 0 && rules.enable_oceanbase_20_protocol_);
    rules.enable_compressed_mysql_protocol_ = mysql_sm_->get_server_protocol() == ObProxyProtocol::PROTOCOL_COMPRESSED_MYSQL;
    rules.enable_full_link_trace_ = get_global_proxy_config().enable_full_link_trace;
    rules.enable_client_session_id_v2_ = is_cs_id_v2();
    rules.client_mysql_cap_ = get_session_info().get_orig_capability_flags();
    if (rules.enable_client_session_id_v2_) {
      extract_proxy_id_v2(cs_id_, rules.proxy_id_);
    }
    // for first login do not match auth plugin method because auth switch may happen
    session_manager_.set_session_matched_rules(rules);
    PROXY_CS_LOG(DEBUG, "init session matched rules", K(rules));
  }
  if (!session_info_.is_sharding_user() && schema_key_.init_) {
    PROXY_CS_LOG(DEBUG, "no sharding already init", K(schema_key_));
  } else if (OB_FAIL(ObMysqlSessionUtils::init_schema_key_with_client_session(schema_key_, this))) {
    PROXY_CS_LOG(WDIAG, "init_schema_key_with_client_session failed", K(ret));
  }
  return ret;
}

int ObMysqlClientSession::acquire_svr_session(const sockaddr &addr, ObMysqlServerSession *&svr_session,
                                              const bool close_last_ss, const bool can_use_session_from_pool)
{
  int ret = OB_SUCCESS;
  svr_session = NULL;

  if (is_enable_session_conn_pool()) {
    if (OB_FAIL(init_session_pool_info())) {
      PROXY_CS_LOG(WDIAG, "init_session_pool_info failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // 1. try last_session
    if (NULL != last_ss_) {
      bool same_dbkey = true;
      // database may be changed so compare the dbkey
      if (is_enable_session_conn_pool()
          && schema_key_.dbkey_.config_string_ != last_ss_->schema_key_.dbkey_.config_string_) {
        same_dbkey = false;
      }

      if (same_dbkey
          && ops_ip_addr_port_eq(last_ss_->server_ip_, addr)
          && last_ss_->auth_user_ == session_info_.get_full_username()) { // com_change_user may be executed
        svr_session = last_ss_;
        set_last_server_session(NULL);
        PROXY_CS_LOG(DEBUG, "[acquire server session] use last server session", K_(cs_id), "server_ip", svr_session->server_ip_);
      } else {
        PROXY_CS_LOG(DEBUG, "[acquire server session] last server session dismatched", K(same_dbkey),
                            "client_session dbkey", schema_key_.dbkey_.config_string_,
                            "last_ss dbkey", last_ss_->schema_key_.dbkey_.config_string_,
                            "last ss auth_user", last_ss_->auth_user_,
                            "client_session full_username", session_info_.get_full_username(),
                            K(last_ss_->server_ip_), K(ops_ip_addr_port_eq(last_ss_->server_ip_, addr)));
        // last_ss 不能用,考虑将 last_ss 归还给 session_manager_
        bool is_non_pooled_mysql = is_proxy_mysql_client() && !is_enable_session_conn_pool();
        if (OB_FAIL(session_manager_.release_server_session(last_ss_, is_non_pooled_mysql || close_last_ss))) {
          PROXY_CS_LOG(WDIAG, "fail to reclaim server session", K(ret), KP(last_ss_));
        } else if (OB_FALSE_IT(last_ss_ = NULL)) { // 会话归还给了 session manager 后需要设置为 NULL
        } else if (is_non_pooled_mysql) {
          ret = OB_SESSION_NOT_FOUND;
        }
      }
    }

    if (OB_ISNULL(svr_session) && !is_proxy_mysql_client()) {
      // for sql
      if (OB_ISNULL(session_info_.get_shard_connector())) {
        if (OB_FAIL(session_manager_.acquire_server_session_local(addr, session_info_.get_full_username(), svr_session))) {
          PROXY_CS_LOG(WDIAG, "fail to acquire local server session", K(ret), "addr", ObIpEndpoint(addr), KP(svr_session));
        }

        bool acquire_session_pool = OB_ISNULL(svr_session) && can_use_session_from_pool && is_enable_session_conn_pool();
        if (acquire_session_pool
            && OB_FAIL(session_manager_.acquire_server_session_global(addr, session_info_.get_full_username(),
                                                                      schema_key_, svr_session))) {
          PROXY_CS_LOG(WDIAG, "fail to acquire global server session", K(ret), "addr", ObIpEndpoint(addr), KP(svr_session));
        }
      // for sharding
      } else {
        if (OB_FAIL(session_manager_sharding_.acquire_server_session(session_info_.get_shard_connector(), addr,
                                                                     session_info_.get_full_username(), schema_key_,
                                                                     svr_session))) {
          PROXY_CS_LOG(WDIAG, "fail to acquire server session for sharding", K(ret), "addr", ObIpEndpoint(addr), KP(last_ss_), KP(svr_session));
        }
      }
    }
  }

  // 3. check svr_session again
  if (OB_SUCC(ret) && OB_ISNULL(svr_session)) {
    ret = OB_SESSION_NOT_FOUND;
    PROXY_CS_LOG(DEBUG, "[acquire server session] session not found, will call connect observer",
                        K_(cs_id), KPC(svr_session), K(ret));
  }
  return ret;
}

int64_t ObMysqlClientSession::get_svr_session_count() const
{
  if (session_info_.is_sharding_user()) {
    return (const_cast<ObMysqlSessionManagerSharding &>(session_manager_sharding_).get_svr_session_count() + (NULL == last_ss_ ? 0 : 1)
            + (NULL == cur_ss_ ? 0 : 1) + (NULL == second_last_ss_ ? 0 : 1));
  } else {
    return (session_manager_.get_svr_session_count() + (NULL == last_ss_ ? 0 : 1)
            + (NULL == cur_ss_ ? 0 : 1) + (NULL == second_last_ss_ ? 0 : 1));
  }
}

const char *ObMysqlClientSession::get_read_state_str() const
{
  const char *states[MCS_MAX + 1] = {"MCS_INIT",
                                     "MCS_ACTIVE_READER",
                                     "MCS_KEEP_ALIVE",
                                     "MCS_HALF_CLOSED",
                                     "MCS_CLOSED",
                                     "MCS_MAX"};
  return states[read_state_];
}

int64_t ObMysqlClientSession::ObSessionStats::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  J_OBJ_START();
  J_KV(K_(modified_time), K_(reported_time), K_(is_first_register));
  for (int64_t i = 0; OB_SUCC(ret) && i < SESSION_STAT_COUNT; ++i) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", %s=%ld", g_mysql_stat_name[i], stats_[i]))) {
    }
  }
  J_OBJ_END();
  return pos;
}

inline void ObMysqlClientSession::update_session_stats()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mysql_sm_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WDIAG, "mysql sm is null", K(ret));
  } else if (mysql_sm_->trans_state_.mysql_config_params_->enable_report_session_stats_) {
    mysql_sm_->update_session_stats(session_stats_.stats_, SESSION_STAT_COUNT);
    session_stats_.modified_time_ = get_hrtime();
    if (0 == session_stats_.reported_time_) {
      session_stats_.reported_time_ = get_hrtime();
    }
    if (!is_proxy_mysql_client()
        && (session_stats_.modified_time_ - session_stats_.reported_time_) > mysql_sm_->trans_state_.mysql_config_params_->stat_table_sync_interval_
        && g_current_report_count < ObStatProcessor::MAX_RUNNING_SESSION_STAT_REPROT_TASK_COUNT) {
      PROXY_CS_LOG(DEBUG, "ObMysqlClientSession::update_session_stats()", K_(cs_id));
      (void)ATOMIC_FAA(&g_current_report_count, 1);

      const ObString &cluster_name = session_info_.get_priv_info().cluster_name_;
      char cluster_name_str[OB_PROXY_MAX_CLUSTER_NAME_LENGTH + 1];
      cluster_name_str[0] = '\0';
      int64_t w_len = snprintf(cluster_name_str, sizeof(cluster_name_str), "%.*s",
                               cluster_name.length(), cluster_name.ptr());
      if (OB_UNLIKELY(w_len <= 0) || OB_UNLIKELY(w_len >= OB_PROXY_MAX_CLUSTER_NAME_LENGTH + 1)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CS_LOG(WDIAG, "copy cluster name string error", K(ret));
      } else if (OB_FAIL(g_stat_processor.report_session_stats(cluster_name_str, get_proxy_sessid(),
          session_stats_.stats_, session_stats_.is_first_register_, g_mysql_stat_name, SESSION_STAT_COUNT))) {
        PROXY_CS_LOG(WDIAG, "fail to init report_session_stats", K_(cs_id), K(ret));
      } else {
        session_stats_.is_first_register_ = false;
        session_stats_.reported_time_ = get_hrtime();
      }
    }
  }
}

int64_t ObMysqlClientSession::get_session_timeout(const char *timeout_name, ObHRTime time_unit) const
{
  int64_t timeout = 0;
  int64_t session_timeout = 0;
  int ret = OB_SUCCESS;

  // if we get session_timeout failed or the value in session_timeout is negative we use default value
  // TODO: get_session_timeout is not efficiency, we may cache some timeout in client session to speed up
  if (OB_FAIL(session_info_.get_session_timeout(timeout_name, session_timeout)) || session_timeout <= 0) {
    // the value is MICROSECOND(us), convert it to nanosecond(ns)
    PROXY_CS_LOG(DEBUG, "fail to get session timeout, will use default value", K(timeout_name), K(session_timeout), K(ret));
    ObMysqlConfigParams *params = NULL;
    if (OB_ISNULL(mysql_sm_)) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CS_LOG(WDIAG, "mysql_sm_ is null", K(ret));
    } else if (OB_ISNULL(params = mysql_sm_->trans_state_.mysql_config_params_)) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CS_LOG(WDIAG, "trans_state_.mysql_config_params_ is null", K(ret));
    } else {
      timeout = HRTIME_NSECONDS(params->default_inactivity_timeout_);
      PROXY_CS_LOG(DEBUG, "use default timeout, unit is ns", K(timeout_name), K(session_timeout), K(timeout));
      ret = OB_SUCCESS;
    }
  } else {
    // convert it to nanosecond(ns)
    timeout = session_timeout * time_unit;
    PROXY_CS_LOG(DEBUG, "succ to get session timeout, unit is ns", K(timeout_name) , K(timeout));
  }
  return timeout;
}

int64_t ObMysqlClientSession::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this),
       K_(session_states_.is_proxy_mysql_client),
       K_(session_states_.is_waiting_trans_first_request),
       K_(session_states_.need_delete_cluster),
       K_(session_states_.is_first_dml_sql_got),
       K_(session_states_.vc_ready_killed),
       K_(active),
       K_(magic),
       K_(conn_decrease),
       K_(current_tid),
       K_(cs_id),
       K_(proxy_sessid),
       K_(session_info),
       K_(dummy_ldc),
       KP_(dummy_entry),
       K_(server_state_version),
       KP_(cur_ss),
       KP_(last_ss),
       KP_(lii_ss),
       KP_(lock_ss),
       KP_(closed_key_ss),
       KPC_(cluster_resource),
       KP_(client_vc),
       K_(session_states_.using_ldg),
       K_(session_states_.using_service_name),
       KPC_(trace_stats));
  J_OBJ_END();
  return pos;
}

int ObMysqlClientSession::fill_session_priv_info()
{
  int ret = OB_SUCCESS;
  ObProxySessionPrivInfo &priv_info = session_info_.get_priv_info();
  priv_info.has_all_privilege_ = is_proxysys_user();
  priv_info.cs_id_ = cs_id_;
  const ObHSRResult &hsr = session_info_.get_login_req().get_hsr_result();
  priv_info.cluster_name_ = hsr.cluster_name_;
  priv_info.tenant_name_ = hsr.tenant_name_;
  priv_info.user_name_ = hsr.user_name_;

  PROXY_CS_LOG(DEBUG, "succ to fill priv info", K_(cs_id), K(priv_info));

  return ret;
}

bool ObMysqlClientSession::is_authorised_proxysys(const ObProxyLoginUserType type)
{
  bool bret = false;
  int ret = OB_SUCCESS;

  //1.check ip
  if (RUN_MODE_PROXY == g_run_mode
      && !get_global_proxy_config().skip_proxy_sys_private_check
      && !ops_is_ip_private(client_vc_->get_remote_addr())
      && !ops_is_ip_loopback(client_vc_->get_remote_addr())) {
    char src_ip[INET6_ADDRSTRLEN];
    ops_ip_ntop(client_vc_->get_remote_addr(), src_ip, sizeof(src_ip));
    PROXY_CS_LOG(WDIAG, "xx@proxysys ip is not private", "ip", src_ip, K(ret));

  //2. check password
  } else {
    const ObString &login_passwd = session_info_.get_login_req().get_hsr_result().response_.get_auth_response();
    const ObString &scramble_string = (session_info_.get_scramble_string().empty()
        ? ObString::make_string(OB_AUTH_DATA_AB)
        : get_scramble_string());
    ObString stored_stage1;

    if (USER_TYPE_PROXYSYS == type && 0 == login_passwd.length()) {
      if (0 != strlen(get_global_proxy_config().obproxy_sys_password.str())) {
        ret = OB_PASSWORD_WRONG;
        PROXY_CS_LOG(WDIAG, "root@proxysys check failed", K(ret));
      } else {
        bret = true;
      }
    } else {
      if (USER_TYPE_PROXYSYS == type) {
        stored_stage1.assign_ptr(get_global_proxy_config().obproxy_sys_password.str(),
                                 static_cast<int32_t>(strlen(get_global_proxy_config().obproxy_sys_password.str())));
      } else {
        stored_stage1.assign_ptr(get_global_proxy_config().inspector_password.str(),
                                 static_cast<int32_t>(strlen(get_global_proxy_config().inspector_password.str())));
      }
      char stored_stage2_hex[SCRAMBLE_LENGTH] = {0};
      int64_t copy_len = 0;
      if (OB_FAIL(ObEncryptedHelper::encrypt_stage1_to_stage2_hex(stored_stage1, stored_stage2_hex, SCRAMBLE_LENGTH, copy_len))) {
        PROXY_CS_LOG(WDIAG, "failed to encrypt_stage1_to_stage2_hex",  K(ret));
      } else {
        ObString stored_stage2_hex_str(copy_len, stored_stage2_hex);
        if (OB_FAIL(ObEncryptedHelper::check_login(login_passwd, scramble_string, stored_stage2_hex_str, bret))) {
          PROXY_CS_LOG(WDIAG, "failed to check proxysys login",  K(ret));
        } else {
          if (!bret) {
            ret = OB_PASSWORD_WRONG;
            PROXY_CS_LOG(WDIAG, "password error", K(ret));
          }
        }
      }
    }
  }
  return bret;
}

bool ObMysqlClientSession::is_need_convert_vip_to_tname()
{
  return (get_global_proxy_config().need_convert_vip_to_tname
          && !this->is_proxy_mysql_client()
          && RUN_MODE_PROXY == g_run_mode);
}

inline bool ObMysqlClientSession::need_close() const
{
  bool bret = false;
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  if (OB_UNLIKELY(info.graceful_exit_start_time_ > 0)
      && OB_LIKELY(!info.need_conn_accept_)
      && !is_proxy_mysql_client()
      && info.active_client_vc_count_ > 0
      && OB_LIKELY(info.graceful_exit_end_time_ > info.graceful_exit_start_time_)) {
    int64_t current_active_count = 0;
    NET_READ_GLOBAL_DYN_SUM(NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN, current_active_count);
    const ObHRTime remain_time = info.graceful_exit_end_time_ - get_hrtime();
    const ObHRTime total_time = info.graceful_exit_end_time_ - info.graceful_exit_start_time_;
    //use CEIL way
    const int64_t need_active_count = static_cast<int64_t>((remain_time * info.active_client_vc_count_ + total_time - 1) / total_time);
    if (remain_time < 0) {
      bret = true;
      PROXY_CS_LOG(INFO, "client need force close", K(current_active_count),
                   "remain_time(ms)", hrtime_to_msec(remain_time));
    } else if (current_active_count > need_active_count) {
      bret = true;
      PROXY_CS_LOG(INFO, "client need orderly close", K(current_active_count), K(need_active_count),
                   "remain_time(ms)", hrtime_to_msec(remain_time));
    } else {/*do nothing*/}
  }
  return bret;
}

bool ObMysqlClientSession::is_hold_conn_id(const uint32_t conn_id)
{
  bool bret = false;
  if (NULL != cur_ss_ && conn_id == cur_ss_->get_server_sessid()) {
    bret = true;
  } else if (NULL != last_ss_ && conn_id == last_ss_->get_server_sessid()) {
    bret = true;
  } else if (NULL != second_last_ss_ && conn_id == second_last_ss_->get_server_sessid()) {
    bret = true;
  } else {
    ObServerSessionPool::IPHashTable &ip_pool = session_manager_.get_session_pool().ip_pool_;
    ObServerSessionPool::IPHashTable::iterator spot = ip_pool.begin();
    ObServerSessionPool::IPHashTable::iterator last = ip_pool.end();
    for (; !bret && spot != last; ++spot) {
      if (conn_id == spot->get_server_sessid()) {
        bret = true;
      }
    }
  }
  return bret;
}

ObString ObMysqlClientSession::get_current_idc_name() const
{
  ObString ret_idc;
  //ldc_name has three case:
  //1. if is_proxy_mysql_client() && is_user_idc_name_set_, use inner_request_param_->current_idc_name_
  //2. if session_info_.is_user_idc_name_set, use user's
  //3. if congfig is avail, use global config
  if (is_proxy_mysql_client()
      && OB_LIKELY(NULL != inner_request_param_)
      && inner_request_param_->is_user_idc_name_set_) {
    if (!inner_request_param_->current_idc_name_.empty()) {
      //if empty, return default empty
      ret_idc = inner_request_param_->current_idc_name_;
    }
  } else {
    if (session_info_.is_user_idc_name_set()) {
      if (!session_info_.get_idc_name().empty()) {
        //if empty, return default empty
        ret_idc = session_info_.get_idc_name();
      }
    } else if (OB_NOT_NULL(mysql_sm_) && OB_NOT_NULL(mysql_sm_->multi_level_config_)) {
      ObString tmp_idc(mysql_sm_->multi_level_config_->proxy_idc_name_);
      if (!tmp_idc.empty()) {
        //if empty, return default empty
        ret_idc = tmp_idc;
      }
    }
  }
  return ret_idc;
}

int ObMysqlClientSession::check_update_ldc()
{
  int ret = OB_SUCCESS;
  common::ModulePageAllocator *allocator = NULL;
  ObClusterResource *cluster_resource = NULL;
  bool need_dec_cr = false;
  if (get_global_resource_pool_processor().get_default_cluster_resource() == cluster_resource_) {
    const ObTableEntryName &name = mysql_sm_->trans_state_.pll_info_.te_name_;
    uint64_t cluster_id = get_cluster_id();
    cluster_resource = get_global_resource_pool_processor().acquire_cluster_resource(name.cluster_name_, cluster_id);
    need_dec_cr = true;
  } else {
    cluster_resource = cluster_resource_;
  }

  if (OB_ISNULL(cluster_resource)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WDIAG, "cluster_resource is not avail", K(ret));
  } else if (OB_ISNULL(dummy_entry_) || OB_UNLIKELY(!dummy_entry_->is_tenant_servers_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WDIAG, "dummy_entry_ is not avail", KPC(dummy_entry_), K(ret));
  } else if (OB_ISNULL(mysql_sm_) || OB_ISNULL(mysql_sm_->trans_state_.mysql_config_params_)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WDIAG, "mysql_sm_ is not avail",  K(ret));
  } else if (OB_FAIL(ObLDCLocation::get_thread_allocator(allocator))) {
    PROXY_CS_LOG(WDIAG, "fail to get_thread_allocator", K(ret));
  } else {
    bool is_base_servers_added = cluster_resource->is_base_servers_added();
    ObString new_idc_name = get_current_idc_name();
    //we need update ldc when the follow happened:
    //1. servers_state_version has changed
    //or
    //2. dummuy_ldc is invalid
    //or
    //3. idc_name has changed
    //or
    //4. base_servers has not added
    if (cluster_resource->server_state_version_ != server_state_version_
        || dummy_ldc_.is_empty()
        || 0 != new_idc_name.case_compare(dummy_ldc_.get_idc_name())
        || !is_base_servers_added) {
      PROXY_CS_LOG(DEBUG, "need update dummy_ldc",
                   "old_idc_name", dummy_ldc_.get_idc_name(),
                   K(new_idc_name),
                   "cluster_name", cluster_resource->get_cluster_name(),
                   "old_ss_version", server_state_version_,
                   "new_ss_version", cluster_resource->server_state_version_,
                   "dummy_ldc_is_empty", dummy_ldc_.is_empty(),
                   K(is_base_servers_added));
      ObSEArray<ObServerStateSimpleInfo, ObServerStateRefreshCont::DEFAULT_SERVER_COUNT> simple_servers_info(
          ObServerStateRefreshCont::DEFAULT_SERVER_COUNT, *allocator);
      bool need_ignore = false;
      if (!is_base_servers_added && 0 == cluster_resource->server_state_version_) {
        PROXY_CS_LOG(INFO, "base servers has not added, treat all tenant server as ok",
                     "tenant_server", *(dummy_entry_->get_tenant_servers()), K(ret));
      } else {
        const uint64_t new_ss_version = cluster_resource->server_state_version_;
        common::ObIArray<ObServerStateSimpleInfo> &server_state_info = cluster_resource->get_server_state_info(new_ss_version);
        common::DRWLock &server_state_lock = cluster_resource->get_server_state_lock(new_ss_version);
        int err_no = 0;
        if (0 != (err_no = server_state_lock.try_rdlock())) {
          if (dummy_ldc_.is_empty()) {
            //treate it as is_base_servers_added is false
            is_base_servers_added = false;
          } else {
            need_ignore = true;
          }
          PROXY_CS_LOG(WDIAG, "fail to tryrdlock server_state_lock, ignore this update",
                       K(err_no),
                       "old_idc_name", dummy_ldc_.get_idc_name(),
                       K(new_idc_name),
                       "old_ss_version", server_state_version_,
                       "new_ss_version", new_ss_version,
                       "dummy_ldc_is_empty", dummy_ldc_.is_empty(),
                       K(cluster_resource->is_base_servers_added()),
                       K(is_base_servers_added), K(need_ignore), K(dummy_ldc_));
        } else {
          if (OB_FAIL(simple_servers_info.assign(server_state_info))) {
            PROXY_CS_LOG(WDIAG, "fail to assign servers_info_", K(ret));
          } else {
            server_state_version_ = new_ss_version;
          }
          server_state_lock.rdunlock();
        }
      }
      if (OB_SUCC(ret) && !need_ignore) {
        // ObLDCLocation::assign中会判断dummy_entry是否在集群的ss_info中，
        // 当定时任务刷新server_status/zone_status发生变化，且dummy_entry不在ss_info中时，
        // found_servers_added被置为true
        bool found_servers_changed = false;
        if (OB_FAIL(dummy_ldc_.assign(dummy_entry_->get_tenant_servers(), simple_servers_info,
            new_idc_name, is_base_servers_added, cluster_resource->get_cluster_name(),
            cluster_resource->get_cluster_id(), found_servers_changed))) {
          if (OB_EMPTY_RESULT == ret) {
            if (dummy_entry_->is_entry_from_rslist()) {
              set_need_delete_cluster();
              PROXY_CS_LOG(WDIAG, "tenant server from rslist is not match the server list, "
                           "need delete this cluster", KPC_(dummy_entry), K(ret));
            } else {
              //sys dummy entry can not set dirty
              if (!dummy_entry_->is_sys_dummy_entry() && dummy_entry_->cas_set_dirty_state()) {
                PROXY_CS_LOG(WDIAG, "tenant server is invalid, set it dirty", KPC_(dummy_entry), K(ret));
              }
            }
          } else {
            PROXY_CS_LOG(WDIAG, "fail to assign dummy_ldc", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_UNLIKELY(found_servers_changed)) {
          if (!dummy_entry_->is_sys_dummy_entry() && dummy_entry_->cas_set_dirty_state()) {
            PROXY_CS_LOG(WDIAG, "dummy_entry isn't AVAIL state, can't set it dirty", KPC_(dummy_entry), K(ret));
          }
        }
      }
      if (cluster_resource->is_base_servers_added()) {
        dummy_ldc_.set_safe_snapshot_manager(&cluster_resource->safe_snapshot_mgr_);
      }
    } else {
      PROXY_CS_LOG(DEBUG, "no need update dummy_ldc",
                   "old_idc_name", dummy_ldc_.get_idc_name(),
                   K(new_idc_name),
                   "old_ss_version", server_state_version_,
                   "new_ss_version", cluster_resource->server_state_version_,
                   "dummy_ldc_is_empty", dummy_ldc_.is_empty(),
                   "cluster_resource", cluster_resource->cluster_info_key_,
                   K(is_base_servers_added),
                   K(dummy_ldc_),
                   KPC_(dummy_entry));
    }
    allocator = NULL;
  }

  if (OB_UNLIKELY(need_dec_cr && NULL != cluster_resource)) {
    cluster_resource->dec_ref();
  }

  return ret;
}

bool ObMysqlClientSession::need_print_trace_stat() const
{
  return (is_proxy_mysql_client()
          && OB_NOT_NULL(inner_request_param_)
          && inner_request_param_->need_print_trace_stat_);
}

ObProxyProtocol ObMysqlClientSession::get_server_protocol() const
{
  ObProxyProtocol ret;
  if (OB_NOT_NULL(mysql_sm_)) {
    ret = mysql_sm_->get_server_protocol();
  } else {
    ret = ObProxyProtocol::PROTOCOL_MAX;
  }
  return ret;
}

int ObMysqlClientSessionMap::set(ObMysqlClientSession &cs)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(id_map_.unique_set(&cs))) {
    PROXY_CS_LOG(DEBUG, "succ to set client session", K(cs.get_cs_id()));
  } else {
    PROXY_CS_LOG(WDIAG, "fail to set client session", K(cs.get_cs_id()), KP(this), K(ret));
  }
  return ret;
}

int ObMysqlClientSessionMap::get(const uint32_t &id, ObMysqlClientSession *&cs)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(id_map_.get_refactored(id, cs))) {
    PROXY_CS_LOG(DEBUG, "succ to get client session", K(id), K(ret));
  } else {
    PROXY_CS_LOG(DEBUG, "fail to get client session", K(id), KP(this), K(ret));
  }
  return ret;
}

int ObMysqlClientSessionMap::erase(const uint32_t &id)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(id_map_.erase_refactored(id))) {
    ret = OB_SUCCESS;
  } else {
    PROXY_CS_LOG(WDIAG, "fail to erase client session", K(id), KP(this), K(ret));
  }
  return ret;
}

int init_cs_map_for_thread()
{
  int ret = OB_SUCCESS;
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_NET];
  for (int64_t i = 0; i < event_thread_count && OB_SUCC(ret); ++i) {
    if (OB_FAIL(init_cs_map_for_one_thread(i))) {
      PROXY_NET_LOG(WDIAG, "fail to new ObMysqlClientSessionMap", K(i), K(ret));
    }
  }
  return ret;
}

int init_cs_map_for_one_thread(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][index]->cs_map_ = new (std::nothrow) ObMysqlClientSessionMap())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WDIAG, "fail to new ObMysqlClientSessionMap", K(index), K(ret));
  }
  return ret;
}

int init_cs_map_for_one_thread(event::ObEThread *thread)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(thread)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WDIAG, "unexpected thread", K(ret));
  } else if (OB_ISNULL(thread->cs_map_ = new (std::nothrow) ObMysqlClientSessionMap())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WDIAG, "fail to new ObMysqlClientSessionMap", K(ret));
  }
  return ret;
}

int init_cs_id_list_for_thread()
{
  int ret = OB_SUCCESS;
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_NET];
  for (int64_t i = 0; i < event_thread_count && OB_SUCC(ret); ++i) {
    if (OB_FAIL(init_cs_id_list_for_one_thread(i))) {
      PROXY_NET_LOG(WDIAG, "fail to new ObClientSessionIDList", K(i), K(ret));
    }
  }
  return ret;
}

int init_cs_id_list_for_one_thread(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][index]->cs_id_list_
                = new (std::nothrow) ObClientSessionIDList())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WDIAG, "fail to new ObClientSessionIDList", K(index), K(ret));
  }
  return ret;
}

int init_cs_id_list_for_one_thread(event::ObEThread *thread)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(thread)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WDIAG, "unexpected thread", K(ret));
  } else if (OB_ISNULL(thread->cs_id_list_ = new (std::nothrow) ObClientSessionIDList())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WDIAG, "fail to new ObClientSessionIDList", K(ret));
  }
  return ret;
}

int init_random_seed_for_thread()
{
  int ret = OB_SUCCESS;
  //1. create init random seed
  ObMysqlRandom *init_seed = new (std::nothrow) ObMysqlRandom();
  if (OB_ISNULL(init_seed)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WDIAG, "fail to new ObMysqlRandom", K(ret));
  } else {
    const uint64_t current_time = static_cast<uint64_t>(get_hrtime_internal());
    init_seed->init(current_time, current_time / 2);
  }

  //2. create random seed of each ethread
  if (OB_SUCC(ret)) {
    const int64_t count = g_event_processor.thread_count_for_type_[ET_NET];
    ObMysqlRandom *tmp_random = NULL;
    for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(tmp_random = new (std::nothrow) ObMysqlRandom())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_NET_LOG(WDIAG, "fail to new ObMysqlRandom", K(i), K(ret));
      } else {
        const uint64_t tmp = init_seed->get_uint64();
        tmp_random->init(tmp + reinterpret_cast<uint64_t>(tmp_random),
                         tmp + static_cast<uint64_t>(g_event_processor.event_thread_[ET_NET][i]->tid_));
        g_event_processor.event_thread_[ET_NET][i]->random_seed_ = tmp_random;
      }
    }
  }

  if (OB_LIKELY(NULL != init_seed)) {
    delete init_seed;
  }
  return ret;
}

int init_random_seed_for_one_thread(int64_t index)
{
  int ret = OB_SUCCESS;
  //1. create init random seed
  ObMysqlRandom *init_seed = new (std::nothrow) ObMysqlRandom();
  if (OB_ISNULL(init_seed)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WDIAG, "fail to new ObMysqlRandom", K(ret));
  } else {
    const uint64_t current_time = static_cast<uint64_t>(get_hrtime_internal());
    init_seed->init(current_time, current_time / 2);
  }

  //2. create random seed of each ethread
  if (OB_SUCC(ret)) {
    ObMysqlRandom *tmp_random = NULL;
    if (OB_ISNULL(tmp_random = new (std::nothrow) ObMysqlRandom())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PROXY_NET_LOG(WDIAG, "fail to new ObMysqlRandom", K(index), K(ret));
    } else {
      const uint64_t tmp = init_seed->get_uint64();
      tmp_random->init(tmp + reinterpret_cast<uint64_t>(tmp_random),
                       tmp + static_cast<uint64_t>(g_event_processor.event_thread_[ET_NET][index]->tid_));
      g_event_processor.event_thread_[ET_NET][index]->random_seed_ = tmp_random;
    }
  }

  if (OB_LIKELY(NULL != init_seed)) {
    delete init_seed;
  }
  return ret;
}

int init_random_seed_for_one_thread(event::ObEThread *thread)
{
  int ret = OB_SUCCESS;
  ObMysqlRandom *init_seed = new (std::nothrow) ObMysqlRandom();
  if (OB_ISNULL(thread)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WDIAG, "unexpected thread", K(ret));
  } else if (OB_ISNULL(init_seed)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WDIAG, "fail to new ObMysqlRandom", K(ret));
  } else {
    const uint64_t current_time = static_cast<uint64_t>(get_hrtime_internal());
    init_seed->init(current_time, current_time / 2);
  }

  // 2. create random seed of each ethread
  if (OB_SUCC(ret)) {
    ObMysqlRandom *tmp_random = NULL;
    if (OB_ISNULL(tmp_random = new (std::nothrow) ObMysqlRandom())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PROXY_NET_LOG(WDIAG, "fail to new ObMysqlRandom", K(ret));
    } else {
      const uint64_t tmp = init_seed->get_uint64();
      tmp_random->init(tmp + reinterpret_cast<uint64_t>(tmp_random),
                       tmp + static_cast<uint64_t>(thread->tid_));
      thread->random_seed_ = tmp_random;
    }
  }

  if (OB_LIKELY(NULL != init_seed)) {
    delete init_seed;
  }
  return ret;
}

inline bool is_proxy_conn_id_avail(const uint64_t conn_id)
{
  bool bret = true;
  int ret = OB_SUCCESS;
  uint32_t thread_init_cs_id = 0;
  uint32_t max_local_seq = 0;
  if (OB_FAIL(ObMysqlClientSession::get_thread_init_cs_id(
          static_cast<ObClientSessionIDVersion>(get_global_proxy_config().client_session_id_version.get_value()),
          thread_init_cs_id, max_local_seq,
          g_event_processor.thread_count_for_type_[ET_NET] - 1))) {
    bret = false;
    PROXY_CS_LOG(WDIAG, "fail to  is get thread init cs id", K(bret), K(ret));
  } else {
    bret = conn_id <= (thread_init_cs_id | max_local_seq);
  }
  return bret;
}

inline bool is_server_conn_id_avail(const uint64_t conn_id)
{
  const uint64_t min_observer_session_id = 0x80000000;
  const uint64_t max_observer_session_id = 0xFFFFFFFF;
  return (min_observer_session_id <= conn_id && conn_id <= max_observer_session_id);
}

int extract_thread_id(const uint32_t cs_id, int64_t &thread_id)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != extract_thread_id_v2(cs_id, thread_id)) {
    PROXY_CS_LOG(INFO, "extract thread id from v2 cs_id failed, maybe the cs_id generated from v1", K(cs_id));
    thread_id = -1;
    if (OB_FAIL(extract_thread_id_v1(cs_id, thread_id))) {
      PROXY_CS_LOG(WDIAG, "fail to extract thread_id", K(cs_id));
    }
  }
  return ret;
}

int extract_thread_id_v1(const uint32_t cs_id, int64_t &thread_id)
{
  int ret = OB_SUCCESS;
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();

  const uint32_t proxy_heads_bits   = 10; //proxy mark + proxy id + upgrade version
  const uint32_t upgrade_ver_offset = 32 - proxy_heads_bits;
  const uint32_t thread_id_bits     = get_thread_id_bits();
  const uint32_t cs_upgrade_ver     = (cs_id & (0x1 << upgrade_ver_offset)) >> upgrade_ver_offset;
  const uint32_t target_upgrade_ver = static_cast<uint32_t>(0x1 & info.upgrade_version_);

  thread_id = -1;
  if (cs_upgrade_ver != target_upgrade_ver) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WDIAG, "error upgrade version, it maybe other proxy conn id", K(cs_upgrade_ver),
             K(target_upgrade_ver), "upgrade_version", info.upgrade_version_, K(ret));
  } else {
    const uint32_t thread_id_tmp = ((cs_id << proxy_heads_bits) >> (32 - thread_id_bits));
    if (thread_id_tmp >= static_cast<uint32_t>(g_event_processor.thread_count_for_type_[ET_NET])) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CS_LOG(WDIAG, "error thread id, it should not happened", K(thread_id_tmp), K(ret));
    } else {
      thread_id = static_cast<int64_t>(thread_id_tmp);
    }
  }
  return ret;
}

int extract_thread_id_v2(const uint32_t cs_id, int64_t &thread_id)
{
  int ret = OB_SUCCESS;
  const uint32_t proxy_head_bits = 15; //proxy mark + proxy id(13) + upgrade version
  const uint32_t thread_id_bits  = get_thread_id_bits();
  // don't need to check upgrade version for unique cs id

  thread_id = -1;
  const uint32_t thread_id_tmp = ((cs_id << proxy_head_bits) >> (32 - thread_id_bits));
  if (thread_id_tmp >= static_cast<uint32_t>(g_event_processor.thread_count_for_type_[ET_NET])) {
    // two case:
    // 1. the cs_id generated with CLIENT_SESSION_ID_V1
    // 2. the cs_id is incorrect
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(INFO, "error thread id, it should not happened", K(cs_id), K(thread_id_tmp), K(ret));
  } else {
    // check if cs_id exist
    bool is_cs_id_exist = false;
    if (OB_FAIL(g_event_processor.event_thread_[ET_NET][thread_id_tmp]->cs_id_list_->is_cs_id_exist(cs_id, is_cs_id_exist))) {
      PROXY_CS_LOG(WDIAG, "fail to check unique cs_id if exist", K(ret), K(cs_id));
    } else if (is_cs_id_exist) {
      thread_id = static_cast<int64_t>(thread_id_tmp);
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  return ret;
}

void extract_proxy_id_v2(const uint32_t cs_id, uint32_t &proxy_id)
{
  const uint32_t proxy_flag_bits = 1;
  const uint32_t proxy_id_bits = 13;
  proxy_id = ((cs_id << proxy_flag_bits) >> (32-proxy_id_bits));
}

bool is_conn_id_avail(const int64_t conn_id, bool &is_proxy_conn_id)
{
  bool bret = false;
  if (conn_id >= 0) {
    const uint64_t observer_session_id_maks = 0x80000000;
    const uint64_t tmp_conn_id = static_cast<uint64_t>(conn_id);
    if (get_global_proxy_config().is_client_service_mode()
        && ((tmp_conn_id & observer_session_id_maks) == observer_session_id_maks)) {
      bret = is_server_conn_id_avail(tmp_conn_id);
      is_proxy_conn_id = false;
    } else {
      bret = is_proxy_conn_id_avail(tmp_conn_id);
      is_proxy_conn_id = true;
    }
  }
  return bret;
}

void ObMysqlClientSession::record_sess_killed(uint32_t cs_id) {
  if (mysql_sm_ != NULL) {
    if (cs_id == cs_id_) {
      COLLECT_INTERNAL_DIAGNOSIS(
          mysql_sm_->connection_diagnosis_trace_, OB_PROXY_INTERNAL_TRACE,
          OB_ERR_QUERY_INTERRUPTED,
          "connection was killed by user self, cs_id: %d", cs_id);
    } else {
      COLLECT_INTERNAL_DIAGNOSIS(
          mysql_sm_->connection_diagnosis_trace_, OB_PROXY_INTERNAL_TRACE,
          OB_ERR_QUERY_INTERRUPTED, "connection was killed by user session %d", cs_id);
    }
  }
}

int ObMysqlClientSession::fill_tenant_info_with_ppv2(proxy_protocol_v2::ProxyProtocolV2 &v2)
{
  int ret = OB_SUCCESS;
  ct_info_.reset();
  ObVipAddr &addr = ct_info_.vip_tenant_.vip_addr_;

  struct sockaddr_storage ss = v2.src_addr_.get_sockaddr();
  client_vc_->set_real_client_addr(ss);
  if (v2.vpc_info_.empty()) {
    addr.set(ops_ip_sa_cast(v2.dst_addr_.get_sockaddr()), 0);
  } else {
    ObString tmp_str(static_cast<int32_t>(v2.vpc_info_.len()), v2.vpc_info_.ptr());
    addr.set(tmp_str);
  }

  if (OB_FAIL(fetch_tenant_by_vip())) {
    PROXY_CS_LOG(WDIAG, "fail to fetch tenant by vip", K(v2), K(ret));
  } else {
    // 需要用新的vip信息获取多级别配置
    mysql_sm_->set_need_update_config(true);
    mysql_sm_->trans_state_.refresh_mysql_config();
  }

  return ret;
}

int ObMysqlClientSession::get_vip_info()
{
  int ret = OB_SUCCESS;
  // temporary trace for diagnosis new_connection;
  const ObAddr &client_addr = get_real_client_addr();
  if (is_need_convert_vip_to_tname()) {
    if (OB_FAIL(fetch_tenant_by_vip())) {
      PROXY_CS_LOG(WDIAG, "fail to fetch tenant by vip", K(ret));
      ret = OB_SUCCESS;
    } else if (is_vip_lookup_success()) {
      ObString user_name;
      if (!get_global_white_list_table_processor().can_ip_pass(
            ct_info_.vip_tenant_.cluster_name_, ct_info_.vip_tenant_.tenant_name_,
            user_name, client_vc_->get_real_client_addr())) {
        ret = OB_ERR_CAN_NOT_PASS_WHITELIST;
        if (mysql_sm_ != NULL) {
          COLLECT_LOGIN_DIAGNOSIS(
              mysql_sm_->connection_diagnosis_trace_, OB_LOGIN_DISCONNECT_TRACE, "",
              OB_ERR_CAN_NOT_PASS_WHITELIST,
              "virtual ip user %.*s@%.*s#%.*s can not pass whitelist",
              user_name.length(), user_name.ptr(), ct_info_.vip_tenant_.tenant_name_.length(),
              ct_info_.vip_tenant_.tenant_name_.ptr(), ct_info_.vip_tenant_.cluster_name_.length(),
              ct_info_.vip_tenant_.cluster_name_.ptr());
        }
        PROXY_CS_LOG(DEBUG, "can not pass white_list", K(ct_info_.vip_tenant_.cluster_name_),
            K(ct_info_.vip_tenant_.tenant_name_), K(client_addr), K(ret));
      }
    }
  }

  return ret;
}

// overwrite exist value directly
// we should lock it, because using_cs_id_set_ may operated by other threads
// in kill proxysession or show proxysession
int ObClientSessionIDList::record_cs_id(const uint32_t cs_id)
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(lock_);
  if (OB_FAIL(using_cs_id_set_.set_refactored(cs_id))) {
    PROXY_LOG(WDIAG, "fail to record unique cs_id and thread_id", K(cs_id));
  }
  return ret;
}

// we should lock it because using_cs_id_set_ may operated by other threads
// in kill proxysession or show proxysession
int ObClientSessionIDList::is_cs_id_exist(const uint32_t cs_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(lock_);
  if (OB_FAIL(using_cs_id_set_.exist_refactored(cs_id))) {
    if (ret == OB_HASH_EXIST) {
      ret = OB_SUCCESS;
      is_exist = true;
    } else if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      is_exist = false;
    } else {
      PROXY_LOG(WDIAG, "fail to check if cs_id exist", K(cs_id), K(ret));
    }
  } else {
    is_exist = false;
  }
  return ret;
}

// we should lock it, because using_cs_id_set_ may operated by other threads
// in kill proxysession or show proxysession
int ObClientSessionIDList::erase_cs_id(const uint32_t cs_id)
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(lock_);
  if (OB_FAIL(using_cs_id_set_.erase_refactored(cs_id))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
    } else {
      PROXY_LOG(WDIAG, "fail to erase cs_id record", K(cs_id), K(ret));
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase