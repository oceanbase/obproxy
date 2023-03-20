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
    : can_direct_ok_(false), is_proxy_mysql_client_(false), can_direct_send_request_(false),
      active_(false), test_server_addr_(),
      vc_ready_killed_(false), is_waiting_trans_first_request_(false),
      is_need_send_trace_info_(true), is_already_send_trace_info_(false),
      is_first_handle_request_(true), is_in_trans_for_close_request_(false), is_last_request_in_trans_(false),
      is_trans_internal_routing_(false), is_need_return_last_bound_ss_(false), need_delete_cluster_(false),
      is_first_dml_sql_got_(false), is_proxy_enable_trans_internal_routing_(false), compressed_seq_(0),
      cluster_resource_(NULL), dummy_entry_(NULL), is_need_update_dummy_entry_(false),
      dummy_ldc_(), dummy_entry_valid_time_ns_(0), server_state_version_(0),
      inner_request_param_(NULL), tcp_init_cwnd_set_(false), half_close_(false),
      conn_decrease_(false), conn_prometheus_decrease_(false), vip_connection_decrease_(false),
      magic_(MYSQL_CS_MAGIC_DEAD), create_thread_(NULL), is_local_connection_(false),
      client_vc_(NULL), in_list_stat_(LIST_INIT), current_tid_(-1),
      cs_id_(0), proxy_sessid_(0), bound_ss_(NULL), cur_ss_(NULL), lii_ss_(NULL), last_bound_ss_(NULL),
      trans_coordinator_ss_addr_(), read_buffer_(NULL),
      buffer_reader_(NULL), mysql_sm_(NULL), read_state_(MCS_INIT), ka_vio_(NULL),
      server_ka_vio_(NULL), trace_stats_(NULL), select_plan_(NULL),
      ps_id_(0), cursor_id_(CURSOR_ID_START), using_ldg_(false)
{
  SET_HANDLER(&ObMysqlClientSession::main_handler);
  bool enable_session_pool = get_global_proxy_config().is_pool_mode
                             && get_global_proxy_config().enable_session_pool_for_no_sharding;
  set_session_pool_client(enable_session_pool);
  can_server_session_release_ = true;
}

void ObMysqlClientSession::destroy()
{
  PROXY_CS_LOG(INFO, "client session destroy", K_(cs_id), K_(proxy_sessid), KP_(client_vc));

  if (OB_UNLIKELY(NULL != client_vc_)
      || OB_UNLIKELY(NULL != bound_ss_)
      || OB_ISNULL(read_buffer_)) {
    PROXY_CS_LOG(WARN, "invalid client session", K(client_vc_), K(bound_ss_), K(read_buffer_));
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

  // here need place before session_info_.destroy, because use some session_info's data
  if (conn_prometheus_decrease_) {
    SESSION_PROMETHEUS_STAT(session_info_, PROMETHEUS_CURRENT_SESSION, true, -1);
    SESSION_PROMETHEUS_STAT(session_info_, PROMETHEUS_USED_CONNECTIONS, -1);
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

  is_need_send_trace_info_ = true;
  is_already_send_trace_info_ = false;
  is_first_handle_request_ = true;
  is_in_trans_for_close_request_ = false;
  is_last_request_in_trans_ = false;
  is_trans_internal_routing_ = false;
  is_need_return_last_bound_ss_ = false;
  is_first_dml_sql_got_ = false;
  is_proxy_enable_trans_internal_routing_ = false;
  compressed_seq_ = 0;
  trans_coordinator_ss_addr_.reset();
  schema_key_.reset();
  ObProxyClientSession::cleanup();
  create_thread_ = NULL;
  using_ldg_ = false;
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
    PROXY_CS_LOG(WARN, "invalid argument", K(cont), K(ret));
  } else if (OB_FAIL(ObProxyClientSession::ssn_hook_append(id, cont))) {
    PROXY_CS_LOG(WARN, "fail to append ssh hook", K(id), K(ret));
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
    PROXY_CS_LOG(WARN, "invalid argument", K(cont), K(ret));
  } else if (OB_FAIL(ObProxyClientSession::ssn_hook_prepend(id, cont))) {
    PROXY_CS_LOG(WARN, "fail to prepend ssn hook", K(id), K(ret));
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
    PROXY_CS_LOG(WARN, "sm should be null when start a new transaction", K(mysql_sm_), K(ret));
  } else {
    // Defensive programming, make sure nothing persists across
    // connection re-use
    half_close_ = false;

    read_state_ = MCS_ACTIVE_READER;
    if (OB_ISNULL(mysql_sm_ = ObMysqlSM::allocate())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PROXY_CS_LOG(ERROR, "fail to alloc memory for sm", K(ret));
    } else if (OB_FAIL(mysql_sm_->init())) {
      PROXY_CS_LOG(WARN, "fail to init sm", K_(cs_id), K(ret));
    } else {
      PROXY_CS_LOG(INFO, "Starting new transaction using sm", K_(cs_id),
                   K(get_transact_count()), "sm_id", mysql_sm_->sm_id_);

      client_vc_->remove_from_keep_alive_lru();
      if (OB_FAIL(mysql_sm_->attach_client_session(*this, *buffer_reader_, is_new_conn))) {
        // if fail to attach client session, mysql sm will handle the failure itself, so we just print a warning log and
        // need not to return error ret to the caller
        PROXY_CS_LOG(WARN, "fail to attach client session", K_(cs_id), K(ret));
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_FAIL(ret)) {
    PROXY_CS_LOG(WARN, "fail to start new transaction, will close client session", K_(cs_id), K(ret));
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
    PROXY_CS_LOG(WARN, "invalid client connection", K(new_vc), K(client_vc_), K(ret));
  } else {
    create_thread_ = this_ethread();
    client_vc_ = new_vc;
    magic_ = MYSQL_CS_MAGIC_ALIVE;
    mutex_ = new_vc->mutex_;
    session_manager_.set_mutex(mutex_);
    session_manager_new_.set_mutex(mutex_);
    MUTEX_TRY_LOCK(lock, mutex_, this_ethread());
    if (OB_LIKELY(lock.is_locked())) {
      current_tid_ = gettid();
      hooks_on_ = true;

      MYSQL_INCREMENT_DYN_STAT(CURRENT_CLIENT_CONNECTIONS);
      conn_decrease_ = true;
      if (is_proxy_mysql_client_) {
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
          PROXY_CS_LOG(ERROR, "fail to release mutex", K_(cs_id));
        }
      }
#endif

      if (NULL != iobuf) {
        read_buffer_ = iobuf;
      } else if (OB_ISNULL(read_buffer_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_CS_LOG(ERROR, "fail to alloc memory for read_buffer", K(ret));
      }

      if (OB_SUCC(ret)) {
        if (NULL != reader) {
          buffer_reader_ = reader;
        } else if (OB_ISNULL(buffer_reader_ = read_buffer_->alloc_reader())) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_CS_LOG(ERROR, "fail to alloc buffer reader", K(ret));
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
          PROXY_CS_LOG(WARN, "fail to start listen event on client vc, ka_vio is null", K(ret));
        } else if (OB_FAIL(acquire_client_session_id())) {
          PROXY_CS_LOG(WARN, "fail to acquire client session_id", K_(cs_id), K(ret));
        } else if (OB_FAIL(add_to_list())) {
          PROXY_CS_LOG(WARN, "fail to add cs to list", K_(cs_id), K(ret));
        } else if (OB_FAIL(session_info_.init())) {
          PROXY_CS_LOG(WARN, "fail to init session_info", K_(cs_id), K(ret));
        } else if (OB_FAIL(get_vip_addr())) {
          PROXY_CS_LOG(WARN, "get vip addr failed", K(ret));
        } else {
          const ObAddr &client_addr = get_real_client_addr();
          session_info_.set_client_host(client_addr);
          set_local_connection();
          PROXY_CS_LOG(INFO, "client session born", K_(cs_id), K_(proxy_sessid), K_(is_local_connection), K_(client_vc),
                       "client_fd", client_vc_->get_conn_fd(), K(client_addr));

          // 1. first convert vip to tenant info, if needed.
          if (is_need_convert_vip_to_tname()) {
            if (OB_FAIL(fetch_tenant_by_vip())) {
              PROXY_CS_LOG(WARN, "fail to fetch tenant by vip", K(ret));
              ret = OB_SUCCESS;
            } else if (is_vip_lookup_success()) {
              ObString user_name;
              if (!get_global_white_list_table_processor().can_ip_pass(
                ct_info_.vip_tenant_.cluster_name_, ct_info_.vip_tenant_.tenant_name_,
                user_name, client_vc_->get_real_client_addr())) {
                ret = OB_ERR_CAN_NOT_PASS_WHITELIST;
                PROXY_CS_LOG(DEBUG, "can not pass white_list", K(ct_info_.vip_tenant_.cluster_name_),
                             K(ct_info_.vip_tenant_.tenant_name_), K(client_addr), K(ret));
              }
            }
          }

          // 2. handle_new_connection no matter convert vip to tenant result.
          if (OB_SUCC(ret)) {
            handle_new_connection();
          }
        }
      } // end if (OB_SUCC(ret))
    } else {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CS_LOG(WARN, "fail to try lock thread mutex, will close connection", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    PROXY_CS_LOG(WARN, "fail to do new connection, do_io_close itself", K_(cs_id), K(ret));
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
    PROXY_CS_LOG(WARN, "get proxy tenant name config failed", K(addr), K(ret));
  } 

  if (OB_SUCC(ret) && found) {
    if (OB_FAIL(get_global_config_processor().get_proxy_config_with_level(
      addr, "", "", "rootservice_cluster_name", cluster_item, "LEVEL_VIP", found))) {
      PROXY_CS_LOG(WARN, "get cluster name config failed", K(addr), K(ret));
    }
  }

  if (OB_SUCC(ret) && found) {
    if (OB_FAIL(ct_info_.vip_tenant_.set_tenant_cluster(tenant_item.str(), cluster_item.str()))) {
      PROXY_CS_LOG(WARN, "set tenant and cluster name failed", K(tenant_item), K(cluster_item), K(ret));
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

  // TODO, get client ip, slb ip from kernal

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
      PROXY_CS_LOG(WARN, "fail to start hook, will close client session", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    do_io_close();
  }
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
int ObMysqlClientSession::acquire_client_session_id()
{
  static __thread uint32_t next_cs_id = 0;
  static __thread uint32_t thread_init_cs_id = 0;
  static __thread uint32_t max_local_seq = 0;

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == next_cs_id) && OB_FAIL(get_thread_init_cs_id(thread_init_cs_id, max_local_seq))) {
    PROXY_CS_LOG(WARN, "fail to  is get thread init cs id", K(next_cs_id), K(ret));
  }

  if (OB_SUCC(ret)) {
    uint32_t cs_id = ++next_cs_id;
    cs_id &= max_local_seq;         // set local seq
    cs_id |= thread_init_cs_id;     // set full cs id

    cs_id_ = cs_id;
  }
  return ret;
}

//connection id from obproxy
//|----1----|-----8-----|------1------|----------22-----------|
//|  MARKS  |  PROXY_ID | UPGRADE_VER |---1~N-----|----22-N---|
//|    0    |   1~255   |     0/1     | THREAD_ID | LOCAL_SEQ |
//|-------------thread init cs id-----------------|
//
int ObMysqlClientSession::get_thread_init_cs_id(uint32_t &thread_init_cs_id,
    uint32_t &max_local_seq, const int64_t thread_id/*-1*/)
{
  int ret = OB_SUCCESS;
  const uint32_t proxy_head_bits      = 9;//MARKS + PROXY_ID
  const uint32_t upgrade_ver_bits     = 1;
  const uint32_t thread_id_bits       = 32 - __builtin_clz(static_cast<uint32_t>(g_event_processor.thread_count_for_type_[ET_CALL] - 1));
  const uint32_t local_seq_bits       = 32 - proxy_head_bits - upgrade_ver_bits - thread_id_bits;

  const uint32_t proxy_id_offset      = 32 - proxy_head_bits;
  const uint32_t upgrade_ver_offset   = 32 - proxy_head_bits - upgrade_ver_bits;
  const uint32_t thread_id_offset     = local_seq_bits;

  const uint32_t proxy_id      = static_cast<uint32_t>(get_global_proxy_config().proxy_id);
  const uint32_t upgrade_ver   = static_cast<uint32_t>(0x1 & get_global_hot_upgrade_info().upgrade_version_); //only use the tail bits

  uint32_t tmp_thread_id = 0;
  if (thread_id < 0 || thread_id >= g_event_processor.thread_count_for_type_[ET_CALL]) {// use curr ethread
    ObEThread &ethread = self_ethread();
    tmp_thread_id = static_cast<uint32_t>(ethread.id_);
  } else {// use assigned ethread
    tmp_thread_id = static_cast<uint32_t>(thread_id);
  }
  max_local_seq = 0;
  thread_init_cs_id = 0;

  if (OB_SUCC(ret)) {
    thread_init_cs_id |= (proxy_id << proxy_id_offset);        // set proxy id
    thread_init_cs_id |= (upgrade_ver << upgrade_ver_offset);  // set upgrade version
    thread_init_cs_id |= (tmp_thread_id << thread_id_offset);  // set thread id
    max_local_seq      = (1 << local_seq_bits) - 1;
  }
  return ret;
}

int ObMysqlClientSession::add_to_list()
{
  int ret = OB_SUCCESS;
  if (is_proxy_mysql_client_) {
    //if it is is proxy mysql client, we do follow things:
    //1. do not add to list
    //2. set cs_id proxy_mark to 1
    //3. use observer session id in handshake as conn id
    //4. do not saved login for inner client vc
    cs_id_ |= 0x80000000;// set top bit to 1
    PROXY_CS_LOG(INFO, "proxy mysql client session born", K_(cs_id));
  } else if (OB_UNLIKELY(LIST_ADDED == in_list_stat_)) {
    ret = OB_ENTRY_EXIST;
    PROXY_CS_LOG(WARN, "cs had already in the list, it should not happened", K_(cs_id), K(ret));
  } else if (OB_ISNULL(mutex_->thread_holding_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WARN, "mutex_->thread_holding_ is null, it should not happened", K_(cs_id), K(ret));
  } else {
    const int64_t MAX_TRY_TIMES = 1000;
    ObMysqlClientSessionMap &cs_map = get_client_session_map(*mutex_->thread_holding_);
    for (int64_t i = 0; OB_SUCC(ret) && LIST_ADDED != in_list_stat_ && i < MAX_TRY_TIMES; ++i) {
      if (OB_FAIL(cs_map.set(*this))) {
        if (OB_LIKELY(OB_HASH_EXIST == ret)) {
          PROXY_CS_LOG(INFO, "repeat cs id, retry to acquire another one", K_(cs_id), K(client_vc_), K(ret));
          if (OB_FAIL(acquire_client_session_id())) {
            PROXY_CS_LOG(WARN, "fail to acquire client session_id", K_(cs_id), K(client_vc_), K(ret));
          }
        } else {
          PROXY_CS_LOG(WARN, "fail to set cs into cs_map", K_(cs_id), K(client_vc_), K(ret));
        }
      } else {
        in_list_stat_ = LIST_ADDED;
      }
    }
    if (OB_SUCC(ret) && LIST_ADDED != in_list_stat_) {
      ret = OB_SESSION_ENTRY_EXIST;
      PROXY_CS_LOG(WARN, "there is no enough cs id, close this connect", K_(cs_id), K(client_vc_), K(ret));
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
  // TODO: Consider IPv6 support
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
      PROXY_CS_LOG(WARN, "set_tcp_init_cwnd failed", K(desired_tcp_init_cwnd));
    }
  }
}

void ObMysqlClientSession::do_io_shutdown(const ShutdownHowToType howto)
{
  client_vc_->do_io_shutdown(howto);
}

void ObMysqlClientSession::do_io_close(const int alerrno)
{
  PROXY_SS_LOG(INFO, "client session do_io_close", K(*this), KP(client_vc_), KP(this));
  int ret = OB_SUCCESS;
  // Prevent double closing
  if (MCS_CLOSED != read_state_) {
    if (NULL != mysql_sm_) {
      mysql_sm_->set_detect_server_info(mysql_sm_->trans_state_.server_info_.addr_, -1, 0);
    }
    if (MCS_ACTIVE_READER == read_state_) {
      if (LIST_ADDED == in_list_stat_ || is_proxy_mysql_client_) {
        MYSQL_DECREMENT_DYN_STAT(CURRENT_CLIENT_TRANSACTIONS);
      }
      if (active_) {
        active_ = false;
        MYSQL_DECREMENT_DYN_STAT(CURRENT_ACTIVE_CLIENT_CONNECTIONS);
      }
    }

    // If we have an attached server session, release
    // it back to our shared pool
    if (NULL != bound_ss_) {
      bound_ss_->do_io_close();
      bound_ss_ = NULL;
      server_ka_vio_ = NULL;
    }

    if (NULL != last_bound_ss_) {
      last_bound_ss_->do_io_close();
      last_bound_ss_ = NULL;
    }

    // close all server sessions in server session manager
    session_manager_.purge_keepalives();
    session_manager_new_.purge_keepalives();

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
        PROXY_CS_LOG(WARN, "ka_vio_ is null", K(ret));
      }

      // Drain any data read.
      // If the buffer is full and the client writes again, we will not receive a
      // READ_READY event.
      if (OB_FAIL(buffer_reader_->consume(buffer_reader_->read_avail()))) {
        PROXY_CS_LOG(WARN, "fail to consume ", K(ret));
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
          PROXY_CS_LOG(WARN, "fail to schedule switch thread", K_(cs_id), K(ret));
        }
      } else {
        ObMysqlClientSessionMap &cs_map = get_client_session_map(*mutex_->thread_holding_);
        if (OB_FAIL(cs_map.erase(cs_id_))) {
          PROXY_CS_LOG(WARN, "current client session is not in table, no need to erase", K_(cs_id), K(ret));
        }
        in_list_stat_ = LIST_REMOVED;
      }
    }

    // in 2 situations we will delete cluster (cluster rslist and resource)
    // 1. all servers of table entry which comes from rslist are not in congestion list
    // 2. fail to verify cluster name in login step, user cluster and server cluster are not the same
    //
    // if we only delete resource, loacl cluster rslist still exist, so we must delete both of them
    if (OB_UNLIKELY(need_delete_cluster_)) {
      if (OB_FAIL(handle_delete_cluster())) {
        PROXY_CS_LOG(WARN, "fail to handle delete cluster", K(ret));
      }
      need_delete_cluster_ = false;
    }

    if ((this_ethread() == create_thread_)
        || is_proxy_mysql_client_ // if proxy mysql client, direct destroy, no need scheudle to create thread
        ) {
      read_state_ = MCS_CLOSED;
      if (OB_FAIL(do_api_callout(OB_MYSQL_SSN_CLOSE_HOOK))) {
        PROXY_CS_LOG(WARN, "fail to close hook, destroy itself", K(ret));
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
      PROXY_CS_LOG(WARN, "unknown event", K(event));
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
    PROXY_CS_LOG(WARN, "fail to delete cluster rslist", K(name), K(cr_id), K(ret));
  }

  //2. delete cluster resource in resource_pool
  ObResourcePoolProcessor &rpp = get_global_resource_pool_processor();
  if (name == OB_META_DB_CLUSTER_NAME) {
    const bool ignore_cluster_not_exist = true;
    if (OB_FAIL(rpp.rebuild_metadb(ignore_cluster_not_exist))) {
      PROXY_CS_LOG(WARN, "fail to rebuild metadb cluster resource", K(ret));
    }
  } else {
    if (OB_FAIL(rpp.delete_cluster_resource(name, cr_id))) {
      PROXY_CS_LOG(WARN, "fail to delete cluster resource", K(name), K(cr_id), K(ret));
    }
  }
  return ret;
}

int ObMysqlClientSession::state_server_keep_alive(int event, void *data)
{
  STATE_ENTER(&ObMysqlClientSession::state_server_keep_alive, event, data);
  if (OB_LIKELY(data == server_ka_vio_) && OB_LIKELY(NULL != bound_ss_)) {
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
        bound_ss_->do_io_close();
        bound_ss_ = NULL;
        server_ka_vio_ = NULL;
        break;

      case VC_EVENT_READ_COMPLETE:
      default:
        // These events are bogus
        PROXY_CS_LOG(WARN, "invalid event", K(event));
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
            PROXY_CS_LOG(WARN, "fail to consume ", K(ret));
          }
        } else {
          // only after obproxy has sent handshake packet to client, client will send data to obproxy;
          // so in that case, client session must has been inited completed.
          // New transaction, need to spawn of new sm to process request
          if (OB_FAIL(new_transaction())) {
            PROXY_CS_LOG(WARN, "fail to start new transaction", K(ret));
          }
        }
        break;
      }
      case VC_EVENT_EOS: {
        PROXY_CS_LOG(WARN, "client session received VC_EVENT_EOS event",
                     K_(cs_id), K_(read_state));
        if (MCS_HALF_CLOSED == read_state_) {
          half_close_ = false;
          do_io_close();
        } else {
          // If there is data in the buffer, start a new
          // transaction, otherwise the client gave up
          if (buffer_reader_->read_avail() > 0) {
            if (OB_FAIL(new_transaction())) {
              PROXY_CS_LOG(WARN, "fail to start new transaction", K(ret));
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
              PROXY_CS_LOG(WARN, "fail to ops_ip_copy client_ip", K(client_vc_));
            }
          }

          PROXY_CS_LOG(WARN, "client connection is idle over wait_timeout, now we will close it.",
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
      PROXY_CS_LOG(WARN, "invalid event", K(event));
      break;
    }
  }
  return VC_EVENT_NONE;
}

void ObMysqlClientSession::close_last_used_ss()
{
  if (NULL != bound_ss_) {
    if (is_session_pool_client() && can_server_session_release_) {
      PROXY_CS_LOG(DEBUG, "is_session_pool_client will release");
      bound_ss_->release();
    } else {
      bound_ss_->do_io_close();
    }
    bound_ss_ = NULL;
    server_ka_vio_ = NULL;
  }
}

int ObMysqlClientSession::swap_mutex(void *data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WARN, "mutex data cannot be null here", K(ret));
  } else if (mutex_ != data) {
    ObProxyMutex *mutex = reinterpret_cast<ObProxyMutex *>(data);
    // swap client session mutex
    mutex_ = mutex;

    //swap server session mutex
    if (NULL != bound_ss_) {
      bound_ss_->do_io_read(this, INT64_MAX, bound_ss_->read_buffer_);
      bound_ss_->do_io_write(this, 0, NULL);
    }

    if (NULL != mysql_sm_ && OB_FAIL(mysql_sm_->swap_mutex(mutex))) {
      PROXY_CS_LOG(WARN, "failed to swap mysql sm mutex", K(ret));
    }

    if (OB_SUCC(ret)) {
      //swap session manager mutex
      // only in async client vc, we can swap mutex, in such situation,
      // server_session_pool must be empty
      if (is_session_pool_client()) {
        PROXY_CS_LOG(DEBUG, "is_session_pool_client, do nothing");
      } else if (OB_LIKELY(0 == session_manager_.get_svr_session_count()
                    && 0 == session_manager_new_.get_svr_session_count())) {
        session_manager_.set_mutex(mutex);
        session_manager_new_.set_mutex(mutex);
      } else {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CS_LOG(WARN, "server session pool is not empty", K(ret));
      }
    }
  }
  return ret;
}

void ObMysqlClientSession::reenable(ObVIO *vio)
{
  client_vc_->reenable(vio);
}

int ObMysqlClientSession::attach_server_session(ObMysqlServerSession *session)
{
  int ret = OB_SUCCESS;
  if (NULL != session) {
    if (OB_UNLIKELY(NULL != bound_ss_) || OB_UNLIKELY(session != cur_ss_)
        || OB_UNLIKELY(0 != session->get_reader()->read_avail())
        || OB_UNLIKELY(session->get_netvc() == client_vc_)) {
      ret = OB_INVALID_ARGUMENT;
      PROXY_CS_LOG(WARN, "invalid server session", K(bound_ss_), K(session), K(cur_ss_),
                   K(session->get_reader()->read_avail()), K(session->get_netvc()), K(client_vc_));
    } else {
      session->state_ = MSS_KA_CLIENT_SLAVE;
      bound_ss_ = session;
      //reset cur_session_
      cur_ss_ = NULL;
      PROXY_CS_LOG(DEBUG, "attaching server session as slave", K_(cs_id), "ss_id", session->ss_id_);
      // reset server read buffer water mark
      session->get_reader()->mbuf_->water_mark_ = MYSQL_NET_META_LENGTH;
      // handling potential keep-alive here
      if (active_) {
        active_ = false;
        MYSQL_DECREMENT_DYN_STAT(CURRENT_ACTIVE_CLIENT_CONNECTIONS);
      }
      // Since this our slave, issue an IO to detect a close and
      // have it call the client session back.  This IO also prevent
      // the server net connection from calling back a dead sm
      if (OB_LIKELY(ka_vio_ != (server_ka_vio_ = session->do_io_read(this, INT64_MAX, session->read_buffer_)))) {
        // Transfer control of the write side as well
        session->do_io_write(this, 0, NULL);
        session->set_inactivity_timeout(session_info_.get_wait_timeout());
      }
    }
  } else {
    bound_ss_ = NULL;
    server_ka_vio_ = NULL;
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
    } else if (NULL != data && data == server_ka_vio_) { // from server vc
      event_ret = state_server_keep_alive(event, data);
    } else if (CLIENT_VC_SWAP_MUTEX_EVENT == event) { // from proxy client vc
      int ret = OB_SUCCESS;
      if (OB_FAIL(swap_mutex(data))) {
        PROXY_CS_LOG(WARN, "fail to swap mutex", KP(data), K(ret));
      }
    } else if (is_proxy_mysql_client_ && CLIENT_VC_DISCONNECT_LAST_USED_SS_EVENT == event) {
      close_last_used_ss();
    } else {
      event_ret = (this->*cs_default_handler_)(event, data); // others
    }
  } else {
    PROXY_CS_LOG(WARN, "unexpected magic, expected MYSQL_CS_MAGIC_ALIVE", K(magic_));
  }

  return event_ret;
}

void ObMysqlClientSession::handle_transaction_complete(ObIOBufferReader *r, bool &close_cs)
{
  int ret = OB_SUCCESS;
  close_cs = false;
  is_waiting_trans_first_request_ = true;
  if (OB_LIKELY(MCS_ACTIVE_READER == read_state_) && OB_LIKELY(NULL != mysql_sm_)) {
    PROXY_CS_LOG(DEBUG, "client session handle transaction complete",
                 K_(cs_id), K(mysql_sm_->sm_id_), KPC_(cluster_resource));
    if (OB_UNLIKELY(get_global_proxy_config().is_metadb_used() && mysql_sm_->trans_state_.mysql_config_params_->enable_report_session_stats_)) {
      update_session_stats();
    }

    // Make sure that the state machine is returning correct buffer reader
    if (OB_UNLIKELY(r != buffer_reader_)) {
      PROXY_CS_LOG(WARN, "buffer reader mismatch, will close client session",
                   K(r), K(buffer_reader_));
      close_cs = true;
    } else if (OB_UNLIKELY(!get_global_hot_upgrade_info().need_conn_accept_) && OB_UNLIKELY(need_close())) {
      close_cs = true;
      PROXY_CS_LOG(INFO, "receive exit cmd, obproxy will exit, now close client session",
                   K(*this));
    // here only check in non-sharding mode.
    // in sharding mode, will switch cluster and set to NULL when use db
    } else if (OB_LIKELY(!session_info_.is_sharding_user() && session_info_.is_oceanbase_server())) {
      if ((OB_ISNULL(cluster_resource_) || OB_UNLIKELY(cluster_resource_->is_deleting())) && !is_proxysys_tenant()) {
        if (NULL != cluster_resource_) {
          PROXY_CS_LOG(INFO, "the cluster resource is deleting, client session will close",
                       KPC_(cluster_resource), K_(cluster_resource), K_(is_proxy_mysql_client));
        }
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
        PROXY_CS_LOG(WARN, "fail to check sys variable", K_(cs_id), K(ret));
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
          PROXY_CS_LOG(WARN, "fail to start new transaction", K(ret));
        }
      } else {
        read_state_ = MCS_KEEP_ALIVE;
        ka_vio_ = do_io_read(this, INT64_MAX, read_buffer_);
        if (OB_LIKELY(server_ka_vio_ != ka_vio_)) {
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
  if (is_proxy_mysql_client_) {
    //pool_client schema_key will set in client_vc
  } else if (!session_info_.is_sharding_user() && schema_key_.init_) {
    PROXY_CS_LOG(DEBUG, "no sharding already init", K(schema_key_));
  } else if (OB_FAIL(ObMysqlSessionUtils::init_schema_key_with_client_session(schema_key_, this))) {
    PROXY_CS_LOG(WARN, "init_schema_key_with_client_session failed", K(ret));
  }
  return ret;
}

int ObMysqlClientSession::acquire_svr_session_in_session_pool(const sockaddr &addr, ObMysqlServerSession *&svr_session)
{
  int ret = OB_SUCCESS;
  PROXY_CS_LOG(DEBUG, "[acquire server session] try to acquire session in session pool", K_(cs_id), K_(schema_key));
  ObShardConnector *shard_conn = session_info_.get_shard_connector();
  ObCommonAddr common_addr;
  if (shard_conn != NULL && common::DB_MYSQL == shard_conn->server_type_ && !shard_conn->is_physic_ip_) {
    if (OB_FAIL(common_addr.assign(shard_conn->physic_addr_.config_string_,
      shard_conn->physic_port_.config_string_, shard_conn->is_physic_ip_))) {
      PROXY_CS_LOG(WARN,"assign addr faield", K(shard_conn->physic_addr_.config_string_),
        K(shard_conn->physic_port_.config_string_), K(ret));
    }
  } else if (OB_FAIL(common_addr.assign(addr))) {
    PROXY_CS_LOG(WARN, "assign addr failed", K(ret));
  }
  if (OB_SUCC(ret) && OB_SUCC(get_global_session_manager().acquire_server_session(
    schema_key_,
    common_addr,
    session_info_.get_full_username(),
    svr_session))) {
    PROXY_CS_LOG(DEBUG, "[acquire server session] succ to acquire session in global session pool", K_(cs_id),
      K(session_info_.get_login_req().get_hsr_result().full_name_),
      K(schema_key_),
      K(common_addr));
  } else {
    PROXY_CS_LOG(DEBUG, "[acquire server session] fail to acquire session in global session pool", K_(cs_id),
      K(session_info_.get_login_req().get_hsr_result().full_name_),
      K(schema_key_), K(common_addr));
  }
  return ret;
}

int ObMysqlClientSession::acquire_svr_session_no_pool(const sockaddr &addr, ObMysqlServerSession *&svr_session)
{
  int ret = OB_SUCCESS;
  PROXY_CS_LOG(DEBUG, "[acquire server session] try to acquire session in session pool", K_(cs_id));
  ObShardConnector *shard_conn = session_info_.get_shard_connector();
  // if shard_conn not null, need use shard_conn
  if (OB_UNLIKELY(NULL != shard_conn)) {
    if (OB_FAIL(session_manager_new_.acquire_server_session(shard_conn->shard_name_.config_string_,
                                                            addr, session_info_.get_full_username(), svr_session))) {
      PROXY_CS_LOG(DEBUG, "[acquire server session] fail to acquire server session from "
                          "new server session pool", K_(cs_id), KPC(svr_session), K(ret));
    }
  } else {
    if (OB_FAIL(session_manager_.acquire_server_session(addr, session_info_.get_full_username(), svr_session))) {
      PROXY_CS_LOG(DEBUG, "[acquire server session] fail to acquire server session from "
                          "server session pool", K_(cs_id), KPC(svr_session), K(ret));
    }
  }
  return ret;
}

int ObMysqlClientSession::acquire_svr_session(const sockaddr &addr, const bool need_close_last_ss, ObMysqlServerSession *&svr_session)
{
  int ret = OB_SUCCESS;
  svr_session = NULL;
  PROXY_CS_LOG(DEBUG, "[acquire server session]", K_(cs_id));

  if (is_session_pool_client()) {
    if (OB_FAIL(init_session_pool_info())) {
      PROXY_CS_LOG(WARN, "init_session_pool_info failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // 1. try last_session
    if (NULL != bound_ss_) {
      bool same_dbkey = true;
      if (is_session_pool_client() &&
        (schema_key_.dbkey_.config_string_.compare(bound_ss_->schema_key_.dbkey_.config_string_) != 0)) {
        same_dbkey = false;
      }

      if (same_dbkey && ops_ip_addr_port_eq(bound_ss_->server_ip_, addr)
          && bound_ss_->auth_user_ == session_info_.get_full_username()) {
        svr_session = bound_ss_;
        PROXY_CS_LOG(DEBUG, "[acquire server session] use last server session", K_(cs_id), "server_ip", bound_ss_->server_ip_);
      } else {
        // Release this session back to the main session pool and
        // then continue looking for one from the shared pool
        // if is mysql client, close last session
        if (is_proxy_mysql_client_ && !is_session_pool_client()) {
          bound_ss_->do_io_close();
          bound_ss_ = NULL;
          ret = OB_SESSION_NOT_FOUND;
        } else if (need_close_last_ss) {
          bound_ss_->do_io_close();
          bound_ss_ = NULL;
        } else {
          bound_ss_->release();
          bound_ss_ = NULL;
          PROXY_CS_LOG(DEBUG, "[acquire server session] last server session not match,"
                       "returning to shared pool", K_(cs_id));
        }
      }
    }

    // 2. try other session in common pool
    if (!is_proxy_mysql_client_ && NULL == svr_session) {
      if (OB_UNLIKELY(is_session_pool_client())) {
        ret = acquire_svr_session_in_session_pool(addr, svr_session);
      } else {
        ret = acquire_svr_session_no_pool(addr, svr_session);
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
    return (const_cast<ObMysqlSessionManagerNew &>(session_manager_new_).get_svr_session_count() + (NULL == bound_ss_ ? 0 : 1)
            + (NULL == cur_ss_ ? 0 : 1) + (NULL == last_bound_ss_ ? 0 : 1));
  } else {
    return (session_manager_.get_svr_session_count() + (NULL == bound_ss_ ? 0 : 1)
            + (NULL == cur_ss_ ? 0 : 1) + (NULL == last_bound_ss_ ? 0 : 1));
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
    PROXY_CS_LOG(WARN, "mysql sm is null", K(ret));
  } else if (mysql_sm_->trans_state_.mysql_config_params_->enable_report_session_stats_) {
    mysql_sm_->update_session_stats(session_stats_.stats_, SESSION_STAT_COUNT);
    session_stats_.modified_time_ = get_hrtime();
    if (0 == session_stats_.reported_time_) {
      session_stats_.reported_time_ = get_hrtime();
    }
    if (!is_proxy_mysql_client_
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
        PROXY_CS_LOG(WARN, "copy cluster name string error", K(ret));
      } else if (OB_FAIL(g_stat_processor.report_session_stats(cluster_name_str, get_proxy_sessid(),
          session_stats_.stats_, session_stats_.is_first_register_, g_mysql_stat_name, SESSION_STAT_COUNT))) {
        PROXY_CS_LOG(WARN, "fail to init report_session_stats", K_(cs_id), K(ret));
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
      PROXY_CS_LOG(WARN, "mysql_sm_ is null", K(ret));
    } else if (OB_ISNULL(params = mysql_sm_->trans_state_.mysql_config_params_)) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CS_LOG(WARN, "trans_state_.mysql_config_params_ is null", K(ret));
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
       K_(is_proxy_mysql_client),
       K_(is_waiting_trans_first_request),
       K_(need_delete_cluster),
       K_(is_first_dml_sql_got),
       K_(vc_ready_killed),
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
       KP_(bound_ss),
       KP_(lii_ss),
       KPC_(cluster_resource),
       KP_(client_vc),
       K_(using_ldg),
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
    PROXY_CS_LOG(WARN, "xx@proxysys ip is not private", "ip", src_ip, K(ret));

  //2. check password
  } else {
    const ObString &login_passwd = session_info_.get_login_req().get_hsr_result().response_.get_auth_response();
    const ObString &scramble_string = (session_info_.get_scramble_string().empty()
        ? ObString::make_string("aaaaaaaabbbbbbbbbbbb")
        : get_scramble_string());
    ObString stored_stage1;

    if (USER_TYPE_PROXYSYS == type && 0 == login_passwd.length()) {
      if (0 != strlen(get_global_proxy_config().obproxy_sys_password.str())) {
        ret = OB_PASSWORD_WRONG;
        PROXY_CS_LOG(WARN, "root@proxysys check failed", K(ret));
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
        PROXY_CS_LOG(WARN, "failed to encrypt_stage1_to_stage2_hex",  K(ret));
      } else {
        ObString stored_stage2_hex_str(copy_len, stored_stage2_hex);
        if (OB_FAIL(ObEncryptedHelper::check_login(login_passwd, scramble_string, stored_stage2_hex_str, bret))) {
          PROXY_CS_LOG(WARN, "failed to check proxysys login",  K(ret));
        } else {
          if (!bret) {
            ret = OB_PASSWORD_WRONG;
            PROXY_CS_LOG(WARN, "password error", K(ret));
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
          && !this->is_proxy_mysql_client_
          && RUN_MODE_PROXY == g_run_mode);
}

inline bool ObMysqlClientSession::need_close() const
{
  bool bret = false;
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  if (OB_UNLIKELY(info.graceful_exit_start_time_ > 0)
      && OB_LIKELY(!info.need_conn_accept_)
      && !is_proxy_mysql_client_
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
  } else if (NULL != bound_ss_ && conn_id == bound_ss_->get_server_sessid()) {
    bret = true;
  } else if (NULL != last_bound_ss_ && conn_id == last_bound_ss_->get_server_sessid()) {
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
  //1. if is_proxy_mysql_client_ && is_user_idc_name_set_, use inner_request_param_->current_idc_name_
  //2. if session_info_.is_user_idc_name_set, use user's
  //3. if congfig is avail, use global config
  if (is_proxy_mysql_client_
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
    } else if (OB_LIKELY(NULL != mysql_sm_)) {
      ObString tmp_idc(mysql_sm_->proxy_idc_name_);
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
    PROXY_CS_LOG(WARN, "cluster_resource is not avail", K(ret));
  } else if (OB_ISNULL(dummy_entry_) || OB_UNLIKELY(!dummy_entry_->is_tenant_servers_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WARN, "dummy_entry_ is not avail", KPC(dummy_entry_), K(ret));
  } else if (OB_ISNULL(mysql_sm_) || OB_ISNULL(mysql_sm_->trans_state_.mysql_config_params_)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WARN, "mysql_sm_ is not avail",  K(ret));
  } else if (OB_FAIL(ObLDCLocation::get_thread_allocator(allocator))) {
    PROXY_CS_LOG(WARN, "fail to get_thread_allocator", K(ret));
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
          PROXY_CS_LOG(WARN, "fail to tryrdlock server_state_lock, ignore this update",
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
            PROXY_CS_LOG(WARN, "fail to assign servers_info_", K(ret));
          } else {
            server_state_version_ = new_ss_version;
          }
          server_state_lock.rdunlock();
        }
      }
      if (OB_SUCC(ret) && !need_ignore) {
        if (OB_FAIL(dummy_ldc_.assign(dummy_entry_->get_tenant_servers(), simple_servers_info,
            new_idc_name, is_base_servers_added, cluster_resource->get_cluster_name(),
            cluster_resource->get_cluster_id()))) {
          if (OB_EMPTY_RESULT == ret) {
            if (dummy_entry_->is_entry_from_rslist()) {
              set_need_delete_cluster();
              PROXY_CS_LOG(WARN, "tenant server from rslist is not match the server list, "
                           "need delete this cluster", KPC_(dummy_entry), K(ret));
            } else {
              //sys dummy entry can not set dirty
              if (!dummy_entry_->is_sys_dummy_entry() && dummy_entry_->cas_set_dirty_state()) {
                PROXY_CS_LOG(WARN, "tenant server is invalid, set it dirty", KPC_(dummy_entry), K(ret));
              }
            }
          } else {
            PROXY_CS_LOG(WARN, "fail to assign dummy_ldc", K(ret));
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
                   K(is_base_servers_added),
                   K(dummy_ldc_));
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
  return (is_proxy_mysql_client_
          && OB_NOT_NULL(inner_request_param_)
          && inner_request_param_->need_print_trace_stat_);
}

int ObMysqlClientSessionMap::set(ObMysqlClientSession &cs)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(id_map_.unique_set(&cs))) {
    PROXY_CS_LOG(DEBUG, "succ to set client session", K(cs.get_cs_id()));
  } else {
    PROXY_CS_LOG(WARN, "fail to set client session", K(cs.get_cs_id()), KP(this), K(ret));
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
    PROXY_CS_LOG(WARN, "fail to erase client session", K(id), KP(this), K(ret));
  }
  return ret;
}

int init_cs_map_for_thread()
{
  int ret = OB_SUCCESS;
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  for (int64_t i = 0; i < event_thread_count && OB_SUCC(ret); ++i) {
    if (OB_FAIL(init_cs_map_for_one_thread(i))) {
      PROXY_NET_LOG(WARN, "fail to new ObInactivityCop", K(i), K(ret));
    }
  }
  return ret;
}

int init_cs_map_for_one_thread(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(g_event_processor.event_thread_[ET_CALL][index]->cs_map_ = new (std::nothrow) ObMysqlClientSessionMap())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WARN, "fail to new ObInactivityCop", K(index), K(ret));
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
    PROXY_NET_LOG(WARN, "fail to new ObMysqlRandom", K(ret));
  } else {
    const uint64_t current_time = static_cast<uint64_t>(get_hrtime_internal());
    init_seed->init(current_time, current_time / 2);
  }

  //2. create random seed of each ethread
  if (OB_SUCC(ret)) {
    const int64_t count = g_event_processor.thread_count_for_type_[ET_CALL];
    ObMysqlRandom *tmp_random = NULL;
    for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(tmp_random = new (std::nothrow) ObMysqlRandom())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_NET_LOG(WARN, "fail to new ObMysqlRandom", K(i), K(ret));
      } else {
        const uint64_t tmp = init_seed->get_uint64();
        tmp_random->init(tmp + reinterpret_cast<uint64_t>(tmp_random),
                         tmp + static_cast<uint64_t>(g_event_processor.event_thread_[ET_CALL][i]->tid_));
        g_event_processor.event_thread_[ET_CALL][i]->random_seed_ = tmp_random;
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
    PROXY_NET_LOG(WARN, "fail to new ObMysqlRandom", K(ret));
  } else {
    const uint64_t current_time = static_cast<uint64_t>(get_hrtime_internal());
    init_seed->init(current_time, current_time / 2);
  }

  //2. create random seed of each ethread
  if (OB_SUCC(ret)) {
    ObMysqlRandom *tmp_random = NULL;
    if (OB_ISNULL(tmp_random = new (std::nothrow) ObMysqlRandom())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PROXY_NET_LOG(WARN, "fail to new ObMysqlRandom", K(index), K(ret));
    } else {
      const uint64_t tmp = init_seed->get_uint64();
      tmp_random->init(tmp + reinterpret_cast<uint64_t>(tmp_random),
                       tmp + static_cast<uint64_t>(g_event_processor.event_thread_[ET_CALL][index]->tid_));
      g_event_processor.event_thread_[ET_CALL][index]->random_seed_ = tmp_random;
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
      thread_init_cs_id, max_local_seq, g_event_processor.thread_count_for_type_[ET_CALL] - 1))) {
    bret = false;
    PROXY_CS_LOG(WARN, "fail to  is get thread init cs id", K(bret), K(ret));
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
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();

  const uint32_t proxy_heads_bits   = 10; //proxy mark + proxy id + upgrade version
  const uint32_t upgrade_ver_offset = 32 - proxy_heads_bits;
  const uint32_t thread_id_bits     = 32 - __builtin_clz(static_cast<uint32_t>(g_event_processor.thread_count_for_type_[ET_CALL] - 1));
  const uint32_t cs_upgrade_ver     = (cs_id & (0x1 << upgrade_ver_offset)) >> upgrade_ver_offset;
  const uint32_t target_upgrade_ver = static_cast<uint32_t>(0x1 & info.upgrade_version_);

  thread_id = -1;
  if (cs_upgrade_ver != target_upgrade_ver) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WARN, "error upgrade version, it maybe other proxy conn id", K(cs_upgrade_ver),
             K(target_upgrade_ver), "upgrade_version", info.upgrade_version_, K(ret));
  } else {
    const uint32_t thread_id_tmp = ((cs_id << proxy_heads_bits) >> (32 - thread_id_bits));
    if (thread_id_tmp >= static_cast<uint32_t>(g_event_processor.thread_count_for_type_[ET_CALL])) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CS_LOG(WARN, "error thread id, it should not happened", K(thread_id_tmp), K(ret));
    } else {
      thread_id = static_cast<int64_t>(thread_id_tmp);
    }
  }
  return ret;
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

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
