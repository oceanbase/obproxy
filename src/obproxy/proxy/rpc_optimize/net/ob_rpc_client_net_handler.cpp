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
#include "lib/utility/ob_print_utils.h"
#include "proxy/api/ob_plugin.h"
#include "obutils/ob_resource_pool_processor.h"
#include "obutils/ob_config_server_processor.h"
#include "dbconfig/ob_proxy_pb_utils.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"
#include "omt/ob_conn_table_processor.h"
#include "omt/ob_white_list_table_processor.h"
#include "proxy/rpc_optimize/net/ob_rpc_client_net_handler.h"
#include "proxy/rpc_optimize/net/ob_rpc_server_net_handler.h"
#include "proxy/rpc_optimize/ob_rpc_req_debug_names.h"
#include "proxy/rpc_optimize/ob_rpc_request_sm.h"
#include "obkv/table/ob_rpc_struct.h"
#include "prometheus/ob_prometheus_info.h"
#include "prometheus/ob_rpc_prometheus.h"
#include "obproxy/stat/ob_rpc_stats.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::prometheus;
using namespace oceanbase::obproxy::dbconfig;
using namespace oceanbase::obproxy::omt;
using namespace oceanbase::obproxy::obkv;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define STATE_ENTER(state_name, event, vio) do { \
  PROXY_CS_LOG(DEBUG, "ENTER STATE "#state_name"", "event", ObRpcReqDebugNames::get_event_name(event), K_(cs_id)); \
} while(0)

static int64_t const MYSQL_BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K);

// We have debugging list that we can use to find stuck
// client sessions
#ifdef USE_MYSQL_DEBUG_LISTS
DLL<ObRpcClientNetHandler> g_debug_rpc_cs_list;
// ObMutex g_debug_rpc_cs_list_mutex;
ObMutex g_debug_rpc_cs_list_mutex;
#endif

ObRpcClientNetHandler::ObRpcClientNetHandler()
    : ObRpcNetHandler(),
      vc_ready_killed_(false),
      half_close_(false),
      cluster_resource_(NULL),
      cluster_version_(0),
      dummy_entry_(NULL), is_need_update_dummy_entry_(false),
      dummy_ldc_(), dummy_entry_valid_time_ns_(0),
      magic_(RPC_C_NET_MAGIC_DEAD), create_thread_(NULL), is_local_connection_(false),
      in_list_stat_(LIST_INIT),
      current_tid_(-1),
      cs_id_(0), atomic_channel_id_(0), proxy_sessid_(0),
      using_ldg_(false), tcp_init_cwnd_set_(0), read_state_(MCS_INIT), active_(true),
      is_sending_response_(false), need_delete_cluster_(false), server_state_version_(0),
      ct_info_(), last_server_ip_(),
      need_send_response_list_(), sending_response_list_(), period_task_action_(NULL), pending_action_(NULL),
      current_need_read_len_(RPC_NET_HEADER_LENGTH), current_ez_header_(),
      session_info_(), net_head_buf_()
{
  cid_to_req_map_.create(OB_RPC_PARALLE_REQUEST_MAP_MAX_BUCKET_NUM, ObModIds::OB_RPC);
  SET_HANDLER(&ObRpcClientNetHandler::main_handler);
}

void ObRpcClientNetHandler::destroy()
{
  PROXY_CS_LOG(INFO, "rpc client session destroy", K_(cs_id), K_(proxy_sessid), KP_(rpc_net_vc));

  if (OB_UNLIKELY(NULL != rpc_net_vc_)
      || OB_ISNULL(read_buffer_)) {
    PROXY_CS_LOG(WDIAG, "invalid rpc client session", K(rpc_net_vc_), K(read_buffer_));
  }
  is_local_connection_ = false;
  cid_to_req_map_.destroy(); //just abandon, may be need to release it used timeout

  if (NULL != dummy_entry_) {
    dummy_entry_->dec_ref();
    dummy_entry_ = NULL;
  }

  dummy_ldc_.reset();
  dummy_entry_valid_time_ns_ = 0;

  if (NULL != cluster_resource_) {
    PROXY_CS_LOG(DEBUG, "client session cluster resource will dec ref", K_(cluster_resource), KPC_(cluster_resource));
    cluster_resource_->dec_ref();
    cluster_resource_ = NULL;
  }

  magic_ = RPC_C_NET_MAGIC_DEAD;
  if (OB_LIKELY(NULL != read_buffer_)) {
    free_miobuffer(read_buffer_);
    read_buffer_ = NULL;
  }
  if (OB_LIKELY(NULL != net_entry_.write_buffer_)) {
    free_miobuffer(net_entry_.write_buffer_);
    net_entry_.write_buffer_ = NULL;
  }

  if (OB_LIKELY(NULL != net_entry_.read_buffer_)) {
    free_miobuffer(net_entry_.read_buffer_);
    net_entry_.read_buffer_ = NULL;
  }

#ifdef USE_MYSQL_DEBUG_LISTS
  mutex_acquire(&g_debug_rpc_cs_list_mutex);
  g_debug_rpc_cs_list.remove(this);
  mutex_release(&g_debug_rpc_cs_list_mutex);
#endif

  if (conn_prometheus_decrease_) {
    RPC_NET_SESSION_PROMETHEUS_STAT(session_info_, PROMETHEUS_CURRENT_SESSION, true, -1);
    conn_prometheus_decrease_ = false;
  }

  session_info_.destroy();
  ObRpcNetHandler::cleanup();
  create_thread_ = NULL;

  op_reclaim_free(this);
}

int ObRpcClientNetHandler::new_connection(
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

int ObRpcClientNetHandler::new_connection(
    ObNetVConnection *new_vc, ObMIOBuffer *iobuf, ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_vc) || OB_UNLIKELY(NULL != rpc_net_vc_)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WDIAG, "invalid client connection", K(new_vc), K(rpc_net_vc_), K(ret));
  } else {
    PROXY_CS_LOG(DEBUG, "ObRpcClientNetHandler::new_connection", K(new_vc), K(iobuf), K(reader),
        "this_thread", this_ethread());
    create_thread_ = this_ethread();
    rpc_net_vc_ = new_vc;
    magic_ = RPC_C_NET_MAGIC_ALIVE;
    mutex_ = new_vc->mutex_;

    MUTEX_TRY_LOCK(lock, mutex_, this_ethread());
    if (OB_LIKELY(lock.is_locked())) {
      current_tid_ = gettid_ob();
      RPC_INCREMENT_DYN_STAT(CURRENT_CLIENT_CONNECTIONS);
      RPC_INCREMENT_DYN_STAT(TOTAL_CLIENT_CONNECTIONS);

      switch (new_vc->get_remote_addr().sa_family) {
        case AF_INET:
          RPC_INCREMENT_DYN_STAT(TOTAL_CLIENT_CONNECTIONS_IPV4);
          break;
        case AF_INET6:
          RPC_INCREMENT_DYN_STAT(TOTAL_CLIENT_CONNECTIONS_IPV6);
          break;
        default:
          break;
      }

#ifdef USE_MYSQL_DEBUG_LISTS
      if (OB_SUCCESS == mutex_acquire(&g_debug_rpc_cs_list_mutex)) {
        g_debug_rpc_cs_list.push(this);
        if (OB_SUCCESS != mutex_release(&g_debug_rpc_cs_list_mutex)) {
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
          // buffer_reader_ = reader;
          buf_reader_ = reader;
        } else if (OB_ISNULL(buf_reader_ = read_buffer_->alloc_reader())) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_CS_LOG(EDIAG, "fail to alloc buffer reader", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        /**
         * we cache the request in the read io buffer, so the water mark of
         * read io buffer must be larger than the reqeust packet size. we set
         * RPC_NET_HEADER_LENGTH as the default water mark, when we read the
         * header of request, we reset the water mark.
         */
        read_buffer_->water_mark_ = RPC_NET_HEADER_LENGTH;
        // start listen event on client vc
        if (OB_FAIL(acquire_client_session_id())) {
          PROXY_CS_LOG(WDIAG, "fail to acquire client session_id", K_(cs_id), K(ret));
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
          PROXY_CS_LOG(INFO, "RPC client session born", K_(cs_id), K_(proxy_sessid), K_(is_local_connection), K_(rpc_net_vc),
                       "client_fd", rpc_net_vc_->get_conn_fd(), K(client_addr));

          // 1. first convert vip to tenant info, if needed.
          if (is_need_convert_vip_to_tname()) {
            //TODO add net
            if (OB_FAIL(fetch_tenant_by_vip())) {
              PROXY_CS_LOG(WDIAG, "fail to fetch tenant by vip", K_(cs_id), K(ret));
              ret = OB_SUCCESS;
            } else if (is_vip_lookup_success()) {
              session_info_.set_is_read_only_user(ct_info_.vip_tenant_.is_read_only());
              session_info_.set_is_request_follower_user(ct_info_.vip_tenant_.is_request_follower());
              session_info_.set_vip_addr_name(ct_info_.vip_tenant_.vip_addr_.addr_);
              ObString user_name;
              if (!get_global_white_list_table_processor().can_ip_pass(
                ct_info_.vip_tenant_.cluster_name_, ct_info_.vip_tenant_.tenant_name_,
                user_name, rpc_net_vc_->get_real_client_addr())) {
                ret = OB_ERR_CAN_NOT_PASS_WHITELIST;
                PROXY_CS_LOG(DEBUG, "can not pass white_list", K_(cs_id), K(ct_info_.vip_tenant_.cluster_name_),
                              K(ct_info_.vip_tenant_.tenant_name_), K(client_addr), K(ret));
              }
            }
          }

          RPC_NET_SESSION_PROMETHEUS_STAT(get_session_info(), PROMETHEUS_CURRENT_SESSION, true, 1);
          RPC_NET_SESSION_PROMETHEUS_STAT(get_session_info(), PROMETHEUS_NEW_CLIENT_CONNECTIONS, 1);
          set_conn_prometheus_decrease(true);


          if (OB_SUCC(ret)) {
            if (OB_FAIL(schedule_period_task())) {
              PROXY_CS_LOG(WDIAG, "fail to call schedule_period_task", K_(cs_id), K(ret));
            } else if (OB_FAIL(setup_client_request_read())) {
              PROXY_CS_LOG(WDIAG, "fail to call setup_client_request_read", K_(cs_id), K(ret));
            }
          }
        }
      } // end if (OB_SUCC(ret))
    } else {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CS_LOG(WDIAG, "fail to try lock thread mutex, will close connection", K_(cs_id), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    PROXY_CS_LOG(WDIAG, "fail to do new connection, do_io_close itself", K_(cs_id), K(ret));
    do_io_close();
  }
  return ret;
}

int ObRpcClientNetHandler::fetch_tenant_by_vip()
{
  int ret = OB_SUCCESS;
  ct_info_.lookup_success_ = false;
  ObVipAddr addr = ct_info_.vip_tenant_.vip_addr_;
  ObConfigItem tenant_item, cluster_item;
  bool found = false;
  if (OB_FAIL(get_global_config_processor().get_proxy_config_with_level(
    addr, "", "", "proxy_tenant_name", tenant_item, "LEVEL_VIP", found))) {
    PROXY_CS_LOG(WDIAG, "get proxy tenant name config failed", K_(cs_id), K(addr), K(ret));
  }

  if (OB_SUCC(ret) && found) {
    if (OB_FAIL(get_global_config_processor().get_proxy_config_with_level(
      addr, "", "", "rootservice_cluster_name", cluster_item, "LEVEL_VIP", found))) {
      PROXY_CS_LOG(WDIAG, "get cluster name config failed", K_(cs_id), K(addr), K(ret));
    }
  }

  if (OB_SUCC(ret) && found) {
    if (OB_FAIL(ct_info_.vip_tenant_.set_tenant_cluster(tenant_item.str(), cluster_item.str()))) {
      PROXY_CS_LOG(WDIAG, "set tenant and cluster name failed", K_(cs_id), K(tenant_item), K(cluster_item), K(ret));
    } else {
      session_info_.set_vip_addr_name(addr.addr_);
      ct_info_.lookup_success_ = true;
      PROXY_CS_LOG(DEBUG, "succ to get conn info", K_(cs_id), "vip_tenant", ct_info_.vip_tenant_);
    }
  }
  return ret;
}

int ObRpcClientNetHandler::get_vip_addr()
{
  int ret = OB_SUCCESS;
  int64_t vid;
  vid = static_cast<int64_t>(rpc_net_vc_->get_virtual_vid());
  ct_info_.vip_tenant_.vip_addr_.set(rpc_net_vc_->get_virtual_addr(), vid);

  // TODO oushen, get client ip, slb ip from kernal

  return ret;
}

void ObRpcClientNetHandler::handle_new_connection()
{
  // RPC do nothing
  // Use a local pointer to the mutex as when we return from do_api_callout,
  // the ClientSession may have already been deallocated.
  // ObEThread &ethread = self_ethread();
  // ObPtr<ObProxyMutex> lmutex = mutex_;
  // int ret = OB_SUCCESS;
  // {
  //   MUTEX_LOCK(lock, lmutex, &ethread);
  //   //TODO RPC new_connection
  //   // if (OB_FAIL(do_api_callout(OB_MYSQL_SSN_START_HOOK))) {
  //   //   PROXY_CS_LOG(WDIAG, "fail to start hook, will close client session", K(ret));
  //   // }
  // }
  // if (OB_FAIL(ret)) {
  //   do_io_close();
  // }
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
int ObRpcClientNetHandler::acquire_client_session_id()
{
  static __thread uint32_t next_cs_id = 0;
  static __thread uint32_t thread_init_cs_id = 0;
  static __thread uint32_t max_local_seq = 0;

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == next_cs_id) && OB_FAIL(get_thread_init_cs_id(thread_init_cs_id, max_local_seq))) {
    PROXY_CS_LOG(WDIAG, "fail to  is get thread init cs id", K(next_cs_id), K(ret));
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
int ObRpcClientNetHandler::get_thread_init_cs_id(uint32_t &thread_init_cs_id,
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

int ObRpcClientNetHandler::add_to_list()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(LIST_ADDED == in_list_stat_)) {
    ret = OB_ENTRY_EXIST;
    PROXY_CS_LOG(WDIAG, "cs had already in the list, it should not happened", K_(cs_id), K(ret));
  } else if (OB_ISNULL(create_thread_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WDIAG, "create_thread_ is null, it should not happened", K_(cs_id), K(ret));
  } else {
    const int64_t MAX_TRY_TIMES = 1000;
    ObRpcClientNetHandlerMap &cs_map = get_rpc_client_net_handler_map(*create_thread_);
    for (int64_t i = 0; OB_SUCC(ret) && LIST_ADDED != in_list_stat_ && i < MAX_TRY_TIMES; ++i) {
      if (OB_FAIL(cs_map.set(*this))) {
        if (OB_LIKELY(OB_HASH_EXIST == ret)) {
          PROXY_CS_LOG(INFO, "repeat cs id, retry to acquire another one", K_(cs_id), K(rpc_net_vc_), K(ret));
          if (OB_FAIL(acquire_client_session_id())) {
            PROXY_CS_LOG(WDIAG, "fail to acquire client session_id", K_(cs_id), K(rpc_net_vc_), K(ret));
          }
        } else {
          PROXY_CS_LOG(WDIAG, "fail to set cs into cs_map", K_(cs_id), K(rpc_net_vc_), K(ret));
        }
      } else {
        in_list_stat_ = LIST_ADDED;
      }
    }
    if (OB_SUCC(ret) && LIST_ADDED != in_list_stat_) {
      ret = OB_SESSION_ENTRY_EXIST;
      PROXY_CS_LOG(WDIAG, "there is no enough cs id, close this connect", K_(cs_id), K(rpc_net_vc_), K(ret));
    }
  }
  return ret;
}

uint64_t ObRpcClientNetHandler::get_next_proxy_sessid()
{
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

ObVIO *ObRpcClientNetHandler::do_io_write(
    ObContinuation *c, const int64_t nbytes, ObIOBufferReader *buf)
{
  // conditionally set the tcp initial congestion window
  // before our first write.
  if (!tcp_init_cwnd_set_) {
    tcp_init_cwnd_set_ = true;
    set_tcp_init_cwnd();
  }
  return rpc_net_vc_->do_io_write(c, nbytes, buf);
}

void ObRpcClientNetHandler::set_tcp_init_cwnd()
{
  int32_t desired_tcp_init_cwnd = static_cast<int32_t>(get_global_proxy_config().server_tcp_init_cwnd);

  if (0 != desired_tcp_init_cwnd) {
    if (0 != rpc_net_vc_->set_tcp_init_cwnd(desired_tcp_init_cwnd)) {
      PROXY_CS_LOG(WDIAG, "set_tcp_init_cwnd failed", K_(cs_id), K(desired_tcp_init_cwnd));
    }
  }
}

void ObRpcClientNetHandler::do_io_close(const int alerrno)
{
  int ret = OB_SUCCESS;
  // Prevent double closing
  PROXY_CS_LOG(DEBUG, "ObRpcClientNetHandler do_io_close", K_(cs_id));

  if (MCS_CLOSED != read_state_) {
    // clean all rpc req
    if (OB_FAIL(cancel_period_task())) {
      PROXY_CS_LOG(WDIAG, "fail to call cancel_period_task", K_(cs_id));
    } else if (OB_FAIL(cancel_pending_action())) {
      PROXY_CS_LOG(WDIAG, "fail to call cancel_pending_action", K_(cs_id));
    }
    clean_all_pending_request();

    if (MCS_ACTIVE_READER == read_state_) { // now not enter
      if (LIST_ADDED == in_list_stat_) {
        RPC_INCREMENT_DYN_STAT(CURRENT_CLIENT_TRANSACTIONS);
      }
      if (active_) {
        active_ = false;
        RPC_INCREMENT_DYN_STAT(CURRENT_ACTIVE_CLIENT_CONNECTIONS);
      }
    }

    if (half_close_) { // now not enter
      read_state_ = MCS_HALF_CLOSED;
      PROXY_CS_LOG(DEBUG, "session half close", K_(cs_id));

      // We want the client to know that that we're finished writing. The
      // write shutdown accomplishes this. Unfortunately, the IO Core
      // semantics don't stop us from getting events on the write side of
      // the connection like timeouts so we need to zero out the write of
      // the continuation with the do_io_write() call
      rpc_net_vc_->do_io_shutdown(IO_SHUTDOWN_WRITE);

      if (OB_ISNULL(net_entry_.read_vio_ = rpc_net_vc_->do_io_read(this, INT64_MAX, read_buffer_))) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CS_LOG(WDIAG, "read_vio_ is null", K_(cs_id), K(ret));
      }

      // Drain any data read.
      // If the buffer is full and the client writes again, we will not receive a
      // READ_READY event.
      if (OB_FAIL(buf_reader_->consume(buf_reader_->read_avail()))) {
        PROXY_CS_LOG(WDIAG, "fail to consume ", K_(cs_id), K(ret));
      }
    }

    if (!half_close_ && NULL != rpc_net_vc_) {
      rpc_net_vc_->do_io_close(alerrno);
      PROXY_CS_LOG(DEBUG, "session closed, session stats", K_(cs_id));
      rpc_net_vc_ = NULL;
      //RPC_SUM_DYN_STAT(TRANSACTIONS_PER_CLIENT_CON, get_transact_count());
      RPC_DECREMENT_DYN_STAT(CURRENT_CLIENT_CONNECTIONS);
    }

    if (!half_close_ && LIST_ADDED == in_list_stat_) {
      // proxy_mysql_client is not in map, no need to erase
      if (this_ethread() != create_thread_) {
        PROXY_CS_LOG(DEBUG, "current thread is not create thread, should schedule",
                     "current thread", this_ethread(), "create thread", create_thread_, K_(cs_id));
        CLIENT_NET_SET_DEFAULT_HANDLER(&ObRpcClientNetHandler::handle_other_event);
        if (OB_ISNULL(create_thread_->schedule_imm(this, CLIENT_SESSION_ERASE_FROM_MAP_EVENT))) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_CS_LOG(WDIAG, "fail to schedule switch thread", K_(cs_id), K(ret));
        }
      } else {
        ObRpcClientNetHandlerMap &cs_map = get_rpc_client_net_handler_map(*create_thread_);
        if (OB_FAIL(cs_map.erase(cs_id_))) {
          PROXY_CS_LOG(WDIAG, "current client session is not in table, no need to erase", K_(cs_id), K(ret));
        }
        in_list_stat_ = LIST_REMOVED;
      }
    }

    // in 2 situations we will delete cluster (cluster rslist and resource)
    // 1. all servers of table entry which comes from rslist are not in congestion list
    // 2. fail to verify cluster name in login step, user cluster and server cluster are not the same
    //
    // if we only delete resource, loacl cluster rslist still exist, so we must delete both of them
    if (OB_UNLIKELY(need_delete_cluster_)) { // now not enter
      if (OB_FAIL(handle_delete_cluster())) {
        PROXY_CS_LOG(WDIAG, "fail to handle delete cluster", K_(cs_id), K(ret));
      }
      need_delete_cluster_ = false;
    }

    if (this_ethread() == create_thread_) {
      read_state_ = MCS_CLOSED;
      destroy(); // clean
    }
  }
}

int ObRpcClientNetHandler::handle_other_event(int event, void *data)
{
  UNUSED(data);
  switch (event) {
    case CLIENT_SESSION_ERASE_FROM_MAP_EVENT: {
      do_io_close();
      break;
    }
    default:
      PROXY_CS_LOG(WDIAG, "unknown event", K_(cs_id), K(event));
      break;
  }

  return VC_EVENT_NONE;
}

//TODO RPC ZDW, no support at first
inline int ObRpcClientNetHandler::handle_delete_cluster()
{
  // in 2 situations we will delete cluster (cluster rslist and resource)
  // 1. all servers of table entry which comes from rslist are not in congestion list
  // 2. fail to verify cluster name in login step, user cluster and server cluster are not the same
  // if we only delete resource, loacl cluster rslist still exist, so we must delete both of them
  int ret = OB_SUCCESS;
  // const ObString &name = session_info_.get_rpc_login_req().cluster_name_;
  // const int64_t cr_id = session_info_.get_rpc_login_req().cluster_id_;
  //TODO NEED add get cluster name/cr_id
  /*
  const ObString name;
  const int64_t cr_id = 0;
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
  */
  return ret;
}

int ObRpcClientNetHandler::state_keep_alive(int event, void *data)
{
  int ret = OB_SUCCESS;
  STATE_ENTER(&ObRpcClientNetHandler::state_keep_alive, event, data);
  if (OB_LIKELY(data == net_entry_.read_vio_)) {
    switch (event) {
      case VC_EVENT_READ_READY:
      case VC_EVENT_READ_COMPLETE:
      {
        // handle half closed
        if (MCS_HALF_CLOSED == read_state_) {
          if (OB_FAIL(buf_reader_->consume(buf_reader_->read_avail()))) {
            PROXY_CS_LOG(WDIAG, "fail to consume ", K_(cs_id), K(ret));
          }
        } else {
          if (OB_FAIL(state_client_request_read(event, data))) { //first
            PROXY_CS_LOG(WDIAG, "fail to call state_client_request_read", K_(cs_id), K(ret));
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
          if (buf_reader_->read_avail() > 0) {
            // if (OB_FAIL(new_transact())) {
            //   PROXY_CS_LOG(WDIAG, "fail to start new transaction", K(ret));
            // }
          } else {
            do_io_close();
          }
        }
        break;
      }
      // fallthrough
      case VC_EVENT_ERROR:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_INACTIVITY_TIMEOUT: {
        if (MCS_HALF_CLOSED == read_state_) {
          half_close_ = false;
        }
        // Keep-alive timed out
        if (VC_EVENT_INACTIVITY_TIMEOUT == event) {
          ObIpEndpoint client_ip;
          if (NULL != rpc_net_vc_) {
            if (OB_UNLIKELY(!ops_ip_copy(client_ip, rpc_net_vc_->get_remote_addr()))) {
              PROXY_CS_LOG(WDIAG, "fail to ops_ip_copy client_ip", K_(cs_id), K(rpc_net_vc_));
            }
          }

          PROXY_CS_LOG(WDIAG, "client connection is idle over wait_timeout, now we will close it.",
                     //  "wait_timeout(s)", hrtime_to_sec(session_info_.get_wait_timeout()),
                       K_(cs_id),
                       K(client_ip),
                       "event", ObRpcReqDebugNames::get_event_name(event));
        }

        do_io_close();
        break;
      }
      default:
        // These events are bogus
      PROXY_CS_LOG(WDIAG, "invalid event", K_(cs_id), K(event));
      break;
    }
  }
  return VC_EVENT_NONE;
}

void ObRpcClientNetHandler::reenable(ObVIO *vio)
{
  rpc_net_vc_->reenable(vio);
}

int ObRpcClientNetHandler::main_handler(int event, void *data)
{
  int event_ret = VC_EVENT_CONT;
  PROXY_CS_LOG(DEBUG, "[ObRpcClientNetHandler::main_handler]",
            K_(cs_id),
            "event_name", ObRpcReqDebugNames::get_event_name(event),
            "read_state", get_read_state_str(), K(data));

  if (OB_LIKELY(RPC_C_NET_MAGIC_ALIVE == magic_)) {
    if (RPC_CLIENT_NET_PERIOD_TASK == event) {
      event_ret = handle_period_task();
    } else if (RPC_CLIENT_NET_SEND_RESPONSE == event) {
      event_ret = setup_client_response_send();
    } else {
      if (NULL != data && data == net_entry_.read_vio_) { // from client vc
        event_ret = state_keep_alive(event, data);
      } else if (NULL != data && data == net_entry_.write_vio_) { // from client vc
        event_ret = state_client_response_send(event, data);
      } else {
        event_ret = (this->*cs_default_handler_)(event, data); // others
      }
    }
  } else {
    PROXY_CS_LOG(WDIAG, "unexpected magic, expected RPC_C_NET_MAGIC_ALIVE", K_(cs_id), K(magic_));
  }

  return event_ret;
}

void ObRpcClientNetHandler::handle_transact_complete(ObIOBufferReader *r, bool &close_cs)
{
  //int ret = OB_SUCCESS;
  close_cs = false;
  // is_waiting_trans_first_request_ = true;
  if (OB_LIKELY(MCS_ACTIVE_READER == read_state_)) {
    PROXY_CS_LOG(DEBUG, "client session handle transaction complete",
                 K_(cs_id), KPC_(cluster_resource));
    if (OB_UNLIKELY(get_global_proxy_config().is_metadb_used() && get_global_proxy_config().enable_report_session_stats)) {
      // update_session_stats();
    }

    // Make sure that the state machine is returning correct buffer reader
    if (OB_UNLIKELY(r != buf_reader_)) {
      PROXY_CS_LOG(WDIAG, "buffer reader mismatch, will close client session",
                   K_(cs_id),
                   K(r), K(buf_reader_));
      close_cs = true;
    } else if (OB_UNLIKELY(!get_global_hot_upgrade_info().need_conn_accept_) && OB_UNLIKELY(need_close())) {
      close_cs = true;
      PROXY_CS_LOG(INFO, "receive exit cmd, obproxy will exit, now close client session",
                   K_(cs_id),
                   K(*this));
    } else if (session_info_.is_oceanbase_server()) {
      if ((OB_ISNULL(cluster_resource_) || OB_UNLIKELY(cluster_resource_->is_deleting()))) {
        if (NULL != cluster_resource_) {
          PROXY_CS_LOG(INFO, "the cluster resource is deleting, client session will close",
                       K_(cs_id),
                       KPC_(cluster_resource), K_(cluster_resource));
        }
        close_cs = true;
      }
    }
    if (OB_LIKELY(!close_cs)) {
      if (OB_UNLIKELY(get_global_performance_params().enable_stat_)) {
        RPC_DECREMENT_DYN_STAT(CURRENT_CLIENT_TRANSACTIONS);
        //TODO add it next
        // if (OB_UNLIKELY(NULL != trace_stats_) && trace_stats_->is_trace_stats_used()) {
        //   PROXY_CS_LOG(DEBUG, "current trace stats", KPC_(trace_stats), K_(cs_id));
        //   //mark_trace_stats_need_reuse() will not remove history stats, just denote trace_stats need reset next time
        //   trace_stats_->mark_trace_stats_need_reuse();
        // }
      }

      // Clean up the write VIO in case of inactivity timeout
      do_io_write(NULL, 0, NULL);

    }
  }
}

int ObRpcClientNetHandler::release(ObIOBufferReader *r)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(MCS_ACTIVE_READER == read_state_)) {
    // handling potential keep-alive here
    if (OB_LIKELY(active_)) {
      active_ = false;
      RPC_DECREMENT_DYN_STAT(CURRENT_ACTIVE_CLIENT_CONNECTIONS);
    }

    bool close_cs = false;
    handle_transact_complete(r, close_cs);

    if (OB_LIKELY(!close_cs)) {
      // reset client read buffer water mark
      buf_reader_->mbuf_->water_mark_ = RPC_NET_HEADER_LENGTH;

      // Check to see there is remaining data in the
      // buffer. If there is, spin up a new state
      // machine to process it. Otherwise, issue an
      // IO to wait for new data
      if (buf_reader_->read_avail() > 0) {
        PROXY_CS_LOG(DEBUG, "data already in buffer, starting new transaction", K_(cs_id));
        // if (OB_FAIL(new_transact())) {
        //   PROXY_CS_LOG(WDIAG, "fail to start new transaction", K(ret));
        // }
      } else {
        read_state_ = MCS_KEEP_ALIVE;
        net_entry_.read_vio_ = do_io_read(this, INT64_MAX, read_buffer_);
        // TODO: add keep alive
        // if (OB_LIKELY(server_ka_vio_ != ka_vio_)) {
        //   rpc_net_vc_->add_to_keep_alive_lru();
        //   //set_wait_timeout();
        // }
      }
    } else {
      do_io_close();
    }
  }
  return ret;
}

const char *ObRpcClientNetHandler::get_read_state_str() const
{
  const char *states[MCS_MAX + 1] = {"MCS_INIT",
                                     "MCS_ACTIVE_READER",
                                     "MCS_KEEP_ALIVE",
                                     "MCS_HALF_CLOSED",
                                     "MCS_CLOSED",
                                     "MCS_MAX"};
  return states[read_state_];
}

int64_t ObRpcClientNetHandler::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this),
       K_(vc_ready_killed),
       K_(active),
       K_(magic),
       K_(current_tid),
       K_(cs_id),
       K_(proxy_sessid),
       K_(session_info),
       K_(dummy_ldc),
       KP_(dummy_entry),
       K_(server_state_version),
       KPC_(cluster_resource),
       KP_(rpc_net_vc),
       K_(using_ldg),
      //  KPC_(trace_stats));
      K_(active));
  J_OBJ_END();
  return pos;
}

bool ObRpcClientNetHandler::is_need_convert_vip_to_tname()
{
  return (get_global_proxy_config().need_convert_vip_to_tname);
}

inline bool ObRpcClientNetHandler::need_close() const
{
  bool bret = false;
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  if (OB_UNLIKELY(info.graceful_exit_start_time_ > 0)
      && OB_LIKELY(!info.need_conn_accept_)
      && OB_LIKELY(info.graceful_exit_end_time_ > info.graceful_exit_start_time_)) {
    int64_t current_active_count = 0;
    NET_READ_GLOBAL_DYN_SUM(NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN, current_active_count);
    const ObHRTime remain_time = info.graceful_exit_end_time_ - get_hrtime();
    const ObHRTime total_time = info.graceful_exit_end_time_ - info.graceful_exit_start_time_;
    //use CEIL way
    const int64_t need_active_count = static_cast<int64_t>((remain_time * info.active_client_vc_count_ + total_time - 1) / total_time);
    if (remain_time < 0) {
      bret = true;
      PROXY_CS_LOG(INFO, "client need force close", K_(cs_id), K(current_active_count),
                   "remain_time(ms)", hrtime_to_msec(remain_time));
    } else if (current_active_count > need_active_count) {
      bret = true;
      PROXY_CS_LOG(INFO, "client need orderly close", K_(cs_id), K(current_active_count), K(need_active_count),
                   "remain_time(ms)", hrtime_to_msec(remain_time));
    } else {/*do nothing*/}
  }
  return bret;
}

ObString ObRpcClientNetHandler::get_current_idc_name() const
{
  ObString ret_idc(get_global_proxy_config().proxy_idc_name);

  return ret_idc;
}

int ObRpcClientNetHandler::check_update_ldc()
{
  return check_update_ldc(dummy_ldc_);
}

int ObRpcClientNetHandler::check_update_ldc(ObLDCLocation &dummy_ldc)
{
  int ret = OB_SUCCESS;
  common::ModulePageAllocator *allocator = NULL;
  ObClusterResource *cluster_resource = NULL;
  bool need_dec_cr = false;
  if (get_global_resource_pool_processor().get_default_cluster_resource() == cluster_resource_) {
    // const ObTableEntryName &name = rpc_sm_->trans_state_.pll_info_.te_name_;
    // ObTableEntryName &name = ; //TODO PRPC fill it next
    uint64_t cluster_id = get_cluster_id();
    // cluster_resource = get_global_resource_pool_processor().acquire_cluster_resource(name.cluster_name_, cluster_id);
    cluster_resource = get_global_resource_pool_processor().acquire_cluster_resource(session_info_.cluster_name_, cluster_id);
    need_dec_cr = true;
  } else {
    cluster_resource = cluster_resource_;
  }

  if (OB_ISNULL(cluster_resource)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WDIAG, "cluster_resource is not avail", K_(cs_id), K(ret));
  } else if (OB_ISNULL(dummy_entry_) || OB_UNLIKELY(!dummy_entry_->is_tenant_servers_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WDIAG, "dummy_entry_ is not avail", K_(cs_id), KPC(dummy_entry_), K(ret));
  } else if (OB_FAIL(ObLDCLocation::get_thread_allocator(allocator))) {
    PROXY_CS_LOG(WDIAG, "fail to get_thread_allocator", K_(cs_id), K(ret));
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
        || dummy_ldc.is_empty()
        || 0 != new_idc_name.case_compare(dummy_ldc.get_idc_name())
        || !is_base_servers_added) {
      PROXY_CS_LOG(DEBUG, "need update dummy_ldc",
                   K_(cs_id),
                   "old_idc_name", dummy_ldc.get_idc_name(),
                   K(new_idc_name),
                   "cluster_name", cluster_resource->get_cluster_name(),
                   "old_ss_version", server_state_version_,
                   "new_ss_version", cluster_resource->server_state_version_,
                   "dummy_ldc_is_empty", dummy_ldc.is_empty(),
                   K(is_base_servers_added));
      ObSEArray<ObServerStateSimpleInfo, ObServerStateRefreshCont::DEFAULT_SERVER_COUNT> simple_servers_info(
          ObServerStateRefreshCont::DEFAULT_SERVER_COUNT, *allocator);
      bool need_ignore = false;
      if (!is_base_servers_added && 0 == cluster_resource->server_state_version_) {
        PROXY_CS_LOG(INFO, "base servers has not added, treat all tenant server as ok",
                    K_(cs_id), "tenant_server", *(dummy_entry_->get_tenant_servers()), K(ret));
      } else {
        const uint64_t new_ss_version = cluster_resource->server_state_version_;
        common::ObIArray<ObServerStateSimpleInfo> &server_state_info = cluster_resource->get_server_state_info(new_ss_version);
        common::DRWLock &server_state_lock = cluster_resource->get_server_state_lock(new_ss_version);
        int err_no = 0;
        if (0 != (err_no = server_state_lock.try_rdlock())) {
          if (dummy_ldc.is_empty()) {
            //treate it as is_base_servers_added is false
            is_base_servers_added = false;
          } else {
            need_ignore = true;
          }
          PROXY_CS_LOG(EDIAG, "fail to tryrdlock server_state_lock, ignore this update",
                       K_(cs_id), K(err_no),
                       "old_idc_name", dummy_ldc.get_idc_name(),
                       K(new_idc_name),
                       "old_ss_version", server_state_version_,
                       "new_ss_version", new_ss_version,
                       "dummy_ldc_is_empty", dummy_ldc.is_empty(),
                       K(cluster_resource->is_base_servers_added()),
                       K(is_base_servers_added), K(need_ignore), K(dummy_ldc));
        } else {
          if (OB_FAIL(simple_servers_info.assign(server_state_info))) {
            PROXY_CS_LOG(WDIAG, "fail to assign servers_info_", K_(cs_id), K(ret));
          } else {
            server_state_version_ = new_ss_version;
          }
          server_state_lock.rdunlock();
        }
      }
      if (OB_SUCC(ret) && !need_ignore) {
        if (OB_FAIL(dummy_ldc.assign(dummy_entry_->get_tenant_servers(), simple_servers_info,
            new_idc_name, is_base_servers_added, cluster_resource->get_cluster_name(),
            cluster_resource->get_cluster_id()))) {
          if (OB_EMPTY_RESULT == ret) {
            if (dummy_entry_->is_entry_from_rslist()) {
              set_need_delete_cluster();
              PROXY_CS_LOG(WDIAG, "tenant server from rslist is not match the server list, "
                           "need delete this cluster", K_(cs_id), KPC_(dummy_entry), K(ret));
            } else {
              //sys dummy entry can not set dirty
              if (!dummy_entry_->is_sys_dummy_entry() && dummy_entry_->cas_set_dirty_state()) {
                PROXY_CS_LOG(WDIAG, "tenant server is invalid, set it dirty", K_(cs_id), KPC_(dummy_entry), K(ret));
              }
            }
          } else {
            PROXY_CS_LOG(WDIAG, "fail to assign dummy_ldc", K_(cs_id), K(ret));
          }
        }
      }
      if (cluster_resource->is_base_servers_added()) {
        dummy_ldc.set_safe_snapshot_manager(&cluster_resource->safe_snapshot_mgr_);
      }
    } else {
      PROXY_CS_LOG(DEBUG, "no need update dummy_ldc",
                   K_(cs_id),
                   "old_idc_name", dummy_ldc.get_idc_name(),
                   K(new_idc_name),
                   "old_ss_version", server_state_version_,
                   "new_ss_version", cluster_resource->server_state_version_,
                   "dummy_ldc_is_empty", dummy_ldc.is_empty(),
                   K(is_base_servers_added),
                   K(dummy_ldc));
    }
    allocator = NULL;
  }

  if (OB_UNLIKELY(need_dec_cr && NULL != cluster_resource)) {
    cluster_resource->dec_ref();
  }

  return ret;
}
// */

//TODO ZDW RPC not need any more
bool ObRpcClientNetHandler::need_print_trace_stat() const
{
  return false;
}

int ObRpcClientNetHandler::setup_client_request_read()
{
  int ret = OB_SUCCESS;
  //set read trigger and read_reschedule. sometimes the data already is in the io buffer
  static_cast<ObUnixNetVConnection *>(this->get_netvc())->set_read_trigger();

  // int64_t read_num = 16; //RPC header len
  int64_t read_num = INT64_MAX; //just header for RPC service

  if (OB_ISNULL(net_entry_.read_vio_ = this->do_io_read(this, read_num, buf_reader_->mbuf_))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WDIAG, "rpc client net handler failed to do_io_read", K_(cs_id), K(ret));
  } else {
    if (buf_reader_->read_avail() > 0) {
      PROXY_CS_LOG(DEBUG, "the request already in buffer, continue to handle it",
              K_(cs_id), "buffer len", buf_reader_->read_avail());
      state_client_request_read(VC_EVENT_READ_READY, net_entry_.read_vio_);
      // handle_event(VC_EVENT_READ_READY, net_entry_.read_vio_);
    }
  }

  return ret;
}

int ObRpcClientNetHandler::state_client_request_read(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = VC_EVENT_NONE;
  UNUSED(event_ret);

  STATE_ENTER(ObRpcClientNetHandler::state_client_request_read, event, data);

  /* 1. check need update cluster resource */
  /* 2. check and add trace info */
  /* 3. check event info */
  if (OB_UNLIKELY(NULL != net_entry_.read_vio_ && net_entry_.read_vio_ != reinterpret_cast<ObVIO *>(data))
      || (net_entry_.eos_)) {
    ret = OB_INNER_STAT_ERROR;
    PROXY_CS_LOG(WDIAG, "invalid internal state", K_(cs_id), K_(net_entry_.read_vio), K(data), K_(net_entry_.eos));
  } else {
    switch (event) {
      case VC_EVENT_READ_READY:
      case VC_EVENT_READ_COMPLETE:
        // More data to fill request
        break;
      case VC_EVENT_EOS: {
        net_entry_.eos_ = true;
        PROXY_CS_LOG(INFO, "ObRpcClientNetHandler::state_client_request_read", "event", "set event name",
                 K_(cs_id), "client_vc", P(this->get_netvc()));
        break;
      }
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_ERROR: {
        PROXY_CS_LOG(WDIAG, "ObRpcClientNetHandler::state_client_request_read", "event",
                 ObRpcReqDebugNames::get_event_name(event), K_(cs_id), "client_vc", P(this->get_netvc()));
        ret = OB_CONNECT_ERROR;
        // The client is closed. Close it.
        // trans_state_.client_info_.abort_ = ObRpcTransact::ABORTED; //TODO need check queueing RPC and broken connection
        break;
      }
      default:
        ret = OB_INNER_STAT_ERROR;
        PROXY_CS_LOG(EDIAG, "unexpected event", K_(cs_id), K(event), K(ret));
        break;
    }

    /* 4. set keep alive base on config */
    ObNetVConnection *vc = this->get_netvc();
    //TODO PRPC need update trans_state_.mysql_config_params_ info to set keep alive opt
    if (OB_UNLIKELY(NULL != vc && vc->options_.sockopt_flags_ != get_global_proxy_config().client_sock_option_flag_out)) {
      vc->options_.sockopt_flags_ = static_cast<uint32_t>(get_global_proxy_config().client_sock_option_flag_out);
      if (vc->options_.sockopt_flags_ & ObNetVCOptions::SOCK_OPT_KEEP_ALIVE) {
        vc->options_.set_keepalive_param(static_cast<int32_t>(get_global_proxy_config().client_tcp_keepidle),
              static_cast<int32_t>(get_global_proxy_config().client_tcp_keepintvl),
              static_cast<int32_t>(get_global_proxy_config().client_tcp_keepcnt),
              static_cast<int32_t>(get_global_proxy_config().client_tcp_user_timeout));
      }
      if (OB_FAIL(vc->apply_options())) {
        PROXY_CS_LOG(WDIAG,"client session failed to apply per-transaction socket options", K_(cs_id), K(ret));
      }
    }

    /* 5. read data from vc buffer and init ObReq */
    if (OB_SUCC(ret) && OB_NOT_NULL(get_reader())) {
      ObRpcReq *rpc_req = NULL;
      ObRpcRequestSM *request_sm = NULL;
      ObRpcReqReadStatus status = RPC_REQUEST_READ_CONT;
      obkv::ObProxyRpcType rpc_type = obkv::OBPROXY_RPC_UNKOWN;
      ObIOBufferReader &buffer_reader = *get_reader();
      ObRpcEzHeader ez_header;
      ObRpcReqTraceId rpc_trace_id;
      uint32_t request_id = 0;
      uint32_t client_channel_id = 0;
      int64_t request_len = 0;
      int64_t trace_id1;
      int64_t trace_id2;
      bool is_set_cid_to_req_map = false;
      int64_t pos = 0;

      if (read_begin_ == 0 && OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
        read_begin_ = ObRpcRequestSM::static_get_based_hrtime();
      }

      if (buffer_reader.read_avail() < current_need_read_len_) { //need handle the rpc header
        PROXY_CS_LOG(DEBUG, "data not meet need", K_(cs_id), "avail_len", buffer_reader.read_avail(), K_(current_need_read_len));
      } else {
        if (RPC_NET_HEADER_LENGTH == current_need_read_len_) {
          char *written_pos = buffer_reader.copy(net_head_buf_, RPC_NET_HEADER_LENGTH);
          if (OB_UNLIKELY(written_pos != net_head_buf_ + RPC_NET_HEADER_LENGTH)) {
            ret = OB_ERR_UNEXPECTED;
            PROXY_CS_LOG(WDIAG, "not copy completely", K_(cs_id), K(written_pos), K(net_head_buf_), "meta_length", RPC_NET_HEADER_LENGTH, K(ret));
          } else if (OB_FAIL(current_ez_header_.deserialize(net_head_buf_, RPC_NET_HEADER_LENGTH, pos))) {
            PROXY_CS_LOG(WDIAG, "fail to deserialize ObRpcEzHeader", K_(cs_id), K(written_pos), K(net_head_buf_), "meta_length", RPC_NET_HEADER_LENGTH, K(ret));
          } else {
            current_need_read_len_ = current_ez_header_.ez_payload_size_ + RPC_NET_HEADER_LENGTH;
          }
        }
        if (OB_SUCC(ret) && RPC_NET_HEADER_LENGTH != current_need_read_len_) {
          request_len = current_need_read_len_;

          if (buffer_reader.read_avail() < request_len) {
            // request not completely got from net, next to read it when meet condition
            PROXY_CS_LOG(DEBUG, "response not ready, need get next", K_(cs_id), K(request_len), "data_len", buffer_reader.read_avail());
            status = RPC_REQUEST_READ_CONT;
          } else {
            if (obkv::OBPROXY_RPC_OBRPC != (rpc_type = current_ez_header_.get_rpc_magic_type())) {
              ret = OB_ERR_UNEXPECTED;
              PROXY_CS_LOG(WDIAG, "get an unsupported rpc type", K_(cs_id), K(rpc_type), K(ret));
            } else if (OB_ISNULL(rpc_req = ObRpcReq::allocate())) {
              //TODO handle the error, maybe need broken connection
              ret = OB_ERR_UNEXPECTED;
              PROXY_CS_LOG(WDIAG,"could not allocate request, abort connection if need", K_(cs_id), K(ret));
            } else if (OB_FAIL(rpc_req->alloc_request_buf(request_len + ObProxyRpcReqAnalyzer::OB_RPC_ANALYZE_MORE_BUFF_LEN))) {
              PROXY_CS_LOG(WDIAG,"fail to allocate rpc request object", K_(cs_id), K(ret));
            } else if (OB_ISNULL(request_sm = ObRpcRequestSM::allocate())) {
              ret = OB_ERR_UNEXPECTED;
              PROXY_CS_LOG(WDIAG, "could not allocate request sm, net need abort connection", K_(cs_id), K(ret));
            } else {
              //init rpc request trace id
              int64_t pos = RPC_NET_HEADER_LENGTH + obrpc::ObRpcPacketHeader::RPC_REQ_TRACE_ID_POS;
              char *request_buf = rpc_req->get_request_buf();
              PROXY_CS_LOG(DEBUG, "receive one rpc request has init rpc request done", K_(cs_id), K(ret), K(this));
              // success we need copy net data to request buffer
              buffer_reader.copy(request_buf, request_len);
              buffer_reader.consume(request_len); //clean the net data of the request
              if (OB_UNLIKELY(OB_FAIL(serialization::decode_i64(request_buf, request_len, pos, &trace_id1))
                    || OB_FAIL(serialization::decode_i64(request_buf, request_len, pos, &trace_id2)))) {
                PROXY_CS_LOG(WDIAG, "fail to retrive trace id from request", K_(cs_id), K(ret), K(request_len), K(pos));
              } else {
                PROXY_CS_LOG(DEBUG, "retrive trace id from request", K_(cs_id), K(pos), K(trace_id1), K(trace_id2));
                rpc_trace_id.set_rpc_trace_id(trace_id2, trace_id1);
              }
              status = RPC_REQUEST_READ_DONE;
              PROXY_CS_LOG(DEBUG, "request has read to buffer, consume it", K_(cs_id), K(request_len),
                  "block_count", buffer_reader.get_block_count(),
                  "block_addr", buffer_reader.get_current_block(),
                  "buffer", buffer_reader.mbuf_,
                  "read_avail", buffer_reader.read_avail()
                  );
              RPC_REQ_CNET_ENTER_STATE(rpc_req, ObRpcReq::ClientNetState::RPC_REQ_CLIENT_REQUEST_READ);
              rpc_req->client_timestamp_.client_begin_ = read_begin_;
              rpc_req->client_timestamp_.client_read_end_ = ObRpcRequestSM::static_get_based_hrtime();
              read_begin_ = 0;
            }
          }
        }
      }

      if (OB_FAIL(ret)) {
        status = RPC_REQUEST_READ_ERROR;
      }

      switch (__builtin_expect(status, RPC_REQUEST_READ_DONE)) {
        case RPC_REQUEST_READ_DONE:
          if (OB_NOT_NULL(rpc_req) && OB_NOT_NULL(request_sm)) {
            request_id = current_ez_header_.chid_;
            client_channel_id = atomic_channel_id_++;

            PROXY_CS_LOG(DEBUG, "[RPC_REQUEST]recv a new rpc_req, to handle", K_(cs_id), K(rpc_trace_id), K(ret), KPC(rpc_req));

            if (OB_FAIL(rpc_req->init(rpc_type, request_sm, this, request_len, cluster_version_,
                                      request_id, client_channel_id, cs_id_, rpc_net_vc_, trace_id1, trace_id2))) {
              PROXY_CS_LOG(WDIAG, "failed to init rpc_req", K_(cs_id), K(rpc_trace_id), K(ret), K(this));
            } else if (OB_FAIL(request_sm->init(rpc_req, mutex_))) {
              PROXY_CS_LOG(WDIAG, "failed to init request_sm", K_(cs_id), K(rpc_trace_id), K(ret), K(this));
            } else if (OB_FAIL(cid_to_req_map_.set_refactored(client_channel_id, rpc_req))) {
              PROXY_CS_LOG(WDIAG, "failed to set_refactored", K_(cs_id), K(rpc_trace_id), K(ret), K(this));
            } else {
              RPC_REQ_CNET_ENTER_STATE(rpc_req, ObRpcReq::ClientNetState::RPC_REQ_CLIENT_REQUEST_HANDLING);
              is_set_cid_to_req_map = true;
              if (OB_FAIL(request_sm->schedule_call_next_action(RPC_REQ_NEW_REQUEST))) {
                PROXY_CS_LOG(WDIAG, "fail to call schedule_call_next_action", K(ret), K(request_sm));
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            PROXY_CS_LOG(WDIAG," handle rpc request expected null request", K_(cs_id), K(ret));
          }

          net_entry_.read_vio_->nbytes_ = INT64_MAX;
          net_entry_.read_vio_->reenable(); //need check next data
          current_need_read_len_ = RPC_NET_HEADER_LENGTH;
          current_ez_header_.reset();
          if (OB_SUCC(ret)) {
            PROXY_CS_LOG(DEBUG, "need read next request immediately when request waiting", K_(cs_id), "net_len", buffer_reader.read_avail());
            if (OB_FAIL(setup_client_request_read())) {
              PROXY_CS_LOG(WDIAG, "fail to call setup_client_request_read", K_(cs_id), K(ret));
            }
          }
          break;
        case RPC_REQUEST_READ_CONT:
          if (net_entry_.eos_) {
            ret = OB_CONNECT_ERROR;
            PROXY_CS_LOG(WDIAG,"EOS before client request parsing finished", K_(cs_id), K(ret));
            //TODO PRPC client need abort and broken connection
            net_entry_.read_vio_->nbytes_ = net_entry_.read_vio_->ndone_;//client_entry_->read_vio_->ndone_;
          } else {
            if (request_len > 0 && request_len > buffer_reader.mbuf_->water_mark_) {
              buffer_reader.mbuf_->water_mark_ = request_len;
            }

            net_entry_.read_vio_->reenable(); //need more data
            event_ret = VC_EVENT_CONT;
          }
          break;
        case RPC_REQUEST_READ_ERROR:
          ret = OB_ERR_UNEXPECTED;
          PROXY_CS_LOG(WDIAG,"error parsing client request", K_(cs_id), K(ret));
          net_entry_.read_vio_->nbytes_ = net_entry_.read_vio_->ndone_;
          break;
        default:
          ret = OB_INNER_STAT_ERROR;
          PROXY_CS_LOG(EDIAG,"unknown analyze mysql request status", K_(cs_id), K(status), K(ret));
          break;
      }

      if (OB_FAIL(ret)) { // clear all buffer
        int tmp_ret = ret;
        if (is_set_cid_to_req_map && OB_FAIL(cid_to_req_map_.erase_refactored(client_channel_id))) {
          PROXY_CS_LOG(WDIAG, "fail to call erase_refactored", K_(cs_id), K(ret), K(client_channel_id));
        }
        if (OB_NOT_NULL(rpc_req)) {
          PROXY_CS_LOG(INFO, "state_client_request_read error, cleanup rpc_req", K_(cs_id), KPC(rpc_req));
          ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ClientNetState::RPC_REQ_CLIENT_DONE);
          rpc_req->cleanup(cleanup_params);
        }
        ret = tmp_ret;
      }
    }
  }

  if (OB_FAIL(ret)) {
    //error state, need abort client session
    do_io_close();
  }

  return ret;
}

int ObRpcClientNetHandler::calc_response_need_send(int64_t &count)
{
  int ret = OB_SUCCESS;
  int64_t need_send_response_count = need_send_response_list_.size();
  int64_t max_response_count = get_global_proxy_config().rpc_max_response_batch_size;

  //TODO : add response bytes limite

  if (need_send_response_count > max_response_count) {
    need_send_response_count = max_response_count;
  }

  count = need_send_response_count;
  PROXY_LOG(DEBUG, "ObRpcClientNetHandler::calc_response_need_send done", K(count));

  return ret;
}

int ObRpcClientNetHandler::store_rpc_req_into_response_buffer(int64_t need_send_resp_count, int64_t &send_response, int64_t &total_response_len)
{
  int ret = OB_SUCCESS;
  ObRpcReq *rpc_req = NULL;
  ObMIOBuffer *response_buffer = net_entry_.write_buffer_;
  int64_t written_len = 0;
  int64_t response_len = 0;
  char *buf = NULL;

  while (OB_SUCC(ret) && !need_send_response_list_.empty() && send_response < need_send_resp_count) {
    written_len = 0;

    if (OB_FAIL(need_send_response_list_.pop_front(rpc_req))) {
      PROXY_CS_LOG(WDIAG, "fail to pop need send response", K_(cs_id), K(ret));
    } else if (OB_ISNULL(rpc_req)) {
      PROXY_CS_LOG(WDIAG, "need send response is invalid", K_(cs_id), K(ret));
    } else if (OB_FAIL(handle_response_rewrite_channel_id(rpc_req))) {
      PROXY_CS_LOG(WDIAG, "rpc_req convert channel_id failed", K_(cs_id), K(rpc_req));
    } else {
      const ObRpcReqTraceId &rpc_trace_id = rpc_req->get_trace_id();
      RPC_REQ_CNET_ENTER_STATE(rpc_req, ObRpcReq::ClientNetState::RPC_REQ_CLIENT_RESPONSE_HANDLING);

      response_len = rpc_req->get_response_len();
      if (!rpc_req->is_use_response_inner_buf()) {
        buf = rpc_req->get_response_buf();
      } else {
        buf = rpc_req->get_response_inner_buf();
      }

      if (OB_FAIL(response_buffer->write(buf, response_len, written_len))) {
        PROXY_CS_LOG(WDIAG, "response is not written completely, all rpc_req handle failed", K_(cs_id), K(written_len), K(response_len), K(rpc_trace_id));
      } else if (OB_UNLIKELY(response_len != written_len)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CS_LOG(WDIAG, "response is not written completely, all rpc_req handle failed", K_(cs_id), K(written_len), K(response_len), K(rpc_trace_id));
      } else if (OB_FAIL(sending_response_list_.push_back(rpc_req))) {
        PROXY_CS_LOG(WDIAG, "response push back into sending_response_list failed", K_(cs_id), K(response_len), K(rpc_trace_id));
      } else {
        PROXY_CS_LOG(DEBUG, "[RPC_REQUEST]sending response...", K_(cs_id), KPC(rpc_req), K(this), K(rpc_trace_id));
        RPC_REQ_CNET_ENTER_STATE(rpc_req, ObRpcReq::ClientNetState::RPC_REQ_CLIENT_RESPONSE_SEND);
        send_response++;
        total_response_len += response_len;
      }
    }
  }

  return ret;
}

int ObRpcClientNetHandler::setup_client_response_send()
{
  int ret = OB_SUCCESS;
  //set read trigger and read_reschedule. sometimes the data already is in the io buffer
  PROXY_CS_LOG(DEBUG, "ObRpcServerNetHandler::setup_client_response send", K_(cs_id), "request_count", need_send_response_list_.size());
  static_cast<ObUnixNetVConnection *>(this->get_netvc())->set_read_trigger();
  pending_action_ = NULL;

  if (OB_LIKELY(!need_send_response_list_.empty()) && !is_sending_response_) {
    int64_t send_response = 0;
    int64_t total_response_len = 0;
    ObIOBufferReader *buf_start = NULL;
    int64_t need_send_resp_count = 0;

    if (OB_ISNULL(net_entry_.write_buffer_)) {
      net_entry_.write_buffer_ = new_empty_miobuffer(MYSQL_BUFFER_SIZE);
    } else {
      net_entry_.write_buffer_->reset(); //cleanup
      net_entry_.write_buffer_->dealloc_all_readers();
    }

    if (OB_ISNULL(buf_start = net_entry_.write_buffer_->alloc_reader())) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CS_LOG(WDIAG, "setup_client_response_send failed to allocate iobuffer reader", K_(cs_id), K(ret));
    } else if (OB_FAIL(calc_response_need_send(need_send_resp_count))) {
      PROXY_CS_LOG(WDIAG, "fail to call calc_response_need_send", K(ret), K_(cs_id), K(ret));
    } else if (OB_FAIL(store_rpc_req_into_response_buffer(need_send_resp_count, send_response, total_response_len))) {
      PROXY_CS_LOG(WDIAG, "fail to call store_rpc_req_into_response_buffer", K(ret), K_(cs_id), K(ret));
    } else if (send_response > 0) {
      if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
        write_begin_ = ObRpcRequestSM::static_get_based_hrtime(); /* record begin time to write */
      }
      // MUTEX_TRY_LOCK(lock, rpc_net_vc_->mutex_, create_thread_);
      // TODO: Using MUTEX_ LOCK may affect performance. In the future, consider using different mutexes for asynchronous tasks within RPC requests
      // compared to client VC to prevent race conditions
      MUTEX_LOCK(lock, rpc_net_vc_->mutex_, create_thread_);    // TODO: check CLIENT_VC_SWAP_MUTEX_EVENT event
      /* check mutex_->thread_holding_ is same with create_thread_ to avoid
        writing failed with net_entry_.write_vio_ in not null */
      if (create_thread_ != mutex_->thread_holding_ || OB_ISNULL(net_entry_.write_vio_ = do_io_write(this, total_response_len, buf_start))) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CS_LOG(WDIAG, "client entry failed to do_io_write", K_(cs_id), K(send_response), K(total_response_len), KP_(create_thread), KP(this_ethread()),
                    KP(mutex_->thread_holding_), K(mutex_.ptr_));
      } else {
        is_sending_response_ = true;
      }
    }
  } else {
    //do nothing
    PROXY_CS_LOG(DEBUG, "client net is in sending state, need to wait complete for last", K_(cs_id), "waiting_count", need_send_response_list_.size());
  }

  if (OB_FAIL(ret)) {
    do_io_close();
  }

  return ret;
}

int ObRpcClientNetHandler::state_client_response_send(int event, void *data)
{
  int ret = OB_SUCCESS;
  bool need_terminal = false;
  ObRpcReq *rpc_req = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INNER_STAT_ERROR;
    PROXY_CS_LOG(EDIAG,"invalid internal state, server entry is NULL or data is NULL",
            /* K_(net_entry), */
            K_(cs_id), K(data), K(ret));
  } else if (OB_UNLIKELY(net_entry_.read_vio_ != reinterpret_cast<ObVIO *>(data)
             && net_entry_.write_vio_ != reinterpret_cast<ObVIO *>(data))) {
    ret = OB_INNER_STAT_ERROR;
    PROXY_CS_LOG(EDIAG,"invalid internal state, server entry read vio isn't the same as data,"
              "and server entry write vio isn't the same as data",
              K_(cs_id), K_(net_entry_.read_vio),
              K_(net_entry_.write_vio), K(data), K(ret));

  } else {
    ObHRTime write_done = 0;
    switch (event) {
      case VC_EVENT_WRITE_READY:
        net_entry_.write_vio_->reenable();
        break;
      case VC_EVENT_WRITE_COMPLETE:
        is_sending_response_ = false;
        if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
          write_done = ObRpcRequestSM::static_get_based_hrtime();
        }
        while (OB_SUCC(ret) && !sending_response_list_.empty()) {
          if (OB_FAIL(sending_response_list_.pop_front(rpc_req))) {
            ret = OB_ERR_UNEXPECTED;
            PROXY_CS_LOG(WDIAG, "fail to pop sending response", K_(cs_id), K(ret));
          } else if (OB_NOT_NULL(rpc_req)) {
            uint32_t key = rpc_req->get_client_channel_id();
            const ObRpcReqTraceId &rpc_trace_id = rpc_req->get_trace_id();

            if (OB_FAIL(cid_to_req_map_.erase_refactored(key))) { //remove it
              // do nothing
              if (OB_HASH_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
                PROXY_CS_LOG(INFO, "rpc_req is not in cid_to_req_map, just destory", K_(cs_id), "rpc_req", *rpc_req, K(ret), K(key), K(rpc_trace_id));
              } else {
                PROXY_CS_LOG(WDIAG, "fail to call erase_refactored", K_(cs_id), "rpc_req", *rpc_req, K(ret), K(key), K(rpc_trace_id));
              }
            } else {
              rpc_req->client_timestamp_.client_write_begin_ = write_begin_;
              rpc_req->client_timestamp_.client_end_ = write_done;
            }
            if (OB_SUCC(ret)) {
              if (rpc_req->is_need_terminal_client_net()) {
                need_terminal = true;
              }
              ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ClientNetState::RPC_REQ_CLIENT_DONE);
              rpc_req->cleanup(cleanup_params);
              rpc_req = NULL;
            }
          } else {
            //do nothing
            PROXY_CS_LOG(DEBUG, "ObRpcServerNetHandler::state_client_response_send rpc_req is NULL", K_(cs_id), K(event), K(data));
          }
        }
        sending_response_list_.reset();
        write_begin_ = 0;

        break;
      case VC_EVENT_READ_READY:
      case VC_EVENT_READ_COMPLETE:
        //do nothing, not to be here
        break;
      case VC_EVENT_EOS:
        net_entry_.eos_ = true;
        break;
      case VC_EVENT_ERROR:
        ret = OB_CONNECT_ERROR;
        //may entry if broken, request need retry
        break;
      default:
        ret = OB_INNER_STAT_ERROR;
        PROXY_CS_LOG(EDIAG,"Unknown event", K_(cs_id), K(event), K(ret));
        break;
    }
  }

  if (OB_FAIL(ret) || need_terminal || net_entry_.eos_) {
    PROXY_CS_LOG(WDIAG, "state_client_response_send failed or get need_terminal", K_(cs_id), K(ret), K(need_terminal), K(event));
    do_io_close();
  } else if (OB_SUCC(ret) && need_send_response_list_.size() > 0) {
    // Responses are returned concurrently on a single connection. Here you need to check whether there are any responses that have not been returned.
    PROXY_CS_LOG(DEBUG, "state_client_response_send need_send_response is not empty, send again", K_(cs_id), K(ret), K_(need_send_response_list));
    if (OB_FAIL(setup_client_response_send())) {
      PROXY_CS_LOG(WDIAG, "fail to send again", K_(cs_id), K(ret), K_(need_send_response_list));
    }
  }

  return ret;
}

int ObRpcClientNetHandler::handle_response_rewrite_channel_id(ObRpcReq *rpc_req)
{
  int ret = OB_SUCCESS;
  const ObRpcReqTraceId &rpc_trace_id = rpc_req->get_trace_id();
  uint32_t request_id = rpc_req->get_origin_channel_id();
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = ObRpcEzHeader::RPC_PKT_CHANNEL_ID_POS;

  if (rpc_req->is_use_response_inner_buf()) {
    buf = rpc_req->get_response_inner_buf();
    buf_len = rpc_req->get_response_inner_buf_len();
  } else {
    buf = rpc_req->get_response_buf();
    buf_len = rpc_req->get_response_buf_len();
  }

  if (OB_ISNULL(buf) || 0 == buf_len) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WDIAG, "rpc req response buf is invalid", K_(cs_id), K(rpc_trace_id));
  } else if (OB_FAIL(common::serialization::encode_i32(buf, buf_len, pos, request_id))) {
    PROXY_CS_LOG(WDIAG, "fail to encode response channel id", K_(cs_id), K(rpc_trace_id));
  }

  return ret;
}

int ObRpcClientNetHandler::schedule_send_response_action()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL != pending_action_)) {
    // do nothing
    PROXY_LOG(DEBUG, "pending send_response_action, do nothing", K_(cs_id), K_(pending_action), K(ret));
  } else if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, RPC_CLIENT_NET_SEND_RESPONSE))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(EDIAG, "fail to schedule send_response", K_(cs_id), K_(pending_action), K(ret));
  } else {
    PROXY_LOG(DEBUG, "succ to schedule send_response for ObRpcClientNetHandler", K_(cs_id), K(pending_action_));
  }

  return ret;
}
int ObRpcClientNetHandler::cancel_pending_action()
{
  int ret = OB_SUCCESS;

  if (NULL != pending_action_) {
    if (OB_FAIL(pending_action_->cancel())) {
      PROXY_LOG(WDIAG, "fail to cancel pending task", K_(cs_id), K_(pending_action), K(ret));
    } else {
      pending_action_ = NULL;
    }
  }

  return ret;
}

void ObRpcClientNetHandler::clean_all_pending_request()
{
  RPC_PKT_REQ_MAP::iterator iter = cid_to_req_map_.begin();
  ObRpcReq *rpc_req = NULL;
  for (; iter != cid_to_req_map_.end(); iter++) {
    if (OB_NOT_NULL(rpc_req = iter->second)) {
      PROXY_CS_LOG(INFO, "client net handle do io close, clean pending rpc req", K_(cs_id), KPC(rpc_req));
      rpc_req->client_net_cancel_request();  // client net done
      ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ClientNetState::RPC_REQ_CLIENT_CANCLED);
      rpc_req->cleanup(cleanup_params);
      rpc_req = NULL;
    }
  }
  cid_to_req_map_.destroy();
}

void ObRpcClientNetHandler::clean_all_timeout_request()
{
  int ret = OB_SUCCESS;
  RPC_PKT_REQ_MAP::iterator iter = cid_to_req_map_.begin();
  ObRpcReq *rpc_req = NULL;
  int64_t current_time_us = common::ObTimeUtility::current_time();
  ObRpcReqList clean_list;
  uint32_t key = 0;

  PROXY_LOG(DEBUG, "ObRpcClientNetHandler::clean_all_timeout_request", K_(cs_id));

  for (;OB_SUCC(ret) && iter != cid_to_req_map_.end(); iter++) {
    if (OB_NOT_NULL(rpc_req = iter->second) && rpc_req->get_cnet_state() < ObRpcReq::ClientNetState::RPC_REQ_CLIENT_RESPONSE_HANDLING) {
      if ((0 != rpc_req->get_client_net_timeout_us() &&
           rpc_req->get_client_net_timeout_us() < current_time_us) ||
          rpc_req->canceled()) {
        if (OB_FAIL(clean_list.push_back(rpc_req))) {
          PROXY_CS_LOG(WDIAG, "fail to push rpc request to clean list", K_(cs_id), K(ret), K(rpc_req));
        }
        rpc_req = NULL;
      }
    }
  }

  while (OB_SUCC(ret) && !clean_list.empty()) {
    if (OB_FAIL(clean_list.pop_front(rpc_req))) {
      PROXY_LOG(WDIAG, "fail to pop need clean request", K_(cs_id), K(ret));
    } else if (OB_NOT_NULL(rpc_req)) {
      key = rpc_req->get_client_channel_id();
      if (OB_FAIL(cid_to_req_map_.erase_refactored(key))) {
        PROXY_LOG(WDIAG, "fail to call erase_refactored for clean_list, do nothing", K_(cs_id), K(ret), KPC(rpc_req));
      } else {
        const ObRpcReqTraceId &rpc_trace_id = rpc_req->get_trace_id();
        PROXY_LOG(INFO, "ObRpcClientNetHandler::clean_all_timeout_request clean rpc_req", K_(cs_id), KPC(rpc_req), K(rpc_trace_id));
        rpc_req->client_net_cancel_request();  // client net done
        ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ClientNetState::RPC_REQ_CLIENT_CANCLED);
        rpc_req->cleanup(cleanup_params);
      }
      rpc_req = NULL;
    }
  }
}

int ObRpcClientNetHandler::schedule_period_task()
{
  int ret = OB_SUCCESS;
  ObHRTime period_task_time = HRTIME_USECONDS(get_global_proxy_config().rpc_period_task_interval);

  if (OB_UNLIKELY(NULL != period_task_action_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(WDIAG, "period_task_action must be NULL here", K_(cs_id), K_(period_task_action), K(ret));
  } else if (OB_ISNULL(create_thread_) || create_thread_ != this_ethread()) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(WDIAG, "ObRpcClientNetHandler::schedule_period_task get wrong thread", K_(cs_id), KP_(create_thread), KP(this_ethread()));
  } else if (OB_ISNULL(period_task_action_ = self_ethread().schedule_every(this, period_task_time, RPC_CLIENT_NET_PERIOD_TASK))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(EDIAG, "fail to schedule timeout", K_(cs_id), K(period_task_action_), K(ret));
  } else {
    PROXY_LOG(DEBUG, "succ to schedule repeat task for ObRpcClientNetHandler", K_(cs_id), K(period_task_time));
  }

  return ret;
}

int ObRpcClientNetHandler::cancel_period_task()
{
  int ret = OB_SUCCESS;

  if (NULL != period_task_action_) {
    if (OB_FAIL(period_task_action_->cancel())) {
      PROXY_LOG(WDIAG, "fail to cancel repeat task", K_(cs_id), K_(period_task_action), K(ret));
    } else {
      period_task_action_ = NULL;
    }
  }

  return ret;
}

int ObRpcClientNetHandler::handle_period_task()
{
  int ret = OB_SUCCESS;

  PROXY_LOG(DEBUG, "ObRpcClientNetHandler::handle_period_task", K_(cs_id));
  // 1. clean timeout request
  clean_all_timeout_request();

  return ret;
}

int ObRpcClientNetHandler::handle_client_entry_setup_error(int event, void *data)
{
  int ret = OB_SUCCESS;
  UNUSED(event);
  UNUSED(data);
  return ret;
}

int ObRpcClientNetHandlerMap::set(ObRpcClientNetHandler &cs)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(id_map_.unique_set(&cs))) {
    PROXY_CS_LOG(DEBUG, "succ to set client session", K(cs.get_cs_id()));
  } else {
    PROXY_CS_LOG(WDIAG, "fail to set client session", K(cs.get_cs_id()), KP(this), K(ret));
  }
  return ret;
}

int ObRpcClientNetHandlerMap::get(const uint32_t &id, ObRpcClientNetHandler *&cs)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(id_map_.get_refactored(id, cs))) {
    PROXY_CS_LOG(DEBUG, "succ to get client session", K(id), K(ret));
  } else {
    PROXY_CS_LOG(DEBUG, "fail to get client session", K(id), KP(this), K(ret));
  }
  return ret;
}

int ObRpcClientNetHandlerMap::erase(const uint32_t &id)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(id_map_.erase_refactored(id))) {
    ret = OB_SUCCESS;
  } else {
    PROXY_CS_LOG(WDIAG, "fail to erase client session", K(id), KP(this), K(ret));
  }
  return ret;
}

int init_rpc_net_cs_map_for_thread()
{
 int ret = OB_SUCCESS;
 const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
 for (int64_t i = 0; i < event_thread_count && OB_SUCC(ret); ++i) {
   if (OB_ISNULL(g_event_processor.event_thread_[ET_CALL][i]->rpc_net_cs_map_ = new (std::nothrow) ObRpcClientNetHandlerMap())) {
     ret = OB_ALLOCATE_MEMORY_FAILED;
     PROXY_NET_LOG(WDIAG, "fail to new ObInactivityCop", K(i), K(ret));
   }
 }
 return ret;
}




} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
