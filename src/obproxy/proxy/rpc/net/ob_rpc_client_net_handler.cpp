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
#include "omt/ob_proxy_config_table_processor.h"
#include "omt/ob_white_list_table_processor.h"
#include "proxy/rpc/net/ob_rpc_client_net_handler.h"
#include "proxy/rpc/net/ob_rpc_obkv_client_net_handler.h"
#include "proxy/rpc/net/ob_rpc_redis_client_net_handler.h"
#include "proxy/rpc/net/ob_rpc_server_net_handler.h"
#include "proxy/rpc/ob_rpc_req_debug_names.h"
#include "proxy/rpc/ob_rpc_request_sm.h"
#include "obkv/table/ob_rpc_struct.h"
#include "prometheus/ob_prometheus_info.h"
#include "prometheus/ob_rpc_prometheus.h"
#include "obproxy/stat/ob_rpc_stats.h"

using namespace oceanbase::common;
using namespace oceanbase::proxy_protocol_v2;
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
      vc_ready_killed_(false), half_close_(false),
      cluster_resource_(NULL), cluster_version_(0), timeout_event_(OB_TIMEOUT_UNKNOWN_EVENT), timeout_record_(0),
      dummy_entry_(NULL), is_need_update_dummy_entry_(false),
      dummy_ldc_(), dummy_entry_valid_time_ns_(0),
      conn_channel_id_(0), conn_unique_id_(0), conn_seq_(0),
      magic_(RPC_C_NET_MAGIC_DEAD), create_thread_(NULL), is_local_connection_(false),
      in_list_stat_(LIST_INIT), current_tid_(-1),
      cs_id_(0), atomic_channel_id_(0), proxy_sessid_(0),
      using_ldg_(false), tcp_init_cwnd_set_(0), read_state_(MCS_INIT), active_(true),
      is_sending_response_(false), need_delete_cluster_(false), is_first_request_(true), server_state_version_(0),
      ct_info_(), last_server_ip_(), pending_action_(NULL),
      session_info_(), net_head_buf_(), is_proxy_protocol_v2_request_(true), proxy_protocol_v2_()
{
  SET_HANDLER(&ObRpcClientNetHandler::main_handler);
}

void ObRpcClientNetHandler::cleanup()
{
  PROXY_CS_LOG(INFO, "rpc client session cleanup", K_(cs_id), K_(proxy_sessid), KP_(rpc_net_vc));

  if (OB_UNLIKELY(NULL != rpc_net_vc_)
      || OB_ISNULL(read_buffer_)) {
    PROXY_CS_LOG(WDIAG, "invalid rpc client session", K_(cs_id), K(rpc_net_vc_), K(read_buffer_));
  }
  is_local_connection_ = false;

  if (NULL != dummy_entry_) {
    dummy_entry_->dec_ref();
    dummy_entry_ = NULL;
  }

  dummy_ldc_.reset();
  dummy_entry_valid_time_ns_ = 0;

  if (NULL != cluster_resource_) {
    PROXY_CS_LOG(DEBUG, "client session cluster resource will dec ref", K_(cs_id), K_(cluster_resource), KPC_(cluster_resource));
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

}

void ObRpcClientNetHandler::destroy()
{
  cleanup();
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
    PROXY_CS_LOG(WDIAG, "invalid client connection", K(new_vc), K(rpc_net_vc_), K(ret), K_(cs_id));
  } else {
    PROXY_CS_LOG(DEBUG, "ObRpcClientNetHandler::new_connection", K(new_vc), K(iobuf), K(reader),
        "this_thread", this_ethread(), K_(cs_id));
    create_thread_ = this_ethread();
    rpc_net_vc_ = new_vc;
    magic_ = RPC_C_NET_MAGIC_ALIVE;
    mutex_ = new_vc->mutex_;

    MUTEX_TRY_LOCK(lock, mutex_, this_ethread());
    if (OB_LIKELY(lock.is_locked())) {
      current_tid_ = GETTID();
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
        PROXY_CS_LOG(EDIAG, "fail to alloc memory for read_buffer", K_(cs_id), K(ret));
      }

      if (OB_SUCC(ret)) {
        if (NULL != reader) {
          // buffer_reader_ = reader;
          buf_reader_ = reader;
        } else if (OB_ISNULL(buf_reader_ = read_buffer_->alloc_reader())) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_CS_LOG(EDIAG, "fail to alloc buffer reader", K(ret), K_(cs_id));
        }
      }

      if (OB_SUCC(ret)) {
        /**
         * we cache the request in the read io buffer, so the water mark of
         * read io buffer must be larger than the reqeust packet size. we set
         * RPC_NET_HEADER_LENGTH as the default water mark, when we read the
         * header of request, we reset the water mark.
         */
        read_buffer_->water_mark_ = RPC_NET_HEADER_LENGTH; //TODO change water_mark
        // start listen event on client vc
        if (OB_FAIL(acquire_client_session_id())) {
          PROXY_CS_LOG(WDIAG, "fail to acquire client session_id", K_(cs_id), K(ret));
        } else if (OB_FAIL(acquire_client_session_id())) {
          PROXY_CS_LOG(WDIAG, "fail to acquire connection unique id", K_(conn_unique_id), K_(cs_id), K(ret));
        } else if (OB_FAIL(add_to_list())) {
          PROXY_CS_LOG(WDIAG, "fail to add cs to list", K_(cs_id), K(ret));
        } else if (OB_FAIL(session_info_.init())) {
          PROXY_CS_LOG(WDIAG, "fail to init session_info", K_(cs_id), K(ret));
        } else if (OB_FAIL(get_vip_addr())) {
          PROXY_CS_LOG(WDIAG, "get vip addr failed", K_(cs_id), K(ret));
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
              session_info_.set_vip_addr_name(ct_info_.vip_tenant_.vip_addr_);
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
            // if (OB_FAIL(schedule_period_task())) { //TODO obkv need init period task
            //   PROXY_CS_LOG(WDIAG, "fail to call schedule_period_task", K_(cs_id), K(ret));
            // } else
            if (OB_FAIL(setup_client_request_read())) {
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
  const uint32_t thread_id_bits       = 32 - get_thread_id_bits();
  const uint32_t local_seq_bits       = 32 - proxy_head_bits - upgrade_ver_bits - thread_id_bits;

  const uint32_t proxy_id_offset      = 32 - proxy_head_bits;
  const uint32_t upgrade_ver_offset   = 32 - proxy_head_bits - upgrade_ver_bits;
  const uint32_t thread_id_offset     = local_seq_bits;

  const uint32_t proxy_id      = static_cast<uint32_t>(get_global_proxy_config().proxy_id);
  const uint32_t upgrade_ver   = static_cast<uint32_t>(0x1 & get_global_hot_upgrade_info().upgrade_version_); //only use the tail bits

  uint32_t tmp_thread_id = 0;
  if (thread_id < 0 || thread_id >= g_event_processor.thread_count_for_type_[ET_NET]) {// use curr ethread
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

int ObRpcClientNetHandler::acquire_conn_unique_id()
{
  int ret = OB_SUCCESS;
  const ObAddr &addr =  get_real_client_addr();
  int64_t ip = addr.get_ipv4();
  int64_t port = int64_t(addr.get_port()) << 32;
  int64_t is_user_req = int64_t(1) << (32 + 16);
  int64_t reserved = 0;
  conn_unique_id_ = (ip | port | is_user_req | reserved);
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
  //   // clean all rpc req
  //   if (OB_FAIL(cancel_period_task())) {
  //     PROXY_CS_LOG(WDIAG, "fail to call cancel_period_task", K_(cs_id));
  //   } else if (OB_FAIL(cancel_pending_action())) {
  //     PROXY_CS_LOG(WDIAG, "fail to call cancel_pending_action", K_(cs_id));
  //   }
  //   clean_all_pending_request();

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

//just release and clean object for detect session not to break net_vc
void ObRpcClientNetHandler::do_io_release()
{
  int ret = OB_SUCCESS;
  PROXY_CS_LOG(DEBUG, "rpc client session handle to release", K_(cs_id), K_(proxy_sessid), KP_(rpc_net_vc));
  if (OB_NOT_NULL(rpc_net_vc_)) {
    PROXY_CS_LOG(WDIAG, "invalid client net to do_io_release", K_(cs_id), K_(rpc_net_vc), K(this));
    rpc_net_vc_->do_io_close();
    rpc_net_vc_ = NULL;
  }

  if (LIST_ADDED == in_list_stat_) {
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

  magic_ = RPC_C_NET_MAGIC_DEAD;
  if (OB_NOT_NULL(read_buffer_)) {
    PROXY_CS_LOG(WDIAG, "invalid client net to do_io_release", K_(cs_id), K_(read_buffer), K(this));
    free_miobuffer(read_buffer_);
    read_buffer_ = NULL;
  }

  //not to handle net_entry_

    if (conn_prometheus_decrease_) {
    RPC_NET_SESSION_PROMETHEUS_STAT(session_info_, PROMETHEUS_CURRENT_SESSION, true, -1);
    conn_prometheus_decrease_ = false;
  }
  session_info_.destroy();
  ObRpcNetHandler::cleanup();
  create_thread_ = NULL;

  op_reclaim_free(this);
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
          // if fail, will do io close, can not print cs_id
            PROXY_CS_LOG(WDIAG, "fail to call state_client_request_read", K(ret));
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
      case VC_EVENT_NET_READ_TIMEOUT:
      case VC_EVENT_NET_WRITE_TIMEOUT:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_INACTIVITY_TIMEOUT: {
        if (MCS_HALF_CLOSED == read_state_) {
          half_close_ = false;
        }
        ObIpEndpoint client_ip;
        if (NULL != rpc_net_vc_) {
          if (OB_UNLIKELY(!ops_ip_copy(client_ip, rpc_net_vc_->get_remote_addr()))) {
            PROXY_CS_LOG(WDIAG, "fail to ops_ip_copy client_ip", K_(cs_id), K(rpc_net_vc_));
          }
        }
        // Keep-alive timed out
        if (VC_EVENT_INACTIVITY_TIMEOUT == event) {
          PROXY_CS_LOG(WDIAG, "client connection is idle over wait_timeout, now we will close it.",
                      "wait_timeout(s)", hrtime_to_sec(rpc_net_vc_->get_inactivity_timeout()),
                       K_(cs_id),
                       K(client_ip),
                       "event", ObRpcReqDebugNames::get_event_name(event));
        } else if (VC_EVENT_NET_READ_TIMEOUT == event) {
          PROXY_CS_LOG(WDIAG, "client connection net read timeout, now we will close it.",
                       "net read timeout(s)", hrtime_to_sec(rpc_net_vc_->get_net_read_timeout()),
                       K_(cs_id),
                       K(client_ip),
                       "event", ObRpcReqDebugNames::get_event_name(event));
        } else if (VC_EVENT_NET_WRITE_TIMEOUT == event) {
          PROXY_CS_LOG(WDIAG, "client connection net write timeout, now we will close it.",
                      "net write timeout(s)", hrtime_to_sec(rpc_net_vc_->get_net_write_timeout()),
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
  //do nothing
  if (OB_LIKELY(RPC_C_NET_MAGIC_ALIVE == magic_)) {
    if (RPC_CLIENT_NET_SEND_RESPONSE == event) {
      event_ret = setup_client_response_send();
    } else if (RPC_CLIENT_NET_READ_REQUEST == event) {
      event_ret = setup_client_request_read();
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
      } else {
        read_state_ = MCS_KEEP_ALIVE;
        net_entry_.read_vio_ = do_io_read(this, INT64_MAX, read_buffer_);
        // TODO: add keep alive
        // if (OB_LIKELY(last_ss_keep_alive_vio_ != ka_vio_)) {
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
        bool found_servers_changed = false;
        if (OB_FAIL(dummy_ldc.assign(dummy_entry_->get_tenant_servers(), simple_servers_info,
            new_idc_name, is_base_servers_added, cluster_resource->get_cluster_name(),
            cluster_resource->get_cluster_id(), found_servers_changed))) {
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
        if (OB_SUCC(ret) && OB_UNLIKELY(found_servers_changed)) {
          if (!dummy_entry_->is_sys_dummy_entry() && dummy_entry_->cas_set_dirty_state()) {
            PROXY_CS_LOG(WDIAG, "dummy_entry isn't AVAIL state, can't set it dirty", KPC_(dummy_entry), K(ret));
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

  if (is_proxy_protocol_v2_request()) {
    buf_reader_->mbuf_->water_mark_ = ProxyProtocolV2::PROXY_PROTOCOL_V2_HEADER_LEN;
    read_num = ProxyProtocolV2::PROXY_PROTOCOL_V2_HEADER_LEN;
  }

  if (OB_ISNULL(net_entry_.read_vio_ = this->do_io_read(this, read_num, buf_reader_->mbuf_))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WDIAG, "rpc client net handler failed to do_io_read", K_(cs_id), K(ret));
  } else if (OB_UNLIKELY(get_global_proxy_config().enable_rpc_throttle && get_global_rpc_throttle().is_freeze_request())) {
    // only print log
    PROXY_CS_LOG(WDIAG, "rpc req reach max mem limit,stop read request!!!",
                  K(obkv::get_global_rpc_throttle().get_holding_resource()), K(ret));
  } else {
    if (buf_reader_->read_avail() > 0) {
      PROXY_CS_LOG(DEBUG, "the request already in buffer, continue to handle it",
            K_(cs_id), "buffer len", buf_reader_->read_avail());
      state_client_request_read(VC_EVENT_READ_READY, net_entry_.read_vio_);
      // handle_event(VC_EVENT_READ_READY, net_entry_.read_vio_);
    }
  }

  //or do nothing

  return ret;
}

int ObRpcClientNetHandler::state_client_request_read(int event, void *data)
{
  int ret = OB_SUCCESS;

  int event_ret = VC_EVENT_NONE;
  UNUSED(event_ret);


  UNUSED(event);
  UNUSED(data);

  STATE_ENTER(ObRpcClientNetHandler::state_client_request_read, event, data);

   bool need_release = false;

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
      case VC_EVENT_ERROR: {
        PROXY_CS_LOG(WDIAG, "ObRpcClientNetHandler::state_client_request_read", "event",
                 ObRpcReqDebugNames::get_event_name(event), K_(cs_id), "client_vc", P(this->get_netvc()));
        ret = OB_CONNECT_ERROR;
        break;
      }
      default:
        ret = OB_INNER_STAT_ERROR;
        PROXY_CS_LOG(EDIAG, "unexpected event", K_(cs_id), K(event), K(ret));
        break;
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(get_reader())) {
      ObIOBufferReader &buffer_reader = *get_reader();
      ObRpcReqReadStatus status = RPC_REQUEST_READ_CONT;
      bool need_read_more_here = false;

      int64_t current_need_read_len = RPC_NET_DETECT_HRD_LEN;
      if (buffer_reader.read_avail() < current_need_read_len) { //DETECT_HRD_LEN > PROXY_PROTOCOL_V2_VALIDATE_LEN(4)
        need_read_more_here = true;
        PROXY_CS_LOG(DEBUG, "data not meet need", K_(cs_id), "avail_len", buffer_reader.read_avail(), K(current_need_read_len));
      } else {
        char *written_pos = buffer_reader.copy(net_head_buf_, current_need_read_len);
        if (OB_UNLIKELY(written_pos != net_head_buf_ + RPC_NET_DETECT_HRD_LEN)) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_CS_LOG(WDIAG, "not copy completely", K_(cs_id), K(written_pos),
                       K(net_head_buf_), "meta_length", current_need_read_len, K(ret));
        } else if (is_first_request_ && proxy_protocol_v2::ProxyProtocolV2::check_proxy_protocol_v2_valid(net_head_buf_)) {
          need_read_more_here = true;
          if (OB_FAIL(handle_proxy_protocol_v2_request(proxy_protocol_v2_, status))) {
            PROXY_CS_LOG(WDIAG, "fail to handle proxy protocol v2", K(ret), K_(cs_id), K(status));
          }
        } else {
          set_proxy_protocol_v2_request(false);
          obkv::ObProxyRpcType rpc_type = obkv::ObRpcEzHeader::check_rpc_magic_type(net_head_buf_, current_need_read_len);
          switch (rpc_type) {
            case obkv::OBPROXY_RPC_OBRPC: //obkv
            {
              // ObRpcClientNetHandler *new_session = op_reclaim_alloc(ObRpcClientNetHandler);
              ObRpcOBKVClientNetHandler *new_session = op_reclaim_alloc(ObRpcOBKVClientNetHandler);
              if (OB_ISNULL(new_session)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                PROXY_NET_LOG(EDIAG, "failed to allocate memory for ObRpcClientNetHandler", K(ret), K_(cs_id));
              } else {
                //has assert rpc_net_vc_ & read_buffer_ & buf_reader_ not NULL, set them to NULL after new_session->new_connection(...) to avoid re-free
                if (OB_FAIL(new_session->new_connection(rpc_net_vc_/*new_vc*/, read_buffer_/*iobuf*/, buf_reader_ /*reader*/))) {
                  PROXY_NET_LOG(EDIAG, "fail to new_connection", K(ret), K_(cs_id));
                } else {
                  need_release = true; //to release detect session handler
                  PROXY_NET_LOG(DEBUG, "handle new obkv client connection", K(ret), K_(cs_id));
                  if (ct_info_.lookup_success_) {
                    new_session->get_ct_info().vip_tenant_.set_tenant_cluster(ct_info_.vip_tenant_.tenant_name_, ct_info_.vip_tenant_.cluster_name_);
                    new_session->get_ct_info().lookup_success_ = true;
                    new_session->get_session_info().set_vip_addr_name(ct_info_.vip_tenant_.vip_addr_);
                    new_session->get_ct_info().vip_tenant_.vip_addr_ = ct_info_.vip_tenant_.vip_addr_;
                  }
                }
                rpc_net_vc_ = NULL; //has passed it to new_session
                buf_reader_ = NULL;
                read_buffer_ = NULL;
              }
            }
            break;
            case obkv::OBPROXY_RPC_REDIS: //redis
            {
              ObRpcRedisClientNetHandler *new_session = op_reclaim_alloc(ObRpcRedisClientNetHandler);
              if (OB_ISNULL(new_session)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                PROXY_NET_LOG(EDIAG, "failed to allocate memory for ObRpcClientNetHandler", K(ret));
              } else {
                //has assert rpc_net_vc_ & read_buffer_ & buf_reader_ not NULL, set them to NULL after new_session->new_connection(...) to avoid re-free
                if (OB_FAIL(new_session->new_connection(rpc_net_vc_/*new_vc*/, read_buffer_/*iobuf*/, buf_reader_ /*reader*/))) {
                  PROXY_NET_LOG(EDIAG, "fail to new_connection", K(ret));
                } else {
                  need_release = true; //to release detect session handler
                  PROXY_NET_LOG(DEBUG, "handle new ob-redis client connection", K(ret), K_(cs_id));
                  if (ct_info_.lookup_success_) {
                    new_session->get_ct_info().vip_tenant_.set_tenant_cluster(ct_info_.vip_tenant_.tenant_name_, ct_info_.vip_tenant_.cluster_name_);
                    new_session->get_ct_info().lookup_success_ = true;
                    new_session->get_session_info().set_vip_addr_name(ct_info_.vip_tenant_.vip_addr_);
                    new_session->get_ct_info().vip_tenant_.vip_addr_ = ct_info_.vip_tenant_.vip_addr_;
                  }
                }
                rpc_net_vc_ = NULL; //has passed it to new_session
                buf_reader_ = NULL;
                read_buffer_ = NULL;
              }
            }
            break;
            case obkv::OBPROXY_RPC_HBASE:
            default:
              ret = OB_NOT_SUPPORTED;
              PROXY_CS_LOG(WDIAG, "unsupported protocol to handle", K_(cs_id), K(rpc_type), K(net_head_buf_),
                           "meta_length", current_need_read_len, K(ret));
            break;
          }
        }
      }
      if (OB_SUCC(ret) && need_read_more_here) {
        switch (__builtin_expect(status, RPC_REQUEST_READ_DONE)) {
        case RPC_REQUEST_READ_DONE:
          set_proxy_protocol_v2_request(false);
          if (OB_FAIL(buffer_reader.consume(proxy_protocol_v2_.get_total_len()))) {
            PROXY_CS_LOG(WDIAG, "fail to consume ppv2 packet", K(ret), K_(cs_id));
          }
          PROXY_CS_LOG(DEBUG, "succ to analyze ppv2 packet", K(proxy_protocol_v2_), K_(cs_id));
          net_entry_.read_vio_->nbytes_ = INT64_MAX;
          net_entry_.read_vio_->reenable(); //need check next data
          if (OB_SUCC(ret)) {
            PROXY_CS_LOG(DEBUG, "need read next request immediately when request waiting", K_(cs_id), "net_len", buffer_reader.read_avail());
            if (OB_FAIL(handle_request_read_throttle())) {
              PROXY_CS_LOG(WDIAG, "fail to handle rpc req throttle", K_(cs_id), K(ret));
            }
          }

          break;
        case RPC_REQUEST_READ_CONT:
          if (VC_EVENT_READ_COMPLETE == event) {
            int64_t read_num = proxy_protocol_v2_.get_len() > 0 ? proxy_protocol_v2_.get_len() : RPC_NET_DETECT_HRD_LEN;
            buffer_reader.mbuf_->water_mark_ = read_num;
            if (OB_ISNULL(net_entry_.read_vio_ = do_io_read(this, read_num, buffer_reader.mbuf_))) {
              ret = OB_ERR_UNEXPECTED;
              PROXY_CS_LOG(WDIAG, "rpc net handler fail to do_io_read", K(ret), K_(cs_id), "packet_len", proxy_protocol_v2_.get_len(), K(read_num));
            } else {
              event_ret = VC_EVENT_CONT;
            }
          } else {
            net_entry_.read_vio_->nbytes_ = INT64_MAX;
            net_entry_.read_vio_->reenable();
            event_ret = VC_EVENT_CONT;
          }

          break;
        case RPC_REQUEST_READ_ERROR:
          ret = OB_ERR_UNEXPECTED;
          PROXY_CS_LOG(WDIAG, "error parsing client request", K_(cs_id), K(ret));
          net_entry_.read_vio_->nbytes_ = net_entry_.read_vio_->ndone_;
          break;
        default:
          ret = OB_INNER_STAT_ERROR;
          PROXY_CS_LOG(EDIAG, "unknown analyze rpc status", K_(cs_id), K(status), K(ret));
          break;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    do_io_close();
  } else if (need_release) {
    do_io_release();
  }

  return ret;
}

int ObRpcClientNetHandler::handle_request_read_throttle()
{
  int ret = OB_SUCCESS;
  bool is_pass = true;
  if (OB_UNLIKELY(get_global_proxy_config().enable_rpc_throttle
            && get_global_rpc_throttle().is_trigger_throttle())) {
    if (OB_FAIL(get_global_rpc_throttle().calc(is_pass))) {
      PROXY_CS_LOG(WDIAG, "rpc throttle fail to calc token backet", K(ret), K_(cs_id));
    }
  }
  if (OB_LIKELY(is_pass)) {
    if (OB_FAIL(setup_client_request_read())) {
      PROXY_CS_LOG(WDIAG, "fail to call setup_client_request_read", K_(cs_id), K(ret));
    }
  } else {
    ObHRTime rpc_req_throttle_waiting_time = HRTIME_USECONDS(get_global_proxy_config().rpc_request_throttle_waiting_time);
    if (OB_ISNULL(self_ethread().schedule_in(this, rpc_req_throttle_waiting_time, RPC_CLIENT_NET_READ_REQUEST))) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CS_LOG(WDIAG, "fail to schedule throttle waiting", K(ret), K_(cs_id));
    }
  }
  return ret;
}

int ObRpcClientNetHandler::handle_proxy_protocol_v2_request(ProxyProtocolV2 &v2, ObRpcReqReadStatus &status)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(get_reader())) {
    ObIOBufferReader &buffer_reader = *get_reader();
    int64_t len = buffer_reader.read_avail();
    status = RPC_REQUEST_READ_CONT;
    if (OB_LIKELY(len >= ProxyProtocolV2::PROXY_PROTOCOL_V2_HEADER_LEN)) {
      char packet[len] ;
      char *written_pos = buffer_reader.copy(packet, len , 0);
      if (written_pos != (packet + len)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CS_LOG(WDIAG, "not copy completely", K(ret), K_(cs_id));
      } else if (OB_FAIL(v2.analyze_packet(packet, len))) {
        PROXY_CS_LOG(WDIAG, "proxy protocol v2 analyze packet failed", K(ret), K_(cs_id));
      } else if (v2.is_finished()) {
        status = RPC_REQUEST_READ_DONE;
        PROXY_CS_LOG(DEBUG, "proxy protocol analzye complete", K(v2), K_(cs_id));
        if (OB_FAIL(fill_tenant_info_with_ppv2(v2))) {
          PROXY_CS_LOG(WDIAG, "fail to fill tenant info with ppv2", K(v2), K(ret), K_(cs_id));
        } else {
          ObString user_name;
          if (!get_global_white_list_table_processor().can_ip_pass(get_vip_cluster_name(),
                                                                   get_vip_tenant_name(),
                                                                   user_name,
                                                                   ops_ip_sa_cast(v2.src_addr_.get_sockaddr()))) {
            ret = OB_ERR_CAN_NOT_PASS_WHITELIST;
            status = RPC_REQUEST_READ_ERROR;
            PROXY_CS_LOG(WDIAG, "can not pass white_list", K(get_vip_cluster_name()), K(get_vip_tenant_name()), K(v2), K(ret), K_(cs_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObRpcClientNetHandler::fill_tenant_info_with_ppv2(ProxyProtocolV2 &ppv2_info)
{
  int ret = OB_SUCCESS;
  //ct_info_.reset();
  ObVipAddr &addr = ct_info_.vip_tenant_.vip_addr_;

  struct sockaddr_storage ss = ppv2_info.src_addr_.get_sockaddr();
  rpc_net_vc_->set_real_client_addr(ss);
  if (ppv2_info.vpc_info_.empty()) {
    // connected by lb
    addr.set(ops_ip_sa_cast(ppv2_info.dst_addr_.get_sockaddr()), 0);
  } else {
    // connected by private link, vid is -1
    ObString tmp_str(static_cast<int32_t>(ppv2_info.vpc_info_.len()), ppv2_info.vpc_info_.ptr());
    addr.set(tmp_str);
    addr.vid_ = -1;
  }

  if (OB_FAIL(fetch_tenant_by_vip())) {
    PROXY_CS_LOG(WDIAG, "fail to fetch tenant by vip", K(ppv2_info), K(ret));
  } else if (!ct_info_.lookup_success_ && OB_FAIL(refresh_tenant_info_from_multi_level_config())) {
    PROXY_CS_LOG(WDIAG, "fail to refresh tenant info from multi level config", K(ret));
  }
  return ret;
}

int ObRpcClientNetHandler::refresh_tenant_info_from_multi_level_config()
{
  int ret = OB_SUCCESS;
  omt::ObProxyMultiLevelConfig *multi_level_config = NULL;
  uint64_t global_version = get_global_proxy_config_table_processor().get_config_version();
  obutils::ObVipAddr &vip_addr = ct_info_.vip_tenant_.vip_addr_;
  ObString cluster_name = ct_info_.vip_tenant_.cluster_name_;
  ObString tenant_name = ct_info_.vip_tenant_.tenant_name_;
  ObString service_name;

  if (OB_FAIL(get_global_proxy_config_table_processor().get_proxy_multi_config(
      vip_addr, cluster_name, tenant_name, global_version, multi_level_config, service_name))) {
    PROXY_CS_LOG(WDIAG, "fail to get proxy multi-level config", K(ret));
  } else if (OB_NOT_NULL(multi_level_config)) {
    ObString new_tenant_name = multi_level_config->proxy_tenant_name_;
    ObString new_cluster_name = multi_level_config->rootservice_cluster_name_;

    if (new_tenant_name.empty()) {
      new_tenant_name = tenant_name;
    }
    if (new_cluster_name.empty()) {
      new_cluster_name = cluster_name;
    }

    if (OB_FAIL(ct_info_.vip_tenant_.set_tenant_cluster(new_tenant_name, new_cluster_name))) {
      PROXY_CS_LOG(WDIAG, "fail to update tenant cluster from config", K(ret));
    } else {
      if (!new_tenant_name.empty() && !new_cluster_name.empty()) {
        ct_info_.lookup_success_ = true;
        session_info_.set_vip_addr_name(vip_addr);
      }
      PROXY_CS_LOG(DEBUG, "refresh tenant info from multi-level config",
                   K(new_tenant_name), K(new_cluster_name));
    }
  }

  if (OB_NOT_NULL(multi_level_config)) {
    multi_level_config->dec_ref();
    multi_level_config = NULL;
  }

  return ret;
}

int ObRpcClientNetHandler::setup_client_response_send()
{
  int ret = OB_SUCCESS;
  //do nothing
  //set read trigger and read_reschedule. sometimes the data already is in the io buffer
  PROXY_CS_LOG(DEBUG, "ObRpcServerNetHandler::setup_client_response send", K_(cs_id));

  //do nothing
  return ret;
}

int ObRpcClientNetHandler::state_client_response_send(int event, void *data)
{
  int ret = OB_SUCCESS;
  UNUSED(event);
  UNUSED(data);
  //do nothing
  return ret;
}

int ObRpcClientNetHandler::schedule_send_response_action()
{
  int ret = OB_SUCCESS;

  //do nothing

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
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_NET];
  for (int64_t i = 0; i < event_thread_count && OB_SUCC(ret); ++i) {
    if (OB_FAIL(init_rpc_net_cs_map_for_one_thread(i))) {
      PROXY_NET_LOG(WDIAG, "fail to new ObRpcClientNetHandlerMap", K(i), K(ret));
    }
  }
  return ret;
}

int init_rpc_net_cs_map_for_one_thread(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][index]->rpc_net_cs_map_
                = new (std::nothrow) ObRpcClientNetHandlerMap())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WDIAG, "fail to new ObRpcClientNetHandlerMap", K(index), K(ret));
  }
  return ret;
}

int init_rpc_net_cs_map_for_one_thread(event::ObEThread *thread)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(thread)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WDIAG, "unexpected thread", K(ret));
  } else if (OB_ISNULL(thread->rpc_net_cs_map_ = new (std::nothrow) ObRpcClientNetHandlerMap())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WDIAG, "fail to new ObRpcClientNetHandlerMap", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
