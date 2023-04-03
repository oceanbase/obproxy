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

#define USING_LOG_PREFIX PROXY_ICMD

#include "cmd/ob_show_net_handler.h"
#include "iocore/net/ob_net.h"

using namespace oceanbase::obmysql;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace net
{
//ThreadColumnID
enum
{
  OB_TC_NET_THREAD_ID = 0,
  OB_TC_CURRENT_CONN_COUNT,
  OB_TC_CURRENT_CLIENT_CONN_COUNT,
  OB_TC_G_CURRENT_CLIENT_CONNE_COUNT,
  OB_TC_G_CURRENT_CONN_COUNT,
  OB_TC_G_CURRENT_DO_ACCEPT_COUNT,
  OB_TC_TOTAL_EPOLL_WAIT,
  OB_TC_LAST_EPOLL_SIZE,
  OB_TC_TOTAL_COP_LOCK_FAILURE,
  OB_TC_TOTAL_LRU_TIMEOUT_COUNT,
  OB_TC_TOTAL_LRU_TIMEOUT_TIME,
  OB_TC_TOTAL_SET_DEFAULT_INACTIVE,
  OB_TC_TOTAL_CALLS_READ_FROM_NET,
  OB_TC_TOTAL_CALLS_READ,
  OB_TC_TOTAL_CALLS_READ_NODATA,
  OB_TC_TOTAL_READ_BYTES,
  OB_TC_TOTAL_CALLS_WRITE_TO_NET,
  OB_TC_TOTAL_CALLS_WRITE,
  OB_TC_TOTAL_CALLS_WRITE_NODATA,
  OB_TC_TOTAL_WRITE_BYTES,
  OB_TC_PROTECTED_QUEUE_AL_SIZE,
  OB_TC_PROTECTED_QUEUE_LOCAL_SIZE,
  OB_TC_PRIORITY_QUEUE_SIZE,
  OB_TC_MAX_THREAD_COLUMN_ID,
};

//ConnectionColumnID
enum
{
  OB_CC_THREAD_ID = 0,
  OB_CC_CONNECT_ID,
  OB_CC_SOCKET_FD,
  OB_CC_TYPE,
  OB_CC_SRC_IP,
  OB_CC_SRC_PORT,
  OB_CC_DST_IP,
  OB_CC_DST_PORT,
  OB_CC_VIRTUAL_IP,
  OB_CC_VIRTUAL_PORT,
  OB_CC_VIRTUAL_VID,
  OB_CC_BIND_STYLE,
  OB_CC_READ_ENABLED,
  OB_CC_READ_NBYTES,
  OB_CC_READ_NDONE,
  OB_CC_WRITE_ENABLED,
  OB_CC_WRITE_NBYTE,
  OB_CC_WRITE_NDONE,
  OB_CC_ALIVE_TIME,
  OB_CC_ACT_TIMEOUT_IN,
  OB_CC_INACT_TIMEOUT_IN,
  OB_CC_TIMEOUT_CLOSE_IN,
  OB_CC_LAST_ERROR_NO,
  OB_CC_SHUTDOWN,
  OB_CC_COMMENTS,
  OB_CC_MAX_CONN_COLUMN_ID,
};

const ObProxyColumnSchema THREAD_COLUMN_ARRAY[OB_TC_MAX_THREAD_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_TC_NET_THREAD_ID,                "net_thread_id",                          OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_CURRENT_CONN_COUNT,           "current_connection_count",               OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_CURRENT_CLIENT_CONN_COUNT,    "current_client_connection_count",        OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_G_CURRENT_CLIENT_CONNE_COUNT, "global_current_client_connection_count", OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_G_CURRENT_CONN_COUNT,         "global_current_connection_count",        OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_G_CURRENT_DO_ACCEPT_COUNT,    "global_current_do_accept_count",         OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_TOTAL_EPOLL_WAIT,             "total_epoll_wait",                       OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_LAST_EPOLL_SIZE,              "last_epoll_size",                        OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_TOTAL_COP_LOCK_FAILURE,       "total_cop_lock_failure",                 OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_TOTAL_LRU_TIMEOUT_COUNT,      "total_lru_timeout_count",                OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_TOTAL_LRU_TIMEOUT_TIME,       "total_lru_timeout_time-sec",             OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_TOTAL_SET_DEFAULT_INACTIVE,   "total_set_default_inactive",             OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_TOTAL_CALLS_READ_FROM_NET,    "total_calls_read_from_net",              OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_TOTAL_CALLS_READ,             "total_calls_read",                       OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_TOTAL_CALLS_READ_NODATA,      "total_calls_read_nodata",                OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_TOTAL_READ_BYTES,             "total_read_bytes",                       OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_TOTAL_CALLS_WRITE_TO_NET,     "total_calls_write_to_net",               OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_TOTAL_CALLS_WRITE,            "total_calls_write",                      OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_TOTAL_CALLS_WRITE_NODATA,     "total_calls_write_nodata",               OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_TOTAL_WRITE_BYTES,            "total_write_bytes",                      OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_PROTECTED_QUEUE_AL_SIZE,      "protected_queue_al_size",                OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_PROTECTED_QUEUE_LOCAL_SIZE,   "protected_queue_local_size",             OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_TC_PRIORITY_QUEUE_SIZE,          "priority_queue_size",                    OB_MYSQL_TYPE_LONGLONG)
};

const ObProxyColumnSchema CONN_COLUMN_ARRAY[OB_CC_MAX_CONN_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_CC_THREAD_ID,        "thread_id",                  OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_CONNECT_ID,       "connect_id",                 OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_CC_SOCKET_FD,        "socket_fd",                  OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_CC_TYPE,             "type",                       OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_SRC_IP,           "src_ip",                     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_SRC_PORT,         "src_port",                   OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_CC_DST_IP,           "dst_ip",                     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_DST_PORT,         "dst_port",                   OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_CC_VIRTUAL_IP,       "virtual_ip",                 OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_VIRTUAL_PORT,     "virtual_port",               OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_CC_VIRTUAL_VID,      "virtual_vid",                OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_BIND_STYLE,       "bind_style",                 OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_READ_ENABLED,     "read_enabled",               OB_MYSQL_TYPE_TINY),
    ObProxyColumnSchema::make_schema(OB_CC_READ_NBYTES,      "read_nbytes",                OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_READ_NDONE,       "read_ndone",                 OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_WRITE_ENABLED,    "write_enabled",              OB_MYSQL_TYPE_TINY),
    ObProxyColumnSchema::make_schema(OB_CC_WRITE_NBYTE,      "write_nbyte",                OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_WRITE_NDONE,      "write_ndone",                OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_ALIVE_TIME,       "alive_time(sec)",            OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_ACT_TIMEOUT_IN,   "activity_timeout_in(sec)",   OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_INACT_TIMEOUT_IN, "inactivity_timeout_in(sec)", OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_TIMEOUT_CLOSE_IN, "timeout_close_in(sec)",      OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_LAST_ERROR_NO,    "last_error_no",              OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_CC_SHUTDOWN,         "shutdown",                   OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_COMMENTS,         "comments",                   OB_MYSQL_TYPE_VARCHAR)
};

ObShowNetHandler::ObShowNetHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
    : ObInternalCmdHandler(cont, buf, info), thread_id_(info.get_thread_id()),
      limit_rows_(info.get_limit_rows()), limit_offset_(info.get_limit_offset()), next_thread_id_(0)
{
}

int ObShowNetHandler::handle_show_threads(int event, ObEvent *e)
{
  int event_ret = EVENT_NONE;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_argument_valid(event, e))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(e), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_thread_header())) {
    WARN_ICMD("fail to dump_threads_header", K(ret));
  } else {
    SET_HANDLER(&ObShowNetHandler::show_single_thread);
    next_thread_id_ = 0;
    if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][next_thread_id_]->schedule_imm(this))) {
      ret = OB_ERR_UNEXPECTED;
      ERROR_ICMD("fail to schedule self", K(next_thread_id_), K(ret));
    } else {
      event_ret = EVENT_CONT;
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowNetHandler::handle_show_connections(int event, ObEvent *e)
{
  int event_ret = EVENT_NONE;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_argument_valid(event, e))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(e), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_connections_header())) {
    WARN_ICMD("fail to dump_connections_header", K(ret));
  } else if (thread_id_ < 0 ) {//dump all connection
    next_thread_id_ = 0;
    SET_HANDLER(&ObShowNetHandler::show_connections);
    if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][next_thread_id_]->schedule_imm(this))) {
      ret = OB_ERR_UNEXPECTED;
      ERROR_ICMD("fail to schedule self", K(next_thread_id_), K(ret));
    } else {
      event_ret = EVENT_CONT;
    }
  } else if (thread_id_ < g_event_processor.thread_count_for_type_[ET_NET]) {//dump single connection
    SET_HANDLER(&ObShowNetHandler::show_connections);
    if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][thread_id_]->schedule_imm(this))) {
      ret = OB_ERR_UNEXPECTED;
      ERROR_ICMD("fail to schedule self", K_(thread_id), K(ret));
    } else {
      event_ret = EVENT_CONT;
    }
  } else {
    if (OB_FAIL(encode_eof_packet())) {
      WARN_ICMD("fail to encode eof packet", K(ret));
    } else {
      INFO_ICMD("dump no connections", K_(thread_id));
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowNetHandler::show_single_thread(int event, ObEvent *e)
{
  int event_ret = EVENT_NONE;
  int ret = OB_SUCCESS;
  ObEThread *ethread = NULL;
  bool is_dump_succ = false;
  if (OB_UNLIKELY(!is_argument_valid(event, e))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(e), K_(is_inited), K(ret));
  } else if (OB_ISNULL(ethread = e->ethread_)) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("it should not happen", K(ethread), K(ret));
  } else {
    ObNetHandler &nh = ethread->get_net_handler();
    MUTEX_TRY_LOCK(lock, nh.mutex_, ethread);
    if (!lock.is_locked()) {
      if (OB_ISNULL(ethread->schedule_in(this, MYSQL_LIST_RETRY))) {
        ret = OB_ERR_UNEXPECTED;
        ERROR_ICMD("fail to schedule self", "this_ethread", ethread->id_, K(ret));
      } else {
        event_ret = EVENT_CONT;
      }
    } else {
      if (OB_FAIL(dump_single_thread(ethread, nh))) {
        WARN_ICMD("fail to dump single thread", K(ret));
      } else {
        is_dump_succ = true;
      }
    }
  }

  if (OB_SUCC(ret) && OB_LIKELY(is_dump_succ)) {
    ++next_thread_id_;
    if (next_thread_id_ < g_event_processor.thread_count_for_type_[ET_NET]) {
      if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][next_thread_id_]->schedule_imm(this))) {
        ret = OB_ERR_UNEXPECTED;
        ERROR_ICMD("fail to do next schedule", K(next_thread_id_), K(ret));
      } else {
        event_ret = EVENT_CONT;
      }
    } else {
      if (OB_FAIL(encode_eof_packet())) {
        WARN_ICMD("fail to encode eof packet", K(ret));
      } else {
        INFO_ICMD("succ to dump threads");
        event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
      }
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowNetHandler::show_connections(int event, ObEvent *e)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_NONE;
  ObEThread *ethread = NULL;
  bool is_dump_succ = false;
  if (OB_UNLIKELY(!is_argument_valid(event, e))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(e), K_(is_inited), K(ret));
  } else if (OB_ISNULL(ethread = e->ethread_)) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("it should not happen", K(ethread), K(ret));
  } else {
    ObNetHandler &nh = ethread->get_net_handler();
    MUTEX_TRY_LOCK(lock, nh.mutex_, ethread);
    if (!lock.is_locked()) {
      if (OB_ISNULL(ethread->schedule_in(this, MYSQL_LIST_RETRY))) {
        ret = OB_ERR_UNEXPECTED;
        ERROR_ICMD("fail to schedule self", "this_ethread", ethread->id_, K(ret));
      } else {
        event_ret = EVENT_CONT;
      }
    } else {
      if (OB_FAIL(dump_connections_on_thread(ethread, &nh))) {
        WARN_ICMD("fail to dump connections on thread", K(ret));
      } else {
        is_dump_succ = true;
      }
    }
  }

  if (OB_SUCC(ret) && OB_LIKELY(is_dump_succ)) {
    if (thread_id_ < 0  && ++next_thread_id_ < g_event_processor.thread_count_for_type_[ET_NET]) {//dump all thread
      if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][next_thread_id_]->schedule_imm(this))) {
        ret = OB_ERR_UNEXPECTED;
        ERROR_ICMD("fail to schedule self", K(next_thread_id_), K(ret));
      } else {
        event_ret = EVENT_CONT;
      }
    } else {
      if (OB_FAIL(encode_eof_packet())) {
       WARN_ICMD("fail to encode eof packet", K(ret));
      } else {
       INFO_ICMD("succ to dump connections", K_(thread_id));
       event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
      }
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowNetHandler::dump_thread_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(THREAD_COLUMN_ARRAY, OB_TC_MAX_THREAD_COLUMN_ID))) {
    WARN_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowNetHandler::dump_connections_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(CONN_COLUMN_ARRAY, OB_CC_MAX_CONN_COLUMN_ID))) {
    WARN_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowNetHandler::dump_single_thread(ObEThread *ethread, const ObNetHandler &nh)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ethread)) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(ret));
  } else {
    ObNewRow row;
    ObObj cells[OB_TC_MAX_THREAD_COLUMN_ID];
    int64_t current_conn_count = 0;
    int64_t result = ethread->get_net_poll().get_poll_descriptor().result_;
    forl_LL(ObUnixNetVConnection, vc, nh.open_list_) {
      ++current_conn_count;
    }
    cells[OB_TC_NET_THREAD_ID].set_int(next_thread_id_);
    cells[OB_TC_CURRENT_CONN_COUNT].set_int(current_conn_count);
    cells[OB_TC_CURRENT_CLIENT_CONN_COUNT].set_int(ObStatProcessor::get_thread_raw_stat_sum(net_rsb, ethread, NET_CLIENT_CONNECTIONS_CURRENTLY_OPEN));
    cells[OB_TC_G_CURRENT_CLIENT_CONNE_COUNT].set_int(ObStatProcessor::get_global_raw_stat_sum(net_rsb, NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN));
    cells[OB_TC_G_CURRENT_CONN_COUNT].set_int(ObStatProcessor::get_global_raw_stat_sum(net_rsb, NET_GLOBAL_CONNECTIONS_CURRENTLY_OPEN));
    cells[OB_TC_G_CURRENT_DO_ACCEPT_COUNT].set_int(ObStatProcessor::get_global_raw_stat_sum(net_rsb, NET_GLOBAL_ACCEPTS_CURRENTLY_OPEN));
    cells[OB_TC_TOTAL_EPOLL_WAIT].set_int(ObStatProcessor::get_thread_raw_stat_sum(net_rsb, ethread, NET_HANDLER_RUN));
    cells[OB_TC_LAST_EPOLL_SIZE].set_int(result);
    cells[OB_TC_TOTAL_COP_LOCK_FAILURE].set_int(ObStatProcessor::get_thread_raw_stat_sum(net_rsb, ethread, INACTIVITY_COP_LOCK_ACQUIRE_FAILURE));
    cells[OB_TC_TOTAL_LRU_TIMEOUT_COUNT].set_int(ObStatProcessor::get_thread_raw_stat_sum(net_rsb, ethread, KEEP_ALIVE_LRU_TIMEOUT_TOTAL));
    cells[OB_TC_TOTAL_LRU_TIMEOUT_TIME].set_int(ObStatProcessor::get_thread_raw_stat_sum(net_rsb, ethread, KEEP_ALIVE_LRU_TIMEOUT_COUNT));
    cells[OB_TC_TOTAL_SET_DEFAULT_INACTIVE].set_int(ObStatProcessor::get_thread_raw_stat_sum(net_rsb, ethread, DEFAULT_INACTIVITY_TIMEOUT));
    cells[OB_TC_TOTAL_CALLS_READ_FROM_NET].set_int(ObStatProcessor::get_thread_raw_stat_sum(net_rsb, ethread, NET_CALLS_TO_READFROMNET));
    cells[OB_TC_TOTAL_CALLS_READ].set_int(ObStatProcessor::get_thread_raw_stat_sum(net_rsb, ethread, NET_CALLS_TO_READ));
    cells[OB_TC_TOTAL_CALLS_READ_NODATA].set_int(ObStatProcessor::get_thread_raw_stat_sum(net_rsb, ethread, NET_CALLS_TO_READ_NODATA));
    cells[OB_TC_TOTAL_READ_BYTES].set_int(ObStatProcessor::get_thread_raw_stat_sum(net_rsb, ethread, NET_READ_BYTES));
    cells[OB_TC_TOTAL_CALLS_WRITE_TO_NET].set_int(ObStatProcessor::get_thread_raw_stat_sum(net_rsb, ethread, NET_CALLS_TO_WRITETONET));
    cells[OB_TC_TOTAL_CALLS_WRITE].set_int(ObStatProcessor::get_thread_raw_stat_sum(net_rsb, ethread, NET_CALLS_TO_WRITE));
    cells[OB_TC_TOTAL_CALLS_WRITE_NODATA].set_int(ObStatProcessor::get_thread_raw_stat_sum(net_rsb, ethread, NET_CALLS_TO_WRITE_NODATA));
    cells[OB_TC_TOTAL_WRITE_BYTES].set_int(ObStatProcessor::get_thread_raw_stat_sum(net_rsb, ethread, NET_WRITE_BYTES));
    cells[OB_TC_PROTECTED_QUEUE_AL_SIZE].set_int(ethread->event_queue_external_.get_atomic_list_size());
    cells[OB_TC_PROTECTED_QUEUE_LOCAL_SIZE].set_int(ethread->event_queue_external_.get_local_queue_size());
    cells[OB_TC_PRIORITY_QUEUE_SIZE].set_int(ethread->event_queue_.get_queue_size());

    row.cells_ = cells;
    row.count_ = OB_TC_MAX_THREAD_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WARN_ICMD("fail to encode row packet", K(row), K(ret));
    } else {
      DEBUG_ICMD("succ to encode row packet", K(row), K(next_thread_id_));
    }
  }
  return ret;
}

int ObShowNetHandler::dump_connections_on_thread(const ObEThread *ethread, const ObNetHandler *nh)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ethread) || OB_ISNULL(nh)) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(ret));
  } else {
    ObHRTime now = get_hrtime();
    int64_t current_rows = limit_rows_;
    int64_t current_offset = limit_offset_;

    DEBUG_ICMD("dump connections on thread", "ethread", ethread->id_, K(current_rows), K(current_offset));
    ObUnixNetVConnection *vc = (nh->open_list_).head_;
    if (thread_id_ >=0 ) {//only single use limit
      while(current_offset > 0 && NULL != vc) {
        vc = (nh->open_list_).next(vc);
        --current_offset;
      };
    }

    ObNewRow row;
    ObObj cells[OB_CC_MAX_CONN_COLUMN_ID];
    char src_ip[INET6_ADDRSTRLEN];
    int32_t src_port = 0;
    char dst_ip[INET6_ADDRSTRLEN];
    int32_t dst_port = 0;
    char virtual_ip[INET6_ADDRSTRLEN];
    for (; OB_SUCC(ret) && NULL != vc; vc = (nh->open_list_).next(vc)) {
      if (thread_id_ >=0 && -1 != limit_rows_) {//only single use limit
        if (current_rows <= 0) {
          DEBUG_ICMD("finish dump connections on thread", K_(next_thread_id), K_(thread_id),
              K(current_rows), K(current_offset));
          break;
        } else {
          --current_rows;
        }
      }/*else show all*/

      const char *type_str = NULL;
      if (ObUnixNetVConnection::VC_ACCEPT == vc->source_type_) {
        ops_ip_ntop(vc->get_remote_addr(), src_ip, sizeof(src_ip));
        src_port = static_cast<int32_t>(ops_ip_port_host_order(vc->get_remote_addr()));
        ops_ip_ntop(vc->get_local_addr(), dst_ip, sizeof(dst_ip));
        dst_port = static_cast<int32_t>(ops_ip_port_host_order(vc->get_local_addr()));
        type_str = "accepted";
      } else {
        if (ObUnixNetVConnection::VC_CONNECT == vc->source_type_) {
          type_str = "connected";
        } else {
          type_str = "inner connected";
        }
        ops_ip_ntop(vc->get_local_addr(), src_ip, sizeof(src_ip));
        src_port = static_cast<int32_t>(ops_ip_port_host_order(vc->get_local_addr()));
        ops_ip_ntop(vc->get_remote_addr(), dst_ip, sizeof(dst_ip));
        dst_port = static_cast<int32_t>(ops_ip_port_host_order(vc->get_remote_addr()));
      }
      ops_ip_ntop(vc->get_virtual_addr(), virtual_ip, sizeof(virtual_ip));

      cells[OB_CC_THREAD_ID].set_int(next_thread_id_);
      cells[OB_CC_CONNECT_ID].set_mediumint(vc->id_);
      cells[OB_CC_SOCKET_FD].set_mediumint(vc->con_.fd_);
      cells[OB_CC_TYPE].set_varchar(type_str);
      cells[OB_CC_SRC_IP].set_varchar(src_ip);
      cells[OB_CC_SRC_PORT].set_mediumint(src_port);
      cells[OB_CC_DST_IP].set_varchar(dst_ip);
      cells[OB_CC_DST_PORT].set_mediumint(dst_port);
      cells[OB_CC_VIRTUAL_IP].set_varchar(virtual_ip);
      cells[OB_CC_VIRTUAL_PORT].set_mediumint(static_cast<int32_t>((vc->get_virtual_port())));
      cells[OB_CC_VIRTUAL_VID].set_int(static_cast<int32_t>((vc->get_virtual_vid())));
      cells[OB_CC_BIND_STYLE].set_varchar(vc->options_.get_bind_style());
      cells[OB_CC_READ_ENABLED].set_tinyint(vc->read_.enabled_);
      cells[OB_CC_READ_NBYTES].set_int(vc->read_.vio_.nbytes_);
      cells[OB_CC_READ_NDONE].set_int(vc->read_.vio_.ndone_);
      cells[OB_CC_WRITE_ENABLED].set_tinyint(vc->write_.enabled_);
      cells[OB_CC_WRITE_NBYTE].set_int(vc->write_.vio_.nbytes_);
      cells[OB_CC_WRITE_NDONE].set_int(vc->write_.vio_.ndone_);
      cells[OB_CC_ALIVE_TIME].set_int(hrtime_to_sec(now - vc->submit_time_));
      cells[OB_CC_ACT_TIMEOUT_IN].set_int(hrtime_to_sec(vc->active_timeout_in_));
      cells[OB_CC_INACT_TIMEOUT_IN].set_int(hrtime_to_sec(vc->inactivity_timeout_in_));
      cells[OB_CC_TIMEOUT_CLOSE_IN].set_int(hrtime_to_sec(vc->next_inactivity_timeout_at_ - now));
      cells[OB_CC_LAST_ERROR_NO].set_mediumint(vc->lerrno_);
      cells[OB_CC_SHUTDOWN].set_int(vc->f_.shutdown_);
      cells[OB_CC_COMMENTS].set_varchar(vc->closed_ ? "closed" : "not closed");

      row.cells_ = cells;
      row.count_ = OB_CC_MAX_CONN_COLUMN_ID;
      if (OB_FAIL(encode_row_packet(row))) {
        WARN_ICMD("fail to encode row packet", K(row), K(ret), K(next_thread_id_), K(vc->id_),
                                              K(current_rows), K(current_offset));
      } else {
        DEBUG_ICMD("succ to encode row packet", K(row), K(next_thread_id_), K(vc->id_),
                                               K(current_rows), K(current_offset));
        src_port = 0;
        dst_port = 0;
      }
    }
  }
  return ret;
}

static int show_net_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObShowNetHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowNetHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_ICMD("fail to new ObShowNetHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_ICMD("fail to init for ObShowSMHandler", K(ret));
  } else {
    switch (info.get_sub_cmd_type()) {
      case OBPROXY_T_SUB_NET_THREAD: {
        SET_CONTINUATION_HANDLER(handler, &ObShowNetHandler::handle_show_threads);
        break;
      }
      case OBPROXY_T_SUB_NET_CONNECTION: {
        SET_CONTINUATION_HANDLER(handler, &ObShowNetHandler::handle_show_connections);
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        ERROR_ICMD("invalid sub_stat_type", K(info.get_sub_cmd_type()), K(ret));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      action = &handler->get_action();
      if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        ERROR_ICMD("fail to schedule ObShowNetHandler", K(ret));
        action = NULL;
      } else {
        DEBUG_ICMD("succ to schedule ObShowNetHandle");
      }
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_net_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_NET,
                                                               &show_net_cmd_callback))) {
    WARN_ICMD("fail to register_cmd CMD_TYPE_NET", K(ret));
  }
  return ret;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase
