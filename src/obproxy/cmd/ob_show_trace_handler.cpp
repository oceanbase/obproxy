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

#include "cmd/ob_show_trace_handler.h"
#include "proxy/mysql/ob_mysql_transact.h"
#include "proxy/mysql/ob_mysql_client_session.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
//TraceColumnID
enum
{
  OB_TC_ATTEMPTS = 0,
  OB_TC_PL_ATTEMPTS,
  OB_TC_ADDR_IP,
  OB_TC_ADDR_PORT,
  OB_TC_SERVER_STATE,
  OB_TC_SEND_ACTION,
  OB_TC_RESP_ERROR,
  OB_TC_COST,
  OB_TC_MAX_TRACE_COLUMN_ID,
};

const ObProxyColumnSchema TRACE_COLUMN_ARRAY[OB_TC_MAX_TRACE_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_TC_ATTEMPTS,      "attempts",     OB_MYSQL_TYPE_TINY),
    ObProxyColumnSchema::make_schema(OB_TC_PL_ATTEMPTS,   "pl_attempts",  OB_MYSQL_TYPE_TINY),
    ObProxyColumnSchema::make_schema(OB_TC_ADDR_IP,       "ip",           OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_TC_ADDR_PORT,     "port",         OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_TC_SERVER_STATE,  "server_state", OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_TC_SEND_ACTION,   "send_action",  OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_TC_RESP_ERROR,    "resp_error",   OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_TC_COST,          "cost_time_us", OB_MYSQL_TYPE_LONG),
};

ObShowTraceHandler::ObShowTraceHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
    : ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type()),
      attempt_limit_(info.get_attempt_limit()), cs_id_array_()
{
  SET_HANDLER(&ObShowTraceHandler::handle_trace);
  SET_CS_HANDLER(&ObShowTraceHandler::dump_trace);
}

int ObShowTraceHandler::dump_trace_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(TRACE_COLUMN_ARRAY, OB_TC_MAX_TRACE_COLUMN_ID))) {
    WARN_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowTraceHandler::dump_trace(const ObMysqlClientSession &cs)
{
  int ret = OB_SUCCESS;
  const ObTraceStats *stats = cs.get_trace_stats();
  if (NULL != stats && NULL != stats->current_block_) {
    const ObTraceBlock *block = stats->current_block_;
    bool is_done = false;
    bool need_show_all = false;
    if (OBPROXY_T_SUB_TRACE_LIMIT == sub_type_) {
      if (attempt_limit_ <= 0 || attempt_limit_ > block->stats_[block->next_idx_ - 1].attempts_) {
        is_done = true;
      }
    } else {
      need_show_all = true;
    }

    int32_t i = 0;
    block = &(stats->first_block_);
    while (!is_done && NULL != block && OB_SUCC(ret)) {
      for (i = 0; i < block->next_idx_ && i < ObTraceBlock::TRACE_BLOCK_ENTRIES && OB_SUCC(ret); ++i) {
        if (need_show_all || block->stats_[i].attempts_ == attempt_limit_) {
          if (OB_FAIL(dump_trace_item(block->stats_[i]))) {
            WARN_ICMD("fail to dump_trace_item", "stat", block->stats_[i], K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        block = block->next_;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_eof_packet())) {
      WARN_ICMD("fail to encode eof packet", K(ret));
    } else {
      DEBUG_ICMD("succ to dump_trace", KPC(stats), K(attempt_limit_));
    }
  }
  return ret;
}

int ObShowTraceHandler::dump_trace_item(const ObTraceRecord &item)
{
  int ret = OB_SUCCESS;
  char ip_buff[INET6_ADDRSTRLEN];
  if (OB_UNLIKELY(!item.addr_.ip_to_string(ip_buff, sizeof(ip_buff)))) {
    ip_buff[0] = '\0';
  }

  ObNewRow row;
  ObObj cells[OB_TC_MAX_TRACE_COLUMN_ID];
  cells[OB_TC_ATTEMPTS].set_tinyint(item.attempts_);
  cells[OB_TC_PL_ATTEMPTS].set_tinyint(item.pl_attempts_);
  cells[OB_TC_ADDR_IP].set_varchar(ip_buff);
  cells[OB_TC_ADDR_PORT].set_int32(item.addr_.get_port());
  cells[OB_TC_SERVER_STATE].set_varchar(ObMysqlTransact::get_server_state_name(static_cast<ObMysqlTransact::ObServerStateType>(item.server_state_)));
  cells[OB_TC_SEND_ACTION].set_varchar(ObMysqlTransact::get_send_action_name(static_cast<ObMysqlTransact::ObServerSendActionType>(item.send_action_)));
  cells[OB_TC_RESP_ERROR].set_varchar(ObMysqlTransact::get_server_resp_error_name(static_cast<ObMysqlTransact::ObServerRespErrorType>(item.resp_error_)));
  cells[OB_TC_COST].set_int32(item.cost_time_us_);

  row.cells_ = cells;
  row.count_ = OB_TC_MAX_TRACE_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WARN_ICMD("fail to encode row packet", K(item), K(row), K(ret));
  } else {
    DEBUG_ICMD("succ to encode row packet", K(item));
  }
  return ret;
}

int ObShowTraceHandler::handle_trace(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  ObEThread *ethread = NULL;
  bool need_callback = true;
  bool is_proxy_conn_id = true;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_trace_header())) {
    WARN_ICMD("fail to dump trace header, try to do internal_error_callback", K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("cur ethread is null, it should not happened", K(ret));
  } else {
    if (!is_conn_id_avail(cs_id_, is_proxy_conn_id)) {
      int errcode = OB_UNKNOWN_CONNECTION; //not found the specific session
      WARN_ICMD("cs_id is not avail", K(cs_id_), K(errcode));
      if (OB_FAIL(encode_err_packet(errcode, cs_id_))) {
        WARN_ICMD("fail to encode err resp packet", K(errcode), K_(cs_id), K(ret));
      }
    } else {
      if (is_proxy_conn_id) {
        //connection id got from obproxy
        int64_t thread_id = -1;
        if (OB_FAIL(extract_thread_id(static_cast<uint32_t>(cs_id_), thread_id))) {
          WARN_ICMD("fail to extract thread id, it should not happen", K(cs_id_), K(ret));
        } else if (thread_id == ethread->id_) {
          need_callback = false;
          event_ret = handle_cs_with_proxy_conn_id(EVENT_NONE, data);
        } else {
          SET_HANDLER(&ObInternalCmdHandler::handle_cs_with_proxy_conn_id);
          if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][thread_id]->schedule_imm(this))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            ERROR_ICMD("fail to schedule self", K(thread_id), K(ret));
          } else {
            need_callback = false;
          }
        }
      } else {
        //connection id got from observer
        SET_HANDLER(&ObInternalCmdHandler::handle_cs_with_server_conn_id);
        if (OB_ISNULL(ethread->schedule_imm(this))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          ERROR_ICMD("fail to schedule self", K(ret));
        } else {
          need_callback = false;
        }
      }
    }
  }

  if (need_callback) {
    if (OB_FAIL(ret)) {
      event_ret = internal_error_callback(ret);
    } else {
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }
  return event_ret;
}

static int show_trace_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObEThread *ethread = NULL;
  ObShowTraceHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowTraceHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_ICMD("fail to new ObShowTraceHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_ICMD("fail to init for ObShowSessionHandler");
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("cur ethread is null, it should not happened", K(ret));
  } else {
    if (-1 == handler->cs_id_) {
      handler->cs_id_ = info.session_priv_->cs_id_;
    }
    action = &handler->get_action();
    if (OB_ISNULL(ethread->schedule_imm(handler, ET_NET))) {// use work thread
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ERROR_ICMD("fail to schedule ObShowTraceHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObShowTraceHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_trace_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_TRACE, &show_trace_cmd_callback))) {
    WARN_ICMD("fail to CMD_TYPE_TRACE", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
