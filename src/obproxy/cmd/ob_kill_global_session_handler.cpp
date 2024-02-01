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

#include "cmd/ob_kill_global_session_handler.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"


using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::opsql;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
ObKillGlobalSessionHandler::ObKillGlobalSessionHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf,
    const ObInternalCmdInfo &info)
  : ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type()),
    cmd_info_(&info), capability_(info.get_capability())
{
  SET_HANDLER(&ObKillGlobalSessionHandler::handle_kill_global_session_info);
}
int ObKillGlobalSessionHandler::handle_kill_global_session_info(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_FAIL(handle_kill_session_info())) {
    WARN_ICMD("fail to handle_kill_session_info", K(ret));
  } else {
    INFO_ICMD("succ to handle_kill_session_info ", K_(like_name));
    event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
  }
  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(OB_SESSION_POOL_CMD_ERROR);
  }
  return event_ret;
}

int ObKillGlobalSessionHandler::encode_err_packet(const int errcode)
{
  int ret = OB_SUCCESS;
  char msg_buf[ERR_MSG_BUF_SIZE];
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    WARN_ICMD("it has not inited", K(ret));
  } else if (OB_FAIL(reset())) {//before encode_err_packet, we need clean buf
    WARN_ICMD("fail to do reset", K(errcode), K(ret));
  } else {
    int32_t length = 0;
    if (strlen(err_msg_) == 0) {
      length = snprintf(msg_buf, sizeof(msg_buf), "Unknown internal error");
    } else {
      length = snprintf(msg_buf, sizeof(msg_buf), err_msg_);
    }
    if (OB_UNLIKELY(length <= 0) || OB_UNLIKELY(length >= ERR_MSG_BUF_SIZE)) {
      ret = OB_BUF_NOT_ENOUGH;
      WARN_ICMD("msg_buf is not enough", K(length), K(err_msg_), K(ret));
    } else {}
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet(*internal_buf_, seq_, errcode, msg_buf))) {
      WARN_ICMD("fail to encode err packet", K(errcode), K(msg_buf), K(ret));
    } else {
      INFO_ICMD("succ to encode err packet", K(errcode), K(msg_buf));
    }
  }
  return ret;
}

int ObKillGlobalSessionHandler::handle_kill_session_info()
{
  int ret = OB_SUCCESS;
  if (sub_type_ == OBPROXY_T_SUB_KILL_GLOBAL_SS_ID) {
    int64_t ssid = cmd_info_->get_first_int();
    const ObString& dbkey= cmd_info_->get_key_string();
    ret = handle_kill_session_by_ssid(dbkey, ssid);
  } else if (sub_type_ == OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY) {
    const ObString& dbkey = cmd_info_->get_key_string();
    LOG_DEBUG("kill dbkey", K(dbkey));
    ObMysqlServerSessionListPool* ss_list_pool = get_global_session_manager().get_server_session_list_pool(dbkey);
    if (OB_ISNULL(ss_list_pool)) {
      LOG_WDIAG("ss_list_pool not exist", K(dbkey));
      snprintf(err_msg_, ERR_MSG_BUF_SIZE, "dbkey:%.*s not exists", dbkey.length(), dbkey.ptr());
      ret = OB_INVALID_ARGUMENT;
    } else {
      ss_list_pool->do_kill_session();
      ss_list_pool->dec_ref();
    }
  }
  if (OB_SUCC(ret)) {
    int64_t affected_row = 0;
    if (OB_FAIL(encode_ok_packet(affected_row, capability_))) {
      WARN_ICMD("fail to encode ok packet", K(ret));
    }
  }
  return ret;
}
int ObKillGlobalSessionHandler::handle_kill_session_by_ssid(const ObString& dbkey, int64_t ss_id) {
  int ret = OB_SUCCESS;
  LOG_DEBUG("handle_kill_session_by_ssid", K(dbkey), K(ss_id));
  ObMysqlServerSessionListPool* ss_list_pool = get_global_session_manager().get_server_session_list_pool(dbkey);
  if (OB_ISNULL(ss_list_pool)) {
      LOG_WDIAG("ss_list_pool not exist", K(dbkey));
      snprintf(err_msg_, ERR_MSG_BUF_SIZE, "dbkey:%.*s not exists", dbkey.length(), dbkey.ptr());
      ret = OB_INVALID_ARGUMENT;
  } else {
    ret = ss_list_pool->do_kill_session_by_ssid(ss_id);
    if (OB_FAIL(ret)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("ssid not found", K(ss_id));
      snprintf(err_msg_, ERR_MSG_BUF_SIZE, "ssid:%ld not exists", ss_id);
    }
  }
  return ret;
}
static int kill_global_session_info_cmd_callback(ObContinuation * cont, ObInternalCmdInfo & info,
    ObMIOBuffer * buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObKillGlobalSessionHandler *handler = NULL;
  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObKillGlobalSessionHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_ICMD("fail to new ObKillGlobalSessionHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_ICMD("fail to init for ObKillGlobalSessionHandler");
  } else {
    action = &handler->get_action();
    if (OB_ISNULL(self_ethread().schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ERROR_ICMD("fail to schedule handler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule handler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}
int kill_global_session_info_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_KILL_GLOBAL_SESSION,
              &kill_global_session_info_cmd_callback))) {
    WARN_ICMD("fail to register OBPROXY_T_ICMD_KILL_GLOBAL_SESSION CMD", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
