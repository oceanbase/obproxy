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

#include "cmd/ob_internal_cmd_processor.h"
#include "cmd/ob_config_v2_handler.h"
#include "obutils/ob_config_processor.h"
#include "proxy/mysql/ob_mysql_sm.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{

void ObInternalCmdInfo::reset()
{
  session_priv_ = NULL;
  pkt_seq_ = -1;
  type_ = OBPROXY_T_INVALID;
  sub_type_ = OBPROXY_T_SUB_INVALID;
  err_type_ = OBPROXY_T_ERR_INVALID;
  capability_.capability_ = 0;
  memory_limit_ = 0;
  is_internal_user_ = false;
  first_int_ = -1;
  second_int_ = -1;
  third_int_ = -1;
  first_string_.reset();
  second_string_.reset();
  first_str_[0] = '\0';
  second_str_[0] = '\0';
  protocol_ = ObProxyProtocol::PROTOCOL_NORMAL;
  ob20_param_.reset();
}

int64_t ObInternalCmdInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("type", get_obproxy_stmt_name(type_),
       "sub_type", get_obproxy_sub_stmt_name(sub_type_),
       "err_type", get_obproxy_err_stmt_name(err_type_),
       K_(pkt_seq), K_(capability_.capability),
       K_(memory_limit), K_(is_internal_user), K_(first_int), K_(second_int), K_(third_int),
       K_(first_string), K_(second_string), KPC_(session_priv));
  J_OBJ_END();
  return pos;
}

ObInternalCmdProcessor &get_global_internal_cmd_processor()
{
  static ObInternalCmdProcessor internal_cmd_processor;
  return internal_cmd_processor;
}

int ObInternalCmdProcessor::init(ObProxyReloadConfig *reload_config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    WARN_ICMD("it has been already inited", K(is_inited_), K(ret));
  } else if (OB_ISNULL(reload_config)) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument", K(reload_config), K(ret));
  } else {
    reload_config_ = reload_config;
    is_inited_ = true;
  }
  return ret;
}

int ObInternalCmdProcessor::execute_cmd(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  DRWLock::WRLockGuard guard(internal_cmd_lock_);
  const ObProxyBasicStmtType type = info.get_cmd_type();
  ObMysqlSM *sm = reinterpret_cast<ObMysqlSM *>(cont);
  ObString table_name;
  if (OB_UNLIKELY(!info.is_internal_cmd())) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("invalid cmd type", K(type), K(ret));
  } else if (NULL == sm) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("sm is null unexpected", K(ret));
  } else {
    ObProxyMysqlRequest &client_request = sm->trans_state_.trans_info_.client_request_;
    table_name = client_request.get_parse_result().get_table_name();
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (get_global_config_processor().is_table_in_service(table_name)
             || OBPROXY_T_DELETE == type || OBPROXY_T_REPLACE == type) {
    // Take the new configuration parsing framework
    DEBUG_ICMD("begin to handle table", K(table_name));
    if (OB_FAIL(ObConfigV2Handler::config_v2_cmd_callback(cont, info, buf, action))) {
      WARN_ICMD("fail to call config_v2_cmd_callback", K(ret));
    } else if (OB_ISNULL(action)) {
      ret = OB_ERR_UNEXPECTED;
      WARN_ICMD("action is still null, it should not happend", K(ret));
    }
  } else if (OB_ISNULL(cmd_table_[type].func_)) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("func is null, it should not happend", K(type), K(ret));
  } else if (OB_FAIL((*(cmd_table_[type].func_))(cont, info, buf, action))) {
    WARN_ICMD("fail to call ObInternalCmdCallbackFunc", K(type), K(ret));
  } else if (OB_ISNULL(action)) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("action is still null, it should not happened", K(type), K(ret));
  } else {
    DEBUG_ICMD("succ to invoke registered func", K(type), K(info));
  }

  if (OB_FAIL(ret) && OB_UNLIKELY(NULL != action)) {
    WARN_ICMD("action is not null, but ret is fail, it should not happened", K(type), K(ret));
    action = NULL;
  }

  return ret;
}

int ObInternalCmdProcessor::register_cmd(const ObProxyBasicStmtType type, ObInternalCmdCallbackFunc func, bool skip_type_check)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(func) || (!skip_type_check &&
      (OB_UNLIKELY(type <= OBPROXY_T_INVALID) || OB_UNLIKELY(type >= OBPROXY_T_ICMD_MAX)))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid func which is NULL", K(type), K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    WARN_ICMD("it has not inited", K(ret));
  } else {
    ObCmdTableInfo &cmd_table = cmd_table_[type];
    if (OB_UNLIKELY(NULL != cmd_table.func_)) {
      ret = OB_INIT_TWICE;
      WARN_ICMD("cmd_table has already been registered", K(cmd_table), K(ret));
    } else {
      cmd_table.type_ = type;
      cmd_table.func_ = func;
    }
  }
  return ret;
}

}//end of obproxy
}//end of oceanbase
