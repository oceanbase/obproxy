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

#define USING_LOG_PREFIX PROXY_IMCD

#include "cmd/ob_kill_op_handler.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
ObKillOpHandler::ObKillOpHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
  : ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type()),
    ss_id_(info.get_ss_id()), capability_(info.get_capability()), cs_id_array_()
{
  SET_HANDLER(&ObKillOpHandler::handle_kill_option);
  SET_CS_HANDLER(&ObKillOpHandler::kill_session);
}

int ObKillOpHandler::handle_kill_option(int event, void *data)
{
  int event_ret = EVENT_NONE;
  int ret = OB_SUCCESS;
  int errcode = OB_SUCCESS;
  bool need_callback = true;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else {
    ObEThread *ethread = NULL;
    bool is_proxy_conn_id = true;
    if (need_privilege_check() && OB_FAIL(do_privilege_check(session_priv_, need_priv_, errcode))) {
      WARN_ICMD("fail to do privilege check", K(errcode), K(ret));
    }
    if (OB_SUCCESS != errcode) {
      WARN_ICMD("fail to check user privilege, try to response err packet", K(errcode), K(ret));
    } else if (OB_ISNULL(ethread = this_ethread())) {
      ret = OB_ERR_UNEXPECTED;
      WARN_ICMD("cur ethread is null, it should not happened", K(ret));
    } else if (!is_conn_id_avail(cs_id_, is_proxy_conn_id)) {
      errcode = OB_UNKNOWN_CONNECTION; //not found the specific session
      WARN_ICMD("cs_id is not avail", K(cs_id_), K(errcode));
      if (OB_FAIL(encode_err_packet(errcode, cs_id_))) {
        WARN_ICMD("fail to encode err resp packet", K(errcode), K_(cs_id), K(ret));
      }
    } else {
      if (is_proxy_conn_id) {
        //session id got from obproxy
        int64_t thread_id = -1;
        if (OB_FAIL(extract_thread_id(static_cast<uint32_t>(cs_id_), thread_id))) {
          WARN_ICMD("fail to extract thread id, it should not happened", K(cs_id_), K(ret));
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
        //session id got from observer
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

int ObKillOpHandler::get_server_session(const ObMysqlClientSession &cs,
    const ObMysqlServerSession *&server_session)
{
  int ret = OB_SUCCESS;
  if (NULL != cs.get_server_session()
      && OB_LIKELY(NULL != cs.get_server_session()->get_client_session())
      && OB_LIKELY(cs.get_server_session()->ss_id_ == ss_id_)) {
    server_session = cs.get_server_session();
  } else if (NULL != cs.get_cur_server_session()
      && OB_LIKELY(NULL != cs.get_cur_server_session()->get_client_session())
      && OB_LIKELY(cs.get_cur_server_session()->ss_id_ == ss_id_)) {
    server_session = cs.get_cur_server_session();
  } else {
    ObServerSessionPool::IPHashTable &ip_pool = const_cast<ObMysqlSessionManager &>(cs.get_session_manager()).get_session_pool().ip_pool_;
    ObServerSessionPool::IPHashTable::iterator end = ip_pool.end();
    ObServerSessionPool::IPHashTable::iterator begin = ip_pool.begin();
    for (ret = OB_ENTRY_NOT_EXIST; OB_ENTRY_NOT_EXIST == ret && begin != end; ++begin) {
      if (NULL != begin->get_client_session()
          && begin->ss_id_ == ss_id_) {
        server_session = &(*begin);
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObKillOpHandler::kill_session(ObMysqlClientSession &cs)
{
  int ret = OB_SUCCESS;
  int64_t affected_row = 0;
  int errcode = OB_SUCCESS;
  switch (sub_type_) {
    case OBPROXY_T_SUB_KILL_CONNECTION:
    case OBPROXY_T_SUB_KILL_CS: {
      if (session_priv_.cs_id_ == cs.get_cs_id()) {// kill self
        errcode = OB_ERR_QUERY_INTERRUPTED;
        DEBUG_ICMD("try to kill self client session", K_(cs_id), K(errcode));
        if (OB_FAIL(encode_err_packet(errcode))) {
          WARN_ICMD("fail to encode err resp packet", K(errcode), K(ret));
        } else {
          cs.vc_ready_killed_ = true;// after send msg succ, we will kill self
        }
      } else {
        ObProxySessionPrivInfo &other_priv_info = cs.get_session_info().get_priv_info();
        DEBUG_ICMD("try to kill other client session", K_(cs_id), K_(session_priv),
                                                      K(other_priv_info));
        // root@sys, and root@proxysys can kill any session of any tenant,
        // the other can only kill the session that at the same cluster.tenant.user,
        // if it has mysql super privilege, it can kill other user of same tenant
        bool has_privilege = false;
        if (session_priv_.has_all_privilege_) {
          DEBUG_ICMD("curr user has the all privilege to kill any client session");
          has_privilege = true;
        } else if (session_priv_.is_same_tenant(other_priv_info)) {
          if (session_priv_.is_same_user(other_priv_info) || session_priv_.has_super_privilege()) {
            DEBUG_ICMD("curr user has the privilege to kill this client session");
            has_privilege = true;
          } else {
            errcode = OB_ERR_KILL_DENIED;
            DEBUG_ICMD("same cluster.tenant, but different user, not the owner to execute kill",
                      K(errcode));
          }
        } else { // different tenant's user is invisible to others user
          errcode = OB_UNKNOWN_CONNECTION;
          DEBUG_ICMD("curr use has no privilege to kill others", K(errcode));
        }
        if (has_privilege) {
          if (OB_FAIL(encode_ok_packet(affected_row, capability_))) {
            WARN_ICMD("fail to encode ok packet", K(ret));
          } else {
            ObNetVConnection *net_vc = cs.get_netvc();
            if (NULL != net_vc) {
              net_vc->set_is_force_timeout(true);
            }
          }
        } else {
          if (OB_FAIL(encode_err_packet(errcode, cs_id_))) {
            WARN_ICMD("fail to encode err resp packet", K(errcode), K_(cs_id), K(ret));
          }
        }
      }
      break;
    }
    case OBPROXY_T_SUB_KILL_SS: {
      DEBUG_ICMD("try to kill server session", K_(cs_id), K_(ss_id));
      const ObMysqlServerSession *server_session = NULL;
      if (OB_FAIL(get_server_session(cs, server_session))) {
        WARN_ICMD("fail to get server session", K_(cs_id), K_(ss_id), K(ret));
        if (OB_ENTRY_NOT_EXIST == ret) {
          errcode = OB_UNKNOWN_CONNECTION;
          DEBUG_ICMD("unknown server session", K_(cs_id), K_(ss_id), K(errcode), K(ret));
          if (OB_FAIL(encode_err_packet(errcode, ss_id_))) {
            WARN_ICMD("fail to encode err resp packet", K(errcode), K_(ss_id), K(ret));
          }
        }
      } else if (OB_FAIL(encode_ok_packet(affected_row, capability_))) {
        WARN_ICMD("fail to encode ok packet", K(ret));
      } else {
        server_session->get_netvc()->set_is_force_timeout(true);
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      WARN_ICMD("it should not come here", K_(sub_type), K(ret));
      break;
    }
  }
  return ret;
}

static int kill_op_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObEThread *ethread = NULL;
  ObKillOpHandler *handler = NULL;
  const bool is_query_cmd = false;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObKillOpHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_ICMD("fail to new ObKillOpHandler", K(ret));
  } else if (OB_FAIL(handler->init(is_query_cmd))) {
    WARN_ICMD("fail to init for ObKillOpHandler");
  } else if (OB_FAIL(handler->session_priv_.deep_copy(info.session_priv_))) {
    WARN_ICMD("fail to deep copy session priv");
  } else if (OB_FAIL(ObProxyPrivilegeCheck::get_need_priv(sql::stmt::T_KILL, handler->session_priv_, handler->need_priv_))) {
    WARN_ICMD("fail to get need priv");
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("cur ethread is null, it should not happened", K(ret));
  } else {
    action = &handler->get_action();
    if (OB_ISNULL(ethread->schedule_imm(handler, ET_NET))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ERROR_ICMD("fail to schedule ObKillOpHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObKillOpHandler", K(handler->session_priv_));
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int kill_op_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_KILL_MYSQL, &kill_op_cmd_callback))) {
    WARN_ICMD("fail to register OBPROXY_T_ICMD_KILL", K(ret));
  } else if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_KILL_SESSION, &kill_op_cmd_callback))) {
    WARN_ICMD("fail to register OBPROXY_T_ICMD_KILL_SESSION", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
