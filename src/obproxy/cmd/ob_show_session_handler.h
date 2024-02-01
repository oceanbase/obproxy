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

#ifndef OBPROXY_SHOW_SESSION_HANDLER_H
#define OBPROXY_SHOW_SESSION_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"
#include "proxy/mysql/ob_mysql_client_session.h"
#include "obutils/ob_read_stale_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
enum ObSessionVariableType
{
  OB_FIELD_CHANGED_SYS_VAR = 0,
  OB_FIELD_SYS_VAR,
  OB_FIELD_USER_VAR,
};

enum ObServerSessionType
{
  OB_SST_INVAILED = 0,
  OB_SST_LAST_USED,
  OB_SST_LAST_USED_AND_LII,
  OB_SST_CURRENT,
  OB_SST_CURRENT_AND_LII,
  OB_SST_SS_POOL,
  OB_SST_SS_POOL_AND_LII,
  OB_SST_MAX_TYPE,
};


class ObShowSessionHandler : public ObInternalCmdHandler
{
public:
  ObShowSessionHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowSessionHandler() { cs_id_array_.destroy(); }
  int handle_cs_list(int event, void *data);
  int handle_cs_details(int event, void *data);
  virtual CSIDHanders *get_cs_id_array() { return &cs_id_array_; }

private:
  bool need_privilege_check() { return OBPROXY_T_SUB_SESSION_LIST == sub_type_; }
  int show_cs_list(int event, void *data);
  int show_cs_list_in_thread(const event::ObEThread &ethread);
  int dump_cs_list_header();
  int dump_cs_list(const ObMysqlClientSession &cs);//for list info, simple info

  int dump_cs_details(ObMysqlClientSession &cs);

  int dump_cs_attribute_header();
  int dump_cs_variable_header();
  int dump_cs_stat_header();
  int dump_cs_attribute(const ObMysqlClientSession &cs);//dump attribute info
  int dump_cs_stat(const ObMysqlClientSession &cs);//dump stat info
  int dump_cs_variables_local(ObMysqlClientSession &cs);//dump variables local info(changed sys and user)
  int dump_cs_variables_all(ObMysqlClientSession &cs);//dump variables all info(all sys and user)
  int dump_cs_variables_user(ObMysqlClientSession &cs);//dump variables user info

  int dump_cs_attribute_item(const char *name, const ObString &value, const char *info);
  int dump_cs_attribute_item(const char *name, const int64_t value, const char *info);
  int dump_cs_attribute_item(const char *name, const char *value, const char *info);
  int dump_cs_attribute_ss(const ObMysqlServerSession &svr_session,
                                    const ObServerSessionType type, const int64_t idx = -1);

  int dump_cs_stat_item(const char *name, const int64_t &value);
  int dump_cs_variables_item(const ObSessionBaseField *field, const ObSessionVariableType var_type);

  int dump_cs_read_stale_replica_header();
  int dump_cs_read_stale_replica(ObMysqlClientSession &cs);
  int dump_cs_read_stale_replica_item(const obutils::ObReadStaleFeedback *feedback);
  
  template<typename T>
  int dump_cs_variables_common(T &vars, const ObSessionVariableType var_type)
  {
    int ret = OB_SUCCESS;
    const ObSessionBaseField *field = NULL;
    ObString name;
    for (int64_t i = 0; OB_SUCC(ret) && i < vars.count(); ++i) {
      field = static_cast<const ObSessionBaseField *>(&(vars.at(i)));
      name.assign_ptr(field->name_, field->name_len_);
      if (match_like(name, like_name_)) {
        DEBUG_ICMD("session variables name matched", K_(like_name), K(name));
        if (OB_FAIL(dump_cs_variables_item(field, var_type))) {
          WARN_ICMD("fail to dump cs variables item", K(ret));
        }
      }
    }
    return ret;
  }

  ObString get_var_info_str(ObSessionVariableType type) const;
  ObString get_var_modify_mod_str(ObSessionFieldModifyMod modify_mod) const;
  int get_var_scope_str(const ObSessionSysField *field, ObString &scope_string) const;
  int fill_scope_string(const char *string, const int32_t string_len, ObString &scope_string) const;
  bool enable_show_session(const ObMysqlClientSession &cs) const;

public:
  ObProxySessionPrivInfo session_priv_;
  share::schema::ObNeedPriv need_priv_;

private:
  static const int64_t MAX_SYS_VAR_COUNT = 64;
  static const int64_t MAX_CHANGED_SYS_VAR_COUNT = 32;
  static const int64_t MAX_USER_VAR_COUNT = 32;

  const ObProxyBasicStmtSubType sub_type_;
  int64_t list_bucket_;
  CSIDHanders cs_id_array_;

  DISALLOW_COPY_AND_ASSIGN(ObShowSessionHandler);
};

int show_session_cmd_init();

inline bool ObShowSessionHandler::enable_show_session(const ObMysqlClientSession &cs) const
{
  const ObProxySessionPrivInfo &other_priv_info = cs.get_session_info().get_priv_info();
  return  session_priv_.has_all_privilege_
          || session_priv_.tenant_name_ == OB_SYS_TENANT_NAME
          || ((!cs.get_session_info().is_sharding_user() && session_priv_.is_same_tenant(other_priv_info))
              && (session_priv_.cs_id_ == cs.get_cs_id())
              && (session_priv_.is_same_user(other_priv_info) || session_priv_.has_process_privilege()))
          || (cs.get_session_info().is_sharding_user() && session_priv_.is_same_logic_user(other_priv_info));
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_SESSION_HANDLER_H */

