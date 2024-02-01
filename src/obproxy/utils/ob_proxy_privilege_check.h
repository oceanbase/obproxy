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

#ifndef OBPROXY_PRIVILEGE_CHECK_H
#define OBPROXY_PRIVILEGE_CHECK_H

#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_stmt_type.h"
#include "utils/ob_proxy_lib.h"

namespace oceanbase
{
namespace obproxy
{

enum OB_PRIV_SHIFT
{
  OB_PRIV_INVALID_SHIFT = 0,
  OB_PRIV_ALTER_SHIFT,
  OB_PRIV_CREATE_SHIFT,
  OB_PRIV_CREATE_USER_SHIFT,
  OB_PRIV_DELETE_SHIFT,
  OB_PRIV_DROP_SHIFT,
  OB_PRIV_GRANT_SHIFT,
  OB_PRIV_INSERT_SHIFT,
  OB_PRIV_UPDATE_SHIFT,
  OB_PRIV_SELECT_SHIFT,
  OB_PRIV_INDEX_SHIFT,
  OB_PRIV_CREATE_VIEW_SHIFT,
  OB_PRIV_SHOW_VIEW_SHIFT,
  OB_PRIV_SHOW_DB_SHIFT,
  OB_PRIV_SUPER_SHIFT,
  OB_PRIV_PROCESS_SHIFT,
  OB_PRIV_BOOTSTRAP_SHIFT,
  OB_PRIV_MAX_SHIFT_PLUS_ONE
};

struct ObProxyUserName
{
  ObProxyUserName(const common::ObString &user_name, const common::ObString &tenant_name,
                  const common::ObString &cluster_name)
      : user_name_(user_name), tenant_name_(tenant_name), cluster_name_(cluster_name)
  { }
  ~ObProxyUserName() { }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    common::databuff_printf(buf, buf_len, pos, "%.*s@%.*s#%.*s",
                            user_name_.length(), user_name_.ptr(),
                            tenant_name_.length(), tenant_name_.ptr(),
                            cluster_name_.length(), cluster_name_.ptr());
    return pos;
  }

  common::ObString user_name_;
  common::ObString tenant_name_;
  common::ObString cluster_name_;
};

class ObProxySessionPrivInfo
{
public:
  ObProxySessionPrivInfo()
  : has_all_privilege_(false), cs_id_(common::OB_INVALID_FILE_ID), user_priv_set_(-1),
    cluster_name_str_(NULL), tenant_name_str_(NULL), user_name_str_(NULL), logic_user_name_str_(NULL) { }
  ~ObProxySessionPrivInfo() { reset(); }

  void reset();
  int deep_copy(const ObProxySessionPrivInfo *priv_info);
  bool is_same_cluster(const ObProxySessionPrivInfo &other_priv_info) const;
  bool is_same_tenant(const ObProxySessionPrivInfo &other_priv_info) const;
  bool is_same_user(const ObProxySessionPrivInfo &other_priv_info) const;
  bool is_same_logic_user(const ObProxySessionPrivInfo &other_priv_info) const;
  bool has_super_privilege() const { return OB_TEST_PRIVS(user_priv_set_, OB_PRIV_SUPER); }
  bool has_process_privilege() const { return OB_TEST_PRIVS(user_priv_set_, OB_PRIV_PROCESS); }
  static bool is_user_priv_set_available(const ObPrivSet user_priv_set) { return -1 != user_priv_set; };
  ObProxyUserName get_proxy_user_name() const { return ObProxyUserName(user_name_, tenant_name_, cluster_name_); }

  TO_STRING_KV(K_(has_all_privilege), K_(cs_id), K_(user_priv_set), K_(cluster_name),
               K_(tenant_name), K_(user_name));

public:
  bool has_all_privilege_;  //proxyadmin or root@sys
  uint32_t cs_id_;
  ObPrivSet user_priv_set_;

  common::ObString cluster_name_;
  common::ObString tenant_name_;
  common::ObString user_name_;
  common::ObString logic_user_name_;  //just for sharding

private:
  char *cluster_name_str_;
  char *tenant_name_str_;
  char *user_name_str_;
  char *logic_user_name_str_;

  DISALLOW_COPY_AND_ASSIGN(ObProxySessionPrivInfo);
};

class ObProxyPrivilegeCheck
{
public:
  ObProxyPrivilegeCheck() {}
  ~ObProxyPrivilegeCheck() {}

  static bool has_privs(const ObPrivSet user_priv_set, const ObPrivSet need_priv_set)
  {
    return (need_priv_set == (user_priv_set & need_priv_set));
  }

  static int get_need_priv(const sql::stmt::StmtType stmt_type,
                           const ObProxySessionPrivInfo &session_priv,
                           share::schema::ObNeedPriv &need_priv);
  static int check_privilege(const ObProxySessionPrivInfo &session_priv,
                             const share::schema::ObNeedPriv &need_priv,
                             char *&priv_name);
  static int check_user_priv(const ObPrivSet &session_priv_set,
                             const ObPrivSet &need_priv_set,
                             char *&priv_name);
private:
  static const char *priv_names_[];

  static const char *get_first_priv_name(int64_t priv_set);
  static const char *get_priv_name(int64_t priv_shift);
};

} // end of namespace obproxy
} // end of namespace oceanbase

#endif  // OBPROXY_PRIVILEGE_CHECK_H
