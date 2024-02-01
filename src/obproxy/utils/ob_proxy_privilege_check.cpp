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

#define USING_LOG_PREFIX PROXY

#include "utils/ob_proxy_privilege_check.h"
#include "iocore/eventsystem/ob_buf_allocator.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql::stmt;

namespace oceanbase
{
namespace obproxy
{

const char *ObProxyPrivilegeCheck::priv_names_[OB_PRIV_MAX_SHIFT_PLUS_ONE] = {
    "INVALID",
    "ALTER",
    "CREATE",
    "CREATE USER",
    "DELETE",
    "DROP",
    "GRANT",
    "INSERT",
    "UPDATE",
    "SELECT",
    "INDEX",
    "CREATE VIEW",
    "SHOW VIEW",
    "SHOW DB",
    "SUPER",
    "PROCESS",
    "BOOTSTRAP",
};

void ObProxySessionPrivInfo::reset()
{
  //reset superclass
  has_all_privilege_ = false;
  cs_id_ = OB_INVALID_FILE_ID;
  user_priv_set_ = -1;
  if (NULL != cluster_name_str_) {
    op_fixed_mem_free(cluster_name_str_, cluster_name_.length());
    cluster_name_str_ = NULL;
  }
  if (NULL != tenant_name_str_) {
    op_fixed_mem_free(tenant_name_str_, tenant_name_.length());
    tenant_name_str_ = NULL;
  }
  if (NULL != user_name_str_) {
    op_fixed_mem_free(user_name_str_, user_name_.length());
    user_name_str_ = NULL;
  }
  if (NULL != logic_user_name_str_) {
    op_fixed_mem_free(logic_user_name_str_, logic_user_name_.length());
    logic_user_name_str_ = NULL;
  }
  cluster_name_.reset();
  tenant_name_.reset();
  user_name_.reset();
  logic_user_name_.reset();
}

int ObProxySessionPrivInfo::deep_copy(const ObProxySessionPrivInfo *priv_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(priv_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("Invalid priv info", K(priv_info), K(ret));
  } else if (OB_UNLIKELY(NULL != cluster_name_str_)
             || OB_UNLIKELY(NULL != tenant_name_str_)
             || OB_UNLIKELY(NULL != user_name_str_)
             || OB_UNLIKELY(NULL != logic_user_name_str_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("name buff is not null", K(cluster_name_str_), K(tenant_name_str_), 
             K(user_name_str_), K(logic_user_name_str_), K(ret));
  } else {
    has_all_privilege_ = priv_info->has_all_privilege_;
    cs_id_ = priv_info->cs_id_;
    user_priv_set_ = priv_info->user_priv_set_;
    int32_t name_length = 0;
    if (OB_SUCC(ret) && OB_LIKELY(!priv_info->cluster_name_.empty())) {
      name_length = priv_info->cluster_name_.length();
      cluster_name_str_ = static_cast<char *>(op_fixed_mem_alloc(name_length));
      if (OB_ISNULL(cluster_name_str_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem", K(name_length), K(ret));
      } else {
        MEMCPY(cluster_name_str_, priv_info->cluster_name_.ptr(), name_length);
        cluster_name_.assign_ptr(cluster_name_str_, name_length);
      }
    }

    if (OB_SUCC(ret) && OB_LIKELY(!priv_info->tenant_name_.empty())) {
      name_length = priv_info->tenant_name_.length();
      tenant_name_str_ = static_cast<char *>(op_fixed_mem_alloc(name_length));
      if (OB_ISNULL(tenant_name_str_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem", K(name_length), K(ret));
      } else {
        MEMCPY(tenant_name_str_, priv_info->tenant_name_.ptr(), name_length);
        tenant_name_.assign_ptr(tenant_name_str_, name_length);
      }
    }

    if (OB_SUCC(ret) && OB_LIKELY(!priv_info->user_name_.empty())) {
      name_length = priv_info->user_name_.length();
      user_name_str_ = static_cast<char *>(op_fixed_mem_alloc(name_length));
      if (OB_ISNULL(user_name_str_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem", K(name_length), K(ret));
      } else {
        MEMCPY(user_name_str_, priv_info->user_name_.ptr(), name_length);
        user_name_.assign_ptr(user_name_str_, name_length);
      }
    }

    if (OB_SUCC(ret) && OB_LIKELY(!priv_info->logic_user_name_.empty())) {
      name_length = priv_info->logic_user_name_.length();
      logic_user_name_str_ = static_cast<char *>(op_fixed_mem_alloc(name_length));
      if (OB_ISNULL(logic_user_name_str_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem", K(name_length), K(ret));
      } else {
        MEMCPY(logic_user_name_str_, priv_info->logic_user_name_.ptr(), name_length);
        logic_user_name_.assign_ptr(logic_user_name_str_, name_length);
      }
    }
  }
  return ret;
}

bool ObProxySessionPrivInfo::is_same_cluster(const ObProxySessionPrivInfo &other_priv_info) const
{
  bool bret = true;
  if (0 != cluster_name_.compare(other_priv_info.cluster_name_)) {
    bret = false;
    LOG_DEBUG("not same cluster", "self", cluster_name_, "other", other_priv_info.cluster_name_);
  }
  return bret;
}

bool ObProxySessionPrivInfo::is_same_tenant(const ObProxySessionPrivInfo &other_priv_info) const
{
  bool bret = true;
  if (0 != cluster_name_.compare(other_priv_info.cluster_name_)) {
    bret = false;
    LOG_DEBUG("not same cluster", "self", cluster_name_, "other", other_priv_info.cluster_name_);
  } else if (0 != tenant_name_.compare(other_priv_info.tenant_name_)) {
    bret = false;
    LOG_DEBUG("not same tenant name", "self", tenant_name_, "other", other_priv_info.tenant_name_);
  }
  return bret;
}

// we need call is_same_tenant() before call it
bool ObProxySessionPrivInfo::is_same_user(const ObProxySessionPrivInfo &other_priv_info) const
{
  bool bret = true;
  if (0 != user_name_.compare(other_priv_info.user_name_)) {
    bret = false;
    LOG_DEBUG("not same user name", "self", user_name_, "other", other_priv_info.user_name_);
  }
  return bret;
}

bool ObProxySessionPrivInfo::is_same_logic_user(const ObProxySessionPrivInfo &other_priv_info) const
{
  bool bret = true;
  if (0 != logic_user_name_.compare(other_priv_info.logic_user_name_)) {
    bret = false;
    LOG_DEBUG("not same logic user name", "self", logic_user_name_, "other", other_priv_info.logic_user_name_);
  }
  return bret;
}

int ObProxyPrivilegeCheck::get_need_priv(const StmtType stmt_type,
    const ObProxySessionPrivInfo &session_priv, ObNeedPriv &need_priv)
{
  int ret = OB_SUCCESS;
  if (session_priv.has_all_privilege_) {
    LOG_DEBUG("session priv has all privilege, no need to get need priv", K(session_priv), K(need_priv));
  } else {
    switch (stmt_type) {
      case T_SHOW_PROCESSLIST:
      case T_KILL: {
        //do nothing
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("it should not come here", K(stmt_type), K(ret));
      }
    }
  }
  return ret;
}

int ObProxyPrivilegeCheck::check_privilege(const ObProxySessionPrivInfo &session_priv,
    const ObNeedPriv &need_priv, char *&priv_name)
{
  int ret = OB_SUCCESS;
  priv_name = NULL;
  if (session_priv.has_all_privilege_) {
    LOG_DEBUG("user has all privilege, can do any thing", K(session_priv));
  } else {
    switch (need_priv.priv_level_) {
      case OB_PRIV_USER_LEVEL: {
        if (OB_FAIL(check_user_priv(session_priv.user_priv_set_, need_priv.priv_set_, priv_name))) {
          LOG_WDIAG("No privilege", K(session_priv), K(need_priv), K(priv_name), K(ret));
        }
        break;
      }
      case OB_PRIV_DB_LEVEL:
      case OB_PRIV_TABLE_LEVEL:
      case OB_PRIV_DB_ACCESS_LEVEL:
      default: {
        break;
      }
    }
  }
  return ret;
}

int ObProxyPrivilegeCheck::check_user_priv(const ObPrivSet &user_priv_set,
    const ObPrivSet &need_priv_set, char *&priv_name)
{
  int ret = OB_SUCCESS;
  if (!has_privs(user_priv_set, need_priv_set)) {
    ret = OB_ERR_NO_PRIVILEGE;
  }
  if (OB_ERR_NO_PRIVILEGE == ret) {
    ObPrivSet lack_priv_set = need_priv_set &(~user_priv_set);
    priv_name = const_cast<char *>(get_first_priv_name(lack_priv_set));
    if (OB_ISNULL(priv_name)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("Invalid priv type", "priv_set", lack_priv_set);
    }
  }
  return ret;
}

const char *ObProxyPrivilegeCheck::get_first_priv_name(int64_t priv_set)
{
  const char *cret = NULL;
  for (int64_t shift = 0; NULL == cret && shift < OB_PRIV_MAX_SHIFT_PLUS_ONE; ++shift) {
    if (OB_PRIV_HAS_ANY(priv_set, OB_PRIV_GET_TYPE(shift))) {
      cret = get_priv_name(shift);
    }
  }
  return cret;
}

const char *ObProxyPrivilegeCheck::get_priv_name(int64_t priv_shift) {
  const char *cret = NULL;
  if (priv_shift > OB_PRIV_INVALID_SHIFT && priv_shift < OB_PRIV_MAX_SHIFT_PLUS_ONE) {
    cret = priv_names_[priv_shift];
  }
  return cret;
}

} // end of namespace obproxy
} // end of namespace oceanbase
