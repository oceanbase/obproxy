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
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX PROXY

#include <sqlite/sqlite3.h>
#include "omt/ob_white_list_table_processor.h"
#include "obutils/ob_config_processor.h"
#include "utils/ob_proxy_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
using namespace obsys;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

int ObWhiteListTableProcessor::execute(void *arg)
{
  int ret = OB_SUCCESS;
  ObFnParams *params = static_cast<ObFnParams*>(arg);
  ObString tenant_name;
  ObString cluster_name;
  ObString ip_list;
  if (NULL == params->fields_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("fileds is null unexpected", K(ret));
  } else {
    int64_t index = params->row_index_;
    for (int64_t i = 0; OB_SUCC(ret) && i < params->fields_->field_num_; i++) {
      SqlField *sql_field = params->fields_->fields_.at(i);
      if (index >= sql_field->column_values_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("index out of range, invalid value for white_list",
                  K(index), K(sql_field->column_values_.count()), K_(sql_field->column_name), K(ret));
      } else if (0 == sql_field->column_name_.config_string_.case_compare("value")) {
        ip_list = sql_field->column_values_.at(index).column_value_;
      } else if (0 == sql_field->column_name_.config_string_.case_compare("cluster_name")) {
        cluster_name = sql_field->column_values_.at(index).column_value_;
      } else if (0 == sql_field->column_name_.config_string_.case_compare("tenant_name")) {
        tenant_name = sql_field->column_values_.at(index).column_value_;
      }
    }
    if (cluster_name.empty() || cluster_name.length() > OB_PROXY_MAX_CLUSTER_NAME_LENGTH
        || tenant_name.empty() || tenant_name.length() > OB_MAX_TENANT_NAME_LENGTH) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("execute failed, tenant_name or cluster_name is null", K(ret),
                K(cluster_name), K(tenant_name));
    } else if (OB_SUCC(ret)){
      // 多值修改时，仅第一个值需要创建backup；其他值可以直接写入backup
      if (params->stmt_type_ == OBPROXY_T_REPLACE) {
        if (OB_FAIL(get_global_white_list_table_processor().set_ip_list(
                cluster_name, tenant_name, ip_list, params->row_index_ == 0))) {
          LOG_WDIAG("set ip list failed", K(ret), K(cluster_name), K(tenant_name), K(ip_list));
        }
      } else if (params->stmt_type_ == OBPROXY_T_DELETE) {
        if (OB_FAIL(get_global_white_list_table_processor().delete_ip_list(
                cluster_name, tenant_name, params->row_index_ == 0))) {
          LOG_WDIAG("delete ip list failed", K(ret), K(cluster_name), K(tenant_name));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected stmt type", K(ret), K(params->stmt_type_));
      }
    }
  }

  return ret;
}

int ObWhiteListTableProcessor::commit(void* arg, bool is_success)
{
  UNUSED(arg);
  if (is_success) {
    get_global_white_list_table_processor().inc_index();
    if (OB_UNLIKELY(IS_DEBUG_ENABLED())) {
      get_global_white_list_table_processor().print_config();
    }
  } else {
    LOG_WDIAG("white list commit failed");
  }
  get_global_white_list_table_processor().clean_hashmap(
    get_global_white_list_table_processor().get_backup_hashmap());

  return OB_SUCCESS;
}

ObWhiteListTableProcessor &get_global_white_list_table_processor()
{
  static ObWhiteListTableProcessor g_white_list_table_processor;
  return g_white_list_table_processor;
}

int ObWhiteListTableProcessor::init()
{
  int ret = OB_SUCCESS;
  ObConfigHandler handler;
  LOG_INFO("start init ObWhiteListTableProcessor");
  handler.execute_func_ = &ObWhiteListTableProcessor::execute;
  handler.commit_func_ = &ObWhiteListTableProcessor::commit;
  if (OB_FAIL(addr_hash_map_array_[0].create(32, ObModIds::OB_HASH_BUCKET))) {
    LOG_WDIAG("create hash map failed", K(ret));
  } else if (OB_FAIL(addr_hash_map_array_[1].create(32, ObModIds::OB_HASH_BUCKET))) {
    LOG_WDIAG("create hash map failed", K(ret));
  } else if (OB_FAIL(get_global_config_processor().register_callback("white_list", handler))) {
    LOG_WDIAG("register white_list table callback failed", K(ret));
  }

  return ret;
}

int ObWhiteListTableProcessor::ip_to_int(char *ip_addr, uint32_t& value)
{
  int ret = OB_SUCCESS;
  in_addr addr4;
  if (1 == inet_aton(ip_addr, &addr4)) {
    value = to_little_endian(addr4.s_addr);
  } else {
    ret = ob_get_sys_errno();
  }
  return ret;
}

int ObWhiteListTableProcessor::set_ip_list(ObString &cluster_name, ObString &tenant_name, ObString &ip_list,
                                          const bool need_to_backup)
{
  int ret = OB_SUCCESS;

  DRWLock::WRLockGuard guard(white_list_lock_);
  if (cluster_name.empty() || tenant_name.empty() || ip_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("argument is unexpected", K(ret), K(cluster_name), K(tenant_name), K(ip_list));
  } else if (need_to_backup && OB_FAIL(backup_hash_map())) {
    LOG_WDIAG("backup hash map failed", K(ret));
  } else {
    ObSEArray<AddrStruct, 4> addr_array;
    char *start = ip_list.ptr();
    int pos = 0;
    AddrStruct ip_addr;
    memset(&ip_addr, 0, sizeof(AddrStruct));
    bool set_ip = false;
    while (OB_SUCC(ret) && pos < ip_list.length()) {
      if (ip_list[pos] == ',' || pos == ip_list.length() - 1 || ip_list[pos] == '/') {
        char buf[64];
        int64_t len = static_cast<int64_t>(ip_list.ptr() + pos - start);
        // to the end
        if (pos == ip_list.length() - 1) {
          len++;
        }
        memcpy(buf, start, len);
        buf[len] = '\0';
        if (!set_ip) {
          if (false == ip_addr.addr_.set_ip_addr(buf, 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("set ip addr failed", K(buf), K(ret));
            break;
          } else {
            if (ObAddr::VER::IPV4 == ip_addr.addr_.version_) {
              ip_addr.v4_ = to_little_endian(htonl(ip_addr.addr_.ip_.v4_));
            }
            set_ip = true;
          }
        } else {
          ip_addr.net_ = atoi(buf);
          if (ip_addr.net_ <= 0 || ip_addr.net_ >= 32) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("atoi failed", K(ip_addr.net_), K(ret));
            break;
          }
        }

        if (ip_list[pos] != '/') {
          if (OB_FAIL(addr_array.push_back(ip_addr))) {
            LOG_WDIAG("ip addr push back failed", K(ret));
          } else {
            memset(&ip_addr, 0, sizeof(AddrStruct));
            set_ip = false;
          }
        }
        pos++;
        start = ip_list.ptr() + pos;
      } else if (ip_list[pos] >= '0' || ip_list[pos] <= '9' || ip_list[pos] == '.') {
        pos++;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid char in ip_list", K(ret), K(ip_list));
      }
    }
    if (OB_SUCC(ret)) {
      ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> key_string;
      if (OB_FAIL(paste_tenant_and_cluster_name(tenant_name, cluster_name, key_string))) {
        LOG_WDIAG("paster tenant and cluser name failed", K(ret), K(tenant_name), K(cluster_name));
      } else {
        WhiteListHashMap &backup_map = addr_hash_map_array_[(index_ + 1) % 2];
        if (OB_FAIL(backup_map.set_refactored(key_string, addr_array, 1))) {
          LOG_WDIAG("addr hash map set failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObWhiteListTableProcessor::delete_ip_list(ObString &cluster_name, ObString &tenant_name,
                                              const bool need_to_backup)
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(white_list_lock_);
  if (cluster_name.empty() || tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("cluster name or tenant name empty unexpected", K(ret));
  } else if (need_to_backup && OB_FAIL(backup_hash_map())) {
    LOG_WDIAG("backup hash map failed", K(ret));
  } else {
    LOG_DEBUG("delete ip list", K(cluster_name), K(tenant_name));
    ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> key_string;
    if (OB_FAIL(paste_tenant_and_cluster_name(tenant_name, cluster_name, key_string))) {
      LOG_WDIAG("paste tenant and cluster name failed", K(ret), K(tenant_name), K(cluster_name));
    } else {
      WhiteListHashMap &backup_map = addr_hash_map_array_[(index_ + 1) % 2];
      if (OB_FAIL(backup_map.erase_refactored(key_string))) {
        LOG_WDIAG("addr hash map erase failed", K(ret));
      }
    }
  }

  return ret;
}

bool ObWhiteListTableProcessor::can_ip_pass(const ObString &cluster_name, const ObString &tenant_name,
                                            const ObString &user_name, const struct sockaddr& addr)
{
  bool can_pass = false;
  if (net::ops_is_ip4(addr)) {
    can_pass = can_ipv4_pass(cluster_name, tenant_name, user_name, addr);
  } else if (net::ops_is_ip6(addr)) {
    can_pass = can_ipv6_pass(cluster_name, tenant_name, user_name, addr);
  }
  return can_pass;
}

bool ObWhiteListTableProcessor::can_ipv4_pass(const ObString &cluster_name, const ObString &tenant_name,
                                              const ObString &user_name, const struct sockaddr& in_addr)
{
  int ret = OB_SUCCESS;
  bool can_pass = false;
  ObIpEndpoint endpoint(in_addr);
  uint32_t ip = to_little_endian(endpoint.sin_.sin_addr.s_addr);
  LOG_DEBUG("check can ip pass", K(cluster_name), K(tenant_name), K(endpoint));
  if (!cluster_name.empty() && !tenant_name.empty()) {
    ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> key_string;
    if (OB_FAIL(paste_tenant_and_cluster_name(tenant_name, cluster_name, key_string))) {
      LOG_WDIAG("paste tenant and cluster name failed", K(ret), K(tenant_name), K(cluster_name));
    } else {
      ObSEArray<AddrStruct, 4> ip_array;
      DRWLock::RDLockGuard guard(white_list_lock_);
      WhiteListHashMap &current_map = addr_hash_map_array_[index_];
      if (OB_SUCC(current_map.get_refactored(key_string, ip_array))) {
        for (int64_t i = 0; !can_pass && i < ip_array.count(); i++) {
          AddrStruct addr = ip_array.at(i);
          if (0 == addr.v4_) {
            can_pass = true;
          } else if (0 == addr.net_) {
            can_pass = (addr.v4_ == ip);
          } else {
            uint32_t mask = 0xffffffff;
            can_pass = ((addr.v4_ & (mask << (32 - addr.net_))) == (ip & (mask << (32 - addr.net_))));
          }
        }
      } else if (OB_HASH_NOT_EXIST == ret) {
        can_pass = true;
      } else {
        LOG_WDIAG("unexpected error", K(ret));
      }
    }
  } else {
    LOG_WDIAG("cluster_name or tenant_name is empty", K(cluster_name), K(tenant_name), K(endpoint));
  }

  if (!can_pass) {
    LOG_WDIAG("can not pass white_list", K(cluster_name), K(tenant_name), K(user_name), K(endpoint));
  }

  return can_pass;
}

bool ObWhiteListTableProcessor::can_ipv6_pass(const ObString &cluster_name, const ObString &tenant_name,
                                              const ObString &user_name, const struct sockaddr& in6_addr)
{
  int ret = OB_SUCCESS;
  bool can_pass =false;
  ObIpEndpoint endpoint(in6_addr);
  LOG_DEBUG("check can ip pass", K(cluster_name), K(tenant_name), K(endpoint));
  if (!cluster_name.empty() && !tenant_name.empty()) {
    ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> key_string;
    if (OB_FAIL(paste_tenant_and_cluster_name(tenant_name, cluster_name, key_string))) {
      LOG_WDIAG("paste tenant and cluster name failed", K(ret), K(tenant_name), K(cluster_name));
    } else {
      ObSEArray<AddrStruct, 4> ip_array;
      DRWLock::RDLockGuard guard(white_list_lock_);
      WhiteListHashMap &current_map = addr_hash_map_array_[index_];
      if (OB_SUCC(current_map.get_refactored(key_string, ip_array))) {
        for (int64_t i = 0; !can_pass && i < ip_array.count(); i++) {
          AddrStruct addr = ip_array.at(i);
          if (addr.addr_.ip_.v6_[0] == 0 && addr.addr_.ip_.v6_[1] == 0
              && addr.addr_.ip_.v6_[2] == 0 && addr.addr_.ip_.v6_[3] == 0) {
            can_pass = true;
          } else {
            int64_t net = addr.net_;
            int64_t index = 0;
            can_pass = true;
            sockaddr_storage ss = addr.addr_.get_sockaddr();
            sockaddr_in6 &in6 = *(sockaddr_in6*)(&ss);
            while ((net - 8) >= 0 && can_pass) {
              if (in6.sin6_addr.s6_addr[index] != endpoint.sin6_.sin6_addr.s6_addr[index]) {
                can_pass = false;
              }
              index++;
              net -= 8;
            }

            if (net && can_pass) {
              can_pass = in6.sin6_addr.s6_addr[index] ==
                 (endpoint.sin6_.sin6_addr.s6_addr[index] & (0xff << (8 - net)));
            }
          }
        }
      } else if (OB_HASH_NOT_EXIST == ret) {
        can_pass = true;
      } else {
        LOG_WDIAG("unexpected error", K(ret));
      }
    }
  } else {
    LOG_WDIAG("cluster_name or tenant_name is empty", K(cluster_name), K(tenant_name), K(endpoint));
  }

  if (!can_pass) {
    LOG_WDIAG("can not pass white_list", K(cluster_name), K(tenant_name), K(user_name), K(endpoint));
  }

  return can_pass;
}

int64_t AddrStruct::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(addr), K_(net));
  J_OBJ_END();
  return pos;
}

int ObWhiteListTableProcessor::backup_hash_map()
{
  int ret = OB_SUCCESS;
  int64_t backup_index = (index_ + 1) % 2;
  WhiteListHashMap &backup_map = addr_hash_map_array_[backup_index];
  WhiteListHashMap &current_map = addr_hash_map_array_[index_];
  if (OB_FAIL(backup_map.reuse())) {
    LOG_WDIAG("backup map failed", K(ret));
  } else {
    WhiteListHashMap::iterator iter = current_map.begin();
    for(; OB_SUCC(ret) && iter != current_map.end(); ++iter) {
      if (OB_FAIL(backup_map.set_refactored(iter->first, iter->second))) {
        LOG_WDIAG("backup map set refactored failed", K(ret));
      }
    }
  }
  return ret;
}

void ObWhiteListTableProcessor::inc_index()
{
  DRWLock::WRLockGuard guard(white_list_lock_);
  index_ = (index_ + 1) % 2;
}

void ObWhiteListTableProcessor::clean_hashmap(WhiteListHashMap& whitelist_map)
{
  DRWLock::WRLockGuard guard(white_list_lock_);
  whitelist_map.reuse();
}

void ObWhiteListTableProcessor::print_config()
{
  DRWLock::RDLockGuard guard(white_list_lock_);
  WhiteListHashMap &current_map = addr_hash_map_array_[index_];
  WhiteListHashMap::iterator iter = current_map.begin();
  for(; iter != current_map.end(); ++iter) {
    LOG_DEBUG("white list map info", K(iter->first));
    for (int64_t i = 0; i < iter->second.count(); i++) {
      AddrStruct addr_info = iter->second.at(i);
      LOG_DEBUG("ip/net info", K(addr_info));
    }
  }
}

uint32_t ObWhiteListTableProcessor::to_little_endian(uint32_t value) {
  // Regardless of whether the current host is big endian or little endian,
  // the storage format of uint32_t is changed to the same as the little endian type
  // Because the network sequence is a big-endian type, the conversion is performed directly
  return __bswap_32 (value);
}

} // end of omt
} // end of obproxy
} // end of oceanbase
