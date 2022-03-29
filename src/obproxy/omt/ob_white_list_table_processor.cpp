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
  ObCloudFnParams *params = reinterpret_cast<ObCloudFnParams*>(arg);
  ObString tenant_name = params->tenant_name_;
  ObString cluster_name = params->cluster_name_;
  ObString ip_list;
  if (NULL == params->fields_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fileds is null unexpected", K(ret));
  } else if (cluster_name.empty() || cluster_name.length() > OB_PROXY_MAX_CLUSTER_NAME_LENGTH
            || tenant_name.empty() || tenant_name.length() > OB_MAX_TENANT_NAME_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("execute failed, tenant_name or cluster_name is null", K(ret), K(cluster_name), K(tenant_name));
  } else {
    for (int64_t i = 0; i < params->fields_->field_num_; i++) {
      SqlField &sql_field = params->fields_->fields_.at(i);
      if (sql_field.column_name_.string_ == "value") {
        ip_list = sql_field.column_value_.config_string_;
      }
    }

    if (params->stmt_type_ == OBPROXY_T_REPLACE) {
      if (OB_FAIL(get_global_white_list_table_processor().set_ip_list(cluster_name, tenant_name, ip_list))) {
        LOG_WARN("set ip list failed", K(ret), K(cluster_name), K(tenant_name), K(ip_list));
      }
    } else if (params->stmt_type_ == OBPROXY_T_DELETE) {
      if (OB_FAIL(get_global_white_list_table_processor().delete_ip_list(cluster_name, tenant_name))) {
        LOG_WARN("delete ip list failed", K(ret), K(cluster_name), K(tenant_name));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt type", K(ret), K(params->stmt_type_));
    }
  }

  return ret;
}

int ObWhiteListTableProcessor::commit(bool is_success)
{
  if (is_success) {
    get_global_white_list_table_processor().inc_index();
    if (OB_UNLIKELY(IS_DEBUG_ENABLED())) {
      get_global_white_list_table_processor().print_config();
    }
  } else {
    LOG_WARN("white list commit failed");
  }

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
  handler.execute_func_ = &ObWhiteListTableProcessor::execute;
  handler.commit_func_ = &ObWhiteListTableProcessor::commit;
  handler.config_type_ = OBPROXY_CONFIG_CLOUD;
  if (OB_FAIL(addr_hash_map_array_[0].create(32, ObModIds::OB_HASH_BUCKET))) {
    LOG_WARN("create hash map failed", K(ret));
  } else if (OB_FAIL(addr_hash_map_array_[1].create(32, ObModIds::OB_HASH_BUCKET))) {
    LOG_WARN("create hash map failed", K(ret));
  } else if (OB_FAIL(get_global_config_processor().register_callback("white_list", handler))) {
    LOG_WARN("register white_list table callback failed", K(ret));
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

int ObWhiteListTableProcessor::set_ip_list(ObString &cluster_name, ObString &tenant_name, ObString &ip_list)
{
  int ret = OB_SUCCESS;

  if (cluster_name.empty() || tenant_name.empty() || ip_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is unexpected", K(ret), K(cluster_name), K(tenant_name), K(ip_list));
  } else if (OB_FAIL(backup_hash_map())) {
    LOG_WARN("backup hash map failed", K(ret));
  } else {
    ObSEArray<AddrStruct, 4> addr_array;
    char *start = ip_list.ptr();
    int pos = 0;
    AddrStruct ip_addr;
    memset(&ip_addr, 0, sizeof(AddrStruct));
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
        if (ip_addr.ip_ == 0) {
          if (OB_FAIL(ip_to_int(buf, ip_addr.ip_))) {
            LOG_WARN("ip to int failed", K(ret));
            break;
          }
        } else {
          ip_addr.net_ = atoi(buf);
          if (ip_addr.net_ <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("atoi failed", K(ret));
            break;
          }
        }

        if (ip_list[pos] != '/') {
          if (OB_FAIL(addr_array.push_back(ip_addr))) {
            LOG_WARN("ip addr push back failed", K(ret));
          } else {
            memset(&ip_addr, 0, sizeof(AddrStruct));
          }
        }
        pos++;
        start = ip_list.ptr() + pos;
      } else if (ip_list[pos] >= '0' || ip_list[pos] <= '9' || ip_list[pos] == '.') {
        pos++;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid char in ip_list", K(ret), K(ip_list));
      }
    }
    if (OB_SUCC(ret)) {
      ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> key_string;
      if (OB_FAIL(paste_tenant_and_cluster_name(tenant_name, cluster_name, key_string))) {
        LOG_WARN("paster tenant and cluser name failed", K(ret), K(tenant_name), K(cluster_name));
      } else {
        DRWLock::WRLockGuard guard(white_list_lock_);
        WhiteListHashMap &backup_map = addr_hash_map_array_[(index_ + 1) % 2];
        if (OB_FAIL(backup_map.set_refactored(key_string, addr_array, 1))) {
          LOG_WARN("addr hash map set failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObWhiteListTableProcessor::delete_ip_list(ObString &cluster_name, ObString &tenant_name)
{
  int ret = OB_SUCCESS;
  if (cluster_name.empty() || tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cluster name or tenant name empty unexpected", K(ret));
  } else if (OB_FAIL(backup_hash_map())) {
    LOG_WARN("backup hash map failed", K(ret));
  } else {
    LOG_DEBUG("delete ip list", K(cluster_name), K(tenant_name));
    ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> key_string;
    if (OB_FAIL(paste_tenant_and_cluster_name(tenant_name, cluster_name, key_string))) {
      LOG_WARN("paste tenant and cluster name failed", K(ret), K(tenant_name), K(cluster_name));
    } else {
      DRWLock::WRLockGuard guard(white_list_lock_);
      WhiteListHashMap &backup_map = addr_hash_map_array_[(index_ + 1) % 2];
      if (OB_FAIL(backup_map.erase_refactored(key_string))) {
        LOG_WARN("addr hash map erase failed", K(ret));
      }
    }
  }

  return ret;
}

bool ObWhiteListTableProcessor::can_ip_pass(ObString &cluster_name, ObString &tenant_name, ObString &user_name, char* ip)
{
  uint32_t ip_value = 0;
  int ret = OB_SUCCESS;
  bool can_pass = false;
  if (OB_FAIL(ip_to_int(ip, ip_value))) {
    LOG_WARN("ip to in failed", K(ret));
  } else {
    can_pass = can_ip_pass(cluster_name, tenant_name, user_name, ip_value, false);
  }
  return can_pass;
}

bool ObWhiteListTableProcessor::can_ip_pass(ObString &cluster_name, ObString &tenant_name,
                                            ObString &user_name, uint32_t src_ip, bool is_big_endian)
{
  int ret = OB_SUCCESS;
  bool can_pass = false;
  uint32_t ip = is_big_endian ? to_little_endian(src_ip) : src_ip;
  LOG_DEBUG("check can ip pass", K(cluster_name), K(tenant_name), K(ip));
  if (!cluster_name.empty() && !tenant_name.empty()) {
    ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> key_string;
    if (OB_FAIL(paste_tenant_and_cluster_name(tenant_name, cluster_name, key_string))) {
      LOG_WARN("paste tenant and cluster name failed", K(ret), K(tenant_name), K(cluster_name));
    } else {
      ObSEArray<AddrStruct, 4> ip_array;
      DRWLock::RDLockGuard guard(white_list_lock_);
      WhiteListHashMap &current_map = addr_hash_map_array_[index_];
      if (OB_SUCC(current_map.get_refactored(key_string, ip_array))) {
        for (int64_t i = 0; !can_pass && i < ip_array.count(); i++) {
          AddrStruct addr = ip_array.at(i);
          if (0 == addr.ip_) {
            can_pass = true;
          } else if (0 == addr.net_) {
            can_pass = (addr.ip_ == ip);
          } else {
            uint32_t mask = 0xffffffff;
            can_pass = (addr.ip_ == (ip & (mask << (32 - addr.net_))));
          }
          if (OB_UNLIKELY(IS_DEBUG_ENABLED())) {
            ObIpEndpoint client_info;
            ObIpEndpoint white_list_info;
            client_info.sin_.sin_addr.s_addr = __bswap_32(ip);
            client_info.sin_.sin_family = AF_INET;
            white_list_info.sin_.sin_addr.s_addr = __bswap_32(addr.ip_);
            white_list_info.sin_.sin_family = AF_INET;
            LOG_DEBUG("ip check can pass", K(client_info), K(white_list_info), "mask", addr.net_, K(can_pass));
          }
        }
      } else if (OB_HASH_NOT_EXIST == ret) {
        can_pass = true;
      } else {
        LOG_WARN("unexpected error", K(ret));
      }
    }
  } else {
    LOG_WARN("cluster_name or tenant_name is empty", K(cluster_name), K(tenant_name), K(ip));
  }

  if (!can_pass) {
    struct sockaddr_in address;
    address.sin_addr.s_addr = __bswap_32(ip);
    LOG_WARN("can not pass white_list", K(cluster_name), K(tenant_name), K(user_name), "client_ip", inet_ntoa(address.sin_addr));
  }

  return can_pass;
}

int64_t AddrStruct::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  in_addr addr;
  addr.s_addr = static_cast<in_addr_t>(ip_);
  J_OBJ_START();
  J_KV("ip", inet_ntoa(addr), K_(net));
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
    LOG_WARN("backup map failed", K(ret));
  } else {
    WhiteListHashMap::iterator iter = current_map.begin();
    for(; OB_SUCC(ret) && iter != current_map.end(); ++iter) {
      if (OB_FAIL(backup_map.set_refactored(iter->first, iter->second))) {
        LOG_WARN("backup map set refactored failed", K(ret));
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

void ObWhiteListTableProcessor::print_config()
{
  DRWLock::RDLockGuard guard(white_list_lock_);
  WhiteListHashMap &current_map = addr_hash_map_array_[index_];
  WhiteListHashMap::iterator iter = current_map.begin();
  for(; iter != current_map.end(); ++iter) {
    LOG_DEBUG("white list map info", K(iter->first));
    for (int64_t i = 0; i < iter->second.count(); i++) {
      AddrStruct addr_info = iter->second.at(i);
      struct sockaddr_in address;
      address.sin_addr.s_addr = __bswap_32(addr_info.ip_);
      LOG_DEBUG("ip/net info", "ip", inet_ntoa(address.sin_addr), "mask", addr_info.net_);
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
