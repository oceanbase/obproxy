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

#include "ob_vip_tenant_conn.h"
#include "lib/oblog/ob_log.h"
#include "omt/ob_conn_table_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

using namespace obsys;
using namespace oceanbase::common;

extern int build_tenant_cluster_vip_name(const ObString &tenant_name, const ObString &cluster_name, const ObString &vip_name,
    ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + MAX_IP_ADDR_LENGTH> &key_string);

int ObVipTenantConn::set_tenant_cluster(const ObString &tenant_name, const ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  if (tenant_name.empty() || tenant_name.length() > OB_MAX_TENANT_NAME_LENGTH
      || cluster_name.empty() || cluster_name.length() > OB_PROXY_MAX_CLUSTER_NAME_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("name invalid", K(tenant_name), K(cluster_name), K(ret));
  } else {
    MEMCPY(tenant_name_str_, tenant_name.ptr(), tenant_name.length());
    tenant_name_.assign_ptr(tenant_name_str_, tenant_name.length());
    MEMCPY(cluster_name_str_, cluster_name.ptr(), cluster_name.length());
    cluster_name_.assign_ptr(cluster_name_str_, cluster_name.length());
  }
  return ret;
}

int ObVipTenantConn::set_addr(const common::ObString addr)
{
  int ret = OB_SUCCESS;
  if (addr.empty() || addr.length() > MAX_IP_ADDR_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("name invalid", K(addr), K(ret));
  } else {
    MEMCPY(vip_name_str_, addr.ptr(), addr.length());
    vip_name_.assign_ptr(vip_name_str_, addr.length());
  }
  return ret;
}

int ObVipTenantConn::set_full_name()
{
  int ret = OB_SUCCESS;

  ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + MAX_IP_ADDR_LENGTH> full_name;
  if (OB_FAIL(build_tenant_cluster_vip_name(tenant_name_, cluster_name_, vip_name_, full_name))) {
    LOG_WARN("build tenant cluser and vip name failed", K(ret), K_(tenant_name), K_(cluster_name), K_(vip_name));
  } else {
    MEMCPY(full_name_str_, full_name.ptr(), full_name.size());
    full_name_.assign_ptr(full_name_str_, (int32_t)full_name.size());
  }

  return ret;
}

int ObVipTenantConn::set(const ObString &cluster_name,
    const ObString &tenant_name, const ObString &vip_name, uint64_t max_connections)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(set_tenant_cluster(tenant_name, cluster_name))) {
    LOG_WARN("fail to set tenant cluster name", K(tenant_name), K(cluster_name), K(ret));
  } else {
    if (!vip_name.empty()) {
      if (OB_FAIL(set_addr(vip_name))) {
        LOG_WARN("fail to set vip name", K(vip_name), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_full_name())) {
        LOG_WARN("fail to set full name", K(ret));
      } else {
        max_connections_ = max_connections;
      }
    }
  }

  return ret;
}

void ObVipTenantConn::reset()
{
  tenant_name_.reset();
  cluster_name_.reset();
  vip_name_.reset();
  full_name_.reset();
  max_connections_ = 0;
}

void ObVipTenantConnCache::destroy()
{
  //CWLockGuard guard(rwlock_);
  vt_conn_map_ = NULL;
  clear_conn_map(vt_conn_map_array[0]);
  clear_conn_map(vt_conn_map_array[1]);
}

int64_t ObVipTenantConnCache::get_conn_map_count() const
{
  int64_t ret = -1;
  //CRLockGuard guard(rwlock_);
  if (OB_LIKELY(NULL != vt_conn_map_)) {
    ret = vt_conn_map_->count();
  }

  return ret;
}

int ObVipTenantConnCache::get(ObString& key_name, ObVipTenantConn*& vt_conn)
{
  int ret = OB_SUCCESS;
  ObVipTenantConn *tmp_vt_conn = NULL;

  if (key_name.empty() || key_name.length() > OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + MAX_IP_ADDR_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("name invalid", K(key_name), K(ret));
  } else {
    //CRLockGuard guard(rwlock_);
    if (OB_ISNULL(vt_conn_map_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vt_conn_map_ should not be null", K(ret));
    } else if (OB_FAIL(vt_conn_map_->get_refactored(key_name, tmp_vt_conn))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_INFO("vip tenant connect is not in vip_tenant_conn_cache", K(key_name), K(ret));
    } else if (OB_ISNULL(tmp_vt_conn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vt get from vt_conn_map is null", K(key_name), K(ret));
    } else {
      LOG_DEBUG("vip tenant connect hit in vip_tenant_conn_cache", K(key_name), K(*tmp_vt_conn));
      vt_conn = tmp_vt_conn;
    }
  }

  return ret;
}

int ObVipTenantConnCache::set(ObVipTenantConn* vt)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!vt->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(vt), K(ret));
  } else {
    //CWLockGuard guard(rwlock_);
    if (OB_ISNULL(vt_conn_map_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vt_conn_map_ should not be null", K(ret));
    } else {
      ret = vt_conn_map_->unique_set(vt);
      if (OB_LIKELY(OB_SUCCESS == ret)) {
        LOG_DEBUG("succ to insert vip_tenant into vt_conn_map");
      } else if (OB_HASH_EXIST == ret) {
        LOG_DEBUG("vip_tenant alreay exist in the cache map");
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to insert vip_tenant into vt_conn_map", K(ret));
      }
    }
  }

  return ret;
}

int ObVipTenantConnCache::erase(ObString& cluster_name, ObString& tenant_name)
{
  int ret = OB_SUCCESS;

  if (cluster_name.empty() || tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(ret), K(cluster_name), K(tenant_name));
  } else {
    //CWLockGuard guard(rwlock_);
    VTHashMap::iterator last = vt_conn_map_->end();
    for (VTHashMap::iterator it = vt_conn_map_->begin(); it != last;) {
      if (it->cluster_name_ == cluster_name && it->tenant_name_ == tenant_name) {
        VTHashMap::iterator tmp = it;
        ++it;
        vt_conn_map_->erase_refactored(tmp->full_name_);
        tmp->destroy();
      } else {
        ++it;
      }
    }
  }

  return ret;
}

int ObVipTenantConnCache::backup()
{
  int ret = OB_SUCCESS;

  //CWLockGuard guard(rwlock_);
  // Initialization state:
  // vt_conn_map_ = vt_conn_map_array[0];
  // replica_vt_conn_map = vt_conn_map_array[1];
  VTHashMap& replica_vt_conn_map = get_conn_map_replica();
  clear_conn_map(replica_vt_conn_map);
  // Even if an error occurs in the backup process and an error is returned to the cloud platform, vt_conn_map_ is still the previous configuration information
  VTHashMap::iterator last = vt_conn_map_->end();
  for (VTHashMap::iterator it = vt_conn_map_->begin(); it != last; ++it) {
    ObVipTenantConn* tmp_vt_conn = NULL;
    if (OB_ISNULL(tmp_vt_conn = op_alloc(ObVipTenantConn))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for ObVipTenantConn", K(ret));
    } else if (OB_FAIL(tmp_vt_conn->set(
      it->cluster_name_, it->tenant_name_, it->vip_name_, it->max_connections_))) {
      LOG_WARN("fail to set vip tenant connect info", K(ret));
    } else if (OB_FAIL(replica_vt_conn_map.unique_set(tmp_vt_conn))) {
      LOG_WARN("insert vip tenant connection failed", K(ret));
      break;
    }
  }

  if (OB_FAIL(ret)) {
    clear_conn_map(replica_vt_conn_map);
  }

  return ret;
}

int ObVipTenantConnCache::recover()
{
  int ret = OB_SUCCESS;

  //CWLockGuard guard(rwlock_);
  if (OB_ISNULL(vt_conn_map_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vt_conn_map should not be null", K(ret));
  } else {
    clear_conn_map(*vt_conn_map_);
    if (&vt_conn_map_array[0] != vt_conn_map_) {
      vt_conn_map_ = &vt_conn_map_array[0];
    } else {
      vt_conn_map_ = &vt_conn_map_array[1];
    }
  }

  return ret;
}

void ObVipTenantConnCache::clear_conn_map(VTHashMap &cache_map)
{
  VTHashMap::iterator last = cache_map.end();
  VTHashMap::iterator tmp;
  for (VTHashMap::iterator it = cache_map.begin(); it != last;) {
    tmp = it;
    ++it;
    tmp->destroy();
  }
  cache_map.reset();
}

void ObVipTenantConnCache::dump_conn_map(VTHashMap &cache_map)
{
  VTHashMap::iterator last = cache_map.end();
  for (VTHashMap::iterator it = cache_map.begin(); it != last; ++it) {
    LOG_DEBUG("key: ", K(it->full_name_));
    LOG_DEBUG("value: ", K(it->max_connections_));
  }
}

int ObUsedConnCache::get(ObString& key_name, ObUsedConn*& used_conn)
{
  int ret = OB_SUCCESS;
  if (key_name.empty() || key_name.length() > OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + MAX_IP_ADDR_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("name invalid", K(key_name), K(ret));
  } else if (OB_FAIL(used_conn_map_.get_refactored(key_name, used_conn))) {
    LOG_DEBUG("fail to get used conn", K(key_name), K(ret));
  }
  return ret;
}

int ObUsedConnCache::set(ObUsedConn* used_conn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(used_conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("used conn is null", K(ret));
  } else if (OB_FAIL(used_conn_map_.unique_set(used_conn))) {
    LOG_WARN("fail to set used conn", K(used_conn), K(ret));
  }
  return ret;
}

int ObUsedConnCache::erase(ObString& key_name)
{
  int ret = OB_SUCCESS;
  if (key_name.empty() || key_name.length() > OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + MAX_IP_ADDR_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("name invalid", K(key_name), K(ret));
  } else if (OB_FAIL(used_conn_map_.erase_refactored(key_name))) {
    LOG_WARN("erase used conn failed", K(key_name), K(ret));
  }
  return ret;
}

} // end of namespace omt
} // end of namespace obproxy
} // end of namespace oceanbase
