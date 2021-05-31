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
#include "obutils/ob_vip_tenant_cache.h"

using namespace obsys;
using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

void ObVipAddr::set(const int32_t ip, const int32_t port, const int64_t vid)
{
  if (OB_UNLIKELY(!addr_.set_ipv4_addr(ip, port))) {
    LOG_WARN("fail to set_ipv4_addr", K(ip), K(port), K(addr_));
    addr_.reset();
  } else {
    vid_ = vid;
  }
}

int ObVipTenant::set_tenant_cluster(const ObString &tenant_name, const ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  if (tenant_name.empty() || tenant_name.length() > OB_MAX_TENANT_NAME_LENGTH
      || cluster_name.empty() || cluster_name.length() > OB_PROXY_MAX_CLUSTER_NAME_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("name invalid", K(tenant_name), K(tenant_name), K(ret));
  } else {
    MEMCPY(tenant_name_str_, tenant_name.ptr(), tenant_name.length());
    tenant_name_.assign_ptr(tenant_name_str_, tenant_name.length());
    MEMCPY(cluster_name_str_, cluster_name.ptr(), cluster_name.length());
    cluster_name_.assign_ptr(cluster_name_str_, cluster_name.length());
  }
  return ret;
}

int ObVipTenant::set_request_target_type(const int64_t request_target_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(request_target_type > static_cast<int64_t>(RequestFollower)
      || request_target_type < static_cast<int64_t>(InvalidRequestType))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid request target type", K(request_target_type));
  } else {
    request_target_type_ = static_cast<ObVipTenantRequestType>(request_target_type);
  }

  return ret;
}

int ObVipTenant::set_rw_type(const int64_t rw_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rw_type > static_cast<int64_t>(ReadWrite)
      || rw_type < static_cast<int64_t>(InvalidRWType))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rw type", K(rw_type));
  } else {
    rw_type_ = static_cast<ObVipTenantRWType>(rw_type);
  }

  return ret;
}

int ObVipTenant::set(const ObVipAddr &vip_addr, const common::ObString &tenant_name,
    const common::ObString &cluster_name, const ObVipTenantRequestType request_target_type,
    const ObVipTenantRWType rw_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_tenant_cluster(tenant_name, cluster_name))) {
    LOG_WARN("fail to set tenant cluster name", K(tenant_name), K(tenant_name), K(ret));
  } else {
    vip_addr_ = vip_addr;
    request_target_type_ = request_target_type;
    rw_type_ = rw_type;
  }
  return ret;
}

void ObVipTenant::reset()
{
  vip_addr_.reset();
  tenant_name_.reset();
  cluster_name_.reset();
  request_target_type_ = InvalidRequestType;
  rw_type_ = InvalidRWType;
}

void ObVipTenantCache::destroy()
{
  CWLockGuard guard(rwlock_);
  vt_cache_map_ = NULL;
  clear_cache_map(vt_cache_map_array_[0]);
  clear_cache_map(vt_cache_map_array_[1]);
}

int64_t ObVipTenantCache::get_vt_cache_count() const
{
  int64_t ret = -1;
  CRLockGuard guard(rwlock_);
  if (OB_LIKELY(NULL != vt_cache_map_)) {
    ret = vt_cache_map_->count();
  }
  return ret;
}

int ObVipTenantCache::get(const ObVipAddr &vip_addr, ObVipTenant &vt)
{
  int ret = OB_SUCCESS;
  ObVipTenant *tmp_vt = NULL;
  if (OB_UNLIKELY(!vip_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_INFO("invalid input value", K(vip_addr), K(ret));
  } else {
    CRLockGuard guard(rwlock_);
    if (OB_ISNULL(vt_cache_map_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vt_cache_map_ should not be null", K(ret));
    } else if (OB_SUCCESS != vt_cache_map_->get_refactored(vip_addr, tmp_vt)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("tenant is not in vip_tenant_cache", K(ret));
    } else if (OB_ISNULL(tmp_vt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vt get from vt_cache_map is null", K(ret));
    } else if (OB_FAIL(vt.set(tmp_vt->vip_addr_, tmp_vt->tenant_name_,
        tmp_vt->cluster_name_, tmp_vt->request_target_type_, tmp_vt->rw_type_))) {
      LOG_WARN("fail to set tenant cluster for ObVipTenant", K(*tmp_vt), K(ret));
    } else {
      LOG_DEBUG("tenant hit in vip_tenant_cache", K(vip_addr), K(*tmp_vt));
    }
  }

  return ret;
}

int ObVipTenantCache::set(ObVipTenant &vt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!vt.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(vt), K(ret));
  } else {
    CWLockGuard guard(rwlock_);
    if (OB_ISNULL(vt_cache_map_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vt_cache_map_ should not be null", K(ret));
    } else {
      ret = vt_cache_map_->unique_set(&vt);
      if (OB_LIKELY(OB_SUCCESS == ret)) {
        LOG_DEBUG("succ to insert vip_tenant into vt_cache_map");
      } else if (OB_HASH_EXIST == ret) {
        LOG_DEBUG("vip_tenant alreay exist in the cache map");
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to insert vip_tenant into vt_cache_map", K(ret));
      }
    }
  }
  return ret;
}

int ObVipTenantCache::update_cache_map()
{
  int ret = OB_SUCCESS;
  CWLockGuard guard(rwlock_);
  if (OB_ISNULL(vt_cache_map_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vt_cache_map_ should not be null", K(ret));
  } else {
    clear_cache_map(*vt_cache_map_);
    if (&vt_cache_map_array_[0] != vt_cache_map_) {
      vt_cache_map_ = &vt_cache_map_array_[0];
    } else {
      vt_cache_map_ = &vt_cache_map_array_[1];
    }
  }
  return ret;
}

void ObVipTenantCache::clear_cache_map(VTHashMap &cache_map)
{
  VTHashMap::iterator last = cache_map.end();
  for (VTHashMap::iterator it = cache_map.begin(); it != last; ++it) {
    it->destroy();
  }
  cache_map.reset();
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

