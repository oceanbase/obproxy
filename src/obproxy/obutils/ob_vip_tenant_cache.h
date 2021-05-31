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

#ifndef OBPROXY_VIP_TENANT_CACHE_H
#define OBPROXY_VIP_TENANT_CACHE_H
#include "lib/net/ob_addr.h"
#include "lib/string/ob_string.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "utils/ob_proxy_lib.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

struct ObVipAddr
{
public:
  ObVipAddr() : addr_(), vid_(-1) { }
  ~ObVipAddr() { reset(); }

  bool is_valid() const { return (addr_.is_valid() && vid_ >= 0); }
  void reset() { vid_ = -1; addr_.reset(); }
  bool operator==(const ObVipAddr &vip_addr) const { return (vip_addr.vid_ == vid_ && vip_addr.addr_ == addr_); }
  void set(const int32_t ip, const int32_t port, const int64_t vid);
  TO_STRING_KV(K_(addr), K_(vid));

public:
  common::ObAddr addr_;
  int64_t vid_;
};

struct ObVipTenant
{
private:
  enum ObVipTenantRequestType
  {
    InvalidRequestType = -1,
    RequestLeader,
    RequestFollower,
  };
  enum ObVipTenantRWType
  {
    InvalidRWType = -1,
    ReadOnly,
    ReadWrite,
  };

public:
  ObVipTenant() : vip_addr_(), tenant_name_(), cluster_name_(),
            request_target_type_(InvalidRequestType), rw_type_(InvalidRWType)
  {
    tenant_name_str_[0] = '\0';
    cluster_name_str_[0] = '\0';
    memset(&vt_link_, 0, sizeof(vt_link_));
  }
  ~ObVipTenant() { reset(); }
  void destroy() { op_free(this); }
  int set_tenant_cluster(const common::ObString &tname, const common::ObString &cluster_name);
  int set(const ObVipAddr &vip_addr, const common::ObString &tname,
        const common::ObString &cluster_name, const ObVipTenantRequestType request_target_type,
        const ObVipTenantRWType rw_type);
  int set_request_target_type(int64_t request_target_type);
  int set_rw_type(int64_t rw_type);
  bool is_request_follower() const { return request_target_type_ == RequestFollower; }
  bool is_read_only() const { return rw_type_ == ReadOnly; }

  void reset();
  bool is_valid() const { return (vip_addr_.is_valid() && !tenant_name_.empty() && !cluster_name_.empty()); }
  TO_STRING_KV(K_(vip_addr), K_(tenant_name), K_(cluster_name),
               K_(request_target_type), K_(rw_type));

public:
  ObVipAddr vip_addr_;
  common::ObString tenant_name_;
  common::ObString cluster_name_;
  ObVipTenantRequestType request_target_type_;
  ObVipTenantRWType rw_type_;

  LINK(ObVipTenant, vt_link_);

private:
  char tenant_name_str_[common::OB_MAX_TENANT_NAME_LENGTH];
  char cluster_name_str_[OB_PROXY_MAX_CLUSTER_NAME_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObVipTenant);
};

class ObVipTenantCache
{
public:
  ObVipTenantCache() : rwlock_(), vt_cache_map_(&vt_cache_map_array_[0]) { }
  ~ObVipTenantCache() { }

  void destroy();

public:
  static const int64_t HASH_BUCKET_SIZE = 64;

  struct VTCacheHashing
  {
    typedef const ObVipAddr &Key;
    typedef ObVipTenant Value;
    typedef ObDLList(ObVipTenant, vt_link_) ListHead;

    static uint64_t hash(Key key) { return common::murmurhash(&key, sizeof(common::ObAddr), 0); }
    static Key key(Value *value) { return value->vip_addr_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<VTCacheHashing, HASH_BUCKET_SIZE> VTHashMap;

public:
  int get(const ObVipAddr &vip_addr, ObVipTenant &vt);
  int set(ObVipTenant &vt);
  VTHashMap &get_cache_map_tmp() { return (&vt_cache_map_array_[0] != vt_cache_map_ ? vt_cache_map_array_[0] : vt_cache_map_array_[1]); };
  VTHashMap *get_cache_map() { return vt_cache_map_; };
  int64_t get_vt_cache_count() const;
  int update_cache_map();
  static void clear_cache_map(VTHashMap &cache_map);

  mutable obsys::CRWLock rwlock_;
private:
  VTHashMap *vt_cache_map_;
  VTHashMap vt_cache_map_array_[2];
  DISALLOW_COPY_AND_ASSIGN(ObVipTenantCache);
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_VIP_TENANT_CACHE_H */
