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

#ifndef OB_VIP_TENANT_CONN_H
#define OB_VIP_TENANT_CONN_H

#include "lib/net/ob_addr.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "utils/ob_proxy_lib.h"
#include "iocore/eventsystem/ob_buf_allocator.h"

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

class ObVipTenantConn {
public:
  ObVipTenantConn() : cluster_name_(), tenant_name_(), vip_name_(),
    full_name_(), max_connections_(0)
  {
    tenant_name_str_[0] = '\0';
    cluster_name_str_[0] = '\0';
    vip_name_str_[0] = '\0';
    full_name_str_[0] = '\0';
    memset(&vt_link_, 0, sizeof(vt_link_));
  }
  virtual ~ObVipTenantConn() { reset(); };
  void destroy() { op_free(this); }
  void reset();

  int set_tenant_cluster(const common::ObString &tenant_name, const common::ObString &cluster_name);
  int set_addr(const common::ObString addr);
  int set_full_name();
  int set(const common::ObString &cluster_name, const common::ObString &tenant_name,
          const common::ObString &vip_name, uint64_t max_connections);
  bool is_valid() const { return (!tenant_name_.empty() && !cluster_name_.empty()); }
  TO_STRING_KV(K_(cluster_name), K_(tenant_name), K_(vip_name), K_(full_name),
               K_(max_connections));

public:
  bool is_inited_;
  common::ObString cluster_name_;
  common::ObString tenant_name_;
  common::ObString vip_name_;
  common::ObString full_name_;
  uint64_t max_connections_;

  LINK(ObVipTenantConn, vt_link_);

private:
  char tenant_name_str_[common::OB_MAX_TENANT_NAME_LENGTH];
  char cluster_name_str_[OB_PROXY_MAX_CLUSTER_NAME_LENGTH];
  char vip_name_str_[common::MAX_IP_ADDR_LENGTH];
  char full_name_str_[OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + common::MAX_IP_ADDR_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObVipTenantConn);
};

class ObVipTenantConnCache
{
public:
  ObVipTenantConnCache() : vt_conn_map_(&vt_conn_map_array[0]) { }
  virtual ~ObVipTenantConnCache() { }

  void destroy();

public:
  static const int64_t HASH_BUCKET_SIZE = 64;

  struct VTCacheHashing
  {
    typedef const common::ObString &Key;
    typedef ObVipTenantConn Value;
    typedef ObDLList(ObVipTenantConn, vt_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->full_name_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<VTCacheHashing, HASH_BUCKET_SIZE> VTHashMap;

public:

  int set(ObVipTenantConn* vt);
  int get(common::ObString& key_name, ObVipTenantConn*& vt_conn);
  int erase(common::ObString& cluster_name, common::ObString& tenant_name);
  int backup();
  int recover();
  int64_t get_conn_map_count() const;
  void dump_conn_map();
  VTHashMap* get_conn_map() { return vt_conn_map_; };
  VTHashMap& get_conn_map_replica() { return (&vt_conn_map_array[0] != vt_conn_map_ ? vt_conn_map_array[0] : vt_conn_map_array[1]); };
  static void clear_conn_map(VTHashMap &cache_map);
  static void dump_conn_map(VTHashMap &cache_map);

  //mutable obsys::CRWLock rwlock_;
private:
  VTHashMap* vt_conn_map_;  // Always point to the latest configuration
  VTHashMap  vt_conn_map_array[2];
  DISALLOW_COPY_AND_ASSIGN(ObVipTenantConnCache);
};

class ObUsedConn : public common::ObSharedRefCount {
public:
  ObUsedConn(common::ObString& full_name) : max_used_connections_(0), is_in_map_(false) {
    if (full_name.length() < OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + common::MAX_IP_ADDR_LENGTH) {
      MEMCPY(full_name_str_, full_name.ptr(), full_name.length());
      full_name_.assign_ptr(full_name_str_, (int32_t)full_name.length());
    }
    memset(&used_conn_link_, 0, sizeof(used_conn_link_));
  }
  virtual ~ObUsedConn() { reset(); };
  virtual void free() { destroy(); }
  void destroy() {
    int64_t total_len = sizeof(ObUsedConn);
    op_fixed_mem_free(this, total_len);
  }
  void reset() {
    full_name_.reset();
    max_used_connections_ = 0;
    is_in_map_ = false;
  }
  TO_STRING_KV(K_(full_name), K_(max_used_connections), K_(is_in_map));
  LINK(ObUsedConn, used_conn_link_);

public:
  common::ObString full_name_;
  volatile int64_t max_used_connections_;
  bool is_in_map_;

private:
  char full_name_str_[OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + common::MAX_IP_ADDR_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObUsedConn);
};

class ObUsedConnCache
{
public:
  ObUsedConnCache() : used_conn_map_() { }
  virtual ~ObUsedConnCache() { }

public:
  static const int64_t HASH_BUCKET_SIZE = 64;

  struct UsedConnCacheHashing
  {
    typedef const common::ObString &Key;
    typedef ObUsedConn Value;
    typedef ObDLList(ObUsedConn, used_conn_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->full_name_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<UsedConnCacheHashing, HASH_BUCKET_SIZE> UsedConnHashMap;

public:
  int set(ObUsedConn* used_conn);
  int get(common::ObString& key_name, ObUsedConn*& used_conn);
  int erase(common::ObString& key_name);

private:
  UsedConnHashMap used_conn_map_;
  DISALLOW_COPY_AND_ASSIGN(ObUsedConnCache);
};

}  // namespace omt
}  // namespace obproxy
}  // namespace oceanbase

#endif  // OB_VIP_TENANT_CONN_H
