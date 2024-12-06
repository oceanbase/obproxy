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

#ifndef OBPROXY_SERVICE_NAME_SESSION_INFO_H
#define OBPROXY_SERVICE_NAME_SESSION_INFO_H

#include "lib/hash/ob_build_in_hashmap.h"
#include "iocore/net/ob_inet.h"
#include "lib/hash/ob_dynamic_build_in_hashmap.h"
#include "lib/container/ob_se_array.h"
#include "share/config/ob_config_helper.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObTenantInfo {
  common::ObConfigVariableString tenant_name_;
  common::ObConfigVariableString cluster_name_;
  int64_t to_string(char *buf, const int64_t buf_len) const;
};

// 存储对应的cursor id，找到对应的tenant name信息
class ObServiceNameCursorInfo
{
public:
  ObServiceNameCursorInfo(): cursor_tenant_info_(), cursor_id_(0) {}
  ObServiceNameCursorInfo(uint32_t client_cursor_id,
                          const common::ObString &tenant_name,
                          const common::ObString &cluster_name): cursor_id_(client_cursor_id)
  { set_cursor_tenant(tenant_name, cluster_name); }
  ~ObServiceNameCursorInfo() {};
  void destroy();
  ObTenantInfo& get_cursor_tenant() { return cursor_tenant_info_; }
  int set_cursor_tenant(const common::ObString &tenant_name, const common::ObString &cluster_name);
  void set_cursor_id(uint32_t client_cursor_id) { cursor_id_ = client_cursor_id; }

  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  ObTenantInfo cursor_tenant_info_;
  uint32_t cursor_id_;

  LINK(ObServiceNameCursorInfo, service_name_cursor_info_link_);
};

// cursor id ----> ObServiceNameCursorInfo
struct ObServiceNameCursorInfoHashing
{
  typedef const uint32_t &Key;
  typedef ObServiceNameCursorInfo Value;
  typedef ObDLList(ObServiceNameCursorInfo, service_name_cursor_info_link_) ListHead;

  static uint64_t hash(Key key) { return common::murmurhash(&key, sizeof(key), 0); }
  static Key key(Value const *value) { return value->cursor_id_; }
  static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
};

// 存储ps_id对应的tenant 信息
class ObServiceNamePsInfo
{
public:
  ObServiceNamePsInfo(): ps_tenant_info_(), ps_id_(0) {}
  ObServiceNamePsInfo(uint32_t client_ps_id,
                      const common::ObString &tenant_name,
                      const common::ObString &cluster_name): ps_id_(client_ps_id)
  { add_ps_tenant(tenant_name, cluster_name); }
  ~ObServiceNamePsInfo() {};
  typedef common::ObSEArray<ObTenantInfo, 4> ObTenantNameArray;

  void destroy();
  ObTenantNameArray& get_ps_tenant_array() { return ps_tenant_info_; }
  int add_ps_tenant(const common::ObString &tenant_name, const common::ObString &cluster_name);

  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  ObTenantNameArray ps_tenant_info_;
  uint32_t ps_id_;

  LINK(ObServiceNamePsInfo, service_name_ps_info_link_);
};
// ps id ----> ObServiceNamePsInfo
struct ObServiceNamePsInfoHashing
{
  typedef const uint32_t &Key;
  typedef ObServiceNamePsInfo Value;
  typedef ObDLList(ObServiceNamePsInfo, service_name_ps_info_link_) ListHead;

  static uint64_t hash(Key key) { return common::murmurhash(&key, sizeof(key), 0); }
  static Key key(Value const *value) { return value->ps_id_; }
  static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
};

typedef common::hash::ObDynamicBuildInHashMap<ObServiceNameCursorInfoHashing> ObServiceNameCursorInfoMap;
typedef common::hash::ObDynamicBuildInHashMap<ObServiceNamePsInfoHashing> ObServiceNamePsInfoMap;

// stored in client session info
struct ObServiceaNameSessionInfo
{
  ObServiceaNameSessionInfo(): cursor_info_map_(), ps_info_map_() {}
public:
  int add_cursor_id_tenant_info(uint32_t client_cursor_id, const common::ObString &tenant_name, const common::ObString &cluster_name);
  int add_ps_id_tenant_info(uint32_t client_ps_id, const common::ObString &tenant_name, const common::ObString &cluster_name);
  ObServiceNameCursorInfo *get_cursor_id_tenant_info(uint32_t client_cursor_id) const;
  ObServiceNamePsInfo *get_ps_id_tenant_info(uint32_t client_ps_id) const;
  void remove_cursor_info(uint32_t client_ps_id);
  void remove_ps_info(uint32_t client_ps_id);
  void destroy();

public:
  ObServiceNameCursorInfoMap cursor_info_map_;
  ObServiceNamePsInfoMap ps_info_map_;
};
// 检查:
// 1. 创建Hash表
// 2. 删除Hash表
// 3. 删除表中，对应的cursor id、ps id

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_CURSOR_STRUCT_H
