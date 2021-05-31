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

#ifndef OBPROXY_SAFE_SNAPSHOT_MANAGER_H
#define OBPROXY_SAFE_SNAPSHOT_MANAGER_H

#include "obutils/ob_safe_snapshot_entry.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObSafeSnapshotManager
{
public:
  static const int64_t HASH_BUF_SIZE = 64 * sizeof(ObSafeSnapshotEntry);

  ObSafeSnapshotManager();
  ~ObSafeSnapshotManager() {}

  //// this function is non thread safe, it should be called in single thread
  //int update_entrys(const common::ObIArray<ObServerStateInfo> &servers_state);
  // lock free get
  ObSafeSnapshotEntry *get(const common::ObAddr &addr) const;
  int add(const common::ObAddr &addr);

  // do not free memory AS:
  // 1. the server count is small
  // 2. destory will be rarely call
  // we may implement it in future version
  void destory() {}

  DECLARE_TO_STRING;
private:
  int err_code_map(const int err) const;
  //uint64_t get_server_hash(const common::ObIArray<ObServerStateInfo> &servers_state);

  char buf_[HASH_BUF_SIZE];
  ObAddrChainHash entry_map_;
  int64_t next_priority_;
  //uint64_t server_hash_;
};

inline int ObSafeSnapshotManager::err_code_map(const int err) const
{
  int ret = common::OB_ERR_UNEXPECTED;
  switch (err) {
    case 0:
      ret = common::OB_SUCCESS;
      break;
    case -ENOENT:
      ret = common::OB_ENTRY_NOT_EXIST;
      break;
    case -EEXIST:
      ret = common::OB_ENTRY_EXIST;
      break;
    case -ENOMEM:
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      break;
    case -EOVERFLOW:
      ret = common::OB_SIZE_OVERFLOW;
      break;
    default:
      ret = common::OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

inline ObSafeSnapshotEntry *ObSafeSnapshotManager::get(const common::ObAddr &addr) const
{
  ObAddrChainHash::node_t *ret_entry = NULL;
  int err = entry_map_.get(addr, ret_entry);
  if (err != 0) {
    ret_entry = NULL;
  }
  return static_cast<ObSafeSnapshotEntry *>(ret_entry);
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_SAFE_SNAPSHOT_MANAGER_H
