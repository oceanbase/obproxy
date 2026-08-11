/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY
#include "obproxy/obutils/ob_safe_snapshot_entry.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

ObSafeSnapshotEntry::ObSafeSnapshotEntry() : node_t()
                                           , value_(0)
                                           , priority_(-1)
{
}

int64_t ObSafeSnapshotEntry::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("addr", key_,
       "safe_read_snapshot", get_safe_read_snapshot(value_),
       "need_force_sync", get_need_force_sync(value_),
       K_(priority));
  J_OBJ_END();
  return pos;
}

void ObSafeSnapshotEntry::update_safe_read_snapshot(const int64_t &snapshot,
                                                    const bool need_force_sync)
{
  int64_t old_value = ATOMIC_LOAD(&value_);
  int64_t new_value = (snapshot | (need_force_sync ? NEED_FORCE_SYNC_MASK : 0));
  while (get_safe_read_snapshot(old_value) < snapshot) {
    old_value = ATOMIC_VCAS(&value_, old_value, new_value);
  }
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase