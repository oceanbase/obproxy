/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SAFE_SNAPSHOT_ENTRY_H
#define OBPROXY_SAFE_SNAPSHOT_ENTRY_H

#include "lib/net/ob_addr.h"
#include "lib/hash/ob_fixed_hash.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
typedef common::FixedHash<common::ObAddr> ObAddrChainHash;
// FixedHash is a lock free hash map, but we should manager memory usage by ourselves
class ObSafeSnapshotEntry : public ObAddrChainHash::node_t
{
public:
  static const int64_t NEED_FORCE_SYNC_MASK = (1L << 63);
  static const int64_t SAFE_READ_SNAPSHOT_MASK = ~(1L << 63);

  ObSafeSnapshotEntry();
  virtual ~ObSafeSnapshotEntry() {}

  static inline int64_t get_safe_read_snapshot(const int64_t v) { return SAFE_READ_SNAPSHOT_MASK & v; }
  static inline bool get_need_force_sync(const int64_t v) { return (NEED_FORCE_SYNC_MASK & v) != 0; }

  void set_addr(const common::ObAddr &addr) { key_ = addr; }
  void update_safe_read_snapshot(const int64_t &snapshot, const bool need_force_sync);
  void set_priority(const int64_t &priority) { priority_ = priority; }

  common::ObAddr get_addr() const { return key_; }
  int64_t get_safe_read_snapshot() const { return get_safe_read_snapshot(value_); }
  bool need_force_sync() const { return get_need_force_sync(value_); }
  int64_t get_priority() const { return priority_; }
  DECLARE_VIRTUAL_TO_STRING;
private:
  int64_t value_;
  int64_t priority_; // the lower the value, the higher the priority
};


} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_SAFE_SNAPSHOT_ENTRY_H
