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

#ifndef OBPROXY_PARTITION_ENTRY_H
#define OBPROXY_PARTITION_ENTRY_H
#include "lib/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/string/ob_string.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "iocore/eventsystem/ob_thread.h"
#include "iocore/eventsystem/ob_continuation.h"
#include "proxy/route/ob_route_struct.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
#define PARTITION_ENTRY_LOOKUP_CACHE_DONE     (PARTITION_ENTRY_EVENT_EVENTS_START + 1)
#define PARTITION_ENTRY_LOOKUP_START_EVENT    (PARTITION_ENTRY_EVENT_EVENTS_START + 2)
#define PARTITION_ENTRY_LOOKUP_CACHE_EVENT    (PARTITION_ENTRY_EVENT_EVENTS_START + 3)
#define PARTITION_ENTRY_LOOKUP_REMOTE_EVENT   (PARTITION_ENTRY_EVENT_EVENTS_START + 4)
#define PARTITION_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT   (PARTITION_ENTRY_EVENT_EVENTS_START + 5)

struct ObPartitionEntryKey
{
public:
  ObPartitionEntryKey()
    : cr_version_(-1), cr_id_(common::OB_INVALID_CLUSTER_ID), table_id_(common::OB_INVALID_ID), partition_id_(common::OB_INVALID_ID) {}
  explicit ObPartitionEntryKey(const int64_t cr_version,
                               const int64_t cr_id,
                               const uint64_t table_id,
                               const uint64_t partition_id)
    : cr_version_(cr_version), cr_id_(cr_id), table_id_(table_id), partition_id_(partition_id) {}
  ~ObPartitionEntryKey() {}

  bool is_valid() const;
  uint64_t hash(uint64_t seed = 0) const;
  bool operator==(const ObPartitionEntryKey &other) const;
  bool operator!=(const ObPartitionEntryKey &other) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();

  int64_t cr_version_;
  int64_t cr_id_;
  uint64_t table_id_;
  uint64_t partition_id_;
};

inline void ObPartitionEntryKey::reset()
{
  cr_version_ = -1;
  cr_id_ = common::OB_INVALID_CLUSTER_ID;
  table_id_ = common::OB_INVALID_ID;
  partition_id_ = common::OB_INVALID_ID;
}

inline bool ObPartitionEntryKey::is_valid() const
{
  return (common::OB_INVALID_ID != table_id_
          && common::OB_INVALID_ID != partition_id_
          && (cr_version_ >= 0)
          && (cr_id_ <= 0));
}

inline uint64_t ObPartitionEntryKey::hash(uint64_t seed) const
{
  uint64_t hashs = common::murmurhash(&cr_version_, sizeof(cr_version_), seed);
  hashs = common::murmurhash(&cr_id_, sizeof(cr_id_), hashs);
  hashs = common::murmurhash(&table_id_, sizeof(table_id_), hashs);
  hashs = common::murmurhash(&partition_id_, sizeof(partition_id_), hashs);
  return hashs;
}

inline bool ObPartitionEntryKey::operator==(const ObPartitionEntryKey &other) const
{
  return ((cr_version_ == other.cr_version_)
          && (cr_id_ == other.cr_id_)
          && (table_id_ == other.table_id_)
          && (partition_id_ == other.partition_id_));
}

inline bool ObPartitionEntryKey::operator!=(const ObPartitionEntryKey &other) const
{
  return !(*this == other);
}

class ObPartitionEntry : public ObRouteEntry
{
public:
  ObPartitionEntry()
    : ObRouteEntry(), table_id_(common::OB_INVALID_ID),
      partition_id_(common::OB_INVALID_ID), pl_()
  {
  }

  virtual ~ObPartitionEntry() {} // must be empty
  virtual void free();

  static int alloc_and_init_partition_entry(const uint64_t table_id, const uint64_t partition_id,
                                            const int64_t cr_version,
                                            const int64_t cr_id,
                                            const common::ObIArray<ObProxyReplicaLocation> &replicas,
                                            ObPartitionEntry *&entry);
  static int alloc_and_init_partition_entry(const ObPartitionEntryKey &key,
                                            const common::ObIArray<ObProxyReplicaLocation> &replicas,
                                            ObPartitionEntry *&entry);
  static int alloc_and_init_partition_entry(const ObPartitionEntryKey &key,
                                            const ObProxyReplicaLocation &replica,
                                            ObPartitionEntry *&entry);

  void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  void set_partition_id(const uint64_t partition_id) { partition_id_ = partition_id; }
  int set_pl(const common::ObIArray<ObProxyReplicaLocation> &replicas) { return pl_.set_replicas(replicas); }

  ObPartitionEntryKey get_key() const;
  uint64_t get_partition_id() const { return partition_id_; }
  const ObProxyPartitionLocation &get_pl() const { return pl_; }
  const ObProxyReplicaLocation *get_leader_replica() const;
  uint64_t get_table_id() const { return table_id_; }

  int64_t get_server_count() const { return pl_.replica_count(); }
  uint64_t get_all_server_hash() const {return pl_.get_all_server_hash(); }
  bool is_leader_server_equal(const ObPartitionEntry &entry) const;

  bool is_valid() const;
  bool need_update_entry() const;
  bool is_the_same_entry(const ObPartitionEntry &entry) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  uint64_t table_id_;
  uint64_t partition_id_;
  ObProxyPartitionLocation pl_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionEntry);
};

inline bool ObPartitionEntry::is_valid() const
{
  return (common::OB_INVALID_ID != table_id_
          && common::OB_INVALID_ID != partition_id_
          && cr_version_ > 0
          && cr_id_ >= 0
          && pl_.is_valid());
}

inline bool ObPartitionEntry::is_leader_server_equal(const ObPartitionEntry &entry) const
{
  const ObProxyReplicaLocation *leader = get_leader_replica();
  const ObProxyReplicaLocation *entry_leader = entry.get_leader_replica();
  return (((NULL == leader) && (NULL == entry_leader))
          || ((NULL != leader) && (NULL != entry_leader) && (*leader == *entry_leader)));
}

inline bool ObPartitionEntry::is_the_same_entry(const ObPartitionEntry &entry) const
{
  bool bret = false;
  if (is_valid() && entry.is_valid()) {
    // PartitionEntry Key
    if (get_key() != entry.get_key()) {
    // server count
    } else if (get_server_count() != entry.get_server_count()) {
    // leader
    } else if (!is_leader_server_equal(entry)) {
    // all server
    } else if (get_all_server_hash() != entry.get_all_server_hash()) {
    // equal
    } else {
      bret = true;
    }
  }
  return bret;
}

inline const ObProxyReplicaLocation *ObPartitionEntry::get_leader_replica() const
{
  return pl_.get_leader();
}

inline bool ObPartitionEntry::need_update_entry() const
{
  return (OB_LIKELY(is_valid()) && is_avail_state());
}

inline ObPartitionEntryKey ObPartitionEntry::get_key() const
{
  ObPartitionEntryKey key(cr_version_, cr_id_, table_id_, partition_id_);
  return key;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_PARTITION_ENTRY_H */
