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

#ifndef OBPROXY_CONGESTION_MANAGER_H
#define OBPROXY_CONGESTION_MANAGER_H

#include "lib/lock/ob_spin_rwlock.h"
#include "lib/container/ob_se_array.h"
#include "utils/ob_ref_hash_map.h"
#include "iocore/eventsystem/ob_event_system.h"
#include "iocore/net/ob_inet.h"
#include "stat/ob_congestion_stats.h"
#include "obutils/ob_mt_hashtable.h"
#include "obutils/ob_congestion_entry.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

/**
 * ObCongestionManager is implemented in a Multithread-Safe hash table
 */

class ObCongestionControlConfig;
struct ObCongestionEntry;

typedef ObMTHashTable<net::ObIpEndpoint, ObCongestionEntry *> CongestionTable;
typedef ObHashTableIteratorState<net::ObIpEndpoint, ObCongestionEntry *> Iter;

/**
 * ObCongestRequestParam is the data structure passed to the request
 * to update the congestion db with the appropriate info
 * It is used when the TS missed a try_lock, the request info will be
 * stored in the ObCongestRequestParam and insert in the to-do list of the
 * approperiate DB partition.
 * The first operation after the TS get the lock for a partition is
 * to run the to do list
 */
struct ObCongestRequestParam
{
  enum Op
  {
    ADD_RECORD,
    REMOVE_RECORD,
    REMOVE_ALL_RECORDS,
    REVALIDATE_BUCKET_CONFIG,
    REVALIDATE_BUCKET_ZONE,
    REVALIDATE_SERVER,
    DELETE_SERVER,
  };

  ObCongestRequestParam()
      : hash_(0),  cr_version_(-1), op_(REVALIDATE_BUCKET_CONFIG), server_state_(ObCongestionEntry::ACTIVE),
        entry_(NULL), zone_state_(NULL), config_(NULL)
  { }

  ~ObCongestRequestParam() { }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(hash), K_(key), K_(op), K_(server_state));
    if (NULL != entry_) {
      J_KV(", entry_", *entry_);
    }
    if (NULL != zone_state_) {
      J_KV(", zone_state_", *zone_state_);
    }
    if (NULL != zone_state_) {
      J_KV(", zone_state_", *zone_state_);
    }
    if (NULL != config_) {
      J_KV(", config_", *entry_);
    }
    J_OBJ_END();
    return pos;
  }

  uint64_t hash_;
  net::ObIpEndpoint key_;
  int64_t cr_version_;
  Op op_;
  ObCongestionEntry::ObServerState server_state_;
  ObCongestionEntry *entry_;
  ObCongestionZoneState *zone_state_;
  ObCongestionControlConfig *config_;

  LINK(ObCongestRequestParam, link_);
};

class ObCongestionManager : public CongestionTable
{
public:
  static const int64_t CONGESTION_MAP_SIZE = 1024;

  ObCongestionManager();
  virtual ~ObCongestionManager();

  void destroy();

  int init(const int64_t table_size = CONGESTION_MAP_SIZE / MT_HASHTABLE_PARTITIONS);
  int update_congestion_config(ObCongestionControlConfig *config, const bool is_init = false);
  int update_zone(const common::ObString &zone_name,
                  const common::ObString &region_name,
                  const ObCongestionZoneState::ObZoneState state,
                  const bool is_init = false);
  int update_server(const net::ObIpEndpoint &ip,
                    const int64_t cr_version,
                    const ObCongestionEntry::ObServerState state,
                    const common::ObString &zone_name,
                    const common::ObString &region_name,
                    const bool is_init = false);

  int get_congest_entry(event::ObContinuation *cont, const net::ObIpEndpoint &ip, const int64_t cr_version,
                        ObCongestionEntry **ppentry, event::ObAction *&action);
  int get_congest_list(event::ObContinuation *cont, event::ObMIOBuffer *buffer, event::ObAction *&action);

  int revalidate_config(ObCongestionControlConfig *config);
  int revalidate_zone(ObCongestionZoneState *zone_state);
  int revalidate_server(const net::ObIpEndpoint &ip,
                        const int64_t cr_version,
                        const ObCongestionEntry::ObServerState server_state,
                        const common::ObString &zone_name,
                        const common::ObString &region_name);

  int add_new_server(const net::ObIpEndpoint &ip,
                     const int64_t cr_version,
                     const ObCongestionEntry::ObServerState state,
                     const common::ObString &zone_name,
                     const common::ObString &region_name,
                     const bool is_init = false);
  // add an entry to the manager
  int add_record(const net::ObIpEndpoint &ip, ObCongestionEntry *entry,
                 const bool skip_try_lock = false);

  // remove an entry from the manager
  int remove_record(const net::ObIpEndpoint &ip);

  int remove_all_records(void);
  int run_todo_list(const int64_t buck_id);

  ObCongestionControlConfig *get_congestion_control_config();
  ObCongestionZoneState *get_zone_state(const common::ObString &zone_name);
  bool is_base_servers_added() const { return is_base_servers_added_; }
  void set_base_servers_added() { is_base_servers_added_ = true; }
  bool is_congestion_avail();
  int update_tc_congestion_map(ObCongestionEntry &entry);
  DECLARE_TO_STRING;

private:
  int add_record_todo_list(const uint64_t hash, const net::ObIpEndpoint &ip,
                           ObCongestionEntry *entry);
  int process(const int64_t buck_id, ObCongestRequestParam *param);
  int revalidate_bucket_config(const int64_t buck_id, ObCongestionControlConfig *config);
  int revalidate_bucket_zone(const int64_t buck_id, ObCongestionZoneState *zone_state);

private:
  bool is_inited_;
  bool is_base_servers_added_;
  bool is_congestion_enabled_;
  int64_t zone_count_;
  ObCongestionZoneState *zones_[common::MAX_ZONE_NUM];

  common::ObAtomicList todo_lists_[MT_HASHTABLE_PARTITIONS];
  common::SpinRWLock rw_lock_; // use to protect zone's array

  ObCongestionControlConfig *config_;

  DISALLOW_COPY_AND_ASSIGN(ObCongestionManager);
};

inline bool ObCongestionManager::is_congestion_avail()
{
  return (is_base_servers_added() && is_congestion_enabled_);
}

int init_congestion_control();


//------------------------------thread cache congestion map---------------------//
struct ObTcCongestionEntryKey
{
public:
  ObTcCongestionEntryKey() : cr_version_(-1), ip_() {}
  ObTcCongestionEntryKey(const int64_t cr_version, const net::ObIpEndpoint ip)
  {
    cr_version_ = cr_version;
    ip_ = ip;
  }

  uint64_t hash() const;
  bool operator==(const ObTcCongestionEntryKey &other) const;
  bool operator!=(const ObTcCongestionEntryKey &other) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

  int64_t cr_version_;
  net::ObIpEndpoint ip_;
};

inline uint64_t ObTcCongestionEntryKey::hash() const
{
  return common::murmurhash(&cr_version_, sizeof(cr_version_), ip_.hash());
}


inline bool ObTcCongestionEntryKey::operator==(const ObTcCongestionEntryKey &other) const
{
  return ((cr_version_ == other.cr_version_) && (ip_ == other.ip_));
}

inline bool ObTcCongestionEntryKey::operator!=(const ObTcCongestionEntryKey &other) const
{
  return !(*this == other);
}

template<class K, class V>
struct ObGetCongestionEntryKey
{
  ObTcCongestionEntryKey operator() (const ObCongestionEntry *cgt_entry) const
  {
    ObTcCongestionEntryKey key(cgt_entry->cr_version_, cgt_entry->server_ip_);
    return key;
  }
};

static const int64_t CONGESTION_ENTRY_HASH_MAP_SIZE = 16 * 1024; // 16KB
typedef ObRefHashMap<ObTcCongestionEntryKey, ObCongestionEntry *, ObGetCongestionEntryKey, CONGESTION_ENTRY_HASH_MAP_SIZE> ObCongestionHashMap;

class ObCongestionRefHashMap : public ObCongestionHashMap
{
public:
  ObCongestionRefHashMap(const common::ObModIds::ObModIdEnum mod_id) : ObCongestionHashMap(mod_id) {}
  virtual ~ObCongestionRefHashMap() {}
  int clean_hash_map();

private:
  DISALLOW_COPY_AND_ASSIGN(ObCongestionRefHashMap);
};

int init_congestion_map_for_thread();
int init_congestion_map_for_one_thread(int64_t index);

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_CONGESTION_MANAGER_H
