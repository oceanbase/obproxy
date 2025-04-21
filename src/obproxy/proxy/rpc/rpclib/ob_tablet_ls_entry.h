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

#ifndef OBPROXY_TABLET_LS_STRUCT_H
#define OBPROXY_TABLET_LS_STRUCT_H

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/ptr/ob_ptr.h"
#include "lib/time/ob_hrtime.h"
#include "iocore/eventsystem/ob_thread.h"
#include "obutils/ob_proxy_config.h"
#include "stat/ob_processor_stats.h"
#include "proxy/route/ob_route_struct.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
#define TABLET_LS_ENTRY_LOOKUP_CACHE_DONE     (TABLET_LS_ENTRY_EVENT_EVENTS_START + 1)
#define TABLET_LS_ENTRY_LOOKUP_START_EVENT    (TABLET_LS_ENTRY_EVENT_EVENTS_START + 2)
#define TABLET_LS_ENTRY_LOOKUP_CACHE_EVENT    (TABLET_LS_ENTRY_EVENT_EVENTS_START + 3)
#define TABLET_LS_ENTRY_LOOKUP_REMOTE_EVENT    (TABLET_LS_ENTRY_EVENT_EVENTS_START + 4)
#define TABLET_LS_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT     (TABLET_LS_ENTRY_EVENT_EVENTS_START + 5)

#define OB_TABLET_TO_LS_MAP_BUCKET_SIZE 16
typedef common::hash::ObHashMap<int64_t, int64_t> OB_TABLET_TO_LS_MAP;

struct ObTabletLsEntryKey
{
public:
  ObTabletLsEntryKey() : tenant_id_(OB_INVALID_TENANT_ID), table_id_(0), cr_version_(-1), cr_id_(common::OB_INVALID_CLUSTER_ID) {}
  ObTabletLsEntryKey(int64_t tenant_id, int64_t table_id, int64_t cr_version, int64_t cr_id)
    : tenant_id_(tenant_id), table_id_(table_id),
      cr_version_(cr_version), cr_id_(cr_id) {}
  ~ObTabletLsEntryKey() { cr_version_ = -1; }

  bool is_valid() const;
  uint64_t hash(const uint64_t seed = 0) const;
  bool operator==(const ObTabletLsEntryKey &other) const;
  bool operator!=(const ObTabletLsEntryKey &other) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();

  int64_t tenant_id_;
  int64_t table_id_;
  int64_t cr_version_;
  int64_t cr_id_;
};

inline void ObTabletLsEntryKey::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  table_id_ = 0;
  cr_version_ = -1;
  cr_id_ = common::OB_INVALID_CLUSTER_ID;
}

inline bool ObTabletLsEntryKey::is_valid() const
{
  return (OB_INVALID_TENANT_ID != tenant_id_
          && 0 != table_id_
          && (cr_version_ >= 0)
          && (cr_id_ >= 0));
}

inline uint64_t ObTabletLsEntryKey::hash(const uint64_t seed) const
{
  uint64_t hashs = common::murmurhash(&tenant_id_, sizeof(tenant_id_), seed);
  hashs = common::murmurhash(&table_id_, sizeof(table_id_), hashs);
  hashs = common::murmurhash(&cr_version_, sizeof(cr_version_), hashs);
  hashs = common::murmurhash(&cr_id_, sizeof(cr_id_), hashs);
  return hashs;
}

inline bool ObTabletLsEntryKey::operator==(const ObTabletLsEntryKey &other) const
{
  return (tenant_id_ == other.tenant_id_ && table_id_ == other.table_id_
    && cr_version_ == other.cr_version_ && cr_id_ == other.cr_id_);
}

inline bool ObTabletLsEntryKey::operator!=(const ObTabletLsEntryKey &other) const
{
  return !(*this == other);
}

class ObTabletLsEntry : public ObRouteEntry
{
public:
  // static const int64_t OB_TABLET_LS_MAX_SHARDING_LENGTH = 16;
public:
  ObTabletLsEntry()
    : ObRouteEntry(), is_inited_(false), tenant_id_(0),
      table_id_(), tablet_to_ls_map_(), buf_(NULL), buf_len_(0)
  {
    tablet_to_ls_map_.create(OB_TABLET_TO_LS_MAP_BUCKET_SIZE, ObModIds::ObModIds::OB_PROXY_TABLET_LS_ID_MAP);
  }
  virtual ~ObTabletLsEntry() {
    tablet_to_ls_map_.destroy();
  }
  virtual void free();
  int64_t to_string(char *buf, const int64_t buf_len) const;

  int init(const int64_t tenant_id, const int64_t table_id, OB_TABLET_TO_LS_MAP &tablet_ls_map,
           char *buf, const int64_t buf_len);
  static int alloc_and_init_tablet_ls_entry(const int64_t tenant_id, const int64_t table_id, OB_TABLET_TO_LS_MAP &tablet_ls_map,
                                            const int64_t cr_version, const int64_t cr_id, ObTabletLsEntry *&entry);

  bool is_valid() const;
  void get_key(ObTabletLsEntryKey &key) const;
  bool is_the_same_entry(const ObTabletLsEntry &entry) const;
  int64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_table_id() const { return table_id_; }
  OB_TABLET_TO_LS_MAP &get_tablet_ls_map() { return tablet_to_ls_map_; }
  const OB_TABLET_TO_LS_MAP &get_tablet_ls_map_const() const { return tablet_to_ls_map_; }
  bool is_same_tablet_ls_map(const OB_TABLET_TO_LS_MAP &new_map) const;

private:
  bool is_inited_;
  int64_t tenant_id_;
  int64_t table_id_;
  // common::ObSEArray<common::ObString, 4> table_names_;
  OB_TABLET_TO_LS_MAP tablet_to_ls_map_; //to build it
  char *buf_;
  int64_t buf_len_;
};

inline bool ObTabletLsEntry::is_valid() const
{
  return (OB_INVALID_TENANT_ID != tenant_id_
          && 0 != table_id_
          && cr_version_ > 0
          && cr_id_ >= 0);
}

inline bool ObTabletLsEntry::is_the_same_entry(const ObTabletLsEntry &entry) const
{
  bool bret = false;
  const OB_TABLET_TO_LS_MAP &new_map = entry.get_tablet_ls_map_const();
  if (is_valid() && entry.is_valid()) {
    if (tenant_id_ != entry.tenant_id_) {
    // table id
    } else if (table_id_ != entry.table_id_) {
      //not the same table_id
    } else if (!is_same_tablet_ls_map(new_map)) {

    } else {
      bret = true;
    }
  }
  return bret;
}

inline void ObTabletLsEntry::get_key(ObTabletLsEntryKey &key) const
{
  key.tenant_id_ = tenant_id_;
  key.table_id_ = table_id_;
  key.cr_version_ = get_cr_version();
  key.cr_id_ = get_cr_id();
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_ROUTE_STRUCT_H
