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

#ifndef OBPROXY_TABLEGROUP_STRUCT_H
#define OBPROXY_TABLEGROUP_STRUCT_H

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
#define TABLEGROUP_ENTRY_LOOKUP_CACHE_DONE     (TABLEGROUP_ENTRY_EVENT_EVENTS_START + 1)
#define TABLEGROUP_ENTRY_LOOKUP_START_EVENT    (TABLEGROUP_ENTRY_EVENT_EVENTS_START + 2)
#define TABLEGROUP_ENTRY_LOOKUP_CACHE_EVENT    (TABLEGROUP_ENTRY_EVENT_EVENTS_START + 3)
#define TABLEGROUP_ENTRY_LOOKUP_REMOTE_EVENT    (TABLEGROUP_ENTRY_EVENT_EVENTS_START + 4)
#define TABLEGROUP_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT     (TABLEGROUP_ENTRY_EVENT_EVENTS_START + 5)

struct ObTableGroupEntryKey
{
public:
  ObTableGroupEntryKey() : tenant_id_(OB_INVALID_TENANT_ID), tablegroup_name_(), database_name_(), cr_version_(-1), cr_id_(common::OB_INVALID_CLUSTER_ID) {}
  ObTableGroupEntryKey(int64_t tenant_id, const ObString &tablegroup_name, const ObString &database_name, int64_t cr_version, int64_t cr_id)
    : tenant_id_(tenant_id), tablegroup_name_(tablegroup_name), database_name_(database_name),
      cr_version_(cr_version), cr_id_(cr_id) {}
  ~ObTableGroupEntryKey() { cr_version_ = -1; }

  bool is_valid() const;
  uint64_t hash(const uint64_t seed = 0) const;
  bool operator==(const ObTableGroupEntryKey &other) const;
  bool operator!=(const ObTableGroupEntryKey &other) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();

  int64_t tenant_id_;
  common::ObString tablegroup_name_;
  common::ObString database_name_;
  int64_t cr_version_;
  int64_t cr_id_;
};

inline void ObTableGroupEntryKey::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tablegroup_name_.reset();
  database_name_.reset();
  cr_version_ = -1;
  cr_id_ = common::OB_INVALID_CLUSTER_ID;
}

inline bool ObTableGroupEntryKey::is_valid() const
{
  return (OB_INVALID_TENANT_ID != tenant_id_
          && !tablegroup_name_.empty()
          && !database_name_.empty()
          && (cr_version_ >= 0)
          && (cr_id_ >= 0));
}

inline uint64_t ObTableGroupEntryKey::hash(const uint64_t seed) const
{
  uint64_t hashs = common::murmurhash(&tenant_id_, sizeof(tenant_id_), seed);
  hashs = tablegroup_name_.hash(hashs);
  hashs = database_name_.hash(hashs);
  hashs = common::murmurhash(&cr_version_, sizeof(cr_version_), hashs);
  hashs = common::murmurhash(&cr_id_, sizeof(cr_id_), hashs);
  return hashs;
}

inline bool ObTableGroupEntryKey::operator==(const ObTableGroupEntryKey &other) const
{
  return (tenant_id_ == other.tenant_id_ && tablegroup_name_ == other.tablegroup_name_ 
         && database_name_ == other.database_name_ && cr_version_ == other.cr_version_ && cr_id_ == other.cr_id_);
}

inline bool ObTableGroupEntryKey::operator!=(const ObTableGroupEntryKey &other) const
{
  return !(*this == other);
}

// Used to store table name. It is different from ObString because ObString requires additional memory allocation. Here, it is directly allocated in the structure in advance to facilitate management.
struct ObTableGroupTableNameInfo
{
  ObTableGroupTableNameInfo() : table_name_length_(0) { table_name_[0] = '\0'; }
  ~ObTableGroupTableNameInfo() {}
  void reset() {
    table_name_length_ = 0;
    table_name_[0] = '\0';
  }

  TO_STRING_KV(K_(table_name_length), K_(table_name));

  char table_name_[OB_MAX_TABLE_NAME_LENGTH];
  int64_t table_name_length_;
};

class ObTableGroupEntry : public ObRouteEntry
{
public:
  static const int64_t OB_TABLEGROUP_MAX_SHARDING_LENGTH = 16;
public:
  ObTableGroupEntry()
    : ObRouteEntry(), is_inited_(false), tenant_id_(0),
      tablegroup_name_(), database_name_(), sharding_(),
      table_names_(), buf_(NULL), buf_len_(0) {}
  virtual ~ObTableGroupEntry() {}
  virtual void free();
  int64_t to_string(char *buf, const int64_t buf_len) const;

  int init(const int64_t tenant_id, const ObString &tablegroup_name,
           const ObString &database_name, const ObString &sharding,
           const ObIArray<ObTableGroupTableNameInfo> &table_names,
           char *buf, const int64_t buf_len);
  static int alloc_and_init_tablegroup_entry(const int64_t tenant_id, const ObString &tablegroup_name,
                                             const ObString &database_name, const ObString &sharding,
                                             const ObIArray<ObTableGroupTableNameInfo> &table_names,
                                             const int64_t cr_version, const int64_t cr_id, ObTableGroupEntry *&entry);

  bool is_valid() const;
  void get_key(ObTableGroupEntryKey &key) const;
  bool is_the_same_entry(const ObTableGroupEntry &entry) const;
  int64_t get_tenant_id() const { return tenant_id_; }
  const ObString &get_tablegroup_name() const { return tablegroup_name_; }
  const ObString &get_database_name() const { return database_name_; }
  const ObString &get_sharding() const { return sharding_; }
  const ObIArray<ObString> &get_table_names() const { return table_names_; }

private:
  bool is_inited_;
  int64_t tenant_id_;
  common::ObString tablegroup_name_;
  common::ObString database_name_;
  common::ObString sharding_;
  common::ObSEArray<common::ObString, 4> table_names_;
  char *buf_;
  int64_t buf_len_;
};

inline bool ObTableGroupEntry::is_valid() const
{
  return (OB_INVALID_TENANT_ID != tenant_id_
          && !tablegroup_name_.empty()
          && !database_name_.empty()
          && !sharding_.empty()
          && !table_names_.empty()
          && cr_version_ > 0
          && cr_id_ >= 0);
}

inline bool ObTableGroupEntry::is_the_same_entry(const ObTableGroupEntry &entry) const
{
  bool bret = false;
  if (is_valid() && entry.is_valid()) {
    if (tablegroup_name_ != entry.tablegroup_name_) {
    // tablegroup type
    } else if (database_name_ != entry.database_name_) {
    } else if (tenant_id_ != entry.tenant_id_) {
    // table id
    } else if (sharding_ != entry.sharding_ ) {
    // TODO add table info cmp
    } else if (table_names_.count() != entry.table_names_.count()) {
    } else {
      bret = true;
      for (int i = 0; i < table_names_.count(); ++i) {
        if (table_names_.at(i) != entry.table_names_.at(i)) {
          bret = false;
        }
      }
    }
  }
  return bret;
}


inline void ObTableGroupEntry::get_key(ObTableGroupEntryKey &key) const
{
  key.tenant_id_ = tenant_id_;
  key.tablegroup_name_ = tablegroup_name_;
  key.database_name_ = database_name_;
  key.cr_version_ = get_cr_version();
  key.cr_id_ = get_cr_id();
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_ROUTE_STRUCT_H
