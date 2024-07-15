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

#ifndef OBPROXY_INDEX_STRUCT_H
#define OBPROXY_INDEX_STRUCT_H

#include "lib/ob_define.h"
// #include "lib/net/ob_addr.h"
#include "lib/string/ob_string.h"
#include "lib/ptr/ob_ptr.h"
#include "lib/time/ob_hrtime.h"
// #include "common/ob_role.h"
// #include "share/inner_table/ob_inner_table_schema_constants.h"
#include "iocore/eventsystem/ob_thread.h"
#include "obutils/ob_proxy_config.h"
#include "stat/ob_processor_stats.h"
// #include "rpc/obmysql/ob_mysql_util.h"
#include "proxy/route/ob_route_struct.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
#define INDEX_ENTRY_LOOKUP_CACHE_DONE     (INDEX_ENTRY_EVENT_EVENTS_START + 1)
#define INDEX_ENTRY_LOOKUP_START_EVENT    (INDEX_ENTRY_EVENT_EVENTS_START + 2)
#define INDEX_ENTRY_LOOKUP_CACHE_EVENT    (INDEX_ENTRY_EVENT_EVENTS_START + 3)

struct ObIndexEntryKey;
struct ObIndexEntryName
{
public:
  ObIndexEntryName() : cluster_name_(), tenant_name_(), database_name_(), table_name_(), index_name_() {}
  ~ObIndexEntryName() {}
  bool is_valid() const;
  int64_t get_total_str_len() const;
  uint64_t hash(const uint64_t seed = 0) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool operator==(const ObIndexEntryName &other) const;
  bool operator!=(const ObIndexEntryName &other) const;
  void reset();
  int deep_copy(const ObIndexEntryName &name, char *buf, const int64_t buf_len);
  void shallow_copy(const ObIndexEntryName &name);
  void shallow_copy(const common::ObString &cluster_name,
    const common::ObString &tenant_name, const common::ObString &database_name,
    const common::ObString &index_name, const common::ObString &table_name);

  common::ObString cluster_name_;
  common::ObString tenant_name_;
  common::ObString database_name_;
  common::ObString table_name_;
  common::ObString index_name_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIndexEntryName);
};

class ObIndexEntry : public ObRouteEntry
{
public:
  ObIndexEntry()
    : ObRouteEntry(), is_inited_(false), index_type_(0), table_id_(0),
      data_table_id_(0), name_(), buf_len_(0), buf_start_(NULL)
  {
    index_table_name_buf_[0] = '\0';
  }
  virtual ~ObIndexEntry() {}
  virtual void free();
  int64_t to_string(char *buf, const int64_t buf_len) const;

  static int alloc_and_init_index_entry(const ObIndexEntryName &name, const int64_t cr_version,
                                        const int64_t cr_id, ObIndexEntry *&entry);

  bool is_valid() const;
  int init(char *buf_start, const int64_t buf_len);
  int generate_index_table_name();

  int set_names(const ObIndexEntryName &name);
  void set_index_type(const int64_t index_type) { index_type_ = index_type; }
  void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  void set_data_table_id(const uint64_t data_table_id) { data_table_id_ = data_table_id; }
  bool is_the_same_entry(const ObIndexEntry &entry) const;

  const ObIndexEntryName &get_names() const { return name_; }
  int64_t get_index_type() const { return index_type_; }
  uint64_t get_table_id() const { return table_id_; }
  uint64_t get_data_table_id() const { return data_table_id_; }
  void get_key(ObIndexEntryKey &key) const;
  ObString get_index_table_name() const { return index_table_name_; }

private:
  bool is_inited_;
  int64_t  index_type_;
  uint64_t table_id_;
  uint64_t data_table_id_;
  ObIndexEntryName name_;
  ObString index_table_name_;
  char index_table_name_buf_[OB_MAX_INDEX_TABLE_NAME_LENGTH];
  int64_t buf_len_;
  char *buf_start_;
};

inline void ObIndexEntryName::shallow_copy(const ObIndexEntryName &name)
{
  reset();
  cluster_name_ = name.cluster_name_;
  tenant_name_ = name.tenant_name_;
  database_name_ = name.database_name_;
  table_name_ = name.table_name_;
  index_name_ = name.index_name_;
}

inline void ObIndexEntryName::shallow_copy(const common::ObString &cluster_name,
    const common::ObString &tenant_name, const common::ObString &database_name,
    const common::ObString &index_name, const common::ObString &table_name)
{
  reset();
  cluster_name_ = cluster_name;
  tenant_name_ = tenant_name;
  database_name_ = database_name;
  index_name_ = index_name;
  table_name_ = table_name;
}

inline void ObIndexEntryName::reset()
{
  cluster_name_.reset();
  tenant_name_.reset();
  database_name_.reset();
  table_name_.reset();
  index_name_.reset();
}

inline int64_t ObIndexEntryName::get_total_str_len() const
{
  return (cluster_name_.length()
          + tenant_name_.length()
          + database_name_.length()
          + index_name_.length()
          + table_name_.length());
}

inline bool ObIndexEntryName::operator==(const ObIndexEntryName &other) const
{
  return ((cluster_name_ == other.cluster_name_)
          && (tenant_name_ == other.tenant_name_)
          && (database_name_ == other.database_name_)
          && (index_name_ == other.index_name_)
          && (table_name_ == other.table_name_));
}

inline bool ObIndexEntryName::operator!=(const ObIndexEntryName &other) const
{
  return !(*this == other);
}

inline bool ObIndexEntryName::is_valid() const
{
  return ((!cluster_name_.empty())
          && (!tenant_name_.empty())
          && (!database_name_.empty())
          && (!index_name_.empty())
          && (!table_name_.empty()));
}

inline uint64_t ObIndexEntryName::hash(const uint64_t seed) const
{
  return ((cluster_name_.hash(seed)
          + tenant_name_.hash(seed)
          + database_name_.hash(seed)
          + index_name_.hash(seed)
          + table_name_.hash(seed)));
}

inline bool ObIndexEntry::is_the_same_entry(const ObIndexEntry &entry) const
{
  bool bret = false;
  if (is_valid() && entry.is_valid()) {
    // IndexEntryName Key
    if (name_ != entry.name_) {
    // index type
    } else if (index_type_ != entry.index_type_) {
    // table id
    } else if (table_id_ != entry.table_id_) {
    // data table id
    } else if (data_table_id_ != entry.data_table_id_) {
    // equal
    } else {
      bret = true;
    }
  }
  return bret;
}

struct ObIndexEntryKey
{
public:
  ObIndexEntryKey() : name_(NULL), cr_version_(-1), cr_id_(common::OB_INVALID_CLUSTER_ID) {}
  ObIndexEntryKey(const ObIndexEntryName &name, int64_t cr_version, int64_t cr_id)
    : name_(&name), cr_version_(cr_version), cr_id_(cr_id) {}
  ~ObIndexEntryKey() { name_ = NULL; cr_version_ = -1; }

  bool is_valid() const;
  uint64_t hash(const uint64_t seed = 0) const;
  bool operator==(const ObIndexEntryKey &other) const;
  bool operator!=(const ObIndexEntryKey &other) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();

  const ObIndexEntryName *name_;
  int64_t cr_version_;
  int64_t cr_id_;
};

inline void ObIndexEntry::get_key(ObIndexEntryKey &key) const
{
  key.name_ = &get_names();
  key.cr_version_ = get_cr_version();
  key.cr_id_ = get_cr_id();
}

inline bool ObIndexEntry::is_valid() const
{
  return (common::OB_INVALID_ID != table_id_
          && common::OB_INVALID_ID != data_table_id_
          && (0 != index_type_));
}

inline void ObIndexEntryKey::reset()
{
  name_ = NULL;
  cr_version_ = -1;
  cr_id_ = common::OB_INVALID_CLUSTER_ID;
}

inline bool ObIndexEntryKey::is_valid() const
{
  return ((NULL == name_) ? (false) : (name_->is_valid() && cr_version_ >= 0 && cr_id_ >= 0));
}

inline uint64_t ObIndexEntryKey::hash(const uint64_t seed) const
{
  uint64_t hashs = NULL == name_ ? 0 : name_->hash(seed);
  hashs = common::murmurhash(&cr_version_, sizeof(cr_version_), hashs);
  hashs = common::murmurhash(&cr_id_, sizeof(cr_id_), hashs);
  return hashs;
}

inline bool ObIndexEntryKey::operator==(const ObIndexEntryKey &other) const
{
  return ((NULL == name_ || NULL == other.name_) ?
             (false) : (*name_ == *other.name_ && cr_version_ == other.cr_version_ && cr_id_ == other.cr_id_));
}

inline bool ObIndexEntryKey::operator!=(const ObIndexEntryKey &other) const
{
  return !(*this == other);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_ROUTE_STRUCT_H
