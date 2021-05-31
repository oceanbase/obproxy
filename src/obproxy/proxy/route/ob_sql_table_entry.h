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

#ifndef OBPROXY_SQL_TABLE_ENTRY_H
#define OBPROXY_SQL_TABLE_ENTRY_H
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_string.h"
#include "iocore/eventsystem/ob_thread.h"
#include "lib/time/ob_hrtime.h"
#include "lib/list/ob_intrusive_list.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObSqlTableEntryKey
{
public:
  ObSqlTableEntryKey() : cr_version_(common::OB_INVALID_VERSION), cr_id_(common::OB_INVALID_CLUSTER_ID),
                         cluster_name_(), tenant_name_(), database_name_(), sql_id_() {}
  ~ObSqlTableEntryKey() { reset(); }

  bool is_valid() const;
  uint64_t hash(const uint64_t seed = 0) const;
  bool operator==(const ObSqlTableEntryKey &other) const;
  bool operator!=(const ObSqlTableEntryKey &other) const;
  void reset();
  
  TO_STRING_KV(K_(cr_version), K_(cr_id), K_(cluster_name),
               K_(tenant_name), K_(database_name), K_(sql_id));

public:
  int64_t cr_version_;
  int64_t cr_id_;
  common::ObString cluster_name_;
  common::ObString tenant_name_;
  common::ObString database_name_;
  common::ObString sql_id_;
};

inline bool ObSqlTableEntryKey::is_valid() const
{
  return !(common::OB_INVALID_VERSION == cr_version_
           || common::OB_INVALID_CLUSTER_ID == cr_id_
           || cluster_name_.empty()
           || tenant_name_.empty()
           || database_name_.empty()
           || sql_id_.empty());
}

inline uint64_t ObSqlTableEntryKey::hash(const uint64_t seed) const
{
  uint64_t ret = cluster_name_.hash(seed);
  ret = tenant_name_.hash(ret);
  ret = database_name_.hash(ret);
  ret = sql_id_.hash(ret);
  ret = common::murmurhash(&cr_version_, sizeof(cr_version_), ret);
  ret = common::murmurhash(&cr_id_, sizeof(cr_id_), ret);
  return ret;
}

inline bool ObSqlTableEntryKey::operator==(const ObSqlTableEntryKey &other) const
{
  return cr_version_ == other.cr_version_
         && cr_id_ == other.cr_id_
         && cluster_name_ == other.cluster_name_
         && tenant_name_ == other.tenant_name_
         && database_name_ == other.database_name_
         && sql_id_ == other.sql_id_;
}

inline bool ObSqlTableEntryKey::operator!=(const ObSqlTableEntryKey &other) const
{
  return !(*this == other);
}

inline void ObSqlTableEntryKey::reset()
{
  cr_version_ = common::OB_INVALID_VERSION;
  cr_id_ = common::OB_INVALID_CLUSTER_ID;
  cluster_name_.reset();
  tenant_name_.reset();
  database_name_.reset();
  sql_id_.reset();
}

class ObSqlTableEntry : public common::ObSharedRefCount
{
public:
  enum ObSqlTableEntryState
  {
    BORN = 0,
    AVAIL,
    DELETED
  };
  
  ObSqlTableEntry()
    : common::ObSharedRefCount(), state_(BORN), is_table_from_reroute_(false), table_name_(), key_(),
      buf_len_(0), buf_start_(NULL), create_time_us_(0),
      last_access_time_us_(0), last_update_time_us_(0) {}

  virtual ~ObSqlTableEntry() {}

  virtual void free() { destroy(); }
  void destroy();
  void set_avail_state() { state_ = AVAIL; }
  void set_deleted_state() { state_ = DELETED; }
  bool is_avail_state() const { return AVAIL == state_; }
  bool is_deleted_state() const { return DELETED == state_; }
  static int alloc_and_init_sql_table_entry(const ObSqlTableEntryKey &key,
                                            const common::ObString &table_name,
                                            ObSqlTableEntry *&entry);

  int init(char *buf_start, const int64_t buf_len);

  bool is_table_from_reroute() const { return is_table_from_reroute_; }
  void set_table_from_reroute() { is_table_from_reroute_ = true; }
  const common::ObString &get_table_name() const { return table_name_; }
  const ObSqlTableEntryKey &get_key() const { return key_; }
  bool is_valid() const;

  int64_t get_cr_version() const { return key_.cr_version_; }
  void set_create_time() { create_time_us_ = common::hrtime_to_usec(event::get_hrtime()); }
  void renew_last_access_time_us() { last_access_time_us_ = common::hrtime_to_usec(event::get_hrtime()); }
  void renew_last_update_time_us() { last_update_time_us_ = common::hrtime_to_usec(event::get_hrtime()); }
  int64_t get_last_access_time_us() const { return last_access_time_us_; }
  int64_t get_create_time_us() const { return create_time_us_; }
  int64_t get_last_update_time_us() const { return last_update_time_us_; }

  TO_STRING_KV(KP(this), K_(state), K_(is_table_from_reroute), K_(key), K_(table_name),
               K_(create_time_us), K_(last_access_time_us),
               K_(last_update_time_us),
               KP_(buf_start), K_(buf_len));

public:
  LINK(ObSqlTableEntry, link_);

private:
  void copy_key_and_name(const ObSqlTableEntryKey &key, const common::ObString &table_name);

private:
  ObSqlTableEntryState state_;
  bool is_table_from_reroute_;
  common::ObString table_name_;
  ObSqlTableEntryKey key_;
  int64_t buf_len_;
  char *buf_start_;

  int64_t create_time_us_;
  int64_t last_access_time_us_;
  int64_t last_update_time_us_;

  DISALLOW_COPY_AND_ASSIGN(ObSqlTableEntry);
};

inline bool ObSqlTableEntry::is_valid() const
{
  return key_.is_valid() && !table_name_.empty();
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_TABLE_ENTRY_H */
