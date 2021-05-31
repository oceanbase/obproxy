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

#ifndef OBPROXY_ROUTINE_ENTRY_H
#define OBPROXY_ROUTINE_ENTRY_H
#include "lib/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/string/ob_string.h"
#include "lib/ptr/ob_ptr.h"
#include "lib/list/ob_intrusive_list.h"
#include "common/ob_role.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_schema_struct.h"
#include "iocore/eventsystem/ob_thread.h"
#include "iocore/eventsystem/ob_continuation.h"
#include "proxy/route/ob_route_struct.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define ROUTINE_ENTRY_LOOKUP_CACHE_DONE     (ROUTINE_ENTRY_EVENT_EVENTS_START + 1)
#define ROUTINE_ENTRY_LOOKUP_START_EVENT    (ROUTINE_ENTRY_EVENT_EVENTS_START + 2)
#define ROUTINE_ENTRY_LOOKUP_CACHE_EVENT    (ROUTINE_ENTRY_EVENT_EVENTS_START + 3)
#define ROUTINE_ENTRY_LOOKUP_REMOTE_EVENT   (ROUTINE_ENTRY_EVENT_EVENTS_START + 4)
#define ROUTINE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT   (ROUTINE_ENTRY_EVENT_EVENTS_START + 5)


enum ObRoutineType
{
  INVALID_ROUTINE_TYPE = 0,
  ROUTINE_PROCEDURE_TYPE = 1,
  ROUTINE_FUNCTION_TYPE = 2,
  ROUTINE_PACKAGE_TYPE = 3,
  ROUTINE_MAX_TYPE
};

common::ObString get_routine_type_string(const ObRoutineType type);

typedef ObTableEntryName ObRoutineEntryName;
typedef ObTableEntryKey ObRoutineEntryKey;

class ObRoutineEntry : public ObRouteEntry
{
public:
  ObRoutineEntry()
    : ObRouteEntry(), is_inited_(false), routine_type_(INVALID_ROUTINE_TYPE),
      routine_id_(common::OB_INVALID_ID), names_(), route_sql_(),
      is_package_database_(), buf_len_(0), buf_start_(NULL)
  {
  }

  virtual ~ObRoutineEntry() {} // must be empty
  virtual void free();

  static int alloc_and_init_routine_entry(const ObRoutineEntryName &name,
                                          const int64_t cr_version,
                                          const int64_t cr_id,
                                          const common::ObString &route_sql,
                                          ObRoutineEntry *&entry);

  int init(char *buf_start, const int64_t buf_len);
  bool is_valid() const;
  bool is_the_same_entry(const ObRoutineEntry &other) const;
  ObRoutineType get_routine_type() const { return routine_type_; }
  uint64_t get_routine_id() const { return routine_id_; }
  const common::ObString &get_cluster_name() const { return names_.cluster_name_; }
  const common::ObString &get_tenant_name() const { return names_.tenant_name_; }
  const common::ObString &get_package_name() const { return names_.package_name_; }
  const common::ObString &get_database_name() const { return names_.database_name_; }
  const common::ObString &get_routine_name() const { return names_.table_name_; }
  const ObRoutineEntryName &get_names() const { return names_; }
  void get_key(ObRoutineEntryKey &key) const;
  common::ObString get_route_sql() const { return route_sql_; }

  void set_routine_type(const int64_t routine_type);
  void set_routine_id(const uint64_t id) { routine_id_ = id; }
  int set_names_sql(const ObRoutineEntryName &names, const common::ObString &route_sql,
                    const uint32_t parse_extra_char_num);
  void set_is_package_database(bool is_package_database) { is_package_database_ = is_package_database; }
  bool is_package_database() const { return is_package_database_; }

  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  Que(event::ObContinuation, link_) pending_queue_;

private:
  bool is_inited_;

  ObRoutineType routine_type_;
  uint64_t routine_id_;
  ObRoutineEntryName names_;

  common::ObString route_sql_;//not include the end '\0', but must has '\0' in the end
  bool is_package_database_; //package name is real database name

  int64_t buf_len_;
  char *buf_start_;

  DISALLOW_COPY_AND_ASSIGN(ObRoutineEntry);
};

inline bool ObRoutineEntry::is_valid() const
{
  return (common::OB_INVALID_ID != routine_id_
          && names_.is_valid()
          && (cr_version_ >= 0)
          && (cr_id_ >= 0)
          );
}

inline bool ObRoutineEntry::is_the_same_entry(const ObRoutineEntry &entry) const
{
  bool bret = false;
  if (is_valid()
      && entry.is_valid()) {
    // cr version
    if (get_cr_version() != entry.get_cr_version()) {
    // routine_id
    } else if (get_cr_id() != entry.get_cr_id()) {
    // cluster id
    }else if (get_routine_id() != entry.get_routine_id()) {
    // routine_type
    } else if (get_routine_type() != entry.get_routine_type()) {
    // names
    } else if (get_names() != entry.get_names()) {
    // route sql
    } else if (get_route_sql() != entry.get_route_sql()) {
    // equal
    } else {
      bret = true;
    }
  }
  return bret;
}

inline void ObRoutineEntry::get_key(ObRoutineEntryKey &key) const
{
  key.name_ = &get_names();
  key.cr_version_ = get_cr_version();
  key.cr_id_ = get_cr_id();
}

inline void ObRoutineEntry::set_routine_type(const int64_t routine_type)
{
  routine_type_ = ((routine_type >= 0 && routine_type < ROUTINE_PACKAGE_TYPE)
      ? static_cast<ObRoutineType>(routine_type)
      : INVALID_ROUTINE_TYPE);
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_ROUTINE_ENTRY_H */
