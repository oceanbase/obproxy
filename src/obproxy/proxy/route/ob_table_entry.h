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

#ifndef OBPROXY_TABLE_ENTRY_H
#define OBPROXY_TABLE_ENTRY_H
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
#include "proxy/route/ob_tenant_server.h"
#include "proxy/route/obproxy_part_info.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObProxyPartInfo;

#define TABLE_ENTRY_EVENT_LOOKUP_DONE     (TABLE_ENTRY_EVENT_EVENTS_START + 1)

class ObTableEntry : public ObRouteEntry
{
public:
  ObTableEntry()
    : ObRouteEntry(), is_inited_(false), is_dummy_entry_(false), is_binlog_entry_(NULL), is_entry_from_rslist_(false),
      is_empty_entry_allowed_(false), is_need_force_flush_(false), has_dup_replica_(false), table_id_(common::OB_INVALID_ID),
      table_type_(share::schema::MAX_TABLE_TYPE), part_num_(0), replica_num_(0), name_(),
      buf_len_(0), buf_start_(NULL), first_pl_(NULL)
  {
  }

  virtual ~ObTableEntry() {} // must be empty
  virtual void free();

  static int alloc_and_init_table_entry(const ObTableEntryName &name, const int64_t cr_version,
                                        const int64_t cr_id, ObTableEntry *&entry);

  int init(char *buf_start, const int64_t buf_len);
  void set_part_num(const int64_t part_num) { part_num_ = part_num; }
  void set_replica_num(const int64_t replica_num) { replica_num_ = replica_num; }
  void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  void set_table_type(const int32_t table_type)
  {
    table_type_ = ((table_type >= 0 && table_type < share::schema::MAX_TABLE_TYPE)
        ? static_cast<share::schema::ObTableType>(table_type)
        : share::schema::MAX_TABLE_TYPE);
  }
  void set_entry_from_rslist() { is_entry_from_rslist_ = true; }
  int set_names(const ObTableEntryName &name);
  int set_first_partition_location(ObProxyPartitionLocation *first_location);
  int set_tenant_servers(ObTenantServer *new_ts);
  bool is_dummy_entry() const { return is_dummy_entry_; }
  bool is_sys_dummy_entry() const { return is_dummy_entry_ && name_.is_sys_tenant(); }
  bool is_common_dummy_entry() const { return is_dummy_entry_ && !name_.is_sys_tenant(); }
  bool is_location_entry() const { return (!is_dummy_entry_ && 1 == part_num_) || is_binlog_entry_; }
  bool is_part_info_entry() const { return !is_dummy_entry_ && part_num_ > 1; }
  bool is_non_partition_table() const { return (1 == get_part_num()); }
  bool is_partition_table() const { return (get_part_num() > 1); }
  bool is_entry_from_rslist() const { return is_entry_from_rslist_; }
  bool is_empty_entry_allowed() const { return is_empty_entry_allowed_; }
  void set_allow_empty_entry(const bool is_empty_entry_allowed) { is_empty_entry_allowed_ = is_empty_entry_allowed; }
  bool is_need_force_flush() const { return is_need_force_flush_; }
  void set_need_force_flush(const bool is_need_force_flush) { is_need_force_flush_ = is_need_force_flush; }
  bool has_dup_replica() const { return has_dup_replica_; }
  void set_has_dup_replica() { has_dup_replica_ = true; }

  bool exist_leader_server() const;
  const ObProxyReplicaLocation *get_leader_replica() const;
  bool need_update_entry() const;
  // Atttention!, this func only use to avoid table entry frequently updating
  bool is_the_same_entry(const ObTableEntry &entry) const;
  const common::ObString &get_cluster_name() const { return name_.cluster_name_; }
  const common::ObString &get_tenant_name() const { return name_.tenant_name_; }
  const common::ObString &get_database_name() const { return name_.database_name_; }
  const common::ObString &get_table_name() const { return name_.table_name_; }
  const ObTableEntryName &get_names() const { return name_; }
  void get_key(ObTableEntryKey &key) const;
  int64_t get_part_num() const { return part_num_; }
  int64_t get_replica_num() const { return replica_num_; }
  uint64_t get_table_id() const { return table_id_; }
  share::schema::ObTableType get_table_type() const { return table_type_; }
  int get_random_servers(ObProxyPartitionLocation &location);
  int get_random_servers_idx(int64_t &chosen_par_idx, int64_t &chosen_init_replica_idx_) const;
  bool is_valid() const;
  bool is_tenant_servers_valid() const;
  int64_t get_server_count() const;
  const ObTenantServer *get_tenant_servers() const { return is_dummy_entry() ? tenant_servers_ : NULL; }
  const ObProxyPartitionLocation *get_first_pl() const;
  int64_t get_tenant_replica_count(const int64_t partition_id) const;
  const ObProxyReplicaLocation *get_replica_location(const int64_t chosen_par_idx,
                                                     const int64_t init_replica_idx,
                                                     const int64_t idx) const;

  // will alloc part info if need
  int alloc_part_info();
  ObProxyPartInfo *get_part_info() { return part_info_; }
  ObProxyPartInfo *get_part_info() const { return part_info_; }
  int is_contain_all_dummy_entry(const ObTableEntry &new_entry, bool &is_contain_all) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  uint64_t get_all_server_hash() const;
  bool is_leader_server_equal(const ObTableEntry &entry) const;

public:
  Que(event::ObContinuation, link_) pending_queue_;

private:
  bool is_inited_;
  bool is_dummy_entry_;
  bool is_binlog_entry_;
  bool is_entry_from_rslist_;
  bool is_empty_entry_allowed_;
  bool is_need_force_flush_;
  bool has_dup_replica_;

  // schema info, add more later
  uint64_t table_id_;
  share::schema::ObTableType table_type_;
  int64_t part_num_;
  int64_t replica_num_;

  ObTableEntryName name_;
  int64_t buf_len_;
  char *buf_start_;

  // partition-table will not use this union
  union
  {
    ObProxyPartitionLocation *first_pl_; // non-partition-table pl execpt __all_dummy
    ObTenantServer *tenant_servers_;     // __all_dummy table use it
    ObProxyPartInfo *part_info_; // part_info use it
  };

  DISALLOW_COPY_AND_ASSIGN(ObTableEntry);
};

inline bool ObTableEntry::is_valid() const
{
  return (common::OB_INVALID_ID != table_id_
          && name_.is_valid()
          && common::OB_INVALID_CLUSTER_ID != cr_id_
          && (is_empty_entry_allowed_
              || ((part_num_ > 0)
                  && (replica_num_ > 0)
                  && (cr_version_ >= 0)
                  && ((is_dummy_entry() && NULL != tenant_servers_ && OB_LIKELY(tenant_servers_->is_valid()))
                      || (is_location_entry() && NULL != first_pl_ && OB_LIKELY(first_pl_->is_valid()))
                      || (is_part_info_entry() && NULL != part_info_ && OB_LIKELY(part_info_->is_valid()))))
              )
          ) || is_binlog_entry_;
}

inline int64_t ObTableEntry::get_server_count() const
{
  int64_t ret = 0;
  if (is_dummy_entry()) {
    if (NULL != tenant_servers_) {
      ret = tenant_servers_->count();
    }
  } else if (is_location_entry()) {
    if (NULL != first_pl_ && first_pl_->is_valid()) {
      ret = first_pl_->replica_count();
    }
  }
  return ret;
}

inline void ObTableEntry::get_key(ObTableEntryKey &key) const
{
  key.name_ = &get_names();
  key.cr_version_ = get_cr_version();
  key.cr_id_ = get_cr_id();
}

inline bool ObTableEntry::need_update_entry() const
{
  return (OB_LIKELY(is_valid())
          && is_avail_state()
          && !is_sys_dummy_entry());
}

inline bool ObTableEntry::is_tenant_servers_valid() const
{
  bool bret = false;
  if (OB_UNLIKELY(!is_inited_)) {
    PROXY_LOG(WDIAG, "not init", K_(is_inited));
  } else if (OB_UNLIKELY(!is_dummy_entry())) {
    PROXY_LOG(WDIAG, "non dummy entry has no tenant_servers_ for random server");
  } else if (OB_ISNULL(tenant_servers_) || OB_UNLIKELY(!tenant_servers_->is_valid())) {
    PROXY_LOG(WDIAG, "tenant_servers_ is not valid", K(*this));
  } else {
    bret = true;
  }
  return bret;
}

inline bool ObTableEntry::exist_leader_server() const
{
  bool bret = false;
  if (is_location_entry()
      && NULL != first_pl_
      && first_pl_->exist_leader()) {
    bret = true;
  }
  return bret;
}

inline uint64_t ObTableEntry::get_all_server_hash() const
{
  uint64_t hash = 0;
  const ObTenantServer *servers = get_tenant_servers();
  if (NULL != servers) {
    hash = servers->hash();
  } else {
    const ObProxyPartitionLocation *pl = get_first_pl();
    if (NULL != pl) {
      hash = pl->get_all_server_hash();
    }
  }
  return hash;
}

inline const ObProxyReplicaLocation *ObTableEntry::get_leader_replica() const
{
  const ObProxyReplicaLocation *replica = NULL;
  const ObProxyPartitionLocation *pl = get_first_pl();
  if (NULL != pl) {
    replica = pl->get_leader();
  }
  return replica;
}

inline bool ObTableEntry::is_leader_server_equal(const ObTableEntry &entry) const
{
  const ObProxyReplicaLocation *leader = get_leader_replica();
  const ObProxyReplicaLocation *entry_leader = entry.get_leader_replica();
  return (((NULL == leader) && (NULL == entry_leader))
          || ((NULL != leader) && (NULL != entry_leader) && (*leader == *entry_leader)));
}

inline const ObProxyReplicaLocation *ObTableEntry::get_replica_location(
    const int64_t chosen_partition_idx,
    const int64_t init_replica_idx,
    const int64_t idx) const
{
  int ret = common::OB_SUCCESS;
  const ObProxyReplicaLocation *replica = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    PROXY_LOG(WDIAG, "not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_LOG(EDIAG, "current entry is not available", K(*this), K(ret));
  } else {
    if (is_dummy_entry()) {
      replica = tenant_servers_->get_replica_location(chosen_partition_idx, init_replica_idx, idx);
      PROXY_LOG(DEBUG, "succ to get replica location from tenant servers", KPC(replica),
                KPC(tenant_servers_), K(chosen_partition_idx), K(init_replica_idx), K(idx));
    }
  }
  return replica;
}

inline int64_t ObTableEntry::get_tenant_replica_count(const int64_t partition_id) const
{
  int64_t count = 0;
  if (is_dummy_entry() && NULL != tenant_servers_) {
    count = tenant_servers_->replica_count(partition_id);
  }
  return count;
}

inline const ObProxyPartitionLocation *ObTableEntry::get_first_pl() const
{
  const ObProxyPartitionLocation *pl = NULL;
  if (is_location_entry()) {
    pl = first_pl_;
  }
  return pl;
}

inline int ObTableEntry::set_first_partition_location(ObProxyPartitionLocation *first_location)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    PROXY_LOG(WDIAG, "not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(!is_location_entry())) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_LOG(WDIAG, "dummy entry or partition can not set first_pl", K(ret));
  } else if (OB_ISNULL(first_location) || OB_UNLIKELY(!first_location->is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid input value", KPC(first_location), K(ret));
  } else {
    PROXY_LOG(DEBUG, "will set first partition location", KPC(first_pl_), KPC(first_location));
    if (NULL != first_pl_) {
      //free old pl
      op_free(first_pl_);
      //set new pl
      first_pl_ = first_location;
    } else {
      first_pl_ = first_location;
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_TABLE_ENTRY_H */
