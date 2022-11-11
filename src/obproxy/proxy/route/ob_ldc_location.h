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

#ifndef OBPROXY_LDC_LOCATION_H
#define OBPROXY_LDC_LOCATION_H
#include "lib/ob_errno.h"
#include "common/ob_zone_type.h"
#include "common/ob_role.h"
#include "proxy/route/ob_ldc_struct.h"
#include "proxy/route/ob_route_struct.h"
#include "proxy/route/ob_tenant_server.h"
#include "lib/allocator/page_arena.h"
#include "lib/random/ob_random.h"



namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObServerStateSimpleInfo;
class ObSafeSnapshotManager;
class ObProxyNameString;
class ObClusterResource;
}
namespace proxy
{
const int64_t OB_MAX_LDC_ITEM_COUNT = 16;

class ObLDCItem
{
public:
  ObLDCItem() : replica_(NULL), idc_type_(MAX_IDC_TYPE),
                zone_type_(common::ZONE_TYPE_INVALID), is_merging_(false),
                is_partition_server_(false), is_force_congested_(false), is_used_(false)
  {}
  ObLDCItem(const ObProxyReplicaLocation &replica, const bool is_merging,
            const ObIDCType idc_type, const common::ObZoneType zone_type,
            const bool is_force_congested)
    : replica_(&replica), idc_type_(idc_type), zone_type_(zone_type), is_merging_(is_merging),
      is_partition_server_(false), is_force_congested_(is_force_congested), is_used_(false)
  {}
  ObLDCItem(const ObProxyReplicaLocation &replica, const bool is_merging,
            const ObIDCType idc_type, const common::ObZoneType zone_type,
            const bool is_partition_server, const bool is_force_congested, const bool is_used)
    : replica_(&replica), idc_type_(idc_type), zone_type_(zone_type), is_merging_(is_merging),
      is_partition_server_(is_partition_server), is_force_congested_(is_force_congested), is_used_(is_used)
  {}
  ~ObLDCItem() {}
  void reset();
  bool is_valid() const
  {
    return (OB_LIKELY(NULL != replica_)
            && OB_LIKELY(MAX_IDC_TYPE != idc_type_)
            && OB_LIKELY(common::ZONE_TYPE_INVALID != zone_type_));
  }
  void set(const ObProxyReplicaLocation &replica, const bool is_merging,
           const ObIDCType idc_type, const common::ObZoneType zone_type,
           const bool is_partition_server, const bool is_force_congested);
  void set_partition_item(const ObProxyReplicaLocation &replica, const ObLDCItem &non_partition_item);
  void set_non_partition_item(const ObLDCItem &non_partition_item);
  TO_STRING_KV("idc_type", get_idc_type_string(idc_type_),
               "zone_type", common::zone_type_to_str(zone_type_),
               KPC_(replica),
               K_(is_merging),
               K_(is_partition_server),
               K_(is_force_congested),
               K_(is_used));

  const ObProxyReplicaLocation *replica_;
  ObIDCType idc_type_;
  common::ObZoneType zone_type_;
  bool is_merging_;
  bool is_partition_server_;
  bool is_force_congested_;
  bool is_used_;
};

inline void ObLDCItem::reset()
{
  replica_ = NULL;
  idc_type_ = MAX_IDC_TYPE;
  zone_type_ = common::ZONE_TYPE_INVALID;
  is_merging_ = false;
  is_partition_server_ = false;
  is_force_congested_ = false;
  is_used_ = false;
}

inline void ObLDCItem::set(const ObProxyReplicaLocation &replica, const bool is_merging,
    const ObIDCType idc_type, const common::ObZoneType zone_type, const bool is_partition_server,
    const bool is_force_congested)
{
  replica_ = &replica;
  is_merging_ = is_merging;
  idc_type_ = idc_type;
  zone_type_ = zone_type;
  is_partition_server_ = is_partition_server;
  is_force_congested_ = is_force_congested;
  is_used_ = false;
}

inline void ObLDCItem::set_partition_item(const ObProxyReplicaLocation &replica,
    const ObLDCItem &non_partition_item)
{
  replica_ = &replica;
  is_merging_ = non_partition_item.is_merging_;
  idc_type_ = non_partition_item.idc_type_;
  zone_type_ = non_partition_item.zone_type_;
  is_force_congested_ = non_partition_item.is_force_congested_;
  is_partition_server_ = true;
  is_used_ = false;
}

inline void ObLDCItem::set_non_partition_item(const ObLDCItem &non_partition_item)
{
  *this = non_partition_item;
  is_used_ = false;
}

class ObLDCLocation
{
public:
  ObLDCLocation()
    : item_array_(NULL), item_count_(0), primary_zone_item_array_(NULL), primary_zone_item_count_(0),
      site_start_index_array_(), pl_(NULL), ts_(NULL), safe_snapshot_mananger_(NULL),
      readonly_exist_status_(READONLY_ZONE_UNKNOWN), use_ldc_(false), idc_name_(), idc_name_buf_(),
      random_()
  {}
  ~ObLDCLocation()
  {
    reset();
  }

  enum ObRegionMatchedType
  {
    MATCHED_BY_NONE = 0,
    MATCHED_BY_IDC,
    MATCHED_BY_ZONE_PREFIX,
    MATCHED_BY_URL,
    MATCHED_MAX
  };
  static common::ObString get_region_match_type_string(const ObRegionMatchedType type);

  enum ObReadOnlyZoneExistStatus
  {
    READONLY_ZONE_UNKNOWN = 0,
    READONLY_ZONE_EXIST,
    READONLY_ZONE_NOT_EXIST,
    READONLY_ZONE_MAX,
  };
  static bool is_status_available(const ObReadOnlyZoneExistStatus status);
  static common::ObString get_zone_exist_status_string(const ObReadOnlyZoneExistStatus status);

  bool is_empty() const;
  int64_t count() const { return (is_empty() ? 0 : item_count_)
                                 + (is_primary_zone_empty() ? 0 : primary_zone_item_count_); }
  void reset_item_status();
  void reset_item_array();
  void reset();
  const ObLDCItem *get_item_array() const { return item_array_; }
  ObLDCItem *get_item_array() { return item_array_; }
  
  bool is_primary_zone_empty() const { return (NULL == primary_zone_item_array_ || primary_zone_item_count_ <= 0); }
  int64_t primary_zone_count() const { return ((is_primary_zone_empty()) ? 0 : primary_zone_item_count_); }

  const ObLDCItem *get_primary_zone_item_array() const { return primary_zone_item_array_; }
  ObLDCItem *get_primary_zone_item_array() { return primary_zone_item_array_; }
  
  const int64_t *get_site_start_index_array() const { return site_start_index_array_; }
  int64_t get_other_region_site_start_index() const { return site_start_index_array_[OTHER_REGION]; }
  bool is_ldc_used() const { return use_ldc_ && !idc_name_.empty(); }
  bool is_readonly_zone_exist();
  bool static is_in_logic_region(const common::ObIArray<common::ObString> &all_region_names,
                                 const common::ObString &region_name);
  const common::ObString &get_idc_name() const { return idc_name_; }
  const common::ObString &get_idc_name() { return idc_name_; }
  const ObTenantServer *get_tenant_server() const { return ts_;}
  const ObProxyPartitionLocation *get_partition_location() const { return pl_;}
  const ObLDCItem *get_item(const int64_t index) const;
  int64_t get_same_idc_count() const;
  int64_t get_same_region_count() const;
  int64_t get_other_region_count() const;
  void shuffle();
  int assign(const ObTenantServer *ts,
             const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
             const common::ObString &idc_name,
             const bool is_base_servers_added,
             const common::ObString &cluster_name,
             const int64_t cluster_id);

  static int shuffle_dummy_ldc(ObLDCLocation &dummy_ldc, const int64_t replica_count,
                               const bool is_weak_read);
  static int64_t get_first_item_index(const ObLDCLocation &dummy_ldc, const int64_t replica_count);

  static int get_region_name(const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
                             const common::ObString &idc_name,
                             const common::ObString &cluster_name,
                             const int64_t cluster_id,
                             obutils::ObProxyNameString &region_name_from_idc_list,
                             ObRegionMatchedType &matched_type,
                             common::ObIArray<common::ObString> &region_names);
  static int fill_strong_read_location(const ObProxyPartitionLocation *pl,
                                       ObLDCLocation &dummy_ldc,
                                       ObLDCItem &leader_item,
                                       ObLDCLocation &ldc_location,
                                       bool &entry_need_update,
                                       const bool is_only_readwrite_zone,
                                       const bool need_use_dup_replica,
                                       const bool need_skip_leader_item,
                                       const bool is_random_routing_mode,
                                       const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
                                       const common::ObIArray<common::ObString> &region_names,
                                       const common::ObString &proxy_primary_zone_name,
                                       const common::ObString &tenant_name,
                                       obutils::ObClusterResource *cluster_resource);
  static int fill_weak_read_location(const ObProxyPartitionLocation *pl,
                                     ObLDCLocation &dummy_ldc,
                                     ObLDCLocation &ldc_location,
                                     bool &entry_need_update,
                                     const bool is_only_readonly_zone,
                                     const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
                                     const common::ObIArray<common::ObString> &region_names,
                                     const common::ObString &proxy_primary_zone_name);
  static bool check_need_update_entry(const ObProxyReplicaLocation &replica,
                                      ObLDCLocation &dummy_ldc,
                                      const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
                                      const common::ObIArray<common::ObString> &region_names);
  static bool is_in_primary_zone(const ObProxyReplicaLocation &replica,
                                 const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
                                 const common::ObString &proxy_primary_zone_name);
  int set_ldc_location(const ObProxyPartitionLocation *pl,
                       const ObLDCLocation &dummy_ldc,
                       const common::ObIArray<ObLDCItem> &tmp_item_array,
                       const common::ObIArray<ObLDCItem> &tmp_pz_item_array);

  void set_safe_snapshot_manager(const obutils::ObSafeSnapshotManager *safe_snapshot_mananger)
  {
    safe_snapshot_mananger_ = safe_snapshot_mananger;
  }
  const obutils::ObSafeSnapshotManager *get_safe_snapshot_manager() const
  {
    return safe_snapshot_mananger_;
  }
  void sort_by_priority(const obutils::ObSafeSnapshotManager *safe_snapshot_mananger);

  bool is_in_same_region_unmerging(ObRouteType &route_type, common::ObZoneType &zone_type,
                                   const common::ObAddr &addr,
                                   const common::ObZoneType except_zone_type,
                                   const bool disable_merge_status_check = false) const;
  bool is_same_region_partition_server_available() const;
  bool is_leader_force_congested() const;
  bool is_readonly_zone(const common::ObAddr &addr) const;
  static int get_thread_allocator(common::ModulePageAllocator *&allocator);

  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  void sort_by_priority(const obutils::ObSafeSnapshotManager &safe_snapshot_mananger,
                        int64_t start_idx,
                        int64_t end_idx);
  int64_t get_priority(const obutils::ObSafeSnapshotManager &safe_snapshot_mananger,
                       const int64_t idx);
  void set_partition(const ObProxyPartitionLocation *partition) { pl_ = partition; }
  void set_tenant_server(const ObTenantServer *tenant_server) { ts_ = tenant_server; }
  void set_idc_name(const common::ObString &name);
  void set_use_ldc(const bool flag) { use_ldc_ = flag; }

  static int add_unique_region_name(const common::ObString &region_name,
                                    common::ObIArray<common::ObString> &region_names);
  typedef common::ObSEArray<ObLDCItem, OB_MAX_LDC_ITEM_COUNT> LdcItemArrayType;
  static int fill_primary_zone_item_array(common::ModulePageAllocator *allocator,
                                          obutils::ObClusterResource *cluster_resource,
                                          const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
                                          const ObString &tenant_name,
                                          ObLDCLocation &dummy_ldc,
                                          LdcItemArrayType &tmp_pz_item_array);
  static int fill_item_array_from_pl(const ObProxyPartitionLocation *pl,
                                     const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
                                     const common::ObIArray<ObString> &region_names,
                                     const ObString &proxy_primary_zone_name,
                                     const bool need_skip_leader_item,
                                     const bool is_only_readwrite_zone,
                                     const bool need_use_dup_replica,
                                     ObLDCLocation &dummy_ldc,
                                     bool &entry_need_update,
                                     ObLDCItem &leader_item,
                                     LdcItemArrayType &tmp_item_array);
private:
  ObLDCItem *item_array_;
  int64_t item_count_;

  ObLDCItem *primary_zone_item_array_;
  int64_t primary_zone_item_count_;

  //store start index of each site,
  //array[MAX_IDC_TYPE] store the end index, just for traverse efficiently
  int64_t site_start_index_array_[MAX_IDC_TYPE + 1];

  const ObProxyPartitionLocation *pl_;
  const ObTenantServer *ts_;
  const obutils::ObSafeSnapshotManager *safe_snapshot_mananger_;

  ObReadOnlyZoneExistStatus readonly_exist_status_;

  bool use_ldc_;
  common::ObString idc_name_;
  char idc_name_buf_[OB_PROXY_MAX_IDC_NAME_LENGTH];

  common::ObRandom random_;

  DISALLOW_COPY_AND_ASSIGN(ObLDCLocation);
};

inline bool ObLDCLocation::is_empty() const
{
  return (NULL == item_array_ || item_count_ <= 0);
}

inline const ObLDCItem *ObLDCLocation::get_item(const int64_t index) const
{
  return ((!is_empty() && index >= 0 && index < item_count_) ? (item_array_ + index) : NULL);
}

void ObLDCLocation::reset_item_status()
{
  if (!is_empty()) {
    for (int64_t i = 0; i < item_count_; ++i) {
      item_array_[i].is_used_ = false;
    }
  }
}

void ObLDCLocation::reset()
{
  reset_item_array();
  pl_ = NULL;
  ts_ = NULL;
  use_ldc_ = false;
  idc_name_.reset();
  safe_snapshot_mananger_ = NULL;
  readonly_exist_status_ = READONLY_ZONE_UNKNOWN;
}

inline int64_t ObLDCLocation::get_same_idc_count() const
{
  return (is_empty() ? 0 : (site_start_index_array_[SAME_REGION] - site_start_index_array_[SAME_IDC]));
}

inline int64_t ObLDCLocation::get_same_region_count() const
{
  return (is_empty() ? 0 : (site_start_index_array_[OTHER_REGION] - site_start_index_array_[SAME_REGION]));
}

inline int64_t ObLDCLocation::get_other_region_count() const
{
  return (is_empty() ? 0 : (site_start_index_array_[MAX_IDC_TYPE] - site_start_index_array_[OTHER_REGION]));
}

inline void ObLDCLocation::set_idc_name(const common::ObString &name)
{
  if (name.empty()) {
    idc_name_.reset();
  } else if (name.length() <= OB_PROXY_MAX_IDC_NAME_LENGTH) {
    MEMCPY(idc_name_buf_, name.ptr(), name.length());
    idc_name_.assign(idc_name_buf_, name.length());
  }
}

inline void ObLDCLocation::shuffle()
{
  if (!is_empty()) {
    for (int64_t i = 0; i < MAX_IDC_TYPE; ++i) {
      if (site_start_index_array_[i + 1] - site_start_index_array_[i] > 1) {
          std::random_shuffle(item_array_ + site_start_index_array_[i],
                              item_array_ + site_start_index_array_[i + 1], random_);
      }
    }
  }
}

inline bool ObLDCLocation::is_readonly_zone_exist()
{
  bool bret = false;
  if (!is_empty()) {
    if (is_status_available(readonly_exist_status_)) {
      bret = (READONLY_ZONE_EXIST == readonly_exist_status_);
    } else {
      readonly_exist_status_ = READONLY_ZONE_NOT_EXIST;
      for (int64_t i = site_start_index_array_[SAME_IDC]; !bret && i < site_start_index_array_[MAX_IDC_TYPE]; ++i) {
        if (common::ZONE_TYPE_READONLY == item_array_[i].zone_type_) {
          bret = true;
          readonly_exist_status_ = READONLY_ZONE_EXIST;
        }
      }
    }
  }
  return bret;
}

inline bool ObLDCLocation::is_in_logic_region(
    const common::ObIArray<common::ObString> &all_region_names,
    const common::ObString &region_name)
{
  bool bret = false;
  if (all_region_names.empty() || region_name.empty()) {
    bret = false;
  } else {
    for (int64_t i = 0; i < all_region_names.count() && !bret; ++i) {
      if (all_region_names.at(i) == region_name) {
        bret = true;
      }
    }
  }
  return bret;
}


inline void ObLDCLocation::sort_by_priority(
    const obutils::ObSafeSnapshotManager *safe_snapshot_mananger)
{
  if (OB_NOT_NULL(safe_snapshot_mananger) && !is_empty()) {
  // maybe safe snapshot function is disabled or not supported by server
    for (int64_t i = 0; i < MAX_IDC_TYPE; ++i) {
      if (site_start_index_array_[i + 1] - site_start_index_array_[i] > 1) {
        sort_by_priority(*safe_snapshot_mananger,
                         site_start_index_array_[i],
                         site_start_index_array_[i + 1]);
      }
    }
  }
}

inline int ObLDCLocation::shuffle_dummy_ldc(ObLDCLocation &dummy_ldc, const int64_t replica_count,
    const bool is_weak_read)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(dummy_ldc.is_empty())) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WARN, "this is not dummy_ldc", K(dummy_ldc), K(ret));
  } else if (OB_UNLIKELY(replica_count <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WARN, "replica_count is invalid", K(replica_count), K(ret));
  } else if (is_weak_read && NULL != dummy_ldc.get_safe_snapshot_manager()) {
    //we will shuffle after fill replicas
  } else {
    dummy_ldc.shuffle();
  }
  return ret;
}

DEF_TO_STRING(ObLDCLocation)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(use_ldc),
       K_(idc_name),
       K_(item_count),
       "site_start_index_array", common::ObArrayWrap<int64_t>(site_start_index_array_, MAX_IDC_TYPE + 1),
       "item_array", common::ObArrayWrap<ObLDCItem>(item_array_, item_count_),
       KP_(pl),
       KP_(ts),
       "readonly_exist_status", get_zone_exist_status_string(readonly_exist_status_));
  J_OBJ_END();
  return pos;
}

inline bool ObLDCLocation::is_in_same_region_unmerging(ObRouteType &route_type,
    common::ObZoneType &zone_type, const common::ObAddr &addr,
    const common::ObZoneType except_zone_type,
    const bool disable_merge_status_check/*false*/) const
{
  bool bret = false;
  bool need_break = false;
  route_type = ROUTE_TYPE_MAX;
  zone_type = common::ZONE_TYPE_INVALID;
  if (addr.is_valid()
      && get_same_region_count() + get_same_idc_count() > 0) {
    //same_region_unmerging = same_idc_unmerging + same_region_unmerging
    for (int64_t i = site_start_index_array_[SAME_IDC]; !need_break && i < site_start_index_array_[OTHER_REGION]; ++i) {
      if (OB_LIKELY(NULL != item_array_[i].replica_)
          && item_array_[i].replica_->server_ == addr) {
        need_break = true;
        if (!item_array_[i].is_force_congested_
            && (disable_merge_status_check || !item_array_[i].is_merging_)
            && (common::ZONE_TYPE_INVALID == except_zone_type || except_zone_type == item_array_[i].zone_type_)) {
          bret = true;
          route_type = (SAME_IDC == item_array_[i].idc_type_ ? ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL : ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
          zone_type = item_array_[i].zone_type_;
          PROXY_LOG(DEBUG, "server is in_same_region_unmerging",
                    "except_zone_type", zone_type_to_str(except_zone_type), K(disable_merge_status_check), "item", item_array_[i]);
        } else {
          PROXY_LOG(DEBUG, "server is not in_same_region_unmerging",
                    "except_zone_type", zone_type_to_str(except_zone_type), K(disable_merge_status_check), "item", item_array_[i]);
        }
      }
    }
  }
  return bret;
}

inline bool ObLDCLocation::is_readonly_zone(const common::ObAddr &addr) const
{
  bool bret = false;
  bool need_break = false;
  if (addr.is_valid()
      && get_same_region_count() + get_same_idc_count() > 0) {
    for (int64_t i = site_start_index_array_[SAME_IDC]; !need_break && i < site_start_index_array_[OTHER_REGION]; ++i) {
      if (OB_LIKELY(NULL != item_array_[i].replica_)
          && item_array_[i].replica_->server_ == addr) {
        need_break = true;
        bret = (common::ZONE_TYPE_READONLY == item_array_[i].zone_type_);
      }
    }
  }
  return bret;
}

inline bool ObLDCLocation::is_same_region_partition_server_available() const
{
  bool bret = false;
  if (!is_empty()) {
    for (int64_t i = site_start_index_array_[SAME_IDC]; !bret && i < site_start_index_array_[OTHER_REGION]; ++i) {
      if (item_array_[i].is_partition_server_ && !item_array_[i].is_force_congested_) {
        bret = true;
      }
    }
  }
  return bret;
}

inline bool ObLDCLocation::is_leader_force_congested() const
{
  bool bret = false;
  if (!is_empty()) {
    for (int64_t i = site_start_index_array_[SAME_IDC]; !bret && i < site_start_index_array_[MAX_IDC_TYPE]; ++i) {
      if (OB_NOT_NULL(item_array_[i].replica_)
          && item_array_[i].replica_->is_leader()
          && item_array_[i].is_force_congested_) {
        bret = true;
      }
    }
  }
  return bret;
}


common::ObString ObLDCLocation::get_region_match_type_string(
    const ObLDCLocation::ObRegionMatchedType type)
{
  static const common::ObString string_array[MATCHED_MAX] =
  {
      common::ObString::make_string("MATCHED_BY_NONE"),
      common::ObString::make_string("MATCHED_BY_IDC"),
      common::ObString::make_string("MATCHED_BY_ZONE_PREFIX"),
      common::ObString::make_string("MATCHED_BY_URL"),
  };

  common::ObString string;
  if (OB_LIKELY(type >= MATCHED_BY_NONE) && OB_LIKELY(type < MATCHED_MAX)) {
    string = string_array[type];
  }
  return string;
}

bool ObLDCLocation::is_status_available(const ObReadOnlyZoneExistStatus status)
{
  return (READONLY_ZONE_EXIST == status || READONLY_ZONE_NOT_EXIST == status);
}

common::ObString ObLDCLocation::get_zone_exist_status_string(const ObReadOnlyZoneExistStatus status)
{
  static const common::ObString string_array[READONLY_ZONE_MAX] =
  {
      common::ObString::make_string("READONLY_ZONE_UNKNOWN"),
      common::ObString::make_string("READONLY_ZONE_EXIST"),
      common::ObString::make_string("READONLY_ZONE_NOT_EXIST"),
  };

  common::ObString string;
  if (OB_LIKELY(status >= READONLY_ZONE_UNKNOWN) && OB_LIKELY(status < READONLY_ZONE_MAX)) {
    string = string_array[status];
  }
  return string;
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_LDC_LOCATION_H */
