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

#ifndef OBPROXY_SERVER_ROUTE_H
#define OBPROXY_SERVER_ROUTE_H

#include "lib/time/ob_hrtime.h"
#include "common/ob_hint.h"
#include "ob_ldc_route.h"
#include "proxy/route/ob_table_entry.h"
#include "proxy/route/ob_partition_entry.h"
#include "proxy/route/ob_mysql_route.h"
#include "obproxy/obutils/ob_proxy_config.h"
#include "obproxy/proxy/mysql/ob_mysql_client_session.h"


namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObClusterResource;
}

namespace proxy
{
class ObMysqlRouteResult;
class ObServerRoute
{
public:
  ObServerRoute()
    : table_entry_(NULL), dummy_entry_(NULL), part_entry_(NULL), cur_chosen_pl_(NULL),
      is_table_entry_from_remote_(false), is_part_entry_from_remote_(false),
      has_dup_replica_(false), need_use_dup_replica_(false), no_need_pl_update_(false),
      consistency_level_(common::INVALID_CONSISTENCY), leader_item_(),
      ldc_route_(), valid_count_(0), cur_chosen_server_(),
      cur_chosen_route_type_(ROUTE_TYPE_MAX), skip_leader_item_(false) {}
  ~ObServerRoute() { reset(); };
  inline void reset();

  // get next avail replica, the order is leader------>follower1---->follower2--->....+
  //                                         ^                                        |
  //                                         |                                        |
  //                                         +<---------------------------------------+
  const ObProxyReplicaLocation *get_next_avail_replica();
  const ObProxyReplicaLocation *get_next_replica(const uint32_t cur_ip, const uint16_t cur_port,
                                                 const bool is_force_retry);
  const ObProxyReplicaLocation *get_leader_replica_from_remote() const;

  ObTableEntry *get_dummy_entry();
  ObTableEntry *get_table_entry() { return table_entry_; }
  int fill_replicas(const common::ObConsistencyLevel level,
                    const ObRoutePolicyEnum route_policy,
                    const bool is_random_routing_mode,
                    const bool disable_merge_status_check,
                    ObMysqlClientSession *client_session,
                    obutils::ObClusterResource *cluster_resource,
                    const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
                    const common::ObIArray<common::ObString> &region_names,
                    const common::ObString &proxy_primary_zone_name
#if OB_DETAILED_SLOW_QUERY
                    ,ObHRTime &debug_random_time,
                    ObHRTime &debug_fill_time
#endif
                    );
  int fill_strong_read_replica(const ObProxyPartitionLocation *pl, ObLDCLocation &dummy_ldc,
                               const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
                               const common::ObIArray<common::ObString> &region_names,
                               const common::ObString &proxy_primary_zone_name,
                               const common::ObString &tenant_name,
                               obutils::ObClusterResource *cluster_resource,
                               const bool is_random_routing_mode);
  int fill_weak_read_replica(const ObProxyPartitionLocation *pl, ObLDCLocation &dummy_ldc,
                             const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
                             const common::ObIArray<common::ObString> &region_names,
                             const common::ObString &proxy_primary_zone_name);
  bool is_non_partition_table() const;
  bool is_partition_table() const;
  bool is_dummy_table() const;
  bool is_leader_server() const;
  bool is_target_location_server() const;
  bool is_remote_readonly() const;
  bool is_no_route_info_found() const { return (NULL == table_entry_) || (table_entry_->is_partition_table() && NULL == part_entry_); }
  bool is_empty_entry_allowed() const { return NULL != table_entry_ && table_entry_->is_empty_entry_allowed(); }
  bool is_all_iterate_once() const { return leader_item_.is_used_ && ldc_route_.is_reach_end(); }
  void reset_cursor() { leader_item_.is_used_ = false; ldc_route_.reset_cursor(); }
  bool is_leader_existent() const { return (NULL != get_leader_replica()); }
  bool is_server_from_rslist() const;
  bool need_update_entry() const;
  bool need_update_dummy_entry() const;
  bool is_follower_first_policy() const { return ldc_route_.is_follower_first_policy(); }
  bool is_leader_force_congested() const { return ldc_route_.location_.is_leader_force_congested(); }

  bool set_target_dirty(bool is_need_force_flush = false);
  bool set_dirty_all(bool is_need_force_flush = false);
  bool set_dummy_dirty();
  bool set_table_entry_dirty();
  bool set_delay_update();
  void set_route_info(ObMysqlRouteResult &result);
  void renew_last_valid_time();

  int64_t replica_size() const { return valid_count_; }
  int64_t get_last_valid_time_us() const;
  common::ObConsistencyLevel get_consistency_level() const { return consistency_level_; }
  bool is_strong_read() const { return common::STRONG == consistency_level_; }
  bool is_weak_read() const { return common::WEAK == consistency_level_; }
  void set_consistency_level(const common::ObConsistencyLevel level) { consistency_level_ = level; }
  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  const ObProxyReplicaLocation *get_leader_replica() const;
  void set_table_entry(ObTableEntry *entry);
  void set_part_entry(ObPartitionEntry *entry);
  void set_dummy_entry(ObTableEntry *entry);

public:
  ObTableEntry *table_entry_;
  ObTableEntry *dummy_entry_;
  ObPartitionEntry *part_entry_;
  const ObProxyPartitionLocation *cur_chosen_pl_;
  bool is_table_entry_from_remote_;
  bool is_part_entry_from_remote_;
  bool has_dup_replica_;
  bool need_use_dup_replica_;
  bool no_need_pl_update_;

  common::ObConsistencyLevel consistency_level_;
  ObLDCItem leader_item_;
  ObLDCRoute ldc_route_;
  int64_t valid_count_;
  ObLDCItem cur_chosen_server_;
  ObRouteType cur_chosen_route_type_;
  bool skip_leader_item_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObServerRoute);
};

inline void ObServerRoute::reset()
{
  is_table_entry_from_remote_ = false;
  is_part_entry_from_remote_ = false;
  has_dup_replica_ = false;
  need_use_dup_replica_ = false;
  no_need_pl_update_ = false;
  skip_leader_item_ = false;
  set_dummy_entry(NULL);
  set_table_entry(NULL);
  set_part_entry(NULL);
  cur_chosen_server_.reset();
  cur_chosen_pl_ = NULL;
  consistency_level_ = common::INVALID_CONSISTENCY;
  cur_chosen_route_type_ = ROUTE_TYPE_MAX;
  valid_count_ = 0;
  leader_item_.reset();
  ldc_route_.reset();
}

inline int ObServerRoute::fill_weak_read_replica(
    const ObProxyPartitionLocation *pl, ObLDCLocation &dummy_ldc,
    const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
    const common::ObIArray<common::ObString> &region_names,
    const ObString &proxy_primary_zone_name)
{
  int ret = common::OB_SUCCESS;
  const bool is_only_readonly_zone = (ONLY_READONLY_ZONE == ldc_route_.policy_);
  bool entry_need_update = false;
  leader_item_.reset();
  if (OB_FAIL(ObLDCLocation::fill_weak_read_location(pl, dummy_ldc, ldc_route_.location_,
                                                     entry_need_update, is_only_readonly_zone,
                                                     ss_info, region_names, proxy_primary_zone_name))) {
    PROXY_LOG(WARN, "fail to fill_weak_read_location", K(ret));
  } else {
    valid_count_ = ldc_route_.location_.count();
    PROXY_LOG(DEBUG, "succ to fill_weak_read_replica", KPC(this), KPC(pl), K(dummy_ldc));
  }
  if (entry_need_update) {
    //if dummy entry has expand, we can update it here
    set_dummy_dirty();
    if (dummy_ldc.is_ldc_used()) {
      //if ldc was used, the miss replica will not used, we need update entry here
      set_target_dirty();
    } else {
      //if ldc was not used, the miss replica will still used, we can update when partition miss
    }
  }
  return ret;
}

inline int ObServerRoute::fill_strong_read_replica(const ObProxyPartitionLocation *pl,
    ObLDCLocation &dummy_ldc, const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
    const common::ObIArray<common::ObString> &region_names,
    const ObString &proxy_primary_zone_name, const ObString &tenant_name,
    obutils::ObClusterResource *cluster_resource, const bool is_random_routing_mode)
{
  int ret = common::OB_SUCCESS;
  const bool is_only_readwrite_zone = (ONLY_READWRITE_ZONE == ldc_route_.policy_);
  bool entry_need_update = false;
  if (OB_FAIL(ObLDCLocation::fill_strong_read_location(pl, dummy_ldc, leader_item_,
                                                    ldc_route_.location_, entry_need_update,
                                                    is_only_readwrite_zone,
                                                    need_use_dup_replica_,
                                                    skip_leader_item_,
                                                    is_random_routing_mode,
                                                    ss_info, region_names,
                                                    proxy_primary_zone_name,
                                                    tenant_name,
                                                    cluster_resource))) {
    PROXY_LOG(WARN, "fail to divide_leader_replica", K(ret));
  } else {
    valid_count_ = ldc_route_.location_.count() + ((!need_use_dup_replica_ && leader_item_.is_valid()) ? 1 : 0);
    PROXY_LOG(DEBUG, "succ to fill_strong_read_replica", KPC(this), KPC(pl), K(dummy_ldc), K(valid_count_));
  }
  if (entry_need_update) {
    //if dummy entry has expand, we can update it here
    set_dirty_all();
  }
  return ret;
}

inline int ObServerRoute::fill_replicas(
    const common::ObConsistencyLevel level,
    const ObRoutePolicyEnum route_policy,
    const bool is_random_routing_mode,
    const bool disable_merge_status_check,
    ObMysqlClientSession *client_session,
    obutils::ObClusterResource *cluster_resource,
    const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
    const common::ObIArray<common::ObString> &region_names,
    const ObString &proxy_primary_zone_name
#if OB_DETAILED_SLOW_QUERY
    ,ObHRTime &debug_random_time,
    ObHRTime &debug_fill_time
#endif
    )
{
  int ret = common::OB_SUCCESS;

  ObTableEntry *dummy_entry = client_session->dummy_entry_;
  ObLDCLocation &dummy_ldc = client_session->dummy_ldc_;

  if (OB_UNLIKELY(common::STRONG != level && common::WEAK != level)) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_LOG(WARN, "unsupport ObConsistencyLevel", K(level), K(ret));
  } else if (OB_ISNULL(dummy_entry) || OB_UNLIKELY(!dummy_entry->is_tenant_servers_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WARN, "dummy_entry is not available", KPC(dummy_entry), K(ret));
  } else if (OB_UNLIKELY(dummy_entry->get_tenant_servers() != dummy_ldc.get_tenant_server())) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_LOG(WARN, "dummy_entry is not equal to dummy_ldc.tenant_server", KPC(dummy_entry), K(dummy_ldc), K(ret));
  } else if (OB_UNLIKELY(dummy_ldc.is_empty())) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_LOG(WARN, "dummy_entry is emtry", K(dummy_ldc), K(ret));
  } else {
    set_dummy_entry(dummy_entry);
    set_consistency_level(level);
    ldc_route_.policy_ = route_policy;
    ldc_route_.disable_merge_status_check_ = disable_merge_status_check;

    cur_chosen_pl_ = NULL;
    if (NULL != table_entry_) {
      if (table_entry_->is_non_partition_table()) {// non partition table
        cur_chosen_pl_ = table_entry_->get_first_pl();
      } else { // partition table
        if (NULL != part_entry_) {
          cur_chosen_pl_ = &(part_entry_->get_pl());
        }
      }
    }

#if OB_DETAILED_SLOW_QUERY
    ObHRTime t1 = common::get_hrtime_internal();
    ObHRTime t2 = 0;
#endif


    const int64_t replica_count = dummy_entry->get_tenant_servers()->replica_count_;
    if (OB_FAIL(ObLDCLocation::shuffle_dummy_ldc(dummy_ldc, replica_count, is_weak_read()))) {
      PROXY_LOG(WARN, "fail to shuffle_dummy_ldc", K(dummy_ldc), K(replica_count), K(ret));
    } else {
#if OB_DETAILED_SLOW_QUERY
      t2 = common::get_hrtime_internal();
      debug_random_time += t2 - t1;
      t1 = t2;
#endif

      if (is_strong_read()) {
        ObString &tenant_name = client_session->get_session_info().get_priv_info().tenant_name_;
        ret = fill_strong_read_replica(cur_chosen_pl_, dummy_ldc, ss_info, region_names,
                                       proxy_primary_zone_name, tenant_name, cluster_resource, is_random_routing_mode);
      } else {
        ret = fill_weak_read_replica(cur_chosen_pl_, dummy_ldc, ss_info, region_names, proxy_primary_zone_name);
      }
#if OB_DETAILED_SLOW_QUERY
      t2 = common::get_hrtime_internal();
      debug_fill_time += t2 - t1;
      t1 = t2;
#endif
    }

    if (OB_SUCC(ret)) {
      //if leader not exist or force congested, AND same region partition server not exist or force congested
      //we need use optimized route policy
      const bool is_need_optimized = (!ldc_route_.location_.is_same_region_partition_server_available()
                                      && (is_weak_read() || !leader_item_.is_valid() || leader_item_.is_force_congested_));
      if (is_need_optimized) {
        PROXY_LOG(DEBUG, "need optimize route policy", K_(leader_item), K_(ldc_route));
        switch (route_policy) {
          case MERGE_IDC_ORDER: {
            ldc_route_.policy_ = MERGE_IDC_ORDER_OPTIMIZED;
            ldc_route_.curr_cursor_index_ = ldc_route_.get_cursor_index(ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
            ldc_route_.next_index_in_site_ = ldc_route_.location_.get_other_region_site_start_index();
            break;
          }
          case READONLY_ZONE_FIRST: {
            ldc_route_.policy_ = READONLY_ZONE_FIRST_OPTIMIZED;
            ldc_route_.curr_cursor_index_ = ldc_route_.get_cursor_index(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
            ldc_route_.next_index_in_site_ = ldc_route_.location_.get_other_region_site_start_index();
            break;
          }
          case ONLY_READONLY_ZONE: {
            ldc_route_.policy_ = ONLY_READONLY_ZONE_OPTIMIZED;
            ldc_route_.curr_cursor_index_ = ldc_route_.get_cursor_index(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
            ldc_route_.next_index_in_site_ = ldc_route_.location_.get_other_region_site_start_index();
            break;
          }
          case UNMERGE_ZONE_FIRST: {
            ldc_route_.policy_ = UNMERGE_ZONE_FIRST_OPTIMIZED;
            ldc_route_.curr_cursor_index_ = ldc_route_.get_cursor_index(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
            ldc_route_.next_index_in_site_ = ldc_route_.location_.get_other_region_site_start_index();
            break;
          }
          case ONLY_READWRITE_ZONE: {
            ldc_route_.policy_ = ONLY_READWRITE_ZONE_OPTIMIZED;
            ldc_route_.curr_cursor_index_ = ldc_route_.get_cursor_index(ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);
            ldc_route_.next_index_in_site_ = ldc_route_.location_.get_other_region_site_start_index();
            break;
          }
          case FOLLOWER_FIRST: {
            ldc_route_.policy_ = FOLLOWER_FIRST_OPTIMIZED;
            ldc_route_.curr_cursor_index_ = ldc_route_.get_cursor_index(ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
            ldc_route_.next_index_in_site_ = ldc_route_.location_.get_other_region_site_start_index();
            break;
          }
          case UNMERGE_FOLLOWER_FIRST: {
            ldc_route_.policy_ = UNMERGE_FOLLOWER_FIRST_OPTIMIZED;
            ldc_route_.curr_cursor_index_ = ldc_route_.get_cursor_index(ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
            ldc_route_.next_index_in_site_ = ldc_route_.location_.get_other_region_site_start_index();
            break;
          }
          case FOLLOWER_ONLY: {
            ldc_route_.policy_ = FOLLOWER_ONLY_OPTIMIZED;
            ldc_route_.curr_cursor_index_ = ldc_route_.get_cursor_index(ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
            ldc_route_.next_index_in_site_ = ldc_route_.location_.get_other_region_site_start_index();
            break;
          }
          default:
            //do nothing
            break;
        }
      }
    }
  }
  return ret;
}

inline const ObProxyReplicaLocation *ObServerRoute::get_next_avail_replica()
{
  const ObLDCItem *item = NULL;
  if (is_strong_read()) {
    if (need_use_dup_replica_) {
      // if need_use_dup_replica,
      // leader item has already been added into item_array of ldc_route
      item = ldc_route_.get_next_item();
      cur_chosen_route_type_ = ldc_route_.get_curr_route_type();
    } else {
      if (NULL != leader_item_.replica_ && !leader_item_.is_used_) {
        //1. get leader
        item = &leader_item_;
        cur_chosen_route_type_ = ROUTE_TYPE_LEADER;
        leader_item_.is_used_ = true;
      } else {
        //2. get follower
        leader_item_.is_used_ = true;
        item = ldc_route_.get_next_primary_zone_item();
        if (item == NULL) {
          item = ldc_route_.get_next_item();  // primary zone no replica, choose from others.
          if (NULL != item) {
            cur_chosen_route_type_ = ldc_route_.get_curr_route_type();
            PROXY_LOG(DEBUG, "enable pz, no pz item, choose from origin item", K(item->replica_->server_));
          }
        } else {
          // succ to get item from primary zone route optimize, no need update pl
          no_need_pl_update_ = true;
          PROXY_LOG(DEBUG, "enable pz, choose pz item", K(item->replica_->server_));
        }
      }
    }

    if (NULL != item) {
      cur_chosen_server_ = *item;
    } else {
      cur_chosen_server_.reset();
      cur_chosen_route_type_ = ROUTE_TYPE_MAX;
    }
  } else if (is_weak_read()) {
    leader_item_.is_used_ = true;

    item = ldc_route_.get_next_item();
    cur_chosen_route_type_ = ldc_route_.get_curr_route_type();

    if (NULL != item) {
      cur_chosen_server_ = *item;
    } else {
      cur_chosen_server_.reset();
      cur_chosen_route_type_ = ROUTE_TYPE_MAX;
    }
  } else {
    cur_chosen_server_.reset();
    cur_chosen_route_type_ = ROUTE_TYPE_MAX;
    PROXY_LOG(ERROR, "it should never arrive here", K(*this));
  }
  PROXY_LOG(DEBUG, "succ to get_next_replica",
            "consistency_level", get_consistency_level_str(consistency_level_),
            "route_policy", get_route_policy_enum_string(ldc_route_.policy_),
            "cur_chosen_route_type", get_route_type_string(cur_chosen_route_type_),
            K_(cur_chosen_server));
  return cur_chosen_server_.replica_;
}

inline const ObProxyReplicaLocation *ObServerRoute::get_next_replica(
    const uint32_t cur_ip,
    const uint16_t cur_port,
    const bool is_force_retry)
{
  UNUSED(is_force_retry);
  const ObProxyReplicaLocation *replica = NULL;
  bool found = false;
  leader_item_.is_used_ = true;
  while (!found) {
    replica = get_next_avail_replica();
    if (NULL == replica) {
      found = true;
    } else if ((cur_ip != replica->server_.get_ipv4())
                || (cur_port != static_cast<uint16_t>(replica->server_.get_port()))) {
      found = true;
    }
  }
  return replica;
}

inline bool ObServerRoute::is_partition_table() const
{
  return ((NULL != table_entry_) && table_entry_->is_partition_table());
}

inline bool ObServerRoute::is_non_partition_table() const
{
  return ((NULL != table_entry_) && table_entry_->is_non_partition_table());
}

inline const ObProxyReplicaLocation *ObServerRoute::get_leader_replica() const
{
  const ObProxyReplicaLocation *leader_replica = NULL;
  if (NULL != table_entry_) {
    if (table_entry_->is_non_partition_table()) {
      leader_replica = table_entry_->get_leader_replica();
    } else {
      if (NULL != part_entry_) {
        leader_replica = part_entry_->get_leader_replica();
      }
    }
  }
  return leader_replica;
}

inline bool ObServerRoute::is_leader_server() const
{
  bool bret = false;
  if (OB_LIKELY(NULL != cur_chosen_server_.replica_)) {
    bret = cur_chosen_server_.replica_->is_leader();
  }
  return bret;
}

inline bool ObServerRoute::is_target_location_server() const
{
  bool bret = false;
  if (cur_chosen_server_.is_valid()) {
    bret = cur_chosen_server_.is_partition_server_;
  }
  return bret;
}

inline bool ObServerRoute::is_remote_readonly() const
{
  bool bret = false;
  if (OB_LIKELY(NULL != cur_chosen_server_.replica_)) {
    bret = cur_chosen_server_.replica_->is_readonly_replica()
           && OTHER_REGION == ObLDCRoute::get_idc_type(cur_chosen_route_type_);
  }
  return bret;
}

inline bool ObServerRoute::need_update_entry() const
{
  bool bret = false;
  if (NULL != table_entry_) {
    if (table_entry_->is_partition_table()) {
      if (NULL != part_entry_ ) {
        bret = part_entry_->need_update_entry();
      }
    } else {
      // include common __all_dummy entry
      bret = table_entry_->need_update_entry();
    }
  }

  return bret;
}

inline bool ObServerRoute::need_update_dummy_entry() const
{
  // if it is partition table, and part entry is null, we should update dummy entry
  // because we will use dummy entry to send sqls
  return is_dummy_table()
         || (is_partition_table() && NULL == part_entry_);
}

inline bool ObServerRoute::is_dummy_table() const
{
  return (NULL != table_entry_ && table_entry_->is_dummy_entry());
}

inline void ObServerRoute::set_route_info(ObMysqlRouteResult &result)
{
  if (NULL != table_entry_) {
    table_entry_->dec_ref();
    table_entry_ = NULL;
    ldc_route_.reset();
    leader_item_.reset();
  }
  if (NULL != part_entry_) {
    part_entry_->dec_ref();
    part_entry_ = NULL;
    ldc_route_.reset();
    leader_item_.reset();
  }

  // handler over ref
  table_entry_ = result.table_entry_;
  part_entry_ = result.part_entry_;
  result.table_entry_ = NULL;
  result.part_entry_ = NULL;

  is_table_entry_from_remote_ = result.is_table_entry_from_remote_;
  is_part_entry_from_remote_ = result.is_partition_entry_from_remote_;
  has_dup_replica_ = result.has_dup_replica_;
}

inline void ObServerRoute::set_table_entry(ObTableEntry *entry)
{
  if (NULL != table_entry_) {
    table_entry_->dec_ref();
    table_entry_ = NULL;
    ldc_route_.reset();
    leader_item_.reset();
  }
  if (NULL != entry) {
    table_entry_ = entry;
    table_entry_->inc_ref();
  }
}

inline void ObServerRoute::set_dummy_entry(ObTableEntry *entry)
{
  if (NULL != dummy_entry_) {
    dummy_entry_->dec_ref();
    dummy_entry_ = NULL;
    ldc_route_.reset();
  }
  if (NULL != entry) {
    dummy_entry_ = entry;
    dummy_entry_->inc_ref();
  }
}

inline void ObServerRoute::set_part_entry(ObPartitionEntry *entry)
{
  if (NULL != part_entry_) {
    part_entry_->dec_ref();
    part_entry_ = NULL;
    ldc_route_.reset();
    leader_item_.reset();
  }
  if (NULL != entry) {
    part_entry_ = entry;
    part_entry_->inc_ref();
  }
}

inline const ObProxyReplicaLocation *ObServerRoute::get_leader_replica_from_remote() const
{
  const ObProxyReplicaLocation *leader_replica = NULL;
  if (NULL != table_entry_) {
    if (table_entry_->is_non_partition_table() && is_table_entry_from_remote_) {
      leader_replica = table_entry_->get_leader_replica();
    } else {
      if (NULL != part_entry_ && is_part_entry_from_remote_) {
        leader_replica = part_entry_->get_leader_replica();
      }
    }
  }
  return leader_replica;
}

inline ObTableEntry *ObServerRoute::get_dummy_entry()
{
  ObTableEntry *table_entry = NULL;
  if (NULL != table_entry_ && table_entry_->is_dummy_entry()) {
    table_entry = table_entry_;
  }
  return table_entry;
}

inline bool ObServerRoute::set_target_dirty(bool is_need_force_flush /*false*/)
{
  bool bret = false;
  if (is_non_partition_table()) {
    if (NULL != table_entry_
        && table_entry_->need_update_entry()
        && table_entry_->cas_set_dirty_state()) {
      table_entry_->set_need_force_flush(is_need_force_flush);
      bret = true;
      PROXY_LOG(INFO, "this table entry will set to dirty and wait for update", K(is_need_force_flush), KPC_(table_entry));
    }
  } else if (is_partition_table()) {
    if (NULL != part_entry_
        && part_entry_->need_update_entry()
        && part_entry_->cas_set_dirty_state()) {
      table_entry_->set_need_force_flush(is_need_force_flush);
      bret = true;
      PROXY_LOG(INFO, "this partition entry will set to dirty and wait for update", K(is_need_force_flush), KPC_(table_entry), KPC_(part_entry));
    }
  }
  return bret;
}

inline bool ObServerRoute::set_table_entry_dirty()
{
  bool bret = false;
  if (NULL != table_entry_
      && table_entry_->need_update_entry()
      && table_entry_->cas_set_dirty_state()) {
    bret = true;
    PROXY_LOG(INFO, "this table entry will set to dirty and wait for update", KPC_(table_entry));
  }
  return bret;
}

inline bool ObServerRoute::set_dirty_all(bool is_need_force_flush /*false*/)
{
  bool bret = false;
  // only normal table locaton need force flush
  if (set_target_dirty(is_need_force_flush)) {
    bret = true;
  }
  if (set_dummy_dirty()) {
    bret = true;
  }
  return bret;
}

inline bool ObServerRoute::set_dummy_dirty()
{
  bool bret = false;
  //sys dummy entry can not set dirty
  if (NULL != dummy_entry_ && !dummy_entry_->is_sys_dummy_entry()) {
    if (dummy_entry_->cas_set_dirty_state()) {
      bret = true;
      PROXY_LOG(INFO, "dummy entry set to diry, and wait for update", KPC_(dummy_entry));
    }
  }
  return bret;
}

inline bool ObServerRoute::set_delay_update()
{
  bool bret = false;
  if (NULL != cur_chosen_server_.replica_) {
    // include common __all_dummy entry
    if (is_non_partition_table()) {
      if (NULL != table_entry_
          && table_entry_->need_update_entry()
          && table_entry_->cas_set_delay_update()) {
        bret = true;
        PROXY_LOG(WARN, "this table entry will set delay update", KPC_(table_entry));
      }
    } else if (is_partition_table()) {
      if (NULL != part_entry_
          && part_entry_->need_update_entry()
          && part_entry_->cas_set_delay_update()) {
        bret = true;
        PROXY_LOG(WARN, "this partition entry will set delay update", KPC_(table_entry), KPC_(part_entry));
      }
    }
  }

  return bret;
}

inline void ObServerRoute::renew_last_valid_time()
{
  if (NULL != table_entry_) {
    table_entry_->renew_last_valid_time();
  }
  if (NULL != part_entry_) {
    part_entry_->renew_last_valid_time();
  }
}

inline int64_t ObServerRoute::get_last_valid_time_us() const
{
  int64_t valid_time = 0;
  bool found = false;
  if (NULL != cur_chosen_server_.replica_) {
    // only compare replica point is enough
    if ((NULL != table_entry_) && (cur_chosen_server_.replica_ == table_entry_->get_leader_replica())) {
      found = true;
      valid_time = table_entry_->get_last_valid_time_us();
    }

    if (!found && (NULL != part_entry_) && (cur_chosen_server_.replica_ == part_entry_->get_leader_replica())) {
      valid_time = part_entry_->get_last_valid_time_us();
    }
  }
  return valid_time;
}

inline bool ObServerRoute::is_server_from_rslist() const
{
  bool bret = false;
  if (NULL != dummy_entry_) {
    bret = dummy_entry_->is_entry_from_rslist();
  }

  return bret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_MYSQL_ROUTE_H
