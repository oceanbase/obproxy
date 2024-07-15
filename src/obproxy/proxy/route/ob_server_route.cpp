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

#include "proxy/route/ob_server_route.h"
#include "obproxy/proxy/rpc_optimize/ob_rpc_req.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

int64_t ObServerRoute::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("consistency_level", get_consistency_level_str(consistency_level_),
       "cur_chosen_route_type", get_route_type_string(cur_chosen_route_type_),
       K_(cur_chosen_server),
       K_(cur_chosen_pl),
       K_(valid_count),
       K_(leader_item),
       K_(ldc_route),
       K_(is_table_entry_from_remote),
       K_(is_part_entry_from_remote),
       K_(need_use_dup_replica),
       K_(use_proxy_primary_zone_name),
       KP_(table_entry),
       KP_(part_entry),
       KP_(dummy_entry),
       K_(skip_leader_item));
  J_OBJ_END();

  return pos;
}

int ObServerRoute::fill_replicas(
    const common::ObConsistencyLevel level,
    const ObRoutePolicyEnum route_policy,
    const bool is_random_routing_mode,
    const bool disable_merge_status_check,
    ObRpcOBKVInfo &obkv_info,
    obutils::ObClusterResource *cluster_resource,
    const common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
    const common::ObIArray<common::ObString> &region_names,
    const common::ObIArray<common::ObString> &proxy_primary_zone_name
    )
{
  int ret = common::OB_SUCCESS;

  if (OB_ISNULL(obkv_info.dummy_entry_)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "dummy_entry is not available", K(obkv_info), K(ret));
  } else {
    ObTableEntry *dummy_entry = obkv_info.dummy_entry_;
    ObLDCLocation &dummy_ldc = obkv_info.dummy_ldc_;

    if (OB_UNLIKELY(common::STRONG != level && common::WEAK != level)) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(WDIAG, "unsupport ObConsistencyLevel", K(level), K(ret));
    } else if (OB_ISNULL(dummy_entry) || OB_UNLIKELY(!dummy_entry->is_tenant_servers_valid())) {
      ret = common::OB_INVALID_ARGUMENT;
      PROXY_LOG(WDIAG, "dummy_entry is not available", KPC(dummy_entry), K(ret));
    } else if (OB_UNLIKELY(dummy_entry->get_tenant_servers() != dummy_ldc.get_tenant_server())) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(WDIAG, "dummy_entry is not equal to dummy_ldc.tenant_server", KPC(dummy_entry), K(dummy_ldc), K(ret));
    } else if (OB_UNLIKELY(dummy_ldc.is_empty())) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(WDIAG, "dummy_entry is emtry", K(dummy_ldc), K(ret));
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

      const int64_t replica_count = dummy_entry->get_tenant_servers()->replica_count_;
      if (OB_FAIL(ObLDCLocation::shuffle_dummy_ldc(dummy_ldc, replica_count, is_weak_read()))) {
        PROXY_LOG(WDIAG, "fail to shuffle_dummy_ldc", K(dummy_ldc), K(replica_count), K(ret));
      } else {
        if (is_strong_read()) {
          ObString &tenant_name = obkv_info.tenant_name_;
          ret = fill_strong_read_replica(cur_chosen_pl_, dummy_ldc, ss_info, region_names,
                                        proxy_primary_zone_name, tenant_name, cluster_resource, is_random_routing_mode);
        } else {
          ret = fill_weak_read_replica(cur_chosen_pl_, dummy_ldc, ss_info, region_names, proxy_primary_zone_name);
        }
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
  }
  return ret;
}

const ObProxyReplicaLocation *ObServerRoute::get_obkv_strong_read_avail_replica()
{
  const ObProxyReplicaLocation *replica = NULL;

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

  for (int64_t i = 0; NULL != cur_chosen_pl_ && i < cur_chosen_pl_->replica_count(); i++) {
    replica = cur_chosen_pl_->get_replica(i);
    if (NULL != replica && replica->is_leader()) {
      PROXY_LOG(DEBUG, "succ to get_obkv_strong_read_avail_replica", KPC(replica));
      break;
    }
  }

  return replica;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
