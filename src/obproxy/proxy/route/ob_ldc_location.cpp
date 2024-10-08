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

#define USING_LOG_PREFIX PROXY

#include "proxy/route/ob_ldc_location.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array_iterator.h"
#include "utils/ob_proxy_utils.h"
#include "obutils/ob_state_info.h"
#include "obutils/ob_safe_snapshot_manager.h"
#include "obutils/ob_config_server_processor.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "obproxy/obutils/ob_resource_pool_processor.h"
#include "obproxy/omt/ob_proxy_config_table_processor.h"
#include "proxy/mysql/ob_mysql_sm.h"


using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
bool ObWeightZoneItems::is_valid() const
{
  bool bret = false;
  for (int64_t i = 0; i < weight_zone_item_array_.count(); ++i) {
    if (!weight_zone_item_array_.at(i).is_used_) {
      bret = true;
      break;
    }
  }
  return bret;
}

void ObLDCLocation::reset_item_array()
{
  if (NULL != item_array_ && item_count_ > 0) {
    op_fixed_mem_free(item_array_, static_cast<int64_t>(sizeof(ObLDCItem)) * item_count_);
  }

  if (NULL != primary_zone_item_array_ && primary_zone_item_count_ > 0) {
    op_fixed_mem_free(primary_zone_item_array_, static_cast<int64_t>(sizeof(ObLDCItem)) * primary_zone_item_count_);
  }

  if (NULL != all_weight_zone_array_) {
    for (int64_t i = 0; i < all_weight_zone_array_->count(); ++i) {
      if (OB_NOT_NULL(all_weight_zone_array_->at(i))) {
        op_free(all_weight_zone_array_->at(i));
      }
    }
    op_free(all_weight_zone_array_);
  }

  item_array_ = NULL;
  item_count_ = 0;
  primary_zone_item_array_ = NULL;
  primary_zone_item_count_ = 0;
  all_weight_zone_array_ = NULL;
  all_weight_zone_item_count_ = 0;

  site_start_index_array_[SAME_IDC] = 0;
  site_start_index_array_[SAME_REGION] = 0;
  site_start_index_array_[OTHER_REGION] = 0;
  site_start_index_array_[MAX_IDC_TYPE] = 0;
}

inline int ObLDCLocation::add_unique_region_name(
    const ObString &region_name,
    ObIArray<ObString> &region_names)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t j = 0; (j < region_names.count()) && !found; ++j) {
    if (region_name == region_names.at(j)) {
      found = true;
    }
  }
  if (!found) {
    if (OB_FAIL(region_names.push_back(region_name))) {
      LOG_WDIAG("fail to push back region_name", K(region_name), K(ret));
    }
  }

  return ret;
}

int ObLDCLocation::get_region_name(
    const ObIArray<ObServerStateSimpleInfo> &ss_info,
    const ObString &idc_name,
    const ObString &cluster_name,
    const int64_t cluster_id,
    ObProxyNameString &region_name_from_idc_list,
    ObRegionMatchedType &matched_type,
    ObIArray<ObString> &region_names)
{
  int ret = OB_SUCCESS;
  // 1. match by idc string
  ObString zone_idc_string;
  ObString zone_region_string;
  matched_type = MATCHED_BY_NONE;
  if (!idc_name.empty()) {
    for (int64_t i = 0; i < ss_info.count() && OB_SUCC(ret); ++i) {
      zone_idc_string = ss_info.at(i).idc_name_;
      if (0 == idc_name.case_compare(zone_idc_string)) { // ignore case
        zone_region_string = ss_info.at(i).region_name_;
        // do not add duplicated region name
        if (OB_FAIL(add_unique_region_name(zone_region_string, region_names))) {
          LOG_WDIAG("fail to add unique region name", K(zone_region_string), K(ret));
        } else {
          matched_type = MATCHED_BY_IDC;
        }
      }
    }

    // 2. if not found by idc name, match by zone prefix
    if (OB_SUCC(ret) && region_names.empty()) {
      for (int64_t i = 0; i < ss_info.count() && OB_SUCC(ret); i++) {
        if (ss_info.at(i).zone_name_.prefix_case_match(idc_name)) {
          zone_region_string = ss_info.at(i).region_name_;
          // do not add duplicated region name
          if (OB_FAIL(add_unique_region_name(zone_region_string, region_names))) {
            LOG_WDIAG("fail to add unique region name", K(zone_region_string), K(ret));
          } else {
            matched_type = MATCHED_BY_ZONE_PREFIX;
          }
        }
      }
    }

    // 3. if specify by url from OCP, which has low priority
    if (OB_SUCC(ret) && region_names.empty() && !cluster_name.empty()) {
      if (OB_FAIL(get_global_config_server_processor().get_cluster_idc_region(
          cluster_name, cluster_id, idc_name, region_name_from_idc_list))) {
        LOG_WDIAG("fail to add unique region name", K(zone_region_string), K(ret));
      } else if (!region_name_from_idc_list.empty()) {
        if (OB_FAIL(add_unique_region_name(region_name_from_idc_list.name_string_, region_names))) {
          LOG_WDIAG("fail to add unique region name", K(zone_region_string), K(ret));
        } else {
          matched_type = MATCHED_BY_URL;
        }
      }
    }
  }
  return ret;
}

int64_t ObLDCLocation::get_first_item_index(const ObLDCLocation &dummy_ldc, const int64_t replica_count)
{
  int64_t ret_idx = 0;
  const int64_t dummy_ldc_count = dummy_ldc.count();
  if (!dummy_ldc.is_ldc_used() && replica_count > 0 && replica_count < dummy_ldc_count) {
    int64_t partition_count = (dummy_ldc_count / replica_count + (0 == dummy_ldc_count % replica_count ? 0 : 1));
    int64_t start_partition_idx = 0;
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObRandomNumUtils::get_random_num(0, partition_count - 1, start_partition_idx))) {
      PROXY_LOG(WDIAG, "fail to get random num", K(partition_count), K(ret));
    } else {
      ret_idx = start_partition_idx * replica_count;
    }
  }
  return ret_idx;
}

//init ldc with given idc_name and ss_info
int ObLDCLocation::assign(const ObTenantServer *ts, const ObIArray<ObServerStateSimpleInfo> &ss_info,
    const ObString &idc_name, const bool is_base_servers_added,
    const ObString &cluster_name,
    const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  common::ModulePageAllocator *allocator = NULL;
  if ((is_base_servers_added && OB_UNLIKELY(ss_info.empty()))
      || OB_UNLIKELY(idc_name.length() > OB_PROXY_MAX_IDC_NAME_LENGTH)
      || OB_ISNULL(ts)
      || OB_UNLIKELY(!ts->is_valid())
      || OB_UNLIKELY(ts->is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("argument is invalid", K(idc_name), KPC(ts), K(ss_info), K(ret));
  } else if (OB_FAIL(get_thread_allocator(allocator))) {
    LOG_WDIAG("fail to get_thread_allocator", K(ret));
  } else {
    reset();
    set_tenant_server(ts);
    ObSEArray<ObString, 5> region_names(5, *allocator);
    ObProxyNameString region_name_from_idc_list;

    ObRegionMatchedType match_type = MATCHED_BY_NONE;
    if (OB_FAIL(ObLDCLocation::get_region_name(ss_info, idc_name, cluster_name, cluster_id,
                                               region_name_from_idc_list,
                                               match_type, region_names))) {
      LOG_WDIAG("fail to get region name", K(idc_name), K(ret));
    } else {
      if (idc_name.empty()) {
        //do nothing
      } else if (region_names.empty()) {
        set_idc_name(idc_name);
        LOG_WDIAG("can not find region name, maybe set error idc name, treat as do not use ldc",
                 K(cluster_name), K(idc_name), K(ret));
      } else {
        set_use_ldc(true);
        set_idc_name(idc_name);
      }

      ObSEArray<ObLDCItem, OB_MAX_LDC_ITEM_COUNT> *tmp_item_array[MAX_IDC_TYPE] = {};
      ObSEArray<ObLDCItem, OB_MAX_LDC_ITEM_COUNT> local_item_array(OB_MAX_LDC_ITEM_COUNT, *allocator);
      ObSEArray<ObLDCItem, OB_MAX_LDC_ITEM_COUNT> region_item_array(OB_MAX_LDC_ITEM_COUNT, *allocator);
      ObSEArray<ObLDCItem, OB_MAX_LDC_ITEM_COUNT> remote_item_array(OB_MAX_LDC_ITEM_COUNT, *allocator);
      bool found = false;
      const bool default_merging_status = false;
      const bool default_congested_status = false;
      const ObIDCType default_idc_type = SAME_IDC;
      const ObZoneType default_zone_type = ZONE_TYPE_READWRITE;

      for (int64_t i = 0; OB_SUCC(ret) && i < ts_->count(); i++) {
        const ObProxyReplicaLocation &replica = ts_->server_array_[i];
        found = false;
        for (int64_t j = 0; !found && OB_SUCC(ret) && j < ss_info.count(); j++) {
          const ObServerStateSimpleInfo &ss = ss_info.at(j);
          if (ss.addr_ == replica.server_) {
            found = true;
            if (is_ldc_used()) {
              if (is_in_logic_region(region_names, ss.region_name_)) {
                if ((MATCHED_BY_IDC == match_type && (0 == ss.idc_name_.case_compare(idc_name))) // ignore case
                    || ((MATCHED_BY_ZONE_PREFIX == match_type) && ss.zone_name_.prefix_case_match(idc_name))) {
                  const ObLDCItem item(replica, ss.is_merging_, SAME_IDC, ss.zone_type_, ss.is_force_congested_);
                  if (OB_FAIL(local_item_array.push_back(item))) {
                    LOG_WDIAG("failed to push back same_idc tmp_item_array", K(i), K(item), K(local_item_array), K(ret));
                  }
                } else {
                  const ObLDCItem item(replica, ss.is_merging_, SAME_REGION, ss.zone_type_, ss.is_force_congested_);
                  if (OB_FAIL(region_item_array.push_back(item))) {
                    LOG_WDIAG("failed to push back same_region tmp_item_array", K(i), K(item), K(region_item_array), K(ret));
                  }
                }
              } else {
                const ObLDCItem item(replica, ss.is_merging_, OTHER_REGION, ss.zone_type_, ss.is_force_congested_);
                if (OB_FAIL(remote_item_array.push_back(item))) {
                  LOG_WDIAG("failed to push back other_region tmp_item_array", K(i), K(item), K(remote_item_array), K(ret));
                }
              }
            } else {
              const ObLDCItem item(replica, ss.is_merging_, SAME_IDC, ss.zone_type_, ss.is_force_congested_);
              if (OB_FAIL(local_item_array.push_back(item))) {
                LOG_WDIAG("failed to push back same_idc tmp_item_array", K(i), K(item), K(local_item_array), K(ret));
              }
            }
          }//end of found server
        }//end of for ss_info
        if (OB_SUCC(ret) && !found) {
          if (is_base_servers_added && !replica.server_.is_ip_loopback()) {
            LOG_WDIAG("fail to find tenant server from server list, maybe has not updated, don not use it", K(replica));
          } else {
            // LDC 情况下, 如果 OBServer 机器没有 IDC 信息, 降低优先级.
            // 避免有 ODP 内存中有下线机器, 却优先选择
            if (is_ldc_used()) {
              const ObLDCItem item(replica, default_merging_status, OTHER_REGION, default_zone_type, default_congested_status);
              if (OB_FAIL(remote_item_array.push_back(item))) {
                LOG_WDIAG("failed to push back same_idc tmp_item_array", K(i), K(item), K(local_item_array), K(ret));
              }
            } else {
              const ObLDCItem item(replica, default_merging_status, default_idc_type, default_zone_type, default_congested_status);
              if (OB_FAIL(local_item_array.push_back(item))) {
                LOG_WDIAG("failed to push back same_idc tmp_item_array", K(i), K(item), K(local_item_array), K(ret));
              }
            }
          }
        }
      }//end of for tenant_server_

      if (OB_SUCC(ret)) {
        int64_t item_count = 0;
        tmp_item_array[SAME_IDC] = &local_item_array;
        tmp_item_array[SAME_REGION] = &region_item_array;
        tmp_item_array[OTHER_REGION] = &remote_item_array;
        for (int64_t i = 0; i < MAX_IDC_TYPE; ++i) {
          item_count += tmp_item_array[i]->count();
        }
        const int64_t alloc_size = static_cast<int64_t>(sizeof(ObLDCItem)) * item_count;
        char *item_array_buf = NULL;
        if (OB_UNLIKELY(item_count <= 0)) {
          ret = OB_EMPTY_RESULT;
          LOG_WDIAG("fail to find any tenant server from server list", KPC(ts), K(ss_info), K(item_count), K(ret));
        } else if (OB_ISNULL(item_array_buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc mem", K(alloc_size), K(item_count), K(ret));
        } else {
          item_count_ = item_count;
          item_array_ = new (item_array_buf) ObLDCItem[item_count];
          site_start_index_array_[0] = 0;
          int64_t memcpy_size = 0;
          for (int64_t i = 0; i < MAX_IDC_TYPE; ++i) {
            site_start_index_array_[i + 1] = site_start_index_array_[i] + tmp_item_array[i]->count();
            if (tmp_item_array[i]->count() > 0) {
              memcpy_size = static_cast<int64_t>(sizeof(ObLDCItem)) * tmp_item_array[i]->count();
              MEMCPY(item_array_ + site_start_index_array_[i], &(tmp_item_array[i]->at(0)), memcpy_size);
            }
          }//end of for
        }//end of else
      }//end of OB_SUCC

      if (OB_SUCC(ret)) {
        LOG_DEBUG("succ to assign ldc location", K(idc_name), K(match_type), K(region_names), KPC(this));
      }
    }

    allocator = NULL;
  }
  return ret;
}

bool ObLDCLocation::check_need_update_entry(const ObProxyReplicaLocation &replica,
                                            ObLDCLocation &dummy_ldc,
                                            const ObIArray<ObServerStateSimpleInfo> &ss_info,
                                            const ObIArray<ObString> &region_names)
{
  bool bret = false;

  if (dummy_ldc.is_ldc_used()) {
    bool found = false;
    // 找到对应的副本的 zone 信息
    for (int64_t j = 0; !found && j < ss_info.count(); j++) {
      const ObServerStateSimpleInfo &ss = ss_info.at(j);
      if (ss.addr_ == replica.server_) {
        found = true;
        // 如果在集群内, 只需要检查是否同 REGION, 相同 IDC 肯定也是相同 REGION
        // 如果是相同 REGION, 或者不是 R 副本, 都需要刷新
        if (is_in_logic_region(region_names, ss.region_name_) || REPLICA_TYPE_READONLY != replica.replica_type_) {
          bret = true;
          LOG_WDIAG("check_need_update_entry, same region or not readonly replica, need update entry", K(replica), K(ss), K(region_names));
        } else {
          LOG_DEBUG("check_need_update_entry, other region and readonly replica, do not need update entry", K(replica), K(ss), K(region_names));
        }
      }
    }

    if (!found) {
      // 如果不在集群内, 需要重新刷新
      bret = true;
      LOG_WDIAG("check_need_update_entry, replica is not the cluster's servers, need update entry", K(replica));
    }
  } else {
    // 如果不是 ldc used, 有两种情况:
    //  1. 没有设置 idc name
    //  2. IDC 对应的 region names 为空
    // 不管哪种情况, 都无法知道这个副本是不是同 IDC/REGION 的, 直接认为需要更新
    bret = true;
    LOG_WDIAG("check_need_update_entry, not use ldc, need update entry", K(replica));
  }

  return bret;
}

bool ObLDCLocation::is_in_proxy_primary_zone(const ObProxyReplicaLocation &replica,
                                       const ObIArray<ObServerStateSimpleInfo> &ss_info,
                                       const ObIArray<ObString> &proxy_primary_zone_name,
                                       int64_t &priority)
{
  bool need_use_it = false;
  priority = 0;
  /* 只有设置了 proxy primary zone 信息, 并且获取到了 zone state 信息后, 才根据 zone 路由 */
  if (!proxy_primary_zone_name.empty() && ss_info.count() > 0) {
    bool found = false;
    need_use_it = false;
    // 找到对应的副本的 zone 信息
    for (int64_t j = 0; !found && j < ss_info.count(); j++) {
      const ObServerStateSimpleInfo &ss = ss_info.at(j);
      if (ss.addr_ == replica.server_) {
        found = true;
        for (int64_t k = 0; k < proxy_primary_zone_name.count(); ++k) {
          if (0 == ss.zone_name_.case_compare(proxy_primary_zone_name.at(k))) {
            priority = k;
            need_use_it = true;
          }
        }
      }
    }
  } else {
    need_use_it = true;
  }

  return need_use_it;
}

bool ObLDCLocation::is_in_primary_zone(const ObProxyReplicaLocation &replica,
                                       const ObIArray<ObServerStateSimpleInfo> &ss_info,
                                       const ObString &primary_zone_name)
{
  bool need_use_it = false;
  /* 只有设置了 primary zone 信息, 并且获取到了 zone state 信息后, 才根据 zone 路由 */
  if (!primary_zone_name.empty() && ss_info.count() > 0) {
    bool found = false;
    need_use_it = false;
    // 找到对应的副本的 zone 信息
    for (int64_t j = 0; !found && j < ss_info.count(); j++) {
      const ObServerStateSimpleInfo &ss = ss_info.at(j);
      if (ss.addr_ == replica.server_) {
        found = true;
        if (0 == ss.zone_name_.case_compare(primary_zone_name)) {
          need_use_it = true;
        }
      }
    }
  } else {
    need_use_it = true;
  }

  return need_use_it;
}

bool ObLDCLocation::is_in_weight_zone(const ObProxyReplicaLocation &replica,
                      const ObIArray<ObServerStateSimpleInfo> &ss_info,
                      const omt::ObZoneWeakReadWeight &weight_zone,
                      int32_t &weight_index)
{
  bool need_use_it = false;
  bool found = false;
  // ss_info不存在，即zone state获取失败，走权重负载均衡，会断连接
  for (int64_t i = 0; !found && i < ss_info.count(); ++i) {
    const ObServerStateSimpleInfo &ss = ss_info.at(i);
    if (ss.addr_ == replica.server_) {
      found = true;
      for (int64_t j = 0; j < weight_zone.zone_array_.count(); ++j) {
        const ObString &zone = weight_zone.zone_array_.at(j);
        if (0 == ss.zone_name_.case_compare(zone)) {
          need_use_it = true;
          weight_index = j;
          break;
        }
      }
    }
  }
  return need_use_it;
}

int ObLDCLocation::fill_strong_read_location(const ObProxyPartitionLocation *pl,
    ObLDCLocation &dummy_ldc, ObLDCItem &leader_item, ObLDCLocation &ldc_location,
    bool &entry_need_update, const bool is_only_readwrite_zone, const bool need_use_dup_replica,
    const bool need_skip_leader_item, const bool is_random_routing_mode,
    const ObIArray<ObServerStateSimpleInfo> &ss_info,
    const ObIArray<ObString> &region_names,
    const ObIArray<ObString> &proxy_primary_zone_name,
    const ObString &tenant_name,
    obutils::ObClusterResource *cluster_resource,
    const ObRoutePolicyEnum &route_policy)
{
  int ret = OB_SUCCESS;
  entry_need_update = false;
  common::ModulePageAllocator *allocator = NULL;
  if (OB_UNLIKELY(dummy_ldc.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("dummy_ldc is empty", K(dummy_ldc), K(ret));
  } else if (OB_FAIL(get_thread_allocator(allocator))) {
    LOG_WDIAG("fail to get_thread_allocator", K(ret));
  } else {
    LdcItemArrayType tmp_item_array(OB_MAX_LDC_ITEM_COUNT, *allocator);
    LdcItemArrayType tmp_pz_item_array(OB_MAX_LDC_ITEM_COUNT, *allocator);

    // reset status and item
    dummy_ldc.reset_item_status();
    leader_item.reset();

    //1. fill tmp_item_array from pl
    if (NULL != pl && pl->is_valid()) {
      if (OB_FAIL(fill_item_array_from_pl(pl, ss_info, region_names, proxy_primary_zone_name, need_skip_leader_item,
                                          is_only_readwrite_zone, need_use_dup_replica, dummy_ldc, entry_need_update,
                                          leader_item, tmp_item_array, route_policy))) {
        LOG_WDIAG("fail to fill item array from pl", K(ret));
      }
    } else if (cluster_resource != NULL
               && !need_use_dup_replica
               && proxy_primary_zone_name.empty()
               && !tenant_name.empty()
               && !is_random_routing_mode
               && get_global_proxy_config().enable_primary_zone) {
      //2. pl = NULL, no table entry, no partition entry, enable flag, choose from primary zone list
      // if proxy_primary_zone_name is set, use proxy_primary_zone_name as the proxy route dest addr
      // random route mode, do not go through primary zone policy
      if (OB_FAIL(fill_primary_zone_item_array(allocator, cluster_resource, ss_info, tenant_name,
                                               dummy_ldc, tmp_pz_item_array))) {
        LOG_WDIAG("fail to fill primary zone item array", K(ret));
      }
    }

    //4. fill tmp_item_array from dummy entry
    if (OB_SUCC(ret)) {
      const int64_t pl_count = tmp_item_array.count();
      if (pl_count > 1) {
        //shuffle the partition server
        std::random_shuffle(tmp_item_array.begin(), tmp_item_array.end(), dummy_ldc.random_);
      }
      ObLDCItem tmp_ldc_item;
      for (int64_t j = 0; OB_SUCC(ret) && j < dummy_ldc.item_count_; ++j) {
        const ObLDCItem &dummy_item = dummy_ldc.item_array_[j];
        int64_t priority = 0;
        // 请求不能发往日志型副本
        if (dummy_item.is_used_
            || not_allowed_replica_type(dummy_item.replica_->get_replica_type(), route_policy)) {
          //continue
        } else if (is_only_readwrite_zone && common::ZONE_TYPE_READWRITE != dummy_item.zone_type_) {
          //do not use id
        } else if (!is_in_proxy_primary_zone(*(dummy_item.replica_), ss_info, proxy_primary_zone_name, priority)) {
          //do not use id
        } else {
          tmp_ldc_item.set_non_partition_item(dummy_item);
          tmp_ldc_item.priority_ = priority;
          if (OB_FAIL(tmp_item_array.push_back(tmp_ldc_item))) {
            LOG_WDIAG("fail to push_back target_item", K(dummy_item), K(tmp_ldc_item), K(tmp_item_array), K(ret));
          }//no need set is_used_= true
        }
      }//end of for
      // 对proxy_primary_zone，所有副本按照优先级排序
      if (!proxy_primary_zone_name.empty() && tmp_item_array.count() > 1) {
        std::sort(tmp_item_array.begin(), tmp_item_array.end());
      }
    }

    //3. fill tenant_ldc without leader
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ldc_location.set_ldc_location(pl, dummy_ldc, tmp_item_array, &tmp_pz_item_array, NULL))) {
        LOG_WDIAG("fail to set_ldc_location", K(ret));
      }
    }
    allocator = NULL;
  }

  return ret;
}

bool ObLDCLocation::is_weak_read_avail_replica(const ObProxyReplicaLocation &replica,
                                       const ObRoutePolicyEnum &route_policy,
                                       const omt::ObTargetReplicaType *target_replica_type,
                                       const bool is_proxy_mysql_client)
{
  bool bret = false;

  // 1. 权重路由(租户级别)和proxy_primary_zone是以zone为维度，内部命令允许发往F/R/C
  // 2. 其它内部命令发往F/R
  if (PROXY_PRIMARY_ZONE_NAME_ONLY == route_policy || WEAKREAD_WEIGHT_LOAD_BALANCE == route_policy) {
    bret = replica.is_full_or_readonly_replica() || replica.is_columnstore_replica();
  } else if (is_proxy_mysql_client) {
    bret = replica.is_full_or_readonly_replica();
  } else if (is_target_replica_route(route_policy) && OB_NOT_NULL(target_replica_type)) {
    if (target_replica_type->is_exist_full_replica()) {
      bret = replica.is_full_replica();
    }
    if (!bret && target_replica_type->is_exist_readonly_replica()) {
      bret = replica.is_readonly_replica();
    }
    if (!bret && target_replica_type->is_exist_column_store_replica()) {
      bret = replica.is_columnstore_replica();
    }
    // 对云上选择主副本的处理，对非C副本下，with_leader和follower_first都允许路由主副本
    if (!bret && (TARGET_REPLICA_TYPE_WITH_LEADER == route_policy
                  || TARGET_REPLICA_TYPE_FOLLOWER_FIRST == route_policy)
        && (!target_replica_type->is_exist_column_store_replica())) {
      bret = replica.is_leader();
    }
  } else {
    // 非指定副本类型/权重/proxy primary路由，兼容老的默认行为
    bret = replica.is_full_or_readonly_replica();
  }
  return bret;
}

int ObLDCLocation::fill_weak_read_location(const ObProxyPartitionLocation *pl,
    ObLDCLocation &dummy_ldc, ObLDCLocation &ldc_location, bool &entry_need_update,
    const bool is_only_readonly_zone,
    const ObIArray<ObServerStateSimpleInfo> &ss_info,
    const ObIArray<ObString> &region_names,
    const ObIArray<ObString> &proxy_primary_zone_name,
    const ObRoutePolicyEnum &route_policy,
    const omt::ObZoneWeakReadWeight *weight_zone/*NULL*/,
    const bool is_proxy_mysql_client/*false*/,
    const omt::ObTargetReplicaType *target_replica_type/*NULL*/)
{
  int ret = OB_SUCCESS;
  entry_need_update = false;
  common::ModulePageAllocator *allocator = NULL;
  if (OB_UNLIKELY(dummy_ldc.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("dummy_ldc is invaild", K(dummy_ldc), K(ret));
  } else if (OB_FAIL(get_thread_allocator(allocator))) {
    LOG_WDIAG("fail to get_thread_allocator", K(ret));
  } else {
    ObSEArray<ObLDCItem, OB_MAX_LDC_ITEM_COUNT> tmp_item_array(OB_MAX_LDC_ITEM_COUNT, *allocator);
    ObSEArray<ObLDCItem, OB_MAX_LDC_ITEM_COUNT> tmp_weight_zone_item_array(OB_MAX_LDC_ITEM_COUNT, *allocator);
    //mainly used for no-ldc, get random start idx
    const bool is_ldc_used = dummy_ldc.is_ldc_used();
    dummy_ldc.reset_item_status();

    //1. fill tmp_item_array from pl
    if (NULL != pl && pl->is_valid()) {
      const bool default_merging_status = false;
      const bool default_congested_status = false;
      const ObIDCType default_idc_type = SAME_IDC;
      const ObZoneType default_zone_type = ZONE_TYPE_READWRITE;
      ObLDCItem tmp_item;
      bool need_use_it = true;

      for (int64_t i = 0; OB_SUCC(ret) && i < pl->replica_count(); ++i) {
        const ObProxyReplicaLocation &replica = *(pl->get_replica(i));
        tmp_item.reset();
        need_use_it = true;
        for (int64_t j = 0; NULL == tmp_item.replica_ && need_use_it && OB_SUCC(ret) && j < dummy_ldc.item_count_; ++j) {
          ObLDCItem &dummy_item = dummy_ldc.item_array_[j];
          if (dummy_item.is_used_) {
            //continue
          } else if (OB_ISNULL(dummy_item.replica_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("dummy_item is invalid", K(dummy_item), K(j), K(dummy_ldc), K(ret));
          } else if (replica.server_ == dummy_item.replica_->server_) {
            if (!is_weak_read_avail_replica(replica, route_policy, target_replica_type, is_proxy_mysql_client)
                || (is_only_readonly_zone && common::ZONE_TYPE_READONLY != dummy_item.zone_type_)) {
              //do not use id
              need_use_it = false;
            } else {
              tmp_item.set_partition_item(replica, dummy_item);
            }
            dummy_item.is_used_ = true;
          }
        }//end of for

        if (OB_SUCC(ret) && need_use_it) {
          need_use_it = is_in_proxy_primary_zone(replica, ss_info, proxy_primary_zone_name, tmp_item.priority_);
          if (need_use_it
              && WEAKREAD_WEIGHT_LOAD_BALANCE == route_policy
              && OB_NOT_NULL(weight_zone)) {
            need_use_it = is_in_weight_zone(replica, ss_info, *weight_zone, tmp_item.weight_zone_index_);
          }
        }

        if (OB_SUCC(ret) && need_use_it) {
          //can not found
          if (NULL == tmp_item.replica_) {
            if (is_weak_read_avail_replica(replica, route_policy, target_replica_type, is_proxy_mysql_client)) {
              // 如果 table location 不在 dummy entry 里, 需要判断该副本是不是同 IDC/REGION 的
              entry_need_update = check_need_update_entry(replica, dummy_ldc, ss_info, region_names);

              if (is_ldc_used) {
                LOG_WDIAG("fail to find replica in dummy ldc with ldc, maybe someone old, "
                         "do not use it", K(replica));
              } else {
                LOG_WDIAG("fail to find replica in dummy ldc without ldc, maybe someone old, "
                         "continue use it", K(replica));
                tmp_item.set(replica, default_merging_status, default_idc_type, default_zone_type,
                             true, default_congested_status);//without ldc, location will put into same_idc
                if (WEAKREAD_WEIGHT_LOAD_BALANCE == route_policy && OB_NOT_NULL(weight_zone)) {
                  if (OB_FAIL(tmp_weight_zone_item_array.push_back(tmp_item))) {
                  LOG_WDIAG("fail to push_back weight zone target_item", K(tmp_item), K(tmp_weight_zone_item_array), K(ret));
                  }
                } else if (OB_FAIL(tmp_item_array.push_back(tmp_item))) {
                  LOG_WDIAG("fail to push_back target_item", K(tmp_item), K(tmp_item_array), K(ret));
                }
              }
            }
          } else {
            //found it
            if (WEAKREAD_WEIGHT_LOAD_BALANCE == route_policy && OB_NOT_NULL(weight_zone)) {
              if (OB_FAIL(tmp_weight_zone_item_array.push_back(tmp_item))) {
                LOG_WDIAG("fail to push_back weight zone target_item",
                          K(tmp_item), K(tmp_weight_zone_item_array), K(ret));
              }
            } else if (OB_FAIL(tmp_item_array.push_back(tmp_item))) {
              LOG_WDIAG("fail to push_back target_item", K(tmp_item), K(tmp_item_array), K(ret));
            }
          }
        }//OB_SUCC
      }//end of for pl
    }//end of pl

    //2. fill tmp_item_array from dummy entry
    if (OB_SUCC(ret)) {
      const int64_t pl_count = tmp_item_array.count();
      if (pl_count > 1) {
        //shuffle the partition server
        std::random_shuffle(tmp_item_array.begin(), tmp_item_array.end(), dummy_ldc.random_);
      }
      if (tmp_weight_zone_item_array.count() > 1) {
        std::random_shuffle(tmp_weight_zone_item_array.begin(), tmp_weight_zone_item_array.end(), dummy_ldc.random_);
      }
      //mainly used for no-ldc, get random start idx
      const int64_t start_idx = get_first_item_index(dummy_ldc, dummy_ldc.get_tenant_server()->replica_count_);
      int64_t current_idx = 0;
      ObLDCItem tmp_ldc_item;
      for (int64_t j = 0; OB_SUCC(ret) && j < dummy_ldc.item_count_; ++j) {
        current_idx = (is_ldc_used ? ((j + start_idx) % dummy_ldc.item_count_) : j);
        const ObLDCItem &dummy_item = dummy_ldc.item_array_[current_idx];
        int64_t priority = 0;
        int32_t weight_index = 0;
        if (dummy_item.is_used_) {
          //continue
        } else if (!is_weak_read_avail_replica(*dummy_item.replica_, route_policy, target_replica_type, is_proxy_mysql_client)
                   || (is_only_readonly_zone && common::ZONE_TYPE_READONLY != dummy_item.zone_type_)) {
          //do not use id
        } else if (!is_in_proxy_primary_zone(*(dummy_item.replica_), ss_info, proxy_primary_zone_name, priority)) {
          //do not use id
        } else if (WEAKREAD_WEIGHT_LOAD_BALANCE == route_policy
                   && OB_NOT_NULL(weight_zone)
                   && !is_in_weight_zone(*(dummy_item.replica_), ss_info, *weight_zone, weight_index)) {
          //do not use it
        } else {
          tmp_ldc_item.set_non_partition_item(dummy_item);
          tmp_ldc_item.weight_zone_index_ = weight_index;
          tmp_ldc_item.priority_ = priority;
          if (WEAKREAD_WEIGHT_LOAD_BALANCE == route_policy && OB_NOT_NULL(weight_zone)) {
            if (OB_FAIL(tmp_weight_zone_item_array.push_back(tmp_ldc_item))) {
              LOG_WDIAG("fail to push_back weight zone target_item",
                        K(dummy_item), K(tmp_ldc_item), K(tmp_weight_zone_item_array), K(ret));
            }
          } else if (OB_FAIL(tmp_item_array.push_back(tmp_ldc_item))) {
            LOG_WDIAG("fail to push_back target_item", K(dummy_item), K(tmp_ldc_item), K(tmp_item_array), K(ret));
          }
        }
      }//end of for
      // 对proxy_primary_zone，所有副本按照优先级排序
      if (!proxy_primary_zone_name.empty() && tmp_item_array.count() > 1) {
        std::sort(tmp_item_array.begin(), tmp_item_array.end());
      }
    }

    //3. fill ldc_location from tmp_item_array
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ldc_location.set_ldc_location(pl, dummy_ldc, tmp_item_array, NULL, &tmp_weight_zone_item_array, weight_zone))) {
        LOG_WDIAG("fail to set_ldc_location", K(ret));
      } else {
        // target_ldc we should use priority
        ldc_location.sort_by_priority(dummy_ldc.get_safe_snapshot_manager());
      }
    }

    allocator = NULL;
  }
  return ret;
}

int ObLDCLocation::fill_item_array_from_pl(const ObProxyPartitionLocation *pl,
                                           const ObIArray<ObServerStateSimpleInfo> &ss_info,
                                           const ObIArray<ObString> &region_names,
                                           const ObIArray<ObString> &proxy_primary_zone_name,
                                           const bool need_skip_leader_item,
                                           const bool is_only_readwrite_zone,
                                           const bool need_use_dup_replica,
                                           ObLDCLocation &dummy_ldc,
                                           bool &entry_need_update,
                                           ObLDCItem &leader_item,
                                           LdcItemArrayType &tmp_item_array,
                                           const ObRoutePolicyEnum &route_policy)
{
  int ret = OB_SUCCESS;

  const bool default_merging_status = false;
  const bool default_congested_status = false;
  const ObIDCType default_idc_type = SAME_IDC;
  const ObZoneType default_zone_type = ZONE_TYPE_READWRITE;
  const bool is_ldc_used = dummy_ldc.is_ldc_used();
  ObLDCItem tmp_item;
  bool need_use_it = true;

  for (int64_t i = 0; OB_SUCC(ret) && i < pl->replica_count(); ++i) {
    const ObProxyReplicaLocation &replica = *(pl->get_replica(i));
    tmp_item.reset();       // reset each for
    need_use_it = true;
    for (int64_t j = 0; NULL == tmp_item.replica_ && need_use_it && OB_SUCC(ret) && j < dummy_ldc.item_count_; ++j) {
      ObLDCItem &dummy_item = dummy_ldc.item_array_[j];
      if (dummy_item.is_used_) {
        //continue
      } else if (OB_ISNULL(dummy_item.replica_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("dummy_item is invalid", K(dummy_item), K(j), K(dummy_ldc), K(ret));
      } else if (replica.server_ == dummy_item.replica_->server_) {
        if (is_only_readwrite_zone
            && common::ZONE_TYPE_READWRITE != dummy_item.zone_type_
            && !replica.is_leader()) {
          //do not use it
          need_use_it = false;
        } else if (not_allowed_replica_type(replica.get_replica_type(), route_policy)) {
          // replica type logonly, pass it
          need_use_it = false;
        } else {
          tmp_item.set_partition_item(replica, dummy_item);
        }
        dummy_item.is_used_ = true;
      }
    } // for

    if (OB_SUCC(ret) && need_use_it) {
      need_use_it = is_in_proxy_primary_zone(replica, ss_info, proxy_primary_zone_name, tmp_item.priority_);
    }

    if (OB_SUCC(ret) && need_use_it) {
      // not found it
      if (NULL == tmp_item.replica_) {
        // if table location is not in dummy entry, need judge whether it is in the same IDC/REGION
        entry_need_update = check_need_update_entry(replica, dummy_ldc, ss_info, region_names);
        if (replica.is_leader()) {
          LOG_WDIAG("fail to find leader in dummy ldc with ldc, maybe someone old, continue use it",
                   K(replica));
          if (!need_skip_leader_item) {
            leader_item.set(replica, default_merging_status, default_idc_type, default_zone_type,
                          true, default_congested_status);
            // need_use_dup_replica和proxy_primary_zone，强读不先选择leader
            if (need_use_dup_replica || !proxy_primary_zone_name.empty()) {
              leader_item.priority_ = tmp_item.priority_;
              if (OB_FAIL(tmp_item_array.push_back(leader_item))) {
                LOG_WDIAG("fail to push_back leader_item", K(leader_item), K(tmp_item_array), K(ret));
              }
            }
          }
        } else if (is_ldc_used) {
          LOG_WDIAG("fail to find replica in dummy ldc with ldc, maybe someone old, "
                   "do not use it", K(replica));
        } else {
          LOG_WDIAG("fail to find replica in dummy ldc without ldc, maybe someone old, "
                   "continue use it", K(replica));
          tmp_item.set(replica, default_merging_status, default_idc_type, default_zone_type,
                       true, default_congested_status); //without ldc, location will put into same_idc
          if (OB_FAIL(tmp_item_array.push_back(tmp_item))) {
            LOG_WDIAG("fail to push_back target_item", K(tmp_item), K(tmp_item_array), K(ret));
          }
        }
      } else {
        //found it
        if (replica.is_leader()) {
          if (!need_skip_leader_item) {
            leader_item = tmp_item;
            // need_use_dup_replica和proxy_primary_zone，强读不先选择leader
            if (need_use_dup_replica || !proxy_primary_zone_name.empty()) {
              if (OB_FAIL(tmp_item_array.push_back(leader_item))) {
                LOG_WDIAG("fail to push_back leader_item", K(leader_item), K(tmp_item_array), K(ret));
              }
            }
          }
        } else if (OB_FAIL(tmp_item_array.push_back(tmp_item))) {
          LOG_WDIAG("fail to push_back target_item", K(tmp_item), K(tmp_item_array), K(ret));
        }
      }
    } // if
  } // for

  return ret;
}

int ObLDCLocation::fill_primary_zone_item_array(common::ModulePageAllocator *allocator,
                                                obutils::ObClusterResource *cluster_resource,
                                                const ObIArray<ObServerStateSimpleInfo> &ss_info,
                                                const ObString &tenant_name,
                                                ObLDCLocation &dummy_ldc,
                                                LdcItemArrayType &tmp_pz_item_array)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("fill strong read location, primary zone route optimize begin.");

  // get location info from map
  ObLDCItem tmp_pz_item;
  ObLocationTenantInfo *info = NULL;
  if (OB_FAIL(cluster_resource->get_location_tenant_info(tenant_name, info))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      LOG_INFO("no location tenant info, do not update, or no right to visit all_tenant table", K(tenant_name));
    } else {
      LOG_WDIAG("fail to get location tenant info", K(ret));
    }
  } else if (info != NULL) {
    // pz zone name list & weight list
    PrimaryZonePrioArrayType &pz_prio_array = info->primary_zone_prio_array_;
    PrimaryZonePrioWeightArrayType &pz_prio_weight_array = info->primary_zone_prio_weight_array_;

    // zone name match
    if (!pz_prio_array.empty()
        && !pz_prio_weight_array.empty()
        && pz_prio_array.count() == pz_prio_weight_array.count()) {
      // same weight choose
      int64_t i = 0;
      int64_t j = 0;
      int64_t pz_prio_weight_count = pz_prio_weight_array.count();
      while (i < pz_prio_weight_count) {
        j = i;
        do {
          j++;
        } while (j < pz_prio_weight_count && pz_prio_weight_array.at(i) == pz_prio_weight_array.at(j));
        LOG_DEBUG("search in range", K(i), K(j), "weight", pz_prio_weight_array.at(i));

        // find all dummy ldc replica, compared with primary zone name and search in ss info
        ObSEArray<ObLDCItem, OB_MAX_LDC_ITEM_COUNT> tmp_pz_tmp_item_array(OB_MAX_LDC_ITEM_COUNT, *allocator);
        for (int64_t k = i; k < j && k < pz_prio_weight_count; ++k) {
          ObString &each_zone = pz_prio_array.at(k);
          for (int64_t p = 0; p < dummy_ldc.item_count_; ++p) {
            ObLDCItem &dummy_item = dummy_ldc.item_array_[p];
            if (dummy_item.is_used_
                  || not_allowed_replica_type(dummy_item.replica_->get_replica_type(), MAX_ROUTE_POLICY_COUNT)
                  || dummy_item.replica_ == NULL) {  // rep != null ?
                LOG_DEBUG("continue this replica", K(dummy_item));
                continue;
            } else {
              if (is_in_primary_zone(*dummy_item.replica_, ss_info, each_zone)) {
                tmp_pz_item.reset();
                tmp_pz_item.set(*dummy_item.replica_, dummy_item.is_merging_, dummy_item.idc_type_,
                            dummy_item.zone_type_, dummy_item.is_partition_server_, dummy_item.is_force_congested_);
                tmp_pz_tmp_item_array.push_back(tmp_pz_item);
                dummy_item.is_used_ = true;
              }
            }
          } // for
        } // for

        // put tmp_pz to pz
        if (!tmp_pz_tmp_item_array.empty()) {
          std::random_shuffle(tmp_pz_tmp_item_array.begin(), tmp_pz_tmp_item_array.end(), dummy_ldc.random_);
          for (int64_t q = 0; q < tmp_pz_tmp_item_array.count(); ++q) {
            ObLDCItem &tmp_item = tmp_pz_tmp_item_array.at(q);
            tmp_pz_item_array.push_back(tmp_item);
          }
          LOG_DEBUG("random shuffle tmp pz tmp item array, and push to tmp pz item array",
                    K(tmp_pz_tmp_item_array), K(tmp_pz_item_array));
        }

        // at last
        i = j;
      } // while
    } else if (pz_prio_array.empty()
               && pz_prio_weight_array.empty()) {
      LOG_DEBUG("no primary zone for this tenant, route with tenant location cache", K(tenant_name));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected error while check the location tenant info", K(ret), KPC(info));
    }

    // dec ref after use
    info->dec_ref();
  }

  return ret;
}

int ObLDCLocation::set_weight_zone_array(const ObIArray<ObLDCItem> &tmp_weight_zone_item_array,
                                         const omt::ObZoneWeakReadWeight &weight_zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL != all_weight_zone_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexcepted all_weight_zone_array_ is not NULl ", K(ret));
  } else if (OB_ISNULL(all_weight_zone_array_ = op_alloc(ObWeightZoneArray))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc weight zone array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_weight_zone_item_array.count(); ++i) {
      // 获取每个item对应的zone name和value
      const ObLDCItem &weight_item = tmp_weight_zone_item_array.at(i);
      if (OB_UNLIKELY(weight_item.weight_zone_index_ >= weight_zone.zone_array_.count()
                      || weight_item.weight_zone_index_ < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to find weight zone value", "weight index", weight_item.weight_zone_index_,
                  "array size", weight_zone.zone_array_.count(), K(ret));
      } else {
        ObString zone = weight_zone.zone_array_.at(weight_item.weight_zone_index_);
        int64_t value = weight_zone.weight_array_.at(weight_item.weight_zone_index_);
        bool found_zone = false;
        // 将权重副本，添加到all_weight_zone_array_
        for (int64_t j = 0; OB_SUCC(ret) && !found_zone && j < all_weight_zone_array_->count(); ++j) {
          ObWeightZoneItems& weight_zone_item = *all_weight_zone_array_->at(j);
          if (0 == static_cast<ObString>(weight_zone_item.zone_name_).case_compare(zone)) {
            found_zone = true;
            if (OB_FAIL(weight_zone_item.weight_zone_item_array_.push_back(weight_item))) {
              LOG_WDIAG("fail to push back tmp weight zone item", K(ret));
            }
          }
        }
        LOG_DEBUG("add weight zone", "idx", i, "array_count", tmp_weight_zone_item_array.count(),
                  K(found_zone), K(weight_item), K(zone), K(ret));
        // all_weight_zone_array_中不存在此zone，需要创建
        if (OB_SUCC(ret) && !found_zone) {
          ObWeightZoneItems* weight_zone_item = NULL;
          if (OB_ISNULL(weight_zone_item = op_alloc(ObWeightZoneItems))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WDIAG("fail to alloc weight zone item", K(ret));
          } else {
            weight_zone_item->zone_name_.rewrite(zone);
            weight_zone_item->weight_value_ = value;
            if (OB_FAIL(weight_zone_item->weight_zone_item_array_.push_back(tmp_weight_zone_item_array.at(i)))) {
              LOG_WDIAG("fail to push back tmp weight zone item for zone", K(ret));
            } else if (OB_FAIL(all_weight_zone_array_->push_back(weight_zone_item))) {
              LOG_WDIAG("fail to push back tmp weight zone", K(ret));
            } else {
              // 成功后，申请的ObWeightZoneItem置为空，否则中间出现失败，需要释放内存
              weight_zone_item = NULL;
            }
          }
          if (OB_UNLIKELY(NULL != weight_zone_item)) {
            op_free(weight_zone_item);
          }
        }
      }
    }// end of for: add tmp item
  }

  if (OB_SUCC(ret)) {
    all_weight_zone_item_count_ = tmp_weight_zone_item_array.count();
  }

  return ret;
}

int ObLDCLocation::set_ldc_location(const ObProxyPartitionLocation *pl,
                                    const ObLDCLocation &dummy_ldc,
                                    const ObIArray<ObLDCItem> &tmp_item_array,
                                    const ObIArray<ObLDCItem> *tmp_pz_item_array,
                                    const ObIArray<ObLDCItem> *tmp_weight_zone_item_array_ptr,
                                    const omt::ObZoneWeakReadWeight *weight_zone/*NULL*/)
{
  int ret = OB_SUCCESS;
  reset();
  set_idc_name(dummy_ldc.get_idc_name());
  set_use_ldc(dummy_ldc.is_ldc_used());
  set_partition(pl);
  set_tenant_server(dummy_ldc.get_tenant_server());

  if (!tmp_item_array.empty()) {
    const int64_t alloc_size = static_cast<int64_t>(sizeof(ObLDCItem)) * tmp_item_array.count();
    char *item_array_buf = NULL;
    if (OB_ISNULL(item_array_buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem", K(alloc_size), K(ret));
    } else {
      item_count_ = tmp_item_array.count();
      item_array_ = new (item_array_buf) ObLDCItem[item_count_];
      int64_t site_item_count[MAX_IDC_TYPE + 1] = {0};
      for (int64_t i = 0; i < item_count_; ++i) {
        ++site_item_count[tmp_item_array.at(i).idc_type_];
      }
      if (OB_UNLIKELY(0 != site_item_count[MAX_IDC_TYPE])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("MAX_IDC_TYPE should not exist", K(tmp_item_array), K(ret));
      } else {
        site_start_index_array_[0] = 0;
        int64_t site_next_use_index_array[MAX_IDC_TYPE] = {0};
        for (int64_t i = 0; i < MAX_IDC_TYPE; ++i) {
          site_start_index_array_[i + 1] = site_start_index_array_[i] + site_item_count[i];
          site_next_use_index_array[i] = site_start_index_array_[i];
        }
        for (int64_t i = 0; i < item_count_; ++i) {
          const ObLDCItem &item = tmp_item_array.at(i);
          item_array_[site_next_use_index_array[item.idc_type_]] = item;
          site_next_use_index_array[item.idc_type_] += 1;
        }
      }
    }
  }

  if (NULL != tmp_pz_item_array && !tmp_pz_item_array->empty()) {
    const int64_t alloc_size = static_cast<int64_t>(sizeof(ObLDCItem)) * tmp_pz_item_array->count();
    char *pz_item_array_buf = NULL;
    if (OB_ISNULL(pz_item_array_buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem", K(ret), K(alloc_size));
    } else {
      primary_zone_item_count_ = tmp_pz_item_array->count();
      primary_zone_item_array_ = new (pz_item_array_buf) ObLDCItem[primary_zone_item_count_];
      for (int64_t i = 0; i < primary_zone_item_count_; ++i) {
        primary_zone_item_array_[i] = tmp_pz_item_array->at(i);
        LOG_DEBUG("push to pz item", K(i), K(primary_zone_item_array_[i]));
      }
    }
  }

  // 添加每个zone的weight item
  if (OB_SUCC(ret) && NULL != tmp_weight_zone_item_array_ptr
      && !tmp_weight_zone_item_array_ptr->empty() && OB_NOT_NULL(weight_zone)) {
    if (OB_FAIL(set_weight_zone_array(*tmp_weight_zone_item_array_ptr, *weight_zone))) {
      LOG_WDIAG("fail to set weight zone array", K(ret));
    }
  }

  return ret;
}

void ObLDCLocation::sort_by_priority(const ObSafeSnapshotManager &safe_snapshot_mananger,
                                     int64_t start_idx,
                                     int64_t end_idx)
{
  int64_t min_priority = INT64_MAX;
  int64_t cur_priority = INT64_MAX;
  int64_t min_priority_idx = 0;
  ObLDCItem tmp_item;
  // selection sort
  for (int64_t i = start_idx; i < end_idx; ++i) {
    min_priority = get_priority(safe_snapshot_mananger, i);
    min_priority_idx = i;
    // find the min priority and swap
    for (int64_t j = i + 1; j < end_idx; ++j) {
      cur_priority = get_priority(safe_snapshot_mananger, j);
      if (min_priority >  cur_priority) {
        min_priority = cur_priority;
        min_priority_idx = j;
      }
    }
    if (min_priority_idx != i) {
      tmp_item = item_array_[i];
      item_array_[i] = item_array_[min_priority_idx];
      item_array_[min_priority_idx] = tmp_item;
    }
  }
}

inline int64_t ObLDCLocation::get_priority(const ObSafeSnapshotManager &safe_snapshot_mananger,
                                           const int64_t idx)
{
  int64_t priority = INT64_MAX;
  if (OB_ISNULL(item_array_[idx].replica_)) {
    LOG_WDIAG("replica_ should not be null");
  } else {
    ObSafeSnapshotEntry *entry = safe_snapshot_mananger.get(item_array_[idx].replica_->server_);
    if (OB_ISNULL(entry)) {
      LOG_WDIAG("safe snapshot entry should not be null");
    } else {
      priority = entry->get_priority();
    }
  }
  return priority;
}

int ObLDCLocation::get_thread_allocator(common::ModulePageAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  static __thread common::ModulePageAllocator *page_allocator = NULL;
  allocator = NULL;
  if (NULL == page_allocator
      && OB_ISNULL(page_allocator = new (std::nothrow) common::ModulePageAllocator(common::ObModIds::OB_PROXY_LDC_ARRAY))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc arena allocator", K(ret));
  } else {
    allocator = page_allocator;
  }
  return ret;
}

// Refer to check_update_ldc and ldc assign functions
int ObLDCLocation::copy_dummy_ldc(ObLDCLocation &src_dummy_ldc, ObLDCLocation &dest_dummy_ldc)
{
  int ret = OB_SUCCESS;
  int64_t item_count = src_dummy_ldc.item_count_;
  int64_t alloc_size = static_cast<int64_t>(sizeof(ObLDCItem)) * item_count;
  char *item_array_buf = NULL;

  if (OB_UNLIKELY(item_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid src_dummy_ldc", K(src_dummy_ldc), K(ret));
  } else if (OB_ISNULL(item_array_buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem", K(alloc_size), K(item_count), K(ret));
  } else {
    dest_dummy_ldc.reset();
    dest_dummy_ldc.set_tenant_server(src_dummy_ldc.get_tenant_server());
    dest_dummy_ldc.set_safe_snapshot_manager(src_dummy_ldc.get_safe_snapshot_manager());
    dest_dummy_ldc.set_use_ldc(src_dummy_ldc.use_ldc_);
    dest_dummy_ldc.set_idc_name(src_dummy_ldc.get_idc_name());

    dest_dummy_ldc.item_count_ = item_count;
    dest_dummy_ldc.item_array_ = new (item_array_buf) ObLDCItem[item_count];
    int64_t memcpy_size = 0;
    int64_t count = src_dummy_ldc.site_start_index_array_[MAX_IDC_TYPE];
    for (int64_t i = 0; i <= MAX_IDC_TYPE; ++i) {
      dest_dummy_ldc.site_start_index_array_[i] = src_dummy_ldc.site_start_index_array_[i];
    }//end of for
    if (count > 0) {
      memcpy_size = static_cast<int64_t>(sizeof(ObLDCItem)) * count;
      MEMCPY(dest_dummy_ldc.item_array_, src_dummy_ldc.item_array_, memcpy_size);
    }
    LOG_DEBUG("succ ObLDCLocation::copy_dummy_ldc", K(src_dummy_ldc), K(dest_dummy_ldc));
  }//end of else

  return ret;
}

int64_t ObLDCLocation::get_rand_zone_index()
{
  int64_t ret_rand_index = -1;
  int64_t weigth_sum = 0;
  int ret = OB_SUCCESS;
  if (!is_weight_zone_empty()) {
    ObSEArray<int64_t, OB_MAX_ZONE_COUNT> non_zero_zone_index;
    ObSEArray<int64_t, OB_MAX_ZONE_COUNT> zero_zone_index;
    // find available zone
    for (int64_t i = 0; OB_SUCC(ret) && i < all_weight_zone_array_->count(); ++i) {
      bool is_available = false;
      if (OB_NOT_NULL(all_weight_zone_array_->at(i))) {
        is_available = all_weight_zone_array_->at(i)->is_valid();
      }
      if (is_available) {
        int64_t value = all_weight_zone_array_->at(i)->weight_value_;
        if (!value) {
          if (OB_FAIL(zero_zone_index.push_back(i))) {
            LOG_WDIAG("fail to push back zero zone index", K(ret));
          }
        } else if (OB_FAIL(non_zero_zone_index.push_back(i))) {
          LOG_WDIAG("fail to push back non-zero zone index", K(ret));
        } else {
          weigth_sum += value;
        }
      }
    }// end for
    // 1. 存在权重非0的zone：根据权重值的前缀和，按照rand的范围找到zone;
    // 2. 只有权重0的zone：均匀随机一个zone
    LOG_DEBUG("has select available zones", "all_weight_zone_count", all_weight_zone_array_->count(),
              K(non_zero_zone_index.count()), K(zero_zone_index.count()));
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (non_zero_zone_index.count()) {
      if (1 == non_zero_zone_index.count()) {
        ret_rand_index = non_zero_zone_index.at(0);
      } else {
        int64_t rand_index = 0;
        if (OB_FAIL(ObRandomNumUtils::get_random_num(0, weigth_sum - 1, rand_index))) {
          LOG_WDIAG("fail to get random non-zero zone", K(ret));
        } else {
          int64_t prefix_sum = 0;
          for (int i = 0; i < non_zero_zone_index.count(); ++i) {
            int array_index = non_zero_zone_index.at(i);
            prefix_sum += all_weight_zone_array_->at(array_index)->weight_value_;
            if (rand_index < prefix_sum) {
              ret_rand_index = array_index;
              break;
            }
          }
        }
      }
      LOG_DEBUG("found non-zero weight value random zone", K(ret_rand_index));
      if (OB_UNLIKELY(-1 == ret_rand_index)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("can't match zone index, maybe rand result is wrong", K(ret));
      }
    } else if (zero_zone_index.count()) {
      int64_t rand_index = 0;
      if (OB_FAIL(ObRandomNumUtils::get_random_num(0, zero_zone_index.count() - 1, rand_index))) {
        LOG_WDIAG("fail to get random zero zone", K(ret));
      } else {
        ret_rand_index = zero_zone_index.at(rand_index);
      }
    } else {
      LOG_DEBUG("not exist available zone when use weight load banlance");
      // not exist available zone
    }
  }

  return ret_rand_index;
}

int ObLDCLocation::get_server_info(ObMysqlSM &sm,
                                    ObIArray<ObServerStateSimpleInfo> &servers_info)
{
  int ret = OB_SUCCESS;
  const uint64_t ss_version = sm.sm_cluster_resource_->server_state_version_;
  ObIArray<ObServerStateSimpleInfo> &server_state_info =
    sm.sm_cluster_resource_->get_server_state_info(ss_version);
  common::DRWLock &server_state_lock = sm.sm_cluster_resource_->get_server_state_lock(ss_version);
  DRWLock::RDLockGuard guard(server_state_lock);
  if (OB_FAIL(servers_info.assign(server_state_info))) {
    LOG_WDIAG("fail to assign simple_servers_info", K(ret));
  }
  return ret;
}

int ObLDCLocation::get_weight_zone(omt::ObZoneWeakReadWeight &weight_zone, ObString &out_zone)
{
  ObSEArray<int64_t, OB_MAX_ZONE_COUNT> non_zero_zone_index;
  ObSEArray<int64_t, OB_MAX_ZONE_COUNT> zero_zone_index;
  int ret = OB_SUCCESS;
  int64_t weigth_sum = 0;
  for (int64_t i = 0; i < weight_zone.weight_array_.count(); ++i) {
    if (0 == weight_zone.weight_array_.at(i)) {
      if (OB_FAIL(zero_zone_index.push_back(i))) {
        LOG_WDIAG("fail to push back zero zone", K(ret));
      }
    } else if (OB_FAIL(non_zero_zone_index.push_back(i))) {
      LOG_WDIAG("fail to push back non-zero zone", K(ret));
    } else {
      weigth_sum += weight_zone.weight_array_.at(i);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (non_zero_zone_index.count() > 0) {
    int64_t random_num = 0;
    int64_t pre_sum = 0;
    if (OB_FAIL(ObRandomNumUtils::get_random_num(0, weigth_sum - 1, random_num))) {
      LOG_WDIAG("fail to get weigth random number for non-zero zone", K(ret));
    } else {
      for (int i = 0; i < weight_zone.weight_array_.count(); ++i) {
        int64_t array_index = non_zero_zone_index.at(i);
        pre_sum += weight_zone.weight_array_.at(array_index);
        if (pre_sum > random_num) {
          out_zone = weight_zone.zone_array_.at(array_index);
          break;
        }
      }
    }
  } else if (zero_zone_index.count() > 0) {
    int64_t random_num = 0;
    if (OB_FAIL(ObRandomNumUtils::get_random_num(0, zero_zone_index.count() - 1, random_num))) {
      LOG_WDIAG("fail to get weigth random number for zero zone", K(ret));
    } else {
      int64_t array_index = zero_zone_index.at(random_num);
      out_zone = weight_zone.zone_array_.at(array_index);
    }
  } else {
    LOG_DEBUG("not exist weight zone");
  }

  return ret;
}

bool ObLDCLocation::is_in_same_zone(const net::ObIpEndpoint &addr,
                     const ObIArray<ObServerStateSimpleInfo> &server_info,
                     const ObString &zone)
{
  bool bret = false;
  if (server_info.empty()) {
    LOG_DEBUG("server_info is empty", K(addr), K(zone));
  } else {
    bool found = false;
    ObAddr addr_ip;
    addr_ip.set_ip_from_ip_addr(net::ObIpAddr(addr));
    addr_ip.port_ = addr.get_port_host_order();
    // 找到对应的副本的 zone 信息
    for (int64_t j = 0; !found && j < server_info.count(); j++) {
      const ObServerStateSimpleInfo &ss = server_info.at(j);
      if (ss.addr_ == addr_ip) {
        found = true;
        bret = (0 == ss.zone_name_.case_compare(zone));
      }
    }
  }

  return bret;
}
int ObLDCLocation::get_route_info(const ObRoutePolicyEnum &policy,
                                   ObMysqlSM &sm,
                                   omt::ObTargetReplicaType &target_replica_type,
                                   ObString &zone,
                                   ObIArray<ObServerStateSimpleInfo> &server_info)
{
  int ret = OB_SUCCESS;
  if (is_weight_load_balance_route(policy)) {
    if (OB_FAIL(get_server_info(sm, server_info))) {
      LOG_WDIAG("fail to get server info", K(ret));
    } else if (OB_FAIL(get_weight_zone(sm.multi_level_config_->weakread_weight_zone_, zone))) {
      LOG_WDIAG("fail to get weigth zone for SingleLeader", K(ret));
    } else {
      target_replica_type.set_all_weakread_replica();
    }
  } else if (is_target_replica_route(policy)) {
    target_replica_type = sm.multi_level_config_->route_target_replica_type_;
  } else {
    // 非权重/指定副本类型，兼容老的行为，发给F/R
    target_replica_type.set_full_replica();
    target_replica_type.set_readonly_replica();
  }
  return ret;
}

bool ObLDCLocation::is_target_replica_type(const omt::ObTargetReplicaType &target_replica_type,
                                            const ObReplicaType &replica_type)
{
  bool bret = false;
  switch (replica_type) {
    case REPLICA_TYPE_FULL:
      bret = target_replica_type.is_exist_full_replica();
      break;
    case REPLICA_TYPE_READONLY:
      bret = target_replica_type.is_exist_readonly_replica();
      break;
    case REPLICA_TYPE_COLUMNSTORE:
      bret = target_replica_type.is_exist_column_store_replica();
      break;
    default:
      break;
  }
  return bret;
}

ObIDCType ObLDCLocation::get_idc_type(const ObAddr &ip, const ObLDCLocation &dummy_ldc)
{
  ObIDCType ret = SAME_IDC;
  int64_t item_count = dummy_ldc.get_item_count();
  for (int64_t item_idx = 0; item_idx < item_count; item_idx++) {
    const ObLDCItem *item = dummy_ldc.get_item(item_idx);
    if (OB_NOT_NULL(item) && OB_NOT_NULL(item->replica_)
        && item->replica_->server_ == ip) {
      ret = item->idc_type_;
      break;
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
