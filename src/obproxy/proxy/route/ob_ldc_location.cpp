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

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
void ObLDCLocation::reset_item_array()
{
  if (NULL != item_array_ && item_count_ > 0) {
    op_fixed_mem_free(item_array_, static_cast<int64_t>(sizeof(ObLDCItem)) * item_count_);
  }
  item_array_ = NULL;
  item_count_ = 0;
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
      LOG_WARN("fail to push back region_name", K(region_name), K(ret));
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
          LOG_WARN("fail to add unique region name", K(zone_region_string), K(ret));
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
            LOG_WARN("fail to add unique region name", K(zone_region_string), K(ret));
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
        LOG_WARN("fail to add unique region name", K(zone_region_string), K(ret));
      } else if (!region_name_from_idc_list.empty()) {
        if (OB_FAIL(add_unique_region_name(region_name_from_idc_list.name_string_, region_names))) {
          LOG_WARN("fail to add unique region name", K(zone_region_string), K(ret));
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
      PROXY_LOG(WARN, "fail to get random num", K(partition_count), K(ret));
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
    LOG_WARN("argument is invalid", K(idc_name), KPC(ts), K(ss_info), K(ret));
  } else if (OB_FAIL(get_thread_allocator(allocator))) {
    LOG_WARN("fail to get_thread_allocator", K(ret));
  } else {
    reset();
    set_tenant_server(ts);
    ObSEArray<ObString, 5> region_names(5, *allocator);
    ObProxyNameString region_name_from_idc_list;

    ObRegionMatchedType match_type = MATCHED_BY_NONE;
    if (OB_FAIL(ObLDCLocation::get_region_name(ss_info, idc_name, cluster_name, cluster_id,
                                               region_name_from_idc_list,
                                               match_type, region_names))) {
      LOG_WARN("fail to get region name", K(idc_name), K(ret));
    } else {
      if (idc_name.empty()) {
        //do nothing
      } else if (region_names.empty()) {
        set_idc_name(idc_name);
        LOG_WARN("can not find region name, maybe set error idc name, treat as do not use ldc",
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
                    LOG_WARN("failed to push back same_idc tmp_item_array", K(i), K(item), K(local_item_array), K(ret));
                  }
                } else {
                  const ObLDCItem item(replica, ss.is_merging_, SAME_REGION, ss.zone_type_, ss.is_force_congested_);
                  if (OB_FAIL(region_item_array.push_back(item))) {
                    LOG_WARN("failed to push back same_region tmp_item_array", K(i), K(item), K(region_item_array), K(ret));
                  }
                }
              } else {
                const ObLDCItem item(replica, ss.is_merging_, OTHER_REGION, ss.zone_type_, ss.is_force_congested_);
                if (OB_FAIL(remote_item_array.push_back(item))) {
                  LOG_WARN("failed to push back other_region tmp_item_array", K(i), K(item), K(remote_item_array), K(ret));
                }
              }
            } else {
              const ObLDCItem item(replica, ss.is_merging_, SAME_IDC, ss.zone_type_, ss.is_force_congested_);
              if (OB_FAIL(local_item_array.push_back(item))) {
                LOG_WARN("failed to push back same_idc tmp_item_array", K(i), K(item), K(local_item_array), K(ret));
              }
            }
          }//end of found server
        }//end of for ss_info
        if (OB_SUCC(ret) && !found) {
          if (is_base_servers_added && !replica.server_.is_ip_loopback()) {
            LOG_WARN("fail to find tenant server from server list, maybe has not updated, don not use it", K(replica));
          } else {
            // if relica has no IDC info, lower priority. Avoid choosing offline relica
            if (is_ldc_used()) {
              const ObLDCItem item(replica, default_merging_status, OTHER_REGION, default_zone_type, default_congested_status);
              if (OB_FAIL(remote_item_array.push_back(item))) {
                LOG_WARN("failed to push back same_idc tmp_item_array", K(i), K(item), K(local_item_array), K(ret));
              }
            } else {
              const ObLDCItem item(replica, default_merging_status, default_idc_type, default_zone_type, default_congested_status);
              if (OB_FAIL(local_item_array.push_back(item))) {
                LOG_WARN("failed to push back same_idc tmp_item_array", K(i), K(item), K(local_item_array), K(ret));
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
          LOG_WARN("fail to find any tenant server from server list", KPC(ts), K(ss_info), K(item_count), K(ret));
        } else if (OB_ISNULL(item_array_buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", K(alloc_size), K(item_count), K(ret));
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
    for (int64_t j = 0; !found && j < ss_info.count(); j++) {
      const ObServerStateSimpleInfo &ss = ss_info.at(j);
      if (ss.addr_ == replica.server_) {
        found = true;
        // same region or not R relica, need refresh
        if (is_in_logic_region(region_names, ss.region_name_) || REPLICA_TYPE_READONLY != replica.replica_type_) {
          bret = true;
          LOG_WARN("check_need_update_entry, same region or not readonly replica, need update entry", K(replica), K(ss), K(region_names));
        } else {
          LOG_DEBUG("check_need_update_entry, other region and readonly replica, do not need update entry", K(replica), K(ss), K(region_names));
        }
      }
    }

    if (!found) {
      bret = true;
      LOG_WARN("check_need_update_entry, replica is not the cluster's servers, need update entry", K(replica));
    }
  } else {
    // if no ldc, have two case:
    //   1. not set idc name
    //   2. region name is empty
    // No way to know if this relica is the same as IDC/REGION, need fresh
    bret = true;
    LOG_WARN("check_need_update_entry, not use ldc, need update entry", K(replica));
  }

  return bret;
}

bool ObLDCLocation::is_in_primary_zone(const ObProxyReplicaLocation &replica,
                                       const ObIArray<ObServerStateSimpleInfo> &ss_info,
                                       const ObString &proxy_primary_zone_name)
{
  bool need_use_it = false;

  // if have primary zone and zone state, route by zone
  if (!proxy_primary_zone_name.empty() && ss_info.count() > 0) {
    bool found = false;
    need_use_it = false;
    for (int64_t j = 0; !found && j < ss_info.count(); j++) {
      const ObServerStateSimpleInfo &ss = ss_info.at(j);
      if (ss.addr_ == replica.server_) {
        found = true;
        if (0 == ss.zone_name_.case_compare(proxy_primary_zone_name)) {
          need_use_it = true;
        }
      }
    }
  } else {
    need_use_it = true;
  }

  return need_use_it;
}

int ObLDCLocation::fill_strong_read_location(const ObProxyPartitionLocation *pl,
    ObLDCLocation &dummy_ldc, ObLDCItem &leader_item, ObLDCLocation &ldc_location,
    bool &entry_need_update, const bool is_only_readwrite_zone, const bool need_use_dup_replica,
    const bool need_skip_leader_item,
    const ObIArray<ObServerStateSimpleInfo> &ss_info,
    const ObIArray<ObString> &region_names,
    const ObString &proxy_primary_zone_name)
{
  int ret = OB_SUCCESS;
  entry_need_update = false;
  common::ModulePageAllocator *allocator = NULL;
  if (OB_UNLIKELY(dummy_ldc.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dummy_ldc is empty", K(dummy_ldc), K(ret));
  } else if (OB_FAIL(get_thread_allocator(allocator))) {
    LOG_WARN("fail to get_thread_allocator", K(ret));
  } else {
    ObSEArray<ObLDCItem, OB_MAX_LDC_ITEM_COUNT> tmp_item_array(OB_MAX_LDC_ITEM_COUNT, *allocator);
    const bool is_ldc_used = dummy_ldc.is_ldc_used();
    dummy_ldc.reset_item_status();
    leader_item.reset();

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
            LOG_WARN("dummy_item is invalid", K(dummy_item), K(j), K(dummy_ldc), K(ret));
          } else if (replica.server_ == dummy_item.replica_->server_) {
            if (is_only_readwrite_zone
                && common::ZONE_TYPE_READWRITE != dummy_item.zone_type_
                && !replica.is_leader()) {
              //do not use it
              need_use_it = false;
            } else if (REPLICA_TYPE_LOGONLY == replica.get_replica_type()
                       || REPLICA_TYPE_ENCRYPTION_LOGONLY == replica.get_replica_type()) {
              // log relica, skip
              need_use_it = false;
            } else {
              tmp_item.set_partition_item(replica, dummy_item);
            }
            dummy_item.is_used_ = true;
          }
        }//end of for

        if (OB_SUCC(ret) && need_use_it) {
          need_use_it = is_in_primary_zone(replica, ss_info, proxy_primary_zone_name);
        }

        if (OB_SUCC(ret) && need_use_it) {
          //not found it
          if (NULL == tmp_item.replica_) {
            // if relica not in dummy entry, need check whether relica is same IDC or Region
            entry_need_update = check_need_update_entry(replica, dummy_ldc, ss_info, region_names);

            if (replica.is_leader()) {
              LOG_WARN("fail to find leader in dummy ldc with ldc, maybe someone old, continue use it",
                       K(replica));
              if (!need_skip_leader_item) {
                leader_item.set(replica, default_merging_status, default_idc_type, default_zone_type,
                              true, default_congested_status);
                if (need_use_dup_replica) {
                  if (OB_FAIL(tmp_item_array.push_back(leader_item))) {
                    LOG_WARN("fail to push_back leader_item", K(leader_item), K(tmp_item_array), K(ret));
                  }
                }
              }
            } else if (is_ldc_used) {
              LOG_WARN("fail to find replica in dummy ldc with ldc, maybe someone old, "
                       "do not use it", K(replica));
            } else {
              LOG_WARN("fail to find replica in dummy ldc without ldc, maybe someone old, "
                       "continue use it", K(replica));
              tmp_item.set(replica, default_merging_status, default_idc_type, default_zone_type,
                           true, default_congested_status);//without ldc, location will put into same_idc
              if (OB_FAIL(tmp_item_array.push_back(tmp_item))) {
                LOG_WARN("fail to push_back target_item", K(tmp_item), K(tmp_item_array), K(ret));
              }
            }
          } else {
            //found it
            if (replica.is_leader()) {
              if (!need_skip_leader_item) {
                leader_item = tmp_item;
                if (need_use_dup_replica) {
                  if (OB_FAIL(tmp_item_array.push_back(leader_item))) {
                    LOG_WARN("fail to push_back leader_item", K(leader_item), K(tmp_item_array), K(ret));
                  }
                }
              }
            } else if (OB_FAIL(tmp_item_array.push_back(tmp_item))) {
              LOG_WARN("fail to push_back target_item", K(tmp_item), K(tmp_item_array), K(ret));
            }
          }
        }//OB_SUCC
      }//end of for pl
    }//end of pl

    //3. fill tmp_item_array from dummy entry
    if (OB_SUCC(ret)) {
      if (tmp_item_array.count() > 1) {
        //shuffle the partition server
        std::random_shuffle(tmp_item_array.begin(), tmp_item_array.end(), dummy_ldc.random_);
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < dummy_ldc.item_count_; ++j) {
        const ObLDCItem &dummy_item = dummy_ldc.item_array_[j];
        // skip log relica
        if (dummy_item.is_used_
            || REPLICA_TYPE_LOGONLY == dummy_item.replica_->get_replica_type()
            || REPLICA_TYPE_ENCRYPTION_LOGONLY == dummy_item.replica_->get_replica_type()) {
          //continue
        } else if (is_only_readwrite_zone && common::ZONE_TYPE_READWRITE != dummy_item.zone_type_) {
          //do not use id
        } else if (!is_in_primary_zone(*(dummy_item.replica_), ss_info, proxy_primary_zone_name)) {
          //do not use id
        } else if (OB_FAIL(tmp_item_array.push_back(dummy_item))) {
          LOG_WARN("fail to push_back target_item", K(dummy_item), K(tmp_item_array), K(ret));
        }//no need set is_used_= true
      }//end of for
    }

    //3. fill tenant_ldc without leader
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ldc_location.set_ldc_location(pl, dummy_ldc, tmp_item_array))) {
        LOG_WARN("fail to set_ldc_location", K(ret));
      }
    }
    allocator = NULL;
  }
  return ret;
}

int ObLDCLocation::fill_weak_read_location(const ObProxyPartitionLocation *pl,
    ObLDCLocation &dummy_ldc, ObLDCLocation &ldc_location, bool &entry_need_update,
    const bool is_only_readonly_zone,
    const ObIArray<ObServerStateSimpleInfo> &ss_info,
    const ObIArray<ObString> &region_names,
    const ObString &proxy_primary_zone_name)
{
  int ret = OB_SUCCESS;
  entry_need_update = false;
  common::ModulePageAllocator *allocator = NULL;
  if (OB_UNLIKELY(dummy_ldc.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dummy_ldc is invaild", K(dummy_ldc), K(ret));
  } else if (OB_FAIL(get_thread_allocator(allocator))) {
    LOG_WARN("fail to get_thread_allocator", K(ret));
  } else {
    ObSEArray<ObLDCItem, OB_MAX_LDC_ITEM_COUNT> tmp_item_array(OB_MAX_LDC_ITEM_COUNT, *allocator);
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
            LOG_WARN("dummy_item is invalid", K(dummy_item), K(j), K(dummy_ldc), K(ret));
          } else if (replica.server_ == dummy_item.replica_->server_) {
            if (!replica.is_weak_read_avail()
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
          need_use_it = is_in_primary_zone(replica, ss_info, proxy_primary_zone_name);
        }

        if (OB_SUCC(ret) && need_use_it) {
          //can not found
          if (NULL == tmp_item.replica_) {
            if (replica.is_weak_read_avail()) {
              // if relica not in dummy entry, need check whether relica is same IDC or Region
              entry_need_update = check_need_update_entry(replica, dummy_ldc, ss_info, region_names);

              if (is_ldc_used) {
                LOG_WARN("fail to find replica in dummy ldc with ldc, maybe someone old, "
                         "do not use it", K(replica));
              } else {
                LOG_WARN("fail to find replica in dummy ldc without ldc, maybe someone old, "
                         "continue use it", K(replica));
                tmp_item.set(replica, default_merging_status, default_idc_type, default_zone_type,
                             true, default_congested_status);//without ldc, location will put into same_idc
                if (OB_FAIL(tmp_item_array.push_back(tmp_item))) {
                  LOG_WARN("fail to push_back target_item", K(tmp_item), K(tmp_item_array), K(ret));
                }
              }
            }
          } else {
            //found it
            if (OB_FAIL(tmp_item_array.push_back(tmp_item))) {
              LOG_WARN("fail to push_back target_item", K(tmp_item), K(tmp_item_array), K(ret));
            }
          }
        }//OB_SUCC
      }//end of for pl
    }//end of pl

    //2. fill tmp_item_array from dummy entry
    if (OB_SUCC(ret)) {
      if (tmp_item_array.count() > 1) {
        //shuffle the partition server
        std::random_shuffle(tmp_item_array.begin(), tmp_item_array.end(), dummy_ldc.random_);
      }
      //mainly used for no-ldc, get random start idx
      const int64_t start_idx = get_first_item_index(dummy_ldc, dummy_ldc.get_tenant_server()->replica_count_);
      int64_t current_idx = 0;
      for (int64_t j = 0; OB_SUCC(ret) && j < dummy_ldc.item_count_; ++j) {
        current_idx = (is_ldc_used ? ((j + start_idx) % dummy_ldc.item_count_) : j);
        const ObLDCItem &dummy_item = dummy_ldc.item_array_[current_idx];
        if (dummy_item.is_used_) {
          //continue
        } else if (!dummy_item.replica_->is_weak_read_avail()
                   || (is_only_readonly_zone && common::ZONE_TYPE_READONLY != dummy_item.zone_type_)) {
          //do not use id
        } else if (!is_in_primary_zone(*(dummy_item.replica_), ss_info, proxy_primary_zone_name)) {
          //do not use id
        } else if (OB_FAIL(tmp_item_array.push_back(dummy_item))) {
          LOG_WARN("fail to push_back target_item", K(dummy_item), K(tmp_item_array), K(ret));
        }
      }//end of for
    }

    //3. fill ldc_location from tmp_item_array
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ldc_location.set_ldc_location(pl, dummy_ldc, tmp_item_array))) {
        LOG_WARN("fail to set_ldc_location", K(ret));
      } else {
        // target_ldc we should use priority
        ldc_location.sort_by_priority(dummy_ldc.get_safe_snapshot_manager());
      }
    }

    allocator = NULL;
  }
  return ret;
}

int ObLDCLocation::set_ldc_location(const ObProxyPartitionLocation *pl,
    const ObLDCLocation &dummy_ldc, const ObIArray<ObLDCItem> &tmp_item_array)
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
      LOG_WARN("fail to alloc mem", K(alloc_size), K(ret));
    } else {
      item_count_ = tmp_item_array.count();
      item_array_ = new (item_array_buf) ObLDCItem[item_count_];
      int64_t site_item_count[MAX_IDC_TYPE + 1] = {0};
      for (int64_t i = 0; i < item_count_; ++i) {
        ++site_item_count[tmp_item_array.at(i).idc_type_];
      }
      if (OB_UNLIKELY(0 != site_item_count[MAX_IDC_TYPE])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MAX_IDC_TYPE should not exist", K(tmp_item_array), K(ret));
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
    LOG_WARN("replica_ should not be null");
  } else {
    ObSafeSnapshotEntry *entry = safe_snapshot_mananger.get(item_array_[idx].replica_->server_);
    if (OB_ISNULL(entry)) {
      LOG_WARN("safe snapshot entry should not be null");
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
    LOG_WARN("fail to alloc arena allocator", K(ret));
  } else {
    allocator = page_allocator;
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
