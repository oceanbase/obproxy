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
#include "obutils/ob_single_leader.h"
#include "obutils/ob_resource_pool_processor.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "proxy/route/ob_ldc_location.h"
namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
namespace obutils
{
/*
  1. 获取单机模式下的副本，以idc优先级依次获取：same_idc > same_region > others
  2. 对权重路由和TARGET_REPLICA_TYPE_WITH_LEADER可以选取leader副本
     2.1 权重路由先根据权重随机一个zone，再判断副本是否在对应的zone内；
     2.2 WITH_LEADER的leader优先级=follower;
     2.3 当route_leader=true时，表示此时选择leader副本
*/
const net::ObIpEndpoint *ObSingleLeader::get_replica(const ObRoutePolicyEnum& policy,
                                                      ObMysqlSM& sm)
{
  const net::ObIpEndpoint *ret_ip = NULL;
  // 对follower优先的路由策略，只获取follower副本，如果获取不到，会在handle_pl_lookup中获取其leader
  // 对leader、follower优先级相同的路由，在random % (total_cnt) == (total_cnt - 1)，一定路由到leader
  if (OB_NOT_NULL(single_leader_info_)) {
    LOG_DEBUG("single leaders all followers", K(single_leader_info_->followers_));
    const static ObIDCType idc_type_priority[3] { SAME_IDC, SAME_REGION, OTHER_REGION };
    int64_t follower_cnt = single_leader_info_->followers_.count();
    bool found = false;
    int64_t random = 0;
    int ret = OB_SUCCESS;
    ObRandomNumUtils::get_random_num(0, 100, random);
    ObString zone;
    ObSEArray<ObServerStateSimpleInfo, ObServerStateRefreshCont::DEFAULT_SERVER_COUNT> simple_servers_info(ObServerStateRefreshCont::DEFAULT_SERVER_COUNT);
    omt::ObTargetReplicaType target_replica_type;
    if (OB_FAIL(ObLDCLocation::get_route_info(policy, sm, target_replica_type, zone, simple_servers_info))) {
      LOG_WDIAG("fail to get route info", K(ret));
    } else {
      const bool follower_only = is_follower_only_route(policy) || (is_target_replica_route(policy) && target_replica_type.is_exist_column_store_replica());
      const int64_t total_cnt = follower_only ? follower_cnt : follower_cnt + 1;
      for (int64_t idc_idx = 0; OB_SUCC(ret) && idc_idx < ARRAYSIZEOF(idc_type_priority) && !found; idc_idx++) {
        for (int64_t check_cnt = 0; check_cnt < total_cnt && !found; random++, check_cnt++) {
          // 允许发leader时，random%total_cnt == total_cnt - 1时发往leader
          bool route_leader = !follower_only && (random % total_cnt == (total_cnt - 1));
          int64_t follower_idx = random % total_cnt;
          const net::ObIpEndpoint &addr = route_leader ? single_leader_info_->leader_addr_ : single_leader_info_->followers_.at(follower_idx).addr_;
          const ObIDCType idc_type = route_leader ? single_leader_idc_ : single_leader_followers_idc_[follower_idx];
          const ObReplicaType replica_type = route_leader ? REPLICA_TYPE_FULL : single_leader_info_->followers_.at(follower_idx).replica_type_;
          LOG_DEBUG("check single leader's follower", K(addr), "idc_type", get_idc_type_string(idc_type_priority[idc_idx]),
                    "route policy", get_route_policy_enum_string(policy), "target_replica_type", target_replica_type.replica_type_, K(random), K(follower_cnt), K(route_leader));
          if (idc_type != idc_type_priority[idc_idx]) {
            LOG_DEBUG("not match idc type", "replica idc", get_idc_type_string(idc_type), "excepted idc_type", get_idc_type_string(idc_type_priority[idc_idx]));
          } else if (!route_leader && !ObLDCLocation::is_target_replica_type(target_replica_type, replica_type)) {
            LOG_DEBUG("not match replica type", "replica type", ObProxyReplicaLocation::get_replica_type_string(replica_type));
          } else if (is_weight_load_balance_route(policy) && !ObLDCLocation::is_in_same_zone(addr, simple_servers_info, zone)) {
            LOG_DEBUG("follower not in weight zone ", K(zone), K(route_leader), K(addr));
          } else if ((found = addr.is_valid())) {
            ret_ip = &addr;
            LOG_DEBUG("succ to found leader's best follower", K(route_leader), K(addr), K(idc_type));
          } else {
            LOG_DEBUG("idc_type and replica_type matched but not an valid addr", K(route_leader), K(addr));
          }
        } // end of check_cnt
      } // end of idc_type
    }
  }

  return ret_ip;
}

int ObSingleLeader::refresh(
    ObClusterResource &cluster_resource,
    const ObString &tenant_name,
    const ObLDCLocation &dummy_ldc)
{
  int ret = OB_SUCCESS;
  int64_t version = cluster_resource.get_single_leader_map_version();
  // 1. version changed
  if (OB_UNLIKELY(version != single_leader_version_)) {
    single_leader_version_ = version;
    if (OB_FAIL(cluster_resource.get_and_update_single_leader_info(tenant_name, single_leader_info_))) {
      LOG_DEBUG("fail to get single leader", K(tenant_name), K(ret));
    } else {
      int64_t item_count = dummy_ldc.get_item_count();
      int64_t follower_count = single_leader_info_->followers_.count();
      LOG_DEBUG("tenant has single leader", K(tenant_name), K(item_count), K(follower_count));

      // size not matches, free the old memory
      if (OB_UNLIKELY(follower_count != single_leader_followers_count_)) {
        if (OB_NOT_NULL(single_leader_followers_idc_)) {
          op_fixed_mem_free(single_leader_followers_idc_, sizeof(ObIDCType) * single_leader_followers_count_);
          single_leader_followers_idc_ = NULL;
        }
        single_leader_followers_count_ = 0;
      }
      if (OB_ISNULL(single_leader_followers_idc_)
          && OB_ISNULL(single_leader_followers_idc_
              = static_cast<ObIDCType*>(op_fixed_mem_alloc(sizeof(ObIDCType) * follower_count)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc memory for followers idc", K(ret));
      } else {
        // idc赋值
        ObAddr leader_addr;
        leader_addr.set_ip_from_ip_addr(net::ObIpAddr(single_leader_info_->leader_addr_));
        leader_addr.port_ = single_leader_info_->leader_addr_.get_port_host_order();
        single_leader_idc_ = ObLDCLocation::get_idc_type(leader_addr, dummy_ldc);
        single_leader_followers_count_ = follower_count;
        for (int64_t follower_idx = 0; follower_idx < follower_count; follower_idx++) {
          net::ObIpAddr follower(single_leader_info_->followers_.at(follower_idx).addr_);
          ObAddr follower_addr;
          follower_addr.set_ip_from_ip_addr(follower);
          follower_addr.port_ = single_leader_info_->followers_.at(follower_idx).addr_.get_port_host_order();
          single_leader_followers_idc_[follower_idx] = ObLDCLocation::get_idc_type(follower_addr, dummy_ldc);
        } // end for
      } // end else
    }
  }

  return ret;
}

int64_t ObSingleLeader::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  if (OB_NOT_NULL(single_leader_info_)) {
    J_KV("tenant", single_leader_info_->tenant_name_,
         "leader", single_leader_info_->leader_addr_,
         "followers", single_leader_info_->followers_);
  } else {
    J_KV(K(single_leader_info_));
    J_COMMA();
    const ObString idc = get_idc_type_string(single_leader_idc_);
    BUF_PRINTF("leader.idc_type=%.*s", idc.length(), idc.ptr());
  }
  J_COMMA();
  if (OB_NOT_NULL(single_leader_followers_idc_)) {
    for (int i = 0; i < single_leader_followers_count_; i++) {
      const ObString idc = get_idc_type_string(single_leader_followers_idc_[i]);
      BUF_PRINTF("followers[%d].idc_type=%.*s", i, idc.length(), idc.ptr());
      if (i < single_leader_followers_count_) {
        J_COMMA();
      }
    }
  }
  J_KV(K(single_leader_version_));
  J_OBJ_END();
  return pos;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase