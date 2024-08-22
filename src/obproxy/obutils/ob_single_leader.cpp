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
namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
namespace obutils
{
const net::ObIpEndpoint *ObSingleLeader::get_follower()
{
  const net::ObIpEndpoint *ret = NULL;
  if (OB_NOT_NULL(single_leader_info_)) {
    LOG_DEBUG("single leaders all followers", K(single_leader_info_->followers_));
    const static ObReplicaType replica_type_priority[2] { REPLICA_TYPE_FULL, REPLICA_TYPE_READONLY };
    const static ObIDCType idc_type_priority[3] { SAME_IDC, SAME_REGION, OTHER_REGION };
    int64_t follower_cnt = single_leader_info_->followers_.count();
    bool found = false;
    int64_t random = 0;
    ObRandomNumUtils::get_random_num(0, 100, random);
    for (int64_t idc_idx = 0; idc_idx < sizeof(idc_type_priority) && !found; idc_idx++) {
      for (int64_t replica_type_idx = 0; replica_type_idx < sizeof(replica_type_priority) && !found; replica_type_idx++) {
        for (int64_t check_cnt = 0; check_cnt < follower_cnt && !found; random++, check_cnt++) {
          int64_t follower_idx = random % follower_cnt;
          const ObSingleLeadersFollower &follower = single_leader_info_->followers_.at(follower_idx);
          LOG_DEBUG("check single leader's follower", K(follower), "idc_type", get_idc_type_string(idc_type_priority[idc_idx]),
                    "replica_type", ObProxyReplicaLocation::get_replica_type_string(replica_type_priority[replica_type_idx]));
          if (single_leader_followers_idc_[follower_idx] == idc_type_priority[idc_idx]
              && follower.replica_type_ == replica_type_priority[replica_type_idx]) {
            if ((found = follower.addr_.is_valid())) {
              ret = &follower.addr_;
              LOG_DEBUG("succ to found leader's best follower", K(follower));
            } else {
              LOG_DEBUG("idc_type and replica_type matched but not an valid addr", K(follower.addr_));
            }
          }
        } // end of check_cnt
      } // end of replica_type
    } // end of idc_type
  }

  return ret;
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
    if (OB_FAIL(cluster_resource.get_single_leader_info(tenant_name, single_leader_info_))) {
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
        single_leader_followers_count_ = follower_count;
        for (int64_t follower_idx = 0; follower_idx < follower_count; follower_idx++) {
          net::ObIpAddr follower(single_leader_info_->followers_.at(follower_idx).addr_);
          ObAddr follower_addr;
          follower_addr.set_ip_from_ip_addr(follower);
          for (int64_t item_idx = 0; item_idx < item_count; item_idx++) {
            const ObLDCItem *item = dummy_ldc.get_item(item_idx);
            if (OB_NOT_NULL(item) && OB_NOT_NULL(item->replica_) && item->replica_->server_ == follower_addr) {
              single_leader_followers_idc_[follower_idx] = item->idc_type_;
              break;
            } // end if
          } // end for
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