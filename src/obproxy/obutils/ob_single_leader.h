/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_SINGLE_LEADER_H_
#define OB_SINGLE_LEADER_H_
#include "lib/ob_define.h"
#include "iocore/net/ob_inet.h"
#include "proxy/route/ob_ldc_struct.h"
#include "proxy/route/ob_ldc_location.h"
#include "obutils/ob_resource_pool_processor.h"
namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
namespace obutils
{
class ObSingleLeader
{
public:
  ObSingleLeader() : single_leader_info_(NULL),
                     single_leader_followers_idc_(NULL),
                     single_leader_idc_(SAME_IDC),
                     single_leader_followers_count_(0),
                     single_leader_version_(0) {}
  ~ObSingleLeader() {
    DEC_SHARED_REF(single_leader_info_);
    if (OB_NOT_NULL(single_leader_followers_idc_)) {
      op_fixed_mem_free(single_leader_followers_idc_, sizeof(ObIDCType) * single_leader_followers_count_);
      single_leader_followers_idc_ = NULL;
      single_leader_followers_count_ = 0;
    }
  }

  int refresh(obutils::ObClusterResource &cluster_resource,
              const common::ObString &tenant_name,
              const ObLDCLocation &dummy_ldc);

  const net::ObIpEndpoint *get_replica(const ObRoutePolicyEnum& policy, ObMysqlSM& sm);

  inline const net::ObIpEndpoint *get_leader() { return OB_NOT_NULL(single_leader_info_) ? &single_leader_info_->leader_addr_ : NULL; }
  const bool need_refresh(int64_t new_version) { return single_leader_version_ != new_version; }
  void set_single_leader_version(int64_t new_version) { single_leader_version_ = new_version; }
  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  ObTenantSingleLeaderInfo *single_leader_info_;
  // followers' idc depends on proxy_idc_name which is an vip level config
  ObIDCType *single_leader_followers_idc_;
  ObIDCType single_leader_idc_;
  int64_t single_leader_followers_count_;
  int64_t single_leader_version_;
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif