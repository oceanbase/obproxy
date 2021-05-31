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

#ifndef OCEANBASE_SHARE_HEARTBEAT_OB_LEASE_STRUCT_H_
#define OCEANBASE_SHARE_HEARTBEAT_OB_LEASE_STRUCT_H_

#include "lib/ob_define.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/net/ob_addr.h"
#include "common/ob_zone.h"

namespace oceanbase
{
namespace share
{

struct ObServerResourceInfo
{
  OB_UNIS_VERSION(1);
public:
  double cpu_;
  int64_t mem_in_use_;                   // in KB
  int64_t mem_total_;
  int64_t disk_in_use_;
  int64_t disk_total_;

  ObServerResourceInfo();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(cpu), K_(mem_in_use), K_(mem_total), K_(disk_in_use), K_(disk_total));
};

struct ObLeaseRequest
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t LEASE_VERSION = 1;
  int64_t version_;
  common::ObZone zone_;
  common::ObAddr server_;
  int64_t inner_port_;                   // mysql listen port
  char build_version_[common::OB_SERVER_VERSION_LENGTH];
  ObServerResourceInfo resource_info_;
  int64_t start_service_time_;

  ObLeaseRequest();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(version), K_(zone), K_(server), K_(inner_port), K_(build_version),
      K_(resource_info), K_(start_service_time));
};

struct ObLeaseResponse
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t LEASE_VERSION = 1;
  int64_t version_;
  int64_t lease_expire_time_;
  int64_t lease_info_version_;           // check whether need to update info from __all_zone_stat
  int64_t frozen_version_;
  int64_t schema_version_;
  uint64_t server_id_;

  ObLeaseResponse();
  TO_STRING_KV(K_(version), K_(lease_expire_time), K_(lease_info_version),
      K_(frozen_version), K_(schema_version), K_(server_id));

  void reset();
  bool is_valid() const;
};

struct ObZoneLeaseInfo
{
  common::ObZone zone_;
  int64_t privilege_version_;
  int64_t config_version_;
  int64_t lease_info_version_;
  int64_t broadcast_version_;
  int64_t last_merged_version_; // zone merged version
  int64_t global_last_merged_version_; // cluster merged version
  bool suspend_merging_;

  ObZoneLeaseInfo(): zone_(), privilege_version_(0), config_version_(0),
    lease_info_version_(0), broadcast_version_(0), last_merged_version_(0),
    global_last_merged_version_(0), suspend_merging_(false) {}
  TO_STRING_KV(K_(zone), K_(privilege_version), K_(config_version), K_(lease_info_version),
      K_(broadcast_version), K_(last_merged_version), K_(global_last_merged_version),
      K_(suspend_merging));

  bool is_valid() const;
};

} // end namespace share
} // end namespace oceanbase
#endif
