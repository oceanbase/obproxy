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

#include "ob_lease_struct.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_serialization_helper.h"
namespace oceanbase
{
using namespace common;
namespace share
{

ObServerResourceInfo::ObServerResourceInfo()
{
  reset();
}

void ObServerResourceInfo::reset()
{
  cpu_ = 0;
  mem_in_use_ = 0;
  mem_total_ = 0;
  disk_in_use_ = 0;
  disk_total_ = 0;
}

bool ObServerResourceInfo::is_valid() const
{
  return cpu_ > 0 && mem_in_use_ >= 0 && mem_total_ > 0
      && disk_in_use_ >= 0 && disk_total_ > 0;
}

OB_SERIALIZE_MEMBER(ObServerResourceInfo,
                    cpu_,
                    mem_in_use_,
                    mem_total_,
                    disk_in_use_,
                    disk_total_);

ObLeaseRequest::ObLeaseRequest()
{
  reset();
}

void ObLeaseRequest::reset()
{
  version_ = LEASE_VERSION;
  zone_.reset();
  server_.reset();
  memset(build_version_, 0, OB_SERVER_VERSION_LENGTH);
  inner_port_ = 0;
  resource_info_.reset();
  start_service_time_ = 0;
}

bool ObLeaseRequest::is_valid() const
{
  return version_ > 0 && !zone_.is_empty() && server_.is_valid()
      && inner_port_ > 0 && resource_info_.is_valid() && start_service_time_ >= 0;
}

OB_SERIALIZE_MEMBER(ObLeaseRequest,
                    version_,
                    zone_,
                    server_,
                    inner_port_,
                    build_version_,
                    resource_info_,
                    start_service_time_);

ObLeaseResponse::ObLeaseResponse()
{
  reset();
}

void ObLeaseResponse::reset()
{
  version_ = LEASE_VERSION;
  lease_expire_time_ = -1;
  lease_info_version_ = 0;
  frozen_version_ = 0;
  schema_version_ = 0;
  server_id_ = OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObLeaseResponse,
                    version_,
                    lease_expire_time_,
                    lease_info_version_,
                    frozen_version_,
                    schema_version_,
                    server_id_);


bool ObLeaseResponse::is_valid() const
{
  // other member may be invalid value while RS restart.
  return version_ > 0 && lease_expire_time_ > 0 && schema_version_ > 0;
}

bool ObZoneLeaseInfo::is_valid() const
{
  return !zone_.is_empty() && privilege_version_ >= 0 && config_version_ >= 0
      && lease_info_version_ >= 0 && broadcast_version_ > 0 && last_merged_version_ > 0
      && global_last_merged_version_ > 0;
}

} // end namespace share
} // end namespace oceanbase
