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

#include "obutils/ob_vip_tenant_processor.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

ObVipTenantProcessor::ObVipTenantProcessor()
  : is_inited_(false)
{
}

void ObVipTenantProcessor::destroy()
{
  if (OB_LIKELY(is_inited_)) {
    is_inited_ = false;
    vt_cache_.destroy();
  }
}

int ObVipTenantProcessor::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObVipTenantProcessor::get_vip_tenant(ObVipTenant &vip_tennat)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(vt_cache_.get(vip_tennat.vip_addr_, vip_tennat))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("vip tenant not in cache", "vip_addr", vip_tennat.vip_addr_, K(ret));
    } else {
      LOG_INFO("fail to get vip tenant in cache", "vip_addr", vip_tennat.vip_addr_, K(ret));
    }
  } else {
    LOG_DEBUG("succ to get get vip tenant in cache", K(vip_tennat));
  }
  return ret;
}

ObVipTenantProcessor &get_global_vip_tenant_processor()
{
  static ObVipTenantProcessor vt_processor;
  return vt_processor;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
