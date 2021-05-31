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

#ifndef OBPROXY_VIP_TENANT_PROCESSOR_H
#define OBPROXY_VIP_TENANT_PROCESSOR_H
#include "obutils/ob_vip_tenant_cache.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
}
namespace obutils
{
class ObVipTenantProcessor
{
public:
  ObVipTenantProcessor();
  ~ObVipTenantProcessor() { destroy(); }
  void destroy();

  int init();
  int get_vip_tenant(ObVipTenant &vip_tennat);

  ObVipTenantCache::VTHashMap &get_cache_map_tmp() { return vt_cache_.get_cache_map_tmp(); }
  int update_cache_map() { return vt_cache_.update_cache_map(); }
  int64_t get_vt_cache_count() const { return vt_cache_.get_vt_cache_count(); }
  ObVipTenantCache &get_vt_cache() { return vt_cache_; }
  TO_STRING_KV(K_(is_inited));

private:
  bool is_inited_;
  ObVipTenantCache vt_cache_;
  DISALLOW_COPY_AND_ASSIGN(ObVipTenantProcessor);
};

ObVipTenantProcessor &get_global_vip_tenant_processor();

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase


#endif /* OBPROXY_VIP_TENANT_PROCESSOR_H */
