/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
