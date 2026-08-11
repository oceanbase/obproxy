/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_SESSION_STAT_H_
#define OB_SESSION_STAT_H_

#include "lib/stat/ob_di_cache.h"
#include "lib/stat/ob_di_tls.h"

namespace oceanbase
{
namespace common
{
struct ObSysSessionIds
{
  enum ObSysSessionIdEnum
  {
    DEFAULT = 1,
    MERGE,
    DUMP,
    MIGRATE,
    BLOOM_FILTER_BUILD,
    NETWORK,
    OMT,
    MAX_RESERVED
  };
};

class ObSessionDIBuffer
{
public:
  ObSessionDIBuffer();
  virtual ~ObSessionDIBuffer();
  int switch_both(const uint64_t tenant_id, const uint64_t session_id);
  int switch_session(const uint64_t session_id);
  int switch_tenant(const uint64_t tenant_id);
  uint64_t get_tenant_id();
  void reset_session();
  inline ObDISessionCollect *get_curr_session() {return session_collect_;}
  inline ObDITenantCollect *get_curr_tenant() {return tenant_collect_;}
  inline ObDITenantCache &get_tenant_cache() {return tenant_cache_;}
private:
  ObDITenantCache tenant_cache_;
  ObDISessionCollect *session_collect_;
  ObDITenantCollect *tenant_collect_;
};

class ObSessionStatEstGuard
{
public:
  ObSessionStatEstGuard(uint64_t tenant_id = OB_SYS_TENANT_ID, uint64_t session_id = ObSysSessionIds::DEFAULT);
  virtual ~ObSessionStatEstGuard();
private:
  uint64_t prev_tenant_id_;
  uint64_t prev_session_id_;
};

class ObTenantStatEstGuard
{
public:
  explicit ObTenantStatEstGuard(uint64_t tenant_id = OB_SYS_TENANT_ID);
  virtual ~ObTenantStatEstGuard();
private:
  uint64_t prev_tenant_id_;
};

} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_SESSION_STAT_H_ */
