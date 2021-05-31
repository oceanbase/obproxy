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

#include "ob_tenant_stat_struct.h"
#include "obutils/ob_proxy_config.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
using namespace oceanbase::common;

/* ----------- ObTenantStatValue -------------- */
int64_t ObTenantStatValue::get_value(const int32_t id) const
{
  return (0 <= id && id < static_cast<int32_t>(MAX_SESSION_STAT_CONT)) ? values_[id] : -1;
}

int64_t ObTenantStatValue::atomic_get_and_reset_value(const ObTenantStatEnum key)
{
  int64_t old_value = -1;
  old_value = ATOMIC_LOAD(values_ + key);
  const int64_t new_value = 0;
  while (false == ATOMIC_BCAS(values_ + key, old_value, new_value)) {
    old_value = ATOMIC_LOAD(values_ + key);
  }
  return old_value;
}

bool ObTenantStatValue::is_active() const
{
  return ATOMIC_LOAD(values_ + STAT_TOTAL_COUNT);
}

/* ----------- ObTenantStatItem -------------- */
void ObTenantStatItem::set_key(const ObString &logic_tenant_name,
                               const ObString &logic_database_name,
                               const ObString &cluster_name,
                               const ObString &tenant_name,
                               const ObString &database_name,
                               const DBServerType database_type,
                               const ObProxyBasicStmtType stmt_type,
                               const ObString &error_code)
{
  if (logic_tenant_name.empty()) {
    logic_tenant_name_.reset();
  } else {
    const int32_t min_len = std::min(logic_tenant_name.length(), static_cast<int32_t>(OB_MAX_TENANT_NAME_LENGTH));
    MEMCPY(logic_tenant_name_str_, logic_tenant_name.ptr(), min_len);
    logic_tenant_name_.assign_ptr(logic_tenant_name_str_, min_len);
  }
  if (logic_database_name.empty()) {
    logic_database_name_.reset();
  } else {
    const int32_t min_len = std::min(logic_database_name.length(), static_cast<int32_t>(OB_MAX_DATABASE_NAME_LENGTH));
    MEMCPY(logic_database_name_str_, logic_database_name.ptr(), min_len);
    logic_database_name_.assign_ptr(logic_database_name_str_, min_len);
  }

  if (cluster_name.empty()) {
    cluster_name_.reset();
  } else {
    const int32_t min_len = std::min(cluster_name.length(), static_cast<int32_t>(OB_PROXY_MAX_CLUSTER_NAME_LENGTH));
    MEMCPY(cluster_name_str_, cluster_name.ptr(), min_len);
    cluster_name_.assign_ptr(cluster_name_str_, min_len);
  }
  if (tenant_name.empty()) {
    tenant_name_.reset();
  } else {
    const int32_t min_len = std::min(tenant_name.length(), static_cast<int32_t>(OB_MAX_TENANT_NAME_LENGTH));
    MEMCPY(tenant_name_str_, tenant_name.ptr(), min_len);
    tenant_name_.assign_ptr(tenant_name_str_, min_len);
  }
  if (database_name.empty()) {
    database_name_.reset();
  } else {
    const int32_t min_len = std::min(database_name.length(), static_cast<int32_t>(OB_MAX_DATABASE_NAME_LENGTH));
    MEMCPY(database_name_str_, database_name.ptr(), min_len);
    database_name_.assign_ptr(database_name_str_, min_len);
  }

  if (error_code.empty()) {
    error_code_.reset();
  } else {
    const int32_t min_len = std::min(error_code.length(), static_cast<int32_t>(OB_MAX_ERROR_CODE_LEN));
    MEMCPY(error_code_str_, error_code.ptr(), min_len);
    error_code_.assign_ptr(error_code_str_, min_len);
  }

  database_type_ = database_type;
  stmt_type_ = stmt_type;
}

int64_t ObTenantStatItem::to_monitor_stat_string(char* buf, const int64_t buf_len)
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;

  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                              "%.*s,%.*s,"
                              "%.*s:%.*s:%.*s,%s,"
                              "%s,%s,%.*s",

                              logic_tenant_name_.length(), logic_tenant_name_.ptr(),
                              logic_database_name_.length(), logic_database_name_.ptr(),

                              cluster_name_.length(), cluster_name_.ptr(),
                              tenant_name_.length(), tenant_name_.ptr(),
                              database_name_.length(), database_name_.ptr(),
                              ObProxyMonitorUtils::get_database_type_name(database_type_),

                              get_print_stmt_name(stmt_type_),
                              (error_code_.length() != 0) ? "failed" : "success",
                              error_code_.length(), error_code_.ptr()))) {
  } else {
    DRWLock::WRLockGuard lock(lock_);
    for (int32_t i = 0; OB_SUCC(ret) && i < static_cast<int32_t>(MAX_SESSION_STAT_CONT); ++i) {
      if (i >= static_cast<int32_t>(STAT_TOTAL_TIME)) {
        if (get_global_proxy_config().monitor_cost_ms_unit) {
          databuff_printf(buf, buf_len, pos, ",%.3fms", static_cast<double>(stat_value_.atomic_get_and_reset_value(static_cast<ObTenantStatEnum>(i)))/1000);
        } else {
          databuff_printf(buf, buf_len, pos, ",%ldus", stat_value_.atomic_get_and_reset_value(static_cast<ObTenantStatEnum>(i)));
        }
      } else {
        databuff_printf(buf, buf_len, pos, ",%ld", stat_value_.atomic_get_and_reset_value(static_cast<ObTenantStatEnum>(i)));
      }
    }
  }
  return pos;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
