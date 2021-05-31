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

#include "ob_tenant_stat_manager.h"
#include "utils/ob_proxy_hot_upgrader.h"
#include "obutils/ob_proxy_config.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

ObTenantStatManager &get_global_tenant_stat_mgr()
{
  static ObTenantStatManager g_tenant_stat_mgr;
  return g_tenant_stat_mgr;
}

int ObTenantStatManager::create_item(const ObString &logic_tenant_name,
                                     const ObString &logic_database_name,
                                     const ObString &cluster_name,
                                     const ObString &tenant_name,
                                     const ObString &database_name,
                                     const DBServerType database_type,
                                     const ObProxyBasicStmtType stmt_type,
                                     const ObString &error_code,
                                     ObTenantStatItem *&item)
{
  int ret = OB_SUCCESS;
  ObTenantStatKey key(logic_tenant_name, logic_database_name, cluster_name,
                      tenant_name, database_name, stmt_type, error_code);
  if (OB_FAIL(item_map_.create(key, item))) {
    if (OB_LIKELY(OB_ENTRY_EXIST == ret)) {
      if (OB_FAIL(get_item(key, item))) {
        LOG_WARN("fail to get_item, it should not happen", K(key), K(ret));
      } else if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("item is NULL, it should not happen", K(key), K(ret));
      } else {
        LOG_INFO("other one create tenant stat item",
                 K(logic_tenant_name), K(logic_database_name),
                 K(cluster_name), K(tenant_name), K(database_name),
                 K(stmt_type), K(error_code));
      }
    } else {
      LOG_WARN("fail to create item", K(key), K(ret));
    }
  } else if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item is NULL, it should not happen", K(key), K(ret));
  } else {
    item->set_key(logic_tenant_name, logic_database_name, cluster_name,
                  tenant_name, database_name, database_type, stmt_type, error_code);
    LOG_INFO("succ to create tenant stat item", KPC(item), K(key));
  }
  return ret;
}

int ObTenantStatManager::get_or_create_item(const ObString &logic_tenant_name,
                                            const ObString &logic_database_name,
                                            const ObString &cluster_name,
                                            const ObString &tenant_name,
                                            const ObString &database_name,
                                            const DBServerType database_type,
                                            const ObProxyBasicStmtType stmt_type,
                                            const ObString &error_code,
                                            ObTenantStatItem *&item)
{
  int ret = OB_SUCCESS;
  ObTenantStatKey key(logic_tenant_name, logic_database_name, cluster_name,
                      tenant_name, database_name, stmt_type, error_code);
  if (OB_FAIL(get_item(key, item))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (ATOMIC_AAF(&item_num_, 1) <= get_global_proxy_config().monitor_item_limit) {
        if (OB_FAIL(create_item(logic_tenant_name, logic_database_name, cluster_name, tenant_name, database_name,
                                database_type, stmt_type, error_code,
                                item))) {
          ATOMIC_AAF(&item_num_, -1);
          LOG_WARN("fail to create tenant stat item", K(logic_tenant_name), K(logic_database_name),
                   K(cluster_name), K(tenant_name), K(database_name),
                   K(database_type), K(stmt_type), K(error_code), K(key), K(ret));
        }
      } else {
        ATOMIC_AAF(&item_num_, -1);
        ret = OB_EXCEED_MEM_LIMIT;
        LOG_WARN("stat num reach limit, will discard", K_(item_num));
      }
    } else {
      LOG_WARN("fail to get tenant stat item", K(logic_tenant_name), K(logic_database_name),
               K(cluster_name), K(tenant_name), K(database_name),
               K(database_type), K(stmt_type), K(error_code), K(key), K(ret));
    }
  }
  return ret;
}

int ObTenantStatManager::check_active_stats(const int64_t max_idle_period)
{
  CheckActiveStats pas_func(max_idle_period, &item_num_);
  return remove_if_idle_timeout(pas_func);
}

bool ObTenantStatManager::CheckActiveStats::operator() (
    ObTenantStatManager::Key key, ObTenantStatItem *item)
{
  UNUSED(key);
  int ret = OB_SUCCESS;
  bool need_clean = false;
  if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item is NULL", K(item), K(ret));
  } else {
    const int64_t MAX_BUF_LEN = 1024;
    char buf[MAX_BUF_LEN];
    if (item->is_active()) {
      item->to_monitor_stat_string(buf, MAX_BUF_LEN);
      _OBPROXY_STAT_LOG(INFO, "%s", buf);
      item->reset_idle_period_count();
    } else if (item->inc_and_fetch_idle_period_count() > max_idle_period_) {
      LOG_DEBUG("will to clean stat item", KPC(item), K_(max_idle_period));
      ATOMIC_AAF(item_num_, -1);
      need_clean = true;
    }
  }
  return need_clean;
}

int ObTenantStatManager::do_tenant_stat_dump()
{
  int ret = OB_SUCCESS;
  int64_t max_idle_period = get_global_proxy_config().monitor_item_max_idle_period / get_global_proxy_config().monitor_stat_dump_interval;
  max_idle_period = max_idle_period == 0 ? 1 : max_idle_period;
  get_global_tenant_stat_mgr().check_active_stats(max_idle_period);

  ObAsyncCommonTask *cont = get_global_tenant_stat_mgr().get_tenant_stat_dump_cont();
  if (OB_LIKELY(cont)) {
    int64_t interval_us = ObProxyMonitorUtils::get_next_schedule_time(get_global_proxy_config().monitor_stat_dump_interval);
    cont->set_interval(interval_us);
  }

  return ret;
}

void ObTenantStatManager::update_tenant_stat_dump_interval()
{
  ObAsyncCommonTask *cont = get_global_tenant_stat_mgr().get_tenant_stat_dump_cont();
  if (OB_LIKELY(cont)) {
    int64_t interval_us = ObProxyMonitorUtils::get_next_schedule_time(get_global_proxy_config().monitor_stat_dump_interval);
    cont->set_interval(interval_us);
  }
}

int ObTenantStatManager::start_tenant_stat_dump_task()
{
  int ret = OB_SUCCESS;

  int64_t interval_us = ObProxyMonitorUtils::get_next_schedule_time(get_global_proxy_config().monitor_stat_dump_interval);

  if (interval_us > 0) {
    if (OB_ISNULL(tenant_stat_dump_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(
            interval_us,
            "tenant_stat_dump_task",
            ObTenantStatManager::do_tenant_stat_dump,
            ObTenantStatManager::update_tenant_stat_dump_interval, false, event::ET_TASK))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create and start tenant_stat_dump_task task", K(ret));
    }
  }
  return ret;
}

int ObTenantStatManager::set_stat_table_sync_interval()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAsyncCommonTask::update_task_interval(tenant_stat_dump_cont_))) {
    LOG_WARN("fail to set tenant_stat_dump interval", K(ret));
  }

  return ret;
}
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
