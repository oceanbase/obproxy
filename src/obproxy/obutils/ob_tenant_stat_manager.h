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

#ifndef OBPROXY_TENANT_STAT_MANAGER_H
#define OBPROXY_TENANT_STAT_MANAGER_H

#include "ob_tenant_stat_struct.h"
#include "obutils/ob_async_common_task.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

class ObTenantStatManager
{
public:
  typedef ObTenantStatKey Key;
  template <typename Function> int remove_if_idle_timeout(Function &fn);

  ObTenantStatManager() : tenant_stat_dump_cont_(NULL), item_map_(), item_num_(0) {}
  ~ObTenantStatManager() {}

  //need call revert_item after use it
  int get_item(const ObTenantStatKey &key,
               ObTenantStatItem *&item);
  //need call revert_item after use it
  int create_item(const common::ObString &logic_tenant_name,
                  const common::ObString &logic_database_name,
                  const common::ObString &cluster_name,
                  const common::ObString &tenant_name,
                  const common::ObString &database_name,
                  const common::DBServerType database_type,
                  const ObProxyBasicStmtType stmt_type,
                  const common::ObString &error_code,
                  ObTenantStatItem *&item);
  //need call revert_item after use it
  int get_or_create_item(const common::ObString &logic_tenant_name,
                         const common::ObString &logic_database_name,
                         const common::ObString &cluster_name,
                         const common::ObString &tenant_name,
                         const common::ObString &database_name,
                         const common::DBServerType database_type,
                         const ObProxyBasicStmtType stmt_type,
                         const common::ObString &error_code,
                         ObTenantStatItem *&item);
  void revert_item(ObTenantStatItem *item);

  int64_t get_item_count() { return item_map_.count(); }

  static int do_tenant_stat_dump();
  static void update_tenant_stat_dump_interval();
  int start_tenant_stat_dump_task();
  int set_stat_table_sync_interval();

  ObAsyncCommonTask *get_tenant_stat_dump_cont() { return tenant_stat_dump_cont_; }

  class CheckActiveStats
  {
  public:
    CheckActiveStats(const int64_t max_idle_period, int32_t *item_num)
        : max_idle_period_(max_idle_period), item_num_(item_num)
    {}
    bool operator() (ObTenantStatManager::Key key, ObTenantStatItem *item);
  private:
    const int64_t max_idle_period_;
    int32_t *item_num_;
  };

  int check_active_stats(const int64_t max_idle_period);

  DECLARE_TO_STRING;

private:
  class ValueAlloc
  {
  public:
    ValueAlloc() {}
    ~ValueAlloc() {}
    ObTenantStatItem* alloc_value() { return op_reclaim_alloc(ObTenantStatItem); }
    void free_value(ObTenantStatItem *sess) { op_reclaim_free(sess); sess = NULL; }
    ObTenantStatHashNode* alloc_node(ObTenantStatItem* value)
    {
      UNUSED(value);
      return op_alloc(ObTenantStatHashNode);
    }
    void free_node(ObTenantStatHashNode* node)
    {
      if (NULL != node) {
        op_free(node);
        node = NULL;
      }
    }
  };
  typedef common::ObLinkHashMap<ObTenantStatKey, ObTenantStatItem, ValueAlloc> HashMap;

private:
  ObAsyncCommonTask *tenant_stat_dump_cont_;
  HashMap item_map_;
  int32_t item_num_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantStatManager);
};

ObTenantStatManager &get_global_tenant_stat_mgr();

inline int ObTenantStatManager::get_item(const ObTenantStatKey &key, ObTenantStatItem *&item)
{
  return item_map_.get(key, item);
}

inline void ObTenantStatManager::revert_item(ObTenantStatItem *item)
{
  return item_map_.revert(item);
}

template <typename Function>
int ObTenantStatManager::remove_if_idle_timeout(Function &fn)
{
  return item_map_.remove_if(fn);
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_TENANT_STAT_MANAGER_H
