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

#ifndef OBPROXY_QOS_STAT_PROCESSOR_H
#define OBPROXY_QOS_STAT_PROCESSOR_H

#include "qos/ob_proxy_qos_stat_info.h"
#include "obutils/ob_async_common_task.h"

namespace oceanbase
{
namespace obproxy
{
namespace qos
{

class ObProxyQosStatProcessor
{
public:
  ObProxyQosStatProcessor()
    : root_node_(QOS_NODE_TYPE_ROOT), qos_stat_clean_cont_(NULL),
      qos_stat_num_(0), need_expire_qos_stat_(false) {};
  ~ObProxyQosStatProcessor() {};

  int calc_qps_and_rt(const common::ObString &cluster_name, const common::ObString &tenant_name,
                      const common::ObString &database_name, const common::ObString &user_name,
                      int64_t limit_rt, int64_t limit_qps, bool &is_reach);
  int calc_cost(const common::ObString &cluster_name, const common::ObString &tenant_name,
                const common::ObString &database_name, const common::ObString &user_name,
                int64_t &cost, int64_t time_window);

  int store_stat(const common::ObString &cluster_name, const common::ObString &tenant_name,
                 const common::ObString &database_name, const common::ObString &user_name,
                 const common::ObString &table_name, int64_t cost);

  static int do_qos_stat_clean();
  static void update_qos_stat_clean_interval();
  int start_qos_stat_clean_task();
  obutils::ObAsyncCommonTask *get_qos_stat_clean_cont() { return qos_stat_clean_cont_; }
  ObProxyQosStatNodeRoot& get_root_node() { return root_node_; }
  bool get_need_expire_qos_stat() { return need_expire_qos_stat_; }

private:
  int get_or_create_node(ObProxyQosStatNodeMiddle *parent_node,
                         const common::ObString &child_key,
                         ObProxyQosNodeTypeEnum node_type,
                         ObProxyQosStatNode *&child_node);
  int store_stat_and_next(ObProxyQosStatNodeMiddle *parent_node,
                          const common::ObString &child_key,
                          ObProxyQosNodeTypeEnum type,
                          ObProxyQosStatNode *&child_node,
                          int64_t cost);

  int get_child_node(ObProxyQosStatNodeRoot *parent_node, const common::ObString &key,
                     ObProxyQosStatNode *&child_node, bool &is_child_node_exist);
  int get_target_node(ObProxyQosStatNode *&target_node, const common::ObString &cluster_name,
                      const common::ObString &tenant_name = "", const common::ObString &database_name = "",
                      const common::ObString &user_name = "", const common::ObString &table_name = "");
  int recursive_do_clean(ObProxyQosStatNodeRoot *node, int64_t max_idle_period);

public:
  static const char* TESTLOAD_TABLE_NAME;

private:
  ObProxyQosStatNodeRoot root_node_;
  obutils::ObAsyncCommonTask *qos_stat_clean_cont_;
  int32_t qos_stat_num_;
  bool need_expire_qos_stat_;
};

extern ObProxyQosStatProcessor g_ob_qos_stat_processor;

} // end of namespace qos
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_QOS_STAT_PROCESSOR_H
