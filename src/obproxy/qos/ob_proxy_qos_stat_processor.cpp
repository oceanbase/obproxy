/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "qos/ob_proxy_qos_stat_processor.h"
#include "obutils/ob_proxy_config.h"
#include "utils/ob_proxy_monitor_utils.h"

namespace oceanbase
{
namespace obproxy
{
namespace qos
{

using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;

ObProxyQosStatProcessor g_ob_qos_stat_processor;
const char* ObProxyQosStatProcessor::TESTLOAD_TABLE_NAME = ".*_t";

int ObProxyQosStatProcessor::get_or_create_node(ObProxyQosStatNodeMiddle *parent_node,
                                                const ObString &child_key,
                                                ObProxyQosNodeTypeEnum node_type,
                                                ObProxyQosStatNode *&child_node)
{
  int ret = OB_SUCCESS;
  child_node = NULL;

  if (OB_FAIL(parent_node->get_node(child_key, child_node))) {
    if (common::OB_HASH_NOT_EXIST == ret) {
      bool is_new = false;
      if (ATOMIC_AAF(&qos_stat_num_, 1) <= get_global_proxy_config().qos_stat_item_limit) {
        if (OB_FAIL(parent_node->create_node(child_key, child_node, node_type, is_new))) {
          PROXY_LOG(WDIAG, "fail to create node", K(child_key), K(ret));
        }

        if (OB_FAIL(ret) || !is_new) {
          ATOMIC_AAF(&qos_stat_num_, -1);
        } else {
          LOG_DEBUG("create qos stat success", K(child_key), K_(qos_stat_num));
        }
      } else {
        ATOMIC_AAF(&qos_stat_num_, -1);
        need_expire_qos_stat_ = true;
        ret = OB_EXCEED_MEM_LIMIT;
        LOG_WDIAG("qos stat num reach limit, will discard and expire metric", KPC(parent_node), K(child_key), K_(qos_stat_num));
      }
    } else {
      PROXY_LOG(WDIAG, "fail to get node", K(child_key), K(ret));
    }
  }

  return ret;
}

int ObProxyQosStatProcessor::store_stat_and_next(ObProxyQosStatNodeMiddle *parent_node,
                                                 const ObString &child_key,
                                                 ObProxyQosNodeTypeEnum type,
                                                 ObProxyQosStatNode *&child_node,
                                                 int64_t cost)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(parent_node->store_stat(cost))) {
    LOG_WDIAG("fail to store value", KP(parent_node), K(child_key), K(cost), K(ret));
  } else if (OB_FAIL(get_or_create_node(parent_node, child_key, type, child_node))) {
    if (OB_EXCEED_MEM_LIMIT == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WDIAG("fail to get or create node", KP(parent_node), K(child_key), K(type), K(ret));
    }
  }

  if (reinterpret_cast<ObProxyQosStatNode*>(parent_node) != child_node) {
    parent_node->dec_ref();
  }

  return ret;
}

int ObProxyQosStatProcessor::store_stat(const ObString &cluster_name, const ObString &tenant_name,
                                        const ObString &database_name, const ObString &user_name,
                                        const ObString &table_name, int64_t cost)
{
  int ret = OB_SUCCESS;

  ObProxyQosStatNode *node = NULL;
  if (cluster_name.empty() || tenant_name.empty() || database_name.empty() || user_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_DEBUG("invalid argument", K(cluster_name), K(tenant_name), K(database_name), K(user_name), K(ret));
  } else if (OB_FAIL(get_or_create_node(&root_node_, cluster_name, QOS_NODE_TYPE_MIDDLE, node))) {
    LOG_WDIAG("fail to get or create node", K(cluster_name), K(ret));
  } else if (OB_NOT_NULL(node) && OB_FAIL(store_stat_and_next(reinterpret_cast<ObProxyQosStatNodeMiddle*>(node), tenant_name, QOS_NODE_TYPE_MIDDLE, node, cost))) {
    LOG_WDIAG("fail to store value", KP(node), K(tenant_name), K(ret));
  } else if (OB_NOT_NULL(node) && OB_FAIL(store_stat_and_next(reinterpret_cast<ObProxyQosStatNodeMiddle*>(node), database_name, QOS_NODE_TYPE_MIDDLE, node, cost))) {
    LOG_WDIAG("fail to store value", KP(node), K(database_name), K(ret));
  } else if (OB_NOT_NULL(node) && OB_FAIL(store_stat_and_next(reinterpret_cast<ObProxyQosStatNodeMiddle*>(node), user_name, QOS_NODE_TYPE_MIDDLE, node, cost))) {
    LOG_WDIAG("fail to store value", KP(node), K(user_name), K(ret));
  } else if (OB_NOT_NULL(node)) {
    if (table_name.empty()) {
      if (OB_FAIL(reinterpret_cast<ObProxyQosStatNodeMiddle*>(node)->store_stat(cost))) {
        LOG_WDIAG("fail to store stat", KP(node), K(ret));
      }
    } else {
      if (OB_FAIL(store_stat_and_next(reinterpret_cast<ObProxyQosStatNodeMiddle*>(node), table_name, QOS_NODE_TYPE_LEAF, node, cost))) {
        LOG_WDIAG("fail to store value", KP(node), K(table_name), K(ret));
      } else if (OB_NOT_NULL(node) && OB_FAIL(reinterpret_cast<ObProxyQosStatNodeLeaf*>(node)->store_stat(cost))) {
        LOG_WDIAG("fail to store stat", KP(node), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succ to store stat", K(cluster_name), K(tenant_name), K(database_name), K(user_name), K(cost));
  }

  if (OB_NOT_NULL(node)) {
    node->dec_ref();
    node = NULL;
  }

  return ret;
}

int ObProxyQosStatProcessor::get_child_node(ObProxyQosStatNodeRoot *parent_node, const ObString &key,
                                            ObProxyQosStatNode *&child_node, bool &is_child_node_exist)
{
  int ret = OB_SUCCESS;

  is_child_node_exist = false;

  if (!key.empty()) {
    if (OB_FAIL(parent_node->get_node(key, child_node))) {
      if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
      } else {
        LOG_WDIAG("fail to get node", KP(parent_node), K(key), K(ret));
      }
    } else {
      is_child_node_exist = true;
    }
  }

  return ret;
}

int ObProxyQosStatProcessor::get_target_node(ObProxyQosStatNode *&target_node, const ObString &cluster_name,
                                             const ObString &tenant_name, const ObString &database_name,
                                             const ObString &user_name, const ObString &table_name)
{
  int ret = OB_SUCCESS;
  target_node = NULL;
  bool is_child_node_exist = false;
  ObProxyQosStatNode *node = NULL;
  ObProxyQosStatNode *child_node = NULL;

  if (OB_FAIL(get_child_node(&root_node_, cluster_name, child_node, is_child_node_exist))) {
    LOG_WDIAG("fail to get node", KP(node), K(cluster_name), K(ret));
  }

  if (OB_SUCC(ret) && is_child_node_exist) {
    // root_node_ 不用减引用计数
    node = child_node;
    if (OB_FAIL(get_child_node(reinterpret_cast<ObProxyQosStatNodeMiddle*>(node), tenant_name, child_node, is_child_node_exist))) {
      LOG_WDIAG("fail to get node", KP(node), K(tenant_name), K(ret));
    }
  }

  if (OB_SUCC(ret) && is_child_node_exist) {
    node->dec_ref();
    node = child_node;
    if (OB_FAIL(get_child_node(reinterpret_cast<ObProxyQosStatNodeMiddle*>(node), database_name, child_node, is_child_node_exist))) {
      LOG_WDIAG("fail to get node", KP(node), K(database_name), K(ret));
    }
  }

  if (OB_SUCC(ret) && is_child_node_exist) {
    node->dec_ref();
    node = child_node;
    if (OB_FAIL(get_child_node(reinterpret_cast<ObProxyQosStatNodeMiddle*>(node), user_name, child_node, is_child_node_exist))) {
      LOG_WDIAG("fail to get node", KP(node), K(user_name), K(ret));
    }
  }

  if (OB_SUCC(ret) && is_child_node_exist) {
    node->dec_ref();
    node = child_node;
    if (OB_FAIL(get_child_node(reinterpret_cast<ObProxyQosStatNodeMiddle*>(node), table_name, child_node, is_child_node_exist))) {
      LOG_WDIAG("fail to get node", KP(node), K(user_name), K(ret));
    }
  }

  if (OB_SUCC(ret) && is_child_node_exist) {
    node->dec_ref();
    node = child_node;
  }
  target_node = node;

  return ret;
}

int ObProxyQosStatProcessor::calc_qps_and_rt(const ObString &cluster_name, const ObString &tenant_name,
                                             const ObString &database_name, const ObString &user_name,
                                             int64_t limit_rt, int64_t limit_qps, bool &is_reach)
{
  int ret = OB_SUCCESS;

  is_reach = false;
  bool is_rt_reach = false;
  bool is_qps_reach = false;
  ObProxyQosStatNode *node = NULL;

  if (OB_FAIL(get_target_node(node, cluster_name, tenant_name, database_name, user_name))) {
    LOG_WDIAG("get target node failed", K(ret), K(cluster_name), K(tenant_name), K(database_name), K(user_name));
  }

  if (OB_SUCC(ret) && node != NULL) {
    if (limit_rt > 0) {
      if (OB_FAIL(node->calc_rt(limit_rt, is_rt_reach))) {
        LOG_WDIAG("fail to calc rt", KPC(node), K(limit_rt), K(ret));
      }
    } else {
      is_rt_reach = true;
    }

    if (OB_SUCC(ret) && is_rt_reach) {
      if (limit_qps > 0) {
        if (OB_FAIL(node->calc_qps(limit_qps, is_qps_reach))) {
          LOG_WDIAG("fail to calc qps", KPC(node), K(limit_qps), K(ret));
        }
      } else {
        is_qps_reach = true;
      }
    }

    if (OB_SUCC(ret)) {
      is_reach = is_rt_reach && is_qps_reach;
    }
  }

  if (node != NULL) {
    node->dec_ref();
  }

  return ret;
}

int ObProxyQosStatProcessor::calc_cost(const ObString &cluster_name, const ObString &tenant_name,
                                       const ObString &database_name, const ObString &user_name,
                                       int64_t &cost, int64_t time_window)
{
  int ret = OB_SUCCESS;
  ObProxyQosStatNode *node = NULL;

  if (OB_FAIL(get_target_node(node, cluster_name, tenant_name, database_name, user_name,
                              ObProxyQosStatProcessor::TESTLOAD_TABLE_NAME))) {
    LOG_WDIAG("get target node failed", K(ret), K(cluster_name), K(tenant_name),
        K(database_name), K(user_name), K(ObProxyQosStatProcessor::TESTLOAD_TABLE_NAME));
  }

  if (OB_SUCC(ret) && node != NULL) {
    if (OB_FAIL(node->calc_cost(cost, time_window))) {
      LOG_WDIAG("fail to calc rt", KPC(node), K(cost), K(time_window), K(ret));
    }
  }

  if (node != NULL) {
    node->dec_ref();
  }

  return ret;
}

int ObProxyQosStatProcessor::recursive_do_clean(ObProxyQosStatNodeRoot *node, int64_t max_idle_period)
{
  int ret = OB_SUCCESS;

  // 遍历时不需要获取锁, 其他并发情况要么是遍历读, 要么是新增插入
  ObProxyQosStatHashTable::iterator iter = node->get_hash_nodes().begin();
  ObProxyQosStatHashTable::iterator end = node->get_hash_nodes().end();
  for (; OB_SUCC(ret) && iter != end; ) {
    // 原子的检查父 node 下的每个子 node 是不是超过时间了
    if (iter->inc_and_fetch_idle_period_count() > max_idle_period) {
      // 如果超过了, 获取父 node 的写锁
      if (OB_SUCC(node->get_hash_lock().wrlock())) {
        // 再次检查子 node 是不是超过时间了
        // 这个时候没有其他并发处理该父 node, 可以放心删除子 node
        if (iter->get_idle_period_count() > max_idle_period) {
          ObProxyQosStatHashTable::iterator tmp_iter = iter;
          ++iter;
          node->get_hash_nodes().remove(&(*tmp_iter));
          // 从 hash 表中删除后就可以释放写锁了
          node->get_hash_lock().wrunlock();

          int64_t count = tmp_iter->count();
          LOG_DEBUG("succ to erase qos stat", "node", *tmp_iter, K(count), K_(qos_stat_num));
          tmp_iter->dec_ref();
          ATOMIC_SAF(&qos_stat_num_, count);
          need_expire_qos_stat_ = false;
        } else {
          // 如果在获取写锁之前有并发修改, 则释放锁, 并跳过该子 node
          node->get_hash_lock().wrunlock();
          ++iter;
        }
      } else {
        // 如果获取写锁失败, 则跳过该子 node
        ++iter;
      }
    } else {
      // 如果该子 node 没有超过时间
      //   如果是叶子类型，就直接跳过该子 node
      //   如果不是叶子类型, 就递归检查该子 node
      if (QOS_NODE_TYPE_LEAF != iter->get_node_type()) {
        if (OB_FAIL(recursive_do_clean(reinterpret_cast<ObProxyQosStatNodeRoot*>(&(*iter)), max_idle_period))) {
          LOG_WDIAG("fail to recursive clean", K(ret));
        }
      }
      ++iter;
    }
  }

  return ret;
}

int ObProxyQosStatProcessor::do_qos_stat_clean()
{
  int ret = OB_SUCCESS;

  int64_t max_idle_period = get_global_proxy_config().qos_stat_item_max_idle_period / get_global_proxy_config().qos_stat_clean_interval;
  max_idle_period = max_idle_period == 0 ? 1 : max_idle_period;

  LOG_DEBUG("start to do qos stat clean", K(max_idle_period), "need_expire_qos_stat", g_ob_qos_stat_processor.get_need_expire_qos_stat());

  if (g_ob_qos_stat_processor.get_need_expire_qos_stat()
      && OB_FAIL(g_ob_qos_stat_processor.recursive_do_clean(&g_ob_qos_stat_processor.get_root_node(), max_idle_period))) {
    LOG_WDIAG("fail to do recursive clean", K(ret));
  } else {
    ObAsyncCommonTask *cont = g_ob_qos_stat_processor.get_qos_stat_clean_cont();
    int64_t interval_us = 0;
    if (OB_LIKELY(cont)) {
      interval_us = ObProxyMonitorUtils::get_next_schedule_time(get_global_proxy_config().qos_stat_clean_interval);
      cont->set_interval(interval_us);
    }
    LOG_DEBUG("succ to do qos stat clean", K(interval_us));
  }

  return ret;
}

void ObProxyQosStatProcessor::update_qos_stat_clean_interval()
{
  ObAsyncCommonTask *cont = g_ob_qos_stat_processor.get_qos_stat_clean_cont();
  if (OB_LIKELY(cont)) {
    int64_t interval_us = ObProxyMonitorUtils::get_next_schedule_time(get_global_proxy_config().qos_stat_clean_interval);
    cont->set_interval(interval_us);
  }
}

int ObProxyQosStatProcessor::start_qos_stat_clean_task()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL != qos_stat_clean_cont_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("qos_stat_clean_cont should be null here", K_(qos_stat_clean_cont), K(ret));
  } else {
    int64_t interval_us = ObProxyMonitorUtils::get_next_schedule_time(get_global_proxy_config().qos_stat_clean_interval);
    // 第一次避免距离当前太近, 直接跳到下个时间
    interval_us += get_global_proxy_config().qos_stat_clean_interval;

    if (interval_us > 0) {
      if (OB_ISNULL(qos_stat_clean_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(
                    interval_us,
                    "qos_stat_clean_task",
                    ObProxyQosStatProcessor::do_qos_stat_clean,
                    ObProxyQosStatProcessor::update_qos_stat_clean_interval, false, event::ET_TASK))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to create and start qos_stat_clean task", K(ret));
      }
    }
  }
  return ret;
}

} // end of namespace qos
} // end of namespace obproxy
} // end of namespace oceanbase
