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

#include "qos/ob_proxy_qos_stat_info.h"
#include "lib/time/ob_hrtime.h"

namespace oceanbase
{
namespace obproxy
{
namespace qos
{
using namespace oceanbase::common;

int64_t ObProxyQosStatNode::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(index_time_sec), K_(key), K_(idle_period_count), K_(node_type));
  J_OBJ_END();
  return pos;
}

int ObProxyQosStatNode::init(const ObString &key)
{
  int ret = OB_SUCCESS;

  if (key.empty()) {
    key_.reset();
  } else {
    const int32_t min_len = std::min(key.length(), static_cast<int32_t>(OB_PROXY_FULL_USER_NAME_MAX_LEN));
    MEMCPY(key_str_, key.ptr(), min_len);
    key_.assign_ptr(key_str_, min_len);
  }

  return ret;
}

int ObProxyQosStatNode::do_push_index(int64_t current_time_sec)
{
  int ret = OB_SUCCESS;

  // if current_time_sec > index_time_sec_, need update index time
  if (current_time_sec > index_time_sec_) {
    // acquire write lock, avoid other use the index time
    if (OB_SUCC(value_lock_.try_wrlock())) {
      // if current_time_sec still greate than index_time_sec_, update index time and cleare values;
      if (current_time_sec > index_time_sec_) {
        if (current_time_sec - index_time_sec_ > QOS_STAT_VALUE_COUNT) {
          MEMSET(values_, 0, sizeof(values_));
        } else {
          int64_t index = -1;
          for (int64_t i = index_time_sec_ + 1; i <= current_time_sec; i++) {
            index = i % QOS_STAT_VALUE_COUNT;
            values_[QOS_STAT_TYPE_COUNT][index] = 0;
            values_[QOS_STAT_TYPE_COST][index] = 0;
            values_[QOS_STAT_TYPE_RT][index] = 0;
          }
        }
        index_time_sec_ = current_time_sec;
      } else {
        // if current_time_sec < index_time_sec_, other has updated the index_time_sec, re-enter this func
        ret = OB_EAGAIN;
      }
      value_lock_.wrunlock();
    } else {
      // if acquire lock failed, other is updating the index_time_sec, re-enter this func
      ret = OB_EAGAIN;
    }
  }

  return ret;
}

int ObProxyQosStatNode::push_index()
{
  int ret = OB_SUCCESS;
  int64_t current_time_sec = hrtime_to_sec(get_hrtime_internal());

  while (OB_EAGAIN == (ret = do_push_index(current_time_sec))) {
    PAUSE();
  }

  return ret;
}

int ObProxyQosStatNode::store_stat(int64_t cost)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(push_index())) {
    PROXY_LOG(WDIAG, "fail to push index", K(ret));
  } else {
    // after updating, current time must less than index time
    // acquire read lock, avoid other update index_time
    if (OB_SUCC(value_lock_.rdlock())) {
      int64_t index = index_time_sec_ % QOS_STAT_VALUE_COUNT;
      int64_t new_value = ATOMIC_AAF(&values_[QOS_STAT_TYPE_COUNT][index], 1);
      ATOMIC_AAF(&values_[QOS_STAT_TYPE_COST][index], cost);
      value_lock_.rdunlock();
      reset_idle_period_count();
      PROXY_LOG(DEBUG, "qos store stat", K(index), "value", new_value);
    }
  }

  return ret;
}

int ObProxyQosStatNode::calc_qps(int64_t limit_qps, bool &is_reach)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(push_index())) {
    PROXY_LOG(WDIAG, "fail to push index", K(ret));
  } else {
    const int64_t MAX_BUF_LEN = 1024;
    char debug_buf[MAX_BUF_LEN];
    int64_t pos = 0;

    int count = 0;
    int64_t index = -1;
    int64_t index_time_sec = index_time_sec_;

    databuff_printf(debug_buf, MAX_BUF_LEN, pos, "type:%s, index_time_sec:%ld, limit_qps:%ld, ", "QOS_STAT_TYPE_QPS", index_time_sec, limit_qps);

    // after updating, can use index_time without lock.
    // algorithm: if there are more than 5 times, it is considered to be a problem. use 5s to anti-shake
    for (int64_t i = index_time_sec - QOS_STAT_CALC_COUNT; i < index_time_sec; i++) {
      index = i % QOS_STAT_VALUE_COUNT;
      if (values_[QOS_STAT_TYPE_COUNT][index] > limit_qps) {
        count++;
      }

      databuff_printf(debug_buf, MAX_BUF_LEN, pos, "values[COUNT][%ld]:%ld", index, values_[QOS_STAT_TYPE_COUNT][index]);
      if (i < index_time_sec - 1) {
        databuff_printf(debug_buf, MAX_BUF_LEN, pos, ",");
      }
    }

    if (count >= QOS_STAT_MATCH_COUNT) {
      is_reach = true;
    }

    _PROXY_LOG(DEBUG, "%s", debug_buf);
  }

  return ret;
}

int ObProxyQosStatNode::calc_rt(int64_t limit_rt, bool &is_reach)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(push_index())) {
    PROXY_LOG(WDIAG, "fail to push index", K(ret));
  } else {
    const int64_t MAX_BUF_LEN = 2048;
    char debug_buf[MAX_BUF_LEN];
    int64_t pos = 0;

    int count = 0;
    int64_t index = -1;
    int64_t index_time_sec = index_time_sec_;

    databuff_printf(debug_buf, MAX_BUF_LEN, pos, "type:%s, index_time_sec:%ld, limit_rt:%ld, ", "QOS_STAT_TYPE_RT", index_time_sec, limit_rt);

    // after updating, can use index_time without lock.
    // algorithm: if there are more than 5 times, it is considered to be a problem. use 5s to anti-shake
    for (int64_t i = index_time_sec - QOS_STAT_CALC_COUNT; i < index_time_sec; i++) {
      index = i % QOS_STAT_VALUE_COUNT;
      if (0 == values_[QOS_STAT_TYPE_RT][index] && 0 != values_[QOS_STAT_TYPE_COUNT][index]) {
        values_[QOS_STAT_TYPE_RT][index] = values_[QOS_STAT_TYPE_COST][index] / values_[QOS_STAT_TYPE_COUNT][index];
      }

      if (values_[QOS_STAT_TYPE_RT][index] > limit_rt) {
        count++;
      }

      databuff_printf(debug_buf, MAX_BUF_LEN, pos, "values[COST][%ld]:%ld,"
                                                   "values[COUNT][%ld]:%ld,"
                                                   "values[RT][%ld]:%ld",
                                                   index, values_[QOS_STAT_TYPE_COST][index],
                                                   index, values_[QOS_STAT_TYPE_COUNT][index],
                                                   index, values_[QOS_STAT_TYPE_RT][index]);
      if (i < index_time_sec - 1) {
        databuff_printf(debug_buf, MAX_BUF_LEN, pos, ",");
      }
    }

    if (count >= QOS_STAT_MATCH_COUNT) {
      is_reach = true;
    }

    _PROXY_LOG(DEBUG, "%s", debug_buf);
  }

  return ret;
}

int ObProxyQosStatNode::calc_cost(int64_t &cost, int64_t time_window)
{
  int ret = OB_SUCCESS;

  if (time_window < 1 || time_window > 10) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "time window is wrong number", K(ret), K(time_window));
  } else if (OB_FAIL(push_index())) {
    PROXY_LOG(WDIAG, "fail to push index", K(ret));
  } else {
    int64_t index = -1;
    int64_t index_time_sec = index_time_sec_;

    for (int64_t i = index_time_sec - time_window; i < index_time_sec; i++) {
      index = i % QOS_STAT_VALUE_COUNT;
      cost += values_[QOS_STAT_TYPE_COST][index];
    }
  }

  return ret;
}

void ObProxyQosStatNodeRoot::free()
{
  ObProxyQosStatHashTable::iterator iter = hash_nodes_.begin();
  ObProxyQosStatHashTable::iterator end = hash_nodes_.end();
  ObProxyQosStatHashTable::iterator tmp_iter;

  for (; iter != end; ) {
    tmp_iter = iter;
    ++iter;
    tmp_iter->dec_ref();
  }

  op_free(this);
}

int64_t ObProxyQosStatNodeRoot::count()
{
  int64_t count = 0;

  ObProxyQosStatHashTable::iterator iter = hash_nodes_.begin();
  ObProxyQosStatHashTable::iterator end = hash_nodes_.end();

  for (; iter != end; ++iter) {
    count += iter->count();
  }

  count++;

  return count;
}

int ObProxyQosStatNodeRoot::create_node(ObProxyQosStatNode *&node, ObProxyQosNodeTypeEnum node_type)
{
  int ret = OB_SUCCESS;
  node = NULL;

  if (QOS_NODE_TYPE_LEAF == node_type) {
    node = op_alloc_args(ObProxyQosStatNodeLeaf, node_type);
  } else {
    node = op_alloc_args(ObProxyQosStatNodeMiddle, node_type);
  }

  if (OB_ISNULL(node)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(WDIAG, "fail to allocate memory", K(node_type), K(ret));
  }

  return ret;
}

int ObProxyQosStatNodeRoot::create_node(const ObString &key, ObProxyQosStatNode *&node, ObProxyQosNodeTypeEnum node_type, bool &is_new)
{
  int ret = common::OB_SUCCESS;

  common::DRWLock::WRLockGuard lock(hash_lock_);
  if (OB_FAIL(hash_nodes_.get_refactored(key, node))) {
    if (common::OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(create_node(node, node_type))) {
        PROXY_LOG(WDIAG, "fail to create node", K(node_type), K(ret));
      } else {
        is_new = true;
        node->inc_ref();
        if (OB_FAIL(node->init(key))) {
          PROXY_LOG(WDIAG, "fail to init node", K(key), K(ret));
        } else if (OB_FAIL(hash_nodes_.unique_set(node))) {
          PROXY_LOG(WDIAG, "fail to set node into hashmap", KPC(node), K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        node->inc_ref();
      } else if (OB_NOT_NULL(node)) {
        node->dec_ref();
        node = NULL;
      }
    }
  } else {
    node->inc_ref();
  }

  return ret;
}

int ObProxyQosStatNodeRoot::get_node(const ObString &key, ObProxyQosStatNode *&node)
{
  int ret = common::OB_SUCCESS;

  common::DRWLock::RDLockGuard lock(hash_lock_);
  if (OB_FAIL(hash_nodes_.get_refactored(key, node))) {
  } else {
    node->inc_ref();
  }

  return ret;
}

} // end of namespace qos
} // end of namespace obproxy
} // end of namespace oceanbase
