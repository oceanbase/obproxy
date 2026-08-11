/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_QOS_STAT_STRUCT_H
#define OBPROXY_QOS_STAT_STRUCT_H

#include "lib/string/ob_string.h"
#include "lib/lock/ob_drw_lock.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "utils/ob_proxy_lib.h"

namespace oceanbase
{
namespace obproxy
{
namespace qos
{

// 熔断算法, 计算当前时间前 10s, 有超过 5 次就认为是有问题的. 有一定的放抖能力, 后续可以考虑加权计.
// 第 11 个是用来存最新的值，比如从 0 秒开始，现在是第 10秒，那使用 0~9，第10秒就存最新的，所以最少是 11 个
// +1，是为了防止时间的不准，允许多个线程间的时间误差在 1s 以内(+2 就表示允许误差在 2s 内)，比如线程 1 认为现在是第 10秒，线程2 认为现在是第 11 s，线程1推进 index 到第 10 秒，随后线程 2 会推进到 11秒。如果只有 11 个，那就会存储到 11/11 = 0，即第 0个位置上，而线程 1 认为当前是在第10s，继续使用 0 ~9 ，就会使用到已经被清空的 0 号数据
#define QOS_STAT_VALUE_COUNT (11 + 1)
#define QOS_STAT_CALC_COUNT 10
#define QOS_STAT_MATCH_COUNT 5
#define QOS_NODE_HASH_BUCKET_SIZE 8

enum ObQosStatTypeEnum {
  QOS_STAT_TYPE_COUNT = 0,
  QOS_STAT_TYPE_COST,
  QOS_STAT_TYPE_RT,
  QOS_STAT_TYPE_MAX
};

enum ObProxyQosNodeTypeEnum {
  QOS_NODE_TYPE_INVALID = 0,
  QOS_NODE_TYPE_LEAF,
  QOS_NODE_TYPE_MIDDLE,
  QOS_NODE_TYPE_ROOT,
  QOS_NODE_TYPE_MAX
};

class ObProxyQosStatNode : public common::ObSharedRefCount
{
public:
  ObProxyQosStatNode(ObProxyQosNodeTypeEnum node_type)
    : idle_period_count_(0), index_time_sec_(0), node_type_(node_type) {
    MEMSET(values_, 0, sizeof(values_));
    MEMSET(key_str_, 0, sizeof(key_str_));
  }

  ~ObProxyQosStatNode() {};
  int init(const common::ObString &key);
  int store_stat(int64_t cost);

  int calc_qps(int64_t limit_qps, bool &is_reach);
  int calc_rt(int64_t limit_rt, bool &is_reach);
  int calc_cost(int64_t &cost, int64_t time_window);


  common::ObString get_key() { return key_; }

  int64_t to_string(char *buf, const int64_t buf_len) const;

  ObProxyQosNodeTypeEnum get_node_type() { return node_type_; }
  int64_t inc_and_fetch_idle_period_count() { return ATOMIC_AAF(&idle_period_count_, 1); }
  int64_t get_idle_period_count() { return ATOMIC_LOAD(&idle_period_count_); }
  void reset_idle_period_count() { idle_period_count_ = 0; }
  virtual int64_t count() { return 1; }

public:
  LINK(ObProxyQosStatNode, node_link_);

private:
  int push_index();
  int do_push_index(int64_t current_time_sec);
  virtual void free() { op_free(this); }

private:
  common::DRWLock value_lock_;

  int64_t idle_period_count_;

  int64_t index_time_sec_; // 表示当前要访问的数据下标的时间
  int64_t values_[QOS_STAT_TYPE_MAX][QOS_STAT_VALUE_COUNT]; // RT 的单位是 us

  common::ObString key_;
  char key_str_[OB_PROXY_FULL_USER_NAME_MAX_LEN];

  ObProxyQosNodeTypeEnum node_type_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyQosStatNode);
};

class ObProxyQosStatHashing
{
public:
  typedef common::ObString Key;
  typedef ObProxyQosStatNode Value;
  typedef ObDLList(ObProxyQosStatNode, node_link_) ListHead;

  static uint64_t hash(Key key) { return key.hash(); }
  static Key key(Value *value)
  {
    return value->get_key();
  }

  static bool equal(Key lhs, Key rhs)
  {
    return 0 == lhs.case_compare(rhs);
  }
};

typedef common::hash::ObBuildInHashMap<ObProxyQosStatHashing, QOS_NODE_HASH_BUCKET_SIZE> ObProxyQosStatHashTable;

typedef ObProxyQosStatNode ObProxyQosStatNodeLeaf;

class ObProxyQosStatNodeRoot : public ObProxyQosStatNode
{
public:
  ObProxyQosStatNodeRoot(ObProxyQosNodeTypeEnum node_type)
    : ObProxyQosStatNode(node_type) {}
  ~ObProxyQosStatNodeRoot() {}

public:
  int create_node(const common::ObString &key, ObProxyQosStatNode *&node,
                  ObProxyQosNodeTypeEnum node_type, bool &is_new);
  int get_node(const common::ObString &key, ObProxyQosStatNode *&node);
  ObProxyQosStatHashTable& get_hash_nodes() { return hash_nodes_; }
  common::DRWLock& get_hash_lock() { return hash_lock_; }
  virtual int64_t count();

private:
  virtual void free();
  int create_node(ObProxyQosStatNode *&node, ObProxyQosNodeTypeEnum node_type);

private:
  common::DRWLock hash_lock_;
  ObProxyQosStatHashTable hash_nodes_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyQosStatNodeRoot);
};

typedef ObProxyQosStatNodeRoot ObProxyQosStatNodeMiddle;

} // end of namespace qos
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_QOS_STAT_STRUCT_H
