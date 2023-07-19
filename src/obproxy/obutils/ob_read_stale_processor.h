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

#ifndef OB_READ_STALE_PROCESSOR
#define OB_READ_STALE_PROCESSOR
#include "iocore/net/ob_inet.h"
#include "lib/list/ob_intrusive_list.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "lib/lock/ob_drw_lock.h"
#include "obutils/ob_vip_tenant_cache.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/allocator/ob_mod_define.h"
#include "obutils/ob_async_common_task.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{


class ObReadStaleProcessor;

class ObReadStaleReplica
{
public:
  ObReadStaleReplica() : server_addr_(), table_id_(0), partition_id_(0) {};
  ObReadStaleReplica(const net::ObIpEndpoint &server_addr, const int64_t table_id, const int64_t partition_id) :
      server_addr_(server_addr), table_id_(table_id), partition_id_(partition_id) {};
  ~ObReadStaleReplica() {};
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool operator == (const ObReadStaleReplica &replica) const;
  uint64_t hash() const;
public:
  net::ObIpEndpoint server_addr_;
  int64_t table_id_;
  int64_t partition_id_;
};

class ObReadStaleFeedback
{
public:
  ObReadStaleFeedback(): replica_(), feedback_time_(0) {};
  ObReadStaleFeedback(ObReadStaleReplica &replica, const int64_t feedback_time) :
      replica_(replica), feedback_time_(feedback_time) {};
  ~ObReadStaleFeedback() {};
  void destroy();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool is_read_stale_feedback_valid(const int64_t retry_interval);
  bool is_read_stale_feedback_need_remove(const int64_t interval, const int64_t now);
public:
  ObReadStaleReplica replica_;
  int64_t feedback_time_; // unit: us
  LINK(ObReadStaleFeedback, read_stale_feedback_link_);
};

class ObVipReadStaleKey
{
public:
  ObVipReadStaleKey(): vip_addr_(), tenant_name_(), cluster_name_() {};
  ObVipReadStaleKey(const ObVipAddr &vip_addr, const common::ObString &tenant_name, const common::ObString &cluster_name)
    : vip_addr_(vip_addr), tenant_name_(tenant_name), cluster_name_(cluster_name) {};
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool operator == (const ObVipReadStaleKey &vip_key) const;
  uint64_t hash() const;
public:
  ObVipAddr vip_addr_;
  common::ObString tenant_name_;
  common::ObString cluster_name_;
};;


class ObVipReadStaleInfo
{
public:
  ObVipReadStaleInfo(): vip_key_(), read_stale_feedback_map_(), read_stale_retry_interval_(0), lock_() {};
  ~ObVipReadStaleInfo() {};
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void destroy();
public:
  static const int64_t HASH_BUCKET_SIZE = 32;
  static const int64_t VIP_READ_STALE_MAX_RECORD = 16384;
  struct ObReadStaleFeedbackHashing
  {
    typedef ObReadStaleReplica &Key;
    typedef ObReadStaleFeedback Value;
    typedef ObDLList(ObReadStaleFeedback, read_stale_feedback_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->replica_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<ObReadStaleFeedbackHashing, HASH_BUCKET_SIZE> ObReadStaleFeedbackMap;
  typedef ObReadStaleFeedbackMap::iterator FeedbackIterator;

public:
  int acquire_feedback_record(const net::ObIpEndpoint &server_addr,
                              const int64_t table_id,
                              const int64_t partition_id,
                              bool &is_stale,
                              ObReadStaleFeedback *&stale_feedback);
  int create_or_update_feedback_record(const net::ObIpEndpoint &server_addr,
                                       const int64_t table_id,
                                       const int64_t partition_id,
                                       const int64_t feedback_time);
  int64_t get_feedback_count() { return read_stale_feedback_map_.count(); }
  void read_stale_feedback_gc(const int64_t max_gc_count,
                              const int64_t now,
                              const int64_t remove_interval,
                              int64_t &gc_count);
  common::DRWLock &get_lock() { return lock_; }

private:
  int create_feedback_record(const net::ObIpEndpoint &server_addr,
                             const int64_t table_id,
                             const int64_t partition_id,
                             const int64_t feedback_time);
public:
  ObVipReadStaleKey vip_key_;
  ObReadStaleFeedbackMap read_stale_feedback_map_;
  int64_t read_stale_retry_interval_;
  LINK(ObVipReadStaleInfo, vip_read_stale_link_);
private:
  common::DRWLock lock_;
};

struct ObReadStaleParam
{
public:
  int64_t to_string(char *buf, const int64_t buf_len) const;
public:
  ObVipAddr vip_addr_;
  common::ObString tenant_name_;
  common::ObString cluster_name_;
  int64_t retry_interval_;
  net::ObIpEndpoint server_addr_;
  int64_t table_id_;
  int64_t partition_id_;
  bool enable_read_stale_feedback_;
};

class ObReadStaleProcessor
{
public:
  ObReadStaleProcessor(): vip_read_stale_map_(), allocator_(), feedback_clean_cont_(NULL), lock_()  {}
  ~ObReadStaleProcessor() {}
public:
  static const int64_t HASH_BUCKET_SIZE = 32;
  static const int64_t MAX_FEEDBACK_GC_COUNT = 512;
  static const int64_t OB_READ_STALE_FEEDBACK_DEFAULT_VALUE = 5000000;
  struct ObVipReadStaleHashing
  {
    typedef const ObVipReadStaleKey &Key;
    typedef ObVipReadStaleInfo Value;
    typedef ObDLList(ObVipReadStaleInfo, vip_read_stale_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->vip_key_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<ObVipReadStaleHashing, HASH_BUCKET_SIZE> ObVipReadStaleMap;
  typedef ObVipReadStaleMap::iterator VipIterator;

public:
  int create_or_update_vip_feedback_record(const ObReadStaleParam &param);
  int acquire_vip_feedback_record(const ObVipAddr &vip,
                                 const common::ObString &tenant_name,
                                 const common::ObString &cluster_name,
                                 ObVipReadStaleInfo *&stale_info);
  int check_read_stale_state(const ObReadStaleParam &param, bool &is_stale);
  static int alloc_read_stale_feedback(const net::ObIpEndpoint &server_addr,
                                       const int64_t table_id,
                                       const int64_t partition_id,
                                       const int64_t feedback_time,
                                       ObReadStaleFeedback *&feedback);
  static int alloc_vip_stale_info(const ObVipAddr &vip,
                                  const common::ObString &tenant_name,
                                  const common::ObString &cluster_name,
                                  const int64_t retry_interval,
                                  ObVipReadStaleInfo *&stale_info);
  common::DRWLock &get_lock() { return lock_; }
  int start_read_stale_feedback_clean_task();
private:
  ObAsyncCommonTask *get_feedback_clean_cont() { return feedback_clean_cont_; }
  static void update_interval();
  static int do_repeat_task();
  int vip_read_stale_gc();
  int create_vip_feedback_record(const ObReadStaleParam &param, const int64_t now);
public:
  ObVipReadStaleMap vip_read_stale_map_;
  common::ObArenaAllocator allocator_;

private:
  ObAsyncCommonTask *feedback_clean_cont_;
  common::DRWLock lock_;

};

ObReadStaleProcessor &get_global_read_stale_processor();
common::ObArenaAllocator &get_read_stale_allocator();

}
}
}
#endif /* OB_READ_STALE_PROCESSOR*/