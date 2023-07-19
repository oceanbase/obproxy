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
#include "obutils/ob_read_stale_processor.h"
#include "lib/lock/ob_drw_lock.h"
#include "lib/time/ob_time_utility.h"
#include "obutils/ob_config_processor.h"
#include "lib/allocator/ob_mod_define.h"
#include "obutils/ob_proxy_config.h"
#include "lib/container/ob_se_array.h"
#include "iocore/eventsystem/ob_thread.h"
#include "lib/time/ob_hrtime.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::obproxy::obutils;

DEF_TO_STRING(ObReadStaleReplica)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(table_id), K_(partition_id), K_(server_addr));
  J_OBJ_END();
  return pos;
}

bool ObReadStaleReplica::operator == (const ObReadStaleReplica &replica) const
{
  bool bret = (table_id_ == replica.table_id_);
  if (bret && partition_id_ != common::OB_INVALID_INDEX) {
    // partition table
    bret = (partition_id_ == replica.partition_id_);
  }
  if (bret) {
    bret = ops_ip_addr_eq(server_addr_, replica.server_addr_);
  }
  return bret;
}

uint64_t ObReadStaleReplica::hash() const
{
  uint64_t hash_key = common::murmurhash(&table_id_, sizeof(table_id_), 0);
  if (partition_id_ != common::OB_INVALID_INDEX) {
    // partition table
    hash_key = common::murmurhash(&partition_id_, sizeof(partition_id_), hash_key);
  }
  hash_key = server_addr_.hash(hash_key);
  return hash_key;
}

DEF_TO_STRING(ObReadStaleFeedback)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(replica), K_(feedback_time));
  J_OBJ_END();
  return pos;
}

void ObReadStaleFeedback::destroy() {
  get_read_stale_allocator().free(this);
}

DEF_TO_STRING(ObReadStaleParam)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(vip_addr), K_(tenant_name), K_(cluster_name), K_(server_addr), K_(table_id), K_(partition_id), K_(retry_interval), K_(enable_read_stale_feedback));
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObVipReadStaleKey)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(vip_addr), K_(tenant_name), K_(cluster_name));
  J_OBJ_END();
  return pos;
}

bool ObReadStaleFeedback::is_read_stale_feedback_valid(int64_t retry_interval)
{
  bool bret = true;
  int64_t now = hrtime_to_usec(event::get_hrtime());
  if (feedback_time_ + retry_interval <= now) {
    bret = false;
  }
  return bret;
}

bool ObReadStaleFeedback::is_read_stale_feedback_need_remove(const int64_t interval, const int64_t now)
{
  bool bret = false;
  if (feedback_time_ + interval <= now) {
    bret = true;
  }
  return bret;
}

bool ObVipReadStaleKey::operator == (const ObVipReadStaleKey &vip_key) const
{
  bool bret = (vip_addr_ == vip_key.vip_addr_
                && tenant_name_ == vip_key.tenant_name_
                && cluster_name_ == vip_key.cluster_name_);
  return bret;
}

uint64_t ObVipReadStaleKey::hash() const
{
  uint64_t hash_key = common::murmurhash(&vip_addr_.addr_, sizeof(vip_addr_.addr_), 0);
  hash_key = common::murmurhash(tenant_name_.ptr(), tenant_name_.length(), hash_key);
  hash_key = common::murmurhash(cluster_name_.ptr(),  cluster_name_.length(),hash_key);
  return hash_key;
}


int ObVipReadStaleInfo::acquire_feedback_record(const net::ObIpEndpoint &server_addr,
                                                const int64_t table_id,
                                                const int64_t partition_id,
                                                bool &is_stale,
                                                ObReadStaleFeedback *&stale_feedback)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(lock_);
  ObReadStaleReplica replica(server_addr, table_id, partition_id);
  is_stale = false;
  if (OB_FAIL(read_stale_feedback_map_.get_refactored(replica, stale_feedback))) {
    if (ret == OB_HASH_NOT_EXIST) {
      stale_feedback = NULL;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get read stale feedback", K(replica), K(ret));
    }
  } else {
    is_stale = stale_feedback->is_read_stale_feedback_valid(read_stale_retry_interval_);
  }
  return ret;
}

int ObVipReadStaleInfo::create_or_update_feedback_record(const net::ObIpEndpoint &server_addr,
                                                         const int64_t table_id,
                                                         const int64_t partition_id,
                                                         const int64_t feedback_time)
{
  int ret = OB_SUCCESS;
  lock_.rdlock();
  ObReadStaleReplica replica(server_addr, table_id, partition_id);
  ObReadStaleFeedback *feedback = NULL;
  if (OB_FAIL(read_stale_feedback_map_.get_refactored(replica, feedback))) {
    lock_.rdunlock();
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(create_feedback_record(server_addr, table_id, partition_id, feedback_time))) {
        LOG_WARN("fail to create feedback record", K(ret));
      }
    } else {
      LOG_WARN("fail to record read stale feedback", K(server_addr), K(table_id), K(partition_id), K(feedback_time));
    }
  } else {
    feedback->feedback_time_ = feedback_time;
    lock_.rdunlock();
  }
  return ret;
}

int ObVipReadStaleInfo::create_feedback_record(const net::ObIpEndpoint &server_addr,
                                               const int64_t table_id,
                                               const int64_t partition_id,
                                               const int64_t feedback_time)
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(lock_);
  ObReadStaleReplica replica(server_addr, table_id, partition_id);
  ObReadStaleFeedback *feedback = NULL;
  if (OB_FAIL(read_stale_feedback_map_.get_refactored(replica, feedback))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(ObReadStaleProcessor::alloc_read_stale_feedback(server_addr, table_id, partition_id, feedback_time, feedback))) {
        LOG_WARN("fail to alloc mem for read stale feedback", K(ret));
      } else if (OB_FAIL(read_stale_feedback_map_.unique_set(feedback))) {
        feedback->destroy();
        feedback = NULL;
        LOG_WARN("fail to record read stale feedback", K(ret));
      }
    } else {
      LOG_WARN("fail to record read stale feedback", K(server_addr), K(table_id), K(partition_id), K(feedback_time));
    }
  } else {
    feedback->feedback_time_ = feedback_time;
  }
  return ret;
}

void ObVipReadStaleInfo::read_stale_feedback_gc(const int64_t max_gc_count,
                                                const int64_t now,
                                                const int64_t remove_interval,
                                                int64_t &gc_count)
{
  common::ObSEArray<ObReadStaleFeedback*, 64> feedback_gc;
  FeedbackIterator feedback_it = read_stale_feedback_map_.begin();
  FeedbackIterator feedback_end = read_stale_feedback_map_.end();
  for(; feedback_it != feedback_end && gc_count < max_gc_count; ++feedback_it) {
    if (feedback_it->is_read_stale_feedback_need_remove(remove_interval, now)) {
      gc_count ++;
      feedback_gc.push_back(feedback_it.value_);
    }
  }
  if (feedback_gc.count() > 0) {
    lock_.wrlock();
    for (int i = 0; i < feedback_gc.count(); i++) {
      read_stale_feedback_map_.remove(feedback_gc[i]);
      feedback_gc[i]->destroy();
    }
    lock_.wrunlock();
  }
}

DEF_TO_STRING(ObVipReadStaleInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(vip_key));
  J_OBJ_END();
  return pos;
}

void ObVipReadStaleInfo::destroy() {
  get_read_stale_allocator().free(this);
}

ObReadStaleProcessor &get_global_read_stale_processor()
{
  static ObReadStaleProcessor processor;
  return processor;
}

common::ObArenaAllocator &get_read_stale_allocator()
{
  return get_global_read_stale_processor().allocator_;
}

int ObReadStaleProcessor::create_or_update_vip_feedback_record(const ObReadStaleParam &param)
{
  int ret = OB_SUCCESS;
  if (param.enable_read_stale_feedback_ && param.table_id_ != OB_INVALID_INDEX) {
    lock_.rdlock();
    ObVipReadStaleKey key(param.vip_addr_, param.tenant_name_, param.cluster_name_);
    ObVipReadStaleInfo *vip_stale_info;
    int64_t now = hrtime_to_usec(event::get_hrtime());
    if (OB_FAIL(vip_read_stale_map_.get_refactored(key, vip_stale_info))) {
      lock_.rdunlock();
      if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
        if (OB_FAIL(create_vip_feedback_record(param, now))) {
          LOG_WARN("fail to acqure or create vip read stale info", K(ret));
        }
      } else {
        LOG_WARN("fail to get vip read stale record", K(param.vip_addr_), K(param.tenant_name_), K(param.cluster_name_), K(ret));
      }
    } else {
      if (OB_FAIL(vip_stale_info->create_or_update_feedback_record(param.server_addr_, param.table_id_, param.partition_id_, now))) {
        LOG_WARN("fail to create feedback record", K(param.server_addr_), K(param.table_id_), K(param.partition_id_), K(ret));
      }
      lock_.rdunlock();
    }
  }
  return ret;
}

int ObReadStaleProcessor::create_vip_feedback_record(const ObReadStaleParam &param, const int64_t now)
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(lock_);
  ObVipReadStaleKey key(param.vip_addr_, param.tenant_name_, param.cluster_name_);
  ObVipReadStaleInfo *vip_info = NULL;
  if (OB_FAIL(vip_read_stale_map_.get_refactored(key, vip_info))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      if (OB_FAIL(alloc_vip_stale_info(param.vip_addr_, param.tenant_name_, param.cluster_name_, param.retry_interval_, vip_info))) {
        vip_info = NULL;
        LOG_WARN("fail to alloc mem for vip read stale info", K(key), K(ret));
      } else if (OB_FAIL(vip_read_stale_map_.unique_set(vip_info))) {
        vip_info->destroy();
        vip_info = NULL;
        LOG_WARN("fail to record vip read stale info", K(key), K(ret));
      }
    } else {
      vip_info = NULL;
      LOG_WARN("fail to get vip read stale info", K(key), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (vip_info->create_or_update_feedback_record(param.server_addr_, param.table_id_, param.partition_id_, now)) {
      LOG_WARN("fail to create feedback record", K(param.server_addr_), K(param.table_id_), K(param.partition_id_), K(ret));
    }
  }
  return ret;
}

int ObReadStaleProcessor::check_read_stale_state(const ObReadStaleParam &param, bool &is_stale)
{
  int ret = OB_SUCCESS;
  if (param.enable_read_stale_feedback_ && param.table_id_ != OB_INVALID_INDEX) {
    DRWLock::RDLockGuard guard(lock_);
    ObVipReadStaleKey key(param.vip_addr_ , param.tenant_name_, param.cluster_name_);
    ObVipReadStaleInfo *vip_stale_info = NULL;
    ObReadStaleFeedback *feedback = NULL;
    is_stale = false;
    if (OB_FAIL(vip_read_stale_map_.get_refactored(ObVipReadStaleKey(param.vip_addr_, param.tenant_name_, param.cluster_name_), vip_stale_info))) {
      if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
        // vip stale info not exist, no stale
      } else {
        LOG_WARN("fail to get vip read stale info", K(param.vip_addr_), K(param.tenant_name_), K(param.cluster_name_), K(ret));
      }
    } else if (OB_FALSE_IT(vip_stale_info->read_stale_retry_interval_ = param.retry_interval_)) {
    } else if (OB_FAIL(vip_stale_info->acquire_feedback_record(param.server_addr_, param.table_id_, param.partition_id_, is_stale, feedback))) {
      LOG_WARN("fail to get vip read stale info", K(param.server_addr_), K(param.table_id_), K(param.partition_id_), K(ret));
    }
  } else {
    is_stale = false;
  }
  return ret;
}

int ObReadStaleProcessor::alloc_vip_stale_info(const ObVipAddr &vip,
                                               const common::ObString &tenant_name,
                                               const common::ObString &cluster_name,
                                               const int64_t retry_interval,
                                               ObVipReadStaleInfo *&stale_info)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = sizeof(ObVipReadStaleInfo) + tenant_name.length() + cluster_name.length();
  char *ptr = NULL;
  int64_t pos = 0;
  stale_info = NULL;
  if (OB_ISNULL(ptr = static_cast<char *>(get_read_stale_allocator().alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_SM_LOG(WARN, "fail to alloc mem for ob vip read stale info", K(ret));
  } else {
    stale_info = new (ptr) ObVipReadStaleInfo();
    pos += sizeof(ObVipReadStaleInfo);
    MEMCPY(ptr + pos, tenant_name.ptr(), tenant_name.length());
    stale_info->vip_key_.tenant_name_.assign_ptr(ptr + pos, tenant_name.length());
    pos += tenant_name.length();
    MEMCPY(ptr + pos, cluster_name.ptr(), cluster_name.length());
    stale_info->vip_key_.cluster_name_.assign_ptr(ptr + pos, cluster_name.length());
    stale_info->vip_key_.vip_addr_ = vip;
    stale_info->read_stale_retry_interval_ = retry_interval;
  }
  return ret;
}

int ObReadStaleProcessor::alloc_read_stale_feedback(const net::ObIpEndpoint &server_addr,
                                                    const int64_t table_id,
                                                    const int64_t partition_id,
                                                    const int64_t feedback_time,
                                                    ObReadStaleFeedback *&feedback)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  feedback = NULL;
  if (OB_ISNULL(ptr = get_read_stale_allocator().alloc(sizeof(ObReadStaleFeedback)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_SM_LOG(WARN, "fail to alloc mem for ob read stale feedback", K(ret));
  } else {
    feedback = new (ptr) ObReadStaleFeedback();
    feedback->replica_.server_addr_ = server_addr;
    feedback->replica_.table_id_ = table_id;
    feedback->replica_.partition_id_ = partition_id;
    feedback->feedback_time_ = feedback_time;
  }
  return ret;
}

int ObReadStaleProcessor::acquire_vip_feedback_record(const ObVipAddr &vip,
                                                     const common::ObString &tenant_name,
                                                     const common::ObString &cluster_name,
                                                     ObVipReadStaleInfo *&stale_info)
{
  // lock on the outside
  int ret = OB_SUCCESS;
  ObVipReadStaleKey key(vip, tenant_name, cluster_name);
  stale_info = NULL;
  if (OB_FAIL(vip_read_stale_map_.get_refactored(key, stale_info))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get vip read stale feedback map", K(vip), K(tenant_name), K(cluster_name), K(ret));
    }
  }
  return ret;
}

int ObReadStaleProcessor::vip_read_stale_gc()
{
  int ret = OB_SUCCESS;
  int64_t gc_count = 0;
  // only remove feedback here, dont't need to lock on iterate
  common::ObSEArray<ObVipReadStaleInfo*, 16> vip_gc;
  int64_t now = hrtime_to_usec(event::get_hrtime());
  int64_t remove_interval = get_global_proxy_config().read_stale_remove_interval;
  VipIterator it = vip_read_stale_map_.begin();
  VipIterator end = vip_read_stale_map_.end();

  for (; it != end && gc_count < MAX_FEEDBACK_GC_COUNT; ++it) {
    it->read_stale_feedback_gc(MAX_FEEDBACK_GC_COUNT, now, remove_interval, gc_count);
    if (it->get_feedback_count() == 0) {
      vip_gc.push_back(it.value_);
    }
  }

  if (vip_gc.count() > 0){
    lock_.wrlock();
    for (int i = 0; i < vip_gc.count(); i++) {
      if (vip_gc[i]->get_feedback_count() == 0) {
        vip_read_stale_map_.remove(vip_gc[i]);
        vip_gc[i]->destroy();
      }
    }
    lock_.wrunlock();
  }
  return ret;
}

int ObReadStaleProcessor::start_read_stale_feedback_clean_task()
{
  int ret = OB_SUCCESS;
  get_read_stale_allocator().set_mod_id(ObModIds::OB_PROXY_READ_STALE_MAP);
  if (OB_UNLIKELY(NULL != feedback_clean_cont_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read stale feedback clean cont has been scheduled", K(ret));
  } else {
    int64_t interval = ObRandomNumUtils::get_random_half_to_full(get_global_proxy_config().cache_cleaner_clean_interval);
    if (OB_ISNULL(feedback_clean_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(interval,
                                                            "read_stale_feedback_clean_task",
                                                            ObReadStaleProcessor::do_repeat_task,
                                                            ObReadStaleProcessor::update_interval))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create and start read stale feedback clean task", K(ret));
    } else {
      LOG_INFO("succ to start read stale feedback clean task", K(interval));
    }
  }
  return ret;
}

void ObReadStaleProcessor::update_interval()
{
  ObAsyncCommonTask *cont = get_global_read_stale_processor().get_feedback_clean_cont();
  if (OB_LIKELY(NULL != cont)) {
    int64_t interval = ObRandomNumUtils::get_random_half_to_full(get_global_proxy_config().cache_cleaner_clean_interval);
    cont->set_interval(interval);
  }
}

int ObReadStaleProcessor::do_repeat_task()
{
  return get_global_read_stale_processor().vip_read_stale_gc();
}

}
}
}
