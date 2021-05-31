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

#include "lib/stat/ob_di_cache.h"
#include "lib/random/ob_random.h"
#include "lib/stat/ob_session_stat.h"

namespace oceanbase
{
namespace common
{
DIRWLock::DIRWLock()
  : lock_(0)
{
}

DIRWLock::~DIRWLock()
{
}

int DIRWLock::try_rdlock()
{
  int ret = OB_EAGAIN;
  uint32_t lock = lock_;
  if (0 == (lock & WRITE_MASK)) {
    if (ATOMIC_BCAS(&lock_, lock, lock + 1)) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int DIRWLock::try_wrlock()
{
  int ret = OB_EAGAIN;
  if (ATOMIC_BCAS(&lock_, 0, WRITE_MASK)) {
    ret = OB_SUCCESS;
  }
  return ret;
}

void DIRWLock::rdlock()
{
  uint32_t lock = 0;
  while (1) {
    lock = lock_;
    if (0 == (lock & WRITE_MASK)) {
      if (ATOMIC_BCAS(&lock_, lock, lock + 1)) {
        break;
      }
    }
#if defined(__aarch64__)
    asm("yield");
#else
    asm("pause");
#endif
  }
}

void DIRWLock::wrlock()
{
  while (!ATOMIC_BCAS(&lock_, 0, WRITE_MASK)) {
#if defined(__aarch64__)
    asm("yield");
#else
    asm("pause");
#endif
  }
}

void DIRWLock::wr2rdlock()
{
  while (!ATOMIC_BCAS(&lock_, WRITE_MASK, 1)) {
#if defined(__aarch64__)
    asm("yield");
#else
    asm("pause");
#endif
  }
}

void DIRWLock::unlock()
{
  uint32_t lock = 0;
  lock = lock_;
  if (0 != (lock & WRITE_MASK)) {
    ATOMIC_STORE(&lock_, 0);
  } else {
    (void)(ATOMIC_AAF(&lock_, -1));
  }
}

ObDISessionCollect::ObDISessionCollect()
  : session_id_(0),
    base_value_(),
    lock_()
{
}

ObDISessionCollect::~ObDISessionCollect()
{
}

void ObDISessionCollect::clean()
{
  session_id_ = 0;
  base_value_.reset();
}

ObDITenantCollect::ObDITenantCollect()
  : tenant_id_(0),
    last_access_time_(0),
    base_value_()
{
}

ObDITenantCollect::~ObDITenantCollect()
{
}

void ObDITenantCollect::clean()
{
  tenant_id_ = 0;
  last_access_time_ = 0;
  base_value_.reset();
}

/*
 * -------------------------------------------------------ObDICache---------------------------------------------------------------
 */
ObDISessionCache::ObDISessionCache()
  : di_map_(),
    collects_()
{
}

ObDISessionCache::~ObDISessionCache()
{
}

ObDISessionCache &ObDISessionCache::get_instance()
{
  static ObDISessionCache instance_;
  return instance_;
}

int ObDISessionCache::get_node(uint64_t session_id, ObDISessionCollect *&session_collect)
{
  int ret = OB_SUCCESS;
  ObRandom *random = ObDITls<ObRandom>::get_instance();
  ObSessionBucket &bucket = di_map_[session_id % OB_MAX_SERVER_SESSION_CNT];
  while (1) {
    bucket.lock_.rdlock();
    if (OB_SUCCESS == (ret = bucket.get_the_node(session_id, session_collect))) {
      if (OB_SUCCESS == (ret = session_collect->lock_.try_rdlock())) {
        bucket.lock_.unlock();
        break;
      }
    }
    if (OB_SUCCESS != ret) {
      bucket.lock_.unlock();
      int64_t pos = 0;
      while (1) {
        pos = random->get(0, OB_MAX_SERVER_SESSION_CNT-1);
        if (OB_SUCCESS == (ret = collects_[pos].lock_.try_wrlock())) {
          break;
        }
      }
      if (OB_SUCCESS == ret) {
        if (0 != collects_[pos].session_id_) {
          ObSessionBucket &des_bucket = di_map_[collects_[pos].session_id_ % OB_MAX_SERVER_SESSION_CNT];
          des_bucket.lock_.wrlock();
          des_bucket.list_.remove(&collects_[pos]);
          collects_[pos].clean();
          des_bucket.lock_.unlock();
        }
        bucket.lock_.wrlock();
        if (OB_SUCCESS != (ret = bucket.get_the_node(session_id, session_collect))) {
          ret = OB_SUCCESS;
          bucket.list_.add_last(&collects_[pos]);
          collects_[pos].session_id_ = session_id;
          bucket.lock_.unlock();
          session_collect = &collects_[pos];
          collects_[pos].lock_.wr2rdlock();
          break;
        } else {
          if (OB_SUCCESS == (ret = session_collect->lock_.try_rdlock())) {
            bucket.lock_.unlock();
            collects_[pos].lock_.unlock();
            break;
          } else {
            bucket.lock_.unlock();
            collects_[pos].lock_.unlock();
          }
        }
      }
    }
  }
  return ret;
}

int ObDISessionCache::get_all_diag_info(ObIArray<std::pair<uint64_t, ObDISessionCollect*> > &diag_infos)
{
  int ret = OB_SUCCESS;
  std::pair<uint64_t, ObDISessionCollect*> pair;
  ObDISessionCollect *head = NULL;
  ObDISessionCollect *node = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < OB_MAX_SERVER_SESSION_CNT; ++i) {
    ObSessionBucket &bucket = di_map_[i];
    bucket.lock_.rdlock();
    head = bucket.list_.get_header();
    node = bucket.list_.get_first();
    while (head != node && NULL != node && OB_SUCC(ret)) {
      pair.first = node->session_id_;
      pair.second = node;
      if (OB_SUCCESS != (ret = diag_infos.push_back(pair))) {
      } else {
        node = node->next_;
      }
    }
    bucket.lock_.unlock();
  }
  return ret;
}

int ObDISessionCache::get_the_diag_info(
  uint64_t session_id,
  ObDISessionCollect *&diag_infos)
{
  int ret = OB_SUCCESS;
  ObSessionBucket &bucket = di_map_[session_id % OB_MAX_SERVER_SESSION_CNT];
  bucket.lock_.rdlock();
  ObDISessionCollect *collect = NULL;
  if (OB_SUCCESS == (ret = bucket.get_the_node(session_id, collect))) {
    diag_infos = collect;
  }
  bucket.lock_.unlock();
  return ret;
}

ObDITenantCache::ObDITenantCache()
  : di_map_(),
    collects_(),
    last_access_time_(0)
{
  ObDIGlobalTenantCache::get_instance().link(this);
}

ObDITenantCache::~ObDITenantCache()
{
  ObDIGlobalTenantCache::get_instance().unlink(this);
}

int ObDITenantCache::get_node(uint64_t tenant_id, ObDITenantCollect *&tenant_collect)
{
  int ret = OB_SUCCESS;
  ObTenantBucket &bucket = di_map_[tenant_id % MAX_TENANT_NODE_NUM];
  if (OB_SUCCESS == (ret = bucket.get_the_node(tenant_id, tenant_collect))) {
    last_access_time_++;
    tenant_collect->last_access_time_ = last_access_time_;
  } else {
    ret = add_node(tenant_id, tenant_collect);
  }
  return ret;
}

int ObDITenantCache::add_node(uint64_t tenant_id, ObDITenantCollect *&tenant_collect)
{
  int ret = OB_SUCCESS;
  int64_t pos = -1;
  uint64_t last_access_time = last_access_time_;
  for (int64_t i = 0; i < MAX_TENANT_NODE_NUM; i++) {
    if (0 == collects_[i].last_access_time_) {
      pos = i;
      break;
    } else {
      if (collects_[i].last_access_time_ < last_access_time) {
        pos = i;
        last_access_time = collects_[i].last_access_time_;
      }
    }
  }
  if (-1 != pos) {
    if (0 != collects_[pos].last_access_time_) {
      del_node(collects_[pos].tenant_id_);
    }
    last_access_time_++;
    collects_[pos].tenant_id_ = tenant_id;
    collects_[pos].last_access_time_ = last_access_time_;
    collects_[pos].base_value_.reset();
    ObTenantBucket &bucket = di_map_[tenant_id % MAX_TENANT_NODE_NUM];
    bucket.list_.add_last(&collects_[pos]);
    tenant_collect = &collects_[pos];
  }
  return ret;
}

int ObDITenantCache::del_node(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantBucket &bucket = di_map_[tenant_id % MAX_TENANT_NODE_NUM];
  ObDITenantCollect *collect = NULL;
  if (OB_SUCCESS == (ret = bucket.get_the_node(tenant_id, collect))) {
    bucket.list_.remove(collect);
    collect->last_access_time_ = 0;
  }
  return ret;
}

int ObDITenantCache::get_the_diag_info(
  uint64_t tenant_id,
  ObDiagnoseTenantInfo &diag_infos)
{
  int ret = OB_SUCCESS;
  ObTenantBucket &bucket = di_map_[tenant_id % MAX_TENANT_NODE_NUM];
  ObDITenantCollect *collect = NULL;
  if (OB_SUCCESS == (ret = bucket.get_the_node(tenant_id, collect))) {
    diag_infos.add(collect->base_value_);
  }
  return ret;
}

template<class _callback>
int ObDITenantCache::get_all_node(ObIAllocator &allocator, ObTenantBucket *di_map, _callback &callback)
{
  int ret = OB_SUCCESS;
  ObDITenantCollect *collect = NULL;
  void *buf = NULL;
  for (int64_t i = 0; i < MAX_TENANT_NODE_NUM && OB_SUCC(ret); i++) {
    if (0 != collects_[i].last_access_time_) {
      ObTenantBucket &bucket = di_map[collects_[i].tenant_id_ % MAX_TENANT_NODE_NUM];
      if (OB_SUCCESS == (ret = bucket.get_the_node(collects_[i].tenant_id_, collect))) {
        callback(collect->base_value_, collects_[i].base_value_);
      } else {
        ret = OB_SUCCESS;
        if (NULL == (buf = allocator.alloc(sizeof(ObDITenantCollect)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          collect = new (buf) ObDITenantCollect();
          collect->tenant_id_ = collects_[i].tenant_id_;
          callback(collect->base_value_, collects_[i].base_value_);
          bucket.list_.add_last(collect);
        }
      }
    }
  }
  return ret;
}

ObDIGlobalTenantCache::ObDIGlobalTenantCache()
  : list_(),
    cnt_(0),
    lock_()
{
}

ObDIGlobalTenantCache::~ObDIGlobalTenantCache()
{
}

ObDIGlobalTenantCache &ObDIGlobalTenantCache::get_instance()
{
  static ObDIGlobalTenantCache instance_;
  return instance_;
}

void ObDIGlobalTenantCache::link(ObDITenantCache *node)
{
  lock_.wrlock();
  list_.add_last(node);
  cnt_++;
  lock_.unlock();
}

void ObDIGlobalTenantCache::unlink(ObDITenantCache *node)
{
  lock_.wrlock();
  list_.remove(node);
  cnt_--;
  lock_.unlock();
}

int ObDIGlobalTenantCache::get_the_diag_info(
  uint64_t tenant_id,
  ObDiagnoseTenantInfo &diag_infos)
{
  int ret = OB_SUCCESS;
  lock_.rdlock();
  if (0 != cnt_) {
    ObDITenantCollect *node = NULL;
    ObDITenantCache *tenant_cache = list_.get_first();
    while (list_.get_header() != tenant_cache && NULL != tenant_cache) {
      if (OB_SUCCESS == tenant_cache->get_node(tenant_id, node)) {
        diag_infos.add(node->base_value_);
      }
      tenant_cache = tenant_cache->next_;
    }
  }
  lock_.unlock();
  return ret;
}

int ObDIGlobalTenantCache::get_all_wait_event(
    ObIAllocator &allocator,
    ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos)
{
  int ret = OB_SUCCESS;
  AddWaitEvent adder;
  ret = get_all_diag_info(allocator, diag_infos, adder);
  return ret;
}

int ObDIGlobalTenantCache::get_all_stat_event(
    ObIAllocator &allocator,
    ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos)
{
  int ret = OB_SUCCESS;
  AddStatEvent adder;
  ret = get_all_diag_info(allocator, diag_infos, adder);
  return ret;
}

int ObDIGlobalTenantCache::get_all_latch_stat(
    ObIAllocator &allocator,
    ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos)
{
  int ret = OB_SUCCESS;
  AddLatchStat adder;
  ret = get_all_diag_info(allocator, diag_infos, adder);
  return ret;
}

template<class _callback>
int ObDIGlobalTenantCache::get_all_diag_info(
    ObIAllocator &allocator,
    ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos, _callback &callback)
{
  int ret = OB_SUCCESS;
  std::pair<uint64_t, ObDiagnoseTenantInfo*> pair;
  ObTenantBucket di_map[ObDITenantCache::MAX_TENANT_NODE_NUM];
  lock_.rdlock();
  if (0 != cnt_) {
    ObDITenantCache *tenant_cache = list_.get_first();
    while (list_.get_header() != tenant_cache && NULL != tenant_cache) {
      tenant_cache->get_all_node(allocator, di_map, callback);
      tenant_cache = tenant_cache->next_;
    }
  }
  lock_.unlock();
  ObDITenantCollect *head = NULL;
  ObDITenantCollect *node = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < ObDITenantCache::MAX_TENANT_NODE_NUM; ++i) {
    ObTenantBucket &bucket = di_map[i];
    head = bucket.list_.get_header();
    node = bucket.list_.get_first();
    while (head != node && NULL != node && OB_SUCC(ret)) {
      pair.first = node->tenant_id_;
      pair.second = &(node->base_value_);
      if (OB_SUCCESS != (ret = diag_infos.push_back(pair))) {
      } else {
        node = node->next_;
      }
    }
  }
  return ret;
}

}
}
