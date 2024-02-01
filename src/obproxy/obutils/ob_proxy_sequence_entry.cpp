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
#include "obutils/ob_proxy_sequence_entry.h"
#include "obutils/ob_proxy_sequence_entry_cont.h"
#include "utils/ob_proxy_utils.h"
#include "proxy/client/ob_mysql_proxy.h"



using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::event;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
const int64_t PRFETCH_THRESHOLD_BASE = 10000;
DEF_TO_STRING(ObSequenceInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(seq_id), K_(seq_name), K_(tnt_id), K_(tnt_col), K_(gmt_create), K_(gmt_modified), K_(db_timestamp), K(err_msg_), K_(is_over),
       K_(value), K_(step), K_(min_value), K_(max_value), K_(local_value), K_(last_renew_time), K(errno_));
  J_OBJ_END();
  return pos;
}
ObSequenceInfo::ObSequenceInfo():
  seq_id_(0), seq_name_(0), tnt_id_(0), tnt_col_(),
  gmt_create_(0), gmt_modified_(0), db_timestamp_(0), timestamp_(0), err_msg_(0),
  value_(0), step_(0), min_value_(0), max_value_(0),
  local_value_(0), is_over_(true), last_renew_time_(0), errno_(0)
{
}

bool ObSequenceInfo::is_expired() {
  int64_t now_time = event::get_hrtime();//ObTimeUtility::current_time();
  int64_t interval = HRTIME_USECONDS(get_global_proxy_config().sequence_entry_expire_time);
  LOG_DEBUG("is_expired:", K(now_time), K(last_renew_time_), K(interval));
  return  now_time - last_renew_time_ > interval;
}

DEF_TO_STRING(ObSequenceRouteParam)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(cluster_name), K_(tenant_name), K_(database_name),
       K_(seq_name), K_(table_name), K_(seq_id), K_(tnt_id), K_(tnt_col),
       K_(username), K_(need_db_timestamp), K_(only_need_db_timestamp),
       K_(min_value), K_(max_value), K_(step), K_(retry_count));
  J_OBJ_END();
  return pos;
}
void ObSequenceRouteParam::reset() {
  cont_ = NULL;
  need_db_timestamp_ = false;
  need_value_ = false;
  only_need_db_timestamp_ = false;
  cluster_name_.reset();
  tenant_name_.reset();
  database_name_.reset();
  seq_name_.reset();
  table_name_.reset();
  seq_id_.reset();
  tnt_id_.reset();
  tnt_col_.reset();
  username_.reset();
  password_.reset();
  min_value_ = 0;
  max_value_ = 0;
  step_ = 0;
  retry_count_ = 0;
  shard_conn_ = NULL;
}

ObSequenceEntry::ObSequenceEntry():
  is_timestamp_fetching_(false),
  se_state_(SE_INITING),
  allow_insert_(true)
{

}
int ObSequenceEntry::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pending_list_.init("sequence init pending list",
                                 reinterpret_cast<int64_t>(&(reinterpret_cast<ObProxySequenceEntryCont*>(0))->link_)))) {
    LOG_WDIAG("fail to init pending list", K(ret));
  } else if (OB_FAIL(timestamp_pending_list_.init("sequence init dbtimestamp pending list",
                     reinterpret_cast<int64_t>(&(reinterpret_cast<ObProxySequenceEntryCont*>(0))->link_)))) {
    LOG_WDIAG("fail to init pending list", K(ret));
  } else {
  }
  return ret;
}

bool ObSequenceEntry::need_prefetch(int64_t ratio)
{
  if (sequence_info_.local_value_ - sequence_info_.value_ >=
      sequence_info_.step_ * ratio / PRFETCH_THRESHOLD_BASE &&
      !sequence_info2_.is_valid()) {
    // only sequence_info_ used and sequence_info_2 is empty need prefetch
    return true;
  }
  return false;
}

void ObSequenceEntry::change_use_info2_if_need() {
  if (!sequence_info_.is_valid() &&
      sequence_info2_.is_valid()) {
    LOG_INFO("change use info2 now", K(sequence_info2_), K(sequence_info_));
    sequence_info_ = sequence_info2_;
    sequence_info2_.reset();
  }
}

void ObSequenceEntryCache::destroy()
{
  for (SequenceEntryHashMap::iterator it = sequence_entry_map_.begin(); it != sequence_entry_map_.end(); ++it) {
    it->dec_ref();
  }
  sequence_entry_map_.reset();
}

int ObSequenceEntryCache::start_async_cont_if_need(ObSequenceRouteParam& param,
    ObSequenceEntry* sequence_entry)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter start_async_cont_if_need", K(param.seq_id_));
  // here first change, if change to disable, sequence_info2_ will also can be used
  sequence_entry->change_use_info2_if_need();
  if (!get_global_proxy_config().enable_sequence_prefetch) {
    LOG_DEBUG("prefetch is disabled, return directly", K(param.seq_id_));
    return ret;
  }
  int64_t prefetch_threshold = get_global_proxy_config().sequence_prefetch_threshold;
  if (sequence_entry->is_remote_fetching_state()) {
    LOG_DEBUG("now is already remote fetching, just return", K(param.seq_id_));
  } else if (sequence_entry->need_prefetch(prefetch_threshold)) {
    LOG_DEBUG("now need need_async_fetch ", K(sequence_entry->sequence_info_),
              K(sequence_entry->sequence_info2_),
              K(prefetch_threshold));
    ObProxySequenceEntryCont* sequence_entry_cont_async = NULL;
    if (OB_ISNULL(sequence_entry_cont_async = op_alloc_args(ObProxySequenceEntryCont, param.cont_, &self_ethread()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc ObProxySequenceEntryCont", K(ret));
    } else if (OB_FAIL(sequence_entry_cont_async->init(param, sequence_entry, OB_SEQUENCE_CONT_ASYNC_TYPE))) {
      LOG_WDIAG("fail to init sequence_entry_cont");
    } else if (OB_FAIL(sequence_entry_cont_async->do_create_sequence_entry())) {
      LOG_WDIAG("fail to do_create_sequence_entry", K(ret));
      sequence_entry->set_dead_state();
    } else {
      LOG_DEBUG("succ to do a async fetch", K(param.seq_id_), K(sequence_entry->sequence_info_),
                K(sequence_entry->sequence_info2_));
      sequence_entry->set_remote_fetching_state();
    }
    if (OB_FAIL(ret) && OB_LIKELY(NULL != sequence_entry_cont_async)) {
      LOG_WDIAG("something wrong in create async cont", K(param.seq_id_), K(ret));
      sequence_entry_cont_async->destroy();
      sequence_entry_cont_async = NULL;
    }
  } else {
    LOG_DEBUG("no need prefetch");
  }
  return ret;
}
int ObSequenceEntryCache::create_entry_cont_task(ObSequenceEntry* sequence_entry,
    const ObSequenceRouteParam& param,
    event::ObAction *&action,
    bool is_timestamp_task)
{
  int ret = OB_SUCCESS;
  ObProxySequenceEntryCont* sequence_entry_cont = NULL;
  if (OB_ISNULL(sequence_entry_cont = op_alloc_args(ObProxySequenceEntryCont, param.cont_, &self_ethread()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc ObProxySequenceEntryCont", K(ret), K(param.seq_id_));
  } else if (OB_FAIL(sequence_entry_cont->init(param, sequence_entry, OB_SEQUENCE_CONT_SYNC_TYPE))) {
    LOG_WDIAG("fail to init sequence_entry_cont", K(ret));
  } else if (is_timestamp_task) {
    if (sequence_entry->is_timestamp_fetching()) {
      // add to sequence_timestamp_pending_list;
      LOG_DEBUG("already in timestamp fetching, juset add to list", K(param.seq_id_));
      sequence_entry->timestamp_pending_list_.push(sequence_entry_cont);
    } else {
      LOG_DEBUG("first timestamp remote, do remote fetching now", K(param.seq_id_));
      if (OB_FAIL(sequence_entry_cont->do_create_sequence_entry())) {
        LOG_WDIAG("fail to do_create_sequence_entry", K(ret));
      } else {
        sequence_entry->set_timestamp_fetching(true);
        action = &sequence_entry_cont->get_action();
      }
    }
  } else {
    if (sequence_entry->is_remote_fetching_state()) {
      LOG_DEBUG("already is remote fetching, just add to pending list", K(param.seq_id_));
      sequence_entry->pending_list_.push(sequence_entry_cont);
    } else {
      LOG_DEBUG("first remote ,do remote fetching now", K(param.seq_id_));
      if (OB_FAIL(sequence_entry_cont->do_create_sequence_entry())) {
        LOG_WDIAG("fail to do_create_sequence_entry", K(ret));
      } else {
        sequence_entry->set_remote_fetching_state();
        action = &sequence_entry_cont->get_action();
      }
    }
  }
  if (OB_FAIL(ret) && OB_LIKELY(NULL != sequence_entry_cont)) {
    LOG_WDIAG("something wrong in create async cont", K(param.seq_id_), K(ret));
    sequence_entry_cont->destroy();
    sequence_entry_cont = NULL;
  }
  return ret;
}

int ObSequenceEntryCache::get_next_sequence(ObSequenceRouteParam& param,
    process_seq_pfn process_sequence_info, event::ObAction *&action)
{
  int ret = OB_SUCCESS;
  ObSequenceEntry* sequence_entry = NULL;
  // first check if exist
  if (OB_FAIL(sequence_entry_map_.get_refactored(param.seq_id_, sequence_entry))) {
    LOG_DEBUG("seq not in map", K(param.seq_id_));
  }
  if (ret == OB_HASH_NOT_EXIST) {
    obsys::CWLockGuard guard(rwlock_);
    if (OB_FAIL(sequence_entry_map_.get_refactored(param.seq_id_, sequence_entry))) {
      LOG_DEBUG("seq still not in map", K(param.seq_id_));
      sequence_entry = op_alloc(ObSequenceEntry);
      if (OB_ISNULL(sequence_entry)) {
        LOG_WDIAG("op_alloc ObSequenceEntry failed", K(param.seq_id_));
        return OB_ALLOCATE_MEMORY_FAILED;
      }
      sequence_entry->inc_ref();
      sequence_entry->sequence_info_.seq_id_.set_value(param.seq_id_);
      sequence_entry->sequence_info_.seq_name_.set_value(param.seq_name_);
      sequence_entry->sequence_info_.tnt_id_.set_value(param.tnt_id_);
      sequence_entry->sequence_info_.tnt_col_.set_value(param.tnt_col_);
      if (OB_FAIL(sequence_entry->init())) {
        LOG_WDIAG("fail to init entry", K(ret), K(param.seq_id_));
        sequence_entry->dec_ref();
        ret  = OB_ERR_UNEXPECTED;
        return ret;
      } else if (OB_FAIL(sequence_entry_map_.unique_set(sequence_entry))) {
        LOG_WDIAG("fail to add to map", K(ret), K(param.seq_id_));
        sequence_entry->dec_ref();
        ret  = OB_ERR_UNEXPECTED;
        return ret;
      } else {
        LOG_INFO("succ add to map, map count now is ", K(param.seq_id_),
                  K(sequence_entry_map_.count()));
      }
    }
  }
  bool return_from_local = false;
  ObSequenceInfo* seq_info = NULL;
  if (OB_ISNULL(seq_info = op_alloc(ObSequenceInfo))) {
    // allocate here for reduce return_from_local logic lock time
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_EDIAG("failt to allocate ObSequenceInfo", K(param.seq_id_));
    return ret;
  }
  { // map exist now but maybe not value, we handle remote fetch here
    obsys::CWLockGuard guard(sequence_entry->rwlock_);
    if ((!param.need_value_ || sequence_entry->is_valid())) {
      // valid and not need timestamp we can return from cache
      start_async_cont_if_need(param, sequence_entry);
      if (!param.need_db_timestamp_) {
        LOG_DEBUG("using local value return now:", K(sequence_entry->sequence_info_));
        *seq_info = sequence_entry->sequence_info_;
        return_from_local = true; // here set tag for release lock
      } else {
        // fetch timestamp
        LOG_DEBUG("local value is valid and only need timestamp");
        param.only_need_db_timestamp_ = true;
        if (OB_FAIL(create_entry_cont_task(sequence_entry, param, action, true))) {
          LOG_WDIAG("create_entry_cont_task for timestamp failed", K(param.seq_id_));
        }
      }
      if (param.need_value_) {
        // if need_value_, increase local value now
        ++sequence_entry->sequence_info_.local_value_;
      }
    } else {
      LOG_DEBUG("local value not valid, will fetch remote", K(sequence_entry->sequence_info_));
      // need remote fetch
      if (OB_FAIL(create_entry_cont_task(sequence_entry, param, action, false))) {
        LOG_WDIAG("create_entry_cont_task for sequence failed", K(param.seq_id_), K(ret));
      }
    }
  }
  if (return_from_local) {
    LOG_DEBUG("return from local now", KPC(seq_info));
    if (((param.cont_->*process_sequence_info)(seq_info)) != EVENT_CONT) {
      LOG_WDIAG("process_sequence_info failed", K(ret), K(param.seq_id_));
    }
  } else {
    if (seq_info != NULL) {
      op_free(seq_info);
      seq_info = NULL;
    }
  }
  return ret;
}
ObMysqlProxyEntry::ObMysqlProxyEntry(): mysql_proxy_(NULL)
{
  reset();
}
ObMysqlProxyEntry::~ObMysqlProxyEntry()
{
}
void ObMysqlProxyEntry::destroy()
{
  if (mysql_proxy_ != NULL) {
    op_free(mysql_proxy_);
    mysql_proxy_ = NULL;
  }
  op_free(this);
}

void ObMysqlProxyCache::destroy()
{
  for (MysqlProxyHashMap::iterator it = proxy_entry_map_.begin(); it != proxy_entry_map_.end(); ++it) {
    it->dec_ref();
  }
  proxy_entry_map_.reset();
}

ObMysqlProxyEntry* ObMysqlProxyCache::accquire_mysql_proxy_entry(const common::ObString & proxy_id)
{
  int ret = OB_SUCCESS;
  obsys::CRLockGuard guard(rwlock_);
  ObMysqlProxyEntry* mysql_proxy_entry = NULL;
  if (OB_FAIL(proxy_entry_map_.get_refactored(proxy_id, mysql_proxy_entry))) {
    LOG_DEBUG("proxy_id not in map", K(proxy_id));
    return NULL;
  }
  mysql_proxy_entry->inc_ref();
  return mysql_proxy_entry;
}

int ObMysqlProxyCache::add_mysql_proxy_entry(ObMysqlProxyEntry * entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    LOG_WDIAG("entry should not null here");
    return OB_ERR_UNEXPECTED;
  }
  ObMysqlProxyEntry* tmp_entry = NULL;
  obsys::CWLockGuard guard(rwlock_);
  tmp_entry = proxy_entry_map_.remove(entry->proxy_id_);
  if (tmp_entry != NULL) {
    LOG_DEBUG("remove old proxy", K(entry->proxy_id_));
    tmp_entry->dec_ref();
  }
  if (OB_FAIL(proxy_entry_map_.unique_set(entry))) {
    LOG_WDIAG("fail to add to map", K(entry->proxy_id_));
    entry->dec_ref();
    ret = OB_ERR_UNEXPECTED;
  } else {
    LOG_DEBUG("succ add to map", K(entry->proxy_id_));
  }
  return ret;
}
int ObMysqlProxyCache::remove_mysql_proxy_entry(ObMysqlProxyEntry* entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    LOG_WDIAG("entry should not null here");
    return OB_ERR_UNEXPECTED;
  }
  ObMysqlProxyEntry* tmp_entry = NULL;
  obsys::CWLockGuard guard(rwlock_);
  tmp_entry = proxy_entry_map_.remove(entry->proxy_id_);
  if (!OB_ISNULL(tmp_entry)) {
    LOG_DEBUG("remove old proxy succ", K(entry->proxy_id_));
    tmp_entry->dec_ref();
  } else {
    LOG_WDIAG("entry not exist", K(entry->proxy_id_));
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

ObSequenceEntryCache& get_global_sequence_cache()
{
  static ObSequenceEntryCache sequence_cache;
  return sequence_cache;
}

ObMysqlProxyCache &get_global_mysql_proxy_cache()
{
  static ObMysqlProxyCache mysql_proxy_cache;
  return mysql_proxy_cache;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
