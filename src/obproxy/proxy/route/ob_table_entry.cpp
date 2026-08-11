/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "iocore/eventsystem/ob_buf_allocator.h"
#include "iocore/eventsystem/ob_lock.h"
#include "proxy/route/ob_table_entry.h"
#include "utils/ob_proxy_utils.h"
#include "proxy/route/obproxy_part_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define MAX_BATCH_PARTION_IDS_NUM 50

int ObTableEntryBatchFetchInfo::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(batch_mutex_ = event::new_proxy_mutex())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to alloc batch fetch mutex", K(ret));
  } else if (OB_FAIL(batch_fetch_tablet_id_set_.create(MAX_BATCH_PARTION_IDS_NUM))) {
    LOG_WDIAG("fail to create batch fetch tablet id set", K(ret));
  } else if (OB_FAIL(remote_fetching_tablet_id_set_.create(MAX_BATCH_PARTION_IDS_NUM))) {
    LOG_WDIAG("fail to create remote fetching tablet id set", K(ret));
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void ObTableEntryBatchFetchInfo::destroy()
{
  if (batch_fetch_tablet_id_set_.created()) {
    batch_fetch_tablet_id_set_.destroy();
  }
  if (remote_fetching_tablet_id_set_.created()) {
    remote_fetching_tablet_id_set_.destroy();
  }
  if (NULL != batch_mutex_) {
    batch_mutex_.release();
  }
  batch_fetch_cont_ = NULL;
}

int ObTableEntry::init(char *buf_start, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(buf_len <= 0) || OB_ISNULL(buf_start)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buf_len), K(buf_start), K(ret));
  } else {
    create_time_us_ = ObTimeUtility::current_time();
    buf_len_ = buf_len;
    buf_start_ = buf_start;
    is_inited_ = true;
  }

  return ret;
}

int ObTableEntry::set_tenant_servers(ObTenantServer *new_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(!is_dummy_entry())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("non dummy_entry_ has no tenant_servers_", K(*this), K(ret));
  } else if (OB_ISNULL(new_ts) || OB_UNLIKELY(!new_ts->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KPC(new_ts), K(ret));
  } else {
    LOG_DEBUG("will set tenant servers", KPC(tenant_servers_), KPC(new_ts));
    if (NULL != tenant_servers_) {
      //free old pl
      op_free(tenant_servers_);
      //set new pl
      tenant_servers_ = new_ts;
    } else {
      tenant_servers_ = new_ts;
    }
  }
  return ret;
}

int ObTableEntry::get_random_servers(ObProxyPartitionLocation &location)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_tenant_servers_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("tenant servers is unavailable", K(ret));
  } else if (OB_FAIL(tenant_servers_->get_random_servers(location))) {
    LOG_WDIAG("fail to get random servers", KPC(tenant_servers_), K(ret));
  } else {
    LOG_DEBUG("succ to get random servers", K(location));
  }
  return ret;
}

int ObTableEntry::get_random_servers_idx(
    int64_t &chosen_partition_idx,
    int64_t &chosen_init_replica_idx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_tenant_servers_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("tenant servers is unavailable", K(ret));
  } else if (OB_FAIL(ObRandomNumUtils::get_random_num(0, tenant_servers_->replica_count_ - 1,
                                                      chosen_init_replica_idx))) {
    LOG_WDIAG("fail to get random num", KPC(tenant_servers_), K(ret));
  } else {
    chosen_partition_idx = tenant_servers_->get_next_partition_index();
    LOG_DEBUG("succ to get random servers idx", KPC(tenant_servers_),
              K(chosen_partition_idx), K(chosen_init_replica_idx));
  }
  return ret;
}

int ObTableEntry::set_names(const ObTableEntryName &name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!name.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(name), K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_FAIL(name_.deep_copy(name, buf_start_, buf_len_))) {
    LOG_WDIAG("fail to deep copy table entry names", K(ret));
  } else {
    is_dummy_entry_ = name_.is_all_dummy_table();
    is_binlog_entry_ = name_.is_binlog_table();
  }
  return ret;
}

void ObTableEntry::free()
{
  LOG_DEBUG("ObTableEntry was freed", K(*this));
  name_.reset();
  level1_decoded_db_name_.reset();
  if (is_dummy_entry()) {
    if (NULL != tenant_servers_) {
      op_free(tenant_servers_);
      tenant_servers_ = NULL;
    }
  } else if (is_location_entry()) {
    if (NULL != first_pl_) {
      op_free(first_pl_);
      first_pl_ = NULL;
    }
  } else if (is_part_info_entry()) {
    // partition table, free part info
    if (NULL != part_info_) {
      part_info_->free();
      part_info_ = NULL;
    }
  } else {
    if (OB_UNLIKELY(NULL != tenant_servers_)) {
      LOG_EDIAG("tenant_servers_ is not null here, we will have memory leak here", KPC(this));
    }
  }

  if (NULL != batch_fetch_info_) {
    op_free(batch_fetch_info_);
    batch_fetch_info_ = NULL;
  }

  is_need_force_flush_ = false;

  int64_t total_len = sizeof(ObTableEntry) + buf_len_;
  buf_start_ = NULL;
  buf_len_ = 0;
  op_fixed_mem_free(this, total_len);
}

void ObTableEntry::reuse()
{
  LOG_DEBUG("ObTableEntry was reused", K(*this));
  if (is_dummy_entry()) {
    if (NULL != tenant_servers_) {
      op_free(tenant_servers_);
      tenant_servers_ = NULL;
    }
  } else if (is_location_entry()) {
    if (NULL != first_pl_) {
      op_free(first_pl_);
      first_pl_ = NULL;
    }
  } else if (is_part_info_entry()) {
    // partition table, free part info
    if (NULL != part_info_) {
      part_info_->free();
      part_info_ = NULL;
    }
  } else {
    if (OB_UNLIKELY(NULL != tenant_servers_|| NULL != part_info_)) {
      LOG_EDIAG("tenant_servers_/part_info_ is not null here, we will have memory leak here", KPC(this));
    }
  }
  // ObRouteEntry
  // cr_version, cr_id
  schema_version_ = 0;
  last_update_time_us_ = 0;
  tenant_version_ = 0;
  time_for_expired_ = 0;
  current_expire_time_config_ = 0;
  create_time_us_ = ObTimeUtility::current_time();
  renew_last_access_time();
  renew_last_valid_time();
  set_avail_state();

  // ObTableEntry
  // is_dummy_entry_, is_binlog_entry_, name_, batch_mutex_
  is_inited_ = true;
  is_entry_from_rslist_ = false;
  is_empty_entry_allowed_ = false;
  is_need_force_flush_ = false;
  has_dup_replica_ = false;
  tenant_id_ = common::OB_INVALID_ID;
  table_id_ = common::OB_INVALID_ID;
  table_type_ = share::schema::MAX_TABLE_TYPE;
  part_num_ = 0;
  replica_num_ = 0;
  // name_.reset();
  // not init batch_fetch_info_ by default when entry is reused
  if (NULL != batch_fetch_info_) {
    op_free(batch_fetch_info_);
    batch_fetch_info_ = NULL;
  }
}

int ObTableEntry::alloc_and_init_table_entry(
    const ObTableEntryName &name,
    const int64_t cr_version,
    const int64_t cr_id,
    ObTableEntry *&entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!name.is_valid() || cr_version < 0 || NULL != entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(name), K(cr_version), K(entry), K(ret));
  } else {
    int64_t name_size = name.get_total_str_len();
    int64_t obj_size = sizeof(ObTableEntry);
    int64_t alloc_size =  name_size + obj_size;
    char *buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem", K(alloc_size), K(ret));
    } else {
      entry = new (buf) ObTableEntry();
      LOG_DEBUG("alloc entry succ", K(alloc_size), K(obj_size), K(name_size), K(name), K(entry));
      if (OB_FAIL(entry->init(buf + obj_size, name_size))) {
        LOG_WDIAG("fail to init entry", K(alloc_size), K(ret));
      } else if (OB_FAIL(entry->set_names(name))) {
        LOG_WDIAG("fail to set name", K(name), K(ret));
      } else {
        entry->inc_ref();
        entry->renew_last_access_time();
        entry->renew_last_valid_time();
        entry->set_avail_state();
        entry->set_cr_version(cr_version);
        entry->set_cr_id(cr_id);
      }
    }
    if ((OB_FAIL(ret)) && (NULL != buf)) {
      op_fixed_mem_free(buf, alloc_size);
      entry = NULL;
      alloc_size = 0;
    }
  }
  return ret;
}

int ObTableEntry::alloc_part_info()
{
  int ret = OB_SUCCESS;
  if (NULL != part_info_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("part info can not alloc twice", K(ret));
  } else if (OB_FAIL(ObProxyPartInfo::alloc(part_info_))) {
    LOG_WDIAG("fail to alloc part info", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

void ObTableEntry::free_part_info()
{
  if (NULL != part_info_) {
    part_info_->free();
    part_info_ = NULL;
  }
}

int ObTableEntry::is_contain_all_dummy_entry(const ObTableEntry &new_entry, bool &is_contain_all) const
{
  int ret = OB_SUCCESS;
  is_contain_all = false;
  if (OB_UNLIKELY(!is_valid()) || OB_UNLIKELY(!new_entry.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("in valid", KPC(this), K(new_entry), K(ret));
  } else if (is_dummy_entry() == new_entry.is_dummy_entry()) {
    bool is_found_one = true;
    const ObTenantServer *new_ts = new_entry.get_tenant_servers();
    for (int64_t i = 0; is_found_one && i < new_ts->server_count_; ++i) {
      is_found_one = false;
      for (int64_t j = 0; !is_found_one && j < tenant_servers_->server_count_; ++j) {
        if (tenant_servers_->server_array_[j] == new_ts->server_array_[i]) {
          is_found_one = true;
        }
      }
    }
    is_contain_all = is_found_one;
  }
  return ret;
}

int64_t ObTableEntry::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  pos += ObRouteEntry::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(KP(this),
       K_(is_inited),
       K_(is_dummy_entry),
       K_(is_entry_from_rslist),
       K_(is_empty_entry_allowed),
       K_(is_need_force_flush),
       K_(has_dup_replica),
       K_(is_single_partition_table),
       K_(cr_id),
       K_(name),
       K_(table_id),
       "table_type", share::schema::ob_table_type_str(table_type_),
       K_(part_num),
       K_(replica_num),
       K_(buf_len),
       KP_(buf_start));
  J_COMMA();
  if (is_dummy_entry()) {
    J_KV(KPC_(tenant_servers));
  } else if (is_location_entry()) {
    J_KV(KPC_(first_pl));
  } else if (is_part_info_entry()) {
    J_KV(KPC_(part_info));
  } else {
    // do nothing
  }
  J_OBJ_END();
  return pos;
}

bool ObTableEntry::is_the_same_entry(const ObTableEntry &entry) const
{
  bool bret = false;
  if (is_valid()
      && entry.is_valid()
      && is_non_partition_table()
      && entry.is_non_partition_table()) { // only handle non_partition table
    // cr version
    if (get_cr_version() != entry.get_cr_version()) {
    // server count
    } else if (get_cr_id() != entry.get_cr_id()) {
    // cr id
    } else if (get_server_count() != entry.get_server_count()) {
    // leader
    } else if (!is_leader_server_equal(entry)) {
    // names
    } else if (get_names() != entry.get_names()) {
    // all server
    } else if (get_all_server_hash() != entry.get_all_server_hash()) {
    // replica num
    } else if (get_replica_num() != entry.get_replica_num()) {
    // equal
    } else {
      bret = true;
    }
  }
  return bret;
}

int ObTableEntryBatchFetchInfo::put_batch_fetch_tablet_id(uint64_t tablet_id)
{
  int ret = OB_SUCCESS; //always 0 
  event::MUTEX_TRY_LOCK(lock, batch_mutex_, event::this_ethread());
  if (lock.is_locked()) { 
    ret = batch_fetch_tablet_id_set_.set_refactored(tablet_id);
    LOG_DEBUG("batch fetch put tablet_id to bucket", K(ret), K(tablet_id), "locked", lock.is_locked(), 
              "size", batch_fetch_tablet_id_set_.size());
  }
  if (ret == OB_HASH_EXIST) {
    ret = OB_SUCCESS;
  }

  return ret;
}

//TODO will be used in ObBatchPartitionEntryCont
int ObTableEntryBatchFetchInfo::put_batch_fetch_tablet_id(uint64_t tablet_id, int &count)
{
  int ret = OB_SUCCESS; //always 0 
  count = 0; //0 means not put to batch set, > 0 batch size in set
  event::MUTEX_TRY_LOCK(lock, batch_mutex_, event::this_ethread());
  if (lock.is_locked()) { 
    if (batch_fetch_tablet_id_set_.size() < obutils::get_global_proxy_config().rpc_async_pull_batch_max_size) {
      batch_fetch_tablet_id_set_.set_refactored(tablet_id);
      count = batch_fetch_tablet_id_set_.size();
      LOG_DEBUG("batch fetch put tablet_id to bucket", K(ret), K(tablet_id), "locked", lock.is_locked(), K(count),
                "size", batch_fetch_tablet_id_set_.size(), "max_size", obutils::get_global_proxy_config().rpc_async_pull_batch_max_size);
    } else if (OB_HASH_EXIST == batch_fetch_tablet_id_set_.exist_refactored(tablet_id)) {
      count = batch_fetch_tablet_id_set_.size(); //has exist
    }
  }

  return ret;
}

//TODO will be used in ObBatchPartitionEntryCont
int ObTableEntryBatchFetchInfo::get_batch_fetch_tablet_ids(ObIArray<uint64_t> &batch_ids)
{
  int ret = OB_SUCCESS;
  event::MUTEX_TRY_LOCK(lock, batch_mutex_, event::this_ethread());
  batch_ids.reset();
  if (lock.is_locked() && batch_fetch_tablet_id_set_.size() > 0) {
    common::hash::ObHashSet<uint64_t>::iterator it = batch_fetch_tablet_id_set_.begin();;
    common::hash::ObHashSet<uint64_t>::iterator end = batch_fetch_tablet_id_set_.end();;
    for (; it != end; it++) {
      batch_ids.push_back(it->first);
    }
    LOG_DEBUG("batch fetch put tablet_id to bucket to get and reset", K(ret), "locked", lock.is_locked(), "count", batch_ids.count(),
            "size", batch_fetch_tablet_id_set_.size(), "max_size", obutils::get_global_proxy_config().rpc_async_pull_batch_max_size);
    batch_fetch_tablet_id_set_.reuse(); //clear all ids
  }

  return ret;
}

int ObTableEntryBatchFetchInfo::get_batch_fetch_tablet_ids(ObIArray<uint64_t> &batch_ids, int max_count, uint64_t tablet_id)
{
  int ret = OB_SUCCESS;
  event::MUTEX_TRY_LOCK(lock, batch_mutex_, event::this_ethread());
  batch_ids.reset();
  if (lock.is_locked() && batch_fetch_tablet_id_set_.size() > 0 && max_count > 0) {
    common::hash::ObHashSet<uint64_t>::iterator it = batch_fetch_tablet_id_set_.begin();;
    common::hash::ObHashSet<uint64_t>::iterator end = batch_fetch_tablet_id_set_.end();;
    if (tablet_id != OB_INVALID_INDEX) {
      ret = batch_ids.push_back(tablet_id); //put it first
      max_count--;
    }
    for (; OB_SUCC(ret) && it != end && max_count > 0; it++) {
      if (OB_LIKELY(tablet_id != it->first)) {
        ret = batch_ids.push_back(it->first);
        max_count--;
      }
    }
    int count = batch_ids.count();
    for (int i = 0; OB_SUCC(ret) && i < count; i++) {
      ret = batch_fetch_tablet_id_set_.erase_refactored(batch_ids.at(i));
      if (ret == OB_HASH_NOT_EXIST || ret == OB_HASH_EXIST) {
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        ret = remote_fetching_tablet_id_set_.set_refactored(batch_ids.at(i));
      }
      if (ret == OB_HASH_EXIST) {
        ret = OB_SUCCESS;
      }
    }
    LOG_DEBUG("batch fetch put tablet_id to bucket to get and reset", K(ret), "locked", lock.is_locked(), "count", batch_ids.count(),
              "size", batch_fetch_tablet_id_set_.size(), "remote_fetching", remote_fetching_tablet_id_set_.size(), K(tablet_id));
  } else {
    ret = OB_NEED_RETRY;
    LOG_DEBUG("batch fetch put tablet_id to bucket not catch the lock", K(ret));
  }
  return ret;
}

int ObTableEntryBatchFetchInfo::remove_pending_batch_fetch_tablet_ids(ObIArray<uint64_t>  &batch_ids, bool force)
{
  int ret = OB_SUCCESS;
  if (batch_ids.count() > 0) {
    if (force) {
      MUTEX_LOCK(lock, batch_mutex_, event::this_ethread());
      int i = 0;
      for (i = 0; i < batch_ids.count(); i++) {
        remote_fetching_tablet_id_set_.erase_refactored(batch_ids.at(i)); // not to care abort return value
      }
      batch_ids.reset();
      //wiil ret OB_SUCCESS only
    } else {
      event::MUTEX_TRY_LOCK(lock, batch_mutex_, event::this_ethread());
      if (lock.is_locked()) {
        int i = 0;
        for (i = 0; i < batch_ids.count(); i++) {
          remote_fetching_tablet_id_set_.erase_refactored(batch_ids.at(i)); // not to care abort return value
        }
        batch_ids.reset();
      } else {
       ret = OB_NEED_RETRY;
      }
    }
  }
  return ret;
}

int ObTableEntryBatchFetchInfo::get_batch_fetch_size()
{
  int ret = 0; //0 means not put to batch set, > 0 batch size in set
  event::MUTEX_TRY_LOCK(lock, batch_mutex_, event::this_ethread());
  if (lock.is_locked()) { 
    ret = batch_fetch_tablet_id_set_.size();
  }

  return ret;
} 

void ObTableEntryBatchFetchInfo::reset_batch_tablet_ids()
{
  MUTEX_LOCK(lock, batch_mutex_, event::this_ethread());
  batch_fetch_tablet_id_set_.reuse();
}

int ObTableEntry::get_or_create_batch_fetch_info(ObTableEntryBatchFetchInfo *&batch_fetch_info)
{
  int ret = OB_SUCCESS;
  batch_fetch_info = batch_fetch_info_;
  if (NULL == batch_fetch_info) {
    ObTableEntryBatchFetchInfo *new_info = NULL;
    if (OB_ISNULL(new_info = op_alloc(ObTableEntryBatchFetchInfo))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc table entry batch fetch info", K(ret), KPC(this));
    } else if (OB_FAIL(new_info->init())) {
      LOG_WDIAG("fail to init table entry batch fetch info", K(ret), KPC(this));
      op_free(new_info);
      new_info = NULL;
    } else if (ATOMIC_BCAS(&batch_fetch_info_, NULL, new_info)) {
      batch_fetch_info = new_info;
    } else {
      op_free(new_info);
      new_info = NULL;
      batch_fetch_info = batch_fetch_info_;
    }
  }
  return ret;
}

int ObTableEntry::put_batch_fetch_tablet_id(uint64_t tablet_id)
{
  int ret = OB_SUCCESS;
  ObTableEntryBatchFetchInfo *batch_fetch_info = NULL;
  if (OB_FAIL(get_or_create_batch_fetch_info(batch_fetch_info))) {
    LOG_WDIAG("fail to get or create batch fetch info", K(ret), K(tablet_id), KPC(this));
  } else if (OB_FAIL(batch_fetch_info->put_batch_fetch_tablet_id(tablet_id))) {
    LOG_WDIAG("fail to put batch fetch tablet id", K(ret), K(tablet_id), KPC(this));
  }
  return ret;
}

int ObTableEntry::put_batch_fetch_tablet_id(uint64_t tablet_id, int &count)
{
  int ret = OB_SUCCESS;
  ObTableEntryBatchFetchInfo *batch_fetch_info = NULL;
  if (OB_FAIL(get_or_create_batch_fetch_info(batch_fetch_info))) {
    count = 0;
    LOG_WDIAG("fail to get or create batch fetch info", K(ret), K(tablet_id), KPC(this));
  } else if (OB_FAIL(batch_fetch_info->put_batch_fetch_tablet_id(tablet_id, count))) {
    LOG_WDIAG("fail to put batch fetch tablet id", K(ret), K(tablet_id), K(count), KPC(this));
  }
  return ret;
}

int ObTableEntry::get_batch_fetch_tablet_ids(ObIArray<uint64_t> &batch_ids)
{
  int ret = OB_SUCCESS;
  batch_ids.reset();
  if (NULL != batch_fetch_info_) {
    if (OB_FAIL(batch_fetch_info_->get_batch_fetch_tablet_ids(batch_ids))) {
      LOG_WDIAG("fail to get batch fetch tablet ids", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObTableEntry::get_batch_fetch_tablet_ids(ObIArray<uint64_t> &batch_ids, int max_count, uint64_t tablet_id)
{
  int ret = OB_NEED_RETRY;
  batch_ids.reset();
  if (NULL != batch_fetch_info_) {
    if (OB_FAIL(batch_fetch_info_->get_batch_fetch_tablet_ids(batch_ids, max_count, tablet_id))) {
      LOG_WDIAG("fail to get batch fetch tablet ids", K(ret), K(max_count), K(tablet_id), KPC(this));
    }
  }
  return ret;
}

int ObTableEntry::remove_pending_batch_fetch_tablet_ids(ObIArray<uint64_t>  &batch_ids, bool force)
{
  int ret = OB_SUCCESS;
  if (NULL != batch_fetch_info_) {
    if (OB_FAIL(batch_fetch_info_->remove_pending_batch_fetch_tablet_ids(batch_ids, force))) {
      LOG_WDIAG("fail to remove pending batch fetch tablet ids", K(ret), K(force), KPC(this));
    }
  } else {
    batch_ids.reset();
  }
  return ret;
}

int ObTableEntry::get_batch_fetch_size()
{
  return NULL == batch_fetch_info_ ? 0 : batch_fetch_info_->get_batch_fetch_size();
}

void ObTableEntry::set_batch_fetch_cont(void *cont)
{
  int ret = OB_SUCCESS;
  ObTableEntryBatchFetchInfo *batch_fetch_info = NULL;
  if (OB_FAIL(get_or_create_batch_fetch_info(batch_fetch_info))) {
    LOG_WDIAG("fail to get or create batch fetch info", K(ret), K(cont));
  } else {
    batch_fetch_info->set_batch_fetch_cont(cont);
  }
}

void ObTableEntry::reset_batch_tablet_ids()
{
  if (NULL != batch_fetch_info_) {
    batch_fetch_info_->reset_batch_tablet_ids();
  }
}

event::ObProxyMutex *ObTableEntry::get_batch_fetch_mutex()
{
  event::ObProxyMutex *mutex = NULL;
  ObTableEntryBatchFetchInfo *batch_fetch_info = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_or_create_batch_fetch_info(batch_fetch_info))) {
    LOG_WDIAG("fail to get or create batch fetch info", K(ret));
  } else {
    mutex = batch_fetch_info->get_batch_fetch_mutex();
  }
  return mutex;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
