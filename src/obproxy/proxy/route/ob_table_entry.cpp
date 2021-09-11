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

#include "iocore/eventsystem/ob_buf_allocator.h"
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

int ObTableEntry::init(char *buf_start, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(buf_len <= 0) || OB_ISNULL(buf_start)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(buf_len), K(buf_start), K(ret));
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
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(!is_dummy_entry())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("non dummy_entry_ has no tenant_servers_", K(*this), K(ret));
  } else if (OB_ISNULL(new_ts) || OB_UNLIKELY(!new_ts->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", KPC(new_ts), K(ret));
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
    LOG_WARN("tenant servers is unavailable", K(ret));
  } else if (OB_FAIL(tenant_servers_->get_random_servers(location))) {
    LOG_WARN("fail to get random servers", KPC(tenant_servers_), K(ret));
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
    LOG_WARN("tenant servers is unavailable", K(ret));
  } else if (OB_FAIL(ObRandomNumUtils::get_random_num(0, tenant_servers_->replica_count_ - 1,
                                                      chosen_init_replica_idx))) {
    LOG_WARN("fail to get random num", KPC(tenant_servers_), K(ret));
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
    LOG_WARN("invalid input value", K(name), K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else if (OB_FAIL(name_.deep_copy(name, buf_start_, buf_len_))) {
    LOG_WARN("fail to deep copy table entry names", K(ret));
  } else {
    is_dummy_entry_ = name_.is_all_dummy_table();
  }
  return ret;
}

void ObTableEntry::free()
{
  LOG_DEBUG("ObTableEntry was freed", K(*this));
  name_.reset();
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
      LOG_ERROR("tenant_servers_ is not null here, we will have memory leak here", KPC(this));
    }
  }

  is_need_force_flush_ = false;

  int64_t total_len = sizeof(ObTableEntry) + buf_len_;
  buf_start_ = NULL;
  buf_len_ = 0;
  op_fixed_mem_free(this, total_len);
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
    LOG_WARN("invalid input value", K(name), K(cr_version), K(entry), K(ret));
  } else {
    int64_t name_size = name.get_total_str_len();
    int64_t obj_size = sizeof(ObTableEntry);
    int64_t alloc_size =  name_size + obj_size;
    char *buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", K(alloc_size), K(ret));
    } else {
      LOG_DEBUG("alloc entry succ", K(alloc_size), K(obj_size), K(name_size), K(name));
      entry = new (buf) ObTableEntry();
      if (OB_FAIL(entry->init(buf + obj_size, name_size))) {
        LOG_WARN("fail to init entry", K(alloc_size), K(ret));
      } else if (OB_FAIL(entry->set_names(name))) {
        LOG_WARN("fail to set name", K(name), K(ret));
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
    LOG_WARN("part info can not alloc twice", K(ret));
  } else if (OB_FAIL(ObProxyPartInfo::alloc(part_info_))) {
    LOG_WARN("fail to alloc part info", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObTableEntry::is_contain_all_dummy_entry(const ObTableEntry &new_entry, bool &is_contain_all) const
{
  int ret = OB_SUCCESS;
  is_contain_all = false;
  if (OB_UNLIKELY(!is_valid()) || OB_UNLIKELY(!new_entry.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in valid", KPC(this), K(new_entry), K(ret));
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

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
