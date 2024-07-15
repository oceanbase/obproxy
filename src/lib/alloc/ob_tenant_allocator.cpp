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

#define USING_LOG_PREFIX LIB

#include "ob_tenant_allocator.h"

#include "lib/allocator/ob_tc_malloc.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "utils/ob_proxy_hot_upgrader.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::obproxy;

void *ObTenantAllocator::alloc(const int64_t size, const ObMemAttr &attr)
{
  abort_unless(attr.tenant_id_ == tenant_id_);
  void *ptr = NULL;
  if (attr.prio_ == OB_HIGH_ALLOC
      || CHUNK_MGR.get_hold() <= CHUNK_MGR.get_limit()) {
    AObject *obj = obj_mgr_.alloc_object(size, attr);
    if (NULL != obj) {
      ptr = obj->data_;
      common::get_global_mem_leak_checker().on_alloc(attr.mod_id_, get_global_mod_set().get_mod_name(attr.mod_id_), ptr, size);
    }
  }

  return ptr;
}

void* ObTenantAllocator::realloc(const void *ptr, const int64_t size, const ObMemAttr &attr)
{
  void *nptr = NULL;
  AObject *obj = NULL;
  if (NULL != ptr) {
    obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
    abort_unless(obj->is_valid());
    abort_unless(obj->in_use_);
    abort_unless(obj->block()->is_valid());
    abort_unless(obj->block()->in_use_);
    common::get_global_mem_leak_checker().on_free(obj->mod_id_, get_global_mod_set().get_mod_name(obj->mod_id_), ptr);
  }
  obj = obj_mgr_.realloc_object(obj, size, attr);
  if (obj != NULL) {
    nptr = obj->data_;
    common::get_global_mem_leak_checker().on_alloc(attr.mod_id_, get_global_mod_set().get_mod_name(attr.mod_id_), nptr, size);
  }
  return nptr;
}

void ObTenantAllocator::free(void *ptr)
{
  if (NULL != ptr) {
    AObject *obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
    abort_unless(NULL != obj);
    abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE
                  || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
    abort_unless(obj->in_use_);

    ABlock *block = obj->block();
    abort_unless(block->check_magic_code());
    abort_unless(block->in_use_);
    abort_unless(block->obj_set_ != NULL);

    common::get_global_mem_leak_checker().on_free(obj->mod_id_, get_global_mod_set().get_mod_name(obj->mod_id_), ptr);
    ObjectSet *set = block->obj_set_;
    set->lock();
    set->free_object(obj);
    set->unlock();
  }
}

int ObTenantAllocator::update_hold(int64_t bytes, const ObMemAttr &attr)
{
  int ret = common::OB_SUCCESS;
  UNUSED(attr);

  if (bytes <= 0) {
    IGNORE_RETURN ATOMIC_AAF(&hold_bytes_, bytes);
  } else if (hold_bytes_ + bytes <= limit_) {
    const int64_t nvalue = ATOMIC_AAF(&hold_bytes_, bytes);
    if (nvalue > limit_) {
      IGNORE_RETURN ATOMIC_AAF(&hold_bytes_, -bytes);
      ret = common::OB_EXCEED_MEM_LIMIT;
    }
  } else {
    ret = common::OB_EXCEED_MEM_LIMIT;
  }
  return ret;
}

void ObTenantAllocator::print_usage() const
{
  int ret = OB_SUCCESS;
  static const int64_t BUFLEN = 1 << 16;
  char buf[BUFLEN] = {};
  int64_t pos = 0;
  ObModItem sum_item;

  buf[pos++] = '\n'; //first mod item print in a new line too.
  for (int32_t idx = 0; OB_SUCC(ret) && idx < ObModSet::MOD_COUNT_LIMIT; idx++) {
    if (!is_allocator_mod(idx)) {
      ObModItem item = obj_mgr_.get_mod_usage(idx);;
      sum_item += item;
      if (item.count_ > 0) {
        ret = databuff_printf(
            buf, BUFLEN, pos,
            "[MEMORY] hold=% 15ld used=% 15ld count=% 8ld avg_used=% 15ld mod=%s\n",
            item.hold_, item.used_, item.count_, (0 == item.count_) ? 0 : item.used_ / item.count_,
            get_global_mod_set().get_mod_name(idx));
      }
    }
  }

  _OBPROXY_XFLUSH_LOG(INFO, "[MEMORY] tenant: %lu, limit: %lu, hold: %lu %s",
                      tenant_id_, limit_, hold_bytes_, buf);

  if (OB_SUCC(ret) && sum_item.count_ > 0) {
    const ObModItem &item = sum_item;
    _OBPROXY_XFLUSH_LOG(INFO, "[MEMORY] hold=% 15ld used=% 15ld count=% 8ld avg_used=% 15ld mod=%s\n",
                        item.hold_, item.used_, item.count_, (0 == item.count_) ? 0 : item.used_ / item.count_,
                        "SUMMARY");
  }

  // TODO: 增加 limit 设置合理的值，目前为 INT64_MAX 没有意义先去掉
  OBPROXY_DIAGNOSIS_LOG(INFO, "[MEMORY]", "hold", hold_bytes_, "buf", buf);
  if (OB_SUCC(ret) && sum_item.count_ > 0) {
    const ObModItem &item = sum_item;
    OBPROXY_DIAGNOSIS_LOG(WDIAG,
                        "[MEMORY]", "hold", item.hold_, "used", item.used_, "count", item.count_,
                        "avg_used", (0 == item.count_) ? 0 : item.used_ / item.count_, "mod", "SUMMARY");
  }
}

