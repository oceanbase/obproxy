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
#include "obutils/ob_safe_snapshot_manager.h"
#include "obutils/ob_state_info.h"
#include "utils/ob_proxy_utils.h"
#include "iocore/eventsystem/ob_buf_allocator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
ObSafeSnapshotManager::ObSafeSnapshotManager() : buf_()
                                               , entry_map_(buf_, HASH_BUF_SIZE)
                                               , next_priority_(0)
{
}

int ObSafeSnapshotManager::add(const ObAddr &addr)
{
  int ret = OB_SUCCESS;
  int64_t random_priority = -1;
  // 1. random pick a priority in [0, next_priority_]
  if (OB_FAIL(ObRandomNumUtils::get_random_num(0, next_priority_, random_priority))) {
    LOG_WARN("fail to get random number", K(ret));
  } else {
    void *buf = op_fixed_mem_alloc(sizeof(ObSafeSnapshotEntry));
    if (OB_ISNULL(buf)) {
      LOG_WARN("fail to alloc buf", K(ret));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      // 2. alloc and init new entry, assign it priority to the randome value
      ObSafeSnapshotEntry *new_entry = new (buf) ObSafeSnapshotEntry();
      new_entry->set_addr(addr);
      new_entry->set_priority(random_priority);

      // 3. we will find the entry which priority is equal to random_priority
      //    and set it to next_priority_
      ObSafeSnapshotEntry *iter_entry = NULL;
      while(NULL != (iter_entry = static_cast<ObSafeSnapshotEntry *>(entry_map_.next(iter_entry)))) {
        if (random_priority == iter_entry->get_priority()) {
          iter_entry->set_priority(next_priority_);
        }
      }
      next_priority_++;

      // 4. put the new entry into entry_map map
      ret = err_code_map(entry_map_.insert(addr, new_entry));
      if (OB_UNLIKELY(OB_ENTRY_EXIST == ret)) {
        LOG_ERROR("new entry already exist, it should not happened", K(*new_entry), KPC(this));
        op_fixed_mem_free(new_entry, sizeof(ObSafeSnapshotEntry));
        buf = NULL;
        new_entry = NULL;
      } else {
        LOG_INFO("add new entry", K(*new_entry), KPC(this));
      }
    }
  }
  return ret;
}

int64_t ObSafeSnapshotManager::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();

  ObSafeSnapshotEntry *entry = NULL;
  while(NULL != (entry = static_cast<ObSafeSnapshotEntry *>(entry_map_.next(entry)))) {
    J_KV("entry", *entry);
    J_COMMA();
  }

  J_OBJ_END();
  return pos;
}
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

