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

#define USING_LOG_PREFIX PROXY_EVENT

#include "iocore/eventsystem/ob_resource_tracker.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{
ObMemoryResourceTracker::ObResourceHashMap ObMemoryResourceTracker::g_resource_map;
ObMutex ObMemoryResourceTracker::g_resource_lock = PTHREAD_MUTEX_INITIALIZER;

void ObMemoryResource::increment(const int64_t size)
{
  (void)ATOMIC_FAA(&value_, size);
  if (size >= 0) {
    (void)ATOMIC_FAA(&increment_count_, 1);
  } else {
    (void)ATOMIC_FAA(&decrement_count_, 1);
  }
}

int ObMemoryResourceTracker::increment(const char *name, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObMemoryResource *resource = NULL;
  if (OB_FAIL(lookup(name, resource))) {
    LOG_WARN("fail to lookup ObMemoryResource", K(name),  K(ret));
  } else {
    resource->increment(size);
  }
  return ret;
}

int ObMemoryResourceTracker::lookup(const char *location, ObMemoryResource *&resource)
{
  int ret = OB_SUCCESS;
  resource = NULL;
  if (OB_FAIL(mutex_acquire(&g_resource_lock))) {
    LOG_ERROR("fail to acquire mutex", K(ret));
  } else {
    if (OB_FAIL(g_resource_map.get_refactored(ObString::make_string(location), resource))) {
      //if lookup fail, we need insert one
      if (OB_ISNULL(resource = new (std::nothrow) ObMemoryResource())) { // create a new entry
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new memory for Resource", K(location), K(ret));
      } else {
        resource->location_.assign_ptr(location, static_cast<int32_t>(strlen(location)));
        if (OB_FAIL(g_resource_map.unique_set(resource))) {
          LOG_WARN("fail to set Resource", K(location), K(ret));
        }
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mutex_release(&g_resource_lock)))) {
      LOG_ERROR("fail to release mutex", K(tmp_ret));
    }
  }
  return ret;
}

void ObMemoryResourceTracker::dump()
{
#ifdef OB_HAS_MEMORY_TRACKER
  int ret = OB_SUCCESS;
  int64_t total = 0;
  int64_t pos = 0;
  int64_t len = OB_MALLOC_NORMAL_BLOCK_SIZE;
  char *buf = reinterpret_cast<char *>(ob_malloc(len, ObModIds::OB_PROXY_PRINTF));
  if (OB_ISNULL(buf)) {
    LOG_ERROR("fail to malloc memory", K(len));
  } else {
    databuff_printf(buf, len, pos, "%100s | %20s\n", "Location", "Size In-use");
    databuff_printf(buf, len, pos, "---------------------------------------------------+------------------------\n");
    if (OB_FAIL(mutex_acquire(&g_resource_lock))) {
      LOG_ERROR("fail to acquire mutex", K(ret));
    } else {
      if (0 != g_resource_map.count()) {
        for (ObResourceHashMap::iterator it = g_resource_map.begin();
             it != g_resource_map.end() && pos < len;
             ++it) {
          databuff_printf(buf, len, pos, "%-100.*s | % '20ld\n", it->location_.length(), it->location_.ptr(), it->get_value());
          total += it->get_value();
        }
      }
      if (OB_FAIL(mutex_release(&g_resource_lock))) {
        LOG_ERROR("fail to release mutex", K(ret));
      }

      databuff_printf(buf, len, pos, "%100s | % '20ld\n", "TOTAL", total);
      _LOG_INFO("dump iobuffer memory statistic::\n%s", buf);
    }
  }

  if (OB_LIKELY(NULL != buf)) {
    ob_free(buf);
    buf = NULL;
  }
#endif
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase
