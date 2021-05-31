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

using namespace oceanbase::lib;
using namespace oceanbase::common;

void ObTenantAllocator::print_usage() const
{
  int ret = OB_SUCCESS;
  static const int64_t BUFLEN = 1 << 16;
  char buf[BUFLEN] = {};
  int64_t pos = 0;
  ObModItem sum_item;

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

  _OBPROXY_XFLUSH_LOG(INFO, "[MEMORY] tenant: %lu, limit: %lu, hold: %lu\n%s",
                      tenant_id_, limit_, hold_bytes_, buf);

  if (OB_SUCC(ret) && sum_item.count_ > 0) {
    const ObModItem &item = sum_item;
    _OBPROXY_XFLUSH_LOG(INFO, "[MEMORY] hold=% 15ld used=% 15ld count=% 8ld avg_used=% 15ld mod=%s\n",
                        item.hold_, item.used_, item.count_, (0 == item.count_) ? 0 : item.used_ / item.count_,
                        "SUMMARY");
  }
}
