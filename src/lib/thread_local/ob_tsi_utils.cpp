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

#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/utility/utility.h"
namespace oceanbase
{
namespace common
{
int64_t ExpStat::to_string(char *buf, const int64_t len) const
{
  int tmp_ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t total_count = 0;
  int64_t total_value = 0;
  int64_t count[TSI_HP_COUNTER_ELEMENTS_SIZE];
  int64_t value[TSI_HP_COUNTER_ELEMENTS_SIZE];
  if (OB_UNLIKELY(NULL == buf) || OB_UNLIKELY(len <= 0)) {
    tmp_ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid argument", K(tmp_ret), KP(buf), K(len));
  } else {
    ExpStatItem item;
    for (int64_t i = 0; i < TSI_HP_COUNTER_ELEMENTS_SIZE; i++) {
      stat_.get_value(item, i);
      count[i] = item.count_;
      value[i] = item.value_;
      total_count += count[i];
      total_value += value[i];
    }
    databuff_printf(buf, len, pos,
                    "total_count=%'ld, total_%s=%'ldus, avg=%'ld\n",
                    total_count, metric_name_, total_value,
                    total_count == 0 ? 0 : total_value / total_count);
    for (int64_t i = 0; i < 64; i++) {
      if (0 != count[i]) {
        databuff_printf(buf, len, pos,
                        "stat[2**%2ld<=x<2**%2ld]: %5.2lf%%, count=%'12ld, avg=%'12ld, %s=%'ld\n",
                        i - 1, i,
                        total_count == 0 ? 0 : 100.0 * (double)count[i] / (double)total_count,
                        count[i], count[i] == 0 ? 0 : value[i] / count[i], metric_name_, value[i]);
      }
    }
  }
  return pos;
}
} // end namespace common
} // end namespace oceanbase
