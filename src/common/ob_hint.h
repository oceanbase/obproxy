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

#ifndef OCEANBASE_COMMON_HINT_
#define OCEANBASE_COMMON_HINT_

#include "lib/utility/utility.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace common
{
enum ObConsistencyLevel
{
  INVALID_CONSISTENCY = -1,
  FROZEN = 1,
  WEAK,
  STRONG,
};
const char *get_consistency_level_str(ObConsistencyLevel level);
/// @retval -1 on error
ObConsistencyLevel get_consistency_level_by_str(const ObString &level);
struct ObTableScanHint
{
public:
  int64_t max_parallel_count_;
  bool enable_parallel_;
  int64_t timeout_us_;
  ObVersion frozen_version_;
  ObConsistencyLevel read_consistency_;

public:
  ObTableScanHint() :
    max_parallel_count_(OB_DEFAULT_MAX_PARALLEL_COUNT),
    enable_parallel_(true),
    timeout_us_(OB_DEFAULT_STMT_TIMEOUT),
    frozen_version_(-1),
    read_consistency_(INVALID_CONSISTENCY)
  {
  }

  void reset();
  /// @brief this function only print hints that not using default value
  int64_t hint_to_string(char *buf, const int64_t buf_len, int64_t &pos) const;

  DECLARE_TO_STRING;
  OB_UNIS_VERSION(1);
};

}
}
#endif // end of header
