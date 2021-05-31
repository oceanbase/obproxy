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

#include "stat/ob_api_stat.h"
#include "proxy/api/ob_api.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObApiStat::ObApiStat() : stat_id_(-1)
{
  // obproxy Guarantees that stat ids will always be > 0.
  // So we can use stat_id_ > 0 to verify that this stat
  // has been properly initialized.
}

ObApiStat::~ObApiStat()
{
  // we really don't have any cleanup since obproxy doesn't
  // expose a method to destroy stats
}

int ObApiStat::init(const char *name, const ObRawStatSyncType type)
{
  int ret = OB_SUCCESS;
  stat_id_ = ObApiStat::get_next_stat_id();
  if (stat_id_ >= 0) {
    if (OB_FAIL(g_stat_processor.register_raw_stat(api_rsb, RECT_PLUGIN, name, RECD_INT, stat_id_, type, RECP_NULL))) {
      WARN_API("fail to register api stat, ret=%d", ret);
    }
  } else {
    ret = OB_ERROR;
  }

  return ret;
}

void ObApiStat::set(const int64_t value)
{
  int ret = OB_SUCCESS;
  if (stat_id_ >= 0) {
    if (OB_FAIL(ObStatProcessor::set_global_raw_stat_sum(api_rsb, stat_id_, value))) {
      WARN_API("fail to set api stat sum to value %ld, ret=%d", value, ret);
    }
  }
}

int64_t ObApiStat::get() const
{
  int64_t ret = 0;

  if (stat_id_ >= 0) {
    ret = ObStatProcessor::get_global_raw_stat_sum(api_rsb, stat_id_);
  }

  return ret;
}

void ObApiStat::increment(const int64_t amount)
{
  int ret = OB_SUCCESS;
  if (stat_id_ >= 0) {
    if (OB_FAIL(ObStatProcessor::incr_raw_stat(api_rsb, NULL, stat_id_, amount))) {
      WARN_API("fail to incr_raw_stat, ret=%d", ret);
    }
  }
}

void ObApiStat::decrement(const int64_t amount)
{
  int ret = OB_SUCCESS;
  if (stat_id_ >= 0) {
    if (OB_FAIL(ObStatProcessor::decr_raw_stat(api_rsb, NULL, stat_id_, amount))) {
      WARN_API("fail to decr_raw_stat, ret=%d", ret);
    }
  }
}

int64_t ObApiStat::get_next_stat_id()
{
  static int64_t next_stat_id = 0;

  return ATOMIC_FAA(&next_stat_id, 1);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
