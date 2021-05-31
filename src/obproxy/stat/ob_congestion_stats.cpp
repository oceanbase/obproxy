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

#include "stat/ob_congestion_stats.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

int register_congest_stats()
{
  int ret = OB_SUCCESS;

  congest_rsb = g_stat_processor.allocate_raw_stat_block(congest_stat_count, XFH_CONGESTION_STATE);
  if (OB_ISNULL(congest_rsb)) {
     ret = OB_ALLOCATE_MEMORY_FAILED;
     PROXY_LOG(ERROR, "failed to allocate raw stat block", K(ret));
  } else if (OB_FAIL(g_stat_processor.register_raw_stat(congest_rsb, RECT_PROCESS, "dead_congested",
                     RECD_INT, dead_congested_stat, SYNC_SUM, RECP_NULL))) {
    PROXY_LOG(WARN, "fail to register dead_congested", K(ret));
  } else if (OB_FAIL(g_stat_processor.register_raw_stat(congest_rsb, RECT_PROCESS, "alive_congested",
                     RECD_INT, alive_congested_stat, SYNC_SUM, RECP_NULL))) {
    PROXY_LOG(WARN, "fail to register alive_congested", K(ret));
  }

  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
