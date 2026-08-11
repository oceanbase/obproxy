/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
     PROXY_LOG(EDIAG, "failed to allocate raw stat block", K(ret));
  } else if (OB_FAIL(g_stat_processor.register_raw_stat(congest_rsb, RECT_PROCESS, "dead_congested",
                     RECD_INT, dead_congested_stat, SYNC_SUM, RECP_NULL))) {
    PROXY_LOG(WDIAG, "fail to register dead_congested", K(ret));
  } else if (OB_FAIL(g_stat_processor.register_raw_stat(congest_rsb, RECT_PROCESS, "alive_congested",
                     RECD_INT, alive_congested_stat, SYNC_SUM, RECP_NULL))) {
    PROXY_LOG(WDIAG, "fail to register alive_congested", K(ret));
  }

  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
