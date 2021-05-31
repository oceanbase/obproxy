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

#include "stat/ob_resource_pool_stats.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

#define RESOURCE_POOL_REGISTER_RAW_STAT(rsb, rec_type, name, data_type, id, sync_type, persist_type) \
  if (OB_SUCC(ret)) { \
    ret = g_stat_processor.register_raw_stat(rsb, rec_type, name, data_type, id, sync_type, persist_type); \
  }

ObRecRawStatBlock *resource_pool_rsb = NULL;

int init_resource_pool_stats()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(resource_pool_rsb = g_stat_processor.allocate_raw_stat_block(RESOURCE_POOL_STAT_COUNT,
      XFH_CLUSTER_RESOURCE_STATE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(WARN, "fail to alloc mem for resource_pool_rsb", K(ret));
  } else {
    RESOURCE_POOL_REGISTER_RAW_STAT(resource_pool_rsb, RECT_PROCESS, "create_cluster_resource_count",
                                    RECD_INT, CREATE_CLUSTER_RESOURCE_COUNT, SYNC_SUM, RECP_NULL);

    RESOURCE_POOL_REGISTER_RAW_STAT(resource_pool_rsb, RECT_PROCESS, "create_cluster_resource_time",
                                    RECD_INT, CREATE_CLUSTER_RESOURCE_TIME, SYNC_SUM, RECP_NULL);

    RESOURCE_POOL_REGISTER_RAW_STAT(resource_pool_rsb, RECT_PROCESS, "get_cluster_resource_fail_count",
                                    RECD_INT, GET_CLUSTER_RESOURCE_FAIL_COUNT, SYNC_SUM, RECP_NULL);

    RESOURCE_POOL_REGISTER_RAW_STAT(resource_pool_rsb, RECT_PROCESS, "parallel_create_cluster_resource_count",
                                    RECD_INT, PARALLEL_CREATE_CLUSTER_RESOURCE_COUNT, SYNC_SUM, RECP_NULL);

    RESOURCE_POOL_REGISTER_RAW_STAT(resource_pool_rsb, RECT_PROCESS, "current_cluster_resource_count",
                                    RECD_INT, CURRENT_CLUSTER_RESOURCE_COUNT, SYNC_SUM, RECP_NULL);

    RESOURCE_POOL_REGISTER_RAW_STAT(resource_pool_rsb, RECT_PROCESS, "create_cluster_resource_succ_count",
                                    RECD_INT, CREATE_CLUSTER_RESOURCE_SUCC_COUNT, SYNC_SUM, RECP_NULL);

    RESOURCE_POOL_REGISTER_RAW_STAT(resource_pool_rsb, RECT_PROCESS, "delete_cluster_resource_count",
                                    RECD_INT, DELETE_CLUSTER_RESOURCE_COUNT, SYNC_SUM, RECP_NULL);

    RESOURCE_POOL_REGISTER_RAW_STAT(resource_pool_rsb, RECT_PROCESS, "free_cluster_resource_count",
                                    RECD_INT, FREE_CLUSTER_RESOURCE_COUNT, SYNC_SUM, RECP_NULL);

    RESOURCE_POOL_REGISTER_RAW_STAT(resource_pool_rsb, RECT_PROCESS, "create_cluster_client_pool_succ_count",
                                    RECD_INT, CREATE_CLUSTER_CLIENT_POOL_SUCC_COUNT, SYNC_SUM, RECP_NULL);

    RESOURCE_POOL_REGISTER_RAW_STAT(resource_pool_rsb, RECT_PROCESS, "delete_cluster_client_pool_count",
                                    RECD_INT, DELETE_CLUSTER_CLIENT_POOL_COUNT, SYNC_SUM, RECP_NULL);

    RESOURCE_POOL_REGISTER_RAW_STAT(resource_pool_rsb, RECT_PROCESS, "free_cluster_client_pool_count",
                                    RECD_INT, FREE_CLUSTER_CLIENT_POOL_COUNT, SYNC_SUM, RECP_NULL);

    RESOURCE_POOL_REGISTER_RAW_STAT(resource_pool_rsb, RECT_PROCESS, "create_async_common_task_count",
                                    RECD_INT, CREATE_ASYNC_COMMON_TASK_COUNT, SYNC_SUM, RECP_NULL);

    RESOURCE_POOL_REGISTER_RAW_STAT(resource_pool_rsb, RECT_PROCESS, "destroy_async_common_task_count",
                                    RECD_INT, DESTROY_ASYNC_COMMON_TASK_COUNT, SYNC_SUM, RECP_NULL);
  }

  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
