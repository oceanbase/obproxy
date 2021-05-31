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

#ifndef OCEANBASE_COMMON_OB_DEBUG_SYNC_POINT_H_
#define OCEANBASE_COMMON_OB_DEBUG_SYNC_POINT_H_

#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{
class ObString;

#define OB_DEBUG_SYNC_POINT_DEF(ACT)                               \
    ACT(INVALID_DEBUG_SYNC_POINT, = 0)                             \
    ACT(NOW,)                                                      \
    ACT(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT,)                \
    ACT(BEFORE_REBALANCE_TASK_EXECUTE,)                            \
    ACT(REBALANCE_TASK_MGR_BEFORE_EXECUTE_OVER,)                   \
    ACT(UNIT_BALANCE_BEFORE_PARTITION_BALANCE,)                    \
    ACT(BEFORE_UNIT_MANAGER_LOAD,)                                 \
    ACT(BEFORE_INNER_SQL_COMMIT,)                                  \
    ACT(BEFORE_ASYNC_PT_UPDATE_TASK_EXECUTE,)                      \
    ACT(AFTER_ASYNC_PT_UPDATE_TASK_EXECUTE,)                       \
    ACT(DAILY_MERGE_BEFORE_RESTORE_LEADER_POS,)                    \
    ACT(SWITCH_LEADER_BEFORE_SYS_COORDINATE_COMMIT,)               \
    ACT(START_UNIT_BALANCE,)                                       \
    ACT(BEFORE_TRY_DELETE_SERVERS,)                                \
    ACT(BEFORE_TRY_FREEZE_PARTITION_TABLE,)                        \
    ACT(BEFORE_WRITE_CHECK_POINT,)                                 \
    ACT(BEFORE_COMMIT_CHECK_POINT,)                                \
    ACT(BEFORE_PARTITION_MERGE_COMMIT,)                            \
    ACT(BEFORE_PARTITION_MIGRATE_COMMIT,)                          \
    ACT(BEFORE_SEND_UPDATE_INDEX_STATUS,)                          \
    ACT(AFTER_INSERT_FLAG_REPLICA,)                                \
    ACT(OBSERVICE_GET_LEADER_CANDIDATES,)                          \
    ACT(CHECK_NEW_TENANT,)                                         \
    ACT(BEFORE_CHECK_MAJOR_FREEZE_DONE,)                           \
    ACT(MAX_DEBUG_SYNC_POINT,)

DECLARE_ENUM(ObDebugSyncPoint, debug_sync_point, OB_DEBUG_SYNC_POINT_DEF);

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_DEBUG_SYNC_POINT_H_
