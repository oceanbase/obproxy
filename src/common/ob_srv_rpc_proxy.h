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

#ifndef RPC_S
#define RPC_S(...)
#endif
#ifndef RPC_AP
#define RPC_AP(...)
#endif
// special usage when can't deliver request.
RPC_S(PR5 nouse, OB_ERROR_PACKET);

RPC_S(PR5 get_rootserver_role, OB_GET_ROLE, ObGetRootserverRoleResult);
RPC_S(PR5 set_config, OB_SET_CONFIG, (common::ObString));
RPC_S(PR5 get_config, OB_GET_CONFIG, common::ObString);

// notify server to add tenant temporary, that server will have
// minimal quota for a while.
// ARG: tenant_id
// RES: NULL
// RET: OB_SUCCESS add tenant successfully
//      OTHER other error
RPC_S(PR5 add_tenant_tmp, OB_ADD_TENANT_TMP, (UInt64));

RPC_AP(PR5 create_partition_batch, OB_CREATE_PARTITION_BATCH, (ObCreatePartitionBatchArg), ObCreatePartitionBatchRes);
RPC_AP(PR5 create_partition, OB_CREATE_PARTITION, (ObCreatePartitionArg));
RPC_S(PR5 fetch_root_partition, OB_FETCH_ROOT_PARTITION,
      share::ObPartitionReplica);
RPC_S(PR5 migrate, OB_MIGRATE, (ObMigrateArg));
RPC_S(PR5 add_temporay_replica, OB_ADD_TEMPORARY_REPLICA, (ObAddTemporaryReplicaArg));
RPC_S(PR5 add_replica, OB_ADD_REPLICA, (ObAddReplicaArg));
RPC_S(PR5 remove_replica, OB_REMOVE_REPLICA, (ObRemoveReplicaArg));
RPC_S(PR5 remove_member, OB_REMOVE_MEMBER, (ObMemberChangeArg));
RPC_S(PR5 migrate_replica, OB_MIGRATE_REPLICA, (ObMigrateReplicaArg));
RPC_AP(PR5 prepare_major_freeze, OB_PREPARE_MAJOR_FREEZE, (ObMajorFreezeArg), obrpc::Int64);
RPC_AP(PR5 commit_major_freeze, OB_COMMIT_MAJOR_FREEZE, (ObMajorFreezeArg), obrpc::Int64);
RPC_AP(PR5 abort_major_freeze, OB_ABORT_MAJOR_FREEZE, (ObMajorFreezeArg), obrpc::Int64);
RPC_AP(PR5 query_major_freeze_status, OB_QUERY_MAJOR_FREEZE_STATUS,
       (ObQueryMajorFreezeStatusArg), ObQueryMajorFreezeStatusResult);

RPC_S(PR5 get_member_list, OB_GET_MEMBER_LIST, (common::ObPartitionKey), ObServerList);
RPC_S(PR5 switch_leader, OB_SWITCH_LEADER, (ObSwitchLeaderArg));
RPC_S(PR5 switch_leader_list, OB_SWITCH_LEADER_LIST, (ObSwitchLeaderListArg));
RPC_S(PR5 get_leader_candidates, OB_GET_LEADER_CANDIDATES,
      (ObGetLeaderCandidatesArg), ObGetLeaderCandidatesResult);
RPC_S(PR5 get_partition_count, OB_GET_PARTITION_COUNT,
    ObGetPartitionCountResult);
RPC_S(PR5 switch_schema, OB_SWITCH_SCHEMA, (Int64));
RPC_S(PR5 bootstrap, OB_BOOTSTRAP, (ObServerInfoList));
RPC_S(PR5 is_empty_server, OB_IS_EMPTY_SERVER, Bool);
RPC_S(PR5 get_partition_stat, OB_GET_PARTITION_STAT, ObPartitionStatList);
RPC_S(PR5 report_replica, OB_REPORT_REPLICA);
RPC_S(PR5 recycle_replica, OB_RECYCLE_REPLICA);
RPC_S(PR5 drop_replica, OB_DROP_REPLICA, (ObDropReplicaArg));
RPC_S(PR5 clear_location_cache, OB_CLEAR_LOCATION_CACHE);
RPC_S(PR5 refresh_sync_value, OB_REFRESH_SYNC_VALUE, (ObAutoincSyncArg));
RPC_S(PR5 sync_auto_increment, OB_SYNC_AUTO_INCREMENT, (ObAutoincSyncArg));
RPC_S(PR5 clear_autoinc_cache, OB_CLEAR_AUTOINC_CACHE, (ObAutoincSyncArg));
RPC_S(PR5 dump_memtable, OB_DUMP_MEMTABLE, (common::ObPartitionKey));
RPC_S(PR5 force_set_as_single_replica, OB_FORCE_SET_AS_SINGLE_REPLICA, (common::ObPartitionKey));
RPC_S(PR5 set_debug_sync_action, OB_SET_DS_ACTION, (obrpc::ObDebugSyncActionArg));
RPC_S(PR5 request_heartbeat, OB_REQUEST_HEARTBEAT, share::ObLeaseRequest);
RPC_S(PR5 broadcast_sys_schema, OB_BROADCAST_SYS_SCHEMA,
      (common::ObSArray<share::schema::ObTableSchema>));
RPC_S(PR5 check_partition_table, OB_CHECK_PARTITION_TABLE);
RPC_S(PR5 get_member_list_and_leader, OB_GET_MEMBER_LIST_AND_LEADER,
    (common::ObPartitionKey), obrpc::ObMemberListAndLeaderArg);
RPC_S(PR5 sync_partition_table, OB_SYNC_PARTITION_TABLE, (obrpc::Int64));

#ifdef RPC_S
#undef RPC_S
#endif
#ifdef RPC_AP
#undef RPC_AP
#endif

#ifndef _OCEABASE_COMMON_OB_SRV_RPC_PROXY_H_
#define _OCEABASE_COMMON_OB_SRV_RPC_PROXY_H_

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "storage/ob_partition_service_rpc.h"
#include "share/ob_lease_struct.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace obrpc
{
#define RPC_S RPC_S_S
#define RPC_AP RPC_AP_S
#include "ob_srv_rpc_proxy.h"
#ifdef RPC_S
#undef RPC_S
#endif
#ifdef RPC_AP
#undef RPC_AP
#endif

class ObSrvRpcProxy
    : public ObRpcProxy
{
public:
  DEFINE_TO(ObSrvRpcProxy);

#define RPC_S RPC_S_M
#define RPC_AP RPC_AP_M
#include "ob_srv_rpc_proxy.h"
#ifdef RPC_S
#undef RPC_S
#endif
#ifdef RPC_AP
#undef RPC_AP
#endif

}; // end of class ObSrvRpcProxy

} // end of namespace rpc
} // end of namespace oceanbase

#endif /* _OCEABASE_COMMON_OB_SRV_RPC_PROXY_H_ */
