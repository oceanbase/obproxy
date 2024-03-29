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

#ifdef DEF_NAME
// define all names here(use lower case)
// @note append only
DEF_NAME(id, "id")
    DEF_NAME(t, "timestamp")
    DEF_NAME(first, "first")
    DEF_NAME(second, "second")
    DEF_NAME(ret, "return code")
    DEF_NAME(key, "key")
    DEF_NAME(value, "value")
    DEF_NAME(trace_id, "trace id")
    DEF_NAME(id1, "id1")
    DEF_NAME(id2, "id2")
    DEF_NAME(id3, "id3")
    DEF_NAME(arg, "arg")
    DEF_NAME(arg1, "arg1")
    DEF_NAME(arg2, "arg2")
    DEF_NAME(arg3, "arg3")
    DEF_NAME(arg4, "arg4")
    DEF_NAME(val1, "val1")
    DEF_NAME(val2, "val2")
    DEF_NAME(val3, "val3")
    DEF_NAME(param, "param")
    DEF_NAME(tag1, "tag1")
    DEF_NAME(tag2, "tag2")
    DEF_NAME(tag3, "tag3")
    DEF_NAME(tag4, "tag4")
    DEF_NAME(tag5, "tag5")
    DEF_NAME(tag6, "tag6")
    DEF_NAME(tag7, "tag7")
    DEF_NAME(tag8, "tag8")
    DEF_NAME(tag9, "tag9")
    DEF_NAME(tag10, "tag10")
    DEF_NAME(tag11, "tag11")
    DEF_NAME(tag12, "tag12")
    DEF_NAME(tag13, "tag13")
    DEF_NAME(tag14, "tag14")
    DEF_NAME(tag15, "tag15")
    DEF_NAME(tag16, "tag16")
    DEF_NAME(tag17, "tag17")
    DEF_NAME(tag18, "tag18")
    DEF_NAME(tag19, "tag19")
    DEF_NAME(tag20, "tag20")
    DEF_NAME(read_only, "read only")
    DEF_NAME(access_mode, "access_mode")
    DEF_NAME(type, "type")
    DEF_NAME(flag, "flag")
    DEF_NAME(null, "null")
    DEF_NAME(tenant, "tenant")
    DEF_NAME(tenant_id, "tenant id")
    DEF_NAME(db, "database")
    DEF_NAME(table, "table")
    DEF_NAME(tid, "table_id")
    DEF_NAME(cid, "column_id")
    DEF_NAME(column, "column")
    DEF_NAME(session, "session")
    DEF_NAME(sid, "session id")
    DEF_NAME(user, "user")
    DEF_NAME(partition, "partition")
    DEF_NAME(pkey, "partition key")
    DEF_NAME(part_idx, "partition index")
    DEF_NAME(part_cnt, "partition count")
    DEF_NAME(plan_id, "plan_id")
    DEF_NAME(affected_rows, "affected_rows")
    DEF_NAME(limit, "limit")
    DEF_NAME(offset, "offset")
    DEF_NAME(scan, "scan")
    DEF_NAME(get, "get")
    DEF_NAME(idx, "idx")
    DEF_NAME(used, "used")
    DEF_NAME(num, "num")
    DEF_NAME(init, "init")
    DEF_NAME(destroy, "destroy")
    DEF_NAME(exiting, "exiting")
    DEF_NAME(ref, "reference count")
    DEF_NAME(length, "length")
    DEF_NAME(precision, "precision")
    DEF_NAME(scale, "scale")
    DEF_NAME(sys_var, "system variable")
    DEF_NAME(user_var, "user variable")
    DEF_NAME(op, "operator")
    DEF_NAME(func, "function")
    DEF_NAME(timeout, "timeout")
    DEF_NAME(ip, "ip address")
    DEF_NAME(port, "port")
    DEF_NAME(addr, "address")
    DEF_NAME(pcode, "packet code")
    DEF_NAME(before_processor_run, "before processor run")
    DEF_NAME(start_rpc, "start rpc")
    DEF_NAME(start_sql, "start sql")
    DEF_NAME(found_rows, "found rows")
    DEF_NAME(return_rows, "return rows")
    DEF_NAME(input_count, "input count")
    DEF_NAME(total_count, "total count")
    DEF_NAME(last_insert_id, "last insert id")
    DEF_NAME(receive, "receive")
    DEF_NAME(transmit, "transmit")
    DEF_NAME(remote_result, "remote result")
    DEF_NAME(process_ret, "process return code")
    DEF_NAME(stmt, "statement")
    DEF_NAME(store_found_rows, "store found rows")
    DEF_NAME(start_trans, "start transaction")
    DEF_NAME(start_part, "start participant")
    DEF_NAME(trans_id, "transaction id")
    DEF_NAME(remote_task_completed, "remote task completed")
    DEF_NAME(task, "task")
    DEF_NAME(hash, "hash")
    DEF_NAME(inc, "inc")
    DEF_NAME(scanner, "scanner")
    DEF_NAME(wait_start, "wait start")
    DEF_NAME(wait_end, "wait end")
    DEF_NAME(job_id, "job id")
    DEF_NAME(task_id, "task id")
    DEF_NAME(runner_svr, "runner server")
    DEF_NAME(ctrl_svr, "scheduler server")
    DEF_NAME(execution_id, "execution id")
    DEF_NAME(lock_row, "lock row")
    DEF_NAME(execute_task, "execute task")
    DEF_NAME(stmt_type, "statement type")
    DEF_NAME(execute_async_task, "execute async task")
    DEF_NAME(check_priv, "check privilege")
    DEF_NAME(result_set_close, "result set close")
    DEF_NAME(async, "async")
    DEF_NAME(row_count, "row count")
    DEF_NAME(post_packet, "post packet")
    DEF_NAME(revert_scan_iter, "revert scan iterator")
    DEF_NAME(get_row, "get row")
    DEF_NAME(plc_sys_cache_get, "plc sys cache get end")
    DEF_NAME(plc_user_cache_get_end, "plc user cache get end")
    DEF_NAME(tl_calc_parid_end, "tl calc part id end")
    DEF_NAME(tl_calc_by_range_end, "tl calc by range end")
    DEF_NAME(cache_get_value, "pc get value end")
    DEF_NAME(cache_update_stat, "pcv update stmt stat")
    DEF_NAME(pc_choose_plan, "pc choose plan end")
    DEF_NAME(calculate_type_end, "pc calc type end")
    DEF_NAME(get_plan_type_end, "pc get plan type end")
    DEF_NAME(tl_calc_part_id_end, "tl calc part id end")
    DEF_NAME(cons_context, "construct context")
    DEF_NAME(handle_message, "handle message")
    DEF_NAME(msg, "message")
    DEF_NAME(msg_type, "message type")
    DEF_NAME(handle_timeout, "handle timeout")
    DEF_NAME(kill, "kill")
    DEF_NAME(scheduler, "scheduler")
    DEF_NAME(trans_version, "transaction version")
    DEF_NAME(part_start_trans, "partition start transaction")
    DEF_NAME(part_end_trans, "partition end transaction")
    DEF_NAME(left_time, "left time")
    DEF_NAME(is_rollback, "is rollback")
    DEF_NAME(end_task, "end task")
    DEF_NAME(on_sync_log_succ, "on_sync_log_success")
    DEF_NAME(leader_active, "leader active")
    DEF_NAME(leader_revoke, "leader revoke")
    DEF_NAME(need_release, "need release")
    DEF_NAME(skip_replay_redo, "skip replay redo log")
    DEF_NAME(replay_redo, "replay redo log")
    DEF_NAME(replay_prepare, "replay prepare log")
    DEF_NAME(replay_commit, "replay commit log")
    DEF_NAME(replay_abort, "replay abort log")
    DEF_NAME(replay_clear, "replay clear log")
    DEF_NAME(submit_log, "submit log")
    DEF_NAME(submit_commit, "submit commit")
    DEF_NAME(submit_abort, "submit abort")
    DEF_NAME(update_trans_version, "update transaction version")
    DEF_NAME(prepare, "prepare")
    DEF_NAME(end_trans_cb, "end trans callback")
    DEF_NAME(publish_version, "publish version")
    DEF_NAME(submit_recommit, "submit recommit")
    DEF_NAME(submit_reabort, "submit reabort")
    DEF_NAME(submit_recreate, "submit recreate")
    DEF_NAME(start_stmt, "submit stmt")
    DEF_NAME(end_stmt, "end stmt")
    DEF_NAME(end_trans, "end transaction")
    DEF_NAME(sender, "sender")
    DEF_NAME(election, "election")
    DEF_NAME(new_period, "election new period")
    DEF_NAME(election_init, "election init")
    DEF_NAME(lease_expired, "leader lease expired")
    DEF_NAME(reappoint, "reappoint")
    DEF_NAME(reappoint_count, "reappoint count")
    DEF_NAME(cur_leader, "current leader")
    DEF_NAME(new_leader, "new leader")
    DEF_NAME(priority, "priority")
    DEF_NAME(ticket, "ticket")
    DEF_NAME(valid_candidates, "valid candidates")
    DEF_NAME(send_timestamp, "send timestamp")
    DEF_NAME(leader, "leader")
    DEF_NAME(vote_leader, "vote leader")
    DEF_NAME(T1_timestamp, "T1 timestamp")
    DEF_NAME(t1, "t1")
    DEF_NAME(epoch, "epoch")
    DEF_NAME(is_candidate, "is candidate")
    DEF_NAME(membership_version, "membership version")
    DEF_NAME(log_id, "log id")
    DEF_NAME(data_version, "data version")
    DEF_NAME(version, "version")
    DEF_NAME(server, "server")
    DEF_NAME(is_running, "is running")
    DEF_NAME(is_changing_leader, "is changing leader")
    DEF_NAME(self, "self")
    DEF_NAME(proposal_leader, "proposal leader")
    DEF_NAME(curr_candidates, "current candidates")
    DEF_NAME(curr_membership_version, "curr_membership_version")
    DEF_NAME(replica_num, "replica number")
    DEF_NAME(leader_epoch, "leader epoch")
    DEF_NAME(leader_lease, "leader lease")
    DEF_NAME(election_time_offset, "election_time_offset")
    DEF_NAME(active_timestamp, "active_timestamp")
    DEF_NAME(stat, "stat")
    DEF_NAME(role, "role")
    DEF_NAME(stage, "stage")
    DEF_NAME(member, "member")
    DEF_NAME(unconfirmed_leader, "unconfirmed_leader")
    DEF_NAME(is_need_query, "is_need_query")
    DEF_NAME(takeover_t1_timestamp, "takeover_t1_timestamp")
    DEF_NAME(click, "click")
    DEF_NAME(start_ts, "start timestamp")
    DEF_NAME(last_ts, "last timestamp")
    DEF_NAME(in_queue_time, "in queue time")
    DEF_NAME(receive_ts, "receive packet timestamp")
    DEF_NAME(enqueue_ts, "enqueue timestamp")
    DEF_NAME(run_ts, "run timestamp")
    DEF_NAME(expect_ts, "expect ts")
    DEF_NAME(handle_devote_prepare, "handle devote prepare")
    DEF_NAME(handle_devote_vote, "handle devote vote")
    DEF_NAME(handle_devote_success, "handle devote success")
    DEF_NAME(handle_vote_prepare, "handle vote prepare")
    DEF_NAME(handle_vote_vote, "handle vote vote")
    DEF_NAME(handle_vote_success, "handle vote success")
    DEF_NAME(handle_query_leader, "handle query leader")
    DEF_NAME(handle_query_leader_response, "handle query leader response")
    DEF_NAME(send_devote_prepare, "send devote prepare")
    DEF_NAME(send_vote_prepare, "send vote prepare")
    DEF_NAME(send_query_leader, "send query leader")
    DEF_NAME(process_devote_prepare, "process devote prepare")
    DEF_NAME(process_vote_prepare, "process vote prepare")
    DEF_NAME(send_devote_vote, "send devote vote")
    DEF_NAME(send_vote_vote, "send vote vote")
    DEF_NAME(process_devote_vote, "process devote vote")
    DEF_NAME(process_vote_vote, "process vote vote")
    DEF_NAME(send_devote_success, "send devote success")
    DEF_NAME(send_vote_success, "send vote success")
    DEF_NAME(process_centrialized_voting, "process centrialized voting")
    DEF_NAME(process_centrialized_counting, "process centrialized counting")
    DEF_NAME(process_centrialized_counting_v2, "process centrialized counting v2")
    DEF_NAME(process_centrialized_success, "process centrialized success")
    DEF_NAME(upgrade_mode, "upgrade mode")
    DEF_NAME(run_gt1_task, "run gt1 task")
    DEF_NAME(response_scheduler, "response scheduler")
    DEF_NAME(compensate_prepare_no_log, "compensate prepare no log")
    DEF_NAME(phy_plan_type, "phy plan type")
    DEF_NAME(start_task, "start task")
    DEF_NAME(sql_no, "sql number")
    DEF_NAME(rescan, "rescan")
    DEF_NAME(range, "range")
    DEF_NAME(cluster_version, "cluster version")
    //////////////// << add new name BEFORE this line
    DEF_NAME(__PAIR_NAME_BEGIN__, "invalid")
    //////////////// << add pair events AFTER this line
    DEF_NAME(process_begin, "process begin")
    DEF_NAME(process_end, "process end")
    DEF_NAME(query_start, "query start")
    DEF_NAME(query_end, "query end")
    DEF_NAME(parse_start, "parse start")
    DEF_NAME(parse_end, "parse end")
    DEF_NAME(cache_get_plan_start, "pc get plan start")
    DEF_NAME(cache_get_plan_end, "pc get plan end")
    DEF_NAME(resolver_start, "resolve start")
    DEF_NAME(resolver_end, "resolve end")
    DEF_NAME(transform_start, "transform start")
    DEF_NAME(transform_end, "transform end")
    DEF_NAME(optimizer_start, "optimizer start")
    DEF_NAME(optimizer_end, "optimizer end")
    DEF_NAME(cg_start, "cg start")
    DEF_NAME(cg_end, "cg end")
    DEF_NAME(exec_start, "execution start")
    DEF_NAME(exec_end, "execution end")
    DEF_NAME(job_exec_step_start, "job execute step start")
    DEF_NAME(job_exec_step_end, "job execute step end")
    DEF_NAME(distributed_schedule_start, "distributed schedule start")
    DEF_NAME(distributed_schedule_end, "distributed schedule end")
    DEF_NAME(exec_plan_start, "execute plan start")
    DEF_NAME(exec_plan_end, "execute plan end")
    DEF_NAME(parse_job_start, "parse job start")
    DEF_NAME(parse_job_end, "parse job end")
    DEF_NAME(do_open_plan_start, "do open plan start")
    DEF_NAME(do_open_plan_end, "do open plan end")
    DEF_NAME(sql_start_stmt_start, "sql start stmt start")
    DEF_NAME(sql_start_stmt_end, "sql start stmt end")
    DEF_NAME(sql_start_participant_start, "sql start participant start")
    DEF_NAME(sql_start_participant_end, "sql start participant end")
    DEF_NAME(kv_get_start, "kv get start")
    DEF_NAME(kv_get_end, "kv get end")
    DEF_NAME(plc_get_from_cache_start, "plc get from cache start")
    DEF_NAME(plc_get_from_cache_end, "plc get from cache end")
    DEF_NAME(plc_serialize_start, "plc serialize start")
    DEF_NAME(plc_serialize_end, "plc serialize end")

    DEF_NAME(storage_table_scan_begin, "table scan begin")
    DEF_NAME(storage_table_scan_end, "table scan end")
    DEF_NAME(get_location_cache_begin, "get location cache begin")
    DEF_NAME(get_location_cache_end, "get location cache end")
    DEF_NAME(calc_partition_location_begin, "calc partition location begin")
    DEF_NAME(calc_partition_location_end, "calc partition location end")
    // << add new name BEFORE this line
    DEF_NAME(NAME_COUNT, "invalid")
#endif
////////////////////////////////////////////////////////////////

#ifndef _OB_NAME_ID_DEF_H
#define _OB_NAME_ID_DEF_H 1
#include <stdint.h>
namespace oceanbase
{
namespace name
{
enum ObNameId
{
#define DEF_NAME(name_sym, description) name_sym,
#include "ob_name_id_def.h"
#undef DEF_NAME
};

// get name at runtime
/* this function is defined for c driver client compile */
inline const char* get_name(int32_t id) {((void)(id)); return nullptr;};
const char* get_description(int32_t id);
} // end namespace name_id_map
} // end namespace oceanbase

#define ID(name_sym) (::oceanbase::name::name_sym)
#define NAME(name_id) (::oceanbase::name::get_name(name_id))
#define Y(x) ID(x), x
#define Y_(x) ID(x), x ##_

#endif /* _OB_NAME_ID_DEF_H */
