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
#define USING_LOG_PREFIX PROXY_SM
#include "proxy/rpc_optimize/ob_rpc_request_sm.h"
#include "common/ob_hint.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_hrtime.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/profile/ob_trace_id_new.h"
#include "iocore/net/ob_inet.h"
#include "iocore/eventsystem/ob_vconnection.h"
#include "iocore/eventsystem/ob_lock.h"
#include "utils/ob_proxy_monitor_utils.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "obutils/ob_config_server_processor.h"
#include "obutils/ob_resource_pool_processor.h"
#include "obutils/ob_task_flow_controller.h"
#include "obutils/ob_tenant_stat_manager.h"
#include "obutils/ob_congestion_entry.h"
#include "prometheus/ob_net_prometheus.h"
#include "prometheus/ob_rpc_prometheus.h"
#include "prometheus/ob_thread_prometheus.h"
#include "omt/ob_proxy_config_table_processor.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "proxy/route/ob_mysql_route.h"
#include "proxy/route/ob_table_entry.h"
#include "obkv/table/ob_table_rpc_request.h"
#include "obkv/table/ob_table_rpc_response.h"
#include "proxy/route/ob_mysql_route.h"
#include "proxy/route/ob_table_cache.h"
#include "proxy/route/ob_index_processor.h"
#include "proxy/mysqllib/ob_mysql_config_processor.h"
#include "proxy/rpc_optimize/ob_rpc_req_debug_names.h"
#include "proxy/rpc_optimize/net/ob_rpc_client_net_handler.h"
#include "proxy/rpc_optimize/net/ob_rpc_server_net_handler.h"
#include "proxy/rpc_optimize/rpclib/ob_table_query_async_processor.h"
#include "proxy/rpc_optimize/rpclib/ob_tablegroup_processor.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_req_ctx_processor.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_req_analyzer.h"
#include "proxy/rpc_optimize/optimizer/ob_rpc_req_sharding_plan.h"

using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obkv;
using namespace oceanbase::obproxy::prometheus;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::share;

#define __REMEMBER(x)  #x
#define _REMEMBER(x)   __REMEMBER(x)

#define REMEMBER(e, r) {                                        \
    add_history_entry(__FILE__ ":" _REMEMBER (__LINE__), e, r); \
  }

#define STATE_ENTER(state_name, e) { \
  if (OB_UNLIKELY(!get_global_performance_params().enable_performance_mode_)) { \
    REMEMBER (e, reentrancy_count_); \
    int64_t stack_start = event::self_ethread().stack_start_; \
    _PROXY_SM_LOG(DEBUG, "sm_id=%u, stack_size=%ld, next_action=%s, event=%s", \
                  sm_id_, stack_start - reinterpret_cast<int64_t>(&stack_start), \
                  #state_name, ObRpcReqDebugNames::get_event_name(static_cast<enum ObRpcRequestSMActionType>(e))); } \
  }

#define RPC_REQUEST_SM_SET_DEFAULT_HANDLER(h) {          \
  REMEMBER(-1,reentrancy_count_);                        \
  default_handler_ = h;}

const int64_t ObRpcRequestSM::MAX_SCATTER_LEN = (sizeof(ObRpcRequestSM) / sizeof(int64_t));
static int64_t val[ObRpcRequestSM::MAX_SCATTER_LEN];
static int16_t to[ObRpcRequestSM::MAX_SCATTER_LEN];
static int64_t scat_count = 0;

int64_t ObRpcReqStreamSizeStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(client_request_bytes),
       K_(server_request_bytes),
       K_(server_response_bytes),
       K_(client_response_bytes));
  J_OBJ_END();
  return pos;
}

int64_t ObRpcReqRetryStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(request_retry_times),
       K_(request_move_reroute_times),
       K_(request_retry_consume_time_us));
  J_OBJ_END();
  return pos;
}


void ObPartitionLookupInfo::pl_update_if_necessary(const ObMysqlConfigParams &mysql_config_params)
{
  const bool need_update = need_update_entry_by_partition_hit();
  const bool is_empty_entry_allowed = route_.is_empty_entry_allowed();
  bool is_read_weak_supported = true;

  LOG_DEBUG("ObRpcRequestSM [ObPartitionLookupInfo::pl_update_if_necessary]",
              // K_(rpc_trace_id),
              "origin_name", te_name_,
              "is_in_standard_routing_mode", mysql_config_params.is_standard_routing_mode(),
               K(need_update),
               K(is_empty_entry_allowed),
              "route_info", route_, KPC(route_.part_entry_));

  if ((mysql_config_params.is_standard_routing_mode()) && need_update && !is_empty_entry_allowed) {
    // we will update pl when:
    // 1. strong read, but leader return miss, or follower return hit
    //or
    // 2. strong read, but leader do not exist
    //or
    // 3. weak read, but calculate server return miss or tenant server return hit
    if (route_.is_strong_read()) {
      const bool is_leader = is_leader_server();
      const bool has_table_but_no_leader = (!route_.is_no_route_info_found() && !is_leader_existent());
      if (has_table_but_no_leader) {
        LOG_INFO("will set strong read route dirty", K(is_leader), K(has_table_but_no_leader),
                "origin_name", te_name_, "route_info", route_);
        //NOTE:: if leader was congested from server, it will set_dirty in ObRpcTransact::handle_congestion_control_lookup
        //       here we only care response
        if (route_.set_target_dirty()) {
          get_pl_task_flow_controller().handle_new_task();
        }
      } else if (is_leader) {
        // this route entry is valid, renew last_valid_time;
        route_.renew_last_valid_time();
      }
    } else if (route_.is_weak_read()) {
      const bool is_target_server = is_target_location_server();
      const bool is_partition_hit = false;
      //TODO::here proxy only calculate one table entry, it is not accuracy.
      //      it may partition miss when use target_server under multi table. @gujian
      const bool update_by_not_hit = true;//(is_target_server && && is_read_weak_supported);
      const bool update_by_not_miss = (!is_target_server && is_partition_hit);
      const bool update_by_leader_dead = (route_.is_follower_first_policy() && route_.is_leader_force_congested());
      if (update_by_not_hit || update_by_not_miss || update_by_leader_dead) {

        bool is_need_force_flush = update_by_not_hit && route_.is_remote_readonly();

        LOG_INFO("will set weak read route dirty", K(is_target_server), K(is_partition_hit),
                  K(is_read_weak_supported), K(update_by_leader_dead), K(is_need_force_flush),
                  "origin_name", te_name_, "route_info", route_);

        if (route_.set_target_dirty(is_need_force_flush)) {
          get_pl_task_flow_controller().handle_new_task();
        }
      }
    } else {
      LOG_EDIAG("it should never arriver here", "origin_name", te_name_, "route_info", route_);
    }
  }
}

void ObRpcRequestSM::init_rpc_trace_id() { 
  if (rpc_req_ != NULL) {
    rpc_trace_id_ = rpc_req_->get_trace_id();
  }
}

void ObRpcRequestSM::make_scatter_list(ObRpcRequestSM &prototype)
{
  int64_t *p = reinterpret_cast<int64_t *>(&prototype);

  for (int64_t i = 0; i < MAX_SCATTER_LEN; ++i) {
    if (0 != p[i]) {
      to[scat_count] = static_cast<int16_t>(i);
      val[scat_count] = p[i];
      ++scat_count;
    }
  }
}

void ObRpcRequestSM::instantiate_func(ObRpcRequestSM &prototype, ObRpcRequestSM &new_instance)
{
  const int64_t history_len = sizeof(prototype.history_);
  const int64_t total_len = sizeof(ObRpcRequestSM);
  const int64_t pre_history_len =
      reinterpret_cast<char *>(&(prototype.history_)) - reinterpret_cast<char *>(&prototype);
  const int64_t post_history_len = total_len - history_len - pre_history_len;
  const int64_t post_offset = pre_history_len + history_len;

  memset(reinterpret_cast<char *>(&new_instance), 0, pre_history_len);
  memset(reinterpret_cast<char *>(&new_instance) + post_offset, 0, post_history_len);

  int64_t *pd = reinterpret_cast<int64_t *>(&new_instance);

  for (int64_t i = 0; i < scat_count; ++i) {
    pd[to[i]] = val[i];
  }
}

ObRpcRequestSM::ObRpcRequestSM()
  : ObContinuation(NULL), sm_id_(0), magic_(RPC_REQUEST_HANDLE_SM_MAGIC_DEAD), next_action_(RPC_REQ_NEW_REQUEST),
    history_pos_(0), default_handler_(NULL), pending_action_(NULL), timeout_action_(NULL), cleanup_action_(NULL),
    sharding_action_(NULL), sm_next_action_(NULL), inner_cont_(NULL), reentrancy_count_(0),
    terminate_sm_(false), rpc_req_(NULL), rpc_req_origin_channel_id_(0), create_thread_(NULL), cmd_size_stats_(), cmd_time_stats_(),
    milestones_(), mysql_config_params_(NULL), cluster_resource_(NULL), real_meta_cluster_name_(),
    real_meta_cluster_name_str_(NULL), cluster_id_(0), timeout_us_(0), retry_need_update_pl_(false),
    need_pl_lookup_(false), need_congestion_lookup_(false),
    force_retry_congested_(false), congestion_lookup_success_(false), is_congestion_entry_updated_(false),
    already_get_async_info_(false), already_get_index_entry_(false), already_get_tablegroup_entry_(false),
    congestion_entry_not_exist_count_(0), congestion_entry_(NULL), pll_info_(), rpc_trace_id_(), rpc_route_mode_(RPC_ROUTE_FIND_LEADER),
    inner_request_cleanup_mutex_()
{
  static bool scatter_inited = false;

  memset(&history_, 0, sizeof(history_));

  if (!scatter_inited) {
    make_scatter_list(*this);
    scatter_inited = true;
  }
}

// Just cleanup memory. Subsequently, you need to call cleanup to clean up all data.
inline void ObRpcRequestSM::inner_request_cleanup()
{
  LOG_DEBUG("ObRpcRequestSM::inner_request_cleanup", K_(rpc_trace_id), K(this), K_(rpc_req));
  pll_info_.reset();
  if (OB_NOT_NULL(cluster_resource_)) {
    cluster_resource_->dec_ref();
    cluster_resource_ = NULL;
  }
  if (NULL != congestion_entry_) {
    congestion_entry_->dec_ref();
    congestion_entry_ = NULL;
  }
}

inline void ObRpcRequestSM::cleanup()
{
  mutex_.release();
  inner_request_cleanup_mutex_.release();
  magic_ = RPC_REQUEST_HANDLE_SM_MAGIC_DEAD;
  rpc_req_origin_channel_id_ = 0;
  pll_info_.reset();
  if (OB_NOT_NULL(rpc_req_)) {
    rpc_req_->sm_ = NULL;    // set rpc_req request_sm to NULL
    rpc_req_ = NULL;
  }
  // todo, add new
  if (OB_NOT_NULL(cluster_resource_)) {
    cluster_resource_->dec_ref();
    cluster_resource_ = NULL;
  }
  if (NULL != congestion_entry_) {
    congestion_entry_->dec_ref();
    congestion_entry_ = NULL;
  }
}

void ObRpcRequestSM::destroy()
{
  RPC_REQ_SM_ENTER_STATE(ObRpcReq::RpcReqSmState::RPC_REQ_SM_DESTROYED);
  cleanup();
  op_thread_free(ObRpcRequestSM, this, get_rpc_request_handle_sm_allocator());
}

inline void ObRpcRequestSM::kill_this()
{
  LOG_DEBUG("ObRpcRequestSM::kill this to release", K(this), K_(rpc_req), K_(rpc_trace_id));

  int ret = OB_SUCCESS;
  if (OB_FAIL(cancel_timeout_action())) {
     LOG_WDIAG("fail to cancel timeout action", K(ret), K(this), K_(sm_id), K_(rpc_trace_id));
  }

  if (OB_FAIL(cancel_pending_action())) {
     LOG_WDIAG("fail to cancel pending action", K(ret), K(this), K_(sm_id), K_(rpc_trace_id));
  }

  if (OB_FAIL(cancel_cleanup_action())) {
     LOG_WDIAG("fail to cancel cleanup action", K(ret), K(this), K_(sm_id), K_(rpc_trace_id));
  }

  if (OB_FAIL(cancel_call_next_action())) {
     LOG_WDIAG("fail to cancel call next action", K(ret), K(this), K_(sm_id), K_(rpc_trace_id));
  }

  destroy();
}

int ObRpcRequestSM::init(ObRpcReq *rpc_req, ObProxyMutex *mutex)
{
  int ret = OB_SUCCESS;
  magic_ = RPC_REQUEST_HANDLE_SM_MAGIC_ALIVE;
  sm_id_ = get_next_sm_id();
  create_thread_ = this_ethread();
  execute_thread_ = this_ethread();
  rpc_req_ = rpc_req;
  mutex_ = mutex;
  rpc_req_origin_channel_id_ = rpc_req->get_origin_channel_id();
  init_rpc_trace_id(); //init trace info for request_sm

  SET_HANDLER(&ObRpcRequestSM::main_handler);
  //false it first
  // if (0 == 1 && ObConnectionDiagnosisTrace::is_enable_diagnosis_log(get_global_proxy_config().connection_diagnosis_option) && OB_ISNULL(connection_diagnosis_trace_)) {
  //   if (OB_ISNULL(connection_diagnosis_trace_ = op_alloc(ObConnectionDiagnosisTrace))) {
  //     ret = OB_ALLOCATE_MEMORY_FAILED;
  //     LOG_WDIAG("fail to alloc mem for diagnosis trace", K(ret), K_(rpc_trace_id));
  //   } else {
  //     connection_diagnosis_trace_->inc_ref();
  //   }
  // }
  return ret;
}

int ObRpcRequestSM::init_inner_request(event::ObContinuation *inner_cont, ObProxyMutex *mutex)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(inner_request_cleanup_mutex_ = event::new_proxy_mutex())) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(EDIAG, "fail to alloc mem for proxymutex", K(ret));
  } else {
    inner_cont_ = inner_cont;
    execute_thread_ = this_ethread();
    mutex_ = mutex;
    refresh_mysql_config();

    if (OB_FAIL(init_request_meta_info())) {
      LOG_WDIAG("fail to init_request_meta_info for inner request", K(ret), K_(rpc_trace_id));
    }
  }

  return ret;
}

int ObRpcRequestSM::schedule_cleanup_action()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(rpc_req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("call ObRpcRequestSM::schedule_cleanup_action get an invalid rpc_req", KPC_(rpc_req), K(this), K_(rpc_trace_id));
  } else if (OB_UNLIKELY(NULL != cleanup_action_)) {
    // do nothing
    LOG_DEBUG("There are already pending cleanup asynchronous tasks, do nothing", K_(cleanup_action), KP_(rpc_req), K(this), K_(rpc_trace_id));
  } else if (rpc_req_->is_inner_request()) {
    if (OB_ISNULL(execute_thread_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("inner_request rpc_request_sm execute_thread is NULL", KPC_(rpc_req), K_(execute_thread), K_(create_thread), K_(rpc_trace_id));
    } else if (OB_ISNULL(cleanup_action_ = execute_thread_->schedule_imm(this, RPC_REQUEST_SM_CLEANUP))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("fail to schedule cleanup", K_(cleanup_action), K(ret), K_(rpc_trace_id));
    } else {
      LOG_DEBUG("succ to schedule cleanup_action in execute_thread", KP_(cleanup_action), K(this), KP_(rpc_req), KP_(execute_thread), K_(rpc_trace_id));
    }
  } else {
    if (OB_ISNULL(create_thread_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("rpc_request_sm create_thread is NULL", KPC_(rpc_req), K_(rpc_trace_id));
    } else if (OB_ISNULL(cleanup_action_ = create_thread_->schedule_imm(this, RPC_REQUEST_SM_CLEANUP))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("fail to schedule cleanup", K_(cleanup_action), K(ret), K_(rpc_trace_id));
    } else {
      LOG_DEBUG("succ to schedule cleanup_action in create_thread", KP_(cleanup_action), K(this), KP_(rpc_req), KP_(create_thread), K_(rpc_trace_id));
    }
  }

  return ret;
}
int ObRpcRequestSM::cancel_cleanup_action()
{
  int ret = common::OB_SUCCESS;
  if (NULL != cleanup_action_) {
    if (OB_FAIL(cleanup_action_->cancel())) {
      PROXY_LOG(WDIAG, "fail to cancel cleanup action", K_(cleanup_action), K(ret), K_(rpc_trace_id));
    } else {
      cleanup_action_ = NULL;
    }
  }
  return ret;
}

int ObRpcRequestSM::calc_rpc_timeout_us()
{
  int ret = OB_SUCCESS;
  ObRpcRequest *request = NULL;

  if (!is_valid_rpc_req() || OB_ISNULL(request = rpc_req_->get_rpc_request())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc req is invalid", K(ret), "req_is_valid", is_valid_rpc_req(), K(request), K_(rpc_trace_id));
  } else {
    timeout_us_ = request->get_rpc_timeout(); //timeout_ is in us
    if (0 == timeout_us_) { // timeout_us_ is 0 get from user's request, need update it to a default value
      timeout_us_ = rpc_req_->get_rpc_request_config_info().rpc_request_timeout_; //5s as default
    }
    timeout_us_ += rpc_req_->get_rpc_request_config_info().rpc_request_timeout_delta_;
    if (0 == rpc_req_->get_client_net_timeout_us()) {
      rpc_req_->set_client_net_timeout_us(common::ObTimeUtility::current_time() + timeout_us_);
    }
    if (0 == rpc_req_->get_server_net_timeout_us()) {
      rpc_req_->set_server_net_timeout_us(common::ObTimeUtility::current_time() + timeout_us_);
    }
  }

  return ret;
}

int ObRpcRequestSM::schedule_timeout_action()
{
  int ret = OB_SUCCESS;

  if (is_inner_request()) {
    // do nothing, inner request need not to schedule timeout action
  } else if (OB_FAIL(calc_rpc_timeout_us())) {
    LOG_WDIAG("fail to calc_rpc_timeout_us", K(ret), K_(rpc_trace_id));
  } else if (OB_UNLIKELY(NULL != timeout_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("timeout_action must be NULL here", K_(timeout_action), K(ret), K_(rpc_trace_id));
  } else {
    int64_t timeout_ns = HRTIME_USECONDS(timeout_us_);
    LOG_DEBUG("schedule_timeout_action get timeout", K_(timeout_us), K_(rpc_trace_id));

    if (OB_ISNULL(timeout_action_ = self_ethread().schedule_in(this, timeout_ns))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("fail to schedule timeout", K(timeout_action_), K(ret), K_(rpc_trace_id));
    }
  }

  return ret;
}

int ObRpcRequestSM::schedule_call_next_action(enum ObRpcRequestSMActionType next_action, const ObHRTime t)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("schedule_call_next_action get next", K(next_action));

  if (OB_UNLIKELY(NULL != sm_next_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("call_next_action must be NULL here", K_(sm_next_action), K(ret), K_(rpc_trace_id),
              "sm current next_action", next_action_, "next_action to be set", next_action);
  } else {
    // set_next
    set_next_state(next_action);

    if (0 == t) {
      if (OB_ISNULL(sm_next_action_ = self_ethread().schedule_imm(this, RPC_REQUEST_SM_CALL_NEXT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("fail to call next action imm", K_(sm_next_action), K(ret), K_(rpc_trace_id));
      }
    } else {
      if (OB_ISNULL(sm_next_action_ = self_ethread().schedule_in(this, t, RPC_REQUEST_SM_CALL_NEXT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("fail to call next action in", K_(sm_next_action), K(ret), K_(rpc_trace_id), K(t));
      }
    }
  }

  return ret;
}

int ObRpcRequestSM::cancel_call_next_action()
{
  int ret = common::OB_SUCCESS;
  if (NULL != sm_next_action_) {
    if (OB_FAIL(sm_next_action_->cancel())) {
      PROXY_LOG(WDIAG, "fail to cancel call next action", K_(sm_next_action), K(ret), K_(rpc_trace_id));
    } else {
      sm_next_action_ = NULL;
    }
  }
  return ret;
}

int ObRpcRequestSM::cancel_pending_action()
{
  int ret = common::OB_SUCCESS;
  if (NULL != pending_action_) {
    if (OB_FAIL(pending_action_->cancel())) {
      PROXY_LOG(WDIAG, "fail to cancel pending action", K_(pending_action), K(ret), K_(rpc_trace_id));
    } else {
      pending_action_ = NULL;
    }
  }
  return ret;
}

int ObRpcRequestSM::cancel_timeout_action()
{
  int ret = common::OB_SUCCESS;
  if (NULL != timeout_action_) {
    if (OB_FAIL(timeout_action_->cancel())) {
      PROXY_LOG(WDIAG, "fail to cancel timeout action", K_(timeout_action), K(ret), K_(rpc_trace_id));
    } else {
      timeout_action_ = NULL;
    }
  }
  return ret;
}

int ObRpcRequestSM::cancel_sharding_action()
{
  int ret = common::OB_SUCCESS;
  optimizer::ObProxyShardRpcReqRequestPlan* plan = NULL;
  engine::ObProxyRpcReqOperator* operator_root = NULL;
  if (NULL != sharding_action_) {
    if (OB_ISNULL(plan = reinterpret_cast<optimizer::ObProxyShardRpcReqRequestPlan *>(rpc_req_->execute_plan_))) {
      LOG_WDIAG("select log plan should not be null", K_(sm_id), K(ret), K_(rpc_trace_id));
    } else if (OB_ISNULL(operator_root = plan->get_plan_root())) {
      LOG_WDIAG("operator should not be null", K_(sm_id), K(ret), K_(rpc_trace_id));
    } else {
      operator_root->close();
      sharding_action_ = NULL;
    }
  }
  return ret;
}

int ObRpcRequestSM::main_handler(int event, void *data)
{
  if (OB_UNLIKELY(RPC_REQUEST_HANDLE_SM_MAGIC_ALIVE != magic_)
      || OB_UNLIKELY(reentrancy_count_ < 0)) {
    LOG_EDIAG("invalid sm magic or reentrancy_count", K(this), K_(magic), K_(reentrancy_count), K_(sm_id), K(event), K_(rpc_trace_id), "ethread", this_ethread());
  } else {
    ++reentrancy_count_;

    // Don't use the state enter macro since it uses history
    // space that we don't care about
    LOG_DEBUG("[ObRpcRequestSM::main_handler]",
            K_(sm_id), K(event), "event", ObRpcReqDebugNames::get_event_name(static_cast<enum ObRpcRequestSMActionType>(event)),
            K(this), "ethread", this_ethread(), K_(rpc_trace_id));

    if (EVENT_INTERVAL == event) {
      set_state_and_call_next(RPC_REQ_REQUEST_TIMEOUT);
    } else if (RPC_REQUEST_SM_DONE == event) {
      set_state_and_call_next(RPC_REQ_REQUEST_DONE);
    } else if (RPC_REQUEST_SM_CALL_NEXT == event) {
    // TODO : 跨模块以及线程调用，不再使用RPC_REQUEST_SM_CALL_NEXT事件，新增状态表示
      sm_next_action_ = NULL;
      call_next_action();
    } else if (RPC_REQUEST_SM_CLEANUP == event) {
      if (is_inner_request() && this_ethread() == execute_thread_) {
        MUTEX_LOCK(lock, inner_request_cleanup_mutex_, this_ethread());
        set_state_and_call_next(RPC_REQ_REQUEST_INNER_REQUEST_CLEANUP);
      } else {
        set_state_and_call_next(RPC_REQ_REQUEST_CLEANUP);
      }
    } else {
      if (OB_ISNULL(default_handler_)) {
        LOG_EDIAG("invalid internal state, default handler is NULL", K_(sm_id), K_(rpc_trace_id));
      } else {
        (this->*default_handler_)(event, data);
      }
    }

    // The sub-handler signals when it is time for the state machine
    // to exit. We can only exit if we are not reentrantly called
    // otherwise when the our call unwinds, we will be
    // running on a dead state machine
    //
    // Because of the need for an api shutdown hook, kill_this()
    // is also reentrant. As such, we don't want to decrement the
    // reentrancy count until after we run kill_this()
    if (terminate_sm_ && 1 == reentrancy_count_) {
      LOG_DEBUG("ObRpcRequestSM call kill_this in main_handler", K_(rpc_trace_id));
      kill_this();
    } else {
      --reentrancy_count_;
      if (OB_UNLIKELY(reentrancy_count_ < 0)) {
        LOG_EDIAG("invalid reentrancy_count", K_(reentrancy_count), K_(sm_id), K_(rpc_trace_id));
      }
    }
  }

  return VC_EVENT_CONT;
}

inline ObHRTime ObRpcRequestSM::get_based_hrtime()
{
  ObHRTime time = 0;
  if ((NULL != mysql_config_params_) && mysql_config_params_->enable_strict_stat_time_) {
    time = get_hrtime_internal();
  } else {
    time = event::get_hrtime();
  }
  return time;
}

void ObRpcRequestSM::dump_history_state()
{
  int64_t hist_size = history_pos_;

  LOG_EDIAG("------- begin mysql state dump -------", K_(sm_id), K_(rpc_trace_id));
  if (history_pos_ > HISTORY_SIZE) {
    hist_size = HISTORY_SIZE;
    LOG_EDIAG("   History Wrap around", K_(history_pos), K_(rpc_trace_id));
  }

  // Loop through the history and dump it
  for (int64_t i = 0; i < hist_size; ++i) {
    _LOG_EDIAG("%d   %d   %s", history_[i].event_, history_[i].reentrancy_, history_[i].file_line_);
  }
}

inline void ObRpcRequestSM::add_history_entry(const char *fileline, const int event, const int32_t reentrant)
{
  int64_t pos = history_pos_++ % HISTORY_SIZE;

  history_[pos].file_line_ = fileline;
  history_[pos].event_ = event;
  history_[pos].reentrancy_ = reentrant;
}

void ObRpcRequestSM::refresh_mysql_config()
{
  if (OB_UNLIKELY(mysql_config_params_ != get_global_mysql_config_processor().get_config())) {
    ObMysqlConfigParams *config = NULL;
    if (OB_ISNULL(config = get_global_mysql_config_processor().acquire())) {
      PROXY_TXN_LOG(WDIAG, "failed to acquire mysql config", K_(rpc_trace_id));
    } else {
      if (OB_LIKELY(NULL != mysql_config_params_)) {
        mysql_config_params_->dec_ref();
        mysql_config_params_ = NULL;
      }
      mysql_config_params_ = config;
    }
  }
}

bool ObRpcRequestSM::is_inner_request() const
{
  bool ret = false;

  if (OB_NOT_NULL(rpc_req_)) {
    ret = rpc_req_->is_inner_request();
  }

  return ret;
}

inline bool ObRpcRequestSM::is_valid_rpc_req() const
{
  return OB_NOT_NULL(rpc_req_)
      && rpc_req_->is_valid()
      && rpc_req_origin_channel_id_ == rpc_req_->get_origin_channel_id();
}

bool ObRpcRequestSM::is_need_convert_vip_to_tname() const
{
  return (get_global_proxy_config().need_convert_vip_to_tname && !is_inner_request());
}

// chect weahter client connections reach throttle
inline bool ObRpcRequestSM::check_connection_throttle()
{
  bool throttle = false;
  bool enable_client_connection_lru_disconnect = mysql_config_params_->enable_client_connection_lru_disconnect_;
  if (!enable_client_connection_lru_disconnect) {
    int64_t currently_open = 0;
    NET_READ_GLOBAL_DYN_SUM(NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN, currently_open);
    int64_t max_client_connection = mysql_config_params_->client_max_connections_;
    if (max_client_connection > 0) { // if 0 == client_max connections means no limit
      throttle = (max_client_connection < currently_open);
      if (throttle) {
        LOG_WDIAG("client connections reach throttle",
                K(currently_open), K(max_client_connection),
                K(throttle), K(enable_client_connection_lru_disconnect), K_(sm_id), K_(rpc_trace_id));
      }
    }
  }
  return throttle;
}

int ObRpcRequestSM::analyze_rpc_login_request(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReqAnalyzeNewStatus &status)
{
  int ret = OB_SUCCESS;

  ObRpcTableLoginRequest *orig_auth_req = NULL;
  ObRpcReqCtx *prpc_ctx = NULL;
  ObRpcClientNetHandler *client_net_handler = NULL;

  if (!is_valid_rpc_req()
      || OB_ISNULL(prpc_ctx = rpc_req_->get_rpc_ctx())
      || OB_ISNULL(orig_auth_req = dynamic_cast<ObRpcTableLoginRequest *>(rpc_req_->get_rpc_request()))
      || OB_ISNULL(client_net_handler = rpc_req_->get_cnet_sm())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc req is invalid", K(ret), "req_is_valid", is_valid_rpc_req(), KP(prpc_ctx), KP(client_net_handler), K_(rpc_trace_id));
  } else if (check_connection_throttle()) {
    ret = OB_ERR_TOO_MANY_SESSIONS;
    rpc_req_->set_rpc_req_error_code(ret);
    rpc_req_->set_need_terminal_client_net(true);
    LOG_WDIAG("failed to check_connection_throttle", K(ret), K_(rpc_trace_id));
  } else {
    ObRpcReqCtx &rpc_ctx = *prpc_ctx;
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    const ObString &full_name = orig_auth_req->get_user_name();
    
    char separator = '\0'; //First just support standard format
    if (OB_FAIL(ObProxyRpcReqAnalyzer::do_parse_full_user_name(rpc_ctx, full_name, separator, ctx))) {
      LOG_WDIAG("client rpc parse full user name failed", K(ret), "full_username", full_name, K_(rpc_trace_id));
    } else {
      // init client session info
      client_net_handler->set_has_tenant_username(ctx.has_tenant_username_);
      client_net_handler->set_has_cluster_username(ctx.has_cluster_username_);
      // init login reqeust meta
      obkv_info.user_name_ = rpc_ctx.user_name_;
      obkv_info.tenant_name_ = rpc_ctx.tenant_name_;
      obkv_info.cluster_name_ = rpc_ctx.cluster_name_;
      MEMCPY(rpc_ctx.schema_name_buf_, orig_auth_req->get_database_name().ptr(), orig_auth_req->get_database_name().length());
      rpc_ctx.database_name_.assign_ptr(rpc_ctx.schema_name_buf_, orig_auth_req->get_database_name().length());
      obkv_info.database_name_ = rpc_ctx.database_name_;
      rpc_route_mode_ = RPC_ROUTE_NORMAL;
      refresh_rpc_request_config(); //login need load config at here when full parser user's name
      LOG_DEBUG("rpc login request", K(rpc_ctx), K_(rpc_trace_id));

      if (OB_FAIL(check_user_identity(rpc_ctx.user_name_, rpc_ctx.tenant_name_,
                        rpc_ctx.cluster_name_, ctx.has_tenant_username_, ctx.has_cluster_username_))) {
        LOG_WDIAG("fail to check user identity", K_(sm_id), K(ret), K_(rpc_trace_id));
        ret = OB_PASSWORD_WRONG;
      } else {
        // do nothing
      }

      if (OB_SUCC(ret)) {
        LOG_INFO("client login audit", "client_addr", obkv_info.client_info_.addr_, K(rpc_ctx.cluster_name_),
                  K(rpc_ctx.tenant_name_), K(rpc_ctx.user_name_),
                  "status", ret == OB_SUCCESS && status != RPC_ANALYZE_NEW_ERROR ? "success" : "failed", K_(rpc_trace_id));
      }
    }
  }
  return ret;
}

int ObRpcRequestSM::init_rpc_request_content(ObProxyRpcReqAnalyzeCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObRpcClientNetHandler *client_net_handler = NULL;

  if (!is_valid_rpc_req() || OB_ISNULL(client_net_handler = rpc_req_->get_cnet_sm())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc req is invalid", K(ret), "req_is_valid", is_valid_rpc_req(), K_(rpc_trace_id));
  } else {
    if (client_net_handler->is_need_convert_vip_to_tname() && client_net_handler->is_vip_lookup_success()) {
      ctx.vip_tenant_name_ = client_net_handler->get_vip_tenant_name();
      ctx.vip_cluster_name_ = client_net_handler->get_vip_cluster_name();
      LOG_DEBUG("init_rpc_request_content get vip", K_(ctx.vip_tenant_name), K_(ctx.vip_cluster_name), K_(rpc_trace_id));
    } else {
      ctx.vip_tenant_name_.assign_ptr(OB_SYS_TENANT_NAME,
                                      static_cast<int32_t>(STRLEN(OB_SYS_TENANT_NAME)));
      if (OB_FAIL(get_global_resource_pool_processor().get_first_cluster_name(ctx.vip_cluster_name_))) {
        LOG_WDIAG("fail to get first cluster name", K_(sm_id), K(ret), K_(rpc_trace_id));
      }
    }

    if (OB_SUCC(ret)) {
      ctx.cluster_version_ = rpc_req_->get_cluster_version();
    }
  }
  return ret;
}

bool ObRpcRequestSM::need_reject_user_login(const ObString &user, const ObString &tenant,
                                       const bool has_tenant_username, const bool has_cluster_username,
                                       const bool is_cloud_user) const
{
  // 以下几种场景需要拒绝用户连接
  // proxysys 为proxy内部租户，不会访问 ob
  // 1. 外部用户使用 proxyro@sys
  // 2. 云上用户带有 tenant 或者 cluster
  // 3. 非云用户没有带 cluster
  bool bret = false;
  bool enable_full_username = false;
  LOG_DEBUG("check need_reject_user_login", K(user), K(tenant), K(has_tenant_username), K(has_cluster_username), K(is_cloud_user));
  if (is_cloud_user) {
    enable_full_username = get_global_proxy_config().enable_cloud_full_username;
    if (OB_NOT_NULL(rpc_req_)) {
      enable_full_username = rpc_req_->get_rpc_request_config_info().enable_cloud_full_username_;
    }
  } else {
    enable_full_username = get_global_proxy_config().enable_full_username;
  }
  if (tenant.case_compare(OB_PROXYSYS_TENANT_NAME) != 0) {
    if (!get_global_proxy_config().skip_proxyro_check &&
        tenant.case_compare(OB_SYS_TENANT_NAME) == 0 &&
        user.case_compare(ObProxyTableInfo::READ_ONLY_USERNAME_USER) == 0) {
      bret = true;
      // sys user, and not skip_proxyro_check
    } else if (is_cloud_user 
               && !enable_full_username 
               && (has_tenant_username || has_cluster_username)) {
      bret = true;
      // cloud user, and contains tenant_username and cluster_username
    } else if (!is_cloud_user
               && enable_full_username
               && (!has_cluster_username || !has_tenant_username)) {
      bret = true;
      // normal proxy user(not cloud user), and contains tenant_username and cluster_username 
    } else {
      // do nothing
    }
  }

  return bret;
}

int ObRpcRequestSM::check_user_identity(const ObString &user_name,
                                        const ObString &tenant_name,
                                        const ObString &cluster_name,
                                        const bool has_tenant_username,
                                        const bool has_cluster_username)
{
  int ret = OB_SUCCESS;

  const bool is_current_cloud_user = is_need_convert_vip_to_tname(); //is_cloud_user();
  if (need_reject_user_login(user_name, tenant_name, has_tenant_username, has_cluster_username, is_current_cloud_user)) {
    ret = OB_USER_NOT_EXIST;
    LOG_WDIAG("access denied for this user", K(is_current_cloud_user), K(ret), K(user_name), K(tenant_name), K(cluster_name), K_(rpc_trace_id));
  } else if (tenant_name == OB_PROXYSYS_TENANT_NAME) {
  //1. "root@proxysys" user has super privilege, and it can only use proxy internal cmd without connect to observer
    if (user_name == OB_PROXYSYS_USER_NAME) {
      ret = OB_USER_NOT_EXIST;
      LOG_EDIAG("check_user_identity failed, invalid user to handle", K(ret), K(user_name), K(tenant_name), K(cluster_name), K_(rpc_trace_id));
    } else if (user_name == OB_INSPECTOR_USER_NAME) {
      //RPC not to be here
      ret = OB_USER_NOT_EXIST;
      LOG_EDIAG("check_user_identity failed, invalid user to handle", K(ret), K(user_name), K(tenant_name), K(cluster_name), K_(rpc_trace_id));
    }

  //2."root@sys" user has super privilege, and it can use proxy internal cmd
  } else if (tenant_name == OB_SYS_TENANT_NAME && user_name == OB_SYS_USER_NAME) {
    // client_session_->set_user_identity(USER_TYPE_ROOTSYS);
    _LOG_DEBUG("This is %s@%s user, enable use proxy internal cmd", OB_SYS_USER_NAME, OB_SYS_TENANT_NAME);

  //3."proxyro@sys" user has will disconect when is checking proxy
  } else if (tenant_name == OB_SYS_TENANT_NAME && user_name == ObProxyTableInfo::READ_ONLY_USERNAME_USER) {
    // client_session_->set_user_identity(USER_TYPE_PROXYRO);
    _LOG_DEBUG("This is %s user", ObProxyTableInfo::READ_ONLY_USERNAME);

  //4. ConfigServer user is proxy internal user, and it can use proxy internal cmd
  // ConfigServer username format: "user@tenant" or "user"
  } else if (get_global_proxy_config().is_metadb_used() && cluster_name == OB_META_DB_CLUSTER_NAME) {
    // ObProxyConfigString cfg_full_user_name;
    // if (OB_FAIL(get_global_config_server_processor().get_proxy_meta_table_username(cfg_full_user_name))) {
    //   LOG_WDIAG("fail to get meta table info", K_(sm_id), K(ret));
    // } else if (rpc_auth_req_.user_tenant_name_ == cfg_full_user_name
    //            || (user_name == cfg_full_user_name && tenant_name == OB_SYS_TENANT_NAME)) {
    //   client_session_->set_user_identity(USER_TYPE_METADB);
    //   LOG_DEBUG("This is metadb user, enable use proxy internal cmd");
    // }
    LOG_DEBUG("This is metadb user, enable use proxy internal cmd", K(user_name), K(tenant_name), K(cluster_name), K_(rpc_trace_id));
  }

  return ret;
}

int ObRpcRequestSM::analyze_request(ObRpcReqAnalyzeNewStatus &status)
{
  int ret = OB_SUCCESS;
  ObProxyRpcReqAnalyzeCtx ctx;

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.analyze_request_begin_ = get_based_hrtime();
  }

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc req is invalid", K(ret), "req_is_valid", is_valid_rpc_req(), K_(rpc_trace_id));
  } else if (OB_FAIL(init_rpc_request_content(ctx))) {
    LOG_WDIAG("fail to call init_rpc_request_content", K(ret), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    LOG_DEBUG("ObRpcRequestSM::analyze_request before", "partition_id", obkv_info.partition_id_, "cluster_version", ctx.cluster_version_, K_(rpc_trace_id));

    if (OB_FAIL(ObProxyRpcReqAnalyzer::analyze_rpc_req(ctx, status, *rpc_req_))) {
      LOG_WDIAG("fail to call analyze_rpc_req", K(ret), K(status), K_(rpc_trace_id));
    }

    if (OB_LIKELY(RPC_ANALYZE_NEW_DONE == status)) {
      // 这里主要init login请求的状态，其他请求在init_request_meta_info中处理
      if (obkv_info.is_auth()) {
        // 1. init rpc req ctx
        // 2. analyze login request, init rpc req ctx
        if (OB_FAIL(ObRpcReqCtx::alloc_and_init_rpc_ctx(obkv_info.rpc_ctx_))) {
          LOG_WDIAG("fail to alloc_new_rpc_req_ctx", K(ret), K_(rpc_trace_id));
        } else if (OB_FAIL(analyze_rpc_login_request(ctx, status))) {
          LOG_WDIAG("fail to analyze login request", K(ret), K_(rpc_trace_id));
        } else {
          // success
        }
      } else {
        // do nothing
      }
    } else {
      // do nothing
    }
  }

  if (OB_FAIL(ret)) {
    status = RPC_ANALYZE_NEW_ERROR;
  }

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.analyze_request_end_ = get_based_hrtime();
  }
  cmd_time_stats_.client_request_analyze_time_ +=
    milestone_diff_new(milestones_.analyze_request_begin_, milestones_.analyze_request_end_);

  return ret;
}

// Initialize meta information except login request
int ObRpcRequestSM::init_request_meta_info()
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *prpc_ctx = NULL;
  if (!is_valid_rpc_req() || OB_ISNULL(prpc_ctx = rpc_req_->get_rpc_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid rpc req to init meta info", K(ret), KPC_(rpc_req), KP(prpc_ctx), K_(rpc_trace_id));
  } else if (prpc_ctx->tenant_name_.empty() || prpc_ctx->cluster_name_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_DEBUG("not have any username or tenant_name in rpc ctx", K(ret), KPC(prpc_ctx), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    ObRpcReqCtx &rpc_ctx = *prpc_ctx;

    obkv_info.user_name_ = rpc_ctx.user_name_;
    obkv_info.tenant_name_ = rpc_ctx.tenant_name_;
    obkv_info.cluster_name_ = rpc_ctx.cluster_name_;
    obkv_info.database_name_ = rpc_ctx.database_name_;

    // 弱读场景下, 回退到常规路由模式, 强读场景下，默认走RPC_ROUTE_FIND_LEADER, 直接找到leader发送
    if (obkv_info.is_read_weak()) {
      rpc_route_mode_ = RPC_ROUTE_NORMAL;
    }

    if (OB_FAIL(get_cached_config_info())) {
      LOG_WDIAG("fail to call get_cached_config_info", K(ret), KPC_(rpc_req), K_(rpc_trace_id));
    } else{
      // check and refresh config
      refresh_rpc_request_config();

      PROXY_CS_LOG(DEBUG, "init rpc request meta info base on rpc ctx", K(ret), KPC_(rpc_req),
                  "database_name", obkv_info.database_name_,
                  "user_name", obkv_info.user_name_,
                  "tenant_name", obkv_info.tenant_name_,
                  "cluster_name", obkv_info.cluster_name_,
                  "partition_id", obkv_info.get_partition_id(),
                  K_(rpc_trace_id)
                  );
    }
  }
  return ret;
}

int ObRpcRequestSM::setup_process_request()
{
  int ret = OB_SUCCESS;
  bool is_canceled = false;

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::setup_process_request got a NULL rpc req", K(ret), K(rpc_req_), K_(rpc_trace_id));
  } else if ((is_canceled = rpc_req_->canceled())) {
    LOG_WDIAG("ObRpcRequestSM::setup_process_request get a canceled rpc_req", K_(rpc_req), K_(rpc_trace_id));
  } else {
    RPC_REQ_SM_ENTER_STATE(ObRpcReq::RpcReqSmState::RPC_REQ_SM_REQUEST_ANALYZE);

    refresh_mysql_config();

    if (OB_FAIL(process_request(rpc_req_))) {
      LOG_WDIAG("fail to call process_request", K(ret), K_(rpc_trace_id));
    }
  }

  if (OB_SUCC(ret)) {
    if (!is_canceled) {
      need_pl_lookup_ = true;
      if (rpc_req_->obkv_info_.is_auth()) {
        set_state_and_call_next(RPC_REQ_IN_CLUSTER_BUILD);
      } else {
        set_state_and_call_next(RPC_REQ_CTX_LOOKUP);
      }
    }
  } else {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::process_request(ObRpcReq *req)
{
  int ret = OB_SUCCESS;
  ObRpcReqAnalyzeNewStatus status = RPC_ANALYZE_NEW_CONT;

  if (OB_ISNULL(req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("process_request get invalid argument", K(req), K(ret), K_(rpc_trace_id));
  } else {
    if (OB_FAIL(analyze_request(status))) {
      LOG_WDIAG("fail to call analyze_request", K(ret), K(status), K(req), K_(rpc_trace_id));
    } else {
      cmd_size_stats_.client_request_bytes_ += req->get_request_buf_len();

      switch (__builtin_expect(status, RPC_ANALYZE_NEW_DONE)) {
      case RPC_ANALYZE_NEW_OBPARSE_ERROR:
        ret = OB_ERR_UNEXPECTED;
        //must read all data of the request, otherwise will read the remain request data when recv next request
        // TODO: 返回error
        break;
      case RPC_ANALYZE_NEW_ERROR:
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("error parsing client request", K(ret), K_(sm_id), K_(rpc_trace_id));
        // set_client_abort(ObRpcRequestSM::ABORTED, event);
        break;
      case RPC_ANALYZE_NEW_CONT:
        ret = OB_ERR_UNEXPECTED;
        //TODO RPC SERVICE not all request has bean read, need read it next
        break;
      case RPC_ANALYZE_NEW_DONE: {
        LOG_DEBUG("[RPC_REQUEST] analyze rpc_req request done", K(ret), KPC(req), K_(rpc_trace_id));
        if (OB_FAIL(schedule_timeout_action())) {
          LOG_WDIAG("fail to call schedule_timeout_action", K(ret), KPC(req), K_(rpc_trace_id));
        }
        break;
      }
      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_EDIAG("unknown analyze rpc request status", K(status), K_(sm_id), K(ret), K_(rpc_trace_id));
        break;
      }
    }
  }
  return ret;
}

int ObRpcRequestSM::setup_rpc_req_ctx_lookup()
{
  int ret = OB_SUCCESS;
  ObAction *rpc_ctx_lookup_action = NULL;
  ObRpcReqCtxParam rpc_ctx_param;

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.ctx_lookup_begin_ = get_based_hrtime();
    milestones_.ctx_lookup_end_ = 0;
  }

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("setup_rpc_req_ctx_lookup get a invalid rpc_req", K(ret), KP_(rpc_req), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    if (obkv_info.is_inner_request_ || obkv_info.is_auth()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("inner or auth request cannot enter setup_rpc_req_ctx_lookup", K(ret), KP_(rpc_req), K_(rpc_trace_id));
    } else {
      rpc_ctx_param.key_ = obkv_info.credential_;
      rpc_ctx_param.cont_ = this;

      LOG_DEBUG("rpc_ctx_param build done", K(rpc_ctx_param), K_(rpc_trace_id));

      RPC_REQUEST_SM_SET_DEFAULT_HANDLER(&ObRpcRequestSM::state_rpc_req_ctx_lookup);
      if (OB_FAIL(ObRpcReqCtxProcessor::get_rpc_req_ctx(rpc_ctx_param, rpc_ctx_lookup_action))) {
        LOG_WDIAG("fail to call ObRpcReqCtxProcessor::get_rpc_req_ctx", K(ret), K_(rpc_trace_id));
      } else {
        if (OB_NOT_NULL(rpc_ctx_lookup_action)) {
          pending_action_ = rpc_ctx_lookup_action;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL == pending_action_) {
      handle_event(RPC_REQ_CTX_LOOKUP_CACHE_DONE, &rpc_ctx_param.result_);
      rpc_ctx_param.result_.reset();
    } else {
      // do nothing, wait pending action
      LOG_DEBUG("ObRpcRequestSM::setup_rpc_req_ctx_lookup wait callback", KP_(pending_action), K_(rpc_trace_id));
    }
  } else {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::state_rpc_req_ctx_lookup(int event, void *data)
{
  int ret = OB_SUCCESS;
  pending_action_ = NULL;

  LOG_DEBUG("ObRpcRequestSM::state_rpc_req_ctx_lookup", K(event), KP(data), KP_(pending_action), K_(rpc_trace_id));

  if (OB_UNLIKELY(RPC_REQ_CTX_LOOKUP_CACHE_DONE != event) || OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected event type, it should not happen", K(event), K(data), K(ret), K_(rpc_trace_id));
  } else if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("state_rpc_req_ctx_lookup get a invalid rpc_req", K(ret), KP_(rpc_req), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    ObRpcReqCtxResult *result = reinterpret_cast<ObRpcReqCtxResult *>(data);
    // hand over the ref
    if (OB_NOT_NULL(obkv_info.rpc_ctx_)) {
      obkv_info.rpc_ctx_->dec_ref();
      obkv_info.rpc_ctx_ = NULL;
    }
    obkv_info.rpc_ctx_ = result->target_ctx_;
    result->target_ctx_ = NULL;

    if (OB_ISNULL(obkv_info.rpc_ctx_)) {
      ret = OB_KV_CREDENTIAL_NOT_MATCH;
      LOG_EDIAG("odp can not find rpc_ctx, maybe invalid credential, return error and need terminal net", K(obkv_info.credential_), K(ret));
      rpc_req_->set_need_terminal_client_net(true);
    } else {
      LOG_DEBUG("ObRpcRequestSM get rpc ctx succ", "rpc_ctx", *obkv_info.rpc_ctx_, K_(rpc_trace_id));
      obkv_info.rpc_ctx_->renew_last_access_time();
      if (OB_FAIL(init_request_meta_info())) {
        LOG_WDIAG("fail to init_request_meta_info", K_(sm_id), K(ret), K_(rpc_trace_id));
      }
    }
  }

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.ctx_lookup_end_ = get_based_hrtime();
    cmd_time_stats_.rpc_ctx_lookup_time_ += milestone_diff_new(milestones_.ctx_lookup_begin_, milestones_.ctx_lookup_end_);
  }

  if (OB_SUCC(ret)) {
    set_state_and_call_next(RPC_REQ_IN_CLUSTER_BUILD);
  } else {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return EVENT_DONE;

}

int ObRpcRequestSM::set_cluster_info(const bool enable_cluster_checkout,
    const ObString &cluster_name, const obutils::ObProxyConfigString &real_meta_cluster_name,
    const int64_t cluster_id, bool &need_delete_cluster)
{
  int ret = OB_SUCCESS;
  cluster_id_ = cluster_id;
  if (cluster_name == OB_META_DB_CLUSTER_NAME) {
    free_real_meta_cluster_name();
    if (OB_UNLIKELY(real_meta_cluster_name.empty())) {
      if (OB_LIKELY(enable_cluster_checkout)) {
        // we need check delete cluster when the follow happened:
        // 1. this is OB_META_DB_CLUSTER_NAME
        // 2. enable cluster checkout
        // 3. real meta cluster do not exist
        need_delete_cluster = true;
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("real meta cluster name is empty, it should not happened, proxy need rebuild meta cluster", K(ret), K_(rpc_trace_id));
      }
    } else {
      if (OB_ISNULL(real_meta_cluster_name_str_ = static_cast<char *>(op_fixed_mem_alloc(real_meta_cluster_name.length())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem for real_meta_cluster_name", "size", real_meta_cluster_name.length(), K(ret), K_(rpc_trace_id));
      } else {
        MEMCPY(real_meta_cluster_name_str_, real_meta_cluster_name.ptr(), real_meta_cluster_name.length());
        real_meta_cluster_name_.assign_ptr(real_meta_cluster_name_str_, real_meta_cluster_name.length());
      }
    }
  }
  return ret;
}

void ObRpcRequestSM::free_real_meta_cluster_name()
{
  if (NULL != real_meta_cluster_name_str_) {
    op_fixed_mem_free(real_meta_cluster_name_str_, real_meta_cluster_name_.length());
    real_meta_cluster_name_str_ = NULL;
  }
  real_meta_cluster_name_.reset();
}

int ObRpcRequestSM::process_cluster_resource(void *data)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("process cluster resource", K_(sm_id), K(data), K_(rpc_trace_id));
  ObRpcReqCtx *rpc_ctx = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(data), K_(sm_id), K(ret), K_(rpc_trace_id));
  } else if (!is_valid_rpc_req() || OB_ISNULL(rpc_ctx = rpc_req_->get_rpc_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc req is invalid", K(ret), "req_is_valid", is_valid_rpc_req(), KP_(rpc_req), K(rpc_ctx), K_(rpc_trace_id));
  } else if (rpc_req_->canceled()) {
    LOG_WDIAG("rpc client request has cleaned canceled it ", K_(rpc_req), KPC_(rpc_req), K_(rpc_trace_id));
  } else {
    ObClusterResource *new_cluster_resource = reinterpret_cast<ObClusterResource *>(data);
    if (new_cluster_resource->is_deleting()) { // maybe has already been deleted
      ret = OB_INNER_STAT_ERROR;
      LOG_WDIAG("this cluster resource has been deleted", K(ret), KPC(new_cluster_resource), K_(rpc_trace_id));
      new_cluster_resource->dec_ref();
      new_cluster_resource = NULL;
    } else {
      if (!is_inner_request()) {
        new_cluster_resource->renew_last_access_time();
      }
      if (OB_NOT_NULL(cluster_resource_)) {
        LOG_WDIAG("cluster resource must be NULl here, or will mem leak",
                 KPC(cluster_resource_), K_(rpc_trace_id));
        cluster_resource_->dec_ref();
        cluster_resource_ = NULL;
      }
      cluster_resource_ = new_cluster_resource;
      cluster_resource_->inc_ref();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(update_cached_cluster_resource())) {
        LOG_WDIAG("fail to call update_cached_cluster_resource", K_(sm_id), K(ret), K_(rpc_trace_id));
      }
    }
  }
  return ret;
}

int ObRpcRequestSM::get_cached_cluster_resource()
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *rpc_ctx = NULL;

  if (!is_valid_rpc_req() || OB_ISNULL(rpc_ctx = rpc_req_->get_rpc_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc req is invalid", K(ret), "req_is_valid", is_valid_rpc_req(), K(rpc_ctx), K_(rpc_trace_id));
  } else {
    if (rpc_ctx->is_cached_cluster_resource_avail_state()) {
      DRWLock &rwlock = rpc_ctx->get_rpc_ctx_lock();
      DRWLock::RDLockGuard rwlock_guard(rwlock);
      if (OB_ISNULL(rpc_ctx->cluster_resource_) || rpc_ctx->cluster_resource_->is_deleting()) {
        // set updating state , update cluster resource and renew rpc ctx cached cluster resource
        rpc_ctx->cas_set_cluster_resource_updating_state();
      }
      if (rpc_ctx->is_cached_cluster_resource_avail_state()) {
        cluster_resource_ = rpc_ctx->cluster_resource_;
        cluster_resource_->inc_ref();
        if (!is_inner_request()) {
          cluster_resource_->renew_last_access_time();
        }
      }
    }

    LOG_DEBUG("rpc get cached cluster resource done", KP_(cluster_resource), K_(rpc_trace_id));
  }

  return ret;
}

int ObRpcRequestSM::update_cached_cluster_resource()
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *rpc_ctx = NULL;

  if (!is_valid_rpc_req() || OB_ISNULL(rpc_ctx = rpc_req_->get_rpc_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc req is invalid", K(ret), "req_is_valid", is_valid_rpc_req(), K(rpc_ctx), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    LOG_DEBUG("rpc update cached cluster resource", "is_auth", obkv_info.is_auth(), K_(rpc_trace_id));

    if (obkv_info.is_auth()) {
      // login请求不会存在线程共享，直接更新
      rpc_ctx->cas_set_cluster_resource_avail_state();
      rpc_ctx->set_cluster_resource(cluster_resource_);
    } else {
      // 双检查锁，判断状态，获取写锁
      if (rpc_ctx->is_cached_cluster_resource_updating_state()) {
        DRWLock &rwlock = rpc_ctx->get_rpc_ctx_lock();
        DRWLock::WRLockGuard rwlock_guard(rwlock);
        if (rpc_ctx->is_cached_cluster_resource_updating_state()) {
          rpc_ctx->cas_set_cluster_resource_avail_state();
          rpc_ctx->set_cluster_resource(cluster_resource_);
        }
      }
    }
  }

  return ret;
}

int ObRpcRequestSM::setup_rpc_get_cluster()
{
  int ret = OB_SUCCESS;
  ObAction *cr_handler = NULL;
  ObRpcReqCtx *rpc_ctx = NULL;
  ObRpcClientNetHandler *client_net_handler = NULL;

  if (!is_valid_rpc_req() || OB_ISNULL(rpc_ctx = rpc_req_->get_rpc_ctx())
    || OB_ISNULL(client_net_handler = rpc_req_->get_cnet_sm())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc req is invalid", K(ret), "req_is_valid", is_valid_rpc_req(), K(rpc_ctx), K_(rpc_trace_id));
  } else if (rpc_req_->canceled()) {
    LOG_WDIAG("ObRpcRequestSM::setup_rpc_get_cluster get a canceled rpc_req", K_(rpc_req), K_(rpc_trace_id));
  } else if (OB_NOT_NULL(cluster_resource_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("already get cluster_resource, unexpected", K(ret), KP(rpc_ctx), K_(rpc_trace_id));
  } else {
    if (OB_FAIL(get_cached_cluster_resource())) {
      LOG_WDIAG("fail to call get_cached_cluster_resource", K(ret), KP(rpc_ctx), K_(rpc_trace_id));
    }
    // need fetch from cache or remote
    if (OB_SUCC(ret) && OB_ISNULL(cluster_resource_)) {
      ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
      const ObString &cluster_name = obkv_info.cluster_name_;
      int64_t cluster_id = obkv_info.cluster_id_;

      RPC_REQ_SM_ENTER_STATE(ObRpcReq::RpcReqSmState::RPC_REQ_SM_CLUSTER_BUILD);

      // Attention! if login via vip and vip tenant cluster is valid, we think its cluster is not from default
      // and no need to tell whether it is multi clusters or not
      // and it is false if the it is the user session init from rpc_client(not need check it from rar.is_clustername_from_default_
      // maybe it is true, if the cluster info converted from vip.
      const bool is_clustername_from_default = ((is_need_convert_vip_to_tname()
                                                && client_net_handler->is_vip_lookup_success()) || obkv_info.is_inner_request_)
                                                ? false : rpc_ctx->is_clustername_from_default_;
      bool need_delete_cluster = false;
      ObProxyConfigString real_meta_cluster_name;
      ObConfigServerProcessor &csp = get_global_config_server_processor();
      if (OB_FAIL(csp.get_cluster_info(cluster_name, is_clustername_from_default,
                                        real_meta_cluster_name))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_CLUSTER_NOT_EXIST;
          LOG_WDIAG("cluster does not exist, this connection will disconnect",
                    K_(sm_id), K(is_clustername_from_default), K(cluster_name), K(ret), K_(rpc_trace_id));
        } else {
          LOG_WDIAG("fail to get cluster info, this connection will disconnect",
                    K_(sm_id), K(is_clustername_from_default), K(cluster_name), K(ret), K_(rpc_trace_id));
        }
      } else if (OB_FAIL(set_cluster_info(get_global_proxy_config().enable_cluster_checkout,
                                          cluster_name,
                                          real_meta_cluster_name,
                                          cluster_id,
                                          need_delete_cluster))) {
        LOG_WDIAG("fail to set cluster info, this connection will disconnect",
                  K_(sm_id), K(cluster_name), K(cluster_id), K(ret), K_(rpc_trace_id));
      } else {
        RPC_REQUEST_SM_SET_DEFAULT_HANDLER(&ObRpcRequestSM::state_rpc_get_cluster);
        milestones_.cluster_resource_create_begin_ = get_based_hrtime();

        //callback for cluster build // TODO merge add  connection_diagnosis_trace_
        ret = get_global_resource_pool_processor().get_cluster_resource(*this,
              (process_async_task_pfn)&ObRpcRequestSM::process_cluster_resource,
              is_inner_request(), cluster_name, cluster_id, connection_diagnosis_trace_, cr_handler);
        if (OB_FAIL(ret)) {
          LOG_WDIAG("cluster_resource_handler is ACTION_RESULT_NONE, something is wrong",
                    K_(sm_id), K(cluster_name), K(cluster_id), K(ret), K_(rpc_trace_id));
        } else if (OB_SUCC(ret) && (NULL != cr_handler)) {
          LOG_DEBUG("should create and init new_cluster_resource and assign pending action",
                    K_(sm_id), K(cluster_name), K(cluster_id), K_(rpc_trace_id));
          if (OB_UNLIKELY(NULL != pending_action_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("pending_action must be NULL here", K_(pending_action), K_(sm_id), K(ret), K_(rpc_trace_id));
            if (OB_NOT_NULL(rpc_req_)) {
              //init proxy error to rpc_req
              rpc_req_->set_rpc_req_error_code(ret);
            }
            set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
          } else {
            pending_action_ = cr_handler;
          }
        } else if (OB_SUCC(ret) && (NULL == cr_handler)) {
          // do nothing
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  } else if (NULL == pending_action_) {
    // 集群构建完毕
    set_state_and_call_next(RPC_REQ_INNER_INFO_GET);
  } else {
    // NULL != pending_action_, nothing, wait callback;
    LOG_DEBUG("ObRpcRequestSM::setup_rpc_get_cluster wait callback", KP_(pending_action), K_(rpc_trace_id));
  }

  return ret;
}

int ObRpcRequestSM::state_rpc_get_cluster(int event, void *data)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObRpcRequestSM::state_rpc_get_cluster", K(event), KP(data), KP_(pending_action), K_(rpc_trace_id));
  pending_action_ = NULL;

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc req is invalid", K(ret), "req_is_valid", is_valid_rpc_req(), KP_(rpc_req), K_(rpc_trace_id));
  } else if (rpc_req_->canceled()) {
    LOG_WDIAG("ObRpcRequestSM::state_rpc_get_cluster get a canceled rpc_req", K_(rpc_req), KPC_(rpc_req), K_(rpc_trace_id));
  } else {
    switch (event) {
      case CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT:
        if (OB_ISNULL(data)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WDIAG("data is NULL", K_(sm_id), K(ret), K_(rpc_trace_id));
        } else if (OB_FAIL(process_cluster_resource(data))) {
          LOG_WDIAG("fail to get cluster_resource, will disconnect", K_(sm_id), K(ret), K_(rpc_trace_id));
        } else {
          milestones_.cluster_resource_create_end_ = get_based_hrtime();
          cmd_time_stats_.cluster_resource_create_time_ =
            milestone_diff_new(milestones_.cluster_resource_create_begin_, milestones_.cluster_resource_create_end_);
          
          LOG_DEBUG("state_rpc_get_cluster build done", KP_(rpc_req), "cluster_version", cluster_resource_->cluster_version_, K_(rpc_trace_id));
        }
        break;
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("Unexpected event", K_(sm_id), K(event), K(ret), K_(rpc_trace_id));
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    set_state_and_call_next(RPC_REQ_INNER_INFO_GET);
  } else {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return VC_EVENT_NONE;
}

int ObRpcRequestSM::setup_req_inner_info_get()
{
  int ret = OB_SUCCESS;

  if (!is_valid_rpc_req() || rpc_req_->canceled()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc_req is invalid or is canceled", K(ret), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    bool need_get_async_info = obkv_info.is_async_query_request();
    bool need_get_index_entry = !obkv_info.is_hbase_request() && !obkv_info.is_inner_request_
        && obkv_info.is_query_with_index() && rpc_req_->get_rpc_request_config_info().rpc_enable_global_index_;
    // TODO : tablegroup sharding 跨分区 确定是否需要再获取entry
    bool need_get_tablegroup_entry = obkv_info.is_hbase_request() && obkv_info.is_hbase_empty_family();

    /**
     * @brief 
     *  1. If the server address has been set, jump directly to the ADDR_SEARCH state
     *  2. If it is a direct load, jump to the DIRECT_ROUTING state
     *  3. If it is a global index, jump to the INDEX_LOOKUP state
     *  4. If it is an Hbase request with an empty family, jump to the TABLEGROUP_LOOKUP state
     *  5. In other cases, jump to PARTITION_LOOKUP state
     */
    if (rpc_req_->is_server_addr_set()) {
      set_state_and_call_next(RPC_REQ_SERVER_ADDR_SEARCHED);
    } else if (obkv_info.is_direct_load_req() 
                 && !obkv_info.is_first_direct_load_request()) {
      //1.the first request for load task: need find addr by dummy_entry in tenant
      //2.the following request for load task: need find addr by request seted by client
      set_state_and_call_next(RPC_REQ_REQUEST_DIRECT_ROUTING);
    } else if (!already_get_async_info_ && need_get_async_info) {
      already_get_async_info_ = true;
      set_state_and_call_next(RPC_REQ_QUERY_ASYNC_INFO_GET);
    } else if (!already_get_index_entry_ && need_get_index_entry) {
      already_get_index_entry_ = true;
      set_state_and_call_next(RPC_REQ_IN_INDEX_LOOKUP);
    } else if (!already_get_tablegroup_entry_ && need_get_tablegroup_entry) {
      already_get_tablegroup_entry_ = true;
      set_state_and_call_next(RPC_REQ_IN_TABLEGROUP_LOOKUP);
    } else {
      set_state_and_call_next(RPC_REQ_IN_PARTITION_LOOKUP);
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }
  return ret;
}

int ObRpcRequestSM::setup_table_query_async_info_get()
{
  int ret = OB_SUCCESS;
  ObAction *query_entry_lookup_action_handle = NULL;
  ObTableQueryAsyncParam table_query_async_param;

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.query_async_lookup_begin_ = get_based_hrtime();
    milestones_.query_async_lookup_end_ = 0;
  }

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("setup_table_query_async_info_get get a invalid rpc_req", K(ret), KP_(rpc_req), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    if (!obkv_info.is_async_query_request()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ObRpcRequestSM::setup_table_query_async_info_get need OB_TABLE_API_EXECUTE_QUERY_SYNC", K(ret), K_(obkv_info.pcode), K_(rpc_trace_id));
    } else {
      LOG_DEBUG("ObRpcRequestSM::setup_table_query_async_info_get for OB_TABLE_API_EXECUTE_QUERY_SYNC", K_(rpc_trace_id));
      // For Async Query，Allocate or get ObTableQueryAsyncEntry
      ObRpcTableQuerySyncRequest *sync_query_request = NULL;
      if (OB_ISNULL(sync_query_request = dynamic_cast<ObRpcTableQuerySyncRequest *>(rpc_req_->get_rpc_request()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("pcode is OB_TABLE_API_EXECUTE_QUERY_SYNC, but can not cast to ObRpcTableQuerySyncRequest", K(ret), K(sync_query_request), K_(rpc_trace_id));
      } else {
        const ObTableQuerySyncRequest &sync_query = sync_query_request->get_query_request();

        if (!sync_query.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("ObTableQuerySyncRequest is invalid", K(ret), K(sync_query), K_(rpc_trace_id));
        } else {
          // need get ObTableQueryAsyncEntry
          table_query_async_param.cont_ = this;
          table_query_async_param.key_ = sync_query.query_session_id_;
          table_query_async_param.new_async_query_ = (sync_query.query_type_ == ObQueryOperationType::QUERY_START);

          LOG_DEBUG("table_query_async_param build done", K(table_query_async_param), K_(rpc_trace_id));

          RPC_REQUEST_SM_SET_DEFAULT_HANDLER(&ObRpcRequestSM::state_table_query_async_info_get);
          if (OB_FAIL(ObTableQueryAsyncProcessor::get_table_query_async_entry(table_query_async_param, query_entry_lookup_action_handle))) {
            LOG_WDIAG("fail to call ObTableQueryAsyncProcessor::get_table_query_async_entry", K(ret), K_(rpc_trace_id));
          } else {
            if (OB_NOT_NULL(query_entry_lookup_action_handle)) {
              pending_action_ = query_entry_lookup_action_handle;
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL == pending_action_) {
      handle_event(RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_DONE, &table_query_async_param.result_);
      table_query_async_param.result_.reset();
    } else {
      // do nothing, wait pending action
      LOG_DEBUG("ObRpcRequestSM::setup_table_query_async_info_get wait callback", KP_(pending_action), K_(rpc_trace_id));
    }
  } else {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::state_table_query_async_info_get(int event, void *data)
{
  LOG_DEBUG("ObRpcRequestSM::state_table_query_async_info_get", K(event), KP(data), KP_(pending_action), K_(rpc_trace_id));
  int ret = OB_SUCCESS;
  pending_action_ = NULL;

  if (OB_UNLIKELY(RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_DONE != event) || OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected event type, it should not happen", K(event), K(data), K(ret), K_(rpc_trace_id));
  } else if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("state_table_query_async_info_get get a invalid rpc_req", K(ret), KP_(rpc_req), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    ObTableQueryAsyncResult *result = reinterpret_cast<ObTableQueryAsyncResult *>(data);
    // hand over the ref
    if (OB_NOT_NULL(obkv_info.query_async_entry_)) {
      obkv_info.query_async_entry_->dec_ref();
      obkv_info.query_async_entry_ = NULL;
    }
    if (OB_ISNULL(result->target_entry_)) {
      ret = OB_ERR_UNKNOWN_SESSION_ID;
      LOG_WDIAG("ObRpcRequestSM::state_table_query_async_info_get fail to get query_async_entry", K(OB_ERR_UNKNOWN_SESSION_ID), KPC(result), K_(rpc_trace_id));
    } else {
      obkv_info.query_async_entry_ = result->target_entry_;
      result->target_entry_ = NULL;
      // update timeout
      if (OB_NOT_NULL(rpc_req_->get_rpc_request())) {
        obkv_info.query_async_entry_->update_timeout_ts(rpc_req_->get_rpc_request()->get_rpc_timeout());
        LOG_DEBUG("succ to update async entry timeout ts", "timeout_ts", obkv_info.query_async_entry_->get_timeout_ts(), K_(rpc_trace_id));
      }

      if (obkv_info.query_async_entry_->is_avail_state()) {
        obkv_info.table_id_ = obkv_info.query_async_entry_->get_table_id();
        if (obkv_info.query_async_entry_->is_partition_table()) {
          if (OB_FAIL(obkv_info.query_async_entry_->get_current_tablet_id(obkv_info.partition_id_))) {
            LOG_WDIAG("fail to get current tablet id", K(ret), KPC_(obkv_info.query_async_entry), K_(rpc_trace_id));
          }
        } else {
          obkv_info.partition_id_ = 0;
        }
        if (OB_SUCC(ret)) {
          LOG_DEBUG("get a query_async_entry for OB_TABLE_API_EXECUTE_QUERY_SYNC", KPC(obkv_info.query_async_entry_),
                      "tablet_id", obkv_info.partition_id_, "table_id", obkv_info.table_id_, K_(rpc_trace_id));
          if (obkv_info.query_async_entry_->is_server_info_set()) {
            LOG_DEBUG("not first tablet async query, use last addr", KPC(obkv_info.query_async_entry_), K_(rpc_trace_id));
            rpc_req_->get_server_addr().addr_ = obkv_info.query_async_entry_->get_server_info().addr_;
            rpc_req_->get_server_addr().sql_addr_ = obkv_info.query_async_entry_->get_server_info().sql_addr_;
            rpc_req_->set_server_addr_set(true);
          }
        }
      } else {
        obkv_info.query_async_entry_->set_avail_state();
      }
    }
  }

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.query_async_lookup_end_ = get_based_hrtime();
    cmd_time_stats_.query_async_lookup_time_ += milestone_diff_new(milestones_.query_async_lookup_begin_, milestones_.query_async_lookup_end_);
  }

  if (OB_SUCC(ret)) {
    set_state_and_call_next(RPC_REQ_INNER_INFO_GET);
  } else {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return EVENT_DONE;
}

int ObRpcRequestSM::setup_rpc_tablegroup_lookup()
{
  int ret = OB_SUCCESS;
  ObAction *tablegroup_lookup_action_handle = NULL;
  ObTableGroupParam tablegroup_param;

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.table_group_lookup_begin_ = get_based_hrtime();
    milestones_.table_group_lookup_end_ = 0;
  }

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("state_rpc_tablegroup_lookup get a invalid rpc_req", K(ret), KP_(rpc_req), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    if (obkv_info.is_inner_request_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("inner request cannot enter state_rpc_tablegroup_lookup", K(ret), KP_(rpc_req), K_(rpc_trace_id));
    } else {
      int64_t cr_id = 0;
      if (get_global_resource_pool_processor().get_default_cluster_resource() == cluster_resource_) {
        // default cluster resource cluster id is always 0, and it is used only for building cluster resource
        cr_id = obkv_info.cluster_id_;
      } else {
        cr_id = cluster_resource_->get_cluster_id();
      }

      tablegroup_param.cont_ = this;
      tablegroup_param.tablegroup_name_ = obkv_info.table_name_;
      tablegroup_param.database_name_ = obkv_info.database_name_;
      tablegroup_param.tenant_id_ = obkv_info.tenant_id_;
      tablegroup_param.cr_version_ = cluster_resource_->version_;
      tablegroup_param.cr_id_ = cr_id;
      tablegroup_param.cluster_version_ = cluster_resource_->cluster_version_;
      tablegroup_param.mysql_proxy_ = &cluster_resource_->mysql_proxy_;
      if (OB_UNLIKELY(get_global_proxy_config().check_tenant_locality_change)) {
        tablegroup_param.tenant_version_ = cluster_resource_->get_location_tenant_version(obkv_info.tenant_name_);
      }

      LOG_DEBUG("tablegroup_param build done", K(tablegroup_param), K_(rpc_trace_id));

      RPC_REQUEST_SM_SET_DEFAULT_HANDLER(&ObRpcRequestSM::state_rpc_tablegroup_lookup);
      if (OB_FAIL(ObTableGroupProcessor::get_tablegroup_entry(tablegroup_param, tablegroup_lookup_action_handle))) {
        LOG_WDIAG("fail to call ObTableGroupProcessor::get_tablegroup_entry", K(ret), K_(rpc_trace_id));
      } else {
        if (OB_NOT_NULL(tablegroup_lookup_action_handle)) {
          pending_action_ = tablegroup_lookup_action_handle;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL == pending_action_) {
      handle_event(TABLEGROUP_ENTRY_LOOKUP_CACHE_DONE, &tablegroup_param.result_);
      tablegroup_param.result_.reset();
    } else {
      // do nothing, wait pending action
      LOG_DEBUG("ObRpcRequestSM::setup_rpc_tablegroup_lookup wait callback", KP_(pending_action), K_(rpc_trace_id));
    }
  } else {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::state_rpc_tablegroup_lookup(int event, void *data)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_version = 0;
  pending_action_ = NULL;

  LOG_DEBUG("ObRpcRequestSM::state_rpc_tablegroup_lookup", K(event), KP(data), KP_(pending_action), K_(rpc_trace_id));

  if (OB_UNLIKELY(TABLEGROUP_ENTRY_LOOKUP_CACHE_DONE != event) || OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected event type, it should not happen", K(event), K(data), K(ret), K_(rpc_trace_id));
  } else if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("state_rpc_tablegroup_lookup get a invalid rpc_req", K(ret), KP_(rpc_req), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    ObTableGroupResult *result = reinterpret_cast<ObTableGroupResult *>(data);
    // hand over the ref
    if (OB_NOT_NULL(obkv_info.tablegroup_entry_)) {
      obkv_info.tablegroup_entry_->dec_ref();
      obkv_info.tablegroup_entry_ = NULL;
    }
    obkv_info.tablegroup_entry_ = result->target_entry_;
    result->target_entry_ = NULL;

    if (OB_NOT_NULL(obkv_info.tablegroup_entry_)) {
      if (OB_UNLIKELY(get_global_proxy_config().check_tenant_locality_change)) {
        tenant_version = cluster_resource_->get_location_tenant_version(obkv_info.tenant_name_);
      }

      obkv_info.tablegroup_entry_->renew_last_access_time();
      obkv_info.tablegroup_entry_->check_and_set_expire_time(tenant_version, false);
      LOG_DEBUG("ObRpcRequestSM get tablegroup entry succ", "tablegroup", *obkv_info.tablegroup_entry_, K_(rpc_trace_id), K_(obkv_info.tenant_id));
    }
  }

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.table_group_lookup_end_ = get_based_hrtime();
    cmd_time_stats_.tablegroup_entry_lookup_time_ += milestone_diff_new(milestones_.table_group_lookup_begin_, milestones_.table_group_lookup_end_);
  }

  if (OB_SUCC(ret)) {
    set_state_and_call_next(RPC_REQ_INNER_INFO_GET);
  } else {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return EVENT_DONE;
}

int ObRpcRequestSM::setup_rpc_index_lookup()
{
  int ret = OB_SUCCESS;
  ObAction *index_lookup_action_handle = NULL;
  ObIndexParam index_param;

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.index_entry_lookup_begin_ = get_based_hrtime();
    milestones_.index_entry_lookup_end_ = 0;
  }

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("state_rpc_index_lookup get a invalid rpc_req", K(ret), KP_(rpc_req), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    if (obkv_info.is_inner_request_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("inner request cannot enter state_rpc_index_lookup", K(ret), KP_(rpc_req), K_(rpc_trace_id));
    } else {
      int64_t cr_id = 0;
      if (get_global_resource_pool_processor().get_default_cluster_resource() == cluster_resource_) {
        // default cluster resource cluster id is always 0, and it is used only for building cluster resource
        cr_id = obkv_info.cluster_id_;
      } else {
        cr_id = cluster_resource_->get_cluster_id();
      }

      index_param.cont_ = this;
      index_param.name_.index_name_ = obkv_info.index_name_;
      index_param.name_.table_name_ = obkv_info.table_name_;
      index_param.name_.cluster_name_ = obkv_info.cluster_name_;
      index_param.name_.database_name_ = obkv_info.database_name_;
      index_param.name_.tenant_name_ = obkv_info.tenant_name_;
      index_param.cluster_id_ = cr_id;
      index_param.cluster_version_ = cluster_resource_->cluster_version_;

      LOG_DEBUG("index_param build done", K(index_param), K_(rpc_trace_id));

      RPC_REQUEST_SM_SET_DEFAULT_HANDLER(&ObRpcRequestSM::state_rpc_index_lookup);
      // 构建index entry key，捞取index entry，如果捞取到了index entry, 跳转
      if (OB_FAIL(ObIndexProcessor::get_index_entry(index_param, index_lookup_action_handle))) {
        LOG_WDIAG("fail to call ObIndexProcessor::get_index_entry", K(ret), K_(rpc_trace_id));
      } else {
        if (OB_NOT_NULL(index_lookup_action_handle)) {
          pending_action_ = index_lookup_action_handle;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    // normal query with index, check if index entry existed, and process
    if (NULL == pending_action_) {
      handle_event(INDEX_ENTRY_LOOKUP_CACHE_DONE, &index_param.result_);
      index_param.result_.reset();
    } else {
      // do nothing, wait pending action
      LOG_DEBUG("ObRpcRequestSM::setup_rpc_index_lookup wait callback", KP_(pending_action), K_(rpc_trace_id));
    }
  } else {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::state_rpc_index_lookup(int event, void *data)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_version = 0;
  pending_action_ = NULL;

  LOG_DEBUG("ObRpcRequestSM::state_rpc_index_lookup", K(event), KP(data), KP_(pending_action), K_(rpc_trace_id));

  if (OB_UNLIKELY(INDEX_ENTRY_LOOKUP_CACHE_DONE != event) || OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected event type, it should not happen", K(event), K(data), K(ret), K_(rpc_trace_id));
  } else if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("state_rpc_index_lookup get a invalid rpc_req", K(ret), KP_(rpc_req), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    ObIndexResult *result = reinterpret_cast<ObIndexResult *>(data);
    // hand over the ref
    if (OB_NOT_NULL(obkv_info.index_entry_)) {
      obkv_info.index_entry_->dec_ref();
      obkv_info.index_entry_ = NULL;
    }
    obkv_info.index_entry_ = result->target_entry_;
    result->target_entry_ = NULL;

    if (OB_NOT_NULL(obkv_info.index_entry_)) {
      if (OB_UNLIKELY(get_global_proxy_config().check_tenant_locality_change)) {
        tenant_version = cluster_resource_->get_location_tenant_version(obkv_info.tenant_name_);
      }

      obkv_info.index_entry_->renew_last_access_time();
      obkv_info.index_entry_->check_and_set_expire_time(tenant_version, false);
      // match global index
      obkv_info.set_global_index_route(true);
      obkv_info.index_table_name_ = obkv_info.index_entry_->get_index_table_name();
      obkv_info.data_table_id_ = obkv_info.index_entry_->get_data_table_id();
      LOG_DEBUG("ObRpcRequestSM get index entry succ", "index_entry", *obkv_info.index_entry_, K_(rpc_trace_id));
    } else {
      // 没有找到index entry
      ObTableQueryAsyncEntry *query_async_entry = obkv_info.query_async_entry_;
      if (OB_NOT_NULL(query_async_entry) && query_async_entry->is_global_index_query() && 0 != query_async_entry->get_data_table_id()) {
        // 如果async info确定是全局索引，但是找不到index entry，可能是开了qa mode导致没有捞取到，这里还是设置全局索引路由
        obkv_info.data_table_id_ = query_async_entry->get_data_table_id();
        if (OB_FAIL(obkv_info.generate_index_table_name())) {
          LOG_WDIAG("fail to generate_index_table_name", K(ret), K_(rpc_trace_id));
        } else {
          LOG_DEBUG("not find index_entry, global index route by async info", "index_table_name", obkv_info.index_table_name_, K_(rpc_trace_id));
          obkv_info.set_global_index_route(true);
        }
      }
    }
  }

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.index_entry_lookup_end_ = get_based_hrtime();
    cmd_time_stats_.index_entry_lookup_time_ += milestone_diff_new(milestones_.index_entry_lookup_begin_, milestones_.index_entry_lookup_end_);
  }

  if (OB_SUCC(ret)) {
    set_state_and_call_next(RPC_REQ_INNER_INFO_GET);
  } else {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return EVENT_DONE;
}

int ObRpcRequestSM::fill_pll_tname()
{
  int ret = OB_SUCCESS;
  ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

  LOG_DEBUG("ObRpcRequestSM::fill_pll_tname", K(obkv_info), K(obkv_info.cluster_name_), K_(rpc_trace_id));

  // 1. get cluster_name
  ObString &cluster_name = obkv_info.cluster_name_;
  // 2. get tenant_name
  ObString &tenant_name = obkv_info.tenant_name_;
  // 3. get database_name and table_name;
  ObString table_name;
  ObString package_name;
  ObString database_name;

  // get table name
  if (obkv_info.is_hbase_request() && obkv_info.is_hbase_empty_family() && OB_NOT_NULL(obkv_info.tablegroup_entry_)) {
    table_name = obkv_info.tablegroup_entry_->get_table_names().at(0);
    LOG_DEBUG("ObRpcRequestSM::fill_pll_tname use tablegroup entry", KPC(obkv_info.tablegroup_entry_));
  } else if (obkv_info.is_query_with_index() && obkv_info.is_global_index_route() && !obkv_info.index_table_name_.empty()) {
    table_name = obkv_info.index_table_name_;
    LOG_DEBUG("ObRpcRequestSM::fill_pll_tname use index entry", "index_table_name", obkv_info.index_table_name_);
  } else if (!obkv_info.is_auth() && !obkv_info.table_name_.empty()) {
    table_name = obkv_info.table_name_;
  }

  // get table name and database name
  if (OB_UNLIKELY(table_name.empty())) {
    // if table is empty , use it __all_dummy
    table_name = OB_ALL_DUMMY_TNAME;
    database_name = OB_SYS_DATABASE_NAME;
  } else if (OB_UNLIKELY(table_name[0] == '_' &&
              table_name == OB_ALL_DUMMY_TNAME)) {
    // if table is __all_dummy, just set the db to oceanbase
    database_name = OB_SYS_DATABASE_NAME;
  } else {
    // is_table_name_from_parser = true;
    database_name = obkv_info.database_name_;
    if (OB_UNLIKELY(database_name.empty())) {
      database_name = OB_SYS_DATABASE_NAME;
    }
    LOG_DEBUG("ObRpcRequestSM::fill_pll_tname", K(table_name), K(database_name), K_(rpc_trace_id));
  }

  // set table entry name
  ObTableEntryName &te_name = pll_info_.te_name_;
  te_name.cluster_name_ = cluster_name;
  te_name.tenant_name_ = tenant_name;
  te_name.database_name_ = database_name;
  te_name.package_name_ = package_name;
  te_name.table_name_ = table_name;

  // print info
  LOG_DEBUG("current extracted table entry name", "table_entry_name", te_name, K(ret), K_(rpc_trace_id));
  if (OB_FAIL(ret) || OB_UNLIKELY(!te_name.is_valid())) {
    LOG_WDIAG("fail to extract_partition_info", K(te_name), K(ret), K_(rpc_trace_id));
    te_name.reset();
  }

  return ret;
}

int ObRpcRequestSM::setup_rpc_partition_lookup()
{
  int ret = OB_SUCCESS;

  LOG_DEBUG("ObRpcRequestSM::setup_rpc_partition_lookup handle", K(rpc_req_), K(this), K_(rpc_trace_id));
  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.pl_lookup_begin_ = get_based_hrtime();
    milestones_.pl_lookup_end_ = 0;
  }

  if (!is_valid_rpc_req() || rpc_req_->canceled()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::setup_rpc_partition_lookup get invalid rpc req or cancel request", K(ret), K_(rpc_req), K_(rpc_trace_id));
  } else if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    pll_info_.lookup_success_ = false;
    LOG_EDIAG("setup_rpc_partition_lookup, pending_action_ should be NULL",
              K_(pending_action), K_(sm_id), K_(rpc_trace_id));
  } else if (pll_info_.lookup_success_) {
    LOG_DEBUG("[ObRpcSM::do_partition_location_lookup] Skipping partition "
              "location lookup, provided by plugin", K_(sm_id), K_(rpc_trace_id));
    set_state_and_call_next(RPC_REQ_PARTITION_LOOKUP_DONE);
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    ObTableQueryAsyncEntry *query_async_entry = obkv_info.query_async_entry_;

    RPC_REQUEST_SM_SET_DEFAULT_HANDLER(&ObRpcRequestSM::state_rpc_partition_lookup);
    RPC_REQ_SM_ENTER_STATE(ObRpcReq::RpcReqSmState::RPC_REQ_SM_PARTITION_LOOKUP);

    if (OB_NOT_NULL(query_async_entry) && query_async_entry->is_global_index_query() && !obkv_info.is_global_index_route()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("setup_rpc_partition_lookup, global index async query but not global index route", K(ret), KPC(query_async_entry), K_(rpc_trace_id));
    } else if (OB_FAIL(fill_pll_tname())) {
      LOG_WDIAG("fail to call fill_pll_tname", K(ret), K_(rpc_trace_id));
    } else {
      ObTableEntryName &name = pll_info_.te_name_;
      ObAction *pl_lookup_action_handle = NULL;
      bool find_entry = false;

      LOG_DEBUG("do_partition_location_lookup name before", "is_query_with_index", obkv_info.is_query_with_index(),
                "is_global_index_route", obkv_info.is_global_index_route(), K_(obkv_info.index_table_name), K_(rpc_trace_id));

      // 1.先从table_map中获取
      if (OB_LIKELY(!pll_info_.is_force_renew() && !name.is_all_dummy_table()
                    && !pll_info_.is_cached_dummy_force_renew())) {

        ObTableRefHashMap &table_map = self_ethread().get_table_map();
        ObTableEntry *tmp_entry = NULL;
        int64_t cr_id = 0;
        if (get_global_resource_pool_processor().get_default_cluster_resource() == cluster_resource_) {
          // default cluster resource cluster id is always 0, and it is used only for building cluster resource
          cr_id = obkv_info.cluster_id_;
        } else {
          cr_id = cluster_resource_->get_cluster_id();
        }
        ObTableEntryKey key(name, cluster_resource_->version_, cr_id);
        tmp_entry = table_map.get(key);
        if (NULL != tmp_entry && !tmp_entry->is_partition_table()
            && (tmp_entry->is_avail_state() || tmp_entry->is_updating_state())
            && !(get_global_table_cache().is_table_entry_expired(*tmp_entry))) {
          //RPC got table entry from local
          tmp_entry->renew_last_access_time();
          ObMysqlRouteResult result; //RPC just used type of ObMysqlRouteResult
          result.table_entry_ = tmp_entry;
          result.is_table_entry_from_remote_ = false;
          result.has_dup_replica_ = tmp_entry->has_dup_replica();
          tmp_entry->set_need_force_flush(false);
          find_entry = true;
          LOG_DEBUG("get table entry from thread map", KPC(tmp_entry), K_(rpc_trace_id));
          //RPC call the next sm state
          state_rpc_partition_lookup(TABLE_ENTRY_EVENT_LOOKUP_DONE, &result);
        } else if (NULL != tmp_entry) {
          tmp_entry->dec_ref();
        }
      }

      if (OB_UNLIKELY(!find_entry)) {
        ObRouteParam param;
        param.cont_ = this;
        param.force_renew_ = pll_info_.is_force_renew();
        param.is_need_force_flush_ = pll_info_.is_need_force_flush();
        // param.use_lower_case_name_ = client_session_->get_session_info().need_use_lower_case_names();
        param.mysql_proxy_ = &cluster_resource_->mysql_proxy_;
        param.cr_version_ = cluster_resource_->version_;
        if (get_global_resource_pool_processor().get_default_cluster_resource() == cluster_resource_) {
          // default cluster resource cluster id is always 0, and it is used only for building cluster resource
          param.cr_id_ = cluster_id_;
        } else {
          param.cr_id_ = cluster_resource_->get_cluster_id();
        }
        if (OB_UNLIKELY(get_global_proxy_config().check_tenant_locality_change)) {
          param.tenant_version_ = cluster_resource_->get_location_tenant_version(
              obkv_info.tenant_name_);
        }
        param.timeout_us_ = hrtime_to_usec(mysql_config_params_->short_async_task_timeout_);
        //  add is_partition_table_route_supported, set it to global config
        param.is_partition_table_route_supported_ = get_global_proxy_config().enable_partition_table_route;
        param.is_oracle_mode_ = false;
        param.need_pl_route_ = false;
        param.current_idc_name_ = get_current_idc_name();
        param.cluster_version_ = cluster_resource_->cluster_version_;

        param.ob_rpc_req_ = rpc_req_;
        LOG_DEBUG("do_partition_location_lookup name", K(name), "renew", pll_info_.is_cached_dummy_force_renew(),
                  "cluster_version", param.cluster_version_, K_(rpc_trace_id));
        if (pll_info_.is_cached_dummy_force_renew() && !rpc_req_->is_inner_request()) {
          param.name_.shallow_copy(name.cluster_name_, name.tenant_name_,
                                  ObString::make_string(OB_SYS_DATABASE_NAME),
                                  ObString::make_string(OB_ALL_DUMMY_TNAME));
        } else {
          param.name_.shallow_copy(name);
        }

        LOG_DEBUG("Doing partition location Lookup", K_(sm_id), K(param), "partition_id", obkv_info.partition_id_,
                  "ls_id", obkv_info.ls_id_, K_(rpc_trace_id));
        pl_lookup_action_handle = NULL;
        param.src_type_ = OB_RPOXY_ROUTE_FOR_RPC;
        if (OB_ISNULL(mutex_->thread_holding_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("mutex_->thread_holding_ is NULL", K(this), KPC_(rpc_req), K(ret), K_(rpc_trace_id));
        } else {
          ret = ObMysqlRoute::get_route_entry(param, cluster_resource_, pl_lookup_action_handle);
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_LIKELY(NULL == pl_lookup_action_handle)) {
          // cache hit and has called back, do nothing
        } else {
          pending_action_ = pl_lookup_action_handle;
          LOG_DEBUG("ObRpcRequestSM::setup_rpc_partition_lookup wait callback", KP_(pending_action), K_(rpc_trace_id));
        }
      } else {
        pll_info_.lookup_success_ = false;
        LOG_WDIAG("failed to get table entry", K_(sm_id), K(ret), K_(rpc_trace_id));
        if (OB_NOT_NULL(rpc_req_)) {
          //init proxy error to rpc_req
          rpc_req_->set_rpc_req_error_code(OB_TABLE_NOT_EXIST);
        }
        set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::keep_dummy_entry_and_update_ldc(ObMysqlRouteResult &result)
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *rpc_ctx = NULL;

  if (!is_valid_rpc_req() || OB_ISNULL(result.table_entry_)
        || !result.table_entry_->is_dummy_entry() || OB_ISNULL(rpc_ctx = rpc_req_->get_rpc_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("keep_dummy_entry_and_update_ldc get a invalid argument", K(ret), KP_(rpc_req), KP(rpc_ctx), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    LOG_DEBUG("get dummy entry, store into obkv_info and check update ldc", K_(rpc_trace_id));

    // keey dummy entry
    if (OB_NOT_NULL(obkv_info.dummy_entry_)) {
      obkv_info.dummy_entry_->dec_ref();
      obkv_info.dummy_entry_ = NULL;
    }
    obkv_info.dummy_entry_ = result.table_entry_;
    obkv_info.dummy_entry_->inc_ref();

    // update ldc
    // 这里读rpc_ctx中的server_state_version不加锁，读到老的影响不大
    if (OB_FAIL(ObRpcReqCtx::check_update_ldc(obkv_info.dummy_entry_, obkv_info.dummy_ldc_, rpc_ctx->server_state_version_, cluster_resource_))) {
      LOG_WDIAG("fail to call check_update_ldc", K(ret), "dummy_entry", obkv_info.dummy_entry_, "dummy_ldc", obkv_info.dummy_ldc_);
    }
  }

  return ret;

}

int ObRpcRequestSM::update_cached_dummy_entry_and_ldc()
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *rpc_ctx = NULL;

  if (!is_valid_rpc_req() || OB_ISNULL(rpc_ctx = rpc_req_->get_rpc_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("update_cached_dummy_entry_and_ldc get a invalid rpc_req", K(ret), KP_(rpc_req), KP(rpc_ctx), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    LOG_DEBUG("rpc update cached dummy entry and ldc", "is_auth", obkv_info.is_auth(), K_(rpc_trace_id));

    if (obkv_info.is_auth()) {
      // 读请求不存在冲突，不需要拿锁
      if (OB_FAIL(rpc_ctx->update_cache_entry(obkv_info.dummy_entry_, obkv_info.dummy_ldc_, mysql_config_params_->tenant_location_valid_time_))) {
        LOG_WARN("fail to update_cache_entry for rpc_ctx", K(ret), K_(rpc_trace_id));
      } else {
        rpc_ctx->cas_set_dummy_entry_avail_state();
      }
    } else {
      // 1. 先获取状态，如果其他线程已经更新了dummy entry以及dummy ldc，则不处理
      // 2. 拿写锁，首先二次判断状态，如果以及AVAIL，则不处理，否则触发更新流程
      // 3. 更新状态、更新cache entry
      if (rpc_ctx->is_cached_dummy_entry_updating_state()) {
        DRWLock &rwlock = rpc_ctx->get_rpc_ctx_lock();
        DRWLock::WRLockGuard rwlock_guard(rwlock);
        if (rpc_ctx->is_cached_dummy_entry_updating_state()) {
          if (OB_FAIL(rpc_ctx->update_cache_entry(obkv_info.dummy_entry_, obkv_info.dummy_ldc_, mysql_config_params_->tenant_location_valid_time_))) {
            LOG_WARN("fail to update_cache_entry for rpc_ctx", K(ret), K_(rpc_trace_id));
          } else {
            // update server state version for update ldc
            rpc_ctx->server_state_version_ = cluster_resource_->server_state_version_;
            rpc_ctx->cas_set_dummy_entry_avail_state();
          }
        }
      }
    }
  }

  return ret;
}

int ObRpcRequestSM::get_cached_dummy_entry_and_ldc()
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *rpc_ctx = NULL;

  if (!is_valid_rpc_req() || OB_ISNULL(rpc_ctx = rpc_req_->get_rpc_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("get_cached_dummy_entry_and_ldc get a invalid rpc_req", K(ret), KP_(rpc_req), KP(rpc_ctx), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    if (rpc_ctx->is_cached_dummy_entry_avail_state()) {
      DRWLock &rwlock = rpc_ctx->get_rpc_ctx_lock();
      DRWLock::RDLockGuard rwlock_guard(rwlock);
      if (OB_ISNULL(rpc_ctx->dummy_entry_)
        || rpc_ctx->is_cached_dummy_entry_expired()
        || rpc_ctx->is_cached_dummy_ldc_empty()) {
        // set update state , update dummy entry and renew rpc ctx cached dummy entry
        rpc_ctx->cas_set_cluster_resource_updating_state();
      }
      if (rpc_ctx->is_cached_dummy_entry_avail_state()) {
        if (OB_FAIL(obkv_info.set_dummy_entry(rpc_ctx->dummy_entry_))) {
          LOG_WARN("fail to set_dummy_entry into obkv_info", K(ret), KP_(rpc_ctx->dummy_entry));
        } else if (OB_FAIL(ObLDCLocation::copy_dummy_ldc(rpc_ctx->dummy_ldc_, obkv_info.dummy_ldc_))) {
          LOG_WARN("fail to copy_dummy_ldc", K(ret), K_(rpc_ctx->dummy_ldc));
        }
      }
    }

    LOG_DEBUG("rpc get cached dummy entry and ldc done", K(ret), KP_(obkv_info.dummy_entry), K_(rpc_trace_id));
  }

  return ret;
}

int ObRpcRequestSM::process_partition_location(ObMysqlRouteResult &result)
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *rpc_ctx = NULL;

  if (!is_valid_rpc_req() || OB_ISNULL(rpc_ctx = rpc_req_->get_rpc_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("process_partition_location get a invalid rpc_req", K(ret), KP_(rpc_req), KP(rpc_ctx), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    if (OB_NOT_NULL(result.table_entry_) && result.table_entry_->is_dummy_entry()) {
      // 如果是dummy entry，检查是否要更新rpc ctx中缓存
      if (OB_FAIL(keep_dummy_entry_and_update_ldc(result))) {
        LOG_WDIAG("fail to call keep_dummy_entry_and_update_ldc", K(ret), KP_(rpc_req), KP(rpc_ctx), K_(rpc_trace_id));
      } else if (OB_FAIL(update_cached_dummy_entry_and_ldc())) {
        LOG_WDIAG("fail to call update_cached_dummy_entry_and_ldc", K(ret), KP_(rpc_req), KP(rpc_ctx), K_(rpc_trace_id));
      }
    }

    if (OB_SUCC(ret)) {
      if (pll_info_.is_cached_dummy_force_renew()) {
        pll_info_.set_cached_dummy_force_renew_done();
        // no need set route info, only to update cached dummy entry
        result.ref_reset();
      } else if (OB_ISNULL(result.table_entry_)) { //avoid core for acquire table_id(obrpc need table_id to send to OBServer, next)
        ret = OB_TABLE_NOT_EXIST;
        LOG_WDIAG("handle the table not exist in cluster", K(ret), K_(rpc_trace_id));
      } else {
        LOG_DEBUG("rpc table id process_partition_location", "table_name", result.table_entry_->get_table_name(),
                  "table_id", result.table_entry_->get_table_id(), K_(rpc_trace_id));

        obkv_info.table_id_ = result.table_entry_->get_table_id();
        // if not global index route, keep data_table_id
        if (!obkv_info.is_global_index_route()) {
          obkv_info.data_table_id_ = obkv_info.table_id_;
        }
        if (OB_NOT_NULL(obkv_info.query_async_entry_)) {
          obkv_info.query_async_entry_->set_table_id(obkv_info.table_id_);   //异步query info保存table id
          obkv_info.query_async_entry_->set_partition_table(result.table_entry_->is_partition_table());
          if (!obkv_info.is_global_index_route()) {
            obkv_info.query_async_entry_->set_data_table_id(obkv_info.table_id_); //异步query info保存data table id
          }
          LOG_DEBUG("keep partition info in async info", KPC(obkv_info.query_async_entry_), K_(rpc_trace_id));
        }
        if (!result.table_entry_->is_partition_table()) {
          obkv_info.partition_id_ = 0; //force update partition id of single table to 0
          obkv_info.set_definitely_single(true);
        }
        pll_info_.set_route_info(result);
      }
    }
  }

  return ret;
}

// When route calculation fails, dirty table entry is required to prevent continuous calculation failures.
int ObRpcRequestSM::dirty_rpc_route_result(ObMysqlRouteResult *result)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::dirty_rpc_route_result get a NULL result", K(result), K(ret), K_(rpc_trace_id));
  } else {
    if (NULL != result->table_entry_
        && result->table_entry_->need_update_entry()
        && result->table_entry_->cas_set_dirty_state()) {
      PROXY_LOG(INFO, "this table entry will set to dirty and wait for update", KPC_(result->table_entry), K_(rpc_trace_id));
    }
    if (NULL != result->part_entry_
        && result->part_entry_->need_update_entry()
        && result->part_entry_->cas_set_dirty_state()) {
      PROXY_LOG(INFO, "this partition entry will set to dirty and wait for update", KPC_(result->part_entry), K_(rpc_trace_id));
    }
  }

  return ret;
}

int ObRpcRequestSM::state_rpc_partition_lookup(int event, void *data)
{
  int ret = OB_SUCCESS;
  bool is_canceled = false;
  bool is_need_check_query_with_index = false;
  bool is_tablegroup_error_table_not_exist = false;
  STATE_ENTER(ObRpcRequestSM::state_rpc_partition_lookup, event);
  pending_action_ = NULL;

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.pl_lookup_end_ = get_based_hrtime();
    cmd_time_stats_.pl_lookup_time_ += milestone_diff_new(milestones_.pl_lookup_begin_, milestones_.pl_lookup_end_);
    milestones_.pl_process_begin_ = milestones_.pl_lookup_end_;
  }

  if (OB_UNLIKELY(TABLE_ENTRY_EVENT_LOOKUP_DONE != event) || OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected event type, it should not happen", K(event), K(data), K(ret), K_(rpc_trace_id));
  } else if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid rpc req", K_(rpc_req), K_(rpc_trace_id));
  } else if ((is_canceled = rpc_req_->canceled())) {
    LOG_INFO("ObRpcRequestSM::state_rpc_partition_lookup get a canceled rpc_req", K_(rpc_req), K_(rpc_trace_id));
  } else {
    ObMysqlRouteResult *result = reinterpret_cast<ObMysqlRouteResult *>(data);
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    if (result->rpc_calc_error_) {
      /**
       * @brief
       * 1. The table entry is obtained successfully.
       * 2. The route calculation fails.
       * 3. The current query is with index.
       * 4. Not inner request
       * 5. rpc_enable_global_index is true
       * If the conditions are met, it means that an error is reported during the calculation phase. It may be that
       * the columns passed in by the user represent the column information of the global index, causing the verification
       * to fail. You need to use global index logic to try again.
       */
      if (!rpc_req_->is_inner_request()
          && OB_ERR_KV_ROWKEY_MISMATCH == result->rpc_error_code_
          && obkv_info.is_query_with_index()
          && !obkv_info.is_global_index_route()
          && OB_NOT_NULL(result->table_entry_)
          && rpc_req_->get_rpc_request_config_info().rpc_enable_global_index_) {
        LOG_DEBUG("rpc calc error in query with index", "error", result->rpc_calc_error_, K_(rpc_trace_id));
        obkv_info.table_id_ = result->table_entry_->get_table_id();
        obkv_info.data_table_id_ = obkv_info.table_id_;

        if (OB_FAIL(obkv_info.generate_index_table_name())) {
          LOG_WDIAG("fail to generate_index_table_name", K(ret), K_(rpc_trace_id));
        } else {
          LOG_DEBUG("generate_index_table_name succeed", "index_table_name", obkv_info.index_table_name_, K_(rpc_trace_id));
          ObTableEntryName &name = pll_info_.te_name_;
          name.table_name_ = obkv_info.index_table_name_;
          is_need_check_query_with_index = true;
        }
      } else {
        if (OB_FAIL(dirty_rpc_route_result(result))) {
          LOG_WDIAG("rpc calc error and fail to ditry result", K(ret), KP(result), K_(rpc_trace_id));
        } else {
          // set ret error
          ret = result->rpc_error_code_ ;
        }
        LOG_WDIAG("state_rpc_partition_lookup get rpc calc error", K(ret),  K_(rpc_trace_id), KPC_(rpc_req));
      }
      result->ref_reset();     // need to free table entry and partition entry
    } else {
      pll_info_.set_need_force_flush(false);

      if (pll_info_.is_force_renew()) {
        pll_info_.set_force_renew_done();
      }

      if (OB_FAIL(process_partition_location(*result))) {
        if (obkv_info.is_hbase_request() && obkv_info.is_hbase_empty_family() && OB_TABLE_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          is_tablegroup_error_table_not_exist = true;
          LOG_INFO("hbase empty family request get OB_TABLE_NOT_EXIST, maybe schema changed, will retry", K_(sm_id), K_(rpc_trace_id), K_(obkv_info.tenant_id));
        } else {
          LOG_WDIAG("fail to process partition location", K_(sm_id), K(ret), K_(rpc_trace_id));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (is_canceled) {
        // do nothing
      } else if (is_tablegroup_error_table_not_exist) {
        pll_info_.lookup_success_ = false;
        obkv_info.set_route_entry_dirty();
        rpc_req_->retry_reset();
        already_get_tablegroup_entry_ = false;
        set_state_and_call_next(RPC_REQ_INNER_INFO_GET);
      } else if (is_need_check_query_with_index) {
        pll_info_.lookup_success_ = false;
        obkv_info.set_global_index_route(true);
        rpc_req_->retry_reset();
        set_state_and_call_next(RPC_REQ_IN_PARTITION_LOOKUP);
      } else {
        pll_info_.lookup_success_ = true;
        // call ObRpcRequestSM::handle_pl_lookup() to handle fail / success
        set_state_and_call_next(RPC_REQ_PARTITION_LOOKUP_DONE);
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return EVENT_DONE;
}

int ObRpcRequestSM::handle_global_index_partition_lookup_done()
{
  int ret = OB_SUCCESS;

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("handle_global_index_partition_lookup_done get a invalid rpc_req", K(ret), KP_(rpc_req), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    if (!obkv_info.is_global_index_route()) {
      // do nothing
    } else {
      if (OB_NOT_NULL(obkv_info.query_async_entry_)) {
        obkv_info.query_async_entry_->set_global_index_query(true);
      }
      if (OB_ISNULL(obkv_info.index_entry_)) {
        // 构建index info
        uint64_t tenant_version = 0;
        int64_t cr_id = 0;
        ObIndexEntry *index_entry = NULL;
        ObIndexEntryName index_name;
        index_name.index_name_ = obkv_info.index_name_;
        index_name.table_name_ = obkv_info.table_name_;
        index_name.cluster_name_ = obkv_info.cluster_name_;
        index_name.database_name_ = obkv_info.database_name_;
        index_name.tenant_name_ = obkv_info.tenant_name_;
        if (get_global_resource_pool_processor().get_default_cluster_resource() == cluster_resource_) {
          // default cluster resource cluster id is always 0, and it is used only for building cluster resource
          cr_id = obkv_info.cluster_id_;
        } else {
          cr_id = cluster_resource_->get_cluster_id();
        }

        if (OB_FAIL(ObIndexEntry::alloc_and_init_index_entry(index_name, cluster_resource_->cluster_version_, cr_id, index_entry))) {
          LOG_WDIAG("fail to alloc_and_init_index_entry", K(ret), K_(rpc_trace_id));
        } else {
          if (OB_UNLIKELY(get_global_proxy_config().check_tenant_locality_change)) {
            tenant_version = cluster_resource_->get_location_tenant_version(obkv_info.tenant_name_);
          }

          // index_entry->set_schema_version(table_entry->get_schema_version()); // do not forget
          index_entry->set_table_id(obkv_info.table_id_);
          index_entry->set_data_table_id(obkv_info.data_table_id_);
          index_entry->set_index_type(3);  // TODO : change to enum
          index_entry->set_tenant_version(tenant_version);
          index_entry->check_and_set_expire_time(tenant_version, false);

          if (OB_FAIL(index_entry->generate_index_table_name())) {
            LOG_WDIAG("fail to generate_index_table_name", K(ret), K_(rpc_trace_id));
          } else {
            obkv_info.index_entry_ = index_entry;
            obkv_info.need_add_into_cache_ = true;
            LOG_DEBUG("build new index entry", KPC(index_entry), K_(rpc_trace_id));
          }
        }
      } else {
        obkv_info.index_entry_->renew_last_valid_time();
      }
    }
  }

  return ret;
}

int ObRpcRequestSM::state_rpc_partition_lookup_done()
{
  int ret = OB_SUCCESS;

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("state_rpc_partition_lookup_done get a invalid rpc_req", K(ret), KP_(rpc_req), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    if (obkv_info.is_empty_query_result()) {
      // 内部回包
      LOG_DEBUG("will return emtpy query result", K_(rpc_trace_id));
      set_state_and_call_next(RPC_REQ_INTERNAL_BUILD_RESPONSE);
    } else {
      LOG_DEBUG("state_rpc_partition_lookup_done", K_(rpc_route_mode), K_(rpc_trace_id), K(obkv_info));
      if (obkv_info.is_global_index_route()) {
        if (OB_FAIL(handle_global_index_partition_lookup_done())) {
          LOG_WDIAG("fail to call handle_global_index_partition_lookup_done", K(ret), K_(rpc_trace_id));
        }
      }
      if (OB_SUCC(ret)) {
        if (obkv_info.is_shard()) {
          set_state_and_call_next(RPC_REQ_HANDLE_SHARD_REQUEST);
        } else {
          set_state_and_call_next(RPC_REQ_SERVER_ADDR_SEARCHING);
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

ObConsistencyLevel ObRpcRequestSM::get_trans_consistency_level()
{
  ObConsistencyLevel ret_level = common::STRONG;

  if (INVALID_CONSISTENCY != pll_info_.route_.consistency_level_) {
    ret_level = pll_info_.route_.consistency_level_;
  } else if (is_valid_rpc_req()) {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    if (obkv_info.is_read_weak()) {
      ret_level = common::WEAK;
    }
    PROXY_LOG(DEBUG, "RPC request, treat it as ",
        "ret_level", get_consistency_level_str(ret_level), K_(rpc_trace_id));

    pll_info_.route_.consistency_level_ = ret_level;
  } else {
    // do nothing
  }
  return ret_level;
}

void ObRpcRequestSM::get_route_policy(ObProxyRoutePolicyEnum policy, ObRoutePolicyEnum& ret_policy) const
{
  if (FOLLOWER_FIRST_ENUM == policy) {
    ret_policy = FOLLOWER_FIRST;
  } else if (UNMERGE_FOLLOWER_FIRST_ENUM == policy) {
    ret_policy = UNMERGE_FOLLOWER_FIRST;
  } else if (FOLLOWER_ONLY_ENUM == policy) {
    ret_policy = FOLLOWER_ONLY;
  }
}

ObRoutePolicyEnum ObRpcRequestSM::get_route_policy(const bool need_use_dup_replica)
{
  ObRoutePolicyEnum ret_policy = READONLY_ZONE_FIRST;
  ObRoutePolicyEnum session_route_policy = READONLY_ZONE_FIRST;

  if (!is_valid_rpc_req()) {
    LOG_WDIAG("invalid rpc req, use default policy", K(ret_policy), K_(rpc_req), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    switch (obkv_info.route_policy_) {
      case 0:
        session_route_policy = MERGE_IDC_ORDER;
        break;
      case 1:
        session_route_policy = READONLY_ZONE_FIRST;
        break;
      case 2:
        session_route_policy = ONLY_READONLY_ZONE;
        break;
      case 3:
        session_route_policy = UNMERGE_ZONE_FIRST;
        break;
      default:
        PROXY_TXN_LOG(WDIAG, "unknown route policy, use default policy",
                      "ob_route_policy", obkv_info.route_policy_,
                      "session_route_policy", get_route_policy_enum_string(session_route_policy), K_(rpc_trace_id));
    }

    if (need_use_dup_replica) {
      //if dup_replica read, use DUP_REPLICA_FIRST, no need care about zone type
      ret_policy = DUP_REPLICA_FIRST;
    } else if (obkv_info.dummy_ldc_.is_readonly_zone_exist()) {
      if (common::WEAK == get_trans_consistency_level()) {
        //if wead read, use session_route_policy
        ret_policy = session_route_policy;
      } else if (obkv_info.is_read_weak()) {
        if (obkv_info.is_read_consistency_set_) {
          if (common::WEAK == static_cast<ObConsistencyLevel>(obkv_info.cs_read_consistency_)) {
            ret_policy = session_route_policy;
          } else {//strong
            ret_policy = ONLY_READWRITE_ZONE;
          }
        } else {
          ret_policy = MERGE_IDC_ORDER;
        }
      } else {
        //if readonly zone not support, only use readwrite zone
        ret_policy = ONLY_READWRITE_ZONE;
      }
    } else {
      //if no readonly zone exist, use orig policy
      ret_policy = MERGE_IDC_ORDER;
      if (common::WEAK == get_trans_consistency_level()) {
        if (is_valid_proxy_route_policy(obkv_info.proxy_route_policy_)
          && obkv_info.is_proxy_route_policy_set_) {
          get_route_policy(obkv_info.proxy_route_policy_, ret_policy);
        } else {
          // const ObString value = get_global_proxy_config().proxy_route_policy.str();
          ObProxyRoutePolicyEnum policy = get_proxy_route_policy(rpc_req_->get_rpc_request_config_info().proxy_route_policy_.get_config_str());
          LOG_DEBUG("succ to global variable proxy_route_policy",
                  "policy", get_proxy_route_policy_enum_string(policy), K_(rpc_trace_id));
          get_route_policy(policy, ret_policy);
        }
      }
    }
  }

  return ret_policy;
}

ObString ObRpcRequestSM::get_current_idc_name() const
{
  ObString ret_idc(mysql_config_params_->proxy_idc_name_);

  return ret_idc;
}

void ObRpcRequestSM::get_region_name_and_server_info(ObIArray<ObServerStateSimpleInfo> &simple_servers_info, ObIArray<ObString> &region_names)
{
  int ret = OB_SUCCESS;

  const uint64_t ss_version = cluster_resource_->server_state_version_;
  ObIArray<ObServerStateSimpleInfo> &server_state_info = cluster_resource_->get_server_state_info(ss_version);
  common::DRWLock &server_state_lock = cluster_resource_->get_server_state_lock(ss_version);
  int err_no = 0;
  if (0 != (err_no = server_state_lock.try_rdlock())) {
    LOG_WDIAG("fail to tryrdlock server_state_lock, ignore", K(ss_version), K(err_no), K_(rpc_trace_id));
  } else {
    if (OB_FAIL(simple_servers_info.assign(server_state_info))) {
      LOG_WDIAG("fail to assign simple_servers_info", K(ret), K_(rpc_trace_id));
    }
    server_state_lock.rdunlock();

    if (OB_SUCC(ret)) {
      const ObString &idc_name = get_current_idc_name();
      const ObString &cluster_name = cluster_resource_->get_cluster_name();
      const int64_t cluster_id = cluster_resource_->get_cluster_id();
      ObProxyNameString region_name_from_idc_list;
      ObLDCLocation::ObRegionMatchedType match_type = ObLDCLocation::MATCHED_BY_NONE;

      if (OB_FAIL(ObLDCLocation::get_region_name(simple_servers_info, idc_name,
                                                 cluster_name, cluster_id,
                                                 region_name_from_idc_list,
                                                 match_type, region_names))) {
        LOG_WDIAG("fail to get region name", K(idc_name), K(ret), K_(rpc_trace_id));
      }
    }
  }
}

int ObRpcRequestSM::setup_rpc_server_addr_searching()
{
  int ret = OB_SUCCESS;
  ++pll_info_.pl_attempts_;
  ObRpcReqCtx *rpc_ctx = NULL;
  bool need_update_cached_dummy_entry = false;
  bool need_retry_with_normal_mode = false;

  if (!is_valid_rpc_req() || OB_ISNULL(rpc_ctx = rpc_req_->get_rpc_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::setup_rpc_server_addr_searching get invalid rpc_req", K(ret), KP_(rpc_req),
              KP(rpc_ctx), K_(rpc_trace_id));
  } else if (rpc_req_->canceled()) {
    LOG_INFO("ObRpcRequestSM::setup_rpc_server_addr_searching get a canceled rpc_req", K_(rpc_req), K_(rpc_trace_id));
  } else if (OB_UNLIKELY(!pll_info_.lookup_success_)) { // partition location lookup failed
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::setup_rpc_server_addr_searching partition location lookup failed", K(ret), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    LOG_DEBUG("[ObRpcRequestSM::setup_rpc_server_addr_searching] Partition location Lookup successful",
              "pl_attempts", pll_info_.pl_attempts_, K_(rpc_trace_id));
    RPC_REQ_SM_ENTER_STATE(ObRpcReq::RpcReqSmState::RPC_REQ_SM_ADDR_SEARCH);
    if (obkv_info.is_shard()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("[ObRpcRequestSM::setup_rpc_server_addr_searching] can not handle shard request", K_(rpc_trace_id));
    } else if (OB_ISNULL(mysql_config_params_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("[ObRpcRequestSM::setup_rpc_server_addr_searching] it should not arrive here, will disconnect", K(ret), K_(rpc_trace_id));
    } else {
      if (ObRpcRouteMode::RPC_ROUTE_FIND_LEADER == rpc_route_mode_) {
        if (OB_FAIL(setup_rpc_server_addr_direct_find_leader(need_retry_with_normal_mode))) {
          LOG_WDIAG("fail to call setup_rpc_server_addr_direct_find_leader", K(ret), K_(rpc_trace_id));
        }
      }
      if (need_retry_with_normal_mode || ObRpcRouteMode::RPC_ROUTE_NORMAL == rpc_route_mode_) {
        if (OB_ISNULL(obkv_info.dummy_entry_)) {
          // 1. get dummy entry from rpc ctx
          // 2. if get nothing, fetch new dummy entry and update rpc ctx
          if (OB_FAIL(get_cached_dummy_entry_and_ldc())) {
            LOG_WDIAG("fail to call get_cached_dummy_entry_and_ldc", K(ret), K_(rpc_trace_id));
          } else if (OB_ISNULL(obkv_info.dummy_entry_)) {
            need_update_cached_dummy_entry = true;
            pll_info_.set_cached_dummy_force_renew();  // 刷新
          }
        }
        if (OB_SUCC(ret) && !need_update_cached_dummy_entry) {
          if (OB_FAIL(setup_rpc_server_addr_search_normal())) {
            LOG_WDIAG("fail to call setup_rpc_server_addr_search_normal", K(ret), K_(rpc_trace_id));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (need_update_cached_dummy_entry) {
      pll_info_.lookup_success_ = false;
      set_state_and_call_next(RPC_REQ_IN_PARTITION_LOOKUP);
    } else {
      set_state_and_call_next(RPC_REQ_SERVER_ADDR_SEARCHED);
    }
  } else {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::setup_rpc_server_addr_direct_find_leader(bool &need_retry_with_normal_mode)
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *rpc_ctx = NULL;

  if (!is_valid_rpc_req() || OB_ISNULL(rpc_ctx = rpc_req_->get_rpc_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::setup_rpc_server_addr_searching get invalid rpc_req", K(ret), KP_(rpc_req),
              KP(rpc_ctx), K_(rpc_trace_id));
  } else {
    ObConnectionAttributes &server_info = rpc_req_->get_server_addr();
    const ObProxyReplicaLocation *replica = pll_info_.route_.get_obkv_strong_read_avail_replica();
    if (OB_ISNULL(replica) || !replica->is_leader()) {
      LOG_DEBUG("can not get leader replica in setup_rpc_server_addr_direct_find_leader, will retry with normal mode", KPC(replica));
      need_retry_with_normal_mode = true;
      rpc_route_mode_ = ObRpcRouteMode::RPC_ROUTE_NORMAL;
    } else {
      LOG_DEBUG("succ to get leader replica in setup_rpc_server_addr_direct_find_leader", KPC(replica));
      server_info.set_addr(ops_ip_sa_cast(replica->rpc_server_.get_sockaddr()));
      server_info.set_sql_addr(ops_ip_sa_cast(replica->server_.get_sockaddr()));
      pll_info_.route_.cur_chosen_server_.replica_ = replica;
      pll_info_.route_.leader_item_.is_used_ = true;
      pll_info_.route_.skip_leader_item_ = true;
    }
  }

  return ret;
}

int ObRpcRequestSM::get_proxy_primary_zone_array(common::ObString zone,
                                                  common::ObSEArray<common::ObString, 5> &zone_array)
{
  int ret = OB_SUCCESS;
  bool zone_finsh = false;
  // z11,z12,z13;z21,z22;..
  while (OB_SUCC(ret) && !zone.empty() && !zone_finsh) {
    ObString group = zone.split_on(';');
    if (group.empty()) {
      group = zone;
      zone_finsh = true;
    }
    const int64_t last_zone_len = zone_array.count();
    bool group_finsh = false;
    while (OB_SUCC(ret) && !group.empty() && !group_finsh) {
      ObString item = group.split_on(',');
      if (item.empty()) {
        item = group;
        group_finsh = true;
      }

      if (OB_FAIL(zone_array.push_back(item))) {
        LOG_WDIAG("fail to push back item", K(item), K(ret));
      } else { /* succ */}
    }
    std::random_shuffle(zone_array.begin() + last_zone_len, zone_array.end());
  }

  return ret;
}

int ObRpcRequestSM::setup_rpc_server_addr_search_normal()
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *rpc_ctx = NULL;

  if (!is_valid_rpc_req() || OB_ISNULL(rpc_ctx = rpc_req_->get_rpc_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::setup_rpc_server_addr_searching get invalid rpc_req", K(ret), KP_(rpc_req),
              KP(rpc_ctx), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    ObConnectionAttributes &server_info = rpc_req_->get_server_addr();
    const ObConsistencyLevel consistency_level = get_trans_consistency_level();
    bool fill_addr = false;

    if (common::STRONG == consistency_level
      && NULL != pll_info_.route_.table_entry_
      && !pll_info_.route_.table_entry_->is_dummy_entry()) {
      const ObProxyPartitionLocation *pl = pll_info_.route_.table_entry_->get_first_pl();
      for (int64_t i = 0; OB_SUCC(ret) && NULL != pl && i < pl->replica_count(); i++) {
        const ObProxyReplicaLocation *replica = pl->get_replica(i);
        if (NULL != replica && replica->is_leader()) {
          fill_addr = true;
          server_info.set_addr(ops_ip_sa_cast(replica->rpc_server_.get_sockaddr()));
          pll_info_.route_.cur_chosen_server_.replica_ = replica;
          pll_info_.route_.leader_item_.is_used_ = true;
          pll_info_.route_.skip_leader_item_ = true;
          break;
        }
      }
    }
    if (!fill_addr) {
      const ObProxyReplicaLocation *replica = NULL;
      pll_info_.route_.need_use_dup_replica_ = false;
      const bool disable_merge_status_check = true;
      ObRoutePolicyEnum route_policy;
      bool is_random_routing_mode = false;
      ModulePageAllocator *allocator = NULL;
      ObLDCLocation::get_thread_allocator(allocator);

      ObSEArray<ObString, 5> region_names(5, *allocator);
      ObSEArray<ObServerStateSimpleInfo, ObServerStateRefreshCont::DEFAULT_SERVER_COUNT> simple_servers_info(
          ObServerStateRefreshCont::DEFAULT_SERVER_COUNT, *allocator);
      get_region_name_and_server_info(simple_servers_info, region_names);
      ObString proxy_primary_zone_name(mysql_config_params_->proxy_primary_zone_name_);
      route_policy = get_route_policy(false);

      ObSEArray<ObString, 5> zone_array;
      if (OB_FAIL(get_proxy_primary_zone_array(proxy_primary_zone_name, zone_array))) {
        LOG_WDIAG("fail to fill proxy primary zone array", K(ret));
      } else if (OB_FAIL(pll_info_.route_.fill_replicas(
              consistency_level,
              route_policy,
              is_random_routing_mode,
              disable_merge_status_check,
              obkv_info,
              cluster_resource_,
              simple_servers_info,
              region_names,
              zone_array
              ))) {
        LOG_WDIAG("fail to fill replicas", K(ret), K_(rpc_trace_id));
      } else {
        int32_t attempt_count = 0;
        bool found_leader_force_congested = false;
        /* if leader is congestioned, it will be return follower for strong read. */
        replica = pll_info_.get_next_avail_replica(force_retry_congested_, attempt_count,
                                                        found_leader_force_congested);
        if ((NULL == replica) && pll_info_.is_all_iterate_once()) { // next round
          // if we has tried all servers, do force retry in next round
          LOG_INFO("all replica is force_congested, force retry congested now", K_(rpc_trace_id));
          force_retry_congested_ = true;

          // get replica again
          replica = pll_info_.get_next_avail_replica();
        }

        if (OB_ISNULL(replica)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("no replica avail", K(replica), K(ret), K_(rpc_trace_id));
        } else {
          server_info.set_addr(ops_ip_sa_cast(replica->rpc_server_.get_sockaddr()));
          server_info.set_sql_addr(ops_ip_sa_cast(replica->server_.get_sockaddr()));
          LOG_DEBUG("get replica by pl lookup, set addr", K(server_info.addr_), K(server_info.sql_addr_), K_(rpc_trace_id));
        }

        //TODO RPC not allowed found_leader_force_congested == true
        if (found_leader_force_congested) {
          ObProxyMutex *mutex_ = this->mutex_; // for stat
          PROCESSOR_INCREMENT_DYN_STAT(UPDATE_ROUTE_ENTRY_BY_CONGESTION);
          // when leader first router && (leader was congested from server or dead congested),
          // we need set forceUpdateTableEntry to avoid senseless of leader migrate
          if (pll_info_.set_target_dirty()) {
            LOG_INFO("leader is force_congested in strong read, set it to dirty "
                      "and wait for updating",
                      "addr", server_info.addr_,
                      "route", pll_info_.route_, K_(rpc_trace_id),
                      "rpc_enable_reroute", rpc_req_->get_rpc_request_config_info().rpc_enable_reroute_);
          }
        } 
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("[ObRpcRequestSM::setup_rpc_server_addr_searching] chosen server",
                "addr", server_info.addr_,
                "sm_id", sm_id_, K_(rpc_trace_id));
    }
  }

  return ret;
}

int ObRpcRequestSM::state_rpc_server_addr_searched()
{
  int ret = OB_SUCCESS;

  if (is_valid_rpc_req()) {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    LOG_DEBUG("ObRpcRequestSM::state_rpc_server_addr_searched", K_(sm_id), "rpc_req", *rpc_req_, K_(rpc_trace_id));

    if (OB_NOT_NULL(obkv_info.query_async_entry_) && !obkv_info.query_async_entry_->is_server_info_set()) {
      obkv_info.query_async_entry_->set_server_info(rpc_req_->get_server_addr().addr_, rpc_req_->get_server_addr().sql_addr_);
      LOG_DEBUG("state_rpc_server_addr_searched in first tablet async query, set addr", KPC(obkv_info.query_async_entry_), K_(rpc_trace_id));
    }
  }
  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.pl_process_end_ = get_based_hrtime();
    cmd_time_stats_.pl_process_time_ +=
      milestone_diff_new(milestones_.pl_process_begin_, milestones_.pl_process_end_);
  }

  set_state_and_call_next(RPC_REQ_IN_CONGESTION_CONTROL_LOOKUP);

  return ret;
}

int ObRpcRequestSM::setup_congestion_control_lookup()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.congestion_control_begin_ = get_based_hrtime();
    milestones_.congestion_control_end_ = 0;
  }

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::setup_congestion_control_lookup get a invalid rpc_req", K(ret), KP_(rpc_req), K_(rpc_trace_id));
  } else {

    // rpc request congestion only controled by rpc_enable_congestion_ config item.
    // For followding direct load request and stream fetch request no need to do
    // congestion_lookup, failed directly when server is failed
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    bool enable_congestion = rpc_req_->get_rpc_request_config_info().rpc_enable_congestion_
                               && (!(obkv_info.is_direct_load_req() && !obkv_info.is_first_direct_load_request()))
                               && (!(OB_NOT_NULL(obkv_info.query_async_entry_) && !obkv_info.query_async_entry_->is_first_query()));

    ObConnectionAttributes &server_info = rpc_req_->get_server_addr();

    // we will lookup congestion, only the follow all happened
    // 1. enable_congestion from config
    // 2. cur congestion is avail(base servers has been added)
    // 3. need pl lookup
    // 4. not mysql route mode
    //
    // Attention! when force_retry_congested_, also need do congestion lookup
    if (OB_UNLIKELY(enable_congestion
        && cluster_resource_->is_congestion_avail()
        && need_pl_lookup_
        && !mysql_config_params_->is_mysql_routing_mode())) {
      need_congestion_lookup_ = true;
    } else {
      LOG_DEBUG("no need do congestion lookup", K_(sm_id), K(enable_congestion),
                K(cluster_resource_->is_congestion_avail()), "need_pl_lookup", need_pl_lookup_,
                "force_retry_congestion", force_retry_congested_,
                "route_mode", mysql_config_params_->server_routing_mode_, K_(rpc_trace_id));
      need_congestion_lookup_ = false;
    }

    if (OB_LIKELY(!need_congestion_lookup_)) { // no need congestion lookup
      set_state_and_call_next(RPC_REQ_CONGESTION_CONTROL_LOOKUP_DONE);
    } else {
      RPC_REQUEST_SM_SET_DEFAULT_HANDLER(&ObRpcRequestSM::state_congestion_control_lookup);

      if (NULL != pending_action_) {
        ret = OB_INNER_STAT_ERROR;
        LOG_EDIAG("setup_congestion_control_lookup, pending_action_ should be NULL",
                  K_(pending_action), K_(congestion_entry), K_(sm_id), K_(rpc_trace_id));
      } else {
        if (NULL != congestion_entry_) {
          LOG_WDIAG("congestion entry must be NULL here",
                  KPC_(congestion_entry), K_(sm_id), K_(rpc_trace_id));
          congestion_entry_->dec_ref();
          congestion_entry_ = NULL;
        }
        is_congestion_entry_updated_ = false;

        ObAction *congestion_control_action_handle = NULL;
        ObCongestionManager &congestion_manager = cluster_resource_->congestion_manager_;
        int64_t cr_version = cluster_resource_->version_;
        ret = congestion_manager.get_congest_entry(this, server_info.sql_addr_, cr_version,
            &congestion_entry_, congestion_control_action_handle);
        if (OB_SUCC(ret)) {
          if (NULL != congestion_control_action_handle) {
            pending_action_ = congestion_control_action_handle;
            LOG_DEBUG("ObRpcRequestSM::setup_congestion_control_lookup wait callback", KP_(pending_action), K_(rpc_trace_id));
          } else {
            congestion_lookup_success_ = true;
            set_state_and_call_next(RPC_REQ_CONGESTION_CONTROL_LOOKUP_DONE);
          }
        } else {
          LOG_WDIAG("failed to get congest entry", K_(sm_id), K(ret), K_(rpc_trace_id));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(rpc_req_)) {
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::state_congestion_control_lookup(int event, void *data)
{
  STATE_ENTER(ObRpcRequestSM::state_congestion_control_lookup, event);

  if (OB_LIKELY(is_valid_rpc_req())) {
    ObConnectionAttributes &server_info = rpc_req_->get_server_addr();
    if (CONGESTION_EVENT_CONTROL_LOOKUP_DONE == event) {
      congestion_lookup_success_ = true;
      if (OB_ISNULL(data)) {
        LOG_WDIAG("congestion entry is not found", "addr", server_info.sql_addr_, K_(sm_id), K_(rpc_trace_id));
      } else {
        congestion_entry_ = reinterpret_cast<ObCongestionEntry *>(data);
      }
    } else {
      congestion_lookup_success_ = false;
      LOG_EDIAG("unexpected event type, it should not happen", K(event), K(data),
                "addr", server_info.sql_addr_, K_(sm_id), K_(rpc_trace_id));
    }
  }

  pending_action_ = NULL;
  // call state_congestion_control_lookup_done() to handle fail / success
  set_state_and_call_next(RPC_REQ_CONGESTION_CONTROL_LOOKUP_DONE);

  return EVENT_DONE;
}

void ObRpcRequestSM::handle_congestion_entry_not_exist()
{
  if (NULL == congestion_entry_ && is_valid_rpc_req()) { // not in server list
    ObConnectionAttributes &server_info = rpc_req_->get_server_addr();
    LOG_INFO("congestion entry does not exist", "addr", server_info.sql_addr_, K_(pll_info), K_(rpc_trace_id));
    congestion_entry_not_exist_count_++;
    // 1. mark entry dirty
    if (pll_info_.set_dirty_all()) {
      //if dummy/target entry has clipped, we can update it here
      LOG_INFO("congestion entry doesn't exist, some route entry is expired, set it to dirty,"
               " and wait for updating", "congestion_entry_not_exist_count",
               congestion_entry_not_exist_count_,
               "addr", server_info.sql_addr_,
               "pll_info", pll_info_, K_(rpc_trace_id));
    }

    // 2. if all servers of the table entry are not in server list, just force renew
    // if dummy entry from rslist, no need force renew, will disconnect and delete cluster resource
    int64_t replica_count = pll_info_.replica_size();
    if ((replica_count > 0)
        && (congestion_entry_not_exist_count_ == replica_count)
        && !pll_info_.is_server_from_rslist()) {
      LOG_INFO("all servers of the route are not in server list, will force renew",
                K(replica_count), "congestion_entry_not_exist_count",
                congestion_entry_not_exist_count_, "route info", pll_info_.route_, K_(rpc_trace_id));
      pll_info_.set_force_renew(); // force renew
    }
  }
}

int ObRpcRequestSM::state_congestion_control_lookup_done()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.congestion_control_end_ = get_based_hrtime();
    cmd_time_stats_.congestion_control_time_ +=
      milestone_diff_new(milestones_.congestion_control_begin_, milestones_.congestion_control_end_);

    milestones_.congestion_process_begin_ = milestones_.congestion_control_end_;
    milestones_.congestion_process_end_ = 0;
  }

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::state_congestion_control_lookup_done get invalid rpc_req", K(ret), K_(rpc_trace_id));
  } else if (rpc_req_->canceled()) {
    LOG_INFO("ObRpcRequestSM::state_congestion_control_lookup_done get a canceled rpc_req", K_(rpc_req), K_(rpc_trace_id));
  } else if (OB_LIKELY(!need_congestion_lookup_)) { // no need congestion lookup
    set_state_and_call_next(RPC_REQ_REQUEST_REWRITE);
  } else if (!congestion_lookup_success_) { // congestion entry lookup failed
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("[ObRpcRequestSM::state_congestion_control_lookup_done]"
              "fail to lookup congestion entry, will return error", K_(rpc_trace_id));
  } else {
    bool is_in_alive_congested = false;
    bool is_in_dead_congested = false;
    bool is_in_detect_congested = false;

    ObCongestionEntry *cgt_entry = congestion_entry_;
    ObConnectionAttributes &server_info = rpc_req_->get_server_addr();

    if (NULL == cgt_entry) {
      CONGEST_INCREMENT_DYN_STAT(dead_congested_stat);
      // s.current_.state_ = ObRpcTransact::DEAD_CONGESTED;
      handle_congestion_entry_not_exist();
      set_state_and_call_next(RPC_REQ_REQUEST_RETRY);
    } else {
      is_in_dead_congested = cgt_entry->is_dead_congested();
      is_in_detect_congested =  cgt_entry->is_detect_congested();
      LOG_DEBUG("is_in_dead_congested", K(is_in_dead_congested), K(is_in_detect_congested), KPC(cgt_entry));
      if (!force_retry_congested_
          && is_in_dead_congested
          && pll_info_.is_strong_read()
          && pll_info_.is_leader_server()) {
        //only table entry is newer then congestion entry revalidate_time, we can treat it force_not_dead_congested
        if (cgt_entry->is_server_replaying()
            && cgt_entry->get_last_revalidate_time_us() <= pll_info_.get_last_valid_time_us()) {
          LOG_DEBUG("leader server is in REPLAY state, treat it not dead congested",
                    KPC(cgt_entry), "pll info", pll_info_, K_(rpc_trace_id));
          is_in_dead_congested = false;
        } else {
          // use mutex_
          PROCESSOR_INCREMENT_DYN_STAT(UPDATE_ROUTE_ENTRY_BY_CONGESTION);
          // when leader first router && (leader was congested from server or dead congested),
          // we need set forceUpdateTableEntry to avoid senseless of leader migrate
          if (pll_info_.set_target_dirty()) {
            LOG_INFO("leader is force_congested in strong read, set it to dirty "
                      "and wait for updating", "addr", server_info.sql_addr_,
                      "route", pll_info_.route_, K_(rpc_trace_id));
          }
        }
      }

      if (is_in_dead_congested) {
        //do nothing
      } else if (cgt_entry->is_force_alive_congested()) {
        is_in_alive_congested = true;
      } else if (cgt_entry->is_alive_congested()) {
        // Attention: if server is alive congested and need retry, we can retry it for this connection
        // and should update last congested time as punishment, otherwise leader
        // may be retried too many times in a short time if it is really blocked.
        // no need set is_congestion_entry_updated_ = true here,
        // if it is really alive, we will set entry alive after transaction is finished
        if (cgt_entry->alive_need_retry(get_hrtime())) {
          LOG_INFO("we can congested server for this connection,"
                    "and will expand its retry interval to avoid other connections use this server",
                    KPC(cgt_entry), K_(rpc_trace_id));
          cgt_entry->set_alive_failed_at(get_hrtime());
        } else {
          is_in_alive_congested = true;
        }
      }

      if (is_in_alive_congested || is_in_dead_congested) {
        LOG_DEBUG("target server is in congested",
                  "addr", server_info.sql_addr_,
                  K(is_in_dead_congested),
                  K(is_in_alive_congested),
                  "force_retry_congested", force_retry_congested_,
                  KPC(cgt_entry),
                  "pll info", pll_info_, K_(rpc_trace_id));
      }

      // 1. if the server is in congestion list and dead, we treat it as server_dead_fail
      // 2. if the server we chosen is not in congestion list, we also treat it as server_dead_fail
      if (!force_retry_congested_ && is_in_dead_congested) {
        // use mutex_
        CONGEST_INCREMENT_DYN_STAT(dead_congested_stat);
        // s.current_.state_ = ObRpcTransact::DEAD_CONGESTED; //need send request to other server(or base on dummy ldc)
        rpc_req_->congest_status_ = ObRpcReq::DEAD_CONGESTED;
        set_state_and_call_next(RPC_REQ_REQUEST_RETRY);
      } else if (!force_retry_congested_ && is_in_alive_congested) {
        // ObProxyMutex *mutex_ = s.sm_->mutex_;
        // use mutex_
        CONGEST_INCREMENT_DYN_STAT(alive_congested_stat);
        // s.current_.state_ = ObRpcTransact::ALIVE_CONGESTED;
        rpc_req_->congest_status_ = ObRpcReq::ALIVE_CONGESTED;
        set_state_and_call_next(RPC_REQ_REQUEST_RETRY);
      } else if (!force_retry_congested_ && is_in_detect_congested) {
        // s.current_.state_ = ObRpcTransact::DETECT_CONGESTED; //need send request to other server(or base on dummy ldc)
        rpc_req_->congest_status_ = ObRpcReq::DETECT_CONGESTED;
        set_state_and_call_next(RPC_REQ_REQUEST_RETRY);
      } else {
        set_state_and_call_next(RPC_REQ_REQUEST_REWRITE);
      }
    }
  }

  return ret;
}

bool ObRpcRequestSM::retry_server_connection_not_open()
{
  bool bret = false;
  // int ret = OB_SUCCESS;
  if (OB_LIKELY(is_valid_rpc_req()) && ObRpcReq::STATE_COMMON != rpc_req_->congest_status_) {
    //need to retry another server
    if (pll_info_.is_strong_read() && !rpc_req_->get_rpc_request_config_info().rpc_enable_reroute_) {
      // could not to use it
      LOG_WDIAG("request meet server failed, but not open rpc_enable_reroute, just to refound new"
                "partition again", K_(rpc_req), "congest_status", rpc_req_->congest_status_, K_(rpc_trace_id));
    } else if (pll_info_.replica_size() > 0) {
      int32_t attempt_count = 0;
      bool found_leader_force_congested = false;
      const ObProxyReplicaLocation *replica = pll_info_.get_next_avail_replica(
            force_retry_congested_, attempt_count, found_leader_force_congested);
      ObConnectionAttributes &server_info = rpc_req_->get_server_addr();
      if ((NULL == replica) && pll_info_.is_all_iterate_once()) { // next round
        // if we has tried all servers, do force retry in next round
        // s.pll_info_.reset_cursor();
        force_retry_congested_ = true;

        // get replica again
        replica = pll_info_.get_next_avail_replica();
      }
      if ((NULL != replica) && replica->is_valid()) {
        server_info.set_addr(ops_ip_sa_cast(replica->rpc_server_.get_sockaddr()));
        server_info.set_sql_addr(ops_ip_sa_cast(replica->server_.get_sockaddr()));
        bret = true;
      } else {
        LOG_EDIAG("request meet server failed, can not found avail replica, unexpected branch", KPC(replica), K_(rpc_trace_id));
        replica = NULL;
        force_retry_congested_ = true; 
      }
    } else {
      ObTableEntryName &te_name = pll_info_.te_name_;
      LOG_INFO("no replica, retry to get table location again, "
               "maybe used last session, ret_status = NOT_FOUND_EXISTING_ADDR", K(te_name), K_(rpc_trace_id));

    }

  }
  return bret;
}

void ObRpcRequestSM::retry_reset()
{
  retry_need_update_pl_ = false;
  pll_info_.route_.reset();
  if (OB_NOT_NULL(rpc_req_)) {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    obkv_info.rpc_request_retry_times_++;
    rpc_req_->retry_reset();
  }
  pll_info_.lookup_success_ = false;
  rpc_route_mode_ = ObRpcRouteMode::RPC_ROUTE_NORMAL; // 重试需要回退到normal模式
}

int ObRpcRequestSM::setup_rpc_request_retry()
{
  int ret = OB_SUCCESS;

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::setup_rpc_request_retry get invalid rpc_req", K(ret), K_(rpc_trace_id));
  } else if (rpc_req_->canceled()) {
    LOG_INFO("ObRpcRequestSM::setup_rpc_request_retry get a canceled rpc_req", K_(rpc_req), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    //update congestion info before retry
    if (OB_UNLIKELY(rpc_req_->is_server_failed()) && OB_NOT_NULL(congestion_entry_)) {
      congestion_entry_->set_dead_failed_at(event::get_hrtime());
      rpc_req_->set_server_failed(false); // reset retry info for next
    }
    LOG_DEBUG("setup_rpc_request_retry", "is_proxy_rpc_client", rpc_req_->is_inner_request(), K_(rpc_req), K(this), K_(rpc_trace_id));
    if (!obkv_info.is_rpc_req_can_retry()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ObRpcRequestSM::setup_rpc_request_retry can not retry", K_(obkv_info.rpc_request_retry_times), K(ret), K_(rpc_trace_id));
    } else if (obkv_info.is_need_retry_with_query_async()) {
      // retry with query async
      ObTableQueryAsyncEntry *query_async_entry = NULL;

      rpc_req_->retry_reset();  // 提前reset，后续会设置partition id以及table id
      if (OB_ISNULL(query_async_entry = obkv_info.query_async_entry_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("need_retry_with_query_async but query_async_entry is NULL", K(ret), K(obkv_info), K_(rpc_trace_id));
      } else if (!query_async_entry->is_first_query() || 0 != query_async_entry->get_server_query_session_id()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("retry with invalid query_async_entry", K(ret), K(obkv_info), KPC(query_async_entry), K_(rpc_trace_id));
      } else if (query_async_entry->is_partition_table() && OB_FAIL(query_async_entry->get_current_tablet_id(obkv_info.partition_id_))) {
        LOG_WDIAG("fail to get current tablet id", K(ret), KPC(query_async_entry), K_(rpc_trace_id));
      } else {
        query_async_entry->set_need_retry(false);
        pll_info_.route_.reset();
        pll_info_.lookup_success_ = false;
        LOG_DEBUG("ObRpcRequestSM::setup_rpc_request_retry retry with query async", KP_(rpc_req), "table_id", obkv_info.table_id_,
                  "partition_id", obkv_info.partition_id_, K_(rpc_trace_id));
        set_state_and_call_next(RPC_REQ_IN_PARTITION_LOOKUP);
      }
    } else {
      LOG_DEBUG("ObRpcRequestSM::setup_rpc_request_retry", KP_(rpc_req),
                "retry_times", obkv_info.rpc_request_retry_times_, K_(retry_need_update_pl), K_(rpc_trace_id));
      if (retry_need_update_pl_ && pll_info_.set_target_dirty()) {
        LOG_INFO("ObRpcRequestSM::setup_rpc_request_retry, the route entry is expired, "
                  "set it to dirty, and wait for updating",
                  "origin_name", pll_info_.te_name_,
                  "server_ip", rpc_req_->get_server_addr().addr_,
                  "route info", pll_info_.route_, K_(rpc_trace_id));
      }
      // For global index retry logic, there are mainly two situations here
       // 1. Use the main table routing to report error -10500, splice it into a global index table, and try again
       // 2. Using global index table routing, error -10500 is reported. This situation is usually caused by using the old cache. In this case, normal retry logic is used, and the main table routing is used.
      if (obkv_info.is_need_retry_with_global_index() && 0 != obkv_info.data_table_id_ && rpc_req_->get_rpc_request_config_info().rpc_enable_global_index_) {
        if (OB_FAIL(obkv_info.generate_index_table_name())) {
          LOG_WDIAG("fail to generate_index_table_name", K(ret), K_(rpc_trace_id));
        } else {
          LOG_DEBUG("retry by global index route", "index_table_name", obkv_info.index_table_name_, K_(rpc_trace_id));
          obkv_info.set_global_index_route(true);
        }
      } else {
        obkv_info.set_global_index_route(false);
      }

      if (OB_SUCC(ret)) {
        if (ObRpcRouteMode::RPC_ROUTE_NORMAL == rpc_route_mode_ && retry_server_connection_not_open()) {
          //retry other server to retry
          obkv_info.rpc_request_retry_times_++;
          set_state_and_call_next(RPC_REQ_SERVER_ADDR_SEARCHED);
        } else if (rpc_req_->is_inner_request()) {
          LOG_DEBUG("inner request retrying", KP(rpc_req_), K(obkv_info.inner_req_retries_));
          retry_reset();
          obkv_info.inner_req_retries_ ++;
          set_state_and_call_next(RPC_REQ_IN_PARTITION_LOOKUP);
        } else {
          ObTableQueryAsyncEntry *query_async_entry = NULL;
          if (OB_NOT_NULL(query_async_entry = obkv_info.query_async_entry_)) {
            query_async_entry->reset_tablet_ids();
            query_async_entry->set_server_query_session_id(0);
            query_async_entry->set_global_index_query(false);
            query_async_entry->reset_server_info();
          }
          retry_reset();
          if (obkv_info.is_hbase_request() && obkv_info.is_hbase_empty_family()) {
            already_get_tablegroup_entry_ = false;
            set_state_and_call_next(RPC_REQ_INNER_INFO_GET);
          } else {
            set_state_and_call_next(RPC_REQ_IN_PARTITION_LOOKUP);
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::setup_rpc_request_server_reroute()
{
  int ret = OB_SUCCESS;
  obkv::ObRpcTableMoveResponse *move_response= NULL;
  common::ObAddr old_server_addr;

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::setup_rpc_request_server_reroute get invalid rpc_req", K(ret), K_(rpc_trace_id));
  } else if (rpc_req_->canceled()) {
    LOG_INFO("ObRpcRequestSM::setup_rpc_request_server_reroute get a canceled rpc_req", K_(rpc_req), K_(rpc_trace_id));
  } else { 
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    if (!obkv_info.is_respo_reroute_info()
        || OB_ISNULL(move_response = dynamic_cast<obkv::ObRpcTableMoveResponse *>(rpc_req_->get_rpc_response()))
        || obkv_info.pcode_ != obrpc::OB_TABLE_API_MOVE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ObRpcRequestSM::setup_rpc_request_server_reroute invliad argument to handle", K(obkv_info), K_(rpc_trace_id));
    } else {
      ObConnectionAttributes &server_info = rpc_req_->get_server_addr();
      const obkv::ObTableMoveResult &move_result = move_response->get_move_result();
      const common::ObAddr &server_addr = move_result.get_replica_info().server_;
      pll_info_.set_target_dirty(); //need fetch from remote next in next request.
      rpc_req_->get_cur_server_addr(old_server_addr);

      if (OB_UNLIKELY(!server_addr.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("ObRpcRequestSM::setup_rpc_request_server_reroute", "server_addr", move_result.get_replica_info().server_, K_(rpc_trace_id));
      } else {
        LOG_DEBUG("[ObRpcRequestSM::setup_rpc_request_server_reroute] server reroute again",
                "server_addr", move_result.get_replica_info().server_, K_(rpc_trace_id));
        obkv_info.rpc_request_reroute_moved_times_++;
        server_info.set_addr(server_addr.get_ipv4(), static_cast<uint16_t>(server_addr.get_port()));
        //TODO check need update pl info if need pl lookup again.

        LOG_DEBUG("[ObRpcRequestSM::setup_rpc_request_server_reroute] chosen server",
                "old_addr", old_server_addr,
                "addr", server_info.addr_,
                "attempts", obkv_info.rpc_request_reroute_moved_times_,
                "sm_id", sm_id_, K_(rpc_trace_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    set_state_and_call_next(RPC_REQ_SERVER_ADDR_SEARCHED);
  } else {
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::setup_rpc_request_direct_load_route()
{
  int ret = OB_SUCCESS;
  obkv::ObRpcTableDirectLoadRequest *direct_load_req = NULL;
  common::ObAddr server_addr;

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::setup_rpc_request_direct_load get invalid rpc_req", K(ret), K_(rpc_trace_id));
  } else if (rpc_req_->canceled()) {
    LOG_INFO("ObRpcRequestSM::setup_rpc_request_direct_load get a canceled rpc_req", K_(rpc_req), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    if (!obkv_info.is_direct_load_req()
        || OB_ISNULL(direct_load_req = dynamic_cast<obkv::ObRpcTableDirectLoadRequest *>(rpc_req_->get_rpc_request()))
        || obkv_info.pcode_ != obrpc::OB_TABLE_API_DIRECT_LOAD
        || direct_load_req->is_begin_request()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ObRpcRequestSM::setup_rpc_request_direct_load invliad argument to handle", K(obkv_info), K_(rpc_trace_id));
    } else {
      ObConnectionAttributes &server_info = rpc_req_->get_server_addr();
      // const obkv::ObTableMoveResult &move_result = move_response->get_move_result();
      const obkv::ObTableDirectLoadRequestHeader &direct_load_request_header = direct_load_req->get_direct_load_request_header();
      const common::ObAddr &server_addr = direct_load_request_header.addr_;
      // pll_info_.set_target_dirty(); //need fetch from remote next in next request.
      // rpc_req_->get_cur_server_addr(old_server_addr);

      if (OB_UNLIKELY(!server_addr.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("ObRpcRequestSM::setup_rpc_request_direct_load", "server_addr", server_addr, K_(rpc_trace_id));
      } else {
        LOG_DEBUG("[ObRpcRequestSM::setup_rpc_request_direct_load] server direct route",
                "server_addr", direct_load_request_header.addr_, K_(rpc_trace_id));
        server_info.set_addr(server_addr.get_ipv4(), static_cast<uint16_t>(server_addr.get_port()));
      }
    }
  }

  if (OB_SUCC(ret)) {
    set_state_and_call_next(RPC_REQ_SERVER_ADDR_SEARCHED);
  } else {
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::state_rpc_server_request_rewrite()
{
  int ret = OB_SUCCESS;
  int64_t build_server_request_begin = 0;
  int64_t build_server_request_end = 0;
  LOG_DEBUG("handle state_rpc_server_request_rewrite", K_(rpc_req), K_(rpc_trace_id));
  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    build_server_request_begin = get_based_hrtime();
  }
  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("handle invalid rpc_req", K(ret), K_(rpc_trace_id));
  } else if (rpc_req_->canceled()) {
    LOG_INFO("ObRpcRequestSM::state_rpc_server_request_rewrite get a canceled rpc_req", K_(rpc_req), K_(rpc_trace_id));
  } else if (OB_SUCC(ObProxyRpcReqAnalyzer::handle_obkv_request_rewrite(*rpc_req_))) {
    if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
      build_server_request_end = get_based_hrtime();
      cmd_time_stats_.build_server_request_time_ += milestone_diff_new(build_server_request_begin, build_server_request_end);
      //reset before send request
      rpc_req_->server_timestamp_.reset();
      cmd_size_stats_.server_request_bytes_ = rpc_req_->get_request_len();
    }
    set_state_and_call_next(RPC_REQ_REQUEST_SERVER_SENDING);
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }
  LOG_DEBUG("handle state_rpc_server_request_rewrite over", K_(rpc_req), K(ret), K_(rpc_trace_id));
  return ret;
}

int ObRpcRequestSM::setup_rpc_send_request_to_server()
{
  int ret = OB_SUCCESS;

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::setup_rpc_send_request_to_server got an invalid rpc_req", K(ret), K_(rpc_req), K_(rpc_trace_id));
  } else if (rpc_req_->canceled()) {
    LOG_INFO("ObRpcRequestSM::setup_rpc_send_request_to_server get a canceled rpc_req", K_(rpc_req), K_(rpc_trace_id));
  } else {
    ObRpcServerNetTableEntry *server_table_entry = NULL;
    RPC_REQ_SM_ENTER_STATE(ObRpcReq::RpcReqSmState::RPC_REQ_SM_SERVER_HANDLE);

    if (OB_ISNULL(server_table_entry = get_rpc_server_net_handler_map(*execute_thread_).acquire_server_table_entry(*rpc_req_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to call acquire_server_table_entry", K(ret), KPC_(rpc_req), K_(rpc_trace_id));
    } else if (OB_FAIL(server_table_entry->add_req_to_waiting_link(rpc_req_))) {
      LOG_WDIAG("fail to call add_req_to_waiting_link", K(ret), K_(rpc_trace_id));
    } else if (OB_FAIL(server_table_entry->schedule_send_request_action())) {
      LOG_WDIAG("fail to call schedule_send_request_action", K(ret), K_(rpc_trace_id));
    } else {
      LOG_DEBUG("succ to add rpc req to waiting link and scheudle send action", KPC_(rpc_req), K_(rpc_trace_id));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::analyze_response(ObRpcReqAnalyzeNewStatus &status)
{
  int ret = OB_SUCCESS;

  int64_t analyze_response_begin = 0;

  ObProxyRpcReqAnalyzeCtx ctx;
  ctx.is_inner_request_ = rpc_req_->is_inner_request();
  ctx.is_response_ = true;
  if (OB_NOT_NULL(cluster_resource_)) {
    ctx.cluster_version_ = cluster_resource_->cluster_version_;
  } else {
    // login 请求不依赖version解析
    ctx.cluster_version_ = 0;
  }

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    analyze_response_begin = get_based_hrtime();
  }

  if (OB_FAIL(ObProxyRpcReqAnalyzer::analyze_rpc_req(ctx, status, *rpc_req_))) {
    status = RPC_ANALYZE_NEW_ERROR;
    LOG_WDIAG("fail to call analyze_rpc_req", K(ret), K(status), K_(rpc_trace_id));
  }

  if (OB_LIKELY(RPC_ANALYZE_NEW_DONE == status)) {
    LOG_DEBUG("handle response successed", K(rpc_req_), "get_rpc_response", rpc_req_->get_rpc_response(), K_(rpc_trace_id));
    if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
       cmd_time_stats_.server_response_analyze_time_ += milestone_diff_new(analyze_response_begin, get_based_hrtime());
    }

  } else if (RPC_ANALYZE_NEW_CONT == status)  {
    //TODO RPC need support next
  } else {
    // is not RPC_ANALYZE_NEW_DONE, do nothing
  }

  if (OB_FAIL(ret)) {
    status = RPC_ANALYZE_NEW_ERROR;
  }

  return ret;
}

int ObRpcRequestSM::handle_server_failed()
{
  int ret = OB_SUCCESS;

  if (!is_valid_rpc_req()) { //need same request which sm has handle
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("handle_server_failed get invalid argument", KP_(rpc_req), K(ret), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    #ifdef ERRSIM
    if (OB_SUCC(ret) && OB_FAIL(OB_E(EventTable::EN_RPC_NO_MASTER) OB_SUCCESS)) {
      ret = OB_SUCCESS;
      obkv_info.set_error_resp(true);
      obkv_info.rpc_origin_error_code_ = OB_NOT_MASTER;
    }
    #endif
    if (obkv_info.is_error() && obkv_info.is_resp()) {
      LOG_DEBUG("ObRpcRequestSM::handle_server_failed", "error_code", obkv_info.rpc_origin_error_code_, K_(rpc_trace_id));

      if (obkv_info.pcode_ == obrpc::OB_TABLE_API_DIRECT_LOAD) {
        LOG_INFO("ObRpcRequestSM::handle_server_failed direct load request receive server failed", "error_code",
                 obkv_info.rpc_origin_error_code_, K_(rpc_trace_id));
        //don't do any retry for OB_TABLE_API_DIRECT_LOAD request
        obkv_info.set_resp_reroute_info(false);
        obkv_info.set_need_retry(false);
        obkv_info.set_need_retry_with_global_index(false);
      }
      // For the -10500 error, there are two main situations:
      //  1. Use the main table routing to report error -10500, splice it into a global index table, and try again.
      //  2. Using global index table routing, error -10500 is reported. This situation is usually caused by using the old cache. In this case, normal retry logic is used, and the main table routing is used.
      else if (OB_ERR_KV_GLOBAL_INDEX_ROUTE == obkv_info.rpc_origin_error_code_) {
        if (!rpc_req_->get_rpc_request_config_info().rpc_enable_global_index_) {
          ret = OB_ERR_KV_GLOBAL_INDEX_ROUTE;
          LOG_WDIAG("Currently a global index error is returned but ODP disables global indexing", "error_code", obkv_info.rpc_origin_error_code_, K_(rpc_trace_id));
        } else if (!obkv_info.is_query_with_index()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WDIAG("Currently it is not an index query request but a related error is returned", "error_code", obkv_info.rpc_origin_error_code_, K_(rpc_trace_id));
        } else {
          LOG_INFO("ObRpcRequestSM::handle_server_failed get global index error", "error_code", obkv_info.rpc_origin_error_code_,
                   "data_table_id", obkv_info.data_table_id_, "table_id", obkv_info.table_id_, "idx_name", obkv_info.index_name_,
                   "is_global_index_route", obkv_info.is_global_index_route(), K_(rpc_trace_id));
          if (obkv_info.is_global_index_route()) {
            // dirtry index entry
            if (OB_NOT_NULL(obkv_info.index_entry_)) {
              obkv_info.index_entry_->cas_set_dirty_state();
              obkv_info.index_entry_->dec_ref();
              obkv_info.index_entry_ = NULL;
            }
            if (pll_info_.set_target_dirty()) {
              LOG_INFO("received not master error, the route entry is expired, "
                        "set it to dirty, and wait for updating",
                        "origin_name", pll_info_.te_name_,
                        "server_ip", rpc_req_->get_server_addr().addr_,
                        "route info", pll_info_.route_, K_(rpc_trace_id));
            }
            obkv_info.table_id_ = 0;
            obkv_info.data_table_id_ = 0;
            obkv_info.index_table_name_.reset();
            obkv_info.set_need_retry(true);
          } else {
            // With index table name, try again
            obkv_info.set_need_retry_with_global_index(true);
          }
        }
      // TODO: There may be many different errors in the future. The reroute flag is not set, but you need to update the routing information and try again. 
      //  1. table level.  2. partition level.
      } else if (OB_SCHEMA_ERROR == obkv_info.rpc_origin_error_code_
                 || OB_TABLE_NOT_EXIST == obkv_info.rpc_origin_error_code_
                 || OB_TABLET_NOT_EXIST == obkv_info.rpc_origin_error_code_
                 || OB_LS_NOT_EXIST == obkv_info.rpc_origin_error_code_
                 || (obrpc::OB_TABLE_API_LS_EXECUTE == obkv_info.pcode_
                     && OB_NOT_MASTER == obkv_info.rpc_origin_error_code_)) {
        LOG_INFO("ObRpcRequestSM::handle_server_failed get OB_SCHEMA_ERROR/OB_TABLE_NOT_EXIST", "error_code",
                 obkv_info.rpc_origin_error_code_, K_(rpc_trace_id));
        obkv_info.set_need_retry(true);
        obkv_info.set_route_entry_dirty();
        if (pll_info_.route_.set_table_entry_dirty()) { //schema changed
          get_pl_task_flow_controller().handle_new_task();
        }
      } else if (obkv_info.is_bad_routing()) {
        obkv_info.set_need_retry(true);
        obkv_info.set_route_entry_dirty();
        LOG_INFO("ObRpcRequestSM::handle_server_failed ", "error_code", obkv_info.rpc_origin_error_code_,
                  "is_inner_request", obkv_info.is_inner_request_,
                  "retry_count", obkv_info.rpc_request_retry_times_, K_(rpc_trace_id));
        // if received not master error, it means the partition locations of
        // the certain table entry has expired, so we need delay to update it;
        // dirtry index entry
        if (pll_info_.set_target_dirty()) {
          LOG_INFO("received not master error, the route entry is expired, "
                    "set it to dirty, and wait for updating",
                    "origin_name", pll_info_.te_name_,
                    "server_ip", rpc_req_->get_server_addr().addr_,
                    "route info", pll_info_.route_, K_(rpc_trace_id));
        }
      } else {
        switch (obkv_info.rpc_origin_error_code_)
        {
        case OB_LOCATION_LEADER_NOT_EXIST:
        case OB_NOT_MASTER:
        case OB_RS_NOT_MASTER:
        case OB_RS_SHUTDOWN:
        case OB_RPC_SEND_ERROR:
        case OB_RPC_POST_ERROR:
        case OB_PARTITION_NOT_EXIST:
        case OB_LOCATION_NOT_EXIST:
        case OB_PARTITION_IS_STOPPED:
        case OB_PARTITION_IS_BLOCKED:
        case OB_SERVER_IS_INIT:
        case OB_SERVER_IS_STOPPING:
        case OB_TENANT_NOT_IN_SERVER:
        case OB_TRANS_RPC_TIMEOUT:
          obkv_info.set_need_retry(true);
          obkv_info.set_route_entry_dirty();
          LOG_INFO("ObRpcRequestSM::handle_server_failed get route error_code", "error_code", obkv_info.rpc_origin_error_code_,
                    "is_inner_request", obkv_info.is_inner_request_,
                    "retry_count", obkv_info.rpc_request_retry_times_, K_(rpc_trace_id));
          // if received not master error, it means the partition locations of
          // the certain table entry has expired, so we need delay to update it;
          if (pll_info_.set_target_dirty()) {
            LOG_INFO("received not master error, the route entry is expired, "
                      "set it to dirty, and wait for updating",
                      "origin_name", pll_info_.te_name_,
                      "server_ip", rpc_req_->get_server_addr().addr_,
                      "route info", pll_info_.route_, K_(rpc_trace_id));
          }
        default:
          break;
        }
      }

      if (obkv_info.is_need_retry()) {
        obkv_info.rpc_request_retry_last_begin_ = get_based_hrtime();
      }
    }
  }

  return ret;
}

int ObRpcRequestSM::handle_rpc_response(bool &need_rewrite, bool &need_retry)
{
  int ret = OB_SUCCESS;
  need_rewrite = false;
  need_retry = false;
  obkv::ObProxyRpcType type = OB_ISNULL(rpc_req_) ? OBPROXY_RPC_UNKOWN : rpc_req_->get_rpc_type();

  switch (type) {
    case OBPROXY_RPC_OBRPC:
      ret = handle_obkv_response(need_rewrite, need_retry);
      break;
    case OBPROXY_RPC_HBASE:
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unknown rpc type need to handle", K(type), K_(rpc_trace_id));
      break;
  }
  return ret;
}

int ObRpcRequestSM::handle_obkv_login_response(bool &need_rewrite, bool &need_retry)
{
  int ret = OB_SUCCESS;
  ObRpcClientNetHandler *client_net_handler = NULL;
  UNUSED(need_rewrite);
  UNUSED(need_retry);

  if (!is_valid_rpc_req() || OB_ISNULL(client_net_handler = rpc_req_->get_cnet_sm())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc req is invalid", K(ret), KP(client_net_handler), K_(rpc_trace_id));
  } else {
    ObRpcReqCtx *rpc_ctx = NULL;
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    ObRpcTableLoginResponse *login_response = NULL;
    if (OB_ISNULL(rpc_ctx = obkv_info.rpc_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("handle login result but rpc_ctx is NULL", K(ret), K_(rpc_trace_id));
    } else if (OB_ISNULL(login_response = dynamic_cast<ObRpcTableLoginResponse *>(rpc_req_->get_rpc_response()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("handle login result but login_response is NULL", K(ret), K_(rpc_trace_id));
    } else {
      // decode and store credential
      int64_t pos = 0;

      const ObString &credential = login_response->get_credential();
      if (OB_FAIL(serialization::decode(credential.ptr(), credential.length(), pos, obkv_info.credential_))) {
        LOG_WDIAG("failed to serialize credential", K(ret), K(pos));
      } else {
        // store cluster version into client net
        client_net_handler->set_cluster_version(cluster_resource_->cluster_version_);

        // set credential
        rpc_ctx->credential_ = obkv_info.credential_;
        // add in global cache
        rpc_ctx->inc_ref();   //inc before add to cache
        if (OB_FAIL(get_global_rpc_req_ctx_cache().add_rpc_req_ctx_if_not_exist(*rpc_ctx, false))) {
          LOG_WDIAG("fail to add rpc ctx", KPC(rpc_ctx), K(ret));
          rpc_ctx->dec_ref();
        } else {
          LOG_DEBUG("succ to add rpc ctx into global cache", KPC(rpc_ctx), K_(rpc_trace_id));
        }
      }
    }
  }

  return ret;
}

int ObRpcRequestSM::handle_obkv_table_query_async_response(bool &need_rewrite, bool &need_retry)
{
  int ret = OB_SUCCESS;
  ObRpcTableQuerySyncResponse *query_response = NULL;

  if (!is_valid_rpc_req()
   || OB_ISNULL(query_response = dynamic_cast<ObRpcTableQuerySyncResponse *>(rpc_req_->get_rpc_response()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc req is invalid", K(ret), "req_is_valid", is_valid_rpc_req(), K(query_response), K_(rpc_trace_id));
  } else {
    LOG_DEBUG("handle_obkv_table_query_async_response for OB_TABLE_API_EXECUTE_QUERY_SYNC", K_(rpc_trace_id));
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    // 处理跨分区SyncQuery的标记，session id处理，标记处理
    ObTableQueryAsyncEntry *query_async_entry = NULL;
    if (OB_ISNULL(query_async_entry = obkv_info.query_async_entry_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_DEBUG("process ObRpcTableQuerySyncResponse get query_async_entry is NULL", K(ret), K(obkv_info), KPC(query_async_entry), K_(rpc_trace_id));
    } else {
      LOG_DEBUG("begin to process ObRpcTableQuerySyncResponse", KPC(query_async_entry), K_(rpc_trace_id));

      if (OB_SUCC(ret)) {
        /**
         * @brief
         * 1. If there is data, directly modify the flag to not end and return
         *  1.1 If it is the last partition with end, clean query_async_entry
         * 2. If there is no data
         *  2.1 If it is the last partition, set client_session_id and return, NOTICE：clean query_async_entry
         *  2.2 If it is not the last partition, you need to try again with the next partition.
         */
        bool is_data = 0 != query_response->get_query_result().get_row_count();
        bool is_end = query_response->get_query_result().is_end_;
        bool need_clean_query_info = false;

        // reset is_first
        query_async_entry->set_first_query(false);

        if (query_async_entry->is_single_query_request()) {
          // single
          if (is_end) {
            need_clean_query_info = true;   // clean query_async_entry
          } else {
            LOG_DEBUG("single async query result", "current server session id", query_async_entry->get_server_query_session_id(),
                        "response server session id", query_response->get_query_result().query_session_id_, K_(rpc_trace_id));
            query_async_entry->set_server_query_session_id(query_response->get_query_result().query_session_id_);
          }
        } else {
          // sharding
          if (is_data) {
            if (is_end) {
              if (query_async_entry->is_last_tablet()) {
                need_clean_query_info = true;   // clean query_async_entry
              } else {
                query_response->get_query_result().is_end_ = false;  // set not end
                query_async_entry->add_current_position();
                query_async_entry->set_server_query_session_id(0);
                query_async_entry->set_first_query(true);
                query_async_entry->reset_server_info();
                LOG_DEBUG("handle async response with", K(is_data), K(is_end), K_(rpc_trace_id));
              }
            } else {
              LOG_DEBUG("shard async query result", "current server session id", query_async_entry->get_server_query_session_id(),
                        "response server session id", query_response->get_query_result().query_session_id_, K_(rpc_trace_id));
              query_async_entry->set_server_query_session_id(query_response->get_query_result().query_session_id_);
            }
          } else {
            if (!is_end) {
              // This situation does not exist. The default is is_end to prevent the server from returning an exception and causing an obproxy exception.
              LOG_WDIAG("Async query get no data but response flag is not end", K(is_end), K(is_data), K(query_response), K_(rpc_trace_id));
              is_end = true;
            }
            if (query_async_entry->is_last_tablet()) {
              need_clean_query_info = true;
            } else {
              query_async_entry->add_current_position();
              query_async_entry->set_server_query_session_id(0);
              query_async_entry->set_first_query(true);
              query_async_entry->reset_server_info();
              query_async_entry->set_need_retry(true);
              need_retry = true;
            }
          }
        }

        query_response->get_query_result().query_session_id_ = query_async_entry->get_client_query_session_id();
        need_rewrite = true;

        if (need_clean_query_info) {
          query_async_entry->set_need_terminal(true);
          query_async_entry->set_deleted_state();
        }
        LOG_DEBUG("process ObRpcTableQuerySyncResponse done", KPC(query_async_entry), K(is_data), K(is_end), K(need_clean_query_info), K(need_rewrite), K_(rpc_trace_id));
      }
    }
  }

  return ret;
}

int ObRpcRequestSM::handle_obkv_response(bool &need_rewrite, bool &need_retry)
{
  int ret = OB_SUCCESS;
  ObRpcResponse *response = NULL;

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc req is invalid", K(ret), "req_is_valid", is_valid_rpc_req(), K(response), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    if (obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC == obkv_info.pcode_) {
      LOG_DEBUG("handle_obkv_response for OB_TABLE_API_EXECUTE_QUERY_SYNC", K_(rpc_trace_id));
      if (OB_ISNULL(response = rpc_req_->get_rpc_response())) {
        LOG_DEBUG("handle_obkv_response get NULL response, need not to rewrite", K(need_rewrite), K_(rpc_trace_id));
      } else if (OB_FAIL(handle_obkv_table_query_async_response(need_rewrite, need_retry))) {
        LOG_WDIAG("fail to call handle_obkv_table_query_async_response", K(ret), K(need_rewrite), K(need_retry), K_(rpc_trace_id));
      }
    } else if (obrpc::OB_TABLE_API_LOGIN == obkv_info.pcode_) {
      LOG_DEBUG("handle_obkv_response for OB_TABLE_API_LOGIN", K_(rpc_trace_id));
      if (OB_FAIL(handle_obkv_login_response(need_rewrite, need_retry))) {
        LOG_WDIAG("fail to call handle_obkv_login_response", K(ret), K(need_rewrite), K(need_retry), K_(rpc_trace_id));
      }
    } else if (obrpc::OB_TABLE_API_DIRECT_LOAD == obkv_info.pcode_) {
      need_retry = false; //not do any retry for direct_load request(it will be errored if retry)
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObRpcRequestSM::process_response(ObRpcReq *req)
{
  int ret = OB_SUCCESS;
  ObRpcReqAnalyzeNewStatus status = RPC_ANALYZE_NEW_CONT;
  bool need_rewrite = false;
  bool need_retry = false;

  if (!is_valid_rpc_req() || (req != rpc_req_)) { //need same request which sm has handle
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("process_response get invalid argument", KP(req), K(ret), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    LOG_DEBUG("ObRpcRequestSM::process_response req", KP(req), K_(rpc_trace_id));
    if (OB_FAIL(analyze_response(status))) {
      LOG_WDIAG("fail to call analyze_response", K(ret), K(status), KP(req), K_(rpc_trace_id));
    } else {
      cmd_size_stats_.server_response_bytes_ += req->get_response_len();

      switch (__builtin_expect(status, RPC_ANALYZE_NEW_DONE)) {
      case RPC_ANALYZE_NEW_OBPARSE_ERROR:
        ret = OB_ERR_UNEXPECTED;
        //must read all data of the request, otherwise will read the remain request data when recv next request
        // TODO: 返回error
        break;
      case RPC_ANALYZE_NEW_ERROR:
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("error parsing client request", K(ret), K_(sm_id), K_(rpc_trace_id));
        // set_client_abort(ObRpcRequestSM::ABORTED, event);
        break;
      case RPC_ANALYZE_NEW_CONT:
        ret = OB_ERR_UNEXPECTED;
        //TODO RPC SERVICE not all request has bean read, need read it next
        break;
      case RPC_ANALYZE_NEW_DONE: {
        if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
          cmd_size_stats_.client_response_bytes_ = rpc_req_->get_response_len();
        }

        LOG_DEBUG("[RPC_REQUEST] analyze rpc_req response done", K(ret), KPC(req), K_(rpc_trace_id));

        if (OB_FAIL(handle_server_failed())) {
          LOG_WDIAG("fail to call handle_server_failed", K(ret), K_(rpc_trace_id));
        } else if (obkv_info.is_respo_reroute_info()) {
          //need reroute base on server reroute info
          set_state_and_call_next(RPC_REQ_RESPONSE_SERVER_REROUTING);
        } else if (obkv_info.is_rpc_req_can_retry()) {
          need_retry = true;
          LOG_DEBUG("[ObRpcRequestSM::process_response] get error from server",
              "error_code", obkv_info.rpc_origin_error_code_,
              "retry_times", obkv_info.rpc_request_retry_times_, K_(rpc_trace_id));
        } else if (req->is_inner_request()) {
          /* inner request need to schedule it to complete task */
          LOG_DEBUG("inner request done, callback", KPC_(rpc_req), K_(rpc_trace_id));
          if (OB_FAIL(inner_request_callback(ASYNC_PROCESS_DONE_EVENT))) {
            LOG_WDIAG("fail to call inner_request_callback", K(ret), K_(rpc_trace_id));
          }
        } else {
          if (OB_FAIL(handle_rpc_response(need_rewrite, need_retry))) {
            LOG_WDIAG("fail to call handle_rpc_response", K(ret), K_(rpc_trace_id));
          } else {
            if (obkv_info.need_add_into_cache_ && OB_NOT_NULL(obkv_info.index_entry_)) {
              if (OB_FAIL(get_global_index_cache().add_index_entry(*obkv_info.index_entry_, false))) {
                LOG_WDIAG("fail to add index entry, not retrun error", KPC_(obkv_info.index_entry), K(ret), K_(rpc_trace_id));
                ret = OB_SUCCESS;
              } else {
                obkv_info.index_entry_->inc_ref();
                LOG_DEBUG("succ to add index entry into index cache", KPC_(obkv_info.index_entry), K_(rpc_trace_id));
              }
            }

            if (need_retry) {
              // do nothing
            } else if (need_rewrite) {  // no need retry, just rewrite response and return
              set_state_and_call_next(RPC_REQ_RESPONSE_REWRITE);
            } else { // no need retry, just retrun
              set_state_and_call_next(RPC_REQ_RESPONSE_CLIENT_SENDING);
            }
          }
        } 

        /**
         * @brief
         *  1. route error
         *    1.1. waiting rpc_request_retry_waiting_time_ns and retry
         *  2. not route error
         *    2.1. is_need_retry_with_global_index
         *    2.2. is_need_retry_with_query_async
         *    2.3. retry imm
         */
        if (OB_SUCC(ret) && need_retry) {
          const int64_t rpc_request_retry_waiting_time_us = rpc_req_->get_rpc_request_config_info().rpc_request_retry_waiting_time_;
          LOG_DEBUG("[ObRpcRequestSM::process_response] rpc need retry",
              "error_code", obkv_info.rpc_origin_error_code_,
              "retry_times", obkv_info.rpc_request_retry_times_,
              K(rpc_request_retry_waiting_time_us), K_(rpc_trace_id));

          retry_need_update_pl_ = false;   // pl updated in handle_server_failed function. Only internal retry situations need to set this variable to true.
          if (obkv_info.is_need_retry()) {
            int64_t rpc_request_retry_waiting_time_ns = HRTIME_USECONDS(rpc_request_retry_waiting_time_us);
            // route error, waiting rpc_request_retry_waiting_time_ns
            if (OB_FAIL(schedule_call_next_action(RPC_REQ_REQUEST_RETRY, rpc_request_retry_waiting_time_ns))) {
              LOG_WDIAG("fail to call schedule_call_next_action", K(ret));
            }
          } else {
            // 1. is_need_retry_with_global_index
            // 2. is_need_retry_with_query_async
            if (OB_FAIL(schedule_call_next_action(RPC_REQ_REQUEST_RETRY))) {
              LOG_WDIAG("fail to call schedule_call_next_action", K(ret));
            }
          }
        }
        break;
      }
      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_EDIAG("unknown analyze rpc request status", K(status), K_(sm_id), K(ret), K_(rpc_trace_id));
        break;
      }
    }
  }
  return ret;
}

int ObRpcRequestSM::state_rpc_client_response_rewrite()
{
  int ret = OB_SUCCESS;
  int64_t rewrite_client_response_begin = 0;
  int64_t rewrite_client_response_end = 0;
  LOG_DEBUG("handle state_rpc_client_response_rewrite", K_(rpc_req), K_(rpc_trace_id));
  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    rewrite_client_response_begin = get_based_hrtime();
  }
  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("handle invalid rpc_req", K(ret), K_(rpc_trace_id));
  } else if (rpc_req_->canceled()) {
    LOG_INFO("ObRpcRequestSM::state_rpc_client_response_rewrite get a canceled rpc_req", K_(rpc_req), K_(rpc_trace_id));
  } else if (OB_SUCC(ObProxyRpcReqAnalyzer::handle_obkv_response_rewrite(*rpc_req_))) {
    if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
      rewrite_client_response_end = get_based_hrtime();
      cmd_time_stats_.server_response_analyze_time_ += milestone_diff_new(rewrite_client_response_begin, rewrite_client_response_end);
    }
    set_state_and_call_next(RPC_REQ_RESPONSE_CLIENT_SENDING);
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }
  LOG_DEBUG("handle state_rpc_client_response_rewrite over", K_(rpc_req), K(ret), K_(rpc_trace_id));
  return ret;
}

int ObRpcRequestSM::setup_rpc_internal_build_response()
{
  int ret = OB_SUCCESS;
  ObRpcClientNetHandler *client_net_handler = NULL;

  if (OB_FAIL(cancel_timeout_action())) {
    LOG_WDIAG("fail to cancel timeout action", K(ret), K_(sm_id), K(this), K_(rpc_trace_id));
  } else if (OB_FAIL(cancel_pending_action())) {
    LOG_WDIAG("fail to cancel pending action", K(ret), K_(sm_id), K(this), K_(rpc_trace_id));
  } else if (!is_valid_rpc_req() || rpc_req_->is_inner_request()) {  // 子任务当前不支持走到这个逻辑
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc return error get invalid rpc_req", K_(sm_id), K(this), K(ret), K_(rpc_trace_id));
  } else if (OB_ISNULL(client_net_handler = reinterpret_cast<ObRpcClientNetHandler *>(rpc_req_->cnet_sm_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc return error get NULL client_net_handler", K_(sm_id), K(this), K(ret), K_(rpc_trace_id));
  } else if (rpc_req_->get_cnet_state() > ObRpcReq::ClientNetState::RPC_REQ_CLIENT_RESPONSE_HANDLING) {
    // do nothing
    LOG_WDIAG("rpc timeout in packet return status, do nothing", K_(sm_id), K(this), KPC_(rpc_req), K_(rpc_trace_id));
  } else {
    RPC_REQ_SM_ENTER_STATE(ObRpcReq::RpcReqSmState::RPC_REQ_SM_RESPONE_RETURN);
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    if (obkv_info.is_empty_query_result()){
      LOG_DEBUG("return empty query result packet to client", K_(rpc_trace_id));
      if (OB_FAIL(ObProxyRpcReqAnalyzer::build_empty_query_response(*rpc_req_))) {
        LOG_WDIAG("failed to build error response", K(ret), K_(rpc_trace_id));
      } else if (OB_FAIL(ObProxyRpcReqAnalyzer::handle_obkv_serialize_response(*rpc_req_))) {
        LOG_WDIAG("invalid to serialize inner error response", K_(sm_id), K(ret), K_(rpc_trace_id));
      } else {
        client_net_handler->add_client_response_request(rpc_req_);
        client_net_handler->schedule_send_response_action();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("The current logic does not support internal return packets", K(ret), KPC_(rpc_req), KP(this), K_(rpc_trace_id));
    }
  }

  if (OB_FAIL(ret)) {
    // 内部回包错误，不再跳转到错误包回包流程，等待超时
    LOG_WDIAG("fail to call ObRpcRequestSM::setup_rpc_internal_build_response", K(ret), K_(rpc_trace_id));
  }

  return ret;
}

int ObRpcRequestSM::setup_process_response()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(rpc_req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::setup_process_response get a NULL rpc req", K(ret), K_(rpc_trace_id));
  } else if (rpc_req_->canceled()) {
    LOG_INFO("ObRpcRequestSM::setup_rpc_send_response_to_client get a canceled rpc_req", K_(rpc_req), K_(rpc_trace_id));
  } else {
    RPC_REQ_SM_ENTER_STATE(ObRpcReq::RpcReqSmState::RPC_REQ_SM_RESPONE_RETURN);

    if (OB_FAIL(process_response(rpc_req_))) {
      LOG_WDIAG("fail to call process_response", K(ret), K_(rpc_trace_id));
    }

    if (OB_FAIL(ret)) {
      // process response error, dirty route
      LOG_INFO("setup_process_response process response error, dirty route", K_(rpc_trace_id));
      pll_info_.set_target_dirty();
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::setup_rpc_send_response_to_client()
{
  int ret = OB_SUCCESS;
  ObRpcClientNetHandler *client_net_handler = NULL;

  if (!is_valid_rpc_req()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("handle invalid rpc_req", K(ret), K_(rpc_trace_id));
  } else if (OB_ISNULL(client_net_handler = reinterpret_cast<ObRpcClientNetHandler *>(rpc_req_->cnet_sm_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc return error get NULL client_net_handler", K_(sm_id), K(this), K(ret), K_(rpc_trace_id));
  } else {
    LOG_DEBUG("succ to call setup_rpc_send_response_to_client", KP_(rpc_req), K_(rpc_trace_id));
    client_net_handler->add_client_response_request(rpc_req_);
    client_net_handler->schedule_send_response_action();
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::setup_rpc_handle_shard_request()
{
  int ret = OB_SUCCESS;
  optimizer::ObProxyShardRpcReqRequestPlan *execute_plan = NULL;
  engine::ObProxyRpcReqOperator *operator_root = NULL;
  ObHRTime execute_timeout = HRTIME_USECONDS(timeout_us_);
  LOG_DEBUG("setup_rpc_handle_shard_request get timeout", K_(timeout_us), K_(rpc_trace_id));

  RPC_REQ_SM_ENTER_STATE(ObRpcReq::RpcReqSmState::RPC_REQ_SM_SHARDING_HANDLE);
  if (OB_ISNULL(rpc_req_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("ObRpcRequestSM::setup_rpc_handle_shard_request get a NULL rpc req", K_(rpc_trace_id));
  } else if (OB_FAIL(optimizer::ObProxyShardRpcReqRequestPlan::handle_shard_rpc_request(rpc_req_))) {
    LOG_WDIAG("invalid to handle shard rpc request plan", K(ret), K_(rpc_trace_id));
  } else if (OB_ISNULL(execute_plan = reinterpret_cast<optimizer::ObProxyShardRpcReqRequestPlan *>(rpc_req_->execute_plan_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("ObRpcRequestSM::setup_rpc_handle_shard_request get a NULL execute_plan", K_(rpc_trace_id));
  } else if (OB_ISNULL(operator_root = execute_plan->get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("operator should not be null", K_(sm_id), K(ret), K_(rpc_trace_id));
  } else if (OB_NOT_NULL(sharding_action_)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("sharding_action must be NULL here", K_(pending_action), K_(sm_id), K(ret), K_(rpc_trace_id));
  } else if (OB_FAIL(operator_root->open(this, sharding_action_, hrtime_to_msec(execute_timeout)))) {
    LOG_WDIAG("fail to open operator", K_(sm_id), K(execute_timeout), K(ret), K_(rpc_trace_id));
  } else if (OB_FAIL(operator_root->execute_rpc_request())) {
    LOG_WDIAG("fail to get next row", K_(sm_id), K(ret), K_(rpc_trace_id));
  } else if (OB_ISNULL(sharding_action_)) {
    LOG_WDIAG("sharding_action should not be null", K_(sm_id), K(ret), K_(rpc_trace_id));
  } else if (OB_FAIL(cancel_timeout_action())) {
    LOG_WDIAG("fail to cancel_timeout_action", K(ret), K_(rpc_trace_id));
  } else {
    rpc_req_->set_snet_state(ObRpcReq::ServerNetState::RPC_REQ_SERVER_SHARDING_REQUEST_HANDLING);
    //TODO need update sharding request info
    RPC_REQUEST_SM_SET_DEFAULT_HANDLER(&ObRpcRequestSM::state_rpc_handle_shard_request);
    LOG_DEBUG("ObRpcRequestSM::setup_rpc_handle_shard_request wait callback", KP_(sharding_action), K_(rpc_trace_id));
  }

  if (OB_FAIL(ret)) {
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  }

  return ret;
}

int ObRpcRequestSM::state_rpc_handle_shard_request(int event, void *data)
{
  int ret = OB_SUCCESS;

  STATE_ENTER(ObRpcRequestSM::state_rpc_handle_shard_request, event);

  optimizer::ObProxyShardRpcReqRequestPlan* plan = NULL;
  engine::ObProxyRpcReqOperator* operator_root = NULL;
  sharding_action_ = NULL;
  bool need_clean_rpc_req = false;

  switch (event) {
    case VC_EVENT_READ_COMPLETE:
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WDIAG("data is NULL", K_(sm_id), K(ret), K_(rpc_trace_id));
      } else if (rpc_req_ != reinterpret_cast<ObRpcReq *>(data)) {
        ret = OB_ERR_UNEXPECTED; //invalid request to handle
        LOG_WDIAG("invalid data to handle", K_(sm_id), K(ret), K_(rpc_trace_id));
      } else if (rpc_req_->canceled()) {
        LOG_DEBUG("sharding rpc_req is canceled by client net or timeout", KPC_(rpc_req), K_(rpc_trace_id));
        need_clean_rpc_req = true;
      } else {
        ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
        ObRpcResponse *rpc_response = NULL;

        LOG_DEBUG("to handle data return", K_(sm_id), K(ret), K(data), K_(rpc_trace_id));
        if (OB_FAIL(ObProxyRpcReqAnalyzer::handle_obkv_serialize_response(*(rpc_req_)))) {
          LOG_WDIAG("invalid to serialize inner sharding response", K_(sm_id), K(ret), K_(rpc_trace_id));
        } else if (obkv_info.is_inner_req_retrying() && rpc_req_->is_inner_request()) {
          LOG_DEBUG("inner request retrying done, callback", KPC_(rpc_req), K_(rpc_trace_id));
          if (OB_NOT_NULL(rpc_response = rpc_req_->get_rpc_response()) && rpc_response->get_result_code().rcode_ != 0) {
            obkv_info.set_error_resp(true);
            obkv_info.rpc_origin_error_code_ = rpc_response->get_result_code().rcode_;
            LOG_WDIAG("retrying shard request meet error, return error response", "error code", obkv_info.rpc_origin_error_code_,
                      K_(rpc_trace_id));
          }

          if (OB_FAIL(inner_request_callback(ASYNC_PROCESS_DONE_EVENT))) {
            LOG_WDIAG("fail to callback", K(ret), KPC(rpc_req_), K_(rpc_trace_id));
          }
        } else if (OB_NOT_NULL(rpc_req_->cnet_sm_) && rpc_req_->get_cnet_state() <= ObRpcReq::ClientNetState::RPC_REQ_CLIENT_RESPONSE_HANDLING) {
          ObRpcClientNetHandler *client_net_handler = rpc_req_->get_cnet_sm();
          obkv_info.set_resp_completed(true);
          if (OB_NOT_NULL(rpc_response = rpc_req_->get_rpc_response()) && rpc_response->get_result_code().rcode_ != 0) {
            obkv_info.set_error_resp(true);
            obkv_info.rpc_origin_error_code_ = rpc_response->get_result_code().rcode_;
            LOG_WDIAG("shard request meet error, return error response", "error code", obkv_info.rpc_origin_error_code_, K_(rpc_trace_id));
          }
          client_net_handler->add_client_response_request(rpc_req_);
          client_net_handler->schedule_send_response_action();
        } else {
          LOG_DEBUG("client request has been canceled by client", KPC_(rpc_req), K_(rpc_trace_id)); //cancle it to free req
        }
      }
      break;
    case VC_EVENT_READ_READY:
      if (OB_ISNULL(data)) {
        LOG_DEBUG("it is not used for client rpc request", K(data), K_(rpc_trace_id));
      } else {
        LOG_DEBUG("it is not used for client rpc request", K(data), K_(rpc_trace_id));
      }
      break;
    case VC_EVENT_EOS:
    case VC_EVENT_ACTIVE_TIMEOUT:
    case VC_EVENT_INACTIVITY_TIMEOUT:
      ret = OB_ERR_KV_ODP_TIMEOUT;
      LOG_WDIAG("state_rpc_handle_shard_request get timeout event", K_(sm_id), K(ret),
                "event", ObRpcReqDebugNames::get_event_name(event), K_(rpc_trace_id), K_(timeout_us));
      break;
    case VC_EVENT_ERROR:
      LOG_WDIAG("state_rpc_handle_shard_request get error event", K_(sm_id), K(ret), K_(rpc_trace_id));
      ret = OB_ERR_UNEXPECTED;
      break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("Unexpected event", K_(sm_id), K(ret), "event", ObRpcReqDebugNames::get_event_name(event), K_(rpc_trace_id));
      break;
    }
  }

  if (OB_ISNULL(plan = reinterpret_cast<optimizer::ObProxyShardRpcReqRequestPlan *>(rpc_req_->execute_plan_))) {
    LOG_WDIAG("select log plan should not be null", K_(sm_id), K(ret), K_(rpc_trace_id));
  } else if (OB_ISNULL(operator_root = plan->get_plan_root())) {
    LOG_WDIAG("operator should not be null", K_(sm_id), K(ret), K_(rpc_trace_id));
  } else {
    operator_root->close();
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(rpc_req_)) {
      //init proxy error to rpc_req
      rpc_req_->set_rpc_req_error_code(ret);
    }
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR);
  } else {
    // 如果cleanup action不为空，说明有其他线程调度释放
    if (need_clean_rpc_req && OB_ISNULL(cleanup_action_)) {
      rpc_req_->set_clean_module(ObRpcReq::RpcReqCleanModule::RPC_REQ_CLEAN_MODULE_REQUEST_SM);
      set_state_and_call_next(RPC_REQ_REQUEST_CLEANUP);
    }
  }

  return VC_EVENT_NONE;
}

int ObRpcRequestSM::inner_request_callback(int event)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(create_thread_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("create_thread is NULL, invalid", K(ret), K_(rpc_trace_id));
  } else if (OB_ISNULL(inner_cont_)) {
    if (ASYNC_PROCESS_DONE_EVENT == event) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ObRpcRequestSM::inner_request_callback with ASYNC_PROCESS_DONE_EVENT, but inner_cont is NULL", KP_(inner_cont), K(event), K_(rpc_trace_id));
    } else {
      LOG_DEBUG("ObRpcRequestSM::inner_request_callback with inner_cont is NULL", K(event), K_(rpc_trace_id));
    }
  } else {
    LOG_DEBUG("ObRpcRequestSM::inner_request_callback", K(event), K_(rpc_trace_id));
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(inner_cont_)) {
    create_thread_->schedule_imm(inner_cont_, event); //task has done
    inner_cont_ = NULL;
  }

  return ret;
}

int ObRpcRequestSM::state_rpc_req_inner_request_cleanup()
{
  int ret = OB_SUCCESS;
  cleanup_action_ = NULL;
  LOG_DEBUG("ObRpcRequestSM::state_rpc_req_inner_request_cleanup clean cleanup_action", KP_(cleanup_action), K_(rpc_trace_id));

  if (OB_ISNULL(rpc_req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::state_rpc_req_inner_request_cleanup get an invalid rpc_req", KPC(rpc_req_), K_(rpc_trace_id));
  } else if (OB_FAIL(cancel_timeout_action())) {
    LOG_WDIAG("fail to cancel timeout action", K(ret), K_(sm_id), K(this), K_(rpc_trace_id));
  } else if (OB_FAIL(cancel_pending_action())) {
    LOG_WDIAG("fail to cancel pending action", K(ret), K_(sm_id), K(this), K_(rpc_trace_id));
  } else if (OB_FAIL(cancel_call_next_action())) {
    LOG_WDIAG("fail to cancel call next action", K(ret), K_(sm_id), K(this), K_(rpc_trace_id));
  } else {
    RPC_REQ_SM_ENTER_STATE(ObRpcReq::RpcReqSmState::RPC_REQ_SM_INNER_REQUEST_CLEANUP);

    ObRpcReq::RpcReqCleanModule &rpc_clean_module = rpc_req_->get_clean_module();
    ObRpcReq::ClientNetState rpc_client_net_state = rpc_req_->get_cnet_state();
    ObRpcReq::ServerNetState rpc_server_net_state = rpc_req_->get_snet_state();
    ObRpcReq::RpcReqSmState rpc_request_net_state = rpc_req_->get_sm_state();

    LOG_DEBUG("enter ObRpcRequestSM::state_rpc_req_inner_request_cleanup", KP_(rpc_req), "client_net_state", ObRpcReqDebugNames::get_client_state_name(rpc_client_net_state),
              "server_net_state", ObRpcReqDebugNames::get_server_state_name(rpc_server_net_state), "request_sm_state", ObRpcReqDebugNames::get_rpc_sm_state_name(rpc_request_net_state),
              "rpc_clean_module", ObRpcReqDebugNames::get_rpc_clean_module_name(rpc_clean_module), K_(rpc_trace_id));

    if (ObRpcReq::RpcReqCleanModule::RPC_REQ_CLEAN_MODULE_MAX == rpc_clean_module) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ObRpcRequestSM::state_rpc_req_inner_request_cleanup get an invalid rpc_clean_module", K(rpc_clean_module), K(ret), K_(rpc_trace_id));
    } else {
      rpc_clean_module = ObRpcReq::RpcReqCleanModule::RPC_REQ_CLEAN_MODULE_MAX;
      rpc_req_->cancel_request(); // If a module is cleaned, a cancel request is required.
      if (0 != rpc_req_->get_sub_rpc_req_array_size()) {
        LOG_DEBUG("sharding inner rpc req clean all sub rpc reqs",
                  "sub_rpc_req_count", rpc_req_->get_sub_rpc_req_array_size(),
                  "is_inner_request", rpc_req_->is_inner_request(),
                  K_(rpc_trace_id));
        rpc_req_->clean_sub_rpc_req(); // 开始处理子任务的释放流程
      }
      if (rpc_req_->could_cleanup_inner_request()) {
        // clean inner request memory
        inner_request_cleanup();
        rpc_req_->inner_request_cleanup();
        if (OB_FAIL(inner_request_callback(ASYNC_PROCESS_DESTROY_SELF_EVENT))) {
          // do nothing
          LOG_WDIAG("fail to call inner_request_callback", K(ret), K_(rpc_trace_id));
        }

        rpc_clean_module = ObRpcReq::RpcReqCleanModule::RPC_REQ_CLEAN_MODULE_REQUEST_SM;
        if (OB_ISNULL(create_thread_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("rpc_request_sm create_thread is NULL", KPC_(rpc_req), K_(rpc_trace_id));
        } else if (create_thread_ == execute_thread_) {
          // 异步任务执行线程和创建线程相同，直接调用即可
          LOG_DEBUG("just call set_state_and_call_next to RPC_REQ_REQUEST_CLEANUP", KP_(cleanup_action), K(this), KP_(rpc_req), K_(rpc_trace_id));
          set_state_and_call_next(RPC_REQ_REQUEST_CLEANUP);
        } else {
          if (OB_NOT_NULL(cleanup_action_)){
            LOG_EDIAG("cleanup_action must be NULL", K_(cleanup_action), K(ret), K(this), KP_(rpc_req), K_(rpc_trace_id));
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_ISNULL(cleanup_action_ = create_thread_->schedule_imm(this, RPC_REQUEST_SM_CLEANUP))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_EDIAG("fail to schedule cleanup", K_(cleanup_action), K(ret));
          } else {
            //success
            LOG_DEBUG("succ to schedule cleanup action", KP_(cleanup_action), K(this), KP_(rpc_req), K_(rpc_trace_id));
          }
        }
      } else {
        LOG_DEBUG("could not clean inner rpc_req", KP_(rpc_req),
                  "client_net_ready_to_cleanup", !(rpc_client_net_state > ObRpcReq::ClientNetState::RPC_REQ_CLIENT_INIT && rpc_client_net_state < ObRpcReq::ClientNetState::RPC_REQ_CLIENT_DONE),
                  "server_net_ready_to_cleanup", !(rpc_server_net_state > ObRpcReq::ServerNetState::RPC_REQ_SERVER_INIT && rpc_server_net_state < ObRpcReq::ServerNetState::RPC_REQ_SERVER_DONE),
                  "request_sm_ready_to_cleanup", !(rpc_request_net_state > ObRpcReq::RpcReqSmState::RPC_REQ_SM_INIT && rpc_request_net_state < ObRpcReq::RpcReqSmState::RPC_REQ_SM_INNER_REQUEST_CLEANUP),
                  K_(rpc_trace_id));
      }
    }
  }

  // 这里报错理论上不太合理，可能是内存异常，不回包和释放处理
  if (OB_FAIL(ret)) {
    LOG_EDIAG("fail to call ObRpcRequestSM::state_rpc_req_inner_request_cleanup, maybe memory error or memory leak", K(ret), KPC_(rpc_req), K_(rpc_trace_id));
  }

  return ret;
}

int ObRpcRequestSM::state_rpc_req_cleanup()
{
  int ret = OB_SUCCESS;
  cleanup_action_ = NULL;
  LOG_DEBUG("ObRpcRequestSM::state_rpc_req_cleanup clean cleanup_action", KP_(cleanup_action), K_(rpc_trace_id));

  if (OB_ISNULL(rpc_req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObRpcRequestSM::state_rpc_req_cleanup get an invalid rpc_req", KPC(rpc_req_), K_(rpc_trace_id));
  } else if (OB_FAIL(cancel_timeout_action())) {
    LOG_WDIAG("fail to cancel timeout action", K(ret), K_(sm_id), K(this), K_(rpc_trace_id));
  } else if (OB_FAIL(cancel_pending_action())) {
    LOG_WDIAG("fail to cancel pending action", K(ret), K_(sm_id), K(this), K_(rpc_trace_id));
  } else if (OB_FAIL(cancel_sharding_action())) {
    LOG_WDIAG("fail to cancel sharding action", K(ret), K_(sm_id), K(this), K_(rpc_trace_id));
  } else if (OB_FAIL(cancel_call_next_action())) {
    LOG_WDIAG("fail to cancel call next action", K(ret), K_(sm_id), K(this), K_(rpc_trace_id));
  } else {
    RPC_REQ_SM_ENTER_STATE(ObRpcReq::RpcReqSmState::RPC_REQ_SM_REQUEST_CLEANUP);

    if (OB_UNLIKELY(rpc_req_->is_server_failed()) && OB_NOT_NULL(congestion_entry_)) {
      congestion_entry_->set_dead_failed_at(event::get_hrtime());
      rpc_req_->set_server_failed(false); // reset retry info for next
    }
    ObRpcReq::RpcReqCleanModule rpc_clean_module = rpc_req_->get_clean_module();
    ObRpcReq::ClientNetState rpc_client_net_state = rpc_req_->get_cnet_state();
    ObRpcReq::ServerNetState rpc_server_net_state = rpc_req_->get_snet_state();
    ObRpcReq::RpcReqSmState rpc_request_net_state = rpc_req_->get_sm_state();

    LOG_DEBUG("enter ObRpcRequestSM::state_rpc_req_cleanup", KP_(rpc_req), "client_net_state", ObRpcReqDebugNames::get_client_state_name(rpc_client_net_state),
              "server_net_state", ObRpcReqDebugNames::get_server_state_name(rpc_server_net_state), "request_sm_state", ObRpcReqDebugNames::get_rpc_sm_state_name(rpc_request_net_state),
              "rpc_clean_module", ObRpcReqDebugNames::get_rpc_clean_module_name(rpc_clean_module), K_(rpc_trace_id));

    if (ObRpcReq::RpcReqCleanModule::RPC_REQ_CLEAN_MODULE_MAX == rpc_clean_module) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ObRpcRequestSM::state_rpc_req_cleanup get an invalid rpc_clean_module", K(rpc_clean_module), K(ret), K_(rpc_trace_id));
    } else {
      rpc_clean_module = ObRpcReq::RpcReqCleanModule::RPC_REQ_CLEAN_MODULE_MAX;
      rpc_req_->set_clean_module(rpc_clean_module);
      rpc_req_->cancel_request(); // If a module is cleaned, a cancel request is required.

      if (0 != rpc_req_->get_sub_rpc_req_array_size()) {
        LOG_DEBUG("sharding rpc req clean all sub rpc reqs",
                  "sub_rpc_req_count", rpc_req_->get_sub_rpc_req_array_size(),
                  "is_inner_request", rpc_req_->is_inner_request(), K_(rpc_trace_id));
        rpc_req_->clean_sub_rpc_req(); // 开始处理子任务的释放流程
      }

      if (rpc_req_->could_release_request()) {
        LOG_DEBUG("will clean rpc req", K_(cleanup_action), K_(rpc_req), K(this), K_(rpc_trace_id));
        set_state_and_call_next(RPC_REQ_REQUEST_DONE);
      } else {
        LOG_DEBUG("could not destroy rpc_req", KP_(rpc_req),
                  "client_net_ready_to_destroy", !(rpc_client_net_state > ObRpcReq::ClientNetState::RPC_REQ_CLIENT_INIT && rpc_client_net_state < ObRpcReq::ClientNetState::RPC_REQ_CLIENT_DONE),
                  "server_net_ready_to_destroy", !(rpc_server_net_state > ObRpcReq::ServerNetState::RPC_REQ_SERVER_INIT && rpc_server_net_state < ObRpcReq::ServerNetState::RPC_REQ_SERVER_DONE),
                  "request_sm_ready_to_destroy", !(rpc_request_net_state > ObRpcReq::RpcReqSmState::RPC_REQ_SM_INIT && rpc_request_net_state < ObRpcReq::RpcReqSmState::RPC_REQ_SM_REQUEST_DONE),
                  K_(rpc_trace_id));
      }
    }
  }

  // 这里报错理论上不太合理，可能是内存异常，不回包和释放处理
  if (OB_FAIL(ret)) {
    LOG_EDIAG("fail to call ObRpcRequestSM::state_rpc_req_cleanup, maybe memory error or memory leak", K(ret), KPC_(rpc_req), K_(rpc_trace_id));
  }

  return ret;
}

int ObRpcRequestSM::state_rpc_req_done()
{
  int ret = OB_SUCCESS;
  RPC_REQ_SM_ENTER_STATE(ObRpcReq::RpcReqSmState::RPC_REQ_SM_REQUEST_DONE);

  // TODO：这里收集统计信息，整个请求结束, 将rpc_req返回给NetSM，请求结束
  update_cmd_stats();

  if (OB_ISNULL(rpc_req_)) {
    LOG_WDIAG("rpc_req is NULL, can not to destroy", K_(rpc_trace_id));
  } else {
    if (rpc_req_->is_inner_request()) {
      ObRpcReq *root_rpc_req = rpc_req_->get_root_rpc_req();
      int64_t &sub_rpc_req_count = root_rpc_req->get_current_sub_rpc_req_count();

      LOG_DEBUG("ObRpcRequestSM::state_rpc_req_done with inner request", K(sub_rpc_req_count), KP(root_rpc_req), K_(rpc_req), K_(rpc_trace_id));
      if (sub_rpc_req_count <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("sub_rpc_req_count must be greater than 0", K(sub_rpc_req_count), K_(rpc_trace_id));
      } else if (0 == --sub_rpc_req_count) {
        LOG_DEBUG("ObRpcRequestSM::state_rpc_req_done callback to root_rpc_req cleanup", K(sub_rpc_req_count), KP(root_rpc_req), K(this), K_(rpc_req), K_(rpc_trace_id));
        ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_SHARDING_REQUEST_DONE);
        // TODO:考虑释放主请求时加锁，因为这里也跨线程访问了，子线程调用cleanup释放父线程
        root_rpc_req->cleanup(cleanup_params);
      }
    } else {
      LOG_DEBUG("ObRpcRequestSM::state_rpc_req_done with normal request", K_(rpc_req), K_(rpc_trace_id));
    }

    rpc_req_->destroy();
    rpc_req_ = NULL;
  }

  terminate_sm_ = true;  // will call kill this in main_handler, this func must call by handle_event(RPC_REQUEST_SM_DONE)
  return ret;
}

int ObRpcRequestSM::setup_rpc_return_error()
{
  int ret = OB_SUCCESS;
  ObRpcClientNetHandler *client_net_handler = NULL;
  LOG_DEBUG("rpc build error response and return to client", KPC_(rpc_req), K_(rpc_trace_id));

  if (OB_FAIL(cancel_timeout_action())) {
    LOG_WDIAG("fail to cancel timeout action", K(ret), K_(sm_id), K(this), K_(rpc_trace_id));
  } else if (OB_FAIL(cancel_pending_action())) {
    LOG_WDIAG("fail to cancel pending action", K(ret), K_(sm_id), K(this), K_(rpc_trace_id));
  } else if (OB_ISNULL(rpc_req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc return error get NULL rpc_req", K_(sm_id), K(this), K_(rpc_trace_id));
  } else if (OB_ISNULL(client_net_handler = reinterpret_cast<ObRpcClientNetHandler *>(rpc_req_->cnet_sm_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc return error get NULL client_net_handler", K_(sm_id), K(this), K_(rpc_trace_id));
  } else if (rpc_req_->get_cnet_state() > ObRpcReq::ClientNetState::RPC_REQ_CLIENT_RESPONSE_HANDLING) {
    // do nothing
    LOG_WDIAG("rpc timeout in packet return status, do nothing", K_(sm_id), K(this), KPC_(rpc_req), K_(rpc_trace_id));
  } else {
    RPC_REQ_SM_ENTER_STATE(ObRpcReq::RpcReqSmState::RPC_REQ_SM_INNER_ERROR);
    // clear in setup_rpc_return_error
    //   1. clear query async entry
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    if (OB_NOT_NULL(obkv_info.query_async_entry_)) {
      obkv_info.query_async_entry_->set_deleted_state();
      obkv_info.query_async_entry_->dec_ref();
      obkv_info.query_async_entry_ = NULL;
    }
    if (0 == rpc_req_->get_rpc_req_error_code()) {
      LOG_WDIAG("invalid to get error code is 0", K(ret), K_(rpc_req), K_(rpc_trace_id));
      rpc_req_->set_rpc_req_error_code(OB_ERR_UNEXPECTED);
    }
    obkv_info.set_error_resp(true);
    obkv_info.rpc_origin_error_code_ = rpc_req_->get_rpc_req_error_code();

    if (OB_FAIL(ObProxyRpcReqAnalyzer::build_error_response(*rpc_req_, rpc_req_->get_rpc_req_error_code()))) {
      LOG_WDIAG("failed to build error response", K(ret), K_(rpc_trace_id));
    } else if (OB_FAIL(ObProxyRpcReqAnalyzer::handle_obkv_serialize_response(*rpc_req_))) {
      LOG_WDIAG("invalid to serialize inner error response", K_(sm_id), K(ret), K_(rpc_trace_id));
    } else if (rpc_req_->is_inner_request()) {
      LOG_DEBUG("inner request error, callback", KPC_(rpc_req), K_(rpc_trace_id));
      if (OB_FAIL(inner_request_callback(ASYNC_PROCESS_DONE_EVENT))) {
        LOG_WDIAG("fail to call inner_request_callback", K(ret), K_(rpc_trace_id));
      }
    } else {
      client_net_handler->add_client_response_request(rpc_req_);
      client_net_handler->schedule_send_response_action();
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_ISNULL(rpc_req_)) {
      // 这里不去清理request sm，因为当前不会存在rpc req被置空，然后再走到这里的逻辑，如果直接释放可能存在内存问题
      LOG_WDIAG("call setup_rpc_return_error but rpc req is NULL", K(ret), K_(rpc_trace_id));
    } else {
      rpc_req_->set_clean_module(ObRpcReq::RpcReqCleanModule::RPC_REQ_CLEAN_MODULE_REQUEST_SM);
      if (OB_FAIL(schedule_cleanup_action())) {
        LOG_WDIAG("fail to schedule_cleanup_action", K(ret), K_(rpc_trace_id));
      }
    }
  }
  return ret;
}

void ObRpcRequestSM::handle_timeout()
{
  LOG_INFO("rpc_request handle timeout directly", KPC_(rpc_req), K(this), K_(timeout_us), K_(rpc_trace_id));
  int ret = OB_SUCCESS;
  bool need_forward_error = true;
  if (OB_FAIL(cancel_timeout_action())) {
    LOG_WDIAG("fail to cancel timeout action", K(ret), K_(sm_id), K(this), K_(rpc_trace_id));
  } else if (OB_FAIL(cancel_pending_action())) {
    LOG_WDIAG("fail to cancel pending action", K(ret), K_(sm_id), K(this), K_(rpc_trace_id));
  } else if (!is_valid_rpc_req()) {
    // to kill itself to free RpcRequestSM object in error request response
    LOG_WDIAG("invalid rpc_req to handle", KPC_(rpc_req), K_(rpc_trace_id));
    need_forward_error = false;
  } else {
    RPC_REQ_SM_ENTER_STATE(ObRpcReq::RpcReqSmState::RPC_REQ_SM_TIMEOUT);
    // If an exception occurs before the timeout, the previous error code is returned.
    if (0 == rpc_req_->get_rpc_req_error_code()) {
      rpc_req_->set_rpc_req_error_code(OB_ERR_KV_ODP_TIMEOUT);
    }
    rpc_req_->cancel_request();
    if (rpc_req_->get_cnet_state() == ObRpcReq::ClientNetState::RPC_REQ_CLIENT_RESPONSE_HANDLING) {
      /* response has in sending state, not forwart error response again,
        do nothing in this situation(not need to kill this for it will be do after
        response has been sended) */
      need_forward_error = false;
    }
  }
  if (need_forward_error) {
    set_state_and_call_next(RPC_REQ_REQUEST_ERROR); //handle the error pkt
  }
}

void ObRpcRequestSM::update_cmd_stats()
{
  if (OB_NOT_NULL(mysql_config_params_) && OB_NOT_NULL(rpc_req_)) {

    cmd_time_stats_.client_request_read_time_ =
      milestone_diff_new(rpc_req_->client_timestamp_.client_begin_, rpc_req_->client_timestamp_.client_read_end_);
    cmd_time_stats_.client_response_write_time_ =
      milestone_diff_new(rpc_req_->client_timestamp_.client_write_begin_, rpc_req_->client_timestamp_.client_end_);

    cmd_time_stats_.server_request_write_time_ =
      milestone_diff_new(rpc_req_->server_timestamp_.server_write_begin_, rpc_req_->server_timestamp_.server_write_end_);
    cmd_time_stats_.server_response_read_time_ =
      milestone_diff_new(rpc_req_->server_timestamp_.server_read_begin_, rpc_req_->server_timestamp_.server_read_end_);
    cmd_time_stats_.server_process_request_time_ =
      milestone_diff_new(rpc_req_->server_timestamp_.server_write_begin_, rpc_req_->server_timestamp_.server_read_begin_);
    cmd_time_stats_.prepare_send_request_to_server_time_ =
      milestone_diff_new(rpc_req_->client_timestamp_.client_begin_, rpc_req_->server_timestamp_.server_write_begin_);

    // client_close will not be assigned properly in some exceptional situation.
    // TODO: Assign client_close with suitable value when ObMysqlTunnel terminates abnormally.
    if (0 == rpc_req_->client_timestamp_.client_end_ && rpc_req_->client_timestamp_.client_read_end_ > 0) {
      rpc_req_->client_timestamp_.client_end_ = get_based_hrtime();
      rpc_req_->client_timestamp_.client_end_ = rpc_req_->client_timestamp_.client_end_;
    }

    cmd_time_stats_.request_total_time_ =
      milestone_diff_new(rpc_req_->client_timestamp_.client_begin_, rpc_req_->client_timestamp_.client_end_);

    int64_t slow_time_threshold = mysql_config_params_->slow_query_time_threshold_;
    int64_t proxy_process_time_threshold = mysql_config_params_->slow_proxy_process_time_threshold_;
    const char *SLOW_QUERY = "Slow RPC Query: ";
    const char *xf_head = NULL;
    const char *log_head = NULL;
    bool print_info_log = false;
    bool rpc_req_has_retried = rpc_req_->get_obkv_info().is_request_has_retried();

    if (slow_time_threshold > 0 && slow_time_threshold < cmd_time_stats_.request_total_time_) {
      xf_head = XFH_RPC_SLOW_QUERY;
      log_head = SLOW_QUERY;
    } else if (proxy_process_time_threshold > 0 && proxy_process_time_threshold < cmd_time_stats_.prepare_send_request_to_server_time_) {
      xf_head = XFH_RPC_SLOW_QUERY;
      log_head = SLOW_QUERY;
      print_info_log = true;
    } else if (rpc_req_->get_rpc_request_config_info().rpc_enable_retry_request_info_log_ && rpc_req_has_retried) {
      xf_head = XFH_RPC_SLOW_QUERY;
      log_head = SLOW_QUERY; 
      print_info_log = true;
    } else {
      //do nothing
    }
    if (OB_NOT_NULL(log_head)) {
      ObString user_name;
      ObString tenant_name;
      ObString table_name;
      ObString cluster_name;
      ObString logic_tenant_name;
      ObString logic_database_name;
      obrpc::ObRpcPacketCode pcode = obrpc::OB_INVALID_RPC_CODE;

      ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
      user_name = obkv_info.user_name_;
      tenant_name = obkv_info.tenant_name_;
      cluster_name = obkv_info.cluster_name_;
      logic_database_name = obkv_info.database_name_;
      table_name = obkv_info.table_name_;
      pcode = obkv_info.pcode_;
      cmd_retry_stats_.request_retry_times_  = obkv_info.rpc_request_retry_times_;
      cmd_retry_stats_.request_move_reroute_times_ = obkv_info.rpc_request_reroute_moved_times_;
      cmd_retry_stats_.request_retry_consume_time_us_ = 
        hrtime_to_usec(milestone_diff_new(rpc_req_->client_timestamp_.client_begin_, obkv_info.rpc_request_retry_last_begin_));

      if (print_info_log) {
        //print info
        // ObRpcReqTraceId &trace_id = obkv_info
        LOG_INFO(log_head,
                "client_ip", obkv_info.client_info_.addr_,
                "server_ip", obkv_info.server_info_.addr_,
                "obproxy_client_port", obkv_info.server_info_.obproxy_addr_,
                "server_trace_id", rpc_trace_id_,
                "route_type", get_route_type_string(pll_info_.route_.cur_chosen_route_type_),
                K(user_name),
                K(tenant_name),
                K(cluster_name),
                K(logic_database_name),
                K(logic_tenant_name),
                "rpc_table_name", table_name,
                K_(cmd_size_stats),
                K_(cmd_retry_stats),
                K_(cmd_time_stats),
                K(pcode));
      } else {
        LOG_WDIAG(log_head,
                "client_ip", obkv_info.client_info_.addr_,
                "server_ip", obkv_info.server_info_.addr_,
                "obproxy_client_port", obkv_info.server_info_.obproxy_addr_,
                "server_trace_id", rpc_trace_id_,
                "route_type", get_route_type_string(pll_info_.route_.cur_chosen_route_type_),
                K(user_name),
                K(tenant_name),
                K(cluster_name),
                K(logic_database_name),
                K(logic_tenant_name),
                "rpc_table_name", table_name,
                K_(cmd_size_stats),
                K_(cmd_retry_stats),
                K_(cmd_time_stats),
                K(pcode));
      }

      if (NULL != xf_head) {
        OBPROXY_XF_LOG(INFO, xf_head,
                      "client_ip", obkv_info.client_info_.addr_,
                      "server_ip", obkv_info.server_info_.addr_,
                      "obproxy_client_port", obkv_info.server_info_.obproxy_addr_,
                      "server_trace_id", rpc_trace_id_,
                      "route_type", get_route_type_string(pll_info_.route_.cur_chosen_route_type_),
                      K(user_name),
                      K(tenant_name),
                      K(cluster_name),
                      K(logic_database_name),
                      K(logic_tenant_name),
                      "rpc_table_name", table_name,
                      K(pcode),
                      K_(cmd_size_stats),
                      K_(cmd_retry_stats),
                      K_(cmd_time_stats));
      }
    }

    update_monitor_log();
  }
}

inline void ObRpcRequestSM::get_monitor_error_info(int32_t &error_code, ObString &error_msg, bool &is_error_resp)
{
  UNUSED(error_msg);
  LOG_DEBUG("ObRpcRequestSM::get_monitor_error_info", K_(reentrancy_count), K_(terminate_sm), K_(rpc_req), K_(rpc_trace_id));
  if (OB_LIKELY(OB_NOT_NULL(rpc_req_))) {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();

    if (obkv_info.is_resp_completed() || obkv_info.is_error() || rpc_req_->get_rpc_req_error_code() != 0) {
      if (obkv_info.is_error()) {
        is_error_resp = true;
        error_code = obkv_info.get_error_code();
      } else if (rpc_req_->get_rpc_req_error_code() != 0) {
        is_error_resp = true;
        error_code = rpc_req_->get_rpc_req_error_code();
      }
    } else { //invalid rpc_req_sm or request to calc
      if (0 != cmd_size_stats_.client_request_bytes_) {
        is_error_resp = true;
        error_code = ER_QUERY_INTERRUPTED;
      }
    }
  }
}

inline void ObRpcRequestSM::update_monitor_stats(const ObString &logic_tenant_name,
                                                 const ObString &logic_database_name,
                                                 const ObString &cluster_name,
                                                 const ObString &tenant_name,
                                                 const ObString &database_name,
                                                 const DBServerType database_type,
                                                 const ObProxyBasicStmtType stmt_type,
                                                 char *error_code_str)
{
  int ret = OB_SUCCESS;

  ObString error_code(error_code_str);
  ObTenantStatItem *item = NULL;
  if (OB_FAIL(get_global_tenant_stat_mgr().get_or_create_item(logic_tenant_name,
                                                              logic_database_name,
                                                              cluster_name, tenant_name,
                                                              database_name, database_type,
                                                              stmt_type, error_code,
                                                              item))) {
    if (OB_EXCEED_MEM_LIMIT == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WDIAG("fail to get or create tenant stat item", K(ret), K_(rpc_trace_id));
    }
  } else if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("tenant stat item is null, it should not happened", K(logic_tenant_name), K(logic_database_name),
             K(cluster_name), K(tenant_name), K(database_name), K(ret), K_(rpc_trace_id));
  } else {
    int64_t low_threshold = get_global_proxy_config().monitor_stat_low_threshold;
    int64_t middle_threshold = get_global_proxy_config().monitor_stat_middle_threshold;
    int64_t high_threshold = get_global_proxy_config().monitor_stat_high_threshold;

    DRWLock::RDLockGuard lock(item->get_lock());
    item->atomic_add(STAT_TOTAL_COUNT);
    item->atomic_add(STAT_TOTAL_TIME, hrtime_to_usec(cmd_time_stats_.request_total_time_));
    item->atomic_add(STAT_PREPARE_TIME, hrtime_to_usec(cmd_time_stats_.prepare_send_request_to_server_time_));
    item->atomic_add(STAT_SERVER_TIME, hrtime_to_usec(cmd_time_stats_.server_process_request_time_));

    if (cmd_time_stats_.request_total_time_ >= high_threshold) {
      item->atomic_add(STAT_HIGH_COUNT);
    } else if (cmd_time_stats_.request_total_time_ >= middle_threshold) {
      item->atomic_add(STAT_MIDDLE_COUNT);
    } else if (cmd_time_stats_.request_total_time_ >= low_threshold) {
      item->atomic_add(STAT_LOW_COUNT);
    }

    LOG_DEBUG("succ to update tenant stats", K(logic_tenant_name), K(logic_database_name),
              K(cluster_name), K(tenant_name), K(database_name),
              K(database_type), K(error_code), KPC(item), K_(rpc_trace_id));
    get_global_tenant_stat_mgr().revert_item(item);
  }
  
}

#define MONITOR_LOG_FORMAT "%.*s,%.*s,%.*s," \
                        "%.*s,%.*s:%.*s:%.*s,%s," \
                        "%.*s,%.*s,%s,%s,%s,%s,"  \
                        RPC_REQUEST_INFO_FORMAT ","   \
                        "%ldus,%ldus,%dus,%ldus," \
                        TRACE_ID_FORMAT ",%.*s,%s," \
                        "%.*s,%s,%s"

#define MONITOR_ERROR_LOG_FORMAT MONITOR_LOG_FORMAT ",%.*s"

#define MONITOR_LOG_PARAM \
          logic_tenant_name.length(), logic_tenant_name.ptr(),     \
          ant_trace_id.length(), ant_trace_id.ptr(),           \
          rpc_id.length(), rpc_id.ptr(),               \
                                                       \
          logic_database_name.length(), logic_database_name.ptr(), \
          cluster_name.length(), cluster_name.ptr(),   \
          tenant_name.length(), tenant_name.ptr(),     \
          database_name.length(), database_name.ptr(), \
          database_type_str,                           \
                                                       \
          logic_table_name.length(), logic_table_name.ptr(),     \
          table_name.length(), table_name.ptr(),       \
          protocol_name,          /*sql_cmd */         \
          protocol_request_name,  /* stmt_type_str */  \
          is_error_resp? "failed" : "success",         \
          is_error_resp? error_code_str : "",          \
          is_rpc_shard_request? "shard" : "single", rpc_request_table_id, rpc_request_partition_id, \
          absolute_net_time, client_net_wait_time, \
                                                       \
          hrtime_to_usec(cmd_time_stats_.request_total_time_),       \
          hrtime_to_usec(cmd_time_stats_.prepare_send_request_to_server_time_),  \
          0,                                           \
          hrtime_to_usec(cmd_time_stats_.server_process_request_time_),    \
                                                                           \
          trace_id_0, trace_id_1,                      \
          server_trace_info.length(), server_trace_info.ptr(), "",   \
          shard_name.length(), shard_name.ptr(),       \
          is_enc_beyond_trust ? "1" : "0",             \
          ip_port_buff

#define MONITOR_ERROR_LOG_PARAM MONITOR_LOG_PARAM, error_msg.length(), error_msg.ptr()

void ObRpcRequestSM::update_monitor_log()
{
  // ObMySQLCmd request_cmd = COM_END;
  obrpc::ObRpcPacketCode pcode = obrpc::OB_INVALID_RPC_CODE;
  // ObRpcRequestSM::ObRpcService rpc_serivce = ObRpcRequestSM::ObRpcService::OB_RPC_UNINITED;
  obkv::ObProxyRpcType rpc_type = obkv::ObProxyRpcType::OBPROXY_RPC_UNKOWN;

  if (OB_NOT_NULL(rpc_req_)
      && OB_NOT_NULL(rpc_req_->get_rpc_request())
      && !rpc_req_->is_inner_request()) {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    int64_t slow_time_threshold = mysql_config_params_->slow_query_time_threshold_;
    int64_t query_digest_time_threshold = mysql_config_params_->query_digest_time_threshold_;
    // int64_t rpc_req_timeout_us = timeout_us_;
    bool is_error_resp = false;
    int error_code = 0;
    ObString error_msg;
    pcode = obkv_info.pcode_;
    rpc_type = rpc_req_->get_rpc_type();
    bool is_shard_request = rpc_req_->get_obkv_info().is_shard();
    get_monitor_error_info(error_code, error_msg, is_error_resp);

    if ((query_digest_time_threshold > 0 && query_digest_time_threshold < cmd_time_stats_.request_total_time_)
        || (slow_time_threshold > 0 && slow_time_threshold < cmd_time_stats_.request_total_time_)
        || is_error_resp
        || get_global_proxy_config().enable_monitor_stat
        || (get_global_proxy_config().enable_qos)) {

      bool is_rpc_shard_request = obkv_info.is_shard();
      int64_t rpc_request_table_id = obkv_info.table_id_;
      int64_t rpc_request_partition_id = obkv_info.partition_id_;
      int64_t absolute_net_time = 0;
      int64_t client_net_wait_time = 0;
      UNUSED(absolute_net_time);
      UNUSED(client_net_wait_time);

      ObString logic_tenant_name = ObString::make_empty_string();
      ObString logic_database_name = ObString::make_empty_string();
      if (OB_UNLIKELY(logic_tenant_name.empty())) {
        logic_tenant_name.assign_ptr(get_global_proxy_config().app_name_str_,
                                     static_cast<int32_t>(STRLEN(get_global_proxy_config().app_name_str_)));
      }

      const ObString &cluster_name = obkv_info.cluster_name_;
      const ObString &tenant_name = obkv_info.tenant_name_;
      const ObString &database_name = obkv_info.database_name_;
      const ObString &user_name = obkv_info.user_name_;
      const ObString &table_name = obkv_info.table_name_;
      UNUSED(user_name);
      ObProxyBasicStmtType stmt_type = ObRpcRequestSM::get_rpc_basic_stmt_type(pcode);
      bool is_slow_query = false;

      char error_code_str[OB_MAX_ERROR_CODE_LEN] = "\0";
      const DBServerType database_type = DB_OB_MYSQL;

      if ((query_digest_time_threshold > 0 && query_digest_time_threshold < cmd_time_stats_.request_total_time_)
          || (slow_time_threshold > 0 && slow_time_threshold < cmd_time_stats_.request_total_time_)
          || is_error_resp) {
        const ObString ant_trace_id = ObString::make_empty_string();
        const ObString rpc_id = ObString::make_empty_string();
        uint32_t client_channel_id = rpc_req_->get_client_channel_id();
        uint32_t server_channel_id = rpc_req_->get_server_channel_id();
        UNUSED(client_channel_id);
        UNUSED(server_channel_id);
        const char *database_type_str = ObProxyMonitorUtils::get_database_type_name(database_type);
        const ObString logic_table_name = ObString::make_empty_string();

        // for compatible sharding mode
        bool is_enc_beyond_trust = false;
        ObString shard_name = ObString::make_empty_string(); //none for RPC

        ObString protocol_name_str = get_rpc_type_string(rpc_type);
        const char *protocol_name = protocol_name_str.ptr();
        const char *protocol_request_name = obrpc::ObRpcPacketSet::name_of_pcode(pcode); //protocol_request_name_str.ptr();

        const uint64_t *trace_id = ObCurTraceId::get();
        uint64_t trace_id_0 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[0];
        uint64_t trace_id_1 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[1];

        char server_trace_buf[NEW_TRACE_ID_BUF_LENTH] = "/0";
        ObCurNewTraceId::NewTraceId new_trace_id;
        new_trace_id.set(rpc_req_->get_rpc_request()->get_packet_meta().get_rpc_header().trace_id_[1],  //sequnence
                         rpc_req_->get_rpc_request()->get_packet_meta().get_rpc_header().trace_id_[0]); //ip & port + user_request +reserverd
        new_trace_id.to_string(server_trace_buf, NEW_TRACE_ID_BUF_LENTH);

        ObString server_trace_info = ObString::make_string(server_trace_buf); //
        // uint64_t server_trace_info_len = server_trace_info.length();

        if (is_error_resp) {
          snprintf(error_code_str, OB_MAX_ERROR_CODE_LEN, "%d", error_code);
        }

        char ip_port_buff[net::INET6_ADDRPORTSTRLEN] = "\0";
        net::ObIpEndpoint &server_addr = obkv_info.server_info_.addr_;
        if (ops_is_ip(server_addr)) {
          char ip_buff_temp[INET6_ADDRSTRLEN] = "\0";
          snprintf(ip_port_buff, net::INET6_ADDRPORTSTRLEN, "%s:%u", ops_ip_ntop(server_addr, ip_buff_temp, INET6_ADDRSTRLEN), ops_ip_port_host_order(server_addr));
        }

        if ((slow_time_threshold > 0 && slow_time_threshold < cmd_time_stats_.request_total_time_)
            || (query_digest_time_threshold > 0 && query_digest_time_threshold < cmd_time_stats_.request_total_time_)
            || is_error_resp) {
          _OBPROXY_DIGEST_LOG(INFO, MONITOR_LOG_FORMAT, MONITOR_LOG_PARAM);
        }

        if (slow_time_threshold > 0 && slow_time_threshold < cmd_time_stats_.request_total_time_) {
          is_slow_query = true;
          _OBPROXY_SLOW_LOG(INFO, MONITOR_LOG_FORMAT, MONITOR_LOG_PARAM);
        }

        if (is_error_resp) {
          _OBPROXY_ERROR_LOG(INFO, MONITOR_ERROR_LOG_FORMAT, MONITOR_ERROR_LOG_PARAM);
        }
      }

      if (get_global_proxy_config().enable_monitor_stat) {
        update_monitor_stats(logic_tenant_name, logic_database_name,
                            cluster_name, tenant_name, database_name,
                            database_type, stmt_type, error_code_str);
      }

      if (is_need_convert_vip_to_tname()
        //  && !cs_info.is_sharding_user()
         && !is_slow_query
         && !is_error_resp) {
       SQLMonitorInfo monitor_info;
       monitor_info.request_type_ = OBPROXY_RPC_REQUEST;
       if (OB_LIKELY(!is_shard_request)) {
         switch (pcode) {
           case obrpc::OB_TABLE_API_LOGIN:
             monitor_info.rpc_request_stat_count_.obkv_login_count_ = 1;
           break;
           case obrpc::OB_TABLE_API_EXECUTE:
             monitor_info.rpc_request_stat_count_.obkv_execute_count_ = 1;
           break;
           case obrpc::OB_TABLE_API_BATCH_EXECUTE:
             monitor_info.rpc_request_stat_count_.obkv_batch_execute_count_ = 1;
           break;
           case obrpc::OB_TABLE_API_EXECUTE_QUERY:
             monitor_info.rpc_request_stat_count_.obkv_execute_query_count_ = 1;
           break;
           case obrpc::OB_TABLE_API_QUERY_AND_MUTATE:
             monitor_info.rpc_request_stat_count_.obkv_query_and_mutate_count_ = 1;
           break;
           case obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC:
             monitor_info.rpc_request_stat_count_.obkv_execute_query_sync_count_ = 1;
           break;
           case obrpc::OB_TABLE_TTL:
             monitor_info.rpc_request_stat_count_.obkv_table_ttl_count_ = 1;
           break;
           case obrpc::OB_TTL_REQUEST:
             monitor_info.rpc_request_stat_count_.obkv_tll_request_count_ = 1;
           break;
           case obrpc::OB_TABLE_API_DIRECT_LOAD:
             monitor_info.rpc_request_stat_count_.obkv_direct_load_count_ = 1;
           break;
           case obrpc::OB_TABLE_API_LS_EXECUTE:
             monitor_info.rpc_request_stat_count_.obkv_ls_execute_count_ = 1;
           break;
             monitor_info.rpc_request_stat_count_.obkv_other_count_ = 1;
           default:
             //not
           break;
         }
       } else {
          switch (pcode) {
           case obrpc::OB_TABLE_API_LOGIN:
           case obrpc::OB_TABLE_API_EXECUTE:
           case obrpc::OB_TABLE_TTL:
           case obrpc::OB_TTL_REQUEST:
           case obrpc::OB_TABLE_API_DIRECT_LOAD:
           break;
           case obrpc::OB_TABLE_API_BATCH_EXECUTE:
             monitor_info.rpc_request_stat_count_.obkv_batch_execute_shard_count_ = 1;
           break;
           case obrpc::OB_TABLE_API_EXECUTE_QUERY:
             monitor_info.rpc_request_stat_count_.obkv_execute_query_shard_count_ = 1;
           break;
           case obrpc::OB_TABLE_API_QUERY_AND_MUTATE:
             monitor_info.rpc_request_stat_count_.obkv_query_and_mutate_shard_count_ = 1;
           break;
           case obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC:
             monitor_info.rpc_request_stat_count_.obkv_execute_query_sync_shard_count_ = 1;
           break;
           case obrpc::OB_TABLE_API_LS_EXECUTE:
             monitor_info.rpc_request_stat_count_.obkv_ls_execute_shard_count_ = 1;
           break;
           default:
             monitor_info.rpc_request_stat_count_.obkv_other_shard_count_ = 1;
             //not
           break;
         }
       }
       monitor_info.request_count_ = 1;
       monitor_info.request_total_time_ = cmd_time_stats_.request_total_time_;
       monitor_info.server_process_request_time_ = cmd_time_stats_.server_process_request_time_;
       monitor_info.prepare_send_request_to_server_time_ = cmd_time_stats_.prepare_send_request_to_server_time_;
       self_ethread().thread_prometheus_->set_sql_monitor_info(tenant_name, cluster_name, monitor_info, OBPROXY_RPC_REQUEST);
      } else {
        if (!is_need_convert_vip_to_tname()) {
          NET_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                              cluster_name, tenant_name, database_name,
                              PROMETHEUS_RPC_REQUEST_BYTE, true, true,
                              cmd_size_stats_.client_request_bytes_);
          NET_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                              cluster_name, tenant_name, database_name,
                              PROMETHEUS_RPC_REQUEST_BYTE, true, false,
                              cmd_size_stats_.server_request_bytes_);
          NET_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                              cluster_name, tenant_name, database_name,
                              PROMETHEUS_RPC_REQUEST_BYTE, false, true,
                              cmd_size_stats_.client_response_bytes_);
          NET_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                              cluster_name, tenant_name, database_name,
                              PROMETHEUS_RPC_REQUEST_BYTE, false, false,
                              cmd_size_stats_.server_response_bytes_);
        } else {
          // item not used in cloud
         //  stmt_type = OBPROXY_T_INVALID;
        }

        RPC_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                            cluster_name, tenant_name, database_name,
                            pcode, PROMETHEUS_REQUEST_COUNT,
                            is_slow_query, is_error_resp, is_shard_request, static_cast<int64_t> (1));
        RPC_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                            cluster_name, tenant_name, database_name,
                            pcode, PROMETHEUS_REQUEST_TOTAL_TIME,
                            hrtime_to_usec(cmd_time_stats_.request_total_time_));
        RPC_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                            cluster_name, tenant_name, database_name,
                            pcode, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME,
                            hrtime_to_usec(cmd_time_stats_.server_process_request_time_));
        RPC_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                            cluster_name, tenant_name, database_name,
                            pcode, PROMETHEUS_PREPARE_SEND_REQUEST_TIME,
                            hrtime_to_usec(cmd_time_stats_.prepare_send_request_to_server_time_));
      }
    }
  }
}

int ObRpcRequestSM::get_cached_config_info()
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *rpc_ctx = NULL;

  if (!is_valid_rpc_req() || OB_ISNULL(rpc_ctx = rpc_req_->get_rpc_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc req is invalid", K(ret), "req_is_valid", is_valid_rpc_req(), K(rpc_ctx), K_(rpc_trace_id));
  } else {
    uint64_t global_version = omt::get_global_proxy_config_table_processor().get_config_version();
    ObRpcRequestConfigInfo &req_config_info = rpc_req_->get_rpc_request_config_info();

    if (rpc_ctx->is_cached_config_info_avail_state()) {
      DRWLock &rwlock = rpc_ctx->get_rpc_ctx_lock();
      DRWLock::RDLockGuard rwlock_guard(rwlock);
      ObRpcRequestConfigInfo &cached_config_info = rpc_ctx->get_config_info();
      if (cached_config_info.config_version_ != global_version) {
        // need update
        rpc_ctx->cas_set_config_info_updating_state();
      }
      if (rpc_ctx->is_cached_config_info_avail_state()) {
        req_config_info.deep_copy(cached_config_info);
      }
    }

    LOG_DEBUG("rpc get cached config info done", K(req_config_info), K_(rpc_trace_id));
  }

  return ret;
}

int ObRpcRequestSM::update_cached_config_info()
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *rpc_ctx = NULL;

  if (!is_valid_rpc_req() || OB_ISNULL(rpc_ctx = rpc_req_->get_rpc_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc req is invalid", K(ret), "req_is_valid", is_valid_rpc_req(), K(rpc_ctx), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req_->get_obkv_info();
    ObRpcRequestConfigInfo &req_config_info = rpc_req_->get_rpc_request_config_info();

    LOG_DEBUG("rpc update cached config info", "is_auth", obkv_info.is_auth(), K_(rpc_trace_id));

    if (obkv_info.is_auth()) {
      ObRpcRequestConfigInfo &cached_config_info = rpc_ctx->get_config_info();
      cached_config_info.deep_copy(req_config_info);
      rpc_ctx->cas_set_config_info_avail_state();
    } else {
      // 双检查锁，判断状态，获取写锁
      if (rpc_ctx->is_cached_config_info_updating_state()) {
        DRWLock &rwlock = rpc_ctx->get_rpc_ctx_lock();
        DRWLock::WRLockGuard rwlock_guard(rwlock);
        if (rpc_ctx->is_cached_config_info_updating_state()) {
          ObRpcRequestConfigInfo &cached_config_info = rpc_ctx->get_config_info();
          cached_config_info.deep_copy(req_config_info);
          rpc_ctx->cas_set_config_info_avail_state();
        }
      }
    }
  }

  return ret;
}

//load rpc request config
void ObRpcRequestSM::refresh_rpc_request_config()
{
  int ret = OB_SUCCESS;
  ObRpcClientNetHandler *client_net_handler = NULL;
  ObRpcReqCtx *rpc_ctx = NULL;
  if (is_valid_rpc_req()
     && OB_NOT_NULL(client_net_handler = rpc_req_->get_cnet_sm())
     && OB_NOT_NULL(rpc_ctx = rpc_req_->get_rpc_ctx())) {
    ObRpcRequestConfigInfo &config_info = rpc_req_->get_rpc_request_config_info();
    uint64_t global_version = omt::get_global_proxy_config_table_processor().get_config_version();
    if (config_info.config_version_ != global_version || !config_info.is_init()) {
      ObString cluster_name = rpc_ctx->cluster_name_;
      ObString tenant_name = rpc_ctx->tenant_name_;
      ObVipAddr addr = client_net_handler->get_ct_info().vip_tenant_.vip_addr_; 
      if (client_net_handler->is_vip_lookup_success()
          && cluster_name.empty()
          && tenant_name.empty()) {
        cluster_name = client_net_handler->get_vip_cluster_name();
        tenant_name = client_net_handler->get_vip_tenant_name();
      }
      //TODO, mysql_sm and mysql_transact has change get_config_item to get_proxy_multi_config in 4.3.0, 
      // need update it in 4.3.1 for rpc request tenant's level config info
      if (OB_FAIL(get_config_item(cluster_name, tenant_name, addr, config_info, global_version))) {
        LOG_WDIAG("get config item failed", K(cluster_name), K(tenant_name), K(addr), K(ret), K_(rpc_trace_id));
      } else if (OB_FAIL(update_cached_config_info())) {
        LOG_WDIAG("fail to call update_cached_config_info", K(cluster_name), K(tenant_name), K(addr), K(ret), K_(rpc_trace_id));
      } else {
        LOG_DEBUG("get config succ", K(addr), K(cluster_name), K(tenant_name), K(ret), K(config_info), K_(rpc_trace_id));
      }
    } else {
      ObRpcRequestConfigInfo &config_info = rpc_req_->get_rpc_request_config_info();
      LOG_DEBUG("get config succ just use cached config", K(ret), K(config_info), K_(rpc_trace_id));
    }
  }
}

int ObRpcRequestSM::get_config_item(const ObString& cluster_name,
                                        const ObString &tenant_name,
                                        const ObVipAddr &addr,
                                        ObRpcRequestConfigInfo &req_config_info,
                                        const int64_t global_version)
{
  int ret = OB_SUCCESS;
  // bool sync_conf_sys_var = false;

  if (OB_SUCC(ret)) {
    ObConfigBoolItem bool_item;
    if (OB_FAIL(get_global_config_processor().get_proxy_config_bool_item(
            addr, cluster_name, tenant_name, "enable_cloud_full_username", bool_item))) {
      LOG_WDIAG("get enable_cloud_full_username failed", K(addr), K(cluster_name), K(tenant_name), K(ret), K_(rpc_trace_id));
    } else {
      req_config_info.enable_cloud_full_username_ = bool_item.get_value();
    }
  }

  if (OB_SUCC(ret)) {
    ObConfigItem item;
    if (OB_FAIL(get_global_config_processor().get_proxy_config(
            addr, cluster_name, tenant_name, "proxy_route_policy", item))) {
      LOG_WDIAG("get proxy route policy config failed", K(addr), K(cluster_name), K(tenant_name), K(ret), K_(rpc_trace_id));
    } else {
      req_config_info.proxy_route_policy_.check_and_extend_str(strlen(item.str()));
      req_config_info.proxy_route_policy_.mem_reset();
      // memset(req_config_info.proxy_route_policy_, 0, sizeof(sm_->proxy_route_policy_));
      MEMCPY(req_config_info.proxy_route_policy_.get_config_str(), item.str(), strlen(item.str()));
    }
  }

  // not used in sql SM, and it not to read at here, use global
  // if (OB_SUCC(ret)) {
  //   ObConfigItem item;
  //   if (OB_FAIL(get_global_config_processor().get_proxy_config(
  //           addr, cluster_name, tenant_name, "proxy_idc_name", item))) {
  //     LOG_WDIAG("get proxy idc name config failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
  //   } else {
  //     req_config_info.proxy_idc_name_.check_and_extend_str(strlen(item.str()));
  //     req_config_info.proxy_idc_name_.mem_reset();
  //     MEMCPY(req_config_info.proxy_idc_name_.get_config_str(), item.str(), strlen(item.str()));
  //   }
  // }

  if (OB_SUCC(ret)) {
    ObConfigIntItem int_item;
    if (OB_FAIL(get_global_config_processor().get_proxy_config_int_item(addr, cluster_name, tenant_name,
                                                     "rpc_request_max_retries", int_item))) {
      LOG_WDIAG("get rpc_request_max_retries failed", K(addr), K(cluster_name), K(tenant_name), K(ret), K_(rpc_trace_id));
    } else {
      req_config_info.rpc_request_max_retries_ = int_item.get_value();
    }
  }

  if (OB_SUCC(ret)) {
    ObConfigBoolItem bool_item;
    if (OB_FAIL(get_global_config_processor().get_proxy_config_bool_item(
            addr, cluster_name, tenant_name, "rpc_support_key_partition_shard_request", bool_item))) {
      LOG_WDIAG("get rpc_support_key_partition_shard_request failed", K(addr), K(cluster_name), K(tenant_name), K(ret), K_(rpc_trace_id));
    } else {
      req_config_info.rpc_support_key_partition_shard_request_ = bool_item.get_value();
    }
  }

  if (OB_SUCC(ret)) {
    ObConfigBoolItem bool_item;
    if (OB_FAIL(get_global_config_processor().get_proxy_config_bool_item(
            addr, cluster_name, tenant_name, "rpc_enable_force_srv_black_list", bool_item))) {
      LOG_WDIAG("get rpc_enable_force_srv_black_list failed", K(addr), K(cluster_name), K(tenant_name), K(ret), K_(rpc_trace_id));
    } else {
      req_config_info.rpc_enable_force_srv_black_list_ = bool_item.get_value();
    }
  }

  if (OB_SUCC(ret)) {
    ObConfigBoolItem bool_item;
    if (OB_FAIL(get_global_config_processor().get_proxy_config_bool_item(
            addr, cluster_name, tenant_name, "rpc_enable_direct_expire_route_entry", bool_item))) {
      LOG_WDIAG("get rpc_enable_direct_expire_route_entry failed", K(addr), K(cluster_name), K(tenant_name), K(ret), K_(rpc_trace_id));
    } else {
      req_config_info.rpc_enable_direct_expire_route_entry_ = bool_item.get_value();
    }
  }

  if (OB_SUCC(ret)) {
    ObConfigIntItem int_item;
    if (OB_FAIL(get_global_config_processor().get_proxy_config_int_item(
            addr, cluster_name, tenant_name, "rpc_request_timeout", int_item))) {
      LOG_WDIAG("get rpc_request_timeout failed", K(addr), K(cluster_name), K(tenant_name), K(ret), K_(rpc_trace_id));
    } else {
      req_config_info.rpc_request_timeout_ = int_item.get_value();
    }
  }

  if (OB_SUCC(ret)) {
    ObConfigBoolItem bool_item;
    if (OB_FAIL(get_global_config_processor().get_proxy_config_bool_item(
            addr, cluster_name, tenant_name, "rpc_enable_reroute", bool_item))) {
      LOG_WDIAG("get rpc_enable_reroute failed", K(addr), K(cluster_name), K(tenant_name), K(ret), K_(rpc_trace_id));
    } else {
      req_config_info.rpc_enable_reroute_ = bool_item.get_value();
    }
  }

  if (OB_SUCC(ret)) {
    ObConfigBoolItem bool_item;
    if (OB_FAIL(get_global_config_processor().get_proxy_config_bool_item(
            addr, cluster_name, tenant_name, "rpc_enable_congestion", bool_item))) {
      LOG_WDIAG("get rpc_enable_congestion failed", K(addr), K(cluster_name), K(tenant_name), K(ret), K_(rpc_trace_id));
    } else {
      req_config_info.rpc_enable_congestion_ = bool_item.get_value();
    }
  }

  if (OB_SUCC(ret)) {
    ObConfigBoolItem bool_item;
    if (OB_FAIL(get_global_config_processor().get_proxy_config_bool_item(
            addr, cluster_name, tenant_name, "rpc_enable_global_index", bool_item))) {
      LOG_WDIAG("get rpc_enable_global_index failed", K(addr), K(cluster_name), K(tenant_name), K(ret), K_(rpc_trace_id));
    } else {
      req_config_info.rpc_enable_global_index_ = bool_item.get_value();
    }
  }

  if (OB_SUCC(ret)) {
    ObConfigBoolItem bool_item;
    if (OB_FAIL(get_global_config_processor().get_proxy_config_bool_item(
            addr, cluster_name, tenant_name, "rpc_enable_retry_request_info_log", bool_item))) {
      LOG_WDIAG("get rpc_enable_retry_request_info_log failed", K(addr), K(cluster_name), K(tenant_name), K(ret), K_(rpc_trace_id));
    } else {
      req_config_info.rpc_enable_retry_request_info_log_ = bool_item.get_value();
    }
  }

  if (OB_SUCC(ret)) {
    ObConfigTimeItem time_item;
    if (OB_FAIL(get_global_config_processor().get_proxy_config(
            addr, cluster_name, tenant_name, "rpc_request_timeout_delta", time_item))) {
      LOG_WDIAG("get rpc_request_timeout_delta failed", K(addr), K(cluster_name), K(tenant_name), K(ret), K_(rpc_trace_id));
    } else {
      req_config_info.rpc_request_timeout_delta_ = time_item.get_value();
    }
  }

  if (OB_SUCC(ret)) {
    ObConfigTimeItem time_item;
    if (OB_FAIL(get_global_config_processor().get_proxy_config(
            addr, cluster_name, tenant_name, "rpc_request_retry_waiting_time", time_item))) {
      LOG_WDIAG("get rpc_request_retry_waiting_time failed", K(addr), K(cluster_name), K(tenant_name), K(ret), K_(rpc_trace_id));
    } else {
      req_config_info.rpc_request_retry_waiting_time_ = time_item.get_value();
    }
  }

  if (OB_SUCC(ret)) {
    if (global_version > 0) {
      req_config_info.config_version_ = global_version;
    }
  }
  return ret;
}

void ObRpcRequestSM::set_next_state(enum ObRpcRequestSMActionType state)
{
  LOG_DEBUG("ObRpcRequestSM::set_state_and_call_next", KP_(rpc_req), "state", ObRpcReqDebugNames::get_action_name(state), K_(rpc_trace_id));
  next_action_ = state;
}

inline void ObRpcRequestSM::set_state_and_call_next(enum ObRpcRequestSMActionType state)
{
  LOG_DEBUG("ObRpcRequestSM::set_state_and_call_next", KP_(rpc_req), "state", ObRpcReqDebugNames::get_action_name(state), K_(rpc_trace_id));
  next_action_ = state;
  call_next_action();
}

inline void ObRpcRequestSM::call_next_action()
{
  switch(next_action_) {
    case RPC_REQ_NEW_REQUEST : {
      setup_process_request();
      break;
    }
    case RPC_REQ_REQUEST_DIRECT_ROUTING : {
      setup_rpc_request_direct_load_route();
      break;
    }
    case RPC_REQ_CTX_LOOKUP : {
      setup_rpc_req_ctx_lookup();
      break;
    } 
    case RPC_REQ_IN_CLUSTER_BUILD : {
      setup_rpc_get_cluster();
      break;
    }
    case RPC_REQ_INNER_INFO_GET : {
      setup_req_inner_info_get();
      break;
    }
    case RPC_REQ_QUERY_ASYNC_INFO_GET : {
      setup_table_query_async_info_get();
      break;
    }
    case RPC_REQ_IN_TABLEGROUP_LOOKUP : {
      setup_rpc_tablegroup_lookup();
      break;
    }
    case RPC_REQ_IN_PARTITION_LOOKUP : {
      setup_rpc_partition_lookup();
      break;
    }
    case RPC_REQ_PARTITION_LOOKUP_DONE : {
      state_rpc_partition_lookup_done();
      break;
    }
    case RPC_REQ_IN_INDEX_LOOKUP: {
      setup_rpc_index_lookup();
      break;
    }
    case RPC_REQ_SERVER_ADDR_SEARCHING : {
      setup_rpc_server_addr_searching();
      break;
    }
    case RPC_REQ_SERVER_ADDR_SEARCHED : {
      state_rpc_server_addr_searched();
      break;
    }
    case RPC_REQ_IN_CONGESTION_CONTROL_LOOKUP: {
      setup_congestion_control_lookup();
      break;
    }
    case RPC_REQ_CONGESTION_CONTROL_LOOKUP_DONE: {
      state_congestion_control_lookup_done();
      break;
    }
    case RPC_REQ_REQUEST_SERVER_SENDING : {
      setup_rpc_send_request_to_server();
      break;
    }
    case RPC_REQ_REQUEST_REWRITE : {
      state_rpc_server_request_rewrite();
      break;
    }
    case RPC_REQ_PROCESS_RESPONSE : {
      setup_process_response();
      break;
    }
    case RPC_REQ_RESPONSE_REWRITE : {
      state_rpc_client_response_rewrite();
      break;
    }
    case RPC_REQ_RESPONSE_CLIENT_SENDING : {
      setup_rpc_send_response_to_client();
      break;
    }
    case RPC_REQ_RESPONSE_SERVER_REROUTING : {
      setup_rpc_request_server_reroute();
      break;
    }
    case RPC_REQ_INTERNAL_BUILD_RESPONSE : {
      setup_rpc_internal_build_response();
      break;
    }
    case RPC_REQ_REQUEST_RETRY : {
      setup_rpc_request_retry();
      break;
    }
    case RPC_REQ_REQUEST_INNER_REQUEST_CLEANUP: {
      state_rpc_req_inner_request_cleanup();
      break;
    }
    case RPC_REQ_REQUEST_CLEANUP: {
      state_rpc_req_cleanup();
      break;
    }
    case RPC_REQ_REQUEST_DONE : {
      state_rpc_req_done();
      break;
    }
    case RPC_REQ_REQUEST_ERROR : {
      setup_rpc_return_error();
      break;
    }
    case RPC_REQ_REQUEST_TIMEOUT : {
      handle_timeout();
      break;
    }
    case RPC_REQ_HANDLE_SHARD_REQUEST: {
      setup_rpc_handle_shard_request();
      break;
    }
    default: {
      LOG_EDIAG("unknown route next action", K_(next_action), K_(rpc_trace_id));
      setup_rpc_return_error();
      break;
    }
  }
}
