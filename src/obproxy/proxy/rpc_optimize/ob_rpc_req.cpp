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
#include "proxy/rpc_optimize/ob_rpc_req.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/profile/ob_trace_id_new.h" //use NewTraceId
#include "utils/ob_proxy_lib.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "iocore/eventsystem/ob_thread_allocator.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "iocore/net/ob_net_vconnection.h"
#include "proxy/route/ob_table_entry.h"
#include "proxy/route/ob_index_entry.h"
#include "proxy/rpc_optimize/optimizer/ob_rpc_req_sharding_plan.h"
#include "proxy/rpc_optimize/optimizer/ob_rpc_req_optimizer_processor.h"
#include "proxy/rpc_optimize/rpclib/ob_table_query_async_entry.h"
#include "proxy/rpc_optimize/rpclib/ob_tablegroup_entry.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_req_ctx.h"
#include "proxy/rpc_optimize/ob_rpc_request_sm.h"

using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::optimizer;
using namespace oceanbase::obrpc;
using namespace oceanbase::obproxy::obkv;
using namespace oceanbase::obproxy::event;

const int64_t ObRpcReq::MAX_SCATTER_LEN = (sizeof(ObRpcReq) / sizeof(int64_t));
static int64_t val[ObRpcReq::MAX_SCATTER_LEN];
static int16_t to[ObRpcReq::MAX_SCATTER_LEN];
static int64_t scat_count = 0;

void ObRpcReq::make_scatter_list(ObRpcReq &prototype)
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

void ObRpcReq::instantiate_func(ObRpcReq &prototype, ObRpcReq &new_instance)
{
  UNUSED(prototype);
  memset(reinterpret_cast<char *>(&new_instance), 0, sizeof(ObRpcReq));

  int64_t *pd = reinterpret_cast<int64_t *>(&new_instance);

  for (int64_t i = 0; i < scat_count; ++i) {
    pd[to[i]] = val[i];
  }
}

ObRpcReq::ObRpcReq() :
               magic_(RPC_REQ_MAGIC_ALIVE), sm_(NULL), cnet_sm_(NULL), snet_sm_(NULL), execute_plan_(NULL),
               client_timestamp_(), server_timestamp_(), server_entry_send_retry_times_(0),
               cs_id_(0), ss_id_(0), congest_status_(STATE_COMMON), request_buf_(NULL), request_inner_buf_(NULL), response_buf_(NULL), response_inner_buf_(NULL),
               request_buf_len_(0), request_inner_buf_len_(0), response_buf_len_(0), response_inner_buf_len_(0), req_buf_repeat_times_(0),
               request_len_(0), response_len_(0), origin_channel_id_(0), c_channel_id_(0), s_channel_id_(0),
               retry_times_(0), inner_req_retry_times_(0), cont_index_(0), cluster_version_(0), is_need_terminal_client_net_(false),
               is_response_(false), is_canceled_(false), is_finish_(false), is_server_addr_set_(false),
               is_use_request_inner_buf_(false), is_use_response_inner_buf_(false),
               rpc_type_(obkv::OBPROXY_RPC_OBRPC), server_add_(), created_thread_(NULL),
               cnet_state_(RPC_REQ_CLIENT_INIT), snet_state_(RPC_REQ_SERVER_INIT), sm_state_(RPC_REQ_SM_INIT), rpc_req_clean_module_(RPC_REQ_CLEAN_MODULE_MAX),
               inner_request_allocator_(NULL), inner_request_allocator_len_(0), rpc_request_(NULL),
               rpc_response_(NULL), rpc_request_len_(0), rpc_response_len_(0), 
               client_net_timeout_us_(0), server_net_timeout_us_(0),
               root_rpc_req_(NULL), sub_rpc_req_array_(NULL), sub_rpc_req_array_size_(0),
               current_sub_rpc_req_count_(0), obkv_info_(), config_info_(), is_sub_req_inited_(false)
{
  static bool scatter_inited = false;

  if (!scatter_inited) {
    make_scatter_list(*this);
    scatter_inited = true;
  }
}

DEF_TO_STRING(ObRpcReq)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), KP_(request_buf), K_(request_buf_len), K_(request_len), KP_(response_buf), K_(response_buf_len), K_(response_len),
    K_(origin_channel_id), K_(c_channel_id), K_(s_channel_id), K_(retry_times), K_(cluster_version), K_(is_need_terminal_client_net),
    K_(is_response), K_(is_canceled), K_(is_finish), "rpc_type", static_cast<int>(rpc_type_), KP_(rpc_request), KP_(rpc_response),
    K_(client_net_timeout_us), K_(server_net_timeout_us), K_(is_sub_req_inited), KP_(cnet_sm), KP_(snet_sm), "server_addr", server_add_.addr_);
  J_COMMA();
  J_KV("obkv_info", obkv_info_);
  J_OBJ_END();
  return pos;
}

void ObRpcReq::finish()
{
  if (!is_finish_) {
    if (OB_NOT_NULL(sm_) && OB_NOT_NULL(sm_->handler_)) {
      sm_->handle_event(RPC_REQUEST_SM_DONE);
      sm_ = NULL;
    }
    is_finish_ = true;
  }
}

int ObRpcReq::init(obkv::ObProxyRpcType rpc_type, ObRpcRequestSM *sm, ObRpcClientNetHandler *client_net,
          int64_t request_len, int64_t cluster_version, int64_t origin_channel_id, int64_t client_channel_id,
          int64_t cs_id, net::ObNetVConnection *rpc_net_vc, int64_t trace_id1, int64_t trace_id2)
{
  int ret = OB_SUCCESS;

  rpc_type_ = rpc_type;
  sm_ = sm;
  cnet_sm_ = reinterpret_cast<ObContinuation *>(client_net);
  request_len_ = request_len;
  cluster_version_ = cluster_version;
  origin_channel_id_ = origin_channel_id;
  c_channel_id_ = client_channel_id;
  cs_id_ = cs_id;
  if (obutils::get_global_proxy_config().need_convert_vip_to_tname) {
    obkv_info_.client_info_.set_addr(rpc_net_vc->get_real_client_addr());
  } else {
    obkv_info_.client_info_.set_addr(rpc_net_vc->get_remote_addr());
  }

  obkv_info_.set_rpc_trace_id(trace_id2, trace_id1);

  return ret;
}

int ObRpcReq::sub_rpc_req_init(ObRpcReq *root_rpc_req, ObRpcRequestSM *sm, ObRpcRequest *rpc_request, int64_t rpc_request_len,
                               int64_t cont_index, int64_t partition_id, int64_t ls_id)
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *root_rpc_ctx = NULL;

  if (OB_ISNULL(root_rpc_req) || OB_ISNULL(sm) || OB_ISNULL(rpc_request)
      || OB_ISNULL(root_rpc_ctx = root_rpc_req->get_rpc_ctx())
      || 0 == rpc_request_len || 0 == cont_index) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "sub_rpc_req_init get invalid argument", KPC(root_rpc_req), KP(sm), KP(rpc_request), K(rpc_request_len), K(cont_index));
  } else {
    ObRpcOBKVInfo &root_obkv_info = root_rpc_req->get_obkv_info();

    // handle obkv info
    obkv_info_.rpc_trace_id_ = root_obkv_info.rpc_trace_id_;
    obkv_info_.rpc_trace_id_.set_rpc_sub_index_id(cont_index, root_obkv_info.inner_req_retries_);
    obkv_info_.set_definitely_single(true);
    obkv_info_.set_partition_id(partition_id);
    obkv_info_.set_ls_id(ls_id);
    obkv_info_.pcode_ = root_obkv_info.pcode_;
    obkv_info_.is_inner_request_ = true;
    // set rpc ctx
    if (OB_NOT_NULL(obkv_info_.rpc_ctx_)) {
      obkv_info_.rpc_ctx_->dec_ref();
      obkv_info_.rpc_ctx_ = NULL;
    }
    obkv_info_.rpc_ctx_ = root_rpc_ctx;
    obkv_info_.rpc_ctx_->inc_ref();
    // for global index route
    if (OB_NOT_NULL(obkv_info_.index_entry_)) {
      obkv_info_.index_entry_->dec_ref();
      obkv_info_.index_entry_ = NULL;
    }
    obkv_info_.set_query_with_index(root_obkv_info.is_query_with_index());
    obkv_info_.set_global_index_route(root_obkv_info.is_global_index_route());
    obkv_info_.data_table_id_ = root_obkv_info.data_table_id_;
    obkv_info_.table_name_ = root_obkv_info.table_name_;
    obkv_info_.index_name_ = root_obkv_info.index_name_;
    obkv_info_.index_table_name_ = root_obkv_info.index_table_name_;
    obkv_info_.index_entry_ = root_obkv_info.index_entry_;
    obkv_info_.inner_req_retries_ = root_obkv_info.inner_req_retries_;
    if (OB_NOT_NULL(obkv_info_.index_entry_)) {
      obkv_info_.index_entry_->inc_ref();
    }

    if (root_obkv_info.is_inner_req_retrying()) {
      PROXY_LOG(INFO, "sub lsop request is retrying and generate new sub request", "new_req", this, "root_req",
                root_rpc_req, "sub_req_retry_times", root_obkv_info.inner_req_retries_, "root_req_trace_id",
                root_obkv_info.rpc_trace_id_, "new_req_trace_id", obkv_info_.rpc_trace_id_);
    }
    // handle rpc req
    sm_ = sm;
    cnet_sm_ = root_rpc_req->cnet_sm_;
    set_rpc_type(root_rpc_req->get_rpc_type());
    set_rpc_request(rpc_request);
    set_rpc_request_len(rpc_request_len);
    set_cluster_version(root_rpc_req->get_cluster_version());
    set_root_rpc_req(root_rpc_req);
    set_server_net_timeout_us(root_rpc_req->get_server_net_timeout_us());
    // get rpc trace id
    ObRpcReqTraceId &rpc_trace_id = obkv_info_.rpc_trace_id_;
    // 需要设置client net状态防止被server net清理
    set_cnet_state(ObRpcReq::ClientNetState::RPC_REQ_CLIENT_INNER_REQUEST);

    if (OB_FAIL(alloc_inner_request_allocator())) {
      PROXY_LOG(WDIAG, "fail to alloc_inner_request_allocator", K(rpc_trace_id), K(ret));
    } else {
      // success
      PROXY_LOG(DEBUG, "succ to init sub rpc req", "sub_rpc_req", *this);
    }
  }

  return ret;
}

int ObRpcReq::clean_sub_rpc_req()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sub_rpc_req_array_) || sub_rpc_req_array_size_ != current_sub_rpc_req_count_) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(WDIAG, "sub_rpc_req_array_size_ must equal to current_sub_rpc_req_count_", K_(sub_rpc_req_array_size), K_(current_sub_rpc_req_count));
  } else {
    uint32_t not_inited_req = 0;
    for (int i = 0; i < sub_rpc_req_array_size_; ++i) {
      ObRpcReq *sub_rpc_req = sub_rpc_req_array_[i];
      PROXY_LOG(DEBUG, "ObRpcReq::clean_sub_rpc_req will clean", K(sub_rpc_req));
      if (sub_rpc_req->is_sub_req_inited()) {
        ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ClientNetState::RPC_REQ_CLIENT_INNER_REQUEST_DONE);
        sub_rpc_req->cleanup(cleanup_params);
      } else {
        PROXY_LOG(INFO, "ObRpcReq sub req is not inited, destroy it directly", K(sub_rpc_req));
        sub_rpc_req->inner_request_cleanup();
        not_inited_req ++;
        ObRpcRequestSM *sub_sm = sub_rpc_req->get_request_sm();
        if (OB_NOT_NULL(sub_sm) && OB_NOT_NULL(sub_sm->handler_)) {
          sub_sm->set_sm_terminate();
          sub_sm->handle_event(RPC_REQUEST_SM_DONE);
          sub_rpc_req->sm_ = NULL;
        }
        sub_rpc_req->destroy();
      }
    }

    current_sub_rpc_req_count_ -= not_inited_req;
    if (current_sub_rpc_req_count_ == 0) {
      PROXY_LOG(DEBUG, "clean_sub_rpc_req done, all sub req cleaned", K(current_sub_rpc_req_count_), K(sub_rpc_req_array_size_), K(this));
      ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_SHARDING_REQUEST_DONE);
      cleanup(cleanup_params);
    }

    if OB_FAIL(free_sub_rpc_req_array()) {
      PROXY_LOG(WDIAG, "fail to call free_sub_rpc_req_array", K(ret));
    }
  }

  return ret;
}

void ObRpcReq::destroy()
{
  if (RPC_REQ_MAGIC_ALIVE == magic_) {
    PROXY_LOG(DEBUG, "ObRpcReq::destroy", "rpc_req", *this, KP(this));

    // first , clean sub request
    if (OB_NOT_NULL(execute_plan_)) {
      optimizer::ObProxyShardRpcReqRequestPlan *plan = reinterpret_cast<optimizer::ObProxyShardRpcReqRequestPlan *>(execute_plan_);
      plan->~ObProxyShardRpcReqRequestPlan();
      get_global_optimizer_rpc_req_processor().free_allocator(plan->get_allocator());
      execute_plan_ = NULL;
    }

    free_rpc_request();
    free_rpc_response();
    free_request_buf(); // free request buffer
    free_response_buf(); // free response buffer
    free_request_inner_buf(); // free inner buffer
    free_inner_request_allocator(); // free inner request allocator
    free_response_inner_buf();
    free_sub_rpc_req_array();
    obkv_info_.reset();

    magic_ = RPC_REQ_SM_MAGIC_DEAD;
    op_thread_free(ObRpcReq, this, event::get_rpc_req_allocator());
  } else {
    PROXY_LOG(EDIAG, "ObRpcReq::destroy but magic is error", "rpc_req", *this, KP(this));
  }
}

void ObRpcReq::cleanup(const ObRpcReqCleanupParams &params)
{
  int ret = OB_SUCCESS;
  if (RPC_REQ_MAGIC_ALIVE == magic_) {
    ObRpcRequestSM *request_sm = NULL;

    if (OB_ISNULL(request_sm = get_request_sm())) {
      PROXY_LOG(EDIAG, "call ObRpcReq::cleanup but request_sm is NULL", K(request_sm));
    } else if (get_origin_channel_id() != request_sm->get_rpc_req_origin_channel_id()) {
      PROXY_LOG(EDIAG, "rpc_req origin channel id is not equal to request_sm origin channle id",
        "rpc_req id", get_origin_channel_id(), "request_sm id", request_sm->get_rpc_req_origin_channel_id());
    } else {
      if (is_inner_request()) {
        // 子任务，需要获取锁
        MUTEX_LOCK(lock, request_sm->inner_request_cleanup_mutex_, this_ethread());
        set_clean_module(params.rpc_req_clean_module_);
        if (params.rpc_req_clean_module_ == RPC_REQ_CLEAN_MODULE_CLIENT_NET) {
          set_cnet_state(params.cnet_state_);
        } else if (params.rpc_req_clean_module_ == RPC_REQ_CLEAN_MODULE_SERVER_NET) {
          set_snet_state(params.snet_state_);
        } else if (params.rpc_req_clean_module_ == RPC_REQ_CLEAN_MODULE_SERVER_NET) {
          set_sm_state(params.sm_state_);
        } else {
          // error
          ret = OB_ERR_UNEXPECTED;
          PROXY_LOG(EDIAG, "invalid clean up params", K(params.rpc_req_clean_module_));
        }

        if (OB_SUCC(ret)) {
          PROXY_LOG(DEBUG, "ObRpcReq::cleanup inner_request done", KP(this), KP(request_sm));
          request_sm->schedule_cleanup_action();    // 调度异步任务释放
        }
      } else {
        set_clean_module(params.rpc_req_clean_module_);
        if (params.rpc_req_clean_module_ == RPC_REQ_CLEAN_MODULE_CLIENT_NET) {
          set_cnet_state(params.cnet_state_);
        } else if (params.rpc_req_clean_module_ == RPC_REQ_CLEAN_MODULE_SERVER_NET) {
          set_snet_state(params.snet_state_);
        } else if (params.rpc_req_clean_module_ == RPC_REQ_CLEAN_MODULE_SERVER_NET) {
          set_sm_state(params.sm_state_);
        } else {
          // error
          ret = OB_ERR_UNEXPECTED;
          PROXY_LOG(EDIAG, "invalid clean up params", K(params.rpc_req_clean_module_));
        }

        if (OB_SUCC(ret)) {
          PROXY_LOG(DEBUG, "ObRpcReq::cleanup done", KP(this), KP(request_sm));
          request_sm->schedule_cleanup_action();    // 调度异步任务释放
        }
      }
    }
  } else {
    PROXY_LOG(EDIAG, "ObRpcReq::cleanup but magic is error", "rpc_req", *this, KP(this));
  }
}

/**
 * @brief
 * The main purpose here is to release the memory allocated by the current thread.
 * 1. request buf
 * 2. request inner buf
 * 3. index entry
 * The following memory is allocated in the main thread and finally released by the main thread
 * 1. request
 * 2. response (allocated by inner_request_allocator)
 * 3. res buf (allocated by inner_request_allocator)
 * 4. inner_request_allocator
 * 5. req_info/obkv_info
 * 6. subtask dummy ldc
 */
void ObRpcReq::inner_request_cleanup()
{
  PROXY_LOG(DEBUG, "ObRpcReq::inner_request_cleanup", "rpc_req", *this, K(this));
  free_request_buf(); // free request buffer
  free_request_inner_buf(); // free inner buffer
  free_response_inner_buf(); // free inner buffer

  ObRpcOBKVInfo &obkv_info = get_obkv_info();
  if (OB_NOT_NULL(obkv_info.index_entry_)) {
    obkv_info.index_entry_->dec_ref();
    obkv_info.index_entry_ = NULL;
  }
}

bool ObRpcOBKVInfo::is_need_retry_with_query_async() const
{
  return OB_ISNULL(query_async_entry_) ? false : query_async_entry_->is_need_retry();
}

void ObRpcOBKVInfo::reset()
{
  request_id_ = 0;

  cluster_name_.reset();
  tenant_name_.reset();
  user_name_.reset();
  table_name_.reset();
  database_name_.reset();

  cluster_id_ = 0;
  tenant_id_ = 0;
  table_id_ = 0;
  partition_id_ = common::OB_INVALID_INDEX;

  route_policy_ = 1;
  is_proxy_route_policy_set_ = false;
  proxy_route_policy_ = MAX_PROXY_ROUTE_POLICY;

  pcode_ = OB_INVALID_RPC_CODE;
  flags_ = 0;
  rpc_origin_error_code_ = 0;
  is_set_rpc_trace_id_ = false;
  is_inner_request_ = false;

  if (OB_NOT_NULL(query_async_entry_)) {
    query_async_entry_->dec_ref();
    query_async_entry_ = NULL;
  }

  if (OB_NOT_NULL(rpc_ctx_)) {
    rpc_ctx_->dec_ref();
    rpc_ctx_ = NULL;
  }

  if (OB_NOT_NULL(tablegroup_entry_)) {
    tablegroup_entry_->dec_ref();
    tablegroup_entry_ = NULL;
  }

  data_table_id_ = OB_INVALID_ID;
  index_name_.reset();
  index_table_name_.reset();
  if (OB_NOT_NULL(index_entry_)) {
    index_entry_->dec_ref();
    index_entry_ = NULL;
  }
  index_table_name_buf_[0] = '\0';

  if (OB_NOT_NULL(dummy_entry_)) {
    dummy_entry_->dec_ref();
    dummy_entry_ = NULL;
  }
  dummy_ldc_.reset();

  rpc_trace_id_.reset();
  rpc_request_retry_last_begin_ = 0;
  rpc_request_retry_times_ = 0;
  rpc_request_reroute_moved_times_ = 0;
}

int ObRpcOBKVInfo::generate_index_table_name()
{
  int ret = OB_SUCCESS;
  int len = 0;
  int64_t buf_len = OB_MAX_INDEX_TABLE_NAME_LENGTH;

  if (0 == data_table_id_ || index_name_.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    len = snprintf(index_table_name_buf_, buf_len, "__idx_%lu_%.*s", data_table_id_, index_name_.length(), index_name_.ptr());

    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_LOG(WDIAG, "fail to fill index table name", K_(data_table_id), K_(index_name), K(ret));
    } else {
      index_table_name_.assign(index_table_name_buf_, len);
    }
  }

  return ret;
}

bool ObRpcOBKVInfo::is_hbase_empty_family() const
{
  bool ret = false;

  if (OB_ISNULL(table_name_.find('$'))) {
    ret = true;
  }

  return ret;
}

void ObRpcOBKVInfo::set_route_entry_dirty()
{
  if (is_global_index_route()) {
    if (OB_NOT_NULL(index_entry_)) {
      index_entry_->cas_set_dirty_state();
      index_entry_->dec_ref();
      index_entry_ = NULL;
    }
    table_id_ = 0;
    data_table_id_ = 0;
    index_table_name_.reset();
    need_add_into_cache_ = false;
  }
  if (OB_NOT_NULL(tablegroup_entry_)) {
    tablegroup_entry_->cas_set_dirty_state();
    tablegroup_entry_->dec_ref();
    tablegroup_entry_= NULL;
  }
}

void ObRpcOBKVInfo::set_rpc_trace_id(const uint64_t id, const uint64_t ipport)
{
  if (!is_set_rpc_trace_id_) {
    rpc_trace_id_.set_rpc_trace_id(id, ipport);
  } else {
    //not reset trace for the same request
    PROXY_LOG(DEBUG, "not to reset trace for the same request", K_(rpc_trace_id));
  }
}

int ObRpcOBKVInfo::set_dummy_entry(ObTableEntry *dummy_entry)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(dummy_entry) || OB_UNLIKELY(!dummy_entry->is_tenant_servers_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WDIAG, "dummy_entry is not avail", KPC(dummy_entry), K(ret));
  } else {
    if (OB_NOT_NULL(dummy_entry_)) {
      dummy_entry_->dec_ref();
      dummy_entry_ = NULL;
    }
    dummy_entry_ = dummy_entry;
    dummy_entry->inc_ref();
  }

  return ret;
}