/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "ob_proxy_rpc_req_split_cont.h"
#include "lib/oblog/ob_log_module.h"
#include "iocore/eventsystem/ob_vconnection.h"
#include "iocore/eventsystem/ob_ethread.h"
#include "iocore/eventsystem/ob_event_processor.h"
#include "iocore/eventsystem/ob_kv_task.h"
#include "stat/ob_rpc_req_stats.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "obproxy/obkv/table/ob_table_rpc_request.h"
#include "proxy/rpc/ob_rpc_request_sm.h"
#include "proxy/rpc/ob_rpc_req.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "proxy/rpc/optimizer/ob_rpc_req_optimizer_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::executor;
using namespace oceanbase::obproxy::obkv;

namespace oceanbase
{
namespace obproxy
{
namespace engine
{

ObProxyRpcReqSplitCont::ObProxyRpcReqSplitCont(proxy::ObRpcReq *root_rpc_req, common::ObIAllocator *allocator)
    : event::ObContinuation(NULL),
      cb_cont_(NULL),
      mutex_(NULL),
      allocator_(allocator),
      execute_thread_(NULL),
      root_rpc_req_(root_rpc_req),
      request_pcode_(obrpc::OB_INVALID_RPC_CODE),
      parallel_param_(common::ObModIds::OB_RPC_TABLE_PROC,
        DEFAULT_PARALLEL_SUB_REQUEST_COUNT * sizeof(executor::ObProxyRpcParallelParam)),
      sub_req_index_(0),
      completed_sub_req_count_(0),
      result_(NULL),
      received_error_event_(false),
      is_canceled_(false),
      detached_(false),
      pending_cb_count_(0){
        fallback_allocator_array_.reserve(DEFAULT_PARALLEL_SUB_REQUEST_COUNT);
      }

ObProxyRpcReqSplitCont::~ObProxyRpcReqSplitCont() {}

int ObProxyRpcReqSplitCont::init_cont(event::ObContinuation *cont)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObProxyRpcReqSplitCont::init start", K_(rpc_trace_id));
  SET_HANDLER(&ObProxyRpcReqSplitCont::main_handler);

  if (OB_ISNULL(root_rpc_req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid root rpc request", K(ret), K_(rpc_trace_id));
  } else if (OB_ISNULL(cont)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid input cont", K(ret), K_(rpc_trace_id));
  } else {
    cb_cont_ = cont;
    mutex_ = cont->mutex_;
    rpc_trace_id_ = root_rpc_req_->get_trace_id();
    request_pcode_ = root_rpc_req_->get_obkv_info().get_pcode();
    root_rpc_req_->set_split_cont_canceled(false);
    if (OB_FAIL(resp_map_.create(DEFAULT_PARALLEL_SUB_REQUEST_COUNT, ObModIds::OB_RPC_TABLE_PROC))) {
      LOG_WDIAG("fail to create resp_map", K(ret), K_(rpc_trace_id));
    } else if (OB_FAIL(allocator_map_.create(DEFAULT_PARALLEL_SUB_REQUEST_COUNT, ObModIds::OB_RPC_TABLE_PROC))) {
      LOG_WDIAG("fail to create allocator_map", K(ret), K_(rpc_trace_id));
    }
  }
  LOG_DEBUG("ObProxyRpcReqSplitCont::init over", K(ret), K_(rpc_trace_id));
  return ret;
}

int ObProxyRpcReqSplitCont::execute_rpc_request()
{
  LOG_DEBUG("ObProxyRpcReqSplitCont::execute_rpc_request enter", K_(rpc_trace_id));
  int ret = OB_SUCCESS;
  execute_thread_ = this_ethread();

  // split root request
  if (OB_ISNULL(root_rpc_req_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("root_rpc_req_ is null", K(ret), K_(rpc_trace_id));
  } else if (OB_FAIL(dispatch_request_by_type(root_rpc_req_, parallel_param_))) {
    LOG_WDIAG("handle shard rpc request failed", K(ret), K_(rpc_trace_id));
  } else if (parallel_param_.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid sharding request to handling", K(ret), K_(rpc_trace_id));
  } else {
    root_rpc_req_->set_snet_state(proxy::ObRpcReq::ServerNetState::RPC_REQ_SERVER_SHARDING_REQUEST_HANDLING);
  }

  // init and start sub sm
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < parallel_param_.count(); i++) {
      executor::ObProxyRpcParallelParam &param = parallel_param_.at(i);
      proxy::ObRpcReq *sub_rpc_req = param.request_;
      if (OB_ISNULL(sub_rpc_req)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid sub request", K(ret), K_(rpc_trace_id), K(i));
      } else if (OB_FAIL(init_and_schedule_sub_sm(sub_rpc_req, i))) {
        LOG_WDIAG("fail to start sub request sm", K(ret), K_(rpc_trace_id), K(i));
      } else {
        param.need_cancel_ = true;
        LOG_DEBUG("schedule sub sm success, set need_cancel_ to true", K(i), K_(rpc_trace_id));
      }
    } // end for
    LOG_DEBUG("ObProxyRpcReqSplitCont::execute_rpc_request over", K(ret), K_(rpc_trace_id), "sub_sm_count", parallel_param_.count());

    if (OB_FAIL(ret)) {
      LOG_WDIAG("execute_rpc_request failed, start to cancel all sub sm", K(ret), K_(rpc_trace_id));
      cancel_recursively();
    }
  }

  return ret;
}

int ObProxyRpcReqSplitCont::init_and_schedule_sub_sm(proxy::ObRpcReq *sub_rpc_req, int64_t index)
{
  int ret = OB_SUCCESS;
  ObRpcRequestSM *sub_sm = NULL;

  // init
  if (OB_ISNULL(sub_rpc_req)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid sub request", K(ret), K_(rpc_trace_id), K(index));
  } else {
    LOG_DEBUG("init sub req", "req", sub_rpc_req, K_(rpc_trace_id));
    sub_rpc_req->set_sub_req_inited(true);
    sub_rpc_req->set_cont_index(index);

    LOG_DEBUG("sub_rpc before send", K(index), "rpc request", sub_rpc_req,
              "info", sub_rpc_req->get_rpc_request()->get_packet_meta(), K_(rpc_trace_id));

    sub_sm = sub_rpc_req->get_request_sm();
    if (OB_ISNULL(sub_sm)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid sub sm", K(ret), K_(rpc_trace_id), K(index));
    } else if (OB_FAIL(sub_sm->init_inner_request(this, new_proxy_mutex()))) {
      LOG_WDIAG("fail to init inner rpc request", K(ret), K_(rpc_trace_id));
      sub_sm->set_inner_cont(NULL);
      sub_sm->cancel_child_callback_action();
    }
  }

  // schedule sub sm
  if (OB_SUCC(ret)) {
    int64_t sub_req_iso_mode = obutils::get_global_proxy_config().rpc_sub_request_isolation_mode;
    int64_t async_thread_count = g_event_processor.thread_count_for_type_[ET_OBKV];
    int64_t async_thread_iso_range = ObRpcReqThreadQpsStat::get_sub_req_async_thread_iso_range();
    ObEvent *scheduled_event = NULL;
    event::ObEThread *target_ethread = NULL;

    if (sub_req_iso_mode == NOT_ISOLAEION || async_thread_count == 0 || async_thread_iso_range == 0) {
      LOG_DEBUG("obkv event processor handle sub request", K_(rpc_trace_id));
      target_ethread = g_event_processor.assign_thread(ET_NET);
    } else if (sub_req_iso_mode == ISOLATE_TO_ALL_ASYNC_THREAD || async_thread_iso_range < 0) {
      LOG_DEBUG("obkv task processor handle sub request", K_(rpc_trace_id));
      target_ethread = g_event_processor.assign_thread(ET_OBKV);
    } else if (sub_req_iso_mode == ISOLATE_TO_PART_ASYNC_THREAD) {
      LOG_DEBUG("obkv task processor handle sub request with range", K(async_thread_iso_range), K_(rpc_trace_id));
      target_ethread = g_event_processor.assign_thread_with_range(ET_OBKV, async_thread_iso_range);
    } else {
      LOG_INFO("invalid rpc_sub_req_isolation_mode, fallback to ET_NET", K(sub_req_iso_mode), K_(rpc_trace_id));
      target_ethread = g_event_processor.assign_thread(ET_NET);
    }

    if (OB_ISNULL(target_ethread)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to assign target thread", K(ret), K_(rpc_trace_id));
    } else {
      // must set execute_thread before schedule
      sub_sm->set_execute_thread(target_ethread);
      scheduled_event = target_ethread->schedule_imm(sub_sm, RPC_REQUEST_SM_START_INNER_REQUEST_PROCESSING);

      if (OB_ISNULL(scheduled_event)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to schedule sub request sm", K(ret), K_(rpc_trace_id));
      } else {
        LOG_DEBUG("success to schedule sub request sm", K_(rpc_trace_id), KP(sub_sm), KP(target_ethread));
      }
    }
  }
  return ret;
}

int ObProxyRpcReqSplitCont::dispatch_request_by_type(proxy::ObRpcReq* root_rpc_req,
                                                      ParallelParamArray &parallel_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_rpc_req)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid rpc request to handle", KP(root_rpc_req), K(ret), K_(rpc_trace_id));
  } else {
    LOG_DEBUG("ObProxyRpcReqSplitCont::dispatch_request_by_type code info", "code", request_pcode_, K_(rpc_trace_id));
    switch (request_pcode_) {
      case obrpc::OB_TABLE_API_BATCH_EXECUTE:
      {
        ret = handle_shard_rpc_obkv_batch_request(root_rpc_req, parallel_param);
        break;
      }
      case obrpc::OB_TABLE_API_LS_EXECUTE:
      {
        ret = handle_shard_rpc_ls_request(root_rpc_req, parallel_param);
        break;
      }
      case obrpc::OB_TABLE_API_EXECUTE_QUERY:
      case obrpc::OB_TABLE_API_QUERY_AND_MUTATE:
      {
        ret = handle_shard_rpc_obkv_query_request(root_rpc_req, parallel_param);
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid pcode for init", K_(request_pcode), K(ret), K_(rpc_trace_id));
        break;
    }
  }
  return ret;
}

int ObProxyRpcReqSplitCont::handle_shard_rpc_obkv_batch_request(proxy::ObRpcReq* root_rpc_req,
                                                                ParallelParamArray &parallel_param)
{
  int ret = OB_SUCCESS;
  ObRpcTableBatchOperationRequest *batch_request = NULL;
  int64_t batch_request_len = sizeof(ObRpcTableBatchOperationRequest);
  if (OB_ISNULL(root_rpc_req)
      || OB_ISNULL(batch_request = dynamic_cast<ObRpcTableBatchOperationRequest *>(root_rpc_req->get_rpc_request()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invlid argument to handle", K(ret), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = root_rpc_req->get_obkv_info();
    LOG_DEBUG("ObProxyRpcReqSplitCont::handle_shard_rpc_obkv_batch_request", K(parallel_param), K(root_rpc_req), "table_id", obkv_info.get_table_id(), K_(rpc_trace_id));
    PARTITION_ID_MAP &partid_to_index_map = batch_request->get_partition_id_map();
    PARTITION_ID_MAP::iterator it = partid_to_index_map.begin();
    PARTITION_ID_MAP::iterator end = partid_to_index_map.end();
    ObSEArray<ObRpcReq *, DEFAULT_PARALLEL_SUB_REQUEST_COUNT> rpc_reqs; //temp variables not to change it
    for (; OB_SUCC(ret) && it != end; it++) {
      int64_t partition_id = it->first;
      ObSEArray<int64_t, 4> &index = it->second;
      ObRpcTableBatchOperationRequest *sub_batch_request = NULL;
      proxy::ObRpcReq *sub_rpc_req = NULL;
      ObRpcRequestSM *request_sm = NULL;
      LOG_DEBUG("handle_shard_rpc_obkv_batch_request handle sub index", K(index), K(partition_id), K_(rpc_trace_id));
      if (index.count() > 0) {
        void *buf = NULL;
        if (OB_ISNULL(buf = op_fixed_mem_alloc(batch_request_len))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("memory not enougth to init rpc request", K(ret), K_(rpc_trace_id));
        } else {
          sub_batch_request = new (buf) ObRpcTableBatchOperationRequest();
          if (OB_ISNULL(sub_rpc_req = ObRpcReq::allocate())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("could not allocate request, abort connection if need", K(ret), K_(rpc_trace_id));
          } else if (OB_ISNULL(request_sm = ObRpcRequestSM::allocate())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("could not allocate request sm, net need abort connection", K(ret), K_(rpc_trace_id));
          }
        }

        if (OB_SUCC(ret) && OB_NOT_NULL(sub_batch_request)) {
          common::ObIAllocator *allocator = NULL;
          ObRpcOBKVInfo &sub_obkv_info = sub_rpc_req->get_obkv_info();
          sub_batch_request->set_packet_meta(batch_request->get_packet_meta());
          sub_batch_request->set_partition_id(partition_id);

          if (OB_FAIL(optimizer::get_global_optimizer_rpc_req_processor().alloc_allocator(allocator))) {
            LOG_WDIAG("alloc allocator failed", K(ret), K_(rpc_trace_id));
          } else if (OB_FAIL(sub_batch_request->init_as_sub_batch_operation_request(*batch_request, index))) {
            LOG_WDIAG("invalid to get sub opertion in batch request", K(ret), K_(rpc_trace_id));
          } else if (OB_FAIL(sub_rpc_req->sub_rpc_req_init(root_rpc_req, request_sm, sub_batch_request, batch_request_len, sub_req_index_ + 1, partition_id, allocator))) {
            LOG_WDIAG("fail to init sub rpc req for batch request", K(ret), K_(rpc_trace_id));
          } else if (request_sm->init(sub_rpc_req)) {
            LOG_WDIAG("fail to init sub request_sm for batch request", K(ret), K_(rpc_trace_id));
          } else {
            executor::ObProxyRpcParallelParam param;

            sub_req_index_++;
            param.partition_id_ = partition_id; //not have any used
            param.request_ = sub_rpc_req;
            request_sm->set_execute_thread(NULL); //not set root ethread to root request, for scheduled error when cleanup
            LOG_DEBUG("handle_shard_rpc_obkv_batch_request ", K(sub_rpc_req), "table_id", sub_obkv_info.get_table_id(), "partition_id", sub_obkv_info.get_partition_id(), K_(rpc_trace_id));
            parallel_param.push_back(param);
            rpc_reqs.push_back(sub_rpc_req);
          }

          if (OB_FAIL(ret) && OB_NOT_NULL(allocator)) {
            optimizer::get_global_optimizer_rpc_req_processor().free_allocator(allocator);
            allocator = NULL;
          }
        }
      }

      if (OB_FAIL(ret)) {
        LOG_DEBUG("clean data in ObProxyRpcReqSplitCont::handle_shard_rpc_obkv_batch_request", K(ret), K_(rpc_trace_id));
        if (OB_NOT_NULL(sub_rpc_req)) {
          sub_rpc_req->destroy();
        }
        if (OB_NOT_NULL(request_sm)) {
          request_sm->destroy();
        }
      }
    } // end for

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < rpc_reqs.count(); ++i) {
        ObRpcReq *sub_rpc_req = rpc_reqs.at(i);
        ObRpcRequestSM *request_sm = NULL;
        if (OB_NOT_NULL(sub_rpc_req)) {
          request_sm = reinterpret_cast<ObRpcRequestSM *>(sub_rpc_req->sm_);
          sub_rpc_req->destroy();
        }
        if (OB_NOT_NULL(request_sm)) {
          request_sm->destroy();
        }
      }
    }
  }
  LOG_DEBUG("ObProxyRpcReqSplitCont::handle_shard_rpc_obkv_batch_request end", K(parallel_param), K_(rpc_trace_id));
  return ret;
}

int ObProxyRpcReqSplitCont::handle_shard_rpc_ls_request(proxy::ObRpcReq *root_rpc_req,
                                                        ParallelParamArray &parallel_param)
{
  int ret = OB_SUCCESS;
  ObRpcTableLSOperationRequest *ls_request = NULL;
  int64_t ls_request_len = sizeof(ObRpcTableLSOperationRequest);
  if (OB_ISNULL(root_rpc_req)
      || OB_ISNULL(ls_request = dynamic_cast<ObRpcTableLSOperationRequest *>(root_rpc_req->get_rpc_request()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invlid argument to handle", K(ret), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = root_rpc_req->get_obkv_info();
    LOG_DEBUG("ObProxyRpcReqSplitCont::handle_shard_rpc_ls_request", K(parallel_param), K(root_rpc_req),
              "table_id", obkv_info.get_table_id(), K_(rpc_trace_id));
    LS_TABLET_ID_MAP::iterator ls_id_iter = ls_request->get_ls_id_tablet_id_map().begin();
    LS_TABLET_ID_MAP::iterator ls_id_end = ls_request->get_ls_id_tablet_id_map().end();
    ObSEArray<ObRpcReq *, 4> rpc_reqs;
    for (; OB_SUCC(ret) && ls_id_iter != ls_id_end; ls_id_iter++) {
      ObRpcTableLSOperationRequest *sub_ls_req = NULL;
      proxy::ObRpcReq *sub_rpc_req = NULL;
      ObSEArray<int64_t, 4> tablet_ids = ls_id_iter->second;
      int64_t ls_id = ls_id_iter->first;
      ObRpcRequestSM *request_sm = NULL;

      void *buf = NULL;
      if (OB_ISNULL(buf = op_fixed_mem_alloc(sizeof(ObRpcTableLSOperationRequest)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("memory not enougth to init rpc request", K(ret), K_(rpc_trace_id));
      } else {
        sub_ls_req = new (buf) ObRpcTableLSOperationRequest();
        if (OB_ISNULL(sub_rpc_req = ObRpcReq::allocate())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("could not allocate request, abort connection if need", K(ret), K_(rpc_trace_id));
        } else if (OB_ISNULL(request_sm = ObRpcRequestSM::allocate())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("could not allocate request sm, net need abort connection", K(ret), K_(rpc_trace_id));
        }
      }

      if (OB_SUCC(ret)) {
        common::ObIAllocator *allocator = NULL;
        int64_t tablet_id = tablet_ids.at(0);
        LOG_DEBUG("handle_shard_rpc_ls_batch_request handle sub index", K(tablet_ids), K(ls_id),K_(rpc_trace_id));
        ObRpcOBKVInfo &sub_obkv_info = sub_rpc_req->get_obkv_info();

        if (OB_FAIL(optimizer::get_global_optimizer_rpc_req_processor().alloc_allocator(allocator))) {
          LOG_WDIAG("alloc allocator failed", K(ret), K_(rpc_trace_id));
        } else if (OB_FAIL(sub_ls_req->init_as_sub_ls_operation_request(*ls_request, ls_id, tablet_ids))) {
          LOG_WDIAG("invalid to get sub opertion in batch request", K(ret), K_(rpc_trace_id));
        } else if (OB_FAIL(sub_rpc_req->sub_rpc_req_init(root_rpc_req, request_sm, sub_ls_req, ls_request_len, sub_req_index_ + 1, tablet_id, allocator, ls_id))) {
          LOG_WDIAG("fail to init sub rpc req for ls request", K(ret), K_(rpc_trace_id));
        } else if (request_sm->init(sub_rpc_req)) {
          LOG_WDIAG("fail to init sub request_sm for ls request", K(ret), K_(rpc_trace_id));
        } else {
          executor::ObProxyRpcParallelParam param;

          sub_req_index_++;
          param.partition_id_ = tablet_id; //not have any used
          param.request_ = sub_rpc_req;
          request_sm->set_execute_thread(NULL); //not set root ethread to root request, for scheduled error when cleanup
          LOG_DEBUG("handle_shard_rpc_obkv_ls_request ", K(*sub_ls_req), K(sub_rpc_req), "table_id", sub_obkv_info.get_table_id(),
                    "partition_id", sub_obkv_info.get_partition_id(), K_(rpc_trace_id));
          parallel_param.push_back(param);
          rpc_reqs.push_back(sub_rpc_req);
        }

        if (OB_FAIL(ret) && OB_NOT_NULL(allocator)) {
          optimizer::get_global_optimizer_rpc_req_processor().free_allocator(allocator);
          allocator = NULL;
        }
      }

      if (OB_FAIL(ret)) {
        LOG_DEBUG("clean data in ObProxyRpcReqSplitCont::handle_shard_rpc_ls_request", K(ret), K_(rpc_trace_id));
        if (OB_NOT_NULL(sub_rpc_req)) {
          sub_rpc_req->destroy();
        }
        if (OB_NOT_NULL(request_sm)) {
          request_sm->destroy();
        }
      }
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < rpc_reqs.count(); ++i) {
        ObRpcReq *sub_rpc_req = rpc_reqs.at(i);
        ObRpcRequestSM *request_sm = NULL;
        if (OB_NOT_NULL(sub_rpc_req)) {
          request_sm = reinterpret_cast<ObRpcRequestSM *>(sub_rpc_req->sm_);
          sub_rpc_req->destroy();
        }
        if (OB_NOT_NULL(request_sm)) {
          request_sm->destroy();
        }
      }
    }
  }
  LOG_DEBUG("ObProxyRpcReqSplitCont::handle_shard_rpc_ls_request end", K(parallel_param), K_(rpc_trace_id));
  return ret;
}

int ObProxyRpcReqSplitCont::handle_shard_rpc_obkv_query_request(proxy::ObRpcReq *root_rpc_req,
                                                                ParallelParamArray &parallel_param)
{
  int ret = OB_SUCCESS;
  ObRpcTableQueryRequest *query_request = NULL;
  int64_t query_request_len = sizeof(ObRpcTableQueryRequest);
  if (OB_ISNULL(root_rpc_req)
   || OB_ISNULL(query_request = dynamic_cast<ObRpcTableQueryRequest *>(root_rpc_req->get_rpc_request()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invlid argument to handle", KP(root_rpc_req), KP(query_request), K(ret), K_(rpc_trace_id));
  } else {
    const ObSEArray<int64_t, 1> &partition_ids = query_request->get_partition_ids();
    ObSEArray<ObRpcReq *, 4> rpc_reqs;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); i++) {
      int64_t partition_id = partition_ids.at(i);
      LOG_DEBUG("handle_shard_rpc_obkv_query_request handle sub index", K(i), K(partition_id), K_(rpc_trace_id));
      ObRpcTableQueryRequest *sub_query_request = NULL;
      proxy::ObRpcReq *sub_rpc_req = NULL;
      ObRpcRequestSM *request_sm = NULL;
      void *buf = NULL;
      if (OB_ISNULL(buf = op_fixed_mem_alloc(sizeof(ObRpcTableQueryRequest)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("memory not enougth to init rpc sub_query_request", K(ret), K_(rpc_trace_id));
      } else {
        sub_query_request = new (buf) ObRpcTableQueryRequest(*query_request);
        if (OB_ISNULL(sub_rpc_req = ObRpcReq::allocate())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("could not allocate request, abort connection if need", K(ret), K_(rpc_trace_id));
        } else if (OB_ISNULL(request_sm =  ObRpcRequestSM::allocate())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("could not allocate request sm, net need abort connection", K(ret), K_(rpc_trace_id));
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(sub_query_request)) {
        common::ObIAllocator *allocator = NULL;
        sub_query_request->set_packet_meta(query_request->get_packet_meta());
        sub_query_request->set_table_operation(query_request->get_query());

        if (OB_FAIL(optimizer::get_global_optimizer_rpc_req_processor().alloc_allocator(allocator))) {
          LOG_WDIAG("alloc allocator failed", K(ret), K_(rpc_trace_id));
        } else if (OB_FAIL(sub_rpc_req->sub_rpc_req_init(root_rpc_req, request_sm, sub_query_request, query_request_len, sub_req_index_ + 1, partition_id, allocator))) {
          LOG_WDIAG("fail to init sub rpc req for query request", K(ret), K_(rpc_trace_id));
        } else if (request_sm->init(sub_rpc_req)) {
          LOG_WDIAG("fail to init sub request_sm for query request", K(ret), K_(rpc_trace_id));
        } else {
          executor::ObProxyRpcParallelParam param;
          param.partition_id_ = partition_id;
          param.request_ = sub_rpc_req;
          request_sm->set_execute_thread(NULL); //not set root ethread to root request, for scheduled error when cleanup

          LOG_DEBUG("sub root_rpc_req init done", KPC(root_rpc_req), KPC(sub_rpc_req), K_(rpc_trace_id));
          sub_req_index_++;
          parallel_param.push_back(param);
          rpc_reqs.push_back(sub_rpc_req);
        }

        if (OB_FAIL(ret) && OB_NOT_NULL(allocator)) {
          optimizer::get_global_optimizer_rpc_req_processor().free_allocator(allocator);
          allocator = NULL;
        }
      }

      if (OB_FAIL(ret)) {
        LOG_DEBUG("clean data in ObProxyRpcReqSplitCont::handle_shard_rpc_obkv_query_request", K(ret), K_(rpc_trace_id));
        if (OB_NOT_NULL(sub_rpc_req)) {
          sub_rpc_req->destroy();
        }
        if (OB_NOT_NULL(request_sm)) {
          request_sm->destroy();
        }
      }
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < rpc_reqs.count(); ++i) {
        ObRpcReq *sub_rpc_req = rpc_reqs.at(i);
        ObRpcRequestSM *request_sm = NULL;
        if (OB_NOT_NULL(sub_rpc_req)) {
          request_sm = reinterpret_cast<ObRpcRequestSM *>(sub_rpc_req->sm_);
          sub_rpc_req->destroy();
        }
        if (OB_NOT_NULL(request_sm)) {
          request_sm->destroy();
        }
      }
    }
  }
  LOG_DEBUG("ObProxyRpcReqSplitCont::handle_shard_rpc_obkv_query_request end", K(parallel_param), K_(rpc_trace_id));
  return ret;
}

int ObProxyRpcReqSplitCont::handle_result(void *data, bool &is_final)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input, data is NULL", K(ret), K_(rpc_trace_id));
  } else {
    proxy::ObRpcReq *sub_rpc_req = reinterpret_cast<proxy::ObRpcReq*>(data);

    if (!sub_rpc_req->has_resultset_resp()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("handle_result get not response", KPC(sub_rpc_req), K_(rpc_trace_id));
    } else if (sub_rpc_req->has_error_resp()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("error response couldn't be here, it should be handled by main_handler", KPC(sub_rpc_req), K_(rpc_trace_id));
    } else {
      if (OB_FAIL(dispatch_result_by_type(sub_rpc_req, is_final))) {
        LOG_WDIAG("failed to handle request type specific result", K(ret), K_(rpc_trace_id));
      }
    }
    LOG_DEBUG("handle_result over", K(ret), K(sub_rpc_req), K_(rpc_trace_id));
  }

  return ret;
}

int ObProxyRpcReqSplitCont::dispatch_result_by_type(proxy::ObRpcReq *sub_rpc_req, bool &is_final)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sub_rpc_req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("sub_rpc_req is NULL", K(ret), K_(rpc_trace_id));
  } else {
    proxy::ObRpcReq *root_rpc_req = get_root_rpc_req();
    if (OB_ISNULL(root_rpc_req)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("root rpc req is NULL", K(ret), K_(rpc_trace_id));
    } else {
      LOG_DEBUG("dispatch_result_by_type", K_(request_pcode), K_(rpc_trace_id));

      switch (request_pcode_) {
        case obrpc::OB_TABLE_API_EXECUTE_QUERY:
        case obrpc::OB_TABLE_API_QUERY_AND_MUTATE:
        {
          if (OB_FAIL(handle_query_result(sub_rpc_req, is_final))) {
            LOG_WDIAG("failed to handle query result", K(ret), K_(rpc_trace_id));
          }
          break;
        }
        case obrpc::OB_TABLE_API_BATCH_EXECUTE:
        {
          if (OB_FAIL(handle_batch_result(sub_rpc_req, is_final))) {
            LOG_WDIAG("failed to handle batch result", K(ret), K_(rpc_trace_id));
          }
          break;
        }
        case obrpc::OB_TABLE_API_LS_EXECUTE:
        {
          if (OB_FAIL(handle_ls_result(sub_rpc_req, is_final))) {
            LOG_WDIAG("failed to handle ls result", K(ret), K_(rpc_trace_id));
          }
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unsupported pcode for specific handling", K(ret), K_(request_pcode), K_(rpc_trace_id));
          break;
      }
    }
  }

  return ret;
}

int ObProxyRpcReqSplitCont::handle_query_result(proxy::ObRpcReq *sub_rpc_req_in, bool &is_final)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sub_rpc_req_in)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("sub_rpc_req is NULL", K(ret), KP(sub_rpc_req_in), K_(rpc_trace_id));
  } else {
    int64_t index = resp_map_.size();
    obkv::ObRpcResponse *sub_resp = sub_rpc_req_in->get_rpc_response();
    if (OB_ISNULL(sub_resp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("sub_resp is NULL", K(ret), K_(rpc_trace_id));
    } else if (OB_FAIL(resp_map_.set_refactored(index, sub_resp))) {
      LOG_WDIAG("set resp_map_ failed", K(ret), K_(rpc_trace_id));
    }
  }

  if (OB_SUCC(ret) && is_final) {
    /* has received all response from server */
    int64_t count = resp_map_.size();
    bool has_inited_meta = false;
    int64_t i = 0;
    ObRpcReq *root_rpc_req = get_root_rpc_req();
    LOG_DEBUG("ObProxyRpcReqSplitCont::handle_query_result", K(count), K_(rpc_trace_id));
    obkv::ObRpcTableQueryResponse *root_resp = NULL;
    ObRpcOBKVInfo &obkv_info = root_rpc_req->get_obkv_info();

    if (OB_FAIL(root_rpc_req->alloc_rpc_response())
       || OB_ISNULL(root_resp = dynamic_cast<obkv::ObRpcTableQueryResponse *>(root_rpc_req->get_rpc_response()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("invalid alloc memory", K(ret), K_(rpc_trace_id));
    }

    for (i = 0; i < count && OB_SUCC(ret); i++) {
      obkv::ObRpcResponse *sub_resp = NULL;
      obkv::ObRpcTableQueryResponse *query_resp = NULL;

      if (OB_FAIL(resp_map_.get_refactored(i, sub_resp))
        || OB_ISNULL(sub_resp)
        || OB_ISNULL(query_resp = dynamic_cast<obkv::ObRpcTableQueryResponse *>(sub_resp))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid response to handle", K_(rpc_trace_id), KP(sub_resp), K(ret), K(i));
      } else {
        if (!has_inited_meta) {
          int old_ret = ret;
          root_resp->set_packet_meta(query_resp->get_packet_meta());
          if (OB_FAIL(root_resp->get_query_result().add_all_property_shallow_copy(query_resp->get_query_result()))) {
            LOG_WDIAG("invalid properity get from server", K(ret), K_(rpc_trace_id));
          } else {
            has_inited_meta = true;
          }
          ret = old_ret;
        }

        int tmp = root_resp->get_query_result().add_all_row_shallow_copy(query_resp->get_query_result());
        LOG_DEBUG("ObProxyRpcReqSplitCont::handle_query_result", KPC(query_resp),
                  "row_count", query_resp->get_query_result().get_row_count(),
                  "buf", query_resp->get_query_result().get_buf(),
                  "return", tmp, K_(rpc_trace_id));
        query_resp = NULL;
        sub_resp = NULL;
      }
    }

    if (OB_SUCC(ret)) {
      if (!has_inited_meta) {
        ObRpcRequest *root_request = NULL;
        if (OB_ISNULL(root_request = root_rpc_req->get_rpc_request())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("split cont shard query meet error, not init meta and get NULL request", KPC(root_rpc_req), K_(rpc_trace_id));
        } else {
          ObRpcPacketMeta &meta = root_resp->get_packet_meta();
          memcpy(&meta.ez_header_, &(root_request->get_packet_meta().ez_header_), sizeof(meta.ez_header_));
          memcpy(&meta.rpc_header_, &(root_request->get_packet_meta().rpc_header_), sizeof(meta.rpc_header_));
          meta.rpc_header_.flags_ &= (uint16_t)~(ObRpcPacketHeader::REQUIRE_REROUTING_FLAG);  // clear reroute flag
          meta.rpc_header_.flags_ |= obrpc::ObRpcPacketHeader::RESP_FLAG;
        }
      }
      root_resp->get_packet_meta().ez_header_.ez_payload_size_ = (uint32_t)root_resp->get_packet_meta().rpc_header_.hlen_
          + (int32_t)(root_resp->get_query_result().get_result_size());
      LOG_DEBUG("ObProxyRpcReqSplitCont::handle_query_result final info", "row_count",
                root_resp->get_query_result().get_row_count(),
                "field_count", root_resp->get_query_result().get_property_count(),
                "new size",  root_resp->get_packet_meta().ez_header_.ez_payload_size_, K_(rpc_trace_id));
      obkv_info.set_resp(true);
      root_rpc_req->set_rpc_response(root_resp);
      result_ = root_rpc_req;
    }
  }

  return ret;
}

int ObProxyRpcReqSplitCont::handle_batch_result(proxy::ObRpcReq *sub_rpc_req, bool &is_final)
{
  int ret = OB_SUCCESS;
  obkv::ObRpcResponse *last_sub_resp = NULL;
  proxy::ObRpcReq *root_rpc_req = get_root_rpc_req();

  if (OB_ISNULL(sub_rpc_req)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("sub_rpc_req is NULL", K(ret), K_(rpc_trace_id));
  } else {
    obkv::ObRpcResponse *sub_resp = sub_rpc_req->get_rpc_response();
    int64_t index = sub_rpc_req->get_cont_index();
    if (OB_ISNULL(sub_resp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("sub_resp is NULL", K(ret), K_(rpc_trace_id));
    } else if (OB_FAIL(resp_map_.set_refactored(index, sub_resp))) {
      LOG_WDIAG("set resp_map_ failed", K(ret), K_(rpc_trace_id));
    }
    LOG_DEBUG("ObProxyRpcReqSplitCont::handle_batch_result", KPC(sub_rpc_req), "index", sub_rpc_req->get_cont_index(),
              "has_response", sub_rpc_req->has_resultset_resp(), "partition id", sub_rpc_req->get_obkv_info().get_partition_id(),
              K(is_final), KPC(root_rpc_req), K_(rpc_trace_id));
  }

  if (OB_SUCC(ret) && is_final) {
    /* has received all response from server */
    ObRpcTableBatchOperationRequest *root_request = NULL;
    if (OB_ISNULL(root_rpc_req)
      || OB_ISNULL(root_request = reinterpret_cast<ObRpcTableBatchOperationRequest *>(root_rpc_req->get_rpc_request()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid shard root_request has set in", K(root_request), K(ret), K_(rpc_trace_id));
    } else {
      obkv::ObRpcTableBatchOperationResponse *root_resp = NULL;
      bool is_return_one_result = root_request->return_one_result();
      proxy::ObRpcOBKVInfo &obkv_info = root_rpc_req->get_obkv_info();

      LOG_DEBUG("need alloc memory for response", "size", sizeof(obkv::ObRpcTableBatchOperationResponse), K_(rpc_trace_id));

      if (OB_SUCC(ret)) {
        if (OB_FAIL(root_rpc_req->alloc_rpc_response())
           || OB_ISNULL(root_resp = dynamic_cast<obkv::ObRpcTableBatchOperationResponse *>(root_rpc_req->get_rpc_response()))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("not enougth alloc memory", K_(rpc_trace_id));
        } else {
          if (is_return_one_result) {
            if (OB_FAIL(generate_batch_one_result_resp(root_resp->get_batch_result(), last_sub_resp))) {
              LOG_WDIAG("fail to generate one result resp for batch operation", K(ret), K_(rpc_trace_id));
            }
          } else {
            if (OB_FAIL(generate_batch_normal_resp(root_resp->get_batch_result(), *root_request, last_sub_resp))) {
              LOG_WDIAG("fail to generate response for batch operation", K(ret), K_(rpc_trace_id));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        LOG_DEBUG("batch operation get result", "batch_result", root_resp->get_batch_result(), K_(rpc_trace_id));
        // need to update sum of the length in meta
        if (OB_ISNULL(last_sub_resp)) {
          /* just used rpc_request meta info when meet error */
          root_resp->set_packet_meta(root_rpc_req->get_rpc_request()->get_packet_meta());
          root_resp->get_packet_meta().rpc_header_.flags_ |= ObRpcPacketHeader::RESP_FLAG;
        } else {
          root_resp->set_packet_meta(last_sub_resp->get_packet_meta());
        }
        root_resp->get_packet_meta().ez_header_.ez_payload_size_ = (uint32_t)(root_resp->get_encode_size() - 16); //EZ_HEADER_LEN
        obkv_info.set_resp(true);
        result_ = root_rpc_req;
        LOG_DEBUG("rpc response", K(root_resp), K(*root_resp), K_(result), K_(rpc_trace_id));
      }

    }
  }
  return ret;
}

int ObProxyRpcReqSplitCont::handle_ls_result(proxy::ObRpcReq *sub_rpc_req, bool &is_final)
{
  int ret = OB_SUCCESS;
  obkv::ObRpcResponse *last_sub_resp = NULL;
  proxy::ObRpcReq *root_rpc_req = get_root_rpc_req();

  if (OB_ISNULL(sub_rpc_req)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("sub_rpc_req is NULL", K(ret), K_(rpc_trace_id));
  } else {
    obkv::ObRpcResponse *sub_resp = sub_rpc_req->get_rpc_response();
    int64_t index = sub_rpc_req->get_cont_index();
    if (OB_ISNULL(sub_resp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("sub_resp is NULL", K(ret), K_(rpc_trace_id));
    } else {
      obkv::ObRpcTableLSOperationResponse *ls_resp = dynamic_cast<obkv::ObRpcTableLSOperationResponse *>(sub_resp);
      if (OB_NOT_NULL(ls_resp)) {
        if (OB_FAIL(clone_ls_response(*ls_resp))) {
          LOG_WDIAG("fail to clone ls response zero copy buffers", K(ret), K_(rpc_trace_id));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid response to handle", K(sub_resp), K(ret), K(index), K_(rpc_trace_id));
      }
      if (OB_SUCC(ret) && OB_FAIL(resp_map_.set_refactored(index, sub_resp))) {
        LOG_WDIAG("set resp_map_ failed", K(ret), K_(rpc_trace_id));
      }
    }
    LOG_DEBUG("ObProxyRpcReqSplitCont::handle_ls_result", KPC(sub_rpc_req), "index", sub_rpc_req->get_cont_index(),
              "has_response", sub_rpc_req->has_resultset_resp(), K(is_final), KPC(root_rpc_req), K_(rpc_trace_id));
  }

  if (OB_SUCC(ret) && is_final) {
    ObRpcTableLSOperationRequest *root_request = NULL;
    if (OB_ISNULL(root_rpc_req)
        || OB_ISNULL(root_request = reinterpret_cast<ObRpcTableLSOperationRequest *>(root_rpc_req->get_rpc_request()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid shard root_request has set in", K(root_request), K(ret), K_(rpc_trace_id));
    } else {
      proxy::ObRpcOBKVInfo &obkv_info = root_rpc_req->get_obkv_info();
      bool return_one_result = root_request->get_operation().return_one_result();
      obkv::ObRpcTableLSOperationResponse *root_resp = NULL;
      LOG_DEBUG("need alloc memory for response", "size", sizeof(obkv::ObRpcTableLSOperationResponse), K_(rpc_trace_id));

      if (OB_FAIL(root_rpc_req->alloc_rpc_response())
         || OB_ISNULL(root_resp = dynamic_cast<obkv::ObRpcTableLSOperationResponse *>(root_rpc_req->get_rpc_response()))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("not enougth alloc memory", K_(rpc_trace_id));
      } else {
        if (return_one_result) {
          if (OB_FAIL(generate_ls_one_result_resp(root_resp->get_ls_result(), last_sub_resp))) {
            LOG_WDIAG("fail to generate one result resp", K(ret), K_(rpc_trace_id));
          }
        } else {
          if (OB_FAIL(generate_ls_normal_resp(root_resp->get_ls_result(), *root_request, last_sub_resp))) {
            LOG_WDIAG("fail to generate resp for ls operation", K(ret), K_(rpc_trace_id));
          }
        }

        if (OB_SUCC(ret)) {
          LOG_DEBUG("ls operation get result", "ls_result", root_resp->get_ls_result(), K_(rpc_trace_id));
          // need to update sum of the length in meta
          if (OB_ISNULL(last_sub_resp)) {
            // init rpc response
            root_resp->get_ls_result().set_all_properties_names(root_request->get_operation().get_all_properties_names());
            root_resp->get_ls_result().set_all_rowkey_names(root_request->get_operation().get_all_rowkey_names());

            /* just used rpc_request meta info when meet error */
            root_resp->set_packet_meta(root_rpc_req->get_rpc_request()->get_packet_meta());
            root_resp->get_packet_meta().rpc_header_.flags_ |= ObRpcPacketHeader::RESP_FLAG;
          } else {
            //by response
            obkv::ObRpcTableLSOperationResponse *sub_ls_resp = dynamic_cast<obkv::ObRpcTableLSOperationResponse *>(last_sub_resp);
            root_resp->get_ls_result().set_all_properties_names(sub_ls_resp->get_ls_result().get_properties_names());
            root_resp->get_ls_result().set_all_rowkey_names(sub_ls_resp->get_ls_result().get_rowkey_names());

            root_resp->set_packet_meta(last_sub_resp->get_packet_meta());
          }

          root_resp->get_packet_meta().ez_header_.ez_payload_size_ = (uint32_t)(root_resp->get_encode_size() - 16); //EZ_HEADER_LEN
          obkv_info.set_resp(true);
          result_ = root_rpc_req;

          LOG_DEBUG("rpc response", K(root_resp), K(*root_resp), K_(result), K_(rpc_trace_id));
        }
      }
    }
  }

  return ret;
}

int ObProxyRpcReqSplitCont::generate_batch_one_result_resp(obkv::ObTableBatchOperationResult &root_batch_result, obkv::ObRpcResponse *&last_sub_resp)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("split cont generate one result resp", K_(rpc_trace_id));
  ObTableOperationResult table_operation_res;
  RESP_MAP::iterator it = resp_map_.begin();
  RESP_MAP::iterator end = resp_map_.end();
  for (int i = 0; it != end && OB_SUCC(ret); it++, i++) {
    obkv::ObRpcResponse *sub_resp = it->second;
    obkv::ObRpcTableBatchOperationResponse *batch_resp = NULL;
    int64_t table_result_count = 0;
    if (OB_ISNULL(sub_resp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("sub_resp is NULL", K(ret), K_(rpc_trace_id));
    } else if (OB_ISNULL(batch_resp = dynamic_cast<obkv::ObRpcTableBatchOperationResponse *>(sub_resp))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid response to handle", K(sub_resp), K(ret), K(index), K_(rpc_trace_id));
    } else if (FALSE_IT(table_result_count = batch_resp->get_batch_result().get_table_operation_result_count())) {
      // do nothing
    } else if (table_result_count != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected table operation count in returning_one_result mode", K(ret), K_(rpc_trace_id), "count",
                table_result_count);
    } else {
      const ObTableOperationResult &mmres = batch_resp->get_batch_result().at(0);
      last_sub_resp = batch_resp;
      if (i == 0) {
        table_operation_res = mmres;
      } else {
        table_operation_res.set_affected_rows(mmres.get_affected_rows() + table_operation_res.get_affected_rows());
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(root_batch_result.push_back(table_operation_res))) {
    LOG_WDIAG("fail to push back table operation result", K(ret), K_(rpc_trace_id));
  }
  return ret;
}

int ObProxyRpcReqSplitCont::generate_batch_normal_resp(obkv::ObTableBatchOperationResult &root_batch_result, obkv::ObRpcTableBatchOperationRequest &root_request, obkv::ObRpcResponse *&last_sub_resp)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<int64_t, 4> sub_request_index_arr;
  common::ObSEArray<int64_t, 4> all_operation_index_arr;
  PARTITION_ID_MAP &partid_to_index_map = root_request.get_partition_id_map();
  int64_t all_operation_count = root_request.get_sub_req_count();
  int64_t sub_request_count = partid_to_index_map.size();

  for (int64_t i = 0; OB_SUCC(ret) && i < sub_request_count; i++) {
    if (OB_FAIL(sub_request_index_arr.push_back(0))) {
      LOG_WDIAG("fail to call push_back for sub_request_index_arr", K(ret), K(i), K(sub_request_count),
                K_(rpc_trace_id));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_operation_count; i++) {
    if (OB_FAIL(all_operation_index_arr.push_back(0))) {
      LOG_WDIAG("fail to call push_back for all_operation_index_arr", K(ret), K(i), K(all_operation_count),
                K_(rpc_trace_id));
    }
  }

  PARTITION_ID_MAP::iterator it = partid_to_index_map.begin();
  PARTITION_ID_MAP::iterator end = partid_to_index_map.end();
  int index = 0;
  for (; it != end && OB_SUCC(ret); it++, index++) {
    ObSEArray<int64_t, 4> &index_arr = it->second;
    for (int64_t i = 0; i < index_arr.count(); i++) {
      all_operation_index_arr.at(index_arr[i]) = index;
      LOG_DEBUG("batch index info", "pos", index_arr[i], K(index), "partition id", it->first, K_(rpc_trace_id));
    }
  }

  for (int64_t i = 0; i < all_operation_count && OB_SUCC(ret); i++) {
    obkv::ObRpcResponse *sub_resp = NULL;
    obkv::ObRpcTableBatchOperationResponse *batch_resp = NULL;
    int64_t index = all_operation_index_arr[i];
    if (OB_FAIL(resp_map_.get_refactored(index, sub_resp))
        || OB_ISNULL(sub_resp)
        || OB_ISNULL(batch_resp = dynamic_cast<obkv::ObRpcTableBatchOperationResponse *>(sub_resp))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid response to handle", K(sub_resp), K(ret), K(index), K_(rpc_trace_id));
    } else {
      int64_t sub_request_index = sub_request_index_arr[index];
      if (sub_request_index >= batch_resp->get_batch_result().get_table_operation_result_count()) {
        if (1 == batch_resp->get_batch_result().get_table_operation_result_count()
            && ObTableEntityType::ET_HKV == root_request.get_entity_type()) {
          sub_request_index = 0; // 对于hbase请求，并且response为1个情况情况下，如果request
                                 // index大于response结果，取第一个response覆盖所有的response即可
          LOG_DEBUG("Hbase root_request result set is 1, and the first result set is used by default",
                    KPC(batch_resp), K_(rpc_trace_id));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("get a invalid sub_request_index", K(sub_request_index), KPC(batch_resp),
                    K(index), K_(rpc_trace_id));
        }
      } else {
        // do nothing
      }
      if (OB_SUCC(ret)) {
        LOG_DEBUG("batch operation get result", "batch", batch_resp->get_batch_result(), "result",
                  batch_resp->get_batch_result().at(sub_request_index), K_(rpc_trace_id));
        const ObTableOperationResult &mmres = batch_resp->get_batch_result().at(sub_request_index);
        root_batch_result.push_back(mmres);
        last_sub_resp = batch_resp;
        sub_request_index_arr.at(index) += 1;
      }
    }
  }
  sub_request_index_arr.reset();
  all_operation_index_arr.reset();

  return ret;
}

int ObProxyRpcReqSplitCont::generate_ls_one_result_resp(obkv::ObTableLSOpResult &root_ls_op_result, obkv::ObRpcResponse *&last_sub_resp)
{
  int ret = OB_SUCCESS;
  ObTableSingleOpResult single_op_result;
  LOG_DEBUG("split cont generate one result resp", K_(rpc_trace_id));
  RESP_MAP::iterator it = resp_map_.begin();
  RESP_MAP::iterator end = resp_map_.end();
  ObTableTabletOpResult tablet_op_result;
  tablet_op_result.set_all_rowkey_names(&root_ls_op_result.get_rowkey_names());
  tablet_op_result.set_all_properties_names(&root_ls_op_result.get_properties_names());

  for (int64_t i = 0; it != end && OB_SUCC(ret); it++, i++) {
    obkv::ObRpcResponse *sub_resp = it->second;
    obkv::ObRpcTableLSOperationResponse *ls_resp = NULL;
    if (OB_ISNULL(sub_resp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("sub_resp is NULL", K(ret), K_(rpc_trace_id));
    } else if (OB_ISNULL(ls_resp = dynamic_cast<obkv::ObRpcTableLSOperationResponse *>(sub_resp))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid response to handle", K(sub_resp), K(ret), K(index), K_(rpc_trace_id));
    } else {
      ObTableLSOpResult &lso_mmres = ls_resp->get_ls_result();
      int tablet_res_count = lso_mmres.get_tablet_op_result().count();
      for (int j = 0; j < tablet_res_count && OB_SUCC(ret); j ++ ) {
        int single_res_count = lso_mmres.get_tablet_op_result().at(j).get_single_op_result().count();
        if (single_res_count != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected single result count in returning_one_result mode", K(ret), K(single_res_count), K_(rpc_trace_id));
        } else {
          ObTableTabletOpResult &tablet_mmres = lso_mmres.get_tablet_op_result().at(j);
          ObTableSingleOpResult &mmres = tablet_mmres.get_single_op_result().at(0);
          if (i == 0 && j == 0) {
            single_op_result = mmres;
          } else {
            single_op_result.set_affected_rows(mmres.get_affected_rows() + single_op_result.get_affected_rows());
          }
          LOG_DEBUG("split cont ls operation get result succ", K(ls_resp), K(mmres), K(i), K(j),
                    K_(rpc_trace_id));
        }
      }
      if (OB_SUCC(ret)) {
        last_sub_resp = ls_resp;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_op_result.get_single_op_result().push_back(single_op_result))) {
      LOG_WDIAG("fail to push back single operation", K(ret), K_(rpc_trace_id));
    } else if (OB_FAIL(root_ls_op_result.get_tablet_op_result().push_back(tablet_op_result))) {
      LOG_WDIAG("fail to push back tablet operation", K(ret), K_(rpc_trace_id));
    }
  }
  return ret;
}

int ObProxyRpcReqSplitCont::generate_ls_normal_resp(obkv::ObTableLSOpResult &root_ls_op_result,
                                                obkv::ObRpcTableLSOperationRequest &ls_request,
                                                obkv::ObRpcResponse *&last_sub_resp)
{
  int ret = OB_SUCCESS;
  int64_t *all_ls_op_index_arr = NULL;                                           // record which ls resp belongs
  int64_t *all_tablet_op_index_arr = NULL;                                       // record which tablet resp belongs
  int64_t *all_single_op_index_arr = NULL;                                       // record which single resp belongs
  LS_TABLET_ID_MAP &ls_id_tablet_id_map = ls_request.get_ls_id_tablet_id_map(); // the map record ls id of all tabelt_id
  TABLET_ID_INDEX_MAP &tablet_id_map = ls_request.get_tablet_id_index_map(); // the map record original index of all  single op

  int64_t all_operation_count = ls_request.get_sub_req_count();
  int64_t all_tablet_op_count = ls_request.get_operation().get_tablet_ops().count();
  int64_t sub_tablet_request_count = tablet_id_map.size();
  int64_t sub_ls_request_count = ls_id_tablet_id_map.size();

  LOG_DEBUG("split cont received ls shard resp completly", K(sub_ls_request_count), K(sub_tablet_request_count),
            K(all_operation_count), K_(rpc_trace_id));
  if (OB_FAIL(init_index_arr(all_ls_op_index_arr, all_operation_count))) {
    LOG_WDIAG("fail to init all ls operation index", K(ret), K_(rpc_trace_id));
  } else if (OB_FAIL(init_index_arr(all_tablet_op_index_arr, all_operation_count))) {
    LOG_WDIAG("fail to init ls operation index", K(ret), K_(rpc_trace_id));
  } else if (OB_FAIL(init_index_arr(all_single_op_index_arr, all_operation_count))) {
    LOG_WDIAG("fail to init sginle operation index", K(ret), K_(rpc_trace_id));
  }
  LS_TABLET_ID_MAP::iterator ls_it = ls_id_tablet_id_map.begin();
  LS_TABLET_ID_MAP::iterator ls_end = ls_id_tablet_id_map.end();

  // calc sub response index of all single operation:
  // we build the parallel async task in the same way, so the index of iterator is the index of sub response of resp_map
  for (int ls_index = 0; ls_it != ls_end && OB_SUCC(ret); ls_it++, ls_index++) {
    ObSEArray<int64_t, 4> &tablet_id_arr = ls_it->second;
    ObSEArray<int64_t, 4>::iterator tablet_it = tablet_id_arr.begin();
    ObSEArray<int64_t, 4>::iterator tablet_end = tablet_id_arr.end();
    for (int tablet_index = 0; tablet_it != tablet_end && OB_SUCC(ret); tablet_it++, tablet_index++) {
      int64_t tablet_id = *tablet_it;
      ObSEArray<int64_t, 4> index_arr;
      if (OB_FAIL(tablet_id_map.get_refactored(tablet_id, index_arr))) {
        LOG_WDIAG("fail to get index arr", K(tablet_id), K(ret), K_(rpc_trace_id));
      } else {
        ObSEArray<int64_t, 4>::iterator single_it = index_arr.begin();
        ObSEArray<int64_t, 4>::iterator singel_end = index_arr.end();

        for (int single_index = 0; single_it != singel_end; single_it++, single_index++) {
          int64_t origin_location = index_arr[single_index];
          if (origin_location >= all_operation_count) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpected single op index", K(origin_location), K(all_operation_count), K(ret),
                      K_(rpc_trace_id));
          } else {
            all_ls_op_index_arr[origin_location] = ls_index;
            all_tablet_op_index_arr[origin_location] = tablet_index;
            all_single_op_index_arr[origin_location] = single_index;
          }
        }
      } // loop original single op index
    }   // loop tablet id
  }     // loop ls id

  int offset = 0;
  for (int i = 0; i < all_tablet_op_count && OB_SUCC(ret); i++) {
    ObTableTabletOpResult tablet_op_result;
    tablet_op_result.set_all_rowkey_names(&root_ls_op_result.get_rowkey_names());
    tablet_op_result.set_all_properties_names(&root_ls_op_result.get_properties_names());
    // push back a default tablet op
    if (OB_FAIL(root_ls_op_result.get_tablet_op_result().push_back(tablet_op_result))) {
      LOG_WDIAG("fail to push back tablet op resutl", K(ret), K_(rpc_trace_id));
    }
    int64_t all_single_op_count = ls_request.get_operation().get_tablet_ops().at(i).count();
    for (int j = 0; j < all_single_op_count && OB_SUCC(ret); j++) {
      obkv::ObRpcResponse *sub_resp = NULL;
      obkv::ObRpcTableLSOperationResponse *ls_resp = NULL;
      int64_t ls_index = all_ls_op_index_arr[offset];
      int64_t tablet_index = all_tablet_op_index_arr[offset];
      int64_t single_index = all_single_op_index_arr[offset];
      if (OB_FAIL(resp_map_.get_refactored(ls_index, sub_resp))
          || OB_ISNULL(sub_resp)
          || OB_ISNULL(ls_resp = dynamic_cast<obkv::ObRpcTableLSOperationResponse *>(sub_resp))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid response to handle", K(sub_resp), K(ret), K(ls_index), K(tablet_index), K(single_index),
                  "request_offset", offset, K_(rpc_trace_id));
      } else {
        ObTableLSOpResult &lso_mmres = ls_resp->get_ls_result();
        if (OB_UNLIKELY(lso_mmres.get_tablet_op_result().count() <= tablet_index)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected tablet op result count", K(tablet_index), "tablet_resp_count",
                    lso_mmres.get_tablet_op_result().count(), K(ret), K_(rpc_trace_id));
        } else if (lso_mmres.get_tablet_op_result().at(tablet_index).get_single_op_result().count() == 1
                   && ObTableEntityType::ET_HKV == ls_request.get_entity_type()) {
          single_index = 0;
        }

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_UNLIKELY(lso_mmres.get_tablet_op_result().at(tablet_index).get_single_op_result().count() <= single_index)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected single op result count", K(tablet_index), K(single_index),
                    "single_resp_count", root_ls_op_result.get_tablet_op_result().at(tablet_index).get_single_op_result().count(),
                    "tablet_resp_count", lso_mmres.get_tablet_op_result().count(), K(ret), K_(rpc_trace_id));
        } else {
          ObTableTabletOpResult &tablet_mmres = lso_mmres.get_tablet_op_result().at(tablet_index);
          ObTableSingleOpResult &mmres = tablet_mmres.get_single_op_result().at(single_index);
          if (OB_FAIL(root_ls_op_result.get_tablet_op_result().at(i).get_single_op_result().push_back(mmres))) {
            LOG_WDIAG("fail to push single op result", K(ret), K_(rpc_trace_id));
          } else {
            last_sub_resp = ls_resp;
            LOG_DEBUG("split cont ls operation get result succ", K(ls_resp), K(mmres), K(offset), K(ls_index),
                      K(tablet_index), K(single_index), K_(rpc_trace_id));
          }
        }
        offset++;
      }
    }
  }
  free_index_arr(all_ls_op_index_arr, all_operation_count);
  free_index_arr(all_tablet_op_index_arr, all_operation_count);
  free_index_arr(all_single_op_index_arr, all_operation_count);
  return ret;
}

int ObProxyRpcReqSplitCont::clone_ls_response(obkv::ObRpcTableLSOperationResponse &ls_resp)
{
  int ret = OB_SUCCESS;
  obkv::ObTableLSOpResult &ls_result = ls_resp.get_ls_result();
  int64_t tablet_count = ls_result.get_tablet_op_result().count();
  for (int64_t i = 0; i < tablet_count && OB_SUCC(ret); ++i) {
    ObTableTabletOpResult &tablet_res = ls_result.get_tablet_op_result().at(i);
    int64_t single_count = tablet_res.get_single_op_result().count();
    for (int64_t j = 0; j < single_count && OB_SUCC(ret); ++j) {
      ObTableSingleOpResult &single_res = tablet_res.get_single_op_result().at(j);
      if (OB_FAIL(single_res.deep_copy_buffers(*allocator_))) {
        LOG_WDIAG("fail to clone single result zero copy buffers", K(ret), K_(rpc_trace_id), K(i), K(j));
      }
    }
  }
  return ret;
}

int ObProxyRpcReqSplitCont::init_index_arr(int64_t *&arr, const int64_t count)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_ISNULL(buf = op_fixed_mem_alloc(sizeof(int64_t) * count))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc index arr", K(ret), K(count), K_(rpc_trace_id));
  } else {
    MEMSET(buf, 0, sizeof(int64_t) * count);
    arr = static_cast<int64_t*>(buf);
  }
  return ret;
}

void ObProxyRpcReqSplitCont::free_index_arr(int64_t *&arr, const int64_t count)
{
  if (OB_NOT_NULL(arr)) {
    op_fixed_mem_free(arr, sizeof(int64_t) * count);
    arr = NULL; //avoid double free
  }
}

int ObProxyRpcReqSplitCont::generate_error_resp(proxy::ObRpcReq *error_sub_req)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(error_sub_req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("error_sub_req is NULL", K(ret), K_(rpc_trace_id));
  } else {
    proxy::ObRpcReq *root_req = get_root_rpc_req();
    if (OB_ISNULL(root_req)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("root_req is NULL", K(ret), K_(rpc_trace_id));
    } else {
      // generate error resp according to request pcode
      if (OB_FAIL(root_req->alloc_rpc_response())) {
        LOG_WDIAG("fail to alloc rpc response for error", K(ret), K_(rpc_trace_id));
      } else {
        obkv::ObRpcResponse *root_resp = root_req->get_rpc_response();
        if (OB_ISNULL(root_resp)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("fail to get rpc response", K(ret), K_(rpc_trace_id));
        } else {
          // set error code
          int32_t error_code = error_sub_req->get_rpc_req_error_code();
          root_resp->get_result_code().rcode_ = error_code;
          root_req->set_rpc_req_error_code(error_code);

          // set resp meta
          root_resp->set_packet_meta(root_req->get_rpc_request()->get_packet_meta());
          root_resp->get_packet_meta().rpc_header_.flags_ |= ObRpcPacketHeader::RESP_FLAG;
          root_resp->get_packet_meta().ez_header_.ez_payload_size_ =
            (uint32_t)(root_resp->get_encode_size() - 16); // EZ_HEADER_LEN

          root_req->get_obkv_info().set_resp(true);
          result_ = root_req;

          LOG_INFO("generated error response",
                   "error_code", error_sub_req->get_rpc_req_error_code(),
                   K_(request_pcode), K_(rpc_trace_id));
        }
      }
    }
  }

  return ret;
}

void ObProxyRpcReqSplitCont::cleanup()
{
  if (is_canceled_) {
    LOG_WDIAG("split cont is canceled, skip cleanup", K_(rpc_trace_id));
  } else {
    rpc_trace_id_.reset();
    cb_cont_ = NULL;
    mutex_ = NULL;
    root_rpc_req_ = NULL;
    result_ = NULL;
    is_canceled_ = true;
  }
}

void ObProxyRpcReqSplitCont::detach()
{
  cb_cont_ = NULL;
  root_rpc_req_ = NULL;
  detached_ = true;

  for (RESP_MAP::iterator it = resp_map_.begin(); it != resp_map_.end(); ++it) {
    if (OB_NOT_NULL(it->second)) {
      it->second->~ObRpcResponse();
      it->second = NULL;
    }
  }
  resp_map_.destroy();

  for (ALLOCATOR_MAP::iterator it = allocator_map_.begin(); it != allocator_map_.end(); ++it) {
    if (OB_NOT_NULL(it->second)) {
      it->second->reset();
      optimizer::get_global_optimizer_rpc_req_processor().free_allocator(it->second);
      it->second = NULL;
    }
  }
  allocator_map_.destroy();

  for (int64_t i = 0; i < fallback_allocator_array_.count(); ++i) {
    uintptr_t allocator_ptr = fallback_allocator_array_.at(i);
    if (allocator_ptr != 0) {
      common::ObIAllocator *allocator = reinterpret_cast<common::ObIAllocator *>(allocator_ptr);
      allocator->reset();
      optimizer::get_global_optimizer_rpc_req_processor().free_allocator(allocator);
      fallback_allocator_array_.at(i) = 0;
    }
  }
  fallback_allocator_array_.destroy();
  parallel_param_.destroy();

  if (0 >= pending_cb_count_) {
    if (OB_NOT_NULL(execute_thread_)) {
      event::ObAction *action = execute_thread_->schedule_imm(this, ObRpcReq::SPLIT_CONT_SELF_TRY_FREE);
      if (OB_ISNULL(action)) {
        LOG_WDIAG("fail to schedule self try free, fallback to sync try_free_self", K_(rpc_trace_id));
        try_free_self();
      }
    } else {
      LOG_WDIAG("execute_thread is NULL, fallback to sync try_free_self", K_(rpc_trace_id));
      try_free_self();
    }
  }
}

int ObProxyRpcReqSplitCont::try_free_self()
{
  int ret = OB_SUCCESS;
  if (0 >= pending_cb_count_ && !is_canceled_) {
    cleanup();
    if (OB_NOT_NULL(allocator_)) {
      allocator_->reset();
      optimizer::get_global_optimizer_rpc_req_processor().free_allocator(allocator_);
      allocator_ = NULL;
    }
    op_fixed_mem_free(this, sizeof(ObProxyRpcReqSplitCont));
  }
  return ret;
}

void ObProxyRpcReqSplitCont::cancel_recursively()
{
  LOG_DEBUG("split cont cancel_recursively (dispatch only)", K_(rpc_trace_id));
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(root_rpc_req_)) {
    root_rpc_req_->set_split_cont_canceled(true);
  }
  if (OB_FAIL(dispatch_cancel_to_child_request_only())) {
    LOG_WDIAG("fail to dispatch cancel to child request", K(ret), K_(rpc_trace_id));
  }
  if (OB_NOT_NULL(root_rpc_req_)) {
    root_rpc_req_->set_snet_state(proxy::ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
  }
  LOG_DEBUG("finish cancel_recursively", K_(rpc_trace_id));
}

int ObProxyRpcReqSplitCont::dispatch_cancel_to_child_request_only()
{
  int ret = OB_SUCCESS;
  ObRpcReq *sub_req = NULL;
  for (int64_t i = 0; i < parallel_param_.count(); ++i) {
    if (!parallel_param_.at(i).need_cancel_) {
      parallel_param_.at(i).request_ = NULL;
      LOG_DEBUG("sub request need_cancel is false, skip", K(i), K_(rpc_trace_id));
    } else {
      if (OB_ISNULL(parallel_param_.at(i).request_)) {
        parallel_param_.at(i).need_cancel_ = false;
        LOG_WDIAG("sub request is NULL, skip cancellation", K(i), K_(rpc_trace_id));
      } else {
        LOG_DEBUG("need to cancel sub request", K(i), K_(rpc_trace_id));
        sub_req = parallel_param_.at(i).request_;
        sub_req->cancel_request();
        ObRpcRequestSM *sub_sm = sub_req->get_request_sm();
        event::ObEThread *execute_thread = OB_NOT_NULL(sub_sm) ? sub_sm->get_execute_thread() : NULL;
        if (OB_ISNULL(sub_sm) || OB_ISNULL(execute_thread)) {
          parallel_param_.at(i).need_cancel_ = false;
          parallel_param_.at(i).request_ = NULL;
          LOG_WDIAG("sub sm is NULL, skip cancellation", K(i), K_(rpc_trace_id));
        } else {
          LOG_DEBUG("begin cancel recursively for sub sm", K(i), K_(rpc_trace_id));
          event::ObProxyMutex *cleanup_mutex = sub_sm->lock_for_inner_request();
          if (OB_NOT_NULL(cleanup_mutex)) {
            MUTEX_TRY_LOCK(lock, cleanup_mutex, this_ethread());
            if (lock.is_locked()) {
              if (OB_NOT_NULL(sub_sm->get_child_callback_action())) {
                dec_pending_cb();
              }
              if (OB_UNLIKELY(OB_SUCCESS != sub_sm->cancel_child_callback_action())) {
                LOG_WDIAG("fail to cancel child callback action", K(i), K_(rpc_trace_id));
              }
            } else {
              LOG_WDIAG("fail to lock cleanup_mutex, need to retry", K(i), K_(rpc_trace_id));
            }
          }
          // proceed with async cancel dispatch regardless of cancel_child_callback_action result
          if (this_thread() == sub_sm->get_execute_thread()) {
            sub_sm->state_cancel_from_split_cont();
            parallel_param_.at(i).need_cancel_ = false;
          } else {
            if (OB_ISNULL(sub_sm->get_execute_thread()->schedule_imm(sub_sm, RPC_REQUEST_SM_CANCEL_FROM_SPLIT_CONT))) {
              LOG_WDIAG("fail to schedule cancel to execute_thread", K_(rpc_trace_id));
            } else {
              parallel_param_.at(i).need_cancel_ = false;
              LOG_DEBUG("schedule cancel to execute_thread success, set need_cancel to false and request to NULL", K(i), K_(rpc_trace_id));
            }
          }
          parallel_param_.at(i).request_ = NULL;
        }
      }
      sub_req = NULL;
    }
  } // end for
  return ret;
}

int ObProxyRpcReqSplitCont::main_handler(int event, void *data)
{
  LOG_DEBUG("[ObProxyRpcReqSplitCont::main_handler] receive event", K(event), KP(data), K_(rpc_trace_id));
  int ret = OB_SUCCESS;
  bool is_final = false;
  bool is_inner_request_execute_error = false;
  proxy::ObRpcReq *sub_rpc_req = NULL;

  if (detached_) {
    if (event == ObRpcReq::SPLIT_CONT_SELF_TRY_FREE) {
      try_free_self();
    } else {
      //make sure only sub request can callback here
      dec_pending_cb();
      if (0 >= pending_cb_count_) {
        try_free_self();
      }
    }
  } else {
    dec_pending_cb();
    // process callback event
    if (OB_ISNULL(data) || OB_ISNULL(reinterpret_cast<ObEvent*>(data)->cookie_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("data is NULL or sub_rpc_req is NULL", KP(data), K(ret), K_(rpc_trace_id));
    } else if (OB_FAIL(count_inner_callback_nums(is_final))) {
      LOG_WDIAG("fail to check sub requests completion", K_(rpc_trace_id));
    } else if (OB_NOT_NULL(root_rpc_req_) && root_rpc_req_->is_split_cont_canceled()) {
      LOG_DEBUG("root request is canceled, skip handle event", K_(rpc_trace_id));
    } else {
      ObEvent *event_data = static_cast<ObEvent*>(data);
      sub_rpc_req = reinterpret_cast<proxy::ObRpcReq*>(event_data->cookie_);
      LOG_DEBUG("successfully get sub_rpc_req from data", KPC(sub_rpc_req), K_(rpc_trace_id));
      int handle_ret = OB_SUCCESS;
      if (OB_UNLIKELY(is_canceled_)) {
        LOG_DEBUG("split cont is canceled, skip handle event", K_(rpc_trace_id));
      } else if (received_error_event_) {
        LOG_DEBUG("received error event before, skip result processing", K_(rpc_trace_id));
      } else {
        switch (event) {
          case ObRpcReq::INNER_REQUEST_ERROR: // sub sm occur error
          {
            received_error_event_ = true;
            if (OB_UNLIKELY(OB_SUCCESS != (handle_ret = generate_error_resp(sub_rpc_req)))) {
              LOG_WDIAG("fail to generate error resp", K(handle_ret), K_(rpc_trace_id));
            }
            break;
          }
          case ObRpcReq::INNER_REQUEST_DONE:
          {
            // maybe sub sm receive error resp from server
            if (OB_UNLIKELY(sub_rpc_req->has_error_resp())) {
              is_inner_request_execute_error = true;
              LOG_DEBUG("sub request receive error response from server, generate error resp", K_(rpc_trace_id));
              received_error_event_ = true;
              if (OB_UNLIKELY(OB_SUCCESS != (handle_ret = generate_error_resp(sub_rpc_req)))) {
                LOG_WDIAG("fail to generate error resp", K(handle_ret), K_(rpc_trace_id));
              }
            } else {
              // handle normal sub req result
              if (OB_UNLIKELY(OB_SUCCESS != (handle_ret = handle_result(sub_rpc_req, is_final)))) {
                LOG_WDIAG("fail to handle sub request result", K(handle_ret), K_(rpc_trace_id));
              }
            }
            break;
          }
          default:
            LOG_WDIAG("unknown event in main_handler", K(event), K_(rpc_trace_id));
            handle_ret = OB_ERR_UNEXPECTED;
            break;
        }
      }

      // cleanup this sub request
      int cleanup_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != (cleanup_ret = cleanup_completed_sub_request(sub_rpc_req, proxy::ObRpcReq::ClientNetState::RPC_REQ_CLIENT_INNER_REQUEST_DONE, true)))) {
        LOG_WDIAG("fail to cleanup completed sub request", K(cleanup_ret), K_(rpc_trace_id));
      }

      if (OB_UNLIKELY(OB_SUCCESS != handle_ret)) {
        ret = handle_ret;
      } else if (OB_UNLIKELY(OB_SUCCESS != cleanup_ret)) {
        ret = cleanup_ret;
      }
    }

    /*
      * 1. If any unexpected error occurs (ret is an error value), or If an error response is received from the server,
      *    immediately cancel the remaining sub sm and transports the error code to the root sm,
      *    and the root sm will report the error code to the client.
      * 2. If all sub reqs are processed successfully, aggregate the results and callback the complete event,
      *    and the root sm will return a normal result to the client.
      */
    if (OB_FAIL(ret) || received_error_event_) {
      if (OB_NOT_NULL(cb_cont_)) {
        LOG_WDIAG("split cont occur or receive error, start to cancel recursively and callback error event", K_(received_error_event), K(ret), K_(rpc_trace_id));
        cancel_recursively();
        if (is_inner_request_execute_error){
          root_rpc_req_->set_snet_state(proxy::ObRpcReq::ServerNetState::RPC_REQ_SERVER_SHARDING_REQUEST_DONE);
          cb_cont_->handle_event(VC_EVENT_READ_COMPLETE, result_);
        } else {
          cb_cont_->handle_event(VC_EVENT_ERROR);
        }
        // the error code has been set to the root rpc req in generate_error_resp method
        cb_cont_ = NULL;
      }
    } else {
      if (is_final) {
        LOG_DEBUG("handle next event VC_EVENT_READ_COMPLETE, data", K(result_), K_(rpc_trace_id));
        if (OB_NOT_NULL(cb_cont_)) {
          root_rpc_req_->set_snet_state(proxy::ObRpcReq::ServerNetState::RPC_REQ_SERVER_SHARDING_REQUEST_DONE);
          cb_cont_->handle_event(VC_EVENT_READ_COMPLETE, result_);
          cb_cont_ = NULL;
        }
      }
    }
  }

  return VC_EVENT_CONT;
}

int ObProxyRpcReqSplitCont::count_inner_callback_nums(bool &is_final)
{
  int ret = OB_SUCCESS;

  completed_sub_req_count_++;
  if (OB_UNLIKELY(completed_sub_req_count_ < 0) || OB_UNLIKELY(completed_sub_req_count_ > parallel_param_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected completed sub requests", K(completed_sub_req_count_), K(parallel_param_.count()), K(ret), K_(rpc_trace_id));
  } else if (completed_sub_req_count_ == parallel_param_.count()) {
    LOG_DEBUG("all sub requests are done", K_(rpc_trace_id));
    is_final = true;
  } else {
    is_final = false;
  }

  LOG_DEBUG("count_inner_callback_nums", K(completed_sub_req_count_), K(parallel_param_.count()), K(is_final), K_(rpc_trace_id));
  return ret;
}

int ObProxyRpcReqSplitCont::cleanup_completed_sub_request(proxy::ObRpcReq *sub_rpc_req,
                                                         proxy::ObRpcReq::ClientNetState cleanup_state,
                                                         bool need_clean_sub_req_allocator)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sub_rpc_req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("sub_rpc_req is NULL", K(ret), K_(rpc_trace_id));
  } else {
    if (need_clean_sub_req_allocator) {
      if (OB_FAIL(allocator_map_.set_refactored(sub_rpc_req->get_cont_index(), sub_rpc_req->get_inner_request_allocator()))) {
        LOG_WDIAG("fail to set allocator_map", K(ret), K_(rpc_trace_id));
        //可能内存不足，此时不能直接释放，会影响sub_resp，需要先保存到fallback_allocator_array_中
        if (OB_NOT_NULL(sub_rpc_req->get_inner_request_allocator())) {
          int ret = OB_SUCCESS;
            if (OB_FAIL(fallback_allocator_array_.push_back(reinterpret_cast<uintptr_t>(sub_rpc_req->get_inner_request_allocator())))) {
            LOG_EDIAG("fail to push back fallback_allocator_array, will leak allocator to avoid crash", K(ret), K_(rpc_trace_id));
          }
        }
      }
    }
    ret = OB_SUCCESS;
    // set need_cancel
    int64_t sub_index = sub_rpc_req->get_cont_index();
    if (sub_index >= 0 && sub_index < parallel_param_.count()) {
      parallel_param_.at(sub_index).need_cancel_ = false;
      LOG_DEBUG("sub request completed, set need_cancel to false", K(sub_index), K_(rpc_trace_id));
    }

    // async cleanup current sub sm (no matter success or fail)
    LOG_DEBUG("start to cleanup sub request", K(sub_index), K_(rpc_trace_id));
    proxy::ObRpcReq::ObRpcReqCleanupParams cleanup_params(cleanup_state);
    sub_rpc_req->cleanup(cleanup_params);
    parallel_param_.at(sub_index).request_ = NULL;
  }

  return ret;
}

} // end of namespace engine
} // end of namespace obproxy
} // end of namespace oceanbase