/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "cmd/ob_show_sm_handler.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
//SMColumnID
enum
{
  OB_SMC_SM_ID = 0,
  OB_SMC_SM_SSID,
  OB_SMC_SM_SERVER_SESSID,
  OB_SMC_SM_SERVER_HOST,
  OB_SMC_SM_CSID,
  OB_SMC_SM_PROXY_SESSID,
  OB_SMC_SM_CLUSTER_NAME,
  OB_SMC_SM_INFO,
  OB_SMC_MAX_SM_COLUMN_ID,
};

const ObProxyColumnSchema SM_COLUMN_ARRAY[OB_SMC_MAX_SM_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_SMC_SM_ID,            "sm_id",          obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SMC_SM_SSID,          "ss_id",          obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SMC_SM_SERVER_SESSID, "server_sessid",  obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SMC_SM_SERVER_HOST,   "server_host",    obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SMC_SM_CSID,          "cs_id",          obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SMC_SM_PROXY_SESSID,  "proxy_sessid",   obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SMC_SM_CLUSTER_NAME,  "cluster_name",   obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SMC_SM_INFO,          "sm_info",        obmysql::OB_MYSQL_TYPE_VARCHAR),
};

//RpcSMColumnID
enum
{
  OB_RSMC_SM_ID = 0,
  OB_RSMC_SM_TRACE_ID,
  OB_RSMC_SM_CLIENT_STATE,
  OB_RSMC_SM_SERVER_STATE,
  OB_RSMC_SM_STATE,
  OB_RSMC_SM_CSID,
  OB_RSMC_SM_INFO,
  OB_RSMC_MAX_SM_COLUMN_ID,
};

const ObProxyColumnSchema RPC_SM_COLUMN_ARRAY[OB_RSMC_MAX_SM_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_RSMC_SM_ID,            "sm_id",          obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RSMC_SM_TRACE_ID,     "trace_id",       obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RSMC_SM_CLIENT_STATE, "client_state",  obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RSMC_SM_SERVER_STATE, "server_state",    obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RSMC_SM_STATE,        "sm_state",       obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RSMC_SM_CSID,          "cs_id",          obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RSMC_SM_INFO,          "sm_info",        obmysql::OB_MYSQL_TYPE_VARCHAR),
};

ObShowSMHandler::ObShowSMHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
    : ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type()), is_hash_set_inited_(false), list_bucket_(0),
      sm_id_(info.get_sm_id())
{
  // if (sm_id_ >= 0) {
  //   SET_HANDLER(&ObShowSMHandler::handle_smdetails);
  // } else {
  //   SET_HANDLER(&ObShowSMHandler::handle_smlist);
  // }
}

int ObShowSMHandler::init_hash_set()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_hash_set_inited_)) {
    ret = OB_INIT_TWICE;
    WDIAG_ICMD("fail to init got_id_set twice", K(ret));
  } else if (OB_FAIL(got_id_set_.create(BUCKET_SIZE))) {
    WDIAG_ICMD("fail to init got_id_set", K(ret));
  } else {
    is_hash_set_inited_ = true;
  }
  return ret;
}

int ObShowSMHandler::handle_smdetails(int event, void *data)
{
  int event_ret = EVENT_NONE;
  int ret = OB_SUCCESS;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("it should not happened, this_ethread is null", K(ret));
  } else if (OB_FAIL(dump_header())) {
    WDIAG_ICMD("fail to dump_header", K(ret));
  } else {
    bool need_encode_eof = false;
    if (sm_id_ >= UINT32_MAX || sm_id_ < 0) {
      need_encode_eof = true;
      INFO_ICMD("unknown sm_id_", K(sm_id_));
    } else {
      const int64_t bucket = (sm_id_ % MYSQL_SM_LIST_BUCKETS);
      MUTEX_TRY_LOCK(lock, g_mysqlsm_list[bucket].mutex_, ethread);
      if (!lock.is_locked()) {
        DEBUG_ICMD("fail to try lock list, schedule in again", K(bucket));
        if (OB_ISNULL(g_event_processor.schedule_in(this, MYSQL_LIST_RETRY, ET_TASK))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          EDIAG_ICMD("fail to schedule self", K(ret));
        } else {
          event_ret = EVENT_CONT;
        }
      } else {
        DEBUG_ICMD("succ to try lock list, traverse the state machines", K(bucket));
        const ObMysqlSM *sm = g_mysqlsm_list[bucket].sm_list_.head_;
        bool terminate = false;
        while (!terminate && OB_SUCC(ret) && NULL != sm) {
          if (static_cast<int64_t>(sm->sm_id_) == sm_id_) {
            // In this block we try to get the lock of the state machine
            MUTEX_TRY_LOCK(sm_lock, sm->mutex_, ethread);
            if (!sm_lock.is_locked()) {
              // We missed the lock so retry
              DEBUG_ICMD("fail to try lock sm, schedule in again", K(sm_id_));
              if (OB_ISNULL(g_event_processor.schedule_in(this, MYSQL_LIST_RETRY, ET_TASK))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                EDIAG_ICMD("fail to schedule self", K(ret));
              } else {
                event_ret = EVENT_CONT;
              }
            } else {
              if (OB_FAIL(dump_sm_internal(*sm))) {
                WDIAG_ICMD("fail to dump sm", K(sm_id_));
              } else {
                DEBUG_ICMD("succ to dump sm", K(sm_id_));
                need_encode_eof = true;
              }
            }
            terminate = true;
          } else {
            sm = sm->stat_link_.next_;
          }
        }
        if (OB_LIKELY(NULL == sm)) {
          need_encode_eof = true;
        }
      }
    }

    if (need_encode_eof) {
      if (OB_FAIL(encode_eof_packet())) {
        WDIAG_ICMD("fail to encode eof packet", K(ret));
      } else {
        INFO_ICMD("succ to dump sm detail", K_(sm_id));
        event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
      }
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowSMHandler::handle_rpc_smdetails(int event, void *data)
{
  int event_ret = EVENT_NONE;
  int ret = OB_SUCCESS;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("it should not happened, this_ethread is null", K(ret));
  } else if (OB_FAIL(dump_header())) {
    WDIAG_ICMD("fail to dump_header", K(ret));
  } else {
    bool need_encode_eof = false;
    if (sm_id_ >= UINT32_MAX || sm_id_ < 0) {
      need_encode_eof = true;
      INFO_ICMD("unknown sm_id_", K(sm_id_));
    } else {
      const int64_t bucket = (sm_id_ % RPCREQUEST_SM_LIST_BUCKETS);
      MUTEX_TRY_LOCK(lock, g_rpcrequestsm_list[bucket].mutex_, ethread);
      if (!lock.is_locked()) {
        DEBUG_ICMD("fail to try lock list, schedule in again", K(bucket));
        if (OB_ISNULL(g_event_processor.schedule_in(this, MYSQL_LIST_RETRY, ET_TASK))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          EDIAG_ICMD("fail to schedule self", K(ret));
        } else {
          event_ret = EVENT_CONT;
        }
      } else {
        DEBUG_ICMD("succ to try lock list, traverse the state machines", K(bucket));
        const ObRpcRequestSM *sm = g_rpcrequestsm_list[bucket].sm_list_.head_;
        bool terminate = false;
        while (!terminate && OB_SUCC(ret) && NULL != sm) {
          if (static_cast<int64_t>(sm->get_sm_id()) == sm_id_) {
            // In this block we try to get the lock of the state machine
            MUTEX_TRY_LOCK(sm_lock, sm->mutex_, ethread);
            if (!sm_lock.is_locked()) {
              // We missed the lock so retry
              DEBUG_ICMD("fail to try lock sm, schedule in again", K(sm_id_));
              if (OB_ISNULL(g_event_processor.schedule_in(this, MYSQL_LIST_RETRY, ET_TASK))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                EDIAG_ICMD("fail to schedule self", K(ret));
              } else {
                event_ret = EVENT_CONT;
              }
            } else {
              if (OB_FAIL(dump_rpc_sm_internal(*sm))) {
                WDIAG_ICMD("fail to dump sm", K(sm_id_));
              } else {
                DEBUG_ICMD("succ to dump sm", K(sm_id_));
                need_encode_eof = true;
              }
            }
            terminate = true;
          } else {
            sm = sm->stat_link_.next_;
          }
        }
        if (OB_LIKELY(NULL == sm)) {
          need_encode_eof = true;
        }
      }
    }

    if (need_encode_eof) {
      if (OB_FAIL(encode_eof_packet())) {
        WDIAG_ICMD("fail to encode eof packet", K(ret));
      } else {
        INFO_ICMD("succ to dump sm detail", K_(sm_id));
        event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
      }
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowSMHandler::handle_smlist(int event, void *data)
{
  int event_ret = EVENT_NONE;
  int ret = OB_SUCCESS;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (!is_hash_set_inited_) {
    ret = OB_NOT_INIT;
    WDIAG_ICMD("hash set is not inited_", K_(is_hash_set_inited), K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("it should not happened, this_ethread is null", K(ret));
  } else if (OB_FAIL(dump_header())) {
    WDIAG_ICMD("fail to dump_header", K(ret));
  } else {
    DEBUG_ICMD("begin traversing the buckets", K_(list_bucket), K(MYSQL_SM_LIST_BUCKETS));
    bool terminate = false;
    const ObMysqlSM *sm = NULL;
    for (; !terminate && OB_SUCC(ret) && (list_bucket_ < MYSQL_SM_LIST_BUCKETS); ++list_bucket_) {
      MUTEX_TRY_LOCK(lock, g_mysqlsm_list[list_bucket_].mutex_, ethread);
      if (!lock.is_locked()) {
        DEBUG_ICMD("fail to try lock list, schedule in again", K(list_bucket_));
        if (OB_ISNULL(g_event_processor.schedule_in(this, MYSQL_LIST_RETRY, ET_TASK))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          EDIAG_ICMD("fail to schedule self", K(ret));
        } else {
          event_ret = EVENT_CONT;
        }
        terminate = true;
      } else {
        sm = g_mysqlsm_list[list_bucket_].sm_list_.head_;
        DEBUG_ICMD("succ to try lock list, begin to traverse the state machines",
                   K(list_bucket_), K(sm));
        while (NULL != sm && !terminate && OB_SUCC(ret)) {
          // In this block we try to get the lock of the state machine
          MUTEX_TRY_LOCK(sm_lock, sm->mutex_, ethread);
          int hash_ret = got_id_set_.exist_refactored(sm->sm_id_);
          if (sm_lock.is_locked()) {
            if (OB_HASH_EXIST == hash_ret) {
              //do nothing
            } else if (OB_HASH_NOT_EXIST != hash_ret) {
              ret = hash_ret;
              WDIAG_ICMD("fail to check if sm has been dumped", K(sm->sm_id_), K(ret));
            } else if (OB_FAIL(dump_sm_internal(*sm))) {
              WDIAG_ICMD("fail to dump sm", K(sm->sm_id_));
            } else {
              if (OB_FAIL(got_id_set_.set_refactored(sm->sm_id_))) {
                WDIAG_ICMD("fail to set sm into got_id_set_", K(sm->sm_id_), K(ret));
              } else {
                DEBUG_ICMD("succ to dump sm", K(sm->sm_id_));
              }
            }
            terminate = !OB_SUCC(ret);
          } else {
            DEBUG_ICMD("fail to try lock sm, skip it", K(sm->sm_id_));
          }
          sm = sm->stat_link_.next_;
        }
        DEBUG_ICMD("finish traversing the list_bucket", K_(list_bucket));
      }
    }

    if (!terminate && list_bucket_ >= MYSQL_SM_LIST_BUCKETS) {
      DEBUG_ICMD("finish traversing all the state machine");
      if (OB_FAIL(encode_eof_packet())) {
        WDIAG_ICMD("fail to encode eof packet", K(ret));
      } else {
        INFO_ICMD("succ to dump sm list");
        event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
      }
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowSMHandler::handle_rpc_smlist(int event, void *data)
{
  int event_ret = EVENT_NONE;
  int ret = OB_SUCCESS;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (!is_hash_set_inited_) {
    ret = OB_NOT_INIT;
    WDIAG_ICMD("hash set is not inited_", K_(is_hash_set_inited), K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("it should not happened, this_ethread is null", K(ret));
  } else if (OB_FAIL(dump_header())) {
    WDIAG_ICMD("fail to dump_header", K(ret));
  } else {
    DEBUG_ICMD("begin traversing the buckets", K_(list_bucket), K(RPCREQUEST_SM_LIST_BUCKETS));
    bool terminate = false;
    const ObRpcRequestSM *sm = NULL;
    for (; !terminate && OB_SUCC(ret) && (list_bucket_ < RPCREQUEST_SM_LIST_BUCKETS); ++list_bucket_) {
      MUTEX_TRY_LOCK(lock, g_rpcrequestsm_list[list_bucket_].mutex_, ethread);
      if (!lock.is_locked()) {
        DEBUG_ICMD("fail to try lock list, schedule in again", K(list_bucket_));
        if (OB_ISNULL(g_event_processor.schedule_in(this, MYSQL_LIST_RETRY, ET_TASK))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          EDIAG_ICMD("fail to schedule self", K(ret));
        } else {
          event_ret = EVENT_CONT;
        }
        terminate = true;
      } else {
        sm = g_rpcrequestsm_list[list_bucket_].sm_list_.head_;
        DEBUG_ICMD("succ to try lock list, begin to traverse the state machines",
                   K(list_bucket_), K(sm));
        while (NULL != sm && !terminate && OB_SUCC(ret)) {
          // In this block we try to get the lock of the state machine
          MUTEX_TRY_LOCK(sm_lock, sm->mutex_, ethread);
          int hash_ret = got_id_set_.exist_refactored(sm->get_sm_id());
          if (sm_lock.is_locked()) {
            if (OB_HASH_EXIST == hash_ret) {
              //do nothing
            } else if (OB_HASH_NOT_EXIST != hash_ret) {
              ret = hash_ret;
              WDIAG_ICMD("fail to check if sm has been dumped", K(sm->get_sm_id()), K(ret));
            } else if (OB_FAIL(dump_rpc_sm_internal(*sm))) {
              WDIAG_ICMD("fail to dump sm", K(sm->get_sm_id()));
            } else {
              if (OB_FAIL(got_id_set_.set_refactored(sm->get_sm_id()))) {
                WDIAG_ICMD("fail to set sm into got_id_set_", K(sm->get_sm_id()), K(ret));
              } else {
                DEBUG_ICMD("succ to dump sm", K(sm->get_sm_id()));
              }
            }
            terminate = !OB_SUCC(ret);
          } else {
            DEBUG_ICMD("fail to try lock sm, skip it", K(sm->get_sm_id()));
          }
          sm = sm->stat_link_.next_;
        }
        DEBUG_ICMD("finish traversing the list_bucket", K_(list_bucket));
      }
    }

    if (!terminate && list_bucket_ >= RPCREQUEST_SM_LIST_BUCKETS) {
      DEBUG_ICMD("finish traversing all the state machine");
      if (OB_FAIL(encode_eof_packet())) {
        WDIAG_ICMD("fail to encode eof packet", K(ret));
      } else {
        INFO_ICMD("succ to dump sm list");
        event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
      }
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowSMHandler::dump_smlist()
{
  int ret = OB_SUCCESS;
  ObEThread *ethread = NULL;
  if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("it should not happened, this_ethread is null", K(ret));
  } else if (OB_FAIL(dump_header())) {
    WDIAG_ICMD("fail to dump_header", K(ret));
  } else {
    DEBUG_ICMD("begin traversing the buckets", K_(list_bucket), K(MYSQL_SM_LIST_BUCKETS));
    ObMysqlSM *sm = NULL;
    for (; OB_SUCC(ret) && (list_bucket_ < MYSQL_SM_LIST_BUCKETS); ++list_bucket_) {
      MUTEX_LOCK(lock, g_mysqlsm_list[list_bucket_].mutex_, ethread);
      sm = g_mysqlsm_list[list_bucket_].sm_list_.head_;
      DEBUG_ICMD("succ to try lock list, begin to traverse the state machines", K(list_bucket_), K(sm));
      while (NULL != sm && OB_SUCC(ret)) {
        // In this block we try to get the lock of the state machine
        MUTEX_LOCK(sm_lock, sm->mutex_, ethread);
        if (OB_FAIL(dump_sm_internal(*sm))) {
          WDIAG_ICMD("fail to dump sm", K(sm->sm_id_), K(ret));
        } else {
          DEBUG_ICMD("succ to dump sm", K(sm->sm_id_));
        }
        sm = sm->stat_link_.next_;
      }
      DEBUG_ICMD("finish traversing the list_bucket", K_(list_bucket));
    }
  }

  if (OB_SUCC(ret)) {
    DEBUG_ICMD("finish traversing all the state machine");
    if (OB_FAIL(encode_eof_packet())) {
      WDIAG_ICMD("fail to encode eof packet", K(ret));
    }
  }
  return ret;
}

int ObShowSMHandler::dump_common_info(const ObMysqlSM &sm, ObSqlString &sm_info)
{
  int ret = OB_SUCCESS;
  const char *sm_state = ObMysqlTransact::get_action_name(sm.trans_state_.next_action_);
  if (OB_FAIL(sm_info.append_fmt("CURRENT_STATE:%s\n", sm_state))) {
    WDIAG_ICMD("fail to append common info", K(sm_state), K(ret));
  }
  return ret;
}

int ObShowSMHandler::dump_tunnel_info(const ObMysqlSM &sm, ObSqlString &sm_info)
{
  int ret = OB_SUCCESS;
  const ObMysqlTunnel &t = sm.get_tunnel();
  if (OB_FAIL(sm_info.append_fmt("PRODUCERS::\n"))) {
    WDIAG_ICMD("fail to append producer info", K(sm_info), K(ret));
  } else {
    bool read_vio_avail = false;
    const ObMysqlTunnelProducer *producer = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < ObMysqlTunnel::MAX_PRODUCERS; ++i) {
      if (OB_FAIL(sm_info.append_fmt("  [%ld]=={", i))) {
        WDIAG_ICMD("fail to append index info", K(sm_info), K(ret));
      } else {
        producer = &(t.producers_[i]);
        if (NULL != (producer->vc_)) {
          read_vio_avail = (true == producer->alive_ && NULL != producer->read_vio_);
          if (OB_FAIL(sm_info.append_fmt("name:%s; is_alive:%s; ndone:%ld; nbytes:%ld;}\n",
                                         get_producer_type_name(producer->type_), (producer->alive_ ? "true" : "false"),
                                         (read_vio_avail ? producer->read_vio_->ndone_ : producer->bytes_read_),
                                         (read_vio_avail ? producer->read_vio_->nbytes_ : -1)))) {
            WDIAG_ICMD("fail to append producer info", K(i), K(sm_info), K(ret));
          }
        } else if (OB_FAIL(sm_info.append_fmt("NULL}\n"))) {
          WDIAG_ICMD("fail to append producer info", K(i), K(sm_info), K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sm_info.append_fmt("CONSUMER:\n"))) {
      WDIAG_ICMD("fail to append Cosumers info", K(sm_info), K(ret));
    } else {
      bool write_vio_avail = false;
      const ObMysqlTunnelConsumer *consumer = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < ObMysqlTunnel::MAX_CONSUMERS; ++i) {
        if (OB_FAIL(sm_info.append_fmt("  [%ld]=={", i))) {
          WDIAG_ICMD("fail to append consumer info", K(i), K(sm_info), K(ret));
        } else {
          consumer = &(t.consumers_[i]);
          write_vio_avail = (true == consumer->alive_ && NULL != consumer->write_vio_);
          if (NULL != (consumer->vc_)) {
            if (OB_FAIL(sm_info.append_fmt("name:%s; is_alive:%s; ndone:%ld; nbytes:%ld;}\n",
                                           get_consumer_type_name(consumer->type_), (consumer->alive_ ? "true" : "false"),
                                           (write_vio_avail ? consumer->write_vio_->ndone_ : consumer->bytes_written_),
                                           (write_vio_avail ? consumer->write_vio_->nbytes_ : -1)))) {
              WDIAG_ICMD("fail to append consumer info", K(i), K(sm_info), K(ret));
            }
          } else if (OB_FAIL(sm_info.append_fmt("NULL}\n"))) {
            WDIAG_ICMD("fail to append consumer info", K(i), K(sm_info), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObShowSMHandler::dump_history_info(const ObMysqlSM &sm, ObSqlString &sm_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sm_info.append_fmt("HISTORY:\n"))) {
    WDIAG_ICMD("fail to append History info", K(sm_info), K(ret));
  } else {
    int64_t size = sm.history_pos_;
    int64_t start = 0;
    if (size >= ObMysqlSM::HISTORY_SIZE) {
      size = ObMysqlSM::HISTORY_SIZE;
      start = sm.history_pos_ % ObMysqlSM::HISTORY_SIZE;
    }
    const ObMysqlSM::ObHistory *history = NULL;
    for (int64_t i = 0, j = start; OB_SUCC(ret) && i < size; ++i) {
      if (OB_FAIL(sm_info.append_fmt("  [%ld]=={", i))) {
        WDIAG_ICMD("fail to append history info", K(i),K(sm_info),  K(ret));
      } else {
        history = &(sm.history_[j]);
        if (OB_FAIL(sm_info.append_fmt("fileline:%s; event:%u; reentrancy:%d;}\n",
                                       history->file_line_, history->event_, history->reentrancy_))) {
          WDIAG_ICMD("fail to append history info", K(i), K(sm_info), K(ret));
        }
      }
      j = (j + 1) % ObMysqlSM::HISTORY_SIZE;
    }
  }
  return ret;
}

int ObShowSMHandler::dump_rpc_history_info(const ObRpcRequestSM &sm, ObSqlString &sm_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sm_info.append_fmt("HISTORY:\n"))) {
    WDIAG_ICMD("fail to append History info", K(sm_info), K(ret));
  } else {
    int64_t size = sm.get_history_pos();
    int64_t start = 0;
    if (size >= ObRpcRequestSM::HISTORY_SIZE) {
      size = ObRpcRequestSM::HISTORY_SIZE;
      start = sm.get_history_pos() % ObRpcRequestSM::HISTORY_SIZE;
    }
    const ObRpcRequestSM::ObHistory *history = NULL;
    for (int64_t i = 0, j = start; OB_SUCC(ret) && i < size; ++i) {
      if (OB_FAIL(sm_info.append_fmt("  [%ld]=={", i))) {
        WDIAG_ICMD("fail to append history info", K(i),K(sm_info),  K(ret));
      } else {
        history = &(sm.history_[j]);
        if (OB_FAIL(sm_info.append_fmt("fileline:%s; event:%u; reentrancy:%d;}",
                                       history->file_line_, history->event_, history->reentrancy_))) {
          WDIAG_ICMD("fail to append history info", K(i), K(sm_info), K(ret));
        }
      }
      j = (j + 1) % ObRpcRequestSM::HISTORY_SIZE;
    }
  }
  return ret;
}

int ObShowSMHandler::dump_sm_internal(const ObMysqlSM &sm)
{
  int ret = OB_SUCCESS;
  ObSqlString sm_info;
  int64_t ss_id = 0;
  uint32_t server_sessid = 0;
  char host_ip_buf[INET6_ADDRSTRLEN];
  host_ip_buf[0] = '\0';
  uint32_t cs_id = 0;
  uint64_t proxy_sessid = 0;
  ObString cluster_name;
  if (NULL != sm.client_session_) {
    cs_id = sm.client_session_->get_cs_id();
    proxy_sessid = sm.client_session_->get_proxy_sessid();
    cluster_name = sm.client_session_->get_session_info().get_priv_info().cluster_name_;
  }
  if (NULL != sm.server_session_) {
    ss_id = sm.server_session_->ss_id_;
    server_sessid = sm.server_session_->get_server_sessid();
    sm.server_session_->server_ip_.to_string(host_ip_buf, sizeof(host_ip_buf));
  }

  ObNewRow row;
  ObObj cells[OB_SMC_MAX_SM_COLUMN_ID];
  cells[OB_SMC_SM_ID].set_int(sm.sm_id_);
  cells[OB_SMC_SM_SSID].set_int(ss_id);
  cells[OB_SMC_SM_SERVER_SESSID].set_uint32(server_sessid);
  cells[OB_SMC_SM_SERVER_HOST].set_varchar(host_ip_buf);
  cells[OB_SMC_SM_CSID].set_uint32(cs_id);
  cells[OB_SMC_SM_PROXY_SESSID].set_uint64(proxy_sessid);
  cells[OB_SMC_SM_CLUSTER_NAME].set_varchar(cluster_name);

  if (OB_FAIL(dump_common_info(sm, sm_info))) {
    WDIAG_ICMD("fail to dump_common_info", K(sm_info), K(ret));
  } else if (OB_FAIL(dump_tunnel_info(sm, sm_info))) {
    WDIAG_ICMD("fail to dump_tunnel_info", K(sm_info), K(ret));
  } else if (OB_FAIL(dump_history_info(sm, sm_info))) {
    WDIAG_ICMD("fail to dump_history_info", K(sm_info), K(ret));
  } else {
    cells[OB_SMC_SM_INFO].set_varchar(sm_info.string());
    row.cells_ = cells;
    row.count_ = OB_SMC_MAX_SM_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
    }
  }
  return ret;
}

int ObShowSMHandler::dump_rpc_sm_internal(const ObRpcRequestSM &sm)
{
  int ret = OB_SUCCESS;
  ObSqlString sm_state_info;
  ObSqlString cnet_state_info;
  ObSqlString snet_state_info;
  ObSqlString sm_info;

  uint32_t cs_id = 0;
  ObString trace_id;
  if (NULL != sm.get_rpc_req()) {
    cs_id = sm.get_rpc_req()->cs_id_;
    trace_id = sm.get_rpc_req()->get_trace_id().get_rpc_trace_id_buf();
  }
  if (NULL != sm.get_rpc_req()) {
    const char *sm_state = ObRpcReqDebugNames::get_rpc_sm_state_name(sm.get_rpc_req()->get_sm_state());
    const char *cnet_state = ObRpcReqDebugNames::get_client_state_name(sm.get_rpc_req()->get_cnet_state());
    const char *snet_state = ObRpcReqDebugNames::get_server_state_name(sm.get_rpc_req()->get_snet_state());
    if (OB_FAIL(sm_state_info.append_fmt("%s", sm_state))) {
      WDIAG_ICMD("fail to append sm_state", K(sm_state), K(ret));
    } else if (OB_FAIL(cnet_state_info.append_fmt("%s", cnet_state))) {
      WDIAG_ICMD("fail to append cnet_state", K(cnet_state), K(ret));
    } else if (OB_FAIL(snet_state_info.append_fmt("%s", snet_state))) {
      WDIAG_ICMD("fail to append snet_state", K(snet_state), K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(dump_rpc_history_info(sm, sm_info))) {
    WDIAG_ICMD("fail to dump_history_info", K(sm_info), K(ret));
  } else {
    ObNewRow row;
    ObObj cells[OB_RSMC_MAX_SM_COLUMN_ID];
    cells[OB_RSMC_SM_ID].set_int(sm.get_sm_id());
    cells[OB_RSMC_SM_CLIENT_STATE].set_varchar(cnet_state_info.string());
    cells[OB_RSMC_SM_SERVER_STATE].set_varchar(snet_state_info.string());
    cells[OB_RSMC_SM_STATE].set_varchar(sm_state_info.string());
    cells[OB_RSMC_SM_TRACE_ID].set_varchar(trace_id);
    cells[OB_RSMC_SM_CSID].set_uint32(cs_id);
    cells[OB_RSMC_SM_INFO].set_varchar(sm_info.string());
    row.cells_ = cells;
    row.count_ = OB_RSMC_MAX_SM_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
    }
  }
  return ret;
}

int ObShowSMHandler::dump_header()
{
  int ret = OB_SUCCESS;
  if (header_encoded_) {
    DEBUG_ICMD("header is already encoded, skip this");
  } else if (OBPROXY_T_SUB_PROXYSM_RPC == sub_type_ ) {
    if (OB_FAIL(encode_header(RPC_SM_COLUMN_ARRAY, OB_RSMC_MAX_SM_COLUMN_ID))) {
      WDIAG_ICMD("fail to encode header", K(ret));
    }
  } else {
    if (OB_FAIL(encode_header(SM_COLUMN_ARRAY, OB_SMC_MAX_SM_COLUMN_ID))) {
      WDIAG_ICMD("fail to encode header", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    header_encoded_ = true;
  }
  return ret;
}

static int proxysm_stat_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObShowSMHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowSMHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    EDIAG_ICMD("fail to new ObShowSMHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WDIAG_ICMD("fail to init for ObShowSMHandler");
  } else if (handler->need_init_hash_set() && OB_FAIL(handler->init_hash_set())) {
    WDIAG_ICMD("fail to init hash set for ObShowSMHandler");
  } else {
    if (OBPROXY_T_SUB_PROXYSM_RPC == info.get_sub_cmd_type() && 0 <= info.get_sm_id()) {
      SET_CONTINUATION_HANDLER(handler, &ObShowSMHandler::handle_rpc_smdetails);
    } else if (OBPROXY_T_SUB_PROXYSM_RPC == info.get_sub_cmd_type()){
      SET_CONTINUATION_HANDLER(handler, &ObShowSMHandler::handle_rpc_smlist);
    } else if (0 <= info.get_sm_id()) {
      SET_CONTINUATION_HANDLER(handler, &ObShowSMHandler::handle_smdetails);
    } else {
      SET_CONTINUATION_HANDLER(handler, &ObShowSMHandler::handle_smlist);
    }
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      EDIAG_ICMD("fail to schedule ObShowSMHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObShowSMHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_sm_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_SM, &proxysm_stat_callback))) {
    WDIAG_ICMD("fail to register CMD_TYPE_SM", K(ret));
  } else {
    // Create the mutexes for mysql list protection
    for (int64_t i = 0; OB_SUCC(ret) && i < MYSQL_SM_LIST_BUCKETS; ++i) {
      if (OB_ISNULL(g_mysqlsm_list[i].mutex_ = new_proxy_mutex())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        WDIAG_ICMD("fail to new_proxy_mutex for g_mysqlsm_list", K(i), K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < RPCREQUEST_SM_LIST_BUCKETS; ++i){
      if (OB_ISNULL(g_rpcrequestsm_list[i].mutex_ = new_proxy_mutex())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        WDIAG_ICMD("fail to new_proxy_mutex for g_rpcrequestsm_list", K(i), K(ret));
      }
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
