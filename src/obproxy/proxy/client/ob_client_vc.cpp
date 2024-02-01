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

#define USING_LOG_PREFIX PROXY
#include "lib/profile/ob_trace_id.h"
#include "proxy/client/ob_client_vc.h"
#include "proxy/client/ob_mysql_client_pool.h"
#include "proxy/mysqllib/ob_mysql_request_builder.h"
#include "proxy/mysql/ob_mysql_client_session.h"
#include "obutils/ob_resource_pool_processor.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "utils/ob_proxy_table_define.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::packet;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

enum
{
  CLIENT_MAGIC_ALIVE = 0xAABBCCDD,
  CLIENT_MAGIC_DEAD  = 0xDDCCBBAA
};

static int64_t const RESCHEDULE_GET_NETHANDLER_LOCK_INTERVAL = HRTIME_MSECONDS(1); // 1ms

ObClientVC::ObClientVC(ObMysqlClient &client_core)
  : ObNetVConnection(), magic_(CLIENT_MAGIC_ALIVE), disconnect_by_client_(false),
    is_request_sent_(false), core_client_(&client_core),
    pending_action_(NULL), read_state_(), write_state_(), addr_()
{
  SET_HANDLER(&ObClientVC::main_handler);
}

int ObClientVC::main_handler(int event, void *data)
{
  int he_ret = EVENT_CONT;
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObClientVC received event", "event",  ObMysqlClient::get_client_event_name(event),
             K(data), "read_vio", &read_state_.vio_, "write_vio", &write_state_.vio_,
             "thread", this_ethread());
  if (OB_UNLIKELY(CLIENT_MAGIC_ALIVE != magic_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this client_vc is dead", K_(magic), K(this), K(ret));
  } else if (OB_UNLIKELY(this_ethread() != mutex_->thread_holding_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this_ethread must be equal with thread_holding", "this_ethread",
              this_ethread(), "thread_holding", mutex_->thread_holding_, K(this), K(ret));
  } else {
    switch (event) {
      case VC_EVENT_EOS: { // client vc disconnect
        if (NULL != read_state_.vio_.cont_) {
          disconnect_by_client_ = true;
          read_state_.vio_.cont_->handle_event(VC_EVENT_EOS, &read_state_.vio_);
        }
        he_ret = EVENT_DONE;
        break;
      }
      case CLIENT_MYSQL_RESP_TRANSFER_COMPLETE_EVENT: {
        pending_action_ = NULL;
        if (NULL != write_state_.vio_.cont_) {
          write_state_.vio_.cont_->handle_event(VC_EVENT_WRITE_COMPLETE, &write_state_.vio_);
        }
        break;
      }
      case CLIENT_INFORM_MYSQL_CLIENT_TRANSFER_RESP_EVENT: {
        pending_action_ = NULL;
        if (OB_FAIL(transfer_bytes())) {
          LOG_EDIAG("fail to transfer bytes", K(ret));
        }
        break;
      }
      case CLIENT_VC_SWAP_MUTEX_EVENT: { // inform client session to swap mutex
        pending_action_ = NULL;
        if (NULL != read_state_.vio_.cont_) {
          read_state_.vio_.cont_->handle_event(CLIENT_VC_SWAP_MUTEX_EVENT, data);
        }
        break;
      }
      case CLIENT_VC_DISCONNECT_LAST_USED_SS_EVENT: {
        pending_action_ = NULL;
        if (NULL != read_state_.vio_.cont_) {
          read_state_.vio_.cont_->handle_event(CLIENT_VC_DISCONNECT_LAST_USED_SS_EVENT, data);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected event", K(event), K(ret));
        break;
      }
    }
  }
  return he_ret;
}

ObVIO *ObClientVC::do_io_read(ObContinuation *c, const int64_t nbytes, ObMIOBuffer *buf)
{
  if (NULL != buf) {
    read_state_.vio_.buffer_.writer_for(buf);
  } else {
    read_state_.vio_.buffer_.destroy();
  }

  read_state_.vio_.mutex_ = ((NULL != c) ? c->mutex_ : mutex_);
  read_state_.vio_.cont_ = c;
  read_state_.vio_.nbytes_ = nbytes;
  read_state_.vio_.ndone_ = 0;
  read_state_.vio_.vc_server_ = this;
  read_state_.vio_.op_ = ObVIO::READ;

  return &read_state_.vio_;
}

ObVIO *ObClientVC::do_io_write(ObContinuation *c, const int64_t nbytes,
                               ObIOBufferReader *buf)
{
  write_state_.vio_.mutex_ = ((NULL != c) ? c->mutex_ : mutex_);
  write_state_.vio_.cont_ = c;
  write_state_.vio_.nbytes_ = nbytes;
  write_state_.vio_.ndone_ = 0;
  write_state_.vio_.vc_server_ = this;
  write_state_.vio_.op_ = ObVIO::WRITE;

  if (NULL != buf) {
    write_state_.vio_.buffer_.reader_for(buf);
    if (nbytes > 0) {
      write_state_.vio_.reenable();
    }
  } else {
    write_state_.vio_.buffer_.destroy();
  }

  return &write_state_.vio_;
}

void ObClientVC::do_io_close(const int lerrno)
{
  LOG_DEBUG("ObClientVC::do_io_close, and will be free", K(lerrno), K_(core_client),
            K_(disconnect_by_client), K_(pending_action), K(this));

  read_state_.vio_.buffer_.destroy();
  read_state_.vio_.nbytes_ = 0;
  read_state_.vio_.op_ = ObVIO::NONE;
  read_state_.vio_.cont_ = NULL;
  write_state_.vio_.buffer_.destroy();
  write_state_.vio_.nbytes_ = 0;
  write_state_.vio_.op_ = ObVIO::NONE;
  write_state_.vio_.cont_ = NULL;
  magic_ = CLIENT_MAGIC_DEAD;

  if (NULL != pending_action_) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(pending_action_->cancel())) {
      LOG_EDIAG("fail to cancel pending action", K(ret));
    } else {
      pending_action_ = NULL;
    }
  }

  is_request_sent_ = false;

  if (NULL != core_client_ && !disconnect_by_client_) {
    core_client_->handle_event(CLIENT_VC_DISCONNECT_EVENT, NULL);
    core_client_ = NULL;
  }
  op_free(this);
}

void ObClientVC::reenable_re(ObVIO *vio)
{
  if (NULL != vio) {
    int64_t read_avail = 0;
    ObIOBufferReader *reader = vio->get_reader();
    if (NULL != reader) {
      read_avail = reader->read_avail();
    }
    _LOG_DEBUG("client_vc reenable_re %s, read_avail=%ld, reader=%p, thread=%p, "
               "is_request_sent=%d",
               (ObVIO::WRITE == vio->op_) ? "Write" : "Read", read_avail,
               reader, this_ethread(), is_request_sent_);

    if (ObVIO::WRITE == vio->op_) { // write_vio
      if (NULL != core_client_ && NULL == pending_action_ && read_avail > 0) {
        int ret = OB_SUCCESS;
        if (OB_ISNULL(pending_action_ = mutex_->thread_holding_->schedule_imm(
                this, CLIENT_INFORM_MYSQL_CLIENT_TRANSFER_RESP_EVENT))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_EDIAG("fail to schedule imm", K(ret));
        }
      }
    } else if (ObVIO::READ == vio->op_) { // read_vio
      if (NULL != read_state_.vio_.cont_) {
        if (!is_request_sent_) {
          if (addr_.is_valid()) {
            ObMysqlSM *sm = reinterpret_cast<ObMysqlSM*>(read_state_.vio_.cont_);
            sm->trans_state_.server_info_.set_addr(net::ops_ip_sa_cast(addr_.get_sockaddr()));
            sm->trans_state_.force_retry_congested_ = true;
            sm->trans_state_.need_retry_ = false;
          }
          // notify client session to read mysql request
          read_state_.vio_.cont_->handle_event(VC_EVENT_READ_READY, &read_state_.vio_);
          is_request_sent_ = true;
        }
      }
    }
  }
}

int ObClientVC::transfer_bytes()
{
  int ret = OB_SUCCESS;

  // Check the state of our write buffer as well as ntodo
  ObVIO &vio = write_state_.vio_;
  int64_t ntodo = vio.ntodo();
  ObIOBufferReader *reader = vio.get_reader();
  if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to get reader", K(ret));
  } else {
    int64_t bytes_avail = reader->read_avail();
    int64_t act_on = MIN(bytes_avail, ntodo);
    int64_t total_added = 0;
    ObMIOBuffer *transfer_to = core_client_->get_client_buf();

    if (act_on <= 0) {
      ret = OB_INVALID_ARGUMENT;
      _LOG_WDIAG("act on data can not <= 0, avail=%ld, ntodo=%ld, "
                "act_on=%ld, ret=%d", bytes_avail, ntodo, act_on, ret);
    } else if (OB_ISNULL(transfer_to)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("resp buffer can not be NULL", K(transfer_to), K(ret));
    } else if (OB_FAIL(transfer_to->write(reader, act_on, total_added, 0))
        || act_on != total_added) {
      LOG_WDIAG("failed to transfer data from iobuffer reader to iobfer",
          K(act_on), K(total_added), K(ret));
    } else if (OB_FAIL(reader->consume(total_added))) {
      LOG_WDIAG("fail to consume", K(total_added), K(ret));
    } else {
      vio.ndone_ += total_added;
      LOG_DEBUG("transfer_bytes succ", "ndone", vio.ndone_, "nbytes", vio.nbytes_);
      if (write_state_.vio_.ntodo() > 0) {
        if (NULL != write_state_.vio_.cont_) {
          write_state_.vio_.cont_->handle_event(VC_EVENT_WRITE_READY, &write_state_.vio_);
        }
      } else {
        // notify ObMysqlClient to read mysql response
        if (NULL != core_client_) {
          core_client_->handle_event(VC_EVENT_READ_READY, &write_state_.vio_);
        }
      }
    }
  }

  return ret;
}

ObMysqlClient::ObMysqlClient()
  : ObContinuation(), magic_(CLIENT_MAGIC_ALIVE), reentrancy_count_(0), terminate_(false),
    is_inited_(false), in_use_(false), is_request_complete_(false), use_short_connection_(false),
    client_vc_(NULL), pool_(NULL), cr_(NULL),
    active_timeout_action_(NULL), common_mutex_(), action_(), active_timeout_ms_(0),
    next_action_(CLIENT_ACTION_UNDEFINED), request_buf_(NULL),
    request_reader_(NULL), mysql_resp_(NULL), info_(), is_session_pool_client_(false),
    need_connect_retry_(false), retry_times_(0)
{
  SET_HANDLER(&ObMysqlClient::main_handler);
}

int ObMysqlClient::init(ObMysqlClientPool *pool,
                        const ObString &user_name,
                        const ObString &password,
                        const ObString &database,
                        const bool is_meta_mysql_client,
                        const ObString &cluster_name,
                        const ObString &password1,
                        ClientPoolOption* client_pool_option)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex = NULL;
  if (OB_ISNULL(pool) || OB_UNLIKELY(user_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(pool), K(user_name), K(ret));
  } else if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_ISNULL(mutex = new_proxy_mutex(CLIENT_VC_LOCK))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allocate mutex", K(ret));
  } else if (OB_FAIL(info_.set_names(user_name, password, database, cluster_name, password1))) {
    LOG_WDIAG("fail to set names", K(user_name), K(password), K(database), K(cluster_name), K(ret));
  } else {
    if (client_pool_option != NULL) {
      info_.set_need_skip_stage2(client_pool_option->need_skip_stage2_);
      is_session_pool_client_ = client_pool_option->is_session_pool_client_;
      server_addr_ = client_pool_option->server_addr_;
      schema_key_ = client_pool_option->schema_key_;
    }
    common_mutex_ = mutex;
    mutex_ = common_mutex_;
    next_action_ = CLIENT_ACTION_CONNECT;
    pool->inc_ref();
    pool_ = pool;
    is_inited_ = true;
    use_short_connection_ = is_meta_mysql_client;
  }
  return ret;
}

// 初始化探测客户端
int ObMysqlClient::init_detect_client(ObClusterResource *cr)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex = NULL;
  ObString password(get_global_proxy_config().observer_sys_password.str());
  if (OB_UNLIKELY(is_inited_ ||NULL == cr)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_ISNULL(mutex = new_proxy_mutex(CLIENT_VC_LOCK))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allocate mutex", K(ret));
  } else if (OB_FAIL(info_.set_names(ObProxyTableInfo::DETECT_USERNAME_USER, password, "obproxy", "obproxy"))) {
    LOG_WDIAG("fail to set names", K(ret));
  } else {
    common_mutex_ = mutex;
    mutex_ = common_mutex_;
    next_action_ = CLIENT_ACTION_CONNECT;
    cr->inc_ref();
    cr_ = cr;
    is_inited_ = true;
    use_short_connection_ = true;
  }
  return ret;
}

// 初始化binlog客户端
int ObMysqlClient::init_binlog_client(const ObString cluster_name, const ObString tenant_name)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex = NULL;
  ObString password(get_global_proxy_config().observer_sys_password.str());
  ObClusterResource *cr = get_global_resource_pool_processor().acquire_avail_cluster_resource(cluster_name);
  if (OB_UNLIKELY(is_inited_ ||NULL == cr)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_ISNULL(mutex = new_proxy_mutex(CLIENT_VC_LOCK))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allocate mutex", K(ret));
  } else {
    char full_user_name[OB_PROXY_FULL_USER_NAME_MAX_LEN + 1];
    full_user_name[0] = '\0';
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(full_user_name, OB_PROXY_FULL_USER_NAME_MAX_LEN + 1, pos, "%s@%.*s#%.*s",
            ObProxyTableInfo::BINLOG_USERNAME_USER, tenant_name.length(), tenant_name.ptr(),
            cluster_name.length(), cluster_name.ptr()))) {
      LOG_WDIAG("fail to databuff_printf", K(cluster_name), K(tenant_name), K(ret));
    } else if (OB_FAIL(info_.set_names(full_user_name, password, "binlog", cluster_name))) {
      LOG_WDIAG("fail to set names", K(ret));
    } else {
      common_mutex_ = mutex;
      mutex_ = common_mutex_;
      next_action_ = CLIENT_ACTION_CONNECT;
      cr_ = cr;
      is_inited_ = true;
      use_short_connection_ = true;
    }
  }
  return ret;
}

int ObMysqlClient::post_request(
    ObContinuation *cont,
    const ObMysqlRequestParam &request_param,
    const int64_t timeout_ms,
    ObAction *&action)
{
  LOG_DEBUG("post request", K(cont), K(request_param), K(timeout_ms), K(this_ethread()));
  action = NULL;
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(mutex_ != common_mutex_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("mysql client mutex is should be equal to common mutex", "mutex", mutex_.ptr_,
              "common_mutex", common_mutex_.ptr_, K(ret));
  } else if (OB_ISNULL(cont) || OB_ISNULL(cont->mutex_)
             || OB_ISNULL(cont->mutex_->thread_holding_)
             || OB_UNLIKELY(!request_param.is_valid()) || OB_UNLIKELY(timeout_ms <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(cont), K(request_param), K(timeout_ms), K(ret));
  } else if (OB_UNLIKELY(this_ethread() != (cont->mutex_->thread_holding_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this_ethread must be equal with the caller's thread_holding",
              "this_ethread", this_ethread(), "caller's thread_holding",
              cont->mutex_->thread_holding_, K(ret));
  } else if (!is_avail()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WDIAG("some client request is flying", "current sql", request_param.sql_,
             "flying sql", info_.get_request_sql(),  K(ret));
  } else if (cont->mutex_ != common_mutex_) {
    // in sync post request, cont->mutex is common mutex, no need to swap mutex
    // in async post request, we should swap mutex to cont->mutex
    MUTEX_TRY_LOCK(lock, common_mutex_, this_ethread());
    if (lock.is_locked()) {
      // under lock, double check
      if (!is_avail()) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WDIAG("some client request is flying", "current sql", request_param.sql_,
                 "flying sql", info_.get_request_sql(),  K(ret));
      } else {
        mutex_ = cont->mutex_;
        if (NULL != client_vc_) {
          if (OB_UNLIKELY(client_vc_->mutex_ != common_mutex_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_EDIAG("client_vc mutex must be common mutex in idle time",
                      "client vc mutex", client_vc_->mutex_.ptr_,
                      "common mutex", common_mutex_.ptr_, K(ret));
          } else {
            client_vc_->handle_event(CLIENT_VC_SWAP_MUTEX_EVENT, mutex_.ptr_);
            client_vc_->mutex_ = mutex_;
          }
        }
      }
    } else {
      // if fail to lock, mysql proxy cont will reschedule post request after 10ms
      ret = OB_EAGAIN;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL != mysql_resp_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("mysql_resp_ should be null, it should not happened",
               K(mysql_resp_), K(ret));
    } else if (OB_ISNULL(mysql_resp_ = op_alloc(ObClientMysqlResp))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to allocate ObClientMysqlResp", K(ret));
    } else if (OB_FAIL(mysql_resp_->init())) {
      LOG_WDIAG("fail to init client mysql resp", K(ret));
    } else if (OB_FAIL(info_.set_request_param(request_param))) {
      LOG_WDIAG("fail to set request sql", K(request_param), K(ret));
    } else {
      active_timeout_ms_ = timeout_ms;
      if (CLIENT_ACTION_CONNECT == next_action_ && info_.can_change_password()) {
        retry_times_ = 1;
      }
      if (OB_FAIL(do_post_request())) {
        LOG_WDIAG("fail to do post request", K(ret));
      } else if (OB_ISNULL(client_vc_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("client vc is null, connection may has been closed", K(ret));
      } else {
        action_.set_continuation(cont);
        action_.cancelled_ = false;
        action = &action_;
        in_use_ = true;
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != mysql_resp_) {
      op_free(mysql_resp_);
      mysql_resp_ = NULL;
    }
    action = NULL;
    release(false);
  }

  return ret;
}

int ObMysqlClient::main_handler(int event, void *data)
{
  int he_ret = EVENT_CONT;
  int ret = OB_SUCCESS;
  ++reentrancy_count_;
  LOG_DEBUG("ObMysqlClient Received event", "event_name", get_client_event_name(event),
            "next action", get_client_action_name(next_action_), K(data), "thread", this_ethread());
  if (OB_UNLIKELY(CLIENT_MAGIC_ALIVE != magic_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this mysql client is dead", K_(magic), K(this), K(ret));
  } else if (this_ethread() != mutex_->thread_holding_ && CLIENT_DESTROY_SELF_EVENT != event) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this_ethread must be equal with thread_holding", "this_ethread",
              this_ethread(), "thread_holding", mutex_->thread_holding_, K(this), K(ret));
  } else {
    switch (event) {
      case VC_EVENT_READ_READY: { // response received
        if (OB_FAIL(do_next_action(data))) {
          LOG_WDIAG("fail to do next action", "next_action",
                   get_client_action_name(next_action_), K(ret));
        }
        break;
      }
      case CLIENT_VC_DISCONNECT_EVENT: {
        if (OB_FAIL(handle_client_vc_disconnect())) {
          LOG_WDIAG("fail to hanlde client vc disconnect", K(ret));
        }
        break;
      }
      case CLIENT_DESTROY_SELF_EVENT: {
        // 1. treat as active timeout
        if (OB_FAIL(cancel_active_timeout())) {
          LOG_WDIAG("fail to cancel active timeout", K(ret));
        } else if (OB_FAIL(handle_active_timeout())) {
          LOG_WDIAG("fail to handle active timeout", K(ret));
        }
        // 2. kill this
        terminate_ = true;
        break;
      }
      case EVENT_INTERVAL: {
        active_timeout_action_ = NULL;
        if (OB_FAIL(handle_active_timeout())) {
          LOG_WDIAG("fail to handle active timeout", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unknown event", K(event), K(ret));
        break;
      }
    }
  }

  if (1 == reentrancy_count_) {
    // here common_mutex_ is free or held by this thread, so we can ensure lock it
    MUTEX_LOCK(lock, common_mutex_, this_ethread());
    if (OB_SUCCESS == ret && need_connect_retry_ && CLIENT_ACTION_CONNECT == next_action_) {
      need_connect_retry_ = false;
      retry_times_ = 0;
      is_request_complete_ = false;
      info_.change_password();
      if (NULL != mysql_resp_) {
        op_free(mysql_resp_);
        if (OB_ISNULL(mysql_resp_ = op_alloc(ObClientMysqlResp))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to allocate ObClientMysqlResp", K(ret));
        } else if (mysql_resp_->init()) {
          LOG_WDIAG("fail to init client mysql resp", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        do_post_request();
      }

      --reentrancy_count_;
      if (OB_UNLIKELY(reentrancy_count_ < 0)) {
        LOG_EDIAG("invalid reentrancy_count", K_(reentrancy_count), K(this));
      }
    } else {
      if (is_request_complete_) {
        if (OB_FAIL(handle_request_complete())) {
          LOG_WDIAG("fail to handle request complete", K(ret));
        }
      }

      if (terminate_) {
        kill_this();
        he_ret = EVENT_DONE;
      } else {
        --reentrancy_count_;
        if (OB_UNLIKELY(reentrancy_count_ < 0)) {
          LOG_EDIAG("invalid reentrancy_count", K_(reentrancy_count), K(this));
        }
      }
    }
  } else {
    --reentrancy_count_;
    if (OB_UNLIKELY(reentrancy_count_ < 0)) {
      LOG_EDIAG("invalid reentrancy_count", K_(reentrancy_count), K(this));
    }
  }

  return he_ret;
}

int ObMysqlClient::transport_mysql_resp()
{
  int ret = OB_SUCCESS;

  if (NULL != client_vc_) {
    if (is_in_auth()) {
      // if fail to auth, free client_vc
      if (mysql_resp_->is_error_resp()) {
        // free client_vc
        if (retry_times_ == 1 && CLIENT_ACTION_READ_LOGIN_RESP == next_action_) {
          //do nothing
          if (ER_ACCESS_DENIED_ERROR == mysql_resp_->get_err_code()) {
            need_connect_retry_ = true;
          }
        }
        client_vc_->handle_event(VC_EVENT_EOS, NULL);
        client_vc_ = NULL;
        //Attention!! the request buf will be free by client session
        request_buf_ = NULL;
        request_reader_ = NULL;
        next_action_ = CLIENT_ACTION_CONNECT;
      }
    } else {
      if (use_short_connection_) {
        client_vc_->handle_event(CLIENT_VC_DISCONNECT_LAST_USED_SS_EVENT, NULL);
      }
    }
  }

  is_request_complete_ = true;
  return ret;
}

int ObMysqlClient::handle_request_complete()
{
  int ret = OB_SUCCESS;
  ObContinuation *cont = action_.continuation_;
  if (NULL != cont) {
    if (OB_FAIL(cancel_active_timeout())) {
      LOG_WDIAG("fail to cancel active timeout", K(ret));
    } else {
      ObClientMysqlResp *mysql_resp = NULL;
      bool need_callback = false;

      if (!action_.cancelled_) {
        need_callback = true;
        mysql_resp = mysql_resp_;
        mysql_resp_ = NULL;
      } else {
        // when cancelled, free the mysql_resp_
        if (NULL != mysql_resp_) {
          op_free(mysql_resp_);
          mysql_resp_ = NULL;
        }
      }

      release(true);
      if (need_callback) {
        cont->handle_event(CLIENT_TRANSPORT_MYSQL_RESP_EVENT, mysql_resp);
      }
    }
  } else {
    LOG_INFO("no caller, no need to call out", K(action_.continuation_));
  }

  return ret;
}

int ObMysqlClient::schedule_active_timeout()
{
  int ret = OB_SUCCESS;
  int64_t timeout_ms = active_timeout_ms_;
  if (timeout_ms <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(timeout_ms), K(ret));
  } else if (OB_FAIL(cancel_active_timeout())) {
    LOG_WDIAG("fail to cancel active timeout", K(ret));
  } else {
    ObEThread *this_thread = this_ethread();
    if (OB_ISNULL(this_thread)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("this thread can not be NULL", K(this_thread), K(ret));
    } else {
      active_timeout_action_ = this_thread->schedule_in(
        this, HRTIME_MSECONDS(timeout_ms));
      if (OB_ISNULL(active_timeout_action_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to schedule in", K(ret));
      }
    }
  }

  return ret;
}

int ObMysqlClient::cancel_active_timeout()
{
  int ret = OB_SUCCESS;
  if (NULL != active_timeout_action_) {
    if (OB_FAIL(active_timeout_action_->cancel())) {
      LOG_WDIAG("fail to cancel active timeout", K(ret));
    } else {
      active_timeout_action_ = NULL;
    }
  }
  return ret;
}

int ObMysqlClient::handle_active_timeout()
{
  int ret = OB_SUCCESS;
  LOG_INFO("mysql client active timeout",
           K_(active_timeout_ms), K_(next_action), K_(info));

  // disconnect client vc
  if (NULL != client_vc_) {
    client_vc_->handle_event(VC_EVENT_EOS, NULL);
    client_vc_ = NULL;
    //Attention!! the request buf will be free by client session
    request_buf_ = NULL;
    request_reader_ = NULL;
  }

  if (OB_FAIL(handle_client_vc_disconnect())) {
    LOG_WDIAG("fail to handle client vc disconnect", K(ret));
  }

  return ret;
}

int ObMysqlClient::handle_client_vc_disconnect()
{
  int ret = OB_SUCCESS;
  request_buf_ = NULL; // request buf will be free in client session
  client_vc_ = NULL;

  if (OB_SUCC(ret)) {
    if (NULL != mysql_resp_) {
      op_free(mysql_resp_);
      mysql_resp_ = NULL;
    }

    next_action_ = CLIENT_ACTION_CONNECT;
    if (NULL != action_.continuation_) {
      is_request_complete_ = true;
    } else {
      // no need call out
      // when fail to new connection, it will come here
      if (NULL != active_timeout_action_) {
        if (OB_FAIL(cancel_active_timeout())) {
          LOG_WDIAG("fail to cancel active timeout", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObMysqlClient::do_post_request()
{
  int ret = OB_SUCCESS;
  switch (next_action_) {
    case CLIENT_ACTION_CONNECT: {
      if (OB_ISNULL(request_buf_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc request miobuffer", K(ret));
      } else if (OB_ISNULL(request_reader_ = request_buf_->alloc_reader())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to alloc reader", K(ret));
      } else if (OB_FAIL(schedule_active_timeout())) {
        LOG_WDIAG("fail to schedule_active_timeout", K(ret));
      } else if (OB_FAIL(setup_read_handshake())) {
        LOG_WDIAG("fail to setup read handshake", K(ret));
      }
      if (OB_FAIL(ret) && (NULL != request_buf_)) {
        free_miobuffer(request_buf_);
        request_buf_ = NULL;
        request_reader_ = NULL;
      }
      break;
    }
    case CLIENT_ACTION_READ_NORMAL_RESP: {
     if (OB_FAIL(setup_read_normal_resp())) {
        LOG_WDIAG("fail to setup read normal resp", K(ret));
      } else if (OB_FAIL(schedule_active_timeout())) {
        LOG_WDIAG("fail to schedule_active_timeout", K(ret));
      } else if (OB_FAIL(forward_mysql_request())) {
        LOG_WDIAG("fail to schedule post reuqest", K(ret));
      }
      break;
    }
    case CLIENT_ACTION_READ_HANDSHAKE:
    case CLIENT_ACTION_READ_LOGIN_RESP:
    case CLIENT_ACTION_SET_AUTOCOMMIT:
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("client' next action can not be this action", "next_action",
               get_client_action_name(next_action_), K(ret));
      break;
    }
  }

  return ret;
}

int ObMysqlClient::do_next_action(void *data)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObMysqlClient::do_next_action", "next action",
            get_client_action_name(next_action_), K(data));

  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(data), K(ret));
  } else {
    ObVIO &vio = *(reinterpret_cast<ObVIO *>(data));
    switch (next_action_) {
      case CLIENT_ACTION_READ_HANDSHAKE: {
        if (OB_FAIL(transfer_and_analyze_response(vio, OB_MYSQL_COM_HANDSHAKE))) {
          LOG_WDIAG("fail to transfer and analyze resposne", K(ret));
        } else if (!mysql_resp_->is_resp_completed()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("mysql resp must be received complete", K(ret));
        } else if (OB_FAIL(notify_transfer_completed())) {
          LOG_WDIAG("fail to notify transfer completed", K(ret));
        } else if (NULL != client_vc_) { // NULL means client_vc has closed
          if (mysql_resp_->is_error_resp()) {
            if (OB_FAIL(transport_mysql_resp())) {
              LOG_WDIAG("fail to transfrom mysql resp", K(ret));
            }
          } else {
            if (OB_FAIL(setup_read_login_resp())) {
              LOG_WDIAG("fail to setup read login resp", K(ret));
            } else if (OB_FAIL(forward_mysql_request())) {
              LOG_WDIAG("fail to schedule post reuqest", K(ret));
            }
          }
        }
        break;
      }
      case CLIENT_ACTION_READ_LOGIN_RESP: {
        if (ObMysqlRequestParam::CLIENT_VC_TYPE_DETECT == info_.get_request_param().client_vc_type_) {
          is_request_complete_ = true;
        } else if (OB_FAIL(transfer_and_analyze_response(vio, OB_MYSQL_COM_LOGIN))) {
          LOG_WDIAG("fail to transfer and analyze resposne", K(ret));
        } else if (!mysql_resp_->is_resp_completed()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("mysql resp must be received complete", K(ret));
        } else if (OB_FAIL(notify_transfer_completed())) {
          LOG_WDIAG("fail to notify transfer completed", K(ret));
        } else if (NULL != client_vc_) { // NULL means client_vc has closed
          if (mysql_resp_->is_error_resp()) {
            if (OB_FAIL(transport_mysql_resp())) {
              LOG_WDIAG("fail to transfrom mysql resp", K(ret));
            }
          } else if (info_.get_request_param().ob_client_flags_.is_skip_autocommit()) {
            if (OB_FAIL(setup_read_normal_resp())) {
              LOG_WDIAG("fail to setup read normal resp", K(ret));
            } else if (OB_FAIL(forward_mysql_request())) {
              LOG_WDIAG("fail to schedule post request", K(ret));
            }
          } else {
            retry_times_ = 0;
            if (OB_FAIL(setup_read_autocommit_resp())) {
              LOG_WDIAG("fail to setup read autocommit resp", K(ret));
            } else if (OB_FAIL(forward_mysql_request())) {
              LOG_WDIAG("fail to schedule post reuqest", K(ret));
            }
          }
        }
        break;
      }
      case CLIENT_ACTION_SET_AUTOCOMMIT: {
        if (OB_FAIL(transfer_and_analyze_response(vio, obmysql::OB_MYSQL_COM_QUERY))) {
          LOG_WDIAG("fail to transfer and analyze resposne", K(ret));
        } else if (!mysql_resp_->is_resp_completed()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("mysql resp must be received complete", K(ret));
        } else if (OB_FAIL(notify_transfer_completed())) {
          LOG_WDIAG("fail to notify transfer completed", K(ret));
        } else if (NULL != client_vc_) { // NULL means client_vc has closed
          if (mysql_resp_->is_error_resp()) {
            if (OB_FAIL(transport_mysql_resp())) {
              LOG_WDIAG("fail to transfrom mysql resp", K(ret));
            }
          } else {
            if (OB_FAIL(setup_read_normal_resp())) {
              LOG_WDIAG("fail to setup read normal resp", K(ret));
            } else if (OB_FAIL(forward_mysql_request())) {
              LOG_WDIAG("fail to schedule post reuqest", K(ret));
            }
          }
        }
        break;
      }
      case CLIENT_ACTION_READ_NORMAL_RESP: {
        if (OB_FAIL(transfer_and_analyze_response(vio, obmysql::OB_MYSQL_COM_QUERY))) {
          LOG_WDIAG("fail to transfer and analyze resposne", K(ret));
        } else if (!mysql_resp_->is_resp_completed()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("mysql resp must be received complete", K(ret));
        } else if (OB_FAIL(notify_transfer_completed())) {
          LOG_WDIAG("fail to notify transfer completed", K(ret));
        } else if (NULL != client_vc_) { // NULL means client_vc has closed
          if (OB_FAIL(transport_mysql_resp())) {
            LOG_WDIAG("fail to transfrom mysql resp", K(ret));
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        _LOG_WDIAG("unknown action, action=%d, ret=%d", next_action_, ret);
      }
    }
  }

  return ret;
}

int ObMysqlClient::transfer_and_analyze_response(ObVIO &vio, const obmysql::ObMySQLCmd cmd)
{
  int ret = OB_SUCCESS;

  if (vio.ndone_ == vio.nbytes_) {
    LOG_DEBUG("transfer_and_analyze_response");
    if (OB_FAIL(mysql_resp_->analyze_resp(cmd))) {
      LOG_WDIAG("fail to analyze_trans_response", K(ret));
    } else if (!mysql_resp_->is_resp_completed()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("mysql response must be received completed here", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ndone should be equal to nbytes",
             K_(vio.ndone), K_(vio.nbytes), K(ret));
  }

  return ret;
}

int ObMysqlClient::forward_mysql_request()
{
  int ret = OB_SUCCESS;
  client_vc_->set_addr(info_.get_request_param().target_addr_);

  if (request_reader_->read_avail() > 0) {
    client_vc_->clear_request_sent();
    client_vc_->reenable_read();
  }

  return ret;
}

int ObMysqlClient::notify_transfer_completed()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_vc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("client vc can not be NULL", K_(client_vc), K(ret));
  } else {
    client_vc_->handle_event(CLIENT_MYSQL_RESP_TRANSFER_COMPLETE_EVENT, NULL);
  }

  return ret;
}

int ObMysqlClient::setup_read_login_resp()
{
  int ret = OB_SUCCESS;
  // 1. before write new rquest, check valid firstly
  if (OB_ISNULL(request_buf_) || OB_ISNULL(request_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("request_buf or request_reader is null", K_(request_buf), K_(request_reader), K(ret));
  } else if (OB_UNLIKELY(request_reader_->read_avail() > 0)) {
    LOG_WDIAG("request buf has remain data, unnormal state", K_(request_reader), K_(request_buf),
             "read_avail", request_reader_->read_avail());
    if (OB_FAIL(request_reader_->consume_all())) {
      LOG_WDIAG("fail to consume all", K(ret));
    }
  }

  // 2. prepare handshake response packet
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObClientUtils::build_handshake_response_packet(mysql_resp_, &info_, request_buf_))) {
      LOG_WDIAG("fail to build handsake response packet", K_(info), K(ret));
    } else {
      // 3. set next action
      next_action_ = CLIENT_ACTION_READ_LOGIN_RESP;
      LOG_DEBUG("ObMysqlClient::setup_read_login_resp, will send handshake response to observer");
      // 4.consume the handshake packet data
      mysql_resp_->consume_resp_buf();
    }
  }
  return ret;
}

int ObMysqlClient::setup_read_autocommit_resp()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(request_buf_) || OB_ISNULL(request_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("request_buf or request_reader is null", K_(request_buf), K_(request_reader), K(ret));
  } else if (OB_UNLIKELY(request_reader_->read_avail() > 0)) {
    LOG_WDIAG("request buf has remain data, unnormal state", K_(request_reader), K_(request_buf),
             "read_avail", request_reader_->read_avail());
    if (OB_FAIL(request_reader_->consume_all())) {
      LOG_WDIAG("fail to consume all", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const bool use_compress = false;
    const bool is_checksum_on = false;
    // current all inner request use normal mysql protocol
    ObString sql ("SET @@autocommit = 1");
    if (OB_FAIL(ObMysqlRequestBuilder::build_mysql_request(*request_buf_, obmysql::OB_MYSQL_COM_QUERY, sql,
                                                           use_compress, is_checksum_on, 0))) {
      LOG_WDIAG("fail to write buffer", K(sql), K_(request_buf), K(ret));
    } else {
      mysql_resp_->consume_resp_buf();
      next_action_ = CLIENT_ACTION_SET_AUTOCOMMIT;
      LOG_DEBUG("ObMysqlClient::will send mysql request", K(sql),
                "read_avail", request_reader_->read_avail());
    }
  }

  return ret;
}

int ObMysqlClient::setup_read_normal_resp()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(request_buf_) || OB_ISNULL(request_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("request_buf or request_reader is null", K_(request_buf), K_(request_reader), K(ret));
  } else if (OB_UNLIKELY(request_reader_->read_avail() > 0)) {
    LOG_WDIAG("request buf has remain data, unnormal state", K_(request_reader), K_(request_buf),
             "read_avail", request_reader_->read_avail());
    if (OB_FAIL(request_reader_->consume_all())) {
      LOG_WDIAG("fail to consume all", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const ObString &sql = info_.get_request_sql();
    const bool use_compress = false;
    const bool is_checksum_on = false;
    if (OB_FAIL(ObMysqlRequestBuilder::build_mysql_request(*request_buf_, obmysql::OB_MYSQL_COM_QUERY, sql,
                                                           use_compress, is_checksum_on, 0))) {
      LOG_WDIAG("fail to write buffer", K(sql), K_(request_buf), K(ret));
    } else {
      mysql_resp_->consume_resp_buf();
      next_action_ = CLIENT_ACTION_READ_NORMAL_RESP;
      LOG_DEBUG("ObMysqlClient::will send mysql request", K(sql),
                "read_avail", request_reader_->read_avail());
    }
  }

  return ret;
}

int ObMysqlClient::do_new_connection_with_shard_conn(ObMysqlClientSession *client_session)
{
  int ret = OB_SUCCESS;

  dbconfig::ObShardConnector *shard_conn = pool_->acquire_shard_conn(); // inc ref
  dbconfig::ObShardProp *shard_prop = pool_->acquire_shard_prop(); // inc ref
  LOG_DEBUG("new connection", KP(shard_conn), KP(shard_prop));
  if (OB_ISNULL(shard_conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("shard conn should not be null here", K(ret));
    client_session->destroy();
  } else if (OB_FAIL(client_session->new_connection(client_vc_, request_buf_, request_reader_,
                                                    shard_conn, shard_prop))) {
    LOG_WDIAG("fail to new_connection", K(ret));
  }

  if (NULL != shard_conn) {
    shard_conn->dec_ref();
  }

  if (NULL != shard_prop) {
    shard_prop->dec_ref();
  }

  return ret;
}

int ObMysqlClient::do_new_connection_with_cr(ObMysqlClientSession *client_session)
{
  int ret = OB_SUCCESS;

  ObClusterResource *cr = NULL;
  if (NULL != cr_) {
    cr_->inc_ref();
    cr = cr_;
  } else {
    cr = pool_->acquire_cluster_resource(); // inc ref
  }
  LOG_DEBUG("new connection", K(cr), KPC(cr));
  if (OB_ISNULL(cr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("cluster resource should not be null here", K(ret));
    client_session->destroy();
  } else if (OB_FAIL(client_session->new_connection(client_vc_, request_buf_, request_reader_, cr))) {
    LOG_WDIAG("fail to new_connection", K(ret));
  }

  if (NULL != cr) {
    cr->dec_ref();
  }
  return ret;
}

int ObMysqlClient::setup_read_handshake()
{
  int ret = OB_SUCCESS;
  ObMysqlClientSession *client_session = NULL;
  if (OB_ISNULL(client_vc_ = op_alloc_args(ObClientVC, *this))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allocate ObClientVC", K(ret));
  } else if (OB_ISNULL(client_session = op_reclaim_alloc(ObMysqlClientSession))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allocate ObMysqlClientSession", K(ret));
  } else {
    client_vc_->mutex_ = mutex_;
    client_session->set_proxy_mysql_client();
    if (info_.get_request_param().ob_client_flags_.is_send_request_direct()) {
      client_session->set_can_send_request();
      client_session->is_need_update_dummy_entry_ = true;
    }
    client_session->set_session_pool_client(is_session_pool_client_);
    if (is_session_pool_client_) {
      // client_session->set_is_dbmesh_user(false);
      client_session->schema_key_ = schema_key_;
      LOG_DEBUG("set schema_key_ ", K(schema_key_), K(client_session));
    }
    client_session->set_server_addr(server_addr_);
    client_session->set_first_dml_sql_got();
    client_session->inner_request_param_ = &info_.get_request_param();

    next_action_ = CLIENT_ACTION_READ_HANDSHAKE;
    if ((NULL != pool_ && pool_->is_cluster_param()) || NULL != cr_) {
      if (OB_FAIL(do_new_connection_with_cr(client_session))) {
        LOG_WDIAG("fail to new connection with cr", K(ret));
      }
    } else {
      if (OB_FAIL(do_new_connection_with_shard_conn(client_session))) {
        LOG_WDIAG("fail to new connection with shard conn", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("create new proxy client_session", K(client_session->get_proxy_sessid()),
                K(client_session->get_cs_id()));
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != client_vc_) {
      op_free(client_vc_);
      client_vc_ = NULL;
    }
    next_action_ = CLIENT_ACTION_CONNECT;
    //client_session will free in new_connection()
  }

  return ret;
}

void ObMysqlClient::release(bool is_need_check_reentry)
{
  if (OB_LIKELY(!is_need_check_reentry || 1 == reentrancy_count_) && OB_LIKELY(!terminate_)) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(cancel_active_timeout())) {
      LOG_EDIAG("fail to cancel timeout action,"
                "we can not release it back to client pool", K(ret));
    } else {
      // for defense, make sure client vc's mutex is common mutex when release to client pool;
      // Never free client vc in mysql client, it will be free by mysql_sm
      if ((ObMysqlRequestParam::CLIENT_VC_TYPE_DETECT != info_.get_request_param().client_vc_type_) && NULL != client_vc_ && client_vc_->mutex_ != common_mutex_) {
        client_vc_->handle_event(CLIENT_VC_SWAP_MUTEX_EVENT, common_mutex_.ptr_);
        if (NULL != client_vc_) {
          client_vc_->mutex_ = common_mutex_;
        }
      }
      action_.set_continuation(NULL);
      action_.cancelled_ = false;
      active_timeout_ms_ = 0;

      info_.reset_sql();
      in_use_ = false;
      is_request_complete_ = false;
      mutex_ = common_mutex_; // when idle, keep common_mutex_
      if (NULL != pool_) {
        pool_->release_mysql_client(this);
      }
    }
  }
}

void ObMysqlClient::kill_this()
{
  LOG_INFO("mysql client will kill self", K(this));
  int ret = OB_SUCCESS;
  // ignore ret, continue
  if (OB_FAIL(cancel_active_timeout())) {
    LOG_WDIAG("fail to cancel active timeout");
  }

  // free client vc
  if (NULL != client_vc_) {
    client_vc_->handle_event(VC_EVENT_EOS, NULL);
    client_vc_ = NULL;
    //Attention!! the request buf will be free by client session
    request_buf_ = NULL;
    request_reader_ = NULL;
    next_action_ = CLIENT_ACTION_UNDEFINED;
  }

  if (OB_LIKELY(NULL != pool_)) {
    pool_->dec_ref();
    pool_ = NULL;
  }

  if (NULL != cr_) {
    cr_->dec_ref();
    cr_ = NULL;
  }

  is_inited_ = false;
  in_use_ = false;
  use_short_connection_ = false;
  active_timeout_ms_ = 0;

  if (NULL != mysql_resp_) {
    op_free(mysql_resp_);
    mysql_resp_ = NULL;
  }
  info_.reset();

  if (NULL != request_buf_) {
    free_miobuffer(request_buf_);
    request_buf_ = NULL;
  }
  request_reader_ = NULL;
  common_mutex_.release();
  mutex_.release();
  magic_ = CLIENT_MAGIC_DEAD;
  action_.mutex_.release();

  op_free(this);
}

int ObMysqlClient::alloc(ObMysqlClientPool *pool, ObMysqlClient *&client,
    const ObString &user_name, const ObString &password,
    const ObString &database, const bool is_meta_mysql_client, const ObString &cluster_name,
    const ObString &password1, ClientPoolOption* client_pool_option)
{
  int ret = OB_SUCCESS;
  client = NULL;
  if (OB_ISNULL(client = op_alloc(ObMysqlClient))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allocate ObMysqlClient", K(ret));
  } else if (OB_FAIL(client->init(pool, user_name, password, database, is_meta_mysql_client, cluster_name, password1, client_pool_option))) {
    LOG_WDIAG("fail to init client", K(ret));
  }
  if (OB_FAIL(ret) && (NULL != client)) {
    client->kill_this();
    client = NULL;
  }
  return ret;
}

const char *ObMysqlClient::get_client_action_name(const ObClientActionType type)
{
  const char *name = NULL;
  switch (type) {
    case CLIENT_ACTION_UNDEFINED:
      name = "CLIENT_ACTION_UNDEFINED";
      break;
    case CLIENT_ACTION_CONNECT:
      name = "CLIENT_ACTION_CONNECT";
      break;
    case CLIENT_ACTION_READ_HANDSHAKE:
      name = "CLIENT_ACTION_READ_HANDSHAKE";
      break;
    case CLIENT_ACTION_SET_AUTOCOMMIT:
      name = "CLIENT_ACTION_SET_AUTOCOMMIT";
      break;
    case CLIENT_ACTION_READ_LOGIN_RESP:
      name = "CLIENT_ACTION_READ_LOGIN_RESP";
      break;
    case CLIENT_ACTION_READ_NORMAL_RESP:
      name = "CLIENT_ACTION_READ_NORMAL_RESP";
      break;
    default:
      name = "CLIENT_ACTION_UNKNOWN";
      break;
  }
  return name;
}

const char *ObMysqlClient::get_client_event_name(const int64_t event)
{
  const char *name = NULL;

  switch (event) {
    case VC_EVENT_READ_READY:
      name = "VC_EVENT_READ_READY";
      break;
    case VC_EVENT_EOS:
      name = "VC_EVENT_EOS";
      break;
    case CLIENT_DESTROY_SELF_EVENT:
      name = "CLIENT_DESTROY_SELF_EVENT";
      break;
    case EVENT_INTERVAL:
      name = "CLIENT_REQUEST_ACTIVE_TIMEOUT_EVENT";
      break;
    case CLIENT_VC_DISCONNECT_EVENT:
      name = "CLIENT_VC_DISCONNECT_EVENT";
      break;
    case CLIENT_MYSQL_RESP_TRANSFER_COMPLETE_EVENT:
      name = "CLIENT_MYSQL_RESP_TRANSFER_COMPLETE_EVENT";
      break;
    case CLIENT_INFORM_MYSQL_CLIENT_TRANSFER_RESP_EVENT:
      name = "CLIENT_INFORM_MYSQL_CLIENT_TRANSFER_RESP_EVENT";
      break;
    case CLIENT_VC_SWAP_MUTEX_EVENT:
      name = "CLIENT_VC_SWAP_MUTEX_EVENT";
      break;
    case CLIENT_VC_DISCONNECT_LAST_USED_SS_EVENT:
      name = "CLIENT_VC_DISCONNECT_LAST_USED_SS_EVENT";
      break;
    default:
      name = "CLIENT_EVENT_UNKNOWN";
      break;
  }
  return name;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
