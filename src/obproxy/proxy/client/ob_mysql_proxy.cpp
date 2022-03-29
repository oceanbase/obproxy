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

#include "lib/utility/ob_print_utils.h"
#include "lib/lock/ob_thread_cond.h"
#include "stat/ob_resource_pool_stats.h"
#include "obutils/ob_async_common_task.h"
#include "obutils/ob_config_server_processor.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/client/ob_client_vc.h"

using namespace obsys;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::dbconfig;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObClientMysqlRespWrapper
{
public:
  ObClientMysqlRespWrapper()
    : resp_(NULL), is_received_resp_(false),
      run_cond_(), is_inited_(false) {}
  ~ObClientMysqlRespWrapper() { destroy(); }

  int init(ObClientMysqlResp **resp);
  bool is_valid() { return (is_inited_) && (NULL != resp_); }
  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  void destroy();

public:
  ObClientMysqlResp **resp_;
  bool is_received_resp_;
  ObThreadCond run_cond_;

private:
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObClientMysqlRespWrapper);
};

int ObClientMysqlRespWrapper::init(ObClientMysqlResp **resp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K_(is_inited), K(ret));
  } else if (OB_ISNULL(resp)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input vlue", K(resp), K(ret));
  } else if (OB_FAIL(run_cond_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to init run cond", K(ret));
  } else {
    resp_ = resp;
    is_received_resp_ = false;
    is_inited_ = true;
  }
  return ret;
}

void ObClientMysqlRespWrapper::destroy()
{
  if (is_inited_) {
    run_cond_.destroy();
    is_inited_ = false;
    resp_ = NULL;
    is_received_resp_ = false;
  }
}

int64_t ObClientMysqlRespWrapper::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(*resp),
       K_(is_received_resp),
       K_(is_inited));
  J_OBJ_END();
  return pos;
}

enum
{
  MYSQL_PROXY_MAGIC_ALIVE = 0xAABBCCDD,
  MYSQL_PROXY_MAGIC_DEAD = 0xDDCCBBAA
};

class ObMysqlProxyCont : public ObAsyncCommonTask
{
public:
  ObMysqlProxyCont(ObProxyMutex *m, ObContinuation *cb_cont = NULL)
    : ObAsyncCommonTask(m, "mysql_proxy_cont_task", cb_cont), magic_(MYSQL_PROXY_MAGIC_ALIVE),
      is_nonblock_(false), timeout_ms_(0),
      resp_wrapper_(NULL), client_pool_(NULL), mysql_client_(NULL), request_param_() {}
  virtual ~ObMysqlProxyCont() {}

  virtual int main_handler(int event, void *data);

  virtual int schedule_timeout();
  virtual int handle_timeout();

  // for nonblock
  int async_post_request(const ObMysqlRequestParam &request_param, const int64_t timeout_ms,
                         ObMysqlClientPool *client_pool, ObAction *&action);
  // for block
  int sync_post_request(const ObString &sql, ObClientMysqlRespWrapper &wrapper,
                        const int64_t timeout_ms, ObMysqlClientPool *client_pool);

  void kill_this();
  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  static const char *get_mysql_proxy_event_name(const int64_t event);

  int do_post_request();
  int notify_caller(void *resp);

private:
  const static int64_t RETRY_INTERVAL_MS = 10; // 10ms
  uint32_t magic_;
  bool is_nonblock_;
  ObString sql_;
  int64_t timeout_ms_;
  ObClientMysqlRespWrapper *resp_wrapper_;
  ObMysqlClientPool *client_pool_;
  ObMysqlClient *mysql_client_;
  ObMysqlRequestParam request_param_;
  DISALLOW_COPY_AND_ASSIGN(ObMysqlProxyCont);
};

int64_t ObMysqlProxyCont::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(terminate),
       K_(is_nonblock),
       K_(request_param),
       KP_(timeout_action),
       KP_(pending_action),
       K_(timeout_ms),
       KP_(resp_wrapper),
       KP_(cb_cont),
       KP_(client_pool));
  J_OBJ_END();
  return pos;
}

int ObMysqlProxyCont::async_post_request(const ObMysqlRequestParam &request_param, const int64_t timeout_ms,
                                         ObMysqlClientPool *client_pool, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  if (OB_UNLIKELY(!request_param.is_valid())
      || OB_UNLIKELY(timeout_ms <= 0)
      || OB_ISNULL(client_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(request_param), K(timeout_ms), K(ret));
  } else if (OB_FAIL(request_param_.deep_copy(request_param))) {
    LOG_WARN("fail to deep_copy", K(request_param), K(ret));
  } else {
    is_nonblock_ = true;
    timeout_ms_ = timeout_ms;
    client_pool->inc_ref();
    client_pool_ = client_pool;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_post_request())) {
      LOG_WARN("fail to post request", K(ret));
    } else {
      action = &get_action();
    }
  }

  return ret;
}

int ObMysqlProxyCont::sync_post_request(const ObString &sql, ObClientMysqlRespWrapper &wrapper,
                                        const int64_t timeout_ms, ObMysqlClientPool *client_pool)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sql.empty()) || OB_UNLIKELY(!wrapper.is_valid())
      || OB_UNLIKELY(timeout_ms <= 0) || OB_ISNULL(client_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(sql), K(wrapper), K(timeout_ms), K(ret));
  } else if (OB_FAIL(request_param_.deep_copy_sql(sql))) {
    LOG_WARN("fail to deep_copy_sql", K(ret));
  } else {
    is_nonblock_ = false;
    timeout_ms_ = timeout_ms;
    resp_wrapper_ = &wrapper;
    client_pool->inc_ref();
    client_pool_ = client_pool;
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL != mysql_client_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mysql client must be null here", K_(mysql_client), K(ret));
    } else if (OB_ISNULL(mysql_client_ = client_pool_->acquire_mysql_client())) {
      // sync post request is only called in timer thread, so mysql client is free when acquired
      ret = OB_RESOURCE_OUT;
      LOG_WARN("fail to acquire mysql client for sync request", K(ret));
    } else if (OB_ISNULL(mutex_ = mysql_client_->get_common_mutex())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common mutex is null", K(ret));
    } else if (OB_ISNULL(g_event_processor.schedule_imm(this, ET_CALL, MYSQL_PROXY_POST_REQUEST_EVENT))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to schedule blocking post request", K(ret));
    }
  }

  return ret;
}

int ObMysqlProxyCont::main_handler(int event, void *data)
{
  int ret = OB_SUCCESS;
  int ret_event = EVENT_CONT;
  LOG_DEBUG("[ObMysqlProxyCont::main_handler]", "event", get_mysql_proxy_event_name(event),
            K_(pending_action), K_(timeout_action), K(this));

  if (OB_UNLIKELY(MYSQL_PROXY_MAGIC_ALIVE != magic_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this ObMysqlProxyCont is dead", K_(magic), K(ret));
  } else if (OB_UNLIKELY(this_ethread() != mutex_->thread_holding_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this_ethread must be equal with thread_holding", "this_ethread",
              this_ethread(), "thread_holding", mutex_->thread_holding_, K(ret));
  } else {
    switch (event) {
      case MYSQL_PROXY_POST_REQUEST_EVENT: {
        pending_action_ = NULL;
        if (OB_FAIL(do_post_request())) {
          LOG_WARN("fail to do_post_request", K(ret));
        }
        break;
      }
      case EVENT_INTERVAL: {
        timeout_action_ = NULL;
        if (OB_FAIL(handle_timeout())) {
          LOG_WARN("fail to handle timeout", K(ret));
        }
        break;
      }
      case CLIENT_TRANSPORT_MYSQL_RESP_EVENT: {
        pending_action_ = NULL;
        if (OB_FAIL(notify_caller(data))) {
          LOG_WARN("fail to notify caller", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown event", K(event), K(ret));
        break;
      }
    }
  }

  if (terminate_) {
    kill_this();
    ret_event = EVENT_DONE;
  }

  return ret_event;
}

int ObMysqlProxyCont::schedule_timeout()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(timeout_action_ = self_ethread().schedule_in(
            this, HRTIME_MSECONDS(timeout_ms_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to schedule in", K_(timeout_ms), K(ret));
  }
  return ret;
}

int ObMysqlProxyCont::do_post_request()
{
  int ret = OB_SUCCESS;
  if (action_.cancelled_) {
    LOG_INFO("mysql proxy cont was cancelled", KPC(this));
    // treat is as active timeout
    if (NULL != timeout_action_) {
      if (OB_FAIL(cancel_timeout_action())) {
        LOG_WARN("fail to cancel timeout action", K(ret));
      }
    }
    // for defense
    if (OB_SUCC(ret) && OB_FAIL(handle_timeout())) {
      LOG_WARN("fail to handle timeout", K(ret));
    }
  } else {
    if (NULL == timeout_action_) {
      if (OB_FAIL(schedule_timeout())) {
        LOG_WARN("fail to schedule timeout action", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ret = OB_EAGAIN;
      if (NULL == mysql_client_) {
        mysql_client_ = client_pool_->acquire_mysql_client();
      }
      if (NULL != mysql_client_) {
        ObAction *action = NULL;

        if (OB_FAIL(mysql_client_->post_request(this, request_param_, timeout_ms_, action))) {
          // if failed, mysql_client_ will be released to connection pool in post request
          if ((OB_EAGAIN == ret) || (OB_OP_NOT_ALLOW == ret)) {
            // fail to try lock mysql client common mutex, will retry
            mysql_client_ = NULL;
          } else {
            LOG_WARN("fail to post request", K(ret));
          }
        } else if (OB_ISNULL(action)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("action cannot be null", K(action), K(ret));
        } else {
          pending_action_ = action;
        }
      }

      if (OB_SUCC(ret)) {
        // succ to post request, do nothing
      } else if ((OB_EAGAIN == ret) || (OB_OP_NOT_ALLOW == ret)) { // fail to post, retry
        LOG_INFO("fail to acquire mysql client, will retry", K(this), K_(mysql_client));
        ret = OB_SUCCESS;
        if (OB_ISNULL(pending_action_ = self_ethread().schedule_in(
                this, HRTIME_MSECONDS(RETRY_INTERVAL_MS), MYSQL_PROXY_POST_REQUEST_EVENT))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to schedule in", LITERAL_K(RETRY_INTERVAL_MS), K(ret));
        }
      } else {
        LOG_WARN("fail to post request", K(ret));
      }
    }
  }

  return ret;
}

int ObMysqlProxyCont::handle_timeout()
{
  int ret = OB_SUCCESS;
  LOG_INFO("MysqlProxyCont active timeout", K(this));
  if (OB_FAIL(cancel_pending_action())) {
    LOG_WARN("fail to cancel pending action", K(ret));
  } else if (OB_FAIL(notify_caller(NULL))) {
    LOG_WARN("fail to notiry caller", K(ret));
  }

  return ret;
}

int ObMysqlProxyCont::notify_caller(void *resp)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObMysqlProxyCont will notify caller", K(resp), K_(is_nonblock));
  if (is_nonblock_) { // nonblocking
    if (!action_.cancelled_) {
      cb_cont_->handle_event(CLIENT_TRANSPORT_MYSQL_RESP_EVENT, resp);
    } else {
      // resp is alloc by client_vc, we must free it after used
      if (NULL != resp) {
        op_free((reinterpret_cast<ObClientMysqlResp *>(resp)));
        resp = NULL;
      }
    }
  } else { // blocking
    if (OB_FAIL(resp_wrapper_->run_cond_.lock())) {
      LOG_WARN("fail to lock", K(ret));
    }
    if (OB_SUCC(ret)) {
      resp_wrapper_->is_received_resp_ = true;
      *resp_wrapper_->resp_ = reinterpret_cast<ObClientMysqlResp *>(resp);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(resp_wrapper_->run_cond_.signal())) {
        LOG_WARN("fail to signal", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(resp_wrapper_->run_cond_.unlock())) {
        LOG_WARN("fail to unlock", K(ret));
      }
    }
  }
  mysql_client_ = NULL;
  LOG_DEBUG("ObMysqlProxyCont notify caller succ");

  terminate_ = true;
  return ret;
}

void ObMysqlProxyCont::kill_this()
{
  LOG_DEBUG("ObMysqlProxyCont will be free", KPC(this));
  int ret = OB_SUCCESS;
  if (OB_FAIL(cancel_pending_action())) {
    LOG_WARN("fail to cancel pending action", K(ret));
  }
  if (OB_FAIL(cancel_timeout_action())) {
    LOG_WARN("fail to cancel timeout action", K(ret));
  }

  mysql_client_ = NULL;
  if (OB_LIKELY(NULL != client_pool_)) {
    client_pool_->dec_ref();
    client_pool_ = NULL;
  }
  request_param_.reset();
  mutex_.release();
  magic_ = MYSQL_PROXY_MAGIC_DEAD;
  action_.mutex_.release();

  op_free(this);
}

const char *ObMysqlProxyCont::get_mysql_proxy_event_name(const int64_t event)
{
  const char *name = NULL;
  switch (event) {
    case MYSQL_PROXY_POST_REQUEST_EVENT:
      name = "MYSQL_PROXY_POST_REQUEST_EVENT";
      break;
    case EVENT_INTERVAL:
      name = "MYSQL_PROXY_TIMEOUT_EVENT";
      break;
    case CLIENT_TRANSPORT_MYSQL_RESP_EVENT:
      name = "CLIENT_TRANSPORT_MYSQL_RESP_EVENT";
      break;
    default:
      name = "CLIENT_ACTION_UNKNOWN";
      break;
  }
  return name;
}

int ObMysqlProxy::init(const int64_t timeout_ms,
                       const ObString &user_name,
                       const ObString &password,
                       const ObString &database,
                       const ObString &password1)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(timeout_ms <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(timeout_ms), K(ret));
  } else if (OB_FAIL(raw_mysql_client_.init(user_name, password, database, password1))) {
    LOG_WARN("fail to init raw mysql client", K(ret));
  } else {
    timeout_ms_ = timeout_ms;
    is_inited_ = true;
    LOG_DEBUG("succ to init mysql proxy", K(user_name), K(database), K(timeout_ms));
  }
  return ret;
}

int ObMysqlProxy::rebuild_client_pool(ObShardConnector *shard_conn,
                                      ObShardProp *shard_prop,
                                      const bool is_meta_mysql_client,
                                      const ObString &user_name,
                                      const ObString &password,
                                      const ObString &database,
                                      const ObString &password1,
                                      ClientPoolOption* client_pool_option)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy has not been inited", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(user_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(user_name));
  } else {
    if (OB_FAIL(alloc_client_pool(is_meta_mysql_client, user_name, password, database, password1, client_pool_option))) {
      LOG_WARN("fail to alloc client pool", K(is_meta_mysql_client), K(user_name), K(database), K(ret));
    } else {
      client_pool_->set_shard_conn(shard_conn);
      client_pool_->set_shard_prop(shard_prop);
    }
  }

  return ret;
}

int ObMysqlProxy::rebuild_client_pool(ObClusterResource *cluster_resource,
                                      const bool is_meta_mysql_client,
                                      const ObString &cluster_name,
                                      const int64_t cluster_id,
                                      const ObString &user_name,
                                      const ObString &password,
                                      const ObString &database,
                                      const ObString &password1,
                                      ClientPoolOption* client_pool_option)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy has not been inited", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(user_name.empty()
             || OB_UNLIKELY(cluster_name.empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(cluster_name), K(user_name));
  } else {
    char full_user_name[OB_PROXY_FULL_USER_NAME_MAX_LEN + 1];
    full_user_name[0] = '\0';
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(full_user_name, OB_PROXY_FULL_USER_NAME_MAX_LEN + 1, pos, "%.*s#%.*s:%ld",
                user_name.length(), user_name.ptr(), cluster_name.length(), cluster_name.ptr(), cluster_id))) {
      LOG_WARN("fail to databuff_printf", K(user_name), K(cluster_name), K(cluster_id), K(ret));
    } else {
      ObString full_username(full_user_name);
      if (OB_FAIL(alloc_client_pool(is_meta_mysql_client, full_username,
                                             password, database, password1, client_pool_option))) {
        LOG_WARN("fail to alloc client pool", K(user_name), K(database), K(ret));
      } else {
        client_pool_->set_cluster_resource(cluster_resource);
      }
    }
  }
  return ret;
}

void ObMysqlProxy::destroy()
{
  LOG_DEBUG("ObMysqlProxy::destroy()", K(this));
  stop();
  if (is_inited_) {
    is_raw_execute_ = false;
    timeout_ms_ = 0;
    raw_mysql_client_.destroy();
    destroy_client_pool();
    is_inited_ = false;
  }
}

DEF_TO_STRING(ObMysqlProxy)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(is_inited), K_(stop), K_(is_raw_execute), K_(timeout_ms),
       K_(client_pool), K_(raw_mysql_client));
  J_OBJ_END();
  return pos;
}

int ObMysqlProxy::async_read(ObContinuation *cont, const char *sql, ObAction *&action)
{
  ObMysqlRequestParam request_param(sql);
  return async_execute(cont, request_param, timeout_ms_, action);
}

int ObMysqlProxy::async_write(ObContinuation *cont, const char *sql, ObAction *&action)
{
  ObMysqlRequestParam request_param(sql);
  return async_execute(cont, request_param, timeout_ms_, action);
}

int ObMysqlProxy::async_read(event::ObContinuation *cont, const ObMysqlRequestParam &request_param, event::ObAction *&action)
{
  return async_execute(cont, request_param, timeout_ms_, action);
}

int ObMysqlProxy::async_execute(ObContinuation *cont,
                                const ObMysqlRequestParam &request_param,
                                const int64_t timeout_ms, ObAction *&action)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(stop_)) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("mysql proxy has stopped", K_(stop), K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(is_raw_execute_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("in raw execute state, can not async execute", K(request_param), K(ret));
  } else {
    LOG_DEBUG("ObMysqlProxy::async_execute", K(cont), K(request_param), K(timeout_ms), K(ret));
    ObMysqlProxyCont *mp_cont = NULL;
    ObMysqlClientPool *pool = NULL;
    if (OB_ISNULL(pool = acquire_client_pool())) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("client pool is null", K(ret));
    } else {
      if (OB_ISNULL(cont) || OB_ISNULL(cont->mutex_) || OB_UNLIKELY(!request_param.is_valid())
          || OB_UNLIKELY(timeout_ms <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid input value", K(cont), K(request_param), K(timeout_ms), K(ret));
      } else if (OB_ISNULL(mp_cont = op_alloc_args(ObMysqlProxyCont, cont->mutex_, cont))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObMysqlProxyCont", K(ret));
      } else if (OB_FAIL(mp_cont->async_post_request(request_param, timeout_ms, pool, action))) {
        LOG_WARN("fail to post request", K(ret));
        if (OB_UNLIKELY(NULL != action)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_FAIL(tmp_ret = action->cancel())) {
            LOG_WARN("fail to cancel unexpected action", K(tmp_ret));
          } else {
            action = NULL;
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      if (NULL != mp_cont) {
        mp_cont->kill_this();
        mp_cont = NULL;
      }
    }
    release_client_pool(pool);
  }

  return ret;
}

int ObMysqlProxy::read(const char *sql, ObMysqlResultHandler &result_handler)
{
  int ret = OB_SUCCESS;
  ObClientMysqlResp *resp = NULL;
  if (OB_UNLIKELY(stop_)) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("mysql proxy has stopped", K_(stop), K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else {
    if (is_raw_execute_) {
      if (OB_FAIL(raw_mysql_client_.sync_raw_execute(sql, timeout_ms_, resp))) {
        LOG_WARN("fail to sync raw execute", K(sql), K_(timeout_ms), K(resp), K(ret));
      }
    } else {
      if (OB_FAIL(sync_execute(sql, timeout_ms_, resp))) {
        LOG_WARN("fail to sync execute", K(sql), K_(timeout_ms), K(resp), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL == resp) {
        ret = OB_EMPTY_RESULT;
        LOG_WARN("resp is NULL", K(ret));
      } else {
        if (resp->is_error_resp()) {
          ret = -resp->get_err_code();
          LOG_WARN("fail to execute sql", K(sql), K(ret));
          op_free(resp);
          resp = NULL;
        } else if (resp->is_resultset_resp()) {
          result_handler.set_resp(resp);
          // nothing
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected resp", K(ret));
          op_free(resp);
          resp = NULL;
        }
      }
    }
  }

  return ret;
}

int ObMysqlProxy::write(const char *sql, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(stop_)) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("mysql proxy has stopped", K_(stop), K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else {
    ObClientMysqlResp *resp = NULL;
    if (is_raw_execute_) {
      if (OB_FAIL(raw_mysql_client_.sync_raw_execute(sql, timeout_ms_, resp))) {
        LOG_WARN("fail to sync raw execute", K(sql), K_(timeout_ms), K(resp), K(ret));
      }
    } else {
      if (OB_FAIL(sync_execute(sql, timeout_ms_, resp))) {
        LOG_WARN("fail to sync execute", K(sql), K_(timeout_ms), K(resp), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL == resp) {
        ret = OB_EMPTY_RESULT;
        LOG_WARN("resp is NULL", K(ret));
      } else {
        if (resp->is_error_resp()) {
          ret = -resp->get_err_code();
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (resp->is_ok_resp()) {
          if (OB_FAIL(resp->get_affected_rows(affected_rows))) {
            LOG_WARN("fail to get affected rows", K(ret));
          } else {
            LOG_DEBUG("write succ", K(affected_rows));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected resp", K(ret));
        }
        op_free(resp);
        resp = NULL;
      }
    }
  }
  return ret;
}

int ObMysqlProxy::sync_execute(const char *sql, const int64_t timeout_ms, ObClientMysqlResp *&res)
{
  int ret = OB_SUCCESS;
  res = NULL;
  ObMysqlProxyCont *mp_cont = NULL;
  ObClientMysqlRespWrapper wrapper;
  LOG_DEBUG("ObMysqlProxy::sync_execute", K(sql), K(timeout_ms), K(ret));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else if (OB_ISNULL(sql) || OB_UNLIKELY(timeout_ms <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(sql), K(timeout_ms), K(ret));
  } else if (OB_ISNULL(mp_cont = op_alloc_args(ObMysqlProxyCont, NULL, NULL))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObMysqlProxyCont", K(ret));
  } else if (OB_FAIL(wrapper.init(&res))) {
    LOG_WARN("fail to init wrapper", K(ret));
  } else {
    if (!wrapper.is_received_resp_) {
      if (OB_FAIL(wrapper.run_cond_.lock())) {
        LOG_WARN("fail to lock", K(ret));
      } else {
        ObMysqlClientPool *pool = NULL;
        if (OB_ISNULL(pool = acquire_client_pool())) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("client pool is null", K(ret));
        } else {
          if (OB_FAIL(mp_cont->sync_post_request(ObString::make_string(sql), wrapper, timeout_ms, pool))) {
            LOG_WARN("fail to sync post request", K(sql), K(timeout_ms), K(ret));
          } else {
            LOG_DEBUG("farword request succ, and blocking wait for resp", K(sql));
            // wait for resp
            while ((!wrapper.is_received_resp_) && OB_SUCC(ret)) {
              if (OB_FAIL(wrapper.run_cond_.wait())) {
                LOG_WARN("fail to wait", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              LOG_DEBUG("received resp succ", K(wrapper));
            }
          }
        }

        int tmp_ret = OB_SUCCESS;
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = wrapper.run_cond_.unlock()))) {
          LOG_WARN("fail to unlock", K(tmp_ret));
          if (OB_SUCC(ret)) {
            ret = tmp_ret;
          }
        }
        release_client_pool(pool);
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != mp_cont) {
      mp_cont->kill_this();
      mp_cont = NULL;
    }
    if (NULL != res) {
      op_free(res);
      res = NULL;
    }
  }
  return ret;
}

int ObMysqlProxy::set_raw_execute(const ObIArray<ObProxyReplicaLocation> &replicas)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(raw_mysql_client_.set_target_server(replicas))) {
    LOG_WARN("fail to set server addrs", K(ret));
  } else {
    is_raw_execute_ = true;
  }
  return ret;
}

int ObMysqlProxy::alloc_client_pool(const bool is_meta_mysql_client,
                                    const ObString &user_name,
                                    const ObString &password,
                                    const ObString &database,
                                    const ObString &password1,
                                    ClientPoolOption* client_pool_option)
{
  int ret = OB_SUCCESS;
  ObMysqlClientPool *client_pool = NULL;
  if (OB_UNLIKELY(user_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(user_name), K(ret));
  } else if (OB_ISNULL(client_pool = op_alloc(ObMysqlClientPool))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc memory for client pool", K(ret));
  } else if (FALSE_IT(client_pool->inc_ref())) {
    // will dec_ref in destroy()
  } else if (OB_FAIL(client_pool->init(is_meta_mysql_client, user_name,
             password, database, password1, client_pool_option))) {
    LOG_WARN("fail to init client pool", K(user_name), K(password),
             K(database), K(is_meta_mysql_client), K(ret));
  } else {
    CLIENT_POOL_INCREMENT_DYN_STAT(CREATE_CLUSTER_CLIENT_POOL_SUCC_COUNT);

    client_pool->inc_ref();
    CWLockGuard guard(rwlock_);
    clear_client_pool();
    client_pool_ = client_pool;

    // update raw mysql client info
    ObClientReuqestInfo &info = raw_mysql_client_.get_request_info();
    info.reset_names();
    bool need_skip_stage2 = false;
    if (client_pool_option) {
      need_skip_stage2 = client_pool_option->need_skip_stage2_;
    }
    info.set_need_skip_stage2(need_skip_stage2);
    if (OB_FAIL(info.set_names(user_name, password, database))) {
      LOG_WARN("fail to set raw mysql client request info", K(ret));
    }
  }

  if (OB_LIKELY(NULL != client_pool)) {
    client_pool->dec_ref();
    client_pool = NULL;
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
