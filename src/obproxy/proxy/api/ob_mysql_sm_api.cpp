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
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX PROXY_SM
#include "proxy/api/ob_mysql_sm_api.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "proxy/mysql/ob_mysql_debug_names.h"
#include "proxy/api/ob_plugin_vc.h"
#include "proxy/api/ob_api_utils_internal.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define __REMEMBER(x)  #x
#define _REMEMBER(x)   __REMEMBER(x)

#define REMEMBER(e, r) {            \
    sm_->add_history_entry(__FILE__ ":" _REMEMBER (__LINE__), e, r); \
  }

#define STATE_ENTER(state_name, e) { \
  REMEMBER (e, sm_->reentrancy_count_);  \
  int64_t stack_start = event::self_ethread().stack_start_; \
  _PROXY_SM_LOG(DEBUG, "sm_id=%u, stack_size=%ld, next_action=%s, event=%s", \
                sm_->sm_id_, stack_start - reinterpret_cast<int64_t>(&stack_start), \
                #state_name, ObMysqlDebugNames::get_event_name(e)); }

#define MYSQL_SM_SET_DEFAULT_HANDLER(h) { \
  REMEMBER(-1, sm_->reentrancy_count_);   \
  sm_->default_handler_ = h; }

ObMysqlSMApi::ObMysqlSMApi()
    : plugin_tunnel_type_(MYSQL_NO_PLUGIN_TUNNEL),
      plugin_tunnel_(NULL),
      plugin_tag_(0),
      plugin_id_(0),
      sm_(NULL),
      schedule_cont_(NULL),
      has_active_plugin_clients_(false),
      cur_hook_id_(OB_MYSQL_LAST_HOOK),
      cur_hook_(NULL),
      cur_hook_count_(0),
      callout_state_(MYSQL_API_NO_CALLOUT)
{ }

void ObMysqlSMApi::reset()
{
  plugin_tag_ =0;
  plugin_id_ = 0;
  schedule_cont_ = NULL;
  cur_hook_id_= OB_MYSQL_LAST_HOOK;
  cur_hook_ = NULL;
  cur_hook_count_ = 0;
  callout_state_ = MYSQL_API_NO_CALLOUT;

  if (api_hooks_.has_hooks()) {
    // It's also possible that the plugin_tunnel vc was never
    // executed due to not contacting the server
    if (NULL != plugin_tunnel_) {
      plugin_tunnel_->kill_no_connect();
    }
    plugin_tunnel_type_ = MYSQL_NO_PLUGIN_TUNNEL;
    plugin_tunnel_ = NULL;

    plugin_clients_cleanup();
    has_active_plugin_clients_ = false;

    if (NULL != response_transform_info_.vc_) {
      if (NULL == response_transform_info_.entry_) {
        LOG_DEBUG("response transform entry has been cleanup, no need clean again");
      } else {
        if (OB_SUCCESS != sm_->vc_table_.cleanup_entry(response_transform_info_.entry_)) {
          LOG_WARN("vc table failed to cleanup response transform entry",
                   K_(response_transform_info_.entry), K_(sm_->sm_id));
        }
      }
      response_transform_info_.vc_ = NULL;
      response_transform_info_.entry_ = NULL;
    } else {
      // It possible that a plugin added transform hook
      // but the hook never executed due to a client abort
      // In that case, we need to manually close all the
      // transforms to prevent memory leaks
      transform_cleanup(OB_MYSQL_RESPONSE_TRANSFORM_HOOK, response_transform_info_);
    }

    if (NULL != request_transform_info_.vc_) {
      if (NULL == request_transform_info_.entry_) {
        LOG_DEBUG("request transform entry has been cleanup, no need clean again");
      } else {
        if (OB_SUCCESS != sm_->vc_table_.cleanup_entry(request_transform_info_.entry_)) {
          LOG_WARN("vc table failed to cleanup request transform entry",
                   K_(request_transform_info_.entry), K_(sm_->sm_id));
        }
      }
      request_transform_info_.vc_ = NULL;
      request_transform_info_.entry_ = NULL;
    } else {
      // It possible that a plugin added transform hook
      // but the hook never executed due to a client abort
      // In that case, we need to manually close all the
      // transforms to prevent memory leaks
      transform_cleanup(OB_MYSQL_REQUEST_TRANSFORM_HOOK, request_transform_info_);
    }

    ObApiTransaction *transaction = ObApiUtilsInternal::get_api_transaction(sm_, false);
    if (NULL != transaction) {
      transaction->reset();
    }
    api_hooks_.destroy();
  }
}

void ObMysqlSMApi::destroy()
{
  ObApiTransaction *transaction = ObApiUtilsInternal::get_api_transaction(sm_, false);
  if (NULL != transaction) {
    transaction->destroy();
  }

  if (api_hooks_.has_hooks()) {
    api_hooks_.destroy();
  }
}

int ObMysqlSMApi::state_api_callout(int event, void *data)
{
  // enum and variable for figuring out what the next action is after
  // after we've finished the api state
  enum ObAfterApiReturn
  {
    API_RETURN_UNKNOWN = 0,
    API_RETURN_CONTINUE,
    API_RETURN_DEFERED_CLOSE,
    API_RETURN_DEFERED_SERVER_ERROR,
    API_RETURN_ERROR_JUMP,
    API_RETURN_SHUTDOWN,
    API_RETURN_INVALIDATE_ERROR
  };

  int ret = OB_SUCCESS;
  ObAfterApiReturn api_next = API_RETURN_UNKNOWN;
  bool is_return = false;

  if (EVENT_NONE != event) {
    STATE_ENTER(ObMysqlSMApi::state_api_callout, event);
  }

  switch (event) {
    case EVENT_INTERVAL:
      if (OB_UNLIKELY(sm_->pending_action_ != data)) {
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("invalid internal state, pending action isn't equal to data",
                  K_(sm_->pending_action), K(data), K_(sm_->sm_id), K(ret));
      } else {
        sm_->pending_action_ = NULL;
      }

      // fall through
    case EVENT_NONE:
    case MYSQL_API_CONTINUE:
      if (OB_SUCC(ret) && (cur_hook_id_ >= 0) && (cur_hook_id_ < OB_MYSQL_LAST_HOOK)) {
        if (NULL == cur_hook_) {
          if (0 == cur_hook_count_) {
            cur_hook_ = mysql_global_hooks->get(cur_hook_id_);
            ++cur_hook_count_;
          }
        }

        LOG_DEBUG("hooks", K(cur_hook_), K(cur_hook_id_));
        // even if client_session is NULL, cur_hooks must
        // be incremented otherwise cur_hooks is not set to 2 and
        // transaction hooks (stored in api_hooks object) are not called.
        if (NULL == cur_hook_) {
          if (1 == cur_hook_count_) {
            if (NULL != sm_->client_session_) {
              cur_hook_ = sm_->client_session_->ssn_hook_get(cur_hook_id_);
            }
            ++cur_hook_count_;
          }
        }

        if (NULL == cur_hook_) {
          if (2 == cur_hook_count_) {
            cur_hook_ = api_hooks_.get(cur_hook_id_);
            ++cur_hook_count_;
          }
        }

        if (NULL != cur_hook_) {
          if (MYSQL_API_NO_CALLOUT == callout_state_) {
            callout_state_ = MYSQL_API_IN_CALLOUT;
          }

          // We need to take a smart pointer to the mutex since
          // the plugin could release it's mutex while we're on
          // the callout
          ObPtr<ObProxyMutex> plugin_mutex;
          if (NULL != cur_hook_->cont_->mutex_) {
            plugin_mutex = cur_hook_->cont_->mutex_;
            MUTEX_TRY_LOCK(plugin_lock, cur_hook_->cont_->mutex_, sm_->mutex_->thread_holding_);

            if (!plugin_lock.is_locked()) {
              MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_api_callout);
              if (NULL != sm_->pending_action_) {
                ret = OB_INNER_STAT_ERROR;
                LOG_ERROR("pending action isn't NULL", K_(sm_->pending_action), K_(sm_->sm_id), K(ret));
              } else {
                sm_->pending_action_ = sm_->mutex_->thread_holding_->schedule_in(sm_, HRTIME_MSECONDS(1));
                is_return = true;
              }
            } else {
              LOG_DEBUG("calling plugin", K_(sm_->sm_id),
                       "hook_name", ObMysqlApiDebugNames::get_api_hook_name(cur_hook_id_),
                       K_(cur_hook));

              ObAPIHook *hook = cur_hook_;
              cur_hook_ = cur_hook_->next();
              hook->invoke(OB_EVENT_MYSQL_READ_REQUEST + cur_hook_id_, sm_);
              is_return = true;
            }
          } else {
            LOG_DEBUG("calling plugin", K_(sm_->sm_id),
                     "hook_name", ObMysqlApiDebugNames::get_api_hook_name(cur_hook_id_),
                     K_(cur_hook));

            ObAPIHook *hook = cur_hook_;
            cur_hook_ = cur_hook_->next();
            hook->invoke(OB_EVENT_MYSQL_READ_REQUEST + cur_hook_id_, sm_);
            is_return = true;
          }
          break;
        }
      }
      // Map the callout state into api_next
      switch (callout_state_) {
        case MYSQL_API_NO_CALLOUT:
        case MYSQL_API_IN_CALLOUT:
          api_next = API_RETURN_CONTINUE;
          break;

        case MYSQL_API_DEFERED_CLOSE:
          api_next = API_RETURN_DEFERED_CLOSE;
          break;

        case MYSQL_API_DEFERED_SERVER_ERROR:
          api_next = API_RETURN_DEFERED_SERVER_ERROR;
          break;

        default:
          ret = OB_INNER_STAT_ERROR;
          LOG_ERROR("Unknown api state type", K_(callout_state), K_(sm_->sm_id), K(ret));
          break;
      }
      break;

    case MYSQL_API_ERROR:
      if (MYSQL_API_DEFERED_CLOSE == callout_state_) {
        api_next = API_RETURN_DEFERED_CLOSE;
      } else if (cur_hook_id_ == OB_MYSQL_TXN_CLOSE_HOOK) {
        // If we are closing the state machine, we can't
        // jump to an error state so just continue
        api_next = API_RETURN_CONTINUE;
      } else if (sm_->trans_state_.api_mysql_sm_shutdown_) {
        sm_->trans_state_.api_mysql_sm_shutdown_ = false;
        sm_->clear_server_entry();
        sm_->terminate_sm_ = true;
        api_next = API_RETURN_SHUTDOWN;
      } else {
        api_next = API_RETURN_ERROR_JUMP;
      }
      break;

      // We may receive an event from the tunnel
      // if it took a long time to call the SEND_RESPONSE hook
    case MYSQL_TUNNEL_EVENT_DONE:
      state_common_wait_for_transform_read(
          response_transform_info_, &ObMysqlSM::tunnel_handler_response_transfered, event, data);
      is_return = true;
      break;

    default:
      sm_->terminate_sm_ = true;
      is_return = true;
      break;
  }

  if (!is_return) {
    // Now that we're completed with the api state and figured out what
    // to do next, do it
    callout_state_ = MYSQL_API_NO_CALLOUT;
    switch (api_next) {
      case API_RETURN_CONTINUE:
        sm_->handle_api_return();
        break;

      case API_RETURN_DEFERED_CLOSE:
        if (OB_UNLIKELY(ObMysqlTransact::SM_ACTION_API_SM_SHUTDOWN != sm_->trans_state_.api_next_action_)) {
          ret = OB_INNER_STAT_ERROR;
          LOG_ERROR("api next action must be SM_ACTION_API_SM_SHUTDOWN",
                    "api_next_action", ObMysqlTransact::get_action_name(sm_->trans_state_.api_next_action_),
                    K_(sm_->sm_id), K(ret));
        } else {
          sm_->callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SM_SHUTDOWN);
        }
        break;

      case API_RETURN_DEFERED_SERVER_ERROR:
        if (OB_UNLIKELY(ObMysqlTransact::SM_ACTION_API_SEND_REQUEST != sm_->trans_state_.api_next_action_)
            || OB_UNLIKELY(ObMysqlTransact::CONNECTION_ALIVE == sm_->trans_state_.current_.state_)) {
          ret = OB_INNER_STAT_ERROR;
          LOG_ERROR("api next action must be SM_ACTION_API_SEND_REQUEST "
                    "or current state should not be CONNECTION_ALIVE",
                    "state", ObMysqlTransact::get_server_state_name(sm_->trans_state_.current_.state_),
                    "api_next_action", ObMysqlTransact::get_action_name(sm_->trans_state_.api_next_action_),
                    K_(sm_->sm_id), K(ret));
        } else {
          sm_->call_transact_and_set_next_state(ObMysqlTransact::handle_response);
        }
        break;

      case API_RETURN_ERROR_JUMP:
        sm_->call_transact_and_set_next_state(ObMysqlTransact::handle_api_error_jump);
        break;

      case API_RETURN_SHUTDOWN:
        break;

      case API_RETURN_INVALIDATE_ERROR:
        break;

      case API_RETURN_UNKNOWN:
      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("Unknown api state type", K_(sm_->sm_id), K(ret));
        break;
    }
  }

  if (OB_FAIL(ret) && !is_return && API_RETURN_ERROR_JUMP != api_next) {
    sm_->call_transact_and_set_next_state(ObMysqlTransact::handle_api_error_jump);
  }

  return EVENT_DONE;
}

void ObMysqlSMApi::do_api_callout_internal()
{
  int ret = OB_SUCCESS;
  switch (sm_->trans_state_.api_next_action_) {
    case ObMysqlTransact::SM_ACTION_API_SM_START:
      cur_hook_id_ = OB_MYSQL_TXN_START_HOOK;
      break;

    case ObMysqlTransact::SM_ACTION_API_READ_REQUEST:
      cur_hook_id_ = OB_MYSQL_READ_REQUEST_HOOK;
      break;

    case ObMysqlTransact::SM_ACTION_API_OBSERVER_PL:
      cur_hook_id_ = OB_MYSQL_OBSERVER_PL_HOOK;
      break;

    case ObMysqlTransact::SM_ACTION_API_SEND_REQUEST:
      cur_hook_id_ = OB_MYSQL_SEND_REQUEST_HOOK;
      break;

    case ObMysqlTransact::SM_ACTION_API_READ_RESPONSE:
      cur_hook_id_ = OB_MYSQL_READ_RESPONSE_HOOK;
      break;

    case ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE:
      cur_hook_id_ = OB_MYSQL_SEND_RESPONSE_HOOK;
      break;

    case ObMysqlTransact::SM_ACTION_API_CMD_COMPLETE:
      cur_hook_id_ = OB_MYSQL_CMD_COMPLETE_HOOK;
      break;

    case ObMysqlTransact::SM_ACTION_API_SM_SHUTDOWN:
      if (MYSQL_API_IN_CALLOUT == callout_state_
          || MYSQL_API_DEFERED_SERVER_ERROR == callout_state_) {
        callout_state_ = MYSQL_API_DEFERED_CLOSE;
        break;
      } else {
        cur_hook_id_ = OB_MYSQL_TXN_CLOSE_HOOK;
      }
      break;

    default:
      cur_hook_id_ = OB_MYSQL_LAST_HOOK;
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("unexpected api next action",
                K_(sm_->trans_state_.api_next_action), K_(sm_->sm_id), K(ret));
      break;
  }

  if (MYSQL_API_DEFERED_CLOSE != callout_state_) {
    cur_hook_ = NULL;
    cur_hook_count_ = 0;
    if (OB_SUCC(ret)) {
      state_api_callout(0, NULL);
    } else {
      state_api_callout(MYSQL_API_ERROR, NULL);
    }
  }
}

ObVConnection *ObMysqlSMApi::do_response_transform_open()
{
  if (OB_UNLIKELY(NULL != response_transform_info_.vc_)) {
    LOG_ERROR("invalid internal state, response transform vc isn't NULL",
              K_(response_transform_info_.vc), K_(sm_->sm_id));
  } else {
    ObAPIHook *hooks = api_hooks_.get(OB_MYSQL_RESPONSE_TRANSFORM_HOOK);
    if (NULL != hooks) {
      response_transform_info_.vc_ = g_transform_processor.open(sm_, hooks);

      // Record the transform VC in our table
      response_transform_info_.entry_ = sm_->vc_table_.new_entry();
      response_transform_info_.entry_->vc_ = response_transform_info_.vc_;
      response_transform_info_.entry_->vc_type_ = MYSQL_TRANSFORM_VC;
    } else {
      response_transform_info_.vc_ = NULL;
    }
  }

  return response_transform_info_.vc_;
}

ObVConnection *ObMysqlSMApi::do_request_transform_open()
{
  if (OB_UNLIKELY(NULL != request_transform_info_.vc_)) {
    LOG_ERROR("invalid internal state, request transform vc isn't NULL",
              K_(request_transform_info_.vc), K_(sm_->sm_id));
  } else {
    ObAPIHook *hooks = api_hooks_.get(OB_MYSQL_REQUEST_TRANSFORM_HOOK);
    if (NULL != hooks) {
      request_transform_info_.vc_ = g_transform_processor.open(sm_, hooks);
      if (NULL != request_transform_info_.vc_) {
        // Record the transform VC in our table
        request_transform_info_.entry_ = sm_->vc_table_.new_entry();
        request_transform_info_.entry_->vc_ = request_transform_info_.vc_;
        request_transform_info_.entry_->vc_type_ = MYSQL_TRANSFORM_VC;
      }
    } else {
      request_transform_info_.vc_ = NULL;
    }
  }

  return request_transform_info_.vc_;
}

// We've done a successful transform open and issued a do_io_write
// to the transform. We are now ready for the transform to tell us
// it is now ready to be read from and it done modifying the server
// request
int ObMysqlSMApi::state_request_wait_for_transform_read(int event, void *data)
{
  STATE_ENTER(ObMysqlSMApi::state_request_wait_for_transform_read, event);

  int64_t size = *(reinterpret_cast<int64_t *>(data));

  switch (event) {
    case TRANSFORM_READ_READY:
      if (INT64_MAX != size && size >= 0) {
        // We got a content length so update our internal data
        sm_->trans_state_.trans_info_.transform_request_cl_ = size;
      } else {
        sm_->trans_state_.trans_info_.transform_request_cl_ = MYSQL_UNDEFINED_CL;
      }
      sm_->callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_REQUEST);
      break;
    default:
      state_common_wait_for_transform_read(
          request_transform_info_, &ObMysqlSM::tunnel_handler_request_transfered, event, data);
      break;
  }

  return VC_EVENT_NONE;
}

// We've done a successful transform open and issued a do_io_write
// to the transform. We are now ready for the transform to tell us
// it is now ready to be read from and it done modifying the client
// response
int ObMysqlSMApi::state_response_wait_for_transform_read(int event, void *data)
{
  STATE_ENTER(ObMysqlSMApi::state_response_wait_for_transform_read, event);

  int64_t size = *(reinterpret_cast<int64_t *>(data));

  switch (event) {
    case TRANSFORM_READ_READY:
      if (INT64_MAX != size && size >= 0) {
        // We got a content length so update our internal state
        sm_->trans_state_.trans_info_.transform_response_cl_ = size;
      } else {
        sm_->trans_state_.trans_info_.transform_response_cl_ = MYSQL_UNDEFINED_CL;
      }
      sm_->call_transact_and_set_next_state(ObMysqlTransact::handle_transform_ready);
      break;

    default:
      state_common_wait_for_transform_read(
          response_transform_info_, &ObMysqlSM::tunnel_handler_response_transfered, event, data);
      break;
  }

  return VC_EVENT_NONE;
}

// This function handles the overlapping cases between request and response
// transforms which prevents code duplication
int ObMysqlSMApi::state_common_wait_for_transform_read(
    ObMysqlTransformInfo &t_info, MysqlSMHandler tunnel_handler, int event, void *data)
{
  STATE_ENTER(ObMysqlSMApi::state_common_wait_for_transform_read, event);

  int ret = OB_SUCCESS;
  ObMysqlTunnelConsumer *c = NULL;

  switch (event) {
    case MYSQL_TUNNEL_EVENT_DONE:
      // There are three reasons why the the tunnel could signal completed
      //   1) there was error from the transform write
      //   2) there was an error from the data source
      //   3) the transform write completed before it sent
      //      TRANSFORM_READ_READY which is legal and in which
      //      case we should just wait for the transform read ready
      c = sm_->tunnel_.get_consumer(t_info.vc_);
      if (OB_ISNULL(c) || OB_UNLIKELY(c->vc_ != t_info.entry_->vc_)) {
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("consumer is NULL or the cosumer vc isn't the same as transform vc",
                  K(c), K_(t_info.entry), K_(sm_->sm_id), K(ret));
      } else {
        if (MYSQL_SM_TRANSFORM_FAIL == c->handler_state_) {
          // Case 1 we failed to complete the write to the
          // transform fall through to vc event error case
          if (c->write_success_) {
            ret = OB_INNER_STAT_ERROR;
            LOG_ERROR("consumer transform fail, consumer write success flag must be false",
                      K_(c->handler_state), K_(c->write_success), K_(sm_->sm_id), K(ret));
          }
        } else if (!c->producer_->read_success_) {
          // Case 2 - error from data source
          if (MT_MYSQL_CLIENT == c->producer_->vc_type_) {
            // Our source is the client. client transfer can't
            // be truncated so forward to the tunnel
            // handler to clean this mess up
            if (&t_info != &request_transform_info_) {
              ret = OB_INNER_STAT_ERROR;
              LOG_ERROR("invalid internal state, must be request tranform",
                        K(&t_info), K(&request_transform_info_), K_(sm_->sm_id), K(ret));
            } else {
              (sm_->*tunnel_handler)(event, data);
            }
            break;
          } else {
            // On the response side, we just forward as much
            // as we can of truncated documents so
            // just don't cache the result
            if (&t_info != &response_transform_info_) {
              ret = OB_INNER_STAT_ERROR;
              LOG_ERROR("invalid internal state, must be response tranform",
                        K(&t_info), K(&response_transform_info_), K_(sm_->sm_id), K(ret));
            }
            break;
          }
        } else {
          // Case 3 - wait for transform read ready
          break;
        }
      }

      // fall through
    case VC_EVENT_ERROR:
      // Transform VC sends NULL on error conditions
      if (NULL == c) {
        c = sm_->tunnel_.get_consumer(t_info.vc_);
        if (OB_ISNULL(c)) {
          ret = OB_INNER_STAT_ERROR;
          LOG_ERROR("consumer is NULL", K(c), K_(sm_->sm_id), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sm_->vc_table_.cleanup_entry(t_info.entry_))) {
          LOG_WARN("vc table failed to cleanup trnaform entry", K_(sm_->sm_id), K(ret));
        } else {
          t_info.entry_ = NULL;
          // In Case 1: error due to transform write,
          // we need to keep the original t_info.vc_ for transform_cleanup()
          // to skip do_io_close(); otherwise, set it to NULL.
          if (MYSQL_SM_TRANSFORM_FAIL != c->handler_state_) {
            t_info.vc_ = NULL;
          }
          // consume all data in internal reader, make sure will disconnect directly
          sm_->consume_all_internal_data();
          sm_->call_transact_and_set_next_state(ObMysqlTransact::handle_api_error_jump);
        }
      }
      break;

    default:
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("Unknown event", K(event), K_(sm_->sm_id), K(ret));
      break;
  }

  if (OB_FAIL(ret)) {
    sm_->call_transact_and_set_next_state(ObMysqlTransact::handle_api_error_jump);
  }

  return VC_EVENT_NONE;
}

int ObMysqlSMApi::tunnel_handler_transform_write(int event, ObMysqlTunnelConsumer &c)
{
  STATE_ENTER(ObMysqlSMApi::tunnel_handler_transform_write, event);

  int ret = OB_SUCCESS;
  ObMysqlTransformInfo *transform = NULL;

  // Figure out if this the request or response transform
  // : use request_transform_info_.entry_ because
  // request_transform_info_.vc_ is not set to NULL after the request
  // transform is done.
  if (NULL != request_transform_info_.entry_) {
    transform = &request_transform_info_;
    if (c.vc_ != transform->entry_->vc_) {
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("consumer vc must be the same transform vc",
                K_(c.vc), K_(transform->entry_->vc), K_(sm_->sm_id), K(ret));
    }
  } else {
    transform = &response_transform_info_;
    if (c.vc_ != transform->vc_ || c.vc_ != transform->entry_->vc_) {
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("consumer vc must be the same transform vc",
                K_(c.vc), K_(transform->vc), K_(transform->entry_->vc), K_(sm_->sm_id), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    switch (event) {
      case VC_EVENT_ERROR:
        // Transform error
        sm_->tunnel_.chain_abort_all(*c.producer_);
        // if we do not clear server entry and client entry here, for it's vc has already
        // close by chain_abort_all, and will close again in ObMysqlSM::setup_error_transfer,
        // evently will core dump;
        sm_->clear_entries();
        // consume all data, make sure will disconnect directly;
        sm_->consume_all_internal_data();
        c.handler_state_ = MYSQL_SM_TRANSFORM_FAIL;
        c.vc_->do_io_close(EMYSQL_ERROR);
        break;

      case VC_EVENT_EOS:
        // It possible the transform quit
        // before the producer finished. If this is true we
        // need shut down the producer if it doesn't have
        // other consumers to serve or else it will fill up
        // buffer and get hung
        if (c.producer_->alive_ && 1 == c.producer_->num_consumers_) {
          // Send a tunnel detach event to the producer
          // to shut it down but indicates it should not abort
          // downstream (on the other side of the transform)
          // cache writes
          sm_->tunnel_.producer_handler(MYSQL_TUNNEL_EVENT_CONSUMER_DETACH, *c.producer_);
        }

        // fall through
      case VC_EVENT_WRITE_COMPLETE:
        // write to transform complete - shutdown the write side
        c.write_success_ = true;
        c.vc_->do_io_shutdown(IO_SHUTDOWN_WRITE);

        // If the read side has not started up yet, then the
        // this transform_vc is no longer owned by the tunnel
        if (NULL == c.self_producer_) {
          transform->entry_->in_tunnel_ = false;
        } else if (!c.self_producer_->alive_) {
          // The read side of the Transform
          // has already completed (possible when the
          // transform intentionally truncates the response).
          // So close it
          c.vc_->do_io_close();
        }
        break;

      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("Unknown event", K(event), K_(sm_->sm_id), K(ret));
        break;
    }
  }

  if (ObMysqlTransact::SOURCE_OBSERVER == sm_->trans_state_.pre_transform_source_) {
    sm_->cmd_size_stats_.server_response_bytes_ = sm_->cmd_size_stats_.client_response_bytes_;
  }

  if (OB_FAIL(ret)) {
    sm_->trans_state_.inner_errcode_ = ret;
    sm_->trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
  }

  return ret;
}

int ObMysqlSMApi::tunnel_handler_transform_read(int event, ObMysqlTunnelProducer &p)
{
  STATE_ENTER(ObMysqlSMApi::tunnel_handler_transform_read, event);

  int ret = OB_SUCCESS;
  if (p.vc_ != response_transform_info_.vc_ && p.vc_ != request_transform_info_.vc_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state, produce vc must be response transform vc or request transform vc",
              K_(p.vc), K_(response_transform_info_.vc), K_(request_transform_info_.vc), K_(sm_->sm_id), K(ret));
  } else {
    switch (event) {
      case VC_EVENT_ERROR:
        // Transform error
        sm_->tunnel_.chain_abort_all(*p.self_consumer_->producer_);
        // if we do not clear server entry and client entry here, for it's vc has already
        // close by chain_abort_all, and will close again in ObMysqlSM::setup_error_transfer,
        // evently will core dump;
        sm_->clear_entries();
        // consume all data, make sure will disconnect directly;
        sm_->consume_all_internal_data();
        break;

      case VC_EVENT_EOS:
        // fall through
      case VC_EVENT_READ_COMPLETE:
      case MYSQL_TUNNEL_EVENT_CMD_COMPLETE:
      case MYSQL_TUNNEL_EVENT_PRECOMPLETE:
        // Transform complete
        p.read_success_ = true;
        sm_->tunnel_.local_finish_all(p);
        break;

      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("Unknown event", K(event), K_(sm_->sm_id), K(ret));
        break;
    }
  }

  // it's possible that the write side of the
  // transform hasn't detached yet. If it is still alive,
  // don't close the transform vc
  if (!p.self_consumer_->alive_) {
    p.vc_->do_io_close();
  }
  p.handler_state_ = MYSQL_SM_TRANSFORM_CLOSED;

  if (OB_FAIL(ret)) {
    sm_->trans_state_.inner_errcode_ = ret;
    sm_->trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
  }

  return ret;
}

int ObMysqlSMApi::tunnel_handler_plugin_client(int event, ObMysqlTunnelConsumer &c)
{
  STATE_ENTER(ObMysqlSMApi::tunnel_handler_plugin_client, event);
  int ret = OB_SUCCESS;

  switch (event) {
    case VC_EVENT_ERROR:
      c.vc_->do_io_close(EMYSQL_ERROR); // close up
      // Signal producer if we're the last consumer.
      if (c.producer_->alive_ && 1 == c.producer_->num_consumers_) {
        sm_->tunnel_.producer_handler(MYSQL_TUNNEL_EVENT_CONSUMER_DETACH, *c.producer_);
      }
      break;

    case VC_EVENT_EOS:
      if (c.producer_->alive_ && 1 == c.producer_->num_consumers_) {
        sm_->tunnel_.producer_handler(MYSQL_TUNNEL_EVENT_CONSUMER_DETACH, *c.producer_);
      }

      // fall through
    case VC_EVENT_WRITE_COMPLETE:
      c.write_success_ = true;
      c.vc_->do_io_close();
      break;

    default:
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("Unknown event", K(event), K_(sm_->sm_id), K(ret));
      break;
  }

  return ret;
}

int ObMysqlSMApi::setup_transform_to_server_transfer()
{
  int ret = OB_SUCCESS;
  ObMIOBuffer *post_buffer = NULL;
  ObIOBufferReader *buf_start = NULL;
  ObMysqlTunnelProducer *p = NULL;
  ObMysqlTunnelConsumer *c = NULL;

  if (OB_ISNULL(request_transform_info_.entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid internal state, request transform entry is NULL",
             K_(request_transform_info_.entry), K_(sm_->sm_id), K(ret));
  } else if (OB_UNLIKELY(request_transform_info_.entry_->vc_ != request_transform_info_.vc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid internal state, request transform entry vc is different with request transform vc",
             K_(request_transform_info_.entry_->vc),
             K_(request_transform_info_.vc), K_(sm_->sm_id), K(ret));
  } else if (OB_ISNULL(post_buffer = new_empty_miobuffer(MYSQL_BUFFER_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to new io buffer", K_(sm_->sm_id), K(ret));
  } else if (OB_ISNULL(buf_start = post_buffer->alloc_reader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to allocate buffer reader", K_(sm_->sm_id), K(ret));
  } else {
    MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::tunnel_handler_request_transfered);

    if (OB_ISNULL(c = sm_->tunnel_.get_consumer(request_transform_info_.vc_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get consumer", K_(request_transform_info_.vc), K_(sm_->sm_id), K(ret));
    } else if (OB_ISNULL(p = sm_->tunnel_.add_producer(request_transform_info_.vc_,
                                                       sm_->trans_state_.trans_info_.transform_request_cl_,
                                                       buf_start,
                                                       &ObMysqlSM::tunnel_handler_transform_read,
                                                       MT_TRANSFORM,
                                                       "transform read"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add producer", K_(sm_->sm_id), K(ret));
    } else {
      sm_->tunnel_.chain(*c, *p);
      request_transform_info_.entry_->in_tunnel_ = true;

      if (OB_ISNULL(c = sm_->tunnel_.add_consumer(sm_->server_entry_->vc_,
                                                  request_transform_info_.vc_,
                                                  &ObMysqlSM::tunnel_handler_request_transfer_server,
                                                  MT_MYSQL_SERVER,
                                                  "observer"))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add consumer", K_(sm_->sm_id), K(ret));
      } else {
        sm_->server_entry_->in_tunnel_ = true;
        if (OB_FAIL(p->set_request_packet_analyzer(MYSQL_REQUEST, NULL))) {
          LOG_WARN("failed to set_producer_packet_analyzer", K(p), K_(sm_->sm_id), K(ret));
        } else if (OB_FAIL(sm_->tunnel_.tunnel_run(p))) {
          LOG_WARN("failed to run tunnel", K(p), K_(sm_->sm_id), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObMysqlSMApi::setup_server_transfer_to_transform()
{
  int ret = OB_SUCCESS;
  int64_t nbytes = 0;
  ObMysqlTunnelProducer *p = NULL;
  ObMysqlTunnelConsumer *c = NULL;

  if (OB_ISNULL(response_transform_info_.entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid internal state, reponse transform entry is NULL",
             K_(response_transform_info_.entry), K_(sm_->sm_id), K(ret));
  } else if (OB_UNLIKELY(response_transform_info_.entry_->vc_ != response_transform_info_.vc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid internal state, response transform entry vc is different with response transform vc",
             K_(response_transform_info_.entry_->vc),
             K_(response_transform_info_.vc), K_(sm_->sm_id), K(ret));
  } else if (OB_FAIL(sm_->trans_state_.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
    LOG_ERROR("fail to allocate internal buffer,", K_(sm_->sm_id), K(ret));
  } else if (OB_FAIL(sm_->server_transfer_init(sm_->trans_state_.internal_buffer_, nbytes))) {
    LOG_WARN("failed to init server transfer", K_(sm_->sm_id), K(ret));
  } else if (OB_ISNULL(p = sm_->tunnel_.add_producer(sm_->server_entry_->vc_,
                                                     nbytes,
                                                     sm_->trans_state_.internal_reader_,
                                                     &ObMysqlSM::tunnel_handler_server,
                                                     MT_MYSQL_SERVER,
                                                     "observer", false))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to add producer", K(p), K_(sm_->sm_id), K (ret));
  } else if (OB_ISNULL(c = sm_->tunnel_.add_consumer(response_transform_info_.vc_,
                                                     sm_->server_entry_->vc_,
                                                     &ObMysqlSM::tunnel_handler_transform_write,
                                                     MT_TRANSFORM,
                                                     "transform write"))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to add consumer", K(c), K_(sm_->sm_id), K(ret));
  } else {
    MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_response_wait_for_transform_read);

    sm_->server_entry_->in_tunnel_ = true;
    response_transform_info_.entry_->in_tunnel_ = true;

    ObMysqlResp *server_response = &sm_->trans_state_.trans_info_.server_response_;
    ObIMysqlRespAnalyzer *analyzer = NULL;
    ObProxyProtocol ob_proxy_protocol = sm_->use_compression_protocol();
    if (PROTOCOL_CHECKSUM == ob_proxy_protocol || PROTOCOL_OB20 == ob_proxy_protocol) {
      const uint8_t req_seq = sm_->get_request_seq();
      const obmysql::ObMySQLCmd cmd = sm_->get_request_cmd();
      ObMysqlCompressAnalyzer *compress_analyzer = &sm_->get_compress_analyzer();
      const ObMysqlProtocolMode mysql_mode = sm_->client_session_->get_session_info().is_oracle_mode() ? OCEANBASE_ORACLE_PROTOCOL_MODE : OCEANBASE_MYSQL_PROTOCOL_MODE;
      compress_analyzer->reset();
      const bool enable_extra_ok_packet_for_stats = sm_->is_extra_ok_packet_for_stats_enabled();
      if (OB_FAIL(compress_analyzer->init(req_seq, ObMysqlCompressAnalyzer::SIMPLE_MODE,
              cmd, mysql_mode, enable_extra_ok_packet_for_stats, req_seq, sm_->get_server_session()->get_server_request_id(),
              sm_->get_server_session()->get_server_sessid()))) {
        LOG_WARN("fail to init compress analyzer", K(req_seq), K(cmd),
                 K(enable_extra_ok_packet_for_stats), K(ret));
      }
      analyzer = compress_analyzer;
    } else {
      //sm_->analyzer_.reset();
      analyzer = &sm_->analyzer_;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(p->set_response_packet_analyzer(0, MYSQL_RESPONSE, analyzer, server_response))) {
        LOG_WARN("failed to set_producer_packet_analyzer", K(p), K_(sm_->sm_id), K(ret));
      } else if (OB_FAIL(sm_->tunnel_.tunnel_run(p))) {
        LOG_WARN("failed to run tunnel", K(p), K_(sm_->sm_id), K(ret));
      }
    }
  }

  return ret;
}

int ObMysqlSMApi::setup_transfer_from_transform()
{
  int ret = OB_SUCCESS;
  ObMIOBuffer *buf = NULL;
  ObIOBufferReader *buf_start = NULL;
  ObMysqlTunnelProducer *p = NULL;
  ObMysqlTunnelConsumer *c = NULL;

  if (OB_ISNULL(response_transform_info_.entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid internal state, reponse transform entry is NULL",
             K_(response_transform_info_.entry), K_(sm_->sm_id), K(ret));
  } else if (OB_UNLIKELY(response_transform_info_.entry_->vc_ != response_transform_info_.vc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid internal state, response transform entry vc is different with response transform vc",
             K_(response_transform_info_.entry_->vc),
             K_(response_transform_info_.vc), K_(sm_->sm_id), K(ret));
  } else if (OB_ISNULL(buf = new_empty_miobuffer(MYSQL_BUFFER_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to new io buffer", K_(sm_->sm_id), K(ret));
  } else if (OB_ISNULL(buf_start = buf->alloc_reader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to allocate buffer reader", K_(sm_->sm_id), K(ret));
  } else {
    MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::tunnel_handler_response_transfered);
    buf->water_mark_ = sm_->trans_state_.mysql_config_params_->default_buffer_water_mark_;
    if (OB_ISNULL(c = sm_->tunnel_.get_consumer(response_transform_info_.vc_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get consumer", K_(response_transform_info_.vc), K_(sm_->sm_id), K(ret));
    } else if (c->vc_ != response_transform_info_.vc_ || MT_TRANSFORM != c->vc_type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vc is different, unecpected vc type",
               K_(c->vc), K_(response_transform_info_.vc), K_(c->vc_type),
               "expected_vc_type", "MT_TRANSFORM", K_(sm_->sm_id), K(ret));
    } else if (OB_ISNULL(p = sm_->tunnel_.add_producer(response_transform_info_.vc_,
                                                       INT64_MAX,
                                                       buf_start,
                                                       &ObMysqlSM::tunnel_handler_transform_read,
                                                       MT_TRANSFORM,
                                                       "transform read"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add producer", K(p), K_(sm_->sm_id), K(ret));
    } else {
      sm_->tunnel_.chain(*c, *p);
      if (OB_ISNULL(c = sm_->tunnel_.add_consumer(sm_->client_entry_->vc_,
                                                  response_transform_info_.vc_,
                                                  &ObMysqlSM::tunnel_handler_client,
                                                  MT_MYSQL_CLIENT,
                                                  "client"))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to add consumer", K(c), K_(sm_->sm_id), K(ret));
      } else {
        response_transform_info_.entry_->in_tunnel_ = true;
        sm_->client_entry_->in_tunnel_ = true;
        if (OB_FAIL(setup_plugin_clients(*p))) {
          LOG_WARN("failed to setup_plugin_clients", K(p), K_(sm_->sm_id), K(ret));
        } else if (OB_FAIL(p->set_response_packet_analyzer(0, MYSQL_RESPONSE, &sm_->analyzer_, NULL))) {
          LOG_WARN("failed to set_producer_packet_analyzer", K(p), K_(sm_->sm_id), K(ret));
        } else if (OB_FAIL(sm_->tunnel_.tunnel_run(p))) {
          LOG_WARN("failed to run tunnel", K(p), K_(sm_->sm_id), K(ret));
        }
      }
    }
  }

  return ret;
}


inline int ObMysqlSMApi::setup_plugin_clients(ObMysqlTunnelProducer &p)
{
  int ret = OB_SUCCESS;
  ObVConnInternal *contp = NULL;
  ObAPIHook *client = txn_hook_get(OB_MYSQL_RESPONSE_CLIENT_HOOK);

  has_active_plugin_clients_ = (NULL != client);
  while (NULL != client && OB_SUCC(ret)) {
    contp = static_cast<ObVConnInternal*>(client->cont_);

    if (OB_ISNULL(sm_->tunnel_.add_consumer(contp,
                                            p.vc_,
                                            &ObMysqlSM::tunnel_handler_plugin_client,
                                            MT_MYSQL_CLIENT,
                                            "plugin client"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add consumer", K_(sm_->sm_id), K(ret));
    } else {
      // We don't put these in the SM VC table because the tunnel
      // will clean them up in do_io_close().
      client = client->next();
    }
  }
  return ret;
}

void ObMysqlSMApi::plugin_clients_cleanup() {
  // If this is set then all of the plugin client VCs were put in
  // the VC table and cleaned up there. This handles the case where
  // something went wrong early.
  if (has_active_plugin_clients_) {
    ObAPIHook *client = txn_hook_get(OB_MYSQL_RESPONSE_CLIENT_HOOK);
    ObVConnInternal *contp = NULL;

    while (NULL != client) {
      contp = static_cast<ObVConnInternal *>(client->cont_);
      if (NULL != contp) {
        contp->do_io_close();
      }
      client = client->next();
    }
  }
}

void ObMysqlSMApi::set_mysql_schedule(ObContinuation *cont) {
  MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::get_mysql_schedule);
  schedule_cont_ = cont;
}

int ObMysqlSMApi::get_mysql_schedule(int event, void *data) {
  UNUSED(data);

  ObPtr<ObProxyMutex> plugin_mutex;
  if (NULL != schedule_cont_->mutex_) {
    plugin_mutex = schedule_cont_->mutex_;
    MUTEX_TRY_LOCK(plugin_lock, schedule_cont_->mutex_, sm_->mutex_->thread_holding_);

    if (!plugin_lock.is_locked()) {
      MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::get_mysql_schedule);
      if (NULL != sm_->pending_action_) {
        LOG_ERROR("pending action isn't NULL", K_(sm_->pending_action), K_(sm_->sm_id));
      }
      sm_->pending_action_ = sm_->mutex_->thread_holding_->schedule_in(sm_, HRTIME_MSECONDS(1));
    } else {
      schedule_cont_->handle_event(event, sm_);
    }
  } else {
    schedule_cont_->handle_event(event, sm_);
  }
  return EVENT_NONE;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
