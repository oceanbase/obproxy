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
#include "obutils/ob_proxy_conn_num_check_cont.h"
#include "lib/ob_errno.h"
#include "lib/net/ob_addr.h"
#include "lib/time/ob_hrtime.h"
#include "obutils/ob_config_server_processor.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/client/ob_client_vc.h"
#include "proxy/mysql/ob_mysql_transact.h"
#include "proxy/mysql/ob_mysql_server_session.h"
#include "obutils/ob_proxy_create_server_conn_cont.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"
#include "obutils/ob_session_pool_processor.h"



using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
ObProxyConnNumCheckCont::ObProxyConnNumCheckCont(event::ObProxyMutex *m, int64_t interval_us)
  : obutils::ObAsyncCommonTask(m, "proxy_conn_num_check_task"),
    reentrancy_count_(0), refresh_interval_us_(interval_us)
{
  SET_HANDLER(&ObProxyConnNumCheckCont::main_handler);
}

ObProxyConnNumCheckCont::~ObProxyConnNumCheckCont()
{
}

const char *ObProxyConnNumCheckCont::get_event_name(const int64_t event)
{
  const char *name = "UNKNOWN_EVENT";
  switch (event) {
    case CONN_NUM_CHECK_ENTRY_START_EVENT: {
      name = "CONN_NUM_CHECK_ENTRY_START_EVENT";
      break;
    }
    case CONN_NUM_CHECK_ENTRY_DESTROY_EVENT: {
      name = "CONN_NUM_CHECK_ENTRY_DESTROY_EVENT";
      break;
    }
    default: {
      break;
    }
  }
  return name;
}

int ObProxyConnNumCheckCont::main_handler(int event, void *data)
{
  int ret_event = EVENT_CONT;
  int ret = OB_SUCCESS;
  ++reentrancy_count_;
  switch (event) {
  case CONN_NUM_CHECK_ENTRY_START_EVENT: {
    pending_action_ = NULL;
    if (OB_FAIL(handle_conn_num_check())) {
      LOG_WARN("fail to handle_conn_num_check", K(ret));
    }
    break;
  }
  case CONN_NUM_CHECK_ENTRY_DESTROY_EVENT: {
    pending_action_ = NULL;
    if (OB_FAIL(handle_detroy_self())) {
      LOG_WARN("fail to handle_detroy_self", K(ret));
    } else {
      terminate_ = true;
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown event", K(event), K(data), K(ret));
    break;
  }
  };
  if (!terminate_) {
    if (OB_FAIL(cancel_pending_action())) {
    LOG_WARN("fail to cancel_pending_action", K(schema_key_.dbkey_), K(ret));
    } else if (OB_FAIL(schedule_check_conn_num_cont())) {
      LOG_WARN("fail to schedule_check_conn_num_cont", K(schema_key_.dbkey_), K(ret));
    }
  }
  if (terminate_ && (1 == reentrancy_count_)) {
    ret_event = EVENT_DONE;
    destroy();
  } else {
    --reentrancy_count_;
    if (OB_UNLIKELY(reentrancy_count_ < 0)) {
      LOG_ERROR("invalid reentrancy_count", K_(reentrancy_count));
    }
  }
  return ret_event;
}
int ObProxyConnNumCheckCont::handle_one_schema_key_num_check()
{
  int ret = OB_SUCCESS;
  ObMysqlSchemaServerAddrInfo* schema_server_addr_info =
    get_global_session_manager().acquire_scheme_server_addr_info(schema_key_);
  if (OB_ISNULL(schema_server_addr_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not null here", K(schema_key_.dbkey_));
  } else if (schema_key_.logic_tenant_name_.config_string_.empty() ||
    schema_key_.logic_tenant_name_.config_string_.compare(DEFAULT_LOGIC_TENANT_NAME) == 0) {
    LOG_DEBUG("not logic tenant", K(schema_key_));
  } else {
    // check conn num ,if less than min will create conn
    int64_t min_count = ObMysqlSessionUtils::get_session_min_conn(schema_key_);
    int64_t max_count = ObMysqlSessionUtils::get_session_max_conn(schema_key_);
    ObMysqlSchemaServerAddrInfo::ServerAddrHashTable::iterator last =
      schema_server_addr_info->server_addr_map_.end();
    for (ObMysqlSchemaServerAddrInfo::ServerAddrHashTable::iterator spot =
           schema_server_addr_info->server_addr_map_.begin(); spot != last; ++spot) {
      ObServerAddrInfo* addr_info = &(*spot);
      if (OB_ISNULL(addr_info)) {
        LOG_WARN("addr info is null, invalid", K(schema_key_.dbkey_));
        continue;
      }
      if (addr_info->reach_max_fail_count()) {
        LOG_WARN("reach_max_fail count, will remove it now", K(addr_info->addr_));
        schema_server_addr_info->remove_server_addr_if_exist(addr_info->addr_);
        continue;
      }
      ObCommonAddr& common_addr = addr_info->addr_;
      int64_t cur_count = get_global_session_manager().get_current_session_conn_count(
                          schema_key_.dbkey_.config_string_,
                          common_addr);
      if (cur_count < min_count) {
        // less than min shoud create conn, only shard need create
        if (TYPE_SHARD_CONNECTOR == schema_key_.get_connector_type()) {
          SchemaKeyConnInfo* schema_key_info = op_alloc(SchemaKeyConnInfo);
          if (OB_ISNULL(schema_key_info)) {
            LOG_WARN("alloc SchemaKeyConnInfo failed", K(schema_key_.dbkey_));
            continue;
          }
          schema_key_info->schema_key_ = schema_key_;
          schema_key_info->addr_ = addr_info->addr_;
          schema_key_info->conn_count_ = min_count - cur_count;
          get_global_server_conn_job_list().push(schema_key_info);
          int32_t count = get_global_server_conn_job_list().count();
          LOG_DEBUG("add a server conn job", K(schema_key_), K(schema_key_info), KPC(schema_key_info),
            K(cur_count), K(min_count), K(common_addr), K(count));
        } else {
          LOG_DEBUG("not shard, do nothing", K(schema_key_));
        }
      } else if (cur_count > max_count) {
        // more than max should close extra conn
        LOG_DEBUG("will do_close_extra_session_conn", K(schema_key_.dbkey_), K(common_addr),
          K(common_addr), K(cur_count - max_count));
        get_global_session_manager().do_close_extra_session_conn(schema_key_, common_addr, cur_count - max_count);
      }
    }
  }
  if (NULL != schema_server_addr_info) {
    schema_server_addr_info->dec_ref();//free
    schema_server_addr_info = NULL;
  }
  return ret;
}
int ObProxyConnNumCheckCont::handle_conn_num_check()
{
  int ret = OB_SUCCESS;
  int count = 0;
  ObHRTime start_time_us = hrtime_to_usec(get_hrtime_internal());
  ObSEArray<ObMysqlServerSessionListPool*, 1024> all_session_list_pool_array;
  get_global_session_manager().get_all_session_list_pool(all_session_list_pool_array);
  for (int64_t i = 0; i < all_session_list_pool_array.count(); i++) {
    ObMysqlServerSessionListPool* session_list_pool = all_session_list_pool_array.at(i);
    schema_key_ = session_list_pool->schema_key_;
    // only shard_connection need num_check
    if (TYPE_SHARD_CONNECTOR == schema_key_.get_connector_type()) {
      handle_one_schema_key_num_check();
      ++count;
    }
    session_list_pool->dec_ref();
    session_list_pool = NULL;
  }
  ObHRTime end_time_us = hrtime_to_usec(get_hrtime_internal());
  ObHRTime cost_time_us = hrtime_to_usec(hrtime_diff(end_time_us, start_time_us));
  LOG_DEBUG("End handle_conn_num_check total handle", K(count), K(cost_time_us));
  return ret;
}

int ObProxyConnNumCheckCont::schedule_check_conn_num_cont(bool imm)
{
  int ret = OB_SUCCESS;
  if (get_global_hot_upgrade_info().is_graceful_exit_timeout(get_hrtime())) {
    ret = OB_SERVER_IS_STOPPING;
    terminate_ = true;
    LOG_WARN("proxy need exit now", K(ret));
  } else if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pending action should be null here", K_(pending_action), K(ret));
  } else {
    int64_t delay_us = 0;
    if (imm) {
      // must be done in work thread
      if (OB_ISNULL(g_event_processor.schedule_imm(this, ET_CALL, CONN_NUM_CHECK_ENTRY_START_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to schedule_check_conn_num event", K(ret));
      }
    } else {
      delay_us = ObRandomNumUtils::get_random_half_to_full(refresh_interval_us_);
      if (OB_UNLIKELY(delay_us <= 0)) {
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("delay must greater than zero", K(delay_us), K(ret));
      } else if (OB_ISNULL(pending_action_ = self_ethread().schedule_in(
          this, HRTIME_USECONDS(delay_us), CONN_NUM_CHECK_ENTRY_START_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to schedule numcheck cont", K(delay_us), K(ret));
      }
    }
  }
  return ret;
}
int ObProxyConnNumCheckCont::stop_check_conn_num()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("stop check conn num now");
  terminate_ = true;
  return ret;
}

int ObProxyConnNumCheckCont::handle_detroy_self()
{
  int ret = OB_SUCCESS;
  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase