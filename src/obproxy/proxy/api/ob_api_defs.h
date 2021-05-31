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
 * Developers, when adding a new element to an enum, append it. DO NOT
 * insert it. Otherwise, binary compatibility of plugins will be broken!
 */

#ifndef OBPROXY_API_DEFS_H
#define OBPROXY_API_DEFS_H

#include <stdint.h>
#include <sys/types.h>
#include <sys/socket.h>
#include "lib/time/ob_hrtime.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define OB_MAX_USER_NAME_LEN 256
#define OB_MAX_API_STATS 256

/**
 * The following struct is used by api_plugin_register(). It stores
 * registration information about the plugin.
 */
struct ObPluginRegistrationInfo
{
  char *plugin_name_;
  char *vendor_name_;
  char *support_email_;
};

/**
 * This set of enums represents the possible hooks where you can
 * set up continuation callbacks. The functions used to register a
 * continuation for a particular hook are:
 *
 * mysql_hook_add: adds a global hook. You can globally add
 * any hook except for
 *  - OB_MYSQL_REQUEST_TRANSFORM_HOOK
 *  - OB_MYSQL_RESPONSE_TRANSFORM_HOOK
 *  - OB_MYSQL_RESPONSE_CLIENT_HOOK
 *
 * The following hooks can ONLY be added globally:
 *  - OB_MYSQL_SSN_START_HOOK
 *  - OB_MYSQL_SSN_CLOSE_HOOK
 *
 * mysql_ssn_hook_add: adds a transaction hook to each transaction
 * within a session. You can only use transaction hooks with this call:
 *  - OB_MYSQL_READ_REQUEST_HOOK
 *  - OB_MYSQL_OBSERVER_PL_HOOK
 *  - OB_MYSQL_SEND_REQUEST_HOOK
 *  - OB_MYSQL_READ_RESPONSE_HOOK
 *  - OB_MYSQL_SEND_RESPONSE_HOOK
 *  - OB_MYSQL_REQUEST_TRANSFORM_HOOK
 *  - OB_MYSQL_RESPONSE_TRANSFORM_HOOK
 *  - OB_MYSQL_RESPONSE_CLIENT_HOOK
 *  - OB_MYSQL_TXN_START_HOOK
 *  - OB_MYSQL_TXN_CLOSE_HOOK
 *
 * mysql_txn_hook_add: adds a callback at a specific point within
 * an Mysql transaction. The following hooks can be used with this
 * function:
 *  - OB_MYSQL_READ_REQUEST_HOOK
 *  - OB_MYSQL_OBSERVER_PL_HOOK
 *  - OB_MYSQL_SEND_REQUEST_HOOK
 *  - OB_MYSQL_READ_RESPONSE_HOOK
 *  - OB_MYSQL_SEND_RESPONSE_HOOK
 *  - OB_MYSQL_REQUEST_TRANSFORM_HOOK
 *  - OB_MYSQL_RESPONSE_TRANSFORM_HOOK
 *  - OB_MYSQL_TXN_CLOSE_HOOK
 *
 * The two transform hooks can ONLY be added as transaction hooks.
 *
 * OB_MYSQL_LAST_HOOK _must_ be the last element. Only right place
 * to insert a new element is just before OB_MYSQL_LAST_HOOK.
 */
enum ObMysqlHookID
{
  OB_MYSQL_READ_REQUEST_HOOK = 0,
  OB_MYSQL_OBSERVER_PL_HOOK,
  OB_MYSQL_SEND_REQUEST_HOOK,
  OB_MYSQL_READ_RESPONSE_HOOK,
  OB_MYSQL_SEND_RESPONSE_HOOK,
  OB_MYSQL_REQUEST_TRANSFORM_HOOK,
  OB_MYSQL_RESPONSE_TRANSFORM_HOOK,
  OB_MYSQL_TXN_START_HOOK,
  OB_MYSQL_TXN_CLOSE_HOOK,
  OB_MYSQL_SSN_START_HOOK,
  OB_MYSQL_SSN_CLOSE_HOOK,
  OB_MYSQL_RESPONSE_CLIENT_HOOK,
  OB_MYSQL_CMD_COMPLETE_HOOK,
  OB_MYSQL_LAST_HOOK
};

/**
 * ObEvents are sent to continuations when they are called back.
 * The ObEvent provides the continuation's handler function with
 * information about the callback. Based on the event it receives,
 * the handler function can decide what to do.
 */
enum ObEventType
{
  OB_EVENT_NONE = 0,
  OB_EVENT_IMMEDIATE = 1,
  OB_EVENT_TIMEOUT = 2,
  OB_EVENT_ERROR = 3,
  OB_EVENT_CONTINUE = 4,

  OB_EVENT_VCONN_READ_READY = 100,
  OB_EVENT_VCONN_WRITE_READY = 101,
  OB_EVENT_VCONN_READ_COMPLETE = 102,
  OB_EVENT_VCONN_WRITE_COMPLETE = 103,
  OB_EVENT_VCONN_EOS = 104,
  OB_EVENT_VCONN_INACTIVITY_TIMEOUT = 105,

  OB_EVENT_NET_CONNECT = 200,
  OB_EVENT_NET_CONNECT_FAILED = 201,
  OB_EVENT_NET_ACCEPT = 202,
  OB_EVENT_NET_ACCEPT_FAILED = 204,

  // EVENTS 206 - 212 for internal use
  OB_EVENT_INTERNAL_206 = 206,
  OB_EVENT_INTERNAL_207 = 207,
  OB_EVENT_INTERNAL_208 = 208,
  OB_EVENT_INTERNAL_209 = 209,
  OB_EVENT_INTERNAL_210 = 210,
  OB_EVENT_INTERNAL_211 = 211,
  OB_EVENT_INTERNAL_212 = 212,

  OB_EVENT_MYSQL_CONTINUE = 60000,
  OB_EVENT_MYSQL_ERROR = 60001,
  OB_EVENT_MYSQL_READ_REQUEST = 60002,
  OB_EVENT_MYSQL_OBSERVER_PL = 60003,
  OB_EVENT_MYSQL_SEND_REQUEST = 60004,

  OB_EVENT_MYSQL_READ_RESPONSE = 60005,
  OB_EVENT_MYSQL_SEND_RESPONSE = 60006,
  OB_EVENT_MYSQL_REQUEST_TRANSFORM = 60007,
  OB_EVENT_MYSQL_RESPONSE_TRANSFORM = 60008,

  OB_EVENT_MYSQL_TXN_START = 60009,
  OB_EVENT_MYSQL_TXN_CLOSE = 60010,
  OB_EVENT_MYSQL_SSN_START = 60011,
  OB_EVENT_MYSQL_SSN_CLOSE = 60012,
  OB_EVENT_MYSQL_RESPONSE_CLIENT = 60013,
  OB_EVENT_MYSQL_CMD_COMPLETE = 60014,

  OB_EVENT_LIFECYCLE_PORTS_INITIALIZED = 60018,
  OB_EVENT_LIFECYCLE_PORTS_READY = 60019,

  // EVENTS 60200 - 60202 for internal use
  OB_EVENT_INTERNAL_60200 = 60200,
  OB_EVENT_INTERNAL_60201 = 60201,
  OB_EVENT_INTERNAL_60202 = 60202
};

enum ObServerState
{
  OB_SRVSTATE_STATE_UNDEFINED = 0,
  OB_SRVSTATE_ACTIVE_TIMEOUT,
  OB_SRVSTATE_CONNECTION_ALIVE,
  OB_SRVSTATE_CONNECTION_CLOSED,
  OB_SRVSTATE_CONNECTION_ERROR,
  OB_SRVSTATE_RESET_SESSION_VARS_ERROR,
  OB_SRVSTATE_INACTIVE_TIMEOUT,
  OB_SRVSTATE_ANALYZE_ERROR,
  OB_SRVSTATE_CMD_COMPLETE,
  OB_SRVSTATE_TRANSACTION_COMPLETE,
};

enum ObVConnCloseFlags
{
  OB_VC_CLOSE_ABORT = -1,
  OB_VC_CLOSE_NORMAL = 1
};

// The TASK pool of threads is the primary method of off-loading continuations from the
// net-threads. Configure this with config.task_thread_num
enum ObThreadPool
{
  OB_THREAD_POOL_DEFAULT = -1,
  OB_THREAD_POOL_NET,
  OB_THREAD_POOL_TASK,
};

class ObContInternal;

typedef void *(*ObThreadFunc) (void* data);
typedef int (*ObEventFunc) (ObContInternal *contp, ObEventType event, void* edata);

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_API_DEFS_H
