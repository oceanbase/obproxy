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

#include "proxy/api/ob_api.h"
#include "ob_api_internal.h"
#include "ob_api.h"
#include "ob_plugin.h"
#include "ob_plugin_vc.h"
#include "ob_api_utils_internal.h"
#include "proxy/plugins/ob_mysql_request_prepare_transform_plugin.h"
#include "proxy/plugins/ob_mysql_response_compress_transform_plugin.h"
#include "proxy/plugins/ob_mysql_response_prepare_transform_plugin.h"
#include "proxy/plugins/ob_mysql_request_compress_transform_plugin.h"
#include "proxy/plugins/ob_mysql_response_cursor_transform_plugin.h"
#include "proxy/plugins/ob_mysql_response_new_ps_transform_plugin.h"

/****************************************************************
 *  IMPORTANT - READ ME
 * Any plugin using the IO Core must enter
 *   with a held mutex.
 * Not only does the plugin have to have a mutex_
 *   before entering the IO Core.  The mutex_ needs to be held.
 *   We now take out the mutex_ on each call to ensure it is
 *   held for the entire duration of the IOCore call
 ***************************************************************/

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace event;
using namespace net;
using namespace common;

ObMysqlAPIHooks *mysql_global_hooks = NULL;
ObRecRawStatBlock *api_rsb = NULL;

ObContInternal::ObContInternal(ObEventFunc funcp, ObProxyMutex *mutexp)
    : ObDummyVConnection((ObProxyMutex *)mutexp),
      data_(NULL), event_func_(funcp), event_count_(0), closed_(1),
      deletable_(false), deleted_(false),
      free_magic_(OB_CONT_INTERN_MAGIC_ALIVE)
{
  SET_HANDLER(&ObContInternal::handle_event);
}

void ObContInternal::destroy()
{
  if (free_magic_ == OB_CONT_INTERN_MAGIC_DEAD) {
    ob_release_assert(!"Plugin tries to use a continuation which is deleted");
  }

  deleted_ = true;
  if (deletable_) {
    mutex_.release();
    free_magic_ = OB_CONT_INTERN_MAGIC_DEAD;
    op_reclaim_free(this);
  } else {
    // TODO: Should this schedule on some other "thread" ?
    // TODO: we don't care about the return action?
    ObApiUtilsInternal::cont_schedule(this, 0, OB_THREAD_POOL_DEFAULT);
  }
}

void ObContInternal::handle_event_count(int event)
{
  if ((event == EVENT_IMMEDIATE) || (event == EVENT_INTERVAL)) {
    int val = 0;

    deletable_ = (closed_ != 0);
    val = ATOMIC_FAA((int *)&event_count_, -1);
    if (val <= 0) {
      ob_assert(!"not reached");
    }

    deletable_ = deletable_ && (val == 1);
  }
}

int ObContInternal::handle_event(int event, void *edata)
{
  int ret = EVENT_DONE;
  if (free_magic_ == OB_CONT_INTERN_MAGIC_DEAD) {
    ob_release_assert(!"Plugin tries to use a continuation which is deleted");
  }

  handle_event_count(event);
  if (deleted_) {
    if (deletable_) {
      mutex_.release();
      free_magic_ = OB_CONT_INTERN_MAGIC_DEAD;
      op_reclaim_free(this);
    }
  } else {
    ret = event_func_(this, (ObEventType)event, edata);
  }

  return ret;
}

ObVConnInternal::ObVConnInternal(ObEventFunc funcp, ObProxyMutex *mutexp)
    : ObContInternal(funcp, mutexp), read_vio_(), write_vio_(), output_vc_(NULL)
{
  closed_ = 0;
  SET_HANDLER(&ObVConnInternal::handle_event);
}

void ObVConnInternal::destroy()
{
  PROXY_EVENT_LOG(DEBUG, "ObVConnInternal::destroy", K(deletable_));
  deleted_ = true;
  if (deletable_) {
    mutex_.release();
    read_vio_.set_continuation(NULL);
    write_vio_.set_continuation(NULL);
    op_reclaim_free(this);
  }
}

int ObVConnInternal::handle_event(int event, void *edata)
{
  int ret = EVENT_DONE;
  handle_event_count(event);
  PROXY_EVENT_LOG(DEBUG, "ObVConnInternal::handle_event", K(event), K(edata));
  if (deleted_) {
    if (deletable_) {
      mutex_.release();
      read_vio_.set_continuation(NULL);
      write_vio_.set_continuation(NULL);
      op_reclaim_free(this);
    }
  } else {
    ret = event_func_(this, (ObEventType)event, edata);
  }

  return ret;
}

ObVIO *ObVConnInternal::do_io_read(
    ObContinuation *c, const int64_t nbytes, ObMIOBuffer *buf)
{
  PROXY_EVENT_LOG(DEBUG, "ObVConnInternal::do_io_read", K(c), K(nbytes), K(buf));
  read_vio_.buffer_.writer_for(buf);
  read_vio_.op_ = ObVIO::READ;
  read_vio_.set_continuation(c);
  read_vio_.nbytes_ = nbytes;
  read_vio_.ndone_ = 0;
  read_vio_.vc_server_ = this;

  if (ATOMIC_FAA((int *)&event_count_, 1) < 0) {
    ob_assert(!"not reached");
  }

  if (REGULAR == this_ethread()->tt_) {
    this_ethread()->schedule_imm(this);
  } else {
    g_event_processor.schedule_imm(this, ET_NET);
  }

  return &read_vio_;
}

ObVIO *ObVConnInternal::do_io_write(
    ObContinuation *c, const int64_t nbytes, ObIOBufferReader *buf)
{
  PROXY_EVENT_LOG(DEBUG, "ObVConnInternal::do_io_write", K(c), K(nbytes), K(buf));
  write_vio_.buffer_.reader_for(buf);
  write_vio_.op_ = ObVIO::WRITE;
  write_vio_.set_continuation(c);
  write_vio_.nbytes_ = nbytes;
  write_vio_.ndone_ = 0;
  write_vio_.vc_server_ = this;

  if (write_vio_.buffer_.reader()->read_avail() > 0) {
    if (ATOMIC_FAA((int *)&event_count_, 1) < 0) {
      ob_assert(!"not reached");
    }

    if (REGULAR == this_ethread()->tt_) {
      this_ethread()->schedule_imm(this);
    } else {
      g_event_processor.schedule_imm(this, ET_NET);
    }
  }

  return &write_vio_;
}

void ObVConnInternal::do_io_transform(ObVConnection *vc)
{
  output_vc_ = vc;
}

void ObVConnInternal::do_io_close(const int error)
{
  PROXY_EVENT_LOG(DEBUG, "ObVConnInternal::do_io_close", K(error));
  if (ATOMIC_FAA((int *)&event_count_, 1) < 0) {
    ob_assert(!"not reached");
  }

  if (-1 != error) {
    lerrno_ = error;
    closed_ = OB_VC_CLOSE_ABORT;
  } else {
    closed_ = OB_VC_CLOSE_NORMAL;
  }

  read_vio_.op_ = ObVIO::NONE;
  read_vio_.buffer_.destroy();

  write_vio_.op_ = ObVIO::NONE;
  write_vio_.buffer_.destroy();

  if (NULL != output_vc_) {
    output_vc_->do_io_close(error);
  }

  if (REGULAR == this_ethread()->tt_) {
    this_ethread()->schedule_imm(this);
  } else {
    g_event_processor.schedule_imm(this, ET_NET);
  }
}

void ObVConnInternal::do_io_shutdown(const ShutdownHowToType howto)
{
  PROXY_EVENT_LOG(DEBUG, "ObVConnInternal::do_io_shutdown", K(howto));
  if ((IO_SHUTDOWN_READ == howto) || (IO_SHUTDOWN_READWRITE == howto)) {
    read_vio_.op_ = ObVIO::NONE;
    read_vio_.buffer_.destroy();
  }

  if ((IO_SHUTDOWN_WRITE == howto) || (IO_SHUTDOWN_READWRITE == howto)) {
    write_vio_.op_ = ObVIO::NONE;
    write_vio_.buffer_.destroy();
  }

  if (ATOMIC_FAA((int *)&event_count_, 1) < 0) {
    ob_assert(!"not reached");
  }

  if (REGULAR == this_ethread()->tt_) {
    this_ethread()->schedule_imm(this);
  } else {
    g_event_processor.schedule_imm(this, ET_NET);
  }
}

void ObVConnInternal::reenable(ObVIO *vio)
{
  PROXY_EVENT_LOG(DEBUG, "ObVConnInternal::reenable", K(vio));
  UNUSED(vio);
  if (ATOMIC_FAA((int *)&event_count_, 1) < 0) {
    ob_assert(!"not reached");
  }

  if (REGULAR == this_ethread()->tt_) {
    this_ethread()->schedule_imm(this);
  } else {
    g_event_processor.schedule_imm(this, ET_NET);
  }
}

void ObVConnInternal::retry(int64_t delay)
{
  if (ATOMIC_FAA((int *)&event_count_, 1) < 0) {
    ob_assert(!"not reached");
  }

  mutex_->thread_holding_->schedule_in(this, HRTIME_MSECONDS(delay));
}

bool ObVConnInternal::get_data(const int32_t id, void *data)
{
  bool ret = true;
  switch (id) {
    case OB_API_DATA_READ_VIO:
      *((ObVIO **)data) = &read_vio_;
      break;

    case OB_API_DATA_WRITE_VIO:
      *((ObVIO **)data) = &write_vio_;
      break;

    case OB_API_DATA_OUTPUT_VC:
      *((ObVConnection **)data) = output_vc_;
      break;

    case OB_API_DATA_CLOSED:
      *((int *)data) = closed_;
      break;

    default:
      ret = ObContInternal::get_data(id, data);
      break;
  }
  return ret;
}

bool ObVConnInternal::set_data(const int32_t id, void *data)
{
  bool ret = true;
  switch (id) {
    case OB_API_DATA_OUTPUT_VC:
      output_vc_ = (ObVConnection *)data;
      break;

    default:
      ret = ObContInternal::set_data(id, data);
      break;
  }
  return ret;
}

static void *thread_trampoline(void *data)
{
  ObThreadInternal *thread = NULL;
  void *ret = NULL;

  thread = (ObThreadInternal *) data;
  if (OB_UNLIKELY(OB_SUCCESS != thread->set_specific())) {
    PROXY_EVENT_LOG(ERROR, "failed to set_specific for thread");
  } else {
    ret = thread->func_(thread->data_);
  }
  delete thread;

  return ret;
}

const char *ObMysqlApiDebugNames::get_api_hook_name(ObMysqlHookID t)
{
  const char *ret = NULL;
  switch (t) {
    case OB_MYSQL_READ_REQUEST_HOOK:
      ret = "OB_MYSQL_READ_REQUEST_HOOK";
      break;

    case OB_MYSQL_OBSERVER_PL_HOOK:
      ret = "OB_MYSQL_OBSERVER_PL_HOOK";
      break;

    case OB_MYSQL_SEND_REQUEST_HOOK:
      ret = "OB_MYSQL_SEND_REQUEST_HOOK";
      break;

    case OB_MYSQL_READ_RESPONSE_HOOK:
      ret = "OB_MYSQL_READ_RESPONSE_HOOK";
      break;

    case OB_MYSQL_SEND_RESPONSE_HOOK:
      ret = "OB_MYSQL_SEND_RESPONSE_HOOK";
      break;

    case OB_MYSQL_REQUEST_TRANSFORM_HOOK:
      ret = "OB_MYSQL_REQUEST_TRANSFORM_HOOK";
      break;

    case OB_MYSQL_RESPONSE_TRANSFORM_HOOK:
      ret = "OB_MYSQL_RESPONSE_TRANSFORM_HOOK";
      break;

    case OB_MYSQL_TXN_START_HOOK:
      ret = "OB_MYSQL_TXN_START_HOOK";
      break;

    case OB_MYSQL_TXN_CLOSE_HOOK:
      ret = "OB_MYSQL_TXN_CLOSE_HOOK";
      break;

    case OB_MYSQL_SSN_START_HOOK:
      ret = "OB_MYSQL_SSN_START_HOOK";
      break;

    case OB_MYSQL_SSN_CLOSE_HOOK:
      ret = "OB_MYSQL_SSN_CLOSE_HOOK";
      break;

    case OB_MYSQL_RESPONSE_CLIENT_HOOK:
      ret = "OB_MYSQL_RESPONSE_CLIENT_HOOK";
      break;

    case OB_MYSQL_CMD_COMPLETE_HOOK:
      ret = "OB_MYSQL_CMD_COMPLETE_HOOK";
      break;

    case OB_MYSQL_LAST_HOOK:
      ret = "OB_MYSQL_LAST_HOOK";
      break;

    default:
      ret = "unknown hook";
      break;
  }

  return ret;
}

ObEThread *ObThreadInternal::create_thread(ObThreadFunc func, void *data)
{
  ObThreadInternal *thread = new(std::nothrow) ObThreadInternal();

  if (NULL == thread) {
    ERROR_API("failed to allocate memory for ObThreadInternal");
  } else {
    ob_assert(0 == thread->event_types_);
    thread->func_ = func;
    thread->data_ = data;

    if (!(thread_create(thread_trampoline, (void *)thread, 1))) {
      ERROR_API("failed to create thread");
      delete thread;
      thread = NULL;
    }
  }

  return thread;
}

int api_init()
{
  static bool init = true;
  int ret = OB_SUCCESS;

  if (init) {
    init = false;
    api_rsb = NULL;

    if (OB_ISNULL(mysql_global_hooks = new(std::nothrow) ObMysqlAPIHooks())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ERROR_API("failed new ObMysqlAPIHooks, ret=%d", ret);
    } else {
      // if (OB_ISNULL(api_rsb = g_stat_processor.allocate_raw_stat_block(OB_MAX_API_STATS, XFH_API_STATE))) {
      //   ret = OB_ALLOCATE_MEMORY_FAILED;
      //   ERROR_API("Can't allocate API stats block, ret=%d", ret);
      //   delete mysql_global_hooks;
      // } else {
          DEBUG_API("initialized API stats with %d slots", OB_MAX_API_STATS);
          //init_stat_intercept(); // for temp test
          //init_null_transform();
          //init_trim_okpacket_transform();
          init_mysql_request_prepare_transform();
          init_mysql_request_compress_transform();
          init_mysql_resposne_compress_transform();
          init_mysql_response_prepare_transform();
          init_mysql_response_cursor_transform();
          init_mysql_response_prepare_execute_transform();
      // }
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
