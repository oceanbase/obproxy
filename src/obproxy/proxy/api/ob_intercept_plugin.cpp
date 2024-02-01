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

#include "proxy/api/ob_intercept_plugin.h"
#include "proxy/api/ob_plugin_vc.h"
#include "proxy/api/ob_api_utils_internal.h"
#include "proxy/mysql/ob_mysql_sm.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace common;
using namespace event;

int handle_intercept_event(ObContInternal *cont, ObEventType event, void *edata);

ObInterceptPlugin::ObInterceptPlugin(ObApiTransaction &transaction)
    : ObTransactionPlugin(transaction), intercept_cont_(NULL),
      net_vc_(NULL), num_bytes_written_(0), saved_event_(OB_EVENT_NONE),
      saved_edata_(NULL), timeout_action_(NULL)
{
  intercept_cont_ = ObContInternal::alloc(handle_intercept_event, new_proxy_mutex());
  intercept_cont_->data_ = static_cast<void *>(this);

  ObMysqlSM *sm = transaction.get_sm();
  sm->api_.plugin_tunnel_type_ = MYSQL_PLUGIN_AS_INTERCEPT;
  sm->api_.plugin_tunnel_ = ObPluginVCCore::alloc();
  sm->api_.plugin_tunnel_->set_accept_cont(intercept_cont_);
}

void ObInterceptPlugin::destroy_cont()
{
  DEBUG_API("Normal shutdown ObInterceptPlugin continuation:%p", intercept_cont_);
  if (NULL != net_vc_) {
    net_vc_->do_io_shutdown(IO_SHUTDOWN_READWRITE);
    net_vc_->do_io_close();
    net_vc_ = NULL;
  }

  if (NULL != timeout_action_) {
    ObApiUtilsInternal::action_cancel(timeout_action_);
    timeout_action_ = NULL;
  }

  if (NULL != intercept_cont_) {
    intercept_cont_->destroy();
    intercept_cont_ = NULL;
  }
}

void ObInterceptPlugin::destroy()
{
  DEBUG_API("Destroy ObInterceptPlugin:%p", this);
  ObTransactionPlugin::destroy();
  destroy_cont();

  input_.destroy();
  output_.destroy();
}

int ObInterceptPlugin::produce(ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;
  if (NULL == net_vc_ || NULL == reader) {
    WARN_API("Intercept not operational, net_vc_=%p, reader=%p", net_vc_, reader);
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (NULL == output_.buffer_) {
      output_.buffer_ = new_empty_miobuffer();
      output_.reader_ = output_.buffer_->alloc_reader();
      output_.vio_ = net_vc_->do_io_write(intercept_cont_, INT64_MAX, output_.reader_);
    }

    int64_t data_size = reader->read_avail();
    int64_t num_bytes_written = 0;
    if (OB_FAIL(output_.buffer_->write(reader, data_size, num_bytes_written, 0))
        || num_bytes_written != data_size) {
      WARN_API("Error while writing to buffer! Attempted %ld bytes "
               "but only wrote %ld bytes",
               data_size, num_bytes_written);
    } else {
      output_.vio_->reenable();
      num_bytes_written_ += data_size;
      DEBUG_API("Wrote %ld bytes in response", data_size);
    }
  }

  return ret;
}

int ObInterceptPlugin::set_output_complete()
{
  int ret = OB_SUCCESS;
  MUTEX_LOCK(lock, get_mutex(), this_ethread());
  if (NULL == net_vc_) {
    WARN_API("Intercept not operational");
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == output_.buffer_) {
    WARN_API("No output produced so far");
    ret = OB_INVALID_ARGUMENT;
  } else {
    output_.vio_->nbytes_ = num_bytes_written_;
    output_.vio_->reenable();
    DEBUG_API("Response complete");
  }

  return ret;
}

int ObInterceptPlugin::do_read()
{
  int ret = OB_SUCCESS;
  int64_t avail = input_.reader_->read_avail();
  if (avail < 0) {
    WARN_API("Error while getting number of bytes available");
    ret = OB_ERROR;
  } else {
    ObMysqlAnalyzeResult result;
    if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*(input_.reader_), result))) {
      PROXY_API_LOG(WDIAG, "fail to analyze one packet", K(ret));
    } else {
      switch (result.status_) {
        case ANALYZE_ERROR:
          WARN_API("error parsing client request");

          // Disable further I/O on the client
          input_.vio_->nbytes_ = input_.vio_->ndone_;
          // TODO: any further action required?
          ret = OB_ERROR;
          break;

        case ANALYZE_CONT:
          DEBUG_API("Reenabling input vio, more request data still need to be read");
          input_.vio_->reenable();
          break;

        case ANALYZE_DONE:
          DEBUG_API("done parsing client request");
          consume(input_.reader_);
          if (OB_FAIL(input_.reader_->consume(avail))) {
            PROXY_LOG(WDIAG, "fail to consume ", K(avail), K(ret));
          }

          // Modify the input VIO to reflect how much data we've completed.
          input_.vio_->ndone_ = avail;
          handle_input_complete();
          break;

        default:
          PROXY_LOG(EDIAG, "not reached");
          break;
      }
    }
  }

  return ret;
}

int ObInterceptPlugin::handle_event_internal(ObEventType event, void *edata)
{
  DEBUG_API("Received event %d", event);

  switch (event) {
    case OB_EVENT_NET_ACCEPT:
      DEBUG_API("Handling net accept");
      net_vc_ = static_cast<ObVConnection *>(edata);
      input_.buffer_ = new_empty_miobuffer();
      input_.reader_ = input_.buffer_->alloc_reader();
      input_.vio_ = net_vc_->do_io_read(intercept_cont_, INT64_MAX, input_.buffer_);
      break;

    case OB_EVENT_VCONN_WRITE_READY: // nothing to do
      DEBUG_API("Got write ready");
      break;

    case OB_EVENT_VCONN_READ_READY:
      DEBUG_API("Handling read ready");
      if (OB_SUCCESS == do_read()) {
        break;
      }

      // else fall through into the next shut down cases
      WARN_API("Error while reading request!");
      __attribute__ ((fallthrough));

    case OB_EVENT_VCONN_READ_COMPLETE: // fall through intentional
    case OB_EVENT_VCONN_WRITE_COMPLETE:
    case OB_EVENT_VCONN_EOS:
    case OB_EVENT_ERROR: // erroring out, nothing more to do
    case OB_EVENT_NET_ACCEPT_FAILED: // somebody canceled the transaction
      if (OB_EVENT_ERROR == event) {
        WARN_API("Unknown Error!");
      } else if (OB_EVENT_NET_ACCEPT_FAILED == event) {
        WARN_API("Got net_accept_failed!");
      }

      DEBUG_API("Shutting down intercept");
      destroy_cont();
      break;

    default:
      WARN_API("Unknown event %d", event);
      break;
  }
  return EVENT_DONE;
}

int ObInterceptPlugin::handle_event(ObEventType event, void *edata)
{
  MUTEX_TRY_LOCK(lock, get_mutex(), this_ethread());
  if (!lock.is_locked()) {
    if (OB_EVENT_TIMEOUT != event) { // save only "non-retry" info
      saved_event_ = event;
      saved_edata_ = edata;
    }

    timeout_action_ = ObApiUtilsInternal::cont_schedule(
        intercept_cont_, 1, OB_THREAD_POOL_DEFAULT);
  } else {
    if (OB_EVENT_TIMEOUT == event) { // restore original event
      event = saved_event_;
      edata = saved_edata_;
    }

    handle_event_internal(event, edata);
  }

  return EVENT_DONE;
}

int handle_intercept_event(ObContInternal *cont, ObEventType event, void *edata)
{
  ObInterceptPlugin *plugin = static_cast<ObInterceptPlugin *>(cont->data_);
  if (NULL != plugin) {
    ObApiUtilsInternal::dispatch_intercept_event(plugin, event, edata);
  }

  return EVENT_DONE;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
