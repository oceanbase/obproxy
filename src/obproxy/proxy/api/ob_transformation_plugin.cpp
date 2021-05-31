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

#include "proxy/api/ob_transformation_plugin.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "proxy/api/ob_api_utils_internal.h"
#include "proxy/api/ob_api.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace common;
using namespace event;

int handle_transform_event(ObContInternal *contp, ObEventType event, void *edata);

ObTransformationPlugin::ObTransformationPlugin(
    ObApiTransaction &transaction, ObTransformationPlugin::ObTransformType type)
    : ObTransactionPlugin(transaction),
      vconn_(NULL), transaction_(&transaction), type_(type),
      output_vio_(NULL), output_buffer_(NULL),
      output_buffer_reader_(NULL), bytes_written_(0), transform_bytes_(-1),
      input_complete_dispatched_(false)
{
  output_buffer_ = new_empty_miobuffer();
  output_buffer_reader_ = output_buffer_->alloc_reader();

  ObProxyMutex *mutex = sm_->mutex_;
  if (NULL == mutex) {
    mutex = new_proxy_mutex();
  }

  vconn_ = ObVConnInternal::alloc(handle_transform_event, mutex);
  vconn_->data_ = static_cast<void *>(this); // edata in a ObTransformationHandler is NOT a ObMysqlSM *
  ObMysqlHookID hook_id = ObApiUtilsInternal::convert_internal_transformation_type(type);
  if (hook_id < OB_MYSQL_LAST_HOOK) {
    sm_->txn_hook_append(hook_id, vconn_);
  }
  DEBUG_API("Creating ObTransformationPlugin=%p (vconn)contp=%p mysqlsm=%p "
            "transformation_type=%d hook_id=%s",
            this, vconn_, sm_, type, ObMysqlApiDebugNames::get_api_hook_name(hook_id));
}

void ObTransformationPlugin::destroy()
{
  ObTransactionPlugin::destroy();

  DEBUG_API("Destroying ObTransformationPlugin=%p", this);
  vconn_->data_ = reinterpret_cast<void *>(0xDEADDEAD);
  vconn_->destroy();

  if (NULL != output_buffer_reader_) {
    output_buffer_reader_->dealloc();
    output_buffer_reader_ = NULL;
  }

  if (NULL != output_buffer_) {
    free_miobuffer(output_buffer_);
    output_buffer_ = NULL;
  }
}

int64_t ObTransformationPlugin::produce(ObIOBufferReader *reader, const int64_t produce_size)
{
  int64_t bytes_written = 0;
  int64_t write_length = produce_size > 0 ? produce_size : reader->read_avail();
  DEBUG_API("ObTransformationPlugin=%p mysqlsm=%p producing output with length=%ld",
            this, sm_, write_length);

  if (write_length > 0) {
    if (NULL == output_vio_) {
      ObVConnection *output_vconn = NULL;
      vconn_->get_data(OB_API_DATA_OUTPUT_VC, &output_vconn);
      DEBUG_API("ObTransformationPlugin=%p mysqlsm=%p will issue a do_io_write, output_vconn=%p.",
                this, sm_, output_vconn);
      if (NULL == output_vconn) {
        WARN_API("ObTransformationPlugin=%p mysqlsm=%p output_vconn=%p cannot issue "
                 "do_io_write due to null output vconn.",
                 this, sm_, output_vconn);
      } else {
        // If you're confused about the following reference the transformation docs.
        // You always write INT64_MAX, this basically says you're not sure how much
        // data you're going to write.
        // FIXME: If you are sure the size, the transform can't  modify the data size.
        // The request transform can't change the data size of request.
        ob_assert(transform_bytes_ >= 0);
        output_vio_ = output_vconn->do_io_write(vconn_, transform_bytes_, output_buffer_reader_);
        if (NULL == output_vio_) {
          WARN_API("ObTransformationPlugin=%p mysqlsm=%p output_vio=%p "
                   "write_length=%ld, do_io_write failed.",
                   this, sm_, output_vio_, write_length);
        }
      }
    }

    if (NULL != output_vio_) {
      // Finally we can copy this data into the output_buffer
      bytes_written = 0;
      int ret = output_buffer_->write(reader, write_length, bytes_written, 0);

      if (OB_FAIL(ret) || bytes_written != write_length) {
        WARN_API("ObTransformationPlugin=%p mysqlsm=%p bytes written < expected. "
                  "bytes_written=%ld write_length=%ld",
                  this, sm_, bytes_written, write_length);
      } else {
        bytes_written_ += bytes_written; // So we can set BytesDone on set_output_complete().
        DEBUG_API("ObTransformationPlugin=%p mysqlsm=%p write to ObMIOBuffer * %ld "
                  "bytes total bytes written %ld",
                  this, sm_, bytes_written, bytes_written_);
      }

      int connection_closed = 0;
      vconn_->get_data(OB_API_DATA_CLOSED, &connection_closed);
      DEBUG_API("ObTransformationPlugin=%p mysqlsm=%p vconn=%p connection_closed=%d, type=%d",
                this, sm_, vconn_, connection_closed, type_);

      if (!connection_closed) {
        if (REQUEST_TRANSFORMATION == type_) {
          // request use buffering transformation, no wake up the downstream vio,
          // it will wake up the downstream vio at set_output_complete()
          output_vio_->reenable(); // Wake up the downstream vio
        } else {
          output_vio_->reenable(); // Wake up the downstream vio
        }
      } else {
        WARN_API("ObTransformationPlugin=%p mysqlsm=%p output_vio=%p connection_closed=%d : "
                 "Couldn't reenable output vio (connection closed).",
                 this, sm_, output_vio_, connection_closed);
        bytes_written = 0;
      }
    }
  }

  return bytes_written;
}

int64_t ObTransformationPlugin::set_output_complete()
{
  int connection_closed = 0;
  vconn_->get_data(OB_API_DATA_CLOSED, &connection_closed);
  DEBUG_API("OutputComplete ObTransformationPlugin=%p mysqlsm=%p vconn=%p "
            "connection_closed=%d, total bytes written=%ld",
            this, sm_, vconn_, connection_closed, bytes_written_);

  if (!connection_closed && NULL == output_vio_) {
    DEBUG_API("ObTransformationPlugin=%p mysqlsm=%p output complete without "
              "writing any data, initiating write of 0 bytes.",
              this, sm_);

    // We're done without ever outputting anything, to correctly
    // clean up we'll initiate a write then immeidately set it to 0 bytes done.
    ObVConnection *output_vconn = NULL;
    vconn_->get_data(OB_API_DATA_OUTPUT_VC, &output_vconn);
    output_vio_ = output_vconn->do_io_write(vconn_, 0, output_buffer_reader_);

    if (NULL != output_vio_) {
      output_vio_->ndone_ = 0;
      output_vio_->reenable(); // Wake up the downstream vio
    } else {
      WARN_API("ObTransformationPlugin=%p mysqlsm=%p unable to reenable "
               "output_vio=%p because VConnWrite failed.",
               this, sm_, output_vio_);
    }
  } else if (!connection_closed) {
    // So there is a possible race condition here, if we wake up a dead
    // VIO it can cause a segfault, so we must check that the VCONN is not dead.
    int connection_closed = 0;
    vconn_->get_data(OB_API_DATA_CLOSED, &connection_closed);
    if (!connection_closed) {
      output_vio_->nbytes_ = bytes_written_;
      output_vio_->reenable(); // Wake up the downstream vio
    } else {
      WARN_API("ObTransformationPlugin=%p mysqlsm=%p unable to reenable "
               "output_vio=%p connection was closed=%d.",
               this, sm_, output_vio_, connection_closed);
    }
  } else {
    WARN_API("ObTransformationPlugin=%p mysqlsm=%p unable to reenable "
             "output_vio=%p connection was closed=%d.",
             this, sm_, output_vio_, connection_closed);
  }

  return bytes_written_;
}

int ObTransformationPlugin::handle_transform_read(ObContInternal *contp)
{
  int ret = OB_SUCCESS;
  // The naming is quite confusing, in this context the write_vio
  // is actually the vio we read from.
  ObVIO *write_vio = NULL;
  contp->get_data(OB_API_DATA_WRITE_VIO, &write_vio);
  if (NULL != write_vio) {
    int64_t to_read = write_vio->ntodo();
    DEBUG_API("Transformation contp=%p write_vio=%p, to_read=%ld", contp, write_vio, to_read);

    if (transform_bytes_ <= 0) {
      transform_bytes_ = write_vio->nbytes_;
    }

    if (to_read > 0) {
      /**
       * The amount of data left to read needs to be truncated by
       * the amount of data actually in the read buffer.
       */
      int64_t avail = write_vio->get_reader()->read_avail();
      DEBUG_API("Transformation contp=%p write_vio=%p, to_read=%ld, buffer reader avail=%ld",
                contp, write_vio, to_read, avail);
      ObContinuation *vio_cont = write_vio->cont_;

      if (to_read > avail) {
        to_read = avail;
        DEBUG_API("Transformation contp=%p write_vio=%p, to read > avail, "
                  "fixing to_read to be equal to avail. to_read=%ld, buffer reader avail=%ld",
                  contp, write_vio, to_read, avail);
      }

      if (to_read > 0) {
        // Now call the client to tell them about data
        if (OB_FAIL(consume(write_vio->get_reader()))) {
          PROXY_API_LOG(WARN, "fail to consume", K(to_read), K(ret));
          if (OB_ISNULL(vio_cont)) {
            PROXY_API_LOG(ERROR, "invalid vio cont", K(ret));
          } else {
            // tell conumser something error
            vio_cont->handle_event(OB_EVENT_ERROR, write_vio);
          }
        } else {
          // Tell the read buffer that we have read the data and are no
          // longer interested in it.
          if (OB_FAIL(write_vio->get_reader()->consume(to_read))) {
            PROXY_API_LOG(WARN, "fail to consume ", K(to_read), K(ret));
          }

          // Modify the read VIO to reflect how much data we've completed.
          write_vio->ndone_ += to_read;

          DEBUG_API("Transformation contp=%p write_vio=%p consumed "
                    "%ld bytes from buffer reader",
                    contp, write_vio, to_read);
        }
      }

      // now that we've finished reading we will check if there is anything left to read.
      if (OB_SUCC(ret)) {
        if (write_vio->ntodo() > 0) {
          DEBUG_API("Transformation contp=%p write_vio=%p, vio_cont=%p still has bytes "
                    "left to process, ntodo > 0.",
                    contp, write_vio, vio_cont);

          if (to_read > 0) {
            write_vio->reenable();

            // Call back the read VIO continuation to let it know that we are ready for more data.
            if (NULL != vio_cont) {
              vio_cont->handle_event(OB_EVENT_VCONN_WRITE_READY, write_vio);
            }
          }
        } else {
          DEBUG_API("Transformation contp=%p write_vio=%p, vio_cont=%p has no bytes "
                    "left to process, will send WRITE_COMPLETE.",
                    contp, write_vio, vio_cont);

          // Call back the write VIO continuation to let it know that we have
          // completed the write operation.
          if (!input_complete_dispatched_) {
            handle_input_complete();
            input_complete_dispatched_ = true;
            if (NULL != vio_cont && NULL != write_vio->get_writer()) {
              vio_cont->handle_event(OB_EVENT_VCONN_WRITE_COMPLETE, write_vio);
            }
          }
        }
      }
    } else {
      ObContinuation *vio_cont = write_vio->cont_; // for some reason this can occasionally be null?
      DEBUG_API("Transformation contp=%p write_vio=%p, vio_cont=%p has no bytes "
                "left to process.",
                contp, write_vio, vio_cont);

      // Call back the write VIO continuation to let it know that we have
      // completed the write operation.
      if (!input_complete_dispatched_) {
        handle_input_complete();
        input_complete_dispatched_ = true;
        if (NULL != vio_cont && NULL != write_vio->get_writer()) {
          vio_cont->handle_event(OB_EVENT_VCONN_WRITE_COMPLETE, write_vio);
        }
      }
    }
  } else {
    WARN_API("Transformation contp=%p write_vio=%p was NULL!", contp, write_vio);
  }

  return 0;
}

int ObTransformationPlugin::handle_event(ObEventType event, void *edata)
{
  int ret = 0;
  DEBUG_API("Transformation event=%d edata=%p mysqlsm=%p",
            event, edata, sm_);

  // The first thing you always do is check if the VConn is closed.
  int connection_closed = 0;
  vconn_->get_data(OB_API_DATA_CLOSED, &connection_closed);
  if (connection_closed) {
    DEBUG_API("Transformation contp=%p mysqlsm=%p is closed connection_closed=%d ",
              vconn_, sm_, connection_closed);
  } else if (OB_EVENT_VCONN_WRITE_COMPLETE == event) {
    ObVConnection *output_vconn = NULL;
    vconn_->get_data(OB_API_DATA_OUTPUT_VC, &output_vconn);
    DEBUG_API("Transformation contp=%p mysqlsm=%p received WRITE_COMPLETE, "
              "shutting down outputvconn=%p ",
              vconn_, sm_, output_vconn);
    output_vconn->do_io_shutdown(IO_SHUTDOWN_WRITE); // The other end is done reading our output
  } else if (OB_EVENT_ERROR == event) {
    ObVIO *write_vio = NULL;
    // Get the write VIO for the write operation that was
    // performed on ourself. This VIO contains the continuation of
    // our parent transformation.
    vconn_->get_data(OB_API_DATA_WRITE_VIO, &write_vio);
    ObContinuation *vio_cont = write_vio->cont_;
    WARN_API("Transformation contp=%p mysqlsm=%p received EVENT_ERROR forwarding "
             "to write_vio=%p viocont=%p",
             vconn_, sm_, write_vio, vio_cont);
    if (NULL != vio_cont) {
      vio_cont->handle_event(OB_EVENT_ERROR, write_vio);
    }
  } else {
    // All other events including WRITE_READY will just attempt to transform more data.
    ret = handle_transform_read(vconn_);
  }

  return ret;
}

int ObTransformationPlugin::get_write_ntodo(int64_t &to_write)
{
  int ret = OB_SUCCESS;
  to_write = -1;
  ObVIO *write_vio = NULL;
  if (OB_ISNULL(vconn_)) {
    ret = OB_ERR_UNEXPECTED;
    WARN_API("vconn_ can not be NULL here, ret=%d", ret);
  } else {
    vconn_->get_data(OB_API_DATA_WRITE_VIO, &write_vio);
    if (OB_ISNULL(write_vio)) {
      ret = OB_ERR_UNEXPECTED;
      WARN_API("write_vio can not be NULL here, ret=%d", ret);
    } else {
      to_write = write_vio->ntodo();
      DEBUG_API("ObTransformationPlugin, to_write=%ld", to_write);
    }
  }

  return ret;
}

int handle_transform_event(ObContInternal *contp, ObEventType event, void *edata)
{
  ObTransformationPlugin *plugin = static_cast<ObTransformationPlugin *>(contp->data_);
  if (NULL != plugin) {
    ObApiUtilsInternal::dispatch_transform_event(plugin, event, edata);
  }
  return 0;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
