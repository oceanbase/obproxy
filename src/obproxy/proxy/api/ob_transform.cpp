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
 * A brief file description
 * @section thoughts Transform thoughts
 *
 * - Must be able to handle a chain of transformations.
 * - Any transformation in the chain may fail.
 *   Failure options:
 *     - abort the client (if transformed data already sent)
 *     - serve the client the untransformed document
 *     - remove the failing transformation from the chain and attempt the transformation again (difficult to do)
 *     - never send untransformed document to client if client would not understand it (e.g. a set top box)
 * - Must be able to change response header fields up until the point that TRANSFORM_READ_READY is sent to the user.
 *
 * @section usage Transform usage
 *
 *   -# ObTransformProcessor.open (cont, hooks); - returns "tvc", a ObTransformVConnection if 'hooks != NULL'
 *   -# tvc->do_io_write (cont, nbytes, buffer1);
 *   -# cont->handle_event (TRANSFORM_READ_READY, NULL);
 *   -# tvc->do_io_read (cont, nbytes, buffer2);
 *   -# tvc->do_io_close ();
 *
 * @section visualization Transform visualization
 *
 * @verbatim
 *        +----+     +----+     +----+     +----+
 *   -IB->| T1 |-B1->| T2 |-B2->| T3 |-B3->| T4 |-OB->
 *        +----+     +----+     +----+     +----+
 * @endverbatim
 *
 * Data flows into the first transform in the form of the buffer
 * passed to ObTransformVConnection::do_io_write (IB). Data flows
 * out of the last transform in the form of the buffer passed to
 * ObTransformVConnection::do_io_read (OB). Between each transformation is
 * another buffer (B1, B2 and B3).
 *
 * A transformation is a ObContinuation. The continuation is called with the
 * event TRANSFORM_IO_WRITE to initialize the write and TRANSFORM_IO_READ
 * to initialize the read.
 */

#include "proxy/api/ob_transform_internal.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace event;
using namespace net;

#define DEBUG_TRANSFORM(tag, fmt...) _PROXY_TRANSFORM_LOG(DEBUG, ##fmt)

ObTransformProcessor g_transform_processor;

void ObTransformProcessor::start()
{

}

ObVConnection *ObTransformProcessor::open(ObContinuation *cont, ObAPIHook *hooks)
{
  if (NULL != hooks) {
    return ObTransformVConnection::alloc(cont, hooks);
  } else {
    return NULL;
  }
}

ObVConnInternal *ObTransformProcessor::null_transform(ObProxyMutex *mutex)
{
  return ObNullTransform::alloc(mutex);
}

ObTransformTerminus::ObTransformTerminus(ObTransformVConnection *tvc)
    : ObVConnection(tvc->mutex_),
      tvc_(tvc), read_vio_(), write_vio_(), event_count_(0),
      deletable_(0), closed_(0), called_user_(0)
{
  SET_HANDLER(&ObTransformTerminus::handle_event);
}

#define RETRY() \
    if (ATOMIC_FAA((int *)&event_count_, 1) < 0) { \
        ob_assert (!"not reached"); \
    } \
    if (REGULAR == this_ethread()->tt_) { \
      this_ethread()->schedule_in(this, HRTIME_MSECONDS(1)); \
    } else { \
      g_event_processor.schedule_in (this, HRTIME_MSECONDS(1), ET_NET); \
    } \
    return 0;

int ObTransformTerminus::handle_event(int event, void *edata)
{
  int32_t val = 0;
  UNUSED(edata);
  ob_release_assert(NULL != tvc_);

  deletable_ = (closed_ && tvc_->closed_);
  val = ATOMIC_FAA((int *)&event_count_, -1);

  DEBUG_TRANSFORM("transform", "[ObTransformTerminus::handle_event] event_count %d", event_count_);

  if (val <= 0) {
    ob_assert(!"not reached");
  }

  deletable_ = deletable_ && (1 == val);
  if (closed_&& tvc_->closed_) {
    if (deletable_) {
      DEBUG_TRANSFORM("transform", "ObTransformVConnection destroy [%p]", tvc_);
      tvc_->destroy();
      // Attention!! Must not set tvc_ = NULL, tvc_'s memory has been free by destroy() above
    }
  } else if (ObVIO::WRITE == write_vio_.op_) {
    if (ObVIO::NONE == read_vio_.op_) {
      if (!called_user_) {
        DEBUG_TRANSFORM("transform", "ObTransformVConnection calling user: %d %d [%p] [%p] [%ld]",
                        event_count_, event, tvc_, tvc_->cont_, write_vio_.nbytes_);

        called_user_ = 1;
        // It is our belief this is safe to pass a reference, i.e. its scope
        // and locking ought to be safe across the lifetime of the continuation.
        tvc_->cont_->handle_event(TRANSFORM_READ_READY, (void *)&write_vio_.nbytes_);
      }
    } else {
      int64_t towrite = 0;
      int64_t written_len = 0;

      MUTEX_TRY_LOCK(trylock1, write_vio_.mutex_, this_ethread());
      if (!trylock1.is_locked()) {
        RETRY();
      }

      MUTEX_TRY_LOCK(trylock2, read_vio_.mutex_, this_ethread());
      if (!trylock2.is_locked()) {
        RETRY();
      }

      if (!closed_ && ObVIO::NONE != write_vio_.op_) {
        towrite = write_vio_.ntodo();
        DEBUG_TRANSFORM("transform", "[ObTransformTerminus::handle_event], towrite=%ld", towrite);
        if (towrite > 0) {
          int64_t read_avail = write_vio_.get_reader()->read_avail();

          if (towrite > read_avail) {
            towrite = read_avail;
          }

          if (towrite > read_vio_.ntodo()) {
            towrite = read_vio_.ntodo();
          }

          if (towrite > 0) {
            int ret = OB_SUCCESS;
            ret = read_vio_.get_writer()->write(write_vio_.get_reader(), towrite, written_len);
            if (OB_FAIL(ret) || towrite != written_len) {
              PROXY_TRANSFORM_LOG(WDIAG, "fail to write iobuffer from reader",
                                  K(towrite), K(written_len), K(ret));
            }
            read_vio_.ndone_ += towrite;

            if (OB_FAIL(write_vio_.get_reader()->consume(towrite))) {
              PROXY_TRANSFORM_LOG(WDIAG, "fail to consume ", K(towrite), K(ret));
            }

            write_vio_.ndone_ += towrite;
          }
        }

        DEBUG_TRANSFORM("transform", "[ObTransformTerminus::handle_event], towrite=%ld, ntodo_=%ld",
                        towrite, write_vio_.ntodo());

        if (write_vio_.ntodo() > 0) {
          if (towrite > 0) {
            write_vio_.cont_->handle_event(VC_EVENT_WRITE_READY, &write_vio_);
          }
        } else {
          write_vio_.cont_->handle_event(VC_EVENT_WRITE_COMPLETE, &write_vio_);
        }

        // We could have closed on the write callback
        if (!(closed_ && tvc_->closed_)) {
          if (read_vio_.ntodo() > 0) {
            if (write_vio_.ntodo() <= 0) {
              read_vio_.cont_->handle_event(VC_EVENT_EOS, &read_vio_);
            } else if (towrite > 0) {
              DEBUG_TRANSFORM("transform", "[ObTransformTerminus::handle_event], towrite=%ld", towrite);
              read_vio_.cont_->handle_event(VC_EVENT_READ_READY, &read_vio_);
            }
          } else {
            DEBUG_TRANSFORM("transform", "[ObTransformTerminus::handle_event] read_vio_.cont_=%p", read_vio_.cont_);
            read_vio_.cont_->handle_event(VC_EVENT_READ_COMPLETE, &read_vio_);
          }
        }
      }
    }
  } else {
    MUTEX_TRY_LOCK(trylock2, read_vio_.mutex_, this_ethread());
    if (!trylock2.is_locked()) {
      RETRY();
    }

    if (closed_) {
      // The terminus was closed, but the enclosing transform
      // vconnection wasn't. If the terminus was aborted then we
      // call the read_vio cont back with VC_EVENT_ERROR. If it
      // was closed normally then we call it back with
      // VC_EVENT_EOS. If a read operation hasn't been initiated
      // yet and we haven't called the user back then we call
      // the user back instead of the read_vio cont (which won't
      // exist).
      if (!tvc_->closed_) {
        int ev = (OB_VC_CLOSE_ABORT == closed_) ? VC_EVENT_ERROR : VC_EVENT_EOS;

        if (!called_user_) {
          called_user_ = 1;
          tvc_->cont_->handle_event(ev, NULL);
        } else {
          ob_assert(NULL != read_vio_.cont_);
          read_vio_.cont_->handle_event(ev, &read_vio_);
        }
      }
    }
  }

  return 0;
}

ObVIO *ObTransformTerminus::do_io_read(ObContinuation *c, int64_t nbytes, ObMIOBuffer *buf)
{
  read_vio_.buffer_.writer_for(buf);
  read_vio_.op_ = ObVIO::READ;
  read_vio_.set_continuation(c);
  read_vio_.nbytes_ = nbytes;
  read_vio_.ndone_ = 0;
  read_vio_.vc_server_ = this;

  if (ATOMIC_FAA((int *)&event_count_, 1) < 0) {
    ob_assert(!"not reached");
  }

  DEBUG_TRANSFORM("transform", "[ObTransformTerminus::do_io_read] event_count %d", event_count_);
  if (REGULAR == this_ethread()->tt_) {
    this_ethread()->schedule_imm(this);
  } else {
    g_event_processor.schedule_imm(this, ET_NET);
  }

  return &read_vio_;
}

ObVIO *ObTransformTerminus::do_io_write(ObContinuation *c, const int64_t nbytes,
                                        ObIOBufferReader *buf)
{
  // In the process of eliminating 'owner' mode so asserting against it
  write_vio_.buffer_.reader_for(buf);
  write_vio_.op_ = ObVIO::WRITE;
  write_vio_.set_continuation(c);
  write_vio_.nbytes_ = nbytes;
  write_vio_.ndone_ = 0;
  write_vio_.vc_server_ = this;

  if (ATOMIC_FAA((int *)&event_count_, 1) < 0) {
    ob_assert(!"not reached");
  }

  DEBUG_TRANSFORM("transform", "[ObTransformTerminus::do_io_write] event_count %d", event_count_);
  if (REGULAR == this_ethread()->tt_) {
    this_ethread()->schedule_imm(this);
  } else {
    g_event_processor.schedule_imm(this, ET_NET);
  }

  return &write_vio_;
}

void ObTransformTerminus::do_io_close(int error)
{
  DEBUG_TRANSFORM("transform", "[ObTransformTerminus::do_io_close] error %d, event_count %d",
                  error , event_count_);
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

  if (REGULAR == this_ethread()->tt_) {
    this_ethread()->schedule_imm(this);
  } else {
    g_event_processor.schedule_imm(this, ET_NET);
  }
}

void ObTransformTerminus::do_io_shutdown(ShutdownHowToType howto)
{
  if ((IO_SHUTDOWN_READ == howto) || (IO_SHUTDOWN_READWRITE == howto)) {
    read_vio_.op_ = ObVIO::NONE;
    read_vio_.buffer_.destroy();
  }

  if ((IO_SHUTDOWN_WRITE == howto) || (IO_SHUTDOWN_READWRITE == howto)) {
    write_vio_.op_ = ObVIO::NONE;
    write_vio_.buffer_.destroy();
  }
}

void ObTransformTerminus::reenable(ObVIO *vio)
{
  ob_assert((vio == &read_vio_) || (vio == &write_vio_));

  if (0 == event_count_) {
    if (ATOMIC_FAA((int *)&event_count_, 1) < 0) {
      ob_assert(!"not reached");
    }

    DEBUG_TRANSFORM("transform", "[ObTransformTerminus::reenable] event_count %d", event_count_);
    if (REGULAR == this_ethread()->tt_) {
      this_ethread()->schedule_imm(this);
    } else {
      g_event_processor.schedule_imm(this, ET_NET);
    }
  } else {
    DEBUG_TRANSFORM("transform", "[ObTransformTerminus::reenable] skipping due to pending events");
  }
}

ObTransformVConnection::ObTransformVConnection(ObContinuation *cont, ObAPIHook *hooks)
    : ObTransformVCChain(cont->mutex_), cont_(cont), terminus_(this), closed_(0)
{
  ObVConnInternal *xform = NULL;

  SET_HANDLER(&ObTransformVConnection::handle_event);

  ob_assert(NULL != hooks);

  transform_ = hooks->cont_;
  while (NULL != hooks->link_.next_) {
    xform = (ObVConnInternal *)hooks->cont_;
    hooks = hooks->link_.next_;
    xform->do_io_transform(hooks->cont_);
  }

  xform = (ObVConnInternal *)hooks->cont_;
  xform->do_io_transform(&terminus_);

  DEBUG_TRANSFORM("transform", "ObTransformVConnection create [%p]", this);
}

void ObTransformVConnection::destroy()
{
  // Clear the continuations in terminus VConnections so that
  // mutex's get released
  terminus_.read_vio_.set_continuation(NULL);
  terminus_.write_vio_.set_continuation(NULL);
  terminus_.mutex_.release();
  mutex_.release();
  op_reclaim_free(this);
}

int ObTransformVConnection::handle_event(int event, void *edata)
{
  ob_assert(!"not reached");
  UNUSED(event);
  UNUSED(edata);
  return 0;
}

ObVIO *ObTransformVConnection::do_io_read(
    ObContinuation *c, const int64_t nbytes, ObMIOBuffer *buf)
{
  DEBUG_TRANSFORM("transform", "ObTransformVConnection do_io_read: %p [%p]", c, this);

  return terminus_.do_io_read(c, nbytes, buf);
}

ObVIO *ObTransformVConnection::do_io_write(
    ObContinuation *c, const int64_t nbytes,
    ObIOBufferReader *buf)
{
  DEBUG_TRANSFORM("transform", "ObTransformVConnection do_io_write: %p [%p]", c, this);
  return transform_->do_io_write(c, nbytes, buf);
}

void ObTransformVConnection::do_io_close(const int error)
{
  DEBUG_TRANSFORM("transform", "ObTransformVConnection do_io_close: %d [%p]", error, this);

  if (-1 != error) {
    closed_ = OB_VC_CLOSE_ABORT;
  } else {
    closed_ = OB_VC_CLOSE_NORMAL;
  }

  transform_->do_io_close(error);
}

void ObTransformVConnection::do_io_shutdown(const ShutdownHowToType howto)
{
  ob_assert(IO_SHUTDOWN_WRITE == howto);

  DEBUG_TRANSFORM("transform", "ObTransformVConnection do_io_shutdown: %d [%p]",
                  howto, this);

  transform_->do_io_shutdown(howto);
}

void ObTransformVConnection::reenable(ObVIO * /* vio */)
{
  ob_assert(!"not reached");
}

uint64_t ObTransformVConnection::backlog(uint64_t limit)
{
  uint64_t b = 0; // backlog
  ObVConnection *raw_vc = transform_;
  ObMIOBuffer *w = NULL;
  while (NULL != raw_vc && raw_vc != &terminus_) {
    ObVConnInternal *vc = static_cast<ObVConnInternal *>(raw_vc);
    if (NULL != (w = vc->read_vio_.buffer_.writer())) {
      b += w->max_read_avail();
    }

    if (b >= limit) {
      break;
    }

    raw_vc = vc->output_vc_;
  }

  if (b < limit) {
    if (NULL != (w = terminus_.read_vio_.buffer_.writer())) {
      b += w->max_read_avail();
    }

    if (b < limit) {
      ObIOBufferReader *r = terminus_.write_vio_.get_reader();
      if (NULL != r) {
        b += r->read_avail();
      }
    }
  }

  return b;
}

ObTransformControl::ObTransformControl()
    : ObContinuation(new_proxy_mutex()), hooks_(), tvc_(NULL), read_buf_(NULL), write_buf_(NULL)
{
  SET_HANDLER(&ObTransformControl::handle_event);

  hooks_.append(g_transform_processor.null_transform(new_proxy_mutex()));
}

int ObTransformControl::handle_event(int event, void *edata)
{
  UNUSED(edata);
  switch (event) {
    case EVENT_IMMEDIATE:
    {
      char *s = NULL;
      char *e = NULL;

      ob_assert(NULL == tvc_);
      if (NULL != mysql_global_hooks
          && mysql_global_hooks->get(OB_MYSQL_RESPONSE_TRANSFORM_HOOK)) {
        tvc_ = g_transform_processor.open(this, mysql_global_hooks->get(OB_MYSQL_RESPONSE_TRANSFORM_HOOK));
      } else {
        tvc_ = g_transform_processor.open(this, hooks_.get());
      }

      ob_assert(NULL != tvc_);

      write_buf_ = new_miobuffer();
      s = write_buf_->end();
      e = write_buf_->buf_end();

      memset(s, 'a', e - s);
      write_buf_->fill(e - s);

      tvc_->do_io_write(this, 4 * 1024, write_buf_->alloc_reader());
      break;
    }

    case TRANSFORM_READ_READY:
    {
      ObMIOBuffer *buf = new_empty_miobuffer();

      read_buf_ = buf->alloc_reader();
      tvc_->do_io_read(this, INT64_MAX, buf);
      break;
    }

    case VC_EVENT_READ_COMPLETE:
    case VC_EVENT_EOS:
      tvc_->do_io_close();

      free_miobuffer(read_buf_->mbuf_);
      read_buf_ = NULL;

      free_miobuffer(write_buf_);
      write_buf_ = NULL;
      break;

    case VC_EVENT_WRITE_COMPLETE:
      break;

    default:
      PROXY_TRANSFORM_LOG(EDIAG, "not reached");
      break;
  }

  return 0;
}

ObNullTransform::ObNullTransform(ObProxyMutex *mutex)
    : ObVConnInternal(NULL, mutex),
      output_buf_(NULL), output_reader_(NULL), output_vio_(NULL)
{
  SET_HANDLER(&ObNullTransform::handle_event);

  DEBUG_TRANSFORM("transform", "ObNullTransform create [%p]", this);
}

void ObNullTransform::destory()
{
  mutex_.release();
  if (NULL != output_buf_) {
    free_miobuffer(output_buf_);
  }
  op_reclaim_free(this);
}

int ObNullTransform::handle_event(int event, void *edata)
{
  handle_event_count(event);

  DEBUG_TRANSFORM("transform", "[ObNullTransform::handle_event] event count %d", event_count_);

  if (closed_) {
    if (deletable_) {
      DEBUG_TRANSFORM("transform", "ObNullTransform destroy: %ld [%p]",
                      output_vio_ ? output_vio_->ndone_ : 0, this);
      destory();
    }
  } else {
    switch (event) {
      case VC_EVENT_ERROR:
        write_vio_.cont_->handle_event(VC_EVENT_ERROR, &write_vio_);
        break;

      case VC_EVENT_WRITE_COMPLETE:
        ob_assert(output_vio_ == (ObVIO *)edata);

        // The write to the output vconnection completed. This
        // could only be the case if the data being fed into us
        // has also completed.
        ob_assert(0 == write_vio_.ntodo());

        output_vc_->do_io_shutdown(IO_SHUTDOWN_WRITE);
        break;

      case VC_EVENT_WRITE_READY:
      default:
      {
        int64_t towrite = 0;
        int64_t avail = 0;
        int64_t written_len = 0;

        ob_assert(NULL != output_vc_);

        if (NULL == output_vio_) {
          output_buf_ = new_empty_miobuffer();
          output_reader_ = output_buf_->alloc_reader();
          output_vio_ = output_vc_->do_io_write(this, write_vio_.nbytes_, output_reader_);
        }

        MUTEX_TRY_LOCK(trylock, write_vio_.mutex_, this_ethread());
        if (!trylock.is_locked()) {
          retry(10);
          break;
        } else if (closed_) {
          break;
        } else if (ObVIO::NONE == write_vio_.op_) {
          output_vio_->nbytes_ = write_vio_.ndone_;
          output_vio_->reenable();
          break;
        }

        towrite = write_vio_.ntodo();
        if (towrite > 0) {
          avail = write_vio_.get_reader()->read_avail();
          if (towrite > avail) {
            towrite = avail;
          }

          if (towrite > 0) {
            int ret = OB_SUCCESS;
            DEBUG_TRANSFORM("transform", "[ObNullTransform::handle_event] "
                            "writing %ld bytes to output", towrite);
            ret = output_buf_->write(write_vio_.get_reader(), towrite, written_len);
            if (OB_FAIL(ret) || towrite != written_len) {
              PROXY_TRANSFORM_LOG(WDIAG, "fail to write iobuffer from reader",
                                  K(towrite), K(written_len), K(ret));
            }

            if (OB_FAIL(write_vio_.get_reader()->consume(towrite))) {
              PROXY_TRANSFORM_LOG(WDIAG, "fail to consume ", K(towrite), K(ret));
            }
            write_vio_.ndone_ += towrite;
          }
        }

        if (write_vio_.ntodo() > 0) {
          if (towrite > 0) {
            output_vio_->reenable();
            write_vio_.cont_->handle_event(VC_EVENT_WRITE_READY, &write_vio_);
          }
        } else {
          output_vio_->nbytes_ = write_vio_.ndone_;
          output_vio_->reenable();
          write_vio_.cont_->handle_event(VC_EVENT_WRITE_COMPLETE, &write_vio_);
        }

        break;
      }
    }
  }

  return 0;
}

/**
 * Reasons the JG transform cannot currently be a plugin:
 *  a) Uses the config system
 *     - Easily avoided by using the plugin.config file to pass the config
 *       values as parameters to the plugin initialization routine.
 *  b) Uses the stat system
 *     - FIXME: should probably solve this.
 */

#if OB_HAS_TESTS
void ObTransformTest::run()
{
  g_event_processor.schedule_imm(new (std::nothrow) ObTransformControl(), ET_NET);
}
#endif

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
