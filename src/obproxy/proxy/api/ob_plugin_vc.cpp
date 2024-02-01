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
 * **************************************************************
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
 *
 * **************************************************************
 *
 * Since data is transfered within Traffic Server, this is a two
 * headed beast.  One NetVC on initiating side (active side) and
 * one NetVC on the receiving side (passive side).
 *
 * The two NetVC subclasses, ObPluginVC, are part ObPluginVCCore object.  All
 * three objects share the same mutex.  That mutex is required
 * for doing operations that affect the shared buffers,
 * read state from the ObPluginVC on the other side or deal with deallocation.
 *
 * To simplify the code, all data passing through the system goes initially
 * into a shared buffer.  There are two shared buffers, one for each
 * direction of the connection.  While it's more efficient to transfer
 * the data from one buffer to another directly, this creates a lot
 * of tricky conditions since you must be holding the lock for both
 * sides, in additional this VC's lock.  Additionally, issues like
 * watermarks are very hard to deal with.  Since we try to
 * to move data by ObIOBufferData references the efficiency penalty shouldn't
 * be too bad and if it is a big penalty, a brave soul can reimplement
 * to move the data directly without the intermediate buffer.
 *
 * Locking is difficult issue for this multi-headed beast.  In each
 * ObPluginVC, there a two locks. The one we got from our ObPluginVCCore and
 * the lock from the state machine using the ObPluginVC.  The read side
 * lock & the write side lock must be the same.  The regular net processor has
 * this constraint as well.  In order to handle scheduling of retry events cleanly,
 * we have two event pointers, one for each lock.  sm_lock_retry_event can only
 * be changed while holding the using state machine's lock and
 * core_lock_retry_event can only be manipulated while holding the ObPluginVC's
 * lock.  On entry to ObPluginVC::main_handler, we obtain all the locks
 * before looking at the events.  If we can't get all the locks
 * we reschedule the event for further retries.  Since all the locks are
 * obtained in the beginning of the handler, we know we are running
 * exclusively in the later parts of the handler and we will
 * be free from do_io or reenable calls on the ObPluginVC.
 *
 * The assumption is made (consistent with IO Core spec) that any close,
 * shutdown, reenable, or do_io_{read,write) operation is done by the callee
 * while holding the lock for that side of the operation.
 *
 */

#include "proxy/api/ob_plugin_vc.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace event;
using namespace net;
using namespace common;

#define PVC_LOCK_RETRY_TIME HRTIME_MSECONDS(1)
#define PVC_DEFAULT_MAX_BYTES 32768L
#define MIN_BLOCK_TRANSFER_BYTES 128

#define EVENT_PTR_LOCKED (void*) 0x1
#define EVENT_PTR_CLOSED (void*) 0x2

#define PVC_TYPE    ((PLUGIN_VC_ACTIVE == vc_type_) ? "Active" : "Passive")

#define DEBUG_PVC(tag, fmt...) _PROXY_PVC_LOG(DEBUG, ##fmt)
#define ERROR_PVC(tag, fmt...) _PROXY_PVC_LOG(EDIAG, ##fmt)

ObPluginVC::ObPluginVC(ObPluginVCCore *core_obj)
    : ObNetVConnection(), magic_(PLUGIN_VC_MAGIC_ALIVE),
      vc_type_(PLUGIN_VC_UNKNOWN), core_obj_(core_obj),
      other_side_(NULL), read_state_(), write_state_(),
      need_read_process_(false), need_write_process_(false),
      closed_(false), sm_lock_retry_event_(NULL),
      core_lock_retry_event_(NULL),
      deletable_(false), reentrancy_count_(0),
      active_timeout_(0), active_event_(NULL),
      inactive_timeout_(0), inactive_timeout_at_(0),
      inactive_event_(NULL), plugin_tag_(NULL), plugin_id_(0)
{
  ob_assert(NULL != core_obj);
  SET_HANDLER(&ObPluginVC::main_handler);
}

ObPluginVC::~ObPluginVC()
{
}

int ObPluginVC::main_handler(int event, void *data)
{

  DEBUG_PVC("pvc_event", "[%ld] %s: Received event %d",
            core_obj_->id_, PVC_TYPE, event);

  ob_release_assert(EVENT_INTERVAL == event || EVENT_IMMEDIATE == event);
  ob_release_assert(PLUGIN_VC_MAGIC_ALIVE == magic_);
  ob_assert(!deletable_);
  ob_assert(NULL != data);
  ob_assert(NULL != read_state_.vio_.mutex_);
  ob_assert(NULL != write_state_.vio_.mutex_);

  ObEvent *call_event = (ObEvent *)data;
  ObEThread *my_ethread = mutex_->thread_holding_;
  ob_release_assert(NULL != my_ethread);

  ObPtr<ObProxyMutex> read_side_mutex = read_state_.vio_.mutex_;
  ObPtr<ObProxyMutex> write_side_mutex = write_state_.vio_.mutex_;

  MUTEX_TRY_LOCK(read_lock, read_side_mutex, my_ethread);
  MUTEX_TRY_LOCK(write_lock, write_side_mutex, my_ethread);

  if (!read_lock.is_locked() || read_side_mutex.ptr_ != read_state_.vio_.mutex_.ptr_
      || !write_lock.is_locked() || write_side_mutex.ptr_ != write_state_.vio_.mutex_.ptr_) {
    if (call_event != inactive_event_) {
      call_event->schedule_in(PVC_LOCK_RETRY_TIME);
    }

  } else if (closed_) {
    process_close();

  } else {
    // We've got all the locks so there should not be any
    // other calls active
    ob_release_assert(0 == reentrancy_count_);

    // We can get closed while we're calling back the
    // continuation. Set the reentrancy count so we know
    // we could be calling the continuation and that we
    // need to defer close processing
    ++reentrancy_count_;

    if (call_event == active_event_) {
      process_timeout(&active_event_, VC_EVENT_ACTIVE_TIMEOUT);
    } else if (call_event == inactive_event_) {
      if (inactive_timeout_at_ && inactive_timeout_at_ < get_hrtime()) {
        process_timeout(&inactive_event_, VC_EVENT_INACTIVITY_TIMEOUT);
        call_event->cancel();
      }
    } else {
      if (call_event == sm_lock_retry_event_) {
        sm_lock_retry_event_ = NULL;
      } else {
        ob_release_assert(call_event == core_lock_retry_event_);
        core_lock_retry_event_ = NULL;
      }

      if (need_read_process_) {
        process_read_side(false);
      }

      if (need_write_process_ && !closed_) {
        process_write_side(false);
      }

    }

    --reentrancy_count_;
    if (closed_) {
      process_close();
    }
  }
  return 0;
}

ObVIO *ObPluginVC::do_io_read(ObContinuation *c, const int64_t nbytes, ObMIOBuffer *buf)
{

  ob_assert(!closed_);
  ob_assert(PLUGIN_VC_MAGIC_ALIVE == magic_);

  if (NULL != buf) {
    read_state_.vio_.buffer_.writer_for(buf);
  } else {
    read_state_.vio_.buffer_.destroy();
  }

  // Note: we set vio.op last because process_read_side looks at it to
  // tell if the ObVConnection is active.
  read_state_.vio_.mutex_ = (NULL != c) ? c->mutex_ : mutex_;
  read_state_.vio_.cont_ = c;
  read_state_.vio_.nbytes_ = nbytes;
  read_state_.vio_.ndone_ = 0;
  read_state_.vio_.vc_server_ = (ObVConnection *)this;
  read_state_.vio_.op_ = ObVIO::READ;

  DEBUG_PVC("pvc", "[%ld] %s: do_io_read for %ld bytes",
            core_obj_->id_, PVC_TYPE, nbytes);

  // Since reentrant callbacks are not allowed on from do_io
  // functions schedule ourselves get on a different stack
  need_read_process_ = true;
  setup_event_cb(0, &sm_lock_retry_event_);

  return &read_state_.vio_;
}

ObVIO *ObPluginVC::do_io_write(ObContinuation *c, const int64_t nbytes,
                               ObIOBufferReader *abuffer)
{
  ob_assert(!closed_);
  ob_assert(PLUGIN_VC_MAGIC_ALIVE == magic_);

  if (NULL != abuffer) {
    write_state_.vio_.buffer_.reader_for(abuffer);
  } else {
    write_state_.vio_.buffer_.destroy();
  }

  // Note: we set vio.op last because process_write_side looks at it to
  // tell if the ObVConnection is active.
  write_state_.vio_.mutex_ = (NULL != c) ? c->mutex_ : mutex_;
  write_state_.vio_.cont_ = c;
  write_state_.vio_.nbytes_ = nbytes;
  write_state_.vio_.ndone_ = 0;
  write_state_.vio_.vc_server_ = (ObVConnection *)this;
  write_state_.vio_.op_ = ObVIO::WRITE;

  DEBUG_PVC("pvc", "[%ld] %s: do_io_write for %ld bytes", core_obj_->id_, PVC_TYPE, nbytes);

  // Since reentrant callbacks are not allowed on from do_io
  // functions schedule ourselves get on a different stack
  need_write_process_ = true;
  setup_event_cb(0, &sm_lock_retry_event_);

  return &write_state_.vio_;
}

void ObPluginVC::reenable(ObVIO *vio)
{

  ob_assert(!closed_);
  ob_assert(PLUGIN_VC_MAGIC_ALIVE == magic_);
  ob_assert(vio->mutex_->thread_holding_ == this_ethread());

  DEBUG_PVC("pvc", "[%ld] %s: reenable %s", core_obj_->id_, PVC_TYPE,
            (ObVIO::WRITE == vio->op_) ? "Write" : "Read");

  if (ObVIO::WRITE == vio->op_) {
    ob_assert(vio == &write_state_.vio_);
    need_write_process_ = true;
  } else if (ObVIO::READ == vio->op_) {
    need_read_process_ = true;
  } else {
    ob_release_assert(0);
  }

  setup_event_cb(0, &sm_lock_retry_event_);
}

void ObPluginVC::reenable_re(ObVIO *vio)
{
  ob_assert(!closed_);
  ob_assert(PLUGIN_VC_MAGIC_ALIVE == magic_);
  ob_assert(vio->mutex_->thread_holding_ == this_ethread());

  DEBUG_PVC("pvc", "[%ld] %s: reenable_re %s", core_obj_->id_, PVC_TYPE,
            (ObVIO::WRITE == vio->op_) ? "Write" : "Read");

  MUTEX_TRY_LOCK(lock, this->mutex_, this_ethread());
  if (!lock.is_locked()) {
    if (ObVIO::WRITE == vio->op_) {
      need_write_process_ = true;
    } else {
      need_read_process_ = true;
    }

    setup_event_cb(PVC_LOCK_RETRY_TIME, &sm_lock_retry_event_);
  } else {
    ++reentrancy_count_;

    if (ObVIO::WRITE == vio->op_) {
      ob_assert(vio == &write_state_.vio_);
      process_write_side(false);
    } else if (ObVIO::READ == vio->op_) {
      ob_assert(vio == &read_state_.vio_);
      process_read_side(false);
    } else {
      ob_release_assert(0);
    }

    --reentrancy_count_;

    // To process the close, we need the lock
    // for the ObPluginVC. Schedule an event
    // to make sure we get it
    if (closed_) {
      setup_event_cb(0, &sm_lock_retry_event_);
    }
  }
}

void ObPluginVC::do_io_close(const int /* flag */)
{
  ob_assert(!closed_);
  ob_assert(PLUGIN_VC_MAGIC_ALIVE == magic_);

  DEBUG_PVC("pvc", "[%ld] %s: do_io_close", core_obj_->id_, PVC_TYPE);

  if (reentrancy_count_ > 0) {
    // Do nothing since dealloacting ourselves
    // now will lead to us running on a dead
    // ObPluginVC since we are being called
    // reentrantly
    closed_ = true;
  } else {
    MUTEX_TRY_LOCK(lock, mutex_, this_ethread());

    if (!lock.is_locked()) {
      setup_event_cb(PVC_LOCK_RETRY_TIME, &sm_lock_retry_event_);
      closed_ = true;
    } else {
      closed_ = true;
    }
    process_close();
  }
}

void ObPluginVC::do_io_shutdown(const ShutdownHowToType howto)
{
  ob_assert(!closed_);
  ob_assert(PLUGIN_VC_MAGIC_ALIVE == magic_);

  switch (howto) {
    case IO_SHUTDOWN_READ:
      read_state_.shutdown_ = true;
      break;

    case IO_SHUTDOWN_WRITE:
      write_state_.shutdown_ = true;
      break;

    case IO_SHUTDOWN_READWRITE:
      read_state_.shutdown_ = true;
      write_state_.shutdown_ = true;
      break;
  }
}

// Takes care of transferring bytes from a reader to another buffer
//    In the case of large transfers, we move blocks.  In the case
//    of small transfers we copy data so as to not build too many
//    buffer blocks
//
// Args:
//   transfer_to:  buffer to copy to
//   transfer_from:  buffer_copy_from
//   act_on: is the max number of bytes we are to copy.  There must
//          be at least act_on bytes available from transfer_from
//
// Returns number of bytes transfered
int64_t ObPluginVC::transfer_bytes(ObMIOBuffer *transfer_to, ObIOBufferReader *transfer_from, int64_t act_on)
{
  int ret = OB_SUCCESS;
  int64_t total_added = 0;
  ob_assert(act_on <= transfer_from->read_avail());

  while (act_on > 0) {
    int64_t block_read_avail = transfer_from->block_read_avail();
    int64_t to_move = MIN(act_on, block_read_avail);
    int64_t moved = 0;

    if (to_move <= 0) {
      break;
    }

    if (to_move >= MIN_BLOCK_TRANSFER_BYTES) {
      ret = transfer_to->write(transfer_from, to_move, moved, 0);
    } else {
      // We have a really small amount of data.  To make
      // sure we don't get a huge build up of blocks which
      // can lead to stack overflows if the buffer is destroyed
      // before we read from it, we need copy over to the new
      // buffer instead of doing a block transfer
      ret = transfer_to->write(transfer_from->start(), to_move, moved);

      if (0 == moved) {
        // We are out of buffer space
        break;
      }
    }

    act_on -= moved;
    if (OB_FAIL(transfer_from->consume(moved))) {
      PROXY_TRANSFORM_LOG(WDIAG, "fail to consume", K(moved), K(ret));
    }

    total_added += moved;
  }

  return total_added;
}

// This function may only be called while holding
// this->mutex_ & while it is ok to callback the
// write side continuation
//
// Does write side processing
void ObPluginVC::process_write_side(bool other_side_call)
{
  ob_assert(!deletable_);
  ob_assert(PLUGIN_VC_MAGIC_ALIVE == magic_);

  ObMIOBuffer *core_buffer = (PLUGIN_VC_ACTIVE == vc_type_)
    ? core_obj_->a_to_p_buffer_ : core_obj_->p_to_a_buffer_;
  need_write_process_ = false;

  if (ObVIO::WRITE != write_state_.vio_.op_ || closed_ || write_state_.shutdown_) {
    // nothing to do
  } else {
    // Acquire the lock of the write side continuation
    ObEThread *my_ethread = mutex_->thread_holding_;
    ob_assert(NULL != my_ethread);
    MUTEX_TRY_LOCK(lock, write_state_.vio_.mutex_, my_ethread);
    if (!lock.is_locked()) {
      DEBUG_PVC("pvc_event", "[%ld] %s: process_write_side lock miss, retrying",
                core_obj_->id_, PVC_TYPE);

      need_write_process_ = true;
      setup_event_cb(PVC_LOCK_RETRY_TIME, &core_lock_retry_event_);
    } else {
      DEBUG_PVC("pvc", "[%ld] %s: process_write_side", core_obj_->id_, PVC_TYPE);
      need_write_process_ = false;

      // Check the state of our write buffer as well as ntodo
      int64_t ntodo = write_state_.vio_.ntodo();
      ObIOBufferReader *reader = write_state_.vio_.get_reader();
      int64_t bytes_avail = reader->read_avail();
      int64_t act_on = MIN(bytes_avail, ntodo);
      int64_t buf_space = PVC_DEFAULT_MAX_BYTES - core_buffer->max_read_avail();
      int64_t added = 0;

      DEBUG_PVC("pvc", "[%ld] %s: process_write_side; act_on %ld",
                core_obj_->id_, PVC_TYPE, act_on);
      if (0 == ntodo) {
        // nothing to do
      }else if (other_side_->closed_ || other_side_->read_state_.shutdown_) {
        write_state_.vio_.cont_->handle_event(VC_EVENT_ERROR, &write_state_.vio_);
      } else if (act_on <= 0) {
        if (ntodo > 0) {
          // Notify the continuation that we are "disabling"
          // ourselves due to nothing to write
          write_state_.vio_.cont_->handle_event(VC_EVENT_WRITE_READY, &write_state_.vio_);
        }
      }
      // Bytes available, try to transfer to the ObPluginVCCore
      // intermediate buffer
      else if (buf_space <= 0) {
        DEBUG_PVC("pvc", "[%ld] %s: process_write_side no buffer space",
                  core_obj_->id_, PVC_TYPE);
      } else if ((added = transfer_bytes(core_buffer, reader,
                                         std::min(act_on, buf_space))) < 0) {
        // Couldn't actually get the buffer space.  This only
        // happens on small transfers with the above
        // PVC_DEFAULT_MAX_BYTES factor doesn't apply
        DEBUG_PVC("pvc", "[%ld] %s: process_write_side out of buffer space",
                  core_obj_->id_, PVC_TYPE);
      } else {
        write_state_.vio_.ndone_ += added;

        DEBUG_PVC("pvc", "[%ld] %s: process_write_side; added %ld",
                  core_obj_->id_, PVC_TYPE, added);

        if (0 == write_state_.vio_.ntodo()) {
          write_state_.vio_.cont_->handle_event(VC_EVENT_WRITE_COMPLETE, &write_state_.vio_);
        } else {
          write_state_.vio_.cont_->handle_event(VC_EVENT_WRITE_READY, &write_state_.vio_);
        }

        update_inactive_time();

        // Wake up the read side on the other side to process these bytes
        if (!other_side_->closed_) {
          if (!other_side_call) {
            other_side_->process_read_side(true);
          } else {
            other_side_->read_state_.vio_.reenable();
          }
        }
      }
    }
  }
}

// This function may only be called while holding
// this->mutex_ & while it is ok to callback the
// read side continuation
//
// Does read side processing
void ObPluginVC::process_read_side(bool other_side_call)
{
  ob_assert(!deletable_);
  ob_assert(PLUGIN_VC_MAGIC_ALIVE == magic_);

  ObIOBufferReader *core_reader = NULL;

  if (PLUGIN_VC_ACTIVE == vc_type_) {
    core_reader = core_obj_->p_to_a_reader_;
  } else {
    ob_assert(PLUGIN_VC_PASSIVE == vc_type_);
    core_reader = core_obj_->a_to_p_reader_;
  }

  need_read_process_ = false;

  if (ObVIO::READ != read_state_.vio_.op_ || closed_) {
    // nothing to do
  } else {
    // Acquire the lock of the read side continuation
    ObEThread *my_ethread = mutex_->thread_holding_;
    ob_assert(NULL != my_ethread);
    MUTEX_TRY_LOCK(lock, read_state_.vio_.mutex_, my_ethread);
    if (!lock.is_locked()) {
      DEBUG_PVC("pvc_event", "[%ld] %s: process_read_side lock miss, retrying",
                core_obj_->id_, PVC_TYPE);

      need_read_process_ = true;
      setup_event_cb(PVC_LOCK_RETRY_TIME, &core_lock_retry_event_);
    }
    // Check read_state.shutdown after the lock has been obtained.
    else if (read_state_.shutdown_) {
      // nothing to do
    } else {
      DEBUG_PVC("pvc", "[%ld] %s: process_read_side", core_obj_->id_, PVC_TYPE);
      need_read_process_ = false;

      // Check the state of our read buffer as well as ntodo
      int64_t ntodo = read_state_.vio_.ntodo();
      int64_t bytes_avail = core_reader->read_avail();
      int64_t act_on = std::min(bytes_avail, ntodo);

      ObMIOBuffer *output_buffer = read_state_.vio_.get_writer();
      int64_t water_mark = std::max(output_buffer->water_mark_, PVC_DEFAULT_MAX_BYTES);
      int64_t buf_space = water_mark - output_buffer->max_read_avail();
      int64_t added = 0;

      DEBUG_PVC("pvc", "[%ld] %s: process_read_side; act_on %ld",
                core_obj_->id_, PVC_TYPE, act_on);
      if (0 == ntodo) {
        // nothing to do
      } else if (act_on <= 0) {
        if (other_side_->closed_ || other_side_->write_state_.shutdown_) {
          read_state_.vio_.cont_->handle_event(VC_EVENT_EOS, &read_state_.vio_);
        }
      }
      // Bytes available, try to transfer from the ObPluginVCCore
      // intermediate buffer
      else if (buf_space <= 0) {
        DEBUG_PVC("pvc", "[%ld] %s: process_read_side no buffer space",
                  core_obj_->id_, PVC_TYPE);
      } else if ((added = transfer_bytes(output_buffer, core_reader,
                                         std::min(act_on, buf_space))) <= 0) {
        // Couldn't actually get the buffer space.  This only
        // happens on small transfers with the above
        // PVC_DEFAULT_MAX_BYTES factor doesn't apply
        DEBUG_PVC("pvc", "[%ld] %s: process_read_side out of buffer space",
                  core_obj_->id_, PVC_TYPE);
      } else {
        read_state_.vio_.ndone_ += added;
        DEBUG_PVC("pvc", "[%ld] %s: process_read_side; added %ld",
                  core_obj_->id_, PVC_TYPE, added);

        if (0 == read_state_.vio_.ntodo()) {
          read_state_.vio_.cont_->handle_event(VC_EVENT_READ_COMPLETE, &read_state_.vio_);
        } else {
          read_state_.vio_.cont_->handle_event(VC_EVENT_READ_READY, &read_state_.vio_);
        }

        update_inactive_time();

        // Wake up the other side so it knows there is space available in
        // intermediate buffer
        if (!other_side_->closed_) {
          if (!other_side_call) {
            other_side_->process_write_side(true);
          } else {
            other_side_->write_state_.vio_.reenable();
          }
        }
      }
    }
  }
}

// This function may only be called while holding
// this->mutex_
//
// Tries to close the and dealloc the vc
void ObPluginVC::process_close()
{
  ob_assert(PLUGIN_VC_MAGIC_ALIVE == magic_);

  DEBUG_PVC("pvc", "[%ld] %s: process_close", core_obj_->id_, PVC_TYPE);

  if (!deletable_) {
    deletable_ = true;
  }

  if (sm_lock_retry_event_) {
    sm_lock_retry_event_->cancel();
    sm_lock_retry_event_ = NULL;
  }

  if (core_lock_retry_event_) {
    core_lock_retry_event_->cancel();
    core_lock_retry_event_ = NULL;
  }

  if (active_event_) {
    active_event_->cancel();
    active_event_ = NULL;
  }

  if (inactive_event_) {
    inactive_event_->cancel();
    inactive_event_ = NULL;
    inactive_timeout_at_ = 0;
  }

  // If the other side of the ObPluginVC is not closed
  // we need to force it process both living sides
  // of the connection in order that it recognizes
  // the close
  if (!other_side_->closed_ && core_obj_->connected_) {
    other_side_->need_write_process_ = true;
    other_side_->need_read_process_ = true;
    other_side_->setup_event_cb(0, &other_side_->core_lock_retry_event_);
  }

  core_obj_->attempt_delete();
}

// Handles sending timeout event to the ObVConnection. e is the event we got
// which indicates the timeout. event_to_send is the event to the
// vc user. our_eptr is a pointer to either inactive_event,
// or active_event. If we successfully send the timeout to vc user,
// we clear the pointer, otherwise we reschedule it.
//
// Because the possibility of reentrant close from vc user, we don't want to
// touch any state after making the call back
void ObPluginVC::process_timeout(ObEvent **e, int event_to_send)
{
  ob_assert(*e == inactive_event_ || *e == active_event_);

  if (ObVIO::READ == read_state_.vio_.op_
      && !read_state_.shutdown_ && read_state_.vio_.ntodo() > 0) {
    MUTEX_TRY_LOCK(lock, read_state_.vio_.mutex_, (*e)->ethread_);
    if (!lock.is_locked()) {
      (*e)->schedule_in(PVC_LOCK_RETRY_TIME);
    } else {
      *e = NULL;
      read_state_.vio_.cont_->handle_event(event_to_send, &read_state_.vio_);
    }
  } else if (ObVIO::WRITE == write_state_.vio_.op_
             && !write_state_.shutdown_ && write_state_.vio_.ntodo() > 0) {
    MUTEX_TRY_LOCK(lock, write_state_.vio_.mutex_, (*e)->ethread_);
    if (!lock.is_locked()) {
      (*e)->schedule_in(PVC_LOCK_RETRY_TIME);
    } else {
      *e = NULL;
      write_state_.vio_.cont_->handle_event(event_to_send, &write_state_.vio_);
    }
  } else {
    *e = NULL;
  }
}

void ObPluginVC::update_inactive_time()
{
  if (inactive_event_ && inactive_timeout_) {
    //inactive_event->cancel();
    //inactive_event = g_event_processor.schedule_in(this, inactive_timeout);
    inactive_timeout_at_ = get_hrtime() + inactive_timeout_;
  }
}


// Setup up the event processor to call us back.
// We've got two different event pointers to handle
// locking issues
void ObPluginVC::setup_event_cb(ObHRTime in, ObEvent **e_ptr)
{
  ob_assert(PLUGIN_VC_MAGIC_ALIVE == magic_);

  if (NULL == *e_ptr) {

    // We locked the pointer so we can now allocate an event
    // to call us back
    if (0 == in) {
      if (REGULAR == this_ethread()->tt_) {
        *e_ptr = this_ethread()->schedule_imm_local(this);
      } else {
        *e_ptr = g_event_processor.schedule_imm(this);
      }
    } else {
      if (REGULAR == this_ethread()->tt_) {
        *e_ptr = this_ethread()->schedule_in_local(this, in);
      } else {
        *e_ptr = g_event_processor.schedule_in(this, in);
      }
    }
  }
}

int ObPluginVC::set_active_timeout(ObHRTime timeout_in)
{
  active_timeout_ = timeout_in;

  // FIX - Do we need to handle the case where the timeout is set
  // but no io has been done?
  if (NULL != active_event_) {
    ob_assert(!active_event_->cancelled_);
    active_event_->cancel();
    active_event_ = NULL;
  }

  if (active_timeout_ > 0) {
    if (REGULAR == this_ethread()->tt_) {
      active_event_ = this_ethread()->schedule_in(this, active_timeout_);
    } else {
      active_event_ = g_event_processor.schedule_in(this, active_timeout_);
    }
  }
  return OB_SUCCESS;
}

void ObPluginVC::set_inactivity_timeout(ObHRTime timeout_in)
{
  inactive_timeout_ = timeout_in;
  if (0 != inactive_timeout_) {
    inactive_timeout_at_ = get_hrtime() + inactive_timeout_;
    if (NULL == inactive_event_) {
      if (REGULAR == this_ethread()->tt_) {
        inactive_event_ = this_ethread()->schedule_every(this, HRTIME_SECONDS(1));
      } else {
        inactive_event_ = g_event_processor.schedule_every(this, HRTIME_SECONDS(1));
      }
    }
  } else {
    inactive_timeout_at_ = 0;
    if (NULL != inactive_event_) {
      inactive_event_->cancel();
      inactive_event_ = NULL;
    }
  }
}

void ObPluginVC::cancel_active_timeout()
{
  set_active_timeout(0);
}

void ObPluginVC::cancel_inactivity_timeout()
{
  set_inactivity_timeout(0);
}

ObHRTime ObPluginVC::get_active_timeout() const
{
  return active_timeout_;
}

ObHRTime ObPluginVC::get_inactivity_timeout() const
{
  return inactive_timeout_;
}

void ObPluginVC::add_to_keep_alive_lru()
{
  // do nothing
}

void ObPluginVC::remove_from_keep_alive_lru()
{
  // do nothing
}

int ObPluginVC::get_socket()
{
  return 0;
}

int ObPluginVC::set_local_addr()
{
  if (PLUGIN_VC_ACTIVE == vc_type_) {
    ops_ip_copy(local_addr_, core_obj_->active_addr_struct_);
  } else {
    ops_ip_copy(local_addr_, core_obj_->passive_addr_struct_);
  }
  return OB_SUCCESS;
}

bool ObPluginVC::set_remote_addr()
{
  if (PLUGIN_VC_ACTIVE == vc_type_) {
    ops_ip_copy(remote_addr_, core_obj_->passive_addr_struct_);
  } else {
    ops_ip_copy(remote_addr_, core_obj_->active_addr_struct_);
  }
  return true;
}

int ObPluginVC::set_tcp_init_cwnd(int /* init_cwnd */)
{
  return -1;
}

int ObPluginVC::apply_options()
{
  // do nothing
  return OB_SUCCESS;
}

bool ObPluginVC::get_data(const int32_t id, void *data)
{
  bool ret = false;
  if (NULL != data) {
    switch (id) {
      case PLUGIN_VC_DATA_LOCAL:
        if (PLUGIN_VC_ACTIVE == vc_type_) {
          *(void **)data = core_obj_->active_data_;
        } else {
          *(void **)data = core_obj_->passive_data_;
        }
        ret = true;
        break;

      case PLUGIN_VC_DATA_REMOTE:
        if (PLUGIN_VC_ACTIVE == vc_type_) {
          *(void **)data = core_obj_->passive_data_;
        } else {
          *(void **)data = core_obj_->active_data_;
        }
        ret = true;
        break;

      default:
        *(void **)data = NULL;
        ret = false;
        break;
    }
  }
  return ret;
}

bool ObPluginVC::set_data(const int32_t id, void *data)
{
  bool ret = false;
  switch (id) {
    case PLUGIN_VC_DATA_LOCAL:
      if (PLUGIN_VC_ACTIVE == vc_type_) {
        core_obj_->active_data_ = data;
      } else {
        core_obj_->passive_data_ = data;
      }
      ret = true;
      break;

    case PLUGIN_VC_DATA_REMOTE:
      if (PLUGIN_VC_ACTIVE == vc_type_) {
        core_obj_->passive_data_ = data;
      } else {
        core_obj_->active_data_ = data;
      }
      ret = true;
      break;

    default:
      ret = false;
      break;
  }
  return ret;
}

// ObPluginVCCore
volatile int64_t ObPluginVCCore::g_nextid = 0;

ObPluginVCCore::~ObPluginVCCore()
{
}

ObPluginVCCore *ObPluginVCCore::alloc()
{
  ObPluginVCCore *pvc = op_reclaim_alloc(ObPluginVCCore);
  if (NULL == pvc) {
    ERROR_PVC("pvc", "failed to allocate memory for ObPluginVCCore");
  } else {
    pvc->init();
  }
  return pvc;
}

void ObPluginVCCore::init()
{
  mutex_ = new_proxy_mutex();

  active_vc_.vc_type_ = PLUGIN_VC_ACTIVE;
  active_vc_.other_side_ = &passive_vc_;
  active_vc_.core_obj_ = this;
  active_vc_.mutex_ = mutex_;
  active_vc_.thread_ = this_ethread();

  passive_vc_.vc_type_ = PLUGIN_VC_PASSIVE;
  passive_vc_.other_side_ = &active_vc_;
  passive_vc_.core_obj_ = this;
  passive_vc_.mutex_ = mutex_;
  passive_vc_.thread_ = active_vc_.thread_;

  p_to_a_buffer_ = new_miobuffer(BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K));
  p_to_a_reader_ = p_to_a_buffer_->alloc_reader();

  a_to_p_buffer_ = new_miobuffer(BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K));
  a_to_p_reader_ = a_to_p_buffer_->alloc_reader();

  DEBUG_PVC("pvc", "[%ld] Created ObPluginVCCore at %p, active %p, passive %p",
            id_, this, &active_vc_, &passive_vc_);
}

void ObPluginVCCore::destroy()
{

  DEBUG_PVC("pvc", "[%ld] Destroying ObPluginVCCore at %p", id_, this);

  ob_assert(active_vc_.closed_|| !connected_);
  active_vc_.mutex_.release();
  active_vc_.read_state_.vio_.buffer_.destroy();
  active_vc_.write_state_.vio_.buffer_.destroy();
  active_vc_.magic_ = PLUGIN_VC_MAGIC_DEAD;

  ob_assert(passive_vc_.closed_ || !connected_);
  passive_vc_.mutex_.release();
  passive_vc_.read_state_.vio_.buffer_.destroy();
  passive_vc_.write_state_.vio_.buffer_.destroy();
  passive_vc_.magic_ = PLUGIN_VC_MAGIC_DEAD;

  if (NULL != p_to_a_buffer_) {
    free_miobuffer(p_to_a_buffer_);
    p_to_a_buffer_ = NULL;
  }

  if (NULL != a_to_p_buffer_) {
    free_miobuffer(a_to_p_buffer_);
    a_to_p_buffer_ = NULL;
  }

  mutex_.release();
  op_reclaim_free(this);
}

void ObPluginVCCore::set_accept_cont(ObContinuation *c)
{
  connect_to_ = c;
  // FIX ME - must return action
}

ObPluginVC *ObPluginVCCore::connect()
{
  ObPluginVC *ret = NULL;

  // Make sure there is another end to connect to
  if (NULL != connect_to_) {
    connected_ = true;
    state_send_accept(EVENT_IMMEDIATE, NULL);

    ret = &active_vc_;
  }
  return ret;
}

int ObPluginVCCore::connect_re(ObContinuation *c, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  // Make sure there is another end to connect to
  if (NULL != connect_to_) {
    ObEThread *my_thread = this_ethread();
    MUTEX_LOCK(lock, this->mutex_, my_thread);

    connected_ = true;
    state_send_accept(EVENT_IMMEDIATE, NULL);

    // We have to take out our mutex_ because rest of the
    // system expects the VC mutex_ to held when calling back.
    // We can use take lock here instead of try lock because the
    // lock should never already be held.

    c->handle_event(NET_EVENT_OPEN, &active_vc_);
  }
  return ret;
}

int ObPluginVCCore::state_send_accept_failed(int event, void *data)
{
  UNUSED(event);
  UNUSED(data);
  MUTEX_TRY_LOCK(lock, connect_to_->mutex_, this_ethread());
  if (lock.is_locked()) {
    connect_to_->handle_event(NET_EVENT_ACCEPT_FAILED, NULL);
    destroy();
  } else {
    SET_HANDLER(&ObPluginVCCore::state_send_accept_failed);
    if (REGULAR == this_ethread()->tt_) {
      this_ethread()->schedule_in(this, PVC_LOCK_RETRY_TIME);
    } else {
      g_event_processor.schedule_in(this, PVC_LOCK_RETRY_TIME);
    }
  }

  return 0;
}

int ObPluginVCCore::state_send_accept(int event, void *data)
{
  UNUSED(event);
  UNUSED(data);
  MUTEX_TRY_LOCK(lock, connect_to_->mutex_, this_ethread());
  if (lock.is_locked()) {
    connect_to_->handle_event(NET_EVENT_ACCEPT, &passive_vc_);
  } else {
    SET_HANDLER(&ObPluginVCCore::state_send_accept);
    if (REGULAR == this_ethread()->tt_) {
      this_ethread()->schedule_in(this, PVC_LOCK_RETRY_TIME);
    } else {
      g_event_processor.schedule_in(this, PVC_LOCK_RETRY_TIME);
    }
  }

  return 0;
}

// ObMutex must be held when calling this function
void ObPluginVCCore::attempt_delete()
{
  if (active_vc_.deletable_) {
    if (passive_vc_.deletable_) {
      destroy();
    } else if (!connected_) {
      state_send_accept_failed(EVENT_IMMEDIATE, NULL);
    }
  }
}

// Called to kill the ObPluginVCCore when the
// connect call hasn't been made yet
void ObPluginVCCore::kill_no_connect()
{
  ob_assert(!connected_);
  ob_assert(!active_vc_.closed_);
  active_vc_.do_io_close();
}

void ObPluginVCCore::set_passive_addr(in_addr_t ip, int port)
{
  ops_ip4_set(passive_addr_struct_, htonl(ip), (htons)((uint16_t)port));
}

void ObPluginVCCore::set_passive_addr(const sockaddr &ip)
{
  passive_addr_struct_.assign(ip);
}

void ObPluginVCCore::set_active_addr(in_addr_t ip, int port)
{
  ops_ip4_set(active_addr_struct_, htonl(ip), (htons)((uint16_t)port));
}

void ObPluginVCCore::set_active_addr(const sockaddr &ip)
{
  active_addr_struct_.assign(ip);
}

void ObPluginVCCore::set_passive_data(void *data)
{
  passive_data_ = data;
}

void ObPluginVCCore::set_active_data(void *data)
{
  active_data_ = data;
}

void ObPluginVCCore::set_plugin_id(int64_t id)
{
  passive_vc_.plugin_id_ = active_vc_.plugin_id_ = id;
}

void ObPluginVCCore::set_plugin_tag(char const *tag)
{
  passive_vc_.plugin_tag_ = active_vc_.plugin_tag_ = tag;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
