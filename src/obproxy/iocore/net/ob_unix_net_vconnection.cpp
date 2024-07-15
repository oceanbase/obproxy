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
 *
 */

#include "lib/profile/ob_trace_id.h"
#include "iocore/net/ob_net.h"
#include "iocore/net/ob_event_io.h"
#include "iocore/net/ob_vtoa_user.h"
#include "iocore/net/ob_ssl_processor.h"
#include "obutils/ob_proxy_config.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace net
{

static const int64_t NET_MAX_IOV = 16;

static inline ObNetState &get_net_state_by_vio(ObVIO &vio)
{
  return *(reinterpret_cast<ObNetState *>(
    reinterpret_cast<char *>(&vio)
    - reinterpret_cast<int64_t>(&(reinterpret_cast<ObNetState *>(0)->vio_))));
}

// Disable a ObUnixNetVConnection
inline void ObUnixNetVConnection::read_disable()
{
  if (!write_.enabled_) {
    next_inactivity_timeout_at_ = 0;
    PROXY_NET_LOG(DEBUG, "read_disable updating inactivity_at",
                  "inactivity_at", next_inactivity_timeout_at_, K(this));
  }
  read_.enabled_ = false;
  nh_->read_ready_list_.remove(this);
  get_event_io().modify(-EVENTIO_READ);
}

inline void ObUnixNetVConnection::write_disable()
{
  if (!read_.enabled_) {
    next_inactivity_timeout_at_ = 0;
    PROXY_NET_LOG(DEBUG, "write_disable updating inactivity_at",
                  "inactivity_at", next_inactivity_timeout_at_, K(this));
  }
  write_.enabled_ = false;
  nh_->write_ready_list_.remove(this);
  get_event_io().modify(-EVENTIO_WRITE);
}

// Reschedule a ObUnixNetVConnection
// by moving it onto or off of the ready_list
inline void ObUnixNetVConnection::read_reschedule()
{
  if (read_.triggered_ && read_.enabled_) {
    nh_->read_ready_list_.in_or_enqueue(this);
  } else {
    nh_->read_ready_list_.remove(this);
  }
}

inline void ObUnixNetVConnection::write_reschedule()
{
  if (write_.triggered_ && write_.enabled_) {
    nh_->write_ready_list_.in_or_enqueue(this);
  } else {
    nh_->write_ready_list_.remove(this);
  }
}

inline int ObUnixNetVConnection::read_signal_and_update(const int event)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_CONT;
  event_record_ = event;
  ++(recursion_);
  if (OB_LIKELY(NULL != read_.vio_.cont_)) {
    ObCurTraceId::set(reinterpret_cast<uint64_t>(read_.vio_.cont_->mutex_.ptr_));
    read_.vio_.cont_->handle_event(event, &read_.vio_);
  } else {
    switch (event) {
    case VC_EVENT_EOS:
    case VC_EVENT_ERROR:
    case VC_EVENT_ACTIVE_TIMEOUT:
    case VC_EVENT_INACTIVITY_TIMEOUT:
    case VC_EVENT_DETECT_SERVER_DEAD:
      PROXY_NET_LOG(DEBUG, "null read.vio cont, closing vc", K(event), K(this));
      closed_ = 1;
      break;
    default:
      closed_ = 1;
      PROXY_NET_LOG(EDIAG, "unexpected event", K(event), K(this));
      break;
    }
  }

  if (0 == --(recursion_) && closed_) {
    if (OB_FAIL(close())) {
      PROXY_NET_LOG(WDIAG, "fail to close unix net vconnection", K(this), K(ret));
    }
    event_ret = EVENT_DONE;
  }
  return event_ret;
}

inline int ObUnixNetVConnection::write_signal_and_update(const int event)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_CONT;
  event_record_ = event;
  ++(recursion_);
  if (OB_LIKELY(NULL != write_.vio_.cont_)) {
    ObCurTraceId::set(reinterpret_cast<uint64_t>(write_.vio_.cont_->mutex_.ptr_));
    write_.vio_.cont_->handle_event(event, &write_.vio_);
  } else {
    switch (event) {
    case VC_EVENT_EOS:
    case VC_EVENT_ERROR:
    case VC_EVENT_ACTIVE_TIMEOUT:
    case VC_EVENT_INACTIVITY_TIMEOUT:
    case VC_EVENT_DETECT_SERVER_DEAD:
      PROXY_NET_LOG(DEBUG, "null write.vio cont, closing vc", K(event), K(this));
      closed_ = 1;
      break;
    default:
      closed_ = 1;
      PROXY_NET_LOG(EDIAG, "unexpected event", K(event), K(this));
      break;
    }
  }

  if (0 == --(recursion_) && closed_) {
    if (OB_FAIL(close())) {
      PROXY_NET_LOG(WDIAG, "fail to close unix net vconnection", K(this), K(ret));
    }
    event_ret = EVENT_DONE;
  }
  return event_ret;
}

inline int ObUnixNetVConnection::read_signal_done(const int event)
{
  int event_ret = EVENT_CONT;
  read_.enabled_ = false;
  if (EVENT_CONT == (event_ret = read_signal_and_update(event))) {
    read_reschedule();
  }
  return event_ret;
}

inline int ObUnixNetVConnection::write_signal_done(const int event)
{
  int event_ret = EVENT_CONT;
  write_.enabled_ = false;
  if (EVENT_CONT == (event_ret = write_signal_and_update(event))) {
    write_reschedule();
  }
  return event_ret;
}

inline int ObUnixNetVConnection::read_signal_error(const int lerrno)
{
  lerrno_ = lerrno;
  return read_signal_done(VC_EVENT_ERROR);
}

inline int ObUnixNetVConnection::write_signal_error(const int lerrno)
{
  lerrno_ = lerrno;
  return write_signal_done(VC_EVENT_ERROR);
}

inline void ObUnixNetVConnection::net_activity()
{
  if (inactivity_timeout_in_ > 0) {
    next_inactivity_timeout_at_ = get_hrtime() + inactivity_timeout_in_;
  } else {
    next_inactivity_timeout_at_ = 0;
  }
}

inline bool ObUnixNetVConnection::check_read_state()
{
  bool ret = true;
  if (OB_UNLIKELY(!read_.enabled_
                  || ObVIO::READ != read_.vio_.op_
                  || read_.vio_.ntodo() <= 0)) {
    read_disable();
    ret = false;
  } else if (OB_ISNULL(read_.vio_.buffer_.writer())) {
    PROXY_NET_LOG(EDIAG, "fail to get writer from buf", "vc", this);
    read_disable();
    ret = false;
  }
  return ret;
}

inline bool ObUnixNetVConnection::check_write_state()
{
  bool ret = true;
  if (OB_UNLIKELY(!write_.enabled_
                  || ObVIO::WRITE != write_.vio_.op_
                  || write_.vio_.ntodo() <= 0)) {
    write_disable();
    ret = false;
  } else if (OB_ISNULL(write_.vio_.buffer_.reader())
             || OB_ISNULL(write_.vio_.buffer_.writer())) {
    PROXY_NET_LOG(EDIAG, "fail to get reader or writer from buf", "vc: ", this);
    write_disable();
    ret = false;
  }
  return ret;
}

inline bool ObUnixNetVConnection::calculate_towrite_size(int64_t &towrite, bool &signalled)
{
  // already check buffer's reader and writer, in func check_write_state.
  bool is_done = false;
  int64_t ntodo = write_.vio_.ntodo();
  ObIOBufferReader *reader = write_.vio_.buffer_.reader();
  ObMIOBuffer *writer = write_.vio_.buffer_.writer();

  if ((towrite = reader->read_avail()) > ntodo) {
    towrite = ntodo;
  }

  if (OB_UNLIKELY(towrite <= 0 && towrite < ntodo && writer->write_avail() > 0)) {
    if (EVENT_CONT == write_signal_and_update(VC_EVENT_WRITE_READY)) {
      if ((ntodo = write_.vio_.ntodo()) <= 0) {
        towrite = ntodo;
        write_disable();
      } else {
        signalled = true;
        if ((towrite = reader->read_avail()) > ntodo) {
          towrite = ntodo;
        }
      }
    } else {
      is_done = true;
    }
  }
  return is_done;
}

inline bool ObUnixNetVConnection::handle_read_from_net_error(ObEThread &thread,
                const int64_t total_read, const int error, const int tmp_code)
{
  bool is_done = false;
  ObProxyMutex *mutex_ = thread.mutex_;
  if (using_ssl_) {
    switch(tmp_code) {
    case SSL_ERROR_WANT_READ:
    case SSL_ERROR_WANT_WRITE:
      read_.triggered_ = false;
      read_reschedule();
      write_.triggered_ = false;
      write_reschedule();
      break;
    default:
      handle_ssl_err_code(tmp_code);
      read_.triggered_ = false;
      write_.triggered_ = false;
      nh_->read_ready_list_.remove(this);
      nh_->write_ready_list_.remove(this);
      if (EVENT_DONE == read_signal_done(VC_EVENT_EOS)) {
        is_done = true;
      }
      break;
    }
  } else if (OB_SYS_EAGAIN == error || OB_SYS_ENOTCONN == error) {
    NET_INCREMENT_DYN_STAT(NET_CALLS_TO_READ_NODATA);
    read_.triggered_ = false;
    nh_->read_ready_list_.remove(this);
  } else if (0 == total_read || OB_SYS_ECONNRESET == error) {
    if (OB_SYS_ETIMEDOUT == error) {
      PROXY_NET_LOG(INFO, "recv OB_SYS_ETIMEDOUT error when read, maybe KeepAlive fail");
    }
    read_.triggered_ = false;
    nh_->read_ready_list_.remove(this);
    if (EVENT_DONE == read_signal_done(VC_EVENT_EOS)) {
      is_done = true;
    }
  } else {
    read_.triggered_ = false;
    if (EVENT_DONE == read_signal_error(error)) {
      is_done = true;
    }
  }
  return is_done;
}

inline bool ObUnixNetVConnection::handle_write_to_net_error(ObEThread &thread,
               const int64_t total_write, const int error, const int tmp_code)
{
  bool is_done = false;
  ObProxyMutex *mutex_ = thread.mutex_;
  if (using_ssl_) {
    switch(tmp_code) {
    case SSL_ERROR_WANT_READ:
    case SSL_ERROR_WANT_WRITE:
      read_.triggered_ = false;
      read_reschedule();
      write_.triggered_ = false;
      write_reschedule();
      break;
    default:
      handle_ssl_err_code(tmp_code);
      write_.triggered_ = false;
      read_.triggered_ = false;
      nh_->read_ready_list_.remove(this);
      nh_->write_ready_list_.remove(this);
      if (EVENT_DONE == write_signal_done(VC_EVENT_EOS)) {
        is_done = true;
      }
      break;
    }
  } else if (OB_SYS_EAGAIN == error || OB_SYS_ENOTCONN == error) {
    NET_INCREMENT_DYN_STAT(NET_CALLS_TO_WRITE_NODATA);
    write_.triggered_ = false;
    nh_->write_ready_list_.remove(this);
    write_reschedule();
  } else if (0 == total_write || OB_SYS_ECONNRESET == error) {
    if (OB_SYS_ETIMEDOUT == error) {
      PROXY_NET_LOG(INFO, "recv OB_SYS_ETIMEDOUT erroru when write, maybe KeepAlive fail");
    }
    write_.triggered_ = false;
    if (EVENT_DONE == write_signal_done(VC_EVENT_EOS)) {
      is_done = true;
    }
  } else {
    write_.triggered_ = false;
    if (EVENT_DONE == write_signal_error(error)) {
      is_done = true;
    }
  }
  return is_done;
}

inline bool ObUnixNetVConnection::handle_read_from_net_success(
    ObEThread &thread, const ObProxyMutex *mutex, const int64_t total_read)
{
  bool is_done = true;
  ObProxyMutex *mutex_ = thread.mutex_;
  NET_SUM_DYN_STAT(NET_READ_BYTES, total_read);

  // Add data to buffer and signal continuation.
  read_.vio_.buffer_.writer()->fill(total_read);
  if (read_.vio_.buffer_.writer()->write_avail() <= 0) {
    PROXY_NET_LOG(DEBUG, "read buffer full");
  }
  read_.vio_.ndone_ += total_read;
  net_activity();

  // If there are no more bytes to read, signal read complete
  if (read_.vio_.ntodo() <= 0) {
    if (EVENT_DONE != read_signal_done(VC_EVENT_READ_COMPLETE)) {
      is_done = false;
    }
  } else {
    if (EVENT_CONT != read_signal_and_update(VC_EVENT_READ_READY)) {
      // finish read, needn't reschedule
    } else if (OB_UNLIKELY(mutex != read_.vio_.mutex_.ptr_)) {
      // change of lock... don't look at shared variables!
      read_reschedule();
    } else {
      // need reschedule
      is_done = false;
    }
  }
  return is_done;
}

inline bool ObUnixNetVConnection::handle_write_to_net_success(
  ObEThread &thread, const ObProxyMutex *mutex, const int64_t total_write, const bool signalled)
{
  bool is_done = true;
  ObProxyMutex *mutex_ = thread.mutex_;
  NET_SUM_DYN_STAT(NET_WRITE_BYTES, total_write);

  // save so we can clear if needed.
  int wbe_event = write_buffer_empty_event_;

  int ret = OB_SUCCESS;
  if (OB_FAIL(write_.vio_.buffer_.reader()->consume(total_write))) {
    PROXY_NET_LOG(WDIAG, "fail to consume ", K(total_write), K(ret));
  }
  write_.vio_.ndone_ += total_write;

  // If the empty write buffer trap is set, clear it.
  if (!(write_.vio_.buffer_.reader()->is_read_avail_more_than(0))) {
    write_buffer_empty_event_ = 0;
  }

  net_activity();

  if (write_.vio_.ntodo() <= 0) {
    if (EVENT_DONE != write_signal_done(VC_EVENT_WRITE_COMPLETE)) {
      is_done = false;
    }
  } else if (signalled && (wbe_event != write_buffer_empty_event_)) {
    // signalled means we won't send an event, and the event values differing means we
    // had a write buffer trap and cleared it, so we need to send it now.
    if (EVENT_CONT == write_signal_and_update(wbe_event)) {
      is_done = false;
    }
  } else if (!signalled) {
    if (EVENT_CONT != write_signal_and_update(VC_EVENT_WRITE_READY)) {
    } else if (OB_UNLIKELY(mutex != write_.vio_.mutex_.ptr_)) {
      // change of lock... don't look at shared variables!
      write_reschedule();
    } else {
      is_done = false;
    }
  } else {
    is_done = false;
  }
  return is_done;
}

inline int ObUnixNetVConnection::close()
{
  // when we close connection,
  // if OB_SUCCESS != ret, we must continue clear up vconnection
  // so this func will swallow error code

  int ret = OB_SUCCESS;
  PROXY_NET_LOG(DEBUG, "close connection", "vc: ", this);

  if (using_ssl_) {
    close_ssl();
  }

  if (OB_FAIL(ep_->stop())) {
    PROXY_NET_LOG(WDIAG, "fail to stop event io", "vc: ", this, K(ret));
  }

  if (OB_FAIL(con_.close())) {
    PROXY_NET_LOG(WDIAG, "fail to close connection", "vc: ", this, K(ret));
  }

  next_inactivity_timeout_at_ = 0;
  reenable_read_time_at_ = 0;
  inactivity_timeout_in_ = 0;

  if (NULL != active_timeout_action_) {
    if (OB_FAIL(active_timeout_action_->cancel(this))) {
      PROXY_NET_LOG(WDIAG, "fail to cancel active timeout action", K(ret));
    }
    active_timeout_action_ = NULL;
  }

  active_timeout_in_ = 0;
  nh_->open_list_.remove(this);
  nh_->cop_list_.remove(this);
  nh_->read_ready_list_.remove(this);
  nh_->write_ready_list_.remove(this);

  if (read_.in_enabled_list_) {
    nh_->read_enable_list_.remove(this);
    read_.in_enabled_list_ = false;
  }

  if (write_.in_enabled_list_) {
    nh_->write_enable_list_.remove(this);
    write_.in_enabled_list_ = false;
  }

  remove_from_keep_alive_lru();

  free();

  return ret;
}

inline int ObUnixNetVConnection::read_from_net_internal(
    ObMIOBuffer &iobuf, const int64_t toread, int64_t &total_read, int &tmp_code)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex_ = &self_ethread().get_mutex();

  int64_t count = 0;
  int64_t rattempted = 0;
  int32_t niov = 0;
  struct iovec tiovec[NET_MAX_IOV];

  int64_t len = -1;
  int64_t remain = 0;

  ObIOBufferBlock *block = NULL;

  if (OB_UNLIKELY(toread <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(block = iobuf.first_write_block())) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if ((len = block->write_avail()) > 0) {
    do {
      niov = 0;
      rattempted = 0;

      do {
        remain = toread - rattempted - total_read;
        if (len > remain) {
          len = remain;
        }
        tiovec[niov].iov_base = block->end_;
        tiovec[niov].iov_len = len;
        rattempted += len;
        ++niov;

        block = block->next_;

        // openssl has no api support iovec, so only need one buffer
        if (using_ssl_) {
          if (NULL != block && len < remain) {
            len = block->write_avail();
          }
          break;
        }
      } while (NULL != block && (len < remain) && (0 < (len = block->write_avail())) && niov < NET_MAX_IOV);

      if (using_ssl_) {
        if (OB_FAIL(ObSocketManager::ssl_read(ssl_, tiovec[0].iov_base, tiovec[0].iov_len, count, tmp_code))) {
          PROXY_NET_LOG(INFO, "ssl read failed", K(ret));
        } else if (count > 0) {
          total_read += count;
        } else {
          break;
        }
      } else if (1 == niov) {
        // here can't use OB_SUCC(),
        // because OB_SUCC use OB_LIKELY
        // if read from socket, we will read until there is no data on the socket,
        // when socket doesn't have any data, read() returns EAGAIN
        ret = ObSocketManager::read(con_.fd_, tiovec[0].iov_base, tiovec[0].iov_len, count);
        if (OB_SUCCESS == ret) {
          total_read += count;
        }
      } else {
        ret = ObSocketManager::readv(con_.fd_, &tiovec[0], niov, count);
        if (OB_SUCCESS == ret) {
          total_read += count;
        }
      }
      NET_INCREMENT_DYN_STAT(NET_CALLS_TO_READ);
    } while (OB_SUCCESS == ret && (count == rattempted) && (total_read < toread) && (NULL != block));
  }

  // if once read data form fd successfully, return OB_SUCCESS.
  if (OB_LIKELY(total_read > 0)) {
    ret = OB_SUCCESS;
  }

  return ret;
}

// Read the data for a ObUnixNetVConnection.
// Rescheduling the ObUnixNetVConnection by moving the VC
// onto or off of the ready_list.
inline void ObUnixNetVConnection::read_from_net(ObEThread &thread)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex_ = thread.mutex_;
  NET_INCREMENT_DYN_STAT(NET_CALLS_TO_READFROMNET);

  int64_t total_read = 0;
  int64_t ntodo = 0;
  int64_t toread = 0;
  bool is_done = true;

  MUTEX_TRY_LOCK(lock, read_.vio_.mutex_, &thread);
  if (OB_UNLIKELY(!lock.is_locked())) {
    read_reschedule();
  } else if (OB_UNLIKELY(!check_read_state())) {
    PROXY_NET_LOG(WDIAG, "fail to check_read_state", K(this));
  } else {
    reenable_read_time_at_ = 0;
    ntodo = read_.vio_.ntodo();
    ObMIOBuffer &writer = *(read_.vio_.buffer_.writer());
    ObIOBufferBlock *block = writer.first_write_block();

    if (0 == read_.active_count_ || (NULL != block && block->write_avail() > 0)) {
      toread = writer.write_avail();
    } else {
      toread = writer.write_avail(ntodo);
    }

    if (toread > ntodo) {
      toread = ntodo;
    }

    // read data
    if (toread > 0) {
      int tmp_code = 0;
      if (OB_FAIL(read_from_net_internal(writer, toread, total_read, tmp_code)) || OB_UNLIKELY((0 == total_read))) {
        is_done = handle_read_from_net_error(thread, total_read, ret, tmp_code);
      } else {
        ++read_.active_count_;
        // just check it
        if (OB_UNLIKELY(ntodo < 0)) {
          PROXY_NET_LOG(EDIAG, "occur fatal error", K(ntodo), K(this));
        }
        is_done = handle_read_from_net_success(thread, lock.get_mutex(), total_read);
      }
    } else {
      // writer.write_avail() <= 0
      // need reschedule
      is_done = false;
    }

    if (!is_done) {
      // If here are is no more room, or nothing to do, disable the connection
      if (read_.vio_.ntodo() <= 0 || !read_.enabled_ || (read_.triggered_ && 0 == writer.write_avail())) {
        read_disable();
      } else {
        read_reschedule();
      }
    }
  }
}

inline int ObUnixNetVConnection::write_to_net_internal(ObIOBufferReader &reader,
                       const int64_t towrite, int64_t &total_write, int &tmp_code)
{
  int ret = OB_SUCCESS;

  int64_t count = 0;
  int64_t wattempted = 0;
  int32_t niov = 0;
  struct iovec tiovec[NET_MAX_IOV];

  int64_t len = -1;
  int64_t remain = 0;

  int64_t offset = 0;

  ObIOBufferBlock *block = NULL;

  if (OB_UNLIKELY(towrite <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(block = reader.block_)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    // XXX because of reserved_size_ in ObIOBufferReader.
    // we can't use skip_empty_blocks() to do this.
    offset = reader.start_offset_;
    while (NULL != block && len <= 0) {
      len = block->read_avail();
      len -= offset;
      if (len <= 0) {
        offset = -len;
        block = block->next_;
      }
    }

    if (OB_LIKELY(len > 0)) {
      do {
        niov = 0;
        wattempted = 0;

        do {
          remain = towrite - wattempted - total_write;
          if (len > remain) {
            len = remain;
          }
          tiovec[niov].iov_base = block->start() + offset;
          offset = 0;
          tiovec[niov].iov_len = len;
          wattempted += len;
          ++niov;

          block = block->next_;

          //openssl not support iovec, just need one buffer
          if (using_ssl_) {
            if (NULL != block) {
              len = block->read_avail();
            }
            break;
          }
        } while (NULL != block && (0 < (len = block->read_avail())) && niov < NET_MAX_IOV);

        if (using_ssl_) {
          if (OB_FAIL(ObSocketManager::ssl_write(ssl_, tiovec[0].iov_base, tiovec[0].iov_len, count, tmp_code))) {
            PROXY_NET_LOG(INFO, "ssl write failed", K(ret));
          } else if (count > 0)  {
            total_write += count;
          } else {
            break;
          }
        } else if (1 == niov) {
          if (OB_SUCC(ObSocketManager::write(con_.fd_, tiovec[0].iov_base, tiovec[0].iov_len, count))) {
            total_write += count;
          }
        } else {
          if (OB_SUCC(ObSocketManager::writev(con_.fd_, &tiovec[0], niov, count))) {
            total_write += count;
          }
        }
        NET_INCREMENT_DYN_STAT(NET_CALLS_TO_WRITE);
      } while (OB_SUCC(ret) && (count == wattempted) && (total_write < towrite) && (NULL != block));
    }
  }

  // if once write data to fd successfully, return OB_SUCCESS
  if (OB_LIKELY(total_write > 0)) {
    ret = OB_SUCCESS;
  }

  return ret;
}

// Write the data for a ObUnixNetVConnection.
// Rescheduling the ObUnixNetVConnection when necessary.
inline void ObUnixNetVConnection::write_to_net(ObEThread &thread)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex_ = thread.mutex_;
  NET_INCREMENT_DYN_STAT(NET_CALLS_TO_WRITETONET);
  int64_t total_write = 0;
  int64_t towrite = 0;
  bool is_done = true;
  bool signalled = false;

  MUTEX_TRY_LOCK(lock, write_.vio_.mutex_, &thread);
  if (OB_UNLIKELY(!lock.is_locked() || lock.get_mutex() != write_.vio_.mutex_.ptr_)) {
    write_reschedule();
  } else if (OB_UNLIKELY(!check_write_state())) {
    PROXY_NET_LOG(DEBUG, "fail to check_write_state", K(this));
  } else {
    ObIOBufferReader &reader = *(write_.vio_.buffer_.reader());
    ObIOBufferReader *old_reader = write_.vio_.buffer_.reader();
    is_done = calculate_towrite_size(towrite, signalled);

    if (OB_LIKELY(!is_done)) {
      if (OB_LIKELY(towrite > 0)) {
        int tmp_code = 0;
        if (OB_FAIL(write_to_net_internal(reader, towrite, total_write, tmp_code)) || OB_UNLIKELY((0 == total_write))) {
          is_done = handle_write_to_net_error(thread, total_write, ret, tmp_code);
        } else {
          is_done = handle_write_to_net_success(thread, lock.get_mutex(), total_write, signalled);
        }
      } else {
        is_done = false;
      }
    }

    if(!is_done) {
      int64_t read_avail = reader.read_avail();
      if (0 == read_avail) {
        /* compare new reader and old reader. because reader maybe modify on write_complete event
         * if not same, get read avail from new reader
         */
        ObIOBufferReader *new_reader = write_.vio_.buffer_.reader();
        if (OB_UNLIKELY(NULL != new_reader && new_reader != old_reader)) {
          read_avail = new_reader->read_avail();
        }
      }

      if (0 == read_avail) {
        write_disable();
      } else {
        if (OB_UNLIKELY(read_avail < 0)) {
          PROXY_NET_LOG(EDIAG, "write vio buffer's reader read_avail < 0",
                        K(read_avail), K(towrite), K(total_write), K(signalled));
        }
        write_reschedule();
      }
    }
  }
}

ObUnixNetVConnection::ObUnixNetVConnection()
    : closed_(0),
      active_timeout_in_(0),
      active_timeout_action_(NULL),
      inactivity_timeout_in_(0),
      next_inactivity_timeout_at_(0),
      reenable_read_time_at_(0),
      ep_(NULL),
      nh_(NULL),
      id_(0),
      event_record_(0),
      flags_(0),
      recursion_(0),
      submit_time_(0),
      source_type_(VC_ACCEPT),
      using_ssl_(false),
      ssl_connected_(false),
      ssl_type_(SSL_NONE),
      ssl_(NULL),
      can_shutdown_ssl_(true),
      ssl_err_code_(SSL_ERROR_NONE),
      io_type_(IO_NONE),
      is_inited_(false)
{
  memset(&server_addr_, 0, sizeof(server_addr_));
  SET_HANDLER((NetVConnHandler)&ObUnixNetVConnection::start_event);
}

// Function used to reenable the VC for reading or writing.
inline void ObUnixNetVConnection::reenable(ObVIO *vio)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(NULL != vio)) {
    ObNetState &ns = get_net_state_by_vio(*vio);
    if ((!ns.enabled_ || using_ssl_) && NULL != thread_) {
      ObEThread &ethread = self_ethread();
      if (OB_UNLIKELY(closed_) || OB_UNLIKELY(vio->mutex_->thread_holding_ != &ethread)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_NET_LOG(EDIAG, "occur fatal error", K(closed_),
                      K(vio->mutex_->thread_holding_),
                      K(&ethread), K(this), K(ret));
      } else {
        ns.enabled_ = true;
        if (0 == next_inactivity_timeout_at_ && inactivity_timeout_in_ > 0) {
          next_inactivity_timeout_at_ = get_hrtime() + inactivity_timeout_in_;
        }

        if (nh_->mutex_->thread_holding_ == &ethread) {
          if (vio == &read_.vio_) {
            ep_->modify(EVENTIO_READ);
            if (read_.triggered_) {
              nh_->read_ready_list_.in_or_enqueue(this);
            } else {
              nh_->read_ready_list_.remove(this);
            }
          } else if (vio == &write_.vio_) {
            ep_->modify(EVENTIO_WRITE);
            if (write_.triggered_) {
              nh_->write_ready_list_.in_or_enqueue(this);
            } else {
              nh_->write_ready_list_.remove(this);
            }
          }
        } else {
          /* connetion poll net io will cross connect.
           * for reduce use lock, connetion poll forct use nolock logic
           * TODO: optimize lock logic
           */
          /*
          MUTEX_TRY_LOCK(lock, nh_->mutex_, &ethread);
          if (!lock.is_locked()) {
          */
            if (vio == &read_.vio_) {
              if (!read_.in_enabled_list_) {
                read_.in_enabled_list_ = true;
                nh_->read_enable_list_.push(this);
              }
            } else if (vio == &write_.vio_) {
              if (!write_.in_enabled_list_) {
                write_.in_enabled_list_ = true;
                nh_->write_enable_list_.push(this);
              }
            }

            if (NULL != nh_->trigger_event_ && NULL != nh_->trigger_event_->get_ethread().signal_hook_) {
              nh_->trigger_event_->get_ethread().signal_hook_(nh_->trigger_event_->get_ethread());
            }
          /*
          } else {
            if (vio == &read_.vio_) {
              ep_->modify(EVENTIO_READ);
              if (read_.triggered_) {
                nh_->read_ready_list_.in_or_enqueue(this);
              } else {
                nh_->read_ready_list_.remove(this);
              }
            } else if (vio == &write_.vio_) {
              ep_->modify(EVENTIO_WRITE);
              if (write_.triggered_) {
                nh_->write_ready_list_.in_or_enqueue(this);
              } else {
                nh_->write_ready_list_.remove(this);
              }
            }
          }
          */
        }
      }
    }
  }
}

void ObUnixNetVConnection::reenable_re(ObVIO *vio)
{
  int ret = OB_SUCCESS;
  if (NULL != thread_ && NULL != vio) {
    ObEThread &ethread = self_ethread();
    if (OB_UNLIKELY(vio->mutex_->thread_holding_ != &ethread)) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_NET_LOG(EDIAG, "occur fatal error",
                    K(vio->mutex_->thread_holding_),
                    K(&ethread), K(this), K(ret));
    } else {
      if (nh_->mutex_->thread_holding_ == &ethread) {
        get_net_state_by_vio(*vio).enabled_ = true;
        if (0 == next_inactivity_timeout_at_ && inactivity_timeout_in_ > 0) {
          next_inactivity_timeout_at_ = get_hrtime() + inactivity_timeout_in_;
        }

        if (using_ssl_) {
          do_ssl_io(ethread);
        } else if (vio == &read_.vio_) {
          ep_->modify(EVENTIO_READ);
          if (read_.triggered_) {
            read_from_net(ethread);
          } else {
            nh_->read_ready_list_.remove(this);
          }
        } else {
          ep_->modify(EVENTIO_WRITE);
          if (write_.triggered_) {
            write_to_net(ethread);
          } else {
            nh_->write_ready_list_.remove(this);
          }
        }
      } else {
        reenable(vio);
      }
    }
  }
}

void ObUnixNetVConnection::reenable_in(event::ObVIO *vio, const ObHRTime atimeout_in)
{
  // only for read vio
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(atimeout_in < 0) || OB_UNLIKELY(vio != &read_.vio_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(EDIAG, "occur fatal error",
                         K(atimeout_in), K(vio), K(&read_.vio_),
                         K(this), K(ret));
  } else {
    if (atimeout_in > 0) {
      reenable_read_time_at_ = atimeout_in + get_hrtime();
    }
    reenable(vio);
  }
}

ObVIO *ObUnixNetVConnection::do_io_read(
    ObContinuation *c, const int64_t nbytes,
    ObMIOBuffer *buf)
{
  read_.active_count_ = 0;
  read_.vio_.op_ = ObVIO::READ;
  read_.vio_.mutex_ = (NULL != c) ? c->mutex_ : mutex_;
  read_.vio_.cont_ = c;
  read_.vio_.nbytes_ = nbytes;
  read_.vio_.ndone_ = 0;
  read_.vio_.vc_server_ = reinterpret_cast<ObVConnection *>(this);
  if (NULL != buf) {
    read_.vio_.buffer_.writer_for(buf);
    io_type_ = IO_READ;
    // SSL_read maybe trigger write and SSL_write maybe trigger read
    // so reenable read
    if (!read_.enabled_ || using_ssl_) {
      read_.vio_.reenable();
    }
  } else {
    read_.vio_.buffer_.destroy();
    read_.enabled_ = false;
  }
  return &read_.vio_;
}

ObVIO *ObUnixNetVConnection::do_io_write(
    ObContinuation *c, const int64_t nbytes,
    ObIOBufferReader *reader)
{
  write_.vio_.op_ = ObVIO::WRITE;
  write_.vio_.mutex_ = (NULL != c) ? c->mutex_ : mutex_;
  write_.vio_.cont_ = c;
  write_.vio_.nbytes_ = nbytes;
  write_.vio_.ndone_ = 0;
  write_.vio_.vc_server_ = reinterpret_cast<ObVConnection *>(this);
  if (NULL != reader) {
    write_.vio_.buffer_.reader_for(reader);
    io_type_ = IO_WRITE;
    // SSL_read maybe trigger write and SSL_write maybe trigger read
    // so reenable write
    if (nbytes > 0 && (!write_.enabled_ || using_ssl_)) {
      if (using_ssl_) {
        write_.triggered_ = true;
      }
      write_.vio_.reenable();
    }
  } else {
    write_.vio_.buffer_.destroy();
    write_.enabled_ = false;
  }

  return &write_.vio_;
}

void ObUnixNetVConnection::do_io_close(const int alerrno /* = -1 */)
{
  int ret = OB_SUCCESS;
  read_.enabled_ = false;
  write_.enabled_ = false;
  read_.vio_.buffer_.destroy();
  read_.vio_.nbytes_ = 0;
  read_.vio_.op_ = ObVIO::NONE;
  read_.vio_.cont_ = NULL;
  write_.vio_.buffer_.destroy();
  write_.vio_.nbytes_ = 0;
  write_.vio_.op_ = ObVIO::NONE;
  write_.vio_.cont_ = NULL;
  if (using_ssl_) {
    close_ssl();
  }

  ObEThread &ethread = self_ethread();
  bool close_inline = (0 == recursion_) && (nh_->mutex_->thread_holding_ == &ethread);

  if (0 != alerrno && -1 != alerrno) {
    lerrno_ = alerrno;
  }

  if (-1 != alerrno) {
    closed_ = 1;
  } else {
    closed_ = -1;
  }

  event_record_ = 0;

  PROXY_NET_LOG(DEBUG, "do_io_close", K(this), K(recursion_), K(&ethread),
                K(nh_->mutex_->thread_holding_),
                K(close_inline));
  if (close_inline) {
    if (OB_FAIL(close())) {
      PROXY_NET_LOG(WDIAG, "fail to close unix net vconnection", K(this), K(ret));
    }
  }
}

void ObUnixNetVConnection::do_io_shutdown(const ShutdownHowToType howto)
{
  int ret = OB_SUCCESS;
  switch (howto) {
    case IO_SHUTDOWN_READ:
      if (OB_FAIL(ObSocketManager::shutdown(con_.fd_, 0))) {
        PROXY_NET_LOG(WDIAG, "fail to shutdown IO_SHUTDOWN_READ", K(con_.fd_), K(ret));
      }
      read_.enabled_ = false;
      read_.vio_.buffer_.destroy();
      read_.vio_.nbytes_ = 0;
      read_.vio_.op_ = ObVIO::NONE;
      f_.shutdown_ = NET_VC_SHUTDOWN_READ;
      break;

    case IO_SHUTDOWN_WRITE:
      if (OB_FAIL(ObSocketManager::shutdown(con_.fd_, 1))) {
        PROXY_NET_LOG(WDIAG, "fail to shutdown IO_SHUTDOWN_WRITE", K(con_.fd_), K(ret));
      }
      write_.enabled_ = false;
      write_.vio_.buffer_.destroy();
      write_.vio_.nbytes_ = 0;
      write_.vio_.op_ = ObVIO::NONE;
      f_.shutdown_ = NET_VC_SHUTDOWN_WRITE;
      break;

    case IO_SHUTDOWN_READWRITE:
      if (OB_FAIL(ObSocketManager::shutdown(con_.fd_, 2))) {
        PROXY_NET_LOG(WDIAG, "fail to shutdown IO_SHUTDOWN_READWRITE", K(con_.fd_), K(ret));
      }
      read_.enabled_ = false;
      write_.enabled_ = false;
      read_.vio_.buffer_.destroy();
      read_.vio_.nbytes_ = 0;
      read_.vio_.op_ = ObVIO::NONE;
      write_.vio_.buffer_.destroy();
      write_.vio_.nbytes_ = 0;
      write_.vio_.op_ = ObVIO::NONE;
      f_.shutdown_ = NET_VC_SHUTDOWN_READ | NET_VC_SHUTDOWN_WRITE;
      break;

    default:
      PROXY_NET_LOG(WDIAG, "can't reach here");
      break;
  }
}

inline int ObUnixNetVConnection::set_active_timeout(const ObHRTime timeout)
{
  int ret = OB_SUCCESS;
  ObEThread *ethread = this_ethread();

  PROXY_NET_LOG(DEBUG, "set active timeout", K(timeout), K(this));
  active_timeout_in_ = timeout;
  if (NULL != active_timeout_action_) {
    if (OB_FAIL(active_timeout_action_->cancel(this))) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_NET_LOG(WDIAG, "fail to cancel reenable action", K(ret));
    } else {
      active_timeout_action_ = NULL;
    }
  }

  if (OB_SUCC(ret) && active_timeout_in_ > 0) {
    if (read_.enabled_) {
      if (OB_FAIL(read_.vio_.mutex_->thread_holding_ != ethread) || OB_ISNULL(thread_)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_NET_LOG(EDIAG, "occur fatal error", K(read_.vio_.mutex_->thread_holding_),
                      K(ethread), K(thread_), K(ret));
      } else {
        if (read_.vio_.mutex_->thread_holding_ == thread_) {
          if (OB_ISNULL(active_timeout_action_ = thread_->schedule_in_local(this, active_timeout_in_))) {
            ret = OB_ERR_UNEXPECTED;
            PROXY_NET_LOG(EDIAG, "thread_ fail to schedule_in_local", K(this), K(active_timeout_in_), K(ret));
          }
        } else {
          if (OB_ISNULL(active_timeout_action_ = thread_->schedule_in(this, active_timeout_in_))) {
            ret = OB_ERR_UNEXPECTED;
            PROXY_NET_LOG(EDIAG, "thread_ fail to schedule_in", K(this), K(active_timeout_in_), K(ret));
          }
        }
      }
    } else if (write_.enabled_) {
      if (OB_FAIL(write_.vio_.mutex_->thread_holding_ != ethread) || OB_ISNULL(thread_)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_NET_LOG(EDIAG, "occur fatal error", K(write_.vio_.mutex_->thread_holding_),
                      K(ethread), K(thread_), K(ret));
      } else {
        if (write_.vio_.mutex_->thread_holding_ == thread_) {
          if (OB_ISNULL(active_timeout_action_ = thread_->schedule_in_local(this, active_timeout_in_))) {
            ret = OB_ERR_UNEXPECTED;
            PROXY_NET_LOG(EDIAG, "thread_ fail to schedule_in_local", K(this), K(active_timeout_in_), K(ret));
          }
        } else {
          if (OB_ISNULL(active_timeout_action_ = thread_->schedule_in(this, active_timeout_in_))) {
            ret = OB_ERR_UNEXPECTED;
            PROXY_NET_LOG(EDIAG, "thread_ fail to schedule_in", K(this), K(active_timeout_in_), K(ret));
          }
        }
      }
    } else {
      active_timeout_action_ = NULL;
    }
  }

  return ret;
}

void ObUnixNetVConnection::add_to_keep_alive_lru()
{
  if (nh_->keep_alive_list_.in(this)) {
    nh_->keep_alive_list_.remove(this);
    nh_->keep_alive_list_.enqueue(this);
  } else {
    nh_->keep_alive_list_.enqueue(this);
    ++nh_->keep_alive_lru_size_;
  }
}

void ObUnixNetVConnection::remove_from_keep_alive_lru()
{
  if (nh_->keep_alive_list_.in(this)) {
    nh_->keep_alive_list_.remove(this);
    --nh_->keep_alive_lru_size_;
  }
}

int ObUnixNetVConnection::set_virtual_addr()
{
  int ret = OB_SUCCESS;
  get_remote_addr();

  if (OB_UNLIKELY(get_global_proxy_config().enable_qa_mode)) {
    // Simulate public cloud SLB to assign IP addresses
    // Get the real client address first, then modify the virutal address
    do_set_virtual_addr();
    if (OB_FAIL(ops_ip_pton(get_global_proxy_config().qa_mode_mock_public_cloud_slb_addr, virtual_addr_))) {
      PROXY_CS_LOG(WARN, "fail to ops ip pton", "qa_mode_mock_public_cloud_slb_addr",
                   get_global_proxy_config().qa_mode_mock_public_cloud_slb_addr, K(ret));
    } else {
      virtual_vid_ = static_cast<uint32_t>(get_global_proxy_config().qa_mode_mock_public_cloud_vid);
    }
  } else if (remote_addr_.is_ip4()) {
    ret = do_set_virtual_addr();
  } else {
    real_client_addr_ = remote_addr_;
    virtual_addr_ = remote_addr_;
    virtual_addr_.sin6_.sin6_port = htons(2883);
  }

  return ret;
}

int ObUnixNetVConnection::do_set_virtual_addr()
{
  int ret = OB_SUCCESS;
  struct vtoa_get_vs4rds vs;
  int vs_len = sizeof(struct vtoa_get_vs4rds);

  if (OB_FAIL(get_vip4rds(con_.fd_, &vs, &vs_len))) {
    PROXY_NET_LOG(DEBUG, "fail to get_vip4rds, use remote ip as virtual ip", K(ret));

    sockaddr_in sin_c;
    sin_c.sin_family = AF_INET;
    sin_c.sin_port = htons(2883);
    sin_c.sin_addr.s_addr = vs.caddr;

    virtual_addr_.assign(ops_ip_sa_cast(sin_c));
  } else {
    ObIpAddr ip_addr(vs.entrytable.vaddr);
    virtual_addr_.assign(ip_addr, vs.entrytable.vport);
    virtual_vid_ = vs.entrytable.vid;
  }
  sockaddr_in sin_c;
  sin_c.sin_family = AF_INET;
  sin_c.sin_port = vs.cport;
  sin_c.sin_addr.s_addr = vs.caddr;

  real_client_addr_.assign(ops_ip_sa_cast(sin_c));

  sockaddr_in sin_d;
  sin_d.sin_family = AF_INET;
  sin_d.sin_port = vs.dport;
  sin_d.sin_addr.s_addr = vs.daddr;

  PROXY_NET_LOG(INFO, "vip connect",
                      "protocol", vs.protocol,
                      "fd", con_.fd_,
                      "vid", virtual_vid_,
                      "vaddr", virtual_addr_,
                      "caddr", ObIpEndpoint(ops_ip_sa_cast(sin_c)),
                      "daddr", ObIpEndpoint(ops_ip_sa_cast(sin_d)));
  return ret;
}

int ObUnixNetVConnection::apply_options()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(con_.apply_options(options_))) {
    PROXY_NET_LOG(WDIAG, "fail to apply_options", K(ret));
  }
  return ret;
}

bool ObUnixNetVConnection::get_data(const int32_t id, void *data)
{
  UNUSED(id);
  UNUSED(data);
  return true;
}

int ObUnixNetVConnection::set_tcp_init_cwnd(const int32_t init_cwnd)
{
  int ret = OB_SUCCESS;
  UNUSED(init_cwnd);
#ifdef TCP_INIT_CWND
  uint32_t val = init_cwnd;
  if (OB_FAIL(ObSocketManager::setsockopt(con_.fd_, IPPROTO_TCP, TCP_INIT_CWND, &val, sizeof(val)))) {
    PROXY_NET_LOG(WDIAG, "fail to set TCP initial congestion window", K(init_cwnd), K(con_.fd_), K(ret));
  }
#endif
  return ret;
}

int ObUnixNetVConnection::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    PROXY_NET_LOG(EDIAG, "init ObUnixNetVConnection twice", K(this), K(ret));
  } else if (OB_ISNULL(ep_ = op_reclaim_alloc(ObEventIO))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(EDIAG, "fail to new ObEventIO", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

inline void ObUnixNetVConnection::free()
{
  // when we free connection
  // if OB_SUCCESS != ret, we must continue free vconnection
  // so this func will swallow error code

  int ret = OB_SUCCESS;
  NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_CONNECTIONS_CURRENTLY_OPEN, -1);
  if (VC_ACCEPT == source_type_) {
    if (OB_LIKELY(NULL != mutex_)) {
      NET_ATOMIC_DECREMENT_DYN_STAT(mutex_->thread_holding_,
                                    NET_CLIENT_CONNECTIONS_CURRENTLY_OPEN);
    }
    NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN, -1);
  }

  if (NO_FD != con_.fd_) {
    if (OB_FAIL(con_.close())) {
      PROXY_NET_LOG(WDIAG, "fail to close fd", K(con_.fd_), K(this), K(ret));
    }
  }

  if (OB_LIKELY(NULL != ep_)) {
    op_reclaim_free(ep_);
    ep_ = NULL;
  }
  is_inited_ = false;

  // clear variables for reuse
  mutex_.release();
  action_.mutex_.release();
  got_remote_addr_ = false;
  got_local_addr_ = false;
  got_virtual_addr_ = false;
  read_.vio_.mutex_.release();
  write_.vio_.mutex_.release();
  flags_ = 0;
  SET_CONTINUATION_HANDLER(this, (NetVConnHandler)&ObUnixNetVConnection::start_event);
  nh_ = NULL;
  read_.triggered_ = false;
  write_.triggered_ = false;
  options_.reset();
  source_type_ = VC_ACCEPT;
  closed_ = 0; // reuse, so this vc isn't closed now
  event_record_ = 0; // record last handle event

  // jsut check
  if (OB_UNLIKELY(NULL != read_.ready_link_.prev_)) {
    PROXY_NET_LOG(WDIAG, "read_.ready_link_.prev_ isn't empty", K(this));
  }
  if (OB_UNLIKELY(NULL != read_.ready_link_.next_)) {
    PROXY_NET_LOG(WDIAG, "read_.ready_link_.next_ isn't empty", K(this));
  }
  if (OB_UNLIKELY(NULL != read_.enable_link_.next_)) {
    PROXY_NET_LOG(WDIAG, "read_.enable_link_.next_ isn't empty", K(this));
  }

  if (OB_UNLIKELY(NULL != write_.ready_link_.prev_)) {
    PROXY_NET_LOG(WDIAG, "write_.ready_link_.prev_ isn't empty", K(this));
  }
  if (OB_UNLIKELY(NULL != write_.ready_link_.next_)) {
    PROXY_NET_LOG(WDIAG, "write_.ready_link_.next_ isn't empty", K(this));
  }
  if (OB_UNLIKELY(NULL != write_.enable_link_.next_)) {
    PROXY_NET_LOG(WDIAG, "write_.enable_link_.next_ isn't empty", K(this));
  }

  if (OB_UNLIKELY(NULL != link_.next_)) {
    PROXY_NET_LOG(WDIAG, "link_.next_ isn't empty", K(this));
  }
  if (OB_UNLIKELY(NULL != link_.prev_)) {
    PROXY_NET_LOG(WDIAG, "link_.prev_ isn't empty", K(this));
  }

  if (OB_UNLIKELY(NULL != active_timeout_action_)) {
    PROXY_NET_LOG(WDIAG, "active_timeout_action_ isn't empty", K(this));
  }

  if (OB_UNLIKELY(NO_FD != con_.fd_)) {
    PROXY_NET_LOG(WDIAG, "con_.fd_ isn't NO_FD", K(con_.fd_), K(this));
  }

  op_reclaim_free(this);
}

int ObUnixNetVConnection::accept_event(int event, ObEvent *e)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_CONT;

  if (OB_ISNULL(e)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WDIAG, "occur fatal error", K(this), K(ret));
    event_ret = EVENT_DONE;
  } else {
    thread_ = &(e->get_ethread());
    MUTEX_TRY_LOCK(lock, thread_->get_net_handler().mutex_, thread_);
    if (!lock.is_locked()) {
      if (EVENT_NONE == event) {
        if (OB_ISNULL(thread_->schedule_in(this, NET_RETRY_DELAY))) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_NET_LOG(WDIAG, "thread fail to schedule_in", K(this), K(ret));
        }
        event_ret = EVENT_DONE;
      } else {
        if (OB_FAIL(e->schedule_in(NET_RETRY_DELAY))) {
          PROXY_NET_LOG(WDIAG, "ObEvent fail to schedule_in", K(this), K(ret));
        }
        event_ret = EVENT_CONT;
      }
    } else if (action_.cancelled_) {
      free();
      event_ret = EVENT_DONE;
    } else {
      SET_HANDLER((NetVConnHandler)&ObUnixNetVConnection::main_event);

      nh_ = &(thread_->get_net_handler());

      if (OB_FAIL(ep_->start(thread_->get_net_poll().get_poll_descriptor(),
                             *this, EVENTIO_READ | EVENTIO_WRITE))) {
        PROXY_NET_LOG(WDIAG, "fail to start ObEventIO", K(this), K(ret));

        if (OB_FAIL(close())) {
          PROXY_NET_LOG(WDIAG, "fail to close unix net vconnection", K(this), K(ret));
        }
        event_ret = EVENT_DONE;
      }

      if (EVENT_DONE != event_ret) {
        nh_->open_list_.enqueue(this);

        if (inactivity_timeout_in_ > 0) {
          set_inactivity_timeout(inactivity_timeout_in_);
        }

        if (active_timeout_in_ > 0) {
          if (OB_FAIL(set_active_timeout(active_timeout_in_))) {
            PROXY_NET_LOG(WDIAG, "fail to set_active_timeout", K(active_timeout_in_), K(this), K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          //callback the cont's handle of accept(cont) in main()
          action_.continuation_->handle_event(NET_EVENT_ACCEPT, this);
        } else {
          action_.continuation_->handle_event(NET_EVENT_ACCEPT_FAILED, reinterpret_cast<void *>(ret));
        }

        event_ret = EVENT_DONE;
      }
    }
  }

  return event_ret;
}

// The main event for ObUnixNetVConnections.
// This is called by the ObEvent subsystem to initialize the ObUnixNetVConnection
// and for active and inactivity timeouts.
int ObUnixNetVConnection::main_event(int event, ObEvent *e)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;

  if (OB_ISNULL(e)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WDIAG, "occur fatal error", K(event), K(e), K(ret));
  } else if (OB_UNLIKELY(EVENT_IMMEDIATE != event && EVENT_INTERVAL != event && EVENT_ERROR != event)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WDIAG, "occur fatal error", K(event), K(e), K(ret));
  } else if (thread_ != this_ethread()) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WDIAG, "occur fatal error", K(thread_), K(event), K(e), K(ret));
  } else {
    ObNetHandler &nh = thread_->get_net_handler();
    ObEThread &ethread = e->get_ethread();

    MUTEX_TRY_LOCK(hlock, nh.mutex_, &ethread);
    MUTEX_TRY_LOCK(rlock, (NULL != read_.vio_.mutex_) ? read_.vio_.mutex_ : ethread.mutex_, &ethread);
    MUTEX_TRY_LOCK(wlock, (NULL != write_.vio_.mutex_) ? write_.vio_.mutex_ : ethread.mutex_, &ethread);

    if (!hlock.is_locked() || !rlock.is_locked() || !wlock.is_locked() ||
        (NULL != read_.vio_.mutex_.ptr_ && rlock.get_mutex() != read_.vio_.mutex_.ptr_) ||
        (NULL != write_.vio_.mutex_.ptr_ && wlock.get_mutex() != write_.vio_.mutex_.ptr_)) {
      if (e == active_timeout_action_) {
        if (OB_FAIL(e->schedule_in(NET_RETRY_DELAY))) {
          PROXY_NET_LOG(WDIAG, "ObEvent fail to schedule_in", K(this), K(ret));
        }
      }
      event_ret = EVENT_CONT;
    } else if (e->cancelled_) {
      event_ret = EVENT_DONE;
    } else {
      int signal_event = 0;
      ObEvent **signal_timeout = NULL;
      ObHRTime next_activity_timeout_at = 0;
      ObHRTime *signal_timeout_at = &next_activity_timeout_at;
      ObEvent *t = NULL;

      signal_timeout = &t;

      if (EVENT_IMMEDIATE == event) {
        if (0 == inactivity_timeout_in_ || next_inactivity_timeout_at_ > get_hrtime()) {
          event_ret = EVENT_CONT;
        } else {
          signal_event = VC_EVENT_INACTIVITY_TIMEOUT;
          signal_timeout_at = &next_inactivity_timeout_at_;
        }
      } else if (EVENT_ERROR == event) {
        signal_event = VC_EVENT_DETECT_SERVER_DEAD;
      } else {
        signal_event = VC_EVENT_ACTIVE_TIMEOUT;
        signal_timeout = &active_timeout_action_;
      }

      if (EVENT_CONT != event_ret) {
        *signal_timeout = NULL;
        *signal_timeout_at = 0;

        if (closed_) {
          if (OB_FAIL(close())) {
            PROXY_NET_LOG(WDIAG, "fail to close unix net vconnection", K(this), K(ret));
          }
          event_ret = EVENT_DONE;
        } else if (ObVIO::READ == read_.vio_.op_
                   && !(f_.shutdown_ & NET_VC_SHUTDOWN_READ)) {
          event_ret = read_signal_and_update(signal_event);
        } else if (ObVIO::WRITE == write_.vio_.op_
                   && !(f_.shutdown_ & NET_VC_SHUTDOWN_WRITE)) {
          event_ret = write_signal_and_update(signal_event);
        }
      }
    }
  }

  return event_ret;
}

int ObUnixNetVConnection::connect_up(ObEThread &ethread, int fd)
{
  int ret = OB_SUCCESS;

  thread_ = &ethread;

  if (NO_FD == fd) {
    if (OB_FAIL(con_.open(options_))) {
      PROXY_NET_LOG(WDIAG, "fail to open connection", K(ret));
    }
  } else {
    // This call will fail if fd is not a socket
    // (e.g. it is a eventfd or a regular file fd.)
    // That is ok, because sock_type is only used when setting up the socket.
    int32_t len = sizeof(con_.sock_type_);
    if (OB_FAIL(ObSocketManager::getsockopt(fd, SOL_SOCKET, SO_TYPE, &con_.sock_type_, &len))) {
      PROXY_NET_LOG(WDIAG, "fail to getsockopt SO_TYPE", K(fd), K(ret));
    } else if ((OB_FAIL(ObSocketManager::nonblocking(fd)))) {
      PROXY_NET_LOG(WDIAG, "fail to getsockopt SO_TYP", K(fd), K(ret));
    } else {
      con_.fd_ = fd;
      con_.is_connected_ = true;
      con_.is_bound_ = true;
    }
  }

  // Must connect after ObEventIO::Start() to avoid a race condition
  // when edge triggering is used.
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ep_->start(thread_->get_net_poll().get_poll_descriptor(),
                           *this, EVENTIO_READ | EVENTIO_WRITE))) {
      PROXY_NET_LOG(WDIAG, "fail to start ObEventIO", K(con_.fd_), K(ret));
    } else if (NO_FD == fd && OB_FAIL(con_.connect(server_addr_.sa_, options_))) {
      PROXY_NET_LOG(WDIAG, "fail to connect", K(con_.fd_), K(ret));
    } else if (OB_FAIL(set_local_addr())) {
      PROXY_NET_LOG(WDIAG, "fail to set_local_addr", K(con_.fd_), K(ret));
    } else {
      SET_HANDLER(&ObUnixNetVConnection::main_event);
      nh_ = &(thread_->get_net_handler());
      nh_->open_list_.enqueue(this);
      action_.continuation_->handle_event(NET_EVENT_OPEN, this);
    }
  }

  if (OB_FAIL(ret)) {
     lerrno_ = ret;
     PROXY_NET_LOG(WDIAG, "fail to connect up, will inform out", K(ret));
     // informed out
     action_.continuation_->handle_event(NET_EVENT_OPEN_FAILED, reinterpret_cast<void *>(ret));
  }
  return ret;
}

void ObUnixNetVConnection::set_read_trigger()
{
#if defined(USE_EDGE_TRIGGER)
  if (read_.enabled_) {
    read_.triggered_ = true;
    read_reschedule();
  }
#endif
}

void ObUnixNetVConnection::reset_read_trigger()
{
#if defined(USE_EDGE_TRIGGER)
  if (read_.triggered_ && read_.enabled_) {
    read_.triggered_ = false;
    nh_->read_ready_list_.remove(this);
  }
#endif
}

int ObUnixNetVConnection::start_event(int event, ObEvent *e)
{
  UNUSED(event);
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  if (OB_ISNULL(e)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WDIAG, "fail to start event", K(e), K(ret));
  } else {
    MUTEX_TRY_LOCK(lock, e->get_ethread().get_net_handler().mutex_, &(e->get_ethread()));
    if (!lock.is_locked()) {
      if (OB_FAIL(e->schedule_in(NET_RETRY_DELAY))) {
        PROXY_NET_LOG(WDIAG, "ObEvent fail to schedule_in", K(e), K(ret));
      } else {
        event_ret = EVENT_CONT;
      }
    } else {
      if (!action_.cancelled_) {
        if (OB_FAIL(connect_up(e->get_ethread(), NO_FD))) {
          PROXY_NET_LOG(WDIAG, "fail to connect_up", K(e), K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        PROXY_NET_LOG(WDIAG, "action has been cancelled", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    free();
  }
  return event_ret;
}

int ObUnixNetVConnection::ssl_init(const SSLType ssl_type,
                                   const ObString &cluster_name,
                                   const ObString &tenant_name,
                                   const uint64_t options)
{
  int ret = OB_SUCCESS;

  if (NULL != ssl_ || ssl_connected_ || using_ssl_) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WDIAG, "ssl is not null, init before", K(ret),
        K(ssl_connected_), K(using_ssl_), K(ssl_));
  } else {
    ssl_ = g_ssl_processor.create_new_ssl(cluster_name, tenant_name, options);
    if (NULL == ssl_) {
      ret = OB_SSL_ERROR;
      PROXY_NET_LOG(WDIAG, "ssl new failed", K(ret));
    } else if (OB_SSL_SUCC_RET != SSL_set_fd(ssl_, con_.fd_)) {
      ret = OB_SSL_ERROR;
      PROXY_NET_LOG(WDIAG, "ssl set fd failed", K(ret));
    } else {
      using_ssl_ = true;
      ssl_type_ = ssl_type;
    }
  }

  return ret;
}

// SSL_read maybe trigger write and SSL_write maybe trigger read
// so adjust enable_ and triggered_ value 
void ObUnixNetVConnection::do_ssl_io(ObEThread &thread)
{
  int ret = OB_SUCCESS;
  if (!ssl_connected_) {
    if (SSL_SERVER == ssl_type_) {
      ObCurTraceId::set(reinterpret_cast<uint64_t>(read_.vio_.cont_->mutex_.ptr_));
    } else if (SSL_CLIENT == ssl_type_) {
      ObCurTraceId::set(reinterpret_cast<uint64_t>(write_.vio_.cont_->mutex_.ptr_));
    }

    if (OB_FAIL(ssl_start_handshake(thread))) {
      PROXY_NET_LOG(WDIAG, "ssl start handlshake failed", K(ret));
    }
  } else if (IO_READ == io_type_) {
    ObCurTraceId::set(reinterpret_cast<uint64_t>(read_.vio_.cont_->mutex_.ptr_));
    read_.triggered_ = true;
    write_.triggered_ = false;
    read_from_net(thread);
  } else if (IO_WRITE == io_type_) {
    ObCurTraceId::set(reinterpret_cast<uint64_t>(write_.vio_.cont_->mutex_.ptr_));
    write_.triggered_ = true;
    read_.triggered_ = false;
    write_to_net(thread);
  } else {
    PROXY_NET_LOG(WDIAG, "wrong write ssl io type", K(io_type_));
  }
}

int ObUnixNetVConnection::ssl_start_handshake(ObEThread &thread)
{
  int ret = OB_SUCCESS;

  if (ssl_type_ >= SSL_MAX_TYPE || ssl_type_ <= SSL_NONE) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WDIAG, "invalid ssl argument", K(ret), K(ssl_type_));
  } else if (NULL == ssl_) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WDIAG, "init ssl failed", K(ret));
  } else if (SSL_SERVER == ssl_type_ && OB_FAIL(ssl_server_handshake(thread))) {
    PROXY_NET_LOG(WDIAG, "ssl server handshake failed", K(ret));
  } else if (SSL_CLIENT == ssl_type_ && OB_FAIL(ssl_client_handshake(thread))) {
    PROXY_NET_LOG(WDIAG, "ssl client handshake failed", K(ret));
  }
  return ret;
}

int ObUnixNetVConnection::ssl_server_handshake(ObEThread &thread)
{
  int ret = OB_SUCCESS;
  int tmp_code = 0;
  MUTEX_TRY_LOCK(lock, read_.vio_.mutex_, &thread);

  if (NULL == ssl_) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WDIAG, "ssl server handshake event", K(ret));
  } else if (!lock.is_locked()) {
    nh_->write_ready_list_.in_or_enqueue(this);
    nh_->read_ready_list_.in_or_enqueue(this);
  } else if (OB_FAIL(ObSocketManager::ssl_accept(ssl_, ssl_connected_, tmp_code))) {
    handle_ssl_err_code(tmp_code);
    read_.triggered_ = false;
    write_.triggered_ = false;
    nh_->read_ready_list_.remove(this);
    nh_->write_ready_list_.remove(this);
    PROXY_NET_LOG(WDIAG, "ssl accepte failed", K(ret), K(tmp_code));
    read_signal_done(VC_EVENT_EOS);
  } else if (ssl_connected_) {
    reenable(&read_.vio_);
  } else if (SSL_ERROR_WANT_READ == tmp_code || SSL_ERROR_WANT_WRITE == tmp_code) {
    read_.triggered_ = false;
    read_reschedule();
    write_.triggered_ = false;
    write_reschedule();
  }

  return ret;
}

int ObUnixNetVConnection::ssl_client_handshake(ObEThread &thread)
{
  int ret = OB_SUCCESS;
  int tmp_code;
  MUTEX_TRY_LOCK(lock, write_.vio_.mutex_, &thread);

  if (NULL == ssl_) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WDIAG, "ssl client handshake event", K(ret));
  } else if (!lock.is_locked()) {
    nh_->write_ready_list_.in_or_enqueue(this);
    nh_->read_ready_list_.in_or_enqueue(this);
  } else if (OB_FAIL(ObSocketManager::ssl_connect(ssl_, ssl_connected_, tmp_code))) {
    handle_ssl_err_code(tmp_code);
    write_.triggered_ = false;
    read_.triggered_ = false;
    nh_->read_ready_list_.remove(this);
    nh_->write_ready_list_.remove(this);
    PROXY_NET_LOG(WDIAG, "ssl connect failed", K(ret), K(tmp_code));
    write_signal_done(VC_EVENT_EOS);
  } else if (ssl_connected_) {
    reenable(&write_.vio_);
  } else if (SSL_ERROR_WANT_READ == tmp_code || SSL_ERROR_WANT_WRITE == tmp_code) {
    read_.triggered_ = false;
    read_reschedule();
    write_.triggered_ = false;
    write_reschedule();
  }

  return ret;
}

void ObUnixNetVConnection::handle_ssl_err_code(const int err_code)
{
  ssl_err_code_ = err_code;
  switch(err_code) {
  case SSL_ERROR_NONE:
  case SSL_ERROR_WANT_READ:
  case SSL_ERROR_WANT_WRITE:
    break;
  case SSL_ERROR_WANT_CONNECT:
    PROXY_NET_LOG(DEBUG, "ssl return SSL_ERROR_WANT_CONNECT");
    break;
  case SSL_ERROR_WANT_ACCEPT:
    PROXY_NET_LOG(DEBUG, "ssl return SSL_ERROR_WANT_ACCEPT");
    break;
  case SSL_ERROR_WANT_X509_LOOKUP:
    PROXY_NET_LOG(INFO, "ssl return SSL_ERROR_WANT_X509_LOOKUP");
    break;
  // case SSL_ERROR_WANT_ASYNC:
  //   PROXY_NET_LOG(WDIAG, "ssl return SSL_ERROR_WANT_ASYNC");
  //   break;
  // case SSL_ERROR_WANT_ASYNC_JOB:
  //   PROXY_NET_LOG(WDIAG, "ssl return SSL_ERROR_WANT_ASYNC_JOB");
  //   break;
  // case SSL_ERROR_WANT_CLIENT_HELLO_CB:
  //   PROXY_NET_LOG(WDIAG, "ssl return SSL_ERROR_WANT_CLIENT_HELLO_CB");
  //   break;
  case SSL_ERROR_ZERO_RETURN:
    PROXY_NET_LOG(INFO, "ssl return SSL_ERROR_ZERO_RETURN");
    close_ssl();
    break;
  case SSL_ERROR_SYSCALL:
    PROXY_NET_LOG(INFO, "ssl return SSL_ERROR_SYSCALL");
    can_shutdown_ssl_ = false;
    close_ssl();
    break;
  case SSL_ERROR_SSL:
    PROXY_NET_LOG(INFO, "ssl return SSL_ERROR_SYSCALL");
    can_shutdown_ssl_ = false;
    close_ssl();
    break;
  default:
    can_shutdown_ssl_ = false;
    close_ssl();
    PROXY_NET_LOG(INFO, "unknown ssl error code, should not happen");
    break;
  }
}

void ObUnixNetVConnection::close_ssl()
{
  using_ssl_ = false;
  ssl_connected_ = false;
  ssl_type_ = SSL_NONE;
  if (NULL != ssl_) {
    g_ssl_processor.release_ssl(ssl_, can_shutdown_ssl_);
    ssl_ = NULL;
  }

  write_.enabled_ = false;
  read_.enabled_ = false;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase
