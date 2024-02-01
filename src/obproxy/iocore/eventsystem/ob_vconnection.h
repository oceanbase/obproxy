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
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * **************************************************************
 *
 * Public VConnection declaration and associated declarations
 *
 */

#ifndef OBPROXY_VCONNECTION_H
#define OBPROXY_VCONNECTION_H

#include "iocore/eventsystem/ob_event_system.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

class ObContinuation;
class ObVIO;
class ObMIOBuffer;
class ObProxyMutex;

// Data Types
#define VCONNECTION_CACHE_DATA_BASE     0
#define VCONNECTION_NET_DATA_BASE       100
#define VCONNECTION_API_DATA_BASE       200

// ObEvent signals
#define VC_EVENT_NONE                    EVENT_NONE

// When a ObContinuation is first scheduled on a processor.
#define VC_EVENT_IMMEDIATE               EVENT_IMMEDIATE

#define VC_EVENT_READ_READY              VC_EVENT_EVENTS_START

/**
 * Any data in the accociated buffer *will be written* when the
 * ObContinuation returns.
 */
#define VC_EVENT_WRITE_READY             (VC_EVENT_EVENTS_START+1)

#define VC_EVENT_READ_COMPLETE           (VC_EVENT_EVENTS_START+2)
#define VC_EVENT_WRITE_COMPLETE          (VC_EVENT_EVENTS_START+3)

/**
 * No more data (end of stream). It should be interpreted by a
 * protocol engine as either a COMPLETE or ERROR.
 */
#define VC_EVENT_EOS                     (VC_EVENT_EVENTS_START+4)

#define VC_EVENT_ERROR                   EVENT_ERROR

/**
 * VC_EVENT_INACTIVITY_TIMEOUT indiates that the operation (read or write) has:
 *   -# been enabled for more than the inactivity timeout period
 *      (for a read, there has been space in the buffer)
 *      (for a write, there has been data in the buffer)
 *   -# no progress has been made
 *      (for a read, no data has been read from the connection)
 *      (for a write, no data has been written to the connection)
 */
#define VC_EVENT_INACTIVITY_TIMEOUT      (VC_EVENT_EVENTS_START+5)

/**
 * Total time for some operation has been exeeded, regardless of any
 * intermediate progress.
 */
#define VC_EVENT_ACTIVE_TIMEOUT          (VC_EVENT_EVENTS_START+6)

#define VC_EVENT_HELLO_PKT_READ_READY    (VC_EVENT_EVENTS_START+7)
// OBProxy detect server dead, should close connection
#define VC_EVENT_DETECT_SERVER_DEAD      (VC_EVENT_EVENTS_START+8)

// ObEvent return codes
#define VC_EVENT_DONE                CONTINUATION_DONE
#define VC_EVENT_CONT                CONTINUATION_CONT

// Used in ObVConnection::shutdown().
enum ShutdownHowToType
{
  IO_SHUTDOWN_READ = 0,
  IO_SHUTDOWN_WRITE,
  IO_SHUTDOWN_READWRITE
};

// Used in ObVConnection::get_data().
enum TSApiDataType
{
  OB_API_DATA_READ_VIO = VCONNECTION_API_DATA_BASE,
  OB_API_DATA_WRITE_VIO,
  OB_API_DATA_OUTPUT_VC,
  OB_API_DATA_CLOSED
};


/**
 * Base class for the connection classes that provide IO capabilities.
 *
 * The ObVConnection class is an abstract representation of a uni or
 * bi-directional data conduit returned by a ObProcessor. In a sense,
 * they serve a similar purpose to file descriptors. A ObVConnection
 * is a pure base class that defines methods to perform stream IO.
 * It is also a ObContinuation that is called back from processors.
 */
class ObVConnection : public ObContinuation
{
public:
  explicit ObVConnection(ObProxyMutex *amutex) : ObContinuation(amutex), lerrno_(0) { }
  virtual ~ObVConnection() { }

  /**
   * Read data from the ObVConnection.
   * Called by a state machine to read data from the ObVConnection.
   * Processors implementing read functionality take out lock, put
   * new bytes on the buffer and call the continuation back before
   * releasing the lock in order to enable the state machine to
   * handle transfer schemes where the end of a given transaction
   * is marked by a special character (ie: NNTP).
   * Possible ObEvent Codes
   * On the callback to the continuation, the ObVConnection may use
   * on of the following values for the event code:
   * VC_EVENT_READ_READY
   *   Data has been added to the buffer or the buffer is full
   * VC_EVENT_READ_COMPLETE
   *   The amount of data indicated by 'nbytes' has been read into the buffer
   * VC_EVENT_EOS
   *   The stream being read from has been shutdown
   * VC_EVENT_ERROR
   *   An error occurred during the read
   *
   * @param c      ObContinuation to be called back with events.
   * @param nbytes Number of bytes to read. If unknown, nbytes must
   *               be set to INT64_MAX.
   * @param buf    buffer to read into.
   *
   * @return ObVIO representing the scheduled IO operation.
   */
  virtual ObVIO *do_io_read(ObContinuation *c = NULL,
                            const int64_t nbytes = INT64_MAX,
                            ObMIOBuffer *buf = 0) = 0;

  /**
   * Write data to the ObVConnection.
   * This method is called by a state machine to write data to the
   * ObVConnection.
   * Possible ObEvent Codes
   * On the callback to the continuation, the ObVConnection may use
   * on of the following event codes:
   * VC_EVENT_WRITE_READY
   *   Data was written from the reader or there are no bytes available
   *   for the reader to write.
   * VC_EVENT_WRITE_COMPLETE
   *   The amount of data indicated by 'nbytes' has been written to the
   *   ObVConnection
   * VC_EVENT_INACTIVITY_TIMEOUT
   *   No activity was performed for a certain period.
   * VC_EVENT_ACTIVE_TIMEOUT
   *   Write operation continued beyond a time limit.
   * VC_EVENT_DETECT_SERVER_DEAD
   *   find Server dead, close connection
   * VC_EVENT_ERROR
   *   An error occurred during the write
   *
   * @param c      ObContinuation to be called back with events.
   * @param nbytes Number of bytes to write. If unknown, nbytes must
   *               be set to INT64_MAX.
   * @param buf    Reader whose data is to be read from.
   * @param owner
   *
   * @return ObVIO representing the scheduled IO operation.
   */
  virtual ObVIO *do_io_write(ObContinuation *c = NULL,
                             const int64_t nbytes = INT64_MAX,
                             ObIOBufferReader *buf = 0) = 0;

  /**
   * Indicate that the ObVConnection is no longer needed.
   * Once the state machine has finished using this ObVConnection, it
   * must call this function to indicate that the ObVConnection can
   * be deallocated.  After a close has been called, the ObVConnection
   * and underlying processor must not send any more events related
   * to this ObVConnection to the state machine. Likeswise, the state
   * machine must not access the ObVConnection or any VIOs obtained
   * from it after calling this method.=
   *
   * @param lerrno indicates where a close is a normal close or an
   *               abort. The difference between a normal close and an abort
   *               depends on the underlying type of the ObVConnection.
   */
  virtual void do_io_close(const int lerrno = -1) = 0;

  /**
   * Terminate one or both directions of the ObVConnection.
   * Indicates that one or both sides of the ObVConnection should be
   * terminated. After this call is issued, no further I/O can be
   * done on the specified direction of the connection. The processor
   * must not send any further events (including timeout events) to
   * the state machine, and the state machine must not use any VIOs
   * from a shutdown direction of the connection. Even if both sides
   * of a connection are shutdown, the state machine must still call
   * do_io_close() when it wishes the ObVConnection to be deallocated.
   * Possible howto values
   * IO_SHUTDOWN_READ
   *   Indicates that this ObVConnection should not generate any more
   *   read events
   * IO_SHUTDOWN_WRITE
   *   Indicates that this ObVConnection should not generate any more
   *   write events
   * IO_SHUTDOWN_READWRITE
   *   Indicates that this ObVConnection should not generate any more
   *   read nor write events
   *
   * @param howto  Specifies which direction of the ObVConnection to
   *               shutdown.
   */
  virtual void do_io_shutdown(const ShutdownHowToType howto) = 0;

  // @deprecated
  ObVIO *do_io(int op, ObContinuation *c = NULL,
               const int64_t nbytes = INT64_MAX,
               ObMIOBuffer *buf = 0,
               const int32_t data = 0);

  /**
   * Private
   * Set continuation on a given vio. The public interface
   * is through ObVIO::set_continuation()
   *
   * @param vio
   * @param cont
   */
  virtual void set_continuation(ObVIO *vio, ObContinuation *cont);

  // Reenable a given vio.  The public interface is through ObVIO::reenable
  virtual void reenable(ObVIO *vio);
  virtual void reenable_re(ObVIO *vio);
  virtual void reenable_in(ObVIO *vio, const ObHRTime atimeout_in);

  /**
   * Convenience function to retrieve information from ObVConnection.
   * This function is provided as a convenience for state machines
   * to transmit information from/to a ObVConnection without breaking
   * the ObVConnection abstraction. Its behavior varies depending on
   * the type of ObVConnection being used.
   *
   * @param id     Identifier associated to interpret the data field
   * @param data   Value or pointer with state machine or ObVConnection data.
   *
   * @return True if the oparation is successful.
   */
  virtual bool get_data(const int32_t id, void *data)
  {
    UNUSED(id);
    UNUSED(data);
    return false;
  }

  /**
   * Convenience function to set information into the ObVConnection.
   * This function is provided as a convenience for state machines
   * to transmit information from/to a ObVConnection without breaking
   * the ObVConnection abstraction. Its behavior varies depending on
   * the type of ObVConnection being used.
   *
   * @param id     Identifier associated to interpret the data field.
   * @param data   Value or pointer with state machine or ObVConnection data.
   *
   * @return True if the oparation is successful.
   */
  virtual bool set_data(const int32_t id, void *data)
  {
    UNUSED(id);
    UNUSED(data);
    return false;
  }

public:
  /**
   * The error code from the last error.
   *
   * Indicates the last error on the ObVConnection. They are either
   * system error codes or from the ob_proxy_lib.h file.
   */
  int lerrno_;
};

struct ObDummyVConnection : public ObVConnection
{
  explicit ObDummyVConnection(ObProxyMutex *m) : ObVConnection(m) { }
  virtual ~ObDummyVConnection() {}

  virtual ObVIO *do_io_write(ObContinuation *c, const int64_t nbytes,
                             ObIOBufferReader *buf)
  {
    UNUSED(c);
    UNUSED(nbytes);
    UNUSED(buf);
    PROXY_EVENT_LOG(EDIAG, "ObVConnection::do_io_write, cannot use default implementation");
    return NULL;
  }

  virtual ObVIO *do_io_read(ObContinuation *c, const int64_t nbytes,
                            ObMIOBuffer *buf)
  {
    UNUSED(c);
    UNUSED(nbytes);
    UNUSED(buf);
    PROXY_EVENT_LOG(EDIAG, "ObVConnection::do_io_read, cannot use default implementation");
    return NULL;
  }

  virtual void do_io_close(const int alerrno)
  {
    UNUSED(alerrno);
    PROXY_EVENT_LOG(EDIAG, "ObVConnection::do_io_close, cannot use default implementation");
  }

  virtual void do_io_shutdown(const ShutdownHowToType howto)
  {
    UNUSED(howto);
    PROXY_EVENT_LOG(EDIAG, "ObVConnection::do_io_shutdown, cannot use default implementation");
  }
};

/**
 * Descriptor for an IO operation.
 *
 * A ObVIO is a descriptor for an in progress IO operation. It is
 * returned from do_io_read() and do_io_write() methods on VConnections.
 * Through the ObVIO, the state machine can monitor the progress of
 * an operation and reenable the operation when data becomes available.
 *
 * The ObVIO operation represents several types of operations, and
 * they can be identified through the 'op' member. It can take any
 * of the following values:
 *
 * READ
 * WRITE
 * CLOSE
 * ABORT
 * SHUTDOWN_READ
 * SHUTDOWN_WRITE
 * SHUTDOWN_READWRITE
 * SEEK
 * PREAD
 * PWRITE
 * STAT
 */
class ObVIO
{
public:
  explicit ObVIO(int aop);
  ObVIO();
  ~ObVIO() { }

  // Interface for the ObVConnection that owns this handle.
  ObContinuation *get_continuation() { return cont_; }
  void set_continuation(ObContinuation *cont);

  /**
   * Set nbytes to be what is current available.
   *
   * Interface to set nbytes to be ndone + buffer.reader()->read_avail()
   * if a reader is set.
   */
  void done();

  /**
   * Determine the number of bytes remaining.
   * Convenience function to determine how many bytes the operation
   * has remaining.
   *
   * @return The number of bytes to be processed by the operation.
   */
  int64_t ntodo() const { return nbytes_ - ndone_; }

  // buffer settings
  void set_writer(ObMIOBuffer *writer) { buffer_.writer_for(writer); }
  void set_reader(ObIOBufferReader *reader) { buffer_.reader_for(reader); }
  ObMIOBuffer *get_writer() { return buffer_.writer(); }
  ObIOBufferReader *get_reader() { return (buffer_.reader()); }

  /**
   * Reenable the IO operation.
   *
   * Interface that the state machine uses to reenable an I/O
   * operation.  Reenable tells the ObVConnection that more data is
   * available for the operation and that it should try to continue
   * the operation in progress.  I/O operations become disabled when
   * they can make no forward progress.  For a read this means that
   * it's buffer is full. For a write, that it's buffer is empty.
   * If reenable is called and progress is still not possible, it
   * is ignored and no events are generated. However, unnecessary
   * reenables (ones where no progress can be made) should be avoided
   * as they hurt system throughput and waste CPU.
   */
  void reenable();

  /**
   * Reenable the IO operation.
   *
   * Interface that the state machine uses to reenable an I/O
   * operation.  Reenable tells the ObVConnection that more data is
   * available for the operation and that it should try to continue
   * the operation in progress.  I/O operations become disabled when
   * they can make no forward progress.  For a read this means that
   * it's buffer is full. For a write, that it's buffer is empty.
   * If reenable is called and progress is still not possible, it
   * is ignored and no events are generated. However, unnecessary
   * reenables (ones where no progress can be made) should be avoided
   * as they hurt system throughput and waste CPU.
   */
  void reenable_re();

  void reenable_in(const ObHRTime atimeout_in);

public:
  enum
  {
    NONE = 0,
    READ,
    WRITE,
    CLOSE,
    ABORT,
    SHUTDOWN_READ,
    SHUTDOWN_WRITE,
    SHUTDOWN_READWRITE,
    SEEK,
    PREAD,
    PWRITE,
    STAT
  };

  /**
   * ObContinuation to callback.
   *
   * Used by the ObVConnection to store who is the continuation to
   * call with events for this operation.
   */
  ObContinuation *cont_;

  /**
   * Number of bytes to be done for this operation.
   *
   * The total number of bytes this operation must complete.
   */
  int64_t nbytes_;

  /**
   * Number of bytes already completed.
   *
   * The number of bytes that already have been completed for the
   * operation. ObProcessor can update this value only if they hold
   * the lock.
   */
  int64_t ndone_;

  /**
   * Type of operation.
   *
   * The type of operation that this ObVIO represents.
   */
  int32_t op_;

  /**
   * Provides access to the reader or writer for this operation.
   *
   * Contains a pointer to the ObIOBufferReader if the operation is a
   * write and a pointer to a ObMIOBuffer if the operation is a read.
   */
  ObMIOBufferAccessor buffer_;

  /**
   * Internal backpointer to the ObVConnection for use in the reenable
   * functions.
   */
  ObVConnection *vc_server_;

  /**
   * Reference to the state machine's mutex.
   *
   * Maintains a reference to the state machine's mutex to allow
   * processors to safely lock the operation even if the state machine
   * has closed the ObVConnection and deallocated itself.
   */
  common::ObPtr<ObProxyMutex> mutex_;
};

inline ObVIO::ObVIO(int aop)
    : cont_(NULL),
      nbytes_(0),
      ndone_(0),
      op_(aop),
      buffer_(),
      vc_server_(NULL),
      mutex_(NULL)
{
}

inline ObVIO::ObVIO()
    : cont_(NULL),
      nbytes_(0),
      ndone_(0),
      op_(ObVIO::NONE),
      buffer_(),
      vc_server_(NULL),
      mutex_(NULL)
{
}

inline void ObVIO::done()
{
  if (NULL != buffer_.reader()) {
    nbytes_ = ndone_ + buffer_.reader()->read_avail();
  } else {
    nbytes_ = ndone_;
  }
}

inline void ObVIO::set_continuation(ObContinuation *acont)
{
  if (NULL != vc_server_) {
    vc_server_->set_continuation(this, acont);
  }
  if (NULL != acont) {
    mutex_ = acont->mutex_;
    cont_ = acont;
  } else {
    mutex_ = NULL;
    cont_ = NULL;
  }
}

inline void ObVIO::reenable()
{
  if (NULL != vc_server_) {
    vc_server_->reenable(this);
  }
}

inline void ObVIO::reenable_re()
{
  if (NULL != vc_server_) {
    vc_server_->reenable_re(this);
  }
}

inline void ObVIO::reenable_in(const ObHRTime atimeout_in)
{
  if (NULL != vc_server_) {
    vc_server_->reenable_in(this, atimeout_in);
  }
}

inline ObVIO *vc_do_io_write(ObVConnection *vc, ObContinuation *cont,
                      int64_t nbytes, ObMIOBuffer *buf, int64_t offset)
{
  ObVIO *vret = NULL;
  int ret = common::OB_SUCCESS;
  ObIOBufferReader *reader = NULL;
  if (OB_ISNULL(reader = buf->alloc_reader())) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_LOG(WDIAG, "fail to alloc_reader", K(ret));
  } else if (offset > 0) {
    if (OB_FAIL(reader->consume(offset))) {
      PROXY_LOG(WDIAG, "fail to consume ", K(offset), K(ret));
    }
  } else {/*do nothing*/}

  if (OB_SUCC(ret)) {
    vret = vc->do_io_write(cont, nbytes, reader);
  }
  return vret;
}

inline ObVIO *ObVConnection::do_io(int op, ObContinuation *c,
                            int64_t nbytes, ObMIOBuffer *cb, int data)
{
  ObVIO *ret = NULL;
  switch (op) {
    case ObVIO::READ:
      ret = do_io_read(c, nbytes, cb);
      break;

    case ObVIO::WRITE:
      ret = vc_do_io_write(this, c, nbytes, cb, data);
      break;

    case ObVIO::CLOSE:
      do_io_close();
      break;

    case ObVIO::ABORT:
      do_io_close(data);
      break;

    case ObVIO::SHUTDOWN_READ:
      do_io_shutdown(IO_SHUTDOWN_READ);
      break;

    case ObVIO::SHUTDOWN_WRITE:
      do_io_shutdown(IO_SHUTDOWN_WRITE);
      break;

    case ObVIO::SHUTDOWN_READWRITE:
      do_io_shutdown(IO_SHUTDOWN_READWRITE);
      break;

    default:
      PROXY_EVENT_LOG(EDIAG, "cannot use default implementation for do_io operation");
      break;
  }

  return ret;
}

inline void ObVConnection::set_continuation(ObVIO *vio, ObContinuation *cont)
{
  UNUSED(vio);
  UNUSED(cont);
}

inline void ObVConnection::reenable(ObVIO *vio)
{
  UNUSED(vio);
}

inline void ObVConnection::reenable_re(ObVIO *vio)
{
  reenable(vio);
}

inline void ObVConnection::reenable_in(ObVIO *vio, const ObHRTime atimeout_in)
{
  UNUSED(vio);
  UNUSED(atimeout_in);
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_VCONNECTION_H
