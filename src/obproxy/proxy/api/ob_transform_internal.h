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

#ifndef OBPROXY_TRANSFORM_INTERNAL_H
#define OBPROXY_TRANSFORM_INTERNAL_H

#include "iocore/eventsystem/ob_event_system.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "proxy/api/ob_transform.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObTransformVConnection;

class ObTransformTerminus : public event::ObVConnection
{
public:
  ObTransformTerminus(ObTransformVConnection *tvc);
  virtual ~ObTransformTerminus() {}

  int handle_event(int event, void *edata);

  event::ObVIO *do_io_read(event::ObContinuation *c, const int64_t nbytes,
                           event::ObMIOBuffer *buf);
  event::ObVIO *do_io_write(event::ObContinuation *c, const int64_t nbytes,
                            event::ObIOBufferReader *buf);
  void do_io_close(int lerrno = -1);
  void do_io_shutdown(event::ShutdownHowToType howto);

  void reenable(event::ObVIO *vio);

public:
  ObTransformVConnection *tvc_;
  event::ObVIO read_vio_;
  event::ObVIO write_vio_;
  volatile int event_count_;
  volatile int deletable_;
  volatile int closed_;
  int called_user_;
};

class ObTransformVConnection : public ObTransformVCChain
{
public:
  ObTransformVConnection(event::ObContinuation *cont, ObAPIHook *hooks);
  virtual ~ObTransformVConnection() {}

  virtual void destroy();

  static ObTransformVConnection *alloc(event::ObContinuation *cont, ObAPIHook *hooks)
  {
    return op_reclaim_alloc_args(ObTransformVConnection, cont, hooks);
  }

  int handle_event(int event, void *edata);

  event::ObVIO *do_io_read(event::ObContinuation *c, const int64_t nbytes,
                           event::ObMIOBuffer *buf);
  event::ObVIO *do_io_write(event::ObContinuation *c, const int64_t nbytes,
                            event::ObIOBufferReader *buf);
  void do_io_close(const int lerrno = -1);
  void do_io_shutdown(const event::ShutdownHowToType howto);

  void reenable(event::ObVIO *vio);

  /**
   * Compute the backlog.
   * @return The actual backlog, or a value at least limit.
   */
  virtual uint64_t backlog(uint64_t limit = UINT64_MAX);

public:
  event::ObVConnection *transform_;
  event::ObContinuation *cont_;
  ObTransformTerminus terminus_;
  volatile int closed_;
};

class ObTransformControl : public event::ObContinuation
{
public:
  ObTransformControl();
  virtual ~ObTransformControl() {}

  int handle_event(int event, void *edata);

public:
  ObAPIHooks hooks_;
  event::ObVConnection *tvc_;
  event::ObIOBufferReader *read_buf_;
  event::ObMIOBuffer *write_buf_;
};

class ObNullTransform : public ObVConnInternal
{
public:
  explicit ObNullTransform(event::ObProxyMutex *mutex);
  virtual ~ObNullTransform() {}

  virtual void destory();

  static ObNullTransform *alloc(event::ObProxyMutex *mutex)
  {
    return op_reclaim_alloc_args(ObNullTransform, mutex);
  }

  int handle_event(int event, void *edata);

public:
  event::ObMIOBuffer *output_buf_;
  event::ObIOBufferReader *output_reader_;
  event::ObVIO *output_vio_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_TRANSFORM_INTERNAL_H
