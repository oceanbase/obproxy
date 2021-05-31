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

#ifndef OBPROXY_IOCORE_API_H
#define OBPROXY_IOCORE_API_H

#include "iocore/eventsystem/ob_event_system.h"
#include "iocore/net/ob_net.h"
#include "proxy/api/ob_api_defs.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

enum ObContInternalMagic
{
  OB_CONT_INTERN_MAGIC_ALIVE = 0x00009631,
  OB_CONT_INTERN_MAGIC_DEAD = 0xDEAD9631
};

class ObContInternal : public event::ObDummyVConnection
{
public:
  ObContInternal(ObEventFunc funcp, event::ObProxyMutex *mutexp);
  virtual ~ObContInternal() {}

  static ObContInternal *alloc(ObEventFunc funcp, event::ObProxyMutex *mutexp)
  {
    return op_reclaim_alloc_args(ObContInternal, funcp, mutexp);
  }

  virtual void destroy();

  void handle_event_count(int event);
  int handle_event(int event, void *edata);

public:
  void *data_;
  ObEventFunc event_func_;
  volatile int event_count_;
  volatile int closed_;
  bool deletable_;
  bool deleted_;
  ObContInternalMagic free_magic_;
};

class ObVConnInternal : public ObContInternal
{
public:
  ObVConnInternal(ObEventFunc funcp, event::ObProxyMutex *mutexp);
  virtual ~ObVConnInternal() {}

  static ObVConnInternal *alloc(ObEventFunc funcp, event::ObProxyMutex *mutexp)
  {
    return op_reclaim_alloc_args(ObVConnInternal, funcp, mutexp);
  }

  virtual void destroy();

  int handle_event(int event, void *edata);

  event::ObVIO *do_io_read(event::ObContinuation *c, const int64_t nbytes,
                           event::ObMIOBuffer *buf);

  event::ObVIO *do_io_write(event::ObContinuation *c,
                            const int64_t nbytes,
                            event::ObIOBufferReader *buf);

  void do_io_transform(event::ObVConnection *vc);

  void do_io_close(const int lerrno = -1);

  void do_io_shutdown(const event::ShutdownHowToType howto);

  void reenable(event::ObVIO *vio);

  void retry(int64_t delay);

  bool get_data(const int32_t id, void *data);
  bool set_data(const int32_t id, void *data);

public:
  event::ObVIO read_vio_;
  event::ObVIO write_vio_;
  event::ObVConnection *output_vc_;
};

struct ObThreadInternal : public event::ObEThread
{
  //TODO::when construct DEDICATED ObEThread, pending_event_ must be specified
  //current construction is error
  ObThreadInternal()
      : ObEThread(event::DEDICATED, -1), func_(NULL), data_(NULL)
  {  }

  virtual ~ObThreadInternal() {}

  static event::ObEThread *create_thread(ObThreadFunc func, void *data);

  ObThreadFunc func_;
  void *data_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif  // OBPROXY_IOCORE_API_H
