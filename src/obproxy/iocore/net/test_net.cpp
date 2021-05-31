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

#include "ob_net.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{
using namespace event;

struct NetTesterSM : public ObContinuation
{
  ObVIO *read_vio_;
  ObIOBufferReader *reader_;
  ObNetVConnection *vc_;
  ObMIOBuffer *buf_;

  NetTesterSM(ObProxyMutex * mutex, ObNetVConnection * vc)
  : ObContinuation(mutex)
  {
    MUTEX_TRY_LOCK(lock, mutex_, vc->thread_);
    ob_release_assert(lock);
    vc_ = vc;
    SET_HANDLER(&NetTesterSM::handle_read);
    buf_ = new_miobuffer(32 * 1024);
    reader_ = buf_->alloc_reader();
    read_vio_ = vc->do_io_read(this, INT64_MAX, buf_);
    printf("create test sm\n");
  }


  int handle_read(int event, void *data)
  {
    UNUSED(data);
    int r;
    char *str;
    switch (event) {
    case VC_EVENT_READ_READY:
      r = (int)reader_->read_avail();
      str = new char[r + 10];
      reader_->read(str, r);
      printf("%s", str);
      fflush(stdout);
      break;
    case VC_EVENT_READ_COMPLETE:
      /* FALLSTHROUGH */
    case VC_EVENT_EOS:
      r = (int)reader_->read_avail();
      str = new char[r + 10];
      reader_->read(str, r);
      printf("%s", str);
      fflush(stdout);
    case VC_EVENT_ERROR:
      vc_->do_io_close();
      break;
    default:
      ob_release_assert(!"unknown event");

    }
    return EVENT_CONT;
  }
};

struct NetTesterAccept : public event::ObContinuation
{

  NetTesterAccept(ObProxyMutex * mutex)
  : ObContinuation(mutex)
  {
    SET_HANDLER(&NetTesterAccept::handle_accept);
  }

  int handle_accept(int event, void *data)
  {
    UNUSED(event);
    printf("Accepted a connection\n");
    fflush(stdout);
    ObNetVConnection *vc = (ObNetVConnection *) data;
    new NetTesterSM(new_proxy_mutex(), vc);
    return EVENT_CONT;
  }
};

void initialize_thread_for_mysql_sessions(ObEThread *thread, int thread_index)
{
  UNUSED(thread);
  UNUSED(thread_index);
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

using namespace oceanbase;
using namespace obproxy;
using namespace event;
using namespace net;

int main()
{
//  event_system_init(EVENT_SYSTEM_MODULE_VERSION);
//  //ObMIOBuffer *mbuf = new_miobuffer(5);
//  ObNetProcessor::ObAcceptOptions options;
//  options.local_port_ = 8080;
//  options.accept_threads_ = 1;
//  //options.f_callback_on_open_ = true;
//  options.frequent_accept_ = true;
//  //options.backdoor_ = true;

//  g_event_processor.start(2);
//  g_net_processor.start(1, 1024 * 1024);
//  g_net_processor.accept(new NetTesterAccept(new_proxy_mutex()), options);
//  this_thread()->execute();
}
