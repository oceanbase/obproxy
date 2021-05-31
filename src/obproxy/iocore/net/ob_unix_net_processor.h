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

#ifndef OBPROXY_UNIX_NET_PROCESSOR_H
#define OBPROXY_UNIX_NET_PROCESSOR_H

#include "iocore/net/ob_net_def.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{

class ObUnixNetVConnection;
class ObNetAccept;

struct ObUnixNetProcessor : public ObNetProcessor
{
public:
  ObUnixNetProcessor()  { }
  virtual ~ObUnixNetProcessor() { }

  event::ObAction *accept_internal(
      event::ObContinuation &cont,
      int fd,
      ObAcceptOptions const &opt);

  int connect_internal(
      event::ObContinuation &cont,
      const sockaddr &target,
      ObNetVCOptions *options = NULL);

  void upgrade_etype(event::ObEventThreadType &etype) { UNUSED(etype); };

  ObNetAccept *create_net_accept();

  virtual ObNetVConnection *allocate_vc();

  virtual int start();

private:
  DISALLOW_COPY_AND_ASSIGN(ObUnixNetProcessor);
};

extern ObUnixNetProcessor g_unix_net_processor;

// Set up a thread to receive events from the ObNetProcessor
// This function should be called for all threads created to
// accept such events by the ObEventProcesor.
extern int initialize_thread_for_net(event::ObEThread *thread);

uint32_t net_next_connection_number();

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_UNIX_NET_PROCESSOR_H
