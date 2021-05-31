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

#ifndef OBPROXY_TRANSFORM_H
#define OBPROXY_TRANSFORM_H

#include "iocore/eventsystem/ob_event_system.h"
#include "proxy/api/ob_api_internal.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define TRANSFORM_READ_READY   (TRANSFORM_EVENTS_START + 0)

class ObTransformProcessor
{
public:
  void start();

public:
  event::ObVConnection *open(event::ObContinuation *cont, ObAPIHook *hooks);
  ObVConnInternal *null_transform(event::ObProxyMutex *mutex);
};

#if OB_HAS_TESTS
class ObTransformTest
{
public:
  static void run();
};
#endif

/**
 * A protocol class.
 * This provides transform VC specific methods for external access
 * without exposing internals or requiring extra includes.
 */
class ObTransformVCChain : public event::ObVConnection
{
protected:
  // Required constructor
  explicit ObTransformVCChain(event::ObProxyMutex *m);
  virtual ~ObTransformVCChain() {}

public:
  /**
   * Compute the backlog. This is the amount of data ready to read
   * for each element of the chain. If limit is non-negative then
   * the method will return as soon as the computed backlog is at
   * least that large. This provides for more efficient checking if
   * the caller is interested only in whether the backlog is at least
   * limit. The default is to accurately compute the backlog.
   */
  virtual uint64_t backlog(uint64_t limit = UINT64_MAX) = 0;
};

inline ObTransformVCChain::ObTransformVCChain(event::ObProxyMutex *m)
    : event::ObVConnection(m)
{
}

extern ObTransformProcessor g_transform_processor;

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_TRANSFORM_H

