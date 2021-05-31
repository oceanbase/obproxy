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

#ifndef _OCEABASE_LIB_OB_WORKER_H_
#define _OCEABASE_LIB_OB_WORKER_H_

#include <stdint.h>
#include <new>
#include "lib/ob_define.h"
#include "allocator/ob_fifo_allocator.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace common
{

class ObWorker
{
public:
  enum RunStatus { RS_RUN, RS_WAIT, RS_PAUSED };
  enum Status { WS_WAIT, WS_NOWAIT, WS_FREQUENT, WS_INVALID };

  ObWorker();
  virtual ~ObWorker() {}

  virtual Status check_wait()
  {
    return WS_NOWAIT;
  }

  // This function is called before worker waiting for some resources
  // and starting to give cpu out so that Multi-Tenancy would be aware
  // of this and assign cpu resource to another worker if necessary.
  //
  // Return:
  //   1. ture    wait sucessfully
  //   2. false   wait fail, should cancel this invocation
  bool sched_wait();

  // This function is opposite to `omt_sched_wait'. It notify
  // Multi-Tenancy that this worker has got enought resource and want to
  // run. Then Multi-Tenancy would judge whether the worker own the
  // right to go ahead.
  //
  // Return:
  //   1. true   the worker has right to go ahead
  //   2. false  the worker hasn't right to go ahead
  bool sched_run(int64_t waittime=0);

  void set_timeout_ts(int64_t timeout_ts);
  int64_t get_timeout_ts() const;
  int64_t get_timeout_remain() const;
  bool is_timeout() const;

  void set_rpc_tenant(uint64_t tenant_id);
  void reset_rpc_tenant();
  uint64_t get_rpc_tenant() const;

  inline ObIAllocator &ssstore_allocator();

  inline void set_tidx(int64_t tidx);
  inline void unset_tidx();
  inline int64_t get_tidx() const;
  inline ObArenaAllocator &get_allocator() ;

public:
  // static variables
  static inline ObWorker& self();

protected:
  static __thread ObWorker *self_;
  volatile RunStatus run_status_;


private:
  int64_t timeout_ts_;
  uint64_t rpc_tenant_id_;
  ObFIFOAllocator ssstore_allocator_;

  ObArenaAllocator allocator_;
  // worker index in its tenant
  int64_t tidx_;

  DISALLOW_COPY_AND_ASSIGN(ObWorker);
}; // end of class ObWorker

extern __thread char wbuf[sizeof (ObWorker)];
inline ObWorker &ObWorker::self()
{
  // wbuf won't been NULL.
  if (NULL == self_) {
    self_ = new (wbuf) ObWorker();
  }
  return *self_;
}

int worker_th_event(bool init);

void ObWorker::set_tidx(int64_t tidx)
{
  tidx_ = tidx;
}

void ObWorker::unset_tidx()
{
  tidx_ = -1;
}

int64_t ObWorker::get_tidx() const
{
  return tidx_;
}

ObIAllocator &ObWorker::ssstore_allocator()
{
  return ssstore_allocator_;
}

inline ObArenaAllocator &ObWorker::get_allocator()
{
  return allocator_;
}

} // end of namespace common
} // end of namespace oceanbase


#define THIS_WORKER oceanbase::common::ObWorker::self()

#endif /* _OCEABASE_LIB_OB_WORKER_H_ */
