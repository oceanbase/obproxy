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

#ifndef OCEANBASE_LOCK_TC_REF_H_
#define OCEANBASE_LOCK_TC_REF_H_
#include "lib/thread_local/ob_tsi_utils.h"

namespace oceanbase
{
namespace common
{
class TCRef
{
public:
  enum{ N_REF_PER_THREAD = 8, REF_LIMIT = INT32_MAX/2 };
  TCRef() { memset(ref_, 0, sizeof(ref_)); }
  ~TCRef() {}
  int32_t born(int32_t* p) { return xref_(p, REF_LIMIT); }
  int32_t end(int32_t* p) { return xref_(p, -REF_LIMIT); }
  void sync() {
    int64_t end = get_max_itid() * N_REF_PER_THREAD;
    for(int64_t i = 0; i < end; i++) {
      int32_t** slot = ref_ + i;
      int32_t* p = NULL;
      if (NULL != ATOMIC_LOAD(slot) && NULL != (p = ATOMIC_TAS(slot, NULL))) {
        xref_(p, 1);
      }
    }
  }
  int32_t inc_ref(int32_t* p) {
    int32_t ref_cnt = REF_LIMIT;
    int64_t start = N_REF_PER_THREAD * get_itid();
    int64_t i = 0;
    for(i = 0; i < N_REF_PER_THREAD; i++) {
      int32_t** slot = ref_ + start + i;
      if (NULL == ATOMIC_LOAD(slot) && ATOMIC_BCAS(slot, NULL, p)) {
        break;
      }
    }
    if (i == N_REF_PER_THREAD) {
      ref_cnt = xref_(p, 1);
    }
    return ref_cnt;
  }
  int32_t dec_ref(int32_t* p) {
    int32_t ref_cnt = REF_LIMIT;
    int64_t start = N_REF_PER_THREAD * get_itid();
    int64_t i = 0;
    for(i = 0; i < N_REF_PER_THREAD; i++) {
      int32_t** slot = ref_ + start + i;
      if (p == ATOMIC_LOAD(slot) && ATOMIC_BCAS(slot, p, NULL)) {
        break;
      }
    }
    if (i == N_REF_PER_THREAD) {
      ref_cnt = xref_(p, -1);
    }
    return ref_cnt;
  }
private:
  int32_t xref_(int32_t* p, int32_t x) { return ATOMIC_AAF(p, x); }
private:
  int32_t* ref_[OB_MAX_THREAD_NUM * N_REF_PER_THREAD];
} CACHE_ALIGNED;
}; // end namespace common
}; // end namespace oceanbase

#endif /* OCEANBASE_LOCK_TC_REF_H_ */

