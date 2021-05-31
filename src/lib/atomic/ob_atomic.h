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

#ifndef _OB_ATOMIC_H_
#define _OB_ATOMIC_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{

#define OB_GCC_VERSION (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__)

#define __COMPILER_BARRIER() asm volatile("" ::: "memory")
#define WEAK_BARRIER() __COMPILER_BARRIER()
#define MEM_BARRIER() __sync_synchronize()
#if defined(__aarch64__)
#define PAUSE() asm("yield\n")
#else
#define PAUSE() asm("pause\n")
#endif

//#if OB_GCC_VERSION > 40704
//#define ATOMIC_LOAD(x) __atomic_load_n((x), __ATOMIC_SEQ_CST)
//#define ATOMIC_STORE(x, v) __atomic_store_n((x), (v), __ATOMIC_SEQ_CST)
//#else
#define ATOMIC_LOAD(x) ({__COMPILER_BARRIER(); *(x);})
#define ATOMIC_STORE(x, v) ({__COMPILER_BARRIER(); *(x) = v; __sync_synchronize(); })
//#endif

#define ATOMIC_FAAx(val, addv, id)                              \
  ({ UNUSED(id); __sync_fetch_and_add((val), (addv)); })
#define ATOMIC_AAFx(val, addv, id)                              \
  ({ UNUSED(id); __sync_add_and_fetch((val), (addv)); })
#define ATOMIC_FASx(val, subv, id)                              \
  ({ UNUSED(id); __sync_fetch_and_sub((val), (subv)); })
#define ATOMIC_SAFx(val, subv, id)                              \
  ({ UNUSED(id); __sync_sub_and_fetch((val), (subv)); })
#define ATOMIC_TASx(val, newv, id)                              \
  ({ UNUSED(id); __sync_lock_test_and_set((val), (newv)); })
#define ATOMIC_SETx(val, newv, id)                              \
  ({ UNUSED(id); __sync_lock_test_and_set((val), (newv)); })
#define ATOMIC_VCASx(val, cmpv, newv, id)                       \
  ({ UNUSED(id);                                               \
    __sync_val_compare_and_swap((val), (cmpv), (newv)); })
#define ATOMIC_BCASx(val, cmpv, newv, id)                       \
  ({ UNUSED(id);                                               \
    __sync_bool_compare_and_swap((val), (cmpv), (newv)); })

#define LA_ATOMIC_ID 0
#define ATOMIC_FAA(val, addv) ATOMIC_FAAx(val, addv, LA_ATOMIC_ID)
#define ATOMIC_AAF(val, addv) ATOMIC_AAFx(val, addv, LA_ATOMIC_ID)
#define ATOMIC_FAS(val, subv) ATOMIC_FASx(val, subv, LA_ATOMIC_ID)
#define ATOMIC_SAF(val, subv) ATOMIC_SAFx(val, subv, LA_ATOMIC_ID)
#define ATOMIC_TAS(val, newv) ATOMIC_TASx(val, newv, LA_ATOMIC_ID)
#define ATOMIC_SET(val, newv) ATOMIC_SETx(val, newv, LA_ATOMIC_ID)
#define ATOMIC_VCAS(val, cmpv, newv) ATOMIC_VCASx(val, cmpv, newv, LA_ATOMIC_ID)
#define ATOMIC_BCAS(val, cmpv, newv) ATOMIC_BCASx(val, cmpv, newv, LA_ATOMIC_ID)

inline int64_t inc_update(int64_t* v_, int64_t x)
{
  int64_t ov = 0;
  int64_t nv = ATOMIC_LOAD(v_);
  while ((ov = nv) < x) {
    if ((nv = ATOMIC_VCAS(v_, ov, x)) == ov) {
      nv = x;
    }
  }
  return nv;
}

#define ATOMIC_CAS(val, cmpv, newv) ATOMIC_VCAS((val), (cmpv), (newv))
#define ATOMIC_INC(val) do { IGNORE_RETURN ATOMIC_AAF((val), 1); } while (0)
#define ATOMIC_DEC(val) do { IGNORE_RETURN ATOMIC_SAF((val), 1); } while (0)

}
}

#endif /* _OB_ATOMIC_H_ */
