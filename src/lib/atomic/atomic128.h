/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_ATOMIC_ATOMIC128_
#define OCEANBASE_LIB_ATOMIC_ATOMIC128_

namespace types
{
struct uint128_t
{
  uint64_t lo;
  uint64_t hi;
}
__attribute__((__aligned__(16)));
}

inline bool cas128(volatile types::uint128_t *src, types::uint128_t *cmp, types::uint128_t with)
{
  bool result;
  __asm__ __volatile__
  (
      "\n\tlock cmpxchg16b %1"
      "\n\tsetz %0\n"
      : "=q"(result), "+m"(*src), "+d"(cmp->hi), "+a"(cmp->lo)
      : "c"(with.hi), "b"(with.lo)
      : "cc"
  );
  return result;
}

inline void load128(__uint128_t &dest, types::uint128_t *src)
{
  __asm__ __volatile__("\n\txor %%rax, %%rax;"
                       "\n\txor %%rbx, %%rbx;"
                       "\n\txor %%rcx, %%rcx;"
                       "\n\txor %%rdx, %%rdx;"
                       "\n\tlock cmpxchg16b %1;\n"
                       : "=&A"(dest)
                       : "m"(*src)
                       : "%rbx", "%rcx", "cc");
}

#define CAS128(src, cmp, with) cas128((types::uint128_t*)(src), ((types::uint128_t*)&(cmp)), *((types::uint128_t*)&(with)))
#define LOAD128(dest, src) load128((__uint128_t&)(dest), (types::uint128_t*)(src))

#endif //OCEANBASE_LIB_ATOMIC_ATOMIC128_
