/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef MALLOC_HOOK_H
#define MALLOC_HOOK_H
#include <cstddef>
#include <stdint.h>

namespace oceanbase {
namespace lib {
extern int64_t memalign_size;
static const uint32_t HOOK_MAGIC_CODE  = 0XA1B2C3D4;
struct HookHeader {
  HookHeader()
    : MAGIC_CODE_(HOOK_MAGIC_CODE), from_glibc_(0), offset_(0)
  {}
  uint32_t MAGIC_CODE_;
  uint32_t data_size_;
  uint8_t from_glibc_;
  uint64_t offset_;
  char padding__[8];
  char data_[0];
} __attribute__((aligned (16)));

static const uint32_t HOOK_HEADER_SIZE = offsetof(HookHeader, data_);

enum GLIBC_HOOK_OPT { GHO_NOHOOK, GHO_HOOK, GHO_NONULL };
extern __thread int glibc_hook_opt;
}  // lib
}  // oceanbase


#endif /* MALLOC_HOOK_H */
