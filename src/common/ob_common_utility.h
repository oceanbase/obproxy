/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEABASE_COMMON_OB_COMMON_UTILITY_H_
#define _OCEABASE_COMMON_OB_COMMON_UTILITY_H_

#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
extern const char *print_server_role(const common::ObServerRole server_role);

//@brief recursive function call should use this function to check if recursion is too deep
//to avoid stack overflow, default reserved statck size is 1M
extern int64_t get_reserved_stack_size();
extern void set_reserved_stack_size(int64_t reserved_size);
extern int check_stack_overflow(
    bool &is_overflow,
    int64_t reserved_stack_size = get_reserved_stack_size(),
    int64_t *used_size = nullptr);
extern int get_stackattr(void *&stackaddr, size_t &stacksize);
extern void set_stackattr(void *stackaddr, size_t stacksize);

// return OB_SIZE_OVERFLOW if stack overflow
inline int check_stack_overflow(void)
{
  bool overflow = false;
  int ret = check_stack_overflow(overflow);
  return OB_LIKELY(OB_SUCCESS == ret) && OB_UNLIKELY(overflow) ? OB_SIZE_OVERFLOW : ret;
}


} // end of namespace common
} // end of namespace oceanbase

#endif /* _OCEABASE_COMMON_OB_COMMON_UTILITY_H_ */
