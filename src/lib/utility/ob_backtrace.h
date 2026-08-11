/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_COMMON_OB_BACKTRACE_H_
#define OCEANBASE_COMMON_OB_BACKTRACE_H_

#include <execinfo.h>
#include<inttypes.h>

namespace oceanbase
{
namespace common
{
void init_proc_map_info();
extern bool g_enable_backtrace;
const int64_t LBT_BUFFER_LENGTH = 1024;
int light_backtrace(void **buffer, int size);
int light_backtrace(void **buffer, int size, int64_t rbp);
int ob_backtrace(void **buffer, int size);
// save one layer of call stack
#define OB_BACKTRACE_M(buffer, size)                      \
  ({                                                      \
    int rv = 0;                                           \
    if (OB_LIKELY(::oceanbase::common::g_enable_backtrace)) {   \
      rv = backtrace(buffer, size);                       \
    }                                                     \
  rv;                                                     \
  })

int64_t get_rel_offset(int64_t addr);
inline int ptr_lbt(void** buf, int max_size)
{
  return backtrace(buf, max_size);
}
char *lbt();
char *lbt(char *buf, int32_t len);
char *parray(int64_t *array, int size);
char *parray(char *buf, int64_t len, int64_t *array, int size);
} // end namespace common
} // end namespace oceanbase
#endif //OCEANBASE_COMMON_OB_BACKTRACE_H_