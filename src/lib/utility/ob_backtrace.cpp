/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX LIB

#include "lib/utility/ob_backtrace.h"

#include <stdio.h>
#include <execinfo.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log.h"
#include "utils/ob_proxy_lib.h"
#include "common/ob_common_utility.h"

namespace oceanbase
{
namespace common
{
int light_backtrace(void **buffer, int size)
{
  int64_t rbp = 0;
#if defined(__x86_64__)
  asm("mov %%rbp, %0" : "=r"(rbp));
#elif defined(__aarch64__)
  asm("mov %0, x29" : "=r"(rbp));
#endif
  return light_backtrace(buffer, size, rbp);
}

int light_backtrace(void **buffer, int size, int64_t rbp)
{
  int rv = 0;
  if (rv < size) {
    int (*fp)(void**, int, int64_t) = light_backtrace;
    buffer[rv++] = (void*)fp;
  }
  void *stack_addr = nullptr;
  size_t stack_size = 0;
  if (OB_LIKELY(OB_SUCCESS == get_stackattr(stack_addr, stack_size))) {
#define addr_in_stack(addr) (addr >= (int64_t)stack_addr && addr < (int64_t)stack_addr + stack_size)
    while (rbp != 0 && rv < size) {
      if (!addr_in_stack(*(int64_t*)rbp) &&
          !FALSE_IT(rbp += 16) &&
          !addr_in_stack(*(int64_t*)rbp)) {
        break;
      } else {
        int64_t return_addr = rbp + 8;
        buffer[rv++] = (void*)*(int64_t*)return_addr;
        rbp = *(int64_t*)rbp;
      }
    }
  }
  return rv;
}

int ob_backtrace(void **buffer, int size)
{
  int rv = 0;
  if (OB_LIKELY(g_enable_backtrace)) {
    rv = backtrace(buffer, size);
  }
  return rv;
}
bool read_min_max_addr(int64_t &min_addr, int64_t &max_addr)
{
  bool bret = false;
  FILE *fp = fopen("/proc/self/maps", "r");
  if (!fp) return bret;
  char line[512];
  min_addr = INT64_MAX;
  max_addr = -1;
  int64_t addr = (int64_t)__func__;
  while (fgets(line, sizeof(line), fp) != NULL) {
    int64_t start, end, inode, offset, major, minor;
    char perms[8];
    char path[256];
    int n = sscanf(line,
                   "%lx-%lx %4s %lx %lx:%lx %ld %255s",
                   &start, &end, perms,
                   &offset, &major, &minor, &inode, path);
    LOG_DEBUG("get line", K(line));
    if (n < 8) {
      continue;
    }
    uint64_t dst_inode = inode;
    if (start <= addr && addr < end) {
      bret = true;
      fseek(fp, 0, SEEK_SET);
      while (fgets(line, sizeof(line), fp) != NULL) {
        int n = sscanf(line,
                       "%lx-%lx %4s %lx %lx:%lx %ld %255s",
                       &start, &end, perms,
                       &offset, &major, &minor, &inode, path);
        if (n < 8) {
          continue;
        }
        if (dst_inode != inode) {
          continue;
        }
        if (start < min_addr) {
          min_addr = start;
          _LOG_INFO("get min_addr, start=%#llx, end=%#llx", static_cast<unsigned long long>(start), static_cast<unsigned long long>(end));
          LOG_INFO("code addr info", K(perms), K(offset), K(major), K(minor), K(inode), K(path));
        }
        if (end > max_addr) {
          max_addr = end;
          _LOG_INFO("get min_addr, start=%#llx, end=%#llx", static_cast<unsigned long long>(start), static_cast<unsigned long long>(end));
          LOG_INFO("code addr info", K(perms), K(offset), K(major), K(minor), K(inode), K(path));
        }
      }
      break;
    }
  }
  fclose(fp);

  return bret;
}
bool g_enable_backtrace = true;
struct ProcMapInfo
{
  int64_t code_start_addr_;
  int64_t code_end_addr_;
  bool is_inited_;
};
ProcMapInfo g_proc_map_info{.code_start_addr_ = -1, .code_end_addr_ = -1, .is_inited_ = false};
void init_proc_map_info()
{
  read_min_max_addr(g_proc_map_info.code_start_addr_, g_proc_map_info.code_end_addr_);
  g_proc_map_info.is_inited_ = true;
}
int64_t get_rel_offset(int64_t addr)
{
  int64_t code_start_addr = -1;
  int64_t code_end_addr = -1;
  if (OB_UNLIKELY(!g_proc_map_info.is_inited_)) {
    read_min_max_addr(code_start_addr, code_end_addr);
  } else {
    code_start_addr = g_proc_map_info.code_start_addr_;
    code_end_addr = g_proc_map_info.code_end_addr_;
  }
  if (code_start_addr != -1) {
    if (OB_LIKELY(addr >= code_start_addr && addr < code_end_addr)) {
      addr -= code_start_addr;
    }
  }
  return addr;
}
constexpr int MAX_ADDRS_COUNT = 100;
using PointerBuf = void *[MAX_ADDRS_COUNT];
static __thread PointerBuf addrs;
static __thread char buffer[LBT_BUFFER_LENGTH];

char *lbt()
{
  int size = OB_BACKTRACE_M(addrs, MAX_ADDRS_COUNT);
  if (OB_UNLIKELY(size > MAX_ADDRS_COUNT)) {
    MPRINT("unknownn backtrace size %d, errno:%s\n", size, strerror(errno));
    size = MAX_ADDRS_COUNT;
  }
  return parray(*&buffer, LBT_BUFFER_LENGTH, (int64_t *)addrs, size);
}

char *lbt(char *buf, int32_t len)
{
  int size = OB_BACKTRACE_M(addrs, MAX_ADDRS_COUNT);
  if (OB_UNLIKELY(size > MAX_ADDRS_COUNT)) {
    MPRINT("unknownn backtrace size %d, errno:%s\n", size, strerror(errno));
    size = MAX_ADDRS_COUNT;
  }
  return parray(buf, len, (int64_t *)addrs, size);
}

char *parray(int64_t *array, int size)
{
  return parray(buffer, LBT_BUFFER_LENGTH, array, size);
}

char *parray(char *buf, int64_t len, int64_t *array, int size)
{
  //As used in lbt, and lbt used when print error log.
  //Can not print error log this function.
  if (NULL != buf && len > 0 && NULL != array) {
    int64_t pos = 0;
    int64_t count = 0;
    for (int64_t i = 0; i < size; i++) {
      int64_t addr = get_rel_offset(array[i]);
      if (0 == i) {
        count = snprintf(buf + pos, len - pos, "0x%lx", addr);
      } else {
        count = snprintf(buf + pos, len - pos, " 0x%lx", addr);
      }
      if (count >= 0 && pos + count < len) {
        pos += count;
      } else {
        // buf not enough
        break;
      }
    }
    buf[pos] = 0;
  }
  return buf;
}
EXTERN_C_BEGIN
int ob_backtrace_c(void **buffer, int size)
{
  return OB_BACKTRACE_M(buffer, size);
}
char *parray_c(char *buf, int64_t len, int64_t *array, int size)
{
  return parray(buf, len, array, size);
}
int64_t get_rel_offset_c(int64_t addr)
{
  return get_rel_offset(addr);
}
EXTERN_C_END
} // end namespace common
} // end namespace oceanbase