#include "easy_util.h"

static char *parray(char *buf, int64_t len, int64_t *array, int size)
{
  int64_t pos = 0;
  int64_t count = 0;
  int64_t i = 0;
  for (i = 0; i < size; i++) {
    count = snprintf(buf + pos, len - pos, "0x%lx ", array[i]);
    if (count >= 0 && pos + count + 1 < len) {
      pos += count;
    } else {
      break;
    }
  }
  buf[pos + 1] = 0;
  return buf;
}

const char *easy_lbt()
{
  static __thread void *addrs[100];
  static __thread char buf[1024];
  int size = backtrace(addrs, 100);
  return parray(buf, sizeof(buf), (int64_t *)addrs, size);
}

