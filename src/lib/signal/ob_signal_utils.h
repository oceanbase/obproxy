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

#ifndef OCEANBASE_SIGNAL_UTILS_H_
#define OCEANBASE_SIGNAL_UTILS_H_

#include <stdio.h>
#include <setjmp.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <poll.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include "lib/coro/co_var.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace common
{
void safe_sleep_micros(int64_t usec);

void safe_current_datetime_str(char *buf, int64_t len, int64_t &pos);
void safe_current_datetime_str_v2(char *buf, int64_t len, int64_t &pos);

int64_t safe_parray(char *buf, int64_t len, int64_t *array, int size);

} // namespace common
} // namespace oceanbase

extern "C" {
  int64_t safe_parray_c(char *buf, int64_t len, int64_t *array, int size);
}

#endif // OCEANBASE_SIGNAL_UTILS_H_
