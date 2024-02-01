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

#include "common/ob_common_utility.h"

namespace oceanbase
{
namespace common
{
const char *print_server_role(const ObServerRole server_role)
{
  const char *role_string = NULL;
  switch (server_role) {
    case OB_CHUNKSERVER:
      role_string = "chunkserver";
      break;
    case OB_MERGESERVER:
      role_string = "mergeserver";
      break;
    case OB_ROOTSERVER:
      role_string = "rootserver";
      break;
    case OB_UPDATESERVER:
      role_string = "updateserver";
      break;
    case OB_SERVER:
      role_string = "observer";
      break;
    default:
      role_string = "invalid_role";
  }
  return role_string;
}

static int64_t reserved_stack_size = 64L << 10;

int64_t get_reserved_stack_size()
{
  return reserved_stack_size;
}

void set_reserved_stack_size(int64_t reserved_size)
{
  reserved_stack_size = reserved_size;
}

int check_stack_overflow(bool &is_overflow,
    int64_t reserved_size/* default equals 'reserved_stack_size' variable*/)
{
  int ret = OB_SUCCESS;
  is_overflow = false;
  size_t stack_size = 0;
  pthread_attr_t attr;
  char *stack_eof = NULL;
  void *cur_stack = NULL;
  void *stack_start = NULL;
  if (OB_UNLIKELY(0 != pthread_getattr_np(pthread_self(), &attr))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(EDIAG, "cannot get thread params", K(ret));
    is_overflow = true;
  } else if (OB_UNLIKELY(0 != pthread_attr_getstack(&attr, &stack_start, &stack_size))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(EDIAG, "cannot get thread statck params", K(ret));
    is_overflow = true;
  } else if (OB_UNLIKELY(0 != pthread_attr_destroy(&attr))) {
    is_overflow = true;
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(EDIAG, "destroy thread attr failed", K(ret));
  } else {
    stack_eof = static_cast<char *>(stack_start) + stack_size;
    cur_stack = &stack_start;
    if (OB_UNLIKELY(static_cast<int64_t>(stack_size) < reserved_size)) { //stack size is the whole stack size
      ret = OB_ERR_UNEXPECTED;
      is_overflow = true;
      COMMON_LOG(EDIAG, "stack size smaller than reserved_stack_size ",
          K(ret), K(stack_size), K(reserved_size));
    } else if (OB_UNLIKELY(stack_eof < static_cast<char *>(cur_stack))) {
      is_overflow = true;
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(EDIAG, "stack incorrect params", K(ret), KP(stack_eof), KP(cur_stack));
    } else {
      int64_t cur_stack_used = stack_eof - (static_cast<char *>(cur_stack));
      COMMON_LOG(DEBUG, "stack info ", K(cur_stack_used), K(stack_size), K(reserved_size));
      if (OB_UNLIKELY(cur_stack_used > (static_cast<int64_t>(stack_size) - reserved_size))) {
        is_overflow = true;
        COMMON_LOG(WDIAG, "stack possible overflow", KP(cur_stack), KP(stack_eof),
            KP(stack_start), K(stack_size), K(reserved_size), K(cur_stack_used));
      }
    }
  }
  return ret;
}
} // end of namespace common
} // end of namespace oceanbse
