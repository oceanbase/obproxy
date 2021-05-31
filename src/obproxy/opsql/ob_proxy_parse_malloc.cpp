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

#define USING_LOG_PREFIX PROXY
#include <string.h>
#include "opsql/ob_proxy_parse_malloc.h"
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"

using namespace oceanbase::common;

// get memory from the thread obStringBuf, and not release untill thread quits
void *obproxy_parse_malloc(const size_t nbyte, void *malloc_pool)
{
  void *ptr = NULL;
  size_t headlen = sizeof(int64_t);
  if (OB_ISNULL(malloc_pool)) {
    LOG_ERROR("malloc pool is NULL");
  } else if (OB_UNLIKELY(nbyte <= 0)) {
    LOG_ERROR("wrong size of obproxy_parse_malloc", K(nbyte));
  } else {
    ObIAllocator *alloc_buf = static_cast<ObIAllocator *>(malloc_pool);
    if (OB_UNLIKELY(NULL == (ptr = alloc_buf->alloc(headlen + nbyte)))) {
      LOG_ERROR("alloc memory failed", K(nbyte));
    } else {
      *(static_cast<int64_t *>(ptr)) = nbyte;
      ptr = static_cast<char *>(ptr) + headlen;
      //MEMSET(ptr, 0, nbyte);
    }
  }
  return ptr;
}

/* ptr must point to a memory allocated by obproxy_parse_malloc */
void *obproxy_parse_realloc(void *ptr, size_t nbyte, void *malloc_pool)
{
  void *new_ptr = NULL;
  //need not to check nbyte
  if (OB_ISNULL(malloc_pool)) {
    LOG_ERROR("malloc pool is NULL");
  } else {
    ObIAllocator *alloc_buf = static_cast<ObIAllocator *>(malloc_pool);
    if (OB_UNLIKELY(NULL == ptr)) {
      new_ptr = obproxy_parse_malloc(nbyte, malloc_pool);
    } else {
      size_t headlen = sizeof(int64_t);
      if (OB_UNLIKELY(NULL == (new_ptr = alloc_buf->alloc(headlen + nbyte)))) {
        LOG_ERROR("alloc memory failed");
      } else {
        int64_t obyte = *(reinterpret_cast<int64_t *>(static_cast<char *>(ptr) - headlen));
        *(static_cast<int64_t *>(new_ptr)) = nbyte;
        new_ptr = static_cast<char *>(new_ptr) + headlen;
        MEMMOVE(new_ptr, ptr, static_cast<int64_t>(nbyte) > obyte ? obyte : nbyte);
        alloc_buf->free(static_cast<char *>(ptr) - headlen);
      }
    }
  }
  return new_ptr;
}

char *obproxy_parse_strndup(const char *str, size_t nbyte, void *malloc_pool)
{
  char *new_str = NULL;
  //need not to check nbyte
  if (OB_ISNULL(str)) {
    LOG_ERROR("duplicate string is NULL");
  } else {
    if (OB_LIKELY(NULL != (new_str = static_cast<char *>(obproxy_parse_malloc(nbyte + 1,
                                                                              malloc_pool))))) {
      MEMMOVE(new_str, str, nbyte);
      new_str[nbyte] = '\0';
    } else {
      LOG_ERROR("parse_strdup gets string buffer error");
    }
  }
  return new_str;
}

char *obproxy_parse_strdup(const char *str, void *malloc_pool, int64_t *out_len)
{
  char *out_str = NULL;
  if (OB_ISNULL(str)) {
    LOG_ERROR("duplicate string is NULL");
  } else if (OB_ISNULL(out_len)) {
    LOG_ERROR("out_len is NULL");
  } else {
    size_t dup_len = STRLEN(str);
    out_str = obproxy_parse_strndup(str, dup_len, malloc_pool);
    *out_len = dup_len;
  }
  return out_str;
}

void obproxy_parse_free(void *ptr)
{
  UNUSED(ptr);
  /* do nothing, we don't really free the memory */
}


// must implement below 4 func
void *parser_alloc_buffer(void *malloc_pool, const int64_t buff_size)
{
  return obproxy_parse_malloc(buff_size, malloc_pool);
}

void parser_free_buffer(void *malloc_pool, void *buffer)
{
  UNUSED(malloc_pool);
  obproxy_parse_free(buffer);
}

extern "C"
{
int check_stack_overflow_in_c(int *check_overflow)
{
  *check_overflow = 0;
  return 0;
}

bool check_stack_overflow_c()
{
  return 0;
}
}

