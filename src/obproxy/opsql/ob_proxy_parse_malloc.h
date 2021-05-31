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

#ifndef OBPROXY_PARSER_MALLOC_H
#define OBPROXY_PARSER_MALLOC_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// NB: Be careful!!!, it is only used in parser module
// NOTE, obproxy_parse_malloc will memset the allocated memory to 0
extern void *obproxy_parse_malloc(const size_t nbyte, void *malloc_pool);
extern void *obproxy_parse_realloc(void *ptr, size_t nbyte, void *malloc_pool);
extern void obproxy_parse_free(void *ptr);
extern char *obproxy_parse_strndup(const char *str, size_t nbyte, void *malloc_pool);
extern char *obproxy_parse_strdup(const char *str, void *malloc_pool, int64_t *out_len);

#ifdef __cplusplus
}
#endif

#endif //OBPROXY_PARSER_MALLOC_H

