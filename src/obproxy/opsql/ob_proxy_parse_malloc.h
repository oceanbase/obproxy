/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

