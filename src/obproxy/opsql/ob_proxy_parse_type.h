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

#ifndef OBPROXY_PARSE_TYPE_H
#define OBPROXY_PARSE_TYPE_H

#include <stdint.h>
typedef enum _ObProxyParseQuoteType
{
  OBPROXY_QUOTE_T_INVALID = 0,
  OBPROXY_QUOTE_T_SINGLE,
  OBPROXY_QUOTE_T_DOUBLE,
  OBPROXY_QUOTE_T_BACK,
  OBPROXY_QUOTE_T_MAX
} ObProxyParseQuoteType;

typedef struct _ObProxyParseString
{
  char *str_;
  char *end_ptr_;
  int32_t str_len_;
  ObProxyParseQuoteType quote_type_;
} ObProxyParseString;

typedef struct _ObProxyTableInfo
{
  ObProxyParseString database_name_;
  ObProxyParseString package_name_;
  ObProxyParseString table_name_;
  ObProxyParseString alias_name_;
  ObProxyParseString dblink_name_;
} ObProxyTableInfo;

#endif /* end of OBPROXY_PARSE_TYPE_H */
