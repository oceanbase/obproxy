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

#ifndef OCEANBASE_COMMON_VERSION_H_
#define OCEANBASE_COMMON_VERSION_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

static const int64_t MAX_VERSION_LENGTH = 10;

const char *build_version();
const char *build_date();
const char *build_time();
const char *build_flags();

#ifdef __cplusplus
}
#endif

namespace oceanbase
{
namespace common
{
void get_package_and_svn(char *server_version, int64_t buf_len);
int get_package_version_array(int *versions, int64_t size);
int check_version_valid(int64_t len, const char *mysql_version);
}
}

#endif
