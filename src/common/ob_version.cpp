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

#include <stdio.h>
#include "common/ob_version.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{

void get_package_and_svn(char *server_version, int64_t buf_len)
{
  const char *server_version_template = "%s_%s(%s %s)";
  (void) snprintf(server_version, buf_len, server_version_template, PACKAGE_VERSION,
           build_version(), build_date(), build_time());
}

int get_package_version_array(int *versions, const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  char buf[64] = {0};
  char *ptr = buf;
  const char *delim = ".";
  char *saveptr = NULL;
  char *token = NULL;
  const int64_t VERSION_ITEM = 3;

  if (NULL == versions || VERSION_ITEM > size) {
    COMMON_LOG(WARN, "invalid argument", KP(versions), K(size));
    ret = OB_INVALID_ARGUMENT;
  } else {
    strncpy(buf, PACKAGE_VERSION, size);
    for (i = 0; i < size; i++) {
      if (NULL != (token = strtok_r(ptr, delim, &saveptr))) {
        versions[i] = atoi(token);
        COMMON_LOG(INFO, "token", K(versions[i]));
      } else {
        break;
      }
      ptr = NULL;
    }
    if (VERSION_ITEM != i) {
      COMMON_LOG(WARN, "invalid package version", K(PACKAGE_VERSION));
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

int check_version_valid(const int64_t len, const char *mysql_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mysql_version) || OB_UNLIKELY(len == 0) || OB_UNLIKELY(len > MAX_VERSION_LENGTH)) {
    ret = OB_INVALID_CONFIG;
  } else if (!isdigit(mysql_version[0]) || !isdigit(mysql_version[len - 1])) {
    ret = OB_INVALID_CONFIG;
  } else {
    uint32_t i = 1;
    for (; i < len - 1; i++) {
      if ((mysql_version[i] == '.')) {
        if (mysql_version[i + 1] == '.') {// should not has two '.' continuous
          ret = OB_INVALID_CONFIG;
          break;
        }
      } else if (!isdigit(mysql_version[i])) { // should be digit or '.'
        ret = OB_INVALID_CONFIG;
        break;
      }
    }
  }

  return ret;
}

}
}
