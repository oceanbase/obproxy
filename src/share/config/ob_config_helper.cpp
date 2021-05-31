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

#include "share/config/ob_config_helper.h"
#include "share/config/ob_config.h"

namespace oceanbase
{
namespace common
{
bool ObConfigIpChecker::check(const ObConfigItem &t) const
{
  struct sockaddr_in sa;
  int result = inet_pton(AF_INET, t.str(), &(sa.sin_addr));
  return result != 0;
}

ObConfigConsChecker:: ~ObConfigConsChecker()
{
  if (NULL != left_) {
    delete left_;
  }
  if (NULL != right_) {
    delete right_;
  }
}
bool ObConfigConsChecker::check(const ObConfigItem &t) const
{
  return (NULL == left_ ? true : left_->check(t))
         && (NULL == right_ ? true : right_->check(t));
}

bool ObConfigGreaterThan::check(const ObConfigItem &t) const
{
  return t > val_;
}

bool ObConfigGreaterEqual::check(const ObConfigItem &t) const
{
  return t >= val_;
}

bool ObConfigLessThan::check(const ObConfigItem &t) const
{
  return t < val_;
}

bool ObConfigLessEqual::check(const ObConfigItem &t) const
{
  return t <= val_;
}

bool ObConfigLogLevelChecker::check(const ObConfigItem &t) const
{
  return OB_SUCCESS == OB_LOGGER.parse_check(t.str(), static_cast<int32_t>(STRLEN(t.str())));
}

int64_t ObConfigCapacityParser::get(const char *str, bool &valid)
{
  char *p_unit = NULL;
  int64_t value = 0;

  if (OB_ISNULL(str) || '\0' == str[0]) {
    valid = false;
  } else {
    valid = true;
    value = std::max(0L, strtol(str, &p_unit, 0));

    if (0 == STRCASECMP("b", p_unit)
        || 0 == STRCASECMP("byte", p_unit)) {
      // do nothing
    } else if (0 == STRCASECMP("kb", p_unit)
               || 0 == STRCASECMP("k", p_unit)) {
      value <<= CAP_KB;
    } else if ('\0' == *p_unit
               || 0 == STRCASECMP("mb", p_unit)
               || 0 == STRCASECMP("m", p_unit)) {
      // default is metabyte
      value <<= CAP_MB;
    } else if (0 == STRCASECMP("gb", p_unit)
               || 0 == STRCASECMP("g", p_unit)) {
      value <<= CAP_GB;
    } else if (0 == STRCASECMP("tb", p_unit)
               || 0 == STRCASECMP("t", p_unit)) {
      value <<= CAP_TB;
    } else if (0 == STRCASECMP("pb", p_unit)
               || 0 == STRCASECMP("p", p_unit)) {
      value <<= CAP_PB;
    } else {
      valid = false;
      OB_LOG(ERROR, "set capacity error", K(str), K(p_unit));
    }
  }

  return value;
}

} // end of namepace common
} // end of namespace oceanbase
