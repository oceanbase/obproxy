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

#ifndef OCEANBASE_SHARE_CONFIG_OB_SYSTEM_CONFIG_VALUE_H_
#define OCEANBASE_SHARE_CONFIG_OB_SYSTEM_CONFIG_VALUE_H_

#include "lib/ob_define.h"


namespace oceanbase
{
namespace common
{
class ObSystemConfigValue
{
public:
  ObSystemConfigValue();
  virtual ~ObSystemConfigValue() {}

  void set_value(const char *value);
  void set_value(const ObString &value);
  const char *value() const { return value_; }
  void set_info(const char *info);
  void set_info(const ObString &info);
  const char *info() const { return info_; }
  void set_section(const char *section);
  void set_section(const ObString &section);
  const char *section() const { return section_; }
  void set_visible_level(const char *visible_level);
  void set_visible_level(const ObString &visible_level);
  const char *visible_level() const { return visible_level_; }
  void set_need_reboot(const bool &need_reboot);
  const char *need_reboot() const { return need_reboot_; }

private:
  char value_[OB_MAX_CONFIG_VALUE_LEN];
  char info_[OB_MAX_CONFIG_INFO_LEN];
  char section_[OB_MAX_CONFIG_SECTION_LEN];
  char visible_level_[OB_MAX_CONFIG_VISIBLE_LEVEL_LEN];
  char need_reboot_[OB_MAX_CONFIG_NEED_REBOOT_LEN];
  //DISALLOW_COPY_AND_ASSIGN(ObSystemConfigValue);
};

inline ObSystemConfigValue::ObSystemConfigValue()
{
  MEMSET(value_, 0, OB_MAX_CONFIG_VALUE_LEN);
  MEMSET(info_, 0, OB_MAX_CONFIG_INFO_LEN);
  MEMSET(section_, 0, OB_MAX_CONFIG_SECTION_LEN);
  MEMSET(visible_level_, 0, OB_MAX_CONFIG_VISIBLE_LEVEL_LEN);
  MEMSET(need_reboot_, 0, OB_MAX_CONFIG_NEED_REBOOT_LEN);
}

inline void ObSystemConfigValue::set_value(const ObString &value)
{
  int64_t value_length = value.length();
  if (value_length >= OB_MAX_CONFIG_VALUE_LEN) {
    value_length = OB_MAX_CONFIG_VALUE_LEN;
  }
  snprintf(value_, OB_MAX_CONFIG_VALUE_LEN, "%.*s",
           static_cast<int>(value_length), value.ptr());
}

inline void ObSystemConfigValue::set_value(const char *value)
{
  set_value(ObString::make_string(value));
}

inline void ObSystemConfigValue::set_info(const ObString &info)
{
  int64_t info_length = info.length();
  if (info_length >= OB_MAX_CONFIG_INFO_LEN) {
    info_length = OB_MAX_CONFIG_INFO_LEN;
  }
  IGNORE_RETURN snprintf(info_, OB_MAX_CONFIG_INFO_LEN, "%.*s",
                         static_cast<int>(info_length), info.ptr());
}

inline void ObSystemConfigValue::set_info(const char *info)
{
  set_info(ObString::make_string(info));
}

inline void ObSystemConfigValue::set_section(const ObString &section)
{
  int64_t section_length = section.length();
  if (section_length >= OB_MAX_CONFIG_SECTION_LEN) {
    section_length = OB_MAX_CONFIG_SECTION_LEN - 1;
  }
  int64_t pos = 0;
  (void) databuff_printf(section_, OB_MAX_CONFIG_SECTION_LEN, pos, "%.*s",
                         static_cast<int>(section_length), section.ptr());
}

inline void ObSystemConfigValue::set_section(const char *section)
{
  set_section(ObString::make_string(section));
}

inline void ObSystemConfigValue::set_visible_level(const ObString &visible_level)
{
  int64_t visible_level_length = visible_level.length();
  if (visible_level_length >= OB_MAX_CONFIG_VISIBLE_LEVEL_LEN) {
    visible_level_length = OB_MAX_CONFIG_VISIBLE_LEVEL_LEN - 1;
  }
  snprintf(visible_level_, OB_MAX_CONFIG_VISIBLE_LEVEL_LEN, "%.*s",
           static_cast<int>(visible_level_length), visible_level.ptr());
}

inline void ObSystemConfigValue::set_visible_level(const char *visible_level)
{
  set_visible_level(ObString::make_string(visible_level));
}

inline void ObSystemConfigValue::set_need_reboot(const bool &need_reboot)
{
  if (need_reboot) {
    snprintf(need_reboot_, OB_MAX_CONFIG_NEED_REBOOT_LEN, "%.*s",
             static_cast<int>(4), "TRUE");
  } else {
    snprintf(need_reboot_, OB_MAX_CONFIG_NEED_REBOOT_LEN, "%.*s",
             static_cast<int>(5), "FALSE");
  }
}
} // end of namespace common
} // end of namespace oceanbase

#endif // OCEANBASE_SHARE_CONFIG_OB_SYSTEM_CONFIG_VALUE_H_
