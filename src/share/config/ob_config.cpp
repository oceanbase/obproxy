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

#include "share/config/ob_config.h"
#include <algorithm>
#include <cstring>
#include <ctype.h>
#include "lib/tbsys.h"

namespace oceanbase
{
namespace common
{
// ObConfigItem
ObConfigItem::ObConfigItem()
    : ck_(NULL), version_(0), need_reboot_(false), is_initial_value_set_(false)
{
  MEMSET(value_str_, 0, sizeof(value_str_));
  MEMSET(name_str_, 0, sizeof(name_str_));
  MEMSET(info_str_, 0, sizeof(info_str_));
  MEMSET(section_str_, 0, sizeof(section_str_));
  MEMSET(visible_level_str_, 0, sizeof(visible_level_str_));
  MEMSET(range_str_, 0, sizeof(range_str_));
}

ObConfigItem::~ObConfigItem()
{
  if (NULL != ck_) {
    delete ck_;
  }
}

int64_t ObConfigItem::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("value_str:", value_str_,
       "name_str:", name_str_,
       "visible_level_str:", visible_level_str_,
       "info:", info_str_);
  J_OBJ_END();
  return pos;
}

void ObConfigItem::init(const char *name,
                        const char *def,
                        const char *info,
                        const ObCfgItemExtraInfo e1,
                        const ObCfgItemExtraInfo e2,
                        const ObCfgItemExtraInfo e3)
{
  if (OB_ISNULL(name) || OB_ISNULL(def) || OB_ISNULL(info)) {
    OB_LOG(ERROR, "name or def or info is null", K(name), K(def), K(info));
  } else {
    set_name(name);
    if (!set_value(def)) {
      OB_LOG(ERROR, "Set config item value failed", K(name), K(def));
    }
    set_info(info);
    const ObCfgItemExtraInfo extra_infos[] = { CFG_EXTRA_INFO_LIST };
    for (int64_t i = 0; i < ARRAYSIZEOF(extra_infos); ++i) {
      switch (extra_infos[i].type_) {
      case ObCfgItemExtraInfo::NONE: {
          break;
        }
      case ObCfgItemExtraInfo::SECTION: {
          set_section(extra_infos[i].value_);
          break;
        }
      case ObCfgItemExtraInfo::VISIBLE_LEVEL: {
          set_visible_level(extra_infos[i].value_);
          break;
        }
      case ObCfgItemExtraInfo::NEED_REBOOT: {
          set_need_reboot(extra_infos[i].value_);
          break;
        }
      default: {
          OB_LOG(ERROR, "Unknown extra info type", "type", extra_infos[i].type_);
        }
      }
    }
  }
}

void ObConfigItem::init(const char *name,
                        const char *def,
                        const char *range,
                        const char *info,
                        const ObCfgItemExtraInfo e1,
                        const ObCfgItemExtraInfo e2,
                        const ObCfgItemExtraInfo e3)
{
  init(name, def, info, CFG_EXTRA_INFO_LIST);
  if (OB_ISNULL(range)) {
    OB_LOG(ERROR, "Range is NULL");
  } else if (!parse_range(range)) {
    OB_LOG(ERROR, "Parse check range fail", K(range));
  } else {
    set_range_str(range);
  }
}

// ObConfigIntListItem
ObConfigItem *ObConfigIntListItem::clone()
{
  ObConfigItem *ret = NULL;
  if (OB_ISNULL(ret = new (std::nothrow) ObConfigIntListItem())) {
    OB_LOG(WARN, "fail to new ObConfigIntListItem", K(name_str_));
  } else {
    ObCfgNeedRebootLabel e1 = need_reboot_ ? ObCfgNeedRebootLabel("true") : ObCfgNeedRebootLabel("false");
    ObCfgVisibleLevelLabel e2(visible_level_str_);
    ObCfgSectionLabel e3(section_str_);
    ret->init(name_str_, value_str_, range_str_, info_str_, CFG_EXTRA_INFO_LIST);
  }
  return ret;
}

ObConfigIntListItem::ObConfigIntListItem(ObConfigContainer *container,
                                         const char *name,
                                         const char *def,
                                         const char *info,
                                         const ObCfgItemExtraInfo e1,
                                         const ObCfgItemExtraInfo e2,
                                         const ObCfgItemExtraInfo e3)
    : value_(), initial_value_()
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(name, def, info, CFG_EXTRA_INFO_LIST);
}

bool ObConfigIntListItem::set(const char *str)
{
  UNUSED(str);
  char *saveptr = NULL;
  char *s = NULL;
  char *endptr = NULL;
  value_.valid_ = true;

  while (value_.size_--) {
    value_.int_list_[value_.size_] = 0;
  }
  value_.size_ = 0;

  char tmp_value_str[OB_MAX_CONFIG_VALUE_LEN];
  MEMCPY(tmp_value_str, value_str_, sizeof (tmp_value_str));
  s = STRTOK_R(tmp_value_str, ";", &saveptr);
  if (OB_LIKELY(NULL != s)) {
    do {
      int64_t v = strtol(s, &endptr, 10);
      if (endptr != s + STRLEN(s)) {
        value_.valid_ = false;
        _OB_LOG(ERROR, "not a valid config, [%s]", s);
      }
      value_.int_list_[value_.size_++] = v;
    } while (OB_LIKELY(NULL != (s = STRTOK_R(NULL, ";", &saveptr))) && value_.valid_);
  }
  return value_.valid_;
}

// ObConfigStrListItem
ObConfigItem *ObConfigStrListItem::clone()
{
  ObConfigItem *ret = NULL;
  if (OB_ISNULL(ret = new (std::nothrow) ObConfigStrListItem())) {
    OB_LOG(WARN, "fail to new ObConfigStrListItem", K(name_str_));
  } else {
    ObCfgNeedRebootLabel e1 = need_reboot_ ? ObCfgNeedRebootLabel("true") : ObCfgNeedRebootLabel("false");
    ObCfgVisibleLevelLabel e2(visible_level_str_);
    ObCfgSectionLabel e3(section_str_);
    ret->init(name_str_, value_str_, range_str_, info_str_, CFG_EXTRA_INFO_LIST);
  }
  return ret;
}

ObConfigStrListItem::ObConfigStrListItem()
    : value_(), initial_value_()
{
}

ObConfigStrListItem::ObConfigStrListItem(ObConfigContainer *container,
                                         const char *name,
                                         const char *def,
                                         const char *info,
                                         const ObCfgItemExtraInfo e1,
                                         const ObCfgItemExtraInfo e2,
                                         const ObCfgItemExtraInfo e3)
    : value_(), initial_value_()
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(name, def, info, CFG_EXTRA_INFO_LIST);
}

int ObConfigStrListItem::tryget(const int64_t idx, char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const struct ObInnerConfigStrListItem *inner_value = ((need_reboot_ && is_initial_value_set_) ? &initial_value_ : &value_);
  ObLatch &latch = const_cast<ObLatch&>(inner_value->rwlock_);
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(idx < 0)
      || OB_UNLIKELY(idx >= MAX_INDEX_SIZE)
      || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "input argument is invalid", K(buf), K(idx), K(buf_len), K(ret));
  } else if (!inner_value->valid_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "ValueStrList is not available, no need to get", K_(inner_value->valid), K(ret));
  } else if (OB_FAIL(latch.try_rdlock(ObLatchIds::CONFIG_LOCK))) {
    OB_LOG(WARN, "failed to tryrdlock rwlock_", K(ret));
  } else { //tryrdlock succ
    int print_size = 0;
    int32_t min_len = 0;
    const char *segment_str = NULL;
    if (idx >= inner_value->size_) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      segment_str = inner_value->value_str_bk_ + inner_value->idx_list_[idx];
      min_len = std::min(static_cast<int32_t>(STRLEN(segment_str) + 1),
                         static_cast<int32_t>(buf_len));
      print_size = snprintf(buf, static_cast<size_t>(buf_len), "%.*s", min_len, segment_str);
      if (print_size < 0 || print_size > min_len) {
        ret = OB_BUF_NOT_ENOUGH;
      }
    }
    latch.unlock();

    if (OB_FAIL(ret)) {
      OB_LOG(WARN, "failed to get value during lock",
             K(idx), K_(inner_value->size), K(buf_len), K(print_size), K(min_len), K(segment_str), K(ret));
    }
  }
  return ret;
}

int ObConfigStrListItem::get(const int64_t idx, char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const struct ObInnerConfigStrListItem *inner_value = ((need_reboot_ && is_initial_value_set_) ? &initial_value_ : &value_);
  if (OB_ISNULL(buf) || idx < 0 || idx >= MAX_INDEX_SIZE || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "input argument is invalid", K(buf), K(idx), K(buf_len), K(ret));
  } else if (!inner_value->valid_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "ValueStrList is not available, no need to get", K(inner_value->valid_), K(ret));
  } else {
    int print_size = 0;
    int32_t min_len = 0;
    const char *segment_str = NULL;
    ObLatch &latch = const_cast<ObLatch&>(inner_value->rwlock_);
    ObLatchRGuard rd_guard(latch, ObLatchIds::CONFIG_LOCK);
    if (idx >= inner_value->size_) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      segment_str = inner_value->value_str_bk_ + inner_value->idx_list_[idx];
      min_len = std::min(static_cast<int32_t>(STRLEN(segment_str) + 1),
                         static_cast<int32_t>(buf_len));
      print_size = snprintf(buf, static_cast<size_t>(min_len), "%.*s", min_len, segment_str);
      if (print_size < 0 || print_size > min_len) {
        ret = OB_BUF_NOT_ENOUGH;
      }
    }

    if (OB_FAIL(ret)) {
      OB_LOG(WARN, "failed to get value during lock",
             K(idx), K(inner_value->size_), K(buf_len), K(print_size),  K(min_len), K(segment_str), K(ret));
    }
  }
  return ret;
}

bool ObConfigStrListItem::set(const char *str)
{
  bool bret = true;
  UNUSED(str);
  int64_t length = static_cast<int64_t>(STRLEN(value_str_));
  if (0 != length) {
    int64_t idx_list[MAX_INDEX_SIZE];
    int64_t curr_idx = 0;
    idx_list[curr_idx++] = 0;
    for (int64_t i = 0; bret && i < length; ++i) {
      if (';' == value_str_[i]) {
        if (curr_idx < MAX_INDEX_SIZE) {
          idx_list[curr_idx++] = i + 1; //record semicolon's site and set next idx site
        } else { //overflow
          bret = false;
        }
      } else {
        //do nothing
      }
    }

    if (bret) { // value_str_ is available, memcpy to value_str_bk_
      int print_size = 0;
      ObLatchRGuard wr_guard(value_.rwlock_, ObLatchIds::CONFIG_LOCK);
      value_.valid_ = true;
      value_.size_ = curr_idx;
      MEMCPY(value_.idx_list_, idx_list, static_cast<size_t>(curr_idx) * sizeof(int64_t));
      int32_t min_len = std::min(static_cast<int32_t>(sizeof(value_.value_str_bk_)),
                                 static_cast<int32_t>(length) + 1);
      print_size = snprintf(
          value_.value_str_bk_,static_cast<size_t>(min_len), "%.*s", min_len, value_str_);
      if (print_size < 0 || print_size > min_len) {
        value_.valid_ = false;
        bret = false;
      } else {
        for (int64_t i = 1; i < value_.size_; ++i) { // ';' --> '\0'
          value_.value_str_bk_[idx_list[i] - 1] = '\0';
        }
      }
    } else {
      OB_LOG(WARN, "input str is not available", K(str), K_(value_.valid), K_(value_.size), K(bret));
    }
  } else {
    ObLatchRGuard wr_guard(value_.rwlock_, ObLatchIds::CONFIG_LOCK);
    value_.size_ = 0;
    value_.valid_ = true;
  }
  return bret;
}

// ObConfigIntegralItem
bool ObConfigIntegralItem::parse_range(const char *range)
{
  char buff[64] = {'\0'};
  const char *p_left = NULL;
  char *p_middle = NULL;
  char *p_right = NULL;
  bool bool_ret = true;
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(range)) {
    OB_LOG(ERROR, "Range is NULL!");
    bool_ret = false;
  } else if ('\0' == range[0]) {
    // do nothing
  } else if (OB_FAIL(databuff_printf(buff, sizeof(buff), pos, "%s", range))) {
    bool_ret = false;
    OB_LOG(WARN, "buf is not long enough", K(sizeof(buff)), K(pos), K(ret));
  } else {
    const int64_t buff_length = static_cast<int64_t>(STRLEN(buff));
    for (int64_t i = 0; i < buff_length; ++i) {
      if ('(' == buff[i] || '[' == buff[i]) {
        p_left = buff + i;
      } else if (buff[i] == ',') {
        p_middle = buff + i;
      } else if (')' == buff[i] || ']' == buff[i]) {
        p_right = buff + i;
      }
    }
    if (!p_left || !p_middle || !p_right
        || p_left >= p_middle || p_middle >= p_right) {
      bool_ret = false;
      // not validated
    } else {
      bool valid = true;
      char ch_right = *p_right;
      *p_right = '\0';
      *p_middle = '\0';

      if ('\0' != p_left[1]) {
        parse(p_left + 1, valid);
        if (valid) {
          if (*p_left == '(') {
            add_checker(new(std::nothrow) ObConfigGreaterThan(p_left + 1));
          } else if (*p_left == '[') {
            add_checker(new(std::nothrow) ObConfigGreaterEqual(p_left + 1));
          }
        }
      }

      if ('\0' != p_middle[1]) {
        parse(p_middle + 1, valid);
        if (valid) {
          if (')' == ch_right) {
            add_checker(new(std::nothrow) ObConfigLessThan(p_middle + 1));
          } else if (']' == ch_right) {
            add_checker(new(std::nothrow) ObConfigLessEqual(p_middle + 1));
          }
        }
      }

      bool_ret = true;
    }
  }
  return bool_ret;
}

// ObConfigDoubleItem
ObConfigItem *ObConfigDoubleItem::clone()
{
  ObConfigItem *ret = NULL;
  if (OB_ISNULL(ret = new (std::nothrow) ObConfigDoubleItem())) {
    OB_LOG(WARN, "fail to new ObConfigDoubleItem", K(name_str_));
  } else {
    ObCfgNeedRebootLabel e1 = need_reboot_ ? ObCfgNeedRebootLabel("true") : ObCfgNeedRebootLabel("false");
    ObCfgVisibleLevelLabel e2(visible_level_str_);
    ObCfgSectionLabel e3(section_str_);
    ret->init(name_str_, value_str_, range_str_, info_str_, CFG_EXTRA_INFO_LIST);
  }
  return ret;
}

ObConfigDoubleItem::ObConfigDoubleItem(ObConfigContainer *container,
                                       const char *name,
                                       const char *def,
                                       const char *range,
                                       const char *info,
                                       const ObCfgItemExtraInfo e1,
                                       const ObCfgItemExtraInfo e2,
                                       const ObCfgItemExtraInfo e3)
    : value_(0), initial_value_(0)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(name, def, range,  info, CFG_EXTRA_INFO_LIST);
}

ObConfigDoubleItem::ObConfigDoubleItem(ObConfigContainer *container,
                                      const char *name,
                                      const char *def,
                                      const char *info,
                                      const ObCfgItemExtraInfo e1,
                                      const ObCfgItemExtraInfo e2,
                                      const ObCfgItemExtraInfo e3)
    : value_(0), initial_value_(0)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(name, def, "", info, CFG_EXTRA_INFO_LIST);
}

double ObConfigDoubleItem::parse(const char *str, bool &valid) const
{
  double v = 0.0;
  if (OB_ISNULL(str) || OB_UNLIKELY('\0' == str[0])) {
    valid = false;
  } else {
    char *endptr = NULL;
    v = strtod(str, &endptr);
    if (OB_ISNULL(endptr) || OB_UNLIKELY('\0' != *endptr)) {
      valid = false;
    } else {
      valid = true;
    }
  }
  return v;
}

bool ObConfigDoubleItem::parse_range(const char *range)
{
  char buff[64] = {'\0'};
  const char *p_left = NULL;
  char *p_middle = NULL;
  char *p_right = NULL;
  bool bool_ret = true;
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(range)) {
    OB_LOG(ERROR, "Range is NULL!");
    bool_ret = false;
  } else if ('\0' == range[0]) {
    // do nothing
  } else if (OB_FAIL(databuff_printf(buff, sizeof(buff), pos, "%s", range))) {
    bool_ret = false;
  } else {
    const int64_t buff_length = static_cast<int64_t>(STRLEN(buff));
    for (int64_t i = 0; i < buff_length; ++i) {
      if ('(' == buff[i] || '[' == buff[i]) {
        p_left = buff + i;
      } else if (',' == buff[i]) {
        p_middle = buff + i;
      } else if (')' == buff[i] || ']' == buff[i]) {
        p_right = buff + i;
      }
    }
    if (OB_ISNULL(p_left) || OB_ISNULL(p_middle) || OB_ISNULL(p_right)) {
      bool_ret = false; // not validated
    } else if (OB_UNLIKELY(p_left >= p_middle) || OB_UNLIKELY(p_middle >= p_right)) {
      bool_ret = false; // not validated
    } else {
      bool valid = true;
      char ch_right = *p_right;
      *p_right = '\0';
      *p_middle = '\0';

      parse(p_left + 1, valid);
      if (valid) {
        if (*p_left == '(') {
          add_checker(new(std::nothrow) ObConfigGreaterThan(p_left + 1));
        } else if (*p_left == '[') {
          add_checker(new(std::nothrow) ObConfigGreaterEqual(p_left + 1));
        }
      }

      parse(p_middle + 1, valid);
      if (valid) {
        if (')' == ch_right) {
          add_checker(new(std::nothrow) ObConfigLessThan(p_middle + 1));
        } else if (']' == ch_right) {
          add_checker(new(std::nothrow) ObConfigLessEqual(p_middle + 1));
        }
      }
      bool_ret = true;
    }
  }
  return bool_ret;
}

// ObConfigCapacityItem
ObConfigItem *ObConfigCapacityItem::clone()
{
  ObConfigItem *ret = NULL;
  if (OB_ISNULL(ret = new (std::nothrow) ObConfigCapacityItem())) {
    OB_LOG(WARN, "fail to new ObConfigCapacityItem", K(name_str_));
  } else {
    ObCfgNeedRebootLabel e1 = need_reboot_ ? ObCfgNeedRebootLabel("true") : ObCfgNeedRebootLabel("false");
    ObCfgVisibleLevelLabel e2(visible_level_str_);
    ObCfgSectionLabel e3(section_str_);
    ret->init(name_str_, value_str_, range_str_, info_str_, CFG_EXTRA_INFO_LIST);
  }
  return ret;
}

ObConfigCapacityItem::ObConfigCapacityItem(ObConfigContainer *container,
                                            const char *name,
                                            const char *def,
                                            const char *range,
                                            const char *info,
                                            const ObCfgItemExtraInfo e1,
                                            const ObCfgItemExtraInfo e2,
                                            const ObCfgItemExtraInfo e3)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(name, def, range, info, CFG_EXTRA_INFO_LIST);
}

ObConfigCapacityItem::ObConfigCapacityItem(ObConfigContainer *container,
                                           const char *name,
                                           const char *def,
                                           const char *info,
                                           const ObCfgItemExtraInfo e1,
                                           const ObCfgItemExtraInfo e2,
                                           const ObCfgItemExtraInfo e3)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(name, def, "", info, CFG_EXTRA_INFO_LIST);
}

int64_t ObConfigCapacityItem::parse(const char *str, bool &valid) const
{
  int64_t ret = ObConfigCapacityParser::get(str, valid);
  if (!valid) {
      OB_LOG(ERROR, "set capacity error", "name", name(), K(str), K(valid));
  }
  return ret;
}

// ObConfigTimeItem
ObConfigItem *ObConfigTimeItem::clone()
{
  ObConfigItem *ret = NULL;
  if (OB_ISNULL(ret = new (std::nothrow) ObConfigTimeItem())) {
    OB_LOG(WARN, "fail to new ObConfigTimeItem", K(name_str_));
  } else {
    ObCfgNeedRebootLabel e1 = need_reboot_ ? ObCfgNeedRebootLabel("true") : ObCfgNeedRebootLabel("false");
    ObCfgVisibleLevelLabel e2(visible_level_str_);
    ObCfgSectionLabel e3(section_str_);
    ret->init(name_str_, value_str_, range_str_, info_str_, CFG_EXTRA_INFO_LIST);
  }
  return ret;
}

ObConfigTimeItem::ObConfigTimeItem(ObConfigContainer *container,
                                   const char *name,
                                   const char *def,
                                   const char *range,
                                   const char *info,
                                   const ObCfgItemExtraInfo e1,
                                   const ObCfgItemExtraInfo e2,
                                   const ObCfgItemExtraInfo e3)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(name, def, range, info, CFG_EXTRA_INFO_LIST);
}

ObConfigTimeItem::ObConfigTimeItem(ObConfigContainer *container,
                                 const char *name,
                                 const char *def,
                                 const char *info,
                                 const ObCfgItemExtraInfo e1,
                                 const ObCfgItemExtraInfo e2,
                                 const ObCfgItemExtraInfo e3)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(name, def, "", info, CFG_EXTRA_INFO_LIST);
}

int64_t ObConfigTimeItem::parse(const char *str, bool &valid) const
{
  char *p_unit = NULL;
  int64_t value = 0;

  if (OB_ISNULL(str) || '\0' == str[0]) {
    valid = false;
  } else {
    valid = true;
    value = std::max(0L, strtol(str, &p_unit, 0));
    if (OB_ISNULL(p_unit)) {
      valid = false;
      OB_LOG(ERROR, "set time error, p_unit is null", "name", name(), K(str), K(valid));
    } else if (0 == STRCASECMP("us", p_unit)) {
      value *= TIME_MICROSECOND;
    } else if (0 == STRCASECMP("ms", p_unit)) {
      value *= TIME_MILLISECOND;
    } else if ('\0' == *p_unit || 0 == STRCASECMP("s", p_unit)) {
      /* default is second */
      value *= TIME_SECOND;
    } else if (0 == STRCASECMP("m", p_unit)) {
      value *= TIME_MINUTE;
    } else if (0 == STRCASECMP("h", p_unit)) {
      value *= TIME_HOUR;
    } else if (0 == STRCASECMP("d", p_unit)) {
      value *= TIME_DAY;
    } else {
      valid = false;
      OB_LOG(ERROR, "set time error", "name", name(), K(str), K(valid));
    }
  }
  return value;
}

// ObConfigIntItem
ObConfigItem *ObConfigIntItem::clone()
{
  ObConfigItem *ret = NULL;
  if (OB_ISNULL(ret = new (std::nothrow) ObConfigIntItem())) {
    OB_LOG(WARN, "fail to new ObConfigIntItem", K(name_str_));
  } else {
    ObCfgNeedRebootLabel e1 = need_reboot_ ? ObCfgNeedRebootLabel("true") : ObCfgNeedRebootLabel("false");
    ObCfgVisibleLevelLabel e2(visible_level_str_);
    ObCfgSectionLabel e3(section_str_);
    ret->init(name_str_, value_str_, range_str_, info_str_, CFG_EXTRA_INFO_LIST);
  }
  return ret;
}

ObConfigIntItem::ObConfigIntItem(ObConfigContainer *container,
                                 const char *name,
                                 const char *def,
                                 const char *range,
                                 const char *info,
                                 const ObCfgItemExtraInfo e1,
                                 const ObCfgItemExtraInfo e2,
                                 const ObCfgItemExtraInfo e3)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(name, def, range, info, CFG_EXTRA_INFO_LIST);
}

ObConfigIntItem::ObConfigIntItem(ObConfigContainer *container,
                                 const char *name,
                                 const char *def,
                                 const char *info,
                                 const ObCfgItemExtraInfo e1,
                                 const ObCfgItemExtraInfo e2,
                                 const ObCfgItemExtraInfo e3)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(name, def, "", info, CFG_EXTRA_INFO_LIST);
}

int64_t ObConfigIntItem::parse(const char *str, bool &valid) const
{
  char *p_end = NULL;
  int64_t value = 0;

  if (OB_ISNULL(str) || '\0' == str[0]) {
    valid = false;
  } else {
    valid = true;
    value = strtol(str, &p_end, 0);
    if ('\0' == *p_end) {
      valid = true;
    } else {
      valid = false;
      OB_LOG(ERROR, "set int error", "name", name(), K(str), K(valid));
    }
  }
  return value;
}

// ObConfigMomentItem
ObConfigItem *ObConfigMomentItem::clone()
{
  ObConfigItem *ret = NULL;
  if (OB_ISNULL(ret = new (std::nothrow) ObConfigMomentItem())) {
    OB_LOG(WARN, "fail to new ObConfigMomentItem", K(name_str_));
  } else {
    ObCfgNeedRebootLabel e1 = need_reboot_ ? ObCfgNeedRebootLabel("true") : ObCfgNeedRebootLabel("false");
    ObCfgVisibleLevelLabel e2(visible_level_str_);
    ObCfgSectionLabel e3(section_str_);
    ret->init(name_str_, value_str_, range_str_, info_str_, CFG_EXTRA_INFO_LIST);
  }
  return ret;
}

ObConfigMomentItem::ObConfigMomentItem(ObConfigContainer *container,
                                       const char *name,
                                       const char *def,
                                       const char *info,
                                       const ObCfgItemExtraInfo e1,
                                       const ObCfgItemExtraInfo e2,
                                       const ObCfgItemExtraInfo e3)
    :  value_(), initial_value_()
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(name, def, info, CFG_EXTRA_INFO_LIST);
}

bool ObConfigMomentItem::set(const char *str)
{
  int ret = true;
  struct tm tm_value;
  if (0 == STRCASECMP(str, "disable")) {
    value_.disable_ = true;
  } else if (OB_ISNULL(strptime(str, "%H:%M", &tm_value))) {
    value_.disable_ = true;
    ret = false;
    OB_LOG(ERROR, "Not well-formed moment item value", K(str));
  } else {
    value_.disable_ = false;
    value_.hour_ = tm_value.tm_hour;
    value_.minute_ = tm_value.tm_min;
  }
  return ret;
}

// ObConfigBoolItem
ObConfigItem *ObConfigBoolItem::clone()
{
  ObConfigItem *ret = NULL;
  if (OB_ISNULL(ret = new (std::nothrow) ObConfigBoolItem())) {
    OB_LOG(WARN, "fail to new ObConfigBoolItem", K(name_str_));
  } else {
    ObCfgNeedRebootLabel e1 = need_reboot_ ? ObCfgNeedRebootLabel("true") : ObCfgNeedRebootLabel("false");
    ObCfgVisibleLevelLabel e2(visible_level_str_);
    ObCfgSectionLabel e3(section_str_);
    ret->init(name_str_, value_str_, range_str_, info_str_, CFG_EXTRA_INFO_LIST);
  }
  return ret;
}

ObConfigBoolItem::ObConfigBoolItem(ObConfigContainer *container,
                                   const char *name,
                                   const char *def,
                                   const char *info,
                                   const ObCfgItemExtraInfo e1,
                                   const ObCfgItemExtraInfo e2,
                                   const ObCfgItemExtraInfo e3)
    : value_(false), initial_value_(false)
{
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(name, def, info, CFG_EXTRA_INFO_LIST);
}

bool ObConfigBoolItem::set(const char *str)
{
  bool valid = false;
  const bool value = parse(str, valid);
  if (valid) {
    int64_t pos = 0;
    (void) databuff_printf(value_str_, sizeof (value_str_), pos, value ? "True" : "False");
    value_ = value;
  }
  return valid;
}

bool ObConfigBoolItem::parse(const char *str, bool &valid) const
{
  bool value = true;
  if (OB_ISNULL(str)) {
    valid = false;
    OB_LOG(ERROR, "Get bool config item fail, str is NULL!");
  } else if (0 == STRCASECMP(str, "false")) {
    valid = true;
    value = false;
  } else if (0 == STRCASECMP(str, "true")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "off")) {
    valid = true;
    value = false;
  } else if (0 == STRCASECMP(str, "on")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "no")) {
    valid = true;
    value = false;
  } else if (0 == STRCASECMP(str, "yes")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "f")) {
    valid = true;
    value = false;
  } else if (0 == STRCASECMP(str, "t")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "1")) {
    valid = true;
    value = true;
  } else if (0 == STRCASECMP(str, "0")) {
    valid = true;
    value = false;
  } else {
    OB_LOG(ERROR, "Get bool config item fail", K(str));
    valid = false;
  }
  return value;
}

// ObConfigStringItem
ObConfigItem *ObConfigStringItem::clone()
{
  ObConfigItem *ret = NULL;
  if (OB_ISNULL(ret = new (std::nothrow) ObConfigStringItem())) {
    OB_LOG(WARN, "fail to new ObConfigStringItem", K(name_str_));
  } else {
    ObCfgNeedRebootLabel e1 = need_reboot_ ? ObCfgNeedRebootLabel("true") : ObCfgNeedRebootLabel("false");
    ObCfgVisibleLevelLabel e2(visible_level_str_);
    ObCfgSectionLabel e3(section_str_);
    ret->init(name_str_, value_str_, range_str_, info_str_, CFG_EXTRA_INFO_LIST);
  }
  return ret;
}

ObConfigStringItem::ObConfigStringItem(ObConfigContainer *container,
                                       const char *name,
                                       const char *def,
                                       const char *info,
                                       const ObCfgItemExtraInfo e1,
                                       const ObCfgItemExtraInfo e2,
                                       const ObCfgItemExtraInfo e3)
{
  MEMSET(initial_value_str_, 0, sizeof(initial_value_str_));
  if (OB_LIKELY(NULL != container)) {
    container->set_refactored(ObConfigStringKey(name), this, 1);
  }
  init(name, def, info, CFG_EXTRA_INFO_LIST);
}

int ObConfigStringItem::copy(char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const char *inner_value = ((need_reboot_ && is_initial_value_set_) ? initial_value_str_ : value_str_);
  int32_t min_len = static_cast<int32_t>(
      std::min(buf_len, static_cast<int64_t>(STRLEN(inner_value)) + 1));
  int32_t length = snprintf(buf, min_len, "%.*s", min_len, inner_value);
  if (length < 0 || length > min_len) {
    ret = OB_BUF_NOT_ENOUGH;
    OB_LOG(WARN, "buffer not enough", K(length), K(min_len), K_(value_str), K(ret));
  }
  return ret;
}

} // end of namespace common
} // end of namespace oceanbase
