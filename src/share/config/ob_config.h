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

#ifndef OCEANBASE_SHARE_CONFIG_OB_CONFIG_H_
#define OCEANBASE_SHARE_CONFIG_OB_CONFIG_H_

#include <pthread.h>
#include "lib/tbsys.h"
#include "share/config/ob_config_helper.h"

namespace oceanbase
{
namespace common
{

struct ObCfgItemExtraInfo
{
  enum InfoType {
    NONE,
    SECTION,
    VISIBLE_LEVEL,
    NEED_REBOOT,
  };
  ObCfgItemExtraInfo() : type_(NONE), value_(NULL) {}
  ObCfgItemExtraInfo(InfoType type, const char *value) : type_(type), value_(value) {}

  InfoType type_;
  const char *value_;
};

struct ObCfgSectionLabel : public ObCfgItemExtraInfo
{
  explicit ObCfgSectionLabel(const char *section) : ObCfgItemExtraInfo(SECTION, section) {}
};

struct ObCfgVisibleLevelLabel : public ObCfgItemExtraInfo
{
  explicit ObCfgVisibleLevelLabel(const char *level) : ObCfgItemExtraInfo(VISIBLE_LEVEL, level) {}
};

struct ObCfgNeedRebootLabel: public ObCfgItemExtraInfo
{
  explicit ObCfgNeedRebootLabel(const char *need_reboot) : ObCfgItemExtraInfo(NEED_REBOOT, need_reboot) {}
};

#define CFG_EXTRA_INFO_DECLARE const ObCfgItemExtraInfo e1 = ObCfgItemExtraInfo(), \
                               const ObCfgItemExtraInfo e2 = ObCfgItemExtraInfo(), \
                               const ObCfgItemExtraInfo e3 = ObCfgItemExtraInfo()

#define CFG_EXTRA_INFO_LIST e1, e2, e3

class ObConfigItem
{
public:
  ObConfigItem();
  virtual ~ObConfigItem();
  ObConfigItem(const ObConfigItem& item);
  ObConfigItem& operator =(const ObConfigItem& item);

  void init(const char *name, const char *def, const char *info, CFG_EXTRA_INFO_DECLARE);
  void init(const char *name, const char *def, const char *range, const char *info, CFG_EXTRA_INFO_DECLARE);
  virtual bool parse_range(const char *range) { UNUSED(range); return true; }
  virtual ObConfigItem *clone() { return NULL; }
  int64_t to_string(char *buf, const int64_t buf_len) const;

  void add_checker(const ObConfigChecker *new_ck)
  {
    ck_ = new(std::nothrow) ObConfigConsChecker(ck_, new_ck);
  }
  bool check() const
  {
    return NULL == ck_ ? true : ck_->check(*this);
  }
  bool set_value(const common::ObString &string)
  {
    bool bool_ret = true;
    int64_t pos = 0;
    int ret = OB_SUCCESS;
    if (OB_FAIL(databuff_printf(value_str_, sizeof(value_str_), pos,
                                "%.*s", string.length(), string.ptr()))) {
      bool_ret = false;
    } else {
      bool_ret = set(value_str_);
    }
    return bool_ret;
  }
  bool set_value(const char *str)
  {
    bool bool_ret = true;
    int64_t pos = 0;
    int ret = OB_SUCCESS;
    if (OB_FAIL(databuff_printf(value_str_, sizeof(value_str_), pos, "%s", str))) {
      bool_ret = false;
    } else {
      bool_ret = set(str);
    }
    return bool_ret;
  }
  void set_name(const char *name)
  {
    int64_t pos = 0;
    (void) databuff_printf(name_str_, sizeof(name_str_), pos, "%s", name);
  }
  void set_info(const char *info)
  {
    int64_t pos = 0;
    (void) databuff_printf(info_str_, sizeof(info_str_), pos, "%s", info);
  }
  void set_section(const char *section)
  {
    int64_t pos = 0;
    (void) databuff_printf(section_str_, sizeof(section_str_), pos, "%s", section);
  }
  void set_visible_level(const char *visible_level)
  {
    int64_t pos = 0;
    (void) databuff_printf(visible_level_str_, sizeof(visible_level_str_),
                           pos, "%s", visible_level);
  }
  void set_need_reboot(const char *need_reboot)
  {
    if (NULL != need_reboot && 0 == STRCMP(OB_CONFIG_NEED_REBOOT, need_reboot)) {
      need_reboot_ = true;
    } else {
      need_reboot_ = false;
    }
  }
  void set_version(int64_t version) { version_ = version; }
  void set_range_str(const char *range_str)
  {
    int64_t pos = 0;
    if (NULL != range_str && range_str[0] != '\0') {
      (void) databuff_printf(range_str_, sizeof(range_str_),
                             pos, "%s", range_str);
    }
  }

  const char *str() const { return value_str_; }
  const char *name() const { return name_str_; }
  const char *info() const { return info_str_; }
  const char *section() const { return section_str_; }
  const char *visible_level() const { return visible_level_str_; }
  const char *range_str() const { return range_str_; }
  bool need_reboot() const { return need_reboot_; }
  bool is_initial_value_set() const { return is_initial_value_set_; }
  int64_t version() const { return version_; }

  virtual bool operator >(const char *) const { return false; }
  virtual bool operator >=(const char *) const { return false; }
  virtual bool operator <(const char *) const { return false; }
  virtual bool operator <=(const char *) const { return false; }
  virtual void set_initial_value() {}

protected:
  //use current value to do input operation
  virtual bool set(const char *str)
  {
    UNUSED(str);
    return true;
  }

  const ObConfigChecker *ck_;
  int64_t version_;
  bool need_reboot_;
  bool is_initial_value_set_;//need reboot value need set it once startup, otherwise it will output current value
  char value_str_[OB_MAX_CONFIG_VALUE_LEN];
  char name_str_[OB_MAX_CONFIG_NAME_LEN];
  char info_str_[OB_MAX_CONFIG_INFO_LEN];
  char section_str_[OB_MAX_CONFIG_SECTION_LEN];
  char visible_level_str_[OB_MAX_CONFIG_VISIBLE_LEVEL_LEN];
  char range_str_[OB_RANGE_STR_BUFSIZ];
};

class ObConfigIntListItem
  : public ObConfigItem
{
public:
  ObConfigIntListItem() : value_(), initial_value_() {}
  ObConfigIntListItem(ObConfigContainer *container,
                      const char *name,
                      const char *def,
                      const char *info,
                      CFG_EXTRA_INFO_DECLARE);
  virtual ~ObConfigIntListItem() {}
  virtual ObConfigItem *clone();

  //need reboot value need set it once startup, otherwise it will output current value
  const int64_t &operator[](int idx) const { return ((need_reboot_ && is_initial_value_set_) ? initial_value_.int_list_[idx] : value_.int_list_[idx]); }
  int64_t &operator[](int idx) { return ((need_reboot_ && is_initial_value_set_) ? initial_value_.int_list_[idx] : value_.int_list_[idx]); }
  ObConfigIntListItem &operator=(const char *str)
  {
    if (!set_value(str)) {
      OB_LOG(WARN, "obconfig int list item set value failed");
    }
    return *this;
  }
  int size() const { return ((need_reboot_ && is_initial_value_set_) ? initial_value_.size_ : value_.size_); }
  bool valid() const { return ((need_reboot_ && is_initial_value_set_) ? initial_value_.valid_ : value_.valid_); }
  virtual void set_initial_value() { initial_value_ = value_; is_initial_value_set_ = true; }

protected:
  //use current value to do input operation
  bool set(const char *str);

  static const int64_t MAX_INDEX_SIZE = 64;
  struct ObInnerConfigIntListItem
  {
    ObInnerConfigIntListItem()
      : size_(0), valid_(false)
    {
      MEMSET(int_list_, 0, sizeof(int_list_));
    }
    ~ObInnerConfigIntListItem() {}

    int64_t int_list_[MAX_INDEX_SIZE];
    int size_;
    bool valid_;
  };

  struct ObInnerConfigIntListItem value_;
  struct ObInnerConfigIntListItem initial_value_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigIntListItem);
};

class ObConfigStrListItem
  : public ObConfigItem
{
public:
  ObConfigStrListItem();
  ObConfigStrListItem(ObConfigContainer *container,
                      const char *name,
                      const char *def,
                      const char *info,
                      CFG_EXTRA_INFO_DECLARE);
  virtual ~ObConfigStrListItem() {}
  virtual ObConfigItem *clone();

  int tryget(const int64_t idx, char *buf, const int64_t buf_len) const;
  int get(const int64_t idx, char *buf, const int64_t buf_len) const;

  ObConfigStrListItem &operator=(const char *str)
  {
    if (!set_value(str)) {
      OB_LOG(WARN, "obconfig str list item set value failed");
    }
    return *this;
  }

  //need reboot value need set it once startup, otherwise it will output current value
  int64_t size() const { return ((need_reboot_ && is_initial_value_set_) ? initial_value_.size_ : value_.size_); }
  bool valid() const { return value_.valid_; }
  virtual void set_initial_value() { initial_value_ = value_; is_initial_value_set_ = true;}

public:
  static const int64_t MAX_INDEX_SIZE = 64;
  struct ObInnerConfigStrListItem
  {
    ObInnerConfigStrListItem()
      : valid_(false), size_(0), rwlock_()
    {
      MEMSET(idx_list_, 0, sizeof(idx_list_));
      MEMSET(value_str_bk_, 0, sizeof(value_str_bk_));
    }
    ~ObInnerConfigStrListItem() {}

    ObInnerConfigStrListItem &operator = (const ObInnerConfigStrListItem &value)
    {
      if (this == &value) {
        //do nothing
      } else {
        ObLatchRGuard rd_guard(const_cast<ObLatch&>(value.rwlock_), ObLatchIds::CONFIG_LOCK);
        ObLatchWGuard wr_guard(rwlock_, ObLatchIds::CONFIG_LOCK);

        valid_ = value.valid_;
        size_ = value.size_;
        MEMCPY(idx_list_, value.idx_list_, sizeof(idx_list_));
        MEMCPY(value_str_bk_, value.value_str_bk_, sizeof(value_str_bk_));
      }
      return *this;
    }

    ObInnerConfigStrListItem(const ObInnerConfigStrListItem &value)
    {
      if (this == &value) {
        //do nothing
      } else {
        ObLatchRGuard rd_guard(const_cast<ObLatch&>(value.rwlock_), ObLatchIds::CONFIG_LOCK);
        ObLatchWGuard wr_guard(rwlock_, ObLatchIds::CONFIG_LOCK);

        valid_ = value.valid_;
        size_ = value.size_;
        MEMCPY(idx_list_, value.idx_list_, sizeof(idx_list_));
        MEMCPY(value_str_bk_, value.value_str_bk_, sizeof(value_str_bk_));
      }
    }

    bool valid_;
    int64_t size_;
    int64_t idx_list_[MAX_INDEX_SIZE];
    char value_str_bk_[OB_MAX_CONFIG_VALUE_LEN];
    ObLatch rwlock_;
  };

  struct ObInnerConfigStrListItem value_;
  struct ObInnerConfigStrListItem initial_value_;

protected:
  //use current value to do input operation
  bool set(const char *str);

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigStrListItem);
};

class ObConfigIntegralItem
  : public ObConfigItem
{
public:
  ObConfigIntegralItem() : value_(0), initial_value_(0) {}
  virtual ~ObConfigIntegralItem() {}

  virtual ObConfigItem *clone() { return NULL; }
  bool operator >(const char *str) const
  { bool valid = true; return get_value() > parse(str, valid) && valid; }
  bool operator >=(const char *str) const
  { bool valid = true; return get_value() >= parse(str, valid) && valid; }
  bool operator <(const char *str) const
  { bool valid = true; return get_value() < parse(str, valid) && valid; }
  bool operator <=(const char *str) const
  { bool valid = true; return get_value() <= parse(str, valid) && valid; }

  // get_value() return the real-time value
  int64_t get_value() const { return value_; }
  // get() return the real-time value if it does not need reboot, otherwise it return initial_value
  int64_t get() const { return ((need_reboot_ && is_initial_value_set_) ? initial_value_ : value_); }
  operator const int64_t &() const { return ((need_reboot_ && is_initial_value_set_) ? initial_value_ : value_); }

  virtual bool parse_range(const char *range);
  virtual void set_initial_value() { initial_value_ = value_; is_initial_value_set_ = true; }

public:
  //use current value to do input operation
  bool set(const char *str);
  virtual int64_t parse(const char *str, bool &valid) const = 0;

private:
  int64_t value_;
  int64_t initial_value_;
  DISALLOW_COPY_AND_ASSIGN(ObConfigIntegralItem);
};
inline bool ObConfigIntegralItem::set(const char *str)
{
  bool valid = true;
  const int64_t value = parse(str, valid);
  if (valid) {
    value_ = value;
  }
  return valid;
}

class ObConfigDoubleItem
  : public ObConfigItem
{
public:
  ObConfigDoubleItem() : value_(0.0), initial_value_(0.0) {}
  ObConfigDoubleItem(ObConfigContainer *container,
                     const char *name,
                     const char *def,
                     const char *range,
                     const char *info,
                     CFG_EXTRA_INFO_DECLARE);
  ObConfigDoubleItem(ObConfigContainer *container,
                     const char *name,
                     const char *def,
                     const char *info,
                     CFG_EXTRA_INFO_DECLARE);
  virtual ~ObConfigDoubleItem() {}
  virtual ObConfigItem *clone();

  bool operator >(const char *str) const
  { bool valid = true; return get_value() > parse(str, valid) && valid; }
  bool operator >=(const char *str) const
  { bool valid = true; return get_value() >= parse(str, valid) && valid; }
  bool operator <(const char *str) const
  { bool valid = true; return get_value() < parse(str, valid) && valid; }
  bool operator <=(const char *str) const
  { bool valid = true; return get_value() <= parse(str, valid) && valid; }

  double get_value() const { return value_; }

  //need reboot value need set it once startup, otherwise it will output current value
  double get() const { return ((need_reboot_ && is_initial_value_set_) ? initial_value_ : value_); }
  operator const double &() const { return ((need_reboot_ && is_initial_value_set_) ? initial_value_ : value_); }

  ObConfigDoubleItem &operator = (double value);
  virtual bool parse_range(const char *range);
  virtual void set_initial_value() { initial_value_ = value_; is_initial_value_set_ = true; }

protected:
  //use current value to do input operation
  bool set(const char *str);
  double parse(const char *str, bool &valid) const;

private:
  double value_;
  double initial_value_;
  DISALLOW_COPY_AND_ASSIGN(ObConfigDoubleItem);
};
inline ObConfigDoubleItem &ObConfigDoubleItem::operator = (double value)
{
  char buf[OB_MAX_CONFIG_VALUE_LEN];
  (void) snprintf(buf, sizeof(buf), "%f", value);
  if (!set_value(buf)) {
    OB_LOG(WARN, "obconfig double item set value failed");
  }
  return *this;
}
inline bool ObConfigDoubleItem::set(const char *str)
{
  bool valid = true;
  const double value = parse(str, valid);
  if (valid) {
    value_ = value;
  }
  return valid;
}


class ObConfigCapacityItem
  : public ObConfigIntegralItem
{
public:
  ObConfigCapacityItem() {}
  ObConfigCapacityItem(ObConfigContainer *container,
                       const char *name,
                       const char *def,
                       const char *range,
                       const char *info,
                       CFG_EXTRA_INFO_DECLARE);
  ObConfigCapacityItem(ObConfigContainer *container,
                       const char *name,
                       const char *def,
                       const char *info,
                       CFG_EXTRA_INFO_DECLARE);
  virtual ~ObConfigCapacityItem() {}
  virtual ObConfigItem *clone();

  ObConfigCapacityItem &operator = (int64_t value);

protected:
  int64_t parse(const char *str, bool &valid) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigCapacityItem);
};
inline ObConfigCapacityItem &ObConfigCapacityItem::operator = (int64_t value)
{
  char buf[OB_MAX_CONFIG_VALUE_LEN];
  (void) snprintf(buf, sizeof(buf), "%ldB", value);
  if (!set_value(buf)) {
    OB_LOG(WARN, "obconfig capacity item set value failed");
  }
  return *this;
}

class ObConfigTimeItem
  : public ObConfigIntegralItem
{
public:
  ObConfigTimeItem() {}
  ObConfigTimeItem(ObConfigContainer *container,
                   const char *name,
                   const char *def,
                   const char *range,
                   const char *info,
                   CFG_EXTRA_INFO_DECLARE);
  ObConfigTimeItem(ObConfigContainer *container,
                   const char *name,
                   const char *def,
                   const char *info,
                   CFG_EXTRA_INFO_DECLARE);
  virtual ~ObConfigTimeItem() {}
  virtual ObConfigItem *clone();
  ObConfigTimeItem &operator = (int64_t value);

protected:
  int64_t parse(const char *str, bool &valid) const;

private:
  const static int64_t TIME_MICROSECOND = 1UL;
  const static int64_t TIME_MILLISECOND = 1000UL;
  const static int64_t TIME_SECOND = 1000 * 1000UL;
  const static int64_t TIME_MINUTE = 60 * 1000 * 1000UL;
  const static int64_t TIME_HOUR = 60 * 60 * 1000 * 1000UL;
  const static int64_t TIME_DAY = 24 * 60 * 60 * 1000 * 1000UL;
  DISALLOW_COPY_AND_ASSIGN(ObConfigTimeItem);
};
inline ObConfigTimeItem &ObConfigTimeItem::operator = (int64_t value){
  char buf[OB_MAX_CONFIG_VALUE_LEN];
  (void) snprintf(buf, sizeof(buf), "%ldus", value);
  if (!set_value(buf)) {
    OB_LOG(WARN, "obconfig time item set value failed");
  }
  return *this;
}

class ObConfigIntItem
  : public ObConfigIntegralItem
{
public:
  ObConfigIntItem() {}
  ObConfigIntItem(ObConfigContainer *container,
                  const char *name,
                  const char *def,
                  const char *range,
                  const char *info,
                  CFG_EXTRA_INFO_DECLARE);
  ObConfigIntItem(ObConfigContainer *container,
                  const char *name,
                  const char *def,
                  const char *info,
                  CFG_EXTRA_INFO_DECLARE);
  virtual ~ObConfigIntItem() {}
  virtual ObConfigItem *clone();
  ObConfigIntItem &operator = (int64_t value);

protected:
  int64_t parse(const char *str, bool &valid) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigIntItem);
};
inline ObConfigIntItem &ObConfigIntItem::operator = (int64_t value)
{
  char buf[OB_MAX_CONFIG_VALUE_LEN];
  (void) snprintf(buf, sizeof(buf), "%ld", value);
  if (!set_value(buf)) {
    OB_LOG(WARN, "obconfig int item set value failed");
  }
  return *this;
}

class ObConfigMomentItem
  : public ObConfigItem
{
public:
  ObConfigMomentItem() : value_(), initial_value_() {}
  ObConfigMomentItem(ObConfigContainer *container,
                     const char *name,
                     const char *def,
                     const char *info,
                     CFG_EXTRA_INFO_DECLARE);
  virtual ~ObConfigMomentItem() {}
  virtual ObConfigItem *clone();
  //use current value to do input operation
  bool set(const char *str);

  //need reboot value need set it once startup, otherwise it will output current value
  bool disable() const { return ((need_reboot_ && is_initial_value_set_) ? initial_value_.disable_ : value_.disable_); }
  int hour() const { return ((need_reboot_ && is_initial_value_set_) ? initial_value_.hour_ : value_.hour_); }
  int minute() const { return ((need_reboot_ && is_initial_value_set_) ? initial_value_.minute_ : value_.minute_); }
  virtual void set_initial_value() { initial_value_ = value_; is_initial_value_set_ = true; }

public:
  static const int64_t MAX_INDEX_SIZE = 64;
  struct ObInnerConfigMomentItem
  {
    ObInnerConfigMomentItem() : disable_(true), hour_(-1), minute_(-1) {}
    ~ObInnerConfigMomentItem() {}

    bool disable_;
    int hour_;
    int minute_;
  };

private:
  struct ObInnerConfigMomentItem value_;
  struct ObInnerConfigMomentItem initial_value_;
  DISALLOW_COPY_AND_ASSIGN(ObConfigMomentItem);
};

class ObConfigBoolItem
  : public ObConfigItem
{
public:
  ObConfigBoolItem() : value_(false), initial_value_(false) {}
  ObConfigBoolItem(ObConfigContainer *container,
                   const char *name,
                   const char *def,
                   const char *info,
                   CFG_EXTRA_INFO_DECLARE);
  virtual ~ObConfigBoolItem() {}
  virtual ObConfigItem *clone();

  //need reboot value need set it once startup, otherwise it will output current value
  operator const bool &() const { return ((need_reboot_ && is_initial_value_set_) ? initial_value_ : value_); }
  ObConfigBoolItem &operator = (const bool value) { set_value(value ? "True" : "False"); return *this; }
  virtual void set_initial_value() { initial_value_ = value_; is_initial_value_set_ = true; }
  const bool get_value() { return value_; }
  bool set(const char *str);

protected:
  //use current value to do input operation
  bool parse(const char *str, bool &valid) const;

private:
  bool value_;
  bool initial_value_;
  DISALLOW_COPY_AND_ASSIGN(ObConfigBoolItem);
};

class ObConfigStringItem
  : public ObConfigItem
{
public:
  ObConfigStringItem()
  {
    MEMSET(initial_value_str_, 0, sizeof(initial_value_str_));
  }
  ObConfigStringItem(ObConfigContainer *container,
                     const char *name,
                     const char *def,
                     const char *info,
                     CFG_EXTRA_INFO_DECLARE);
  virtual ~ObConfigStringItem() {}
  virtual ObConfigItem *clone();

  //need reboot value need set it once startup, otherwise it will output current value
  operator const char *() const { return ((need_reboot_ && is_initial_value_set_) ? initial_value_str_ : value_str_); } // not safe, value maybe changed
  int copy(char *buf, const int64_t buf_len); // '\0' will be added
  const char *initial_value() const { return initial_value_str_; }
  virtual void set_initial_value()
  {
    int32_t length = snprintf(initial_value_str_, sizeof(initial_value_str_), "%s", value_str_);
    if (length < 0 || length > static_cast<int32_t>(sizeof(initial_value_str_))) {
      OB_LOG(WARN, "buffer not enough", K(length), "buff_size", sizeof(initial_value_str_),
             K_(value_str));
    } else {
      is_initial_value_set_ = true;
    }
  }

protected:
  //use current value to do input operation
  bool set(const char *str) { UNUSED(str); return true; }

private:
  char initial_value_str_[OB_MAX_CONFIG_VALUE_LEN];
  DISALLOW_COPY_AND_ASSIGN(ObConfigStringItem);
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CONFIG_OB_CONFIG_H_
