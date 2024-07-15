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

#ifndef OCEANBASE_SHARE_CONFIG_OB_CONFIG_HELPER_H_
#define OCEANBASE_SHARE_CONFIG_OB_CONFIG_HELPER_H_

#include <arpa/inet.h>
#include "lib/hash/ob_hashmap.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/ob_define.h"

#define _DEF_CONFIG_EASY(cfg,name,args...)                           \
  class ObConfig##cfg##Item##_##name                                 \
      : public common::ObConfig##cfg##Item                           \
  {                                                                  \
  public:                                                            \
    ObConfig##cfg##Item##_##name()                                   \
        : common::ObConfig##cfg##Item(local_container(),#name,args)  \
    {}                                                               \
    template <class T>                                               \
    ObConfig##cfg##Item##_##name& operator=(T value)                 \
    {                                                                \
      common::ObConfig##cfg##Item::operator=(value);                 \
      return *this;                                                  \
    }                                                                \
  } name;

#define _DEF_CONFIG_RANGE_EASY(cfg,name,args...)                     \
  class ObConfig##cfg##Item##_##name                                 \
      : public common::ObConfig##cfg##Item                           \
  {                                                                  \
  public:                                                            \
    ObConfig##cfg##Item##_##name()                                   \
        : common::ObConfig##cfg##Item(local_container(),#name,args)  \
    {}                                                               \
    template <class T>                                               \
    ObConfig##cfg##Item##_##name& operator=(T value)                 \
    {                                                                \
      common::ObConfig##cfg##Item::operator=(value);                 \
      return *this;                                                  \
    }                                                                \
  } name;

#define _DEF_CONFIG_CHECKER_EASY(cfg,name,def,checker,args...)       \
  class ObConfig##cfg##Item##_##name                                 \
      : public common::ObConfig##cfg##Item                           \
  {                                                                  \
  public:                                                            \
    ObConfig##cfg##Item##_##name()                                   \
        : common::ObConfig##cfg##Item(                               \
            local_container(),#name,def,args)                        \
    { add_checker(new (std::nothrow) checker()); }                   \
    template <class T>                                               \
    ObConfig##cfg##Item##_##name& operator=(T value)                 \
    {                                                                \
      common::ObConfig##cfg##Item::operator=(value);                 \
      return *this;                                                  \
    }                                                                \
  } name;

#define _DEF_CONFIG_IP_EASY(cfg,name,def,args...)                    \
  _DEF_CONFIG_CHECKER_EASY(cfg,name,                                 \
                           def,common::ObConfigIpChecker,args)


#define DEF_INT(args...)                        \
  _DEF_CONFIG_RANGE_EASY(Int,args)
#define DEF_DBL(args...)                        \
  _DEF_CONFIG_RANGE_EASY(Double,args)
#define DEF_CAP(args...)                        \
  _DEF_CONFIG_RANGE_EASY(Capacity,args)
#define DEF_TIME(args...)                       \
  _DEF_CONFIG_RANGE_EASY(Time,args)
#define DEF_BOOL(args...)                       \
  _DEF_CONFIG_EASY(Bool,args)
#define DEF_STR(args...)                        \
  _DEF_CONFIG_EASY(String,args)
#define DEF_IP(args...)                         \
  _DEF_CONFIG_IP_EASY(String,args)
#define DEF_MOMENT(args...)                     \
  _DEF_CONFIG_EASY(Moment,args)
#define DEF_INT_LIST(args...)                   \
  _DEF_CONFIG_EASY(IntList,args)
#define DEF_STR_LIST(args...)                   \
  _DEF_CONFIG_EASY(StrList,args)
#define DEF_LOG_LEVEL(name, def, args...)       \
  _DEF_CONFIG_CHECKER_EASY(String, name,        \
                           def, common::ObConfigLogLevelChecker, args)

namespace oceanbase
{
namespace common
{
class ObConfigItem;
class ObConfigIntegralItem;
class ObConfigAlwaysTrue;

class ObConfigChecker
{
public:
  ObConfigChecker() {}
  virtual ~ObConfigChecker() {}
  virtual bool check(const ObConfigItem &t) const = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigChecker);
};

class ObConfigAlwaysTrue
  : public ObConfigChecker
{
public:
  ObConfigAlwaysTrue() {}
  virtual ~ObConfigAlwaysTrue() {}
  bool check(const ObConfigItem &t) const { UNUSED(t); return true; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigAlwaysTrue);
};

class ObConfigIpChecker
  : public ObConfigChecker
{
public:
  ObConfigIpChecker() {}
  virtual ~ObConfigIpChecker() {}
  bool check(const ObConfigItem &t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigIpChecker);
};

class ObConfigConsChecker
  : public ObConfigChecker
{
public:
  ObConfigConsChecker(const ObConfigChecker *left, const ObConfigChecker *right)
      : left_(left), right_(right)
  {}
  virtual ~ObConfigConsChecker();
  bool check(const ObConfigItem &t) const;

private:
  const ObConfigChecker *left_;
  const ObConfigChecker *right_;
  DISALLOW_COPY_AND_ASSIGN(ObConfigConsChecker);
};

class ObConfigBinaryChecker
  : public ObConfigChecker
{
public:
  explicit ObConfigBinaryChecker(const char *str);
  virtual ~ObConfigBinaryChecker() {}
  const char *value() const { return val_; }

protected:
  char val_[OB_MAX_CONFIG_VALUE_LEN];

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigBinaryChecker);
};
inline ObConfigBinaryChecker::ObConfigBinaryChecker(const char *str)
{
  int64_t pos = 0;
  (void) databuff_printf(val_, sizeof(val_), pos, "%s", str);
}

class ObConfigGreaterThan
  : public ObConfigBinaryChecker
{
public:
  explicit ObConfigGreaterThan(const char *str) : ObConfigBinaryChecker(str) {}
  virtual ~ObConfigGreaterThan() {}
  bool check(const ObConfigItem &t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigGreaterThan);
};

class ObConfigGreaterEqual
  : public ObConfigBinaryChecker
{
public:
  explicit ObConfigGreaterEqual(const char *str) : ObConfigBinaryChecker(str) {}
  virtual ~ObConfigGreaterEqual() {}
  bool check(const ObConfigItem &t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigGreaterEqual);
};

class ObConfigLessThan
  : public ObConfigBinaryChecker
{
public:
  explicit ObConfigLessThan(const char *str) : ObConfigBinaryChecker(str) {}
  virtual ~ObConfigLessThan() {}
  bool check(const ObConfigItem &t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigLessThan);
};

class ObConfigLessEqual
  : public ObConfigBinaryChecker
{
public:
  explicit ObConfigLessEqual(const char *str) : ObConfigBinaryChecker(str) {}
  virtual ~ObConfigLessEqual() {}
  bool check(const ObConfigItem &t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigLessEqual);
};

class ObConfigLogLevelChecker
  : public ObConfigChecker
{
public:
  ObConfigLogLevelChecker() {}
  virtual ~ObConfigLogLevelChecker() {};
  bool check(const ObConfigItem &t) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigLogLevelChecker);
};

// config item container
class ObConfigStringKey
{
public:
  ObConfigStringKey() { MEMSET(str_, 0, sizeof(str_)); }
  explicit ObConfigStringKey(const char *str);
  explicit ObConfigStringKey(const ObString &string);
  virtual ~ObConfigStringKey() {}
  uint64_t hash() const;

  // case unsensitive
  bool operator == (const ObConfigStringKey &str) const
  {
    return 0 == STRCASECMP(str.str_, this->str_);
  }

  const char *str() const { return str_; }

private:
  char str_[OB_MAX_CONFIG_NAME_LEN];
  //DISALLOW_COPY_AND_ASSIGN(ObConfigStringKey);
};
inline ObConfigStringKey::ObConfigStringKey(const char *str)
{
  int64_t pos = 0;
  (void) databuff_printf(str_, sizeof(str_), pos, "%s", str);
}

inline ObConfigStringKey::ObConfigStringKey(const ObString &string)
{
  int64_t pos = 0;
  (void) databuff_printf(str_, sizeof(str_), pos, "%.*s", string.length(), string.ptr());
}
inline uint64_t ObConfigStringKey::hash() const
{
  // if str_ is null, hash value will be 0
  uint64_t h = 0;
  if (OB_LIKELY(NULL != str_)) {
    murmurhash(str_, (int32_t)STRLEN(str_), 0);
  }
  return h;
}

template <class Key, class Value, int num>
class __ObConfigContainer
  : public hash::ObHashMap<Key, Value *, hash::NoPthreadDefendMode>
{
public:
  __ObConfigContainer()
  {
    this->create(num,
                 oceanbase::common::ObModIds::OB_HASH_BUCKET_CONF_CONTAINER,
                 oceanbase::common::ObModIds::OB_HASH_NODE_CONF_CONTAINER);
  }
 virtual ~__ObConfigContainer() {}

private:
  DISALLOW_COPY_AND_ASSIGN(__ObConfigContainer);
};

class ObConfigCapacityParser
{
public:
  ObConfigCapacityParser() {}
  virtual ~ObConfigCapacityParser() {}
  static int64_t get(const char *str, bool &valid);

private:
  enum CAP_UNIT
  {
    // shift bits between unit of byte and that
    CAP_B = 0,
    CAP_KB = 10,
    CAP_MB = 20,
    CAP_GB = 30,
    CAP_TB = 40,
    CAP_PB = 50,
  };
  DISALLOW_COPY_AND_ASSIGN(ObConfigCapacityParser);
};

typedef __ObConfigContainer<ObConfigStringKey,
                            ObConfigItem, OB_MAX_CONFIG_NUMBER> ObConfigContainer;

// ObConfigVariableString
  // 1. 适合短字符（15个字符以内）使用，短字符下，内存使用更低
  // 2. 短字符下，ObConfigVariableString内存占用24bytes，ObVariableLenBuffer<16>内存占用48bytes
  // ObConfigVariableString相对ObVariableLenBuffer，内存优化50%，更适合配置使用场景
class ObConfigVariableString
{
public: 
  ObConfigVariableString(): used_len_(0), data_union_() {}
  ~ObConfigVariableString() { reset(); }
  ObConfigVariableString(const ObConfigVariableString& other);
  ObConfigVariableString& operator=(const ObConfigVariableString& other);
  void reset();
  bool is_empty() const { return used_len_ == 0; }
  uint64_t hash(uint64_t seed) const;
  bool operator==(const ObConfigVariableString& other) const;
  int64_t size() const { return used_len_; }
  int rewrite(const char *ptr, const int64_t len);
  int rewrite(const ObString &str);
  int rewrite(const char *ptr);
  const char *ptr() const
  {
    return used_len_ > VARIABLE_BUF_LEN ? data_union_.ptr_ : data_union_.buf_;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  operator const ObString() const;
private:
  // SSO优化，堆上指针ptr_和buf_共用内存
  int alloc_mem(int64_t len);
  static constexpr int64_t VARIABLE_BUF_LEN = 15;
  int64_t used_len_;  // 已写入的内存，注意：如果在堆上，分配内存为used_len_+1
  union DataUnion
  {
    DataUnion(): buf_() {}
    char* ptr_;    // 指向op_fixed_mem_alloc堆上分配的内存
    char buf_[VARIABLE_BUF_LEN + 1];
  } ;
  DataUnion data_union_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CONFIG_OB_CONFIG_HELPER_H_
