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

#ifndef OB_PROXY_STRING_UTILS_H_
#define OB_PROXY_STRING_UTILS_H_
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "obutils/ob_proxy_json_config_info.h"

namespace oceanbase
{
namespace common
{
class ObMalloc;
}
namespace obproxy
{
namespace obutils
{
// variant string, alloc mem on demand
class ObProxyVariantString
{
public:
  ObProxyVariantString();
  ~ObProxyVariantString();
  ObProxyVariantString(const ObProxyVariantString& vstr);
  ObProxyVariantString& operator=(const ObProxyVariantString& vstr);
  bool equals(const ObProxyVariantString& vstr) const;
  void reset();
  bool empty() const { return config_string_.empty(); }
  bool is_valid() const { return !config_string_.empty(); }
  int32_t length() const { return config_string_.length(); }
  const char *ptr() const{ return data_; }
  operator const common::ObString &() const { return config_string_; }
  bool init(common::ObMalloc* allocator, const int64_t mod_id);
  bool set_value(const common::ObString &value);
  bool set_value(const int32_t len, const char *value);
  bool set_value_with_quote(const common::ObString &value, const char quote);
  bool set_value_with_quote(const int32_t len, const char *value, const char quote);
  uint64_t hash(uint64_t seed = 0) const { return config_string_.hash(seed); }
  void set_integer(const int64_t other);
  common::ObString config_string_;
  DECLARE_TO_STRING;
private:
  char* data_;
  int32_t data_size_;
};
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif //OB_PROXY_STRING_UTILS_H_
