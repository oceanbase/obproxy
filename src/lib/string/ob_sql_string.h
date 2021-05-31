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

#ifndef OCEANBASE_COMMON_OB_SQL_STRING_H_
#define OCEANBASE_COMMON_OB_SQL_STRING_H_

#include <stdarg.h>
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace common
{
class ObString;

// ObSqlString only used to concatenate SQL strings, for example:
//
//   ObSqlString sql;
//   int ret = sql.assign_fmt("SELECT c2 FROM %s WHERE c1 = %d", table_name, c1);
//
// ObSqlString add '\0' to the end of data, it's safe to use ptr() as C string.
//
class ObSqlString
{

public:
  explicit ObSqlString(const int64_t mod_id = ObModIds::OB_SQL_STRING);
  virtual ~ObSqlString();

  void set_mod_id(const int64_t mod_id) { allocator_.set_mod_id(mod_id); }
  void reset();
  int reserve(const int64_t size);

  int append(const char *str);
  int append(const char *str, const int64_t len);
  int append(const ObString &str);
  int append_fmt(const char *fmt, ...) __attribute__((format(printf, 2, 3)));

  int assign(const char *str);
  int assign(const char *str, const int64_t len);
  int assign(const ObString &str);
  int assign_fmt(const char *fmt, ...) __attribute__((format(printf, 2, 3)));

  const ObString string() const;
  const char *ptr() const { return data_; }
  int64_t length() const { return len_; }
  int64_t capacity() const { return data_size_ > 0 ? data_size_ - 1 : 0; }
  bool empty() const { return 0 == length(); }

  // Splice sql out of ObSqlString. example:
  //
  //   while (int64_t i = 0; i < sql.capacity(); ++i) {
  //      sql.ptr()[i] = ';';
  //   }
  //   ret = sql.set_length(sql.capacity());
  //
  // Dangerous api, caller's responsibility to ensure that modification occurs in [0, capacity())
  // and ptr() stable (no appendxxx(), assignxxx(), reset() called before set_length()).
  char *ptr() { return data_; }
  int set_length(const int64_t len);
  void reuse();

  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  int vappend(const char *fmt, va_list ap);
  int extend(const int64_t size);

private:
  static const int64_t MAX_SQL_STRING_LEN = 512;
  char *data_;
  int64_t data_size_;
  int64_t len_;
  ObMalloc allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObSqlString);
};
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_SQL_STRING_H_
