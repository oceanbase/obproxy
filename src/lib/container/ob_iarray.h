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

#ifndef OCEANBASE_LIB_CONTAINER_IARRAY_
#define OCEANBASE_LIB_CONTAINER_IARRAY_
#include <stdint.h>
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace common
{
// this interface has three derived classes: ObArray, ObSEArray and Ob2DArray.
template <typename T>
class ObIArray
{
public:
  virtual ~ObIArray() {}
  virtual int push_back(const T &obj) = 0;
  virtual void pop_back() = 0;
  virtual int pop_back(T &obj) = 0;
  virtual int remove(int64_t idx) = 0;

  virtual int at(int64_t idx, T &obj) const = 0;
  virtual T &at(int64_t idx) = 0;
  virtual const T &at(int64_t idx) const = 0;

  virtual int64_t count() const = 0;
  /// reset is the same as clear()
  virtual void reset() = 0;
  virtual void reuse() = 0;
  virtual void destroy() = 0;
  virtual int reserve(int64_t capacity) = 0;
  virtual int assign(const ObIArray &other) = 0;
  virtual int prepare_allocate(int64_t capacity) = 0;
  virtual T *alloc_place_holder()
  { OB_LOG(WARN, "Not supported"); return NULL; }

  //virtual bool empty() { return 0 == count(); }
  virtual bool empty() const { return 0 == count(); }
  virtual int64_t to_string(char* buf, int64_t buf_len) const
  {
    int64_t pos = 0;
    J_ARRAY_START();
    int64_t N = count();
    for (int64_t index = 0; index < N - 1; ++index) {
      databuff_printf(buf, buf_len, pos, "[%ld]", index);
      BUF_PRINTO(at(index));
      J_COMMA();
    }
    if (0 < N) {
      BUF_PRINTO(at(N - 1));
    }
    J_ARRAY_END();
    return pos;
  }
};

template<typename T>
int append(ObIArray<T>&dst, const ObIArray<T> &other)
{
  int ret = OB_SUCCESS;
  int64_t N = other.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    ret = dst.push_back(other.at(i));
  } // end for
  return ret;
}

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_LIB_CONTAINER_IARRAY_
