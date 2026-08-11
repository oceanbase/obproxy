/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef  OCEANBASE_LIB_UNION_FIND_SET_
#define  OCEANBASE_LIB_UNION_FIND_SET_

#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_fixed_array.h"

namespace oceanbase
{
namespace lib
{

class ObUnionFindSet
{
public:
  ObUnionFindSet() : is_init_(false), default_allocator_(), local_arr_() {}
  ~ObUnionFindSet() {}
  int init(const int64_t size, common::ObIAllocator* allocator = NULL) {
    int ret = common::OB_SUCCESS;

    allocator = allocator == NULL? &default_allocator_ : allocator;
    if (OB_UNLIKELY(is_init_)) {
      ret = common::OB_INIT_TWICE;
      OB_LOG(WDIAG, "double init ", K(size), K(ret));
    } else if (OB_ISNULL(allocator)) {
      ret = common::OB_INVALID_ARGUMENT;
      OB_LOG(WDIAG, "fail to prepare allocate local_arr_", K(size), K(ret));
    } else if (FALSE_IT(local_arr_.set_allocator(allocator))) {
      // impossible
    } else if (OB_FAIL(local_arr_.prepare_allocate(size))) {
      OB_LOG(WDIAG, "fail to prepare allocate local_arr_", K(ret));
    } else {
      for (int64_t i = 0; i < size; ++i) {
        local_arr_.at(i) = i;
      }
      is_init_ = true;
    }
    return ret;
  }

  int find(int64_t x, int64_t& root) {
    int ret = common::OB_SUCCESS;

    if (OB_UNLIKELY(!is_init_)) {
      ret = common::OB_NOT_INIT;
      OB_LOG(WDIAG, "not init", K(x), "count", local_arr_.count(), K(ret));
    } else if (OB_UNLIKELY(x < 0 || local_arr_.count() <= x)) {
      ret = common::OB_ARRAY_OUT_OF_RANGE;
      OB_LOG(WDIAG, "idx out of range", K(x), "count", local_arr_.count(), K(ret));
    } else {
      root = do_find(x);
    }

    return ret;
  }

  int union_merge(int64_t x, int64_t y) {
    int ret = common::OB_SUCCESS;

    if (OB_UNLIKELY(!is_init_)) {
      ret = common::OB_NOT_INIT;
      OB_LOG(WDIAG, "not init", K(x), "count", local_arr_.count(), K(ret));
    } else if (OB_UNLIKELY(x < 0 || local_arr_.count() <= x
               || y < 0 || local_arr_.count() <= y)) {
      ret = common::OB_ARRAY_OUT_OF_RANGE;
      OB_LOG(WDIAG, "idx out of range", K(x), K(y), "count", local_arr_.count(), K(ret));
    } else {
      do_union_merge(x, y);
    }

    return ret;
  }

  int64_t count() const { return local_arr_.count(); }

  int to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(is_init) /*, K_(local_arr)*/ );
    J_OBJ_END();
    return pos;
  }

private:
  int64_t do_find(int64_t x) {
    if (x == local_arr_.at(x)) {
      return x;
    } else {
      return do_find(local_arr_.at(x));
    }
  }

  void do_union_merge(int64_t x, int64_t y) {
    local_arr_.at(do_find(y)) = local_arr_.at(do_find(x));
  }

private:
  bool is_init_;
  common::ObArenaAllocator default_allocator_;
  common::ObFixedArray<int64_t, common::ObIAllocator> local_arr_;
};

}//namespace lib
}//namespace oceanbase
#endif //OCEANBASE_LIB_UNION_FIND_SET_
