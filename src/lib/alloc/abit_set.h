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

#ifndef _OCEABASE_LIB_ALLOC_BIT_SET_H_
#define _OCEABASE_LIB_ALLOC_BIT_SET_H_

#include <stdint.h>
#include <cstring>

namespace oceanbase
{
namespace lib
{

// Thread unsafe, two level BitSet
template <int64_t N>
class ABitSet
{
public:
  ABitSet()
  {
    clear();
  }

  void clear()
  {
    first_level_ = 0;
    memset(second_level_, 0, sizeof (second_level_));
  }

  // find first least significant bit start from start(includ).
  int find_first_significant(int start) const
  {
    int ret = 0;
    const int maxseg = (N-1)/64 + 1;
    const int seg = start / 64;
    const int pos = start % 64;

    if (start >= N) {
      // overflow
    } else {
      ret = myffsl(second_level_[seg], pos);
      if (ret > 0) {
        // found directly in second level
        ret += seg * 64;
      } else if (seg >= maxseg) {
        // not found
      } else {
        ret = myffsl(first_level_, seg+1);
        if (ret <= 0) {
          // not found
        } else {
          int ret2 = myffsl(second_level_[ret-1], 0);
          ret = (ret - 1) * 64 + ret2;
        }
      }
    }
    return ret - 1;
  };

  void set(int idx)
  {
    if (idx >= N) {
      // not allowed
    } else {
      const int seg = idx / 64;
      const int pos = idx % 64;

      first_level_ |= 1UL << seg;
      second_level_[seg] |= 1UL << pos;
    }
  }

  void unset(int idx)
  {
    if (idx >= N) {
      // not allowed
    } else {
      const int seg = idx / 64;
      const int pos = idx % 64;

      second_level_[seg] &= ~(1UL << pos);
      if (0 == second_level_[seg]) {
        first_level_ &= ~(1UL << seg);
      }
    }
  }
#ifdef isset
# undef isset
#endif
  bool isset(int idx)
  {
    bool ret = false;
    if (idx >= N) {
      // not allowed
    } else {
      const int seg = idx / 64;
      const int pos = idx % 64;

      ret = second_level_[seg] & (1UL << pos);
    }
    return ret;
  }

private:
  int myffsl(uint64_t v, int pos) const
  {
    return ffsl(v & ~((1UL << pos) - 1));
  }

private:
  uint64_t first_level_;
  uint64_t second_level_[(N-1)/64+1];
};

} // end of namespace
} // end of namespace oceanbase


#endif /* _OCEABASE_LIB_ALLOC_BIT_SET_H_ */
