/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ATOMIC_REFERENCE_H_
#define OB_ATOMIC_REFERENCE_H_
#include <stdint.h>

namespace oceanbase
{
namespace common
{

union AtomicInt64
{
  volatile uint64_t atomic;
  struct
  {
    uint32_t buffer;
    uint32_t pairs;
  };
  struct
  {
    uint32_t ref;
    uint32_t seq;
  };
};


class ObAtomicReference
{
public:
  ObAtomicReference();
  virtual ~ObAtomicReference();
  void reset();
  int inc_ref_cnt();
  int check_seq_num_and_inc_ref_cnt(const uint32_t seq_num);
  int check_and_inc_ref_cnt();
  int dec_ref_cnt_and_inc_seq_num(uint32_t &ref_cnt);
  inline uint32_t get_seq_num() const { return atomic_num_.seq; }
private:
  AtomicInt64 atomic_num_;
};

}
}

#endif /* OB_ATOMIC_REFERENCE_H_ */
