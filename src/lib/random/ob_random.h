/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_RANDOM_H_
#define OB_RANDOM_H_

#include <stdint.h>
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{

class ObRandom
{
public:
  ObRandom();
  virtual ~ObRandom();
  //get a random int64_t number in [min(a,b), max(a,b)]
  static int64_t rand(const int64_t a, const int64_t b);
  //get a random int64_t number
  int64_t get();
  //get a random int64_t number in [min(a,b), max(a,b)]
  int64_t get(const int64_t a, const int64_t b);
  //get a random int32_t number
  int32_t get_int32();
  //get a random int32_t number in [min(a,b), max(a,b)]
  int32_t get_int32(const int32_t a, const int32_t b);
  int64_t operator() (int64_t max);
private:
  uint16_t seed_[3];
private:
  DISALLOW_COPY_AND_ASSIGN(ObRandom);
};

} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_RANDOM_H_ */
