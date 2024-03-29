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

#include "ob_random.h"
#include <stdlib.h>
#include <math.h>

namespace oceanbase
{
namespace common
{
ObRandom::ObRandom()
    : seed_()
{
  seed_[0] = (uint16_t)rand(0, 65535);
  seed_[1] = (uint16_t)rand(0, 65535);
  seed_[2] = (uint16_t)rand(0, 65535);
  seed48(seed_);
}

ObRandom::~ObRandom()
{
}

int64_t ObRandom::rand(const int64_t a, const int64_t b)
{
  static __thread uint16_t seed[3] = {0, 0, 0};
  if (0 == seed[0] && 0 == seed[1] && 0 == seed[2]) {
    seed[0] = static_cast<uint16_t>(GETTID());
    seed[1] = seed[0];
    seed[2] = seed[1];
    seed48(seed);
  }
  const int64_t r1 = jrand48(seed);
  const int64_t r2 = jrand48(seed);
  int64_t min = a < b ? a : b;
  int64_t max = a < b ? b : a;
  return min + labs((r1 << 32) | r2) % (max - min + 1);
}

int64_t ObRandom::get()
{
  const int64_t r1 = jrand48(seed_);
  const int64_t r2 = jrand48(seed_);
  return ((r1 << 32) | r2);
}

int64_t ObRandom::get(const int64_t a, const int64_t b)
{
  int64_t min = a < b ? a : b;
  int64_t max = a < b ? b : a;
  return min + labs(get()) % (max - min + 1);
}

int32_t ObRandom::get_int32()
{
  return static_cast<int32_t>(jrand48(seed_));
}

int32_t ObRandom::get_int32(const int32_t a, const int32_t b)
{
  int32_t min = a < b ? a : b;
  int32_t max = a < b ? b : a;
  return min + abs(get_int32()) % (max - min + 1);
}

int64_t ObRandom::operator() (int64_t max)
{
  if (max <= 1) {
    return 0;
  }

  return get(0, max - 1);
}

} /* namespace common */
} /* namespace oceanbase */
