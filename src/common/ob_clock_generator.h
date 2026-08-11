/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_CLOCK_GENERATOR_
#define OCEANBASE_COMMON_OB_CLOCK_GENERATOR_

#include <stdint.h>
#include <pthread.h>

namespace oceanbase
{
namespace common
{

class ObClockGenerator 
{
private:
  ObClockGenerator() {}
  virtual ~ObClockGenerator() {}
public:
  static int init();
  static void destroy();
  static int64_t getClock();
  static int64_t getRealClock();
  static void msleep(const int64_t ms);
private:
  static int64_t get_us();
  static void *routine(void *arg);
private:
  static bool inited_;
  static bool ready_;
  static pthread_t tid_;
  static int64_t cur_ts_;
};

} // oceanbase
} // common

#endif //OCEANBASE_COMMON_OB_CLOCK_GENERATOR_
