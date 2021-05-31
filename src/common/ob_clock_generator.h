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
