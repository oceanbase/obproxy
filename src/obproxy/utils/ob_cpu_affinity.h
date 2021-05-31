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

#ifndef OBPROXY_CPU_AFFINITY
#define OBPROXY_CPU_AFFINITY

#include <stdint.h>
#include <pthread.h>
#include "lib/utility/ob_macro_utils.h"

/*

# The following is the parsable format, which can be fed to other
# programs. Each different item in every column has an unique ID
# starting from zero.
CPU, Core, Socket, Node, ,L1d, L1i, L2, L3
0,   0,    0,      0,    ,0,   0,   0,  0
1,   1,    0,      0,    ,1,   1,   1,  0
2,   2,    0,      0,    ,2,   2,   2,  0
3,   3,    0,      0,    ,3,   3,   3,  0
4,   4,    0,      0,    ,4,   4,   4,  0
5,   5,    0,      0,    ,5,   5,   5,  0
6,   6,    1,      0,    ,6,   6,   6,  1
7,   7,    1,      0,    ,7,   7,   7,  1
8,   8,    1,      0,    ,8,   8,   8,  1
9,   9,    1,      0,    ,9,   9,   9,  1
10,  10,   1,      0,    ,10,  10,  10, 1
11,  11,   1,      0,    ,11,  11,  11, 1
12,  0,    0,      0,    ,0,   0,   0,  0
13,  1,    0,      0,    ,1,   1,   1,  0
14,  2,    0,      0,    ,2,   2,   2,  0
15,  3,    0,      0,    ,3,   3,   3,  0
16,  4,    0,      0,    ,4,   4,   4,  0
17,  5,    0,      0,    ,5,   5,   5,  0
18,  6,    1,      0,    ,6,   6,   6,  1
19,  7,    1,      0,    ,7,   7,   7,  1
20,  8,    1,      0,    ,8,   8,   8,  1
21,  9,    1,      0,    ,9,   9,   9,  1
22,  10,   1,      0,    ,10,  10,  10, 1
23,  11,   1,      0,    ,11,  11,  11, 1

*/

namespace oceanbase
{
namespace obproxy
{

class ObCpuTopology
{
public:
  static const int64_t MAX_CPU_NUMBER_PER_CORE = 4;
  static const int64_t MAX_CORE_NUMBER = 128;

public:
  struct CoreInfo
  {
    int64_t cpu_number_;
    int64_t cpues_[MAX_CPU_NUMBER_PER_CORE];
  };

  ObCpuTopology();
  ~ObCpuTopology() { }

  int init();
  int64_t get_core_number() const;
  int64_t get_cpu_number() const;
  CoreInfo *get_core_info(const int64_t core_id);
  int bind_cpu(const int64_t cpu_id, const pthread_t thread_id);

private:
  bool is_inited_;
  int64_t core_number_;
  int64_t cpu_number_;
  CoreInfo cores_[MAX_CORE_NUMBER];

  DISALLOW_COPY_AND_ASSIGN(ObCpuTopology);
};

} // namespace obproxy
} // namespace oceanbase

#endif // OBPROXY_CPU_AFFINITY
