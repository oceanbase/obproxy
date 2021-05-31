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

#ifndef _OCEABASE_COMMON_OB_COMMON_UTILITY_H_
#define _OCEABASE_COMMON_OB_COMMON_UTILITY_H_

#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
extern const char *print_server_role(const common::ObServerRole server_role);

//@brief recursive function call should use this function to check if recursion is too deep
//to avoid stack overflow, default reserved statck size is 1M
extern int64_t get_reserved_stack_size();
extern void set_reserved_stack_size(int64_t reserved_size);
extern int check_stack_overflow(
    bool &is_overflow, int64_t reserved_stack_size = get_reserved_stack_size());

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OCEABASE_COMMON_OB_COMMON_UTILITY_H_ */
