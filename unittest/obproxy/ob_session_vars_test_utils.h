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

#ifndef OBPROXY_PARSER_UTILS_H
#define OBPROXY_PARSER_UTILS_H
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
};
namespace obproxy
{
class ObDefaultSysVarSet;
class ObSessionVarsTestUtils
{
public:
  static int load_default_system_variables(common::ObIAllocator &allocator, ObDefaultSysVarSet &default_set, bool print_log);

};

}
}
#endif // OBPROXY_PARSER_UTILS_H


