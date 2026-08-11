/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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


