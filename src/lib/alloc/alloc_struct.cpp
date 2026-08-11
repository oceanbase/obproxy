/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "alloc_struct.h"
#include "lib/allocator/ob_mod_define.h"

using namespace oceanbase::lib;

namespace oceanbase
{
namespace lib
{

bool ObMemAttr::is_global_mem_mod() const {
  return oceanbase::common::ObModIds::OB_PROXY_GLOBAL_PS == mod_id_;
}

} // end of namespace lib
} // end of namespace oceanbased