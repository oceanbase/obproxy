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