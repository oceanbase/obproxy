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

#include "opsql/expr_resolver/ob_proxy_obj_caster.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace obproxy
{
namespace opsql
{
ObProxyObjCaster::ObProxyObjCaster(ObIAllocator &allocator) : allocator_(allocator)
                                                            , casted_cell_()
                                                            , cast_ctx_(&allocator_,
                                                                        NULL,
                                                                        CM_NONE,
                                                                        CS_TYPE_UTF8MB4_GENERAL_CI)
{
}

int ObProxyObjCaster::cast(const ObObj orig_obj, const ObObjType expect_type, const ObObj *&res_obj)
{
  return ObObjCasterV2::to_type(expect_type, cast_ctx_, orig_obj, casted_cell_, res_obj);
}

} // end of opsql
} // end of obproxy
} // end of oceanbase
