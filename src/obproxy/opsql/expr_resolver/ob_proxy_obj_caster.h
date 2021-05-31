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

#ifndef OBPROXY_OBJ_CASTER_H
#define OBPROXY_OBJ_CASTER_H
#include "common/ob_object.h"
#include "common/ob_obj_cast.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace obproxy
{
namespace opsql
{
class ObProxyObjCaster
{
public:
  explicit ObProxyObjCaster(common::ObIAllocator &allocator);
  int cast(const common::ObObj orig_obj, const common::ObObjType expect_type,
           const common::ObObj *&res_obj);
private:
  common::ObIAllocator &allocator_;
  common::ObObj casted_cell_;
  common::ObCastCtx cast_ctx_;
};

} // end of namespace opsql
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_OBJ_CASTER_H
