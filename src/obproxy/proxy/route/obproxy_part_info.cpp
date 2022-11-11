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

#define USING_LOG_PREFIX PROXY
#include "proxy/route/obproxy_part_info.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "opsql/expr_parser/ob_expr_parser_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::obproxy::opsql;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
ObProxyPartOption::ObProxyPartOption() : part_func_type_(PARTITION_FUNC_TYPE_MAX)
                                       , part_space_(0)
                                       , part_num_(0)
{
}

ObProxyPartInfo::ObProxyPartInfo() : is_oracle_mode_(false)
                                   , has_generated_key_(false)
                                   , has_unknown_part_key_(false)
                                   , is_template_table_(true)
                                   , part_level_(PARTITION_LEVEL_ZERO)
                                   , table_cs_type_(CS_TYPE_INVALID)
                                   , allocator_()
                                   , first_part_option_()
                                   , sub_part_option_()
                                   , part_key_info_()
                                   , part_mgr_(allocator_)
                                   , cluster_version_()
{
}

int64_t ObProxyPartOption::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this),
       K_(part_func_type),
       K_(part_space),
       K_(part_num)
       );
  J_OBJ_END();
  return pos;
}

int64_t ObProxyPartInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this),
       K_(is_oracle_mode),
       K_(has_generated_key),
       K_(has_unknown_part_key),
       K_(is_template_table),
       K_(part_level),
       K_(table_cs_type),
       K_(first_part_option),
       K_(sub_part_option),
       "part_key_info", ObProxyPartKeyInfoPrintWrapper(part_key_info_),
       K_(part_mgr)
       );

  J_OBJ_END();
  return pos;
}

int ObProxyPartInfo::alloc(ObProxyPartInfo *&part_info)
{
  int ret = OB_SUCCESS;
  char *buf = static_cast<char *>(op_fixed_mem_alloc(sizeof(ObProxyPartInfo)));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", K(sizeof(ObProxyPartInfo)), K(ret));
  } else {
    part_info = new (buf) ObProxyPartInfo();
  }
  return ret;
}

void ObProxyPartInfo::free()
{
  allocator_.reset();
  op_fixed_mem_free(this, sizeof(ObProxyPartInfo));
}

} // namespace route
} // namespace obproxy
} // namespace oceanbase
