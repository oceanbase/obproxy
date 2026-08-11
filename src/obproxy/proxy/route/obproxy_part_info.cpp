/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
                                   , has_part_func_key_(false)
                                   , has_unknown_part_key_(false)
                                   , is_template_table_(true)
                                   , is_primary_key_as_part_expr_(false)
                                   , part_level_(PARTITION_LEVEL_ZERO)
                                   , table_cs_type_(CS_TYPE_INVALID)
                                   , part_expr_()
                                   , sub_part_expr_()
                                   , part_range_type_()
                                   , sub_part_range_type_()
                                   , allocator_()
                                   , first_part_option_()
                                   , sub_part_option_()
                                   , part_key_info_()
                                   , part_mgr_(allocator_)
                                   , cluster_version_()
                                   , schema_version_(0)
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
       K_(has_part_func_key),
       K_(has_unknown_part_key),
       K_(is_template_table),
       K_(is_primary_key_as_part_expr),
       K_(part_level),
       K_(table_cs_type),
       K_(first_part_option),
       K_(sub_part_option),
       K_(schema_version),
       "part_key_info", ObProxyPartKeyInfoPrintWrapper(part_key_info_),
       K_(part_mgr),
       K_(first_part_columns),
       K_(sub_part_columns)
       );

  J_OBJ_END();
  return pos;
}

int64_t ObProxyPartInfo::get_part_idx(const ObString& column_name)
{
  int64_t part_idx = -1;

  for (int64_t i = 0; i < part_key_info_.key_num_; ++i) {
    ObProxyParseString& parse_part_key_name = part_key_info_.part_keys_[i].name_;
    ObString part_key_name(parse_part_key_name.str_len_, parse_part_key_name.str_);
    if (0 == column_name.case_compare(part_key_name)) {
      part_idx = i;
      break;
    }
  }

  return part_idx;
}

int64_t ObProxyPartInfo::get_first_part_idx(const ObString& column_name)
{
  int64_t first_part_idx = -1;

  for (int64_t i = 0; i < first_part_columns_.count(); ++i) {
    if (0 == column_name.case_compare(first_part_columns_.at(i))) {
      first_part_idx = i;
      break;
    }
  }

  return first_part_idx;
}

int64_t ObProxyPartInfo::get_sub_part_idx(const ObString& column_name)
{
  int64_t sub_part_idx = -1;

  for (int64_t i = 0; i < sub_part_columns_.count(); ++i) {
    if (0 == column_name.case_compare(sub_part_columns_.at(i))) {
      sub_part_idx = i;
      break;
    }
  }

  return sub_part_idx;
}


int ObProxyPartInfo::alloc(ObProxyPartInfo *&part_info)
{
  int ret = OB_SUCCESS;
  char *buf = static_cast<char *>(op_fixed_mem_alloc(sizeof(ObProxyPartInfo)));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem", K(sizeof(ObProxyPartInfo)), K(ret));
  } else {
    part_info = new (buf) ObProxyPartInfo();
  }
  return ret;
}

void ObProxyPartInfo::free()
{
  part_mgr_.destroy();
  first_part_columns_.reset();
  sub_part_columns_.reset();
  allocator_.reset();
  op_fixed_mem_free(this, sizeof(ObProxyPartInfo));
}

} // namespace route
} // namespace obproxy
} // namespace oceanbase
