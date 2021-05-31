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
#include <iostream>
#include <fstream>
#include <iterator>
#include "ob_session_vars_test_utils.h"
#include "lib/ob_define.h"
#include "ob_session_field_mgr.h"
#include "sql/session/ob_system_variable_init.h"

namespace oceanbase
{
namespace obproxy
{
using namespace oceanbase::common;
using namespace oceanbase::sql;
using std::cout;

int ObSessionVarsTestUtils::load_default_system_variables(
    ObIAllocator &allocator,
    ObDefaultSysVarSet &default_set,
    bool  print_info_log)
{
  UNUSED(print_info_log);
  int ret = OB_SUCCESS;
  /*ObString name;
  ObObj type;
  ObObj value;

#define LOAD_SYS_VAR_INT(ret, cname, dvalue, dflags, print_info_log) \
  if (OB_SUCC(ret)) { \
    if (!default_set.sys_variable_exists_local(ObString::make_string(cname))) { \
      name.assign_ptr(const_cast<char*>(cname),\
                      static_cast<ObString::obstr_size_t>(strlen(cname))); \
      type.set_type(ObIntType); \
      value.set_int(dvalue);\
      if (OB_FAIL(default_set.load_system_variable(name, type, value, dflags))) {\
        LOG_ERROR("fail to load default system variable", cname, dvalue, K(ret)); \
      } else if (print_info_log) { \
        LOG_INFO("load default system variable,", cname, dvalue); \
      }\
    } else {\
      ret = OB_ERR_ALREADY_EXISTS; \
      LOG_INFO("system variable already exist", cname, dvalue, K(ret)); \
    } \
  }

#define LOAD_SYS_VAR_VARCHAR(ret, cname, dvalue, dflags, print_info_log) \
  if (OB_SUCC(ret)) {\
    if (!default_set.sys_variable_exists_local(ObString::make_string(cname))) { \
      name.assign_ptr(const_cast<char*>(cname),\
                      static_cast<ObString::obstr_size_t>(strlen(cname))); \
      type.set_type(ObVarcharType); \
      value.set_varchar(dvalue);\
      if (OB_FAIL(default_set.load_system_variable(name, type, value, dflags))) {\
        LOG_ERROR("fail to load default system variable", cname, to_cstring(dvalue), K(ret)); \
      } else if (print_info_log) {\
        LOG_INFO("load default system variable", cname, to_cstring(dvalue)); \
      }\
    } else {\
      ret = OB_ERR_ALREADY_EXISTS; \
      LOG_INFO("system variable already exist", cname, dvalue, K(ret)); \
    } \
  }

  const int64_t both_scope = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
  char version_comment[256];
  snprintf(version_comment, 256, "OceanBase 1.0 (r1101) (Built yyy hust)");
  LOAD_SYS_VAR_INT(ret, "autocommit", 1, both_scope, print_info_log);
  LOAD_SYS_VAR_INT(ret, "auto_increment_increment", 1, both_scope, print_info_log);
  LOAD_SYS_VAR_INT(ret, "auto_increment_offset", 1, both_scope, print_info_log);
  LOAD_SYS_VAR_INT(ret, "last_insert_id", 0, both_scope, print_info_log);
  LOAD_SYS_VAR_VARCHAR(ret, "character_set_results", ObString::make_string("latin1"), both_scope, print_info_log);
  LOAD_SYS_VAR_INT(ret, "interactive_timeout", 0, both_scope, print_info_log);
  LOAD_SYS_VAR_INT(ret, "ob_query_timeout", 5000000, both_scope, print_info_log);
  LOAD_SYS_VAR_INT(ret, "ob_read_consistency", 4, both_scope, print_info_log);
  LOAD_SYS_VAR_INT(ret, "ob_trx_timeout", 100000000, both_scope, print_info_log);
  LOAD_SYS_VAR_VARCHAR(ret, "sql_mode", ObString::make_empty_string(), both_scope, print_info_log);
  LOAD_SYS_VAR_VARCHAR(ret, "tx_isolation", ObString::make_string("READ-COMMITTED"), both_scope, print_info_log);
  LOAD_SYS_VAR_VARCHAR(ret, "version_comment", ObString::make_string(version_comment),
                       (ObSysVarFlag::READONLY | ObSysVarFlag::GLOBAL_SCOPE), print_info_log);
  LOAD_SYS_VAR_INT(ret, "wait_timeout", 0, both_scope, print_info_log);
  LOAD_SYS_VAR_VARCHAR(ret, "ob_log_level", ObString::make_string("disabled"), both_scope, print_info_log);

  LOAD_SYS_VAR_VARCHAR(ret, "character_set_connection", ObString::make_string("utf8mb4"), both_scope, print_info_log);
  LOAD_SYS_VAR_VARCHAR(ret, "collation_connection", ObString::make_string("utf8mb4_general_ci"), both_scope, print_info_log);
  LOAD_SYS_VAR_INT(ret, "lower_case_table_names", 2, both_scope, print_info_log);
  LOAD_SYS_VAR_INT(ret, "net_read_timeout", 30, both_scope, print_info_log);
  LOAD_SYS_VAR_INT(ret, "net_write_timeout", 60, both_scope, print_info_log);
  LOAD_SYS_VAR_INT(ret, "wait_timeout", 28800, both_scope, print_info_log);*/

  ObString name;
  ObObj type;
  ObObj value;
  int64_t var_flag = ObSysVarFlag::NONE;
  ObObjType var_type = ObNullType;
  int64_t var_amount = ObSysVariables::get_amount();
  for(int64_t i = 0; OB_SUCC(ret) && i < var_amount; i++){
    name.assign_ptr(const_cast<char*>(ObSysVariables::get_name(i).ptr()),
                    static_cast<ObString::obstr_size_t>(strlen(ObSysVariables::get_name(i).ptr())));
    var_type = ObSysVariables::get_type(i);
    var_flag = ObSysVariables::get_flags(i);
    value.set_varchar(ObSysVariables::get_value(i));
    type.set_type(var_type);
    ObObj casted_cell;
    const ObObj *res_cell = NULL;
    ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, ObCharset::get_system_collation());
    if (OB_FAIL(ObObjCasterV2::to_type(type.get_type(), cast_ctx, value, casted_cell, res_cell))) {
      LOG_WARN("failed to cast object", K(ret), K(value), K(value.get_type()), K(type.get_type()));
    } else if (OB_FAIL(default_set.load_system_variable(name, type, *res_cell, var_flag))) {
      LOG_ERROR("fail to load default system variable", K(ret), K(name), K(*res_cell), K(var_flag));
    } else {
      LOG_INFO("load default system variable", K(name), K(value));
    }
  }
  return ret;
}

}
}


