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

#ifndef OCEANBASE_SQL_SESSION_OB_SYSTEM_VARIABLE_INIT_
#define OCEANBASE_SQL_SESSION_OB_SYSTEM_VARIABLE_INIT_
#include "sql/session/ob_sys_var_class_type.h"
#include <stdint.h>
#include "common/ob_object.h"
namespace oceanbase
{
namespace sql
{
struct ObSysVarFlag
{
  const static int64_t NONE = 0LL;
  const static int64_t GLOBAL_SCOPE = 1LL;
  const static int64_t SESSION_SCOPE = (1LL << 1);
  const static int64_t READONLY = (1LL << 2);
  const static int64_t SESSION_READONLY = (1LL << 3);
  const static int64_t INVISIBLE = (1LL << 4);
  const static int64_t NULLABLE = (1LL << 5);
  const static int64_t INFLUENCE_PLAN = (1LL << 6);
  const static int64_t NEED_SERIALIZE = (1LL << 7);
};
struct ObSysVarFromJson{
  ObSysVarClassType id_;
  common::ObString name_;
  common::ObObjType data_type_;
  common::ObString value_;
  common::ObString min_val_;
  common::ObString max_val_;
  common::ObString enum_names_;
  common::ObString info_;
  int64_t flags_;
  common::ObString alias_;
  common::ObString base_class_;
  common::ObString on_check_and_convert_func_;
  common::ObString on_update_func_;
  common::ObString to_select_obj_func_;
  common::ObString to_show_str_func_;
  common::ObString get_meta_type_func_;
  common::ObString session_special_update_func_;

  ObSysVarFromJson():id_(SYS_VAR_INVALID), name_(""), data_type_(common::ObNullType), value_(""), min_val_(""), max_val_(""), enum_names_(""), info_(""), flags_(ObSysVarFlag::NONE), alias_(""), base_class_(""), on_check_and_convert_func_(), on_update_func_(), to_select_obj_func_(), to_show_str_func_(), get_meta_type_func_(), session_special_update_func_() {}
};

class ObSysVariables
{
public:
  static int64_t get_all_sys_var_count();
  static ObSysVarClassType get_sys_var_id(int64_t i);
  static common::ObString get_name(int64_t i);
  static common::ObObjType get_type(int64_t i);
  static common::ObString get_value(int64_t i);
  static common::ObString get_min(int64_t i);
  static common::ObString get_max(int64_t i);
  static common::ObString get_info(int64_t i);
  static int64_t get_flags(int64_t i);
  static common::ObString get_alias(int64_t i);
  static const common::ObObj &get_default_value(int64_t i);
  static int64_t get_amount();
  static int set_value(const char *name, const char * new_value);
  static int init_default_values();
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_SESSION_OB_SYSTEM_VARIABLE_INIT_ */
