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

#ifndef OBPROXY_UTILS_CACHED_VARIABLES_H
#define OBPROXY_UTILS_CACHED_VARIABLES_H

#include "common/ob_object.h"
#include "common/ob_sql_mode.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
enum ObCachedVariableType
{
  // Don't forget to modify type names in ob_cached_variables.cpp
  CACHED_INT_VAR_QUERY_TIMEOUT,
  CACHED_INT_VAR_WAIT_TIMEOUT,
  CACHED_INT_VAR_NET_READ_TIMEOUT,
  CACHED_INT_VAR_NET_WRITE_TIMEOUT,
  CACHED_INT_VAR_TRX_TIMEOUT,
  CACHED_INT_VAR_AUTOCOMMIT,
  CACHED_INT_VAR_LOWER_CASE_TABLE_NAMES,
  CACHED_INT_VAR_TX_READ_ONLY,
  CACHED_INT_VAR_READ_CONSISTENCY,
  CACHED_VAR_MAX,
};

class ObCachedVariables
{
public:
  static ObCachedVariableType get_type(const common::ObString &name);
  static common::ObString get_name(const ObCachedVariableType type);

  ObCachedVariables();
  ~ObCachedVariables() {}

  int update_var(const common::ObString &name, const common::ObObj &obj);
  int update_var(const ObCachedVariableType &type, const common::ObObj &obj);

  int64_t get_query_timeout() const { return get_int_var(CACHED_INT_VAR_QUERY_TIMEOUT); }
  int64_t get_trx_timeout() const { return get_int_var(CACHED_INT_VAR_TRX_TIMEOUT); }
  int64_t get_wait_timeout() const { return get_int_var(CACHED_INT_VAR_WAIT_TIMEOUT); }
  int64_t get_net_read_timeout() const { return get_int_var(CACHED_INT_VAR_NET_READ_TIMEOUT); }
  int64_t get_net_write_timeout() const { return get_int_var(CACHED_INT_VAR_NET_WRITE_TIMEOUT); }
  int64_t get_autocommit() const { return get_int_var(CACHED_INT_VAR_AUTOCOMMIT); }
  int64_t get_lower_case_table_names() const { return get_int_var(CACHED_INT_VAR_LOWER_CASE_TABLE_NAMES); }
  int64_t get_tx_read_only() const { return get_int_var(CACHED_INT_VAR_TX_READ_ONLY); }
  int64_t get_read_consistency() const { return get_int_var(CACHED_INT_VAR_READ_CONSISTENCY); }

  const common::ObObj &get_query_timeout_obj() const { return get_obj_var(CACHED_INT_VAR_QUERY_TIMEOUT); }
  const common::ObObj &get_trx_timeout_obj() const { return get_obj_var(CACHED_INT_VAR_TRX_TIMEOUT); }
  const common::ObObj &get_wait_timeout_obj() const { return get_obj_var(CACHED_INT_VAR_WAIT_TIMEOUT); }
  const common::ObObj &get_net_read_timeout_obj() const { return get_obj_var(CACHED_INT_VAR_NET_READ_TIMEOUT); }
  const common::ObObj &get_net_write_timeout_obj() const  { return get_obj_var(CACHED_INT_VAR_NET_WRITE_TIMEOUT); }
  const common::ObObj &get_autocommit_obj() const { return get_obj_var(CACHED_INT_VAR_AUTOCOMMIT); }
  const common::ObObj &get_lower_case_table_names_obj() const { return get_obj_var(CACHED_INT_VAR_LOWER_CASE_TABLE_NAMES); }
  const common::ObObj &get_tx_read_only_obj() const { return get_obj_var(CACHED_INT_VAR_TX_READ_ONLY); }
  const common::ObObj &get_read_consistency_obj() const { return get_obj_var(CACHED_INT_VAR_READ_CONSISTENCY); }

  bool need_use_lower_case_names() const { return common::OB_ORIGIN_AND_SENSITIVE != get_int_var(CACHED_INT_VAR_LOWER_CASE_TABLE_NAMES);}
  ObSQLMode get_sql_mode() const { return cached_sql_mode_; }
  const common::ObString& get_tx_read_only_str() const { return tx_read_only_str_; }

  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  // int get_obj_var(const ObCachedVariableType type, common::ObObj &val);
  // int get_int_var(const ObCachedVariableType type, common::ObString &val);
  // int get_str_var(const ObCachedVariableType type, int64_t &val);
  // unsafe caller must make sure the type is the corresponding type
  const common::ObObj &get_obj_var(const ObCachedVariableType type) const;
  int64_t get_int_var(const ObCachedVariableType type) const;
  const common::ObString get_str_var(const ObCachedVariableType type) const;

  // casted type, for fast access
  ObSQLMode cached_sql_mode_;
  common::ObString tx_read_only_str_;

  common::ObObj cached_vars_[CACHED_VAR_MAX];
};

inline int ObCachedVariables::update_var(const common::ObString &name, const common::ObObj &obj)
{
  return update_var(get_type(name), obj);
}

inline const common::ObObj &ObCachedVariables::get_obj_var(const ObCachedVariableType type) const
{
  return cached_vars_[type];
}

inline int64_t ObCachedVariables::get_int_var(const ObCachedVariableType type) const
{
  return cached_vars_[type].get_int();
}

} // end of obutils
} // end of obproxy
} // end of oceanbase
#endif /* OBPROXY_UTILS_CACHED_VARIABLES_H */
