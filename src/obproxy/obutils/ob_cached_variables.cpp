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
#include "obutils/ob_cached_variables.h"
#include "lib/time/ob_hrtime.h"
#include "sql/session/ob_system_variable_alias.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

ObCachedVariables::ObCachedVariables()
{
  memset(cached_vars_, 0, sizeof(cached_vars_));
  cached_sql_mode_ = SMO_DEFAULT;
  tx_read_only_str_ = ObString::make_string("0");
}

// Keep the order with ObCachedVariableType
static const ObString type_names[CACHED_VAR_MAX + 1] = {
    ObString(OB_SV_QUERY_TIMEOUT),
    ObString(OB_SV_WAIT_TIMEOUT),
    ObString(OB_SV_NET_READ_TIMEOUT),
    ObString(OB_SV_NET_WRITE_TIMEOUT),
    ObString(OB_SV_TRX_TIMEOUT),
    ObString(OB_SV_AUTOCOMMIT),
    ObString(OB_SV_LOWER_CASE_TABLE_NAMES),
    ObString(OB_SV_TX_READ_ONLY),
    ObString(OB_SV_READ_CONSISTENCY),
    ObString(OB_SV_COLLATION_CONNECTION),
    ObString(OB_SV_NCHARACTER_SET_CONNECTION),
    ObString(OB_SV_ENABLE_TRANSMISSION_CHECKSUM),
    ObString("CACHED_VAR_MAX"),
};

ObCachedVariableType ObCachedVariables::get_type(const ObString &name)
{
  ObCachedVariableType ret_type = CACHED_VAR_MAX;
  bool found = false;
  for (int64_t i = 0; !found && i < CACHED_VAR_MAX; ++i) {
    if (0 == type_names[i].case_compare(name)) {
      ret_type = static_cast<ObCachedVariableType>(i);
      found = true;
    }
  }
  return ret_type;
}

ObString ObCachedVariables::get_name(const ObCachedVariableType type)
{
  int64_t index = static_cast<int64_t>(type);
  if (index < 0 || index > CACHED_VAR_MAX) {
    index = CACHED_VAR_MAX;
  }
  return type_names[index];
}

int ObCachedVariables::update_var(const ObCachedVariableType &type, const ObObj &obj)
{
  int ret = OB_SUCCESS;
  int64_t index = static_cast<int64_t>(type);
  switch (type) {
    // transform timeout(query,trx,wait,net_read,net_write) to ns
    case CACHED_INT_VAR_QUERY_TIMEOUT:
    case CACHED_INT_VAR_TRX_TIMEOUT: {
      cached_vars_[index].set_int(HRTIME_USECONDS(obj.get_int()));
      break;
    }

    case CACHED_INT_VAR_WAIT_TIMEOUT:
    case CACHED_INT_VAR_NET_READ_TIMEOUT:
    case CACHED_INT_VAR_NET_WRITE_TIMEOUT: {
      cached_vars_[index].set_int(HRTIME_SECONDS(obj.get_int()));
      break;
    }

    case CACHED_INT_VAR_TX_READ_ONLY: {
      cached_vars_[index] = obj;
      if (obj.get_int() != 0) {
        tx_read_only_str_ = ObString::make_string("1");
      } else {
        tx_read_only_str_ = ObString::make_string("0");
      }
      break;
    }

    case CACHED_INT_VAR_AUTOCOMMIT:
    case CACHED_INT_VAR_LOWER_CASE_TABLE_NAMES:
    case CACHED_INT_VAR_READ_CONSISTENCY:
    case CACHED_INT_VAR_COLLATION_CONNECTION:
    case CACHED_INT_VAR_NCHARACTER_SET_CONNECTION:
    case CACHED_INT_VAR_ENABLE_TRANSMISSION_CHECKSUM: {
      cached_vars_[index] = obj;
      break;
    }

    case CACHED_VAR_MAX:
      // not a cached variables, do nothing
      break;

    default:
      ret = OB_ERR_UNEXPECTED;
  }
  if (index < CACHED_VAR_MAX) {
    LOG_DEBUG("update cached value ", K(obj), K(index), K(cached_vars_[index]));
  }
  return ret;
}

int64_t ObCachedVariables::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  for (int64_t i = 0; i < CACHED_VAR_MAX; ++i) {
    BUF_PRINTO(cached_vars_[i]);
  }
  J_COMMA();
  J_KV(K_(cached_sql_mode),
       K_(tx_read_only_str));
  J_OBJ_END();
  return pos;
}

} // end of obutils
} // end of obproxy
} // end of oceanbase
