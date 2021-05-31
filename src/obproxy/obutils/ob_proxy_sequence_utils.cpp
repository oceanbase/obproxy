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
#include "obutils/ob_proxy_sequence_utils.h"
#include "proxy/mysqllib/ob_resultset_fetcher.h"
#include "utils/ob_proxy_utils.h"
#include "obutils/ob_proxy_config.h"

#include <sys/time.h>
#include <time.h>
using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
static const char *PROXY_SELECT_SEQUENCE_INFO_SQL =
  "SELECT CURRENT_TIMESTAMP(6) as now, value, min_value, max_value, step, gmt_create, gmt_modified "
  "FROM %.*s.%.*s WHERE name = '%.*s' LIMIT 1";

static const char* PROXY_INSERT_SEQUENCE_INFO_SQL =
  "INSERT INTO %.*s.%.*s(name, min_value, max_value, step, value, gmt_create, gmt_modified) "
  "VALUES('%.*s', '%ld','%ld', '%ld', '%ld', '%.*s', '%.*s')";

static const char* PROXY_UPDATE_SEQUENCE_INFO_SQL =
  "UPDATE %.*s.%.*s SET value = '%ld', gmt_modified = now() WHERE name = '%.*s' and value = '%ld'";

static const char *PROXY_SELECT_SEQUENCE_INFO_WITH_TNT_SQL =
  "SELECT CURRENT_TIMESTAMP(6) as now, value, min_value, max_value, step, gmt_create, gmt_modified "
  "FROM %.*s.%.*s WHERE name = '%.*s' and %.*s = '%.*s' LIMIT 1";

static const char* PROXY_INSERT_SEQUENCE_INFO_WITH_TNT_SQL =
  "INSERT INTO %.*s.%.*s(name, %.*s, min_value, max_value, step, value, gmt_create, gmt_modified) "
  "VALUES('%.*s', '%.*s', '%ld','%ld', '%ld', '%ld', '%.*s', '%.*s')";

static const char* PROXY_UPDATE_SEQUENCE_INFO_WITH_TNT_SQL =
  "UPDATE %.*s.%.*s SET value = '%ld', gmt_modified = now() WHERE name = '%.*s' and %.*s = '%.*s' and value = '%ld'";

static const ObString ODP_DEFAULT_SEQUENCE_TABLE_NAME("mesh_sequence");
static const ObString DBP_DEFAULT_SEQUENCE_TABLE_NAME("dbp_sequence");
const ObString& ObProxySequenceUtils::get_default_sequence_table_name()
{
  ObString dbp_runtime = ObString::make_string(OB_PROXY_DBP_RUNTIME_ENV);
  if (0 == dbp_runtime.case_compare(get_global_proxy_config().runtime_env.str())) {
    return DBP_DEFAULT_SEQUENCE_TABLE_NAME;
  } else {
    return ODP_DEFAULT_SEQUENCE_TABLE_NAME;
  }
}
int ObProxySequenceUtils::get_sequence_entry_sql(char*sql_buf, const int64_t buf_len,
    const ObString& database_name,
    const ObString& table_name,
    const ObString& seq_name,
    const ObString& tnt_id,
    const ObString& tnt_col)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", LITERAL_K(sql_buf), K(buf_len), K(seq_name), K(ret));
  } else {
    int64_t len = 0;
    if (tnt_id.empty()) {
      len = static_cast<int64_t> (snprintf(sql_buf, buf_len,
                                         PROXY_SELECT_SEQUENCE_INFO_SQL,
                                         database_name.length(), database_name.ptr(),
                                         table_name.length(), table_name.ptr(),
                                         seq_name.length(), seq_name.ptr()));
    } else {
      len = static_cast<int64_t> (snprintf(sql_buf, buf_len,
                                         PROXY_SELECT_SEQUENCE_INFO_WITH_TNT_SQL,
                                         database_name.length(), database_name.ptr(),
                                         table_name.length(), table_name.ptr(),
                                         seq_name.length(), seq_name.ptr(),
                                         tnt_col.length(), tnt_col.ptr(),
                                         tnt_id.length(), tnt_id.ptr()));
    }
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }
  return ret;
}
int ObProxySequenceUtils::insert_sequence_entry_sql(char*sql_buf, const int64_t buf_len,
    const common::ObString& database_name,
    const common::ObString& table_name,
    const common::ObString& seq_name,
    const common::ObString& tnt_id,
    const common::ObString& tnt_col,
    int64_t min_value,
    int64_t max_value,
    int64_t step,
    int64_t value,
    const common::ObString& now_time)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", LITERAL_K(sql_buf), K(buf_len), K(seq_name), K(ret));
  } else {
    int64_t len = 0;
    if (tnt_id.empty()){
      len = static_cast<int64_t> (snprintf(sql_buf, buf_len,
                                         PROXY_INSERT_SEQUENCE_INFO_SQL,
                                         database_name.length(), database_name.ptr(),
                                         table_name.length(), table_name.ptr(),
                                         seq_name.length(), seq_name.ptr(),
                                         min_value, max_value,
                                         step, value,
                                         now_time.length(), now_time.ptr(),
                                         now_time.length(), now_time.ptr()));
    } else {
      len = static_cast<int64_t> (snprintf(sql_buf, buf_len,
                                         PROXY_INSERT_SEQUENCE_INFO_WITH_TNT_SQL,
                                         database_name.length(), database_name.ptr(),
                                         table_name.length(), table_name.ptr(),
                                         tnt_col.length(), tnt_col.ptr(),
                                         seq_name.length(), seq_name.ptr(),
                                         tnt_id.length(), tnt_id.ptr(),
                                         min_value, max_value,
                                         step, value,
                                         now_time.length(), now_time.ptr(),
                                         now_time.length(), now_time.ptr()));
    }
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }
  return ret;
}
int ObProxySequenceUtils::update_sequence_entry_sql(char*sql_buf,
    const int64_t buf_len,
    const common::ObString& database_name,
    const common::ObString& table_name,
    const common::ObString& seq_name,
    const common::ObString& tnt_id,
    const common::ObString& tnt_col,
    int64_t new_value, int64_t old_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", LITERAL_K(sql_buf), K(buf_len), K(seq_name), K(ret));
  } else {
    int64_t len = 0;
    if (tnt_id.empty()) {
      len = static_cast<int64_t> (snprintf(sql_buf, buf_len,
                                         PROXY_UPDATE_SEQUENCE_INFO_SQL,
                                         database_name.length(), database_name.ptr(),
                                         table_name.length(), table_name.ptr(),
                                         new_value,
                                         seq_name.length(), seq_name.ptr(),
                                         old_value));
    } else {
      len = static_cast<int64_t> (snprintf(sql_buf, buf_len,
                                         PROXY_UPDATE_SEQUENCE_INFO_WITH_TNT_SQL,
                                         database_name.length(), database_name.ptr(),
                                         table_name.length(), table_name.ptr(),
                                         new_value,
                                         seq_name.length(), seq_name.ptr(),
                                         tnt_col.length(), tnt_col.ptr(),
                                         tnt_id.length(), tnt_id.ptr(),
                                         old_value));
    }
    
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }
  return ret;
}

int ObProxySequenceUtils::get_nowtime_string(char* now_time_buf, int buf_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTimeUtility::usec_to_str(ObTimeUtility::current_time(), now_time_buf, (int64_t)buf_size, pos);
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbse
