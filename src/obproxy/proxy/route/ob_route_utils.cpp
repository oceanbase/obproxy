/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "proxy/route/ob_route_utils.h"
#include "proxy/mysqllib/ob_resultset_fetcher.h"
#include "proxy/route/ob_table_processor.h"
#include "proxy/route/ob_partition_processor.h"
#include "proxy/route/ob_routine_processor.h"
#include "proxy/route/ob_mysql_route.h"
#include "proxy/route/ob_partition_entry.h"
#include "proxy/route/ob_index_entry.h"
#include "proxy/route/ob_table_cache.h"
#include "proxy/route/ob_table_entry.h"
#include "proxy/route/ob_routine_cache.h"
#include "proxy/route/ob_routine_entry.h"
#include "proxy/route/ob_tenant_server.h"
#include "proxy/rpc/rpclib/ob_tablegroup_entry.h"
#include "proxy/rpc/rpclib/ob_tablet_ls_entry.h"
#include "opsql/expr_resolver/ob_expr_resolver.h"
#include "opsql/func_expr_parser/ob_func_expr_parser.h"
#include "opsql/func_expr_parser/ob_func_expr_parser_utils.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "utils/ob_proxy_utils.h"
#include "opsql/func_expr_resolver/ob_func_expr_resolver.h"
#include "lib/utility/ob_ls_id.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

static const char PART_KEY_EXTRA_SEPARATOR = ';';

//Not to use any more
// static const char *PROXY_PLAIN_SCHEMA_SQL =
//     //svr_ip, sql_port, table_id, role, part_num, replica_num, spare1
//     "SELECT /*+READ_CONSISTENCY(WEAK)%s*/ * "
//     "FROM oceanbase.%s "
//     "WHERE tenant_name = '%.*s' AND database_name = '%.*s' AND table_name = '%.*s' "
//     "AND %s = %ld "
//     "ORDER BY role ASC LIMIT %ld";
//RPC serivce
static const char *PROXY_PLAIN_SCHEMA_SQL_RPC =
    //svr_ip, sql_port, table_id, role, part_num, replica_num, spare1, svr_port
    "SELECT /*+READ_CONSISTENCY(WEAK)%s*/ A.*, B.svr_port as svr_port "
    "FROM oceanbase.%s A inner join oceanbase.%s B "
    "ON A.svr_ip = B.svr_ip and A.sql_port = B.inner_port "
    "WHERE A.tenant_name = '%.*s' AND A.database_name = '%.*s' AND A.table_name = '%.*s' "
    "AND A.partition_id = %ld "
    "ORDER BY A.role ASC LIMIT %ld";

static const char *PROXY_PLAIN_SCHEMA_SQL_RPC_V4 =
    //svr_ip, sql_port, table_id, role, part_num, replica_num, spare1, svr_port
    "SELECT /*+READ_CONSISTENCY(WEAK)%s*/ A.*, IFNULL(B.svr_port, 0) as svr_port, C.tenant_id as tenant_id "
    "FROM oceanbase.%s A left join oceanbase.%s B "
    "ON A.svr_ip = B.svr_ip and A.sql_port = B.sql_port "
    "left join oceanbase.%s C on A.tenant_name = C.tenant_name "
    "WHERE A.tenant_name = '%.*s' AND A.database_name = '%.*s' AND A.table_name = '%.*s' "
    "AND A.tablet_id = %ld "
    "ORDER BY A.role ASC LIMIT %ld";

static const char *PROXY_PLAIN_SCHEMA_SQL_RPC_AFTER_OB_4352 =
    // svr_ip, sql_port, table_id, role, part_num, replica_num, spare1, svr_port, tenant_id
    "SELECT /*+READ_CONSISTENCY(WEAK)%s*/ A.*, B.tenant_id as tenant_id "
    "FROM oceanbase.%s A left join oceanbase.%s B "
    "on A.tenant_name = B.tenant_name "
    "WHERE A.tenant_name = '%.*s' AND A.database_name = '%.*s' AND A.table_name = '%.*s' "
    "AND A.tablet_id = %ld "
    "ORDER BY A.role ASC LIMIT %ld";

static const char *PROXY_PLAIN_SCHEMA_SQL_RPC_BATCH =
    //svr_ip, sql_port, table_id, role, part_num, replica_num, spare1, svr_port
    "SELECT /*+READ_CONSISTENCY(WEAK)%s*/ A.*, B.svr_port as svr_port "
    "FROM oceanbase.%s A inner join oceanbase.%s B "
    "ON A.svr_ip = B.svr_ip and A.sql_port = B.inner_port "
    "WHERE A.tenant_name = '%.*s' AND A.database_name = '%.*s' AND A.table_name = '%.*s' "
    "AND A.partition_id IN (%.*s) "
    "ORDER BY A.role ASC LIMIT %ld";

static const char *PROXY_PLAIN_SCHEMA_SQL_RPC_V4_BATCH =
    //svr_ip, sql_port, table_id, role, part_num, replica_num, spare1, svr_port
    "SELECT /*+READ_CONSISTENCY(WEAK)%s*/ A.*, IFNULL(B.svr_port, 0) as svr_port "
    "FROM oceanbase.%s A left join oceanbase.%s B "
    "ON A.svr_ip = B.svr_ip and A.sql_port = B.sql_port "
    "WHERE A.tenant_name = '%.*s' AND A.database_name = '%.*s' AND A.table_name = '%.*s' "
    "AND A.tablet_id IN (%.*s) "
    "ORDER BY A.tablet_id, A.role ASC LIMIT %ld";

static const char *PROXY_PLAIN_SCHEMA_SQL_RPC_AFTER_OB_4352_BATCH =
    "SELECT /*+READ_CONSISTENCY(WEAK)%s*/ * "
    "FROM oceanbase.%s A "
    "WHERE A.tenant_name = '%.*s' AND A.database_name = '%.*s' AND A.table_name = '%.*s' "
    "AND A.tablet_id IN (%.*s) "
    "ORDER BY A.tablet_id, A.role ASC LIMIT %ld";

// Not to use any more
// static const char *PROXY_TENANT_SCHEMA_SQL =
//     //svr_ip, sql_port, table_id, role, part_num, replica_num
//     "SELECT /*+READ_CONSISTENCY(WEAK)*/ * "
//     "FROM oceanbase.%s "
//     "WHERE tenant_name = '%.*s' AND database_name = '%.*s' AND table_name = '%.*s' AND sql_port > 0 "
//     "ORDER BY %s ASC, role ASC LIMIT %ld";

//RPC Service
static const char *PROXY_TENANT_SCHEMA_SQL_RPC =
    //svr_ip, sql_port, table_id, role, part_num, replica_num, svr_port
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ A.*, B.svr_port as svr_port "
    "FROM oceanbase.%s A inner join oceanbase.%s B "
    "ON A.svr_ip = B.svr_ip and A.sql_port = B.inner_port "
    "WHERE A.tenant_name = '%.*s' AND A.database_name = '%.*s' AND A.table_name = '%.*s' AND A.sql_port > 0 "
    "ORDER BY A.partition_id ASC, role ASC LIMIT %ld";

static const char *PROXY_TENANT_SCHEMA_SQL_RPC_V4 =
    //svr_ip, sql_port, table_id, role, part_num, replica_num, svr_port
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ A.*, B.svr_port as svr_port, C.tenant_id as tenant_id "
    "FROM oceanbase.%s A inner join oceanbase.%s B "
    "ON A.svr_ip = B.svr_ip and A.sql_port = B.sql_port "
    "left join oceanbase.%s C on A.tenant_name = C.tenant_name "
    "WHERE A.tenant_name = '%.*s' AND A.database_name = '%.*s' AND A.table_name = '%.*s' AND A.sql_port > 0 "
    "ORDER BY A.tablet_id ASC, role ASC LIMIT %ld";

static const char *PROXY_TENANT_SCHEMA_SQL_RPC_AFTER_OB_4352 =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ A.*, B.tenant_id as tenant_id "
    "FROM oceanbase.%s A left join oceanbase.%s B on A.tenant_name = B.tenant_name "
    "WHERE A.tenant_name = '%.*s' AND A.database_name = '%.*s' AND A.table_name = '%.*s' AND A.sql_port > 0 "
    "ORDER BY A.tablet_id ASC, role ASC LIMIT %ld";

static const char *PROXY_PART_INFO_SQL =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ * "
    "FROM oceanbase.%s "
    "WHERE table_id = %lu order by part_key_idx LIMIT %d;";

static const char *PROXY_PART_INFO_SQL_V4 =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ * "
    "FROM oceanbase.%s "
    "WHERE table_id = %lu and tenant_name = '%.*s' order by part_key_idx LIMIT %d;";

/* For first part info from observer
 *  1. PROXY_FIRST_PART_SQL/PROXY_HASH_FIRST_PART_SQL: < ObServer-4.0( hash_part or not)
 *  2. PROXY_FIRST_PART_SQL_V4: >= ObServer-4.0.0 & < ObServer-4.3.5.2
 *  3. PROXY_FIRST_PART_SQL_AFTER_OB_4352: >= ObServer-4.3.5.2
 */
static const char *PROXY_FIRST_PART_SQL =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_id, part_name, high_bound_val_bin, high_bound_val, sub_part_num "
    "FROM oceanbase.%s "
    "WHERE table_id = %lu LIMIT %ld;";

// observer 2.1.1不兼容high_bound_val_bin, 所以分成2条sql
static const char *PROXY_HASH_FIRST_PART_SQL =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_id, part_name, sub_part_num "
    "FROM oceanbase.%s "
    "WHERE table_id = %lu LIMIT %ld;";

static const char *PROXY_FIRST_PART_SQL_V4 =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/  tablet_id, part_id, part_name, high_bound_val_bin, high_bound_val, sub_part_num "
    "FROM oceanbase.%s "
    "WHERE table_id = %lu AND tenant_name = '%.*s' LIMIT %ld;";

//add `spares` to get schema_version for FIRST_PART_SQL since observer-4.3.5.2(bp2)
static const char *PROXY_FIRST_PART_SQL_AFTER_OB_4352 =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ tablet_id, part_id, part_name, high_bound_val_bin, high_bound_val, sub_part_num, spare1 "
    "FROM oceanbase.%s "
    "WHERE table_id = %lu and tenant_name = '%.*s' LIMIT %ld;";

/* For sub part info from observer
 *  1. PROXY_SUB_PART_SQL/PROXY_NON_TEMPLATE_SUB_PART_SQL: < ObServer-4.0(template table or not)
 *  2. PROXY_SUB_PART_SQL_V4: >= ObServer-4.0.0 & < ObServer-4.3.5.2
 *  3. PROXY_SUB_PART_SQL_AFTER_OB_4352: >= ObServer-4.3.5.2
 */
static const char *PROXY_SUB_PART_SQL =
  "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_id, sub_part_id, part_name, high_bound_val_bin, high_bound_val "
  "FROM oceanbase.%s "
  "WHERE table_id = %lu and part_id = %ld LIMIT %ld;";

static const char *PROXY_NON_TEMPLATE_SUB_PART_SQL =
  "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_id, sub_part_id, part_name, high_bound_val_bin , high_bound_val "
  "FROM oceanbase.%s "
  "WHERE table_id = %lu LIMIT %ld;";

static const char *PROXY_SUB_PART_SQL_V4 =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ tablet_id, part_id, sub_part_id, part_name, high_bound_val_bin, high_bound_val "
    "FROM oceanbase.%s "
    "WHERE table_id = %lu AND tenant_name = '%.*s' LIMIT %ld;";

//add `spares` to get schema_version for SUB_PART_SQL since observer-4.3.5.2(bp2)
static const char *PROXY_SUB_PART_SQL_AFTER_OB_4352 =
    // tablet_id, sub_part_id, part_name, high_bound_val_bin, schema_version
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ tablet_id, part_id, sub_part_id, part_name, high_bound_val_bin, high_bound_val, spare1 "
    "FROM oceanbase.%s "
    "WHERE table_id = %lu and tenant_name = '%.*s' LIMIT %ld;";

static const char *PROXY_ROUTINE_SCHEMA_SQL =
  "SELECT /*+READ_CONSISTENCY(WEAK)*/ * "
  "FROM oceanbase.%s "
  "WHERE tenant_name = '%.*s' AND database_name = '%.*s' AND table_name = '%.*s' "
  "AND partition_id = 0 AND svr_ip = '' AND sql_port = 0 "
  "ORDER BY table_id ASC LIMIT 1;";

static const char *PROXY_ROUTINE_SCHEMA_SQL_WITH_PACKAGE =
  "SELECT /*+READ_CONSISTENCY(WEAK)*/ * "
  "FROM oceanbase.%s "
  "WHERE tenant_name = '%.*s' AND (database_name = '%.*s' OR database_name = '%.*s') AND table_name = '%.*s.%.*s' "
  "AND partition_id = 0 AND svr_ip = '' AND sql_port = 0 "
  "ORDER BY table_id ASC LIMIT 1;";

static const char *PROXY_ROUTINE_SCHEMA_SQL_V4 =
  "SELECT /*+READ_CONSISTENCY(WEAK)*/ * "
  "FROM %s "
  "WHERE tenant_name = '%.*s' and database_name = '%.*s' and package_name = '%.*s' "
  "and routine_name = '%.*s';";

static const char *PROXY_BINLOG_ADDR_SQL =
  "show binlog server for tenant `%.*s`.`%.*s`";

static const char *PROXY_TABLEGROUP_TABLES_SQL_V4 =
  "SELECT /*+READ_CONSISTENCY(WEAK)*/ * "
  "FROM %s "
  "WHERE tenant_id = '%lu' and tablegroup_name = '%.*s' and owner = '%.*s' "
  "LIMIT %ld;";

  static const char *PROXY_TABLET_LS_TABLES_SQL_V4 =
  "SELECT /*+READ_CONSISTENCY(WEAK)*/ tenant_id, tablet_id, ls_id, table_id "
  "FROM %s "
  "WHERE tenant_id = '%lu' and table_id = '%lu' "
  "LIMIT %ld;";

  static const char *PROXY_CDC_MSGSERVICE_ADDR_SQL =
    "SHOW CDC SERVER FOR CHANNEL %lu, STREAM '%s'";

static void get_tenant_name(const ObString &origin_tenant_name, char *new_tenant_name_buf, ObString &new_tenant_name) {
  // All callers size new_tenant_name_buf to OB_MAX_TENANT_NAME_LENGTH * 2 + 1.
  // Reject overlong input to prevent stack buffer overflow on attacker-controlled tenant_name.
  // Callers should already gate on ObTableEntryName::is_valid(); this is defense in depth.
  if (OB_UNLIKELY(origin_tenant_name.length() > OB_MAX_TENANT_NAME_LENGTH)) {
    new_tenant_name = origin_tenant_name;
    LOG_WDIAG("tenant name length exceeds limit, skip escape",
              "length", origin_tenant_name.length(),
              "limit", OB_MAX_TENANT_NAME_LENGTH);
  } else {
    new_tenant_name = origin_tenant_name;
    int32_t pos = 0;
    for (int64_t i = 0; i < origin_tenant_name.length(); i++) {
      if (origin_tenant_name[i] == '\'') {
        new_tenant_name_buf[pos++] = '\'';
        new_tenant_name_buf[pos++] = '\'';
      } else if (origin_tenant_name[i] == '\\') {
        new_tenant_name_buf[pos++] = '\\';
        new_tenant_name_buf[pos++] = '\\';
      } else {
        new_tenant_name_buf[pos++] = origin_tenant_name[i];
      }
    }
    // Buffer is sized [OB_MAX_TENANT_NAME_LENGTH * 2 + 1]; pos <= length*2 <= buf_size - 1.
    new_tenant_name_buf[pos] = '\0';

    if (pos != origin_tenant_name.length()) {
      new_tenant_name.assign_ptr(new_tenant_name_buf, pos);
    }
  }
}

int ObRouteUtils::get_table_entry_sql(char *sql_buf, const int64_t buf_len,
                                      ObTableEntryName &name,
                                      bool is_need_force_flush, /*false*/
                                      const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(!name.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", LITERAL_K(sql_buf), K(buf_len), K(name), K(ret));
  } else {
    int64_t len = 0;
    char new_tenant_name_buf[OB_MAX_TENANT_NAME_LENGTH * 2 + 1];
    ObString new_tenant_name;
    get_tenant_name(name.tenant_name_, new_tenant_name_buf, new_tenant_name);

    //contains RPC service in fetch sql
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
      if (name.is_all_dummy_table()) {
        len = static_cast<int64_t>(snprintf(sql_buf, buf_len, PROXY_TENANT_SCHEMA_SQL_RPC,
                                            OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                            OB_ALL_VIRTUAL_SERVER_STAT_TNAME,
                                            new_tenant_name.length(), new_tenant_name.ptr(),
                                            name.database_name_.length(), name.database_name_.ptr(),
                                            name.table_name_.length(), name.table_name_.ptr(),
                                            INT64_MAX));
      } else {
        const int64_t FIRST_PARTITION_ID = 0;
        len = static_cast<int64_t>(snprintf(sql_buf, buf_len, PROXY_PLAIN_SCHEMA_SQL_RPC,
                                            is_need_force_flush ? ", FORCE_REFRESH_LOCATION_CACHE" : "",
                                            OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                            OB_ALL_VIRTUAL_SERVER_STAT_TNAME,
                                            new_tenant_name.length(), new_tenant_name.ptr(),
                                            name.database_name_.length(), name.database_name_.ptr(),
                                            name.table_name_.length(), name.table_name_.ptr(),
                                            FIRST_PARTITION_ID, INT64_MAX));
      }
    } else if (IS_CLUSTER_VERSION_BEFORE_4_3_5_2(cluster_version)) {
      if (name.is_all_dummy_table()) {
        len = static_cast<int64_t>(snprintf(sql_buf, buf_len, PROXY_TENANT_SCHEMA_SQL_RPC_V4,
                                            OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                            DBA_OB_SERVERS_VNAME,
                                            DBA_OB_TENANTS_VNAME,
                                            new_tenant_name.length(), new_tenant_name.ptr(),
                                            name.database_name_.length(), name.database_name_.ptr(),
                                            name.table_name_.length(), name.table_name_.ptr(),
                                            INT64_MAX));
      } else {
        const int64_t FIRST_PARTITION_ID = 0;
        len = static_cast<int64_t>(snprintf(sql_buf, buf_len, PROXY_PLAIN_SCHEMA_SQL_RPC_V4,
                                            is_need_force_flush ? ", FORCE_REFRESH_LOCATION_CACHE" : "",
                                            OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                            DBA_OB_SERVERS_VNAME,
                                            DBA_OB_TENANTS_VNAME,
                                            new_tenant_name.length(), new_tenant_name.ptr(),
                                            name.database_name_.length(), name.database_name_.ptr(),
                                            name.table_name_.length(), name.table_name_.ptr(),
                                            FIRST_PARTITION_ID, INT64_MAX));
      }
    } else {
      if (name.is_all_dummy_table()) {
        len = static_cast<int64_t>(snprintf(sql_buf, buf_len, PROXY_TENANT_SCHEMA_SQL_RPC_AFTER_OB_4352,
                                            OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                            DBA_OB_TENANTS_VNAME,
                                            new_tenant_name.length(), new_tenant_name.ptr(),
                                            name.database_name_.length(), name.database_name_.ptr(),
                                            name.table_name_.length(), name.table_name_.ptr(),
                                            INT64_MAX));
      } else {
        const int64_t FIRST_PARTITION_ID = 0;
        len = static_cast<int64_t>(snprintf(sql_buf, buf_len, PROXY_PLAIN_SCHEMA_SQL_RPC_AFTER_OB_4352,
                                            is_need_force_flush ? ", FORCE_REFRESH_LOCATION_CACHE" : "",
                                            OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                            DBA_OB_TENANTS_VNAME,
                                            new_tenant_name.length(), new_tenant_name.ptr(),
                                            name.database_name_.length(), name.database_name_.ptr(),
                                            name.table_name_.length(), name.table_name_.ptr(),
                                            FIRST_PARTITION_ID, INT64_MAX));
      }
    }

    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }

  return ret;
}

int ObRouteUtils::get_cdc_msgservice_addr_sql(char *sql_buf, const int64_t buf_len,
                                              ObTableRouteParam &param)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", LITERAL_K(sql_buf), K(buf_len), K(ret));
  } else {
    len = static_cast<int64_t>(snprintf(sql_buf, buf_len, PROXY_CDC_MSGSERVICE_ADDR_SQL,
                                        param.cdc_msgservice_param_.get_channel_id(),
                                        param.cdc_msgservice_param_.get_stream_name().ptr()));
  }

  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
  }

  return ret;
}
int ObRouteUtils::get_part_info_sql(char *sql_buf,
                                    const int64_t buf_len,
                                    const uint64_t table_id,
                                    ObTableEntryName &name,
                                    const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(!name.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", LITERAL_K(sql_buf), K(buf_len), K(name), K(ret));
  } else {
    int64_t len = 0;
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
      len = snprintf(sql_buf, buf_len, PROXY_PART_INFO_SQL,
                     OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TNAME,
                     table_id,
                     OB_MAX_PARTITION_KEY_COLUMN_NUMBER);
    } else {
      char new_tenant_name_buf[OB_MAX_TENANT_NAME_LENGTH * 2 + 1];
      ObString new_tenant_name;
      get_tenant_name(name.tenant_name_, new_tenant_name_buf, new_tenant_name);
      len = snprintf(sql_buf, buf_len, PROXY_PART_INFO_SQL_V4,
                     OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TNAME,
                     table_id,
                     new_tenant_name.length(), new_tenant_name.ptr(),
                     OB_MAX_PARTITION_KEY_COLUMN_NUMBER);
    }
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }

  return ret;
}

int ObRouteUtils::get_first_part_sql(char *sql_buf,
                                     const int64_t buf_len,
                                     const uint64_t table_id,
                                     const bool is_hash_part,
                                     ObTableEntryName &name,
                                     const int64_t cluster_version)
{
  int ret = OB_SUCCESS;

  int64_t len = 0;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(!name.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", LITERAL_K(sql_buf), K(buf_len), K(name), K(ret));
  } else if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
    len = snprintf(sql_buf, buf_len, is_hash_part ? PROXY_HASH_FIRST_PART_SQL : PROXY_FIRST_PART_SQL,
        OB_ALL_VIRTUAL_PROXY_PARTITION_TNAME,
        table_id,
        INT64_MAX);
  } else if (IS_CLUSTER_VERSION_BEFORE_4_3_5_2(cluster_version)) {
    char new_tenant_name_buf[OB_MAX_TENANT_NAME_LENGTH * 2 + 1];
    ObString new_tenant_name;
    get_tenant_name(name.tenant_name_, new_tenant_name_buf, new_tenant_name);
    len = snprintf(sql_buf, buf_len, PROXY_FIRST_PART_SQL_V4,
                   OB_ALL_VIRTUAL_PROXY_PARTITION_TNAME,
                   table_id,
                   new_tenant_name.length(), new_tenant_name.ptr(),
                   INT64_MAX);
  } else {
    char new_tenant_name_buf[OB_MAX_TENANT_NAME_LENGTH * 2 + 1];
    ObString new_tenant_name;
    get_tenant_name(name.tenant_name_, new_tenant_name_buf, new_tenant_name);
    len = snprintf(sql_buf, buf_len, PROXY_FIRST_PART_SQL_AFTER_OB_4352,
                   OB_ALL_VIRTUAL_PROXY_PARTITION_TNAME,
                   table_id,
                   new_tenant_name.length(), new_tenant_name.ptr(),
                   INT64_MAX);
  }
  if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
  }

  return ret;
}

int ObRouteUtils::get_sub_part_sql(char *sql_buf,
                                   const int64_t buf_len,
                                   const uint64_t table_id,
                                   const bool is_template_table,
                                   ObTableEntryName &name,
                                   const int64_t cluster_version)
{
  int ret = OB_SUCCESS;

  int64_t len = 0;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(!name.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", LITERAL_K(sql_buf), K(buf_len), K(name), K(ret));
  } else {
    // assume all sub_part are same for each fisrt part
    // templete part id is -1
    if (!IS_CLUSTER_VERSION_BEFORE_4_3_5_2(cluster_version)) {
      char new_tenant_name_buf[OB_MAX_TENANT_NAME_LENGTH * 2 + 1];
      ObString new_tenant_name;
      get_tenant_name(name.tenant_name_, new_tenant_name_buf, new_tenant_name);
      len = snprintf(sql_buf, buf_len, PROXY_SUB_PART_SQL_AFTER_OB_4352,
                    OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TNAME,
                    table_id,
                    new_tenant_name.length(), new_tenant_name.ptr(),
                    INT64_MAX);
    } else if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
      char new_tenant_name_buf[OB_MAX_TENANT_NAME_LENGTH * 2 + 1];
      ObString new_tenant_name;
      get_tenant_name(name.tenant_name_, new_tenant_name_buf, new_tenant_name);
      len = snprintf(sql_buf, buf_len, PROXY_SUB_PART_SQL_V4,
                    OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TNAME,
                    table_id,
                    new_tenant_name.length(), new_tenant_name.ptr(),
                    INT64_MAX);
    } else if (is_template_table) {
      const int64_t TEMPLATE_PART_ID = -1;
      len = snprintf(sql_buf, buf_len, PROXY_SUB_PART_SQL,
                     OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TNAME,
                     table_id,
                     TEMPLATE_PART_ID,
                     INT64_MAX);
    } else {
      len = snprintf(sql_buf, buf_len, PROXY_NON_TEMPLATE_SUB_PART_SQL,
                     OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TNAME,
                     table_id,
                     INT64_MAX);
    }

    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }

  return ret;
}

int ObRouteUtils::get_binlog_entry_sql(char *sql_buf,
                                       const int64_t buf_len,
                                       const ObString &cluster_name,
                                       const ObString &tenant_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0)
      || OB_UNLIKELY(cluster_name.empty())
      || OB_UNLIKELY(tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", LITERAL_K(sql_buf), K(buf_len), K(cluster_name), K(tenant_name), K(ret));
  } else {
    int64_t len = 0;
    len = static_cast<int64_t>(snprintf(sql_buf, buf_len, PROXY_BINLOG_ADDR_SQL,
                                        cluster_name.length(), cluster_name.ptr(),
                                        tenant_name.length(), tenant_name.ptr()));

    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to fill binlog sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }

  return ret;
}

bool is_fake_ip_port(const char *ip_str, const int64_t port)
{
  const ObString fake_ip("0.0.0.0");
  const int64_t fake_port = 0;
  return (NULL != ip_str && 0 == fake_ip.compare(ip_str) && fake_port == port);
}

int ObRouteUtils::fetch_table_entry(ObResultSetFetcher &rs_fetcher,
                                    ObTableEntry &entry,
                                    const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0; // 仅用于填充出参，不起作用，需保证对应的字符串中间没有'\0'字符
  char ip_str[MAX_IP_ADDR_LENGTH];
  ip_str[0] = '\0';
  int64_t port = 0;
  int64_t svr_port = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  uint64_t table_id = OB_INVALID_ID;
  int64_t part_num = 0;
  int64_t replica_num = 0;
  int64_t schema_version = 0;
  int64_t role = -1;
  int32_t replica_type = -1;
  int32_t dup_replica_type = 0;
  int32_t table_type = -1;
  ObProxyReplicaLocation prl;
  ObProxyPartitionLocation *ppl = NULL;
  ObTenantServer *pts = NULL;
  ObSEArray<ObProxyReplicaLocation, 32> server_list;
  const bool is_dummy_entry = entry.is_dummy_entry();
  bool use_fake_addrs = false;
  bool has_dup_replica = false;
  int64_t complex_table_type = 0;
  char level1_decoded_db_name[OB_MAX_DATABASE_NAME_LENGTH];
  level1_decoded_db_name[0] = '\0';
  int64_t decoded_db_str_len = 0;

  while ((OB_SUCC(ret)) && (OB_SUCC(rs_fetcher.next()))) {
    ip_str[0] = '\0';
    port = 0;
    prl.reset();
    role = -1;
    replica_type = -1;
    table_type = -1;
    svr_port = 0;
    decoded_db_str_len = 0;

    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "svr_ip", ip_str, MAX_IP_ADDR_LENGTH, tmp_real_str_len);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sql_port", port, int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "table_id", table_id, uint64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "role", role, int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_num", part_num, int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "replica_num", replica_num, int64_t);

    if (OB_SUCC(ret)) {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "schema_version", schema_version, int64_t);
      if (OB_ERR_COLUMN_NOT_FOUND == ret) {
        LOG_DEBUG("can not found schema_version, maybe is old server, ignore", K(ret));
        ret = OB_SUCCESS;
        schema_version = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare1", replica_type, int32_t);
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "replica_type", replica_type, int32_t);
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "tenant_id", tenant_id, uint64_t);
        if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
          // don`t stem the construction of table entry
          // it just potentially result in route problems for part table
          LOG_WDIAG("observer return invalid tenant_id", K(tenant_id), K(table_id), K(ret));
        }
      }
      if (OB_ERR_COLUMN_NOT_FOUND == ret) {
        LOG_DEBUG("can not find spare1, maybe is old server, ignore", K(replica_type), K(ret));
        ret = OB_SUCCESS;
        replica_type = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare2", dup_replica_type, int32_t);
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "dup_replica_type", dup_replica_type, int32_t);
      }
      if (OB_ERR_COLUMN_NOT_FOUND == ret) {
        LOG_DEBUG("can not find spare2, maybe is old server, ignore", K(dup_replica_type), K(ret));
        ret = OB_SUCCESS;
        dup_replica_type = 0;
      }
    }

    if (OB_SUCC(ret)) {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "table_type", table_type, int32_t);
      if (OB_ERR_COLUMN_NOT_FOUND == ret) {
        LOG_DEBUG("can not found table_type, maybe is old server, ignore", K(ret));
        ret = OB_SUCCESS;
        table_type = -1;
      }
    }

    if (OB_SUCC(ret)) {
      if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
        // nothing
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "complex_table_type", complex_table_type, int64_t);
        PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "level1_decoded_db_name", level1_decoded_db_name, OB_MAX_DATABASE_NAME_LENGTH, decoded_db_str_len);
      }

      if (OB_UNLIKELY(CT_SYNONYM == complex_table_type
                      && decoded_db_str_len > 0)
          && OB_FAIL(entry.set_level1_decoded_db_name(ObString(decoded_db_str_len, level1_decoded_db_name)))) {
        LOG_WDIAG("fail to set level1_decoded_db_name", K(level1_decoded_db_name), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("RPC service mode need fetch srv_port");
      if (IS_CLUSTER_VERSION_BEFORE_4_3_5_2(cluster_version)) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "svr_port", svr_port, int64_t);
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare1", svr_port, int64_t);
      }
      if (OB_ERR_COLUMN_NOT_FOUND == ret) {
        LOG_DEBUG("can not found svr_port, maybe not in rpc service mode, ignore", K(ret));
        ret = OB_SUCCESS;
        svr_port = 0;
      }
    }

    if (OB_SUCC(ret)) {
      prl.role_ = static_cast<ObRole>(role);
      if (OB_FAIL(prl.add_addr(ip_str, port))) {
        if (is_fake_ip_port(ip_str, port)) {
          use_fake_addrs = true;
        } else {
          LOG_WDIAG("invalid ip, port in fetching table entry, just skip it,"
                   " do not return err", K(ip_str), K(port), K(ret));
        }
        ret = OB_SUCCESS;
      } else if (OB_UNLIKELY(LEADER != prl.role_) && OB_UNLIKELY(FOLLOWER != prl.role_)) {
        LOG_WDIAG("invalid role in fetching table entry, just skip it,"
                 " do not return err", "role", prl.role_);
        ret = OB_SUCCESS;
      } else if (OB_FAIL(prl.set_replica_type(replica_type))) {
        LOG_WDIAG("invalid replica_type in fetching table entry, just skip it,"
                 " do not return err", "replica_type", replica_type);
        ret = OB_SUCCESS;
      } else if (FALSE_IT(prl.set_dup_replica_type(dup_replica_type))) {
        // can not happen
      } else if (svr_port > 0 && OB_FAIL(prl.add_rpc_addr(ip_str, svr_port))) {
        if (is_fake_ip_port(ip_str, svr_port)) {
          use_fake_addrs = true;
        } else {
          LOG_WDIAG("invalid rpc ip, port in fetching table entry, just skip it,"
                   " do not return err", K(ip_str), K(svr_port), K(ret));
        }
        ret = OB_SUCCESS;
      } else if (OB_FAIL(server_list.push_back(prl))) {
        LOG_WDIAG("fail to add server", K(prl), K(ret));
      } else {
        if (prl.is_dup_replica() && !has_dup_replica) {
          has_dup_replica = true;
        }
      }//end of else
      //RPC Service add rpc servce port
      LOG_DEBUG("RPC service mode need fetch srv_port", K(svr_port));
      //if (OB_SUCC(ret) && svr_port > 0) {
      //  if (OB_FAIL(prl.add_rpc_addr(ip_str, svr_port))) {
      //    if (is_fake_ip_port(ip_str, svr_port)) {
      //      use_fake_addrs = true;
      //    } else {
      //      LOG_WDIAG("invalid rpc ip, port in fetching table entry, just skip it,"
      //               " do not return err", K(ip_str), K(svr_port), K(ret));
      //    }
      //    ret = OB_SUCCESS;
      //  }
      //}
    }//end of OB_SUCC(ret)
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret) && is_dummy_entry) {
    if (!server_list.empty()) {
      if (OB_ISNULL(pts = op_alloc(ObTenantServer))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to allocate memory for ObTenantServer", K(ret));
      } else if (OB_FAIL(pts->init(server_list))) {
        LOG_WDIAG("fail to init tenant servers", KPC(pts), K(server_list), K(ret));
      } else if (OB_UNLIKELY(!pts->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("pts should not unavailable", KPC(pts), K(ret));
      } else if (OB_FAIL(entry.set_tenant_servers(pts))) {
        LOG_WDIAG("fail to set tenant servers", KPC(pts), K(ret));
      } else {
        //must set it NULL as we had move pointer's value
        pts = NULL;
        entry.set_part_num(1);
        entry.set_replica_num(entry.get_tenant_servers()->replica_count());
        entry.set_table_id(table_id);
        entry.set_schema_version(schema_version);
        LOG_INFO("this is all_dummy table, use tenant servers", K(entry), K(server_list));
      }
    } else {
      LOG_INFO("can not find tenant servers, empty resultset", K(server_list));
    }
  }

  if (OB_SUCC(ret) && !is_dummy_entry) {
    if (!server_list.empty() || use_fake_addrs) {
      if (OB_ISNULL(ppl = op_alloc(ObProxyPartitionLocation))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to allocate memory for ObProxyPartitionLocation", K(ret));
      } else if (OB_FAIL(ppl->set_replicas(server_list))) {
        LOG_WDIAG("fail to set replicas", K(server_list), K(ret));
      } else if (!server_list.empty() && OB_UNLIKELY(!ppl->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("ppl should not unavailable", KPC(ppl), K(ret));
      } else {
        const bool is_empty_entry_allowed = (use_fake_addrs && server_list.empty());
        entry.set_allow_empty_entry(is_empty_entry_allowed);
        entry.set_part_num(part_num);
        entry.set_replica_num(replica_num);
        entry.set_schema_version(schema_version);
        entry.set_tenant_id(tenant_id);
        entry.set_table_id(table_id);
        entry.set_table_type(table_type);
        if (has_dup_replica) {
          entry.set_has_dup_replica();
        }
        if (entry.is_non_partition_table() && ppl->is_valid()) {
          if (OB_FAIL(entry.set_first_partition_location(ppl))) {
            LOG_WDIAG("fail to set first partition location", K(ret));
          } else {
            ppl = NULL; // set to NULL, if succ
          }
        }
      }
    } else {
      // maybe it is a empty ResultSet, do not ret error;
      LOG_INFO("can not find table location, empty resultset");
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != pts) {
      op_free(pts);
      pts = NULL;
    }
  }

  if (NULL != ppl) {
    op_free(ppl);
    ppl = NULL;
  }
  return ret;
}

int ObRouteUtils::split_part_expr(ObString expr, ObIArray<ObString> &arr)
{
  int ret = OB_SUCCESS;
  ObString tmp;
  while (OB_SUCC(ret) && !expr.empty()) {
    tmp = expr.split_on(',').trim();
    size_t i = 0;
    if (tmp.empty()) {
      tmp = expr.trim();
      expr.reset();
    }
    //TODO support for next, skip ' ','\n','\t' first,
    while(i < tmp.length() && (tmp[i] == ' ' || tmp[i] == '\n' || tmp[i] == '\t')) {
      i++;
    }
    tmp.assign_ptr(tmp.ptr() + i, tmp.length() - i);

    // Remove backticks (`) or double quotes (") from the beginning and end
    if (tmp[0] == '`' || tmp[0] == '"') {
      tmp.assign_ptr(tmp.ptr() + 1, tmp.length() - 1);
    }
    if (tmp[tmp.length() - 1] == '`' || tmp[tmp.length() - 1] == '"') {
      tmp.assign_ptr(tmp.ptr(), tmp.length() - 1);
    }
    if (OB_FAIL(arr.push_back(tmp))) {
      LOG_WDIAG("fail to push back", K(tmp), K(ret));
    } else {
      LOG_DEBUG("succ to push back part expr", K(tmp));
    }
  }
  return ret;
}


int ObRouteUtils::fetch_part_info(ObResultSetFetcher &rs_fetcher, ObProxyPartInfo &part_info, const int64_t cluster_version)
{
  int ret = OB_SUCCESS;

  ObPartitionLevel part_level = PARTITION_LEVEL_ONE;
  int64_t part_key_num = 1;
  int64_t template_num = 1;
  ObString part_range_type;
  ObString sub_part_range_type;
  ObString part_expr;
  ObString sub_part_expr;

  // init part key info
  part_info.get_part_key_info().key_num_ = 0;
  part_info.set_cluster_version(cluster_version);
  for (int64_t i = 0; i < part_key_num && OB_SUCC(ret); ++i) {
    // get first row
    if (OB_FAIL(rs_fetcher.next())) {
      LOG_WDIAG("fail to fetch part info", K(ret));
    } else {
      // get basic info ONLY for the first line
      if (0 == i) {
        // get part level
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "template_num", template_num, int64_t);
        if (template_num != 0 && template_num != 1) {
          ret = OB_INVALID_ARGUMENT_FOR_EXTRACT;
          LOG_WDIAG("part level is invalid", K(template_num), K(ret));
        } else {
          part_info.set_template_table(1 == template_num);
        }

        // get part level
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_level", part_level, ObPartitionLevel);
        if (part_level < PARTITION_LEVEL_ONE || part_level > PARTITION_LEVEL_TWO) {
          ret = OB_INVALID_ARGUMENT_FOR_EXTRACT;
          LOG_WDIAG("part level is invalid", K(part_level), K(ret));
        } else {
          part_info.set_part_level(part_level);
        }

        // get part key num
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_key_num", part_key_num, int64_t);
        if (part_key_num > OBPROXY_MAX_PART_KEY_NUM) {
          part_key_num = OBPROXY_MAX_PART_KEY_NUM;
        }

        // get part range type
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_range_type", part_range_type);
        char *buf = NULL;
        if (part_range_type.empty()) {
          LOG_DEBUG("part expression is empty");
        } else if (OB_ISNULL(buf = static_cast<char *>(part_info.get_allocator().alloc(part_range_type.length())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to allc part range type", K(buf), K(part_range_type.length()), K(ret));
        } else {
          memcpy(buf, part_range_type.ptr(), part_range_type.length());
          part_expr.assign_ptr(buf, part_range_type.length());
          part_info.set_part_range_type(part_expr);
        }

        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "sub_part_range_type", sub_part_range_type);
        buf = NULL;
        if (sub_part_range_type.empty()) {
          LOG_DEBUG("part expression is empty");
        } else if (OB_ISNULL(buf = static_cast<char *>(part_info.get_allocator().alloc(sub_part_range_type.length())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to allc part range type", K(buf), K(sub_part_range_type.length()), K(ret));
        } else {
          memcpy(buf, sub_part_range_type.ptr(), sub_part_range_type.length());
          part_expr.assign_ptr(buf, sub_part_range_type.length());
          part_info.set_sub_part_range_type(part_expr);
        }
        // get part expr
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_expr", part_expr);
        buf = NULL;
        if (part_expr.empty()) {
          LOG_DEBUG("part expression is empty");
        } else if (OB_ISNULL(buf = static_cast<char *>(part_info.get_allocator().alloc(part_expr.length())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to allc part key name", K(buf), K(part_expr.length()), K(ret));
        } else {
          memcpy(buf, part_expr.ptr(), part_expr.length());
          part_expr.assign_ptr(buf, part_expr.length());
          part_info.set_part_expr(part_expr);
        }

        // get sub part expr
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "sub_part_expr", sub_part_expr);
        if (sub_part_expr.empty()) {
          LOG_DEBUG("sub part expression is empty");
        } else if (OB_ISNULL(buf = static_cast<char *>(part_info.get_allocator().alloc(sub_part_expr.length())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to allc part key name", K(buf), K(sub_part_expr.length()), K(ret));
        } else {
          memcpy(buf, sub_part_expr.ptr(), sub_part_expr.length());
          sub_part_expr.assign_ptr(buf, sub_part_expr.length());
          part_info.set_sub_part_expr(sub_part_expr);
        }
        // split part expression
        if (part_info.get_part_level() >= PARTITION_LEVEL_ONE
            && OB_FAIL(split_part_expr(part_expr, part_info.get_first_part_columns()))) {
          LOG_WDIAG("fail to split part expr", K(ret));
        } else if (part_info.get_part_level() == PARTITION_LEVEL_TWO
                   && OB_FAIL(split_part_expr(sub_part_expr, part_info.get_sub_part_columns())) ) {
          LOG_WDIAG("fail to split sub part expr", K(ret));
        }

        if (OB_FAIL(fetch_part_option(rs_fetcher, part_info))) {
          LOG_WDIAG("fail to get part option", K(ret));
        } else {
          if (part_expr.empty()) {
            part_info.set_primary_key_as_part_expr(true);
          }
        }
      } else {
        // do nothing here
      }

      // get part key info for EACH line
      if (OB_SUCC(ret)) {
        if (OB_FAIL(fetch_part_key(rs_fetcher, part_info, cluster_version))) {
          LOG_WDIAG("fail to get part key", K(ret));
        }
      } // end of if (OB_SUCC(ret))

      // 分配新的列信息，最大16列
      if (OB_SUCC(ret) && OBPROXY_MAX_PART_KEY_NUM > part_info.get_part_key_info().key_num_) {
        ObProxyPartKeyInfo &part_key_info = part_info.get_part_key_info();
        ++part_key_info.key_num_;
      }
    } // end of else
  } // end of for
  if (OB_SUCC(ret)) {
    // handle generated key,  map the key for generated key calculation to real key
    for (int64_t i = 0; i < part_info.get_part_key_info().key_num_; ++i) {
      if (part_info.get_part_key_info().part_keys_[i].generated_col_idx_ >= 0) {
        for (int64_t j = 0; j < part_info.get_part_key_info().key_num_; ++j) {
          ObProxyParseString *l = &part_info.get_part_key_info().part_keys_[i].name_;
          ObProxyParseString *r = &part_info.get_part_key_info().part_keys_[j].name_;
          if (i != j && l->str_ != NULL
              && r->str_ != NULL
              && l->str_len_ == r->str_len_
              && 0 == strncasecmp(l->str_, r->str_, l->str_len_)) {
            part_info.get_part_key_info().part_keys_[i].real_source_idx_ = j;
          }
        }
      }
    }
  }

  return ret;
}

void ObRouteUtils::set_part_key_accuracy(ObProxyPartKey *part_key, ObObjType part_key_type,
                         const int32_t length, const int16_t precision, const int16_t scale)
{
  part_key->accuracy_.length_ = -1;         // init -1 means not used
  part_key->accuracy_.precision_ = -1;
  part_key->accuracy_.scale_ = -1;

  // use accord to obj_type
  if (ob_is_otimestamp_type(part_key_type)) {
    if (scale < MIN_SCALE_FOR_TEMPORAL || scale > MAX_SCALE_FOR_ORACLE_TEMPORAL) {
      part_key->accuracy_.scale_ = DEFAULT_SCALE_FOR_ORACLE_TIMESTAMP;
      LOG_WDIAG("invalid scale for timestamp in oracle, set to default:6", K(scale));
    } else {
      part_key->accuracy_.scale_ = static_cast<int16_t>(scale); // timestamp only need scale
      LOG_DEBUG("succ to set timestamp scale of accuracy", K(scale));
    }
  } else if (ob_is_number_tc(part_key_type)
      || ob_is_datetime_tc(part_key_type)
      || ob_is_time_tc(part_key_type)
      || ob_is_decimal_int_type(part_key_type)) {
    part_key->accuracy_.precision_ = precision;
    part_key->accuracy_.scale_ = scale;
    part_key->accuracy_.valid_ = 1;
  } else if (ob_is_string_tc(part_key_type)) {
    part_key->accuracy_.length_ = length;
    part_key->accuracy_.precision_ = precision;
    part_key->accuracy_.valid_ = 1;
  }
  // more obj type could be supported here.
}

/*
 * according to rs observer, spare5 will specify the accuracy of all the part key
 * set this if the part_key_type is valid, otherwise maybe invalid value
 * accuracy is different between diff types, for eg: "6" for TIMESTAMP, "(2, 5)" for NUMBER
 * see more info in ObProxyPartKeyAccuracy
 *
 * format: "length,precision/length_semantics,scale", type: "int32_t,int16_t,int16_t"
 * the guide of how to handle the accuracy of part key type is reffered to yuque.
 *
 * return: no ret for this function, cause we need to continue our part info build procedure,
 *         even if the accuracy parse failed or invalid value.
 */
void ObRouteUtils::parse_part_key_accuracy(ObProxyPartKey *part_key,
                                           ObObjType part_key_type,
                                           ObIAllocator *allocator,
                                           ObString &part_key_accuracy)
{
  int ret = OB_SUCCESS;

  if (part_key_accuracy.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_DEBUG("empty part key accuracy from rs observer");
  } else {
    // alloc buf to maintain string
    char *buf = NULL;
    if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(part_key_accuracy.length() + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc buf", K(ret), K(part_key_accuracy.length()));
    } else {
      memcpy(buf, part_key_accuracy.ptr(), part_key_accuracy.length());
      buf[part_key_accuracy.length()] = '\0';
    }

    if (OB_SUCC(ret)) {
      const char *delim = ",";
      char *token = NULL;
      char *saveptr = NULL;
      int64_t nums[3] = {0};

      token = strtok_r(buf, delim, &saveptr);
      int i;
      for (i = 0; OB_SUCC(ret) && token != NULL && i < 3; ++i) {
        if (OB_FAIL(get_int_value(ObString::make_string(token), nums[i]))) {
          LOG_WDIAG("fail to get int value from each token", K(ret), K(token));
        }
        token = strtok_r(NULL, delim, &saveptr);
      }

      // succ to parse the triple
      if (i != 3 || OB_FAIL(ret)) {
        LOG_WDIAG("invalid token or get failed from rs observer", K(i), K(ret));
      } else {
        int32_t length = static_cast<int32_t>(nums[0]);
        int16_t precision = static_cast<int16_t>(nums[1]);
        int16_t scale = static_cast<int16_t>(nums[2]);
        set_part_key_accuracy(part_key, part_key_type, length, precision, scale);
      }
    }

    if (OB_NOT_NULL(buf)) {
      allocator->free(buf);
      buf = NULL;
    }
  }

  return;
}

inline int ObRouteUtils::fetch_part_key(ObResultSetFetcher &rs_fetcher,
                                        ObProxyPartInfo &part_info,
                                        const int64_t cluster_version)
{
  int ret = OB_SUCCESS;

  // only process OBPROXY_MAX_PART_KEY_NUM part key
  if (OBPROXY_MAX_PART_KEY_NUM <= part_info.get_part_key_info().key_num_) {
    LOG_WDIAG("proxy does not support to fetch more part key", K(OBPROXY_MAX_PART_KEY_NUM));
  } else {
    ObProxyPartKeyInfo &part_key_info = part_info.get_part_key_info();
    ObIAllocator &allocator = part_info.get_allocator();
    ObPartitionLevel part_key_level = PARTITION_LEVEL_ONE;
    int64_t part_key_idx = -1;
    ObObjType part_key_type = ObMaxType;
    ObCollationType part_key_cs_type = CS_TYPE_INVALID;
    ObString part_key_name;
    ObString part_key_extra;

    // here store serialized default value
    // mysql mode return serialized ObObj with relevant column's type
    // oracle mode return serialized ObObj with varchar type
    char *part_key_default_value = NULL;
    int default_val_len = 0;

    ObString constraint_part_key;
    int64_t idx_in_rowid = -1;
    ObString part_key_accuracy;
    int64_t part_key_length = -1;
    int64_t part_key_precision = -1;
    int64_t part_key_scale = -1;
    int64_t schema_version = 0;

    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_key_level", part_key_level, ObPartitionLevel);
    // part key idx is the order of part key in all columns
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_key_idx", part_key_idx, int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_key_type", part_key_type, ObObjType);
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_key_name", part_key_name);
    // use part_key_extra as generated key expr
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_key_extra", part_key_extra);
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "spare4", constraint_part_key);
      // use spare1 as table collation type
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare1", part_key_cs_type, ObCollationType);
      // use spare2 as rowid index
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare2", idx_in_rowid, int64_t);
      // use spare5 as the accuracy of the part key
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "spare5", part_key_accuracy);
    } else {
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_key_expr", constraint_part_key);
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_key_collation_type", part_key_cs_type, ObCollationType);
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_key_rowkey_idx", idx_in_rowid, int64_t);
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_key_length", part_key_length, int64_t);
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_key_precision", part_key_precision, int64_t);
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_key_scale", part_key_scale, int64_t);
      // use spare1 as schema_version
      if (OB_SUCC(ret) && !IS_CLUSTER_VERSION_BEFORE_4_3_5_2(cluster_version)) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare1", schema_version, int64_t);
      }
    }

    // primary key as part key expr only for first part in mysql mode
    if (OB_SUCC(ret)) {
      if (part_info.is_primary_key_as_part_expr() && PARTITION_LEVEL_ONE == part_key_level) {
        char *buf = NULL;
        if (OB_ISNULL(buf = static_cast<char *>(part_info.get_allocator().alloc(part_key_name.length())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc part key name for primary key expr", K(buf), "size", part_key_name.length(), K(ret));
        } else if (FALSE_IT(MEMCPY(buf, part_key_name.ptr(), part_key_name.length()))) {
        } else if (OB_FAIL(part_info.get_first_part_columns().push_back(ObString(part_key_name.length(), buf)))) {
          LOG_WDIAG("fail to push back primary key columns", K(part_key_name), K(ret));
        } else {
          LOG_DEBUG("succ to push back primary key columns", K(part_key_name));
        }
      }
    }

    if (OB_SUCC(ret)) {
      // 可选列 3.2.3 7u 之前的版本没有 part_key_default_value 列
      PROXY_EXTRACT_STRBUF_FIELD_MYSQL_UNLIMIT_LENGTH(rs_fetcher, "part_key_default_value", part_key_default_value, default_val_len, allocator);
      if (OB_ERR_COLUMN_NOT_FOUND == ret) {
        LOG_DEBUG("part key default value not exist, continue", K(ret));
        ret = OB_SUCCESS;
      }
    }

    LOG_DEBUG("fetch part key", K(part_key_level), K(part_key_idx), K(part_key_type), K(part_key_name),
              K(part_key_extra), K(constraint_part_key), K(part_key_cs_type),
              K(idx_in_rowid), K(part_key_accuracy), K(part_key_default_value),
              K(part_key_length), K(part_key_precision), K(part_key_scale), K(schema_version));

    if (!is_obj_type_supported(part_key_type)) {
      part_info.set_unknown_part_key(true);
    }
    part_info.set_schema_version(schema_version);

    ObProxyPartKey *part_key = &part_key_info.part_keys_[part_key_info.key_num_];

    if (PARTITION_LEVEL_ONE == part_key_level) {
      part_key->level_ = PART_KEY_LEVEL_ONE;
    } else if (PARTITION_LEVEL_TWO == part_key_level) {
      part_key->level_ = PART_KEY_LEVEL_TWO;
    } else {
      ret = OB_INVALID_ARGUMENT_FOR_EXTRACT;
      LOG_WDIAG("part key level is invalid", K(part_key_level), K(ret));
    }

    char *buf = NULL;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(part_key_name.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to allc part key name", K(buf), K(part_key_name.length()), K(ret));
    } else {
      memcpy(buf, part_key_name.ptr(), part_key_name.length());
    }

    if (OB_SUCC(ret) && 0 != part_key_extra.length()) {
      char *extra_buf = NULL;
      if (OB_ISNULL(extra_buf = static_cast<char *>(allocator.alloc(part_key_extra.length())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to allc part key extra", K(extra_buf), K(part_key_extra.length()), K(ret));
      } else {
        memcpy(extra_buf, part_key_extra.ptr(), part_key_extra.length());
        part_key->part_key_extra_.str_ = extra_buf;
        part_key->part_key_extra_.str_len_ = part_key_extra.length();
      }
    }

    if (OB_SUCC(ret)) {
      part_key->idx_ = part_key_idx;
      part_key->name_.str_len_ = part_key_name.length();
      part_key->name_.str_ = buf;
      part_key->obj_type_ = part_key_type;
      part_key->idx_in_rowid_ = idx_in_rowid;
      part_key->accuracy_.valid_ = 0;               // not valid accuracy
      part_key->idx_in_part_columns_ = 0;
      // 如果建表时不含表达式，这个columns返回的是列名
      ObIArray<ObString> &columns = (part_key->level_ == PART_KEY_LEVEL_ONE ?
                                    part_info.get_first_part_columns() : part_info.get_sub_part_columns());
      for (int i = 0; i < columns.count(); i++) {
        ObString col(part_key->name_.str_len_, part_key->name_.str_);
        if (columns.at(i).case_compare(col) == 0) {
          part_key->idx_in_part_columns_ = i;
          break;
        }
      }
      part_key->default_value_.str_len_ = default_val_len;
      part_key->default_value_.str_ = part_key_default_value;
      part_key->generated_col_idx_ = -1;
      part_key->real_source_idx_ = -1;
      part_key->part_key_func_info_.part_key_func_type_ = OB_PROXY_EXPR_TYPE_NONE;

      if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
        parse_part_key_accuracy(part_key, part_key_type, &allocator, part_key_accuracy);
      } else {
        set_part_key_accuracy(part_key, part_key_type,
                              static_cast<int32_t>(part_key_length),
                              static_cast<int16_t>(part_key_precision),
                              static_cast<int16_t>(part_key_scale));
      }

      if (CS_TYPE_INVALID == part_key_cs_type) {
        part_key->cs_type_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
      } else {
        part_key->cs_type_ = part_key_cs_type;
      }
      // 解析建表分区表达式，如果建表时part_key是个func，预期columns不会被分割，只有一个元素
      if (OB_SUCC(ret) && 1 == columns.count()) {
        if (OB_FAIL(add_part_key_expr(columns.at(0), part_info, *part_key))) {
          LOG_WDIAG("fail to add_part_key_expr", K(ret));
        } else if (IS_DEBUG_ENABLED()) {
          ObString name(part_key->name_.str_len_, part_key->name_.str_);
          LOG_DEBUG("succ to add_part_key_expr", K(name), "key_num", part_key_info.key_num_,
                    K(columns), K(part_info));
        }
      }
      // 生成列
      if (!part_key_extra.empty() || !constraint_part_key.empty()) {
        part_key->is_generated_ = true;
        part_info.set_has_generated_key(true);
        int64_t generated_key_idx = part_key_info.key_num_;
        if (OB_FAIL(add_generated_part_key(part_key_extra, generated_key_idx, part_info))) {
          LOG_WDIAG("fail to add generated key", K(part_key_extra), K(ret));
        }
        const char *sep_pos = NULL;
        ObString tmp_str;
        while (NULL != (sep_pos = (constraint_part_key.find(PART_KEY_EXTRA_SEPARATOR)))) {
          tmp_str = constraint_part_key.split_on(sep_pos);
          if (OB_FAIL(add_generated_part_key(tmp_str, generated_key_idx, part_info))) {
            LOG_WDIAG("fail to add generated key", K(tmp_str), K(ret));
          }
        }
        if (OB_FAIL(add_generated_part_key(constraint_part_key, generated_key_idx, part_info))) {
          LOG_WDIAG("fail to add generated key", K(constraint_part_key), K(ret));
        }
      } else {
        part_key->is_generated_ = false;
      }
    }
  }

  return ret;
}

int ObRouteUtils::add_part_key_expr(const ObString &part_expr,
                                    ObProxyPartInfo &part_info,
                                    ObProxyPartKey &part_key/*src part key*/)
{
  int ret = OB_SUCCESS;
  if (!part_expr.empty()) {
    ObIAllocator &allocator = part_info.get_allocator();
    // parse part key expr
    ObFuncExprParseResult result;
    ObFuncExprParser parser(allocator, GENERATE_FUNC_PARSE_MODE);
    ObFuncExprNode *func_expr_node = NULL;
    ObString func_name;
    ObProxyExprType type = OB_PROXY_EXPR_TYPE_NONE;
    ObProxyParamNodeList *child = NULL;
    ObProxyPartKeyInfo &part_key_info = part_info.get_part_key_info();
    if (OB_UNLIKELY(part_key_info.key_num_ >= OBPROXY_MAX_PART_KEY_NUM)) {
      LOG_INFO("part key num is larger than OBPROXY_MAX_PART_KEY_NUM, we do not support",
               K_(part_key_info.key_num), K(OBPROXY_MAX_PART_KEY_NUM));
    } else if (OB_FAIL(parser.parse(part_expr, result))) {
      LOG_WDIAG("fail to parse part_expr expr", K(part_expr), K(ret));
    } else if (OB_ISNULL(result.param_node_)
               || PARAM_FUNC != result.param_node_->type_
               || OB_ISNULL(func_expr_node = result.param_node_->func_expr_node_)
               || OB_ISNULL(child = func_expr_node->child_)) {
      LOG_INFO("only support function part key expr", K(part_expr), "param node", reinterpret_cast<const void*>(result.param_node_),
               "type", result.param_node_->type_, KP(func_expr_node), KP(child), K(ret));
    } else if (OB_ISNULL(func_expr_node->func_name_.str_) || func_expr_node->func_name_.str_len_ <= 0) {
      LOG_WDIAG("empty function name");
      ret = OB_INVALID_ARGUMENT;
    } else if (FALSE_IT(func_name = ObString(func_expr_node->func_name_.str_len_, func_expr_node->func_name_.str_))) {
    } else if (OB_FAIL(ObProxyExprFactory::get_type_by_name(func_name, type))) {
      LOG_WDIAG("fail to get func type by name", K(func_name), K(type), K(ret));
    } else if (type == OB_PROXY_EXPR_TYPE_NONE) {
      LOG_DEBUG("unsupported function", K(func_name));
    } else {
      part_info.set_has_part_func_key(true);
      part_key.part_key_func_info_.part_key_func_type_ = type;
      part_key.part_key_func_info_.func_params_ = result.param_node_;
      LOG_DEBUG("succ to add part_key_func", K(func_name), K(type), K(part_key_info.key_num_), K(part_info), K(ret));
    }
  }

  return ret;
}

int ObRouteUtils::add_generated_part_key(const ObString &part_key_extra,
                                         const int64_t generated_key_idx,
                                         ObProxyPartInfo &part_info)
{
  int ret = OB_SUCCESS;
  if (!part_key_extra.empty()) {
    ObProxyPartKeyInfo &part_key_info = part_info.get_part_key_info();
    const ObProxyPartKey &part_key = part_key_info.part_keys_[generated_key_idx];
    ObIAllocator &allocator = part_info.get_allocator();
    // parse generated key expr
    ObFuncExprParseResult result;
    ObFuncExprParser parser(allocator, GENERATE_FUNC_PARSE_MODE);
    ObFuncExprNode *func_expr_node = NULL;
    ObString func_name;
    ObProxyExprType type = OB_PROXY_EXPR_TYPE_NONE;
    ObProxyParamNodeList *child = NULL;
    // part_key_info.key_num_代表现在已经使用的列信息，生成列需要分配新的列，所以判断key_num_ + 1
    if (OB_UNLIKELY(part_key_info.key_num_ + 1 >= OBPROXY_MAX_PART_KEY_NUM)) {
      LOG_INFO("part key num is larger than OBPROXY_MAX_PART_KEY_NUM, we do not support",
               K_(part_key_info.key_num), K(OBPROXY_MAX_PART_KEY_NUM));
    } else if (OB_FAIL(parser.parse(part_key_extra, result))) {
      LOG_WDIAG("fail to parse generated function expr", K(ret), K(part_key_extra));
    } else if (OB_ISNULL(result.param_node_)
               || PARAM_FUNC != result.param_node_->type_
               || OB_ISNULL(func_expr_node = result.param_node_->func_expr_node_)
               || OB_ISNULL(child = func_expr_node->child_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("generated function must be a func", "param node", reinterpret_cast<const void*>(result.param_node_),
               "type", result.param_node_->type_, KP(func_expr_node), KP(child), K(ret));
    } else if (OB_UNLIKELY(child->child_num_ > OBPROXY_MAX_PARAM_NUM)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("generated function param num is unexpectedlly larger than 3",
               "child num", child->child_num_, K(OBPROXY_MAX_PARAM_NUM), K(ret));
    } else if (OB_ISNULL(func_expr_node->func_name_.str_) || func_expr_node->func_name_.str_len_ <= 0) {
      LOG_WDIAG("empty generated function name");
      ret = OB_INVALID_ARGUMENT;
    } else if (FALSE_IT(func_name = ObString(func_expr_node->func_name_.str_len_, func_expr_node->func_name_.str_))) {
    } else if (OB_FAIL(ObProxyExprFactory::get_type_by_name(func_name, type))) {
      LOG_WDIAG("fail to get func type by name", K(func_name), K(type), K(ret));
    } else if (type == OB_PROXY_EXPR_TYPE_NONE) {
      LOG_DEBUG("unsupported function", K(func_name));
      ret = OB_ERR_FUNCTION_UNKNOWN;
    } else {
      // 分配新的列
      ++part_key_info.key_num_;
      part_key_info.part_keys_[part_key_info.key_num_].func_type_ = type;
      part_key_info.part_keys_[part_key_info.key_num_].param_num_ = child->child_num_;
      part_key_info.part_keys_[part_key_info.key_num_].generated_col_idx_ = generated_key_idx;
      memset(part_key_info.part_keys_[part_key_info.key_num_].params_, 0, OBPROXY_MAX_PARAM_NUM);
      ObProxyParamNode* child_param_node = child->head_;
      for (int64_t i = 0; i < child->child_num_ && OB_SUCC(ret); ++i) {
        if (OB_ISNULL(child_param_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("generated function param is null", K(ret));
        } else {
          part_key_info.part_keys_[part_key_info.key_num_].params_[i] = child_param_node;
          if (PARAM_COLUMN == child_param_node->type_) {
            // add src key into part_key_info
            part_key_info.part_keys_[part_key_info.key_num_].name_ = child_param_node->col_name_;
            part_key_info.part_keys_[part_key_info.key_num_].level_ = part_key.level_;
            part_key_info.part_keys_[part_key_info.key_num_].obj_type_ = part_key.obj_type_;
            part_key_info.part_keys_[part_key_info.key_num_].cs_type_ = part_key.cs_type_;
            part_key_info.part_keys_[part_key_info.key_num_].idx_in_part_columns_ = part_key.idx_in_part_columns_;
            part_key_info.part_keys_[part_key_info.key_num_].is_generated_ = false;
            part_key_info.part_keys_[part_key_info.key_num_].part_key_func_info_.part_key_func_type_ = OB_PROXY_EXPR_TYPE_NONE;
          }
          child_param_node = child_param_node->next_;
        }
      }
    }
  }
  return ret;
}

inline int ObRouteUtils::fetch_part_option(ObResultSetFetcher &rs_fetcher,
                                           ObProxyPartInfo &part_info)
{
  int ret = OB_SUCCESS;
  ObProxyPartOption &first_part_opt = part_info.get_first_part_option();
  ObProxyPartOption &sub_part_opt = part_info.get_sub_part_option();

  //get all sub part num
  int64_t all_sub_part_num=0;
  //"all_part_num" in __all_virtual_proxy_partition_info
  PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "all_part_num", all_sub_part_num, int64_t);
  part_info.get_part_mgr().set_all_sub_part_num(all_sub_part_num);

  // get first part
  PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_num", first_part_opt.part_num_, int64_t);
  PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_type", first_part_opt.part_func_type_,
                                ObPartitionFuncType);
  PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_space", first_part_opt.part_space_, int32_t);

  // get sub part
  PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_num", sub_part_opt.part_num_, int64_t);
  PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_type", sub_part_opt.part_func_type_,
                                ObPartitionFuncType);
  PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_space", sub_part_opt.part_space_, int32_t);

  return ret;
}

int ObRouteUtils::fetch_first_part(ObResultSetFetcher &rs_fetcher, ObProxyPartInfo &part_info, const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = part_info.get_schema_version();
  part_info.get_part_mgr().set_cluster_version(cluster_version);
  if (part_info.get_first_part_option().is_range_part(cluster_version)) {
    if (OB_FAIL(part_info.get_part_mgr().build_range_part(share::schema::PARTITION_LEVEL_ONE,
                                                          part_info.get_first_part_option().part_func_type_,
                                                          part_info.get_first_part_option().part_num_,
                                                          part_info.get_first_part_columns().count(),
                                                          part_info.is_template_table(),
                                                          part_info.get_part_key_info(),
                                                          rs_fetcher,
                                                          cluster_version,
                                                          schema_version))) {
      LOG_WDIAG("fail to build range part", K(ret));
    }
  } else if (part_info.get_first_part_option().is_list_part(cluster_version)) {
    if (OB_FAIL(part_info.get_part_mgr().build_list_part(share::schema::PARTITION_LEVEL_ONE,
                                                         part_info.get_first_part_option().part_func_type_,
                                                         part_info.get_first_part_option().part_num_,
                                                         part_info.get_first_part_columns().count(),
                                                         part_info.is_template_table(),
                                                         part_info.get_part_key_info(),
                                                         rs_fetcher,
                                                         cluster_version,
                                                         schema_version))) {
      LOG_WDIAG("fail to build list part", K(ret));
    }
  } else if (part_info.get_first_part_option().is_hash_part(cluster_version)) {
    if (OB_FAIL(part_info.get_part_mgr().build_hash_part(part_info.is_oracle_mode(),
                                                         share::schema::PARTITION_LEVEL_ONE,
                                                         part_info.get_first_part_option().part_func_type_,
                                                         part_info.get_first_part_option().part_num_,
                                                         part_info.get_first_part_option().part_space_,
                                                         part_info.get_first_part_columns().count(),
                                                         part_info.is_template_table(),
                                                         part_info.get_part_key_info(),
                                                         &rs_fetcher,
                                                         cluster_version,
                                                         schema_version))) {
      LOG_WDIAG("fail to build hash part", K(ret));
    }
  } else if (part_info.get_first_part_option().is_key_part(cluster_version)) {
    if (OB_FAIL(part_info.get_part_mgr().build_key_part(share::schema::PARTITION_LEVEL_ONE,
                                                        part_info.get_first_part_option().part_func_type_,
                                                        part_info.get_first_part_option().part_num_,
                                                        part_info.get_first_part_option().part_space_,
                                                        part_info.get_first_part_columns().count(),
                                                        part_info.is_template_table(),
                                                        part_info.get_part_key_info(),
                                                        &rs_fetcher,
                                                        cluster_version,
                                                        schema_version))) {
      LOG_WDIAG("fail to build key part", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("not support part type", "part_type", part_info.get_first_part_option().part_func_type_,
             K(ret));
  }
  part_info.set_schema_version(schema_version);
  return ret;
}

int ObRouteUtils::fetch_sub_part(ObResultSetFetcher &rs_fetcher, ObProxyPartInfo &part_info, const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = part_info.get_schema_version();
  if (part_info.get_sub_part_option().is_range_part(cluster_version)) {
    if (part_info.is_template_table()) {
      if (OB_FAIL(part_info.get_part_mgr().build_range_part(share::schema::PARTITION_LEVEL_TWO,
                                                            part_info.get_sub_part_option().part_func_type_,
                                                            part_info.get_sub_part_option().part_num_,
                                                            part_info.get_sub_part_columns().count(),
                                                            part_info.is_template_table(),
                                                            part_info.get_part_key_info(),
                                                            rs_fetcher,
                                                            cluster_version,
                                                            schema_version))) {
        LOG_WDIAG("fail to build range part", K(ret));
      }
    } else {
      if (OB_FAIL(part_info.get_part_mgr().build_sub_range_part_with_non_template(part_info.get_sub_part_option().part_func_type_,
                                                                                  part_info.get_sub_part_columns().count(),
                                                                                  part_info.get_part_key_info(),
                                                                                  rs_fetcher,
                                                                                  cluster_version,
                                                                                  schema_version))) {
        LOG_WDIAG("fail to build range part", K(ret));
      }
    }
  } else if (part_info.get_sub_part_option().is_list_part(cluster_version)) {
    if (part_info.is_template_table()) {
      if (OB_FAIL(part_info.get_part_mgr().build_list_part(share::schema::PARTITION_LEVEL_TWO,
                                                           part_info.get_sub_part_option().part_func_type_,
                                                           part_info.get_sub_part_option().part_num_,
                                                           part_info.get_sub_part_columns().count(),
                                                           part_info.is_template_table(),
                                                           part_info.get_part_key_info(),
                                                           rs_fetcher,
                                                           cluster_version,
                                                           schema_version))) {
        LOG_WDIAG("fail to build list part", K(ret));
      }
    } else {
      if (OB_FAIL(part_info.get_part_mgr().build_sub_list_part_with_non_template(part_info.get_sub_part_option().part_func_type_,
                                                                                 part_info.get_sub_part_columns().count(),
                                                                                 part_info.get_part_key_info(),
                                                                                 rs_fetcher,
                                                                                 cluster_version,
                                                                                 schema_version))) {
        LOG_WDIAG("fail to build range part", K(ret));
      }
    }
  } else if (part_info.get_sub_part_option().is_hash_part(cluster_version)) {
    if (part_info.is_template_table()) {
      if (OB_FAIL(part_info.get_part_mgr().build_hash_part(part_info.is_oracle_mode(),
                                                           share::schema::PARTITION_LEVEL_TWO,
                                                           part_info.get_sub_part_option().part_func_type_,
                                                           part_info.get_sub_part_option().part_num_,
                                                           part_info.get_sub_part_option().part_space_,
                                                           part_info.get_sub_part_columns().count(),
                                                           part_info.is_template_table(),
                                                           part_info.get_part_key_info(),
                                                           &rs_fetcher,
                                                           cluster_version,
                                                           schema_version))) {
        LOG_WDIAG("fail to build hash part", K(ret));
      }
    } else {
      if (OB_FAIL(part_info.get_part_mgr().build_sub_hash_part_with_non_template(part_info.is_oracle_mode(),
                                                                                 part_info.get_sub_part_option().part_func_type_,
                                                                                 part_info.get_sub_part_option().part_space_,
                                                                                 part_info.get_sub_part_columns().count(),
                                                                                 part_info.get_part_key_info(),
                                                                                 rs_fetcher,
                                                                                 cluster_version,
                                                                                 schema_version))) {
        LOG_WDIAG("fail to build range part", K(ret));
      }
    }
  } else if (part_info.get_sub_part_option().is_key_part(cluster_version)) {
    if (part_info.is_template_table()) {
      if (OB_FAIL(part_info.get_part_mgr().build_key_part(share::schema::PARTITION_LEVEL_TWO,
                                                          part_info.get_sub_part_option().part_func_type_,
                                                          part_info.get_sub_part_option().part_num_,
                                                          part_info.get_sub_part_option().part_space_,
                                                          part_info.get_sub_part_columns().count(),
                                                          part_info.is_template_table(),
                                                          part_info.get_part_key_info(),
                                                          &rs_fetcher,
                                                          cluster_version,
                                                          schema_version))) {
        LOG_WDIAG("fail to build key part", K(ret));
      }
    } else {
      if (OB_FAIL(part_info.get_part_mgr().build_sub_key_part_with_non_template(part_info.get_sub_part_option().part_func_type_,
                                                                                part_info.get_sub_part_option().part_space_,
                                                                                part_info.get_sub_part_columns().count(),
                                                                                part_info.get_part_key_info(),
                                                                                rs_fetcher,
                                                                                cluster_version,
                                                                                schema_version))) {
        LOG_WDIAG("fail to build range part", K(ret));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("not support part type", "part_type", part_info.get_sub_part_option().part_func_type_,
             K(ret));
  }
  part_info.set_schema_version(schema_version);
  return ret;
}

int ObRouteUtils::build_sys_dummy_entry(
    const ObString &cluster_name,
    const int64_t cluster_id,
    const LocationList &rs_list,
    const bool is_rslist,
    ObTableEntry *&entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cluster_name.empty()) || OB_UNLIKELY(rs_list.empty())
      || OB_UNLIKELY(cluster_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(cluster_name), K(cluster_id), K(rs_list), K(ret));
  } else {
    ObString tenant_name(OB_SYS_TENANT_NAME);
    ObString database_name(OB_SYS_DATABASE_NAME);
    ObString table_name(OB_ALL_DUMMY_TNAME);
    ObTableEntryName name;
    name.cluster_name_ = cluster_name;
    name.tenant_name_ = tenant_name;
    name.database_name_ = database_name;
    name.table_name_ = table_name;
    ObTenantServer *tenant_servers = NULL;
    // sys tenant's all_dummy_entry cr_version must be 0
    const int64_t belonged_cr_version = 0;

    if (OB_FAIL(ObTableEntry::alloc_and_init_table_entry(name, belonged_cr_version, cluster_id, entry))) {
      LOG_WDIAG("fail to alloc and init table entry", K(name), K(cluster_id), K(ret));
    } else if (OB_ISNULL(tenant_servers = op_alloc(ObTenantServer))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to allocate memory for ObProxyTenantServers", K(ret));
    } else if (OB_FAIL(tenant_servers->init(rs_list))) {
      LOG_WDIAG("fail to init tenant servers", K(rs_list), K(ret));
    } else if (OB_UNLIKELY(!tenant_servers->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("tenant_server should not unavailable", KPC(tenant_servers), K(ret));
    } else if (OB_FAIL(entry->set_tenant_servers(tenant_servers))) {
      LOG_WDIAG("fail to set tenant servers", K(tenant_servers), K(ret));
    } else {
      if (is_rslist) {
        entry->set_entry_from_rslist();
      }
      //must set it NULL as we had move pointer's value
      tenant_servers = NULL;
      entry->set_part_num(1);
      entry->set_replica_num(entry->get_tenant_servers()->replica_count());
      entry->set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_DUMMY_TID));
      entry->set_avail_state();
    }
    if (OB_FAIL(ret)) {
      if (NULL != entry) {
        entry->dec_ref();
        entry = NULL;
      }
      if (NULL != tenant_servers) {
        op_free(tenant_servers);
        tenant_servers = NULL;
      }
    }
  }
  return ret;
}


int ObRouteUtils::build_and_add_sys_dummy_entry(const ObString &cluster_name,
                                                const int64_t cluster_id,
                                                const LocationList &locaiton_list,
                                                const bool is_rslist)
{
  int ret = OB_SUCCESS;
  ObTableEntry *entry = NULL;
  ObTableCache &table_cache = get_global_table_cache();
  if (OB_FAIL(build_sys_dummy_entry(cluster_name, cluster_id, locaiton_list, is_rslist, entry))) {
    LOG_WDIAG("fail to build sys dummy entry", K(cluster_name), K(cluster_id), K(locaiton_list), K(ret));
  } else if (OB_ISNULL(entry) || OB_UNLIKELY(!entry->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("entry can not be NULL here", KPC(entry), K(ret));
  } else {
    entry->inc_ref();
    ObTableEntry *tmp_entry = entry;
    bool direct_add = (NULL == this_ethread()) ? true : false;
    if (OB_FAIL(table_cache.add_table_entry(*tmp_entry, direct_add))) {
      LOG_WDIAG("fail to add table entry", KPC(tmp_entry), K(ret));
    } else {
      LOG_INFO("update sys tenant __all_dummy succ", K(cluster_name), KPC(tmp_entry));
    }
    tmp_entry->dec_ref();
    tmp_entry = NULL;
  }

  if (OB_FAIL(ret)) {
    if (NULL != entry) {
      entry->dec_ref();
      entry = NULL;
    }
  }
  return ret;
}

int ObRouteUtils::build_and_add_sys_dummy_entry(const ObString &cluster_name,
                                                const int64_t cluster_id,
                                                const ObIArray<ObAddr> &addr_list,
                                                const ObIArray<ObAddr> &rpc_addr_list,
                                                const bool is_rslist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cluster_name.empty() || addr_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input vlue", K(addr_list), K(cluster_name), K(ret));
  } else {
    LocationList location_list;
    if (OB_FAIL(convert_addrs_to_locations(addr_list, rpc_addr_list, location_list))) {
      LOG_WDIAG("fail to convert_addrs_to_locations", K(ret));
    } else if (OB_FAIL(build_and_add_sys_dummy_entry(cluster_name, cluster_id, location_list, is_rslist))) {
      LOG_WDIAG("fail to build_and_add_sys_dummy_entry", K(cluster_name), K(cluster_id), K(location_list), K(ret));
    }
  }

  return ret;
}

int ObRouteUtils::build_cdc_dummy_entry(
    const ObString &cluster_name,
    const ObString &tenant_name,
    const LocationList &cdc_coordinator_list,
    ObTableEntry *&entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cdc_coordinator_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(cluster_name), K(tenant_name), K(cdc_coordinator_list), K(ret));
  } else {
    // there is need to set database name for __all_cdc_coordinator_dummy.
    ObString database_name(OB_SYS_DATABASE_NAME);
    ObString table_name(OB_ALL_CDC_COORDINATOR_DUMMY_TNAME);
    ObTableEntryName name;
    name.cluster_name_ = cluster_name;
    name.tenant_name_ = tenant_name;
    name.database_name_ = database_name;
    name.table_name_ = table_name;
    // cr_version and cluster_id should be 0
    const int64_t belonged_cr_version = 0;
    const int64_t cluster_id = 0;

    ObProxyPartitionLocation *pp_location = NULL;
    if (OB_FAIL(ObTableEntry::alloc_and_init_table_entry(name, belonged_cr_version, cluster_id, entry))) {
      LOG_WDIAG("fail to alloc and init table entry", K(name), K(cluster_id), K(ret));
    } else if (OB_ISNULL(pp_location = op_alloc(ObProxyPartitionLocation))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to allocate memory for ObProxyPartitionLocation", K(ret));
    } else if (OB_FAIL(pp_location->set_replicas(cdc_coordinator_list))) {
      LOG_WDIAG("fail to set replicas", K(cdc_coordinator_list), K(ret));
    } else if (!cdc_coordinator_list.empty() && OB_UNLIKELY(!pp_location->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ppl should not unavailable", KPC(pp_location), K(ret));
    } else {
      entry->set_cdc_coordinator_entry(true);
      entry->set_allow_empty_entry(false);
      entry->set_part_num(1);
      entry->set_replica_num(1);
      entry->set_schema_version(0);
      entry->set_tenant_id(OB_INVALID_TENANT_ID);
      entry->set_table_id(0);
      entry->set_table_type(MAX_TABLE_TYPE);
      if (OB_FAIL(entry->set_first_partition_location(pp_location))) {
        LOG_WDIAG("fail to set first partition location", K(ret));
      } else {
        pp_location = NULL; // set to NULL, if succ
      }
    }

    if (OB_FAIL(ret)) {
      if (NULL != entry) {
        entry->dec_ref();
        entry = NULL;
      }
    }
    if (NULL != pp_location) {
      op_free(pp_location);
    }
  }
  return ret;
}

int ObRouteUtils::build_and_add_cdc_dummy_entry(const ObString &cluster_name,
                                                const ObString &tenant_name,
                                                const LocationList &locaiton_list)
{
  int ret = OB_SUCCESS;
  ObTableEntry *entry = NULL;
  ObTableCache &table_cache = get_global_table_cache();
  if (OB_FAIL(build_cdc_dummy_entry(cluster_name, tenant_name, locaiton_list, entry))) {
    LOG_WDIAG("fail to build cdc dummy entry", K(cluster_name), K(tenant_name),
              K(locaiton_list), KP(entry), K(ret));
  } else if (OB_ISNULL(entry) || OB_UNLIKELY(!entry->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("entry can not be NULL here", KPC(entry), K(ret));
  } else {
    entry->inc_ref();
    ObTableEntry *tmp_entry = entry;
    bool direct_add = (NULL == this_ethread()) ? true : false;
    if (OB_FAIL(table_cache.add_table_entry(*tmp_entry, direct_add))) {
      LOG_WDIAG("fail to add table entry", KPC(tmp_entry), K(ret));
    } else {
      LOG_INFO("update sys tenant __all_cdc_coordinator_dummy succ", K(cluster_name), KPC(tmp_entry));
    }
    tmp_entry->dec_ref();
    tmp_entry = NULL;
  }

  if (OB_FAIL(ret)) {
    if (NULL != entry) {
      entry->dec_ref();
      entry = NULL;
    }
  }
  return ret;
}

int ObRouteUtils::convert_ip_string_to_locations(const ObString &ip_port_list_string,
                                                 LocationList &location_list)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObAddr, 4> addr_list;
  if (OB_FAIL(convert_ip_string_to_addrs(ip_port_list_string, addr_list))) {
    LOG_WDIAG("fail to convert to addrs", K(ip_port_list_string), K(ret));
  } else if (OB_FAIL(convert_addrs_to_locations(addr_list, location_list))) {
    LOG_WDIAG("fail to convert to locations", K(ip_port_list_string), K(ret));
  }

  return ret;
}

int ObRouteUtils::convert_ip_string_to_addrs(const ObString &ip_port_list_string,
                                             ObIArray<ObAddr> &addr_list)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObString, 4> ip_port_list;
  if (OB_FAIL(obproxy::split_string_by_char(ip_port_list_string, ip_port_list, ';'))) {
    LOG_WDIAG("fail to split ip_port_list_string", K(ip_port_list_string), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ip_port_list.count(); ++i) {
      ObAddr tmp_addr;

      if (OB_FAIL(tmp_addr.parse_from_obtring(ip_port_list.at(i)))) {
        LOG_WDIAG("fail to parse ip_port_string", K(tmp_addr), "ip_port_string", ip_port_list.at(i), K(ret));
      } else if (OB_FAIL(addr_list.push_back(tmp_addr))) {
        LOG_WDIAG("fail to push back tmp_addr", K(ret));
      }
    }
  }

  return ret;
}

int ObRouteUtils::refresh_cdc_dummy_entry_by_config(const ObString &ip_port_list_string,
                                                    const ObString &cluster_name,
                                                    const ObString &tenant_name)
{
  int ret = OB_SUCCESS;

  LocationList location_list;
  if (OB_FAIL(convert_ip_string_to_locations(ip_port_list_string, location_list))) {
    LOG_WDIAG("fail to convert to locations", K(ret));
  } else if (OB_UNLIKELY(location_list.empty())) {
    LOG_DEBUG("empty cdc coordinator list, ignore it", K(ip_port_list_string), K(cluster_name),
              K(tenant_name));
  } else if (OB_FALSE_IT(location_list.at(0).set_role(LEADER))) {
    // treat first ip:port as leader
  } else if (OB_FAIL(build_and_add_cdc_dummy_entry(cluster_name, tenant_name, location_list))) {
    LOG_WDIAG("fail to add cdc dummy_entry", K(ret));
  }

  return ret;
}

int ObRouteUtils::convert_addrs_to_locations(const ObIArray<ObAddr> &addr_list,
                                             const ObIArray<ObAddr> &rpc_addr_list,
                                             LocationList &location_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(addr_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input vlue", K(addr_list), K(ret));
  } else {
    ObProxyReplicaLocation prl;
    for (int64_t i = 0; (i < addr_list.count()) && OB_SUCC(ret); ++i) {
      prl.reset();
      prl.server_ = addr_list.at(i);
      prl.rpc_server_ = rpc_addr_list.at(i);
      prl.role_ = FOLLOWER;
      if (OB_FAIL(location_list.push_back(prl))) {
        LOG_WDIAG("fail to push back replica location", K(prl), K(ret));
      }
    }
  }

  return ret;
}

int ObRouteUtils::convert_addrs_to_locations(const ObIArray<ObAddr> &addr_list,
                                             LocationList &location_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(addr_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input vlue", K(addr_list), K(ret));
  } else {
    ObProxyReplicaLocation prl;
    for (int64_t i = 0; (i < addr_list.count()) && OB_SUCC(ret); ++i) {
      prl.reset();
      prl.server_ = addr_list.at(i);
      prl.role_ = FOLLOWER;
      if (OB_FAIL(location_list.push_back(prl))) {
        LOG_WDIAG("fail to push back replica location", K(prl), K(ret));
      }
    }
  }

  return ret;
}

int ObRouteUtils::convert_route_param_to_table_param(
    const ObRouteParam &route_param,
    ObTableRouteParam &table_param)
{
  int ret = OB_SUCCESS;
  if (!route_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid route param", K(route_param), K(ret));
  } else {
    table_param.name_.shallow_copy(route_param.name_);
    table_param.current_idc_name_ = route_param.current_idc_name_;//shallow copy
    table_param.force_renew_ = route_param.force_renew_;
    table_param.force_use_cache_ = route_param.force_use_cache_;
    table_param.skip_refresh_cache_ = route_param.skip_refresh_cache_;
    table_param.mysql_proxy_ = route_param.mysql_proxy_;
    table_param.cr_version_ = route_param.cr_version_;
    table_param.cr_id_ = route_param.cr_id_;
    table_param.cluster_version_ = route_param.cluster_version_;
    table_param.tenant_version_ = route_param.tenant_version_;
    table_param.is_partition_table_route_supported_ = route_param.is_partition_table_route_supported_;
    table_param.is_oracle_mode_ = route_param.is_oracle_mode_;
    table_param.is_need_force_flush_ = route_param.is_need_force_flush_;
    table_param.set_route_diagnosis(route_param.route_diagnosis_);
    table_param.binlog_service_ip_ = route_param.binlog_service_ip_;
    table_param.is_single_partition_table_ = route_param.is_single_partition_table_;
    table_param.cdc_msgservice_param_ = route_param.cdc_msgservice_param_;
  }

  return ret;
}

int ObRouteUtils::convert_route_param_to_routine_param(
    const ObRouteParam &route_param,
    ObRoutineParam &routine_param)
{
  int ret = OB_SUCCESS;
  if (!route_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid route param", K(route_param), K(ret));
  } else {
    routine_param.name_.shallow_copy(route_param.name_);
    routine_param.current_idc_name_ = route_param.current_idc_name_;//shallow copy
    routine_param.force_renew_ = route_param.force_renew_;
    routine_param.mysql_proxy_ = route_param.mysql_proxy_;
    routine_param.cr_version_ = route_param.cr_version_;
    routine_param.cr_id_ = route_param.cr_id_;
    routine_param.cluster_version_ = route_param.cluster_version_;
  }
  return ret;
}

int ObRouteUtils::get_partition_entry_sql(char *sql_buf, const int64_t buf_len,
                                          const ObTableEntryName &name,
                                          const uint64_t partition_id,
                                          bool is_need_force_flush,
                                          const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0)
      || OB_UNLIKELY(!name.is_valid()) || common::OB_INVALID_ID == partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", LITERAL_K(sql_buf), K(buf_len),
             K(name), K(partition_id), K(ret));
  } else {
    int64_t len = 0;
    char new_tenant_name_buf[OB_MAX_TENANT_NAME_LENGTH * 2 + 1];
    ObString new_tenant_name;
    get_tenant_name(name.tenant_name_, new_tenant_name_buf, new_tenant_name);
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
      len = static_cast<int64_t>(snprintf(sql_buf, OB_SHORT_SQL_LENGTH, PROXY_PLAIN_SCHEMA_SQL_RPC,
                                          is_need_force_flush ? ", FORCE_REFRESH_LOCATION_CACHE" : "",
                                          OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                          OB_ALL_VIRTUAL_SERVER_STAT_TNAME,
                                          new_tenant_name.length(), new_tenant_name.ptr(),
                                          name.database_name_.length(), name.database_name_.ptr(),
                                          name.table_name_.length(), name.table_name_.ptr(),
                                          partition_id, INT64_MAX));
    } else if (IS_CLUSTER_VERSION_BEFORE_4_3_5_2(cluster_version)) {
      len = static_cast<int64_t>(snprintf(sql_buf, OB_SHORT_SQL_LENGTH, PROXY_PLAIN_SCHEMA_SQL_RPC_V4,
                                          is_need_force_flush ? ", FORCE_REFRESH_LOCATION_CACHE" : "",
                                          OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                          DBA_OB_SERVERS_VNAME,
                                          DBA_OB_TENANTS_VNAME,
                                          new_tenant_name.length(), new_tenant_name.ptr(),
                                          name.database_name_.length(), name.database_name_.ptr(),
                                          name.table_name_.length(), name.table_name_.ptr(),
                                          partition_id, INT64_MAX));
    } else {
      len = static_cast<int64_t>(snprintf(sql_buf, OB_SHORT_SQL_LENGTH, PROXY_PLAIN_SCHEMA_SQL_RPC_AFTER_OB_4352,
                                          is_need_force_flush ? ", FORCE_REFRESH_LOCATION_CACHE" : "",
                                          OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                          DBA_OB_TENANTS_VNAME,
                                          new_tenant_name.length(), new_tenant_name.ptr(),
                                          name.database_name_.length(), name.database_name_.ptr(),
                                          name.table_name_.length(), name.table_name_.ptr(),
                                          partition_id, INT64_MAX));
    }

    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }

  return ret;
}

int ObRouteUtils::get_batch_partition_entry_sql(char *sql_buf, const int64_t buf_len,
                                                const ObTableEntryName &name,
                                                const common::ObIArray<uint64_t> &partition_ids,
                                                bool is_need_force_flush,
                                                const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0)
      || OB_UNLIKELY(!name.is_valid()) || 0 == partition_ids.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", LITERAL_K(sql_buf), K(buf_len),
             K(name), K(partition_ids), K(ret));
  } else {
    int64_t len = 0;
    char new_tenant_name_buf[OB_MAX_TENANT_NAME_LENGTH * 2 + 1];
    ObString new_tenant_name;
    get_tenant_name(name.tenant_name_, new_tenant_name_buf, new_tenant_name);

    char partitions_buf[OB_SHORT_SQL_LENGTH]; // batch not more than 40{ UINT64_MAX = 18446744073709552000(20)  + ', '(2) 22 * 40 }
    uint32_t i = 0;
    char *buf = partitions_buf;
    uint32_t pbuf_len = OB_SHORT_SQL_LENGTH;
    for (i = 0; i < partition_ids.count(); i++) {
      int32_t write_len = snprintf(buf, pbuf_len, i != partition_ids.count() - 1 ? "%ld, " : "%ld", partition_ids.at(i));
      if (OB_UNLIKELY(write_len < 0 || write_len >= pbuf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid batch count to handle", K(ret), "count", partition_ids.count());
      } else {
        pbuf_len -= write_len;
        buf += write_len;
      }
    }
    pbuf_len = buf - partitions_buf;
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
      // to use for <4.0.0
      if (OB_SUCC(ret)) {
        len = static_cast<int64_t>(snprintf(sql_buf, OB_MEDIUM_SQL_LENGTH, PROXY_PLAIN_SCHEMA_SQL_RPC_BATCH,
                                            is_need_force_flush ? ", FORCE_REFRESH_LOCATION_CACHE" : "",
                                            OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                            OB_ALL_VIRTUAL_SERVER_STAT_TNAME,
                                            new_tenant_name.length(), new_tenant_name.ptr(),
                                            name.database_name_.length(), name.database_name_.ptr(),
                                            name.table_name_.length(), name.table_name_.ptr(),
                                            pbuf_len, partitions_buf, //partition_id,
                                            INT64_MAX));
      }
    } else if (IS_CLUSTER_VERSION_BEFORE_4_3_5_2(cluster_version)) {
      if (OB_SUCC(ret)) {
        len = static_cast<int64_t>(snprintf(sql_buf, OB_MEDIUM_SQL_LENGTH, PROXY_PLAIN_SCHEMA_SQL_RPC_V4_BATCH,
                                            is_need_force_flush ? ", FORCE_REFRESH_LOCATION_CACHE" : "",
                                            OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                            DBA_OB_SERVERS_VNAME,
                                            new_tenant_name.length(), new_tenant_name.ptr(),
                                            name.database_name_.length(), name.database_name_.ptr(),
                                            name.table_name_.length(), name.table_name_.ptr(),
                                            pbuf_len, partitions_buf,
                                            INT64_MAX));
      }
    } else {
      if (OB_SUCC(ret)) {
        len = static_cast<int64_t>(snprintf(sql_buf, OB_MEDIUM_SQL_LENGTH, PROXY_PLAIN_SCHEMA_SQL_RPC_AFTER_OB_4352_BATCH,
                                            is_need_force_flush ? ", FORCE_REFRESH_LOCATION_CACHE" : "",
                                            OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                            new_tenant_name.length(), new_tenant_name.ptr(),
                                            name.database_name_.length(), name.database_name_.ptr(),
                                            name.table_name_.length(), name.table_name_.ptr(),
                                            pbuf_len, partitions_buf,
                                            INT64_MAX));
      }
    }

    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }

  return ret;
}

int ObRouteUtils::fetch_one_partition_entry_info(
      ObResultSetFetcher &rs_fetcher,
      ObTableEntry &table_entry,
      ObPartitionEntry *&entry,
      const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0; // 仅用于填充出参，不起作用，需保证对应的字符串中间没有'\0'字符
  char ip_str[MAX_IP_ADDR_LENGTH];
  ip_str[0] = '\0';
  int64_t port = 0;
  int64_t rpc_port = 0;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t partition_id = OB_INVALID_ID;
  int64_t part_num = 0;
  int64_t schema_version = 0;
  int64_t role = -1;
  int32_t replica_type = -1;
  int32_t dup_replica_type = 0; // 0 for non dup replica; 1 for dup replica; -1 for invalid dup replica type
  ObAddr leader_addr;
  ObPartitionEntry *part_entry = NULL;
  ObProxyReplicaLocation prl;
  ObSEArray<ObProxyReplicaLocation, 32> replicas;
  bool has_dup_replica = false;

  while ((OB_SUCC(ret)) && (OB_SUCC(rs_fetcher.next()))) {
    ip_str[0] = '\0';
    port = 0;

    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "svr_ip", ip_str, MAX_IP_ADDR_LENGTH, tmp_real_str_len);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sql_port", port, int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "table_id", table_id, uint64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "role", role, int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_num", part_num, int64_t);
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "partition_id", partition_id, uint64_t);
    } else {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "tablet_id", partition_id, uint64_t);
    }
    if (IS_CLUSTER_VERSION_BEFORE_4_3_5_2(cluster_version)) {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "svr_port", rpc_port, int64_t);
    } else {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare1", rpc_port, int64_t);
    }

    if (OB_SUCC(ret)) {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "schema_version", schema_version, int64_t);
      if (OB_ERR_COLUMN_NOT_FOUND == ret) {
        LOG_DEBUG("can not find schema version, maybe is old server, ignore", K(ret));
        ret = OB_SUCCESS;
        schema_version = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare1", replica_type, int32_t);
        if (OB_ERR_COLUMN_NOT_FOUND == ret) {
          LOG_DEBUG("can not find spare1, maybe is old server, ignore", K(replica_type), K(ret));
          ret = OB_SUCCESS;
          replica_type = 0;
        }
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "replica_type", replica_type, int32_t);
      }
    }

    if (OB_SUCC(ret)) {
      if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare2", dup_replica_type, int32_t);
        if (OB_ERR_COLUMN_NOT_FOUND == ret) {
          LOG_DEBUG("can not find spare2, maybe is old server, ignore", K(dup_replica_type), K(ret));
          ret = OB_SUCCESS;
          dup_replica_type = 0;
        }
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "dup_replica_type", dup_replica_type, int32_t);
      }
    }

    if (OB_SUCC(ret)) {
      prl.reset();
      prl.role_ = static_cast<ObRole>(role);
      if (OB_FAIL(prl.add_addr(ip_str, port))) {
        LOG_WDIAG("invalid ip, port in fetching table entry, just skip it,"
                 " do not return err", K(ip_str), K(port), K(ret));
        ret = OB_SUCCESS;
      } else if (OB_FAIL(prl.add_rpc_addr(ip_str, rpc_port))) {
        LOG_WDIAG("invalid rpc ip, port in fetching table entry, just skip it,"
                 " do not return err", K(ip_str), K(rpc_port), K(ret));
        ret = OB_SUCCESS;
      } else if (OB_UNLIKELY(LEADER != prl.role_) && OB_UNLIKELY(FOLLOWER != prl.role_)) {
        LOG_WDIAG("invalid role in fetching table entry, just skip it,"
                 " do not return err", "role", prl.role_);
        ret = OB_SUCCESS;
      } else if (OB_FAIL(prl.set_replica_type(replica_type))) {
        LOG_INFO("invalid replica_type in fetching table entry, just skip it,"
                 " do not return err", "replica_type", replica_type);
        ret = OB_SUCCESS;
      } else if (FALSE_IT(prl.set_dup_replica_type(dup_replica_type))) {
        // can not happen
      } else if (OB_FAIL(replicas.push_back(prl))) {
        LOG_WDIAG("fail to add replica location", K(replicas), K(prl), K(ret));
      } else {
        if (prl.is_dup_replica() && !has_dup_replica) {
          has_dup_replica = true;
        }
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret) && !replicas.empty()) {
    if (table_id != table_entry.get_table_id() || part_num != table_entry.get_part_num()) {
      LOG_INFO("table id or part num is changed, this table entry is expired",
               "table names", table_entry.get_names(),
               "origin table id", table_entry.get_table_id(),
               "current table id", table_id,
               "origin part num", table_entry.get_part_num(),
               "current part num", part_num);
      if (table_entry.cas_compare_and_swap_state(ObTableEntry::AVAIL, ObTableEntry::DIRTY)) {
        LOG_INFO("mark this table entry dirty succ", K(table_entry));
      }
    } else if (table_entry.get_schema_version() > 0
               && schema_version > 0
               && table_entry.get_schema_version() != schema_version) {
      LOG_WDIAG("schema version has changed, the table entry is expired",
               K(table_entry), K(schema_version));
      if (table_entry.cas_compare_and_swap_state(ObTableEntry::AVAIL, ObTableEntry::DIRTY)) {
        LOG_INFO("mark this table entry dirty succ", K(table_entry));
      }
    } else if (OB_FAIL(ObPartitionEntry::alloc_and_init_partition_entry(table_id, partition_id,
            table_entry.get_cr_version(), table_entry.get_cr_id(), table_entry.get_tenant_id(),
            replicas, part_entry))) {
      LOG_WDIAG("fail to alloc and init partition entry", K(ret));
    } else {
      part_entry->set_schema_version(schema_version); // do not forget
      if (has_dup_replica) {
        part_entry->set_has_dup_replica();
      }
      entry = part_entry; // hand over the ref count
      part_entry = NULL;
    }
  }

  return ret;
}

int ObRouteUtils::fetch_more_partition_entrys_info(obproxy::ObResultSetFetcher &rs_fetcher,
                                            ObTableEntry &table_entry,
                                            // ObPartitionEntry *&entry,
                                            common::ObIArray<ObPartitionEntry *> &entry_array,
                                            const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0; // 仅用于填充出参，不起作用，需保证对应的字符串中间没有'\0'字符
  char ip_str[MAX_IP_ADDR_LENGTH];
  ip_str[0] = '\0';
  int64_t port = 0;
  int64_t rpc_port = 0;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t partition_id = OB_INVALID_ID;
  uint64_t last_partition_id = OB_INVALID_ID;
  int64_t part_num = 0;
  int64_t schema_version = 0;
  int64_t role = -1;
  int32_t replica_type = -1;
  int32_t dup_replica_type = 0; // 0 for non dup replica; 1 for dup replica; -1 for invalid dup replica type
  ObAddr leader_addr;
  ObPartitionEntry *part_entry = NULL;
  ObProxyReplicaLocation prl;
  ObSEArray<ObProxyReplicaLocation, 32> replicas;
  bool has_dup_replica = false;

  while ((OB_SUCC(ret)) && (OB_SUCC(rs_fetcher.next()))) {
    ip_str[0] = '\0';
    port = 0;

    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "svr_ip", ip_str, MAX_IP_ADDR_LENGTH, tmp_real_str_len);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sql_port", port, int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "table_id", table_id, uint64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "role", role, int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_num", part_num, int64_t);
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "partition_id", partition_id, uint64_t);
    } else {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "tablet_id", partition_id, uint64_t);
    }
    if (IS_CLUSTER_VERSION_BEFORE_4_3_5_2(cluster_version)) {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "svr_port", rpc_port, int64_t);
    } else {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare1", rpc_port, int64_t);
    }

    if (OB_SUCC(ret)) {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "schema_version", schema_version, int64_t);
      if (OB_ERR_COLUMN_NOT_FOUND == ret) {
        LOG_DEBUG("can not find schema version, maybe is old server, ignore", K(ret));
        ret = OB_SUCCESS;
        schema_version = 0;
      }
    }

    if (OB_SUCC(ret)) {
      if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare1", replica_type, int32_t);
        if (OB_ERR_COLUMN_NOT_FOUND == ret) {
          LOG_DEBUG("can not find spare1, maybe is old server, ignore", K(replica_type), K(ret));
          ret = OB_SUCCESS;
          replica_type = 0;
        }
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "replica_type", replica_type, int32_t);
      }
    }

    if (OB_SUCC(ret)) {
      if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare2", dup_replica_type, int32_t);
        if (OB_ERR_COLUMN_NOT_FOUND == ret) {
          LOG_DEBUG("can not find spare2, maybe is old server, ignore", K(dup_replica_type), K(ret));
          ret = OB_SUCCESS;
          dup_replica_type = 0;
        }
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "dup_replica_type", dup_replica_type, int32_t);
      }
    }

    if (OB_SUCC(ret)) {
      if (last_partition_id != partition_id && last_partition_id != OB_INVALID_PARTITION) {
        //fill the previous partition id first
        if (OB_SUCC(ret) && !replicas.empty()) {
          if (table_id != table_entry.get_table_id() || part_num != table_entry.get_part_num()) {
            LOG_INFO("table id or part num is changed, this table entry is expired",
                     "table names", table_entry.get_names(),
                     "origin table id", table_entry.get_table_id(),
                     "current table id", table_id,
                     "origin part num", table_entry.get_part_num(),
                     "current part num", part_num);
            if (table_entry.cas_compare_and_swap_state(ObTableEntry::AVAIL, ObTableEntry::DIRTY)) {
              LOG_INFO("mark this table entry dirty succ", K(table_entry));
            }
          } else if (table_entry.get_schema_version() > 0
                     && schema_version > 0
                     && table_entry.get_schema_version() != schema_version) {
            LOG_WDIAG("schema version has changed, the table entry is expired",
                     K(table_entry), K(schema_version));
            if (table_entry.cas_compare_and_swap_state(ObTableEntry::AVAIL, ObTableEntry::DIRTY)) {
              LOG_INFO("mark this table entry dirty succ", K(table_entry));
            }
          } else if (OB_FAIL(ObPartitionEntry::alloc_and_init_partition_entry(table_id, last_partition_id,
                     table_entry.get_cr_version(), table_entry.get_cr_id(), table_entry.get_tenant_id(),
                     replicas, part_entry))) {
            LOG_WDIAG("fail to alloc and init partition entry", K(ret));
          } else {
            part_entry->set_schema_version(schema_version); // do not forget
            if (has_dup_replica) {
              part_entry->set_has_dup_replica();
            }
            // entry = part_entry; // hand over the ref count
            entry_array.push_back(part_entry);
            part_entry = NULL;
          }
          replicas.reset();  //reset replicas info
        }
      }

      prl.reset();
      prl.role_ = static_cast<ObRole>(role);
      if (OB_FAIL(prl.add_addr(ip_str, port))) {
        LOG_WDIAG("invalid ip, port in fetching table entry, just skip it,"
                 " do not return err", K(ip_str), K(port), K(ret));
        ret = OB_SUCCESS;
      } else if (OB_FAIL(prl.add_rpc_addr(ip_str, rpc_port))) {
        LOG_WDIAG("invalid rpc ip, port in fetching table entry, just skip it,"
                 " do not return err", K(ip_str), K(rpc_port), K(ret));
        ret = OB_SUCCESS;
      } else if (OB_UNLIKELY(LEADER != prl.role_) && OB_UNLIKELY(FOLLOWER != prl.role_)) {
        LOG_WDIAG("invalid role in fetching table entry, just skip it,"
                 " do not return err", "role", prl.role_);
        ret = OB_SUCCESS;
      } else if (OB_FAIL(prl.set_replica_type(replica_type))) {
        LOG_INFO("invalid replica_type in fetching table entry, just skip it,"
                 " do not return err", "replica_type", replica_type);
        ret = OB_SUCCESS;
      } else if (FALSE_IT(prl.set_dup_replica_type(dup_replica_type))) {
        // can not happen
      } else if (OB_FAIL(replicas.push_back(prl))) {
        LOG_WDIAG("fail to add replica location", K(replicas), K(prl), K(ret));
      } else {
        if (prl.is_dup_replica() && !has_dup_replica) {
          has_dup_replica = true;
        }
      }
      last_partition_id = partition_id;
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret) && !replicas.empty()) {
    if (table_id != table_entry.get_table_id() || part_num != table_entry.get_part_num()) {
      LOG_INFO("table id or part num is changed, this table entry is expired",
               "table names", table_entry.get_names(),
               "origin table id", table_entry.get_table_id(),
               "current table id", table_id,
               "origin part num", table_entry.get_part_num(),
               "current part num", part_num);
      if (table_entry.cas_compare_and_swap_state(ObTableEntry::AVAIL, ObTableEntry::DIRTY)) {
        LOG_INFO("mark this table entry dirty succ", K(table_entry));
      }
    } else if (table_entry.get_schema_version() > 0
               && schema_version > 0
               && table_entry.get_schema_version() != schema_version) {
      LOG_WDIAG("schema version has changed, the table entry is expired",
               K(table_entry), K(schema_version));
      if (table_entry.cas_compare_and_swap_state(ObTableEntry::AVAIL, ObTableEntry::DIRTY)) {
        LOG_INFO("mark this table entry dirty succ", K(table_entry));
      }
    } else if (OB_FAIL(ObPartitionEntry::alloc_and_init_partition_entry(table_id, partition_id,
            table_entry.get_cr_version(), table_entry.get_cr_id(), table_entry.get_tenant_id(),
            replicas, part_entry))) {
      LOG_WDIAG("fail to alloc and init partition entry", K(ret));
    } else {
      part_entry->set_schema_version(schema_version); // do not forget
      if (has_dup_replica) {
        part_entry->set_has_dup_replica();
      }
      // entry = part_entry; // hand over the ref count
      entry_array.push_back(part_entry);
      part_entry = NULL;
    }
  }

  return ret;
}

int ObRouteUtils::get_routine_entry_sql(char *sql_buf, const int64_t buf_len,
                                        const ObTableEntryName &name,
                                        const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf)
      || OB_UNLIKELY(buf_len <= 0)
      || OB_UNLIKELY(!name.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", LITERAL_K(sql_buf), K(buf_len),
             K(name), K(ret));
  } else {
    int64_t len = 0;
    char new_tenant_name_buf[OB_MAX_TENANT_NAME_LENGTH * 2 + 1];
    ObString new_tenant_name;
    get_tenant_name(name.tenant_name_, new_tenant_name_buf, new_tenant_name);
    // call A.B or call A.B.C
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
      if (OB_UNLIKELY(!name.package_name_.empty())) {
        // TODO : add rpc if (is_route_contains_rpc_port())
        len = static_cast<int64_t>(snprintf(sql_buf, OB_SHORT_SQL_LENGTH, PROXY_ROUTINE_SCHEMA_SQL_WITH_PACKAGE,
                                            OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                            new_tenant_name.length(), new_tenant_name.ptr(),
                                            name.database_name_.length(), name.database_name_.ptr(),
                                            name.package_name_.length(), name.package_name_.ptr(),
                                            name.package_name_.length(), name.package_name_.ptr(),
                                            name.table_name_.length(), name.table_name_.ptr()));
      } else {
        len = static_cast<int64_t>(snprintf(sql_buf, OB_SHORT_SQL_LENGTH, PROXY_ROUTINE_SCHEMA_SQL,
                                            OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                            new_tenant_name.length(), new_tenant_name.ptr(),
                                            name.database_name_.length(), name.database_name_.ptr(),
                                            name.table_name_.length(), name.table_name_.ptr()));
      }
    } else {
      len = static_cast<int64_t>(snprintf(sql_buf, OB_SHORT_SQL_LENGTH, PROXY_ROUTINE_SCHEMA_SQL_V4,
                                          OB_ALL_VIRTUAL_PROXY_ROUTINE_TNAME,
                                          new_tenant_name.length(), new_tenant_name.ptr(),
                                          name.database_name_.length(), name.database_name_.ptr(),
                                          name.package_name_.length(), name.package_name_.ptr(),
                                          name.table_name_.length(), name.table_name_.ptr()));
    }

    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }

  return ret;
}

int ObRouteUtils::fetch_one_routine_entry_info(
    ObResultSetFetcher &rs_fetcher,
    const ObTableEntryName &name,
    const int64_t cr_version,
    const int64_t cr_id,
    ObRoutineEntry *&entry,
    const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  int64_t routine_type = 0;
  uint64_t routine_id = common::OB_INVALID_ID;
  int64_t schema_version = 0;
  ObString route_sql;
  ObString database_name;
  ObRoutineEntry *tmp_entry = NULL;

  if (OB_SUCC(rs_fetcher.next())) {
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "database_name", database_name);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "schema_version", schema_version, int64_t);
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "table_type", routine_type, int64_t);
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "table_id", routine_id, uint64_t);
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "spare4", route_sql);//used for route_sql
    } else {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "routine_type", routine_type, int64_t);
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "routine_id", routine_id, uint64_t);
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "routine_sql", route_sql);//used for route_sql
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret) && common::OB_INVALID_ID != routine_id) {
    if (OB_FAIL(ObRoutineEntry::alloc_and_init_routine_entry(name, cr_version, cr_id,
        route_sql, tmp_entry))) {
      LOG_WDIAG("fail to alloc and init partition entry", K(ret));
    } else {
      tmp_entry->set_is_package_database(name.package_name_ == database_name);
      tmp_entry->set_schema_version(schema_version);
      tmp_entry->set_routine_id(routine_id);
      tmp_entry->set_routine_type(routine_type);
      entry = tmp_entry; // hand over the ref count
      tmp_entry = NULL;
    }
  }
  return ret;
}

int ObRouteUtils::fetch_binlog_entry(ObResultSetFetcher &rs_fetcher,
                                     ObTableEntry &entry)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0;
  char ip[OB_IP_PORT_STR_BUFF];
  char status[32];
  int64_t port = 0;
  ObProxyReplicaLocation prl;
  ObProxyPartitionLocation *ppl = NULL;
  ObSEArray<ObProxyReplicaLocation, 1> server_list;

  while (OB_SUCC(ret) && OB_SUCC(rs_fetcher.next())) {
    ip[0] = '\0';
    status[0] = '\0';
    prl.reset();
    bool binlog_service_ok = false;

    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "ip", ip, OB_IP_PORT_STR_BUFF, tmp_real_str_len);
    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "status", status, sizeof(status), tmp_real_str_len);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "port", port, int64_t);

    if (0 == strcasecmp("OK", status)) {
      binlog_service_ok = true;
    }

    if (OB_SUCC(ret) && binlog_service_ok) {
      if (OB_FAIL(prl.add_addr(ip, port))) {
        LOG_WDIAG("invalid ip or port in fetching binlog entry", K(ret));
      } else if (OB_FAIL(server_list.push_back(prl))) {
        LOG_WDIAG("fail to add server", K(prl), K(ret));
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret) && !server_list.empty()) {
    if (OB_ISNULL(ppl = op_alloc(ObProxyPartitionLocation))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to allocate memory for ObProxyPartitionLocation", K(ret));
    } else if (OB_FAIL(ppl->set_replicas(server_list))) {
      LOG_WDIAG("fail to set replicas", K(server_list), K(ret));
    } else if (!server_list.empty() && OB_UNLIKELY(!ppl->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ppl should not unavailabe", KPC(ppl), K(ret));
    } else {
      if (ppl->is_valid()) {
        if (OB_FAIL(entry.set_first_partition_location(ppl))) {
          LOG_WDIAG("fail to set first partition location", K(ret));
        } else {
          ppl = NULL;
        }
      }
    }
  }

  if (NULL != ppl) {
    op_free(ppl);
    ppl = NULL;
  }

  return ret;
}

int ObRouteUtils::fetch_cdc_msgservice_entry(ObResultSetFetcher &rs_fetcher,
                                             ObTableEntry &entry)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0;

  const int64_t MAX_DISPLAY_STATE_LEN = 64;
  char ip[OB_IP_PORT_STR_BUFF];
  char state[MAX_DISPLAY_STATE_LEN];
  int64_t port = 0;
  ObProxyReplicaLocation prl;
  ObProxyPartitionLocation *ppl = NULL;
  ObSEArray<ObProxyReplicaLocation, 1> server_list;

  while (OB_SUCC(ret) && OB_SUCC(rs_fetcher.next())) {
    ip[0] = '\0';
    state[0] = '\0';
    prl.reset();
    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "HOST", ip, OB_IP_PORT_STR_BUFF, tmp_real_str_len);
    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "STATE", state, MAX_DISPLAY_STATE_LEN, tmp_real_str_len);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "DATA_PORT", port, int64_t);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(prl.add_addr(ip, port))) {
        LOG_WDIAG("invalid ip or port in fetching cdc msgservice entry", K(ret));
      } else if (OB_FAIL(server_list.push_back(prl))) {
        LOG_WDIAG("fail to add server", K(prl), K(ret));
      }
      LOG_DEBUG("get msgservice add", K(prl), "state", state);
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret) && !server_list.empty()) {
    if (OB_ISNULL(ppl = op_alloc(ObProxyPartitionLocation))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to allocate memory for ObProxyPartitionLocation", K(ret));
    } else if (OB_FAIL(ppl->set_replicas(server_list))) {
      LOG_WDIAG("fail to set replicas", K(server_list), K(ret));
    } else if (!server_list.empty() && OB_UNLIKELY(!ppl->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ppl should not unavailabe", KPC(ppl), K(ret));
    } else {
      // in normal case, msgservice addr won`t be more than 1
      if (OB_UNLIKELY(server_list.count() > 1)) {
        LOG_WDIAG("msgservice addr more than 1, maybe there are exceptions", K(server_list));
      }

      entry.set_allow_empty_entry(false);
      entry.set_part_num(1);
      entry.set_replica_num(1);
      entry.set_schema_version(0);
      entry.set_tenant_id(OB_INVALID_TENANT_ID);
      entry.set_table_id(0);
      entry.set_table_type(MAX_TABLE_TYPE);
      if (ppl->is_valid()) {
        if (OB_FAIL(entry.set_first_partition_location(ppl))) {
          LOG_WDIAG("fail to set first partition location", K(ret));
        } else {
          ppl = NULL;
        }
      }
    }
  }

  if (NULL != ppl) {
    op_free(ppl);
    ppl = NULL;
  }

  return ret;
}

int ObRouteUtils::get_tablegroup_entry_sql(char *sql_buf,
                                           const int64_t buf_len,
                                           int64_t tenant_id,
                                           const ObString &tablegroup_name,
                                           const ObString &database_name,
                                           const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0)
      || OB_UNLIKELY(tablegroup_name.empty()) || OB_UNLIKELY(database_name.empty())
      ||common::OB_INVALID_TENANT_ID == tenant_id || cluster_version < 4) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", LITERAL_K(sql_buf), K(buf_len),
             K(tenant_id), K(tablegroup_name), K(database_name), K(cluster_version));
  } else {
    int64_t len = 0;
    len = static_cast<int64_t>(snprintf(sql_buf, OB_SHORT_SQL_LENGTH, PROXY_TABLEGROUP_TABLES_SQL_V4,
                                        CDB_OB_TABLEGROUP_TABLES_VNAME,
                                        tenant_id,
                                        tablegroup_name.length(), tablegroup_name.ptr(),
                                        database_name.length(), database_name.ptr(),
                                        INT64_MAX));

    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }

  return ret;
}

int ObRouteUtils::fetch_one_tablegroup_entry_info(obproxy::ObResultSetFetcher &rs_fetcher,
                                                  const int64_t cr_version,
                                                  const int64_t cr_id,
                                                  ObTableGroupEntry *&entry,
                                                  const int64_t cluster_version)
{
  UNUSED(cluster_version);
  int ret = OB_SUCCESS;
  bool is_first_row = true;
  int64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t sharding_len = 0;
  int64_t database_len = 0;
  int64_t tablegroup_len = 0;
  int64_t max_sharding_len = ObTableGroupEntry::OB_TABLEGROUP_MAX_SHARDING_LENGTH;
  char sharding_str[max_sharding_len];
  char database_str[OB_MAX_DATABASE_NAME_LENGTH];
  char tablegroup_str[OB_MAX_TABLEGROUP_NAME_LENGTH];
  ObTableGroupTableNameInfo table_name_info;
  ObSEArray<ObTableGroupTableNameInfo, 4> table_names;
  ObTableGroupEntry *tmp_entry = NULL;
  sharding_str[0] = '\0';

  while ((OB_SUCC(ret)) && (OB_SUCC(rs_fetcher.next()))) {
    table_name_info.reset();

    if (is_first_row) {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "TENANT_ID", tenant_id, int64_t);
      PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "TABLEGROUP_NAME", tablegroup_str, OB_MAX_TABLEGROUP_NAME_LENGTH, tablegroup_len);
      PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "OWNER", database_str, OB_MAX_DATABASE_NAME_LENGTH, database_len);
      PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "SHARDING", sharding_str, max_sharding_len, sharding_len);
      is_first_row = false;
    }
    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "TABLE_NAME", table_name_info.table_name_, OB_MAX_TABLE_NAME_LENGTH, table_name_info.table_name_length_);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_names.push_back(table_name_info))) {
        LOG_WDIAG("fail to add table_name_info", K(table_name_info), K(ret));
      } else {
        LOG_DEBUG("succ to get table_name_info", K(table_name_info));
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret) && !table_names.empty()) {
    ObString tablegroup_name(tablegroup_len, tablegroup_str);
    ObString database_name(database_len, database_str);
    ObString sharding(sharding_len, sharding_str);
    if (OB_FAIL(ObTableGroupEntry::alloc_and_init_tablegroup_entry(tenant_id, tablegroup_name, database_name, sharding, table_names, cr_version, cr_id, tmp_entry))) {
      LOG_WDIAG("fail to alloc and init tablegroup entry", K(ret));
    } else {
      entry = tmp_entry; // hand over the ref count
      tmp_entry = NULL;
    }
  }

  return ret;
}

int ObRouteUtils::get_tablet_ls_entry_sql(char *sql_buf,
  const int64_t buf_len,
  int64_t tenant_id,
  int64_t table_id,
  const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0)
      || OB_UNLIKELY(tenant_id < 0)
      ||common::OB_INVALID_TENANT_ID == tenant_id
      || cluster_version < 4) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", LITERAL_K(sql_buf), K(buf_len),
              K(tenant_id), K(table_id), K(cluster_version));
  } else {
    int64_t len = 0;
    len = static_cast<int64_t>(snprintf(sql_buf, OB_SHORT_SQL_LENGTH, PROXY_TABLET_LS_TABLES_SQL_V4,
        OB_ALL_VIRTUAL_TABLET_TO_LS,
        tenant_id, table_id, INT64_MAX));
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }
  return ret;
}

//TODO need add for LS zdw
int ObRouteUtils::fetch_one_tablet_ls_entry_info(obproxy::ObResultSetFetcher &rs_fetcher,
         const int64_t cr_version,
         const int64_t cr_id,
         ObTabletLsEntry *&entry,
         const int64_t cluster_version)
{
  UNUSED(cluster_version);
  int ret = OB_SUCCESS;
  int64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t tablet_id = 0;
  int64_t ls_id = 0;
  int64_t table_id = 0;

  ObTabletLsEntry *tmp_entry = NULL;

  OB_TABLET_TO_LS_MAP tmp_map;
  tmp_map.create(OB_TABLET_TO_LS_MAP_BUCKET_SIZE, ObModIds::OB_PROXY_TABLET_LS_ID_MAP);
//
  while ((OB_SUCC(ret)) && (OB_SUCC(rs_fetcher.next()))) {
//  table_name_info.reset();
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "tenant_id", tenant_id,  int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "tablet_id", tablet_id,  int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "ls_id",     ls_id,      int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "table_id",  table_id,   int64_t);

    tmp_map.set_refactored(tablet_id, ls_id);
    UNUSED(tenant_id);
    UNUSED(table_id);
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret) && tmp_map.size() > 0) {
    if (OB_FAIL(ObTabletLsEntry::alloc_and_init_tablet_ls_entry(tenant_id, table_id, tmp_map, cr_version, cr_id, tmp_entry))) {
      LOG_WDIAG("fail to alloc and init tablet_ls entry", K(ret));
    } else {
      entry = tmp_entry; // hand over the ref count
      tmp_entry = NULL;
    }
  }
  return ret;
}
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
