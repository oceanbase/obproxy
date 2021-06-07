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

#include "proxy/route/ob_route_utils.h"
#include "proxy/mysqllib/ob_resultset_fetcher.h"
#include "proxy/route/ob_table_processor.h"
#include "proxy/route/ob_partition_processor.h"
#include "proxy/route/ob_routine_processor.h"
#include "proxy/route/ob_mysql_route.h"
#include "proxy/route/ob_partition_entry.h"
#include "proxy/route/ob_table_cache.h"
#include "proxy/route/ob_table_entry.h"
#include "proxy/route/ob_routine_cache.h"
#include "proxy/route/ob_routine_entry.h"
#include "proxy/route/ob_tenant_server.h"
#include "opsql/expr_resolver/ob_expr_resolver.h"
#include "opsql/func_expr_parser/ob_func_expr_parser.h"
#include "opsql/func_expr_parser/ob_func_expr_parser_utils.h"
#include "obutils/ob_proxy_sql_parser.h"

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

static const char PART_KEY_EXTRA_SEPARATOR               = ';';

static const char *PROXY_PLAIN_SCHEMA_SQL                =
    //svr_ip, sql_port, table_id, role, part_num, replica_num, spare1
    "SELECT /*+READ_CONSISTENCY(WEAK)%s*/ * "
    "FROM oceanbase.%s "
    "WHERE tenant_name = '%.*s' AND database_name = '%.*s' AND table_name = '%.*s' "
    "AND partition_id = %ld "
    "ORDER BY role ASC LIMIT %ld";
static const char *PROXY_TENANT_SCHEMA_SQL               =
    //svr_ip, sql_port, table_id, role, part_num, replica_num
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ * "
    "FROM oceanbase.%s "
    "WHERE tenant_name = '%.*s' AND database_name = '%.*s' AND table_name = '%.*s' AND sql_port > 0 "
    "ORDER BY partition_id ASC, role ASC LIMIT %ld";

static const char *PROXY_PART_INFO_SQL                   =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ template_num, part_level, part_num, part_type, part_space, part_expr, "
    "part_interval_bin, interval_start_bin, sub_part_num, sub_part_type, sub_part_space, "
    "sub_part_expr, def_sub_part_interval_bin, def_sub_interval_start_bin, "
    "part_key_num, part_key_name, part_key_type, part_key_idx, part_key_level, part_key_extra, "
    "spare1, spare2, spare4 "
    "FROM oceanbase.%s "
    "WHERE table_id = %lu LIMIT %d;";

static const char *PROXY_FIRST_PART_SQL                  =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_id, part_name, high_bound_val_bin, sub_part_num "
    "FROM oceanbase.%s "
    "WHERE table_id = %lu LIMIT %ld;";

// observer 2.1.1 do not have high_bound_val_bin, so use two different sql
static const char *PROXY_HASH_FIRST_PART_SQL             =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_id, part_name, sub_part_num "
    "FROM oceanbase.%s "
    "WHERE table_id = %lu LIMIT %ld;";

static const char *PROXY_SUB_PART_SQL                    =
  "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_id, sub_part_id, high_bound_val_bin "
  "FROM oceanbase.%s "
  "WHERE table_id = %lu and part_id = %ld LIMIT %ld;";

static const char *PROXY_NON_TEMPLATE_SUB_PART_SQL       =
  "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_id, sub_part_id, high_bound_val_bin "
  "FROM oceanbase.%s "
  "WHERE table_id = %lu LIMIT %ld;";

static const char *PROXY_ROUTINE_SCHEMA_SQL              =
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

int ObRouteUtils::get_table_entry_sql(char *sql_buf, const int64_t buf_len,
                                      ObTableEntryName &name,
                                      bool is_need_force_flush /*false*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(!name.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", LITERAL_K(sql_buf), K(buf_len), K(name), K(ret));
  } else {
    int64_t len = 0;
    if (name.is_all_dummy_table()) {
      len = static_cast<int64_t>(snprintf(sql_buf, buf_len, PROXY_TENANT_SCHEMA_SQL,
                                          OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                          name.tenant_name_.length(), name.tenant_name_.ptr(),
                                          name.database_name_.length(), name.database_name_.ptr(),
                                          name.table_name_.length(), name.table_name_.ptr(),
                                          INT64_MAX));
    } else {
      const int64_t FIRST_PARTITION_ID = 0;
      len = static_cast<int64_t>(snprintf(sql_buf, buf_len, PROXY_PLAIN_SCHEMA_SQL,
                                          is_need_force_flush ? ", FORCE_REFRESH_LOCATION_CACHE" : "",
                                          OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                          name.tenant_name_.length(), name.tenant_name_.ptr(),
                                          name.database_name_.length(), name.database_name_.ptr(),
                                          name.table_name_.length(), name.table_name_.ptr(),
                                          FIRST_PARTITION_ID, INT64_MAX));
    }

    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }

  return ret;
}

int ObRouteUtils::get_part_info_sql(char *sql_buf,
                                    const int64_t buf_len,
                                    const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", LITERAL_K(sql_buf), K(buf_len), K(ret));
  } else {
    int64_t len = snprintf(sql_buf, buf_len, PROXY_PART_INFO_SQL,
                           OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TNAME,
                           table_id,
                           OB_MAX_PARTITION_KEY_COLUMN_NUMBER);
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }

  return ret;
}

int ObRouteUtils::get_first_part_sql(char *sql_buf,
                                     const int64_t buf_len,
                                     const uint64_t table_id,
                                     const bool is_hash_part)
{
  int ret = OB_SUCCESS;

  int64_t len = 0;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", LITERAL_K(sql_buf), K(buf_len), K(ret));
  } else {
    len = snprintf(sql_buf, buf_len, is_hash_part ? PROXY_HASH_FIRST_PART_SQL : PROXY_FIRST_PART_SQL,
                   OB_ALL_VIRTUAL_PROXY_PARTITION_TNAME,
                   table_id,
                   INT64_MAX);
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }

  return ret;
}

int ObRouteUtils::get_sub_part_sql(char *sql_buf,
                                   const int64_t buf_len,
                                   const uint64_t table_id,
                                   const bool is_template_table)
{
  int ret = OB_SUCCESS;

  int64_t len = 0;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", LITERAL_K(sql_buf), K(buf_len), K(ret));
  } else {
    // assume all sub_part are same for each fisrt part
    // templete part id is -1
    if (is_template_table) {
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
      LOG_WARN("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
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
                                    ObTableEntry &entry)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0;
  char ip_str[OB_IP_STR_BUFF];
  ip_str[0] = '\0';
  int64_t port = 0;
  uint64_t table_id = OB_INVALID_ID;
  int64_t part_num = 0;
  int64_t replica_num = 0;
  int64_t schema_version = 0;
  int64_t role = -1;
  int32_t replica_type = -1;
  int32_t table_type = -1;
  ObProxyReplicaLocation prl;
  ObProxyPartitionLocation *ppl = NULL;
  ObTenantServer *pts = NULL;
  ObSEArray<ObProxyReplicaLocation, 32> server_list;
  const bool is_dummy_entry = entry.is_dummy_entry();
  bool use_fake_addrs = false;

  while ((OB_SUCC(ret)) && (OB_SUCC(rs_fetcher.next()))) {
    ip_str[0] = '\0';
    port = 0;
    prl.reset();
    role = -1;
    replica_type = -1;
    table_type = -1;

    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "svr_ip", ip_str, OB_IP_STR_BUFF, tmp_real_str_len);
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
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare1", replica_type, int32_t);
      if (OB_ERR_COLUMN_NOT_FOUND == ret) {
        LOG_DEBUG("can not find spare1, maybe is old server, ignore", K(replica_type), K(ret));
        ret = OB_SUCCESS;
        replica_type = 0;
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
      prl.role_ = static_cast<ObRole>(role);
      if (OB_FAIL(prl.add_addr(ip_str, port))) {
        if (is_fake_ip_port(ip_str, port)) {
          use_fake_addrs = true;
        } else {
          LOG_WARN("invalid ip, port in fetching table entry, just skip it,"
                   " do not return err", K(ip_str), K(port), K(ret));
        }
        ret = OB_SUCCESS;
      } else if (OB_UNLIKELY(LEADER != prl.role_) && OB_UNLIKELY(FOLLOWER != prl.role_)) {
        LOG_WARN("invalid role in fetching table entry, just skip it,"
                 " do not return err", "role", prl.role_);
        ret = OB_SUCCESS;
      } else if (OB_FAIL(prl.set_replica_type(replica_type))) {
        LOG_WARN("invalid replica_type in fetching table entry, just skip it,"
                 " do not return err", "replica_type", replica_type);
        ret = OB_SUCCESS;
      } else if (OB_FAIL(server_list.push_back(prl))) {
        LOG_WARN("fail to add server", K(prl), K(ret));
      }//end of else
    }//end of OB_SUCC(ret)
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret) && is_dummy_entry) {
    if (!server_list.empty()) {
      if (OB_ISNULL(pts = op_alloc(ObTenantServer))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for ObTenantServer", K(ret));
      } else if (OB_FAIL(pts->init(server_list))) {
        LOG_WARN("fail to init tenant servers", KPC(pts), K(server_list), K(ret));
      } else if (OB_UNLIKELY(!pts->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pts should not unavailable", KPC(pts), K(ret));
      } else if (OB_FAIL(entry.set_tenant_servers(pts))) {
        LOG_WARN("fail to set tenant servers", KPC(pts), K(ret));
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
        LOG_WARN("fail to allocate memory for ObProxyPartitionLocation", K(ret));
      } else if (OB_FAIL(ppl->set_replicas(server_list))) {
        LOG_WARN("fail to set replicas", K(server_list), K(ret));
      } else if (!server_list.empty() && OB_UNLIKELY(!ppl->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ppl should not unavailable", KPC(ppl), K(ret));
      } else {
        const bool is_empty_entry_allowed = (use_fake_addrs && server_list.empty());
        entry.set_allow_empty_entry(is_empty_entry_allowed);
        entry.set_part_num(part_num);
        entry.set_replica_num(replica_num);
        entry.set_schema_version(schema_version);
        entry.set_table_id(table_id);
        entry.set_table_type(table_type);
        if (entry.is_non_partition_table() && ppl->is_valid()) {
          if (OB_FAIL(entry.set_first_partition_location(ppl))) {
            LOG_WARN("fail to set first partition location", K(ret));
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

int ObRouteUtils::fetch_part_info(ObResultSetFetcher &rs_fetcher, ObProxyPartInfo &part_info)
{
  int ret = OB_SUCCESS;

  ObPartitionLevel part_level = PARTITION_LEVEL_ONE;
  int64_t part_key_num = 1;
  int64_t template_num = 1;

  // init part key info
  part_info.get_part_key_info().key_num_ = 0;
  for (int64_t i = 0; i < part_key_num && OB_SUCC(ret); ++i) {
    // get first row
    if (OB_FAIL(rs_fetcher.next())) {
      LOG_WARN("fail to fetch part info", K(ret));
    } else {
      // get basic info ONLY for the first line
      if (0 == i) {
        // get part level
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "template_num", template_num, int64_t);
        if (template_num != 0 && template_num != 1) {
          ret = OB_INVALID_ARGUMENT_FOR_EXTRACT;
          LOG_WARN("part level is invalid", K(template_num), K(ret));
        } else {
          part_info.set_template_table(1 == template_num);
        }

        // get part level
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_level", part_level, ObPartitionLevel);
        if (part_level < PARTITION_LEVEL_ONE || part_level > PARTITION_LEVEL_TWO) {
          ret = OB_INVALID_ARGUMENT_FOR_EXTRACT;
          LOG_WARN("part level is invalid", K(part_level), K(ret));
        } else {
          part_info.set_part_level(part_level);
        }

        // get part key num
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_key_num", part_key_num, int64_t);
        if (part_key_num > OBPROXY_MAX_PART_KEY_NUM) {
          part_key_num = OBPROXY_MAX_PART_KEY_NUM;
        }

        if (OB_FAIL(fetch_part_option(rs_fetcher, part_info))) {
          LOG_WARN("fail to get part option", K(ret));
        }
      } else {
        // do nothing here
      }

      // get part key info for EACH line
      if (OB_SUCC(ret)) {
        if (OB_FAIL(fetch_part_key(rs_fetcher, part_info))) {
          LOG_WARN("fail to get part key", K(ret));
        } // end of if
      } // end of if (OB_SUCC(ret))
    } // end of else
  } // end of for

  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_part_desc(part_info))) {
      LOG_WARN("fail to build part desc ", K(part_info), K(ret));
    }
  }
  return ret;
}

int ObRouteUtils::build_part_desc(ObProxyPartInfo &part_info) {
  int ret = OB_SUCCESS;
  ObProxyPartOption &sub_part_opt = part_info.get_sub_part_option();

  // for first part, we will build part desc in fetch_first_part
  if (part_info.has_unknown_part_key()) {
    LOG_INFO("part key type is unsopported, no need build part_desc", K(part_info));
  } else if (!part_info.is_template_table()) {
    LOG_INFO("part table is non-template table, build hash and key part_desc later", K(part_info));
  } else if (OB_FAIL(build_part_desc(part_info, PARTITION_LEVEL_TWO, sub_part_opt))) {
    LOG_WARN("fail to build sub part", K(sub_part_opt), K(ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObRouteUtils::build_part_desc(ObProxyPartInfo &part_info,
                                  const ObPartitionLevel part_level,
                                  ObProxyPartOption &part_opt) {
  int ret = OB_SUCCESS;
  ObProxyPartMgr &part_mgr = part_info.get_part_mgr();

  if (part_level != PARTITION_LEVEL_TWO) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(part_level), K(ret));
  } else {
    if (part_opt.is_hash_part()) {
      if (OB_FAIL(part_mgr.build_hash_part(part_info.is_oracle_mode(),
                                           part_level,
                                           part_opt.part_func_type_,
                                           part_opt.part_num_,
                                           part_opt.part_space_,
                                           part_info.is_template_table(),
                                           part_info.get_part_key_info()))) {
        LOG_WARN("fail to build hash part", K(part_opt), K(ret));
      }
    } else if (part_opt.is_key_part()) {
      if (OB_FAIL(part_mgr.build_key_part(part_level,
                                          part_opt.part_func_type_,
                                          part_opt.part_num_,
                                          part_opt.part_space_,
                                          part_info.is_template_table(),
                                          part_info.get_part_key_info()))) {
        LOG_WARN("fail to build key part", K(part_opt), K(ret));
      }
    } else {
      // we will build range part desc when fetch the range column
    }
  }
  return ret;
}

inline int ObRouteUtils::fetch_part_key(ObResultSetFetcher &rs_fetcher,
                                        ObProxyPartInfo &part_info)
{
  int ret = OB_SUCCESS;

  ObProxyPartKeyInfo &part_key_info = part_info.get_part_key_info();
  ObIAllocator &allocator = part_info.get_allocator();
  ObPartitionLevel part_key_level = PARTITION_LEVEL_ONE;
  int64_t part_key_idx = -1;
  ObObjType part_key_type = ObMaxType;
  ObCollationType part_key_cs_type = CS_TYPE_INVALID;
  ObString part_key_name;
  ObString part_key_extra;
  ObString constraint_part_key;
  int64_t idx_in_rowid = -1;

  PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_key_level", part_key_level, ObPartitionLevel);
  // part key idx is the order of part key in all columns
  PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_key_idx", part_key_idx, int64_t);
  PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_key_type", part_key_type, ObObjType);
  PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_key_name", part_key_name);
  // use part_key_extra as generated key expr
  PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_key_extra", part_key_extra);
  PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "spare4", constraint_part_key);
  // use spare1 as table collation type
  PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare1", part_key_cs_type, ObCollationType);
  // use spare2 as rowid index
  PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare2", idx_in_rowid, int64_t);

  if (!is_obj_type_supported(part_key_type)) {
    part_info.set_unknown_part_key(true);
  }

  if (PARTITION_LEVEL_ONE == part_key_level) {
    part_key_info.part_keys_[part_key_info.key_num_].level_ = PART_KEY_LEVEL_ONE;
  } else if (PARTITION_LEVEL_TWO == part_key_level) {
    part_key_info.part_keys_[part_key_info.key_num_].level_ = PART_KEY_LEVEL_TWO;
  } else {
    ret = OB_INVALID_ARGUMENT_FOR_EXTRACT;
    LOG_WARN("part key level is invalid", K(part_key_level), K(ret));
  }

  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(part_key_name.length())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allc part key name", K(buf), K(part_key_name.length()), K(ret));
  } else {
    memcpy(buf, part_key_name.ptr(), part_key_name.length());
  }

  if (OB_SUCC(ret)) {
    part_key_info.part_keys_[part_key_info.key_num_].idx_ = part_key_idx;
    part_key_info.part_keys_[part_key_info.key_num_].name_.str_len_ = part_key_name.length();
    part_key_info.part_keys_[part_key_info.key_num_].name_.str_ = buf;
    part_key_info.part_keys_[part_key_info.key_num_].obj_type_ = part_key_type;
    part_key_info.part_keys_[part_key_info.key_num_].idx_in_rowid_ = idx_in_rowid;
    if (CS_TYPE_INVALID == part_key_cs_type) {
      part_key_info.part_keys_[part_key_info.key_num_].cs_type_ =
        ObCharset::get_default_collation(ObCharset::get_default_charset());
    } else {
      part_key_info.part_keys_[part_key_info.key_num_].cs_type_ = part_key_cs_type;
    }

    if (!part_key_extra.empty() || !constraint_part_key.empty()) {
      part_key_info.part_keys_[part_key_info.key_num_].is_generated_ = true;
      part_info.set_has_generated_key(true);
      int64_t generated_key_idx = part_key_info.key_num_;
      ++part_key_info.key_num_;
      if (OB_FAIL(add_generated_part_key(part_key_extra, generated_key_idx, part_info))) {
        LOG_WARN("fail to add generated key", K(part_key_extra), K(ret));
      }
      const char *sep_pos = NULL;
      ObString tmp_str;
      while (NULL != (sep_pos = (constraint_part_key.find(PART_KEY_EXTRA_SEPARATOR)))) {
        tmp_str = constraint_part_key.split_on(sep_pos);
        if (OB_FAIL(add_generated_part_key(tmp_str, generated_key_idx, part_info))) {
          LOG_WARN("fail to add generated key", K(tmp_str), K(ret));
        }
      }
      if (OB_FAIL(add_generated_part_key(constraint_part_key, generated_key_idx, part_info))) {
        LOG_WARN("fail to add generated key", K(constraint_part_key), K(ret));
      }
    } else {
      part_key_info.part_keys_[part_key_info.key_num_].is_generated_ = false;
      ++part_key_info.key_num_;
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
    ObProxyParamNodeList *child = NULL;
    if (OB_UNLIKELY(part_key_info.key_num_ >= OBPROXY_MAX_PART_KEY_NUM)) {
      LOG_INFO("part key num is larger than OBPROXY_MAX_PART_KEY_NUM, we do not support",
               K_(part_key_info.key_num), K(OBPROXY_MAX_PART_KEY_NUM));
    } else if (OB_FAIL(parser.parse(part_key_extra, result))) {
      LOG_WARN("fail to parse generated function expr", K(ret), K(part_key_extra));
    } else if (OB_ISNULL(result.param_node_)
               || PARAM_FUNC != result.param_node_->type_
               || OB_ISNULL(func_expr_node = result.param_node_->func_expr_node_)
               || OB_ISNULL(child = func_expr_node->child_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generated function must be a func", "param node", reinterpret_cast<const void*>(result.param_node_),
               "type", result.param_node_->type_, KP(func_expr_node), KP(child), K(ret));
    } else if (OB_UNLIKELY(child->child_num_ > OBPROXY_MAX_PARAM_NUM)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generated function param num is unexpectedlly larger than 3",
               "child num", child->child_num_, K(OBPROXY_MAX_PARAM_NUM), K(ret));
    } else {
      LOG_DEBUG("succ to parse generated function expr", "func_expr_result:", ObFuncExprParseResultPrintWrapper(result));
      part_key_info.part_keys_[part_key_info.key_num_].func_type_ = func_expr_node->func_type_;
      part_key_info.part_keys_[part_key_info.key_num_].param_num_ = child->child_num_;
      part_key_info.part_keys_[part_key_info.key_num_].generated_col_idx_ = generated_key_idx;
      memset(part_key_info.part_keys_[part_key_info.key_num_].params_, 0, OBPROXY_MAX_PARAM_NUM);
      ObProxyParamNode* child_param_node = child->head_;
      for (int64_t i = 0; i < child->child_num_ && OB_SUCC(ret); ++i) {
        if (OB_ISNULL(child_param_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("generated function param is null", K(ret));
        } else {
          part_key_info.part_keys_[part_key_info.key_num_].params_[i] = child_param_node;
          if (PARAM_COLUMN == child_param_node->type_) {
            // add src key into part_key_info
            part_key_info.part_keys_[part_key_info.key_num_].name_ = child_param_node->col_name_;
            part_key_info.part_keys_[part_key_info.key_num_].level_ = part_key.level_;
            part_key_info.part_keys_[part_key_info.key_num_].obj_type_ = part_key.obj_type_;
            part_key_info.part_keys_[part_key_info.key_num_].cs_type_ = part_key.cs_type_;
            part_key_info.part_keys_[part_key_info.key_num_].is_generated_ = false;
          }
          child_param_node = child_param_node->next_;
        }
      }
      ++part_key_info.key_num_;
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

int ObRouteUtils::fetch_first_part(ObResultSetFetcher &rs_fetcher, ObProxyPartInfo &part_info)
{
  int ret = OB_SUCCESS;
  if (part_info.get_first_part_option().is_range_part()) {
    if (OB_FAIL(part_info.get_part_mgr().build_range_part(share::schema::PARTITION_LEVEL_ONE,
                                                          part_info.get_first_part_option().part_func_type_,
                                                          part_info.get_first_part_option().part_num_,
                                                          part_info.is_template_table(),
                                                          rs_fetcher))) {
      LOG_WARN("fail to build range part", K(ret));
    }
  } else if (part_info.get_first_part_option().is_list_part()) {
    if (OB_FAIL(part_info.get_part_mgr().build_list_part(share::schema::PARTITION_LEVEL_ONE,
                                                         part_info.get_first_part_option().part_func_type_,
                                                         part_info.get_first_part_option().part_num_,
                                                         part_info.is_template_table(),
                                                         rs_fetcher))) {
      LOG_WARN("fail to build list part", K(ret));
    }
  } else if (part_info.get_first_part_option().is_hash_part()) {
    if (OB_FAIL(part_info.get_part_mgr().build_hash_part(part_info.is_oracle_mode(),
                                                         share::schema::PARTITION_LEVEL_ONE,
                                                         part_info.get_first_part_option().part_func_type_,
                                                         part_info.get_first_part_option().part_num_,
                                                         part_info.get_first_part_option().part_space_,
                                                         part_info.is_template_table(),
                                                         part_info.get_part_key_info(),
                                                         &rs_fetcher))) {
      LOG_WARN("fail to build hash part", K(ret));
    }
  } else if (part_info.get_first_part_option().is_key_part()) {
    if (OB_FAIL(part_info.get_part_mgr().build_key_part(share::schema::PARTITION_LEVEL_ONE,
                                                        part_info.get_first_part_option().part_func_type_,
                                                        part_info.get_first_part_option().part_num_,
                                                        part_info.get_first_part_option().part_space_,
                                                        part_info.is_template_table(),
                                                        part_info.get_part_key_info(),
                                                        &rs_fetcher))) {
      LOG_WARN("fail to build key part", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not support part type", "part_type", part_info.get_first_part_option().part_func_type_,
             K(ret));
  }
  return ret;
}

int ObRouteUtils::fetch_sub_part(ObResultSetFetcher &rs_fetcher, ObProxyPartInfo &part_info)
{
  int ret = OB_SUCCESS;
  if (part_info.get_sub_part_option().is_range_part()) {
    if (part_info.is_template_table()) {
      if (OB_FAIL(part_info.get_part_mgr().build_range_part(share::schema::PARTITION_LEVEL_TWO,
                                                            part_info.get_sub_part_option().part_func_type_,
                                                            part_info.get_sub_part_option().part_num_,
                                                            part_info.is_template_table(),
                                                            rs_fetcher))) {
        LOG_WARN("fail to build range part", K(ret));
      }
    } else {
      if (OB_FAIL(part_info.get_part_mgr().build_sub_range_part_with_non_template(part_info.get_sub_part_option().part_func_type_,
                                                                                  rs_fetcher))) {
        LOG_WARN("fail to build range part", K(ret));
      }
    }
  } else if (part_info.get_sub_part_option().is_list_part()) {
    if (part_info.is_template_table()) {
      if (OB_FAIL(part_info.get_part_mgr().build_list_part(share::schema::PARTITION_LEVEL_TWO,
                                                           part_info.get_sub_part_option().part_func_type_,
                                                           part_info.get_sub_part_option().part_num_,
                                                           part_info.is_template_table(),
                                                           rs_fetcher))) {
        LOG_WARN("fail to build list part", K(ret));
      }
    } else {
      if (OB_FAIL(part_info.get_part_mgr().build_sub_list_part_with_non_template(part_info.get_sub_part_option().part_func_type_,
                                                                                 rs_fetcher))) {
        LOG_WARN("fail to build range part", K(ret));
      }
    }
  } else if (part_info.get_sub_part_option().is_hash_part()) {
    if (part_info.is_template_table()) {
      if (OB_FAIL(part_info.get_part_mgr().build_hash_part(part_info.is_oracle_mode(),
                                                           share::schema::PARTITION_LEVEL_TWO,
                                                           part_info.get_sub_part_option().part_func_type_,
                                                           part_info.get_sub_part_option().part_num_,
                                                           part_info.get_sub_part_option().part_space_,
                                                           part_info.is_template_table(),
                                                           part_info.get_part_key_info(),
                                                           &rs_fetcher))) {
        LOG_WARN("fail to build hash part", K(ret));
      }
    } else {
      if (OB_FAIL(part_info.get_part_mgr().build_sub_hash_part_with_non_template(part_info.is_oracle_mode(),
                                                                                 part_info.get_sub_part_option().part_func_type_,
                                                                                 part_info.get_sub_part_option().part_space_,
                                                                                 part_info.get_part_key_info(),
                                                                                 rs_fetcher))) {
        LOG_WARN("fail to build range part", K(ret));
      }
    }
  } else if (part_info.get_sub_part_option().is_key_part()) {
    if (part_info.is_template_table()) {
      if (OB_FAIL(part_info.get_part_mgr().build_key_part(share::schema::PARTITION_LEVEL_TWO,
                                                          part_info.get_sub_part_option().part_func_type_,
                                                          part_info.get_sub_part_option().part_num_,
                                                          part_info.get_sub_part_option().part_space_,
                                                          part_info.is_template_table(),
                                                          part_info.get_part_key_info(),
                                                          &rs_fetcher))) {
        LOG_WARN("fail to build key part", K(ret));
      }
    } else {
      if (OB_FAIL(part_info.get_part_mgr().build_sub_key_part_with_non_template(part_info.get_sub_part_option().part_func_type_,
                                                                                part_info.get_sub_part_option().part_space_,
                                                                                part_info.get_part_key_info(),
                                                                                rs_fetcher))) {
        LOG_WARN("fail to build range part", K(ret));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not support part type", "part_type", part_info.get_sub_part_option().part_func_type_,
             K(ret));
  }
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
    LOG_WARN("invalid input value", K(cluster_name), K(cluster_id), K(rs_list), K(ret));
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
      LOG_WARN("fail to alloc and init table entry", K(name), K(cluster_id), K(ret));
    } else if (OB_ISNULL(tenant_servers = op_alloc(ObTenantServer))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for ObProxyTenantServers", K(ret));
    } else if (OB_FAIL(tenant_servers->init(rs_list))) {
      LOG_WARN("fail to init tenant servers", K(rs_list), K(ret));
    } else if (OB_UNLIKELY(!tenant_servers->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant_server should not unavailable", KPC(tenant_servers), K(ret));
    } else if (OB_FAIL(entry->set_tenant_servers(tenant_servers))) {
      LOG_WARN("fail to set tenant servers", K(tenant_servers), K(ret));
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


int ObRouteUtils::build_and_add_sys_dummy_entry(const common::ObString &cluster_name,
                                                const int64_t cluster_id,
                                                const LocationList &locaiton_list,
                                                const bool is_rslist)
{
  int ret = OB_SUCCESS;
  ObTableEntry *entry = NULL;
  ObTableCache &table_cache = get_global_table_cache();
  if (OB_FAIL(build_sys_dummy_entry(cluster_name, cluster_id, locaiton_list, is_rslist, entry))) {
    LOG_WARN("fail to build sys dummy entry", K(cluster_name), K(cluster_id), K(locaiton_list), K(ret));
  } else if (OB_ISNULL(entry) || OB_UNLIKELY(!entry->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entry can not be NULL here", KPC(entry), K(ret));
  } else {
    entry->inc_ref();
    ObTableEntry *tmp_entry = entry;
    bool direct_add = (NULL == this_ethread()) ? true : false;
    if (OB_FAIL(table_cache.add_table_entry(*tmp_entry, direct_add))) {
      LOG_WARN("fail to add table entry", KPC(tmp_entry), K(ret));
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

int ObRouteUtils::build_and_add_sys_dummy_entry(const common::ObString &cluster_name,
                                                const int64_t cluster_id,
                                                const ObIArray<ObAddr> &addr_list,
                                                const bool is_rslist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cluster_name.empty() || addr_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input vlue", K(addr_list), K(cluster_name), K(ret));
  } else {
    LocationList location_list;
    if (OB_FAIL(convert_addrs_to_locations(addr_list, location_list))) {
      LOG_WARN("fail to convert_addrs_to_locations", K(ret));
    } else if (OB_FAIL(build_and_add_sys_dummy_entry(cluster_name, cluster_id, location_list, is_rslist))) {
      LOG_WARN("fail to build_and_add_sys_dummy_entry", K(cluster_name), K(cluster_id), K(location_list), K(ret));
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
    LOG_WARN("invalid input vlue", K(addr_list), K(ret));
  } else {
    ObProxyReplicaLocation prl;
    for (int64_t i = 0; (i < addr_list.count()) && OB_SUCC(ret); ++i) {
      prl.reset();
      prl.server_ = addr_list.at(i);
      prl.role_ = FOLLOWER;
      if (OB_FAIL(location_list.push_back(prl))) {
        LOG_WARN("fail to push back replica location", K(prl), K(ret));
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
    LOG_WARN("invalid route param", K(route_param), K(ret));
  } else {
    table_param.name_.shallow_copy(route_param.name_);
    table_param.current_idc_name_ = route_param.current_idc_name_;//shallow copy
    table_param.force_renew_ = route_param.force_renew_;
    table_param.mysql_proxy_ = route_param.mysql_proxy_;
    table_param.cr_version_ = route_param.cr_version_;
    table_param.cr_id_ = route_param.cr_id_;
    table_param.tenant_version_ = route_param.tenant_version_;
    table_param.is_partition_table_route_supported_ = route_param.is_partition_table_route_supported_;
    table_param.is_oracle_mode_ = route_param.is_oracle_mode_;
    table_param.is_need_force_flush_ = route_param.is_need_force_flush_;
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
    LOG_WARN("invalid route param", K(route_param), K(ret));
  } else {
    routine_param.name_.shallow_copy(route_param.name_);
    routine_param.current_idc_name_ = route_param.current_idc_name_;//shallow copy
    routine_param.force_renew_ = route_param.force_renew_;
    routine_param.mysql_proxy_ = route_param.mysql_proxy_;
    routine_param.cr_version_ = route_param.cr_version_;
    routine_param.cr_id_ = route_param.cr_id_;
  }
  return ret;
}

int ObRouteUtils::get_partition_entry_sql(char *sql_buf, const int64_t buf_len,
                                          const ObTableEntryName &name,
                                          const uint64_t partition_id,
                                          bool is_need_force_flush)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf) || OB_UNLIKELY(buf_len <= 0)
      || OB_UNLIKELY(!name.is_valid()) || common::OB_INVALID_ID == partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", LITERAL_K(sql_buf), K(buf_len),
             K(name), K(partition_id), K(ret));
  } else {
    int64_t len = 0;
    len = static_cast<int64_t>(snprintf(sql_buf, OB_SHORT_SQL_LENGTH, PROXY_PLAIN_SCHEMA_SQL,
                                        is_need_force_flush ? ", FORCE_REFRESH_LOCATION_CACHE" : "",
                                        OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                        name.tenant_name_.length(), name.tenant_name_.ptr(),
                                        name.database_name_.length(), name.database_name_.ptr(),
                                        name.table_name_.length(), name.table_name_.ptr(),
                                        partition_id, INT64_MAX));

    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }

  return ret;
}

int ObRouteUtils::fetch_one_partition_entry_info(
      ObResultSetFetcher &rs_fetcher,
      ObTableEntry &table_entry,
      ObPartitionEntry *&entry)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0;
  char ip_str[OB_IP_STR_BUFF];
  ip_str[0] = '\0';
  int64_t port = 0;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t partition_id = OB_INVALID_ID;
  int64_t part_num = 0;
  int64_t schema_version = 0;
  int64_t role = -1;
  int32_t replica_type = -1;
  ObAddr leader_addr;
  ObPartitionEntry *part_entry = NULL;
  ObProxyReplicaLocation prl;
  ObSEArray<ObProxyReplicaLocation, 32> replicas;

  while ((OB_SUCC(ret)) && (OB_SUCC(rs_fetcher.next()))) {
    ip_str[0] = '\0';
    port = 0;

    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "svr_ip", ip_str, OB_IP_STR_BUFF, tmp_real_str_len);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sql_port", port, int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "table_id", table_id, uint64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "partition_id", partition_id, uint64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "role", role, int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_num", part_num, int64_t);

    if (OB_SUCC(ret)) {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "schema_version", schema_version, int64_t);
      if (OB_ERR_COLUMN_NOT_FOUND == ret) {
        LOG_DEBUG("can not find schema version, maybe is old server, ignore", K(ret));
        ret = OB_SUCCESS;
        schema_version = 0;
      }
    }

    if (OB_SUCC(ret)) {
      PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "spare1", replica_type, int32_t);
      if (OB_ERR_COLUMN_NOT_FOUND == ret) {
        LOG_DEBUG("can not find spare1, maybe is old server, ignore", K(replica_type), K(ret));
        ret = OB_SUCCESS;
        replica_type = 0;
      }
    }

    if (OB_SUCC(ret)) {
      prl.reset();
      prl.role_ = static_cast<ObRole>(role);
      if (OB_FAIL(prl.add_addr(ip_str, port))) {
        LOG_WARN("invalid ip, port in fetching table entry, just skip it,"
                 " do not return err", K(ip_str), K(port), K(ret));
        ret = OB_SUCCESS;
      } else if (OB_UNLIKELY(LEADER != prl.role_) && OB_UNLIKELY(FOLLOWER != prl.role_)) {
        LOG_WARN("invalid role in fetching table entry, just skip it,"
                 " do not return err", "role", prl.role_);
        ret = OB_SUCCESS;
      } else if (OB_FAIL(prl.set_replica_type(replica_type))) {
        LOG_INFO("invalid replica_type in fetching table entry, just skip it,"
                 " do not return err", "replica_type", replica_type);
        ret = OB_SUCCESS;
      } else if (OB_FAIL(replicas.push_back(prl))) {
        LOG_WARN("fail to add replica location", K(replicas), K(prl), K(ret));
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
      LOG_WARN("schema version has changed, the table entry is expired",
               K(table_entry), K(schema_version));
      if (table_entry.cas_compare_and_swap_state(ObTableEntry::AVAIL, ObTableEntry::DIRTY)) {
        LOG_INFO("mark this table entry dirty succ", K(table_entry));
      }
    } else if (OB_FAIL(ObPartitionEntry::alloc_and_init_partition_entry(table_id, partition_id,
            table_entry.get_cr_version(), table_entry.get_cr_id(), replicas, part_entry))) {
      LOG_WARN("fail to alloc and init partition entry", K(ret));
    } else {
      part_entry->set_schema_version(schema_version); // do not forget
      entry = part_entry; // hand over the ref count
      part_entry = NULL;
    }
  }

  return ret;
}

int ObRouteUtils::get_routine_entry_sql(char *sql_buf, const int64_t buf_len,
                                        const ObTableEntryName &name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf)
      || OB_UNLIKELY(buf_len <= 0)
      || OB_UNLIKELY(!name.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", LITERAL_K(sql_buf), K(buf_len),
             K(name), K(ret));
  } else {
    int64_t len = 0;
    // call A.B or call A.B.C
    if (OB_UNLIKELY(!name.package_name_.empty())) {
      len = static_cast<int64_t>(snprintf(sql_buf, OB_SHORT_SQL_LENGTH, PROXY_ROUTINE_SCHEMA_SQL_WITH_PACKAGE,
                                          OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                          name.tenant_name_.length(), name.tenant_name_.ptr(),
                                          name.database_name_.length(), name.database_name_.ptr(),
                                          name.package_name_.length(), name.package_name_.ptr(),
                                          name.package_name_.length(), name.package_name_.ptr(),
                                          name.table_name_.length(), name.table_name_.ptr()));
    } else {
      len = static_cast<int64_t>(snprintf(sql_buf, OB_SHORT_SQL_LENGTH, PROXY_ROUTINE_SCHEMA_SQL,
                                          OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                                          name.tenant_name_.length(), name.tenant_name_.ptr(),
                                          name.database_name_.length(), name.database_name_.ptr(),
                                          name.table_name_.length(), name.table_name_.ptr()));
    }

    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(sql_buf), K(len), K(buf_len), K(ret));
    }
  }

  return ret;
}

int ObRouteUtils::fetch_one_routine_entry_info(
    ObResultSetFetcher &rs_fetcher,
    const ObTableEntryName &name,
    const int64_t cr_version,
    const int64_t cr_id,
    ObRoutineEntry *&entry)
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
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "table_type", routine_type, int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "table_id", routine_id, uint64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "schema_version", schema_version, int64_t);
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "spare4", route_sql);//used for route_sql
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret) && common::OB_INVALID_ID != routine_id) {
    if (OB_FAIL(ObRoutineEntry::alloc_and_init_routine_entry(name, cr_version, cr_id,
        route_sql, tmp_entry))) {
      LOG_WARN("fail to alloc and init partition entry", K(ret));
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

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
