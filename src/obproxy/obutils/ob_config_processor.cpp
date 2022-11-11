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
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX PROXY

#include "obutils/ob_config_processor.h"
#include "opsql/parser/ob_proxy_parser.h"
#include "iocore/eventsystem/ob_ethread.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "obutils/ob_proxy_stmt.h"
#include "cmd/ob_config_v2_handler.h"
#include "omt/ob_white_list_table_processor.h"
#include "omt/ob_resource_unit_table_processor.h"
#include "omt/ob_ssl_config_table_processor.h"
#include "omt/ob_proxy_config_table_processor.h"
#include "obutils/ob_vip_tenant_cache.h"
#include "utils/ob_proxy_utils.h"

using namespace obsys;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::omt;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

//the table name need to be lower case, or func'handle_dml_stmt' will get error
#define MAX_INIT_SQL_LEN 1024
static const char* sqlite3_db_name = "etc/proxyconfig.db";
static const char* all_table_version_table_name = "all_table_version";
static const char* white_list_table_name = "white_list";
static const char* resource_unit_table_name = "resource_unit";
static const char *ssl_config_table_name = "ssl_config";
static const char *proxy_config_table_name = "proxy_config";
static const char *table_name_array[] = {white_list_table_name, resource_unit_table_name, ssl_config_table_name, proxy_config_table_name};

static const char* create_all_table_version_table =
                 "create table if not exists "
                 "all_table_version(gmt_created date default (datetime('now', 'localtime')), "
                 "gmt_modified date, table_name varchar(1000), "
                 "version int default 0, primary key(table_name))";
static const char* create_white_list_table =
                 "create table if not exists "
                 "white_list(gmt_created date default (datetime('now', 'localtime')), gmt_modified date, "
                 "cluster_name varchar(256) collate nocase, tenant_name varchar(256) collate nocase, name varchar(256) collate nocase, "
                 "value text collate nocase, primary key(cluster_name, tenant_name))";
static const char* create_resource_unit_table =
                 "create table if not exists "
                 "resource_unit(gmt_created date default (datetime('now', 'localtime')), "
                 "gmt_modified date, cluster_name varchar(256) collate nocase, tenant_name varchar(256) collate nocase, "
                 "name varchar(256) collate nocase, value text collate nocase, primary key(cluster_name, tenant_name, name))";
static const char* create_ssl_config_table =
                 "create table if not exists "
                 "ssl_config(gmt_created date default (datetime('now', 'localtime')), gmt_modified date, "
                 "cluster_name varchar(256) collate nocase, tenant_name varchar(256) collate nocase, name varchar(256) collate nocase, "
                 " value text collate nocase, primary key(cluster_name, tenant_name, name))";

static const char* create_proxy_config_table =
                 "create table if not exists "
                 "proxy_config(vid int NOT NULL default -1, vip varchar(100) NOT NULL default '', "
                 "vport int NOT NULL default 0, tenant_name varchar(128) collate nocase NOT NULL default '', "
                 "cluster_name varchar(260) collate nocase NOT NULL default '', "
                 "name varchar(130) collate nocase NOT NULL, value varchar(4100) NOT NULL, info varchar(1024), "
                 "range varchar(64), need_reboot bool, visible_level varchar(20), config_level varchar(20) NOT NULL, "
                 "primary key(vid, vip, vport, tenant_name, cluster_name, name))";

ObConfigProcessor &get_global_config_processor()
{
  static ObConfigProcessor g_config_processor;
  return g_config_processor;
}

ObFnParams::ObFnParams() : stmt_type_(OBPROXY_T_INVALID),
                           table_name_(), fields_(NULL)
{}

ObFnParams::~ObFnParams() {}

ObCloudFnParams::ObCloudFnParams() : ObFnParams(), cluster_name_(), tenant_name_() {}

ObCloudFnParams::~ObCloudFnParams() {}

ObConfigProcessor::ObConfigProcessor() : proxy_config_db_(NULL), table_handler_map_()
{}

ObConfigProcessor::~ObConfigProcessor()
{
}

int ObConfigProcessor::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_handler_map_.create(32, ObModIds::OB_HASH_BUCKET))) {
    LOG_WARN("create hash map failed", K(ret));
  } else if (SQLITE_OK != sqlite3_open_v2(
    sqlite3_db_name, &proxy_config_db_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sqlite3 open failed", K(ret), "err_msg", sqlite3_errmsg(proxy_config_db_));
  } else if (OB_FAIL(check_and_create_table())) {
    LOG_WARN("check create table failed", K(ret));
  } else {
    get_global_proxy_config().proxy_config_db_ = proxy_config_db_;
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(get_global_proxy_config().dump_config_to_sqlite())) {
    LOG_WARN("dump config to sqlite3 failed", K(ret));
  } else {
    // TODO: Compatibility Check
    if (OB_FAIL(get_global_white_list_table_processor().init())) {
      LOG_WARN("white list table processor init failed", K(ret));
    } else if (OB_FAIL(get_global_resource_unit_table_processor().init())) {
      LOG_WARN("resource unit table processor init failed", K(ret));
    } else if (OB_FAIL(get_global_ssl_config_table_processor().init())) {
      LOG_WARN("ssl config table processor init failed", K(ret));
    } else if (OB_FAIL(get_global_proxy_config_table_processor().init())) {
      LOG_WARN("proxy config table processor init failed", K(ret));
    } else if (OB_FAIL(init_config_from_disk())) {
      LOG_WARN("init config from disk failed", K(ret));
    } else {
      get_global_proxy_config_table_processor().set_need_sync_to_file(true);
    }
  }

  return ret;
}

int ObConfigProcessor::init_callback(void *data, int argc, char **argv, char **column_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == data || NULL == argv || NULL == column_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is null unexpected", K(ret));
  } else {
    ObConfigHandler *handler = reinterpret_cast<ObConfigHandler*>(data);
    ObCloudFnParams params;
    params.stmt_type_ = OBPROXY_T_REPLACE;
    SqlFieldResult field_result;
    for (int64_t i = 0; OB_SUCC(ret) && i < argc; i++) {
      if (NULL == argv[i]) {
        continue;
      } else {
        SqlField sql_field;
        sql_field.column_name_.set_string(column_name[i], static_cast<int32_t>(strlen(column_name[i])));
        sql_field.value_type_ = TOKEN_STR_VAL;
        sql_field.column_value_.set_value(static_cast<int32_t>(strlen(argv[i])), argv[i]);
        if (OB_FAIL(field_result.fields_.push_back(sql_field))) {
          LOG_WARN("push back failed", K(ret));
        } else {
          field_result.field_num_++;
        }
      }
    }

    if (OB_SUCC(ret)) {
      params.fields_ = &field_result;
      const char* cluster_name_str = "cluster_name";
      const char* tenant_name_str = "tenant_name";
      for (int64_t i = 0; i < field_result.field_num_; i++) {
        SqlField &field = field_result.fields_.at(i);
        if (field.column_name_.string_ == cluster_name_str) {
          params.cluster_name_ = field.column_value_.config_string_;
        } else if (field.column_name_.string_ == tenant_name_str) {
          params.tenant_name_ = field.column_value_.config_string_;
        }
      }

      bool is_success = true;
      if (OB_FAIL(handler->execute_func_(&params))) {
        is_success = false;
        LOG_WARN("execute fn failed", K(ret));
      }

      if (OB_FAIL(handler->commit_func_(&params, is_success))) {
        LOG_WARN("commit func failed", K(ret), K(is_success));
      }
    }
  }

  return ret;
}

int ObConfigProcessor::init_config_from_disk()
{
  int ret = OB_SUCCESS;

  const char* select_sql = "select * from %.*s";
  for(int64_t i = 0; OB_SUCC(ret) && i < (sizeof(table_name_array) / sizeof(const char*)); i++) {
    const char *table_name = table_name_array[i];
    ObConfigHandler handler;
    char sql_buf[MAX_INIT_SQL_LEN];
    char *err_msg = NULL;
    int64_t len = snprintf(sql_buf, MAX_INIT_SQL_LEN, select_sql, strlen(table_name), table_name);
    if (OB_UNLIKELY(len <= 0 || len >= MAX_INIT_SQL_LEN)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generate sql faield", K(ret), K(len));
    } else if (OB_FAIL(table_handler_map_.get_refactored(table_name, handler))) {
      LOG_WARN("get handler failed", K(ret), "table_name", table_name);
    } else if (SQLITE_OK != sqlite3_exec(proxy_config_db_, sql_buf, ObConfigProcessor::init_callback, &handler, &err_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec sql failed", K(ret), "err_msg", err_msg);
      sqlite3_free(err_msg);
    }
  }
  return ret;
}

int ObConfigProcessor::execute(ObString &sql, const ObProxyBasicStmtType stmt_type, obproxy::ObConfigV2Handler *v2_handler)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("begin to execute", K(sql), K(stmt_type));
  DRWLock::WRLockGuard guard(config_lock_);
  if (OB_UNLIKELY(sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", K(ret), K(sql.length()));
  } else if (stmt_type == OBPROXY_T_SELECT) {
    if (OB_FAIL(handle_select_stmt(sql, v2_handler))) {
      LOG_WARN("handle select stmt failed", K(ret));
    }
  } else {
    ObArenaAllocator allocator;
    ParseResult parse_result;
    const int64_t EXTRA_NUM = 2;
    ObString parse_sql(sql.length() + EXTRA_NUM, sql.ptr());
    ObProxyParser obproxy_parser(allocator, NORMAL_PARSE_MODE);
    if (OB_FAIL(obproxy_parser.obparse(parse_sql, parse_result))) {
      LOG_WARN("fail to parse sql", K(sql), K(ret));
    } else if (OB_FAIL(handle_dml_stmt(sql, parse_result, allocator))) {
      LOG_WARN("handle stmt failed", K(ret), K(sql));
    }
  }

  return ret;
}

int ObConfigProcessor::check_and_create_table()
{
  int ret = OB_SUCCESS;
  char *err_msg = NULL;
  if (SQLITE_OK != sqlite3_exec(proxy_config_db_, create_all_table_version_table, NULL, 0, &err_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec create all table version table sql failed", K(ret), "err_msg", err_msg);
  } else if (SQLITE_OK != sqlite3_exec(proxy_config_db_, create_white_list_table, NULL, 0, &err_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec create white list table sql failed", K(ret), "err_msg", err_msg);
  } else if (SQLITE_OK != sqlite3_exec(proxy_config_db_, create_resource_unit_table, NULL, 0, &err_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec create resource unit table sql failed", K(ret), "err_msg", err_msg);
  } else if (SQLITE_OK != sqlite3_exec(proxy_config_db_, create_ssl_config_table, NULL, 0, &err_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec create ssl config table sql failed", K(ret), "err_msg", err_msg);
  } else if (SQLITE_OK != sqlite3_exec(proxy_config_db_, create_proxy_config_table, NULL, 0, &err_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec create proxy config table sql failed", K(ret), "err_msg", err_msg);
  }

  if (NULL != err_msg) {
    sqlite3_free(err_msg);
  }

  return ret;
}

int ObConfigProcessor::handle_dml_stmt(ObString &sql, ParseResult& parse_result, ObArenaAllocator&allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(IS_DEBUG_ENABLED())) {
    const int64_t buf_len = 64 * 1024;
    int64_t pos = 0;
    char tree_str_buf[buf_len];
    ObSqlParseResult::ob_parse_resul_to_string(parse_result, sql, tree_str_buf, buf_len, pos);
    ObString tree_str(pos, tree_str_buf);
    LOG_DEBUG("result_tree_ is \n", K(tree_str));
  }

  ParseNode* node = parse_result.result_tree_;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is unexpected null", K(ret));
  } else if (T_STMT_LIST != node->type_ || node->num_child_ < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "node_type", get_type_name(node->type_), K(node->num_child_), K(ret));
  } else if (OB_ISNULL(node = node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_DEBUG("unexpected null child", K(ret));
  } else {
    ObProxyDMLStmt *stmt = NULL;
    ObProxyInsertStmt *insert_stmt = NULL;
    ObProxyDeleteStmt *delete_stmt = NULL;
    switch(node->type_) {
      case T_INSERT:
      {
        if (OB_ISNULL(insert_stmt = op_alloc_args(ObProxyInsertStmt, allocator))) {
          LOG_WARN("alloc insert stmt failed", K(ret));
        } else if (OB_FAIL(insert_stmt->init())) {
            LOG_WARN("insert stmt init failed", K(ret));
        } else if (OB_FAIL(insert_stmt->handle_parse_result(parse_result))) {
          LOG_WARN("insert stmt handle parse result failed", K(ret));
        } else if (insert_stmt->has_unsupport_expr_type()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("insert stmt has unsupport expr type", K(ret));
        }
        stmt = insert_stmt;
        break;
      }
      case T_DELETE:
      {
        if (OB_ISNULL(delete_stmt = op_alloc_args(ObProxyDeleteStmt, allocator))) {
          LOG_WARN("alloc delete stmt failed", K(ret));
        } else if (OB_FAIL(delete_stmt->init())) {
            LOG_WARN("delete stmt init failed", K(ret));
        } else if (OB_FAIL(delete_stmt->handle_parse_result(parse_result))) {
          LOG_WARN("delete stmt handle parse result failed", K(ret));
        } else if (delete_stmt->has_unsupport_expr_type()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("insert stmt has unsupport expr type", K(ret));
        }
        stmt = delete_stmt;
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unsupported type", K(node->type_));
    }

    ObCloudFnParams params;
    bool is_success = true;
    bool is_execute = false;
    int tmp_ret = OB_SUCCESS;
    ObConfigHandler handler;
    if (OB_SUCC(ret)) {
      params.stmt_type_ = stmt->get_stmt_type();
      params.table_name_ = stmt->get_table_name();
      params.fields_ = &(stmt->get_dml_field_result());
      const char* cluster_name_str = "cluster_name";
      const char* tenant_name_str = "tenant_name";
      for (int64_t i = 0; i < params.fields_->field_num_; i++) {
        SqlField &field = params.fields_->fields_.at(i);
        if (field.column_name_.string_ == cluster_name_str) {
          params.cluster_name_ = field.column_value_.config_string_;
        } else if (field.column_name_.string_ == tenant_name_str) {
          params.tenant_name_ = field.column_value_.config_string_;
        }
      }
      //table name is UPPER CASE in stmt, need convert to lower case as origin
      //depends on the assumption that the table name in 'table_handler_map_' is lower case
      string_to_lower_case(params.table_name_.ptr(), params.table_name_.length());
      if (OB_FAIL(table_handler_map_.get_refactored(params.table_name_, handler))) {
        if (params.table_name_ == all_table_version_table_name) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get table handler failed", K_(params.table_name), K(ret));
        }
      } else if (OB_FAIL(handler.execute_func_(&params))) {
        is_execute = true;
        LOG_WARN("execute fn failed", K(ret));
      } else {
        is_execute = true;
      }
      tmp_ret = ret;
    }

    if (OB_SUCC(ret)) {
      char *err_msg = NULL;
      char *sql_buf = (char*)ob_malloc(sql.length() + 1);
      if (NULL == sql_buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(sql.length()));
      } else {
        memcpy(sql_buf, sql.ptr(), sql.length());
        sql_buf[sql.length()] = '\0';
        if (SQLITE_OK != sqlite3_exec(proxy_config_db_, sql_buf, NULL, 0, &err_msg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sqlite3 exec failed", K(sql), "err_msg", err_msg);
          sqlite3_free(err_msg);
        }
        ob_free(sql_buf);
      }
      tmp_ret = ret;
    }

    if (OB_FAIL(ret)) {
      is_success = false;
    }

    if (is_execute) {
      if (OB_FAIL(handler.commit_func_(&params, is_success))) {
        LOG_WARN("commit failed", K(ret), K(is_success));
      }
    }
    ret = (OB_SUCCESS != tmp_ret ? tmp_ret : ret);

    if (NULL != insert_stmt) {
      op_free(insert_stmt);
      insert_stmt = NULL;
    }
    if (NULL != delete_stmt) {
      op_free(delete_stmt);
      delete_stmt = NULL;
    }
  }

  return ret;
}

int ObConfigProcessor::sqlite3_callback(void *data, int argc, char **argv, char **column_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == data || NULL == argv || NULL == column_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is null unexpected", K(ret));
  } else {
    obproxy::ObConfigV2Handler *v2_handler = reinterpret_cast<obproxy::ObConfigV2Handler*>(data);
    bool need_add_column_name = (v2_handler->sqlite3_column_name_.count() == 0);
    ObSEArray<ObProxyVariantString, 4> value_array;
    for (int i = 0; i < argc; i++) {
      if (need_add_column_name) {
        ObFixSizeString<OB_MAX_COLUMN_NAME_LENGTH> name;
        name.set(column_name[i]);
        if (OB_FAIL(v2_handler->sqlite3_column_name_.push_back(name))) {
          LOG_WARN("sqlite3 column name push back failed", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        ObProxyVariantString value;
        int32_t len = (NULL == argv[i] ? 0 : static_cast<int32_t>(strlen(argv[i])));
        value.set_value(len, argv[i]);
        if (OB_FAIL(value_array.push_back(value))) {
          LOG_WARN("value array push back failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(v2_handler->sqlite3_column_value_.push_back(value_array))) {
      LOG_WARN("column value push back failed", K(ret));
    }
  }

  return ret;
}

int ObConfigProcessor::handle_select_stmt(ObString& sql, obproxy::ObConfigV2Handler *v2_handler)
{
  int ret = OB_SUCCESS;
  char *sql_str = (char*)ob_malloc(sql.length() + 1);
  if (NULL == sql_str) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(sql.length()));
  } else {
    char *err_msg = NULL;
    memcpy(sql_str, sql.ptr(), sql.length());
    sql_str[sql.length()] = '\0';

    if (SQLITE_OK != sqlite3_exec(proxy_config_db_, sql_str, sqlite3_callback, (void*)v2_handler, &err_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sqlite3 exec sql failed", K(ret), K(sql), "err_msg", err_msg);
      sqlite3_free(err_msg);
    }

    ob_free(sql_str);
  }

  return ret;
}

int ObConfigProcessor::register_callback(const common::ObString &table_name, ObConfigHandler &handler)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_handler_map_.set_refactored(table_name, handler))) {
    LOG_WARN("set refactored failed", K(ret), K(table_name));
  }

  return ret;
}

bool ObConfigProcessor::is_table_in_service(const ObString &table_name)
{
  bool is_in_service = false;
  if (table_name == all_table_version_table_name
      || table_name == white_list_table_name
      || table_name == resource_unit_table_name
      || table_name == ssl_config_table_name
      || table_name == proxy_config_table_name) {
    is_in_service = true;
  }

  return is_in_service;
}

int ObConfigProcessor::store_global_ssl_config(const ObString &name, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(name.empty() || value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(name), K(value));
  } else {
    const char *sql = "replace into ssl_config(cluster_name, tenant_name, name, value) values('*', '*', '%.*s', '%.*s')";
    int64_t buf_len = name.length() + value.length() + strlen(sql);
    char *sql_buf = (char*)ob_malloc(buf_len + 1, ObModIds::OB_PROXY_CONFIG_TABLE);
    if (OB_ISNULL(sql_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(buf_len));
    } else {
      ObString sql_string;
      int64_t len = 0;
      len = static_cast<int64_t>(snprintf(sql_buf, buf_len, sql,
                                 name.length(), name.ptr(),
                                 value.length(), value.ptr()));
      sql_string.assign_ptr(sql_buf, static_cast<int32_t>(len));
      if (OB_UNLIKELY(len <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to fill sql", K(len), K(ret));
      } else if (OB_FAIL(execute(sql_string, OBPROXY_T_REPLACE, NULL))) {
        LOG_WARN("execute sql failed", K(ret));
      }
    }

    if (OB_NOT_NULL(sql_buf)) {
      ob_free(sql_buf);
    }
  }

  return ret;
}

int ObConfigProcessor::store_global_proxy_config(const ObString &name, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(name), K(value));
  } else {
    const char *sql = "replace into proxy_config(name, value, config_level) values('%.*s', '%.*s', 'LEVEL_GLOBAL')";
    int64_t buf_len = name.length() + value.length() + strlen(sql);
    char *sql_buf = (char*)ob_malloc(buf_len + 1, ObModIds::OB_PROXY_CONFIG_TABLE);
    if (OB_ISNULL(sql_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(buf_len));
    } else {
      ObString sql_string;
      int64_t len = 0;
      get_global_proxy_config_table_processor().set_need_sync_to_file(false);
      len = static_cast<int64_t>(snprintf(sql_buf, buf_len, sql,
                                 name.length(), name.ptr(),
                                 value.length(), value.ptr()));
      sql_string.assign_ptr(sql_buf, static_cast<int32_t>(len));
      if (OB_UNLIKELY(len <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to fill sql", K(len), K(ret));
      } else if (OB_FAIL(execute(sql_string, OBPROXY_T_REPLACE, NULL))) {
        LOG_WARN("execute sql failed", K(ret));
      }
      get_global_proxy_config_table_processor().set_need_sync_to_file(true);
    }

    if (OB_NOT_NULL(sql_buf)) {
      ob_free(sql_buf);
    }
  }

  return ret;
}

int ObConfigProcessor::get_proxy_config_with_level(const ObVipAddr &addr, const common::ObString &cluster_name,
                       const common::ObString &tenant_name, const common::ObString& name,
                       common::ObConfigItem &ret_item, const ObString level, bool &found)
{
  int ret = OB_SUCCESS;
  ObProxyConfigItem proxy_item;
  if (OB_FAIL(get_global_proxy_config_table_processor().get_config_item(
      addr, cluster_name, tenant_name, name, proxy_item))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get config item failed", K(addr), K(cluster_name), K(tenant_name), K(name), K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (0 != strcasecmp(proxy_item.config_level_.ptr(), level.ptr())) {
    ret = OB_SUCCESS;
    LOG_DEBUG("vip config level not match", K(proxy_item));
  } else {
    found = true;
    ret_item = proxy_item.config_item_;
  }

  return ret;
}

int ObConfigProcessor::get_proxy_config(const ObVipAddr &addr, const ObString &cluster_name,
                                        const ObString &tenant_name, const ObString& name,
                                        ObConfigItem &ret_item)
{
  int ret = OB_SUCCESS;
  ObVipAddr tmp_addr = addr;
  ObString tmp_cluster_name = cluster_name;
  ObString tmp_tenant_name = tenant_name;
  bool found = false;
  if (OB_FAIL(get_proxy_config_with_level(tmp_addr, tmp_cluster_name, tmp_tenant_name, name, ret_item, "LEVEL_VIP", found))) {
    LOG_WARN("get_proxy_config_with_level failed", K(ret));
  }

  if (OB_SUCC(ret) && !found) {
    tmp_addr.reset();
    if (OB_FAIL(get_proxy_config_with_level(tmp_addr, tmp_cluster_name, tmp_tenant_name, name, ret_item, "LEVEL_TENANT", found))) {
      LOG_WARN("get_proxy_config_with_level failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && !found) {
    tmp_tenant_name.reset();
    if (OB_FAIL(get_proxy_config_with_level(tmp_addr, tmp_cluster_name, tmp_tenant_name, name, ret_item, "LEVEL_CLUSTER", found))) {
      LOG_WARN("get_proxy_config_with_level failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && !found) {
    if (OB_FAIL(get_global_proxy_config().get_config_item(name, ret_item))) {
      LOG_WARN("get global config item failed", K(ret));
    }
  }

  LOG_DEBUG("get proxy config", K(ret_item), K(found), K(ret));

  return ret;
}

int ObConfigProcessor::get_proxy_config_bool_item(const ObVipAddr &addr, const ObString &cluster_name,
                                                  const ObString &tenant_name, const ObString& name,
                                                  ObConfigBoolItem &ret_item)
{
  int ret = OB_SUCCESS;
  ObConfigItem item;
  if (OB_FAIL(get_proxy_config(addr, cluster_name, tenant_name, name, item))) {
    LOG_WARN("get proxy config failed", K(addr), K(cluster_name), K(tenant_name), K(name), K(ret));
  } else {
    ret_item.set(item.str());
    LOG_DEBUG("get bool item succ", K(ret_item));
  }

  return ret;
}

} // end of obutils
} // end of obproxy
} // end of oceanbase
