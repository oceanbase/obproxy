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
// sqlite限制了一条sql最多插入多少个元素：500，考虑稳定性，设置为上限为200
#define MAX_INSERT_SQL_ROW_NUM 200
// sqlite本身对sql长度有限制，考虑到一条sql太长可能有性能影响，限制长度不超过1MB
#define MAX_INSERT_SQL_LENGTH  (1024 * 1024)
static const char* sqlite3_db_name = "proxyconfig_v1.db";
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
                           table_name_(), fields_(NULL),
                           row_index_(0), sync_master_failed_(false)
{}

ObFnParams::~ObFnParams() {}

ObConfigProcessor::ObConfigProcessor() : init_need_commit_(false), proxy_config_db_(NULL), table_handler_map_()
{}

ObConfigProcessor::~ObConfigProcessor()
{
}

int ObConfigProcessor::init()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start init ObConfigProcessor");
  if (OB_FAIL(table_handler_map_.create(32, ObModIds::OB_HASH_BUCKET))) {
    LOG_WDIAG("create hash map failed", K(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(get_global_proxy_config().dump_config_to_sqlite())) {
    LOG_WDIAG("dump config to sqlite3 failed", K(ret));
  } else if (OB_FAIL(get_global_white_list_table_processor().init())) {
    LOG_WDIAG("white list table processor init failed", K(ret));
  } else if (OB_FAIL(get_global_resource_unit_table_processor().init())) {
    LOG_WDIAG("resource unit table processor init failed", K(ret));
  } else if (OB_FAIL(get_global_ssl_config_table_processor().init())) {
    LOG_WDIAG("ssl config table processor init failed", K(ret));
  } else if (OB_FAIL(get_global_proxy_config_table_processor().init())) {
    LOG_WDIAG("proxy config table processor init failed", K(ret));
  } else if (OB_FAIL(init_config_from_disk())) {
    LOG_WDIAG("init config from disk failed", K(ret));
  } else {
    get_global_proxy_config_table_processor().set_need_sync_to_file(true);
  }

  return ret;
}

int ObConfigProcessor::init_callback(void *data, int argc, char **argv, char **column_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == data || NULL == argv || NULL == column_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("argument is null unexpected", K(ret));
  } else {
    ObConfigHandler *handler = reinterpret_cast<ObConfigHandler*>(data);
    ObFnParams params;
    params.stmt_type_ = OBPROXY_T_REPLACE;
    SqlFieldResult field_result;
    for (int64_t i = 0; OB_SUCC(ret) && i < argc; i++) {
      if (NULL == argv[i]) {
        continue;
      } else {
        SqlField *sql_field = NULL;
        if (OB_FAIL(SqlField::alloc_sql_field(sql_field))) {
          LOG_WDIAG("fail to alloc sql field", K(ret));
        } else {
          SqlColumnValue column_value;
          if (!sql_field->column_name_.set_value(static_cast<int32_t>(strlen(column_name[i])), column_name[i])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to set column name value",  K(ret));
          } else if (!column_value.column_value_.set_value(static_cast<int32_t>(strlen(argv[i])), argv[i])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to set column name value", K(ret));
          } else {
            column_value.value_type_ = TOKEN_STR_VAL;
            if (OB_FAIL(sql_field->column_values_.push_back(column_value))) {
              LOG_WDIAG("fail to push back column value", K(ret));
            } else if (OB_FAIL(field_result.fields_.push_back(sql_field))) {
              sql_field->reset();
              sql_field = NULL;
              LOG_WDIAG("fail to push back sql fields", K(ret));
            } else {
              field_result.field_num_++;
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      params.fields_ = &field_result;

      if (OB_SUCC(ret)) {
        bool is_success = true;
        if (OB_FAIL(handler->execute_func_(&params))) {
          is_success = false;
          LOG_WDIAG("execute fn failed", K(ret));
        } else if (OB_ISNULL(handler->before_commit_func_)) {
          // do nothing
        } else if (OB_FAIL(handler->before_commit_func_(get_global_config_processor().get_sqlite_db(), &params, is_success, 1))) {
          LOG_WDIAG("fail to before commit func", K(is_success), K_(params.table_name), K(ret));
        }

        if (OB_FAIL(handler->commit_func_(&params, is_success))) {
          LOG_WDIAG("commit func failed", K(ret), K(is_success));
        }
      }
    }
  }

  return ret;
}

int ObConfigProcessor::init_config_from_disk()
{
  int ret = OB_SUCCESS;

  const char* select_sql = "select * from %.*s";
  int sqlite_err_code = SQLITE_OK;
  char *err_msg = NULL;
  for(int64_t i = 0; OB_SUCC(ret) && i < (sizeof(table_name_array) / sizeof(const char*)); i++) {
    const char *table_name = table_name_array[i];
    ObConfigHandler handler;
    char sql_buf[MAX_INIT_SQL_LEN];
    int64_t len = snprintf(sql_buf, MAX_INIT_SQL_LEN, select_sql, strlen(table_name), table_name);
    if (OB_UNLIKELY(len <= 0 || len >= MAX_INIT_SQL_LEN)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("generate sql faield", K(ret), K(len));
    } else if (OB_FAIL(table_handler_map_.get_refactored(table_name, handler))) {
      LOG_WDIAG("get handler failed", K(ret), "table_name", table_name);
    } else if (SQLITE_OK != (sqlite_err_code = sqlite3_exec(proxy_config_db_, sql_buf, ObConfigProcessor::init_callback, &handler, &err_msg))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("exec select sql failed", K(sqlite_err_code), K(proxy_config_db_), K(ret), "err_msg", err_msg);
      sqlite3_free(err_msg);
    }
  }
  return ret;
}

int ObConfigProcessor::execute(ObString &sql,
                               const ObProxyBasicStmtType stmt_type,
                               obproxy::ObConfigV2Handler *v2_handler,
                               const bool need_change_sync_file)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("begin to execute", K(sql), K(stmt_type));
  DRWLock::WRLockGuard guard(config_lock_);
  if (need_change_sync_file) {
    get_global_proxy_config_table_processor().set_need_sync_to_file(false);
  }
  if (OB_UNLIKELY(sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid sql", K(ret), K(sql.length()));
  } else if (sql.length() > MAX_INSERT_SQL_LENGTH) {
    ret = OB_NOT_SUPPORTED;
    LOG_WDIAG("proxy config not supported SQL length greater than 1MB", K(sql.length()), K(MAX_INSERT_SQL_LENGTH), K(ret));
  } else if (stmt_type == OBPROXY_T_SELECT) {
    if (OB_FAIL(handle_select_stmt(sql, v2_handler))) {
      LOG_WDIAG("handle select stmt failed", K(ret));
    }
  } else {
    ObArenaAllocator allocator;
    ParseResult parse_result;
    const int64_t EXTRA_NUM = 2;
    ObString parse_sql(sql.length() + EXTRA_NUM, sql.ptr());
    ObProxyParser obproxy_parser(allocator, NORMAL_PARSE_MODE);
    if (OB_FAIL(obproxy_parser.obparse(parse_sql, parse_result))) {
      LOG_WDIAG("fail to parse sql", K(sql), K(ret));
    } else if (OB_FAIL(handle_dml_stmt(sql, parse_result, allocator))) {
      LOG_WDIAG("handle stmt failed", K(ret), K(sql));
    }
  }

  if (need_change_sync_file) {
    get_global_proxy_config_table_processor().set_need_sync_to_file(true);
  }

  return ret;
}

int ObConfigProcessor::check_and_create_table()
{
  int ret = OB_SUCCESS;
  char *err_msg = NULL;
  const char *begin_execlusive = "BEGIN EXCLUSIVE TRANSACTION;";
  int sqlite_errcode = SQLITE_OK;
  const uint sleep_time_us = 100 * 1000;  // 100ms
  uint64_t try_lock_count = 0;
  LOG_INFO("start check_and_create_table");
  // 每隔100ms尝试上锁，尝试10次（1s）不成功，进程退出
  while (SQLITE_BUSY == (sqlite_errcode = sqlite3_exec(proxy_config_db_, begin_execlusive, NULL, 0, &err_msg))
         && ++try_lock_count <= 10) {
    LOG_INFO("exec execlusive sql failed, maybe other process is read/write sqlite",
              K(ret), K(sqlite_errcode), K(try_lock_count), "err_msg", err_msg);
    if (OB_NOT_NULL(err_msg)) {
      sqlite3_free(err_msg);
      err_msg = NULL;
    }
    usleep(sleep_time_us);
  }
  init_need_commit_ = (SQLITE_OK == sqlite_errcode);

  if (SQLITE_OK != sqlite_errcode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to lock sqltie", K(try_lock_count), K(sqlite_errcode), K(ret));
  } else if (SQLITE_OK != sqlite3_exec(proxy_config_db_, create_all_table_version_table, NULL, 0, &err_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("exec create all table version table sql failed", K(ret), "err_msg", err_msg);
  } else if (SQLITE_OK != sqlite3_exec(proxy_config_db_, create_white_list_table, NULL, 0, &err_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("exec create white list table sql failed", K(ret), "err_msg", err_msg);
  } else if (SQLITE_OK != sqlite3_exec(proxy_config_db_, create_resource_unit_table, NULL, 0, &err_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("exec create resource unit table sql failed", K(ret), "err_msg", err_msg);
  } else if (SQLITE_OK != sqlite3_exec(proxy_config_db_, create_ssl_config_table, NULL, 0, &err_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("exec create ssl config table sql failed", K(ret), "err_msg", err_msg);
  } else if (SQLITE_OK != sqlite3_exec(proxy_config_db_, create_proxy_config_table, NULL, 0, &err_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("exec create proxy config table sql failed", K(ret), "err_msg", err_msg);
  }

  if (NULL != err_msg) {
    sqlite3_free(err_msg);
  }

  return ret;
}

int ObConfigProcessor::parse_and_resolve_config(ParseResult& parse_result, const ObString &sql, ObArenaAllocator&allocator,
                                                int64_t &row_num, ObProxyDMLStmt *&stmt,
                                                ObProxyInsertStmt *&insert_stmt, ObProxyDeleteStmt *&delete_stmt,
                                                ObFnParams &params, ObConfigHandler &handler)
{
  int ret = OB_SUCCESS;
  insert_stmt = NULL;
  delete_stmt = NULL;
  row_num = 1;
  ParseNode* node = parse_result.result_tree_;
  if (OB_UNLIKELY(IS_DEBUG_ENABLED())) {
    const int64_t buf_len = 64 * 1024;
    int64_t pos = 0;
    char tree_str_buf[buf_len];
    ObSqlParseResult::ob_parse_resul_to_string(parse_result, sql, tree_str_buf, buf_len, pos);
    ObString tree_str(pos, tree_str_buf);
    LOG_DEBUG("result_tree_ is \n", K(tree_str));
  }

  // 1. 解析node，判断sql类型
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("node is unexpected null", K(ret));
  } else if (T_STMT_LIST != node->type_ || node->num_child_ < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", "node_type", get_type_name(node->type_), K(node->num_child_), K(ret));
  } else if (OB_ISNULL(node = node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_DEBUG("unexpected null child", K(ret));
  } else {
    switch(node->type_) {
      case T_INSERT:
      {
        if (OB_ISNULL(insert_stmt = op_alloc_args(ObProxyInsertStmt, allocator))) {
          LOG_WDIAG("alloc insert stmt failed", K(ret));
        } else if (OB_FAIL(insert_stmt->init())) {
            LOG_WDIAG("insert stmt init failed", K(ret));
        } else if (OB_FAIL(insert_stmt->handle_parse_result(parse_result))) {
          LOG_WDIAG("insert stmt handle parse result failed", K(ret));
        } else if (insert_stmt->has_unsupport_expr_type()
                   || insert_stmt->has_unsupport_expr_type_for_config()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WDIAG("insert stmt has unsupport expr type", K(ret));
        } else {
          row_num = insert_stmt->get_row_count();
          if (row_num > MAX_INSERT_SQL_ROW_NUM || OB_UNLIKELY(row_num <= 0)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WDIAG("proxy config not supported insert rows greater than MAX_INSERT_SQL_ROW_NUM", K(row_num), K(MAX_INSERT_SQL_ROW_NUM), K(ret));
          }
        }
        stmt = insert_stmt;
        break;
      }
      case T_DELETE:
      {
        if (OB_ISNULL(delete_stmt = op_alloc_args(ObProxyDeleteStmt, allocator))) {
          LOG_WDIAG("alloc delete stmt failed", K(ret));
        } else if (OB_FAIL(delete_stmt->init())) {
            LOG_WDIAG("delete stmt init failed", K(ret));
        } else if (OB_FAIL(delete_stmt->handle_parse_result(parse_result))) {
          LOG_WDIAG("delete stmt handle parse result failed", K(ret));
        } else if (delete_stmt->has_unsupport_expr_type()
                   || delete_stmt->has_unsupport_expr_type_for_config()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WDIAG("insert stmt has unsupport expr type", K(ret));
        }
        stmt = delete_stmt;
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unsupported type", K(node->type_));
    }
  }

  // 2. 将解析结果保存到params中
  if (OB_SUCC(ret)) {
    params.stmt_type_ = stmt->get_stmt_type();
    params.table_name_ = stmt->get_table_name();
    params.fields_ = &(stmt->get_dml_field_result());
    // table name is UPPER CASE in stmt, need convert to lower case as origin
    // depends on the assumption that the table name in 'table_handler_map_'
    // is lower case
    string_to_lower_case(params.table_name_.ptr(), params.table_name_.length());
    if (OB_FAIL(table_handler_map_.get_refactored(params.table_name_, handler))) {
      if (params.table_name_ == all_table_version_table_name) {
        ret = OB_SUCCESS;
      } else {
        LOG_WDIAG("get table handler failed", K_(params.table_name), K(ret));
      }
    }
  }
  return ret;
}

int ObConfigProcessor::execute_and_commit_config(const ObString &sql, const ObConfigHandler &handler, ObFnParams &params, const int64_t row_num) {
  int ret = OB_SUCCESS;
  bool is_success = true;
  int tmp_ret = OB_SUCCESS;
  char *err_msg = NULL;

  // 1. 开启begin，执行sql
  if (OB_SUCC(ret)) {
    char *sql_buf = (char *)ob_malloc(sql.length() + 1);
    if (NULL == sql_buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("allocate memory failed", K(ret), K(sql.length()));
    } else {
      memcpy(sql_buf, sql.ptr(), sql.length());
      sql_buf[sql.length()] = '\0';
      int sqlite_err_code = SQLITE_OK;
      if (SQLITE_OK != (sqlite_err_code = sqlite3_exec(proxy_config_db_, "begin;", NULL, 0, &err_msg))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("sqlite3 exec begin failed", K(sqlite_err_code), K(proxy_config_db_), K(ret), K(sql), "err_msg", err_msg);
        sqlite3_free(err_msg);
      }

      if (OB_SUCC(ret) && SQLITE_OK != (sqlite_err_code = sqlite3_exec(proxy_config_db_, sql_buf,
                                                    NULL, 0, &err_msg))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("sqlite3 exec sql failed", K(sqlite_err_code), K(proxy_config_db_), K(sql), "err_msg", err_msg);
        sqlite3_free(err_msg);
      }
      ob_free(sql_buf);
    }
    tmp_ret = ret;
  }

  // 2. 执行execute和commit，all table version只在sqlite修改，不在内存修改
  if (OB_SUCC(ret) && params.table_name_ != all_table_version_table_name) {
    // 2.1 执行sql成功，进入此函数，先依次执行备份内存的每行数据的修改：如果某行数据写入失败，退出，只污染备份，sql回滚;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_num; ++i) {
      params.row_index_ = i;
      if (OB_FAIL(handler.execute_func_(&params))) {
        is_success = false;
        LOG_WDIAG("fail to execute fn", K(row_num), K(i), K(ret));
      }
    }
    // 2.2 before_commit_sql：先执行execute中需要继续执行的修改，再写主内存
    if (OB_SUCC(ret) && NULL != handler.before_commit_func_) {
      // 2.2.1 先执行execute遗留的更改（全局配置同步，执行sql），目前只有proxy_config表需要before_commit
      // 2.2.2 再将配置同步写到主内存
      if (OB_FAIL(handler.before_commit_func_(proxy_config_db_, &params, is_success, row_num))) {
        LOG_WDIAG("fail to before commit func",
                  K(row_num), K(is_success), K_(params.table_name), K(ret));
      } else {
        // do nothing
      }
    }
    tmp_ret = ret;
    ret = OB_SUCCESS;

    // 3. commit：处理失败回滚，成功则提交
    if (OB_FAIL(handler.commit_func_(&params, is_success))) {
      LOG_WDIAG("fail to commit func", K(ret), K(row_num), K(is_success));
    }
  }

  // 最后提交事务
  ret = (!is_success ? tmp_ret : ret);
  const char *end_sql = (is_success ? "commit;" : "rollback;");
  int sqlite_err_code = SQLITE_OK;
  if (SQLITE_OK != (sqlite_err_code = sqlite3_exec(proxy_config_db_, end_sql, NULL, 0, &err_msg))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("sqlite3 exec commit or rollback failed", K(sqlite_err_code), K(proxy_config_db_), K(sql), "err_msg", err_msg);
    sqlite3_free(err_msg);
  }
  return ret;
}

int ObConfigProcessor::handle_dml_stmt(ObString &sql, ParseResult& parse_result, ObArenaAllocator&allocator)
{
  int ret = OB_SUCCESS;
  int64_t row_num = 1;
  ObProxyDMLStmt *stmt = NULL;
  ObProxyInsertStmt *insert_stmt = NULL;
  ObProxyDeleteStmt *delete_stmt = NULL;
  ObFnParams params;
  ObConfigHandler handler;

  if (OB_FAIL(parse_and_resolve_config(parse_result, sql, allocator, row_num, stmt, insert_stmt, delete_stmt, params, handler))) {
    LOG_WDIAG("fail to parser and resolve config", K(ret));
  } else if (OB_FAIL(execute_and_commit_config(sql, handler, params, row_num))) {
    LOG_WDIAG("fail to execute and commit config", K(ret));
  }
  // 释放parse_and_resolve_config申请的内存
  if (NULL != insert_stmt) {
    op_free(insert_stmt);
    insert_stmt = NULL;
  }
  if (NULL != delete_stmt) {
    op_free(delete_stmt);
    delete_stmt = NULL;
  }

  return ret;
}

int ObConfigProcessor::sqlite3_callback(void *data, int argc, char **argv, char **column_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == data || NULL == argv || NULL == column_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("argument is null unexpected", K(ret));
  } else {
    obproxy::ObConfigV2Handler *v2_handler = reinterpret_cast<obproxy::ObConfigV2Handler*>(data);
    bool need_add_column_name = (v2_handler->sqlite3_column_name_.count() == 0);
    ObSEArray<ObProxyVariantString, 4> value_array;
    for (int i = 0; i < argc; i++) {
      if (need_add_column_name) {
        ObFixSizeString<OB_MAX_COLUMN_NAME_LENGTH> name;
        name.set(column_name[i]);
        if (OB_FAIL(v2_handler->sqlite3_column_name_.push_back(name))) {
          LOG_WDIAG("sqlite3 column name push back failed", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        ObProxyVariantString value;
        int32_t len = (NULL == argv[i] ? 0 : static_cast<int32_t>(strlen(argv[i])));
        value.set_value(len, argv[i]);
        if (OB_FAIL(value_array.push_back(value))) {
          LOG_WDIAG("value array push back failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(v2_handler->sqlite3_column_value_.push_back(value_array))) {
      LOG_WDIAG("column value push back failed", K(ret));
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
    LOG_WDIAG("allocate memory failed", K(ret), K(sql.length()));
  } else {
    char *err_msg = NULL;
    memcpy(sql_str, sql.ptr(), sql.length());
    sql_str[sql.length()] = '\0';

    if (SQLITE_OK != sqlite3_exec(proxy_config_db_, sql_str, sqlite3_callback, (void*)v2_handler, &err_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("sqlite3 exec sql failed", K(ret), K(sql), "err_msg", err_msg);
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
    LOG_WDIAG("set refactored failed", K(ret), K(table_name));
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

int ObConfigProcessor::execute(const char* sql, int (*callback)(void*, int, char**, char**), void* handler)
{
  int ret = OB_SUCCESS;
  char *err_msg = NULL;
  if (SQLITE_OK != sqlite3_exec(proxy_config_db_, sql, callback, handler, &err_msg)) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("exec sql failed", K(ret), "err_msg", err_msg);
    sqlite3_free(err_msg);
  }
  return ret;
}

int ObConfigProcessor::store_global_ssl_config(const ObString &name, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(name.empty() || value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(name), K(value));
  } else {
    const char *sql = "replace into ssl_config(cluster_name, tenant_name, name, value) values('*', '*', '%.*s', '%.*s')";
    int64_t buf_len = name.length() + value.length() + strlen(sql);
    char *sql_buf = (char*)ob_malloc(buf_len + 1, ObModIds::OB_PROXY_CONFIG_TABLE);
    if (OB_ISNULL(sql_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("allocate memory failed", K(ret), K(buf_len));
    } else {
      ObString sql_string;
      int64_t len = 0;
      len = static_cast<int64_t>(snprintf(sql_buf, buf_len, sql,
                                 name.length(), name.ptr(),
                                 value.length(), value.ptr()));
      sql_string.assign_ptr(sql_buf, static_cast<int32_t>(len));
      if (OB_UNLIKELY(len <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to fill sql", K(len), K(ret));
      } else if (OB_FAIL(execute(sql_string, OBPROXY_T_REPLACE, NULL))) {
        LOG_WDIAG("execute sql failed", K(ret));
      }
    }

    if (OB_NOT_NULL(sql_buf)) {
      ob_free(sql_buf);
    }
  }

  return ret;
}

int ObConfigProcessor::store_proxy_config_with_level(int64_t vid, const ObString &vip, int64_t vport,
                                                     const ObString &tenant_name, const ObString &cluster_name,
                                                     const ObString &name, const ObString& value, const ObString &level)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(name.empty() || level.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(name), K(level));
  } else {
    const char *sql =
      "replace into proxy_config(vid, vip, vport, cluster_name, tenant_name, name, value, config_level) "
      "values(%ld, '%.*s', %ld, '%.*s', '%.*s', '%.*s', '%.*s', '%.*s')";
    char vid_buf[64];
    char vport_buf[64];
    int32_t vid_len = snprintf(vid_buf, sizeof(vid_buf), "%ld", vid);
    int32_t vport_len = snprintf(vport_buf, sizeof(vport_buf), "%ld", vport);
    int64_t buf_len = vid_len + vport_len + vip.length() + tenant_name.length() + cluster_name.length() + name.length() + value.length() + level.length() + strlen(sql);
    char *sql_buf = (char*)ob_malloc(buf_len + 1, ObModIds::OB_PROXY_CONFIG_TABLE);
    if (OB_ISNULL(sql_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("allocate memory failed", K(ret), K(buf_len));
    } else {
      ObString sql_string;
      int32_t len = 0;
      len = static_cast<int32_t>(snprintf(sql_buf, buf_len, sql,
                                 vid,
                                 vip.length(), vip.ptr(),
                                 vport,
                                 cluster_name.length(), cluster_name.ptr(),
                                 tenant_name.length(), tenant_name.ptr(),
                                 name.length(), name.ptr(),
                                 value.length(), value.ptr(),
                                 level.length(), level.ptr()));
      sql_string.assign_ptr(sql_buf, len);
      LOG_DEBUG("execute sql", K(sql_string));
      if (OB_UNLIKELY(len <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to fill sql", K(len), K(ret));
      } else if (OB_FAIL(execute(sql_string, OBPROXY_T_REPLACE, NULL, true))) {
        LOG_WDIAG("execute sql failed", K(ret));
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
  if (OB_FAIL(store_proxy_config_with_level(-1, "", 0, "", "", name, value, "LEVEL_GLOBAL"))) {
    LOG_WDIAG("store_proxy_config_with_level failed", K(ret));
  }
  return ret;
}

int ObConfigProcessor::get_proxy_config_with_level(const ObVipAddr &addr, const common::ObString &cluster_name,
                       const common::ObString &tenant_name, const common::ObString& name,
                       common::ObConfigItem &ret_item, const ObString level,
                       bool &found, const bool lock_required/*true*/)
{
  int ret = OB_SUCCESS;
  ObProxyConfigItem proxy_item;
  if (OB_FAIL(get_global_proxy_config_table_processor().get_config_item(
      addr, cluster_name, tenant_name, name, proxy_item, lock_required))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WDIAG("get config item failed", K(addr), K(cluster_name), K(tenant_name), K(name), K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (0 != strcasecmp(proxy_item.config_level_.ptr(), level.ptr())) {
    ret = OB_SUCCESS;
    LOG_TRACE("vip config level not match", K(proxy_item), K(level));
  } else {
    found = true;
    ret_item = proxy_item.config_item_;
  }

  return ret;
}

int ObConfigProcessor::get_proxy_config(const ObVipAddr &addr, const ObString &cluster_name,
                                        const ObString &tenant_name, const ObString& name,
                                        ObConfigItem &ret_item, const bool lock_required/*true*/)
{
  int ret = OB_SUCCESS;
  ObVipAddr tmp_addr = addr;
  ObString tmp_cluster_name = cluster_name;
  ObString tmp_tenant_name = tenant_name;
  bool found = false;
  if (OB_FAIL(get_proxy_config_with_level(tmp_addr, tmp_cluster_name, tmp_tenant_name, name, ret_item, "LEVEL_VIP", found, lock_required))) {
    LOG_WDIAG("get_proxy_config_with_level failed", K(ret));
  }

  if (OB_SUCC(ret) && !found) {
    tmp_addr.reset();
    if (OB_FAIL(get_proxy_config_with_level(tmp_addr, tmp_cluster_name, tmp_tenant_name, name, ret_item, "LEVEL_TENANT", found, lock_required))) {
      LOG_WDIAG("get_proxy_config_with_level failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && !found) {
    tmp_tenant_name.reset();
    if (OB_FAIL(get_proxy_config_with_level(tmp_addr, tmp_cluster_name, tmp_tenant_name, name, ret_item, "LEVEL_CLUSTER", found, lock_required))) {
      LOG_WDIAG("get_proxy_config_with_level failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && !found) {
    if (OB_FAIL(get_global_proxy_config().get_config_item(name, ret_item))) {
      LOG_WDIAG("get global config item failed", K(ret));
    }
  }

  LOG_DEBUG("get proxy config", K(ret_item), K(found), K(ret));

  return ret;
}

int ObConfigProcessor::get_proxy_config_bool_item(const ObVipAddr &addr, const ObString &cluster_name,
                                                  const ObString &tenant_name, const ObString& name,
                                                  ObConfigBoolItem &ret_item, const bool lock_required/*true*/)
{
  int ret = OB_SUCCESS;
  ObConfigItem item;
  if (OB_FAIL(get_proxy_config(addr, cluster_name, tenant_name, name, item, lock_required))) {
    LOG_WDIAG("get proxy config failed", K(addr), K(cluster_name), K(tenant_name), K(name), K(ret));
  } else {
    ret_item.set(item.str());
    LOG_DEBUG("get bool item succ", K(ret_item));
  }

  return ret;
}

int ObConfigProcessor::get_proxy_config_int_item(const ObVipAddr &addr, const ObString &cluster_name,
                                                  const ObString &tenant_name, const ObString& name,
                                                  ObConfigIntItem &ret_item, const bool lock_required/*true*/)
{
  int ret = OB_SUCCESS;
  ObConfigItem item;
  if (OB_FAIL(get_proxy_config(addr, cluster_name, tenant_name, name, item, lock_required))) {
    LOG_WDIAG("get proxy config failed", K(addr), K(cluster_name), K(tenant_name), K(name), K(ret));
  } else {
    ret_item.set(item.str());
    LOG_DEBUG("get int item succ", K(ret_item));
  }

  return ret;
}

int ObConfigProcessor::get_proxy_config_strlist_item(const ObVipAddr &addr, const ObString &cluster_name,
                                                  const ObString &tenant_name, const ObString& name,
                                                  ObConfigStrListItem &ret_item, const bool lock_required)
{
  int ret = OB_SUCCESS;
  ObConfigItem item;
  if (OB_FAIL(get_proxy_config(addr, cluster_name, tenant_name, name, item, lock_required))) {
    LOG_WDIAG("get proxy config failed", K(addr), K(cluster_name), K(tenant_name), K(name), K(ret));
  } else {
    ret_item = item.str();
    LOG_DEBUG("get list item succ", K(ret_item));
  }

  return ret;
}

int ObConfigProcessor::close_sqlite3()
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(config_lock_);
  if (NULL != proxy_config_db_ && SQLITE_OK != sqlite3_close(proxy_config_db_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("sqlite3_close failed", "err_msg", sqlite3_errmsg(proxy_config_db_), K(ret));
  } else {
    proxy_config_db_ = NULL;
  }
  return ret;
}
int ObConfigProcessor::open_sqlite3()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start open sqlite3");
  DRWLock::WRLockGuard guard(config_lock_);
  if (NULL == proxy_config_db_) {
    const char *dir = NULL;
    if (OB_ISNULL(dir = get_global_layout().get_etc_dir())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("get etc dir failed", K(ret));
    } else {
      char *path = NULL;
      ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
      if (OB_FAIL(ObLayout::merge_file_path(dir, sqlite3_db_name, allocator, path))) {
        LOG_WDIAG("fail to merge file path", K(sqlite3_db_name), K(ret));
      } else if (OB_ISNULL(path)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (SQLITE_OK != sqlite3_open_v2(path, &proxy_config_db_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("sqlite3 open failed", "path", path, "err_msg", sqlite3_errmsg(proxy_config_db_), K(ret));
      }
    }
  }
  return ret;
}

} // end of obutils
} // end of obproxy
} // end of oceanbase
