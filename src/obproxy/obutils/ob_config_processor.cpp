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
#include "cmd/ob_config_v2_handler.h"
#include "omt/ob_white_list_table_processor.h"
#include "omt/ob_resource_unit_table_processor.h"
#include "omt/ob_ssl_config_table_processor.h"

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

#define MAX_INIT_SQL_LEN 1024
static const char* sqlite3_db_name = "etc/proxyconfig.db";
static const char* all_table_version_table_name = "all_table_version";
static const char* white_list_table_name = "white_list";
static const char* resource_unit_table_name = "resource_unit";
static const char *ssl_config_table_name = "ssl_config";
static const char *table_name_array[] = {white_list_table_name, resource_unit_table_name, ssl_config_table_name};

static const char* create_all_table_version_table =
                 "create table if not exists "
                 "all_table_version(gmt_created date default (datetime('now', 'localtime')), "
                 "gmt_modified date, table_name varchar(1000), "
                 "version int default 0, primary key(table_name))";
static const char* create_white_list_table = 
                 "create table if not exists "
                 "white_list(gmt_created date default (datetime('now', 'localtime')), gmt_modified date, "
                 "cluster_name varchar(256), tenant_name varchar(256), name varchar(256), "
                 "value text, primary key(cluster_name, tenant_name))";
static const char* create_resource_unit_table = 
                 "create table if not exists "
                 "resource_unit(gmt_created date default (datetime('now', 'localtime')), "
                 "gmt_modified date, cluster_name varchar(256), tenant_name varchar(256), "
                 "name varchar(256), value text, primary key(cluster_name, tenant_name, name))";
static const char* create_ssl_config_table =
                 "create table if not exists "
                 "ssl_config(gmt_created date default (datetime('now', 'localtime')), gmt_modified date, "
                 "cluster_name varchar(256), tenant_name varchar(256), name varchar(256), value text, "
                 "primary key(cluster_name, tenant_name, name))";

ObConfigProcessor &get_global_config_processor()
{
  static ObConfigProcessor g_config_processor;
  return g_config_processor;
}

ObFnParams::ObFnParams() : config_type_(OBPROXY_CONFIG_INVALID),
                           stmt_type_(OBPROXY_T_INVALID),
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
    // TODO: Compatibility Check
    if (OB_FAIL(get_global_white_list_table_processor().init())) {
      LOG_WARN("white list table processor init failed", K(ret));
    } else if (OB_FAIL(get_global_resource_unit_table_processor().init())) {
      LOG_WARN("resource unit table processor init failed", K(ret));
    } else if (OB_FAIL(get_global_ssl_config_table_processor().init())) {
      LOG_WARN("ssl config table processor init failed", K(ret));
    } else if (OB_FAIL(init_config_from_disk())) {
      LOG_WARN("init config from disk failed", K(ret));
    }
  }

  // Do compatibility processing, currently it is related to ssl,
  // if the -o startup parameter specifies that synchronization is required here
  if (OB_SUCC(ret)) {
    if (OB_FAIL(store_cloud_config(ssl_config_table_name, "*", "*", "enable_client_ssl",
                                   get_global_proxy_config().enable_client_ssl.str()))) {
      LOG_WARN("store client ssl failed", K(ret));
    } else if (OB_FAIL(store_cloud_config(ssl_config_table_name, "*", "*", "enable_server_ssl",
                                   get_global_proxy_config().enable_server_ssl.str()))) {
      LOG_WARN("store server ssl failed", K(ret));
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
    params.config_type_ = OBPROXY_CONFIG_CLOUD;
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

      if (OB_FAIL(handler->commit_func_(is_success))) {
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
    } else if (OB_FAIL(handle_dml_stmt(sql, parse_result))) {
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
  }

  if (NULL != err_msg) {
    sqlite3_free(err_msg);
  }

  return ret;
}

int ObConfigProcessor::resolve_insert(const ParseNode* node, ResolveContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || T_INSERT != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null or wrong type unexpected", K(ret));
  } else {
    if (node->children_[1] != NULL) {
      if (node->children_[1]->type_ == T_INSERT) {
        ctx.stmt_type_ = OBPROXY_T_INSERT;
      } else if (node->children_[1]->type_ == T_REPLACE) {
        ctx.stmt_type_ = OBPROXY_T_REPLACE;
      }
    }

    if (node->children_[0] != NULL && node->children_[0]->type_ == T_SINGLE_TABLE_INSERT) {
      ParseNode* tmp_node = node->children_[0];
      if (tmp_node->children_[0] != NULL && tmp_node->children_[0]->type_ == T_INSERT_INTO_CLAUSE) {
        ParseNode* tmp_node1 = tmp_node->children_[0];
        if (tmp_node1->children_[0] != NULL && tmp_node1->children_[0]->type_ == T_ORG) {
          ParseNode* tmp_node2 = tmp_node1->children_[0];
          if (tmp_node2->children_[0] != NULL && tmp_node2->children_[0]->type_ == T_RELATION_FACTOR) {
            ParseNode* tmp_node3 = tmp_node2->children_[0];
            ctx.table_name_.assign_ptr(tmp_node3->str_value_, static_cast<int32_t>(tmp_node3->str_len_));
            if (OB_FAIL(table_handler_map_.get_refactored(ctx.table_name_, ctx.handler_))) {
              LOG_DEBUG("fail to get handler by table name", K(ret), K(ctx.table_name_));
              ret = OB_SUCCESS;
            } else {
              ctx.config_type_ = ctx.handler_.config_type_;
            }
          }
        }

        if (!ctx.table_name_.empty() && tmp_node1->children_[1] != NULL && tmp_node1->children_[1]->type_ == T_COLUMN_LIST) {
          ParseNode *tmp_node2 = tmp_node1->children_[1];
          for (int64_t i = 0; OB_SUCC(ret) && i < tmp_node2->num_child_; i++) {
            if (NULL != tmp_node2->children_[i]) {
              ObString column_name;
              column_name.assign_ptr(tmp_node2->children_[i]->str_value_, static_cast<int32_t>(tmp_node2->children_[i]->str_len_));
              if (OB_FAIL(ctx.column_name_array_.push_back(column_name))) {
                LOG_WARN("column name push back failed", K(ret), K(column_name));
              }
            }
          }
        }
      }

      if (!ctx.table_name_.empty() && tmp_node->children_[1] != NULL && tmp_node->children_[1]->type_ == T_VALUE_LIST) {
        ParseNode* tmp_node1 = tmp_node->children_[1];
        if (tmp_node1->children_[0] != NULL && tmp_node1->children_[0]->type_ == T_VALUE_VECTOR) {
          ParseNode* tmp_node2 = tmp_node1->children_[0];
          for (int64_t i = 0; OB_SUCC(ret) && ctx.column_name_array_.count() == tmp_node2->num_child_ && i < tmp_node2->num_child_; i++) {
            if (tmp_node2->children_[i] != NULL) {
              if (tmp_node2->children_[i]->type_ == T_INT) {
                SqlField field;
                field.column_name_.set(ctx.column_name_array_.at(i));
                field.value_type_ = TOKEN_INT_VAL;
                field.column_int_value_ = tmp_node2->children_[i]->value_;
                ctx.sql_field_.fields_.push_back(field);
                ctx.sql_field_.field_num_++;
              } else if (tmp_node2->children_[i]->type_ == T_VARCHAR) {
                SqlField field;
                field.column_name_.set(ctx.column_name_array_.at(i));
                field.value_type_ = TOKEN_STR_VAL;
                ObString value;
                if (tmp_node2->children_[i]->str_len_ - 2 <= 0) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected len", K(ret));
                } else {
                  value.assign_ptr(tmp_node2->children_[i]->str_value_ + 1, static_cast<int32_t>(tmp_node2->children_[i]->str_len_ - 2));
                  field.column_value_.set_value(value);
                  ctx.sql_field_.fields_.push_back(field);
                  ctx.sql_field_.field_num_++;
                }
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObConfigProcessor::resolve_delete(const ParseNode* node, ResolveContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || T_DELETE != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null or wrong type", K(ret));
  } else {
    ctx.stmt_type_ = OBPROXY_T_DELETE;
    if (node->children_[0] != NULL && node->children_[0]->type_ == T_DELETE_TABLE_NODE) {
      ParseNode *tmp_node = node->children_[0];
      if (tmp_node->children_[1] != NULL && tmp_node->children_[1]->type_ == T_TABLE_REFERENCES) {
        ParseNode *tmp_node1 = tmp_node->children_[1];
        if (tmp_node1->children_[0] != NULL && tmp_node1->children_[0]->type_ == T_ORG) {
          ParseNode *tmp_node2 = tmp_node1->children_[0];
          if (tmp_node2->children_[0] != NULL && tmp_node2->children_[0]->type_ == T_RELATION_FACTOR) {
            ctx.table_name_.assign_ptr(tmp_node2->children_[0]->str_value_, static_cast<int32_t>(tmp_node2->children_[0]->str_len_));
            if (OB_FAIL(table_handler_map_.get_refactored(ctx.table_name_, ctx.handler_))) {
              LOG_DEBUG("fail to get handler by table name", K(ret), K(ctx.table_name_));
              ret = OB_SUCCESS;
            } else {
              ctx.config_type_ = ctx.handler_.config_type_;
            }
          }
        }
      }
    }

    if (!ctx.table_name_.empty() && node->children_[1] != NULL && node->children_[1]->type_ == T_WHERE_CLAUSE) {
      ParseNode* tmp_node = node->children_[1];
      if (tmp_node->children_[0] != NULL && tmp_node->children_[0]->type_ == T_OP_AND) {
        ParseNode* tmp_node1 = tmp_node->children_[0];
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_node1->num_child_; i++) {
          if (tmp_node1->children_[i] != NULL && tmp_node1->children_[i]->type_ == T_OP_EQ) {
            ParseNode* tmp_node2 = tmp_node1->children_[i];
            SqlField field;
            if (tmp_node2->children_[0] != NULL && tmp_node2->children_[0]->type_ == T_COLUMN_REF) {
              ObString column_name;
              column_name.assign_ptr(tmp_node2->children_[0]->str_value_, static_cast<int32_t>(tmp_node2->children_[0]->str_len_));
              field.column_name_.set(column_name);
              LOG_DEBUG("parse column name", K(column_name), K(i));
            }

            if (tmp_node2->children_[1] != NULL) {
              if (tmp_node2->children_[1]->type_ == T_INT) {
                field.value_type_ = TOKEN_INT_VAL;
                field.column_int_value_ = tmp_node2->children_[1]->value_;
                ctx.sql_field_.fields_.push_back(field);
                ctx.sql_field_.field_num_++;
              } else if (tmp_node2->children_[1]->type_ == T_VARCHAR) {
                if (tmp_node2->children_[1]->str_len_ - 2 <= 0) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected len", K(ret));
                } else {
                  ObString value;
                  value.assign_ptr(tmp_node2->children_[1]->str_value_ + 1, static_cast<int32_t>(tmp_node2->children_[1]->str_len_ - 2));
                  field.value_type_ = TOKEN_STR_VAL;
                  field.column_value_.set_value(value);
                  ctx.sql_field_.fields_.push_back(field);
                  ctx.sql_field_.field_num_++;
                  LOG_DEBUG("parse column value", K(value), K(i));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObConfigProcessor::handle_dml_stmt(ObString &sql, ParseResult& parse_result)
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
    // At present, our grammar parsing is relatively fixed. There can only be two equal conditions
    // after the delete statement where, otherwise an error will be reported
    // The new requirement is changed to the implementation of ob_proxy_stmt
    ResolveContext ctx;
    switch(node->type_) {
      case T_INSERT:
      {
        if (OB_FAIL(resolve_insert(node, ctx))) {
          LOG_WARN("resove insert failed", K(ret));
        }
        break;
      }
      case T_DELETE:
      {
        if (OB_FAIL(resolve_delete(node, ctx))) {
          LOG_WARN("resolve delete failed", K(ret));
        }
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
    if (OB_SUCC(ret) && OBPROXY_CONFIG_CLOUD == ctx.config_type_ && !ctx.table_name_.empty()) {
      params.config_type_ = ctx.config_type_;
      params.stmt_type_ = ctx.stmt_type_;
      params.table_name_ = ctx.table_name_;
      params.fields_ = &ctx.sql_field_;
      const char* cluster_name_str = "cluster_name";
      const char* tenant_name_str = "tenant_name";
      for (int64_t i = 0; i < ctx.sql_field_.field_num_; i++) {
        SqlField &field = ctx.sql_field_.fields_.at(i);
        if (field.column_name_.string_ == cluster_name_str) {
          params.cluster_name_ = field.column_value_.config_string_;
        } else if (field.column_name_.string_ == tenant_name_str) {
          params.tenant_name_ = field.column_value_.config_string_;
        }
      }

      if (OB_FAIL(ctx.handler_.execute_func_(&params))) {
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
      if (OB_FAIL(ctx.handler_.commit_func_(is_success))) {
        LOG_WARN("commit failed", K(ret), K(is_success));
      }
    }
    ret = (OB_SUCCESS != tmp_ret ? tmp_ret : ret);
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

int ObConfigProcessor::register_callback(const common::ObString &table_name, ObConfigHandler&handler)
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
      || table_name == ssl_config_table_name) {
    is_in_service = true;
  }

  return is_in_service;
}

int ObConfigProcessor::store_cloud_config(const ObString &table_name,
                                          const ObString &cluster_name,
                                          const ObString &tenant_name,
                                          const ObString &name,
                                          const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cluster_name.empty() || tenant_name.empty() || name.empty() || value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(cluster_name), K(tenant_name), K(name), K(value));
  } else {
    const char *sql = "replace into %.*s(cluster_name, tenant_name, name, value) values('%.*s', '%.*s', '%.*s', '%.*s')";
    int64_t buf_len = cluster_name.length() + tenant_name.length() + name.length() + value.length() + strlen(sql);
    char *sql_buf = (char*)ob_malloc(buf_len + 1, ObModIds::OB_PROXY_CONFIG_TABLE);
    if (OB_ISNULL(sql_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(buf_len));
    } else {
      ObString sql_string;
      int64_t len = 0;
      len = static_cast<int64_t>(snprintf(sql_buf, buf_len, sql, table_name.length(), table_name.ptr(),
                                 cluster_name.length(), cluster_name.ptr(),
                                 tenant_name.length(), tenant_name.ptr(),
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

} // end of obutils
} // end of obproxy
} // end of oceanbase
