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

#ifndef OB_CONFIG_PROCESSOR_H_
#define OB_CONFIG_PROCESSOR_H_

#include <sqlite/sqlite3.h>

#include "lib/hash/ob_hashmap.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/ptr/ob_ptr.h"
#include "iocore/eventsystem/ob_lock.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "lib/lock/ob_drw_lock.h"

namespace oceanbase
{
namespace obproxy
{

class ObConfigV2Handler;
namespace obutils
{

enum ObConfigType
{
  OBPROXY_CONFIG_INVALID = -1,
  OBPROXY_CONFIG_CLOUD,
  OBPROXY_CONFIG_MAX
};

typedef int (*config_processor_execute) (void *);
typedef int (*config_processor_commit) (bool is_success);

struct ObConfigHandler
{
  config_processor_execute execute_func_;
  config_processor_commit commit_func_;
  ObConfigType config_type_;
};

typedef common::hash::ObHashMap<common::ObFixedLengthString<common::OB_MAX_TABLE_NAME_LENGTH>, ObConfigHandler> ConfigHandlerHashMap;

class ObFnParams
{
public:
  ObFnParams();
  ~ObFnParams();

public:
  ObConfigType config_type_;
  ObProxyBasicStmtType stmt_type_;
  common::ObString table_name_;
  SqlFieldResult *fields_;
};

class ObCloudFnParams : public ObFnParams
{
public:
  ObCloudFnParams();
  ~ObCloudFnParams();

public:
  common::ObString cluster_name_;
  common::ObString tenant_name_;
};

struct ResolveContext
{
  ResolveContext() : stmt_type_(OBPROXY_T_INVALID), table_name_(),
      config_type_(OBPROXY_CONFIG_INVALID), column_name_array_(),
      sql_field_(), handler_()   
  {}

  ObProxyBasicStmtType stmt_type_;
  common::ObString table_name_;
  ObConfigType config_type_;
  common::ObSEArray<common::ObString, 4> column_name_array_;
  obutils::SqlFieldResult sql_field_;
  ObConfigHandler handler_;
};

class ObConfigProcessor
{
public:
  ObConfigProcessor();
  ~ObConfigProcessor();

  int init();
  int execute(common::ObString &sql, const ObProxyBasicStmtType stmt_type, obproxy::ObConfigV2Handler *handler);
  int register_callback(const common::ObString &table_name, ObConfigHandler&handler);
  bool is_table_in_service(const common::ObString &table_name);
  int store_cloud_config(const common::ObString &table_name,
                         const common::ObString &cluster_name,
                         const common::ObString& tenant_name,
                         const common::ObString& name,
                         const common::ObString &value);

private:
  int init_config_from_disk();
  int check_and_create_table();
  int resolve_insert(const ParseNode* node, ResolveContext &ctx);
  int resolve_delete(const ParseNode* node, ResolveContext &ctx);
  int handle_dml_stmt(common::ObString &sql, ParseResult& parse_result);
  int handle_select_stmt(common::ObString &sql, obproxy::ObConfigV2Handler *v2_handler);
  
  static int init_callback(void *data, int argc, char **argv, char **column_name);
  static int sqlite3_callback(void *data, int argc, char **argv, char **column_name);
private:
  sqlite3 *proxy_config_db_;
  ConfigHandlerHashMap table_handler_map_;
  common::DRWLock config_lock_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConfigProcessor);
};

ObConfigProcessor &get_global_config_processor();

} // end of obutils
} // end of obproxy
} // end of oceanbase
#endif
