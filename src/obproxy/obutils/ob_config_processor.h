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

struct ObVipAddr;

typedef int (*config_processor_execute) (void *);
typedef int (*config_processor_commit) (void*, bool is_success);
typedef int (*config_processor_before_commit) (void *);

struct ObConfigHandler
{
  ObConfigHandler() : execute_func_(NULL), commit_func_(NULL), before_commit_func_(NULL) {}
  config_processor_execute execute_func_;
  config_processor_commit commit_func_;
  config_processor_before_commit before_commit_func_;
};

typedef common::hash::ObHashMap<common::ObFixedLengthString<common::OB_MAX_TABLE_NAME_LENGTH>, ObConfigHandler> ConfigHandlerHashMap;

class ObFnParams
{
public:
  ObFnParams();
  ~ObFnParams();

public:
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

class ObConfigProcessor
{
public:
  ObConfigProcessor();
  ~ObConfigProcessor();

  int init();
  int execute(common::ObString &sql, const ObProxyBasicStmtType stmt_type, obproxy::ObConfigV2Handler *handler);
  int register_callback(const common::ObString &table_name, ObConfigHandler&handler);
  bool is_table_in_service(const common::ObString &table_name);
  int store_global_ssl_config(const common::ObString& name, const common::ObString &value);
  int store_global_proxy_config(const common::ObString &name, const common::ObString &value);
  int store_vip_tenant_cluster_config(int64_t vid, const ObString &vip, int64_t vport,
      const ObString &name, const ObString &value);
  int store_proxy_config_with_level(int64_t vid, const ObString &vip, int64_t vport,
      const ObString &cluster_name, const ObString &tenant_name,
      const ObString &name, const ObString &value, const ObString &level);

  int get_proxy_config(const ObVipAddr &addr, const common::ObString &cluster_name,
                       const common::ObString &tenant_name, const common::ObString& name,
                       common::ObConfigItem &ret_item);
  int get_proxy_config_bool_item(const ObVipAddr &addr, const common::ObString &cluster_name,
                                 const common::ObString &tenant_name, const common::ObString& name,
                                 common::ObConfigBoolItem &ret_item);
  int get_proxy_config_int_item(const ObVipAddr &addr, const common::ObString &cluster_name,
                                 const common::ObString &tenant_name, const common::ObString& name,
                                 common::ObConfigIntItem &ret_item);
  int get_proxy_config_list_item(const ObVipAddr &addr, const common::ObString &cluster_name,
                                 const common::ObString &tenant_name, const common::ObString& name,
                                 common::ObConfigStrListItem &ret_item);
  int get_proxy_config_with_level(const ObVipAddr &addr, const common::ObString &cluster_name,
                                  const common::ObString &tenant_name, const common::ObString& name,
                                  common::ObConfigItem &ret_item, const ObString level, bool &found);
  int execute(const char* sql, int (*callback)(void*, int, char**, char**), void* handler);

private:
  int init_config_from_disk();
  int check_and_create_table();
  int handle_dml_stmt(common::ObString &sql, ParseResult& parse_result, ObArenaAllocator&allocator);
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
