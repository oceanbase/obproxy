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

//#define private public
#define USING_LOG_PREFIX PROXY
#include <gtest/gtest.h>
#include <sqlite/sqlite3.h>
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "obproxy/omt/ob_conn_table_processor.h"
#include "obproxy/omt/ob_resource_unit_table_processor.h"
#include "obproxy/obutils/ob_config_processor.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
using namespace common;
namespace obproxy
{
using namespace omt;
using namespace obutils;

struct conn_map_def {
  ObString key_;
  int32_t value_;
};

class TestTenantProcessor : public ::testing::Test
{
public:
  TestTenantProcessor(): t_processor_(get_global_conn_table_processor()) {}
  int init_env();
  int check_map_value(conn_map_def* conn_map, uint32_t size);

  virtual void SetUp() { }

  virtual void TearDown()
  {
    //t_processor_.destroy();
  }

  ObConnTableProcessor& t_processor_;
};

static int callback(void *NotUsed, int argc, char **argv, char **azColName)
{
  (void)(NotUsed);
  int i;
  for(i=0; i<argc; i++){
    fprintf(stdout, "%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  printf("\n");
  return 0;
}

sqlite3 *db;

int TestTenantProcessor::init_env()
{
  char *zErrMsg = 0;
  int rc;
  char *sql;

  /* Open database */
  rc = sqlite3_open("etc/proxyconfig.db", &db);
  if( rc ){
    fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
    exit(0);
  }else{
    fprintf(stderr, "Opened database successfully\n");
  }

  sql = (char*)"drop table if exists resource_unit";
  /* Execute SQL statement */
   rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
   if( rc != SQLITE_OK ){
   fprintf(stderr, "SQL error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
   }else{
      fprintf(stdout, "drop table successfully\n");
   }

  /* Create SQL statement */
  sql = (char*)"CREATE TABLE resource_unit("  \
       "cluster_name TEXT    NOT NULL," \
       "tenant_name  TEXT    NOT NULL," \
       "name         TEXT    NOT NULL," \
       "value        TEXT," \
       "primary key(cluster_name, tenant_name, name))";

   /* Execute SQL statement */
   rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
   if( rc != SQLITE_OK ){
   fprintf(stderr, "SQL error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
   }else{
      fprintf(stdout, "Table created successfully\n");
   }

   return rc;
}

int TestTenantProcessor::check_map_value(conn_map_def* conn_map, uint32_t size)
{
  int ret = 0;

  ObVipTenantConnCache::VTHashMap* vt_conn_map = t_processor_.get_conn_map();
  ObVipTenantConnCache::VTHashMap::iterator last = vt_conn_map->end();
  if (size != vt_conn_map->count()) {
    ret = -1;
  } else {
    int i = 0;
    for (ObVipTenantConnCache::VTHashMap::iterator it = vt_conn_map->begin(); it != last; ++it) {
      if (conn_map[i].key_ != it->full_name_ || conn_map[i].value_ != it->max_connections_) {
        ret = -1;
        break;
      }
      i++;
    }
  }

  return ret;
}


// Insert a kv in a single line
TEST_F(TestTenantProcessor, test_one_vt_conn)
{
  int rc;
  char *sql;
  char *zErrMsg = 0;

  ASSERT_EQ(0, init_env());
  /* replace SQL statement */
  sql = (char*)"replace into resource_unit(name, value, cluster_name, tenant_name)"  \
        "values('resource_max_connections', '[{\"vip\": \"127.0.0.1\", \"value\": 2000}]', 'ob_cluster', 'ob_tenant');";

  /* Execute SQL statement */
  rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
  if( rc != SQLITE_OK ){
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }else{
    fprintf(stdout, "Records created successfully\n");
  }

  ASSERT_EQ(0, get_global_config_processor().init());
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ASSERT_EQ(1, t_processor_.get_conn_map_count());
  ObVipTenantConnCache::VTHashMap* vt_conn_map = t_processor_.get_conn_map();
  ObVipTenantConnCache::dump_conn_map(*vt_conn_map);
}

// Insert multiple kvs in a single row
TEST_F(TestTenantProcessor, test_multi_vt_conn)
{
  int rc;
  char *sql;
  char *zErrMsg = 0;

  ASSERT_EQ(0, init_env());

  /* replace SQL statement */
  sql = (char*)"replace into resource_unit(name, value, cluster_name, tenant_name)"  \
        "values('resource_max_connections', '[{\"vip\": \"127.0.0.1\", \"value\": 2000}, {\"vip\": \"127.0.0.2\", \"value\": 3000}]', 'ob_cluster', 'ob_tenant');";

  /* Execute SQL statement */
  rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
  if( rc != SQLITE_OK ){
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }else{
    fprintf(stdout, "Records created successfully\n");
  }

  ASSERT_EQ(0, get_global_config_processor().init());
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ASSERT_EQ(2, t_processor_.get_conn_map_count());
  ObVipTenantConnCache::VTHashMap* vt_conn_map = t_processor_.get_conn_map();
  ObVipTenantConnCache::dump_conn_map(*vt_conn_map);
}

// Insert multiple kvs in multiple rows
TEST_F(TestTenantProcessor, test_multi_row_vt_conn)
{
  int rc;
  char *sql;
  char *zErrMsg = 0;

  ASSERT_EQ(0, init_env());

  /* replace SQL statement */
  sql = (char*)"replace into resource_unit(name, value, cluster_name, tenant_name)"  \
        "values('resource_max_connections', '[{\"vip\": \"127.0.0.1\", \"value\": 2000}, {\"vip\": \"127.0.0.2\", \"value\": 3000}]', 'ob_cluster', 'ob_tenant');";

  /* Execute SQL statement */
  rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
  if( rc != SQLITE_OK ){
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }else{
    fprintf(stdout, "Records created successfully\n");
  }

  sql = (char*)"replace into resource_unit(name, value, cluster_name, tenant_name)"  \
        "values('resource_max_connections', '[{\"vip\": \"127.0.0.1\", \"value\": 2000}, {\"vip\": \"127.0.0.2\", \"value\": 3000}]', 'ob_cluster_1', 'ob_tenant_1');";

  /* Execute SQL statement */
  rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
  if( rc != SQLITE_OK ){
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }else{
    fprintf(stdout, "Records created successfully\n");
  }

  ASSERT_EQ(0, get_global_config_processor().init());
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ObVipTenantConnCache::VTHashMap* vt_conn_map = t_processor_.get_conn_map();
  ObVipTenantConnCache::dump_conn_map(*vt_conn_map);
  ASSERT_EQ(4, t_processor_.get_conn_map_count());
}

// Insert duplicate key
TEST_F(TestTenantProcessor, test_repeat_key)
{
  int rc;
  char *sql;
  char *zErrMsg = 0;

  ASSERT_EQ(0, init_env());

  /* replace SQL statement */
  sql = (char*)"replace into resource_unit(name, value, cluster_name, tenant_name)"  \
        "values('resource_max_connections', '[{\"vip\": \"127.0.0.1\", \"value\": 2000}," \
        "{\"vip\": \"127.0.0.2\", \"value\": 3000}, {\"vip\": \"127.0.0.2\", \"value\": 4450}]', 'ob_cluster', 'ob_tenant');";

  /* Execute SQL statement */
  rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
  if( rc != SQLITE_OK ){
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }else{
    fprintf(stdout, "Records created successfully\n");
  }

  ASSERT_EQ(0, get_global_config_processor().init());
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ASSERT_EQ(2, t_processor_.get_conn_map_count());
  ObVipTenantConnCache::VTHashMap* vt_conn_map = t_processor_.get_conn_map();
  ObVipTenantConnCache::dump_conn_map(*vt_conn_map);
}

// insert a different name
TEST_F(TestTenantProcessor, test_diff_key)
{
  int rc;
  char *sql;
  char *zErrMsg = 0;

  ASSERT_EQ(0, init_env());

  // Insert connection related information

  sql = (char*)"replace into resource_unit(name, value, cluster_name, tenant_name)"  \
        "values('resource_max_connections', '[{\"vip\": \"127.0.0.1\", \"value\": 2000}," \
        "{\"vip\": \"127.0.0.2\", \"value\": 4450}]', 'ob_cluster', 'ob_tenant');";

  /* Execute SQL statement */
  rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
  if( rc != SQLITE_OK ){
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }else{
    fprintf(stdout, "Records created successfully\n");
  }

  // Insert cpu related information
  sql = (char*)"replace into resource_unit(name, value, cluster_name, tenant_name)"  \
        "values('resource_cpu', '[{\"vip\": \"127.0.0.1\", \"value\": 50}," \
        "{\"vip\": \"127.0.0.2\", \"value\": 50}]', 'ob_cluster', 'ob_tenant');";

  /* Execute SQL statement */
  rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
  if( rc != SQLITE_OK ){
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }else{
    fprintf(stdout, "Records created successfully\n");
  }

  ASSERT_NE(0, get_global_config_processor().init());
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ObVipTenantConnCache::VTHashMap* vt_conn_map = t_processor_.get_conn_map();
  ObVipTenantConnCache::dump_conn_map(*vt_conn_map);
  ASSERT_EQ(0, t_processor_.get_conn_map_count());
}

TEST_F(TestTenantProcessor, test_no_conn_key)
{
  int rc;
  char *sql;
  char *zErrMsg = 0;

  ASSERT_EQ(0, init_env());

  // Insert cpu related information
  sql = (char*)"replace into resource_unit(name, value, cluster_name, tenant_name)"  \
        "values('resource_cpu', '[{\"vip\": \"127.0.0.1\", \"value\": 50}," \
        "{\"vip\": \"127.0.0.2\", \"value\": 50}]', 'ob_cluster', 'ob_tenant');";

  /* Execute SQL statement */
  rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
  if( rc != SQLITE_OK ){
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }else{
    fprintf(stdout, "Records created successfully\n");
  }

  ASSERT_NE(0, get_global_config_processor().init());
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ASSERT_EQ(0, t_processor_.get_conn_map_count());
  ObVipTenantConnCache::VTHashMap* vt_conn_map = t_processor_.get_conn_map();
  ObVipTenantConnCache::dump_conn_map(*vt_conn_map);
}

TEST_F(TestTenantProcessor, test_execute)
{
  ASSERT_EQ(0, init_env());

  SqlFieldResult sql_result;
  sql_result.field_num_ = 4;
  SqlField sql_field;
  sql_field.column_name_.set("cluster");
  sql_field.column_value_.set_value("ob_cluster");
  sql_result.fields_.push_back(sql_field);

  sql_field.column_name_.set("tenant");
  sql_field.column_value_.set_value("ob_tenant");
  sql_result.fields_.push_back(sql_field);

  sql_field.column_name_.set("name");
  sql_field.column_value_.set_value("resource_max_connections");
  sql_result.fields_.push_back(sql_field);

  sql_field.column_name_.set("value");
  sql_field.column_value_.set_value("[{\"vip\": \"127.0.0.1\", \"value\": 5000}]");
  sql_result.fields_.push_back(sql_field);

  ObCloudFnParams cloud_params;
  cloud_params.stmt_type_ = OBPROXY_T_REPLACE;
  cloud_params.fields_ = &sql_result;
  cloud_params.cluster_name_ = "ob_cluster";
  cloud_params.tenant_name_ = "ob_tenant";
  cloud_params.table_name_ = "resource_unit";

  ASSERT_EQ(0, get_global_config_processor().init());
  ASSERT_EQ(0, ObResourceUnitTableProcessor::execute(&cloud_params));
  ObVipTenantConnCache::VTHashMap* vt_conn_map = t_processor_.get_conn_map();
  ObVipTenantConnCache::dump_conn_map(*vt_conn_map);
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ASSERT_EQ(1, t_processor_.get_conn_map_count());
  conn_map_def conn_map[1] = {
    {"ob_tenant#ob_cluster|127.0.0.1", 5000}
  };
  ASSERT_EQ(0, check_map_value(conn_map, 1));

  // vt_conn_map_replica is empty
  ObVipTenantConnCache::VTHashMap& vt_conn_map_replica = t_processor_.get_conn_map_replica();
  ObVipTenantConnCache::dump_conn_map(vt_conn_map_replica);
}

TEST_F(TestTenantProcessor, test_rollback)
{
  ASSERT_EQ(0, init_env());

  SqlFieldResult sql_result;
  sql_result.field_num_ = 4;
  SqlField sql_field;
  sql_field.column_name_.set("cluster");
  sql_field.column_value_.set_value("ob_cluster");
  sql_result.fields_.push_back(sql_field);

  sql_field.column_name_.set("tenant");
  sql_field.column_value_.set_value("ob_tenant");
  sql_result.fields_.push_back(sql_field);

  sql_field.column_name_.set("name");
  sql_field.column_value_.set_value("resource_max_connections");
  sql_result.fields_.push_back(sql_field);

  sql_field.column_name_.set("value");
  sql_field.column_value_.set_value("[{\"vip\": \"127.0.0.1\", \"value\": 5000}]");
  sql_result.fields_.push_back(sql_field);

  ObCloudFnParams cloud_params;
  cloud_params.stmt_type_ = OBPROXY_T_REPLACE;
  cloud_params.fields_ = &sql_result;
  cloud_params.cluster_name_ = "ob_cluster";
  cloud_params.tenant_name_ = "ob_tenant";
  cloud_params.table_name_ = "resource_unit";

  ASSERT_EQ(0, get_global_config_processor().init());
  ASSERT_EQ(0, ObResourceUnitTableProcessor::execute(&cloud_params));
  ObVipTenantConnCache::VTHashMap* vt_conn_map = t_processor_.get_conn_map();
  ObVipTenantConnCache::dump_conn_map(*vt_conn_map);
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ASSERT_EQ(1, t_processor_.get_conn_map_count());
  conn_map_def conn_map[1] = {
    {"ob_tenant#ob_cluster|127.0.0.1", 5000}
  };
  ASSERT_EQ(0, check_map_value(conn_map, 1));

  // vt_conn_map is empty after rollback
  ObResourceUnitTableProcessor::commit(&cloud_params, false);
  ObVipTenantConnCache::dump_conn_map(*vt_conn_map);
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ASSERT_EQ(0, t_processor_.get_conn_map_count());
}

TEST_F(TestTenantProcessor, test_execute_update_value)
{
  ASSERT_EQ(0, init_env());

  SqlFieldResult sql_result;
  sql_result.field_num_ = 4;
  SqlField sql_field[4];
  sql_field[0].column_name_.set("cluster");
  sql_field[0].column_value_.set_value("ob_cluster");
  sql_result.fields_.push_back(sql_field[0]);

  sql_field[1].column_name_.set("tenant");
  sql_field[1].column_value_.set_value("ob_tenant");
  sql_result.fields_.push_back(sql_field[1]);

  sql_field[2].column_name_.set("name");
  sql_field[2].column_value_.set_value("resource_max_connections");
  sql_result.fields_.push_back(sql_field[2]);

  sql_field[3].column_name_.set("value");
  sql_field[3].column_value_.set_value("[{\"vip\": \"127.0.0.1\", \"value\": 5000}, {\"vip\": \"127.0.0.2\", \"value\": 6000}]");
  sql_result.fields_.push_back(sql_field[3]);

  ObCloudFnParams cloud_params;
  cloud_params.stmt_type_ = OBPROXY_T_REPLACE;
  cloud_params.fields_ = &sql_result;
  cloud_params.cluster_name_ = "ob_cluster";
  cloud_params.tenant_name_ = "ob_tenant";
  cloud_params.table_name_ = "resource_unit";

  ASSERT_EQ(0, get_global_config_processor().init());
  ASSERT_EQ(0, ObResourceUnitTableProcessor::execute(&cloud_params));
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ASSERT_EQ(2, t_processor_.get_conn_map_count());
  conn_map_def conn_map[2] = {
    {"ob_tenant#ob_cluster|127.0.0.2", 6000},
    {"ob_tenant#ob_cluster|127.0.0.1", 5000}
  };
  ASSERT_EQ(0, check_map_value(conn_map, 2));
  ObResourceUnitTableProcessor::commit(&cloud_params, true);
  LOG_DEBUG("success1");

  sql_result.reset();
  sql_result.field_num_ = 4;
  sql_result.fields_.push_back(sql_field[0]);
  sql_result.fields_.push_back(sql_field[1]);
  sql_result.fields_.push_back(sql_field[2]);
  sql_field[3].column_name_.set("value");
  sql_field[3].column_value_.set_value("[{\"vip\": \"127.0.0.3\", \"value\": 4000}, {\"vip\": \"127.0.0.2\", \"value\": 7777}]");
  sql_result.fields_.push_back(sql_field[3]);
  cloud_params.fields_ = &sql_result;

  ASSERT_EQ(0, ObResourceUnitTableProcessor::execute(&cloud_params));
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ASSERT_EQ(3, t_processor_.get_conn_map_count());
  conn_map_def conn_map_1[3] = {
    {"ob_tenant#ob_cluster|127.0.0.3", 4000},
    {"ob_tenant#ob_cluster|127.0.0.2", 7777},
    {"ob_tenant#ob_cluster|127.0.0.1", 5000}
  };
  ASSERT_EQ(0, check_map_value(conn_map_1, 3));
  LOG_DEBUG("success2");

  ObResourceUnitTableProcessor::commit(&cloud_params, false);
  ObVipTenantConnCache::VTHashMap* vt_conn_map = t_processor_.get_conn_map();
  ObVipTenantConnCache::dump_conn_map(*vt_conn_map);
  conn_map_def conn_map_2[2] = {
    {"ob_tenant#ob_cluster|127.0.0.1", 5000},
    {"ob_tenant#ob_cluster|127.0.0.2", 6000},
  };
  ASSERT_EQ(0, check_map_value(conn_map_2, 2));
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ASSERT_EQ(2, t_processor_.get_conn_map_count());
}

TEST_F(TestTenantProcessor, test_delete_tenant)
{
  ASSERT_EQ(0, init_env());

  SqlFieldResult sql_result;
  sql_result.field_num_ = 4;
  SqlField sql_field[4];
  sql_field[0].column_name_.set("cluster");
  sql_field[0].column_value_.set_value("ob_cluster");
  sql_result.fields_.push_back(sql_field[0]);

  sql_field[1].column_name_.set("tenant");
  sql_field[1].column_value_.set_value("ob_tenant");
  sql_result.fields_.push_back(sql_field[1]);

  sql_field[2].column_name_.set("name");
  sql_field[2].column_value_.set_value("resource_max_connections");
  sql_result.fields_.push_back(sql_field[2]);

  sql_field[3].column_name_.set("value");
  sql_field[3].column_value_.set_value("[{\"vip\": \"127.0.0.1\", \"value\": 5000}, {\"vip\": \"127.0.0.2\", \"value\": 6000}]");
  sql_result.fields_.push_back(sql_field[3]);

  ObCloudFnParams cloud_params;
  cloud_params.stmt_type_ = OBPROXY_T_REPLACE;
  cloud_params.fields_ = &sql_result;
  cloud_params.cluster_name_ = "ob_cluster";
  cloud_params.tenant_name_ = "ob_tenant";
  cloud_params.table_name_ = "resource_unit";

  ASSERT_EQ(0, get_global_config_processor().init());
  ASSERT_EQ(0, ObResourceUnitTableProcessor::execute(&cloud_params));
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ASSERT_EQ(2, t_processor_.get_conn_map_count());
  conn_map_def conn_map[2] = {
    {"ob_tenant#ob_cluster|127.0.0.2", 6000},
    {"ob_tenant#ob_cluster|127.0.0.1", 5000}
  };
  ASSERT_EQ(0, check_map_value(conn_map, 2));
  LOG_DEBUG("success1");

  sql_result.reset();
  sql_result.field_num_ = 4;
  sql_result.fields_.push_back(sql_field[0]);
  sql_field[1].column_name_.set("tenant");
  sql_field[1].column_value_.set_value("ob_tenant_1");
  sql_result.fields_.push_back(sql_field[1]);
  sql_result.fields_.push_back(sql_field[2]);
  sql_field[3].column_name_.set("value");
  sql_field[3].column_value_.set_value("[{\"vip\": \"127.0.0.3\", \"value\": 4000}, {\"vip\": \"127.0.0.2\", \"value\": 7777}]");
  sql_result.fields_.push_back(sql_field[3]);
  cloud_params.fields_ = &sql_result;
  cloud_params.tenant_name_ = "ob_tenant_1";

  ASSERT_EQ(0, ObResourceUnitTableProcessor::execute(&cloud_params));
  ObVipTenantConnCache::VTHashMap* vt_conn_map = t_processor_.get_conn_map();
  ObVipTenantConnCache::dump_conn_map(*vt_conn_map);
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ASSERT_EQ(4, t_processor_.get_conn_map_count());
  conn_map_def conn_map_1[4] = {
    {"ob_tenant_1#ob_cluster|127.0.0.2", 7777},
    {"ob_tenant_1#ob_cluster|127.0.0.3", 4000},
    {"ob_tenant#ob_cluster|127.0.0.2", 6000},
    {"ob_tenant#ob_cluster|127.0.0.1", 5000}
  };
  ASSERT_EQ(0, check_map_value(conn_map_1, 4));
  LOG_DEBUG("success2");

  // delete configuration
  cloud_params.stmt_type_ = OBPROXY_T_DELETE;
  cloud_params.tenant_name_ = "ob_tenant_1";
  ASSERT_EQ(0, ObResourceUnitTableProcessor::execute(&cloud_params));
  vt_conn_map = t_processor_.get_conn_map();
  ObVipTenantConnCache::dump_conn_map(*vt_conn_map);
  LOG_DEBUG("conn map", "count", t_processor_.get_conn_map_count());
  ASSERT_EQ(2, t_processor_.get_conn_map_count());
  conn_map_def conn_map_2[2] = {
    {"ob_tenant#ob_cluster|127.0.0.2", 6000},
    {"ob_tenant#ob_cluster|127.0.0.1", 5000}
  };
  ASSERT_EQ(0, check_map_value(conn_map_2, 2));
  LOG_DEBUG("success3");
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
