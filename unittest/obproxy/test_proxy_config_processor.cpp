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

#include <gtest/gtest.h>
#include "../unittest/share/schema/db_initializer.h"
#include "../unittest/share/schema/ob_schema_test_utils.cpp"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "common/mysql_proxy/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "obproxy/obutils/ob_proxy_config_processor.h"
#include "obproxy/obutils/ob_proxy_reload_config.h"
#include "lib/time/ob_time_utility.h"

#define OB_INSERT_PROXY_CONFIG_SQL                                               \
    "INSERT INTO __all_proxy_config(name, value, version, value_strict,"         \
    " info, need_reboot)  VALUES ('%s', '%s', '%ld','xxx', 'xxx', 1)"

#define OB_UPDATE_PROXY_CONFIG_SQL                                               \
    "UPDATE __all_proxy_config SET value = '%s', version = %ld WHERE name = '%s'"

namespace oceanbase
{
namespace obproxy
{
using namespace common;
using namespace share::schema;

class TestProxyConfigProcessor : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}
  void insert_config_item(const char *name, const char *value);
  void update_config_item(const char *name, const char *value);
protected:
  DBInitializer db_initer_;
  ObSchemaServiceSQLImpl schema_service_;
};

void TestProxyConfigProcessor::insert_config_item(const char *name, const char *value)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObMySQLProxy::MySQLResult res;
  int64_t affected_row = 0;
  ret = trans.start(&db_initer_.get_sql_proxy());
  ASSERT_EQ(OB_SUCCESS, ret);
  char sql[OB_MAX_SQL_LENGTH] = {'\0'};
  snprintf(sql, OB_MAX_SQL_LENGTH, OB_INSERT_PROXY_CONFIG_SQL,
           name, value, ObTimeUtility::current_time());
  ret = trans.write(sql, affected_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_row);

  bool is_commit = (OB_SUCC(ret));
  ret = trans.end(is_commit);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestProxyConfigProcessor::update_config_item(const char *name, const char *value)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObMySQLProxy::MySQLResult res;
  int64_t affected_row = 0;
  ret = trans.start(&db_initer_.get_sql_proxy());
  ASSERT_EQ(OB_SUCCESS, ret);
  char sql[OB_MAX_SQL_LENGTH] = {'\0'};
  snprintf(sql, OB_MAX_SQL_LENGTH, OB_UPDATE_PROXY_CONFIG_SQL,
           value, ObTimeUtility::current_time(), name);
  ret = trans.write(sql, affected_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_row);

  bool is_commit = (OB_SUCC(ret));
  ret = trans.end(is_commit);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestProxyConfigProcessor::SetUp()
{
  int ret = db_initer_.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  const bool only_core_tables = false;
  ret = db_initer_.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = db_initer_.fill_sys_stat_table();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_service_.init(&db_initer_.get_sql_proxy());
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestProxyConfigProcessor, test_all)
{
  ObProxyConfig config;
  ObProxyReloadConfig reload_config(&config);
  ObProxyConfigProcessor processor(config, reload_config);

  ASSERT_EQ(OB_NOT_INIT, processor.start_update_config());
  ASSERT_EQ(OB_NOT_INIT, processor.update_proxy_config_manual());

  ASSERT_EQ(OB_INVALID_ARGUMENT, processor.init(NULL));
  ASSERT_EQ(OB_SUCCESS, processor.init(&db_initer_.get_sql_proxy()));
  ASSERT_EQ(OB_INIT_TWICE, processor.init(&db_initer_.get_sql_proxy()));

  ASSERT_EQ(OB_SUCCESS, processor.update_proxy_config_manual());
  ASSERT_EQ(OB_SUCCESS, processor.start_update_config());
  ASSERT_EQ(OB_ERR_ALREADY_EXISTS, processor.start_update_config());
  insert_config_item("retry_times", "10");
  sleep(3);
  ASSERT_EQ(10, config.retry_times);


  ASSERT_STREQ("obproxy", config.proxy_name);
  ASSERT_EQ(2*1000*1000, config.network_timeout);
  insert_config_item("proxy_name", "obproxyob");
  insert_config_item("network_timeout", "3s");
  sleep(3);
  ASSERT_STREQ("obproxyob", config.proxy_name);
  ASSERT_EQ(3*1000*1000, config.network_timeout);

  update_config_item("proxy_name", "OBPROXYOB");
  update_config_item("network_timeout", "1s");
  sleep(3);
  ASSERT_STREQ("OBPROXYOB", config.proxy_name);
  ASSERT_EQ(1*1000*1000, config.network_timeout);

  insert_config_item("config_update_interval", "1s");
  sleep(3);

  //invalid config, will not enable
  insert_config_item("xxxxxx", "xxxxx");
  sleep(2);

  //invalid value, will not be valid
  update_config_item("network_timeout", "sss");
  sleep(2);
  ASSERT_EQ(1*1000*1000, config.network_timeout);

  processor.destroy();
  sleep(1);
};

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

