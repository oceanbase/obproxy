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
#define private public
#define protected public

#include <gtest/gtest.h>
#include "easy_io_struct.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/string/ob_sql_string.h"
#include "obproxy/proxy/mysqllib/ob_proxy_session_info.h"
#include "lib/allocator/ob_malloc.h"
#include "ob_session_vars_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::proxy;
namespace oceanbase
{
namespace obproxy
{
ObDefaultSysVarSet g_default_sys_var_set;
ObArenaAllocator g_allocator(ObModIds::TEST);
class TestProxySessionInfo : public ::testing::Test
{
public:
  static void SetUpTestCase();
  static void TearDownTestCase() {}
};

void TestProxySessionInfo::SetUpTestCase()
{
  ASSERT_EQ(OB_SUCCESS, g_default_sys_var_set.init());
  ObSessionVarsTestUtils::load_default_system_variables(g_allocator, g_default_sys_var_set, true);
}

//--------test ObClientSessionInfo-----------//
TEST_F(TestProxySessionInfo, basic_func)
{
  ObServerSessionInfo session;
  session.reset();
  ObAddr server(static_cast<ObAddr::VER>(4), "10.232.4.4", 3300);
  session.version_.sys_var_version_ = 100;
  session.version_.hot_sys_var_version_ = 100;
  session.version_.user_var_version_ = 1000;
  session.version_.db_name_version_ = 10;
  session.set_ob_server(server);
  ASSERT_EQ(session.get_ob_server(), server);

  ASSERT_EQ(session.get_sys_var_version(), 100);
  ASSERT_EQ(session.get_user_var_version(), 1000);
  ASSERT_EQ(session.get_hot_sys_var_version(), 100);
  ASSERT_EQ(session.get_db_name_version(), 10);
  LOG_INFO("to_string of ObServerSessionInfo", K(session));
}

TEST_F(TestProxySessionInfo, set_get_func)
{
  ObClientSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, session.init());
  ASSERT_EQ(OB_INIT_TWICE, session.init());
  LOG_INFO("to_string of ObServerSessionInfo", K(session));
  ASSERT_EQ(OB_SUCCESS, session.set_tenant_name(ObString::make_string("OB")));
  ASSERT_EQ(OB_SUCCESS, session.set_user_name(ObString::make_string("yyy")));
  ASSERT_EQ(OB_SUCCESS, session.set_database_name(ObString::make_string("obproxy")));
  ObString tenant_name;
  ObString user_name;
  ObString database_name;
  ASSERT_EQ(OB_SUCCESS, session.get_tenant_name(tenant_name));
  ASSERT_EQ(ObString::make_string("OB"), tenant_name);
  ASSERT_EQ(OB_SUCCESS, session.get_user_name(user_name));
  ASSERT_EQ(ObString::make_string("yyy"), user_name);
  ASSERT_EQ(OB_SUCCESS, session.get_database_name(database_name));
  ASSERT_EQ(ObString::make_string("obproxy"), database_name);
  ASSERT_EQ(session.get_db_name_version(), 1);
  ASSERT_EQ(OB_SUCCESS, session.set_database_name(ObString::make_string("hust")));
  ASSERT_EQ(OB_SUCCESS, session.get_database_name(database_name));
  ASSERT_EQ(ObString::make_string("hust"), database_name);
  ASSERT_EQ(session.get_db_name_version(), 2);
}

TEST_F(TestProxySessionInfo, not_init)
{
  ObClientSessionInfo session;
  ObServerSessionInfo server_session;
  ObSqlString sql_str;
  ObString var_name;
  ObObj value;
  ObObjType type;
  ObSessionSysField *sys_field = NULL;
  ObSessionUserField *user_field = NULL;
  bool is_exist = false;
  var_name = ObString::make_string("autocommit");
  ASSERT_EQ(OB_INVALID_ARGUMENT, session.update_sys_variable(ObString::make_empty_string(), value));
  ASSERT_EQ(OB_INVALID_ARGUMENT, session.get_sys_variable(ObString::make_empty_string(), sys_field));
  ASSERT_EQ(OB_INVALID_ARGUMENT, session.get_sys_variable_value(ObString::make_empty_string(), value));
  ASSERT_EQ(OB_INVALID_ARGUMENT, session.get_sys_variable_value(NULL, value));
  ASSERT_EQ(OB_INVALID_ARGUMENT, session.sys_variable_exists(ObString::make_empty_string(), is_exist));
  ASSERT_EQ(OB_INVALID_ARGUMENT, session.update_sys_variable(ObString::make_empty_string(), value));
  ASSERT_EQ(OB_INVALID_ARGUMENT, session.get_sys_variable_type(ObString::make_empty_string(), type));

  ASSERT_EQ(OB_INVALID_ARGUMENT, session.replace_user_variable(ObString::make_empty_string(), value));
  ASSERT_EQ(OB_INVALID_ARGUMENT, session.remove_user_variable(ObString::make_empty_string()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, session.get_user_variable(ObString::make_empty_string(), user_field));
  ASSERT_EQ(OB_INVALID_ARGUMENT, session.get_user_variable_value(ObString::make_empty_string(), value));
  ASSERT_EQ(OB_INVALID_ARGUMENT, session.user_variable_exists(ObString::make_empty_string(), is_exist));

  ASSERT_EQ(OB_NOT_INIT, session.update_sys_variable(var_name, value));
  ASSERT_EQ(OB_NOT_INIT, session.get_sys_variable(var_name, sys_field));
  ASSERT_EQ(OB_NOT_INIT, session.get_sys_variable_value(var_name, value));
  char var[100] = "aaa";
  ASSERT_EQ(OB_NOT_INIT, session.get_sys_variable_value(var, value));
  ASSERT_EQ(OB_NOT_INIT, session.sys_variable_exists(var_name, is_exist));
  ASSERT_EQ(OB_NOT_INIT, session.get_sys_variable_type(var_name, type));

  ASSERT_EQ(OB_NOT_INIT, session.replace_user_variable(var_name, value));
  ASSERT_EQ(OB_NOT_INIT, session.remove_user_variable(var_name));
  ASSERT_EQ(OB_NOT_INIT, session.get_user_variable(var_name, user_field));
  ASSERT_EQ(OB_NOT_INIT, session.get_user_variable_value(var_name, value));
  ASSERT_EQ(OB_NOT_INIT, session.user_variable_exists(var_name, is_exist));
  ASSERT_EQ(OB_NOT_INIT, session.extract_variable_reset_sql(&server_session, sql_str));
}

TEST_F(TestProxySessionInfo, sys_variable_func)
{
  //ASSERT_EQ(OB_SUCCESS, g_default_sys_var_set.init());
  ObClientSessionInfo session;
  ObString var_name;
  ObObjType type;
  ObSessionSysField *sys_field = NULL;
  var_name = ObString::make_string("autocommit");
  ASSERT_EQ(OB_SUCCESS, session.init());
  ASSERT_EQ(OB_SUCCESS, session.add_sys_var_set(g_default_sys_var_set));
  ObObj value;
  ObObj value_out;
  value.set_int(0);
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, session.update_sys_variable(var_name, value));
  ASSERT_EQ(OB_SUCCESS, session.get_sys_variable(var_name, sys_field));
  ASSERT_EQ(ObString::make_string("autocommit"), sys_field->get_variable_name_str());
  ASSERT_EQ(OB_FIELD_USED, sys_field->stat_);
  ASSERT_EQ(ObIntType, sys_field->type_);
  ASSERT_EQ(0, value.compare(sys_field->value_));

  ASSERT_EQ(OB_SUCCESS, session.get_sys_variable_value(var_name, value_out));
  ASSERT_EQ(value_out, value);
  char var[100] = "autocommit";
  ASSERT_EQ(OB_SUCCESS, session.get_sys_variable_value(var, value));
  ASSERT_EQ(value_out, value);
  ASSERT_EQ(OB_SUCCESS, session.sys_variable_exists(var_name, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, session.get_sys_variable_type(var_name, type));
  ASSERT_EQ(ObIntType, type);
}

TEST_F(TestProxySessionInfo, user_variable_func)
{
  ObClientSessionInfo session;
  ObServerSessionInfo server_session;
  ObString var_name;
  ObSessionUserField *user_field = NULL;
  var_name = ObString::make_string("autocommit");
  ASSERT_EQ(OB_SUCCESS, session.init());
  ObObj value;
  ObObj value_out;
  value.set_int(0);
  bool is_exist = false;

  ASSERT_EQ(OB_SUCCESS, session.replace_user_variable(var_name, value));
  ASSERT_EQ(OB_SUCCESS, session.remove_user_variable(var_name));
  ASSERT_EQ(OB_SUCCESS, session.replace_user_variable(var_name, value));
  ASSERT_EQ(OB_SUCCESS, session.get_user_variable(var_name, user_field));
  ASSERT_EQ(OB_FIELD_USED, user_field->stat_);

  ASSERT_EQ(OB_SUCCESS, session.get_user_variable_value(var_name, value_out));
  ASSERT_EQ(value_out, value);
  ASSERT_EQ(OB_SUCCESS, session.user_variable_exists(var_name, is_exist));
  ASSERT_EQ(true, is_exist);

}

TEST_F(TestProxySessionInfo, extract_variable_reset_sql_func)
{
  ObClientSessionInfo session;
  ObServerSessionInfo server_session;
  ObSqlString sql_str;
  ASSERT_EQ(OB_NOT_INIT, session.extract_variable_reset_sql(&server_session, sql_str));
  ASSERT_EQ(OB_SUCCESS, session.init());
  ASSERT_EQ(OB_SUCCESS, session.add_sys_var_set(g_default_sys_var_set));
  ASSERT_EQ(OB_INVALID_ARGUMENT, session.extract_variable_reset_sql(NULL, sql_str));
  ASSERT_EQ(OB_SUCCESS, session.extract_variable_reset_sql(&server_session, sql_str));
  ASSERT_TRUE(sql_str.empty());
  server_session.set_sys_var_version(0);
  server_session.set_user_var_version(0);

  ObObj value;
  value.set_int(0);
  sql_str.reset();

  ObObj isolation;
  ObObj isolation_out;
  isolation.set_varchar(ObString::make_string("READ-COMMITTED"));
  isolation.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_SUCCESS, session.update_sys_variable(ObString::make_string("tx_isolation"), isolation));
  ObString user_var_name = ObString::make_string("yyy");
  ASSERT_EQ(OB_SUCCESS, session.replace_user_variable(user_var_name, value));
  ASSERT_EQ(OB_SUCCESS, session.extract_variable_reset_sql(&server_session, sql_str));
  ObString sql_reset(sql_str.length(), sql_str.ptr());
  LOG_INFO("sql reset", K(sql_reset));
  ASSERT_EQ(sql_reset, ObString::make_string("SET @@tx_isolation = 'READ-COMMITTED', @yyy = 0;"));

}
}//end of obproxy
}//end of oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
