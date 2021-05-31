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
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "lib/number/ob_number_v2.h"
#include "obproxy/proxy/mysqllib/ob_session_field_mgr.h"
#include "ob_session_vars_test_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
ObDefaultSysVarSet g_default_sys_var_set;
ObArenaAllocator g_allocator(ObModIds::TEST);
class TestSessionFieldMgr : public ::testing::Test
{
public:
  static void SetUpTestCase();
  static void TearDownTestCase() {}
  ObSessionFieldMgr mgr_;
};

void TestSessionFieldMgr::SetUpTestCase()
{
  ObSessionSysField *value = NULL;
  bool is_exist = false;
  ASSERT_EQ(OB_NOT_INIT, g_default_sys_var_set.get_sys_variable(ObString::make_empty_string(), value));
  ASSERT_EQ(OB_NOT_INIT, g_default_sys_var_set.sys_variable_exists(ObString::make_empty_string(), is_exist));
  ASSERT_EQ(OB_SUCCESS, g_default_sys_var_set.init());
  ObSessionVarsTestUtils::load_default_system_variables(g_allocator, g_default_sys_var_set, true);
}

TEST_F(TestSessionFieldMgr, test_str_func)
{
  ObString tenant_name;
  ObString user_name;
  ObString database_name;
  ASSERT_EQ(OB_SUCCESS, mgr_.init());
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr_.set_tenant_name(ObString::make_empty_string()));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr_.get_tenant_name(tenant_name));
  ASSERT_EQ(OB_SUCCESS, mgr_.set_tenant_name(ObString::make_string("yyy")));
  ASSERT_EQ(OB_SUCCESS, mgr_.set_user_name(ObString::make_string("admin")));
  ASSERT_EQ(OB_SUCCESS, mgr_.set_user_name(ObString::make_string("aa")));
  ASSERT_EQ(OB_SUCCESS, mgr_.set_user_name(ObString::make_string("aaaaaaaaa")));
  ASSERT_EQ(OB_SUCCESS, mgr_.set_user_name(ObString::make_string("admin")));
  ASSERT_EQ(OB_SUCCESS, mgr_.set_database_name(ObString::make_string("test")));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_tenant_name(tenant_name));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_user_name(user_name));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_database_name(database_name));
  ASSERT_EQ(tenant_name, ObString::make_string("yyy"));
  ASSERT_EQ(user_name, ObString::make_string("admin"));
  ASSERT_EQ(database_name, ObString::make_string("test"));
  ObSessionVField field;
  ASSERT_TRUE(field.is_empty());
  LOG_INFO("v field", K(field));
}

TEST_F(TestSessionFieldMgr, test_sys_variable)
{
  ObObj autocommit;
  ObSessionSysField *field = NULL;
  autocommit.set_int(0);
  //not init
  ASSERT_EQ(OB_NOT_INIT, mgr_.update_system_variable(ObString::make_string("autocommit"), autocommit, field));
  ASSERT_EQ(NULL, field);
  ASSERT_EQ(OB_NOT_INIT, mgr_.get_sys_variable(ObString::make_string("autocommit"), field));
  ObObj value;
  ASSERT_EQ(OB_NOT_INIT, mgr_.get_sys_variable_value(ObString::make_string("autocommit"), value));
  char buf[20] = "autocommit";
  ASSERT_EQ(OB_NOT_INIT, mgr_.get_sys_variable_value(buf, value));
  bool is_exist = false;
  ASSERT_EQ(OB_NOT_INIT, mgr_.sys_variable_exists(ObString::make_string("autocommit"), is_exist));
  ObObjType type;
  ASSERT_EQ(OB_NOT_INIT, mgr_.get_sys_variable_type(ObString::make_string("autocommit"), type));

  ASSERT_EQ(OB_SUCCESS, mgr_.init());
  mgr_.set_sys_var_set(&g_default_sys_var_set);
  field = NULL;
  ASSERT_EQ(OB_SUCCESS, mgr_.update_system_variable(ObString::make_string("autocommit"), autocommit, field));
  ASSERT_TRUE(NULL != field);
  ASSERT_EQ(OB_FIELD_HOT_MODIFY_MOD, field->modify_mod_);
  value.reset();
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sys_variable_value(ObString::make_string("autocommit"), value));
  ASSERT_EQ(autocommit, value);
  autocommit.set_int(1);
  field = NULL;
  ASSERT_EQ(OB_SUCCESS, mgr_.update_system_variable(ObString::make_string("autocommit"), autocommit, field));
  ASSERT_TRUE(NULL != field);
  ASSERT_EQ(OB_FIELD_HOT_MODIFY_MOD, field->modify_mod_);
  ObObj isolation;
  ObObj isolation_out;
  isolation.set_varchar(ObString::make_string("READ-COMMITTED"));
  isolation.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sys_variable_value(ObString::make_string("tx_isolation"), isolation_out));
  _OB_LOG(INFO,"in:%s, out:%s", to_cstring(isolation), to_cstring(isolation_out));
  ASSERT_EQ(0, isolation.compare(isolation_out, CS_TYPE_UTF8MB4_GENERAL_CI));
  field = NULL;
  ASSERT_EQ(OB_SUCCESS, mgr_.update_system_variable(ObString::make_string("tx_isolation"), isolation, field));
  ASSERT_TRUE(NULL != field);
  ASSERT_EQ(OB_FIELD_COLD_MODIFY_MOD, field->modify_mod_);
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sys_variable_value(ObString::make_string("tx_isolation"), isolation_out));
  ASSERT_EQ(0, isolation.compare(isolation_out, CS_TYPE_UTF8MB4_GENERAL_CI));
  isolation.set_varchar(ObString::make_string("READ-STATIC"));
  isolation.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  field = NULL;
  ASSERT_EQ(OB_SUCCESS, mgr_.update_system_variable(ObString::make_string("tx_isolation"), isolation, field));
  ASSERT_TRUE(NULL != field);
  ASSERT_EQ(OB_FIELD_COLD_MODIFY_MOD, field->modify_mod_);
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sys_variable_value(ObString::make_string("tx_isolation"), isolation_out));
  ASSERT_EQ(0, isolation.compare(isolation_out, CS_TYPE_UTF8MB4_GENERAL_CI));


  ASSERT_EQ(OB_ERR_SYS_VARIABLE_UNKNOWN, mgr_.get_sys_variable(ObString::make_string("not_exist_sys_variable"), field));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sys_variable(ObString::make_string("autocommit"), field));
  ASSERT_EQ(ObString::make_string("autocommit"), field->get_variable_name_str());
  ASSERT_EQ(OB_FIELD_USED, field->stat_);
  ASSERT_EQ(ObIntType, field->type_);
  ASSERT_EQ(0, autocommit.compare(field->value_));
  ASSERT_EQ(ObString::make_string("autocommit"), field->get_variable_name_str());
  value.reset();
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sys_variable_value(ObString::make_string("autocommit"), value));
  ASSERT_EQ(autocommit, value);
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr_.get_sys_variable_value(NULL, value));
  value.reset();
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sys_variable_value(buf, value));
  ASSERT_EQ(autocommit, value);
  ASSERT_EQ(OB_SUCCESS, mgr_.sys_variable_exists(ObString::make_string("autocommit"), is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, mgr_.sys_variable_exists(ObString::make_string("wait_timeout"), is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, mgr_.sys_variable_exists(ObString::make_string("not_exist"), is_exist));
  ASSERT_TRUE(false == is_exist);
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sys_variable_type(ObString::make_string("autocommit"), type));
  ASSERT_EQ(ObIntType, type);
  field->reset();
  ASSERT_EQ(OB_FIELD_EMPTY, field->stat_);
  ASSERT_EQ(ObNullType, field->type_);
//ASSERT_EQ(ObSysVarFlag::INVALID, field->scope_);
  ObFieldBaseMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init());
  ASSERT_EQ(OB_INIT_TWICE, mgr.init());

}

TEST_F(TestSessionFieldMgr, test_user_variable)
{
  ObObj value_in;
  ObObj value_out;
  ObSessionUserField *field_out = NULL;
  bool is_exist = false;
  ObSqlString sql;
  ASSERT_EQ(OB_NOT_INIT, mgr_.replace_user_variable(ObString::make_string("yyy"), value_out));
  ASSERT_EQ(OB_NOT_INIT, mgr_.remove_user_variable(ObString::make_string("yyy")));
  ASSERT_EQ(OB_NOT_INIT, mgr_.get_user_variable(ObString::make_string("yyy"), field_out));
  ASSERT_EQ(OB_NOT_INIT, mgr_.get_user_variable_value(ObString::make_string("yyy"), value_out));
  ASSERT_EQ(OB_NOT_INIT, mgr_.user_variable_exists(ObString::make_string("yyy"), is_exist));
  ASSERT_EQ(OB_NOT_INIT, mgr_.format_sys_var(sql));
  ASSERT_EQ(OB_NOT_INIT, mgr_.format_user_var(sql));

  ASSERT_EQ(OB_SUCCESS, mgr_.init());
  mgr_.set_sys_var_set(&g_default_sys_var_set);
  ASSERT_EQ(OB_INIT_TWICE, mgr_.init());
  value_in.set_int(10);

  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr_.get_user_variable(ObString::make_string("yyy"), field_out));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr_.get_user_variable_value(ObString::make_string("yyy"), value_out));

  ASSERT_EQ(OB_SUCCESS, mgr_.replace_user_variable(ObString::make_string("yyy"), value_in));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_user_variable(ObString::make_string("yyy"), field_out));
  ASSERT_EQ(OB_FIELD_USED, field_out->stat_);
  ASSERT_EQ(OB_SUCCESS, mgr_.get_user_variable_value(ObString::make_string("yyy"), value_out));
  ASSERT_EQ(value_out, value_in);
  LOG_INFO("test to_string func", K(*field_out));

  ASSERT_EQ(OB_SUCCESS, mgr_.remove_user_variable(ObString::make_string("yyy")));
  value_in.set_varchar(ObString::make_string("aaaa"));
  value_in.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_SUCCESS, mgr_.replace_user_variable(ObString::make_string("yyy"), value_in));
  value_in.set_varchar(ObString::make_string("aaaaaaaaaa"));
  value_in.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_SUCCESS, mgr_.replace_user_variable(ObString::make_string("yyy"), value_in));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_user_variable(ObString::make_string("yyy"), field_out));
  ASSERT_EQ(OB_FIELD_USED, field_out->stat_);
  ASSERT_EQ(0, value_in.compare(field_out->value_, CS_TYPE_UTF8MB4_GENERAL_CI));

  ASSERT_EQ(OB_SUCCESS, mgr_.get_user_variable_value(ObString::make_string("yyy"), value_out));
  ASSERT_EQ(value_out.get_varchar(), value_in.get_varchar());
  ASSERT_EQ(0, value_in.compare(value_out, CS_TYPE_UTF8MB4_GENERAL_CI));
  ASSERT_EQ(OB_SUCCESS, mgr_.user_variable_exists(ObString::make_string("yyy"), is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, mgr_.user_variable_exists(ObString::make_string("aaa"), is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestSessionFieldMgr, test_format)
{
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, mgr_.init());
  mgr_.set_sys_var_set(&g_default_sys_var_set);

  ObObj autocommit;
  ObObj value_out;
  autocommit.set_int(0);
  number::ObNumber number;
  char buf[1024];
  ModuleArena arena;
  snprintf(buf,1024, "123456.789");
  int ret = number.from(buf, arena);
  ObSessionSysField *field = NULL;
  ASSERT_EQ(OB_SUCCESS, ret);
  value_out.set_number(number);
  field = NULL;
  ASSERT_EQ(OB_SUCCESS, mgr_.update_system_variable(ObString::make_string("autocommit"), autocommit, field));
  ASSERT_TRUE(NULL != field);
  ASSERT_EQ(OB_FIELD_HOT_MODIFY_MOD, field->modify_mod_);
  ASSERT_EQ(OB_SUCCESS, mgr_.replace_user_variable(ObString::make_string("yyy"), value_out));
  value_out.set_varchar("bbb");
  ASSERT_EQ(OB_SUCCESS, mgr_.replace_user_variable(ObString::make_string("aaa"), value_out));
  ASSERT_EQ(OB_SUCCESS, mgr_.format_hot_sys_var(sql));
  ObString sql_out(sql.length(), sql.ptr());
  _OB_LOG(WARN, "sql_out:%s", to_cstring(sql_out));
  ASSERT_EQ(sql_out, ObString::make_string(" @@autocommit = 0,"));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, mgr_.format_user_var(sql));
  ObString sql_out_user(sql.length(), sql.ptr());
  _OB_LOG(WARN, "sql_out_user:%s", to_cstring(sql_out_user));
  ASSERT_EQ(sql_out_user, ObString::make_string(" @yyy = 123456.789, @aaa = bbb,"));
}

TEST_F(TestSessionFieldMgr, test_Default_sys_var_set)
{
  ASSERT_EQ(OB_INIT_TWICE, g_default_sys_var_set.init());
  ObObj type;
  ObObj value;
  ASSERT_EQ(OB_SUCCESS, g_default_sys_var_set.load_system_variable(ObString::make_string("yyy"), type,value, 1));
  ASSERT_EQ(OB_ERR_ALREADY_EXISTS, g_default_sys_var_set.load_system_variable(ObString::make_string("yyy"), type,value, 1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, g_default_sys_var_set.load_system_variable(ObString::make_empty_string(), type,value, 1));
}

TEST_F(TestSessionFieldMgr, test_move_strings)
{
  ASSERT_EQ(OB_SUCCESS, mgr_.init());
  mgr_.set_sys_var_set(&g_default_sys_var_set);

  ObObj value;
  static const int64_t BUF_SIZE = 1025;
  char buf[BUF_SIZE];
  for (int64_t i = 0; i < BUF_SIZE; ++i) {
    buf[i] = 'y';
  }
  buf[BUF_SIZE - 1] = '\0';
  value.set_varchar(buf);
  value.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

  LOG_INFO("replace 1");
  ASSERT_TRUE(OB_SUCCESS == mgr_.replace_user_variable(ObString::make_string("1"), value));
  LOG_INFO("remove 1");
  ASSERT_TRUE(OB_SUCCESS == mgr_.remove_user_variable(ObString::make_string("1")));
  LOG_INFO("NEXT REFORM");
  LOG_INFO("replace 2");
  ASSERT_TRUE(OB_SUCCESS == mgr_.replace_user_variable(ObString::make_string("2"), value));
  LOG_INFO("NEXT DEMOTE");
  LOG_INFO("replace 3");
  ASSERT_TRUE(OB_SUCCESS == mgr_.replace_user_variable(ObString::make_string("3"), value));
  LOG_INFO("NEXT DEMOTE");
  LOG_INFO("replace 4");
  ASSERT_TRUE(OB_SUCCESS == mgr_.replace_user_variable(ObString::make_string("4"), value));
  LOG_INFO("NEXT DEMOTE");
  LOG_INFO("replace 5");
  ASSERT_TRUE(OB_SUCCESS == mgr_.replace_user_variable(ObString::make_string("5"), value));
  LOG_INFO("NEXT DEMOTE AND REFROM");
  LOG_INFO("replace 6");
  ObObj autocommit;
  ObObj value_out;
  autocommit.set_int(0);
  ObSessionSysField *field = NULL;

  ASSERT_EQ(OB_SUCCESS, mgr_.update_system_variable(ObString::make_string("autocommit"), autocommit, field));
  ASSERT_TRUE(NULL != field);
  ASSERT_EQ(OB_FIELD_HOT_MODIFY_MOD, field->modify_mod_);
  ASSERT_TRUE(OB_SUCCESS == mgr_.set_user_name(ObString::make_string("yyyyyyy")));
  ASSERT_TRUE(OB_SUCCESS == mgr_.set_tenant_name(ObString::make_string("admin")));
  ASSERT_TRUE(OB_SUCCESS == mgr_.set_database_name(ObString::make_string("test")));
  ObObj isolation;
  ObObj isolation_out;
  isolation.set_varchar(ObString::make_string("READ-COMMITTED"));
  isolation.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  field = NULL;
  ASSERT_EQ(OB_SUCCESS, mgr_.update_system_variable(ObString::make_string("tx_isolation"), isolation, field));
  ASSERT_TRUE(NULL != field);
  ASSERT_EQ(OB_FIELD_COLD_MODIFY_MOD, field->modify_mod_);


  ASSERT_TRUE(OB_SUCCESS == mgr_.replace_user_variable(ObString::make_string("6"), value));
  //LOG_INFO("replace 7");
  ////here will demote and reform
  //ASSERT_TRUE(OB_SUCCESS == mgr_.replace_user_variable(ObString::make_string("7"), value));
}

}//end of namespace proxy
}//end of namespace obproxy
}//end of namespace oceanbase
int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
