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
#include "obproxy/obutils/ob_proxy_config.h"

namespace oceanbase
{
namespace obproxy
{
using namespace common;
using namespace obutils;
class TestProxyConfig : public ::testing::Test
{
};

TEST_F(TestProxyConfig, test_all)
{
  ObProxyConfig config;
  ASSERT_EQ(60*1000*1000, config.proxy_info_check_interval);
  ASSERT_STREQ("undefined", config.app_name);
  ASSERT_EQ(800*1024*1024, config.proxy_mem_limited);



  config.print();

  ASSERT_EQ(OB_SUCCESS, config.check_all());
  //invalid value,but return OB_SUCCESS and recover it with origin value
  ASSERT_EQ(OB_INVALID_CONFIG, config.update_config_item(ObString::make_string("client_max_connections"), ObString::make_string("-1")));
  ASSERT_EQ(8192, config.client_max_connections);
  config.print();
  ASSERT_EQ(OB_INVALID_CONFIG, config.update_config_item(ObString::make_string("client_max_connections"), ObString::make_string("adb")));
  ASSERT_EQ(8192, config.client_max_connections);
  config.print();
  ASSERT_EQ(OB_INVALID_CONFIG, config.update_config_item(ObString::make_string("client_max_connections"), ObString::make_string("100000")));
  ASSERT_EQ(8192, config.client_max_connections);
  config.print();
  //config item not exit, return OB_SUCCESS
  ASSERT_EQ(OB_ERR_SYS_CONFIG_UNKNOWN, config.update_config_item(ObString::make_string("xxxxxxx"), ObString::make_string("4")));
  ASSERT_EQ(OB_SUCCESS, config.update_user_config_item(ObString::make_string("xxxxxxx"), ObString::make_string("4")));
  ASSERT_EQ(OB_SUCCESS, config.update_user_config_item(ObString::make_string("proxy_id"), ObString::make_string("4")));
};

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

