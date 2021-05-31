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
#include "lib/ob_define.h"
#include "iocore/net/ob_net_def.h"
#include "utils/ob_proxy_hot_upgrader.h"
#include "obutils/ob_proxy_config.h"
#include "obutils/ob_proxy_table_processor_utils.h"
#include "proxy/mysql/ob_mysql_proxy_port.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
using namespace oceanbase::common;
using namespace oceanbase::obproxy::net;

class TestProxyTableProcessorUtils : public ::testing::Test
{
};

TEST_F(TestProxyTableProcessorUtils, test_get_proxy_local_addr)
{
  ObProxyConfig &config = get_global_proxy_config();
  config.listen_port = 666;
  config.local_bound_ip.set_value("66.66.66.66");
  ObAddr addr_org(ObAddr::IPV4, "66.66.66.66", static_cast<uint16_t>(config.listen_port));
  ObAddr addr;
  ObProxyTableProcessorUtils::get_proxy_local_addr(addr);
  ASSERT_EQ(addr, addr_org);

  ObAddr my_addr;
  char ip_str[OB_IP_STR_BUFF] = {'\0'};
  int ret = ObProxyTableProcessorUtils::get_one_local_addr(ip_str, OB_IP_STR_BUFF);
  ASSERT_EQ(OB_SUCCESS, ret);
  my_addr.set_ipv4_addr(ip_str, static_cast<uint16_t>(config.listen_port));

  config.local_bound_ip.set_value("127.0.0.1");
  ret = ObProxyTableProcessorUtils::get_proxy_local_addr(addr);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(my_addr, addr);

  config.local_bound_ip.set_value("0.0.0.0");
  ret = ObProxyTableProcessorUtils::get_proxy_local_addr(addr);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(my_addr, addr);
};

TEST_F(TestProxyTableProcessorUtils, test_get_proxy_local_addr_inherited)
{
  int ret = OB_SUCCESS;
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  info.is_inherited_ = true;
  info.fd_ = NO_FD;
  ObAddr addr;
  ret = ObProxyTableProcessorUtils::get_proxy_local_addr(addr);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);



  char ip_str[OB_IP_STR_BUFF] = {'\0'};
  ret = ObProxyTableProcessorUtils::get_one_local_addr(ip_str, OB_IP_STR_BUFF);
  ASSERT_EQ(OB_SUCCESS, ret);

  struct sockaddr_in s_add;
  uint16_t port = 6666;
  int test_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_TRUE(NO_FD != test_fd);
  bzero(&s_add,sizeof(struct sockaddr_in));
  s_add.sin_family = AF_INET;
  s_add.sin_addr.s_addr= (htons)(INADDR_ANY);
  s_add.sin_port = (htons)(port);
  int bind_ret = bind(test_fd, (struct sockaddr *)(&s_add), sizeof(struct sockaddr));
  if (-1 == bind_ret) {
    close(test_fd);
  }
  ASSERT_TRUE(-1 != bind_ret);
  info.fd_ = test_fd;
  ret = ObProxyTableProcessorUtils::get_proxy_local_addr(addr);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObAddr addr2(ObAddr::IPV4, ip_str, port);
  ASSERT_EQ(addr2, addr);
  close(test_fd);



  test_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_TRUE(NO_FD != test_fd);
  bzero(&s_add,sizeof(struct sockaddr_in));
  s_add.sin_family = AF_INET;
  s_add.sin_addr.s_addr= inet_addr(ip_str);
  s_add.sin_port = (htons)(port);
  bind_ret = bind(test_fd, (struct sockaddr *)(&s_add), sizeof(struct sockaddr));
  if (-1 == bind_ret) {
    close(test_fd);
  }
  ASSERT_TRUE(-1 != bind_ret);
  info.fd_ = test_fd;
  ret = ObProxyTableProcessorUtils::get_proxy_local_addr(addr);
  ASSERT_EQ(OB_SUCCESS, ret);
  addr2.reset();
  addr2.set_ipv4_addr(ip_str, port);
  ASSERT_EQ(addr2, addr);
  close(test_fd);
};

TEST_F(TestProxyTableProcessorUtils, test_get_one_local_addr)
{
  int ret = OB_SUCCESS;
  ret = ObProxyTableProcessorUtils::get_one_local_addr(NULL, 2);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  char str[128];
  ret = ObProxyTableProcessorUtils::get_one_local_addr(str, 0);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = ObProxyTableProcessorUtils::get_one_local_addr(str, 128);
  ASSERT_EQ(OB_SUCCESS, ret);
  std::cout<<"local ip:"<<str<<std::endl;
};

TEST_F(TestProxyTableProcessorUtils, test_inc_addr_info_compare)
{
  int ret = OB_SUCCESS;
  bool is_need_all = false;
  ObIpAddr addr_arr1[MAX_LOCAL_ADDR_LIST_COUNT];
  int64_t valid_cnt1 = 0;
  ret = net::get_local_addr_list(addr_arr1, net::MAX_LOCAL_ADDR_LIST_COUNT, is_need_all, valid_cnt1);
  ASSERT_EQ(OB_SUCCESS, ret);
  std::sort(addr_arr1, addr_arr1 + valid_cnt1);

  ObIpAddr addr_arr2[MAX_LOCAL_ADDR_LIST_COUNT];
  int64_t valid_cnt2 = 0;
  ret = net::get_local_addr_list_by_nic(addr_arr2, net::MAX_LOCAL_ADDR_LIST_COUNT, is_need_all, valid_cnt2);
  ASSERT_EQ(OB_SUCCESS, ret);
  std::sort(addr_arr2, addr_arr2 + valid_cnt2);

  ASSERT_EQ(valid_cnt2, valid_cnt1);
  std::cout<<"ip count:"<<valid_cnt2<<std::endl;
  ASSERT_EQ(addr_arr2[0], addr_arr1[0]);

  char str[128];
  addr_arr1[0].get_ip_str(str, 128);
  std::cout<<"local ip:"<<str<<std::endl;
};

}
}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

