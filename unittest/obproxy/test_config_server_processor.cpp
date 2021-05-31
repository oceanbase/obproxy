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

#define private public
#define USING_LOG_PREFIX PROXY
#include <gtest/gtest.h>
#include <curl/curl.h>
#include "lib/oblog/ob_log.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_sql_string.h"
#include "obproxy/proxy/route/ob_table_cache.h"
#include "obproxy/obutils/ob_proxy_config.h"
#include "obproxy/obutils/ob_config_server_processor.h"
#include "obproxy/utils/ob_layout.h"

//valid json config info
#define TEST_CONFIG_INFO \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":200,\
           \"Data\":{\"Version\":\"123456789123456789123456789123\",\"ObRootServiceInfoUrlList\":[{\"ObRegion\":\"ob1\",\
           \"ObRootServiceInfoUrl\":\"xxx\"},{\"ObRegion\":\"ob2\", \"ObRootServiceInfoUrl\":\"yyy\"}],\
           \"ObProxyDatabaseInfo\":{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"ob\",\
           \"User\":\"admin\", \"Password\":\"admin\"},\
           \"ObProxyBinUrl\":\"binurl\"}}"
#define TEST_NEW_CONFIG_INFO \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":200,\
           \"Data\":{\"Version\":\"123456789123456789123456789321\",\"ObRootServiceInfoUrlList\":[{\"ObRegion\":\"ob1\",\
           \"ObRootServiceInfoUrl\":\"aaa\"},{\"ObRegion\":\"ob2\", \"ObRootServiceInfoUrl\":\"bbb\"},{\"ObRegion\":\"ob3\", \"ObRootServiceInfoUrl\":\"ccc\"}],\
           \"ObProxyDatabaseInfo\":{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"ob\",\
           \"User\":\"admin\", \"Password\":\"admin\"},\
           \"ObProxyBinUrl\":\"binurl\"}}"
#define TEST_NEW_CONFIG_TABLE_INFO \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":200,\
           \"Data\":{\"Version\":\"123456789123456789123456789111\",\"ObRootServiceInfoUrlList\":[{\"ObRegion\":\"ob1\",\
           \"ObRootServiceInfoUrl\":\"xxx\"},{\"ObRegion\":\"ob2\", \"ObRootServiceInfoUrl\":\"yyy\"}],\
           \"ObProxyDatabaseInfo\":{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"test\",\
           \"User\":\"admin\", \"Password\":\"admin\"},\
           \"ObProxyBinUrl\":\"binurl\"}}"

//invalid json config info
#define TEST_NO_CONFIG_INFO "{\"Message\":\"successful\",\"Success\":true,\"Code\":200, \"Data\":{}}"
#define TEST_LARGE_CONFIG_INFO \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":200,\
           \"Data\":{\"Version\":\"123456789123456789123456789321\",\"ObRootServiceInfoUrlList\":[{\"ObRegion\":\"ob1\",\
           \"ObRootServiceInfoUrl\":\"xxx\"},{\"ObRegion\":\"ob2\", \"ObRootServiceInfoUrl\":\"yyy\"},{\"ObRegion\":\"ob3\", \"ObRootServiceInfoUrl\":\"yyy\"},\
           {\"ObRegion\":\"ob4\", \"ObRootServiceInfoUrl\":\"yyy\"}, {\"ObRegion\":\"ob5\", \"ObRootServiceInfoUrl\":\"yyy\"}, {\"ObRegion\":\"ob6\", \"ObRootServiceInfoUrl\":\"yyy\"},\
           {\"ObRegion\":\"ob7\", \"ObRootServiceInfoUrl\":\"yyy\"},{\"ObRegion\":\"ob8\", \"ObRootServiceInfoUrl\":\"yyy\"},{\"ObRegion\":\"ob9\", \"ObRootServiceInfoUrl\":\"yyy\"},\
           {\"ObRegion\":\"ob10\", \"ObRootServiceInfoUrl\":\"yyy\"},{\"ObRegion\":\"ob11\", \"ObRootServiceInfoUrl\":\"yyy\"},{\"ObRegion\":\"ob12\", \"ObRootServiceInfoUrl\":\"yyy\"},\
           {\"ObRegion\":\"ob13\", \"ObRootServiceInfoUrl\":\"yyy\"},{\"ObRegion\":\"ob14\", \"ObRootServiceInfoUrl\":\"yyy\"},{\"ObRegion\":\"ob15\", \"ObRootServiceInfoUrl\":\"yyy\"},\
           {\"ObRegion\":\"ob16\", \"ObRootServiceInfoUrl\":\"yyy\"},{\"ObRegion\":\"ob17\", \"ObRootServiceInfoUrl\":\"yyy\"}],\
           \"ObProxyDatabaseInfo\":{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"ob\",\
           \"User\":\"admin\", \"Password\":\"admin\"},\
           \"ObProxyBinUrl\":\"binurl\"}}"
#define TEST_WRONG_HTTP_CODE \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":404,\
           \"Data\":{\"Version\":\"123456789123456789123456789111\",\"ObRootServiceInfoUrlList\":[{\"ObRegion\":\"ob1\",\
           \"ObRootServiceInfoUrl\":\"xxx\"},{\"ObRegion\":\"ob2\", \"ObRootServiceInfoUrl\":\"yyy\"}],\
           \"ObProxyDatabaseInfo\":{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"ob\",\
           \"User\":\"admin\", \"Password\":\"admin\"},\
           \"ObProxyBinUrl\":\"binurl\"}}"
#define TEST_WRONG_HTTP_TYPE \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":\"200\",\
           \"Data\":{\"Version\":\"123456789123456789123456789111\",\"ObRootServiceInfoUrlList\":[{\"ObRegion\":\"ob1\",\
           \"ObRootServiceInfoUrl\":\"xxx\"},{\"ObRegion\":\"ob2\", \"ObRootServiceInfoUrl\":\"yyy\"}],\
           \"ObProxyDatabaseInfo\":{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"ob\",\
           \"User\":\"admin\", \"Password\":\"admin\"},\
           \"ObProxyBinUrl\":\"binurl\"}}"
#define TEST_WRONG_NAME \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":200,\
           \"Data\":{\"Version\":\"123456789123456789123456789111\",\"ObRootServiceInfoUrlList\":[{\"ObXXX\":\"ob1\",\
           \"ObRootServiceInfoUrl\":\"xxx\"},{\"ObRegion\":\"ob2\", \"ObRootServiceInfoUrl\":\"yyy\"}],\
           \"ObProxyDatabaseInfo\":{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"ob\",\
           \"User\":\"admin\", \"Password\":\"admin\"},\
           \"ObProxyBinUrl\":\"binurl\"}}"
#define TEST_WRONG_METADB \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":200,\
           \"Data\":{\"Version\":\"123456789123456789123456789111\",\"ObRootServiceInfoUrlList\":[{\"ObRegion\":\"ob1\",\
           \"ObRootServiceInfoUrl\":\"xxx\"},{\"ObRegion\":\"ob2\", \"ObRootServiceInfoUrl\":\"yyy\"}],\
           \"ObProxyDatabaseInfo\":{\"aabb\":\"1.1.1.1\", \"DataBase\":\"ob\",\
           \"User\":\"admin\", \"Password\":\"admin\"},\
           \"ObProxyBinUrl\":\"binurl\"}}"
#define TEST_NO_USERNAME \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":200,\
           \"Data\":{\"Version\":\"123456789123456789123456789111\",\"ObRootServiceInfoUrlList\":[{\"ObRegion\":\"ob1\",\
           \"ObRootServiceInfoUrl\":\"xxx\"},{\"ObRegion\":\"ob2\", \"ObRootServiceInfoUrl\":\"yyy\"}],\
           \"ObProxyDatabaseInfo\":{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"ob\",\
           \"User\":\"\", \"Password\":\"admin\"},\
           \"ObProxyBinUrl\":\"binurl\"}}"
#define TEST_NO_DATABASE \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":200,\
           \"Data\":{\"Version\":\"123456789123456789123456789111\",\"ObRootServiceInfoUrlList\":[{\"ObRegion\":\"ob1\",\
           \"ObRootServiceInfoUrl\":\"xxx\"},{\"ObRegion\":\"ob2\", \"ObRootServiceInfoUrl\":\"yyy\"}],\
           \"ObProxyDatabaseInfo\":{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"\",\
           \"User\":\"admin\", \"Password\":\"admin\"},\
           \"ObProxyBinUrl\":\"binurl\"}}"
#define TEST_CONFIG_NO_PASSWORD \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":200,\
           \"Data\":{\"Version\":\"123456789123456789123456789111\",\"ObRootServiceInfoUrlList\":[{\"ObRegion\":\"ob1\",\
           \"ObRootServiceInfoUrl\":\"xxx\"},{\"ObRegion\":\"ob2\", \"ObRootServiceInfoUrl\":\"yyy\"}],\
           \"ObProxyDatabaseInfo\":{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"ob\",\
           \"User\":\"admin\", \"Password\":\"\"},\
           \"ObProxyBinUrl\":\"binurl\"}}"
#define TEST_NO_RS_URL_LIST \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":200,\
           \"Data\":{\"Version\":\"123456789123456789123456789111\",\"ObRootServiceInfoUrlList\":[],\
           \"ObProxyDatabaseInfo\":{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"ob\",\
           \"User\":\"admin\", \"Password\":\"admin\"},\
           \"ObProxyBinUrl\":\"binurl\"}}"
#define TEST_NO_BIN_URL \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":200,\
           \"Data\":{\"Version\":\"123456789123456789123456789111\",\"ObRootServiceInfoUrlList\":[{\"ObRegion\":\"ob1\",\
           \"ObRootServiceInfoUrl\":\"xxx\"},{\"ObRegion\":\"ob2\", \"ObRootServiceInfoUrl\":\"yyy\"}],\
           \"ObProxyDatabaseInfo\":{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"ob\",\
           \"User\":\"admin\", \"Password\":\"admin\"},\
           \"ObProxyBinUrl\":\"\"}}"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace obproxy
{
using namespace obutils;
using namespace proxy;
using namespace event;
class ObFakeConfigServerProcessor : public ObConfigServerProcessor
{
public:
  ObFakeConfigServerProcessor() {
    memset(test_buf_, 0, OB_PROXY_CONFIG_BUFFER_SIZE + 10);
  }
public:
  int do_fetch_json_info(const char *url, ObString &json);
  char test_buf_[OB_PROXY_CONFIG_BUFFER_SIZE + 10];
};

int ObFakeConfigServerProcessor::do_fetch_json_info(const char *url, ObString &json)
{
  UNUSED(url);

  int ret = OB_SUCCESS;
  if (json.size() <= static_cast<int32_t>(strlen(test_buf_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf is not enough", K(ret));
  } else if (json.write(test_buf_, static_cast<int32_t>(strlen(test_buf_))) <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to append string", K(ret));
  }
  return ret;
}

class TestConfigServerProcessor : public ::testing::Test
{
public :
  virtual void SetUp();
  virtual void TearDown() {}
  ObConfigServerProcessor config_processor_;
};

void TestConfigServerProcessor::SetUp()
{
  config_processor_.is_inited_ = true;
  config_processor_.kernel_release_ = RELEASE_6U;
  config_processor_.json_config_info_ = op_alloc(ObProxyJsonConfigInfo);
  get_global_table_cache().init(ObTableCache::TABLE_CACHE_MAP_SIZE);
  get_global_layout().init("./");
}

TEST_F(TestConfigServerProcessor, test_set_default_web_rs_list)
{
  ObProxyClusterInfo *cluster_info = NULL;
  ObProxySubClusterInfo *sub_cluster_info = NULL;
  config_processor_.proxy_config_.rootservice_list.set_value("1.1.1.1:111;2.2.2.2:222");
  ObString cluster_name1("ob1");
  int ret = config_processor_.set_default_rs_list(cluster_name1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = config_processor_.json_config_info_->get_cluster_info(cluster_name1, cluster_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = config_processor_.json_config_info_->get_sub_cluster_info(cluster_name1, 0, sub_cluster_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, sub_cluster_info->web_rs_list_.count());
  ObAddr addr1, addr2;
  addr1.set_ipv4_addr("1.1.1.1", 111);
  addr2.set_ipv4_addr("2.2.2.2", 222);
  ASSERT_EQ(addr1, sub_cluster_info->web_rs_list_[0].server_);
  ASSERT_EQ(addr2, sub_cluster_info->web_rs_list_[1].server_);

  config_processor_.json_config_info_->data_info_.cluster_array_.destroy();
  config_processor_.proxy_config_.rootservice_list.set_value("...:xxx;2.2.2.2:222");
  ObString cluster_name2("ob2");
  ret = config_processor_.set_default_rs_list(cluster_name2);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  config_processor_.json_config_info_->data_info_.cluster_array_.destroy();
  ObString cluster_name3("obxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
  config_processor_.proxy_config_.rootservice_list.set_value("1.1.1.1:111;2.2.2.2:222");
  ret = config_processor_.set_default_rs_list(cluster_name3);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
}

TEST_F(TestConfigServerProcessor, test_get_json_config_info)
{
  int ret = OB_SUCCESS;
  config_processor_.proxy_config_.app_name.set_value("ob1.kyle.sj");
  config_processor_.proxy_config_.obproxy_config_server_url.set_value(
      "http://11.166.86.153:8080/diamond/cgi/a.py?key=unittest_test_config_server.proxy&method=get");
  ret = config_processor_.refresh_json_config_info();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, memcmp(config_processor_.json_config_info_->data_info_.bin_url_.ptr(),
      "http://11.166.86.153:8877", config_processor_.json_config_info_->data_info_.bin_url_.length()));
  ASSERT_EQ(0, memcmp(config_processor_.json_config_info_->data_info_.meta_table_info_.db_.ptr(),
      "oceanbase", config_processor_.json_config_info_->data_info_.meta_table_info_.db_.length()));
  ASSERT_EQ(0, memcmp(config_processor_.json_config_info_->data_info_.meta_table_info_.username_.ptr(),
      "admin", config_processor_.json_config_info_->data_info_.meta_table_info_.username_.length()));
  ASSERT_EQ(0, memcmp(config_processor_.json_config_info_->data_info_.meta_table_info_.password_.ptr(),
      "admin", config_processor_.json_config_info_->data_info_.meta_table_info_.password_.length()));
  ASSERT_EQ(1, config_processor_.json_config_info_->data_info_.cluster_array_.count());

  config_processor_.proxy_config_.obproxy_config_server_url.set_value(
      "http://11.166.86.153:8899/xxx");
  ret = config_processor_.refresh_json_config_info();
  ASSERT_EQ(OB_CURL_ERROR, ret);
}

TEST_F(TestConfigServerProcessor, test_do_fetch_json_config)
{
  int ret = OB_SUCCESS;
  char buf[OB_PROXY_CONFIG_BUFFER_SIZE];
  buf[0] = '\0';
  ObString content;
  content.assign_buffer(buf, OB_PROXY_CONFIG_BUFFER_SIZE);

  config_processor_.proxy_config_.obproxy_config_server_url.set_value(
      "http://11.166.86.153:8080/diamond/cgi/a.py?key=unittest_test_config_server.proxy&method=get");
  const char *config_url = config_processor_.proxy_config_.obproxy_config_server_url;
  ret = config_processor_.fetch_by_curl(config_url, ObConfigServerProcessor::CURL_TRANSFER_TIMEOUT,
      static_cast<void *>(&content), ObConfigServerProcessor::write_data);
  ASSERT_EQ(OB_SUCCESS, ret);

  config_processor_.proxy_config_.obproxy_config_server_url.set_value(
      "http://11.166.86.153/xxx");
  const char *wrong_config_url = config_processor_.proxy_config_.obproxy_config_server_url;
  ret = config_processor_.fetch_by_curl(wrong_config_url, ObConfigServerProcessor::CURL_TRANSFER_TIMEOUT,
      static_cast<void *>(&content), ObConfigServerProcessor::write_data);
  ASSERT_EQ(OB_CURL_ERROR, ret);

  config_processor_.proxy_config_.obproxy_config_server_url.set_value(
      "http://11.166.86.153:8888/diamond/cgi/a.py?key=ob1.kyle.sj.test&method=get");
  const char *wrong_http_port = config_processor_.proxy_config_.obproxy_config_server_url;
  ret = config_processor_.fetch_by_curl(wrong_http_port, ObConfigServerProcessor::CURL_TRANSFER_TIMEOUT,
          static_cast<void *>(&content), ObConfigServerProcessor::write_data);
  ASSERT_EQ(OB_CURL_ERROR, ret);
}

TEST_F(TestConfigServerProcessor, test_get_newest_cluster_rs_list)
{
  int ret = OB_SUCCESS;
  config_processor_.proxy_config_.app_name.set_value("ob1.kyle.sj");
  config_processor_.proxy_config_.obproxy_config_server_url.set_value(
      "http://11.166.86.153:8080/diamond/cgi/a.py?key=unittest_test_config_server.proxy&method=get");
  ret = config_processor_.refresh_json_config_info();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObAddr, 16> rs_list;
  ObString cluster_name = ObString::make_string("xxx");
  ret = config_processor_.get_newest_cluster_rs_list(cluster_name, 0, rs_list);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

TEST_F(TestConfigServerProcessor, test_do_fetch_proxy_bin)
{
  int ret = OB_SUCCESS;
  const char *save_path = "obproxy_new";
  config_processor_.json_config_info_->data_info_.bin_url_.url_ = ObString::make_string("http://10.125.224.4:9191/method=get");
  ret = config_processor_.do_fetch_proxy_bin(save_path, "obproxy.el6.x86_64.rpm");
  ASSERT_EQ(OB_SUCCESS, ret);
  remove(save_path);

  //test error new_proxy_bin_version
  ret = config_processor_.do_fetch_proxy_bin(save_path, "obproxy.el5.x86_64.rpm");
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  //test unknown new_proxy_bin_version
  ret = config_processor_.do_fetch_proxy_bin(save_path, "obproxy.x86_64.rpm");
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  //test short timeout
  const char *bin_url = "http://10.125.224.4:9191/method=get&Version=big.obproxy.el6.x86_64.rpm";

  int fd = 0;
  if ((fd = ::open(save_path, O_WRONLY | O_CREAT,
                S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH)) > 0) {
    ret = config_processor_.fetch_by_curl(bin_url, 1L,
        reinterpret_cast<void *>(fd), config_processor_.write_proxy_bin);

  }
  if (fd > 0) close(fd);
  ASSERT_EQ(OB_CURL_ERROR, ret);
  remove(save_path);

  //test wrong url
  const char *wrong_bin_url = "http://10.125.224.4:9191/method=get&Version=/test_dir/obproxy.el6.x86_64.rpm";
  int64_t fetch_timeout = config_processor_.proxy_config_.fetch_proxy_bin_timeout / 1000000;//us --> s
  if (fetch_timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("error fetch timeout", K(fetch_timeout), K(ret));
  } else if ((fd = ::open(save_path, O_WRONLY | O_CREAT | O_TRUNC,
                       S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH)) > 0) {
    ret = config_processor_.fetch_by_curl(wrong_bin_url, fetch_timeout, reinterpret_cast<void *>((int64_t)fd), config_processor_.write_proxy_bin);
  }
  if (fd > 0) close(fd);
  ASSERT_EQ(OB_CURL_ERROR, ret);
  remove(save_path);

}

TEST_F(TestConfigServerProcessor, test_get_idc_url)
{
  const char *buf1 = "http://11.166.86.153:8080/oceanbase_obconfig/obtest_jianhua.sjh_10.125.224.4_ob1";
  const char *buf2 = "http://ocp-api.alipay.com/services?Action=ObRootServiceInfo&User_ID=alibaba&UID=zhitao.rzt&ObRegion=rhz_obtrans60";
  const char *buf3 = "http://ocp-api.alipay.com/services?action=obrootserviceinfo&user_id=alibaba&uid=zhitao.rzt&obregion=rhz_obtrans60";
  const char *buf4 = "HTTP://OCP-API.ALIPAY.COM/SERVICES?ACTION=OBROOTSERVICEINFO&USER_ID=ALIBABA&UID=ZHITAO.RZT&OBREGION=RHZ_OBTRANS60";
  const char *expect_buf1 = "http://11.166.86.153:8080/oceanbase_obconfig/obtest_jianhua.sjh_10.125.224.4_ob1_idc_list";
  const char *expect_buf2 = "http://ocp-api.alipay.com/services?Action=ObIDCRegionInfo&User_ID=alibaba&UID=zhitao.rzt&ObRegion=rhz_obtrans60";
  const char *expect_buf3 = "http://ocp-api.alipay.com/services?action=ObIDCRegionInfo&user_id=alibaba&uid=zhitao.rzt&obregion=rhz_obtrans60";
  const char *expect_buf4 = "HTTP://OCP-API.ALIPAY.COM/SERVICES?ACTION=ObIDCRegionInfo&USER_ID=ALIBABA&UID=ZHITAO.RZT&OBREGION=RHZ_OBTRANS60";
  const int64_t max_size = 256;
  char common_buf[256];
  char *but_ptr = common_buf;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObConfigServerProcessor::get_idc_url(buf1, static_cast<int64_t>(strlen(buf1)),
      but_ptr, static_cast<int64_t>(strlen(buf1))));
  EXPECT_EQ(OB_SUCCESS, ObConfigServerProcessor::get_idc_url(buf1, static_cast<int64_t>(strlen(buf1)),
      but_ptr, max_size));
  EXPECT_EQ(0, memcmp(expect_buf1, common_buf, strlen(expect_buf1)));

  EXPECT_EQ(OB_INVALID_ARGUMENT, ObConfigServerProcessor::get_idc_url(buf2, static_cast<int64_t>(strlen(buf2)),
      but_ptr, static_cast<int64_t>(strlen(buf2))));
  EXPECT_EQ(OB_SUCCESS, ObConfigServerProcessor::get_idc_url(buf2, static_cast<int64_t>(strlen(buf2)),
      but_ptr, max_size));
  EXPECT_EQ(0, memcmp(expect_buf2, common_buf, strlen(expect_buf2)));

  EXPECT_EQ(OB_INVALID_ARGUMENT, ObConfigServerProcessor::get_idc_url(buf3, static_cast<int64_t>(strlen(buf3)),
      but_ptr, static_cast<int64_t>(strlen(buf3))));
  EXPECT_EQ(OB_SUCCESS, ObConfigServerProcessor::get_idc_url(buf3, static_cast<int64_t>(strlen(buf3)),
      but_ptr, max_size));
  EXPECT_EQ(0, memcmp(expect_buf3, common_buf, strlen(expect_buf3)));

  EXPECT_EQ(OB_INVALID_ARGUMENT, ObConfigServerProcessor::get_idc_url(buf4, static_cast<int64_t>(strlen(buf4)),
      but_ptr, static_cast<int64_t>(strlen(buf4))));
  EXPECT_EQ(OB_SUCCESS, ObConfigServerProcessor::get_idc_url(buf4, static_cast<int64_t>(strlen(buf4)),
      but_ptr, max_size));
  EXPECT_EQ(0, memcmp(expect_buf4, common_buf, strlen(expect_buf4)));
}

TEST_F(TestConfigServerProcessor, test_get_idc_list)
{
  int ret = OB_SUCCESS;
  config_processor_.proxy_config_.app_name.set_value("ob1.kyle.sj");
  config_processor_.proxy_config_.obproxy_config_server_url.set_value(
      "http://11.166.86.153:8080/diamond/cgi/a.py?key=unittest_test_config_server.proxy&method=get");
  ret = config_processor_.refresh_json_config_info();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObProxyIDCInfo, 16> idcs_list;
  ObString cluster_name = ObString::make_string("xxx");
  ret = config_processor_.refresh_idc_list(cluster_name, 0, idcs_list);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}


class TestFakeConfigServerProcessor : public ::testing::Test
{
public :
  virtual void SetUp();
  virtual void TearDown() {}

  ObFakeConfigServerProcessor config_processor_;
};

void TestFakeConfigServerProcessor::SetUp()
{
  config_processor_.is_inited_ = true;
  config_processor_.kernel_release_ = RELEASE_6U;
  config_processor_.json_config_info_ = op_alloc(ObProxyJsonConfigInfo);
  get_global_layout().init("./");
}

TEST_F(TestFakeConfigServerProcessor, test_get_json_config_info)
{
  int ret = OB_SUCCESS;
  memset(config_processor_.test_buf_, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(config_processor_.test_buf_, TEST_CONFIG_INFO, sizeof(TEST_CONFIG_INFO));
  ret = config_processor_.refresh_json_config_info();
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(0, memcmp(config_processor_.json_config_info_->data_info_.bin_url_.ptr(),
      "binurl", config_processor_.json_config_info_->data_info_.bin_url_.length()));
  ASSERT_EQ(0, memcmp(config_processor_.json_config_info_->data_info_.meta_table_info_.db_.ptr(),
      "ob", config_processor_.json_config_info_->data_info_.meta_table_info_.db_.length()));
  ASSERT_EQ(0, memcmp(config_processor_.json_config_info_->data_info_.meta_table_info_.username_.ptr(),
      "admin", config_processor_.json_config_info_->data_info_.meta_table_info_.username_.length()));
  ASSERT_EQ(0, memcmp(config_processor_.json_config_info_->data_info_.meta_table_info_.password_.ptr(),
      "admin", config_processor_.json_config_info_->data_info_.meta_table_info_.password_.length()));

  memset(config_processor_.test_buf_, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(config_processor_.test_buf_, TEST_LARGE_CONFIG_INFO, sizeof(TEST_LARGE_CONFIG_INFO));
  ret = config_processor_.refresh_json_config_info();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestFakeConfigServerProcessor, test_update_json_config_info)
{
  int ret = OB_SUCCESS;
  memset(config_processor_.test_buf_, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(config_processor_.test_buf_, TEST_CONFIG_INFO, sizeof(TEST_CONFIG_INFO));
  ret = config_processor_.refresh_json_config_info();
  ASSERT_EQ(OB_SUCCESS, ret);

  memset(config_processor_.test_buf_, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(config_processor_.test_buf_, TEST_NEW_CONFIG_INFO, sizeof(TEST_NEW_CONFIG_INFO));
  ret = config_processor_.refresh_json_config_info();
  ASSERT_EQ(OB_SUCCESS, ret);

  //test the same version
  memset(config_processor_.test_buf_, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(config_processor_.test_buf_, TEST_NEW_CONFIG_INFO, sizeof(TEST_NEW_CONFIG_INFO));
  ret = config_processor_.refresh_json_config_info();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestFakeConfigServerProcessor, test_get_first_cluster_name)
{
  int ret = OB_SUCCESS;
  char cluster_name[OB_PROXY_MAX_CLUSTER_NAME_LENGTH + 1];
  memset(config_processor_.test_buf_, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(config_processor_.test_buf_, TEST_CONFIG_INFO, sizeof(TEST_CONFIG_INFO));
  ret = config_processor_.refresh_json_config_info();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = config_processor_.get_default_cluster_name(cluster_name, OB_PROXY_MAX_CLUSTER_NAME_LENGTH + 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, memcmp("ob1", cluster_name, strlen("ob1")));

  ret = config_processor_.get_default_cluster_name(cluster_name, 3);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
}

TEST_F(TestFakeConfigServerProcessor, test_is_cluster_name_exists)
{
  int ret = OB_SUCCESS;
  memset(config_processor_.test_buf_, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(config_processor_.test_buf_, TEST_CONFIG_INFO, sizeof(TEST_CONFIG_INFO));
  ret = config_processor_.refresh_json_config_info();
  ASSERT_EQ(OB_SUCCESS, ret);

  bool bool_ret = true;
  ObString cluster_name = ObString::make_string("ob1");
  bool_ret = config_processor_.is_cluster_name_exists(cluster_name);
  ASSERT_EQ(true, bool_ret);

  cluster_name.reset();
  cluster_name = ObString::make_string("xxx");
  bool_ret = config_processor_.is_cluster_name_exists(cluster_name);
  ASSERT_EQ(bool_ret, false);
}

TEST_F(TestFakeConfigServerProcessor, test_parse_json_info)
{
  int ret = OB_SUCCESS;
  json::Value* root = NULL;
  ObProxyJsonConfigInfo json_info;
  char buf[OB_PROXY_CONFIG_BUFFER_SIZE];
  ObString json;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);

  memset(buf, 'a', OB_PROXY_CONFIG_BUFFER_SIZE - 10);
  json.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
  ret = config_processor_.init_json(json, root, allocator);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ret);
  root = NULL;

  memset(buf, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(buf, TEST_WRONG_HTTP_CODE, sizeof(TEST_WRONG_HTTP_CODE));
  json_info.reset();
  json_info.destroy_cluster_info();
  json.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
  ret = config_processor_.init_json(json, root, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info.parse(root);
  ASSERT_EQ(OB_INVALID_CONFIG, ret);
  root = NULL;

  memset(buf, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(buf, TEST_WRONG_HTTP_TYPE, sizeof(TEST_WRONG_HTTP_TYPE));
  json_info.reset();
  json_info.destroy_cluster_info();
  json.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
  ret = config_processor_.init_json(json, root, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info.parse(root);
  ASSERT_EQ(OB_INVALID_CONFIG, ret);

  memset(buf, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(buf, TEST_NO_CONFIG_INFO, sizeof(TEST_NO_CONFIG_INFO));
  json_info.reset();
  json_info.destroy_cluster_info();
  json.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
  ret = config_processor_.init_json(json, root, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info.parse(root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(json_info.data_info_.meta_table_info_.is_valid(), false);
  ASSERT_EQ(json_info.data_info_.bin_url_.is_valid(), false);
  ASSERT_EQ(json_info.data_info_.cluster_array_.is_valid(), false);
  ASSERT_EQ(json_info.data_info_.is_valid(), false);
  ASSERT_EQ(json_info.is_valid(), false);

  memset(buf, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(buf, TEST_NO_RS_URL_LIST, sizeof(TEST_NO_RS_URL_LIST));
  json_info.reset();
  json_info.destroy_cluster_info();
  json.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
  ret = config_processor_.init_json(json, root, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info.parse(root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(json_info.data_info_.cluster_array_.is_valid(), false);
  ASSERT_EQ(json_info.data_info_.is_valid(), false);
  ASSERT_EQ(json_info.is_valid(), false);

  memset(buf, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(buf, TEST_NO_DATABASE, sizeof(TEST_NO_DATABASE));
  json_info.reset();
  json_info.destroy_cluster_info();
  json.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
  ret = config_processor_.init_json(json, root, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info.parse(root);
  ASSERT_EQ(OB_INVALID_CONFIG, ret);

  memset(buf, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(buf, TEST_NO_USERNAME, sizeof(TEST_NO_USERNAME));
  json_info.reset();
  json_info.destroy_cluster_info();
  json.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
  ret = config_processor_.init_json(json, root, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info.parse(root);
  ASSERT_EQ(OB_INVALID_CONFIG, ret);

  memset(buf, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(buf, TEST_CONFIG_NO_PASSWORD, sizeof(TEST_CONFIG_NO_PASSWORD));
  json_info.reset();
  json_info.destroy_cluster_info();
  json.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
  ret = config_processor_.init_json(json, root, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info.parse(root);
  ASSERT_EQ(OB_INVALID_CONFIG, ret);

  memset(buf, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(buf, TEST_WRONG_NAME, sizeof(TEST_WRONG_NAME));
  json_info.reset();
  json_info.destroy_cluster_info();
  json.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
  ret = config_processor_.init_json(json, root, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info.parse(root);
  ASSERT_EQ(OB_SUCCESS, ret);

  memset(buf, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(buf, TEST_WRONG_METADB, sizeof(TEST_WRONG_METADB));
  json_info.reset();
  json_info.destroy_cluster_info();
  json.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
  ret = config_processor_.init_json(json, root, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info.parse(root);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestFakeConfigServerProcessor, test_get_cluster_rs_list)
{
  ObProxyClusterInfo *cluster_info = NULL;
  int ret = OB_SUCCESS;
  config_processor_.proxy_config_.rootservice_list.set_value("1.1.1.1:111;2.2.2.2:222");
  ObString cluster_name1("ob");
  config_processor_.is_inited_ = true;
  ret = config_processor_.set_default_rs_list(cluster_name1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = config_processor_.json_config_info_->get_cluster_info(cluster_name1, cluster_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, cluster_info->get_rs_list_count());

  ObSEArray<ObAddr, 16> rs_list;
  ret = config_processor_.get_cluster_rs_list(cluster_name1, 0, rs_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(rs_list.count(), 2);
  ObAddr rs_1;
  rs_1.set_ipv4_addr("1.1.1.1", 111);
  ObAddr rs_2;
  rs_2.set_ipv4_addr("2.2.2.2", 222);
  ASSERT_EQ(rs_list[0], rs_1);
  ASSERT_EQ(rs_list[1], rs_2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObString cluster_name2("mysql");
  ret = config_processor_.get_cluster_rs_list(cluster_name2, 0, rs_list);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

TEST_F(TestFakeConfigServerProcessor, test_get_proxy_table_info_from_json)
{
  int ret = OB_SUCCESS;
  memset(config_processor_.test_buf_, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(config_processor_.test_buf_, TEST_CONFIG_INFO, sizeof(TEST_CONFIG_INFO));
  ret = config_processor_.refresh_json_config_info();
  ASSERT_EQ(OB_SUCCESS, ret);
  ObProxyMetaTableInfo table_info;
  config_processor_.get_proxy_meta_table_info(table_info);
  ASSERT_EQ(true, table_info.is_valid());
  ASSERT_EQ(0, memcmp(table_info.db_.ptr(), "ob", table_info.db_.length()));
  ASSERT_EQ(0, memcmp(table_info.username_.ptr(), "admin", table_info.username_.length()));
  ASSERT_EQ(0, memcmp(table_info.password_.ptr(), "admin", table_info.password_.length()));
}

TEST_F(TestFakeConfigServerProcessor, test_build_proxy_bin_url)
{
  int ret = OB_SUCCESS;
  memset(config_processor_.test_buf_, 0, OB_PROXY_CONFIG_BUFFER_SIZE);
  memcpy(config_processor_.test_buf_, TEST_CONFIG_INFO, sizeof(TEST_CONFIG_INFO));
  ret = config_processor_.refresh_json_config_info();
  ASSERT_EQ(OB_SUCCESS ,ret);

  char *buf = NULL;
  ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
  ret = config_processor_.build_proxy_bin_url("obproxy-1.0.3-324751.el6.x86_64", allocator, buf);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, memcmp(buf, "binurl&Version=obproxy-1.0.3-324751.el6.x86_64", strlen(buf)));

  config_processor_.kernel_release_ = RELEASE_5U;
  buf = NULL;
  ret = config_processor_.build_proxy_bin_url("obproxy-1.0.3-324751.el5.x86_64", allocator, buf);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, memcmp(buf, "binurl&Version=obproxy-1.0.3-324751.el5.x86_64", strlen(buf)));

  config_processor_.kernel_release_ = RELEASE_7U;
  buf = NULL;
  ret = config_processor_.build_proxy_bin_url("obproxy-1.0.3-324751.el7.x86_64", allocator, buf);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, memcmp(buf, "binurl&Version=obproxy-1.0.3-324751.el7.x86_64", strlen(buf)));
}


}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
