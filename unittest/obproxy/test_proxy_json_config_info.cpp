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
#include <gtest/gtest.h>
#define private public
#include "obproxy/proxy/route/ob_table_cache.h"
#include "obproxy/obutils/ob_proxy_json_config_info.h"

#define TEST_CLUSTER_INFO \
         "{\"ObRegion\":\"ob1\",\"ObRootServiceInfoUrl\":\"xxx\"}"
#define TEST_EX_CLUSTER_INFO \
         "{\"ObRegion\":\"ob2\",\"ObRootServiceInfoUrl\":\"yyy\", \"extra\":\"extra info\"}"
#define TEST_CLUSTER_LIST \
         "["TEST_CLUSTER_INFO","TEST_EX_CLUSTER_INFO"]"
#define TEST_META_TABLE_INFO \
         "{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"test\",\
           \"User\":\"admin\", \"Password\":\"admin\"}"
#define TEST_META_TABLE_INFO_WRONG_USERNAME \
         "{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"test\",\
           \"User\":\"admin@sys#xxx\", \"Password\":\"admin\"}"
#define TEST_META_TABLE_INFO_USERNAME \
         "{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"test\",\
           \"User\":\"admin@sys#MetaDataBase\", \"Password\":\"admin\"}"
#define TEST_EX_META_TABLE_INFO \
         "{\"MetaDataBase\":\"1.1.1.1\", \"DataBase\":\"test\",\
           \"User\":\"admin\", \"Password\":\"admin\",\
           \"extra\":\"extra info\"}"
#define TEST_DATA_INFO \
         "{\"Version\":\"123456789123456789123456789111\",\
           \"ObRootServiceInfoUrlList\":"TEST_CLUSTER_LIST",\
           \"ObProxyDatabaseInfo\":"TEST_META_TABLE_INFO",\
           \"ObProxyBinUrl\":\"binurl\"}"
#define TEST_EX_DATA_INFO \
         "{\"Version\":\"123456789123456789123456789111\",\
           \"ObRootServiceInfoUrlList\":"TEST_CLUSTER_LIST",\
           \"ObProxyDatabaseInfo\":"TEST_META_TABLE_INFO",\
           \"ObProxyBinUrl\":\"binurl\",\
           \"extra\":\"extra info\"}"
#define TEST_CONFIG_INFO \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":200,\
           \"Data\":"TEST_DATA_INFO"}"
#define TEST_EX_CONFIG_INFO \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":200,\
           \"Data\":"TEST_DATA_INFO",\
           \"extra\":\"extra info\"}"
#define TEST_RSLIST \
         "{\"ObRegion\" : \"ob1\",\
           \"RsList\":[{\"address\":\"1.1.1.1:111\",\"role\":\"LEADER\",\"sql_port\":111}]}"
#define TEST_MASTER_RSLIST \
         "{\"ObRegion\" : \"ob1\",\
           \"ObRegionId\":123,\
           \"type\":\"leader\",\
           \"RsList\":[{\"address\":\"1.1.1.1:111\",\"role\":\"LEADER\",\"sql_port\":111}]}"
#define TEST_FOLLOWER_RSLIST \
         "{\"ObRegion\" : \"ob1\",\
           \"ObRegionId\":456,\
           \"type\":\"follower\",\
           \"RsList\":[{\"address\":\"1.1.1.1:111\",\"role\":\"LEADER\",\"sql_port\":111}]}"
#define TEST_RSLIST_NO_TYPE \
         "{\"ObRegion\" : \"ob1\",\
           \"ObRegionId\":123,\
           \"RsList\":[{\"address\":\"1.1.1.1:111\",\"role\":\"LEADER\",\"sql_port\":111}]}"
#define TEST_RSLIST_EMPTY_READONLY \
         "{\"ObRegion\" : \"ob1\",\
           \"RsList\":[{\"address\":\"1.1.1.2:111\",\"role\":\"LEADER\",\"sql_port\":111}],\
           \"ReadonlyRsList\":[]}"
#define TEST_EMPTY_RSLIST_AND_READONLY \
         "{\"ObRegion\" : \"ob1\",\
           \"RsList\":[],\
           \"ReadonlyRsList\":[{\"address\":\"1.1.1.3:111\",\"role\":\"LEADER\",\"sql_port\":111}]}"
#define TEST_RSLIST_AND_READONLY \
         "{\"ObRegion\" : \"ob1\",\
           \"RsList\":[{\"address\":\"1.1.1.4:111\",\"role\":\"LEADER\",\"sql_port\":111}],\
           \"ReadonlyRsList\":[{\"address\":\"1.1.1.5:111\",\"role\":\"LEADER\",\"sql_port\":111}]}"
#define TEST_EMPTY_RSLIST_EMPTY_READONLY \
         "{\"ObRegion\" : \"ob1\",\
           \"RsList\":[]}"
#define TEST_INVALID_RSLIST \
         "{\"ObRegion\" : \"ob1\",\
           \"RsList\":[{\"address\":\"1.1.1.1:111\",\"role\":\"LEADER\",\"sql_port\":-1},\
           {\"address\":\"1.1.1.1:111\",\"role\":\"LEADER\",\"sql_port\":0}]}"
#define TEST_EX_RSLIST \
         "{\"ObRegion\" : \"ob2\",\
           \"RsList\":[{\"address\":\"2.2.2.2:222\",\"role\":\"LEADER\",\"sql_port\":222}],\
           \"extra\":\"extra info\"}"
#define TEST_REMOTE_RSLIST \
         "{\"Message\":\"successful\", \"Success\":true, \"Code\":200,\
           \"Data\":"TEST_RSLIST"}"
#define TEST_LOCAL_RSLIST \
         "["TEST_RSLIST","TEST_EX_RSLIST"]"
#define TEST_RSLIST_ARRAY \
         "["TEST_MASTER_RSLIST","TEST_FOLLOWER_RSLIST"]"

using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;
namespace oceanbase
{
namespace obproxy
{

class TestProxyJsonConfigInfo : public ::testing::Test
{
public :
  virtual void SetUp();
  virtual void TearDown() {}

  int init_json(const char *buf, ObArenaAllocator &allocator, Value *&root);

public:
  ObProxyJsonConfigInfo json_info_;
  char test_buf[OB_PROXY_CONFIG_BUFFER_SIZE];
};

void TestProxyJsonConfigInfo::SetUp()
{
  test_buf[0] = '\0';
  get_global_table_cache().init(ObTableCache::TABLE_CACHE_MAP_SIZE);
}

int TestProxyJsonConfigInfo::init_json(const char *buf, ObArenaAllocator &allocator, Value *&root)
{
  int ret = OB_SUCCESS;
  Parser parser;
  root = NULL;
  allocator.reuse();
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(parser.init(&allocator))) {
    LOG_WARN("fail to init json parser", K(ret));
  } else if (OB_FAIL(parser.parse(buf, STRLEN(buf), root))) {
    LOG_WARN("fail to parse json buf", K(ret));
  }
  return ret;
}

TEST_F(TestProxyJsonConfigInfo, test_parse_header)
{
  int ret = OB_SUCCESS;
  Value *root = NULL;
  Value *value = NULL;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);

  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_CONFIG_INFO, sizeof(TEST_CONFIG_INFO));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObProxyJsonUtils::parse_header(root, value);
  ASSERT_EQ(OB_SUCCESS, ret);

  root = NULL;
  value = NULL;
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_EX_CONFIG_INFO, sizeof(TEST_EX_CONFIG_INFO));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObProxyJsonUtils::parse_header(root, value);
  ASSERT_EQ(OB_SUCCESS, ret);

  root = NULL;
  value = NULL;
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_REMOTE_RSLIST, sizeof(TEST_REMOTE_RSLIST));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObProxyJsonUtils::parse_header(root, value);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestProxyJsonConfigInfo, test_parse_data)
{
  int ret = OB_SUCCESS;
  Value *root = NULL;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);

  root = NULL;
  json_info_.reset();
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_DATA_INFO, sizeof(TEST_DATA_INFO));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.data_info_.parse(root, json_info_.allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  root = NULL;
  json_info_.reset();
  json_info_.destroy_cluster_info();
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_EX_DATA_INFO, sizeof(TEST_EX_DATA_INFO));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.data_info_.parse(root, json_info_.allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
}


TEST_F(TestProxyJsonConfigInfo, test_parse_meta_table_info)
{
  int ret = OB_SUCCESS;
  Value *root = NULL;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);

  root = NULL;
  json_info_.reset();
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_META_TABLE_INFO, sizeof(TEST_META_TABLE_INFO));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.data_info_.meta_table_info_.parse(root, json_info_.allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  root = NULL;
  json_info_.reset();
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_EX_META_TABLE_INFO, sizeof(TEST_EX_META_TABLE_INFO));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.data_info_.meta_table_info_.parse(root, json_info_.allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  root = NULL;
  json_info_.reset();
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_META_TABLE_INFO_USERNAME, sizeof(TEST_META_TABLE_INFO_USERNAME));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.data_info_.meta_table_info_.parse(root, json_info_.allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, json_info_.data_info_.meta_table_info_.username_.config_string_ == "admin@sys");

  root = NULL;
  json_info_.reset();
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_META_TABLE_INFO_WRONG_USERNAME, sizeof(TEST_META_TABLE_INFO_WRONG_USERNAME));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.data_info_.meta_table_info_.parse(root, json_info_.allocator_);
  ASSERT_EQ(OB_INVALID_CONFIG, ret);
}

TEST_F(TestProxyJsonConfigInfo, test_parse_cluster_list)
{
  int ret = OB_SUCCESS;
  Value *root = NULL;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);

  root = NULL;
  json_info_.reset();
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_CLUSTER_LIST, sizeof(TEST_CLUSTER_LIST));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.data_info_.cluster_array_.parse(root, json_info_.allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestProxyJsonConfigInfo, test_parse_cluster_info)
{
  int ret = OB_SUCCESS;
  Value *root = NULL;
  ObProxyClusterInfo cluster_info;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);

  root = NULL;
  cluster_info.reset();
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_CLUSTER_INFO, sizeof(TEST_CLUSTER_INFO));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = cluster_info.parse(root, json_info_.allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  root = NULL;
  cluster_info.reset();
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_EX_CLUSTER_INFO, sizeof(TEST_EX_CLUSTER_INFO));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = cluster_info.parse(root, json_info_.allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestProxyJsonConfigInfo, test_parse_rslist_data)
{
  int ret = OB_SUCCESS;
  Value *root = NULL;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
  LocationList web_rslist;
  int64_t cluster_id = 0;
  bool is_primary = false;
  ObString cluster_name;

  root = NULL;
  json_info_.reset();
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_DATA_INFO, sizeof(TEST_DATA_INFO));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.data_info_.parse(root, json_info_.allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  root = NULL;
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_RSLIST, sizeof(TEST_RSLIST));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.parse_rslist_data(root, ObString::make_string("ob1"), cluster_id, web_rslist, is_primary, cluster_name);
  ASSERT_EQ(OB_SUCCESS, ret);

  root = NULL;
  web_rslist.reuse();
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_EX_RSLIST, sizeof(TEST_EX_RSLIST));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.parse_rslist_data(root, ObString::make_string("ob2"), cluster_id, web_rslist, is_primary, cluster_name);
  ASSERT_EQ(OB_SUCCESS, ret);

  root = NULL;
  web_rslist.reuse();
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_INVALID_RSLIST, sizeof(TEST_INVALID_RSLIST));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.parse_rslist_data(root, ObString::make_string("ob1"), cluster_id, web_rslist, is_primary, cluster_name);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, web_rslist.count());
}

TEST_F(TestProxyJsonConfigInfo, test_parse_rslist_data_readonly)
{
  int ret = OB_SUCCESS;
  Value *root = NULL;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
  LocationList web_rslist;
  int64_t cluster_id = 0;
  bool is_primary = false;
  ObString cluster_name;

  root = NULL;
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_DATA_INFO, sizeof(TEST_DATA_INFO));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.data_info_.parse(root, json_info_.allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  root = NULL;
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_EMPTY_RSLIST_EMPTY_READONLY, sizeof(TEST_EMPTY_RSLIST_EMPTY_READONLY));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.parse_rslist_data(root, ObString::make_string("ob1"), cluster_id, web_rslist, is_primary, cluster_name);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, web_rslist.count());


  root = NULL;
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_EMPTY_RSLIST_AND_READONLY, sizeof(TEST_EMPTY_RSLIST_AND_READONLY));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.parse_rslist_data(root, ObString::make_string("ob1"), cluster_id, web_rslist, is_primary, cluster_name);
  ASSERT_EQ(OB_SUCCESS, ret);

  root = NULL;
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_RSLIST_EMPTY_READONLY, sizeof(TEST_RSLIST_EMPTY_READONLY));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.parse_rslist_data(root, ObString::make_string("ob1"), cluster_id, web_rslist, is_primary, cluster_name);
  ASSERT_EQ(OB_SUCCESS, ret);

  root = NULL;
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_RSLIST_AND_READONLY, sizeof(TEST_RSLIST_AND_READONLY));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.parse_rslist_data(root, ObString::make_string("ob1"), cluster_id, web_rslist, is_primary, cluster_name);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestProxyJsonConfigInfo, test_parse_remote_rslist)
{
  int ret = OB_SUCCESS;
  Value *root = NULL;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);

  root = NULL;
  json_info_.reset();
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_DATA_INFO, sizeof(TEST_DATA_INFO));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.data_info_.parse(root, json_info_.allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  root = NULL;
  LocationList web_rslist;
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_REMOTE_RSLIST, sizeof(TEST_REMOTE_RSLIST));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.parse_remote_rslist(root, ObString::make_string("ob1"), 0, web_rslist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, web_rslist.count());
}

TEST_F(TestProxyJsonConfigInfo, test_parse_local_rslist)
{
  int ret = OB_SUCCESS;
  Value *root = NULL;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);

  root = NULL;
  json_info_.reset();
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_DATA_INFO, sizeof(TEST_DATA_INFO));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.data_info_.parse(root, json_info_.allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);

  root = NULL;
  memset(test_buf, 0, sizeof(test_buf));
  memcpy(test_buf, TEST_LOCAL_RSLIST, sizeof(TEST_LOCAL_RSLIST));
  ret = init_json(test_buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.parse_local_rslist(root);
  ASSERT_EQ(OB_SUCCESS, ret);

  char buf[OB_PROXY_CONFIG_BUFFER_SIZE];
  buf[0] = '\0';
  int64_t read_len = 0;
  ret = json_info_.rslist_to_json(buf, OB_PROXY_CONFIG_BUFFER_SIZE, read_len);
  ASSERT_EQ(OB_SUCCESS, ret);
  root = NULL;
  ret = init_json(buf, allocator, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_info_.parse_local_rslist(root);
  ASSERT_EQ(OB_SUCCESS, ret);
}


}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
