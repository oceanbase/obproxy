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
#include "ob_proxy_auth_parser.h"
#include "obproxy/iocore/eventsystem/ob_io_buffer.h"
#include "obproxy/iocore/eventsystem/ob_event_system.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace oceanbase::common;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obmysql;

class TestProxyAuthParser : public ::testing::Test
{
};
static const int64_t OB_MAX_AUTH_PKT_LEN = 1024;
TEST_F(TestProxyAuthParser, test_illegal_condition)
{
  int ret = OB_SUCCESS;
  ObMysqlAuthRequest request;
  init_event_system(EVENT_SYSTEM_MODULE_VERSION);
  const char *hex = "\0";

  char pkt[OB_MAX_AUTH_PKT_LEN];
  ret = ObProxyAuthParser::covert_hex_to_string(ObString::make_string(hex), pkt, OB_MAX_AUTH_PKT_LEN);
  ASSERT_TRUE(OB_INVALID_ARGUMENT == ret);

  ObMIOBuffer *buffer = new_miobuffer(8 * 1024);
  ASSERT_TRUE(NULL != buffer);
  ObIOBufferReader *reader = buffer->alloc_reader();
  ASSERT_TRUE(NULL != reader);

  ObMysqlPacketMeta meta;
  meta.cmd_ = OB_MYSQL_COM_QUERY;
  request.set_packet_meta(meta);

  ret = request.add_auth_request(NULL, 0);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = request.add_auth_request(reader, 0);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ObString default_tenant_name(OB_SYS_TENANT_NAME);
  ObString default_cluster_name("obcluster");
  ret = ObProxyAuthParser::parse_auth(request, default_tenant_name, default_cluster_name);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
};

TEST_F(TestProxyAuthParser, test_login_no_database)
{
  int ret = OB_SUCCESS;
  ObMysqlAuthRequest request;
  init_event_system(EVENT_SYSTEM_MODULE_VERSION);
  const char *hex = "3a00000185a60300000000010800000000000000000"
                    "00000000000000000000000000000726f6f74001420e"
                    "bffc065616ed970ada3fec231049392a2338d";
  char pkt[OB_MAX_AUTH_PKT_LEN];
  int64_t pkt_len = 0;
  ret = ObProxyAuthParser::covert_hex_to_string(ObString::make_string(hex), pkt, OB_MAX_AUTH_PKT_LEN);
  pkt_len = (int64_t)strlen(hex)/2;
  ASSERT_TRUE(OB_SUCC(ret));

  ObMIOBuffer *buffer = new_miobuffer(8 * 1024);
  ASSERT_TRUE(NULL != buffer);
  ObIOBufferReader *reader = buffer->alloc_reader();
  ASSERT_TRUE(NULL != reader);
  int64_t written = 0;
  buffer->write(pkt, pkt_len, written);
  ASSERT_TRUE(written == pkt_len);

  ObMysqlPacketMeta meta;
  meta.cmd_ = OB_MYSQL_COM_LOGIN;
  request.set_packet_meta(meta);
  ret = request.add_auth_request(reader, 0);
  ASSERT_TRUE(OB_SUCC(ret));

  ObString default_tenant_name("obproxy");
  ObString default_cluster_name("obcluster");
  ret = ObProxyAuthParser::parse_auth(request, default_tenant_name, default_cluster_name);
  ASSERT_TRUE(OB_SUCC(ret));

  ObHSRResult &result = request.get_hsr_result();
  const char *username = "root";
  ObString username_str(username);
  int64_t max_pkt_len = 16*1024*1024; // 16M
  int64_t charset =8;
  char auth_resp_hex[OB_MAX_AUTH_PKT_LEN];
  ret = ObProxyAuthParser::covert_string_to_hex(result.response_.get_auth_response(), auth_resp_hex, OB_MAX_AUTH_PKT_LEN);
  ASSERT_EQ(OB_SUCCESS, ret);
  const char *auth_resp_hex_parsed = "20ebffc065616ed970ada3fec231049392a2338d";

  ASSERT_EQ(result.user_name_, username_str);
  char fullname[OB_PROXY_FULL_USER_NAME_MAX_LEN + 1] = {'\0'};
  snprintf(fullname, OB_PROXY_FULL_USER_NAME_MAX_LEN + 1, "%s@%s",
           username, to_cstring(default_tenant_name));
  ASSERT_EQ(result.response_.get_username(), ObString::make_string(fullname));
  ASSERT_EQ(default_tenant_name, result.tenant_name_);
  ASSERT_EQ(result.response_.get_char_set(), charset);
  ASSERT_EQ(result.response_.get_max_packet_size(), max_pkt_len);
  ASSERT_STREQ(auth_resp_hex, auth_resp_hex_parsed);
};

TEST_F(TestProxyAuthParser, test_login_has_database)
{
  int ret = OB_SUCCESS;
  ObMysqlAuthRequest request;
  init_event_system(EVENT_SYSTEM_MODULE_VERSION);
  const char *hex = "490000018da603000000000108000000000000000"
                    "0000000000000000000000000000000726f6f7472"
                    "6f6f74001473a4465b151a2941c223d5244653097"
                    "4448417366d795f746573745f646200";
  char pkt[OB_MAX_AUTH_PKT_LEN];
  int64_t pkt_len = 0;
  ret = ObProxyAuthParser::covert_hex_to_string(ObString::make_string(hex), pkt, OB_MAX_AUTH_PKT_LEN);
  pkt_len = (int64_t)strlen(hex)/2;
  ASSERT_TRUE(OB_SUCC(ret));

  ObMIOBuffer *buffer = new_miobuffer(8 * 1024);
  ASSERT_TRUE(NULL != buffer);
  ObIOBufferReader *reader = buffer->alloc_reader();
  ASSERT_TRUE(NULL != reader);
  int64_t written = 0;
  buffer->write(pkt, pkt_len, written);
  ASSERT_TRUE(written == pkt_len);

  ObMysqlPacketMeta meta;
  meta.cmd_ = OB_MYSQL_COM_LOGIN;
  request.set_packet_meta(meta);
  ret = request.add_auth_request(reader, 0);
  ASSERT_TRUE(OB_SUCC(ret));

  ObString default_tenant_name(OB_SYS_TENANT_NAME);
  ObString default_cluster_name("obcluster");
  ret = ObProxyAuthParser::parse_auth(request, default_tenant_name, default_cluster_name);
  ASSERT_TRUE(OB_SUCC(ret));

  ObHSRResult &result = request.get_hsr_result();
  const char *username = "rootroot";
  ObString username_str(username);
  const char *db_name = "my_test_db";
  ObString db_name_str(db_name);
  char auth_resp_hex[OB_MAX_AUTH_PKT_LEN];

  ret = ObProxyAuthParser::covert_string_to_hex(result.response_.get_auth_response(), auth_resp_hex, OB_MAX_AUTH_PKT_LEN);
  ASSERT_EQ(OB_SUCCESS, ret);
  const char *auth_resp_hex_parsed = "73a4465b151a2941c223d5244653097444841736";

  int64_t max_pkt_len = 16*1024*1024; // 16M
  int64_t charset =8;

  ASSERT_EQ(result.user_name_, username_str);
  char fullname[OB_PROXY_FULL_USER_NAME_MAX_LEN + 1] = {'\0'};
  snprintf(fullname, OB_PROXY_FULL_USER_NAME_MAX_LEN + 1, "%s@%s",
           username,  to_cstring(default_tenant_name));
  ASSERT_EQ(result.response_.get_username(), ObString::make_string(fullname));
  ASSERT_EQ(ObString::make_string(OB_SYS_TENANT_NAME), result.tenant_name_);
  ASSERT_EQ(result.response_.get_database(), db_name_str);
  ASSERT_EQ(result.response_.get_char_set(), charset);
  ASSERT_EQ((int64_t)(result.response_.get_max_packet_size()), max_pkt_len);
  ASSERT_STREQ(auth_resp_hex_parsed, auth_resp_hex);
};

TEST_F(TestProxyAuthParser, test_login_with_tenant_name_and_user_name)
{
  int ret = OB_SUCCESS;
  ObMysqlAuthRequest request;
  init_event_system(EVENT_SYSTEM_MODULE_VERSION);
  const char *hex = "c600000185a67f000000000121000000000"
                    "00000000000000000000000000000000000"
                    "00757365726e616d654074656e616e746e6"
                    "16d650014c7b74b415eb1c1050fb3a30ddec"
                    "5d015aa2bd7e26d7973716c5f6e617469766"
                    "55f70617373776f72640066035f6f73054c6"
                    "96e75780c5f636c69656e745f6e616d65086c"
                    "69626d7973716c045f7069640532373934300f"
                    "5f636c69656e745f76657273696f6e06352e36"
                    "2e3233095f706c6174666f726d067838365f3"
                    "6340c70726f6772616d5f6e616d65056d7973716c";
  char pkt[OB_MAX_AUTH_PKT_LEN];
  int64_t pkt_len = 0;
  ret = ObProxyAuthParser::covert_hex_to_string(ObString::make_string(hex), pkt, OB_MAX_AUTH_PKT_LEN);
  pkt_len = (int64_t)strlen(hex)/2;
  ASSERT_TRUE(OB_SUCC(ret));

  ObMIOBuffer *buffer = new_miobuffer(8 * 1024);
  ASSERT_TRUE(NULL != buffer);
  ObIOBufferReader *reader = buffer->alloc_reader();
  ASSERT_TRUE(NULL != reader);
  int64_t written = 0;
  buffer->write(pkt, pkt_len, written);
  ASSERT_TRUE(written == pkt_len);

  ObMysqlPacketMeta meta;
  meta.cmd_ = OB_MYSQL_COM_LOGIN;
  request.set_packet_meta(meta);
  ret = request.add_auth_request(reader, 0);
  ASSERT_TRUE(OB_SUCC(ret));

  ObString default_tenant_name(OB_SYS_TENANT_NAME);
  ObString default_cluster_name("obcluster");
  ret = ObProxyAuthParser::parse_auth(request, default_tenant_name, default_cluster_name);
  ASSERT_TRUE(OB_SUCC(ret));

  ObHSRResult &result = request.get_hsr_result();
  ObString fullname_str("username@tenantname");
  ObString username_str("username");
  ObString tenantname_str("tenantname");
  char auth_resp_hex[OB_MAX_AUTH_PKT_LEN];
  ret = ObProxyAuthParser::covert_string_to_hex(result.response_.get_auth_response(), auth_resp_hex, OB_MAX_AUTH_PKT_LEN);
  ASSERT_EQ(OB_SUCCESS, ret);
  const char *auth_resp_hex_parsed = "c7b74b415eb1c1050fb3a30ddec5d015aa2bd7e2";


  int64_t max_pkt_len = 16*1024*1024; // 16M
  int64_t charset = 0x21;

  ASSERT_EQ(result.user_name_, username_str);
  ASSERT_EQ(result.response_.get_username(), fullname_str);
  ASSERT_EQ(result.tenant_name_, tenantname_str);
  ASSERT_TRUE(result.response_.get_database().empty());
  ASSERT_EQ(result.response_.get_char_set(), charset);
  ASSERT_EQ((int64_t)(result.response_.get_max_packet_size()), max_pkt_len);
  ASSERT_STREQ(auth_resp_hex_parsed, auth_resp_hex);
};
}
}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

