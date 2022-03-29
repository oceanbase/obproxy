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
#include "obproxy/proxy/mysqllib/ob_mysql_transaction_analyzer.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace oceanbase::common;
using namespace oceanbase::obmysql;

static const int64_t DEFAULT_PKT_LEN = 1024;

class TestMysqlTransactionAnalyzer : public ::testing::Test
{
public:
   int covert_hex_to_string(const char *hex_str, int64_t len, char *str);
   void analyze_mysql_response(ObMysqlTransactionAnalyzer &trans_analyzer,
                               const char* hex, int64_t len, bool trans_end);
   void analyze_mysql_response(ObMysqlTransactionAnalyzer &trans_analyzer,
                               const char* hex, bool trans_end = false);
   void get_analyzer_status(bool &is_trans_completed, bool &is_resp_complted,
                            ObMysqlTransactionAnalyzer &trans_analyzer);
  void make_one_16MB_packet(char *start) {
    // 1. packet len
    start[0] = 'F';
    start[1] = 'F';
    start[2] = 'F';
    start[3] = 'F';
    start[4] = 'F';
    start[5] = 'F';

    // 2. seq no need set

    // 3. content, on set the first is enough
    start[8] = '1';
    start[9] = '1';
  }

};

int TestMysqlTransactionAnalyzer::covert_hex_to_string(const char *hex_str, int64_t len, char *str)
{
  int ret = OB_SUCCESS;
  if (NULL == hex_str || len <= 0 || NULL == str || len%2 != 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; i < len/2; i++) {
      char tmp[3];
      tmp[0] = hex_str[2*i];
      tmp[1] = hex_str[2*i + 1];
      tmp[2] = '\0';
      sscanf(tmp, "%x", (unsigned int*)(&str[i]));
    }
    str[len/2] = '\0';
  }
  return ret;
}

void TestMysqlTransactionAnalyzer::analyze_mysql_response(
    ObMysqlTransactionAnalyzer &trans_analyzer, const char* hex, int64_t len, bool trans_end)
{
  int ret = OB_SUCCESS;
  bool is_completed = false;
  bool is_request_completed = false;
  char resp[len] = "\0";
  ret = covert_hex_to_string(hex, len, resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString resp_str;
  resp_str.assign(resp, (uint32_t)(len/2));
  ret = trans_analyzer.analyze_trans_response(resp_str);
  ASSERT_EQ(OB_SUCCESS, ret);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  if (!trans_end) {
    ASSERT_FALSE(is_completed);
    ASSERT_FALSE(trans_analyzer.is_trans_completed());
  } else {
    ASSERT_TRUE(is_request_completed);
    ASSERT_TRUE(is_completed);
    ASSERT_TRUE(trans_analyzer.is_trans_completed());
  }
}

void TestMysqlTransactionAnalyzer::analyze_mysql_response(
    ObMysqlTransactionAnalyzer &trans_analyzer, const char* hex, bool trans_end)
{
  analyze_mysql_response(trans_analyzer, hex, strlen(hex), trans_end);
}

void TestMysqlTransactionAnalyzer::get_analyzer_status(
    bool &is_trans_completed,
    bool &is_resp_completed,
    ObMysqlTransactionAnalyzer &trans_analyzer)
{
  is_trans_completed = false;
  is_resp_completed = false;
  is_trans_completed = trans_analyzer.is_trans_completed();
  is_resp_completed = trans_analyzer.is_resp_completed();
}

TEST_F(TestMysqlTransactionAnalyzer, test_insert)
{
  ObMysqlTransactionAnalyzer trans_analyzer;
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  int ret = OB_SUCCESS;
  bool is_completed = false;
  bool is_request_completed = false;
  char resp[] = {0x07, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00};
  char tmp[1024];
  const char *hex = "0700000100010002000000";
  ret = covert_hex_to_string(hex, strlen(hex), tmp);
  ObString resp_str;
  resp_str.assign(resp, 11);
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_completed);

  trans_analyzer.reset();
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  resp_str.assign(resp, 1);
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_request_completed);
  ASSERT_FALSE(is_completed);
  resp_str.assign(resp+1, 2);
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_request_completed);
  ASSERT_FALSE(is_completed);
  resp_str.assign(resp+3, 5);
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_request_completed);
  ASSERT_FALSE(is_completed);
  resp_str.assign(resp+8, 3);
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_request_completed);
  ASSERT_TRUE(is_completed);
};

TEST_F(TestMysqlTransactionAnalyzer, test_OB_MYSQL_COM_STATISTICS)
{
  ObMysqlTransactionAnalyzer trans_analyzer;
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_STATISTICS, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  int ret = OB_SUCCESS;
  bool is_completed = false;
  bool is_request_completed = false;
  char resp[DEFAULT_PKT_LEN];
  //OB_MYSQL_COM_STATISTICS response packet
  const char *hex = "84000001557074696d653a2036393739202054687265616473"
                    "3a203120205175657374696f6e733a2032382020536c6f77207"
                    "17565726965733a203020204f70656e733a2037352020466c75"
                    "7368207461626c65733a203120204f70656e207461626c65733"
                    "a20363820205175657269657320706572207365636f6e642061"
                    "76673a20302e303034";
  ret = covert_hex_to_string(hex, strlen(hex), resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString resp_str;
  resp_str.assign(resp, (uint32_t)(strlen(hex)/2));
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
};

TEST_F(TestMysqlTransactionAnalyzer, test_OB_MYSQL_COM_FIELD_LIST)
{
  ObMysqlTransactionAnalyzer trans_analyzer;
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_FIELD_LIST, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  int ret = OB_SUCCESS;
  bool is_completed = false;
  bool is_request_completed = false;
  char resp_column_num[DEFAULT_PKT_LEN];
  //OB_MYSQL_COM_FIELD_LIST response packet(SHOW  FIELDS FROM t3)
  const char *hex_column_num = "0100000106";
  ret = covert_hex_to_string(hex_column_num, strlen(hex_column_num), resp_column_num);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString resp_str;
  resp_str.assign(resp_column_num, (uint32_t)(strlen(hex_column_num)/2));
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);

  char resp_column_field1[DEFAULT_PKT_LEN];
  const char *hex_column_field1 = "460000020364656612696e666f726d6174696f6"
                                  "e5f736368656d6107434f4c554d4e5307434f4c"
                                  "554d4e53054669656c640b434f4c554d4e5f4e4"
                                  "14d450c080040000000fd0100000000";
  ret = covert_hex_to_string(hex_column_field1, strlen(hex_column_field1), resp_column_field1);
  ASSERT_EQ(OB_SUCCESS, ret);
  resp_str.assign(resp_column_field1, (uint32_t)(strlen(hex_column_field1)/2));
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);

  char resp_column_field2[DEFAULT_PKT_LEN];
  const char *hex_column_field2 = "450000030364656612696e666f726d6174696f6"
                                  "e5f736368656d6107434f4c554d4e5307434f4c"
                                  "554d4e5304547970650b434f4c554d4e5f54595"
                                  "0450c0800fdff0200fc1100000000";
  ret = covert_hex_to_string(hex_column_field2, strlen(hex_column_field2), resp_column_field2);
  ASSERT_EQ(OB_SUCCESS, ret);
  resp_str.assign(resp_column_field2, (uint32_t)(strlen(hex_column_field2)/2));
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);

  char resp_column_field3[DEFAULT_PKT_LEN];
  const char *hex_column_field3 = "450000040364656612696e666f726d6174696f"
                                  "6e5f736368656d6107434f4c554d4e5307434f"
                                  "4c554d4e53044e756c6c0b49535f4e554c4c41"
                                  "424c450c080003000000fd0100000000";
  ret = covert_hex_to_string(hex_column_field3, strlen(hex_column_field3), resp_column_field3);
  ASSERT_EQ(OB_SUCCESS, ret);
  resp_str.assign(resp_column_field3, (uint32_t)(strlen(hex_column_field3)/2));
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);

  char resp_eof[DEFAULT_PKT_LEN];
  const char *hex_eof = "0500000afe00002200";
  ret = covert_hex_to_string(hex_eof, strlen(hex_eof), resp_eof);
  ASSERT_EQ(OB_SUCCESS, ret);
  resp_str.assign(resp_eof, (uint32_t)(strlen(hex_eof)/2));
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
};

TEST_F(TestMysqlTransactionAnalyzer, test_transaction)
{
  ObMysqlTransactionAnalyzer trans_analyzer;
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  int ret = OB_SUCCESS;
  bool is_completed = false;
  bool is_request_completed = false;
  char resp_begin[DEFAULT_PKT_LEN];
  //OB_MYSQL_COM_QUERY response packet(begin;)
  const char *hex_begin = "0700000100000003000000";
  ret = covert_hex_to_string(hex_begin, strlen(hex_begin), resp_begin);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString resp_str;
  resp_str.assign(resp_begin, (uint32_t)(strlen(hex_begin)/2));
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_request_completed);
  ASSERT_FALSE(is_completed);

  //OB_MYSQL_COM_QUERY response (insert into t3 values(111))
  char resp_ok[DEFAULT_PKT_LEN];
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex_ok = "0700000100000003000000";
  ret = covert_hex_to_string(hex_ok, strlen(hex_ok), resp_ok);
  ASSERT_EQ(OB_SUCCESS, ret);
  resp_str.assign(resp_ok, (uint32_t)(strlen(hex_ok)/2));
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_request_completed);
  ASSERT_FALSE(is_completed);

  //OB_MYSQL_COM_QUERY response (insert into t3 (dd)) error response
  char resp_err[DEFAULT_PKT_LEN];
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex_err = "9b000001ff2804233432303030"
                        "596f75206861766520616e206572"
                        "726f7220696e20796f75722053514"
                        "c2073796e7461783b20636865636b"
                        "20746865206d616e75616c2074686"
                        "17420636f72726573706f6e647320"
                        "746f20796f7572204d7953514c207"
                        "365727665722076657273696f6e20"
                        "666f7220746865207269676874207"
                        "3796e74617820746f20757365206e"
                        "656172202727206174206c696e652031";
  ret = covert_hex_to_string(hex_err, strlen(hex_err), resp_err);
  ASSERT_EQ(OB_SUCCESS, ret);
  resp_str.assign(resp_err, (uint32_t)(strlen(hex_err)/2));
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_request_completed);
  ASSERT_FALSE(is_completed);
  ASSERT_FALSE(trans_analyzer.is_trans_completed());

  //OB_MYSQL_COM_QUERY response (rollback) response
  char resp_rb[DEFAULT_PKT_LEN];
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex_rb = "0700000100000002000000";
  ret = covert_hex_to_string(hex_rb, strlen(hex_rb), resp_rb);
  ASSERT_EQ(OB_SUCCESS, ret);
  resp_str.assign(resp_rb, (uint32_t)(strlen(hex_rb)/2));
  ret = trans_analyzer.analyze_trans_response(resp_str);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_request_completed);
  ASSERT_TRUE(is_completed);
  ASSERT_TRUE(trans_analyzer.is_trans_completed());
};

TEST_F(TestMysqlTransactionAnalyzer, test_OB_MYSQL_COM_QUERY_select)
{
  ObMysqlTransactionAnalyzer trans_analyzer;

  //OB_MYSQL_COM_QUERY response packet(begin;)
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex_begin = "0700000100000003000000";
  analyze_mysql_response(trans_analyzer, hex_begin);

  //OB_MYSQL_COM_QUERY response packet(select * from t1)
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex_column_num = "0100000101";
  analyze_mysql_response(trans_analyzer, hex_column_num);

  const char *hex_column_field1 = "28000002036465660a6d795f746"
                                  "573745f64620274330274330270"
                                  "6b02706b0c3f000b000000030350000000";
  analyze_mysql_response(trans_analyzer, hex_column_field1);

  const char *hex_column_eof = "05000003fe00002300";
  analyze_mysql_response(trans_analyzer, hex_column_eof);

  const char *hex_row_data1 = "020000040131";
  analyze_mysql_response(trans_analyzer, hex_row_data1);

  const char *hex_row_data2 = "020000050132";
  analyze_mysql_response(trans_analyzer, hex_row_data2);

  const char *hex_row_eof = "0500000afe00002300";
  analyze_mysql_response(trans_analyzer, hex_row_eof);

  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex_commit = "0700000100000002000000";
  analyze_mysql_response(trans_analyzer, hex_commit, true);
};

TEST_F(TestMysqlTransactionAnalyzer, test_OB_MYSQL_COM_QUERY_select_seq)
{
  ObMysqlTransactionAnalyzer trans_analyzer;

  //OB_MYSQL_COM_QUERY response packet(begin;)
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex_begin = "0700000100000003000000";
  analyze_mysql_response(trans_analyzer, hex_begin);

  //OB_MYSQL_COM_QUERY response packet(select * from t1)
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex = "0100000101"//column num
                    "28000002036465660a6d795f746"//column field1
                    "573745f64620274330274330270"
                    "6b02706b0c3f000b000000030350000000"//column EOF
                    "05000003fe00002300"
                    "020000040131"//row data 1
                    "020000050132"//row data 2
                    "0500000afe00002300";//EOF
  analyze_mysql_response(trans_analyzer, hex);

  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex_commit = "0700000100000002000000";
  analyze_mysql_response(trans_analyzer, hex_commit, true);
  int a = 0;
  a++;
};

TEST_F(TestMysqlTransactionAnalyzer, test_OB_MYSQL_COM_PROCESS_INFO)
{
  ObMysqlTransactionAnalyzer trans_analyzer;

  //OB_MYSQL_COM_PROCESS_INFO, response packet(begin;)
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_PROCESS_INFO, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex = "0100000108"//column num
                    "180000020364656600000002"
                    "4964000c3f0015000000088100000000" //column field1
                    "1a0000030364656600000004557"
                    "36572000c080010000000fd01001f0000"//column field2
                    "0500000afe00000200"//column EOF
                    "4100000b013204726f6f741031302e32"
                    "33322e33312e383a393431350a6d795f7"
                    "46573745f646205517565727901300469"
                    "6e69741053484f572050524f434553534c495354"//row data 1
                    "0500000cfe00000200";//row EOF
  analyze_mysql_response(trans_analyzer, hex, true);
};

TEST_F(TestMysqlTransactionAnalyzer, test_OB_MYSQL_COM_CREATE_DB)
{
  ObMysqlTransactionAnalyzer trans_analyzer;

  //OB_MYSQL_COM_CREATE_DB, response packet()
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_CREATE_DB, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex = "0700000100010002000000";
  analyze_mysql_response(trans_analyzer, hex, true);
};

TEST_F(TestMysqlTransactionAnalyzer, test_OB_MYSQL_COM_SHANDSHAKE)
{
  ObMysqlTransactionAnalyzer trans_analyzer;

  trans_analyzer.set_server_cmd(OB_MYSQL_COM_HANDSHAKE, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex = "4a0000000a352e362e313900030000006f532f3148"
                    "58615d00fff72102007f80150000000000000000000"
                    "0327c40676545675862644f3d006d7973716c5f6e617"
                    "46976655f70617373776f7264";
  analyze_mysql_response(trans_analyzer, hex, false);
  const char *hex1 = "00";
  analyze_mysql_response(trans_analyzer, hex1, false);
};

TEST_F(TestMysqlTransactionAnalyzer, test_OB_MYSQL_COM_LOGIN_error)
{
  ObMysqlTransactionAnalyzer trans_analyzer;

  trans_analyzer.set_server_cmd(OB_MYSQL_COM_LOGIN, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex = "4a000002ff1504233238303030416363657373206465"
                    "6e69656420666f7220757365722027726f6f742740273"
                    "1302e3233322e33312e382720287573696e67207061737"
                    "3776f72643a2059455329";
  analyze_mysql_response(trans_analyzer, hex, false);
};

TEST_F(TestMysqlTransactionAnalyzer, test_OB_MYSQL_COM_LOGIN_ok)
{
  ObMysqlTransactionAnalyzer trans_analyzer;

  trans_analyzer.set_server_cmd(OB_MYSQL_COM_LOGIN, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex = "070000020000000200";
  analyze_mysql_response(trans_analyzer, hex, false);
  const char *hex1 = "0000";
  analyze_mysql_response(trans_analyzer, hex1, true);
};


TEST_F(TestMysqlTransactionAnalyzer, test_EOF_response)
{
  ObMysqlTransactionAnalyzer trans_analyzer;
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  int ret = OB_SUCCESS;
  bool is_completed = false;
  bool is_request_completed = false;
  char resp_s[DEFAULT_PKT_LEN];
  //OB_MYSQL_COM_QUERY(select * from t3), response packet(EOF packet)
  const char *hex = "010000010128000002036465660a6d795"
                    "f746573745f646202743302743302706b02706b0c3"
                    "f000b00000003035000000005000003fe0000220002"
                    "00000401310200000501320200000601350400000703"
                    "31303104000008033131310500000904313131310500"
                    "000afe00002280";
  ret = covert_hex_to_string(hex, strlen(hex), resp_s);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString resp_str;
  resp_str.assign(resp_s, (uint32_t)(strlen(hex)/2));
  ObMysqlResp resp;
  ret = trans_analyzer.analyze_trans_response(resp_str, &resp);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_completed);
  ASSERT_TRUE(is_request_completed);

  ObRespAnalyzeResult &result = resp.get_analyze_result();
  ASSERT_TRUE(result.is_trans_completed());
  ASSERT_TRUE(result.is_resp_completed());
  ASSERT_FALSE(result.is_error_resp());
  ASSERT_TRUE(result.is_partition_hit());
};


TEST_F(TestMysqlTransactionAnalyzer, test_OK_response)
{
  ObMysqlTransactionAnalyzer trans_analyzer;
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  int ret = OB_SUCCESS;
  bool is_completed = false;
  bool is_request_completed = false;
  char resp_s[DEFAULT_PKT_LEN];
  //OB_MYSQL_COM_QUERY(insert into t3 values(6)), response packet(OK packet)
  const char *hex = "0700000100010002800000";
  ret = covert_hex_to_string(hex, strlen(hex), resp_s);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString resp_str;
  resp_str.assign(resp_s, (uint32_t)(strlen(hex)/2));
  ObMysqlResp resp;
  ret = trans_analyzer.analyze_trans_response(resp_str, &resp);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_completed);
  ASSERT_TRUE(is_request_completed);

  ObRespAnalyzeResult &result = resp.get_analyze_result();
  ASSERT_TRUE(result.is_trans_completed());
  ASSERT_TRUE(result.is_resp_completed());
  ASSERT_FALSE(result.is_error_resp());
  ASSERT_TRUE(result.is_partition_hit());

  // don not compare ok packet, analyer do not decode ok packet
};

TEST_F(TestMysqlTransactionAnalyzer, test_ERROR_response)
{
  ObMysqlTransactionAnalyzer trans_analyzer;
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  int ret = OB_SUCCESS;
  bool is_completed = false;
  bool is_request_completed = false;
  char resp_s[DEFAULT_PKT_LEN];
  //OB_MYSQL_COM_QUERY(insert into t3 values(6, 8)), response packet(OK packet)
  const char *hex = "38000001ff7004233231533031436f6"
                    "c756d6e20636f756e7420646f65736e"
                    "2774206d617463682076616c75652063"
                    "6f756e7420617420726f772031";
  ret = covert_hex_to_string(hex, strlen(hex), resp_s);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString resp_str;
  resp_str.assign(resp_s, (uint32_t)(strlen(hex)/2));
  ObMysqlResp resp;
  ret = trans_analyzer.analyze_trans_response(resp_str, &resp);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_request_completed);

  ObRespAnalyzeResult &result = resp.get_analyze_result();
  ASSERT_TRUE(result.is_resp_completed());
  ASSERT_TRUE(result.is_error_resp());
  ASSERT_TRUE(result.is_partition_hit());

  OMPKError &error_pkt = result.get_error_pkt();
  ASSERT_EQ(0xffU, error_pkt.get_field_count());
  ASSERT_EQ(1136U, error_pkt.get_err_code());
};

TEST_F(TestMysqlTransactionAnalyzer, test_oceanbase_mysql_protocol)
{
  ObMysqlTransactionAnalyzer trans_analyzer;
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, OCEANBASE_MYSQL_PROTOCOL_MODE, false, false);
  int ret = OB_SUCCESS;
  bool is_completed = false;
  bool is_request_completed = false;
  char resp_s[DEFAULT_PKT_LEN];
  //OB_MYSQL_COM_QUERY(select * from t3), response packet(2 * EOF packet + OK packet)
  const char *hex = "010000010128000002036465660a6d795"
                    "f746573745f646202743302743302706b02706b0c3"
                    "f000b00000003035000000005000003fe0000220002"
                    "00000401310200000501320200000601350400000703"
                    "31303104000008033131310500000904313131310500"
                    "000afe000022800700000100000002000000";
  ret = covert_hex_to_string(hex, strlen(hex), resp_s);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString resp_str;
  resp_str.assign(resp_s, (uint32_t)(strlen(hex)/2));
  ObMysqlResp resp;
  ret = trans_analyzer.analyze_trans_response(resp_str, &resp);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_completed);
  ASSERT_TRUE(is_request_completed);

  ObRespAnalyzeResult &result = resp.get_analyze_result();
  ASSERT_TRUE(result.is_trans_completed());
  ASSERT_TRUE(result.is_resp_completed());
  ASSERT_FALSE(result.is_error_resp());
  ASSERT_TRUE(result.is_partition_hit());

};

TEST_F(TestMysqlTransactionAnalyzer, test_OB_MYSQL_COM_STATISTICS_WITH_OK)
{
  ObMysqlTransactionAnalyzer trans_analyzer;
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_STATISTICS, OCEANBASE_MYSQL_PROTOCOL_MODE, true, false);
  int ret = OB_SUCCESS;
  bool is_completed = false;
  bool is_request_completed = false;
  char resp_s[DEFAULT_PKT_LEN];
  // OB_MYSQL_COM_STASTICS response  packet(String EOF packet + OK packet)
  const char *hex = "1a00000141637469766520746872656164732"
                    "06e6f7420737570706f7274080000020000002"
                    "200000000";
  ret = covert_hex_to_string(hex, strlen(hex), resp_s);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString resp_str;
  resp_str.assign(resp_s, (uint32_t)(strlen(hex)/2));
  ObMysqlResp resp;
  ret = trans_analyzer.analyze_trans_response(resp_str, &resp);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_completed);
  ASSERT_TRUE(is_request_completed);

  ObRespAnalyzeResult &result = resp.get_analyze_result();
  ASSERT_TRUE(result.is_trans_completed());
  ASSERT_TRUE(result.is_resp_completed());
};


TEST_F(TestMysqlTransactionAnalyzer, test_OB_MYSQL_COM_STATISTICS_WITHOUT_BODY_AND_WITH_OK)
{
  ObMysqlTransactionAnalyzer trans_analyzer;
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_STATISTICS, OCEANBASE_MYSQL_PROTOCOL_MODE, true, false);
  int ret = OB_SUCCESS;
  bool is_completed = false;
  bool is_request_completed = false;
  char resp_s[DEFAULT_PKT_LEN];
  // OB_MYSQL_COM_STASTICS response packe: String EOF packet(only has packet header with out body) + OK packet
  const char *hex = "00000000080000020000002"
                    "200000000";
  ret = covert_hex_to_string(hex, strlen(hex), resp_s);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString resp_str;
  resp_str.assign(resp_s, (uint32_t)(strlen(hex)/2));
  ObMysqlResp resp;
  ret = trans_analyzer.analyze_trans_response(resp_str, &resp);
  get_analyzer_status(is_completed, is_request_completed, trans_analyzer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_completed);
  ASSERT_TRUE(is_request_completed);

  ObRespAnalyzeResult &result = resp.get_analyze_result();
  ASSERT_TRUE(result.is_trans_completed());
  ASSERT_TRUE(result.is_resp_completed());
};

TEST_F(TestMysqlTransactionAnalyzer, test_OB_MYSQL_COM_QUERY_select_great_than_16MB)
{
  ObMysqlTransactionAnalyzer trans_analyzer;

  //OB_MYSQL_COM_QUERY response packet(begin;)
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex_begin = "0700000100000003000000";
  analyze_mysql_response(trans_analyzer, hex_begin);

  //OB_MYSQL_COM_QUERY response packet(select * from t1)
  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex_column_num = "0100000101";
  analyze_mysql_response(trans_analyzer, hex_column_num);

  const char *hex_column_field1 = "28000002036465660a6d795f746"
                                  "573745f64620274330274330270"
                                  "6b02706b0c3f000b000000030350000000";
  analyze_mysql_response(trans_analyzer, hex_column_field1);

  const char *hex_column_eof = "05000003fe00002300";
  analyze_mysql_response(trans_analyzer, hex_column_eof);

  // make large row > 16MB (packet1 16MB-1  packet2 16MB-1  packet3 0B)
  int total_len = (MYSQL_PACKET_MAX_LENGTH * 2 + 3 * MYSQL_NET_HEADER_LENGTH) * 2;
  char *new_row_1 = new char[total_len];
  memset(new_row_1, '\0', total_len);
  make_one_16MB_packet(new_row_1);
  make_one_16MB_packet(new_row_1 + (MYSQL_PACKET_MAX_LENGTH + MYSQL_NET_HEADER_LENGTH) * 2);
  char *tmp_start = new_row_1 + (MYSQL_PACKET_MAX_LENGTH + MYSQL_NET_HEADER_LENGTH) * 4;
  tmp_start[0]='0';
  tmp_start[1]='0';
  tmp_start[2]='0';
  tmp_start[3]='0';
  tmp_start[4]='0';
  tmp_start[5]='0';

  analyze_mysql_response(trans_analyzer, new_row_1, total_len, false);
  delete []new_row_1;

  // make large row > 16MB (packet1 16MB-1  packet2 16MB-1  packet3 10B)
  total_len = (MYSQL_PACKET_MAX_LENGTH * 2 + 3 * MYSQL_NET_HEADER_LENGTH + 10) * 2;
  char *new_row_2 = new char[total_len];
  memset(new_row_2, '\0', total_len);
  make_one_16MB_packet(new_row_2);
  make_one_16MB_packet(new_row_2 + (MYSQL_PACKET_MAX_LENGTH + MYSQL_NET_HEADER_LENGTH) * 2);
  tmp_start = new_row_2 + (MYSQL_PACKET_MAX_LENGTH + MYSQL_NET_HEADER_LENGTH) * 4;
  tmp_start[0]='0';
  tmp_start[1]='a';
  tmp_start[2]='0';
  tmp_start[3]='0';
  tmp_start[4]='0';
  tmp_start[5]='0';

  analyze_mysql_response(trans_analyzer, new_row_2, total_len, false);
  delete []new_row_2;

  // make large row > 16MB (packet1 16MB-1  packet2 16MB-1  packet3 16M - 2)
  total_len = (MYSQL_PACKET_MAX_LENGTH * 2 + 3 * MYSQL_NET_HEADER_LENGTH + 10 + MYSQL_PACKET_MAX_LENGTH - 1) * 2;
  char *new_row_3 = new char[total_len];
  memset(new_row_3, '\0', total_len);
  make_one_16MB_packet(new_row_3);
  make_one_16MB_packet(new_row_3 + (MYSQL_PACKET_MAX_LENGTH + MYSQL_NET_HEADER_LENGTH) * 2);
  tmp_start = new_row_3 + (MYSQL_PACKET_MAX_LENGTH + MYSQL_NET_HEADER_LENGTH) * 4;
  tmp_start[0]='f';
  tmp_start[1]='e';
  tmp_start[2]='f';
  tmp_start[3]='f';
  tmp_start[4]='f';
  tmp_start[5]='f';

  analyze_mysql_response(trans_analyzer, new_row_3, total_len, false);
  delete []new_row_3;

  const char *hex_row_eof = "0500000afe00002300";
  analyze_mysql_response(trans_analyzer, hex_row_eof);

  trans_analyzer.set_server_cmd(OB_MYSQL_COM_QUERY, STANDARD_MYSQL_PROTOCOL_MODE, false, false);
  const char *hex_commit = "0700000100000002000000";
  analyze_mysql_response(trans_analyzer, hex_commit, true);
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

