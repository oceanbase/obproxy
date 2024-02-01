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
#include "lib/ob_define.h"
#include "obproxy/utils/ob_zlib_stream_compressor.h"
#include "obproxy/proxy/mysqllib/ob_mysql_compress_analyzer.h"
#include "obproxy/iocore/eventsystem/ob_io_buffer.h"
#include "ob_mysql_test_utils.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace obproxy
{
using namespace oceanbase::common;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::event;

class TestMysqlCompressAnalyzer : public ::testing::Test
{
public:
};

TEST_F(TestMysqlCompressAnalyzer, test_comressed_ok_packet)
{
  //  Server Status: 0x0003
  //  .... .... .... ...1 = In transaction: Set
  //  .... .... .... ..1. = AUTO_COMMIT: Set
  //  .... .... .... .0.. = More results: Not set
  //  .... .... .... 0... = Multi query - more resultsets: Not set
  //  .... .... ...0 .... = Bad index used: Not set
  //  .... .... ..0. .... = No index used: Not set
  //  .... .... .0.. .... = Cursor exists: Not set
  //  .... .... 0... .... = Last row sent: Not set
  //  .... ...0 .... .... = database dropped: Not set
  //  .... ..0. .... .... = No backslash escapes: Not set
  //  .... .0.. .... .... = Session state changed: Not set
  //  .... 0... .... .... = Query was slow: Not set
  //  ...0 .... .... .... = PS Out Params: Not set
  const char *ok_hex = "0700000100010003000000";

  int64_t ok_str_len = strlen(ok_hex) / 2;
  char ok_str[100];
  int ret = OB_SUCCESS;
  ret = ObMysqlTestUtils::covert_hex_to_string(ok_hex, strlen(ok_hex), ok_str);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool is_checksum_on[2] = {true, false};
  for (int64_t k = 0; k < 2; ++k) {
    char compressed_buf[100];
    uint8_t seq = 1;
    int64_t compress_packet_len = 0;
    ret = ObMysqlTestUtils::build_mysql_compress_packet(ok_str, ok_str_len, seq, compressed_buf,
        sizeof(compressed_buf), compress_packet_len, is_checksum_on[k]);
    ASSERT_EQ(OB_SUCCESS, ret);

    LOG_INFO("after build compress packet", "origin len", ok_str_len, K(compress_packet_len),
             "is_checksum_on", is_checksum_on[k]);

    ObMysqlCompressAnalyzer analyzer;
    ret = analyzer.init(seq, ObMysqlCompressAnalyzer::DECOMPRESS_MODE, OB_MYSQL_COM_QUERY, OCEANBASE_MYSQL_PROTOCOL_MODE, false, 0, 0, 0, false);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObMysqlResp resp;
    ObString resp_str(compress_packet_len, compressed_buf);
    ret = analyzer.analyze_compressed_response(resp_str, resp);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(is_checksum_on[k], analyzer.is_compressed_payload());
    ASSERT_TRUE(resp.get_analyze_result().is_resp_completed_);
    ASSERT_FALSE(resp.get_analyze_result().is_trans_completed_);
    ASSERT_TRUE(resp.get_analyze_result().is_ok_resp());

    // stream analyze
    ObMysqlResp resp2;
    ObMysqlCompressAnalyzer analyzer2;
    ret = analyzer2.init(seq, ObMysqlCompressAnalyzer::DECOMPRESS_MODE, OB_MYSQL_COM_QUERY, OCEANBASE_MYSQL_PROTOCOL_MODE, false, 0, 0, 0, false);
    ASSERT_EQ(OB_SUCCESS, ret);
    for (int64_t i = 0; i < compress_packet_len; ++i) {
      ObString resp_str(1, compressed_buf + i);
      ret = analyzer2.analyze_compressed_response(resp_str, resp2);
      ASSERT_EQ(OB_SUCCESS, ret);
      if (i == compress_packet_len - 1) {
        ASSERT_TRUE(resp2.get_analyze_result().is_resp_completed_);
      } else {
        ASSERT_FALSE(resp2.get_analyze_result().is_resp_completed_);
      }
    }
    ASSERT_EQ(is_checksum_on[k], analyzer.is_compressed_payload());
    ASSERT_TRUE(resp2.get_analyze_result().is_resp_completed_);
    ASSERT_FALSE(resp2.get_analyze_result().is_trans_completed_);
    ASSERT_TRUE(resp2.get_analyze_result().is_ok_resp());
  }
};


TEST_F(TestMysqlCompressAnalyzer, test_comressed_eof_packet)
{
  // mysql> select * from t1;
  //   +-----+--------+
  //   | id  | status |
  //   +-----+--------+
  //   |   0 |      0 |
  //   |   1 |      0 |
  //   |   2 |      2 |
  //   |   3 |      2 |
  //   | 111 |  11111 |
  //   +-----+--------+
  //   5 rows in set (0.00 sec)
  //
  //
  // .. eof...eof + ok
  const char *rs_hex =
      "0100000102" // column count
      "220000020364656604746573740274310274310269640269640c3f000b000000030350000000" // column id
      "2a00000303646566047465737402743102743106737461747573067374617475730c3f000b000000030000000000" // column status
      "05000004fe00002300" // eof
      "0400000501300130"   // row 1
      "0400000601310130"   // row 2
      "0400000701320132"   // row 3
      "0400000801330132"   // row 4
      "0a00000903313131053131313131"; // row 5
  const char *rs_tail_hex =
      "0500000afe00002300"  // eof
      "0700000b00000002000000" // etra ok packet
      ;

  // 1. build first compress packet
  char rs_str[1000];
  int64_t rs_str_len = strlen(rs_hex) / 2;
  int ret = OB_SUCCESS;
  ret = ObMysqlTestUtils::covert_hex_to_string(rs_hex, strlen(rs_hex), rs_str);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool is_checksum_on[2] = {true, false};
  for (int64_t k = 0; k < 2; ++k) {
    char compressed_buf[1000];
    uint8_t seq = 2;
    int64_t compress_packet_len = 0;
    ret = ObMysqlTestUtils::build_mysql_compress_packet(rs_str, rs_str_len, seq, compressed_buf,
        sizeof(compressed_buf), compress_packet_len, is_checksum_on[k]);
    ASSERT_EQ(OB_SUCCESS, ret);

    LOG_INFO("after build compress packet1", "origin len", rs_str_len, K(compress_packet_len),
        "is_checksum_on", is_checksum_on[k]);

    // 2. build second compress packet
    char rs_tail_str[1000];
    int64_t rs_tail_len = strlen(rs_tail_hex) / 2;
    ret = ObMysqlTestUtils::covert_hex_to_string(rs_tail_hex, strlen(rs_tail_hex), rs_tail_str);
    ASSERT_EQ(OB_SUCCESS, ret);

    char compressed_buf2[1000];
    uint8_t seq2 = 2;
    int64_t compress_packet_len2 = 0;
    ret = ObMysqlTestUtils::build_mysql_compress_packet(rs_tail_str, rs_tail_len, seq2,
        compressed_buf2, sizeof(compressed_buf2), compress_packet_len2, is_checksum_on[k]);
    ASSERT_EQ(OB_SUCCESS, ret);

    LOG_INFO("after build compress packet2", "origin len", rs_tail_len, K(compress_packet_len2),
        "is_checksum_on", is_checksum_on[k]);

    // 3. copy two compressed packets to one buff
    char compressed_packet[1000];
    int64_t total_compressed_packet_len = 0;
    MEMCPY(compressed_packet, compressed_buf, compress_packet_len);
    MEMCPY(compressed_packet + compress_packet_len, compressed_buf2, compress_packet_len2);
    total_compressed_packet_len = compress_packet_len + compress_packet_len2;

    // 4. analyze in one buf
    ObMysqlCompressAnalyzer analyzer;
    uint8_t request_seq = 1;
    ret = analyzer.init(request_seq, ObMysqlCompressAnalyzer::DECOMPRESS_MODE, OB_MYSQL_COM_QUERY, OCEANBASE_MYSQL_PROTOCOL_MODE, false, 0, 0, 0, false);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObMysqlResp resp;
    ObString resp_str(total_compressed_packet_len, compressed_packet);
    ret = analyzer.analyze_compressed_response(resp_str, resp);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(is_checksum_on[k], analyzer.is_compressed_payload());
    ASSERT_TRUE(resp.get_analyze_result().is_resp_completed_);
    ASSERT_TRUE(resp.get_analyze_result().is_trans_completed_);
    ASSERT_TRUE(resp.get_analyze_result().is_eof_resp());

    // 5. analyze in stream one byte by one byte
    ObMysqlCompressAnalyzer analyzer2;
    ret = analyzer2.init(request_seq, ObMysqlCompressAnalyzer::DECOMPRESS_MODE, OB_MYSQL_COM_QUERY, OCEANBASE_MYSQL_PROTOCOL_MODE, false, 0, 0, 0, false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObMIOBuffer *mio_buffer = analyzer2.get_transfer_miobuf();
    ObIOBufferReader *reader = mio_buffer->alloc_reader();
    ObMysqlResp resp2;
    for (int64_t i = 0; i < total_compressed_packet_len; ++i) {
      ObString resp_str(1, compressed_packet + i);
      ret = analyzer2.analyze_compressed_response(resp_str, resp2);
      ASSERT_EQ(OB_SUCCESS, ret);
      if (i == total_compressed_packet_len - 1) {
        ASSERT_TRUE(resp2.get_analyze_result().is_resp_completed_);
      } else {
        ASSERT_FALSE(resp2.get_analyze_result().is_resp_completed_);
      }
    }
    ASSERT_EQ(is_checksum_on[k], analyzer.is_compressed_payload());
    ASSERT_TRUE(resp2.get_analyze_result().is_resp_completed_);
    ASSERT_TRUE(resp2.get_analyze_result().is_trans_completed_);
    ASSERT_TRUE(resp2.get_analyze_result().is_eof_resp());
    int64_t total_len = rs_str_len + rs_tail_len;
    ASSERT_EQ(reader->read_avail(), total_len); // mio_buffer contain the last compressed packet
    char tmp_buf[1000];
    char *end_pos = reader->copy(tmp_buf, total_len);
    ASSERT_EQ(end_pos - tmp_buf, total_len);
    ASSERT_EQ(0, MEMCMP(tmp_buf, rs_str, static_cast<uint32_t>(rs_str_len)));
    ASSERT_EQ(0, MEMCMP(tmp_buf + rs_str_len, rs_tail_str, static_cast<uint32_t>(rs_tail_len)));

    reader = NULL;
    mio_buffer = NULL;
  }
};

TEST_F(TestMysqlCompressAnalyzer, test_comressed_error_packet)
{
  // mysql> select * from t1xxx;
  // ERROR 1146 (42S02): Table 'test.t1xxx' doesn't exist
  const char *error_hex =
    // error packet
    "29000001ff7a042334325330325461626c652027746573742e74317878782720646f65736e2774206578697374"
    // ok packet
    "0700000200010003000000"
    ;

  int64_t error_str_len = strlen(error_hex) / 2;
  char error_str[1000];
  int ret = OB_SUCCESS;
  ret = ObMysqlTestUtils::covert_hex_to_string(error_hex, strlen(error_hex), error_str);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool is_checksum_on[2] = {true, false};
  for (int64_t k = 0; k < 2; ++k) {
    char compressed_buf[1000];
    uint8_t seq = 0;
    int64_t compress_packet_len = 0;
    ret = ObMysqlTestUtils::build_mysql_compress_packet(error_str, error_str_len, seq,
        compressed_buf, sizeof(compressed_buf), compress_packet_len, is_checksum_on[k]);
    ASSERT_EQ(OB_SUCCESS, ret);

    LOG_INFO("after build compress packet", "origin len", error_str_len, K(compress_packet_len),
        "is_checksum_on", is_checksum_on[k]);

    ObMysqlCompressAnalyzer analyzer;
    uint8_t request_req = 0;
    ret = analyzer.init(request_req, ObMysqlCompressAnalyzer::DECOMPRESS_MODE, OB_MYSQL_COM_QUERY, OCEANBASE_MYSQL_PROTOCOL_MODE, false, 0, 0, 0, false);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObMysqlResp resp;
    ObString resp_str(compress_packet_len, compressed_buf);
    ret = analyzer.analyze_compressed_response(resp_str, resp);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(is_checksum_on[k], analyzer.is_compressed_payload());
    ASSERT_TRUE(resp.get_analyze_result().is_resp_completed_);
    ASSERT_FALSE(resp.get_analyze_result().is_trans_completed_);
    ASSERT_TRUE(resp.get_analyze_result().is_error_resp());

    // stream analyze
    ObMysqlCompressAnalyzer analyzer2;
    ret = analyzer2.init(request_req, ObMysqlCompressAnalyzer::DECOMPRESS_MODE, OB_MYSQL_COM_QUERY, OCEANBASE_MYSQL_PROTOCOL_MODE, false, 0, 0, 0, false);
    ObMysqlResp resp2;
    ASSERT_EQ(OB_SUCCESS, ret);
    for (int64_t i = 0; i < compress_packet_len; ++i) {
      ObString resp_str(1, compressed_buf + i);
      ret = analyzer2.analyze_compressed_response(resp_str, resp2);
      ASSERT_EQ(OB_SUCCESS, ret);
      if (i == compress_packet_len - 1) {
        ASSERT_TRUE(resp2.get_analyze_result().is_resp_completed_);
      } else {
        ASSERT_FALSE(resp2.get_analyze_result().is_resp_completed_);
      }
    }
    ASSERT_EQ(is_checksum_on[k], analyzer.is_compressed_payload());
    ASSERT_TRUE(resp2.get_analyze_result().is_resp_completed_);
    ASSERT_FALSE(resp2.get_analyze_result().is_trans_completed_);
    ASSERT_TRUE(resp2.get_analyze_result().is_error_resp());
    ASSERT_EQ(1146, resp2.get_analyze_result().get_error_code());
  }
};

TEST_F(TestMysqlCompressAnalyzer, test_comressed_OB_MYSQL_COM_STATISTICS)
{
  // OB_MYSQL_COM_STATISTICS response String EOF packet and OK packet;
  const char *packet_hex = "1a00000141637469766520746872656164732"
                           "06e6f7420737570706f7274080000020000002"
                           "200000000";

  int64_t packet_str_len = strlen(packet_hex) / 2;
  char packet_str[1000];
  int ret = OB_SUCCESS;
  ret = ObMysqlTestUtils::covert_hex_to_string(packet_hex, strlen(packet_hex), packet_str);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool is_checksum_on[2] = {true, false};
  for (int64_t k = 0; k < 2; ++k) {
    char compressed_buf[1000];
    uint8_t seq = 0;
    int64_t compress_packet_len = 0;
    ret = ObMysqlTestUtils::build_mysql_compress_packet(packet_str, packet_str_len, seq,
        compressed_buf, sizeof(compressed_buf), compress_packet_len, is_checksum_on[k]);
    ASSERT_EQ(OB_SUCCESS, ret);

    LOG_INFO("after build compress packet", "origin len", packet_str_len, K(compress_packet_len),
        "is_checksum_on", is_checksum_on[k]);

    ObMysqlCompressAnalyzer analyzer;
    uint8_t request_req = 0;
    ret = analyzer.init(request_req, ObMysqlCompressAnalyzer::DECOMPRESS_MODE, OB_MYSQL_COM_STATISTICS, OCEANBASE_MYSQL_PROTOCOL_MODE, true, 0, 0, 0, false);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObMysqlResp resp;
    ObString resp_str(compress_packet_len, compressed_buf);
    ret = analyzer.analyze_compressed_response(resp_str, resp);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(is_checksum_on[k], analyzer.is_compressed_payload());
    ASSERT_TRUE(resp.get_analyze_result().is_resp_completed_);
    ASSERT_TRUE(resp.get_analyze_result().is_trans_completed_);
    ASSERT_FALSE(resp.get_analyze_result().is_error_resp());

    // stream analyze
    ObMysqlCompressAnalyzer analyzer2;
    ret = analyzer2.init(request_req, ObMysqlCompressAnalyzer::DECOMPRESS_MODE, OB_MYSQL_COM_STATISTICS, OCEANBASE_MYSQL_PROTOCOL_MODE, true, 0, 0, 0, false);
    ObMysqlResp resp2;
    ASSERT_EQ(OB_SUCCESS, ret);
    for (int64_t i = 0; i < compress_packet_len; ++i) {
      ObString resp_str(1, compressed_buf + i);
      ret = analyzer2.analyze_compressed_response(resp_str, resp2);
      ASSERT_EQ(OB_SUCCESS, ret);
      if (i == compress_packet_len - 1) {
        ASSERT_TRUE(resp2.get_analyze_result().is_resp_completed_);
      } else {
        ASSERT_FALSE(resp2.get_analyze_result().is_resp_completed_);
      }
    }
    ASSERT_EQ(is_checksum_on[k], analyzer.is_compressed_payload());
    ASSERT_TRUE(resp2.get_analyze_result().is_resp_completed_);
    ASSERT_TRUE(resp2.get_analyze_result().is_trans_completed_);
    ASSERT_FALSE(resp2.get_analyze_result().is_error_resp());
  }
};

TEST_F(TestMysqlCompressAnalyzer, test_comressed_OB_MYSQL_COM_STATISTICS2)
{
  // OB_MYSQL_COM_STATISTICS response String EOF packet(without body, only has header) and OK packet;
  const char *packet_hex = "00000000080000020000002"
                           "200000000";

  int64_t packet_str_len = strlen(packet_hex) / 2;
  char packet_str[1000];
  int ret = OB_SUCCESS;
  ret = ObMysqlTestUtils::covert_hex_to_string(packet_hex, strlen(packet_hex), packet_str);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool is_checksum_on[2] = {true, false};
  for (int64_t k = 0; k < 2; ++k) {
    char compressed_buf[1000];
    uint8_t seq = 0;
    int64_t compress_packet_len = 0;
    ret = ObMysqlTestUtils::build_mysql_compress_packet(packet_str, packet_str_len, seq,
        compressed_buf, sizeof(compressed_buf), compress_packet_len, is_checksum_on[k]);
    ASSERT_EQ(OB_SUCCESS, ret);

    LOG_INFO("after build compress packet", "origin len", packet_str_len, K(compress_packet_len),
        "is_checksum_on", is_checksum_on[k]);

    ObMysqlCompressAnalyzer analyzer;
    uint8_t request_req = 0;
    ret = analyzer.init(request_req, ObMysqlCompressAnalyzer::DECOMPRESS_MODE, OB_MYSQL_COM_STATISTICS, OCEANBASE_MYSQL_PROTOCOL_MODE, true, 0, 0, 0, false);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObMysqlResp resp;
    ObString resp_str(compress_packet_len, compressed_buf);
    ret = analyzer.analyze_compressed_response(resp_str, resp);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(is_checksum_on[k], analyzer.is_compressed_payload());
    ASSERT_TRUE(resp.get_analyze_result().is_resp_completed_);
    ASSERT_TRUE(resp.get_analyze_result().is_trans_completed_);
    ASSERT_FALSE(resp.get_analyze_result().is_error_resp());

    // stream analyze
    ObMysqlCompressAnalyzer analyzer2;
    ret = analyzer2.init(request_req, ObMysqlCompressAnalyzer::DECOMPRESS_MODE, OB_MYSQL_COM_STATISTICS, OCEANBASE_MYSQL_PROTOCOL_MODE, true, 0, 0, 0, false);
    ObMysqlResp resp2;
    ASSERT_EQ(OB_SUCCESS, ret);
    for (int64_t i = 0; i < compress_packet_len; ++i) {
      ObString resp_str(1, compressed_buf + i);
      ret = analyzer2.analyze_compressed_response(resp_str, resp2);
      ASSERT_EQ(OB_SUCCESS, ret);
      if (i == compress_packet_len - 1) {
        ASSERT_TRUE(resp2.get_analyze_result().is_resp_completed_);
      } else {
        ASSERT_FALSE(resp2.get_analyze_result().is_resp_completed_);
      }
    }
    ASSERT_EQ(is_checksum_on[k], analyzer.is_compressed_payload());
    ASSERT_TRUE(resp2.get_analyze_result().is_resp_completed_);
    ASSERT_TRUE(resp2.get_analyze_result().is_trans_completed_);
    ASSERT_FALSE(resp2.get_analyze_result().is_error_resp());
  }
};

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
