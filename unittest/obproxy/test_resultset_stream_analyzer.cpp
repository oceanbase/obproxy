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
#include "obproxy/proxy/mysqllib/ob_resultset_stream_analyzer.h"
#include "obproxy/iocore/eventsystem/ob_io_buffer.h"
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;

static const int64_t DEFAULT_PKT_LEN = 1024;

static int64_t const MYSQL_BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K);

class TestResultsetStreamAnalyzer : public ::testing::Test
{
public:
   int covert_hex_to_string(const char *hex_str, int64_t len, char *str);
};

int TestResultsetStreamAnalyzer::covert_hex_to_string(const char *hex_str, int64_t len, char *str)
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

TEST_F(TestResultsetStreamAnalyzer, test_entire)
{
  int ret = OB_SUCCESS;
  ObResultsetStreamAnalyzer rs_analyzer;
  ObMIOBuffer *write_buf = new_miobuffer(MYSQL_BUFFER_SIZE);
  ObIOBufferReader *reader = write_buf->alloc_reader();
  ASSERT_TRUE(NULL != write_buf);

  char resp[DEFAULT_PKT_LEN];
  //mysql> select * from t2;
  //  +---+------+
  //  | a | b    |
  //  +---+------+
  //  | 1 |    1 |
  //  | 2 |    1 |
  //  +---+------+
  // add an ok packet in the end
  const char *hex = "0100000102200000020364656604746573740274320"
                    "27432016101610c3f000b0000000303500000002000"
                    "0003036465660474657374027432027432016201620"
                    "c3f000b00000003000000000005000004fe00002200"
                    "0400000501310131040000060132013105000007fe0"
                    "00022000700000100000002000000";
  ret = covert_hex_to_string(hex, strlen(hex), resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t written_size = 0;
  write_buf->write(resp, strlen(hex)/2, written_size);
  ASSERT_EQ(written_size, strlen(hex)/2);
  int64_t to_consume_size = 0;
  ObResultsetStreamStatus status = RSS_CONTINUE_STATUS;

  ret = rs_analyzer.analyze_resultset_stream(NULL, to_consume_size, status);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = rs_analyzer.analyze_resultset_stream(reader, to_consume_size, status);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(RSS_END_NORMAL_STATUS, status);
  ASSERT_EQ(111,  to_consume_size);

  if (NULL != write_buf) {
    free_miobuffer(write_buf);
  }
};

TEST_F(TestResultsetStreamAnalyzer, test_separate)
{
  int ret = OB_SUCCESS;
  ObResultsetStreamAnalyzer rs_analyzer;
  ObMIOBuffer *write_buf = new_miobuffer(MYSQL_BUFFER_SIZE);
  ObIOBufferReader *reader = write_buf->alloc_reader();
  ASSERT_TRUE(NULL != write_buf);

  char resp[DEFAULT_PKT_LEN];
  //mysql> select * from t2;
  //  +---+------+
  //  | a | b    |
  //  +---+------+
  //  | 1 |    1 |
  //  | 2 |    1 |
  //  +---+------+
  // add an ok packet in the end
  const char *hex = "0100000102200000020364656604746573740274320"
                    "27432016101610c3f000b0000000303500000002000"
                    "0003036465660474657374027432027432016201620"
                    "c3f000b00000003000000000005000004fe00002200"
                    "0400000501310131040000060132013105000007fe0"
                    "000";
  ret = covert_hex_to_string(hex, strlen(hex), resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t written_size = 0;
  write_buf->write(resp, strlen(hex)/2, written_size);
  ASSERT_EQ(written_size, strlen(hex)/2);
  int64_t to_consume_size = 0;
  ObResultsetStreamStatus status = RSS_CONTINUE_STATUS;

  ret = rs_analyzer.analyze_resultset_stream(reader, to_consume_size, status);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(RSS_CONTINUE_STATUS, status);
  ASSERT_EQ(102,  to_consume_size);
  reader->consume(102);

  const char *hex2 = "22000700000100000002000000";
  ret = covert_hex_to_string(hex2, strlen(hex2), resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  write_buf->write(resp, strlen(hex2)/2, written_size);
  ASSERT_EQ(written_size, strlen(hex2)/2);
  ret = rs_analyzer.analyze_resultset_stream(reader, to_consume_size, status);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(RSS_END_NORMAL_STATUS, status);
  ASSERT_EQ(9,  to_consume_size);

  if (NULL != write_buf) {
    free_miobuffer(write_buf);
  }
};

TEST_F(TestResultsetStreamAnalyzer, test_separate2)
{
  int ret = OB_SUCCESS;
  ObResultsetStreamAnalyzer rs_analyzer;
  ObMIOBuffer *write_buf = new_miobuffer(MYSQL_BUFFER_SIZE);
  ObIOBufferReader *reader = write_buf->alloc_reader();
  ASSERT_TRUE(NULL != write_buf);

  char resp[DEFAULT_PKT_LEN];
  //mysql> select * from t2;
  //  +---+------+
  //  | a | b    |
  //  +---+------+
  //  | 1 |    1 |
  //  | 2 |    1 |
  //  +---+------+
  // add an ok packet in the end
  const char *hex = "0100000102200000020364656604746573740274320"
                    "27432016101610c3f000b0000000303500000002000"
                    "0003036465660474657374027432027432016201620"
                    "c3f000b00000003000000000005000004fe00002200"
                    "04000005013101310400"
                    "";
  ret = covert_hex_to_string(hex, strlen(hex), resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t written_size = 0;
  write_buf->write(resp, strlen(hex)/2, written_size);
  ASSERT_EQ(written_size, strlen(hex)/2);
  int64_t to_consume_size = 0;
  ObResultsetStreamStatus status = RSS_CONTINUE_STATUS;

  ret = rs_analyzer.analyze_resultset_stream(reader, to_consume_size, status);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(RSS_CONTINUE_STATUS, status);
  ASSERT_EQ(94,  to_consume_size);
  reader->consume(94);

  const char *hex2 = "00060132013105000007fe000022000700000100000002000000";
  ret = covert_hex_to_string(hex2, strlen(hex2), resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  write_buf->write(resp, strlen(hex2)/2, written_size);
  ASSERT_EQ(written_size, strlen(hex2)/2);
  ret = rs_analyzer.analyze_resultset_stream(reader, to_consume_size, status);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(RSS_END_NORMAL_STATUS, status);
  ASSERT_EQ(17,  to_consume_size);

  if (NULL != write_buf) {
    free_miobuffer(write_buf);
  }
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

