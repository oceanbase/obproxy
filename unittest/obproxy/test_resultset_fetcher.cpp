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
#include "obproxy/proxy/mysqllib/ob_resultset_fetcher.h"
#include "obproxy/iocore/eventsystem/ob_io_buffer.h"
namespace oceanbase
{
namespace obproxy
{
using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;

static const int64_t DEFAULT_PKT_LEN = 1024;

static int64_t const MYSQL_BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K);

class TestResultsetFetcher : public ::testing::Test
{
public:
   int covert_hex_to_string(const char *hex_str, int64_t len, char *str);
   bool is_double_equal(const double d1, const double d2);
};

int TestResultsetFetcher::covert_hex_to_string(const char *hex_str, int64_t len, char *str)
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

bool TestResultsetFetcher::is_double_equal(const double d1, const double d2)
{
  bool bret = false;
  if (d1 - d2 > -0.0001 && d1 - d2 < 0.0001) {
    bret= true;
  }
  return bret;
}

TEST_F(TestResultsetFetcher, test_simple)
{
  int ret = OB_SUCCESS;
  ObResultSetFetcher fetcher;
  ObMIOBuffer *write_buf = new_miobuffer(MYSQL_BUFFER_SIZE);
  ObIOBufferReader *reader = write_buf->alloc_reader();
  ASSERT_TRUE(NULL != write_buf);

  char resp[DEFAULT_PKT_LEN];
  // mysql> select * from t1;
  //  +----+----------+
  //  | c1 | c2       |
  //  +----+----------+
  //  |  1 | lianluzu |
  //  |  2 | lianluz2 |
  //  +----+----------+
  //
  const char *hex = "01000001022200000203646566047465737402743"
                    "10274310263310263310c3f000b00000003035000"
                    "000022000003036465660474657374027431027431"
                    "0263320263320c210080010000fd00000000000500"
                    "0004fe000022000b0000050131086c69616e6c757a"
                    "750b0000060132086c69616e6c757a3205000007fe"
                    "00002200";
  ret = covert_hex_to_string(hex, strlen(hex), resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t written_size = 0;
  write_buf->write(resp, strlen(hex)/2, written_size);
  ASSERT_EQ(written_size, strlen(hex)/2);

  ret = fetcher.init(NULL);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = fetcher.init(reader);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, fetcher.get_column_count());

  ret = fetcher.next();
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t c1_value = 0;
  ret = fetcher.get_int("c1", c1_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, c1_value);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObString c2_value;
  ret = fetcher.get_varchar("c2", c2_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  char tmp_str[128];
  snprintf(tmp_str, 128, "%.*s", c2_value.length(), c2_value.ptr());
  ASSERT_STREQ("lianluzu", tmp_str);

  ret = fetcher.next();
  ASSERT_EQ(OB_SUCCESS, ret);
  c1_value = 0;
  ret = fetcher.get_int("c1", c1_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, c1_value);

  ASSERT_EQ(OB_SUCCESS, ret);
  c2_value.reset();
  ret = fetcher.get_varchar("c2", c2_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  snprintf(tmp_str, 128, "%.*s", c2_value.length(), c2_value.ptr());
  ASSERT_STREQ("lianluz2", tmp_str);

  ret = fetcher.next();
  ASSERT_EQ(OB_ITER_END, ret);

  if (NULL != write_buf) {
    free_miobuffer(write_buf);
  }
};

TEST_F(TestResultsetFetcher, test_all_data_type)
{
  int ret = OB_SUCCESS;
  ObResultSetFetcher fetcher;
  ObMIOBuffer *write_buf = new_miobuffer(MYSQL_BUFFER_SIZE);
  ObIOBufferReader *reader = write_buf->alloc_reader();
  ASSERT_TRUE(NULL != write_buf);

  char resp[DEFAULT_PKT_LEN];
  // mysql> desc t10;
  //  +-------+--------------+------+-----+---------+-------+
  //  | Field | Type         | Null | Key | Default | Extra |
  //  +-------+--------------+------+-----+---------+-------+
  //  | c1    | int(11)      | NO   | PRI | NULL    |       |
  //  | c2    | varchar(128) | YES  |     | NULL    |       |
  //  | c3    | tinyint(1)   | YES  |     | NULL    |       |
  //  | c4    | double       | YES  |     | NULL    |       |
  //  +-------+--------------+------+-----+---------+-------+

  // mysql> select * from t10;
  //  +----+----------+------+------+
  //  | c1 | c2       | c3   | c4   |
  //  +----+----------+------+------+
  //  |  1 | lianluzu |    0 |  1.1 |
  //  |  2 |          | NULL |  2.2 |
  //  +----+----------+------+------+
  //
  const char *hex = "01000001042400000203646566047465737403743130"
                    "037431300263310263310c3f000b00000003035000000"
                    "024000003036465660474657374037431300374313002"
                    "63320263320c210080010000fd0000000000240000040"
                    "3646566047465737403743130037431300263330263330"
                    "c3f0001000000010000000000240000050364656604746"
                    "5737403743130037431300263340263340c3f001600000"
                    "00500001f000005000006fe00002200110000070131086"
                    "c69616e6c757a75013003312e3108000008013200fb0332"
                    "2e3205000009fe00002200";
  ret = covert_hex_to_string(hex, strlen(hex), resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t written_size = 0;
  write_buf->write(resp, strlen(hex)/2, written_size);
  ASSERT_EQ(written_size, strlen(hex)/2);

  ret = fetcher.init(reader);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, fetcher.get_column_count());

  ret = fetcher.next();
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t c1_value = 0;
  ret = fetcher.get_int("c1", c1_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, c1_value);

  ObString c2_value;
  ret = fetcher.get_varchar("c2", c2_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  char tmp_str[128];
  snprintf(tmp_str, 128, "%.*s", c2_value.length(), c2_value.ptr());
  ASSERT_STREQ("lianluzu", tmp_str);

  bool c3_value;
  ret = fetcher.get_bool("c3", c3_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(c3_value);

  double c4_value;
  ret = fetcher.get_double("c4", c4_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_double_equal(1.1, c4_value));


  ret = fetcher.next();
  ASSERT_EQ(OB_SUCCESS, ret);
  c1_value = 0;
  ret = fetcher.get_int("c1", c1_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, c1_value);

  ASSERT_EQ(OB_SUCCESS, ret);
  c2_value.reset();
  ret = fetcher.get_varchar("c2", c2_value);
  ASSERT_EQ(OB_SUCCESS, ret);

  c3_value = false;
  ret = fetcher.get_bool("c3", c3_value);
  ASSERT_EQ(OB_INVALID_DATA, ret);

  c4_value = 0.0;
  ret = fetcher.get_double("c4", c4_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_double_equal(2.2, c4_value));

  ret = fetcher.next();
  ASSERT_EQ(OB_ITER_END, ret);

  c4_value = 0.0;
  ret = fetcher.get_double("c4", c4_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_double_equal(2.2, c4_value));

  if (NULL != write_buf) {
    free_miobuffer(write_buf);
  }
};


TEST_F(TestResultsetFetcher, test_empty_resultset)
{
  int ret = OB_SUCCESS;
  ObResultSetFetcher fetcher;
  ObMIOBuffer *write_buf = new_miobuffer(MYSQL_BUFFER_SIZE);
  ObIOBufferReader *reader = write_buf->alloc_reader();
  ASSERT_TRUE(NULL != write_buf);

  char resp[DEFAULT_PKT_LEN];

  // mysql> select * from t1;
  // Empty set (0.01 sec)
  //
  const char *hex = "01000001022200000203646566047465737402743"
                    "10274310263310263310c3f000b00000003035000"
                    "00002200000303646566047465737402743102743"
                    "10263320263320c210080010000fd000000000005"
                    "000004fe0000220005000005fe00002200";

  ret = covert_hex_to_string(hex, strlen(hex), resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t written_size = 0;
  write_buf->write(resp, strlen(hex)/2, written_size);
  ASSERT_EQ(written_size, strlen(hex)/2);

  ret = fetcher.init(NULL);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = fetcher.init(reader);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, fetcher.get_column_count());

  ret = fetcher.next();
  ASSERT_EQ(OB_ITER_END, ret);

  int64_t c1_value = 0;
  ret = fetcher.get_int("c1", c1_value);
  ASSERT_EQ(OB_INVALID_DATA, ret);

  if (NULL != write_buf) {
    free_miobuffer(write_buf);
  }
};

TEST_F(TestResultsetFetcher, test_multi_buffer_block)
{
  int ret = OB_SUCCESS;
  ObResultSetFetcher fetcher;
  ObMIOBuffer *write_buf = new_miobuffer(MYSQL_BUFFER_SIZE);
  ObIOBufferReader *reader = write_buf->alloc_reader();
  ASSERT_TRUE(NULL != write_buf);

  char resp[DEFAULT_PKT_LEN];
  // mysql> select * from t1;
  //  +----+----------+
  //  | c1 | c2       |
  //  +----+----------+
  //  |  1 | lianluzu |
  //  |  2 | lianluz2 |
  //  +----+----------+
  //
  const char *hex = "01000001022200000203646566047465737402743"
                    "10274310263310263310c3f000b00000003035000"
                    "000022000003036465660474657374027431027431"
                    "0263320263320c210080010000fd00000000000500"
                    "0004fe000022000b0000050131086c69616e6c757a"
                    "750b0000060132086c69616e6c757a3205000007fe"
                    "00002200";
  ret = covert_hex_to_string(hex, strlen(hex), resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t extra_size = 8152;
  int64_t written_size = 0;
  for (int64_t i = 0; i < extra_size; ++i) {
    write_buf->write("U", 1, written_size);
  }

  reader->start_offset_ = extra_size;
  write_buf->write(resp, strlen(hex)/2, written_size);
  ASSERT_EQ(written_size, strlen(hex)/2);

  ret = fetcher.init(NULL);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = fetcher.init(reader);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, fetcher.get_column_count());

  ret = fetcher.next();
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t c1_value = 0;
  ret = fetcher.get_int("c1", c1_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, c1_value);

  ASSERT_EQ(OB_SUCCESS, ret);
  ObString c2_value;
  ret = fetcher.get_varchar("c2", c2_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  char tmp_str[128];
  snprintf(tmp_str, 128, "%.*s", c2_value.length(), c2_value.ptr());
  ASSERT_STREQ("lianluzu", tmp_str);

  ret = fetcher.next();
  ASSERT_EQ(OB_SUCCESS, ret);
  c1_value = 0;
  ret = fetcher.get_int("c1", c1_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, c1_value);

  ASSERT_EQ(OB_SUCCESS, ret);
  c2_value.reset();
  ret = fetcher.get_varchar("c2", c2_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  snprintf(tmp_str, 128, "%.*s", c2_value.length(), c2_value.ptr());
  ASSERT_STREQ("lianluz2", tmp_str);

  ret = fetcher.next();
  ASSERT_EQ(OB_ITER_END, ret);

  if (NULL != write_buf) {
    free_miobuffer(write_buf);
  }
};

TEST_F(TestResultsetFetcher, test_resultset_first_error)
{
  int ret = OB_SUCCESS;
  ObResultSetFetcher fetcher;
  ObMIOBuffer *write_buf = new_miobuffer(MYSQL_BUFFER_SIZE);
  ObIOBufferReader *reader = write_buf->alloc_reader();
  ASSERT_TRUE(NULL != write_buf);

  char resp[DEFAULT_PKT_LEN];
  // mysql> select * from t1;
  // ERROR 1046 (3D000): No database selected
  //
  const char *hex = "1d000001ff16042333443030304e6f2064617461626173652073656c6563746564";
  ret = covert_hex_to_string(hex, strlen(hex), resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t written_size = 0;
  write_buf->write(resp, strlen(hex)/2, written_size);
  ASSERT_EQ(written_size, strlen(hex)/2);

  ret = fetcher.init(reader);
  ASSERT_EQ(1046, ret);

  if (NULL != write_buf) {
    free_miobuffer(write_buf);
  }
};

TEST_F(TestResultsetFetcher, test_resultset_first_ok)
{
  int ret = OB_SUCCESS;
  ObResultSetFetcher fetcher;
  ObMIOBuffer *write_buf = new_miobuffer(MYSQL_BUFFER_SIZE);
  ObIOBufferReader *reader = write_buf->alloc_reader();
  ASSERT_TRUE(NULL != write_buf);

  char resp[DEFAULT_PKT_LEN];
  // mysql> select * from t1;
  // ERROR 1046 (3D000): No database selected
  //
  const char *hex = "0700000100000002000000";
  ret = covert_hex_to_string(hex, strlen(hex), resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t written_size = 0;
  write_buf->write(resp, strlen(hex)/2, written_size);
  ASSERT_EQ(written_size, strlen(hex)/2);

  ret = fetcher.init(reader);
  ASSERT_EQ(OB_INVALID_DATA, ret);

  if (NULL != write_buf) {
    free_miobuffer(write_buf);
  }
};

TEST_F(TestResultsetFetcher, test_resultset_error)
{
  int ret = OB_SUCCESS;
  ObResultSetFetcher fetcher;
  ObMIOBuffer *write_buf = new_miobuffer(MYSQL_BUFFER_SIZE);
  ObIOBufferReader *reader = write_buf->alloc_reader();
  ASSERT_TRUE(NULL != write_buf);

  char resp[DEFAULT_PKT_LEN];
  // mysql> select * from t1;
  //  +----+----------+
  //  | c1 | c2       |
  //  +----+----------+
  //  |  1 | lianluzu |
  //  |  2 | lianluz2 |
  //  +----+----------+
  // error packet followed by mannul (ERROR 1046 (3D000): No database selected)
  const char *hex = "01000001022200000203646566047465737402743"
                    "10274310263310263310c3f000b00000003035000"
                    "000022000003036465660474657374027431027431"
                    "0263320263320c210080010000fd00000000000500"
                    "0004fe000022000b0000050131086c69616e6c757a"
                    "750b0000060132086c69616e6c757a321d000007ff"
                    "16042333443030304e6f2064617461626173652073"
                    "656c6563746564";
  ret = covert_hex_to_string(hex, strlen(hex), resp);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t written_size = 0;
  write_buf->write(resp, strlen(hex)/2, written_size);
  ASSERT_EQ(written_size, strlen(hex)/2);

  ret = fetcher.init(reader);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = fetcher.next();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = fetcher.next();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = fetcher.next();
  ASSERT_EQ(1046, ret);

  if (NULL != write_buf) {
    free_miobuffer(write_buf);
  }
};

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

