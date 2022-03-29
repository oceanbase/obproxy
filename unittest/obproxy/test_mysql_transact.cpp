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
#define protected public
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include "lib/utility/utility.h"
#include "obproxy/proxy/mysql/ob_mysql_transact.h"

using namespace oceanbase::common;
using namespace std;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class TestMysqlTransact : public ::testing::Test
{
public:
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
};

bool is_for_update_sql2(ObString src_sql)
{
  const ObString FOR_STRING("for");
  const ObString UPDATE_STRING("update");
  bool ret_bool = false;
  bool found_for = false;
  src_sql = trim_tailer_space(src_sql);
  common::ObString sql = src_sql;
  while (!sql.empty() && !ret_bool) {
    found_for = false;
    sql = trim_header_space(sql);
    if (sql.prefix_case_match(FOR_STRING)
        && src_sql.ptr() != sql.ptr()
        && IS_SPACE(*(sql.ptr() - 1))) {
      sql.assign_ptr(sql.ptr() + FOR_STRING.length(), sql.length() - FOR_STRING.length());
      if (!sql.empty() && IS_SPACE(*(sql.ptr() + 0))) {
        sql = trim_header_space(sql);
        found_for = true;
      }
    } else {
      ++sql;
    }

    if (found_for) {
      if (sql.prefix_case_match(UPDATE_STRING)) {
        sql.assign_ptr(sql.ptr() + UPDATE_STRING.length(), sql.length() - UPDATE_STRING.length());
        if (sql.empty() || IS_SPACE(*(sql.ptr() + 0)) || ';' == *(sql.ptr() + 0)) {
          ret_bool = true;
        }
      }
    }
  }
  return ret_bool;
}

TEST_F(TestMysqlTransact, is_for_update_sql)
{
  const char ok_buf[7][100] = {
      "select * from t1 where t1=2 for update",
      "select * from t1 where t1=2 FOR UPDATE",
      "select * from t1 where t1=2 FOR UPDATE;",
      "select * from t1 where t1=2       FOR             UPDATE;",
      "select FOR UPDATE;",
      "select * from t1 where t1=2 FOR UPDATE2 FOR UPDATE",
      "select * from t1 where t1=2 FOR UPDATE; FOR UPDATE2"
  };

  const char error_buf[12][100] = {
      "select 1;",
      "FOR UPDATE;",
      "select * from t1 where t1=2for update",
      "select * from t1 where t1=2 FOR UPDATE union select 1",
      "select * from t1 where t1=2 FOR UPDATE FOR UPDATE2",
      "select * from t1 where t1=2 for",
      "select * from t1 where t1=2 FORUPDATE",
      "select * from t1 where t1=2 FOR2 UPDATE;",
      "select * from t1 where t1=2 FOR UPDATE2;",
      "select * from (select * from t1 where t1=2 FOR UPDATE)",
      "select * from t1 where t1=2 FOR FOR UPDATE2;",
      "select * from t1 where t1=2 FOR UPDATE2 UPDATE;"
  };

  ObHRTime t1 = get_hrtime_internal();
  for (int64_t j = 0; j < 1000; j++) {
    for (int64_t i = 0; i < 7; i++) {
      is_for_update_sql2(ObString::make_string(ok_buf[i]));
    }
    for (int64_t i = 0; i < 12; i++) {
      is_for_update_sql2(ObString::make_string(error_buf[i]));
    }
  }
  ObHRTime t2 = get_hrtime_internal();

  ObHRTime t3 = get_hrtime_internal();
  for (int64_t j = 0; j < 1000; j++) {
    for (int64_t i = 0; i < 7; i++) {
      ASSERT_TRUE(ObMysqlTransact::ObTransState::is_for_update_sql(ObString::make_string(ok_buf[i])));
    }
    for (int64_t i = 0; i < 12; i++) {
      ASSERT_TRUE(!ObMysqlTransact::ObTransState::is_for_update_sql(ObString::make_string(error_buf[i])));
    }
  }
  ObHRTime t4 = get_hrtime_internal();
  LOG_DEBUG("debug ", "d1", (t2-t1)/1000, "d2", (t4-t3)/1000);

}

TEST_F(TestMysqlTransact, str_replace)
{
  const char *input[7] = {
      "select * from t1 where t1=2 for update",
      "select * from t1 where t1=:0 for update",
      ":0select * from t1 where t1= for update:0",
      "select :0:0:0:0:0 from t1 where t1=2 for update",
      ":0select :0:0:0:0:0 from t1 where t1=2 for update:0",
      "select :0:1:2:0:3 from t1 where t1=2 FOR UPDATE2 UPDATE;",
      "select :0:1:0:2:0:3 from t1 where t1=2 FOR UPDATE2 UPDATE;"
  };
  const char *expect_output[7] = {
      "select * from t1 where t1=2 for update",
      "select * from t1 where t1=ABCD for update",
      "ABCDselect * from t1 where t1= for updateABCD",
      "select ABCDABCDABCDABCDABCD from t1 where t1=2 for update",
      "ABCDselect ABCDABCDABCDABCDABCD from t1 where t1=2 for updateABCD",
      "select ABCD:1:2ABCD:3 from t1 where t1=2 FOR UPDATE2 UPDATE;",
      "select :1:2:3 from t1 where t1=2 FOR UPDATE2 UPDATE;"
  };
  char output[10][100] = {};
  const char *target_key = ":0";
  ObString target_value("ABCD");
  int32_t output_pos = 0;
  int ret = OB_SUCCESS;
  for (int i = 0; i < 6; ++i) {
    output_pos = 0;
    ret = str_replace(const_cast<char *>(input[i]), static_cast<int32_t>(strlen(input[i])), output[i], 100, target_key, static_cast<int32_t>(strlen(target_key)), target_value, output_pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(output_pos, strlen(expect_output[i]));
    ASSERT_TRUE(0 == memcmp(expect_output[i], output[i], output_pos));
  }

  output_pos = 0;
  ret = str_replace(const_cast<char *>(input[4]), static_cast<int32_t>(strlen(input[4])), output[4], 10, target_key, static_cast<int32_t>(strlen(target_key)), target_value, output_pos);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);

  output_pos = 90;
  ret = str_replace(const_cast<char *>(input[4]), static_cast<int32_t>(strlen(input[4])), output[4], 100, target_key, static_cast<int32_t>(strlen(target_key)), target_value, output_pos);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);

  output_pos = 100;
  ret = str_replace(const_cast<char *>(input[4]), static_cast<int32_t>(strlen(input[4])), output[4], 100, target_key, static_cast<int32_t>(strlen(target_key)), target_value, output_pos);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  output_pos = 0;
  ObString target_value2("ABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCD");
  ret = str_replace(const_cast<char *>(input[4]), static_cast<int32_t>(strlen(input[4])), output[4], 100, target_key, static_cast<int32_t>(strlen(target_key)), target_value2, output_pos);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);

  output_pos = 0;
  ObString target_value3("");
  ret = str_replace(const_cast<char *>(input[6]), static_cast<int32_t>(strlen(input[6])), output[6], 100, target_key, static_cast<int32_t>(strlen(target_key)), target_value3, output_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(output_pos, strlen(expect_output[6]));
  ASSERT_TRUE(0 == memcmp(expect_output[6], output[6], output_pos));
}


TEST(utility, ob_localtime)
{
  struct tm std_tm;
  struct tm ob_tm;

  time_t last_sec = 0;
  struct tm last_tm;
  struct timeval tv;
  (void)gettimeofday(&tv, NULL);

  //check next five year, need 180s
  const int64_t min_time = static_cast<int64_t>(tv.tv_sec);
  const int64_t max_time = min_time + 5 * 365 * 24 * 60 * 60;
  for (int64_t i = min_time; i < max_time; i += 1) {
    ::localtime_r((const time_t *)&i, &std_tm);
    ob_localtime((const time_t *)&i, &ob_tm);

//    _COMMON_LOG(WARN, "check time, std_tm=%d %d-%d %d:%d:%d, ob_tm=%d %d-%d %d:%d:%d, value=%ld",
//        std_tm.tm_year, std_tm.tm_mon, std_tm.tm_mday, std_tm.tm_hour, std_tm.tm_min, std_tm.tm_sec,
//        ob_tm.tm_year, ob_tm.tm_mon, ob_tm.tm_mday, ob_tm.tm_hour, ob_tm.tm_min, ob_tm.tm_sec,
//        i);

    EXPECT_EQ(std_tm.tm_sec, ob_tm.tm_sec);
    EXPECT_EQ(std_tm.tm_min, ob_tm.tm_min);
    EXPECT_EQ(std_tm.tm_hour, ob_tm.tm_hour);
    EXPECT_EQ(std_tm.tm_mday, ob_tm.tm_mday);
    EXPECT_EQ(std_tm.tm_mon, ob_tm.tm_mon);
    EXPECT_EQ(std_tm.tm_year, ob_tm.tm_year);

    ob_fast_localtime(last_sec, last_tm, (const time_t)i, &std_tm);
    EXPECT_EQ(i, last_sec);
    EXPECT_EQ(std_tm.tm_sec, last_tm.tm_sec);
    EXPECT_EQ(std_tm.tm_min, last_tm.tm_min);
    EXPECT_EQ(std_tm.tm_hour, last_tm.tm_hour);
    EXPECT_EQ(std_tm.tm_mday, last_tm.tm_mday);
    EXPECT_EQ(std_tm.tm_mon, last_tm.tm_mon);
    EXPECT_EQ(std_tm.tm_year, last_tm.tm_year);
  }

  int64_t special_value[18] = {
    825523199,//1996.02.28 23.59.59
    825523200,//1996.02.29 00.00.00
    825609599,//1996.02.29 23.59.59
    825609600,//1996.03.01 00.00.00

    951753599,//2000.02.28 23.59.59
    951753600,//2000.02.29 00.00.00
    951839999,//2000.02.29 23.59.59
    951840000,//2000.03.01 00.00.00

    4107513599,//2100.02.28 23:59:59
    4107513600,//2100.03.01 00:00:00
    920217599,//1999.02.28 23.59.59
    920217600,//1999.03.01 00.00.00

    946655999,//1999.12.31 23.59.59
    946656000,//2000.01.01 0:0:0

    0,//1970.01.01 08:00:00
    -1,//1970.01.01 07:59:59
    -2203920344,//1900.02.28 23.59.59
    -2203920343//1900.03.01 00.00.00
  };

  for (int64_t i = 0; i < 14; ++i) {
    ::localtime_r((const time_t *)(special_value + i), &std_tm);
    ob_localtime((const time_t *)(special_value + i), &ob_tm);

    _COMMON_LOG(WARN, "check time, std_tm=%d %d-%d %d:%d:%d, ob_tm=%d %d-%d %d:%d:%d, value=%ld, __time=%ld",
        std_tm.tm_year, std_tm.tm_mon, std_tm.tm_mday, std_tm.tm_hour, std_tm.tm_min, std_tm.tm_sec,
        ob_tm.tm_year, ob_tm.tm_mon, ob_tm.tm_mday, ob_tm.tm_hour, ob_tm.tm_min, ob_tm.tm_sec,
        special_value[i], __timezone / 60);

    EXPECT_EQ(std_tm.tm_sec, ob_tm.tm_sec);
    EXPECT_EQ(std_tm.tm_min, ob_tm.tm_min);
    EXPECT_EQ(std_tm.tm_hour, ob_tm.tm_hour);
    EXPECT_EQ(std_tm.tm_mday, ob_tm.tm_mday);
    EXPECT_EQ(std_tm.tm_mon, ob_tm.tm_mon);
    EXPECT_EQ(std_tm.tm_year, ob_tm.tm_year);

    ob_fast_localtime(last_sec, last_tm, (const time_t)i, &std_tm);
    EXPECT_EQ(i, last_sec);
    EXPECT_EQ(std_tm.tm_sec, last_tm.tm_sec);
    EXPECT_EQ(std_tm.tm_min, last_tm.tm_min);
    EXPECT_EQ(std_tm.tm_hour, last_tm.tm_hour);
    EXPECT_EQ(std_tm.tm_mday, last_tm.tm_mday);
    EXPECT_EQ(std_tm.tm_mon, last_tm.tm_mon);
    EXPECT_EQ(std_tm.tm_year, last_tm.tm_year);
  }

  ob_tm.tm_sec = 0;
  ob_tm.tm_min = 0;
  ob_tm.tm_hour = 0;
  ob_tm.tm_mday = 0;
  ob_tm.tm_mon = 0;
  ob_tm.tm_year = 0;

  for (int64_t i = 14; i < 18; ++i) {
    ob_localtime((const time_t *)(special_value + i), &ob_tm);

    EXPECT_EQ(0, ob_tm.tm_sec);
    EXPECT_EQ(0, ob_tm.tm_min);
    EXPECT_EQ(0, ob_tm.tm_hour);
    EXPECT_EQ(0, ob_tm.tm_mday);
    EXPECT_EQ(0, ob_tm.tm_mon);
    EXPECT_EQ(0, ob_tm.tm_year);
  }
}

TEST(utility, ob_localtime_performance)
{
  struct tm ob_tm;

  const int64_t MAX_COUNT = 10000000;
  const int64_t WEIGHT = 1000000;
  struct timeval time_begin, time_end;

  gettimeofday(&time_begin, NULL);
  for (int64_t i = 1; i < MAX_COUNT; i += 1) {
    ::localtime_r((const time_t *)&i, &ob_tm);
  }
  gettimeofday(&time_end, NULL);
  OB_LOG(INFO, "performance average(ns)",
         "localtime_r", (WEIGHT * (time_end.tv_sec - time_begin.tv_sec) + time_end.tv_usec - time_begin.tv_usec) / (MAX_COUNT / 1000),
         "cost_sec", time_end.tv_sec - time_begin.tv_sec,
         "cost_us", time_end.tv_usec - time_begin.tv_usec);


  gettimeofday(&time_begin, NULL);
  for (int64_t i = 1; i < MAX_COUNT; i += 1) {
    ob_localtime((const time_t *)&i, &ob_tm);
  }
  gettimeofday(&time_end, NULL);
  OB_LOG(INFO, "performance average(ns)",
         "ob_localtime", (WEIGHT * (time_end.tv_sec - time_begin.tv_sec) + time_end.tv_usec - time_begin.tv_usec) / (MAX_COUNT / 1000),
         "cost_sec", time_end.tv_sec - time_begin.tv_sec,
         "cost_us", time_end.tv_usec - time_begin.tv_usec);
}

struct TestCheckStruct  {
  TestCheckStruct() : is_ob_localtime_(false), stop_(false) {}
  bool is_ob_localtime_;
  volatile bool stop_;
};

void* ob_localtime_pthread_op_func(void* arg)
{
  TestCheckStruct *tmp = static_cast<TestCheckStruct *>(arg);
  struct tm ob_tm;
//  OB_LOG(INFO, "thread begin");
  if (tmp->is_ob_localtime_) {
    for (int64_t i = 0; !tmp->stop_; i += 1) {
      ob_localtime((const time_t *)&i, &ob_tm);
    }
  } else {
    for (int64_t i = 0; !tmp->stop_; i += 1) {
      ::localtime_r((const time_t *)&i, &ob_tm);
    }
  }
//  OB_LOG(INFO, "thread end");
  return NULL;
}

TEST(utility, multi_thread_sys)
{
  const int64_t max_thread_count[] = {10, 100};
  for (int64_t j = 0; j < 2; ++j) {
    vector<pthread_t> pid_vector;
    struct TestCheckStruct tmp;
    tmp.is_ob_localtime_ = false;
    for (int64_t i = 0; i < max_thread_count[j]; ++i) {
      pthread_t pid = 0;
      pthread_create(&pid, NULL, ob_localtime_pthread_op_func, &tmp);
      pid_vector.push_back(pid);
    }

    sleep(2);

    OB_LOG(INFO, "after sleep");

    struct tm ob_tm;

    const int64_t MAX_COUNT = 1000000;
    const int64_t WEIGHT = 1000000;
    struct timeval time_begin, time_end;

    gettimeofday(&time_begin, NULL);
    for (int64_t i = 1; i < MAX_COUNT; i += 1) {
      ::localtime_r((const time_t *)&i, &ob_tm);
    }
    gettimeofday(&time_end, NULL);
    OB_LOG(INFO, "performance average(ns)",
           "localtime_r", (WEIGHT * (time_end.tv_sec - time_begin.tv_sec) + time_end.tv_usec - time_begin.tv_usec) / (MAX_COUNT / 1000),
           "thread_count", max_thread_count[j]);

    tmp.stop_ = true;
    for (int64_t i = 0; i < max_thread_count[j]; ++i) {
      pthread_join(pid_vector[i], NULL);
    }
  }
}

TEST(utility, multi_thread_ob)
{
  const int64_t max_thread_count[] = {10, 100};
  for (int64_t j = 0; j < 2; ++j) {
    vector<pthread_t> pid_vector;
    struct TestCheckStruct tmp;
    tmp.is_ob_localtime_ = true;
    for (int64_t i = 0; i < max_thread_count[j]; ++i) {
      pthread_t pid = 0;
      pthread_create(&pid, NULL, ob_localtime_pthread_op_func, &tmp);
      pid_vector.push_back(pid);
    }

    sleep(2);

    OB_LOG(INFO, "after sleep");

    struct tm ob_tm;

    const int64_t MAX_COUNT = 1000000;
    const int64_t WEIGHT = 1000000;
    struct timeval time_begin, time_end;

    gettimeofday(&time_begin, NULL);
    for (int64_t i = 1; i < MAX_COUNT; i += 1) {
      ob_localtime((const time_t *)&i, &ob_tm);
    }
    gettimeofday(&time_end, NULL);
    OB_LOG(INFO, "performance average(ns)",
           "ob_localtime", (WEIGHT * (time_end.tv_sec - time_begin.tv_sec) + time_end.tv_usec - time_begin.tv_usec) / (MAX_COUNT / 1000),
           "thread_count", max_thread_count[j]);

    tmp.stop_ = true;
    for (int64_t i = 0; i < max_thread_count[j]; ++i) {
      pthread_join(pid_vector[i], NULL);
    }
  }

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
