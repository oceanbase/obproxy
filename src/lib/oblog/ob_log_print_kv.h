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
 *
 * **************************************************************
 *
 * @brief Define template of logdata_print_obj and macro of DEFINE_LOG_PRINT_KV.
 * This file should be included in ob_log.h
 */

#ifndef OCEABASE_COMMON_OB_LOG_PRINT_KV_H_
#define OCEABASE_COMMON_OB_LOG_PRINT_KV_H_
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_template_utils.h"
#include "lib/alloc/alloc_assist.h"

#define LOG_N_TRUE "true"
#define LOG_N_FLASE "false"
namespace oceanbase
{
namespace common
{

#define COMMA_FORMAT ", "
#define WITH_COMMA(format)  (with_comma ? COMMA_FORMAT format: format)

/// @return OB_SUCCESS or OB_BUF_NOT_ENOUGH
int logdata_printf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt, ...)
__attribute__((format(printf, 4, 5)));
int logdata_vprintf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt, va_list args);

#define LOG_STDERR(...) do {if(isatty(STDERR_FILENO)) {fprintf(stderr, __VA_ARGS__); }} while(0)
#define LOG_STDOUT(...) do {if(isatty(STDOUT_FILENO)) {fprintf(stdout, __VA_ARGS__); }} while(0)

//print errmsg of errno.As strerror not thread-safe, need
//to call ERRMSG, KERRMSG which use this class.
struct ObLogPrintErrMsg
{
  ObLogPrintErrMsg()
  {}
  ~ObLogPrintErrMsg()
  {}
  int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    if (OB_LIKELY(NULL != buf) && OB_LIKELY(len > 0)) {
      (void)logdata_printf(buf, len, pos, "\"%m\"");
    }
    return pos;
  }
};

//print errmsg of no specified. As strerror not thread-safe, need
//to call ERRNOMSG, KERRNOMSG which use this class.
struct ObLogPrintErrNoMsg
{
  explicit ObLogPrintErrNoMsg(int no) : errno_(no)
  {}
  ~ObLogPrintErrNoMsg()
  {}
  int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    int old_err = errno;
    errno = errno_;
    if (OB_LIKELY(NULL != buf) && OB_LIKELY(len > 0)) {
      (void)logdata_printf(buf, len, pos, "\"%m\"");
    }
    errno = old_err;
    return pos;
  }
  int errno_;
};

// print object with to_string() member
template<class T>
int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const T &obj, FalseType)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(pos >= buf_len)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    pos += obj.to_string(buf + pos, buf_len - pos);
  }
  return ret;
}
// print enum object
template<class T>
int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const T &obj, TrueType)
{
  return logdata_printf(buf, buf_len, pos, "%ld", static_cast<int64_t>(obj));
}
// print object
template<class T>
int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const T &obj)
{
  int ret = OB_SUCCESS;
  if (NULL == &obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_print_obj(buf, buf_len, pos, obj, BoolType<__is_enum(T)>());
  }
  return ret;
}
template<class T>
int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, T *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "%p", obj);
  }
  return ret;
}
template<class T>
int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const T *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "%p", obj);
  }
  return ret;
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const uint64_t obj)
{
  return logdata_printf(buf, buf_len, pos, "%lu", obj);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const int64_t obj)
{
  return logdata_printf(buf, buf_len, pos, "%ld", obj);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const uint32_t obj)
{
  return logdata_printf(buf, buf_len, pos, "%u", obj);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const int32_t obj)
{
  return logdata_printf(buf, buf_len, pos, "%d", obj);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const uint16_t obj)
{
  return logdata_printf(buf, buf_len, pos, "%u", obj);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const int16_t obj)
{
  return logdata_printf(buf, buf_len, pos, "%d", obj);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const uint8_t obj)
{
  return logdata_printf(buf, buf_len, pos, "%u", obj);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const int8_t obj)
{
  return logdata_printf(buf, buf_len, pos, "%d", obj);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const char obj)
{
  return logdata_printf(buf, buf_len, pos, "%c", obj);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const float obj)
{
  return logdata_printf(buf, buf_len, pos, "%f", obj);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const double obj)
{
  return logdata_printf(buf, buf_len, pos, "%.12f", obj);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const bool obj)
{
  return logdata_printf(buf, buf_len, pos, "%s", obj ? LOG_N_TRUE : LOG_N_FLASE);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, char *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "\"%s\"", obj);
  }
  return ret;
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "\"%s\"", obj);
  }
  return ret;
}
inline int logdata_print_info(char *buf, const int64_t buf_len, int64_t &pos, const char *info)
{
  int ret = OB_SUCCESS;
  if (NULL == info) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "%s", info);
  }
  return ret;
}

inline int logdata_print_info_begin(char *buf, const int64_t buf_len, int64_t &pos, const char *info)
{
  int ret = OB_SUCCESS;
  if (NULL == info) {
    ret = logdata_printf(buf, buf_len, pos, "NULL(");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "%s(", info);
  }
  return ret;
}

// print object with to_string() member
template<class T>
int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                          const bool with_comma, const T &obj, FalseType)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(pos >= buf_len)) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(logdata_printf(buf, buf_len, pos, WITH_COMMA("%s="), key))) {
    //
  } else {
    pos += obj.to_string(buf + pos, buf_len - pos);
  }
  return ret;
}
// print enum object
template<class T>
int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                          const bool with_comma, const T &obj, TrueType)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%ld"), key, static_cast<int64_t>(obj));
}
// print object
template<class T>
int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                          const bool with_comma, const T &obj)
{
  int ret = OB_SUCCESS;
  ret = logdata_print_key_obj(buf, buf_len, pos, key, with_comma, obj, BoolType<__is_enum(T)>());
  return ret;
}
template<class T>
int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                          const bool with_comma, T *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=NULL"), key);
  } else {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%p"), key, obj);
  }
  return ret;
}
template<class T>
int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                          const bool with_comma, const T *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=NULL"), key);
  } else {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%p"), key, obj);
  }
  return ret;
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const uint64_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%lu"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const int64_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%ld"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const uint32_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%u"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const int32_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%d"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const uint16_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%u"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const int16_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%d"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const uint8_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%u"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const int8_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%d"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const char obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%c"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const float obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%f"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const double obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%.12f"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const bool obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%s"), key, obj ? LOG_N_TRUE : LOG_N_FLASE);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, char *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=NULL"), key);
  } else {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=\"%s\""), key, obj);
  }
  return ret;
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const char *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=NULL"), key);
  } else {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=\"%s\""), key, obj);
  }
  return ret;
}

template <class T>
int logdata_print_kv(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const T &obj)
{
  return logdata_print_key_obj(buf, buf_len, pos, key, false, obj);
}


}
}

#define LOG_PRINT_INFO(obj)                                                      \
  if (OB_SUCC(ret)) {                                                            \
    ret = ::oceanbase::common::logdata_print_info(data, MAX_LOG_SIZE, pos, obj); \
  }

#define LOG_PRINT_INFO_BEGIN(obj)                                                   \
  if (OB_SUCC(ret)) {                                                               \
    ret = ::oceanbase::common::logdata_print_info_begin(data, MAX_LOG_SIZE, pos, obj); \
  }

#define LOG_PRINT_KV(key, value)                                                      \
  if (OB_SUCC(ret)) {                                                                 \
    ret = ::oceanbase::common::logdata_print_kv(data, MAX_LOG_SIZE, pos, key, value); \
  }

#define LOG_DATA_PRINTF(args...)                                                  \
  if (OB_SUCC(ret)) {                                                             \
    ret = ::oceanbase::common::logdata_printf(data, MAX_LOG_SIZE, pos, ##args);   \
  }

#define LOG_COMMA() LOG_DATA_PRINTF(", ")
#define LOG_KV_BEGIN() LOG_DATA_PRINTF("(")
#define LOG_KV_END()   LOG_DATA_PRINTF(")")

#define LOG_TYPENAME_TN0
#define LOG_TYPENAME_TN1 typename T1
#define LOG_TYPENAME_TN2 LOG_TYPENAME_TN1, typename T2
#define LOG_TYPENAME_TN3 LOG_TYPENAME_TN2, typename T3
#define LOG_TYPENAME_TN4 LOG_TYPENAME_TN3, typename T4
#define LOG_TYPENAME_TN5 LOG_TYPENAME_TN4, typename T5
#define LOG_TYPENAME_TN6 LOG_TYPENAME_TN5, typename T6
#define LOG_TYPENAME_TN7 LOG_TYPENAME_TN6, typename T7
#define LOG_TYPENAME_TN8 LOG_TYPENAME_TN7, typename T8
#define LOG_TYPENAME_TN9 LOG_TYPENAME_TN8, typename T9
#define LOG_TYPENAME_TN10 LOG_TYPENAME_TN9, typename T10
#define LOG_TYPENAME_TN11 LOG_TYPENAME_TN10, typename T11
#define LOG_TYPENAME_TN12 LOG_TYPENAME_TN11, typename T12
#define LOG_TYPENAME_TN13 LOG_TYPENAME_TN12, typename T13
#define LOG_TYPENAME_TN14 LOG_TYPENAME_TN13, typename T14
#define LOG_TYPENAME_TN15 LOG_TYPENAME_TN14, typename T15
#define LOG_TYPENAME_TN16 LOG_TYPENAME_TN15, typename T16
#define LOG_TYPENAME_TN17 LOG_TYPENAME_TN16, typename T17
#define LOG_TYPENAME_TN18 LOG_TYPENAME_TN17, typename T18
#define LOG_TYPENAME_TN19 LOG_TYPENAME_TN18, typename T19
#define LOG_TYPENAME_TN20 LOG_TYPENAME_TN19, typename T20

#define LOG_FUNC_PARA(n) const char* key##n, const T##n &obj##n
#define LOG_PARAMETER_KV0
#define LOG_PARAMETER_KV1 LOG_FUNC_PARA(1)
#define LOG_PARAMETER_KV2 LOG_PARAMETER_KV1, LOG_FUNC_PARA(2)
#define LOG_PARAMETER_KV3 LOG_PARAMETER_KV2, LOG_FUNC_PARA(3)
#define LOG_PARAMETER_KV4 LOG_PARAMETER_KV3, LOG_FUNC_PARA(4)
#define LOG_PARAMETER_KV5 LOG_PARAMETER_KV4, LOG_FUNC_PARA(5)
#define LOG_PARAMETER_KV6 LOG_PARAMETER_KV5, LOG_FUNC_PARA(6)
#define LOG_PARAMETER_KV7 LOG_PARAMETER_KV6, LOG_FUNC_PARA(7)
#define LOG_PARAMETER_KV8 LOG_PARAMETER_KV7, LOG_FUNC_PARA(8)
#define LOG_PARAMETER_KV9 LOG_PARAMETER_KV8, LOG_FUNC_PARA(9)
#define LOG_PARAMETER_KV10 LOG_PARAMETER_KV9, LOG_FUNC_PARA(10)
#define LOG_PARAMETER_KV11 LOG_PARAMETER_KV10, LOG_FUNC_PARA(11)
#define LOG_PARAMETER_KV12 LOG_PARAMETER_KV11, LOG_FUNC_PARA(12)
#define LOG_PARAMETER_KV13 LOG_PARAMETER_KV12, LOG_FUNC_PARA(13)
#define LOG_PARAMETER_KV14 LOG_PARAMETER_KV13, LOG_FUNC_PARA(14)
#define LOG_PARAMETER_KV15 LOG_PARAMETER_KV14, LOG_FUNC_PARA(15)
#define LOG_PARAMETER_KV16 LOG_PARAMETER_KV15, LOG_FUNC_PARA(16)
#define LOG_PARAMETER_KV17 LOG_PARAMETER_KV16, LOG_FUNC_PARA(17)
#define LOG_PARAMETER_KV18 LOG_PARAMETER_KV17, LOG_FUNC_PARA(18)
#define LOG_PARAMETER_KV19 LOG_PARAMETER_KV18, LOG_FUNC_PARA(19)
#define LOG_PARAMETER_KV20 LOG_PARAMETER_KV19, LOG_FUNC_PARA(20)

#define LOG_FUNC_ARG(n) key##n, obj##n
#define LOG_ARGUMENT_KV0
#define LOG_ARGUMENT_KV1 LOG_FUNC_ARG(1)
#define LOG_ARGUMENT_KV2 LOG_ARGUMENT_KV1, LOG_FUNC_ARG(2)
#define LOG_ARGUMENT_KV3 LOG_ARGUMENT_KV2, LOG_FUNC_ARG(3)
#define LOG_ARGUMENT_KV4 LOG_ARGUMENT_KV3, LOG_FUNC_ARG(4)
#define LOG_ARGUMENT_KV5 LOG_ARGUMENT_KV4, LOG_FUNC_ARG(5)
#define LOG_ARGUMENT_KV6 LOG_ARGUMENT_KV5, LOG_FUNC_ARG(6)
#define LOG_ARGUMENT_KV7 LOG_ARGUMENT_KV6, LOG_FUNC_ARG(7)
#define LOG_ARGUMENT_KV8 LOG_ARGUMENT_KV7, LOG_FUNC_ARG(8)
#define LOG_ARGUMENT_KV9 LOG_ARGUMENT_KV8, LOG_FUNC_ARG(9)
#define LOG_ARGUMENT_KV10 LOG_ARGUMENT_KV9, LOG_FUNC_ARG(10)
#define LOG_ARGUMENT_KV11 LOG_ARGUMENT_KV10, LOG_FUNC_ARG(11)
#define LOG_ARGUMENT_KV12 LOG_ARGUMENT_KV11, LOG_FUNC_ARG(12)
#define LOG_ARGUMENT_KV13 LOG_ARGUMENT_KV12, LOG_FUNC_ARG(13)
#define LOG_ARGUMENT_KV14 LOG_ARGUMENT_KV13, LOG_FUNC_ARG(14)
#define LOG_ARGUMENT_KV15 LOG_ARGUMENT_KV14, LOG_FUNC_ARG(15)
#define LOG_ARGUMENT_KV16 LOG_ARGUMENT_KV15, LOG_FUNC_ARG(16)
#define LOG_ARGUMENT_KV17 LOG_ARGUMENT_KV16, LOG_FUNC_ARG(17)
#define LOG_ARGUMENT_KV18 LOG_ARGUMENT_KV17, LOG_FUNC_ARG(18)
#define LOG_ARGUMENT_KV19 LOG_ARGUMENT_KV18, LOG_FUNC_ARG(19)
#define LOG_ARGUMENT_KV20 LOG_ARGUMENT_KV19, LOG_FUNC_ARG(20)

#define LOG_BODY(n)                                                                       \
  if (OB_SUCC(ret)) { \
    if (1 == n) {\
      ret = ::oceanbase::common::logdata_print_kv(data, MAX_LOG_SIZE, pos, key##n, obj##n); \
    } else {\
      ret = ::oceanbase::common::logdata_print_key_obj(data, MAX_LOG_SIZE, pos, key##n, true, obj##n); \
    }\
  }

#define LOG_FUNC_BODY_0
#define LOG_FUNC_BODY_1 LOG_BODY(1)
#define LOG_FUNC_BODY_2 LOG_FUNC_BODY_1; LOG_BODY(2)
#define LOG_FUNC_BODY_3 LOG_FUNC_BODY_2; LOG_BODY(3)
#define LOG_FUNC_BODY_4 LOG_FUNC_BODY_3; LOG_BODY(4)
#define LOG_FUNC_BODY_5 LOG_FUNC_BODY_4; LOG_BODY(5)
#define LOG_FUNC_BODY_6 LOG_FUNC_BODY_5; LOG_BODY(6)
#define LOG_FUNC_BODY_7 LOG_FUNC_BODY_6; LOG_BODY(7)
#define LOG_FUNC_BODY_8 LOG_FUNC_BODY_7; LOG_BODY(8)
#define LOG_FUNC_BODY_9 LOG_FUNC_BODY_8; LOG_BODY(9)
#define LOG_FUNC_BODY_10 LOG_FUNC_BODY_9; LOG_BODY(10)
#define LOG_FUNC_BODY_11 LOG_FUNC_BODY_10; LOG_BODY(11)
#define LOG_FUNC_BODY_12 LOG_FUNC_BODY_11; LOG_BODY(12)
#define LOG_FUNC_BODY_13 LOG_FUNC_BODY_12; LOG_BODY(13)
#define LOG_FUNC_BODY_14 LOG_FUNC_BODY_13; LOG_BODY(14)
#define LOG_FUNC_BODY_15 LOG_FUNC_BODY_14; LOG_BODY(15)
#define LOG_FUNC_BODY_16 LOG_FUNC_BODY_15; LOG_BODY(16)
#define LOG_FUNC_BODY_17 LOG_FUNC_BODY_16; LOG_BODY(17)
#define LOG_FUNC_BODY_18 LOG_FUNC_BODY_17; LOG_BODY(18)
#define LOG_FUNC_BODY_19 LOG_FUNC_BODY_18; LOG_BODY(19)
#define LOG_FUNC_BODY_20 LOG_FUNC_BODY_19; LOG_BODY(20)

#define CHECK_LOG_END_AND_ERROR_LOG(log_item)                                         \
  if (OB_SUCC(ret) || OB_UNLIKELY(OB_SIZE_OVERFLOW == ret)) {                         \
    ret = OB_SUCCESS;                                                                 \
    check_log_end(*log_item, pos);                                                    \
    if (OB_FAIL(check_error_log(*log_item))) {                                        \
      LOG_STDERR("check_error_log error ret = %d\n", ret);                            \
    }                                                                                 \
  }

//for ObLogger
#define DEFINE_LOG_PRINT_KV(n)                                                                  \
  template <LOG_TYPENAME_TN##n>                                                                 \
  void log_message_kv(const char *mod_name, const int32_t level, const char *file,              \
                      const int32_t line, const char *function,                                 \
                      const char *info_string, LOG_PARAMETER_KV##n)                              \
  {                                                                                             \
    const ObLogFDType type = (NULL == mod_name ? FD_XFLUSH_FILE : FD_DEFAULT_FILE);             \
    log_message_kv(type, mod_name, level, file, line, function, info_string, LOG_ARGUMENT_KV##n);\
  }                                                                                             \
  template <LOG_TYPENAME_TN##n>                                                                 \
  void log_message_kv(const ObLogFDType type, const char *mod_name,                             \
                      const int32_t level, const char *file,                                    \
                      const int32_t line, const char *function,                                 \
                      const char *info_string, LOG_PARAMETER_KV##n)                              \
  {                                                                                             \
  int ret = OB_SUCCESS;                                                                       \
  LogBuffer *log_buffer = NULL;                                                               \
  if (OB_LIKELY(level <= OB_LOG_LEVEL_DEBUG)                                                  \
      && OB_LIKELY(level >= OB_LOG_LEVEL_ERROR)                                               \
      && OB_LIKELY(is_enable_logging())                                                       \
      && OB_NOT_NULL(file) && OB_NOT_NULL(function)                                           \
      && OB_NOT_NULL(function) && OB_NOT_NULL(info_string)) {                                 \
    set_disable_logging(true);                                                              \
    if (get_trace_mode()) {                                                                   \
      if (OB_NOT_NULL(log_buffer = get_thread_buffer())                                       \
          && OB_LIKELY(!log_buffer->is_oversize())) {                                         \
        log_head_info(type, mod_name, level, LogLocation(file, line, function), *log_buffer);       \
        int64_t &pos = log_buffer->pos_;                                                      \
        char *data = log_buffer->buffer_;                                                     \
        LOG_PRINT_INFO_BEGIN(info_string);                                                    \
        LOG_FUNC_BODY_##n;                                                                    \
        LOG_KV_END();                                                                         \
        log_tail(level, *log_buffer);                                                         \
      }                                                                                       \
    } else if (!is_async_log_used()) {                                                        \
      if (OB_NOT_NULL(log_buffer = get_thread_buffer())                                     \
          && OB_LIKELY(!log_buffer->is_oversize())) {                                       \
        int64_t &pos = log_buffer->pos_;                                                    \
        char *data = log_buffer->buffer_;                                                   \
        LOG_PRINT_INFO_BEGIN(info_string);                                                  \
        LOG_FUNC_BODY_##n;                                                                  \
        LOG_KV_END();                                                                       \
        log_data(type, mod_name, level, LogLocation(file, line, function), *log_buffer);          \
      }                                                                                     \
    } else {                                                                                \
      struct timeval tv;                                                                    \
      gettimeofday(&tv, NULL);                                                              \
      const int64_t logging_time_us_begin = static_cast<int64_t>(tv.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(tv.tv_usec);\
      ++curr_logging_seq_;                                                                  \
      ObLogItem *log_item = NULL;                                                           \
      if (OB_FAIL(pop_from_free_queue(level, log_item))) {                                  \
        LOG_STDERR("pop_from_free_queue error, ret=%d\n", ret);                             \
      } else if (OB_FAIL(async_log_data_header(type, *log_item, tv, mod_name, level, file, line, function))) {\
        LOG_STDERR("async_log_data_header error ret = %d\n", ret);                          \
      } else {                                                                              \
        int64_t MAX_LOG_SIZE = log_item->get_buf_size();                                    \
        int64_t pos = log_item->get_data_len();                                             \
        char *data = log_item->get_buf();                                                   \
        LOG_PRINT_INFO_BEGIN(info_string);                                                  \
        LOG_FUNC_BODY_##n;                                                                  \
        LOG_KV_END();                                                                       \
        CHECK_LOG_END_AND_ERROR_LOG(log_item)                                               \
      }                                                                                     \
                                                                                            \
      if (OB_SUCC(ret)                                                                      \
          && OB_NOT_NULL(log_item)                                                          \
          && log_item->is_size_overflow()) {                                                \
        bool upgrade_result = false;                                                        \
        if (OB_FAIL(try_upgrade_log_item(log_item, upgrade_result))) {                      \
          LOG_STDERR("try_upgrade_log_item error, ret=%d\n", ret);                          \
        } else if (upgrade_result) {                                                        \
          int64_t MAX_LOG_SIZE = log_item->get_buf_size();                                  \
          int64_t pos = log_item->get_data_len();                                           \
          char *data = log_item->get_buf();                                                 \
          LOG_PRINT_INFO_BEGIN(info_string);                                                \
          LOG_KV_BEGIN();                                                                   \
          LOG_FUNC_BODY_##n;                                                                \
          LOG_KV_END();                                                                     \
          CHECK_LOG_END_AND_ERROR_LOG(log_item)                                             \
        }                                                                                   \
      }                                                                                     \
                                                                                            \
      if (OB_SUCC(ret)                                                                      \
          && OB_NOT_NULL(log_item)                                                          \
          && log_item->is_size_overflow()) {                                                \
        bool upgrade_result = false;                                                        \
        if (OB_FAIL(try_upgrade_log_item(log_item, upgrade_result))) {                      \
          LOG_STDERR("try_upgrade_log_item error, ret=%d\n", ret);                          \
        } else if (upgrade_result) {                                                        \
          int64_t MAX_LOG_SIZE = log_item->get_buf_size();                                  \
          int64_t pos = log_item->get_data_len();                                           \
          char *data = log_item->get_buf();                                                 \
          LOG_PRINT_INFO_BEGIN(info_string);                                                \
          LOG_KV_BEGIN();                                                                   \
          LOG_FUNC_BODY_##n;                                                                \
          LOG_KV_END();                                                                     \
          CHECK_LOG_END_AND_ERROR_LOG(log_item)                                             \
        }                                                                                   \
      }                                                                                     \
                                                                                            \
      if (OB_SUCC(ret) && OB_NOT_NULL(log_item)) {                                            \
        if (OB_FAIL(check_callback(*log_item))) {                                             \
          LOG_STDERR("check_callback error ret = %d\n", ret);                                 \
        } else if (OB_FAIL(push_to_async_queue(*log_item))) {                                 \
          LOG_STDERR("push log item to queue error ret = %d\n", ret);                         \
        } else {                                                                              \
          struct timeval tv_end;                                                              \
          gettimeofday(&tv_end, NULL);                                                        \
          const int64_t logging_time_us_end = static_cast<int64_t>(tv_end.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(tv_end.tv_usec);\
          last_logging_cost_time_us_ = logging_time_us_end - logging_time_us_begin;           \
          last_logging_seq_ = curr_logging_seq_;                                              \
        }                                                                                     \
      }                                                                                       \
      if (OB_FAIL(ret)) {                                                                     \
        inc_dropped_log_count(level);                                                         \
        push_to_free_queue(log_item);                                                         \
        log_item = NULL;                                                                      \
      }                                                                                       \
    }                                                                                         \
    set_disable_logging(false);                                                               \
  }                                                                                           \
}

//for TraceLog
#define DEFINE_FILL_LOG_KV(n)                                                               \
  template <LOG_TYPENAME_TN##n>                                                             \
  static void fill_log_kv(LogBuffer &log_buffer, const char *function,                      \
                          const char *info_string, LOG_PARAMETER_KV##n)                      \
  {                                                                                         \
    int ret = OB_SUCCESS;                                                                   \
    log_buffer.check_and_lock();                                                            \
    int64_t MAX_LOG_SIZE = LogBuffer::LOG_BUFFER_SIZE - log_buffer.cur_pos_;                \
    if (MAX_LOG_SIZE > 0) {                                                                 \
      char *data = log_buffer.buffer_ + log_buffer.cur_pos_;                                \
      int64_t pos = 0;                                                                      \
      LOG_DATA_PRINTF("[%s] ", function);                                                   \
      LOG_PRINT_INFO_BEGIN(info_string);                                                          \
      LOG_FUNC_BODY_##n;                                                                    \
      LOG_KV_END();                                                                         \
      if (pos >= 0                                                                          \
          && pos < MAX_LOG_SIZE) {                                                          \
        log_buffer.cur_pos_ += pos;                                                         \
        add_stamp(log_buffer, MAX_LOG_SIZE, pos);                                           \
      } else {                                                                              \
        if (log_buffer.cur_pos_ >= 0                                                        \
            && log_buffer.cur_pos_ < LogBuffer::LOG_BUFFER_SIZE) {                          \
          log_buffer.buffer_[log_buffer.cur_pos_] = '\0';                                   \
        }                                                                                   \
        print_log(log_buffer);                                                              \
        MAX_LOG_SIZE = LogBuffer::LOG_BUFFER_SIZE - log_buffer.cur_pos_;                    \
        data = log_buffer.buffer_ + log_buffer.cur_pos_;                                    \
        pos = 0;                                                                            \
        LOG_DATA_PRINTF("[%s] ", function);                                                 \
        LOG_PRINT_INFO_BEGIN(info_string);                                                        \
        LOG_FUNC_BODY_##n;                                                                  \
        LOG_KV_END();                                                                       \
        if (pos >= 0                                                                        \
            && pos < MAX_LOG_SIZE) {                                                        \
          log_buffer.cur_pos_ += pos;                                                       \
          add_stamp(log_buffer, MAX_LOG_SIZE, pos);                                         \
        } else {                                                                            \
          print_log(log_buffer);                                                            \
        }                                                                                   \
      }                                                                                     \
      if (log_buffer.cur_pos_ >=0                                                           \
          && log_buffer.cur_pos_ < LogBuffer::LOG_BUFFER_SIZE) {                            \
        log_buffer.buffer_[log_buffer.cur_pos_] = '\0';                                     \
      }                                                                                     \
    }                                                                                       \
    log_buffer.check_and_unlock();                                                          \
  }

#endif
