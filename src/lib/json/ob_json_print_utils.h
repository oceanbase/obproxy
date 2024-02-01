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

#ifndef LIB_JSON_OB_JSON_PRINT_UTILS_
#define LIB_JSON_OB_JSON_PRINT_UTILS_
#include "lib/utility/ob_print_utils.h"
#include "lib/json/ob_json.h"
#include "lib/allocator/ob_mod_define.h"
namespace oceanbase
{
namespace json
{

// To be more readable, we don't quote name in our logging json.
// This class convert it to standard json format, for example:
//
//   From:
//     {name1:123, name2:"string1", "name3":[]}
//   To:
//     {"name1":123, "name2":"string1", "name3":[]}
//
class ObStdJsonConvertor
{
public:
  ObStdJsonConvertor()
      : backslash_escape_(true), json_(NULL), buf_(NULL), buf_size_(0), pos_(0) {}
  virtual ~ObStdJsonConvertor() {}

  int init(const char *json, char *buf, const int64_t buf_size_);

  void disable_backslash_escape() { backslash_escape_ = false; }
  void enable_backslash_escape() { backslash_escape_ = true; }

  int convert(int64_t &out_len);

private:
  // output [begin, p]
  int output(const char *p, const char *&begin, int64_t &out_len);
  int quoted_output(const char *p, const char *&begin, int64_t &out_len);
  int add_quote_mark(int64_t &out_len);

private:
  bool backslash_escape_;
  const char *json_;
  char *buf_;
  int64_t buf_size_;
  int64_t pos_;
};

// another to_cstring that prints JSON string pretty
template <typename T>
const char *to_json_cstring(const T &obj1)
{
  int ret = common::OB_SUCCESS;
  const char* str_ret = ::oceanbase::common::to_cstring<T>(obj1);
  common::ObArenaAllocator allocator(common::ObModIds::JSON_PARSER);
  static const int64_t BUFFER_SIZE = 2 * 1024 * 1024;
  common::ToCStringBufferObj<T, 1, BUFFER_SIZE> *buffer_obj
      = GET_TSI_MULT(__typeof__(*buffer_obj), common::TSI_COMMON_TO_CSTRING_BUFFER_OBJ_3);
  if (OB_ISNULL(buffer_obj)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WDIAG, "buffer obj is NULL", K(ret));
  } else {
    char *buf = buffer_obj->buffers[0];
    ObStdJsonConvertor convertor;
    int64_t out_len = 0;
    if (OB_FAIL(convertor.init(str_ret, buf, BUFFER_SIZE))) {
      LIB_LOG(WDIAG, "fail to init convertor", K(ret));
    } else {
      convertor.disable_backslash_escape();
      if (OB_FAIL(convertor.convert(out_len))) {
        LIB_LOG(WDIAG, "fail to do convert", K(ret));
      } else {
        str_ret = buf;
      }
    }
    buf[std::min(out_len, BUFFER_SIZE - 1)] = '\0';
  }
  if (OB_SUCC(ret)) {
    // try tidy
    Parser json_parser;
    Value *root = NULL;
    if (OB_FAIL(json_parser.init(&allocator))) {
      LIB_LOG(WDIAG, "fail to init json parser", K(ret));
    } else if (OB_FAIL(json_parser.parse(str_ret, strlen(str_ret), root))) {
      LIB_LOG(WDIAG, "fail to parse json", K(ret));
    } else {
      Tidy tidy(root);
      str_ret = ::oceanbase::common::to_cstring<Tidy>(tidy);
    }
  }
  return str_ret;
}

template <typename T>
const char *to_json_cstring(T *obj)
{
  if (NULL == obj) {
    return "NULL";
  } else {
    return to_json_cstring(*obj);
  }
}

} // end namespace json
} // end namespace oceanbase

#define SJ(X) oceanbase::json::to_json_cstring((X))

#endif /* LIB_JSON_OB_JSON_PRINT_UTILS_ */
