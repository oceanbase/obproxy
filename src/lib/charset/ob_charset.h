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

#ifndef OCEANBASE_CHARSET_H_
#define OCEANBASE_CHARSET_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/charset/ob_ctype.h"

namespace oceanbase
{
namespace common
{
enum ObCharsetType
{
  CHARSET_INVALID = 0,
  CHARSET_BINARY = 1,
  CHARSET_UTF8MB4 = 2,
  CHARSET_GBK = 3,
  CHARSET_UTF16 = 4,
  CHARSET_GB18030 = 5,
  CHARSET_LATIN1 = 6,
  CHARSET_GB18030_2022 = 7,
  CHARSET_MAX,
};

enum ObCollationType
{
  CS_TYPE_INVALID = 0,
  CS_TYPE_LATIN1_SWEDISH_CI = 8,
  CS_TYPE_GBK_CHINESE_CI = 28,
  CS_TYPE_UTF8MB4_GENERAL_CI = 45,
  CS_TYPE_UTF8MB4_BIN = 46,
  CS_TYPE_LATIN1_BIN = 47,
  CS_TYPE_UTF16_GENERAL_CI = 54,
  CS_TYPE_UTF16_BIN = 55,
  CS_TYPE_BINARY = 63,
  CS_TYPE_GBK_BIN = 87,
  CS_TYPE_UTF16_UNICODE_CI = 101,
  CS_TYPE_GB18030_2022_BIN = 216, // unused in mysql
  CS_TYPE_GB18030_2022_PINYIN_CI = 217, // unused in mysql
  CS_TYPE_GB18030_2022_PINYIN_CS = 218, // unused in mysql
  CS_TYPE_GB18030_2022_RADICAL_CI = 219, // unused in mysql
  CS_TYPE_GB18030_2022_RADICAL_CS = 220, // unused in mysql
  CS_TYPE_GB18030_2022_STROKE_CI = 221, // unused in mysql
  CS_TYPE_GB18030_2022_STROKE_CS = 222, // unused in mysql
  CS_TYPE_UTF8MB4_UNICODE_CI = 224,
  CS_TYPE_GB18030_CHINESE_CI = 248,
  CS_TYPE_GB18030_BIN = 249,
  CS_TYPE_GB18030_CHINESE_CS = 251,

  CS_TYPE_EXTENDED_MARK = 256, //the cs types below can not used for storing
  CS_TYPE_UTF8MB4_0900_BIN, //309 in mysql 8.0

  //pinyin order (occupied)
  CS_TYPE_PINYIN_BEGIN_MARK,
  CS_TYPE_UTF8MB4_ZH_0900_AS_CS, //308 in mysql 8.0
  CS_TYPE_GBK_ZH_0900_AS_CS,
  CS_TYPE_UTF16_ZH_0900_AS_CS,
  CS_TYPE_GB18030_ZH_0900_AS_CS,
  CS_TYPE_latin1_ZH_0900_AS_CS, //invaid, not really used
  CS_TYPE_GB18030_2022_ZH_0900_AS_CS,
  //radical-stroke order
  CS_TYPE_RADICAL_BEGIN_MARK,
  CS_TYPE_UTF8MB4_ZH2_0900_AS_CS,
  CS_TYPE_GBK_ZH2_0900_AS_CS,
  CS_TYPE_UTF16_ZH2_0900_AS_CS,
  CS_TYPE_GB18030_ZH2_0900_AS_CS,
  CS_TYPE_latin1_ZH2_0900_AS_CS ,//invaid
  CS_TYPE_GB18030_2022_ZH2_0900_AS_CS,
  //stroke order
  CS_TYPE_STROKE_BEGIN_MARK,
  CS_TYPE_UTF8MB4_ZH3_0900_AS_CS,
  CS_TYPE_GBK_ZH3_0900_AS_CS,
  CS_TYPE_UTF16_ZH3_0900_AS_CS,
  CS_TYPE_GB18030_ZH3_0900_AS_CS,
  CS_TYPE_latin1_ZH3_0900_AS_CS, //invaid
  CS_TYPE_GB18030_2022_ZH3_0900_AS_CS,
  CS_TYPE_MAX,
};
/*
Coercibility Meaning Example
0 Explicit collation Value with COLLATE clause
1 No collation Concatenation of strings with different collations
2 Implicit collation Column value, stored routine parameter or local variable
3 System constant USER() return value
4 Coercible Literal string
5 Ignorable NULL or an expression derived from NULL
*/
enum ObCollationLevel
{
  CS_LEVEL_EXPLICIT = 0,
  CS_LEVEL_NONE = 1,
  CS_LEVEL_IMPLICIT = 2,
  CS_LEVEL_SYSCONST = 3,
  CS_LEVEL_COERCIBLE = 4,
  CS_LEVEL_NUMERIC = 5,
  CS_LEVEL_IGNORABLE = 6,
  CS_LEVEL_INVALID,   // here we didn't define CS_LEVEL_INVALID as 0,
                      // since 0 is a valid value for CS_LEVEL_EXPLICIT in mysql 5.6.
                      // fortunately we didn't need to use it to define array like charset_arr,
                      // and we didn't persist it on storage.
};

struct ObCharsetWrapper
{
  ObCharsetType charset_;
  const char *description_;
  ObCollationType default_collation_;
  int64_t maxlen_;
};

struct ObCollationWrapper
{
  ObCollationType collation_;
  ObCharsetType charset_;
  int64_t id_;
  bool default_;
  bool compiled_;
  int64_t sortlen_;
};

class ObCharset
{
private:
  ObCharset() {};
  virtual ~ObCharset() {};

public:
  static const int64_t CHARSET_WRAPPER_COUNT = 7;
  static const int64_t COLLATION_WRAPPER_COUNT = 21;

  static double strntod(const char *str,
                        size_t str_len,
                        char **endptr,
                        int *err);
  static int64_t strntoll(const char *str,
                   size_t str_len,
                   int base,
                   char **end_ptr,
                   int *err);
  static uint64_t strntoull(const char *str,
                            size_t str_len,
                            int base,
                            char **end_ptr,
                            int *err);
  static int64_t strntoll(const char *str,
                   size_t str_len,
                   int base,
                   int *err);
  static uint64_t strntoull(const char *str,
                            size_t str_len,
                            int base,
                            int *err);
  static uint64_t strntoullrnd(const char *str,
                               size_t str_len,
                               int unsigned_fl,
                               char **endptr,
                               int *err);
  static size_t scan_str(const char *str,
                         const char *end,
                         int sq);
  static uint32_t instr(ObCollationType collation_type,
                        const char *str1,
                        int64_t str1_len,
                        const char *str2,
                        int64_t str2_len);
  static uint32_t locate(ObCollationType collation_type,
                         const char *str1,
                         int64_t str1_len,
                         const char *str2,
                         int64_t str2_len,
                         int64_t pos);
  static int well_formed_len(ObCollationType collation_type,
                             const char *str,
                             int64_t str_len,
                             int64_t &well_formed_len);
  static int well_formed_len(ObCollationType collation_type,
                             const char *str,
                             int64_t str_len,
                             int64_t &well_formed_len,
                             int32_t &well_formed_error);
  static int strcmp(ObCollationType collation_type,
                    const char *str1,
                    int64_t str1_len,
                    const char *str2,
                    int64_t str2_len);

  static int strcmpsp(ObCollationType collation_type,
                      const char *str1,
                      int64_t str1_len,
                      const char *str2,
                      int64_t str2_len,
                      bool cmp_endspace);

  static size_t casedn(const ObCollationType collation_type,
                       char *src, size_t src_len,
                       char *dest, size_t dest_len);
  static size_t caseup(const ObCollationType collation_type,
                       char *src, size_t src_len,
                       char *dest, size_t dest_len);
  static size_t sortkey(ObCollationType collation_type,
                        const char *str,
                        int64_t str_len,
                        char *key,
                        int64_t key_len,
                        bool &is_valid_unicode);
  static uint64_t hash(ObCollationType collation_type,
                       const char *str,
                       int64_t str_len,
                       uint64_t seed,
                       const bool calc_end_space,
                       hash_algo hash_algo);

  static int like_range(ObCollationType collation_type,
                        const ObString &like_str,
                        char escape,
                        char *min_str,
                        size_t *min_str_len,
                        char *max_str,
                        size_t *max_str_len);
  static size_t strlen_char(ObCollationType collation_type,
                            const char *str,
                            int64_t str_len);
  static size_t strlen_byte_no_sp(ObCollationType collation_type,
                                  const char *str,
                                  int64_t str_len);
  static size_t charpos(ObCollationType collation_type,
                        const char *str,
                        const int64_t str_len,
                        const int64_t length,
                        int *ret = NULL);
  // match like pattern
  static bool wildcmp(ObCollationType collation_type,
                      const ObString &str,
                      const ObString &wildstr,
                      int32_t escape, int32_t w_one, int32_t w_many);
  static int mb_wc(ObCollationType collation_type,
                   const char *mb,
                   const int64_t mb_size,
                   int32_t &length,
                   int32_t &wc);
  static int wc_mb(ObCollationType collation_type,
                   int32_t wc,
                   char *buff,
                   int32_t buff_len,
                   int32_t &length);
  static const char *charset_name(ObCharsetType charset_type);
  static const char *charset_name(ObCollationType coll_type);
  static const char *collation_name(ObCollationType collation_type);
  static int collation_name(ObCollationType coll_type, ObString &coll_name);
  static const char* collation_level(const ObCollationLevel cs_level);
  static ObCharsetType charset_type(const char *cs_name);
  static ObCollationType collation_type(const char *cs_name);
  static ObCharsetType charset_type(const ObString &cs_name);
  static ObCollationType collation_type(const ObString &cs_name);
  static bool is_valid_collation(ObCharsetType charset_type, ObCollationType coll_type);
  static bool is_valid_collation(int64_t coll_type_int);
  static bool is_valid_charset(int64_t cs_type_int);
  static bool is_gb18030_2022(int64_t coll_type_int) {
    ObCollationType coll_type = static_cast<ObCollationType>(coll_type_int);
    return CS_TYPE_GB18030_2022_BIN <= coll_type && coll_type <= CS_TYPE_GB18030_2022_STROKE_CS;
  }
  
  static bool is_gb_charset(int64_t cs_type_int);
  static ObCharsetType charset_type_by_coll(ObCollationType coll_type);
  static int charset_name_by_coll(const ObString &coll_name, common::ObString &cs_name);
  static int charset_name_by_coll(ObCollationType coll_type, common::ObString &cs_name);
  static int calc_collation(const ObCollationLevel level1,
                            const ObCollationType type1,
                            const ObCollationLevel level2,
                            const ObCollationType type2,
                            ObCollationLevel &res_level,
                            ObCollationType &res_type);
  static int result_collation(const ObCollationLevel level1,
                              const ObCollationType type1,
                              const ObCollationLevel level2,
                              const ObCollationType type2,
                              ObCollationLevel &res_level,
                              ObCollationType &res_type);
  static int aggregate_collation(const ObCollationLevel level1,
                                 const ObCollationType type1,
                                 const ObCollationLevel level2,
                                 const ObCollationType type2,
                                 ObCollationLevel &res_level,
                                 ObCollationType &res_type);
  static bool is_bin_sort(ObCollationType collation_type);
  static ObCollationType get_bin_collation(const ObCharsetType charset_type);
  static int first_valid_char(const ObCollationType collation_type,
                              const char *buf,
                              const int64_t buf_size,
                              int64_t &char_len);

  static int last_valid_char(const ObCollationType collation_type,
                             const char *buf,
                             const int64_t buf_size,
                             int64_t &char_len);

  static ObCharsetType get_default_charset();
  static ObCollationType get_default_collation_oracle(ObCharsetType charset_type);
  static ObCollationType get_default_collation(ObCharsetType charset_type);
  static int get_default_collation(ObCharsetType charset_type, ObCollationType &coll_type);
  static int get_default_collation(const ObCollationType &in, ObCollationType &out);
  static ObCollationType get_system_collation();
  static bool is_default_collation(ObCollationType type);
  static bool is_default_collation(ObCharsetType charset_type, ObCollationType coll_type);
  static const char* get_default_charset_name()
  { return ObCharset::charset_name(ObCharset::get_default_charset()); }
  static const char* get_default_collation_name()
  { return ObCharset::collation_name(ObCharset::get_default_collation(ObCharset::get_default_charset())); }
  static void get_charset_wrap_arr(const ObCharsetWrapper *&charset_wrap_arr, int64_t &charset_wrap_arr_len)
  { charset_wrap_arr = charset_wrap_arr_; charset_wrap_arr_len = CHARSET_WRAPPER_COUNT; }
  static void get_collation_wrap_arr(const ObCollationWrapper *&collation_wrap_arr, int64_t &collation_wrap_arr_len)
  { collation_wrap_arr = collation_wrap_arr_; collation_wrap_arr_len = COLLATION_WRAPPER_COUNT; }
  static int check_and_fill_info(ObCharsetType &charset_type, ObCollationType &collation_type);

  static int strcmp(const ObCollationType collation_type,
                    const ObString &l_str,
                    const ObString &r_str);
  //when invoke this, if ObString a = "134";  this func will core; so avoid passing src as a style
  static size_t casedn(const ObCollationType collation_type, ObString &src);
  static bool case_insensitive_equal(const ObString &one,
                                     const ObString &another,
                                     const ObCollationType &collation_type = CS_TYPE_UTF8MB4_GENERAL_CI);
  static uint64_t hash(const ObCollationType collation_type, const ObString &str,
                       uint64_t seed = 0, hash_algo hash_algo = NULL);
  static bool case_mode_equal(const ObNameCaseMode mode,
                              const ObString &one,
                              const ObString &another);
  static bool is_space(const ObCollationType collation_type, char c);
  static bool is_graph(const ObCollationType collation_type, char c);
  static bool usemb(const ObCollationType collation_type);
  static int is_mbchar(const ObCollationType collation_type, const char *str, const char *end);
  static const ObCharsetInfo *get_charset(const ObCollationType coll_type);
  static int get_mbmaxlen_by_coll(const ObCollationType collation_type, int64_t &mbmaxlen);

  static int fit_string(const ObCollationType collation_type,
                        const char *str,
                        const int64_t str_len,
                        const int64_t len_limit_in_byte,
                        int64_t &byte_num,
                        int64_t &char_num);
  static int charset_convert(const ObCollationType from_type,
                             const char *from_str,
                             const uint32_t from_len,
                             const ObCollationType to_type,
                             char *to_str,
                             uint32_t to_len,
                             uint32_t &result_len,
                             bool trim_incomplete_tail = true);
public:
  static const int64_t VALID_COLLATION_TYPES = 3;
private:
  static bool is_argument_valid(const ObCharsetInfo *charset_info, const char *str, int64_t str_len);
  static bool is_argument_valid(const ObCollationType collation_type, const char *str1, int64_t str_len1, const char *str2, int64_t str_len2);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCharset);
private:
  static const ObCharsetWrapper charset_wrap_arr_[CHARSET_WRAPPER_COUNT];
  static const ObCollationWrapper collation_wrap_arr_[COLLATION_WRAPPER_COUNT];
  static void *charset_arr[CS_TYPE_MAX];   // CHARSET_INFO *
  static ObCharsetType default_charset_type_;
  static ObCollationType default_collation_type_;
};

class ObStringScanner
{
public:
  enum {
    IGNORE_INVALID_CHARACTER = 1<<0,
  };
  ObStringScanner(const ObString &str, common::ObCollationType collation_type, uint64_t flags = 0)
    : origin_str_(str), str_(str), collation_type_(collation_type), flags_(flags)
  {}
  int next_character(ObString &encoding_value, int32_t &unicode_value);
  bool next_character(ObString &encoding_value, int32_t &unicode_value, int &ret);
  ObString get_remain_str() { return str_; }
  void forward_bytes(int64_t n) { str_ += n; }
  TO_STRING_KV(K_(str), K_(collation_type));
private:
  const ObString &origin_str_;
  ObString str_;
  common::ObCollationType collation_type_;
  uint64_t flags_;
};

// to_string adapter
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len,
                              int64_t &pos, const ObCollationType &t)
{
  return databuff_printf(buf, buf_len, pos, "\"%s\"", ObCharset::collation_name(t));
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                  const bool with_comma, const ObCollationType &t)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%s\""), key, ObCharset::collation_name(t));
}
} // namespace common
} // namespace oceanbase

#endif /* OCEANBASE_CHARSET_H_ */
