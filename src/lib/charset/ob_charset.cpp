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

#define USING_LOG_PREFIX LIB_CHARSET
#include "ob_charset.h"
#include "lib/utility/serialization.h"
#include "lib/ob_proxy_worker.h"
#include "lib/allocator/ob_malloc.h"

#define PLANE_SIZE 0x100
#define PLANE_NUM 0x100
#define PLANE_NUMBER(x) (((x) >> 8) % PLANE_NUM)

namespace oceanbase
{
namespace common
{
const ObCharsetWrapper ObCharset::charset_info_arr_[CHARSET_INFO_COUNT] =
{
  {CHARSET_BINARY, "Binary pseudo charset", CS_TYPE_BINARY, 1},
  {CHARSET_UTF8MB4, "UTF-8 Unicode", CS_TYPE_UTF8MB4_GENERAL_CI, 4},
  {CHARSET_GBK, "GBK charset", CS_TYPE_GBK_CHINESE_CI, 2},
  {CHARSET_UTF16, "UTF-16 Unicode", CS_TYPE_UTF16_GENERAL_CI, 2},
  {CHARSET_GB18030, "GB18030 charset", CS_TYPE_GB18030_CHINESE_CI, 4},
  {CHARSET_LATIN1, "cp1252 West European", CS_TYPE_LATIN1_SWEDISH_CI, 1},
  {CHARSET_GB18030_2022, "GB18030-2022 charset", CS_TYPE_GB18030_2022_PINYIN_CI, 4},
  {CHARSET_ASCII, "US ASCII", CS_TYPE_ASCII_GENERAL_CI, 1},
  {CHARSET_TIS620, "TIS620 Thai", CS_TYPE_TIS620_THAI_CI, 1},
};

const ObCollationWrapper ObCharset::collation_info_arr_[COLLATION_INFO_COUNT] =
{
  {CS_TYPE_UTF8MB4_GENERAL_CI, CHARSET_UTF8MB4, CS_TYPE_UTF8MB4_GENERAL_CI, true, true, 1},
  {CS_TYPE_UTF8MB4_BIN, CHARSET_UTF8MB4, CS_TYPE_UTF8MB4_BIN, false, true, 1},
  {CS_TYPE_BINARY, CHARSET_BINARY, CS_TYPE_BINARY, true, true, 1},
  {CS_TYPE_GBK_CHINESE_CI, CHARSET_GBK, CS_TYPE_GBK_CHINESE_CI, true, true, 1},
  {CS_TYPE_GBK_BIN, CHARSET_GBK, CS_TYPE_GBK_BIN, false, true, 1},
  {CS_TYPE_UTF16_GENERAL_CI, CHARSET_UTF16, CS_TYPE_UTF16_GENERAL_CI, true, true, 1},
  {CS_TYPE_UTF16_BIN, CHARSET_UTF16, CS_TYPE_UTF16_BIN, false, true, 1},
  {CS_TYPE_UTF8MB4_UNICODE_CI, CHARSET_UTF8MB4, CS_TYPE_UTF8MB4_UNICODE_CI, false, true, 1},
  {CS_TYPE_UTF16_UNICODE_CI, CHARSET_UTF16, CS_TYPE_UTF16_UNICODE_CI, false, true, 1},
  {CS_TYPE_GB18030_CHINESE_CI, CHARSET_GB18030, CS_TYPE_GB18030_CHINESE_CI, true, true, 1},
  {CS_TYPE_GB18030_BIN, CHARSET_GB18030, CS_TYPE_GB18030_BIN, false, true, 1},
  {CS_TYPE_LATIN1_SWEDISH_CI, CHARSET_LATIN1, CS_TYPE_LATIN1_SWEDISH_CI,true, true, 1},
  {CS_TYPE_LATIN1_BIN, CHARSET_LATIN1, CS_TYPE_LATIN1_BIN,false, true, 1},
  {CS_TYPE_GB18030_2022_BIN, CHARSET_GB18030_2022, CS_TYPE_GB18030_2022_BIN, false, true, 1},
  {CS_TYPE_GB18030_2022_PINYIN_CI, CHARSET_GB18030_2022, CS_TYPE_GB18030_2022_PINYIN_CI, true, true, 1},
  {CS_TYPE_GB18030_2022_PINYIN_CS, CHARSET_GB18030_2022, CS_TYPE_GB18030_2022_PINYIN_CS, false, true, 1},
  {CS_TYPE_GB18030_2022_RADICAL_CI, CHARSET_GB18030_2022, CS_TYPE_GB18030_2022_RADICAL_CI, false, true, 1},
  {CS_TYPE_GB18030_2022_RADICAL_CS, CHARSET_GB18030_2022, CS_TYPE_GB18030_2022_RADICAL_CS, false, true, 1},
  {CS_TYPE_GB18030_2022_STROKE_CI, CHARSET_GB18030_2022, CS_TYPE_GB18030_2022_STROKE_CI, false, true, 1},
  {CS_TYPE_GB18030_2022_STROKE_CS, CHARSET_GB18030_2022, CS_TYPE_GB18030_2022_STROKE_CS, false, true, 1},
  {CS_TYPE_UTF8MB4_CROATIAN_CI, CHARSET_UTF8MB4, CS_TYPE_UTF8MB4_CROATIAN_CI, false, true, 8},
  {CS_TYPE_UTF8MB4_UNICODE_520_CI, CHARSET_UTF8MB4, CS_TYPE_UTF8MB4_UNICODE_520_CI, false, true, 8},
  {CS_TYPE_UTF8MB4_CZECH_CI, CHARSET_UTF8MB4, CS_TYPE_UTF8MB4_CZECH_CI, false, true, 8},
  {CS_TYPE_ASCII_GENERAL_CI, CHARSET_ASCII, CS_TYPE_ASCII_GENERAL_CI,true, true, 1},
  {CS_TYPE_ASCII_BIN, CHARSET_ASCII, CS_TYPE_ASCII_BIN,false, true, 1},
  {CS_TYPE_TIS620_THAI_CI, CHARSET_TIS620, CS_TYPE_TIS620_THAI_CI,true, true, 1},
  {CS_TYPE_TIS620_BIN, CHARSET_TIS620, CS_TYPE_TIS620_BIN,false, true, 1},
  {CS_TYPE_UTF8MB4_0900_AI_CI, CHARSET_UTF8MB4, CS_TYPE_UTF8MB4_0900_AI_CI, false, true, 1},
};

void *ObCharset::charset_arr[CS_TYPE_MAX] = {
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 0 ~ 7
  &ob_charset_latin1, NULL, NULL, NULL, NULL, NULL, NULL, NULL,   // 8
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 16
  NULL, NULL, NULL, NULL, &ob_charset_gbk_chinese_ci,             // 24
                                NULL, NULL, NULL,                 // 29
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 32
  NULL, NULL, NULL, NULL, NULL,                                   // 40
                                &ob_charset_utf8mb4_general_ci,   // 45
                                      &ob_charset_utf8mb4_bin,    // 46
                                      &ob_charset_latin1_bin,     // 47
  NULL, NULL, NULL, NULL, NULL, NULL,                             // 48
                                     &ob_charset_utf16_general_ci,// 54
                                     &ob_charset_utf16_bin,       // 55
  NULL, NULL, NULL, NULL, NULL, NULL, NULL,                       // 56
                                            &ob_charset_bin,      // 63
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 64
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 72
  NULL, NULL, NULL, NULL, NULL, NULL, NULL,                       // 80
                                           &ob_charset_gbk_bin,   // 87
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 88
  NULL, NULL, NULL, NULL, NULL,                                   // 96
                                &ob_charset_utf16_unicode_ci,     // 101
                                NULL, NULL,                       // 102
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 104
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 112
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 120
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 128
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 136
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 144
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 152
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 160
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 168
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 176
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 184
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 192
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 200
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 208
  &ob_charset_gb18030_2022_bin,        &ob_charset_gb18030_2022_pinyin_ci, // 216
  &ob_charset_gb18030_2022_pinyin_cs,  &ob_charset_gb18030_2022_radical_ci,// 218
  &ob_charset_gb18030_2022_radical_cs, &ob_charset_gb18030_2022_stroke_ci, // 220
  &ob_charset_gb18030_2022_stroke_cs, NULL,                       // 222
  &ob_charset_utf8mb4_unicode_ci,                                 // 224
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 225
  NULL, &ob_charset_utf8mb4_czech_uca_ci/*234*/, NULL, NULL,      // 233
  NULL, NULL, &ob_charset_utf8mb4_czech_uca_ci, NULL,             // 237
  NULL, NULL, NULL, NULL, &ob_charset_utf8mb4_croatian_uca_ci,    // 241
  &ob_charset_utf8mb4_unicode_520_ci, NULL,                       // 246
  &ob_charset_gb18030_chinese_ci,                                 // 248
  &ob_charset_gb18030_bin,                                        // 249
};

double ObCharset::strntod(const char *str,
                          size_t str_len,
                          char **endptr,
                          int *err)
{
  ObCharsetInfo *cs = &ob_charset_bin;
  double result = 0.0;
  if (is_argument_valid(cs, str, str_len)) {
    //cs and cs->coll and cs->cset have been checked in is_argument_valid already
    //so no need to do it again. why bother yourself ? Just use it !
    result = cs->cset->strntod(cs, const_cast<char *>(str), str_len, endptr, err);
  }
  return result;
}

int64_t ObCharset::strntoll(const char *str,
                            size_t str_len,
                            int base,
                            int *err)
{
  ObCharsetInfo *cs = &ob_charset_bin;
  char *end_ptr = NULL;
  int64_t result = 0;
  if (is_argument_valid(cs, str, str_len)) {
    //cs and cs->coll and cs->cset have been checked in is_argument_valid already
    //so no need to do it again. why bother yourself ? Just use it !
    result = cs->cset->strntoll(cs, str, str_len, base, &end_ptr, err);
  }
  return result;
}

uint64_t ObCharset::strntoull(const char *str,
                              size_t str_len,
                              int base,
                              int *err)
{
  ObCharsetInfo *cs = &ob_charset_bin;
  char *end_ptr = NULL;
  uint64_t result = 0;
  if (is_argument_valid(cs, str, str_len)) {
    //cs and cs->coll and cs->cset have been checked in is_argument_valid already
    //so no need to do it again. why bother yourself ? Just use it !
    result = cs->cset->strntoull(cs,
                             str,
                             str_len,
                             base,
                             &end_ptr,
                             err);
  }
  return result;
}
uint64_t ObCharset::strntoullrnd(const char *str,
                                 size_t str_len,
                                 int unsigned_fl,
                                 char **endptr,
                                 int *err)
{
  ObCharsetInfo *cs = &ob_charset_bin;
  uint64_t result = 0;
  if (is_argument_valid(cs, str, str_len)) {
    //cs and cs->coll and cs->cset have been checked in is_argument_valid already
    //so no need to do it again. why bother yourself ? Just use it !
    result = cs->cset->strntoull10rnd(cs,
                                  str,
                                  str_len,
                                  unsigned_fl,
                                  endptr,
                                  err);
  }
  return result;
}
size_t ObCharset::scan_str(const char *str,
                           const char *end,
                           int sq)
{
  ObCharsetInfo *cs = &ob_charset_bin;
  size_t result = 0;
  if (OB_ISNULL(str) || OB_ISNULL(end) || OB_ISNULL(cs)) {
    BACKTRACE(EDIAG, true, "invalid argument. str = %p, end = %p, cs = %p", str, end, cs);
  } else {
    result = cs->cset->scan(cs, str, end, sq);
  }
  return result;
}
uint32_t ObCharset::instr(ObCollationType cs_type,
                          const char *str1,
                          int64_t str1_len,
                          const char *str2,
                          int64_t str2_len)
{
  uint32_t result = 0;
  if (is_argument_valid(cs_type, str1, str1_len, str2, str2_len)) {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    ob_match_t m_match_t[2];
    uint nmatch = 1;
    //cs and cs->coll and cs->cset have been checked in is_argument_valid already
    //so no need to do it again. why bother yourself ? Just use it !
    uint m_ret = cs->coll->instr(cs, str1, str1_len, str2, str2_len, m_match_t, nmatch);
    if (0 == m_ret ) {
      result = 0;
    } else {
      result =  m_match_t[0].mb_len + 1;
    }
  }
  return result;
}

uint32_t ObCharset::locate(ObCollationType cs_type,
                const char *str1,
                int64_t str1_len,
                const char *str2,
                int64_t str2_len,
                int64_t pos)
{
  uint32_t result = 0;
  if (is_argument_valid(cs_type, str1, str1_len, str2, str2_len)) {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    int64_t start0 = pos - 1;
    int64_t start = start0;
    if (OB_UNLIKELY(start < 0 || start > str1_len)) {
      result = 0;
    } else {
      int ret = OB_SUCCESS;
      start = static_cast<int64_t>(charpos(cs_type, str1, str1_len, start, &ret));
      if (OB_FAIL(ret)) {
        result = 0;
      } else if (static_cast<int64_t>(start) + str2_len > str1_len) {
        result = 0;
      } else if (0 == str2_len) {
        result = static_cast<uint32_t>(start) + 1;
      } else {
        ob_match_t match_t;
        uint32_t nmatch = 1;
        //cs and cs->coll and cs->cset have been checked in is_argument_valid already
        //so no need to do it again. why bother yourself ? Just use it !
        uint32_t m_ret = cs->coll->instr(cs, str1 + start, str1_len - start, str2, str2_len, &match_t, nmatch);
        if (0 == m_ret) {
          result = 0;
        } else {
          result = match_t.mb_len + static_cast<uint32_t>(start0) + 1;
        }
      }
    }
  }
  return result;
}

int ObCharset::strcmp(ObCollationType cs_type,
                      const char *str1,
                      int64_t str1_len,
                      const char *str2,
                      int64_t str2_len)
{
  int result = 0;
  if (is_argument_valid(cs_type, str1, str1_len, str2, str2_len)) {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    //cs and cs->coll and cs->cset have been checked in is_argument_valid already
    //so no need to do it again. why bother yourself ? Just use it !
    const bool t_is_prefix = false;
    result = cs->coll->strnncoll(cs,
                              reinterpret_cast<const uchar *>(str1),
                              str1_len,
                              reinterpret_cast<const uchar *>(str2),
                              str2_len, t_is_prefix);
  }
  return result;
}

int ObCharset::strcmpsp(ObCollationType cs_type,
                        const char *str1,
                        int64_t str1_len,
                        const char *str2,
                        int64_t str2_len,
                        bool cmp_endspace)
{
  int result = 0;
  if (is_argument_valid(cs_type, str1, str1_len, str2, str2_len)) {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    //cs and cs->coll and cs->cset have been checked in is_argument_valid already
    //so no need to do it again. why bother yourself ? Just use it !
    result = cs->coll->strnncollsp(cs,
                                reinterpret_cast<const uchar *>(str1),
                                str1_len,
                                reinterpret_cast<const uchar *>(str2),
                                str2_len,
                                cmp_endspace);
  }
  return result;
}

size_t ObCharset::casedn(const ObCollationType cs_type, char *src, size_t src_len,
              char *dest, size_t dest_len)
{
  size_t size = 0;
  if (is_argument_valid(cs_type, src, src_len, dest, dest_len)) {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    //cs and cs->coll and cs->cset have been checked in is_argument_valid already
    //so no need to do it again. why bother yourself ? Just use it !
    size = cs->cset->casedn(cs, src, src_len, dest, dest_len);
  }
  return size;
}

size_t ObCharset::caseup(const ObCollationType cs_type, char *src, size_t src_len,
                         char *dest, size_t dest_len)
{
  size_t size = 0;
  if (is_argument_valid(cs_type, src, src_len, dest, dest_len)) {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    //cs and cs->coll and cs->cset have been checked in is_argument_valid already
    //so no need to do it again. why bother yourself ? Just use it !
    size = cs->cset->caseup(cs, src, src_len, dest, dest_len);
  }
  return size;
}

#define OB_MAX_WEIGHT  OB_MAX_VARCHAR_LENGTH
size_t ObCharset::sortkey(ObCollationType collation_type,
                          const char *str,
                          int64_t str_len,
                          char *key,
                          int64_t key_len,
                          bool &is_valid_unicode)
{
  size_t result = 0;
  ob_bool is_valid_unicode_tmp = 0;
  if (is_argument_valid(collation_type, str, str_len, key, key_len)) {
    //cs and cs->coll and cs->cset have been checked in is_argument_valid already
    //so no need to do it again. why bother yourself ? Just use it !
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);

    // compare_collation_free函数已经能自动过滤尾部空格了，sortkey中过滤空格的逻辑不需要了

    // is_valid_unicode参数的作用如下：
    // 以一个例子说明，待比较的字符串为：
    //
    // 第一个字符串：0x2c 0x80
    // 第二个字符串：0x2c 0x80 0x20
    //
    // 如果不采用sortkey转换后的字符串比较，会认为0x80及之后的字符为非法的unicode字符，对这之后的字符串采用二进制比较，则认为第二个字符串更大。
    //
    // 而采用sortkey转换后的字符串，则在碰到0x80非法字符之后，就停止转换，导致认为比较结果相等。
    // 修复方案：
    //
    // 对于有非法字符的unicode字符串，采用原生的不转换sortkey的方式进行比较。
    result = cs->coll->strnxfrm(cs,
                                reinterpret_cast<uchar *>(key),
                                key_len,
                                OB_MAX_WEIGHT,
                                reinterpret_cast<const uchar *>(str),
                                str_len,
                                0,
                                &is_valid_unicode_tmp);
    is_valid_unicode = is_valid_unicode_tmp;
  }
  return result;
}

uint64_t ObCharset::hash(ObCollationType cs_type,
                         const char *str,
                         int64_t str_len,
                         uint64_t seed,
                         const bool calc_end_space,
                         hash_algo hash_algo)
{
  uint64_t ret = seed;
  if (is_argument_valid(cs_type, str, str_len, NULL, 0)) {
    // since hash_sort() of MY_COLLATION_HANDLER need two intergers, one for input and output as
    // result, the other only for input as random seed, so I find 0xc6a4a7935bd1e995 from
    // murmurhash64A(), U can also find similar usage too.

    //cs and cs->coll and cs->cset have been checked in is_argument_valid already
    //so no need to do it again. why bother yourself ? Just use it !
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    if (OB_ISNULL(cs->coll)) {
      LOG_EDIAG("unexpected error. invalid argument(s)", K(cs), K(cs->coll));
    } else {
      seed = 0xc6a4a7935bd1e995;
      cs->coll->hash_sort(cs, reinterpret_cast<const uchar *>(str), str_len,
                          &ret, &seed, calc_end_space, hash_algo);
    }
  }
  return ret;
}

int ObCharset::like_range(ObCollationType cs_type,
                          const ObString &like_str,
                          char escape,
                          char *min_str,
                          size_t *min_str_len,
                          char *max_str,
                          size_t *max_str_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(min_str) ||
                  OB_ISNULL(min_str_len) ||
                  OB_ISNULL(max_str) ||
                  OB_ISNULL(max_str_len) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected error. invalid argument(s)",
              K(ret),
              K(cs_type), K(ObCharset::charset_arr[cs_type]),
              K(max_str), K(max_str_len),
              K(min_str), K(min_str_len));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    static char w_one = '_';
    static char w_many = '%';
    size_t res_size = *min_str_len < *max_str_len ? *min_str_len : *max_str_len;
    if (OB_ISNULL(cs->coll)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("unexpected error. invalid argument(s)", K(cs), K(cs->coll));
    } else if (0 != cs->coll->like_range(cs,
                                  like_str.ptr(),
                                  like_str.length(),
                                  escape,
                                  w_one,
                                  w_many,
                                  res_size,
                                  min_str,
                                  max_str,
                                  min_str_len,
                                  max_str_len)) {
      ret = OB_EMPTY_RANGE;
    }
  }
  return ret;
}

size_t ObCharset::strlen_char(const ObCollationType cs_type,
                              const char *str,
                              int64_t str_len)
{
  size_t ret = 0;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    LOG_EDIAG("unexpected error. invalid argument(s)", K(cs_type), K(ObCharset::charset_arr[cs_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    if (OB_ISNULL(cs->cset)) {
      LOG_EDIAG("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
    } else {
      ret = cs->cset->numchars(cs, str, str + str_len);
    }
  }
  return ret;
}

size_t ObCharset::strlen_byte_no_sp(const ObCollationType cs_type,
                                    const char *str,
                                    int64_t str_len)
{
  size_t ret = 0;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    LOG_EDIAG("unexpected error. invalid argument(s)", K(cs_type), K(ObCharset::charset_arr[cs_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    if (OB_ISNULL(cs->cset)) {
      LOG_EDIAG("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
    } else {
      ret = cs->cset->lengthsp(cs, str, str_len);
    }
  }
  return ret;
}

int ObCharset::well_formed_len(ObCollationType cs_type, const char *str,
                           int64_t str_len, int64_t &well_formed_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected error. invalid argument(s)", K(cs_type), K(ObCharset::charset_arr[cs_type]));
  } else if (OB_UNLIKELY(NULL == str && 0 != str_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument, str is null  and  str_len is nonzero",
             K(str), K(str_len), K(ret));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    if (OB_ISNULL(cs->cset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
    } else {
      well_formed_len = cs->cset->well_formed_len(cs, str, str + str_len, UINT64_MAX, &ret);
    }
  }
  return ret;
}


int ObCharset::well_formed_len(ObCollationType cs_type, const char *str,
                           int64_t str_len, int64_t &well_formed_len, int32_t &well_formed_error)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected error. invalid argument(s)", K(cs_type), K(ObCharset::charset_arr[cs_type]));
  } else if (OB_UNLIKELY(NULL == str && 0 != str_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument, str is null  and  str_len is nonzero",
             K(str), K(str_len), K(ret));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    if (OB_ISNULL(cs->cset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
    } else {
      well_formed_len = cs->cset->well_formed_len(cs, str, str + str_len, UINT64_MAX, &well_formed_error);
    }
  }
  return ret;
}

// Be careful with this function. The return value may be out of range.
size_t ObCharset::charpos(const ObCollationType cs_type,
                          const char *str,
                          const int64_t str_len,
                          const int64_t length,
                          int *ret)
{
  size_t res_pos = 0;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    LOG_EDIAG("unexpected error. invalid argument(s)", K(cs_type), K(ObCharset::charset_arr[cs_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    if (OB_ISNULL(cs->cset)) {
      LOG_EDIAG("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
    } else {
      res_pos = cs->cset->charpos(cs, str, str + str_len, length);
      if (res_pos > str_len) {
        res_pos = str_len;
        if (OB_NOT_NULL(ret)) {
          *ret = OB_ERROR_OUT_OF_RANGE;
        }
      }
    }
  }
  return res_pos;
}

bool ObCharset::wildcmp(ObCollationType cs_type,
                       const ObString &str,
                       const ObString &wildstr,
                       int32_t escape, int32_t w_one, int32_t w_many)
{
  bool ret = false;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    LOG_EDIAG("unexpected error. invalid argument(s)", K(cs_type), K(ObCharset::charset_arr[cs_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    if (OB_ISNULL(cs->coll)) {
      LOG_EDIAG("unexpected error. invalid argument(s)", K(cs), K(cs->coll));
    } else {
      int tmp = cs->coll->wildcmp(cs, str.ptr(), str.ptr() + str.length(),
                                wildstr.ptr(), wildstr.ptr() + wildstr.length(),
                                escape, w_one, w_many);
      /*
      **	0 if matched
      **	-1 if not matched with wildcard
      **	 1 if matched with wildcard
      */
      ret = (0 == tmp);
    }
  }
  return ret;
}

int ObCharset::mb_wc(ObCollationType collation_type,
                     const char *mb,
                     const int64_t mb_size,
                     int32_t &length,
                     int32_t &wc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected error. invalid argument(s)",
              K(ret), K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    ob_wc_t my_wc;
    if (OB_ISNULL(cs->cset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
    } else {
      int tmp = cs->cset->mb_wc(cs, &my_wc, reinterpret_cast<const uchar*>(mb),
                            reinterpret_cast<const uchar*>(mb + mb_size));
      if (tmp <= 0) {
        ret = OB_ERROR;
      } else {
        ret = OB_SUCCESS;
        wc = static_cast<int32_t>(my_wc);
        length = static_cast<int32_t>(tmp);
      }
    }
  }
  return ret;
}

int ObCharset::wc_mb(ObCollationType collation_type, int32_t wc, char *buff, int32_t buff_len, int32_t &length)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID || collation_type >= CS_TYPE_MAX)
      || OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected error. invalid argument(s)", K(ret), K(collation_type));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    if (OB_ISNULL(cs) || OB_ISNULL(cs->cset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("unexpected error. invalid argument(s)", K(cs), K(ret));
    } else {
      int tmp = cs->cset->wc_mb(cs, wc, reinterpret_cast<uchar*>(buff),
              reinterpret_cast<uchar*>(buff + buff_len));
      if (tmp <= 0) {
        ret = OB_ERR_INCORRECT_STRING_VALUE;
      } else {
        ret = OB_SUCCESS;
        length = tmp;
      }
    }
  }
  return ret;
}


const char *ObCharset::charset_name(ObCharsetType cs_type)
{
  const char *ret_name = "invalid_type";
  switch(cs_type) {
  case CHARSET_BINARY: {
      ret_name = "binary";
      break;
    }
  case CHARSET_UTF8MB4: {
      ret_name = "utf8mb4";
      break;
  }
  case CHARSET_GBK: {
      ret_name = "gbk";
      break;
  }
  case CHARSET_UTF16: {
      ret_name = "utf16";
      break;
  }
  case CHARSET_GB18030: {
      ret_name = "gb18030";
      break;
  }
  case CHARSET_LATIN1: {
      ret_name = "latin1";
      break;
  }
  case CHARSET_GB18030_2022: {
    ret_name = "gb18030_2022";
    break;
  }
  case CHARSET_ASCII: {
    ret_name = "charset_ascii";
    break;
  }
  case CHARSET_TIS620: {
    ret_name = "charset_tis620";
    break;
  }
  default: {
      break;
    }
  }
  return ret_name;
}
const char *ObCharset::charset_name(ObCollationType coll_type)
{
  return charset_name(charset_type_by_coll(coll_type));
}

const char *ObCharset::collation_name(ObCollationType cs_type)
{
  ObCharsetInfo *cs = NULL;
  if (cs_type < CS_TYPE_MAX && cs_type >= CS_TYPE_INVALID) {
    cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
  }
  return (NULL == cs) ? "invalid_type" : cs->name;
}

int ObCharset::collation_name(ObCollationType coll_type, ObString &coll_name)
{
  int ret = OB_SUCCESS;
  ObCharsetInfo *coll = NULL;
  if (coll_type < CS_TYPE_MAX && coll_type >= CS_TYPE_INVALID) {
    coll = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[coll_type]);
  }
  if (OB_ISNULL(coll)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid collation type", K(ret), K(coll_type));
  } else {
    coll_name = ObString(coll->name);
  }
  return ret;
}

const char* ObCharset::collation_level(const ObCollationLevel cs_level)
{
  const char* ret = "unknown_collation_level";
  switch(cs_level) {
  case CS_LEVEL_EXPLICIT: {
      ret = "EXPLICIT";
      break;
    }
  case CS_LEVEL_NONE: {
      ret = "NONE";
      break;
    }
  case CS_LEVEL_IMPLICIT: {
      ret = "IMPLICIT";
      break;
    }
  case CS_LEVEL_SYSCONST: {
      ret = "SYSCONST";
      break;
    }
  case CS_LEVEL_COERCIBLE: {
      ret = "COERCIBLE";
      break;
    }
  case CS_LEVEL_NUMERIC: {
      ret = "NUMERIC";
      break;
    }
  case CS_LEVEL_IGNORABLE: {
      ret = "IGNORABLE";
      break;
    }
  case CS_LEVEL_INVALID: {
      ret = "INVALID";
      break;
    }
  default: {
      break;
    }
  }
  return ret;
}


ObCharsetType ObCharset::charset_type(const ObString &cs_name)
{
  ObCharsetType cs_type = CHARSET_INVALID;
  if (0 == cs_name.case_compare("utf8") || 0 == cs_name.case_compare("utf8mb3")) {
    // utf8是utf8mb4的别名
    cs_type = CHARSET_UTF8MB4;
  } else if (0 == cs_name.case_compare(ob_charset_utf8mb4_bin.csname)) {
    cs_type = CHARSET_UTF8MB4;
  } else if (0 == cs_name.case_compare(ob_charset_bin.csname)) {
    cs_type = CHARSET_BINARY;
  } else if (0 == cs_name.case_compare(ob_charset_gb18030_chinese_ci.csname)) {
    cs_type = CHARSET_GB18030;
  } else if (0 == cs_name.case_compare(ob_charset_gbk_chinese_ci.csname)) {
    cs_type = CHARSET_GBK;
  } else if (0 == cs_name.case_compare(ob_charset_utf16_general_ci.csname)) {
    cs_type = CHARSET_UTF16;
  } else if (0 == cs_name.case_compare(ob_charset_latin1.csname)) {
    cs_type = CHARSET_LATIN1;
  } else if (0 == cs_name.case_compare(ob_charset_latin1_bin.csname)) {
    cs_type = CHARSET_LATIN1;
  } else if (0 == cs_name.case_compare(ob_charset_gb18030_2022_bin.csname)) {
    cs_type = CHARSET_GB18030_2022;
  } else if (0 == cs_name.case_compare(ob_charset_ascii_bin.csname)) {
    cs_type = CHARSET_ASCII;
  } else if (0 == cs_name.case_compare(ob_charset_tis620_bin.csname)) {
    cs_type = CHARSET_TIS620;
  }
  return cs_type;
}

ObCharsetType ObCharset::charset_type(const char *cs_name)
{
  ObCharsetType ct = CHARSET_INVALID;
  if (OB_ISNULL(cs_name)) {
    LOG_EDIAG("unexpected error. invalid argument(s)",
              K(ct), K(cs_name), K(ct));
  } else {
    ObString cs_name_str = ObString::make_string(cs_name);
    ct = charset_type(cs_name_str);
  }
  return ct;
}

ObCollationType ObCharset::collation_type(const ObString &cs_name)
{
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (0 == cs_name.case_compare("utf8_bin")
      || 0 == cs_name.case_compare("utf8mb3_bin")) {
    cs_type = CS_TYPE_UTF8MB4_BIN;
  } else if (0 == cs_name.case_compare("utf8_general_ci")
             || 0 == cs_name.case_compare("utf8mb3_general_ci")) {
    cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  } else if (0 == cs_name.case_compare("utf8_unicode_ci")) {
    cs_type = CS_TYPE_UTF8MB4_UNICODE_CI;
  } else if (0 == cs_name.case_compare(ob_charset_utf16_unicode_ci.name)) {
    cs_type = CS_TYPE_UTF8MB4_0900_AI_CI;
  }  else if (0 == cs_name.case_compare(ob_charset_utf8mb4_bin.name)) {
    cs_type = CS_TYPE_UTF8MB4_BIN;
  } else if (0 == cs_name.case_compare(ob_charset_utf8mb4_general_ci.name)) {
    cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  } else if (0 == cs_name.case_compare(ob_charset_utf8mb4_unicode_ci.name)) {
    cs_type = CS_TYPE_UTF8MB4_UNICODE_CI;
  } else if (0 == cs_name.case_compare(ob_charset_bin.name)) {
    cs_type = CS_TYPE_BINARY;
  } else if (0 == cs_name.case_compare(ob_charset_gb18030_bin.name)) {
    cs_type = CS_TYPE_GB18030_BIN;
  } else if (0 == cs_name.case_compare(ob_charset_gb18030_chinese_ci.name)) {
    cs_type = CS_TYPE_GB18030_CHINESE_CI;
  } else if (0 == cs_name.case_compare(ob_charset_gbk_bin.name)) {
    cs_type = CS_TYPE_GBK_BIN;
  } else if (0 == cs_name.case_compare(ob_charset_gbk_chinese_ci.name)) {
    cs_type = CS_TYPE_GBK_CHINESE_CI;
  } else if (0 == cs_name.case_compare(ob_charset_utf16_bin.name)) {
    cs_type = CS_TYPE_UTF16_BIN;
  } else if (0 == cs_name.case_compare(ob_charset_utf16_general_ci.name)) {
    cs_type = CS_TYPE_UTF16_GENERAL_CI;
  } else if (0 == cs_name.case_compare(ob_charset_utf16_unicode_ci.name)) {
    cs_type = CS_TYPE_UTF16_UNICODE_CI;
  } else if (0 == cs_name.case_compare(ob_charset_latin1.name)) {
    cs_type = CS_TYPE_LATIN1_SWEDISH_CI;
  } else if (0 == cs_name.case_compare(ob_charset_latin1_bin.name)) {
    cs_type = CS_TYPE_LATIN1_BIN;
  } else if (0 == cs_name.case_compare(ob_charset_gb18030_2022_bin.name)) {
    cs_type = CS_TYPE_GB18030_2022_BIN;
  } else if (0 == cs_name.case_compare(ob_charset_gb18030_2022_pinyin_ci.name)) {
    cs_type = CS_TYPE_GB18030_2022_PINYIN_CI;
  } else if (0 == cs_name.case_compare(ob_charset_gb18030_2022_pinyin_cs.name)) {
    cs_type = CS_TYPE_GB18030_2022_PINYIN_CS;
  } else if (0 == cs_name.case_compare(ob_charset_gb18030_2022_radical_ci.name)) {
    cs_type = CS_TYPE_GB18030_2022_RADICAL_CI;
  } else if (0 == cs_name.case_compare(ob_charset_gb18030_2022_radical_cs.name)) {
    cs_type = CS_TYPE_GB18030_2022_RADICAL_CS;
  } else if (0 == cs_name.case_compare(ob_charset_gb18030_2022_stroke_ci.name)) {
    cs_type = CS_TYPE_GB18030_2022_STROKE_CI;
  } else if (0 == cs_name.case_compare(ob_charset_gb18030_2022_stroke_cs.name)) {
    cs_type = CS_TYPE_GB18030_2022_STROKE_CS;
  } else if (0 == cs_name.case_compare("utf8_croatian_ci")) {
    cs_type = CS_TYPE_UTF8MB4_CROATIAN_CI;
  } else if (0 == cs_name.case_compare(ob_charset_utf8mb4_croatian_uca_ci.name)) {
    cs_type = CS_TYPE_UTF8MB4_CROATIAN_CI;
  } else if (0 == cs_name.case_compare("utf8_unicode_520_ci")) {
    cs_type = CS_TYPE_UTF8MB4_UNICODE_520_CI;
  } else if (0 == cs_name.case_compare(ob_charset_utf8mb4_unicode_520_ci.name)) {
    cs_type = CS_TYPE_UTF8MB4_UNICODE_520_CI;
  } else if (0 == cs_name.case_compare("utf8_czech_ci")) {
    cs_type = CS_TYPE_UTF8MB4_CZECH_CI;
  } else if (0 == cs_name.case_compare(ob_charset_utf8mb4_czech_uca_ci.name)) {
    cs_type = CS_TYPE_UTF8MB4_CZECH_CI;
  } else if (0 == cs_name.case_compare(ob_charset_ascii_bin.name)) {
    cs_type = CS_TYPE_ASCII_BIN;
  } else if (0 == cs_name.case_compare(ob_charset_ascii.name)) {
    cs_type = CS_TYPE_ASCII_GENERAL_CI;
  } else if (0 == cs_name.case_compare(ob_charset_tis620_bin.name)) {
    cs_type = CS_TYPE_TIS620_BIN;
  } else if (0 == cs_name.case_compare(ob_charset_tis620_thai_ci.name)) {
    cs_type = CS_TYPE_TIS620_THAI_CI;
  }
  return cs_type;
}

ObCollationType ObCharset::collation_type(const char* cs_name)
{
  ObString cs_name_str = ObString::make_string(cs_name);
  return collation_type(cs_name_str);
}

bool ObCharset::is_valid_collation(ObCharsetType cs_type, ObCollationType coll_type)
{
  bool ret = false;
  if (CHARSET_UTF8MB4 == cs_type) {
    if (CS_TYPE_UTF8MB4_BIN == coll_type
        || CS_TYPE_UTF8MB4_GENERAL_CI == coll_type
        || CS_TYPE_UTF8MB4_UNICODE_CI == coll_type
        || CS_TYPE_UTF8MB4_CROATIAN_CI == coll_type
        || CS_TYPE_UTF8MB4_UNICODE_520_CI == coll_type
        || CS_TYPE_UTF8MB4_CZECH_CI == coll_type
        || CS_TYPE_UTF8MB4_0900_AI_CI == coll_type) {
      ret = true;
    }
  } else if (CHARSET_BINARY == cs_type
             && CS_TYPE_BINARY == coll_type) {
    ret = true;
  } else if (CHARSET_GB18030 == cs_type) {
    if (CS_TYPE_GB18030_BIN == coll_type
        || CS_TYPE_GB18030_CHINESE_CI == coll_type) {
      ret = true;
    }
  } else if (CHARSET_GBK == cs_type) {
    if (CS_TYPE_GBK_BIN == coll_type
        || CS_TYPE_GBK_CHINESE_CI == coll_type) {
      ret = true;
    }
  } else if (CHARSET_UTF16 == cs_type) {
    if (CS_TYPE_UTF16_BIN == coll_type
        || CS_TYPE_UTF16_GENERAL_CI == coll_type
        || CS_TYPE_UTF16_UNICODE_CI == coll_type) {
      ret = true;
    }
  } else if (CHARSET_LATIN1 == cs_type) {
    if (CS_TYPE_LATIN1_BIN == coll_type
        || CS_TYPE_LATIN1_SWEDISH_CI == coll_type) {
      ret = true;
    }
  } else if (CHARSET_GB18030_2022 == cs_type) {
    ret = is_gb18030_2022(coll_type);
  } else if (CHARSET_ASCII == cs_type) {
    if (CS_TYPE_ASCII_GENERAL_CI == coll_type || CS_TYPE_ASCII_BIN == coll_type) {
      ret = true;
    }
  } else if (CHARSET_TIS620 == cs_type) {
    if (CS_TYPE_TIS620_THAI_CI == coll_type || CS_TYPE_TIS620_BIN == coll_type) {
      ret = true;
    }
  }
  return ret;
}

bool ObCharset::is_valid_collation(int64_t coll_type_int)
{
  ObCollationType coll_type = static_cast<ObCollationType>(coll_type_int);
  return CS_TYPE_UTF8MB4_GENERAL_CI == coll_type
      || CS_TYPE_UTF8MB4_BIN == coll_type
      || CS_TYPE_UTF8MB4_UNICODE_CI == coll_type
      || CS_TYPE_BINARY == coll_type
      || CS_TYPE_GB18030_BIN == coll_type
      || CS_TYPE_GB18030_CHINESE_CI == coll_type
      || CS_TYPE_GBK_BIN == coll_type
      || CS_TYPE_GBK_CHINESE_CI == coll_type
      || CS_TYPE_UTF16_BIN == coll_type
      || CS_TYPE_UTF16_GENERAL_CI == coll_type
      || CS_TYPE_UTF16_UNICODE_CI == coll_type
      || CS_TYPE_LATIN1_BIN == coll_type
      || CS_TYPE_LATIN1_SWEDISH_CI == coll_type
      || is_gb18030_2022(coll_type)
      || CS_TYPE_UTF8MB4_0900_AI_CI == coll_type
      || CS_TYPE_UTF8MB4_CROATIAN_CI == coll_type
      || CS_TYPE_UTF8MB4_UNICODE_520_CI == coll_type
      || CS_TYPE_UTF8MB4_CZECH_CI == coll_type
      || CS_TYPE_ASCII_GENERAL_CI == coll_type
      || CS_TYPE_ASCII_BIN == coll_type
      || CS_TYPE_TIS620_THAI_CI == coll_type
      || CS_TYPE_TIS620_BIN == coll_type
      || (CS_TYPE_EXTENDED_MARK < coll_type && coll_type < CS_TYPE_MAX);
}

bool ObCharset::is_valid_charset(int64_t cs_type_int)
{
  ObCharsetType cs_type = static_cast<ObCharsetType>(cs_type_int);
  return CHARSET_BINARY == cs_type || CHARSET_UTF8MB4 == cs_type || CHARSET_GB18030 == cs_type
         || CHARSET_GBK == cs_type || CHARSET_UTF16 == cs_type || CHARSET_LATIN1 == cs_type
         || CHARSET_GB18030_2022 == cs_type || CHARSET_ASCII == cs_type
         || CHARSET_TIS620 == cs_type;
}

bool is_gb_charset(int64_t cs_type_int)
{
  ObCharsetType charset_type = static_cast<ObCharsetType>(cs_type_int);
  return CHARSET_GBK == charset_type
    || CHARSET_GB18030 == charset_type
    || CHARSET_GB18030_2022 == charset_type;
}

ObCharsetType ObCharset::charset_type_by_coll(ObCollationType coll_type)
{
  ObCharsetType type = CHARSET_INVALID;
  switch(coll_type) {
  case CS_TYPE_UTF8MB4_GENERAL_CI:
    //fall through
  case CS_TYPE_UTF8MB4_BIN:
  case CS_TYPE_UTF8MB4_UNICODE_CI:
  case CS_TYPE_UTF8MB4_CROATIAN_CI:
  case CS_TYPE_UTF8MB4_UNICODE_520_CI:
  case CS_TYPE_UTF8MB4_CZECH_CI:
  case CS_TYPE_UTF8MB4_0900_AI_CI: {
      type = CHARSET_UTF8MB4;
      break;
    }
  case CS_TYPE_BINARY: {
      type = CHARSET_BINARY;
      break;
    }
  case CS_TYPE_GB18030_BIN:
  case CS_TYPE_GB18030_CHINESE_CI: {
      type = CHARSET_GB18030;
      break;
  }
  case CS_TYPE_GBK_BIN:
  case CS_TYPE_GBK_CHINESE_CI: {
      type = CHARSET_GBK;
      break;
  }
  case CS_TYPE_UTF16_BIN:
  case CS_TYPE_UTF16_GENERAL_CI:
  case CS_TYPE_UTF16_UNICODE_CI: {
      type = CHARSET_UTF16;
      break;
  }
  case CS_TYPE_LATIN1_SWEDISH_CI:
  case CS_TYPE_LATIN1_BIN: {
      type = CHARSET_LATIN1;
      break;
  }
  case CS_TYPE_GB18030_2022_BIN:
  case CS_TYPE_GB18030_2022_PINYIN_CI:
  case CS_TYPE_GB18030_2022_PINYIN_CS:
  case CS_TYPE_GB18030_2022_RADICAL_CI:
  case CS_TYPE_GB18030_2022_RADICAL_CS:
  case CS_TYPE_GB18030_2022_STROKE_CI:
  case CS_TYPE_GB18030_2022_STROKE_CS:
  case CS_TYPE_GB18030_2022_ZH_0900_AS_CS:
  case CS_TYPE_GB18030_2022_ZH2_0900_AS_CS:
  case CS_TYPE_GB18030_2022_ZH3_0900_AS_CS: {
      type = CHARSET_GB18030_2022;
      break;
  }
  case CS_TYPE_ASCII_GENERAL_CI:
  case CS_TYPE_ASCII_BIN:
  case CS_TYPE_ASCII_ZH_0900_AS_CS:
  case CS_TYPE_ASCII_ZH2_0900_AS_CS:
  case CS_TYPE_ASCII_ZH3_0900_AS_CS: {
    type = CHARSET_ASCII;
    break;
  }
  case CS_TYPE_TIS620_THAI_CI:
  case CS_TYPE_TIS620_BIN:
  case CS_TYPE_TIS620_ZH_0900_AS_CS:
  case CS_TYPE_TIS620_ZH2_0900_AS_CS:
  case CS_TYPE_TIS620_ZH3_0900_AS_CS: {
    type = CHARSET_TIS620;
    break;
  }
  default: {
      break;
    }
  }
  return type;
}

int ObCharset::charset_name_by_coll(const ObString &coll_name, ObString &cs_name)
{
  int ret = OB_SUCCESS;
  ObCollationType coll_type = collation_type(coll_name);
  if (OB_UNLIKELY(CS_TYPE_INVALID == coll_type)) {
    ret = OB_ERR_UNKNOWN_COLLATION;
    LOG_WDIAG("invalid collation type", K(ret), K(coll_name));
  } else if (OB_FAIL(charset_name_by_coll(coll_type, cs_name))) {
    LOG_WDIAG("fail to get charset type by collation type", K(ret), K(coll_type), K(coll_name));
  }
  return ret;
}

int ObCharset::charset_name_by_coll(ObCollationType coll_type, ObString &cs_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(CS_TYPE_INVALID == coll_type)) {
    ret = OB_ERR_UNKNOWN_COLLATION;
    LOG_WDIAG("invalid collation type", K(ret), K(coll_type));
  } else {
    ObCharsetType cs_type = charset_type_by_coll(coll_type);
    if (OB_UNLIKELY(CHARSET_INVALID == cs_type)) {
      ret = OB_ERR_UNKNOWN_CHARSET;
      LOG_WDIAG("has no charset type of this collation type", K(ret), K(coll_type));
    } else {
      ObString tmp_cs_name = ObString(charset_name(cs_type));
      if (OB_UNLIKELY(tmp_cs_name == "invalid_type")) {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("charset str is invalid_type", K(ret), K(cs_type), K(coll_type));
      } else {
        cs_name = tmp_cs_name;
      }
    }
  }
  return ret;
}

int ObCharset::calc_collation(
    const ObCollationLevel level1,
    const ObCollationType type1,
    const ObCollationLevel level2,
    const ObCollationType type2,
    ObCollationLevel &res_level,
    ObCollationType &res_type)
{
  return ObCharset::result_collation(level1, type1, level2, type2, res_level, res_type);
}

int ObCharset::result_collation(
    const ObCollationLevel level1,
    const ObCollationType type1,
    const ObCollationLevel level2,
    const ObCollationType type2,
    ObCollationLevel &res_level,
    ObCollationType &res_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(CS_LEVEL_INVALID == level1
      || CS_LEVEL_INVALID == level2
      || CS_TYPE_INVALID == type1
      || CS_TYPE_INVALID == type2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid collation level or type", K(level1), K(type1), K(level2), K(type2));
  } else if (level1 == level2) {
    if (CS_LEVEL_EXPLICIT == level1 && type1 != type2) {
      // ERROR 1267 (HY000): Illegal mix of collations (utf8_general_ci,EXPLICIT) and (utf8_bin,EXPLICIT) for operation '='
      ret = OB_CANT_AGGREGATE_2COLLATIONS;
      LOG_USER_ERROR(ret);
    } else {
      // just consider two collations: bin & general_ci.
      // we must change the code below if we need to support more collations.
      res_level = level1;
      res_type = (type1 == type2) ? type1 : CS_TYPE_UTF8MB4_BIN;
    }
  } else if (level1 < level2) {
    res_level = level1;
    res_type = type1;
  } else {
    res_level = level2;
    res_type = type2;
  }
  return ret;
}

/** note from mysql:
  Aggregate two collations together taking
  into account their coercibility (aka derivation):.

  0 == DERIVATION_EXPLICIT  - an explicitly written COLLATE clause @n
  1 == DERIVATION_NONE      - a mix of two different collations @n
  2 == DERIVATION_IMPLICIT  - a column @n
  3 == DERIVATION_COERCIBLE - a string constant.

  The most important rules are:
  -# If collations are the same:
  chose this collation, and the strongest derivation.
  -# If collations are different:
  - Character sets may differ, but only if conversion without
  data loss is possible. The caller provides flags whether
  character set conversion attempts should be done. If no
  flags are substituted, then the character sets must be the same.
  Currently processed flags are:
  MY_COLL_ALLOW_SUPERSET_CONV  - allow conversion to a superset
  MY_COLL_ALLOW_COERCIBLE_CONV - allow conversion of a coercible value
  - two EXPLICIT collations produce an error, e.g. this is wrong:
  CONCAT(expr1 collate latin1_swedish_ci, expr2 collate latin1_german_ci)
  - the side with smaller derivation value wins,
  i.e. a column is stronger than a string constant,
  an explicit COLLATE clause is stronger than a column.
  - if derivations are the same, we have DERIVATION_NONE,
  we'll wait for an explicit COLLATE clause which possibly can
  come from another argument later: for example, this is valid,
  but we don't know yet when collecting the first two arguments:
     @code
       CONCAT(latin1_swedish_ci_column,
              latin1_german1_ci_column,
              expr COLLATE latin1_german2_ci)
  @endcode
*/
// We consider only two charsets(binary and utf8mb4), so the rule is simpler. Especially,
// res_level can not be CS_LEVEL_NONE.
int ObCharset::aggregate_collation(
    const ObCollationLevel level1,
    const ObCollationType type1,
    const ObCollationLevel level2,
    const ObCollationType type2,
    ObCollationLevel &res_level,
    ObCollationType &res_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(CS_LEVEL_INVALID == level1
      || CS_LEVEL_INVALID == level2
      || CS_TYPE_INVALID == type1
      || CS_TYPE_INVALID == type2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("invalid collation level or type",
              K(ret), K(level1), K(type1), K(level2), K(type2));
  } else {
    if (charset_type_by_coll(type1) != charset_type_by_coll(type2)) {
      /*
         We do allow to use binary strings (like BLOBS)
         together with character strings.
         Binaries have more precedence than a character
         string of the same derivation.
      */
      if (CS_TYPE_BINARY == type1) {
        if (level1 <= level2) {
          res_level = level1;
          res_type = type1;
        } else {
          res_level = level2;
          res_type = type2;
        }
      } else if (CS_TYPE_BINARY == type2) {
        if (level2 <= level1) {
          res_level = level2;
          res_type = type2;
        } else {
          res_level = level1;
          res_type = type1;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("Unexpected charset", K(type1), K(type2));
      }
    } else {
      if (level1 < level2) {
        res_type = type1;
        res_level = level1;
      } else if (level2 < level1) {
        res_type = type2;
        res_level = level2;
      } else {  // level1 == level2
        if (type1 == type2) {
          res_type = type1;
          res_level = level1;
        } else {
          // utf8mb4_general_ci vs utf8mb4_bin
          if (OB_UNLIKELY(type1 != CS_TYPE_UTF8MB4_BIN && type1 != CS_TYPE_UTF8MB4_GENERAL_CI) ||
              OB_UNLIKELY(type2 != CS_TYPE_UTF8MB4_BIN && type2 != CS_TYPE_UTF8MB4_GENERAL_CI)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid collation level or type",
                      K(ret), K(level1), K(type1), K(level2), K(type2));
          } else if (level1 == CS_LEVEL_EXPLICIT) {
            ret = OB_CANT_AGGREGATE_2COLLATIONS;
            // ERROR 1267 (HY000): Illegal mix of collations (utf8_general_ci,EXPLICIT) and (utf8_bin,EXPLICIT) for operation '='
            LOG_USER_ERROR(ret);
          } else {
            // utf8mb4_bin is prefer to utf8_XXX
            res_type = CS_TYPE_UTF8MB4_BIN;
            res_level = (CS_TYPE_UTF8MB4_BIN == type1) ? level1 : level2;
          }
        }
      }
    }
  }
  return ret;
}

bool ObCharset::is_bin_sort(ObCollationType cs_type)
{
  bool ret = false;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    LOG_EDIAG("unexpected error. invalid argument(s)",
              K(ret), K(cs_type), K(ObCharset::charset_arr[cs_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    ret = (0 != (cs->state & OB_CS_BINSORT));
  }
  return ret;
}

ObCharsetType ObCharset::default_charset_type_ = CHARSET_UTF8MB4;
ObCollationType ObCharset::default_collation_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;

ObCharsetType ObCharset::get_default_charset()
{
  return ObCharset::default_charset_type_;
}

ObCollationType ObCharset::get_default_collation(ObCharsetType cs_type)
{
  ObCollationType coll_type = CS_TYPE_INVALID;
  switch(cs_type) {
  case CHARSET_UTF8MB4: {
      coll_type = CS_TYPE_UTF8MB4_GENERAL_CI;
      break;
    }
  case CHARSET_BINARY: {
      coll_type = CS_TYPE_BINARY;
      break;
    }
  case CHARSET_GB18030: {
    coll_type = CS_TYPE_GB18030_CHINESE_CI;
    break;
  }
  case CHARSET_GBK: {
    coll_type = CS_TYPE_GBK_CHINESE_CI;
    break;
  }
  case CHARSET_UTF16: {
    coll_type = CS_TYPE_UTF16_GENERAL_CI;
    break;
  }
  case CHARSET_LATIN1: {
    coll_type = CS_TYPE_LATIN1_SWEDISH_CI;
    break;
  }
  case CHARSET_GB18030_2022: {
    coll_type = CS_TYPE_GB18030_2022_PINYIN_CI;
    break;
  }
  case CHARSET_ASCII: {
    coll_type = CS_TYPE_ASCII_GENERAL_CI;
    break;
  }
  case CHARSET_TIS620: {
    coll_type = CS_TYPE_TIS620_THAI_CI;
    break;
  }
  default: {
      break;
    }
  }
  return coll_type;
}

ObCollationType ObCharset::get_default_collation_oracle(ObCharsetType charset_type)
{
  ObCollationType collation_type = CS_TYPE_INVALID;
  switch(charset_type) {
    case CHARSET_UTF8MB4: {
      collation_type = CS_TYPE_UTF8MB4_BIN;
      break;
    }
    case CHARSET_BINARY: {
      collation_type = CS_TYPE_BINARY;
      break;
    }
    case CHARSET_GB18030: {
      collation_type = CS_TYPE_GB18030_BIN;
      break;
    }
    case CHARSET_GBK: {
      collation_type = CS_TYPE_GBK_BIN;
      break;
    }
    case CHARSET_UTF16: {
      collation_type = CS_TYPE_UTF16_BIN;
      break;
    }
    case CHARSET_LATIN1: {
      collation_type = CS_TYPE_LATIN1_BIN;
      break;
    }
    case CHARSET_GB18030_2022: {
      collation_type = CS_TYPE_GB18030_2022_BIN;
      break;
    }
    case CHARSET_ASCII: {
      collation_type = CS_TYPE_ASCII_BIN;
      break;
    }
    case CHARSET_TIS620: {
      collation_type = CS_TYPE_TIS620_BIN;
      break;
    }
    default: {
      break;
    }
  }
  return collation_type;
}

int ObCharset::get_default_collation(ObCharsetType cs_type, ObCollationType &coll_type)
{
  int ret = OB_SUCCESS;
  switch(cs_type) {
  case CHARSET_UTF8MB4: {
      coll_type = CS_TYPE_UTF8MB4_GENERAL_CI;
      break;
    }
  case CHARSET_BINARY: {
      coll_type = CS_TYPE_BINARY;
      break;
    }
  case CHARSET_GB18030: {
      coll_type = CS_TYPE_GB18030_CHINESE_CI;
      break;
  }
  case CHARSET_GBK: {
    coll_type = CS_TYPE_GBK_CHINESE_CI;
    break;
  }
  case CHARSET_UTF16: {
    coll_type = CS_TYPE_UTF16_GENERAL_CI;
    break;
  }
  case CHARSET_LATIN1: {
    coll_type = CS_TYPE_LATIN1_SWEDISH_CI;
    break;
  }
  case CHARSET_GB18030_2022: {
    coll_type = CS_TYPE_GB18030_2022_PINYIN_CI;
    break;
  }
  case CHARSET_ASCII: {
    coll_type = CS_TYPE_ASCII_GENERAL_CI;
    break;
  }
  case CHARSET_TIS620: {
    coll_type = CS_TYPE_TIS620_THAI_CI;
    break;
  }
  default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid charset type", K(ret), K(cs_type));
      break;
    }
  }
  return ret;
}

ObCollationType ObCharset::get_bin_collation(ObCharsetType cs_type)
{
  ObCollationType coll_type = CS_TYPE_INVALID;
  switch(cs_type) {
  case CHARSET_UTF8MB4: {
      coll_type = CS_TYPE_UTF8MB4_BIN;
      break;
    }
  case CHARSET_BINARY: {
      coll_type = CS_TYPE_BINARY;
      break;
    }
  case CHARSET_GB18030: {
      coll_type = CS_TYPE_GB18030_BIN;
      break;
  }
  case CHARSET_GBK: {
    coll_type = CS_TYPE_GBK_BIN;
    break;
  }
  case CHARSET_UTF16: {
    coll_type = CS_TYPE_UTF16_BIN;
    break;
  }
  case CHARSET_LATIN1: {
    coll_type = CS_TYPE_LATIN1_BIN;
    break;
  }
  case CHARSET_GB18030_2022: {
    coll_type = CS_TYPE_GB18030_2022_BIN;
    break;
  }
  case CHARSET_ASCII: {
    coll_type = CS_TYPE_ASCII_BIN;
    break;
  }
  case CHARSET_TIS620: {
    coll_type = CS_TYPE_TIS620_BIN;
    break;
  }
  default: {
      break;
    }
  }
  return coll_type;
}

int ObCharset::get_default_collation(const ObCollationType &in, ObCollationType &out)
{
  int ret = OB_SUCCESS;
  ObCharsetType charset_type = CHARSET_INVALID;
  if (OB_UNLIKELY(in == CS_TYPE_INVALID)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(CHARSET_INVALID == (charset_type = ObCharset::charset_type_by_coll(in)))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(CS_TYPE_INVALID == (out = ObCharset::get_default_collation(charset_type)))) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

ObCollationType ObCharset::get_system_collation()
{
  return CS_TYPE_UTF8MB4_GENERAL_CI;
}

int ObCharset::first_valid_char(
    const ObCollationType cs_type,
    const char *buf,
    const int64_t buf_size,
    int64_t &char_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected error. invalid argument(s)",
              K(ret), K(cs_type), K(ObCharset::charset_arr[cs_type]));
  } else if (OB_UNLIKELY(NULL == buf)) {
    ret = OB_NOT_INIT;
    LOG_EDIAG("Null buffer passed in", K(ret), K(buf));
  } else if (buf_size <= 0) {
    char_len = 0;
  } else {
    int error = 0;
    int64_t len = 0;
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    if (OB_ISNULL(cs->cset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
    } else {
      len = static_cast<int64_t>(cs->cset->well_formed_len(cs, buf, buf + buf_size, 1, &error));
      if (OB_LIKELY(0 == error)) {
        char_len = len;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("invalid encoding found");
      }
    }
  }
  return ret;
}

int ObCharset::last_valid_char(
    const ObCollationType cs_type,
    const char *buf,
    const int64_t buf_size,
    int64_t &char_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected error. invalid argument(s)",
              K(ret), K(cs_type), K(ObCharset::charset_arr[cs_type]));
  } else if (OB_UNLIKELY(NULL == buf)) {
    ret = OB_NOT_INIT;
    LOG_EDIAG("Null buffer passed in", K(ret), K(buf));
  } else if (buf_size <= 0) {
    char_len = 0;
  } else {
    int64_t char_begin_pos = buf_size;
    int64_t len = 0;
    int error = 0;
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    if (OB_ISNULL(cs->cset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
    } else {
      do {//search back for the first head byte
        --char_begin_pos;
        len = cs->cset->mbcharlen(
            cs, static_cast<unsigned char>(buf[char_begin_pos]));
      } while ((0 < char_begin_pos) && (0 == len));
      len = cs->cset->well_formed_len(//see if it's valid
          cs, buf + char_begin_pos, buf + buf_size, 1, &error);
      if (OB_LIKELY((1 != error) && ((char_begin_pos + len) == buf_size))) {
        char_len = len;
      } else {//invalid, or valid but has invalid trailing bytes
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("invalid encoding found");
      }
    }
  }
  return ret;
}

int ObCharset::check_and_fill_info(ObCharsetType &charset_type, ObCollationType &collation_type)
{
  int ret = OB_SUCCESS;
  if (charset_type == CHARSET_INVALID && collation_type == CS_TYPE_INVALID) {
    ret = OB_ERR_UNEXPECTED;
  } else if (charset_type == CHARSET_INVALID) {
    charset_type = ObCharset::charset_type_by_coll(collation_type);
  } else if (collation_type == CS_TYPE_INVALID) {
    collation_type = ObCharset::get_default_collation(charset_type);
  } else {
    if (!ObCharset::is_valid_collation(charset_type, collation_type)) {
      ret = OB_ERR_COLLATION_MISMATCH;
      LOG_WDIAG("invalid collation info", K(charset_type), K(collation_type));
    }
  }
  return ret;
}

bool ObCharset::is_default_collation(ObCollationType type)
{
  bool ret = false;
  switch (type) {
  case CS_TYPE_UTF8MB4_GENERAL_CI:
    //fall through
  case CS_TYPE_BINARY:
  case CS_TYPE_GB18030_CHINESE_CI:
  case CS_TYPE_GBK_CHINESE_CI:
  case CS_TYPE_ASCII_GENERAL_CI:
  case CS_TYPE_TIS620_THAI_CI:
  case CS_TYPE_GB18030_2022_PINYIN_CI:
  case CS_TYPE_UTF16_GENERAL_CI: {
      ret = true;
      break;
    }
  default: {
      break;
    }
  }
  return ret;
}


bool ObCharset::is_default_collation(ObCharsetType cs_type, ObCollationType coll_type)
{
  bool ret = false;
  ObCollationType default_coll_type = get_default_collation(cs_type);
  if (CS_TYPE_INVALID != default_coll_type && coll_type == default_coll_type) {
    ret = true;
  } else { /* empty */ }
  return ret;
}

int ObCharset::strcmp(const ObCollationType cs_type, const ObString &l_str,
                      const ObString &r_str)
{
  int32_t ret = 0;
  if (l_str.empty()) {
    if (!r_str.empty()) {
      ret = -1;
    }
  } else if (r_str.empty()) {
    ret = 1;
  } else {
    ret = ObCharset::strcmp(cs_type, l_str.ptr(), l_str.length(), r_str.ptr(), r_str.length());
  }
  return ret;
}

size_t ObCharset::casedn(const ObCollationType cs_type, ObString &src)
{
  size_t size = 0;
  if (!src.empty()) {
    size = casedn(cs_type, src.ptr(), src.length(), src.ptr(), src.length());
    src.set_length(static_cast<int32_t>(size));
  }
  return size;
}

bool ObCharset::case_insensitive_equal(const ObString &one,
                                       const ObString &another)
{
  return 0 == strcmp(CS_TYPE_UTF8MB4_GENERAL_CI, one, another);
}

uint64_t ObCharset::hash(const ObCollationType cs_type, const ObString &str,
                         uint64_t seed, hash_algo hash_algo)
{
  uint64_t ret = 0;
  if (!str.empty()) {
    ret = ObCharset::hash(cs_type, str.ptr(), str.length(), seed, lib::is_oracle_mode(), hash_algo);
  }
  return ret;
}

bool ObCharset::case_mode_equal(const ObNameCaseMode case_mode, const ObString &one,
                                const ObString &another)
{
  bool is_equal = false;
  if (OB_UNLIKELY(OB_NAME_CASE_INVALID >= case_mode ||
                  case_mode >= OB_NAME_CASE_MAX)) {
    LOG_EDIAG("unexpected error. invalid cast_mode",
              K(case_mode));
  } else {
    ObCollationType cs_type = CS_TYPE_INVALID;
    if (OB_ORIGIN_AND_SENSITIVE == case_mode) {
      cs_type = CS_TYPE_UTF8MB4_BIN;
    } else if (OB_ORIGIN_AND_INSENSITIVE == case_mode ||
              OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
      cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
    }

    if (0 == strcmp(cs_type, one, another)) {
      is_equal = true;
    }
  }
  return is_equal;
}

bool ObCharset::is_space(const ObCollationType cs_type, char c)
{
  bool ret = false;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    LOG_EDIAG("unexpected error. invalid argument(s)",
              K(ret), K(cs_type), K(ObCharset::charset_arr[cs_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    ret = ob_isspace(cs, c);
  }
  return ret;
}

bool ObCharset::is_graph(const ObCollationType cs_type, char c)
{
  bool ret = false;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    LOG_EDIAG("unexpected error. invalid argument(s)",
              K(ret), K(cs_type), K(ObCharset::charset_arr[cs_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    ret = ob_isgraph(cs, c);
  }
  return ret;
}

bool ObCharset::usemb(const ObCollationType cs_type)
{
  bool ret = false;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    LOG_EDIAG("unexpected error. invalid argument(s)",
              K(ret), K(cs_type), K(ObCharset::charset_arr[cs_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    ret = use_mb(cs);
  }
  return ret;
}

int ObCharset::is_mbchar(const ObCollationType cs_type, const char *str, const char *end)
{
  bool ret = false;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    LOG_EDIAG("unexpected error. invalid argument(s)",
              K(ret), K(cs_type), K(ObCharset::charset_arr[cs_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    ret = ob_ismbchar(cs, str, end);
  }
  return ret;
}

const ObCharsetInfo *ObCharset::get_charset(const ObCollationType cs_type)
{
  ObCharsetInfo *ret = NULL;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX)) {
    LOG_EDIAG("unexpected error. invalid argument(s)",
              K(cs_type));
  } else {
    ret = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
  }
  return ret;
}

int ObCharset::get_mbmaxlen_by_coll(const ObCollationType coll_type, int64_t &mbmaxlen)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(coll_type <= CS_TYPE_INVALID ||
                  coll_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[coll_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected error. invalid argument(s)",
              K(ret), K(coll_type), K(ObCharset::charset_arr[coll_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[coll_type]);
    mbmaxlen = cs->mbmaxlen;
  }
  return ret;
}

/*in order to prevent a char from be splitted into 2 blocks
We have to get the right bound of a string in terms a block
Take "我爱你" as an example
if len_limit_in_byte = 8 which means that the max size of a block is 8 Bytes
since '我' and '爱' takes 6 Bytes in total already.
and '你' takes 3 Bytes.
if we assign the '你' to the block
then the total length will be 9 which is greater than 8
so , byte_num = 6  and char_num = 2 will be returned.
and '你' has to be assigned to another block.

Please note that:

byte_num and char_num should not be used if the status returned by this func is not ob_success!

*/

int ObCharset::fit_string(const ObCollationType cs_type,
                          const char *str,
                          const int64_t str_len,
                          const int64_t len_limit_in_byte,
                          int64_t &byte_num,
                          int64_t &char_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID ||
                  cs_type >= CS_TYPE_MAX) ||
                  len_limit_in_byte <= 0 ||
                  str_len <= 0 ||
                  OB_ISNULL(str) ||
                  OB_ISNULL(ObCharset::charset_arr[cs_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected error. invalid argument(s)",
        K(cs_type), K(str), K(str_len), K(len_limit_in_byte));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    byte_num = 0;
    char_num = 0;
    int64_t max_len =  std::min(str_len, len_limit_in_byte);
    const char *buf_start = str;
    const char *buf_end = str + str_len;
    int64_t char_len = 0;
    int error = 0;
    while(buf_start < buf_end) {
      char_len = static_cast<int64_t>(cs->cset->well_formed_len(cs, buf_start, buf_end, 1, &error));
      if (OB_UNLIKELY(0 != error || char_len <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        break;
      } else if (OB_UNLIKELY(byte_num > max_len - char_len)) {
        break;
      } else {
        byte_num += char_len;
        buf_start += char_len;
        ++char_num;
      }
    }
  }
  return ret;
}

bool ObCharset::is_argument_valid(const ObCharsetInfo *cs, const char *str, int64_t str_len)
{
  //the unexpected case is str is null while str_len is not zero at the same time
  //Yeah, this is obvious. But... Wait a second !
  //What if str is null and str_len is zero which means empty string?
  //Do not worry at all. the routine called (like cs->cset->xxxx) will deal with this
  bool is_arg_valid = true;
  if ((OB_ISNULL(str) && OB_UNLIKELY(0 != str_len)) ||
      OB_UNLIKELY(str_len < 0) ||
      OB_ISNULL(cs) ||
      OB_ISNULL(cs->cset)) {
    is_arg_valid = false;
    BACKTRACE(EDIAG, true, "invalid argument. charset info = %p, str = %p, str_len = %ld", cs, str, str_len);
  }
  return is_arg_valid;
}
bool ObCharset::is_argument_valid(const ObCollationType cs_type, const char *str1, int64_t str_len1, const char *str2, int64_t str_len2)
{
  bool is_arg_valid = true;
  if (OB_UNLIKELY(cs_type <= CS_TYPE_INVALID || cs_type >= CS_TYPE_MAX) ||
      OB_ISNULL(ObCharset::charset_arr[cs_type]) ||
      OB_UNLIKELY(str_len1 < 0) ||
      OB_UNLIKELY(str_len2 < 0) ||
      (OB_ISNULL(str1) && OB_UNLIKELY(0 != str_len1)) ||
      (OB_ISNULL(str2) && OB_UNLIKELY(0 != str_len2))) {
    is_arg_valid = false;
    BACKTRACE(EDIAG, true, "invalid argument."
        "cs_type = %d,"
        "str1 = %p,"
        "str1_len = %ld,"
        "str2 = %p,"
        "str2_len = %ld", cs_type, str1, str_len1, str2, str_len2);
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[cs_type]);
    if (OB_ISNULL(cs->cset) || OB_ISNULL(cs->coll)) {
      is_arg_valid = false;
      BACKTRACE(EDIAG, true, "invalid argument."
          "cs_type = %d,"
          "str1 = %p,"
          "str1_len = %ld,"
          "str2 = %p,"
          "str2_len = %ld,"
          "charset handler = %p,"
          "collation handler = %p", cs_type, str1, str_len1, str2, str_len2, cs->cset, cs->coll);
    }
  }
  return is_arg_valid;
}

static int pcmp(const void *f, const void *s) {
  const uni_idx *F = (const uni_idx *)f;
  const uni_idx *S = (const uni_idx *)s;
  int res;

  if (!(res = ((S->nchars) - (F->nchars))))
    res = ((F->uidx.from) - (S->uidx.to));
  return res;
}


// 下面通过ob_malloc分配的内存，不会销毁，用来存码表
static bool create_fromuni(ObCharsetInfo *cs)
{
  uni_idx idx[PLANE_NUM];
  int i, n;
  OB_UNI_IDX *tab_from_uni;

  /*
    Check that Unicode map is loaded.
    It can be not loaded when the collation is
    listed in Index.xml but not specified
    in the character set specific XML file.
  */
  if (!cs->tab_to_uni) return true;

  /* Clear plane statistics */
  memset(idx, 0, sizeof(idx));

  /* Count number of characters in each plane */
  for (i = 0; i < 0x100; i++) {
    uint16 wc = cs->tab_to_uni[i];
    int pl = PLANE_NUMBER(wc);

    if (wc || !i) {
      if (!idx[pl].nchars) {
        idx[pl].uidx.from = wc;
        idx[pl].uidx.to = wc;
      } else {
        idx[pl].uidx.from = wc < idx[pl].uidx.from ? wc : idx[pl].uidx.from;
        idx[pl].uidx.to = wc > idx[pl].uidx.to ? wc : idx[pl].uidx.to;
      }
      idx[pl].nchars++;
    }
  }

  /* Sort planes in descending order */
  qsort(&idx, PLANE_NUM, sizeof(uni_idx), &pcmp);

  for (i = 0; i < PLANE_NUM; i++) {
    int ch, numchars;
    uchar *tab;

    /* Skip empty plane */
    if (!idx[i].nchars) break;

    numchars = idx[i].uidx.to - idx[i].uidx.from + 1;
    if (!(idx[i].uidx.tab = tab = (uchar *)ob_malloc(
              numchars * sizeof(*idx[i].uidx.tab))))
      return true;

    memset(tab, 0, numchars * sizeof(*idx[i].uidx.tab));

    for (ch = 1; ch < PLANE_SIZE; ch++) {
      uint16 wc = cs->tab_to_uni[ch];
      if (wc >= idx[i].uidx.from && wc <= idx[i].uidx.to && wc) {
        int ofs = wc - idx[i].uidx.from;
        /*
          Character sets like armscii8 may have two code points for
          one character. When converting from UNICODE back to
          armscii8, select the lowest one, which is in the ASCII
          range.
        */
        if (tab[ofs] == '\0') tab[ofs] = ch;
      }
    }
  }

  /* Allocate and fill reverse table for each plane */
  n = i;
  if (!(cs->tab_from_uni = tab_from_uni =
            (OB_UNI_IDX *)ob_malloc(sizeof(OB_UNI_IDX) * (n + 1))))
    return true;

  for (i = 0; i < n; i++) tab_from_uni[i] = idx[i].uidx;

  /* Set end-of-list marker */
  memset(&tab_from_uni[i], 0, sizeof(OB_UNI_IDX));
  return false;
}

static bool ob_cset_init_8bit(ObCharsetInfo *cs)
{
  cs->caseup_multiply = 1;
  cs->casedn_multiply = 1;
  cs->pad_char = ' ';
  return create_fromuni(cs);
}

int ObCharset::init_charset()
{
  int ret = OB_SUCCESS;
  init_gb18030_2022();
  auto add_coll = [&ret](ObCollationType coll_type, ObCharsetInfo *cs)->void {
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(cs) || !ObCharset::is_valid_collation(coll_type)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(cs), K(coll_type));
      } else {
        ObCharset::charset_arr[coll_type] = cs;
        cs->state |= OB_CS_COMPILED;
      }
    }
  };
  add_coll(CS_TYPE_UTF8MB4_0900_BIN, &ob_charset_utf8mb4_0900_bin);
  add_coll(CS_TYPE_UTF8MB4_0900_AI_CI, &ob_charset_utf8mb4_0900_ai_ci);
  add_coll(CS_TYPE_TIS620_BIN, &ob_charset_tis620_bin);
  add_coll(CS_TYPE_TIS620_THAI_CI, &ob_charset_tis620_thai_ci);
  ObCharsetInfo *special_charset[] = {&ob_charset_ascii,&ob_charset_ascii_bin};
  for (int64_t i = 0; i < ARRAYSIZEOF(special_charset); ++i) {
    ObCharsetInfo *cs = special_charset[i];
    ObCharsetHandler *charset_handler = cs->cset;
    ObCollationHandler *coll_handler = cs->coll;
    if  (OB_ISNULL(charset_handler) || OB_ISNULL(coll_handler)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null pointer", K(charset_handler), K(coll_handler), K(ret));
    } else if (OB_UNLIKELY(ob_cset_init_8bit(cs))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to init charset handler", K(ret));
    } else {
      add_coll((ObCollationType)cs->number, cs);
    }
  }
  /* 支持下面字符集的初始化：
    ob_charset_utf8mb4_croatian_uca_ci.coll
    ob_charset_utf8mb4_czech_uca_ci.coll
    observer调用ob_coll_init_uca，proxy直接在结构体里初始化
  */
  return ret;
}


//进行字符集之间的转换，from_type为源字符集，to_type为目标字符集
int ObCharset::charset_convert(const ObCollationType from_type,
                               const char *from_str,
                               const uint32_t from_len,
                               const ObCollationType to_type,
                               char *to_str,
                               uint32_t to_len,
                               uint32_t &result_len,
                               bool trim_incomplete_tail /* true */)
{
  int ret = OB_SUCCESS;
  if (NULL == from_str || from_len <=0) {
    result_len = 0;
  } else if (OB_UNLIKELY(from_type <= CS_TYPE_INVALID
             || from_type >= CS_TYPE_MAX
             || to_type <= CS_TYPE_INVALID
             || to_type >= CS_TYPE_MAX
             || (OB_ISNULL(to_str)
             || OB_UNLIKELY(to_len <= 0)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid convert", K(ret), K(from_type), K(to_type),
             K(from_str), K(to_str), K(from_len), K(to_len));
  } else {
    ObCharsetInfo *from_cs = static_cast<ObCharsetInfo*>(ObCharset::charset_arr[from_type]);
    ObCharsetInfo *to_cs = static_cast<ObCharsetInfo*>(ObCharset::charset_arr[to_type]);
    ObCharsetType src_cs = ObCharset::charset_type_by_coll(from_type);
    ObCharsetType dst_cs = ObCharset::charset_type_by_coll(to_type);
    if ((src_cs == CHARSET_GB18030 && dst_cs == CHARSET_GB18030_2022) ||
        (src_cs == CHARSET_GB18030_2022 && dst_cs == CHARSET_GB18030)) {
      /** GB18030 and GB18030_2022 have the same code points,
        *  but they have different mapping to unicode.
        *  So, we do charset_convert from the charset to the same charset*/
      to_cs = from_cs;
    }
    uint errors;
    if (NULL == from_cs || NULL == to_cs || NULL == from_str || NULL == to_str) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid arugment", K(from_cs), K(to_cs), K(from_str), K(to_str));
    } else {
      result_len = ob_convert(to_str, to_len, to_cs, from_str, from_len, from_cs, &errors, trim_incomplete_tail);
      if (errors != 0) {
        ret = OB_ERR_INCORRECT_STRING_VALUE;
        LOG_WDIAG("ob_convert failed", K(ret), K(errors), K(from_str), K(to_str));
      }
    }
  }

  return ret;
}

bool match_like(const char *text, const char *pattern)
{
  bool ret = false;
  if (OB_ISNULL(pattern) || '\0' == pattern[0]) {
    ret = true;
  } else if (OB_ISNULL(text)) {
    //return false if text config namme is NULL
  } else {
    ObString str_text(text);
    ObString str_pattern(pattern);
    ret = ObCharset::wildcmp(CS_TYPE_UTF8MB4_BIN, str_text, str_pattern, 0, '_', '%');
  }
  return ret;
}

bool match_like(const ObString &str_text, const char *pattern)
{
  bool ret = false;
  if (OB_ISNULL(pattern) || '\0' == pattern[0]) {
    ret = true;
  } else if (str_text.empty()) {
    //return false if text config namme is NULL
  } else {
    ObString str_pattern(pattern);
    ret = ObCharset::wildcmp(CS_TYPE_UTF8MB4_BIN, str_text, str_pattern, 0, '_', '%');
  }
  return ret;
}

bool match_like(const ObString &str_text, const ObString &str_pattern)
{
  bool ret = false;
  if (str_pattern.empty()) {
    ret = true;
  } else if (str_text.empty()) {
    //return false if text config namme is NULL
  } else {
    ret = ObCharset::wildcmp(CS_TYPE_UTF8MB4_BIN, str_text, str_pattern, 0, '_', '%');
  }
  return ret;
}

bool ObStringScanner::next_character(ObString &encoding_value, int32_t &unicode_value, int &ret)
{
  bool has_next = false;
  ret = next_character(encoding_value, unicode_value);

  if (OB_ITER_END == ret) {
    has_next = false;
    ret = OB_SUCCESS;
  } else if (OB_SUCC(ret)) {
    has_next = true;
  } else {
    LOG_WDIAG("fail to get next character", K(ret), K(*this));
    has_next = false;
  }
  return has_next;
}

int ObStringScanner::next_character(ObString &encoding_value, int32_t &unicode_value)
{
  int ret = OB_SUCCESS;
  int32_t length = 0;

  ObString &str = str_;

  if (str.empty()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ObCharset::mb_wc(collation_type_, str.ptr(), str.length(), length, unicode_value))) {
    if (!!(IGNORE_INVALID_CHARACTER & flags_)) {
      ret = OB_SUCCESS;
      length = 1;
    } else {
      ret = OB_ERR_INCORRECT_STRING_VALUE;
      LOG_WDIAG("fail to call mb_wc", K(ret), K(str));
    }
  }
  if (OB_SUCC(ret)) {
    encoding_value.assign_ptr(str.ptr(), length);
    str += length;
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
