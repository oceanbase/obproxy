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

namespace oceanbase
{
namespace common
{
const ObCharsetWrapper ObCharset::charset_wrap_arr_[CHARSET_WRAPPER_COUNT] =
{
  {CHARSET_BINARY, "Binary pseudo charset", CS_TYPE_BINARY, 1},
  {CHARSET_UTF8MB4, "UTF-8 Unicode", CS_TYPE_UTF8MB4_GENERAL_CI, 4},
  {CHARSET_GBK, "GBK charset", CS_TYPE_GBK_CHINESE_CI, 2},
  {CHARSET_UTF16, "UTF-16 Unicode", CS_TYPE_UTF16_GENERAL_CI, 2},
  {CHARSET_GB18030, "GB18030 charset", CS_TYPE_GB18030_CHINESE_CI, 4},
};

const ObCollationWrapper ObCharset::collation_wrap_arr_[COLLATION_WRAPPER_COUNT] =
{
  {CS_TYPE_UTF8MB4_GENERAL_CI, CHARSET_UTF8MB4, CS_TYPE_UTF8MB4_GENERAL_CI, true, true, 1},
  {CS_TYPE_UTF8MB4_BIN, CHARSET_UTF8MB4, CS_TYPE_UTF8MB4_BIN, false, true, 1},
  {CS_TYPE_BINARY, CHARSET_BINARY, CS_TYPE_BINARY, true, true, 1},
  {CS_TYPE_GBK_CHINESE_CI, CHARSET_GBK, CS_TYPE_GBK_CHINESE_CI, true, true, 1},
  {CS_TYPE_GBK_BIN, CHARSET_GBK, CS_TYPE_GBK_BIN, false, true, 1},
  {CS_TYPE_UTF16_GENERAL_CI, CHARSET_UTF16, CS_TYPE_UTF16_GENERAL_CI, true, true, 1},
  {CS_TYPE_UTF16_BIN, CHARSET_UTF16, CS_TYPE_UTF16_BIN, false, true, 1},
  {CS_TYPE_INVALID, CHARSET_INVALID, CS_TYPE_INVALID, false, false, 1},
  {CS_TYPE_INVALID, CHARSET_INVALID, CS_TYPE_INVALID, false, false, 1},
  {CS_TYPE_GB18030_CHINESE_CI, CHARSET_GB18030, CS_TYPE_GB18030_CHINESE_CI, true, true, 1},
  {CS_TYPE_GB18030_BIN, CHARSET_GB18030, CS_TYPE_GB18030_BIN, false, true, 1},
};

void *ObCharset::charset_arr[CS_TYPE_MAX] = {
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 0 ~ 7
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 8
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 16
  NULL, NULL, NULL, NULL, &ob_charset_gbk_chinese_ci,             // 24
                                NULL, NULL, NULL,                 // 29
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 32
  NULL, NULL, NULL, NULL, NULL,                                   // 40
                                &ob_charset_utf8mb4_general_ci,   // 45
                                      &ob_charset_utf8mb4_bin,    // 46
                                            NULL,                 // 47
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
                                NULL,                             // 101
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
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 216
  NULL,                                                           // 224
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 225
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,                 // 233
  NULL, NULL, NULL, NULL, NULL, NULL, NULL,                       // 241
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
    result = cs->cset->strntod(cs, const_cast<char *>(str), str_len, endptr, err);
  }
  return result;
}

int64_t ObCharset::strntoll(const char *str,
                            size_t str_len,
                            int base,
                            char **end_ptr,
                            int *err)
{
  ObCharsetInfo *cs = &ob_charset_bin;
  *end_ptr = const_cast<char*>(str);
  int64_t result = 0;
  if (is_argument_valid(cs, str, str_len)) {
    result = cs->cset->strntoll(cs, str, str_len, base, end_ptr, err);
  }
  return result;
}

uint64_t ObCharset::strntoull(const char *str,
                              size_t str_len,
                              int base,
                              char **end_ptr,
                              int *err)
{
  ObCharsetInfo *cs = &ob_charset_bin;
  *end_ptr = const_cast<char*>(str);
  uint64_t result = 0;
  if (is_argument_valid(cs, str, str_len)) {
    result = cs->cset->strntoull(cs,
                             str,
                             str_len,
                             base,
                             end_ptr,
                             err);
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
    BACKTRACE(ERROR, true, "invalid argument. str = %p, end = %p, cs = %p", str, end, cs);
  } else {
    result = cs->cset->scan(cs, str, end, sq);
  }
  return result;
}
uint32_t ObCharset::instr(ObCollationType collation_type,
                          const char *str1,
                          int64_t str1_len,
                          const char *str2,
                          int64_t str2_len)
{
  uint32_t result = 0;
  if (is_argument_valid(collation_type, str1, str1_len, str2, str2_len)) {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    ob_match_t m_match_t[2];
    uint nmatch = 1;
    uint m_ret = cs->coll->instr(cs, str1, str1_len, str2, str2_len, m_match_t, nmatch);
    if (0 == m_ret ) {
      result = 0;
    } else {
      result =  m_match_t[0].mb_len + 1;
    }
  }
  return result;
}

uint32_t ObCharset::locate(ObCollationType collation_type,
                const char *str1,
                int64_t str1_len,
                const char *str2,
                int64_t str2_len,
                int64_t pos)
{
  uint32_t result = 0;
  if (is_argument_valid(collation_type, str1, str1_len, str2, str2_len)) {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    int64_t start0 = pos - 1;
    int64_t start = start0;
    if (OB_UNLIKELY(start < 0 || start > str1_len)) {
      result = 0;
    } else {
      start = static_cast<int64_t>(charpos(collation_type, str1, str1_len, start));
      if (static_cast<int64_t>(start) + str2_len > str1_len) {
        result = 0;
      } else if (0 == str2_len) {
        result = static_cast<uint32_t>(start) + 1;
      } else {
        ob_match_t match_t;
        uint32_t nmatch = 1;
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

int ObCharset::strcmp(ObCollationType collation_type,
                      const char *str1,
                      int64_t str1_len,
                      const char *str2,
                      int64_t str2_len)
{
  int result = 0;
  if (is_argument_valid(collation_type, str1, str1_len, str2, str2_len)) {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    const bool t_is_prefix = false;
    result = cs->coll->strnncoll(cs,
                              reinterpret_cast<const uchar *>(str1),
                              str1_len,
                              reinterpret_cast<const uchar *>(str2),
                              str2_len, t_is_prefix);
  }
  return result;
}

int ObCharset::strcmpsp(ObCollationType collation_type,
                        const char *str1,
                        int64_t str1_len,
                        const char *str2,
                        int64_t str2_len,
                        bool cmp_endspace)
{
  int result = 0;
  if (is_argument_valid(collation_type, str1, str1_len, str2, str2_len)) {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    result = cs->coll->strnncollsp(cs,
                                reinterpret_cast<const uchar *>(str1),
                                str1_len,
                                reinterpret_cast<const uchar *>(str2),
                                str2_len,
                                cmp_endspace);
  }
  return result;
}

size_t ObCharset::casedn(const ObCollationType collation_type, char *src, size_t src_len,
              char *dest, size_t dest_len)
{
  size_t size = 0;
  if (is_argument_valid(collation_type, src, src_len, dest, dest_len)) {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    size = cs->cset->casedn(cs, src, src_len, dest, dest_len);
  }
  return size;
}

size_t ObCharset::caseup(const ObCollationType collation_type, char *src, size_t src_len,
                         char *dest, size_t dest_len)
{
  size_t size = 0;
  if (is_argument_valid(collation_type, src, src_len, dest, dest_len)) {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
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
  bool is_valid_unicode_tmp = 0;
  if (is_argument_valid(collation_type, str, str_len, key, key_len)) {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);

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

uint64_t ObCharset::hash(ObCollationType collation_type,
                         const char *str,
                         int64_t str_len,
                         uint64_t seed,
                         const bool calc_end_space,
                         hash_algo hash_algo)
{
  uint64_t ret = seed;
  if (is_argument_valid(collation_type, str, str_len, NULL, 0)) {
    // since hash_sort() of MY_COLLATION_HANDLER need two intergers, one for input and output as
    // result, the other only for input as random seed, so I find 0xc6a4a7935bd1e995 from
    // murmurhash64A(), U can also find similar usage too.

    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    if (OB_ISNULL(cs->coll)) {
      LOG_ERROR("unexpected error. invalid argument(s)", K(cs), K(cs->coll));
    } else {
      seed = 0xc6a4a7935bd1e995;
      cs->coll->hash_sort(cs, reinterpret_cast<const uchar *>(str), str_len,
                          &ret, &seed, calc_end_space, hash_algo);
    }
  }
  return ret;
}

int ObCharset::like_range(ObCollationType collation_type,
                          const ObString &like_str,
                          char escape,
                          char *min_str,
                          size_t *min_str_len,
                          char *max_str,
                          size_t *max_str_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(min_str) ||
                  OB_ISNULL(min_str_len) ||
                  OB_ISNULL(max_str) ||
                  OB_ISNULL(max_str_len) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. invalid argument(s)",
              K(ret),
              K(collation_type), K(ObCharset::charset_arr[collation_type]),
              K(max_str), K(max_str_len),
              K(min_str), K(min_str_len));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    static char w_one = '_';
    static char w_many = '%';
    size_t res_size = *min_str_len < *max_str_len ? *min_str_len : *max_str_len;
    if (OB_ISNULL(cs->coll)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected error. invalid argument(s)", K(cs), K(cs->coll));
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

size_t ObCharset::strlen_char(const ObCollationType collation_type,
                              const char *str,
                              int64_t str_len)
{
  size_t ret = 0;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    LOG_ERROR("unexpected error. invalid argument(s)", K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    if (OB_ISNULL(cs->cset)) {
      LOG_ERROR("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
    } else {
      ret = cs->cset->numchars(cs, str, str + str_len);
    }
  }
  return ret;
}

size_t ObCharset::strlen_byte_no_sp(const ObCollationType collation_type,
                                    const char *str,
                                    int64_t str_len)
{
  size_t ret = 0;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    LOG_ERROR("unexpected error. invalid argument(s)", K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    if (OB_ISNULL(cs->cset)) {
      LOG_ERROR("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
    } else {
      ret = cs->cset->lengthsp(cs, str, str_len);
    }
  }
  return ret;
}

int ObCharset::well_formed_len(ObCollationType collation_type, const char *str,
                           int64_t str_len, int64_t &well_formed_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. invalid argument(s)", K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else if (OB_UNLIKELY(NULL == str && 0 != str_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, str is null  and  str_len is nonzero",
             KP(str), K(str_len), K(ret));
  } else if (str_len > 0) {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    if (OB_ISNULL(cs->cset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
    } else {
      int32_t error = 0;
      well_formed_len = cs->cset->well_formed_len(cs, str, str + str_len, UINT64_MAX, &error);
      if (0 != error) {
        ret = OB_ERR_INCORRECT_STRING_VALUE;
        LOG_WARN("well_formed_len failed. invalid char found",
                 K(ret), K(error), "str", ObString(str_len, str));
      }
    }
  } else {
    well_formed_len = 0;
  }
  return ret;
}


int ObCharset::well_formed_len(ObCollationType collation_type, const char *str,
                           int64_t str_len, int64_t &well_formed_len, int32_t &well_formed_error)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. invalid argument(s)", K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else if (OB_UNLIKELY(NULL == str && 0 != str_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, str is null  and  str_len is nonzero",
             KP(str), K(str_len), K(ret));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    if (OB_ISNULL(cs->cset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
    } else {
      well_formed_len = cs->cset->well_formed_len(cs, str, str + str_len, UINT64_MAX, &well_formed_error);
    }
  }
  return ret;
}
size_t ObCharset::charpos(const ObCollationType collation_type,
                              const char *str,
                              const int64_t str_len,
                              const int64_t length)
{
  size_t ret = 0;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    LOG_ERROR("unexpected error. invalid argument(s)", K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    if (OB_ISNULL(cs->cset)) {
      LOG_ERROR("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
    } else {
      ret = cs->cset->charpos(cs, str, str + str_len, length);
    }
  }
  return ret;
}

bool ObCharset::wildcmp(ObCollationType collation_type,
                       const ObString &str,
                       const ObString &wildstr,
                       int32_t escape, int32_t w_one, int32_t w_many)
{
  bool ret = false;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    LOG_ERROR("unexpected error. invalid argument(s)", K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    if (OB_ISNULL(cs->coll)) {
      LOG_ERROR("unexpected error. invalid argument(s)", K(cs), K(cs->coll));
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
    LOG_ERROR("unexpected error. invalid argument(s)",
              K(ret), K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    ob_wc_t my_wc;
    if (OB_ISNULL(cs->cset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
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
    LOG_ERROR("unexpected error. invalid argument(s)", K(ret), K(collation_type));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    if (OB_ISNULL(cs) || OB_ISNULL(cs->cset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected error. invalid argument(s)", K(cs), K(ret));
    } else {
      int tmp = cs->cset->wc_mb(cs, wc, reinterpret_cast<uchar*>(buff),
                                reinterpret_cast<uchar*>(buff + buff_len));
      if (tmp <= 0) {
        ret = OB_ERROR;
      } else {
        ret = OB_SUCCESS;
        length = tmp;
      }
    }
  }
  return ret;
}

const char *ObCharset::charset_name(ObCharsetType charset_type)
{
  const char *ret_name = "invalid_type";
  switch(charset_type) {
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
    default: {
      break;
    }
  }
  return ret_name;
}

const char *ObCharset::charset_name(ObCollationType collation_type)
{
  return charset_name(charset_type_by_coll(collation_type));
}

const char *ObCharset::collation_name(ObCollationType collation_type)
{
  ObCharsetInfo *cs = NULL;
  if (collation_type < CS_TYPE_MAX && collation_type >= CS_TYPE_INVALID) {
    cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
  }
  return (NULL == cs) ? "invalid_type" : cs->name;
}

int ObCharset::collation_name(ObCollationType collation_type, ObString &coll_name)
{
  int ret = OB_SUCCESS;
  ObCharsetInfo *charset_info = NULL;
  if (collation_type < CS_TYPE_MAX && collation_type >= CS_TYPE_INVALID) {
    charset_info = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
  }
  if (OB_ISNULL(charset_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid collation type", K(ret), K(collation_type));
  } else {
    coll_name = ObString(charset_info->name);
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
  ObCharsetType charset_type = CHARSET_INVALID;
  if (0 == cs_name.case_compare("utf8")) {
    charset_type = CHARSET_UTF8MB4;
  } else if (0 == cs_name.case_compare(ob_charset_utf8mb4_bin.csname)) {
    charset_type = CHARSET_UTF8MB4;
  } else if (0 == cs_name.case_compare(ob_charset_bin.csname)) {
    charset_type = CHARSET_BINARY;
  } else if (0 == cs_name.case_compare(ob_charset_gb18030_chinese_ci.csname)) {
    charset_type = CHARSET_GB18030;
  } else if (0 == cs_name.case_compare(ob_charset_gbk_chinese_ci.csname)) {
    charset_type = CHARSET_GBK;
  } else if (0 == cs_name.case_compare(ob_charset_utf16_general_ci.csname)) {
    charset_type = CHARSET_UTF16;
  }
  return charset_type;
}

ObCharsetType ObCharset::charset_type(const char *cs_name)
{
  ObCharsetType ct = CHARSET_INVALID;
  if (OB_ISNULL(cs_name)) {
    LOG_ERROR("unexpected error. invalid argument(s)",
              K(ct), K(cs_name), K(ct));
  } else {
    ObString cs_name_str = ObString::make_string(cs_name);
    ct = charset_type(cs_name_str);
  }
  return ct;
}

ObCollationType ObCharset::collation_type(const ObString &cs_name)
{
  ObCollationType collation_type = CS_TYPE_INVALID;
  if (0 == cs_name.case_compare("utf8_bin")) {
    collation_type = CS_TYPE_UTF8MB4_BIN;
  } else if (0 == cs_name.case_compare("utf8_general_ci")) {
    collation_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  } else if (0 == cs_name.case_compare(ob_charset_utf8mb4_bin.name)) {
    collation_type = CS_TYPE_UTF8MB4_BIN;
  } else if (0 == cs_name.case_compare(ob_charset_utf8mb4_general_ci.name)) {
    collation_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  } else if (0 == cs_name.case_compare(ob_charset_bin.name)) {
    collation_type = CS_TYPE_BINARY;
  } else if (0 == cs_name.case_compare(ob_charset_gb18030_bin.name)) {
    collation_type = CS_TYPE_GB18030_BIN;
  } else if (0 == cs_name.case_compare(ob_charset_gb18030_chinese_ci.name)) {
    collation_type = CS_TYPE_GB18030_CHINESE_CI;
  } else if (0 == cs_name.case_compare(ob_charset_gbk_bin.name)) {
    collation_type = CS_TYPE_GBK_BIN;
  } else if (0 == cs_name.case_compare(ob_charset_gbk_chinese_ci.name)) {
    collation_type = CS_TYPE_GBK_CHINESE_CI;
  } else if (0 == cs_name.case_compare(ob_charset_utf16_bin.name)) {
    collation_type = CS_TYPE_UTF16_BIN;
  } else if (0 == cs_name.case_compare(ob_charset_utf16_general_ci.name)) {
    collation_type = CS_TYPE_UTF16_GENERAL_CI;
  }
  return collation_type;
}

ObCollationType ObCharset::collation_type(const char* cs_name)
{
  ObString cs_name_str = ObString::make_string(cs_name);
  return collation_type(cs_name_str);
}

bool ObCharset::is_valid_collation(ObCharsetType charset_type, ObCollationType collation_type)
{
  bool ret = false;
  if (CHARSET_UTF8MB4 == charset_type) {
    if (CS_TYPE_UTF8MB4_BIN == collation_type
        || CS_TYPE_UTF8MB4_GENERAL_CI == collation_type) {
      ret = true;
    }
  } else if (CHARSET_BINARY == charset_type
      && CS_TYPE_BINARY == collation_type) {
    ret = true;
  } else if (CHARSET_GB18030 == charset_type) {
    if (CS_TYPE_GB18030_BIN == collation_type
        || CS_TYPE_GB18030_CHINESE_CI == collation_type) {
      ret = true;
    }
  } else if (CHARSET_GBK == charset_type) {
    if (CS_TYPE_GBK_BIN == collation_type
        || CS_TYPE_GBK_CHINESE_CI == collation_type) {
      ret = true;
    }
  } else if (CHARSET_UTF16 == charset_type) {
    if (CS_TYPE_UTF16_BIN == collation_type
        || CS_TYPE_UTF16_GENERAL_CI == collation_type) {
      ret = true;
    }
  }
  return ret;
}

bool ObCharset::is_valid_collation(int64_t collation_type_int)
{
  ObCollationType collation_type = static_cast<ObCollationType>(collation_type_int);
  return CS_TYPE_UTF8MB4_GENERAL_CI == collation_type
    || CS_TYPE_UTF8MB4_BIN == collation_type
    || CS_TYPE_BINARY == collation_type
    || CS_TYPE_GB18030_BIN == collation_type
    || CS_TYPE_GB18030_CHINESE_CI == collation_type
    || CS_TYPE_GBK_BIN == collation_type
    || CS_TYPE_GBK_CHINESE_CI == collation_type
    || CS_TYPE_UTF16_BIN == collation_type
    || CS_TYPE_UTF16_GENERAL_CI == collation_type;
}

bool ObCharset::is_valid_charset(int64_t cs_type_int)
{
  ObCharsetType charset_type = static_cast<ObCharsetType>(cs_type_int);
  return CHARSET_BINARY == charset_type
    || CHARSET_UTF8MB4 == charset_type
    || CHARSET_GBK == charset_type
    || CHARSET_UTF16 == charset_type
    || CHARSET_GB18030 == charset_type;
}

ObCharsetType ObCharset::charset_type_by_coll(ObCollationType collation_type)
{
  ObCharsetType charset_type = CHARSET_INVALID;
  switch(collation_type) {
    case CS_TYPE_UTF8MB4_GENERAL_CI:
      //fall through
    case CS_TYPE_UTF8MB4_BIN: {
      charset_type = CHARSET_UTF8MB4;
      break;
    }
    case CS_TYPE_BINARY: {
      charset_type = CHARSET_BINARY;
      break;
    }
    case CS_TYPE_GB18030_BIN:
    case CS_TYPE_GB18030_CHINESE_CI: {
      charset_type = CHARSET_GB18030;
      break;
    }
    case CS_TYPE_GBK_BIN:
    case CS_TYPE_GBK_CHINESE_CI: {
      charset_type = CHARSET_GBK;
      break;
    }
    case CS_TYPE_UTF16_BIN:
    case CS_TYPE_UTF16_GENERAL_CI: {
      charset_type = CHARSET_UTF16;
      break;
    }
    default: {
      break;
    }
  }
  return charset_type;
}

int ObCharset::charset_name_by_coll(const ObString &coll_name, ObString &cs_name)
{
  int ret = OB_SUCCESS;
  ObCollationType coll_type = collation_type(coll_name);
  if (OB_UNLIKELY(CS_TYPE_INVALID == coll_type)) {
    ret = OB_ERR_UNKNOWN_COLLATION;
    LOG_WARN("invalid collation type", K(ret), K(coll_name));
  } else if (OB_FAIL(charset_name_by_coll(coll_type, cs_name))) {
    LOG_WARN("fail to get charset type by collation type", K(ret), K(coll_type), K(coll_name));
  }
  return ret;
}

int ObCharset::charset_name_by_coll(ObCollationType collation_type, ObString &cs_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(CS_TYPE_INVALID == collation_type)) {
    ret = OB_ERR_UNKNOWN_COLLATION;
    LOG_WARN("invalid collation type", K(ret), K(collation_type));
  } else {
    ObCharsetType charset_type = charset_type_by_coll(collation_type);
    if (OB_UNLIKELY(CHARSET_INVALID == charset_type)) {
      ret = OB_ERR_UNKNOWN_CHARSET;
      LOG_WARN("has no charset type of this collation type", K(ret), K(collation_type));
    } else {
      ObString tmp_cs_name = ObString(charset_name(charset_type));
      if (OB_UNLIKELY(tmp_cs_name == "invalid_type")) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("charset str is invalid_type", K(ret), K(charset_type), K(collation_type));
      } else {
        cs_name = tmp_cs_name;
      }
    }
  }
  return ret;
}

int ObCharset::calc_collation(
    const ObCollationLevel collation_level1,
    const ObCollationType collation_type1,
    const ObCollationLevel collation_level2,
    const ObCollationType collation_type2,
    ObCollationLevel &res_level,
    ObCollationType &res_type)
{
  return ObCharset::result_collation(collation_level1, collation_type1,
                                     collation_level2, collation_type2,
                                     res_level, res_type);
}

int ObCharset::result_collation(
    const ObCollationLevel collation_level1,
    const ObCollationType collation_type1,
    const ObCollationLevel collation_level2,
    const ObCollationType collation_type2,
    ObCollationLevel &res_level,
    ObCollationType &res_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(CS_LEVEL_INVALID == collation_level1
      || CS_LEVEL_INVALID == collation_level2
      || CS_TYPE_INVALID == collation_type1
      || CS_TYPE_INVALID == collation_type2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid collation level or type", K(collation_level1), K(collation_type1), K(collation_level2), K(collation_type2));
  } else if (collation_level1 == collation_level2) {
    if (CS_LEVEL_EXPLICIT == collation_level1 && collation_type1 != collation_type2) {
      // ERROR 1267 (HY000): Illegal mix of collations (utf8_general_ci,EXPLICIT) and (utf8_bin,EXPLICIT) for operation '='
      ret = OB_CANT_AGGREGATE_2COLLATIONS;
      // LOG_USER_ERROR(ret);
    } else {
      // just consider two collations: bin & general_ci.
      // we must change the code below if we need to support more collations.
      res_level = collation_level1;
      res_type = (collation_type1 == collation_type2) ? collation_type1 : CS_TYPE_UTF8MB4_BIN;
    }
  } else if (collation_level1 < collation_level2) {
    res_level = collation_level1;
    res_type = collation_type1;
  } else {
    res_level = collation_level2;
    res_type = collation_type2;
  }
  return ret;
}

/** this function is to determine use which charset when compare
 *
 * MySQL uses coercibility values with the following rules to resolve ambiguities:
 * 1. Use the collation with the lowest coercibility value.
 * 2. If both sides have the same coercibility, then:
 *  2.a If both sides are Unicode, or both sides are not Unicode, it is an error.
 *  2.b If one of the sides has a Unicode character set, and another side has a non-Unicode character set, the side with Unicode character set wins,
 *      and automatic character set conversion is applied to the non-Unicode side.
 *  2.c For an operation with operands from the same character set but that mix a _bin collation and a _ci or _cs collation, the _bin collation is used.
 *  This is similar to how operations that mix nonbinary and binary strings evaluate the operands as binary strings, except that it is for collations rather than data types.
*/
int ObCharset::aggregate_collation(
    const ObCollationLevel collation_level1,
    const ObCollationType collation_type1,
    const ObCollationLevel collation_level2,
    const ObCollationType collation_type2,
    ObCollationLevel &res_level,
    ObCollationType &res_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
      CS_LEVEL_INVALID == collation_level1
      || CS_LEVEL_INVALID == collation_level2
      || CS_TYPE_INVALID == collation_type1
      || CS_TYPE_INVALID == collation_type2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid collation level or type",
              K(ret), K(collation_level1), K(collation_type1), K(collation_level2), K(collation_type2));
  } else {
    /** compare level first
      * with the same levels, if has binary then use binary else compare with regulations
      */
    if (collation_level1 < collation_level2) {
      res_type = collation_type1;
      res_level = collation_level1;
    } else if (collation_level2 < collation_level1) {
      res_type = collation_type2;
      res_level = collation_level2;
    } else if (CS_TYPE_BINARY == collation_type1) {
      res_level = collation_level1;
      res_type = collation_type1;
    } else if (CS_TYPE_BINARY == collation_type2) {
      res_level = collation_level2;
      res_type = collation_type2;
    } else if (charset_type_by_coll(collation_type1) != charset_type_by_coll(collation_type2)) {
      ret = OB_CANT_AGGREGATE_2COLLATIONS;
    } else {
      if (collation_type1 == collation_type2) {
        res_type = collation_type1;
        res_level = collation_level1;
      } else if (CS_LEVEL_EXPLICIT == collation_level1) {
        ret = OB_CANT_AGGREGATE_2COLLATIONS;
        // ERROR 1267 (HY000): Illegal mix of collations (utf8_general_ci,EXPLICIT) and (utf8_bin,EXPLICIT) for operation '='
        // LOG_USER_ERROR(ret);
      } else if (charset_type_by_coll(collation_type1) == CHARSET_UTF8MB4) {
        if (OB_UNLIKELY(collation_type1 != CS_TYPE_UTF8MB4_BIN
              && collation_type1 != CS_TYPE_UTF8MB4_GENERAL_CI) ||
            OB_UNLIKELY(collation_type2 != CS_TYPE_UTF8MB4_BIN
              && collation_type2 != CS_TYPE_UTF8MB4_GENERAL_CI)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid collation level or type",
                   K(ret), K(collation_level1), K(collation_type1), K(collation_level2), K(collation_type2));
        } else {
          if (collation_type1 == CS_TYPE_UTF8MB4_BIN || collation_type2 == CS_TYPE_UTF8MB4_BIN) {
            res_type = CS_TYPE_UTF8MB4_BIN;
            res_level = (CS_TYPE_UTF8MB4_BIN == collation_type1) ? collation_level1 : collation_level2;
          } else {
            ret = OB_CANT_AGGREGATE_2COLLATIONS;
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("Unexpected charset", K(collation_type1), K(collation_type2), K(lbt()));
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("Illegal mix of collations", K(ret),
             "type1", ObCharset::collation_name(collation_type1),
             "level1", ObCharset::collation_level(collation_level1),
             "type2", ObCharset::collation_name(collation_type2),
             "level2", ObCharset::collation_level(collation_level2));
  }
  return ret;
}

bool ObCharset::is_bin_sort(ObCollationType collation_type)
{
  bool ret = false;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    LOG_ERROR("unexpected error. invalid argument(s)",
              K(ret), K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
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

ObCollationType ObCharset::get_default_collation(ObCharsetType charset_type)
{
  ObCollationType collation_type = CS_TYPE_INVALID;
  switch(charset_type) {
    case CHARSET_UTF8MB4: {
      collation_type = CS_TYPE_UTF8MB4_GENERAL_CI;
      break;
    }
    case CHARSET_BINARY: {
      collation_type = CS_TYPE_BINARY;
      break;
    }
    case CHARSET_GB18030: {
      collation_type = CS_TYPE_GB18030_CHINESE_CI;
      break;
    }
    case CHARSET_GBK: {
      collation_type = CS_TYPE_GBK_CHINESE_CI;
      break;
    }
    case CHARSET_UTF16: {
      collation_type = CS_TYPE_UTF16_GENERAL_CI;
      break;
    }
    default: {
      break;
    }
  }
  return collation_type;
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
    default: {
      break;
    }
  }
  return collation_type;
}

int ObCharset::get_default_collation(ObCharsetType charset_type, ObCollationType &collation_type)
{
  int ret = OB_SUCCESS;
  switch(charset_type) {
    case CHARSET_UTF8MB4: {
      collation_type = CS_TYPE_UTF8MB4_GENERAL_CI;
      break;
    }
    case CHARSET_BINARY: {
      collation_type = CS_TYPE_BINARY;
      break;
    }
    case CHARSET_GB18030: {
      collation_type = CS_TYPE_GB18030_CHINESE_CI;
      break;
    }
    case CHARSET_GBK: {
      collation_type = CS_TYPE_GBK_CHINESE_CI;
      break;
    }
    case CHARSET_UTF16: {
      collation_type = CS_TYPE_UTF16_GENERAL_CI;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid charset type", K(ret), K(charset_type));
      break;
    }
  }
  return ret;
}

ObCollationType ObCharset::get_bin_collation(ObCharsetType charset_type)
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
    default: {
      break;
    }
  }
  return collation_type;
}

int ObCharset::get_default_collation(const ObCollationType &in, ObCollationType &out)
{
  int ret = OB_SUCCESS;
  ObCharsetType charset_type = CHARSET_INVALID;
  if (OB_UNLIKELY(in == CS_TYPE_INVALID)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(CHARSET_INVALID == (charset_type = ObCharset::charset_type_by_coll(in)))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(CS_TYPE_INVALID == (out = (lib::is_mysql_mode() ?
              ObCharset::get_default_collation(charset_type)
            : ObCharset::get_default_collation_oracle(charset_type))))) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

ObCollationType ObCharset::get_system_collation()
{
  return CS_TYPE_UTF8MB4_GENERAL_CI;
}

int ObCharset::first_valid_char(
    const ObCollationType collation_type,
    const char *buf,
    const int64_t buf_size,
    int64_t &char_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. invalid argument(s)",
              K(ret), K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else if (OB_UNLIKELY(NULL == buf)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("Null buffer passed in", K(ret), K(buf));
  } else if (buf_size <= 0) {
    char_len = 0;
  } else {
    int error = 0;
    int64_t len = 0;
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    if (OB_ISNULL(cs->cset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected error. invalid argument(s)", K(cs), K(cs->cset));
    } else {
      len = static_cast<int64_t>(cs->cset->well_formed_len(cs, buf, buf + buf_size, 1, &error));
      if (OB_LIKELY(0 == error)) {
        char_len = len;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid encoding found");
      }
    }
  }
  return ret;
}

int ObCharset::last_valid_char(
    const ObCollationType collation_type,
    const char *buf,
    const int64_t buf_size,
    int64_t &char_len)
{
  int ret = OB_SUCCESS;
  ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);

  if (OB_ISNULL(cs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("collation type is invalid", K(collation_type), K(ret));
  } else {
    if (buf_size <= 0 || OB_ISNULL(buf)) {
      char_len = 0;
    } else {
      int64_t len = 0;
      for (len = cs->mbminlen; len <= cs->mbmaxlen; ++len) {
        int error = 0;
        int real_len =
            cs->cset->well_formed_len(cs, buf + buf_size - len, buf + buf_size, len, &error);
        if (0 == error && real_len == len) {
          char_len = len;
          break;
        }
      }
      if (len > cs->mbmaxlen) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid encoding found", K(ret), "str", ObString(buf_size, buf));
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
      LOG_WARN("invalid collation info", K(charset_type), K(collation_type));
    }
  }
  return ret;
}

bool ObCharset::is_default_collation(ObCollationType collation_type)
{
  bool ret = false;
  switch (collation_type) {
    case CS_TYPE_UTF8MB4_GENERAL_CI:
    case CS_TYPE_BINARY:
    case CS_TYPE_GB18030_CHINESE_CI:
    case CS_TYPE_GBK_CHINESE_CI:
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


bool ObCharset::is_default_collation(ObCharsetType charset_type, ObCollationType collation_type)
{
  bool ret = false;
  ObCollationType default_collation_type = get_default_collation(charset_type);
  if (CS_TYPE_INVALID != default_collation_type && collation_type == default_collation_type) {
    ret = true;
  } else { /* empty */ }
  return ret;
}

int ObCharset::strcmp(const ObCollationType collation_type, const ObString &l_str,
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
    ret = ObCharset::strcmp(collation_type, l_str.ptr(), l_str.length(), r_str.ptr(), r_str.length());
  }
  return ret;
}

size_t ObCharset::casedn(const ObCollationType collation_type, ObString &src)
{
  size_t size = 0;
  if (!src.empty()) {
    size = casedn(collation_type, src.ptr(), src.length(), src.ptr(), src.length());
    src.set_length(static_cast<int32_t>(size));
  }
  return size;
}

bool ObCharset::case_insensitive_equal(const ObString &one,
                                       const ObString &another,
                                       const ObCollationType &collation_type)
{
  return 0 == strcmp(collation_type, one, another);
}

/* for db objects' name use, like column names, table names; on oracle mode, trailing spaces are always part of the hash calc
 * although trailing spaces are not allowed in db object's name, "a" and "a " are two different names in Oracle
 * if you want to use this hash fun in other places, please contact @maoli */
uint64_t ObCharset::hash(const ObCollationType collation_type, const ObString &str,
                         uint64_t seed, hash_algo hash_algo)
{
  uint64_t ret = 0;
  if (!str.empty()) {
    ret = ObCharset::hash(collation_type, str.ptr(), str.length(),
                          seed, lib::is_oracle_mode(), hash_algo);
  }
  return ret;
}

bool ObCharset::case_mode_equal(const ObNameCaseMode case_mode, const ObString &one,
                                const ObString &another)
{
  bool is_equal = false;
  if (OB_UNLIKELY(OB_NAME_CASE_INVALID >= case_mode ||
                  case_mode >= OB_NAME_CASE_MAX)) {
    LOG_ERROR("unexpected error. invalid cast_mode",
              K(case_mode));
  } else {
    ObCollationType collation_type = CS_TYPE_INVALID;
    if (OB_ORIGIN_AND_SENSITIVE == case_mode) {
      collation_type = CS_TYPE_UTF8MB4_BIN;
    } else if (OB_ORIGIN_AND_INSENSITIVE == case_mode ||
              OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
      collation_type = CS_TYPE_UTF8MB4_GENERAL_CI;
    }

    if (0 == strcmp(collation_type, one, another)) {
      is_equal = true;
    }
  }
  return is_equal;
}

bool ObCharset::is_space(const ObCollationType collation_type, char c)
{
  bool ret = false;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    LOG_ERROR("unexpected error. invalid argument(s)",
              K(ret), K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    ret = ob_isspace(cs, c);
  }
  return ret;
}

bool ObCharset::is_graph(const ObCollationType collation_type, char c)
{
  bool ret = false;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    LOG_ERROR("unexpected error. invalid argument(s)",
              K(ret), K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    ret = ob_isgraph(cs, c);
  }
  return ret;
}

bool ObCharset::usemb(const ObCollationType collation_type)
{
  bool ret = false;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    LOG_ERROR("unexpected error. invalid argument(s)",
              K(ret), K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    ret = use_mb(cs);
  }
  return ret;
}

int ObCharset::is_mbchar(const ObCollationType collation_type, const char *str, const char *end)
{
  bool ret = false;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    LOG_ERROR("unexpected error. invalid argument(s)",
              K(ret), K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    ret = ob_ismbchar(cs, str, end);
  }
  return ret;
}

const ObCharsetInfo *ObCharset::get_charset(const ObCollationType collation_type)
{
  ObCharsetInfo *ret = NULL;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX)) {
    LOG_ERROR("unexpected error. invalid argument(s)",
              K(collation_type));
  } else {
    ret = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
  }
  return ret;
}

int ObCharset::get_mbmaxlen_by_coll(const ObCollationType collation_type, int64_t &mbmaxlen)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. invalid argument(s)",
              K(ret), K(collation_type), K(ObCharset::charset_arr[collation_type]));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
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

int ObCharset::fit_string(const ObCollationType collation_type,
                          const char *str,
                          const int64_t str_len,
                          const int64_t len_limit_in_byte,
                          int64_t &byte_num,
                          int64_t &char_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID ||
                  collation_type >= CS_TYPE_MAX) ||
                  len_limit_in_byte <= 0 ||
                  str_len <= 0 ||
                  OB_ISNULL(str) ||
                  OB_ISNULL(ObCharset::charset_arr[collation_type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. invalid argument(s)",
        K(collation_type), K(str), K(str_len), K(len_limit_in_byte));
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
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
    BACKTRACE(ERROR, true, "invalid argument. charset info = %p, str = %p, str_len = %ld", cs, str, str_len);
  }
  return is_arg_valid;
}
bool ObCharset::is_argument_valid(const ObCollationType collation_type, const char *str1, int64_t str_len1, const char *str2, int64_t str_len2)
{
  bool is_arg_valid = true;
  if (OB_UNLIKELY(collation_type <= CS_TYPE_INVALID || collation_type >= CS_TYPE_MAX) ||
      OB_ISNULL(ObCharset::charset_arr[collation_type]) ||
      OB_UNLIKELY(str_len1 < 0) ||
      OB_UNLIKELY(str_len2 < 0) ||
      (OB_ISNULL(str1) && OB_UNLIKELY(0 != str_len1)) ||
      (OB_ISNULL(str2) && OB_UNLIKELY(0 != str_len2))) {
    is_arg_valid = false;
    BACKTRACE(ERROR, true, "invalid argument."
        "collation_type = %d,"
        "str1 = %p,"
        "str1_len = %ld,"
        "str2 = %p,"
        "str2_len = %ld", collation_type, str1, str_len1, str2, str_len2);
  } else {
    ObCharsetInfo *cs = static_cast<ObCharsetInfo *>(ObCharset::charset_arr[collation_type]);
    if (OB_ISNULL(cs->cset) || OB_ISNULL(cs->coll)) {
      is_arg_valid = false;
      BACKTRACE(ERROR, true, "invalid argument."
          "collation_type = %d,"
          "str1 = %p,"
          "str1_len = %ld,"
          "str2 = %p,"
          "str2_len = %ld,"
          "charset handler = %p,"
          "collation handler = %p", collation_type, str1, str_len1, str2, str_len2, cs->cset, cs->coll);
    }
  }
  return is_arg_valid;
}

int ObCharset::charset_convert(const ObCollationType from_type,
                               const char *from_str,
                               const uint32_t from_len,
                               const ObCollationType to_type,
                               char *to_str,
                               uint32_t to_len,
                               uint32_t &result_len) {
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
    LOG_WARN("invalid convert", K(ret), K(from_type), K(to_type),
             K(from_str), K(to_str), K(from_len), K(to_len));
  } else {
    ObCharsetInfo *from_cs = static_cast<ObCharsetInfo*>(ObCharset::charset_arr[from_type]);
    ObCharsetInfo *to_cs = static_cast<ObCharsetInfo*>(ObCharset::charset_arr[to_type]);
    unsigned int errors;
    if (NULL == from_cs || NULL == to_cs || NULL == from_str || NULL == to_str) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arugment", K(from_cs), K(to_cs), K(from_str), K(to_str));
    } else {
      result_len = ob_convert(to_str, to_len, to_cs, from_str, from_len, from_cs, &errors);
      if (errors != 0) {
        ret = OB_ERR_INCORRECT_STRING_VALUE;
        LOG_WARN("ob_convert failed", K(ret), K(errors),
                 K(from_type), K(to_type), K(ObString(from_len, from_str)), K(to_len));
      }
    }
  }

  return ret;
}


} // namespace common
} // namespace oceanbase
