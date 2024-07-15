/** 
 * Copyright (code) 2021 OceanBase 
 * OceanBase Database Proxy(ODP) is licensed under Mulan PubL v2. 
 * You can use this software according to the terms and conditions of the Mulan PubL v2. 
 * You may obtain a copy of Mulan PubL v2 at: 
 *          http://license.coscl.org.cn/MulanPubL-2.0 
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, 
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, 
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. 
 * See the Mulan PubL v2 for more details.
 */

#include "lib/charset/ob_mysql_global.h"
#include "lib/charset/ob_ctype.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/charset/ob_ctype_ascii_tab.h"

int ob_wc_mb_8bit(const ObCharsetInfo *cs, ob_wc_t wc, uchar *str, uchar *end) {
  const OB_UNI_IDX *idx;

  if (str >= end) return OB_CS_TOOSMALL;

  for (idx = cs->tab_from_uni; idx->tab; idx++) {
    if (idx->from <= wc && idx->to >= wc) {
      str[0] = idx->tab[wc - idx->from];
      return (!str[0] && wc) ? OB_CS_ILUNI : 1;
    }
  }
  return OB_CS_ILUNI;
}

int ob_mb_wc_8bit(const ObCharsetInfo *cs, ob_wc_t *wc, const uchar *str,
                  const uchar *end) {
  if (str >= end) return OB_CS_TOOSMALL;

  *wc = cs->tab_to_uni[*str];
  return (!wc[0] && str[0]) ? -1 : 1;
}

size_t ob_well_formed_len_ascii(const ObCharsetInfo *cs,
                                const char *start, const char *end,
                                size_t nchars,
                                int *error)
{
  const char *oldstart = start;
  *error = 0;
  while (start < end) {
    if ((*start & 0x80) != 0) {
      *error = 1;
      break;
    }
    start++;
  }
  return start - oldstart;
}

static ObCharsetHandler ob_charset_ascii_handler = {
    // ob_cset_init_8bit, /* init */
    NULL,
    ob_mbcharlen_8bit,
    ob_numchars_8bit,
    ob_charpos_8bit,
    ob_max_bytes_charpos_8bit,
    ob_well_formed_len_ascii,
    ob_lengthsp_8bit,
    //ob_numcells_8bit,
    ob_mb_wc_8bit,
    ob_wc_mb_8bit,
    ob_mb_ctype_8bit,
    //ob_caseup_str_8bit,
    //ob_casedn_str_8bit,
    ob_caseup_8bit,
    ob_casedn_8bit,
    //ob_snprintf_8bit,
    //ob_long10_to_str_8bit,
    //ob_longlong10_to_str_8bit,
    ob_fill_8bit,
    ob_strntol_8bit,
    ob_strntoul_8bit,
    ob_strntoll_8bit,
    ob_strntoull_8bit,
    ob_strntod_8bit,
    //ob_strtoll10_8bit,
    ob_strntoull10rnd_8bit,
    ob_scan_8bit};

ObCharsetInfo ob_charset_ascii = {
  11,0,0,
  OB_CS_COMPILED | OB_CS_PRIMARY | OB_CS_PUREASCII,
  "ascii",
  "ascii_general_ci",
  "US ASCII",
  NULL,
  // NULL,  /* coll_param */
  ctype_ascii_general_ci,
  to_lower_ascii_general_ci,
  to_upper_ascii_general_ci,
  sort_order_ascii_general_ci,
  NULL,
  to_uni_ascii_general_ci,
  NULL,
  &ob_unicase_default,
  NULL,
  NULL,
  1,
  1,
  1,
  1,
  1,
  0,
  255,
  ' ',
  false,
  1,
  1,
  &ob_charset_ascii_handler,
  &ob_collation_8bit_simple_ci_handler,
  /*PAD_SPACE*/};

ObCharsetInfo ob_charset_ascii_bin = {
  65,0,0,
  OB_CS_COMPILED | OB_CS_BINSORT | OB_CS_PUREASCII,
  "ascii",
  "ascii_bin",
  "US ASCII",
  NULL,
  // NULL, /* coll_param */
  ctype_ascii_bin,
  to_lower_ascii_bin,
  to_upper_ascii_bin,
  NULL,
  NULL,
  to_uni_ascii_bin,
  NULL,
  &ob_unicase_default,
  NULL,
  NULL,
  1,
  1,
  1,
  1,
  1,
  0,
  255,
  ' ',
  false,
  1,
  1,
  &ob_charset_ascii_handler,
  &ob_collation_8bit_bin_handler};