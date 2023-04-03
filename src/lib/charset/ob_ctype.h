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

#ifndef OCEANBASE_LIB_OBMYSQL_OB_CTYPE_
#define OCEANBASE_LIB_OBMYSQL_OB_CTYPE_

#include "lib/charset/ob_mysql_global.h"
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#ifdef	__cplusplus
extern "C" {
#endif


#define OB_UTF8MB4                 "utf8mb4"

#define OB_UTF8MB4_GENERAL_CI OB_UTF8MB4 "_general_ci"
#define OB_UTF8MB4_GENERAL_CS OB_UTF8MB4 "_general_cs"
#define OB_UTF8MB4_BIN        OB_UTF8MB4 "_bin"
#define OB_UTF8MB4_UNICODE_CI OB_UTF8MB4 "_unicode_ci"

#define OB_UTF16                 "utf16"

#define OB_UTF16_GENERAL_CI OB_UTF16 "_general_ci"
#define OB_UTF16_BIN        OB_UTF16 "_bin"
#define OB_UTF16_UNICODE_CI OB_UTF16 "_unicode_ci"

#define OB_LATIN1 "latin1"
#define OB_LATIN1_SWEDISH_CI OB_LATIN1 "_swedish_ci"
#define OB_LATIN1_BIN OB_LATIN1 "_bin"

/* wm_wc and wc_mb return codes */
#define OB_CS_ILSEQ 0     // mb_wc wrong sequence
#define OB_CS_ILUNI 0     // wc_mb fail to encode Unicode to charset
#define OB_CS_TOOSMALL  -101  /* Need at least one byte:    wc_mb and mb_wc */
#define OB_CS_TOOSMALL2 -102  /* Need at least two bytes:   wc_mb and mb_wc */
#define OB_CS_TOOSMALL3 -103  /* Need at least three bytes: wc_mb and mb_wc */
/* These following three are currently not really used */
#define OB_CS_TOOSMALL4 -104  /* Need at least 4 bytes: wc_mb and mb_wc */
#define OB_CS_TOOSMALL5 -105  /* Need at least 5 bytes: wc_mb and mb_wc */
#define OB_CS_TOOSMALL6 -106  /* Need at least 6 bytes: wc_mb and mb_wc */
/* A helper macros for "need at least n bytes" */
#define OB_CS_TOOSMALLN(n)    (-100-(n))

#define OB_SEQ_INTTAIL	1
#define OB_SEQ_SPACES	2

#define OB_CS_COMPILED  1
#define OB_CS_CONFIG    2
#define OB_CS_INDEX     4
#define OB_CS_LOADED    8
#define OB_CS_BINSORT 16
#define OB_CS_PRIMARY 32
#define OB_CS_STRNXFRM  64
#define OB_CS_UNICODE 128
#define OB_CS_READY 256
#define OB_CS_AVAILABLE 512
#define OB_CS_CSSORT  1024
#define OB_CS_HIDDEN  2048
#define OB_CS_PUREASCII 4096
#define OB_CS_NONASCII  8192
#define OB_CS_UNICODE_SUPPLEMENT 16384
#define OB_CS_LOWER_SORT 32768
#define OB_CHARSET_UNDEFINED 0

/* Character repertoire flags */
#define OB_REPERTOIRE_ASCII      1
#define OB_REPERTOIRE_EXTENDED   2
#define OB_REPERTOIRE_UNICODE30  3

/* Flags for strxfrm */
#define OB_STRXFRM_LEVEL1          0x00000001
#define OB_STRXFRM_LEVEL2          0x00000002
#define OB_STRXFRM_LEVEL3          0x00000004
#define OB_STRXFRM_LEVEL4          0x00000008
#define OB_STRXFRM_LEVEL5          0x00000010
#define OB_STRXFRM_LEVEL6          0x00000020
#define OB_STRXFRM_LEVEL_ALL       0x0000003F
#define OB_STRXFRM_NLEVELS         6         

#define OB_STRXFRM_PAD_WITH_SPACE  0x00000040
#define OB_STRXFRM_PAD_TO_MAXLEN   0x00000080

#define OB_STRXFRM_DESC_LEVEL1     0x00000100
#define OB_STRXFRM_DESC_LEVEL2     0x00000200
#define OB_STRXFRM_DESC_LEVEL3     0x00000300
#define OB_STRXFRM_DESC_LEVEL4     0x00000800
#define OB_STRXFRM_DESC_LEVEL5     0x00001000
#define OB_STRXFRM_DESC_LEVEL6     0x00002000
#define OB_STRXFRM_DESC_SHIFT      8

#define OB_STRXFRM_UNUSED_00004000 0x00004000
#define OB_STRXFRM_UNUSED_00008000 0x00008000

#define OB_STRXFRM_REVERSE_LEVEL1  0x00010000
#define OB_STRXFRM_REVERSE_LEVEL2  0x00020000
#define OB_STRXFRM_REVERSE_LEVEL3  0x00040000
#define OB_STRXFRM_REVERSE_LEVEL4  0x00080000
#define OB_STRXFRM_REVERSE_LEVEL5  0x00100000
#define OB_STRXFRM_REVERSE_LEVEL6  0x00200000
#define OB_STRXFRM_REVERSE_SHIFT   16

#define	_MY_U	01	/* Upper case */
#define	_MY_L	02	/* Lower case */
#define	_MY_NMR	04	/* Numeral (digit) */
#define	_MY_SPC	010	/* Spacing character */
#define	_MY_PNT	020	/* Punctuation */
#define	_MY_CTR	040	/* Control character */
#define	_MY_B	0100	/* Blank */
#define	_MY_X	0200	/* heXadecimal digit */

#define ob_charset_assert(condition) \
  if (!(condition)) {\
    while(1) {\
      sleep(120);\
    }\
  }

struct ObCharsetInfo;

typedef char        ob_bool; /* Small bool */
#define ob_wc_t ulong

#define OB_CS_REPLACEMENT_CHARACTER 0xFFFD

/* Internal error numbers (for assembler functions) */
#define OB_ERRNO_EDOM		33
#define OB_ERRNO_ERANGE		34

/* Some typedef to make it easy for C++ to make function pointers */
typedef int (*ob_charset_conv_mb_wc)(const struct ObCharsetInfo *,
                                     ob_wc_t *, const uchar *, const uchar *);
typedef int (*ob_charset_conv_wc_mb)(const struct ObCharsetInfo *, ob_wc_t,
                                     uchar *, uchar *);
typedef size_t (*ob_charset_conv_case)(const struct ObCharsetInfo *,
                                       char *, size_t, char *, size_t);

#define OB_UCA_MAX_CONTRACTION 6
#define OB_UCA_MAX_WEIGHT_SIZE 8
#define OB_UCA_WEIGHT_LEVELS   1

typedef struct ob_contraction_t
{
  ob_wc_t ch[OB_UCA_MAX_CONTRACTION];   /* Character sequence              */
  uint16 weight[OB_UCA_MAX_WEIGHT_SIZE];/* Its weight string, 0-terminated */
  ob_bool with_context;
} ObContraction;

typedef struct ob_contraction_list_t
{
  size_t nitems;         /* Number of items in the list                  */
  ObContraction *item;  /* List of contractions                         */
  char *flags;           /* Character flags, e.g. "is contraction head") */
} ObContractions;

typedef struct ob_uca_level_info_st
{
  ob_wc_t maxchar;
  uchar   *lengths;
  uint16  **weights;
  ObContractions contractions;
} ObUCAWeightLevel;

typedef struct uca_info_st
{
  ObUCAWeightLevel level[OB_UCA_WEIGHT_LEVELS];
  /* Logical positions */
  ob_wc_t first_non_ignorable;
  ob_wc_t last_non_ignorable;
  ob_wc_t first_primary_ignorable;
  ob_wc_t last_primary_ignorable;
  ob_wc_t first_secondary_ignorable;
  ob_wc_t last_secondary_ignorable;
  ob_wc_t first_tertiary_ignorable;
  ob_wc_t last_tertiary_ignorable;
  ob_wc_t first_trailing;
  ob_wc_t last_trailing;
  ob_wc_t first_variable;
  ob_wc_t last_variable;
} ObUCAInfo;

extern ObUCAInfo ob_uca_v400;
extern uchar uca520_length[4352];
extern uint16 *uca520_weight[4352];
extern uchar uca_length[256];
extern uint16 *uca_weight[256];

typedef struct
{
  uint beg;
  uint end;
  uint mb_len;
} ob_match_t;

typedef struct ObUnicaseInfoChar
{
  uint32 toupper;
  uint32 tolower;
  uint32 sort;
} ObUnicaseInfoChar;


typedef struct ObUnicaseInfo
{
  ob_wc_t maxchar;
  const ObUnicaseInfoChar **page;
} ObUnicaseInfo;

typedef struct ObCharsetHandler
{
  //ob_bool (*init)(struct ObCharsetInfo *, MY_CHARSET_LOADER *loader);
  /* Multibyte routines */
  uint    (*ismbchar)(const struct ObCharsetInfo *, const char *,
                      const char *);
  uint    (*mbcharlen)(const struct ObCharsetInfo *, uint c);
  size_t  (*numchars)(const struct ObCharsetInfo *, const char *b,
                      const char *e);
  size_t  (*charpos)(const struct ObCharsetInfo *, const char *b,
                     const char *e, size_t pos);
  size_t  (*max_bytes_charpos)(const struct ObCharsetInfo *, const char *b,
      const char *e, size_t max_bytes, size_t *char_len);
  size_t  (*well_formed_len)(const struct ObCharsetInfo *,
                             const char *b,const char *e,
                             size_t nchars, int *error);
  size_t  (*lengthsp)(const struct ObCharsetInfo *, const char *ptr,
                      size_t length);
  /*size_t  (*numcells)(const struct ObCharsetInfo *, const char *b,
                      const char *e);*/

  /* Unicode conversion */
  ob_charset_conv_mb_wc mb_wc;
  ob_charset_conv_wc_mb wc_mb;

  /* CTYPE scanner */
  int (*ctype)(const struct ObCharsetInfo *cs, int *ctype,
               const uchar *s, const uchar *e);

  /* Functions for case and sort conversion */
  /*size_t  (*caseup_str)(const struct ObCharsetInfo *, char *);
  size_t  (*casedn_str)(const struct ObCharsetInfo *, char *);*/

  ob_charset_conv_case caseup;
  ob_charset_conv_case casedn;

  /* Charset dependant snprintf() */
  /*size_t (*snprintf)(const struct ObCharsetInfo *, char *to, size_t n,
                     const char *fmt,
                     ...) __attribute__((format(printf, 4, 5)));
  size_t (*long10_to_str)(const struct ObCharsetInfo *, char *to, size_t n,
                          int radix, long int val);
  size_t (*longlong10_to_str)(const struct ObCharsetInfo *, char *to,
                              size_t n, int radix, longlong val);*/

  void (*fill)(const struct ObCharsetInfo *, char *to, size_t len, int fill);

  /* String-to-number conversion routines */
  long        (*strntol)(const struct ObCharsetInfo *, const char *s,
                         size_t l, int base, char **e, int *err);
  ulong      (*strntoul)(const struct ObCharsetInfo *, const char *s,
                         size_t l, int base, char **e, int *err);
  longlong   (*strntoll)(const struct ObCharsetInfo *, const char *s,
                         size_t l, int base, char **e, int *err);
  ulonglong (*strntoull)(const struct ObCharsetInfo *, const char *s,
                         size_t l, int base, char **e, int *err);
  double      (*strntod)(const struct ObCharsetInfo *, char *s,
                         size_t l, char **e, int *err);
  /*longlong    (*strtoll10)(const struct ObCharsetInfo *cs,
                           const char *nptr, char **endptr, int *error);*/
  ulonglong   (*strntoull10rnd)(const struct ObCharsetInfo *cs,
                                const char *str, size_t length,
                                int unsigned_fl,
                                char **endptr, int *error);
  size_t        (*scan)(const struct ObCharsetInfo *, const char *b,
                        const char *e, int sq);
} ObCharsetHandler;


static const int HASH_BUFFER_LENGTH = 128;

typedef uint64_t (*hash_algo)(const void* input, uint64_t length, uint64_t seed);

typedef struct ObCollationHandler
{
  //bool (*init)(ObCharsetInfo *, ObCharsetLoader *);
  /* Collation routines */
  // Functions that do string comparisons
  int     (*strnncoll)(const struct ObCharsetInfo *,
               const uchar *, size_t, const uchar *, size_t, bool);
  // Ignore trailing spaces when comparing strings
  int     (*strnncollsp)(const struct ObCharsetInfo *,
                         const uchar *, size_t, const uchar *, size_t,
                         bool diff_if_only_endspace_difference);
  // makes a sort key suitable for memcmp() corresponding to the given string
  size_t  (*strnxfrm)(const struct ObCharsetInfo *,
                      uchar *dst, size_t dstlen, uint nweights,
                      const uchar *src, size_t srclen, uint flags, bool *is_valid_unicode);
  //size_t    (*strnxfrmlen)(const struct ObCharsetInfo *, size_t);

  // creates a LIKE range, for optimizer, the query range module is used
  bool (*like_range)(const struct ObCharsetInfo *,
            const char *s, size_t s_length,
            pchar w_prefix, pchar w_one, pchar w_many,
            size_t res_length,
            char *min_str, char *max_str,
            size_t *min_len, size_t *max_len);
  // wildcard comparison, for LIKE
  int     (*wildcmp)(const struct ObCharsetInfo *,
  		     const char *str,const char *str_end,
                     const char *wildstr,const char *wildend,
                     int escape,int w_one, int w_many);

  /*int  (*strcasecmp)(const struct ObCharsetInfo *, const char *,
                     const char *);*/

  // finds the first substring appearance in the string
  uint (*instr)(const struct ObCharsetInfo *,
                const char *b, size_t b_length,
                const char *s, size_t s_length,
                ob_match_t *match, uint nmatch);

  /* Hash calculation */
  // calculates hash value taking into account the collation rules, e.g. case-insensitivity
  void (*hash_sort)(const struct ObCharsetInfo *cs, const uchar *key, size_t len, ulong *nr1,
                    ulong *nr2, const bool calc_end_space, hash_algo hash_algo);
  bool (*propagate)(const struct ObCharsetInfo *cs, const uchar *str,
                       size_t len);
} ObCollationHandler;

typedef struct ObCharsetInfo
{
  uint      number;
  uint      primary_number;
  uint      binary_number;
  uint      state;
  const char *csname;
  const char *name;
  const char *comment;
  const char *tailoring;
  uchar    *ctype;
  uchar    *to_lower;
  uchar    *to_upper;
  uchar    *sort_order;
  ObUCAInfo *uca;
  //uint16      *tab_to_uni;
  //MY_UNI_IDX  *tab_from_uni;
  ObUnicaseInfo *caseinfo;
  uchar     *state_map;
  uchar     *ident_map;
  uint      strxfrm_multiply;
  uchar     caseup_multiply;
  uchar     casedn_multiply;
  uint      mbminlen;
  uint      mbmaxlen;
  ob_wc_t   min_sort_char;
  ob_wc_t   max_sort_char; /* For LIKE optimization */
  uchar     pad_char;
  bool   escape_with_backslash_is_dangerous;
  uchar     levels_for_compare;
  uchar     levels_for_order;

  ObCharsetHandler *cset;
  ObCollationHandler *coll;

} ObCharsetInfo;

#define	ob_isascii(c)	(0 == ((c) & ~0177))
#define	ob_toascii(c)	((c) & 0177)
#define ob_tocntrl(c)	((c) & 31)
#define ob_toprint(c)	((c) | 64)
#define ob_toupper(s,c)	(char) ((s)->to_upper[(uchar) (c)])
#define ob_tolower(s,c)	(char) ((s)->to_lower[(uchar) (c)])
#define ob_sort_order(s,c) (char)((s)->sort_order[(uchar)(c)])
#define	ob_isalpha(s, c)  ((s)->ctype != NULL ? ((s)->ctype+1)[(uchar) (c)] & (_MY_U | _MY_L) : 0)
#define	ob_isupper(s, c)  ((s)->ctype != NULL ? ((s)->ctype+1)[(uchar) (c)] & _MY_U : 0)
#define	ob_islower(s, c)  ((s)->ctype != NULL ? ((s)->ctype+1)[(uchar) (c)] & _MY_L : 0)
#define	ob_isdigit(s, c)  ((s)->ctype != NULL ? ((s)->ctype+1)[(uchar) (c)] & _MY_NMR : 0)
#define	ob_isxdigit(s, c) ((s)->ctype != NULL ? ((s)->ctype+1)[(uchar) (c)] & _MY_X : 0)
#define	ob_isalnum(s, c)  ((s)->ctype != NULL ? ((s)->ctype+1)[(uchar) (c)] & (_MY_U | _MY_L | _MY_NMR) : 0)
#define	ob_isspace(s, c)  ((s)->ctype != NULL ? ((s)->ctype+1)[(uchar) (c)] & _MY_SPC : 0)
#define	ob_ispunct(s, c)  ((s)->ctype != NULL ? ((s)->ctype+1)[(uchar) (c)] & _MY_PNT : 0)
#define	ob_isprint(s, c)  ((s)->ctype != NULL ? ((s)->ctype+1)[(uchar) (c)] & (_MY_PNT | _MY_U | _MY_L | _MY_NMR | _MY_B) : 0)
#define	ob_isgraph(s, c)  ((s)->ctype != NULL ? ((s)->ctype+1)[(uchar) (c)] & (_MY_PNT | _MY_U | _MY_L | _MY_NMR) : 0)

#define use_mb(s)                     ((s)->cset->ismbchar != NULL)
static inline uint ob_ismbchar(const ObCharsetInfo *cs, const char *str,
                               const char *strend)
{
  return cs->cset->ismbchar(cs, str, strend);
}

typedef struct ob_uni_ctype
{
  uchar  pctype;
  uchar  *ctype;
} ObUniCtype;

extern ObUniCtype ob_uni_ctype[256];

//=============================================================================

extern ObUnicaseInfo ob_unicase_default;

//=============================================================================

extern ObCharsetInfo ob_charset_bin;
extern ObCharsetInfo ob_charset_utf8mb4_bin;
extern ObCharsetInfo ob_charset_utf8mb4_general_ci;
extern ObCharsetInfo ob_charset_latin1;
extern ObCharsetInfo ob_charset_latin1_bin;
extern ObCharsetInfo ob_charset_gbk_chinese_ci;
extern ObCharsetInfo ob_charset_gbk_bin;
extern ObCharsetInfo ob_charset_utf16_general_ci;
extern ObCharsetInfo ob_charset_utf16_bin;
extern ObCharsetInfo ob_charset_gb18030_chinese_ci;
extern ObCharsetInfo ob_charset_gb18030_bin;

extern ObCollationHandler ob_collation_mb_bin_handler;
extern ObCharsetHandler ob_charset_utf8mb4_handler;
extern ObCharsetHandler ob_charset_utf16_handler;
extern ObCollationHandler ob_collation_binary_handler;
extern ObCollationHandler ob_collation_8bit_simple_ci_handler;

//=============================================================================

void ob_fill_8bit(const ObCharsetInfo *cs, char* to, size_t l, int fill);

long       ob_strntol_8bit(const ObCharsetInfo *, const char *s, size_t l,
                           int base, char **e, int *err);
ulong      ob_strntoul_8bit(const ObCharsetInfo *, const char *s, size_t l,
                            int base, char **e, int *err);
longlong   ob_strntoll_8bit(const ObCharsetInfo *, const char *s, size_t l,
                            int base, char **e, int *err);
ulonglong ob_strntoull_8bit(const ObCharsetInfo *, const char *s, size_t l,
                            int base, char **e, int *err);
double      ob_strntod_8bit(const ObCharsetInfo *, char *s, size_t l, char **e,
			    int *err);
/*size_t ob_long10_to_str_8bit(const ObCharsetInfo *, char *to, size_t l,
                             int radix, long int val);
size_t ob_longlong10_to_str_8bit(const ObCharsetInfo *, char *to, size_t l,
                                 int radix, longlong val);

longlong ob_strtoll10_8bit(const ObCharsetInfo *cs,
                           const char *nptr, char **endptr, int *error);*/

ulonglong ob_strntoull10rnd_8bit(const ObCharsetInfo *cs,
                                 const char *str, size_t length, int
                                 unsigned_fl, char **endptr, int *error);

size_t ob_scan_8bit(const ObCharsetInfo *cs, const char *b, const char *e,
                    int sq);

//======================================================================

/* For 8-bit character set */
bool  ob_like_range_simple(const ObCharsetInfo *cs,
			      const char *ptr, size_t ptr_length,
			      pbool escape, pbool w_one, pbool w_many,
			      size_t res_length,
			      char *min_str, char *max_str,
			      size_t *min_length, size_t *max_length);

bool ob_propagate_simple(const ObCharsetInfo *cs, const uchar *str,
                            size_t len);
bool ob_propagate_complex(const ObCharsetInfo *cs, const uchar *str,
                             size_t len);

void ob_strxfrm_desc_and_reverse(uchar *str, uchar *strend,
                                 uint flags, uint level);

size_t ob_strxfrm_pad_desc_and_reverse(const ObCharsetInfo *cs,
                                       uchar *str, uchar *frmend, uchar *strend,
                                       uint nweights, uint flags, uint level);
int64_t ob_strntoll(const char *ptr, size_t len, int base, char **end, int *err);
int64_t ob_strntoull(const char *ptr, size_t len, int base, char **end, int *err);

bool ob_like_range_mb(const ObCharsetInfo *cs,
			 const char *ptr,size_t ptr_length,
			 pbool escape, pbool w_one, pbool w_many,
			 size_t res_length,
			 char *min_str,char *max_str,
       size_t *min_length,size_t *max_length);

int ob_wildcmp_mb(const ObCharsetInfo *cs,
                  const char *str,const char *str_end,
                  const char *wildstr,const char *wildend,
                  int escape, int w_one, int w_many);

int ob_wildcmp_mb_impl(const ObCharsetInfo *cs,
                       const char *str,const char *str_end,
                       const char *wildstr,const char *wildend,
                       int escape, int w_one, int w_many, int recurse_level);

uint ob_instr_mb(const ObCharsetInfo *cs,
                 const char *b, size_t b_length,
                 const char *s, size_t s_length,
                 ob_match_t *match, uint nmatch);

void ob_hash_sort_simple(const ObCharsetInfo *cs,
				const uchar *key, size_t len,
                ulong *nr1, ulong *nr2,
        const bool calc_end_space, hash_algo hash_algo);

const uchar *skip_trailing_space(const uchar *ptr,size_t len);

size_t ob_numchars_mb(const ObCharsetInfo *cs __attribute__((unused)), const char *pos, const char *end);

size_t ob_charpos_mb(const ObCharsetInfo *cs __attribute__((unused)), const char *pos, const char *end, size_t length);

size_t ob_max_bytes_charpos_mb(const ObCharsetInfo *cs __attribute__((unused)), const char *pos, const char *end, size_t max_bytes, size_t *char_len);

int ob_mb_ctype_mb(const ObCharsetInfo *cs __attribute__((unused)), int *ctype,
                   const uchar *s, const uchar *e);

size_t ob_caseup_mb(const ObCharsetInfo *, char *src, size_t srclen,
                                         char *dst, size_t dstlen);

size_t ob_casedn_mb(const ObCharsetInfo *, char *src, size_t srclen,
                                         char *dst, size_t dstlen);

const ObContractions *ob_charset_get_contractions(const ObCharsetInfo *cs,
                                                   int level);

bool ob_uca_can_be_contraction_head(const ObContractions *c, ob_wc_t wc);

bool ob_uca_can_be_contraction_tail(const ObContractions *c, ob_wc_t wc);

uint16 *ob_uca_contraction2_weight(const ObContractions *list, ob_wc_t wc1, ob_wc_t wc2);

size_t ob_lengthsp_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                        const char *ptr, size_t length);

int ob_strnncoll_mb_bin(const ObCharsetInfo *cs __attribute__((unused)),
                    const uchar *s, size_t slen,
                    const uchar *t, size_t tlen,
                        bool t_is_prefix);

int ob_strnncollsp_mb_bin(const ObCharsetInfo *cs __attribute__((unused)),
                      const uchar *a, size_t a_length,
                      const uchar *b, size_t b_length,
                          bool diff_if_only_endspace_difference);

size_t ob_strnxfrm_mb(const ObCharsetInfo *,
                      uchar *dst, size_t dstlen, uint nweights,
                      const uchar *src, size_t srclen, uint flags, bool *is_valid_unicode);

int ob_wildcmp_mb_bin(const ObCharsetInfo *cs,
                  const char *str,const char *str_end,
                  const char *wildstr,const char *wildend,
                      int escape, int w_one, int w_many);

void ob_hash_sort_mb_bin(const ObCharsetInfo *cs __attribute__((unused)),
                         const uchar *key, size_t len, ulong *nr1, ulong *nr2,
                         const bool calc_end_space, hash_algo hash_algo);

uint32 ob_convert(char *to, uint32 to_length, const ObCharsetInfo *to_cs,
                  const char *from, uint32 from_length,
                  const ObCharsetInfo *from_cs, uint *errors);

size_t ob_strnxfrm_unicode_full_bin(const ObCharsetInfo *cs,
                             uchar *dst, size_t dstlen, uint nweights,
                             const uchar *src, size_t srclen, uint flags, bool *is_valid_unicode);

bool ob_like_range_generic(const ObCharsetInfo *cs, const char *ptr,
                              size_t ptr_length, char escape, char w_one,
                              char w_many, size_t res_length, char *min_str,
                              char *max_str, size_t *min_length,
                              size_t *max_length);

size_t ob_strnxfrm_unicode(const ObCharsetInfo *cs,
                    uchar *dst, size_t dstlen, uint nweights,
                    const uchar *src, size_t srclen, uint flags, bool *is_valid_unicode);

int ob_wildcmp_unicode(const ObCharsetInfo *cs,
                   const char *str,const char *str_end,
                   const char *wildstr,const char *wildend,
                   int escape, int w_one, int w_many,
                   ObUnicaseInfo *weights);

size_t ob_strxfrm_pad(const ObCharsetInfo *cs, uchar *str, uchar *frmend,
                      uchar *strend, uint nweights, uint flags);

uint ob_mbcharlen_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                       uint c __attribute__((unused)));

size_t ob_numchars_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                                   const char *b, const char *e);

size_t ob_charpos_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                       const char *b  __attribute__((unused)),
                       const char *e  __attribute__((unused)),
                       size_t pos);


size_t ob_max_bytes_charpos_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                                 const char *b  __attribute__((unused)),
                                 const char *e  __attribute__((unused)),
                                 size_t max_bytes,
                                 size_t *char_len);

size_t ob_well_formed_len_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                               const char *start, const char *end,
                               size_t nchars, int *error);

size_t ob_lengthsp_binary(const ObCharsetInfo *cs __attribute__((unused)),
                          const char *ptr __attribute__((unused)),
                          size_t length);

int ob_mb_ctype_8bit(const ObCharsetInfo *cs, int *ctype,
                     const uchar *s, const uchar *e);

size_t ob_caseup_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                      char* src __attribute__((unused)), size_t srclen __attribute__((unused)),
                      char* dst __attribute__((unused)), size_t dstlen __attribute__((unused)));

size_t ob_casedn_8bit(const ObCharsetInfo *cs __attribute__((unused)),
                      char* src __attribute__((unused)), size_t srclen __attribute__((unused)),
                      char* dst __attribute__((unused)), size_t dstlen __attribute__((unused)));

#ifdef	__cplusplus
}
#endif

#endif /* OCEANBASE_LIB_OBMYSQL_OB_CTYPE_ */

