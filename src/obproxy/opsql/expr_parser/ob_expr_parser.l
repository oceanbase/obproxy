%option noyywrap nounput noinput case-insensitive
%option stack noyyalloc noyyrealloc noyyfree
%option reentrant bison-bridge bison-locations
%option prefix="ob_expr_parser_yy"
%option header-file="ob_expr_parser_lex.h"
%{
#define YYSTYPE         OBEXPRSTYPE
#define YYLTYPE         OBEXPRLTYPE
#include <stdlib.h>
#include "opsql/ob_proxy_parse_define.h"
#include "opsql/ob_proxy_parse_malloc.h"
#include "opsql/expr_parser/ob_expr_parse_result.h"
#include "opsql/expr_parser/ob_expr_parser_tab.h"
extern void ob_expr_parser_fatal_error(yyconst char *msg, yyscan_t yyscanner);

void store_expr_str(char* str, int64_t str_len, char* end_ptr, void *yyscanner);
void store_pos_place_holder(char* str, void *yyscanner);

#define YY_FATAL_ERROR(msg) ob_expr_parser_fatal_error(msg, yyscanner)

#define PUSH_STATE(state) \
  { yy_push_state(state, yyscanner); }

#define POP_STATE(state) \
  { yy_pop_state(yyscanner); }

#define RETURN_INT_VAL() \
do {\
  errno = 0;\
  yylval->num = strtoll(yytext, NULL, 10);\
  if (0 != errno) {\
      yylval->num = 0;\
  }\
  return INT_VAL;\
} while (0);

#define RETURN_STR_VAL() \
  { store_expr_str(yytext, yyleng, yytext + yyleng, yyscanner); return STR_VAL; }

#define RETURN_NUMBER_VAL() \
  { store_expr_str(yytext, yyleng, yytext + yyleng, yyscanner); return STR_VAL; }

#define RETURN_NAME_OB() \
  { store_expr_str(yytext, yyleng, yytext + yyleng, yyscanner); return NAME_OB; }

#define RETURN_ROW_ID() \
  { store_expr_str(yytext, yyleng, yytext + yyleng, yyscanner); return ROW_ID; }

#define RETURN_REVERSED_EXPR_FUNC() \
  { store_expr_str(yytext, yyleng, yytext + yyleng, yyscanner); return REVERSED_EXPR_FUNC; }

#define RETURN_HEX_VAL() \
  { store_expr_str(yytext, yyleng, yytext + yyleng, yyscanner); return HEX_VAL; }

%}

%x                      in_c_comment
%x                      sq
%x                      dq
%x                      bt

/*following character status will be rewrite by gen_parse.sh according to connection character*/
multi_byte_space              [\u3000]
multi_byte_comma              [\uff0c]
multi_byte_left_parenthesis   [\uff08]
multi_byte_right_parenthesis  [\uff09]
hexval                  (x'([a-fA-F0-9]+)')|(0x([a-fA-F0-9]+))
space                   [ \t\n\r\f]
identifer               ([A-Za-z0-9$_]*)

/* limit the length of int_num from 1 to 17 in case of int64_t out of bound, 17 -> [ ( length of 2^64 ) - 2 ]*/
int_num                 [\-\+]?[0-9]{1,17}
number      ([\-]?[0-9]+E[-+]?[0-9]+)|([\-]?[0-9]+"."[0-9]*E[-+]?[0-9]+)|([\-]?"."[0-9]+E[-+]?[0-9]+)|([\-]?[0-9]+"."[0-9]*)|([\-]?"."[0-9]+)|([\-\+]?[0-9]{18,})

non_newline             [^\n\r]
sql_comment             ("--"{space}+{non_newline}*)|(#{non_newline}*)
whitespace              ({space}+|{sql_comment})
c_cmt_begin             \/\*
c_cmt_end               \*+\/

quote         '
sqbegin       {quote}
sqend         {quote}
sqdouble      {quote}{quote}
sqcontent     [^\\']+
qescape       [\\](.|\n)
sqnewline     {quote}{whitespace}{quote}

dquote         \"
dqbegin       {dquote}
dqend         {dquote}
dqdouble      {dquote}{dquote}
dqcontent     [^\\"]+
dqnewline     {dquote}{whitespace}{dquote}

backtick      `
btbegin       {backtick}
btend         {backtick}
btdouble      {backtick}{backtick}
btcontent     [^`]+

comma         ,
leftbracket   \(
rightbracket  \)

%%
WHERE  { return WHERE; }
AS     { return AS; }
VALUES { return VALUES; }
VALUE { return VALUES; }
SET    { return SET; }
FOR    { return END_WHERE; }
LIMIT  { return END_WHERE; }
GROUP  { return END_WHERE; }
HAVING { return END_WHERE; }
ROWID  { RETURN_ROW_ID(); }

JOIN      { return JOIN; }
ON        { return ON; }
BETWEEN   { return BETWEEN; }
AND       { return AND_OP; }
"&&"      { return AND_OP; }
OR        { return OR_OP; }
"||"      { return OR_OP; }
IN        { return IN; }
IS        { return IS; }
NULL      { return TOKEN_NULL; }
NOT       { return NOT; }
"="       { return COMP_EQ; }
"<=>"     { return COMP_NSEQ; }
">="      { return COMP_GE; }
">"       { return COMP_GT; }
"<="      { return COMP_LE; }
"<"       { return COMP_LT; }
"!="|"<>" { return COMP_NE; }
"?"       { return PLACE_HOLDER; }
":"{int_num} { store_pos_place_holder(yytext + 1, yyscanner); return POS_PLACE_HOLDER; }
"SYSDATE" { RETURN_REVERSED_EXPR_FUNC();}

{int_num}              { RETURN_INT_VAL(); }
{number}               { RETURN_NUMBER_VAL(); }
{hexval}               { RETURN_HEX_VAL(); }
{identifer}            { RETURN_NAME_OB(); }
{whitespace}           { }
{comma}|{multi_byte_comma} { return ','; }
{leftbracket}|{multi_byte_left_parenthesis}    { return '('; }
{rightbracket}|{multi_byte_right_parenthesis}  { return ')'; }
[-+.;*&~|^/%:!@=]  { return yytext[0]; }

 /* comment */
{c_cmt_begin}             { PUSH_STATE(in_c_comment); }
<in_c_comment>{c_cmt_end} { POP_STATE(); }
<in_c_comment>[\n]        {}
<in_c_comment>.           {}

 /* quote */
{sqbegin} {
  PUSH_STATE(sq);
  ObExprParseResult *p = (ObExprParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_buf_ = (yytext + 1);
    p->tmp_start_ptr_ = yytext;
    p->tmp_len_ = 0;
  }
}

<sq>{sqcontent} {
  ObExprParseResult *p = (ObExprParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_len_ += yyleng;
  }
}

<sq>{sqend} {
  POP_STATE();
  ObExprParseResult *p = (ObExprParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    store_expr_str(p->tmp_buf_, p->tmp_len_, p->tmp_start_ptr_ + p->tmp_len_ + 2, yyscanner);
  }
  return STR_VAL;
}

<sq>{sqdouble}            {}
<sq>{qescape}             {
  ObExprParseResult *p = (ObExprParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_len_ += yyleng;
  }
}
<sq>{sqnewline}           {}
<sq><<EOF>>               { return ERROR; }


 /* dquote */
{dqbegin} {
  PUSH_STATE(dq);
  ObExprParseResult *p = (ObExprParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_buf_ = (yytext + 1);
    p->tmp_start_ptr_ = yytext;
    p->tmp_len_ = 0;
  }
}

<dq>{dqcontent} {
  ObExprParseResult *p = (ObExprParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_len_ += yyleng;
  }
}

<dq>{dqend} {
  POP_STATE();
  ObExprParseResult *p = (ObExprParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    store_expr_str(p->tmp_buf_, p->tmp_len_, p->tmp_start_ptr_ + p->tmp_len_ + 2, yyscanner);
    if (p->is_oracle_mode_) {
      return NAME_OB;
    }
  }
  return STR_VAL;
}

<dq>{dqdouble}            {}
<dq>{qescape}             {}
<dq>{dqnewline}           {}
<dq><<EOF>>               { return ERROR; }


 /* backtick */
{btbegin} {
  PUSH_STATE(bt);
  ObExprParseResult *p = (ObExprParseResult *)yyextra;
  if (NULL != p) {
    p->tmp_buf_ = (char *)obproxy_parse_malloc(OBPROXY_MAX_NAME_LENGTH, p->malloc_pool_);
    p->tmp_start_ptr_ = yytext;
    p->tmp_len_ = 0;
  }
}

<bt>{btdouble} {
  ObExprParseResult *p = (ObExprParseResult *)yyextra;
  if (NULL != p && NULL != p->tmp_buf_ && p->tmp_len_ + 1 < OBPROXY_MAX_NAME_LENGTH) {
    p->tmp_buf_[p->tmp_len_++] = '`';
  }
}

<bt>{btcontent} {
  ObExprParseResult *p = (ObExprParseResult *)yyextra;
  if (NULL != p && NULL != p->tmp_buf_ && p->tmp_len_ + yyleng < OBPROXY_MAX_NAME_LENGTH) {
    memcpy(p->tmp_buf_ + p->tmp_len_, yytext, yyleng);
    p->tmp_len_ += yyleng;
  }
}

<bt>{btend} {
  POP_STATE();
  ObExprParseResult *p = (ObExprParseResult *)yyextra;
  if (NULL != p && NULL != p->tmp_buf_) {
    yylval->str.str_  = obproxy_parse_strndup(p->tmp_buf_, p->tmp_len_, p->malloc_pool_);
    yylval->str.str_len_ = p->tmp_len_;
    yylval->str.end_ptr_ = p->tmp_start_ptr_ + p->tmp_len_ + 2;
  }
  return NAME_OB;
}

<bt><<EOF>>  {
  return ERROR;
}

<<EOF>>                     { return END_P; }
.                           { return IGNORED_WORD; }
%%

inline void *yyalloc(size_t bytes, void *yyscanner)
{
  void *ptr_ret = NULL;
  ObExprParseResult *p = yyget_extra(yyscanner);
  if (OB_ISNULL(p)) {
    // print err into msg buffer later
  } else {
    ptr_ret = obproxy_parse_malloc(bytes, p->malloc_pool_);
  }
  return ptr_ret;
}

inline void *yyrealloc(void *ptr, size_t bytes, void *yyscanner)
{
  void *ptr_ret = NULL;
  ObExprParseResult *p = yyget_extra(yyscanner);
  if (OB_ISNULL(p)) {
    // print err into msg buffer later
  } else {
    ptr_ret = obproxy_parse_realloc(ptr, bytes, p->malloc_pool_);
  }
  return ptr_ret;

}

inline void yyfree(void *ptr, void *yyscanner)
{
  // Do nothing -- we leave it to the garbage collector.
  obproxy_parse_free(ptr);
}

inline void store_expr_str(char* str, int64_t str_len, char*end_ptr, void *yyscanner)
{
  YYSTYPE *lval = yyget_lval(yyscanner);
  if (OB_ISNULL(lval)) {
    // do nothing
  } else {
    lval->str.str_ = str;
    lval->str.end_ptr_ = end_ptr;
    lval->str.str_len_ = str_len;
  }
}

inline void store_pos_place_holder(char *str, void *yyscanner)
{
  YYSTYPE *lval = yyget_lval(yyscanner);
  if (OB_ISNULL(lval)) {
    // do nothing
  } else {
    errno = 0;
    lval->num = strtoll(str, NULL, 10);
    if (0 != errno) {
      lval->num = 0;
    }
  }
}
