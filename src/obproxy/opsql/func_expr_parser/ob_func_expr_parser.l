%option noyywrap nounput noinput case-insensitive
%option stack noyyalloc noyyrealloc noyyfree
%option reentrant bison-bridge bison-locations
%option header-file="ob_func_expr_parser_lex.h"
%{
#define YYSTYPE         OBFUNCEXPRSTYPE
#define YYLTYPE         OBFUNCEXPRLTYPE
#include <stdlib.h>
#include "opsql/ob_proxy_parse_define.h"
#include "opsql/ob_proxy_parse_malloc.h"
#include "opsql/func_expr_parser/ob_func_expr_parse_result.h"
#include "opsql/func_expr_parser/ob_func_expr_parser_tab.h"
extern void ob_func_expr_parser_fatal_error(yyconst char *msg, yyscan_t yyscanner);

void store_func_expr_str(char* str, int64_t str_len, char* end_ptr, void *yyscanner);
void store_func_expr_str_with_quote(ObProxyParseQuoteType type, char* str, int64_t str_len, char* end_ptr, void *yyscanner);

#define YY_FATAL_ERROR(msg) ob_func_expr_parser_fatal_error(msg, yyscanner)

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
  { store_func_expr_str(yytext, yyleng, yytext + yyleng, yyscanner); return STR_VAL; }

#define RETURN_NUMBER_VAL() \
  { store_func_expr_str(yytext, yyleng, yytext + yyleng, yyscanner); return STR_VAL; }

#define RETURN_NAME_OB() \
  { store_func_expr_str(yytext, yyleng, yytext + yyleng, yyscanner); return NAME_OB; }

#define RETURN_NAME_OB_WITH_QUOTE(quote_type) \
{ \
  store_func_expr_str_with_quote(quote_type, yytext + 1, yyleng - 2, yytext + yyleng, yyscanner); \
  ObFuncExprParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p)) {\
    if (GENERATE_FUNC_PARSE_MODE == p->parse_mode_) {\
      return NAME_OB; \
    } \
  } \
  return STR_VAL; \
}

%}

%x                      in_c_comment
%x                      sq
%x                      dq
%x                      bt

space                   [ \t\n\r\f]
identifer               ([A-Za-z0-9$_]*)
int_num                 [\-\+]?[0-9]+
number      ([0-9]+E[-+]?[0-9]+)|([0-9]+"."[0-9]*E[-+]?[0-9]+)|("."[0-9]+E[-+]?[0-9]+)|([0-9]+"."[0-9]*)|("."[0-9]+)

whitespace              ({space}+)
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

leftbracket   \(
rightbracket  \)

endtoken \#

%%
substr      { return FUNC_SUBSTR; }
substring   { return FUNC_SUBSTR; }
concat      { return FUNC_CONCAT; }
hash        { return FUNC_HASH; }
toint       { return FUNC_TOINT; }
div         { return FUNC_DIV; }
add         { return FUNC_ADD; }
sub         { return FUNC_SUB; }
mul         { return FUNC_MUL; }
testload    { return FUNC_TESTLOAD; }

{endtoken}  { return TOKEN_SPECIAL; }

{int_num}              { RETURN_INT_VAL(); }
{number}               { RETURN_NUMBER_VAL(); }
{identifer}            { RETURN_NAME_OB(); }
{sqbegin}{identifer}{sqend} { RETURN_NAME_OB_WITH_QUOTE(OBPROXY_QUOTE_T_SINGLE); }
{dqbegin}{identifer}{dqend} { RETURN_NAME_OB_WITH_QUOTE(OBPROXY_QUOTE_T_DOUBLE); }
{btbegin}{identifer}{btend} { RETURN_NAME_OB_WITH_QUOTE(OBPROXY_QUOTE_T_BACK); }
{whitespace}           { }
[-+().;*&~|^/%,:!?@=]  { return yytext[0]; }

 /* comment */
{c_cmt_begin}             { PUSH_STATE(in_c_comment); }
<in_c_comment>{c_cmt_end} { POP_STATE(); }
<in_c_comment>[\n]        {}
<in_c_comment>.           {}

 /* quote */
{sqbegin} {
  PUSH_STATE(sq);
  ObFuncExprParseResult *p = (ObFuncExprParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_buf_ = (yytext + 1);
    p->tmp_start_ptr_ = yytext;
    p->tmp_len_ = 0;
  }
}

<sq>{sqcontent} {
  ObFuncExprParseResult *p = (ObFuncExprParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_len_ += yyleng;
  }
}

<sq>{sqend} {
  POP_STATE();
  ObFuncExprParseResult *p = (ObFuncExprParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    store_func_expr_str(p->tmp_buf_, p->tmp_len_, p->tmp_start_ptr_ + p->tmp_len_ + 2, yyscanner);
  }
  return STR_VAL;
}

<sq>{sqdouble}            {}
<sq>{qescape}             {}
<sq>{sqnewline}           {}
<sq><<EOF>>               { return ERROR; }


 /* dquote */
{dqbegin} {
  PUSH_STATE(dq);
  ObFuncExprParseResult *p = (ObFuncExprParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_buf_ = (yytext + 1);
    p->tmp_start_ptr_ = yytext;
    p->tmp_len_ = 0;
  }
}

<dq>{dqcontent} {
  ObFuncExprParseResult *p = (ObFuncExprParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_len_ += yyleng;
  }
}

<dq>{dqend} {
  POP_STATE();
  ObFuncExprParseResult *p = (ObFuncExprParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    store_func_expr_str(p->tmp_buf_, p->tmp_len_, p->tmp_start_ptr_ + p->tmp_len_ + 2, yyscanner);
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
  ObFuncExprParseResult *p = (ObFuncExprParseResult *)yyextra;
  if (NULL != p) {
    p->tmp_buf_ = (char *)obproxy_parse_malloc(OBPROXY_MAX_NAME_LENGTH, p->malloc_pool_);
    p->tmp_start_ptr_ = yytext;
    p->tmp_len_ = 0;
  }
}

<bt>{btdouble} {
  ObFuncExprParseResult *p = (ObFuncExprParseResult *)yyextra;
  if (NULL != p && NULL != p->tmp_buf_ && p->tmp_len_ + 1 < OBPROXY_MAX_NAME_LENGTH) {
    p->tmp_buf_[p->tmp_len_++] = '`';
  }
}

<bt>{btcontent} {
  ObFuncExprParseResult *p = (ObFuncExprParseResult *)yyextra;
  if (NULL != p && NULL != p->tmp_buf_ && p->tmp_len_ + yyleng < OBPROXY_MAX_NAME_LENGTH) {
    memcpy(p->tmp_buf_ + p->tmp_len_, yytext, yyleng);
    p->tmp_len_ += yyleng;
  }
}

<bt>{btend} {
  POP_STATE();
  ObFuncExprParseResult *p = (ObFuncExprParseResult *)yyextra;
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
  ObFuncExprParseResult *p = yyget_extra(yyscanner);
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
  ObFuncExprParseResult *p = yyget_extra(yyscanner);
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

inline void store_func_expr_str(char* str, int64_t str_len, char*end_ptr, void *yyscanner)
{
  YYSTYPE *lval = yyget_lval(yyscanner);
  if (OB_ISNULL(lval)) {
    // do nothing
  } else {
    lval->str.str_ = str;
    lval->str.end_ptr_ = end_ptr;
    lval->str.str_len_ = str_len;
    lval->str.quote_type_ = OBPROXY_QUOTE_T_INVALID;
  }
}

inline void store_func_expr_str_with_quote(ObProxyParseQuoteType type, char* str, int64_t str_len, char* end_ptr, void *yyscanner)
{
  YYSTYPE *lval = yyget_lval(yyscanner);
  if (OB_ISNULL(lval)) {
    // do nothing
  } else {
    lval->str.str_ = str;
    lval->str.end_ptr_ = end_ptr;
    lval->str.str_len_ = str_len;
    lval->str.quote_type_ = type;
  }
}
