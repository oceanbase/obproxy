%define api.pure
%parse-param {ObFuncExprParseResult* result}
%locations
%no-lines
%verbose
%{
#include <stdint.h>
#include "opsql/ob_proxy_parse_type.h"
#include "opsql/ob_proxy_parse_define.h"
#include "opsql/func_expr_parser/ob_func_expr_parse_result.h"
%}

%union
{
  int64_t              num;
  ObProxyParseString   str;
  ObProxyParamNode     *param_node;
  ObFuncExprNode        *func_node;
  ObProxyParamNodeList *list;
  ObProxyExprType function_type;
};

%{
#include "ob_func_expr_parser_lex.h"
#define YYLEX_PARAM result->yyscan_info_
extern void yyerror(YYLTYPE* yylloc, ObFuncExprParseResult* p, char* s,...);
extern void *obproxy_parse_malloc(const size_t nbyte, void *malloc_pool);

static inline void add_param_node(ObProxyParamNodeList *list, ObFuncExprParseResult *result,
                                  ObProxyParamNode *node)
{
  UNUSED(result); // use for perf later
  if (OB_ISNULL(list) || OB_ISNULL(node)) {
    // do nothing
  } else {
    if (NULL != list->tail_) {
      list->child_num_++;
      list->tail_->next_ = node;
      list->tail_ = node;
    }
  }
}

#define malloc_param_node(node, result, type)                                                 \
  do {                                                                                        \
    if (OB_ISNULL(node = ((ObProxyParamNode *)obproxy_parse_malloc(sizeof(ObProxyParamNode),  \
                                                                   result->malloc_pool_)))) { \
      YYABORT;                                                                                \
    } else {                                                                                  \
      node->type_ = type;                                                                     \
      node->next_ = NULL;                                                                     \
    }                                                                                         \
  } while(0)                                                                                  \

#define malloc_func_expr_node(func_node, result, name)                                        \
  do {                                                                                        \
    if (OB_ISNULL(func_node = ((ObFuncExprNode *)obproxy_parse_malloc(sizeof(ObFuncExprNode), \
                                                                   result->malloc_pool_)))) { \
      YYABORT;                                                                                \
    } else {                                                                                  \
      func_node->func_name_ = name;                                                           \
      func_node->child_ = NULL;                                                               \
    }                                                                                         \
  } while(0)                                                                                  \

#define malloc_list(list, result, node)                                                       \
  do {                                                                                        \
    if (OB_ISNULL(list = ((ObProxyParamNodeList *)obproxy_parse_malloc(                       \
      sizeof(ObProxyParamNodeList), result->malloc_pool_)))) {                                \
      YYABORT;                                                                                \
    } else if (OB_ISNULL(node))  {                                                            \
      YYABORT;                                                                                \
    } else {                                                                                  \
      list->head_ = node;                                                                     \
      list->tail_ = node;                                                                     \
      list->child_num_ = 1;                                                                   \
    }                                                                                         \
  } while(0)                                                                                  \

#define store_str_val(parse_str, str, str_len)   \
  do {                                           \
    parse_str.str_ = NULL;                       \
    parse_str.str_len_ = 0;                      \
    parse_str.end_ptr_ = NULL;                   \
    if (str != 0 && str_len >= 0) {              \
      parse_str.str_ = str;                      \
      parse_str.str_len_ = str_len;              \
      parse_str.end_ptr_ = str + str_len;        \
    }                                            \
  } while (0)

%}

 /* dummy node */
%token DUMMY_FUNCTION_CLAUSE
 /* reserved keyword */
%token TOKEN_SPECIAL FUNC_SUBSTR FUNC_CONCAT FUNC_HASH FUNC_TOINT FUNC_DIV FUNC_ADD FUNC_SUB FUNC_MUL FUNC_TESTLOAD
%token FUNC_TO_DATE FUNC_TO_TIMESTAMP FUNC_NVL FUNC_TO_CHAR FUNC_MOD FUNC_SYSDATE FUNC_ISNULL FUNC_CEIL FUNC_FLOOR
%token FUNC_LTRIM FUNC_RTRIM FUNC_TRIM FUNC_REPLACE FUNC_LENGTH FUNC_UPPER FUNC_LOWER TRIM_FROM TRIM_BOTH TRIM_LEADING TRIM_TRAILING FUNC_TO_NUMBER
%token FUNC_ROUND FUNC_TRUNCATE FUNC_ABS FUNC_SYSTIMESTAMP FUNC_CURRENTDATE FUNC_CURRENTTIME FUNC_CURRENTTIMESTAMP
%token END_P ERROR IGNORED_WORD
 /* expression priority */
 %left '&'
 %left '+' '-'
 %left '*' '/' '%'
 %left '(' ')'
 /* type token */
%token<str> NAME_OB STR_VAL NUMBER_VAL NONE_PARAM_FUNC
%token<num> INT_VAL
%type<list> param_list
%type<func_node> func_expr
%type<param_node> param simple_expr
%start start
%%
start: func_root

func_root: simple_expr
           {
             result->param_node_ = $1;
             YYACCEPT;
           }
           | error { YYABORT; }

func_expr: NAME_OB '(' param_list ')'
          {
            malloc_func_expr_node($$, result, $1);
            $$->child_ = $3;
          }
          | NAME_OB '(' ')'
          {
            malloc_func_expr_node($$, result, $1);
            $$->child_ = NULL;
          }
          | NONE_PARAM_FUNC
          {
            malloc_func_expr_node($$, result, $1);
            $$->child_ = NULL;
          }
          | NONE_PARAM_FUNC '(' ')'
          {
            malloc_func_expr_node($$, result, $1);
            $$->child_ = NULL;
          }
          | NONE_PARAM_FUNC '(' param_list ')'
          {
            malloc_func_expr_node($$, result, $1);
            $$->child_ = $3;
          }

simple_expr: param { $$ = $1; }
           | '(' simple_expr ')' {$$ = $2; }
           | '+' simple_expr %prec '*' { $$ = $2; }
           | '-' simple_expr %prec '*'
           {
            ObFuncExprNode *dummyfunc = NULL;
            ObProxyParamNodeList *dummylist = NULL;
            ObProxyParamNode * dummynode = NULL;
            
            // -mod(2,1)-> 0 - mod(2,1)
            malloc_param_node(dummynode, result, PARAM_INT_VAL);
            dummynode->int_value_ = 0;
            malloc_list(dummylist, result, dummynode);
            add_param_node(dummylist, result, $2);

            ObProxyParseString str;
            store_str_val(str, "-", 1);
            malloc_func_expr_node(dummyfunc, result, str);
            dummyfunc->child_ = dummylist;
            malloc_param_node($$, result, PARAM_FUNC);
            $$->func_expr_node_ = dummyfunc;
           }
           | simple_expr '+' simple_expr
           {
            ObFuncExprNode *dummyfunc = NULL;
            ObProxyParamNodeList *dummylist = NULL;

            malloc_list(dummylist, result, $1);
            add_param_node(dummylist, result, $3);

            ObProxyParseString str;
            store_str_val(str, "+", 1);
            malloc_func_expr_node(dummyfunc, result, str);
            dummyfunc->child_ = dummylist;
            malloc_param_node($$, result, PARAM_FUNC);
            $$->func_expr_node_ = dummyfunc;
           }
           | simple_expr '-' simple_expr
           {
            ObFuncExprNode *dummyfunc = NULL;
            ObProxyParamNodeList *dummylist = NULL;

            malloc_list(dummylist, result, $1);
            add_param_node(dummylist, result, $3);
            ObProxyParseString str;
            store_str_val(str, "-", 1);
            malloc_func_expr_node(dummyfunc, result, str);
            dummyfunc->child_ = dummylist;
            malloc_param_node($$, result, PARAM_FUNC);
            $$->func_expr_node_ = dummyfunc;
           }
           | simple_expr '*' simple_expr
           {
            ObFuncExprNode *dummyfunc = NULL;
            ObProxyParamNodeList *dummylist = NULL;
            
            malloc_list(dummylist, result, $1);
            add_param_node(dummylist, result, $3);
            ObProxyParseString str;
            store_str_val(str, "*", 1);
            malloc_func_expr_node(dummyfunc, result, str);
            dummyfunc->child_ = dummylist;
            malloc_param_node($$, result, PARAM_FUNC);
            $$->func_expr_node_ = dummyfunc;
           }
           | simple_expr '/' simple_expr
           {
            ObFuncExprNode *dummyfunc = NULL;
            ObProxyParamNodeList *dummylist = NULL;
            
            malloc_list(dummylist, result, $1);
            add_param_node(dummylist, result, $3);
            ObProxyParseString str;
            store_str_val(str, "/", 1);
            malloc_func_expr_node(dummyfunc, result, str);
            dummyfunc->child_ = dummylist;
            malloc_param_node($$, result, PARAM_FUNC);
            $$->func_expr_node_ = dummyfunc;
           }
           | simple_expr '%' simple_expr
           {
            ObFuncExprNode *dummyfunc = NULL;
            ObProxyParamNodeList *dummylist = NULL;
            
            malloc_list(dummylist, result, $1);
            add_param_node(dummylist, result, $3);
            ObProxyParseString str;
            store_str_val(str, "%", 1);
            malloc_func_expr_node(dummyfunc, result, str);
            dummyfunc->child_ = dummylist;
            malloc_param_node($$, result, PARAM_FUNC);
            $$->func_expr_node_ = dummyfunc;
           }

param_list: simple_expr
          {
            malloc_list($$, result, $1);
          }
          | param_list ',' simple_expr
          {
            add_param_node($1, result, $3);
            $$ = $1;
          }

param: TOKEN_SPECIAL NAME_OB TOKEN_SPECIAL { malloc_param_node($$, result, PARAM_COLUMN); $$->col_name_ = $2; }
     | NAME_OB { malloc_param_node($$, result, PARAM_COLUMN); $$->col_name_ = $1; }
     | INT_VAL { malloc_param_node($$, result, PARAM_INT_VAL); $$->int_value_ = $1; }
     | STR_VAL { malloc_param_node($$, result, PARAM_STR_VAL); $$->str_value_ = $1; }
     | func_expr { malloc_param_node($$, result, PARAM_FUNC); $$->func_expr_node_ = $1; }

%%
void yyerror(YYLTYPE* yylloc, ObFuncExprParseResult* p, char* s, ...)
{
  // do nothing
}

void ob_func_expr_parser_fatal_error(yyconst char *msg, yyscan_t yyscanner)
{
  fprintf(stderr, "FATAL ERROR:%s\n", msg);
  ObFuncExprParseResult *p = obfuncexprget_extra(yyscanner);
  if (OB_ISNULL(p)) {
    fprintf(stderr, "unexpected null parse result\n");
  } else {
    longjmp(p->jmp_buf_, 1);//the secord param must be non-zero value
  }
}

int ob_func_expr_parse_sql(ObFuncExprParseResult* p, const char* buf, size_t len)
{
  int ret = OB_SUCCESS;
  //obexprdebug = 1;
  if (OB_ISNULL(p) || OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    // print err msg later
  } else if (OB_FAIL(obfuncexprlex_init_extra(p, &(p->yyscan_info_)))) {
    // print err msg later
  } else {
    int val = setjmp(p->jmp_buf_);
    if (val) {
      ret = OB_PARSER_ERR_PARSE_SQL;
    } else {
      obfuncexpr_scan_bytes((char *)buf, len, p->yyscan_info_);
      if (OB_FAIL(obfuncexprparse(p))) {
        // print err msg later
      } else {
        // do nothing
      }
    }
  }

  return ret;
}
