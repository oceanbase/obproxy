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

#define malloc_func_expr_node(func_node, result, type)                                        \
  do {                                                                                        \
    if (OB_ISNULL(func_node = ((ObFuncExprNode *)obproxy_parse_malloc(sizeof(ObFuncExprNode), \
                                                                   result->malloc_pool_)))) { \
      YYABORT;                                                                                \
    } else {                                                                                  \
      func_node->func_type_ = type;                                                           \
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

%}

 /* dummy node */
%token DUMMY_FUNCTION_CLAUSE
 /* reserved keyword */
%token TOKEN_SPECIAL FUNC_SUBSTR FUNC_CONCAT FUNC_HASH FUNC_TOINT FUNC_DIV FUNC_ADD FUNC_SUB FUNC_MUL FUNC_TESTLOAD
%token FUNC_TO_DATE FUNC_TO_TIMESTAMP FUNC_NVL FUNC_TO_CHAR FUNC_MOD FUNC_SYSDATE
%token END_P ERROR IGNORED_WORD
 /* type token */
%token<str> NAME_OB STR_VAL NUMBER_VAL
%token<num> INT_VAL
%type<function_type> func_name reserved_func
%type<list> param_list
%type<func_node> func_expr
%type<param_node> param
%start start
%%
start: func_root

func_root: func_expr
           {
             malloc_param_node(result->param_node_, result, PARAM_FUNC);
             result->param_node_->func_expr_node_ = $1;
             YYACCEPT;
           }
           | error { YYABORT; }

func_expr: func_name '(' param_list ')'
          {
            malloc_func_expr_node($$, result, $1);
            $$->child_ = $3;
          }
          | func_name '(' ')'
          {
            malloc_func_expr_node($$, result, $1);
            $$->child_ = NULL;
          }
          | reserved_func 
          {
            malloc_func_expr_node($$, result, $1);
            $$->child_ = NULL;
          }

func_name: FUNC_SUBSTR { $$ = OB_PROXY_EXPR_TYPE_FUNC_SUBSTR; }
         | FUNC_CONCAT { $$ = OB_PROXY_EXPR_TYPE_FUNC_CONCAT; }
         | FUNC_HASH   { $$ = OB_PROXY_EXPR_TYPE_FUNC_HASH; }
         | FUNC_TOINT  { $$ = OB_PROXY_EXPR_TYPE_FUNC_TOINT; }
         | FUNC_DIV    { $$ = OB_PROXY_EXPR_TYPE_FUNC_DIV; }
         | FUNC_ADD    { $$ = OB_PROXY_EXPR_TYPE_FUNC_ADD; }
         | FUNC_SUB    { $$ = OB_PROXY_EXPR_TYPE_FUNC_SUB; }
         | FUNC_MUL    { $$ = OB_PROXY_EXPR_TYPE_FUNC_MUL; }
         | FUNC_TESTLOAD { $$ = OB_PROXY_EXPR_TYPE_FUNC_TESTLOAD; }
         | FUNC_TO_DATE { $$ = OB_PROXY_EXPR_TYPE_FUNC_TO_DATE; }
         | FUNC_TO_TIMESTAMP { $$ = OB_PROXY_EXPR_TYPE_FUNC_TO_TIMESTAMP; }
         | FUNC_NVL { $$ = OB_PROXY_EXPR_TYPE_FUNC_NVL; }
         | FUNC_TO_CHAR { $$ = OB_PROXY_EXPR_TYPE_FUNC_TO_CHAR; }
         | FUNC_MOD { $$ = OB_PROXY_EXPR_TYPE_FUNC_MOD; }

reserved_func: FUNC_SYSDATE  { $$ = OB_PROXY_EXPR_TYPE_FUNC_SYSDATE; }

param_list: param
          {
            malloc_list($$, result, $1);
          }
          | param_list ',' param
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
