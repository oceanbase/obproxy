
/* A Bison parser, made by GNU Bison 2.4.1.  */

/* Skeleton implementation for Bison's Yacc-like parsers in C
   
      Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "2.4.1"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 1

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1

/* Using locations.  */
#define YYLSP_NEEDED 1

/* Substitute the variable and function names.  */
#define YYSTYPE         OBEXPRSTYPE
#define YYLTYPE         OBEXPRLTYPE
#define yyparse         ob_expr_parser_gbk_yyparse
#define yylex           ob_expr_parser_gbk_yylex
#define yyerror         ob_expr_parser_gbk_yyerror
#define yylval          ob_expr_parser_gbk_yylval
#define yychar          ob_expr_parser_gbk_yychar
#define yydebug         ob_expr_parser_gbk_yydebug
#define yynerrs         ob_expr_parser_gbk_yynerrs
#define yylloc          ob_expr_parser_gbk_yylloc

/* Copy the first part of user declarations.  */


#include <stdint.h>
#include "opsql/ob_proxy_parse_define.h"
#include "opsql/expr_parser/ob_expr_parse_result.h"



/* Enabling traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 0
#endif

/* Enabling the token table.  */
#ifndef YYTOKEN_TABLE
# define YYTOKEN_TABLE 0
#endif


#ifndef YY_OBEXPR_OB_EXPR_PARSER_TAB_H_INCLUDED
# define YY_OBEXPR_OB_EXPR_PARSER_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef OBEXPR_UTF8_DEBUG
# if defined YYDEBUG
#if YYDEBUG
#   define OBEXPR_UTF8_DEBUG 1
#  else
#   define OBEXPR_UTF8_DEBUG 0
#  endif
# else /* ! defined YYDEBUG */
#  define OBEXPR_UTF8_DEBUG 0
# endif /* ! defined YYDEBUG */
#endif  /* ! defined OBEXPR_UTF8_DEBUG */
#if OBEXPR_UTF8_DEBUG
extern int ob_expr_parser_gbk_yydebug;
#endif
/* Tokens.  */
#ifndef OBEXPRTOKENTYPE
# define OBEXPRTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum obexprtokentype {
     DUMMY_SELECT_CLAUSE = 258,
     DUMMY_INSERT_CLAUSE = 259,
     WHERE = 260,
     AS = 261,
     VALUES = 262,
     SET = 263,
     END_WHERE = 264,
     JOIN = 265,
     BOTH = 266,
     LEADING = 267,
     TRAILING = 268,
     FROM = 269,
     AND_OP = 270,
     OR_OP = 271,
     IN = 272,
     ON = 273,
     BETWEEN = 274,
     IS = 275,
     NULL_VAL = 276,
     NOT = 277,
     COMP_EQ = 278,
     COMP_NSEQ = 279,
     COMP_GE = 280,
     COMP_GT = 281,
     COMP_LE = 282,
     COMP_LT = 283,
     COMP_NE = 284,
     PLACE_HOLDER = 285,
     END_P = 286,
     ERROR = 287,
     IGNORED_WORD = 288,
     LOWER_PARENS = 289,
     HIGHER_PARENS = 290,
     NAME_OB = 291,
     STR_VAL = 292,
     ROW_ID = 293,
     NONE_PARAM_FUNC = 294,
     HEX_VAL = 295,
     TRIM = 296,
     TIMESTAMP = 297,
     QUOTE_STR_VAL = 298,
     DATE = 299,
     TIME = 300,
     INT_VAL = 301,
     POS_PLACE_HOLDER = 302
   };
#endif



#if ! defined OBEXPRSTYPE && ! defined OBEXPRSTYPE_IS_DECLARED
typedef union OBEXPRSTYPE
{


  int64_t              num;
  ObProxyParseString   str;
  ObProxyFunctionType  func;
  ObProxyOperatorType  operator;
  ObProxyTokenNode     *node;
  ObProxyTokenList     *list;
  ObProxyRelationExpr  *relation;



} OBEXPRSTYPE;
# define OBEXPRSTYPE_IS_TRIVIAL 1
# define obexprstype OBEXPRSTYPE /* obsolescent; will be withdrawn */
# define OBEXPRSTYPE_IS_DECLARED 1
#endif

#if ! defined OBEXPRLTYPE && ! defined OBEXPRLTYPE_IS_DECLARED
typedef struct OBEXPRLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} OBEXPRLTYPE;
# define obexprltype OBEXPRLTYPE /* obsolescent; will be withdrawn */
# define OBEXPRLTYPE_IS_DECLARED 1
# define OBEXPRLTYPE_IS_TRIVIAL 1
#endif


#endif
/* Copy the second part of user declarations.  */


#include "ob_expr_parser_gbk_lex.h"
#define YYLEX_PARAM result->yyscan_info_
extern void yyerror(YYLTYPE* yylloc, ObExprParseResult* p, char* s,...);
extern void *obproxy_parse_malloc(const size_t nbyte, void *malloc_pool);
extern void obproxy_parse_free(void *ptr);

static inline bool is_equal(ObProxyParseString *l, ObProxyParseString *r)
{
  return NULL != l && NULL != r && l->str_len_ == r->str_len_
         && 0 == strncasecmp(l->str_, r->str_, l->str_len_);
}

static inline bool is_parameter_token_type(const ObProxyTokenType type)
{
  bool bret = false;
  switch (type)
  {
  case TOKEN_STR_VAL:
  case TOKEN_INT_VAL:
  case TOKEN_FUNC:
  case TOKEN_NULL:
    bret = true; 
    break;
  default:
    bret = false;
    break;
  }
  return bret;
}

static inline void add_token(ObProxyTokenList *list, ObExprParseResult *result, ObProxyTokenNode *node)
{
  UNUSED(result); // use for perf later
  if (OB_ISNULL(list) || OB_ISNULL(node)) {
    // do nothing
  } else {
    if (TOKEN_COLUMN == node->type_) {
      list->column_node_ = node;
    }
    if (NULL != list->tail_) {
      list->tail_->next_ = node;
      list->tail_ = node;
    }
  }
}

// column in (xxx,xxx,xxx) / column = func(xxx,xxx,xxx)
// we only care about the first parameter of the first case
// and function parameters only support str/int/func/null
static inline void add_token_list(ObProxyTokenList *list, ObProxyTokenList *next_list)
{
  if (OB_ISNULL(list) || OB_ISNULL(next_list)) {
  } else if (NULL != list->tail_
             && NULL  != next_list->head_
             && is_parameter_token_type(list->tail_->type_)
             && is_parameter_token_type(next_list->head_->type_)) {
    list->tail_->next_ = next_list->head_;
    list->tail_ = next_list->head_;
    list->tail_->next_ = NULL;
  }
}

static inline ObProxyFunctionType get_reverse_func(ObProxyFunctionType type)
{
  ObProxyFunctionType ret_type = type;
  switch (type) {
    case F_COMP_GE:
      ret_type = F_COMP_LE;
      break;
    case F_COMP_GT:
      ret_type = F_COMP_LT;
      break;
    case F_COMP_LE:
      ret_type = F_COMP_GE;
      break;
    case F_COMP_LT:
      ret_type = F_COMP_GT;
      break;
    default:
      // do nothing
      break;
  }
  return ret_type;
}

static inline void init_empty_column_insert_stmt(ObExprParseResult *result)
{
  result->is_empty_column_insert_stmt_ = true;
}

#define store_const_str(str_value, str, str_len)                 \
  do {                                                           \
    str_value.str_ = str;                                        \
    if (str == NULL) {                                           \
      str_value.str_len_ = 0;                                    \
      str_value.end_ptr_ = NULL;                                 \
    } else {                                                     \
      str_value.str_len_ = str_len;                              \
      str_value.end_ptr_ = str_value.str_ + str_value.str_len_;  \
    }                                                            \
  } while (0)

#define malloc_func_node(node, result, operator)                           \
  do {                                                                     \
    if (OB_ISNULL(node = ((ObProxyTokenNode *)obproxy_parse_malloc(        \
                      sizeof(ObProxyTokenNode), result->malloc_pool_)))) { \
      YYABORT;                                                             \
    } else {                                                               \
      node->type_ = TOKEN_FUNC;                                            \
      node->child_ = NULL;                                                 \
      node->next_ = NULL;                                                  \
      node->str_value_.str_len_ = 1;                                       \
      switch (operator) {                                                  \
        case OPT_ADD:                                                      \
          store_const_str(node->str_value_, "+", 1);                       \
          break;                                                           \
        case OPT_MINUS:                                                    \
          store_const_str(node->str_value_, "-", 1);                       \
          break;                                                           \
        case OPT_MUL:                                                      \
          store_const_str(node->str_value_, "*", 1);                       \
          break;                                                           \
        case OPT_DIV:                                                      \
          store_const_str(node->str_value_, "/", 1);                       \
          break;                                                           \
        case OPT_MOD:                                                      \
          store_const_str(node->str_value_, "%", 1);                       \
          break;                                                           \
        case OPT_AND:                                                      \
          store_const_str(node->str_value_, "&", 1);                       \
          break;                                                           \
        case OPT_NOT:                                                      \
          store_const_str(node->str_value_, "!", 1);                       \
          break;                                                           \
        default:                                                           \
          break;                                                           \
      }                                                                    \
    }                                                                      \
  } while (0)

#define malloc_node(node, result, type)                                                       \
  do {                                                                                        \
    if (OB_ISNULL(node = ((ObProxyTokenNode *)obproxy_parse_malloc(sizeof(ObProxyTokenNode),  \
                                                                   result->malloc_pool_)))) { \
      YYABORT;                                                                                \
    } else {                                                                                  \
      node->type_ = type;                                                                     \
      node->child_ = NULL;                                                                    \
      node->next_ = NULL;                                                                     \
    }                                                                                         \
  } while(0)                                                                                  \

#define malloc_list(list, result, node)                                                       \
  do {                                                                                        \
    if (OB_ISNULL(list = ((ObProxyTokenList *)obproxy_parse_malloc(sizeof(ObProxyTokenList),  \
                                                                   result->malloc_pool_)))) { \
      YYABORT;                                                                                \
    } else if (OB_ISNULL(node))  {                                                            \
      list->column_node_ = NULL;                                                              \
    } else {                                                                                  \
      if (TOKEN_COLUMN == node->type_) {                                                      \
        list->column_node_ = node;                                                            \
      } else {                                                                                \
        list->column_node_ = NULL;                                                            \
      }                                                                                       \
      list->head_ = node;                                                                     \
      list->tail_ = node;                                                                     \
    }                                                                                         \
  } while(0)                                                                                  \

#define check_and_add_relation(result, relation)                                                \
  do {                                                                                          \
    if (NULL == relation) {                                                                     \
    } else {                                                                                    \
      if (result->all_relation_info_.relation_num_ < OBPROXY_MAX_RELATION_NUM) {                    \
        result->all_relation_info_.relations_[result->all_relation_info_.relation_num_++] = relation;   \
      } else {                                                                                  \
        /* YYACCEPT; */                                                                         \
      }                                                                                         \
    }                                                                                           \
  } while(0)                                                                                    \

static int64_t get_part_key_idx(ObProxyParseString *db_name,
                                ObProxyParseString *table_name,
                                ObProxyParseString *column_name,
                                ObExprParseResult *result)
{
  int64_t part_key_idx = IDX_NO_PART_KEY_COLUMN;
  
  if (result->part_key_info_.key_num_ > 0) {
    if (NULL != db_name && !is_equal(db_name, &result->table_info_.database_name_)) {
      part_key_idx = IDX_NO_PART_KEY_COLUMN;
    } else if (NULL != table_name
               && !is_equal(table_name, &result->table_info_.table_name_)
               && !is_equal(table_name, &result->table_info_.alias_name_)) {
      part_key_idx = IDX_NO_PART_KEY_COLUMN;
    } else if (NULL != column_name) {
      int64_t i = 0;
      for (i = 0; i < result->part_key_info_.key_num_ && part_key_idx  < 0; ++i) {
        if (is_equal(column_name, &result->part_key_info_.part_keys_[i].name_)) {
          part_key_idx = i;
          break;
        }
      }
    }
  }
  return part_key_idx;
}

static inline void add_relation(ObExprParseResult *result,
                                ObProxyTokenList *left_value,
                                ObProxyFunctionType type,
                                ObProxyTokenList *right_value)
{
  if (result->all_relation_info_.relation_num_ < OBPROXY_MAX_RELATION_NUM) {
    ObProxyRelationExpr *relation = NULL;
    ObProxyTokenList *tmp_left = NULL;
    ObProxyTokenList *tmp_right = NULL;
    ObProxyFunctionType tmp_type = F_NONE;

    if (NULL != left_value->column_node_
        && TOKEN_COLUMN == left_value->column_node_->type_) {
      tmp_left = left_value;
      tmp_right = right_value;
      tmp_type = type;
    } else if (NULL != right_value->column_node_
               && TOKEN_COLUMN == right_value->column_node_->type_) {
      tmp_left = right_value;
      tmp_right = left_value;
      tmp_type = get_reverse_func(type);
    }

    if (NULL == tmp_left || NULL == tmp_right || F_COMP_NE == tmp_type
        || NULL == tmp_left->column_node_) {
      // will return null
    } else if ((IDX_NO_PART_KEY_COLUMN == tmp_left->column_node_->part_key_idx_)
               && (F_COMP_EQ != tmp_type)
               && (F_COMP_NSEQ != tmp_type)) {
      // only reserve equal relation for non-part key
    } else if (OB_ISNULL(relation = ((ObProxyRelationExpr *)obproxy_parse_malloc(
                                          sizeof(ObProxyRelationExpr), result->malloc_pool_)))) {
      // will return null
    } else {
      relation->left_value_ = tmp_left;
      relation->type_ = tmp_type;
      relation->right_value_ = tmp_right;

      result->all_relation_info_.relations_[result->all_relation_info_.relation_num_++] = relation;
    }
  }
}

static inline ObProxyRelationExpr *get_values_relation(ObExprParseResult *result,
                                                       ObProxyTokenList *right_value)
{
  ObProxyRelationExpr *relation = NULL;
  if (NULL == right_value) {
  // will return null
  } else {
    int64_t i = 0;
    for (i = 0; i < result->part_key_info_.key_num_; ++i) {
      // make sure the part_key exist in sql to avoid the uninitialized idx_
      if (result->values_list_idx_ == result->part_key_info_.part_keys_[i].idx_) {
        if (OB_ISNULL(relation = ((ObProxyRelationExpr *)obproxy_parse_malloc(
                                        sizeof(ObProxyRelationExpr), result->malloc_pool_)))) {
        } else {
          relation->part_key_idx_ = i;
          relation->type_ = F_COMP_EQ;
          relation->right_value_ = right_value;
          relation->left_value_ = NULL;
        }
        break;
      }
    }
  }
  return relation;
}

static inline void add_left_relation_value(ObExprParseResult *result,
                                                       ObProxyTokenList *left_value)
{
  ObProxyRelationExpr *relation = NULL;
  if (NULL == left_value) {
    // will return
  } else if (result->all_relation_info_.relation_num_ >= OBPROXY_MAX_RELATION_NUM) {
    // do nothing
  } else if (OB_ISNULL(relation = ((ObProxyRelationExpr *)obproxy_parse_malloc(
                                        sizeof(ObProxyRelationExpr), result->malloc_pool_)))) {
  } else {
    relation->type_ = F_COMP_EQ;
    relation->left_value_ = left_value;
    relation->right_value_ = NULL;
    result->all_relation_info_.relations_[result->all_relation_info_.relation_num_++] = relation;
  }
}

static inline void add_right_relation_value(ObExprParseResult *result,
                                            ObProxyTokenList *right_value)
{
  if (NULL == right_value) {
    // will return
  } else if (result->all_relation_info_.relation_num_ >= OBPROXY_MAX_RELATION_NUM) {
    // do nohting
  } else if (result->all_relation_info_.right_value_num_ >= result->all_relation_info_.relation_num_) {
    // ignore
  } else if (OB_ISNULL(result->all_relation_info_.relations_[result->all_relation_info_.right_value_num_])) {
  } else {
    ObProxyRelationExpr *relation = result->all_relation_info_.relations_[result->all_relation_info_.right_value_num_++];
    relation->type_ = F_COMP_EQ;
    relation->right_value_ = right_value;
  }
}

static inline ObProxyTokenNode* calc_unary_operator(ObProxyTokenNode *node, ObExprParseResult *result, 
                                       ObProxyOperatorType operator)
{
  ObProxyTokenNode *token_node = node;
  if (node == NULL) {
    // do nothing
  } else if (OPT_MINUS != operator) {
    // only need handle '-' now
    // do nothing
  } else if (TOKEN_INT_VAL == node->type_) {
    // don't worry out of range
    node->int_value_ = - node->int_value_;
  } else if (TOKEN_STR_VAL == node->type_) {
    // convert 'str' -> '-str'
    void *tmp_buf = NULL;
    if (OB_ISNULL(tmp_buf = ((char *)obproxy_parse_malloc(node->str_value_.str_len_ + 1, result->malloc_pool_)))) { 
        //do nothing                                                                                
    } else {
      node->str_value_.str_len_ = node->str_value_.str_len_ + 1;
      if (OPT_ADD == operator) {
        memcpy(tmp_buf, "+", 1);
      } else {
        memcpy(tmp_buf, "-", 1);
      }
      memcpy(tmp_buf + 1, node->str_value_.str_, node->str_value_.str_len_ - 1);
      node->str_value_.str_ = tmp_buf;
      node->str_value_.end_ptr_ = node->str_value_.str_ + node->str_value_.str_len_;
    }
  } else if (TOKEN_FUNC == node->type_) {
    // convert func: [-xxx()] ->  [0 - xxx()]                                                                
    ObProxyTokenNode *dummy_param = NULL;
    ObProxyTokenList *list = NULL;
    // we do nothing in obproxy_parse_free, memory will be free in allocator.resuse() finnaly
    if (OB_ISNULL(token_node = ((ObProxyTokenNode *)obproxy_parse_malloc(sizeof(ObProxyTokenNode), result->malloc_pool_)))) {
      // return NULL
    } else if (OB_ISNULL(dummy_param = ((ObProxyTokenNode *)obproxy_parse_malloc(sizeof(ObProxyTokenNode), result->malloc_pool_)))) {
      token_node = NULL;
      obproxy_parse_free(token_node);
      // return NULL
    } else if (OB_ISNULL(list = ((ObProxyTokenList *)obproxy_parse_malloc(sizeof(ObProxyTokenList), result->malloc_pool_)))) {
      token_node = NULL;
      list = NULL;
      obproxy_parse_free(token_node);
      obproxy_parse_free(list);
      // return NULL
    } else {
      token_node->type_ = TOKEN_FUNC;                                                               
      token_node->child_ = list;                                                                    
      token_node->next_ = NULL;
      store_const_str(token_node->str_value_, "-", 1);
      dummy_param->type_ = TOKEN_INT_VAL;
      dummy_param->child_ = NULL;
      dummy_param->next_ = NULL;
      dummy_param->int_value_ = 0;
      list->head_ = dummy_param;
      list->tail_ = dummy_param;
      list->tail_->next_ = node;
      list->tail_ = node;
    }
  }
  return token_node;
}




#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#elif (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
typedef signed char yytype_int8;
#else
typedef short int yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short int yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short int yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned int
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(e) ((void) (e))
#else
# define YYUSE(e) /* empty */
#endif

/* Identity function, used to suppress warnings about constant conditions.  */
#ifndef lint
# define YYID(n) (n)
#else
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static int
YYID (int yyi)
#else
static int
YYID (yyi)
    int yyi;
#endif
{
  return yyi;
}
#endif

#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#     ifndef _STDLIB_H
#      define _STDLIB_H 1
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's `empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (YYID (0))
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined _STDLIB_H \
       && ! ((defined YYMALLOC || defined malloc) \
	     && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef _STDLIB_H
#    define _STDLIB_H 1
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
	 || (defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL \
	     && defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE) + sizeof (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

/* Copy COUNT objects from FROM to TO.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(To, From, Count) \
      __builtin_memcpy (To, From, (Count) * sizeof (*(From)))
#  else
#   define YYCOPY(To, From, Count)		\
      do					\
	{					\
	  YYSIZE_T yyi;				\
	  for (yyi = 0; yyi < (Count); yyi++)	\
	    (To)[yyi] = (From)[yyi];		\
	}					\
      while (YYID (0))
#  endif
# endif

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)				\
    do									\
      {									\
	YYSIZE_T yynewbytes;						\
	YYCOPY (&yyptr->Stack_alloc, Stack, yysize);			\
	Stack = &yyptr->Stack_alloc;					\
	yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
	yyptr += yynewbytes / sizeof (*yyptr);				\
      }									\
    while (YYID (0))

#endif

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  17
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   365

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  60
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  29
/* YYNRULES -- Number of rules.  */
#define YYNRULES  104
/* YYNRULES -- Number of states.  */
#define YYNSTATES  190

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   302

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    44,     2,     2,     2,    39,    34,     2,
      41,    42,    37,    35,    59,    36,    58,    38,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    57,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    40,
      43,    45,    46,    47,    48,    49,    50,    51,    52,    53,
      54,    55,    56
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     6,     9,    13,    16,    21,    23,    25,
      27,    29,    31,    34,    38,    41,    46,    50,    55,    61,
      68,    70,    74,    80,    84,    90,    94,   100,   106,   113,
     119,   123,   128,   130,   132,   134,   136,   138,   140,   142,
     144,   148,   150,   152,   155,   157,   161,   164,   167,   170,
     174,   178,   182,   186,   190,   194,   195,   199,   203,   205,
     209,   211,   213,   216,   218,   220,   222,   224,   226,   228,
     230,   232,   236,   242,   244,   248,   253,   258,   261,   264,
     267,   272,   277,   279,   281,   283,   285,   287,   289,   294,
     299,   303,   305,   309,   315,   316,   320,   322,   326,   328,
     330,   334,   336,   340,   341
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int8 yyrhs[] =
{
      61,     0,    -1,     3,    62,    -1,     4,    81,    -1,     5,
      67,    63,    -1,    64,    63,    -1,    64,     5,    67,    63,
      -1,     1,    -1,     9,    -1,    57,    -1,    31,    -1,    65,
      -1,    64,    65,    -1,    66,    18,    67,    -1,    10,    45,
      -1,    10,    45,    58,    45,    -1,    10,    45,    45,    -1,
      10,    45,     6,    45,    -1,    10,    45,    58,    45,    45,
      -1,    10,    45,    58,    45,     6,    45,    -1,    68,    -1,
      67,    15,    67,    -1,    41,    67,    15,    67,    42,    -1,
      67,    16,    67,    -1,    41,    67,    16,    67,    42,    -1,
      71,    69,    71,    -1,    41,    71,    69,    71,    42,    -1,
      71,    17,    41,    70,    42,    -1,    71,    22,    17,    41,
      70,    42,    -1,    71,    19,    71,    15,    71,    -1,    71,
      20,    21,    -1,    71,    20,    22,    21,    -1,    23,    -1,
      24,    -1,    25,    -1,    26,    -1,    27,    -1,    28,    -1,
      29,    -1,    71,    -1,    70,    59,    71,    -1,    72,    -1,
      73,    -1,    72,    80,    -1,    80,    -1,    41,    73,    42,
      -1,    35,    73,    -1,    36,    73,    -1,    44,    73,    -1,
      73,    35,    73,    -1,    73,    36,    73,    -1,    73,    37,
      73,    -1,    73,    38,    73,    -1,    73,    34,    73,    -1,
      73,    39,    73,    -1,    -1,    72,    59,    72,    -1,    74,
      59,    72,    -1,    73,    -1,    76,    14,    80,    -1,    77,
      -1,    80,    -1,    77,    80,    -1,    11,    -1,    12,    -1,
      13,    -1,    79,    -1,    52,    -1,    47,    -1,    49,    -1,
      45,    -1,    45,    58,    45,    -1,    45,    58,    45,    58,
      45,    -1,    48,    -1,    48,    41,    42,    -1,    48,    41,
      72,    42,    -1,    50,    41,    75,    42,    -1,    51,    78,
      -1,    54,    78,    -1,    53,    78,    -1,    45,    41,    72,
      42,    -1,    45,    41,    74,    42,    -1,    55,    -1,    46,
      -1,    52,    -1,    30,    -1,    56,    -1,    21,    -1,    83,
       7,    82,    63,    -1,     8,    87,    88,    63,    -1,    18,
      67,    63,    -1,    62,    -1,    41,    86,    42,    -1,    82,
      59,    41,    86,    42,    -1,    -1,    41,    84,    42,    -1,
      85,    -1,    84,    59,    85,    -1,    45,    -1,    71,    -1,
      86,    59,    71,    -1,    67,    -1,    87,    59,    67,    -1,
      -1,     5,    67,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   431,   431,   432,   434,   435,   436,   437,   439,   440,
     441,   443,   444,   446,   448,   449,   450,   451,   452,   453,
     455,   456,   457,   458,   459,   461,   462,   463,   464,   465,
     471,   479,   481,   482,   483,   484,   485,   486,   487,   489,
     490,   492,   494,   495,   497,   498,   499,   500,   501,   508,
     516,   524,   532,   540,   548,   557,   558,   559,   561,   562,
     564,   565,   573,   579,   580,   581,   583,   588,   595,   602,
     606,   612,   619,   626,   631,   636,   642,   648,   654,   660,
     666,   672,   678,   679,   680,   681,   687,   692,   697,   698,
     699,   700,   702,   706,   711,   712,   714,   719,   725,   731,
     746,   759,   760,   762,   763
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "DUMMY_SELECT_CLAUSE",
  "DUMMY_INSERT_CLAUSE", "WHERE", "AS", "VALUES", "SET", "END_WHERE",
  "JOIN", "BOTH", "LEADING", "TRAILING", "FROM", "AND_OP", "OR_OP", "IN",
  "ON", "BETWEEN", "IS", "NULL_VAL", "NOT", "COMP_EQ", "COMP_NSEQ",
  "COMP_GE", "COMP_GT", "COMP_LE", "COMP_LT", "COMP_NE", "PLACE_HOLDER",
  "END_P", "ERROR", "IGNORED_WORD", "'&'", "'+'", "'-'", "'*'", "'/'",
  "'%'", "LOWER_PARENS", "'('", "')'", "HIGHER_PARENS", "'!'", "NAME_OB",
  "STR_VAL", "ROW_ID", "NONE_PARAM_FUNC", "HEX_VAL", "TRIM", "TIMESTAMP",
  "QUOTE_STR_VAL", "DATE", "TIME", "INT_VAL", "POS_PLACE_HOLDER", "';'",
  "'.'", "','", "$accept", "start", "select_root", "end_flag",
  "join_expr_list", "join_on_expr", "join_expr", "cond_expr", "bool_pri",
  "comp", "in_expr_list", "expr", "token_list", "simple_expr",
  "func_param_list", "trim_param_list", "trim_type_list", "trim_type",
  "date_value_list", "date_value_type", "token", "insert_root",
  "values_expr_lists", "opt_column_list", "column_list", "opt_column",
  "values_expr_list", "set_expr", "opt_where_clause", 0
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[YYLEX-NUM] -- Internal token number corresponding to
   token YYLEX-NUM.  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,    38,    43,    45,    42,    47,    37,
     289,    40,    41,   290,    33,   291,   292,   293,   294,   295,
     296,   297,   298,   299,   300,   301,   302,    59,    46,    44
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,    60,    61,    61,    62,    62,    62,    62,    63,    63,
      63,    64,    64,    65,    66,    66,    66,    66,    66,    66,
      67,    67,    67,    67,    67,    68,    68,    68,    68,    68,
      68,    68,    69,    69,    69,    69,    69,    69,    69,    70,
      70,    71,    72,    72,    73,    73,    73,    73,    73,    73,
      73,    73,    73,    73,    73,    74,    74,    74,    75,    75,
      76,    76,    76,    77,    77,    77,    78,    79,    80,    80,
      80,    80,    80,    80,    80,    80,    80,    80,    80,    80,
      80,    80,    80,    80,    80,    80,    80,    80,    81,    81,
      81,    81,    82,    82,    83,    83,    84,    84,    85,    86,
      86,    87,    87,    88,    88
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     2,     2,     3,     2,     4,     1,     1,     1,
       1,     1,     2,     3,     2,     4,     3,     4,     5,     6,
       1,     3,     5,     3,     5,     3,     5,     5,     6,     5,
       3,     4,     1,     1,     1,     1,     1,     1,     1,     1,
       3,     1,     1,     2,     1,     3,     2,     2,     2,     3,
       3,     3,     3,     3,     3,     0,     3,     3,     1,     3,
       1,     1,     2,     1,     1,     1,     1,     1,     1,     1,
       1,     3,     5,     1,     3,     4,     4,     2,     2,     2,
       4,     4,     1,     1,     1,     1,     1,     1,     4,     4,
       3,     1,     3,     5,     0,     3,     1,     3,     1,     1,
       3,     1,     3,     0,     2
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,     0,     0,     0,     7,     0,     0,     2,     0,    11,
       0,     0,     0,     0,    91,     3,     0,     1,    87,    85,
       0,     0,     0,     0,    70,    83,    68,    73,    69,     0,
       0,    84,     0,     0,    82,    86,     0,    20,     0,    41,
      42,    44,    14,     0,     8,    10,     9,     5,    12,     0,
     101,   103,     0,    98,     0,    96,     0,     0,    46,    47,
       0,     0,    42,    48,    55,     0,     0,     0,    67,    77,
      66,    79,    78,     0,     0,     4,     0,     0,     0,     0,
      32,    33,    34,    35,    36,    37,    38,     0,    43,     0,
       0,     0,     0,     0,     0,     0,    16,     0,     0,    13,
       0,     0,     0,    90,    95,     0,     0,     0,     0,     0,
       0,     0,    45,     0,     0,    71,    74,     0,    63,    64,
      65,    58,     0,     0,    60,    44,    21,    23,     0,     0,
      30,     0,     0,    25,    53,    49,    50,    51,    52,    54,
      17,    15,     6,   104,   102,    89,    97,    99,     0,     0,
      88,    21,    23,    25,    80,     0,    81,     0,     0,    75,
      76,     0,    62,     0,    39,     0,    31,     0,     0,    18,
      92,     0,     0,    22,    24,    26,    56,    57,    72,    59,
      27,     0,    29,     0,    19,   100,     0,    40,    28,    93
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,     3,     7,    47,     8,     9,    10,    36,    37,    87,
     163,    38,    39,    40,   114,   122,   123,   124,    69,    70,
      41,    15,   107,    16,    54,    55,   148,    51,   102
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -59
static const yytype_int16 yypact[] =
{
      59,    12,   100,    16,   -59,   224,   -26,   -59,     2,   -59,
      13,   224,   224,     4,   -59,   -59,    44,   -59,   -59,   -59,
     260,   260,   224,   260,   -20,   -59,   -59,    31,   -59,    33,
      17,   -59,    17,    17,   -59,   -59,    19,   -59,   161,   309,
      96,   -59,     3,   224,   -59,   -59,   -59,   -59,   -59,   224,
      66,    -2,    19,   -59,   -19,   -59,    36,   260,   -59,   -59,
      76,   161,   178,   -59,   260,    38,   188,   116,   -59,   -59,
     -59,   -59,   -59,   224,   224,   -59,    57,   260,    74,    68,
     -59,   -59,   -59,   -59,   -59,   -59,   -59,   260,   -59,   260,
     260,   260,   260,   260,   260,    55,   -59,    64,    19,    66,
     224,   224,    21,   -59,   -59,     4,   260,     1,   178,   224,
     224,   260,   -59,   152,   -18,    46,   -59,   296,   -59,   -59,
     -59,    96,    69,   107,   309,   108,   -59,   109,   260,   110,
     -59,   105,    95,   -59,   211,   -12,   -12,   -59,   -59,   -59,
     -59,     8,   -59,    66,    66,   -59,   -59,   -59,    -5,    99,
     -59,   102,     5,   106,   -59,   260,   -59,   260,    97,   -59,
     -59,   309,   -59,    -3,   -59,   260,   -59,   260,   111,   -59,
     -59,   260,   260,   -59,   -59,   -59,   309,   309,   -59,   -59,
     -59,   260,   -59,    28,   -59,   -59,    29,   -59,   -59,   -59
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
     -59,   -59,   145,   -34,   -59,   146,   -59,    -7,   -59,    92,
      -9,   -22,   -58,    23,   -59,   -59,   -59,   -59,    87,   -59,
     -38,   -59,   -59,   -59,   -59,    50,     7,   -59,   -59
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -95
static const yytype_int16 yytable[] =
{
      61,    88,    75,   100,    50,    52,   113,    43,   117,    95,
      44,    44,     6,     4,   168,    60,    17,     5,   103,    42,
      73,    64,     6,   104,   156,    92,    93,    94,    44,   125,
      44,    49,    45,    45,    73,    74,    98,   170,    65,   180,
     105,   157,    99,    58,    59,    62,    63,   174,    96,    53,
      45,    56,    45,   169,   171,   129,   181,   101,    46,    46,
     149,    97,     1,     2,   142,   133,   126,   127,   145,    68,
     188,   189,    66,   150,    67,    88,    46,   106,    46,    88,
     108,    73,    74,   115,   147,   132,   162,   181,   171,   153,
     121,   109,   110,   143,   144,   130,   131,   176,   128,   177,
     140,     4,   151,   152,   158,     5,   164,   -94,    11,   141,
       6,   160,   134,   135,   136,   137,   138,   139,    12,    71,
      72,   161,   -61,   179,    73,   165,   166,   118,   119,   120,
      89,    90,    91,    92,    93,    94,   167,    18,    88,    88,
     172,    13,   178,   182,   173,   164,    19,    14,   175,   185,
     147,    20,    21,   111,    48,   146,   184,    57,   183,   187,
      23,    24,    25,    26,    27,    28,    29,    30,    31,    32,
      33,    34,    35,    18,     0,     0,     0,     0,    76,   186,
      77,    78,    19,    79,    80,    81,    82,    83,    84,    85,
      86,     0,     0,     0,   154,     0,     0,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    35,    18,
       0,   155,    89,    90,    91,    92,    93,    94,    19,     0,
     112,     0,     0,    20,    21,     0,     0,     0,     0,    57,
     116,     0,    23,    24,    25,    26,    27,    28,    29,    30,
      31,    32,    33,    34,    35,    18,    90,    91,    92,    93,
      94,     0,     0,     0,    19,     0,     0,     0,     0,    20,
      21,     0,     0,     0,     0,    22,     0,     0,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    18,     0,     0,     0,     0,     0,     0,     0,     0,
      19,     0,     0,     0,     0,    20,    21,     0,     0,     0,
       0,    57,     0,     0,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    18,     0,     0,
       0,     0,     0,     0,     0,     0,    19,     0,     0,     0,
      18,     0,     0,     0,     0,     0,     0,     0,   159,    19,
       0,    24,    25,    26,    27,    28,    29,    30,    31,    32,
      33,    34,    35,     0,    24,    25,    26,    27,    28,    29,
      30,    31,    32,    33,    34,    35
};

static const yytype_int16 yycheck[] =
{
      22,    39,    36,     5,    11,    12,    64,     5,    66,     6,
       9,     9,    10,     1,     6,    22,     0,     5,    52,    45,
      15,    41,    10,    42,    42,    37,    38,    39,     9,    67,
       9,    18,    31,    31,    15,    16,    43,    42,    58,    42,
      59,    59,    49,    20,    21,    22,    23,    42,    45,    45,
      31,     7,    31,    45,    59,    77,    59,    59,    57,    57,
      59,    58,     3,     4,    98,    87,    73,    74,   102,    52,
      42,    42,    41,   107,    41,   113,    57,    41,    57,   117,
      57,    15,    16,    45,   106,    17,   124,    59,    59,   111,
      67,    15,    16,   100,   101,    21,    22,   155,    41,   157,
      45,     1,   109,   110,    58,     5,   128,     7,     8,    45,
      10,    42,    89,    90,    91,    92,    93,    94,    18,    32,
      33,    14,    14,   161,    15,    15,    21,    11,    12,    13,
      34,    35,    36,    37,    38,    39,    41,    21,   176,   177,
      41,    41,    45,   165,    42,   167,    30,     2,    42,   171,
     172,    35,    36,    61,     8,   105,    45,    41,   167,   181,
      44,    45,    46,    47,    48,    49,    50,    51,    52,    53,
      54,    55,    56,    21,    -1,    -1,    -1,    -1,    17,   172,
      19,    20,    30,    22,    23,    24,    25,    26,    27,    28,
      29,    -1,    -1,    -1,    42,    -1,    -1,    45,    46,    47,
      48,    49,    50,    51,    52,    53,    54,    55,    56,    21,
      -1,    59,    34,    35,    36,    37,    38,    39,    30,    -1,
      42,    -1,    -1,    35,    36,    -1,    -1,    -1,    -1,    41,
      42,    -1,    44,    45,    46,    47,    48,    49,    50,    51,
      52,    53,    54,    55,    56,    21,    35,    36,    37,    38,
      39,    -1,    -1,    -1,    30,    -1,    -1,    -1,    -1,    35,
      36,    -1,    -1,    -1,    -1,    41,    -1,    -1,    44,    45,
      46,    47,    48,    49,    50,    51,    52,    53,    54,    55,
      56,    21,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      30,    -1,    -1,    -1,    -1,    35,    36,    -1,    -1,    -1,
      -1,    41,    -1,    -1,    44,    45,    46,    47,    48,    49,
      50,    51,    52,    53,    54,    55,    56,    21,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    30,    -1,    -1,    -1,
      21,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    42,    30,
      -1,    45,    46,    47,    48,    49,    50,    51,    52,    53,
      54,    55,    56,    -1,    45,    46,    47,    48,    49,    50,
      51,    52,    53,    54,    55,    56
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,     3,     4,    61,     1,     5,    10,    62,    64,    65,
      66,     8,    18,    41,    62,    81,    83,     0,    21,    30,
      35,    36,    41,    44,    45,    46,    47,    48,    49,    50,
      51,    52,    53,    54,    55,    56,    67,    68,    71,    72,
      73,    80,    45,     5,     9,    31,    57,    63,    65,    18,
      67,    87,    67,    45,    84,    85,     7,    41,    73,    73,
      67,    71,    73,    73,    41,    58,    41,    41,    52,    78,
      79,    78,    78,    15,    16,    63,    17,    19,    20,    22,
      23,    24,    25,    26,    27,    28,    29,    69,    80,    34,
      35,    36,    37,    38,    39,     6,    45,    58,    67,    67,
       5,    59,    88,    63,    42,    59,    41,    82,    73,    15,
      16,    69,    42,    72,    74,    45,    42,    72,    11,    12,
      13,    73,    75,    76,    77,    80,    67,    67,    41,    71,
      21,    22,    17,    71,    73,    73,    73,    73,    73,    73,
      45,    45,    63,    67,    67,    63,    85,    71,    86,    59,
      63,    67,    67,    71,    42,    59,    42,    59,    58,    42,
      42,    14,    80,    70,    71,    15,    21,    41,     6,    45,
      42,    59,    41,    42,    42,    42,    72,    72,    45,    80,
      42,    59,    71,    70,    45,    71,    86,    71,    42,    42
};

#define yyerrok		(yyerrstatus = 0)
#define yyclearin	(yychar = YYEMPTY)
#define YYEMPTY		(-2)
#define YYEOF		0

#define YYACCEPT	goto yyacceptlab
#define YYABORT		goto yyabortlab
#define YYERROR		goto yyerrorlab


/* Like YYERROR except do call yyerror.  This remains here temporarily
   to ease the transition to the new meaning of YYERROR, for GCC.
   Once GCC version 2 has supplanted version 1, this can go.  */

#define YYFAIL		goto yyerrlab

#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)					\
do								\
  if (yychar == YYEMPTY && yylen == 1)				\
    {								\
      yychar = (Token);						\
      yylval = (Value);						\
      yytoken = YYTRANSLATE (yychar);				\
      YYPOPSTACK (1);						\
      goto yybackup;						\
    }								\
  else								\
    {								\
      yyerror (&yylloc, result, YY_("syntax error: cannot back up")); \
      YYERROR;							\
    }								\
while (YYID (0))


#define YYTERROR	1
#define YYERRCODE	256


/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#define YYRHSLOC(Rhs, K) ((Rhs)[K])
#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)				\
    do									\
      if (YYID (N))                                                    \
	{								\
	  (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;	\
	  (Current).first_column = YYRHSLOC (Rhs, 1).first_column;	\
	  (Current).last_line    = YYRHSLOC (Rhs, N).last_line;		\
	  (Current).last_column  = YYRHSLOC (Rhs, N).last_column;	\
	}								\
      else								\
	{								\
	  (Current).first_line   = (Current).last_line   =		\
	    YYRHSLOC (Rhs, 0).last_line;				\
	  (Current).first_column = (Current).last_column =		\
	    YYRHSLOC (Rhs, 0).last_column;				\
	}								\
    while (YYID (0))
#endif


/* YY_LOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

#ifndef YY_LOCATION_PRINT
# if YYLTYPE_IS_TRIVIAL
#  define YY_LOCATION_PRINT(File, Loc)			\
     fprintf (File, "%d.%d-%d.%d",			\
	      (Loc).first_line, (Loc).first_column,	\
	      (Loc).last_line,  (Loc).last_column)
# else
#  define YY_LOCATION_PRINT(File, Loc) ((void) 0)
# endif
#endif


/* YYLEX -- calling `yylex' with the right arguments.  */

#ifdef YYLEX_PARAM
# define YYLEX yylex (&yylval, &yylloc, YYLEX_PARAM)
#else
# define YYLEX yylex (&yylval, &yylloc, YYLEX_PARAM)
#endif

/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)			\
do {						\
  if (yydebug)					\
    YYFPRINTF Args;				\
} while (YYID (0))

# define YY_SYMBOL_PRINT(Title, Type, Value, Location)			  \
do {									  \
  if (yydebug)								  \
    {									  \
      YYFPRINTF (stderr, "%s ", Title);					  \
      yy_symbol_print (stderr,						  \
		  Type, Value, Location, result); \
      YYFPRINTF (stderr, "\n");						  \
    }									  \
} while (YYID (0))


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ObExprParseResult* result)
#else
static void
yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, result)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ObExprParseResult* result;
#endif
{
  if (!yyvaluep)
    return;
  YYUSE (yylocationp);
  YYUSE (result);
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# else
  YYUSE (yyoutput);
# endif
  switch (yytype)
    {
      default:
	break;
    }
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ObExprParseResult* result)
#else
static void
yy_symbol_print (yyoutput, yytype, yyvaluep, yylocationp, result)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ObExprParseResult* result;
#endif
{
  if (yytype < YYNTOKENS)
    YYFPRINTF (yyoutput, "token %s (", yytname[yytype]);
  else
    YYFPRINTF (yyoutput, "nterm %s (", yytname[yytype]);

  YY_LOCATION_PRINT (yyoutput, *yylocationp);
  YYFPRINTF (yyoutput, ": ");
  yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, result);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
#else
static void
yy_stack_print (yybottom, yytop)
    yytype_int16 *yybottom;
    yytype_int16 *yytop;
#endif
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)				\
do {								\
  if (yydebug)							\
    yy_stack_print ((Bottom), (Top));				\
} while (YYID (0))


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_reduce_print (YYSTYPE *yyvsp, YYLTYPE *yylsp, int yyrule, ObExprParseResult* result)
#else
static void
yy_reduce_print (yyvsp, yylsp, yyrule, result)
    YYSTYPE *yyvsp;
    YYLTYPE *yylsp;
    int yyrule;
    ObExprParseResult* result;
#endif
{
  int yynrhs = yyr2[yyrule];
  int yyi;
  unsigned long int yylno = yyrline[yyrule];
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
	     yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr, yyrhs[yyprhs[yyrule] + yyi],
		       &(yyvsp[(yyi + 1) - (yynrhs)])
		       , &(yylsp[(yyi + 1) - (yynrhs)])		       , result);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)		\
do {					\
  if (yydebug)				\
    yy_reduce_print (yyvsp, yylsp, Rule, result); \
} while (YYID (0))

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef	YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif



#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static YYSIZE_T
yystrlen (const char *yystr)
#else
static YYSIZE_T
yystrlen (yystr)
    const char *yystr;
#endif
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static char *
yystpcpy (char *yydest, const char *yysrc)
#else
static char *
yystpcpy (yydest, yysrc)
    char *yydest;
    const char *yysrc;
#endif
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
	switch (*++yyp)
	  {
	  case '\'':
	  case ',':
	    goto do_not_strip_quotes;

	  case '\\':
	    if (*++yyp != '\\')
	      goto do_not_strip_quotes;
	    /* Fall through.  */
	  default:
	    if (yyres)
	      yyres[yyn] = *yyp;
	    yyn++;
	    break;

	  case '"':
	    if (yyres)
	      yyres[yyn] = '\0';
	    return yyn;
	  }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return yystpcpy (yyres, yystr) - yyres;
}
# endif

/* Copy into YYRESULT an error message about the unexpected token
   YYCHAR while in state YYSTATE.  Return the number of bytes copied,
   including the terminating null byte.  If YYRESULT is null, do not
   copy anything; just return the number of bytes that would be
   copied.  As a special case, return 0 if an ordinary "syntax error"
   message will do.  Return YYSIZE_MAXIMUM if overflow occurs during
   size calculation.  */
static YYSIZE_T
yysyntax_error (char *yyresult, int yystate, int yychar)
{
  int yyn = yypact[yystate];

  if (! (YYPACT_NINF < yyn && yyn <= YYLAST))
    return 0;
  else
    {
      int yytype = YYTRANSLATE (yychar);
      YYSIZE_T yysize0 = yytnamerr (0, yytname[yytype]);
      YYSIZE_T yysize = yysize0;
      YYSIZE_T yysize1;
      int yysize_overflow = 0;
      enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
      char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
      int yyx;

# if 0
      /* This is so xgettext sees the translatable formats that are
	 constructed on the fly.  */
      YY_("syntax error, unexpected %s");
      YY_("syntax error, unexpected %s, expecting %s");
      YY_("syntax error, unexpected %s, expecting %s or %s");
      YY_("syntax error, unexpected %s, expecting %s or %s or %s");
      YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s");
# endif
      char *yyfmt;
      char const *yyf;
      static char const yyunexpected[] = "syntax error, unexpected %s";
      static char const yyexpecting[] = ", expecting %s";
      static char const yyor[] = " or %s";
      char yyformat[sizeof yyunexpected
		    + sizeof yyexpecting - 1
		    + ((YYERROR_VERBOSE_ARGS_MAXIMUM - 2)
		       * (sizeof yyor - 1))];
      char const *yyprefix = yyexpecting;

      /* Start YYX at -YYN if negative to avoid negative indexes in
	 YYCHECK.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;

      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yycount = 1;

      yyarg[0] = yytname[yytype];
      yyfmt = yystpcpy (yyformat, yyunexpected);

      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
	if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR)
	  {
	    if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
	      {
		yycount = 1;
		yysize = yysize0;
		yyformat[sizeof yyunexpected - 1] = '\0';
		break;
	      }
	    yyarg[yycount++] = yytname[yyx];
	    yysize1 = yysize + yytnamerr (0, yytname[yyx]);
	    yysize_overflow |= (yysize1 < yysize);
	    yysize = yysize1;
	    yyfmt = yystpcpy (yyfmt, yyprefix);
	    yyprefix = yyor;
	  }

      yyf = YY_(yyformat);
      yysize1 = yysize + yystrlen (yyf);
      yysize_overflow |= (yysize1 < yysize);
      yysize = yysize1;

      if (yysize_overflow)
	return YYSIZE_MAXIMUM;

      if (yyresult)
	{
	  /* Avoid sprintf, as that infringes on the user's name space.
	     Don't have undefined behavior even if the translation
	     produced a string with the wrong number of "%s"s.  */
	  char *yyp = yyresult;
	  int yyi = 0;
	  while ((*yyp = *yyf) != '\0')
	    {
	      if (*yyp == '%' && yyf[1] == 's' && yyi < yycount)
		{
		  yyp += yytnamerr (yyp, yyarg[yyi++]);
		  yyf += 2;
		}
	      else
		{
		  yyp++;
		  yyf++;
		}
	    }
	}
      return yysize;
    }
}
#endif /* YYERROR_VERBOSE */


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, ObExprParseResult* result)
#else
static void
yydestruct (yymsg, yytype, yyvaluep, yylocationp, result)
    const char *yymsg;
    int yytype;
    YYSTYPE *yyvaluep;
    YYLTYPE *yylocationp;
    ObExprParseResult* result;
#endif
{
  YYUSE (yyvaluep);
  YYUSE (yylocationp);
  YYUSE (result);

  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  switch (yytype)
    {

      default:
	break;
    }
}

/* Prevent warnings from -Wmissing-prototypes.  */
#ifdef YYPARSE_PARAM
#if defined __STDC__ || defined __cplusplus
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined __STDC__ || defined __cplusplus
int yyparse (ObExprParseResult* result);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */





/*-------------------------.
| yyparse or yypush_parse.  |
`-------------------------*/

#ifdef YYPARSE_PARAM
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void *YYPARSE_PARAM)
#else
int
yyparse (YYPARSE_PARAM)
    void *YYPARSE_PARAM;
#endif
#else /* ! YYPARSE_PARAM */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (ObExprParseResult* result)
#else
int
yyparse (result)
    ObExprParseResult* result;
#endif
#endif
{
/* The lookahead symbol.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;

/* Location data for the lookahead symbol.  */
YYLTYPE yylloc;

    /* Number of syntax errors so far.  */
    int yynerrs;

    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       `yyss': related to states.
       `yyvs': related to semantic values.
       `yyls': related to locations.

       Refer to the stacks thru separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    /* The location stack.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls;
    YYLTYPE *yylsp;

    /* The locations where the error started and ended.  */
    YYLTYPE yyerror_range[2];

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yytoken = 0;
  yyss = yyssa;
  yyvs = yyvsa;
  yyls = yylsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */
  if (SELECT_STMT_PARSE_MODE == result->parse_mode_) {
    yychar = DUMMY_SELECT_CLAUSE;
  } else if (INSERT_STMT_PARSE_MODE == result->parse_mode_) {
    yychar = DUMMY_INSERT_CLAUSE;
  }


  /* Initialize stack pointers.
     Waste one element of value and location stack
     so that they stay on the same level as the state stack.
     The wasted elements are never initialized.  */
  yyssp = yyss;
  yyvsp = yyvs;
  yylsp = yyls;

#if YYLTYPE_IS_TRIVIAL
  /* Initialize the default location before parsing starts.  */
  yylloc.first_line   = yylloc.last_line   = 1;
  yylloc.first_column = yylloc.last_column = 1;
#endif

  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
	/* Give user a chance to reallocate the stack.  Use copies of
	   these so that the &'s don't force the real ones into
	   memory.  */
	YYSTYPE *yyvs1 = yyvs;
	yytype_int16 *yyss1 = yyss;
	YYLTYPE *yyls1 = yyls;

	/* Each stack pointer address is followed by the size of the
	   data in use in that stack, in bytes.  This used to be a
	   conditional around just the two extra args, but that might
	   be undefined if yyoverflow is a macro.  */
	yyoverflow (YY_("memory exhausted"),
		    &yyss1, yysize * sizeof (*yyssp),
		    &yyvs1, yysize * sizeof (*yyvsp),
		    &yyls1, yysize * sizeof (*yylsp),
		    &yystacksize);

	yyls = yyls1;
	yyss = yyss1;
	yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
	goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
	yystacksize = YYMAXDEPTH;

      {
	yytype_int16 *yyss1 = yyss;
	union yyalloc *yyptr =
	  (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
	if (! yyptr)
	  goto yyexhaustedlab;
	YYSTACK_RELOCATE (yyss_alloc, yyss);
	YYSTACK_RELOCATE (yyvs_alloc, yyvs);
	YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
	if (yyss1 != yyssa)
	  YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
		  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
	YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yyn == YYPACT_NINF)
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = YYLEX;
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yyn == 0 || yyn == YYTABLE_NINF)
	goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  *++yyvsp = yylval;
  *++yylsp = yylloc;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     `$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location.  */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 4:

    { YYACCEPT; ;}
    break;

  case 5:

    { YYACCEPT; ;}
    break;

  case 6:

    { YYACCEPT; ;}
    break;

  case 7:

    { YYACCEPT; ;}
    break;

  case 25:

    { add_relation(result, (yyvsp[(1) - (3)].list), (yyvsp[(2) - (3)].func), (yyvsp[(3) - (3)].list)); ;}
    break;

  case 26:

    { add_relation(result, (yyvsp[(2) - (5)].list), (yyvsp[(3) - (5)].func), (yyvsp[(4) - (5)].list)); ;}
    break;

  case 27:

    { add_relation(result, (yyvsp[(1) - (5)].list), F_COMP_EQ, (yyvsp[(4) - (5)].list)); ;}
    break;

  case 28:

    { (yyval.relation) = NULL; ;}
    break;

  case 29:

    {
          add_relation(result, (yyvsp[(1) - (5)].list), F_COMP_GE, (yyvsp[(3) - (5)].list));
          add_relation(result, (yyvsp[(1) - (5)].list), F_COMP_LE, (yyvsp[(5) - (5)].list));
          (yyval.relation) = NULL;
        ;}
    break;

  case 30:

    {
          ObProxyTokenNode *null_node = NULL;
          ObProxyTokenList *token_list = NULL;
          malloc_node(null_node, result, TOKEN_NULL);
          malloc_list(token_list, result, null_node);
          add_relation(result, (yyvsp[(1) - (3)].list), F_COMP_EQ, token_list);
        ;}
    break;

  case 31:

    {;}
    break;

  case 32:

    { (yyval.func) = F_COMP_EQ; ;}
    break;

  case 33:

    { (yyval.func) = F_COMP_NSEQ; ;}
    break;

  case 34:

    { (yyval.func) = F_COMP_GE; ;}
    break;

  case 35:

    { (yyval.func) = F_COMP_GT; ;}
    break;

  case 36:

    { (yyval.func) = F_COMP_LE; ;}
    break;

  case 37:

    { (yyval.func) = F_COMP_LT; ;}
    break;

  case 38:

    { (yyval.func) = F_COMP_NE; ;}
    break;

  case 39:

    { (yyval.list) = (yyvsp[(1) - (1)].list); ;}
    break;

  case 40:

    { (yyval.list) = (yyvsp[(1) - (3)].list); add_token_list((yyvsp[(1) - (3)].list), (yyvsp[(3) - (3)].list)); ;}
    break;

  case 41:

    { (yyval.list) = (yyvsp[(1) - (1)].list); ;}
    break;

  case 42:

    { malloc_list((yyval.list), result, (yyvsp[(1) - (1)].node)); ;}
    break;

  case 43:

    { add_token((yyvsp[(1) - (2)].list), result, (yyvsp[(2) - (2)].node)); (yyval.list) = (yyvsp[(1) - (2)].list); ;}
    break;

  case 44:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 45:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 46:

    { (yyval.node) = calc_unary_operator((yyvsp[(2) - (2)].node), result, OPT_ADD); ;}
    break;

  case 47:

    { (yyval.node) = calc_unary_operator((yyvsp[(2) - (2)].node), result, OPT_MINUS); ;}
    break;

  case 48:

    {
            ObProxyTokenList *dummylist = NULL;
            malloc_list(dummylist, result, (yyvsp[(2) - (2)].node));
            malloc_func_node((yyval.node), result, OPT_NOT);
            (yyval.node)->child_ = dummylist;
          ;}
    break;

  case 49:

    {
            ObProxyTokenList *dummylist = NULL;
            malloc_list(dummylist, result, (yyvsp[(1) - (3)].node));
            add_token(dummylist, result, (yyvsp[(3) - (3)].node));
            malloc_func_node((yyval.node), result, OPT_ADD);
            (yyval.node)->child_ = dummylist;
          ;}
    break;

  case 50:

    {
            ObProxyTokenList *dummylist = NULL;
            malloc_list(dummylist, result, (yyvsp[(1) - (3)].node));
            add_token(dummylist, result, (yyvsp[(3) - (3)].node));
            malloc_func_node((yyval.node), result, OPT_MINUS);
            (yyval.node)->child_ = dummylist;
          ;}
    break;

  case 51:

    {
            ObProxyTokenList *dummylist = NULL;
            malloc_list(dummylist, result, (yyvsp[(1) - (3)].node));
            add_token(dummylist, result, (yyvsp[(3) - (3)].node));
            malloc_func_node((yyval.node), result, OPT_MUL);
            (yyval.node)->child_ = dummylist;
          ;}
    break;

  case 52:

    {
            ObProxyTokenList *dummylist = NULL;
            malloc_list(dummylist, result, (yyvsp[(1) - (3)].node));
            add_token(dummylist, result, (yyvsp[(3) - (3)].node));
            malloc_func_node((yyval.node), result, OPT_DIV);
            (yyval.node)->child_ = dummylist;
          ;}
    break;

  case 53:

    {
            ObProxyTokenList *dummylist = NULL;
            malloc_list(dummylist, result, (yyvsp[(1) - (3)].node));
            add_token(dummylist, result, (yyvsp[(3) - (3)].node));
            malloc_func_node((yyval.node), result, OPT_AND);
            (yyval.node)->child_ = dummylist;
          ;}
    break;

  case 54:

    {
            ObProxyTokenList *dummylist = NULL;
            malloc_list(dummylist, result, (yyvsp[(1) - (3)].node));
            add_token(dummylist, result, (yyvsp[(3) - (3)].node));
            malloc_func_node((yyval.node), result, OPT_MOD);
            (yyval.node)->child_ = dummylist;
          ;}
    break;

  case 55:

    { (yyval.list) = NULL; ;}
    break;

  case 56:

    { add_token_list((yyvsp[(1) - (3)].list), (yyvsp[(3) - (3)].list)); (yyval.list) = (yyvsp[(1) - (3)].list); ;}
    break;

  case 57:

    { add_token_list((yyvsp[(1) - (3)].list), (yyvsp[(3) - (3)].list)); (yyval.list) = (yyvsp[(1) - (3)].list); ;}
    break;

  case 58:

    { malloc_list((yyval.list), result, (yyvsp[(1) - (1)].node)); ;}
    break;

  case 59:

    { add_token((yyvsp[(1) - (3)].list), result, (yyvsp[(3) - (3)].node)); (yyval.list) = (yyvsp[(1) - (3)].list); ;}
    break;

  case 60:

    { malloc_list((yyval.list), result, (yyvsp[(1) - (1)].node)); ;}
    break;

  case 61:

    { 
                ObProxyTokenNode *default_type = NULL;
                malloc_node(default_type, result, TOKEN_INT_VAL);
                default_type->int_value_ = 0;
                malloc_list((yyval.list), result, default_type);
                add_token((yyval.list), result, (yyvsp[(1) - (1)].node)); 
              ;}
    break;

  case 62:

    {
                malloc_list((yyval.list), result, (yyvsp[(1) - (2)].node));
                add_token((yyval.list), result, (yyvsp[(2) - (2)].node));
              ;}
    break;

  case 63:

    { malloc_node((yyval.node), result, TOKEN_INT_VAL); (yyval.node)->int_value_ = 0; ;}
    break;

  case 64:

    { malloc_node((yyval.node), result, TOKEN_INT_VAL); (yyval.node)->int_value_ = 1; ;}
    break;

  case 65:

    { malloc_node((yyval.node), result, TOKEN_INT_VAL); (yyval.node)->int_value_ = 2; ;}
    break;

  case 66:

    {
                        malloc_list((yyval.list), result, (yyvsp[(1) - (1)].node));
                      ;}
    break;

  case 67:

    {
                       malloc_node((yyval.node), result, TOKEN_STR_VAL);
                       (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
                     ;}
    break;

  case 68:

    {
       malloc_node((yyval.node), result, TOKEN_COLUMN);
       (yyval.node)->part_key_idx_ = -1;
       (yyval.node)->column_name_ = (yyvsp[(1) - (1)].str);
       result->has_rowid_ = true;
     ;}
    break;

  case 69:

    { 
      malloc_node((yyval.node), result, TOKEN_HEX_VAL); (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str); 
     ;}
    break;

  case 70:

    {
       malloc_node((yyval.node), result, TOKEN_COLUMN);
       (yyval.node)->part_key_idx_ = get_part_key_idx(NULL, NULL, &(yyvsp[(1) - (1)].str), result);
       (yyval.node)->column_name_ = (yyvsp[(1) - (1)].str);
     ;}
    break;

  case 71:

    {
       malloc_node((yyval.node), result, TOKEN_COLUMN);
       (yyval.node)->part_key_idx_ = get_part_key_idx(NULL, &(yyvsp[(1) - (3)].str), &(yyvsp[(3) - (3)].str), result);
       (yyval.node)->table_name_ = (yyvsp[(1) - (3)].str);
       (yyval.node)->column_name_ = (yyvsp[(3) - (3)].str);
     ;}
    break;

  case 72:

    {
       malloc_node((yyval.node), result, TOKEN_COLUMN);
       (yyval.node)->part_key_idx_ = get_part_key_idx(&(yyvsp[(1) - (5)].str), &(yyvsp[(3) - (5)].str), &(yyvsp[(5) - (5)].str), result);
       (yyval.node)->table_name_ = (yyvsp[(3) - (5)].str);
       (yyval.node)->column_name_ = (yyvsp[(5) - (5)].str);
     ;}
    break;

  case 73:

    {
       malloc_node((yyval.node), result, TOKEN_FUNC);
       (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
     ;}
    break;

  case 74:

    {
       malloc_node((yyval.node), result, TOKEN_FUNC);
       (yyval.node)->str_value_ = (yyvsp[(1) - (3)].str);
     ;}
    break;

  case 75:

    {
       malloc_node((yyval.node), result, TOKEN_FUNC);
       (yyval.node)->str_value_ = (yyvsp[(1) - (4)].str);
       (yyval.node)->child_ = (yyvsp[(3) - (4)].list);
     ;}
    break;

  case 76:

    {
      malloc_node((yyval.node), result, TOKEN_FUNC);
      (yyval.node)->str_value_ = (yyvsp[(1) - (4)].str);
      (yyval.node)->child_ = (yyvsp[(3) - (4)].list);
     ;}
    break;

  case 77:

    {
      malloc_node((yyval.node), result, TOKEN_FUNC);
      (yyval.node)->str_value_ = (yyvsp[(1) - (2)].str);
      (yyval.node)->child_ = (yyvsp[(2) - (2)].list);
     ;}
    break;

  case 78:

    {
      malloc_node((yyval.node), result, TOKEN_FUNC);
      (yyval.node)->str_value_ = (yyvsp[(1) - (2)].str);
      (yyval.node)->child_ = (yyvsp[(2) - (2)].list);
     ;}
    break;

  case 79:

    {
      malloc_node((yyval.node), result, TOKEN_FUNC);
      (yyval.node)->str_value_ = (yyvsp[(1) - (2)].str);
      (yyval.node)->child_ = (yyvsp[(2) - (2)].list);
     ;}
    break;

  case 80:

    {
       malloc_node((yyval.node), result, TOKEN_FUNC);
       (yyval.node)->str_value_ = (yyvsp[(1) - (4)].str);
       (yyval.node)->child_ = (yyvsp[(3) - (4)].list);
     ;}
    break;

  case 81:

    {
       malloc_node((yyval.node), result, TOKEN_FUNC);
       (yyval.node)->str_value_ = (yyvsp[(1) - (4)].str);
       (yyval.node)->child_ = (yyvsp[(3) - (4)].list);
     ;}
    break;

  case 82:

    { malloc_node((yyval.node), result, TOKEN_INT_VAL); (yyval.node)->int_value_ = (yyvsp[(1) - (1)].num); ;}
    break;

  case 83:

    { malloc_node((yyval.node), result, TOKEN_STR_VAL); (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str); ;}
    break;

  case 84:

    { malloc_node((yyval.node), result, TOKEN_STR_VAL); (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str); ;}
    break;

  case 85:

    {
       result->placeholder_list_idx_++;
       malloc_node((yyval.node), result, TOKEN_PLACE_HOLDER);
       (yyval.node)->placeholder_idx_ = result->placeholder_list_idx_ - 1;
     ;}
    break;

  case 86:

    {
       malloc_node((yyval.node), result, TOKEN_PLACE_HOLDER);
       (yyval.node)->placeholder_idx_ = (yyvsp[(1) - (1)].num);
     ;}
    break;

  case 87:

    {
       malloc_node((yyval.node), result, TOKEN_NULL);
     ;}
    break;

  case 88:

    { YYACCEPT; ;}
    break;

  case 89:

    { YYACCEPT; ;}
    break;

  case 90:

    { YYACCEPT; ;}
    break;

  case 92:

    {
                   result->multi_param_values_++;
                 ;}
    break;

  case 93:

    {
                   result->multi_param_values_++;
                 ;}
    break;

  case 94:

    { init_empty_column_insert_stmt(result);;}
    break;

  case 96:

    {
              malloc_list((yyval.list), result, (yyvsp[(1) - (1)].node));
              add_left_relation_value(result, (yyval.list));
           ;}
    break;

  case 97:

    {
              malloc_list((yyval.list), result, (yyvsp[(3) - (3)].node));
              add_left_relation_value(result, (yyval.list));
           ;}
    break;

  case 98:

    {
                      result->column_idx_++;
                      malloc_node((yyval.node), result, TOKEN_COLUMN);
                      (yyval.node)->part_key_idx_ = get_part_key_idx(NULL, NULL, &(yyvsp[(1) - (1)].str), result);
                      (yyval.node)->column_name_ = (yyvsp[(1) - (1)].str);
                    ;}
    break;

  case 99:

    {
                  if (result->multi_param_values_ < 1) {
                    result->values_list_idx_ = 0;
                    if (result->is_empty_column_insert_stmt_) {
                      // only record relation of part key here
                      ObProxyRelationExpr *relation = get_values_relation(result, (yyvsp[(1) - (1)].list));
                      check_and_add_relation(result, relation);
                    } else {
                      result->all_relation_info_.right_value_num_ = 0;
                      add_right_relation_value(result, (yyvsp[(1) - (1)].list));
                    }

                  }
                ;}
    break;

  case 100:

    {
                  if (result->multi_param_values_ < 1) {
                    result->values_list_idx_++;
                    if (result->is_empty_column_insert_stmt_) {
                      ObProxyRelationExpr *relation = get_values_relation(result, (yyvsp[(3) - (3)].list));
                      check_and_add_relation(result, relation);
                    } else {
                      add_right_relation_value(result, (yyvsp[(3) - (3)].list));
                    }
                  }
                ;}
    break;

  case 101:

    {;}
    break;

  case 102:

    {;}
    break;

  case 104:

    {;}
    break;



      default: break;
    }
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now `shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*------------------------------------.
| yyerrlab -- here on detecting error |
`------------------------------------*/
yyerrlab:
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (&yylloc, result, YY_("syntax error"));
#else
      {
	YYSIZE_T yysize = yysyntax_error (0, yystate, yychar);
	if (yymsg_alloc < yysize && yymsg_alloc < YYSTACK_ALLOC_MAXIMUM)
	  {
	    YYSIZE_T yyalloc = 2 * yysize;
	    if (! (yysize <= yyalloc && yyalloc <= YYSTACK_ALLOC_MAXIMUM))
	      yyalloc = YYSTACK_ALLOC_MAXIMUM;
	    if (yymsg != yymsgbuf)
	      YYSTACK_FREE (yymsg);
	    yymsg = (char *) YYSTACK_ALLOC (yyalloc);
	    if (yymsg)
	      yymsg_alloc = yyalloc;
	    else
	      {
		yymsg = yymsgbuf;
		yymsg_alloc = sizeof yymsgbuf;
	      }
	  }

	if (0 < yysize && yysize <= yymsg_alloc)
	  {
	    (void) yysyntax_error (yymsg, yystate, yychar);
	    yyerror (&yylloc, result, yymsg);
	  }
	else
	  {
	    yyerror (&yylloc, result, YY_("syntax error"));
	    if (yysize != 0)
	      goto yyexhaustedlab;
	  }
      }
#endif
    }

  yyerror_range[0] = yylloc;

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
	 error, discard it.  */

      if (yychar <= YYEOF)
	{
	  /* Return failure if at end of input.  */
	  if (yychar == YYEOF)
	    YYABORT;
	}
      else
	{
	  yydestruct ("Error: discarding",
		      yytoken, &yylval, &yylloc, result);
	  yychar = YYEMPTY;
	}
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

  yyerror_range[0] = yylsp[1-yylen];
  /* Do not reclaim the symbols of the rule which action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;	/* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (yyn != YYPACT_NINF)
	{
	  yyn += YYTERROR;
	  if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
	    {
	      yyn = yytable[yyn];
	      if (0 < yyn)
		break;
	    }
	}

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
	YYABORT;

      yyerror_range[0] = *yylsp;
      yydestruct ("Error: popping",
		  yystos[yystate], yyvsp, yylsp, result);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  *++yyvsp = yylval;

  yyerror_range[1] = yylloc;
  /* Using YYLLOC is tempting, but would change the location of
     the lookahead.  YYLOC is available though.  */
  YYLLOC_DEFAULT (yyloc, (yyerror_range - 1), 2);
  *++yylsp = yyloc;

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#if !defined(yyoverflow) || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (&yylloc, result, YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEMPTY)
     yydestruct ("Cleanup: discarding lookahead",
		 yytoken, &yylval, &yylloc, result);
  /* Do not reclaim the symbols of the rule which action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
		  yystos[*yyssp], yyvsp, yylsp, result);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  /* Make sure YYID is used.  */
  return YYID (yyresult);
}




void yyerror(YYLTYPE* yylloc, ObExprParseResult* p, char* s, ...)
{
  UNUSED(yylloc);
  UNUSED(p);
  UNUSED(s);
  // do nothing
}

void ob_expr_gbk_parser_fatal_error(yyconst char *msg, yyscan_t yyscanner)
{
  fprintf(stderr, "FATAL ERROR:%s\n", msg);
  ObExprParseResult *p = ob_expr_parser_gbk_yyget_extra(yyscanner);
  if (OB_ISNULL(p)) {
    fprintf(stderr, "unexpected null parse result\n");
  } else {
    longjmp(p->jmp_buf_, 1);//the secord param must be non-zero value
  }
}

int ob_expr_parse_gbk_sql(ObExprParseResult* p, const char* buf, size_t len)
{
  int ret = OB_SUCCESS;
  //obexprdebug = 1;
  if (OB_ISNULL(p) || OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    // print err msg later
  } else if (OB_FAIL(ob_expr_parser_gbk_yylex_init_extra(p, &(p->yyscan_info_)))) {
    // print err msg later
  } else {
    int val = setjmp(p->jmp_buf_);
    if (val) {
      ret = OB_PARSER_ERR_PARSE_SQL;
    } else {
      ob_expr_parser_gbk_yy_scan_buffer((char *)buf, len, p->yyscan_info_);
      if (OB_FAIL(ob_expr_parser_gbk_yyparse(p))) {
        // print err msg later
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

