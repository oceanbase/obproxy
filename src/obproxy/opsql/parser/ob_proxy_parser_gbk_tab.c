
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
#define YYSTYPE         OBPROXYSTYPE
#define YYLTYPE         OBPROXYLTYPE
#define yyparse         ob_proxy_parser_gbk_yyparse
#define yylex           ob_proxy_parser_gbk_yylex
#define yyerror         ob_proxy_parser_gbk_yyerror
#define yylval          ob_proxy_parser_gbk_yylval
#define yychar          ob_proxy_parser_gbk_yychar
#define yydebug         ob_proxy_parser_gbk_yydebug
#define yynerrs         ob_proxy_parser_gbk_yynerrs
#define yylloc          ob_proxy_parser_gbk_yylloc

/* Copy the first part of user declarations.  */


#include <stdint.h>
#include "opsql/ob_proxy_parse_define.h"
#include "opsql/parser/ob_proxy_parse_result.h"

#define HANDLE_ACCEPT() \
do {\
  if (result->stmt_count_ > 1) {\
    result->stmt_type_ = OBPROXY_T_MULTI_STMT;\
  }\
  if (NULL != result->end_pos_) {\
  } else if (NULL != result->table_info_.table_name_.str_ && result->table_info_.table_name_.str_len_ > 0) {\
    if (NULL != result->part_name_.str_ && result->part_name_.str_len_ > 0) {\
      result->end_pos_ = result->part_name_.end_ptr_;\
    } else if (NULL != result->table_info_.alias_name_.str_ && result->table_info_.alias_name_.str_len_ > 0) {\
      result->end_pos_ = result->table_info_.alias_name_.end_ptr_;\
    } else {\
      result->end_pos_ = result->table_info_.table_name_.end_ptr_;\
    }\
  } else {\
    result->end_pos_ = ob_proxy_parser_gbk_yyget_text(result->yyscan_info_);\
  }\
  YYACCEPT;\
} while (0);

static inline void handle_stmt_end(ObProxyParseResult* result)
{
  // no need to judge NULL
  if (result->has_ignored_word_) {
    switch (result->cur_stmt_type_) {
      // these stmt should match exactly,
      // so if we have ignored word we should reset type
      case OBPROXY_T_SELECT_TX_RO:
        result->stmt_type_ = OBPROXY_T_SELECT;
        break;
      case OBPROXY_T_SET_AC_0:
        result->stmt_type_ = OBPROXY_T_OTHERS;
        break;
      case OBPROXY_T_BEGIN:
        result->stmt_type_ = OBPROXY_T_OTHERS;
        break;
      case OBPROXY_T_SHOW_TRACE:
        result->stmt_type_ = OBPROXY_T_OTHERS;
        break;
      case OBPROXY_T_SELECT_ROUTE_ADDR:
        result->stmt_type_ = OBPROXY_T_OTHERS;
        break;
      case OBPROXY_T_SET_ROUTE_ADDR:
        result->stmt_type_ = OBPROXY_T_OTHERS;
        break;
      default:
        result->stmt_type_ = result->cur_stmt_type_;
        break;
    }
  } else {
    result->stmt_type_ = result->cur_stmt_type_;
  } 

  if (OBPROXY_T_TEXT_PS_PREPARE == result->text_ps_inner_stmt_type_) {
    ObProxyBasicStmtType tmp_type = result->cur_stmt_type_;
    result->stmt_type_ = OBPROXY_T_TEXT_PS_PREPARE;
    result->text_ps_inner_stmt_type_ = tmp_type;
  }

  result->cur_stmt_type_ = OBPROXY_T_INVALID;
  result->stmt_count_++;
}

#define UPDATE_ALIAS_NAME(name) \
    /* only support select and update with alias name */ \
    /* insert into ... select also have alias name */ \
    if (NULL != result && (OBPROXY_T_SELECT == result->cur_stmt_type_ || OBPROXY_T_UPDATE == result->cur_stmt_type_ \
                           || OBPROXY_T_INSERT == result->cur_stmt_type_ || OBPROXY_T_MERGE == result->cur_stmt_type_)) { \
      result->table_info_.alias_name_ = name; \
    } \

#define HANDLE_ERROR_ACCEPT() \
do {\
  result->has_ignored_word_ = true;\
  if ((OBPROXY_T_INVALID < result->cur_stmt_type_ && result->cur_stmt_type_ < OBPROXY_T_ICMD_MAX) || (OBPROXY_T_PING_PROXY == result->cur_stmt_type_)) {\
    result->cmd_info_.err_type_ = OBPROXY_T_ERR_PARSE;\
  }\
  handle_stmt_end(result);\
  HANDLE_ACCEPT();\
} while (0);

#define SET_ICMD_SUB_TYPE(sub_type) \
do {\
  result->cmd_info_.sub_type_ = sub_type;\
} while (0);

#define SET_ICMD_SUB_AND_ONE_ID(sub_type, id) \
do {\
  result->cmd_info_.sub_type_ = sub_type;\
  result->cmd_info_.integer_[0] = id;\
} while (0);

#define SET_ICMD_SUB_AND_TWO_ID(sub_type, id, id_two) \
do {\
  result->cmd_info_.sub_type_ = sub_type;\
  result->cmd_info_.integer_[0] = id;\
  result->cmd_info_.integer_[1] = id_two;\
} while (0);

#define SET_ICMD_SUB_AND_ONE_STRING(sub_type, string) \
do {\
  result->cmd_info_.sub_type_ = sub_type;\
  result->cmd_info_.string_[0] = string;\
} while (0);

#define SET_ICMD_ONE_STRING(string) \
do {\
  result->cmd_info_.string_[0] = string;\
} while (0);

#define SET_ICMD_TWO_STRING(string, string_two) \
do {\
  result->cmd_info_.string_[0] = string;\
  result->cmd_info_.string_[1] = string_two;\
} while (0);

#define SET_ICMD_SECOND_STRING(string) \
do {\
  result->cmd_info_.string_[1] = string;\
} while (0);

#define SET_ICMD_CONFIG_INT_VALUE(string, integer) \
do {\
  result->cmd_info_.sub_type_ = OBPROXY_T_SUB_CONFIG_INT_VAULE;\
  result->cmd_info_.string_[0] = string;\
  result->cmd_info_.integer_[0] = integer;\
} while (0);

#define SET_ICMD_TYPE_STRING_INT_VALUE(sub_type, string, integer) \
do {\
  result->cmd_info_.sub_type_ = sub_type;\
  result->cmd_info_.string_[0] = string;\
  result->cmd_info_.integer_[0] = integer;\
} while (0);

#define SET_ICMD_ONE_ID(id) \
do {\
  result->cmd_info_.integer_[0] = id;\
} while (0);

#define SET_ICMD_TWO_ID(id, id_two) \
do {\
  result->cmd_info_.integer_[0] = id;\
  result->cmd_info_.integer_[1] = id_two;\
} while (0);

#define SET_ICMD_SECOND_ID(id) \
do {\
  result->cmd_info_.integer_[1] = id;\
} while (0);

#define SET_READ_CONSISTENCY(read_consistency_type) \
do {\
  if (OBPROXY_READ_CONSISTENCY_INVALID == result->read_consistency_type_) {\
    result->read_consistency_type_ = read_consistency_type;\
  }\
} while (0);

#define add_call_node(call_parse_info, call_node) \
do {                                                      \
  if (NULL != call_parse_info.tail_) {\
    call_parse_info.tail_->next_ = call_node;\
    call_parse_info.tail_ = call_node;\
  } else {\
    call_parse_info.head_ = call_node;\
    call_parse_info.tail_ = call_node;\
  }\
  ++call_parse_info.node_count_;\
} while(0)

#define malloc_call_node(call_node, type) \
do {                                                                                        \
  if (OB_ISNULL(call_node = ((ObProxyCallParseNode *)obproxy_parse_malloc(sizeof(ObProxyCallParseNode), result->malloc_pool_)))) { \
    YYABORT;                                                                                \
  } else {                                                                                  \
    call_node->type_ = type;                                                                \
    call_node->next_ = NULL;                                                                 \
  }                                                                                         \
} while(0)                                                                                  \

#define add_text_ps_node(text_ps_parse_info, parse_node) \
do {                                                      \
  if (NULL != text_ps_parse_info.tail_) {\
    text_ps_parse_info.tail_->next_ = parse_node;\
    text_ps_parse_info.tail_ = parse_node;\
  } else {\
    text_ps_parse_info.head_ = parse_node;\
    text_ps_parse_info.tail_ = parse_node;\
  }\
  ++text_ps_parse_info.node_count_;\
} while(0)

#define malloc_parse_node(parse_node) \
do {                                                                                        \
  if (OB_ISNULL(parse_node = ((ObProxyTextPsParseNode *)obproxy_parse_malloc(sizeof(ObProxyTextPsParseNode), result->malloc_pool_)))) { \
    YYABORT;                                                                                \
  } else {                                                                                  \
    parse_node->next_ = NULL;                                                       \
  }                                                                                         \
} while(0)                                                                                  \

#define malloc_shard_column_node(col_node, tb_name, col_name, col_type) \
do {                                                      \
  if (OB_ISNULL(col_node = ((ObShardColumnNode *)obproxy_parse_malloc(sizeof(ObShardColumnNode), result->malloc_pool_)))) { \
    YYABORT;                                                                                \
  }                                                                                         \
  col_node->tb_name_ = tb_name;\
  col_node->col_name_ = col_name;\
  col_node->type_ = col_type;\
} while(0)

#define add_shard_column_node(route_info, col_node) \
do {                                                \
  col_node->next_ = NULL;\
  if (NULL != route_info.tail_) {\
    route_info.tail_->next_ = col_node;\
    route_info.tail_ = col_node;\
  } else {\
    route_info.head_ = col_node;\
    route_info.tail_ = col_node;\
  }\
  ++route_info.node_count_;\
} while(0)

#define add_hint_index(route_info, index_tb_name)   \
do {                                                \
  if (route_info.index_count_ >=0 && route_info.index_count_ < OBPROXY_MAX_HINT_INDEX_COUNT) {\
    route_info.index_tb_name_[route_info.index_count_] = index_tb_name; \
  }\
} while(0)

#define malloc_set_var_node(var_node, value_type) \
do {                                                      \
  if (OB_ISNULL(var_node = ((ObProxySetVarNode *)obproxy_parse_malloc(sizeof(ObProxySetVarNode), result->malloc_pool_)))) { \
    YYABORT;                                                                                \
  }                                                                                         \
  var_node->value_type_ = value_type;\
  var_node->next_ = NULL;\
} while(0)

#define add_set_var_node(set_info, var_node, name, type) \
do {                                                     \
  var_node->name_ = name; \
  var_node->type_ = type; \
  if (NULL != set_info.tail_) {\
    set_info.tail_->next_ = var_node;\
    set_info.tail_ = var_node;\
  } else {\
    set_info.head_ = var_node;\
    set_info.tail_ = var_node;\
  }\
  ++set_info.node_count_;\
} while(0)




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


#ifndef YY_OBPROXY_OB_PROXY_PARSER_TAB_H_INCLUDED
# define YY_OBPROXY_OB_PROXY_PARSER_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef OBPROXY_GBK_DEBUG
# if defined YYDEBUG
#if YYDEBUG
#   define OBPROXY_GBK_DEBUG 1
#  else
#   define OBPROXY_GBK_DEBUG 0
#  endif
# else /* ! defined YYDEBUG */
#  define OBPROXY_GBK_DEBUG 0
# endif /* ! defined YYDEBUG */
#endif  /* ! defined OBPROXY_GBK_DEBUG */
#if OBPROXY_GBK_DEBUG
extern int ob_proxy_parser_gbk_yydebug;
#endif
/* Tokens.  */
#ifndef OBPROXYTOKENTYPE
# define OBPROXYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum obproxytokentype {
     DUMMY_WHERE_CLAUSE = 258,
     DUMMY_INSERT_CLAUSE = 259,
     SELECT = 260,
     DELETE = 261,
     INSERT = 262,
     UPDATE = 263,
     REPLACE = 264,
     MERGE = 265,
     SHOW = 266,
     SET = 267,
     CALL = 268,
     CREATE = 269,
     DROP = 270,
     ALTER = 271,
     TRUNCATE = 272,
     RENAME = 273,
     TABLE = 274,
     STATUS = 275,
     UNIQUE = 276,
     GRANT = 277,
     REVOKE = 278,
     ANALYZE = 279,
     PURGE = 280,
     COMMENT = 281,
     FROM = 282,
     DUAL = 283,
     PREPARE = 284,
     EXECUTE = 285,
     USING = 286,
     DEALLOCATE = 287,
     SELECT_HINT_BEGIN = 288,
     UPDATE_HINT_BEGIN = 289,
     DELETE_HINT_BEGIN = 290,
     INSERT_HINT_BEGIN = 291,
     REPLACE_HINT_BEGIN = 292,
     MERGE_HINT_BEGIN = 293,
     HINT_END = 294,
     COMMENT_BEGIN = 295,
     COMMENT_END = 296,
     ROUTE_TABLE = 297,
     ROUTE_PART_KEY = 298,
     QUERY_TIMEOUT = 299,
     READ_CONSISTENCY = 300,
     WEAK = 301,
     STRONG = 302,
     FROZEN = 303,
     PLACE_HOLDER = 304,
     END_P = 305,
     ERROR = 306,
     WHEN = 307,
     FLASHBACK = 308,
     AUDIT = 309,
     NOAUDIT = 310,
     BEGI = 311,
     START = 312,
     TRANSACTION = 313,
     READ = 314,
     ONLY = 315,
     WITH = 316,
     CONSISTENT = 317,
     SNAPSHOT = 318,
     INDEX = 319,
     XA = 320,
     WARNINGS = 321,
     ERRORS = 322,
     TRACE = 323,
     QUICK = 324,
     COUNT = 325,
     AS = 326,
     WHERE = 327,
     VALUES = 328,
     ORDER = 329,
     GROUP = 330,
     HAVING = 331,
     INTO = 332,
     UNION = 333,
     FOR = 334,
     TX_READ_ONLY = 335,
     AUTOCOMMIT_0 = 336,
     SELECT_OBPROXY_ROUTE_ADDR = 337,
     SET_OBPROXY_ROUTE_ADDR = 338,
     NAME_OB_DOT = 339,
     NAME_OB = 340,
     EXPLAIN = 341,
     DESC = 342,
     DESCRIBE = 343,
     NAME_STR = 344,
     USE = 345,
     HELP = 346,
     SET_NAMES = 347,
     SET_CHARSET = 348,
     SET_PASSWORD = 349,
     SET_DEFAULT = 350,
     SET_OB_READ_CONSISTENCY = 351,
     SET_TX_READ_ONLY = 352,
     GLOBAL = 353,
     SESSION = 354,
     NUMBER_VAL = 355,
     GROUP_ID = 356,
     TABLE_ID = 357,
     ELASTIC_ID = 358,
     TESTLOAD = 359,
     ODP_COMMENT = 360,
     TNT_ID = 361,
     DISASTER_STATUS = 362,
     TRACE_ID = 363,
     RPC_ID = 364,
     DBP_COMMENT = 365,
     ROUTE_TAG = 366,
     SYS_TAG = 367,
     TABLE_NAME = 368,
     SCAN_ALL = 369,
     PARALL = 370,
     SHARD_KEY = 371,
     INT_NUM = 372,
     SHOW_PROXYNET = 373,
     THREAD = 374,
     CONNECTION = 375,
     LIMIT = 376,
     OFFSET = 377,
     SHOW_PROCESSLIST = 378,
     SHOW_PROXYSESSION = 379,
     SHOW_GLOBALSESSION = 380,
     ATTRIBUTE = 381,
     VARIABLES = 382,
     ALL = 383,
     STAT = 384,
     SHOW_PROXYCONFIG = 385,
     DIFF = 386,
     USER = 387,
     LIKE = 388,
     SHOW_PROXYSM = 389,
     SHOW_PROXYCLUSTER = 390,
     SHOW_PROXYRESOURCE = 391,
     SHOW_PROXYCONGESTION = 392,
     SHOW_PROXYROUTE = 393,
     PARTITION = 394,
     ROUTINE = 395,
     SHOW_PROXYVIP = 396,
     SHOW_PROXYMEMORY = 397,
     OBJPOOL = 398,
     SHOW_SQLAUDIT = 399,
     SHOW_WARNLOG = 400,
     SHOW_PROXYSTAT = 401,
     REFRESH = 402,
     SHOW_PROXYTRACE = 403,
     SHOW_PROXYINFO = 404,
     BINARY = 405,
     UPGRADE = 406,
     IDC = 407,
     SHOW_TOPOLOGY = 408,
     GROUP_NAME = 409,
     SHOW_DB_VERSION = 410,
     SHOW_DATABASES = 411,
     SHOW_TABLES = 412,
     SHOW_FULL_TABLES = 413,
     SELECT_DATABASE = 414,
     SHOW_CREATE_TABLE = 415,
     SELECT_PROXY_VERSION = 416,
     SHOW_COLUMNS = 417,
     SHOW_INDEX = 418,
     ALTER_PROXYCONFIG = 419,
     ALTER_PROXYRESOURCE = 420,
     PING_PROXY = 421,
     KILL_PROXYSESSION = 422,
     KILL_GLOBALSESSION = 423,
     KILL = 424,
     QUERY = 425
   };
#endif



#if ! defined OBPROXYSTYPE && ! defined OBPROXYSTYPE_IS_DECLARED
typedef union OBPROXYSTYPE
{


  int64_t               num;
  ObProxyParseString    str;
  ObProxyCallParseNode  *node;
  ObShardColumnNode     *shard_node;
  ObProxySetVarNode     *var_node;



} OBPROXYSTYPE;
# define OBPROXYSTYPE_IS_TRIVIAL 1
# define obproxystype OBPROXYSTYPE /* obsolescent; will be withdrawn */
# define OBPROXYSTYPE_IS_DECLARED 1
#endif

#if ! defined OBPROXYLTYPE && ! defined OBPROXYLTYPE_IS_DECLARED
typedef struct OBPROXYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} OBPROXYLTYPE;
# define obproxyltype OBPROXYLTYPE /* obsolescent; will be withdrawn */
# define OBPROXYLTYPE_IS_DECLARED 1
# define OBPROXYLTYPE_IS_TRIVIAL 1
#endif


#endif
/* Copy the second part of user declarations.  */


#include "ob_proxy_parser_gbk_lex.h"
#define YYLEX_PARAM result->yyscan_info_
extern void yyerror(YYLTYPE* yylloc, ObProxyParseResult* p, char* s,...);
extern void *obproxy_parse_malloc(const size_t nbyte, void *malloc_pool);



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
#define YYFINAL  322
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   1614

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  182
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  137
/* YYNRULES -- Number of rules.  */
#define YYNRULES  442
/* YYNRULES -- Number of states.  */
#define YYNSTATES  713

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   425

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,   180,     2,     2,     2,     2,
     175,   176,   181,     2,   173,     2,   177,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   171,
       2,   174,     2,     2,   172,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   178,     2,   179,     2,     2,     2,     2,
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
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   112,   113,   114,
     115,   116,   117,   118,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,   131,   132,   133,   134,
     135,   136,   137,   138,   139,   140,   141,   142,   143,   144,
     145,   146,   147,   148,   149,   150,   151,   152,   153,   154,
     155,   156,   157,   158,   159,   160,   161,   162,   163,   164,
     165,   166,   167,   168,   169,   170
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     5,     7,     9,    12,    15,    18,    22,
      24,    27,    31,    33,    36,    38,    40,    42,    44,    46,
      48,    50,    52,    54,    56,    58,    60,    62,    64,    66,
      68,    70,    72,    74,    76,    78,    80,    82,    84,    88,
      91,    94,    97,   100,   103,   106,   108,   110,   113,   115,
     117,   119,   121,   122,   124,   126,   129,   131,   133,   135,
     137,   139,   141,   143,   145,   148,   153,   156,   158,   160,
     164,   167,   171,   174,   177,   181,   185,   187,   189,   191,
     193,   195,   197,   199,   201,   203,   206,   208,   210,   212,
     213,   216,   217,   219,   222,   228,   232,   234,   238,   241,
     245,   247,   249,   251,   253,   255,   257,   259,   261,   263,
     265,   267,   269,   271,   273,   276,   279,   283,   289,   293,
     299,   300,   303,   304,   307,   311,   315,   321,   323,   325,
     329,   335,   343,   345,   349,   351,   353,   355,   357,   359,
     361,   367,   369,   373,   379,   380,   382,   386,   388,   390,
     392,   395,   399,   401,   403,   406,   408,   411,   415,   419,
     421,   423,   425,   426,   430,   432,   436,   440,   446,   449,
     452,   457,   460,   463,   467,   469,   474,   481,   486,   492,
     499,   504,   508,   510,   512,   514,   516,   519,   523,   529,
     536,   543,   550,   557,   564,   572,   579,   586,   593,   600,
     609,   618,   619,   622,   625,   628,   630,   634,   636,   641,
     646,   650,   657,   662,   667,   674,   678,   680,   684,   685,
     689,   693,   697,   701,   705,   709,   713,   717,   721,   725,
     731,   735,   736,   738,   739,   741,   743,   745,   747,   750,
     752,   755,   757,   760,   763,   767,   768,   770,   773,   775,
     778,   780,   783,   786,   787,   790,   795,   797,   802,   808,
     810,   815,   820,   826,   827,   829,   831,   833,   834,   836,
     840,   844,   847,   849,   851,   853,   855,   857,   859,   861,
     863,   865,   867,   869,   871,   873,   875,   877,   879,   881,
     883,   885,   887,   889,   891,   893,   894,   897,   902,   907,
     908,   911,   912,   915,   918,   920,   922,   926,   929,   933,
     938,   940,   943,   944,   947,   951,   954,   957,   960,   961,
     964,   968,   971,   975,   978,   982,   986,   991,   993,   996,
     999,  1003,  1006,  1009,  1010,  1012,  1014,  1017,  1020,  1024,
    1027,  1029,  1032,  1034,  1037,  1040,  1043,  1046,  1047,  1049,
    1053,  1059,  1062,  1066,  1069,  1070,  1072,  1075,  1078,  1081,
    1084,  1089,  1095,  1101,  1105,  1107,  1110,  1114,  1118,  1121,
    1124,  1128,  1132,  1133,  1136,  1138,  1142,  1146,  1150,  1151,
    1153,  1155,  1159,  1162,  1166,  1169,  1172,  1174,  1175,  1178,
    1183,  1186,  1188,  1192,  1195,  1200,  1204,  1210,  1212,  1214,
    1216,  1218,  1220,  1222,  1224,  1226,  1228,  1230,  1232,  1234,
    1236,  1238,  1240,  1242,  1244,  1246,  1248,  1250,  1252,  1254,
    1256,  1258,  1260,  1262,  1264,  1266,  1268,  1270,  1272,  1274,
    1276,  1278,  1280,  1282,  1284,  1286,  1288,  1290,  1292,  1294,
    1296,  1298,  1300
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     183,     0,    -1,   184,    -1,     1,    -1,   185,    -1,   184,
     185,    -1,   186,    50,    -1,   186,   171,    -1,   186,   171,
      50,    -1,   171,    -1,   171,    50,    -1,    56,   186,   171,
      -1,   187,    -1,   246,   187,    -1,   188,    -1,   237,    -1,
     242,    -1,   238,    -1,   239,    -1,   240,    -1,   189,    -1,
     307,    -1,   272,    -1,   208,    -1,   273,    -1,   311,    -1,
     312,    -1,   220,    -1,   221,    -1,   222,    -1,   223,    -1,
     224,    -1,   225,    -1,   226,    -1,   190,    -1,   199,    -1,
     241,    -1,   313,    -1,   260,   204,   203,    -1,   201,   188,
      -1,   201,   237,    -1,   201,   239,    -1,   201,   240,    -1,
     201,   238,    -1,   201,   241,    -1,   191,    -1,   200,    -1,
      14,   192,    -1,    15,    -1,    16,    -1,    17,    -1,    18,
      -1,    -1,    19,    -1,    64,    -1,    21,    64,    -1,   188,
      -1,   237,    -1,   238,    -1,   240,    -1,   239,    -1,   313,
      -1,   226,    -1,   241,    -1,   172,    85,    -1,   194,   173,
     172,    85,    -1,   172,    85,    -1,   195,    -1,   193,    -1,
      29,   318,    27,    -1,    30,   318,    -1,    30,   318,    31,
      -1,   197,   196,    -1,   198,   194,    -1,    15,    29,   318,
      -1,    32,    29,   318,    -1,    22,    -1,    23,    -1,    24,
      -1,    25,    -1,    53,    -1,    26,    -1,    54,    -1,    55,
      -1,   202,    -1,   202,    85,    -1,    86,    -1,    87,    -1,
      88,    -1,    -1,    27,   233,    -1,    -1,   230,    -1,     5,
      80,    -1,     5,    80,   204,    27,   233,    -1,     5,    80,
     230,    -1,   161,    -1,   161,    71,   318,    -1,    12,    81,
      -1,    12,    81,   230,    -1,   205,    -1,   206,    -1,   207,
      -1,   218,    -1,   219,    -1,   209,    -1,   217,    -1,   216,
      -1,   159,    -1,   156,    -1,   214,    -1,   215,    -1,   210,
      -1,   211,    -1,   160,   227,    -1,   202,   227,    -1,   162,
      27,    85,    -1,   162,    27,    85,    27,    85,    -1,   163,
      27,    85,    -1,   163,    27,    85,    27,    85,    -1,    -1,
     133,    85,    -1,    -1,    27,    85,    -1,   157,   213,   212,
      -1,   158,   213,   212,    -1,    11,    19,    20,   213,   212,
      -1,   155,    -1,   153,    -1,   153,    27,    85,    -1,   153,
      72,   154,   174,    85,    -1,   153,    27,    85,    72,   154,
     174,    85,    -1,    82,    -1,    83,   174,   117,    -1,    92,
      -1,    93,    -1,    94,    -1,    95,    -1,    96,    -1,    97,
      -1,    13,   227,   175,   228,   176,    -1,   318,    -1,   318,
     177,   318,    -1,   318,   177,   318,   177,   318,    -1,    -1,
     229,    -1,   228,   173,   229,    -1,    85,    -1,   117,    -1,
     100,    -1,   172,    85,    -1,   172,   172,    85,    -1,    49,
      -1,   231,    -1,   230,   231,    -1,   232,    -1,   175,   176,
      -1,   175,   188,   176,    -1,   175,   230,   176,    -1,   315,
      -1,   234,    -1,   188,    -1,    -1,   175,   236,   176,    -1,
      85,    -1,   236,   173,    85,    -1,   263,   316,   314,    -1,
     263,   316,   314,   235,   234,    -1,   265,   233,    -1,   261,
     233,    -1,   262,   271,    27,   233,    -1,   266,   316,    -1,
      12,   243,    -1,   244,   173,   243,    -1,   244,    -1,   172,
      85,   174,   245,    -1,   172,   172,    98,    85,   174,   245,
      -1,    98,    85,   174,   245,    -1,   172,   172,    85,   174,
     245,    -1,   172,   172,    99,    85,   174,   245,    -1,    99,
      85,   174,   245,    -1,    85,   174,   245,    -1,    85,    -1,
     117,    -1,   100,    -1,   247,    -1,   247,   246,    -1,    40,
     248,    41,    -1,    40,   105,   256,   255,    41,    -1,    40,
     102,   174,   259,   255,    41,    -1,    40,   113,   174,   259,
     255,    41,    -1,    40,   101,   174,   259,   255,    41,    -1,
      40,   103,   174,   259,   255,    41,    -1,    40,   104,   174,
     259,   255,    41,    -1,    40,    84,    85,   174,   258,   255,
      41,    -1,    40,   108,   174,   257,   255,    41,    -1,    40,
     109,   174,   257,   255,    41,    -1,    40,   106,   174,   259,
     255,    41,    -1,    40,   107,   174,   259,   255,    41,    -1,
      40,   110,   111,   174,   178,   250,   179,    41,    -1,    40,
     110,   112,   174,   178,   252,   179,    41,    -1,    -1,   248,
     249,    -1,    42,    85,    -1,    43,    85,    -1,    85,    -1,
     251,   173,   250,    -1,   251,    -1,   101,   175,   259,   176,
      -1,   113,   175,   259,   176,    -1,   114,   175,   176,    -1,
     114,   175,   115,   174,   259,   176,    -1,   116,   175,   253,
     176,    -1,    68,   175,   257,   176,    -1,    68,   175,   257,
     180,   257,   176,    -1,   254,   173,   253,    -1,   254,    -1,
      85,   174,   259,    -1,    -1,   255,   173,   256,    -1,   101,
     174,   259,    -1,   102,   174,   259,    -1,   113,   174,   259,
      -1,   103,   174,   259,    -1,   104,   174,   259,    -1,   108,
     174,   257,    -1,   109,   174,   257,    -1,   106,   174,   259,
      -1,   107,   174,   259,    -1,    85,   177,    85,   174,   258,
      -1,    85,   174,   258,    -1,    -1,   259,    -1,    -1,   259,
      -1,    85,    -1,    89,    -1,     5,    -1,    33,   267,    -1,
       8,    -1,    34,   267,    -1,     6,    -1,    35,   267,    -1,
       7,   264,    -1,    36,   267,   264,    -1,    -1,   128,    -1,
     128,    52,    -1,     9,    -1,    37,   267,    -1,    10,    -1,
      38,   267,    -1,   268,    39,    -1,    -1,   269,   268,    -1,
      44,   175,   117,   176,    -1,   117,    -1,    45,   175,   270,
     176,    -1,    64,   175,    85,    85,   176,    -1,    85,    -1,
      85,   175,   117,   176,    -1,    85,   175,    85,   176,    -1,
      85,   175,    85,    85,   176,    -1,    -1,    46,    -1,    47,
      -1,    48,    -1,    -1,    69,    -1,    11,   306,    66,    -1,
      11,   306,    67,    -1,    11,    68,    -1,   277,    -1,   279,
      -1,   280,    -1,   283,    -1,   281,    -1,   285,    -1,   286,
      -1,   287,    -1,   288,    -1,   290,    -1,   291,    -1,   292,
      -1,   293,    -1,   294,    -1,   296,    -1,   297,    -1,   299,
      -1,   300,    -1,   301,    -1,   302,    -1,   303,    -1,   304,
      -1,   305,    -1,    -1,   121,   117,    -1,   121,   117,   173,
     117,    -1,   121,   117,   122,   117,    -1,    -1,   133,    85,
      -1,    -1,   133,    85,    -1,   118,   278,    -1,   119,    -1,
     120,    -1,   120,   117,   274,    -1,   130,   275,    -1,   130,
     131,   275,    -1,   130,   131,   132,   275,    -1,   123,    -1,
     125,   282,    -1,    -1,   126,    85,    -1,   126,   133,    85,
      -1,   126,   128,    -1,   133,    85,    -1,   124,   284,    -1,
      -1,   126,   275,    -1,   126,   117,   275,    -1,   129,   275,
      -1,   129,   117,   275,    -1,   127,   275,    -1,   127,   117,
     275,    -1,   127,   128,   275,    -1,   127,   128,   117,   275,
      -1,   134,    -1,   134,   117,    -1,   135,   275,    -1,   135,
     152,   275,    -1,   136,   275,    -1,   137,   289,    -1,    -1,
      85,    -1,   128,    -1,   128,    85,    -1,   138,   276,    -1,
     138,   140,   276,    -1,   138,   139,    -1,   141,    -1,   141,
      85,    -1,   142,    -1,   142,   143,    -1,   144,   274,    -1,
     144,   117,    -1,   145,   295,    -1,    -1,   117,    -1,   117,
     173,   117,    -1,   117,   173,   117,   173,    85,    -1,   146,
     275,    -1,   146,   147,   275,    -1,   148,   298,    -1,    -1,
     117,    -1,   117,   117,    -1,   149,   150,    -1,   149,   151,
      -1,   149,   152,    -1,   164,    12,    85,   174,    -1,   164,
      12,    85,   174,    85,    -1,   164,    12,    85,   174,   117,
      -1,   165,     6,    85,    -1,   166,    -1,   167,   117,    -1,
     167,   117,   117,    -1,   168,    85,   117,    -1,   168,    85,
      -1,   169,   117,    -1,   169,   120,   117,    -1,   169,   170,
     117,    -1,    -1,    70,   181,    -1,    56,    -1,    57,    58,
     308,    -1,    65,    56,    85,    -1,    65,    57,    85,    -1,
      -1,   309,    -1,   310,    -1,   309,   173,   310,    -1,    59,
      60,    -1,    61,    62,    63,    -1,    90,    85,    -1,    91,
      85,    -1,    85,    -1,    -1,   139,    85,    -1,   139,   175,
      85,   176,    -1,   316,   314,    -1,   318,    -1,   318,   177,
     318,    -1,   318,   318,    -1,   318,   177,   318,   318,    -1,
     318,    71,   318,    -1,   318,   177,   318,    71,   318,    -1,
      57,    -1,    65,    -1,    56,    -1,    58,    -1,    62,    -1,
      67,    -1,    66,    -1,    70,    -1,    69,    -1,    68,    -1,
     119,    -1,   120,    -1,   122,    -1,   126,    -1,   127,    -1,
     129,    -1,   131,    -1,   132,    -1,   143,    -1,   147,    -1,
     151,    -1,   152,    -1,   170,    -1,   101,    -1,   102,    -1,
     103,    -1,   104,    -1,   154,    -1,   105,    -1,   106,    -1,
     107,    -1,   108,    -1,   109,    -1,   110,    -1,   111,    -1,
     112,    -1,   113,    -1,   115,    -1,   114,    -1,   116,    -1,
      64,    -1,    53,    -1,    54,    -1,    55,    -1,    85,    -1,
     317,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   336,   336,   337,   339,   340,   342,   343,   344,   345,
     346,   347,   349,   350,   352,   353,   354,   355,   356,   357,
     358,   359,   360,   361,   362,   363,   364,   365,   366,   367,
     368,   369,   370,   371,   372,   373,   374,   375,   377,   379,
     380,   381,   382,   383,   384,   386,   387,   389,   390,   391,
     392,   393,   395,   396,   397,   398,   400,   401,   402,   403,
     404,   405,   406,   407,   409,   416,   424,   432,   433,   436,
     442,   447,   453,   456,   459,   464,   470,   471,   472,   473,
     474,   475,   476,   477,   479,   480,   482,   483,   484,   486,
     487,   489,   490,   492,   493,   494,   496,   497,   499,   500,
     502,   503,   504,   505,   506,   507,   509,   510,   511,   512,
     513,   514,   515,   516,   517,   518,   524,   529,   536,   541,
     548,   549,   551,   552,   554,   558,   563,   568,   570,   571,
     576,   581,   588,   591,   597,   598,   599,   600,   601,   602,
     605,   607,   611,   616,   624,   627,   632,   637,   642,   647,
     652,   657,   662,   670,   671,   673,   675,   676,   677,   679,
     680,   682,   684,   685,   687,   688,   690,   694,   695,   696,
     697,   698,   703,   705,   706,   708,   712,   716,   720,   724,
     728,   732,   736,   741,   746,   752,   753,   755,   756,   757,
     758,   759,   760,   761,   762,   768,   769,   770,   771,   772,
     773,   775,   776,   778,   779,   780,   782,   783,   785,   790,
     795,   796,   797,   799,   800,   802,   803,   805,   813,   814,
     816,   817,   818,   819,   820,   821,   822,   823,   824,   825,
     831,   833,   834,   836,   837,   839,   840,   842,   843,   844,
     845,   846,   847,   848,   849,   851,   852,   853,   855,   856,
     857,   858,   859,   860,   861,   863,   864,   865,   866,   871,
     872,   873,   874,   876,   877,   878,   879,   881,   882,   885,
     886,   887,   890,   891,   892,   893,   894,   895,   896,   897,
     898,   899,   900,   901,   902,   903,   904,   905,   906,   907,
     908,   909,   910,   911,   912,   917,   919,   923,   928,   936,
     937,   941,   942,   945,   947,   948,   949,   953,   954,   955,
     959,   961,   963,   964,   965,   966,   967,   970,   972,   973,
     974,   975,   976,   977,   978,   979,   980,   984,   985,   989,
     990,   995,   998,  1000,  1001,  1002,  1003,  1007,  1008,  1009,
    1013,  1014,  1018,  1019,  1023,  1024,  1027,  1029,  1030,  1031,
    1032,  1036,  1037,  1040,  1042,  1043,  1044,  1048,  1049,  1050,
    1054,  1055,  1056,  1060,  1064,  1068,  1069,  1073,  1074,  1078,
    1079,  1080,  1083,  1084,  1087,  1091,  1092,  1093,  1095,  1096,
    1098,  1099,  1102,  1103,  1106,  1112,  1115,  1117,  1118,  1119,
    1121,  1126,  1129,  1133,  1137,  1142,  1146,  1152,  1153,  1154,
    1155,  1156,  1157,  1158,  1159,  1160,  1161,  1162,  1163,  1164,
    1165,  1166,  1167,  1168,  1169,  1170,  1171,  1172,  1173,  1174,
    1175,  1176,  1177,  1178,  1179,  1180,  1181,  1182,  1183,  1184,
    1185,  1186,  1187,  1188,  1189,  1190,  1191,  1192,  1193,  1194,
    1195,  1197,  1198
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "DUMMY_WHERE_CLAUSE",
  "DUMMY_INSERT_CLAUSE", "SELECT", "DELETE", "INSERT", "UPDATE", "REPLACE",
  "MERGE", "SHOW", "SET", "CALL", "CREATE", "DROP", "ALTER", "TRUNCATE",
  "RENAME", "TABLE", "STATUS", "UNIQUE", "GRANT", "REVOKE", "ANALYZE",
  "PURGE", "COMMENT", "FROM", "DUAL", "PREPARE", "EXECUTE", "USING",
  "DEALLOCATE", "SELECT_HINT_BEGIN", "UPDATE_HINT_BEGIN",
  "DELETE_HINT_BEGIN", "INSERT_HINT_BEGIN", "REPLACE_HINT_BEGIN",
  "MERGE_HINT_BEGIN", "HINT_END", "COMMENT_BEGIN", "COMMENT_END",
  "ROUTE_TABLE", "ROUTE_PART_KEY", "QUERY_TIMEOUT", "READ_CONSISTENCY",
  "WEAK", "STRONG", "FROZEN", "PLACE_HOLDER", "END_P", "ERROR", "WHEN",
  "FLASHBACK", "AUDIT", "NOAUDIT", "BEGI", "START", "TRANSACTION", "READ",
  "ONLY", "WITH", "CONSISTENT", "SNAPSHOT", "INDEX", "XA", "WARNINGS",
  "ERRORS", "TRACE", "QUICK", "COUNT", "AS", "WHERE", "VALUES", "ORDER",
  "GROUP", "HAVING", "INTO", "UNION", "FOR", "TX_READ_ONLY",
  "AUTOCOMMIT_0", "SELECT_OBPROXY_ROUTE_ADDR", "SET_OBPROXY_ROUTE_ADDR",
  "NAME_OB_DOT", "NAME_OB", "EXPLAIN", "DESC", "DESCRIBE", "NAME_STR",
  "USE", "HELP", "SET_NAMES", "SET_CHARSET", "SET_PASSWORD", "SET_DEFAULT",
  "SET_OB_READ_CONSISTENCY", "SET_TX_READ_ONLY", "GLOBAL", "SESSION",
  "NUMBER_VAL", "GROUP_ID", "TABLE_ID", "ELASTIC_ID", "TESTLOAD",
  "ODP_COMMENT", "TNT_ID", "DISASTER_STATUS", "TRACE_ID", "RPC_ID",
  "DBP_COMMENT", "ROUTE_TAG", "SYS_TAG", "TABLE_NAME", "SCAN_ALL",
  "PARALL", "SHARD_KEY", "INT_NUM", "SHOW_PROXYNET", "THREAD",
  "CONNECTION", "LIMIT", "OFFSET", "SHOW_PROCESSLIST", "SHOW_PROXYSESSION",
  "SHOW_GLOBALSESSION", "ATTRIBUTE", "VARIABLES", "ALL", "STAT",
  "SHOW_PROXYCONFIG", "DIFF", "USER", "LIKE", "SHOW_PROXYSM",
  "SHOW_PROXYCLUSTER", "SHOW_PROXYRESOURCE", "SHOW_PROXYCONGESTION",
  "SHOW_PROXYROUTE", "PARTITION", "ROUTINE", "SHOW_PROXYVIP",
  "SHOW_PROXYMEMORY", "OBJPOOL", "SHOW_SQLAUDIT", "SHOW_WARNLOG",
  "SHOW_PROXYSTAT", "REFRESH", "SHOW_PROXYTRACE", "SHOW_PROXYINFO",
  "BINARY", "UPGRADE", "IDC", "SHOW_TOPOLOGY", "GROUP_NAME",
  "SHOW_DB_VERSION", "SHOW_DATABASES", "SHOW_TABLES", "SHOW_FULL_TABLES",
  "SELECT_DATABASE", "SHOW_CREATE_TABLE", "SELECT_PROXY_VERSION",
  "SHOW_COLUMNS", "SHOW_INDEX", "ALTER_PROXYCONFIG", "ALTER_PROXYRESOURCE",
  "PING_PROXY", "KILL_PROXYSESSION", "KILL_GLOBALSESSION", "KILL", "QUERY",
  "';'", "'@'", "','", "'='", "'('", "')'", "'.'", "'{'", "'}'", "'#'",
  "'*'", "$accept", "root", "sql_stmts", "sql_stmt", "comment_stmt",
  "stmt", "select_stmt", "explain_stmt", "ddl_stmt", "mysql_ddl_stmt",
  "create_dll_expr", "text_ps_from_stmt", "text_ps_execute_using_var_list",
  "text_ps_prepare_var_list", "text_ps_prepare_args_stmt",
  "text_ps_prepare_stmt", "text_ps_execute_stmt", "text_ps_stmt",
  "oracle_ddl_stmt", "explain_or_desc_stmt", "explain_or_desc", "opt_from",
  "select_expr_list", "select_tx_read_only_stmt",
  "select_proxy_version_stmt", "set_autocommit_0_stmt", "hooked_stmt",
  "shard_special_stmt", "show_columns_stmt", "show_index_stmt",
  "opt_show_like", "opt_show_from", "show_tables_stmt",
  "show_table_status_stmt", "show_db_version_stmt", "show_es_id_stmt",
  "select_obproxy_route_addr_stmt", "set_obproxy_route_addr_stmt",
  "set_names_stmt", "set_charset_stmt", "set_password_stmt",
  "set_default_stmt", "set_ob_read_consistency_stmt",
  "set_tx_read_only_stmt", "call_stmt", "routine_name_stmt",
  "call_expr_list", "call_expr", "expr_list", "expr", "clause", "fromlist",
  "sub_query", "opt_column_list", "column_list", "insert_stmt",
  "replace_stmt", "update_stmt", "delete_stmt", "merge_stmt", "set_stmt",
  "set_expr_list", "set_expr", "set_var_value", "comment_expr_list",
  "comment_expr", "comment_list", "comment", "dbp_comment_list",
  "dbp_comment", "dbp_sys_comment", "dbp_kv_comment_list",
  "dbp_kv_comment", "odp_comment_list", "odp_comment",
  "tracer_right_string_val", "name_right_string_val", "right_string_val",
  "select_with_opt_hint", "update_with_opt_hint", "delete_with_opt_hint",
  "insert_with_opt_hint", "insert_all_when", "replace_with_opt_hint",
  "merge_with_opt_hint", "hint_list_with_end", "hint_list", "hint",
  "opt_read_consistency", "opt_quick", "show_stmt", "icmd_stmt",
  "opt_limit", "opt_like", "opt_large_like", "show_proxynet",
  "opt_show_net", "show_proxyconfig", "show_processlist",
  "show_globalsession", "opt_show_global_session", "show_proxysession",
  "opt_show_session", "show_proxysm", "show_proxycluster",
  "show_proxyresource", "show_proxycongestion", "opt_show_congestion",
  "show_proxyroute", "show_proxyvip", "show_proxymemory", "show_sqlaudit",
  "show_warnlog", "opt_show_warnlog", "show_proxystat", "show_proxytrace",
  "opt_show_trace", "show_proxyinfo", "alter_proxyconfig",
  "alter_proxyresource", "ping_proxy", "kill_proxysession",
  "kill_globalsession", "kill_mysql", "opt_count", "begin_stmt",
  "opt_transaction_characteristics", "transaction_characteristics",
  "transaction_characteristic", "use_db_stmt", "help_stmt", "other_stmt",
  "partition_factor", "table_references", "table_factor",
  "non_reserved_keyword", "var_name", 0
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
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,   301,   302,   303,   304,
     305,   306,   307,   308,   309,   310,   311,   312,   313,   314,
     315,   316,   317,   318,   319,   320,   321,   322,   323,   324,
     325,   326,   327,   328,   329,   330,   331,   332,   333,   334,
     335,   336,   337,   338,   339,   340,   341,   342,   343,   344,
     345,   346,   347,   348,   349,   350,   351,   352,   353,   354,
     355,   356,   357,   358,   359,   360,   361,   362,   363,   364,
     365,   366,   367,   368,   369,   370,   371,   372,   373,   374,
     375,   376,   377,   378,   379,   380,   381,   382,   383,   384,
     385,   386,   387,   388,   389,   390,   391,   392,   393,   394,
     395,   396,   397,   398,   399,   400,   401,   402,   403,   404,
     405,   406,   407,   408,   409,   410,   411,   412,   413,   414,
     415,   416,   417,   418,   419,   420,   421,   422,   423,   424,
     425,    59,    64,    44,    61,    40,    41,    46,   123,   125,
      35,    42
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   182,   183,   183,   184,   184,   185,   185,   185,   185,
     185,   185,   186,   186,   187,   187,   187,   187,   187,   187,
     187,   187,   187,   187,   187,   187,   187,   187,   187,   187,
     187,   187,   187,   187,   187,   187,   187,   187,   188,   189,
     189,   189,   189,   189,   189,   190,   190,   191,   191,   191,
     191,   191,   192,   192,   192,   192,   193,   193,   193,   193,
     193,   193,   193,   193,   194,   194,   195,   196,   196,   197,
     198,   198,   199,   199,   199,   199,   200,   200,   200,   200,
     200,   200,   200,   200,   201,   201,   202,   202,   202,   203,
     203,   204,   204,   205,   205,   205,   206,   206,   207,   207,
     208,   208,   208,   208,   208,   208,   209,   209,   209,   209,
     209,   209,   209,   209,   209,   209,   210,   210,   211,   211,
     212,   212,   213,   213,   214,   214,   215,   216,   217,   217,
     217,   217,   218,   219,   220,   221,   222,   223,   224,   225,
     226,   227,   227,   227,   228,   228,   228,   229,   229,   229,
     229,   229,   229,   230,   230,   231,   232,   232,   232,   233,
     233,   234,   235,   235,   236,   236,   237,   237,   238,   239,
     240,   241,   242,   243,   243,   244,   244,   244,   244,   244,
     244,   244,   245,   245,   245,   246,   246,   247,   247,   247,
     247,   247,   247,   247,   247,   247,   247,   247,   247,   247,
     247,   248,   248,   249,   249,   249,   250,   250,   251,   251,
     251,   251,   251,   252,   252,   253,   253,   254,   255,   255,
     256,   256,   256,   256,   256,   256,   256,   256,   256,   256,
     256,   257,   257,   258,   258,   259,   259,   260,   260,   261,
     261,   262,   262,   263,   263,   264,   264,   264,   265,   265,
     266,   266,   267,   268,   268,   269,   269,   269,   269,   269,
     269,   269,   269,   270,   270,   270,   270,   271,   271,   272,
     272,   272,   273,   273,   273,   273,   273,   273,   273,   273,
     273,   273,   273,   273,   273,   273,   273,   273,   273,   273,
     273,   273,   273,   273,   273,   274,   274,   274,   274,   275,
     275,   276,   276,   277,   278,   278,   278,   279,   279,   279,
     280,   281,   282,   282,   282,   282,   282,   283,   284,   284,
     284,   284,   284,   284,   284,   284,   284,   285,   285,   286,
     286,   287,   288,   289,   289,   289,   289,   290,   290,   290,
     291,   291,   292,   292,   293,   293,   294,   295,   295,   295,
     295,   296,   296,   297,   298,   298,   298,   299,   299,   299,
     300,   300,   300,   301,   302,   303,   303,   304,   304,   305,
     305,   305,   306,   306,   307,   307,   307,   307,   308,   308,
     309,   309,   310,   310,   311,   312,   313,   314,   314,   314,
     315,   316,   316,   316,   316,   316,   316,   317,   317,   317,
     317,   317,   317,   317,   317,   317,   317,   317,   317,   317,
     317,   317,   317,   317,   317,   317,   317,   317,   317,   317,
     317,   317,   317,   317,   317,   317,   317,   317,   317,   317,
     317,   317,   317,   317,   317,   317,   317,   317,   317,   317,
     317,   318,   318
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     1,     1,     1,     2,     2,     2,     3,     1,
       2,     3,     1,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     3,     2,
       2,     2,     2,     2,     2,     1,     1,     2,     1,     1,
       1,     1,     0,     1,     1,     2,     1,     1,     1,     1,
       1,     1,     1,     1,     2,     4,     2,     1,     1,     3,
       2,     3,     2,     2,     3,     3,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     2,     1,     1,     1,     0,
       2,     0,     1,     2,     5,     3,     1,     3,     2,     3,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     2,     2,     3,     5,     3,     5,
       0,     2,     0,     2,     3,     3,     5,     1,     1,     3,
       5,     7,     1,     3,     1,     1,     1,     1,     1,     1,
       5,     1,     3,     5,     0,     1,     3,     1,     1,     1,
       2,     3,     1,     1,     2,     1,     2,     3,     3,     1,
       1,     1,     0,     3,     1,     3,     3,     5,     2,     2,
       4,     2,     2,     3,     1,     4,     6,     4,     5,     6,
       4,     3,     1,     1,     1,     1,     2,     3,     5,     6,
       6,     6,     6,     6,     7,     6,     6,     6,     6,     8,
       8,     0,     2,     2,     2,     1,     3,     1,     4,     4,
       3,     6,     4,     4,     6,     3,     1,     3,     0,     3,
       3,     3,     3,     3,     3,     3,     3,     3,     3,     5,
       3,     0,     1,     0,     1,     1,     1,     1,     2,     1,
       2,     1,     2,     2,     3,     0,     1,     2,     1,     2,
       1,     2,     2,     0,     2,     4,     1,     4,     5,     1,
       4,     4,     5,     0,     1,     1,     1,     0,     1,     3,
       3,     2,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     0,     2,     4,     4,     0,
       2,     0,     2,     2,     1,     1,     3,     2,     3,     4,
       1,     2,     0,     2,     3,     2,     2,     2,     0,     2,
       3,     2,     3,     2,     3,     3,     4,     1,     2,     2,
       3,     2,     2,     0,     1,     1,     2,     2,     3,     2,
       1,     2,     1,     2,     2,     2,     2,     0,     1,     3,
       5,     2,     3,     2,     0,     1,     2,     2,     2,     2,
       4,     5,     5,     3,     1,     2,     3,     3,     2,     2,
       3,     3,     0,     2,     1,     3,     3,     3,     0,     1,
       1,     3,     2,     3,     2,     2,     1,     0,     2,     4,
       2,     1,     3,     2,     4,     3,     5,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
       0,     3,   237,   241,   245,   239,   248,   250,   372,     0,
       0,    52,    48,    49,    50,    51,    76,    77,    78,    79,
      81,     0,     0,     0,   253,   253,   253,   253,   253,   253,
     201,    80,    82,    83,   374,     0,     0,   132,     0,   386,
      86,    87,    88,     0,     0,   134,   135,   136,   137,   138,
     139,     0,   310,   318,   312,   299,   327,   299,   299,   333,
     301,   340,   342,   295,   347,   299,   354,     0,   128,   127,
     109,   122,   122,   108,     0,    96,     0,     0,     0,     0,
     364,     0,     0,     0,     9,     0,     2,     4,     0,    12,
      14,    20,    34,    45,     0,     0,    35,    46,     0,    84,
     100,   101,   102,    23,   105,   112,   113,   110,   111,   107,
     106,   103,   104,    27,    28,    29,    30,    31,    32,    33,
      15,    17,    18,    19,    36,    16,     0,   185,    91,     0,
     267,     0,     0,     0,    22,    24,   272,   273,   274,   276,
     275,   277,   278,   279,   280,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,    21,
      25,    26,    37,    93,   246,   243,     0,   271,     0,     0,
      98,     0,     0,     0,     0,   172,   174,   438,   439,   440,
     399,   397,   400,   401,   437,   398,   403,   402,   406,   405,
     404,   441,   420,   421,   422,   423,   425,   426,   427,   428,
     429,   430,   431,   432,   433,   435,   434,   436,   407,   408,
     409,   410,   411,   412,   413,   414,   415,   416,   417,   418,
     424,   419,     0,   442,   141,    53,     0,    54,    47,     0,
       0,    70,     0,     0,     0,     0,   259,   256,   238,     0,
     253,   240,   242,   245,   249,   251,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   374,
       0,   378,     0,     0,     0,   384,   385,   304,   305,   303,
     299,   299,   299,   317,     0,     0,   311,   299,     0,   307,
     328,   299,   329,   331,   334,   335,   332,     0,   339,   301,
     337,   341,   343,   345,     0,   344,   348,   346,   299,   351,
     355,   353,   357,   358,   359,     0,     0,     0,   120,   120,
     114,     0,     0,     0,     0,     0,   365,   368,   369,     0,
       0,    10,     1,     5,     6,     7,   237,     0,    56,    68,
      67,    72,    62,    57,    58,    60,    59,    63,    61,     0,
      73,    39,    40,    43,    41,    42,    44,    85,   115,    13,
     186,     0,    89,    92,   153,   155,   161,   169,   160,   159,
     387,   391,   268,     0,   387,   168,   171,     0,    95,   247,
     122,   373,   269,   270,    99,     0,     0,     0,     0,     0,
       0,   144,     0,    55,    74,    69,    71,    75,     0,   263,
       0,     0,   252,   254,   244,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     218,     0,     0,   231,   231,     0,     0,     0,   187,     0,
       0,   205,   202,    11,     0,     0,   375,   379,   380,   376,
     377,   133,   295,   299,   319,   299,   299,   323,   299,   321,
     313,   315,     0,   316,   299,   308,   300,   330,   336,   302,
     338,   296,     0,   352,   356,   129,     0,   123,     0,   124,
     125,    97,   116,   118,     0,   363,   366,   367,   370,   371,
       8,    66,    64,     0,   156,     0,     0,     0,    38,   154,
       0,   390,     0,     0,   393,     0,   162,     0,   120,   182,
     184,   183,   181,     0,     0,     0,     0,     0,     0,   173,
     152,   147,   149,   148,     0,     0,   145,   142,     0,   264,
     265,   266,     0,     0,     0,     0,   233,   235,   236,   218,
     218,   218,   218,   233,     0,     0,     0,     0,     0,     0,
       0,   231,   231,     0,     0,   218,   218,   218,   232,   218,
       0,     0,   218,   203,   204,   382,     0,     0,   306,   320,
     324,   299,   325,   322,   314,   309,     0,     0,   349,     0,
       0,   121,     0,     0,   360,     0,   157,   158,    90,   388,
       0,   395,   392,   170,     0,     0,    94,   126,   177,   180,
     175,     0,     0,     0,   150,     0,     0,   140,     0,   255,
     257,     0,     0,   261,   260,   218,   234,     0,     0,     0,
       0,   230,     0,   220,   221,   223,   224,   227,   228,   225,
     226,   222,   188,     0,     0,     0,     0,     0,     0,     0,
       0,   383,   381,   326,   298,   297,     0,     0,   130,   117,
     119,   361,   362,    65,     0,     0,   394,   164,     0,   167,
     178,     0,     0,   151,   146,   143,   258,   262,     0,   191,
     189,   192,   193,   233,   219,   197,   198,   195,   196,     0,
       0,     0,     0,     0,   207,     0,     0,   190,   350,     0,
     389,   396,     0,   163,   176,   179,   194,   229,     0,     0,
       0,     0,     0,     0,   231,     0,   131,   165,     0,     0,
       0,   210,     0,     0,   216,   199,   206,     0,   200,   208,
     209,     0,     0,   212,     0,   213,   231,     0,   217,   215,
       0,   211,   214
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    85,    86,    87,    88,    89,   356,    91,    92,    93,
     228,   329,   340,   330,   331,    94,    95,    96,    97,    98,
      99,   478,   352,   100,   101,   102,   103,   104,   105,   106,
     459,   308,   107,   108,   109,   110,   111,   112,   113,   114,
     115,   116,   117,   118,   119,   222,   505,   506,   353,   354,
     355,   357,   358,   575,   638,   120,   121,   122,   123,   124,
     125,   175,   176,   492,   126,   127,   258,   422,   663,   664,
     666,   693,   694,   534,   410,   537,   595,   538,   128,   129,
     130,   131,   165,   132,   133,   238,   239,   240,   512,   363,
     134,   135,   295,   279,   290,   136,   269,   137,   138,   139,
     276,   140,   273,   141,   142,   143,   144,   286,   145,   146,
     147,   148,   149,   297,   150,   151,   301,   152,   153,   154,
     155,   156,   157,   158,   169,   159,   426,   427,   428,   160,
     161,   162,   481,   359,   360,   223,   361
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -485
static const yytype_int16 yypact[] =
{
     392,  -485,    32,  -485,    42,  -485,  -485,  -485,    49,   -12,
    1337,    63,    91,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  1337,  1337,   172,    31,    31,    31,    31,    31,    31,
     145,  -485,  -485,  -485,   830,   151,   161,  -485,   102,  -485,
    -485,  -485,  -485,   138,   210,  -485,  -485,  -485,  -485,  -485,
    -485,   124,  -485,   105,   -33,   169,   190,   -45,   176,    -5,
      57,   231,   174,    34,   203,    35,   204,    56,    50,  -485,
    -485,   291,   291,  -485,  1337,   251,   296,   297,   313,   320,
    -485,   211,   242,   -65,   279,   330,   559,  -485,    17,  -485,
    -485,  -485,  -485,  -485,    23,   159,  -485,  -485,   230,  1444,
    -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,  -485,  -485,  -485,  -485,   995,   292,   158,   680,
     265,  1337,   680,  1337,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,  -485,    -8,   283,  -485,   316,  -485,   156,   193,
     158,   164,   254,   255,   -31,  -485,   168,  -485,  -485,  -485,
    -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,   167,  -485,   166,  -485,   280,  -485,  -485,  1337,
     318,   315,  1337,   175,   177,   178,   179,  -485,  -485,   308,
      31,  -485,  -485,    42,  -485,  -485,   272,   185,   186,   188,
     189,   171,   191,   192,   194,   195,   201,   196,    68,  -485,
     202,   244,   276,   282,   232,  -485,  -485,  -485,   247,  -485,
      26,   -36,    30,  -485,   -32,   286,  -485,   150,   289,  -485,
    -485,   176,  -485,  -485,  -485,   290,  -485,   293,  -485,   243,
    -485,  -485,  -485,  -485,   266,  -485,   208,  -485,   176,  -485,
     267,  -485,  -485,  -485,  -485,   300,   233,   301,   256,   256,
    -485,  1337,   305,   306,   307,   309,   278,   294,  -485,   295,
     302,  -485,  -485,  -485,  -485,   346,  -485,   328,  -485,  -485,
    -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,   335,
     250,  -485,  -485,  -485,  -485,  -485,  -485,    12,  -485,  -485,
    -485,     9,   404,   158,  -485,  -485,  -485,  -485,  -485,  -485,
     298,  1112,  -485,   406,   298,  -485,  -485,   407,    -4,  -485,
     291,  -485,  -485,  -485,   158,    33,   261,   262,   268,   106,
     -20,    14,  1337,  -485,  -485,  -485,  -485,  -485,   321,   223,
     354,   -14,  -485,  -485,  -485,   269,    39,    39,    39,    39,
     -83,   270,   277,   281,   284,   285,   287,   288,   317,   319,
    -485,    39,    39,    39,    39,   322,   323,    39,  -485,   355,
     356,  -485,  -485,  -485,   390,   391,  -485,   303,  -485,  -485,
    -485,  -485,   331,   176,  -485,   176,    60,  -485,   176,  -485,
    -485,  -485,   369,  -485,   176,  -485,  -485,  -485,  -485,  -485,
    -485,   -52,   339,  -485,  -485,   388,   324,  -485,   378,  -485,
    -485,  -485,   437,   438,   325,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,  -485,   299,  -485,   314,   139,   680,  -485,  -485,
     -35,  -485,  1337,  1337,  -485,   680,   -13,   680,   256,  -485,
    -485,  -485,  -485,    33,    33,    33,   326,   381,   382,  -485,
    -485,  -485,  -485,  -485,   -11,   -27,  -485,   304,   327,  -485,
    -485,  -485,   329,   383,   -50,   332,    39,  -485,  -485,  -485,
    -485,  -485,  -485,    39,   384,    39,    39,    39,    39,    39,
      39,    39,    39,    39,   -17,  -485,  -485,  -485,  -485,  -485,
     333,   334,  -485,  -485,  -485,  -485,   429,   244,  -485,  -485,
    -485,   176,  -485,  -485,  -485,  -485,   353,   377,   336,   347,
     417,  -485,   419,   421,    -2,   422,  -485,  -485,  -485,  -485,
     428,  -485,  1230,  -485,   433,    80,  -485,  -485,  -485,  -485,
    -485,    33,   340,   345,  -485,   435,    14,  -485,  1337,  -485,
    -485,   348,   349,  -485,  -485,  -485,  -485,   -16,    -7,    -1,
       0,  -485,   357,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,  -485,   171,     2,     3,     5,     6,    86,   453,
       7,  -485,  -485,  -485,  -485,  -485,   447,   361,  -485,  -485,
    -485,  -485,  -485,  -485,   363,  1337,  -485,  -485,   123,  -485,
    -485,    33,    33,  -485,  -485,  -485,  -485,  -485,     8,  -485,
    -485,  -485,  -485,    39,  -485,  -485,  -485,  -485,  -485,   367,
     368,   371,   405,   344,   413,   412,   365,  -485,  -485,   494,
    -485,  -485,   505,  -485,  -485,  -485,  -485,  -485,    39,    39,
     -70,   513,   560,    86,    39,   561,  -485,  -485,   424,   427,
     430,  -485,   431,   432,   434,  -485,  -485,    18,  -485,  -485,
    -485,    39,    39,  -485,   513,  -485,    39,   441,  -485,  -485,
     442,  -485,  -485
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -485,  -485,  -485,   520,   575,   484,     4,  -485,  -485,  -485,
    -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,   448,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -296,   -62,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,  -485,  -485,   525,    55,  -485,    36,  -148,  -302,
    -485,  -129,    45,  -485,  -485,   126,   147,   163,   187,   200,
    -485,   241,  -485,  -477,   496,  -485,  -485,  -485,   -57,  -485,
    -485,   -77,  -485,  -309,    15,  -387,  -484,  -391,  -485,  -485,
    -485,  -485,   386,  -485,  -485,   264,   393,  -485,  -485,  -485,
    -485,  -485,   198,   -56,   342,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,
    -485,  -485,  -485,  -485,  -485,  -485,  -485,  -485,    85,  -485,
    -485,   540,   271,  -485,   173,  -485,   -10
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -442
static const yytype_int16 yytable[] =
{
     224,   282,   283,   365,    90,   519,   520,   521,   522,   299,
     309,   230,   231,   460,   326,   368,   578,   579,   580,   -91,
     535,   536,   374,   -92,   612,   649,   542,   539,   326,     3,
       4,     5,     6,     7,   650,   592,    10,  -166,    90,   601,
     651,   652,    24,   655,   656,   690,   657,   658,   667,   676,
     569,   479,   318,   440,   378,   319,    24,    25,    26,    27,
      28,    29,  -441,   500,   224,   171,   479,   324,   166,   170,
     556,   514,   479,   171,   584,   233,   234,   305,   172,   173,
     284,   435,   225,   631,   226,   326,   172,   173,   278,   224,
      90,   523,   436,   274,   524,   235,   441,   278,   328,   501,
     275,   442,   341,   515,   640,   320,   691,   281,    39,   418,
     419,   420,   163,    24,   502,   632,   236,   167,   489,   168,
     229,   557,   306,   285,   517,   596,   593,   227,   518,   310,
      90,   503,   596,   490,   603,   604,   605,   606,   607,   608,
     570,   379,   611,   433,   609,   610,   586,   438,   237,   587,
     491,   293,   174,   421,   348,   294,   613,   613,  -166,   278,
     174,   585,   574,   278,   674,   675,   613,   351,   278,   677,
     164,   351,   613,   613,   479,   613,   613,   551,   613,   613,
     613,   613,   298,  -441,   351,   474,   504,   659,   325,  -441,
     287,   496,   577,   278,   705,   327,   288,   289,   706,   660,
     661,   232,   662,   476,   497,   498,   302,   303,   304,   261,
     597,   598,   599,   600,   434,   437,   439,   262,   263,   384,
     333,   445,   387,   265,   342,   447,   614,   615,   616,   246,
     617,   270,   271,   620,   272,   326,     3,     4,     5,     6,
       7,   334,   453,   267,   268,   343,   247,   248,   249,   250,
     251,   252,   253,   254,   255,   256,   400,   335,   257,   372,
     373,   344,   596,    24,    25,    26,    27,    28,    29,   509,
     510,   511,   401,   402,   403,   404,   264,   405,   406,   407,
     408,   336,   444,   278,   409,   345,   648,   688,   689,   241,
     242,   243,   244,   245,   337,   266,   672,   697,   346,   673,
     277,   461,   278,   424,   364,   425,   366,   280,   488,   278,
     707,   708,   415,   416,   351,   567,   291,   292,   307,   710,
     296,   300,   311,   312,   313,   314,   315,   317,   316,   321,
     322,   339,    30,   351,   362,   369,   370,   371,   375,   376,
     377,   380,   381,   382,   383,   385,   386,   392,   568,   431,
     388,   484,   389,   390,   391,   475,   573,   395,   576,   396,
     397,   429,   398,   399,   432,   411,   412,   430,   413,   414,
     417,   443,   507,   423,   446,   448,   287,   549,   449,   550,
     552,   452,   553,   451,   454,   455,   457,   456,   555,   458,
     462,   463,   464,     1,   465,   466,   470,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,   467,   468,   471,    16,    17,    18,    19,    20,   469,
     472,    21,    22,   473,    23,    24,    25,    26,    27,    28,
      29,   477,    30,   485,   487,   493,   494,   480,   508,   513,
     543,   544,   495,   516,   525,    31,    32,    33,    34,    35,
     545,   526,   294,   546,   554,   527,   558,    36,   528,   529,
     559,   530,   531,   561,   562,   563,   582,   583,   591,   602,
     624,   565,   571,   572,    37,    38,   547,    39,    40,    41,
      42,   588,    43,    44,    45,    46,    47,    48,    49,    50,
     566,   532,   621,   533,   625,   623,   540,   541,   560,   564,
     581,   627,   628,   589,   629,   590,   630,   633,   594,   626,
      51,   618,   619,   634,   641,    52,    53,    54,   637,   642,
     643,   665,    55,   682,   646,   647,    56,    57,    58,    59,
      60,   653,   668,    61,    62,   669,    63,    64,    65,   670,
      66,    67,   678,   679,   685,    68,   680,    69,    70,    71,
      72,    73,    74,    75,    76,    77,    78,    79,    80,    81,
      82,    83,   636,    84,     2,     3,     4,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,   645,   686,
     681,    16,    17,    18,    19,    20,   683,   684,    21,    22,
     687,    23,    24,    25,    26,    27,    28,    29,   692,    30,
     699,   695,   698,   700,   701,   702,   323,   704,   703,   260,
     349,   367,    31,    32,    33,    34,    35,   711,   712,   332,
     639,   499,   644,   350,    36,   671,   696,   709,   654,   394,
     548,   450,   622,   393,   338,   486,     0,     0,     0,     0,
       0,    37,    38,     0,    39,    40,    41,    42,     0,    43,
      44,    45,    46,    47,    48,    49,    50,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    51,     0,     0,
       0,     0,    52,    53,    54,   326,     0,     0,     0,    55,
       0,     0,     0,    56,    57,    58,    59,    60,     0,     0,
      61,    62,     0,    63,    64,    65,     0,    66,    67,     0,
       0,     0,    68,    24,    69,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,     0,
      84,     0,     0,   177,   178,   179,   180,   181,   182,     0,
       0,     0,   183,     0,   184,   185,   186,   187,   188,   189,
     190,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   191,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   192,   193,   194,   195,   196,   197,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,     0,     0,   208,
     209,     0,   210,     0,     0,     0,   211,   212,     0,   213,
       0,   214,   215,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   216,     0,     0,     0,   217,     0,     0,
       0,   218,   219,     0,   220,     2,     3,     4,     5,     6,
       7,     8,     9,    10,    11,    12,    13,    14,    15,     0,
     221,     0,    16,    17,    18,    19,    20,     0,     0,    21,
      22,     0,    23,    24,    25,    26,    27,    28,    29,     0,
      30,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    31,    32,    33,   259,    35,     0,     0,
       0,     0,     0,     0,     0,    36,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    37,    38,     0,    39,    40,    41,    42,     0,
      43,    44,    45,    46,    47,    48,    49,    50,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    51,     0,
       0,     0,     0,    52,    53,    54,     0,     0,     0,     0,
      55,     0,     0,     0,    56,    57,    58,    59,    60,     0,
       0,    61,    62,     0,    63,    64,    65,     0,    66,    67,
       0,     0,     0,    68,     0,    69,    70,    71,    72,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
       2,     3,     4,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,     0,     0,     0,    16,    17,    18,
      19,    20,     0,     0,    21,    22,     0,    23,    24,    25,
      26,    27,    28,    29,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    31,    32,
      33,   259,    35,     0,     0,     0,     0,     0,     0,     0,
      36,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    37,    38,     0,
      39,    40,    41,    42,     0,    43,    44,    45,    46,    47,
      48,    49,    50,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    51,     0,     0,     0,     0,    52,    53,
      54,     0,     0,     0,     0,    55,     0,     0,     0,    56,
      57,    58,    59,    60,     0,     0,    61,    62,     0,    63,
      64,    65,     0,    66,    67,     0,     0,     0,    68,     0,
      69,    70,    71,    72,    73,    74,    75,    76,    77,    78,
      79,    80,    81,    82,    83,   177,   178,   179,   180,   181,
     182,     0,     0,     0,   183,     0,   184,   185,   186,   187,
     188,   189,   190,   482,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   191,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   192,   193,   194,   195,   196,   197,   198,
     199,   200,   201,   202,   203,   204,   205,   206,   207,     0,
       0,   208,   209,     0,   210,     0,     0,     0,   211,   212,
       0,   213,     0,   214,   215,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   216,     0,     0,     0,   217,
       0,     0,     0,   218,   219,     0,   220,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   221,   177,   178,   179,   180,   181,   182,   483,
       0,     0,   183,     0,   184,   185,   186,   187,   188,   189,
     190,   635,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   191,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   192,   193,   194,   195,   196,   197,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,     0,     0,   208,
     209,     0,   210,     0,     0,     0,   211,   212,     0,   213,
       0,   214,   215,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   216,     0,     0,     0,   217,     0,     0,
       0,   218,   219,     0,   220,     0,     0,     0,     0,     0,
     177,   178,   179,   180,   181,   182,     0,     0,     0,   183,
     221,   184,   185,   186,   187,   188,   189,   190,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   191,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   192,   193,
     194,   195,   196,   197,   198,   199,   200,   201,   202,   203,
     204,   205,   206,   207,     0,     0,   208,   209,     0,   210,
       0,     0,     0,   211,   212,     0,   213,     0,   214,   215,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     216,     0,     0,     0,   217,     0,     0,     0,   218,   219,
       0,   220,     0,     0,     0,     0,     0,   177,   178,   179,
     180,   181,   182,     0,     0,     0,   183,   221,   184,   185,
     186,   187,   188,   189,   190,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   347,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   192,   193,   194,   195,   196,
     197,   198,   199,   200,   201,   202,   203,   204,   205,   206,
     207,     0,     0,   208,   209,     0,   210,     0,     0,     0,
     211,   212,     0,   213,     0,   214,   215,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   216,     0,     0,
       0,   217,     0,     0,     0,   218,   219,     0,   220,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   221
};

static const yytype_int16 yycheck[] =
{
      10,    57,    58,   132,     0,   396,   397,   398,   399,    65,
      72,    21,    22,   309,     5,   163,   493,   494,   495,    27,
     411,   412,   170,    27,    41,    41,   417,   414,     5,     6,
       7,     8,     9,    10,    41,    85,    13,    50,    34,   523,
      41,    41,    33,    41,    41,   115,    41,    41,    41,    41,
      85,   353,   117,    85,    85,   120,    33,    34,    35,    36,
      37,    38,    50,    49,    74,    85,   368,    50,    19,    81,
     122,    85,   374,    85,    85,    44,    45,    27,    98,    99,
      85,   117,    19,    85,    21,     5,    98,    99,   133,    99,
      86,   174,   128,   126,   177,    64,   128,   133,    94,    85,
     133,   133,    98,   117,   581,   170,   176,   152,    85,    41,
      42,    43,    80,    33,   100,   117,    85,    68,    85,    70,
      29,   173,    72,   128,    85,   516,   176,    64,    89,    74,
     126,   117,   523,   100,   525,   526,   527,   528,   529,   530,
     175,   172,   533,   117,   531,   532,   173,   117,   117,   176,
     117,   117,   172,    85,    99,   121,   173,   173,   171,   133,
     172,   172,   175,   133,   641,   642,   173,   175,   133,   653,
     128,   175,   173,   173,   476,   173,   173,   117,   173,   173,
     173,   173,   147,   171,   175,   176,   172,   101,   171,   177,
     133,    85,   488,   133,   176,   172,   139,   140,   180,   113,
     114,    29,   116,   351,    98,    99,   150,   151,   152,    58,
     519,   520,   521,   522,   270,   271,   272,    56,    57,   229,
      94,   277,   232,    85,    98,   281,   535,   536,   537,    84,
     539,   126,   127,   542,   129,     5,     6,     7,     8,     9,
      10,    94,   298,   119,   120,    98,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,    85,    94,   113,    66,
      67,    98,   653,    33,    34,    35,    36,    37,    38,    46,
      47,    48,   101,   102,   103,   104,   174,   106,   107,   108,
     109,    94,   132,   133,   113,    98,   595,   678,   679,    25,
      26,    27,    28,    29,    94,    85,   173,   684,    98,   176,
     131,   311,   133,    59,   131,    61,   133,   117,   370,   133,
     701,   702,   111,   112,   175,   176,    85,   143,    27,   706,
     117,   117,    71,    27,    27,    12,     6,    85,   117,    50,
       0,   172,    40,   175,    69,    52,    20,   181,   174,    85,
      85,   173,   175,   177,    64,    27,    31,    39,   477,   117,
     175,   361,   175,   175,   175,   351,   485,    85,   487,   174,
     174,    85,   174,   174,   117,   174,   174,    85,   174,   174,
     174,    85,   382,   171,    85,    85,   133,   433,    85,   435,
     436,   173,   438,   117,   117,    85,    85,   154,   444,   133,
      85,    85,    85,     1,    85,   117,    50,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,    16,    17,
      18,   117,   117,    85,    22,    23,    24,    25,    26,   117,
      85,    29,    30,   173,    32,    33,    34,    35,    36,    37,
      38,    27,    40,    27,    27,   174,   174,   139,   117,    85,
      85,    85,   174,   174,   174,    53,    54,    55,    56,    57,
      60,   174,   121,    62,    85,   174,   117,    65,   174,   174,
      72,   174,   174,    85,    27,    27,    85,    85,    85,    85,
     117,   172,   482,   483,    82,    83,   173,    85,    86,    87,
      88,   177,    90,    91,    92,    93,    94,    95,    96,    97,
     176,   174,    63,   174,   117,   551,   174,   174,   174,   174,
     174,   154,    85,   176,    85,   176,    85,    85,   176,   173,
     118,   178,   178,    85,   174,   123,   124,   125,    85,   174,
      85,    68,   130,   179,   176,   176,   134,   135,   136,   137,
     138,   174,    85,   141,   142,   174,   144,   145,   146,   176,
     148,   149,   175,   175,   179,   153,   175,   155,   156,   157,
     158,   159,   160,   161,   162,   163,   164,   165,   166,   167,
     168,   169,   572,   171,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,    16,    17,    18,   588,    85,
     175,    22,    23,    24,    25,    26,   173,   175,    29,    30,
      85,    32,    33,    34,    35,    36,    37,    38,    85,    40,
     176,    41,    41,   176,   174,   174,    86,   173,   176,    34,
     126,   163,    53,    54,    55,    56,    57,   176,   176,    94,
     575,   380,   586,   127,    65,   635,   683,   704,   613,   243,
     432,   289,   547,   240,    94,   364,    -1,    -1,    -1,    -1,
      -1,    82,    83,    -1,    85,    86,    87,    88,    -1,    90,
      91,    92,    93,    94,    95,    96,    97,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   118,    -1,    -1,
      -1,    -1,   123,   124,   125,     5,    -1,    -1,    -1,   130,
      -1,    -1,    -1,   134,   135,   136,   137,   138,    -1,    -1,
     141,   142,    -1,   144,   145,   146,    -1,   148,   149,    -1,
      -1,    -1,   153,    33,   155,   156,   157,   158,   159,   160,
     161,   162,   163,   164,   165,   166,   167,   168,   169,    -1,
     171,    -1,    -1,    53,    54,    55,    56,    57,    58,    -1,
      -1,    -1,    62,    -1,    64,    65,    66,    67,    68,    69,
      70,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    85,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,   114,   115,   116,    -1,    -1,   119,
     120,    -1,   122,    -1,    -1,    -1,   126,   127,    -1,   129,
      -1,   131,   132,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   143,    -1,    -1,    -1,   147,    -1,    -1,
      -1,   151,   152,    -1,   154,     5,     6,     7,     8,     9,
      10,    11,    12,    13,    14,    15,    16,    17,    18,    -1,
     170,    -1,    22,    23,    24,    25,    26,    -1,    -1,    29,
      30,    -1,    32,    33,    34,    35,    36,    37,    38,    -1,
      40,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    53,    54,    55,    56,    57,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    65,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    82,    83,    -1,    85,    86,    87,    88,    -1,
      90,    91,    92,    93,    94,    95,    96,    97,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   118,    -1,
      -1,    -1,    -1,   123,   124,   125,    -1,    -1,    -1,    -1,
     130,    -1,    -1,    -1,   134,   135,   136,   137,   138,    -1,
      -1,   141,   142,    -1,   144,   145,   146,    -1,   148,   149,
      -1,    -1,    -1,   153,    -1,   155,   156,   157,   158,   159,
     160,   161,   162,   163,   164,   165,   166,   167,   168,   169,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    -1,    -1,    -1,    22,    23,    24,
      25,    26,    -1,    -1,    29,    30,    -1,    32,    33,    34,
      35,    36,    37,    38,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    53,    54,
      55,    56,    57,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      65,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,    83,    -1,
      85,    86,    87,    88,    -1,    90,    91,    92,    93,    94,
      95,    96,    97,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   118,    -1,    -1,    -1,    -1,   123,   124,
     125,    -1,    -1,    -1,    -1,   130,    -1,    -1,    -1,   134,
     135,   136,   137,   138,    -1,    -1,   141,   142,    -1,   144,
     145,   146,    -1,   148,   149,    -1,    -1,    -1,   153,    -1,
     155,   156,   157,   158,   159,   160,   161,   162,   163,   164,
     165,   166,   167,   168,   169,    53,    54,    55,    56,    57,
      58,    -1,    -1,    -1,    62,    -1,    64,    65,    66,    67,
      68,    69,    70,    71,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    85,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   101,   102,   103,   104,   105,   106,   107,
     108,   109,   110,   111,   112,   113,   114,   115,   116,    -1,
      -1,   119,   120,    -1,   122,    -1,    -1,    -1,   126,   127,
      -1,   129,    -1,   131,   132,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   143,    -1,    -1,    -1,   147,
      -1,    -1,    -1,   151,   152,    -1,   154,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   170,    53,    54,    55,    56,    57,    58,   177,
      -1,    -1,    62,    -1,    64,    65,    66,    67,    68,    69,
      70,    71,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    85,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,   114,   115,   116,    -1,    -1,   119,
     120,    -1,   122,    -1,    -1,    -1,   126,   127,    -1,   129,
      -1,   131,   132,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   143,    -1,    -1,    -1,   147,    -1,    -1,
      -1,   151,   152,    -1,   154,    -1,    -1,    -1,    -1,    -1,
      53,    54,    55,    56,    57,    58,    -1,    -1,    -1,    62,
     170,    64,    65,    66,    67,    68,    69,    70,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    85,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   101,   102,
     103,   104,   105,   106,   107,   108,   109,   110,   111,   112,
     113,   114,   115,   116,    -1,    -1,   119,   120,    -1,   122,
      -1,    -1,    -1,   126,   127,    -1,   129,    -1,   131,   132,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     143,    -1,    -1,    -1,   147,    -1,    -1,    -1,   151,   152,
      -1,   154,    -1,    -1,    -1,    -1,    -1,    53,    54,    55,
      56,    57,    58,    -1,    -1,    -1,    62,   170,    64,    65,
      66,    67,    68,    69,    70,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    85,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   101,   102,   103,   104,   105,
     106,   107,   108,   109,   110,   111,   112,   113,   114,   115,
     116,    -1,    -1,   119,   120,    -1,   122,    -1,    -1,    -1,
     126,   127,    -1,   129,    -1,   131,   132,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   143,    -1,    -1,
      -1,   147,    -1,    -1,    -1,   151,   152,    -1,   154,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   170
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     1,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    22,    23,    24,    25,
      26,    29,    30,    32,    33,    34,    35,    36,    37,    38,
      40,    53,    54,    55,    56,    57,    65,    82,    83,    85,
      86,    87,    88,    90,    91,    92,    93,    94,    95,    96,
      97,   118,   123,   124,   125,   130,   134,   135,   136,   137,
     138,   141,   142,   144,   145,   146,   148,   149,   153,   155,
     156,   157,   158,   159,   160,   161,   162,   163,   164,   165,
     166,   167,   168,   169,   171,   183,   184,   185,   186,   187,
     188,   189,   190,   191,   197,   198,   199,   200,   201,   202,
     205,   206,   207,   208,   209,   210,   211,   214,   215,   216,
     217,   218,   219,   220,   221,   222,   223,   224,   225,   226,
     237,   238,   239,   240,   241,   242,   246,   247,   260,   261,
     262,   263,   265,   266,   272,   273,   277,   279,   280,   281,
     283,   285,   286,   287,   288,   290,   291,   292,   293,   294,
     296,   297,   299,   300,   301,   302,   303,   304,   305,   307,
     311,   312,   313,    80,   128,   264,    19,    68,    70,   306,
      81,    85,    98,    99,   172,   243,   244,    53,    54,    55,
      56,    57,    58,    62,    64,    65,    66,    67,    68,    69,
      70,    85,   101,   102,   103,   104,   105,   106,   107,   108,
     109,   110,   111,   112,   113,   114,   115,   116,   119,   120,
     122,   126,   127,   129,   131,   132,   143,   147,   151,   152,
     154,   170,   227,   317,   318,    19,    21,    64,   192,    29,
     318,   318,    29,    44,    45,    64,    85,   117,   267,   268,
     269,   267,   267,   267,   267,   267,    84,   101,   102,   103,
     104,   105,   106,   107,   108,   109,   110,   113,   248,    56,
     186,    58,    56,    57,   174,    85,    85,   119,   120,   278,
     126,   127,   129,   284,   126,   133,   282,   131,   133,   275,
     117,   152,   275,   275,    85,   128,   289,   133,   139,   140,
     276,    85,   143,   117,   121,   274,   117,   295,   147,   275,
     117,   298,   150,   151,   152,    27,    72,    27,   213,   213,
     227,    71,    27,    27,    12,     6,   117,    85,   117,   120,
     170,    50,     0,   185,    50,   171,     5,   172,   188,   193,
     195,   196,   226,   237,   238,   239,   240,   241,   313,   172,
     194,   188,   237,   238,   239,   240,   241,    85,   227,   187,
     246,   175,   204,   230,   231,   232,   188,   233,   234,   315,
     316,   318,    69,   271,   316,   233,   316,   204,   230,    52,
      20,   181,    66,    67,   230,   174,    85,    85,    85,   172,
     173,   175,   177,    64,   318,    27,    31,   318,   175,   175,
     175,   175,    39,   268,   264,    85,   174,   174,   174,   174,
      85,   101,   102,   103,   104,   106,   107,   108,   109,   113,
     256,   174,   174,   174,   174,   111,   112,   174,    41,    42,
      43,    85,   249,   171,    59,    61,   308,   309,   310,    85,
      85,   117,   117,   117,   275,   117,   128,   275,   117,   275,
      85,   128,   133,    85,   132,   275,    85,   275,    85,    85,
     276,   117,   173,   275,   117,    85,   154,    85,   133,   212,
     212,   318,    85,    85,    85,    85,   117,   117,   117,   117,
      50,    85,    85,   173,   176,   188,   230,    27,   203,   231,
     139,   314,    71,   177,   318,    27,   314,    27,   213,    85,
     100,   117,   245,   174,   174,   174,    85,    98,    99,   243,
      49,    85,   100,   117,   172,   228,   229,   318,   117,    46,
      47,    48,   270,    85,    85,   117,   174,    85,    89,   259,
     259,   259,   259,   174,   177,   174,   174,   174,   174,   174,
     174,   174,   174,   174,   255,   259,   259,   257,   259,   257,
     174,   174,   259,    85,    85,    60,    62,   173,   274,   275,
     275,   117,   275,   275,    85,   275,   122,   173,   117,    72,
     174,    85,    27,    27,   174,   172,   176,   176,   233,    85,
     175,   318,   318,   233,   175,   235,   233,   212,   245,   245,
     245,   174,    85,    85,    85,   172,   173,   176,   177,   176,
     176,    85,    85,   176,   176,   258,   259,   255,   255,   255,
     255,   258,    85,   259,   259,   259,   259,   259,   259,   257,
     257,   259,    41,   173,   255,   255,   255,   255,   178,   178,
     255,    63,   310,   275,   117,   117,   173,   154,    85,    85,
      85,    85,   117,    85,    85,    71,   318,    85,   236,   234,
     245,   174,   174,    85,   229,   318,   176,   176,   255,    41,
      41,    41,    41,   174,   256,    41,    41,    41,    41,   101,
     113,   114,   116,   250,   251,    68,   252,    41,    85,   174,
     176,   318,   173,   176,   245,   245,    41,   258,   175,   175,
     175,   175,   179,   173,   175,   179,    85,    85,   259,   259,
     115,   176,    85,   253,   254,    41,   250,   257,    41,   176,
     176,   174,   174,   176,   173,   176,   180,   259,   259,   253,
     257,   176,   176
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
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ObProxyParseResult* result)
#else
static void
yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, result)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ObProxyParseResult* result;
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
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ObProxyParseResult* result)
#else
static void
yy_symbol_print (yyoutput, yytype, yyvaluep, yylocationp, result)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ObProxyParseResult* result;
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
yy_reduce_print (YYSTYPE *yyvsp, YYLTYPE *yylsp, int yyrule, ObProxyParseResult* result)
#else
static void
yy_reduce_print (yyvsp, yylsp, yyrule, result)
    YYSTYPE *yyvsp;
    YYLTYPE *yylsp;
    int yyrule;
    ObProxyParseResult* result;
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
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, ObProxyParseResult* result)
#else
static void
yydestruct (yymsg, yytype, yyvaluep, yylocationp, result)
    const char *yymsg;
    int yytype;
    YYSTYPE *yyvaluep;
    YYLTYPE *yylocationp;
    ObProxyParseResult* result;
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
int yyparse (ObProxyParseResult* result);
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
yyparse (ObProxyParseResult* result)
#else
int
yyparse (result)
    ObProxyParseResult* result;
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
        case 2:

    { HANDLE_ACCEPT(); ;}
    break;

  case 3:

    { HANDLE_ERROR_ACCEPT(); ;}
    break;

  case 6:

    { handle_stmt_end(result); HANDLE_ACCEPT(); ;}
    break;

  case 7:

    { handle_stmt_end(result); ;}
    break;

  case 8:

    { handle_stmt_end(result); HANDLE_ACCEPT(); ;}
    break;

  case 9:

    { handle_stmt_end(result); ;}
    break;

  case 10:

    { handle_stmt_end(result); HANDLE_ACCEPT(); ;}
    break;

  case 11:

    { handle_stmt_end(result); ;}
    break;

  case 14:

    {;}
    break;

  case 15:

    {;}
    break;

  case 16:

    {;}
    break;

  case 17:

    {;}
    break;

  case 18:

    {;}
    break;

  case 19:

    {;}
    break;

  case 20:

    {;}
    break;

  case 21:

    {;}
    break;

  case 22:

    {;}
    break;

  case 23:

    {;}
    break;

  case 24:

    {;}
    break;

  case 25:

    {;}
    break;

  case 26:

    {;}
    break;

  case 27:

    {;}
    break;

  case 28:

    {;}
    break;

  case 29:

    {;}
    break;

  case 30:

    {;}
    break;

  case 31:

    {;}
    break;

  case 32:

    {;}
    break;

  case 33:

    {;}
    break;

  case 34:

    {;}
    break;

  case 35:

    {;}
    break;

  case 36:

    {;}
    break;

  case 37:

    { result->cur_stmt_type_ = OBPROXY_T_OTHERS; ;}
    break;

  case 47:

    { result->cur_stmt_type_ = OBPROXY_T_CREATE; ;}
    break;

  case 48:

    { result->cur_stmt_type_ = OBPROXY_T_DROP; ;}
    break;

  case 49:

    { result->cur_stmt_type_ = OBPROXY_T_ALTER; ;}
    break;

  case 50:

    { result->cur_stmt_type_ = OBPROXY_T_TRUNCATE; ;}
    break;

  case 51:

    { result->cur_stmt_type_ = OBPROXY_T_RENAME; ;}
    break;

  case 53:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_TABLE; ;}
    break;

  case 54:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 55:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 56:

    {;}
    break;

  case 57:

    {;}
    break;

  case 58:

    {;}
    break;

  case 59:

    {;}
    break;

  case 60:

    {;}
    break;

  case 61:

    {;}
    break;

  case 62:

    {;}
    break;

  case 63:

    {;}
    break;

  case 64:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(2) - (2)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 65:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(4) - (4)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 66:

    {
                          ObProxyTextPsParseNode *node = NULL;
                          malloc_parse_node(node);
                          node->str_value_ = (yyvsp[(2) - (2)].str);
                          add_text_ps_node(result->text_ps_parse_info_, node);
                        ;}
    break;

  case 69:

    {
                      result->text_ps_inner_stmt_type_ = OBPROXY_T_TEXT_PS_PREPARE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 70:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (2)].str);
                    ;}
    break;

  case 71:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 72:

    {
            ;}
    break;

  case 73:

    {
            ;}
    break;

  case 74:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 75:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 76:

    { result->cur_stmt_type_ = OBPROXY_T_GRANT; ;}
    break;

  case 77:

    { result->cur_stmt_type_ = OBPROXY_T_REVOKE; ;}
    break;

  case 78:

    { result->cur_stmt_type_ = OBPROXY_T_ANALYZE; ;}
    break;

  case 79:

    { result->cur_stmt_type_ = OBPROXY_T_PURGE; ;}
    break;

  case 80:

    { result->cur_stmt_type_ = OBPROXY_T_FLASHBACK; ;}
    break;

  case 81:

    { result->cur_stmt_type_ = OBPROXY_T_COMMENT; ;}
    break;

  case 82:

    { result->cur_stmt_type_ = OBPROXY_T_AUDIT; ;}
    break;

  case 83:

    { result->cur_stmt_type_ = OBPROXY_T_NOAUDIT; ;}
    break;

  case 86:

    {;}
    break;

  case 87:

    {;}
    break;

  case 88:

    {;}
    break;

  case 93:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_TX_RO; ;}
    break;

  case 97:

    { result->col_name_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 98:

    { result->cur_stmt_type_ = OBPROXY_T_SET_AC_0; ;}
    break;

  case 100:

    {;}
    break;

  case 101:

    {;}
    break;

  case 102:

    {;}
    break;

  case 103:

    {;}
    break;

  case 104:

    {;}
    break;

  case 105:

    {;}
    break;

  case 106:

    {;}
    break;

  case 107:

    {;}
    break;

  case 108:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_DATABASE; ;}
    break;

  case 109:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DATABASES; ;}
    break;

  case 110:

    {;}
    break;

  case 111:

    {;}
    break;

  case 112:

    {;}
    break;

  case 113:

    {;}
    break;

  case 114:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE; ;}
    break;

  case 115:

    {
                      result->cur_stmt_type_ = OBPROXY_T_DESC;
                      result->sub_stmt_type_ = OBPROXY_T_SUB_DESC_TABLE;
                  ;}
    break;

  case 116:

    {
                    result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_COLUMNS;
                    result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                  ;}
    break;

  case 117:

    {
                    result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_COLUMNS;
                    result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                    result->table_info_.database_name_ = (yyvsp[(5) - (5)].str);
                  ;}
    break;

  case 118:

    {
                 result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_INDEX;
                 result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
               ;}
    break;

  case 119:

    {
                 result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_INDEX;
                 result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                 result->table_info_.database_name_ = (yyvsp[(5) - (5)].str);
               ;}
    break;

  case 120:

    {;}
    break;

  case 121:

    { result->table_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 122:

    {;}
    break;

  case 123:

    { result->table_info_.database_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 124:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLES;
                ;}
    break;

  case 125:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_FULL_TABLES;
                ;}
    break;

  case 126:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLE_STATUS;
                      ;}
    break;

  case 127:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DB_VERSION; ;}
    break;

  case 128:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY; ;}
    break;

  case 129:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY;
                 ;}
    break;

  case 130:

    {
                     SET_ICMD_SECOND_STRING((yyvsp[(5) - (5)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY;
                 ;}
    break;

  case 131:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (7)].str));
                     SET_ICMD_SECOND_STRING((yyvsp[(7) - (7)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY;
                 ;}
    break;

  case 132:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_ROUTE_ADDR; ;}
    break;

  case 133:

    {
                              result->cur_stmt_type_ = OBPROXY_T_SET_ROUTE_ADDR;
                              result->cmd_info_.integer_[0] = (yyvsp[(3) - (3)].num);
                           ;}
    break;

  case 134:

    {;}
    break;

  case 135:

    {;}
    break;

  case 136:

    {;}
    break;

  case 137:

    {;}
    break;

  case 138:

    {;}
    break;

  case 139:

    {;}
    break;

  case 141:

    {
                   result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                 ;}
    break;

  case 142:

    {
                   result->table_info_.package_name_ = (yyvsp[(1) - (3)].str);
                   result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                 ;}
    break;

  case 143:

    {
                   result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                   result->table_info_.package_name_ = (yyvsp[(3) - (5)].str);
                   result->table_info_.table_name_ = (yyvsp[(5) - (5)].str);
                 ;}
    break;

  case 144:

    {
                result->call_parse_info_.node_count_ = 0;
              ;}
    break;

  case 145:

    {
                result->call_parse_info_.node_count_ = 0;
                add_call_node(result->call_parse_info_, (yyvsp[(1) - (1)].node));
              ;}
    break;

  case 146:

    {
                add_call_node(result->call_parse_info_, (yyvsp[(3) - (3)].node));
              ;}
    break;

  case 147:

    {
            malloc_call_node((yyval.node), CALL_TOKEN_STR_VAL);
            (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 148:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_INT_VAL);
           (yyval.node)->int_value_ = (yyvsp[(1) - (1)].num);
         ;}
    break;

  case 149:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_NUMBER_VAL);
           (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 150:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_USER_VAR);
           (yyval.node)->str_value_ = (yyvsp[(2) - (2)].str);
         ;}
    break;

  case 151:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_SYS_VAR);
           (yyval.node)->str_value_ = (yyvsp[(3) - (3)].str);
         ;}
    break;

  case 152:

    {
           result->placeholder_list_idx_++;
           malloc_call_node((yyval.node), CALL_TOKEN_PLACE_HOLDER);
           (yyval.node)->placeholder_idx_ = result->placeholder_list_idx_ - 1;
         ;}
    break;

  case 166:

    {
                                                                  handle_stmt_end(result);
                                                                  HANDLE_ACCEPT();
                                                                ;}
    break;

  case 171:

    {
                                                 handle_stmt_end(result);
                                                 HANDLE_ACCEPT();
                                               ;}
    break;

  case 175:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_USER);
        ;}
    break;

  case 176:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 177:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 178:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(5) - (5)].var_node), (yyvsp[(3) - (5)].str), SET_VAR_SYS);
        ;}
    break;

  case 179:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 180:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 181:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_SYS);
        ;}
    break;

  case 182:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_STR);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 183:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_INT);
               (yyval.var_node)->int_value_ = (yyvsp[(1) - (1)].num);
             ;}
    break;

  case 184:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_NUMBER);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 187:

    {;}
    break;

  case 188:

    {;}
    break;

  case 189:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 190:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 191:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 192:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 193:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 194:

    {
              malloc_shard_column_node((yyval.shard_node), (yyvsp[(2) - (7)].str), (yyvsp[(3) - (7)].str), DBMESH_TOKEN_STR_VAL);
              (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (7)].str);
              add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
            ;}
    break;

  case 195:

    { result->trace_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 196:

    { result->rpc_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 197:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 198:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 199:

    {;}
    break;

  case 200:

    {;}
    break;

  case 201:

    {;}
    break;

  case 203:

    { result->has_simple_route_info_ = true; result->simple_route_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 204:

    { result->simple_route_info_.part_key_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 208:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.group_idx_str_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 209:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.table_name_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 210:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 211:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 212:

    {result->dbp_route_info_.has_shard_key_ = true;;}
    break;

  case 213:

    { result->trace_id_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 214:

    { result->trace_id_ = (yyvsp[(3) - (6)].str); result->rpc_id_ = (yyvsp[(5) - (6)].str); ;}
    break;

  case 215:

    {;}
    break;

  case 217:

    {
                   if (result->dbp_route_info_.shard_key_count_ < OBPROXY_MAX_DBP_SHARD_KEY_NUM) {
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].left_str_ = (yyvsp[(1) - (3)].str);
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].right_str_ = (yyvsp[(3) - (3)].str);
                     ++result->dbp_route_info_.shard_key_count_;
                   }
                 ;}
    break;

  case 220:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 221:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 222:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 223:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 224:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 225:

    { result->trace_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 226:

    { result->rpc_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 227:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 228:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 229:

    {
             malloc_shard_column_node((yyval.shard_node), (yyvsp[(1) - (5)].str), (yyvsp[(3) - (5)].str), DBMESH_TOKEN_STR_VAL);
             (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (5)].str);
             add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
           ;}
    break;

  case 230:

    {;}
    break;

  case 231:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 233:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 255:

    { result->query_timeout_ = (yyvsp[(3) - (4)].num); ;}
    break;

  case 256:

    {;}
    break;

  case 258:

    {
      add_hint_index(result->dbmesh_route_info_, (yyvsp[(3) - (5)].str));
      result->dbmesh_route_info_.index_count_++;
    ;}
    break;

  case 259:

    {;}
    break;

  case 260:

    {;}
    break;

  case 261:

    {;}
    break;

  case 262:

    {;}
    break;

  case 263:

    {;}
    break;

  case 264:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_WEAK); ;}
    break;

  case 265:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_STRONG); ;}
    break;

  case 266:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_FROZEN); ;}
    break;

  case 269:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_WARNINGS; ;}
    break;

  case 270:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_ERRORS; ;}
    break;

  case 271:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 295:

    {
;}
    break;

  case 296:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 297:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 298:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 299:

    {;}
    break;

  case 300:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 301:

    {;}
    break;

  case 302:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 304:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 305:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 306:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 307:

    {;}
    break;

  case 308:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 309:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 310:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 312:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 313:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 314:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 315:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 316:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 318:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 319:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 320:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 321:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 322:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 323:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 324:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 325:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 326:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 327:

    {;}
    break;

  case 328:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 329:

    {;}
    break;

  case 330:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 331:

    {;}
    break;

  case 333:

    {;}
    break;

  case 334:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 335:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 336:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 337:

    {;}
    break;

  case 338:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 339:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 340:

    {;}
    break;

  case 341:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 342:

    {;}
    break;

  case 343:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 344:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 345:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 347:

    {;}
    break;

  case 348:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 349:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 350:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 351:

    {;}
    break;

  case 352:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 354:

    {;}
    break;

  case 355:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 356:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 357:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 358:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 359:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 360:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 361:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 362:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 363:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 364:

    {;}
    break;

  case 365:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 366:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 367:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 368:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 369:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 370:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 371:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 374:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 375:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 376:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 377:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 384:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 385:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 387:

    {;}
    break;

  case 388:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 389:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 390:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT();
                                                ;}
    break;

  case 391:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 392:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 393:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 394:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 395:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 396:

    {
                                                  UPDATE_ALIAS_NAME((yyvsp[(5) - (5)].str));
                                                  result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                                                  result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                                                ;}
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





void yyerror(YYLTYPE* yylloc, ObProxyParseResult* p, char* s, ...)
{
  // do nothing
}

void ob_proxy_gbk_parser_fatal_error(yyconst char *msg, yyscan_t yyscanner)
{
  fprintf(stderr, "FATAL ERROR:%s\n", msg);
  ObProxyParseResult *p = ob_proxy_parser_gbk_yyget_extra(yyscanner);
  if (OB_ISNULL(p)) {
    fprintf(stderr, "unexpected null parse result\n");
  } else {
    longjmp(p->jmp_buf_, 1);//the secord param must be non-zero value
  }
}

int obproxy_parse_gbk_sql(ObProxyParseResult* p, const char* buf, size_t len)
{
  int ret = OB_SUCCESS;
  //obproxydebug = 1;
  if (OB_ISNULL(p) || OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    // print err msg later
  } else if (OB_FAIL(ob_proxy_parser_gbk_yylex_init_extra(p, &(p->yyscan_info_)))) {
    // print err msg later
  } else {
    int val = setjmp(p->jmp_buf_);
    if (val) {
      ret = OB_PARSER_ERR_PARSE_SQL;
    } else {
      ob_proxy_parser_gbk_yy_scan_buffer((char *)buf, len, p->yyscan_info_);
      if (OB_FAIL(ob_proxy_parser_gbk_yyparse(p))) {
        // print err msg later
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

