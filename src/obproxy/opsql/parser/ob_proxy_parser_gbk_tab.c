
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
    /* insert into ... select 语法也需要支持 alias name */ \
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
     UNIQUE = 275,
     GRANT = 276,
     REVOKE = 277,
     ANALYZE = 278,
     PURGE = 279,
     COMMENT = 280,
     FROM = 281,
     DUAL = 282,
     PREPARE = 283,
     EXECUTE = 284,
     USING = 285,
     DEALLOCATE = 286,
     SELECT_HINT_BEGIN = 287,
     UPDATE_HINT_BEGIN = 288,
     DELETE_HINT_BEGIN = 289,
     INSERT_HINT_BEGIN = 290,
     REPLACE_HINT_BEGIN = 291,
     MERGE_HINT_BEGIN = 292,
     LOAD_DATA_HINT_BEGIN = 293,
     HINT_END = 294,
     COMMENT_BEGIN = 295,
     COMMENT_END = 296,
     ROUTE_TABLE = 297,
     ROUTE_PART_KEY = 298,
     PLACE_HOLDER = 299,
     END_P = 300,
     ERROR = 301,
     WHEN = 302,
     TABLEGROUP = 303,
     FLASHBACK = 304,
     AUDIT = 305,
     NOAUDIT = 306,
     STATUS = 307,
     BEGI = 308,
     START = 309,
     TRANSACTION = 310,
     READ = 311,
     ONLY = 312,
     WITH = 313,
     CONSISTENT = 314,
     SNAPSHOT = 315,
     INDEX = 316,
     XA = 317,
     GLOBALINDEX = 318,
     WARNINGS = 319,
     ERRORS = 320,
     TRACE = 321,
     QUICK = 322,
     COUNT = 323,
     AS = 324,
     WHERE = 325,
     VALUES = 326,
     ORDER = 327,
     GROUP = 328,
     HAVING = 329,
     INTO = 330,
     UNION = 331,
     FOR = 332,
     TX_READ_ONLY = 333,
     SELECT_OBPROXY_ROUTE_ADDR = 334,
     SET_OBPROXY_ROUTE_ADDR = 335,
     NAME_OB_DOT = 336,
     NAME_OB = 337,
     EXPLAIN = 338,
     EXPLAIN_ROUTE = 339,
     DESC = 340,
     DESCRIBE = 341,
     NAME_STR = 342,
     LOAD = 343,
     DATA = 344,
     LOCAL = 345,
     INFILE = 346,
     SLAVE = 347,
     RELAYLOG = 348,
     EVENTS = 349,
     HOSTS = 350,
     BINLOG = 351,
     USE = 352,
     HELP = 353,
     SET_NAMES = 354,
     SET_CHARSET = 355,
     SET_PASSWORD = 356,
     SET_DEFAULT = 357,
     SET_OB_READ_CONSISTENCY = 358,
     SET_TX_READ_ONLY = 359,
     GLOBAL = 360,
     SESSION = 361,
     GLOBAL_ALIAS = 362,
     SESSION_ALIAS = 363,
     LOCAL_ALIAS = 364,
     NUMBER_VAL = 365,
     GROUP_ID = 366,
     TABLE_ID = 367,
     ELASTIC_ID = 368,
     TESTLOAD = 369,
     ODP_COMMENT = 370,
     TNT_ID = 371,
     DISASTER_STATUS = 372,
     TRACE_ID = 373,
     RPC_ID = 374,
     TARGET_DB_SERVER = 375,
     TRACE_LOG = 376,
     DBP_COMMENT = 377,
     ROUTE_TAG = 378,
     SYS_TAG = 379,
     TABLE_NAME = 380,
     SCAN_ALL = 381,
     STICKY_SESSION = 382,
     PARALL = 383,
     SHARD_KEY = 384,
     STOP_DDL_TASK = 385,
     RETRY_DDL_TASK = 386,
     QUERY_TIMEOUT = 387,
     READ_CONSISTENCY = 388,
     WEAK = 389,
     STRONG = 390,
     FROZEN = 391,
     INT_NUM = 392,
     SHOW_PROXYNET = 393,
     THREAD = 394,
     CONNECTION = 395,
     LIMIT = 396,
     OFFSET = 397,
     SHOW_PROCESSLIST = 398,
     SHOW_PROXYSESSION = 399,
     SHOW_GLOBALSESSION = 400,
     ATTRIBUTE = 401,
     VARIABLES = 402,
     ALL = 403,
     STAT = 404,
     READ_STALE = 405,
     SHOW_PROXYCONFIG = 406,
     DIFF = 407,
     USER = 408,
     LIKE = 409,
     SHOW_PROXYSM = 410,
     SHOW_PROXYCLUSTER = 411,
     SHOW_PROXYRESOURCE = 412,
     SHOW_PROXYCONGESTION = 413,
     SHOW_PROXYROUTE = 414,
     PARTITION = 415,
     ROUTINE = 416,
     SUBPARTITION = 417,
     SHOW_PROXYVIP = 418,
     SHOW_PROXYMEMORY = 419,
     OBJPOOL = 420,
     SHOW_SQLAUDIT = 421,
     SHOW_WARNLOG = 422,
     SHOW_PROXYSTAT = 423,
     REFRESH = 424,
     SHOW_PROXYTRACE = 425,
     SHOW_PROXYINFO = 426,
     BINARY = 427,
     UPGRADE = 428,
     IDC = 429,
     SHOW_ELASTIC_ID = 430,
     SHOW_TOPOLOGY = 431,
     GROUP_NAME = 432,
     SHOW_DB_VERSION = 433,
     SHOW_DATABASES = 434,
     SHOW_TABLES = 435,
     SHOW_FULL_TABLES = 436,
     SELECT_DATABASE = 437,
     SELECT_PROXY_STATUS = 438,
     SHOW_CREATE_TABLE = 439,
     SELECT_PROXY_VERSION = 440,
     SHOW_COLUMNS = 441,
     SHOW_INDEX = 442,
     ALTER_PROXYCONFIG = 443,
     ALTER_PROXYRESOURCE = 444,
     PING_PROXY = 445,
     KILL_PROXYSESSION = 446,
     KILL_GLOBALSESSION = 447,
     KILL = 448,
     QUERY = 449,
     SHOW_BINLOG_SERVER_FOR_TENANT = 450,
     BINLOG_VARIABLE = 451,
     BINLOG_USER_VAR = 452,
     BINLOG_SYS_VAR = 453
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
#define YYFINAL  358
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   2762

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  210
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  153
/* YYNRULES -- Number of rules.  */
#define YYNRULES  492
/* YYNRULES -- Number of states.  */
#define YYNSTATES  801

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   453

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,   208,     2,     2,     2,     2,
     204,   205,   209,     2,   201,     2,   202,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   199,
       2,   203,     2,     2,   200,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   206,     2,   207,     2,     2,     2,     2,
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
     165,   166,   167,   168,   169,   170,   171,   172,   173,   174,
     175,   176,   177,   178,   179,   180,   181,   182,   183,   184,
     185,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     5,     7,     9,    12,    15,    18,    22,
      24,    27,    31,    33,    36,    38,    40,    42,    44,    46,
      48,    50,    52,    54,    56,    58,    60,    62,    64,    66,
      68,    70,    72,    74,    76,    78,    80,    82,    84,    86,
      88,    90,    92,    96,    97,    99,   105,   112,   114,   116,
     119,   122,   125,   128,   131,   134,   137,   140,   142,   144,
     147,   150,   152,   154,   156,   158,   160,   161,   163,   165,
     168,   170,   171,   173,   176,   179,   181,   183,   185,   187,
     189,   191,   193,   195,   198,   203,   206,   208,   210,   214,
     217,   221,   224,   227,   231,   235,   237,   239,   241,   243,
     245,   247,   249,   251,   253,   256,   258,   260,   262,   264,
     265,   268,   269,   271,   274,   280,   284,   286,   290,   292,
     294,   296,   298,   300,   302,   305,   307,   309,   311,   313,
     315,   317,   320,   323,   325,   328,   331,   336,   341,   344,
     349,   350,   353,   354,   357,   361,   365,   371,   373,   375,
     379,   385,   393,   395,   399,   401,   403,   405,   407,   409,
     411,   417,   419,   423,   429,   430,   432,   436,   438,   440,
     442,   445,   449,   451,   453,   456,   458,   461,   465,   469,
     471,   473,   475,   476,   480,   482,   486,   490,   496,   499,
     502,   507,   510,   512,   514,   516,   519,   523,   525,   530,
     537,   542,   548,   555,   560,   564,   566,   568,   570,   572,
     575,   579,   585,   592,   599,   606,   613,   620,   628,   635,
     642,   649,   656,   665,   674,   681,   682,   685,   688,   691,
     693,   697,   699,   704,   709,   713,   720,   724,   729,   734,
     741,   745,   747,   751,   752,   756,   760,   764,   768,   772,
     776,   780,   784,   788,   792,   794,   798,   804,   808,   809,
     811,   812,   814,   816,   818,   822,   826,   831,   833,   836,
     838,   841,   843,   846,   849,   853,   854,   856,   859,   861,
     864,   866,   869,   872,   875,   878,   879,   882,   884,   886,
     887,   890,   895,   900,   906,   908,   913,   915,   917,   918,
     920,   922,   924,   925,   927,   931,   935,   938,   944,   948,
     952,   956,   958,   960,   962,   964,   966,   968,   970,   972,
     974,   976,   978,   980,   982,   984,   986,   988,   990,   992,
     994,   996,   998,  1000,  1002,  1003,  1005,  1007,  1009,  1012,
    1014,  1020,  1021,  1024,  1029,  1034,  1035,  1038,  1039,  1042,
    1045,  1047,  1049,  1053,  1056,  1060,  1064,  1069,  1071,  1074,
    1075,  1078,  1082,  1085,  1088,  1091,  1092,  1095,  1099,  1102,
    1106,  1109,  1113,  1117,  1122,  1125,  1127,  1130,  1133,  1137,
    1140,  1143,  1144,  1146,  1148,  1151,  1154,  1158,  1161,  1164,
    1166,  1169,  1171,  1174,  1177,  1181,  1184,  1187,  1190,  1191,
    1193,  1197,  1203,  1206,  1210,  1213,  1214,  1216,  1219,  1222,
    1225,  1228,  1233,  1239,  1245,  1249,  1251,  1254,  1258,  1262,
    1265,  1268,  1272,  1276,  1277,  1280,  1282,  1286,  1290,  1294,
    1295,  1297,  1299,  1303,  1306,  1310,  1313,  1316,  1318,  1319,
    1322,  1327,  1330,  1335,  1338,  1340,  1346,  1350,  1354,  1357,
    1362,  1366,  1372,  1374,  1376,  1378,  1380,  1382,  1384,  1386,
    1388,  1390,  1392,  1394,  1396,  1398,  1400,  1402,  1404,  1406,
    1408,  1410,  1412,  1414,  1416,  1418,  1420,  1422,  1424,  1426,
    1428,  1430,  1432,  1434,  1436,  1438,  1440,  1442,  1444,  1446,
    1448,  1450,  1452
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     211,     0,    -1,   212,    -1,     1,    -1,   213,    -1,   212,
     213,    -1,   214,    45,    -1,   214,   199,    -1,   214,   199,
      45,    -1,   199,    -1,   199,    45,    -1,    53,   214,   199,
      -1,   215,    -1,   284,   215,    -1,   216,    -1,   274,    -1,
     280,    -1,   275,    -1,   276,    -1,   277,    -1,   223,    -1,
     222,    -1,   351,    -1,   314,    -1,   245,    -1,   315,    -1,
     355,    -1,   356,    -1,   257,    -1,   258,    -1,   259,    -1,
     260,    -1,   261,    -1,   262,    -1,   263,    -1,   224,    -1,
     236,    -1,   278,    -1,   317,    -1,   221,    -1,   357,    -1,
     298,    -1,   299,   242,   241,    -1,    -1,     9,    -1,    91,
      82,   217,    19,   360,    -1,    90,    91,    82,   217,    19,
     360,    -1,   219,    -1,   218,    -1,   306,   220,    -1,   238,
     216,    -1,   238,   274,    -1,   238,   276,    -1,   238,   277,
      -1,   238,   275,    -1,   238,   278,    -1,   240,   215,    -1,
     225,    -1,   237,    -1,    14,   226,    -1,    15,   227,    -1,
      16,    -1,    17,    -1,    18,    -1,   228,    -1,   229,    -1,
      -1,    19,    -1,    61,    -1,    20,    61,    -1,    48,    -1,
      -1,    48,    -1,   130,   137,    -1,   131,   137,    -1,   216,
      -1,   274,    -1,   275,    -1,   277,    -1,   276,    -1,   357,
      -1,   263,    -1,   278,    -1,   200,    82,    -1,   231,   201,
     200,    82,    -1,   200,    82,    -1,   232,    -1,   230,    -1,
      28,   362,    26,    -1,    29,   362,    -1,    29,   362,    30,
      -1,   234,   233,    -1,   235,   231,    -1,    15,    28,   362,
      -1,    31,    28,   362,    -1,    21,    -1,    22,    -1,    23,
      -1,    24,    -1,    49,    -1,    25,    -1,    50,    -1,    51,
      -1,   239,    -1,   239,    82,    -1,    83,    -1,    85,    -1,
      86,    -1,    84,    -1,    -1,    26,   270,    -1,    -1,   267,
      -1,     5,    78,    -1,     5,    78,   242,    26,   270,    -1,
       5,    78,   267,    -1,   185,    -1,   185,    69,   362,    -1,
     243,    -1,   244,    -1,   255,    -1,   256,    -1,   246,    -1,
     254,    -1,   176,   247,    -1,   253,    -1,   182,    -1,   183,
      -1,   179,    -1,   251,    -1,   252,    -1,   186,   247,    -1,
     187,   247,    -1,   248,    -1,   239,   362,    -1,    26,   362,
      -1,    26,   362,    26,   362,    -1,    26,   362,   202,   362,
      -1,   184,    82,    -1,   184,    82,   202,    82,    -1,    -1,
     154,    82,    -1,    -1,    26,    82,    -1,   180,   250,   249,
      -1,   181,   250,   249,    -1,    11,    19,    52,   250,   249,
      -1,   178,    -1,   175,    -1,   175,    26,    82,    -1,   175,
      70,   177,   203,    82,    -1,   175,    26,    82,    70,   177,
     203,    82,    -1,    79,    -1,    80,   203,   137,    -1,    99,
      -1,   100,    -1,   101,    -1,   102,    -1,   103,    -1,   104,
      -1,    13,   264,   204,   265,   205,    -1,   362,    -1,   362,
     202,   362,    -1,   362,   202,   362,   202,   362,    -1,    -1,
     266,    -1,   265,   201,   266,    -1,    82,    -1,   137,    -1,
     110,    -1,   200,    82,    -1,   200,   200,    82,    -1,    44,
      -1,   268,    -1,   267,   268,    -1,   269,    -1,   204,   205,
      -1,   204,   216,   205,    -1,   204,   267,   205,    -1,   359,
      -1,   271,    -1,   216,    -1,    -1,   204,   273,   205,    -1,
     362,    -1,   273,   201,   362,    -1,   302,   360,   358,    -1,
     302,   360,   358,   272,   271,    -1,   304,   270,    -1,   300,
     270,    -1,   301,   313,    26,   270,    -1,   305,   360,    -1,
     107,    -1,   108,    -1,   109,    -1,    12,   281,    -1,   282,
     201,   281,    -1,   282,    -1,   200,   362,   203,   283,    -1,
     200,   200,   105,   362,   203,   283,    -1,   105,   362,   203,
     283,    -1,   200,   200,   362,   203,   283,    -1,   200,   200,
     106,   362,   203,   283,    -1,   106,   362,   203,   283,    -1,
     362,   203,   283,    -1,   362,    -1,   137,    -1,   110,    -1,
     285,    -1,   285,   284,    -1,    40,   286,    41,    -1,    40,
     115,   294,   293,    41,    -1,    40,   112,   203,   297,   293,
      41,    -1,    40,   125,   203,   297,   293,    41,    -1,    40,
     111,   203,   297,   293,    41,    -1,    40,   113,   203,   297,
     293,    41,    -1,    40,   114,   203,   297,   293,    41,    -1,
      40,    81,    82,   203,   296,   293,    41,    -1,    40,   118,
     203,   295,   293,    41,    -1,    40,   119,   203,   295,   293,
      41,    -1,    40,   116,   203,   297,   293,    41,    -1,    40,
     117,   203,   297,   293,    41,    -1,    40,   122,   123,   203,
     206,   288,   207,    41,    -1,    40,   122,   124,   203,   206,
     290,   207,    41,    -1,    40,   120,   203,   297,   293,    41,
      -1,    -1,   286,   287,    -1,    42,    82,    -1,    43,    82,
      -1,    82,    -1,   289,   201,   288,    -1,   289,    -1,   111,
     204,   297,   205,    -1,   125,   204,   297,   205,    -1,   126,
     204,   205,    -1,   126,   204,   128,   203,   297,   205,    -1,
     127,   204,   205,    -1,   129,   204,   291,   205,    -1,    66,
     204,   295,   205,    -1,    66,   204,   295,   208,   295,   205,
      -1,   292,   201,   291,    -1,   292,    -1,    82,   203,   297,
      -1,    -1,   293,   201,   294,    -1,   111,   203,   297,    -1,
     112,   203,   297,    -1,   125,   203,   297,    -1,   113,   203,
     297,    -1,   114,   203,   297,    -1,   118,   203,   295,    -1,
     119,   203,   295,    -1,   116,   203,   297,    -1,   117,   203,
     297,    -1,   121,    -1,   120,   203,   297,    -1,    82,   202,
      82,   203,   296,    -1,    82,   203,   296,    -1,    -1,   297,
      -1,    -1,   297,    -1,    82,    -1,    87,    -1,     5,   200,
     197,    -1,     5,   279,   198,    -1,     5,   200,   200,   198,
      -1,     5,    -1,    32,   307,    -1,     8,    -1,    33,   307,
      -1,     6,    -1,    34,   307,    -1,     7,   303,    -1,    35,
     307,   303,    -1,    -1,   148,    -1,   148,    47,    -1,     9,
      -1,    36,   307,    -1,    10,    -1,    37,   307,    -1,    88,
      89,    -1,    38,   307,    -1,   308,    39,    -1,    -1,   311,
     308,    -1,   137,    -1,   362,    -1,    -1,   309,   310,    -1,
     132,   204,   137,   205,    -1,   133,   204,   312,   205,    -1,
      61,   204,   362,   362,   205,    -1,   121,    -1,   362,   204,
     310,   205,    -1,   362,    -1,   137,    -1,    -1,   134,    -1,
     135,    -1,   136,    -1,    -1,    67,    -1,    11,   350,    64,
      -1,    11,   350,    65,    -1,    11,    66,    -1,    11,    66,
      82,   203,    82,    -1,    11,    92,    95,    -1,    11,    92,
      52,    -1,    11,    93,    94,    -1,   321,    -1,   323,    -1,
     324,    -1,   327,    -1,   325,    -1,   329,    -1,   330,    -1,
     331,    -1,   332,    -1,   334,    -1,   335,    -1,   336,    -1,
     337,    -1,   338,    -1,   340,    -1,   341,    -1,   343,    -1,
     344,    -1,   345,    -1,   346,    -1,   347,    -1,   348,    -1,
     349,    -1,    -1,   105,    -1,   106,    -1,    90,    -1,    96,
     362,    -1,   195,    -1,    11,   316,   147,   154,   196,    -1,
      -1,   141,   137,    -1,   141,   137,   201,   137,    -1,   141,
     137,   142,   137,    -1,    -1,   154,    82,    -1,    -1,   154,
      82,    -1,   138,   322,    -1,   139,    -1,   140,    -1,   140,
     137,   318,    -1,   151,   319,    -1,   151,   148,   319,    -1,
     151,   152,   319,    -1,   151,   152,   153,   319,    -1,   143,
      -1,   145,   326,    -1,    -1,   146,    82,    -1,   146,   154,
      82,    -1,   146,   148,    -1,   154,    82,    -1,   144,   328,
      -1,    -1,   146,   319,    -1,   146,   137,   319,    -1,   149,
     319,    -1,   149,   137,   319,    -1,   147,   319,    -1,   147,
     137,   319,    -1,   147,   148,   319,    -1,   147,   148,   137,
     319,    -1,   150,   319,    -1,   155,    -1,   155,   137,    -1,
     156,   319,    -1,   156,   174,   319,    -1,   157,   319,    -1,
     158,   333,    -1,    -1,    82,    -1,   148,    -1,   148,    82,
      -1,   159,   320,    -1,   159,   161,   320,    -1,   159,   160,
      -1,   159,    63,    -1,   163,    -1,   163,    82,    -1,   164,
      -1,   164,   137,    -1,   164,   165,    -1,   164,   165,   137,
      -1,   166,   318,    -1,   166,   137,    -1,   167,   339,    -1,
      -1,   137,    -1,   137,   201,   137,    -1,   137,   201,   137,
     201,    82,    -1,   168,   319,    -1,   168,   169,   319,    -1,
     170,   342,    -1,    -1,   137,    -1,   137,   137,    -1,   171,
     172,    -1,   171,   173,    -1,   171,   174,    -1,   188,    12,
      82,   203,    -1,   188,    12,    82,   203,    82,    -1,   188,
      12,    82,   203,   137,    -1,   189,     6,    82,    -1,   190,
      -1,   191,   137,    -1,   191,   137,   137,    -1,   192,    82,
     137,    -1,   192,    82,    -1,   193,   137,    -1,   193,   140,
     137,    -1,   193,   194,   137,    -1,    -1,    68,   209,    -1,
      53,    -1,    54,    55,   352,    -1,    62,    53,    82,    -1,
      62,    54,    82,    -1,    -1,   353,    -1,   354,    -1,   353,
     201,   354,    -1,    56,    57,    -1,    58,    59,    60,    -1,
      97,   362,    -1,    98,    82,    -1,    82,    -1,    -1,   162,
     362,    -1,   162,   204,   362,   205,    -1,   160,   362,    -1,
     160,   204,   362,   205,    -1,   360,   358,    -1,   362,    -1,
     362,   202,   362,   200,   362,    -1,   362,   202,   362,    -1,
     362,   200,   362,    -1,   362,   362,    -1,   362,   202,   362,
     362,    -1,   362,    69,   362,    -1,   362,   202,   362,    69,
     362,    -1,    54,    -1,    62,    -1,    53,    -1,    55,    -1,
      59,    -1,    65,    -1,    64,    -1,    68,    -1,    67,    -1,
      66,    -1,   139,    -1,   140,    -1,   142,    -1,   146,    -1,
     147,    -1,   149,    -1,   152,    -1,   153,    -1,   165,    -1,
     169,    -1,   173,    -1,   174,    -1,   194,    -1,   177,    -1,
      49,    -1,    50,    -1,    51,    -1,    90,    -1,    89,    -1,
      52,    -1,   134,    -1,   135,    -1,   136,    -1,   105,    -1,
     106,    -1,    95,    -1,    94,    -1,    93,    -1,    96,    -1,
      82,    -1,   361,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   338,   338,   339,   341,   342,   344,   345,   346,   347,
     348,   349,   351,   352,   354,   355,   356,   357,   358,   359,
     360,   361,   362,   363,   364,   365,   366,   367,   368,   369,
     370,   371,   372,   373,   374,   375,   376,   377,   378,   379,
     380,   382,   383,   388,   390,   392,   397,   402,   403,   405,
     407,   408,   409,   410,   411,   412,   414,   416,   417,   419,
     420,   421,   422,   423,   424,   425,   427,   428,   429,   430,
     431,   433,   434,   436,   442,   448,   449,   450,   451,   452,
     453,   454,   455,   457,   464,   472,   480,   481,   484,   490,
     495,   501,   504,   507,   512,   518,   519,   520,   521,   522,
     523,   524,   525,   527,   528,   530,   531,   532,   534,   536,
     537,   539,   540,   542,   543,   544,   546,   547,   549,   550,
     551,   552,   553,   555,   556,   557,   558,   559,   560,   561,
     562,   563,   564,   565,   566,   573,   577,   582,   588,   593,
     600,   601,   603,   604,   606,   610,   615,   620,   622,   623,
     628,   633,   640,   643,   649,   650,   651,   652,   653,   654,
     657,   659,   663,   668,   676,   679,   684,   689,   694,   699,
     704,   709,   714,   722,   723,   725,   727,   728,   729,   731,
     732,   734,   736,   737,   739,   740,   742,   746,   747,   748,
     749,   750,   755,   756,   757,   759,   761,   762,   764,   768,
     772,   776,   780,   784,   788,   792,   797,   802,   808,   809,
     811,   812,   813,   814,   815,   816,   817,   818,   824,   825,
     826,   827,   828,   829,   830,   831,   832,   834,   835,   836,
     838,   839,   841,   846,   851,   852,   853,   854,   856,   857,
     859,   860,   862,   870,   871,   873,   874,   875,   876,   877,
     878,   879,   880,   881,   882,   883,   884,   890,   892,   893,
     895,   896,   898,   899,   901,   902,   903,   905,   906,   907,
     908,   909,   910,   911,   912,   914,   915,   916,   918,   919,
     920,   921,   922,   923,   925,   926,   927,   929,   930,   932,
     933,   935,   936,   937,   942,   943,   944,   945,   947,   948,
     949,   950,   952,   953,   956,   957,   958,   959,   960,   961,
     966,   970,   971,   972,   973,   974,   975,   976,   977,   978,
     979,   980,   981,   982,   983,   984,   985,   986,   987,   988,
     989,   990,   991,   992,   994,   995,   996,   997,  1000,  1001,
    1002,  1007,  1009,  1013,  1018,  1026,  1027,  1031,  1032,  1035,
    1037,  1038,  1039,  1043,  1044,  1045,  1046,  1051,  1053,  1055,
    1056,  1057,  1058,  1059,  1062,  1064,  1065,  1066,  1067,  1068,
    1069,  1070,  1071,  1072,  1073,  1077,  1078,  1082,  1083,  1088,
    1091,  1093,  1094,  1095,  1096,  1100,  1101,  1102,  1103,  1107,
    1108,  1112,  1113,  1114,  1115,  1119,  1120,  1123,  1125,  1126,
    1127,  1128,  1132,  1133,  1136,  1138,  1139,  1140,  1144,  1145,
    1146,  1150,  1151,  1152,  1156,  1160,  1164,  1165,  1169,  1170,
    1174,  1175,  1176,  1179,  1180,  1183,  1187,  1188,  1189,  1191,
    1192,  1194,  1195,  1198,  1199,  1202,  1208,  1211,  1213,  1214,
    1215,  1216,  1217,  1219,  1224,  1227,  1232,  1236,  1240,  1244,
    1249,  1253,  1259,  1260,  1261,  1262,  1263,  1264,  1265,  1266,
    1267,  1268,  1269,  1270,  1271,  1272,  1273,  1274,  1275,  1276,
    1277,  1278,  1279,  1280,  1281,  1282,  1283,  1284,  1285,  1286,
    1287,  1288,  1289,  1290,  1291,  1292,  1293,  1294,  1295,  1296,
    1297,  1299,  1300
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
  "RENAME", "TABLE", "UNIQUE", "GRANT", "REVOKE", "ANALYZE", "PURGE",
  "COMMENT", "FROM", "DUAL", "PREPARE", "EXECUTE", "USING", "DEALLOCATE",
  "SELECT_HINT_BEGIN", "UPDATE_HINT_BEGIN", "DELETE_HINT_BEGIN",
  "INSERT_HINT_BEGIN", "REPLACE_HINT_BEGIN", "MERGE_HINT_BEGIN",
  "LOAD_DATA_HINT_BEGIN", "HINT_END", "COMMENT_BEGIN", "COMMENT_END",
  "ROUTE_TABLE", "ROUTE_PART_KEY", "PLACE_HOLDER", "END_P", "ERROR",
  "WHEN", "TABLEGROUP", "FLASHBACK", "AUDIT", "NOAUDIT", "STATUS", "BEGI",
  "START", "TRANSACTION", "READ", "ONLY", "WITH", "CONSISTENT", "SNAPSHOT",
  "INDEX", "XA", "GLOBALINDEX", "WARNINGS", "ERRORS", "TRACE", "QUICK",
  "COUNT", "AS", "WHERE", "VALUES", "ORDER", "GROUP", "HAVING", "INTO",
  "UNION", "FOR", "TX_READ_ONLY", "SELECT_OBPROXY_ROUTE_ADDR",
  "SET_OBPROXY_ROUTE_ADDR", "NAME_OB_DOT", "NAME_OB", "EXPLAIN",
  "EXPLAIN_ROUTE", "DESC", "DESCRIBE", "NAME_STR", "LOAD", "DATA", "LOCAL",
  "INFILE", "SLAVE", "RELAYLOG", "EVENTS", "HOSTS", "BINLOG", "USE",
  "HELP", "SET_NAMES", "SET_CHARSET", "SET_PASSWORD", "SET_DEFAULT",
  "SET_OB_READ_CONSISTENCY", "SET_TX_READ_ONLY", "GLOBAL", "SESSION",
  "GLOBAL_ALIAS", "SESSION_ALIAS", "LOCAL_ALIAS", "NUMBER_VAL", "GROUP_ID",
  "TABLE_ID", "ELASTIC_ID", "TESTLOAD", "ODP_COMMENT", "TNT_ID",
  "DISASTER_STATUS", "TRACE_ID", "RPC_ID", "TARGET_DB_SERVER", "TRACE_LOG",
  "DBP_COMMENT", "ROUTE_TAG", "SYS_TAG", "TABLE_NAME", "SCAN_ALL",
  "STICKY_SESSION", "PARALL", "SHARD_KEY", "STOP_DDL_TASK",
  "RETRY_DDL_TASK", "QUERY_TIMEOUT", "READ_CONSISTENCY", "WEAK", "STRONG",
  "FROZEN", "INT_NUM", "SHOW_PROXYNET", "THREAD", "CONNECTION", "LIMIT",
  "OFFSET", "SHOW_PROCESSLIST", "SHOW_PROXYSESSION", "SHOW_GLOBALSESSION",
  "ATTRIBUTE", "VARIABLES", "ALL", "STAT", "READ_STALE",
  "SHOW_PROXYCONFIG", "DIFF", "USER", "LIKE", "SHOW_PROXYSM",
  "SHOW_PROXYCLUSTER", "SHOW_PROXYRESOURCE", "SHOW_PROXYCONGESTION",
  "SHOW_PROXYROUTE", "PARTITION", "ROUTINE", "SUBPARTITION",
  "SHOW_PROXYVIP", "SHOW_PROXYMEMORY", "OBJPOOL", "SHOW_SQLAUDIT",
  "SHOW_WARNLOG", "SHOW_PROXYSTAT", "REFRESH", "SHOW_PROXYTRACE",
  "SHOW_PROXYINFO", "BINARY", "UPGRADE", "IDC", "SHOW_ELASTIC_ID",
  "SHOW_TOPOLOGY", "GROUP_NAME", "SHOW_DB_VERSION", "SHOW_DATABASES",
  "SHOW_TABLES", "SHOW_FULL_TABLES", "SELECT_DATABASE",
  "SELECT_PROXY_STATUS", "SHOW_CREATE_TABLE", "SELECT_PROXY_VERSION",
  "SHOW_COLUMNS", "SHOW_INDEX", "ALTER_PROXYCONFIG", "ALTER_PROXYRESOURCE",
  "PING_PROXY", "KILL_PROXYSESSION", "KILL_GLOBALSESSION", "KILL", "QUERY",
  "SHOW_BINLOG_SERVER_FOR_TENANT", "BINLOG_VARIABLE", "BINLOG_USER_VAR",
  "BINLOG_SYS_VAR", "';'", "'@'", "','", "'.'", "'='", "'('", "')'", "'{'",
  "'}'", "'#'", "'*'", "$accept", "root", "sql_stmts", "sql_stmt",
  "comment_stmt", "stmt", "select_stmt", "opt_replace_ignore",
  "infile_desc", "local_infile_desc", "load_infile_desc", "load_data_stmt",
  "explain_stmt", "explain_route_stmt", "ddl_stmt", "mysql_ddl_stmt",
  "create_dll_expr", "drop_ddl_expr", "stop_ddl_task_stmt",
  "retry_ddl_task_stmt", "text_ps_from_stmt",
  "text_ps_execute_using_var_list", "text_ps_prepare_var_list",
  "text_ps_prepare_args_stmt", "text_ps_prepare_stmt",
  "text_ps_execute_stmt", "text_ps_stmt", "oracle_ddl_stmt",
  "explain_or_desc_stmt", "explain_or_desc", "explain_route", "opt_from",
  "select_expr_list", "select_tx_read_only_stmt",
  "select_proxy_version_stmt", "hooked_stmt", "shard_special_stmt",
  "db_tb_stmt", "show_create_table_stmt", "opt_show_like", "opt_show_from",
  "show_tables_stmt", "show_table_status_stmt", "show_db_version_stmt",
  "show_es_id_stmt", "select_obproxy_route_addr_stmt",
  "set_obproxy_route_addr_stmt", "set_names_stmt", "set_charset_stmt",
  "set_password_stmt", "set_default_stmt", "set_ob_read_consistency_stmt",
  "set_tx_read_only_stmt", "call_stmt", "routine_name_stmt",
  "call_expr_list", "call_expr", "expr_list", "expr", "clause", "fromlist",
  "sub_query", "opt_column_list", "column_list", "insert_stmt",
  "replace_stmt", "update_stmt", "delete_stmt", "merge_stmt",
  "opt_sys_var_alias", "set_stmt", "set_expr_list", "set_expr",
  "set_var_value", "comment_expr_list", "comment_expr", "comment_list",
  "comment", "dbp_comment_list", "dbp_comment", "dbp_sys_comment",
  "dbp_kv_comment_list", "dbp_kv_comment", "odp_comment_list",
  "odp_comment", "tracer_right_string_val", "name_right_string_val",
  "right_string_val", "select_with_binlog", "select_with_opt_hint",
  "update_with_opt_hint", "delete_with_opt_hint", "insert_with_opt_hint",
  "insert_all_when", "replace_with_opt_hint", "merge_with_opt_hint",
  "load_data_opt_hint", "hint_list_with_end", "hint_list", "hint_val",
  "hint_val_list", "hint", "opt_read_consistency", "opt_quick",
  "show_stmt", "icmd_stmt", "opt_global_or_session", "binlog_stmt",
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
     425,   426,   427,   428,   429,   430,   431,   432,   433,   434,
     435,   436,   437,   438,   439,   440,   441,   442,   443,   444,
     445,   446,   447,   448,   449,   450,   451,   452,   453,    59,
      64,    44,    46,    61,    40,    41,   123,   125,    35,    42
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   210,   211,   211,   212,   212,   213,   213,   213,   213,
     213,   213,   214,   214,   215,   215,   215,   215,   215,   215,
     215,   215,   215,   215,   215,   215,   215,   215,   215,   215,
     215,   215,   215,   215,   215,   215,   215,   215,   215,   215,
     215,   216,   216,   217,   217,   218,   219,   220,   220,   221,
     222,   222,   222,   222,   222,   222,   223,   224,   224,   225,
     225,   225,   225,   225,   225,   225,   226,   226,   226,   226,
     226,   227,   227,   228,   229,   230,   230,   230,   230,   230,
     230,   230,   230,   231,   231,   232,   233,   233,   234,   235,
     235,   236,   236,   236,   236,   237,   237,   237,   237,   237,
     237,   237,   237,   238,   238,   239,   239,   239,   240,   241,
     241,   242,   242,   243,   243,   243,   244,   244,   245,   245,
     245,   245,   245,   246,   246,   246,   246,   246,   246,   246,
     246,   246,   246,   246,   246,   247,   247,   247,   248,   248,
     249,   249,   250,   250,   251,   251,   252,   253,   254,   254,
     254,   254,   255,   256,   257,   258,   259,   260,   261,   262,
     263,   264,   264,   264,   265,   265,   265,   266,   266,   266,
     266,   266,   266,   267,   267,   268,   269,   269,   269,   270,
     270,   271,   272,   272,   273,   273,   274,   274,   275,   276,
     277,   278,   279,   279,   279,   280,   281,   281,   282,   282,
     282,   282,   282,   282,   282,   283,   283,   283,   284,   284,
     285,   285,   285,   285,   285,   285,   285,   285,   285,   285,
     285,   285,   285,   285,   285,   286,   286,   287,   287,   287,
     288,   288,   289,   289,   289,   289,   289,   289,   290,   290,
     291,   291,   292,   293,   293,   294,   294,   294,   294,   294,
     294,   294,   294,   294,   294,   294,   294,   294,   295,   295,
     296,   296,   297,   297,   298,   298,   298,   299,   299,   300,
     300,   301,   301,   302,   302,   303,   303,   303,   304,   304,
     305,   305,   306,   306,   307,   308,   308,   309,   309,   310,
     310,   311,   311,   311,   311,   311,   311,   311,   312,   312,
     312,   312,   313,   313,   314,   314,   314,   314,   314,   314,
     314,   315,   315,   315,   315,   315,   315,   315,   315,   315,
     315,   315,   315,   315,   315,   315,   315,   315,   315,   315,
     315,   315,   315,   315,   316,   316,   316,   316,   317,   317,
     317,   318,   318,   318,   318,   319,   319,   320,   320,   321,
     322,   322,   322,   323,   323,   323,   323,   324,   325,   326,
     326,   326,   326,   326,   327,   328,   328,   328,   328,   328,
     328,   328,   328,   328,   328,   329,   329,   330,   330,   331,
     332,   333,   333,   333,   333,   334,   334,   334,   334,   335,
     335,   336,   336,   336,   336,   337,   337,   338,   339,   339,
     339,   339,   340,   340,   341,   342,   342,   342,   343,   343,
     343,   344,   344,   344,   345,   346,   347,   347,   348,   348,
     349,   349,   349,   350,   350,   351,   351,   351,   351,   352,
     352,   353,   353,   354,   354,   355,   356,   357,   358,   358,
     358,   358,   358,   359,   360,   360,   360,   360,   360,   360,
     360,   360,   361,   361,   361,   361,   361,   361,   361,   361,
     361,   361,   361,   361,   361,   361,   361,   361,   361,   361,
     361,   361,   361,   361,   361,   361,   361,   361,   361,   361,
     361,   361,   361,   361,   361,   361,   361,   361,   361,   361,
     361,   362,   362
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     1,     1,     1,     2,     2,     2,     3,     1,
       2,     3,     1,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     3,     0,     1,     5,     6,     1,     1,     2,
       2,     2,     2,     2,     2,     2,     2,     1,     1,     2,
       2,     1,     1,     1,     1,     1,     0,     1,     1,     2,
       1,     0,     1,     2,     2,     1,     1,     1,     1,     1,
       1,     1,     1,     2,     4,     2,     1,     1,     3,     2,
       3,     2,     2,     3,     3,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     2,     1,     1,     1,     1,     0,
       2,     0,     1,     2,     5,     3,     1,     3,     1,     1,
       1,     1,     1,     1,     2,     1,     1,     1,     1,     1,
       1,     2,     2,     1,     2,     2,     4,     4,     2,     4,
       0,     2,     0,     2,     3,     3,     5,     1,     1,     3,
       5,     7,     1,     3,     1,     1,     1,     1,     1,     1,
       5,     1,     3,     5,     0,     1,     3,     1,     1,     1,
       2,     3,     1,     1,     2,     1,     2,     3,     3,     1,
       1,     1,     0,     3,     1,     3,     3,     5,     2,     2,
       4,     2,     1,     1,     1,     2,     3,     1,     4,     6,
       4,     5,     6,     4,     3,     1,     1,     1,     1,     2,
       3,     5,     6,     6,     6,     6,     6,     7,     6,     6,
       6,     6,     8,     8,     6,     0,     2,     2,     2,     1,
       3,     1,     4,     4,     3,     6,     3,     4,     4,     6,
       3,     1,     3,     0,     3,     3,     3,     3,     3,     3,
       3,     3,     3,     3,     1,     3,     5,     3,     0,     1,
       0,     1,     1,     1,     3,     3,     4,     1,     2,     1,
       2,     1,     2,     2,     3,     0,     1,     2,     1,     2,
       1,     2,     2,     2,     2,     0,     2,     1,     1,     0,
       2,     4,     4,     5,     1,     4,     1,     1,     0,     1,
       1,     1,     0,     1,     3,     3,     2,     5,     3,     3,
       3,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     0,     1,     1,     1,     2,     1,
       5,     0,     2,     4,     4,     0,     2,     0,     2,     2,
       1,     1,     3,     2,     3,     3,     4,     1,     2,     0,
       2,     3,     2,     2,     2,     0,     2,     3,     2,     3,
       2,     3,     3,     4,     2,     1,     2,     2,     3,     2,
       2,     0,     1,     1,     2,     2,     3,     2,     2,     1,
       2,     1,     2,     2,     3,     2,     2,     2,     0,     1,
       3,     5,     2,     3,     2,     0,     1,     2,     2,     2,
       2,     4,     5,     5,     3,     1,     2,     3,     3,     2,
       2,     3,     3,     0,     2,     1,     3,     3,     3,     0,
       1,     1,     3,     2,     3,     2,     2,     1,     0,     2,
       4,     2,     4,     2,     1,     5,     3,     3,     2,     4,
       3,     5,     1,     1,     1,     1,     1,     1,     1,     1,
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
       0,     3,   267,   271,   275,   269,   278,   280,   423,     0,
       0,    66,    71,    61,    62,    63,    95,    96,    97,    98,
     100,     0,     0,     0,   285,   285,   285,   285,   285,   285,
     285,   225,    99,   101,   102,   425,     0,     0,   152,     0,
     437,   105,   108,   106,   107,     0,     0,     0,     0,   154,
     155,   156,   157,   158,   159,     0,     0,     0,   357,   365,
     359,   345,   375,   345,   345,   381,   347,   389,   391,   341,
     398,   345,   405,     0,   148,     0,   147,   128,   142,   142,
     126,   127,     0,   116,     0,     0,     0,     0,   415,     0,
       0,     0,   339,     9,     0,     2,     4,     0,    12,    14,
      39,    21,    20,    35,    57,    64,    65,     0,     0,    36,
      58,     0,   103,     0,   118,   119,    24,   122,   133,   129,
     130,   125,   123,   120,   121,    28,    29,    30,    31,    32,
      33,    34,    15,    17,    18,    19,    37,    16,     0,   208,
      41,   111,     0,   302,     0,     0,     0,     0,    23,    25,
      38,   311,   312,   313,   315,   314,   316,   317,   318,   319,
     320,   321,   322,   323,   324,   325,   326,   327,   328,   329,
     330,   331,   332,   333,    22,    26,    27,    40,   113,   192,
     193,   194,     0,     0,   276,   273,     0,   306,     0,   337,
       0,     0,   335,   336,     0,     0,   476,   477,   478,   481,
     454,   452,   455,   456,   453,   458,   457,   461,   460,   459,
     491,   480,   479,   489,   488,   487,   490,   485,   486,   482,
     483,   484,   462,   463,   464,   465,   466,   467,   468,   469,
     470,   471,   472,   473,   475,   474,     0,   195,   197,   492,
       0,   485,   486,     0,   161,    67,     0,    70,    68,    59,
       0,    72,    60,     0,    89,     0,     0,   294,     0,     0,
     297,   268,     0,   285,   296,   270,   272,   275,   279,   281,
     283,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   425,     0,   429,     0,     0,
       0,   282,   338,   435,   436,    73,    74,   350,   351,   349,
     345,   345,   345,   345,   364,     0,     0,   358,   345,   345,
       0,   353,   376,   345,   377,   379,   382,   383,   380,   388,
       0,   387,   347,   385,   390,   392,   393,   396,     0,   395,
     399,   397,   345,   402,   406,   404,   408,   409,   410,     0,
       0,     0,   124,     0,   140,   140,   138,     0,   131,   132,
       0,     0,   416,   419,   420,     0,     0,    10,     1,     5,
       6,     7,   267,     0,    75,    87,    86,    91,    81,    76,
      77,    79,    78,    82,    80,     0,    92,    50,    51,    54,
      52,    53,    55,   104,   134,    56,    13,   209,     0,   109,
     112,   173,   175,   181,   189,   180,   179,   438,   444,   303,
       0,   438,   188,   191,     0,     0,    48,    47,    49,     0,
     115,   264,     0,   265,   277,   142,     0,   424,   309,   308,
     310,     0,   304,   305,     0,     0,     0,     0,     0,     0,
     164,     0,    69,    93,    88,    90,    94,     0,     0,   298,
     284,   286,   289,   274,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   254,
       0,   243,     0,     0,   258,   258,     0,     0,     0,     0,
     210,     0,     0,   229,   226,    11,     0,     0,   426,   430,
     431,   427,   428,   153,   341,   345,   366,   345,   345,   370,
     345,   368,   374,   360,   362,     0,   363,   354,   345,   355,
     346,   378,   384,   348,   386,   394,   342,     0,   403,   407,
     149,     0,   135,   143,     0,   144,   145,     0,   117,     0,
     414,   417,   418,   421,   422,     8,    85,    83,     0,   176,
       0,     0,     0,    42,   174,     0,     0,   443,     0,     0,
       0,   448,     0,   182,     0,    43,     0,   266,   140,     0,
       0,     0,     0,   485,   486,     0,     0,   196,   207,   206,
     204,   205,   172,   167,   169,   168,     0,     0,   165,   162,
       0,     0,   299,   300,   301,     0,   287,   289,     0,   288,
     260,   262,   263,   243,   243,   243,   243,     0,   260,     0,
       0,     0,     0,     0,     0,   258,   258,     0,     0,     0,
     243,   243,   243,   259,   243,   243,     0,     0,   243,   227,
     228,   433,     0,     0,   352,   367,   371,   345,   372,   369,
     361,   356,     0,     0,   400,     0,     0,     0,     0,   141,
     139,   411,     0,   177,   178,   110,     0,   441,     0,   439,
     450,   447,   446,   190,     0,     0,    43,    44,     0,   114,
     146,   307,   340,   200,   203,     0,     0,     0,   198,   170,
       0,     0,   160,     0,     0,   291,   292,   290,   295,   243,
     261,     0,     0,     0,     0,     0,   257,   245,   246,   248,
     249,   252,   253,   250,   251,   255,   247,   211,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   434,   432,   373,
     344,   343,     0,     0,   150,   136,   137,   412,   413,    84,
       0,     0,     0,     0,   449,     0,   184,   187,     0,     0,
       0,     0,   201,   171,   166,   163,   293,     0,   214,   212,
     215,   216,   260,   244,   220,   221,   218,   219,   224,     0,
       0,     0,     0,     0,     0,   231,     0,     0,   213,   401,
       0,   442,   440,   451,   445,     0,   183,     0,    45,   199,
     202,   217,   256,     0,     0,     0,     0,     0,     0,     0,
     258,     0,   151,   185,    46,     0,     0,     0,   234,   236,
       0,     0,   241,   222,   230,     0,   223,   232,   233,     0,
       0,   237,     0,   238,   258,     0,   242,   240,     0,   235,
     239
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    94,    95,    96,    97,    98,   393,   648,   406,   407,
     408,   100,   101,   102,   103,   104,   249,   252,   105,   106,
     365,   376,   366,   367,   107,   108,   109,   110,   111,   112,
     113,   533,   389,   114,   115,   116,   117,   342,   118,   515,
     344,   119,   120,   121,   122,   123,   124,   125,   126,   127,
     128,   129,   130,   131,   243,   567,   568,   390,   391,   392,
     394,   395,   645,   715,   132,   133,   134,   135,   136,   183,
     137,   237,   238,   560,   138,   139,   284,   474,   744,   745,
     747,   781,   782,   599,   461,   602,   669,   603,   140,   141,
     142,   143,   144,   185,   145,   146,   147,   261,   262,   577,
     578,   263,   575,   400,   148,   149,   194,   150,   329,   311,
     323,   151,   299,   152,   153,   154,   307,   155,   304,   156,
     157,   158,   159,   318,   160,   161,   162,   163,   164,   331,
     165,   166,   335,   167,   168,   169,   170,   171,   172,   173,
     195,   174,   478,   479,   480,   175,   176,   177,   537,   396,
     397,   239,   398
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -547
static const yytype_int16 yypact[] =
{
     656,  -547,   -21,  -547,   -48,  -547,  -547,  -547,    43,  1861,
    2376,    77,    56,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  2376,  2376,   112,  2123,  2123,  2123,  2123,  2123,  2123,
    2123,   162,  -547,  -547,  -547,  1046,    66,    93,  -547,   -28,
    -547,  -547,  -547,  -547,  -547,    94,  2376,  2376,   107,  -547,
    -547,  -547,  -547,  -547,  -547,    69,    74,    29,  -547,   139,
     -52,    99,    85,   -56,    88,     9,    27,   163,   -70,    39,
     146,    23,   156,   155,    67,   273,  -547,  -547,   282,   282,
    -547,  -547,   230,   264,   273,   273,   324,   349,  -547,   219,
     275,   -55,  -547,   313,   359,   851,  -547,     4,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,    38,   160,  -547,
    -547,   308,  2507,  1237,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  1237,   322,
    -547,   159,   418,   297,  2376,   418,  2376,   166,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,     1,  -547,
    -547,  -547,   -15,   167,   319,  -547,   315,   286,   161,  -547,
      37,   277,  -547,  -547,   222,   206,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  2376,  2376,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  1992,  -547,   171,  -547,
     170,  -547,  -547,   172,   173,  -547,   316,  -547,  -547,  -547,
    2376,  -547,  -547,   348,   350,  2376,   175,  -547,   178,   179,
    -547,  -547,   339,  2123,   180,  -547,  -547,   -48,  -547,  -547,
    -547,   299,   182,   183,   185,   187,   184,   188,   190,   192,
     193,   194,   140,   195,   131,  -547,   201,   174,   305,   317,
     266,  -547,  -547,  -547,  -547,  -547,  -547,  -547,   267,  -547,
     -38,    83,   -23,    88,  -547,    10,   323,  -547,    88,   177,
     325,  -547,  -547,    88,  -547,  -547,  -547,   326,  -547,  -547,
     327,  -547,   247,  -547,  -547,  -547,   274,  -547,   276,  -547,
     209,  -547,    88,  -547,   278,  -547,  -547,  -547,  -547,   330,
     237,  2376,  -547,   334,   270,   270,   216,  2376,  -547,  -547,
     343,   344,   284,   290,  -547,   292,   293,  -547,  -547,  -547,
    -547,   386,   -29,   352,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,   353,   231,  -547,  -547,  -547,
    -547,  -547,  -547,     5,  -547,  -547,  -547,  -547,    21,   410,
     159,  -547,  -547,  -547,  -547,  -547,  -547,   130,  1576,  -547,
     411,   130,  -547,  -547,   347,   357,  -547,  -547,  -547,   414,
       3,  -547,   243,  -547,  -547,   282,   239,  -547,  -547,  -547,
    -547,   289,  -547,  -547,   242,   245,  2568,   248,  1861,  2184,
      33,  2376,  -547,  -547,  -547,  -547,  -547,  2376,   318,   212,
    -547,  -547,  2315,  -547,   249,    47,    47,    47,    47,   132,
     250,   251,   253,   255,   256,   257,   258,   259,   260,  -547,
     261,  -547,    47,    47,    47,    47,    47,   262,   263,    47,
    -547,   392,   393,  -547,  -547,  -547,   419,   420,  -547,   280,
    -547,  -547,  -547,  -547,   337,    88,  -547,    88,    24,  -547,
      88,  -547,  -547,  -547,  -547,   405,  -547,  -547,    88,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,   -73,   351,  -547,  -547,
     421,   287,     8,  -547,   407,  -547,  -547,   412,  -547,   295,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,   296,  -547,
     288,   145,   418,  -547,  -547,  1384,  1445,  -547,  2376,  2376,
    2376,  -547,   418,    20,   413,   483,   418,  -547,   270,   415,
     303,  2184,  2184,  2376,  2376,   298,  2184,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,   -16,    -7,  -547,   300,
    2376,   301,  -547,  -547,  -547,   304,  -547,  2315,   310,  -547,
      47,  -547,  -547,  -547,  -547,  -547,  -547,   422,    47,    47,
      47,    47,    47,    47,    47,    47,    47,    47,    47,    -8,
    -547,  -547,  -547,  -547,  -547,  -547,   311,   312,  -547,  -547,
    -547,  -547,   443,   174,  -547,  -547,  -547,    88,  -547,  -547,
    -547,  -547,   368,   373,   320,   342,   434,  2376,  2376,  -547,
    -547,    30,   438,  -547,  -547,  -547,  2376,  -547,  2376,  -547,
    -547,  -547,  1730,  -547,  2376,    78,   483,  -547,   503,  -547,
    -547,  -547,  -547,  -547,  -547,   329,   331,  2184,  -547,  -547,
     446,    33,  -547,  2376,   328,  -547,  -547,  -547,  -547,  -547,
    -547,    -6,    -5,    -2,    -1,   332,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,   184,    11,
      13,    14,    15,    17,   123,   459,    22,  -547,  -547,  -547,
    -547,  -547,   454,   335,  -547,  -547,  -547,  -547,  -547,  -547,
     336,   341,  2376,  2376,  -547,    -4,  -547,  -547,   518,  2376,
    2184,  2184,  -547,  -547,  -547,  -547,  -547,    35,  -547,  -547,
    -547,  -547,    47,  -547,  -547,  -547,  -547,  -547,  -547,   345,
     346,   355,   358,   362,   333,   338,   365,   356,  -547,  -547,
     466,  -547,  -547,  -547,  -547,  2376,  -547,  2376,  -547,  -547,
    -547,  -547,  -547,    47,    47,   -60,   367,   469,   514,   123,
      47,   515,  -547,  -547,  -547,   369,   370,   375,  -547,  -547,
     376,   377,   372,  -547,  -547,    50,  -547,  -547,  -547,    47,
      47,  -547,   469,  -547,    47,   379,  -547,  -547,   380,  -547,
    -547
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -547,  -547,  -547,   485,   546,    28,     6,   -58,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,   408,  -547,  -547,  -547,  -547,   269,  -547,  -331,
     -76,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,   482,  -547,  -547,   -71,  -167,  -329,  -547,
    -140,   -51,  -547,  -547,   128,   133,   158,   199,   200,  -547,
    -547,   165,  -547,  -492,   457,  -547,  -547,  -547,  -172,  -547,
    -547,  -194,  -547,  -478,   -89,  -433,  -546,  -438,  -547,  -547,
    -547,  -547,  -547,   340,  -547,  -547,  -547,   294,   354,  -547,
      25,  -547,  -547,  -547,  -547,  -547,  -547,  -547,   116,   -41,
     279,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,   -10,  -547,  -547,   497,   204,  -547,
    -142,  -547,    -9
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -492
static const yytype_int16 yytable[] =
{
     240,   244,   401,   345,   403,   402,    99,   583,   584,   585,
     586,   410,   253,   254,   516,   264,   264,   264,   264,   264,
     264,   264,   314,   315,   600,   601,   362,  -111,   605,  -112,
     333,   608,   604,   687,   627,   728,   729,   292,   293,   730,
     731,    99,   676,   362,     3,     4,     5,     6,     7,   360,
    -491,    10,   734,    24,   735,   736,   737,   178,   738,   653,
     654,   534,   186,   748,   658,  -186,   659,   325,   777,   622,
      24,    25,    26,    27,    28,    29,   761,   562,   179,   180,
     181,   534,   354,   362,   250,   355,   179,   180,   181,   418,
     319,   316,   493,   339,   305,   326,   245,   246,   310,   485,
     184,    99,   306,   384,   251,   671,   672,   673,   674,   187,
      24,   188,   707,   364,   490,   563,   310,   377,   313,    99,
      40,   287,   689,   690,   691,   247,   692,   693,   623,   581,
     696,   310,   419,   189,   582,   190,   191,   340,   248,   356,
     255,   385,   670,   564,    99,   778,   288,   289,   192,   193,
     670,   677,   678,   679,   680,   681,   682,   317,   494,   685,
     686,   617,   683,   684,   495,   722,   386,   708,   297,   298,
     565,   182,   470,   471,   472,   290,   327,   310,   310,   182,
     328,   320,   411,   291,   660,   412,   762,   321,   322,   294,
    -334,   727,   332,   688,   661,   688,   688,   755,   662,   688,
     688,   756,   534,   361,  -491,   388,   295,   388,   424,   425,
     628,   296,   688,   473,   688,   688,   688,   650,   688,  -186,
     487,   531,   312,   688,   644,   388,   529,   427,   759,   760,
     476,   488,   477,   566,   739,   369,   688,   310,   363,   378,
     370,   433,   310,   271,   379,   324,   436,   308,   740,   741,
     742,   309,   743,   310,   264,   793,   404,   405,   794,   486,
     489,   491,   492,   467,   468,   371,   449,   497,   499,   380,
     422,   423,   501,   272,   273,   274,   275,   276,   277,   278,
     279,   280,   281,   330,   282,   300,   301,   283,   302,   303,
     535,   508,   536,   334,   670,   450,   451,   452,   453,   341,
     454,   455,   456,   457,   458,   459,   372,   373,   343,   460,
     381,   382,   346,   362,     3,     4,     5,     6,     7,   265,
     266,   267,   268,   269,   270,   775,   776,   336,   337,   338,
     498,   310,   512,   347,   587,   588,   350,   785,   518,   548,
      24,    25,    26,    27,    28,    29,   572,   573,   574,   388,
     634,   795,   796,   348,   349,   351,   352,   353,   357,   358,
     375,   798,    31,   388,   399,   413,   414,   415,   416,   421,
     417,   420,   428,   429,   434,   431,   430,   432,   440,   437,
     435,   444,   438,   439,   442,   445,   446,   481,   447,   541,
     448,   462,   635,   463,   530,   464,   465,   466,   469,   482,
     475,   320,   643,   483,   484,   496,   649,   500,   502,   503,
     507,   505,   510,   506,   511,   509,   513,   555,   517,   240,
     561,   521,   569,   362,   514,   519,   520,   522,   570,   523,
     524,   525,   528,   579,   526,   527,   532,   542,   544,   545,
     546,   547,   549,   550,   615,   551,   616,   618,   552,   619,
      24,   556,   580,   589,   590,   571,   591,   621,   592,   593,
     594,   595,   596,   597,   598,   606,   607,   196,   197,   198,
     199,   200,   201,   202,   609,   610,   611,   203,   328,   612,
     204,   613,   205,   206,   207,   208,   209,   620,   624,   629,
     626,   625,   647,   633,   630,   646,   632,   651,   631,   652,
     210,   657,   663,   697,   675,   700,   665,   211,   212,   666,
     701,   213,   214,   215,   216,   668,   704,   694,   695,   703,
     709,   702,   719,   241,   242,   746,   637,   639,   723,   640,
     641,   642,   720,   726,   721,   732,   749,   757,   750,   769,
     768,   751,   561,   561,   655,   656,   752,   561,   772,   763,
     764,   780,   219,   220,   221,   783,   786,   222,   223,   765,
     224,   664,   766,   771,   225,   226,   767,   227,   579,   770,
     228,   229,   779,   792,   787,   788,   699,   758,   789,   790,
     359,   286,   791,   230,   799,   800,   409,   231,   718,   368,
     724,   232,   233,   557,   717,   234,   387,   784,   797,   733,
     614,   504,   667,   698,   374,   543,     0,   443,     0,     0,
       0,     0,   235,     0,     0,   774,     0,   441,   705,   706,
       0,     0,     0,     0,     0,     0,     0,   710,     0,   711,
       0,     0,     0,   714,     0,   716,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   561,     0,
       0,     0,     0,     0,   725,     0,     0,     1,     0,     0,
       0,     2,     3,     4,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,     0,     0,    16,    17,    18,
      19,    20,     0,     0,    21,    22,     0,    23,    24,    25,
      26,    27,    28,    29,    30,     0,    31,     0,     0,     0,
       0,     0,     0,   753,   754,    32,    33,    34,     0,    35,
      36,   561,   561,     0,     0,     0,     0,     0,    37,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    38,    39,     0,    40,    41,
      42,    43,    44,     0,    45,     0,   773,     0,     0,     0,
       0,     0,    46,    47,    48,    49,    50,    51,    52,    53,
      54,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    55,    56,     0,     0,
       0,     0,     0,     0,    57,     0,     0,     0,     0,    58,
      59,    60,     0,     0,     0,     0,     0,    61,     0,     0,
       0,    62,    63,    64,    65,    66,     0,     0,     0,    67,
      68,     0,    69,    70,    71,     0,    72,    73,     0,     0,
       0,    74,    75,     0,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
       0,    92,     0,     0,     0,    93,     2,     3,     4,     5,
       6,     7,     8,     9,    10,    11,    12,    13,    14,    15,
       0,     0,    16,    17,    18,    19,    20,     0,     0,    21,
      22,     0,    23,    24,    25,    26,    27,    28,    29,    30,
       0,    31,     0,     0,     0,     0,     0,     0,     0,     0,
      32,    33,    34,     0,    35,    36,     0,     0,     0,     0,
       0,     0,     0,    37,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      38,    39,     0,    40,    41,    42,    43,    44,     0,    45,
       0,     0,     0,     0,     0,     0,     0,    46,    47,    48,
      49,    50,    51,    52,    53,    54,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    55,    56,     0,     0,     0,     0,     0,     0,    57,
       0,     0,     0,     0,    58,    59,    60,     0,     0,     0,
       0,     0,    61,     0,     0,     0,    62,    63,    64,    65,
      66,     0,     0,     0,    67,    68,     0,    69,    70,    71,
       0,    72,    73,     0,     0,     0,    74,    75,     0,    76,
      77,    78,    79,    80,    81,    82,    83,    84,    85,    86,
      87,    88,    89,    90,    91,     0,    92,     0,     0,     0,
      93,     2,     3,     4,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,     0,     0,    16,    17,    18,
      19,    20,     0,     0,    21,    22,     0,    23,    24,    25,
      26,    27,    28,    29,    30,     0,    31,     0,     0,     0,
       0,     0,     0,     0,     0,    32,    33,    34,     0,   285,
      36,     0,     0,     0,     0,     0,     0,     0,    37,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    38,    39,     0,    40,    41,
      42,    43,    44,     0,    45,     0,     0,     0,     0,     0,
       0,     0,    46,    47,    48,    49,    50,    51,    52,    53,
      54,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    55,    56,     0,     0,
       0,     0,     0,     0,    57,     0,     0,     0,     0,    58,
      59,    60,     0,     0,     0,     0,     0,    61,     0,     0,
       0,    62,    63,    64,    65,    66,     0,     0,     0,    67,
      68,     0,    69,    70,    71,     0,    72,    73,     0,     0,
       0,    74,    75,     0,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
       0,    92,     2,     3,     4,     5,     6,     7,     8,     9,
      10,    11,    12,    13,    14,    15,     0,     0,    16,    17,
      18,    19,    20,     0,     0,    21,    22,     0,    23,    24,
      25,    26,    27,    28,    29,    30,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    32,    33,    34,     0,
     285,    36,     0,     0,     0,     0,     0,     0,     0,    37,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    38,    39,     0,    40,
      41,    42,    43,    44,     0,    45,     0,     0,     0,     0,
       0,     0,     0,    46,    47,    48,    49,    50,    51,    52,
      53,    54,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    55,    56,     0,
       0,     0,     0,     0,     0,    57,     0,     0,     0,     0,
      58,    59,    60,     0,     0,     0,     0,     0,    61,     0,
       0,     0,    62,    63,    64,    65,    66,     0,     0,     0,
      67,    68,     0,    69,    70,    71,     0,    72,    73,     0,
       0,     0,    74,    75,     0,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,     0,    92,   196,   197,   198,   199,   200,   201,   202,
       0,     0,     0,   203,     0,     0,   204,     0,   205,   206,
     207,   208,   209,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   210,     0,     0,     0,
       0,     0,     0,   211,   212,     0,     0,   213,   214,   215,
     216,     0,     0,     0,     0,     0,     0,     0,     0,   241,
     242,     0,     0,     0,   196,   197,   198,   199,   200,   201,
     202,     0,     0,     0,   203,     0,     0,   204,     0,   205,
     206,   207,   208,   209,     0,     0,     0,     0,   219,   220,
     221,     0,     0,   222,   223,     0,   224,   210,     0,     0,
     225,   226,     0,   227,   211,   212,   228,   229,   213,   214,
     215,   216,     0,     0,     0,     0,     0,     0,     0,   230,
     241,   242,     0,   231,     0,     0,     0,   232,   233,     0,
       0,   234,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   235,   219,
     220,   221,     0,     0,   222,   223,     0,   224,   636,     0,
       0,   225,   226,     0,   227,     0,     0,   228,   229,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     230,     0,     0,     0,   231,     0,     0,     0,   232,   233,
       0,     0,   234,     0,     0,   196,   197,   198,   199,   200,
     201,   202,     0,     0,     0,   203,     0,     0,   204,   235,
     205,   206,   207,   208,   209,   538,     0,     0,     0,   638,
       0,     0,     0,     0,     0,     0,     0,     0,   210,     0,
       0,     0,     0,     0,     0,   211,   212,     0,     0,   213,
     214,   215,   216,     0,     0,     0,     0,     0,     0,     0,
       0,   241,   242,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     219,   220,   221,     0,     0,   222,   223,     0,   224,     0,
       0,     0,   225,   226,     0,   227,     0,     0,   228,   229,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   230,     0,     0,     0,   231,     0,     0,     0,   232,
     233,     0,     0,   234,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     235,     0,     0,     0,     0,     0,   539,     0,   540,   196,
     197,   198,   199,   200,   201,   202,     0,     0,     0,   203,
       0,     0,   204,     0,   205,   206,   207,   208,   209,   712,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   210,     0,     0,     0,     0,     0,     0,   211,
     212,     0,     0,   213,   214,   215,   216,     0,     0,     0,
       0,     0,     0,     0,     0,   241,   242,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   219,   220,   221,     0,     0,   222,
     223,     0,   224,     0,     0,     0,   225,   226,     0,   227,
       0,     0,   228,   229,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   230,     0,     0,     0,   231,
       0,     0,     0,   232,   233,     0,     0,   234,     0,     0,
     196,   197,   198,   199,   200,   201,   202,     0,     0,     0,
     203,     0,     0,   204,   235,   205,   206,   207,   208,   209,
     713,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   210,     0,     0,     0,     0,     0,     0,
     211,   212,     0,     0,   213,   214,   215,   216,     0,     0,
       0,     0,     0,     0,     0,     0,   217,   218,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   219,   220,   221,     0,     0,
     222,   223,     0,   224,     0,     0,     0,   225,   226,     0,
     227,     0,     0,   228,   229,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   230,     0,     0,     0,
     231,     0,     0,     0,   232,   233,     0,     0,   234,     0,
       0,   196,   197,   198,   199,   200,   201,   202,     0,     0,
       0,   203,     0,     0,   204,   235,   205,   206,   207,   208,
     209,   236,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   210,     0,     0,     0,     0,     0,
       0,   211,   212,     0,     0,   213,   214,   215,   216,     0,
       0,     0,     0,     0,     0,     0,     0,   241,   242,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   219,   220,   221,     0,
       0,   222,   223,     0,   224,     0,     0,     0,   225,   226,
       0,   227,     0,     0,   228,   229,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   230,     0,     0,
       0,   231,     0,     0,     0,   232,   233,     0,     0,   234,
       0,     0,   196,   197,   198,   199,   200,   201,   202,     0,
       0,     0,   203,     0,   256,   204,   235,   205,   206,   207,
     208,   209,   426,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   210,     0,     0,     0,     0,
       0,     0,   211,   212,     0,     0,   213,   214,   215,   216,
       0,     0,     0,     0,     0,     0,     0,     0,   241,   242,
       0,     0,     0,   196,   197,   198,   199,   200,   201,   202,
       0,     0,     0,   203,   257,     0,   204,     0,   205,   206,
     207,   208,   209,     0,     0,   258,   259,   219,   220,   221,
     260,     0,   222,   223,     0,   224,   210,     0,     0,   225,
     226,     0,   227,   211,   212,   228,   229,   213,   214,   215,
     216,     0,     0,     0,     0,     0,     0,     0,   230,   241,
     242,     0,   231,     0,   558,     0,   232,   233,     0,     0,
     234,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   235,   219,   220,
     221,   559,     0,   222,   223,     0,   224,     0,     0,     0,
     225,   226,     0,   227,     0,     0,   228,   229,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   230,
       0,     0,     0,   231,     0,     0,     0,   232,   233,     0,
       0,   234,     0,     0,   196,   197,   198,   199,   200,   201,
     202,     0,     0,     0,   203,     0,     0,   204,   235,   205,
     206,   207,   208,   209,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   210,     0,     0,
       0,     0,     0,     0,   211,   212,     0,     0,   213,   214,
     215,   216,     0,     0,     0,     0,     0,     0,     0,     0,
     241,   242,     0,     0,     0,   196,   197,   198,   199,   200,
     201,   202,     0,     0,     0,   203,     0,     0,   204,     0,
     205,   206,   207,   208,   209,     0,     0,     0,     0,   219,
     220,   221,   576,     0,   222,   223,     0,   224,   210,     0,
       0,   225,   226,     0,   227,   211,   212,   228,   229,   213,
     214,   215,   216,     0,     0,     0,     0,     0,     0,     0,
     230,   241,   242,     0,   231,     0,     0,     0,   232,   233,
       0,     0,   234,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   235,
     219,   220,   221,     0,     0,   222,   223,     0,   224,     0,
       0,     0,   225,   226,     0,   227,     0,     0,   228,   229,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   230,     0,     0,     0,   231,     0,     0,     0,   232,
     233,     0,     0,   234,     0,     0,   196,   197,   198,   199,
     200,   201,   202,     0,     0,     0,   203,     0,     0,   204,
     235,   205,   206,   207,   208,   209,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   383,
       0,     0,     0,     0,     0,     0,   211,   212,     0,     0,
     213,   214,   215,   216,     0,     0,     0,     0,     0,     0,
       0,     0,   241,   242,     0,     0,     0,   196,   197,   198,
     199,   200,   201,   202,     0,     0,     0,   203,     0,     0,
     204,     0,   205,   206,   207,   208,   209,     0,     0,     0,
       0,   219,   220,   221,     0,     0,   222,   223,     0,   224,
     210,     0,     0,   225,   226,     0,   227,   211,   212,   228,
     229,   213,   214,   215,   216,     0,     0,     0,     0,     0,
       0,     0,   230,   553,   554,     0,   231,     0,     0,     0,
     232,   233,     0,     0,   234,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   235,   219,   220,   221,     0,     0,   222,   223,     0,
     224,     0,     0,     0,   225,   226,     0,   227,     0,     0,
     228,   229,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   230,     0,     0,     0,   231,     0,     0,
       0,   232,   233,     0,     0,   234,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   235
};

static const yytype_int16 yycheck[] =
{
       9,    10,   144,    79,   146,   145,     0,   445,   446,   447,
     448,   178,    21,    22,   345,    24,    25,    26,    27,    28,
      29,    30,    63,    64,   462,   463,     5,    26,   466,    26,
      71,   469,   465,    41,    26,    41,    41,    46,    47,    41,
      41,    35,   588,     5,     6,     7,     8,     9,    10,    45,
      45,    13,    41,    32,    41,    41,    41,    78,    41,   551,
     552,   390,    19,    41,   556,    45,    82,   137,   128,   142,
      32,    33,    34,    35,    36,    37,    41,    44,   107,   108,
     109,   410,   137,     5,    28,   140,   107,   108,   109,    52,
      63,    82,    82,    26,   146,   165,    19,    20,   154,   137,
     148,    95,   154,   112,    48,   583,   584,   585,   586,    66,
      32,    68,    82,   107,   137,    82,   154,   111,   174,   113,
      82,    55,   600,   601,   602,    48,   604,   605,   201,    82,
     608,   154,    95,    90,    87,    92,    93,    70,    61,   194,
      28,   113,   580,   110,   138,   205,    53,    54,   105,   106,
     588,   589,   590,   591,   592,   593,   594,   148,   148,   597,
     598,   137,   595,   596,   154,   657,   138,   137,   139,   140,
     137,   200,    41,    42,    43,   203,   137,   154,   154,   200,
     141,   154,   197,    89,   200,   200,   732,   160,   161,    82,
     147,   669,   169,   201,   201,   201,   201,   201,   205,   201,
     201,   205,   531,   199,   199,   204,   137,   204,   217,   218,
     202,   137,   201,    82,   201,   201,   201,   548,   201,   199,
     137,   388,   137,   201,   204,   204,   205,   236,   720,   721,
      56,   148,    58,   200,   111,   107,   201,   154,   200,   111,
     107,   250,   154,    81,   111,    82,   255,   148,   125,   126,
     127,   152,   129,   154,   263,   205,    90,    91,   208,   300,
     301,   302,   303,   123,   124,   107,    82,   308,   309,   111,
      64,    65,   313,   111,   112,   113,   114,   115,   116,   117,
     118,   119,   120,   137,   122,   146,   147,   125,   149,   150,
     160,   332,   162,   137,   732,   111,   112,   113,   114,    26,
     116,   117,   118,   119,   120,   121,   107,   107,    26,   125,
     111,   111,    82,     5,     6,     7,     8,     9,    10,    25,
      26,    27,    28,    29,    30,   763,   764,   172,   173,   174,
     153,   154,   341,    69,   202,   203,    12,   770,   347,   415,
      32,    33,    34,    35,    36,    37,   134,   135,   136,   204,
     205,   789,   790,    84,    85,     6,   137,    82,    45,     0,
     200,   794,    40,   204,    67,   198,    47,    52,    82,   147,
     209,    94,   201,   203,    26,   202,   204,    61,    39,   204,
      30,    82,   204,   204,   204,   203,   203,    82,   203,   398,
     203,   203,   532,   203,   388,   203,   203,   203,   203,    82,
     199,   154,   542,   137,   137,    82,   546,    82,    82,    82,
     201,   137,    82,   137,   177,   137,    82,   426,   202,   428,
     429,   137,   431,     5,   154,    82,    82,   137,   437,   137,
     137,    45,   201,   442,    82,    82,    26,    26,    91,    82,
      26,   198,   203,   154,   485,   203,   487,   488,   203,   490,
      32,   203,   203,   203,   203,   137,   203,   498,   203,   203,
     203,   203,   203,   203,   203,   203,   203,    49,    50,    51,
      52,    53,    54,    55,    82,    82,    57,    59,   141,    59,
      62,   201,    64,    65,    66,    67,    68,    82,   137,    82,
     203,    70,     9,   205,    82,    82,   200,    82,   203,   196,
      82,   203,   202,    60,    82,   137,   205,    89,    90,   205,
     137,    93,    94,    95,    96,   205,    82,   206,   206,   177,
      82,   201,    19,   105,   106,    66,   535,   536,    82,   538,
     539,   540,   203,   205,   203,   203,    82,    19,   203,   201,
     207,   205,   551,   552,   553,   554,   205,   556,    82,   204,
     204,    82,   134,   135,   136,    41,    41,   139,   140,   204,
     142,   570,   204,   207,   146,   147,   204,   149,   577,   204,
     152,   153,   205,   201,   205,   205,   617,   719,   203,   203,
      95,    35,   205,   165,   205,   205,   178,   169,   646,   107,
     661,   173,   174,   428,   645,   177,   139,   769,   792,   688,
     484,   322,   577,   613,   107,   401,    -1,   267,    -1,    -1,
      -1,    -1,   194,    -1,    -1,   757,    -1,   263,   627,   628,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   636,    -1,   638,
      -1,    -1,    -1,   642,    -1,   644,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   657,    -1,
      -1,    -1,    -1,    -1,   663,    -1,    -1,     1,    -1,    -1,
      -1,     5,     6,     7,     8,     9,    10,    11,    12,    13,
      14,    15,    16,    17,    18,    -1,    -1,    21,    22,    23,
      24,    25,    -1,    -1,    28,    29,    -1,    31,    32,    33,
      34,    35,    36,    37,    38,    -1,    40,    -1,    -1,    -1,
      -1,    -1,    -1,   712,   713,    49,    50,    51,    -1,    53,
      54,   720,   721,    -1,    -1,    -1,    -1,    -1,    62,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    79,    80,    -1,    82,    83,
      84,    85,    86,    -1,    88,    -1,   755,    -1,    -1,    -1,
      -1,    -1,    96,    97,    98,    99,   100,   101,   102,   103,
     104,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   130,   131,    -1,    -1,
      -1,    -1,    -1,    -1,   138,    -1,    -1,    -1,    -1,   143,
     144,   145,    -1,    -1,    -1,    -1,    -1,   151,    -1,    -1,
      -1,   155,   156,   157,   158,   159,    -1,    -1,    -1,   163,
     164,    -1,   166,   167,   168,    -1,   170,   171,    -1,    -1,
      -1,   175,   176,    -1,   178,   179,   180,   181,   182,   183,
     184,   185,   186,   187,   188,   189,   190,   191,   192,   193,
      -1,   195,    -1,    -1,    -1,   199,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,    18,
      -1,    -1,    21,    22,    23,    24,    25,    -1,    -1,    28,
      29,    -1,    31,    32,    33,    34,    35,    36,    37,    38,
      -1,    40,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      49,    50,    51,    -1,    53,    54,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    62,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      79,    80,    -1,    82,    83,    84,    85,    86,    -1,    88,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    96,    97,    98,
      99,   100,   101,   102,   103,   104,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   130,   131,    -1,    -1,    -1,    -1,    -1,    -1,   138,
      -1,    -1,    -1,    -1,   143,   144,   145,    -1,    -1,    -1,
      -1,    -1,   151,    -1,    -1,    -1,   155,   156,   157,   158,
     159,    -1,    -1,    -1,   163,   164,    -1,   166,   167,   168,
      -1,   170,   171,    -1,    -1,    -1,   175,   176,    -1,   178,
     179,   180,   181,   182,   183,   184,   185,   186,   187,   188,
     189,   190,   191,   192,   193,    -1,   195,    -1,    -1,    -1,
     199,     5,     6,     7,     8,     9,    10,    11,    12,    13,
      14,    15,    16,    17,    18,    -1,    -1,    21,    22,    23,
      24,    25,    -1,    -1,    28,    29,    -1,    31,    32,    33,
      34,    35,    36,    37,    38,    -1,    40,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    49,    50,    51,    -1,    53,
      54,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    62,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    79,    80,    -1,    82,    83,
      84,    85,    86,    -1,    88,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    96,    97,    98,    99,   100,   101,   102,   103,
     104,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   130,   131,    -1,    -1,
      -1,    -1,    -1,    -1,   138,    -1,    -1,    -1,    -1,   143,
     144,   145,    -1,    -1,    -1,    -1,    -1,   151,    -1,    -1,
      -1,   155,   156,   157,   158,   159,    -1,    -1,    -1,   163,
     164,    -1,   166,   167,   168,    -1,   170,   171,    -1,    -1,
      -1,   175,   176,    -1,   178,   179,   180,   181,   182,   183,
     184,   185,   186,   187,   188,   189,   190,   191,   192,   193,
      -1,   195,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    -1,    -1,    21,    22,
      23,    24,    25,    -1,    -1,    28,    29,    -1,    31,    32,
      33,    34,    35,    36,    37,    38,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    49,    50,    51,    -1,
      53,    54,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    62,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    79,    80,    -1,    82,
      83,    84,    85,    86,    -1,    88,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    96,    97,    98,    99,   100,   101,   102,
     103,   104,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   130,   131,    -1,
      -1,    -1,    -1,    -1,    -1,   138,    -1,    -1,    -1,    -1,
     143,   144,   145,    -1,    -1,    -1,    -1,    -1,   151,    -1,
      -1,    -1,   155,   156,   157,   158,   159,    -1,    -1,    -1,
     163,   164,    -1,   166,   167,   168,    -1,   170,   171,    -1,
      -1,    -1,   175,   176,    -1,   178,   179,   180,   181,   182,
     183,   184,   185,   186,   187,   188,   189,   190,   191,   192,
     193,    -1,   195,    49,    50,    51,    52,    53,    54,    55,
      -1,    -1,    -1,    59,    -1,    -1,    62,    -1,    64,    65,
      66,    67,    68,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,    -1,
      -1,    -1,    -1,    89,    90,    -1,    -1,    93,    94,    95,
      96,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   105,
     106,    -1,    -1,    -1,    49,    50,    51,    52,    53,    54,
      55,    -1,    -1,    -1,    59,    -1,    -1,    62,    -1,    64,
      65,    66,    67,    68,    -1,    -1,    -1,    -1,   134,   135,
     136,    -1,    -1,   139,   140,    -1,   142,    82,    -1,    -1,
     146,   147,    -1,   149,    89,    90,   152,   153,    93,    94,
      95,    96,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   165,
     105,   106,    -1,   169,    -1,    -1,    -1,   173,   174,    -1,
      -1,   177,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   194,   134,
     135,   136,    -1,    -1,   139,   140,    -1,   142,   204,    -1,
      -1,   146,   147,    -1,   149,    -1,    -1,   152,   153,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     165,    -1,    -1,    -1,   169,    -1,    -1,    -1,   173,   174,
      -1,    -1,   177,    -1,    -1,    49,    50,    51,    52,    53,
      54,    55,    -1,    -1,    -1,    59,    -1,    -1,    62,   194,
      64,    65,    66,    67,    68,    69,    -1,    -1,    -1,   204,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,    -1,
      -1,    -1,    -1,    -1,    -1,    89,    90,    -1,    -1,    93,
      94,    95,    96,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   105,   106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     134,   135,   136,    -1,    -1,   139,   140,    -1,   142,    -1,
      -1,    -1,   146,   147,    -1,   149,    -1,    -1,   152,   153,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   165,    -1,    -1,    -1,   169,    -1,    -1,    -1,   173,
     174,    -1,    -1,   177,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     194,    -1,    -1,    -1,    -1,    -1,   200,    -1,   202,    49,
      50,    51,    52,    53,    54,    55,    -1,    -1,    -1,    59,
      -1,    -1,    62,    -1,    64,    65,    66,    67,    68,    69,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,    89,
      90,    -1,    -1,    93,    94,    95,    96,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   105,   106,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   134,   135,   136,    -1,    -1,   139,
     140,    -1,   142,    -1,    -1,    -1,   146,   147,    -1,   149,
      -1,    -1,   152,   153,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   165,    -1,    -1,    -1,   169,
      -1,    -1,    -1,   173,   174,    -1,    -1,   177,    -1,    -1,
      49,    50,    51,    52,    53,    54,    55,    -1,    -1,    -1,
      59,    -1,    -1,    62,   194,    64,    65,    66,    67,    68,
     200,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,
      89,    90,    -1,    -1,    93,    94,    95,    96,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   105,   106,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   134,   135,   136,    -1,    -1,
     139,   140,    -1,   142,    -1,    -1,    -1,   146,   147,    -1,
     149,    -1,    -1,   152,   153,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   165,    -1,    -1,    -1,
     169,    -1,    -1,    -1,   173,   174,    -1,    -1,   177,    -1,
      -1,    49,    50,    51,    52,    53,    54,    55,    -1,    -1,
      -1,    59,    -1,    -1,    62,   194,    64,    65,    66,    67,
      68,   200,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,    -1,
      -1,    89,    90,    -1,    -1,    93,    94,    95,    96,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   105,   106,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   134,   135,   136,    -1,
      -1,   139,   140,    -1,   142,    -1,    -1,    -1,   146,   147,
      -1,   149,    -1,    -1,   152,   153,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   165,    -1,    -1,
      -1,   169,    -1,    -1,    -1,   173,   174,    -1,    -1,   177,
      -1,    -1,    49,    50,    51,    52,    53,    54,    55,    -1,
      -1,    -1,    59,    -1,    61,    62,   194,    64,    65,    66,
      67,    68,   200,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,
      -1,    -1,    89,    90,    -1,    -1,    93,    94,    95,    96,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   105,   106,
      -1,    -1,    -1,    49,    50,    51,    52,    53,    54,    55,
      -1,    -1,    -1,    59,   121,    -1,    62,    -1,    64,    65,
      66,    67,    68,    -1,    -1,   132,   133,   134,   135,   136,
     137,    -1,   139,   140,    -1,   142,    82,    -1,    -1,   146,
     147,    -1,   149,    89,    90,   152,   153,    93,    94,    95,
      96,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   165,   105,
     106,    -1,   169,    -1,   110,    -1,   173,   174,    -1,    -1,
     177,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   194,   134,   135,
     136,   137,    -1,   139,   140,    -1,   142,    -1,    -1,    -1,
     146,   147,    -1,   149,    -1,    -1,   152,   153,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   165,
      -1,    -1,    -1,   169,    -1,    -1,    -1,   173,   174,    -1,
      -1,   177,    -1,    -1,    49,    50,    51,    52,    53,    54,
      55,    -1,    -1,    -1,    59,    -1,    -1,    62,   194,    64,
      65,    66,    67,    68,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,
      -1,    -1,    -1,    -1,    89,    90,    -1,    -1,    93,    94,
      95,    96,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     105,   106,    -1,    -1,    -1,    49,    50,    51,    52,    53,
      54,    55,    -1,    -1,    -1,    59,    -1,    -1,    62,    -1,
      64,    65,    66,    67,    68,    -1,    -1,    -1,    -1,   134,
     135,   136,   137,    -1,   139,   140,    -1,   142,    82,    -1,
      -1,   146,   147,    -1,   149,    89,    90,   152,   153,    93,
      94,    95,    96,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     165,   105,   106,    -1,   169,    -1,    -1,    -1,   173,   174,
      -1,    -1,   177,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   194,
     134,   135,   136,    -1,    -1,   139,   140,    -1,   142,    -1,
      -1,    -1,   146,   147,    -1,   149,    -1,    -1,   152,   153,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   165,    -1,    -1,    -1,   169,    -1,    -1,    -1,   173,
     174,    -1,    -1,   177,    -1,    -1,    49,    50,    51,    52,
      53,    54,    55,    -1,    -1,    -1,    59,    -1,    -1,    62,
     194,    64,    65,    66,    67,    68,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,
      -1,    -1,    -1,    -1,    -1,    -1,    89,    90,    -1,    -1,
      93,    94,    95,    96,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   105,   106,    -1,    -1,    -1,    49,    50,    51,
      52,    53,    54,    55,    -1,    -1,    -1,    59,    -1,    -1,
      62,    -1,    64,    65,    66,    67,    68,    -1,    -1,    -1,
      -1,   134,   135,   136,    -1,    -1,   139,   140,    -1,   142,
      82,    -1,    -1,   146,   147,    -1,   149,    89,    90,   152,
     153,    93,    94,    95,    96,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   165,   105,   106,    -1,   169,    -1,    -1,    -1,
     173,   174,    -1,    -1,   177,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   194,   134,   135,   136,    -1,    -1,   139,   140,    -1,
     142,    -1,    -1,    -1,   146,   147,    -1,   149,    -1,    -1,
     152,   153,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   165,    -1,    -1,    -1,   169,    -1,    -1,
      -1,   173,   174,    -1,    -1,   177,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   194
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     1,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    21,    22,    23,    24,
      25,    28,    29,    31,    32,    33,    34,    35,    36,    37,
      38,    40,    49,    50,    51,    53,    54,    62,    79,    80,
      82,    83,    84,    85,    86,    88,    96,    97,    98,    99,
     100,   101,   102,   103,   104,   130,   131,   138,   143,   144,
     145,   151,   155,   156,   157,   158,   159,   163,   164,   166,
     167,   168,   170,   171,   175,   176,   178,   179,   180,   181,
     182,   183,   184,   185,   186,   187,   188,   189,   190,   191,
     192,   193,   195,   199,   211,   212,   213,   214,   215,   216,
     221,   222,   223,   224,   225,   228,   229,   234,   235,   236,
     237,   238,   239,   240,   243,   244,   245,   246,   248,   251,
     252,   253,   254,   255,   256,   257,   258,   259,   260,   261,
     262,   263,   274,   275,   276,   277,   278,   280,   284,   285,
     298,   299,   300,   301,   302,   304,   305,   306,   314,   315,
     317,   321,   323,   324,   325,   327,   329,   330,   331,   332,
     334,   335,   336,   337,   338,   340,   341,   343,   344,   345,
     346,   347,   348,   349,   351,   355,   356,   357,    78,   107,
     108,   109,   200,   279,   148,   303,    19,    66,    68,    90,
      92,    93,   105,   106,   316,   350,    49,    50,    51,    52,
      53,    54,    55,    59,    62,    64,    65,    66,    67,    68,
      82,    89,    90,    93,    94,    95,    96,   105,   106,   134,
     135,   136,   139,   140,   142,   146,   147,   149,   152,   153,
     165,   169,   173,   174,   177,   194,   200,   281,   282,   361,
     362,   105,   106,   264,   362,    19,    20,    48,    61,   226,
      28,    48,   227,   362,   362,    28,    61,   121,   132,   133,
     137,   307,   308,   311,   362,   307,   307,   307,   307,   307,
     307,    81,   111,   112,   113,   114,   115,   116,   117,   118,
     119,   120,   122,   125,   286,    53,   214,    55,    53,    54,
     203,    89,   362,   362,    82,   137,   137,   139,   140,   322,
     146,   147,   149,   150,   328,   146,   154,   326,   148,   152,
     154,   319,   137,   174,   319,   319,    82,   148,   333,    63,
     154,   160,   161,   320,    82,   137,   165,   137,   141,   318,
     137,   339,   169,   319,   137,   342,   172,   173,   174,    26,
      70,    26,   247,    26,   250,   250,    82,    69,   247,   247,
      12,     6,   137,    82,   137,   140,   194,    45,     0,   213,
      45,   199,     5,   200,   216,   230,   232,   233,   263,   274,
     275,   276,   277,   278,   357,   200,   231,   216,   274,   275,
     276,   277,   278,    82,   362,   215,   215,   284,   204,   242,
     267,   268,   269,   216,   270,   271,   359,   360,   362,    67,
     313,   360,   270,   360,    90,    91,   218,   219,   220,   242,
     267,   197,   200,   198,    47,    52,    82,   209,    52,    95,
      94,   147,    64,    65,   362,   362,   200,   362,   201,   203,
     204,   202,    61,   362,    26,    30,   362,   204,   204,   204,
      39,   308,   204,   303,    82,   203,   203,   203,   203,    82,
     111,   112,   113,   114,   116,   117,   118,   119,   120,   121,
     125,   294,   203,   203,   203,   203,   203,   123,   124,   203,
      41,    42,    43,    82,   287,   199,    56,    58,   352,   353,
     354,    82,    82,   137,   137,   137,   319,   137,   148,   319,
     137,   319,   319,    82,   148,   154,    82,   319,   153,   319,
      82,   319,    82,    82,   320,   137,   137,   201,   319,   137,
      82,   177,   362,    82,   154,   249,   249,   202,   362,    82,
      82,   137,   137,   137,   137,    45,    82,    82,   201,   205,
     216,   267,    26,   241,   268,   160,   162,   358,    69,   200,
     202,   362,    26,   358,    91,    82,    26,   198,   250,   203,
     154,   203,   203,   105,   106,   362,   203,   281,   110,   137,
     283,   362,    44,    82,   110,   137,   200,   265,   266,   362,
     362,   137,   134,   135,   136,   312,   137,   309,   310,   362,
     203,    82,    87,   297,   297,   297,   297,   202,   203,   203,
     203,   203,   203,   203,   203,   203,   203,   203,   203,   293,
     297,   297,   295,   297,   295,   297,   203,   203,   297,    82,
      82,    57,    59,   201,   318,   319,   319,   137,   319,   319,
      82,   319,   142,   201,   137,    70,   203,    26,   202,    82,
      82,   203,   200,   205,   205,   270,   204,   362,   204,   362,
     362,   362,   362,   270,   204,   272,    82,     9,   217,   270,
     249,    82,   196,   283,   283,   362,   362,   203,   283,    82,
     200,   201,   205,   202,   362,   205,   205,   310,   205,   296,
     297,   293,   293,   293,   293,    82,   296,   297,   297,   297,
     297,   297,   297,   295,   295,   297,   297,    41,   201,   293,
     293,   293,   293,   293,   206,   206,   293,    60,   354,   319,
     137,   137,   201,   177,    82,   362,   362,    82,   137,    82,
     362,   362,    69,   200,   362,   273,   362,   271,   217,    19,
     203,   203,   283,    82,   266,   362,   205,   293,    41,    41,
      41,    41,   203,   294,    41,    41,    41,    41,    41,   111,
     125,   126,   127,   129,   288,   289,    66,   290,    41,    82,
     203,   205,   205,   362,   362,   201,   205,    19,   360,   283,
     283,    41,   296,   204,   204,   204,   204,   204,   207,   201,
     204,   207,    82,   362,   360,   297,   297,   128,   205,   205,
      82,   291,   292,    41,   288,   295,    41,   205,   205,   203,
     203,   205,   201,   205,   208,   297,   297,   291,   295,   205,
     205
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

    {;}
    break;

  case 38:

    {;}
    break;

  case 39:

    {;}
    break;

  case 40:

    { result->cur_stmt_type_ = OBPROXY_T_OTHERS; ;}
    break;

  case 41:

    { result->is_binlog_related_ = true; ;}
    break;

  case 42:

    {
              result->cur_stmt_type_ = OBPROXY_T_SELECT;
            ;}
    break;

  case 45:

    {
              result->cur_stmt_type_ = OBPROXY_T_LOAD_DATA_INFILE;
            ;}
    break;

  case 46:

    {
              result->cur_stmt_type_ = OBPROXY_T_LOAD_DATA_LOCAL_INFILE;
            ;}
    break;

  case 59:

    { result->cur_stmt_type_ = OBPROXY_T_CREATE; ;}
    break;

  case 60:

    { result->cur_stmt_type_ = OBPROXY_T_DROP; ;}
    break;

  case 61:

    { result->cur_stmt_type_ = OBPROXY_T_ALTER; ;}
    break;

  case 62:

    { result->cur_stmt_type_ = OBPROXY_T_TRUNCATE; ;}
    break;

  case 63:

    { result->cur_stmt_type_ = OBPROXY_T_RENAME; ;}
    break;

  case 64:

    {;}
    break;

  case 65:

    {;}
    break;

  case 67:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_TABLE; ;}
    break;

  case 68:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 69:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 70:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_TABLEGROUP; ;}
    break;

  case 72:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_DROP_TABLEGROUP; ;}
    break;

  case 73:

    {
            SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num));
            result->cur_stmt_type_ = OBPROXY_T_STOP_DDL_TASK;
          ;}
    break;

  case 74:

    {
            SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num));
            result->cur_stmt_type_ = OBPROXY_T_RETRY_DDL_TASK;
          ;}
    break;

  case 75:

    {;}
    break;

  case 76:

    {;}
    break;

  case 77:

    {;}
    break;

  case 78:

    {;}
    break;

  case 79:

    {;}
    break;

  case 80:

    {;}
    break;

  case 81:

    {;}
    break;

  case 82:

    {;}
    break;

  case 83:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(2) - (2)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 84:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(4) - (4)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 85:

    {
                          ObProxyTextPsParseNode *node = NULL;
                          malloc_parse_node(node);
                          node->str_value_ = (yyvsp[(2) - (2)].str);
                          add_text_ps_node(result->text_ps_parse_info_, node);
                        ;}
    break;

  case 88:

    {
                      result->text_ps_inner_stmt_type_ = OBPROXY_T_TEXT_PS_PREPARE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 89:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (2)].str);
                    ;}
    break;

  case 90:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 91:

    {
            ;}
    break;

  case 92:

    {
            ;}
    break;

  case 93:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 94:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 95:

    { result->cur_stmt_type_ = OBPROXY_T_GRANT; ;}
    break;

  case 96:

    { result->cur_stmt_type_ = OBPROXY_T_REVOKE; ;}
    break;

  case 97:

    { result->cur_stmt_type_ = OBPROXY_T_ANALYZE; ;}
    break;

  case 98:

    { result->cur_stmt_type_ = OBPROXY_T_PURGE; ;}
    break;

  case 99:

    { result->cur_stmt_type_ = OBPROXY_T_FLASHBACK; ;}
    break;

  case 100:

    { result->cur_stmt_type_ = OBPROXY_T_COMMENT; ;}
    break;

  case 101:

    { result->cur_stmt_type_ = OBPROXY_T_AUDIT; ;}
    break;

  case 102:

    { result->cur_stmt_type_ = OBPROXY_T_NOAUDIT; ;}
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

  case 113:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_TX_RO; ;}
    break;

  case 117:

    { result->col_name_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 118:

    {;}
    break;

  case 119:

    {;}
    break;

  case 120:

    {;}
    break;

  case 121:

    {;}
    break;

  case 122:

    {;}
    break;

  case 123:

    {;}
    break;

  case 124:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY; ;}
    break;

  case 125:

    {;}
    break;

  case 126:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_DATABASE; ;}
    break;

  case 127:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_PROXY_STATUS; ;}
    break;

  case 128:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DATABASES; ;}
    break;

  case 129:

    {;}
    break;

  case 130:

    {;}
    break;

  case 131:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_COLUMNS; ;}
    break;

  case 132:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_INDEX; ;}
    break;

  case 133:

    {;}
    break;

  case 134:

    {
                      result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
                      result->cur_stmt_type_ = OBPROXY_T_DESC;
                      result->sub_stmt_type_ = OBPROXY_T_SUB_DESC_TABLE;
                  ;}
    break;

  case 135:

    {
            result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
          ;}
    break;

  case 136:

    {
            result->table_info_.table_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.database_name_ = (yyvsp[(4) - (4)].str);
          ;}
    break;

  case 137:

    {
            result->table_info_.database_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.table_name_ = (yyvsp[(4) - (4)].str);
          ;}
    break;

  case 138:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE;
                        result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
                      ;}
    break;

  case 139:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE;
                        result->table_info_.database_name_ = (yyvsp[(2) - (4)].str);
                        result->table_info_.table_name_ = (yyvsp[(4) - (4)].str);
                      ;}
    break;

  case 140:

    {;}
    break;

  case 141:

    { result->table_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 142:

    {;}
    break;

  case 143:

    { result->table_info_.database_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 144:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLES;
                ;}
    break;

  case 145:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_FULL_TABLES;
                ;}
    break;

  case 146:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLE_STATUS;
                      ;}
    break;

  case 147:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DB_VERSION; ;}
    break;

  case 148:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID; ;}
    break;

  case 149:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 150:

    {
                     SET_ICMD_SECOND_STRING((yyvsp[(5) - (5)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 151:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (7)].str));
                     SET_ICMD_SECOND_STRING((yyvsp[(7) - (7)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 152:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_ROUTE_ADDR; ;}
    break;

  case 153:

    {
                              result->cur_stmt_type_ = OBPROXY_T_SET_ROUTE_ADDR;
                              result->cmd_info_.integer_[0] = (yyvsp[(3) - (3)].num);
                           ;}
    break;

  case 154:

    {;}
    break;

  case 155:

    {;}
    break;

  case 156:

    {;}
    break;

  case 157:

    {;}
    break;

  case 158:

    {;}
    break;

  case 159:

    {;}
    break;

  case 161:

    {
                   result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                 ;}
    break;

  case 162:

    {
                   result->table_info_.package_name_ = (yyvsp[(1) - (3)].str);
                   result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                 ;}
    break;

  case 163:

    {
                   result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                   result->table_info_.package_name_ = (yyvsp[(3) - (5)].str);
                   result->table_info_.table_name_ = (yyvsp[(5) - (5)].str);
                 ;}
    break;

  case 164:

    {
                result->call_parse_info_.node_count_ = 0;
              ;}
    break;

  case 165:

    {
                result->call_parse_info_.node_count_ = 0;
                add_call_node(result->call_parse_info_, (yyvsp[(1) - (1)].node));
              ;}
    break;

  case 166:

    {
                add_call_node(result->call_parse_info_, (yyvsp[(3) - (3)].node));
              ;}
    break;

  case 167:

    {
            malloc_call_node((yyval.node), CALL_TOKEN_STR_VAL);
            (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 168:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_INT_VAL);
           (yyval.node)->int_value_ = (yyvsp[(1) - (1)].num);
         ;}
    break;

  case 169:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_NUMBER_VAL);
           (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 170:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_USER_VAR);
           (yyval.node)->str_value_ = (yyvsp[(2) - (2)].str);
         ;}
    break;

  case 171:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_SYS_VAR);
           (yyval.node)->str_value_ = (yyvsp[(3) - (3)].str);
         ;}
    break;

  case 172:

    {
           result->placeholder_list_idx_++;
           malloc_call_node((yyval.node), CALL_TOKEN_PLACE_HOLDER);
           (yyval.node)->placeholder_idx_ = result->placeholder_list_idx_ - 1;
         ;}
    break;

  case 186:

    {
                                                                  handle_stmt_end(result);
                                                                  HANDLE_ACCEPT();
                                                                ;}
    break;

  case 191:

    {
                                                 handle_stmt_end(result);
                                                 HANDLE_ACCEPT();
                                               ;}
    break;

  case 198:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_USER);
        ;}
    break;

  case 199:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 200:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 201:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(5) - (5)].var_node), (yyvsp[(3) - (5)].str), SET_VAR_SYS);
        ;}
    break;

  case 202:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 203:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 204:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_SYS);
        ;}
    break;

  case 205:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_STR);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 206:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_INT);
               (yyval.var_node)->int_value_ = (yyvsp[(1) - (1)].num);
             ;}
    break;

  case 207:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_NUMBER);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 210:

    {;}
    break;

  case 211:

    {;}
    break;

  case 212:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 213:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 214:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 215:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 216:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 217:

    {
              malloc_shard_column_node((yyval.shard_node), (yyvsp[(2) - (7)].str), (yyvsp[(3) - (7)].str), DBMESH_TOKEN_STR_VAL);
              (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (7)].str);
              add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
            ;}
    break;

  case 218:

    { result->trace_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 219:

    { result->rpc_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 220:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 221:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 222:

    {;}
    break;

  case 223:

    {;}
    break;

  case 224:

    { result->target_db_server_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 225:

    {;}
    break;

  case 227:

    { result->has_simple_route_info_ = true; result->simple_route_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 228:

    { result->simple_route_info_.part_key_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 232:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.group_idx_str_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 233:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.table_name_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 234:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 235:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 236:

    { result->dbp_route_info_.sticky_session_ = true; ;}
    break;

  case 237:

    {result->dbp_route_info_.has_shard_key_ = true;;}
    break;

  case 238:

    { result->trace_id_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 239:

    { result->trace_id_ = (yyvsp[(3) - (6)].str); result->rpc_id_ = (yyvsp[(5) - (6)].str); ;}
    break;

  case 240:

    {;}
    break;

  case 242:

    {
                   if (result->dbp_route_info_.shard_key_count_ < OBPROXY_MAX_DBP_SHARD_KEY_NUM) {
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].left_str_ = (yyvsp[(1) - (3)].str);
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].right_str_ = (yyvsp[(3) - (3)].str);
                     ++result->dbp_route_info_.shard_key_count_;
                   }
                 ;}
    break;

  case 245:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 246:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 247:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 248:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 249:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 250:

    { result->trace_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 251:

    { result->rpc_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 252:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 253:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 254:

    { result->has_trace_log_hint_ = true; ;}
    break;

  case 255:

    { result->target_db_server_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 256:

    {
             malloc_shard_column_node((yyval.shard_node), (yyvsp[(1) - (5)].str), (yyvsp[(3) - (5)].str), DBMESH_TOKEN_STR_VAL);
             (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (5)].str);
             add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
           ;}
    break;

  case 257:

    {;}
    break;

  case 258:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 260:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 291:

    { result->query_timeout_ = (yyvsp[(3) - (4)].num); ;}
    break;

  case 293:

    {
      add_hint_index(result->dbmesh_route_info_, (yyvsp[(3) - (5)].str));
      result->dbmesh_route_info_.index_count_++;
    ;}
    break;

  case 294:

    { result->has_trace_log_hint_ = true; ;}
    break;

  case 298:

    {;}
    break;

  case 299:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_WEAK); ;}
    break;

  case 300:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_STRONG); ;}
    break;

  case 301:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_FROZEN); ;}
    break;

  case 304:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_WARNINGS; ;}
    break;

  case 305:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_ERRORS; ;}
    break;

  case 306:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 307:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 308:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_SLAVE_HOSTS; ;}
    break;

  case 309:

    {
            result->is_binlog_related_ = true;
            result->cur_stmt_type_ = OBPROXY_T_SHOW_SLAVE_STATUS;
          ;}
    break;

  case 310:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_RELAYLOG_EVENTS; ;}
    break;

  case 338:

    { result->cur_stmt_type_ = OBPROXY_T_BINLOG_STR; ;}
    break;

  case 339:

    {;}
    break;

  case 340:

    { result->is_binlog_related_ = true; ;}
    break;

  case 341:

    {
;}
    break;

  case 342:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 343:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 344:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 345:

    {;}
    break;

  case 346:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 347:

    {;}
    break;

  case 348:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 350:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 351:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 352:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 353:

    {;}
    break;

  case 354:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_ALL); ;}
    break;

  case 355:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 356:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 357:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 359:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 360:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 361:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 362:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 363:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 365:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 366:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 367:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 368:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 369:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 370:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 371:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 372:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 373:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 374:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_READ_STALE); ;}
    break;

  case 375:

    {;}
    break;

  case 376:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 377:

    {;}
    break;

  case 378:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 379:

    {;}
    break;

  case 381:

    {;}
    break;

  case 382:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 383:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 384:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 385:

    {;}
    break;

  case 386:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 387:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 388:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_GLOBALINDEX); ;}
    break;

  case 389:

    {;}
    break;

  case 390:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 391:

    {;}
    break;

  case 392:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 393:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 394:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); SET_ICMD_ONE_ID((yyvsp[(3) - (3)].num)); ;}
    break;

  case 395:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 396:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 398:

    {;}
    break;

  case 399:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 400:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 401:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 402:

    {;}
    break;

  case 403:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 405:

    {;}
    break;

  case 406:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 407:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 408:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 409:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 410:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 411:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 412:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 413:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 414:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 415:

    {;}
    break;

  case 416:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 417:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 418:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 419:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 420:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 421:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 422:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 425:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 426:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 427:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 428:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 435:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 436:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 438:

    {;}
    break;

  case 439:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 440:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 441:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 442:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 443:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT();
                                                ;}
    break;

  case 444:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 445:

    {
                                                  result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                                                  result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                                                  result->table_info_.dblink_name_ = (yyvsp[(5) - (5)].str);
                                                 ;}
    break;

  case 446:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 447:

    {
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.dblink_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 448:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 449:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 450:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 451:

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

