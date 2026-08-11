
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

#define UNUSED(v) ((void)(v))

#define HANDLE_ACCEPT_FINISH() \
do {\
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
  HANDLE_ACCEPT_FINISH();\
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
     JOIN = 283,
     INNER = 284,
     CROSS = 285,
     FULL = 286,
     LEFT = 287,
     RIGHT = 288,
     OUTER = 289,
     PREPARE = 290,
     EXECUTE = 291,
     USING = 292,
     DEALLOCATE = 293,
     SELECT_HINT_BEGIN = 294,
     UPDATE_HINT_BEGIN = 295,
     DELETE_HINT_BEGIN = 296,
     INSERT_HINT_BEGIN = 297,
     REPLACE_HINT_BEGIN = 298,
     MERGE_HINT_BEGIN = 299,
     LOAD_DATA_HINT_BEGIN = 300,
     HINT_END = 301,
     COMMENT_BEGIN = 302,
     COMMENT_END = 303,
     ROUTE_TABLE = 304,
     ROUTE_PART_KEY = 305,
     PLACE_HOLDER = 306,
     END_P = 307,
     ERROR = 308,
     WHEN = 309,
     TABLEGROUP = 310,
     FLASHBACK = 311,
     AUDIT = 312,
     NOAUDIT = 313,
     STATUS = 314,
     BEGI = 315,
     START = 316,
     TRANSACTION = 317,
     READ = 318,
     ONLY = 319,
     WITH = 320,
     CONSISTENT = 321,
     SNAPSHOT = 322,
     INDEX = 323,
     XA = 324,
     GLOBALINDEX = 325,
     WARNINGS = 326,
     ERRORS = 327,
     TRACE = 328,
     QUICK = 329,
     COUNT = 330,
     AS = 331,
     WHERE = 332,
     VALUES = 333,
     ORDER = 334,
     GROUP = 335,
     HAVING = 336,
     INTO = 337,
     UNION = 338,
     FOR = 339,
     TX_READ_ONLY = 340,
     SELECT_OBPROXY_ROUTE_ADDR = 341,
     SET_OBPROXY_ROUTE_ADDR = 342,
     NAME_OB_DOT = 343,
     NAME_OB = 344,
     EXPLAIN = 345,
     EXPLAIN_ROUTE = 346,
     DESC = 347,
     DESCRIBE = 348,
     NAME_STR = 349,
     USER_VARIABLE = 350,
     SYSTEM_VARIABLE = 351,
     LOAD = 352,
     DATA = 353,
     LOCAL = 354,
     INFILE = 355,
     SLAVE = 356,
     RELAYLOG = 357,
     EVENTS = 358,
     HOSTS = 359,
     BINLOG = 360,
     PORT = 361,
     USE = 362,
     HELP = 363,
     SET_NAMES = 364,
     SET_CHARSET = 365,
     SET_PASSWORD = 366,
     SET_DEFAULT = 367,
     SET_OB_READ_CONSISTENCY = 368,
     SET_TX_READ_ONLY = 369,
     GLOBAL = 370,
     SESSION = 371,
     GLOBAL_ALIAS = 372,
     SESSION_ALIAS = 373,
     MASTER = 374,
     LOGS = 375,
     RESET = 376,
     FLUSH = 377,
     SERVER = 378,
     TENANT = 379,
     NUMBER_VAL = 380,
     GROUP_ID = 381,
     TABLE_ID = 382,
     ELASTIC_ID = 383,
     TESTLOAD = 384,
     ODP_COMMENT = 385,
     TNT_ID = 386,
     DISASTER_STATUS = 387,
     TRACE_ID = 388,
     RPC_ID = 389,
     TARGET_DB_SERVER = 390,
     TRACE_LOG = 391,
     DBP_COMMENT = 392,
     ROUTE_TAG = 393,
     SYS_TAG = 394,
     TABLE_NAME = 395,
     SCAN_ALL = 396,
     STICKY_SESSION = 397,
     PARALL = 398,
     SHARD_KEY = 399,
     STOP_DDL_TASK = 400,
     RETRY_DDL_TASK = 401,
     QUERY_TIMEOUT = 402,
     MAX_EXECUTION_TIME = 403,
     READ_CONSISTENCY = 404,
     WEAK = 405,
     STRONG = 406,
     FROZEN = 407,
     INT_NUM = 408,
     SHOW_PROXYNET = 409,
     THREAD = 410,
     CONNECTION = 411,
     LIMIT = 412,
     OFFSET = 413,
     SHOW_PROCESSLIST = 414,
     SHOW_PROXYSESSION = 415,
     SHOW_GLOBALSESSION = 416,
     ATTRIBUTE = 417,
     VARIABLES = 418,
     ALL = 419,
     STAT = 420,
     READ_STALE = 421,
     SHOW_PROXYCONFIG = 422,
     DIFF = 423,
     USER = 424,
     LIKE = 425,
     SHOW_PROXYSM = 426,
     RPC = 427,
     SHOW_PROXYRPC = 428,
     REQUESTSTAT = 429,
     SHOW_PROXYCLUSTER = 430,
     SHOW_PROXYRESOURCE = 431,
     SHOW_PROXYCONGESTION = 432,
     SHOW_PROXYROUTE = 433,
     PARTITION = 434,
     ROUTINE = 435,
     SUBPARTITION = 436,
     TABLETLS = 437,
     QUERYASYNC = 438,
     RPCCTX = 439,
     SHOW_PROXYVIP = 440,
     SHOW_PROXYMEMORY = 441,
     OBJPOOL = 442,
     SHOW_SQLAUDIT = 443,
     SHOW_WARNLOG = 444,
     SHOW_PROXYSTAT = 445,
     REFRESH = 446,
     SHOW_PROXYTRACE = 447,
     SHOW_PROXYINFO = 448,
     BINARY = 449,
     UPGRADE = 450,
     IDC = 451,
     SHOW_PROXYPS = 452,
     DETAIL = 453,
     SHOW_ELASTIC_ID = 454,
     SHOW_TOPOLOGY = 455,
     GROUP_NAME = 456,
     SHOW_DB_VERSION = 457,
     SHOW_DATABASES = 458,
     SHOW_TABLES = 459,
     SHOW_FULL_TABLES = 460,
     SELECT_DATABASE = 461,
     SELECT_PROXY_STATUS = 462,
     SHOW_CREATE_TABLE = 463,
     SELECT_PROXY_VERSION = 464,
     SHOW_COLUMNS = 465,
     SHOW_INDEX = 466,
     ALTER_PROXYCONFIG = 467,
     ALTER_PROXYRESOURCE = 468,
     PING_PROXY = 469,
     KILL_PROXYSESSION = 470,
     KILL_GLOBALSESSION = 471,
     KILL = 472,
     QUERY = 473,
     BINLOG_VARIABLE = 474,
     BINLOG_USER_VAR = 475,
     BINLOG_SYS_VAR = 476
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
#define YYFINAL  393
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   3826

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  233
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  163
/* YYNRULES -- Number of rules.  */
#define YYNRULES  549
/* YYNRULES -- Number of states.  */
#define YYNSTATES  883

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   476

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,   230,     2,     2,     2,     2,
     226,   227,   232,     2,   223,     2,   224,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   222,
       2,   225,     2,     2,   231,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   228,     2,   229,     2,     2,     2,     2,
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
     195,   196,   197,   198,   199,   200,   201,   202,   203,   204,
     205,   206,   207,   208,   209,   210,   211,   212,   213,   214,
     215,   216,   217,   218,   219,   220,   221
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
      88,    90,    92,    94,    98,    99,   101,   107,   114,   116,
     118,   121,   124,   127,   130,   133,   136,   139,   142,   144,
     146,   149,   152,   154,   156,   158,   160,   162,   163,   165,
     167,   170,   172,   173,   175,   178,   181,   183,   185,   187,
     189,   191,   193,   195,   197,   199,   203,   205,   207,   209,
     213,   216,   220,   223,   226,   230,   234,   236,   238,   240,
     242,   244,   246,   248,   250,   252,   255,   257,   259,   261,
     263,   264,   267,   268,   270,   273,   279,   283,   285,   289,
     291,   293,   295,   297,   299,   301,   304,   306,   308,   310,
     312,   314,   316,   319,   322,   324,   327,   330,   335,   340,
     343,   348,   349,   352,   353,   356,   360,   364,   370,   372,
     374,   378,   384,   392,   394,   398,   400,   402,   404,   406,
     408,   410,   416,   418,   422,   428,   429,   431,   435,   437,
     439,   441,   443,   445,   447,   449,   452,   454,   457,   461,
     465,   467,   469,   471,   472,   476,   478,   482,   486,   492,
     495,   498,   503,   506,   509,   513,   515,   519,   524,   529,
     533,   538,   543,   547,   549,   551,   553,   555,   558,   562,
     568,   575,   582,   589,   596,   603,   611,   618,   625,   632,
     639,   648,   657,   664,   665,   668,   670,   672,   674,   678,
     680,   685,   690,   694,   701,   705,   710,   715,   722,   726,
     728,   732,   733,   737,   741,   745,   749,   753,   757,   761,
     765,   769,   773,   775,   779,   785,   789,   794,   799,   803,
     805,   809,   810,   812,   813,   815,   817,   819,   822,   825,
     829,   834,   836,   839,   841,   844,   846,   849,   852,   856,
     857,   859,   862,   864,   867,   869,   872,   875,   878,   881,
     882,   885,   887,   889,   890,   893,   898,   903,   908,   914,
     916,   921,   923,   925,   927,   929,   930,   932,   934,   936,
     937,   939,   943,   947,   950,   956,   960,   964,   968,   972,
     976,   980,   984,   986,   988,   990,   992,   994,   996,   998,
    1000,  1002,  1004,  1006,  1008,  1010,  1012,  1014,  1016,  1018,
    1020,  1022,  1024,  1026,  1028,  1030,  1032,  1034,  1035,  1037,
    1039,  1041,  1044,  1050,  1056,  1059,  1063,  1067,  1068,  1071,
    1076,  1081,  1082,  1085,  1086,  1089,  1092,  1094,  1096,  1099,
    1102,  1104,  1106,  1110,  1113,  1117,  1121,  1126,  1128,  1131,
    1132,  1135,  1139,  1142,  1145,  1148,  1149,  1152,  1156,  1159,
    1163,  1166,  1170,  1174,  1179,  1182,  1184,  1187,  1190,  1194,
    1197,  1201,  1204,  1207,  1208,  1210,  1212,  1215,  1218,  1222,
    1225,  1228,  1231,  1234,  1237,  1241,  1244,  1246,  1249,  1251,
    1254,  1257,  1261,  1264,  1267,  1270,  1271,  1273,  1277,  1283,
    1286,  1290,  1293,  1294,  1296,  1299,  1302,  1305,  1308,  1313,
    1320,  1321,  1323,  1324,  1326,  1331,  1337,  1343,  1347,  1349,
    1352,  1356,  1360,  1363,  1366,  1370,  1374,  1375,  1378,  1380,
    1384,  1388,  1392,  1393,  1395,  1397,  1401,  1404,  1408,  1411,
    1414,  1416,  1417,  1420,  1425,  1428,  1433,  1436,  1440,  1442,
    1447,  1451,  1454,  1457,  1462,  1466,  1472,  1475,  1480,  1484,
    1489,  1495,  1502,  1504,  1506,  1509,  1512,  1516,  1520,  1524,
    1526,  1527,  1529,  1531,  1533,  1535,  1537,  1539,  1541,  1543,
    1545,  1547,  1549,  1551,  1553,  1555,  1557,  1559,  1561,  1563,
    1565,  1567,  1569,  1571,  1573,  1575,  1577,  1579,  1581,  1583,
    1585,  1587,  1589,  1591,  1593,  1595,  1597,  1599,  1601,  1603,
    1605,  1607,  1609,  1611,  1613,  1615,  1617,  1619,  1621,  1623
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     234,     0,    -1,   235,    -1,     1,    -1,   236,    -1,   235,
     236,    -1,   237,    52,    -1,   237,   222,    -1,   237,   222,
      52,    -1,   222,    -1,   222,    52,    -1,    60,   237,   222,
      -1,   238,    -1,   306,   238,    -1,   239,    -1,   297,    -1,
     302,    -1,   298,    -1,   299,    -1,   300,    -1,   246,    -1,
     245,    -1,   381,    -1,   339,    -1,   268,    -1,   340,    -1,
     385,    -1,   386,    -1,   280,    -1,   281,    -1,   282,    -1,
     283,    -1,   284,    -1,   285,    -1,   286,    -1,   247,    -1,
     259,    -1,   301,    -1,   342,    -1,   244,    -1,   387,    -1,
     322,    -1,   323,    -1,   324,   265,   264,    -1,    -1,     9,
      -1,   100,    89,   240,    19,   390,    -1,    99,   100,    89,
     240,    19,   390,    -1,   242,    -1,   241,    -1,   331,   243,
      -1,   261,   239,    -1,   261,   297,    -1,   261,   299,    -1,
     261,   300,    -1,   261,   298,    -1,   261,   301,    -1,   263,
     238,    -1,   248,    -1,   260,    -1,    14,   249,    -1,    15,
     250,    -1,    16,    -1,    17,    -1,    18,    -1,   251,    -1,
     252,    -1,    -1,    19,    -1,    68,    -1,    20,    68,    -1,
      55,    -1,    -1,    55,    -1,   145,   153,    -1,   146,   153,
      -1,   239,    -1,   297,    -1,   298,    -1,   300,    -1,   299,
      -1,   387,    -1,   286,    -1,   301,    -1,    95,    -1,   254,
     223,    95,    -1,    95,    -1,   255,    -1,   253,    -1,    35,
     395,    26,    -1,    36,   395,    -1,    36,   395,    37,    -1,
     257,   256,    -1,   258,   254,    -1,    15,    35,   395,    -1,
      38,    35,   395,    -1,    21,    -1,    22,    -1,    23,    -1,
      24,    -1,    56,    -1,    25,    -1,    57,    -1,    58,    -1,
     262,    -1,   262,    89,    -1,    90,    -1,    92,    -1,    93,
      -1,    91,    -1,    -1,    26,   293,    -1,    -1,   290,    -1,
       5,    85,    -1,     5,    85,   265,    26,   293,    -1,     5,
      85,   290,    -1,   209,    -1,   209,    76,   395,    -1,   266,
      -1,   267,    -1,   278,    -1,   279,    -1,   269,    -1,   277,
      -1,   200,   270,    -1,   276,    -1,   206,    -1,   207,    -1,
     203,    -1,   274,    -1,   275,    -1,   210,   270,    -1,   211,
     270,    -1,   271,    -1,   262,   395,    -1,    26,   395,    -1,
      26,   395,    26,   395,    -1,    26,   395,   224,   395,    -1,
     208,    89,    -1,   208,    89,   224,    89,    -1,    -1,   170,
      89,    -1,    -1,    26,    89,    -1,   204,   273,   272,    -1,
     205,   273,   272,    -1,    11,    19,    59,   273,   272,    -1,
     202,    -1,   199,    -1,   199,    26,    89,    -1,   199,    77,
     201,   225,    89,    -1,   199,    26,    89,    77,   201,   225,
      89,    -1,    86,    -1,    87,   225,   153,    -1,   109,    -1,
     110,    -1,   111,    -1,   112,    -1,   113,    -1,   114,    -1,
      13,   287,   226,   288,   227,    -1,   395,    -1,   395,   224,
     395,    -1,   395,   224,   395,   224,   395,    -1,    -1,   289,
      -1,   288,   223,   289,    -1,    89,    -1,   153,    -1,   125,
      -1,    95,    -1,    96,    -1,    51,    -1,   291,    -1,   290,
     291,    -1,   292,    -1,   226,   227,    -1,   226,   239,   227,
      -1,   226,   290,   227,    -1,   389,    -1,   294,    -1,   239,
      -1,    -1,   226,   296,   227,    -1,   395,    -1,   296,   223,
     395,    -1,   327,   390,   388,    -1,   327,   390,   388,   295,
     294,    -1,   329,   293,    -1,   325,   293,    -1,   326,   338,
      26,   293,    -1,   330,   390,    -1,    12,   303,    -1,   304,
     223,   303,    -1,   304,    -1,    95,   225,   305,    -1,   117,
     395,   225,   305,    -1,   115,   395,   225,   305,    -1,    96,
     225,   305,    -1,   118,   395,   225,   305,    -1,   116,   395,
     225,   305,    -1,   395,   225,   305,    -1,   395,    -1,   153,
      -1,   125,    -1,   307,    -1,   307,   306,    -1,    47,   308,
      48,    -1,    47,   130,   316,   315,    48,    -1,    47,   127,
     225,   321,   315,    48,    -1,    47,   140,   225,   321,   315,
      48,    -1,    47,   126,   225,   321,   315,    48,    -1,    47,
     128,   225,   321,   315,    48,    -1,    47,   129,   225,   321,
     315,    48,    -1,    47,    88,    89,   225,   320,   315,    48,
      -1,    47,   133,   225,   319,   315,    48,    -1,    47,   134,
     225,   319,   315,    48,    -1,    47,   131,   225,   321,   315,
      48,    -1,    47,   132,   225,   321,   315,    48,    -1,    47,
     137,   138,   225,   228,   310,   229,    48,    -1,    47,   137,
     139,   225,   228,   312,   229,    48,    -1,    47,   135,   225,
     321,   315,    48,    -1,    -1,   308,   309,    -1,   395,    -1,
      52,    -1,     1,    -1,   311,   223,   310,    -1,   311,    -1,
     126,   226,   321,   227,    -1,   140,   226,   321,   227,    -1,
     141,   226,   227,    -1,   141,   226,   143,   225,   321,   227,
      -1,   142,   226,   227,    -1,   144,   226,   313,   227,    -1,
      73,   226,   319,   227,    -1,    73,   226,   319,   230,   319,
     227,    -1,   314,   223,   313,    -1,   314,    -1,    89,   225,
     321,    -1,    -1,   315,   223,   316,    -1,   126,   225,   321,
      -1,   127,   225,   321,    -1,   140,   225,   321,    -1,   128,
     225,   321,    -1,   129,   225,   321,    -1,   133,   225,   319,
      -1,   134,   225,   319,    -1,   131,   225,   321,    -1,   132,
     225,   321,    -1,   136,    -1,   135,   225,   321,    -1,    89,
     224,    89,   225,   320,    -1,    89,   225,   320,    -1,    49,
     226,   395,   227,    -1,    50,   226,   317,   227,    -1,   318,
     223,   317,    -1,   318,    -1,   395,   225,   305,    -1,    -1,
     321,    -1,    -1,   321,    -1,   395,    -1,    94,    -1,     5,
     220,    -1,     5,   221,    -1,     5,   117,   106,    -1,     5,
     231,   231,   106,    -1,     5,    -1,    39,   332,    -1,     8,
      -1,    40,   332,    -1,     6,    -1,    41,   332,    -1,     7,
     328,    -1,    42,   332,   328,    -1,    -1,   164,    -1,   164,
      54,    -1,     9,    -1,    43,   332,    -1,    10,    -1,    44,
     332,    -1,    97,    98,    -1,    45,   332,    -1,   333,    46,
      -1,    -1,   336,   333,    -1,   153,    -1,   395,    -1,    -1,
     334,   335,    -1,   147,   226,   153,   227,    -1,   148,   226,
     153,   227,    -1,   149,   226,   337,   227,    -1,    68,   226,
     395,   395,   227,    -1,   136,    -1,   395,   226,   335,   227,
      -1,   395,    -1,   153,    -1,    52,    -1,     1,    -1,    -1,
     150,    -1,   151,    -1,   152,    -1,    -1,    74,    -1,    11,
     380,    71,    -1,    11,   380,    72,    -1,    11,    73,    -1,
      11,    73,    89,   225,    89,    -1,    11,   101,   104,    -1,
      11,   101,    59,    -1,    11,   102,   103,    -1,    11,   119,
      59,    -1,    11,   194,   120,    -1,    11,   105,   103,    -1,
      11,   119,   120,    -1,   348,    -1,   350,    -1,   351,    -1,
     354,    -1,   352,    -1,   356,    -1,   357,    -1,   358,    -1,
     359,    -1,   361,    -1,   362,    -1,   363,    -1,   364,    -1,
     365,    -1,   367,    -1,   368,    -1,   370,    -1,   346,    -1,
     371,    -1,   374,    -1,   375,    -1,   376,    -1,   377,    -1,
     378,    -1,   379,    -1,    -1,   115,    -1,   116,    -1,    99,
      -1,   105,   395,    -1,    11,   105,   123,    84,   124,    -1,
      11,   341,   163,   170,   219,    -1,   121,   119,    -1,    24,
     194,   120,    -1,   122,   194,   120,    -1,    -1,   157,   153,
      -1,   157,   153,   223,   153,    -1,   157,   153,   158,   153,
      -1,    -1,   170,    89,    -1,    -1,   170,    89,    -1,   173,
     347,    -1,   155,    -1,   174,    -1,   174,    89,    -1,   154,
     349,    -1,   155,    -1,   156,    -1,   156,   153,   343,    -1,
     167,   344,    -1,   167,   164,   344,    -1,   167,   168,   344,
      -1,   167,   168,   169,   344,    -1,   159,    -1,   161,   353,
      -1,    -1,   162,    89,    -1,   162,   170,    89,    -1,   162,
     164,    -1,   170,    89,    -1,   160,   355,    -1,    -1,   162,
     344,    -1,   162,   153,   344,    -1,   165,   344,    -1,   165,
     153,   344,    -1,   163,   344,    -1,   163,   153,   344,    -1,
     163,   164,   344,    -1,   163,   164,   153,   344,    -1,   166,
     344,    -1,   171,    -1,   171,   153,    -1,   171,   172,    -1,
     171,   172,   153,    -1,   175,   344,    -1,   175,   196,   344,
      -1,   176,   344,    -1,   177,   360,    -1,    -1,    89,    -1,
     164,    -1,   164,    89,    -1,   178,   345,    -1,   178,   180,
     345,    -1,   178,   179,    -1,   178,    70,    -1,   178,    55,
      -1,   178,   183,    -1,   178,   182,    -1,   178,   182,   153,
      -1,   178,   184,    -1,   185,    -1,   185,    89,    -1,   186,
      -1,   186,   153,    -1,   186,   187,    -1,   186,   187,   153,
      -1,   188,   343,    -1,   188,   153,    -1,   189,   366,    -1,
      -1,   153,    -1,   153,   223,   153,    -1,   153,   223,   153,
     223,    89,    -1,   190,   344,    -1,   190,   191,   344,    -1,
     192,   369,    -1,    -1,   153,    -1,   153,   153,    -1,   193,
     194,    -1,   193,   195,    -1,   193,   196,    -1,   197,   373,
     344,   372,    -1,   197,   373,   344,   124,   345,   372,    -1,
      -1,   198,    -1,    -1,   153,    -1,   212,    12,    89,   225,
      -1,   212,    12,    89,   225,    89,    -1,   212,    12,    89,
     225,   153,    -1,   213,     6,    89,    -1,   214,    -1,   215,
     153,    -1,   215,   153,   153,    -1,   216,    89,   153,    -1,
     216,    89,    -1,   217,   153,    -1,   217,   156,   153,    -1,
     217,   218,   153,    -1,    -1,    75,   232,    -1,    60,    -1,
      61,    62,   382,    -1,    69,    60,    89,    -1,    69,    61,
      89,    -1,    -1,   383,    -1,   384,    -1,   383,   223,   384,
      -1,    63,    64,    -1,    65,    66,    67,    -1,   107,   395,
      -1,   108,    89,    -1,    89,    -1,    -1,   181,   395,    -1,
     181,   226,   395,   227,    -1,   179,   395,    -1,   179,   226,
     395,   227,    -1,   390,   388,    -1,   390,   388,   391,    -1,
     395,    -1,   395,   224,   395,    95,    -1,   395,   224,   395,
      -1,   395,    95,    -1,   395,   395,    -1,   395,   224,   395,
     395,    -1,   395,    76,   395,    -1,   395,   224,   395,    76,
     395,    -1,   392,   395,    -1,   392,   395,   224,   395,    -1,
     392,   395,   395,    -1,   392,   395,    76,   395,    -1,   392,
     395,   224,   395,   395,    -1,   392,   395,   224,   395,    76,
     395,    -1,   223,    -1,    28,    -1,    29,    28,    -1,    30,
      28,    -1,    31,   393,    28,    -1,    32,   393,    28,    -1,
      33,   393,    28,    -1,    34,    -1,    -1,    61,    -1,    69,
      -1,    60,    -1,    62,    -1,    66,    -1,    72,    -1,    71,
      -1,    75,    -1,    74,    -1,    73,    -1,   155,    -1,   156,
      -1,   158,    -1,   162,    -1,   163,    -1,   165,    -1,   168,
      -1,   169,    -1,   187,    -1,   191,    -1,   195,    -1,   196,
      -1,   218,    -1,   201,    -1,    56,    -1,    57,    -1,    58,
      -1,    99,    -1,    98,    -1,    59,    -1,   150,    -1,   151,
      -1,   152,    -1,   115,    -1,   116,    -1,   104,    -1,   103,
      -1,   102,    -1,   105,    -1,   106,    -1,   119,    -1,   120,
      -1,   121,    -1,   122,    -1,   123,    -1,   124,    -1,   198,
      -1,    89,    -1,   394,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   338,   338,   339,   341,   342,   344,   345,   346,   347,
     348,   349,   351,   352,   354,   355,   356,   357,   358,   359,
     360,   361,   362,   363,   364,   365,   366,   367,   368,   369,
     370,   371,   372,   373,   374,   375,   376,   377,   378,   379,
     380,   382,   383,   384,   389,   391,   393,   398,   403,   404,
     406,   408,   409,   410,   411,   412,   413,   415,   417,   418,
     420,   421,   422,   423,   424,   425,   426,   428,   429,   430,
     431,   432,   434,   435,   437,   443,   449,   450,   451,   452,
     453,   454,   455,   456,   458,   465,   473,   481,   482,   485,
     491,   496,   502,   505,   508,   513,   519,   520,   521,   522,
     523,   524,   525,   526,   528,   529,   531,   532,   533,   535,
     537,   538,   540,   541,   543,   544,   545,   547,   548,   550,
     551,   552,   553,   554,   556,   557,   558,   559,   560,   561,
     562,   563,   564,   565,   566,   567,   574,   578,   583,   589,
     594,   601,   602,   604,   605,   607,   611,   616,   621,   623,
     624,   629,   634,   641,   644,   650,   651,   652,   653,   654,
     655,   658,   660,   664,   669,   677,   680,   685,   690,   695,
     700,   705,   710,   715,   723,   724,   726,   728,   729,   730,
     732,   733,   735,   737,   738,   740,   741,   743,   747,   748,
     749,   750,   751,   756,   758,   759,   761,   765,   769,   773,
     777,   781,   785,   789,   794,   799,   805,   806,   808,   809,
     810,   811,   812,   813,   814,   815,   821,   822,   823,   824,
     825,   826,   827,   829,   830,   834,   835,   836,   838,   839,
     841,   846,   851,   852,   853,   854,   856,   857,   859,   860,
     862,   870,   871,   873,   874,   875,   876,   877,   878,   879,
     880,   881,   882,   883,   884,   890,   891,   892,   894,   895,
     897,   903,   904,   906,   907,   909,   910,   912,   913,   915,
     916,   918,   919,   920,   921,   922,   923,   924,   925,   927,
     928,   929,   931,   932,   933,   934,   935,   936,   938,   939,
     940,   942,   943,   945,   946,   948,   949,   950,   951,   956,
     957,   958,   959,   960,   961,   963,   964,   965,   966,   968,
     969,   972,   973,   974,   975,   976,   977,   982,   983,   984,
     985,   986,   990,   991,   992,   993,   994,   995,   996,   997,
     998,   999,  1000,  1001,  1002,  1003,  1004,  1005,  1006,  1007,
    1008,  1009,  1010,  1011,  1012,  1013,  1014,  1016,  1017,  1018,
    1019,  1022,  1023,  1028,  1029,  1030,  1031,  1036,  1038,  1042,
    1047,  1055,  1056,  1060,  1061,  1064,  1066,  1067,  1068,  1071,
    1073,  1074,  1075,  1079,  1080,  1081,  1082,  1087,  1089,  1091,
    1092,  1093,  1094,  1095,  1098,  1100,  1101,  1102,  1103,  1104,
    1105,  1106,  1107,  1108,  1109,  1113,  1114,  1115,  1116,  1120,
    1121,  1126,  1129,  1131,  1132,  1133,  1134,  1138,  1139,  1140,
    1141,  1142,  1143,  1144,  1145,  1146,  1150,  1151,  1155,  1156,
    1157,  1158,  1162,  1163,  1166,  1168,  1169,  1170,  1171,  1175,
    1176,  1179,  1181,  1182,  1183,  1187,  1188,  1189,  1192,  1193,
    1196,  1197,  1200,  1201,  1205,  1206,  1207,  1211,  1215,  1219,
    1220,  1224,  1225,  1229,  1230,  1231,  1234,  1235,  1238,  1242,
    1243,  1244,  1246,  1247,  1249,  1250,  1253,  1254,  1257,  1263,
    1266,  1268,  1269,  1270,  1271,  1272,  1274,  1278,  1284,  1287,
    1292,  1296,  1300,  1304,  1309,  1313,  1319,  1320,  1325,  1330,
    1335,  1341,  1348,  1349,  1350,  1351,  1352,  1353,  1354,  1356,
    1357,  1359,  1360,  1361,  1362,  1363,  1364,  1365,  1366,  1367,
    1368,  1369,  1370,  1371,  1372,  1373,  1374,  1375,  1376,  1377,
    1378,  1379,  1380,  1381,  1382,  1383,  1384,  1385,  1386,  1387,
    1388,  1389,  1390,  1391,  1392,  1393,  1394,  1395,  1396,  1397,
    1398,  1399,  1400,  1401,  1402,  1403,  1404,  1405,  1407,  1408
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
  "COMMENT", "FROM", "DUAL", "JOIN", "INNER", "CROSS", "FULL", "LEFT",
  "RIGHT", "OUTER", "PREPARE", "EXECUTE", "USING", "DEALLOCATE",
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
  "EXPLAIN_ROUTE", "DESC", "DESCRIBE", "NAME_STR", "USER_VARIABLE",
  "SYSTEM_VARIABLE", "LOAD", "DATA", "LOCAL", "INFILE", "SLAVE",
  "RELAYLOG", "EVENTS", "HOSTS", "BINLOG", "PORT", "USE", "HELP",
  "SET_NAMES", "SET_CHARSET", "SET_PASSWORD", "SET_DEFAULT",
  "SET_OB_READ_CONSISTENCY", "SET_TX_READ_ONLY", "GLOBAL", "SESSION",
  "GLOBAL_ALIAS", "SESSION_ALIAS", "MASTER", "LOGS", "RESET", "FLUSH",
  "SERVER", "TENANT", "NUMBER_VAL", "GROUP_ID", "TABLE_ID", "ELASTIC_ID",
  "TESTLOAD", "ODP_COMMENT", "TNT_ID", "DISASTER_STATUS", "TRACE_ID",
  "RPC_ID", "TARGET_DB_SERVER", "TRACE_LOG", "DBP_COMMENT", "ROUTE_TAG",
  "SYS_TAG", "TABLE_NAME", "SCAN_ALL", "STICKY_SESSION", "PARALL",
  "SHARD_KEY", "STOP_DDL_TASK", "RETRY_DDL_TASK", "QUERY_TIMEOUT",
  "MAX_EXECUTION_TIME", "READ_CONSISTENCY", "WEAK", "STRONG", "FROZEN",
  "INT_NUM", "SHOW_PROXYNET", "THREAD", "CONNECTION", "LIMIT", "OFFSET",
  "SHOW_PROCESSLIST", "SHOW_PROXYSESSION", "SHOW_GLOBALSESSION",
  "ATTRIBUTE", "VARIABLES", "ALL", "STAT", "READ_STALE",
  "SHOW_PROXYCONFIG", "DIFF", "USER", "LIKE", "SHOW_PROXYSM", "RPC",
  "SHOW_PROXYRPC", "REQUESTSTAT", "SHOW_PROXYCLUSTER",
  "SHOW_PROXYRESOURCE", "SHOW_PROXYCONGESTION", "SHOW_PROXYROUTE",
  "PARTITION", "ROUTINE", "SUBPARTITION", "TABLETLS", "QUERYASYNC",
  "RPCCTX", "SHOW_PROXYVIP", "SHOW_PROXYMEMORY", "OBJPOOL",
  "SHOW_SQLAUDIT", "SHOW_WARNLOG", "SHOW_PROXYSTAT", "REFRESH",
  "SHOW_PROXYTRACE", "SHOW_PROXYINFO", "BINARY", "UPGRADE", "IDC",
  "SHOW_PROXYPS", "DETAIL", "SHOW_ELASTIC_ID", "SHOW_TOPOLOGY",
  "GROUP_NAME", "SHOW_DB_VERSION", "SHOW_DATABASES", "SHOW_TABLES",
  "SHOW_FULL_TABLES", "SELECT_DATABASE", "SELECT_PROXY_STATUS",
  "SHOW_CREATE_TABLE", "SELECT_PROXY_VERSION", "SHOW_COLUMNS",
  "SHOW_INDEX", "ALTER_PROXYCONFIG", "ALTER_PROXYRESOURCE", "PING_PROXY",
  "KILL_PROXYSESSION", "KILL_GLOBALSESSION", "KILL", "QUERY",
  "BINLOG_VARIABLE", "BINLOG_USER_VAR", "BINLOG_SYS_VAR", "';'", "','",
  "'.'", "'='", "'('", "')'", "'{'", "'}'", "'#'", "'@'", "'*'", "$accept",
  "root", "sql_stmts", "sql_stmt", "comment_stmt", "stmt", "select_stmt",
  "opt_replace_ignore", "infile_desc", "local_infile_desc",
  "load_infile_desc", "load_data_stmt", "explain_stmt",
  "explain_route_stmt", "ddl_stmt", "mysql_ddl_stmt", "create_dll_expr",
  "drop_ddl_expr", "stop_ddl_task_stmt", "retry_ddl_task_stmt",
  "text_ps_from_stmt", "text_ps_execute_using_var_list",
  "text_ps_prepare_var_list", "text_ps_prepare_args_stmt",
  "text_ps_prepare_stmt", "text_ps_execute_stmt", "text_ps_stmt",
  "oracle_ddl_stmt", "explain_or_desc_stmt", "explain_or_desc",
  "explain_route", "opt_from", "select_expr_list",
  "select_tx_read_only_stmt", "select_proxy_version_stmt", "hooked_stmt",
  "shard_special_stmt", "db_tb_stmt", "show_create_table_stmt",
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
  "part_kv_comment_list", "part_kv_comment", "tracer_right_string_val",
  "name_right_string_val", "right_string_val", "select_with_binlog",
  "select_with_port", "select_with_opt_hint", "update_with_opt_hint",
  "delete_with_opt_hint", "insert_with_opt_hint", "insert_all_when",
  "replace_with_opt_hint", "merge_with_opt_hint", "load_data_opt_hint",
  "hint_list_with_end", "hint_list", "hint_val", "hint_val_list", "hint",
  "opt_read_consistency", "opt_quick", "show_stmt", "icmd_stmt",
  "opt_global_or_session", "binlog_stmt", "opt_limit", "opt_like",
  "opt_large_like", "show_proxyrpc", "opt_show_rpc", "show_proxynet",
  "opt_show_net", "show_proxyconfig", "show_processlist",
  "show_globalsession", "opt_show_global_session", "show_proxysession",
  "opt_show_session", "show_proxysm", "show_proxycluster",
  "show_proxyresource", "show_proxycongestion", "opt_show_congestion",
  "show_proxyroute", "show_proxyvip", "show_proxymemory", "show_sqlaudit",
  "show_warnlog", "opt_show_warnlog", "show_proxystat", "show_proxytrace",
  "opt_show_trace", "show_proxyinfo", "show_proxyps", "opt_detail",
  "opt_int", "alter_proxyconfig", "alter_proxyresource", "ping_proxy",
  "kill_proxysession", "kill_globalsession", "kill_mysql", "opt_count",
  "begin_stmt", "opt_transaction_characteristics",
  "transaction_characteristics", "transaction_characteristic",
  "use_db_stmt", "help_stmt", "other_stmt", "partition_factor",
  "table_references", "table_factor", "join_expr", "join_type",
  "opt_outer", "non_reserved_keyword", "var_name", 0
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
     445,   446,   447,   448,   449,   450,   451,   452,   453,   454,
     455,   456,   457,   458,   459,   460,   461,   462,   463,   464,
     465,   466,   467,   468,   469,   470,   471,   472,   473,   474,
     475,   476,    59,    44,    46,    61,    40,    41,   123,   125,
      35,    64,    42
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   233,   234,   234,   235,   235,   236,   236,   236,   236,
     236,   236,   237,   237,   238,   238,   238,   238,   238,   238,
     238,   238,   238,   238,   238,   238,   238,   238,   238,   238,
     238,   238,   238,   238,   238,   238,   238,   238,   238,   238,
     238,   239,   239,   239,   240,   240,   241,   242,   243,   243,
     244,   245,   245,   245,   245,   245,   245,   246,   247,   247,
     248,   248,   248,   248,   248,   248,   248,   249,   249,   249,
     249,   249,   250,   250,   251,   252,   253,   253,   253,   253,
     253,   253,   253,   253,   254,   254,   255,   256,   256,   257,
     258,   258,   259,   259,   259,   259,   260,   260,   260,   260,
     260,   260,   260,   260,   261,   261,   262,   262,   262,   263,
     264,   264,   265,   265,   266,   266,   266,   267,   267,   268,
     268,   268,   268,   268,   269,   269,   269,   269,   269,   269,
     269,   269,   269,   269,   269,   269,   270,   270,   270,   271,
     271,   272,   272,   273,   273,   274,   274,   275,   276,   277,
     277,   277,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   287,   287,   288,   288,   288,   289,   289,
     289,   289,   289,   289,   290,   290,   291,   292,   292,   292,
     293,   293,   294,   295,   295,   296,   296,   297,   297,   298,
     299,   300,   301,   302,   303,   303,   304,   304,   304,   304,
     304,   304,   304,   305,   305,   305,   306,   306,   307,   307,
     307,   307,   307,   307,   307,   307,   307,   307,   307,   307,
     307,   307,   307,   308,   308,   309,   309,   309,   310,   310,
     311,   311,   311,   311,   311,   311,   312,   312,   313,   313,
     314,   315,   315,   316,   316,   316,   316,   316,   316,   316,
     316,   316,   316,   316,   316,   316,   316,   316,   317,   317,
     318,   319,   319,   320,   320,   321,   321,   322,   322,   323,
     323,   324,   324,   325,   325,   326,   326,   327,   327,   328,
     328,   328,   329,   329,   330,   330,   331,   331,   332,   333,
     333,   334,   334,   335,   335,   336,   336,   336,   336,   336,
     336,   336,   336,   336,   336,   337,   337,   337,   337,   338,
     338,   339,   339,   339,   339,   339,   339,   339,   339,   339,
     339,   339,   340,   340,   340,   340,   340,   340,   340,   340,
     340,   340,   340,   340,   340,   340,   340,   340,   340,   340,
     340,   340,   340,   340,   340,   340,   340,   341,   341,   341,
     341,   342,   342,   342,   342,   342,   342,   343,   343,   343,
     343,   344,   344,   345,   345,   346,   347,   347,   347,   348,
     349,   349,   349,   350,   350,   350,   350,   351,   352,   353,
     353,   353,   353,   353,   354,   355,   355,   355,   355,   355,
     355,   355,   355,   355,   355,   356,   356,   356,   356,   357,
     357,   358,   359,   360,   360,   360,   360,   361,   361,   361,
     361,   361,   361,   361,   361,   361,   362,   362,   363,   363,
     363,   363,   364,   364,   365,   366,   366,   366,   366,   367,
     367,   368,   369,   369,   369,   370,   370,   370,   371,   371,
     372,   372,   373,   373,   374,   374,   374,   375,   376,   377,
     377,   378,   378,   379,   379,   379,   380,   380,   381,   381,
     381,   381,   382,   382,   383,   383,   384,   384,   385,   386,
     387,   388,   388,   388,   388,   388,   389,   389,   390,   390,
     390,   390,   390,   390,   390,   390,   391,   391,   391,   391,
     391,   391,   392,   392,   392,   392,   392,   392,   392,   393,
     393,   394,   394,   394,   394,   394,   394,   394,   394,   394,
     394,   394,   394,   394,   394,   394,   394,   394,   394,   394,
     394,   394,   394,   394,   394,   394,   394,   394,   394,   394,
     394,   394,   394,   394,   394,   394,   394,   394,   394,   394,
     394,   394,   394,   394,   394,   394,   394,   394,   395,   395
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     1,     1,     1,     2,     2,     2,     3,     1,
       2,     3,     1,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     3,     0,     1,     5,     6,     1,     1,
       2,     2,     2,     2,     2,     2,     2,     2,     1,     1,
       2,     2,     1,     1,     1,     1,     1,     0,     1,     1,
       2,     1,     0,     1,     2,     2,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     3,     1,     1,     1,     3,
       2,     3,     2,     2,     3,     3,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     2,     1,     1,     1,     1,
       0,     2,     0,     1,     2,     5,     3,     1,     3,     1,
       1,     1,     1,     1,     1,     2,     1,     1,     1,     1,
       1,     1,     2,     2,     1,     2,     2,     4,     4,     2,
       4,     0,     2,     0,     2,     3,     3,     5,     1,     1,
       3,     5,     7,     1,     3,     1,     1,     1,     1,     1,
       1,     5,     1,     3,     5,     0,     1,     3,     1,     1,
       1,     1,     1,     1,     1,     2,     1,     2,     3,     3,
       1,     1,     1,     0,     3,     1,     3,     3,     5,     2,
       2,     4,     2,     2,     3,     1,     3,     4,     4,     3,
       4,     4,     3,     1,     1,     1,     1,     2,     3,     5,
       6,     6,     6,     6,     6,     7,     6,     6,     6,     6,
       8,     8,     6,     0,     2,     1,     1,     1,     3,     1,
       4,     4,     3,     6,     3,     4,     4,     6,     3,     1,
       3,     0,     3,     3,     3,     3,     3,     3,     3,     3,
       3,     3,     1,     3,     5,     3,     4,     4,     3,     1,
       3,     0,     1,     0,     1,     1,     1,     2,     2,     3,
       4,     1,     2,     1,     2,     1,     2,     2,     3,     0,
       1,     2,     1,     2,     1,     2,     2,     2,     2,     0,
       2,     1,     1,     0,     2,     4,     4,     4,     5,     1,
       4,     1,     1,     1,     1,     0,     1,     1,     1,     0,
       1,     3,     3,     2,     5,     3,     3,     3,     3,     3,
       3,     3,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     0,     1,     1,
       1,     2,     5,     5,     2,     3,     3,     0,     2,     4,
       4,     0,     2,     0,     2,     2,     1,     1,     2,     2,
       1,     1,     3,     2,     3,     3,     4,     1,     2,     0,
       2,     3,     2,     2,     2,     0,     2,     3,     2,     3,
       2,     3,     3,     4,     2,     1,     2,     2,     3,     2,
       3,     2,     2,     0,     1,     1,     2,     2,     3,     2,
       2,     2,     2,     2,     3,     2,     1,     2,     1,     2,
       2,     3,     2,     2,     2,     0,     1,     3,     5,     2,
       3,     2,     0,     1,     2,     2,     2,     2,     4,     6,
       0,     1,     0,     1,     4,     5,     5,     3,     1,     2,
       3,     3,     2,     2,     3,     3,     0,     2,     1,     3,
       3,     3,     0,     1,     1,     3,     2,     3,     2,     2,
       1,     0,     2,     4,     2,     4,     2,     3,     1,     4,
       3,     2,     2,     4,     3,     5,     2,     4,     3,     4,
       5,     6,     1,     1,     2,     2,     3,     3,     3,     1,
       0,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
       0,     3,   271,   275,   279,   273,   282,   284,   456,     0,
       0,    67,    72,    62,    63,    64,    96,    97,    98,    99,
     101,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   223,   100,   102,   103,   458,     0,     0,   153,     0,
     470,   106,   109,   107,   108,     0,     0,     0,     0,   155,
     156,   157,   158,   159,   160,     0,     0,     0,     0,     0,
     377,   385,   379,   361,   395,     0,   361,   361,   403,   363,
     416,   418,   357,   425,   361,   432,     0,   442,   149,     0,
     148,   129,   143,   143,   127,   128,     0,   117,     0,     0,
       0,     0,   448,     0,     0,     0,     9,     0,     2,     4,
       0,    12,    14,    39,    21,    20,    35,    58,    65,    66,
       0,     0,    36,    59,     0,   104,     0,   119,   120,    24,
     123,   134,   130,   131,   126,   124,   121,   122,    28,    29,
      30,    31,    32,    33,    34,    15,    17,    18,    19,    37,
      16,     0,   206,    41,    42,   112,     0,   309,     0,     0,
       0,     0,    23,    25,    38,   339,   322,   323,   324,   326,
     325,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   340,   341,   342,   343,   344,   345,   346,
      22,    26,    27,    40,   114,     0,   267,   268,     0,   280,
     277,     0,   313,     0,   350,     0,     0,     0,   348,   349,
       0,     0,     0,     0,   525,   526,   527,   530,   503,   501,
     504,   505,   502,   507,   506,   510,   509,   508,   548,     0,
       0,   529,   528,   538,   537,   536,   539,   540,   534,   535,
       0,     0,   541,   542,   543,   544,   545,   546,   531,   532,
     533,   511,   512,   513,   514,   515,   516,   517,   518,   519,
     520,   521,   522,   547,   524,   523,   193,   195,   549,     0,
     534,   535,     0,   162,    68,     0,    71,    69,    60,     0,
      73,    61,     0,     0,    90,     0,   304,   303,     0,   299,
       0,     0,     0,   302,   272,     0,     0,   301,   274,   276,
     279,   283,   285,   287,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   458,     0,
     462,     0,     0,     0,   286,   351,   468,   469,   354,     0,
      74,    75,   370,   371,   369,   361,   361,   361,   361,   384,
       0,     0,   378,   361,   361,     0,   373,   396,   397,   366,
     367,   365,   361,   399,   401,   404,   405,   402,   411,   410,
       0,   409,   363,   413,   412,   415,   407,   417,   419,   420,
     423,     0,   422,   426,   424,   361,   429,   433,   431,   435,
     436,   437,   443,   361,     0,     0,     0,   125,     0,   141,
     141,   139,     0,   132,   133,     0,     0,   449,   452,   453,
       0,     0,    10,     1,     5,     6,     7,   271,    86,    76,
      88,    87,    92,    82,    77,    78,    80,    79,    83,    81,
      84,    93,    51,    52,    55,    53,    54,    56,   105,   135,
      57,    13,   207,     0,   110,   113,   174,   176,   182,   190,
     181,   180,   471,   478,   310,     0,   471,   189,   192,     0,
       0,    49,    48,    50,     0,   116,   269,     0,   281,   143,
       0,   457,   316,   315,   317,   320,     0,   318,   321,   319,
       0,   311,   312,     0,     0,     0,     0,     0,     0,     0,
       0,   165,     0,    70,    94,   355,    89,    91,    95,     0,
       0,     0,   305,   288,   290,   293,   278,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   252,     0,   241,     0,     0,   261,
     261,     0,     0,     0,     0,   227,   208,   226,   224,   225,
      11,     0,     0,   459,   463,   464,   460,   461,   154,   356,
     357,   361,   386,   361,   361,   390,   361,   388,   394,   380,
     382,     0,   383,   374,   361,   375,   362,   398,   368,   400,
     406,   364,   408,   414,   421,   358,     0,   430,   434,   440,
     150,     0,   136,   144,     0,   145,   146,     0,   118,     0,
     447,   450,   451,   454,   455,     8,     0,   177,     0,     0,
       0,    43,   175,     0,     0,   476,     0,   481,     0,   482,
       0,   183,     0,    44,     0,   270,   141,     0,     0,     0,
     205,   204,   196,   203,   199,     0,     0,     0,     0,   194,
     202,   173,   168,   171,   172,   170,   169,     0,   166,   163,
       0,     0,     0,   306,   307,   308,     0,   291,   293,     0,
     292,   263,   266,   241,   265,   241,   241,   241,     0,     0,
       0,   263,     0,     0,     0,     0,     0,     0,   261,   261,
       0,     0,     0,   241,   241,   241,   262,   241,   241,     0,
       0,   241,   466,     0,     0,   372,   387,   391,   361,   392,
     389,   381,   376,     0,     0,   427,   363,   441,   438,     0,
       0,     0,     0,   142,   140,   444,    85,   178,   179,   111,
       0,   474,     0,   472,   493,     0,     0,   500,   500,   500,
     492,   477,     0,   484,   480,   191,     0,     0,    44,    45,
       0,   115,   147,   314,   352,   353,   198,   201,   197,   200,
       0,   161,     0,     0,   295,   296,   297,   294,   300,   241,
     264,     0,     0,     0,     0,     0,     0,   259,     0,     0,
     255,   243,   244,   246,   247,   250,   251,   248,   249,   253,
     245,   209,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   467,   465,   393,   360,   359,     0,   440,     0,   151,
     137,   138,   445,   446,     0,     0,   494,   495,   499,     0,
       0,     0,   486,     0,   479,   483,     0,   185,   188,     0,
       0,   167,   164,   298,     0,   212,   210,   213,   214,   256,
     257,     0,     0,   263,   242,   218,   219,   216,   217,   222,
       0,     0,     0,     0,     0,     0,   229,     0,     0,   211,
     428,   439,     0,   475,   473,   496,   497,   498,     0,     0,
     488,   485,     0,   184,     0,    46,   215,   258,   260,   254,
       0,     0,     0,     0,     0,     0,     0,   261,     0,   152,
     489,   487,   186,    47,     0,     0,     0,   232,   234,     0,
       0,   239,   220,   228,     0,   221,     0,   490,   230,   231,
       0,     0,   235,     0,   236,   261,   491,     0,   240,   238,
       0,   233,   237
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    97,    98,    99,   100,   101,   428,   710,   441,   442,
     443,   103,   104,   105,   106,   107,   268,   271,   108,   109,
     400,   411,   401,   402,   110,   111,   112,   113,   114,   115,
     116,   581,   424,   117,   118,   119,   120,   377,   121,   565,
     379,   122,   123,   124,   125,   126,   127,   128,   129,   130,
     131,   132,   133,   134,   262,   617,   618,   425,   426,   427,
     429,   430,   707,   786,   135,   136,   137,   138,   139,   140,
     256,   257,   602,   141,   142,   307,   518,   815,   816,   818,
     860,   861,   652,   506,   736,   737,   655,   729,   656,   143,
     144,   145,   146,   147,   148,   190,   149,   150,   151,   284,
     285,   628,   629,   286,   626,   435,   152,   153,   202,   154,
     362,   336,   356,   155,   341,   156,   324,   157,   158,   159,
     332,   160,   329,   161,   162,   163,   164,   347,   165,   166,
     167,   168,   169,   364,   170,   171,   368,   172,   173,   678,
     373,   174,   175,   176,   177,   178,   179,   203,   180,   523,
     524,   525,   181,   182,   183,   585,   431,   432,   701,   702,
     779,   258,   634
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -603
static const yytype_int16 yypact[] =
{
     693,  -603,   -18,  -603,   -79,  -603,  -603,  -603,    13,  2572,
    3460,   105,    43,  -603,  -603,  -603,  -603,  -603,  -603,   -37,
    -603,  3460,  3460,    82,  1084,  1084,  1084,  1084,  1084,  1084,
    1084,   283,  -603,  -603,  -603,  1599,   102,    36,  -603,   -56,
    -603,  -603,  -603,  -603,  -603,   116,  3460,  3460,   145,  -603,
    -603,  -603,  -603,  -603,  -603,   107,    62,    87,   114,    30,
    -603,    27,   -75,    -3,   -46,    -2,   -94,   100,   -21,    53,
     185,   -84,    59,   142,   -78,   144,    10,   157,    54,   263,
    -603,  -603,   295,   295,  -603,  -603,   245,   259,   263,   263,
     324,   331,  -603,   186,   249,   -74,   289,   343,   911,  -603,
      23,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
     310,   250,  -603,  -603,   319,  3608,  1812,  -603,  -603,  -603,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,  1812,   301,  -603,  -603,   129,  1401,   282,  3460,  1401,
    3460,   181,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,  -603,  -603,  -603,     3,   251,  -603,  -603,   133,   311,
    -603,   307,   279,   138,  -603,    32,   271,    35,  -603,  -603,
      22,   255,   213,   229,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,   158,
     165,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  3460,  3460,
    3460,  3460,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,   168,  -603,   167,
    -603,  -603,   169,   172,  -603,   326,  -603,  -603,  -603,  3460,
    -603,  -603,   277,   372,   363,  3460,  -603,  -603,   175,  -603,
     176,   177,   178,  -603,  -603,   360,  1084,   193,  -603,  -603,
     -79,  -603,  -603,  -603,   332,   197,   200,   201,   202,   253,
     203,   205,   206,   207,   208,   166,   209,  1247,  -603,   215,
      46,   346,   349,   286,  -603,  -603,  -603,  -603,  -603,   320,
    -603,  -603,  -603,   288,  -603,   -69,   -70,   -60,   100,  -603,
     -16,   353,  -603,   100,   137,   354,  -603,  -603,   291,  -603,
     356,  -603,   100,  -603,  -603,  -603,   358,  -603,  -603,  -603,
     359,  -603,   281,   296,  -603,  -603,  -603,  -603,  -603,   299,
    -603,   300,  -603,   233,  -603,   100,  -603,   304,  -603,  -603,
    -603,  -603,  -603,   100,   369,   258,  3460,  -603,   373,   294,
     294,   241,  3460,  -603,  -603,   377,   378,   315,   316,  -603,
     318,   321,  -603,  -603,  -603,  -603,   420,   -65,  -603,  -603,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,   252,  -603,  -603,  -603,  -603,  -603,  -603,    25,  -603,
    -603,  -603,  -603,    17,   447,   129,  -603,  -603,  -603,  -603,
    -603,  -603,    69,  2270,  -603,   451,    69,  -603,  -603,   379,
     389,  -603,  -603,  -603,   454,     4,  -603,   375,  -603,   295,
     257,  -603,  -603,  -603,  -603,  -603,   399,  -603,  -603,  -603,
     314,  -603,  -603,  2720,  2720,   260,   261,   262,   264,  2572,
    2720,    38,  3460,  -603,  -603,  -603,  -603,  -603,  -603,  3460,
     339,   341,   136,  -603,  -603,  3016,  -603,   270,  3164,  3164,
    3164,  3164,   272,   273,    84,   275,   278,   280,   284,   285,
     287,   290,   292,   293,  -603,   297,  -603,  3164,  3164,  3164,
    3164,  3164,   298,   302,  3164,  -603,  -603,  -603,  -603,  -603,
    -603,   432,   431,  -603,   303,  -603,  -603,  -603,  -603,  -603,
     345,   100,  -603,   100,    24,  -603,   100,  -603,  -603,  -603,
    -603,   415,  -603,  -603,   100,  -603,  -603,  -603,  -603,  -603,
    -603,  -603,  -603,  -603,  -603,   -88,   355,  -603,  -603,   -77,
     429,   305,     7,  -603,   418,  -603,  -603,   422,  -603,   306,
    -603,  -603,  -603,  -603,  -603,  -603,   419,  -603,   308,    85,
    1401,  -603,  -603,  1974,  2122,    29,  3460,  -603,  3460,  -603,
    1401,    20,   424,   507,  1401,  -603,   294,   430,   396,   309,
    -603,  -603,  -603,  -603,  -603,  2720,  2720,  2720,  2720,  -603,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,   -53,  -603,   312,
    3460,   313,   317,  -603,  -603,  -603,   322,  -603,  3016,   323,
    -603,  3164,  -603,  -603,  -603,  -603,  -603,  -603,  3460,  3460,
     435,  3164,  3164,  3164,  3164,  3164,  3164,  3164,  3164,  3164,
    3164,  3164,   -14,  -603,  -603,  -603,  -603,  -603,  -603,   325,
     327,  -603,  -603,   458,    46,  -603,  -603,  -603,   100,  -603,
    -603,  -603,  -603,   368,   376,   328,   281,  -603,  -603,   333,
     443,  3460,  3460,  -603,  -603,   -34,  -603,  -603,  -603,  -603,
    3460,  -603,  3460,  -603,  -603,   505,   509,   504,   504,   504,
    -603,  -603,  3460,  -603,  2868,  -603,  3460,    66,   507,  -603,
     520,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
      38,  -603,  3460,   329,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,   -13,   -12,    -8,    -6,   330,   334,   335,   337,   338,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,  -603,   253,    -5,     1,     2,     5,    15,   152,   468,
      16,  -603,  -603,  -603,  -603,  -603,   453,   347,   340,  -603,
    -603,  -603,  -603,  -603,   342,   351,  -603,  -603,  -603,   515,
     518,   519,  2424,  3460,  -603,  -603,   -52,  -603,  -603,   529,
    3460,  -603,  -603,  -603,    18,  -603,  -603,  -603,  -603,  -603,
    -603,  3460,  2720,  3164,  -603,  -603,  -603,  -603,  -603,  -603,
     344,   350,   357,   361,   362,   364,   336,   365,   366,  -603,
    -603,  -603,   463,  -603,  -603,  -603,  -603,  -603,  3460,  3460,
    -603,  -603,  3460,  -603,  3460,  -603,  -603,  -603,  -603,  -603,
    3164,  3164,   -97,   367,   465,   512,   152,  3164,   516,  -603,
    -603,  3312,  -603,  -603,   374,   380,   348,  -603,  -603,   381,
     382,   385,  -603,  -603,   -68,  -603,  3460,  -603,  -603,  -603,
    3164,  3164,  -603,   465,  -603,  3164,  -603,   383,  -603,  -603,
     386,  -603,  -603
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -603,  -603,  -603,   469,   531,   -51,     6,  -140,  -603,  -603,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,  -603,   388,  -603,  -603,  -603,  -603,   225,  -603,  -369,
     -80,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,  -603,  -603,   470,  -603,  -603,  -138,  -174,  -371,  -603,
    -144,  -123,  -603,  -603,   147,   148,   149,   154,   155,  -603,
     117,  -603,  -456,   448,  -603,  -603,  -603,  -257,  -603,  -603,
    -281,  -603,  -382,  -152,  -199,  -603,  -503,  -602,  -463,  -603,
    -603,  -603,  -603,  -603,  -603,   352,  -603,  -603,  -603,   170,
     370,  -603,   -25,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
      74,   -43,  -343,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -162,
    -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,  -603,
    -603,   -50,  -603,  -603,   502,   179,  -603,  -146,  -603,  -603,
    -367,  -603,    -9
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -549
static const yytype_int16 yytable[] =
{
     259,   263,   436,   380,   438,   437,   102,   657,   604,   552,
     445,   566,   273,   274,   610,   287,   287,   287,   287,   287,
     287,   287,   397,   343,   344,   633,   635,   636,   637,  -112,
    -113,   366,   191,   681,   751,   795,   796,   315,   316,   740,
     797,   102,   798,   805,   653,   654,   856,   676,   658,   806,
     807,   661,   185,   808,   582,   772,    24,   694,   695,   696,
     697,   698,   699,   809,   819,   420,   836,   184,   345,   358,
     673,   397,  -187,   539,   582,   395,   335,  -548,   269,   389,
     374,   457,   390,   533,   531,   189,   192,   330,   193,   611,
     421,   452,   335,   536,   534,   331,   311,   312,   270,   185,
     335,   335,   342,   359,   102,    24,   419,   337,   348,   521,
     335,   522,   194,   365,   195,   196,   399,   275,   197,   773,
     412,   677,   102,   349,   264,   265,   338,   612,   198,   199,
     857,   375,   200,   613,   614,   674,   453,   433,   455,   433,
     433,   433,   458,   346,   391,   747,   748,   102,   540,   716,
     717,   718,   719,   339,   541,   186,   187,   272,   456,   874,
     266,   333,   875,   615,   310,   334,   188,   335,   730,   313,
     720,   832,   340,   267,   721,   833,  -347,   668,   730,   741,
     742,   743,   744,   745,   746,   322,   323,   749,   750,   325,
     326,   616,   327,   328,   335,   288,   289,   290,   291,   292,
     293,   839,   186,   187,   369,   370,   371,   201,   582,   752,
     752,   752,   360,   188,   314,   752,   361,   752,   752,   465,
     466,   467,   468,   350,   752,   752,   318,   712,   752,   423,
     423,   682,   351,   352,   317,   353,   354,   355,   752,   752,
     320,   752,  -187,   423,   577,   396,   706,  -548,   583,   579,
     584,   731,   700,   732,   733,   734,   319,   404,   405,   406,
     474,   413,   414,   415,   407,   408,   478,   321,   416,   417,
     335,   753,   754,   755,   357,   756,   757,   287,   810,   760,
     439,   440,   532,   535,   537,   538,   623,   624,   625,   376,
     543,   545,   811,   812,   813,   363,   814,   367,   519,   549,
     461,   462,   492,   493,   512,   513,   544,   335,   640,   641,
     372,   423,   688,   383,   384,   397,     3,     4,     5,     6,
       7,   378,   557,    10,   397,     3,     4,     5,     6,     7,
     559,   780,   781,   767,   381,   382,   385,   386,   388,   387,
     730,   392,   494,   393,   864,   410,   838,   794,    31,    24,
      25,    26,    27,    28,    29,   423,   434,   446,    24,    25,
      26,    27,    28,    29,   447,   448,   449,   562,   450,   596,
     451,   294,   880,   568,   454,   459,   460,   854,   855,   495,
     496,   497,   498,   463,   499,   500,   501,   502,   503,   504,
     464,   469,   470,   505,   473,   471,   472,   475,   476,    40,
     477,   479,   480,   481,   482,   398,   483,   877,   878,   295,
     296,   297,   298,   299,   300,   301,   302,   303,   304,   485,
     305,   487,   488,   306,   589,   489,   490,   491,   507,   578,
     508,   509,   510,   511,   514,   526,   689,   520,   527,   528,
     529,   530,   542,   546,   547,   548,   705,   550,   551,   553,
     711,   350,   554,   555,   603,   603,   556,   558,   560,   561,
     259,   603,   563,   619,   564,   567,   569,   570,   571,   572,
     620,   573,   575,   580,   574,   576,   630,   590,   593,   592,
     594,   595,   597,   598,   599,   605,   606,   607,   666,   608,
     667,   669,   621,   670,   622,   631,   662,   663,   638,   639,
     642,   672,   361,   643,   671,   644,   679,   683,   675,   645,
     646,   684,   647,   708,   686,   648,   709,   649,   650,   713,
     714,   764,   651,   659,   739,   761,   664,   660,   715,   765,
     680,   685,   769,   776,   768,   687,   722,   777,   778,   790,
     724,   817,   820,   825,   725,   677,   826,   827,   834,   726,
     728,   766,   849,   758,   859,   759,   793,   799,   801,   846,
     862,   800,   802,   803,   865,   822,   309,   394,   789,   823,
     840,   433,   444,   870,   691,   693,   841,   703,   824,   704,
     403,   433,   791,   842,   788,   433,   609,   843,   844,   863,
     422,   847,   879,   845,   858,   848,   603,   603,   603,   603,
     804,   868,   837,   727,   665,   821,   871,   869,   873,   872,
     881,   723,   409,   882,   762,   591,     0,     0,     0,   630,
       0,     0,     0,     0,     0,   763,     0,     0,     0,   735,
     738,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   486,     0,   835,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   484,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   770,   771,     0,     0,     0,     0,     0,     0,
       0,   774,     0,   775,     0,     0,     0,     0,   853,     0,
       0,     0,     0,   782,     1,   785,     0,   787,     2,     3,
       4,     5,     6,     7,     8,     9,    10,    11,    12,    13,
      14,    15,     0,   792,    16,    17,    18,    19,    20,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    21,    22,
       0,    23,    24,    25,    26,    27,    28,    29,    30,     0,
      31,     0,     0,     0,     0,     0,     0,     0,     0,    32,
      33,    34,     0,    35,    36,     0,     0,     0,     0,     0,
       0,     0,    37,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   830,   831,     0,     0,     0,     0,    38,
      39,   433,    40,    41,    42,    43,    44,     0,     0,     0,
      45,     0,   738,   603,     0,     0,     0,     0,    46,     0,
      47,    48,    49,    50,    51,    52,    53,    54,     0,     0,
       0,     0,     0,     0,    55,    56,     0,     0,     0,   850,
     851,     0,     0,   852,     0,   433,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    57,    58,
       0,     0,   867,     0,     0,     0,     0,    59,     0,     0,
       0,     0,    60,    61,    62,     0,     0,   876,     0,     0,
      63,     0,     0,     0,    64,     0,    65,     0,    66,    67,
      68,    69,     0,     0,     0,     0,     0,     0,    70,    71,
       0,    72,    73,    74,     0,    75,    76,     0,     0,     0,
      77,     0,    78,    79,     0,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,     0,     0,     0,     0,    96,     2,     3,     4,     5,
       6,     7,     8,     9,    10,    11,    12,    13,    14,    15,
       0,     0,    16,    17,    18,    19,    20,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    21,    22,     0,    23,
      24,    25,    26,    27,    28,    29,    30,     0,    31,     0,
       0,     0,     0,     0,     0,     0,     0,    32,    33,    34,
       0,    35,    36,     0,     0,     0,     0,     0,     0,     0,
      37,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    38,    39,     0,
      40,    41,    42,    43,    44,     0,     0,     0,    45,     0,
       0,     0,     0,     0,     0,     0,    46,     0,    47,    48,
      49,    50,    51,    52,    53,    54,     0,     0,     0,     0,
       0,     0,    55,    56,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    57,    58,     0,     0,
       0,     0,     0,     0,     0,    59,     0,     0,     0,     0,
      60,    61,    62,     0,     0,     0,     0,     0,    63,     0,
       0,     0,    64,     0,    65,   276,    66,    67,    68,    69,
       0,     0,     0,     0,     0,     0,    70,    71,     0,    72,
      73,    74,     0,    75,    76,     0,     0,     0,    77,     0,
      78,    79,     0,    80,    81,    82,    83,    84,    85,    86,
      87,    88,    89,    90,    91,    92,    93,    94,    95,     0,
    -289,     0,     0,    96,     0,     0,   277,     0,     0,     0,
     204,   205,   206,   207,   208,   209,   210,     0,     0,     0,
     211,     0,   278,   212,     0,   213,   214,   215,   216,   217,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   218,     0,     0,     0,     0,     0,     0,
       0,     0,   221,   222,     0,     0,   223,   224,   225,   226,
     227,     0,     0,     0,     0,     0,     0,     0,     0,   260,
     261,     0,     0,   232,   233,   234,   235,   236,   237,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     279,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   280,   281,   282,   238,   239,   240,   283,     0,   241,
     242,     0,   243,     0,     0,     0,   244,   245,   515,   246,
       0,     0,   247,   248,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   249,     0,     0,     0,   250,     0,     0,     0,   251,
     252,     0,   253,     0,     0,   254,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   516,     0,     0,     0,   517,
       0,     0,   255,   204,   205,   206,   207,   208,   209,   210,
       0,     0,     0,   211,     0,     0,   212,     0,   213,   214,
     215,   216,   217,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   218,     0,     0,     0,
       0,     0,     0,     0,     0,   221,   222,     0,     0,   223,
     224,   225,   226,   227,     0,     0,     0,     0,     0,     0,
       0,     0,   260,   261,     0,     0,   232,   233,   234,   235,
     236,   237,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   238,   239,   240,
       0,     0,   241,   242,     0,   243,   397,     0,     0,   244,
     245,     0,   246,     0,     0,   247,   248,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   249,     0,     0,     0,   250,     0,
      24,     0,   251,   252,     0,   253,     0,     0,   254,     0,
       0,     0,     0,     0,     0,     0,     0,   204,   205,   206,
     207,   208,   209,   210,     0,   255,     0,   211,     0,     0,
     212,     0,   213,   214,   215,   216,   217,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     218,     0,     0,     0,     0,     0,     0,     0,     0,   221,
     222,     0,     0,   223,   224,   225,   226,   227,     0,     0,
       0,     0,     0,     0,     0,     0,   260,   261,     0,     0,
     232,   233,   234,   235,   236,   237,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   238,   239,   240,     0,     0,   241,   242,     0,   243,
       0,     0,     0,   244,   245,     0,   246,     0,     0,   247,
     248,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   249,     0,
       0,     0,   250,     0,     0,     0,   251,   252,     0,   253,
       0,     0,   254,     0,     2,     3,     4,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,     0,   255,
      16,    17,    18,    19,    20,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    21,    22,     0,    23,    24,    25,
      26,    27,    28,    29,    30,     0,    31,     0,     0,     0,
       0,     0,     0,     0,     0,    32,    33,    34,     0,   308,
      36,     0,     0,     0,     0,     0,     0,     0,    37,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    38,    39,     0,    40,    41,
      42,    43,    44,     0,     0,     0,    45,     0,     0,     0,
       0,     0,     0,     0,    46,     0,    47,    48,    49,    50,
      51,    52,    53,    54,     0,     0,     0,     0,     0,     0,
      55,    56,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    57,    58,     0,     0,     0,     0,
       0,     0,     0,    59,     0,     0,     0,     0,    60,    61,
      62,     0,     0,     0,     0,     0,    63,     0,     0,     0,
      64,     0,    65,     0,    66,    67,    68,    69,     0,     0,
       0,     0,     0,     0,    70,    71,     0,    72,    73,    74,
       0,    75,    76,     0,     0,     0,    77,     0,    78,    79,
       0,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,     0,     0,    16,    17,    18,    19,    20,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    21,    22,     0,
      23,    24,    25,    26,    27,    28,    29,    30,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    32,    33,
      34,     0,   308,    36,     0,     0,     0,     0,     0,     0,
       0,    37,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    38,    39,
       0,    40,    41,    42,    43,    44,     0,     0,     0,    45,
       0,     0,     0,     0,     0,     0,     0,    46,     0,    47,
      48,    49,    50,    51,    52,    53,    54,     0,     0,     0,
       0,     0,     0,    55,    56,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    57,    58,     0,
       0,     0,     0,     0,     0,     0,    59,     0,     0,     0,
       0,    60,    61,    62,     0,     0,     0,     0,     0,    63,
       0,     0,     0,    64,     0,    65,     0,    66,    67,    68,
      69,     0,     0,     0,     0,     0,     0,    70,    71,     0,
      72,    73,    74,     0,    75,    76,     0,     0,     0,    77,
       0,    78,    79,     0,    80,    81,    82,    83,    84,    85,
      86,    87,    88,    89,    90,    91,    92,    93,    94,    95,
     204,   205,   206,   207,   208,   209,   210,     0,     0,     0,
     211,     0,     0,   212,     0,   213,   214,   215,   216,   217,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   218,     0,     0,     0,     0,     0,     0,
       0,     0,   221,   222,     0,     0,   223,   224,   225,   226,
     227,     0,     0,     0,     0,     0,     0,     0,     0,   260,
     261,     0,     0,   232,   233,   234,   235,   236,   237,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   238,   239,   240,     0,     0,   241,
     242,     0,   243,     0,     0,     0,   244,   245,     0,   246,
       0,     0,   247,   248,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   249,     0,     0,     0,   250,     0,     0,     0,   251,
     252,     0,   253,     0,     0,   254,     0,     0,   204,   205,
     206,   207,   208,   209,   210,     0,     0,     0,   211,     0,
       0,   212,   255,   213,   214,   215,   216,   217,     0,     0,
     690,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   218,     0,     0,     0,     0,     0,     0,     0,     0,
     221,   222,     0,     0,   223,   224,   225,   226,   227,     0,
       0,     0,     0,     0,     0,     0,     0,   260,   261,     0,
       0,   232,   233,   234,   235,   236,   237,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   238,   239,   240,     0,     0,   241,   242,     0,
     243,     0,     0,     0,   244,   245,     0,   246,     0,     0,
     247,   248,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   249,
       0,     0,     0,   250,     0,     0,     0,   251,   252,     0,
     253,     0,     0,   254,     0,     0,   204,   205,   206,   207,
     208,   209,   210,     0,     0,     0,   211,     0,     0,   212,
     255,   213,   214,   215,   216,   217,   586,     0,   692,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   218,
       0,     0,     0,     0,     0,   587,     0,     0,   221,   222,
       0,     0,   223,   224,   225,   226,   227,     0,     0,     0,
       0,     0,     0,     0,     0,   260,   261,     0,     0,   232,
     233,   234,   235,   236,   237,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     238,   239,   240,     0,     0,   241,   242,     0,   243,     0,
       0,     0,   244,   245,     0,   246,     0,     0,   247,   248,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   249,     0,     0,
       0,   250,     0,     0,     0,   251,   252,     0,   253,     0,
       0,   254,     0,     0,     0,     0,     0,     0,     0,     0,
     204,   205,   206,   207,   208,   209,   210,     0,   255,     0,
     211,     0,     0,   212,   588,   213,   214,   215,   216,   217,
     828,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   218,     0,     0,     0,     0,     0,     0,
       0,     0,   221,   222,     0,     0,   223,   224,   225,   226,
     227,     0,     0,     0,     0,     0,     0,     0,     0,   260,
     261,     0,     0,   232,   233,   234,   235,   236,   237,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   238,   239,   240,     0,     0,   241,
     242,     0,   243,     0,     0,     0,   244,   245,     0,   246,
       0,     0,   247,   248,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   249,     0,     0,     0,   250,     0,     0,     0,   251,
     252,     0,   253,     0,     0,   254,     0,     0,   204,   205,
     206,   207,   208,   209,   210,     0,     0,     0,   211,     0,
       0,   212,   255,   213,   214,   215,   216,   217,   829,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   218,     0,     0,     0,     0,     0,   219,   220,     0,
     221,   222,     0,     0,   223,   224,   225,   226,   227,     0,
       0,     0,     0,     0,     0,     0,     0,   228,   229,   230,
     231,   232,   233,   234,   235,   236,   237,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   238,   239,   240,     0,     0,   241,   242,     0,
     243,     0,     0,     0,   244,   245,     0,   246,     0,     0,
     247,   248,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   249,
       0,     0,     0,   250,     0,     0,     0,   251,   252,     0,
     253,     0,     0,   254,     0,     0,   204,   205,   206,   207,
     208,   209,   210,     0,     0,     0,   211,     0,     0,   212,
     255,   213,   214,   215,   216,   217,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   218,
       0,     0,     0,     0,     0,     0,     0,     0,   221,   222,
       0,     0,   223,   224,   225,   226,   227,     0,     0,     0,
       0,     0,     0,     0,     0,   260,   261,     0,     0,   232,
     233,   234,   235,   236,   237,   600,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     238,   239,   240,   601,     0,   241,   242,     0,   243,     0,
       0,     0,   244,   245,     0,   246,     0,     0,   247,   248,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   249,     0,     0,
       0,   250,     0,     0,     0,   251,   252,     0,   253,     0,
       0,   254,     0,     0,   204,   205,   206,   207,   208,   209,
     210,     0,     0,     0,   211,     0,     0,   212,   255,   213,
     214,   215,   216,   217,   783,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   218,     0,     0,
       0,     0,     0,   784,     0,     0,   221,   222,     0,     0,
     223,   224,   225,   226,   227,     0,     0,     0,     0,     0,
       0,     0,     0,   260,   261,     0,     0,   232,   233,   234,
     235,   236,   237,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   238,   239,
     240,     0,     0,   241,   242,     0,   243,     0,     0,     0,
     244,   245,     0,   246,     0,     0,   247,   248,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   249,     0,     0,     0,   250,
       0,     0,     0,   251,   252,     0,   253,     0,     0,   254,
       0,     0,   204,   205,   206,   207,   208,   209,   210,     0,
       0,     0,   211,     0,     0,   212,   255,   213,   214,   215,
     216,   217,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   218,     0,     0,     0,     0,
       0,     0,     0,     0,   221,   222,     0,     0,   223,   224,
     225,   226,   227,     0,     0,     0,     0,     0,     0,     0,
       0,   260,   261,     0,     0,   232,   233,   234,   235,   236,
     237,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   238,   239,   240,   627,
       0,   241,   242,     0,   243,     0,     0,     0,   244,   245,
       0,   246,     0,     0,   247,   248,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   249,     0,     0,     0,   250,     0,     0,
       0,   251,   252,     0,   253,     0,     0,   254,     0,     0,
     204,   205,   206,   207,   208,   209,   210,     0,     0,     0,
     211,     0,     0,   212,   255,   213,   214,   215,   216,   217,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   218,     0,     0,     0,     0,   632,     0,
       0,     0,   221,   222,     0,     0,   223,   224,   225,   226,
     227,     0,     0,     0,     0,     0,     0,     0,     0,   260,
     261,     0,     0,   232,   233,   234,   235,   236,   237,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   238,   239,   240,     0,     0,   241,
     242,     0,   243,     0,     0,     0,   244,   245,     0,   246,
       0,     0,   247,   248,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   249,     0,     0,     0,   250,     0,     0,     0,   251,
     252,     0,   253,     0,     0,   254,     0,     0,   204,   205,
     206,   207,   208,   209,   210,     0,     0,     0,   211,     0,
       0,   212,   255,   213,   214,   215,   216,   217,   866,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   218,     0,     0,     0,     0,     0,     0,     0,     0,
     221,   222,     0,     0,   223,   224,   225,   226,   227,     0,
       0,     0,     0,     0,     0,     0,     0,   260,   261,     0,
       0,   232,   233,   234,   235,   236,   237,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   238,   239,   240,     0,     0,   241,   242,     0,
     243,     0,     0,     0,   244,   245,     0,   246,     0,     0,
     247,   248,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   249,
       0,     0,     0,   250,     0,     0,     0,   251,   252,     0,
     253,     0,     0,   254,     0,     0,   204,   205,   206,   207,
     208,   209,   210,     0,     0,     0,   211,     0,     0,   212,
     255,   213,   214,   215,   216,   217,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   218,
       0,     0,     0,     0,     0,     0,     0,     0,   221,   222,
       0,     0,   223,   224,   225,   226,   227,     0,     0,     0,
       0,     0,     0,     0,     0,   260,   261,     0,     0,   232,
     233,   234,   235,   236,   237,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     238,   239,   240,     0,     0,   241,   242,     0,   243,     0,
       0,     0,   244,   245,     0,   246,     0,     0,   247,   248,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   249,     0,     0,
       0,   250,     0,     0,     0,   251,   252,     0,   253,     0,
       0,   254,     0,     0,   204,   205,   206,   207,   208,   209,
     210,     0,     0,     0,   211,     0,     0,   212,   255,   213,
     214,   215,   216,   217,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   418,     0,     0,
       0,     0,     0,     0,     0,     0,   221,   222,     0,     0,
     223,   224,   225,   226,   227,     0,     0,     0,     0,     0,
       0,     0,     0,   260,   261,     0,     0,   232,   233,   234,
     235,   236,   237,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   238,   239,
     240,     0,     0,   241,   242,     0,   243,     0,     0,     0,
     244,   245,     0,   246,     0,     0,   247,   248,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   249,     0,     0,     0,   250,
       0,     0,     0,   251,   252,     0,   253,     0,     0,   254,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   255
};

static const yytype_int16 yycheck[] =
{
       9,    10,   148,    83,   150,   149,     0,   510,   464,   352,
     184,   380,    21,    22,   470,    24,    25,    26,    27,    28,
      29,    30,     5,    66,    67,   488,   489,   490,   491,    26,
      26,    74,    19,    26,    48,    48,    48,    46,    47,   641,
      48,    35,    48,    48,   507,   508,   143,   124,   511,    48,
      48,   514,   117,    48,   425,    89,    39,    28,    29,    30,
      31,    32,    33,    48,    48,   116,    48,    85,    89,   153,
     158,     5,    52,    89,   445,    52,   170,    52,    35,   153,
      26,    59,   156,   153,   153,   164,    73,   162,    75,    51,
     141,    59,   170,   153,   164,   170,    60,    61,    55,   117,
     170,   170,   196,   187,    98,    39,   115,   153,    55,    63,
     170,    65,    99,   191,   101,   102,   110,    35,   105,   153,
     114,   198,   116,    70,    19,    20,   172,    89,   115,   116,
     227,    77,   119,    95,    96,   223,   104,   146,   103,   148,
     149,   150,   120,   164,   218,   648,   649,   141,   164,   605,
     606,   607,   608,   155,   170,   220,   221,   194,   123,   227,
      55,   164,   230,   125,    62,   168,   231,   170,   631,   225,
     223,   223,   174,    68,   227,   227,   163,   153,   641,   642,
     643,   644,   645,   646,   647,   155,   156,   650,   651,   162,
     163,   153,   165,   166,   170,    25,    26,    27,    28,    29,
      30,   803,   220,   221,   194,   195,   196,   194,   579,   223,
     223,   223,   153,   231,    98,   223,   157,   223,   223,   228,
     229,   230,   231,   170,   223,   223,   119,   596,   223,   226,
     226,   224,   179,   180,    89,   182,   183,   184,   223,   223,
     153,   223,   222,   226,   227,   222,   226,   222,   179,   423,
     181,   633,   223,   635,   636,   637,   194,   110,   110,   110,
     269,   114,   114,   114,   110,   110,   275,   153,   114,   114,
     170,   653,   654,   655,    89,   657,   658,   286,   126,   661,
      99,   100,   325,   326,   327,   328,   150,   151,   152,    26,
     333,   334,   140,   141,   142,   153,   144,   153,   307,   342,
      71,    72,    49,    50,   138,   139,   169,   170,   224,   225,
     153,   226,   227,    88,    89,     5,     6,     7,     8,     9,
      10,    26,   365,    13,     5,     6,     7,     8,     9,    10,
     373,   698,   699,   676,    89,    76,    12,     6,    89,   153,
     803,    52,    89,     0,   847,    95,   802,   729,    47,    39,
      40,    41,    42,    43,    44,   226,    74,   106,    39,    40,
      41,    42,    43,    44,   231,    54,    59,   376,    89,   449,
     232,    88,   875,   382,   103,   120,   163,   840,   841,   126,
     127,   128,   129,   225,   131,   132,   133,   134,   135,   136,
     225,   223,   225,   140,    68,   226,   224,   120,    26,    89,
      37,   226,   226,   226,   226,    95,    46,   870,   871,   126,
     127,   128,   129,   130,   131,   132,   133,   134,   135,   226,
     137,    89,   225,   140,   433,   225,   225,   225,   225,   423,
     225,   225,   225,   225,   225,    89,   580,   222,    89,   153,
     120,   153,    89,    89,   153,    89,   590,    89,    89,   153,
     594,   170,   153,   153,   463,   464,   223,   153,    89,   201,
     469,   470,    89,   472,   170,   224,    89,    89,   153,   153,
     479,   153,    52,    26,   153,   223,   485,    26,    89,   100,
      26,   106,   225,    84,   170,   225,   225,   225,   531,   225,
     533,   534,   153,   536,   153,   225,    64,    66,   226,   226,
     225,   544,   157,   225,    89,   225,    77,    89,   153,   225,
     225,    89,   225,    89,    95,   225,     9,   225,   225,    89,
     124,   153,   225,   225,    89,    67,   223,   225,   219,   153,
     225,   225,    89,    28,   201,   227,   224,    28,    34,    19,
     227,    73,    89,    28,   227,   198,    28,    28,    19,   227,
     227,   223,    89,   228,    89,   228,   227,   227,   223,   223,
      48,   227,   225,   225,    48,   225,    35,    98,   708,   227,
     226,   580,   184,   225,   583,   584,   226,   586,   227,   588,
     110,   590,   720,   226,   707,   594,   469,   226,   226,   846,
     142,   226,   873,   229,   227,   229,   605,   606,   607,   608,
     752,   227,   801,   628,   530,   767,   225,   227,   223,   227,
     227,   620,   110,   227,   664,   436,    -1,    -1,    -1,   628,
      -1,    -1,    -1,    -1,    -1,   668,    -1,    -1,    -1,   638,
     639,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   290,    -1,   790,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   286,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   681,   682,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   690,    -1,   692,    -1,    -1,    -1,    -1,   834,    -1,
      -1,    -1,    -1,   702,     1,   704,    -1,   706,     5,     6,
       7,     8,     9,    10,    11,    12,    13,    14,    15,    16,
      17,    18,    -1,   722,    21,    22,    23,    24,    25,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    35,    36,
      -1,    38,    39,    40,    41,    42,    43,    44,    45,    -1,
      47,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    56,
      57,    58,    -1,    60,    61,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    69,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   782,   783,    -1,    -1,    -1,    -1,    86,
      87,   790,    89,    90,    91,    92,    93,    -1,    -1,    -1,
      97,    -1,   801,   802,    -1,    -1,    -1,    -1,   105,    -1,
     107,   108,   109,   110,   111,   112,   113,   114,    -1,    -1,
      -1,    -1,    -1,    -1,   121,   122,    -1,    -1,    -1,   828,
     829,    -1,    -1,   832,    -1,   834,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   145,   146,
      -1,    -1,   851,    -1,    -1,    -1,    -1,   154,    -1,    -1,
      -1,    -1,   159,   160,   161,    -1,    -1,   866,    -1,    -1,
     167,    -1,    -1,    -1,   171,    -1,   173,    -1,   175,   176,
     177,   178,    -1,    -1,    -1,    -1,    -1,    -1,   185,   186,
      -1,   188,   189,   190,    -1,   192,   193,    -1,    -1,    -1,
     197,    -1,   199,   200,    -1,   202,   203,   204,   205,   206,
     207,   208,   209,   210,   211,   212,   213,   214,   215,   216,
     217,    -1,    -1,    -1,    -1,   222,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,    18,
      -1,    -1,    21,    22,    23,    24,    25,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    35,    36,    -1,    38,
      39,    40,    41,    42,    43,    44,    45,    -1,    47,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    56,    57,    58,
      -1,    60,    61,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      69,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    86,    87,    -1,
      89,    90,    91,    92,    93,    -1,    -1,    -1,    97,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   105,    -1,   107,   108,
     109,   110,   111,   112,   113,   114,    -1,    -1,    -1,    -1,
      -1,    -1,   121,   122,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   145,   146,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   154,    -1,    -1,    -1,    -1,
     159,   160,   161,    -1,    -1,    -1,    -1,    -1,   167,    -1,
      -1,    -1,   171,    -1,   173,     1,   175,   176,   177,   178,
      -1,    -1,    -1,    -1,    -1,    -1,   185,   186,    -1,   188,
     189,   190,    -1,   192,   193,    -1,    -1,    -1,   197,    -1,
     199,   200,    -1,   202,   203,   204,   205,   206,   207,   208,
     209,   210,   211,   212,   213,   214,   215,   216,   217,    -1,
      46,    -1,    -1,   222,    -1,    -1,    52,    -1,    -1,    -1,
      56,    57,    58,    59,    60,    61,    62,    -1,    -1,    -1,
      66,    -1,    68,    69,    -1,    71,    72,    73,    74,    75,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    89,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    98,    99,    -1,    -1,   102,   103,   104,   105,
     106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   115,
     116,    -1,    -1,   119,   120,   121,   122,   123,   124,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     136,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   147,   148,   149,   150,   151,   152,   153,    -1,   155,
     156,    -1,   158,    -1,    -1,    -1,   162,   163,     1,   165,
      -1,    -1,   168,   169,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   187,    -1,    -1,    -1,   191,    -1,    -1,    -1,   195,
     196,    -1,   198,    -1,    -1,   201,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    48,    -1,    -1,    -1,    52,
      -1,    -1,   218,    56,    57,    58,    59,    60,    61,    62,
      -1,    -1,    -1,    66,    -1,    -1,    69,    -1,    71,    72,
      73,    74,    75,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    89,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    98,    99,    -1,    -1,   102,
     103,   104,   105,   106,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   115,   116,    -1,    -1,   119,   120,   121,   122,
     123,   124,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   150,   151,   152,
      -1,    -1,   155,   156,    -1,   158,     5,    -1,    -1,   162,
     163,    -1,   165,    -1,    -1,   168,   169,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   187,    -1,    -1,    -1,   191,    -1,
      39,    -1,   195,   196,    -1,   198,    -1,    -1,   201,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    56,    57,    58,
      59,    60,    61,    62,    -1,   218,    -1,    66,    -1,    -1,
      69,    -1,    71,    72,    73,    74,    75,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      89,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    98,
      99,    -1,    -1,   102,   103,   104,   105,   106,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   115,   116,    -1,    -1,
     119,   120,   121,   122,   123,   124,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   150,   151,   152,    -1,    -1,   155,   156,    -1,   158,
      -1,    -1,    -1,   162,   163,    -1,   165,    -1,    -1,   168,
     169,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   187,    -1,
      -1,    -1,   191,    -1,    -1,    -1,   195,   196,    -1,   198,
      -1,    -1,   201,    -1,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,    16,    17,    18,    -1,   218,
      21,    22,    23,    24,    25,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    35,    36,    -1,    38,    39,    40,
      41,    42,    43,    44,    45,    -1,    47,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    56,    57,    58,    -1,    60,
      61,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    69,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    86,    87,    -1,    89,    90,
      91,    92,    93,    -1,    -1,    -1,    97,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   105,    -1,   107,   108,   109,   110,
     111,   112,   113,   114,    -1,    -1,    -1,    -1,    -1,    -1,
     121,   122,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   145,   146,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   154,    -1,    -1,    -1,    -1,   159,   160,
     161,    -1,    -1,    -1,    -1,    -1,   167,    -1,    -1,    -1,
     171,    -1,   173,    -1,   175,   176,   177,   178,    -1,    -1,
      -1,    -1,    -1,    -1,   185,   186,    -1,   188,   189,   190,
      -1,   192,   193,    -1,    -1,    -1,   197,    -1,   199,   200,
      -1,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,    16,    17,
      18,    -1,    -1,    21,    22,    23,    24,    25,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    35,    36,    -1,
      38,    39,    40,    41,    42,    43,    44,    45,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    56,    57,
      58,    -1,    60,    61,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    69,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    86,    87,
      -1,    89,    90,    91,    92,    93,    -1,    -1,    -1,    97,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   105,    -1,   107,
     108,   109,   110,   111,   112,   113,   114,    -1,    -1,    -1,
      -1,    -1,    -1,   121,   122,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   145,   146,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   154,    -1,    -1,    -1,
      -1,   159,   160,   161,    -1,    -1,    -1,    -1,    -1,   167,
      -1,    -1,    -1,   171,    -1,   173,    -1,   175,   176,   177,
     178,    -1,    -1,    -1,    -1,    -1,    -1,   185,   186,    -1,
     188,   189,   190,    -1,   192,   193,    -1,    -1,    -1,   197,
      -1,   199,   200,    -1,   202,   203,   204,   205,   206,   207,
     208,   209,   210,   211,   212,   213,   214,   215,   216,   217,
      56,    57,    58,    59,    60,    61,    62,    -1,    -1,    -1,
      66,    -1,    -1,    69,    -1,    71,    72,    73,    74,    75,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    89,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    98,    99,    -1,    -1,   102,   103,   104,   105,
     106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   115,
     116,    -1,    -1,   119,   120,   121,   122,   123,   124,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   150,   151,   152,    -1,    -1,   155,
     156,    -1,   158,    -1,    -1,    -1,   162,   163,    -1,   165,
      -1,    -1,   168,   169,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   187,    -1,    -1,    -1,   191,    -1,    -1,    -1,   195,
     196,    -1,   198,    -1,    -1,   201,    -1,    -1,    56,    57,
      58,    59,    60,    61,    62,    -1,    -1,    -1,    66,    -1,
      -1,    69,   218,    71,    72,    73,    74,    75,    -1,    -1,
     226,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    89,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      98,    99,    -1,    -1,   102,   103,   104,   105,   106,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   115,   116,    -1,
      -1,   119,   120,   121,   122,   123,   124,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   150,   151,   152,    -1,    -1,   155,   156,    -1,
     158,    -1,    -1,    -1,   162,   163,    -1,   165,    -1,    -1,
     168,   169,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   187,
      -1,    -1,    -1,   191,    -1,    -1,    -1,   195,   196,    -1,
     198,    -1,    -1,   201,    -1,    -1,    56,    57,    58,    59,
      60,    61,    62,    -1,    -1,    -1,    66,    -1,    -1,    69,
     218,    71,    72,    73,    74,    75,    76,    -1,   226,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    89,
      -1,    -1,    -1,    -1,    -1,    95,    -1,    -1,    98,    99,
      -1,    -1,   102,   103,   104,   105,   106,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   115,   116,    -1,    -1,   119,
     120,   121,   122,   123,   124,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     150,   151,   152,    -1,    -1,   155,   156,    -1,   158,    -1,
      -1,    -1,   162,   163,    -1,   165,    -1,    -1,   168,   169,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   187,    -1,    -1,
      -1,   191,    -1,    -1,    -1,   195,   196,    -1,   198,    -1,
      -1,   201,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      56,    57,    58,    59,    60,    61,    62,    -1,   218,    -1,
      66,    -1,    -1,    69,   224,    71,    72,    73,    74,    75,
      76,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    89,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    98,    99,    -1,    -1,   102,   103,   104,   105,
     106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   115,
     116,    -1,    -1,   119,   120,   121,   122,   123,   124,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   150,   151,   152,    -1,    -1,   155,
     156,    -1,   158,    -1,    -1,    -1,   162,   163,    -1,   165,
      -1,    -1,   168,   169,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   187,    -1,    -1,    -1,   191,    -1,    -1,    -1,   195,
     196,    -1,   198,    -1,    -1,   201,    -1,    -1,    56,    57,
      58,    59,    60,    61,    62,    -1,    -1,    -1,    66,    -1,
      -1,    69,   218,    71,    72,    73,    74,    75,   224,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    89,    -1,    -1,    -1,    -1,    -1,    95,    96,    -1,
      98,    99,    -1,    -1,   102,   103,   104,   105,   106,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   115,   116,   117,
     118,   119,   120,   121,   122,   123,   124,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   150,   151,   152,    -1,    -1,   155,   156,    -1,
     158,    -1,    -1,    -1,   162,   163,    -1,   165,    -1,    -1,
     168,   169,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   187,
      -1,    -1,    -1,   191,    -1,    -1,    -1,   195,   196,    -1,
     198,    -1,    -1,   201,    -1,    -1,    56,    57,    58,    59,
      60,    61,    62,    -1,    -1,    -1,    66,    -1,    -1,    69,
     218,    71,    72,    73,    74,    75,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    89,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    98,    99,
      -1,    -1,   102,   103,   104,   105,   106,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   115,   116,    -1,    -1,   119,
     120,   121,   122,   123,   124,   125,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     150,   151,   152,   153,    -1,   155,   156,    -1,   158,    -1,
      -1,    -1,   162,   163,    -1,   165,    -1,    -1,   168,   169,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   187,    -1,    -1,
      -1,   191,    -1,    -1,    -1,   195,   196,    -1,   198,    -1,
      -1,   201,    -1,    -1,    56,    57,    58,    59,    60,    61,
      62,    -1,    -1,    -1,    66,    -1,    -1,    69,   218,    71,
      72,    73,    74,    75,    76,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    89,    -1,    -1,
      -1,    -1,    -1,    95,    -1,    -1,    98,    99,    -1,    -1,
     102,   103,   104,   105,   106,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   115,   116,    -1,    -1,   119,   120,   121,
     122,   123,   124,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   150,   151,
     152,    -1,    -1,   155,   156,    -1,   158,    -1,    -1,    -1,
     162,   163,    -1,   165,    -1,    -1,   168,   169,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   187,    -1,    -1,    -1,   191,
      -1,    -1,    -1,   195,   196,    -1,   198,    -1,    -1,   201,
      -1,    -1,    56,    57,    58,    59,    60,    61,    62,    -1,
      -1,    -1,    66,    -1,    -1,    69,   218,    71,    72,    73,
      74,    75,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    89,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    98,    99,    -1,    -1,   102,   103,
     104,   105,   106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   115,   116,    -1,    -1,   119,   120,   121,   122,   123,
     124,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   150,   151,   152,   153,
      -1,   155,   156,    -1,   158,    -1,    -1,    -1,   162,   163,
      -1,   165,    -1,    -1,   168,   169,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   187,    -1,    -1,    -1,   191,    -1,    -1,
      -1,   195,   196,    -1,   198,    -1,    -1,   201,    -1,    -1,
      56,    57,    58,    59,    60,    61,    62,    -1,    -1,    -1,
      66,    -1,    -1,    69,   218,    71,    72,    73,    74,    75,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    89,    -1,    -1,    -1,    -1,    94,    -1,
      -1,    -1,    98,    99,    -1,    -1,   102,   103,   104,   105,
     106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   115,
     116,    -1,    -1,   119,   120,   121,   122,   123,   124,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   150,   151,   152,    -1,    -1,   155,
     156,    -1,   158,    -1,    -1,    -1,   162,   163,    -1,   165,
      -1,    -1,   168,   169,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   187,    -1,    -1,    -1,   191,    -1,    -1,    -1,   195,
     196,    -1,   198,    -1,    -1,   201,    -1,    -1,    56,    57,
      58,    59,    60,    61,    62,    -1,    -1,    -1,    66,    -1,
      -1,    69,   218,    71,    72,    73,    74,    75,    76,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    89,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      98,    99,    -1,    -1,   102,   103,   104,   105,   106,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   115,   116,    -1,
      -1,   119,   120,   121,   122,   123,   124,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   150,   151,   152,    -1,    -1,   155,   156,    -1,
     158,    -1,    -1,    -1,   162,   163,    -1,   165,    -1,    -1,
     168,   169,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   187,
      -1,    -1,    -1,   191,    -1,    -1,    -1,   195,   196,    -1,
     198,    -1,    -1,   201,    -1,    -1,    56,    57,    58,    59,
      60,    61,    62,    -1,    -1,    -1,    66,    -1,    -1,    69,
     218,    71,    72,    73,    74,    75,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    89,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    98,    99,
      -1,    -1,   102,   103,   104,   105,   106,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   115,   116,    -1,    -1,   119,
     120,   121,   122,   123,   124,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     150,   151,   152,    -1,    -1,   155,   156,    -1,   158,    -1,
      -1,    -1,   162,   163,    -1,   165,    -1,    -1,   168,   169,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   187,    -1,    -1,
      -1,   191,    -1,    -1,    -1,   195,   196,    -1,   198,    -1,
      -1,   201,    -1,    -1,    56,    57,    58,    59,    60,    61,
      62,    -1,    -1,    -1,    66,    -1,    -1,    69,   218,    71,
      72,    73,    74,    75,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    89,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    98,    99,    -1,    -1,
     102,   103,   104,   105,   106,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   115,   116,    -1,    -1,   119,   120,   121,
     122,   123,   124,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   150,   151,
     152,    -1,    -1,   155,   156,    -1,   158,    -1,    -1,    -1,
     162,   163,    -1,   165,    -1,    -1,   168,   169,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   187,    -1,    -1,    -1,   191,
      -1,    -1,    -1,   195,   196,    -1,   198,    -1,    -1,   201,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   218
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     1,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    21,    22,    23,    24,
      25,    35,    36,    38,    39,    40,    41,    42,    43,    44,
      45,    47,    56,    57,    58,    60,    61,    69,    86,    87,
      89,    90,    91,    92,    93,    97,   105,   107,   108,   109,
     110,   111,   112,   113,   114,   121,   122,   145,   146,   154,
     159,   160,   161,   167,   171,   173,   175,   176,   177,   178,
     185,   186,   188,   189,   190,   192,   193,   197,   199,   200,
     202,   203,   204,   205,   206,   207,   208,   209,   210,   211,
     212,   213,   214,   215,   216,   217,   222,   234,   235,   236,
     237,   238,   239,   244,   245,   246,   247,   248,   251,   252,
     257,   258,   259,   260,   261,   262,   263,   266,   267,   268,
     269,   271,   274,   275,   276,   277,   278,   279,   280,   281,
     282,   283,   284,   285,   286,   297,   298,   299,   300,   301,
     302,   306,   307,   322,   323,   324,   325,   326,   327,   329,
     330,   331,   339,   340,   342,   346,   348,   350,   351,   352,
     354,   356,   357,   358,   359,   361,   362,   363,   364,   365,
     367,   368,   370,   371,   374,   375,   376,   377,   378,   379,
     381,   385,   386,   387,    85,   117,   220,   221,   231,   164,
     328,    19,    73,    75,    99,   101,   102,   105,   115,   116,
     119,   194,   341,   380,    56,    57,    58,    59,    60,    61,
      62,    66,    69,    71,    72,    73,    74,    75,    89,    95,
      96,    98,    99,   102,   103,   104,   105,   106,   115,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   150,   151,
     152,   155,   156,   158,   162,   163,   165,   168,   169,   187,
     191,   195,   196,   198,   201,   218,   303,   304,   394,   395,
     115,   116,   287,   395,    19,    20,    55,    68,   249,    35,
      55,   250,   194,   395,   395,    35,     1,    52,    68,   136,
     147,   148,   149,   153,   332,   333,   336,   395,   332,   332,
     332,   332,   332,   332,    88,   126,   127,   128,   129,   130,
     131,   132,   133,   134,   135,   137,   140,   308,    60,   237,
      62,    60,    61,   225,    98,   395,   395,    89,   119,   194,
     153,   153,   155,   156,   349,   162,   163,   165,   166,   355,
     162,   170,   353,   164,   168,   170,   344,   153,   172,   155,
     174,   347,   196,   344,   344,    89,   164,   360,    55,    70,
     170,   179,   180,   182,   183,   184,   345,    89,   153,   187,
     153,   157,   343,   153,   366,   191,   344,   153,   369,   194,
     195,   196,   153,   373,    26,    77,    26,   270,    26,   273,
     273,    89,    76,   270,   270,    12,     6,   153,    89,   153,
     156,   218,    52,     0,   236,    52,   222,     5,    95,   239,
     253,   255,   256,   286,   297,   298,   299,   300,   301,   387,
      95,   254,   239,   297,   298,   299,   300,   301,    89,   395,
     238,   238,   306,   226,   265,   290,   291,   292,   239,   293,
     294,   389,   390,   395,    74,   338,   390,   293,   390,    99,
     100,   241,   242,   243,   265,   290,   106,   231,    54,    59,
      89,   232,    59,   104,   103,   103,   123,    59,   120,   120,
     163,    71,    72,   225,   225,   395,   395,   395,   395,   223,
     225,   226,   224,    68,   395,   120,    26,    37,   395,   226,
     226,   226,   226,    46,   333,   226,   328,    89,   225,   225,
     225,   225,    49,    50,    89,   126,   127,   128,   129,   131,
     132,   133,   134,   135,   136,   140,   316,   225,   225,   225,
     225,   225,   138,   139,   225,     1,    48,    52,   309,   395,
     222,    63,    65,   382,   383,   384,    89,    89,   153,   120,
     153,   153,   344,   153,   164,   344,   153,   344,   344,    89,
     164,   170,    89,   344,   169,   344,    89,   153,    89,   344,
      89,    89,   345,   153,   153,   153,   223,   344,   153,   344,
      89,   201,   395,    89,   170,   272,   272,   224,   395,    89,
      89,   153,   153,   153,   153,    52,   223,   227,   239,   290,
      26,   264,   291,   179,   181,   388,    76,    95,   224,   395,
      26,   388,   100,    89,    26,   106,   273,   225,    84,   170,
     125,   153,   305,   395,   305,   225,   225,   225,   225,   303,
     305,    51,    89,    95,    96,   125,   153,   288,   289,   395,
     395,   153,   153,   150,   151,   152,   337,   153,   334,   335,
     395,   225,    94,   321,   395,   321,   321,   321,   226,   226,
     224,   225,   225,   225,   225,   225,   225,   225,   225,   225,
     225,   225,   315,   321,   321,   319,   321,   319,   321,   225,
     225,   321,    64,    66,   223,   343,   344,   344,   153,   344,
     344,    89,   344,   158,   223,   153,   124,   198,   372,    77,
     225,    26,   224,    89,    89,   225,    95,   227,   227,   293,
     226,   395,   226,   395,    28,    29,    30,    31,    32,    33,
     223,   391,   392,   395,   395,   293,   226,   295,    89,     9,
     240,   293,   272,    89,   124,   219,   305,   305,   305,   305,
     223,   227,   224,   395,   227,   227,   227,   335,   227,   320,
     321,   315,   315,   315,   315,   395,   317,   318,   395,    89,
     320,   321,   321,   321,   321,   321,   321,   319,   319,   321,
     321,    48,   223,   315,   315,   315,   315,   315,   228,   228,
     315,    67,   384,   344,   153,   153,   223,   345,   201,    89,
     395,   395,    89,   153,   395,   395,    28,    28,    34,   393,
     393,   393,   395,    76,    95,   395,   296,   395,   294,   240,
      19,   289,   395,   227,   315,    48,    48,    48,    48,   227,
     227,   223,   225,   225,   316,    48,    48,    48,    48,    48,
     126,   140,   141,   142,   144,   310,   311,    73,   312,    48,
      89,   372,   225,   227,   227,    28,    28,    28,    76,   224,
     395,   395,   223,   227,    19,   390,    48,   317,   305,   320,
     226,   226,   226,   226,   226,   229,   223,   226,   229,    89,
     395,   395,   395,   390,   321,   321,   143,   227,   227,    89,
     313,   314,    48,   310,   319,    48,    76,   395,   227,   227,
     225,   225,   227,   223,   227,   230,   395,   321,   321,   313,
     319,   227,   227
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

    { HANDLE_ACCEPT_FINISH(); ;}
    break;

  case 3:

    { HANDLE_ERROR_ACCEPT(); ;}
    break;

  case 6:

    { handle_stmt_end(result); HANDLE_ACCEPT_FINISH(); ;}
    break;

  case 7:

    { handle_stmt_end(result); ;}
    break;

  case 8:

    { handle_stmt_end(result); HANDLE_ACCEPT_FINISH(); ;}
    break;

  case 9:

    { handle_stmt_end(result); ;}
    break;

  case 10:

    { handle_stmt_end(result); HANDLE_ACCEPT_FINISH(); ;}
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

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_GLOBAL_PORT; ;}
    break;

  case 43:

    {
              result->cur_stmt_type_ = OBPROXY_T_SELECT;
            ;}
    break;

  case 46:

    {
              result->cur_stmt_type_ = OBPROXY_T_LOAD_DATA_INFILE;
            ;}
    break;

  case 47:

    {
              result->cur_stmt_type_ = OBPROXY_T_LOAD_DATA_LOCAL_INFILE;
            ;}
    break;

  case 60:

    { result->cur_stmt_type_ = OBPROXY_T_CREATE; ;}
    break;

  case 61:

    { result->cur_stmt_type_ = OBPROXY_T_DROP; ;}
    break;

  case 62:

    { result->cur_stmt_type_ = OBPROXY_T_ALTER; ;}
    break;

  case 63:

    { result->cur_stmt_type_ = OBPROXY_T_TRUNCATE; ;}
    break;

  case 64:

    { result->cur_stmt_type_ = OBPROXY_T_RENAME; ;}
    break;

  case 65:

    {;}
    break;

  case 66:

    {;}
    break;

  case 68:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_TABLE; ;}
    break;

  case 69:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 70:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 71:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_TABLEGROUP; ;}
    break;

  case 73:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_DROP_TABLEGROUP; ;}
    break;

  case 74:

    {
            SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num));
            result->cur_stmt_type_ = OBPROXY_T_STOP_DDL_TASK;
          ;}
    break;

  case 75:

    {
            SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num));
            result->cur_stmt_type_ = OBPROXY_T_RETRY_DDL_TASK;
          ;}
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

    {;}
    break;

  case 84:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(1) - (1)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 85:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(3) - (3)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 86:

    {
                          ObProxyTextPsParseNode *node = NULL;
                          malloc_parse_node(node);
                          node->str_value_ = (yyvsp[(1) - (1)].str);
                          add_text_ps_node(result->text_ps_parse_info_, node);
                        ;}
    break;

  case 89:

    {
                      result->text_ps_inner_stmt_type_ = OBPROXY_T_TEXT_PS_PREPARE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 90:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (2)].str);
                    ;}
    break;

  case 91:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 92:

    {
            ;}
    break;

  case 93:

    {
            ;}
    break;

  case 94:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 95:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 96:

    { result->cur_stmt_type_ = OBPROXY_T_GRANT; ;}
    break;

  case 97:

    { result->cur_stmt_type_ = OBPROXY_T_REVOKE; ;}
    break;

  case 98:

    { result->cur_stmt_type_ = OBPROXY_T_ANALYZE; ;}
    break;

  case 99:

    { result->cur_stmt_type_ = OBPROXY_T_PURGE; ;}
    break;

  case 100:

    { result->cur_stmt_type_ = OBPROXY_T_FLASHBACK; ;}
    break;

  case 101:

    { result->cur_stmt_type_ = OBPROXY_T_COMMENT; ;}
    break;

  case 102:

    { result->cur_stmt_type_ = OBPROXY_T_AUDIT; ;}
    break;

  case 103:

    { result->cur_stmt_type_ = OBPROXY_T_NOAUDIT; ;}
    break;

  case 106:

    {;}
    break;

  case 107:

    {;}
    break;

  case 108:

    {;}
    break;

  case 114:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_TX_RO; ;}
    break;

  case 118:

    { result->col_name_ = (yyvsp[(3) - (3)].str); ;}
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

    {;}
    break;

  case 125:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY; ;}
    break;

  case 126:

    {;}
    break;

  case 127:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_DATABASE; ;}
    break;

  case 128:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_PROXY_STATUS; ;}
    break;

  case 129:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DATABASES; ;}
    break;

  case 130:

    {;}
    break;

  case 131:

    {;}
    break;

  case 132:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_COLUMNS; ;}
    break;

  case 133:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_INDEX; ;}
    break;

  case 134:

    {;}
    break;

  case 135:

    {
                      result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
                      result->cur_stmt_type_ = OBPROXY_T_DESC;
                      result->sub_stmt_type_ = OBPROXY_T_SUB_DESC_TABLE;
                  ;}
    break;

  case 136:

    {
            result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
          ;}
    break;

  case 137:

    {
            result->table_info_.table_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.database_name_ = (yyvsp[(4) - (4)].str);
          ;}
    break;

  case 138:

    {
            result->table_info_.database_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.table_name_ = (yyvsp[(4) - (4)].str);
          ;}
    break;

  case 139:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE;
                        result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
                      ;}
    break;

  case 140:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE;
                        result->table_info_.database_name_ = (yyvsp[(2) - (4)].str);
                        result->table_info_.table_name_ = (yyvsp[(4) - (4)].str);
                      ;}
    break;

  case 141:

    {;}
    break;

  case 142:

    { result->table_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 143:

    {;}
    break;

  case 144:

    { result->table_info_.database_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 145:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLES;
                ;}
    break;

  case 146:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_FULL_TABLES;
                ;}
    break;

  case 147:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLE_STATUS;
                      ;}
    break;

  case 148:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DB_VERSION; ;}
    break;

  case 149:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID; ;}
    break;

  case 150:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 151:

    {
                     SET_ICMD_SECOND_STRING((yyvsp[(5) - (5)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 152:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (7)].str));
                     SET_ICMD_SECOND_STRING((yyvsp[(7) - (7)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 153:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_ROUTE_ADDR; ;}
    break;

  case 154:

    {
                              result->cur_stmt_type_ = OBPROXY_T_SET_ROUTE_ADDR;
                              result->cmd_info_.integer_[0] = (yyvsp[(3) - (3)].num);
                           ;}
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

  case 160:

    {;}
    break;

  case 162:

    {
                   result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                 ;}
    break;

  case 163:

    {
                   result->table_info_.package_name_ = (yyvsp[(1) - (3)].str);
                   result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                 ;}
    break;

  case 164:

    {
                   result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                   result->table_info_.package_name_ = (yyvsp[(3) - (5)].str);
                   result->table_info_.table_name_ = (yyvsp[(5) - (5)].str);
                 ;}
    break;

  case 165:

    {
                result->call_parse_info_.node_count_ = 0;
              ;}
    break;

  case 166:

    {
                result->call_parse_info_.node_count_ = 0;
                add_call_node(result->call_parse_info_, (yyvsp[(1) - (1)].node));
              ;}
    break;

  case 167:

    {
                add_call_node(result->call_parse_info_, (yyvsp[(3) - (3)].node));
              ;}
    break;

  case 168:

    {
            malloc_call_node((yyval.node), CALL_TOKEN_STR_VAL);
            (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 169:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_INT_VAL);
           (yyval.node)->int_value_ = (yyvsp[(1) - (1)].num);
         ;}
    break;

  case 170:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_NUMBER_VAL);
           (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 171:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_USER_VAR);
           (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 172:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_SYS_VAR);
           (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 173:

    {
           result->placeholder_list_idx_++;
           malloc_call_node((yyval.node), CALL_TOKEN_PLACE_HOLDER);
           (yyval.node)->placeholder_idx_ = result->placeholder_list_idx_ - 1;
         ;}
    break;

  case 187:

    {
                                                                  handle_stmt_end(result);
                                                                  HANDLE_ACCEPT_FINISH();
                                                                ;}
    break;

  case 192:

    {
                                                 handle_stmt_end(result);
                                                 HANDLE_ACCEPT_FINISH();
                                               ;}
    break;

  case 196:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_USER);
        ;}
    break;

  case 197:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 198:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 199:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_SYS);
        ;}
    break;

  case 200:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 201:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 202:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_SYS);
        ;}
    break;

  case 203:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_STR);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 204:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_INT);
               (yyval.var_node)->int_value_ = (yyvsp[(1) - (1)].num);
             ;}
    break;

  case 205:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_NUMBER);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 208:

    {;}
    break;

  case 209:

    {;}
    break;

  case 210:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 211:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 212:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 213:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 214:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 215:

    {
              malloc_shard_column_node((yyval.shard_node), (yyvsp[(2) - (7)].str), (yyvsp[(3) - (7)].str), DBMESH_TOKEN_STR_VAL);
              (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (7)].str);
              add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
            ;}
    break;

  case 216:

    { result->trace_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 217:

    { result->rpc_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 218:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 219:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 220:

    {;}
    break;

  case 221:

    {;}
    break;

  case 222:

    { result->target_db_server_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 223:

    {;}
    break;

  case 226:

    { handle_stmt_end(result); HANDLE_ACCEPT_FINISH(); ;}
    break;

  case 227:

    { yyerrok; yyclearin; ;}
    break;

  case 230:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.group_idx_str_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 231:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.table_name_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 232:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 233:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 234:

    { result->dbp_route_info_.sticky_session_ = true; ;}
    break;

  case 235:

    {result->dbp_route_info_.has_shard_key_ = true;;}
    break;

  case 236:

    { result->trace_id_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 237:

    { result->trace_id_ = (yyvsp[(3) - (6)].str); result->rpc_id_ = (yyvsp[(5) - (6)].str); ;}
    break;

  case 238:

    {;}
    break;

  case 240:

    {
                   if (result->dbp_route_info_.shard_key_count_ < OBPROXY_MAX_DBP_SHARD_KEY_NUM) {
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].left_str_ = (yyvsp[(1) - (3)].str);
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].right_str_ = (yyvsp[(3) - (3)].str);
                     ++result->dbp_route_info_.shard_key_count_;
                   }
                 ;}
    break;

  case 243:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 244:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 245:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 246:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 247:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 248:

    { result->trace_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 249:

    { result->rpc_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 250:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 251:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 252:

    { result->has_trace_log_hint_ = true; ;}
    break;

  case 253:

    { result->target_db_server_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 254:

    {
             malloc_shard_column_node((yyval.shard_node), (yyvsp[(1) - (5)].str), (yyvsp[(3) - (5)].str), DBMESH_TOKEN_STR_VAL);
             (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (5)].str);
             add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
           ;}
    break;

  case 255:

    {;}
    break;

  case 256:

    { result->has_hint_route_info_ = true; result->hint_route_info_.table_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 257:

    { result->has_hint_route_info_ = true; ;}
    break;

  case 258:

    {;}
    break;

  case 260:

    {
                    if (result->hint_route_info_.part_key_info_.node_count_ < OBPROXY_MAX_PART_KEY_PARSE_NUM) {
                      add_set_var_node(result->hint_route_info_.part_key_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_USER);
                    }
                  ;}
    break;

  case 261:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 263:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 295:

    { result->query_timeout_ = (yyvsp[(3) - (4)].num); ;}
    break;

  case 296:

    { result->max_execution_time_ = (yyvsp[(3) - (4)].num); ;}
    break;

  case 298:

    {
      add_hint_index(result->dbmesh_route_info_, (yyvsp[(3) - (5)].str));
      result->dbmesh_route_info_.index_count_++;
    ;}
    break;

  case 299:

    { result->has_trace_log_hint_ = true; ;}
    break;

  case 303:

    { handle_stmt_end(result); HANDLE_ACCEPT_FINISH(); ;}
    break;

  case 304:

    { yyerrok; yyclearin; ;}
    break;

  case 305:

    {;}
    break;

  case 306:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_WEAK); ;}
    break;

  case 307:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_STRONG); ;}
    break;

  case 308:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_FROZEN); ;}
    break;

  case 311:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_WARNINGS; ;}
    break;

  case 312:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_ERRORS; ;}
    break;

  case 313:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 314:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 315:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_SLAVE_HOSTS; ;}
    break;

  case 316:

    {
            result->is_binlog_related_ = true;
            result->cur_stmt_type_ = OBPROXY_T_SHOW_SLAVE_STATUS;
          ;}
    break;

  case 317:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_RELAYLOG_EVENTS; ;}
    break;

  case 318:

    { result->is_binlog_related_ = true; ;}
    break;

  case 319:

    { result->is_binlog_related_ = true; ;}
    break;

  case 320:

    { result->is_binlog_related_ = true; ;}
    break;

  case 321:

    { result->is_binlog_related_ = true; ;}
    break;

  case 351:

    { result->cur_stmt_type_ = OBPROXY_T_BINLOG_STR; ;}
    break;

  case 352:

    {
    result->cur_stmt_type_ = OBPROXY_T_SHOW_BINLOG_SERVER_FOR_TENANT;
    result->is_binlog_related_ = true;
;}
    break;

  case 353:

    { result->is_binlog_related_ = true; ;}
    break;

  case 354:

    { result->is_binlog_related_ = true; ;}
    break;

  case 355:

    { result->is_binlog_related_ = true; ;}
    break;

  case 356:

    { result->is_binlog_related_ = true; ;}
    break;

  case 357:

    {
;}
    break;

  case 358:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 359:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 360:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 361:

    {;}
    break;

  case 362:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 363:

    {;}
    break;

  case 364:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 366:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_KV_THREAD); ;}
    break;

  case 367:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_KV_REQUESTSTAT); ;}
    break;

  case 368:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KV_REQUESTSTAT, (yyvsp[(2) - (2)].str)); ;}
    break;

  case 370:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 371:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 372:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 373:

    {;}
    break;

  case 374:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_ALL); ;}
    break;

  case 375:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 376:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 377:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 379:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 380:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 381:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 382:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 383:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 385:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 386:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 387:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 388:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 389:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 390:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 391:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 392:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 393:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 394:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_READ_STALE); ;}
    break;

  case 395:

    {;}
    break;

  case 396:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 397:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_PROXYSM_RPC); ;}
    break;

  case 398:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_PROXYSM_RPC, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 399:

    {;}
    break;

  case 400:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 401:

    {;}
    break;

  case 403:

    {;}
    break;

  case 404:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 405:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 406:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 407:

    {;}
    break;

  case 408:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 409:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 410:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_GLOBALINDEX); ;}
    break;

  case 411:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_TABLEGROUP); ;}
    break;

  case 412:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_QUERYASYNC); ;}
    break;

  case 413:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_TABLETLS); ;}
    break;

  case 414:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_ROUTE_TABLETLS, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 415:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_RPCCTX); ;}
    break;

  case 416:

    {;}
    break;

  case 417:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 418:

    {;}
    break;

  case 419:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 420:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 421:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); SET_ICMD_ONE_ID((yyvsp[(3) - (3)].num)); ;}
    break;

  case 422:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 423:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 425:

    {;}
    break;

  case 426:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 427:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 428:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 429:

    {;}
    break;

  case 430:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 432:

    {;}
    break;

  case 433:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 434:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 435:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 436:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 437:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 438:

    {;}
    break;

  case 439:

    {;}
    break;

  case 440:

    {;}
    break;

  case 441:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_PS_ALL);;}
    break;

  case 442:

    {;}
    break;

  case 443:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 444:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 445:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 446:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 447:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 448:

    {;}
    break;

  case 449:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 450:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 451:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 452:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 453:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 454:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 455:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 458:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 459:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 460:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 461:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 468:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 469:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 471:

    {;}
    break;

  case 472:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 473:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 474:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 475:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 476:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT_FINISH();
                                                ;}
    break;

  case 477:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT_FINISH();
                                                ;}
    break;

  case 478:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 479:

    {
                                                  result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                  result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                                  result->table_info_.dblink_name_ = (yyvsp[(4) - (4)].str);
                                                 ;}
    break;

  case 480:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 481:

    {
                                      result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                      result->table_info_.dblink_name_ = (yyvsp[(2) - (2)].str);
                                    ;}
    break;

  case 482:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 483:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 484:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 485:

    {
                                                  UPDATE_ALIAS_NAME((yyvsp[(5) - (5)].str));
                                                  result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                                                  result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                                                ;}
    break;

  case 486:

    { result->table_info_.join_table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 487:

    {
            result->table_info_.join_database_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.join_table_name_ = (yyvsp[(4) - (4)].str);
          ;}
    break;

  case 488:

    {
            result->table_info_.join_table_name_ = (yyvsp[(2) - (3)].str);
            result->table_info_.join_table_alias_name_ = (yyvsp[(3) - (3)].str);
         ;}
    break;

  case 489:

    {
            result->table_info_.join_table_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.join_table_alias_name_ = (yyvsp[(4) - (4)].str);
         ;}
    break;

  case 490:

    {
            result->table_info_.join_database_name_ = (yyvsp[(2) - (5)].str);
            result->table_info_.join_table_name_ = (yyvsp[(4) - (5)].str);
            result->table_info_.join_table_alias_name_ = (yyvsp[(5) - (5)].str);
         ;}
    break;

  case 491:

    {
            result->table_info_.join_database_name_ = (yyvsp[(2) - (6)].str);
            result->table_info_.join_table_name_ = (yyvsp[(4) - (6)].str);
            result->table_info_.join_table_alias_name_ = (yyvsp[(6) - (6)].str);
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
  UNUSED(yylloc);
  UNUSED(p);
  UNUSED(s);
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

