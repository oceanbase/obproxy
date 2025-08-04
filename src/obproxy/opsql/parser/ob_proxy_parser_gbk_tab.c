
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
     READ_CONSISTENCY = 403,
     WEAK = 404,
     STRONG = 405,
     FROZEN = 406,
     INT_NUM = 407,
     SHOW_PROXYNET = 408,
     THREAD = 409,
     CONNECTION = 410,
     LIMIT = 411,
     OFFSET = 412,
     SHOW_PROCESSLIST = 413,
     SHOW_PROXYSESSION = 414,
     SHOW_GLOBALSESSION = 415,
     ATTRIBUTE = 416,
     VARIABLES = 417,
     ALL = 418,
     STAT = 419,
     READ_STALE = 420,
     SHOW_PROXYCONFIG = 421,
     DIFF = 422,
     USER = 423,
     LIKE = 424,
     SHOW_PROXYSM = 425,
     RPC = 426,
     SHOW_PROXYRPC = 427,
     REQUESTSTAT = 428,
     SHOW_PROXYCLUSTER = 429,
     SHOW_PROXYRESOURCE = 430,
     SHOW_PROXYCONGESTION = 431,
     SHOW_PROXYROUTE = 432,
     PARTITION = 433,
     ROUTINE = 434,
     SUBPARTITION = 435,
     TABLETLS = 436,
     QUERYASYNC = 437,
     RPCCTX = 438,
     SHOW_PROXYVIP = 439,
     SHOW_PROXYMEMORY = 440,
     OBJPOOL = 441,
     SHOW_SQLAUDIT = 442,
     SHOW_WARNLOG = 443,
     SHOW_PROXYSTAT = 444,
     REFRESH = 445,
     SHOW_PROXYTRACE = 446,
     SHOW_PROXYINFO = 447,
     BINARY = 448,
     UPGRADE = 449,
     IDC = 450,
     SHOW_PROXYPS = 451,
     DETAIL = 452,
     SHOW_ELASTIC_ID = 453,
     SHOW_TOPOLOGY = 454,
     GROUP_NAME = 455,
     SHOW_DB_VERSION = 456,
     SHOW_DATABASES = 457,
     SHOW_TABLES = 458,
     SHOW_FULL_TABLES = 459,
     SELECT_DATABASE = 460,
     SELECT_PROXY_STATUS = 461,
     SHOW_CREATE_TABLE = 462,
     SELECT_PROXY_VERSION = 463,
     SHOW_COLUMNS = 464,
     SHOW_INDEX = 465,
     ALTER_PROXYCONFIG = 466,
     ALTER_PROXYRESOURCE = 467,
     PING_PROXY = 468,
     KILL_PROXYSESSION = 469,
     KILL_GLOBALSESSION = 470,
     KILL = 471,
     QUERY = 472,
     BINLOG_VARIABLE = 473,
     BINLOG_USER_VAR = 474,
     BINLOG_SYS_VAR = 475
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
#define YYFINAL  392
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   3829

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  232
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  163
/* YYNRULES -- Number of rules.  */
#define YYNRULES  548
/* YYNRULES -- Number of states.  */
#define YYNSTATES  879

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   475

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,   229,     2,     2,     2,     2,
     225,   226,   231,     2,   222,     2,   223,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   221,
       2,   224,     2,     2,   230,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   227,     2,   228,     2,     2,     2,     2,
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
     215,   216,   217,   218,   219,   220
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
     882,   885,   887,   889,   890,   893,   898,   903,   909,   911,
     916,   918,   920,   922,   924,   925,   927,   929,   931,   932,
     934,   938,   942,   945,   951,   955,   959,   963,   967,   971,
     975,   979,   981,   983,   985,   987,   989,   991,   993,   995,
     997,   999,  1001,  1003,  1005,  1007,  1009,  1011,  1013,  1015,
    1017,  1019,  1021,  1023,  1025,  1027,  1029,  1030,  1032,  1034,
    1036,  1039,  1045,  1051,  1054,  1058,  1062,  1063,  1066,  1071,
    1076,  1077,  1080,  1081,  1084,  1087,  1089,  1091,  1094,  1097,
    1099,  1101,  1105,  1108,  1112,  1116,  1121,  1123,  1126,  1127,
    1130,  1134,  1137,  1140,  1143,  1144,  1147,  1151,  1154,  1158,
    1161,  1165,  1169,  1174,  1177,  1179,  1182,  1185,  1189,  1192,
    1196,  1199,  1202,  1203,  1205,  1207,  1210,  1213,  1217,  1220,
    1223,  1226,  1229,  1232,  1236,  1239,  1241,  1244,  1246,  1249,
    1252,  1256,  1259,  1262,  1265,  1266,  1268,  1272,  1278,  1281,
    1285,  1288,  1289,  1291,  1294,  1297,  1300,  1303,  1308,  1315,
    1316,  1318,  1319,  1321,  1326,  1332,  1338,  1342,  1344,  1347,
    1351,  1355,  1358,  1361,  1365,  1369,  1370,  1373,  1375,  1379,
    1383,  1387,  1388,  1390,  1392,  1396,  1399,  1403,  1406,  1409,
    1411,  1412,  1415,  1420,  1423,  1428,  1431,  1435,  1437,  1442,
    1446,  1449,  1452,  1457,  1461,  1467,  1470,  1475,  1479,  1484,
    1490,  1497,  1499,  1501,  1504,  1507,  1511,  1515,  1519,  1521,
    1522,  1524,  1526,  1528,  1530,  1532,  1534,  1536,  1538,  1540,
    1542,  1544,  1546,  1548,  1550,  1552,  1554,  1556,  1558,  1560,
    1562,  1564,  1566,  1568,  1570,  1572,  1574,  1576,  1578,  1580,
    1582,  1584,  1586,  1588,  1590,  1592,  1594,  1596,  1598,  1600,
    1602,  1604,  1606,  1608,  1610,  1612,  1614,  1616,  1618
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     233,     0,    -1,   234,    -1,     1,    -1,   235,    -1,   234,
     235,    -1,   236,    52,    -1,   236,   221,    -1,   236,   221,
      52,    -1,   221,    -1,   221,    52,    -1,    60,   236,   221,
      -1,   237,    -1,   305,   237,    -1,   238,    -1,   296,    -1,
     301,    -1,   297,    -1,   298,    -1,   299,    -1,   245,    -1,
     244,    -1,   380,    -1,   338,    -1,   267,    -1,   339,    -1,
     384,    -1,   385,    -1,   279,    -1,   280,    -1,   281,    -1,
     282,    -1,   283,    -1,   284,    -1,   285,    -1,   246,    -1,
     258,    -1,   300,    -1,   341,    -1,   243,    -1,   386,    -1,
     321,    -1,   322,    -1,   323,   264,   263,    -1,    -1,     9,
      -1,   100,    89,   239,    19,   389,    -1,    99,   100,    89,
     239,    19,   389,    -1,   241,    -1,   240,    -1,   330,   242,
      -1,   260,   238,    -1,   260,   296,    -1,   260,   298,    -1,
     260,   299,    -1,   260,   297,    -1,   260,   300,    -1,   262,
     237,    -1,   247,    -1,   259,    -1,    14,   248,    -1,    15,
     249,    -1,    16,    -1,    17,    -1,    18,    -1,   250,    -1,
     251,    -1,    -1,    19,    -1,    68,    -1,    20,    68,    -1,
      55,    -1,    -1,    55,    -1,   145,   152,    -1,   146,   152,
      -1,   238,    -1,   296,    -1,   297,    -1,   299,    -1,   298,
      -1,   386,    -1,   285,    -1,   300,    -1,    95,    -1,   253,
     222,    95,    -1,    95,    -1,   254,    -1,   252,    -1,    35,
     394,    26,    -1,    36,   394,    -1,    36,   394,    37,    -1,
     256,   255,    -1,   257,   253,    -1,    15,    35,   394,    -1,
      38,    35,   394,    -1,    21,    -1,    22,    -1,    23,    -1,
      24,    -1,    56,    -1,    25,    -1,    57,    -1,    58,    -1,
     261,    -1,   261,    89,    -1,    90,    -1,    92,    -1,    93,
      -1,    91,    -1,    -1,    26,   292,    -1,    -1,   289,    -1,
       5,    85,    -1,     5,    85,   264,    26,   292,    -1,     5,
      85,   289,    -1,   208,    -1,   208,    76,   394,    -1,   265,
      -1,   266,    -1,   277,    -1,   278,    -1,   268,    -1,   276,
      -1,   199,   269,    -1,   275,    -1,   205,    -1,   206,    -1,
     202,    -1,   273,    -1,   274,    -1,   209,   269,    -1,   210,
     269,    -1,   270,    -1,   261,   394,    -1,    26,   394,    -1,
      26,   394,    26,   394,    -1,    26,   394,   223,   394,    -1,
     207,    89,    -1,   207,    89,   223,    89,    -1,    -1,   169,
      89,    -1,    -1,    26,    89,    -1,   203,   272,   271,    -1,
     204,   272,   271,    -1,    11,    19,    59,   272,   271,    -1,
     201,    -1,   198,    -1,   198,    26,    89,    -1,   198,    77,
     200,   224,    89,    -1,   198,    26,    89,    77,   200,   224,
      89,    -1,    86,    -1,    87,   224,   152,    -1,   109,    -1,
     110,    -1,   111,    -1,   112,    -1,   113,    -1,   114,    -1,
      13,   286,   225,   287,   226,    -1,   394,    -1,   394,   223,
     394,    -1,   394,   223,   394,   223,   394,    -1,    -1,   288,
      -1,   287,   222,   288,    -1,    89,    -1,   152,    -1,   125,
      -1,    95,    -1,    96,    -1,    51,    -1,   290,    -1,   289,
     290,    -1,   291,    -1,   225,   226,    -1,   225,   238,   226,
      -1,   225,   289,   226,    -1,   388,    -1,   293,    -1,   238,
      -1,    -1,   225,   295,   226,    -1,   394,    -1,   295,   222,
     394,    -1,   326,   389,   387,    -1,   326,   389,   387,   294,
     293,    -1,   328,   292,    -1,   324,   292,    -1,   325,   337,
      26,   292,    -1,   329,   389,    -1,    12,   302,    -1,   303,
     222,   302,    -1,   303,    -1,    95,   224,   304,    -1,   117,
     394,   224,   304,    -1,   115,   394,   224,   304,    -1,    96,
     224,   304,    -1,   118,   394,   224,   304,    -1,   116,   394,
     224,   304,    -1,   394,   224,   304,    -1,   394,    -1,   152,
      -1,   125,    -1,   306,    -1,   306,   305,    -1,    47,   307,
      48,    -1,    47,   130,   315,   314,    48,    -1,    47,   127,
     224,   320,   314,    48,    -1,    47,   140,   224,   320,   314,
      48,    -1,    47,   126,   224,   320,   314,    48,    -1,    47,
     128,   224,   320,   314,    48,    -1,    47,   129,   224,   320,
     314,    48,    -1,    47,    88,    89,   224,   319,   314,    48,
      -1,    47,   133,   224,   318,   314,    48,    -1,    47,   134,
     224,   318,   314,    48,    -1,    47,   131,   224,   320,   314,
      48,    -1,    47,   132,   224,   320,   314,    48,    -1,    47,
     137,   138,   224,   227,   309,   228,    48,    -1,    47,   137,
     139,   224,   227,   311,   228,    48,    -1,    47,   135,   224,
     320,   314,    48,    -1,    -1,   307,   308,    -1,   394,    -1,
      52,    -1,     1,    -1,   310,   222,   309,    -1,   310,    -1,
     126,   225,   320,   226,    -1,   140,   225,   320,   226,    -1,
     141,   225,   226,    -1,   141,   225,   143,   224,   320,   226,
      -1,   142,   225,   226,    -1,   144,   225,   312,   226,    -1,
      73,   225,   318,   226,    -1,    73,   225,   318,   229,   318,
     226,    -1,   313,   222,   312,    -1,   313,    -1,    89,   224,
     320,    -1,    -1,   314,   222,   315,    -1,   126,   224,   320,
      -1,   127,   224,   320,    -1,   140,   224,   320,    -1,   128,
     224,   320,    -1,   129,   224,   320,    -1,   133,   224,   318,
      -1,   134,   224,   318,    -1,   131,   224,   320,    -1,   132,
     224,   320,    -1,   136,    -1,   135,   224,   320,    -1,    89,
     223,    89,   224,   319,    -1,    89,   224,   319,    -1,    49,
     225,   394,   226,    -1,    50,   225,   316,   226,    -1,   317,
     222,   316,    -1,   317,    -1,   394,   224,   304,    -1,    -1,
     320,    -1,    -1,   320,    -1,   394,    -1,    94,    -1,     5,
     219,    -1,     5,   220,    -1,     5,   117,   106,    -1,     5,
     230,   230,   106,    -1,     5,    -1,    39,   331,    -1,     8,
      -1,    40,   331,    -1,     6,    -1,    41,   331,    -1,     7,
     327,    -1,    42,   331,   327,    -1,    -1,   163,    -1,   163,
      54,    -1,     9,    -1,    43,   331,    -1,    10,    -1,    44,
     331,    -1,    97,    98,    -1,    45,   331,    -1,   332,    46,
      -1,    -1,   335,   332,    -1,   152,    -1,   394,    -1,    -1,
     333,   334,    -1,   147,   225,   152,   226,    -1,   148,   225,
     336,   226,    -1,    68,   225,   394,   394,   226,    -1,   136,
      -1,   394,   225,   334,   226,    -1,   394,    -1,   152,    -1,
      52,    -1,     1,    -1,    -1,   149,    -1,   150,    -1,   151,
      -1,    -1,    74,    -1,    11,   379,    71,    -1,    11,   379,
      72,    -1,    11,    73,    -1,    11,    73,    89,   224,    89,
      -1,    11,   101,   104,    -1,    11,   101,    59,    -1,    11,
     102,   103,    -1,    11,   119,    59,    -1,    11,   193,   120,
      -1,    11,   105,   103,    -1,    11,   119,   120,    -1,   347,
      -1,   349,    -1,   350,    -1,   353,    -1,   351,    -1,   355,
      -1,   356,    -1,   357,    -1,   358,    -1,   360,    -1,   361,
      -1,   362,    -1,   363,    -1,   364,    -1,   366,    -1,   367,
      -1,   369,    -1,   345,    -1,   370,    -1,   373,    -1,   374,
      -1,   375,    -1,   376,    -1,   377,    -1,   378,    -1,    -1,
     115,    -1,   116,    -1,    99,    -1,   105,   394,    -1,    11,
     105,   123,    84,   124,    -1,    11,   340,   162,   169,   218,
      -1,   121,   119,    -1,    24,   193,   120,    -1,   122,   193,
     120,    -1,    -1,   156,   152,    -1,   156,   152,   222,   152,
      -1,   156,   152,   157,   152,    -1,    -1,   169,    89,    -1,
      -1,   169,    89,    -1,   172,   346,    -1,   154,    -1,   173,
      -1,   173,    89,    -1,   153,   348,    -1,   154,    -1,   155,
      -1,   155,   152,   342,    -1,   166,   343,    -1,   166,   163,
     343,    -1,   166,   167,   343,    -1,   166,   167,   168,   343,
      -1,   158,    -1,   160,   352,    -1,    -1,   161,    89,    -1,
     161,   169,    89,    -1,   161,   163,    -1,   169,    89,    -1,
     159,   354,    -1,    -1,   161,   343,    -1,   161,   152,   343,
      -1,   164,   343,    -1,   164,   152,   343,    -1,   162,   343,
      -1,   162,   152,   343,    -1,   162,   163,   343,    -1,   162,
     163,   152,   343,    -1,   165,   343,    -1,   170,    -1,   170,
     152,    -1,   170,   171,    -1,   170,   171,   152,    -1,   174,
     343,    -1,   174,   195,   343,    -1,   175,   343,    -1,   176,
     359,    -1,    -1,    89,    -1,   163,    -1,   163,    89,    -1,
     177,   344,    -1,   177,   179,   344,    -1,   177,   178,    -1,
     177,    70,    -1,   177,    55,    -1,   177,   182,    -1,   177,
     181,    -1,   177,   181,   152,    -1,   177,   183,    -1,   184,
      -1,   184,    89,    -1,   185,    -1,   185,   152,    -1,   185,
     186,    -1,   185,   186,   152,    -1,   187,   342,    -1,   187,
     152,    -1,   188,   365,    -1,    -1,   152,    -1,   152,   222,
     152,    -1,   152,   222,   152,   222,    89,    -1,   189,   343,
      -1,   189,   190,   343,    -1,   191,   368,    -1,    -1,   152,
      -1,   152,   152,    -1,   192,   193,    -1,   192,   194,    -1,
     192,   195,    -1,   196,   372,   343,   371,    -1,   196,   372,
     343,   124,   344,   371,    -1,    -1,   197,    -1,    -1,   152,
      -1,   211,    12,    89,   224,    -1,   211,    12,    89,   224,
      89,    -1,   211,    12,    89,   224,   152,    -1,   212,     6,
      89,    -1,   213,    -1,   214,   152,    -1,   214,   152,   152,
      -1,   215,    89,   152,    -1,   215,    89,    -1,   216,   152,
      -1,   216,   155,   152,    -1,   216,   217,   152,    -1,    -1,
      75,   231,    -1,    60,    -1,    61,    62,   381,    -1,    69,
      60,    89,    -1,    69,    61,    89,    -1,    -1,   382,    -1,
     383,    -1,   382,   222,   383,    -1,    63,    64,    -1,    65,
      66,    67,    -1,   107,   394,    -1,   108,    89,    -1,    89,
      -1,    -1,   180,   394,    -1,   180,   225,   394,   226,    -1,
     178,   394,    -1,   178,   225,   394,   226,    -1,   389,   387,
      -1,   389,   387,   390,    -1,   394,    -1,   394,   223,   394,
      95,    -1,   394,   223,   394,    -1,   394,    95,    -1,   394,
     394,    -1,   394,   223,   394,   394,    -1,   394,    76,   394,
      -1,   394,   223,   394,    76,   394,    -1,   391,   394,    -1,
     391,   394,   223,   394,    -1,   391,   394,   394,    -1,   391,
     394,    76,   394,    -1,   391,   394,   223,   394,   394,    -1,
     391,   394,   223,   394,    76,   394,    -1,   222,    -1,    28,
      -1,    29,    28,    -1,    30,    28,    -1,    31,   392,    28,
      -1,    32,   392,    28,    -1,    33,   392,    28,    -1,    34,
      -1,    -1,    61,    -1,    69,    -1,    60,    -1,    62,    -1,
      66,    -1,    72,    -1,    71,    -1,    75,    -1,    74,    -1,
      73,    -1,   154,    -1,   155,    -1,   157,    -1,   161,    -1,
     162,    -1,   164,    -1,   167,    -1,   168,    -1,   186,    -1,
     190,    -1,   194,    -1,   195,    -1,   217,    -1,   200,    -1,
      56,    -1,    57,    -1,    58,    -1,    99,    -1,    98,    -1,
      59,    -1,   149,    -1,   150,    -1,   151,    -1,   115,    -1,
     116,    -1,   104,    -1,   103,    -1,   102,    -1,   105,    -1,
     106,    -1,   119,    -1,   120,    -1,   121,    -1,   122,    -1,
     123,    -1,   124,    -1,   197,    -1,    89,    -1,   393,    -1
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
     940,   942,   943,   945,   946,   948,   949,   950,   955,   956,
     957,   958,   959,   960,   962,   963,   964,   965,   967,   968,
     971,   972,   973,   974,   975,   976,   981,   982,   983,   984,
     985,   989,   990,   991,   992,   993,   994,   995,   996,   997,
     998,   999,  1000,  1001,  1002,  1003,  1004,  1005,  1006,  1007,
    1008,  1009,  1010,  1011,  1012,  1013,  1015,  1016,  1017,  1018,
    1021,  1022,  1027,  1028,  1029,  1030,  1035,  1037,  1041,  1046,
    1054,  1055,  1059,  1060,  1063,  1065,  1066,  1067,  1070,  1072,
    1073,  1074,  1078,  1079,  1080,  1081,  1086,  1088,  1090,  1091,
    1092,  1093,  1094,  1097,  1099,  1100,  1101,  1102,  1103,  1104,
    1105,  1106,  1107,  1108,  1112,  1113,  1114,  1115,  1119,  1120,
    1125,  1128,  1130,  1131,  1132,  1133,  1137,  1138,  1139,  1140,
    1141,  1142,  1143,  1144,  1145,  1149,  1150,  1154,  1155,  1156,
    1157,  1161,  1162,  1165,  1167,  1168,  1169,  1170,  1174,  1175,
    1178,  1180,  1181,  1182,  1186,  1187,  1188,  1191,  1192,  1195,
    1196,  1199,  1200,  1204,  1205,  1206,  1210,  1214,  1218,  1219,
    1223,  1224,  1228,  1229,  1230,  1233,  1234,  1237,  1241,  1242,
    1243,  1245,  1246,  1248,  1249,  1252,  1253,  1256,  1262,  1265,
    1267,  1268,  1269,  1270,  1271,  1273,  1277,  1283,  1286,  1291,
    1295,  1299,  1303,  1308,  1312,  1318,  1319,  1324,  1329,  1334,
    1340,  1347,  1348,  1349,  1350,  1351,  1352,  1353,  1355,  1356,
    1358,  1359,  1360,  1361,  1362,  1363,  1364,  1365,  1366,  1367,
    1368,  1369,  1370,  1371,  1372,  1373,  1374,  1375,  1376,  1377,
    1378,  1379,  1380,  1381,  1382,  1383,  1384,  1385,  1386,  1387,
    1388,  1389,  1390,  1391,  1392,  1393,  1394,  1395,  1396,  1397,
    1398,  1399,  1400,  1401,  1402,  1403,  1404,  1406,  1407
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
  "READ_CONSISTENCY", "WEAK", "STRONG", "FROZEN", "INT_NUM",
  "SHOW_PROXYNET", "THREAD", "CONNECTION", "LIMIT", "OFFSET",
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
     475,    59,    44,    46,    61,    40,    41,   123,   125,    35,
      64,    42
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   232,   233,   233,   234,   234,   235,   235,   235,   235,
     235,   235,   236,   236,   237,   237,   237,   237,   237,   237,
     237,   237,   237,   237,   237,   237,   237,   237,   237,   237,
     237,   237,   237,   237,   237,   237,   237,   237,   237,   237,
     237,   238,   238,   238,   239,   239,   240,   241,   242,   242,
     243,   244,   244,   244,   244,   244,   244,   245,   246,   246,
     247,   247,   247,   247,   247,   247,   247,   248,   248,   248,
     248,   248,   249,   249,   250,   251,   252,   252,   252,   252,
     252,   252,   252,   252,   253,   253,   254,   255,   255,   256,
     257,   257,   258,   258,   258,   258,   259,   259,   259,   259,
     259,   259,   259,   259,   260,   260,   261,   261,   261,   262,
     263,   263,   264,   264,   265,   265,   265,   266,   266,   267,
     267,   267,   267,   267,   268,   268,   268,   268,   268,   268,
     268,   268,   268,   268,   268,   268,   269,   269,   269,   270,
     270,   271,   271,   272,   272,   273,   273,   274,   275,   276,
     276,   276,   276,   277,   278,   279,   280,   281,   282,   283,
     284,   285,   286,   286,   286,   287,   287,   287,   288,   288,
     288,   288,   288,   288,   289,   289,   290,   291,   291,   291,
     292,   292,   293,   294,   294,   295,   295,   296,   296,   297,
     298,   299,   300,   301,   302,   302,   303,   303,   303,   303,
     303,   303,   303,   304,   304,   304,   305,   305,   306,   306,
     306,   306,   306,   306,   306,   306,   306,   306,   306,   306,
     306,   306,   306,   307,   307,   308,   308,   308,   309,   309,
     310,   310,   310,   310,   310,   310,   311,   311,   312,   312,
     313,   314,   314,   315,   315,   315,   315,   315,   315,   315,
     315,   315,   315,   315,   315,   315,   315,   315,   316,   316,
     317,   318,   318,   319,   319,   320,   320,   321,   321,   322,
     322,   323,   323,   324,   324,   325,   325,   326,   326,   327,
     327,   327,   328,   328,   329,   329,   330,   330,   331,   332,
     332,   333,   333,   334,   334,   335,   335,   335,   335,   335,
     335,   335,   335,   335,   336,   336,   336,   336,   337,   337,
     338,   338,   338,   338,   338,   338,   338,   338,   338,   338,
     338,   339,   339,   339,   339,   339,   339,   339,   339,   339,
     339,   339,   339,   339,   339,   339,   339,   339,   339,   339,
     339,   339,   339,   339,   339,   339,   340,   340,   340,   340,
     341,   341,   341,   341,   341,   341,   342,   342,   342,   342,
     343,   343,   344,   344,   345,   346,   346,   346,   347,   348,
     348,   348,   349,   349,   349,   349,   350,   351,   352,   352,
     352,   352,   352,   353,   354,   354,   354,   354,   354,   354,
     354,   354,   354,   354,   355,   355,   355,   355,   356,   356,
     357,   358,   359,   359,   359,   359,   360,   360,   360,   360,
     360,   360,   360,   360,   360,   361,   361,   362,   362,   362,
     362,   363,   363,   364,   365,   365,   365,   365,   366,   366,
     367,   368,   368,   368,   369,   369,   369,   370,   370,   371,
     371,   372,   372,   373,   373,   373,   374,   375,   376,   376,
     377,   377,   378,   378,   378,   379,   379,   380,   380,   380,
     380,   381,   381,   382,   382,   383,   383,   384,   385,   386,
     387,   387,   387,   387,   387,   388,   388,   389,   389,   389,
     389,   389,   389,   389,   389,   390,   390,   390,   390,   390,
     390,   391,   391,   391,   391,   391,   391,   391,   392,   392,
     393,   393,   393,   393,   393,   393,   393,   393,   393,   393,
     393,   393,   393,   393,   393,   393,   393,   393,   393,   393,
     393,   393,   393,   393,   393,   393,   393,   393,   393,   393,
     393,   393,   393,   393,   393,   393,   393,   393,   393,   393,
     393,   393,   393,   393,   393,   393,   393,   394,   394
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
       2,     1,     1,     0,     2,     4,     4,     5,     1,     4,
       1,     1,     1,     1,     0,     1,     1,     1,     0,     1,
       3,     3,     2,     5,     3,     3,     3,     3,     3,     3,
       3,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     0,     1,     1,     1,
       2,     5,     5,     2,     3,     3,     0,     2,     4,     4,
       0,     2,     0,     2,     2,     1,     1,     2,     2,     1,
       1,     3,     2,     3,     3,     4,     1,     2,     0,     2,
       3,     2,     2,     2,     0,     2,     3,     2,     3,     2,
       3,     3,     4,     2,     1,     2,     2,     3,     2,     3,
       2,     2,     0,     1,     1,     2,     2,     3,     2,     2,
       2,     2,     2,     3,     2,     1,     2,     1,     2,     2,
       3,     2,     2,     2,     0,     1,     3,     5,     2,     3,
       2,     0,     1,     2,     2,     2,     2,     4,     6,     0,
       1,     0,     1,     4,     5,     5,     3,     1,     2,     3,
       3,     2,     2,     3,     3,     0,     2,     1,     3,     3,
       3,     0,     1,     1,     3,     2,     3,     2,     2,     1,
       0,     2,     4,     2,     4,     2,     3,     1,     4,     3,
       2,     2,     4,     3,     5,     2,     4,     3,     4,     5,
       6,     1,     1,     2,     2,     3,     3,     3,     1,     0,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
       0,     3,   271,   275,   279,   273,   282,   284,   455,     0,
       0,    67,    72,    62,    63,    64,    96,    97,    98,    99,
     101,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   223,   100,   102,   103,   457,     0,     0,   153,     0,
     469,   106,   109,   107,   108,     0,     0,     0,     0,   155,
     156,   157,   158,   159,   160,     0,     0,     0,     0,     0,
     376,   384,   378,   360,   394,     0,   360,   360,   402,   362,
     415,   417,   356,   424,   360,   431,     0,   441,   149,     0,
     148,   129,   143,   143,   127,   128,     0,   117,     0,     0,
       0,     0,   447,     0,     0,     0,     9,     0,     2,     4,
       0,    12,    14,    39,    21,    20,    35,    58,    65,    66,
       0,     0,    36,    59,     0,   104,     0,   119,   120,    24,
     123,   134,   130,   131,   126,   124,   121,   122,    28,    29,
      30,    31,    32,    33,    34,    15,    17,    18,    19,    37,
      16,     0,   206,    41,    42,   112,     0,   308,     0,     0,
       0,     0,    23,    25,    38,   338,   321,   322,   323,   325,
     324,   326,   327,   328,   329,   330,   331,   332,   333,   334,
     335,   336,   337,   339,   340,   341,   342,   343,   344,   345,
      22,    26,    27,    40,   114,     0,   267,   268,     0,   280,
     277,     0,   312,     0,   349,     0,     0,     0,   347,   348,
       0,     0,     0,     0,   524,   525,   526,   529,   502,   500,
     503,   504,   501,   506,   505,   509,   508,   507,   547,     0,
       0,   528,   527,   537,   536,   535,   538,   539,   533,   534,
       0,     0,   540,   541,   542,   543,   544,   545,   530,   531,
     532,   510,   511,   512,   513,   514,   515,   516,   517,   518,
     519,   520,   521,   546,   523,   522,   193,   195,   548,     0,
     533,   534,     0,   162,    68,     0,    71,    69,    60,     0,
      73,    61,     0,     0,    90,     0,   303,   302,     0,   298,
       0,     0,   301,   272,     0,     0,   300,   274,   276,   279,
     283,   285,   287,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   457,     0,   461,
       0,     0,     0,   286,   350,   467,   468,   353,     0,    74,
      75,   369,   370,   368,   360,   360,   360,   360,   383,     0,
       0,   377,   360,   360,     0,   372,   395,   396,   365,   366,
     364,   360,   398,   400,   403,   404,   401,   410,   409,     0,
     408,   362,   412,   411,   414,   406,   416,   418,   419,   422,
       0,   421,   425,   423,   360,   428,   432,   430,   434,   435,
     436,   442,   360,     0,     0,     0,   125,     0,   141,   141,
     139,     0,   132,   133,     0,     0,   448,   451,   452,     0,
       0,    10,     1,     5,     6,     7,   271,    86,    76,    88,
      87,    92,    82,    77,    78,    80,    79,    83,    81,    84,
      93,    51,    52,    55,    53,    54,    56,   105,   135,    57,
      13,   207,     0,   110,   113,   174,   176,   182,   190,   181,
     180,   470,   477,   309,     0,   470,   189,   192,     0,     0,
      49,    48,    50,     0,   116,   269,     0,   281,   143,     0,
     456,   315,   314,   316,   319,     0,   317,   320,   318,     0,
     310,   311,     0,     0,     0,     0,     0,     0,     0,     0,
     165,     0,    70,    94,   354,    89,    91,    95,     0,     0,
     304,   288,   290,   293,   278,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   252,     0,   241,     0,     0,   261,   261,     0,
       0,     0,     0,   227,   208,   226,   224,   225,    11,     0,
       0,   458,   462,   463,   459,   460,   154,   355,   356,   360,
     385,   360,   360,   389,   360,   387,   393,   379,   381,     0,
     382,   373,   360,   374,   361,   397,   367,   399,   405,   363,
     407,   413,   420,   357,     0,   429,   433,   439,   150,     0,
     136,   144,     0,   145,   146,     0,   118,     0,   446,   449,
     450,   453,   454,     8,     0,   177,     0,     0,     0,    43,
     175,     0,     0,   475,     0,   480,     0,   481,     0,   183,
       0,    44,     0,   270,   141,     0,     0,     0,   205,   204,
     196,   203,   199,     0,     0,     0,     0,   194,   202,   173,
     168,   171,   172,   170,   169,     0,   166,   163,     0,     0,
     305,   306,   307,     0,   291,   293,     0,   292,   263,   266,
     241,   265,   241,   241,   241,     0,     0,     0,   263,     0,
       0,     0,     0,     0,     0,   261,   261,     0,     0,     0,
     241,   241,   241,   262,   241,   241,     0,     0,   241,   465,
       0,     0,   371,   386,   390,   360,   391,   388,   380,   375,
       0,     0,   426,   362,   440,   437,     0,     0,     0,     0,
     142,   140,   443,    85,   178,   179,   111,     0,   473,     0,
     471,   492,     0,     0,   499,   499,   499,   491,   476,     0,
     483,   479,   191,     0,     0,    44,    45,     0,   115,   147,
     313,   351,   352,   198,   201,   197,   200,     0,   161,     0,
       0,   295,   296,   294,   299,   241,   264,     0,     0,     0,
       0,     0,     0,   259,     0,     0,   255,   243,   244,   246,
     247,   250,   251,   248,   249,   253,   245,   209,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   466,   464,   392,
     359,   358,     0,   439,     0,   151,   137,   138,   444,   445,
       0,     0,   493,   494,   498,     0,     0,     0,   485,     0,
     478,   482,     0,   185,   188,     0,     0,   167,   164,   297,
       0,   212,   210,   213,   214,   256,   257,     0,     0,   263,
     242,   218,   219,   216,   217,   222,     0,     0,     0,     0,
       0,     0,   229,     0,     0,   211,   427,   438,     0,   474,
     472,   495,   496,   497,     0,     0,   487,   484,     0,   184,
       0,    46,   215,   258,   260,   254,     0,     0,     0,     0,
       0,     0,     0,   261,     0,   152,   488,   486,   186,    47,
       0,     0,     0,   232,   234,     0,     0,   239,   220,   228,
       0,   221,     0,   489,   230,   231,     0,     0,   235,     0,
     236,   261,   490,     0,   240,   238,     0,   233,   237
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    97,    98,    99,   100,   101,   427,   707,   440,   441,
     442,   103,   104,   105,   106,   107,   268,   271,   108,   109,
     399,   410,   400,   401,   110,   111,   112,   113,   114,   115,
     116,   579,   423,   117,   118,   119,   120,   376,   121,   563,
     378,   122,   123,   124,   125,   126,   127,   128,   129,   130,
     131,   132,   133,   134,   262,   615,   616,   424,   425,   426,
     428,   429,   704,   782,   135,   136,   137,   138,   139,   140,
     256,   257,   600,   141,   142,   306,   516,   811,   812,   814,
     856,   857,   649,   504,   732,   733,   652,   725,   653,   143,
     144,   145,   146,   147,   148,   190,   149,   150,   151,   283,
     284,   625,   626,   285,   623,   434,   152,   153,   202,   154,
     361,   335,   355,   155,   340,   156,   323,   157,   158,   159,
     331,   160,   328,   161,   162,   163,   164,   346,   165,   166,
     167,   168,   169,   363,   170,   171,   367,   172,   173,   675,
     372,   174,   175,   176,   177,   178,   179,   203,   180,   521,
     522,   523,   181,   182,   183,   583,   430,   431,   698,   699,
     775,   258,   631
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -600
static const yytype_int16 yypact[] =
{
     715,  -600,   -34,  -600,   -73,  -600,  -600,  -600,   109,  2583,
    3465,    80,    52,  -600,  -600,  -600,  -600,  -600,  -600,  -115,
    -600,  3465,  3465,    84,  1104,  1104,  1104,  1104,  1104,  1104,
    1104,   277,  -600,  -600,  -600,  1616,    72,   118,  -600,   -54,
    -600,  -600,  -600,  -600,  -600,    93,  3465,  3465,   167,  -600,
    -600,  -600,  -600,  -600,  -600,    99,    74,   153,   163,    96,
    -600,    51,    20,    35,   -61,   -72,   -93,   101,   -32,   125,
     186,   -83,    31,   164,   -85,   165,    37,   166,    48,   267,
    -600,  -600,   293,   293,  -600,  -600,   233,   247,   267,   267,
     312,   319,  -600,   174,   238,   -87,   276,   330,   932,  -600,
      -4,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
     354,   236,  -600,  -600,   470,  3612,  1828,  -600,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,  1828,   285,  -600,  -600,   110,  1419,   260,  3465,  1419,
    3465,   159,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,  -600,  -600,  -600,     2,   230,  -600,  -600,   107,   288,
    -600,   291,   262,   122,  -600,    22,   251,   -15,  -600,  -600,
      26,   235,   194,   206,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,   146,
     147,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  3465,  3465,
    3465,  3465,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,   151,  -600,   150,
    -600,  -600,   156,   152,  -600,   308,  -600,  -600,  -600,  3465,
    -600,  -600,   258,   353,   343,  3465,  -600,  -600,   157,  -600,
     158,   160,  -600,  -600,   338,  1104,   161,  -600,  -600,   -73,
    -600,  -600,  -600,   300,   168,   175,   176,   177,   212,   178,
     189,   191,   192,   195,   141,   196,  1266,  -600,   169,   129,
     302,   329,   269,  -600,  -600,  -600,  -600,  -600,   304,  -600,
    -600,  -600,   270,  -600,   -75,   -19,   -40,   101,  -600,   -31,
     336,  -600,   101,   117,   337,  -600,  -600,   275,  -600,   340,
    -600,   101,  -600,  -600,  -600,   341,  -600,  -600,  -600,   342,
    -600,   263,   281,  -600,  -600,  -600,  -600,  -600,   283,  -600,
     286,  -600,   214,  -600,   101,  -600,   287,  -600,  -600,  -600,
    -600,  -600,   101,   348,   240,  3465,  -600,   352,   273,   273,
     222,  3465,  -600,  -600,   357,   358,   298,   299,  -600,   303,
     305,  -600,  -600,  -600,  -600,   400,   -64,  -600,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
     234,  -600,  -600,  -600,  -600,  -600,  -600,    28,  -600,  -600,
    -600,  -600,    17,   432,   110,  -600,  -600,  -600,  -600,  -600,
    -600,    29,  2283,  -600,   435,    29,  -600,  -600,   363,   375,
    -600,  -600,  -600,   439,     9,  -600,   360,  -600,   293,   243,
    -600,  -600,  -600,  -600,  -600,   384,  -600,  -600,  -600,   301,
    -600,  -600,  2730,  2730,   248,   249,   257,   259,  2583,  2730,
      47,  3465,  -600,  -600,  -600,  -600,  -600,  -600,  3465,   332,
     123,  -600,  -600,  3024,  -600,   261,  3171,  3171,  3171,  3171,
     246,   265,    64,   268,   271,   272,   274,   278,   279,   280,
     282,   292,  -600,   294,  -600,  3171,  3171,  3171,  3171,  3171,
     295,   296,  3171,  -600,  -600,  -600,  -600,  -600,  -600,   418,
     421,  -600,   306,  -600,  -600,  -600,  -600,  -600,   344,   101,
    -600,   101,    21,  -600,   101,  -600,  -600,  -600,  -600,   404,
    -600,  -600,   101,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,  -600,  -600,   -98,   345,  -600,  -600,   -70,   417,   297,
      10,  -600,   412,  -600,  -600,   416,  -600,   307,  -600,  -600,
    -600,  -600,  -600,  -600,   413,  -600,   289,    66,  1419,  -600,
    -600,  1989,  2136,    14,  3465,  -600,  3465,  -600,  1419,    27,
     428,   498,  1419,  -600,   273,   433,   399,   309,  -600,  -600,
    -600,  -600,  -600,  2730,  2730,  2730,  2730,  -600,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,   -69,  -600,   310,  3465,   311,
    -600,  -600,  -600,   313,  -600,  3024,   314,  -600,  3171,  -600,
    -600,  -600,  -600,  -600,  -600,  3465,  3465,   436,  3171,  3171,
    3171,  3171,  3171,  3171,  3171,  3171,  3171,  3171,  3171,   -16,
    -600,  -600,  -600,  -600,  -600,  -600,   315,   316,  -600,  -600,
     457,   129,  -600,  -600,  -600,   101,  -600,  -600,  -600,  -600,
     374,   377,   322,   263,  -600,  -600,   334,   441,  3465,  3465,
    -600,  -600,   -29,  -600,  -600,  -600,  -600,  3465,  -600,  3465,
    -600,  -600,   504,   507,   502,   502,   502,  -600,  -600,  3465,
    -600,  2877,  -600,  3465,    70,   498,  -600,   519,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,    47,  -600,  3465,
     320,  -600,  -600,  -600,  -600,  -600,  -600,     1,     4,     7,
      13,   321,   323,   326,   317,   327,  -600,  -600,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,   212,    15,
      16,    18,    19,    23,   113,   472,    24,  -600,  -600,  -600,
    -600,  -600,   461,   355,   331,  -600,  -600,  -600,  -600,  -600,
     328,   333,  -600,  -600,  -600,   525,   528,   529,  2436,  3465,
    -600,  -600,   -68,  -600,  -600,   539,  3465,  -600,  -600,  -600,
      25,  -600,  -600,  -600,  -600,  -600,  -600,  3465,  2730,  3171,
    -600,  -600,  -600,  -600,  -600,  -600,   335,   339,   346,   349,
     351,   350,   359,   361,   356,  -600,  -600,  -600,   473,  -600,
    -600,  -600,  -600,  -600,  3465,  3465,  -600,  -600,  3465,  -600,
    3465,  -600,  -600,  -600,  -600,  -600,  3171,  3171,   -81,   362,
     474,   513,   113,  3171,   517,  -600,  -600,  3318,  -600,  -600,
     364,   365,   368,  -600,  -600,   369,   372,   367,  -600,  -600,
    -137,  -600,  3465,  -600,  -600,  -600,  3171,  3171,  -600,   474,
    -600,  3171,  -600,   373,  -600,  -600,   376,  -600,  -600
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -600,  -600,  -600,   468,   532,   -30,     6,  -135,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,  -600,   396,  -600,  -600,  -600,  -600,   207,  -600,  -350,
     -80,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,  -600,  -600,   458,  -600,  -600,  -132,  -159,  -374,  -600,
    -144,  -122,  -600,  -600,    57,    83,    91,   154,   155,  -600,
     119,  -600,  -429,   459,  -600,  -600,  -600,  -242,  -600,  -600,
    -266,  -600,  -537,  -143,  -193,  -600,  -494,  -599,  -479,  -600,
    -600,  -600,  -600,  -600,  -600,   318,  -600,  -600,  -600,   284,
     325,  -600,   -17,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
      78,   -43,  -340,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -152,
    -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,  -600,
    -600,   -49,  -600,  -600,   503,   179,  -600,  -146,  -600,  -600,
    -396,  -600,    -9
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -548
static const yytype_int16 yytable[] =
{
     259,   263,   435,   379,   437,   436,   102,   630,   632,   633,
     634,   550,   273,   274,   654,   286,   286,   286,   286,   286,
     286,   286,   396,   342,   343,   444,   650,   651,  -112,   564,
     655,   365,   747,   658,   602,  -113,   678,   314,   315,   736,
     608,   102,   691,   692,   693,   694,   695,   696,   394,   791,
     580,   184,   792,   185,   673,   793,    24,   344,   537,   670,
     768,   794,   852,   801,   802,   388,   803,   804,   389,   357,
     580,   805,   815,   832,   373,   396,   334,   529,   272,  -187,
    -547,   451,   338,   185,   334,   456,   419,   269,   454,   870,
     189,   336,   871,   727,   334,   728,   729,   730,   609,   264,
     265,   339,   341,   358,   102,   364,   418,   270,   455,    24,
     337,   420,   534,   749,   750,   751,   398,   752,   753,   275,
     411,   756,   102,   769,   671,   374,   452,   674,   191,   334,
     390,   345,   538,   531,   309,   266,   610,   432,   539,   432,
     432,   432,   611,   612,   532,   853,   457,   102,   267,   726,
     334,   743,   744,   717,   828,   186,   187,   718,   829,   726,
     737,   738,   739,   740,   741,   742,   188,   403,   745,   746,
     312,   412,   613,   665,   713,   714,   715,   716,   310,   311,
     347,   329,   192,   359,   193,   186,   187,   360,   790,   330,
     334,   313,   519,   404,   520,   348,   188,   413,   332,   614,
     835,   405,   333,   580,   334,   414,   748,   581,   194,   582,
     195,   196,   324,   325,   197,   326,   327,   395,   317,   464,
     465,   466,   467,   748,   198,   199,   748,   422,   200,   748,
     368,   369,   370,   679,   422,   748,   697,   748,   748,   806,
     748,   748,   422,   575,   709,   748,   748,   748,  -187,  -547,
     321,   322,   703,   807,   808,   809,   316,   810,   438,   439,
     473,   490,   491,   577,   406,   407,   477,   318,   415,   416,
     334,  -346,   620,   621,   622,   356,   286,   460,   461,   510,
     511,   530,   533,   535,   536,   542,   334,   637,   638,   541,
     543,   422,   685,   375,   349,   382,   383,   517,   547,   776,
     777,   492,   201,   350,   351,   319,   352,   353,   354,   287,
     288,   289,   290,   291,   292,   320,   362,   366,   371,   377,
     726,   555,   380,   381,   384,   385,   386,   387,   391,   557,
     392,   409,    31,   763,   433,   422,   445,   446,   493,   494,
     495,   496,   447,   497,   498,   499,   500,   501,   502,   860,
     448,   449,   503,   450,   453,   458,   459,   850,   851,   396,
       3,     4,     5,     6,     7,   293,   560,    10,   594,   834,
     462,   463,   566,   468,   469,   471,   472,   876,   474,   475,
     476,   470,   478,   479,   481,   480,   483,   873,   874,   485,
     518,   524,   486,    24,    25,    26,    27,    28,    29,   487,
     488,   489,   505,   294,   295,   296,   297,   298,   299,   300,
     301,   302,   303,   506,   304,   507,   508,   305,   525,   509,
     512,   526,   528,   587,   527,   540,   544,   545,   576,   546,
     548,   549,   349,   551,   686,   552,   554,   558,   553,   556,
     559,   561,   562,    40,   702,   565,   567,   568,   708,   397,
     569,   570,   573,   601,   601,   571,   574,   572,   578,   259,
     601,   588,   617,   590,   591,   592,   593,   595,   596,   618,
     597,   635,   603,   604,   627,   396,     3,     4,     5,     6,
       7,   605,   659,   606,   619,   628,   663,   660,   664,   666,
     636,   667,   639,   668,   676,   640,   641,   672,   642,   669,
     360,   680,   643,   644,   645,   681,   646,   706,   683,    24,
      25,    26,    27,    28,    29,   684,   647,   705,   648,   656,
     657,   677,   710,   711,   757,   735,   760,   712,   661,   761,
     765,   682,   772,   719,   764,   773,   774,   721,   786,   722,
     724,   798,   754,   755,   762,   813,   789,   795,   797,   796,
     816,   799,   674,   821,   819,   818,   822,   823,   830,   820,
     836,   858,   845,   855,   837,   861,   393,   308,   402,   432,
     785,   838,   688,   690,   839,   700,   840,   701,   841,   432,
     443,   842,   784,   432,   844,   787,   843,   607,   854,   869,
     864,   865,   866,   867,   601,   601,   601,   601,   868,   877,
     859,   421,   878,   875,   833,   800,   662,   484,   723,   720,
     482,   817,   758,   408,   589,     0,   627,     0,     0,     0,
       0,     0,   759,     0,     0,     0,   731,   734,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     831,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   766,
     767,     0,     0,     0,     0,     0,     0,     0,   770,     0,
     771,     0,     0,     0,   849,     0,     0,     0,     0,     0,
     778,     0,   781,     0,   783,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     788,     0,     0,     0,     0,     0,     1,     0,     0,     0,
       2,     3,     4,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,     0,     0,    16,    17,    18,    19,
      20,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      21,    22,     0,    23,    24,    25,    26,    27,    28,    29,
      30,     0,    31,     0,     0,     0,     0,     0,     0,   826,
     827,    32,    33,    34,     0,    35,    36,   432,     0,     0,
       0,     0,     0,     0,    37,     0,     0,     0,   734,   601,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    38,    39,     0,    40,    41,    42,    43,    44,     0,
       0,     0,    45,     0,     0,   846,   847,     0,     0,   848,
      46,   432,    47,    48,    49,    50,    51,    52,    53,    54,
       0,     0,     0,     0,     0,     0,    55,    56,   863,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   872,     0,     0,     0,     0,     0,     0,
      57,    58,     0,     0,     0,     0,     0,     0,    59,     0,
       0,     0,     0,    60,    61,    62,     0,     0,     0,     0,
       0,    63,     0,     0,     0,    64,     0,    65,     0,    66,
      67,    68,    69,     0,     0,     0,     0,     0,     0,    70,
      71,     0,    72,    73,    74,     0,    75,    76,     0,     0,
       0,    77,     0,    78,    79,     0,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,     0,     0,     0,     0,    96,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,     0,     0,    16,    17,    18,    19,    20,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    21,    22,     0,
      23,    24,    25,    26,    27,    28,    29,    30,     0,    31,
       0,     0,     0,     0,     0,     0,     0,     0,    32,    33,
      34,     0,    35,    36,     0,     0,     0,     0,     0,     0,
       0,    37,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    38,    39,
       0,    40,    41,    42,    43,    44,     0,     0,     0,    45,
       0,     0,     0,     0,     0,     0,     0,    46,     0,    47,
      48,    49,    50,    51,    52,    53,    54,     0,     0,     0,
       0,     0,     0,    55,    56,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    57,    58,     0,
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
       0,   280,   281,   238,   239,   240,   282,     0,   241,   242,
       0,   243,     0,     0,     0,   244,   245,   513,   246,     0,
       0,   247,   248,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     249,     0,     0,     0,   250,     0,     0,     0,   251,   252,
       0,   253,     0,     0,   254,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   514,     0,     0,     0,   515,     0,
       0,   255,   204,   205,   206,   207,   208,   209,   210,     0,
       0,     0,   211,     0,     0,   212,     0,   213,   214,   215,
     216,   217,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   218,     0,     0,     0,     0,
       0,     0,     0,     0,   221,   222,     0,     0,   223,   224,
     225,   226,   227,     0,     0,     0,     0,     0,     0,     0,
       0,   260,   261,     0,     0,   232,   233,   234,   235,   236,
     237,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   238,   239,   240,     0,     0,
     241,   242,     0,   243,   396,     0,     0,   244,   245,     0,
     246,     0,     0,   247,   248,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   249,     0,     0,     0,   250,     0,    24,     0,
     251,   252,     0,   253,     0,     0,   254,     0,     0,     0,
       0,     0,     0,     0,     0,   204,   205,   206,   207,   208,
     209,   210,     0,   255,     0,   211,     0,     0,   212,     0,
     213,   214,   215,   216,   217,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   218,     0,
       0,     0,     0,     0,     0,     0,     0,   221,   222,     0,
       0,   223,   224,   225,   226,   227,     0,     0,     0,     0,
       0,     0,     0,     0,   260,   261,     0,     0,   232,   233,
     234,   235,   236,   237,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   238,   239,
     240,     0,     0,   241,   242,     0,   243,     0,     0,     0,
     244,   245,     0,   246,     0,     0,   247,   248,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   249,     0,     0,     0,   250,
       0,     0,     0,   251,   252,     0,   253,     0,     0,   254,
       0,     2,     3,     4,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,     0,   255,    16,    17,    18,
      19,    20,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    21,    22,     0,    23,    24,    25,    26,    27,    28,
      29,    30,     0,    31,     0,     0,     0,     0,     0,     0,
       0,     0,    32,    33,    34,     0,   307,    36,     0,     0,
       0,     0,     0,     0,     0,    37,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    38,    39,     0,    40,    41,    42,    43,    44,
       0,     0,     0,    45,     0,     0,     0,     0,     0,     0,
       0,    46,     0,    47,    48,    49,    50,    51,    52,    53,
      54,     0,     0,     0,     0,     0,     0,    55,    56,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    57,    58,     0,     0,     0,     0,     0,     0,    59,
       0,     0,     0,     0,    60,    61,    62,     0,     0,     0,
       0,     0,    63,     0,     0,     0,    64,     0,    65,     0,
      66,    67,    68,    69,     0,     0,     0,     0,     0,     0,
      70,    71,     0,    72,    73,    74,     0,    75,    76,     0,
       0,     0,    77,     0,    78,    79,     0,    80,    81,    82,
      83,    84,    85,    86,    87,    88,    89,    90,    91,    92,
      93,    94,    95,     2,     3,     4,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,     0,     0,    16,
      17,    18,    19,    20,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    21,    22,     0,    23,    24,    25,    26,
      27,    28,    29,    30,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    32,    33,    34,     0,   307,    36,
       0,     0,     0,     0,     0,     0,     0,    37,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    38,    39,     0,    40,    41,    42,
      43,    44,     0,     0,     0,    45,     0,     0,     0,     0,
       0,     0,     0,    46,     0,    47,    48,    49,    50,    51,
      52,    53,    54,     0,     0,     0,     0,     0,     0,    55,
      56,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    57,    58,     0,     0,     0,     0,     0,
       0,    59,     0,     0,     0,     0,    60,    61,    62,     0,
       0,     0,     0,     0,    63,     0,     0,     0,    64,     0,
      65,     0,    66,    67,    68,    69,     0,     0,     0,     0,
       0,     0,    70,    71,     0,    72,    73,    74,     0,    75,
      76,     0,     0,     0,    77,     0,    78,    79,     0,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,   204,   205,   206,   207,   208,
     209,   210,     0,     0,     0,   211,     0,     0,   212,     0,
     213,   214,   215,   216,   217,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   218,     0,
       0,     0,     0,     0,     0,     0,     0,   221,   222,     0,
       0,   223,   224,   225,   226,   227,     0,     0,     0,     0,
       0,     0,     0,     0,   260,   261,     0,     0,   232,   233,
     234,   235,   236,   237,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   238,   239,
     240,     0,     0,   241,   242,     0,   243,     0,     0,     0,
     244,   245,     0,   246,     0,     0,   247,   248,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   249,     0,     0,     0,   250,
       0,     0,     0,   251,   252,     0,   253,     0,     0,   254,
       0,     0,   204,   205,   206,   207,   208,   209,   210,     0,
       0,     0,   211,     0,     0,   212,   255,   213,   214,   215,
     216,   217,     0,     0,   687,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   218,     0,     0,     0,     0,
       0,     0,     0,     0,   221,   222,     0,     0,   223,   224,
     225,   226,   227,     0,     0,     0,     0,     0,     0,     0,
       0,   260,   261,     0,     0,   232,   233,   234,   235,   236,
     237,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   238,   239,   240,     0,     0,
     241,   242,     0,   243,     0,     0,     0,   244,   245,     0,
     246,     0,     0,   247,   248,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   249,     0,     0,     0,   250,     0,     0,     0,
     251,   252,     0,   253,     0,     0,   254,     0,     0,   204,
     205,   206,   207,   208,   209,   210,     0,     0,     0,   211,
       0,     0,   212,   255,   213,   214,   215,   216,   217,   584,
       0,   689,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   218,     0,     0,     0,     0,     0,   585,     0,
       0,   221,   222,     0,     0,   223,   224,   225,   226,   227,
       0,     0,     0,     0,     0,     0,     0,     0,   260,   261,
       0,     0,   232,   233,   234,   235,   236,   237,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   238,   239,   240,     0,     0,   241,   242,     0,
     243,     0,     0,     0,   244,   245,     0,   246,     0,     0,
     247,   248,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   249,
       0,     0,     0,   250,     0,     0,     0,   251,   252,     0,
     253,     0,     0,   254,     0,     0,     0,     0,     0,     0,
       0,     0,   204,   205,   206,   207,   208,   209,   210,     0,
     255,     0,   211,     0,     0,   212,   586,   213,   214,   215,
     216,   217,   824,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   218,     0,     0,     0,     0,
       0,     0,     0,     0,   221,   222,     0,     0,   223,   224,
     225,   226,   227,     0,     0,     0,     0,     0,     0,     0,
       0,   260,   261,     0,     0,   232,   233,   234,   235,   236,
     237,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   238,   239,   240,     0,     0,
     241,   242,     0,   243,     0,     0,     0,   244,   245,     0,
     246,     0,     0,   247,   248,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   249,     0,     0,     0,   250,     0,     0,     0,
     251,   252,     0,   253,     0,     0,   254,     0,     0,   204,
     205,   206,   207,   208,   209,   210,     0,     0,     0,   211,
       0,     0,   212,   255,   213,   214,   215,   216,   217,   825,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   218,     0,     0,     0,     0,     0,   219,   220,
       0,   221,   222,     0,     0,   223,   224,   225,   226,   227,
       0,     0,     0,     0,     0,     0,     0,     0,   228,   229,
     230,   231,   232,   233,   234,   235,   236,   237,     0,     0,
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
     233,   234,   235,   236,   237,   598,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   238,
     239,   240,   599,     0,   241,   242,     0,   243,     0,     0,
       0,   244,   245,     0,   246,     0,     0,   247,   248,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   249,     0,     0,     0,
     250,     0,     0,     0,   251,   252,     0,   253,     0,     0,
     254,     0,     0,   204,   205,   206,   207,   208,   209,   210,
       0,     0,     0,   211,     0,     0,   212,   255,   213,   214,
     215,   216,   217,   779,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   218,     0,     0,     0,
       0,     0,   780,     0,     0,   221,   222,     0,     0,   223,
     224,   225,   226,   227,     0,     0,     0,     0,     0,     0,
       0,     0,   260,   261,     0,     0,   232,   233,   234,   235,
     236,   237,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   238,   239,   240,     0,
       0,   241,   242,     0,   243,     0,     0,     0,   244,   245,
       0,   246,     0,     0,   247,   248,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   249,     0,     0,     0,   250,     0,     0,
       0,   251,   252,     0,   253,     0,     0,   254,     0,     0,
     204,   205,   206,   207,   208,   209,   210,     0,     0,     0,
     211,     0,     0,   212,   255,   213,   214,   215,   216,   217,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   218,     0,     0,     0,     0,     0,     0,
       0,     0,   221,   222,     0,     0,   223,   224,   225,   226,
     227,     0,     0,     0,     0,     0,     0,     0,     0,   260,
     261,     0,     0,   232,   233,   234,   235,   236,   237,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   238,   239,   240,   624,     0,   241,   242,
       0,   243,     0,     0,     0,   244,   245,     0,   246,     0,
       0,   247,   248,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     249,     0,     0,     0,   250,     0,     0,     0,   251,   252,
       0,   253,     0,     0,   254,     0,     0,   204,   205,   206,
     207,   208,   209,   210,     0,     0,     0,   211,     0,     0,
     212,   255,   213,   214,   215,   216,   217,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     218,     0,     0,     0,     0,   629,     0,     0,     0,   221,
     222,     0,     0,   223,   224,   225,   226,   227,     0,     0,
       0,     0,     0,     0,     0,     0,   260,   261,     0,     0,
     232,   233,   234,   235,   236,   237,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     238,   239,   240,     0,     0,   241,   242,     0,   243,     0,
       0,     0,   244,   245,     0,   246,     0,     0,   247,   248,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   249,     0,     0,
       0,   250,     0,     0,     0,   251,   252,     0,   253,     0,
       0,   254,     0,     0,   204,   205,   206,   207,   208,   209,
     210,     0,     0,     0,   211,     0,     0,   212,   255,   213,
     214,   215,   216,   217,   862,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   218,     0,     0,
       0,     0,     0,     0,     0,     0,   221,   222,     0,     0,
     223,   224,   225,   226,   227,     0,     0,     0,     0,     0,
       0,     0,     0,   260,   261,     0,     0,   232,   233,   234,
     235,   236,   237,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   238,   239,   240,
       0,     0,   241,   242,     0,   243,     0,     0,     0,   244,
     245,     0,   246,     0,     0,   247,   248,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   249,     0,     0,     0,   250,     0,
       0,     0,   251,   252,     0,   253,     0,     0,   254,     0,
       0,   204,   205,   206,   207,   208,   209,   210,     0,     0,
       0,   211,     0,     0,   212,   255,   213,   214,   215,   216,
     217,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   218,     0,     0,     0,     0,     0,
       0,     0,     0,   221,   222,     0,     0,   223,   224,   225,
     226,   227,     0,     0,     0,     0,     0,     0,     0,     0,
     260,   261,     0,     0,   232,   233,   234,   235,   236,   237,
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
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   417,     0,     0,     0,     0,     0,     0,     0,     0,
     221,   222,     0,     0,   223,   224,   225,   226,   227,     0,
       0,     0,     0,     0,     0,     0,     0,   260,   261,     0,
       0,   232,   233,   234,   235,   236,   237,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   238,   239,   240,     0,     0,   241,   242,     0,   243,
       0,     0,     0,   244,   245,     0,   246,     0,     0,   247,
     248,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   249,     0,
       0,     0,   250,     0,     0,     0,   251,   252,     0,   253,
       0,     0,   254,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   255
};

static const yytype_int16 yycheck[] =
{
       9,    10,   148,    83,   150,   149,     0,   486,   487,   488,
     489,   351,    21,    22,   508,    24,    25,    26,    27,    28,
      29,    30,     5,    66,    67,   184,   505,   506,    26,   379,
     509,    74,    48,   512,   463,    26,    26,    46,    47,   638,
     469,    35,    28,    29,    30,    31,    32,    33,    52,    48,
     424,    85,    48,   117,   124,    48,    39,    89,    89,   157,
      89,    48,   143,    48,    48,   152,    48,    48,   155,   152,
     444,    48,    48,    48,    26,     5,   169,   152,   193,    52,
      52,    59,   154,   117,   169,    59,   116,    35,   103,   226,
     163,   152,   229,   630,   169,   632,   633,   634,    51,    19,
      20,   173,   195,   186,    98,   190,   115,    55,   123,    39,
     171,   141,   152,   650,   651,   652,   110,   654,   655,    35,
     114,   658,   116,   152,   222,    77,   104,   197,    19,   169,
     217,   163,   163,   152,    62,    55,    89,   146,   169,   148,
     149,   150,    95,    96,   163,   226,   120,   141,    68,   628,
     169,   645,   646,   222,   222,   219,   220,   226,   226,   638,
     639,   640,   641,   642,   643,   644,   230,   110,   647,   648,
     224,   114,   125,   152,   603,   604,   605,   606,    60,    61,
      55,   161,    73,   152,    75,   219,   220,   156,   725,   169,
     169,    98,    63,   110,    65,    70,   230,   114,   163,   152,
     799,   110,   167,   577,   169,   114,   222,   178,    99,   180,
     101,   102,   161,   162,   105,   164,   165,   221,   119,   228,
     229,   230,   231,   222,   115,   116,   222,   225,   119,   222,
     193,   194,   195,   223,   225,   222,   222,   222,   222,   126,
     222,   222,   225,   226,   594,   222,   222,   222,   221,   221,
     154,   155,   225,   140,   141,   142,    89,   144,    99,   100,
     269,    49,    50,   422,   110,   110,   275,   193,   114,   114,
     169,   162,   149,   150,   151,    89,   285,    71,    72,   138,
     139,   324,   325,   326,   327,   168,   169,   223,   224,   332,
     333,   225,   226,    26,   169,    88,    89,   306,   341,   695,
     696,    89,   193,   178,   179,   152,   181,   182,   183,    25,
      26,    27,    28,    29,    30,   152,   152,   152,   152,    26,
     799,   364,    89,    76,    12,     6,   152,    89,    52,   372,
       0,    95,    47,   673,    74,   225,   106,   230,   126,   127,
     128,   129,    54,   131,   132,   133,   134,   135,   136,   843,
      59,    89,   140,   231,   103,   120,   162,   836,   837,     5,
       6,     7,     8,     9,    10,    88,   375,    13,   448,   798,
     224,   224,   381,   222,   224,   223,    68,   871,   120,    26,
      37,   225,   225,   225,    46,   225,   225,   866,   867,    89,
     221,    89,   224,    39,    40,    41,    42,    43,    44,   224,
     224,   224,   224,   126,   127,   128,   129,   130,   131,   132,
     133,   134,   135,   224,   137,   224,   224,   140,    89,   224,
     224,   152,   152,   432,   120,    89,    89,   152,   422,    89,
      89,    89,   169,   152,   578,   152,   222,    89,   152,   152,
     200,    89,   169,    89,   588,   223,    89,    89,   592,    95,
     152,   152,    52,   462,   463,   152,   222,   152,    26,   468,
     469,    26,   471,   100,    89,    26,   106,   224,    84,   478,
     169,   225,   224,   224,   483,     5,     6,     7,     8,     9,
      10,   224,    64,   224,   152,   224,   529,    66,   531,   532,
     225,   534,   224,    89,    77,   224,   224,   152,   224,   542,
     156,    89,   224,   224,   224,    89,   224,     9,    95,    39,
      40,    41,    42,    43,    44,   226,   224,    89,   224,   224,
     224,   224,    89,   124,    67,    89,   152,   218,   222,   152,
      89,   224,    28,   223,   200,    28,    34,   226,    19,   226,
     226,   224,   227,   227,   222,    73,   226,   226,   222,   226,
      89,   224,   197,    28,   226,   224,    28,    28,    19,   226,
     225,    48,    89,    89,   225,    48,    98,    35,   110,   578,
     705,   225,   581,   582,   225,   584,   225,   586,   228,   588,
     184,   222,   704,   592,   228,   717,   225,   468,   226,   222,
     226,   226,   224,   224,   603,   604,   605,   606,   226,   226,
     842,   142,   226,   869,   797,   748,   528,   289,   625,   618,
     285,   763,   661,   110,   435,    -1,   625,    -1,    -1,    -1,
      -1,    -1,   665,    -1,    -1,    -1,   635,   636,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     786,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   678,
     679,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   687,    -1,
     689,    -1,    -1,    -1,   830,    -1,    -1,    -1,    -1,    -1,
     699,    -1,   701,    -1,   703,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     719,    -1,    -1,    -1,    -1,    -1,     1,    -1,    -1,    -1,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    -1,    -1,    21,    22,    23,    24,
      25,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      35,    36,    -1,    38,    39,    40,    41,    42,    43,    44,
      45,    -1,    47,    -1,    -1,    -1,    -1,    -1,    -1,   778,
     779,    56,    57,    58,    -1,    60,    61,   786,    -1,    -1,
      -1,    -1,    -1,    -1,    69,    -1,    -1,    -1,   797,   798,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    86,    87,    -1,    89,    90,    91,    92,    93,    -1,
      -1,    -1,    97,    -1,    -1,   824,   825,    -1,    -1,   828,
     105,   830,   107,   108,   109,   110,   111,   112,   113,   114,
      -1,    -1,    -1,    -1,    -1,    -1,   121,   122,   847,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   862,    -1,    -1,    -1,    -1,    -1,    -1,
     145,   146,    -1,    -1,    -1,    -1,    -1,    -1,   153,    -1,
      -1,    -1,    -1,   158,   159,   160,    -1,    -1,    -1,    -1,
      -1,   166,    -1,    -1,    -1,   170,    -1,   172,    -1,   174,
     175,   176,   177,    -1,    -1,    -1,    -1,    -1,    -1,   184,
     185,    -1,   187,   188,   189,    -1,   191,   192,    -1,    -1,
      -1,   196,    -1,   198,   199,    -1,   201,   202,   203,   204,
     205,   206,   207,   208,   209,   210,   211,   212,   213,   214,
     215,   216,    -1,    -1,    -1,    -1,   221,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,    16,    17,
      18,    -1,    -1,    21,    22,    23,    24,    25,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    35,    36,    -1,
      38,    39,    40,    41,    42,    43,    44,    45,    -1,    47,
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
      -1,    -1,    -1,    -1,    -1,   153,    -1,    -1,    -1,    -1,
     158,   159,   160,    -1,    -1,    -1,    -1,    -1,   166,    -1,
      -1,    -1,   170,    -1,   172,     1,   174,   175,   176,   177,
      -1,    -1,    -1,    -1,    -1,    -1,   184,   185,    -1,   187,
     188,   189,    -1,   191,   192,    -1,    -1,    -1,   196,    -1,
     198,   199,    -1,   201,   202,   203,   204,   205,   206,   207,
     208,   209,   210,   211,   212,   213,   214,   215,   216,    -1,
      46,    -1,    -1,   221,    -1,    -1,    52,    -1,    -1,    -1,
      56,    57,    58,    59,    60,    61,    62,    -1,    -1,    -1,
      66,    -1,    68,    69,    -1,    71,    72,    73,    74,    75,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    89,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    98,    99,    -1,    -1,   102,   103,   104,   105,
     106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   115,
     116,    -1,    -1,   119,   120,   121,   122,   123,   124,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     136,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   147,   148,   149,   150,   151,   152,    -1,   154,   155,
      -1,   157,    -1,    -1,    -1,   161,   162,     1,   164,    -1,
      -1,   167,   168,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     186,    -1,    -1,    -1,   190,    -1,    -1,    -1,   194,   195,
      -1,   197,    -1,    -1,   200,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    48,    -1,    -1,    -1,    52,    -1,
      -1,   217,    56,    57,    58,    59,    60,    61,    62,    -1,
      -1,    -1,    66,    -1,    -1,    69,    -1,    71,    72,    73,
      74,    75,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    89,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    98,    99,    -1,    -1,   102,   103,
     104,   105,   106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   115,   116,    -1,    -1,   119,   120,   121,   122,   123,
     124,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   149,   150,   151,    -1,    -1,
     154,   155,    -1,   157,     5,    -1,    -1,   161,   162,    -1,
     164,    -1,    -1,   167,   168,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   186,    -1,    -1,    -1,   190,    -1,    39,    -1,
     194,   195,    -1,   197,    -1,    -1,   200,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    56,    57,    58,    59,    60,
      61,    62,    -1,   217,    -1,    66,    -1,    -1,    69,    -1,
      71,    72,    73,    74,    75,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    89,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    98,    99,    -1,
      -1,   102,   103,   104,   105,   106,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   115,   116,    -1,    -1,   119,   120,
     121,   122,   123,   124,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   149,   150,
     151,    -1,    -1,   154,   155,    -1,   157,    -1,    -1,    -1,
     161,   162,    -1,   164,    -1,    -1,   167,   168,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   186,    -1,    -1,    -1,   190,
      -1,    -1,    -1,   194,   195,    -1,   197,    -1,    -1,   200,
      -1,     5,     6,     7,     8,     9,    10,    11,    12,    13,
      14,    15,    16,    17,    18,    -1,   217,    21,    22,    23,
      24,    25,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    35,    36,    -1,    38,    39,    40,    41,    42,    43,
      44,    45,    -1,    47,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    56,    57,    58,    -1,    60,    61,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    69,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    86,    87,    -1,    89,    90,    91,    92,    93,
      -1,    -1,    -1,    97,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   105,    -1,   107,   108,   109,   110,   111,   112,   113,
     114,    -1,    -1,    -1,    -1,    -1,    -1,   121,   122,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   145,   146,    -1,    -1,    -1,    -1,    -1,    -1,   153,
      -1,    -1,    -1,    -1,   158,   159,   160,    -1,    -1,    -1,
      -1,    -1,   166,    -1,    -1,    -1,   170,    -1,   172,    -1,
     174,   175,   176,   177,    -1,    -1,    -1,    -1,    -1,    -1,
     184,   185,    -1,   187,   188,   189,    -1,   191,   192,    -1,
      -1,    -1,   196,    -1,   198,   199,    -1,   201,   202,   203,
     204,   205,   206,   207,   208,   209,   210,   211,   212,   213,
     214,   215,   216,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,    16,    17,    18,    -1,    -1,    21,
      22,    23,    24,    25,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    35,    36,    -1,    38,    39,    40,    41,
      42,    43,    44,    45,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    56,    57,    58,    -1,    60,    61,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    69,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    86,    87,    -1,    89,    90,    91,
      92,    93,    -1,    -1,    -1,    97,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   105,    -1,   107,   108,   109,   110,   111,
     112,   113,   114,    -1,    -1,    -1,    -1,    -1,    -1,   121,
     122,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   145,   146,    -1,    -1,    -1,    -1,    -1,
      -1,   153,    -1,    -1,    -1,    -1,   158,   159,   160,    -1,
      -1,    -1,    -1,    -1,   166,    -1,    -1,    -1,   170,    -1,
     172,    -1,   174,   175,   176,   177,    -1,    -1,    -1,    -1,
      -1,    -1,   184,   185,    -1,   187,   188,   189,    -1,   191,
     192,    -1,    -1,    -1,   196,    -1,   198,   199,    -1,   201,
     202,   203,   204,   205,   206,   207,   208,   209,   210,   211,
     212,   213,   214,   215,   216,    56,    57,    58,    59,    60,
      61,    62,    -1,    -1,    -1,    66,    -1,    -1,    69,    -1,
      71,    72,    73,    74,    75,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    89,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    98,    99,    -1,
      -1,   102,   103,   104,   105,   106,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   115,   116,    -1,    -1,   119,   120,
     121,   122,   123,   124,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   149,   150,
     151,    -1,    -1,   154,   155,    -1,   157,    -1,    -1,    -1,
     161,   162,    -1,   164,    -1,    -1,   167,   168,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   186,    -1,    -1,    -1,   190,
      -1,    -1,    -1,   194,   195,    -1,   197,    -1,    -1,   200,
      -1,    -1,    56,    57,    58,    59,    60,    61,    62,    -1,
      -1,    -1,    66,    -1,    -1,    69,   217,    71,    72,    73,
      74,    75,    -1,    -1,   225,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    89,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    98,    99,    -1,    -1,   102,   103,
     104,   105,   106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   115,   116,    -1,    -1,   119,   120,   121,   122,   123,
     124,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   149,   150,   151,    -1,    -1,
     154,   155,    -1,   157,    -1,    -1,    -1,   161,   162,    -1,
     164,    -1,    -1,   167,   168,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   186,    -1,    -1,    -1,   190,    -1,    -1,    -1,
     194,   195,    -1,   197,    -1,    -1,   200,    -1,    -1,    56,
      57,    58,    59,    60,    61,    62,    -1,    -1,    -1,    66,
      -1,    -1,    69,   217,    71,    72,    73,    74,    75,    76,
      -1,   225,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    89,    -1,    -1,    -1,    -1,    -1,    95,    -1,
      -1,    98,    99,    -1,    -1,   102,   103,   104,   105,   106,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   115,   116,
      -1,    -1,   119,   120,   121,   122,   123,   124,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   149,   150,   151,    -1,    -1,   154,   155,    -1,
     157,    -1,    -1,    -1,   161,   162,    -1,   164,    -1,    -1,
     167,   168,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   186,
      -1,    -1,    -1,   190,    -1,    -1,    -1,   194,   195,    -1,
     197,    -1,    -1,   200,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    56,    57,    58,    59,    60,    61,    62,    -1,
     217,    -1,    66,    -1,    -1,    69,   223,    71,    72,    73,
      74,    75,    76,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    89,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    98,    99,    -1,    -1,   102,   103,
     104,   105,   106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   115,   116,    -1,    -1,   119,   120,   121,   122,   123,
     124,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   149,   150,   151,    -1,    -1,
     154,   155,    -1,   157,    -1,    -1,    -1,   161,   162,    -1,
     164,    -1,    -1,   167,   168,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   186,    -1,    -1,    -1,   190,    -1,    -1,    -1,
     194,   195,    -1,   197,    -1,    -1,   200,    -1,    -1,    56,
      57,    58,    59,    60,    61,    62,    -1,    -1,    -1,    66,
      -1,    -1,    69,   217,    71,    72,    73,    74,    75,   223,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    89,    -1,    -1,    -1,    -1,    -1,    95,    96,
      -1,    98,    99,    -1,    -1,   102,   103,   104,   105,   106,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   115,   116,
     117,   118,   119,   120,   121,   122,   123,   124,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   149,   150,   151,    -1,    -1,   154,   155,    -1,
     157,    -1,    -1,    -1,   161,   162,    -1,   164,    -1,    -1,
     167,   168,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   186,
      -1,    -1,    -1,   190,    -1,    -1,    -1,   194,   195,    -1,
     197,    -1,    -1,   200,    -1,    -1,    56,    57,    58,    59,
      60,    61,    62,    -1,    -1,    -1,    66,    -1,    -1,    69,
     217,    71,    72,    73,    74,    75,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    89,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    98,    99,
      -1,    -1,   102,   103,   104,   105,   106,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   115,   116,    -1,    -1,   119,
     120,   121,   122,   123,   124,   125,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   149,
     150,   151,   152,    -1,   154,   155,    -1,   157,    -1,    -1,
      -1,   161,   162,    -1,   164,    -1,    -1,   167,   168,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   186,    -1,    -1,    -1,
     190,    -1,    -1,    -1,   194,   195,    -1,   197,    -1,    -1,
     200,    -1,    -1,    56,    57,    58,    59,    60,    61,    62,
      -1,    -1,    -1,    66,    -1,    -1,    69,   217,    71,    72,
      73,    74,    75,    76,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    89,    -1,    -1,    -1,
      -1,    -1,    95,    -1,    -1,    98,    99,    -1,    -1,   102,
     103,   104,   105,   106,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   115,   116,    -1,    -1,   119,   120,   121,   122,
     123,   124,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   149,   150,   151,    -1,
      -1,   154,   155,    -1,   157,    -1,    -1,    -1,   161,   162,
      -1,   164,    -1,    -1,   167,   168,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   186,    -1,    -1,    -1,   190,    -1,    -1,
      -1,   194,   195,    -1,   197,    -1,    -1,   200,    -1,    -1,
      56,    57,    58,    59,    60,    61,    62,    -1,    -1,    -1,
      66,    -1,    -1,    69,   217,    71,    72,    73,    74,    75,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    89,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    98,    99,    -1,    -1,   102,   103,   104,   105,
     106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   115,
     116,    -1,    -1,   119,   120,   121,   122,   123,   124,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   149,   150,   151,   152,    -1,   154,   155,
      -1,   157,    -1,    -1,    -1,   161,   162,    -1,   164,    -1,
      -1,   167,   168,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     186,    -1,    -1,    -1,   190,    -1,    -1,    -1,   194,   195,
      -1,   197,    -1,    -1,   200,    -1,    -1,    56,    57,    58,
      59,    60,    61,    62,    -1,    -1,    -1,    66,    -1,    -1,
      69,   217,    71,    72,    73,    74,    75,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      89,    -1,    -1,    -1,    -1,    94,    -1,    -1,    -1,    98,
      99,    -1,    -1,   102,   103,   104,   105,   106,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   115,   116,    -1,    -1,
     119,   120,   121,   122,   123,   124,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     149,   150,   151,    -1,    -1,   154,   155,    -1,   157,    -1,
      -1,    -1,   161,   162,    -1,   164,    -1,    -1,   167,   168,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   186,    -1,    -1,
      -1,   190,    -1,    -1,    -1,   194,   195,    -1,   197,    -1,
      -1,   200,    -1,    -1,    56,    57,    58,    59,    60,    61,
      62,    -1,    -1,    -1,    66,    -1,    -1,    69,   217,    71,
      72,    73,    74,    75,    76,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    89,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    98,    99,    -1,    -1,
     102,   103,   104,   105,   106,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   115,   116,    -1,    -1,   119,   120,   121,
     122,   123,   124,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   149,   150,   151,
      -1,    -1,   154,   155,    -1,   157,    -1,    -1,    -1,   161,
     162,    -1,   164,    -1,    -1,   167,   168,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   186,    -1,    -1,    -1,   190,    -1,
      -1,    -1,   194,   195,    -1,   197,    -1,    -1,   200,    -1,
      -1,    56,    57,    58,    59,    60,    61,    62,    -1,    -1,
      -1,    66,    -1,    -1,    69,   217,    71,    72,    73,    74,
      75,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    89,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    98,    99,    -1,    -1,   102,   103,   104,
     105,   106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     115,   116,    -1,    -1,   119,   120,   121,   122,   123,   124,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   149,   150,   151,    -1,    -1,   154,
     155,    -1,   157,    -1,    -1,    -1,   161,   162,    -1,   164,
      -1,    -1,   167,   168,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   186,    -1,    -1,    -1,   190,    -1,    -1,    -1,   194,
     195,    -1,   197,    -1,    -1,   200,    -1,    -1,    56,    57,
      58,    59,    60,    61,    62,    -1,    -1,    -1,    66,    -1,
      -1,    69,   217,    71,    72,    73,    74,    75,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    89,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      98,    99,    -1,    -1,   102,   103,   104,   105,   106,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   115,   116,    -1,
      -1,   119,   120,   121,   122,   123,   124,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   149,   150,   151,    -1,    -1,   154,   155,    -1,   157,
      -1,    -1,    -1,   161,   162,    -1,   164,    -1,    -1,   167,
     168,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   186,    -1,
      -1,    -1,   190,    -1,    -1,    -1,   194,   195,    -1,   197,
      -1,    -1,   200,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   217
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
     110,   111,   112,   113,   114,   121,   122,   145,   146,   153,
     158,   159,   160,   166,   170,   172,   174,   175,   176,   177,
     184,   185,   187,   188,   189,   191,   192,   196,   198,   199,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   221,   233,   234,   235,
     236,   237,   238,   243,   244,   245,   246,   247,   250,   251,
     256,   257,   258,   259,   260,   261,   262,   265,   266,   267,
     268,   270,   273,   274,   275,   276,   277,   278,   279,   280,
     281,   282,   283,   284,   285,   296,   297,   298,   299,   300,
     301,   305,   306,   321,   322,   323,   324,   325,   326,   328,
     329,   330,   338,   339,   341,   345,   347,   349,   350,   351,
     353,   355,   356,   357,   358,   360,   361,   362,   363,   364,
     366,   367,   369,   370,   373,   374,   375,   376,   377,   378,
     380,   384,   385,   386,    85,   117,   219,   220,   230,   163,
     327,    19,    73,    75,    99,   101,   102,   105,   115,   116,
     119,   193,   340,   379,    56,    57,    58,    59,    60,    61,
      62,    66,    69,    71,    72,    73,    74,    75,    89,    95,
      96,    98,    99,   102,   103,   104,   105,   106,   115,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   149,   150,
     151,   154,   155,   157,   161,   162,   164,   167,   168,   186,
     190,   194,   195,   197,   200,   217,   302,   303,   393,   394,
     115,   116,   286,   394,    19,    20,    55,    68,   248,    35,
      55,   249,   193,   394,   394,    35,     1,    52,    68,   136,
     147,   148,   152,   331,   332,   335,   394,   331,   331,   331,
     331,   331,   331,    88,   126,   127,   128,   129,   130,   131,
     132,   133,   134,   135,   137,   140,   307,    60,   236,    62,
      60,    61,   224,    98,   394,   394,    89,   119,   193,   152,
     152,   154,   155,   348,   161,   162,   164,   165,   354,   161,
     169,   352,   163,   167,   169,   343,   152,   171,   154,   173,
     346,   195,   343,   343,    89,   163,   359,    55,    70,   169,
     178,   179,   181,   182,   183,   344,    89,   152,   186,   152,
     156,   342,   152,   365,   190,   343,   152,   368,   193,   194,
     195,   152,   372,    26,    77,    26,   269,    26,   272,   272,
      89,    76,   269,   269,    12,     6,   152,    89,   152,   155,
     217,    52,     0,   235,    52,   221,     5,    95,   238,   252,
     254,   255,   285,   296,   297,   298,   299,   300,   386,    95,
     253,   238,   296,   297,   298,   299,   300,    89,   394,   237,
     237,   305,   225,   264,   289,   290,   291,   238,   292,   293,
     388,   389,   394,    74,   337,   389,   292,   389,    99,   100,
     240,   241,   242,   264,   289,   106,   230,    54,    59,    89,
     231,    59,   104,   103,   103,   123,    59,   120,   120,   162,
      71,    72,   224,   224,   394,   394,   394,   394,   222,   224,
     225,   223,    68,   394,   120,    26,    37,   394,   225,   225,
     225,    46,   332,   225,   327,    89,   224,   224,   224,   224,
      49,    50,    89,   126,   127,   128,   129,   131,   132,   133,
     134,   135,   136,   140,   315,   224,   224,   224,   224,   224,
     138,   139,   224,     1,    48,    52,   308,   394,   221,    63,
      65,   381,   382,   383,    89,    89,   152,   120,   152,   152,
     343,   152,   163,   343,   152,   343,   343,    89,   163,   169,
      89,   343,   168,   343,    89,   152,    89,   343,    89,    89,
     344,   152,   152,   152,   222,   343,   152,   343,    89,   200,
     394,    89,   169,   271,   271,   223,   394,    89,    89,   152,
     152,   152,   152,    52,   222,   226,   238,   289,    26,   263,
     290,   178,   180,   387,    76,    95,   223,   394,    26,   387,
     100,    89,    26,   106,   272,   224,    84,   169,   125,   152,
     304,   394,   304,   224,   224,   224,   224,   302,   304,    51,
      89,    95,    96,   125,   152,   287,   288,   394,   394,   152,
     149,   150,   151,   336,   152,   333,   334,   394,   224,    94,
     320,   394,   320,   320,   320,   225,   225,   223,   224,   224,
     224,   224,   224,   224,   224,   224,   224,   224,   224,   314,
     320,   320,   318,   320,   318,   320,   224,   224,   320,    64,
      66,   222,   342,   343,   343,   152,   343,   343,    89,   343,
     157,   222,   152,   124,   197,   371,    77,   224,    26,   223,
      89,    89,   224,    95,   226,   226,   292,   225,   394,   225,
     394,    28,    29,    30,    31,    32,    33,   222,   390,   391,
     394,   394,   292,   225,   294,    89,     9,   239,   292,   271,
      89,   124,   218,   304,   304,   304,   304,   222,   226,   223,
     394,   226,   226,   334,   226,   319,   320,   314,   314,   314,
     314,   394,   316,   317,   394,    89,   319,   320,   320,   320,
     320,   320,   320,   318,   318,   320,   320,    48,   222,   314,
     314,   314,   314,   314,   227,   227,   314,    67,   383,   343,
     152,   152,   222,   344,   200,    89,   394,   394,    89,   152,
     394,   394,    28,    28,    34,   392,   392,   392,   394,    76,
      95,   394,   295,   394,   293,   239,    19,   288,   394,   226,
     314,    48,    48,    48,    48,   226,   226,   222,   224,   224,
     315,    48,    48,    48,    48,    48,   126,   140,   141,   142,
     144,   309,   310,    73,   311,    48,    89,   371,   224,   226,
     226,    28,    28,    28,    76,   223,   394,   394,   222,   226,
      19,   389,    48,   316,   304,   319,   225,   225,   225,   225,
     225,   228,   222,   225,   228,    89,   394,   394,   394,   389,
     320,   320,   143,   226,   226,    89,   312,   313,    48,   309,
     318,    48,    76,   394,   226,   226,   224,   224,   226,   222,
     226,   229,   394,   320,   320,   312,   318,   226,   226
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

  case 297:

    {
      add_hint_index(result->dbmesh_route_info_, (yyvsp[(3) - (5)].str));
      result->dbmesh_route_info_.index_count_++;
    ;}
    break;

  case 298:

    { result->has_trace_log_hint_ = true; ;}
    break;

  case 302:

    { handle_stmt_end(result); HANDLE_ACCEPT_FINISH(); ;}
    break;

  case 303:

    { yyerrok; yyclearin; ;}
    break;

  case 304:

    {;}
    break;

  case 305:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_WEAK); ;}
    break;

  case 306:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_STRONG); ;}
    break;

  case 307:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_FROZEN); ;}
    break;

  case 310:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_WARNINGS; ;}
    break;

  case 311:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_ERRORS; ;}
    break;

  case 312:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 313:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 314:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_SLAVE_HOSTS; ;}
    break;

  case 315:

    {
            result->is_binlog_related_ = true;
            result->cur_stmt_type_ = OBPROXY_T_SHOW_SLAVE_STATUS;
          ;}
    break;

  case 316:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_RELAYLOG_EVENTS; ;}
    break;

  case 317:

    { result->is_binlog_related_ = true; ;}
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

  case 350:

    { result->cur_stmt_type_ = OBPROXY_T_BINLOG_STR; ;}
    break;

  case 351:

    {
    result->cur_stmt_type_ = OBPROXY_T_SHOW_BINLOG_SERVER_FOR_TENANT;
    result->is_binlog_related_ = true;
;}
    break;

  case 352:

    { result->is_binlog_related_ = true; ;}
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

    {
;}
    break;

  case 357:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 358:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 359:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 360:

    {;}
    break;

  case 361:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 362:

    {;}
    break;

  case 363:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 365:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_KV_THREAD); ;}
    break;

  case 366:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_KV_REQUESTSTAT); ;}
    break;

  case 367:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KV_REQUESTSTAT, (yyvsp[(2) - (2)].str)); ;}
    break;

  case 369:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 370:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 371:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 372:

    {;}
    break;

  case 373:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_ALL); ;}
    break;

  case 374:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 375:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 376:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 378:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 379:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 380:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 381:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 382:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 384:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 385:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 386:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 387:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 388:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 389:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 390:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 391:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 392:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 393:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_READ_STALE); ;}
    break;

  case 394:

    {;}
    break;

  case 395:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 396:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_PROXYSM_RPC); ;}
    break;

  case 397:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_PROXYSM_RPC, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 398:

    {;}
    break;

  case 399:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 400:

    {;}
    break;

  case 402:

    {;}
    break;

  case 403:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 404:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 405:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 406:

    {;}
    break;

  case 407:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 408:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 409:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_GLOBALINDEX); ;}
    break;

  case 410:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_TABLEGROUP); ;}
    break;

  case 411:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_QUERYASYNC); ;}
    break;

  case 412:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_TABLETLS); ;}
    break;

  case 413:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_ROUTE_TABLETLS, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 414:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_RPCCTX); ;}
    break;

  case 415:

    {;}
    break;

  case 416:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 417:

    {;}
    break;

  case 418:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 419:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 420:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); SET_ICMD_ONE_ID((yyvsp[(3) - (3)].num)); ;}
    break;

  case 421:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 422:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 424:

    {;}
    break;

  case 425:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 426:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 427:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 428:

    {;}
    break;

  case 429:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 431:

    {;}
    break;

  case 432:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 433:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 434:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 435:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 436:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 437:

    {;}
    break;

  case 438:

    {;}
    break;

  case 439:

    {;}
    break;

  case 440:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_PS_ALL);;}
    break;

  case 441:

    {;}
    break;

  case 442:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 443:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 444:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 445:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 446:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 447:

    {;}
    break;

  case 448:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 449:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 450:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 451:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 452:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 453:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 454:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 457:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 458:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 459:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 460:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 467:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 468:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 470:

    {;}
    break;

  case 471:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 472:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 473:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 474:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 475:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT_FINISH();
                                                ;}
    break;

  case 476:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT_FINISH();
                                                ;}
    break;

  case 477:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 478:

    {
                                                  result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                  result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                                  result->table_info_.dblink_name_ = (yyvsp[(4) - (4)].str);
                                                 ;}
    break;

  case 479:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 480:

    {
                                      result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                      result->table_info_.dblink_name_ = (yyvsp[(2) - (2)].str);
                                    ;}
    break;

  case 481:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 482:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 483:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 484:

    {
                                                  UPDATE_ALIAS_NAME((yyvsp[(5) - (5)].str));
                                                  result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                                                  result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                                                ;}
    break;

  case 485:

    { result->table_info_.join_table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 486:

    {
            result->table_info_.join_database_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.join_table_name_ = (yyvsp[(4) - (4)].str);
          ;}
    break;

  case 487:

    {
            result->table_info_.join_table_name_ = (yyvsp[(2) - (3)].str);
            result->table_info_.join_table_alias_name_ = (yyvsp[(3) - (3)].str);
         ;}
    break;

  case 488:

    {
            result->table_info_.join_table_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.join_table_alias_name_ = (yyvsp[(4) - (4)].str);
         ;}
    break;

  case 489:

    {
            result->table_info_.join_database_name_ = (yyvsp[(2) - (5)].str);
            result->table_info_.join_table_name_ = (yyvsp[(4) - (5)].str);
            result->table_info_.join_table_alias_name_ = (yyvsp[(5) - (5)].str);
         ;}
    break;

  case 490:

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

