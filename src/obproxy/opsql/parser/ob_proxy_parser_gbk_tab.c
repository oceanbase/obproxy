
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

#define SET_AP_QUERY_ROUTE_POLICY(policy_type) \
do {\
  if (OBPROXY_AP_QUERY_ROUTE_POLICY_INVALID == result->ap_query_route_policy_type_) {\
    result->ap_query_route_policy_type_ = policy_type;\
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
     FORCE_MASTER_HINT = 304,
     ROUTE_TABLE = 305,
     ROUTE_PART_KEY = 306,
     PLACE_HOLDER = 307,
     END_P = 308,
     ERROR = 309,
     WHEN = 310,
     TABLEGROUP = 311,
     FLASHBACK = 312,
     AUDIT = 313,
     NOAUDIT = 314,
     STATUS = 315,
     BEGI = 316,
     START = 317,
     TRANSACTION = 318,
     READ = 319,
     ONLY = 320,
     WITH = 321,
     CONSISTENT = 322,
     SNAPSHOT = 323,
     INDEX = 324,
     XA = 325,
     GLOBALINDEX = 326,
     WARNINGS = 327,
     ERRORS = 328,
     TRACE = 329,
     QUICK = 330,
     COUNT = 331,
     AS = 332,
     WHERE = 333,
     VALUES = 334,
     ORDER = 335,
     GROUP = 336,
     HAVING = 337,
     INTO = 338,
     UNION = 339,
     FOR = 340,
     TX_READ_ONLY = 341,
     SELECT_OBPROXY_ROUTE_ADDR = 342,
     SET_OBPROXY_ROUTE_ADDR = 343,
     NAME_OB_DOT = 344,
     NAME_OB = 345,
     EXPLAIN = 346,
     EXPLAIN_ROUTE = 347,
     DESC = 348,
     DESCRIBE = 349,
     NAME_STR = 350,
     USER_VARIABLE = 351,
     SYSTEM_VARIABLE = 352,
     LOAD = 353,
     DATA = 354,
     LOCAL = 355,
     INFILE = 356,
     SLAVE = 357,
     RELAYLOG = 358,
     EVENTS = 359,
     HOSTS = 360,
     BINLOG = 361,
     PORT = 362,
     USE = 363,
     HELP = 364,
     SET_NAMES = 365,
     SET_CHARSET = 366,
     SET_PASSWORD = 367,
     SET_DEFAULT = 368,
     SET_OB_READ_CONSISTENCY = 369,
     SET_TX_READ_ONLY = 370,
     GLOBAL = 371,
     SESSION = 372,
     GLOBAL_ALIAS = 373,
     SESSION_ALIAS = 374,
     MASTER = 375,
     LOGS = 376,
     RESET = 377,
     FLUSH = 378,
     SERVER = 379,
     TENANT = 380,
     NUMBER_VAL = 381,
     GROUP_ID = 382,
     TABLE_ID = 383,
     ELASTIC_ID = 384,
     TESTLOAD = 385,
     ODP_COMMENT = 386,
     TNT_ID = 387,
     DISASTER_STATUS = 388,
     TRACE_ID = 389,
     RPC_ID = 390,
     TARGET_DB_SERVER = 391,
     TRACE_LOG = 392,
     DBP_COMMENT = 393,
     ROUTE_TAG = 394,
     SYS_TAG = 395,
     TABLE_NAME = 396,
     SCAN_ALL = 397,
     STICKY_SESSION = 398,
     PARALL = 399,
     SHARD_KEY = 400,
     STOP_DDL_TASK = 401,
     RETRY_DDL_TASK = 402,
     QUERY_TIMEOUT = 403,
     MAX_EXECUTION_TIME = 404,
     READ_CONSISTENCY = 405,
     WEAK = 406,
     STRONG = 407,
     FROZEN = 408,
     HINT_OPT_PARAM = 409,
     HINT_AP_QUERY_ROUTE_POLICY = 410,
     HINT_AP_QRP_FORCE = 411,
     HINT_AP_QRP_AUTO = 412,
     HINT_AP_QRP_OFF = 413,
     INT_NUM = 414,
     SHOW_PROXYNET = 415,
     THREAD = 416,
     CONNECTION = 417,
     LIMIT = 418,
     OFFSET = 419,
     SHOW_PROCESSLIST = 420,
     SHOW_PROXYSESSION = 421,
     SHOW_GLOBALSESSION = 422,
     ATTRIBUTE = 423,
     VARIABLES = 424,
     ALL = 425,
     STAT = 426,
     READ_STALE = 427,
     SHOW_PROXYCONFIG = 428,
     DIFF = 429,
     USER = 430,
     LIKE = 431,
     SHOW_PROXYSM = 432,
     RPC = 433,
     SHOW_PROXYRPC = 434,
     REQUESTSTAT = 435,
     SHOW_PROXYCLUSTER = 436,
     SHOW_PROXYRESOURCE = 437,
     SHOW_PROXYCONGESTION = 438,
     SHOW_PROXYROUTE = 439,
     PARTITION = 440,
     ROUTINE = 441,
     SUBPARTITION = 442,
     TABLETLS = 443,
     QUERYASYNC = 444,
     RPCCTX = 445,
     SHOW_PROXYVIP = 446,
     SHOW_PROXYMEMORY = 447,
     OBJPOOL = 448,
     SHOW_SQLAUDIT = 449,
     SHOW_WARNLOG = 450,
     SHOW_PROXYSTAT = 451,
     REFRESH = 452,
     SHOW_PROXYTRACE = 453,
     SHOW_PROXYINFO = 454,
     BINARY = 455,
     UPGRADE = 456,
     IDC = 457,
     SHOW_PROXYPS = 458,
     DETAIL = 459,
     SHOW_ELASTIC_ID = 460,
     SHOW_TOPOLOGY = 461,
     GROUP_NAME = 462,
     SHOW_DB_VERSION = 463,
     SHOW_DATABASES = 464,
     SHOW_TABLES = 465,
     SHOW_FULL_TABLES = 466,
     SELECT_DATABASE = 467,
     SELECT_PROXY_STATUS = 468,
     SHOW_CREATE_TABLE = 469,
     SELECT_PROXY_VERSION = 470,
     SHOW_COLUMNS = 471,
     SHOW_INDEX = 472,
     ALTER_PROXYCONFIG = 473,
     ALTER_PROXYRESOURCE = 474,
     PING_PROXY = 475,
     KILL_PROXYSESSION = 476,
     KILL_GLOBALSESSION = 477,
     KILL = 478,
     QUERY = 479,
     BINLOG_VARIABLE = 480,
     BINLOG_USER_VAR = 481,
     BINLOG_SYS_VAR = 482,
     CDC = 483,
     REGISTER = 484,
     UNREGISTER = 485,
     AUTH = 486,
     ACK = 487,
     DESCRIBE_CDC = 488
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
#define YYFINAL  417
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   4566

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  245
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  164
/* YYNRULES -- Number of rules.  */
#define YYNRULES  573
/* YYNRULES -- Number of states.  */
#define YYNSTATES  923

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   488

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,   242,     2,     2,     2,     2,
     238,   239,   244,     2,   235,     2,   236,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   234,
       2,   237,     2,     2,   243,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   240,     2,   241,     2,     2,     2,     2,
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
     215,   216,   217,   218,   219,   220,   221,   222,   223,   224,
     225,   226,   227,   228,   229,   230,   231,   232,   233
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
      88,    90,    92,    94,    96,   100,   101,   103,   109,   116,
     118,   120,   123,   126,   129,   132,   135,   138,   141,   144,
     146,   148,   151,   154,   156,   158,   160,   162,   164,   165,
     167,   169,   172,   174,   175,   177,   180,   183,   185,   187,
     189,   191,   193,   195,   197,   199,   201,   205,   207,   209,
     211,   215,   218,   222,   225,   228,   232,   236,   238,   240,
     242,   244,   246,   248,   250,   252,   254,   257,   259,   261,
     263,   265,   266,   269,   270,   272,   275,   281,   285,   287,
     291,   293,   295,   297,   299,   301,   303,   306,   308,   310,
     312,   314,   316,   318,   321,   324,   326,   329,   332,   337,
     342,   345,   350,   351,   354,   355,   358,   362,   366,   372,
     374,   376,   380,   386,   394,   396,   400,   402,   404,   406,
     408,   410,   412,   418,   420,   424,   430,   431,   433,   437,
     439,   441,   443,   445,   447,   449,   451,   454,   456,   459,
     463,   467,   469,   471,   473,   474,   478,   480,   484,   488,
     494,   497,   500,   505,   508,   511,   515,   517,   521,   526,
     531,   535,   540,   545,   549,   551,   553,   555,   557,   560,
     564,   568,   574,   581,   588,   595,   602,   609,   617,   624,
     631,   638,   645,   654,   663,   670,   674,   675,   678,   680,
     682,   686,   688,   693,   698,   702,   709,   713,   718,   723,
     730,   734,   736,   740,   741,   745,   749,   753,   757,   761,
     765,   769,   773,   777,   781,   783,   787,   793,   797,   802,
     807,   811,   813,   817,   818,   820,   821,   823,   825,   827,
     830,   833,   837,   842,   844,   847,   849,   852,   854,   857,
     860,   864,   865,   867,   870,   872,   875,   877,   880,   883,
     886,   889,   890,   893,   895,   897,   898,   901,   906,   911,
     916,   922,   928,   934,   941,   948,   955,   961,   963,   968,
     970,   972,   974,   976,   977,   979,   981,   983,   984,   986,
     990,   994,   997,  1003,  1007,  1011,  1015,  1019,  1023,  1027,
    1031,  1033,  1035,  1037,  1039,  1041,  1043,  1045,  1047,  1049,
    1051,  1053,  1055,  1057,  1059,  1061,  1063,  1065,  1067,  1069,
    1071,  1073,  1075,  1077,  1079,  1081,  1082,  1084,  1086,  1088,
    1091,  1097,  1103,  1106,  1110,  1114,  1117,  1120,  1123,  1125,
    1128,  1131,  1134,  1137,  1140,  1143,  1146,  1147,  1150,  1155,
    1160,  1161,  1164,  1165,  1168,  1171,  1173,  1175,  1178,  1181,
    1183,  1185,  1189,  1192,  1196,  1200,  1205,  1207,  1210,  1211,
    1214,  1218,  1221,  1224,  1227,  1228,  1231,  1235,  1238,  1242,
    1245,  1249,  1253,  1258,  1261,  1263,  1266,  1269,  1273,  1276,
    1280,  1283,  1286,  1287,  1289,  1291,  1294,  1297,  1301,  1304,
    1307,  1310,  1313,  1316,  1320,  1323,  1325,  1328,  1330,  1333,
    1336,  1340,  1343,  1346,  1349,  1350,  1352,  1356,  1362,  1365,
    1369,  1372,  1373,  1375,  1378,  1381,  1384,  1387,  1392,  1399,
    1400,  1402,  1403,  1405,  1410,  1416,  1422,  1426,  1428,  1431,
    1435,  1439,  1442,  1445,  1449,  1453,  1454,  1457,  1459,  1463,
    1467,  1471,  1472,  1474,  1476,  1480,  1483,  1487,  1490,  1493,
    1495,  1496,  1499,  1504,  1507,  1512,  1515,  1519,  1521,  1526,
    1530,  1533,  1536,  1541,  1545,  1551,  1554,  1559,  1563,  1568,
    1574,  1581,  1583,  1585,  1588,  1591,  1595,  1599,  1603,  1605,
    1606,  1608,  1610,  1612,  1614,  1616,  1618,  1620,  1622,  1624,
    1626,  1628,  1630,  1632,  1634,  1636,  1638,  1640,  1642,  1644,
    1646,  1648,  1650,  1652,  1654,  1656,  1658,  1660,  1662,  1664,
    1666,  1668,  1670,  1672,  1674,  1676,  1678,  1680,  1682,  1684,
    1686,  1688,  1690,  1692,  1694,  1696,  1698,  1700,  1702,  1704,
    1706,  1708,  1710,  1712
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     246,     0,    -1,   247,    -1,     1,    -1,   248,    -1,   247,
     248,    -1,   249,    53,    -1,   249,   234,    -1,   249,   234,
      53,    -1,   234,    -1,   234,    53,    -1,    61,   249,   234,
      -1,   250,    -1,   318,   250,    -1,   251,    -1,   309,    -1,
     314,    -1,   310,    -1,   311,    -1,   312,    -1,   258,    -1,
     257,    -1,   394,    -1,   351,    -1,   280,    -1,   352,    -1,
     398,    -1,   399,    -1,   292,    -1,   293,    -1,   294,    -1,
     295,    -1,   296,    -1,   297,    -1,   298,    -1,   259,    -1,
     271,    -1,   313,    -1,   354,    -1,   355,    -1,   256,    -1,
     400,    -1,   334,    -1,   335,    -1,   336,   277,   276,    -1,
      -1,     9,    -1,   101,    90,   252,    19,   403,    -1,   100,
     101,    90,   252,    19,   403,    -1,   254,    -1,   253,    -1,
     343,   255,    -1,   273,   251,    -1,   273,   309,    -1,   273,
     311,    -1,   273,   312,    -1,   273,   310,    -1,   273,   313,
      -1,   275,   250,    -1,   260,    -1,   272,    -1,    14,   261,
      -1,    15,   262,    -1,    16,    -1,    17,    -1,    18,    -1,
     263,    -1,   264,    -1,    -1,    19,    -1,    69,    -1,    20,
      69,    -1,    56,    -1,    -1,    56,    -1,   146,   159,    -1,
     147,   159,    -1,   251,    -1,   309,    -1,   310,    -1,   312,
      -1,   311,    -1,   400,    -1,   298,    -1,   313,    -1,    96,
      -1,   266,   235,    96,    -1,    96,    -1,   267,    -1,   265,
      -1,    35,   408,    26,    -1,    36,   408,    -1,    36,   408,
      37,    -1,   269,   268,    -1,   270,   266,    -1,    15,    35,
     408,    -1,    38,    35,   408,    -1,    21,    -1,    22,    -1,
      23,    -1,    24,    -1,    57,    -1,    25,    -1,    58,    -1,
      59,    -1,   274,    -1,   274,    90,    -1,    91,    -1,    93,
      -1,    94,    -1,    92,    -1,    -1,    26,   305,    -1,    -1,
     302,    -1,     5,    86,    -1,     5,    86,   277,    26,   305,
      -1,     5,    86,   302,    -1,   215,    -1,   215,    77,   408,
      -1,   278,    -1,   279,    -1,   290,    -1,   291,    -1,   281,
      -1,   289,    -1,   206,   282,    -1,   288,    -1,   212,    -1,
     213,    -1,   209,    -1,   286,    -1,   287,    -1,   216,   282,
      -1,   217,   282,    -1,   283,    -1,   274,   408,    -1,    26,
     408,    -1,    26,   408,    26,   408,    -1,    26,   408,   236,
     408,    -1,   214,    90,    -1,   214,    90,   236,    90,    -1,
      -1,   176,    90,    -1,    -1,    26,    90,    -1,   210,   285,
     284,    -1,   211,   285,   284,    -1,    11,    19,    60,   285,
     284,    -1,   208,    -1,   205,    -1,   205,    26,    90,    -1,
     205,    78,   207,   237,    90,    -1,   205,    26,    90,    78,
     207,   237,    90,    -1,    87,    -1,    88,   237,   159,    -1,
     110,    -1,   111,    -1,   112,    -1,   113,    -1,   114,    -1,
     115,    -1,    13,   299,   238,   300,   239,    -1,   408,    -1,
     408,   236,   408,    -1,   408,   236,   408,   236,   408,    -1,
      -1,   301,    -1,   300,   235,   301,    -1,    90,    -1,   159,
      -1,   126,    -1,    96,    -1,    97,    -1,    52,    -1,   303,
      -1,   302,   303,    -1,   304,    -1,   238,   239,    -1,   238,
     251,   239,    -1,   238,   302,   239,    -1,   402,    -1,   306,
      -1,   251,    -1,    -1,   238,   308,   239,    -1,   408,    -1,
     308,   235,   408,    -1,   339,   403,   401,    -1,   339,   403,
     401,   307,   306,    -1,   341,   305,    -1,   337,   305,    -1,
     338,   350,    26,   305,    -1,   342,   403,    -1,    12,   315,
      -1,   316,   235,   315,    -1,   316,    -1,    96,   237,   317,
      -1,   118,   408,   237,   317,    -1,   116,   408,   237,   317,
      -1,    97,   237,   317,    -1,   119,   408,   237,   317,    -1,
     117,   408,   237,   317,    -1,   408,   237,   317,    -1,   408,
      -1,   159,    -1,   126,    -1,   319,    -1,   319,   318,    -1,
      47,   320,    48,    -1,    47,    49,    48,    -1,    47,   131,
     328,   327,    48,    -1,    47,   128,   237,   333,   327,    48,
      -1,    47,   141,   237,   333,   327,    48,    -1,    47,   127,
     237,   333,   327,    48,    -1,    47,   129,   237,   333,   327,
      48,    -1,    47,   130,   237,   333,   327,    48,    -1,    47,
      89,    90,   237,   332,   327,    48,    -1,    47,   134,   237,
     331,   327,    48,    -1,    47,   135,   237,   331,   327,    48,
      -1,    47,   132,   237,   333,   327,    48,    -1,    47,   133,
     237,   333,   327,    48,    -1,    47,   138,   139,   237,   240,
     322,   241,    48,    -1,    47,   138,   140,   237,   240,   324,
     241,    48,    -1,    47,   136,   237,   333,   327,    48,    -1,
      47,     1,    48,    -1,    -1,   320,   321,    -1,   408,    -1,
      53,    -1,   323,   235,   322,    -1,   323,    -1,   127,   238,
     333,   239,    -1,   141,   238,   333,   239,    -1,   142,   238,
     239,    -1,   142,   238,   144,   237,   333,   239,    -1,   143,
     238,   239,    -1,   145,   238,   325,   239,    -1,    74,   238,
     331,   239,    -1,    74,   238,   331,   242,   331,   239,    -1,
     326,   235,   325,    -1,   326,    -1,    90,   237,   333,    -1,
      -1,   327,   235,   328,    -1,   127,   237,   333,    -1,   128,
     237,   333,    -1,   141,   237,   333,    -1,   129,   237,   333,
      -1,   130,   237,   333,    -1,   134,   237,   331,    -1,   135,
     237,   331,    -1,   132,   237,   333,    -1,   133,   237,   333,
      -1,   137,    -1,   136,   237,   333,    -1,    90,   236,    90,
     237,   332,    -1,    90,   237,   332,    -1,    50,   238,   408,
     239,    -1,    51,   238,   329,   239,    -1,   330,   235,   329,
      -1,   330,    -1,   408,   237,   317,    -1,    -1,   333,    -1,
      -1,   333,    -1,   408,    -1,    95,    -1,     5,   226,    -1,
       5,   227,    -1,     5,   118,   107,    -1,     5,   243,   243,
     107,    -1,     5,    -1,    39,   344,    -1,     8,    -1,    40,
     344,    -1,     6,    -1,    41,   344,    -1,     7,   340,    -1,
      42,   344,   340,    -1,    -1,   170,    -1,   170,    55,    -1,
       9,    -1,    43,   344,    -1,    10,    -1,    44,   344,    -1,
      98,    99,    -1,    45,   344,    -1,   345,    46,    -1,    -1,
     348,   345,    -1,   159,    -1,   408,    -1,    -1,   346,   347,
      -1,   148,   238,   159,   239,    -1,   149,   238,   159,   239,
      -1,   150,   238,   349,   239,    -1,   154,   238,   155,   156,
     239,    -1,   154,   238,   155,   157,   239,    -1,   154,   238,
     155,   158,   239,    -1,   154,   238,   155,   235,   156,   239,
      -1,   154,   238,   155,   235,   157,   239,    -1,   154,   238,
     155,   235,   158,   239,    -1,    69,   238,   408,   408,   239,
      -1,   137,    -1,   408,   238,   347,   239,    -1,   408,    -1,
     159,    -1,    53,    -1,     1,    -1,    -1,   151,    -1,   152,
      -1,   153,    -1,    -1,    75,    -1,    11,   393,    72,    -1,
      11,   393,    73,    -1,    11,    74,    -1,    11,    74,    90,
     237,    90,    -1,    11,   102,   105,    -1,    11,   102,    60,
      -1,    11,   103,   104,    -1,    11,   120,    60,    -1,    11,
     200,   121,    -1,    11,   106,   104,    -1,    11,   120,   121,
      -1,   361,    -1,   363,    -1,   364,    -1,   367,    -1,   365,
      -1,   369,    -1,   370,    -1,   371,    -1,   372,    -1,   374,
      -1,   375,    -1,   376,    -1,   377,    -1,   378,    -1,   380,
      -1,   381,    -1,   383,    -1,   359,    -1,   384,    -1,   387,
      -1,   388,    -1,   389,    -1,   390,    -1,   391,    -1,   392,
      -1,    -1,   116,    -1,   117,    -1,   100,    -1,   106,   408,
      -1,    11,   106,   124,    85,   125,    -1,    11,   353,   169,
     176,   225,    -1,   122,   120,    -1,    24,   200,   121,    -1,
     123,   200,   121,    -1,    14,   228,    -1,    15,   228,    -1,
      11,   228,    -1,   233,    -1,   229,   228,    -1,   230,   228,
      -1,    16,   228,    -1,    24,   228,    -1,   122,   228,    -1,
     231,   228,    -1,   232,   228,    -1,    -1,   163,   159,    -1,
     163,   159,   235,   159,    -1,   163,   159,   164,   159,    -1,
      -1,   176,    90,    -1,    -1,   176,    90,    -1,   179,   360,
      -1,   161,    -1,   180,    -1,   180,    90,    -1,   160,   362,
      -1,   161,    -1,   162,    -1,   162,   159,   356,    -1,   173,
     357,    -1,   173,   170,   357,    -1,   173,   174,   357,    -1,
     173,   174,   175,   357,    -1,   165,    -1,   167,   366,    -1,
      -1,   168,    90,    -1,   168,   176,    90,    -1,   168,   170,
      -1,   176,    90,    -1,   166,   368,    -1,    -1,   168,   357,
      -1,   168,   159,   357,    -1,   171,   357,    -1,   171,   159,
     357,    -1,   169,   357,    -1,   169,   159,   357,    -1,   169,
     170,   357,    -1,   169,   170,   159,   357,    -1,   172,   357,
      -1,   177,    -1,   177,   159,    -1,   177,   178,    -1,   177,
     178,   159,    -1,   181,   357,    -1,   181,   202,   357,    -1,
     182,   357,    -1,   183,   373,    -1,    -1,    90,    -1,   170,
      -1,   170,    90,    -1,   184,   358,    -1,   184,   186,   358,
      -1,   184,   185,    -1,   184,    71,    -1,   184,    56,    -1,
     184,   189,    -1,   184,   188,    -1,   184,   188,   159,    -1,
     184,   190,    -1,   191,    -1,   191,    90,    -1,   192,    -1,
     192,   159,    -1,   192,   193,    -1,   192,   193,   159,    -1,
     194,   356,    -1,   194,   159,    -1,   195,   379,    -1,    -1,
     159,    -1,   159,   235,   159,    -1,   159,   235,   159,   235,
      90,    -1,   196,   357,    -1,   196,   197,   357,    -1,   198,
     382,    -1,    -1,   159,    -1,   159,   159,    -1,   199,   200,
      -1,   199,   201,    -1,   199,   202,    -1,   203,   386,   357,
     385,    -1,   203,   386,   357,   125,   358,   385,    -1,    -1,
     204,    -1,    -1,   159,    -1,   218,    12,    90,   237,    -1,
     218,    12,    90,   237,    90,    -1,   218,    12,    90,   237,
     159,    -1,   219,     6,    90,    -1,   220,    -1,   221,   159,
      -1,   221,   159,   159,    -1,   222,    90,   159,    -1,   222,
      90,    -1,   223,   159,    -1,   223,   162,   159,    -1,   223,
     224,   159,    -1,    -1,    76,   244,    -1,    61,    -1,    62,
      63,   395,    -1,    70,    61,    90,    -1,    70,    62,    90,
      -1,    -1,   396,    -1,   397,    -1,   396,   235,   397,    -1,
      64,    65,    -1,    66,    67,    68,    -1,   108,   408,    -1,
     109,    90,    -1,    90,    -1,    -1,   187,   408,    -1,   187,
     238,   408,   239,    -1,   185,   408,    -1,   185,   238,   408,
     239,    -1,   403,   401,    -1,   403,   401,   404,    -1,   408,
      -1,   408,   236,   408,    96,    -1,   408,   236,   408,    -1,
     408,    96,    -1,   408,   408,    -1,   408,   236,   408,   408,
      -1,   408,    77,   408,    -1,   408,   236,   408,    77,   408,
      -1,   405,   408,    -1,   405,   408,   236,   408,    -1,   405,
     408,   408,    -1,   405,   408,    77,   408,    -1,   405,   408,
     236,   408,   408,    -1,   405,   408,   236,   408,    77,   408,
      -1,   235,    -1,    28,    -1,    29,    28,    -1,    30,    28,
      -1,    31,   406,    28,    -1,    32,   406,    28,    -1,    33,
     406,    28,    -1,    34,    -1,    -1,    62,    -1,    70,    -1,
      61,    -1,    63,    -1,    67,    -1,    73,    -1,    72,    -1,
      76,    -1,    75,    -1,    74,    -1,   161,    -1,   162,    -1,
     164,    -1,   168,    -1,   169,    -1,   171,    -1,   174,    -1,
     175,    -1,   193,    -1,   197,    -1,   201,    -1,   202,    -1,
     224,    -1,   207,    -1,    57,    -1,    58,    -1,    59,    -1,
     100,    -1,    99,    -1,    60,    -1,   151,    -1,   152,    -1,
     153,    -1,   116,    -1,   117,    -1,   105,    -1,   104,    -1,
     103,    -1,   106,    -1,   107,    -1,   120,    -1,   121,    -1,
     122,    -1,   123,    -1,   124,    -1,   125,    -1,   204,    -1,
     228,    -1,   229,    -1,   230,    -1,   231,    -1,   232,    -1,
      90,    -1,   407,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   348,   348,   349,   351,   352,   354,   355,   356,   357,
     358,   359,   361,   362,   364,   365,   366,   367,   368,   369,
     370,   371,   372,   373,   374,   375,   376,   377,   378,   379,
     380,   381,   382,   383,   384,   385,   386,   387,   388,   389,
     390,   391,   393,   394,   395,   400,   402,   404,   409,   414,
     415,   417,   419,   420,   421,   422,   423,   424,   426,   428,
     429,   431,   432,   433,   434,   435,   436,   437,   439,   440,
     441,   442,   443,   445,   446,   448,   454,   460,   461,   462,
     463,   464,   465,   466,   467,   469,   476,   484,   492,   493,
     496,   502,   507,   513,   516,   519,   524,   530,   531,   532,
     533,   534,   535,   536,   537,   539,   540,   542,   543,   544,
     546,   548,   549,   551,   552,   554,   555,   556,   558,   559,
     561,   562,   563,   564,   565,   567,   568,   569,   570,   571,
     572,   573,   574,   575,   576,   577,   578,   585,   589,   594,
     600,   605,   612,   613,   615,   616,   618,   622,   627,   632,
     634,   635,   640,   645,   652,   655,   661,   662,   663,   664,
     665,   666,   669,   671,   675,   680,   688,   691,   696,   701,
     706,   711,   716,   721,   726,   734,   735,   737,   739,   740,
     741,   743,   744,   746,   748,   749,   751,   752,   754,   758,
     759,   760,   761,   762,   767,   769,   770,   772,   776,   780,
     784,   788,   792,   796,   800,   805,   810,   816,   817,   819,
     820,   821,   822,   823,   824,   825,   826,   827,   833,   834,
     835,   836,   837,   838,   839,   840,   842,   843,   847,   848,
     850,   851,   853,   858,   863,   864,   865,   866,   868,   869,
     871,   872,   874,   882,   883,   885,   886,   887,   888,   889,
     890,   891,   892,   893,   894,   895,   896,   902,   903,   904,
     906,   907,   909,   915,   916,   918,   919,   921,   922,   924,
     925,   927,   928,   930,   931,   932,   933,   934,   935,   936,
     937,   939,   940,   941,   943,   944,   945,   946,   947,   948,
     950,   951,   952,   954,   955,   957,   958,   960,   961,   962,
     964,   965,   966,   968,   969,   970,   971,   976,   977,   978,
     979,   980,   981,   983,   984,   985,   986,   988,   989,   992,
     993,   994,   995,   996,   997,  1002,  1003,  1004,  1005,  1006,
    1010,  1011,  1012,  1013,  1014,  1015,  1016,  1017,  1018,  1019,
    1020,  1021,  1022,  1023,  1024,  1025,  1026,  1027,  1028,  1029,
    1030,  1031,  1032,  1033,  1034,  1036,  1037,  1038,  1039,  1042,
    1043,  1048,  1049,  1050,  1051,  1055,  1056,  1057,  1058,  1059,
    1060,  1061,  1062,  1063,  1064,  1065,  1070,  1072,  1076,  1081,
    1089,  1090,  1094,  1095,  1098,  1100,  1101,  1102,  1105,  1107,
    1108,  1109,  1113,  1114,  1115,  1116,  1121,  1123,  1125,  1126,
    1127,  1128,  1129,  1132,  1134,  1135,  1136,  1137,  1138,  1139,
    1140,  1141,  1142,  1143,  1147,  1148,  1149,  1150,  1154,  1155,
    1160,  1163,  1165,  1166,  1167,  1168,  1172,  1173,  1174,  1175,
    1176,  1177,  1178,  1179,  1180,  1184,  1185,  1189,  1190,  1191,
    1192,  1196,  1197,  1200,  1202,  1203,  1204,  1205,  1209,  1210,
    1213,  1215,  1216,  1217,  1221,  1222,  1223,  1226,  1227,  1230,
    1231,  1234,  1235,  1239,  1240,  1241,  1245,  1249,  1253,  1254,
    1258,  1259,  1263,  1264,  1265,  1268,  1269,  1272,  1276,  1277,
    1278,  1280,  1281,  1283,  1284,  1287,  1288,  1291,  1297,  1300,
    1302,  1303,  1304,  1305,  1306,  1308,  1312,  1318,  1321,  1326,
    1330,  1334,  1338,  1343,  1347,  1353,  1354,  1359,  1364,  1369,
    1375,  1382,  1383,  1384,  1385,  1386,  1387,  1388,  1390,  1391,
    1393,  1394,  1395,  1396,  1397,  1398,  1399,  1400,  1401,  1402,
    1403,  1404,  1405,  1406,  1407,  1408,  1409,  1410,  1411,  1412,
    1413,  1414,  1415,  1416,  1417,  1418,  1419,  1420,  1421,  1422,
    1423,  1424,  1425,  1426,  1427,  1428,  1429,  1430,  1431,  1432,
    1433,  1434,  1435,  1436,  1437,  1438,  1439,  1440,  1441,  1442,
    1443,  1444,  1446,  1447
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
  "FORCE_MASTER_HINT", "ROUTE_TABLE", "ROUTE_PART_KEY", "PLACE_HOLDER",
  "END_P", "ERROR", "WHEN", "TABLEGROUP", "FLASHBACK", "AUDIT", "NOAUDIT",
  "STATUS", "BEGI", "START", "TRANSACTION", "READ", "ONLY", "WITH",
  "CONSISTENT", "SNAPSHOT", "INDEX", "XA", "GLOBALINDEX", "WARNINGS",
  "ERRORS", "TRACE", "QUICK", "COUNT", "AS", "WHERE", "VALUES", "ORDER",
  "GROUP", "HAVING", "INTO", "UNION", "FOR", "TX_READ_ONLY",
  "SELECT_OBPROXY_ROUTE_ADDR", "SET_OBPROXY_ROUTE_ADDR", "NAME_OB_DOT",
  "NAME_OB", "EXPLAIN", "EXPLAIN_ROUTE", "DESC", "DESCRIBE", "NAME_STR",
  "USER_VARIABLE", "SYSTEM_VARIABLE", "LOAD", "DATA", "LOCAL", "INFILE",
  "SLAVE", "RELAYLOG", "EVENTS", "HOSTS", "BINLOG", "PORT", "USE", "HELP",
  "SET_NAMES", "SET_CHARSET", "SET_PASSWORD", "SET_DEFAULT",
  "SET_OB_READ_CONSISTENCY", "SET_TX_READ_ONLY", "GLOBAL", "SESSION",
  "GLOBAL_ALIAS", "SESSION_ALIAS", "MASTER", "LOGS", "RESET", "FLUSH",
  "SERVER", "TENANT", "NUMBER_VAL", "GROUP_ID", "TABLE_ID", "ELASTIC_ID",
  "TESTLOAD", "ODP_COMMENT", "TNT_ID", "DISASTER_STATUS", "TRACE_ID",
  "RPC_ID", "TARGET_DB_SERVER", "TRACE_LOG", "DBP_COMMENT", "ROUTE_TAG",
  "SYS_TAG", "TABLE_NAME", "SCAN_ALL", "STICKY_SESSION", "PARALL",
  "SHARD_KEY", "STOP_DDL_TASK", "RETRY_DDL_TASK", "QUERY_TIMEOUT",
  "MAX_EXECUTION_TIME", "READ_CONSISTENCY", "WEAK", "STRONG", "FROZEN",
  "HINT_OPT_PARAM", "HINT_AP_QUERY_ROUTE_POLICY", "HINT_AP_QRP_FORCE",
  "HINT_AP_QRP_AUTO", "HINT_AP_QRP_OFF", "INT_NUM", "SHOW_PROXYNET",
  "THREAD", "CONNECTION", "LIMIT", "OFFSET", "SHOW_PROCESSLIST",
  "SHOW_PROXYSESSION", "SHOW_GLOBALSESSION", "ATTRIBUTE", "VARIABLES",
  "ALL", "STAT", "READ_STALE", "SHOW_PROXYCONFIG", "DIFF", "USER", "LIKE",
  "SHOW_PROXYSM", "RPC", "SHOW_PROXYRPC", "REQUESTSTAT",
  "SHOW_PROXYCLUSTER", "SHOW_PROXYRESOURCE", "SHOW_PROXYCONGESTION",
  "SHOW_PROXYROUTE", "PARTITION", "ROUTINE", "SUBPARTITION", "TABLETLS",
  "QUERYASYNC", "RPCCTX", "SHOW_PROXYVIP", "SHOW_PROXYMEMORY", "OBJPOOL",
  "SHOW_SQLAUDIT", "SHOW_WARNLOG", "SHOW_PROXYSTAT", "REFRESH",
  "SHOW_PROXYTRACE", "SHOW_PROXYINFO", "BINARY", "UPGRADE", "IDC",
  "SHOW_PROXYPS", "DETAIL", "SHOW_ELASTIC_ID", "SHOW_TOPOLOGY",
  "GROUP_NAME", "SHOW_DB_VERSION", "SHOW_DATABASES", "SHOW_TABLES",
  "SHOW_FULL_TABLES", "SELECT_DATABASE", "SELECT_PROXY_STATUS",
  "SHOW_CREATE_TABLE", "SELECT_PROXY_VERSION", "SHOW_COLUMNS",
  "SHOW_INDEX", "ALTER_PROXYCONFIG", "ALTER_PROXYRESOURCE", "PING_PROXY",
  "KILL_PROXYSESSION", "KILL_GLOBALSESSION", "KILL", "QUERY",
  "BINLOG_VARIABLE", "BINLOG_USER_VAR", "BINLOG_SYS_VAR", "CDC",
  "REGISTER", "UNREGISTER", "AUTH", "ACK", "DESCRIBE_CDC", "';'", "','",
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
  "opt_global_or_session", "binlog_stmt", "cdc_coordinator_stmt",
  "opt_limit", "opt_like", "opt_large_like", "show_proxyrpc",
  "opt_show_rpc", "show_proxynet", "opt_show_net", "show_proxyconfig",
  "show_processlist", "show_globalsession", "opt_show_global_session",
  "show_proxysession", "opt_show_session", "show_proxysm",
  "show_proxycluster", "show_proxyresource", "show_proxycongestion",
  "opt_show_congestion", "show_proxyroute", "show_proxyvip",
  "show_proxymemory", "show_sqlaudit", "show_warnlog", "opt_show_warnlog",
  "show_proxystat", "show_proxytrace", "opt_show_trace", "show_proxyinfo",
  "show_proxyps", "opt_detail", "opt_int", "alter_proxyconfig",
  "alter_proxyresource", "ping_proxy", "kill_proxysession",
  "kill_globalsession", "kill_mysql", "opt_count", "begin_stmt",
  "opt_transaction_characteristics", "transaction_characteristics",
  "transaction_characteristic", "use_db_stmt", "help_stmt", "other_stmt",
  "partition_factor", "table_references", "table_factor", "join_expr",
  "join_type", "opt_outer", "non_reserved_keyword", "var_name", 0
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
     475,   476,   477,   478,   479,   480,   481,   482,   483,   484,
     485,   486,   487,   488,    59,    44,    46,    61,    40,    41,
     123,   125,    35,    64,    42
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   245,   246,   246,   247,   247,   248,   248,   248,   248,
     248,   248,   249,   249,   250,   250,   250,   250,   250,   250,
     250,   250,   250,   250,   250,   250,   250,   250,   250,   250,
     250,   250,   250,   250,   250,   250,   250,   250,   250,   250,
     250,   250,   251,   251,   251,   252,   252,   253,   254,   255,
     255,   256,   257,   257,   257,   257,   257,   257,   258,   259,
     259,   260,   260,   260,   260,   260,   260,   260,   261,   261,
     261,   261,   261,   262,   262,   263,   264,   265,   265,   265,
     265,   265,   265,   265,   265,   266,   266,   267,   268,   268,
     269,   270,   270,   271,   271,   271,   271,   272,   272,   272,
     272,   272,   272,   272,   272,   273,   273,   274,   274,   274,
     275,   276,   276,   277,   277,   278,   278,   278,   279,   279,
     280,   280,   280,   280,   280,   281,   281,   281,   281,   281,
     281,   281,   281,   281,   281,   281,   281,   282,   282,   282,
     283,   283,   284,   284,   285,   285,   286,   286,   287,   288,
     289,   289,   289,   289,   290,   291,   292,   293,   294,   295,
     296,   297,   298,   299,   299,   299,   300,   300,   300,   301,
     301,   301,   301,   301,   301,   302,   302,   303,   304,   304,
     304,   305,   305,   306,   307,   307,   308,   308,   309,   309,
     310,   311,   312,   313,   314,   315,   315,   316,   316,   316,
     316,   316,   316,   316,   317,   317,   317,   318,   318,   319,
     319,   319,   319,   319,   319,   319,   319,   319,   319,   319,
     319,   319,   319,   319,   319,   319,   320,   320,   321,   321,
     322,   322,   323,   323,   323,   323,   323,   323,   324,   324,
     325,   325,   326,   327,   327,   328,   328,   328,   328,   328,
     328,   328,   328,   328,   328,   328,   328,   328,   328,   328,
     329,   329,   330,   331,   331,   332,   332,   333,   333,   334,
     334,   335,   335,   336,   336,   337,   337,   338,   338,   339,
     339,   340,   340,   340,   341,   341,   342,   342,   343,   343,
     344,   345,   345,   346,   346,   347,   347,   348,   348,   348,
     348,   348,   348,   348,   348,   348,   348,   348,   348,   348,
     348,   348,   348,   349,   349,   349,   349,   350,   350,   351,
     351,   351,   351,   351,   351,   351,   351,   351,   351,   351,
     352,   352,   352,   352,   352,   352,   352,   352,   352,   352,
     352,   352,   352,   352,   352,   352,   352,   352,   352,   352,
     352,   352,   352,   352,   352,   353,   353,   353,   353,   354,
     354,   354,   354,   354,   354,   355,   355,   355,   355,   355,
     355,   355,   355,   355,   355,   355,   356,   356,   356,   356,
     357,   357,   358,   358,   359,   360,   360,   360,   361,   362,
     362,   362,   363,   363,   363,   363,   364,   365,   366,   366,
     366,   366,   366,   367,   368,   368,   368,   368,   368,   368,
     368,   368,   368,   368,   369,   369,   369,   369,   370,   370,
     371,   372,   373,   373,   373,   373,   374,   374,   374,   374,
     374,   374,   374,   374,   374,   375,   375,   376,   376,   376,
     376,   377,   377,   378,   379,   379,   379,   379,   380,   380,
     381,   382,   382,   382,   383,   383,   383,   384,   384,   385,
     385,   386,   386,   387,   387,   387,   388,   389,   390,   390,
     391,   391,   392,   392,   392,   393,   393,   394,   394,   394,
     394,   395,   395,   396,   396,   397,   397,   398,   399,   400,
     401,   401,   401,   401,   401,   402,   402,   403,   403,   403,
     403,   403,   403,   403,   403,   404,   404,   404,   404,   404,
     404,   405,   405,   405,   405,   405,   405,   405,   406,   406,
     407,   407,   407,   407,   407,   407,   407,   407,   407,   407,
     407,   407,   407,   407,   407,   407,   407,   407,   407,   407,
     407,   407,   407,   407,   407,   407,   407,   407,   407,   407,
     407,   407,   407,   407,   407,   407,   407,   407,   407,   407,
     407,   407,   407,   407,   407,   407,   407,   407,   407,   407,
     407,   407,   408,   408
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     1,     1,     1,     2,     2,     2,     3,     1,
       2,     3,     1,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     3,     0,     1,     5,     6,     1,
       1,     2,     2,     2,     2,     2,     2,     2,     2,     1,
       1,     2,     2,     1,     1,     1,     1,     1,     0,     1,
       1,     2,     1,     0,     1,     2,     2,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     3,     1,     1,     1,
       3,     2,     3,     2,     2,     3,     3,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     2,     1,     1,     1,
       1,     0,     2,     0,     1,     2,     5,     3,     1,     3,
       1,     1,     1,     1,     1,     1,     2,     1,     1,     1,
       1,     1,     1,     2,     2,     1,     2,     2,     4,     4,
       2,     4,     0,     2,     0,     2,     3,     3,     5,     1,
       1,     3,     5,     7,     1,     3,     1,     1,     1,     1,
       1,     1,     5,     1,     3,     5,     0,     1,     3,     1,
       1,     1,     1,     1,     1,     1,     2,     1,     2,     3,
       3,     1,     1,     1,     0,     3,     1,     3,     3,     5,
       2,     2,     4,     2,     2,     3,     1,     3,     4,     4,
       3,     4,     4,     3,     1,     1,     1,     1,     2,     3,
       3,     5,     6,     6,     6,     6,     6,     7,     6,     6,
       6,     6,     8,     8,     6,     3,     0,     2,     1,     1,
       3,     1,     4,     4,     3,     6,     3,     4,     4,     6,
       3,     1,     3,     0,     3,     3,     3,     3,     3,     3,
       3,     3,     3,     3,     1,     3,     5,     3,     4,     4,
       3,     1,     3,     0,     1,     0,     1,     1,     1,     2,
       2,     3,     4,     1,     2,     1,     2,     1,     2,     2,
       3,     0,     1,     2,     1,     2,     1,     2,     2,     2,
       2,     0,     2,     1,     1,     0,     2,     4,     4,     4,
       5,     5,     5,     6,     6,     6,     5,     1,     4,     1,
       1,     1,     1,     0,     1,     1,     1,     0,     1,     3,
       3,     2,     5,     3,     3,     3,     3,     3,     3,     3,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     0,     1,     1,     1,     2,
       5,     5,     2,     3,     3,     2,     2,     2,     1,     2,
       2,     2,     2,     2,     2,     2,     0,     2,     4,     4,
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
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
       0,     3,   273,   277,   281,   275,   284,   286,   475,     0,
       0,    68,    73,    63,    64,    65,    97,    98,    99,   100,
     102,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   101,   103,   104,   477,     0,     0,   154,     0,
     489,   107,   110,   108,   109,     0,     0,     0,     0,   156,
     157,   158,   159,   160,   161,     0,     0,     0,     0,     0,
     396,   404,   398,   380,   414,     0,   380,   380,   422,   382,
     435,   437,   376,   444,   380,   451,     0,   461,   150,     0,
     149,   130,   144,   144,   128,   129,     0,   118,     0,     0,
       0,     0,   467,     0,     0,     0,     0,     0,     0,     0,
     368,     9,     0,     2,     4,     0,    12,    14,    40,    21,
      20,    35,    59,    66,    67,     0,     0,    36,    60,     0,
     105,     0,   120,   121,    24,   124,   135,   131,   132,   127,
     125,   122,   123,    28,    29,    30,    31,    32,    33,    34,
      15,    17,    18,    19,    37,    16,     0,   207,    42,    43,
     113,     0,   317,     0,     0,     0,     0,    23,    25,    38,
      39,   347,   330,   331,   332,   334,   333,   335,   336,   337,
     338,   339,   340,   341,   342,   343,   344,   345,   346,   348,
     349,   350,   351,   352,   353,   354,    22,    26,    27,    41,
     115,     0,   269,   270,     0,   282,   279,     0,   321,     0,
     358,     0,     0,     0,   356,   357,     0,     0,   367,     0,
       0,   544,   545,   546,   549,   522,   520,   523,   524,   521,
     526,   525,   529,   528,   527,   572,     0,     0,   548,   547,
     557,   556,   555,   558,   559,   553,   554,     0,     0,   560,
     561,   562,   563,   564,   565,   550,   551,   552,   530,   531,
     532,   533,   534,   535,   536,   537,   538,   539,   540,   541,
     566,   543,   542,   567,   568,   569,   570,   571,   194,   196,
     573,     0,   553,   554,     0,   163,    69,     0,    72,    70,
     365,    61,     0,    74,   366,    62,   371,     0,   372,     0,
      91,     0,   312,   311,     0,   307,     0,     0,     0,     0,
     310,   274,     0,     0,   309,   276,   278,   281,   285,   287,
     289,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   477,     0,   481,
       0,     0,     0,   288,   359,   487,   488,   362,   373,     0,
      75,    76,   389,   390,   388,   380,   380,   380,   380,   403,
       0,     0,   397,   380,   380,     0,   392,   415,   416,   385,
     386,   384,   380,   418,   420,   423,   424,   421,   430,   429,
       0,   428,   382,   432,   431,   434,   426,   436,   438,   439,
     442,     0,   441,   445,   443,   380,   448,   452,   450,   454,
     455,   456,   462,   380,     0,     0,     0,   126,     0,   142,
     142,   140,     0,   133,   134,     0,     0,   468,   471,   472,
       0,     0,   369,   370,   374,   375,    10,     1,     5,     6,
       7,   273,    87,    77,    89,    88,    93,    83,    78,    79,
      81,    80,    84,    82,    85,    94,    52,    53,    56,    54,
      55,    57,   106,   136,    58,    13,   208,     0,   111,   114,
     175,   177,   183,   191,   182,   181,   490,   497,   318,     0,
     490,   190,   193,     0,     0,    50,    49,    51,     0,   117,
     271,     0,   283,   144,     0,   476,   324,   323,   325,   328,
       0,   326,   329,   327,     0,   319,   320,     0,     0,     0,
       0,     0,     0,     0,     0,   166,     0,    71,    95,   363,
      90,    92,    96,     0,     0,     0,   313,     0,   290,   292,
     295,   280,   225,   210,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   254,     0,   243,     0,     0,   263,   263,     0,     0,
       0,     0,   209,   229,   227,   228,    11,     0,     0,   478,
     482,   483,   479,   480,   155,   364,   376,   380,   405,   380,
     380,   409,   380,   407,   413,   399,   401,     0,   402,   393,
     380,   394,   381,   417,   387,   419,   425,   383,   427,   433,
     440,   377,     0,   449,   453,   459,   151,     0,   137,   145,
       0,   146,   147,     0,   119,     0,   466,   469,   470,   473,
     474,     8,     0,   178,     0,     0,     0,    44,   176,     0,
       0,   495,     0,   500,     0,   501,     0,   184,     0,    45,
       0,   272,   142,     0,     0,     0,   206,   205,   197,   204,
     200,     0,     0,     0,     0,   195,   203,   174,   169,   172,
     173,   171,   170,     0,   167,   164,     0,     0,     0,   314,
     315,   316,     0,     0,   293,   295,     0,   294,   265,   268,
     243,   267,   243,   243,   243,     0,     0,     0,   265,     0,
       0,     0,     0,     0,     0,   263,   263,     0,     0,     0,
     243,   243,   243,   264,   243,   243,     0,     0,   243,   485,
       0,     0,   391,   406,   410,   380,   411,   408,   400,   395,
       0,     0,   446,   382,   460,   457,     0,     0,     0,     0,
     143,   141,   463,    86,   179,   180,   112,     0,   493,     0,
     491,   512,     0,     0,   519,   519,   519,   511,   496,     0,
     503,   499,   192,     0,     0,    45,    46,     0,   116,   148,
     322,   360,   361,   199,   202,   198,   201,     0,   162,     0,
       0,   297,   298,   299,     0,     0,     0,     0,   296,   308,
     243,   266,     0,     0,     0,     0,     0,     0,   261,     0,
       0,   257,   245,   246,   248,   249,   252,   253,   250,   251,
     255,   247,   211,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   486,   484,   412,   379,   378,     0,   459,     0,
     152,   138,   139,   464,   465,     0,     0,   513,   514,   518,
       0,     0,     0,   505,     0,   498,   502,     0,   186,   189,
       0,     0,   168,   165,   306,   300,   301,   302,     0,     0,
       0,     0,   214,   212,   215,   216,   258,   259,     0,     0,
     265,   244,   220,   221,   218,   219,   224,     0,     0,     0,
       0,     0,     0,   231,     0,     0,   213,   447,   458,     0,
     494,   492,   515,   516,   517,     0,     0,   507,   504,     0,
     185,     0,    47,   303,   304,   305,   217,   260,   262,   256,
       0,     0,     0,     0,     0,     0,     0,   263,     0,   153,
     508,   506,   187,    48,     0,     0,     0,   234,   236,     0,
       0,   241,   222,   230,     0,   223,     0,   509,   232,   233,
       0,     0,   237,     0,   238,   263,   510,     0,   242,   240,
       0,   235,   239
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,   102,   103,   104,   105,   106,   452,   737,   465,   466,
     467,   108,   109,   110,   111,   112,   281,   285,   113,   114,
     424,   435,   425,   426,   115,   116,   117,   118,   119,   120,
     121,   607,   448,   122,   123,   124,   125,   397,   126,   591,
     399,   127,   128,   129,   130,   131,   132,   133,   134,   135,
     136,   137,   138,   139,   274,   643,   644,   449,   450,   451,
     453,   454,   734,   817,   140,   141,   142,   143,   144,   145,
     268,   269,   628,   146,   147,   326,   544,   852,   853,   855,
     900,   901,   679,   533,   767,   768,   682,   760,   683,   148,
     149,   150,   151,   152,   153,   196,   154,   155,   156,   301,
     302,   655,   656,   303,   652,   459,   157,   158,   209,   159,
     160,   382,   356,   376,   161,   361,   162,   344,   163,   164,
     165,   352,   166,   349,   167,   168,   169,   170,   367,   171,
     172,   173,   174,   175,   384,   176,   177,   388,   178,   179,
     705,   393,   180,   181,   182,   183,   184,   185,   210,   186,
     549,   550,   551,   187,   188,   189,   611,   455,   456,   728,
     729,   810,   270,   661
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -628
static const yytype_int16 yypact[] =
{
     778,  -628,   -21,  -628,   -70,  -628,  -628,  -628,    80,  3102,
    4158,    24,     7,  -102,  -628,  -628,  -628,  -628,  -628,  -119,
    -628,  4158,  4158,   105,  1145,  1145,  1145,  1145,  1145,  1145,
    1145,   965,  -628,  -628,  -628,  1603,   127,    96,  -628,   -36,
    -628,  -628,  -628,  -628,  -628,    94,  4158,  4158,   113,  -628,
    -628,  -628,  -628,  -628,  -628,   -49,    44,    52,    72,    63,
    -628,   115,   -55,    28,   -74,   -58,   -85,   109,   -23,    31,
     244,   -83,    26,   171,   -92,   177,   -24,   179,    49,   309,
    -628,  -628,   313,   313,  -628,  -628,   250,   266,   309,   309,
     332,   339,  -628,   187,   257,   -76,   120,   122,   131,   132,
    -628,   298,   352,  1373,  -628,     2,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,   314,   265,  -628,  -628,   389,
    4334,  1832,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,  -628,  1832,   315,  -628,  -628,
     125,  2017,   290,  4158,  2017,  4158,   158,  -628,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
       8,   259,  -628,  -628,   126,   312,  -628,   310,   283,   141,
    -628,    34,   282,    -8,  -628,  -628,    17,   268,  -628,   221,
     227,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,  -628,   154,   166,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,  4158,  4158,  4158,  4158,  -628,
    -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,   170,
    -628,   169,  -628,  -628,   173,   178,  -628,   338,  -628,  -628,
    -628,  -628,  4158,  -628,  -628,  -628,  -628,   287,  -628,   383,
     375,  4158,  -628,  -628,   175,  -628,   180,   181,   182,   183,
    -628,  -628,   369,  1145,   184,  -628,  -628,   -70,  -628,  -628,
    -628,   368,   376,   327,   186,   188,   189,   190,   247,   197,
     198,   199,   200,   201,   172,   202,  2202,  -628,   206,    42,
     351,   353,   285,  -628,  -628,  -628,  -628,  -628,  -628,   321,
    -628,  -628,  -628,   286,  -628,   -61,   -41,   -45,   109,  -628,
      11,   356,  -628,   109,   138,   357,  -628,  -628,   291,  -628,
     359,  -628,   109,  -628,  -628,  -628,   362,  -628,  -628,  -628,
     363,  -628,   279,   297,  -628,  -628,  -628,  -628,  -628,   299,
    -628,   300,  -628,   222,  -628,   109,  -628,   301,  -628,  -628,
    -628,  -628,  -628,   109,   371,   255,  4158,  -628,   373,   289,
     289,   230,  4158,  -628,  -628,   377,   379,   311,   316,  -628,
     317,   318,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
     418,   -52,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,   237,  -628,  -628,  -628,  -628,
    -628,  -628,     3,  -628,  -628,  -628,  -628,    23,   447,   125,
    -628,  -628,  -628,  -628,  -628,  -628,    79,  2742,  -628,   448,
      79,  -628,  -628,   380,   390,  -628,  -628,  -628,   456,     9,
    -628,   381,  -628,   313,   246,  -628,  -628,  -628,  -628,  -628,
     401,  -628,  -628,  -628,   319,  -628,  -628,  3278,  3278,   252,
     253,   254,   256,  3102,  3278,    40,  4158,  -628,  -628,  -628,
    -628,  -628,  -628,  4158,   333,   337,   -19,   342,  -628,  -628,
    3630,  -628,  -628,  -628,   261,  3806,  3806,  3806,  3806,   262,
     264,    89,   267,   269,   270,   271,   272,   273,   274,   275,
     277,  -628,   280,  -628,  3806,  3806,  3806,  3806,  3806,   284,
     288,  3806,  -628,  -628,  -628,  -628,  -628,   434,   436,  -628,
     292,  -628,  -628,  -628,  -628,  -628,   360,   109,  -628,   109,
     -35,  -628,   109,  -628,  -628,  -628,  -628,   415,  -628,  -628,
     109,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -107,   361,  -628,  -628,   -53,   441,   293,    14,  -628,
     432,  -628,  -628,   438,  -628,   294,  -628,  -628,  -628,  -628,
    -628,  -628,   428,  -628,   295,    77,  2017,  -628,  -628,  2378,
    2560,    19,  4158,  -628,  4158,  -628,  2017,    29,   439,   523,
    2017,  -628,   289,   443,   410,   320,  -628,  -628,  -628,  -628,
    -628,  3278,  3278,  3278,  3278,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,   -51,  -628,   302,  4158,   303,   304,  -628,
    -628,  -628,   305,   -68,  -628,  3630,   307,  -628,  3806,  -628,
    -628,  -628,  -628,  -628,  -628,  4158,  4158,   446,  3806,  3806,
    3806,  3806,  3806,  3806,  3806,  3806,  3806,  3806,  3806,   -12,
    -628,  -628,  -628,  -628,  -628,  -628,   308,   322,  -628,  -628,
     469,    42,  -628,  -628,  -628,   109,  -628,  -628,  -628,  -628,
     382,   388,   323,   279,  -628,  -628,   343,   449,  4158,  4158,
    -628,  -628,   -16,  -628,  -628,  -628,  -628,  4158,  -628,  4158,
    -628,  -628,   512,   521,   517,   517,   517,  -628,  -628,  4158,
    -628,  3454,  -628,  4158,    73,   523,  -628,   533,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,  -628,  -628,    40,  -628,  4158,
     324,  -628,  -628,  -628,   325,   326,   328,   149,  -628,  -628,
    -628,  -628,    -3,    -2,     5,     6,   329,   330,   331,   334,
     335,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,   247,    10,    13,    16,    20,    21,    67,
     479,    22,  -628,  -628,  -628,  -628,  -628,   464,   355,   336,
    -628,  -628,  -628,  -628,  -628,   340,   341,  -628,  -628,  -628,
     527,   528,   529,  2922,  4158,  -628,  -628,    -5,  -628,  -628,
     541,  4158,  -628,  -628,  -628,  -628,  -628,  -628,   344,   345,
     346,    25,  -628,  -628,  -628,  -628,  -628,  -628,  4158,  3278,
    3806,  -628,  -628,  -628,  -628,  -628,  -628,   348,   349,   350,
     354,   358,   365,   347,   364,   367,  -628,  -628,  -628,   471,
    -628,  -628,  -628,  -628,  -628,  4158,  4158,  -628,  -628,  4158,
    -628,  4158,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    3806,  3806,   -84,   370,   480,   526,    67,  3806,   530,  -628,
    -628,  3982,  -628,  -628,   374,   378,   384,  -628,  -628,   391,
     387,   385,  -628,  -628,   -47,  -628,  4158,  -628,  -628,  -628,
    3806,  3806,  -628,   480,  -628,  3806,  -628,   392,  -628,  -628,
     393,  -628,  -628
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -628,  -628,  -628,   472,   542,   -26,     4,  -159,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,   399,  -628,  -628,  -628,  -628,   240,  -628,  -369,
     -73,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,   466,  -628,  -628,  -157,  -166,  -390,  -628,
    -152,  -143,  -628,  -628,    99,   123,   124,   150,   176,  -628,
     100,  -628,  -461,   451,  -628,  -628,  -628,  -292,  -628,  -628,
    -318,  -628,  -392,  -184,  -234,  -628,  -523,  -627,  -509,  -628,
    -628,  -628,  -628,  -628,  -628,   366,  -628,  -628,  -628,   249,
     372,  -628,   -43,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,    54,   -44,  -361,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -183,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,  -628,
    -628,  -628,   -77,  -628,  -628,   501,   159,  -628,  -150,  -628,
    -628,  -393,  -628,    -9
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -573
static const yytype_int16 yytable[] =
{
     271,   275,   461,   460,   107,   462,   660,   662,   663,   664,
     400,   578,   289,   290,   684,   304,   304,   304,   304,   304,
     304,   304,   363,   364,   469,   680,   681,   630,   421,   685,
     386,   592,   688,   636,  -113,  -114,   782,   334,   335,   107,
     708,   771,   282,   276,   277,   832,   833,   721,   722,   723,
     724,   725,   726,   834,   835,   419,  -572,   700,   842,   608,
     896,   843,    24,   283,   844,   190,   191,   365,   845,   846,
     856,   337,   703,   876,   803,   394,   378,   481,   421,   608,
     278,   287,  -188,   409,   355,   357,   410,   368,   754,   755,
     756,   355,   637,   279,   476,   444,   479,   191,   557,   197,
     195,   565,   369,   359,   358,   385,   547,   107,   548,   288,
     379,   443,    24,   350,   562,   355,   480,   362,   559,   423,
     445,   351,   360,   436,   695,   107,   286,   395,   701,   560,
     638,   355,   649,   650,   651,   355,   639,   640,   482,   477,
     291,   355,   457,   804,   457,   457,   457,   366,   411,   761,
     107,   704,   778,   779,   198,   897,   199,   330,   331,   761,
     772,   773,   774,   775,   776,   777,   641,   757,   780,   781,
     743,   744,   745,   746,   192,   193,   389,   390,   391,   338,
     200,   566,   201,   202,   747,   380,   203,   567,   748,   381,
     329,   194,   914,   333,   847,   915,   204,   205,   353,   642,
     206,   332,   354,   336,   355,   192,   193,   370,   848,   849,
     850,   340,   851,   879,   428,   608,   371,   372,   437,   373,
     374,   375,   194,   783,   342,   343,   489,   490,   491,   492,
     869,   341,   783,   783,   870,   284,   420,  -572,   429,   430,
     783,   783,   438,   439,   339,   783,   447,   447,   783,  -355,
     709,   783,   280,   739,   727,   783,   783,   783,   463,   464,
     783,   447,   603,  -188,   609,   431,   610,   733,   762,   440,
     763,   764,   765,   498,   305,   306,   307,   308,   309,   310,
     207,   605,   502,   345,   346,   355,   347,   348,   784,   785,
     786,   432,   787,   788,   304,   441,   791,   519,   520,   485,
     486,   558,   561,   563,   564,   828,   829,   830,   208,   569,
     571,   539,   540,   570,   355,   447,   715,   545,   575,   421,
       3,     4,     5,     6,     7,   667,   668,    10,   403,   404,
     383,   761,   811,   812,   377,   396,   387,   521,   392,   398,
     401,   583,   798,   402,   405,   406,   407,   408,   412,   585,
     413,   416,   417,    24,    25,    26,    27,    28,    29,   414,
     415,   434,    31,   447,   904,   458,   470,   472,   831,   471,
     473,   894,   895,   474,   522,   523,   524,   525,   878,   526,
     527,   528,   529,   530,   531,   475,   478,   588,   532,   483,
     484,   487,   920,   594,   421,     3,     4,     5,     6,     7,
     622,   917,   918,   488,    40,   493,   494,   497,   499,   500,
     422,   495,   501,   503,   496,   508,   512,   514,   504,   505,
     506,   507,   510,   515,   513,   516,   517,   518,    24,    25,
      26,    27,    28,    29,   534,   535,   536,   537,   538,   541,
     546,   552,   555,   553,   554,   556,   568,   572,   615,   574,
     573,   604,   576,   577,   716,   370,   579,   582,   580,   581,
     584,   586,   587,   589,   732,   590,   593,   595,   738,   596,
     597,   601,   602,   606,   616,   598,   599,   600,   629,   629,
     619,   618,   620,   623,   271,   629,   624,   645,   621,   631,
     632,   633,   647,   634,   646,   625,   648,   653,   658,   689,
     665,   657,   666,   690,   669,   698,   670,   671,   672,   673,
     674,   675,   676,   693,   677,   694,   696,   678,   697,   706,
     702,   686,   710,   381,   713,   687,   699,   691,   711,   735,
     707,   712,   736,   740,   714,   741,   770,   792,   749,   800,
     807,   795,   751,   752,   753,   742,   759,   796,   789,   808,
     799,   809,   821,   854,   857,   862,   863,   864,   797,   704,
     871,   889,   790,   824,   825,   826,   838,   827,   836,   837,
     899,   839,   840,   859,   902,   418,   820,   328,   905,   860,
     861,   427,   886,   873,   874,   875,   880,   881,   882,   468,
     822,   819,   883,   635,   903,   919,   884,   457,   446,   841,
     718,   720,   887,   730,   877,   731,   885,   457,   888,   898,
     692,   457,   758,   908,   793,   858,   433,   909,     0,   617,
     913,   910,   629,   629,   629,   629,   912,     0,   911,     0,
       0,   921,   922,     0,     0,     0,     0,   750,     0,     0,
       0,     0,     0,     0,     0,     0,   657,     0,     0,     0,
       0,   794,     0,     0,     0,     0,   766,   769,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   872,     0,   511,     0,   509,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   801,
     802,     0,     0,     0,     0,     0,     0,     0,   805,     0,
     806,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     813,   893,   816,     0,   818,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     823,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     1,
       0,     0,     0,     2,     3,     4,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,     0,     0,    16,
      17,    18,    19,    20,   867,   868,     0,     0,     0,     0,
       0,     0,   457,    21,    22,     0,    23,    24,    25,    26,
      27,    28,    29,    30,     0,    31,     0,     0,     0,   769,
     629,     0,     0,     0,     0,    32,    33,    34,     0,    35,
      36,     0,     0,     0,     0,     0,     0,     0,    37,     0,
       0,     0,     0,     0,     0,     0,   890,   891,     0,     0,
     892,     0,   457,     0,     0,    38,    39,     0,    40,    41,
      42,    43,    44,     0,     0,     0,    45,     0,     0,     0,
       0,     0,   907,     0,    46,     0,    47,    48,    49,    50,
      51,    52,    53,    54,     0,     0,     0,   916,     0,     0,
      55,    56,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    57,    58,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    59,     0,
       0,     0,     0,    60,    61,    62,     0,     0,     0,     0,
       0,    63,     0,     0,     0,    64,     0,    65,     0,    66,
      67,    68,    69,     0,     0,     0,   311,     0,     0,    70,
      71,     0,    72,    73,    74,     0,    75,    76,     0,     0,
       0,    77,     0,    78,    79,     0,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,     0,     0,     0,     0,     0,    96,    97,    98,
      99,   100,   101,  -226,   312,     0,     0,     0,  -226,     0,
       0,     0,  -226,  -226,  -226,  -226,  -226,  -226,  -226,     0,
       0,     0,  -226,     0,     0,  -226,     0,  -226,  -226,  -226,
    -226,  -226,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   313,  -226,     0,     0,     0,     0,
       0,     0,     0,     0,  -226,  -226,     0,     0,  -226,  -226,
    -226,  -226,  -226,     0,     0,     0,     0,     0,     0,     0,
       0,  -226,  -226,     0,     0,  -226,  -226,  -226,  -226,  -226,
    -226,     0,   314,   315,   316,   317,   318,   319,   320,   321,
     322,   323,     0,   324,     0,     0,   325,     0,     0,     0,
       0,     0,     0,     0,     0,     0,  -226,  -226,  -226,     0,
       0,     0,     0,     0,     0,     0,  -226,  -226,     0,  -226,
       0,     0,     0,  -226,  -226,     0,  -226,     0,     0,  -226,
    -226,     0,     0,     0,     0,     0,   292,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,  -226,     0,
       0,     0,  -226,     0,     0,     0,  -226,  -226,     0,  -226,
       0,     0,  -226,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,  -226,
       0,  -291,     0,  -226,  -226,  -226,  -226,  -226,   293,     0,
       0,     0,   211,   212,   213,   214,   215,   216,   217,     0,
       0,     0,   218,     0,   294,   219,     0,   220,   221,   222,
     223,   224,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   225,     0,     0,     0,     0,
       0,     0,     0,     0,   228,   229,     0,     0,   230,   231,
     232,   233,   234,     0,     0,     0,     0,     0,     0,     0,
       0,   272,   273,     0,     0,   239,   240,   241,   242,   243,
     244,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   295,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   296,   297,   298,   245,   246,   247,   299,
       0,     0,     0,     0,   300,     0,   248,   249,     0,   250,
       0,     0,     0,   251,   252,     0,   253,     0,     0,   254,
     255,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   256,     0,
       0,     0,   257,     0,     0,     0,   258,   259,     0,   260,
       0,     0,   261,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   262,
       0,     0,     0,   263,   264,   265,   266,   267,     2,     3,
       4,     5,     6,     7,     8,     9,    10,    11,    12,    13,
      14,    15,     0,     0,    16,    17,    18,    19,    20,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    21,    22,
       0,    23,    24,    25,    26,    27,    28,    29,    30,     0,
      31,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      32,    33,    34,     0,    35,    36,     0,     0,     0,     0,
       0,     0,     0,    37,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      38,    39,     0,    40,    41,    42,    43,    44,     0,     0,
       0,    45,     0,     0,     0,     0,     0,     0,     0,    46,
       0,    47,    48,    49,    50,    51,    52,    53,    54,     0,
       0,     0,     0,     0,     0,    55,    56,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    57,
      58,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    59,     0,     0,     0,     0,    60,    61,
      62,     0,     0,     0,     0,     0,    63,     0,     0,     0,
      64,     0,    65,     0,    66,    67,    68,    69,     0,     0,
       0,     0,     0,     0,    70,    71,     0,    72,    73,    74,
       0,    75,    76,     0,     0,     0,    77,     0,    78,    79,
       0,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,     0,     0,     0,
       0,     0,    96,    97,    98,    99,   100,   101,     2,     3,
       4,     5,     6,     7,     8,     9,    10,    11,    12,    13,
      14,    15,     0,     0,    16,    17,    18,    19,    20,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    21,    22,
       0,    23,    24,    25,    26,    27,    28,    29,    30,     0,
      31,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      32,    33,    34,     0,   327,    36,     0,     0,     0,     0,
       0,     0,     0,    37,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      38,    39,     0,    40,    41,    42,    43,    44,     0,     0,
       0,    45,     0,     0,     0,     0,     0,     0,     0,    46,
       0,    47,    48,    49,    50,    51,    52,    53,    54,     0,
       0,     0,     0,     0,     0,    55,    56,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    57,
      58,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    59,     0,     0,     0,     0,    60,    61,
      62,     0,     0,     0,     0,     0,    63,     0,     0,     0,
      64,     0,    65,     0,    66,    67,    68,    69,     0,     0,
       0,     0,     0,     0,    70,    71,     0,    72,    73,    74,
       0,    75,    76,     0,     0,     0,    77,     0,    78,    79,
       0,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,     0,     0,     0,
       0,     0,    96,    97,    98,    99,   100,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,     0,     0,    16,    17,    18,    19,    20,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    21,    22,     0,
      23,    24,    25,    26,    27,    28,    29,    30,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    32,
      33,    34,     0,   327,    36,     0,     0,     0,     0,     0,
       0,     0,    37,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    38,
      39,     0,    40,    41,    42,    43,    44,     0,     0,     0,
      45,     0,     0,     0,     0,     0,     0,     0,    46,     0,
      47,    48,    49,    50,    51,    52,    53,    54,     0,     0,
       0,     0,     0,     0,    55,    56,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    57,    58,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    59,     0,     0,     0,     0,    60,    61,    62,
       0,     0,     0,     0,     0,    63,     0,     0,     0,    64,
       0,    65,     0,    66,    67,    68,    69,     0,     0,     0,
       0,     0,   421,    70,    71,     0,    72,    73,    74,     0,
      75,    76,     0,     0,     0,    77,     0,    78,    79,     0,
      80,    81,    82,    83,    84,    85,    86,    87,    88,    89,
      90,    91,    92,    93,    94,    95,    24,     0,     0,     0,
       0,    96,    97,    98,    99,   100,     0,     0,     0,     0,
       0,     0,     0,     0,   211,   212,   213,   214,   215,   216,
     217,     0,     0,     0,   218,     0,     0,   219,     0,   220,
     221,   222,   223,   224,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   225,     0,     0,
       0,     0,     0,     0,     0,     0,   228,   229,     0,     0,
     230,   231,   232,   233,   234,     0,     0,     0,     0,     0,
       0,     0,     0,   272,   273,     0,     0,   239,   240,   241,
     242,   243,   244,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   245,   246,
     247,     0,     0,     0,     0,     0,     0,     0,   248,   249,
       0,   250,     0,     0,     0,   251,   252,     0,   253,     0,
       0,   254,   255,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     256,     0,     0,     0,   257,     0,     0,     0,   258,   259,
       0,   260,     0,     0,   261,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   262,     0,     0,     0,   263,   264,   265,   266,   267,
     542,     0,     0,     0,     0,   543,     0,     0,     0,   211,
     212,   213,   214,   215,   216,   217,     0,     0,     0,   218,
       0,     0,   219,     0,   220,   221,   222,   223,   224,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   225,     0,     0,     0,     0,     0,     0,     0,
       0,   228,   229,     0,     0,   230,   231,   232,   233,   234,
       0,     0,     0,     0,     0,     0,     0,     0,   272,   273,
       0,     0,   239,   240,   241,   242,   243,   244,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   245,   246,   247,     0,     0,     0,     0,
       0,     0,     0,   248,   249,     0,   250,     0,     0,     0,
     251,   252,     0,   253,     0,     0,   254,   255,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   256,     0,     0,     0,   257,
       0,     0,     0,   258,   259,     0,   260,     0,     0,   261,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   262,     0,     0,     0,
     263,   264,   265,   266,   267,   211,   212,   213,   214,   215,
     216,   217,     0,     0,     0,   218,     0,     0,   219,     0,
     220,   221,   222,   223,   224,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   225,     0,
       0,     0,     0,     0,     0,     0,     0,   228,   229,     0,
       0,   230,   231,   232,   233,   234,     0,     0,     0,     0,
       0,     0,     0,     0,   272,   273,     0,     0,   239,   240,
     241,   242,   243,   244,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   245,
     246,   247,     0,     0,     0,     0,     0,     0,     0,   248,
     249,     0,   250,     0,     0,     0,   251,   252,     0,   253,
       0,     0,   254,   255,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   256,     0,     0,     0,   257,     0,     0,     0,   258,
     259,     0,   260,     0,     0,   261,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   262,     0,     0,     0,   263,   264,   265,   266,
     267,     0,     0,     0,     0,     0,   717,   211,   212,   213,
     214,   215,   216,   217,     0,     0,     0,   218,     0,     0,
     219,     0,   220,   221,   222,   223,   224,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     225,     0,     0,     0,     0,     0,     0,     0,     0,   228,
     229,     0,     0,   230,   231,   232,   233,   234,     0,     0,
       0,     0,     0,     0,     0,     0,   272,   273,     0,     0,
     239,   240,   241,   242,   243,   244,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   245,   246,   247,     0,     0,     0,     0,     0,     0,
       0,   248,   249,     0,   250,     0,     0,     0,   251,   252,
       0,   253,     0,     0,   254,   255,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   256,     0,     0,     0,   257,     0,     0,
       0,   258,   259,     0,   260,     0,     0,   261,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   262,     0,     0,     0,   263,   264,
     265,   266,   267,     0,     0,     0,     0,     0,   719,   211,
     212,   213,   214,   215,   216,   217,     0,     0,     0,   218,
       0,     0,   219,     0,   220,   221,   222,   223,   224,   612,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   225,     0,     0,     0,     0,     0,   613,     0,
       0,   228,   229,     0,     0,   230,   231,   232,   233,   234,
       0,     0,     0,     0,     0,     0,     0,     0,   272,   273,
       0,     0,   239,   240,   241,   242,   243,   244,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   245,   246,   247,     0,     0,     0,     0,
       0,     0,     0,   248,   249,     0,   250,     0,     0,     0,
     251,   252,     0,   253,     0,     0,   254,   255,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   256,     0,     0,     0,   257,
       0,     0,     0,   258,   259,     0,   260,     0,     0,   261,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   262,     0,     0,     0,
     263,   264,   265,   266,   267,     0,     0,     0,   614,   211,
     212,   213,   214,   215,   216,   217,     0,     0,     0,   218,
       0,     0,   219,     0,   220,   221,   222,   223,   224,   865,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   225,     0,     0,     0,     0,     0,     0,     0,
       0,   228,   229,     0,     0,   230,   231,   232,   233,   234,
       0,     0,     0,     0,     0,     0,     0,     0,   272,   273,
       0,     0,   239,   240,   241,   242,   243,   244,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   245,   246,   247,     0,     0,     0,     0,
       0,     0,     0,   248,   249,     0,   250,     0,     0,     0,
     251,   252,     0,   253,     0,     0,   254,   255,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   256,     0,     0,     0,   257,
       0,     0,     0,   258,   259,     0,   260,     0,     0,   261,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   262,     0,     0,     0,
     263,   264,   265,   266,   267,     0,     0,     0,   866,   211,
     212,   213,   214,   215,   216,   217,     0,     0,     0,   218,
       0,     0,   219,     0,   220,   221,   222,   223,   224,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   225,     0,     0,     0,     0,     0,   226,   227,
       0,   228,   229,     0,     0,   230,   231,   232,   233,   234,
       0,     0,     0,     0,     0,     0,     0,     0,   235,   236,
     237,   238,   239,   240,   241,   242,   243,   244,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   245,   246,   247,     0,     0,     0,     0,
       0,     0,     0,   248,   249,     0,   250,     0,     0,     0,
     251,   252,     0,   253,     0,     0,   254,   255,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   256,     0,     0,     0,   257,
       0,     0,     0,   258,   259,     0,   260,     0,     0,   261,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   262,     0,     0,     0,
     263,   264,   265,   266,   267,   211,   212,   213,   214,   215,
     216,   217,     0,     0,     0,   218,     0,     0,   219,     0,
     220,   221,   222,   223,   224,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   225,     0,
       0,     0,     0,     0,     0,     0,     0,   228,   229,     0,
       0,   230,   231,   232,   233,   234,     0,     0,     0,     0,
       0,     0,     0,     0,   272,   273,     0,     0,   239,   240,
     241,   242,   243,   244,   626,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   245,
     246,   247,     0,     0,     0,     0,     0,   627,     0,   248,
     249,     0,   250,     0,     0,     0,   251,   252,     0,   253,
       0,     0,   254,   255,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   256,     0,     0,     0,   257,     0,     0,     0,   258,
     259,     0,   260,     0,     0,   261,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   262,     0,     0,     0,   263,   264,   265,   266,
     267,   211,   212,   213,   214,   215,   216,   217,     0,     0,
       0,   218,     0,     0,   219,     0,   220,   221,   222,   223,
     224,   814,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   225,     0,     0,     0,     0,     0,
     815,     0,     0,   228,   229,     0,     0,   230,   231,   232,
     233,   234,     0,     0,     0,     0,     0,     0,     0,     0,
     272,   273,     0,     0,   239,   240,   241,   242,   243,   244,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   245,   246,   247,     0,     0,
       0,     0,     0,     0,     0,   248,   249,     0,   250,     0,
       0,     0,   251,   252,     0,   253,     0,     0,   254,   255,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   256,     0,     0,
       0,   257,     0,     0,     0,   258,   259,     0,   260,     0,
       0,   261,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   262,     0,
       0,     0,   263,   264,   265,   266,   267,   211,   212,   213,
     214,   215,   216,   217,     0,     0,     0,   218,     0,     0,
     219,     0,   220,   221,   222,   223,   224,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     225,     0,     0,     0,     0,     0,     0,     0,     0,   228,
     229,     0,     0,   230,   231,   232,   233,   234,     0,     0,
       0,     0,     0,     0,     0,     0,   272,   273,     0,     0,
     239,   240,   241,   242,   243,   244,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   245,   246,   247,     0,     0,     0,     0,     0,   654,
       0,   248,   249,     0,   250,     0,     0,     0,   251,   252,
       0,   253,     0,     0,   254,   255,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   256,     0,     0,     0,   257,     0,     0,
       0,   258,   259,     0,   260,     0,     0,   261,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   262,     0,     0,     0,   263,   264,
     265,   266,   267,   211,   212,   213,   214,   215,   216,   217,
       0,     0,     0,   218,     0,     0,   219,     0,   220,   221,
     222,   223,   224,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   225,     0,     0,     0,
       0,   659,     0,     0,     0,   228,   229,     0,     0,   230,
     231,   232,   233,   234,     0,     0,     0,     0,     0,     0,
       0,     0,   272,   273,     0,     0,   239,   240,   241,   242,
     243,   244,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   245,   246,   247,
       0,     0,     0,     0,     0,     0,     0,   248,   249,     0,
     250,     0,     0,     0,   251,   252,     0,   253,     0,     0,
     254,   255,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   256,
       0,     0,     0,   257,     0,     0,     0,   258,   259,     0,
     260,     0,     0,   261,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     262,     0,     0,     0,   263,   264,   265,   266,   267,   211,
     212,   213,   214,   215,   216,   217,     0,     0,     0,   218,
       0,     0,   219,     0,   220,   221,   222,   223,   224,   906,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   225,     0,     0,     0,     0,     0,     0,     0,
       0,   228,   229,     0,     0,   230,   231,   232,   233,   234,
       0,     0,     0,     0,     0,     0,     0,     0,   272,   273,
       0,     0,   239,   240,   241,   242,   243,   244,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   245,   246,   247,     0,     0,     0,     0,
       0,     0,     0,   248,   249,     0,   250,     0,     0,     0,
     251,   252,     0,   253,     0,     0,   254,   255,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   256,     0,     0,     0,   257,
       0,     0,     0,   258,   259,     0,   260,     0,     0,   261,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   262,     0,     0,     0,
     263,   264,   265,   266,   267,   211,   212,   213,   214,   215,
     216,   217,     0,     0,     0,   218,     0,     0,   219,     0,
     220,   221,   222,   223,   224,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   225,     0,
       0,     0,     0,     0,     0,     0,     0,   228,   229,     0,
       0,   230,   231,   232,   233,   234,     0,     0,     0,     0,
       0,     0,     0,     0,   272,   273,     0,     0,   239,   240,
     241,   242,   243,   244,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   245,
     246,   247,     0,     0,     0,     0,     0,     0,     0,   248,
     249,     0,   250,     0,     0,     0,   251,   252,     0,   253,
       0,     0,   254,   255,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   256,     0,     0,     0,   257,     0,     0,     0,   258,
     259,     0,   260,     0,     0,   261,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   262,     0,     0,     0,   263,   264,   265,   266,
     267,   211,   212,   213,   214,   215,   216,   217,     0,     0,
       0,   218,     0,     0,   219,     0,   220,   221,   222,   223,
     224,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   442,     0,     0,     0,     0,     0,
       0,     0,     0,   228,   229,     0,     0,   230,   231,   232,
     233,   234,     0,     0,     0,     0,     0,     0,     0,     0,
     272,   273,     0,     0,   239,   240,   241,   242,   243,   244,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   245,   246,   247,     0,     0,
       0,     0,     0,     0,     0,   248,   249,     0,   250,     0,
       0,     0,   251,   252,     0,   253,     0,     0,   254,   255,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   256,     0,     0,
       0,   257,     0,     0,     0,   258,   259,     0,   260,     0,
       0,   261,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   262,     0,
       0,     0,   263,   264,   265,   266,   267
};

static const yytype_int16 yycheck[] =
{
       9,    10,   154,   153,     0,   155,   515,   516,   517,   518,
      83,   372,    21,    22,   537,    24,    25,    26,    27,    28,
      29,    30,    66,    67,   190,   534,   535,   488,     5,   538,
      74,   400,   541,   494,    26,    26,    48,    46,    47,    35,
      26,   668,    35,    19,    20,    48,    48,    28,    29,    30,
      31,    32,    33,    48,    48,    53,    53,   164,    48,   449,
     144,    48,    39,    56,    48,    86,   118,    90,    48,    48,
      48,   120,   125,    48,    90,    26,   159,    60,     5,   469,
      56,   200,    53,   159,   176,   159,   162,    56,   156,   157,
     158,   176,    52,    69,    60,   121,   104,   118,   159,    19,
     170,    90,    71,   161,   178,   197,    64,   103,    66,   228,
     193,   120,    39,   168,   159,   176,   124,   202,   159,   115,
     146,   176,   180,   119,   159,   121,   228,    78,   235,   170,
      90,   176,   151,   152,   153,   176,    96,    97,   121,   105,
      35,   176,   151,   159,   153,   154,   155,   170,   224,   658,
     146,   204,   675,   676,    74,   239,    76,    61,    62,   668,
     669,   670,   671,   672,   673,   674,   126,   235,   677,   678,
     631,   632,   633,   634,   226,   227,   200,   201,   202,   228,
     100,   170,   102,   103,   235,   159,   106,   176,   239,   163,
      63,   243,   239,    99,   127,   242,   116,   117,   170,   159,
     120,   237,   174,    90,   176,   226,   227,   176,   141,   142,
     143,   159,   145,   840,   115,   605,   185,   186,   119,   188,
     189,   190,   243,   235,   161,   162,   235,   236,   237,   238,
     235,   159,   235,   235,   239,   228,   234,   234,   115,   115,
     235,   235,   119,   119,   200,   235,   238,   238,   235,   169,
     236,   235,   228,   622,   235,   235,   235,   235,   100,   101,
     235,   238,   239,   234,   185,   115,   187,   238,   660,   119,
     662,   663,   664,   282,    25,    26,    27,    28,    29,    30,
     200,   447,   291,   168,   169,   176,   171,   172,   680,   681,
     682,   115,   684,   685,   303,   119,   688,    50,    51,    72,
      73,   345,   346,   347,   348,   156,   157,   158,   228,   353,
     354,   139,   140,   175,   176,   238,   239,   326,   362,     5,
       6,     7,     8,     9,    10,   236,   237,    13,    88,    89,
     159,   840,   725,   726,    90,    26,   159,    90,   159,    26,
      90,   385,   703,    77,    12,     6,   159,    90,   228,   393,
     228,    53,     0,    39,    40,    41,    42,    43,    44,   228,
     228,    96,    47,   238,   887,    75,   107,    55,   760,   243,
      60,   880,   881,    90,   127,   128,   129,   130,   839,   132,
     133,   134,   135,   136,   137,   244,   104,   396,   141,   121,
     169,   237,   915,   402,     5,     6,     7,     8,     9,    10,
     473,   910,   911,   237,    90,   235,   237,    69,   121,    26,
      96,   238,    37,   238,   236,    46,    48,    90,   238,   238,
     238,   238,   238,   237,    48,   237,   237,   237,    39,    40,
      41,    42,    43,    44,   237,   237,   237,   237,   237,   237,
     234,    90,   121,    90,   159,   159,    90,    90,   457,    90,
     159,   447,    90,    90,   606,   176,   159,   235,   159,   159,
     159,    90,   207,    90,   616,   176,   236,    90,   620,    90,
     159,    53,   235,    26,    26,   159,   159,   159,   487,   488,
      90,   101,    26,   237,   493,   494,    85,   496,   107,   237,
     237,   237,   159,   237,   503,   176,   159,   155,   237,    65,
     238,   510,   238,    67,   237,    90,   237,   237,   237,   237,
     237,   237,   237,   557,   237,   559,   560,   237,   562,    78,
     159,   237,    90,   163,    96,   237,   570,   235,    90,    90,
     237,   237,     9,    90,   239,   125,    90,    68,   236,    90,
      28,   159,   239,   239,   239,   225,   239,   159,   240,    28,
     207,    34,    19,    74,    90,    28,    28,    28,   235,   204,
      19,    90,   240,   239,   239,   239,   235,   239,   239,   239,
      90,   237,   237,   237,    48,   103,   735,    35,    48,   239,
     239,   115,   235,   239,   239,   239,   238,   238,   238,   190,
     747,   734,   238,   493,   886,   913,   238,   606,   147,   783,
     609,   610,   238,   612,   838,   614,   241,   616,   241,   239,
     556,   620,   655,   239,   691,   798,   115,   239,    -1,   460,
     235,   237,   631,   632,   633,   634,   239,    -1,   237,    -1,
      -1,   239,   239,    -1,    -1,    -1,    -1,   646,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   655,    -1,    -1,    -1,
      -1,   695,    -1,    -1,    -1,    -1,   665,   666,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   821,    -1,   307,    -1,   303,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   708,
     709,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   717,    -1,
     719,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     729,   871,   731,    -1,   733,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     749,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,     1,
      -1,    -1,    -1,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,    16,    17,    18,    -1,    -1,    21,
      22,    23,    24,    25,   813,   814,    -1,    -1,    -1,    -1,
      -1,    -1,   821,    35,    36,    -1,    38,    39,    40,    41,
      42,    43,    44,    45,    -1,    47,    -1,    -1,    -1,   838,
     839,    -1,    -1,    -1,    -1,    57,    58,    59,    -1,    61,
      62,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    70,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   865,   866,    -1,    -1,
     869,    -1,   871,    -1,    -1,    87,    88,    -1,    90,    91,
      92,    93,    94,    -1,    -1,    -1,    98,    -1,    -1,    -1,
      -1,    -1,   891,    -1,   106,    -1,   108,   109,   110,   111,
     112,   113,   114,   115,    -1,    -1,    -1,   906,    -1,    -1,
     122,   123,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   146,   147,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   160,    -1,
      -1,    -1,    -1,   165,   166,   167,    -1,    -1,    -1,    -1,
      -1,   173,    -1,    -1,    -1,   177,    -1,   179,    -1,   181,
     182,   183,   184,    -1,    -1,    -1,     1,    -1,    -1,   191,
     192,    -1,   194,   195,   196,    -1,   198,   199,    -1,    -1,
      -1,   203,    -1,   205,   206,    -1,   208,   209,   210,   211,
     212,   213,   214,   215,   216,   217,   218,   219,   220,   221,
     222,   223,    -1,    -1,    -1,    -1,    -1,   229,   230,   231,
     232,   233,   234,    48,    49,    -1,    -1,    -1,    53,    -1,
      -1,    -1,    57,    58,    59,    60,    61,    62,    63,    -1,
      -1,    -1,    67,    -1,    -1,    70,    -1,    72,    73,    74,
      75,    76,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    89,    90,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    99,   100,    -1,    -1,   103,   104,
     105,   106,   107,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   116,   117,    -1,    -1,   120,   121,   122,   123,   124,
     125,    -1,   127,   128,   129,   130,   131,   132,   133,   134,
     135,   136,    -1,   138,    -1,    -1,   141,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   151,   152,   153,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   161,   162,    -1,   164,
      -1,    -1,    -1,   168,   169,    -1,   171,    -1,    -1,   174,
     175,    -1,    -1,    -1,    -1,    -1,     1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   193,    -1,
      -1,    -1,   197,    -1,    -1,    -1,   201,   202,    -1,   204,
      -1,    -1,   207,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   224,
      -1,    46,    -1,   228,   229,   230,   231,   232,    53,    -1,
      -1,    -1,    57,    58,    59,    60,    61,    62,    63,    -1,
      -1,    -1,    67,    -1,    69,    70,    -1,    72,    73,    74,
      75,    76,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    90,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    99,   100,    -1,    -1,   103,   104,
     105,   106,   107,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   116,   117,    -1,    -1,   120,   121,   122,   123,   124,
     125,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   137,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   148,   149,   150,   151,   152,   153,   154,
      -1,    -1,    -1,    -1,   159,    -1,   161,   162,    -1,   164,
      -1,    -1,    -1,   168,   169,    -1,   171,    -1,    -1,   174,
     175,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   193,    -1,
      -1,    -1,   197,    -1,    -1,    -1,   201,   202,    -1,   204,
      -1,    -1,   207,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   224,
      -1,    -1,    -1,   228,   229,   230,   231,   232,     5,     6,
       7,     8,     9,    10,    11,    12,    13,    14,    15,    16,
      17,    18,    -1,    -1,    21,    22,    23,    24,    25,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    35,    36,
      -1,    38,    39,    40,    41,    42,    43,    44,    45,    -1,
      47,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      57,    58,    59,    -1,    61,    62,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    70,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      87,    88,    -1,    90,    91,    92,    93,    94,    -1,    -1,
      -1,    98,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,
      -1,   108,   109,   110,   111,   112,   113,   114,   115,    -1,
      -1,    -1,    -1,    -1,    -1,   122,   123,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   146,
     147,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   160,    -1,    -1,    -1,    -1,   165,   166,
     167,    -1,    -1,    -1,    -1,    -1,   173,    -1,    -1,    -1,
     177,    -1,   179,    -1,   181,   182,   183,   184,    -1,    -1,
      -1,    -1,    -1,    -1,   191,   192,    -1,   194,   195,   196,
      -1,   198,   199,    -1,    -1,    -1,   203,    -1,   205,   206,
      -1,   208,   209,   210,   211,   212,   213,   214,   215,   216,
     217,   218,   219,   220,   221,   222,   223,    -1,    -1,    -1,
      -1,    -1,   229,   230,   231,   232,   233,   234,     5,     6,
       7,     8,     9,    10,    11,    12,    13,    14,    15,    16,
      17,    18,    -1,    -1,    21,    22,    23,    24,    25,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    35,    36,
      -1,    38,    39,    40,    41,    42,    43,    44,    45,    -1,
      47,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      57,    58,    59,    -1,    61,    62,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    70,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      87,    88,    -1,    90,    91,    92,    93,    94,    -1,    -1,
      -1,    98,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,
      -1,   108,   109,   110,   111,   112,   113,   114,   115,    -1,
      -1,    -1,    -1,    -1,    -1,   122,   123,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   146,
     147,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   160,    -1,    -1,    -1,    -1,   165,   166,
     167,    -1,    -1,    -1,    -1,    -1,   173,    -1,    -1,    -1,
     177,    -1,   179,    -1,   181,   182,   183,   184,    -1,    -1,
      -1,    -1,    -1,    -1,   191,   192,    -1,   194,   195,   196,
      -1,   198,   199,    -1,    -1,    -1,   203,    -1,   205,   206,
      -1,   208,   209,   210,   211,   212,   213,   214,   215,   216,
     217,   218,   219,   220,   221,   222,   223,    -1,    -1,    -1,
      -1,    -1,   229,   230,   231,   232,   233,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,    16,    17,
      18,    -1,    -1,    21,    22,    23,    24,    25,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    35,    36,    -1,
      38,    39,    40,    41,    42,    43,    44,    45,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    57,
      58,    59,    -1,    61,    62,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    70,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    87,
      88,    -1,    90,    91,    92,    93,    94,    -1,    -1,    -1,
      98,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,    -1,
     108,   109,   110,   111,   112,   113,   114,   115,    -1,    -1,
      -1,    -1,    -1,    -1,   122,   123,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   146,   147,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   160,    -1,    -1,    -1,    -1,   165,   166,   167,
      -1,    -1,    -1,    -1,    -1,   173,    -1,    -1,    -1,   177,
      -1,   179,    -1,   181,   182,   183,   184,    -1,    -1,    -1,
      -1,    -1,     5,   191,   192,    -1,   194,   195,   196,    -1,
     198,   199,    -1,    -1,    -1,   203,    -1,   205,   206,    -1,
     208,   209,   210,   211,   212,   213,   214,   215,   216,   217,
     218,   219,   220,   221,   222,   223,    39,    -1,    -1,    -1,
      -1,   229,   230,   231,   232,   233,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    57,    58,    59,    60,    61,    62,
      63,    -1,    -1,    -1,    67,    -1,    -1,    70,    -1,    72,
      73,    74,    75,    76,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    90,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    99,   100,    -1,    -1,
     103,   104,   105,   106,   107,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   116,   117,    -1,    -1,   120,   121,   122,
     123,   124,   125,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   151,   152,
     153,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   161,   162,
      -1,   164,    -1,    -1,    -1,   168,   169,    -1,   171,    -1,
      -1,   174,   175,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     193,    -1,    -1,    -1,   197,    -1,    -1,    -1,   201,   202,
      -1,   204,    -1,    -1,   207,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   224,    -1,    -1,    -1,   228,   229,   230,   231,   232,
      48,    -1,    -1,    -1,    -1,    53,    -1,    -1,    -1,    57,
      58,    59,    60,    61,    62,    63,    -1,    -1,    -1,    67,
      -1,    -1,    70,    -1,    72,    73,    74,    75,    76,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    90,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    99,   100,    -1,    -1,   103,   104,   105,   106,   107,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   116,   117,
      -1,    -1,   120,   121,   122,   123,   124,   125,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   151,   152,   153,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   161,   162,    -1,   164,    -1,    -1,    -1,
     168,   169,    -1,   171,    -1,    -1,   174,   175,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   193,    -1,    -1,    -1,   197,
      -1,    -1,    -1,   201,   202,    -1,   204,    -1,    -1,   207,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   224,    -1,    -1,    -1,
     228,   229,   230,   231,   232,    57,    58,    59,    60,    61,
      62,    63,    -1,    -1,    -1,    67,    -1,    -1,    70,    -1,
      72,    73,    74,    75,    76,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    90,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    99,   100,    -1,
      -1,   103,   104,   105,   106,   107,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   116,   117,    -1,    -1,   120,   121,
     122,   123,   124,   125,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   151,
     152,   153,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   161,
     162,    -1,   164,    -1,    -1,    -1,   168,   169,    -1,   171,
      -1,    -1,   174,   175,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   193,    -1,    -1,    -1,   197,    -1,    -1,    -1,   201,
     202,    -1,   204,    -1,    -1,   207,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   224,    -1,    -1,    -1,   228,   229,   230,   231,
     232,    -1,    -1,    -1,    -1,    -1,   238,    57,    58,    59,
      60,    61,    62,    63,    -1,    -1,    -1,    67,    -1,    -1,
      70,    -1,    72,    73,    74,    75,    76,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      90,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    99,
     100,    -1,    -1,   103,   104,   105,   106,   107,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   116,   117,    -1,    -1,
     120,   121,   122,   123,   124,   125,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   151,   152,   153,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   161,   162,    -1,   164,    -1,    -1,    -1,   168,   169,
      -1,   171,    -1,    -1,   174,   175,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   193,    -1,    -1,    -1,   197,    -1,    -1,
      -1,   201,   202,    -1,   204,    -1,    -1,   207,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   224,    -1,    -1,    -1,   228,   229,
     230,   231,   232,    -1,    -1,    -1,    -1,    -1,   238,    57,
      58,    59,    60,    61,    62,    63,    -1,    -1,    -1,    67,
      -1,    -1,    70,    -1,    72,    73,    74,    75,    76,    77,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    90,    -1,    -1,    -1,    -1,    -1,    96,    -1,
      -1,    99,   100,    -1,    -1,   103,   104,   105,   106,   107,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   116,   117,
      -1,    -1,   120,   121,   122,   123,   124,   125,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   151,   152,   153,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   161,   162,    -1,   164,    -1,    -1,    -1,
     168,   169,    -1,   171,    -1,    -1,   174,   175,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   193,    -1,    -1,    -1,   197,
      -1,    -1,    -1,   201,   202,    -1,   204,    -1,    -1,   207,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   224,    -1,    -1,    -1,
     228,   229,   230,   231,   232,    -1,    -1,    -1,   236,    57,
      58,    59,    60,    61,    62,    63,    -1,    -1,    -1,    67,
      -1,    -1,    70,    -1,    72,    73,    74,    75,    76,    77,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    90,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    99,   100,    -1,    -1,   103,   104,   105,   106,   107,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   116,   117,
      -1,    -1,   120,   121,   122,   123,   124,   125,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   151,   152,   153,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   161,   162,    -1,   164,    -1,    -1,    -1,
     168,   169,    -1,   171,    -1,    -1,   174,   175,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   193,    -1,    -1,    -1,   197,
      -1,    -1,    -1,   201,   202,    -1,   204,    -1,    -1,   207,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   224,    -1,    -1,    -1,
     228,   229,   230,   231,   232,    -1,    -1,    -1,   236,    57,
      58,    59,    60,    61,    62,    63,    -1,    -1,    -1,    67,
      -1,    -1,    70,    -1,    72,    73,    74,    75,    76,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    90,    -1,    -1,    -1,    -1,    -1,    96,    97,
      -1,    99,   100,    -1,    -1,   103,   104,   105,   106,   107,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   116,   117,
     118,   119,   120,   121,   122,   123,   124,   125,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   151,   152,   153,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   161,   162,    -1,   164,    -1,    -1,    -1,
     168,   169,    -1,   171,    -1,    -1,   174,   175,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   193,    -1,    -1,    -1,   197,
      -1,    -1,    -1,   201,   202,    -1,   204,    -1,    -1,   207,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   224,    -1,    -1,    -1,
     228,   229,   230,   231,   232,    57,    58,    59,    60,    61,
      62,    63,    -1,    -1,    -1,    67,    -1,    -1,    70,    -1,
      72,    73,    74,    75,    76,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    90,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    99,   100,    -1,
      -1,   103,   104,   105,   106,   107,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   116,   117,    -1,    -1,   120,   121,
     122,   123,   124,   125,   126,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   151,
     152,   153,    -1,    -1,    -1,    -1,    -1,   159,    -1,   161,
     162,    -1,   164,    -1,    -1,    -1,   168,   169,    -1,   171,
      -1,    -1,   174,   175,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   193,    -1,    -1,    -1,   197,    -1,    -1,    -1,   201,
     202,    -1,   204,    -1,    -1,   207,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   224,    -1,    -1,    -1,   228,   229,   230,   231,
     232,    57,    58,    59,    60,    61,    62,    63,    -1,    -1,
      -1,    67,    -1,    -1,    70,    -1,    72,    73,    74,    75,
      76,    77,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    90,    -1,    -1,    -1,    -1,    -1,
      96,    -1,    -1,    99,   100,    -1,    -1,   103,   104,   105,
     106,   107,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     116,   117,    -1,    -1,   120,   121,   122,   123,   124,   125,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   151,   152,   153,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   161,   162,    -1,   164,    -1,
      -1,    -1,   168,   169,    -1,   171,    -1,    -1,   174,   175,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   193,    -1,    -1,
      -1,   197,    -1,    -1,    -1,   201,   202,    -1,   204,    -1,
      -1,   207,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   224,    -1,
      -1,    -1,   228,   229,   230,   231,   232,    57,    58,    59,
      60,    61,    62,    63,    -1,    -1,    -1,    67,    -1,    -1,
      70,    -1,    72,    73,    74,    75,    76,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      90,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    99,
     100,    -1,    -1,   103,   104,   105,   106,   107,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   116,   117,    -1,    -1,
     120,   121,   122,   123,   124,   125,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   151,   152,   153,    -1,    -1,    -1,    -1,    -1,   159,
      -1,   161,   162,    -1,   164,    -1,    -1,    -1,   168,   169,
      -1,   171,    -1,    -1,   174,   175,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   193,    -1,    -1,    -1,   197,    -1,    -1,
      -1,   201,   202,    -1,   204,    -1,    -1,   207,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   224,    -1,    -1,    -1,   228,   229,
     230,   231,   232,    57,    58,    59,    60,    61,    62,    63,
      -1,    -1,    -1,    67,    -1,    -1,    70,    -1,    72,    73,
      74,    75,    76,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    90,    -1,    -1,    -1,
      -1,    95,    -1,    -1,    -1,    99,   100,    -1,    -1,   103,
     104,   105,   106,   107,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   116,   117,    -1,    -1,   120,   121,   122,   123,
     124,   125,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   151,   152,   153,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   161,   162,    -1,
     164,    -1,    -1,    -1,   168,   169,    -1,   171,    -1,    -1,
     174,   175,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   193,
      -1,    -1,    -1,   197,    -1,    -1,    -1,   201,   202,    -1,
     204,    -1,    -1,   207,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     224,    -1,    -1,    -1,   228,   229,   230,   231,   232,    57,
      58,    59,    60,    61,    62,    63,    -1,    -1,    -1,    67,
      -1,    -1,    70,    -1,    72,    73,    74,    75,    76,    77,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    90,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    99,   100,    -1,    -1,   103,   104,   105,   106,   107,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   116,   117,
      -1,    -1,   120,   121,   122,   123,   124,   125,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   151,   152,   153,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   161,   162,    -1,   164,    -1,    -1,    -1,
     168,   169,    -1,   171,    -1,    -1,   174,   175,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   193,    -1,    -1,    -1,   197,
      -1,    -1,    -1,   201,   202,    -1,   204,    -1,    -1,   207,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   224,    -1,    -1,    -1,
     228,   229,   230,   231,   232,    57,    58,    59,    60,    61,
      62,    63,    -1,    -1,    -1,    67,    -1,    -1,    70,    -1,
      72,    73,    74,    75,    76,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    90,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    99,   100,    -1,
      -1,   103,   104,   105,   106,   107,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   116,   117,    -1,    -1,   120,   121,
     122,   123,   124,   125,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   151,
     152,   153,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   161,
     162,    -1,   164,    -1,    -1,    -1,   168,   169,    -1,   171,
      -1,    -1,   174,   175,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   193,    -1,    -1,    -1,   197,    -1,    -1,    -1,   201,
     202,    -1,   204,    -1,    -1,   207,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   224,    -1,    -1,    -1,   228,   229,   230,   231,
     232,    57,    58,    59,    60,    61,    62,    63,    -1,    -1,
      -1,    67,    -1,    -1,    70,    -1,    72,    73,    74,    75,
      76,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    90,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    99,   100,    -1,    -1,   103,   104,   105,
     106,   107,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     116,   117,    -1,    -1,   120,   121,   122,   123,   124,   125,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   151,   152,   153,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   161,   162,    -1,   164,    -1,
      -1,    -1,   168,   169,    -1,   171,    -1,    -1,   174,   175,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   193,    -1,    -1,
      -1,   197,    -1,    -1,    -1,   201,   202,    -1,   204,    -1,
      -1,   207,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   224,    -1,
      -1,    -1,   228,   229,   230,   231,   232
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     1,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    21,    22,    23,    24,
      25,    35,    36,    38,    39,    40,    41,    42,    43,    44,
      45,    47,    57,    58,    59,    61,    62,    70,    87,    88,
      90,    91,    92,    93,    94,    98,   106,   108,   109,   110,
     111,   112,   113,   114,   115,   122,   123,   146,   147,   160,
     165,   166,   167,   173,   177,   179,   181,   182,   183,   184,
     191,   192,   194,   195,   196,   198,   199,   203,   205,   206,
     208,   209,   210,   211,   212,   213,   214,   215,   216,   217,
     218,   219,   220,   221,   222,   223,   229,   230,   231,   232,
     233,   234,   246,   247,   248,   249,   250,   251,   256,   257,
     258,   259,   260,   263,   264,   269,   270,   271,   272,   273,
     274,   275,   278,   279,   280,   281,   283,   286,   287,   288,
     289,   290,   291,   292,   293,   294,   295,   296,   297,   298,
     309,   310,   311,   312,   313,   314,   318,   319,   334,   335,
     336,   337,   338,   339,   341,   342,   343,   351,   352,   354,
     355,   359,   361,   363,   364,   365,   367,   369,   370,   371,
     372,   374,   375,   376,   377,   378,   380,   381,   383,   384,
     387,   388,   389,   390,   391,   392,   394,   398,   399,   400,
      86,   118,   226,   227,   243,   170,   340,    19,    74,    76,
     100,   102,   103,   106,   116,   117,   120,   200,   228,   353,
     393,    57,    58,    59,    60,    61,    62,    63,    67,    70,
      72,    73,    74,    75,    76,    90,    96,    97,    99,   100,
     103,   104,   105,   106,   107,   116,   117,   118,   119,   120,
     121,   122,   123,   124,   125,   151,   152,   153,   161,   162,
     164,   168,   169,   171,   174,   175,   193,   197,   201,   202,
     204,   207,   224,   228,   229,   230,   231,   232,   315,   316,
     407,   408,   116,   117,   299,   408,    19,    20,    56,    69,
     228,   261,    35,    56,   228,   262,   228,   200,   228,   408,
     408,    35,     1,    53,    69,   137,   148,   149,   150,   154,
     159,   344,   345,   348,   408,   344,   344,   344,   344,   344,
     344,     1,    49,    89,   127,   128,   129,   130,   131,   132,
     133,   134,   135,   136,   138,   141,   320,    61,   249,    63,
      61,    62,   237,    99,   408,   408,    90,   120,   228,   200,
     159,   159,   161,   162,   362,   168,   169,   171,   172,   368,
     168,   176,   366,   170,   174,   176,   357,   159,   178,   161,
     180,   360,   202,   357,   357,    90,   170,   373,    56,    71,
     176,   185,   186,   188,   189,   190,   358,    90,   159,   193,
     159,   163,   356,   159,   379,   197,   357,   159,   382,   200,
     201,   202,   159,   386,    26,    78,    26,   282,    26,   285,
     285,    90,    77,   282,   282,    12,     6,   159,    90,   159,
     162,   224,   228,   228,   228,   228,    53,     0,   248,    53,
     234,     5,    96,   251,   265,   267,   268,   298,   309,   310,
     311,   312,   313,   400,    96,   266,   251,   309,   310,   311,
     312,   313,    90,   408,   250,   250,   318,   238,   277,   302,
     303,   304,   251,   305,   306,   402,   403,   408,    75,   350,
     403,   305,   403,   100,   101,   253,   254,   255,   277,   302,
     107,   243,    55,    60,    90,   244,    60,   105,   104,   104,
     124,    60,   121,   121,   169,    72,    73,   237,   237,   408,
     408,   408,   408,   235,   237,   238,   236,    69,   408,   121,
      26,    37,   408,   238,   238,   238,   238,   238,    46,   345,
     238,   340,    48,    48,    90,   237,   237,   237,   237,    50,
      51,    90,   127,   128,   129,   130,   132,   133,   134,   135,
     136,   137,   141,   328,   237,   237,   237,   237,   237,   139,
     140,   237,    48,    53,   321,   408,   234,    64,    66,   395,
     396,   397,    90,    90,   159,   121,   159,   159,   357,   159,
     170,   357,   159,   357,   357,    90,   170,   176,    90,   357,
     175,   357,    90,   159,    90,   357,    90,    90,   358,   159,
     159,   159,   235,   357,   159,   357,    90,   207,   408,    90,
     176,   284,   284,   236,   408,    90,    90,   159,   159,   159,
     159,    53,   235,   239,   251,   302,    26,   276,   303,   185,
     187,   401,    77,    96,   236,   408,    26,   401,   101,    90,
      26,   107,   285,   237,    85,   176,   126,   159,   317,   408,
     317,   237,   237,   237,   237,   315,   317,    52,    90,    96,
      97,   126,   159,   300,   301,   408,   408,   159,   159,   151,
     152,   153,   349,   155,   159,   346,   347,   408,   237,    95,
     333,   408,   333,   333,   333,   238,   238,   236,   237,   237,
     237,   237,   237,   237,   237,   237,   237,   237,   237,   327,
     333,   333,   331,   333,   331,   333,   237,   237,   333,    65,
      67,   235,   356,   357,   357,   159,   357,   357,    90,   357,
     164,   235,   159,   125,   204,   385,    78,   237,    26,   236,
      90,    90,   237,    96,   239,   239,   305,   238,   408,   238,
     408,    28,    29,    30,    31,    32,    33,   235,   404,   405,
     408,   408,   305,   238,   307,    90,     9,   252,   305,   284,
      90,   125,   225,   317,   317,   317,   317,   235,   239,   236,
     408,   239,   239,   239,   156,   157,   158,   235,   347,   239,
     332,   333,   327,   327,   327,   327,   408,   329,   330,   408,
      90,   332,   333,   333,   333,   333,   333,   333,   331,   331,
     333,   333,    48,   235,   327,   327,   327,   327,   327,   240,
     240,   327,    68,   397,   357,   159,   159,   235,   358,   207,
      90,   408,   408,    90,   159,   408,   408,    28,    28,    34,
     406,   406,   406,   408,    77,    96,   408,   308,   408,   306,
     252,    19,   301,   408,   239,   239,   239,   239,   156,   157,
     158,   327,    48,    48,    48,    48,   239,   239,   235,   237,
     237,   328,    48,    48,    48,    48,    48,   127,   141,   142,
     143,   145,   322,   323,    74,   324,    48,    90,   385,   237,
     239,   239,    28,    28,    28,    77,   236,   408,   408,   235,
     239,    19,   403,   239,   239,   239,    48,   329,   317,   332,
     238,   238,   238,   238,   238,   241,   235,   238,   241,    90,
     408,   408,   408,   403,   333,   333,   144,   239,   239,    90,
     325,   326,    48,   322,   331,    48,    77,   408,   239,   239,
     237,   237,   239,   235,   239,   242,   408,   333,   333,   325,
     331,   239,   239
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

    {;}
    break;

  case 41:

    { result->cur_stmt_type_ = OBPROXY_T_OTHERS; ;}
    break;

  case 42:

    { result->is_binlog_related_ = true; ;}
    break;

  case 43:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_GLOBAL_PORT; ;}
    break;

  case 44:

    {
              result->cur_stmt_type_ = OBPROXY_T_SELECT;
            ;}
    break;

  case 47:

    {
              result->cur_stmt_type_ = OBPROXY_T_LOAD_DATA_INFILE;
            ;}
    break;

  case 48:

    {
              result->cur_stmt_type_ = OBPROXY_T_LOAD_DATA_LOCAL_INFILE;
            ;}
    break;

  case 61:

    { result->cur_stmt_type_ = OBPROXY_T_CREATE; ;}
    break;

  case 62:

    { result->cur_stmt_type_ = OBPROXY_T_DROP; ;}
    break;

  case 63:

    { result->cur_stmt_type_ = OBPROXY_T_ALTER; ;}
    break;

  case 64:

    { result->cur_stmt_type_ = OBPROXY_T_TRUNCATE; ;}
    break;

  case 65:

    { result->cur_stmt_type_ = OBPROXY_T_RENAME; ;}
    break;

  case 66:

    {;}
    break;

  case 67:

    {;}
    break;

  case 69:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_TABLE; ;}
    break;

  case 70:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 71:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 72:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_TABLEGROUP; ;}
    break;

  case 74:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_DROP_TABLEGROUP; ;}
    break;

  case 75:

    {
            SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num));
            result->cur_stmt_type_ = OBPROXY_T_STOP_DDL_TASK;
          ;}
    break;

  case 76:

    {
            SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num));
            result->cur_stmt_type_ = OBPROXY_T_RETRY_DDL_TASK;
          ;}
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

    {;}
    break;

  case 85:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(1) - (1)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 86:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(3) - (3)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 87:

    {
                          ObProxyTextPsParseNode *node = NULL;
                          malloc_parse_node(node);
                          node->str_value_ = (yyvsp[(1) - (1)].str);
                          add_text_ps_node(result->text_ps_parse_info_, node);
                        ;}
    break;

  case 90:

    {
                      result->text_ps_inner_stmt_type_ = OBPROXY_T_TEXT_PS_PREPARE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 91:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (2)].str);
                    ;}
    break;

  case 92:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 93:

    {
            ;}
    break;

  case 94:

    {
            ;}
    break;

  case 95:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 96:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 97:

    { result->cur_stmt_type_ = OBPROXY_T_GRANT; ;}
    break;

  case 98:

    { result->cur_stmt_type_ = OBPROXY_T_REVOKE; ;}
    break;

  case 99:

    { result->cur_stmt_type_ = OBPROXY_T_ANALYZE; ;}
    break;

  case 100:

    { result->cur_stmt_type_ = OBPROXY_T_PURGE; ;}
    break;

  case 101:

    { result->cur_stmt_type_ = OBPROXY_T_FLASHBACK; ;}
    break;

  case 102:

    { result->cur_stmt_type_ = OBPROXY_T_COMMENT; ;}
    break;

  case 103:

    { result->cur_stmt_type_ = OBPROXY_T_AUDIT; ;}
    break;

  case 104:

    { result->cur_stmt_type_ = OBPROXY_T_NOAUDIT; ;}
    break;

  case 107:

    {;}
    break;

  case 108:

    {;}
    break;

  case 109:

    {;}
    break;

  case 115:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_TX_RO; ;}
    break;

  case 119:

    { result->col_name_ = (yyvsp[(3) - (3)].str); ;}
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

    {;}
    break;

  case 126:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY; ;}
    break;

  case 127:

    {;}
    break;

  case 128:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_DATABASE; ;}
    break;

  case 129:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_PROXY_STATUS; ;}
    break;

  case 130:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DATABASES; ;}
    break;

  case 131:

    {;}
    break;

  case 132:

    {;}
    break;

  case 133:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_COLUMNS; ;}
    break;

  case 134:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_INDEX; ;}
    break;

  case 135:

    {;}
    break;

  case 136:

    {
                      result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
                      result->cur_stmt_type_ = OBPROXY_T_DESC;
                      result->sub_stmt_type_ = OBPROXY_T_SUB_DESC_TABLE;
                  ;}
    break;

  case 137:

    {
            result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
          ;}
    break;

  case 138:

    {
            result->table_info_.table_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.database_name_ = (yyvsp[(4) - (4)].str);
          ;}
    break;

  case 139:

    {
            result->table_info_.database_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.table_name_ = (yyvsp[(4) - (4)].str);
          ;}
    break;

  case 140:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE;
                        result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
                      ;}
    break;

  case 141:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE;
                        result->table_info_.database_name_ = (yyvsp[(2) - (4)].str);
                        result->table_info_.table_name_ = (yyvsp[(4) - (4)].str);
                      ;}
    break;

  case 142:

    {;}
    break;

  case 143:

    { result->table_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 144:

    {;}
    break;

  case 145:

    { result->table_info_.database_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 146:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLES;
                ;}
    break;

  case 147:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_FULL_TABLES;
                ;}
    break;

  case 148:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLE_STATUS;
                      ;}
    break;

  case 149:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DB_VERSION; ;}
    break;

  case 150:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID; ;}
    break;

  case 151:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 152:

    {
                     SET_ICMD_SECOND_STRING((yyvsp[(5) - (5)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 153:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (7)].str));
                     SET_ICMD_SECOND_STRING((yyvsp[(7) - (7)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 154:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_ROUTE_ADDR; ;}
    break;

  case 155:

    {
                              result->cur_stmt_type_ = OBPROXY_T_SET_ROUTE_ADDR;
                              result->cmd_info_.integer_[0] = (yyvsp[(3) - (3)].num);
                           ;}
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

  case 161:

    {;}
    break;

  case 163:

    {
                   result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                 ;}
    break;

  case 164:

    {
                   result->table_info_.package_name_ = (yyvsp[(1) - (3)].str);
                   result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                 ;}
    break;

  case 165:

    {
                   result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                   result->table_info_.package_name_ = (yyvsp[(3) - (5)].str);
                   result->table_info_.table_name_ = (yyvsp[(5) - (5)].str);
                 ;}
    break;

  case 166:

    {
                result->call_parse_info_.node_count_ = 0;
              ;}
    break;

  case 167:

    {
                result->call_parse_info_.node_count_ = 0;
                add_call_node(result->call_parse_info_, (yyvsp[(1) - (1)].node));
              ;}
    break;

  case 168:

    {
                add_call_node(result->call_parse_info_, (yyvsp[(3) - (3)].node));
              ;}
    break;

  case 169:

    {
            malloc_call_node((yyval.node), CALL_TOKEN_STR_VAL);
            (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 170:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_INT_VAL);
           (yyval.node)->int_value_ = (yyvsp[(1) - (1)].num);
         ;}
    break;

  case 171:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_NUMBER_VAL);
           (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 172:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_USER_VAR);
           (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 173:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_SYS_VAR);
           (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 174:

    {
           result->placeholder_list_idx_++;
           malloc_call_node((yyval.node), CALL_TOKEN_PLACE_HOLDER);
           (yyval.node)->placeholder_idx_ = result->placeholder_list_idx_ - 1;
         ;}
    break;

  case 188:

    {
                                                                  handle_stmt_end(result);
                                                                  HANDLE_ACCEPT_FINISH();
                                                                ;}
    break;

  case 193:

    {
                                                 handle_stmt_end(result);
                                                 HANDLE_ACCEPT_FINISH();
                                               ;}
    break;

  case 197:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_USER);
        ;}
    break;

  case 198:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 199:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 200:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_SYS);
        ;}
    break;

  case 201:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 202:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 203:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_SYS);
        ;}
    break;

  case 204:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_STR);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 205:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_INT);
               (yyval.var_node)->int_value_ = (yyvsp[(1) - (1)].num);
             ;}
    break;

  case 206:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_NUMBER);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 209:

    {;}
    break;

  case 210:

    { result->has_force_master_hint_ = true; ;}
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

    { yyerrok; yyclearin; ;}
    break;

  case 226:

    {;}
    break;

  case 229:

    { handle_stmt_end(result); HANDLE_ACCEPT_FINISH(); ;}
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

    { result->has_hint_route_info_ = true; result->hint_route_info_.table_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 259:

    { result->has_hint_route_info_ = true; ;}
    break;

  case 260:

    {;}
    break;

  case 262:

    {
                    if (result->hint_route_info_.part_key_info_.node_count_ < OBPROXY_MAX_PART_KEY_PARSE_NUM) {
                      add_set_var_node(result->hint_route_info_.part_key_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_USER);
                    }
                  ;}
    break;

  case 263:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 265:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 297:

    { result->query_timeout_ = (yyvsp[(3) - (4)].num); ;}
    break;

  case 298:

    { result->max_execution_time_ = (yyvsp[(3) - (4)].num); ;}
    break;

  case 300:

    { SET_AP_QUERY_ROUTE_POLICY(OBPROXY_AP_QUERY_ROUTE_POLICY_FORCE); ;}
    break;

  case 301:

    { SET_AP_QUERY_ROUTE_POLICY(OBPROXY_AP_QUERY_ROUTE_POLICY_AUTO); ;}
    break;

  case 302:

    { SET_AP_QUERY_ROUTE_POLICY(OBPROXY_AP_QUERY_ROUTE_POLICY_OFF); ;}
    break;

  case 303:

    { SET_AP_QUERY_ROUTE_POLICY(OBPROXY_AP_QUERY_ROUTE_POLICY_FORCE); ;}
    break;

  case 304:

    { SET_AP_QUERY_ROUTE_POLICY(OBPROXY_AP_QUERY_ROUTE_POLICY_AUTO); ;}
    break;

  case 305:

    { SET_AP_QUERY_ROUTE_POLICY(OBPROXY_AP_QUERY_ROUTE_POLICY_OFF); ;}
    break;

  case 306:

    {
      add_hint_index(result->dbmesh_route_info_, (yyvsp[(3) - (5)].str));
      result->dbmesh_route_info_.index_count_++;
    ;}
    break;

  case 307:

    { result->has_trace_log_hint_ = true; ;}
    break;

  case 311:

    { handle_stmt_end(result); HANDLE_ACCEPT_FINISH(); ;}
    break;

  case 312:

    { yyerrok; yyclearin; ;}
    break;

  case 313:

    {;}
    break;

  case 314:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_WEAK); ;}
    break;

  case 315:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_STRONG); ;}
    break;

  case 316:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_FROZEN); ;}
    break;

  case 319:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_WARNINGS; ;}
    break;

  case 320:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_ERRORS; ;}
    break;

  case 321:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 322:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 323:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_SLAVE_HOSTS; ;}
    break;

  case 324:

    {
            result->is_binlog_related_ = true;
            result->cur_stmt_type_ = OBPROXY_T_SHOW_SLAVE_STATUS;
          ;}
    break;

  case 325:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_RELAYLOG_EVENTS; ;}
    break;

  case 326:

    { result->is_binlog_related_ = true; ;}
    break;

  case 327:

    { result->is_binlog_related_ = true; ;}
    break;

  case 328:

    { result->is_binlog_related_ = true; ;}
    break;

  case 329:

    { result->is_binlog_related_ = true; ;}
    break;

  case 359:

    { result->cur_stmt_type_ = OBPROXY_T_BINLOG_STR; ;}
    break;

  case 360:

    {
    result->cur_stmt_type_ = OBPROXY_T_SHOW_BINLOG_SERVER_FOR_TENANT;
    result->is_binlog_related_ = true;
;}
    break;

  case 361:

    { result->is_binlog_related_ = true; ;}
    break;

  case 362:

    { result->is_binlog_related_ = true; ;}
    break;

  case 363:

    { result->is_binlog_related_ = true; ;}
    break;

  case 364:

    { result->is_binlog_related_ = true; ;}
    break;

  case 365:

    { result->is_cdc_coordinator_related_ = true; ;}
    break;

  case 366:

    { result->is_cdc_coordinator_related_ = true; ;}
    break;

  case 367:

    { result->is_cdc_coordinator_related_ = true; result->is_cdc_coordinator_readonly_ = true; ;}
    break;

  case 368:

    { result->is_cdc_coordinator_related_ = true; result->is_cdc_coordinator_readonly_ = true; ;}
    break;

  case 369:

    { result->is_cdc_coordinator_related_ = true; ;}
    break;

  case 370:

    { result->is_cdc_coordinator_related_ = true; ;}
    break;

  case 371:

    { result->is_cdc_coordinator_related_ = true; ;}
    break;

  case 372:

    { result->is_cdc_coordinator_related_ = true; ;}
    break;

  case 373:

    { result->is_cdc_coordinator_related_ = true; ;}
    break;

  case 374:

    { result->is_cdc_coordinator_related_ = true; ;}
    break;

  case 375:

    { result->is_cdc_coordinator_related_ = true; ;}
    break;

  case 376:

    {
;}
    break;

  case 377:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 378:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 379:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 380:

    {;}
    break;

  case 381:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 382:

    {;}
    break;

  case 383:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 385:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_KV_THREAD); ;}
    break;

  case 386:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_KV_REQUESTSTAT); ;}
    break;

  case 387:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KV_REQUESTSTAT, (yyvsp[(2) - (2)].str)); ;}
    break;

  case 389:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 390:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 391:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 392:

    {;}
    break;

  case 393:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_ALL); ;}
    break;

  case 394:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 395:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 396:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 398:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 399:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 400:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 401:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 402:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 404:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 405:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 406:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 407:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 408:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 409:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 410:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 411:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 412:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 413:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_READ_STALE); ;}
    break;

  case 414:

    {;}
    break;

  case 415:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 416:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_PROXYSM_RPC); ;}
    break;

  case 417:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_PROXYSM_RPC, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 418:

    {;}
    break;

  case 419:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 420:

    {;}
    break;

  case 422:

    {;}
    break;

  case 423:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 424:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 425:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 426:

    {;}
    break;

  case 427:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 428:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 429:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_GLOBALINDEX); ;}
    break;

  case 430:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_TABLEGROUP); ;}
    break;

  case 431:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_QUERYASYNC); ;}
    break;

  case 432:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_TABLETLS); ;}
    break;

  case 433:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_ROUTE_TABLETLS, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 434:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_RPCCTX); ;}
    break;

  case 435:

    {;}
    break;

  case 436:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 437:

    {;}
    break;

  case 438:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 439:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 440:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); SET_ICMD_ONE_ID((yyvsp[(3) - (3)].num)); ;}
    break;

  case 441:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 442:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 444:

    {;}
    break;

  case 445:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 446:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 447:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 448:

    {;}
    break;

  case 449:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 451:

    {;}
    break;

  case 452:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 453:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 454:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 455:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 456:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 457:

    {;}
    break;

  case 458:

    {;}
    break;

  case 459:

    {;}
    break;

  case 460:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_PS_ALL);;}
    break;

  case 461:

    {;}
    break;

  case 462:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 463:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 464:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 465:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 466:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 467:

    {;}
    break;

  case 468:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 469:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 470:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 471:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 472:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 473:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 474:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 477:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 478:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 479:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 480:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 487:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 488:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 490:

    {;}
    break;

  case 491:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 492:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 493:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 494:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 495:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT_FINISH();
                                                ;}
    break;

  case 496:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT_FINISH();
                                                ;}
    break;

  case 497:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 498:

    {
                                                  result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                  result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                                  result->table_info_.dblink_name_ = (yyvsp[(4) - (4)].str);
                                                 ;}
    break;

  case 499:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 500:

    {
                                      result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                      result->table_info_.dblink_name_ = (yyvsp[(2) - (2)].str);
                                    ;}
    break;

  case 501:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 502:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 503:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 504:

    {
                                                  UPDATE_ALIAS_NAME((yyvsp[(5) - (5)].str));
                                                  result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                                                  result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                                                ;}
    break;

  case 505:

    { result->table_info_.join_table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 506:

    {
            result->table_info_.join_database_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.join_table_name_ = (yyvsp[(4) - (4)].str);
          ;}
    break;

  case 507:

    {
            result->table_info_.join_table_name_ = (yyvsp[(2) - (3)].str);
            result->table_info_.join_table_alias_name_ = (yyvsp[(3) - (3)].str);
         ;}
    break;

  case 508:

    {
            result->table_info_.join_table_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.join_table_alias_name_ = (yyvsp[(4) - (4)].str);
         ;}
    break;

  case 509:

    {
            result->table_info_.join_database_name_ = (yyvsp[(2) - (5)].str);
            result->table_info_.join_table_name_ = (yyvsp[(4) - (5)].str);
            result->table_info_.join_table_alias_name_ = (yyvsp[(5) - (5)].str);
         ;}
    break;

  case 510:

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

