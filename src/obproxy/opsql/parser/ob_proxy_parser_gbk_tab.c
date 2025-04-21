
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
     LOAD_DATA_HINT_BEGIN = 294,
     HINT_END = 295,
     COMMENT_BEGIN = 296,
     COMMENT_END = 297,
     ROUTE_TABLE = 298,
     ROUTE_PART_KEY = 299,
     PLACE_HOLDER = 300,
     END_P = 301,
     ERROR = 302,
     WHEN = 303,
     TABLEGROUP = 304,
     FLASHBACK = 305,
     AUDIT = 306,
     NOAUDIT = 307,
     STATUS = 308,
     BEGI = 309,
     START = 310,
     TRANSACTION = 311,
     READ = 312,
     ONLY = 313,
     WITH = 314,
     CONSISTENT = 315,
     SNAPSHOT = 316,
     INDEX = 317,
     XA = 318,
     GLOBALINDEX = 319,
     WARNINGS = 320,
     ERRORS = 321,
     TRACE = 322,
     QUICK = 323,
     COUNT = 324,
     AS = 325,
     WHERE = 326,
     VALUES = 327,
     ORDER = 328,
     GROUP = 329,
     HAVING = 330,
     INTO = 331,
     UNION = 332,
     FOR = 333,
     TX_READ_ONLY = 334,
     SELECT_OBPROXY_ROUTE_ADDR = 335,
     SET_OBPROXY_ROUTE_ADDR = 336,
     NAME_OB_DOT = 337,
     NAME_OB = 338,
     EXPLAIN = 339,
     EXPLAIN_ROUTE = 340,
     DESC = 341,
     DESCRIBE = 342,
     NAME_STR = 343,
     USER_VARIABLE = 344,
     SYSTEM_VARIABLE = 345,
     LOAD = 346,
     DATA = 347,
     LOCAL = 348,
     INFILE = 349,
     SLAVE = 350,
     RELAYLOG = 351,
     EVENTS = 352,
     HOSTS = 353,
     BINLOG = 354,
     PORT = 355,
     USE = 356,
     HELP = 357,
     SET_NAMES = 358,
     SET_CHARSET = 359,
     SET_PASSWORD = 360,
     SET_DEFAULT = 361,
     SET_OB_READ_CONSISTENCY = 362,
     SET_TX_READ_ONLY = 363,
     GLOBAL = 364,
     SESSION = 365,
     GLOBAL_ALIAS = 366,
     SESSION_ALIAS = 367,
     MASTER = 368,
     LOGS = 369,
     RESET = 370,
     FLUSH = 371,
     SERVER = 372,
     TENANT = 373,
     NUMBER_VAL = 374,
     GROUP_ID = 375,
     TABLE_ID = 376,
     ELASTIC_ID = 377,
     TESTLOAD = 378,
     ODP_COMMENT = 379,
     TNT_ID = 380,
     DISASTER_STATUS = 381,
     TRACE_ID = 382,
     RPC_ID = 383,
     TARGET_DB_SERVER = 384,
     TRACE_LOG = 385,
     DBP_COMMENT = 386,
     ROUTE_TAG = 387,
     SYS_TAG = 388,
     TABLE_NAME = 389,
     SCAN_ALL = 390,
     STICKY_SESSION = 391,
     PARALL = 392,
     SHARD_KEY = 393,
     STOP_DDL_TASK = 394,
     RETRY_DDL_TASK = 395,
     QUERY_TIMEOUT = 396,
     READ_CONSISTENCY = 397,
     WEAK = 398,
     STRONG = 399,
     FROZEN = 400,
     INT_NUM = 401,
     SHOW_PROXYNET = 402,
     THREAD = 403,
     CONNECTION = 404,
     LIMIT = 405,
     OFFSET = 406,
     SHOW_PROCESSLIST = 407,
     SHOW_PROXYSESSION = 408,
     SHOW_GLOBALSESSION = 409,
     ATTRIBUTE = 410,
     VARIABLES = 411,
     ALL = 412,
     STAT = 413,
     READ_STALE = 414,
     SHOW_PROXYCONFIG = 415,
     DIFF = 416,
     USER = 417,
     LIKE = 418,
     SHOW_PROXYSM = 419,
     SHOW_PROXYKV = 420,
     SHOW_PROXYCLUSTER = 421,
     SHOW_PROXYRESOURCE = 422,
     SHOW_PROXYCONGESTION = 423,
     SHOW_PROXYROUTE = 424,
     PARTITION = 425,
     ROUTINE = 426,
     SUBPARTITION = 427,
     SHOW_PROXYVIP = 428,
     SHOW_PROXYMEMORY = 429,
     OBJPOOL = 430,
     SHOW_SQLAUDIT = 431,
     SHOW_WARNLOG = 432,
     SHOW_PROXYSTAT = 433,
     REFRESH = 434,
     SHOW_PROXYTRACE = 435,
     SHOW_PROXYINFO = 436,
     BINARY = 437,
     UPGRADE = 438,
     IDC = 439,
     SHOW_PROXYPS = 440,
     DETAIL = 441,
     SHOW_ELASTIC_ID = 442,
     SHOW_TOPOLOGY = 443,
     GROUP_NAME = 444,
     SHOW_DB_VERSION = 445,
     SHOW_DATABASES = 446,
     SHOW_TABLES = 447,
     SHOW_FULL_TABLES = 448,
     SELECT_DATABASE = 449,
     SELECT_PROXY_STATUS = 450,
     SHOW_CREATE_TABLE = 451,
     SELECT_PROXY_VERSION = 452,
     SHOW_COLUMNS = 453,
     SHOW_INDEX = 454,
     ALTER_PROXYCONFIG = 455,
     ALTER_PROXYRESOURCE = 456,
     PING_PROXY = 457,
     KILL_PROXYSESSION = 458,
     KILL_GLOBALSESSION = 459,
     KILL = 460,
     QUERY = 461,
     BINLOG_VARIABLE = 462,
     BINLOG_USER_VAR = 463,
     BINLOG_SYS_VAR = 464
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
#define YYFINAL  384
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   3687

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  221
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  161
/* YYNRULES -- Number of rules.  */
#define YYNRULES  526
/* YYNRULES -- Number of states.  */
#define YYNSTATES  850

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   464

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,   218,     2,     2,     2,     2,
     214,   215,   220,     2,   211,     2,   212,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   210,
       2,   213,     2,     2,   219,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   216,     2,   217,     2,     2,     2,     2,
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
     205,   206,   207,   208,   209
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
     639,   648,   657,   664,   665,   668,   670,   674,   676,   681,
     686,   690,   697,   701,   706,   711,   718,   722,   724,   728,
     729,   733,   737,   741,   745,   749,   753,   757,   761,   765,
     769,   771,   775,   781,   785,   790,   795,   799,   801,   805,
     806,   808,   809,   811,   813,   815,   818,   821,   825,   830,
     832,   835,   837,   840,   842,   845,   848,   852,   853,   855,
     858,   860,   863,   865,   868,   871,   874,   877,   878,   881,
     883,   885,   886,   889,   894,   899,   905,   907,   912,   914,
     916,   917,   919,   921,   923,   924,   926,   930,   934,   937,
     943,   947,   951,   955,   959,   963,   967,   971,   973,   975,
     977,   979,   981,   983,   985,   987,   989,   991,   993,   995,
     997,   999,  1001,  1003,  1005,  1007,  1009,  1011,  1013,  1015,
    1017,  1019,  1021,  1022,  1024,  1026,  1028,  1031,  1037,  1043,
    1046,  1050,  1054,  1055,  1058,  1063,  1068,  1069,  1072,  1073,
    1076,  1079,  1081,  1084,  1086,  1088,  1092,  1095,  1099,  1103,
    1108,  1110,  1113,  1114,  1117,  1121,  1124,  1127,  1130,  1131,
    1134,  1138,  1141,  1145,  1148,  1152,  1156,  1161,  1164,  1166,
    1169,  1172,  1176,  1179,  1182,  1183,  1185,  1187,  1190,  1193,
    1197,  1200,  1203,  1205,  1208,  1210,  1213,  1216,  1220,  1223,
    1226,  1229,  1230,  1232,  1236,  1242,  1245,  1249,  1252,  1253,
    1255,  1258,  1261,  1264,  1267,  1272,  1279,  1280,  1282,  1283,
    1285,  1290,  1296,  1302,  1306,  1308,  1311,  1315,  1319,  1322,
    1325,  1329,  1333,  1334,  1337,  1339,  1343,  1347,  1351,  1352,
    1354,  1356,  1360,  1363,  1367,  1370,  1373,  1375,  1376,  1379,
    1384,  1387,  1392,  1395,  1399,  1401,  1406,  1410,  1413,  1416,
    1421,  1425,  1431,  1434,  1439,  1443,  1448,  1454,  1461,  1463,
    1465,  1467,  1469,  1471,  1473,  1475,  1477,  1479,  1481,  1483,
    1485,  1487,  1489,  1491,  1493,  1495,  1497,  1499,  1501,  1503,
    1505,  1507,  1509,  1511,  1513,  1515,  1517,  1519,  1521,  1523,
    1525,  1527,  1529,  1531,  1533,  1535,  1537,  1539,  1541,  1543,
    1545,  1547,  1549,  1551,  1553,  1555,  1557
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     222,     0,    -1,   223,    -1,     1,    -1,   224,    -1,   223,
     224,    -1,   225,    46,    -1,   225,   210,    -1,   225,   210,
      46,    -1,   210,    -1,   210,    46,    -1,    54,   225,   210,
      -1,   226,    -1,   294,   226,    -1,   227,    -1,   285,    -1,
     290,    -1,   286,    -1,   287,    -1,   288,    -1,   234,    -1,
     233,    -1,   369,    -1,   327,    -1,   256,    -1,   328,    -1,
     373,    -1,   374,    -1,   268,    -1,   269,    -1,   270,    -1,
     271,    -1,   272,    -1,   273,    -1,   274,    -1,   235,    -1,
     247,    -1,   289,    -1,   330,    -1,   232,    -1,   375,    -1,
     310,    -1,   311,    -1,   312,   253,   252,    -1,    -1,     9,
      -1,    94,    83,   228,    19,   378,    -1,    93,    94,    83,
     228,    19,   378,    -1,   230,    -1,   229,    -1,   319,   231,
      -1,   249,   227,    -1,   249,   285,    -1,   249,   287,    -1,
     249,   288,    -1,   249,   286,    -1,   249,   289,    -1,   251,
     226,    -1,   236,    -1,   248,    -1,    14,   237,    -1,    15,
     238,    -1,    16,    -1,    17,    -1,    18,    -1,   239,    -1,
     240,    -1,    -1,    19,    -1,    62,    -1,    20,    62,    -1,
      49,    -1,    -1,    49,    -1,   139,   146,    -1,   140,   146,
      -1,   227,    -1,   285,    -1,   286,    -1,   288,    -1,   287,
      -1,   375,    -1,   274,    -1,   289,    -1,    89,    -1,   242,
     211,    89,    -1,    89,    -1,   243,    -1,   241,    -1,    29,
     381,    26,    -1,    30,   381,    -1,    30,   381,    31,    -1,
     245,   244,    -1,   246,   242,    -1,    15,    29,   381,    -1,
      32,    29,   381,    -1,    21,    -1,    22,    -1,    23,    -1,
      24,    -1,    50,    -1,    25,    -1,    51,    -1,    52,    -1,
     250,    -1,   250,    83,    -1,    84,    -1,    86,    -1,    87,
      -1,    85,    -1,    -1,    26,   281,    -1,    -1,   278,    -1,
       5,    79,    -1,     5,    79,   253,    26,   281,    -1,     5,
      79,   278,    -1,   197,    -1,   197,    70,   381,    -1,   254,
      -1,   255,    -1,   266,    -1,   267,    -1,   257,    -1,   265,
      -1,   188,   258,    -1,   264,    -1,   194,    -1,   195,    -1,
     191,    -1,   262,    -1,   263,    -1,   198,   258,    -1,   199,
     258,    -1,   259,    -1,   250,   381,    -1,    26,   381,    -1,
      26,   381,    26,   381,    -1,    26,   381,   212,   381,    -1,
     196,    83,    -1,   196,    83,   212,    83,    -1,    -1,   163,
      83,    -1,    -1,    26,    83,    -1,   192,   261,   260,    -1,
     193,   261,   260,    -1,    11,    19,    53,   261,   260,    -1,
     190,    -1,   187,    -1,   187,    26,    83,    -1,   187,    71,
     189,   213,    83,    -1,   187,    26,    83,    71,   189,   213,
      83,    -1,    80,    -1,    81,   213,   146,    -1,   103,    -1,
     104,    -1,   105,    -1,   106,    -1,   107,    -1,   108,    -1,
      13,   275,   214,   276,   215,    -1,   381,    -1,   381,   212,
     381,    -1,   381,   212,   381,   212,   381,    -1,    -1,   277,
      -1,   276,   211,   277,    -1,    83,    -1,   146,    -1,   119,
      -1,    89,    -1,    90,    -1,    45,    -1,   279,    -1,   278,
     279,    -1,   280,    -1,   214,   215,    -1,   214,   227,   215,
      -1,   214,   278,   215,    -1,   377,    -1,   282,    -1,   227,
      -1,    -1,   214,   284,   215,    -1,   381,    -1,   284,   211,
     381,    -1,   315,   378,   376,    -1,   315,   378,   376,   283,
     282,    -1,   317,   281,    -1,   313,   281,    -1,   314,   326,
      26,   281,    -1,   318,   378,    -1,    12,   291,    -1,   292,
     211,   291,    -1,   292,    -1,    89,   213,   293,    -1,   111,
     381,   213,   293,    -1,   109,   381,   213,   293,    -1,    90,
     213,   293,    -1,   112,   381,   213,   293,    -1,   110,   381,
     213,   293,    -1,   381,   213,   293,    -1,   381,    -1,   146,
      -1,   119,    -1,   295,    -1,   295,   294,    -1,    41,   296,
      42,    -1,    41,   124,   304,   303,    42,    -1,    41,   121,
     213,   309,   303,    42,    -1,    41,   134,   213,   309,   303,
      42,    -1,    41,   120,   213,   309,   303,    42,    -1,    41,
     122,   213,   309,   303,    42,    -1,    41,   123,   213,   309,
     303,    42,    -1,    41,    82,    83,   213,   308,   303,    42,
      -1,    41,   127,   213,   307,   303,    42,    -1,    41,   128,
     213,   307,   303,    42,    -1,    41,   125,   213,   309,   303,
      42,    -1,    41,   126,   213,   309,   303,    42,    -1,    41,
     131,   132,   213,   216,   298,   217,    42,    -1,    41,   131,
     133,   213,   216,   300,   217,    42,    -1,    41,   129,   213,
     309,   303,    42,    -1,    -1,   296,   297,    -1,   381,    -1,
     299,   211,   298,    -1,   299,    -1,   120,   214,   309,   215,
      -1,   134,   214,   309,   215,    -1,   135,   214,   215,    -1,
     135,   214,   137,   213,   309,   215,    -1,   136,   214,   215,
      -1,   138,   214,   301,   215,    -1,    67,   214,   307,   215,
      -1,    67,   214,   307,   218,   307,   215,    -1,   302,   211,
     301,    -1,   302,    -1,    83,   213,   309,    -1,    -1,   303,
     211,   304,    -1,   120,   213,   309,    -1,   121,   213,   309,
      -1,   134,   213,   309,    -1,   122,   213,   309,    -1,   123,
     213,   309,    -1,   127,   213,   307,    -1,   128,   213,   307,
      -1,   125,   213,   309,    -1,   126,   213,   309,    -1,   130,
      -1,   129,   213,   309,    -1,    83,   212,    83,   213,   308,
      -1,    83,   213,   308,    -1,    43,   214,   381,   215,    -1,
      44,   214,   305,   215,    -1,   306,   211,   305,    -1,   306,
      -1,   381,   213,   293,    -1,    -1,   309,    -1,    -1,   309,
      -1,   381,    -1,    88,    -1,     5,   208,    -1,     5,   209,
      -1,     5,   111,   100,    -1,     5,   219,   219,   100,    -1,
       5,    -1,    33,   320,    -1,     8,    -1,    34,   320,    -1,
       6,    -1,    35,   320,    -1,     7,   316,    -1,    36,   320,
     316,    -1,    -1,   157,    -1,   157,    48,    -1,     9,    -1,
      37,   320,    -1,    10,    -1,    38,   320,    -1,    91,    92,
      -1,    39,   320,    -1,   321,    40,    -1,    -1,   324,   321,
      -1,   146,    -1,   381,    -1,    -1,   322,   323,    -1,   141,
     214,   146,   215,    -1,   142,   214,   325,   215,    -1,    62,
     214,   381,   381,   215,    -1,   130,    -1,   381,   214,   323,
     215,    -1,   381,    -1,   146,    -1,    -1,   143,    -1,   144,
      -1,   145,    -1,    -1,    68,    -1,    11,   368,    65,    -1,
      11,   368,    66,    -1,    11,    67,    -1,    11,    67,    83,
     213,    83,    -1,    11,    95,    98,    -1,    11,    95,    53,
      -1,    11,    96,    97,    -1,    11,   113,    53,    -1,    11,
     182,   114,    -1,    11,    99,    97,    -1,    11,   113,   114,
      -1,   336,    -1,   338,    -1,   339,    -1,   342,    -1,   340,
      -1,   344,    -1,   345,    -1,   346,    -1,   347,    -1,   349,
      -1,   350,    -1,   351,    -1,   352,    -1,   353,    -1,   355,
      -1,   356,    -1,   358,    -1,   334,    -1,   359,    -1,   362,
      -1,   363,    -1,   364,    -1,   365,    -1,   366,    -1,   367,
      -1,    -1,   109,    -1,   110,    -1,    93,    -1,    99,   381,
      -1,    11,    99,   117,    78,   118,    -1,    11,   329,   156,
     163,   207,    -1,   115,   113,    -1,    24,   182,   114,    -1,
     116,   182,   114,    -1,    -1,   150,   146,    -1,   150,   146,
     211,   146,    -1,   150,   146,   151,   146,    -1,    -1,   163,
      83,    -1,    -1,   163,    83,    -1,   165,   335,    -1,   148,
      -1,   147,   337,    -1,   148,    -1,   149,    -1,   149,   146,
     331,    -1,   160,   332,    -1,   160,   157,   332,    -1,   160,
     161,   332,    -1,   160,   161,   162,   332,    -1,   152,    -1,
     154,   341,    -1,    -1,   155,    83,    -1,   155,   163,    83,
      -1,   155,   157,    -1,   163,    83,    -1,   153,   343,    -1,
      -1,   155,   332,    -1,   155,   146,   332,    -1,   158,   332,
      -1,   158,   146,   332,    -1,   156,   332,    -1,   156,   146,
     332,    -1,   156,   157,   332,    -1,   156,   157,   146,   332,
      -1,   159,   332,    -1,   164,    -1,   164,   146,    -1,   166,
     332,    -1,   166,   184,   332,    -1,   167,   332,    -1,   168,
     348,    -1,    -1,    83,    -1,   157,    -1,   157,    83,    -1,
     169,   333,    -1,   169,   171,   333,    -1,   169,   170,    -1,
     169,    64,    -1,   173,    -1,   173,    83,    -1,   174,    -1,
     174,   146,    -1,   174,   175,    -1,   174,   175,   146,    -1,
     176,   331,    -1,   176,   146,    -1,   177,   354,    -1,    -1,
     146,    -1,   146,   211,   146,    -1,   146,   211,   146,   211,
      83,    -1,   178,   332,    -1,   178,   179,   332,    -1,   180,
     357,    -1,    -1,   146,    -1,   146,   146,    -1,   181,   182,
      -1,   181,   183,    -1,   181,   184,    -1,   185,   361,   332,
     360,    -1,   185,   361,   332,   118,   333,   360,    -1,    -1,
     186,    -1,    -1,   146,    -1,   200,    12,    83,   213,    -1,
     200,    12,    83,   213,    83,    -1,   200,    12,    83,   213,
     146,    -1,   201,     6,    83,    -1,   202,    -1,   203,   146,
      -1,   203,   146,   146,    -1,   204,    83,   146,    -1,   204,
      83,    -1,   205,   146,    -1,   205,   149,   146,    -1,   205,
     206,   146,    -1,    -1,    69,   220,    -1,    54,    -1,    55,
      56,   370,    -1,    63,    54,    83,    -1,    63,    55,    83,
      -1,    -1,   371,    -1,   372,    -1,   371,   211,   372,    -1,
      57,    58,    -1,    59,    60,    61,    -1,   101,   381,    -1,
     102,    83,    -1,    83,    -1,    -1,   172,   381,    -1,   172,
     214,   381,   215,    -1,   170,   381,    -1,   170,   214,   381,
     215,    -1,   378,   376,    -1,   378,   376,   379,    -1,   381,
      -1,   381,   212,   381,    89,    -1,   381,   212,   381,    -1,
     381,    89,    -1,   381,   381,    -1,   381,   212,   381,   381,
      -1,   381,    70,   381,    -1,   381,   212,   381,    70,   381,
      -1,    28,   381,    -1,    28,   381,   212,   381,    -1,    28,
     381,   381,    -1,    28,   381,    70,   381,    -1,    28,   381,
     212,   381,   381,    -1,    28,   381,   212,   381,    70,   381,
      -1,    55,    -1,    63,    -1,    54,    -1,    56,    -1,    60,
      -1,    66,    -1,    65,    -1,    69,    -1,    68,    -1,    67,
      -1,   148,    -1,   149,    -1,   151,    -1,   155,    -1,   156,
      -1,   158,    -1,   161,    -1,   162,    -1,   175,    -1,   179,
      -1,   183,    -1,   184,    -1,   206,    -1,   189,    -1,    50,
      -1,    51,    -1,    52,    -1,    93,    -1,    92,    -1,    53,
      -1,   143,    -1,   144,    -1,   145,    -1,   109,    -1,   110,
      -1,    98,    -1,    97,    -1,    96,    -1,    99,    -1,   100,
      -1,   113,    -1,   114,    -1,   115,    -1,   116,    -1,   117,
      -1,   118,    -1,   186,    -1,    83,    -1,   380,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   341,   341,   342,   344,   345,   347,   348,   349,   350,
     351,   352,   354,   355,   357,   358,   359,   360,   361,   362,
     363,   364,   365,   366,   367,   368,   369,   370,   371,   372,
     373,   374,   375,   376,   377,   378,   379,   380,   381,   382,
     383,   385,   386,   387,   392,   394,   396,   401,   406,   407,
     409,   411,   412,   413,   414,   415,   416,   418,   420,   421,
     423,   424,   425,   426,   427,   428,   429,   431,   432,   433,
     434,   435,   437,   438,   440,   446,   452,   453,   454,   455,
     456,   457,   458,   459,   461,   468,   476,   484,   485,   488,
     494,   499,   505,   508,   511,   516,   522,   523,   524,   525,
     526,   527,   528,   529,   531,   532,   534,   535,   536,   538,
     540,   541,   543,   544,   546,   547,   548,   550,   551,   553,
     554,   555,   556,   557,   559,   560,   561,   562,   563,   564,
     565,   566,   567,   568,   569,   570,   577,   581,   586,   592,
     597,   604,   605,   607,   608,   610,   614,   619,   624,   626,
     627,   632,   637,   644,   647,   653,   654,   655,   656,   657,
     658,   661,   663,   667,   672,   680,   683,   688,   693,   698,
     703,   708,   713,   718,   726,   727,   729,   731,   732,   733,
     735,   736,   738,   740,   741,   743,   744,   746,   750,   751,
     752,   753,   754,   759,   761,   762,   764,   768,   772,   776,
     780,   784,   788,   792,   797,   802,   808,   809,   811,   812,
     813,   814,   815,   816,   817,   818,   824,   825,   826,   827,
     828,   829,   830,   832,   833,   835,   837,   838,   840,   845,
     850,   851,   852,   853,   855,   856,   858,   859,   861,   869,
     870,   872,   873,   874,   875,   876,   877,   878,   879,   880,
     881,   882,   883,   889,   890,   891,   893,   894,   896,   902,
     903,   905,   906,   908,   909,   911,   912,   914,   915,   917,
     918,   919,   920,   921,   922,   923,   924,   926,   927,   928,
     930,   931,   932,   933,   934,   935,   937,   938,   939,   941,
     942,   944,   945,   947,   948,   949,   954,   955,   956,   957,
     959,   960,   961,   962,   964,   965,   968,   969,   970,   971,
     972,   973,   978,   979,   980,   981,   982,   986,   987,   988,
     989,   990,   991,   992,   993,   994,   995,   996,   997,   998,
     999,  1000,  1001,  1002,  1003,  1004,  1005,  1006,  1007,  1008,
    1009,  1010,  1012,  1013,  1014,  1015,  1018,  1019,  1024,  1025,
    1026,  1027,  1032,  1034,  1038,  1043,  1051,  1052,  1056,  1057,
    1060,  1062,  1065,  1067,  1068,  1069,  1073,  1074,  1075,  1076,
    1081,  1083,  1085,  1086,  1087,  1088,  1089,  1092,  1094,  1095,
    1096,  1097,  1098,  1099,  1100,  1101,  1102,  1103,  1107,  1108,
    1112,  1113,  1118,  1121,  1123,  1124,  1125,  1126,  1130,  1131,
    1132,  1133,  1137,  1138,  1142,  1143,  1144,  1145,  1149,  1150,
    1153,  1155,  1156,  1157,  1158,  1162,  1163,  1166,  1168,  1169,
    1170,  1174,  1175,  1176,  1179,  1180,  1183,  1184,  1187,  1188,
    1192,  1193,  1194,  1198,  1202,  1206,  1207,  1211,  1212,  1216,
    1217,  1218,  1221,  1222,  1225,  1229,  1230,  1231,  1233,  1234,
    1236,  1237,  1240,  1241,  1244,  1250,  1253,  1255,  1256,  1257,
    1258,  1259,  1261,  1265,  1271,  1274,  1279,  1283,  1287,  1291,
    1296,  1300,  1306,  1307,  1312,  1317,  1322,  1328,  1335,  1336,
    1337,  1338,  1339,  1340,  1341,  1342,  1343,  1344,  1345,  1346,
    1347,  1348,  1349,  1350,  1351,  1352,  1353,  1354,  1355,  1356,
    1357,  1358,  1359,  1360,  1361,  1362,  1363,  1364,  1365,  1366,
    1367,  1368,  1369,  1370,  1371,  1372,  1373,  1374,  1375,  1376,
    1377,  1378,  1379,  1380,  1381,  1383,  1384
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
  "COMMENT", "FROM", "DUAL", "JOIN", "PREPARE", "EXECUTE", "USING",
  "DEALLOCATE", "SELECT_HINT_BEGIN", "UPDATE_HINT_BEGIN",
  "DELETE_HINT_BEGIN", "INSERT_HINT_BEGIN", "REPLACE_HINT_BEGIN",
  "MERGE_HINT_BEGIN", "LOAD_DATA_HINT_BEGIN", "HINT_END", "COMMENT_BEGIN",
  "COMMENT_END", "ROUTE_TABLE", "ROUTE_PART_KEY", "PLACE_HOLDER", "END_P",
  "ERROR", "WHEN", "TABLEGROUP", "FLASHBACK", "AUDIT", "NOAUDIT", "STATUS",
  "BEGI", "START", "TRANSACTION", "READ", "ONLY", "WITH", "CONSISTENT",
  "SNAPSHOT", "INDEX", "XA", "GLOBALINDEX", "WARNINGS", "ERRORS", "TRACE",
  "QUICK", "COUNT", "AS", "WHERE", "VALUES", "ORDER", "GROUP", "HAVING",
  "INTO", "UNION", "FOR", "TX_READ_ONLY", "SELECT_OBPROXY_ROUTE_ADDR",
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
  "SHOW_PROXYCONFIG", "DIFF", "USER", "LIKE", "SHOW_PROXYSM",
  "SHOW_PROXYKV", "SHOW_PROXYCLUSTER", "SHOW_PROXYRESOURCE",
  "SHOW_PROXYCONGESTION", "SHOW_PROXYROUTE", "PARTITION", "ROUTINE",
  "SUBPARTITION", "SHOW_PROXYVIP", "SHOW_PROXYMEMORY", "OBJPOOL",
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
  "opt_large_like", "show_proxykv", "opt_show_kv", "show_proxynet",
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
  "table_references", "table_factor", "join_expr", "non_reserved_keyword",
  "var_name", 0
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
      59,    44,    46,    61,    40,    41,   123,   125,    35,    64,
      42
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   221,   222,   222,   223,   223,   224,   224,   224,   224,
     224,   224,   225,   225,   226,   226,   226,   226,   226,   226,
     226,   226,   226,   226,   226,   226,   226,   226,   226,   226,
     226,   226,   226,   226,   226,   226,   226,   226,   226,   226,
     226,   227,   227,   227,   228,   228,   229,   230,   231,   231,
     232,   233,   233,   233,   233,   233,   233,   234,   235,   235,
     236,   236,   236,   236,   236,   236,   236,   237,   237,   237,
     237,   237,   238,   238,   239,   240,   241,   241,   241,   241,
     241,   241,   241,   241,   242,   242,   243,   244,   244,   245,
     246,   246,   247,   247,   247,   247,   248,   248,   248,   248,
     248,   248,   248,   248,   249,   249,   250,   250,   250,   251,
     252,   252,   253,   253,   254,   254,   254,   255,   255,   256,
     256,   256,   256,   256,   257,   257,   257,   257,   257,   257,
     257,   257,   257,   257,   257,   257,   258,   258,   258,   259,
     259,   260,   260,   261,   261,   262,   262,   263,   264,   265,
     265,   265,   265,   266,   267,   268,   269,   270,   271,   272,
     273,   274,   275,   275,   275,   276,   276,   276,   277,   277,
     277,   277,   277,   277,   278,   278,   279,   280,   280,   280,
     281,   281,   282,   283,   283,   284,   284,   285,   285,   286,
     287,   288,   289,   290,   291,   291,   292,   292,   292,   292,
     292,   292,   292,   293,   293,   293,   294,   294,   295,   295,
     295,   295,   295,   295,   295,   295,   295,   295,   295,   295,
     295,   295,   295,   296,   296,   297,   298,   298,   299,   299,
     299,   299,   299,   299,   300,   300,   301,   301,   302,   303,
     303,   304,   304,   304,   304,   304,   304,   304,   304,   304,
     304,   304,   304,   304,   304,   304,   305,   305,   306,   307,
     307,   308,   308,   309,   309,   310,   310,   311,   311,   312,
     312,   313,   313,   314,   314,   315,   315,   316,   316,   316,
     317,   317,   318,   318,   319,   319,   320,   321,   321,   322,
     322,   323,   323,   324,   324,   324,   324,   324,   324,   324,
     325,   325,   325,   325,   326,   326,   327,   327,   327,   327,
     327,   327,   327,   327,   327,   327,   327,   328,   328,   328,
     328,   328,   328,   328,   328,   328,   328,   328,   328,   328,
     328,   328,   328,   328,   328,   328,   328,   328,   328,   328,
     328,   328,   329,   329,   329,   329,   330,   330,   330,   330,
     330,   330,   331,   331,   331,   331,   332,   332,   333,   333,
     334,   335,   336,   337,   337,   337,   338,   338,   338,   338,
     339,   340,   341,   341,   341,   341,   341,   342,   343,   343,
     343,   343,   343,   343,   343,   343,   343,   343,   344,   344,
     345,   345,   346,   347,   348,   348,   348,   348,   349,   349,
     349,   349,   350,   350,   351,   351,   351,   351,   352,   352,
     353,   354,   354,   354,   354,   355,   355,   356,   357,   357,
     357,   358,   358,   358,   359,   359,   360,   360,   361,   361,
     362,   362,   362,   363,   364,   365,   365,   366,   366,   367,
     367,   367,   368,   368,   369,   369,   369,   369,   370,   370,
     371,   371,   372,   372,   373,   374,   375,   376,   376,   376,
     376,   376,   377,   377,   378,   378,   378,   378,   378,   378,
     378,   378,   379,   379,   379,   379,   379,   379,   380,   380,
     380,   380,   380,   380,   380,   380,   380,   380,   380,   380,
     380,   380,   380,   380,   380,   380,   380,   380,   380,   380,
     380,   380,   380,   380,   380,   380,   380,   380,   380,   380,
     380,   380,   380,   380,   380,   380,   380,   380,   380,   380,
     380,   380,   380,   380,   380,   381,   381
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
       8,     8,     6,     0,     2,     1,     3,     1,     4,     4,
       3,     6,     3,     4,     4,     6,     3,     1,     3,     0,
       3,     3,     3,     3,     3,     3,     3,     3,     3,     3,
       1,     3,     5,     3,     4,     4,     3,     1,     3,     0,
       1,     0,     1,     1,     1,     2,     2,     3,     4,     1,
       2,     1,     2,     1,     2,     2,     3,     0,     1,     2,
       1,     2,     1,     2,     2,     2,     2,     0,     2,     1,
       1,     0,     2,     4,     4,     5,     1,     4,     1,     1,
       0,     1,     1,     1,     0,     1,     3,     3,     2,     5,
       3,     3,     3,     3,     3,     3,     3,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     0,     1,     1,     1,     2,     5,     5,     2,
       3,     3,     0,     2,     4,     4,     0,     2,     0,     2,
       2,     1,     2,     1,     1,     3,     2,     3,     3,     4,
       1,     2,     0,     2,     3,     2,     2,     2,     0,     2,
       3,     2,     3,     2,     3,     3,     4,     2,     1,     2,
       2,     3,     2,     2,     0,     1,     1,     2,     2,     3,
       2,     2,     1,     2,     1,     2,     2,     3,     2,     2,
       2,     0,     1,     3,     5,     2,     3,     2,     0,     1,
       2,     2,     2,     2,     4,     6,     0,     1,     0,     1,
       4,     5,     5,     3,     1,     2,     3,     3,     2,     2,
       3,     3,     0,     2,     1,     3,     3,     3,     0,     1,
       1,     3,     2,     3,     2,     2,     1,     0,     2,     4,
       2,     4,     2,     3,     1,     4,     3,     2,     2,     4,
       3,     5,     2,     4,     3,     4,     5,     6,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
       0,     3,   269,   273,   277,   271,   280,   282,   442,     0,
       0,    67,    72,    62,    63,    64,    96,    97,    98,    99,
     101,     0,     0,     0,   287,   287,   287,   287,   287,   287,
     287,   223,   100,   102,   103,   444,     0,     0,   153,     0,
     456,   106,   109,   107,   108,     0,     0,     0,     0,   155,
     156,   157,   158,   159,   160,     0,     0,     0,     0,     0,
     370,   378,   372,   356,   388,     0,   356,   356,   394,   358,
     402,   404,   352,   411,   356,   418,     0,   428,   149,     0,
     148,   129,   143,   143,   127,   128,     0,   117,     0,     0,
       0,     0,   434,     0,     0,     0,     9,     0,     2,     4,
       0,    12,    14,    39,    21,    20,    35,    58,    65,    66,
       0,     0,    36,    59,     0,   104,     0,   119,   120,    24,
     123,   134,   130,   131,   126,   124,   121,   122,    28,    29,
      30,    31,    32,    33,    34,    15,    17,    18,    19,    37,
      16,     0,   206,    41,    42,   112,     0,   304,     0,     0,
       0,     0,    23,    25,    38,   334,   317,   318,   319,   321,
     320,   322,   323,   324,   325,   326,   327,   328,   329,   330,
     331,   332,   333,   335,   336,   337,   338,   339,   340,   341,
      22,    26,    27,    40,   114,     0,   265,   266,     0,   278,
     275,     0,   308,     0,   345,     0,     0,     0,   343,   344,
       0,     0,     0,     0,   502,   503,   504,   507,   480,   478,
     481,   482,   479,   484,   483,   487,   486,   485,   525,     0,
       0,   506,   505,   515,   514,   513,   516,   517,   511,   512,
       0,     0,   518,   519,   520,   521,   522,   523,   508,   509,
     510,   488,   489,   490,   491,   492,   493,   494,   495,   496,
     497,   498,   499,   524,   501,   500,   193,   195,   526,     0,
     511,   512,     0,   162,    68,     0,    71,    69,    60,     0,
      73,    61,     0,     0,    90,     0,     0,   296,     0,     0,
     299,   270,     0,   287,   298,   272,   274,   277,   281,   283,
     285,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   444,     0,   448,     0,     0,
       0,   284,   346,   454,   455,   349,     0,    74,    75,   363,
     364,   362,   356,   356,   356,   356,   377,     0,     0,   371,
     356,   356,     0,   366,   389,   361,   360,   356,   390,   392,
     395,   396,   393,   401,     0,   400,   358,   398,   403,   405,
     406,   409,     0,   408,   412,   410,   356,   415,   419,   417,
     421,   422,   423,   429,   356,     0,     0,     0,   125,     0,
     141,   141,   139,     0,   132,   133,     0,     0,   435,   438,
     439,     0,     0,    10,     1,     5,     6,     7,   269,    86,
      76,    88,    87,    92,    82,    77,    78,    80,    79,    83,
      81,    84,    93,    51,    52,    55,    53,    54,    56,   105,
     135,    57,    13,   207,     0,   110,   113,   174,   176,   182,
     190,   181,   180,   457,   464,   305,     0,   457,   189,   192,
       0,     0,    49,    48,    50,     0,   116,   267,     0,   279,
     143,     0,   443,   311,   310,   312,   315,     0,   313,   316,
     314,     0,   306,   307,     0,     0,     0,     0,     0,     0,
       0,     0,   165,     0,    70,    94,   350,    89,    91,    95,
       0,     0,   300,   286,   288,   291,   276,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   250,     0,   239,     0,     0,   259,
     259,     0,     0,     0,     0,   208,   224,   225,    11,     0,
       0,   445,   449,   450,   446,   447,   154,   351,   352,   356,
     379,   356,   356,   383,   356,   381,   387,   373,   375,     0,
     376,   367,   356,   368,   357,   391,   397,   359,   399,   407,
     353,     0,   416,   420,   426,   150,     0,   136,   144,     0,
     145,   146,     0,   118,     0,   433,   436,   437,   440,   441,
       8,     0,   177,     0,     0,     0,    43,   175,     0,     0,
     462,     0,   467,     0,   468,     0,   183,     0,    44,     0,
     268,   141,     0,     0,     0,   205,   204,   196,   203,   199,
       0,     0,     0,     0,   194,   202,   173,   168,   171,   172,
     170,   169,     0,   166,   163,     0,     0,   301,   302,   303,
       0,   289,   291,     0,   290,   261,   264,   239,   263,   239,
     239,   239,     0,     0,     0,   261,     0,     0,     0,     0,
       0,     0,   259,   259,     0,     0,     0,   239,   239,   239,
     260,   239,   239,     0,     0,   239,   452,     0,     0,   365,
     380,   384,   356,   385,   382,   374,   369,     0,     0,   413,
     358,   427,   424,     0,     0,     0,     0,   142,   140,   430,
      85,   178,   179,   111,     0,   460,     0,   458,     0,   463,
     470,   466,   191,     0,     0,    44,    45,     0,   115,   147,
     309,   347,   348,   198,   201,   197,   200,     0,   161,     0,
       0,   293,   294,   292,   297,   239,   262,     0,     0,     0,
       0,     0,     0,   257,     0,     0,   253,   241,   242,   244,
     245,   248,   249,   246,   247,   251,   243,   209,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   453,   451,   386,
     355,   354,     0,   426,     0,   151,   137,   138,   431,   432,
       0,     0,   472,     0,   465,   469,     0,   185,   188,     0,
       0,   167,   164,   295,     0,   212,   210,   213,   214,   254,
     255,     0,     0,   261,   240,   218,   219,   216,   217,   222,
       0,     0,     0,     0,     0,     0,   227,     0,     0,   211,
     414,   425,     0,   461,   459,     0,     0,   474,   471,     0,
     184,     0,    46,   215,   256,   258,   252,     0,     0,     0,
       0,     0,     0,     0,   259,     0,   152,   475,   473,   186,
      47,     0,     0,     0,   230,   232,     0,     0,   237,   220,
     226,     0,   221,     0,   476,   228,   229,     0,     0,   233,
       0,   234,   259,   477,     0,   238,   236,     0,   231,   235
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    97,    98,    99,   100,   101,   419,   687,   432,   433,
     434,   103,   104,   105,   106,   107,   268,   271,   108,   109,
     391,   402,   392,   393,   110,   111,   112,   113,   114,   115,
     116,   566,   415,   117,   118,   119,   120,   368,   121,   550,
     370,   122,   123,   124,   125,   126,   127,   128,   129,   130,
     131,   132,   133,   134,   262,   602,   603,   416,   417,   418,
     420,   421,   684,   756,   135,   136,   137,   138,   139,   140,
     256,   257,   587,   141,   142,   304,   506,   785,   786,   788,
     827,   828,   636,   496,   712,   713,   639,   705,   640,   143,
     144,   145,   146,   147,   148,   190,   149,   150,   151,   281,
     282,   612,   613,   283,   610,   426,   152,   153,   202,   154,
     353,   333,   347,   155,   336,   156,   321,   157,   158,   159,
     329,   160,   326,   161,   162,   163,   164,   342,   165,   166,
     167,   168,   169,   355,   170,   171,   359,   172,   173,   662,
     364,   174,   175,   176,   177,   178,   179,   203,   180,   511,
     512,   513,   181,   182,   183,   570,   422,   423,   679,   258,
     618
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -578
static const yytype_int16 yypact[] =
{
     698,  -578,   -29,  -578,   -55,  -578,  -578,  -578,    82,  2487,
    3339,   264,    45,  -578,  -578,  -578,  -578,  -578,  -578,   -98,
    -578,  3339,  3339,    97,  2345,  2345,  2345,  2345,  2345,  2345,
    2345,   128,  -578,  -578,  -578,  1264,    90,    53,  -578,   -63,
    -578,  -578,  -578,  -578,  -578,    74,  3339,  3339,    93,  -578,
    -578,  -578,  -578,  -578,  -578,    73,    26,    78,    85,   162,
    -578,    84,   -75,   -74,   101,    51,   -91,    95,   -14,    -1,
     199,   -79,   -69,   156,   -84,   173,   107,   175,    44,   291,
    -578,  -578,   309,   309,  -578,  -578,   211,   275,   291,   291,
     342,   349,  -578,   210,   274,   -73,   313,   361,   904,  -578,
     -13,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
     263,   273,  -578,  -578,   364,  3481,  1465,  -578,  -578,  -578,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  1465,   322,  -578,  -578,   152,  1078,   297,  3339,  1078,
    3339,    24,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  -578,  -578,  -578,     0,   276,  -578,  -578,   159,   327,
    -578,   326,   298,   160,  -578,    25,   285,   -12,  -578,  -578,
      11,   269,   228,   268,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,   172,
     174,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  3339,  3339,
    3339,  3339,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,   177,  -578,   176,
    -578,  -578,   180,   181,  -578,   324,  -578,  -578,  -578,  3339,
    -578,  -578,   277,   366,   359,  3339,   182,  -578,   189,   190,
    -578,  -578,   355,  2345,   191,  -578,  -578,   -55,  -578,  -578,
    -578,   323,   194,   195,   196,   197,   202,   198,   200,   201,
     203,   204,   207,   206,  1629,  -578,   212,   148,   329,   338,
     278,  -578,  -578,  -578,  -578,  -578,   311,  -578,  -578,  -578,
     280,  -578,   -35,   -54,    37,    95,  -578,   -21,   340,  -578,
      95,   179,   344,  -578,  -578,  -578,  -578,    95,  -578,  -578,
    -578,   346,  -578,  -578,   347,  -578,   270,  -578,  -578,  -578,
     288,  -578,   289,  -578,   220,  -578,    95,  -578,   290,  -578,
    -578,  -578,  -578,  -578,    95,   354,   249,  3339,  -578,   356,
     281,   281,   229,  3339,  -578,  -578,   357,   360,   296,   301,
    -578,   302,   303,  -578,  -578,  -578,  -578,   404,   -56,  -578,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  -578,   242,  -578,  -578,  -578,  -578,  -578,  -578,    20,
    -578,  -578,  -578,  -578,    19,   429,   152,  -578,  -578,  -578,
    -578,  -578,  -578,   105,  2055,  -578,   430,   105,  -578,  -578,
     363,   375,  -578,  -578,  -578,   433,     3,  -578,   362,  -578,
     309,   247,  -578,  -578,  -578,  -578,  -578,   385,  -578,  -578,
    -578,   304,  -578,  -578,  2629,  2629,   251,   252,   255,   256,
    2487,  2629,     8,  3339,  -578,  -578,  -578,  -578,  -578,  -578,
    3339,   325,   171,  -578,  -578,  2913,  -578,   257,  3055,  3055,
    3055,  3055,   258,   259,   131,   261,   266,   271,   272,   282,
     283,   284,   286,   287,  -578,   292,  -578,  3055,  3055,  3055,
    3055,  3055,   293,   294,  3055,  -578,  -578,  -578,  -578,   418,
     421,  -578,   299,  -578,  -578,  -578,  -578,  -578,   332,    95,
    -578,    95,    48,  -578,    95,  -578,  -578,  -578,  -578,   400,
    -578,  -578,    95,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
     -90,   341,  -578,  -578,   -62,   415,   295,    13,  -578,   406,
    -578,  -578,   407,  -578,   300,  -578,  -578,  -578,  -578,  -578,
    -578,   402,  -578,   305,   133,  1078,  -578,  -578,  1771,  1913,
     464,  3339,  -578,  3339,  -578,  1078,    22,   410,   485,  1078,
    -578,   281,   419,   380,   307,  -578,  -578,  -578,  -578,  -578,
    2629,  2629,  2629,  2629,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  -578,  -125,  -578,   306,  3339,   308,  -578,  -578,  -578,
     310,  -578,  2913,   312,  -578,  3055,  -578,  -578,  -578,  -578,
    -578,  -578,  3339,  3339,   420,  3055,  3055,  3055,  3055,  3055,
    3055,  3055,  3055,  3055,  3055,  3055,   -10,  -578,  -578,  -578,
    -578,  -578,  -578,   314,   315,  -578,  -578,   440,   148,  -578,
    -578,  -578,    95,  -578,  -578,  -578,  -578,   358,   365,   317,
     270,  -578,  -578,   320,   432,  3339,  3339,  -578,  -578,    36,
    -578,  -578,  -578,  -578,  3339,  -578,  3339,  -578,  3339,  -578,
    -578,  2771,  -578,  3339,    66,   485,  -578,   493,  -578,  -578,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,     8,  -578,  3339,
     318,  -578,  -578,  -578,  -578,  -578,  -578,    -2,     1,     2,
       4,   319,   321,   328,   316,   330,  -578,  -578,  -578,  -578,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,   202,     5,
       7,    12,    15,    16,    68,   449,    17,  -578,  -578,  -578,
    -578,  -578,   434,   333,   331,  -578,  -578,  -578,  -578,  -578,
     334,   335,  2203,  3339,  -578,  -578,   -67,  -578,  -578,   502,
    3339,  -578,  -578,  -578,    18,  -578,  -578,  -578,  -578,  -578,
    -578,  3339,  2629,  3055,  -578,  -578,  -578,  -578,  -578,  -578,
     337,   339,   343,   351,   353,   352,   336,   371,   369,  -578,
    -578,  -578,   439,  -578,  -578,  3339,  3339,  -578,  -578,  3339,
    -578,  3339,  -578,  -578,  -578,  -578,  -578,  3055,  3055,   -86,
     348,   441,   484,    68,  3055,   490,  -578,  -578,  3197,  -578,
    -578,   372,   373,   345,  -578,  -578,   367,   374,   350,  -578,
    -578,   -31,  -578,  3339,  -578,  -578,  -578,  3055,  3055,  -578,
     441,  -578,  3055,  -578,   376,  -578,  -578,   377,  -578,  -578
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -578,  -578,  -578,   437,   503,   -41,     6,  -148,  -578,  -578,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  -578,   368,  -578,  -578,  -578,  -578,   262,  -578,  -346,
     -80,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  -578,  -578,   431,  -578,  -578,  -157,  -170,  -371,  -578,
    -147,  -142,  -578,  -578,    75,   127,   151,   153,   178,  -578,
      86,  -578,  -419,   403,  -578,  -578,  -578,  -265,  -578,  -578,
    -286,  -578,  -507,  -173,  -203,  -578,  -465,  -577,  -470,  -578,
    -578,  -578,  -578,  -578,  -578,   379,  -578,  -578,  -578,   279,
     370,  -578,   -40,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
      55,   -44,  -342,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -172,
    -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,  -578,
    -578,   -72,  -578,  -578,   465,   147,  -578,  -143,  -578,  -578,
      -9
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -526
static const yytype_int16 yytable[] =
{
     259,   263,   428,   371,   538,   427,   102,   429,   617,   619,
     620,   621,   273,   274,   436,   284,   284,   284,   284,   284,
     284,   284,   338,   339,   388,   551,  -112,   637,   638,  -113,
     357,   642,   727,   386,   645,   641,   589,   312,   313,   665,
     765,   102,   595,   766,   767,   567,   768,   775,   716,   776,
     184,   823,    24,   596,   777,   185,   660,   778,   779,   789,
     803,   657,   527,   343,   448,   567,  -525,   349,  -187,   340,
     365,   388,   332,   380,   269,   411,   381,   351,   443,   332,
     327,   352,   185,   330,   272,   446,   697,   331,   328,   332,
     698,   597,   521,   337,   270,   356,   350,   598,   599,    24,
     412,   191,   189,   522,   102,   447,   410,   308,   309,   332,
     707,   519,   708,   709,   710,   366,   390,   430,   431,   748,
     403,   658,   102,   444,   661,   449,   275,   600,   332,   824,
     729,   730,   731,   382,   732,   733,   528,   424,   736,   424,
     424,   424,   529,   341,   799,   706,   307,   102,   800,   192,
     310,   193,   186,   187,   601,   706,   717,   718,   719,   720,
     721,   722,   344,   188,   725,   726,   311,   723,   724,   345,
     346,   693,   694,   695,   696,   194,   314,   195,   196,   186,
     187,   197,   749,   524,   841,   395,   315,   842,   780,   404,
     188,   198,   199,   567,   652,   200,   806,   387,   764,   335,
     332,   728,   781,   782,   783,   509,   784,   510,   316,   728,
     291,   332,   728,   728,   414,   728,   728,   414,   728,   456,
     457,   458,   459,   728,   317,   666,   728,   728,   728,   728,
    -525,   318,  -187,   414,   562,   689,   683,   396,  -342,   322,
     323,   405,   324,   325,   564,   482,   483,   334,   292,   293,
     294,   295,   296,   297,   298,   299,   300,   301,   332,   302,
     465,   397,   303,   398,   201,   406,   469,   407,   388,     3,
       4,     5,     6,     7,   284,   568,    10,   569,   520,   523,
     525,   526,   348,   264,   265,   484,   531,   533,   399,   360,
     361,   362,   408,   535,   372,   507,    24,    25,    26,    27,
      28,    29,   354,   706,   285,   286,   287,   288,   289,   290,
     319,   320,   542,   266,   607,   608,   609,   367,   743,   358,
     544,   363,   485,   486,   487,   488,   267,   489,   490,   491,
     492,   493,   494,   452,   453,   369,   495,   821,   822,   502,
     503,   532,   332,   624,   625,   373,    40,   414,   672,   831,
     374,   375,   389,   805,   376,   377,   378,   379,   547,   383,
     581,   384,   401,    31,   553,   425,   414,   844,   845,   388,
       3,     4,     5,     6,     7,   439,   437,   847,   438,   440,
     442,   441,   445,   450,   451,   454,   464,   455,   460,   461,
     468,   466,   467,   463,   462,   473,   470,    24,    25,    26,
      27,    28,    29,   471,   472,   475,   477,   478,   479,   480,
     481,   497,   514,   498,   499,   574,   500,   501,   673,   504,
     563,   515,   508,   530,   516,   517,   518,   534,   682,   536,
     537,   541,   688,   344,   539,   540,   543,   545,   546,   548,
     554,   552,   556,   555,   549,   588,   588,   557,   558,   559,
     560,   259,   588,   561,   604,   565,   575,   577,   578,   579,
     582,   605,   580,   583,   590,   591,   614,   584,   592,   593,
     615,   606,   622,   623,   626,   650,   646,   651,   653,   627,
     654,   647,   352,   655,   628,   629,   663,   659,   656,   667,
     668,   670,   678,   685,   686,   630,   631,   632,   691,   633,
     634,   737,   690,   715,   740,   635,   643,   644,   664,   744,
     648,   741,   760,   669,   692,   745,   787,   790,   699,   661,
     671,   801,   816,   701,   826,   702,   829,   704,   742,   772,
     734,   735,   832,   763,   769,   385,   770,   759,   306,   771,
     761,   394,   758,   773,   792,   413,   594,   813,   830,   793,
     794,   807,   435,   808,   846,   774,   424,   809,   837,   675,
     677,   840,   680,   825,   681,   810,   424,   811,   804,   812,
     424,   791,   703,   649,   576,   400,   738,     0,     0,     0,
     838,   588,   588,   588,   588,   814,   815,   835,   836,   839,
       0,   848,   849,     0,     0,     0,   700,     0,     0,     0,
       0,     0,     0,   614,     0,     0,     0,     0,   739,     0,
       0,     0,     0,   711,   714,     0,     0,   802,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   474,     0,     0,   746,   747,   820,     0,
       0,     0,     0,     0,     0,   750,   476,   751,     0,   752,
       0,     0,   755,     0,   757,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     762,     0,     0,     0,     0,     0,     0,     0,     0,     1,
       0,     0,     0,     2,     3,     4,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,     0,     0,    16,
      17,    18,    19,    20,     0,     0,     0,    21,    22,     0,
      23,    24,    25,    26,    27,    28,    29,    30,     0,    31,
       0,     0,     0,   797,   798,     0,     0,     0,    32,    33,
      34,   424,    35,    36,     0,     0,     0,     0,     0,     0,
       0,    37,   714,   588,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    38,    39,
       0,    40,    41,    42,    43,    44,   817,   818,     0,    45,
     819,     0,   424,     0,     0,     0,     0,    46,     0,    47,
      48,    49,    50,    51,    52,    53,    54,     0,     0,   834,
       0,     0,     0,    55,    56,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   843,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    57,    58,     0,
       0,     0,     0,     0,     0,    59,     0,     0,     0,     0,
      60,    61,    62,     0,     0,     0,     0,     0,    63,     0,
       0,     0,    64,    65,    66,    67,    68,    69,     0,     0,
       0,    70,    71,     0,    72,    73,    74,     0,    75,    76,
       0,     0,     0,    77,     0,    78,    79,     0,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,     0,     0,     0,     0,    96,     2,
       3,     4,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,     0,     0,    16,    17,    18,    19,    20,
       0,     0,     0,    21,    22,     0,    23,    24,    25,    26,
      27,    28,    29,    30,     0,    31,     0,     0,     0,     0,
       0,     0,     0,     0,    32,    33,    34,     0,    35,    36,
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
       0,     0,     0,     0,    63,     0,     0,     0,    64,    65,
      66,    67,    68,    69,     0,     0,     0,    70,    71,     0,
      72,    73,    74,   388,    75,    76,     0,     0,     0,    77,
       0,    78,    79,     0,    80,    81,    82,    83,    84,    85,
      86,    87,    88,    89,    90,    91,    92,    93,    94,    95,
       0,    24,     0,     0,    96,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   204,   205,
     206,   207,   208,   209,   210,     0,     0,     0,   211,     0,
       0,   212,     0,   213,   214,   215,   216,   217,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   218,     0,     0,     0,     0,     0,     0,     0,     0,
     221,   222,     0,     0,   223,   224,   225,   226,   227,     0,
       0,     0,     0,     0,     0,     0,     0,   260,   261,     0,
       0,   232,   233,   234,   235,   236,   237,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   238,   239,   240,     0,     0,   241,   242,     0,   243,
       0,     0,     0,   244,   245,     0,   246,     0,     0,   247,
     248,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   249,     0,     0,     0,   250,     0,     0,
       0,   251,   252,     0,   253,     0,     0,   254,     0,     2,
       3,     4,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,     0,   255,    16,    17,    18,    19,    20,
       0,     0,     0,    21,    22,     0,    23,    24,    25,    26,
      27,    28,    29,    30,     0,    31,     0,     0,     0,     0,
       0,     0,     0,     0,    32,    33,    34,     0,   305,    36,
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
       0,     0,     0,     0,    63,     0,     0,     0,    64,    65,
      66,    67,    68,    69,     0,     0,     0,    70,    71,     0,
      72,    73,    74,     0,    75,    76,     0,     0,     0,    77,
       0,    78,    79,     0,    80,    81,    82,    83,    84,    85,
      86,    87,    88,    89,    90,    91,    92,    93,    94,    95,
       2,     3,     4,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,     0,     0,    16,    17,    18,    19,
      20,     0,     0,     0,    21,    22,     0,    23,    24,    25,
      26,    27,    28,    29,    30,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    32,    33,    34,     0,   305,
      36,     0,     0,     0,     0,     0,     0,     0,    37,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    38,    39,     0,    40,    41,
      42,    43,    44,     0,     0,     0,    45,     0,     0,     0,
       0,     0,     0,     0,    46,     0,    47,    48,    49,    50,
      51,    52,    53,    54,     0,     0,     0,     0,     0,     0,
      55,    56,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    57,    58,     0,     0,     0,     0,
       0,     0,    59,     0,     0,     0,     0,    60,    61,    62,
       0,     0,     0,     0,     0,    63,     0,     0,     0,    64,
      65,    66,    67,    68,    69,     0,     0,     0,    70,    71,
       0,    72,    73,    74,     0,    75,    76,     0,     0,     0,
      77,     0,    78,    79,     0,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,   505,     0,     0,     0,     0,     0,     0,     0,   204,
     205,   206,   207,   208,   209,   210,     0,     0,     0,   211,
       0,     0,   212,     0,   213,   214,   215,   216,   217,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   218,     0,     0,     0,     0,     0,     0,     0,
       0,   221,   222,     0,     0,   223,   224,   225,   226,   227,
       0,     0,     0,     0,     0,     0,     0,     0,   260,   261,
       0,     0,   232,   233,   234,   235,   236,   237,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   238,   239,   240,     0,     0,   241,   242,     0,
     243,     0,     0,     0,   244,   245,     0,   246,     0,     0,
     247,   248,     0,     0,     0,     0,     0,     0,     0,     0,
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
       0,     0,     0,     0,     0,     0,   249,     0,     0,     0,
     250,     0,     0,     0,   251,   252,     0,   253,     0,     0,
     254,     0,     0,   204,   205,   206,   207,   208,   209,   210,
       0,     0,     0,   211,     0,     0,   212,   255,   213,   214,
     215,   216,   217,     0,     0,   674,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   218,     0,     0,     0,
       0,     0,     0,     0,     0,   221,   222,     0,     0,   223,
     224,   225,   226,   227,     0,     0,     0,     0,     0,     0,
       0,     0,   260,   261,     0,     0,   232,   233,   234,   235,
     236,   237,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   238,   239,   240,     0,
       0,   241,   242,     0,   243,     0,     0,     0,   244,   245,
       0,   246,     0,     0,   247,   248,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   249,     0,
       0,     0,   250,     0,     0,     0,   251,   252,     0,   253,
       0,     0,   254,     0,     0,   204,   205,   206,   207,   208,
     209,   210,     0,     0,     0,   211,     0,     0,   212,   255,
     213,   214,   215,   216,   217,   571,     0,   676,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   218,     0,
       0,     0,     0,     0,   572,     0,     0,   221,   222,     0,
       0,   223,   224,   225,   226,   227,     0,     0,     0,     0,
       0,     0,     0,     0,   260,   261,     0,     0,   232,   233,
     234,   235,   236,   237,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   238,   239,
     240,     0,     0,   241,   242,     0,   243,     0,     0,     0,
     244,   245,     0,   246,     0,     0,   247,   248,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     249,     0,     0,     0,   250,     0,     0,     0,   251,   252,
       0,   253,     0,     0,   254,     0,     0,     0,     0,     0,
       0,     0,     0,   204,   205,   206,   207,   208,   209,   210,
       0,   255,     0,   211,     0,     0,   212,   573,   213,   214,
     215,   216,   217,   795,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   218,     0,     0,     0,
       0,     0,     0,     0,     0,   221,   222,     0,     0,   223,
     224,   225,   226,   227,     0,     0,     0,     0,     0,     0,
       0,     0,   260,   261,     0,     0,   232,   233,   234,   235,
     236,   237,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   238,   239,   240,     0,
       0,   241,   242,     0,   243,     0,     0,     0,   244,   245,
       0,   246,     0,     0,   247,   248,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   249,     0,
       0,     0,   250,     0,     0,     0,   251,   252,     0,   253,
       0,     0,   254,     0,     0,   204,   205,   206,   207,   208,
     209,   210,     0,     0,     0,   211,     0,   276,   212,   255,
     213,   214,   215,   216,   217,   796,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   218,     0,
       0,     0,     0,     0,     0,     0,     0,   221,   222,     0,
       0,   223,   224,   225,   226,   227,     0,     0,     0,     0,
       0,     0,     0,     0,   260,   261,     0,     0,   232,   233,
     234,   235,   236,   237,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   277,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   278,   279,   238,   239,
     240,   280,     0,   241,   242,     0,   243,     0,     0,     0,
     244,   245,     0,   246,     0,     0,   247,   248,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     249,     0,     0,     0,   250,     0,     0,     0,   251,   252,
       0,   253,     0,     0,   254,     0,     0,   204,   205,   206,
     207,   208,   209,   210,     0,     0,     0,   211,     0,     0,
     212,   255,   213,   214,   215,   216,   217,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     218,     0,     0,     0,     0,     0,   219,   220,     0,   221,
     222,     0,     0,   223,   224,   225,   226,   227,     0,     0,
       0,     0,     0,     0,     0,     0,   228,   229,   230,   231,
     232,   233,   234,   235,   236,   237,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     238,   239,   240,     0,     0,   241,   242,     0,   243,     0,
       0,     0,   244,   245,     0,   246,     0,     0,   247,   248,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   249,     0,     0,     0,   250,     0,     0,     0,
     251,   252,     0,   253,     0,     0,   254,     0,     0,   204,
     205,   206,   207,   208,   209,   210,     0,     0,     0,   211,
       0,     0,   212,   255,   213,   214,   215,   216,   217,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   218,     0,     0,     0,     0,     0,     0,     0,
       0,   221,   222,     0,     0,   223,   224,   225,   226,   227,
       0,     0,     0,     0,     0,     0,     0,     0,   260,   261,
       0,     0,   232,   233,   234,   235,   236,   237,   585,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   238,   239,   240,   586,     0,   241,   242,     0,
     243,     0,     0,     0,   244,   245,     0,   246,     0,     0,
     247,   248,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   249,     0,     0,     0,   250,     0,
       0,     0,   251,   252,     0,   253,     0,     0,   254,     0,
       0,   204,   205,   206,   207,   208,   209,   210,     0,     0,
       0,   211,     0,     0,   212,   255,   213,   214,   215,   216,
     217,   753,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   218,     0,     0,     0,     0,     0,
     754,     0,     0,   221,   222,     0,     0,   223,   224,   225,
     226,   227,     0,     0,     0,     0,     0,     0,     0,     0,
     260,   261,     0,     0,   232,   233,   234,   235,   236,   237,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   238,   239,   240,     0,     0,   241,
     242,     0,   243,     0,     0,     0,   244,   245,     0,   246,
       0,     0,   247,   248,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   249,     0,     0,     0,
     250,     0,     0,     0,   251,   252,     0,   253,     0,     0,
     254,     0,     0,   204,   205,   206,   207,   208,   209,   210,
       0,     0,     0,   211,     0,     0,   212,   255,   213,   214,
     215,   216,   217,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   218,     0,     0,     0,
       0,     0,     0,     0,     0,   221,   222,     0,     0,   223,
     224,   225,   226,   227,     0,     0,     0,     0,     0,     0,
       0,     0,   260,   261,     0,     0,   232,   233,   234,   235,
     236,   237,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   238,   239,   240,   611,
       0,   241,   242,     0,   243,     0,     0,     0,   244,   245,
       0,   246,     0,     0,   247,   248,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   249,     0,
       0,     0,   250,     0,     0,     0,   251,   252,     0,   253,
       0,     0,   254,     0,     0,   204,   205,   206,   207,   208,
     209,   210,     0,     0,     0,   211,     0,     0,   212,   255,
     213,   214,   215,   216,   217,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   218,     0,
       0,     0,     0,   616,     0,     0,     0,   221,   222,     0,
       0,   223,   224,   225,   226,   227,     0,     0,     0,     0,
       0,     0,     0,     0,   260,   261,     0,     0,   232,   233,
     234,   235,   236,   237,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   238,   239,
     240,     0,     0,   241,   242,     0,   243,     0,     0,     0,
     244,   245,     0,   246,     0,     0,   247,   248,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     249,     0,     0,     0,   250,     0,     0,     0,   251,   252,
       0,   253,     0,     0,   254,     0,     0,   204,   205,   206,
     207,   208,   209,   210,     0,     0,     0,   211,     0,     0,
     212,   255,   213,   214,   215,   216,   217,   833,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     218,     0,     0,     0,     0,     0,     0,     0,     0,   221,
     222,     0,     0,   223,   224,   225,   226,   227,     0,     0,
       0,     0,     0,     0,     0,     0,   260,   261,     0,     0,
     232,   233,   234,   235,   236,   237,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     238,   239,   240,     0,     0,   241,   242,     0,   243,     0,
       0,     0,   244,   245,     0,   246,     0,     0,   247,   248,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   249,     0,     0,     0,   250,     0,     0,     0,
     251,   252,     0,   253,     0,     0,   254,     0,     0,   204,
     205,   206,   207,   208,   209,   210,     0,     0,     0,   211,
       0,     0,   212,   255,   213,   214,   215,   216,   217,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   218,     0,     0,     0,     0,     0,     0,     0,
       0,   221,   222,     0,     0,   223,   224,   225,   226,   227,
       0,     0,     0,     0,     0,     0,     0,     0,   260,   261,
       0,     0,   232,   233,   234,   235,   236,   237,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   238,   239,   240,     0,     0,   241,   242,     0,
     243,     0,     0,     0,   244,   245,     0,   246,     0,     0,
     247,   248,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   249,     0,     0,     0,   250,     0,
       0,     0,   251,   252,     0,   253,     0,     0,   254,     0,
       0,   204,   205,   206,   207,   208,   209,   210,     0,     0,
       0,   211,     0,     0,   212,   255,   213,   214,   215,   216,
     217,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   409,     0,     0,     0,     0,     0,
       0,     0,     0,   221,   222,     0,     0,   223,   224,   225,
     226,   227,     0,     0,     0,     0,     0,     0,     0,     0,
     260,   261,     0,     0,   232,   233,   234,   235,   236,   237,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   238,   239,   240,     0,     0,   241,
     242,     0,   243,     0,     0,     0,   244,   245,     0,   246,
       0,     0,   247,   248,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   249,     0,     0,     0,
     250,     0,     0,     0,   251,   252,     0,   253,     0,     0,
     254,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   255
};

static const yytype_int16 yycheck[] =
{
       9,    10,   149,    83,   346,   148,     0,   150,   478,   479,
     480,   481,    21,    22,   184,    24,    25,    26,    27,    28,
      29,    30,    66,    67,     5,   371,    26,   497,   498,    26,
      74,   501,    42,    46,   504,   500,   455,    46,    47,    26,
      42,    35,   461,    42,    42,   416,    42,    42,   625,    42,
      79,   137,    33,    45,    42,   111,   118,    42,    42,    42,
      42,   151,    83,    64,    53,   436,    46,   146,    46,    83,
      26,     5,   163,   146,    29,   116,   149,   146,    53,   163,
     155,   150,   111,   157,   182,    97,   211,   161,   163,   163,
     215,    83,   146,   184,    49,   179,   175,    89,    90,    33,
     141,    19,   157,   157,    98,   117,   115,    54,    55,   163,
     617,   146,   619,   620,   621,    71,   110,    93,    94,    83,
     114,   211,   116,    98,   186,   114,    29,   119,   163,   215,
     637,   638,   639,   206,   641,   642,   157,   146,   645,   148,
     149,   150,   163,   157,   211,   615,    56,   141,   215,    67,
     213,    69,   208,   209,   146,   625,   626,   627,   628,   629,
     630,   631,   163,   219,   634,   635,    92,   632,   633,   170,
     171,   590,   591,   592,   593,    93,    83,    95,    96,   208,
     209,    99,   146,   146,   215,   110,   113,   218,   120,   114,
     219,   109,   110,   564,   146,   113,   773,   210,   705,   148,
     163,   211,   134,   135,   136,    57,   138,    59,   182,   211,
      82,   163,   211,   211,   214,   211,   211,   214,   211,   228,
     229,   230,   231,   211,   146,   212,   211,   211,   211,   211,
     210,   146,   210,   214,   215,   581,   214,   110,   156,   155,
     156,   114,   158,   159,   414,    43,    44,   146,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   163,   131,
     269,   110,   134,   110,   182,   114,   275,   114,     5,     6,
       7,     8,     9,    10,   283,   170,    13,   172,   322,   323,
     324,   325,    83,    19,    20,    83,   330,   331,   110,   182,
     183,   184,   114,   337,    83,   304,    33,    34,    35,    36,
      37,    38,   146,   773,    25,    26,    27,    28,    29,    30,
     148,   149,   356,    49,   143,   144,   145,    26,   660,   146,
     364,   146,   120,   121,   122,   123,    62,   125,   126,   127,
     128,   129,   130,    65,    66,    26,   134,   807,   808,   132,
     133,   162,   163,   212,   213,    70,    83,   214,   215,   814,
      88,    89,    89,   772,    12,     6,   146,    83,   367,    46,
     440,     0,    89,    41,   373,    68,   214,   837,   838,     5,
       6,     7,     8,     9,    10,    48,   100,   842,   219,    53,
     220,    83,    97,   114,   156,   213,    62,   213,   211,   213,
      31,   114,    26,   212,   214,    40,   214,    33,    34,    35,
      36,    37,    38,   214,   214,   214,    83,   213,   213,   213,
     213,   213,    83,   213,   213,   424,   213,   213,   565,   213,
     414,    83,   210,    83,   146,   114,   146,    83,   575,    83,
      83,   211,   579,   163,   146,   146,   146,    83,   189,    83,
      83,   212,   146,    83,   163,   454,   455,   146,   146,   146,
      46,   460,   461,   211,   463,    26,    26,    94,    83,    26,
     213,   470,   100,    78,   213,   213,   475,   163,   213,   213,
     213,   146,   214,   214,   213,   519,    58,   521,   522,   213,
     524,    60,   150,    83,   213,   213,    71,   146,   532,    83,
      83,    89,    28,    83,     9,   213,   213,   213,   118,   213,
     213,    61,    83,    83,   146,   213,   213,   213,   213,   189,
     211,   146,    19,   213,   207,    83,    67,    83,   212,   186,
     215,    19,    83,   215,    83,   215,    42,   215,   211,   213,
     216,   216,    42,   215,   215,    98,   215,   685,    35,   211,
     697,   110,   684,   213,   213,   142,   460,   211,   813,   215,
     215,   214,   184,   214,   840,   728,   565,   214,   213,   568,
     569,   211,   571,   215,   573,   214,   575,   214,   771,   217,
     579,   743,   612,   518,   427,   110,   648,    -1,    -1,    -1,
     213,   590,   591,   592,   593,   214,   217,   215,   215,   215,
      -1,   215,   215,    -1,    -1,    -1,   605,    -1,    -1,    -1,
      -1,    -1,    -1,   612,    -1,    -1,    -1,    -1,   652,    -1,
      -1,    -1,    -1,   622,   623,    -1,    -1,   760,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   283,    -1,    -1,   665,   666,   801,    -1,
      -1,    -1,    -1,    -1,    -1,   674,   287,   676,    -1,   678,
      -1,    -1,   681,    -1,   683,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     699,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,     1,
      -1,    -1,    -1,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,    16,    17,    18,    -1,    -1,    21,
      22,    23,    24,    25,    -1,    -1,    -1,    29,    30,    -1,
      32,    33,    34,    35,    36,    37,    38,    39,    -1,    41,
      -1,    -1,    -1,   752,   753,    -1,    -1,    -1,    50,    51,
      52,   760,    54,    55,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    63,   771,   772,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    80,    81,
      -1,    83,    84,    85,    86,    87,   795,   796,    -1,    91,
     799,    -1,   801,    -1,    -1,    -1,    -1,    99,    -1,   101,
     102,   103,   104,   105,   106,   107,   108,    -1,    -1,   818,
      -1,    -1,    -1,   115,   116,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   833,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   139,   140,    -1,
      -1,    -1,    -1,    -1,    -1,   147,    -1,    -1,    -1,    -1,
     152,   153,   154,    -1,    -1,    -1,    -1,    -1,   160,    -1,
      -1,    -1,   164,   165,   166,   167,   168,   169,    -1,    -1,
      -1,   173,   174,    -1,   176,   177,   178,    -1,   180,   181,
      -1,    -1,    -1,   185,    -1,   187,   188,    -1,   190,   191,
     192,   193,   194,   195,   196,   197,   198,   199,   200,   201,
     202,   203,   204,   205,    -1,    -1,    -1,    -1,   210,     5,
       6,     7,     8,     9,    10,    11,    12,    13,    14,    15,
      16,    17,    18,    -1,    -1,    21,    22,    23,    24,    25,
      -1,    -1,    -1,    29,    30,    -1,    32,    33,    34,    35,
      36,    37,    38,    39,    -1,    41,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    50,    51,    52,    -1,    54,    55,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    63,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    80,    81,    -1,    83,    84,    85,
      86,    87,    -1,    -1,    -1,    91,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    99,    -1,   101,   102,   103,   104,   105,
     106,   107,   108,    -1,    -1,    -1,    -1,    -1,    -1,   115,
     116,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   139,   140,    -1,    -1,    -1,    -1,    -1,
      -1,   147,    -1,    -1,    -1,    -1,   152,   153,   154,    -1,
      -1,    -1,    -1,    -1,   160,    -1,    -1,    -1,   164,   165,
     166,   167,   168,   169,    -1,    -1,    -1,   173,   174,    -1,
     176,   177,   178,     5,   180,   181,    -1,    -1,    -1,   185,
      -1,   187,   188,    -1,   190,   191,   192,   193,   194,   195,
     196,   197,   198,   199,   200,   201,   202,   203,   204,   205,
      -1,    33,    -1,    -1,   210,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    50,    51,
      52,    53,    54,    55,    56,    -1,    -1,    -1,    60,    -1,
      -1,    63,    -1,    65,    66,    67,    68,    69,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    83,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      92,    93,    -1,    -1,    96,    97,    98,    99,   100,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   109,   110,    -1,
      -1,   113,   114,   115,   116,   117,   118,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   143,   144,   145,    -1,    -1,   148,   149,    -1,   151,
      -1,    -1,    -1,   155,   156,    -1,   158,    -1,    -1,   161,
     162,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   175,    -1,    -1,    -1,   179,    -1,    -1,
      -1,   183,   184,    -1,   186,    -1,    -1,   189,    -1,     5,
       6,     7,     8,     9,    10,    11,    12,    13,    14,    15,
      16,    17,    18,    -1,   206,    21,    22,    23,    24,    25,
      -1,    -1,    -1,    29,    30,    -1,    32,    33,    34,    35,
      36,    37,    38,    39,    -1,    41,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    50,    51,    52,    -1,    54,    55,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    63,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    80,    81,    -1,    83,    84,    85,
      86,    87,    -1,    -1,    -1,    91,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    99,    -1,   101,   102,   103,   104,   105,
     106,   107,   108,    -1,    -1,    -1,    -1,    -1,    -1,   115,
     116,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   139,   140,    -1,    -1,    -1,    -1,    -1,
      -1,   147,    -1,    -1,    -1,    -1,   152,   153,   154,    -1,
      -1,    -1,    -1,    -1,   160,    -1,    -1,    -1,   164,   165,
     166,   167,   168,   169,    -1,    -1,    -1,   173,   174,    -1,
     176,   177,   178,    -1,   180,   181,    -1,    -1,    -1,   185,
      -1,   187,   188,    -1,   190,   191,   192,   193,   194,   195,
     196,   197,   198,   199,   200,   201,   202,   203,   204,   205,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    -1,    -1,    21,    22,    23,    24,
      25,    -1,    -1,    -1,    29,    30,    -1,    32,    33,    34,
      35,    36,    37,    38,    39,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    50,    51,    52,    -1,    54,
      55,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    63,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    80,    81,    -1,    83,    84,
      85,    86,    87,    -1,    -1,    -1,    91,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    99,    -1,   101,   102,   103,   104,
     105,   106,   107,   108,    -1,    -1,    -1,    -1,    -1,    -1,
     115,   116,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   139,   140,    -1,    -1,    -1,    -1,
      -1,    -1,   147,    -1,    -1,    -1,    -1,   152,   153,   154,
      -1,    -1,    -1,    -1,    -1,   160,    -1,    -1,    -1,   164,
     165,   166,   167,   168,   169,    -1,    -1,    -1,   173,   174,
      -1,   176,   177,   178,    -1,   180,   181,    -1,    -1,    -1,
     185,    -1,   187,   188,    -1,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,   200,   201,   202,   203,   204,
     205,    42,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    50,
      51,    52,    53,    54,    55,    56,    -1,    -1,    -1,    60,
      -1,    -1,    63,    -1,    65,    66,    67,    68,    69,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    83,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    92,    93,    -1,    -1,    96,    97,    98,    99,   100,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   109,   110,
      -1,    -1,   113,   114,   115,   116,   117,   118,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   143,   144,   145,    -1,    -1,   148,   149,    -1,
     151,    -1,    -1,    -1,   155,   156,    -1,   158,    -1,    -1,
     161,   162,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   175,    -1,    -1,    -1,   179,    -1,
      -1,    -1,   183,   184,    -1,   186,    -1,    -1,   189,    -1,
      -1,    50,    51,    52,    53,    54,    55,    56,    -1,    -1,
      -1,    60,    -1,    -1,    63,   206,    65,    66,    67,    68,
      69,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    83,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    92,    93,    -1,    -1,    96,    97,    98,
      99,   100,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     109,   110,    -1,    -1,   113,   114,   115,   116,   117,   118,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   143,   144,   145,    -1,    -1,   148,
     149,    -1,   151,    -1,    -1,    -1,   155,   156,    -1,   158,
      -1,    -1,   161,   162,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   175,    -1,    -1,    -1,
     179,    -1,    -1,    -1,   183,   184,    -1,   186,    -1,    -1,
     189,    -1,    -1,    50,    51,    52,    53,    54,    55,    56,
      -1,    -1,    -1,    60,    -1,    -1,    63,   206,    65,    66,
      67,    68,    69,    -1,    -1,   214,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    83,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    92,    93,    -1,    -1,    96,
      97,    98,    99,   100,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   109,   110,    -1,    -1,   113,   114,   115,   116,
     117,   118,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   143,   144,   145,    -1,
      -1,   148,   149,    -1,   151,    -1,    -1,    -1,   155,   156,
      -1,   158,    -1,    -1,   161,   162,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   175,    -1,
      -1,    -1,   179,    -1,    -1,    -1,   183,   184,    -1,   186,
      -1,    -1,   189,    -1,    -1,    50,    51,    52,    53,    54,
      55,    56,    -1,    -1,    -1,    60,    -1,    -1,    63,   206,
      65,    66,    67,    68,    69,    70,    -1,   214,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    83,    -1,
      -1,    -1,    -1,    -1,    89,    -1,    -1,    92,    93,    -1,
      -1,    96,    97,    98,    99,   100,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   109,   110,    -1,    -1,   113,   114,
     115,   116,   117,   118,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   143,   144,
     145,    -1,    -1,   148,   149,    -1,   151,    -1,    -1,    -1,
     155,   156,    -1,   158,    -1,    -1,   161,   162,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     175,    -1,    -1,    -1,   179,    -1,    -1,    -1,   183,   184,
      -1,   186,    -1,    -1,   189,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    50,    51,    52,    53,    54,    55,    56,
      -1,   206,    -1,    60,    -1,    -1,    63,   212,    65,    66,
      67,    68,    69,    70,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    83,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    92,    93,    -1,    -1,    96,
      97,    98,    99,   100,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   109,   110,    -1,    -1,   113,   114,   115,   116,
     117,   118,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   143,   144,   145,    -1,
      -1,   148,   149,    -1,   151,    -1,    -1,    -1,   155,   156,
      -1,   158,    -1,    -1,   161,   162,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   175,    -1,
      -1,    -1,   179,    -1,    -1,    -1,   183,   184,    -1,   186,
      -1,    -1,   189,    -1,    -1,    50,    51,    52,    53,    54,
      55,    56,    -1,    -1,    -1,    60,    -1,    62,    63,   206,
      65,    66,    67,    68,    69,   212,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    83,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    92,    93,    -1,
      -1,    96,    97,    98,    99,   100,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   109,   110,    -1,    -1,   113,   114,
     115,   116,   117,   118,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   130,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   141,   142,   143,   144,
     145,   146,    -1,   148,   149,    -1,   151,    -1,    -1,    -1,
     155,   156,    -1,   158,    -1,    -1,   161,   162,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     175,    -1,    -1,    -1,   179,    -1,    -1,    -1,   183,   184,
      -1,   186,    -1,    -1,   189,    -1,    -1,    50,    51,    52,
      53,    54,    55,    56,    -1,    -1,    -1,    60,    -1,    -1,
      63,   206,    65,    66,    67,    68,    69,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      83,    -1,    -1,    -1,    -1,    -1,    89,    90,    -1,    92,
      93,    -1,    -1,    96,    97,    98,    99,   100,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   109,   110,   111,   112,
     113,   114,   115,   116,   117,   118,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     143,   144,   145,    -1,    -1,   148,   149,    -1,   151,    -1,
      -1,    -1,   155,   156,    -1,   158,    -1,    -1,   161,   162,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   175,    -1,    -1,    -1,   179,    -1,    -1,    -1,
     183,   184,    -1,   186,    -1,    -1,   189,    -1,    -1,    50,
      51,    52,    53,    54,    55,    56,    -1,    -1,    -1,    60,
      -1,    -1,    63,   206,    65,    66,    67,    68,    69,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    83,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    92,    93,    -1,    -1,    96,    97,    98,    99,   100,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   109,   110,
      -1,    -1,   113,   114,   115,   116,   117,   118,   119,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   143,   144,   145,   146,    -1,   148,   149,    -1,
     151,    -1,    -1,    -1,   155,   156,    -1,   158,    -1,    -1,
     161,   162,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   175,    -1,    -1,    -1,   179,    -1,
      -1,    -1,   183,   184,    -1,   186,    -1,    -1,   189,    -1,
      -1,    50,    51,    52,    53,    54,    55,    56,    -1,    -1,
      -1,    60,    -1,    -1,    63,   206,    65,    66,    67,    68,
      69,    70,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    83,    -1,    -1,    -1,    -1,    -1,
      89,    -1,    -1,    92,    93,    -1,    -1,    96,    97,    98,
      99,   100,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     109,   110,    -1,    -1,   113,   114,   115,   116,   117,   118,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   143,   144,   145,    -1,    -1,   148,
     149,    -1,   151,    -1,    -1,    -1,   155,   156,    -1,   158,
      -1,    -1,   161,   162,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   175,    -1,    -1,    -1,
     179,    -1,    -1,    -1,   183,   184,    -1,   186,    -1,    -1,
     189,    -1,    -1,    50,    51,    52,    53,    54,    55,    56,
      -1,    -1,    -1,    60,    -1,    -1,    63,   206,    65,    66,
      67,    68,    69,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    83,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    92,    93,    -1,    -1,    96,
      97,    98,    99,   100,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   109,   110,    -1,    -1,   113,   114,   115,   116,
     117,   118,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   143,   144,   145,   146,
      -1,   148,   149,    -1,   151,    -1,    -1,    -1,   155,   156,
      -1,   158,    -1,    -1,   161,   162,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   175,    -1,
      -1,    -1,   179,    -1,    -1,    -1,   183,   184,    -1,   186,
      -1,    -1,   189,    -1,    -1,    50,    51,    52,    53,    54,
      55,    56,    -1,    -1,    -1,    60,    -1,    -1,    63,   206,
      65,    66,    67,    68,    69,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    83,    -1,
      -1,    -1,    -1,    88,    -1,    -1,    -1,    92,    93,    -1,
      -1,    96,    97,    98,    99,   100,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   109,   110,    -1,    -1,   113,   114,
     115,   116,   117,   118,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   143,   144,
     145,    -1,    -1,   148,   149,    -1,   151,    -1,    -1,    -1,
     155,   156,    -1,   158,    -1,    -1,   161,   162,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     175,    -1,    -1,    -1,   179,    -1,    -1,    -1,   183,   184,
      -1,   186,    -1,    -1,   189,    -1,    -1,    50,    51,    52,
      53,    54,    55,    56,    -1,    -1,    -1,    60,    -1,    -1,
      63,   206,    65,    66,    67,    68,    69,    70,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      83,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    92,
      93,    -1,    -1,    96,    97,    98,    99,   100,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   109,   110,    -1,    -1,
     113,   114,   115,   116,   117,   118,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     143,   144,   145,    -1,    -1,   148,   149,    -1,   151,    -1,
      -1,    -1,   155,   156,    -1,   158,    -1,    -1,   161,   162,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   175,    -1,    -1,    -1,   179,    -1,    -1,    -1,
     183,   184,    -1,   186,    -1,    -1,   189,    -1,    -1,    50,
      51,    52,    53,    54,    55,    56,    -1,    -1,    -1,    60,
      -1,    -1,    63,   206,    65,    66,    67,    68,    69,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    83,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    92,    93,    -1,    -1,    96,    97,    98,    99,   100,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   109,   110,
      -1,    -1,   113,   114,   115,   116,   117,   118,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   143,   144,   145,    -1,    -1,   148,   149,    -1,
     151,    -1,    -1,    -1,   155,   156,    -1,   158,    -1,    -1,
     161,   162,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   175,    -1,    -1,    -1,   179,    -1,
      -1,    -1,   183,   184,    -1,   186,    -1,    -1,   189,    -1,
      -1,    50,    51,    52,    53,    54,    55,    56,    -1,    -1,
      -1,    60,    -1,    -1,    63,   206,    65,    66,    67,    68,
      69,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    83,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    92,    93,    -1,    -1,    96,    97,    98,
      99,   100,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     109,   110,    -1,    -1,   113,   114,   115,   116,   117,   118,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   143,   144,   145,    -1,    -1,   148,
     149,    -1,   151,    -1,    -1,    -1,   155,   156,    -1,   158,
      -1,    -1,   161,   162,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   175,    -1,    -1,    -1,
     179,    -1,    -1,    -1,   183,   184,    -1,   186,    -1,    -1,
     189,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   206
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     1,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    21,    22,    23,    24,
      25,    29,    30,    32,    33,    34,    35,    36,    37,    38,
      39,    41,    50,    51,    52,    54,    55,    63,    80,    81,
      83,    84,    85,    86,    87,    91,    99,   101,   102,   103,
     104,   105,   106,   107,   108,   115,   116,   139,   140,   147,
     152,   153,   154,   160,   164,   165,   166,   167,   168,   169,
     173,   174,   176,   177,   178,   180,   181,   185,   187,   188,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
     200,   201,   202,   203,   204,   205,   210,   222,   223,   224,
     225,   226,   227,   232,   233,   234,   235,   236,   239,   240,
     245,   246,   247,   248,   249,   250,   251,   254,   255,   256,
     257,   259,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   285,   286,   287,   288,   289,
     290,   294,   295,   310,   311,   312,   313,   314,   315,   317,
     318,   319,   327,   328,   330,   334,   336,   338,   339,   340,
     342,   344,   345,   346,   347,   349,   350,   351,   352,   353,
     355,   356,   358,   359,   362,   363,   364,   365,   366,   367,
     369,   373,   374,   375,    79,   111,   208,   209,   219,   157,
     316,    19,    67,    69,    93,    95,    96,    99,   109,   110,
     113,   182,   329,   368,    50,    51,    52,    53,    54,    55,
      56,    60,    63,    65,    66,    67,    68,    69,    83,    89,
      90,    92,    93,    96,    97,    98,    99,   100,   109,   110,
     111,   112,   113,   114,   115,   116,   117,   118,   143,   144,
     145,   148,   149,   151,   155,   156,   158,   161,   162,   175,
     179,   183,   184,   186,   189,   206,   291,   292,   380,   381,
     109,   110,   275,   381,    19,    20,    49,    62,   237,    29,
      49,   238,   182,   381,   381,    29,    62,   130,   141,   142,
     146,   320,   321,   324,   381,   320,   320,   320,   320,   320,
     320,    82,   120,   121,   122,   123,   124,   125,   126,   127,
     128,   129,   131,   134,   296,    54,   225,    56,    54,    55,
     213,    92,   381,   381,    83,   113,   182,   146,   146,   148,
     149,   337,   155,   156,   158,   159,   343,   155,   163,   341,
     157,   161,   163,   332,   146,   148,   335,   184,   332,   332,
      83,   157,   348,    64,   163,   170,   171,   333,    83,   146,
     175,   146,   150,   331,   146,   354,   179,   332,   146,   357,
     182,   183,   184,   146,   361,    26,    71,    26,   258,    26,
     261,   261,    83,    70,   258,   258,    12,     6,   146,    83,
     146,   149,   206,    46,     0,   224,    46,   210,     5,    89,
     227,   241,   243,   244,   274,   285,   286,   287,   288,   289,
     375,    89,   242,   227,   285,   286,   287,   288,   289,    83,
     381,   226,   226,   294,   214,   253,   278,   279,   280,   227,
     281,   282,   377,   378,   381,    68,   326,   378,   281,   378,
      93,    94,   229,   230,   231,   253,   278,   100,   219,    48,
      53,    83,   220,    53,    98,    97,    97,   117,    53,   114,
     114,   156,    65,    66,   213,   213,   381,   381,   381,   381,
     211,   213,   214,   212,    62,   381,   114,    26,    31,   381,
     214,   214,   214,    40,   321,   214,   316,    83,   213,   213,
     213,   213,    43,    44,    83,   120,   121,   122,   123,   125,
     126,   127,   128,   129,   130,   134,   304,   213,   213,   213,
     213,   213,   132,   133,   213,    42,   297,   381,   210,    57,
      59,   370,   371,   372,    83,    83,   146,   114,   146,   146,
     332,   146,   157,   332,   146,   332,   332,    83,   157,   163,
      83,   332,   162,   332,    83,   332,    83,    83,   333,   146,
     146,   211,   332,   146,   332,    83,   189,   381,    83,   163,
     260,   260,   212,   381,    83,    83,   146,   146,   146,   146,
      46,   211,   215,   227,   278,    26,   252,   279,   170,   172,
     376,    70,    89,   212,   381,    26,   376,    94,    83,    26,
     100,   261,   213,    78,   163,   119,   146,   293,   381,   293,
     213,   213,   213,   213,   291,   293,    45,    83,    89,    90,
     119,   146,   276,   277,   381,   381,   146,   143,   144,   145,
     325,   146,   322,   323,   381,   213,    88,   309,   381,   309,
     309,   309,   214,   214,   212,   213,   213,   213,   213,   213,
     213,   213,   213,   213,   213,   213,   303,   309,   309,   307,
     309,   307,   309,   213,   213,   309,    58,    60,   211,   331,
     332,   332,   146,   332,   332,    83,   332,   151,   211,   146,
     118,   186,   360,    71,   213,    26,   212,    83,    83,   213,
      89,   215,   215,   281,   214,   381,   214,   381,    28,   379,
     381,   381,   281,   214,   283,    83,     9,   228,   281,   260,
      83,   118,   207,   293,   293,   293,   293,   211,   215,   212,
     381,   215,   215,   323,   215,   308,   309,   303,   303,   303,
     303,   381,   305,   306,   381,    83,   308,   309,   309,   309,
     309,   309,   309,   307,   307,   309,   309,    42,   211,   303,
     303,   303,   303,   303,   216,   216,   303,    61,   372,   332,
     146,   146,   211,   333,   189,    83,   381,   381,    83,   146,
     381,   381,   381,    70,    89,   381,   284,   381,   282,   228,
      19,   277,   381,   215,   303,    42,    42,    42,    42,   215,
     215,   211,   213,   213,   304,    42,    42,    42,    42,    42,
     120,   134,   135,   136,   138,   298,   299,    67,   300,    42,
      83,   360,   213,   215,   215,    70,   212,   381,   381,   211,
     215,    19,   378,    42,   305,   293,   308,   214,   214,   214,
     214,   214,   217,   211,   214,   217,    83,   381,   381,   381,
     378,   309,   309,   137,   215,   215,    83,   301,   302,    42,
     298,   307,    42,    70,   381,   215,   215,   213,   213,   215,
     211,   215,   218,   381,   309,   309,   301,   307,   215,   215
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

  case 228:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.group_idx_str_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 229:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.table_name_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 230:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 231:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 232:

    { result->dbp_route_info_.sticky_session_ = true; ;}
    break;

  case 233:

    {result->dbp_route_info_.has_shard_key_ = true;;}
    break;

  case 234:

    { result->trace_id_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 235:

    { result->trace_id_ = (yyvsp[(3) - (6)].str); result->rpc_id_ = (yyvsp[(5) - (6)].str); ;}
    break;

  case 236:

    {;}
    break;

  case 238:

    {
                   if (result->dbp_route_info_.shard_key_count_ < OBPROXY_MAX_DBP_SHARD_KEY_NUM) {
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].left_str_ = (yyvsp[(1) - (3)].str);
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].right_str_ = (yyvsp[(3) - (3)].str);
                     ++result->dbp_route_info_.shard_key_count_;
                   }
                 ;}
    break;

  case 241:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 242:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 243:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 244:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 245:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 246:

    { result->trace_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 247:

    { result->rpc_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 248:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 249:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 250:

    { result->has_trace_log_hint_ = true; ;}
    break;

  case 251:

    { result->target_db_server_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 252:

    {
             malloc_shard_column_node((yyval.shard_node), (yyvsp[(1) - (5)].str), (yyvsp[(3) - (5)].str), DBMESH_TOKEN_STR_VAL);
             (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (5)].str);
             add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
           ;}
    break;

  case 253:

    {;}
    break;

  case 254:

    { result->has_hint_route_info_ = true; result->hint_route_info_.table_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 255:

    { result->has_hint_route_info_ = true; ;}
    break;

  case 256:

    {;}
    break;

  case 258:

    {
                    if (result->hint_route_info_.part_key_info_.node_count_ < OBPROXY_MAX_PART_KEY_PARSE_NUM) {
                      add_set_var_node(result->hint_route_info_.part_key_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_USER);
                    }
                  ;}
    break;

  case 259:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 261:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 293:

    { result->query_timeout_ = (yyvsp[(3) - (4)].num); ;}
    break;

  case 295:

    {
      add_hint_index(result->dbmesh_route_info_, (yyvsp[(3) - (5)].str));
      result->dbmesh_route_info_.index_count_++;
    ;}
    break;

  case 296:

    { result->has_trace_log_hint_ = true; ;}
    break;

  case 300:

    {;}
    break;

  case 301:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_WEAK); ;}
    break;

  case 302:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_STRONG); ;}
    break;

  case 303:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_FROZEN); ;}
    break;

  case 306:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_WARNINGS; ;}
    break;

  case 307:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_ERRORS; ;}
    break;

  case 308:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 309:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 310:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_SLAVE_HOSTS; ;}
    break;

  case 311:

    {
            result->is_binlog_related_ = true;
            result->cur_stmt_type_ = OBPROXY_T_SHOW_SLAVE_STATUS;
          ;}
    break;

  case 312:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_RELAYLOG_EVENTS; ;}
    break;

  case 313:

    { result->is_binlog_related_ = true; ;}
    break;

  case 314:

    { result->is_binlog_related_ = true; ;}
    break;

  case 315:

    { result->is_binlog_related_ = true; ;}
    break;

  case 316:

    { result->is_binlog_related_ = true; ;}
    break;

  case 346:

    { result->cur_stmt_type_ = OBPROXY_T_BINLOG_STR; ;}
    break;

  case 347:

    {
    result->cur_stmt_type_ = OBPROXY_T_SHOW_BINLOG_SERVER_FOR_TENANT;
    result->is_binlog_related_ = true;
;}
    break;

  case 348:

    { result->is_binlog_related_ = true; ;}
    break;

  case 349:

    { result->is_binlog_related_ = true; ;}
    break;

  case 350:

    { result->is_binlog_related_ = true; ;}
    break;

  case 351:

    { result->is_binlog_related_ = true; ;}
    break;

  case 352:

    {
;}
    break;

  case 353:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 354:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 355:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 356:

    {;}
    break;

  case 357:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 358:

    {;}
    break;

  case 359:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 361:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_KV_THREAD); ;}
    break;

  case 363:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 364:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 365:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 366:

    {;}
    break;

  case 367:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_ALL); ;}
    break;

  case 368:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 369:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 370:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 372:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 373:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 374:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 375:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 376:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 378:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 379:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 380:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 381:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 382:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 383:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 384:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 385:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 386:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 387:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_READ_STALE); ;}
    break;

  case 388:

    {;}
    break;

  case 389:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 390:

    {;}
    break;

  case 391:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 392:

    {;}
    break;

  case 394:

    {;}
    break;

  case 395:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 396:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 397:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 398:

    {;}
    break;

  case 399:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 400:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 401:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_GLOBALINDEX); ;}
    break;

  case 402:

    {;}
    break;

  case 403:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 404:

    {;}
    break;

  case 405:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 406:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 407:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); SET_ICMD_ONE_ID((yyvsp[(3) - (3)].num)); ;}
    break;

  case 408:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 409:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 411:

    {;}
    break;

  case 412:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 413:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 414:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 415:

    {;}
    break;

  case 416:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 418:

    {;}
    break;

  case 419:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 420:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 421:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 422:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 423:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 424:

    {;}
    break;

  case 425:

    {;}
    break;

  case 426:

    {;}
    break;

  case 427:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_PS_ALL);;}
    break;

  case 428:

    {;}
    break;

  case 429:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 430:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 431:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 432:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 433:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 434:

    {;}
    break;

  case 435:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 436:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 437:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 438:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 439:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 440:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 441:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 444:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 445:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 446:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 447:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 454:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 455:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 457:

    {;}
    break;

  case 458:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 459:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 460:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 461:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 462:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT_FINISH();
                                                ;}
    break;

  case 463:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT_FINISH();
                                                ;}
    break;

  case 464:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 465:

    {
                                                  result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                  result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                                  result->table_info_.dblink_name_ = (yyvsp[(4) - (4)].str);
                                                 ;}
    break;

  case 466:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 467:

    {
                                      result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                      result->table_info_.dblink_name_ = (yyvsp[(2) - (2)].str);
                                    ;}
    break;

  case 468:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 469:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 470:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 471:

    {
                                                  UPDATE_ALIAS_NAME((yyvsp[(5) - (5)].str));
                                                  result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                                                  result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                                                ;}
    break;

  case 472:

    { result->table_info_.join_table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 473:

    {
            result->table_info_.join_database_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.join_table_name_ = (yyvsp[(4) - (4)].str);
          ;}
    break;

  case 474:

    {
            result->table_info_.join_table_name_ = (yyvsp[(2) - (3)].str);
            result->table_info_.join_table_alias_name_ = (yyvsp[(3) - (3)].str);
         ;}
    break;

  case 475:

    {
            result->table_info_.join_table_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.join_table_alias_name_ = (yyvsp[(4) - (4)].str);
         ;}
    break;

  case 476:

    {
            result->table_info_.join_database_name_ = (yyvsp[(2) - (5)].str);
            result->table_info_.join_table_name_ = (yyvsp[(4) - (5)].str);
            result->table_info_.join_table_alias_name_ = (yyvsp[(5) - (5)].str);
         ;}
    break;

  case 477:

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

