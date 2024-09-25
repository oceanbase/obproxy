
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
     PORT = 352,
     USE = 353,
     HELP = 354,
     SET_NAMES = 355,
     SET_CHARSET = 356,
     SET_PASSWORD = 357,
     SET_DEFAULT = 358,
     SET_OB_READ_CONSISTENCY = 359,
     SET_TX_READ_ONLY = 360,
     GLOBAL = 361,
     SESSION = 362,
     GLOBAL_ALIAS = 363,
     SESSION_ALIAS = 364,
     LOCAL_ALIAS = 365,
     MASTER = 366,
     LOGS = 367,
     RESET = 368,
     FLUSH = 369,
     SERVER = 370,
     TENANT = 371,
     NUMBER_VAL = 372,
     GROUP_ID = 373,
     TABLE_ID = 374,
     ELASTIC_ID = 375,
     TESTLOAD = 376,
     ODP_COMMENT = 377,
     TNT_ID = 378,
     DISASTER_STATUS = 379,
     TRACE_ID = 380,
     RPC_ID = 381,
     TARGET_DB_SERVER = 382,
     TRACE_LOG = 383,
     DBP_COMMENT = 384,
     ROUTE_TAG = 385,
     SYS_TAG = 386,
     TABLE_NAME = 387,
     SCAN_ALL = 388,
     STICKY_SESSION = 389,
     PARALL = 390,
     SHARD_KEY = 391,
     STOP_DDL_TASK = 392,
     RETRY_DDL_TASK = 393,
     QUERY_TIMEOUT = 394,
     READ_CONSISTENCY = 395,
     WEAK = 396,
     STRONG = 397,
     FROZEN = 398,
     INT_NUM = 399,
     SHOW_PROXYNET = 400,
     THREAD = 401,
     CONNECTION = 402,
     LIMIT = 403,
     OFFSET = 404,
     SHOW_PROCESSLIST = 405,
     SHOW_PROXYSESSION = 406,
     SHOW_GLOBALSESSION = 407,
     ATTRIBUTE = 408,
     VARIABLES = 409,
     ALL = 410,
     STAT = 411,
     READ_STALE = 412,
     SHOW_PROXYCONFIG = 413,
     DIFF = 414,
     USER = 415,
     LIKE = 416,
     SHOW_PROXYSM = 417,
     SHOW_PROXYKV = 418,
     SHOW_PROXYCLUSTER = 419,
     SHOW_PROXYRESOURCE = 420,
     SHOW_PROXYCONGESTION = 421,
     SHOW_PROXYROUTE = 422,
     PARTITION = 423,
     ROUTINE = 424,
     SUBPARTITION = 425,
     SHOW_PROXYVIP = 426,
     SHOW_PROXYMEMORY = 427,
     OBJPOOL = 428,
     SHOW_SQLAUDIT = 429,
     SHOW_WARNLOG = 430,
     SHOW_PROXYSTAT = 431,
     REFRESH = 432,
     SHOW_PROXYTRACE = 433,
     SHOW_PROXYINFO = 434,
     BINARY = 435,
     UPGRADE = 436,
     IDC = 437,
     SHOW_ELASTIC_ID = 438,
     SHOW_TOPOLOGY = 439,
     GROUP_NAME = 440,
     SHOW_DB_VERSION = 441,
     SHOW_DATABASES = 442,
     SHOW_TABLES = 443,
     SHOW_FULL_TABLES = 444,
     SELECT_DATABASE = 445,
     SELECT_PROXY_STATUS = 446,
     SHOW_CREATE_TABLE = 447,
     SELECT_PROXY_VERSION = 448,
     SHOW_COLUMNS = 449,
     SHOW_INDEX = 450,
     ALTER_PROXYCONFIG = 451,
     ALTER_PROXYRESOURCE = 452,
     PING_PROXY = 453,
     KILL_PROXYSESSION = 454,
     KILL_GLOBALSESSION = 455,
     KILL = 456,
     QUERY = 457,
     BINLOG_VARIABLE = 458,
     BINLOG_USER_VAR = 459,
     BINLOG_SYS_VAR = 460
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
#define YYFINAL  377
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   3344

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  217
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  156
/* YYNRULES -- Number of rules.  */
#define YYNRULES  511
/* YYNRULES -- Number of states.  */
#define YYNSTATES  830

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   460

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,   215,     2,     2,     2,     2,
     211,   212,   216,     2,   208,     2,   209,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   206,
       2,   210,     2,     2,   207,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   213,     2,   214,     2,     2,     2,     2,
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
     205
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
     189,   191,   193,   195,   197,   200,   205,   208,   210,   212,
     216,   219,   223,   226,   229,   233,   237,   239,   241,   243,
     245,   247,   249,   251,   253,   255,   258,   260,   262,   264,
     266,   267,   270,   271,   273,   276,   282,   286,   288,   292,
     294,   296,   298,   300,   302,   304,   307,   309,   311,   313,
     315,   317,   319,   322,   325,   327,   330,   333,   338,   343,
     346,   351,   352,   355,   356,   359,   363,   367,   373,   375,
     377,   381,   387,   395,   397,   401,   403,   405,   407,   409,
     411,   413,   419,   421,   425,   431,   432,   434,   438,   440,
     442,   444,   447,   451,   453,   455,   458,   460,   463,   467,
     471,   473,   475,   477,   478,   482,   484,   488,   492,   498,
     501,   504,   509,   512,   514,   516,   518,   521,   525,   527,
     532,   539,   544,   550,   557,   562,   566,   568,   570,   572,
     574,   577,   581,   587,   594,   601,   608,   615,   622,   630,
     637,   644,   651,   658,   667,   676,   683,   684,   687,   690,
     693,   695,   699,   701,   706,   711,   715,   722,   726,   731,
     736,   743,   747,   749,   753,   754,   758,   762,   766,   770,
     774,   778,   782,   786,   790,   794,   796,   800,   806,   810,
     811,   813,   814,   816,   818,   820,   824,   828,   833,   837,
     842,   844,   847,   849,   852,   854,   857,   860,   864,   865,
     867,   870,   872,   875,   877,   880,   883,   886,   889,   890,
     893,   895,   897,   898,   901,   906,   911,   917,   919,   924,
     926,   928,   929,   931,   933,   935,   936,   938,   942,   946,
     949,   955,   959,   963,   967,   971,   975,   979,   981,   983,
     985,   987,   989,   991,   993,   995,   997,   999,  1001,  1003,
    1005,  1007,  1009,  1011,  1013,  1015,  1017,  1019,  1021,  1023,
    1025,  1027,  1028,  1030,  1032,  1034,  1037,  1043,  1049,  1052,
    1056,  1060,  1061,  1064,  1069,  1074,  1075,  1078,  1079,  1082,
    1085,  1087,  1090,  1092,  1094,  1098,  1101,  1105,  1109,  1114,
    1116,  1119,  1120,  1123,  1127,  1130,  1133,  1136,  1137,  1140,
    1144,  1147,  1151,  1154,  1158,  1162,  1167,  1170,  1172,  1175,
    1178,  1182,  1185,  1188,  1189,  1191,  1193,  1196,  1199,  1203,
    1206,  1209,  1211,  1214,  1216,  1219,  1222,  1226,  1229,  1232,
    1235,  1236,  1238,  1242,  1248,  1251,  1255,  1258,  1259,  1261,
    1264,  1267,  1270,  1273,  1278,  1284,  1290,  1294,  1296,  1299,
    1303,  1307,  1310,  1313,  1317,  1321,  1322,  1325,  1327,  1331,
    1335,  1339,  1340,  1342,  1344,  1348,  1351,  1355,  1358,  1361,
    1363,  1364,  1367,  1372,  1375,  1380,  1383,  1385,  1391,  1395,
    1399,  1402,  1407,  1411,  1417,  1419,  1421,  1423,  1425,  1427,
    1429,  1431,  1433,  1435,  1437,  1439,  1441,  1443,  1445,  1447,
    1449,  1451,  1453,  1455,  1457,  1459,  1461,  1463,  1465,  1467,
    1469,  1471,  1473,  1475,  1477,  1479,  1481,  1483,  1485,  1487,
    1489,  1491,  1493,  1495,  1497,  1499,  1501,  1503,  1505,  1507,
    1509,  1511
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     218,     0,    -1,   219,    -1,     1,    -1,   220,    -1,   219,
     220,    -1,   221,    45,    -1,   221,   206,    -1,   221,   206,
      45,    -1,   206,    -1,   206,    45,    -1,    53,   221,   206,
      -1,   222,    -1,   291,   222,    -1,   223,    -1,   281,    -1,
     287,    -1,   282,    -1,   283,    -1,   284,    -1,   230,    -1,
     229,    -1,   361,    -1,   322,    -1,   252,    -1,   323,    -1,
     365,    -1,   366,    -1,   264,    -1,   265,    -1,   266,    -1,
     267,    -1,   268,    -1,   269,    -1,   270,    -1,   231,    -1,
     243,    -1,   285,    -1,   325,    -1,   228,    -1,   367,    -1,
     305,    -1,   306,    -1,   307,   249,   248,    -1,    -1,     9,
      -1,    91,    82,   224,    19,   370,    -1,    90,    91,    82,
     224,    19,   370,    -1,   226,    -1,   225,    -1,   314,   227,
      -1,   245,   223,    -1,   245,   281,    -1,   245,   283,    -1,
     245,   284,    -1,   245,   282,    -1,   245,   285,    -1,   247,
     222,    -1,   232,    -1,   244,    -1,    14,   233,    -1,    15,
     234,    -1,    16,    -1,    17,    -1,    18,    -1,   235,    -1,
     236,    -1,    -1,    19,    -1,    61,    -1,    20,    61,    -1,
      48,    -1,    -1,    48,    -1,   137,   144,    -1,   138,   144,
      -1,   223,    -1,   281,    -1,   282,    -1,   284,    -1,   283,
      -1,   367,    -1,   270,    -1,   285,    -1,   207,    82,    -1,
     238,   208,   207,    82,    -1,   207,    82,    -1,   239,    -1,
     237,    -1,    28,   372,    26,    -1,    29,   372,    -1,    29,
     372,    30,    -1,   241,   240,    -1,   242,   238,    -1,    15,
      28,   372,    -1,    31,    28,   372,    -1,    21,    -1,    22,
      -1,    23,    -1,    24,    -1,    49,    -1,    25,    -1,    50,
      -1,    51,    -1,   246,    -1,   246,    82,    -1,    83,    -1,
      85,    -1,    86,    -1,    84,    -1,    -1,    26,   277,    -1,
      -1,   274,    -1,     5,    78,    -1,     5,    78,   249,    26,
     277,    -1,     5,    78,   274,    -1,   193,    -1,   193,    69,
     372,    -1,   250,    -1,   251,    -1,   262,    -1,   263,    -1,
     253,    -1,   261,    -1,   184,   254,    -1,   260,    -1,   190,
      -1,   191,    -1,   187,    -1,   258,    -1,   259,    -1,   194,
     254,    -1,   195,   254,    -1,   255,    -1,   246,   372,    -1,
      26,   372,    -1,    26,   372,    26,   372,    -1,    26,   372,
     209,   372,    -1,   192,    82,    -1,   192,    82,   209,    82,
      -1,    -1,   161,    82,    -1,    -1,    26,    82,    -1,   188,
     257,   256,    -1,   189,   257,   256,    -1,    11,    19,    52,
     257,   256,    -1,   186,    -1,   183,    -1,   183,    26,    82,
      -1,   183,    70,   185,   210,    82,    -1,   183,    26,    82,
      70,   185,   210,    82,    -1,    79,    -1,    80,   210,   144,
      -1,   100,    -1,   101,    -1,   102,    -1,   103,    -1,   104,
      -1,   105,    -1,    13,   271,   211,   272,   212,    -1,   372,
      -1,   372,   209,   372,    -1,   372,   209,   372,   209,   372,
      -1,    -1,   273,    -1,   272,   208,   273,    -1,    82,    -1,
     144,    -1,   117,    -1,   207,    82,    -1,   207,   207,    82,
      -1,    44,    -1,   275,    -1,   274,   275,    -1,   276,    -1,
     211,   212,    -1,   211,   223,   212,    -1,   211,   274,   212,
      -1,   369,    -1,   278,    -1,   223,    -1,    -1,   211,   280,
     212,    -1,   372,    -1,   280,   208,   372,    -1,   310,   370,
     368,    -1,   310,   370,   368,   279,   278,    -1,   312,   277,
      -1,   308,   277,    -1,   309,   321,    26,   277,    -1,   313,
     370,    -1,   108,    -1,   109,    -1,   110,    -1,    12,   288,
      -1,   289,   208,   288,    -1,   289,    -1,   207,   372,   210,
     290,    -1,   207,   207,   106,   372,   210,   290,    -1,   106,
     372,   210,   290,    -1,   207,   207,   372,   210,   290,    -1,
     207,   207,   107,   372,   210,   290,    -1,   107,   372,   210,
     290,    -1,   372,   210,   290,    -1,   372,    -1,   144,    -1,
     117,    -1,   292,    -1,   292,   291,    -1,    40,   293,    41,
      -1,    40,   122,   301,   300,    41,    -1,    40,   119,   210,
     304,   300,    41,    -1,    40,   132,   210,   304,   300,    41,
      -1,    40,   118,   210,   304,   300,    41,    -1,    40,   120,
     210,   304,   300,    41,    -1,    40,   121,   210,   304,   300,
      41,    -1,    40,    81,    82,   210,   303,   300,    41,    -1,
      40,   125,   210,   302,   300,    41,    -1,    40,   126,   210,
     302,   300,    41,    -1,    40,   123,   210,   304,   300,    41,
      -1,    40,   124,   210,   304,   300,    41,    -1,    40,   129,
     130,   210,   213,   295,   214,    41,    -1,    40,   129,   131,
     210,   213,   297,   214,    41,    -1,    40,   127,   210,   304,
     300,    41,    -1,    -1,   293,   294,    -1,    42,    82,    -1,
      43,    82,    -1,    82,    -1,   296,   208,   295,    -1,   296,
      -1,   118,   211,   304,   212,    -1,   132,   211,   304,   212,
      -1,   133,   211,   212,    -1,   133,   211,   135,   210,   304,
     212,    -1,   134,   211,   212,    -1,   136,   211,   298,   212,
      -1,    66,   211,   302,   212,    -1,    66,   211,   302,   215,
     302,   212,    -1,   299,   208,   298,    -1,   299,    -1,    82,
     210,   304,    -1,    -1,   300,   208,   301,    -1,   118,   210,
     304,    -1,   119,   210,   304,    -1,   132,   210,   304,    -1,
     120,   210,   304,    -1,   121,   210,   304,    -1,   125,   210,
     302,    -1,   126,   210,   302,    -1,   123,   210,   304,    -1,
     124,   210,   304,    -1,   128,    -1,   127,   210,   304,    -1,
      82,   209,    82,   210,   303,    -1,    82,   210,   303,    -1,
      -1,   304,    -1,    -1,   304,    -1,    82,    -1,    87,    -1,
       5,   207,   204,    -1,     5,   286,   205,    -1,     5,   207,
     207,   205,    -1,     5,   108,    97,    -1,     5,   207,   207,
      97,    -1,     5,    -1,    32,   315,    -1,     8,    -1,    33,
     315,    -1,     6,    -1,    34,   315,    -1,     7,   311,    -1,
      35,   315,   311,    -1,    -1,   155,    -1,   155,    47,    -1,
       9,    -1,    36,   315,    -1,    10,    -1,    37,   315,    -1,
      88,    89,    -1,    38,   315,    -1,   316,    39,    -1,    -1,
     319,   316,    -1,   144,    -1,   372,    -1,    -1,   317,   318,
      -1,   139,   211,   144,   212,    -1,   140,   211,   320,   212,
      -1,    61,   211,   372,   372,   212,    -1,   128,    -1,   372,
     211,   318,   212,    -1,   372,    -1,   144,    -1,    -1,   141,
      -1,   142,    -1,   143,    -1,    -1,    67,    -1,    11,   360,
      64,    -1,    11,   360,    65,    -1,    11,    66,    -1,    11,
      66,    82,   210,    82,    -1,    11,    92,    95,    -1,    11,
      92,    52,    -1,    11,    93,    94,    -1,    11,   111,    52,
      -1,    11,   180,   112,    -1,    11,    96,    94,    -1,   331,
      -1,   333,    -1,   334,    -1,   337,    -1,   335,    -1,   339,
      -1,   340,    -1,   341,    -1,   342,    -1,   344,    -1,   345,
      -1,   346,    -1,   347,    -1,   348,    -1,   350,    -1,   351,
      -1,   353,    -1,   329,    -1,   354,    -1,   355,    -1,   356,
      -1,   357,    -1,   358,    -1,   359,    -1,    -1,   106,    -1,
     107,    -1,    90,    -1,    96,   372,    -1,    11,    96,   115,
      77,   116,    -1,    11,   324,   154,   161,   203,    -1,   113,
     111,    -1,    24,   180,   112,    -1,   114,   180,   112,    -1,
      -1,   148,   144,    -1,   148,   144,   208,   144,    -1,   148,
     144,   149,   144,    -1,    -1,   161,    82,    -1,    -1,   161,
      82,    -1,   163,   330,    -1,   146,    -1,   145,   332,    -1,
     146,    -1,   147,    -1,   147,   144,   326,    -1,   158,   327,
      -1,   158,   155,   327,    -1,   158,   159,   327,    -1,   158,
     159,   160,   327,    -1,   150,    -1,   152,   336,    -1,    -1,
     153,    82,    -1,   153,   161,    82,    -1,   153,   155,    -1,
     161,    82,    -1,   151,   338,    -1,    -1,   153,   327,    -1,
     153,   144,   327,    -1,   156,   327,    -1,   156,   144,   327,
      -1,   154,   327,    -1,   154,   144,   327,    -1,   154,   155,
     327,    -1,   154,   155,   144,   327,    -1,   157,   327,    -1,
     162,    -1,   162,   144,    -1,   164,   327,    -1,   164,   182,
     327,    -1,   165,   327,    -1,   166,   343,    -1,    -1,    82,
      -1,   155,    -1,   155,    82,    -1,   167,   328,    -1,   167,
     169,   328,    -1,   167,   168,    -1,   167,    63,    -1,   171,
      -1,   171,    82,    -1,   172,    -1,   172,   144,    -1,   172,
     173,    -1,   172,   173,   144,    -1,   174,   326,    -1,   174,
     144,    -1,   175,   349,    -1,    -1,   144,    -1,   144,   208,
     144,    -1,   144,   208,   144,   208,    82,    -1,   176,   327,
      -1,   176,   177,   327,    -1,   178,   352,    -1,    -1,   144,
      -1,   144,   144,    -1,   179,   180,    -1,   179,   181,    -1,
     179,   182,    -1,   196,    12,    82,   210,    -1,   196,    12,
      82,   210,    82,    -1,   196,    12,    82,   210,   144,    -1,
     197,     6,    82,    -1,   198,    -1,   199,   144,    -1,   199,
     144,   144,    -1,   200,    82,   144,    -1,   200,    82,    -1,
     201,   144,    -1,   201,   147,   144,    -1,   201,   202,   144,
      -1,    -1,    68,   216,    -1,    53,    -1,    54,    55,   362,
      -1,    62,    53,    82,    -1,    62,    54,    82,    -1,    -1,
     363,    -1,   364,    -1,   363,   208,   364,    -1,    56,    57,
      -1,    58,    59,    60,    -1,    98,   372,    -1,    99,    82,
      -1,    82,    -1,    -1,   170,   372,    -1,   170,   211,   372,
     212,    -1,   168,   372,    -1,   168,   211,   372,   212,    -1,
     370,   368,    -1,   372,    -1,   372,   209,   372,   207,   372,
      -1,   372,   209,   372,    -1,   372,   207,   372,    -1,   372,
     372,    -1,   372,   209,   372,   372,    -1,   372,    69,   372,
      -1,   372,   209,   372,    69,   372,    -1,    54,    -1,    62,
      -1,    53,    -1,    55,    -1,    59,    -1,    65,    -1,    64,
      -1,    68,    -1,    67,    -1,    66,    -1,   146,    -1,   147,
      -1,   149,    -1,   153,    -1,   154,    -1,   156,    -1,   159,
      -1,   160,    -1,   173,    -1,   177,    -1,   181,    -1,   182,
      -1,   202,    -1,   185,    -1,    49,    -1,    50,    -1,    51,
      -1,    90,    -1,    89,    -1,    52,    -1,   141,    -1,   142,
      -1,   143,    -1,   106,    -1,   107,    -1,    95,    -1,    94,
      -1,    93,    -1,    96,    -1,    97,    -1,   111,    -1,   112,
      -1,   113,    -1,   114,    -1,   115,    -1,   116,    -1,    82,
      -1,   371,    -1
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
     752,   753,   754,   759,   760,   761,   763,   765,   766,   768,
     772,   776,   780,   784,   788,   792,   796,   801,   806,   812,
     813,   815,   816,   817,   818,   819,   820,   821,   822,   828,
     829,   830,   831,   832,   833,   834,   835,   836,   838,   839,
     840,   842,   843,   845,   850,   855,   856,   857,   858,   860,
     861,   863,   864,   866,   874,   875,   877,   878,   879,   880,
     881,   882,   883,   884,   885,   886,   887,   888,   894,   896,
     897,   899,   900,   902,   903,   905,   906,   907,   909,   910,
     912,   913,   914,   915,   916,   917,   918,   919,   921,   922,
     923,   925,   926,   927,   928,   929,   930,   932,   933,   934,
     936,   937,   939,   940,   942,   943,   944,   949,   950,   951,
     952,   954,   955,   956,   957,   959,   960,   963,   964,   965,
     966,   967,   968,   973,   974,   975,   976,   980,   981,   982,
     983,   984,   985,   986,   987,   988,   989,   990,   991,   992,
     993,   994,   995,   996,   997,   998,   999,  1000,  1001,  1002,
    1003,  1005,  1006,  1007,  1008,  1011,  1012,  1017,  1018,  1019,
    1020,  1025,  1027,  1031,  1036,  1044,  1045,  1049,  1050,  1053,
    1055,  1058,  1060,  1061,  1062,  1066,  1067,  1068,  1069,  1074,
    1076,  1078,  1079,  1080,  1081,  1082,  1085,  1087,  1088,  1089,
    1090,  1091,  1092,  1093,  1094,  1095,  1096,  1100,  1101,  1105,
    1106,  1111,  1114,  1116,  1117,  1118,  1119,  1123,  1124,  1125,
    1126,  1130,  1131,  1135,  1136,  1137,  1138,  1142,  1143,  1146,
    1148,  1149,  1150,  1151,  1155,  1156,  1159,  1161,  1162,  1163,
    1167,  1168,  1169,  1173,  1174,  1175,  1179,  1183,  1187,  1188,
    1192,  1193,  1197,  1198,  1199,  1202,  1203,  1206,  1210,  1211,
    1212,  1214,  1215,  1217,  1218,  1221,  1222,  1225,  1231,  1234,
    1236,  1237,  1238,  1239,  1240,  1242,  1247,  1250,  1255,  1259,
    1263,  1267,  1272,  1276,  1282,  1283,  1284,  1285,  1286,  1287,
    1288,  1289,  1290,  1291,  1292,  1293,  1294,  1295,  1296,  1297,
    1298,  1299,  1300,  1301,  1302,  1303,  1304,  1305,  1306,  1307,
    1308,  1309,  1310,  1311,  1312,  1313,  1314,  1315,  1316,  1317,
    1318,  1319,  1320,  1321,  1322,  1323,  1324,  1325,  1326,  1327,
    1329,  1330
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
  "INFILE", "SLAVE", "RELAYLOG", "EVENTS", "HOSTS", "BINLOG", "PORT",
  "USE", "HELP", "SET_NAMES", "SET_CHARSET", "SET_PASSWORD", "SET_DEFAULT",
  "SET_OB_READ_CONSISTENCY", "SET_TX_READ_ONLY", "GLOBAL", "SESSION",
  "GLOBAL_ALIAS", "SESSION_ALIAS", "LOCAL_ALIAS", "MASTER", "LOGS",
  "RESET", "FLUSH", "SERVER", "TENANT", "NUMBER_VAL", "GROUP_ID",
  "TABLE_ID", "ELASTIC_ID", "TESTLOAD", "ODP_COMMENT", "TNT_ID",
  "DISASTER_STATUS", "TRACE_ID", "RPC_ID", "TARGET_DB_SERVER", "TRACE_LOG",
  "DBP_COMMENT", "ROUTE_TAG", "SYS_TAG", "TABLE_NAME", "SCAN_ALL",
  "STICKY_SESSION", "PARALL", "SHARD_KEY", "STOP_DDL_TASK",
  "RETRY_DDL_TASK", "QUERY_TIMEOUT", "READ_CONSISTENCY", "WEAK", "STRONG",
  "FROZEN", "INT_NUM", "SHOW_PROXYNET", "THREAD", "CONNECTION", "LIMIT",
  "OFFSET", "SHOW_PROCESSLIST", "SHOW_PROXYSESSION", "SHOW_GLOBALSESSION",
  "ATTRIBUTE", "VARIABLES", "ALL", "STAT", "READ_STALE",
  "SHOW_PROXYCONFIG", "DIFF", "USER", "LIKE", "SHOW_PROXYSM",
  "SHOW_PROXYKV", "SHOW_PROXYCLUSTER", "SHOW_PROXYRESOURCE",
  "SHOW_PROXYCONGESTION", "SHOW_PROXYROUTE", "PARTITION", "ROUTINE",
  "SUBPARTITION", "SHOW_PROXYVIP", "SHOW_PROXYMEMORY", "OBJPOOL",
  "SHOW_SQLAUDIT", "SHOW_WARNLOG", "SHOW_PROXYSTAT", "REFRESH",
  "SHOW_PROXYTRACE", "SHOW_PROXYINFO", "BINARY", "UPGRADE", "IDC",
  "SHOW_ELASTIC_ID", "SHOW_TOPOLOGY", "GROUP_NAME", "SHOW_DB_VERSION",
  "SHOW_DATABASES", "SHOW_TABLES", "SHOW_FULL_TABLES", "SELECT_DATABASE",
  "SELECT_PROXY_STATUS", "SHOW_CREATE_TABLE", "SELECT_PROXY_VERSION",
  "SHOW_COLUMNS", "SHOW_INDEX", "ALTER_PROXYCONFIG", "ALTER_PROXYRESOURCE",
  "PING_PROXY", "KILL_PROXYSESSION", "KILL_GLOBALSESSION", "KILL", "QUERY",
  "BINLOG_VARIABLE", "BINLOG_USER_VAR", "BINLOG_SYS_VAR", "';'", "'@'",
  "','", "'.'", "'='", "'('", "')'", "'{'", "'}'", "'#'", "'*'", "$accept",
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
  "replace_stmt", "update_stmt", "delete_stmt", "merge_stmt",
  "opt_sys_var_alias", "set_stmt", "set_expr_list", "set_expr",
  "set_var_value", "comment_expr_list", "comment_expr", "comment_list",
  "comment", "dbp_comment_list", "dbp_comment", "dbp_sys_comment",
  "dbp_kv_comment_list", "dbp_kv_comment", "odp_comment_list",
  "odp_comment", "tracer_right_string_val", "name_right_string_val",
  "right_string_val", "select_with_binlog", "select_with_port",
  "select_with_opt_hint", "update_with_opt_hint", "delete_with_opt_hint",
  "insert_with_opt_hint", "insert_all_when", "replace_with_opt_hint",
  "merge_with_opt_hint", "load_data_opt_hint", "hint_list_with_end",
  "hint_list", "hint_val", "hint_val_list", "hint", "opt_read_consistency",
  "opt_quick", "show_stmt", "icmd_stmt", "opt_global_or_session",
  "binlog_stmt", "opt_limit", "opt_like", "opt_large_like", "show_proxykv",
  "opt_show_kv", "show_proxynet", "opt_show_net", "show_proxyconfig",
  "show_processlist", "show_globalsession", "opt_show_global_session",
  "show_proxysession", "opt_show_session", "show_proxysm",
  "show_proxycluster", "show_proxyresource", "show_proxycongestion",
  "opt_show_congestion", "show_proxyroute", "show_proxyvip",
  "show_proxymemory", "show_sqlaudit", "show_warnlog", "opt_show_warnlog",
  "show_proxystat", "show_proxytrace", "opt_show_trace", "show_proxyinfo",
  "alter_proxyconfig", "alter_proxyresource", "ping_proxy",
  "kill_proxysession", "kill_globalsession", "kill_mysql", "opt_count",
  "begin_stmt", "opt_transaction_characteristics",
  "transaction_characteristics", "transaction_characteristic",
  "use_db_stmt", "help_stmt", "other_stmt", "partition_factor",
  "table_references", "table_factor", "non_reserved_keyword", "var_name", 0
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
     455,   456,   457,   458,   459,   460,    59,    64,    44,    46,
      61,    40,    41,   123,   125,    35,    42
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   217,   218,   218,   219,   219,   220,   220,   220,   220,
     220,   220,   221,   221,   222,   222,   222,   222,   222,   222,
     222,   222,   222,   222,   222,   222,   222,   222,   222,   222,
     222,   222,   222,   222,   222,   222,   222,   222,   222,   222,
     222,   223,   223,   223,   224,   224,   225,   226,   227,   227,
     228,   229,   229,   229,   229,   229,   229,   230,   231,   231,
     232,   232,   232,   232,   232,   232,   232,   233,   233,   233,
     233,   233,   234,   234,   235,   236,   237,   237,   237,   237,
     237,   237,   237,   237,   238,   238,   239,   240,   240,   241,
     242,   242,   243,   243,   243,   243,   244,   244,   244,   244,
     244,   244,   244,   244,   245,   245,   246,   246,   246,   247,
     248,   248,   249,   249,   250,   250,   250,   251,   251,   252,
     252,   252,   252,   252,   253,   253,   253,   253,   253,   253,
     253,   253,   253,   253,   253,   253,   254,   254,   254,   255,
     255,   256,   256,   257,   257,   258,   258,   259,   260,   261,
     261,   261,   261,   262,   263,   264,   265,   266,   267,   268,
     269,   270,   271,   271,   271,   272,   272,   272,   273,   273,
     273,   273,   273,   273,   274,   274,   275,   276,   276,   276,
     277,   277,   278,   279,   279,   280,   280,   281,   281,   282,
     283,   284,   285,   286,   286,   286,   287,   288,   288,   289,
     289,   289,   289,   289,   289,   289,   290,   290,   290,   291,
     291,   292,   292,   292,   292,   292,   292,   292,   292,   292,
     292,   292,   292,   292,   292,   292,   293,   293,   294,   294,
     294,   295,   295,   296,   296,   296,   296,   296,   296,   297,
     297,   298,   298,   299,   300,   300,   301,   301,   301,   301,
     301,   301,   301,   301,   301,   301,   301,   301,   301,   302,
     302,   303,   303,   304,   304,   305,   305,   305,   306,   306,
     307,   307,   308,   308,   309,   309,   310,   310,   311,   311,
     311,   312,   312,   313,   313,   314,   314,   315,   316,   316,
     317,   317,   318,   318,   319,   319,   319,   319,   319,   319,
     319,   320,   320,   320,   320,   321,   321,   322,   322,   322,
     322,   322,   322,   322,   322,   322,   322,   323,   323,   323,
     323,   323,   323,   323,   323,   323,   323,   323,   323,   323,
     323,   323,   323,   323,   323,   323,   323,   323,   323,   323,
     323,   324,   324,   324,   324,   325,   325,   325,   325,   325,
     325,   326,   326,   326,   326,   327,   327,   328,   328,   329,
     330,   331,   332,   332,   332,   333,   333,   333,   333,   334,
     335,   336,   336,   336,   336,   336,   337,   338,   338,   338,
     338,   338,   338,   338,   338,   338,   338,   339,   339,   340,
     340,   341,   342,   343,   343,   343,   343,   344,   344,   344,
     344,   345,   345,   346,   346,   346,   346,   347,   347,   348,
     349,   349,   349,   349,   350,   350,   351,   352,   352,   352,
     353,   353,   353,   354,   354,   354,   355,   356,   357,   357,
     358,   358,   359,   359,   359,   360,   360,   361,   361,   361,
     361,   362,   362,   363,   363,   364,   364,   365,   366,   367,
     368,   368,   368,   368,   368,   369,   370,   370,   370,   370,
     370,   370,   370,   370,   371,   371,   371,   371,   371,   371,
     371,   371,   371,   371,   371,   371,   371,   371,   371,   371,
     371,   371,   371,   371,   371,   371,   371,   371,   371,   371,
     371,   371,   371,   371,   371,   371,   371,   371,   371,   371,
     371,   371,   371,   371,   371,   371,   371,   371,   371,   371,
     372,   372
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
       1,     1,     1,     1,     2,     4,     2,     1,     1,     3,
       2,     3,     2,     2,     3,     3,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     2,     1,     1,     1,     1,
       0,     2,     0,     1,     2,     5,     3,     1,     3,     1,
       1,     1,     1,     1,     1,     2,     1,     1,     1,     1,
       1,     1,     2,     2,     1,     2,     2,     4,     4,     2,
       4,     0,     2,     0,     2,     3,     3,     5,     1,     1,
       3,     5,     7,     1,     3,     1,     1,     1,     1,     1,
       1,     5,     1,     3,     5,     0,     1,     3,     1,     1,
       1,     2,     3,     1,     1,     2,     1,     2,     3,     3,
       1,     1,     1,     0,     3,     1,     3,     3,     5,     2,
       2,     4,     2,     1,     1,     1,     2,     3,     1,     4,
       6,     4,     5,     6,     4,     3,     1,     1,     1,     1,
       2,     3,     5,     6,     6,     6,     6,     6,     7,     6,
       6,     6,     6,     8,     8,     6,     0,     2,     2,     2,
       1,     3,     1,     4,     4,     3,     6,     3,     4,     4,
       6,     3,     1,     3,     0,     3,     3,     3,     3,     3,
       3,     3,     3,     3,     3,     1,     3,     5,     3,     0,
       1,     0,     1,     1,     1,     3,     3,     4,     3,     4,
       1,     2,     1,     2,     1,     2,     2,     3,     0,     1,
       2,     1,     2,     1,     2,     2,     2,     2,     0,     2,
       1,     1,     0,     2,     4,     4,     5,     1,     4,     1,
       1,     0,     1,     1,     1,     0,     1,     3,     3,     2,
       5,     3,     3,     3,     3,     3,     3,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     0,     1,     1,     1,     2,     5,     5,     2,     3,
       3,     0,     2,     4,     4,     0,     2,     0,     2,     2,
       1,     2,     1,     1,     3,     2,     3,     3,     4,     1,
       2,     0,     2,     3,     2,     2,     2,     0,     2,     3,
       2,     3,     2,     3,     3,     4,     2,     1,     2,     2,
       3,     2,     2,     0,     1,     1,     2,     2,     3,     2,
       2,     1,     2,     1,     2,     2,     3,     2,     2,     2,
       0,     1,     3,     5,     2,     3,     2,     0,     1,     2,
       2,     2,     2,     4,     5,     5,     3,     1,     2,     3,
       3,     2,     2,     3,     3,     0,     2,     1,     3,     3,
       3,     0,     1,     1,     3,     2,     3,     2,     2,     1,
       0,     2,     4,     2,     4,     2,     1,     5,     3,     3,
       2,     4,     3,     5,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
       0,     3,   270,   274,   278,   272,   281,   283,   435,     0,
       0,    67,    72,    62,    63,    64,    96,    97,    98,    99,
     101,     0,     0,     0,   288,   288,   288,   288,   288,   288,
     288,   226,   100,   102,   103,   437,     0,     0,   153,     0,
     449,   106,   109,   107,   108,     0,     0,     0,     0,   155,
     156,   157,   158,   159,   160,     0,     0,     0,     0,     0,
     369,   377,   371,   355,   387,     0,   355,   355,   393,   357,
     401,   403,   351,   410,   355,   417,     0,   149,     0,   148,
     129,   143,   143,   127,   128,     0,   117,     0,     0,     0,
       0,   427,     0,     0,     0,     9,     0,     2,     4,     0,
      12,    14,    39,    21,    20,    35,    58,    65,    66,     0,
       0,    36,    59,     0,   104,     0,   119,   120,    24,   123,
     134,   130,   131,   126,   124,   121,   122,    28,    29,    30,
      31,    32,    33,    34,    15,    17,    18,    19,    37,    16,
       0,   209,    41,    42,   112,     0,   305,     0,     0,     0,
       0,    23,    25,    38,   334,   317,   318,   319,   321,   320,
     322,   323,   324,   325,   326,   327,   328,   329,   330,   331,
     332,   333,   335,   336,   337,   338,   339,   340,    22,    26,
      27,    40,   114,   193,   194,   195,     0,     0,   279,   276,
       0,   309,     0,   344,     0,     0,     0,   342,   343,     0,
       0,     0,     0,   488,   489,   490,   493,   466,   464,   467,
     468,   465,   470,   469,   473,   472,   471,   510,   492,   491,
     501,   500,   499,   502,   503,   497,   498,   504,   505,   506,
     507,   508,   509,   494,   495,   496,   474,   475,   476,   477,
     478,   479,   480,   481,   482,   483,   484,   485,   487,   486,
       0,   196,   198,   511,     0,   497,   498,     0,   162,    68,
       0,    71,    69,    60,     0,    73,    61,     0,     0,    90,
       0,     0,   297,     0,     0,   300,   271,     0,   288,   299,
     273,   275,   278,   282,   284,   286,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     437,     0,   441,     0,     0,     0,   285,   345,   447,   448,
     348,     0,    74,    75,   362,   363,   361,   355,   355,   355,
     355,   376,     0,     0,   370,   355,   355,     0,   365,   388,
     360,   359,   355,   389,   391,   394,   395,   392,   400,     0,
     399,   357,   397,   402,   404,   405,   408,     0,   407,   411,
     409,   355,   414,   418,   416,   420,   421,   422,     0,     0,
       0,   125,     0,   141,   141,   139,     0,   132,   133,     0,
       0,   428,   431,   432,     0,     0,    10,     1,     5,     6,
       7,   270,     0,    76,    88,    87,    92,    82,    77,    78,
      80,    79,    83,    81,     0,    93,    51,    52,    55,    53,
      54,    56,   105,   135,    57,    13,   210,     0,   110,   113,
     174,   176,   182,   190,   181,   180,   450,   456,   306,     0,
     450,   189,   192,     0,     0,    49,    48,    50,     0,   116,
     268,   265,     0,   266,   280,   143,     0,   436,   312,   311,
     313,   316,     0,   314,   315,     0,   307,   308,     0,     0,
       0,     0,     0,     0,   165,     0,    70,    94,   349,    89,
      91,    95,     0,     0,   301,   287,   289,   292,   277,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   255,     0,   244,     0,     0,   259,
     259,     0,     0,     0,     0,   211,     0,     0,   230,   227,
      11,     0,     0,   438,   442,   443,   439,   440,   154,   350,
     351,   355,   378,   355,   355,   382,   355,   380,   386,   372,
     374,     0,   375,   366,   355,   367,   356,   390,   396,   358,
     398,   406,   352,     0,   415,   419,   150,     0,   136,   144,
       0,   145,   146,     0,   118,     0,   426,   429,   430,   433,
     434,     8,    86,    84,     0,   177,     0,     0,     0,    43,
     175,     0,     0,   455,     0,     0,     0,   460,     0,   183,
       0,    44,     0,   269,   267,   141,     0,     0,     0,     0,
       0,   497,   498,     0,     0,   197,   208,   207,   205,   206,
     173,   168,   170,   169,     0,     0,   166,   163,     0,     0,
     302,   303,   304,     0,   290,   292,     0,   291,   261,   263,
     264,   244,   244,   244,   244,     0,   261,     0,     0,     0,
       0,     0,     0,   259,   259,     0,     0,     0,   244,   244,
     244,   260,   244,   244,     0,     0,   244,   228,   229,   445,
       0,     0,   364,   379,   383,   355,   384,   381,   373,   368,
       0,     0,   412,     0,     0,     0,     0,   142,   140,   423,
       0,   178,   179,   111,     0,   453,     0,   451,   462,   459,
     458,   191,     0,     0,    44,    45,     0,   115,   147,   310,
     346,   347,   201,   204,     0,     0,     0,   199,   171,     0,
       0,   161,     0,     0,   294,   295,   293,   298,   244,   262,
       0,     0,     0,     0,     0,   258,   246,   247,   249,   250,
     253,   254,   251,   252,   256,   248,   212,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   446,   444,   385,   354,
     353,     0,     0,   151,   137,   138,   424,   425,    85,     0,
       0,     0,     0,   461,     0,   185,   188,     0,     0,     0,
       0,   202,   172,   167,   164,   296,     0,   215,   213,   216,
     217,   261,   245,   221,   222,   219,   220,   225,     0,     0,
       0,     0,     0,     0,   232,     0,     0,   214,   413,     0,
     454,   452,   463,   457,     0,   184,     0,    46,   200,   203,
     218,   257,     0,     0,     0,     0,     0,     0,     0,   259,
       0,   152,   186,    47,     0,     0,     0,   235,   237,     0,
       0,   242,   223,   231,     0,   224,   233,   234,     0,     0,
     238,     0,   239,   259,     0,   243,   241,     0,   236,   240
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    96,    97,    98,    99,   100,   412,   676,   425,   426,
     427,   102,   103,   104,   105,   106,   263,   266,   107,   108,
     384,   395,   385,   386,   109,   110,   111,   112,   113,   114,
     115,   559,   408,   116,   117,   118,   119,   361,   120,   541,
     363,   121,   122,   123,   124,   125,   126,   127,   128,   129,
     130,   131,   132,   133,   257,   595,   596,   409,   410,   411,
     413,   414,   673,   744,   134,   135,   136,   137,   138,   187,
     139,   251,   252,   588,   140,   141,   299,   499,   773,   774,
     776,   810,   811,   627,   486,   630,   698,   631,   142,   143,
     144,   145,   146,   147,   189,   148,   149,   150,   276,   277,
     605,   606,   278,   603,   419,   151,   152,   201,   153,   348,
     328,   342,   154,   331,   155,   316,   156,   157,   158,   324,
     159,   321,   160,   161,   162,   163,   337,   164,   165,   166,
     167,   168,   350,   169,   170,   354,   171,   172,   173,   174,
     175,   176,   177,   202,   178,   503,   504,   505,   179,   180,
     181,   563,   415,   416,   253,   417
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -553
static const yytype_int16 yypact[] =
{
     685,  -553,   -13,  -553,   -33,  -553,  -553,  -553,    72,  2159,
    2864,    82,    66,  -553,  -553,  -553,  -553,  -553,  -553,   -20,
    -553,  2864,  2864,   105,  2447,  2447,  2447,  2447,  2447,  2447,
    2447,   172,  -553,  -553,  -553,  1241,   114,   190,  -553,   -22,
    -553,  -553,  -553,  -553,  -553,   104,  2864,  2864,   123,  -553,
    -553,  -553,  -553,  -553,  -553,    87,    39,    80,    90,   112,
    -553,    33,    -4,    95,   127,   119,   -55,   139,    -5,    23,
     236,   -63,    58,   179,   -54,   198,    97,    54,   308,  -553,
    -553,   320,   320,  -553,  -553,   272,   286,   308,   308,   352,
     359,  -553,   222,   285,   -57,   323,   369,   887,  -553,    21,
    -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,    26,
     163,  -553,  -553,   303,  3003,  1438,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    1438,   331,  -553,  -553,   161,  1059,   309,  2864,  1059,  2864,
     196,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,   -16,   278,  -553,  -553,   -37,   173,   330,  -553,
     327,   298,   165,  -553,    36,   288,    10,  -553,  -553,   332,
     271,   231,   238,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,  -553,  2864,  2864,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    2303,  -553,   178,  -553,   177,  -553,  -553,   180,   181,  -553,
     328,  -553,  -553,  -553,  2864,  -553,  -553,   276,   366,   363,
    2864,   183,  -553,   184,   185,  -553,  -553,   358,  2447,   187,
    -553,  -553,   -33,  -553,  -553,  -553,   317,   191,   192,   193,
     194,   201,   195,   197,   199,   200,   204,   175,   206,    57,
    -553,   205,    55,   318,   324,   268,  -553,  -553,  -553,  -553,
    -553,   305,  -553,  -553,  -553,   274,  -553,   -32,   -35,   -26,
     139,  -553,    -8,   337,  -553,   139,   154,   338,  -553,  -553,
    -553,  -553,   139,  -553,  -553,  -553,   339,  -553,  -553,   340,
    -553,   262,  -553,  -553,  -553,   280,  -553,   282,  -553,   219,
    -553,   139,  -553,   284,  -553,  -553,  -553,  -553,   348,   246,
    2864,  -553,   350,   273,   273,   224,  2864,  -553,  -553,   353,
     354,   293,   294,  -553,   295,   296,  -553,  -553,  -553,  -553,
     397,   -25,   365,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,   367,   237,  -553,  -553,  -553,  -553,
    -553,  -553,    22,  -553,  -553,  -553,  -553,    19,   422,   161,
    -553,  -553,  -553,  -553,  -553,  -553,    81,  1869,  -553,   424,
      81,  -553,  -553,   360,   370,  -553,  -553,  -553,   428,   -15,
    -553,  -553,   -47,  -553,  -553,   320,   245,  -553,  -553,  -553,
    -553,  -553,   379,  -553,  -553,   299,  -553,  -553,   247,   249,
    3142,   251,  2159,  2586,    11,  2864,  -553,  -553,  -553,  -553,
    -553,  -553,  2864,   319,   202,  -553,  -553,  2725,  -553,   252,
      79,    79,    79,    79,   121,   254,   255,   256,   258,   261,
     263,   264,   266,   267,  -553,   269,  -553,    79,    79,    79,
      79,    79,   275,   277,    79,  -553,   393,   396,  -553,  -553,
    -553,   425,   427,  -553,   281,  -553,  -553,  -553,  -553,  -553,
     335,   139,  -553,   139,   -10,  -553,   139,  -553,  -553,  -553,
    -553,   399,  -553,  -553,   139,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,   -71,   344,  -553,  -553,   414,   283,   -12,  -553,
     408,  -553,  -553,   409,  -553,   287,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,   289,  -553,   290,   138,  1059,  -553,
    -553,  1591,  1730,  -553,  2864,  2864,  2864,  -553,  1059,     2,
     410,   485,  1059,  -553,  -553,   273,   413,   382,   297,  2586,
    2586,  2864,  2864,   291,  2586,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,   -53,   -76,  -553,   300,  2864,   292,
    -553,  -553,  -553,   301,  -553,  2725,   302,  -553,    79,  -553,
    -553,  -553,  -553,  -553,  -553,   417,    79,    79,    79,    79,
      79,    79,    79,    79,    79,    79,    79,    -1,  -553,  -553,
    -553,  -553,  -553,  -553,   304,   306,  -553,  -553,  -553,  -553,
     443,    55,  -553,  -553,  -553,   139,  -553,  -553,  -553,  -553,
     361,   362,   307,   322,   426,  2864,  2864,  -553,  -553,     0,
     429,  -553,  -553,  -553,  2864,  -553,  2864,  -553,  -553,  -553,
    2015,  -553,  2864,    84,   485,  -553,   491,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,   310,   311,  2586,  -553,  -553,   430,
      11,  -553,  2864,   312,  -553,  -553,  -553,  -553,  -553,  -553,
       3,     4,     7,    13,   313,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,  -553,  -553,  -553,   201,    27,    28,
      29,    30,    31,    67,   450,    32,  -553,  -553,  -553,  -553,
    -553,   436,   315,  -553,  -553,  -553,  -553,  -553,  -553,   314,
     316,  2864,  2864,  -553,   -60,  -553,  -553,   503,  2864,  2586,
    2586,  -553,  -553,  -553,  -553,  -553,    34,  -553,  -553,  -553,
    -553,    79,  -553,  -553,  -553,  -553,  -553,  -553,   321,   325,
     326,   329,   334,   336,   341,   343,   345,  -553,  -553,   445,
    -553,  -553,  -553,  -553,  2864,  -553,  2864,  -553,  -553,  -553,
    -553,  -553,    79,    79,   -56,   346,   447,   489,    67,    79,
     490,  -553,  -553,  -553,   349,   351,   355,  -553,  -553,   356,
     357,   368,  -553,  -553,    45,  -553,  -553,  -553,    79,    79,
    -553,   447,  -553,    79,   371,  -553,  -553,   372,  -553,  -553
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -553,  -553,  -553,   437,   498,   -23,     6,  -139,  -553,  -553,
    -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,   364,  -553,  -553,  -553,  -553,   265,  -553,  -355,
     -79,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,   432,  -553,  -553,  -152,  -175,  -353,  -553,
    -143,  -134,  -553,  -553,   101,   116,   159,   171,   176,  -553,
    -553,    91,  -553,  -527,   401,  -553,  -553,  -553,  -254,  -553,
    -553,  -274,  -553,  -366,  -169,  -482,  -552,  -445,  -553,  -553,
    -553,  -553,  -553,  -553,   342,  -553,  -553,  -553,   333,   347,
    -553,   -45,  -553,  -553,  -553,  -553,  -553,  -553,  -553,    41,
     -44,   221,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,  -553,
    -553,  -553,  -553,  -553,  -553,  -553,  -553,   -77,  -553,  -553,
     458,   148,  -553,  -145,  -553,    -9
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -511
static const yytype_int16 yytable[] =
{
     254,   258,   420,   364,   422,   421,   101,   429,   632,   542,
    -112,  -113,   268,   269,   655,   279,   279,   279,   279,   279,
     279,   279,   333,   334,   381,   611,   612,   613,   614,   688,
     352,   381,     3,     4,     5,     6,     7,   307,   308,    10,
     716,   101,   628,   629,   757,   758,   633,  -187,   759,   636,
     573,    24,   682,   683,   760,   590,   560,   687,    24,    25,
      26,    27,    28,    29,   705,   182,   379,  -510,   763,   764,
     765,   766,   767,   777,   519,   790,   560,   335,   650,   806,
     358,   344,   736,   183,   184,   185,   338,   373,   438,   381,
     374,   190,   404,   591,   264,   183,   184,   185,   495,   496,
     497,   259,   260,   101,   441,   403,   327,   327,    40,   513,
     345,   501,   511,   502,   265,   383,    24,   405,   516,   396,
     514,   101,   188,   351,   359,   442,   327,   332,   592,   327,
     261,   439,   690,   270,   645,   327,   691,   651,   191,   498,
     192,   712,   713,   262,   737,   375,   101,   520,   784,   322,
     336,   327,   785,   521,   689,   593,   807,   323,   574,   751,
     267,   609,   193,   699,   194,   195,   610,   431,   196,   302,
     432,   699,   706,   707,   708,   709,   710,   711,   197,   198,
     714,   715,   186,   199,   339,   768,   317,   318,   305,   319,
     320,   340,   341,   306,   186,   407,   407,   656,   310,   769,
     770,   771,   346,   772,   560,   309,   347,   717,  -187,   791,
     388,   717,   717,   672,   397,   717,   448,   449,   594,   311,
     678,   717,   788,   789,   312,   389,  -341,   380,  -510,   398,
     407,   555,   557,   382,   313,   717,   717,   717,   717,   717,
     717,   451,   717,   303,   304,   700,   701,   702,   703,   561,
     325,   562,   200,   286,   326,   457,   327,   822,   314,   315,
     823,   461,   718,   719,   720,   330,   721,   722,   390,   279,
     725,   329,   399,   512,   515,   517,   518,   355,   356,   357,
     391,   523,   525,   474,   400,   392,   423,   424,   527,   401,
     287,   288,   289,   290,   291,   292,   293,   294,   295,   296,
     327,   297,   446,   447,   298,   492,   493,   534,   381,     3,
       4,     5,     6,     7,   524,   327,   699,   814,   343,   475,
     476,   477,   478,   349,   479,   480,   481,   482,   483,   484,
     615,   616,   756,   485,   360,    24,    25,    26,    27,    28,
      29,   827,   353,   600,   601,   602,   362,   804,   805,   407,
     662,   538,   367,   368,   365,   366,   575,   544,   280,   281,
     282,   283,   284,   285,   369,   370,   371,   372,   376,   377,
     394,    31,   407,   824,   825,   430,   418,   434,   433,   435,
     436,   437,   440,   444,   443,   445,   452,   453,   458,   456,
     455,   454,   459,   460,   462,   463,   464,   465,   467,   469,
     506,   470,   471,   472,   473,   487,   507,   488,   567,   489,
     490,   500,   508,   556,   491,   663,   494,   509,   510,   522,
     526,   528,   529,   339,   531,   671,   532,   533,   535,   677,
     536,   537,   539,   543,   540,   545,   546,   547,   548,   549,
     550,   583,   551,   254,   589,   554,   597,   552,   558,   553,
     568,   570,   571,   598,   572,   576,   577,   579,   607,   580,
     578,   584,   608,   599,   617,   618,   619,   643,   620,   644,
     646,   621,   647,   622,   623,   637,   624,   625,   638,   626,
     649,   648,   639,   347,   653,   634,   640,   635,   652,   641,
     657,   658,   674,   654,   675,   679,   660,   659,   680,   704,
     681,   686,   661,   726,   694,   729,   730,   732,   733,   692,
     748,   738,   752,   695,   697,   731,   775,   723,   778,   724,
     749,   750,   786,   761,   755,   779,   780,   801,   781,   809,
     812,   815,   792,   301,   378,   747,   793,   794,   753,   746,
     795,   387,   406,   585,   813,   796,   428,   826,   762,   798,
     797,   642,   665,   667,   799,   668,   669,   670,   808,   800,
     696,   816,   530,   817,   727,   818,   819,   393,   569,   820,
     589,   589,   684,   685,     0,   589,   821,     0,     0,     0,
       0,     0,     0,   828,   829,     0,     0,     0,     0,   693,
       0,     0,     0,     0,     0,     0,   607,     0,     0,     0,
       0,   728,     0,   787,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   468,   466,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   803,     0,     0,     0,     0,   734,   735,     0,     0,
       0,     0,     0,     0,     0,   739,     0,   740,     0,     0,
       0,   743,     0,   745,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   589,     0,     0,
       0,     0,     0,   754,     0,     0,     1,     0,     0,     0,
       2,     3,     4,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,     0,     0,    16,    17,    18,    19,
      20,     0,     0,    21,    22,     0,    23,    24,    25,    26,
      27,    28,    29,    30,     0,    31,     0,     0,     0,     0,
       0,     0,   782,   783,    32,    33,    34,     0,    35,    36,
     589,   589,     0,     0,     0,     0,     0,    37,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    38,    39,     0,    40,    41,    42,
      43,    44,     0,    45,     0,   802,     0,     0,     0,     0,
       0,    46,     0,    47,    48,    49,    50,    51,    52,    53,
      54,     0,     0,     0,     0,     0,     0,     0,    55,    56,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    57,    58,     0,     0,     0,     0,     0,     0,
      59,     0,     0,     0,     0,    60,    61,    62,     0,     0,
       0,     0,     0,    63,     0,     0,     0,    64,    65,    66,
      67,    68,    69,     0,     0,     0,    70,    71,     0,    72,
      73,    74,     0,    75,    76,     0,     0,     0,    77,    78,
       0,    79,    80,    81,    82,    83,    84,    85,    86,    87,
      88,    89,    90,    91,    92,    93,    94,     0,     0,     0,
       0,    95,     2,     3,     4,     5,     6,     7,     8,     9,
      10,    11,    12,    13,    14,    15,     0,     0,    16,    17,
      18,    19,    20,     0,     0,    21,    22,     0,    23,    24,
      25,    26,    27,    28,    29,    30,     0,    31,     0,     0,
       0,     0,     0,     0,     0,     0,    32,    33,    34,     0,
      35,    36,     0,     0,     0,     0,     0,     0,     0,    37,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    38,    39,     0,    40,
      41,    42,    43,    44,     0,    45,     0,     0,     0,     0,
       0,     0,     0,    46,     0,    47,    48,    49,    50,    51,
      52,    53,    54,     0,     0,     0,     0,     0,     0,     0,
      55,    56,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    57,    58,     0,     0,     0,     0,
       0,     0,    59,     0,     0,     0,     0,    60,    61,    62,
       0,     0,     0,     0,     0,    63,     0,     0,     0,    64,
      65,    66,    67,    68,    69,     0,     0,     0,    70,    71,
       0,    72,    73,    74,   381,    75,    76,     0,     0,     0,
      77,    78,     0,    79,    80,    81,    82,    83,    84,    85,
      86,    87,    88,    89,    90,    91,    92,    93,    94,     0,
       0,    24,     0,    95,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   203,   204,
     205,   206,   207,   208,   209,     0,     0,     0,   210,     0,
       0,   211,     0,   212,   213,   214,   215,   216,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   217,     0,     0,     0,     0,     0,     0,   218,   219,
       0,     0,   220,   221,   222,   223,   224,     0,     0,     0,
       0,     0,     0,     0,     0,   255,   256,     0,     0,     0,
     227,   228,   229,   230,   231,   232,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     233,   234,   235,     0,     0,   236,   237,     0,   238,     0,
       0,     0,   239,   240,     0,   241,     0,     0,   242,   243,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   244,     0,     0,     0,   245,     0,     0,     0,
     246,   247,     0,     0,   248,     0,     2,     3,     4,     5,
       6,     7,     8,     9,    10,    11,    12,    13,    14,    15,
       0,   249,    16,    17,    18,    19,    20,     0,     0,    21,
      22,     0,    23,    24,    25,    26,    27,    28,    29,    30,
       0,    31,     0,     0,     0,     0,     0,     0,     0,     0,
      32,    33,    34,     0,   300,    36,     0,     0,     0,     0,
       0,     0,     0,    37,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      38,    39,     0,    40,    41,    42,    43,    44,     0,    45,
       0,     0,     0,     0,     0,     0,     0,    46,     0,    47,
      48,    49,    50,    51,    52,    53,    54,     0,     0,     0,
       0,     0,     0,     0,    55,    56,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    57,    58,
       0,     0,     0,     0,     0,     0,    59,     0,     0,     0,
       0,    60,    61,    62,     0,     0,     0,     0,     0,    63,
       0,     0,     0,    64,    65,    66,    67,    68,    69,     0,
       0,     0,    70,    71,     0,    72,    73,    74,     0,    75,
      76,     0,     0,     0,    77,    78,     0,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,     2,     3,     4,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,     0,     0,    16,
      17,    18,    19,    20,     0,     0,    21,    22,     0,    23,
      24,    25,    26,    27,    28,    29,    30,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    32,    33,    34,
       0,   300,    36,     0,     0,     0,     0,     0,     0,     0,
      37,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    38,    39,     0,
      40,    41,    42,    43,    44,     0,    45,     0,     0,     0,
       0,     0,     0,     0,    46,     0,    47,    48,    49,    50,
      51,    52,    53,    54,     0,     0,     0,     0,     0,     0,
       0,    55,    56,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    57,    58,     0,     0,     0,
       0,     0,     0,    59,     0,     0,     0,     0,    60,    61,
      62,     0,     0,     0,     0,     0,    63,     0,     0,     0,
      64,    65,    66,    67,    68,    69,     0,     0,     0,    70,
      71,     0,    72,    73,    74,     0,    75,    76,     0,     0,
       0,    77,    78,     0,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
     203,   204,   205,   206,   207,   208,   209,     0,     0,     0,
     210,     0,     0,   211,     0,   212,   213,   214,   215,   216,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   217,     0,     0,     0,     0,     0,     0,
     218,   219,     0,     0,   220,   221,   222,   223,   224,     0,
       0,     0,     0,     0,     0,     0,     0,   255,   256,     0,
       0,     0,   227,   228,   229,   230,   231,   232,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   233,   234,   235,     0,     0,   236,   237,     0,
     238,     0,     0,     0,   239,   240,     0,   241,     0,     0,
     242,   243,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   244,     0,     0,     0,   245,     0,
       0,     0,   246,   247,     0,     0,   248,     0,     0,   203,
     204,   205,   206,   207,   208,   209,     0,     0,     0,   210,
       0,     0,   211,   249,   212,   213,   214,   215,   216,     0,
       0,     0,   664,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   217,     0,     0,     0,     0,     0,     0,   218,
     219,     0,     0,   220,   221,   222,   223,   224,     0,     0,
       0,     0,     0,     0,     0,     0,   255,   256,     0,     0,
       0,   227,   228,   229,   230,   231,   232,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   233,   234,   235,     0,     0,   236,   237,     0,   238,
       0,     0,     0,   239,   240,     0,   241,     0,     0,   242,
     243,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   244,     0,     0,     0,   245,     0,     0,
       0,   246,   247,     0,     0,   248,     0,     0,   203,   204,
     205,   206,   207,   208,   209,     0,     0,     0,   210,     0,
       0,   211,   249,   212,   213,   214,   215,   216,   564,     0,
       0,   666,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   217,     0,     0,     0,     0,     0,     0,   218,   219,
       0,     0,   220,   221,   222,   223,   224,     0,     0,     0,
       0,     0,     0,     0,     0,   255,   256,     0,     0,     0,
     227,   228,   229,   230,   231,   232,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     233,   234,   235,     0,     0,   236,   237,     0,   238,     0,
       0,     0,   239,   240,     0,   241,     0,     0,   242,   243,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   244,     0,     0,     0,   245,     0,     0,     0,
     246,   247,     0,     0,   248,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   203,   204,   205,   206,   207,   208,
     209,   249,     0,     0,   210,     0,   565,   211,   566,   212,
     213,   214,   215,   216,   741,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   217,     0,     0,
       0,     0,     0,     0,   218,   219,     0,     0,   220,   221,
     222,   223,   224,     0,     0,     0,     0,     0,     0,     0,
       0,   255,   256,     0,     0,     0,   227,   228,   229,   230,
     231,   232,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   233,   234,   235,     0,
       0,   236,   237,     0,   238,     0,     0,     0,   239,   240,
       0,   241,     0,     0,   242,   243,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   244,     0,
       0,     0,   245,     0,     0,     0,   246,   247,     0,     0,
     248,     0,     0,     0,     0,     0,     0,     0,   203,   204,
     205,   206,   207,   208,   209,     0,     0,   249,   210,     0,
       0,   211,   742,   212,   213,   214,   215,   216,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   217,     0,     0,     0,     0,     0,     0,   218,   219,
       0,     0,   220,   221,   222,   223,   224,     0,     0,     0,
       0,     0,     0,     0,     0,   225,   226,     0,     0,     0,
     227,   228,   229,   230,   231,   232,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     233,   234,   235,     0,     0,   236,   237,     0,   238,     0,
       0,     0,   239,   240,     0,   241,     0,     0,   242,   243,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   244,     0,     0,     0,   245,     0,     0,     0,
     246,   247,     0,     0,   248,     0,     0,     0,     0,     0,
       0,     0,   203,   204,   205,   206,   207,   208,   209,     0,
       0,   249,   210,     0,     0,   211,   250,   212,   213,   214,
     215,   216,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   217,     0,     0,     0,     0,
       0,     0,   218,   219,     0,     0,   220,   221,   222,   223,
     224,     0,     0,     0,     0,     0,     0,     0,     0,   255,
     256,     0,     0,     0,   227,   228,   229,   230,   231,   232,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   233,   234,   235,     0,     0,   236,
     237,     0,   238,     0,     0,     0,   239,   240,     0,   241,
       0,     0,   242,   243,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   244,     0,     0,     0,
     245,     0,     0,     0,   246,   247,     0,     0,   248,     0,
       0,     0,     0,     0,     0,     0,   203,   204,   205,   206,
     207,   208,   209,     0,     0,   249,   210,     0,   271,   211,
     450,   212,   213,   214,   215,   216,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   217,
       0,     0,     0,     0,     0,     0,   218,   219,     0,     0,
     220,   221,   222,   223,   224,     0,     0,     0,     0,     0,
       0,     0,     0,   255,   256,     0,     0,     0,   227,   228,
     229,   230,   231,   232,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   272,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   273,   274,   233,   234,
     235,   275,     0,   236,   237,     0,   238,     0,     0,     0,
     239,   240,     0,   241,     0,     0,   242,   243,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     244,     0,     0,     0,   245,     0,     0,     0,   246,   247,
       0,     0,   248,     0,     0,   203,   204,   205,   206,   207,
     208,   209,     0,     0,     0,   210,     0,     0,   211,   249,
     212,   213,   214,   215,   216,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   217,     0,
       0,     0,     0,     0,     0,   218,   219,     0,     0,   220,
     221,   222,   223,   224,     0,     0,     0,     0,     0,     0,
       0,     0,   255,   256,     0,     0,     0,   227,   228,   229,
     230,   231,   232,   586,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   233,   234,   235,
     587,     0,   236,   237,     0,   238,     0,     0,     0,   239,
     240,     0,   241,     0,     0,   242,   243,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   244,
       0,     0,     0,   245,     0,     0,     0,   246,   247,     0,
       0,   248,     0,     0,   203,   204,   205,   206,   207,   208,
     209,     0,     0,     0,   210,     0,     0,   211,   249,   212,
     213,   214,   215,   216,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   217,     0,     0,
       0,     0,     0,     0,   218,   219,     0,     0,   220,   221,
     222,   223,   224,     0,     0,     0,     0,     0,     0,     0,
       0,   255,   256,     0,     0,     0,   227,   228,   229,   230,
     231,   232,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   233,   234,   235,   604,
       0,   236,   237,     0,   238,     0,     0,     0,   239,   240,
       0,   241,     0,     0,   242,   243,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   244,     0,
       0,     0,   245,     0,     0,     0,   246,   247,     0,     0,
     248,     0,     0,   203,   204,   205,   206,   207,   208,   209,
       0,     0,     0,   210,     0,     0,   211,   249,   212,   213,
     214,   215,   216,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   217,     0,     0,     0,
       0,     0,     0,   218,   219,     0,     0,   220,   221,   222,
     223,   224,     0,     0,     0,     0,     0,     0,     0,     0,
     255,   256,     0,     0,     0,   227,   228,   229,   230,   231,
     232,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   233,   234,   235,     0,     0,
     236,   237,     0,   238,     0,     0,     0,   239,   240,     0,
     241,     0,     0,   242,   243,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   244,     0,     0,
       0,   245,     0,     0,     0,   246,   247,     0,     0,   248,
       0,     0,   203,   204,   205,   206,   207,   208,   209,     0,
       0,     0,   210,     0,     0,   211,   249,   212,   213,   214,
     215,   216,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   402,     0,     0,     0,     0,
       0,     0,   218,   219,     0,     0,   220,   221,   222,   223,
     224,     0,     0,     0,     0,     0,     0,     0,     0,   255,
     256,     0,     0,     0,   227,   228,   229,   230,   231,   232,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   233,   234,   235,     0,     0,   236,
     237,     0,   238,     0,     0,     0,   239,   240,     0,   241,
       0,     0,   242,   243,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   244,     0,     0,     0,
     245,     0,     0,     0,   246,   247,     0,     0,   248,     0,
       0,   203,   204,   205,   206,   207,   208,   209,     0,     0,
       0,   210,     0,     0,   211,   249,   212,   213,   214,   215,
     216,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   217,     0,     0,     0,     0,     0,
       0,   218,   219,     0,     0,   220,   221,   222,   223,   224,
       0,     0,     0,     0,     0,     0,     0,     0,   581,   582,
       0,     0,     0,   227,   228,   229,   230,   231,   232,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   233,   234,   235,     0,     0,   236,   237,
       0,   238,     0,     0,     0,   239,   240,     0,   241,     0,
       0,   242,   243,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   244,     0,     0,     0,   245,
       0,     0,     0,   246,   247,     0,     0,   248,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   249
};

static const yytype_int16 yycheck[] =
{
       9,    10,   147,    82,   149,   148,     0,   182,   490,   364,
      26,    26,    21,    22,    26,    24,    25,    26,    27,    28,
      29,    30,    66,    67,     5,   470,   471,   472,   473,    82,
      74,     5,     6,     7,     8,     9,    10,    46,    47,    13,
      41,    35,   487,   488,    41,    41,   491,    45,    41,   494,
      97,    32,   579,   580,    41,    44,   409,   584,    32,    33,
      34,    35,    36,    37,   616,    78,    45,    45,    41,    41,
      41,    41,    41,    41,    82,    41,   429,    82,   149,   135,
      26,   144,    82,   108,   109,   110,    63,   144,    52,     5,
     147,    19,   115,    82,    28,   108,   109,   110,    41,    42,
      43,    19,    20,    97,    94,   114,   161,   161,    82,   144,
     173,    56,   144,    58,    48,   109,    32,   140,   144,   113,
     155,   115,   155,   177,    70,   115,   161,   182,   117,   161,
      48,    95,   208,    28,   144,   161,   212,   208,    66,    82,
      68,   623,   624,    61,   144,   202,   140,   155,   208,   153,
     155,   161,   212,   161,   207,   144,   212,   161,   205,   686,
     180,    82,    90,   608,    92,    93,    87,   204,    96,    55,
     207,   616,   617,   618,   619,   620,   621,   622,   106,   107,
     625,   626,   207,   111,   161,   118,   153,   154,   210,   156,
     157,   168,   169,    89,   207,   211,   211,   209,   111,   132,
     133,   134,   144,   136,   557,    82,   148,   208,   206,   761,
     109,   208,   208,   211,   113,   208,   225,   226,   207,   180,
     575,   208,   749,   750,   144,   109,   154,   206,   206,   113,
     211,   212,   407,   207,   144,   208,   208,   208,   208,   208,
     208,   250,   208,    53,    54,   611,   612,   613,   614,   168,
     155,   170,   180,    81,   159,   264,   161,   212,   146,   147,
     215,   270,   628,   629,   630,   146,   632,   633,   109,   278,
     636,   144,   113,   317,   318,   319,   320,   180,   181,   182,
     109,   325,   326,    82,   113,   109,    90,    91,   332,   113,
     118,   119,   120,   121,   122,   123,   124,   125,   126,   127,
     161,   129,    64,    65,   132,   130,   131,   351,     5,     6,
       7,     8,     9,    10,   160,   161,   761,   799,    82,   118,
     119,   120,   121,   144,   123,   124,   125,   126,   127,   128,
     209,   210,   698,   132,    26,    32,    33,    34,    35,    36,
      37,   823,   144,   141,   142,   143,    26,   792,   793,   211,
     212,   360,    87,    88,    82,    69,   435,   366,    25,    26,
      27,    28,    29,    30,    12,     6,   144,    82,    45,     0,
     207,    40,   211,   818,   819,    97,    67,    47,   205,    52,
      82,   216,    94,   112,    52,   154,   208,   210,   112,    61,
     209,   211,    26,    30,   211,   211,   211,    39,   211,    82,
      82,   210,   210,   210,   210,   210,    82,   210,   417,   210,
     210,   206,   144,   407,   210,   558,   210,   112,   144,    82,
      82,    82,    82,   161,   144,   568,   144,   208,   144,   572,
      82,   185,    82,   209,   161,    82,    82,   144,   144,   144,
     144,   450,    45,   452,   453,   208,   455,    82,    26,    82,
      26,    91,    82,   462,    26,   210,    77,   210,   467,   210,
     161,   210,   210,   144,   210,   210,   210,   511,   210,   513,
     514,   210,   516,   210,   210,    82,   210,   210,    82,   210,
     524,    82,    57,   148,    70,   210,    59,   210,   144,   208,
      82,    82,    82,   210,     9,    82,   207,   210,   116,    82,
     203,   210,   212,    60,   212,   144,   144,   185,    82,   209,
      19,    82,    82,   212,   212,   208,    66,   213,    82,   213,
     210,   210,    19,   210,   212,   210,   212,    82,   212,    82,
      41,    41,   211,    35,    97,   674,   211,   211,   690,   673,
     211,   109,   141,   452,   798,   211,   182,   821,   717,   208,
     214,   510,   561,   562,   211,   564,   565,   566,   212,   214,
     605,   212,   341,   212,   641,   210,   210,   109,   420,   212,
     579,   580,   581,   582,    -1,   584,   208,    -1,    -1,    -1,
      -1,    -1,    -1,   212,   212,    -1,    -1,    -1,    -1,   598,
      -1,    -1,    -1,    -1,    -1,    -1,   605,    -1,    -1,    -1,
      -1,   645,    -1,   748,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   282,   278,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   786,    -1,    -1,    -1,    -1,   655,   656,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   664,    -1,   666,    -1,    -1,
      -1,   670,    -1,   672,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   686,    -1,    -1,
      -1,    -1,    -1,   692,    -1,    -1,     1,    -1,    -1,    -1,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    -1,    -1,    21,    22,    23,    24,
      25,    -1,    -1,    28,    29,    -1,    31,    32,    33,    34,
      35,    36,    37,    38,    -1,    40,    -1,    -1,    -1,    -1,
      -1,    -1,   741,   742,    49,    50,    51,    -1,    53,    54,
     749,   750,    -1,    -1,    -1,    -1,    -1,    62,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    79,    80,    -1,    82,    83,    84,
      85,    86,    -1,    88,    -1,   784,    -1,    -1,    -1,    -1,
      -1,    96,    -1,    98,    99,   100,   101,   102,   103,   104,
     105,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   113,   114,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   137,   138,    -1,    -1,    -1,    -1,    -1,    -1,
     145,    -1,    -1,    -1,    -1,   150,   151,   152,    -1,    -1,
      -1,    -1,    -1,   158,    -1,    -1,    -1,   162,   163,   164,
     165,   166,   167,    -1,    -1,    -1,   171,   172,    -1,   174,
     175,   176,    -1,   178,   179,    -1,    -1,    -1,   183,   184,
      -1,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,   200,   201,    -1,    -1,    -1,
      -1,   206,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    -1,    -1,    21,    22,
      23,    24,    25,    -1,    -1,    28,    29,    -1,    31,    32,
      33,    34,    35,    36,    37,    38,    -1,    40,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    49,    50,    51,    -1,
      53,    54,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    62,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    79,    80,    -1,    82,
      83,    84,    85,    86,    -1,    88,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    96,    -1,    98,    99,   100,   101,   102,
     103,   104,   105,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     113,   114,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   137,   138,    -1,    -1,    -1,    -1,
      -1,    -1,   145,    -1,    -1,    -1,    -1,   150,   151,   152,
      -1,    -1,    -1,    -1,    -1,   158,    -1,    -1,    -1,   162,
     163,   164,   165,   166,   167,    -1,    -1,    -1,   171,   172,
      -1,   174,   175,   176,     5,   178,   179,    -1,    -1,    -1,
     183,   184,    -1,   186,   187,   188,   189,   190,   191,   192,
     193,   194,   195,   196,   197,   198,   199,   200,   201,    -1,
      -1,    32,    -1,   206,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    49,    50,
      51,    52,    53,    54,    55,    -1,    -1,    -1,    59,    -1,
      -1,    62,    -1,    64,    65,    66,    67,    68,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,    89,    90,
      -1,    -1,    93,    94,    95,    96,    97,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   106,   107,    -1,    -1,    -1,
     111,   112,   113,   114,   115,   116,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     141,   142,   143,    -1,    -1,   146,   147,    -1,   149,    -1,
      -1,    -1,   153,   154,    -1,   156,    -1,    -1,   159,   160,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   173,    -1,    -1,    -1,   177,    -1,    -1,    -1,
     181,   182,    -1,    -1,   185,    -1,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,    18,
      -1,   202,    21,    22,    23,    24,    25,    -1,    -1,    28,
      29,    -1,    31,    32,    33,    34,    35,    36,    37,    38,
      -1,    40,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      49,    50,    51,    -1,    53,    54,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    62,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      79,    80,    -1,    82,    83,    84,    85,    86,    -1,    88,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    96,    -1,    98,
      99,   100,   101,   102,   103,   104,   105,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   113,   114,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   137,   138,
      -1,    -1,    -1,    -1,    -1,    -1,   145,    -1,    -1,    -1,
      -1,   150,   151,   152,    -1,    -1,    -1,    -1,    -1,   158,
      -1,    -1,    -1,   162,   163,   164,   165,   166,   167,    -1,
      -1,    -1,   171,   172,    -1,   174,   175,   176,    -1,   178,
     179,    -1,    -1,    -1,   183,   184,    -1,   186,   187,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   200,   201,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,    16,    17,    18,    -1,    -1,    21,
      22,    23,    24,    25,    -1,    -1,    28,    29,    -1,    31,
      32,    33,    34,    35,    36,    37,    38,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    49,    50,    51,
      -1,    53,    54,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      62,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    79,    80,    -1,
      82,    83,    84,    85,    86,    -1,    88,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    96,    -1,    98,    99,   100,   101,
     102,   103,   104,   105,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   113,   114,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   137,   138,    -1,    -1,    -1,
      -1,    -1,    -1,   145,    -1,    -1,    -1,    -1,   150,   151,
     152,    -1,    -1,    -1,    -1,    -1,   158,    -1,    -1,    -1,
     162,   163,   164,   165,   166,   167,    -1,    -1,    -1,   171,
     172,    -1,   174,   175,   176,    -1,   178,   179,    -1,    -1,
      -1,   183,   184,    -1,   186,   187,   188,   189,   190,   191,
     192,   193,   194,   195,   196,   197,   198,   199,   200,   201,
      49,    50,    51,    52,    53,    54,    55,    -1,    -1,    -1,
      59,    -1,    -1,    62,    -1,    64,    65,    66,    67,    68,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,
      89,    90,    -1,    -1,    93,    94,    95,    96,    97,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,   107,    -1,
      -1,    -1,   111,   112,   113,   114,   115,   116,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   141,   142,   143,    -1,    -1,   146,   147,    -1,
     149,    -1,    -1,    -1,   153,   154,    -1,   156,    -1,    -1,
     159,   160,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   173,    -1,    -1,    -1,   177,    -1,
      -1,    -1,   181,   182,    -1,    -1,   185,    -1,    -1,    49,
      50,    51,    52,    53,    54,    55,    -1,    -1,    -1,    59,
      -1,    -1,    62,   202,    64,    65,    66,    67,    68,    -1,
      -1,    -1,   211,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,    89,
      90,    -1,    -1,    93,    94,    95,    96,    97,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   106,   107,    -1,    -1,
      -1,   111,   112,   113,   114,   115,   116,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   141,   142,   143,    -1,    -1,   146,   147,    -1,   149,
      -1,    -1,    -1,   153,   154,    -1,   156,    -1,    -1,   159,
     160,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   173,    -1,    -1,    -1,   177,    -1,    -1,
      -1,   181,   182,    -1,    -1,   185,    -1,    -1,    49,    50,
      51,    52,    53,    54,    55,    -1,    -1,    -1,    59,    -1,
      -1,    62,   202,    64,    65,    66,    67,    68,    69,    -1,
      -1,   211,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,    89,    90,
      -1,    -1,    93,    94,    95,    96,    97,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   106,   107,    -1,    -1,    -1,
     111,   112,   113,   114,   115,   116,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     141,   142,   143,    -1,    -1,   146,   147,    -1,   149,    -1,
      -1,    -1,   153,   154,    -1,   156,    -1,    -1,   159,   160,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   173,    -1,    -1,    -1,   177,    -1,    -1,    -1,
     181,   182,    -1,    -1,   185,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    49,    50,    51,    52,    53,    54,
      55,   202,    -1,    -1,    59,    -1,   207,    62,   209,    64,
      65,    66,    67,    68,    69,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,
      -1,    -1,    -1,    -1,    89,    90,    -1,    -1,    93,    94,
      95,    96,    97,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   106,   107,    -1,    -1,    -1,   111,   112,   113,   114,
     115,   116,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   141,   142,   143,    -1,
      -1,   146,   147,    -1,   149,    -1,    -1,    -1,   153,   154,
      -1,   156,    -1,    -1,   159,   160,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   173,    -1,
      -1,    -1,   177,    -1,    -1,    -1,   181,   182,    -1,    -1,
     185,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    49,    50,
      51,    52,    53,    54,    55,    -1,    -1,   202,    59,    -1,
      -1,    62,   207,    64,    65,    66,    67,    68,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,    89,    90,
      -1,    -1,    93,    94,    95,    96,    97,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   106,   107,    -1,    -1,    -1,
     111,   112,   113,   114,   115,   116,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     141,   142,   143,    -1,    -1,   146,   147,    -1,   149,    -1,
      -1,    -1,   153,   154,    -1,   156,    -1,    -1,   159,   160,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   173,    -1,    -1,    -1,   177,    -1,    -1,    -1,
     181,   182,    -1,    -1,   185,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    49,    50,    51,    52,    53,    54,    55,    -1,
      -1,   202,    59,    -1,    -1,    62,   207,    64,    65,    66,
      67,    68,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,
      -1,    -1,    89,    90,    -1,    -1,    93,    94,    95,    96,
      97,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,
     107,    -1,    -1,    -1,   111,   112,   113,   114,   115,   116,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   141,   142,   143,    -1,    -1,   146,
     147,    -1,   149,    -1,    -1,    -1,   153,   154,    -1,   156,
      -1,    -1,   159,   160,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   173,    -1,    -1,    -1,
     177,    -1,    -1,    -1,   181,   182,    -1,    -1,   185,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    49,    50,    51,    52,
      53,    54,    55,    -1,    -1,   202,    59,    -1,    61,    62,
     207,    64,    65,    66,    67,    68,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,
      -1,    -1,    -1,    -1,    -1,    -1,    89,    90,    -1,    -1,
      93,    94,    95,    96,    97,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   106,   107,    -1,    -1,    -1,   111,   112,
     113,   114,   115,   116,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   128,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   139,   140,   141,   142,
     143,   144,    -1,   146,   147,    -1,   149,    -1,    -1,    -1,
     153,   154,    -1,   156,    -1,    -1,   159,   160,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     173,    -1,    -1,    -1,   177,    -1,    -1,    -1,   181,   182,
      -1,    -1,   185,    -1,    -1,    49,    50,    51,    52,    53,
      54,    55,    -1,    -1,    -1,    59,    -1,    -1,    62,   202,
      64,    65,    66,    67,    68,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,    -1,
      -1,    -1,    -1,    -1,    -1,    89,    90,    -1,    -1,    93,
      94,    95,    96,    97,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   106,   107,    -1,    -1,    -1,   111,   112,   113,
     114,   115,   116,   117,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   141,   142,   143,
     144,    -1,   146,   147,    -1,   149,    -1,    -1,    -1,   153,
     154,    -1,   156,    -1,    -1,   159,   160,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   173,
      -1,    -1,    -1,   177,    -1,    -1,    -1,   181,   182,    -1,
      -1,   185,    -1,    -1,    49,    50,    51,    52,    53,    54,
      55,    -1,    -1,    -1,    59,    -1,    -1,    62,   202,    64,
      65,    66,    67,    68,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,
      -1,    -1,    -1,    -1,    89,    90,    -1,    -1,    93,    94,
      95,    96,    97,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   106,   107,    -1,    -1,    -1,   111,   112,   113,   114,
     115,   116,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   141,   142,   143,   144,
      -1,   146,   147,    -1,   149,    -1,    -1,    -1,   153,   154,
      -1,   156,    -1,    -1,   159,   160,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   173,    -1,
      -1,    -1,   177,    -1,    -1,    -1,   181,   182,    -1,    -1,
     185,    -1,    -1,    49,    50,    51,    52,    53,    54,    55,
      -1,    -1,    -1,    59,    -1,    -1,    62,   202,    64,    65,
      66,    67,    68,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,    -1,
      -1,    -1,    -1,    89,    90,    -1,    -1,    93,    94,    95,
      96,    97,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     106,   107,    -1,    -1,    -1,   111,   112,   113,   114,   115,
     116,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   141,   142,   143,    -1,    -1,
     146,   147,    -1,   149,    -1,    -1,    -1,   153,   154,    -1,
     156,    -1,    -1,   159,   160,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   173,    -1,    -1,
      -1,   177,    -1,    -1,    -1,   181,   182,    -1,    -1,   185,
      -1,    -1,    49,    50,    51,    52,    53,    54,    55,    -1,
      -1,    -1,    59,    -1,    -1,    62,   202,    64,    65,    66,
      67,    68,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,
      -1,    -1,    89,    90,    -1,    -1,    93,    94,    95,    96,
      97,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,
     107,    -1,    -1,    -1,   111,   112,   113,   114,   115,   116,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   141,   142,   143,    -1,    -1,   146,
     147,    -1,   149,    -1,    -1,    -1,   153,   154,    -1,   156,
      -1,    -1,   159,   160,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   173,    -1,    -1,    -1,
     177,    -1,    -1,    -1,   181,   182,    -1,    -1,   185,    -1,
      -1,    49,    50,    51,    52,    53,    54,    55,    -1,    -1,
      -1,    59,    -1,    -1,    62,   202,    64,    65,    66,    67,
      68,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,    -1,
      -1,    89,    90,    -1,    -1,    93,    94,    95,    96,    97,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,   107,
      -1,    -1,    -1,   111,   112,   113,   114,   115,   116,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   141,   142,   143,    -1,    -1,   146,   147,
      -1,   149,    -1,    -1,    -1,   153,   154,    -1,   156,    -1,
      -1,   159,   160,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   173,    -1,    -1,    -1,   177,
      -1,    -1,    -1,   181,   182,    -1,    -1,   185,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   202
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     1,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    21,    22,    23,    24,
      25,    28,    29,    31,    32,    33,    34,    35,    36,    37,
      38,    40,    49,    50,    51,    53,    54,    62,    79,    80,
      82,    83,    84,    85,    86,    88,    96,    98,    99,   100,
     101,   102,   103,   104,   105,   113,   114,   137,   138,   145,
     150,   151,   152,   158,   162,   163,   164,   165,   166,   167,
     171,   172,   174,   175,   176,   178,   179,   183,   184,   186,
     187,   188,   189,   190,   191,   192,   193,   194,   195,   196,
     197,   198,   199,   200,   201,   206,   218,   219,   220,   221,
     222,   223,   228,   229,   230,   231,   232,   235,   236,   241,
     242,   243,   244,   245,   246,   247,   250,   251,   252,   253,
     255,   258,   259,   260,   261,   262,   263,   264,   265,   266,
     267,   268,   269,   270,   281,   282,   283,   284,   285,   287,
     291,   292,   305,   306,   307,   308,   309,   310,   312,   313,
     314,   322,   323,   325,   329,   331,   333,   334,   335,   337,
     339,   340,   341,   342,   344,   345,   346,   347,   348,   350,
     351,   353,   354,   355,   356,   357,   358,   359,   361,   365,
     366,   367,    78,   108,   109,   110,   207,   286,   155,   311,
      19,    66,    68,    90,    92,    93,    96,   106,   107,   111,
     180,   324,   360,    49,    50,    51,    52,    53,    54,    55,
      59,    62,    64,    65,    66,    67,    68,    82,    89,    90,
      93,    94,    95,    96,    97,   106,   107,   111,   112,   113,
     114,   115,   116,   141,   142,   143,   146,   147,   149,   153,
     154,   156,   159,   160,   173,   177,   181,   182,   185,   202,
     207,   288,   289,   371,   372,   106,   107,   271,   372,    19,
      20,    48,    61,   233,    28,    48,   234,   180,   372,   372,
      28,    61,   128,   139,   140,   144,   315,   316,   319,   372,
     315,   315,   315,   315,   315,   315,    81,   118,   119,   120,
     121,   122,   123,   124,   125,   126,   127,   129,   132,   293,
      53,   221,    55,    53,    54,   210,    89,   372,   372,    82,
     111,   180,   144,   144,   146,   147,   332,   153,   154,   156,
     157,   338,   153,   161,   336,   155,   159,   161,   327,   144,
     146,   330,   182,   327,   327,    82,   155,   343,    63,   161,
     168,   169,   328,    82,   144,   173,   144,   148,   326,   144,
     349,   177,   327,   144,   352,   180,   181,   182,    26,    70,
      26,   254,    26,   257,   257,    82,    69,   254,   254,    12,
       6,   144,    82,   144,   147,   202,    45,     0,   220,    45,
     206,     5,   207,   223,   237,   239,   240,   270,   281,   282,
     283,   284,   285,   367,   207,   238,   223,   281,   282,   283,
     284,   285,    82,   372,   222,   222,   291,   211,   249,   274,
     275,   276,   223,   277,   278,   369,   370,   372,    67,   321,
     370,   277,   370,    90,    91,   225,   226,   227,   249,   274,
      97,   204,   207,   205,    47,    52,    82,   216,    52,    95,
      94,    94,   115,    52,   112,   154,    64,    65,   372,   372,
     207,   372,   208,   210,   211,   209,    61,   372,   112,    26,
      30,   372,   211,   211,   211,    39,   316,   211,   311,    82,
     210,   210,   210,   210,    82,   118,   119,   120,   121,   123,
     124,   125,   126,   127,   128,   132,   301,   210,   210,   210,
     210,   210,   130,   131,   210,    41,    42,    43,    82,   294,
     206,    56,    58,   362,   363,   364,    82,    82,   144,   112,
     144,   144,   327,   144,   155,   327,   144,   327,   327,    82,
     155,   161,    82,   327,   160,   327,    82,   327,    82,    82,
     328,   144,   144,   208,   327,   144,    82,   185,   372,    82,
     161,   256,   256,   209,   372,    82,    82,   144,   144,   144,
     144,    45,    82,    82,   208,   212,   223,   274,    26,   248,
     275,   168,   170,   368,    69,   207,   209,   372,    26,   368,
      91,    82,    26,    97,   205,   257,   210,    77,   161,   210,
     210,   106,   107,   372,   210,   288,   117,   144,   290,   372,
      44,    82,   117,   144,   207,   272,   273,   372,   372,   144,
     141,   142,   143,   320,   144,   317,   318,   372,   210,    82,
      87,   304,   304,   304,   304,   209,   210,   210,   210,   210,
     210,   210,   210,   210,   210,   210,   210,   300,   304,   304,
     302,   304,   302,   304,   210,   210,   304,    82,    82,    57,
      59,   208,   326,   327,   327,   144,   327,   327,    82,   327,
     149,   208,   144,    70,   210,    26,   209,    82,    82,   210,
     207,   212,   212,   277,   211,   372,   211,   372,   372,   372,
     372,   277,   211,   279,    82,     9,   224,   277,   256,    82,
     116,   203,   290,   290,   372,   372,   210,   290,    82,   207,
     208,   212,   209,   372,   212,   212,   318,   212,   303,   304,
     300,   300,   300,   300,    82,   303,   304,   304,   304,   304,
     304,   304,   302,   302,   304,   304,    41,   208,   300,   300,
     300,   300,   300,   213,   213,   300,    60,   364,   327,   144,
     144,   208,   185,    82,   372,   372,    82,   144,    82,   372,
     372,    69,   207,   372,   280,   372,   278,   224,    19,   210,
     210,   290,    82,   273,   372,   212,   300,    41,    41,    41,
      41,   210,   301,    41,    41,    41,    41,    41,   118,   132,
     133,   134,   136,   295,   296,    66,   297,    41,    82,   210,
     212,   212,   372,   372,   208,   212,    19,   370,   290,   290,
      41,   303,   211,   211,   211,   211,   211,   214,   208,   211,
     214,    82,   372,   370,   304,   304,   135,   212,   212,    82,
     298,   299,    41,   295,   302,    41,   212,   212,   210,   210,
     212,   208,   212,   215,   304,   304,   298,   302,   212,   212
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
                                node->str_value_ = (yyvsp[(2) - (2)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 85:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(4) - (4)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 86:

    {
                          ObProxyTextPsParseNode *node = NULL;
                          malloc_parse_node(node);
                          node->str_value_ = (yyvsp[(2) - (2)].str);
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
           (yyval.node)->str_value_ = (yyvsp[(2) - (2)].str);
         ;}
    break;

  case 172:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_SYS_VAR);
           (yyval.node)->str_value_ = (yyvsp[(3) - (3)].str);
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
                                                                  HANDLE_ACCEPT();
                                                                ;}
    break;

  case 192:

    {
                                                 handle_stmt_end(result);
                                                 HANDLE_ACCEPT();
                                               ;}
    break;

  case 199:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_USER);
        ;}
    break;

  case 200:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 201:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 202:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(5) - (5)].var_node), (yyvsp[(3) - (5)].str), SET_VAR_SYS);
        ;}
    break;

  case 203:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 204:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 205:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_SYS);
        ;}
    break;

  case 206:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_STR);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 207:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_INT);
               (yyval.var_node)->int_value_ = (yyvsp[(1) - (1)].num);
             ;}
    break;

  case 208:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_NUMBER);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 211:

    {;}
    break;

  case 212:

    {;}
    break;

  case 213:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 214:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 215:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 216:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 217:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 218:

    {
              malloc_shard_column_node((yyval.shard_node), (yyvsp[(2) - (7)].str), (yyvsp[(3) - (7)].str), DBMESH_TOKEN_STR_VAL);
              (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (7)].str);
              add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
            ;}
    break;

  case 219:

    { result->trace_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 220:

    { result->rpc_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 221:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 222:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 223:

    {;}
    break;

  case 224:

    {;}
    break;

  case 225:

    { result->target_db_server_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 226:

    {;}
    break;

  case 228:

    { result->has_simple_route_info_ = true; result->simple_route_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 229:

    { result->simple_route_info_.part_key_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 233:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.group_idx_str_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 234:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.table_name_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 235:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 236:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 237:

    { result->dbp_route_info_.sticky_session_ = true; ;}
    break;

  case 238:

    {result->dbp_route_info_.has_shard_key_ = true;;}
    break;

  case 239:

    { result->trace_id_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 240:

    { result->trace_id_ = (yyvsp[(3) - (6)].str); result->rpc_id_ = (yyvsp[(5) - (6)].str); ;}
    break;

  case 241:

    {;}
    break;

  case 243:

    {
                   if (result->dbp_route_info_.shard_key_count_ < OBPROXY_MAX_DBP_SHARD_KEY_NUM) {
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].left_str_ = (yyvsp[(1) - (3)].str);
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].right_str_ = (yyvsp[(3) - (3)].str);
                     ++result->dbp_route_info_.shard_key_count_;
                   }
                 ;}
    break;

  case 246:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 247:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 248:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 249:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 250:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 251:

    { result->trace_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 252:

    { result->rpc_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 253:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 254:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 255:

    { result->has_trace_log_hint_ = true; ;}
    break;

  case 256:

    { result->target_db_server_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 257:

    {
             malloc_shard_column_node((yyval.shard_node), (yyvsp[(1) - (5)].str), (yyvsp[(3) - (5)].str), DBMESH_TOKEN_STR_VAL);
             (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (5)].str);
             add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
           ;}
    break;

  case 258:

    {;}
    break;

  case 259:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 261:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 294:

    { result->query_timeout_ = (yyvsp[(3) - (4)].num); ;}
    break;

  case 296:

    {
      add_hint_index(result->dbmesh_route_info_, (yyvsp[(3) - (5)].str));
      result->dbmesh_route_info_.index_count_++;
    ;}
    break;

  case 297:

    { result->has_trace_log_hint_ = true; ;}
    break;

  case 301:

    {;}
    break;

  case 302:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_WEAK); ;}
    break;

  case 303:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_STRONG); ;}
    break;

  case 304:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_FROZEN); ;}
    break;

  case 307:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_WARNINGS; ;}
    break;

  case 308:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_ERRORS; ;}
    break;

  case 309:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 310:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 311:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_SLAVE_HOSTS; ;}
    break;

  case 312:

    {
            result->is_binlog_related_ = true;
            result->cur_stmt_type_ = OBPROXY_T_SHOW_SLAVE_STATUS;
          ;}
    break;

  case 313:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_RELAYLOG_EVENTS; ;}
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

  case 345:

    { result->cur_stmt_type_ = OBPROXY_T_BINLOG_STR; ;}
    break;

  case 346:

    {
    result->cur_stmt_type_ = OBPROXY_T_SHOW_BINLOG_SERVER_FOR_TENANT;
    result->is_binlog_related_ = true;
;}
    break;

  case 347:

    { result->is_binlog_related_ = true; ;}
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

    {
;}
    break;

  case 352:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 353:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 354:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 355:

    {;}
    break;

  case 356:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 357:

    {;}
    break;

  case 358:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 360:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_KV_THREAD); ;}
    break;

  case 362:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 363:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 364:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 365:

    {;}
    break;

  case 366:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_ALL); ;}
    break;

  case 367:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 368:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 369:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 371:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 372:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 373:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 374:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 375:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 377:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 378:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 379:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 380:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 381:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 382:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 383:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 384:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 385:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 386:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_READ_STALE); ;}
    break;

  case 387:

    {;}
    break;

  case 388:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 389:

    {;}
    break;

  case 390:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 391:

    {;}
    break;

  case 393:

    {;}
    break;

  case 394:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 395:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 396:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 397:

    {;}
    break;

  case 398:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 399:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 400:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_GLOBALINDEX); ;}
    break;

  case 401:

    {;}
    break;

  case 402:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 403:

    {;}
    break;

  case 404:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 405:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 406:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); SET_ICMD_ONE_ID((yyvsp[(3) - (3)].num)); ;}
    break;

  case 407:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 408:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 410:

    {;}
    break;

  case 411:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 412:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 413:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 414:

    {;}
    break;

  case 415:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 417:

    {;}
    break;

  case 418:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 419:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 420:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 421:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 422:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 423:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 424:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 425:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 426:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 427:

    {;}
    break;

  case 428:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 429:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 430:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 431:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 432:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 433:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 434:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 437:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 438:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 439:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 440:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 447:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 448:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 450:

    {;}
    break;

  case 451:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 452:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 453:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 454:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 455:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT();
                                                ;}
    break;

  case 456:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 457:

    {
                                                  result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                                                  result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                                                  result->table_info_.dblink_name_ = (yyvsp[(5) - (5)].str);
                                                 ;}
    break;

  case 458:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 459:

    {
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.dblink_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 460:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 461:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 462:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 463:

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

