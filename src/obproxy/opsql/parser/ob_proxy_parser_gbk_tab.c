
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
     MASTER = 364,
     LOGS = 365,
     RESET = 366,
     FLUSH = 367,
     SERVER = 368,
     TENANT = 369,
     NUMBER_VAL = 370,
     GROUP_ID = 371,
     TABLE_ID = 372,
     ELASTIC_ID = 373,
     TESTLOAD = 374,
     ODP_COMMENT = 375,
     TNT_ID = 376,
     DISASTER_STATUS = 377,
     TRACE_ID = 378,
     RPC_ID = 379,
     TARGET_DB_SERVER = 380,
     TRACE_LOG = 381,
     DBP_COMMENT = 382,
     ROUTE_TAG = 383,
     SYS_TAG = 384,
     TABLE_NAME = 385,
     SCAN_ALL = 386,
     STICKY_SESSION = 387,
     PARALL = 388,
     SHARD_KEY = 389,
     STOP_DDL_TASK = 390,
     RETRY_DDL_TASK = 391,
     QUERY_TIMEOUT = 392,
     READ_CONSISTENCY = 393,
     WEAK = 394,
     STRONG = 395,
     FROZEN = 396,
     INT_NUM = 397,
     SHOW_PROXYNET = 398,
     THREAD = 399,
     CONNECTION = 400,
     LIMIT = 401,
     OFFSET = 402,
     SHOW_PROCESSLIST = 403,
     SHOW_PROXYSESSION = 404,
     SHOW_GLOBALSESSION = 405,
     ATTRIBUTE = 406,
     VARIABLES = 407,
     ALL = 408,
     STAT = 409,
     READ_STALE = 410,
     SHOW_PROXYCONFIG = 411,
     DIFF = 412,
     USER = 413,
     LIKE = 414,
     SHOW_PROXYSM = 415,
     SHOW_PROXYKV = 416,
     SHOW_PROXYCLUSTER = 417,
     SHOW_PROXYRESOURCE = 418,
     SHOW_PROXYCONGESTION = 419,
     SHOW_PROXYROUTE = 420,
     PARTITION = 421,
     ROUTINE = 422,
     SUBPARTITION = 423,
     SHOW_PROXYVIP = 424,
     SHOW_PROXYMEMORY = 425,
     OBJPOOL = 426,
     SHOW_SQLAUDIT = 427,
     SHOW_WARNLOG = 428,
     SHOW_PROXYSTAT = 429,
     REFRESH = 430,
     SHOW_PROXYTRACE = 431,
     SHOW_PROXYINFO = 432,
     BINARY = 433,
     UPGRADE = 434,
     IDC = 435,
     SHOW_PROXYPS = 436,
     DETAIL = 437,
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
#define YYFINAL  381
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   3727

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  217
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  160
/* YYNRULES -- Number of rules.  */
#define YYNRULES  519
/* YYNRULES -- Number of states.  */
#define YYNSTATES  848

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
     501,   504,   509,   512,   515,   519,   521,   526,   533,   538,
     544,   551,   556,   560,   562,   564,   566,   568,   571,   575,
     581,   588,   595,   602,   609,   616,   624,   631,   638,   645,
     652,   661,   670,   677,   678,   681,   683,   687,   689,   694,
     699,   703,   710,   714,   719,   724,   731,   735,   737,   741,
     742,   746,   750,   754,   758,   762,   766,   770,   774,   778,
     782,   784,   788,   794,   798,   803,   808,   812,   814,   818,
     819,   821,   822,   824,   826,   828,   831,   834,   838,   843,
     845,   848,   850,   853,   855,   858,   861,   865,   866,   868,
     871,   873,   876,   878,   881,   884,   887,   890,   891,   894,
     896,   898,   899,   902,   907,   912,   918,   920,   925,   927,
     929,   930,   932,   934,   936,   937,   939,   943,   947,   950,
     956,   960,   964,   968,   972,   976,   980,   984,   986,   988,
     990,   992,   994,   996,   998,  1000,  1002,  1004,  1006,  1008,
    1010,  1012,  1014,  1016,  1018,  1020,  1022,  1024,  1026,  1028,
    1030,  1032,  1034,  1035,  1037,  1039,  1041,  1044,  1050,  1056,
    1059,  1063,  1067,  1068,  1071,  1076,  1081,  1082,  1085,  1086,
    1089,  1092,  1094,  1097,  1099,  1101,  1105,  1108,  1112,  1116,
    1121,  1123,  1126,  1127,  1130,  1134,  1137,  1140,  1143,  1144,
    1147,  1151,  1154,  1158,  1161,  1165,  1169,  1174,  1177,  1179,
    1182,  1185,  1189,  1192,  1195,  1196,  1198,  1200,  1203,  1206,
    1210,  1213,  1216,  1218,  1221,  1223,  1226,  1229,  1233,  1236,
    1239,  1242,  1243,  1245,  1249,  1255,  1258,  1262,  1265,  1266,
    1268,  1271,  1274,  1277,  1280,  1285,  1292,  1293,  1295,  1296,
    1298,  1303,  1309,  1315,  1319,  1321,  1324,  1328,  1332,  1335,
    1338,  1342,  1346,  1347,  1350,  1352,  1356,  1360,  1364,  1365,
    1367,  1369,  1373,  1376,  1380,  1383,  1386,  1388,  1389,  1392,
    1397,  1400,  1405,  1408,  1410,  1416,  1420,  1424,  1427,  1432,
    1436,  1442,  1444,  1446,  1448,  1450,  1452,  1454,  1456,  1458,
    1460,  1462,  1464,  1466,  1468,  1470,  1472,  1474,  1476,  1478,
    1480,  1482,  1484,  1486,  1488,  1490,  1492,  1494,  1496,  1498,
    1500,  1502,  1504,  1506,  1508,  1510,  1512,  1514,  1516,  1518,
    1520,  1522,  1524,  1526,  1528,  1530,  1532,  1534,  1536,  1538
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     218,     0,    -1,   219,    -1,     1,    -1,   220,    -1,   219,
     220,    -1,   221,    45,    -1,   221,   206,    -1,   221,   206,
      45,    -1,   206,    -1,   206,    45,    -1,    53,   221,   206,
      -1,   222,    -1,   290,   222,    -1,   223,    -1,   281,    -1,
     286,    -1,   282,    -1,   283,    -1,   284,    -1,   230,    -1,
     229,    -1,   365,    -1,   323,    -1,   252,    -1,   324,    -1,
     369,    -1,   370,    -1,   264,    -1,   265,    -1,   266,    -1,
     267,    -1,   268,    -1,   269,    -1,   270,    -1,   231,    -1,
     243,    -1,   285,    -1,   326,    -1,   228,    -1,   371,    -1,
     306,    -1,   307,    -1,   308,   249,   248,    -1,    -1,     9,
      -1,    91,    82,   224,    19,   374,    -1,    90,    91,    82,
     224,    19,   374,    -1,   226,    -1,   225,    -1,   315,   227,
      -1,   245,   223,    -1,   245,   281,    -1,   245,   283,    -1,
     245,   284,    -1,   245,   282,    -1,   245,   285,    -1,   247,
     222,    -1,   232,    -1,   244,    -1,    14,   233,    -1,    15,
     234,    -1,    16,    -1,    17,    -1,    18,    -1,   235,    -1,
     236,    -1,    -1,    19,    -1,    61,    -1,    20,    61,    -1,
      48,    -1,    -1,    48,    -1,   135,   142,    -1,   136,   142,
      -1,   223,    -1,   281,    -1,   282,    -1,   284,    -1,   283,
      -1,   371,    -1,   270,    -1,   285,    -1,   207,    82,    -1,
     238,   208,   207,    82,    -1,   207,    82,    -1,   239,    -1,
     237,    -1,    28,   376,    26,    -1,    29,   376,    -1,    29,
     376,    30,    -1,   241,   240,    -1,   242,   238,    -1,    15,
      28,   376,    -1,    31,    28,   376,    -1,    21,    -1,    22,
      -1,    23,    -1,    24,    -1,    49,    -1,    25,    -1,    50,
      -1,    51,    -1,   246,    -1,   246,    82,    -1,    83,    -1,
      85,    -1,    86,    -1,    84,    -1,    -1,    26,   277,    -1,
      -1,   274,    -1,     5,    78,    -1,     5,    78,   249,    26,
     277,    -1,     5,    78,   274,    -1,   193,    -1,   193,    69,
     376,    -1,   250,    -1,   251,    -1,   262,    -1,   263,    -1,
     253,    -1,   261,    -1,   184,   254,    -1,   260,    -1,   190,
      -1,   191,    -1,   187,    -1,   258,    -1,   259,    -1,   194,
     254,    -1,   195,   254,    -1,   255,    -1,   246,   376,    -1,
      26,   376,    -1,    26,   376,    26,   376,    -1,    26,   376,
     209,   376,    -1,   192,    82,    -1,   192,    82,   209,    82,
      -1,    -1,   159,    82,    -1,    -1,    26,    82,    -1,   188,
     257,   256,    -1,   189,   257,   256,    -1,    11,    19,    52,
     257,   256,    -1,   186,    -1,   183,    -1,   183,    26,    82,
      -1,   183,    70,   185,   210,    82,    -1,   183,    26,    82,
      70,   185,   210,    82,    -1,    79,    -1,    80,   210,   142,
      -1,   100,    -1,   101,    -1,   102,    -1,   103,    -1,   104,
      -1,   105,    -1,    13,   271,   211,   272,   212,    -1,   376,
      -1,   376,   209,   376,    -1,   376,   209,   376,   209,   376,
      -1,    -1,   273,    -1,   272,   208,   273,    -1,    82,    -1,
     142,    -1,   115,    -1,   207,    82,    -1,   207,   207,    82,
      -1,    44,    -1,   275,    -1,   274,   275,    -1,   276,    -1,
     211,   212,    -1,   211,   223,   212,    -1,   211,   274,   212,
      -1,   373,    -1,   278,    -1,   223,    -1,    -1,   211,   280,
     212,    -1,   376,    -1,   280,   208,   376,    -1,   311,   374,
     372,    -1,   311,   374,   372,   279,   278,    -1,   313,   277,
      -1,   309,   277,    -1,   310,   322,    26,   277,    -1,   314,
     374,    -1,    12,   287,    -1,   288,   208,   287,    -1,   288,
      -1,   207,   376,   210,   289,    -1,   207,   207,   106,   376,
     210,   289,    -1,   106,   376,   210,   289,    -1,   207,   207,
     376,   210,   289,    -1,   207,   207,   107,   376,   210,   289,
      -1,   107,   376,   210,   289,    -1,   376,   210,   289,    -1,
     376,    -1,   142,    -1,   115,    -1,   291,    -1,   291,   290,
      -1,    40,   292,    41,    -1,    40,   120,   300,   299,    41,
      -1,    40,   117,   210,   305,   299,    41,    -1,    40,   130,
     210,   305,   299,    41,    -1,    40,   116,   210,   305,   299,
      41,    -1,    40,   118,   210,   305,   299,    41,    -1,    40,
     119,   210,   305,   299,    41,    -1,    40,    81,    82,   210,
     304,   299,    41,    -1,    40,   123,   210,   303,   299,    41,
      -1,    40,   124,   210,   303,   299,    41,    -1,    40,   121,
     210,   305,   299,    41,    -1,    40,   122,   210,   305,   299,
      41,    -1,    40,   127,   128,   210,   213,   294,   214,    41,
      -1,    40,   127,   129,   210,   213,   296,   214,    41,    -1,
      40,   125,   210,   305,   299,    41,    -1,    -1,   292,   293,
      -1,   376,    -1,   295,   208,   294,    -1,   295,    -1,   116,
     211,   305,   212,    -1,   130,   211,   305,   212,    -1,   131,
     211,   212,    -1,   131,   211,   133,   210,   305,   212,    -1,
     132,   211,   212,    -1,   134,   211,   297,   212,    -1,    66,
     211,   303,   212,    -1,    66,   211,   303,   215,   303,   212,
      -1,   298,   208,   297,    -1,   298,    -1,    82,   210,   305,
      -1,    -1,   299,   208,   300,    -1,   116,   210,   305,    -1,
     117,   210,   305,    -1,   130,   210,   305,    -1,   118,   210,
     305,    -1,   119,   210,   305,    -1,   123,   210,   303,    -1,
     124,   210,   303,    -1,   121,   210,   305,    -1,   122,   210,
     305,    -1,   126,    -1,   125,   210,   305,    -1,    82,   209,
      82,   210,   304,    -1,    82,   210,   304,    -1,    42,   211,
     376,   212,    -1,    43,   211,   301,   212,    -1,   302,   208,
     301,    -1,   302,    -1,   376,   210,   289,    -1,    -1,   305,
      -1,    -1,   305,    -1,   376,    -1,    87,    -1,     5,   204,
      -1,     5,   205,    -1,     5,   108,    97,    -1,     5,   207,
     207,    97,    -1,     5,    -1,    32,   316,    -1,     8,    -1,
      33,   316,    -1,     6,    -1,    34,   316,    -1,     7,   312,
      -1,    35,   316,   312,    -1,    -1,   153,    -1,   153,    47,
      -1,     9,    -1,    36,   316,    -1,    10,    -1,    37,   316,
      -1,    88,    89,    -1,    38,   316,    -1,   317,    39,    -1,
      -1,   320,   317,    -1,   142,    -1,   376,    -1,    -1,   318,
     319,    -1,   137,   211,   142,   212,    -1,   138,   211,   321,
     212,    -1,    61,   211,   376,   376,   212,    -1,   126,    -1,
     376,   211,   319,   212,    -1,   376,    -1,   142,    -1,    -1,
     139,    -1,   140,    -1,   141,    -1,    -1,    67,    -1,    11,
     364,    64,    -1,    11,   364,    65,    -1,    11,    66,    -1,
      11,    66,    82,   210,    82,    -1,    11,    92,    95,    -1,
      11,    92,    52,    -1,    11,    93,    94,    -1,    11,   109,
      52,    -1,    11,   178,   110,    -1,    11,    96,    94,    -1,
      11,   109,   110,    -1,   332,    -1,   334,    -1,   335,    -1,
     338,    -1,   336,    -1,   340,    -1,   341,    -1,   342,    -1,
     343,    -1,   345,    -1,   346,    -1,   347,    -1,   348,    -1,
     349,    -1,   351,    -1,   352,    -1,   354,    -1,   330,    -1,
     355,    -1,   358,    -1,   359,    -1,   360,    -1,   361,    -1,
     362,    -1,   363,    -1,    -1,   106,    -1,   107,    -1,    90,
      -1,    96,   376,    -1,    11,    96,   113,    77,   114,    -1,
      11,   325,   152,   159,   203,    -1,   111,   109,    -1,    24,
     178,   110,    -1,   112,   178,   110,    -1,    -1,   146,   142,
      -1,   146,   142,   208,   142,    -1,   146,   142,   147,   142,
      -1,    -1,   159,    82,    -1,    -1,   159,    82,    -1,   161,
     331,    -1,   144,    -1,   143,   333,    -1,   144,    -1,   145,
      -1,   145,   142,   327,    -1,   156,   328,    -1,   156,   153,
     328,    -1,   156,   157,   328,    -1,   156,   157,   158,   328,
      -1,   148,    -1,   150,   337,    -1,    -1,   151,    82,    -1,
     151,   159,    82,    -1,   151,   153,    -1,   159,    82,    -1,
     149,   339,    -1,    -1,   151,   328,    -1,   151,   142,   328,
      -1,   154,   328,    -1,   154,   142,   328,    -1,   152,   328,
      -1,   152,   142,   328,    -1,   152,   153,   328,    -1,   152,
     153,   142,   328,    -1,   155,   328,    -1,   160,    -1,   160,
     142,    -1,   162,   328,    -1,   162,   180,   328,    -1,   163,
     328,    -1,   164,   344,    -1,    -1,    82,    -1,   153,    -1,
     153,    82,    -1,   165,   329,    -1,   165,   167,   329,    -1,
     165,   166,    -1,   165,    63,    -1,   169,    -1,   169,    82,
      -1,   170,    -1,   170,   142,    -1,   170,   171,    -1,   170,
     171,   142,    -1,   172,   327,    -1,   172,   142,    -1,   173,
     350,    -1,    -1,   142,    -1,   142,   208,   142,    -1,   142,
     208,   142,   208,    82,    -1,   174,   328,    -1,   174,   175,
     328,    -1,   176,   353,    -1,    -1,   142,    -1,   142,   142,
      -1,   177,   178,    -1,   177,   179,    -1,   177,   180,    -1,
     181,   357,   328,   356,    -1,   181,   357,   328,   114,   329,
     356,    -1,    -1,   182,    -1,    -1,   142,    -1,   196,    12,
      82,   210,    -1,   196,    12,    82,   210,    82,    -1,   196,
      12,    82,   210,   142,    -1,   197,     6,    82,    -1,   198,
      -1,   199,   142,    -1,   199,   142,   142,    -1,   200,    82,
     142,    -1,   200,    82,    -1,   201,   142,    -1,   201,   145,
     142,    -1,   201,   202,   142,    -1,    -1,    68,   216,    -1,
      53,    -1,    54,    55,   366,    -1,    62,    53,    82,    -1,
      62,    54,    82,    -1,    -1,   367,    -1,   368,    -1,   367,
     208,   368,    -1,    56,    57,    -1,    58,    59,    60,    -1,
      98,   376,    -1,    99,    82,    -1,    82,    -1,    -1,   168,
     376,    -1,   168,   211,   376,   212,    -1,   166,   376,    -1,
     166,   211,   376,   212,    -1,   374,   372,    -1,   376,    -1,
     376,   209,   376,   207,   376,    -1,   376,   209,   376,    -1,
     376,   207,   376,    -1,   376,   376,    -1,   376,   209,   376,
     376,    -1,   376,    69,   376,    -1,   376,   209,   376,    69,
     376,    -1,    54,    -1,    62,    -1,    53,    -1,    55,    -1,
      59,    -1,    65,    -1,    64,    -1,    68,    -1,    67,    -1,
      66,    -1,   144,    -1,   145,    -1,   147,    -1,   151,    -1,
     152,    -1,   154,    -1,   157,    -1,   158,    -1,   171,    -1,
     175,    -1,   179,    -1,   180,    -1,   202,    -1,   185,    -1,
      49,    -1,    50,    -1,    51,    -1,    90,    -1,    89,    -1,
      52,    -1,   139,    -1,   140,    -1,   141,    -1,   106,    -1,
     107,    -1,    95,    -1,    94,    -1,    93,    -1,    96,    -1,
      97,    -1,   109,    -1,   110,    -1,   111,    -1,   112,    -1,
     113,    -1,   114,    -1,   182,    -1,    82,    -1,   375,    -1
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
    1258,  1259,  1261,  1266,  1269,  1274,  1278,  1282,  1286,  1291,
    1295,  1301,  1302,  1303,  1304,  1305,  1306,  1307,  1308,  1309,
    1310,  1311,  1312,  1313,  1314,  1315,  1316,  1317,  1318,  1319,
    1320,  1321,  1322,  1323,  1324,  1325,  1326,  1327,  1328,  1329,
    1330,  1331,  1332,  1333,  1334,  1335,  1336,  1337,  1338,  1339,
    1340,  1341,  1342,  1343,  1344,  1345,  1346,  1347,  1349,  1350
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
  "GLOBAL_ALIAS", "MASTER", "LOGS", "RESET", "FLUSH", "SERVER", "TENANT",
  "NUMBER_VAL", "GROUP_ID", "TABLE_ID", "ELASTIC_ID", "TESTLOAD",
  "ODP_COMMENT", "TNT_ID", "DISASTER_STATUS", "TRACE_ID", "RPC_ID",
  "TARGET_DB_SERVER", "TRACE_LOG", "DBP_COMMENT", "ROUTE_TAG", "SYS_TAG",
  "TABLE_NAME", "SCAN_ALL", "STICKY_SESSION", "PARALL", "SHARD_KEY",
  "STOP_DDL_TASK", "RETRY_DDL_TASK", "QUERY_TIMEOUT", "READ_CONSISTENCY",
  "WEAK", "STRONG", "FROZEN", "INT_NUM", "SHOW_PROXYNET", "THREAD",
  "CONNECTION", "LIMIT", "OFFSET", "SHOW_PROCESSLIST", "SHOW_PROXYSESSION",
  "SHOW_GLOBALSESSION", "ATTRIBUTE", "VARIABLES", "ALL", "STAT",
  "READ_STALE", "SHOW_PROXYCONFIG", "DIFF", "USER", "LIKE", "SHOW_PROXYSM",
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
     283,   284,   285,   286,   287,   287,   288,   288,   288,   288,
     288,   288,   288,   289,   289,   289,   290,   290,   291,   291,
     291,   291,   291,   291,   291,   291,   291,   291,   291,   291,
     291,   291,   291,   292,   292,   293,   294,   294,   295,   295,
     295,   295,   295,   295,   296,   296,   297,   297,   298,   299,
     299,   300,   300,   300,   300,   300,   300,   300,   300,   300,
     300,   300,   300,   300,   300,   300,   301,   301,   302,   303,
     303,   304,   304,   305,   305,   306,   306,   307,   307,   308,
     308,   309,   309,   310,   310,   311,   311,   312,   312,   312,
     313,   313,   314,   314,   315,   315,   316,   317,   317,   318,
     318,   319,   319,   320,   320,   320,   320,   320,   320,   320,
     321,   321,   321,   321,   322,   322,   323,   323,   323,   323,
     323,   323,   323,   323,   323,   323,   323,   324,   324,   324,
     324,   324,   324,   324,   324,   324,   324,   324,   324,   324,
     324,   324,   324,   324,   324,   324,   324,   324,   324,   324,
     324,   324,   325,   325,   325,   325,   326,   326,   326,   326,
     326,   326,   327,   327,   327,   327,   328,   328,   329,   329,
     330,   331,   332,   333,   333,   333,   334,   334,   334,   334,
     335,   336,   337,   337,   337,   337,   337,   338,   339,   339,
     339,   339,   339,   339,   339,   339,   339,   339,   340,   340,
     341,   341,   342,   343,   344,   344,   344,   344,   345,   345,
     345,   345,   346,   346,   347,   347,   347,   347,   348,   348,
     349,   350,   350,   350,   350,   351,   351,   352,   353,   353,
     353,   354,   354,   354,   355,   355,   356,   356,   357,   357,
     358,   358,   358,   359,   360,   361,   361,   362,   362,   363,
     363,   363,   364,   364,   365,   365,   365,   365,   366,   366,
     367,   367,   368,   368,   369,   370,   371,   372,   372,   372,
     372,   372,   373,   374,   374,   374,   374,   374,   374,   374,
     374,   375,   375,   375,   375,   375,   375,   375,   375,   375,
     375,   375,   375,   375,   375,   375,   375,   375,   375,   375,
     375,   375,   375,   375,   375,   375,   375,   375,   375,   375,
     375,   375,   375,   375,   375,   375,   375,   375,   375,   375,
     375,   375,   375,   375,   375,   375,   375,   375,   376,   376
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
       2,     4,     2,     2,     3,     1,     4,     6,     4,     5,
       6,     4,     3,     1,     1,     1,     1,     2,     3,     5,
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
       2,     4,     2,     1,     5,     3,     3,     2,     4,     3,
       5,     1,     1,     1,     1,     1,     1,     1,     1,     1,
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
       0,     0,     0,     0,   495,   496,   497,   500,   473,   471,
     474,   475,   472,   477,   476,   480,   479,   478,   518,   499,
     498,   508,   507,   506,   509,   510,   504,   505,   511,   512,
     513,   514,   515,   516,   501,   502,   503,   481,   482,   483,
     484,   485,   486,   487,   488,   489,   490,   491,   492,   517,
     494,   493,     0,   193,   195,   519,     0,   504,   505,     0,
     162,    68,     0,    71,    69,    60,     0,    73,    61,     0,
       0,    90,     0,     0,   296,     0,     0,   299,   270,     0,
     287,   298,   272,   274,   277,   281,   283,   285,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   444,     0,   448,     0,     0,     0,   284,   346,
     454,   455,   349,     0,    74,    75,   363,   364,   362,   356,
     356,   356,   356,   377,     0,     0,   371,   356,   356,     0,
     366,   389,   361,   360,   356,   390,   392,   395,   396,   393,
     401,     0,   400,   358,   398,   403,   405,   406,   409,     0,
     408,   412,   410,   356,   415,   419,   417,   421,   422,   423,
     429,   356,     0,     0,     0,   125,     0,   141,   141,   139,
       0,   132,   133,     0,     0,   435,   438,   439,     0,     0,
      10,     1,     5,     6,     7,   269,     0,    76,    88,    87,
      92,    82,    77,    78,    80,    79,    83,    81,     0,    93,
      51,    52,    55,    53,    54,    56,   105,   135,    57,    13,
     207,     0,   110,   113,   174,   176,   182,   190,   181,   180,
     457,   463,   305,     0,   457,   189,   192,     0,     0,    49,
      48,    50,     0,   116,   267,     0,   279,   143,     0,   443,
     311,   310,   312,   315,     0,   313,   316,   314,     0,   306,
     307,     0,     0,     0,     0,     0,     0,   165,     0,    70,
      94,   350,    89,    91,    95,     0,     0,   300,   286,   288,
     291,   276,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   250,
       0,   239,     0,     0,   259,   259,     0,     0,     0,     0,
     208,   224,   225,    11,     0,     0,   445,   449,   450,   446,
     447,   154,   351,   352,   356,   379,   356,   356,   383,   356,
     381,   387,   373,   375,     0,   376,   367,   356,   368,   357,
     391,   397,   359,   399,   407,   353,     0,   416,   420,   426,
     150,     0,   136,   144,     0,   145,   146,     0,   118,     0,
     433,   436,   437,   440,   441,     8,    86,    84,     0,   177,
       0,     0,     0,    43,   175,     0,     0,   462,     0,     0,
       0,   467,     0,   183,     0,    44,     0,   268,   141,     0,
       0,     0,     0,     0,   504,   505,     0,     0,   194,   205,
     204,   202,   203,   173,   168,   170,   169,     0,     0,   166,
     163,     0,     0,   301,   302,   303,     0,   289,   291,     0,
     290,   261,   264,   239,   263,   239,   239,   239,     0,     0,
       0,   261,     0,     0,     0,     0,     0,     0,   259,   259,
       0,     0,     0,   239,   239,   239,   260,   239,   239,     0,
       0,   239,   452,     0,     0,   365,   380,   384,   356,   385,
     382,   374,   369,     0,     0,   413,   358,   427,   424,     0,
       0,     0,     0,   142,   140,   430,     0,   178,   179,   111,
       0,   460,     0,   458,   469,   466,   465,   191,     0,     0,
      44,    45,     0,   115,   147,   309,   347,   348,   198,   201,
       0,     0,     0,   196,   171,     0,     0,   161,     0,     0,
     293,   294,   292,   297,   239,   262,     0,     0,     0,     0,
       0,     0,   257,     0,     0,   253,   241,   242,   244,   245,
     248,   249,   246,   247,   251,   243,   209,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   453,   451,   386,   355,
     354,     0,   426,     0,   151,   137,   138,   431,   432,    85,
       0,     0,     0,     0,   468,     0,   185,   188,     0,     0,
       0,     0,   199,   172,   167,   164,   295,     0,   212,   210,
     213,   214,   254,   255,     0,     0,   261,   240,   218,   219,
     216,   217,   222,     0,     0,     0,     0,     0,     0,   227,
       0,     0,   211,   414,   425,     0,   461,   459,   470,   464,
       0,   184,     0,    46,   197,   200,   215,   256,   258,   252,
       0,     0,     0,     0,     0,     0,     0,   259,     0,   152,
     186,    47,     0,     0,     0,   230,   232,     0,     0,   237,
     220,   226,     0,   221,   228,   229,     0,     0,   233,     0,
     234,   259,     0,   238,   236,     0,   231,   235
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    97,    98,    99,   100,   101,   416,   682,   429,   430,
     431,   103,   104,   105,   106,   107,   265,   268,   108,   109,
     388,   399,   389,   390,   110,   111,   112,   113,   114,   115,
     116,   563,   412,   117,   118,   119,   120,   365,   121,   545,
     367,   122,   123,   124,   125,   126,   127,   128,   129,   130,
     131,   132,   133,   134,   259,   598,   599,   413,   414,   415,
     417,   418,   679,   755,   135,   136,   137,   138,   139,   140,
     253,   254,   591,   141,   142,   301,   501,   788,   789,   791,
     828,   829,   632,   491,   711,   712,   635,   704,   636,   143,
     144,   145,   146,   147,   148,   190,   149,   150,   151,   278,
     279,   608,   609,   280,   606,   423,   152,   153,   202,   154,
     350,   330,   344,   155,   333,   156,   318,   157,   158,   159,
     326,   160,   323,   161,   162,   163,   164,   339,   165,   166,
     167,   168,   169,   352,   170,   171,   356,   172,   173,   658,
     361,   174,   175,   176,   177,   178,   179,   203,   180,   506,
     507,   508,   181,   182,   183,   567,   419,   420,   255,   614
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -574
static const yytype_int16 yypact[] =
{
     789,  -574,    47,  -574,   -21,  -574,  -574,  -574,   229,  2403,
    3247,    67,    70,  -574,  -574,  -574,  -574,  -574,  -574,   -43,
    -574,  3247,  3247,   125,  2691,  2691,  2691,  2691,  2691,  2691,
    2691,   267,  -574,  -574,  -574,  1343,    87,   151,  -574,   -46,
    -574,  -574,  -574,  -574,  -574,    82,  3247,  3247,   108,  -574,
    -574,  -574,  -574,  -574,  -574,   115,    62,   130,   146,   -49,
    -574,    45,   -52,   128,   149,   112,   -57,    99,    -5,    16,
     214,   -42,   -41,   166,    33,   168,   127,   170,    54,   292,
    -574,  -574,   308,   308,  -574,  -574,   255,   278,   292,   292,
     339,   346,  -574,   211,   272,   -48,   311,   359,   991,  -574,
       4,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
      37,   153,  -574,  -574,   294,  3386,  1540,  -574,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,  1540,   322,  -574,  -574,   152,  1161,   297,  3247,  1161,
    3247,   122,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,  -574,  -574,  -574,     0,   268,  -574,  -574,   159,   320,
    -574,   316,   287,   154,  -574,    26,   279,    32,  -574,  -574,
      24,   264,   223,   215,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  3247,  3247,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,  -574,  2547,  -574,   169,  -574,   183,  -574,  -574,   165,
     171,  -574,   317,  -574,  -574,  -574,  3247,  -574,  -574,   269,
     369,   366,  3247,   187,  -574,   188,   189,  -574,  -574,   362,
    2691,   191,  -574,  -574,   -21,  -574,  -574,  -574,   321,   194,
     195,   196,   198,   143,   199,   200,   201,   203,   204,   185,
     205,  1974,  -574,   210,   116,   337,   338,   280,  -574,  -574,
    -574,  -574,  -574,   313,  -574,  -574,  -574,   282,  -574,     9,
      42,    35,    99,  -574,   -15,   343,  -574,    99,   157,   344,
    -574,  -574,  -574,  -574,    99,  -574,  -574,  -574,   345,  -574,
    -574,   347,  -574,   262,  -574,  -574,  -574,   288,  -574,   289,
    -574,   225,  -574,    99,  -574,   293,  -574,  -574,  -574,  -574,
    -574,    99,   352,   251,  3247,  -574,   355,   281,   281,   230,
    3247,  -574,  -574,   356,   360,   299,   301,  -574,   303,   306,
    -574,  -574,  -574,  -574,   405,   -26,   370,  -574,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,   371,   243,
    -574,  -574,  -574,  -574,  -574,  -574,    23,  -574,  -574,  -574,
    -574,    20,   428,   152,  -574,  -574,  -574,  -574,  -574,  -574,
      71,  2113,  -574,   429,    71,  -574,  -574,   367,   375,  -574,
    -574,  -574,   433,     3,  -574,   363,  -574,   308,   252,  -574,
    -574,  -574,  -574,  -574,   386,  -574,  -574,  -574,   305,  -574,
    -574,   256,   257,  3525,   258,  2403,  2830,    21,  3247,  -574,
    -574,  -574,  -574,  -574,  -574,  3247,   323,   106,  -574,  -574,
    2969,  -574,   259,  3108,  3108,  3108,  3108,   260,   263,   114,
     266,   270,   271,   274,   275,   276,   277,   283,   284,  -574,
     285,  -574,  3108,  3108,  3108,  3108,  3108,   286,   290,  3108,
    -574,  -574,  -574,  -574,   420,   419,  -574,   291,  -574,  -574,
    -574,  -574,  -574,   333,    99,  -574,    99,    96,  -574,    99,
    -574,  -574,  -574,  -574,   400,  -574,  -574,    99,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,   -59,   348,  -574,  -574,   -30,
     418,   295,     7,  -574,   407,  -574,  -574,   409,  -574,   296,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,   300,  -574,
     298,   121,  1161,  -574,  -574,  1693,  1832,  -574,  3247,  3247,
    3247,  -574,  1161,   -13,   410,   488,  1161,  -574,   281,   416,
     387,   309,  2830,  2830,  3247,  3247,   304,  2830,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,   -31,   -81,  -574,
     302,  3247,   307,  -574,  -574,  -574,   310,  -574,  2969,   312,
    -574,  3108,  -574,  -574,  -574,  -574,  -574,  -574,  3247,  3247,
     421,  3108,  3108,  3108,  3108,  3108,  3108,  3108,  3108,  3108,
    3108,  3108,    -6,  -574,  -574,  -574,  -574,  -574,  -574,   315,
     318,  -574,  -574,   442,   116,  -574,  -574,  -574,    99,  -574,
    -574,  -574,  -574,   373,   374,   319,   262,  -574,  -574,   324,
     422,  3247,  3247,  -574,  -574,     8,   426,  -574,  -574,  -574,
    3247,  -574,  3247,  -574,  -574,  -574,  2259,  -574,  3247,    80,
     488,  -574,   494,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
     325,   326,  2830,  -574,  -574,   435,    21,  -574,  3247,   327,
    -574,  -574,  -574,  -574,  -574,  -574,    -2,    -1,    12,    13,
     328,   329,   330,   332,   334,  -574,  -574,  -574,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,   143,    14,    15,
      18,    19,    22,    57,   452,    25,  -574,  -574,  -574,  -574,
    -574,   438,   341,   335,  -574,  -574,  -574,  -574,  -574,  -574,
     331,   336,  3247,  3247,  -574,    41,  -574,  -574,   502,  3247,
    2830,  2830,  -574,  -574,  -574,  -574,  -574,    34,  -574,  -574,
    -574,  -574,  -574,  -574,  3247,  2830,  3108,  -574,  -574,  -574,
    -574,  -574,  -574,   340,   351,   353,   354,   357,   358,   342,
     368,   372,  -574,  -574,  -574,   443,  -574,  -574,  -574,  -574,
    3247,  -574,  3247,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    3108,  3108,   -69,   365,   444,   489,    57,  3108,   491,  -574,
    -574,  -574,   376,   377,   361,  -574,  -574,   380,   379,   350,
    -574,  -574,   -82,  -574,  -574,  -574,  3108,  3108,  -574,   444,
    -574,  3108,   381,  -574,  -574,   382,  -574,  -574
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -574,  -574,  -574,   431,   498,   -33,     6,  -143,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,  -574,   385,  -574,  -574,  -574,  -574,   261,  -574,  -344,
     -80,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,  -574,  -574,   424,  -574,  -574,  -150,  -170,  -352,  -574,
    -144,  -132,  -574,  -574,   105,   160,   172,   179,   184,  -574,
      94,  -574,  -525,   412,  -574,  -574,  -574,  -264,  -574,  -574,
    -284,  -574,  -524,  -161,  -204,  -574,  -459,  -573,  -465,  -574,
    -574,  -574,  -574,  -574,  -574,   349,  -574,  -574,  -574,   314,
     364,  -574,   -28,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
      68,   -44,  -336,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -160,
    -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,  -574,
    -574,   -61,  -574,  -574,   474,   161,  -574,  -146,  -574,    -9
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -519
static const yytype_int16 yytable[] =
{
     256,   260,   424,   368,   426,   425,   102,   533,   613,   615,
     616,   617,   270,   271,   433,   281,   281,   281,   281,   281,
     281,   281,   335,   336,   546,   385,  -112,   633,   634,  -113,
     354,   638,  -187,   661,   641,   726,   637,   309,   310,   768,
     769,   102,   385,     3,     4,     5,     6,     7,   715,   383,
      10,   694,    24,   770,   771,   778,   779,   688,   689,   780,
     781,   564,   693,   782,   824,   593,   792,   522,  -518,    24,
      25,    26,    27,    28,    29,   806,   445,   337,   440,   340,
     362,   564,   185,   408,   656,   385,   261,   262,   653,   706,
     747,   707,   708,   709,   377,   316,   317,   378,   266,   324,
     346,   348,   329,   594,   102,   349,   407,   325,   409,   728,
     729,   730,    24,   731,   732,   263,   387,   735,   267,    40,
     400,   441,   102,   334,   363,   184,   443,   696,   264,   347,
     840,   697,   189,   841,   446,   269,   595,   421,   523,   421,
     421,   421,   304,   825,   524,   444,   705,   102,   338,   654,
     748,   514,   657,   272,   379,   185,   705,   716,   717,   718,
     719,   720,   721,   596,   307,   724,   725,   762,   329,   722,
     723,   308,   504,   783,   505,   341,   695,   519,   186,   187,
     767,   188,   342,   343,   516,   477,   478,   784,   785,   786,
     311,   787,   329,  -187,   329,   517,   319,   320,   678,   321,
     322,   329,   727,   809,   305,   306,   727,   727,   353,   564,
     384,   411,   427,   428,   411,   392,   662,   451,   452,   401,
     727,   727,   727,   727,   312,   479,   727,   727,   597,  -518,
     727,   411,   559,   727,   684,   804,   805,   565,   648,   566,
     313,   561,   727,   454,   386,   603,   604,   605,   191,   800,
     808,   186,   187,   801,   188,   329,   332,   460,   329,   480,
     481,   482,   483,   464,   484,   485,   486,   487,   488,   489,
     393,   281,   314,   490,   402,   515,   518,   520,   521,   449,
     450,   327,   394,   526,   528,   328,   403,   329,   315,   395,
     530,   331,   502,   404,   396,   192,   345,   193,   405,   385,
       3,     4,     5,     6,     7,   357,   358,   359,   351,   537,
     355,   705,   360,   497,   498,   527,   329,   539,   364,   194,
     742,   195,   196,   620,   621,   197,    24,    25,    26,    27,
      28,    29,   411,   668,   366,   198,   199,   369,   200,   282,
     283,   284,   285,   286,   287,   822,   823,   370,   288,   371,
     372,   373,   374,   375,   376,   542,   380,   578,   832,   381,
     398,   548,    31,   411,   422,   434,   435,   436,   437,   438,
     439,   842,   843,   442,   447,   448,   457,   455,   459,   461,
     458,  -342,   845,   289,   290,   291,   292,   293,   294,   295,
     296,   297,   298,   456,   299,   462,   463,   300,   465,   466,
     467,   468,   470,   472,   473,   474,   475,   201,   476,   492,
     493,   494,   571,   495,   496,   499,   503,   560,   669,   509,
     510,   341,   511,   512,   513,   525,   529,   531,   677,   532,
     534,   535,   683,   536,   540,   538,   541,   543,   549,   547,
     544,   551,   550,   552,   586,   553,   256,   592,   554,   600,
     555,   558,   556,   557,   562,   572,   601,   575,   574,   576,
     577,   610,   579,   580,   581,   602,   582,   583,   587,   611,
     646,   618,   647,   649,   619,   650,   622,   642,   643,   349,
     623,   624,   651,   652,   625,   626,   627,   628,   659,   663,
     655,   664,   680,   629,   630,   631,   639,   681,   685,   644,
     640,   686,   736,   714,   744,   660,   665,   666,   749,   743,
     667,   698,   687,   759,   692,   739,   740,   763,   790,   700,
     793,   802,   701,   657,   703,   819,   827,   741,   733,   382,
     830,   734,   833,   303,   391,   760,   761,   758,   774,   766,
     772,   773,   775,   796,   776,   795,   764,   757,   797,   588,
     816,   810,   831,   421,   410,   844,   671,   673,   839,   674,
     675,   676,   811,   421,   812,   813,   777,   421,   814,   432,
     807,   836,   815,   592,   592,   690,   691,   826,   592,   817,
     702,   645,   794,   737,   397,   573,   818,     0,   834,   835,
     837,   838,   699,   846,   847,     0,     0,     0,     0,   610,
       0,     0,     0,     0,   738,     0,     0,     0,     0,   710,
     713,     0,     0,   803,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   471,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   469,     0,     0,     0,     0,     0,
       0,     0,   745,   746,     0,     0,   821,     0,     0,     0,
       0,   750,     0,   751,     0,     0,     0,   754,     0,   756,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   592,     0,     0,     0,     0,     0,   765,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   798,   799,     0,     0,     0,     0,     0,
     421,   592,   592,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   713,   592,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       1,   820,     0,   421,     2,     3,     4,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,     0,     0,
      16,    17,    18,    19,    20,     0,     0,    21,    22,     0,
      23,    24,    25,    26,    27,    28,    29,    30,     0,    31,
       0,     0,     0,     0,     0,     0,     0,     0,    32,    33,
      34,     0,    35,    36,     0,     0,     0,     0,     0,     0,
       0,    37,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    38,    39,
       0,    40,    41,    42,    43,    44,     0,    45,     0,     0,
       0,     0,     0,     0,     0,    46,     0,    47,    48,    49,
      50,    51,    52,    53,    54,     0,     0,     0,     0,     0,
      55,    56,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    57,    58,     0,     0,     0,     0,
       0,     0,    59,     0,     0,     0,     0,    60,    61,    62,
       0,     0,     0,     0,     0,    63,     0,     0,     0,    64,
      65,    66,    67,    68,    69,     0,     0,     0,    70,    71,
       0,    72,    73,    74,     0,    75,    76,     0,     0,     0,
      77,     0,    78,    79,     0,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,     0,     0,     0,     0,    96,     2,     3,     4,     5,
       6,     7,     8,     9,    10,    11,    12,    13,    14,    15,
       0,     0,    16,    17,    18,    19,    20,     0,     0,    21,
      22,     0,    23,    24,    25,    26,    27,    28,    29,    30,
       0,    31,     0,     0,     0,     0,     0,     0,     0,     0,
      32,    33,    34,     0,    35,    36,     0,     0,     0,     0,
       0,     0,     0,    37,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      38,    39,     0,    40,    41,    42,    43,    44,     0,    45,
       0,     0,     0,     0,     0,     0,     0,    46,     0,    47,
      48,    49,    50,    51,    52,    53,    54,     0,     0,     0,
       0,     0,    55,    56,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    57,    58,     0,     0,
       0,     0,     0,     0,    59,     0,     0,     0,     0,    60,
      61,    62,     0,     0,     0,     0,     0,    63,     0,     0,
       0,    64,    65,    66,    67,    68,    69,     0,     0,     0,
      70,    71,     0,    72,    73,    74,   385,    75,    76,     0,
       0,     0,    77,     0,    78,    79,     0,    80,    81,    82,
      83,    84,    85,    86,    87,    88,    89,    90,    91,    92,
      93,    94,    95,    24,     0,     0,     0,    96,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     204,   205,   206,   207,   208,   209,   210,     0,     0,     0,
     211,     0,     0,   212,     0,   213,   214,   215,   216,   217,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   218,     0,     0,     0,     0,     0,     0,
     219,   220,     0,     0,   221,   222,   223,   224,   225,     0,
       0,     0,     0,     0,     0,     0,     0,   257,   258,     0,
     228,   229,   230,   231,   232,   233,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     234,   235,   236,     0,     0,   237,   238,     0,   239,     0,
       0,     0,   240,   241,     0,   242,     0,     0,   243,   244,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   245,     0,     0,     0,   246,     0,     0,     0,
     247,   248,     0,   249,     0,     0,   250,     0,     2,     3,
       4,     5,     6,     7,     8,     9,    10,    11,    12,    13,
      14,    15,     0,   251,    16,    17,    18,    19,    20,     0,
       0,    21,    22,     0,    23,    24,    25,    26,    27,    28,
      29,    30,     0,    31,     0,     0,     0,     0,     0,     0,
       0,     0,    32,    33,    34,     0,   302,    36,     0,     0,
       0,     0,     0,     0,     0,    37,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    38,    39,     0,    40,    41,    42,    43,    44,
       0,    45,     0,     0,     0,     0,     0,     0,     0,    46,
       0,    47,    48,    49,    50,    51,    52,    53,    54,     0,
       0,     0,     0,     0,    55,    56,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    57,    58,
       0,     0,     0,     0,     0,     0,    59,     0,     0,     0,
       0,    60,    61,    62,     0,     0,     0,     0,     0,    63,
       0,     0,     0,    64,    65,    66,    67,    68,    69,     0,
       0,     0,    70,    71,     0,    72,    73,    74,     0,    75,
      76,     0,     0,     0,    77,     0,    78,    79,     0,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,     2,     3,     4,     5,     6,
       7,     8,     9,    10,    11,    12,    13,    14,    15,     0,
       0,    16,    17,    18,    19,    20,     0,     0,    21,    22,
       0,    23,    24,    25,    26,    27,    28,    29,    30,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    32,
      33,    34,     0,   302,    36,     0,     0,     0,     0,     0,
       0,     0,    37,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    38,
      39,     0,    40,    41,    42,    43,    44,     0,    45,     0,
       0,     0,     0,     0,     0,     0,    46,     0,    47,    48,
      49,    50,    51,    52,    53,    54,     0,     0,     0,     0,
       0,    55,    56,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    57,    58,     0,     0,     0,
       0,     0,     0,    59,     0,     0,     0,     0,    60,    61,
      62,     0,     0,     0,     0,     0,    63,     0,     0,     0,
      64,    65,    66,    67,    68,    69,     0,     0,     0,    70,
      71,     0,    72,    73,    74,     0,    75,    76,     0,     0,
       0,    77,     0,    78,    79,     0,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,   204,   205,   206,   207,   208,   209,   210,     0,
       0,     0,   211,     0,     0,   212,     0,   213,   214,   215,
     216,   217,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   218,     0,     0,     0,     0,
       0,     0,   219,   220,     0,     0,   221,   222,   223,   224,
     225,     0,     0,     0,     0,     0,     0,     0,     0,   257,
     258,     0,   228,   229,   230,   231,   232,   233,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   234,   235,   236,     0,     0,   237,   238,     0,
     239,     0,     0,     0,   240,   241,     0,   242,     0,     0,
     243,   244,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   245,     0,     0,     0,   246,     0,
       0,     0,   247,   248,     0,   249,     0,     0,   250,     0,
       0,   204,   205,   206,   207,   208,   209,   210,     0,     0,
       0,   211,     0,     0,   212,   251,   213,   214,   215,   216,
     217,     0,     0,     0,   670,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   218,     0,     0,     0,     0,     0,
       0,   219,   220,     0,     0,   221,   222,   223,   224,   225,
       0,     0,     0,     0,     0,     0,     0,     0,   257,   258,
       0,   228,   229,   230,   231,   232,   233,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   234,   235,   236,     0,     0,   237,   238,     0,   239,
       0,     0,     0,   240,   241,     0,   242,     0,     0,   243,
     244,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   245,     0,     0,     0,   246,     0,     0,
       0,   247,   248,     0,   249,   500,     0,   250,     0,     0,
       0,     0,     0,   204,   205,   206,   207,   208,   209,   210,
       0,     0,     0,   211,   251,     0,   212,     0,   213,   214,
     215,   216,   217,   672,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   218,     0,     0,     0,
       0,     0,     0,   219,   220,     0,     0,   221,   222,   223,
     224,   225,     0,     0,     0,     0,     0,     0,     0,     0,
     257,   258,     0,   228,   229,   230,   231,   232,   233,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   234,   235,   236,     0,     0,   237,   238,
       0,   239,     0,     0,     0,   240,   241,     0,   242,     0,
       0,   243,   244,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   245,     0,     0,     0,   246,
       0,     0,     0,   247,   248,     0,   249,     0,     0,   250,
       0,     0,   204,   205,   206,   207,   208,   209,   210,     0,
       0,     0,   211,     0,     0,   212,   251,   213,   214,   215,
     216,   217,   568,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   218,     0,     0,     0,     0,
       0,     0,   219,   220,     0,     0,   221,   222,   223,   224,
     225,     0,     0,     0,     0,     0,     0,     0,     0,   257,
     258,     0,   228,   229,   230,   231,   232,   233,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   234,   235,   236,     0,     0,   237,   238,     0,
     239,     0,     0,     0,   240,   241,     0,   242,     0,     0,
     243,   244,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   245,     0,     0,     0,   246,     0,
       0,     0,   247,   248,     0,   249,     0,     0,   250,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   204,   205,
     206,   207,   208,   209,   210,   251,     0,     0,   211,     0,
     569,   212,   570,   213,   214,   215,   216,   217,   752,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   218,     0,     0,     0,     0,     0,     0,   219,   220,
       0,     0,   221,   222,   223,   224,   225,     0,     0,     0,
       0,     0,     0,     0,     0,   257,   258,     0,   228,   229,
     230,   231,   232,   233,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   234,   235,
     236,     0,     0,   237,   238,     0,   239,     0,     0,     0,
     240,   241,     0,   242,     0,     0,   243,   244,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     245,     0,     0,     0,   246,     0,     0,     0,   247,   248,
       0,   249,     0,     0,   250,     0,     0,     0,     0,     0,
       0,     0,   204,   205,   206,   207,   208,   209,   210,     0,
       0,   251,   211,     0,     0,   212,   753,   213,   214,   215,
     216,   217,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   218,     0,     0,     0,     0,
       0,     0,   219,   220,     0,     0,   221,   222,   223,   224,
     225,     0,     0,     0,     0,     0,     0,     0,     0,   226,
     227,     0,   228,   229,   230,   231,   232,   233,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   234,   235,   236,     0,     0,   237,   238,     0,
     239,     0,     0,     0,   240,   241,     0,   242,     0,     0,
     243,   244,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   245,     0,     0,     0,   246,     0,
       0,     0,   247,   248,     0,   249,     0,     0,   250,     0,
       0,     0,     0,     0,     0,     0,   204,   205,   206,   207,
     208,   209,   210,     0,     0,   251,   211,     0,     0,   212,
     252,   213,   214,   215,   216,   217,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   218,
       0,     0,     0,     0,     0,     0,   219,   220,     0,     0,
     221,   222,   223,   224,   225,     0,     0,     0,     0,     0,
       0,     0,     0,   257,   258,     0,   228,   229,   230,   231,
     232,   233,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   234,   235,   236,     0,
       0,   237,   238,     0,   239,     0,     0,     0,   240,   241,
       0,   242,     0,     0,   243,   244,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   245,     0,
       0,     0,   246,     0,     0,     0,   247,   248,     0,   249,
       0,     0,   250,     0,     0,     0,     0,     0,     0,     0,
     204,   205,   206,   207,   208,   209,   210,     0,     0,   251,
     211,     0,   273,   212,   453,   213,   214,   215,   216,   217,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   218,     0,     0,     0,     0,     0,     0,
     219,   220,     0,     0,   221,   222,   223,   224,   225,     0,
       0,     0,     0,     0,     0,     0,     0,   257,   258,     0,
     228,   229,   230,   231,   232,   233,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   274,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   275,   276,
     234,   235,   236,   277,     0,   237,   238,     0,   239,     0,
       0,     0,   240,   241,     0,   242,     0,     0,   243,   244,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   245,     0,     0,     0,   246,     0,     0,     0,
     247,   248,     0,   249,     0,     0,   250,     0,     0,   204,
     205,   206,   207,   208,   209,   210,     0,     0,     0,   211,
       0,     0,   212,   251,   213,   214,   215,   216,   217,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   218,     0,     0,     0,     0,     0,     0,   219,
     220,     0,     0,   221,   222,   223,   224,   225,     0,     0,
       0,     0,     0,     0,     0,     0,   257,   258,     0,   228,
     229,   230,   231,   232,   233,   589,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   590,     0,   237,   238,     0,   239,     0,     0,
       0,   240,   241,     0,   242,     0,     0,   243,   244,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   245,     0,     0,     0,   246,     0,     0,     0,   247,
     248,     0,   249,     0,     0,   250,     0,     0,   204,   205,
     206,   207,   208,   209,   210,     0,     0,     0,   211,     0,
       0,   212,   251,   213,   214,   215,   216,   217,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   218,     0,     0,     0,     0,     0,     0,   219,   220,
       0,     0,   221,   222,   223,   224,   225,     0,     0,     0,
       0,     0,     0,     0,     0,   257,   258,     0,   228,   229,
     230,   231,   232,   233,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   234,   235,
     236,   607,     0,   237,   238,     0,   239,     0,     0,     0,
     240,   241,     0,   242,     0,     0,   243,   244,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     245,     0,     0,     0,   246,     0,     0,     0,   247,   248,
       0,   249,     0,     0,   250,     0,     0,   204,   205,   206,
     207,   208,   209,   210,     0,     0,     0,   211,     0,     0,
     212,   251,   213,   214,   215,   216,   217,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     218,     0,     0,     0,     0,   612,     0,   219,   220,     0,
       0,   221,   222,   223,   224,   225,     0,     0,     0,     0,
       0,     0,     0,     0,   257,   258,     0,   228,   229,   230,
     231,   232,   233,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   234,   235,   236,
       0,     0,   237,   238,     0,   239,     0,     0,     0,   240,
     241,     0,   242,     0,     0,   243,   244,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   245,
       0,     0,     0,   246,     0,     0,     0,   247,   248,     0,
     249,     0,     0,   250,     0,     0,   204,   205,   206,   207,
     208,   209,   210,     0,     0,     0,   211,     0,     0,   212,
     251,   213,   214,   215,   216,   217,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   218,
       0,     0,     0,     0,     0,     0,   219,   220,     0,     0,
     221,   222,   223,   224,   225,     0,     0,     0,     0,     0,
       0,     0,     0,   257,   258,     0,   228,   229,   230,   231,
     232,   233,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   234,   235,   236,     0,
       0,   237,   238,     0,   239,     0,     0,     0,   240,   241,
       0,   242,     0,     0,   243,   244,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   245,     0,
       0,     0,   246,     0,     0,     0,   247,   248,     0,   249,
       0,     0,   250,     0,     0,   204,   205,   206,   207,   208,
     209,   210,     0,     0,     0,   211,     0,     0,   212,   251,
     213,   214,   215,   216,   217,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   406,     0,
       0,     0,     0,     0,     0,   219,   220,     0,     0,   221,
     222,   223,   224,   225,     0,     0,     0,     0,     0,     0,
       0,     0,   257,   258,     0,   228,   229,   230,   231,   232,
     233,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   234,   235,   236,     0,     0,
     237,   238,     0,   239,     0,     0,     0,   240,   241,     0,
     242,     0,     0,   243,   244,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   245,     0,     0,
       0,   246,     0,     0,     0,   247,   248,     0,   249,     0,
       0,   250,     0,     0,   204,   205,   206,   207,   208,   209,
     210,     0,     0,     0,   211,     0,     0,   212,   251,   213,
     214,   215,   216,   217,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   218,     0,     0,
       0,     0,     0,     0,   219,   220,     0,     0,   221,   222,
     223,   224,   225,     0,     0,     0,     0,     0,     0,     0,
       0,   584,   585,     0,   228,   229,   230,   231,   232,   233,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   234,   235,   236,     0,     0,   237,
     238,     0,   239,     0,     0,     0,   240,   241,     0,   242,
       0,     0,   243,   244,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   245,     0,     0,     0,
     246,     0,     0,     0,   247,   248,     0,   249,     0,     0,
     250,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   251
};

static const yytype_int16 yycheck[] =
{
       9,    10,   148,    83,   150,   149,     0,   343,   473,   474,
     475,   476,    21,    22,   184,    24,    25,    26,    27,    28,
      29,    30,    66,    67,   368,     5,    26,   492,   493,    26,
      74,   496,    45,    26,   499,    41,   495,    46,    47,    41,
      41,    35,     5,     6,     7,     8,     9,    10,   621,    45,
      13,    82,    32,    41,    41,    41,    41,   582,   583,    41,
      41,   413,   587,    41,   133,    44,    41,    82,    45,    32,
      33,    34,    35,    36,    37,    41,    52,    82,    52,    63,
      26,   433,   108,   116,   114,     5,    19,    20,   147,   613,
      82,   615,   616,   617,   142,   144,   145,   145,    28,   151,
     142,   142,   159,    82,    98,   146,   115,   159,   141,   633,
     634,   635,    32,   637,   638,    48,   110,   641,    48,    82,
     114,    95,   116,   180,    70,    78,    94,   208,    61,   171,
     212,   212,   153,   215,   110,   178,   115,   146,   153,   148,
     149,   150,    55,   212,   159,   113,   611,   141,   153,   208,
     142,   142,   182,    28,   202,   108,   621,   622,   623,   624,
     625,   626,   627,   142,   210,   630,   631,   692,   159,   628,
     629,    89,    56,   116,    58,   159,   207,   142,   204,   205,
     704,   207,   166,   167,   142,    42,    43,   130,   131,   132,
      82,   134,   159,   206,   159,   153,   151,   152,   211,   154,
     155,   159,   208,   776,    53,    54,   208,   208,   175,   561,
     206,   211,    90,    91,   211,   110,   209,   226,   227,   114,
     208,   208,   208,   208,   109,    82,   208,   208,   207,   206,
     208,   211,   212,   208,   578,   760,   761,   166,   142,   168,
     178,   411,   208,   252,   207,   139,   140,   141,    19,   208,
     775,   204,   205,   212,   207,   159,   144,   266,   159,   116,
     117,   118,   119,   272,   121,   122,   123,   124,   125,   126,
     110,   280,   142,   130,   114,   319,   320,   321,   322,    64,
      65,   153,   110,   327,   328,   157,   114,   159,   142,   110,
     334,   142,   301,   114,   110,    66,    82,    68,   114,     5,
       6,     7,     8,     9,    10,   178,   179,   180,   142,   353,
     142,   776,   142,   128,   129,   158,   159,   361,    26,    90,
     656,    92,    93,   209,   210,    96,    32,    33,    34,    35,
      36,    37,   211,   212,    26,   106,   107,    82,   109,    25,
      26,    27,    28,    29,    30,   810,   811,    69,    81,    88,
      89,    12,     6,   142,    82,   364,    45,   437,   817,     0,
     207,   370,    40,   211,    67,    97,   207,    47,    52,    82,
     216,   836,   837,    94,   110,   152,   211,   208,    61,   110,
     209,   152,   841,   116,   117,   118,   119,   120,   121,   122,
     123,   124,   125,   210,   127,    26,    30,   130,   211,   211,
     211,    39,   211,    82,   210,   210,   210,   178,   210,   210,
     210,   210,   421,   210,   210,   210,   206,   411,   562,    82,
      82,   159,   142,   110,   142,    82,    82,    82,   572,    82,
     142,   142,   576,   208,    82,   142,   185,    82,    82,   209,
     159,   142,    82,   142,   453,   142,   455,   456,   142,   458,
      45,   208,    82,    82,    26,    26,   465,    82,    91,    26,
      97,   470,   210,    77,   159,   142,   210,   210,   210,   210,
     514,   211,   516,   517,   211,   519,   210,    57,    59,   146,
     210,   210,    82,   527,   210,   210,   210,   210,    70,    82,
     142,    82,    82,   210,   210,   210,   210,     9,    82,   208,
     210,   114,    60,    82,    82,   210,   210,   207,    82,   185,
     212,   209,   203,    19,   210,   142,   142,    82,    66,   212,
      82,    19,   212,   182,   212,    82,    82,   208,   213,    98,
      41,   213,    41,    35,   110,   210,   210,   680,   208,   212,
     212,   212,   210,   212,   210,   210,   696,   679,   212,   455,
     208,   211,   816,   562,   142,   839,   565,   566,   208,   568,
     569,   570,   211,   572,   211,   211,   727,   576,   211,   184,
     774,   210,   214,   582,   583,   584,   585,   212,   587,   211,
     608,   513,   742,   644,   110,   424,   214,    -1,   212,   212,
     210,   212,   601,   212,   212,    -1,    -1,    -1,    -1,   608,
      -1,    -1,    -1,    -1,   648,    -1,    -1,    -1,    -1,   618,
     619,    -1,    -1,   759,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   284,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   280,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   661,   662,    -1,    -1,   802,    -1,    -1,    -1,
      -1,   670,    -1,   672,    -1,    -1,    -1,   676,    -1,   678,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   692,    -1,    -1,    -1,    -1,    -1,   698,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   752,   753,    -1,    -1,    -1,    -1,    -1,
     759,   760,   761,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   774,   775,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
       1,   800,    -1,   802,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,    16,    17,    18,    -1,    -1,
      21,    22,    23,    24,    25,    -1,    -1,    28,    29,    -1,
      31,    32,    33,    34,    35,    36,    37,    38,    -1,    40,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    49,    50,
      51,    -1,    53,    54,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    62,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    79,    80,
      -1,    82,    83,    84,    85,    86,    -1,    88,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    96,    -1,    98,    99,   100,
     101,   102,   103,   104,   105,    -1,    -1,    -1,    -1,    -1,
     111,   112,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   135,   136,    -1,    -1,    -1,    -1,
      -1,    -1,   143,    -1,    -1,    -1,    -1,   148,   149,   150,
      -1,    -1,    -1,    -1,    -1,   156,    -1,    -1,    -1,   160,
     161,   162,   163,   164,   165,    -1,    -1,    -1,   169,   170,
      -1,   172,   173,   174,    -1,   176,   177,    -1,    -1,    -1,
     181,    -1,   183,   184,    -1,   186,   187,   188,   189,   190,
     191,   192,   193,   194,   195,   196,   197,   198,   199,   200,
     201,    -1,    -1,    -1,    -1,   206,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,    18,
      -1,    -1,    21,    22,    23,    24,    25,    -1,    -1,    28,
      29,    -1,    31,    32,    33,    34,    35,    36,    37,    38,
      -1,    40,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      49,    50,    51,    -1,    53,    54,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    62,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      79,    80,    -1,    82,    83,    84,    85,    86,    -1,    88,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    96,    -1,    98,
      99,   100,   101,   102,   103,   104,   105,    -1,    -1,    -1,
      -1,    -1,   111,   112,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   135,   136,    -1,    -1,
      -1,    -1,    -1,    -1,   143,    -1,    -1,    -1,    -1,   148,
     149,   150,    -1,    -1,    -1,    -1,    -1,   156,    -1,    -1,
      -1,   160,   161,   162,   163,   164,   165,    -1,    -1,    -1,
     169,   170,    -1,   172,   173,   174,     5,   176,   177,    -1,
      -1,    -1,   181,    -1,   183,   184,    -1,   186,   187,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   200,   201,    32,    -1,    -1,    -1,   206,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      49,    50,    51,    52,    53,    54,    55,    -1,    -1,    -1,
      59,    -1,    -1,    62,    -1,    64,    65,    66,    67,    68,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,
      89,    90,    -1,    -1,    93,    94,    95,    96,    97,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,   107,    -1,
     109,   110,   111,   112,   113,   114,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     139,   140,   141,    -1,    -1,   144,   145,    -1,   147,    -1,
      -1,    -1,   151,   152,    -1,   154,    -1,    -1,   157,   158,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   171,    -1,    -1,    -1,   175,    -1,    -1,    -1,
     179,   180,    -1,   182,    -1,    -1,   185,    -1,     5,     6,
       7,     8,     9,    10,    11,    12,    13,    14,    15,    16,
      17,    18,    -1,   202,    21,    22,    23,    24,    25,    -1,
      -1,    28,    29,    -1,    31,    32,    33,    34,    35,    36,
      37,    38,    -1,    40,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    49,    50,    51,    -1,    53,    54,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    62,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    79,    80,    -1,    82,    83,    84,    85,    86,
      -1,    88,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    96,
      -1,    98,    99,   100,   101,   102,   103,   104,   105,    -1,
      -1,    -1,    -1,    -1,   111,   112,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   135,   136,
      -1,    -1,    -1,    -1,    -1,    -1,   143,    -1,    -1,    -1,
      -1,   148,   149,   150,    -1,    -1,    -1,    -1,    -1,   156,
      -1,    -1,    -1,   160,   161,   162,   163,   164,   165,    -1,
      -1,    -1,   169,   170,    -1,   172,   173,   174,    -1,   176,
     177,    -1,    -1,    -1,   181,    -1,   183,   184,    -1,   186,
     187,   188,   189,   190,   191,   192,   193,   194,   195,   196,
     197,   198,   199,   200,   201,     5,     6,     7,     8,     9,
      10,    11,    12,    13,    14,    15,    16,    17,    18,    -1,
      -1,    21,    22,    23,    24,    25,    -1,    -1,    28,    29,
      -1,    31,    32,    33,    34,    35,    36,    37,    38,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    49,
      50,    51,    -1,    53,    54,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    62,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    79,
      80,    -1,    82,    83,    84,    85,    86,    -1,    88,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    96,    -1,    98,    99,
     100,   101,   102,   103,   104,   105,    -1,    -1,    -1,    -1,
      -1,   111,   112,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   135,   136,    -1,    -1,    -1,
      -1,    -1,    -1,   143,    -1,    -1,    -1,    -1,   148,   149,
     150,    -1,    -1,    -1,    -1,    -1,   156,    -1,    -1,    -1,
     160,   161,   162,   163,   164,   165,    -1,    -1,    -1,   169,
     170,    -1,   172,   173,   174,    -1,   176,   177,    -1,    -1,
      -1,   181,    -1,   183,   184,    -1,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
     200,   201,    49,    50,    51,    52,    53,    54,    55,    -1,
      -1,    -1,    59,    -1,    -1,    62,    -1,    64,    65,    66,
      67,    68,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,
      -1,    -1,    89,    90,    -1,    -1,    93,    94,    95,    96,
      97,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,
     107,    -1,   109,   110,   111,   112,   113,   114,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   139,   140,   141,    -1,    -1,   144,   145,    -1,
     147,    -1,    -1,    -1,   151,   152,    -1,   154,    -1,    -1,
     157,   158,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   171,    -1,    -1,    -1,   175,    -1,
      -1,    -1,   179,   180,    -1,   182,    -1,    -1,   185,    -1,
      -1,    49,    50,    51,    52,    53,    54,    55,    -1,    -1,
      -1,    59,    -1,    -1,    62,   202,    64,    65,    66,    67,
      68,    -1,    -1,    -1,   211,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,    -1,
      -1,    89,    90,    -1,    -1,    93,    94,    95,    96,    97,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,   107,
      -1,   109,   110,   111,   112,   113,   114,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   139,   140,   141,    -1,    -1,   144,   145,    -1,   147,
      -1,    -1,    -1,   151,   152,    -1,   154,    -1,    -1,   157,
     158,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   171,    -1,    -1,    -1,   175,    -1,    -1,
      -1,   179,   180,    -1,   182,    41,    -1,   185,    -1,    -1,
      -1,    -1,    -1,    49,    50,    51,    52,    53,    54,    55,
      -1,    -1,    -1,    59,   202,    -1,    62,    -1,    64,    65,
      66,    67,    68,   211,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,    -1,
      -1,    -1,    -1,    89,    90,    -1,    -1,    93,    94,    95,
      96,    97,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     106,   107,    -1,   109,   110,   111,   112,   113,   114,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   139,   140,   141,    -1,    -1,   144,   145,
      -1,   147,    -1,    -1,    -1,   151,   152,    -1,   154,    -1,
      -1,   157,   158,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   171,    -1,    -1,    -1,   175,
      -1,    -1,    -1,   179,   180,    -1,   182,    -1,    -1,   185,
      -1,    -1,    49,    50,    51,    52,    53,    54,    55,    -1,
      -1,    -1,    59,    -1,    -1,    62,   202,    64,    65,    66,
      67,    68,    69,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,
      -1,    -1,    89,    90,    -1,    -1,    93,    94,    95,    96,
      97,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,
     107,    -1,   109,   110,   111,   112,   113,   114,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   139,   140,   141,    -1,    -1,   144,   145,    -1,
     147,    -1,    -1,    -1,   151,   152,    -1,   154,    -1,    -1,
     157,   158,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   171,    -1,    -1,    -1,   175,    -1,
      -1,    -1,   179,   180,    -1,   182,    -1,    -1,   185,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    49,    50,
      51,    52,    53,    54,    55,   202,    -1,    -1,    59,    -1,
     207,    62,   209,    64,    65,    66,    67,    68,    69,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,    89,    90,
      -1,    -1,    93,    94,    95,    96,    97,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   106,   107,    -1,   109,   110,
     111,   112,   113,   114,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   139,   140,
     141,    -1,    -1,   144,   145,    -1,   147,    -1,    -1,    -1,
     151,   152,    -1,   154,    -1,    -1,   157,   158,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     171,    -1,    -1,    -1,   175,    -1,    -1,    -1,   179,   180,
      -1,   182,    -1,    -1,   185,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    49,    50,    51,    52,    53,    54,    55,    -1,
      -1,   202,    59,    -1,    -1,    62,   207,    64,    65,    66,
      67,    68,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,
      -1,    -1,    89,    90,    -1,    -1,    93,    94,    95,    96,
      97,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,
     107,    -1,   109,   110,   111,   112,   113,   114,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   139,   140,   141,    -1,    -1,   144,   145,    -1,
     147,    -1,    -1,    -1,   151,   152,    -1,   154,    -1,    -1,
     157,   158,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   171,    -1,    -1,    -1,   175,    -1,
      -1,    -1,   179,   180,    -1,   182,    -1,    -1,   185,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    49,    50,    51,    52,
      53,    54,    55,    -1,    -1,   202,    59,    -1,    -1,    62,
     207,    64,    65,    66,    67,    68,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,
      -1,    -1,    -1,    -1,    -1,    -1,    89,    90,    -1,    -1,
      93,    94,    95,    96,    97,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   106,   107,    -1,   109,   110,   111,   112,
     113,   114,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   139,   140,   141,    -1,
      -1,   144,   145,    -1,   147,    -1,    -1,    -1,   151,   152,
      -1,   154,    -1,    -1,   157,   158,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   171,    -1,
      -1,    -1,   175,    -1,    -1,    -1,   179,   180,    -1,   182,
      -1,    -1,   185,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      49,    50,    51,    52,    53,    54,    55,    -1,    -1,   202,
      59,    -1,    61,    62,   207,    64,    65,    66,    67,    68,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,
      89,    90,    -1,    -1,    93,    94,    95,    96,    97,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,   107,    -1,
     109,   110,   111,   112,   113,   114,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   126,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   137,   138,
     139,   140,   141,   142,    -1,   144,   145,    -1,   147,    -1,
      -1,    -1,   151,   152,    -1,   154,    -1,    -1,   157,   158,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   171,    -1,    -1,    -1,   175,    -1,    -1,    -1,
     179,   180,    -1,   182,    -1,    -1,   185,    -1,    -1,    49,
      50,    51,    52,    53,    54,    55,    -1,    -1,    -1,    59,
      -1,    -1,    62,   202,    64,    65,    66,    67,    68,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,    89,
      90,    -1,    -1,    93,    94,    95,    96,    97,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   106,   107,    -1,   109,
     110,   111,   112,   113,   114,   115,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   139,
     140,   141,   142,    -1,   144,   145,    -1,   147,    -1,    -1,
      -1,   151,   152,    -1,   154,    -1,    -1,   157,   158,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   171,    -1,    -1,    -1,   175,    -1,    -1,    -1,   179,
     180,    -1,   182,    -1,    -1,   185,    -1,    -1,    49,    50,
      51,    52,    53,    54,    55,    -1,    -1,    -1,    59,    -1,
      -1,    62,   202,    64,    65,    66,    67,    68,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,    89,    90,
      -1,    -1,    93,    94,    95,    96,    97,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   106,   107,    -1,   109,   110,
     111,   112,   113,   114,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   139,   140,
     141,   142,    -1,   144,   145,    -1,   147,    -1,    -1,    -1,
     151,   152,    -1,   154,    -1,    -1,   157,   158,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     171,    -1,    -1,    -1,   175,    -1,    -1,    -1,   179,   180,
      -1,   182,    -1,    -1,   185,    -1,    -1,    49,    50,    51,
      52,    53,    54,    55,    -1,    -1,    -1,    59,    -1,    -1,
      62,   202,    64,    65,    66,    67,    68,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      82,    -1,    -1,    -1,    -1,    87,    -1,    89,    90,    -1,
      -1,    93,    94,    95,    96,    97,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   106,   107,    -1,   109,   110,   111,
     112,   113,   114,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   139,   140,   141,
      -1,    -1,   144,   145,    -1,   147,    -1,    -1,    -1,   151,
     152,    -1,   154,    -1,    -1,   157,   158,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   171,
      -1,    -1,    -1,   175,    -1,    -1,    -1,   179,   180,    -1,
     182,    -1,    -1,   185,    -1,    -1,    49,    50,    51,    52,
      53,    54,    55,    -1,    -1,    -1,    59,    -1,    -1,    62,
     202,    64,    65,    66,    67,    68,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,
      -1,    -1,    -1,    -1,    -1,    -1,    89,    90,    -1,    -1,
      93,    94,    95,    96,    97,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   106,   107,    -1,   109,   110,   111,   112,
     113,   114,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   139,   140,   141,    -1,
      -1,   144,   145,    -1,   147,    -1,    -1,    -1,   151,   152,
      -1,   154,    -1,    -1,   157,   158,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   171,    -1,
      -1,    -1,   175,    -1,    -1,    -1,   179,   180,    -1,   182,
      -1,    -1,   185,    -1,    -1,    49,    50,    51,    52,    53,
      54,    55,    -1,    -1,    -1,    59,    -1,    -1,    62,   202,
      64,    65,    66,    67,    68,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,    -1,
      -1,    -1,    -1,    -1,    -1,    89,    90,    -1,    -1,    93,
      94,    95,    96,    97,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   106,   107,    -1,   109,   110,   111,   112,   113,
     114,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   139,   140,   141,    -1,    -1,
     144,   145,    -1,   147,    -1,    -1,    -1,   151,   152,    -1,
     154,    -1,    -1,   157,   158,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   171,    -1,    -1,
      -1,   175,    -1,    -1,    -1,   179,   180,    -1,   182,    -1,
      -1,   185,    -1,    -1,    49,    50,    51,    52,    53,    54,
      55,    -1,    -1,    -1,    59,    -1,    -1,    62,   202,    64,
      65,    66,    67,    68,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,
      -1,    -1,    -1,    -1,    89,    90,    -1,    -1,    93,    94,
      95,    96,    97,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   106,   107,    -1,   109,   110,   111,   112,   113,   114,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   139,   140,   141,    -1,    -1,   144,
     145,    -1,   147,    -1,    -1,    -1,   151,   152,    -1,   154,
      -1,    -1,   157,   158,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   171,    -1,    -1,    -1,
     175,    -1,    -1,    -1,   179,   180,    -1,   182,    -1,    -1,
     185,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   202
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
     101,   102,   103,   104,   105,   111,   112,   135,   136,   143,
     148,   149,   150,   156,   160,   161,   162,   163,   164,   165,
     169,   170,   172,   173,   174,   176,   177,   181,   183,   184,
     186,   187,   188,   189,   190,   191,   192,   193,   194,   195,
     196,   197,   198,   199,   200,   201,   206,   218,   219,   220,
     221,   222,   223,   228,   229,   230,   231,   232,   235,   236,
     241,   242,   243,   244,   245,   246,   247,   250,   251,   252,
     253,   255,   258,   259,   260,   261,   262,   263,   264,   265,
     266,   267,   268,   269,   270,   281,   282,   283,   284,   285,
     286,   290,   291,   306,   307,   308,   309,   310,   311,   313,
     314,   315,   323,   324,   326,   330,   332,   334,   335,   336,
     338,   340,   341,   342,   343,   345,   346,   347,   348,   349,
     351,   352,   354,   355,   358,   359,   360,   361,   362,   363,
     365,   369,   370,   371,    78,   108,   204,   205,   207,   153,
     312,    19,    66,    68,    90,    92,    93,    96,   106,   107,
     109,   178,   325,   364,    49,    50,    51,    52,    53,    54,
      55,    59,    62,    64,    65,    66,    67,    68,    82,    89,
      90,    93,    94,    95,    96,    97,   106,   107,   109,   110,
     111,   112,   113,   114,   139,   140,   141,   144,   145,   147,
     151,   152,   154,   157,   158,   171,   175,   179,   180,   182,
     185,   202,   207,   287,   288,   375,   376,   106,   107,   271,
     376,    19,    20,    48,    61,   233,    28,    48,   234,   178,
     376,   376,    28,    61,   126,   137,   138,   142,   316,   317,
     320,   376,   316,   316,   316,   316,   316,   316,    81,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   127,
     130,   292,    53,   221,    55,    53,    54,   210,    89,   376,
     376,    82,   109,   178,   142,   142,   144,   145,   333,   151,
     152,   154,   155,   339,   151,   159,   337,   153,   157,   159,
     328,   142,   144,   331,   180,   328,   328,    82,   153,   344,
      63,   159,   166,   167,   329,    82,   142,   171,   142,   146,
     327,   142,   350,   175,   328,   142,   353,   178,   179,   180,
     142,   357,    26,    70,    26,   254,    26,   257,   257,    82,
      69,   254,   254,    12,     6,   142,    82,   142,   145,   202,
      45,     0,   220,    45,   206,     5,   207,   223,   237,   239,
     240,   270,   281,   282,   283,   284,   285,   371,   207,   238,
     223,   281,   282,   283,   284,   285,    82,   376,   222,   222,
     290,   211,   249,   274,   275,   276,   223,   277,   278,   373,
     374,   376,    67,   322,   374,   277,   374,    90,    91,   225,
     226,   227,   249,   274,    97,   207,    47,    52,    82,   216,
      52,    95,    94,    94,   113,    52,   110,   110,   152,    64,
      65,   376,   376,   207,   376,   208,   210,   211,   209,    61,
     376,   110,    26,    30,   376,   211,   211,   211,    39,   317,
     211,   312,    82,   210,   210,   210,   210,    42,    43,    82,
     116,   117,   118,   119,   121,   122,   123,   124,   125,   126,
     130,   300,   210,   210,   210,   210,   210,   128,   129,   210,
      41,   293,   376,   206,    56,    58,   366,   367,   368,    82,
      82,   142,   110,   142,   142,   328,   142,   153,   328,   142,
     328,   328,    82,   153,   159,    82,   328,   158,   328,    82,
     328,    82,    82,   329,   142,   142,   208,   328,   142,   328,
      82,   185,   376,    82,   159,   256,   256,   209,   376,    82,
      82,   142,   142,   142,   142,    45,    82,    82,   208,   212,
     223,   274,    26,   248,   275,   166,   168,   372,    69,   207,
     209,   376,    26,   372,    91,    82,    26,    97,   257,   210,
      77,   159,   210,   210,   106,   107,   376,   210,   287,   115,
     142,   289,   376,    44,    82,   115,   142,   207,   272,   273,
     376,   376,   142,   139,   140,   141,   321,   142,   318,   319,
     376,   210,    87,   305,   376,   305,   305,   305,   211,   211,
     209,   210,   210,   210,   210,   210,   210,   210,   210,   210,
     210,   210,   299,   305,   305,   303,   305,   303,   305,   210,
     210,   305,    57,    59,   208,   327,   328,   328,   142,   328,
     328,    82,   328,   147,   208,   142,   114,   182,   356,    70,
     210,    26,   209,    82,    82,   210,   207,   212,   212,   277,
     211,   376,   211,   376,   376,   376,   376,   277,   211,   279,
      82,     9,   224,   277,   256,    82,   114,   203,   289,   289,
     376,   376,   210,   289,    82,   207,   208,   212,   209,   376,
     212,   212,   319,   212,   304,   305,   299,   299,   299,   299,
     376,   301,   302,   376,    82,   304,   305,   305,   305,   305,
     305,   305,   303,   303,   305,   305,    41,   208,   299,   299,
     299,   299,   299,   213,   213,   299,    60,   368,   328,   142,
     142,   208,   329,   185,    82,   376,   376,    82,   142,    82,
     376,   376,    69,   207,   376,   280,   376,   278,   224,    19,
     210,   210,   289,    82,   273,   376,   212,   299,    41,    41,
      41,    41,   212,   212,   208,   210,   210,   300,    41,    41,
      41,    41,    41,   116,   130,   131,   132,   134,   294,   295,
      66,   296,    41,    82,   356,   210,   212,   212,   376,   376,
     208,   212,    19,   374,   289,   289,    41,   301,   289,   304,
     211,   211,   211,   211,   211,   214,   208,   211,   214,    82,
     376,   374,   305,   305,   133,   212,   212,    82,   297,   298,
      41,   294,   303,    41,   212,   212,   210,   210,   212,   208,
     212,   215,   305,   305,   297,   303,   212,   212
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

  case 196:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_USER);
        ;}
    break;

  case 197:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 198:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 199:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(5) - (5)].var_node), (yyvsp[(3) - (5)].str), SET_VAR_SYS);
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
                                                  HANDLE_ACCEPT();
                                                ;}
    break;

  case 463:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 464:

    {
                                                  result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                                                  result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                                                  result->table_info_.dblink_name_ = (yyvsp[(5) - (5)].str);
                                                 ;}
    break;

  case 465:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 466:

    {
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.dblink_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 467:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 468:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 469:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 470:

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

