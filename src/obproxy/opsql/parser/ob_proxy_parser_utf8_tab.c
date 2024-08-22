
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
#define yyparse         ob_proxy_parser_utf8_yyparse
#define yylex           ob_proxy_parser_utf8_yylex
#define yyerror         ob_proxy_parser_utf8_yyerror
#define yylval          ob_proxy_parser_utf8_yylval
#define yychar          ob_proxy_parser_utf8_yychar
#define yydebug         ob_proxy_parser_utf8_yydebug
#define yynerrs         ob_proxy_parser_utf8_yynerrs
#define yylloc          ob_proxy_parser_utf8_yylloc

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
    result->end_pos_ = ob_proxy_parser_utf8_yyget_text(result->yyscan_info_);\
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
#ifndef OBPROXY_UTF8_DEBUG
# if defined YYDEBUG
#if YYDEBUG
#   define OBPROXY_UTF8_DEBUG 1
#  else
#   define OBPROXY_UTF8_DEBUG 0
#  endif
# else /* ! defined YYDEBUG */
#  define OBPROXY_UTF8_DEBUG 0
# endif /* ! defined YYDEBUG */
#endif  /* ! defined OBPROXY_UTF8_DEBUG */
#if OBPROXY_UTF8_DEBUG
extern int ob_proxy_parser_utf8_yydebug;
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
     SHOW_PROXYCLUSTER = 418,
     SHOW_PROXYRESOURCE = 419,
     SHOW_PROXYCONGESTION = 420,
     SHOW_PROXYROUTE = 421,
     PARTITION = 422,
     ROUTINE = 423,
     SUBPARTITION = 424,
     SHOW_PROXYVIP = 425,
     SHOW_PROXYMEMORY = 426,
     OBJPOOL = 427,
     SHOW_SQLAUDIT = 428,
     SHOW_WARNLOG = 429,
     SHOW_PROXYSTAT = 430,
     REFRESH = 431,
     SHOW_PROXYTRACE = 432,
     SHOW_PROXYINFO = 433,
     BINARY = 434,
     UPGRADE = 435,
     IDC = 436,
     SHOW_ELASTIC_ID = 437,
     SHOW_TOPOLOGY = 438,
     GROUP_NAME = 439,
     SHOW_DB_VERSION = 440,
     SHOW_DATABASES = 441,
     SHOW_TABLES = 442,
     SHOW_FULL_TABLES = 443,
     SELECT_DATABASE = 444,
     SELECT_PROXY_STATUS = 445,
     SHOW_CREATE_TABLE = 446,
     SELECT_PROXY_VERSION = 447,
     SHOW_COLUMNS = 448,
     SHOW_INDEX = 449,
     ALTER_PROXYCONFIG = 450,
     ALTER_PROXYRESOURCE = 451,
     PING_PROXY = 452,
     KILL_PROXYSESSION = 453,
     KILL_GLOBALSESSION = 454,
     KILL = 455,
     QUERY = 456,
     BINLOG_VARIABLE = 457,
     BINLOG_USER_VAR = 458,
     BINLOG_SYS_VAR = 459
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


#include "ob_proxy_parser_utf8_lex.h"
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
#define YYFINAL  373
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   3172

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  216
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  154
/* YYNRULES -- Number of rules.  */
#define YYNRULES  508
/* YYNRULES -- Number of states.  */
#define YYNSTATES  826

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   459

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,   214,     2,     2,     2,     2,
     210,   211,   215,     2,   207,     2,   208,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   205,
       2,   209,     2,     2,   206,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   212,     2,   213,     2,     2,     2,     2,
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
     195,   196,   197,   198,   199,   200,   201,   202,   203,   204
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
    1025,  1026,  1028,  1030,  1032,  1035,  1041,  1047,  1050,  1054,
    1058,  1059,  1062,  1067,  1072,  1073,  1076,  1077,  1080,  1083,
    1085,  1087,  1091,  1094,  1098,  1102,  1107,  1109,  1112,  1113,
    1116,  1120,  1123,  1126,  1129,  1130,  1133,  1137,  1140,  1144,
    1147,  1151,  1155,  1160,  1163,  1165,  1168,  1171,  1175,  1178,
    1181,  1182,  1184,  1186,  1189,  1192,  1196,  1199,  1202,  1204,
    1207,  1209,  1212,  1215,  1219,  1222,  1225,  1228,  1229,  1231,
    1235,  1241,  1244,  1248,  1251,  1252,  1254,  1257,  1260,  1263,
    1266,  1271,  1277,  1283,  1287,  1289,  1292,  1296,  1300,  1303,
    1306,  1310,  1314,  1315,  1318,  1320,  1324,  1328,  1332,  1333,
    1335,  1337,  1341,  1344,  1348,  1351,  1354,  1356,  1357,  1360,
    1365,  1368,  1373,  1376,  1378,  1384,  1388,  1392,  1395,  1400,
    1404,  1410,  1412,  1414,  1416,  1418,  1420,  1422,  1424,  1426,
    1428,  1430,  1432,  1434,  1436,  1438,  1440,  1442,  1444,  1446,
    1448,  1450,  1452,  1454,  1456,  1458,  1460,  1462,  1464,  1466,
    1468,  1470,  1472,  1474,  1476,  1478,  1480,  1482,  1484,  1486,
    1488,  1490,  1492,  1494,  1496,  1498,  1500,  1502,  1504
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     217,     0,    -1,   218,    -1,     1,    -1,   219,    -1,   218,
     219,    -1,   220,    45,    -1,   220,   205,    -1,   220,   205,
      45,    -1,   205,    -1,   205,    45,    -1,    53,   220,   205,
      -1,   221,    -1,   290,   221,    -1,   222,    -1,   280,    -1,
     286,    -1,   281,    -1,   282,    -1,   283,    -1,   229,    -1,
     228,    -1,   358,    -1,   321,    -1,   251,    -1,   322,    -1,
     362,    -1,   363,    -1,   263,    -1,   264,    -1,   265,    -1,
     266,    -1,   267,    -1,   268,    -1,   269,    -1,   230,    -1,
     242,    -1,   284,    -1,   324,    -1,   227,    -1,   364,    -1,
     304,    -1,   305,    -1,   306,   248,   247,    -1,    -1,     9,
      -1,    91,    82,   223,    19,   367,    -1,    90,    91,    82,
     223,    19,   367,    -1,   225,    -1,   224,    -1,   313,   226,
      -1,   244,   222,    -1,   244,   280,    -1,   244,   282,    -1,
     244,   283,    -1,   244,   281,    -1,   244,   284,    -1,   246,
     221,    -1,   231,    -1,   243,    -1,    14,   232,    -1,    15,
     233,    -1,    16,    -1,    17,    -1,    18,    -1,   234,    -1,
     235,    -1,    -1,    19,    -1,    61,    -1,    20,    61,    -1,
      48,    -1,    -1,    48,    -1,   137,   144,    -1,   138,   144,
      -1,   222,    -1,   280,    -1,   281,    -1,   283,    -1,   282,
      -1,   364,    -1,   269,    -1,   284,    -1,   206,    82,    -1,
     237,   207,   206,    82,    -1,   206,    82,    -1,   238,    -1,
     236,    -1,    28,   369,    26,    -1,    29,   369,    -1,    29,
     369,    30,    -1,   240,   239,    -1,   241,   237,    -1,    15,
      28,   369,    -1,    31,    28,   369,    -1,    21,    -1,    22,
      -1,    23,    -1,    24,    -1,    49,    -1,    25,    -1,    50,
      -1,    51,    -1,   245,    -1,   245,    82,    -1,    83,    -1,
      85,    -1,    86,    -1,    84,    -1,    -1,    26,   276,    -1,
      -1,   273,    -1,     5,    78,    -1,     5,    78,   248,    26,
     276,    -1,     5,    78,   273,    -1,   192,    -1,   192,    69,
     369,    -1,   249,    -1,   250,    -1,   261,    -1,   262,    -1,
     252,    -1,   260,    -1,   183,   253,    -1,   259,    -1,   189,
      -1,   190,    -1,   186,    -1,   257,    -1,   258,    -1,   193,
     253,    -1,   194,   253,    -1,   254,    -1,   245,   369,    -1,
      26,   369,    -1,    26,   369,    26,   369,    -1,    26,   369,
     208,   369,    -1,   191,    82,    -1,   191,    82,   208,    82,
      -1,    -1,   161,    82,    -1,    -1,    26,    82,    -1,   187,
     256,   255,    -1,   188,   256,   255,    -1,    11,    19,    52,
     256,   255,    -1,   185,    -1,   182,    -1,   182,    26,    82,
      -1,   182,    70,   184,   209,    82,    -1,   182,    26,    82,
      70,   184,   209,    82,    -1,    79,    -1,    80,   209,   144,
      -1,   100,    -1,   101,    -1,   102,    -1,   103,    -1,   104,
      -1,   105,    -1,    13,   270,   210,   271,   211,    -1,   369,
      -1,   369,   208,   369,    -1,   369,   208,   369,   208,   369,
      -1,    -1,   272,    -1,   271,   207,   272,    -1,    82,    -1,
     144,    -1,   117,    -1,   206,    82,    -1,   206,   206,    82,
      -1,    44,    -1,   274,    -1,   273,   274,    -1,   275,    -1,
     210,   211,    -1,   210,   222,   211,    -1,   210,   273,   211,
      -1,   366,    -1,   277,    -1,   222,    -1,    -1,   210,   279,
     211,    -1,   369,    -1,   279,   207,   369,    -1,   309,   367,
     365,    -1,   309,   367,   365,   278,   277,    -1,   311,   276,
      -1,   307,   276,    -1,   308,   320,    26,   276,    -1,   312,
     367,    -1,   108,    -1,   109,    -1,   110,    -1,    12,   287,
      -1,   288,   207,   287,    -1,   288,    -1,   206,   369,   209,
     289,    -1,   206,   206,   106,   369,   209,   289,    -1,   106,
     369,   209,   289,    -1,   206,   206,   369,   209,   289,    -1,
     206,   206,   107,   369,   209,   289,    -1,   107,   369,   209,
     289,    -1,   369,   209,   289,    -1,   369,    -1,   144,    -1,
     117,    -1,   291,    -1,   291,   290,    -1,    40,   292,    41,
      -1,    40,   122,   300,   299,    41,    -1,    40,   119,   209,
     303,   299,    41,    -1,    40,   132,   209,   303,   299,    41,
      -1,    40,   118,   209,   303,   299,    41,    -1,    40,   120,
     209,   303,   299,    41,    -1,    40,   121,   209,   303,   299,
      41,    -1,    40,    81,    82,   209,   302,   299,    41,    -1,
      40,   125,   209,   301,   299,    41,    -1,    40,   126,   209,
     301,   299,    41,    -1,    40,   123,   209,   303,   299,    41,
      -1,    40,   124,   209,   303,   299,    41,    -1,    40,   129,
     130,   209,   212,   294,   213,    41,    -1,    40,   129,   131,
     209,   212,   296,   213,    41,    -1,    40,   127,   209,   303,
     299,    41,    -1,    -1,   292,   293,    -1,    42,    82,    -1,
      43,    82,    -1,    82,    -1,   295,   207,   294,    -1,   295,
      -1,   118,   210,   303,   211,    -1,   132,   210,   303,   211,
      -1,   133,   210,   211,    -1,   133,   210,   135,   209,   303,
     211,    -1,   134,   210,   211,    -1,   136,   210,   297,   211,
      -1,    66,   210,   301,   211,    -1,    66,   210,   301,   214,
     301,   211,    -1,   298,   207,   297,    -1,   298,    -1,    82,
     209,   303,    -1,    -1,   299,   207,   300,    -1,   118,   209,
     303,    -1,   119,   209,   303,    -1,   132,   209,   303,    -1,
     120,   209,   303,    -1,   121,   209,   303,    -1,   125,   209,
     301,    -1,   126,   209,   301,    -1,   123,   209,   303,    -1,
     124,   209,   303,    -1,   128,    -1,   127,   209,   303,    -1,
      82,   208,    82,   209,   302,    -1,    82,   209,   302,    -1,
      -1,   303,    -1,    -1,   303,    -1,    82,    -1,    87,    -1,
       5,   206,   203,    -1,     5,   285,   204,    -1,     5,   206,
     206,   204,    -1,     5,   108,    97,    -1,     5,   206,   206,
      97,    -1,     5,    -1,    32,   314,    -1,     8,    -1,    33,
     314,    -1,     6,    -1,    34,   314,    -1,     7,   310,    -1,
      35,   314,   310,    -1,    -1,   155,    -1,   155,    47,    -1,
       9,    -1,    36,   314,    -1,    10,    -1,    37,   314,    -1,
      88,    89,    -1,    38,   314,    -1,   315,    39,    -1,    -1,
     318,   315,    -1,   144,    -1,   369,    -1,    -1,   316,   317,
      -1,   139,   210,   144,   211,    -1,   140,   210,   319,   211,
      -1,    61,   210,   369,   369,   211,    -1,   128,    -1,   369,
     210,   317,   211,    -1,   369,    -1,   144,    -1,    -1,   141,
      -1,   142,    -1,   143,    -1,    -1,    67,    -1,    11,   357,
      64,    -1,    11,   357,    65,    -1,    11,    66,    -1,    11,
      66,    82,   209,    82,    -1,    11,    92,    95,    -1,    11,
      92,    52,    -1,    11,    93,    94,    -1,    11,   111,    52,
      -1,    11,   179,   112,    -1,    11,    96,    94,    -1,   328,
      -1,   330,    -1,   331,    -1,   334,    -1,   332,    -1,   336,
      -1,   337,    -1,   338,    -1,   339,    -1,   341,    -1,   342,
      -1,   343,    -1,   344,    -1,   345,    -1,   347,    -1,   348,
      -1,   350,    -1,   351,    -1,   352,    -1,   353,    -1,   354,
      -1,   355,    -1,   356,    -1,    -1,   106,    -1,   107,    -1,
      90,    -1,    96,   369,    -1,    11,    96,   115,    77,   116,
      -1,    11,   323,   154,   161,   202,    -1,   113,   111,    -1,
      24,   179,   112,    -1,   114,   179,   112,    -1,    -1,   148,
     144,    -1,   148,   144,   207,   144,    -1,   148,   144,   149,
     144,    -1,    -1,   161,    82,    -1,    -1,   161,    82,    -1,
     145,   329,    -1,   146,    -1,   147,    -1,   147,   144,   325,
      -1,   158,   326,    -1,   158,   155,   326,    -1,   158,   159,
     326,    -1,   158,   159,   160,   326,    -1,   150,    -1,   152,
     333,    -1,    -1,   153,    82,    -1,   153,   161,    82,    -1,
     153,   155,    -1,   161,    82,    -1,   151,   335,    -1,    -1,
     153,   326,    -1,   153,   144,   326,    -1,   156,   326,    -1,
     156,   144,   326,    -1,   154,   326,    -1,   154,   144,   326,
      -1,   154,   155,   326,    -1,   154,   155,   144,   326,    -1,
     157,   326,    -1,   162,    -1,   162,   144,    -1,   163,   326,
      -1,   163,   181,   326,    -1,   164,   326,    -1,   165,   340,
      -1,    -1,    82,    -1,   155,    -1,   155,    82,    -1,   166,
     327,    -1,   166,   168,   327,    -1,   166,   167,    -1,   166,
      63,    -1,   170,    -1,   170,    82,    -1,   171,    -1,   171,
     144,    -1,   171,   172,    -1,   171,   172,   144,    -1,   173,
     325,    -1,   173,   144,    -1,   174,   346,    -1,    -1,   144,
      -1,   144,   207,   144,    -1,   144,   207,   144,   207,    82,
      -1,   175,   326,    -1,   175,   176,   326,    -1,   177,   349,
      -1,    -1,   144,    -1,   144,   144,    -1,   178,   179,    -1,
     178,   180,    -1,   178,   181,    -1,   195,    12,    82,   209,
      -1,   195,    12,    82,   209,    82,    -1,   195,    12,    82,
     209,   144,    -1,   196,     6,    82,    -1,   197,    -1,   198,
     144,    -1,   198,   144,   144,    -1,   199,    82,   144,    -1,
     199,    82,    -1,   200,   144,    -1,   200,   147,   144,    -1,
     200,   201,   144,    -1,    -1,    68,   215,    -1,    53,    -1,
      54,    55,   359,    -1,    62,    53,    82,    -1,    62,    54,
      82,    -1,    -1,   360,    -1,   361,    -1,   360,   207,   361,
      -1,    56,    57,    -1,    58,    59,    60,    -1,    98,   369,
      -1,    99,    82,    -1,    82,    -1,    -1,   169,   369,    -1,
     169,   210,   369,   211,    -1,   167,   369,    -1,   167,   210,
     369,   211,    -1,   367,   365,    -1,   369,    -1,   369,   208,
     369,   206,   369,    -1,   369,   208,   369,    -1,   369,   206,
     369,    -1,   369,   369,    -1,   369,   208,   369,   369,    -1,
     369,    69,   369,    -1,   369,   208,   369,    69,   369,    -1,
      54,    -1,    62,    -1,    53,    -1,    55,    -1,    59,    -1,
      65,    -1,    64,    -1,    68,    -1,    67,    -1,    66,    -1,
     146,    -1,   147,    -1,   149,    -1,   153,    -1,   154,    -1,
     156,    -1,   159,    -1,   160,    -1,   172,    -1,   176,    -1,
     180,    -1,   181,    -1,   201,    -1,   184,    -1,    49,    -1,
      50,    -1,    51,    -1,    90,    -1,    89,    -1,    52,    -1,
     141,    -1,   142,    -1,   143,    -1,   106,    -1,   107,    -1,
      95,    -1,    94,    -1,    93,    -1,    96,    -1,    97,    -1,
     111,    -1,   112,    -1,   113,    -1,   114,    -1,   115,    -1,
     116,    -1,    82,    -1,   368,    -1
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
     749,   750,   751,   756,   757,   758,   760,   762,   763,   765,
     769,   773,   777,   781,   785,   789,   793,   798,   803,   809,
     810,   812,   813,   814,   815,   816,   817,   818,   819,   825,
     826,   827,   828,   829,   830,   831,   832,   833,   835,   836,
     837,   839,   840,   842,   847,   852,   853,   854,   855,   857,
     858,   860,   861,   863,   871,   872,   874,   875,   876,   877,
     878,   879,   880,   881,   882,   883,   884,   885,   891,   893,
     894,   896,   897,   899,   900,   902,   903,   904,   906,   907,
     909,   910,   911,   912,   913,   914,   915,   916,   918,   919,
     920,   922,   923,   924,   925,   926,   927,   929,   930,   931,
     933,   934,   936,   937,   939,   940,   941,   946,   947,   948,
     949,   951,   952,   953,   954,   956,   957,   960,   961,   962,
     963,   964,   965,   970,   971,   972,   973,   977,   978,   979,
     980,   981,   982,   983,   984,   985,   986,   987,   988,   989,
     990,   991,   992,   993,   994,   995,   996,   997,   998,   999,
    1001,  1002,  1003,  1004,  1007,  1008,  1013,  1014,  1015,  1016,
    1021,  1023,  1027,  1032,  1040,  1041,  1045,  1046,  1049,  1051,
    1052,  1053,  1057,  1058,  1059,  1060,  1065,  1067,  1069,  1070,
    1071,  1072,  1073,  1076,  1078,  1079,  1080,  1081,  1082,  1083,
    1084,  1085,  1086,  1087,  1091,  1092,  1096,  1097,  1102,  1105,
    1107,  1108,  1109,  1110,  1114,  1115,  1116,  1117,  1121,  1122,
    1126,  1127,  1128,  1129,  1133,  1134,  1137,  1139,  1140,  1141,
    1142,  1146,  1147,  1150,  1152,  1153,  1154,  1158,  1159,  1160,
    1164,  1165,  1166,  1170,  1174,  1178,  1179,  1183,  1184,  1188,
    1189,  1190,  1193,  1194,  1197,  1201,  1202,  1203,  1205,  1206,
    1208,  1209,  1212,  1213,  1216,  1222,  1225,  1227,  1228,  1229,
    1230,  1231,  1233,  1238,  1241,  1246,  1250,  1254,  1258,  1263,
    1267,  1273,  1274,  1275,  1276,  1277,  1278,  1279,  1280,  1281,
    1282,  1283,  1284,  1285,  1286,  1287,  1288,  1289,  1290,  1291,
    1292,  1293,  1294,  1295,  1296,  1297,  1298,  1299,  1300,  1301,
    1302,  1303,  1304,  1305,  1306,  1307,  1308,  1309,  1310,  1311,
    1312,  1313,  1314,  1315,  1316,  1317,  1318,  1320,  1321
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
  "binlog_stmt", "opt_limit", "opt_like", "opt_large_like",
  "show_proxynet", "opt_show_net", "show_proxyconfig", "show_processlist",
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
     445,   446,   447,   448,   449,   450,   451,   452,   453,   454,
     455,   456,   457,   458,   459,    59,    64,    44,    46,    61,
      40,    41,   123,   125,    35,    42
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   216,   217,   217,   218,   218,   219,   219,   219,   219,
     219,   219,   220,   220,   221,   221,   221,   221,   221,   221,
     221,   221,   221,   221,   221,   221,   221,   221,   221,   221,
     221,   221,   221,   221,   221,   221,   221,   221,   221,   221,
     221,   222,   222,   222,   223,   223,   224,   225,   226,   226,
     227,   228,   228,   228,   228,   228,   228,   229,   230,   230,
     231,   231,   231,   231,   231,   231,   231,   232,   232,   232,
     232,   232,   233,   233,   234,   235,   236,   236,   236,   236,
     236,   236,   236,   236,   237,   237,   238,   239,   239,   240,
     241,   241,   242,   242,   242,   242,   243,   243,   243,   243,
     243,   243,   243,   243,   244,   244,   245,   245,   245,   246,
     247,   247,   248,   248,   249,   249,   249,   250,   250,   251,
     251,   251,   251,   251,   252,   252,   252,   252,   252,   252,
     252,   252,   252,   252,   252,   252,   253,   253,   253,   254,
     254,   255,   255,   256,   256,   257,   257,   258,   259,   260,
     260,   260,   260,   261,   262,   263,   264,   265,   266,   267,
     268,   269,   270,   270,   270,   271,   271,   271,   272,   272,
     272,   272,   272,   272,   273,   273,   274,   275,   275,   275,
     276,   276,   277,   278,   278,   279,   279,   280,   280,   281,
     282,   283,   284,   285,   285,   285,   286,   287,   287,   288,
     288,   288,   288,   288,   288,   288,   289,   289,   289,   290,
     290,   291,   291,   291,   291,   291,   291,   291,   291,   291,
     291,   291,   291,   291,   291,   291,   292,   292,   293,   293,
     293,   294,   294,   295,   295,   295,   295,   295,   295,   296,
     296,   297,   297,   298,   299,   299,   300,   300,   300,   300,
     300,   300,   300,   300,   300,   300,   300,   300,   300,   301,
     301,   302,   302,   303,   303,   304,   304,   304,   305,   305,
     306,   306,   307,   307,   308,   308,   309,   309,   310,   310,
     310,   311,   311,   312,   312,   313,   313,   314,   315,   315,
     316,   316,   317,   317,   318,   318,   318,   318,   318,   318,
     318,   319,   319,   319,   319,   320,   320,   321,   321,   321,
     321,   321,   321,   321,   321,   321,   321,   322,   322,   322,
     322,   322,   322,   322,   322,   322,   322,   322,   322,   322,
     322,   322,   322,   322,   322,   322,   322,   322,   322,   322,
     323,   323,   323,   323,   324,   324,   324,   324,   324,   324,
     325,   325,   325,   325,   326,   326,   327,   327,   328,   329,
     329,   329,   330,   330,   330,   330,   331,   332,   333,   333,
     333,   333,   333,   334,   335,   335,   335,   335,   335,   335,
     335,   335,   335,   335,   336,   336,   337,   337,   338,   339,
     340,   340,   340,   340,   341,   341,   341,   341,   342,   342,
     343,   343,   343,   343,   344,   344,   345,   346,   346,   346,
     346,   347,   347,   348,   349,   349,   349,   350,   350,   350,
     351,   351,   351,   352,   353,   354,   354,   355,   355,   356,
     356,   356,   357,   357,   358,   358,   358,   358,   359,   359,
     360,   360,   361,   361,   362,   363,   364,   365,   365,   365,
     365,   365,   366,   367,   367,   367,   367,   367,   367,   367,
     367,   368,   368,   368,   368,   368,   368,   368,   368,   368,
     368,   368,   368,   368,   368,   368,   368,   368,   368,   368,
     368,   368,   368,   368,   368,   368,   368,   368,   368,   368,
     368,   368,   368,   368,   368,   368,   368,   368,   368,   368,
     368,   368,   368,   368,   368,   368,   368,   369,   369
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
       0,     1,     1,     1,     2,     5,     5,     2,     3,     3,
       0,     2,     4,     4,     0,     2,     0,     2,     2,     1,
       1,     3,     2,     3,     3,     4,     1,     2,     0,     2,
       3,     2,     2,     2,     0,     2,     3,     2,     3,     2,
       3,     3,     4,     2,     1,     2,     2,     3,     2,     2,
       0,     1,     1,     2,     2,     3,     2,     2,     1,     2,
       1,     2,     2,     3,     2,     2,     2,     0,     1,     3,
       5,     2,     3,     2,     0,     1,     2,     2,     2,     2,
       4,     5,     5,     3,     1,     2,     3,     3,     2,     2,
       3,     3,     0,     2,     1,     3,     3,     3,     0,     1,
       1,     3,     2,     3,     2,     2,     1,     0,     2,     4,
       2,     4,     2,     1,     5,     3,     3,     2,     4,     3,
       5,     1,     1,     1,     1,     1,     1,     1,     1,     1,
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
       0,     3,   270,   274,   278,   272,   281,   283,   432,     0,
       0,    67,    72,    62,    63,    64,    96,    97,    98,    99,
     101,     0,     0,     0,   288,   288,   288,   288,   288,   288,
     288,   226,   100,   102,   103,   434,     0,     0,   153,     0,
     446,   106,   109,   107,   108,     0,     0,     0,     0,   155,
     156,   157,   158,   159,   160,     0,     0,     0,     0,     0,
     366,   374,   368,   354,   384,   354,   354,   390,   356,   398,
     400,   350,   407,   354,   414,     0,   149,     0,   148,   129,
     143,   143,   127,   128,     0,   117,     0,     0,     0,     0,
     424,     0,     0,     0,     9,     0,     2,     4,     0,    12,
      14,    39,    21,    20,    35,    58,    65,    66,     0,     0,
      36,    59,     0,   104,     0,   119,   120,    24,   123,   134,
     130,   131,   126,   124,   121,   122,    28,    29,    30,    31,
      32,    33,    34,    15,    17,    18,    19,    37,    16,     0,
     209,    41,    42,   112,     0,   305,     0,     0,     0,     0,
      23,    25,    38,   317,   318,   319,   321,   320,   322,   323,
     324,   325,   326,   327,   328,   329,   330,   331,   332,   333,
     334,   335,   336,   337,   338,   339,    22,    26,    27,    40,
     114,   193,   194,   195,     0,     0,   279,   276,     0,   309,
       0,   343,     0,     0,     0,   341,   342,     0,     0,     0,
       0,   485,   486,   487,   490,   463,   461,   464,   465,   462,
     467,   466,   470,   469,   468,   507,   489,   488,   498,   497,
     496,   499,   500,   494,   495,   501,   502,   503,   504,   505,
     506,   491,   492,   493,   471,   472,   473,   474,   475,   476,
     477,   478,   479,   480,   481,   482,   484,   483,     0,   196,
     198,   508,     0,   494,   495,     0,   162,    68,     0,    71,
      69,    60,     0,    73,    61,     0,     0,    90,     0,     0,
     297,     0,     0,   300,   271,     0,   288,   299,   273,   275,
     278,   282,   284,   286,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   434,     0,
     438,     0,     0,     0,   285,   344,   444,   445,   347,     0,
      74,    75,   359,   360,   358,   354,   354,   354,   354,   373,
       0,     0,   367,   354,   354,     0,   362,   385,   354,   386,
     388,   391,   392,   389,   397,     0,   396,   356,   394,   399,
     401,   402,   405,     0,   404,   408,   406,   354,   411,   415,
     413,   417,   418,   419,     0,     0,     0,   125,     0,   141,
     141,   139,     0,   132,   133,     0,     0,   425,   428,   429,
       0,     0,    10,     1,     5,     6,     7,   270,     0,    76,
      88,    87,    92,    82,    77,    78,    80,    79,    83,    81,
       0,    93,    51,    52,    55,    53,    54,    56,   105,   135,
      57,    13,   210,     0,   110,   113,   174,   176,   182,   190,
     181,   180,   447,   453,   306,     0,   447,   189,   192,     0,
       0,    49,    48,    50,     0,   116,   268,   265,     0,   266,
     280,   143,     0,   433,   312,   311,   313,   316,     0,   314,
     315,     0,   307,   308,     0,     0,     0,     0,     0,     0,
     165,     0,    70,    94,   348,    89,    91,    95,     0,     0,
     301,   287,   289,   292,   277,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     255,     0,   244,     0,     0,   259,   259,     0,     0,     0,
       0,   211,     0,     0,   230,   227,    11,     0,     0,   435,
     439,   440,   436,   437,   154,   349,   350,   354,   375,   354,
     354,   379,   354,   377,   383,   369,   371,     0,   372,   363,
     354,   364,   355,   387,   393,   357,   395,   403,   351,     0,
     412,   416,   150,     0,   136,   144,     0,   145,   146,     0,
     118,     0,   423,   426,   427,   430,   431,     8,    86,    84,
       0,   177,     0,     0,     0,    43,   175,     0,     0,   452,
       0,     0,     0,   457,     0,   183,     0,    44,     0,   269,
     267,   141,     0,     0,     0,     0,     0,   494,   495,     0,
       0,   197,   208,   207,   205,   206,   173,   168,   170,   169,
       0,     0,   166,   163,     0,     0,   302,   303,   304,     0,
     290,   292,     0,   291,   261,   263,   264,   244,   244,   244,
     244,     0,   261,     0,     0,     0,     0,     0,     0,   259,
     259,     0,     0,     0,   244,   244,   244,   260,   244,   244,
       0,     0,   244,   228,   229,   442,     0,     0,   361,   376,
     380,   354,   381,   378,   370,   365,     0,     0,   409,     0,
       0,     0,     0,   142,   140,   420,     0,   178,   179,   111,
       0,   450,     0,   448,   459,   456,   455,   191,     0,     0,
      44,    45,     0,   115,   147,   310,   345,   346,   201,   204,
       0,     0,     0,   199,   171,     0,     0,   161,     0,     0,
     294,   295,   293,   298,   244,   262,     0,     0,     0,     0,
       0,   258,   246,   247,   249,   250,   253,   254,   251,   252,
     256,   248,   212,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   443,   441,   382,   353,   352,     0,     0,   151,
     137,   138,   421,   422,    85,     0,     0,     0,     0,   458,
       0,   185,   188,     0,     0,     0,     0,   202,   172,   167,
     164,   296,     0,   215,   213,   216,   217,   261,   245,   221,
     222,   219,   220,   225,     0,     0,     0,     0,     0,     0,
     232,     0,     0,   214,   410,     0,   451,   449,   460,   454,
       0,   184,     0,    46,   200,   203,   218,   257,     0,     0,
       0,     0,     0,     0,     0,   259,     0,   152,   186,    47,
       0,     0,     0,   235,   237,     0,     0,   242,   223,   231,
       0,   224,   233,   234,     0,     0,   238,     0,   239,   259,
       0,   243,   241,     0,   236,   240
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    95,    96,    97,    98,    99,   408,   672,   421,   422,
     423,   101,   102,   103,   104,   105,   261,   264,   106,   107,
     380,   391,   381,   382,   108,   109,   110,   111,   112,   113,
     114,   555,   404,   115,   116,   117,   118,   357,   119,   537,
     359,   120,   121,   122,   123,   124,   125,   126,   127,   128,
     129,   130,   131,   132,   255,   591,   592,   405,   406,   407,
     409,   410,   669,   740,   133,   134,   135,   136,   137,   185,
     138,   249,   250,   584,   139,   140,   297,   495,   769,   770,
     772,   806,   807,   623,   482,   626,   694,   627,   141,   142,
     143,   144,   145,   146,   187,   147,   148,   149,   274,   275,
     601,   602,   276,   599,   415,   150,   151,   199,   152,   344,
     326,   338,   153,   314,   154,   155,   156,   322,   157,   319,
     158,   159,   160,   161,   333,   162,   163,   164,   165,   166,
     346,   167,   168,   350,   169,   170,   171,   172,   173,   174,
     175,   200,   176,   499,   500,   501,   177,   178,   179,   559,
     411,   412,   251,   413
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -561
static const yytype_int16 yypact[] =
{
     681,  -561,    -3,  -561,   -76,  -561,  -561,  -561,   186,  1995,
    2695,   110,    61,  -561,  -561,  -561,  -561,  -561,  -561,   -86,
    -561,  2695,  2695,   121,  2281,  2281,  2281,  2281,  2281,  2281,
    2281,   202,  -561,  -561,  -561,  1083,    98,   241,  -561,   -40,
    -561,  -561,  -561,  -561,  -561,   100,  2695,  2695,   117,  -561,
    -561,  -561,  -561,  -561,  -561,    96,    48,    66,   104,   152,
    -561,    93,   -58,    43,   127,   -71,   116,    -4,    25,   214,
     -57,    17,   167,   -45,   169,   107,    41,   258,  -561,  -561,
     304,   304,  -561,  -561,   253,   270,   258,   258,   340,   350,
    -561,   216,   276,   -53,   314,   361,   882,  -561,    20,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,    26,   156,
    -561,  -561,   336,  2833,  1279,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  1279,
     323,  -561,  -561,   154,   448,   299,  2695,   448,  2695,   225,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
     -16,   279,  -561,  -561,    60,   163,   330,  -561,   326,   297,
     165,  -561,    33,   287,    18,  -561,  -561,   331,   272,   228,
      70,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  2695,  2695,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  2138,  -561,
     178,  -561,   177,  -561,  -561,   179,   180,  -561,   329,  -561,
    -561,  -561,  2695,  -561,  -561,   275,   365,   362,  2695,   183,
    -561,   184,   185,  -561,  -561,   357,  2281,   187,  -561,  -561,
     -76,  -561,  -561,  -561,   316,   190,   191,   192,   193,   182,
     194,   196,   197,   198,   199,   188,   201,    56,  -561,   206,
      63,   332,   333,   269,  -561,  -561,  -561,  -561,  -561,   305,
    -561,  -561,  -561,   274,  -561,   -44,     2,    39,   116,  -561,
      -5,   334,  -561,   116,   172,   337,  -561,  -561,   116,  -561,
    -561,  -561,   338,  -561,  -561,   339,  -561,   262,  -561,  -561,
    -561,   280,  -561,   281,  -561,   220,  -561,   116,  -561,   284,
    -561,  -561,  -561,  -561,   347,   246,  2695,  -561,   349,   271,
     271,   226,  2695,  -561,  -561,   351,   353,   292,   294,  -561,
     300,   301,  -561,  -561,  -561,  -561,   396,   -27,   364,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
     366,   236,  -561,  -561,  -561,  -561,  -561,  -561,    21,  -561,
    -561,  -561,  -561,    19,   421,   154,  -561,  -561,  -561,  -561,
    -561,  -561,   101,  1707,  -561,   424,   101,  -561,  -561,   360,
     370,  -561,  -561,  -561,   429,   -15,  -561,  -561,   -24,  -561,
    -561,   304,   247,  -561,  -561,  -561,  -561,  -561,   380,  -561,
    -561,   298,  -561,  -561,   249,   251,  2971,   252,  1995,  2419,
      10,  2695,  -561,  -561,  -561,  -561,  -561,  -561,  2695,   318,
     148,  -561,  -561,  2557,  -561,   254,   146,   146,   146,   146,
     129,   256,   259,   261,   263,   264,   265,   266,   267,   273,
    -561,   277,  -561,   146,   146,   146,   146,   146,   278,   282,
     146,  -561,   389,   397,  -561,  -561,  -561,   426,   419,  -561,
     283,  -561,  -561,  -561,  -561,  -561,   341,   116,  -561,   116,
      40,  -561,   116,  -561,  -561,  -561,  -561,   399,  -561,  -561,
     116,  -561,  -561,  -561,  -561,  -561,  -561,  -561,   -75,   344,
    -561,  -561,   414,   285,   -12,  -561,   403,  -561,  -561,   410,
    -561,   286,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
     290,  -561,   293,   140,   448,  -561,  -561,  1431,  1569,  -561,
    2695,  2695,  2695,  -561,   448,   -20,   411,   496,   448,  -561,
    -561,   271,   427,   390,   306,  2419,  2419,  2695,  2695,   302,
    2419,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
     -18,    44,  -561,   309,  2695,   307,  -561,  -561,  -561,   308,
    -561,  2557,   310,  -561,   146,  -561,  -561,  -561,  -561,  -561,
    -561,   438,   146,   146,   146,   146,   146,   146,   146,   146,
     146,   146,   146,    -1,  -561,  -561,  -561,  -561,  -561,  -561,
     311,   312,  -561,  -561,  -561,  -561,   462,    63,  -561,  -561,
    -561,   116,  -561,  -561,  -561,  -561,   381,   382,   320,   345,
     446,  2695,  2695,  -561,  -561,    -2,   449,  -561,  -561,  -561,
    2695,  -561,  2695,  -561,  -561,  -561,  1852,  -561,  2695,    81,
     496,  -561,   513,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
     324,   325,  2419,  -561,  -561,   453,    10,  -561,  2695,   328,
    -561,  -561,  -561,  -561,  -561,  -561,     1,     4,     5,    14,
     327,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,   182,    15,    16,    27,    28,    29,    34,
     474,    30,  -561,  -561,  -561,  -561,  -561,   464,   348,  -561,
    -561,  -561,  -561,  -561,  -561,   354,   359,  2695,  2695,  -561,
      49,  -561,  -561,   528,  2695,  2419,  2419,  -561,  -561,  -561,
    -561,  -561,    31,  -561,  -561,  -561,  -561,   146,  -561,  -561,
    -561,  -561,  -561,  -561,   346,   363,   367,   368,   369,   371,
     343,   372,   373,  -561,  -561,   476,  -561,  -561,  -561,  -561,
    2695,  -561,  2695,  -561,  -561,  -561,  -561,  -561,   146,   146,
     -51,   376,   490,   533,    34,   146,   534,  -561,  -561,  -561,
     377,   385,   374,  -561,  -561,   384,   388,   398,  -561,  -561,
     -52,  -561,  -561,  -561,   146,   146,  -561,   490,  -561,   146,
     392,  -561,  -561,   395,  -561,  -561
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -561,  -561,  -561,   480,   545,   -13,     6,   -89,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,   430,  -561,  -561,  -561,  -561,   268,  -561,  -351,
     -74,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,   501,  -561,  -561,   -73,  -172,  -329,  -561,
    -142,   -55,  -561,  -561,    79,   105,   149,   150,   157,  -561,
    -561,   164,  -561,  -527,   471,  -561,  -561,  -561,  -179,  -561,
    -561,  -201,  -561,  -485,   -96,  -483,  -560,  -440,  -561,  -561,
    -561,  -561,  -561,  -561,   342,  -561,  -561,  -561,   215,   355,
    -561,    22,  -561,  -561,  -561,  -561,  -561,  -561,  -561,   112,
     -43,   288,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,   -11,  -561,  -561,   511,   205,
    -561,  -144,  -561,    -9
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -508
static const yytype_int16 yytable[] =
{
     252,   256,   416,   628,   418,   417,   100,   360,   425,   538,
    -112,  -113,   266,   267,   651,   277,   277,   277,   277,   277,
     277,   277,   329,   330,   377,  -187,   607,   608,   609,   610,
     348,   377,     3,     4,     5,     6,     7,   305,   306,    10,
     712,   100,   753,   624,   625,   754,   755,   629,   678,   679,
     632,    24,   701,   683,   586,   756,   759,   760,    24,    25,
      26,    27,    28,    29,   684,   375,  -507,   354,   761,   762,
     763,   773,   786,   569,   646,   180,   556,   515,   331,   186,
     732,   181,   182,   183,   802,   434,   377,   340,   334,   262,
     325,   369,   587,   265,   370,   320,   556,   491,   492,   493,
     507,   400,   100,   321,   399,   181,   182,   183,    40,   263,
     328,   355,   437,    24,   379,   341,   325,   325,   392,   497,
     100,   498,   696,   697,   698,   699,   401,   588,   435,   257,
     258,   347,   647,   438,   442,   443,   708,   709,   494,   714,
     715,   716,   733,   717,   718,   100,   509,   721,   371,   268,
     516,   332,   764,   300,   589,   747,   517,   510,   259,   818,
     803,   342,   819,   325,   695,   343,   765,   766,   767,   303,
     768,   260,   695,   702,   703,   704,   705,   706,   707,   184,
     570,   710,   711,   512,   641,  -187,   335,   384,   685,   304,
     668,   393,   336,   337,   403,   403,   652,   787,   323,   307,
     325,   325,   324,   184,   325,   188,   713,   308,   713,   752,
     310,   713,   713,   385,   444,   445,   590,   394,   784,   785,
     674,   713,   713,   713,   556,   376,  -507,   309,   605,   403,
     551,   553,   378,   606,   713,   713,   713,   713,   713,   447,
     278,   279,   280,   281,   282,   283,   315,   316,   311,   317,
     318,   686,   189,   453,   190,   687,   780,   386,   387,   457,
     781,   395,   396,   427,   470,   388,   428,   277,   557,   397,
     558,   327,   508,   511,   513,   514,   191,   325,   192,   193,
     519,   521,   194,   284,   356,   523,   351,   352,   353,   596,
     597,   598,   195,   196,   301,   302,   339,   197,   312,   313,
     471,   472,   473,   474,   530,   475,   476,   477,   478,   479,
     480,   345,   810,   349,   481,   419,   420,   695,   488,   489,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     358,   295,   520,   325,   296,   361,   823,   611,   612,   362,
    -340,   377,     3,     4,     5,     6,     7,   534,   800,   801,
     403,   658,   365,   540,   363,   364,   366,   571,   368,   372,
     367,   373,   390,    31,   403,   198,   414,   429,    24,    25,
      26,    27,    28,    29,   820,   821,   426,   430,   431,   432,
     433,   436,   441,   439,   440,   448,   449,   454,   451,   450,
     452,   455,   456,   458,   459,   460,   461,   463,   465,   466,
     467,   468,   469,   483,   563,   484,   485,   486,   487,   552,
     490,   496,   659,   504,   502,   503,   518,   505,   506,   522,
     524,   525,   667,   335,   527,   528,   673,   529,   531,   532,
     533,   535,   536,   541,   539,   542,   543,   579,   544,   252,
     585,   547,   593,   550,   545,   546,   548,   554,   549,   594,
     564,   566,   567,   377,   603,   568,   572,   573,   575,   574,
     576,   580,   595,   604,   639,   613,   640,   642,   614,   643,
     615,   633,   616,   617,   618,   619,   620,   645,   636,   634,
      24,   644,   621,   635,   649,   653,   622,   630,   648,   343,
     637,   631,   654,   670,   650,   655,   656,   201,   202,   203,
     204,   205,   206,   207,   657,   671,   676,   208,   677,   675,
     209,   682,   210,   211,   212,   213,   214,   688,   690,   691,
     700,   693,   722,   719,   720,   725,   726,   727,   729,   728,
     215,   734,   744,   745,   746,   748,   757,   216,   217,   751,
     771,   218,   219,   220,   221,   222,   774,   782,   661,   663,
     794,   664,   665,   666,   253,   254,   788,   775,   797,   225,
     226,   227,   228,   229,   230,   776,   585,   585,   680,   681,
     777,   585,   805,   789,   808,   811,   374,   790,   791,   792,
     299,   743,   795,   814,   793,   689,   796,   804,   812,   231,
     232,   233,   603,   815,   234,   235,   813,   236,   724,   816,
     783,   237,   238,   824,   239,   817,   825,   240,   241,   383,
     424,   402,   581,   749,   742,   809,   822,   758,   638,   389,
     242,   565,   464,   692,   243,   526,   723,     0,   244,   245,
       0,   462,   246,     0,     0,     0,     0,     0,   799,     0,
       0,     0,   730,   731,     0,     0,     0,     0,     0,   247,
       0,   735,     0,   736,     0,     0,     0,   739,     0,   741,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   585,     0,     0,     0,     0,     0,   750,
       0,     0,     1,     0,     0,     0,     2,     3,     4,     5,
       6,     7,     8,     9,    10,    11,    12,    13,    14,    15,
       0,     0,    16,    17,    18,    19,    20,     0,     0,    21,
      22,     0,    23,    24,    25,    26,    27,    28,    29,    30,
       0,    31,     0,     0,     0,     0,     0,     0,   778,   779,
      32,    33,    34,     0,    35,    36,   585,   585,     0,     0,
       0,     0,     0,    37,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      38,    39,     0,    40,    41,    42,    43,    44,     0,    45,
       0,   798,     0,     0,     0,     0,     0,    46,     0,    47,
      48,    49,    50,    51,    52,    53,    54,     0,     0,     0,
       0,     0,     0,     0,    55,    56,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    57,    58,
       0,     0,     0,     0,     0,     0,    59,     0,     0,     0,
       0,    60,    61,    62,     0,     0,     0,     0,     0,    63,
       0,     0,     0,    64,    65,    66,    67,    68,     0,     0,
       0,    69,    70,     0,    71,    72,    73,     0,    74,    75,
       0,     0,     0,    76,    77,     0,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,     0,     0,     0,     0,    94,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,     0,     0,    16,    17,    18,    19,    20,     0,     0,
      21,    22,     0,    23,    24,    25,    26,    27,    28,    29,
      30,     0,    31,     0,     0,     0,     0,     0,     0,     0,
       0,    32,    33,    34,     0,    35,    36,     0,     0,     0,
       0,     0,     0,     0,    37,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    38,    39,     0,    40,    41,    42,    43,    44,     0,
      45,     0,     0,     0,     0,     0,     0,     0,    46,     0,
      47,    48,    49,    50,    51,    52,    53,    54,     0,     0,
       0,     0,     0,     0,     0,    55,    56,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    57,
      58,     0,     0,     0,     0,     0,     0,    59,     0,     0,
       0,     0,    60,    61,    62,     0,     0,     0,     0,     0,
      63,     0,     0,     0,    64,    65,    66,    67,    68,     0,
       0,     0,    69,    70,     0,    71,    72,    73,     0,    74,
      75,     0,     0,     0,    76,    77,     0,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,     0,     0,     0,     0,    94,     2,     3,
       4,     5,     6,     7,     8,     9,    10,    11,    12,    13,
      14,    15,     0,     0,    16,    17,    18,    19,    20,     0,
       0,    21,    22,     0,    23,    24,    25,    26,    27,    28,
      29,    30,     0,    31,     0,     0,     0,     0,     0,     0,
       0,     0,    32,    33,    34,     0,   298,    36,     0,     0,
       0,     0,     0,     0,     0,    37,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    38,    39,     0,    40,    41,    42,    43,    44,
       0,    45,     0,     0,     0,     0,     0,     0,     0,    46,
       0,    47,    48,    49,    50,    51,    52,    53,    54,     0,
       0,     0,     0,     0,     0,     0,    55,    56,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      57,    58,     0,     0,     0,     0,     0,     0,    59,     0,
       0,     0,     0,    60,    61,    62,     0,     0,     0,     0,
       0,    63,     0,     0,     0,    64,    65,    66,    67,    68,
       0,     0,     0,    69,    70,     0,    71,    72,    73,     0,
      74,    75,     0,     0,     0,    76,    77,     0,    78,    79,
      80,    81,    82,    83,    84,    85,    86,    87,    88,    89,
      90,    91,    92,    93,     2,     3,     4,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,     0,     0,
      16,    17,    18,    19,    20,     0,     0,    21,    22,     0,
      23,    24,    25,    26,    27,    28,    29,    30,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    32,    33,
      34,     0,   298,    36,     0,     0,     0,     0,     0,     0,
       0,    37,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    38,    39,
       0,    40,    41,    42,    43,    44,     0,    45,     0,     0,
       0,     0,     0,     0,     0,    46,     0,    47,    48,    49,
      50,    51,    52,    53,    54,     0,     0,     0,     0,     0,
       0,     0,    55,    56,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    57,    58,     0,     0,
       0,     0,     0,     0,    59,     0,     0,     0,     0,    60,
      61,    62,     0,     0,     0,     0,     0,    63,     0,     0,
       0,    64,    65,    66,    67,    68,     0,     0,     0,    69,
      70,     0,    71,    72,    73,     0,    74,    75,     0,     0,
       0,    76,    77,     0,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
     201,   202,   203,   204,   205,   206,   207,     0,     0,     0,
     208,     0,     0,   209,     0,   210,   211,   212,   213,   214,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   215,     0,     0,     0,     0,     0,     0,
     216,   217,     0,     0,   218,   219,   220,   221,   222,     0,
       0,     0,     0,     0,     0,     0,     0,   253,   254,     0,
       0,     0,   225,   226,   227,   228,   229,   230,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   231,   232,   233,     0,     0,   234,   235,     0,
     236,     0,     0,     0,   237,   238,     0,   239,     0,     0,
     240,   241,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   242,     0,     0,     0,   243,     0,     0,
       0,   244,   245,     0,     0,   246,     0,     0,   201,   202,
     203,   204,   205,   206,   207,     0,     0,     0,   208,     0,
       0,   209,   247,   210,   211,   212,   213,   214,     0,     0,
       0,   660,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   215,     0,     0,     0,     0,     0,     0,   216,   217,
       0,     0,   218,   219,   220,   221,   222,     0,     0,     0,
       0,     0,     0,     0,     0,   253,   254,     0,     0,     0,
     225,   226,   227,   228,   229,   230,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     231,   232,   233,     0,     0,   234,   235,     0,   236,     0,
       0,     0,   237,   238,     0,   239,     0,     0,   240,   241,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   242,     0,     0,     0,   243,     0,     0,     0,   244,
     245,     0,     0,   246,     0,     0,   201,   202,   203,   204,
     205,   206,   207,     0,     0,     0,   208,     0,     0,   209,
     247,   210,   211,   212,   213,   214,   560,     0,     0,   662,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   215,
       0,     0,     0,     0,     0,     0,   216,   217,     0,     0,
     218,   219,   220,   221,   222,     0,     0,     0,     0,     0,
       0,     0,     0,   253,   254,     0,     0,     0,   225,   226,
     227,   228,   229,   230,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   231,   232,
     233,     0,     0,   234,   235,     0,   236,     0,     0,     0,
     237,   238,     0,   239,     0,     0,   240,   241,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   242,
       0,     0,     0,   243,     0,     0,     0,   244,   245,     0,
       0,   246,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   201,   202,   203,   204,   205,   206,   207,   247,     0,
       0,   208,     0,   561,   209,   562,   210,   211,   212,   213,
     214,   737,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   215,     0,     0,     0,     0,     0,
       0,   216,   217,     0,     0,   218,   219,   220,   221,   222,
       0,     0,     0,     0,     0,     0,     0,     0,   253,   254,
       0,     0,     0,   225,   226,   227,   228,   229,   230,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   231,   232,   233,     0,     0,   234,   235,
       0,   236,     0,     0,     0,   237,   238,     0,   239,     0,
       0,   240,   241,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   242,     0,     0,     0,   243,     0,
       0,     0,   244,   245,     0,     0,   246,     0,     0,     0,
       0,     0,     0,     0,   201,   202,   203,   204,   205,   206,
     207,     0,     0,   247,   208,     0,     0,   209,   738,   210,
     211,   212,   213,   214,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   215,     0,     0,
       0,     0,     0,     0,   216,   217,     0,     0,   218,   219,
     220,   221,   222,     0,     0,     0,     0,     0,     0,     0,
       0,   223,   224,     0,     0,     0,   225,   226,   227,   228,
     229,   230,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   231,   232,   233,     0,
       0,   234,   235,     0,   236,     0,     0,     0,   237,   238,
       0,   239,     0,     0,   240,   241,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   242,     0,     0,
       0,   243,     0,     0,     0,   244,   245,     0,     0,   246,
       0,     0,     0,     0,     0,     0,     0,   201,   202,   203,
     204,   205,   206,   207,     0,     0,   247,   208,     0,     0,
     209,   248,   210,   211,   212,   213,   214,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     215,     0,     0,     0,     0,     0,     0,   216,   217,     0,
       0,   218,   219,   220,   221,   222,     0,     0,     0,     0,
       0,     0,     0,     0,   253,   254,     0,     0,     0,   225,
     226,   227,   228,   229,   230,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   231,
     232,   233,     0,     0,   234,   235,     0,   236,     0,     0,
       0,   237,   238,     0,   239,     0,     0,   240,   241,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     242,     0,     0,     0,   243,     0,     0,     0,   244,   245,
       0,     0,   246,     0,     0,     0,     0,     0,     0,     0,
     201,   202,   203,   204,   205,   206,   207,     0,     0,   247,
     208,     0,   269,   209,   446,   210,   211,   212,   213,   214,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   215,     0,     0,     0,     0,     0,     0,
     216,   217,     0,     0,   218,   219,   220,   221,   222,     0,
       0,     0,     0,     0,     0,     0,     0,   253,   254,     0,
       0,     0,   225,   226,   227,   228,   229,   230,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   270,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     271,   272,   231,   232,   233,   273,     0,   234,   235,     0,
     236,     0,     0,     0,   237,   238,     0,   239,     0,     0,
     240,   241,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   242,     0,     0,     0,   243,     0,     0,
       0,   244,   245,     0,     0,   246,     0,     0,   201,   202,
     203,   204,   205,   206,   207,     0,     0,     0,   208,     0,
       0,   209,   247,   210,   211,   212,   213,   214,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   215,     0,     0,     0,     0,     0,     0,   216,   217,
       0,     0,   218,   219,   220,   221,   222,     0,     0,     0,
       0,     0,     0,     0,     0,   253,   254,     0,     0,     0,
     225,   226,   227,   228,   229,   230,   582,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     231,   232,   233,   583,     0,   234,   235,     0,   236,     0,
       0,     0,   237,   238,     0,   239,     0,     0,   240,   241,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   242,     0,     0,     0,   243,     0,     0,     0,   244,
     245,     0,     0,   246,     0,     0,   201,   202,   203,   204,
     205,   206,   207,     0,     0,     0,   208,     0,     0,   209,
     247,   210,   211,   212,   213,   214,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   215,
       0,     0,     0,     0,     0,     0,   216,   217,     0,     0,
     218,   219,   220,   221,   222,     0,     0,     0,     0,     0,
       0,     0,     0,   253,   254,     0,     0,     0,   225,   226,
     227,   228,   229,   230,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   231,   232,
     233,   600,     0,   234,   235,     0,   236,     0,     0,     0,
     237,   238,     0,   239,     0,     0,   240,   241,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   242,
       0,     0,     0,   243,     0,     0,     0,   244,   245,     0,
       0,   246,     0,     0,   201,   202,   203,   204,   205,   206,
     207,     0,     0,     0,   208,     0,     0,   209,   247,   210,
     211,   212,   213,   214,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   215,     0,     0,
       0,     0,     0,     0,   216,   217,     0,     0,   218,   219,
     220,   221,   222,     0,     0,     0,     0,     0,     0,     0,
       0,   253,   254,     0,     0,     0,   225,   226,   227,   228,
     229,   230,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   231,   232,   233,     0,
       0,   234,   235,     0,   236,     0,     0,     0,   237,   238,
       0,   239,     0,     0,   240,   241,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   242,     0,     0,
       0,   243,     0,     0,     0,   244,   245,     0,     0,   246,
       0,     0,   201,   202,   203,   204,   205,   206,   207,     0,
       0,     0,   208,     0,     0,   209,   247,   210,   211,   212,
     213,   214,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   398,     0,     0,     0,     0,
       0,     0,   216,   217,     0,     0,   218,   219,   220,   221,
     222,     0,     0,     0,     0,     0,     0,     0,     0,   253,
     254,     0,     0,     0,   225,   226,   227,   228,   229,   230,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   231,   232,   233,     0,     0,   234,
     235,     0,   236,     0,     0,     0,   237,   238,     0,   239,
       0,     0,   240,   241,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   242,     0,     0,     0,   243,
       0,     0,     0,   244,   245,     0,     0,   246,     0,     0,
     201,   202,   203,   204,   205,   206,   207,     0,     0,     0,
     208,     0,     0,   209,   247,   210,   211,   212,   213,   214,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   215,     0,     0,     0,     0,     0,     0,
     216,   217,     0,     0,   218,   219,   220,   221,   222,     0,
       0,     0,     0,     0,     0,     0,     0,   577,   578,     0,
       0,     0,   225,   226,   227,   228,   229,   230,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   231,   232,   233,     0,     0,   234,   235,     0,
     236,     0,     0,     0,   237,   238,     0,   239,     0,     0,
     240,   241,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   242,     0,     0,     0,   243,     0,     0,
       0,   244,   245,     0,     0,   246,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   247
};

static const yytype_int16 yycheck[] =
{
       9,    10,   146,   486,   148,   147,     0,    81,   180,   360,
      26,    26,    21,    22,    26,    24,    25,    26,    27,    28,
      29,    30,    65,    66,     5,    45,   466,   467,   468,   469,
      73,     5,     6,     7,     8,     9,    10,    46,    47,    13,
      41,    35,    41,   483,   484,    41,    41,   487,   575,   576,
     490,    32,   612,   580,    44,    41,    41,    41,    32,    33,
      34,    35,    36,    37,    82,    45,    45,    26,    41,    41,
      41,    41,    41,    97,   149,    78,   405,    82,    82,   155,
      82,   108,   109,   110,   135,    52,     5,   144,    63,    28,
     161,   144,    82,   179,   147,   153,   425,    41,    42,    43,
     144,   114,    96,   161,   113,   108,   109,   110,    82,    48,
     181,    70,    94,    32,   108,   172,   161,   161,   112,    56,
     114,    58,   607,   608,   609,   610,   139,   117,    95,    19,
      20,   176,   207,   115,    64,    65,   619,   620,    82,   624,
     625,   626,   144,   628,   629,   139,   144,   632,   201,    28,
     155,   155,   118,    55,   144,   682,   161,   155,    48,   211,
     211,   144,   214,   161,   604,   148,   132,   133,   134,   209,
     136,    61,   612,   613,   614,   615,   616,   617,   618,   206,
     204,   621,   622,   144,   144,   205,   161,   108,   206,    89,
     210,   112,   167,   168,   210,   210,   208,   757,   155,    82,
     161,   161,   159,   206,   161,    19,   207,   111,   207,   694,
     144,   207,   207,   108,   223,   224,   206,   112,   745,   746,
     571,   207,   207,   207,   553,   205,   205,   179,    82,   210,
     211,   403,   206,    87,   207,   207,   207,   207,   207,   248,
      25,    26,    27,    28,    29,    30,   153,   154,   144,   156,
     157,   207,    66,   262,    68,   211,   207,   108,   108,   268,
     211,   112,   112,   203,    82,   108,   206,   276,   167,   112,
     169,   144,   315,   316,   317,   318,    90,   161,    92,    93,
     323,   324,    96,    81,    26,   328,   179,   180,   181,   141,
     142,   143,   106,   107,    53,    54,    82,   111,   146,   147,
     118,   119,   120,   121,   347,   123,   124,   125,   126,   127,
     128,   144,   795,   144,   132,    90,    91,   757,   130,   131,
     118,   119,   120,   121,   122,   123,   124,   125,   126,   127,
      26,   129,   160,   161,   132,    82,   819,   208,   209,    69,
     154,     5,     6,     7,     8,     9,    10,   356,   788,   789,
     210,   211,    12,   362,    86,    87,     6,   431,    82,    45,
     144,     0,   206,    40,   210,   179,    67,   204,    32,    33,
      34,    35,    36,    37,   814,   815,    97,    47,    52,    82,
     215,    94,   154,    52,   112,   207,   209,   112,   208,   210,
      61,    26,    30,   210,   210,   210,    39,   210,    82,   209,
     209,   209,   209,   209,   413,   209,   209,   209,   209,   403,
     209,   205,   554,   144,    82,    82,    82,   112,   144,    82,
      82,    82,   564,   161,   144,   144,   568,   207,   144,    82,
     184,    82,   161,    82,   208,    82,   144,   446,   144,   448,
     449,    45,   451,   207,   144,   144,    82,    26,    82,   458,
      26,    91,    82,     5,   463,    26,   209,    77,   209,   161,
     209,   209,   144,   209,   507,   209,   509,   510,   209,   512,
     209,    82,   209,   209,   209,   209,   209,   520,    59,    82,
      32,    82,   209,    57,    70,    82,   209,   209,   144,   148,
     207,   209,    82,    82,   209,   209,   206,    49,    50,    51,
      52,    53,    54,    55,   211,     9,   116,    59,   202,    82,
      62,   209,    64,    65,    66,    67,    68,   208,   211,   211,
      82,   211,    60,   212,   212,   144,   144,   207,    82,   184,
      82,    82,    19,   209,   209,    82,   209,    89,    90,   211,
      66,    93,    94,    95,    96,    97,    82,    19,   557,   558,
     207,   560,   561,   562,   106,   107,   210,   209,    82,   111,
     112,   113,   114,   115,   116,   211,   575,   576,   577,   578,
     211,   580,    82,   210,    41,    41,    96,   210,   210,   210,
      35,   670,   210,   209,   213,   594,   213,   211,   211,   141,
     142,   143,   601,   209,   146,   147,   211,   149,   641,   211,
     744,   153,   154,   211,   156,   207,   211,   159,   160,   108,
     180,   140,   448,   686,   669,   794,   817,   713,   506,   108,
     172,   416,   280,   601,   176,   337,   637,    -1,   180,   181,
      -1,   276,   184,    -1,    -1,    -1,    -1,    -1,   782,    -1,
      -1,    -1,   651,   652,    -1,    -1,    -1,    -1,    -1,   201,
      -1,   660,    -1,   662,    -1,    -1,    -1,   666,    -1,   668,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   682,    -1,    -1,    -1,    -1,    -1,   688,
      -1,    -1,     1,    -1,    -1,    -1,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,    18,
      -1,    -1,    21,    22,    23,    24,    25,    -1,    -1,    28,
      29,    -1,    31,    32,    33,    34,    35,    36,    37,    38,
      -1,    40,    -1,    -1,    -1,    -1,    -1,    -1,   737,   738,
      49,    50,    51,    -1,    53,    54,   745,   746,    -1,    -1,
      -1,    -1,    -1,    62,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      79,    80,    -1,    82,    83,    84,    85,    86,    -1,    88,
      -1,   780,    -1,    -1,    -1,    -1,    -1,    96,    -1,    98,
      99,   100,   101,   102,   103,   104,   105,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   113,   114,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   137,   138,
      -1,    -1,    -1,    -1,    -1,    -1,   145,    -1,    -1,    -1,
      -1,   150,   151,   152,    -1,    -1,    -1,    -1,    -1,   158,
      -1,    -1,    -1,   162,   163,   164,   165,   166,    -1,    -1,
      -1,   170,   171,    -1,   173,   174,   175,    -1,   177,   178,
      -1,    -1,    -1,   182,   183,    -1,   185,   186,   187,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   200,    -1,    -1,    -1,    -1,   205,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,    16,    17,
      18,    -1,    -1,    21,    22,    23,    24,    25,    -1,    -1,
      28,    29,    -1,    31,    32,    33,    34,    35,    36,    37,
      38,    -1,    40,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    49,    50,    51,    -1,    53,    54,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    62,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    79,    80,    -1,    82,    83,    84,    85,    86,    -1,
      88,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    96,    -1,
      98,    99,   100,   101,   102,   103,   104,   105,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   113,   114,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   137,
     138,    -1,    -1,    -1,    -1,    -1,    -1,   145,    -1,    -1,
      -1,    -1,   150,   151,   152,    -1,    -1,    -1,    -1,    -1,
     158,    -1,    -1,    -1,   162,   163,   164,   165,   166,    -1,
      -1,    -1,   170,   171,    -1,   173,   174,   175,    -1,   177,
     178,    -1,    -1,    -1,   182,   183,    -1,   185,   186,   187,
     188,   189,   190,   191,   192,   193,   194,   195,   196,   197,
     198,   199,   200,    -1,    -1,    -1,    -1,   205,     5,     6,
       7,     8,     9,    10,    11,    12,    13,    14,    15,    16,
      17,    18,    -1,    -1,    21,    22,    23,    24,    25,    -1,
      -1,    28,    29,    -1,    31,    32,    33,    34,    35,    36,
      37,    38,    -1,    40,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    49,    50,    51,    -1,    53,    54,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    62,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    79,    80,    -1,    82,    83,    84,    85,    86,
      -1,    88,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    96,
      -1,    98,    99,   100,   101,   102,   103,   104,   105,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   113,   114,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     137,   138,    -1,    -1,    -1,    -1,    -1,    -1,   145,    -1,
      -1,    -1,    -1,   150,   151,   152,    -1,    -1,    -1,    -1,
      -1,   158,    -1,    -1,    -1,   162,   163,   164,   165,   166,
      -1,    -1,    -1,   170,   171,    -1,   173,   174,   175,    -1,
     177,   178,    -1,    -1,    -1,   182,   183,    -1,   185,   186,
     187,   188,   189,   190,   191,   192,   193,   194,   195,   196,
     197,   198,   199,   200,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,    16,    17,    18,    -1,    -1,
      21,    22,    23,    24,    25,    -1,    -1,    28,    29,    -1,
      31,    32,    33,    34,    35,    36,    37,    38,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    49,    50,
      51,    -1,    53,    54,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    62,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    79,    80,
      -1,    82,    83,    84,    85,    86,    -1,    88,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    96,    -1,    98,    99,   100,
     101,   102,   103,   104,   105,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   113,   114,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   137,   138,    -1,    -1,
      -1,    -1,    -1,    -1,   145,    -1,    -1,    -1,    -1,   150,
     151,   152,    -1,    -1,    -1,    -1,    -1,   158,    -1,    -1,
      -1,   162,   163,   164,   165,   166,    -1,    -1,    -1,   170,
     171,    -1,   173,   174,   175,    -1,   177,   178,    -1,    -1,
      -1,   182,   183,    -1,   185,   186,   187,   188,   189,   190,
     191,   192,   193,   194,   195,   196,   197,   198,   199,   200,
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
      -1,    -1,    -1,   172,    -1,    -1,    -1,   176,    -1,    -1,
      -1,   180,   181,    -1,    -1,   184,    -1,    -1,    49,    50,
      51,    52,    53,    54,    55,    -1,    -1,    -1,    59,    -1,
      -1,    62,   201,    64,    65,    66,    67,    68,    -1,    -1,
      -1,   210,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,    89,    90,
      -1,    -1,    93,    94,    95,    96,    97,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   106,   107,    -1,    -1,    -1,
     111,   112,   113,   114,   115,   116,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     141,   142,   143,    -1,    -1,   146,   147,    -1,   149,    -1,
      -1,    -1,   153,   154,    -1,   156,    -1,    -1,   159,   160,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   172,    -1,    -1,    -1,   176,    -1,    -1,    -1,   180,
     181,    -1,    -1,   184,    -1,    -1,    49,    50,    51,    52,
      53,    54,    55,    -1,    -1,    -1,    59,    -1,    -1,    62,
     201,    64,    65,    66,    67,    68,    69,    -1,    -1,   210,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,
      -1,    -1,    -1,    -1,    -1,    -1,    89,    90,    -1,    -1,
      93,    94,    95,    96,    97,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   106,   107,    -1,    -1,    -1,   111,   112,
     113,   114,   115,   116,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   141,   142,
     143,    -1,    -1,   146,   147,    -1,   149,    -1,    -1,    -1,
     153,   154,    -1,   156,    -1,    -1,   159,   160,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   172,
      -1,    -1,    -1,   176,    -1,    -1,    -1,   180,   181,    -1,
      -1,   184,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    49,    50,    51,    52,    53,    54,    55,   201,    -1,
      -1,    59,    -1,   206,    62,   208,    64,    65,    66,    67,
      68,    69,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,    -1,
      -1,    89,    90,    -1,    -1,    93,    94,    95,    96,    97,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,   107,
      -1,    -1,    -1,   111,   112,   113,   114,   115,   116,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   141,   142,   143,    -1,    -1,   146,   147,
      -1,   149,    -1,    -1,    -1,   153,   154,    -1,   156,    -1,
      -1,   159,   160,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   172,    -1,    -1,    -1,   176,    -1,
      -1,    -1,   180,   181,    -1,    -1,   184,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    49,    50,    51,    52,    53,    54,
      55,    -1,    -1,   201,    59,    -1,    -1,    62,   206,    64,
      65,    66,    67,    68,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,
      -1,    -1,    -1,    -1,    89,    90,    -1,    -1,    93,    94,
      95,    96,    97,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   106,   107,    -1,    -1,    -1,   111,   112,   113,   114,
     115,   116,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   141,   142,   143,    -1,
      -1,   146,   147,    -1,   149,    -1,    -1,    -1,   153,   154,
      -1,   156,    -1,    -1,   159,   160,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   172,    -1,    -1,
      -1,   176,    -1,    -1,    -1,   180,   181,    -1,    -1,   184,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    49,    50,    51,
      52,    53,    54,    55,    -1,    -1,   201,    59,    -1,    -1,
      62,   206,    64,    65,    66,    67,    68,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      82,    -1,    -1,    -1,    -1,    -1,    -1,    89,    90,    -1,
      -1,    93,    94,    95,    96,    97,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   106,   107,    -1,    -1,    -1,   111,
     112,   113,   114,   115,   116,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   141,
     142,   143,    -1,    -1,   146,   147,    -1,   149,    -1,    -1,
      -1,   153,   154,    -1,   156,    -1,    -1,   159,   160,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     172,    -1,    -1,    -1,   176,    -1,    -1,    -1,   180,   181,
      -1,    -1,   184,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      49,    50,    51,    52,    53,    54,    55,    -1,    -1,   201,
      59,    -1,    61,    62,   206,    64,    65,    66,    67,    68,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,
      89,    90,    -1,    -1,    93,    94,    95,    96,    97,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,   107,    -1,
      -1,    -1,   111,   112,   113,   114,   115,   116,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   128,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     139,   140,   141,   142,   143,   144,    -1,   146,   147,    -1,
     149,    -1,    -1,    -1,   153,   154,    -1,   156,    -1,    -1,
     159,   160,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   172,    -1,    -1,    -1,   176,    -1,    -1,
      -1,   180,   181,    -1,    -1,   184,    -1,    -1,    49,    50,
      51,    52,    53,    54,    55,    -1,    -1,    -1,    59,    -1,
      -1,    62,   201,    64,    65,    66,    67,    68,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    82,    -1,    -1,    -1,    -1,    -1,    -1,    89,    90,
      -1,    -1,    93,    94,    95,    96,    97,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   106,   107,    -1,    -1,    -1,
     111,   112,   113,   114,   115,   116,   117,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     141,   142,   143,   144,    -1,   146,   147,    -1,   149,    -1,
      -1,    -1,   153,   154,    -1,   156,    -1,    -1,   159,   160,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   172,    -1,    -1,    -1,   176,    -1,    -1,    -1,   180,
     181,    -1,    -1,   184,    -1,    -1,    49,    50,    51,    52,
      53,    54,    55,    -1,    -1,    -1,    59,    -1,    -1,    62,
     201,    64,    65,    66,    67,    68,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,
      -1,    -1,    -1,    -1,    -1,    -1,    89,    90,    -1,    -1,
      93,    94,    95,    96,    97,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   106,   107,    -1,    -1,    -1,   111,   112,
     113,   114,   115,   116,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   141,   142,
     143,   144,    -1,   146,   147,    -1,   149,    -1,    -1,    -1,
     153,   154,    -1,   156,    -1,    -1,   159,   160,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   172,
      -1,    -1,    -1,   176,    -1,    -1,    -1,   180,   181,    -1,
      -1,   184,    -1,    -1,    49,    50,    51,    52,    53,    54,
      55,    -1,    -1,    -1,    59,    -1,    -1,    62,   201,    64,
      65,    66,    67,    68,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    82,    -1,    -1,
      -1,    -1,    -1,    -1,    89,    90,    -1,    -1,    93,    94,
      95,    96,    97,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   106,   107,    -1,    -1,    -1,   111,   112,   113,   114,
     115,   116,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   141,   142,   143,    -1,
      -1,   146,   147,    -1,   149,    -1,    -1,    -1,   153,   154,
      -1,   156,    -1,    -1,   159,   160,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   172,    -1,    -1,
      -1,   176,    -1,    -1,    -1,   180,   181,    -1,    -1,   184,
      -1,    -1,    49,    50,    51,    52,    53,    54,    55,    -1,
      -1,    -1,    59,    -1,    -1,    62,   201,    64,    65,    66,
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
      -1,    -1,    -1,    -1,    -1,   172,    -1,    -1,    -1,   176,
      -1,    -1,    -1,   180,   181,    -1,    -1,   184,    -1,    -1,
      49,    50,    51,    52,    53,    54,    55,    -1,    -1,    -1,
      59,    -1,    -1,    62,   201,    64,    65,    66,    67,    68,
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
      -1,    -1,    -1,   172,    -1,    -1,    -1,   176,    -1,    -1,
      -1,   180,   181,    -1,    -1,   184,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   201
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
     150,   151,   152,   158,   162,   163,   164,   165,   166,   170,
     171,   173,   174,   175,   177,   178,   182,   183,   185,   186,
     187,   188,   189,   190,   191,   192,   193,   194,   195,   196,
     197,   198,   199,   200,   205,   217,   218,   219,   220,   221,
     222,   227,   228,   229,   230,   231,   234,   235,   240,   241,
     242,   243,   244,   245,   246,   249,   250,   251,   252,   254,
     257,   258,   259,   260,   261,   262,   263,   264,   265,   266,
     267,   268,   269,   280,   281,   282,   283,   284,   286,   290,
     291,   304,   305,   306,   307,   308,   309,   311,   312,   313,
     321,   322,   324,   328,   330,   331,   332,   334,   336,   337,
     338,   339,   341,   342,   343,   344,   345,   347,   348,   350,
     351,   352,   353,   354,   355,   356,   358,   362,   363,   364,
      78,   108,   109,   110,   206,   285,   155,   310,    19,    66,
      68,    90,    92,    93,    96,   106,   107,   111,   179,   323,
     357,    49,    50,    51,    52,    53,    54,    55,    59,    62,
      64,    65,    66,    67,    68,    82,    89,    90,    93,    94,
      95,    96,    97,   106,   107,   111,   112,   113,   114,   115,
     116,   141,   142,   143,   146,   147,   149,   153,   154,   156,
     159,   160,   172,   176,   180,   181,   184,   201,   206,   287,
     288,   368,   369,   106,   107,   270,   369,    19,    20,    48,
      61,   232,    28,    48,   233,   179,   369,   369,    28,    61,
     128,   139,   140,   144,   314,   315,   318,   369,   314,   314,
     314,   314,   314,   314,    81,   118,   119,   120,   121,   122,
     123,   124,   125,   126,   127,   129,   132,   292,    53,   220,
      55,    53,    54,   209,    89,   369,   369,    82,   111,   179,
     144,   144,   146,   147,   329,   153,   154,   156,   157,   335,
     153,   161,   333,   155,   159,   161,   326,   144,   181,   326,
     326,    82,   155,   340,    63,   161,   167,   168,   327,    82,
     144,   172,   144,   148,   325,   144,   346,   176,   326,   144,
     349,   179,   180,   181,    26,    70,    26,   253,    26,   256,
     256,    82,    69,   253,   253,    12,     6,   144,    82,   144,
     147,   201,    45,     0,   219,    45,   205,     5,   206,   222,
     236,   238,   239,   269,   280,   281,   282,   283,   284,   364,
     206,   237,   222,   280,   281,   282,   283,   284,    82,   369,
     221,   221,   290,   210,   248,   273,   274,   275,   222,   276,
     277,   366,   367,   369,    67,   320,   367,   276,   367,    90,
      91,   224,   225,   226,   248,   273,    97,   203,   206,   204,
      47,    52,    82,   215,    52,    95,    94,    94,   115,    52,
     112,   154,    64,    65,   369,   369,   206,   369,   207,   209,
     210,   208,    61,   369,   112,    26,    30,   369,   210,   210,
     210,    39,   315,   210,   310,    82,   209,   209,   209,   209,
      82,   118,   119,   120,   121,   123,   124,   125,   126,   127,
     128,   132,   300,   209,   209,   209,   209,   209,   130,   131,
     209,    41,    42,    43,    82,   293,   205,    56,    58,   359,
     360,   361,    82,    82,   144,   112,   144,   144,   326,   144,
     155,   326,   144,   326,   326,    82,   155,   161,    82,   326,
     160,   326,    82,   326,    82,    82,   327,   144,   144,   207,
     326,   144,    82,   184,   369,    82,   161,   255,   255,   208,
     369,    82,    82,   144,   144,   144,   144,    45,    82,    82,
     207,   211,   222,   273,    26,   247,   274,   167,   169,   365,
      69,   206,   208,   369,    26,   365,    91,    82,    26,    97,
     204,   256,   209,    77,   161,   209,   209,   106,   107,   369,
     209,   287,   117,   144,   289,   369,    44,    82,   117,   144,
     206,   271,   272,   369,   369,   144,   141,   142,   143,   319,
     144,   316,   317,   369,   209,    82,    87,   303,   303,   303,
     303,   208,   209,   209,   209,   209,   209,   209,   209,   209,
     209,   209,   209,   299,   303,   303,   301,   303,   301,   303,
     209,   209,   303,    82,    82,    57,    59,   207,   325,   326,
     326,   144,   326,   326,    82,   326,   149,   207,   144,    70,
     209,    26,   208,    82,    82,   209,   206,   211,   211,   276,
     210,   369,   210,   369,   369,   369,   369,   276,   210,   278,
      82,     9,   223,   276,   255,    82,   116,   202,   289,   289,
     369,   369,   209,   289,    82,   206,   207,   211,   208,   369,
     211,   211,   317,   211,   302,   303,   299,   299,   299,   299,
      82,   302,   303,   303,   303,   303,   303,   303,   301,   301,
     303,   303,    41,   207,   299,   299,   299,   299,   299,   212,
     212,   299,    60,   361,   326,   144,   144,   207,   184,    82,
     369,   369,    82,   144,    82,   369,   369,    69,   206,   369,
     279,   369,   277,   223,    19,   209,   209,   289,    82,   272,
     369,   211,   299,    41,    41,    41,    41,   209,   300,    41,
      41,    41,    41,    41,   118,   132,   133,   134,   136,   294,
     295,    66,   296,    41,    82,   209,   211,   211,   369,   369,
     207,   211,    19,   367,   289,   289,    41,   302,   210,   210,
     210,   210,   210,   213,   207,   210,   213,    82,   369,   367,
     303,   303,   135,   211,   211,    82,   297,   298,    41,   294,
     301,    41,   211,   211,   209,   209,   211,   207,   211,   214,
     303,   303,   297,   301,   211,   211
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

  case 344:

    { result->cur_stmt_type_ = OBPROXY_T_BINLOG_STR; ;}
    break;

  case 345:

    {
    result->cur_stmt_type_ = OBPROXY_T_SHOW_BINLOG_SERVER_FOR_TENANT;
    result->is_binlog_related_ = true;
;}
    break;

  case 346:

    { result->is_binlog_related_ = true; ;}
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

    {
;}
    break;

  case 351:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 352:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 353:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 354:

    {;}
    break;

  case 355:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 356:

    {;}
    break;

  case 357:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 359:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 360:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 361:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 362:

    {;}
    break;

  case 363:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_ALL); ;}
    break;

  case 364:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 365:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 366:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 368:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 369:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 370:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 371:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 372:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 374:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 375:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 376:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 377:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 378:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 379:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 380:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 381:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 382:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 383:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_READ_STALE); ;}
    break;

  case 384:

    {;}
    break;

  case 385:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 386:

    {;}
    break;

  case 387:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 388:

    {;}
    break;

  case 390:

    {;}
    break;

  case 391:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 392:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 393:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 394:

    {;}
    break;

  case 395:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 396:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 397:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_GLOBALINDEX); ;}
    break;

  case 398:

    {;}
    break;

  case 399:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 400:

    {;}
    break;

  case 401:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 402:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 403:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); SET_ICMD_ONE_ID((yyvsp[(3) - (3)].num)); ;}
    break;

  case 404:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 405:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 407:

    {;}
    break;

  case 408:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 409:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 410:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 411:

    {;}
    break;

  case 412:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 414:

    {;}
    break;

  case 415:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 416:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 417:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 418:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 419:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 420:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 421:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 422:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 423:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 424:

    {;}
    break;

  case 425:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 426:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 427:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 428:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 429:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 430:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 431:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 434:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 435:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 436:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 437:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 444:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 445:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 447:

    {;}
    break;

  case 448:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 449:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 450:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 451:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 452:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT();
                                                ;}
    break;

  case 453:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 454:

    {
                                                  result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                                                  result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                                                  result->table_info_.dblink_name_ = (yyvsp[(5) - (5)].str);
                                                 ;}
    break;

  case 455:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 456:

    {
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.dblink_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 457:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 458:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 459:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 460:

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

void ob_proxy_utf8_parser_fatal_error(yyconst char *msg, yyscan_t yyscanner)
{
  fprintf(stderr, "FATAL ERROR:%s\n", msg);
  ObProxyParseResult *p = ob_proxy_parser_utf8_yyget_extra(yyscanner);
  if (OB_ISNULL(p)) {
    fprintf(stderr, "unexpected null parse result\n");
  } else {
    longjmp(p->jmp_buf_, 1);//the secord param must be non-zero value
  }
}

int obproxy_parse_utf8_sql(ObProxyParseResult* p, const char* buf, size_t len)
{
  int ret = OB_SUCCESS;
  //obproxydebug = 1;
  if (OB_ISNULL(p) || OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    // print err msg later
  } else if (OB_FAIL(ob_proxy_parser_utf8_yylex_init_extra(p, &(p->yyscan_info_)))) {
    // print err msg later
  } else {
    int val = setjmp(p->jmp_buf_);
    if (val) {
      ret = OB_PARSER_ERR_PARSE_SQL;
    } else {
      ob_proxy_parser_utf8_yy_scan_buffer((char *)buf, len, p->yyscan_info_);
      if (OB_FAIL(ob_proxy_parser_utf8_yyparse(p))) {
        // print err msg later
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

