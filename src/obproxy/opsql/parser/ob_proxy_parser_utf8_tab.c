
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
     WARNINGS = 318,
     ERRORS = 319,
     TRACE = 320,
     QUICK = 321,
     COUNT = 322,
     AS = 323,
     WHERE = 324,
     VALUES = 325,
     ORDER = 326,
     GROUP = 327,
     HAVING = 328,
     INTO = 329,
     UNION = 330,
     FOR = 331,
     TX_READ_ONLY = 332,
     SELECT_OBPROXY_ROUTE_ADDR = 333,
     SET_OBPROXY_ROUTE_ADDR = 334,
     NAME_OB_DOT = 335,
     NAME_OB = 336,
     EXPLAIN = 337,
     EXPLAIN_ROUTE = 338,
     DESC = 339,
     DESCRIBE = 340,
     NAME_STR = 341,
     LOAD = 342,
     DATA = 343,
     LOCAL = 344,
     INFILE = 345,
     USE = 346,
     HELP = 347,
     SET_NAMES = 348,
     SET_CHARSET = 349,
     SET_PASSWORD = 350,
     SET_DEFAULT = 351,
     SET_OB_READ_CONSISTENCY = 352,
     SET_TX_READ_ONLY = 353,
     GLOBAL = 354,
     SESSION = 355,
     NUMBER_VAL = 356,
     GROUP_ID = 357,
     TABLE_ID = 358,
     ELASTIC_ID = 359,
     TESTLOAD = 360,
     ODP_COMMENT = 361,
     TNT_ID = 362,
     DISASTER_STATUS = 363,
     TRACE_ID = 364,
     RPC_ID = 365,
     TARGET_DB_SERVER = 366,
     TRACE_LOG = 367,
     DBP_COMMENT = 368,
     ROUTE_TAG = 369,
     SYS_TAG = 370,
     TABLE_NAME = 371,
     SCAN_ALL = 372,
     STICKY_SESSION = 373,
     PARALL = 374,
     SHARD_KEY = 375,
     STOP_DDL_TASK = 376,
     RETRY_DDL_TASK = 377,
     QUERY_TIMEOUT = 378,
     READ_CONSISTENCY = 379,
     WEAK = 380,
     STRONG = 381,
     FROZEN = 382,
     INT_NUM = 383,
     SHOW_PROXYNET = 384,
     THREAD = 385,
     CONNECTION = 386,
     LIMIT = 387,
     OFFSET = 388,
     SHOW_PROCESSLIST = 389,
     SHOW_PROXYSESSION = 390,
     SHOW_GLOBALSESSION = 391,
     ATTRIBUTE = 392,
     VARIABLES = 393,
     ALL = 394,
     STAT = 395,
     READ_STALE = 396,
     SHOW_PROXYCONFIG = 397,
     DIFF = 398,
     USER = 399,
     LIKE = 400,
     SHOW_PROXYSM = 401,
     SHOW_PROXYCLUSTER = 402,
     SHOW_PROXYRESOURCE = 403,
     SHOW_PROXYCONGESTION = 404,
     SHOW_PROXYROUTE = 405,
     PARTITION = 406,
     ROUTINE = 407,
     SUBPARTITION = 408,
     SHOW_PROXYVIP = 409,
     SHOW_PROXYMEMORY = 410,
     OBJPOOL = 411,
     SHOW_SQLAUDIT = 412,
     SHOW_WARNLOG = 413,
     SHOW_PROXYSTAT = 414,
     REFRESH = 415,
     SHOW_PROXYTRACE = 416,
     SHOW_PROXYINFO = 417,
     BINARY = 418,
     UPGRADE = 419,
     IDC = 420,
     SHOW_ELASTIC_ID = 421,
     SHOW_TOPOLOGY = 422,
     GROUP_NAME = 423,
     SHOW_DB_VERSION = 424,
     SHOW_DATABASES = 425,
     SHOW_TABLES = 426,
     SHOW_FULL_TABLES = 427,
     SELECT_DATABASE = 428,
     SELECT_PROXY_STATUS = 429,
     SHOW_CREATE_TABLE = 430,
     SELECT_PROXY_VERSION = 431,
     SHOW_COLUMNS = 432,
     SHOW_INDEX = 433,
     ALTER_PROXYCONFIG = 434,
     ALTER_PROXYRESOURCE = 435,
     PING_PROXY = 436,
     KILL_PROXYSESSION = 437,
     KILL_GLOBALSESSION = 438,
     KILL = 439,
     QUERY = 440,
     SHOW_BINLOG_SERVER_FOR_TENANT = 441
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
#define YYFINAL  336
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   2581

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  198
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  148
/* YYNRULES -- Number of rules.  */
#define YYNRULES  465
/* YYNRULES -- Number of states.  */
#define YYNSTATES  766

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   441

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,   196,     2,     2,     2,     2,
     192,   193,   197,     2,   189,     2,   190,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   187,
       2,   191,     2,     2,   188,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   194,     2,   195,     2,     2,     2,     2,
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
     185,   186
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
      88,    90,    94,    95,    97,   103,   110,   112,   114,   117,
     120,   123,   126,   129,   132,   135,   138,   140,   142,   145,
     148,   150,   152,   154,   156,   158,   159,   161,   163,   166,
     168,   169,   171,   174,   177,   179,   181,   183,   185,   187,
     189,   191,   193,   196,   201,   204,   206,   208,   212,   215,
     219,   222,   225,   229,   233,   235,   237,   239,   241,   243,
     245,   247,   249,   251,   254,   256,   258,   260,   262,   263,
     266,   267,   269,   272,   278,   282,   284,   288,   290,   292,
     294,   296,   298,   300,   303,   305,   307,   309,   311,   313,
     315,   318,   321,   323,   326,   329,   334,   339,   342,   347,
     348,   351,   352,   355,   359,   363,   369,   371,   373,   377,
     383,   391,   393,   397,   399,   401,   403,   405,   407,   409,
     415,   417,   421,   427,   428,   430,   434,   436,   438,   440,
     443,   447,   449,   451,   454,   456,   459,   463,   467,   469,
     471,   473,   474,   478,   480,   484,   488,   494,   497,   500,
     505,   508,   511,   515,   517,   522,   529,   534,   540,   547,
     552,   556,   558,   560,   562,   564,   567,   571,   577,   584,
     591,   598,   605,   612,   620,   627,   634,   641,   648,   657,
     666,   673,   674,   677,   680,   683,   685,   689,   691,   696,
     701,   705,   712,   716,   721,   726,   733,   737,   739,   743,
     744,   748,   752,   756,   760,   764,   768,   772,   776,   780,
     784,   786,   790,   796,   800,   801,   803,   804,   806,   808,
     810,   812,   815,   817,   820,   822,   825,   828,   832,   833,
     835,   838,   840,   843,   845,   848,   851,   854,   857,   858,
     861,   863,   868,   873,   879,   885,   890,   892,   897,   903,
     905,   906,   908,   910,   912,   913,   915,   919,   923,   926,
     932,   934,   936,   938,   940,   942,   944,   946,   948,   950,
     952,   954,   956,   958,   960,   962,   964,   966,   968,   970,
     972,   974,   976,   978,   980,   981,   984,   989,   994,   995,
     998,   999,  1002,  1005,  1007,  1009,  1013,  1016,  1020,  1025,
    1027,  1030,  1031,  1034,  1038,  1041,  1044,  1047,  1048,  1051,
    1055,  1058,  1062,  1065,  1069,  1073,  1078,  1081,  1083,  1086,
    1089,  1093,  1096,  1099,  1100,  1102,  1104,  1107,  1110,  1114,
    1117,  1119,  1122,  1124,  1127,  1130,  1134,  1137,  1140,  1143,
    1144,  1146,  1150,  1156,  1159,  1163,  1166,  1167,  1169,  1172,
    1175,  1178,  1181,  1186,  1192,  1198,  1202,  1204,  1207,  1211,
    1215,  1218,  1221,  1225,  1229,  1230,  1233,  1235,  1239,  1243,
    1247,  1248,  1250,  1252,  1256,  1259,  1263,  1266,  1269,  1271,
    1272,  1275,  1280,  1283,  1288,  1291,  1293,  1297,  1300,  1305,
    1309,  1315,  1317,  1319,  1321,  1323,  1325,  1327,  1329,  1331,
    1333,  1335,  1337,  1339,  1341,  1343,  1345,  1347,  1349,  1351,
    1353,  1355,  1357,  1359,  1361,  1363,  1365,  1367,  1369,  1371,
    1373,  1375,  1377,  1379,  1381,  1383
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     199,     0,    -1,   200,    -1,     1,    -1,   201,    -1,   200,
     201,    -1,   202,    45,    -1,   202,   187,    -1,   202,   187,
      45,    -1,   187,    -1,   187,    45,    -1,    53,   202,   187,
      -1,   203,    -1,   271,   203,    -1,   204,    -1,   262,    -1,
     267,    -1,   263,    -1,   264,    -1,   265,    -1,   211,    -1,
     210,    -1,   334,    -1,   298,    -1,   233,    -1,   299,    -1,
     338,    -1,   339,    -1,   245,    -1,   246,    -1,   247,    -1,
     248,    -1,   249,    -1,   250,    -1,   251,    -1,   212,    -1,
     224,    -1,   266,    -1,   300,    -1,   209,    -1,   340,    -1,
     285,   230,   229,    -1,    -1,     9,    -1,    90,    81,   205,
      19,   343,    -1,    89,    90,    81,   205,    19,   343,    -1,
     207,    -1,   206,    -1,   292,   208,    -1,   226,   204,    -1,
     226,   262,    -1,   226,   264,    -1,   226,   265,    -1,   226,
     263,    -1,   226,   266,    -1,   228,   203,    -1,   213,    -1,
     225,    -1,    14,   214,    -1,    15,   215,    -1,    16,    -1,
      17,    -1,    18,    -1,   216,    -1,   217,    -1,    -1,    19,
      -1,    61,    -1,    20,    61,    -1,    48,    -1,    -1,    48,
      -1,   121,   128,    -1,   122,   128,    -1,   204,    -1,   262,
      -1,   263,    -1,   265,    -1,   264,    -1,   340,    -1,   251,
      -1,   266,    -1,   188,    81,    -1,   219,   189,   188,    81,
      -1,   188,    81,    -1,   220,    -1,   218,    -1,    28,   345,
      26,    -1,    29,   345,    -1,    29,   345,    30,    -1,   222,
     221,    -1,   223,   219,    -1,    15,    28,   345,    -1,    31,
      28,   345,    -1,    21,    -1,    22,    -1,    23,    -1,    24,
      -1,    49,    -1,    25,    -1,    50,    -1,    51,    -1,   227,
      -1,   227,    81,    -1,    82,    -1,    84,    -1,    85,    -1,
      83,    -1,    -1,    26,   258,    -1,    -1,   255,    -1,     5,
      77,    -1,     5,    77,   230,    26,   258,    -1,     5,    77,
     255,    -1,   176,    -1,   176,    68,   345,    -1,   231,    -1,
     232,    -1,   243,    -1,   244,    -1,   234,    -1,   242,    -1,
     167,   235,    -1,   241,    -1,   173,    -1,   174,    -1,   170,
      -1,   239,    -1,   240,    -1,   177,   235,    -1,   178,   235,
      -1,   236,    -1,   227,   345,    -1,    26,   345,    -1,    26,
     345,    26,   345,    -1,    26,   345,   190,   345,    -1,   175,
      81,    -1,   175,    81,   190,    81,    -1,    -1,   145,    81,
      -1,    -1,    26,    81,    -1,   171,   238,   237,    -1,   172,
     238,   237,    -1,    11,    19,    52,   238,   237,    -1,   169,
      -1,   166,    -1,   166,    26,    81,    -1,   166,    69,   168,
     191,    81,    -1,   166,    26,    81,    69,   168,   191,    81,
      -1,    78,    -1,    79,   191,   128,    -1,    93,    -1,    94,
      -1,    95,    -1,    96,    -1,    97,    -1,    98,    -1,    13,
     252,   192,   253,   193,    -1,   345,    -1,   345,   190,   345,
      -1,   345,   190,   345,   190,   345,    -1,    -1,   254,    -1,
     253,   189,   254,    -1,    81,    -1,   128,    -1,   101,    -1,
     188,    81,    -1,   188,   188,    81,    -1,    44,    -1,   256,
      -1,   255,   256,    -1,   257,    -1,   192,   193,    -1,   192,
     204,   193,    -1,   192,   255,   193,    -1,   342,    -1,   259,
      -1,   204,    -1,    -1,   192,   261,   193,    -1,   345,    -1,
     261,   189,   345,    -1,   288,   343,   341,    -1,   288,   343,
     341,   260,   259,    -1,   290,   258,    -1,   286,   258,    -1,
     287,   297,    26,   258,    -1,   291,   343,    -1,    12,   268,
      -1,   269,   189,   268,    -1,   269,    -1,   188,   345,   191,
     270,    -1,   188,   188,    99,   345,   191,   270,    -1,    99,
     345,   191,   270,    -1,   188,   188,   345,   191,   270,    -1,
     188,   188,   100,   345,   191,   270,    -1,   100,   345,   191,
     270,    -1,   345,   191,   270,    -1,   345,    -1,   128,    -1,
     101,    -1,   272,    -1,   272,   271,    -1,    40,   273,    41,
      -1,    40,   106,   281,   280,    41,    -1,    40,   103,   191,
     284,   280,    41,    -1,    40,   116,   191,   284,   280,    41,
      -1,    40,   102,   191,   284,   280,    41,    -1,    40,   104,
     191,   284,   280,    41,    -1,    40,   105,   191,   284,   280,
      41,    -1,    40,    80,    81,   191,   283,   280,    41,    -1,
      40,   109,   191,   282,   280,    41,    -1,    40,   110,   191,
     282,   280,    41,    -1,    40,   107,   191,   284,   280,    41,
      -1,    40,   108,   191,   284,   280,    41,    -1,    40,   113,
     114,   191,   194,   275,   195,    41,    -1,    40,   113,   115,
     191,   194,   277,   195,    41,    -1,    40,   111,   191,   284,
     280,    41,    -1,    -1,   273,   274,    -1,    42,    81,    -1,
      43,    81,    -1,    81,    -1,   276,   189,   275,    -1,   276,
      -1,   102,   192,   284,   193,    -1,   116,   192,   284,   193,
      -1,   117,   192,   193,    -1,   117,   192,   119,   191,   284,
     193,    -1,   118,   192,   193,    -1,   120,   192,   278,   193,
      -1,    65,   192,   282,   193,    -1,    65,   192,   282,   196,
     282,   193,    -1,   279,   189,   278,    -1,   279,    -1,    81,
     191,   284,    -1,    -1,   280,   189,   281,    -1,   102,   191,
     284,    -1,   103,   191,   284,    -1,   116,   191,   284,    -1,
     104,   191,   284,    -1,   105,   191,   284,    -1,   109,   191,
     282,    -1,   110,   191,   282,    -1,   107,   191,   284,    -1,
     108,   191,   284,    -1,   112,    -1,   111,   191,   284,    -1,
      81,   190,    81,   191,   283,    -1,    81,   191,   283,    -1,
      -1,   284,    -1,    -1,   284,    -1,    81,    -1,    86,    -1,
       5,    -1,    32,   293,    -1,     8,    -1,    33,   293,    -1,
       6,    -1,    34,   293,    -1,     7,   289,    -1,    35,   293,
     289,    -1,    -1,   139,    -1,   139,    47,    -1,     9,    -1,
      36,   293,    -1,    10,    -1,    37,   293,    -1,    87,    88,
      -1,    38,   293,    -1,   294,    39,    -1,    -1,   295,   294,
      -1,   345,    -1,   345,   192,   128,   193,    -1,   345,   192,
     345,   193,    -1,   345,   192,   345,   345,   193,    -1,   345,
     192,   345,   128,   193,    -1,   123,   192,   128,   193,    -1,
     128,    -1,   124,   192,   296,   193,    -1,    61,   192,   345,
     345,   193,    -1,   112,    -1,    -1,   125,    -1,   126,    -1,
     127,    -1,    -1,    66,    -1,    11,   333,    63,    -1,    11,
     333,    64,    -1,    11,    65,    -1,    11,    65,    81,   191,
      81,    -1,   304,    -1,   306,    -1,   307,    -1,   310,    -1,
     308,    -1,   312,    -1,   313,    -1,   314,    -1,   315,    -1,
     317,    -1,   318,    -1,   319,    -1,   320,    -1,   321,    -1,
     323,    -1,   324,    -1,   326,    -1,   327,    -1,   328,    -1,
     329,    -1,   330,    -1,   331,    -1,   332,    -1,   186,    -1,
      -1,   132,   128,    -1,   132,   128,   189,   128,    -1,   132,
     128,   133,   128,    -1,    -1,   145,    81,    -1,    -1,   145,
      81,    -1,   129,   305,    -1,   130,    -1,   131,    -1,   131,
     128,   301,    -1,   142,   302,    -1,   142,   143,   302,    -1,
     142,   143,   144,   302,    -1,   134,    -1,   136,   309,    -1,
      -1,   137,    81,    -1,   137,   145,    81,    -1,   137,   139,
      -1,   145,    81,    -1,   135,   311,    -1,    -1,   137,   302,
      -1,   137,   128,   302,    -1,   140,   302,    -1,   140,   128,
     302,    -1,   138,   302,    -1,   138,   128,   302,    -1,   138,
     139,   302,    -1,   138,   139,   128,   302,    -1,   141,   302,
      -1,   146,    -1,   146,   128,    -1,   147,   302,    -1,   147,
     165,   302,    -1,   148,   302,    -1,   149,   316,    -1,    -1,
      81,    -1,   139,    -1,   139,    81,    -1,   150,   303,    -1,
     150,   152,   303,    -1,   150,   151,    -1,   154,    -1,   154,
      81,    -1,   155,    -1,   155,   128,    -1,   155,   156,    -1,
     155,   156,   128,    -1,   157,   301,    -1,   157,   128,    -1,
     158,   322,    -1,    -1,   128,    -1,   128,   189,   128,    -1,
     128,   189,   128,   189,    81,    -1,   159,   302,    -1,   159,
     160,   302,    -1,   161,   325,    -1,    -1,   128,    -1,   128,
     128,    -1,   162,   163,    -1,   162,   164,    -1,   162,   165,
      -1,   179,    12,    81,   191,    -1,   179,    12,    81,   191,
      81,    -1,   179,    12,    81,   191,   128,    -1,   180,     6,
      81,    -1,   181,    -1,   182,   128,    -1,   182,   128,   128,
      -1,   183,    81,   128,    -1,   183,    81,    -1,   184,   128,
      -1,   184,   131,   128,    -1,   184,   185,   128,    -1,    -1,
      67,   197,    -1,    53,    -1,    54,    55,   335,    -1,    62,
      53,    81,    -1,    62,    54,    81,    -1,    -1,   336,    -1,
     337,    -1,   336,   189,   337,    -1,    56,    57,    -1,    58,
      59,    60,    -1,    91,   345,    -1,    92,    81,    -1,    81,
      -1,    -1,   153,   345,    -1,   153,   192,   345,   193,    -1,
     151,   345,    -1,   151,   192,   345,   193,    -1,   343,   341,
      -1,   345,    -1,   345,   190,   345,    -1,   345,   345,    -1,
     345,   190,   345,   345,    -1,   345,    68,   345,    -1,   345,
     190,   345,    68,   345,    -1,    54,    -1,    62,    -1,    53,
      -1,    55,    -1,    59,    -1,    64,    -1,    63,    -1,    67,
      -1,    66,    -1,    65,    -1,   130,    -1,   131,    -1,   133,
      -1,   137,    -1,   138,    -1,   140,    -1,   143,    -1,   144,
      -1,   156,    -1,   160,    -1,   164,    -1,   165,    -1,   185,
      -1,   168,    -1,    49,    -1,    50,    -1,    51,    -1,    89,
      -1,    88,    -1,    52,    -1,   125,    -1,   126,    -1,   127,
      -1,    81,    -1,   344,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   338,   338,   339,   341,   342,   344,   345,   346,   347,
     348,   349,   351,   352,   354,   355,   356,   357,   358,   359,
     360,   361,   362,   363,   364,   365,   366,   367,   368,   369,
     370,   371,   372,   373,   374,   375,   376,   377,   378,   379,
     380,   382,   387,   389,   391,   396,   401,   402,   404,   406,
     407,   408,   409,   410,   411,   413,   415,   416,   418,   419,
     420,   421,   422,   423,   424,   426,   427,   428,   429,   430,
     432,   433,   435,   441,   447,   448,   449,   450,   451,   452,
     453,   454,   456,   463,   471,   479,   480,   483,   489,   494,
     500,   503,   506,   511,   517,   518,   519,   520,   521,   522,
     523,   524,   526,   527,   529,   530,   531,   533,   535,   536,
     538,   539,   541,   542,   543,   545,   546,   548,   549,   550,
     551,   552,   554,   555,   556,   557,   558,   559,   560,   561,
     562,   563,   564,   565,   572,   576,   581,   587,   592,   599,
     600,   602,   603,   605,   609,   614,   619,   621,   622,   627,
     632,   639,   642,   648,   649,   650,   651,   652,   653,   656,
     658,   662,   667,   675,   678,   683,   688,   693,   698,   703,
     708,   713,   721,   722,   724,   726,   727,   728,   730,   731,
     733,   735,   736,   738,   739,   741,   745,   746,   747,   748,
     749,   754,   756,   757,   759,   763,   767,   771,   775,   779,
     783,   787,   792,   797,   803,   804,   806,   807,   808,   809,
     810,   811,   812,   813,   819,   820,   821,   822,   823,   824,
     825,   826,   827,   829,   830,   831,   833,   834,   836,   841,
     846,   847,   848,   849,   851,   852,   854,   855,   857,   865,
     866,   868,   869,   870,   871,   872,   873,   874,   875,   876,
     877,   878,   879,   885,   887,   888,   890,   891,   893,   894,
     896,   897,   898,   899,   900,   901,   902,   903,   905,   906,
     907,   909,   910,   911,   912,   913,   914,   916,   917,   918,
     920,   921,   922,   923,   924,   925,   926,   927,   928,   933,
     935,   936,   937,   938,   940,   941,   944,   945,   946,   947,
     950,   951,   952,   953,   954,   955,   956,   957,   958,   959,
     960,   961,   962,   963,   964,   965,   966,   967,   968,   969,
     970,   971,   972,   974,   979,   981,   985,   990,   998,   999,
    1003,  1004,  1007,  1009,  1010,  1011,  1015,  1016,  1017,  1021,
    1023,  1025,  1026,  1027,  1028,  1029,  1032,  1034,  1035,  1036,
    1037,  1038,  1039,  1040,  1041,  1042,  1043,  1047,  1048,  1052,
    1053,  1058,  1061,  1063,  1064,  1065,  1066,  1070,  1071,  1072,
    1076,  1077,  1081,  1082,  1083,  1084,  1088,  1089,  1092,  1094,
    1095,  1096,  1097,  1101,  1102,  1105,  1107,  1108,  1109,  1113,
    1114,  1115,  1119,  1120,  1121,  1125,  1129,  1133,  1134,  1138,
    1139,  1143,  1144,  1145,  1148,  1149,  1152,  1156,  1157,  1158,
    1160,  1161,  1163,  1164,  1167,  1168,  1171,  1177,  1180,  1182,
    1183,  1184,  1185,  1186,  1188,  1193,  1196,  1200,  1204,  1209,
    1213,  1219,  1220,  1221,  1222,  1223,  1224,  1225,  1226,  1227,
    1228,  1229,  1230,  1231,  1232,  1233,  1234,  1235,  1236,  1237,
    1238,  1239,  1240,  1241,  1242,  1243,  1244,  1245,  1246,  1247,
    1248,  1249,  1250,  1251,  1253,  1254
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
  "INDEX", "XA", "WARNINGS", "ERRORS", "TRACE", "QUICK", "COUNT", "AS",
  "WHERE", "VALUES", "ORDER", "GROUP", "HAVING", "INTO", "UNION", "FOR",
  "TX_READ_ONLY", "SELECT_OBPROXY_ROUTE_ADDR", "SET_OBPROXY_ROUTE_ADDR",
  "NAME_OB_DOT", "NAME_OB", "EXPLAIN", "EXPLAIN_ROUTE", "DESC", "DESCRIBE",
  "NAME_STR", "LOAD", "DATA", "LOCAL", "INFILE", "USE", "HELP",
  "SET_NAMES", "SET_CHARSET", "SET_PASSWORD", "SET_DEFAULT",
  "SET_OB_READ_CONSISTENCY", "SET_TX_READ_ONLY", "GLOBAL", "SESSION",
  "NUMBER_VAL", "GROUP_ID", "TABLE_ID", "ELASTIC_ID", "TESTLOAD",
  "ODP_COMMENT", "TNT_ID", "DISASTER_STATUS", "TRACE_ID", "RPC_ID",
  "TARGET_DB_SERVER", "TRACE_LOG", "DBP_COMMENT", "ROUTE_TAG", "SYS_TAG",
  "TABLE_NAME", "SCAN_ALL", "STICKY_SESSION", "PARALL", "SHARD_KEY",
  "STOP_DDL_TASK", "RETRY_DDL_TASK", "QUERY_TIMEOUT", "READ_CONSISTENCY",
  "WEAK", "STRONG", "FROZEN", "INT_NUM", "SHOW_PROXYNET", "THREAD",
  "CONNECTION", "LIMIT", "OFFSET", "SHOW_PROCESSLIST", "SHOW_PROXYSESSION",
  "SHOW_GLOBALSESSION", "ATTRIBUTE", "VARIABLES", "ALL", "STAT",
  "READ_STALE", "SHOW_PROXYCONFIG", "DIFF", "USER", "LIKE", "SHOW_PROXYSM",
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
  "SHOW_BINLOG_SERVER_FOR_TENANT", "';'", "'@'", "','", "'.'", "'='",
  "'('", "')'", "'{'", "'}'", "'#'", "'*'", "$accept", "root", "sql_stmts",
  "sql_stmt", "comment_stmt", "stmt", "select_stmt", "opt_replace_ignore",
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
  "replace_stmt", "update_stmt", "delete_stmt", "merge_stmt", "set_stmt",
  "set_expr_list", "set_expr", "set_var_value", "comment_expr_list",
  "comment_expr", "comment_list", "comment", "dbp_comment_list",
  "dbp_comment", "dbp_sys_comment", "dbp_kv_comment_list",
  "dbp_kv_comment", "odp_comment_list", "odp_comment",
  "tracer_right_string_val", "name_right_string_val", "right_string_val",
  "select_with_opt_hint", "update_with_opt_hint", "delete_with_opt_hint",
  "insert_with_opt_hint", "insert_all_when", "replace_with_opt_hint",
  "merge_with_opt_hint", "load_data_opt_hint", "hint_list_with_end",
  "hint_list", "hint", "opt_read_consistency", "opt_quick", "show_stmt",
  "icmd_stmt", "binlog_stmt", "opt_limit", "opt_like", "opt_large_like",
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
     435,   436,   437,   438,   439,   440,   441,    59,    64,    44,
      46,    61,    40,    41,   123,   125,    35,    42
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   198,   199,   199,   200,   200,   201,   201,   201,   201,
     201,   201,   202,   202,   203,   203,   203,   203,   203,   203,
     203,   203,   203,   203,   203,   203,   203,   203,   203,   203,
     203,   203,   203,   203,   203,   203,   203,   203,   203,   203,
     203,   204,   205,   205,   206,   207,   208,   208,   209,   210,
     210,   210,   210,   210,   210,   211,   212,   212,   213,   213,
     213,   213,   213,   213,   213,   214,   214,   214,   214,   214,
     215,   215,   216,   217,   218,   218,   218,   218,   218,   218,
     218,   218,   219,   219,   220,   221,   221,   222,   223,   223,
     224,   224,   224,   224,   225,   225,   225,   225,   225,   225,
     225,   225,   226,   226,   227,   227,   227,   228,   229,   229,
     230,   230,   231,   231,   231,   232,   232,   233,   233,   233,
     233,   233,   234,   234,   234,   234,   234,   234,   234,   234,
     234,   234,   234,   234,   235,   235,   235,   236,   236,   237,
     237,   238,   238,   239,   239,   240,   241,   242,   242,   242,
     242,   243,   244,   245,   246,   247,   248,   249,   250,   251,
     252,   252,   252,   253,   253,   253,   254,   254,   254,   254,
     254,   254,   255,   255,   256,   257,   257,   257,   258,   258,
     259,   260,   260,   261,   261,   262,   262,   263,   264,   265,
     266,   267,   268,   268,   269,   269,   269,   269,   269,   269,
     269,   270,   270,   270,   271,   271,   272,   272,   272,   272,
     272,   272,   272,   272,   272,   272,   272,   272,   272,   272,
     272,   273,   273,   274,   274,   274,   275,   275,   276,   276,
     276,   276,   276,   276,   277,   277,   278,   278,   279,   280,
     280,   281,   281,   281,   281,   281,   281,   281,   281,   281,
     281,   281,   281,   281,   282,   282,   283,   283,   284,   284,
     285,   285,   286,   286,   287,   287,   288,   288,   289,   289,
     289,   290,   290,   291,   291,   292,   292,   293,   294,   294,
     295,   295,   295,   295,   295,   295,   295,   295,   295,   295,
     296,   296,   296,   296,   297,   297,   298,   298,   298,   298,
     299,   299,   299,   299,   299,   299,   299,   299,   299,   299,
     299,   299,   299,   299,   299,   299,   299,   299,   299,   299,
     299,   299,   299,   300,   301,   301,   301,   301,   302,   302,
     303,   303,   304,   305,   305,   305,   306,   306,   306,   307,
     308,   309,   309,   309,   309,   309,   310,   311,   311,   311,
     311,   311,   311,   311,   311,   311,   311,   312,   312,   313,
     313,   314,   315,   316,   316,   316,   316,   317,   317,   317,
     318,   318,   319,   319,   319,   319,   320,   320,   321,   322,
     322,   322,   322,   323,   323,   324,   325,   325,   325,   326,
     326,   326,   327,   327,   327,   328,   329,   330,   330,   331,
     331,   332,   332,   332,   333,   333,   334,   334,   334,   334,
     335,   335,   336,   336,   337,   337,   338,   339,   340,   341,
     341,   341,   341,   341,   342,   343,   343,   343,   343,   343,
     343,   344,   344,   344,   344,   344,   344,   344,   344,   344,
     344,   344,   344,   344,   344,   344,   344,   344,   344,   344,
     344,   344,   344,   344,   344,   344,   344,   344,   344,   344,
     344,   344,   344,   344,   345,   345
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     1,     1,     1,     2,     2,     2,     3,     1,
       2,     3,     1,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     3,     0,     1,     5,     6,     1,     1,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     1,     2,     2,
       1,     1,     1,     1,     1,     0,     1,     1,     2,     1,
       0,     1,     2,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     2,     4,     2,     1,     1,     3,     2,     3,
       2,     2,     3,     3,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     2,     1,     1,     1,     1,     0,     2,
       0,     1,     2,     5,     3,     1,     3,     1,     1,     1,
       1,     1,     1,     2,     1,     1,     1,     1,     1,     1,
       2,     2,     1,     2,     2,     4,     4,     2,     4,     0,
       2,     0,     2,     3,     3,     5,     1,     1,     3,     5,
       7,     1,     3,     1,     1,     1,     1,     1,     1,     5,
       1,     3,     5,     0,     1,     3,     1,     1,     1,     2,
       3,     1,     1,     2,     1,     2,     3,     3,     1,     1,
       1,     0,     3,     1,     3,     3,     5,     2,     2,     4,
       2,     2,     3,     1,     4,     6,     4,     5,     6,     4,
       3,     1,     1,     1,     1,     2,     3,     5,     6,     6,
       6,     6,     6,     7,     6,     6,     6,     6,     8,     8,
       6,     0,     2,     2,     2,     1,     3,     1,     4,     4,
       3,     6,     3,     4,     4,     6,     3,     1,     3,     0,
       3,     3,     3,     3,     3,     3,     3,     3,     3,     3,
       1,     3,     5,     3,     0,     1,     0,     1,     1,     1,
       1,     2,     1,     2,     1,     2,     2,     3,     0,     1,
       2,     1,     2,     1,     2,     2,     2,     2,     0,     2,
       1,     4,     4,     5,     5,     4,     1,     4,     5,     1,
       0,     1,     1,     1,     0,     1,     3,     3,     2,     5,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     0,     2,     4,     4,     0,     2,
       0,     2,     2,     1,     1,     3,     2,     3,     4,     1,
       2,     0,     2,     3,     2,     2,     2,     0,     2,     3,
       2,     3,     2,     3,     3,     4,     2,     1,     2,     2,
       3,     2,     2,     0,     1,     1,     2,     2,     3,     2,
       1,     2,     1,     2,     2,     3,     2,     2,     2,     0,
       1,     3,     5,     2,     3,     2,     0,     1,     2,     2,
       2,     2,     4,     5,     5,     3,     1,     2,     3,     3,
       2,     2,     3,     3,     0,     2,     1,     3,     3,     3,
       0,     1,     1,     3,     2,     3,     2,     2,     1,     0,
       2,     4,     2,     4,     2,     1,     3,     2,     4,     3,
       5,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
       0,     3,   260,   264,   268,   262,   271,   273,   404,     0,
       0,    65,    70,    60,    61,    62,    94,    95,    96,    97,
      99,     0,     0,     0,   278,   278,   278,   278,   278,   278,
     278,   221,    98,   100,   101,   406,     0,     0,   151,     0,
     418,   104,   107,   105,   106,     0,     0,     0,   153,   154,
     155,   156,   157,   158,     0,     0,     0,   339,   347,   341,
     328,   357,   328,   328,   363,   330,   370,   372,   324,   379,
     328,   386,     0,   147,     0,   146,   127,   141,   141,   125,
     126,     0,   115,     0,     0,     0,     0,   396,     0,     0,
       0,   323,     9,     0,     2,     4,     0,    12,    14,    39,
      21,    20,    35,    56,    63,    64,     0,     0,    36,    57,
       0,   102,     0,   117,   118,    24,   121,   132,   128,   129,
     124,   122,   119,   120,    28,    29,    30,    31,    32,    33,
      34,    15,    17,    18,    19,    37,    16,     0,   204,   110,
       0,   294,     0,     0,     0,     0,    23,    25,    38,   300,
     301,   302,   304,   303,   305,   306,   307,   308,   309,   310,
     311,   312,   313,   314,   315,   316,   317,   318,   319,   320,
     321,   322,    22,    26,    27,    40,   112,   269,   266,     0,
     298,     0,     0,   455,   456,   457,   460,   433,   431,   434,
     435,   432,   437,   436,   440,   439,   438,   464,   459,   458,
       0,     0,   461,   462,   463,   441,   442,   443,   444,   445,
     446,   447,   448,   449,   450,   451,   452,   454,   453,     0,
     191,   193,   465,     0,     0,   160,    66,     0,    69,    67,
      58,     0,    71,    59,     0,    88,     0,     0,   289,     0,
       0,   286,   261,     0,   278,   280,   263,   265,   268,   272,
     274,   276,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   406,     0,   410,     0,
       0,     0,   275,   416,   417,    72,    73,   333,   334,   332,
     328,   328,   328,   328,   346,     0,     0,   340,   328,     0,
     336,   358,   328,   359,   361,   364,   365,   362,     0,   369,
     330,   367,   371,   373,   374,   377,     0,   376,   380,   378,
     328,   383,   387,   385,   389,   390,   391,     0,     0,     0,
     123,     0,   139,   139,   137,     0,   130,   131,     0,     0,
     397,   400,   401,     0,     0,    10,     1,     5,     6,     7,
     260,     0,    74,    86,    85,    90,    80,    75,    76,    78,
      77,    81,    79,     0,    91,    49,    50,    53,    51,    52,
      54,   103,   133,    55,    13,   205,     0,   108,   111,   172,
     174,   180,   188,   179,   178,   419,   425,   295,     0,   419,
     187,   190,     0,     0,    47,    46,    48,     0,   114,   270,
     141,     0,   405,   296,   297,     0,     0,     0,     0,     0,
       0,   163,     0,    68,    92,    87,    89,    93,     0,     0,
     290,   277,   279,     0,   267,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     250,     0,   239,     0,     0,   254,   254,     0,     0,     0,
       0,   206,     0,     0,   225,   222,    11,     0,     0,   407,
     411,   412,   408,   409,   152,   324,   328,   348,   328,   328,
     352,   328,   350,   356,   342,   344,     0,   345,   328,   337,
     329,   360,   366,   331,   368,   375,   325,     0,   384,   388,
     148,     0,   134,   142,     0,   143,   144,     0,   116,     0,
     395,   398,   399,   402,   403,     8,    84,    82,     0,   175,
       0,     0,     0,    41,   173,     0,     0,   424,     0,     0,
     427,     0,   181,     0,    42,     0,   139,     0,     0,     0,
       0,     0,     0,     0,   192,   203,   202,   200,   201,   171,
     166,   168,   167,     0,     0,   164,   161,     0,     0,   291,
     292,   293,     0,     0,     0,   256,   258,   259,   239,   239,
     239,   239,     0,   256,     0,     0,     0,     0,     0,     0,
     254,   254,     0,     0,     0,   239,   239,   239,   255,   239,
     239,     0,     0,   239,   223,   224,   414,     0,     0,   335,
     349,   353,   328,   354,   351,   343,   338,     0,     0,   381,
       0,     0,     0,     0,   140,   138,   392,     0,   176,   177,
     109,     0,   422,     0,   420,   429,   426,   189,     0,     0,
      42,    43,     0,   113,   145,   299,   196,   199,     0,     0,
       0,   194,   169,     0,     0,   159,     0,     0,   285,   287,
     281,     0,   282,     0,   239,   257,     0,     0,     0,     0,
       0,   253,   241,   242,   244,   245,   248,   249,   246,   247,
     251,   243,   207,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   415,   413,   355,   327,   326,     0,     0,   149,
     135,   136,   393,   394,    83,     0,     0,     0,   428,     0,
     183,   186,     0,     0,     0,     0,   197,   170,   165,   162,
     288,   284,   283,     0,   210,   208,   211,   212,   256,   240,
     216,   217,   214,   215,   220,     0,     0,     0,     0,     0,
       0,   227,     0,     0,   209,   382,     0,   423,   421,   430,
       0,   182,     0,    44,   195,   198,   213,   252,     0,     0,
       0,     0,     0,     0,     0,   254,     0,   150,   184,    45,
       0,     0,     0,   230,   232,     0,     0,   237,   218,   226,
       0,   219,   228,   229,     0,     0,   233,     0,   234,   254,
       0,   238,   236,     0,   231,   235
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    93,    94,    95,    96,    97,   371,   612,   384,   385,
     386,    99,   100,   101,   102,   103,   230,   233,   104,   105,
     343,   354,   344,   345,   106,   107,   108,   109,   110,   111,
     112,   503,   367,   113,   114,   115,   116,   320,   117,   485,
     322,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,   224,   534,   535,   368,   369,   370,
     372,   373,   609,   679,   131,   132,   133,   134,   135,   136,
     220,   221,   527,   137,   138,   265,   445,   710,   711,   713,
     746,   747,   564,   432,   567,   634,   568,   139,   140,   141,
     142,   178,   143,   144,   145,   242,   243,   244,   542,   378,
     146,   147,   148,   307,   290,   301,   149,   279,   150,   151,
     152,   287,   153,   284,   154,   155,   156,   157,   297,   158,
     159,   160,   161,   162,   309,   163,   164,   313,   165,   166,
     167,   168,   169,   170,   171,   182,   172,   449,   450,   451,
     173,   174,   175,   507,   374,   375,   222,   376
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -504
static const yytype_int16 yypact[] =
{
     672,  -504,    42,  -504,   -44,  -504,  -504,  -504,    57,  1684,
    2343,   123,    77,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  2343,  2343,    80,  1865,  1865,  1865,  1865,  1865,  1865,
    1865,   177,  -504,  -504,  -504,  1038,    97,   158,  -504,   -50,
    -504,  -504,  -504,  -504,  -504,    74,  2343,    95,  -504,  -504,
    -504,  -504,  -504,  -504,    60,    65,   114,  -504,   109,   -47,
      18,    68,   -10,    73,   -29,    27,   121,   -62,   -36,   108,
     -31,   150,   131,    48,   227,  -504,  -504,   230,   230,  -504,
    -504,   211,   229,   227,   227,   301,   318,  -504,   197,   245,
     -51,  -504,   282,   328,   855,  -504,     2,  -504,  -504,  -504,
    -504,  -504,  -504,  -504,  -504,  -504,    35,   142,  -504,  -504,
     350,  2396,  1220,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,  -504,  -504,  -504,  -504,  1220,   291,   140,
     393,   267,  2343,   393,  2343,   214,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,  -504,  -504,  -504,     6,   287,  -504,   290,
     258,   147,   243,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    2343,  2343,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  1737,
    -504,   160,  -504,   170,   159,   162,  -504,   302,  -504,  -504,
    -504,  2343,  -504,  -504,   336,   335,  2343,   176,  -504,   179,
     180,  -504,  -504,   337,  1865,   187,  -504,  -504,   -44,  -504,
    -504,  -504,   299,   190,   198,   201,   205,   359,   206,   209,
     215,   216,   217,   194,   218,    46,  -504,   207,   181,   320,
     321,   277,  -504,  -504,  -504,  -504,  -504,  -504,   283,  -504,
     -22,    36,    11,    73,  -504,   -19,   329,  -504,   167,   331,
    -504,  -504,    73,  -504,  -504,  -504,   332,  -504,   333,  -504,
     270,  -504,  -504,  -504,   289,  -504,   292,  -504,   233,  -504,
      73,  -504,   295,  -504,  -504,  -504,  -504,   343,   259,  2343,
    -504,   345,   284,   284,   240,  2343,  -504,  -504,   351,   352,
     303,   306,  -504,   307,   308,  -504,  -504,  -504,  -504,   392,
    -504,   357,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,   358,   252,  -504,  -504,  -504,  -504,  -504,
    -504,    33,  -504,  -504,  -504,  -504,    22,   423,   140,  -504,
    -504,  -504,  -504,  -504,  -504,   115,  1555,  -504,   424,   115,
    -504,  -504,   361,   372,  -504,  -504,  -504,   428,     7,  -504,
     230,   274,  -504,  -504,  -504,   281,   285,  1989,   286,  1684,
    2042,    31,  2343,  -504,  -504,  -504,  -504,  -504,  2343,   355,
     175,  -504,  -504,  2166,  -504,   288,    99,    99,    99,    99,
     124,   293,   294,   296,   297,   298,   300,   304,   310,   311,
    -504,   312,  -504,    99,    99,    99,    99,    99,   313,   314,
      99,  -504,   397,   399,  -504,  -504,  -504,   416,   427,  -504,
     305,  -504,  -504,  -504,  -504,   360,    73,  -504,    73,    29,
    -504,    73,  -504,  -504,  -504,  -504,   409,  -504,    73,  -504,
    -504,  -504,  -504,  -504,  -504,  -504,   -68,   365,  -504,  -504,
     429,   315,    10,  -504,   426,  -504,  -504,   432,  -504,   317,
    -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,   327,  -504,
     323,   125,   393,  -504,  -504,  1358,  1411,  -504,  2343,  2343,
    -504,   393,    34,   436,   512,   393,   284,   441,  2042,  2042,
    2343,  2343,   334,  2042,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,   -30,   -56,  -504,   339,  2343,   341,  -504,
    -504,  -504,   346,   347,   210,    99,  -504,  -504,  -504,  -504,
    -504,  -504,   446,    99,    99,    99,    99,    99,    99,    99,
      99,    99,    99,    99,   -12,  -504,  -504,  -504,  -504,  -504,
    -504,   338,   344,  -504,  -504,  -504,  -504,   481,   181,  -504,
    -504,  -504,    73,  -504,  -504,  -504,  -504,   417,   418,   362,
     376,   466,  2343,  2343,  -504,  -504,     0,   467,  -504,  -504,
    -504,  2343,  -504,  2343,  -504,  -504,  2219,  -504,  2343,    86,
     512,  -504,   531,  -504,  -504,  -504,  -504,  -504,   363,   364,
    2042,  -504,  -504,   471,    31,  -504,  2343,   366,  -504,  -504,
    -504,   367,  -504,   369,  -504,  -504,    -7,    -6,    -2,     5,
     373,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,   359,     8,    12,    14,    15,    16,    49,
     491,    19,  -504,  -504,  -504,  -504,  -504,   482,   374,  -504,
    -504,  -504,  -504,  -504,  -504,   375,   377,  2343,  -504,    -3,
    -504,  -504,   547,  2343,  2042,  2042,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,    20,  -504,  -504,  -504,  -504,    99,  -504,
    -504,  -504,  -504,  -504,  -504,   379,   380,   381,   382,   383,
     384,   378,   385,   386,  -504,  -504,   488,  -504,  -504,  -504,
    2343,  -504,  2343,  -504,  -504,  -504,  -504,  -504,    99,    99,
     -55,   387,   495,   544,    49,    99,   545,  -504,  -504,  -504,
     394,   395,   398,  -504,  -504,   400,   402,   401,  -504,  -504,
     -23,  -504,  -504,  -504,    99,    99,  -504,   495,  -504,    99,
     403,  -504,  -504,   405,  -504,  -504
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -504,  -504,  -504,   499,   565,   -26,     3,    -8,  -504,  -504,
    -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  -504,   425,  -504,  -504,  -504,  -504,   238,  -504,  -309,
     -67,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,   497,  -504,  -504,   -20,  -150,  -295,  -504,
    -138,    -4,  -504,  -504,   107,   128,   145,   148,   161,  -504,
     208,  -504,  -460,   468,  -504,  -504,  -504,  -126,  -504,  -504,
    -148,  -504,  -466,   -43,  -430,  -503,  -409,  -504,  -504,  -504,
    -504,   368,  -504,  -504,  -504,   203,   370,  -504,  -504,  -504,
    -504,  -504,  -504,   157,   -40,   319,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,
    -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,  -504,    37,
    -504,  -504,   507,   239,  -504,  -140,  -504,    -9
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -465
static const yytype_int16 yytable[] =
{
     223,   225,   379,    98,   381,   380,   569,   548,   549,   550,
     551,   323,   234,   235,   486,   245,   245,   245,   245,   245,
     245,   245,   293,   294,   565,   566,   388,   340,   570,   652,
     311,   573,  -110,  -111,   694,   695,   592,   273,    98,   696,
     340,     3,     4,     5,     6,     7,   697,   338,    10,   700,
     641,   622,   295,   701,    24,   702,   703,   704,   616,   617,
     714,   726,   464,   621,   742,   587,   303,    24,    25,    26,
      27,    28,    29,   504,   317,   529,   179,   332,  -464,  -185,
     333,   672,   636,   637,   638,   639,   363,   441,   442,   443,
     285,   340,   305,   504,   304,   177,   306,    98,   286,   654,
     655,   656,   362,   657,   658,   231,   456,   661,   236,   342,
     296,   364,   530,   355,   289,    98,    40,   318,    24,   176,
     465,   588,   180,   289,   181,   232,   466,   444,   673,   310,
     648,   649,   531,   624,   334,   289,   635,   625,   743,   461,
      98,   271,   226,   227,   635,   642,   643,   644,   645,   646,
     647,   705,   268,   650,   651,   292,   289,   582,   623,   532,
     686,   288,   272,   289,   458,   706,   707,   708,   693,   709,
     758,   228,   298,   759,   289,   459,   274,   653,   299,   300,
     546,   289,   653,   653,   229,   547,   720,   653,   275,   339,
     721,   395,   396,   276,   653,   727,   291,   653,   366,   366,
     593,   653,   302,   653,   653,   653,   504,   614,   653,   653,
     398,   269,   270,   347,   366,   499,   501,   356,   289,   533,
    -464,  -185,   404,   341,   724,   725,   608,   407,   246,   247,
     248,   249,   250,   251,   348,   245,   308,   447,   357,   448,
     457,   460,   462,   463,   277,   278,   280,   281,   469,   282,
     283,   349,   471,   319,   350,   358,   321,   252,   359,   183,
     184,   185,   186,   187,   188,   189,   505,   351,   506,   190,
     478,   360,   191,   192,   193,   194,   195,   196,   312,   253,
     254,   255,   256,   257,   258,   259,   260,   261,   262,   635,
     263,   197,   324,   264,   314,   315,   316,   325,   198,   199,
     539,   540,   541,   382,   383,   750,   393,   394,   438,   439,
     482,   468,   289,   328,   552,   553,   488,   366,   599,   740,
     741,   326,   327,   516,   329,   330,   331,   335,   336,   763,
     353,    31,   366,   377,   389,   202,   203,   204,   631,   391,
     205,   206,   390,   207,   392,   760,   761,   208,   209,   399,
     210,   401,   402,   211,   212,   340,     3,     4,     5,     6,
       7,   400,   405,   403,   600,   406,   213,   510,   408,   500,
     214,   409,   410,   607,   215,   216,   411,   613,   217,   413,
     415,   416,    24,    25,    26,    27,    28,    29,   522,   417,
     223,   528,   418,   536,   446,   218,   419,   433,   340,   537,
     434,   452,   453,   632,   544,   454,   435,   436,   437,   440,
     467,   455,   470,   472,   473,   298,   580,   475,   581,   583,
     476,   584,   477,   479,   480,    24,   483,   481,   586,   484,
     487,   491,   489,   490,   492,   493,   494,   495,   496,   497,
     420,   498,   183,   184,   185,   186,   187,   188,   189,   502,
     511,   513,   190,   514,   515,   191,   192,   193,   194,   195,
     196,   421,   422,   423,   424,   517,   425,   426,   427,   428,
     429,   430,   518,   576,   197,   431,   519,   523,   574,   545,
     575,   198,   199,   538,   554,   555,   577,   556,   557,   558,
     585,   559,   306,   589,   578,   560,   602,   604,   590,   605,
     606,   561,   562,   563,   571,   572,   591,   594,   596,   528,
     528,   618,   619,   595,   528,   597,   598,   610,   202,   203,
     204,   611,   615,   205,   206,   620,   207,   640,   627,   626,
     208,   209,   659,   210,   628,   633,   211,   212,   660,   629,
     630,   662,   664,   723,   668,   665,   666,   669,   674,   213,
     683,   667,   687,   214,   684,   685,   712,   215,   216,   690,
     691,   217,   692,   715,   698,   716,   722,   734,   717,   737,
     718,   728,   729,   730,   731,   732,   745,   735,   218,   733,
     744,   736,   739,   670,   671,   748,   751,   752,   753,   754,
     757,   755,   675,   337,   676,   756,   764,   678,   765,   680,
     267,   387,   682,   346,   688,   681,   365,   524,   749,   762,
     699,   528,   579,   352,   412,   663,   414,   689,   512,   474,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   719,     0,
       0,     0,     0,     1,     0,   528,   528,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,     0,     0,    16,    17,    18,    19,    20,     0,     0,
      21,    22,     0,    23,    24,    25,    26,    27,    28,    29,
      30,   738,    31,     0,     0,     0,     0,     0,     0,     0,
       0,    32,    33,    34,     0,    35,    36,     0,     0,     0,
       0,     0,     0,     0,    37,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      38,    39,     0,    40,    41,    42,    43,    44,     0,    45,
       0,     0,     0,    46,    47,    48,    49,    50,    51,    52,
      53,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    54,    55,     0,     0,     0,     0,     0,
       0,    56,     0,     0,     0,     0,    57,    58,    59,     0,
       0,     0,     0,     0,    60,     0,     0,     0,    61,    62,
      63,    64,    65,     0,     0,     0,    66,    67,     0,    68,
      69,    70,     0,    71,    72,     0,     0,     0,    73,    74,
       0,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,     0,    91,    92,
       2,     3,     4,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,     0,     0,    16,    17,    18,    19,
      20,     0,     0,    21,    22,     0,    23,    24,    25,    26,
      27,    28,    29,    30,     0,    31,     0,     0,     0,     0,
       0,     0,     0,     0,    32,    33,    34,     0,    35,    36,
       0,     0,     0,     0,     0,     0,     0,    37,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    38,    39,     0,    40,    41,    42,    43,
      44,     0,    45,     0,     0,     0,    46,    47,    48,    49,
      50,    51,    52,    53,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    54,    55,     0,     0,
       0,     0,     0,     0,    56,     0,     0,     0,     0,    57,
      58,    59,     0,     0,     0,     0,     0,    60,     0,     0,
       0,    61,    62,    63,    64,    65,     0,     0,     0,    66,
      67,     0,    68,    69,    70,     0,    71,    72,     0,     0,
       0,    73,    74,     0,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
       0,    91,    92,     2,     3,     4,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,     0,     0,    16,
      17,    18,    19,    20,     0,     0,    21,    22,     0,    23,
      24,    25,    26,    27,    28,    29,    30,     0,    31,     0,
       0,     0,     0,     0,     0,     0,     0,    32,    33,    34,
       0,   266,    36,     0,     0,     0,     0,     0,     0,     0,
      37,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    38,    39,     0,    40,
      41,    42,    43,    44,     0,    45,     0,     0,     0,    46,
      47,    48,    49,    50,    51,    52,    53,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    54,
      55,     0,     0,     0,     0,     0,     0,    56,     0,     0,
       0,     0,    57,    58,    59,     0,     0,     0,     0,     0,
      60,     0,     0,     0,    61,    62,    63,    64,    65,     0,
       0,     0,    66,    67,     0,    68,    69,    70,     0,    71,
      72,     0,     0,     0,    73,    74,     0,    75,    76,    77,
      78,    79,    80,    81,    82,    83,    84,    85,    86,    87,
      88,    89,    90,     0,    91,     2,     3,     4,     5,     6,
       7,     8,     9,    10,    11,    12,    13,    14,    15,     0,
       0,    16,    17,    18,    19,    20,     0,     0,    21,    22,
       0,    23,    24,    25,    26,    27,    28,    29,    30,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    32,
      33,    34,     0,   266,    36,     0,     0,     0,     0,     0,
       0,     0,    37,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    38,    39,
       0,    40,    41,    42,    43,    44,     0,    45,     0,     0,
       0,    46,    47,    48,    49,    50,    51,    52,    53,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    54,    55,     0,     0,     0,     0,     0,     0,    56,
       0,     0,     0,     0,    57,    58,    59,     0,     0,     0,
       0,     0,    60,     0,     0,     0,    61,    62,    63,    64,
      65,     0,     0,     0,    66,    67,     0,    68,    69,    70,
       0,    71,    72,     0,     0,     0,    73,    74,     0,    75,
      76,    77,    78,    79,    80,    81,    82,    83,    84,    85,
      86,    87,    88,    89,    90,     0,    91,   183,   184,   185,
     186,   187,   188,   189,     0,     0,     0,   190,     0,     0,
     191,   192,   193,   194,   195,   196,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   197,
       0,     0,     0,     0,     0,     0,   198,   199,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     183,   184,   185,   186,   187,   188,   189,     0,     0,     0,
     190,     0,     0,   191,   192,   193,   194,   195,   196,     0,
       0,     0,     0,   202,   203,   204,     0,     0,   205,   206,
       0,   207,   197,     0,     0,   208,   209,     0,   210,   198,
     199,   211,   212,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   213,     0,     0,     0,   214,     0,
       0,     0,   215,   216,     0,     0,   217,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   202,   203,   204,     0,
       0,   205,   206,   218,   207,     0,     0,     0,   208,   209,
     601,   210,     0,     0,   211,   212,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   213,     0,     0,
       0,   214,     0,     0,     0,   215,   216,     0,     0,   217,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   218,     0,     0,     0,
       0,     0,     0,   603,   183,   184,   185,   186,   187,   188,
     189,     0,     0,     0,   190,     0,     0,   191,   192,   193,
     194,   195,   196,   508,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   197,     0,     0,     0,
       0,     0,     0,   198,   199,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     202,   203,   204,     0,     0,   205,   206,     0,   207,     0,
       0,     0,   208,   209,     0,   210,     0,     0,   211,   212,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   213,     0,     0,     0,   214,     0,     0,     0,   215,
     216,     0,     0,   217,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   183,   184,   185,   186,   187,   188,   189,
     218,     0,     0,   190,     0,   509,   191,   192,   193,   194,
     195,   196,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   197,     0,     0,     0,     0,
       0,     0,   198,   199,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   200,   201,     0,   183,   184,   185,   186,
     187,   188,   189,     0,     0,     0,   190,     0,     0,   191,
     192,   193,   194,   195,   196,     0,     0,     0,     0,   202,
     203,   204,     0,     0,   205,   206,     0,   207,   197,     0,
       0,   208,   209,     0,   210,   198,   199,   211,   212,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     213,     0,     0,     0,   214,     0,     0,     0,   215,   216,
       0,     0,   217,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   202,   203,   204,     0,     0,   205,   206,   218,
     207,     0,   219,     0,   208,   209,     0,   210,     0,     0,
     211,   212,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   213,     0,     0,     0,   214,     0,     0,
       0,   215,   216,     0,     0,   217,     0,     0,     0,     0,
       0,     0,     0,     0,   183,   184,   185,   186,   187,   188,
     189,     0,   218,     0,   190,   397,   237,   191,   192,   193,
     194,   195,   196,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   197,     0,     0,     0,
       0,     0,     0,   198,   199,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   238,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   239,   240,
     202,   203,   204,   241,     0,   205,   206,     0,   207,     0,
       0,     0,   208,   209,     0,   210,     0,     0,   211,   212,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   213,     0,     0,     0,   214,     0,     0,     0,   215,
     216,     0,     0,   217,     0,     0,     0,     0,   183,   184,
     185,   186,   187,   188,   189,     0,     0,     0,   190,     0,
     218,   191,   192,   193,   194,   195,   196,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     197,     0,     0,     0,     0,     0,     0,   198,   199,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   520,   521,
       0,   183,   184,   185,   186,   187,   188,   189,     0,     0,
       0,   190,     0,     0,   191,   192,   193,   194,   195,   196,
       0,     0,     0,     0,   202,   203,   204,     0,     0,   205,
     206,     0,   207,   197,     0,     0,   208,   209,     0,   210,
     198,   199,   211,   212,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   525,     0,   213,     0,     0,     0,   214,
       0,     0,     0,   215,   216,     0,     0,   217,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   202,   203,   204,
     526,     0,   205,   206,   218,   207,     0,     0,     0,   208,
     209,     0,   210,     0,     0,   211,   212,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   213,     0,
       0,     0,   214,     0,     0,     0,   215,   216,     0,     0,
     217,     0,     0,     0,     0,   183,   184,   185,   186,   187,
     188,   189,     0,     0,     0,   190,     0,   218,   191,   192,
     193,   194,   195,   196,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   197,     0,     0,
       0,     0,     0,     0,   198,   199,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   183,   184,
     185,   186,   187,   188,   189,     0,     0,     0,   190,     0,
       0,   191,   192,   193,   194,   195,   196,   677,     0,     0,
       0,   202,   203,   204,   543,     0,   205,   206,     0,   207,
     197,     0,     0,   208,   209,     0,   210,   198,   199,   211,
     212,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   213,     0,     0,     0,   214,     0,     0,     0,
     215,   216,     0,     0,   217,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   202,   203,   204,     0,     0,   205,
     206,   218,   207,     0,     0,     0,   208,   209,     0,   210,
       0,     0,   211,   212,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   213,     0,     0,     0,   214,
       0,     0,     0,   215,   216,     0,     0,   217,     0,     0,
       0,     0,   183,   184,   185,   186,   187,   188,   189,     0,
       0,     0,   190,     0,   218,   191,   192,   193,   194,   195,
     196,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   197,     0,     0,     0,     0,     0,
       0,   198,   199,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   183,   184,   185,   186,   187,
     188,   189,     0,     0,     0,   190,     0,     0,   191,   192,
     193,   194,   195,   196,     0,     0,     0,     0,   202,   203,
     204,     0,     0,   205,   206,     0,   207,   361,     0,     0,
     208,   209,     0,   210,   198,   199,   211,   212,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   213,
       0,     0,     0,   214,     0,     0,     0,   215,   216,     0,
       0,   217,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   202,   203,   204,     0,     0,   205,   206,   218,   207,
       0,     0,     0,   208,   209,     0,   210,     0,     0,   211,
     212,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   213,     0,     0,     0,   214,     0,     0,     0,
     215,   216,     0,     0,   217,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   218
};

static const yytype_int16 yycheck[] =
{
       9,    10,   142,     0,   144,   143,   436,   416,   417,   418,
     419,    78,    21,    22,   323,    24,    25,    26,    27,    28,
      29,    30,    62,    63,   433,   434,   176,     5,   437,    41,
      70,   440,    26,    26,    41,    41,    26,    46,    35,    41,
       5,     6,     7,     8,     9,    10,    41,    45,    13,    41,
     553,    81,    81,    41,    32,    41,    41,    41,   518,   519,
      41,    41,    81,   523,   119,   133,   128,    32,    33,    34,
      35,    36,    37,   368,    26,    44,    19,   128,    45,    45,
     131,    81,   548,   549,   550,   551,   112,    41,    42,    43,
     137,     5,   128,   388,   156,   139,   132,    94,   145,   565,
     566,   567,   111,   569,   570,    28,   128,   573,    28,   106,
     139,   137,    81,   110,   145,   112,    81,    69,    32,    77,
     139,   189,    65,   145,    67,    48,   145,    81,   128,   160,
     560,   561,   101,   189,   185,   145,   545,   193,   193,   128,
     137,   191,    19,    20,   553,   554,   555,   556,   557,   558,
     559,   102,    55,   562,   563,   165,   145,   128,   188,   128,
     620,   143,    88,   145,   128,   116,   117,   118,   634,   120,
     193,    48,   145,   196,   145,   139,    81,   189,   151,   152,
      81,   145,   189,   189,    61,    86,   189,   189,   128,   187,
     193,   200,   201,   128,   189,   698,   128,   189,   192,   192,
     190,   189,    81,   189,   189,   189,   501,   516,   189,   189,
     219,    53,    54,   106,   192,   193,   366,   110,   145,   188,
     187,   187,   231,   188,   684,   685,   192,   236,    25,    26,
      27,    28,    29,    30,   106,   244,   128,    56,   110,    58,
     280,   281,   282,   283,   130,   131,   137,   138,   288,   140,
     141,   106,   292,    26,   106,   110,    26,    80,   110,    49,
      50,    51,    52,    53,    54,    55,   151,   106,   153,    59,
     310,   110,    62,    63,    64,    65,    66,    67,   128,   102,
     103,   104,   105,   106,   107,   108,   109,   110,   111,   698,
     113,    81,    81,   116,   163,   164,   165,    68,    88,    89,
     125,   126,   127,    89,    90,   735,    63,    64,   114,   115,
     319,   144,   145,    12,   190,   191,   325,   192,   193,   728,
     729,    83,    84,   390,     6,   128,    81,    45,     0,   759,
     188,    40,   192,    66,    47,   125,   126,   127,   128,    81,
     130,   131,    52,   133,   197,   754,   755,   137,   138,   189,
     140,   192,   190,   143,   144,     5,     6,     7,     8,     9,
      10,   191,    26,    61,   502,    30,   156,   376,   192,   366,
     160,   192,   192,   511,   164,   165,    39,   515,   168,   192,
      81,   191,    32,    33,    34,    35,    36,    37,   397,   191,
     399,   400,   191,   402,   187,   185,   191,   191,     5,   408,
     191,    81,    81,   193,   413,   128,   191,   191,   191,   191,
      81,   128,    81,    81,    81,   145,   456,   128,   458,   459,
     128,   461,   189,   128,    81,    32,    81,   168,   468,   145,
     190,   128,    81,    81,   128,   128,   128,    45,    81,    81,
      81,   189,    49,    50,    51,    52,    53,    54,    55,    26,
      26,    90,    59,    81,    26,    62,    63,    64,    65,    66,
      67,   102,   103,   104,   105,   191,   107,   108,   109,   110,
     111,   112,   191,    57,    81,   116,   191,   191,    81,   191,
      81,    88,    89,   128,   191,   191,    59,   191,   191,   191,
      81,   191,   132,   128,   189,   191,   505,   506,    69,   508,
     509,   191,   191,   191,   191,   191,   191,    81,   191,   518,
     519,   520,   521,    81,   523,   188,   193,    81,   125,   126,
     127,     9,    81,   130,   131,   191,   133,    81,   537,   190,
     137,   138,   194,   140,   193,   544,   143,   144,   194,   193,
     193,    60,   582,   683,   168,   128,   128,    81,    81,   156,
      19,   189,    81,   160,   191,   191,    65,   164,   165,   193,
     193,   168,   193,    81,   191,   191,    19,   189,   193,    81,
     193,   192,   192,   192,   192,   192,    81,   192,   185,   195,
     193,   195,   722,   592,   593,    41,    41,   193,   193,   191,
     189,   191,   601,    94,   603,   193,   193,   606,   193,   608,
      35,   176,   610,   106,   624,   609,   138,   399,   734,   757,
     653,   620,   455,   106,   244,   578,   248,   626,   379,   300,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   677,    -1,
      -1,    -1,    -1,     1,    -1,   684,   685,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,    16,    17,
      18,    -1,    -1,    21,    22,    23,    24,    25,    -1,    -1,
      28,    29,    -1,    31,    32,    33,    34,    35,    36,    37,
      38,   720,    40,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    49,    50,    51,    -1,    53,    54,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    62,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      78,    79,    -1,    81,    82,    83,    84,    85,    -1,    87,
      -1,    -1,    -1,    91,    92,    93,    94,    95,    96,    97,
      98,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   121,   122,    -1,    -1,    -1,    -1,    -1,
      -1,   129,    -1,    -1,    -1,    -1,   134,   135,   136,    -1,
      -1,    -1,    -1,    -1,   142,    -1,    -1,    -1,   146,   147,
     148,   149,   150,    -1,    -1,    -1,   154,   155,    -1,   157,
     158,   159,    -1,   161,   162,    -1,    -1,    -1,   166,   167,
      -1,   169,   170,   171,   172,   173,   174,   175,   176,   177,
     178,   179,   180,   181,   182,   183,   184,    -1,   186,   187,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    -1,    -1,    21,    22,    23,    24,
      25,    -1,    -1,    28,    29,    -1,    31,    32,    33,    34,
      35,    36,    37,    38,    -1,    40,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    49,    50,    51,    -1,    53,    54,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    62,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    78,    79,    -1,    81,    82,    83,    84,
      85,    -1,    87,    -1,    -1,    -1,    91,    92,    93,    94,
      95,    96,    97,    98,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   121,   122,    -1,    -1,
      -1,    -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,
     135,   136,    -1,    -1,    -1,    -1,    -1,   142,    -1,    -1,
      -1,   146,   147,   148,   149,   150,    -1,    -1,    -1,   154,
     155,    -1,   157,   158,   159,    -1,   161,   162,    -1,    -1,
      -1,   166,   167,    -1,   169,   170,   171,   172,   173,   174,
     175,   176,   177,   178,   179,   180,   181,   182,   183,   184,
      -1,   186,   187,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,    16,    17,    18,    -1,    -1,    21,
      22,    23,    24,    25,    -1,    -1,    28,    29,    -1,    31,
      32,    33,    34,    35,    36,    37,    38,    -1,    40,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    49,    50,    51,
      -1,    53,    54,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      62,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    78,    79,    -1,    81,
      82,    83,    84,    85,    -1,    87,    -1,    -1,    -1,    91,
      92,    93,    94,    95,    96,    97,    98,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   121,
     122,    -1,    -1,    -1,    -1,    -1,    -1,   129,    -1,    -1,
      -1,    -1,   134,   135,   136,    -1,    -1,    -1,    -1,    -1,
     142,    -1,    -1,    -1,   146,   147,   148,   149,   150,    -1,
      -1,    -1,   154,   155,    -1,   157,   158,   159,    -1,   161,
     162,    -1,    -1,    -1,   166,   167,    -1,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,   180,   181,
     182,   183,   184,    -1,   186,     5,     6,     7,     8,     9,
      10,    11,    12,    13,    14,    15,    16,    17,    18,    -1,
      -1,    21,    22,    23,    24,    25,    -1,    -1,    28,    29,
      -1,    31,    32,    33,    34,    35,    36,    37,    38,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    49,
      50,    51,    -1,    53,    54,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    62,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    78,    79,
      -1,    81,    82,    83,    84,    85,    -1,    87,    -1,    -1,
      -1,    91,    92,    93,    94,    95,    96,    97,    98,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   121,   122,    -1,    -1,    -1,    -1,    -1,    -1,   129,
      -1,    -1,    -1,    -1,   134,   135,   136,    -1,    -1,    -1,
      -1,    -1,   142,    -1,    -1,    -1,   146,   147,   148,   149,
     150,    -1,    -1,    -1,   154,   155,    -1,   157,   158,   159,
      -1,   161,   162,    -1,    -1,    -1,   166,   167,    -1,   169,
     170,   171,   172,   173,   174,   175,   176,   177,   178,   179,
     180,   181,   182,   183,   184,    -1,   186,    49,    50,    51,
      52,    53,    54,    55,    -1,    -1,    -1,    59,    -1,    -1,
      62,    63,    64,    65,    66,    67,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    81,
      -1,    -1,    -1,    -1,    -1,    -1,    88,    89,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      49,    50,    51,    52,    53,    54,    55,    -1,    -1,    -1,
      59,    -1,    -1,    62,    63,    64,    65,    66,    67,    -1,
      -1,    -1,    -1,   125,   126,   127,    -1,    -1,   130,   131,
      -1,   133,    81,    -1,    -1,   137,   138,    -1,   140,    88,
      89,   143,   144,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   156,    -1,    -1,    -1,   160,    -1,
      -1,    -1,   164,   165,    -1,    -1,   168,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   125,   126,   127,    -1,
      -1,   130,   131,   185,   133,    -1,    -1,    -1,   137,   138,
     192,   140,    -1,    -1,   143,   144,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   156,    -1,    -1,
      -1,   160,    -1,    -1,    -1,   164,   165,    -1,    -1,   168,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   185,    -1,    -1,    -1,
      -1,    -1,    -1,   192,    49,    50,    51,    52,    53,    54,
      55,    -1,    -1,    -1,    59,    -1,    -1,    62,    63,    64,
      65,    66,    67,    68,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    81,    -1,    -1,    -1,
      -1,    -1,    -1,    88,    89,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     125,   126,   127,    -1,    -1,   130,   131,    -1,   133,    -1,
      -1,    -1,   137,   138,    -1,   140,    -1,    -1,   143,   144,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   156,    -1,    -1,    -1,   160,    -1,    -1,    -1,   164,
     165,    -1,    -1,   168,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    49,    50,    51,    52,    53,    54,    55,
     185,    -1,    -1,    59,    -1,   190,    62,    63,    64,    65,
      66,    67,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    81,    -1,    -1,    -1,    -1,
      -1,    -1,    88,    89,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    99,   100,    -1,    49,    50,    51,    52,
      53,    54,    55,    -1,    -1,    -1,    59,    -1,    -1,    62,
      63,    64,    65,    66,    67,    -1,    -1,    -1,    -1,   125,
     126,   127,    -1,    -1,   130,   131,    -1,   133,    81,    -1,
      -1,   137,   138,    -1,   140,    88,    89,   143,   144,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     156,    -1,    -1,    -1,   160,    -1,    -1,    -1,   164,   165,
      -1,    -1,   168,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   125,   126,   127,    -1,    -1,   130,   131,   185,
     133,    -1,   188,    -1,   137,   138,    -1,   140,    -1,    -1,
     143,   144,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   156,    -1,    -1,    -1,   160,    -1,    -1,
      -1,   164,   165,    -1,    -1,   168,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    49,    50,    51,    52,    53,    54,
      55,    -1,   185,    -1,    59,   188,    61,    62,    63,    64,
      65,    66,    67,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    81,    -1,    -1,    -1,
      -1,    -1,    -1,    88,    89,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   112,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   123,   124,
     125,   126,   127,   128,    -1,   130,   131,    -1,   133,    -1,
      -1,    -1,   137,   138,    -1,   140,    -1,    -1,   143,   144,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   156,    -1,    -1,    -1,   160,    -1,    -1,    -1,   164,
     165,    -1,    -1,   168,    -1,    -1,    -1,    -1,    49,    50,
      51,    52,    53,    54,    55,    -1,    -1,    -1,    59,    -1,
     185,    62,    63,    64,    65,    66,    67,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      81,    -1,    -1,    -1,    -1,    -1,    -1,    88,    89,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    99,   100,
      -1,    49,    50,    51,    52,    53,    54,    55,    -1,    -1,
      -1,    59,    -1,    -1,    62,    63,    64,    65,    66,    67,
      -1,    -1,    -1,    -1,   125,   126,   127,    -1,    -1,   130,
     131,    -1,   133,    81,    -1,    -1,   137,   138,    -1,   140,
      88,    89,   143,   144,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   101,    -1,   156,    -1,    -1,    -1,   160,
      -1,    -1,    -1,   164,   165,    -1,    -1,   168,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   125,   126,   127,
     128,    -1,   130,   131,   185,   133,    -1,    -1,    -1,   137,
     138,    -1,   140,    -1,    -1,   143,   144,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   156,    -1,
      -1,    -1,   160,    -1,    -1,    -1,   164,   165,    -1,    -1,
     168,    -1,    -1,    -1,    -1,    49,    50,    51,    52,    53,
      54,    55,    -1,    -1,    -1,    59,    -1,   185,    62,    63,
      64,    65,    66,    67,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    81,    -1,    -1,
      -1,    -1,    -1,    -1,    88,    89,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    49,    50,
      51,    52,    53,    54,    55,    -1,    -1,    -1,    59,    -1,
      -1,    62,    63,    64,    65,    66,    67,    68,    -1,    -1,
      -1,   125,   126,   127,   128,    -1,   130,   131,    -1,   133,
      81,    -1,    -1,   137,   138,    -1,   140,    88,    89,   143,
     144,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   156,    -1,    -1,    -1,   160,    -1,    -1,    -1,
     164,   165,    -1,    -1,   168,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   125,   126,   127,    -1,    -1,   130,
     131,   185,   133,    -1,    -1,    -1,   137,   138,    -1,   140,
      -1,    -1,   143,   144,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   156,    -1,    -1,    -1,   160,
      -1,    -1,    -1,   164,   165,    -1,    -1,   168,    -1,    -1,
      -1,    -1,    49,    50,    51,    52,    53,    54,    55,    -1,
      -1,    -1,    59,    -1,   185,    62,    63,    64,    65,    66,
      67,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    81,    -1,    -1,    -1,    -1,    -1,
      -1,    88,    89,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    49,    50,    51,    52,    53,
      54,    55,    -1,    -1,    -1,    59,    -1,    -1,    62,    63,
      64,    65,    66,    67,    -1,    -1,    -1,    -1,   125,   126,
     127,    -1,    -1,   130,   131,    -1,   133,    81,    -1,    -1,
     137,   138,    -1,   140,    88,    89,   143,   144,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   156,
      -1,    -1,    -1,   160,    -1,    -1,    -1,   164,   165,    -1,
      -1,   168,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   125,   126,   127,    -1,    -1,   130,   131,   185,   133,
      -1,    -1,    -1,   137,   138,    -1,   140,    -1,    -1,   143,
     144,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   156,    -1,    -1,    -1,   160,    -1,    -1,    -1,
     164,   165,    -1,    -1,   168,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   185
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     1,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    21,    22,    23,    24,
      25,    28,    29,    31,    32,    33,    34,    35,    36,    37,
      38,    40,    49,    50,    51,    53,    54,    62,    78,    79,
      81,    82,    83,    84,    85,    87,    91,    92,    93,    94,
      95,    96,    97,    98,   121,   122,   129,   134,   135,   136,
     142,   146,   147,   148,   149,   150,   154,   155,   157,   158,
     159,   161,   162,   166,   167,   169,   170,   171,   172,   173,
     174,   175,   176,   177,   178,   179,   180,   181,   182,   183,
     184,   186,   187,   199,   200,   201,   202,   203,   204,   209,
     210,   211,   212,   213,   216,   217,   222,   223,   224,   225,
     226,   227,   228,   231,   232,   233,   234,   236,   239,   240,
     241,   242,   243,   244,   245,   246,   247,   248,   249,   250,
     251,   262,   263,   264,   265,   266,   267,   271,   272,   285,
     286,   287,   288,   290,   291,   292,   298,   299,   300,   304,
     306,   307,   308,   310,   312,   313,   314,   315,   317,   318,
     319,   320,   321,   323,   324,   326,   327,   328,   329,   330,
     331,   332,   334,   338,   339,   340,    77,   139,   289,    19,
      65,    67,   333,    49,    50,    51,    52,    53,    54,    55,
      59,    62,    63,    64,    65,    66,    67,    81,    88,    89,
      99,   100,   125,   126,   127,   130,   131,   133,   137,   138,
     140,   143,   144,   156,   160,   164,   165,   168,   185,   188,
     268,   269,   344,   345,   252,   345,    19,    20,    48,    61,
     214,    28,    48,   215,   345,   345,    28,    61,   112,   123,
     124,   128,   293,   294,   295,   345,   293,   293,   293,   293,
     293,   293,    80,   102,   103,   104,   105,   106,   107,   108,
     109,   110,   111,   113,   116,   273,    53,   202,    55,    53,
      54,   191,    88,   345,    81,   128,   128,   130,   131,   305,
     137,   138,   140,   141,   311,   137,   145,   309,   143,   145,
     302,   128,   165,   302,   302,    81,   139,   316,   145,   151,
     152,   303,    81,   128,   156,   128,   132,   301,   128,   322,
     160,   302,   128,   325,   163,   164,   165,    26,    69,    26,
     235,    26,   238,   238,    81,    68,   235,   235,    12,     6,
     128,    81,   128,   131,   185,    45,     0,   201,    45,   187,
       5,   188,   204,   218,   220,   221,   251,   262,   263,   264,
     265,   266,   340,   188,   219,   204,   262,   263,   264,   265,
     266,    81,   345,   203,   203,   271,   192,   230,   255,   256,
     257,   204,   258,   259,   342,   343,   345,    66,   297,   343,
     258,   343,    89,    90,   206,   207,   208,   230,   255,    47,
      52,    81,   197,    63,    64,   345,   345,   188,   345,   189,
     191,   192,   190,    61,   345,    26,    30,   345,   192,   192,
     192,    39,   294,   192,   289,    81,   191,   191,   191,   191,
      81,   102,   103,   104,   105,   107,   108,   109,   110,   111,
     112,   116,   281,   191,   191,   191,   191,   191,   114,   115,
     191,    41,    42,    43,    81,   274,   187,    56,    58,   335,
     336,   337,    81,    81,   128,   128,   128,   302,   128,   139,
     302,   128,   302,   302,    81,   139,   145,    81,   144,   302,
      81,   302,    81,    81,   303,   128,   128,   189,   302,   128,
      81,   168,   345,    81,   145,   237,   237,   190,   345,    81,
      81,   128,   128,   128,   128,    45,    81,    81,   189,   193,
     204,   255,    26,   229,   256,   151,   153,   341,    68,   190,
     345,    26,   341,    90,    81,    26,   238,   191,   191,   191,
      99,   100,   345,   191,   268,   101,   128,   270,   345,    44,
      81,   101,   128,   188,   253,   254,   345,   345,   128,   125,
     126,   127,   296,   128,   345,   191,    81,    86,   284,   284,
     284,   284,   190,   191,   191,   191,   191,   191,   191,   191,
     191,   191,   191,   191,   280,   284,   284,   282,   284,   282,
     284,   191,   191,   284,    81,    81,    57,    59,   189,   301,
     302,   302,   128,   302,   302,    81,   302,   133,   189,   128,
      69,   191,    26,   190,    81,    81,   191,   188,   193,   193,
     258,   192,   345,   192,   345,   345,   345,   258,   192,   260,
      81,     9,   205,   258,   237,    81,   270,   270,   345,   345,
     191,   270,    81,   188,   189,   193,   190,   345,   193,   193,
     193,   128,   193,   345,   283,   284,   280,   280,   280,   280,
      81,   283,   284,   284,   284,   284,   284,   284,   282,   282,
     284,   284,    41,   189,   280,   280,   280,   280,   280,   194,
     194,   280,    60,   337,   302,   128,   128,   189,   168,    81,
     345,   345,    81,   128,    81,   345,   345,    68,   345,   261,
     345,   259,   205,    19,   191,   191,   270,    81,   254,   345,
     193,   193,   193,   280,    41,    41,    41,    41,   191,   281,
      41,    41,    41,    41,    41,   102,   116,   117,   118,   120,
     275,   276,    65,   277,    41,    81,   191,   193,   193,   345,
     189,   193,    19,   343,   270,   270,    41,   283,   192,   192,
     192,   192,   192,   195,   189,   192,   195,    81,   345,   343,
     284,   284,   119,   193,   193,    81,   278,   279,    41,   275,
     282,    41,   193,   193,   191,   191,   193,   189,   193,   196,
     284,   284,   278,   282,   193,   193
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

    {
              result->cur_stmt_type_ = OBPROXY_T_SELECT;
            ;}
    break;

  case 44:

    {
              result->cur_stmt_type_ = OBPROXY_T_LOAD_DATA_INFILE;
            ;}
    break;

  case 45:

    {
              result->cur_stmt_type_ = OBPROXY_T_LOAD_DATA_LOCAL_INFILE;
            ;}
    break;

  case 58:

    { result->cur_stmt_type_ = OBPROXY_T_CREATE; ;}
    break;

  case 59:

    { result->cur_stmt_type_ = OBPROXY_T_DROP; ;}
    break;

  case 60:

    { result->cur_stmt_type_ = OBPROXY_T_ALTER; ;}
    break;

  case 61:

    { result->cur_stmt_type_ = OBPROXY_T_TRUNCATE; ;}
    break;

  case 62:

    { result->cur_stmt_type_ = OBPROXY_T_RENAME; ;}
    break;

  case 63:

    {;}
    break;

  case 64:

    {;}
    break;

  case 66:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_TABLE; ;}
    break;

  case 67:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 68:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 69:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_TABLEGROUP; ;}
    break;

  case 71:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_DROP_TABLEGROUP; ;}
    break;

  case 72:

    {
            SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num));
            result->cur_stmt_type_ = OBPROXY_T_STOP_DDL_TASK;
          ;}
    break;

  case 73:

    {
            SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num));
            result->cur_stmt_type_ = OBPROXY_T_RETRY_DDL_TASK;
          ;}
    break;

  case 74:

    {;}
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

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(2) - (2)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 83:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(4) - (4)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 84:

    {
                          ObProxyTextPsParseNode *node = NULL;
                          malloc_parse_node(node);
                          node->str_value_ = (yyvsp[(2) - (2)].str);
                          add_text_ps_node(result->text_ps_parse_info_, node);
                        ;}
    break;

  case 87:

    {
                      result->text_ps_inner_stmt_type_ = OBPROXY_T_TEXT_PS_PREPARE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 88:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (2)].str);
                    ;}
    break;

  case 89:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 90:

    {
            ;}
    break;

  case 91:

    {
            ;}
    break;

  case 92:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 93:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 94:

    { result->cur_stmt_type_ = OBPROXY_T_GRANT; ;}
    break;

  case 95:

    { result->cur_stmt_type_ = OBPROXY_T_REVOKE; ;}
    break;

  case 96:

    { result->cur_stmt_type_ = OBPROXY_T_ANALYZE; ;}
    break;

  case 97:

    { result->cur_stmt_type_ = OBPROXY_T_PURGE; ;}
    break;

  case 98:

    { result->cur_stmt_type_ = OBPROXY_T_FLASHBACK; ;}
    break;

  case 99:

    { result->cur_stmt_type_ = OBPROXY_T_COMMENT; ;}
    break;

  case 100:

    { result->cur_stmt_type_ = OBPROXY_T_AUDIT; ;}
    break;

  case 101:

    { result->cur_stmt_type_ = OBPROXY_T_NOAUDIT; ;}
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

  case 112:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_TX_RO; ;}
    break;

  case 116:

    { result->col_name_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 117:

    {;}
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

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY; ;}
    break;

  case 124:

    {;}
    break;

  case 125:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_DATABASE; ;}
    break;

  case 126:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_PROXY_STATUS; ;}
    break;

  case 127:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DATABASES; ;}
    break;

  case 128:

    {;}
    break;

  case 129:

    {;}
    break;

  case 130:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_COLUMNS; ;}
    break;

  case 131:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_INDEX; ;}
    break;

  case 132:

    {;}
    break;

  case 133:

    {
                      result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
                      result->cur_stmt_type_ = OBPROXY_T_DESC;
                      result->sub_stmt_type_ = OBPROXY_T_SUB_DESC_TABLE;
                  ;}
    break;

  case 134:

    {
            result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
          ;}
    break;

  case 135:

    {
            result->table_info_.table_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.database_name_ = (yyvsp[(4) - (4)].str);
          ;}
    break;

  case 136:

    {
            result->table_info_.database_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.table_name_ = (yyvsp[(4) - (4)].str);
          ;}
    break;

  case 137:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE;
                        result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
                      ;}
    break;

  case 138:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE;
                        result->table_info_.database_name_ = (yyvsp[(2) - (4)].str);
                        result->table_info_.table_name_ = (yyvsp[(4) - (4)].str);
                      ;}
    break;

  case 139:

    {;}
    break;

  case 140:

    { result->table_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 141:

    {;}
    break;

  case 142:

    { result->table_info_.database_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 143:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLES;
                ;}
    break;

  case 144:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_FULL_TABLES;
                ;}
    break;

  case 145:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLE_STATUS;
                      ;}
    break;

  case 146:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DB_VERSION; ;}
    break;

  case 147:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID; ;}
    break;

  case 148:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 149:

    {
                     SET_ICMD_SECOND_STRING((yyvsp[(5) - (5)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 150:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (7)].str));
                     SET_ICMD_SECOND_STRING((yyvsp[(7) - (7)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 151:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_ROUTE_ADDR; ;}
    break;

  case 152:

    {
                              result->cur_stmt_type_ = OBPROXY_T_SET_ROUTE_ADDR;
                              result->cmd_info_.integer_[0] = (yyvsp[(3) - (3)].num);
                           ;}
    break;

  case 153:

    {;}
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

  case 160:

    {
                   result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                 ;}
    break;

  case 161:

    {
                   result->table_info_.package_name_ = (yyvsp[(1) - (3)].str);
                   result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                 ;}
    break;

  case 162:

    {
                   result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                   result->table_info_.package_name_ = (yyvsp[(3) - (5)].str);
                   result->table_info_.table_name_ = (yyvsp[(5) - (5)].str);
                 ;}
    break;

  case 163:

    {
                result->call_parse_info_.node_count_ = 0;
              ;}
    break;

  case 164:

    {
                result->call_parse_info_.node_count_ = 0;
                add_call_node(result->call_parse_info_, (yyvsp[(1) - (1)].node));
              ;}
    break;

  case 165:

    {
                add_call_node(result->call_parse_info_, (yyvsp[(3) - (3)].node));
              ;}
    break;

  case 166:

    {
            malloc_call_node((yyval.node), CALL_TOKEN_STR_VAL);
            (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 167:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_INT_VAL);
           (yyval.node)->int_value_ = (yyvsp[(1) - (1)].num);
         ;}
    break;

  case 168:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_NUMBER_VAL);
           (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 169:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_USER_VAR);
           (yyval.node)->str_value_ = (yyvsp[(2) - (2)].str);
         ;}
    break;

  case 170:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_SYS_VAR);
           (yyval.node)->str_value_ = (yyvsp[(3) - (3)].str);
         ;}
    break;

  case 171:

    {
           result->placeholder_list_idx_++;
           malloc_call_node((yyval.node), CALL_TOKEN_PLACE_HOLDER);
           (yyval.node)->placeholder_idx_ = result->placeholder_list_idx_ - 1;
         ;}
    break;

  case 185:

    {
                                                                  handle_stmt_end(result);
                                                                  HANDLE_ACCEPT();
                                                                ;}
    break;

  case 190:

    {
                                                 handle_stmt_end(result);
                                                 HANDLE_ACCEPT();
                                               ;}
    break;

  case 194:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_USER);
        ;}
    break;

  case 195:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 196:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 197:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(5) - (5)].var_node), (yyvsp[(3) - (5)].str), SET_VAR_SYS);
        ;}
    break;

  case 198:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
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
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_STR);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 202:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_INT);
               (yyval.var_node)->int_value_ = (yyvsp[(1) - (1)].num);
             ;}
    break;

  case 203:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_NUMBER);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 206:

    {;}
    break;

  case 207:

    {;}
    break;

  case 208:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 209:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 210:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 211:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 212:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 213:

    {
              malloc_shard_column_node((yyval.shard_node), (yyvsp[(2) - (7)].str), (yyvsp[(3) - (7)].str), DBMESH_TOKEN_STR_VAL);
              (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (7)].str);
              add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
            ;}
    break;

  case 214:

    { result->trace_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 215:

    { result->rpc_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 216:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 217:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 218:

    {;}
    break;

  case 219:

    {;}
    break;

  case 220:

    { result->target_db_server_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 221:

    {;}
    break;

  case 223:

    { result->has_simple_route_info_ = true; result->simple_route_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 224:

    { result->simple_route_info_.part_key_ = (yyvsp[(2) - (2)].str); ;}
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

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 256:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 280:

    {;}
    break;

  case 281:

    {;}
    break;

  case 282:

    {;}
    break;

  case 283:

    {;}
    break;

  case 284:

    {;}
    break;

  case 285:

    { result->query_timeout_ = (yyvsp[(3) - (4)].num); ;}
    break;

  case 286:

    {;}
    break;

  case 288:

    {
      add_hint_index(result->dbmesh_route_info_, (yyvsp[(3) - (5)].str));
      result->dbmesh_route_info_.index_count_++;
    ;}
    break;

  case 289:

    { result->has_trace_log_hint_ = true; ;}
    break;

  case 290:

    {;}
    break;

  case 291:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_WEAK); ;}
    break;

  case 292:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_STRONG); ;}
    break;

  case 293:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_FROZEN); ;}
    break;

  case 296:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_WARNINGS; ;}
    break;

  case 297:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_ERRORS; ;}
    break;

  case 298:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 299:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 323:

    {;}
    break;

  case 324:

    {
;}
    break;

  case 325:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 326:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 327:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 328:

    {;}
    break;

  case 329:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 330:

    {;}
    break;

  case 331:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 333:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 334:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 335:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 336:

    {;}
    break;

  case 337:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 338:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 339:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 341:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 342:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 343:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 344:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 345:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 347:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 348:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 349:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 350:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 351:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 352:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 353:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 354:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 355:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 356:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_READ_STALE); ;}
    break;

  case 357:

    {;}
    break;

  case 358:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 359:

    {;}
    break;

  case 360:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 361:

    {;}
    break;

  case 363:

    {;}
    break;

  case 364:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 365:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 366:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 367:

    {;}
    break;

  case 368:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 369:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 370:

    {;}
    break;

  case 371:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 372:

    {;}
    break;

  case 373:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 374:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 375:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); SET_ICMD_ONE_ID((yyvsp[(3) - (3)].num)); ;}
    break;

  case 376:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 377:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 379:

    {;}
    break;

  case 380:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 381:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 382:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 383:

    {;}
    break;

  case 384:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 386:

    {;}
    break;

  case 387:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 388:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 389:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 390:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 391:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 392:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 393:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 394:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 395:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 396:

    {;}
    break;

  case 397:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 398:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 399:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 400:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 401:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 402:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 403:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 406:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 407:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 408:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 409:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 416:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 417:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 419:

    {;}
    break;

  case 420:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 421:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 422:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 423:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 424:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT();
                                                ;}
    break;

  case 425:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 426:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 427:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 428:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 429:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 430:

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

