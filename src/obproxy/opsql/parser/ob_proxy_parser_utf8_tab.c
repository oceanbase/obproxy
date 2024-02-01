
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

#define SET_BASIC_STMT(stmt_type) \
do {\
  if (OBPROXY_T_INVALID == result->cur_stmt_type_\
      || OBPROXY_T_BEGIN == result->cur_stmt_type_) {\
    result->cur_stmt_type_ = stmt_type;\
  }\
} while (0);




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
     COMMENT_BEGIN = 287,
     COMMENT_END = 288,
     ROUTE_TABLE = 289,
     ROUTE_PART_KEY = 290,
     QUERY_TIMEOUT = 291,
     READ_CONSISTENCY = 292,
     WEAK = 293,
     STRONG = 294,
     FROZEN = 295,
     PLACE_HOLDER = 296,
     END_P = 297,
     ERROR = 298,
     WHEN = 299,
     TABLEGROUP = 300,
     FLASHBACK = 301,
     AUDIT = 302,
     NOAUDIT = 303,
     STATUS = 304,
     BEGI = 305,
     START = 306,
     TRANSACTION = 307,
     READ = 308,
     ONLY = 309,
     WITH = 310,
     CONSISTENT = 311,
     SNAPSHOT = 312,
     INDEX = 313,
     XA = 314,
     WARNINGS = 315,
     ERRORS = 316,
     TRACE = 317,
     QUICK = 318,
     COUNT = 319,
     AS = 320,
     WHERE = 321,
     VALUES = 322,
     ORDER = 323,
     GROUP = 324,
     HAVING = 325,
     INTO = 326,
     UNION = 327,
     FOR = 328,
     TX_READ_ONLY = 329,
     SELECT_OBPROXY_ROUTE_ADDR = 330,
     SET_OBPROXY_ROUTE_ADDR = 331,
     NAME_OB_DOT = 332,
     NAME_OB = 333,
     EXPLAIN = 334,
     EXPLAIN_ROUTE = 335,
     DESC = 336,
     DESCRIBE = 337,
     NAME_STR = 338,
     LOAD = 339,
     DATA = 340,
     HINT_BEGIN = 341,
     LOCAL = 342,
     INFILE = 343,
     USE = 344,
     HELP = 345,
     SET_NAMES = 346,
     SET_CHARSET = 347,
     SET_PASSWORD = 348,
     SET_DEFAULT = 349,
     SET_OB_READ_CONSISTENCY = 350,
     SET_TX_READ_ONLY = 351,
     GLOBAL = 352,
     SESSION = 353,
     NUMBER_VAL = 354,
     GROUP_ID = 355,
     TABLE_ID = 356,
     ELASTIC_ID = 357,
     TESTLOAD = 358,
     ODP_COMMENT = 359,
     TNT_ID = 360,
     DISASTER_STATUS = 361,
     TRACE_ID = 362,
     RPC_ID = 363,
     TARGET_DB_SERVER = 364,
     TRACE_LOG = 365,
     DBP_COMMENT = 366,
     ROUTE_TAG = 367,
     SYS_TAG = 368,
     TABLE_NAME = 369,
     SCAN_ALL = 370,
     STICKY_SESSION = 371,
     PARALL = 372,
     SHARD_KEY = 373,
     STOP_DDL_TASK = 374,
     RETRY_DDL_TASK = 375,
     INT_NUM = 376,
     SHOW_PROXYNET = 377,
     THREAD = 378,
     CONNECTION = 379,
     LIMIT = 380,
     OFFSET = 381,
     SHOW_PROCESSLIST = 382,
     SHOW_PROXYSESSION = 383,
     SHOW_GLOBALSESSION = 384,
     ATTRIBUTE = 385,
     VARIABLES = 386,
     ALL = 387,
     STAT = 388,
     READ_STALE = 389,
     SHOW_PROXYCONFIG = 390,
     DIFF = 391,
     USER = 392,
     LIKE = 393,
     SHOW_PROXYSM = 394,
     SHOW_PROXYCLUSTER = 395,
     SHOW_PROXYRESOURCE = 396,
     SHOW_PROXYCONGESTION = 397,
     SHOW_PROXYROUTE = 398,
     PARTITION = 399,
     ROUTINE = 400,
     SUBPARTITION = 401,
     SHOW_PROXYVIP = 402,
     SHOW_PROXYMEMORY = 403,
     OBJPOOL = 404,
     SHOW_SQLAUDIT = 405,
     SHOW_WARNLOG = 406,
     SHOW_PROXYSTAT = 407,
     REFRESH = 408,
     SHOW_PROXYTRACE = 409,
     SHOW_PROXYINFO = 410,
     BINARY = 411,
     UPGRADE = 412,
     IDC = 413,
     SHOW_ELASTIC_ID = 414,
     SHOW_TOPOLOGY = 415,
     GROUP_NAME = 416,
     SHOW_DB_VERSION = 417,
     SHOW_DATABASES = 418,
     SHOW_TABLES = 419,
     SHOW_FULL_TABLES = 420,
     SELECT_DATABASE = 421,
     SELECT_PROXY_STATUS = 422,
     SHOW_CREATE_TABLE = 423,
     SELECT_PROXY_VERSION = 424,
     SHOW_COLUMNS = 425,
     SHOW_INDEX = 426,
     ALTER_PROXYCONFIG = 427,
     ALTER_PROXYRESOURCE = 428,
     PING_PROXY = 429,
     KILL_PROXYSESSION = 430,
     KILL_GLOBALSESSION = 431,
     KILL = 432,
     QUERY = 433,
     SHOW_BINLOG_SERVER_FOR_TENANT = 434
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
#define YYFINAL  318
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   2170

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  191
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  149
/* YYNRULES -- Number of rules.  */
#define YYNRULES  463
/* YYNRULES -- Number of states.  */
#define YYNSTATES  765

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   434

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,   189,     2,     2,     2,     2,
     185,   186,   190,     2,   182,     2,   183,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   180,
       2,   184,     2,     2,   181,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   187,     2,   188,     2,     2,     2,     2,
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
     175,   176,   177,   178,   179
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
     810,   812,   816,   818,   822,   824,   828,   831,   836,   837,
     839,   842,   844,   848,   850,   854,   857,   862,   865,   868,
     869,   872,   877,   879,   884,   890,   892,   894,   899,   904,
     910,   916,   917,   919,   921,   923,   924,   926,   930,   934,
     937,   943,   945,   947,   949,   951,   953,   955,   957,   959,
     961,   963,   965,   967,   969,   971,   973,   975,   977,   979,
     981,   983,   985,   987,   989,   991,   992,   995,  1000,  1005,
    1006,  1009,  1010,  1013,  1016,  1018,  1020,  1024,  1027,  1031,
    1036,  1038,  1041,  1042,  1045,  1049,  1052,  1055,  1058,  1059,
    1062,  1066,  1069,  1073,  1076,  1080,  1084,  1089,  1092,  1094,
    1097,  1100,  1104,  1107,  1110,  1111,  1113,  1115,  1118,  1121,
    1125,  1128,  1130,  1133,  1135,  1138,  1141,  1145,  1148,  1151,
    1154,  1155,  1157,  1161,  1167,  1170,  1174,  1177,  1178,  1180,
    1183,  1186,  1189,  1192,  1197,  1203,  1209,  1213,  1215,  1218,
    1222,  1226,  1229,  1232,  1236,  1240,  1241,  1244,  1246,  1250,
    1254,  1258,  1259,  1261,  1263,  1267,  1270,  1274,  1277,  1280,
    1282,  1283,  1286,  1291,  1294,  1299,  1302,  1304,  1308,  1311,
    1316,  1320,  1326,  1328,  1330,  1332,  1334,  1336,  1338,  1340,
    1342,  1344,  1346,  1348,  1350,  1352,  1354,  1356,  1358,  1360,
    1362,  1364,  1366,  1368,  1370,  1372,  1374,  1376,  1378,  1380,
    1382,  1384,  1386,  1388
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     192,     0,    -1,   193,    -1,     1,    -1,   194,    -1,   193,
     194,    -1,   195,    42,    -1,   195,   180,    -1,   195,   180,
      42,    -1,   180,    -1,   180,    42,    -1,    50,   195,   180,
      -1,   196,    -1,   264,   196,    -1,   197,    -1,   255,    -1,
     260,    -1,   256,    -1,   257,    -1,   258,    -1,   204,    -1,
     203,    -1,   328,    -1,   292,    -1,   226,    -1,   293,    -1,
     332,    -1,   333,    -1,   238,    -1,   239,    -1,   240,    -1,
     241,    -1,   242,    -1,   243,    -1,   244,    -1,   205,    -1,
     217,    -1,   259,    -1,   294,    -1,   202,    -1,   334,    -1,
     278,   223,   222,    -1,    -1,     9,    -1,    88,    78,   198,
      19,   337,    -1,    87,    88,    78,   198,    19,   337,    -1,
     200,    -1,   199,    -1,   285,   201,    -1,   219,   197,    -1,
     219,   255,    -1,   219,   257,    -1,   219,   258,    -1,   219,
     256,    -1,   219,   259,    -1,   221,   196,    -1,   206,    -1,
     218,    -1,    14,   207,    -1,    15,   208,    -1,    16,    -1,
      17,    -1,    18,    -1,   209,    -1,   210,    -1,    -1,    19,
      -1,    58,    -1,    20,    58,    -1,    45,    -1,    -1,    45,
      -1,   119,   121,    -1,   120,   121,    -1,   197,    -1,   255,
      -1,   256,    -1,   258,    -1,   257,    -1,   334,    -1,   244,
      -1,   259,    -1,   181,    78,    -1,   212,   182,   181,    78,
      -1,   181,    78,    -1,   213,    -1,   211,    -1,    28,   339,
      26,    -1,    29,   339,    -1,    29,   339,    30,    -1,   215,
     214,    -1,   216,   212,    -1,    15,    28,   339,    -1,    31,
      28,   339,    -1,    21,    -1,    22,    -1,    23,    -1,    24,
      -1,    46,    -1,    25,    -1,    47,    -1,    48,    -1,   220,
      -1,   220,    78,    -1,    79,    -1,    81,    -1,    82,    -1,
      80,    -1,    -1,    26,   251,    -1,    -1,   248,    -1,     5,
      74,    -1,     5,    74,   223,    26,   251,    -1,     5,    74,
     248,    -1,   169,    -1,   169,    65,   339,    -1,   224,    -1,
     225,    -1,   236,    -1,   237,    -1,   227,    -1,   235,    -1,
     160,   228,    -1,   234,    -1,   166,    -1,   167,    -1,   163,
      -1,   232,    -1,   233,    -1,   170,   228,    -1,   171,   228,
      -1,   229,    -1,   220,   339,    -1,    26,   339,    -1,    26,
     339,    26,   339,    -1,    26,   339,   183,   339,    -1,   168,
      78,    -1,   168,    78,   183,    78,    -1,    -1,   138,    78,
      -1,    -1,    26,    78,    -1,   164,   231,   230,    -1,   165,
     231,   230,    -1,    11,    19,    49,   231,   230,    -1,   162,
      -1,   159,    -1,   159,    26,    78,    -1,   159,    66,   161,
     184,    78,    -1,   159,    26,    78,    66,   161,   184,    78,
      -1,    75,    -1,    76,   184,   121,    -1,    91,    -1,    92,
      -1,    93,    -1,    94,    -1,    95,    -1,    96,    -1,    13,
     245,   185,   246,   186,    -1,   339,    -1,   339,   183,   339,
      -1,   339,   183,   339,   183,   339,    -1,    -1,   247,    -1,
     246,   182,   247,    -1,    78,    -1,   121,    -1,    99,    -1,
     181,    78,    -1,   181,   181,    78,    -1,    41,    -1,   249,
      -1,   248,   249,    -1,   250,    -1,   185,   186,    -1,   185,
     197,   186,    -1,   185,   248,   186,    -1,   336,    -1,   252,
      -1,   197,    -1,    -1,   185,   254,   186,    -1,   339,    -1,
     254,   182,   339,    -1,   281,   337,   335,    -1,   281,   337,
     335,   253,   252,    -1,   283,   251,    -1,   279,   251,    -1,
     280,   291,    26,   251,    -1,   284,   337,    -1,    12,   261,
      -1,   262,   182,   261,    -1,   262,    -1,   181,   339,   184,
     263,    -1,   181,   181,    97,   339,   184,   263,    -1,    97,
     339,   184,   263,    -1,   181,   181,   339,   184,   263,    -1,
     181,   181,    98,   339,   184,   263,    -1,    98,   339,   184,
     263,    -1,   339,   184,   263,    -1,   339,    -1,   121,    -1,
      99,    -1,   265,    -1,   265,   264,    -1,    32,   266,    33,
      -1,    32,   104,   274,   273,    33,    -1,    32,   101,   184,
     277,   273,    33,    -1,    32,   114,   184,   277,   273,    33,
      -1,    32,   100,   184,   277,   273,    33,    -1,    32,   102,
     184,   277,   273,    33,    -1,    32,   103,   184,   277,   273,
      33,    -1,    32,    77,    78,   184,   276,   273,    33,    -1,
      32,   107,   184,   275,   273,    33,    -1,    32,   108,   184,
     275,   273,    33,    -1,    32,   105,   184,   277,   273,    33,
      -1,    32,   106,   184,   277,   273,    33,    -1,    32,   111,
     112,   184,   187,   268,   188,    33,    -1,    32,   111,   113,
     184,   187,   270,   188,    33,    -1,    32,   109,   184,   277,
     273,    33,    -1,    -1,   266,   267,    -1,    34,    78,    -1,
      35,    78,    -1,    78,    -1,   269,   182,   268,    -1,   269,
      -1,   100,   185,   277,   186,    -1,   114,   185,   277,   186,
      -1,   115,   185,   186,    -1,   115,   185,   117,   184,   277,
     186,    -1,   116,   185,   186,    -1,   118,   185,   271,   186,
      -1,    62,   185,   275,   186,    -1,    62,   185,   275,   189,
     275,   186,    -1,   272,   182,   271,    -1,   272,    -1,    78,
     184,   277,    -1,    -1,   273,   182,   274,    -1,   100,   184,
     277,    -1,   101,   184,   277,    -1,   114,   184,   277,    -1,
     102,   184,   277,    -1,   103,   184,   277,    -1,   107,   184,
     275,    -1,   108,   184,   275,    -1,   105,   184,   277,    -1,
     106,   184,   277,    -1,   110,    -1,   109,   184,   277,    -1,
      78,   183,    78,   184,   276,    -1,    78,   184,   276,    -1,
      -1,   277,    -1,    -1,   277,    -1,    78,    -1,    83,    -1,
       5,    -1,     5,   286,   287,    -1,     8,    -1,     8,   286,
     287,    -1,     6,    -1,     6,   286,   287,    -1,     7,   282,
      -1,     7,   286,   287,   282,    -1,    -1,   132,    -1,   132,
      44,    -1,     9,    -1,     9,   286,   287,    -1,    10,    -1,
      10,   286,   287,    -1,    84,    85,    -1,    84,    85,   286,
     287,    -1,    32,    86,    -1,   288,    33,    -1,    -1,   289,
     288,    -1,    36,   185,   121,   186,    -1,   121,    -1,    37,
     185,   290,   186,    -1,    58,   185,    78,    78,   186,    -1,
     110,    -1,    78,    -1,    78,   185,   121,   186,    -1,    78,
     185,    78,   186,    -1,    78,   185,    78,    78,   186,    -1,
      78,   185,    78,   121,   186,    -1,    -1,    38,    -1,    39,
      -1,    40,    -1,    -1,    63,    -1,    11,   327,    60,    -1,
      11,   327,    61,    -1,    11,    62,    -1,    11,    62,    78,
     184,    78,    -1,   298,    -1,   300,    -1,   301,    -1,   304,
      -1,   302,    -1,   306,    -1,   307,    -1,   308,    -1,   309,
      -1,   311,    -1,   312,    -1,   313,    -1,   314,    -1,   315,
      -1,   317,    -1,   318,    -1,   320,    -1,   321,    -1,   322,
      -1,   323,    -1,   324,    -1,   325,    -1,   326,    -1,   179,
      -1,    -1,   125,   121,    -1,   125,   121,   182,   121,    -1,
     125,   121,   126,   121,    -1,    -1,   138,    78,    -1,    -1,
     138,    78,    -1,   122,   299,    -1,   123,    -1,   124,    -1,
     124,   121,   295,    -1,   135,   296,    -1,   135,   136,   296,
      -1,   135,   136,   137,   296,    -1,   127,    -1,   129,   303,
      -1,    -1,   130,    78,    -1,   130,   138,    78,    -1,   130,
     132,    -1,   138,    78,    -1,   128,   305,    -1,    -1,   130,
     296,    -1,   130,   121,   296,    -1,   133,   296,    -1,   133,
     121,   296,    -1,   131,   296,    -1,   131,   121,   296,    -1,
     131,   132,   296,    -1,   131,   132,   121,   296,    -1,   134,
     296,    -1,   139,    -1,   139,   121,    -1,   140,   296,    -1,
     140,   158,   296,    -1,   141,   296,    -1,   142,   310,    -1,
      -1,    78,    -1,   132,    -1,   132,    78,    -1,   143,   297,
      -1,   143,   145,   297,    -1,   143,   144,    -1,   147,    -1,
     147,    78,    -1,   148,    -1,   148,   121,    -1,   148,   149,
      -1,   148,   149,   121,    -1,   150,   295,    -1,   150,   121,
      -1,   151,   316,    -1,    -1,   121,    -1,   121,   182,   121,
      -1,   121,   182,   121,   182,    78,    -1,   152,   296,    -1,
     152,   153,   296,    -1,   154,   319,    -1,    -1,   121,    -1,
     121,   121,    -1,   155,   156,    -1,   155,   157,    -1,   155,
     158,    -1,   172,    12,    78,   184,    -1,   172,    12,    78,
     184,    78,    -1,   172,    12,    78,   184,   121,    -1,   173,
       6,    78,    -1,   174,    -1,   175,   121,    -1,   175,   121,
     121,    -1,   176,    78,   121,    -1,   176,    78,    -1,   177,
     121,    -1,   177,   124,   121,    -1,   177,   178,   121,    -1,
      -1,    64,   190,    -1,    50,    -1,    51,    52,   329,    -1,
      59,    50,    78,    -1,    59,    51,    78,    -1,    -1,   330,
      -1,   331,    -1,   330,   182,   331,    -1,    53,    54,    -1,
      55,    56,    57,    -1,    89,   339,    -1,    90,    78,    -1,
      78,    -1,    -1,   146,   339,    -1,   146,   185,   339,   186,
      -1,   144,   339,    -1,   144,   185,   339,   186,    -1,   337,
     335,    -1,   339,    -1,   339,   183,   339,    -1,   339,   339,
      -1,   339,   183,   339,   339,    -1,   339,    65,   339,    -1,
     339,   183,   339,    65,   339,    -1,    51,    -1,    59,    -1,
      50,    -1,    52,    -1,    56,    -1,    61,    -1,    60,    -1,
      64,    -1,    63,    -1,    62,    -1,   123,    -1,   124,    -1,
     126,    -1,   130,    -1,   131,    -1,   133,    -1,   136,    -1,
     137,    -1,   149,    -1,   153,    -1,   157,    -1,   158,    -1,
     178,    -1,   161,    -1,    46,    -1,    47,    -1,    48,    -1,
      87,    -1,    85,    -1,    49,    -1,    78,    -1,   338,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   345,   345,   346,   348,   349,   351,   352,   353,   354,
     355,   356,   358,   359,   361,   362,   363,   364,   365,   366,
     367,   368,   369,   370,   371,   372,   373,   374,   375,   376,
     377,   378,   379,   380,   381,   382,   383,   384,   385,   386,
     387,   389,   394,   396,   398,   403,   408,   409,   411,   413,
     414,   415,   416,   417,   418,   420,   422,   423,   425,   426,
     427,   428,   429,   430,   431,   433,   434,   435,   436,   437,
     439,   440,   442,   448,   454,   455,   456,   457,   458,   459,
     460,   461,   463,   470,   478,   486,   487,   490,   496,   501,
     507,   510,   513,   518,   524,   525,   526,   527,   528,   529,
     530,   531,   533,   534,   536,   537,   538,   540,   542,   543,
     545,   546,   548,   549,   550,   552,   553,   555,   556,   557,
     558,   559,   561,   562,   563,   564,   565,   566,   567,   568,
     569,   570,   571,   572,   579,   583,   588,   594,   599,   606,
     607,   609,   610,   612,   616,   621,   626,   628,   629,   634,
     639,   646,   649,   655,   656,   657,   658,   659,   660,   663,
     665,   669,   674,   682,   685,   690,   695,   700,   705,   710,
     715,   720,   728,   729,   731,   733,   734,   735,   737,   738,
     740,   742,   743,   745,   746,   748,   752,   753,   754,   755,
     756,   761,   763,   764,   766,   770,   774,   778,   782,   786,
     790,   794,   799,   804,   810,   811,   813,   814,   815,   816,
     817,   818,   819,   820,   826,   827,   828,   829,   830,   831,
     832,   833,   834,   836,   837,   838,   840,   841,   843,   848,
     853,   854,   855,   856,   858,   859,   861,   862,   864,   872,
     873,   875,   876,   877,   878,   879,   880,   881,   882,   883,
     884,   885,   886,   892,   894,   895,   897,   898,   900,   901,
     903,   904,   905,   906,   907,   908,   910,   911,   913,   914,
     915,   917,   918,   919,   920,   921,   922,   924,   925,   926,
     927,   929,   930,   931,   932,   937,   938,   939,   940,   941,
     942,   944,   945,   946,   947,   949,   950,   953,   954,   955,
     956,   959,   960,   961,   962,   963,   964,   965,   966,   967,
     968,   969,   970,   971,   972,   973,   974,   975,   976,   977,
     978,   979,   980,   981,   983,   988,   990,   994,   999,  1007,
    1008,  1012,  1013,  1016,  1018,  1019,  1020,  1024,  1025,  1026,
    1030,  1032,  1034,  1035,  1036,  1037,  1038,  1041,  1043,  1044,
    1045,  1046,  1047,  1048,  1049,  1050,  1051,  1052,  1056,  1057,
    1061,  1062,  1067,  1070,  1072,  1073,  1074,  1075,  1079,  1080,
    1081,  1085,  1086,  1090,  1091,  1092,  1093,  1097,  1098,  1101,
    1103,  1104,  1105,  1106,  1110,  1111,  1114,  1116,  1117,  1118,
    1122,  1123,  1124,  1128,  1129,  1130,  1134,  1138,  1142,  1143,
    1147,  1148,  1152,  1153,  1154,  1157,  1158,  1161,  1165,  1166,
    1167,  1169,  1170,  1172,  1173,  1176,  1177,  1180,  1186,  1189,
    1191,  1192,  1193,  1194,  1195,  1197,  1202,  1205,  1209,  1213,
    1218,  1222,  1228,  1229,  1230,  1231,  1232,  1233,  1234,  1235,
    1236,  1237,  1238,  1239,  1240,  1241,  1242,  1243,  1244,  1245,
    1246,  1247,  1248,  1249,  1250,  1251,  1252,  1253,  1254,  1255,
    1256,  1257,  1259,  1260
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
  "COMMENT_BEGIN", "COMMENT_END", "ROUTE_TABLE", "ROUTE_PART_KEY",
  "QUERY_TIMEOUT", "READ_CONSISTENCY", "WEAK", "STRONG", "FROZEN",
  "PLACE_HOLDER", "END_P", "ERROR", "WHEN", "TABLEGROUP", "FLASHBACK",
  "AUDIT", "NOAUDIT", "STATUS", "BEGI", "START", "TRANSACTION", "READ",
  "ONLY", "WITH", "CONSISTENT", "SNAPSHOT", "INDEX", "XA", "WARNINGS",
  "ERRORS", "TRACE", "QUICK", "COUNT", "AS", "WHERE", "VALUES", "ORDER",
  "GROUP", "HAVING", "INTO", "UNION", "FOR", "TX_READ_ONLY",
  "SELECT_OBPROXY_ROUTE_ADDR", "SET_OBPROXY_ROUTE_ADDR", "NAME_OB_DOT",
  "NAME_OB", "EXPLAIN", "EXPLAIN_ROUTE", "DESC", "DESCRIBE", "NAME_STR",
  "LOAD", "DATA", "HINT_BEGIN", "LOCAL", "INFILE", "USE", "HELP",
  "SET_NAMES", "SET_CHARSET", "SET_PASSWORD", "SET_DEFAULT",
  "SET_OB_READ_CONSISTENCY", "SET_TX_READ_ONLY", "GLOBAL", "SESSION",
  "NUMBER_VAL", "GROUP_ID", "TABLE_ID", "ELASTIC_ID", "TESTLOAD",
  "ODP_COMMENT", "TNT_ID", "DISASTER_STATUS", "TRACE_ID", "RPC_ID",
  "TARGET_DB_SERVER", "TRACE_LOG", "DBP_COMMENT", "ROUTE_TAG", "SYS_TAG",
  "TABLE_NAME", "SCAN_ALL", "STICKY_SESSION", "PARALL", "SHARD_KEY",
  "STOP_DDL_TASK", "RETRY_DDL_TASK", "INT_NUM", "SHOW_PROXYNET", "THREAD",
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
  "merge_with_opt_hint", "load_data_opt_hint", "hint_list_begin",
  "hint_list_with_end", "hint_list", "hint", "opt_read_consistency",
  "opt_quick", "show_stmt", "icmd_stmt", "binlog_stmt", "opt_limit",
  "opt_like", "opt_large_like", "show_proxynet", "opt_show_net",
  "show_proxyconfig", "show_processlist", "show_globalsession",
  "opt_show_global_session", "show_proxysession", "opt_show_session",
  "show_proxysm", "show_proxycluster", "show_proxyresource",
  "show_proxycongestion", "opt_show_congestion", "show_proxyroute",
  "show_proxyvip", "show_proxymemory", "show_sqlaudit", "show_warnlog",
  "opt_show_warnlog", "show_proxystat", "show_proxytrace",
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
      59,    64,    44,    46,    61,    40,    41,   123,   125,    35,
      42
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   191,   192,   192,   193,   193,   194,   194,   194,   194,
     194,   194,   195,   195,   196,   196,   196,   196,   196,   196,
     196,   196,   196,   196,   196,   196,   196,   196,   196,   196,
     196,   196,   196,   196,   196,   196,   196,   196,   196,   196,
     196,   197,   198,   198,   199,   200,   201,   201,   202,   203,
     203,   203,   203,   203,   203,   204,   205,   205,   206,   206,
     206,   206,   206,   206,   206,   207,   207,   207,   207,   207,
     208,   208,   209,   210,   211,   211,   211,   211,   211,   211,
     211,   211,   212,   212,   213,   214,   214,   215,   216,   216,
     217,   217,   217,   217,   218,   218,   218,   218,   218,   218,
     218,   218,   219,   219,   220,   220,   220,   221,   222,   222,
     223,   223,   224,   224,   224,   225,   225,   226,   226,   226,
     226,   226,   227,   227,   227,   227,   227,   227,   227,   227,
     227,   227,   227,   227,   228,   228,   228,   229,   229,   230,
     230,   231,   231,   232,   232,   233,   234,   235,   235,   235,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   245,   245,   246,   246,   246,   247,   247,   247,   247,
     247,   247,   248,   248,   249,   250,   250,   250,   251,   251,
     252,   253,   253,   254,   254,   255,   255,   256,   257,   258,
     259,   260,   261,   261,   262,   262,   262,   262,   262,   262,
     262,   263,   263,   263,   264,   264,   265,   265,   265,   265,
     265,   265,   265,   265,   265,   265,   265,   265,   265,   265,
     265,   266,   266,   267,   267,   267,   268,   268,   269,   269,
     269,   269,   269,   269,   270,   270,   271,   271,   272,   273,
     273,   274,   274,   274,   274,   274,   274,   274,   274,   274,
     274,   274,   274,   274,   275,   275,   276,   276,   277,   277,
     278,   278,   279,   279,   280,   280,   281,   281,   282,   282,
     282,   283,   283,   284,   284,   285,   285,   286,   287,   288,
     288,   289,   289,   289,   289,   289,   289,   289,   289,   289,
     289,   290,   290,   290,   290,   291,   291,   292,   292,   292,
     292,   293,   293,   293,   293,   293,   293,   293,   293,   293,
     293,   293,   293,   293,   293,   293,   293,   293,   293,   293,
     293,   293,   293,   293,   294,   295,   295,   295,   295,   296,
     296,   297,   297,   298,   299,   299,   299,   300,   300,   300,
     301,   302,   303,   303,   303,   303,   303,   304,   305,   305,
     305,   305,   305,   305,   305,   305,   305,   305,   306,   306,
     307,   307,   308,   309,   310,   310,   310,   310,   311,   311,
     311,   312,   312,   313,   313,   313,   313,   314,   314,   315,
     316,   316,   316,   316,   317,   317,   318,   319,   319,   319,
     320,   320,   320,   321,   321,   321,   322,   323,   324,   324,
     325,   325,   326,   326,   326,   327,   327,   328,   328,   328,
     328,   329,   329,   330,   330,   331,   331,   332,   333,   334,
     335,   335,   335,   335,   335,   336,   337,   337,   337,   337,
     337,   337,   338,   338,   338,   338,   338,   338,   338,   338,
     338,   338,   338,   338,   338,   338,   338,   338,   338,   338,
     338,   338,   338,   338,   338,   338,   338,   338,   338,   338,
     338,   338,   339,   339
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
       1,     3,     1,     3,     1,     3,     2,     4,     0,     1,
       2,     1,     3,     1,     3,     2,     4,     2,     2,     0,
       2,     4,     1,     4,     5,     1,     1,     4,     4,     5,
       5,     0,     1,     1,     1,     0,     1,     3,     3,     2,
       5,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     0,     2,     4,     4,     0,
       2,     0,     2,     2,     1,     1,     3,     2,     3,     4,
       1,     2,     0,     2,     3,     2,     2,     2,     0,     2,
       3,     2,     3,     2,     3,     3,     4,     2,     1,     2,
       2,     3,     2,     2,     0,     1,     1,     2,     2,     3,
       2,     1,     2,     1,     2,     2,     3,     2,     2,     2,
       0,     1,     3,     5,     2,     3,     2,     0,     1,     2,
       2,     2,     2,     4,     5,     5,     3,     1,     2,     3,
       3,     2,     2,     3,     3,     0,     2,     1,     3,     3,
       3,     0,     1,     1,     3,     2,     3,     2,     2,     1,
       0,     2,     4,     2,     4,     2,     1,     3,     2,     4,
       3,     5,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
       0,     3,   260,   264,   268,   262,   271,   273,   405,     0,
       0,    65,    70,    60,    61,    62,    94,    95,    96,    97,
      99,     0,     0,     0,   221,    98,   100,   101,   407,     0,
       0,   151,     0,   419,   104,   107,   105,   106,     0,     0,
       0,   153,   154,   155,   156,   157,   158,     0,     0,     0,
     340,   348,   342,   329,   358,   329,   329,   364,   331,   371,
     373,   325,   380,   329,   387,     0,   147,     0,   146,   127,
     141,   141,   125,   126,     0,   115,     0,     0,     0,     0,
     397,     0,     0,     0,   324,     9,     0,     2,     4,     0,
      12,    14,    39,    21,    20,    35,    56,    63,    64,     0,
       0,    36,    57,     0,   102,     0,   117,   118,    24,   121,
     132,   128,   129,   124,   122,   119,   120,    28,    29,    30,
      31,    32,    33,    34,    15,    17,    18,    19,    37,    16,
       0,   204,   110,     0,   295,     0,     0,     0,     0,    23,
      25,    38,   301,   302,   303,   305,   304,   306,   307,   308,
     309,   310,   311,   312,   313,   314,   315,   316,   317,   318,
     319,   320,   321,   322,   323,    22,    26,    27,    40,     0,
     112,   279,   279,   269,   266,   279,   279,   279,   279,     0,
     299,     0,     0,   456,   457,   458,   461,   434,   432,   435,
     436,   433,   438,   437,   441,   440,   439,   462,   460,   459,
       0,     0,   442,   443,   444,   445,   446,   447,   448,   449,
     450,   451,   452,   453,   455,   454,     0,   191,   193,   463,
       0,     0,   160,    66,     0,    69,    67,    58,     0,    71,
      59,     0,    88,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   407,     0,
     411,     0,     0,     0,   275,   417,   418,    72,    73,   334,
     335,   333,   329,   329,   329,   329,   347,     0,     0,   341,
     329,     0,   337,   359,   329,   360,   362,   365,   366,   363,
       0,   370,   331,   368,   372,   374,   375,   378,     0,   377,
     381,   379,   329,   384,   388,   386,   390,   391,   392,     0,
       0,     0,   123,     0,   139,   139,   137,     0,   130,   131,
       0,     0,   398,   401,   402,     0,     0,    10,     1,     5,
       6,     7,   260,     0,    74,    86,    85,    90,    80,    75,
      76,    78,    77,    81,    79,     0,    91,    49,    50,    53,
      51,    52,    54,   103,   133,    55,    13,   205,     0,   108,
     111,   172,   174,   180,   188,   179,   178,   420,   426,   296,
       0,   420,   187,   190,     0,     0,    47,    46,    48,   277,
       0,   114,     0,     0,     0,   286,   285,   282,   261,     0,
     279,   265,   270,   268,   263,   272,   274,   141,     0,   406,
     297,   298,     0,     0,     0,     0,     0,     0,   163,     0,
      68,    92,    87,    89,    93,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     250,     0,   239,     0,     0,   254,   254,     0,     0,     0,
       0,   206,     0,     0,   225,   222,    11,     0,     0,   408,
     412,   413,   409,   410,   152,   279,   325,   329,   349,   329,
     329,   353,   329,   351,   357,   343,   345,     0,   346,   329,
     338,   330,   361,   367,   332,   369,   376,   326,     0,   385,
     389,   148,     0,   134,   142,     0,   143,   144,     0,   116,
       0,   396,   399,   400,   403,   404,     8,    84,    82,     0,
     175,     0,     0,     0,    41,   173,     0,     0,   425,     0,
       0,   428,     0,   185,     0,    42,     0,     0,   291,     0,
       0,   278,   280,   267,   139,     0,     0,     0,     0,     0,
       0,     0,   192,   203,   202,   200,   201,   171,   166,   168,
     167,     0,     0,   164,   161,   256,   258,   259,   239,   239,
     239,   239,     0,   256,     0,     0,     0,     0,     0,     0,
     254,   254,     0,     0,     0,   239,   239,   239,   255,   239,
     239,     0,     0,   239,   223,   224,   415,     0,     0,   276,
     336,   350,   354,   329,   355,   352,   344,   339,     0,     0,
     382,     0,     0,     0,     0,   140,   138,   393,     0,   176,
     177,   109,     0,   423,     0,   421,   430,   427,   189,     0,
       0,    42,    43,     0,   113,     0,   292,   293,   294,     0,
       0,     0,     0,   145,   300,   196,   199,     0,     0,     0,
     194,   169,     0,     0,   159,     0,   239,   257,     0,     0,
       0,     0,     0,   253,   241,   242,   244,   245,   248,   249,
     246,   247,   251,   243,   207,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   416,   414,   356,   328,   327,     0,
       0,   149,   135,   136,   394,   395,    83,     0,     0,     0,
     429,     0,   183,   186,     0,     0,   281,   283,     0,     0,
       0,   288,   287,     0,     0,   197,   170,   165,   162,     0,
     210,   208,   211,   212,   256,   240,   216,   217,   214,   215,
     220,     0,     0,     0,     0,     0,     0,   227,     0,     0,
     209,   383,     0,   424,   422,   431,     0,   182,     0,    44,
     284,   289,   290,   195,   198,   213,   252,     0,     0,     0,
       0,     0,     0,     0,   254,     0,   150,   184,    45,     0,
       0,     0,   230,   232,     0,     0,   237,   218,   226,     0,
     219,   228,   229,     0,     0,   233,     0,   234,   254,     0,
     238,   236,     0,   231,   235
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    86,    87,    88,    89,    90,   353,   603,   366,   367,
     368,    92,    93,    94,    95,    96,   227,   230,    97,    98,
     325,   336,   326,   327,    99,   100,   101,   102,   103,   104,
     105,   494,   349,   106,   107,   108,   109,   302,   110,   476,
     304,   111,   112,   113,   114,   115,   116,   117,   118,   119,
     120,   121,   122,   123,   221,   532,   533,   350,   351,   352,
     354,   355,   600,   671,   124,   125,   126,   127,   128,   129,
     217,   218,   525,   130,   131,   247,   435,   706,   707,   709,
     745,   746,   554,   422,   557,   626,   558,   132,   133,   134,
     135,   174,   136,   137,   138,   171,   378,   379,   380,   609,
     360,   139,   140,   141,   289,   272,   283,   142,   261,   143,
     144,   145,   269,   146,   266,   147,   148,   149,   150,   279,
     151,   152,   153,   154,   155,   291,   156,   157,   295,   158,
     159,   160,   161,   162,   163,   164,   182,   165,   439,   440,
     441,   166,   167,   168,   498,   356,   357,   219,   358
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -496
static const yytype_int16 yypact[] =
{
     619,  -496,    43,    35,    33,    35,    35,    35,    74,  1461,
    1872,   124,    63,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    -496,  1872,  1872,    91,   213,  -496,  -496,  -496,   971,   135,
      90,  -496,   -58,  -496,  -496,  -496,  -496,  -496,    83,  1872,
     122,  -496,  -496,  -496,  -496,  -496,  -496,    84,    97,    87,
    -496,   132,   -41,   173,   117,   -54,   123,     2,    95,   201,
     -51,    58,   134,   -31,   167,   150,    73,   268,  -496,  -496,
     317,   317,  -496,  -496,   267,   297,   268,   268,   351,   358,
    -496,   244,   288,   -45,  -496,   325,   371,   795,  -496,    -2,
    -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,    36,
     193,  -496,  -496,   277,  1992,  1146,  -496,  -496,  -496,  -496,
    -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    1146,   337,   190,   111,   310,  1872,   111,  1872,   158,  -496,
    -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,   290,
       1,    32,    32,   333,  -496,    32,    32,    32,    32,   329,
     301,   191,   210,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    1872,  1872,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    -496,  -496,  -496,  -496,  -496,  -496,  1517,  -496,   198,  -496,
     199,   197,   203,  -496,   326,  -496,  -496,  -496,  1872,  -496,
    -496,   363,   361,  1872,   314,   209,   211,   212,   215,   232,
     218,   220,   221,   222,   223,   188,   224,    52,  -496,   214,
     270,   319,   331,   291,    35,  -496,  -496,  -496,  -496,  -496,
     292,  -496,   -18,   -27,    -6,   123,  -496,   -14,   336,  -496,
     216,   344,  -496,  -496,   123,  -496,  -496,  -496,   345,  -496,
     346,  -496,   273,  -496,  -496,  -496,   305,  -496,   306,  -496,
     252,  -496,   123,  -496,   315,  -496,  -496,  -496,  -496,   357,
     276,  1872,  -496,   360,   302,   302,   256,  1872,  -496,  -496,
     364,   365,   320,   323,  -496,   324,   327,  -496,  -496,  -496,
    -496,   404,    35,   372,  -496,  -496,  -496,  -496,  -496,  -496,
    -496,  -496,  -496,  -496,  -496,   373,   271,  -496,  -496,  -496,
    -496,  -496,  -496,     5,  -496,  -496,  -496,  -496,    18,   423,
     190,  -496,  -496,  -496,  -496,  -496,  -496,   182,  1336,  -496,
     426,   182,  -496,  -496,   367,   379,  -496,  -496,  -496,  -496,
     432,    12,   274,   275,   278,   279,  -496,  -496,  -496,   428,
      32,  -496,  -496,   330,  -496,  -496,  -496,   317,   281,  -496,
    -496,  -496,   282,   283,  1640,   285,  1461,  1696,    14,  1872,
    -496,  -496,  -496,  -496,  -496,   286,    98,    98,    98,    98,
     164,   287,   289,   293,   294,   295,   296,   298,   299,   300,
    -496,   313,  -496,    98,    98,    98,    98,    98,   332,   335,
      98,  -496,   390,   394,  -496,  -496,  -496,   420,   419,  -496,
     303,  -496,  -496,  -496,  -496,    32,   356,   123,  -496,   123,
      56,  -496,   123,  -496,  -496,  -496,  -496,   398,  -496,   123,
    -496,  -496,  -496,  -496,  -496,  -496,  -496,   -70,   368,  -496,
    -496,   430,   339,     7,  -496,   408,  -496,  -496,   416,  -496,
     341,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,   322,
    -496,   318,   172,   111,  -496,  -496,   369,  1280,  -496,  1872,
    1872,  -496,   111,    23,   433,   489,   111,   380,   312,   435,
     -19,  -496,  -496,  -496,   302,   436,  1696,  1696,  1872,  1872,
     347,  1696,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    -496,   -25,   -86,  -496,   334,    98,  -496,  -496,  -496,  -496,
    -496,  -496,   437,    98,    98,    98,    98,    98,    98,    98,
      98,    98,    98,    98,     6,  -496,  -496,  -496,  -496,  -496,
    -496,   342,   348,  -496,  -496,  -496,  -496,   463,   270,  -496,
    -496,  -496,  -496,   123,  -496,  -496,  -496,  -496,   400,   407,
     350,   375,   455,  1872,  1872,  -496,  -496,    10,   456,  -496,
    -496,  -496,  1872,  -496,  1872,  -496,  -496,  1816,  -496,  1872,
     532,   489,  -496,   519,  -496,   353,  -496,  -496,  -496,   354,
     464,   -20,   355,  -496,  -496,  -496,  -496,   362,   366,  1696,
    -496,  -496,   466,    14,  -496,  1872,  -496,  -496,    19,    24,
      27,    30,   374,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    -496,  -496,  -496,  -496,  -496,   232,    38,    40,    41,    44,
      45,   136,   483,    49,  -496,  -496,  -496,  -496,  -496,   470,
     376,  -496,  -496,  -496,  -496,  -496,  -496,   370,   377,  1872,
    -496,    67,  -496,  -496,   530,  1872,  -496,  -496,   378,   381,
     382,  -496,  -496,  1696,  1696,  -496,  -496,  -496,  -496,    50,
    -496,  -496,  -496,  -496,    98,  -496,  -496,  -496,  -496,  -496,
    -496,   384,   385,   386,   387,   388,   389,   383,   391,   392,
    -496,  -496,   473,  -496,  -496,  -496,  1872,  -496,  1872,  -496,
    -496,  -496,  -496,  -496,  -496,  -496,  -496,    98,    98,   -63,
     393,   474,   520,   136,    98,   522,  -496,  -496,  -496,   395,
     396,   403,  -496,  -496,   405,   406,   402,  -496,  -496,   116,
    -496,  -496,  -496,    98,    98,  -496,   474,  -496,    98,   409,
    -496,  -496,   410,  -496,  -496
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -496,  -496,  -496,   472,   529,   -24,    22,   -40,  -496,  -496,
    -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    -496,  -496,   421,  -496,  -496,  -496,  -496,   284,  -496,  -284,
     -56,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    -496,  -496,  -496,   467,  -496,  -496,   -61,  -146,  -299,  -496,
    -134,   -22,  -496,  -496,    81,   177,   192,   194,   200,  -496,
     202,  -496,  -455,   462,  -496,  -496,  -496,  -139,  -496,  -496,
    -159,  -496,  -282,   -46,  -422,  -495,  -398,  -496,  -496,  -496,
    -496,   217,  -496,  -496,  -496,    13,  -141,   225,  -496,  -496,
    -496,  -496,  -496,  -496,   155,   -49,   321,  -496,  -496,  -496,
    -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
    -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,  -496,
      34,  -496,  -496,   505,   245,  -496,  -132,  -496,    -9
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -463
static const yytype_int16 yytable[] =
{
     220,   222,   362,   361,   559,   363,   275,   276,   538,   539,
     540,   541,   231,   232,   293,   305,   172,   175,   176,   177,
     178,   477,    91,   322,   371,   555,   556,  -110,  -181,   560,
     255,   381,   563,   583,   383,   384,   385,   386,  -111,   644,
     320,   322,     3,     4,     5,     6,     7,  -462,   633,    10,
      91,   495,   690,   621,   741,   527,   578,   691,   679,   611,
     692,   615,   616,   693,   455,   169,   620,   169,   372,   373,
     285,   696,   495,   697,   698,   169,   314,   699,   700,   315,
     277,   345,   710,   725,   271,   431,   432,   433,   664,   267,
     374,   228,   528,   179,   449,   344,   623,   268,   286,   299,
     624,   680,   612,   447,   274,   450,   346,   271,   229,    91,
     375,   271,   579,   529,    33,   452,   322,   170,   456,   233,
     271,   324,   292,   742,   457,   337,   253,    91,   640,   641,
     434,   665,   271,   316,   278,   530,   180,   627,   181,   300,
     251,   252,   376,   223,   224,   627,   634,   635,   636,   637,
     638,   639,    91,   377,   642,   643,   622,   183,   184,   185,
     186,   187,   188,   189,   685,   173,   681,   190,   254,   225,
     191,   192,   193,   194,   195,   196,   536,   573,   321,   287,
     329,   537,   226,   288,   338,  -462,   348,   250,   645,   197,
     584,   392,   393,   495,   271,   531,   198,   348,   199,   726,
     256,   645,   492,   348,   490,   257,   645,   395,   599,   645,
     259,   260,   645,   448,   451,   453,   454,   323,   258,   401,
     645,   460,   645,   645,   404,   462,   645,   645,   723,   724,
     613,   645,   645,   280,   202,   203,   701,   204,   273,   281,
     282,   205,   206,   469,   207,   364,   365,   208,   209,   716,
     702,   703,   704,   717,   705,   290,   628,   629,   630,   631,
     210,   271,   262,   263,   211,   264,   265,   445,   212,   213,
     390,   391,   214,   646,   647,   648,   330,   649,   650,   284,
     339,   653,   322,     3,     4,     5,     6,     7,   294,   215,
     234,   331,   473,   332,   301,   340,   627,   341,   479,   333,
     428,   429,   757,   342,   569,   758,   296,   297,   298,   270,
     410,   271,   749,   235,   236,   237,   238,   239,   240,   241,
     242,   243,   244,   437,   245,   438,   496,   246,   497,   739,
     740,   514,   411,   412,   413,   414,   762,   415,   416,   417,
     418,   419,   420,   303,   689,   306,   421,   542,   543,   501,
     606,   607,   608,   459,   271,   759,   760,   348,   590,   591,
     308,   309,   307,   310,   311,   312,   313,   317,   598,    24,
     491,   318,   604,   359,   335,   348,   369,   382,   387,   388,
     396,   389,   398,   397,   400,   520,   399,   220,   526,   402,
     534,   403,   405,   406,   436,   407,   408,   442,   571,   409,
     572,   574,   423,   575,   424,   425,   426,   427,   430,   443,
     577,   280,   444,   446,   458,   183,   184,   185,   186,   187,
     188,   189,   461,   463,   464,   190,   466,   467,   191,   192,
     193,   194,   195,   196,   468,   471,   470,   472,   474,   478,
     475,   482,   480,   481,   483,   484,   486,   197,   485,   493,
     487,   488,   502,   489,   198,   504,   199,   505,   506,   507,
     508,   511,   173,   509,   510,   515,   516,   517,   564,   521,
     535,   544,   565,   545,   566,   567,   576,   546,   547,   548,
     549,   288,   550,   551,   552,   568,   585,   593,   595,   580,
     596,   597,   202,   203,   586,   204,   581,   553,   602,   205,
     206,   605,   207,   588,   589,   208,   209,   526,   526,   617,
     618,   601,   526,   610,   614,   632,   561,   625,   210,   562,
     654,   657,   211,   582,   656,   587,   212,   213,   658,   651,
     214,   619,   659,   661,   666,   652,   660,   322,   675,   676,
     677,   682,   678,   719,   686,   708,   683,   215,   711,   718,
     684,   736,   744,   747,   592,   750,   713,   249,   694,   319,
     712,   674,   687,   714,   720,   733,   328,   721,   722,   727,
     728,   729,   730,   731,   662,   663,   734,   732,   673,   743,
     735,   751,   752,   667,   756,   668,   738,   753,   670,   754,
     672,   370,   755,   347,   748,   763,   764,   761,   522,   695,
     513,   570,   655,   465,   334,   512,   503,     0,     0,     0,
     526,     0,     0,     0,     0,     0,   688,     0,     0,     0,
       1,     0,     0,     0,     2,     3,     4,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,     0,     0,
      16,    17,    18,    19,    20,     0,     0,    21,    22,     0,
      23,    24,     0,     0,     0,     0,     0,     0,     0,     0,
     715,     0,     0,     0,     0,    25,    26,    27,     0,    28,
      29,     0,     0,     0,   526,   526,     0,     0,    30,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    31,    32,     0,    33,    34,    35,
      36,    37,     0,    38,     0,     0,     0,   737,    39,    40,
      41,    42,    43,    44,    45,    46,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    47,    48,
       0,    49,     0,     0,     0,     0,    50,    51,    52,     0,
       0,     0,     0,     0,    53,     0,     0,     0,    54,    55,
      56,    57,    58,     0,     0,     0,    59,    60,     0,    61,
      62,    63,     0,    64,    65,     0,     0,     0,    66,    67,
       0,    68,    69,    70,    71,    72,    73,    74,    75,    76,
      77,    78,    79,    80,    81,    82,    83,     0,    84,    85,
       2,     3,     4,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,     0,     0,    16,    17,    18,    19,
      20,     0,     0,    21,    22,     0,    23,    24,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    25,    26,    27,     0,    28,    29,     0,     0,     0,
       0,     0,     0,     0,    30,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      31,    32,     0,    33,    34,    35,    36,    37,     0,    38,
       0,     0,     0,     0,    39,    40,    41,    42,    43,    44,
      45,    46,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    47,    48,     0,    49,     0,     0,
       0,     0,    50,    51,    52,     0,     0,     0,     0,     0,
      53,     0,     0,     0,    54,    55,    56,    57,    58,     0,
       0,     0,    59,    60,     0,    61,    62,    63,     0,    64,
      65,     0,     0,     0,    66,    67,     0,    68,    69,    70,
      71,    72,    73,    74,    75,    76,    77,    78,    79,    80,
      81,    82,    83,     0,    84,    85,     2,     3,     4,     5,
       6,     7,     8,     9,    10,    11,    12,    13,    14,    15,
       0,     0,    16,    17,    18,    19,    20,     0,     0,    21,
      22,     0,    23,    24,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    25,    26,    27,
       0,   248,    29,     0,     0,     0,     0,     0,     0,     0,
      30,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    31,    32,     0,    33,
      34,    35,    36,    37,     0,    38,     0,     0,     0,     0,
      39,    40,    41,    42,    43,    44,    45,    46,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      47,    48,     0,    49,     0,     0,     0,     0,    50,    51,
      52,     0,     0,     0,     0,     0,    53,     0,     0,     0,
      54,    55,    56,    57,    58,     0,     0,     0,    59,    60,
       0,    61,    62,    63,     0,    64,    65,     0,     0,     0,
      66,    67,     0,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,     0,
      84,     2,     3,     4,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,     0,     0,    16,    17,    18,
      19,    20,     0,     0,    21,    22,     0,    23,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    25,    26,    27,     0,   248,    29,     0,     0,
       0,     0,     0,     0,     0,    30,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    31,    32,     0,    33,    34,    35,    36,    37,     0,
      38,     0,     0,     0,     0,    39,    40,    41,    42,    43,
      44,    45,    46,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    47,    48,     0,    49,     0,
       0,     0,     0,    50,    51,    52,     0,     0,     0,     0,
       0,    53,     0,     0,     0,    54,    55,    56,    57,    58,
       0,     0,     0,    59,    60,     0,    61,    62,    63,     0,
      64,    65,     0,     0,     0,    66,    67,     0,    68,    69,
      70,    71,    72,    73,    74,    75,    76,    77,    78,    79,
      80,    81,    82,    83,     0,    84,   183,   184,   185,   186,
     187,   188,   189,     0,     0,     0,   190,     0,     0,   191,
     192,   193,   194,   195,   196,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   197,     0,
       0,     0,     0,     0,     0,   198,     0,   199,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   183,   184,   185,   186,   187,   188,   189,     0,
       0,     0,   190,     0,     0,   191,   192,   193,   194,   195,
     196,   499,     0,   202,   203,     0,   204,     0,     0,     0,
     205,   206,     0,   207,   197,     0,   208,   209,     0,     0,
       0,   198,     0,   199,     0,     0,     0,     0,     0,   210,
       0,     0,     0,   211,     0,     0,     0,   212,   213,     0,
       0,   214,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   215,   202,
     203,     0,   204,     0,     0,   594,   205,   206,     0,   207,
       0,     0,   208,   209,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   210,     0,     0,     0,   211,
       0,     0,     0,   212,   213,     0,     0,   214,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   183,   184,   185,
     186,   187,   188,   189,   215,     0,     0,   190,     0,   500,
     191,   192,   193,   194,   195,   196,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   197,
       0,     0,     0,     0,     0,     0,   198,     0,   199,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   200,   201,
       0,     0,     0,   183,   184,   185,   186,   187,   188,   189,
       0,     0,     0,   190,     0,     0,   191,   192,   193,   194,
     195,   196,     0,     0,   202,   203,     0,   204,     0,     0,
       0,   205,   206,     0,   207,   197,     0,   208,   209,     0,
       0,     0,   198,     0,   199,     0,     0,     0,     0,     0,
     210,     0,     0,     0,   211,     0,     0,     0,   212,   213,
       0,     0,   214,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   215,
     202,   203,   216,   204,     0,     0,     0,   205,   206,     0,
     207,     0,     0,   208,   209,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   210,     0,     0,     0,
     211,     0,     0,     0,   212,   213,     0,     0,   214,     0,
       0,     0,     0,     0,     0,     0,   183,   184,   185,   186,
     187,   188,   189,     0,     0,   215,   190,     0,   394,   191,
     192,   193,   194,   195,   196,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   197,     0,
       0,     0,     0,     0,     0,   198,     0,   199,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   518,   519,     0,
       0,     0,   183,   184,   185,   186,   187,   188,   189,     0,
       0,     0,   190,     0,     0,   191,   192,   193,   194,   195,
     196,     0,     0,   202,   203,     0,   204,     0,     0,     0,
     205,   206,     0,   207,   197,     0,   208,   209,     0,     0,
       0,   198,     0,   199,     0,     0,     0,     0,     0,   210,
       0,     0,     0,   211,     0,   523,     0,   212,   213,     0,
       0,   214,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   524,   215,   202,
     203,     0,   204,     0,     0,     0,   205,   206,     0,   207,
       0,     0,   208,   209,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   210,     0,     0,     0,   211,
       0,     0,     0,   212,   213,     0,     0,   214,     0,     0,
       0,     0,   183,   184,   185,   186,   187,   188,   189,     0,
       0,     0,   190,     0,   215,   191,   192,   193,   194,   195,
     196,   669,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   197,     0,     0,     0,     0,     0,
       0,   198,     0,   199,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   183,   184,
     185,   186,   187,   188,   189,     0,     0,     0,   190,     0,
       0,   191,   192,   193,   194,   195,   196,     0,     0,   202,
     203,     0,   204,     0,     0,     0,   205,   206,     0,   207,
     197,     0,   208,   209,     0,     0,     0,   198,     0,   199,
       0,     0,     0,     0,     0,   210,     0,     0,     0,   211,
       0,     0,     0,   212,   213,     0,     0,   214,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   215,   202,   203,     0,   204,     0,
       0,     0,   205,   206,     0,   207,     0,     0,   208,   209,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   210,     0,     0,     0,   211,     0,     0,     0,   212,
     213,     0,     0,   214,     0,     0,     0,     0,   183,   184,
     185,   186,   187,   188,   189,     0,     0,     0,   190,     0,
     215,   191,   192,   193,   194,   195,   196,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     343,     0,     0,     0,     0,     0,     0,   198,     0,   199,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   202,   203,     0,   204,     0,
       0,     0,   205,   206,     0,   207,     0,     0,   208,   209,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   210,     0,     0,     0,   211,     0,     0,     0,   212,
     213,     0,     0,   214,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     215
};

static const yytype_int16 yycheck[] =
{
       9,    10,   136,   135,   426,   137,    55,    56,   406,   407,
     408,   409,    21,    22,    63,    71,     3,     4,     5,     6,
       7,   305,     0,     5,   170,   423,   424,    26,     5,   427,
      39,   172,   430,    26,   175,   176,   177,   178,    26,    33,
      42,     5,     6,     7,     8,     9,    10,    42,   543,    13,
      28,   350,    33,    78,   117,    41,   126,    33,    78,    78,
      33,   516,   517,    33,    78,    32,   521,    32,    36,    37,
     121,    33,   371,    33,    33,    32,   121,    33,    33,   124,
      78,   105,    33,    33,   138,    33,    34,    35,    78,   130,
      58,    28,    78,    19,   121,   104,   182,   138,   149,    26,
     186,   121,   121,   121,   158,   132,   130,   138,    45,    87,
      78,   138,   182,    99,    78,   121,     5,    74,   132,    28,
     138,    99,   153,   186,   138,   103,   184,   105,   550,   551,
      78,   121,   138,   178,   132,   121,    62,   535,    64,    66,
      50,    51,   110,    19,    20,   543,   544,   545,   546,   547,
     548,   549,   130,   121,   552,   553,   181,    46,    47,    48,
      49,    50,    51,    52,   619,   132,   186,    56,    85,    45,
      59,    60,    61,    62,    63,    64,    78,   121,   180,   121,
      99,    83,    58,   125,   103,   180,   185,    52,   182,    78,
     183,   200,   201,   492,   138,   181,    85,   185,    87,   694,
      78,   182,   348,   185,   186,   121,   182,   216,   185,   182,
     123,   124,   182,   262,   263,   264,   265,   181,   121,   228,
     182,   270,   182,   182,   233,   274,   182,   182,   683,   684,
     514,   182,   182,   138,   123,   124,   100,   126,   121,   144,
     145,   130,   131,   292,   133,    87,    88,   136,   137,   182,
     114,   115,   116,   186,   118,   121,   538,   539,   540,   541,
     149,   138,   130,   131,   153,   133,   134,   254,   157,   158,
      60,    61,   161,   555,   556,   557,    99,   559,   560,    78,
     103,   563,     5,     6,     7,     8,     9,    10,   121,   178,
      77,    99,   301,    99,    26,   103,   694,   103,   307,    99,
     112,   113,   186,   103,   445,   189,   156,   157,   158,   136,
      78,   138,   734,   100,   101,   102,   103,   104,   105,   106,
     107,   108,   109,    53,   111,    55,   144,   114,   146,   727,
     728,   387,   100,   101,   102,   103,   758,   105,   106,   107,
     108,   109,   110,    26,   626,    78,   114,   183,   184,   358,
      38,    39,    40,   137,   138,   753,   754,   185,   186,   493,
      76,    77,    65,    12,     6,   121,    78,    42,   502,    32,
     348,     0,   506,    63,   181,   185,    86,    44,    49,    78,
     182,   190,   185,   184,    58,   394,   183,   396,   397,    26,
     399,    30,    78,   184,   180,   184,   184,    78,   447,   184,
     449,   450,   184,   452,   184,   184,   184,   184,   184,    78,
     459,   138,   121,   121,    78,    46,    47,    48,    49,    50,
      51,    52,    78,    78,    78,    56,   121,   121,    59,    60,
      61,    62,    63,    64,   182,    78,   121,   161,    78,   183,
     138,   121,    78,    78,   121,   121,    42,    78,   121,    26,
      78,    78,    26,   182,    85,    88,    87,    78,    26,   185,
     185,    33,   132,   185,   185,   184,   184,   184,    78,   184,
     184,   184,    78,   184,    54,    56,    78,   184,   184,   184,
     184,   125,   184,   184,   184,   182,    78,   496,   497,   121,
     499,   500,   123,   124,    78,   126,    66,   184,     9,   130,
     131,   121,   133,   181,   186,   136,   137,   516,   517,   518,
     519,    78,   521,    78,    78,    78,   184,   183,   149,   184,
      57,   121,   153,   184,   573,   184,   157,   158,   121,   187,
     161,   184,   182,    78,    78,   187,   161,     5,    19,   186,
     186,   186,    78,   675,    78,    62,   184,   178,    78,    19,
     184,    78,    78,    33,   185,    33,   186,    28,   184,    87,
     184,   601,   623,   186,   186,   182,    99,   186,   186,   185,
     185,   185,   185,   185,   583,   584,   185,   188,   600,   186,
     188,   186,   186,   592,   182,   594,   718,   184,   597,   184,
     599,   170,   186,   131,   733,   186,   186,   756,   396,   645,
     383,   446,   568,   282,    99,   380,   361,    -1,    -1,    -1,
     619,    -1,    -1,    -1,    -1,    -1,   625,    -1,    -1,    -1,
       1,    -1,    -1,    -1,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,    16,    17,    18,    -1,    -1,
      21,    22,    23,    24,    25,    -1,    -1,    28,    29,    -1,
      31,    32,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     669,    -1,    -1,    -1,    -1,    46,    47,    48,    -1,    50,
      51,    -1,    -1,    -1,   683,   684,    -1,    -1,    59,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    75,    76,    -1,    78,    79,    80,
      81,    82,    -1,    84,    -1,    -1,    -1,   716,    89,    90,
      91,    92,    93,    94,    95,    96,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   119,   120,
      -1,   122,    -1,    -1,    -1,    -1,   127,   128,   129,    -1,
      -1,    -1,    -1,    -1,   135,    -1,    -1,    -1,   139,   140,
     141,   142,   143,    -1,    -1,    -1,   147,   148,    -1,   150,
     151,   152,    -1,   154,   155,    -1,    -1,    -1,   159,   160,
      -1,   162,   163,   164,   165,   166,   167,   168,   169,   170,
     171,   172,   173,   174,   175,   176,   177,    -1,   179,   180,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    -1,    -1,    21,    22,    23,    24,
      25,    -1,    -1,    28,    29,    -1,    31,    32,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    46,    47,    48,    -1,    50,    51,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    59,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      75,    76,    -1,    78,    79,    80,    81,    82,    -1,    84,
      -1,    -1,    -1,    -1,    89,    90,    91,    92,    93,    94,
      95,    96,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   119,   120,    -1,   122,    -1,    -1,
      -1,    -1,   127,   128,   129,    -1,    -1,    -1,    -1,    -1,
     135,    -1,    -1,    -1,   139,   140,   141,   142,   143,    -1,
      -1,    -1,   147,   148,    -1,   150,   151,   152,    -1,   154,
     155,    -1,    -1,    -1,   159,   160,    -1,   162,   163,   164,
     165,   166,   167,   168,   169,   170,   171,   172,   173,   174,
     175,   176,   177,    -1,   179,   180,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,    18,
      -1,    -1,    21,    22,    23,    24,    25,    -1,    -1,    28,
      29,    -1,    31,    32,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    46,    47,    48,
      -1,    50,    51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      59,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    75,    76,    -1,    78,
      79,    80,    81,    82,    -1,    84,    -1,    -1,    -1,    -1,
      89,    90,    91,    92,    93,    94,    95,    96,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     119,   120,    -1,   122,    -1,    -1,    -1,    -1,   127,   128,
     129,    -1,    -1,    -1,    -1,    -1,   135,    -1,    -1,    -1,
     139,   140,   141,   142,   143,    -1,    -1,    -1,   147,   148,
      -1,   150,   151,   152,    -1,   154,   155,    -1,    -1,    -1,
     159,   160,    -1,   162,   163,   164,   165,   166,   167,   168,
     169,   170,   171,   172,   173,   174,   175,   176,   177,    -1,
     179,     5,     6,     7,     8,     9,    10,    11,    12,    13,
      14,    15,    16,    17,    18,    -1,    -1,    21,    22,    23,
      24,    25,    -1,    -1,    28,    29,    -1,    31,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    46,    47,    48,    -1,    50,    51,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    59,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    75,    76,    -1,    78,    79,    80,    81,    82,    -1,
      84,    -1,    -1,    -1,    -1,    89,    90,    91,    92,    93,
      94,    95,    96,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   119,   120,    -1,   122,    -1,
      -1,    -1,    -1,   127,   128,   129,    -1,    -1,    -1,    -1,
      -1,   135,    -1,    -1,    -1,   139,   140,   141,   142,   143,
      -1,    -1,    -1,   147,   148,    -1,   150,   151,   152,    -1,
     154,   155,    -1,    -1,    -1,   159,   160,    -1,   162,   163,
     164,   165,   166,   167,   168,   169,   170,   171,   172,   173,
     174,   175,   176,   177,    -1,   179,    46,    47,    48,    49,
      50,    51,    52,    -1,    -1,    -1,    56,    -1,    -1,    59,
      60,    61,    62,    63,    64,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,
      -1,    -1,    -1,    -1,    -1,    85,    -1,    87,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    46,    47,    48,    49,    50,    51,    52,    -1,
      -1,    -1,    56,    -1,    -1,    59,    60,    61,    62,    63,
      64,    65,    -1,   123,   124,    -1,   126,    -1,    -1,    -1,
     130,   131,    -1,   133,    78,    -1,   136,   137,    -1,    -1,
      -1,    85,    -1,    87,    -1,    -1,    -1,    -1,    -1,   149,
      -1,    -1,    -1,   153,    -1,    -1,    -1,   157,   158,    -1,
      -1,   161,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   178,   123,
     124,    -1,   126,    -1,    -1,   185,   130,   131,    -1,   133,
      -1,    -1,   136,   137,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   149,    -1,    -1,    -1,   153,
      -1,    -1,    -1,   157,   158,    -1,    -1,   161,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    46,    47,    48,
      49,    50,    51,    52,   178,    -1,    -1,    56,    -1,   183,
      59,    60,    61,    62,    63,    64,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    78,
      -1,    -1,    -1,    -1,    -1,    -1,    85,    -1,    87,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    97,    98,
      -1,    -1,    -1,    46,    47,    48,    49,    50,    51,    52,
      -1,    -1,    -1,    56,    -1,    -1,    59,    60,    61,    62,
      63,    64,    -1,    -1,   123,   124,    -1,   126,    -1,    -1,
      -1,   130,   131,    -1,   133,    78,    -1,   136,   137,    -1,
      -1,    -1,    85,    -1,    87,    -1,    -1,    -1,    -1,    -1,
     149,    -1,    -1,    -1,   153,    -1,    -1,    -1,   157,   158,
      -1,    -1,   161,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   178,
     123,   124,   181,   126,    -1,    -1,    -1,   130,   131,    -1,
     133,    -1,    -1,   136,   137,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   149,    -1,    -1,    -1,
     153,    -1,    -1,    -1,   157,   158,    -1,    -1,   161,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    46,    47,    48,    49,
      50,    51,    52,    -1,    -1,   178,    56,    -1,   181,    59,
      60,    61,    62,    63,    64,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,
      -1,    -1,    -1,    -1,    -1,    85,    -1,    87,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    97,    98,    -1,
      -1,    -1,    46,    47,    48,    49,    50,    51,    52,    -1,
      -1,    -1,    56,    -1,    -1,    59,    60,    61,    62,    63,
      64,    -1,    -1,   123,   124,    -1,   126,    -1,    -1,    -1,
     130,   131,    -1,   133,    78,    -1,   136,   137,    -1,    -1,
      -1,    85,    -1,    87,    -1,    -1,    -1,    -1,    -1,   149,
      -1,    -1,    -1,   153,    -1,    99,    -1,   157,   158,    -1,
      -1,   161,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   121,   178,   123,
     124,    -1,   126,    -1,    -1,    -1,   130,   131,    -1,   133,
      -1,    -1,   136,   137,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   149,    -1,    -1,    -1,   153,
      -1,    -1,    -1,   157,   158,    -1,    -1,   161,    -1,    -1,
      -1,    -1,    46,    47,    48,    49,    50,    51,    52,    -1,
      -1,    -1,    56,    -1,   178,    59,    60,    61,    62,    63,
      64,    65,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    78,    -1,    -1,    -1,    -1,    -1,
      -1,    85,    -1,    87,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    46,    47,
      48,    49,    50,    51,    52,    -1,    -1,    -1,    56,    -1,
      -1,    59,    60,    61,    62,    63,    64,    -1,    -1,   123,
     124,    -1,   126,    -1,    -1,    -1,   130,   131,    -1,   133,
      78,    -1,   136,   137,    -1,    -1,    -1,    85,    -1,    87,
      -1,    -1,    -1,    -1,    -1,   149,    -1,    -1,    -1,   153,
      -1,    -1,    -1,   157,   158,    -1,    -1,   161,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   178,   123,   124,    -1,   126,    -1,
      -1,    -1,   130,   131,    -1,   133,    -1,    -1,   136,   137,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   149,    -1,    -1,    -1,   153,    -1,    -1,    -1,   157,
     158,    -1,    -1,   161,    -1,    -1,    -1,    -1,    46,    47,
      48,    49,    50,    51,    52,    -1,    -1,    -1,    56,    -1,
     178,    59,    60,    61,    62,    63,    64,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      78,    -1,    -1,    -1,    -1,    -1,    -1,    85,    -1,    87,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   123,   124,    -1,   126,    -1,
      -1,    -1,   130,   131,    -1,   133,    -1,    -1,   136,   137,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   149,    -1,    -1,    -1,   153,    -1,    -1,    -1,   157,
     158,    -1,    -1,   161,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     178
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     1,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    21,    22,    23,    24,
      25,    28,    29,    31,    32,    46,    47,    48,    50,    51,
      59,    75,    76,    78,    79,    80,    81,    82,    84,    89,
      90,    91,    92,    93,    94,    95,    96,   119,   120,   122,
     127,   128,   129,   135,   139,   140,   141,   142,   143,   147,
     148,   150,   151,   152,   154,   155,   159,   160,   162,   163,
     164,   165,   166,   167,   168,   169,   170,   171,   172,   173,
     174,   175,   176,   177,   179,   180,   192,   193,   194,   195,
     196,   197,   202,   203,   204,   205,   206,   209,   210,   215,
     216,   217,   218,   219,   220,   221,   224,   225,   226,   227,
     229,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,   244,   255,   256,   257,   258,   259,   260,
     264,   265,   278,   279,   280,   281,   283,   284,   285,   292,
     293,   294,   298,   300,   301,   302,   304,   306,   307,   308,
     309,   311,   312,   313,   314,   315,   317,   318,   320,   321,
     322,   323,   324,   325,   326,   328,   332,   333,   334,    32,
      74,   286,   286,   132,   282,   286,   286,   286,   286,    19,
      62,    64,   327,    46,    47,    48,    49,    50,    51,    52,
      56,    59,    60,    61,    62,    63,    64,    78,    85,    87,
      97,    98,   123,   124,   126,   130,   131,   133,   136,   137,
     149,   153,   157,   158,   161,   178,   181,   261,   262,   338,
     339,   245,   339,    19,    20,    45,    58,   207,    28,    45,
     208,   339,   339,    28,    77,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   111,   114,   266,    50,   195,
      52,    50,    51,   184,    85,   339,    78,   121,   121,   123,
     124,   299,   130,   131,   133,   134,   305,   130,   138,   303,
     136,   138,   296,   121,   158,   296,   296,    78,   132,   310,
     138,   144,   145,   297,    78,   121,   149,   121,   125,   295,
     121,   316,   153,   296,   121,   319,   156,   157,   158,    26,
      66,    26,   228,    26,   231,   231,    78,    65,   228,   228,
      12,     6,   121,    78,   121,   124,   178,    42,     0,   194,
      42,   180,     5,   181,   197,   211,   213,   214,   244,   255,
     256,   257,   258,   259,   334,   181,   212,   197,   255,   256,
     257,   258,   259,    78,   339,   196,   196,   264,   185,   223,
     248,   249,   250,   197,   251,   252,   336,   337,   339,    63,
     291,   337,   251,   337,    87,    88,   199,   200,   201,    86,
     223,   248,    36,    37,    58,    78,   110,   121,   287,   288,
     289,   287,    44,   287,   287,   287,   287,    49,    78,   190,
      60,    61,   339,   339,   181,   339,   182,   184,   185,   183,
      58,   339,    26,    30,   339,    78,   184,   184,   184,   184,
      78,   100,   101,   102,   103,   105,   106,   107,   108,   109,
     110,   114,   274,   184,   184,   184,   184,   184,   112,   113,
     184,    33,    34,    35,    78,   267,   180,    53,    55,   329,
     330,   331,    78,    78,   121,   286,   121,   121,   296,   121,
     132,   296,   121,   296,   296,    78,   132,   138,    78,   137,
     296,    78,   296,    78,    78,   297,   121,   121,   182,   296,
     121,    78,   161,   339,    78,   138,   230,   230,   183,   339,
      78,    78,   121,   121,   121,   121,    42,    78,    78,   182,
     186,   197,   248,    26,   222,   249,   144,   146,   335,    65,
     183,   339,    26,   335,    88,    78,    26,   185,   185,   185,
     185,    33,   288,   282,   231,   184,   184,   184,    97,    98,
     339,   184,   261,    99,   121,   263,   339,    41,    78,    99,
     121,   181,   246,   247,   339,   184,    78,    83,   277,   277,
     277,   277,   183,   184,   184,   184,   184,   184,   184,   184,
     184,   184,   184,   184,   273,   277,   277,   275,   277,   275,
     277,   184,   184,   277,    78,    78,    54,    56,   182,   287,
     295,   296,   296,   121,   296,   296,    78,   296,   126,   182,
     121,    66,   184,    26,   183,    78,    78,   184,   181,   186,
     186,   251,   185,   339,   185,   339,   339,   339,   251,   185,
     253,    78,     9,   198,   251,   121,    38,    39,    40,   290,
      78,    78,   121,   230,    78,   263,   263,   339,   339,   184,
     263,    78,   181,   182,   186,   183,   276,   277,   273,   273,
     273,   273,    78,   276,   277,   277,   277,   277,   277,   277,
     275,   275,   277,   277,    33,   182,   273,   273,   273,   273,
     273,   187,   187,   273,    57,   331,   296,   121,   121,   182,
     161,    78,   339,   339,    78,   121,    78,   339,   339,    65,
     339,   254,   339,   252,   198,    19,   186,   186,    78,    78,
     121,   186,   186,   184,   184,   263,    78,   247,   339,   273,
      33,    33,    33,    33,   184,   274,    33,    33,    33,    33,
      33,   100,   114,   115,   116,   118,   268,   269,    62,   270,
      33,    78,   184,   186,   186,   339,   182,   186,    19,   337,
     186,   186,   186,   263,   263,    33,   276,   185,   185,   185,
     185,   185,   188,   182,   185,   188,    78,   339,   337,   277,
     277,   117,   186,   186,    78,   271,   272,    33,   268,   275,
      33,   186,   186,   184,   184,   186,   182,   186,   189,   277,
     277,   271,   275,   186,   186
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

  case 261:

    { SET_BASIC_STMT(OBPROXY_T_SELECT); ;}
    break;

  case 263:

    { SET_BASIC_STMT(OBPROXY_T_UPDATE); ;}
    break;

  case 265:

    { SET_BASIC_STMT(OBPROXY_T_DELETE); ;}
    break;

  case 267:

    { SET_BASIC_STMT(OBPROXY_T_INSERT); ;}
    break;

  case 272:

    { SET_BASIC_STMT(OBPROXY_T_REPLACE); ;}
    break;

  case 274:

    { SET_BASIC_STMT(OBPROXY_T_MERGE); ;}
    break;

  case 281:

    { result->query_timeout_ = (yyvsp[(3) - (4)].num); ;}
    break;

  case 282:

    {;}
    break;

  case 284:

    {
      add_hint_index(result->dbmesh_route_info_, (yyvsp[(3) - (5)].str));
      result->dbmesh_route_info_.index_count_++;
    ;}
    break;

  case 285:

    { result->has_trace_log_hint_ = true; ;}
    break;

  case 286:

    {;}
    break;

  case 287:

    {;}
    break;

  case 288:

    {;}
    break;

  case 289:

    {;}
    break;

  case 290:

    {;}
    break;

  case 291:

    {;}
    break;

  case 292:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_WEAK); ;}
    break;

  case 293:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_STRONG); ;}
    break;

  case 294:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_FROZEN); ;}
    break;

  case 297:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_WARNINGS; ;}
    break;

  case 298:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_ERRORS; ;}
    break;

  case 299:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 300:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 324:

    {;}
    break;

  case 325:

    {
;}
    break;

  case 326:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 327:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 328:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 329:

    {;}
    break;

  case 330:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 331:

    {;}
    break;

  case 332:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 334:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 335:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 336:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 337:

    {;}
    break;

  case 338:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 339:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 340:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 342:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 343:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 344:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 345:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 346:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 348:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 349:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 350:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 351:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 352:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 353:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 354:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 355:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 356:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 357:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_READ_STALE); ;}
    break;

  case 358:

    {;}
    break;

  case 359:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 360:

    {;}
    break;

  case 361:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 362:

    {;}
    break;

  case 364:

    {;}
    break;

  case 365:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 366:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 367:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 368:

    {;}
    break;

  case 369:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 370:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 371:

    {;}
    break;

  case 372:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 373:

    {;}
    break;

  case 374:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 375:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 376:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); SET_ICMD_ONE_ID((yyvsp[(3) - (3)].num)); ;}
    break;

  case 377:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 378:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 380:

    {;}
    break;

  case 381:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 382:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 383:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 384:

    {;}
    break;

  case 385:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 387:

    {;}
    break;

  case 388:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 389:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 390:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 391:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 392:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 393:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 394:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 395:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 396:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 397:

    {;}
    break;

  case 398:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 399:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 400:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 401:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 402:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 403:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 404:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 407:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 408:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 409:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 410:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 417:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 418:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 420:

    {;}
    break;

  case 421:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 422:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 423:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 424:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 425:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT();
                                                ;}
    break;

  case 426:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 427:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 428:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 429:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 430:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 431:

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

