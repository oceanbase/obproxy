
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
    /* insert into ... select also have alias name */ \
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
     STATUS = 275,
     UNIQUE = 276,
     STOP_DDL_TASK = 277,
     RETRY_DDL_TASK = 278,
     GRANT = 279,
     REVOKE = 280,
     ANALYZE = 281,
     PURGE = 282,
     COMMENT = 283,
     FROM = 284,
     DUAL = 285,
     PREPARE = 286,
     EXECUTE = 287,
     USING = 288,
     DEALLOCATE = 289,
     SELECT_HINT_BEGIN = 290,
     UPDATE_HINT_BEGIN = 291,
     DELETE_HINT_BEGIN = 292,
     INSERT_HINT_BEGIN = 293,
     REPLACE_HINT_BEGIN = 294,
     MERGE_HINT_BEGIN = 295,
     HINT_END = 296,
     COMMENT_BEGIN = 297,
     COMMENT_END = 298,
     ROUTE_TABLE = 299,
     ROUTE_PART_KEY = 300,
     QUERY_TIMEOUT = 301,
     READ_CONSISTENCY = 302,
     WEAK = 303,
     STRONG = 304,
     FROZEN = 305,
     PLACE_HOLDER = 306,
     END_P = 307,
     ERROR = 308,
     WHEN = 309,
     FLASHBACK = 310,
     AUDIT = 311,
     NOAUDIT = 312,
     BEGI = 313,
     START = 314,
     TRANSACTION = 315,
     READ = 316,
     ONLY = 317,
     WITH = 318,
     CONSISTENT = 319,
     SNAPSHOT = 320,
     INDEX = 321,
     XA = 322,
     WARNINGS = 323,
     ERRORS = 324,
     TRACE = 325,
     QUICK = 326,
     COUNT = 327,
     AS = 328,
     WHERE = 329,
     VALUES = 330,
     ORDER = 331,
     GROUP = 332,
     HAVING = 333,
     INTO = 334,
     UNION = 335,
     FOR = 336,
     TX_READ_ONLY = 337,
     SELECT_OBPROXY_ROUTE_ADDR = 338,
     SET_OBPROXY_ROUTE_ADDR = 339,
     NAME_OB_DOT = 340,
     NAME_OB = 341,
     EXPLAIN = 342,
     DESC = 343,
     DESCRIBE = 344,
     NAME_STR = 345,
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
     DBP_COMMENT = 367,
     ROUTE_TAG = 368,
     SYS_TAG = 369,
     TABLE_NAME = 370,
     SCAN_ALL = 371,
     STICKY_SESSION = 372,
     PARALL = 373,
     SHARD_KEY = 374,
     INT_NUM = 375,
     SHOW_PROXYNET = 376,
     THREAD = 377,
     CONNECTION = 378,
     LIMIT = 379,
     OFFSET = 380,
     SHOW_PROCESSLIST = 381,
     SHOW_PROXYSESSION = 382,
     SHOW_GLOBALSESSION = 383,
     ATTRIBUTE = 384,
     VARIABLES = 385,
     ALL = 386,
     STAT = 387,
     READ_STALE = 388,
     SHOW_PROXYCONFIG = 389,
     DIFF = 390,
     USER = 391,
     LIKE = 392,
     SHOW_PROXYSM = 393,
     SHOW_PROXYCLUSTER = 394,
     SHOW_PROXYRESOURCE = 395,
     SHOW_PROXYCONGESTION = 396,
     SHOW_PROXYROUTE = 397,
     PARTITION = 398,
     ROUTINE = 399,
     SUBPARTITION = 400,
     SHOW_PROXYVIP = 401,
     SHOW_PROXYMEMORY = 402,
     OBJPOOL = 403,
     SHOW_SQLAUDIT = 404,
     SHOW_WARNLOG = 405,
     SHOW_PROXYSTAT = 406,
     REFRESH = 407,
     SHOW_PROXYTRACE = 408,
     SHOW_PROXYINFO = 409,
     BINARY = 410,
     UPGRADE = 411,
     IDC = 412,
     SHOW_ELASTIC_ID = 413,
     SHOW_TOPOLOGY = 414,
     GROUP_NAME = 415,
     SHOW_DB_VERSION = 416,
     SHOW_DATABASES = 417,
     SHOW_TABLES = 418,
     SHOW_FULL_TABLES = 419,
     SELECT_DATABASE = 420,
     SELECT_PROXY_STATUS = 421,
     SHOW_CREATE_TABLE = 422,
     SELECT_PROXY_VERSION = 423,
     SHOW_COLUMNS = 424,
     SHOW_INDEX = 425,
     ALTER_PROXYCONFIG = 426,
     ALTER_PROXYRESOURCE = 427,
     PING_PROXY = 428,
     KILL_PROXYSESSION = 429,
     KILL_GLOBALSESSION = 430,
     KILL = 431,
     QUERY = 432,
     SHOW_MASTER_STATUS = 433,
     SHOW_BINARY_LOGS = 434,
     SHOW_BINLOG_EVENTS = 435,
     PURGE_BINARY_LOGS = 436,
     RESET_MASTER = 437,
     SHOW_BINLOG_SERVER_FOR_TENANT = 438
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
#define YYFINAL  322
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   1425

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  195
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  140
/* YYNRULES -- Number of rules.  */
#define YYNRULES  444
/* YYNRULES -- Number of states.  */
#define YYNSTATES  732

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   438

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,   193,     2,     2,     2,     2,
     188,   189,   194,     2,   186,     2,   190,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   184,
       2,   187,     2,     2,   185,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   191,     2,   192,     2,     2,     2,     2,
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
     175,   176,   177,   178,   179,   180,   181,   182,   183
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
      90,    93,    96,    99,   102,   105,   108,   110,   112,   115,
     117,   119,   121,   123,   125,   127,   128,   130,   132,   135,
     138,   141,   143,   145,   147,   149,   151,   153,   155,   157,
     160,   165,   168,   170,   172,   176,   179,   183,   186,   189,
     193,   197,   199,   201,   203,   205,   207,   209,   211,   213,
     215,   218,   220,   222,   224,   225,   228,   229,   231,   234,
     240,   244,   246,   250,   252,   254,   256,   258,   260,   262,
     264,   266,   268,   270,   272,   274,   276,   278,   280,   283,
     286,   290,   296,   300,   306,   307,   310,   311,   314,   318,
     322,   328,   330,   332,   336,   342,   350,   352,   356,   358,
     362,   364,   366,   368,   370,   372,   374,   380,   382,   386,
     392,   393,   395,   399,   401,   403,   405,   408,   412,   414,
     416,   419,   421,   424,   428,   432,   434,   436,   438,   439,
     443,   445,   449,   453,   459,   462,   465,   470,   473,   476,
     480,   482,   487,   494,   499,   505,   512,   517,   521,   523,
     525,   527,   529,   532,   536,   542,   549,   556,   563,   570,
     577,   585,   592,   599,   606,   613,   622,   631,   638,   639,
     642,   645,   648,   650,   654,   656,   661,   666,   670,   677,
     681,   686,   691,   698,   702,   704,   708,   709,   713,   717,
     721,   725,   729,   733,   737,   741,   745,   749,   753,   759,
     763,   764,   766,   767,   769,   771,   773,   775,   778,   780,
     783,   785,   788,   791,   795,   796,   798,   801,   803,   806,
     808,   811,   814,   815,   818,   823,   825,   830,   836,   838,
     843,   848,   854,   855,   857,   859,   861,   862,   864,   868,
     872,   875,   881,   883,   885,   887,   889,   891,   893,   895,
     897,   899,   901,   903,   905,   907,   909,   911,   913,   915,
     917,   919,   921,   923,   925,   927,   929,   931,   933,   935,
     937,   939,   940,   943,   948,   953,   954,   957,   958,   961,
     964,   966,   968,   972,   975,   979,   984,   986,   989,   990,
     993,   997,  1000,  1003,  1006,  1007,  1010,  1014,  1017,  1021,
    1024,  1028,  1032,  1037,  1040,  1042,  1045,  1048,  1052,  1055,
    1058,  1059,  1061,  1063,  1066,  1069,  1073,  1076,  1078,  1081,
    1083,  1086,  1089,  1092,  1095,  1096,  1098,  1102,  1108,  1111,
    1115,  1118,  1119,  1121,  1124,  1127,  1130,  1133,  1138,  1144,
    1150,  1154,  1156,  1159,  1163,  1167,  1170,  1173,  1177,  1181,
    1182,  1185,  1187,  1191,  1195,  1199,  1200,  1202,  1204,  1208,
    1211,  1215,  1218,  1221,  1223,  1224,  1227,  1232,  1235,  1240,
    1243,  1245,  1249,  1252,  1257,  1261,  1267,  1269,  1271,  1273,
    1275,  1277,  1279,  1281,  1283,  1285,  1287,  1289,  1291,  1293,
    1295,  1297,  1299,  1301,  1303,  1305,  1307,  1309,  1311,  1313,
    1315,  1317,  1319,  1321,  1323
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     196,     0,    -1,   197,    -1,     1,    -1,   198,    -1,   197,
     198,    -1,   199,    52,    -1,   199,   184,    -1,   199,   184,
      52,    -1,   184,    -1,   184,    52,    -1,    58,   199,   184,
      -1,   200,    -1,   261,   200,    -1,   201,    -1,   252,    -1,
     257,    -1,   253,    -1,   254,    -1,   255,    -1,   202,    -1,
     323,    -1,   287,    -1,   222,    -1,   288,    -1,   327,    -1,
     328,    -1,   235,    -1,   236,    -1,   237,    -1,   238,    -1,
     239,    -1,   240,    -1,   241,    -1,   203,    -1,   214,    -1,
     256,    -1,   289,    -1,   329,    -1,   275,   219,   218,    -1,
     216,   201,    -1,   216,   252,    -1,   216,   254,    -1,   216,
     255,    -1,   216,   253,    -1,   216,   256,    -1,   204,    -1,
     215,    -1,    14,   205,    -1,    15,    -1,    16,    -1,    17,
      -1,    18,    -1,   206,    -1,   207,    -1,    -1,    19,    -1,
      66,    -1,    21,    66,    -1,    22,   120,    -1,    23,   120,
      -1,   201,    -1,   252,    -1,   253,    -1,   255,    -1,   254,
      -1,   329,    -1,   241,    -1,   256,    -1,   185,    86,    -1,
     209,   186,   185,    86,    -1,   185,    86,    -1,   210,    -1,
     208,    -1,    31,   334,    29,    -1,    32,   334,    -1,    32,
     334,    33,    -1,   212,   211,    -1,   213,   209,    -1,    15,
      31,   334,    -1,    34,    31,   334,    -1,    24,    -1,    25,
      -1,    26,    -1,    27,    -1,    55,    -1,    28,    -1,    56,
      -1,    57,    -1,   217,    -1,   217,    86,    -1,    87,    -1,
      88,    -1,    89,    -1,    -1,    29,   248,    -1,    -1,   245,
      -1,     5,    82,    -1,     5,    82,   219,    29,   248,    -1,
       5,    82,   245,    -1,   168,    -1,   168,    73,   334,    -1,
     220,    -1,   221,    -1,   233,    -1,   234,    -1,   223,    -1,
     231,    -1,   232,    -1,   230,    -1,   165,    -1,   166,    -1,
     162,    -1,   228,    -1,   229,    -1,   224,    -1,   225,    -1,
     167,   242,    -1,   217,   334,    -1,   169,    29,    86,    -1,
     169,    29,    86,    29,    86,    -1,   170,    29,    86,    -1,
     170,    29,    86,    29,    86,    -1,    -1,   137,    86,    -1,
      -1,    29,    86,    -1,   163,   227,   226,    -1,   164,   227,
     226,    -1,    11,    19,    20,   227,   226,    -1,   161,    -1,
     158,    -1,   158,    29,    86,    -1,   158,    74,   160,   187,
      86,    -1,   158,    29,    86,    74,   160,   187,    86,    -1,
     159,    -1,   159,    29,    86,    -1,    83,    -1,    84,   187,
     120,    -1,    93,    -1,    94,    -1,    95,    -1,    96,    -1,
      97,    -1,    98,    -1,    13,   242,   188,   243,   189,    -1,
     334,    -1,   334,   190,   334,    -1,   334,   190,   334,   190,
     334,    -1,    -1,   244,    -1,   243,   186,   244,    -1,    86,
      -1,   120,    -1,   101,    -1,   185,    86,    -1,   185,   185,
      86,    -1,    51,    -1,   246,    -1,   245,   246,    -1,   247,
      -1,   188,   189,    -1,   188,   201,   189,    -1,   188,   245,
     189,    -1,   331,    -1,   249,    -1,   201,    -1,    -1,   188,
     251,   189,    -1,    86,    -1,   251,   186,    86,    -1,   278,
     332,   330,    -1,   278,   332,   330,   250,   249,    -1,   280,
     248,    -1,   276,   248,    -1,   277,   286,    29,   248,    -1,
     281,   332,    -1,    12,   258,    -1,   259,   186,   258,    -1,
     259,    -1,   185,    86,   187,   260,    -1,   185,   185,    99,
      86,   187,   260,    -1,    99,    86,   187,   260,    -1,   185,
     185,    86,   187,   260,    -1,   185,   185,   100,    86,   187,
     260,    -1,   100,    86,   187,   260,    -1,    86,   187,   260,
      -1,    86,    -1,   120,    -1,   101,    -1,   262,    -1,   262,
     261,    -1,    42,   263,    43,    -1,    42,   106,   271,   270,
      43,    -1,    42,   103,   187,   274,   270,    43,    -1,    42,
     115,   187,   274,   270,    43,    -1,    42,   102,   187,   274,
     270,    43,    -1,    42,   104,   187,   274,   270,    43,    -1,
      42,   105,   187,   274,   270,    43,    -1,    42,    85,    86,
     187,   273,   270,    43,    -1,    42,   109,   187,   272,   270,
      43,    -1,    42,   110,   187,   272,   270,    43,    -1,    42,
     107,   187,   274,   270,    43,    -1,    42,   108,   187,   274,
     270,    43,    -1,    42,   112,   113,   187,   191,   265,   192,
      43,    -1,    42,   112,   114,   187,   191,   267,   192,    43,
      -1,    42,   111,   187,   274,   270,    43,    -1,    -1,   263,
     264,    -1,    44,    86,    -1,    45,    86,    -1,    86,    -1,
     266,   186,   265,    -1,   266,    -1,   102,   188,   274,   189,
      -1,   115,   188,   274,   189,    -1,   116,   188,   189,    -1,
     116,   188,   118,   187,   274,   189,    -1,   117,   188,   189,
      -1,   119,   188,   268,   189,    -1,    70,   188,   272,   189,
      -1,    70,   188,   272,   193,   272,   189,    -1,   269,   186,
     268,    -1,   269,    -1,    86,   187,   274,    -1,    -1,   270,
     186,   271,    -1,   102,   187,   274,    -1,   103,   187,   274,
      -1,   115,   187,   274,    -1,   104,   187,   274,    -1,   105,
     187,   274,    -1,   109,   187,   272,    -1,   110,   187,   272,
      -1,   107,   187,   274,    -1,   108,   187,   274,    -1,   111,
     187,   274,    -1,    86,   190,    86,   187,   273,    -1,    86,
     187,   273,    -1,    -1,   274,    -1,    -1,   274,    -1,    86,
      -1,    90,    -1,     5,    -1,    35,   282,    -1,     8,    -1,
      36,   282,    -1,     6,    -1,    37,   282,    -1,     7,   279,
      -1,    38,   282,   279,    -1,    -1,   131,    -1,   131,    54,
      -1,     9,    -1,    39,   282,    -1,    10,    -1,    40,   282,
      -1,   283,    41,    -1,    -1,   284,   283,    -1,    46,   188,
     120,   189,    -1,   120,    -1,    47,   188,   285,   189,    -1,
      66,   188,    86,    86,   189,    -1,    86,    -1,    86,   188,
     120,   189,    -1,    86,   188,    86,   189,    -1,    86,   188,
      86,    86,   189,    -1,    -1,    48,    -1,    49,    -1,    50,
      -1,    -1,    71,    -1,    11,   322,    68,    -1,    11,   322,
      69,    -1,    11,    70,    -1,    11,    70,    86,   187,    86,
      -1,   293,    -1,   295,    -1,   296,    -1,   299,    -1,   297,
      -1,   301,    -1,   302,    -1,   303,    -1,   304,    -1,   306,
      -1,   307,    -1,   308,    -1,   309,    -1,   310,    -1,   312,
      -1,   313,    -1,   315,    -1,   316,    -1,   317,    -1,   318,
      -1,   319,    -1,   320,    -1,   321,    -1,   178,    -1,   179,
      -1,   180,    -1,   181,    -1,   182,    -1,   183,    -1,    -1,
     124,   120,    -1,   124,   120,   186,   120,    -1,   124,   120,
     125,   120,    -1,    -1,   137,    86,    -1,    -1,   137,    86,
      -1,   121,   294,    -1,   122,    -1,   123,    -1,   123,   120,
     290,    -1,   134,   291,    -1,   134,   135,   291,    -1,   134,
     135,   136,   291,    -1,   126,    -1,   128,   298,    -1,    -1,
     129,    86,    -1,   129,   137,    86,    -1,   129,   131,    -1,
     137,    86,    -1,   127,   300,    -1,    -1,   129,   291,    -1,
     129,   120,   291,    -1,   132,   291,    -1,   132,   120,   291,
      -1,   130,   291,    -1,   130,   120,   291,    -1,   130,   131,
     291,    -1,   130,   131,   120,   291,    -1,   133,   291,    -1,
     138,    -1,   138,   120,    -1,   139,   291,    -1,   139,   157,
     291,    -1,   140,   291,    -1,   141,   305,    -1,    -1,    86,
      -1,   131,    -1,   131,    86,    -1,   142,   292,    -1,   142,
     144,   292,    -1,   142,   143,    -1,   146,    -1,   146,    86,
      -1,   147,    -1,   147,   148,    -1,   149,   290,    -1,   149,
     120,    -1,   150,   311,    -1,    -1,   120,    -1,   120,   186,
     120,    -1,   120,   186,   120,   186,    86,    -1,   151,   291,
      -1,   151,   152,   291,    -1,   153,   314,    -1,    -1,   120,
      -1,   120,   120,    -1,   154,   155,    -1,   154,   156,    -1,
     154,   157,    -1,   171,    12,    86,   187,    -1,   171,    12,
      86,   187,    86,    -1,   171,    12,    86,   187,   120,    -1,
     172,     6,    86,    -1,   173,    -1,   174,   120,    -1,   174,
     120,   120,    -1,   175,    86,   120,    -1,   175,    86,    -1,
     176,   120,    -1,   176,   123,   120,    -1,   176,   177,   120,
      -1,    -1,    72,   194,    -1,    58,    -1,    59,    60,   324,
      -1,    67,    58,    86,    -1,    67,    59,    86,    -1,    -1,
     325,    -1,   326,    -1,   325,   186,   326,    -1,    61,    62,
      -1,    63,    64,    65,    -1,    91,    86,    -1,    92,    86,
      -1,    86,    -1,    -1,   145,    86,    -1,   145,   188,    86,
     189,    -1,   143,    86,    -1,   143,   188,    86,   189,    -1,
     332,   330,    -1,   334,    -1,   334,   190,   334,    -1,   334,
     334,    -1,   334,   190,   334,   334,    -1,   334,    73,   334,
      -1,   334,   190,   334,    73,   334,    -1,    59,    -1,    67,
      -1,    58,    -1,    60,    -1,    64,    -1,    69,    -1,    68,
      -1,    72,    -1,    71,    -1,    70,    -1,   122,    -1,   123,
      -1,   125,    -1,   129,    -1,   130,    -1,   132,    -1,   135,
      -1,   136,    -1,   148,    -1,   152,    -1,   156,    -1,   157,
      -1,   177,    -1,   160,    -1,    55,    -1,    56,    -1,    57,
      -1,    86,    -1,   333,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   335,   335,   336,   338,   339,   341,   342,   343,   344,
     345,   346,   348,   349,   351,   352,   353,   354,   355,   356,
     357,   358,   359,   360,   361,   362,   363,   364,   365,   366,
     367,   368,   369,   370,   371,   372,   373,   374,   375,   377,
     379,   380,   381,   382,   383,   384,   386,   387,   389,   390,
     391,   392,   393,   394,   395,   397,   398,   399,   400,   402,
     408,   414,   415,   416,   417,   418,   419,   420,   421,   423,
     430,   438,   446,   447,   450,   456,   461,   467,   470,   473,
     478,   484,   485,   486,   487,   488,   489,   490,   491,   493,
     494,   496,   497,   498,   500,   501,   503,   504,   506,   507,
     508,   510,   511,   513,   514,   515,   516,   517,   519,   520,
     521,   522,   523,   524,   525,   526,   527,   528,   529,   530,
     537,   542,   549,   554,   561,   562,   564,   565,   567,   571,
     576,   581,   583,   584,   589,   594,   601,   602,   608,   611,
     617,   618,   619,   620,   621,   622,   625,   627,   631,   636,
     644,   647,   652,   657,   662,   667,   672,   677,   682,   690,
     691,   693,   695,   696,   697,   699,   700,   702,   704,   705,
     707,   708,   710,   714,   715,   716,   717,   718,   723,   725,
     726,   728,   732,   736,   740,   744,   748,   752,   756,   761,
     766,   772,   773,   775,   776,   777,   778,   779,   780,   781,
     782,   788,   789,   790,   791,   792,   793,   794,   795,   796,
     798,   799,   800,   802,   803,   805,   810,   815,   816,   817,
     818,   820,   821,   823,   824,   826,   834,   835,   837,   838,
     839,   840,   841,   842,   843,   844,   845,   846,   847,   853,
     855,   856,   858,   859,   861,   862,   864,   865,   866,   867,
     868,   869,   870,   871,   873,   874,   875,   877,   878,   879,
     880,   881,   882,   883,   885,   886,   887,   888,   893,   894,
     895,   896,   898,   899,   900,   901,   903,   904,   907,   908,
     909,   910,   913,   914,   915,   916,   917,   918,   919,   920,
     921,   922,   923,   924,   925,   926,   927,   928,   929,   930,
     931,   932,   933,   934,   935,   937,   938,   939,   940,   941,
     942,   947,   949,   953,   958,   966,   967,   971,   972,   975,
     977,   978,   979,   983,   984,   985,   989,   991,   993,   994,
     995,   996,   997,  1000,  1002,  1003,  1004,  1005,  1006,  1007,
    1008,  1009,  1010,  1011,  1015,  1016,  1020,  1021,  1026,  1029,
    1031,  1032,  1033,  1034,  1038,  1039,  1040,  1044,  1045,  1049,
    1050,  1054,  1055,  1058,  1060,  1061,  1062,  1063,  1067,  1068,
    1071,  1073,  1074,  1075,  1079,  1080,  1081,  1085,  1086,  1087,
    1091,  1095,  1099,  1100,  1104,  1105,  1109,  1110,  1111,  1114,
    1115,  1118,  1122,  1123,  1124,  1126,  1127,  1129,  1130,  1133,
    1134,  1137,  1143,  1146,  1148,  1149,  1150,  1151,  1152,  1154,
    1159,  1162,  1166,  1170,  1175,  1179,  1185,  1186,  1187,  1188,
    1189,  1190,  1191,  1192,  1193,  1194,  1195,  1196,  1197,  1198,
    1199,  1200,  1201,  1202,  1203,  1204,  1205,  1206,  1207,  1208,
    1209,  1210,  1211,  1213,  1214
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
  "RENAME", "TABLE", "STATUS", "UNIQUE", "STOP_DDL_TASK", "RETRY_DDL_TASK",
  "GRANT", "REVOKE", "ANALYZE", "PURGE", "COMMENT", "FROM", "DUAL",
  "PREPARE", "EXECUTE", "USING", "DEALLOCATE", "SELECT_HINT_BEGIN",
  "UPDATE_HINT_BEGIN", "DELETE_HINT_BEGIN", "INSERT_HINT_BEGIN",
  "REPLACE_HINT_BEGIN", "MERGE_HINT_BEGIN", "HINT_END", "COMMENT_BEGIN",
  "COMMENT_END", "ROUTE_TABLE", "ROUTE_PART_KEY", "QUERY_TIMEOUT",
  "READ_CONSISTENCY", "WEAK", "STRONG", "FROZEN", "PLACE_HOLDER", "END_P",
  "ERROR", "WHEN", "FLASHBACK", "AUDIT", "NOAUDIT", "BEGI", "START",
  "TRANSACTION", "READ", "ONLY", "WITH", "CONSISTENT", "SNAPSHOT", "INDEX",
  "XA", "WARNINGS", "ERRORS", "TRACE", "QUICK", "COUNT", "AS", "WHERE",
  "VALUES", "ORDER", "GROUP", "HAVING", "INTO", "UNION", "FOR",
  "TX_READ_ONLY", "SELECT_OBPROXY_ROUTE_ADDR", "SET_OBPROXY_ROUTE_ADDR",
  "NAME_OB_DOT", "NAME_OB", "EXPLAIN", "DESC", "DESCRIBE", "NAME_STR",
  "USE", "HELP", "SET_NAMES", "SET_CHARSET", "SET_PASSWORD", "SET_DEFAULT",
  "SET_OB_READ_CONSISTENCY", "SET_TX_READ_ONLY", "GLOBAL", "SESSION",
  "NUMBER_VAL", "GROUP_ID", "TABLE_ID", "ELASTIC_ID", "TESTLOAD",
  "ODP_COMMENT", "TNT_ID", "DISASTER_STATUS", "TRACE_ID", "RPC_ID",
  "TARGET_DB_SERVER", "DBP_COMMENT", "ROUTE_TAG", "SYS_TAG", "TABLE_NAME",
  "SCAN_ALL", "STICKY_SESSION", "PARALL", "SHARD_KEY", "INT_NUM",
  "SHOW_PROXYNET", "THREAD", "CONNECTION", "LIMIT", "OFFSET",
  "SHOW_PROCESSLIST", "SHOW_PROXYSESSION", "SHOW_GLOBALSESSION",
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
  "SHOW_MASTER_STATUS", "SHOW_BINARY_LOGS", "SHOW_BINLOG_EVENTS",
  "PURGE_BINARY_LOGS", "RESET_MASTER", "SHOW_BINLOG_SERVER_FOR_TENANT",
  "';'", "'@'", "','", "'='", "'('", "')'", "'.'", "'{'", "'}'", "'#'",
  "'*'", "$accept", "root", "sql_stmts", "sql_stmt", "comment_stmt",
  "stmt", "select_stmt", "explain_stmt", "ddl_stmt", "mysql_ddl_stmt",
  "create_dll_expr", "stop_ddl_task_stmt", "retry_ddl_task_stmt",
  "text_ps_from_stmt", "text_ps_execute_using_var_list",
  "text_ps_prepare_var_list", "text_ps_prepare_args_stmt",
  "text_ps_prepare_stmt", "text_ps_execute_stmt", "text_ps_stmt",
  "oracle_ddl_stmt", "explain_or_desc_stmt", "explain_or_desc", "opt_from",
  "select_expr_list", "select_tx_read_only_stmt",
  "select_proxy_version_stmt", "hooked_stmt", "shard_special_stmt",
  "show_columns_stmt", "show_index_stmt", "opt_show_like", "opt_show_from",
  "show_tables_stmt", "show_table_status_stmt", "show_db_version_stmt",
  "show_es_id_stmt", "show_topology_stmt",
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
  "tracer_right_string_val", "name_right_string_val", "right_string_val",
  "select_with_opt_hint", "update_with_opt_hint", "delete_with_opt_hint",
  "insert_with_opt_hint", "insert_all_when", "replace_with_opt_hint",
  "merge_with_opt_hint", "hint_list_with_end", "hint_list", "hint",
  "opt_read_consistency", "opt_quick", "show_stmt", "icmd_stmt",
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
     435,   436,   437,   438,    59,    64,    44,    61,    40,    41,
      46,   123,   125,    35,    42
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   195,   196,   196,   197,   197,   198,   198,   198,   198,
     198,   198,   199,   199,   200,   200,   200,   200,   200,   200,
     200,   200,   200,   200,   200,   200,   200,   200,   200,   200,
     200,   200,   200,   200,   200,   200,   200,   200,   200,   201,
     202,   202,   202,   202,   202,   202,   203,   203,   204,   204,
     204,   204,   204,   204,   204,   205,   205,   205,   205,   206,
     207,   208,   208,   208,   208,   208,   208,   208,   208,   209,
     209,   210,   211,   211,   212,   213,   213,   214,   214,   214,
     214,   215,   215,   215,   215,   215,   215,   215,   215,   216,
     216,   217,   217,   217,   218,   218,   219,   219,   220,   220,
     220,   221,   221,   222,   222,   222,   222,   222,   223,   223,
     223,   223,   223,   223,   223,   223,   223,   223,   223,   223,
     224,   224,   225,   225,   226,   226,   227,   227,   228,   228,
     229,   230,   231,   231,   231,   231,   232,   232,   233,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   242,   242,
     243,   243,   243,   244,   244,   244,   244,   244,   244,   245,
     245,   246,   247,   247,   247,   248,   248,   249,   250,   250,
     251,   251,   252,   252,   253,   254,   255,   256,   257,   258,
     258,   259,   259,   259,   259,   259,   259,   259,   260,   260,
     260,   261,   261,   262,   262,   262,   262,   262,   262,   262,
     262,   262,   262,   262,   262,   262,   262,   262,   263,   263,
     264,   264,   264,   265,   265,   266,   266,   266,   266,   266,
     266,   267,   267,   268,   268,   269,   270,   270,   271,   271,
     271,   271,   271,   271,   271,   271,   271,   271,   271,   271,
     272,   272,   273,   273,   274,   274,   275,   275,   276,   276,
     277,   277,   278,   278,   279,   279,   279,   280,   280,   281,
     281,   282,   283,   283,   284,   284,   284,   284,   284,   284,
     284,   284,   285,   285,   285,   285,   286,   286,   287,   287,
     287,   287,   288,   288,   288,   288,   288,   288,   288,   288,
     288,   288,   288,   288,   288,   288,   288,   288,   288,   288,
     288,   288,   288,   288,   288,   289,   289,   289,   289,   289,
     289,   290,   290,   290,   290,   291,   291,   292,   292,   293,
     294,   294,   294,   295,   295,   295,   296,   297,   298,   298,
     298,   298,   298,   299,   300,   300,   300,   300,   300,   300,
     300,   300,   300,   300,   301,   301,   302,   302,   303,   304,
     305,   305,   305,   305,   306,   306,   306,   307,   307,   308,
     308,   309,   309,   310,   311,   311,   311,   311,   312,   312,
     313,   314,   314,   314,   315,   315,   315,   316,   316,   316,
     317,   318,   319,   319,   320,   320,   321,   321,   321,   322,
     322,   323,   323,   323,   323,   324,   324,   325,   325,   326,
     326,   327,   328,   329,   330,   330,   330,   330,   330,   331,
     332,   332,   332,   332,   332,   332,   333,   333,   333,   333,
     333,   333,   333,   333,   333,   333,   333,   333,   333,   333,
     333,   333,   333,   333,   333,   333,   333,   333,   333,   333,
     333,   333,   333,   334,   334
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     1,     1,     1,     2,     2,     2,     3,     1,
       2,     3,     1,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     3,
       2,     2,     2,     2,     2,     2,     1,     1,     2,     1,
       1,     1,     1,     1,     1,     0,     1,     1,     2,     2,
       2,     1,     1,     1,     1,     1,     1,     1,     1,     2,
       4,     2,     1,     1,     3,     2,     3,     2,     2,     3,
       3,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       2,     1,     1,     1,     0,     2,     0,     1,     2,     5,
       3,     1,     3,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     2,     2,
       3,     5,     3,     5,     0,     2,     0,     2,     3,     3,
       5,     1,     1,     3,     5,     7,     1,     3,     1,     3,
       1,     1,     1,     1,     1,     1,     5,     1,     3,     5,
       0,     1,     3,     1,     1,     1,     2,     3,     1,     1,
       2,     1,     2,     3,     3,     1,     1,     1,     0,     3,
       1,     3,     3,     5,     2,     2,     4,     2,     2,     3,
       1,     4,     6,     4,     5,     6,     4,     3,     1,     1,
       1,     1,     2,     3,     5,     6,     6,     6,     6,     6,
       7,     6,     6,     6,     6,     8,     8,     6,     0,     2,
       2,     2,     1,     3,     1,     4,     4,     3,     6,     3,
       4,     4,     6,     3,     1,     3,     0,     3,     3,     3,
       3,     3,     3,     3,     3,     3,     3,     3,     5,     3,
       0,     1,     0,     1,     1,     1,     1,     2,     1,     2,
       1,     2,     2,     3,     0,     1,     2,     1,     2,     1,
       2,     2,     0,     2,     4,     1,     4,     5,     1,     4,
       4,     5,     0,     1,     1,     1,     0,     1,     3,     3,
       2,     5,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     0,     2,     4,     4,     0,     2,     0,     2,     2,
       1,     1,     3,     2,     3,     4,     1,     2,     0,     2,
       3,     2,     2,     2,     0,     2,     3,     2,     3,     2,
       3,     3,     4,     2,     1,     2,     2,     3,     2,     2,
       0,     1,     1,     2,     2,     3,     2,     1,     2,     1,
       2,     2,     2,     2,     0,     1,     3,     5,     2,     3,
       2,     0,     1,     2,     2,     2,     2,     4,     5,     5,
       3,     1,     2,     3,     3,     2,     2,     3,     3,     0,
       2,     1,     3,     3,     3,     0,     1,     1,     3,     2,
       3,     2,     2,     1,     0,     2,     4,     2,     4,     2,
       1,     3,     2,     4,     3,     5,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
       0,     3,   246,   250,   254,   248,   257,   259,   389,     0,
       0,    55,    49,    50,    51,    52,     0,     0,    81,    82,
      83,    84,    86,     0,     0,     0,   262,   262,   262,   262,
     262,   262,   208,    85,    87,    88,   391,     0,     0,   138,
       0,   403,    91,    92,    93,     0,     0,   140,   141,   142,
     143,   144,   145,     0,   326,   334,   328,   315,   344,   315,
     315,   350,   317,   357,   359,   311,   364,   315,   371,     0,
     132,   136,   131,   113,   126,   126,   111,   112,     0,   101,
       0,     0,     0,     0,   381,     0,     0,     0,   305,   306,
     307,   308,   309,   310,     9,     0,     2,     4,     0,    12,
      14,    20,    34,    46,    53,    54,     0,     0,    35,    47,
       0,    89,   103,   104,    23,   107,   116,   117,   114,   115,
     110,   108,   109,   105,   106,    27,    28,    29,    30,    31,
      32,    33,    15,    17,    18,    19,    36,    16,     0,   191,
      96,     0,   276,     0,     0,     0,    22,    24,    37,   282,
     283,   284,   286,   285,   287,   288,   289,   290,   291,   292,
     293,   294,   295,   296,   297,   298,   299,   300,   301,   302,
     303,   304,    21,    25,    26,    38,    98,   255,   252,     0,
     280,     0,     0,     0,     0,     0,     0,   178,   180,   440,
     441,   442,   418,   416,   419,   420,   417,   422,   421,   425,
     424,   423,   443,   426,   427,   428,   429,   430,   431,   432,
     433,   434,   435,   436,   437,   439,   438,     0,   444,   147,
      56,     0,    57,    48,     0,    59,    60,     0,    75,     0,
       0,     0,     0,   268,   265,   247,     0,   262,   249,   251,
     254,   258,   260,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   391,     0,   395,
       0,     0,     0,   401,   402,   320,   321,   319,   315,   315,
     315,   315,   333,     0,     0,   327,   315,     0,   323,   345,
     315,   346,   348,   351,   352,   349,     0,   356,   317,   354,
     358,   360,   362,     0,   361,   365,   363,   315,   368,   372,
     370,   374,   375,   376,     0,     0,     0,     0,   124,   124,
     118,     0,     0,     0,     0,     0,   382,   385,   386,     0,
       0,    10,     1,     5,     6,     7,   246,     0,    61,    73,
      72,    77,    67,    62,    63,    65,    64,    68,    66,     0,
      78,    40,    41,    44,    42,    43,    45,    90,   119,    13,
     192,     0,    94,    97,   159,   161,   167,   175,   166,   165,
     404,   410,   277,     0,   404,   174,   177,     0,   100,   256,
     126,     0,   390,   278,   279,     0,     0,     0,     0,     0,
       0,   150,     0,    58,    79,    74,    76,    80,     0,   272,
       0,     0,   261,   263,   253,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   226,     0,     0,   240,   240,     0,     0,     0,     0,
     193,     0,     0,   212,   209,    11,     0,     0,   392,   396,
     397,   393,   394,   139,   311,   315,   335,   315,   315,   339,
     315,   337,   343,   329,   331,     0,   332,   315,   324,   316,
     347,   353,   318,   355,   312,     0,   369,   373,   133,     0,
     137,   127,     0,   128,   129,   102,   120,   122,     0,   380,
     383,   384,   387,   388,     8,    71,    69,     0,   162,     0,
       0,     0,    39,   160,     0,     0,   409,     0,     0,   412,
       0,   168,     0,   124,     0,   188,   190,   189,   187,     0,
       0,     0,     0,     0,     0,   179,   158,   153,   155,   154,
       0,     0,   151,   148,     0,   273,   274,   275,     0,     0,
       0,     0,   242,   244,   245,   226,   226,   226,   226,   242,
       0,     0,     0,     0,     0,     0,     0,   240,   240,     0,
       0,     0,   226,   226,   226,   241,   226,   226,     0,     0,
     226,   210,   211,   399,     0,     0,   322,   336,   340,   315,
     341,   338,   330,   325,     0,     0,   366,     0,     0,   125,
       0,     0,   377,     0,   163,   164,    95,   407,     0,   405,
       0,   414,   411,   176,     0,     0,    99,   130,   281,   183,
     186,   181,     0,     0,     0,   156,     0,     0,   146,     0,
     264,   266,     0,     0,   270,   269,   226,   243,     0,     0,
       0,     0,   239,     0,   228,   229,   231,   232,   235,   236,
     233,   234,   237,   230,   194,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   400,   398,   342,   314,   313,     0,
       0,   134,   121,   123,   378,   379,    70,     0,     0,     0,
     413,   170,     0,   173,   184,     0,     0,   157,   152,   149,
     267,   271,     0,   197,   195,   198,   199,   242,   227,   203,
     204,   201,   202,   207,     0,     0,     0,     0,     0,     0,
     214,     0,     0,   196,   367,     0,   408,   406,   415,     0,
     169,   182,   185,   200,   238,     0,     0,     0,     0,     0,
       0,     0,   240,     0,   135,   171,     0,     0,     0,   217,
     219,     0,     0,   224,   205,   213,     0,   206,   215,   216,
       0,     0,   220,     0,   221,   240,     0,   225,   223,     0,
     218,   222
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    95,    96,    97,    98,    99,   356,   101,   102,   103,
     223,   104,   105,   329,   340,   330,   331,   106,   107,   108,
     109,   110,   111,   482,   352,   112,   113,   114,   115,   116,
     117,   463,   308,   118,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,   131,   217,   511,   512,
     353,   354,   355,   357,   358,   585,   652,   132,   133,   134,
     135,   136,   137,   187,   188,   498,   138,   139,   256,   424,
     679,   680,   682,   712,   713,   541,   411,   544,   606,   545,
     140,   141,   142,   143,   178,   144,   145,   235,   236,   237,
     518,   363,   146,   147,   148,   294,   278,   289,   149,   267,
     150,   151,   152,   275,   153,   272,   154,   155,   156,   157,
     285,   158,   159,   160,   161,   162,   296,   163,   164,   300,
     165,   166,   167,   168,   169,   170,   171,   182,   172,   428,
     429,   430,   173,   174,   175,   486,   359,   360,   218,   361
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -519
static const yytype_int16 yypact[] =
{
     429,  -519,    55,  -519,   -17,  -519,  -519,  -519,    51,   -40,
    1137,    59,    58,  -519,  -519,  -519,    27,   106,  -519,  -519,
    -519,  -519,  -519,  1137,  1137,   229,    64,    64,    64,    64,
      64,    64,   298,  -519,  -519,  -519,   789,    82,   114,  -519,
      97,  -519,  -519,  -519,  -519,   183,   205,  -519,  -519,  -519,
    -519,  -519,  -519,   191,  -519,   132,    -3,   -33,   173,   -60,
     163,   -44,   105,   218,   168,    16,   215,    86,   220,   176,
      45,   319,  -519,  -519,   320,   320,  -519,  -519,  1137,   280,
     326,   327,   345,   352,  -519,   240,   273,   -48,  -519,  -519,
    -519,  -519,  -519,  -519,   311,   365,   609,  -519,    -7,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,    17,   181,  -519,  -519,
     497,  1248,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,   968,   325,
     182,   111,   302,  1137,   111,  1137,  -519,  -519,  -519,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,   -14,   322,  -519,   358,
     295,   190,   251,   198,   300,   304,   -42,  -519,   202,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,  -519,   204,  -519,   203,
    -519,   328,  -519,  -519,  1137,  -519,  -519,   368,   378,  1137,
     210,   224,   226,   227,  -519,  -519,   376,    64,  -519,  -519,
     -17,  -519,  -519,   332,   232,   233,   234,   235,   219,   236,
     237,   238,   241,   244,   -32,   245,    49,  -519,   242,   102,
     341,   347,   329,  -519,  -519,  -519,   330,  -519,   -25,   -41,
     -20,   163,  -519,   -46,   362,  -519,   208,   372,  -519,  -519,
     163,  -519,  -519,  -519,   373,  -519,   384,  -519,   335,  -519,
    -519,  -519,  -519,   342,  -519,   287,  -519,   163,  -519,   354,
    -519,  -519,  -519,  -519,   389,   316,   393,   394,   344,   344,
    -519,  1137,   396,   397,   403,   404,   371,   374,  -519,   375,
     377,  -519,  -519,  -519,  -519,   440,  -519,   407,  -519,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,   412,
     313,  -519,  -519,  -519,  -519,  -519,  -519,    36,  -519,  -519,
    -519,     1,   471,   182,  -519,  -519,  -519,  -519,  -519,  -519,
     -23,   239,  -519,   479,   -23,  -519,  -519,   480,   -12,  -519,
     320,   323,  -519,  -519,  -519,   -15,   324,   343,   351,   145,
     -40,    32,  1137,  -519,  -519,  -519,  -519,  -519,   399,   289,
     428,   -13,  -519,  -519,  -519,   353,   135,   135,   135,   135,
      13,   355,   356,   357,   359,   360,   361,   364,   366,   367,
     379,  -519,   135,   135,   135,   135,   135,   386,   387,   135,
    -519,   442,   443,  -519,  -519,  -519,   469,   475,  -519,   363,
    -519,  -519,  -519,  -519,   417,   163,  -519,   163,    79,  -519,
     163,  -519,  -519,  -519,  -519,   459,  -519,   163,  -519,  -519,
    -519,  -519,  -519,  -519,   -78,   432,  -519,  -519,   484,   390,
    -519,  -519,   473,  -519,  -519,  -519,   531,   532,   398,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,  -519,   380,  -519,   392,
     158,   111,  -519,  -519,   -57,   -49,  -519,  1137,  1137,  -519,
     111,   -24,   111,   344,   476,  -519,  -519,  -519,  -519,   -15,
     -15,   -15,   419,   478,   498,  -519,  -519,  -519,  -519,  -519,
     -37,     9,  -519,   438,   441,  -519,  -519,  -519,   449,   500,
     -74,   453,   135,  -519,  -519,  -519,  -519,  -519,  -519,   135,
     543,   135,   135,   135,   135,   135,   135,   135,   135,   135,
     135,   -27,  -519,  -519,  -519,  -519,  -519,  -519,   461,   462,
    -519,  -519,  -519,  -519,   585,   102,  -519,  -519,  -519,   163,
    -519,  -519,  -519,  -519,   534,   535,   470,   499,   571,  -519,
     572,   574,    12,   575,  -519,  -519,  -519,  -519,   576,  -519,
     577,  -519,  1097,  -519,   583,    71,  -519,  -519,  -519,  -519,
    -519,  -519,   -15,   483,   485,  -519,   587,    32,  -519,  1137,
    -519,  -519,   482,   486,  -519,  -519,  -519,  -519,     0,     5,
       7,     8,  -519,   487,  -519,  -519,  -519,  -519,  -519,  -519,
    -519,  -519,  -519,  -519,  -519,   219,    15,    18,    19,    20,
      21,   113,   607,    22,  -519,  -519,  -519,  -519,  -519,   592,
     492,  -519,  -519,  -519,  -519,  -519,  -519,   491,   493,  1137,
    -519,  -519,    38,  -519,  -519,   -15,   -15,  -519,  -519,  -519,
    -519,  -519,    23,  -519,  -519,  -519,  -519,   135,  -519,  -519,
    -519,  -519,  -519,  -519,   495,   496,   501,   502,   503,   489,
     508,   511,   494,  -519,  -519,   599,  -519,  -519,  -519,   601,
    -519,  -519,  -519,  -519,  -519,   135,   135,   -51,   519,   602,
     666,   113,   135,   667,  -519,  -519,   522,   523,   526,  -519,
    -519,   527,   528,   529,  -519,  -519,    77,  -519,  -519,  -519,
     135,   135,  -519,   602,  -519,   135,   530,  -519,  -519,   533,
    -519,  -519
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -519,  -519,  -519,   620,   682,   582,     3,  -519,  -519,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,
    -519,  -519,  -519,  -519,   545,  -519,  -519,  -519,  -519,  -519,
    -519,  -301,   -68,  -519,  -519,  -519,  -519,  -519,  -519,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,   617,   646,  -519,   128,
    -166,  -284,  -519,  -140,   141,  -519,  -519,   169,   172,   175,
     177,   180,  -519,   348,  -519,  -468,   588,  -519,  -519,  -519,
      28,  -519,  -519,    10,  -519,  -270,   107,  -410,  -518,  -378,
    -519,  -519,  -519,  -519,   504,  -519,  -519,   223,   505,  -519,
    -519,  -519,  -519,  -519,  -519,   297,   -58,   446,  -519,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,
    -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,  -519,
    -519,   184,  -519,  -519,   632,   381,  -519,    92,  -519,   -10
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -444
static const yytype_int16 yytable[] =
{
     219,   281,   282,   100,   365,   546,   326,   309,   464,   298,
     368,   612,   603,   227,   228,   -96,   624,   -97,   525,   526,
     527,   528,   326,     3,     4,     5,     6,     7,  -172,   577,
      10,   589,   590,   591,   542,   543,    26,   579,   547,   100,
     443,   550,   283,   663,   378,   324,   183,   564,   664,   595,
     665,   666,    26,    27,    28,    29,    30,    31,   669,   184,
     185,   670,   671,   672,   673,   683,   693,   708,   219,   483,
     179,   495,   318,   520,   304,   319,   326,   277,   220,   437,
     221,   417,   418,   506,   483,   444,   496,   284,  -443,   224,
     438,   445,   420,   421,   422,   435,   277,   280,   644,   100,
     440,   348,   276,    41,   277,   497,    26,   521,   565,   328,
     230,   231,   277,   341,   177,   604,   326,   277,   507,   305,
     484,   180,   485,   181,   654,   222,   273,   620,   621,   320,
     232,   578,   645,   508,   274,   423,   292,   176,   709,   580,
     293,   100,   259,   379,   607,   186,    26,   225,   596,   694,
     233,   607,   509,   614,   615,   616,   617,   618,   619,   625,
    -172,   622,   623,   426,   584,   427,   189,   190,   191,   192,
     193,   194,   260,   261,   351,   195,   351,   325,   196,   197,
     198,   199,   200,   201,   234,   480,   625,   691,   692,   351,
     478,   625,   587,   625,   625,   597,   483,   202,   598,   559,
     529,   625,   327,   530,   625,   625,   625,   625,   625,   625,
     436,   439,   441,   442,   384,   674,   277,   510,   448,   387,
    -443,   523,   450,   277,   689,   524,   226,   690,   675,   676,
     677,   502,   678,   203,   204,   364,   205,   366,   297,   456,
     206,   207,   286,   208,   503,   504,   209,   210,   287,   288,
     238,   239,   240,   241,   242,   608,   609,   610,   611,   211,
     229,   268,   269,   212,   270,   271,   724,   213,   214,   263,
     725,   215,   626,   627,   628,   333,   629,   630,   334,   342,
     633,   335,   343,   336,   262,   344,   337,   345,   216,   607,
     346,   264,   716,   279,   189,   190,   191,   192,   193,   194,
     277,   465,   493,   195,   290,   400,   196,   197,   198,   199,
     200,   201,   487,   265,   266,   729,   291,   706,   707,   373,
     374,   401,   402,   403,   404,   202,   405,   406,   407,   408,
     409,   301,   302,   303,   410,   295,   662,   515,   516,   517,
     299,   576,   726,   727,   447,   277,   351,   575,   306,   307,
     583,   489,   586,   311,   479,   312,   313,   314,   315,   317,
     316,   203,   204,   321,   205,   322,   339,    32,   206,   207,
     351,   208,   513,   362,   209,   210,   369,   557,   370,   558,
     560,   371,   561,   243,   372,   375,   376,   211,   380,   563,
     377,   212,   381,   382,   383,   213,   214,   385,   388,   215,
     244,   245,   246,   247,   248,   249,   250,   251,   252,   253,
     254,   386,   389,   255,   390,   391,   216,   392,   395,   396,
     397,   398,   399,   412,   413,   414,   425,   431,   415,   488,
       1,   416,   419,   432,     2,     3,     4,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,   446,   433,
     434,    16,    17,    18,    19,    20,    21,    22,   449,   451,
      23,    24,   454,    25,    26,    27,    28,    29,    30,    31,
     452,    32,   286,   455,   457,   458,   459,   581,   582,   460,
     461,   462,   466,   467,    33,    34,    35,    36,    37,   468,
     469,   470,   474,   475,   471,   472,    38,   473,   476,   477,
     481,   636,   326,     3,     4,     5,     6,     7,   490,   492,
     494,   499,    39,    40,   519,    41,    42,    43,    44,   514,
      45,    46,    47,    48,    49,    50,    51,    52,   551,   552,
     500,   553,    26,    27,    28,    29,    30,    31,   501,   554,
     522,   293,   531,   532,   533,   562,   534,   535,   536,   555,
      53,   537,   566,   538,   539,    54,    55,    56,   567,   569,
     570,   571,   588,    57,   593,   573,   540,    58,    59,    60,
      61,    62,   650,   548,   549,    63,    64,   568,    65,    66,
      67,   574,    68,    69,   594,   572,   602,    70,    71,   659,
      72,    73,    74,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,   592,    88,    89,    90,
      91,    92,    93,    94,     2,     3,     4,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,   599,   613,
     600,    16,    17,    18,    19,    20,    21,    22,   601,   688,
      23,    24,   605,    25,    26,    27,    28,    29,    30,    31,
     634,    32,   631,   632,   637,   638,   639,   641,   642,   640,
     643,   646,   647,   648,    33,    34,    35,    36,    37,   651,
     655,   660,   656,   657,   667,   661,    38,   681,   684,   685,
     686,   700,   687,   695,   696,   704,   703,   705,   711,   697,
     698,   699,    39,    40,   701,    41,    42,    43,    44,   702,
      45,    46,    47,    48,    49,    50,    51,    52,   710,   714,
     717,   718,   719,   720,   721,   723,   323,   722,   258,   730,
     349,   367,   731,   332,   310,   658,   653,   350,   505,   715,
      53,   556,   668,   728,   453,    54,    55,    56,   338,   635,
       0,     0,   393,    57,   394,   491,     0,    58,    59,    60,
      61,    62,     0,     0,     0,    63,    64,     0,    65,    66,
      67,     0,    68,    69,     0,     0,     0,    70,    71,     0,
      72,    73,    74,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,     0,    88,    89,    90,
      91,    92,    93,    94,     2,     3,     4,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,     0,     0,
       0,    16,    17,    18,    19,    20,    21,    22,     0,     0,
      23,    24,     0,    25,    26,    27,    28,    29,    30,    31,
       0,    32,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    33,    34,    35,   257,    37,     0,
       0,     0,     0,     0,     0,     0,    38,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    39,    40,     0,    41,    42,    43,    44,     0,
      45,    46,    47,    48,    49,    50,    51,    52,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      53,     0,     0,     0,     0,    54,    55,    56,     0,     0,
       0,     0,     0,    57,     0,     0,     0,    58,    59,    60,
      61,    62,     0,     0,     0,    63,    64,     0,    65,    66,
      67,     0,    68,    69,     0,     0,     0,    70,    71,     0,
      72,    73,    74,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,     0,    88,    89,    90,
      91,    92,    93,     2,     3,     4,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,     0,     0,     0,
      16,    17,    18,    19,    20,    21,    22,     0,     0,    23,
      24,     0,    25,    26,    27,    28,    29,    30,    31,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    33,    34,    35,   257,    37,     0,     0,
       0,     0,     0,     0,     0,    38,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    39,    40,     0,    41,    42,    43,    44,     0,    45,
      46,    47,    48,    49,    50,    51,    52,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    53,
       0,     0,     0,     0,    54,    55,    56,     0,     0,     0,
       0,     0,    57,     0,     0,     0,    58,    59,    60,    61,
      62,     0,     0,     0,    63,    64,     0,    65,    66,    67,
       0,    68,    69,     0,     0,     0,    70,    71,     0,    72,
      73,    74,    75,    76,    77,    78,    79,    80,    81,    82,
      83,    84,    85,    86,    87,     0,    88,    89,    90,    91,
      92,    93,   189,   190,   191,   192,   193,   194,     0,     0,
       0,   195,     0,     0,   196,   197,   198,   199,   200,   201,
     649,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   202,     0,     0,     0,     0,     0,     0,
       0,     0,   189,   190,   191,   192,   193,   194,     0,     0,
       0,   195,     0,     0,   196,   197,   198,   199,   200,   201,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   203,
     204,     0,   205,   202,     0,     0,   206,   207,     0,   208,
       0,     0,   209,   210,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   211,     0,     0,     0,   212,
       0,     0,     0,   213,   214,     0,     0,   215,     0,   203,
     204,     0,   205,     0,     0,     0,   206,   207,     0,   208,
       0,     0,   209,   210,   216,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   211,     0,     0,     0,   212,
       0,     0,     0,   213,   214,     0,     0,   215,     0,     0,
       0,     0,     0,   189,   190,   191,   192,   193,   194,     0,
       0,     0,   195,     0,   216,   196,   197,   198,   199,   200,
     201,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   347,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     203,   204,     0,   205,     0,     0,     0,   206,   207,     0,
     208,     0,     0,   209,   210,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   211,     0,     0,     0,
     212,     0,     0,     0,   213,   214,     0,     0,   215,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   216
};

static const yytype_int16 yycheck[] =
{
      10,    59,    60,     0,   144,   415,     5,    75,   309,    67,
     176,   529,    86,    23,    24,    29,    43,    29,   396,   397,
     398,   399,     5,     6,     7,     8,     9,    10,    52,    86,
      13,   499,   500,   501,   412,   413,    35,    86,   416,    36,
      86,   419,    86,    43,    86,    52,    86,   125,    43,    86,
      43,    43,    35,    36,    37,    38,    39,    40,    43,    99,
     100,    43,    43,    43,    43,    43,    43,   118,    78,   353,
      19,    86,   120,    86,    29,   123,     5,   137,    19,   120,
      21,   113,   114,    51,   368,   131,   101,   131,    52,    31,
     131,   137,    43,    44,    45,   120,   137,   157,    86,    96,
     120,   111,   135,    86,   137,   120,    35,   120,   186,   106,
      46,    47,   137,   110,   131,   189,     5,   137,    86,    74,
     143,    70,   145,    72,   592,    66,   129,   537,   538,   177,
      66,   188,   120,   101,   137,    86,   120,    82,   189,   188,
     124,   138,    60,   185,   522,   185,    35,   120,   185,   667,
      86,   529,   120,   531,   532,   533,   534,   535,   536,   186,
     184,   539,   540,    61,   188,    63,    55,    56,    57,    58,
      59,    60,    58,    59,   188,    64,   188,   184,    67,    68,
      69,    70,    71,    72,   120,   351,   186,   655,   656,   188,
     189,   186,   493,   186,   186,   186,   480,    86,   189,   120,
     187,   186,   185,   190,   186,   186,   186,   186,   186,   186,
     268,   269,   270,   271,   224,   102,   137,   185,   276,   229,
     184,    86,   280,   137,   186,    90,   120,   189,   115,   116,
     117,    86,   119,   122,   123,   143,   125,   145,   152,   297,
     129,   130,   137,   132,    99,   100,   135,   136,   143,   144,
      27,    28,    29,    30,    31,   525,   526,   527,   528,   148,
      31,   129,   130,   152,   132,   133,   189,   156,   157,    86,
     193,   160,   542,   543,   544,   106,   546,   547,   106,   110,
     550,   106,   110,   106,   187,   110,   106,   110,   177,   667,
     110,    86,   702,   120,    55,    56,    57,    58,    59,    60,
     137,   311,   370,    64,    86,    86,    67,    68,    69,    70,
      71,    72,    73,   122,   123,   725,   148,   695,   696,    68,
      69,   102,   103,   104,   105,    86,   107,   108,   109,   110,
     111,   155,   156,   157,   115,   120,   606,    48,    49,    50,
     120,   481,   720,   721,   136,   137,   188,   189,    29,    29,
     490,   361,   492,    73,   351,    29,    29,    12,     6,    86,
     120,   122,   123,    52,   125,     0,   185,    42,   129,   130,
     188,   132,   382,    71,   135,   136,    54,   435,    20,   437,
     438,    86,   440,    85,   194,   187,    86,   148,   186,   447,
      86,   152,   188,   190,    66,   156,   157,    29,   188,   160,
     102,   103,   104,   105,   106,   107,   108,   109,   110,   111,
     112,    33,   188,   115,   188,   188,   177,    41,    86,   187,
     187,   187,   187,   187,   187,   187,   184,    86,   187,   190,
       1,   187,   187,    86,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,    16,    17,    18,    86,   120,
     120,    22,    23,    24,    25,    26,    27,    28,    86,    86,
      31,    32,   120,    34,    35,    36,    37,    38,    39,    40,
      86,    42,   137,   186,   120,    86,   160,   487,   488,    86,
      86,   137,    86,    86,    55,    56,    57,    58,    59,    86,
      86,   120,    52,    86,   120,   120,    67,   120,    86,   186,
      29,   559,     5,     6,     7,     8,     9,    10,    29,    29,
     187,   187,    83,    84,    86,    86,    87,    88,    89,   120,
      91,    92,    93,    94,    95,    96,    97,    98,    86,    86,
     187,    62,    35,    36,    37,    38,    39,    40,   187,    64,
     187,   124,   187,   187,   187,    86,   187,   187,   187,   186,
     121,   187,   120,   187,   187,   126,   127,   128,    74,    86,
      29,    29,    86,   134,    86,   185,   187,   138,   139,   140,
     141,   142,   582,   187,   187,   146,   147,   187,   149,   150,
     151,   189,   153,   154,    86,   187,    86,   158,   159,   599,
     161,   162,   163,   164,   165,   166,   167,   168,   169,   170,
     171,   172,   173,   174,   175,   176,   187,   178,   179,   180,
     181,   182,   183,   184,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,    16,    17,    18,   190,    86,
     189,    22,    23,    24,    25,    26,    27,    28,   189,   649,
      31,    32,   189,    34,    35,    36,    37,    38,    39,    40,
      65,    42,   191,   191,   120,   120,   186,    86,    86,   160,
      86,    86,    86,    86,    55,    56,    57,    58,    59,    86,
     187,   189,   187,    86,   187,   189,    67,    70,    86,   187,
     189,   192,   189,   188,   188,    86,   192,    86,    86,   188,
     188,   188,    83,    84,   186,    86,    87,    88,    89,   188,
      91,    92,    93,    94,    95,    96,    97,    98,   189,    43,
      43,   189,   189,   187,   187,   186,    96,   189,    36,   189,
     138,   176,   189,   106,    78,   597,   585,   139,   380,   701,
     121,   434,   625,   723,   288,   126,   127,   128,   106,   555,
      -1,    -1,   237,   134,   240,   364,    -1,   138,   139,   140,
     141,   142,    -1,    -1,    -1,   146,   147,    -1,   149,   150,
     151,    -1,   153,   154,    -1,    -1,    -1,   158,   159,    -1,
     161,   162,   163,   164,   165,   166,   167,   168,   169,   170,
     171,   172,   173,   174,   175,   176,    -1,   178,   179,   180,
     181,   182,   183,   184,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,    16,    17,    18,    -1,    -1,
      -1,    22,    23,    24,    25,    26,    27,    28,    -1,    -1,
      31,    32,    -1,    34,    35,    36,    37,    38,    39,    40,
      -1,    42,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    55,    56,    57,    58,    59,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    67,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    83,    84,    -1,    86,    87,    88,    89,    -1,
      91,    92,    93,    94,    95,    96,    97,    98,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     121,    -1,    -1,    -1,    -1,   126,   127,   128,    -1,    -1,
      -1,    -1,    -1,   134,    -1,    -1,    -1,   138,   139,   140,
     141,   142,    -1,    -1,    -1,   146,   147,    -1,   149,   150,
     151,    -1,   153,   154,    -1,    -1,    -1,   158,   159,    -1,
     161,   162,   163,   164,   165,   166,   167,   168,   169,   170,
     171,   172,   173,   174,   175,   176,    -1,   178,   179,   180,
     181,   182,   183,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,    16,    17,    18,    -1,    -1,    -1,
      22,    23,    24,    25,    26,    27,    28,    -1,    -1,    31,
      32,    -1,    34,    35,    36,    37,    38,    39,    40,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    55,    56,    57,    58,    59,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    67,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    83,    84,    -1,    86,    87,    88,    89,    -1,    91,
      92,    93,    94,    95,    96,    97,    98,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   121,
      -1,    -1,    -1,    -1,   126,   127,   128,    -1,    -1,    -1,
      -1,    -1,   134,    -1,    -1,    -1,   138,   139,   140,   141,
     142,    -1,    -1,    -1,   146,   147,    -1,   149,   150,   151,
      -1,   153,   154,    -1,    -1,    -1,   158,   159,    -1,   161,
     162,   163,   164,   165,   166,   167,   168,   169,   170,   171,
     172,   173,   174,   175,   176,    -1,   178,   179,   180,   181,
     182,   183,    55,    56,    57,    58,    59,    60,    -1,    -1,
      -1,    64,    -1,    -1,    67,    68,    69,    70,    71,    72,
      73,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    86,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    55,    56,    57,    58,    59,    60,    -1,    -1,
      -1,    64,    -1,    -1,    67,    68,    69,    70,    71,    72,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   122,
     123,    -1,   125,    86,    -1,    -1,   129,   130,    -1,   132,
      -1,    -1,   135,   136,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   148,    -1,    -1,    -1,   152,
      -1,    -1,    -1,   156,   157,    -1,    -1,   160,    -1,   122,
     123,    -1,   125,    -1,    -1,    -1,   129,   130,    -1,   132,
      -1,    -1,   135,   136,   177,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   148,    -1,    -1,    -1,   152,
      -1,    -1,    -1,   156,   157,    -1,    -1,   160,    -1,    -1,
      -1,    -1,    -1,    55,    56,    57,    58,    59,    60,    -1,
      -1,    -1,    64,    -1,   177,    67,    68,    69,    70,    71,
      72,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    86,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     122,   123,    -1,   125,    -1,    -1,    -1,   129,   130,    -1,
     132,    -1,    -1,   135,   136,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   148,    -1,    -1,    -1,
     152,    -1,    -1,    -1,   156,   157,    -1,    -1,   160,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   177
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     1,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    22,    23,    24,    25,
      26,    27,    28,    31,    32,    34,    35,    36,    37,    38,
      39,    40,    42,    55,    56,    57,    58,    59,    67,    83,
      84,    86,    87,    88,    89,    91,    92,    93,    94,    95,
      96,    97,    98,   121,   126,   127,   128,   134,   138,   139,
     140,   141,   142,   146,   147,   149,   150,   151,   153,   154,
     158,   159,   161,   162,   163,   164,   165,   166,   167,   168,
     169,   170,   171,   172,   173,   174,   175,   176,   178,   179,
     180,   181,   182,   183,   184,   196,   197,   198,   199,   200,
     201,   202,   203,   204,   206,   207,   212,   213,   214,   215,
     216,   217,   220,   221,   222,   223,   224,   225,   228,   229,
     230,   231,   232,   233,   234,   235,   236,   237,   238,   239,
     240,   241,   252,   253,   254,   255,   256,   257,   261,   262,
     275,   276,   277,   278,   280,   281,   287,   288,   289,   293,
     295,   296,   297,   299,   301,   302,   303,   304,   306,   307,
     308,   309,   310,   312,   313,   315,   316,   317,   318,   319,
     320,   321,   323,   327,   328,   329,    82,   131,   279,    19,
      70,    72,   322,    86,    99,   100,   185,   258,   259,    55,
      56,    57,    58,    59,    60,    64,    67,    68,    69,    70,
      71,    72,    86,   122,   123,   125,   129,   130,   132,   135,
     136,   148,   152,   156,   157,   160,   177,   242,   333,   334,
      19,    21,    66,   205,    31,   120,   120,   334,   334,    31,
      46,    47,    66,    86,   120,   282,   283,   284,   282,   282,
     282,   282,   282,    85,   102,   103,   104,   105,   106,   107,
     108,   109,   110,   111,   112,   115,   263,    58,   199,    60,
      58,    59,   187,    86,    86,   122,   123,   294,   129,   130,
     132,   133,   300,   129,   137,   298,   135,   137,   291,   120,
     157,   291,   291,    86,   131,   305,   137,   143,   144,   292,
      86,   148,   120,   124,   290,   120,   311,   152,   291,   120,
     314,   155,   156,   157,    29,    74,    29,    29,   227,   227,
     242,    73,    29,    29,    12,     6,   120,    86,   120,   123,
     177,    52,     0,   198,    52,   184,     5,   185,   201,   208,
     210,   211,   241,   252,   253,   254,   255,   256,   329,   185,
     209,   201,   252,   253,   254,   255,   256,    86,   334,   200,
     261,   188,   219,   245,   246,   247,   201,   248,   249,   331,
     332,   334,    71,   286,   332,   248,   332,   219,   245,    54,
      20,    86,   194,    68,    69,   187,    86,    86,    86,   185,
     186,   188,   190,    66,   334,    29,    33,   334,   188,   188,
     188,   188,    41,   283,   279,    86,   187,   187,   187,   187,
      86,   102,   103,   104,   105,   107,   108,   109,   110,   111,
     115,   271,   187,   187,   187,   187,   187,   113,   114,   187,
      43,    44,    45,    86,   264,   184,    61,    63,   324,   325,
     326,    86,    86,   120,   120,   120,   291,   120,   131,   291,
     120,   291,   291,    86,   131,   137,    86,   136,   291,    86,
     291,    86,    86,   292,   120,   186,   291,   120,    86,   160,
      86,    86,   137,   226,   226,   334,    86,    86,    86,    86,
     120,   120,   120,   120,    52,    86,    86,   186,   189,   201,
     245,    29,   218,   246,   143,   145,   330,    73,   190,   334,
      29,   330,    29,   227,   187,    86,   101,   120,   260,   187,
     187,   187,    86,    99,   100,   258,    51,    86,   101,   120,
     185,   243,   244,   334,   120,    48,    49,    50,   285,    86,
      86,   120,   187,    86,    90,   274,   274,   274,   274,   187,
     190,   187,   187,   187,   187,   187,   187,   187,   187,   187,
     187,   270,   274,   274,   272,   274,   272,   274,   187,   187,
     274,    86,    86,    62,    64,   186,   290,   291,   291,   120,
     291,   291,    86,   291,   125,   186,   120,    74,   187,    86,
      29,    29,   187,   185,   189,   189,   248,    86,   188,    86,
     188,   334,   334,   248,   188,   250,   248,   226,    86,   260,
     260,   260,   187,    86,    86,    86,   185,   186,   189,   190,
     189,   189,    86,    86,   189,   189,   273,   274,   270,   270,
     270,   270,   273,    86,   274,   274,   274,   274,   274,   274,
     272,   272,   274,   274,    43,   186,   270,   270,   270,   270,
     270,   191,   191,   270,    65,   326,   291,   120,   120,   186,
     160,    86,    86,    86,    86,   120,    86,    86,    86,    73,
     334,    86,   251,   249,   260,   187,   187,    86,   244,   334,
     189,   189,   270,    43,    43,    43,    43,   187,   271,    43,
      43,    43,    43,    43,   102,   115,   116,   117,   119,   265,
     266,    70,   267,    43,    86,   187,   189,   189,   334,   186,
     189,   260,   260,    43,   273,   188,   188,   188,   188,   188,
     192,   186,   188,   192,    86,    86,   274,   274,   118,   189,
     189,    86,   268,   269,    43,   265,   272,    43,   189,   189,
     187,   187,   189,   186,   189,   193,   274,   274,   268,   272,
     189,   189
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

    { result->cur_stmt_type_ = OBPROXY_T_OTHERS; ;}
    break;

  case 48:

    { result->cur_stmt_type_ = OBPROXY_T_CREATE; ;}
    break;

  case 49:

    { result->cur_stmt_type_ = OBPROXY_T_DROP; ;}
    break;

  case 50:

    { result->cur_stmt_type_ = OBPROXY_T_ALTER; ;}
    break;

  case 51:

    { result->cur_stmt_type_ = OBPROXY_T_TRUNCATE; ;}
    break;

  case 52:

    { result->cur_stmt_type_ = OBPROXY_T_RENAME; ;}
    break;

  case 53:

    {;}
    break;

  case 54:

    {;}
    break;

  case 56:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_TABLE; ;}
    break;

  case 57:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 58:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 59:

    {
            SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num));
            result->cur_stmt_type_ = OBPROXY_T_STOP_DDL_TASK;
          ;}
    break;

  case 60:

    {
            SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num));
            result->cur_stmt_type_ = OBPROXY_T_RETRY_DDL_TASK;
          ;}
    break;

  case 61:

    {;}
    break;

  case 62:

    {;}
    break;

  case 63:

    {;}
    break;

  case 64:

    {;}
    break;

  case 65:

    {;}
    break;

  case 66:

    {;}
    break;

  case 67:

    {;}
    break;

  case 68:

    {;}
    break;

  case 69:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(2) - (2)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 70:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(4) - (4)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 71:

    {
                          ObProxyTextPsParseNode *node = NULL;
                          malloc_parse_node(node);
                          node->str_value_ = (yyvsp[(2) - (2)].str);
                          add_text_ps_node(result->text_ps_parse_info_, node);
                        ;}
    break;

  case 74:

    {
                      result->text_ps_inner_stmt_type_ = OBPROXY_T_TEXT_PS_PREPARE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 75:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (2)].str);
                    ;}
    break;

  case 76:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 77:

    {
            ;}
    break;

  case 78:

    {
            ;}
    break;

  case 79:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 80:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 81:

    { result->cur_stmt_type_ = OBPROXY_T_GRANT; ;}
    break;

  case 82:

    { result->cur_stmt_type_ = OBPROXY_T_REVOKE; ;}
    break;

  case 83:

    { result->cur_stmt_type_ = OBPROXY_T_ANALYZE; ;}
    break;

  case 84:

    { result->cur_stmt_type_ = OBPROXY_T_PURGE; ;}
    break;

  case 85:

    { result->cur_stmt_type_ = OBPROXY_T_FLASHBACK; ;}
    break;

  case 86:

    { result->cur_stmt_type_ = OBPROXY_T_COMMENT; ;}
    break;

  case 87:

    { result->cur_stmt_type_ = OBPROXY_T_AUDIT; ;}
    break;

  case 88:

    { result->cur_stmt_type_ = OBPROXY_T_NOAUDIT; ;}
    break;

  case 91:

    {;}
    break;

  case 92:

    {;}
    break;

  case 93:

    {;}
    break;

  case 98:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_TX_RO; ;}
    break;

  case 102:

    { result->col_name_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 103:

    {;}
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

  case 107:

    {;}
    break;

  case 108:

    {;}
    break;

  case 109:

    {;}
    break;

  case 110:

    {;}
    break;

  case 111:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_DATABASE; ;}
    break;

  case 112:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_PROXY_STATUS; ;}
    break;

  case 113:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DATABASES; ;}
    break;

  case 114:

    {;}
    break;

  case 115:

    {;}
    break;

  case 116:

    {;}
    break;

  case 117:

    {;}
    break;

  case 118:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE; ;}
    break;

  case 119:

    {
                      result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
                      result->cur_stmt_type_ = OBPROXY_T_DESC;
                      result->sub_stmt_type_ = OBPROXY_T_SUB_DESC_TABLE;
                  ;}
    break;

  case 120:

    {
                    result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_COLUMNS;
                    result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                  ;}
    break;

  case 121:

    {
                    result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_COLUMNS;
                    result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                    result->table_info_.database_name_ = (yyvsp[(5) - (5)].str);
                  ;}
    break;

  case 122:

    {
                 result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_INDEX;
                 result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
               ;}
    break;

  case 123:

    {
                 result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_INDEX;
                 result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                 result->table_info_.database_name_ = (yyvsp[(5) - (5)].str);
               ;}
    break;

  case 124:

    {;}
    break;

  case 125:

    { result->table_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 126:

    {;}
    break;

  case 127:

    { result->table_info_.database_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 128:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLES;
                ;}
    break;

  case 129:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_FULL_TABLES;
                ;}
    break;

  case 130:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLE_STATUS;
                      ;}
    break;

  case 131:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DB_VERSION; ;}
    break;

  case 132:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID; ;}
    break;

  case 133:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 134:

    {
                     SET_ICMD_SECOND_STRING((yyvsp[(5) - (5)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 135:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (7)].str));
                     SET_ICMD_SECOND_STRING((yyvsp[(7) - (7)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 136:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY; ;}
    break;

  case 137:

    {
                     result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY;
                 ;}
    break;

  case 138:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_ROUTE_ADDR; ;}
    break;

  case 139:

    {
                              result->cur_stmt_type_ = OBPROXY_T_SET_ROUTE_ADDR;
                              result->cmd_info_.integer_[0] = (yyvsp[(3) - (3)].num);
                           ;}
    break;

  case 140:

    {;}
    break;

  case 141:

    {;}
    break;

  case 142:

    {;}
    break;

  case 143:

    {;}
    break;

  case 144:

    {;}
    break;

  case 145:

    {;}
    break;

  case 147:

    {
                   result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                 ;}
    break;

  case 148:

    {
                   result->table_info_.package_name_ = (yyvsp[(1) - (3)].str);
                   result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                 ;}
    break;

  case 149:

    {
                   result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                   result->table_info_.package_name_ = (yyvsp[(3) - (5)].str);
                   result->table_info_.table_name_ = (yyvsp[(5) - (5)].str);
                 ;}
    break;

  case 150:

    {
                result->call_parse_info_.node_count_ = 0;
              ;}
    break;

  case 151:

    {
                result->call_parse_info_.node_count_ = 0;
                add_call_node(result->call_parse_info_, (yyvsp[(1) - (1)].node));
              ;}
    break;

  case 152:

    {
                add_call_node(result->call_parse_info_, (yyvsp[(3) - (3)].node));
              ;}
    break;

  case 153:

    {
            malloc_call_node((yyval.node), CALL_TOKEN_STR_VAL);
            (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 154:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_INT_VAL);
           (yyval.node)->int_value_ = (yyvsp[(1) - (1)].num);
         ;}
    break;

  case 155:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_NUMBER_VAL);
           (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 156:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_USER_VAR);
           (yyval.node)->str_value_ = (yyvsp[(2) - (2)].str);
         ;}
    break;

  case 157:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_SYS_VAR);
           (yyval.node)->str_value_ = (yyvsp[(3) - (3)].str);
         ;}
    break;

  case 158:

    {
           result->placeholder_list_idx_++;
           malloc_call_node((yyval.node), CALL_TOKEN_PLACE_HOLDER);
           (yyval.node)->placeholder_idx_ = result->placeholder_list_idx_ - 1;
         ;}
    break;

  case 172:

    {
                                                                  handle_stmt_end(result);
                                                                  HANDLE_ACCEPT();
                                                                ;}
    break;

  case 177:

    {
                                                 handle_stmt_end(result);
                                                 HANDLE_ACCEPT();
                                               ;}
    break;

  case 181:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_USER);
        ;}
    break;

  case 182:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 183:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 184:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(5) - (5)].var_node), (yyvsp[(3) - (5)].str), SET_VAR_SYS);
        ;}
    break;

  case 185:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 186:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 187:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_SYS);
        ;}
    break;

  case 188:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_STR);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 189:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_INT);
               (yyval.var_node)->int_value_ = (yyvsp[(1) - (1)].num);
             ;}
    break;

  case 190:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_NUMBER);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 193:

    {;}
    break;

  case 194:

    {;}
    break;

  case 195:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 196:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 197:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 198:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 199:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 200:

    {
              malloc_shard_column_node((yyval.shard_node), (yyvsp[(2) - (7)].str), (yyvsp[(3) - (7)].str), DBMESH_TOKEN_STR_VAL);
              (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (7)].str);
              add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
            ;}
    break;

  case 201:

    { result->trace_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 202:

    { result->rpc_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 203:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 204:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 205:

    {;}
    break;

  case 206:

    {;}
    break;

  case 207:

    { result->target_db_server_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 208:

    {;}
    break;

  case 210:

    { result->has_simple_route_info_ = true; result->simple_route_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 211:

    { result->simple_route_info_.part_key_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 215:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.group_idx_str_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 216:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.table_name_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 217:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 218:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 219:

    { result->dbp_route_info_.sticky_session_ = true; ;}
    break;

  case 220:

    {result->dbp_route_info_.has_shard_key_ = true;;}
    break;

  case 221:

    { result->trace_id_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 222:

    { result->trace_id_ = (yyvsp[(3) - (6)].str); result->rpc_id_ = (yyvsp[(5) - (6)].str); ;}
    break;

  case 223:

    {;}
    break;

  case 225:

    {
                   if (result->dbp_route_info_.shard_key_count_ < OBPROXY_MAX_DBP_SHARD_KEY_NUM) {
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].left_str_ = (yyvsp[(1) - (3)].str);
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].right_str_ = (yyvsp[(3) - (3)].str);
                     ++result->dbp_route_info_.shard_key_count_;
                   }
                 ;}
    break;

  case 228:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 229:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 230:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 231:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 232:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 233:

    { result->trace_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 234:

    { result->rpc_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 235:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 236:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 237:

    { result->target_db_server_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 238:

    {
             malloc_shard_column_node((yyval.shard_node), (yyvsp[(1) - (5)].str), (yyvsp[(3) - (5)].str), DBMESH_TOKEN_STR_VAL);
             (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (5)].str);
             add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
           ;}
    break;

  case 239:

    {;}
    break;

  case 240:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 242:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 264:

    { result->query_timeout_ = (yyvsp[(3) - (4)].num); ;}
    break;

  case 265:

    {;}
    break;

  case 267:

    {
      add_hint_index(result->dbmesh_route_info_, (yyvsp[(3) - (5)].str));
      result->dbmesh_route_info_.index_count_++;
    ;}
    break;

  case 268:

    {;}
    break;

  case 269:

    {;}
    break;

  case 270:

    {;}
    break;

  case 271:

    {;}
    break;

  case 272:

    {;}
    break;

  case 273:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_WEAK); ;}
    break;

  case 274:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_STRONG); ;}
    break;

  case 275:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_FROZEN); ;}
    break;

  case 278:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_WARNINGS; ;}
    break;

  case 279:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_ERRORS; ;}
    break;

  case 280:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 281:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 305:

    {;}
    break;

  case 306:

    {;}
    break;

  case 307:

    {;}
    break;

  case 308:

    {;}
    break;

  case 309:

    {;}
    break;

  case 310:

    {;}
    break;

  case 311:

    {
;}
    break;

  case 312:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 313:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 314:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 315:

    {;}
    break;

  case 316:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 317:

    {;}
    break;

  case 318:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 320:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 321:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 322:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 323:

    {;}
    break;

  case 324:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 325:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 326:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 328:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 329:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 330:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 331:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 332:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 334:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 335:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 336:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 337:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 338:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 339:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 340:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 341:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 342:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 343:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_READ_STALE); ;}
    break;

  case 344:

    {;}
    break;

  case 345:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 346:

    {;}
    break;

  case 347:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 348:

    {;}
    break;

  case 350:

    {;}
    break;

  case 351:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 352:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 353:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 354:

    {;}
    break;

  case 355:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 356:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 357:

    {;}
    break;

  case 358:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 359:

    {;}
    break;

  case 360:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 361:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 362:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 364:

    {;}
    break;

  case 365:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 366:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 367:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 368:

    {;}
    break;

  case 369:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 371:

    {;}
    break;

  case 372:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 373:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 374:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 375:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 376:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 377:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 378:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 379:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 380:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 381:

    {;}
    break;

  case 382:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 383:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 384:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 385:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 386:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 387:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 388:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 391:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 392:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 393:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 394:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 401:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 402:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 404:

    {;}
    break;

  case 405:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 406:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 407:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 408:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 409:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT();
                                                ;}
    break;

  case 410:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 411:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 412:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 413:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 414:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 415:

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

