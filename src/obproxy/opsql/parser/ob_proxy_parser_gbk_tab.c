
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
     EXPLAIN_ROUTE = 343,
     DESC = 344,
     DESCRIBE = 345,
     NAME_STR = 346,
     USE = 347,
     HELP = 348,
     SET_NAMES = 349,
     SET_CHARSET = 350,
     SET_PASSWORD = 351,
     SET_DEFAULT = 352,
     SET_OB_READ_CONSISTENCY = 353,
     SET_TX_READ_ONLY = 354,
     GLOBAL = 355,
     SESSION = 356,
     NUMBER_VAL = 357,
     GROUP_ID = 358,
     TABLE_ID = 359,
     ELASTIC_ID = 360,
     TESTLOAD = 361,
     ODP_COMMENT = 362,
     TNT_ID = 363,
     DISASTER_STATUS = 364,
     TRACE_ID = 365,
     RPC_ID = 366,
     TARGET_DB_SERVER = 367,
     DBP_COMMENT = 368,
     ROUTE_TAG = 369,
     SYS_TAG = 370,
     TABLE_NAME = 371,
     SCAN_ALL = 372,
     STICKY_SESSION = 373,
     PARALL = 374,
     SHARD_KEY = 375,
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
#define YYFINAL  318
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   1439

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  191
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  140
/* YYNRULES -- Number of rules.  */
#define YYNRULES  439
/* YYNRULES -- Number of states.  */
#define YYNSTATES  727

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
      88,    92,    95,    98,   101,   104,   107,   110,   113,   115,
     117,   120,   122,   124,   126,   128,   130,   132,   133,   135,
     137,   140,   143,   146,   148,   150,   152,   154,   156,   158,
     160,   162,   165,   170,   173,   175,   177,   181,   184,   188,
     191,   194,   198,   202,   204,   206,   208,   210,   212,   214,
     216,   218,   220,   223,   225,   227,   229,   231,   232,   235,
     236,   238,   241,   247,   251,   253,   257,   259,   261,   263,
     265,   267,   269,   272,   274,   276,   278,   280,   282,   284,
     287,   290,   293,   296,   299,   304,   309,   310,   313,   314,
     317,   321,   325,   331,   333,   335,   339,   345,   353,   355,
     359,   361,   363,   365,   367,   369,   371,   377,   379,   383,
     389,   390,   392,   396,   398,   400,   402,   405,   409,   411,
     413,   416,   418,   421,   425,   429,   431,   433,   435,   436,
     440,   442,   446,   450,   456,   459,   462,   467,   470,   473,
     477,   479,   484,   491,   496,   502,   509,   514,   518,   520,
     522,   524,   526,   529,   533,   539,   546,   553,   560,   567,
     574,   582,   589,   596,   603,   610,   619,   628,   635,   636,
     639,   642,   645,   647,   651,   653,   658,   663,   667,   674,
     678,   683,   688,   695,   699,   701,   705,   706,   710,   714,
     718,   722,   726,   730,   734,   738,   742,   746,   750,   756,
     760,   761,   763,   764,   766,   768,   770,   772,   775,   777,
     780,   782,   785,   788,   792,   793,   795,   798,   800,   803,
     805,   808,   811,   812,   815,   820,   822,   827,   833,   835,
     840,   845,   851,   852,   854,   856,   858,   859,   861,   865,
     869,   872,   878,   880,   882,   884,   886,   888,   890,   892,
     894,   896,   898,   900,   902,   904,   906,   908,   910,   912,
     914,   916,   918,   920,   922,   924,   926,   927,   930,   935,
     940,   941,   944,   945,   948,   951,   953,   955,   959,   962,
     966,   971,   973,   976,   977,   980,   984,   987,   990,   993,
     994,   997,  1001,  1004,  1008,  1011,  1015,  1019,  1024,  1027,
    1029,  1032,  1035,  1039,  1042,  1045,  1046,  1048,  1050,  1053,
    1056,  1060,  1063,  1065,  1068,  1070,  1073,  1076,  1079,  1082,
    1083,  1085,  1089,  1095,  1098,  1102,  1105,  1106,  1108,  1111,
    1114,  1117,  1120,  1125,  1131,  1137,  1141,  1143,  1146,  1150,
    1154,  1157,  1160,  1164,  1168,  1169,  1172,  1174,  1178,  1182,
    1186,  1187,  1189,  1191,  1195,  1198,  1202,  1205,  1208,  1210,
    1211,  1214,  1219,  1222,  1227,  1230,  1232,  1236,  1239,  1244,
    1248,  1254,  1256,  1258,  1260,  1262,  1264,  1266,  1268,  1270,
    1272,  1274,  1276,  1278,  1280,  1282,  1284,  1286,  1288,  1290,
    1292,  1294,  1296,  1298,  1300,  1302,  1304,  1306,  1308,  1310
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     192,     0,    -1,   193,    -1,     1,    -1,   194,    -1,   193,
     194,    -1,   195,    52,    -1,   195,   180,    -1,   195,   180,
      52,    -1,   180,    -1,   180,    52,    -1,    58,   195,   180,
      -1,   196,    -1,   257,   196,    -1,   197,    -1,   248,    -1,
     253,    -1,   249,    -1,   250,    -1,   251,    -1,   199,    -1,
     198,    -1,   319,    -1,   283,    -1,   220,    -1,   284,    -1,
     323,    -1,   324,    -1,   231,    -1,   232,    -1,   233,    -1,
     234,    -1,   235,    -1,   236,    -1,   237,    -1,   200,    -1,
     211,    -1,   252,    -1,   285,    -1,   325,    -1,   271,   217,
     216,    -1,   213,   197,    -1,   213,   248,    -1,   213,   250,
      -1,   213,   251,    -1,   213,   249,    -1,   213,   252,    -1,
     215,   196,    -1,   201,    -1,   212,    -1,    14,   202,    -1,
      15,    -1,    16,    -1,    17,    -1,    18,    -1,   203,    -1,
     204,    -1,    -1,    19,    -1,    66,    -1,    21,    66,    -1,
      22,   121,    -1,    23,   121,    -1,   197,    -1,   248,    -1,
     249,    -1,   251,    -1,   250,    -1,   325,    -1,   237,    -1,
     252,    -1,   181,    86,    -1,   206,   182,   181,    86,    -1,
     181,    86,    -1,   207,    -1,   205,    -1,    31,   330,    29,
      -1,    32,   330,    -1,    32,   330,    33,    -1,   209,   208,
      -1,   210,   206,    -1,    15,    31,   330,    -1,    34,    31,
     330,    -1,    24,    -1,    25,    -1,    26,    -1,    27,    -1,
      55,    -1,    28,    -1,    56,    -1,    57,    -1,   214,    -1,
     214,    86,    -1,    87,    -1,    89,    -1,    90,    -1,    88,
      -1,    -1,    29,   244,    -1,    -1,   241,    -1,     5,    82,
      -1,     5,    82,   217,    29,   244,    -1,     5,    82,   241,
      -1,   169,    -1,   169,    73,   330,    -1,   218,    -1,   219,
      -1,   229,    -1,   230,    -1,   221,    -1,   228,    -1,   160,
     222,    -1,   227,    -1,   166,    -1,   167,    -1,   163,    -1,
     225,    -1,   226,    -1,   170,   222,    -1,   171,   222,    -1,
     168,   238,    -1,   214,   330,    -1,    29,   330,    -1,    29,
     330,    29,   330,    -1,    29,   330,   183,   330,    -1,    -1,
     138,    86,    -1,    -1,    29,    86,    -1,   164,   224,   223,
      -1,   165,   224,   223,    -1,    11,    19,    20,   224,   223,
      -1,   162,    -1,   159,    -1,   159,    29,    86,    -1,   159,
      74,   161,   184,    86,    -1,   159,    29,    86,    74,   161,
     184,    86,    -1,    83,    -1,    84,   184,   121,    -1,    94,
      -1,    95,    -1,    96,    -1,    97,    -1,    98,    -1,    99,
      -1,    13,   238,   185,   239,   186,    -1,   330,    -1,   330,
     183,   330,    -1,   330,   183,   330,   183,   330,    -1,    -1,
     240,    -1,   239,   182,   240,    -1,    86,    -1,   121,    -1,
     102,    -1,   181,    86,    -1,   181,   181,    86,    -1,    51,
      -1,   242,    -1,   241,   242,    -1,   243,    -1,   185,   186,
      -1,   185,   197,   186,    -1,   185,   241,   186,    -1,   327,
      -1,   245,    -1,   197,    -1,    -1,   185,   247,   186,    -1,
      86,    -1,   247,   182,    86,    -1,   274,   328,   326,    -1,
     274,   328,   326,   246,   245,    -1,   276,   244,    -1,   272,
     244,    -1,   273,   282,    29,   244,    -1,   277,   328,    -1,
      12,   254,    -1,   255,   182,   254,    -1,   255,    -1,   181,
      86,   184,   256,    -1,   181,   181,   100,    86,   184,   256,
      -1,   100,    86,   184,   256,    -1,   181,   181,    86,   184,
     256,    -1,   181,   181,   101,    86,   184,   256,    -1,   101,
      86,   184,   256,    -1,    86,   184,   256,    -1,    86,    -1,
     121,    -1,   102,    -1,   258,    -1,   258,   257,    -1,    42,
     259,    43,    -1,    42,   107,   267,   266,    43,    -1,    42,
     104,   184,   270,   266,    43,    -1,    42,   116,   184,   270,
     266,    43,    -1,    42,   103,   184,   270,   266,    43,    -1,
      42,   105,   184,   270,   266,    43,    -1,    42,   106,   184,
     270,   266,    43,    -1,    42,    85,    86,   184,   269,   266,
      43,    -1,    42,   110,   184,   268,   266,    43,    -1,    42,
     111,   184,   268,   266,    43,    -1,    42,   108,   184,   270,
     266,    43,    -1,    42,   109,   184,   270,   266,    43,    -1,
      42,   113,   114,   184,   187,   261,   188,    43,    -1,    42,
     113,   115,   184,   187,   263,   188,    43,    -1,    42,   112,
     184,   270,   266,    43,    -1,    -1,   259,   260,    -1,    44,
      86,    -1,    45,    86,    -1,    86,    -1,   262,   182,   261,
      -1,   262,    -1,   103,   185,   270,   186,    -1,   116,   185,
     270,   186,    -1,   117,   185,   186,    -1,   117,   185,   119,
     184,   270,   186,    -1,   118,   185,   186,    -1,   120,   185,
     264,   186,    -1,    70,   185,   268,   186,    -1,    70,   185,
     268,   189,   268,   186,    -1,   265,   182,   264,    -1,   265,
      -1,    86,   184,   270,    -1,    -1,   266,   182,   267,    -1,
     103,   184,   270,    -1,   104,   184,   270,    -1,   116,   184,
     270,    -1,   105,   184,   270,    -1,   106,   184,   270,    -1,
     110,   184,   268,    -1,   111,   184,   268,    -1,   108,   184,
     270,    -1,   109,   184,   270,    -1,   112,   184,   270,    -1,
      86,   183,    86,   184,   269,    -1,    86,   184,   269,    -1,
      -1,   270,    -1,    -1,   270,    -1,    86,    -1,    91,    -1,
       5,    -1,    35,   278,    -1,     8,    -1,    36,   278,    -1,
       6,    -1,    37,   278,    -1,     7,   275,    -1,    38,   278,
     275,    -1,    -1,   132,    -1,   132,    54,    -1,     9,    -1,
      39,   278,    -1,    10,    -1,    40,   278,    -1,   279,    41,
      -1,    -1,   280,   279,    -1,    46,   185,   121,   186,    -1,
     121,    -1,    47,   185,   281,   186,    -1,    66,   185,    86,
      86,   186,    -1,    86,    -1,    86,   185,   121,   186,    -1,
      86,   185,    86,   186,    -1,    86,   185,    86,    86,   186,
      -1,    -1,    48,    -1,    49,    -1,    50,    -1,    -1,    71,
      -1,    11,   318,    68,    -1,    11,   318,    69,    -1,    11,
      70,    -1,    11,    70,    86,   184,    86,    -1,   289,    -1,
     291,    -1,   292,    -1,   295,    -1,   293,    -1,   297,    -1,
     298,    -1,   299,    -1,   300,    -1,   302,    -1,   303,    -1,
     304,    -1,   305,    -1,   306,    -1,   308,    -1,   309,    -1,
     311,    -1,   312,    -1,   313,    -1,   314,    -1,   315,    -1,
     316,    -1,   317,    -1,   179,    -1,    -1,   125,   121,    -1,
     125,   121,   182,   121,    -1,   125,   121,   126,   121,    -1,
      -1,   138,    86,    -1,    -1,   138,    86,    -1,   122,   290,
      -1,   123,    -1,   124,    -1,   124,   121,   286,    -1,   135,
     287,    -1,   135,   136,   287,    -1,   135,   136,   137,   287,
      -1,   127,    -1,   129,   294,    -1,    -1,   130,    86,    -1,
     130,   138,    86,    -1,   130,   132,    -1,   138,    86,    -1,
     128,   296,    -1,    -1,   130,   287,    -1,   130,   121,   287,
      -1,   133,   287,    -1,   133,   121,   287,    -1,   131,   287,
      -1,   131,   121,   287,    -1,   131,   132,   287,    -1,   131,
     132,   121,   287,    -1,   134,   287,    -1,   139,    -1,   139,
     121,    -1,   140,   287,    -1,   140,   158,   287,    -1,   141,
     287,    -1,   142,   301,    -1,    -1,    86,    -1,   132,    -1,
     132,    86,    -1,   143,   288,    -1,   143,   145,   288,    -1,
     143,   144,    -1,   147,    -1,   147,    86,    -1,   148,    -1,
     148,   149,    -1,   150,   286,    -1,   150,   121,    -1,   151,
     307,    -1,    -1,   121,    -1,   121,   182,   121,    -1,   121,
     182,   121,   182,    86,    -1,   152,   287,    -1,   152,   153,
     287,    -1,   154,   310,    -1,    -1,   121,    -1,   121,   121,
      -1,   155,   156,    -1,   155,   157,    -1,   155,   158,    -1,
     172,    12,    86,   184,    -1,   172,    12,    86,   184,    86,
      -1,   172,    12,    86,   184,   121,    -1,   173,     6,    86,
      -1,   174,    -1,   175,   121,    -1,   175,   121,   121,    -1,
     176,    86,   121,    -1,   176,    86,    -1,   177,   121,    -1,
     177,   124,   121,    -1,   177,   178,   121,    -1,    -1,    72,
     190,    -1,    58,    -1,    59,    60,   320,    -1,    67,    58,
      86,    -1,    67,    59,    86,    -1,    -1,   321,    -1,   322,
      -1,   321,   182,   322,    -1,    61,    62,    -1,    63,    64,
      65,    -1,    92,    86,    -1,    93,    86,    -1,    86,    -1,
      -1,   146,    86,    -1,   146,   185,    86,   186,    -1,   144,
      86,    -1,   144,   185,    86,   186,    -1,   328,   326,    -1,
     330,    -1,   330,   183,   330,    -1,   330,   330,    -1,   330,
     183,   330,   330,    -1,   330,    73,   330,    -1,   330,   183,
     330,    73,   330,    -1,    59,    -1,    67,    -1,    58,    -1,
      60,    -1,    64,    -1,    69,    -1,    68,    -1,    72,    -1,
      71,    -1,    70,    -1,   123,    -1,   124,    -1,   126,    -1,
     130,    -1,   131,    -1,   133,    -1,   136,    -1,   137,    -1,
     149,    -1,   153,    -1,   157,    -1,   158,    -1,   178,    -1,
     161,    -1,    55,    -1,    56,    -1,    57,    -1,    86,    -1,
     329,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   335,   335,   336,   338,   339,   341,   342,   343,   344,
     345,   346,   348,   349,   351,   352,   353,   354,   355,   356,
     357,   358,   359,   360,   361,   362,   363,   364,   365,   366,
     367,   368,   369,   370,   371,   372,   373,   374,   375,   376,
     378,   380,   381,   382,   383,   384,   385,   387,   389,   390,
     392,   393,   394,   395,   396,   397,   398,   400,   401,   402,
     403,   405,   411,   417,   418,   419,   420,   421,   422,   423,
     424,   426,   433,   441,   449,   450,   453,   459,   464,   470,
     473,   476,   481,   487,   488,   489,   490,   491,   492,   493,
     494,   496,   497,   499,   500,   501,   503,   505,   506,   508,
     509,   511,   512,   513,   515,   516,   518,   519,   520,   521,
     522,   524,   525,   526,   527,   528,   529,   530,   531,   532,
     533,   534,   535,   542,   546,   551,   557,   558,   560,   561,
     563,   567,   572,   577,   579,   580,   585,   590,   597,   600,
     606,   607,   608,   609,   610,   611,   614,   616,   620,   625,
     633,   636,   641,   646,   651,   656,   661,   666,   671,   679,
     680,   682,   684,   685,   686,   688,   689,   691,   693,   694,
     696,   697,   699,   703,   704,   705,   706,   707,   712,   714,
     715,   717,   721,   725,   729,   733,   737,   741,   745,   750,
     755,   761,   762,   764,   765,   766,   767,   768,   769,   770,
     771,   777,   778,   779,   780,   781,   782,   783,   784,   785,
     787,   788,   789,   791,   792,   794,   799,   804,   805,   806,
     807,   809,   810,   812,   813,   815,   823,   824,   826,   827,
     828,   829,   830,   831,   832,   833,   834,   835,   836,   842,
     844,   845,   847,   848,   850,   851,   853,   854,   855,   856,
     857,   858,   859,   860,   862,   863,   864,   866,   867,   868,
     869,   870,   871,   872,   874,   875,   876,   877,   882,   883,
     884,   885,   887,   888,   889,   890,   892,   893,   896,   897,
     898,   899,   902,   903,   904,   905,   906,   907,   908,   909,
     910,   911,   912,   913,   914,   915,   916,   917,   918,   919,
     920,   921,   922,   923,   924,   926,   931,   933,   937,   942,
     950,   951,   955,   956,   959,   961,   962,   963,   967,   968,
     969,   973,   975,   977,   978,   979,   980,   981,   984,   986,
     987,   988,   989,   990,   991,   992,   993,   994,   995,   999,
    1000,  1004,  1005,  1010,  1013,  1015,  1016,  1017,  1018,  1022,
    1023,  1024,  1028,  1029,  1033,  1034,  1038,  1039,  1042,  1044,
    1045,  1046,  1047,  1051,  1052,  1055,  1057,  1058,  1059,  1063,
    1064,  1065,  1069,  1070,  1071,  1075,  1079,  1083,  1084,  1088,
    1089,  1093,  1094,  1095,  1098,  1099,  1102,  1106,  1107,  1108,
    1110,  1111,  1113,  1114,  1117,  1118,  1121,  1127,  1130,  1132,
    1133,  1134,  1135,  1136,  1138,  1143,  1146,  1150,  1154,  1159,
    1163,  1169,  1170,  1171,  1172,  1173,  1174,  1175,  1176,  1177,
    1178,  1179,  1180,  1181,  1182,  1183,  1184,  1185,  1186,  1187,
    1188,  1189,  1190,  1191,  1192,  1193,  1194,  1195,  1197,  1198
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
  "NAME_OB_DOT", "NAME_OB", "EXPLAIN", "EXPLAIN_ROUTE", "DESC", "DESCRIBE",
  "NAME_STR", "USE", "HELP", "SET_NAMES", "SET_CHARSET", "SET_PASSWORD",
  "SET_DEFAULT", "SET_OB_READ_CONSISTENCY", "SET_TX_READ_ONLY", "GLOBAL",
  "SESSION", "NUMBER_VAL", "GROUP_ID", "TABLE_ID", "ELASTIC_ID",
  "TESTLOAD", "ODP_COMMENT", "TNT_ID", "DISASTER_STATUS", "TRACE_ID",
  "RPC_ID", "TARGET_DB_SERVER", "DBP_COMMENT", "ROUTE_TAG", "SYS_TAG",
  "TABLE_NAME", "SCAN_ALL", "STICKY_SESSION", "PARALL", "SHARD_KEY",
  "INT_NUM", "SHOW_PROXYNET", "THREAD", "CONNECTION", "LIMIT", "OFFSET",
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
  "SHOW_BINLOG_SERVER_FOR_TENANT", "';'", "'@'", "','", "'.'", "'='",
  "'('", "')'", "'{'", "'}'", "'#'", "'*'", "$accept", "root", "sql_stmts",
  "sql_stmt", "comment_stmt", "stmt", "select_stmt", "explain_stmt",
  "explain_route_stmt", "ddl_stmt", "mysql_ddl_stmt", "create_dll_expr",
  "stop_ddl_task_stmt", "retry_ddl_task_stmt", "text_ps_from_stmt",
  "text_ps_execute_using_var_list", "text_ps_prepare_var_list",
  "text_ps_prepare_args_stmt", "text_ps_prepare_stmt",
  "text_ps_execute_stmt", "text_ps_stmt", "oracle_ddl_stmt",
  "explain_or_desc_stmt", "explain_or_desc", "explain_route", "opt_from",
  "select_expr_list", "select_tx_read_only_stmt",
  "select_proxy_version_stmt", "hooked_stmt", "shard_special_stmt",
  "db_tb_stmt", "opt_show_like", "opt_show_from", "show_tables_stmt",
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
     197,   198,   198,   198,   198,   198,   198,   199,   200,   200,
     201,   201,   201,   201,   201,   201,   201,   202,   202,   202,
     202,   203,   204,   205,   205,   205,   205,   205,   205,   205,
     205,   206,   206,   207,   208,   208,   209,   210,   210,   211,
     211,   211,   211,   212,   212,   212,   212,   212,   212,   212,
     212,   213,   213,   214,   214,   214,   215,   216,   216,   217,
     217,   218,   218,   218,   219,   219,   220,   220,   220,   220,
     220,   221,   221,   221,   221,   221,   221,   221,   221,   221,
     221,   221,   221,   222,   222,   222,   223,   223,   224,   224,
     225,   225,   226,   227,   228,   228,   228,   228,   229,   230,
     231,   232,   233,   234,   235,   236,   237,   238,   238,   238,
     239,   239,   239,   240,   240,   240,   240,   240,   240,   241,
     241,   242,   243,   243,   243,   244,   244,   245,   246,   246,
     247,   247,   248,   248,   249,   250,   251,   252,   253,   254,
     254,   255,   255,   255,   255,   255,   255,   255,   256,   256,
     256,   257,   257,   258,   258,   258,   258,   258,   258,   258,
     258,   258,   258,   258,   258,   258,   258,   258,   259,   259,
     260,   260,   260,   261,   261,   262,   262,   262,   262,   262,
     262,   263,   263,   264,   264,   265,   266,   266,   267,   267,
     267,   267,   267,   267,   267,   267,   267,   267,   267,   267,
     268,   268,   269,   269,   270,   270,   271,   271,   272,   272,
     273,   273,   274,   274,   275,   275,   275,   276,   276,   277,
     277,   278,   279,   279,   280,   280,   280,   280,   280,   280,
     280,   280,   281,   281,   281,   281,   282,   282,   283,   283,
     283,   283,   284,   284,   284,   284,   284,   284,   284,   284,
     284,   284,   284,   284,   284,   284,   284,   284,   284,   284,
     284,   284,   284,   284,   284,   285,   286,   286,   286,   286,
     287,   287,   288,   288,   289,   290,   290,   290,   291,   291,
     291,   292,   293,   294,   294,   294,   294,   294,   295,   296,
     296,   296,   296,   296,   296,   296,   296,   296,   296,   297,
     297,   298,   298,   299,   300,   301,   301,   301,   301,   302,
     302,   302,   303,   303,   304,   304,   305,   305,   306,   307,
     307,   307,   307,   308,   308,   309,   310,   310,   310,   311,
     311,   311,   312,   312,   312,   313,   314,   315,   315,   316,
     316,   317,   317,   317,   318,   318,   319,   319,   319,   319,
     320,   320,   321,   321,   322,   322,   323,   324,   325,   326,
     326,   326,   326,   326,   327,   328,   328,   328,   328,   328,
     328,   329,   329,   329,   329,   329,   329,   329,   329,   329,
     329,   329,   329,   329,   329,   329,   329,   329,   329,   329,
     329,   329,   329,   329,   329,   329,   329,   329,   330,   330
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     1,     1,     1,     2,     2,     2,     3,     1,
       2,     3,     1,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       3,     2,     2,     2,     2,     2,     2,     2,     1,     1,
       2,     1,     1,     1,     1,     1,     1,     0,     1,     1,
       2,     2,     2,     1,     1,     1,     1,     1,     1,     1,
       1,     2,     4,     2,     1,     1,     3,     2,     3,     2,
       2,     3,     3,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     2,     1,     1,     1,     1,     0,     2,     0,
       1,     2,     5,     3,     1,     3,     1,     1,     1,     1,
       1,     1,     2,     1,     1,     1,     1,     1,     1,     2,
       2,     2,     2,     2,     4,     4,     0,     2,     0,     2,
       3,     3,     5,     1,     1,     3,     5,     7,     1,     3,
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
       1,     1,     1,     1,     1,     1,     0,     2,     4,     4,
       0,     2,     0,     2,     2,     1,     1,     3,     2,     3,
       4,     1,     2,     0,     2,     3,     2,     2,     2,     0,
       2,     3,     2,     3,     2,     3,     3,     4,     2,     1,
       2,     2,     3,     2,     2,     0,     1,     1,     2,     2,
       3,     2,     1,     2,     1,     2,     2,     2,     2,     0,
       1,     3,     5,     2,     3,     2,     0,     1,     2,     2,
       2,     2,     4,     5,     5,     3,     1,     2,     3,     3,
       2,     2,     3,     3,     0,     2,     1,     3,     3,     3,
       0,     1,     1,     3,     2,     3,     2,     2,     1,     0,
       2,     4,     2,     4,     2,     1,     3,     2,     4,     3,
       5,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
       0,     3,   246,   250,   254,   248,   257,   259,   384,     0,
       0,    57,    51,    52,    53,    54,     0,     0,    83,    84,
      85,    86,    88,     0,     0,     0,   262,   262,   262,   262,
     262,   262,   208,    87,    89,    90,   386,     0,     0,   138,
       0,   398,    93,    96,    94,    95,     0,     0,   140,   141,
     142,   143,   144,   145,     0,   321,   329,   323,   310,   339,
     310,   310,   345,   312,   352,   354,   306,   359,   310,   366,
       0,   134,     0,   133,   116,   128,   128,   114,   115,     0,
     104,     0,     0,     0,     0,   376,     0,     0,     0,   305,
       9,     0,     2,     4,     0,    12,    14,    21,    20,    35,
      48,    55,    56,     0,     0,    36,    49,     0,    91,     0,
     106,   107,    24,   110,   117,   118,   113,   111,   108,   109,
      28,    29,    30,    31,    32,    33,    34,    15,    17,    18,
      19,    37,    16,     0,   191,    99,     0,   276,     0,     0,
       0,    23,    25,    38,   282,   283,   284,   286,   285,   287,
     288,   289,   290,   291,   292,   293,   294,   295,   296,   297,
     298,   299,   300,   301,   302,   303,   304,    22,    26,    27,
      39,   101,   255,   252,     0,   280,     0,     0,     0,     0,
       0,     0,   178,   180,   435,   436,   437,   413,   411,   414,
     415,   412,   417,   416,   420,   419,   418,   438,   421,   422,
     423,   424,   425,   426,   427,   428,   429,   430,   431,   432,
     434,   433,     0,   439,   147,    58,     0,    59,    50,     0,
      61,    62,     0,    77,     0,     0,     0,     0,   268,   265,
     247,     0,   262,   249,   251,   254,   258,   260,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   386,     0,   390,     0,     0,     0,   396,   397,
     315,   316,   314,   310,   310,   310,   310,   328,     0,     0,
     322,   310,     0,   318,   340,   310,   341,   343,   346,   347,
     344,     0,   351,   312,   349,   353,   355,   357,     0,   356,
     360,   358,   310,   363,   367,   365,   369,   370,   371,     0,
       0,     0,   112,     0,   126,   126,   121,     0,   119,   120,
       0,     0,   377,   380,   381,     0,     0,    10,     1,     5,
       6,     7,   246,     0,    63,    75,    74,    79,    69,    64,
      65,    67,    66,    70,    68,     0,    80,    41,    42,    45,
      43,    44,    46,    92,   122,    47,    13,   192,     0,    97,
     100,   159,   161,   167,   175,   166,   165,   399,   405,   277,
       0,   399,   174,   177,     0,   103,   256,   128,     0,   385,
     278,   279,     0,     0,     0,     0,     0,     0,   150,     0,
      60,    81,    76,    78,    82,     0,   272,     0,     0,   261,
     263,   253,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   226,     0,
       0,   240,   240,     0,     0,     0,     0,   193,     0,     0,
     212,   209,    11,     0,     0,   387,   391,   392,   388,   389,
     139,   306,   310,   330,   310,   310,   334,   310,   332,   338,
     324,   326,     0,   327,   310,   319,   311,   342,   348,   313,
     350,   307,     0,   364,   368,   135,     0,   123,   129,     0,
     130,   131,   105,     0,   375,   378,   379,   382,   383,     8,
      73,    71,     0,   162,     0,     0,     0,    40,   160,     0,
       0,   404,     0,     0,   407,     0,   168,     0,   126,     0,
     188,   190,   189,   187,     0,     0,     0,     0,     0,     0,
     179,   158,   153,   155,   154,     0,     0,   151,   148,     0,
     273,   274,   275,     0,     0,     0,     0,   242,   244,   245,
     226,   226,   226,   226,     0,   242,     0,     0,     0,     0,
       0,     0,   240,   240,     0,     0,     0,   226,   226,   226,
     241,   226,   226,     0,     0,   226,   210,   211,   394,     0,
       0,   317,   331,   335,   310,   336,   333,   325,   320,     0,
       0,   361,     0,     0,     0,     0,   127,   372,     0,   163,
     164,    98,   402,     0,   400,     0,   409,   406,   176,     0,
       0,   102,   132,   281,   183,   186,   181,     0,     0,     0,
     156,     0,     0,   146,     0,   264,   266,     0,     0,   270,
     269,   226,   243,     0,     0,     0,     0,     0,   239,   228,
     229,   231,   232,   235,   236,   233,   234,   237,   230,   194,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   395,
     393,   337,   309,   308,     0,     0,   136,   124,   125,   373,
     374,    72,     0,     0,     0,   408,   170,     0,   173,   184,
       0,     0,   157,   152,   149,   267,   271,     0,   197,   195,
     198,   199,   242,   227,   203,   204,   201,   202,   207,     0,
       0,     0,     0,     0,     0,   214,     0,     0,   196,   362,
       0,   403,   401,   410,     0,   169,   182,   185,   200,   238,
       0,     0,     0,     0,     0,     0,     0,   240,     0,   137,
     171,     0,     0,     0,   217,   219,     0,     0,   224,   205,
     213,     0,   206,   215,   216,     0,     0,   220,     0,   221,
     240,     0,   225,   223,     0,   218,   222
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    91,    92,    93,    94,    95,   353,    97,    98,    99,
     100,   218,   101,   102,   325,   336,   326,   327,   103,   104,
     105,   106,   107,   108,   109,   477,   349,   110,   111,   112,
     113,   302,   460,   304,   114,   115,   116,   117,   118,   119,
     120,   121,   122,   123,   124,   125,   126,   212,   506,   507,
     350,   351,   352,   354,   355,   580,   647,   127,   128,   129,
     130,   131,   132,   182,   183,   493,   133,   134,   251,   421,
     674,   675,   677,   707,   708,   536,   408,   539,   601,   540,
     135,   136,   137,   138,   173,   139,   140,   230,   231,   232,
     513,   360,   141,   142,   143,   289,   273,   284,   144,   262,
     145,   146,   147,   270,   148,   267,   149,   150,   151,   152,
     280,   153,   154,   155,   156,   157,   291,   158,   159,   295,
     160,   161,   162,   163,   164,   165,   166,   177,   167,   425,
     426,   427,   168,   169,   170,   481,   356,   357,   213,   358
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -503
static const yytype_int16 yypact[] =
{
     423,  -503,    38,  -503,   -55,  -503,  -503,  -503,    37,   117,
    1225,    55,   150,  -503,  -503,  -503,   -24,    68,  -503,  -503,
    -503,  -503,  -503,  1225,  1225,   180,   179,   179,   179,   179,
     179,   179,   250,  -503,  -503,  -503,   777,    75,   271,  -503,
      17,  -503,  -503,  -503,  -503,  -503,    62,   146,  -503,  -503,
    -503,  -503,  -503,  -503,    52,  -503,   156,   -51,   158,   128,
      21,   134,   -18,   119,   192,   144,   125,   178,   105,   204,
     104,    44,   307,  -503,  -503,   315,   315,  -503,  -503,  1225,
     272,   307,   307,   334,   341,  -503,   228,   279,   -46,  -503,
     316,   367,   601,  -503,    19,  -503,  -503,  -503,  -503,  -503,
    -503,  -503,  -503,    27,   189,  -503,  -503,   275,  1261,   952,
    -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,  -503,  -503,   952,   329,   187,    98,   303,  1225,    98,
    1225,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,   -13,   323,  -503,   359,   294,   202,    23,   207,   309,
     310,   -36,  -503,   211,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,  -503,   212,  -503,   215,  -503,   333,  -503,  -503,  1225,
    -503,  -503,   371,   368,  1225,   218,   219,   220,   221,  -503,
    -503,   361,   179,  -503,  -503,   -55,  -503,  -503,   321,   224,
     225,   226,   227,   278,   229,   230,   231,   232,   233,   217,
     234,   193,  -503,   240,   246,   326,   335,   298,  -503,  -503,
    -503,   301,  -503,    40,    -4,    81,   134,  -503,   -39,   337,
    -503,   196,   339,  -503,  -503,   134,  -503,  -503,  -503,   340,
    -503,   356,  -503,   289,  -503,  -503,  -503,  -503,   322,  -503,
     262,  -503,   134,  -503,   331,  -503,  -503,  -503,  -503,   370,
     292,  1225,  -503,   378,   328,   328,  -503,  1225,  -503,  -503,
     381,   382,   348,   349,  -503,   350,   353,  -503,  -503,  -503,
    -503,   424,  -503,   389,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,  -503,  -503,  -503,  -503,   391,   302,  -503,  -503,  -503,
    -503,  -503,  -503,    20,  -503,  -503,  -503,  -503,     7,   454,
     187,  -503,  -503,  -503,  -503,  -503,  -503,   173,  1077,  -503,
     456,   173,  -503,  -503,   457,   -12,  -503,   315,   304,  -503,
    -503,  -503,    -6,   305,   308,   312,   153,   117,   -32,  1225,
    -503,  -503,  -503,  -503,  -503,   366,   273,   405,     2,  -503,
    -503,  -503,   313,    33,    33,    33,    33,   154,   314,   317,
     318,   319,   320,   324,   330,   342,   343,   344,  -503,    33,
      33,    33,    33,    33,   345,   346,    33,  -503,   407,   408,
    -503,  -503,  -503,   437,   436,  -503,   351,  -503,  -503,  -503,
    -503,   380,   134,  -503,   134,    92,  -503,   134,  -503,  -503,
    -503,  -503,   438,  -503,   134,  -503,  -503,  -503,  -503,  -503,
    -503,   -66,   402,  -503,  -503,   451,   347,    -9,  -503,   446,
    -503,  -503,  -503,   352,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,  -503,   354,  -503,   355,   157,    98,  -503,  -503,   -41,
     -33,  -503,  1225,  1225,  -503,    98,    30,    98,   328,   448,
    -503,  -503,  -503,  -503,    -6,    -6,    -6,   358,   452,   453,
    -503,  -503,  -503,  -503,  -503,   -31,    66,  -503,   357,   360,
    -503,  -503,  -503,   362,   458,   -60,   363,    33,  -503,  -503,
    -503,  -503,  -503,  -503,   461,    33,    33,    33,    33,    33,
      33,    33,    33,    33,    33,    33,   -19,  -503,  -503,  -503,
    -503,  -503,  -503,   369,   372,  -503,  -503,  -503,  -503,   472,
     246,  -503,  -503,  -503,   134,  -503,  -503,  -503,  -503,   422,
     432,   375,   399,   475,  1225,  1225,  -503,     4,   482,  -503,
    -503,  -503,  -503,   483,  -503,   486,  -503,  1113,  -503,   490,
      76,  -503,  -503,  -503,  -503,  -503,  -503,    -6,   395,   396,
    -503,   495,   -32,  -503,  1225,  -503,  -503,   415,   418,  -503,
    -503,  -503,  -503,    -5,    -2,     0,     1,   421,  -503,  -503,
    -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,
     278,     5,     6,     8,     9,    14,   124,   550,    15,  -503,
    -503,  -503,  -503,  -503,   535,   447,  -503,  -503,  -503,  -503,
    -503,  -503,   444,   459,  1225,  -503,  -503,    89,  -503,  -503,
      -6,    -6,  -503,  -503,  -503,  -503,  -503,    16,  -503,  -503,
    -503,  -503,    33,  -503,  -503,  -503,  -503,  -503,  -503,   462,
     463,   464,   465,   466,   434,   460,   467,   473,  -503,  -503,
     558,  -503,  -503,  -503,   560,  -503,  -503,  -503,  -503,  -503,
      33,    33,   -92,   468,   567,   612,   124,    33,   619,  -503,
    -503,   477,   478,   481,  -503,  -503,   485,   480,   488,  -503,
    -503,    34,  -503,  -503,  -503,    33,    33,  -503,   567,  -503,
      33,   487,  -503,  -503,   489,  -503,  -503
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -503,  -503,  -503,   575,   635,    18,     3,  -503,  -503,  -503,
    -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,  -503,  -503,  -503,  -503,  -503,   501,  -503,  -503,  -503,
    -503,   258,  -294,   -61,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,  -503,  -503,  -503,  -503,  -503,   571,   597,  -503,    85,
    -153,  -304,  -503,  -135,    99,  -503,  -503,   170,   185,   198,
     201,   213,  -503,   306,  -503,  -465,   544,  -503,  -503,  -503,
     -16,  -503,  -503,   -37,  -503,  -437,    72,  -402,  -502,  -388,
    -503,  -503,  -503,  -503,   469,  -503,  -503,   239,   450,  -503,
    -503,  -503,  -503,  -503,  -503,   255,   -59,   419,  -503,  -503,
    -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,  -503,
    -503,   151,  -503,  -503,   600,   364,  -503,   186,  -503,   -10
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -439
static const yytype_int16 yytable[] =
{
     214,   276,   277,    96,   362,   520,   521,   522,   523,   293,
     541,   461,   322,   222,   223,   305,   -99,  -100,   365,   501,
     564,   537,   538,   608,   619,   542,   598,   703,   545,   584,
     585,   586,   322,     3,     4,     5,     6,     7,   658,    96,
      10,   659,    26,   660,   661,   572,   478,   440,   664,   665,
     375,   666,   667,   574,   502,   590,   174,   668,   678,   688,
     559,   478,    26,    27,    28,    29,    30,    31,   278,   214,
     503,   320,  -438,   299,   215,   314,   216,   172,   315,   268,
     490,   322,  -172,   603,   604,   605,   606,   269,   515,   504,
     639,   370,   371,   441,   704,    96,   491,   220,   344,   442,
     621,   622,   623,   322,   624,   625,   324,   175,   628,   176,
     337,    26,    96,    41,   279,   492,   560,   434,   300,   518,
     171,   217,   649,   516,   519,   640,   599,   345,   435,   602,
     615,   616,   316,    26,   272,   254,    96,   602,   609,   610,
     611,   612,   613,   614,   573,   376,   617,   618,   258,   505,
     591,   346,   575,   184,   185,   186,   187,   188,   189,   272,
     689,   432,   190,   620,   657,   191,   192,   193,   194,   195,
     196,   478,   348,   348,   565,   260,   261,   620,   272,   275,
     620,   219,   620,   620,   197,   686,   687,   620,   620,   221,
     620,   620,   348,   473,   582,   475,   620,   620,   620,   321,
    -438,   257,   437,   178,   433,   436,   438,   439,   323,   381,
    -172,   224,   445,   554,   384,   579,   447,   179,   180,   272,
     719,   198,   199,   720,   200,   225,   226,   669,   201,   202,
     272,   203,   259,   453,   204,   205,   417,   418,   419,   497,
     670,   671,   672,   272,   673,   227,   287,   206,   592,   274,
     288,   207,   593,   498,   499,   208,   209,   281,   292,   210,
     296,   297,   298,   282,   283,   228,   233,   234,   235,   236,
     237,   684,   272,   329,   602,   685,   211,   338,   285,   420,
     322,     3,     4,     5,     6,     7,   263,   264,   330,   265,
     266,   457,   339,   286,   271,   711,   272,   462,   181,   290,
     229,   331,   701,   702,   332,   340,   488,   423,   341,   424,
      26,    27,    28,    29,    30,    31,   333,   479,   724,   480,
     342,   510,   511,   512,   361,   294,   363,   721,   722,   255,
     256,   414,   415,   444,   272,   238,   301,   524,   525,   308,
     309,   571,   348,   570,   303,   307,   310,   311,   484,   312,
     578,   474,   581,   239,   240,   241,   242,   243,   244,   245,
     246,   247,   248,   249,   397,   313,   250,   318,   317,   508,
     335,    32,   348,   552,   359,   553,   555,   366,   556,   367,
     368,   398,   399,   400,   401,   558,   402,   403,   404,   405,
     406,   372,   369,   377,   407,   373,   374,   378,   379,   380,
     382,   383,   389,   385,   386,   387,   388,   392,   393,   394,
     395,   396,   428,   409,   410,   411,   412,   413,   416,   430,
     422,   429,   431,   443,     1,   446,   448,   281,     2,     3,
       4,     5,     6,     7,     8,     9,    10,    11,    12,    13,
      14,    15,   449,   451,   452,    16,    17,    18,    19,    20,
      21,    22,   454,   456,    23,    24,   455,    25,    26,    27,
      28,    29,    30,    31,   458,    32,   459,   463,   464,   465,
     466,   467,   576,   577,   468,   470,   469,   471,    33,    34,
      35,    36,    37,   476,   472,   485,   487,   509,   489,   494,
      38,   514,   495,   546,   547,   631,   496,   517,   526,   548,
     549,   527,   528,   529,   530,   288,    39,    40,   531,    41,
      42,    43,    44,    45,   532,    46,    47,    48,    49,    50,
      51,    52,    53,   561,   557,   562,   533,   534,   535,   543,
     544,   563,   566,   550,   583,   568,   567,   629,   588,   589,
     594,   569,   587,   632,   597,    54,   595,   607,   596,   600,
      55,    56,    57,   633,   637,   638,   626,   634,    58,   627,
     635,   636,    59,    60,    61,    62,    63,   645,   641,   642,
      64,    65,   643,    66,    67,    68,   646,    69,    70,   650,
     651,   652,    71,    72,   654,    73,    74,    75,    76,    77,
      78,    79,    80,    81,    82,    83,    84,    85,    86,    87,
      88,   655,    89,    90,   656,   662,     2,     3,     4,     5,
       6,     7,     8,     9,    10,    11,    12,    13,    14,    15,
     676,   679,   695,    16,    17,    18,    19,    20,    21,    22,
     681,   680,    23,    24,   683,    25,    26,    27,    28,    29,
      30,    31,   696,    32,   699,   682,   700,   690,   691,   692,
     693,   694,   697,   706,   705,   709,    33,    34,    35,    36,
      37,   698,   712,   713,   714,   715,   717,   319,    38,   716,
     718,   253,   364,   725,   328,   726,   306,   653,   347,   648,
     710,   723,   390,   500,    39,    40,   551,    41,    42,    43,
      44,    45,   663,    46,    47,    48,    49,    50,    51,    52,
      53,   630,   450,   334,   391,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    54,     0,   486,     0,     0,    55,    56,
      57,     0,     0,     0,     0,     0,    58,     0,     0,     0,
      59,    60,    61,    62,    63,     0,     0,     0,    64,    65,
       0,    66,    67,    68,     0,    69,    70,     0,     0,     0,
      71,    72,     0,    73,    74,    75,    76,    77,    78,    79,
      80,    81,    82,    83,    84,    85,    86,    87,    88,     0,
      89,    90,     2,     3,     4,     5,     6,     7,     8,     9,
      10,    11,    12,    13,    14,    15,     0,     0,     0,    16,
      17,    18,    19,    20,    21,    22,     0,     0,    23,    24,
       0,    25,    26,    27,    28,    29,    30,    31,     0,    32,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    33,    34,    35,   252,    37,     0,     0,     0,
       0,     0,     0,     0,    38,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      39,    40,     0,    41,    42,    43,    44,    45,     0,    46,
      47,    48,    49,    50,    51,    52,    53,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    54,
       0,     0,     0,     0,    55,    56,    57,     0,     0,     0,
       0,     0,    58,     0,     0,     0,    59,    60,    61,    62,
      63,     0,     0,     0,    64,    65,     0,    66,    67,    68,
       0,    69,    70,     0,     0,     0,    71,    72,     0,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,     0,    89,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,     0,     0,     0,    16,    17,    18,    19,    20,    21,
      22,     0,     0,    23,    24,     0,    25,    26,    27,    28,
      29,    30,    31,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    33,    34,    35,
     252,    37,     0,     0,     0,     0,     0,     0,     0,    38,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    39,    40,     0,    41,    42,
      43,    44,    45,     0,    46,    47,    48,    49,    50,    51,
      52,    53,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    54,     0,     0,     0,     0,    55,
      56,    57,     0,     0,     0,     0,     0,    58,     0,     0,
       0,    59,    60,    61,    62,    63,     0,     0,     0,    64,
      65,     0,    66,    67,    68,     0,    69,    70,     0,     0,
       0,    71,    72,     0,    73,    74,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
       0,    89,   184,   185,   186,   187,   188,   189,     0,     0,
       0,   190,     0,     0,   191,   192,   193,   194,   195,   196,
     482,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   197,     0,     0,     0,     0,   184,   185,
     186,   187,   188,   189,     0,     0,     0,   190,     0,     0,
     191,   192,   193,   194,   195,   196,   644,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   197,
     198,   199,     0,   200,     0,     0,     0,   201,   202,     0,
     203,     0,     0,   204,   205,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   206,     0,     0,     0,
     207,     0,     0,     0,   208,   209,   198,   199,   210,   200,
       0,     0,     0,   201,   202,     0,   203,     0,     0,   204,
     205,     0,     0,     0,     0,   211,     0,     0,     0,     0,
     483,     0,   206,     0,     0,     0,   207,     0,     0,     0,
     208,   209,     0,     0,   210,     0,     0,     0,     0,     0,
     184,   185,   186,   187,   188,   189,     0,     0,     0,   190,
       0,   211,   191,   192,   193,   194,   195,   196,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   197,     0,     0,     0,     0,   184,   185,   186,   187,
     188,   189,     0,     0,     0,   190,     0,     0,   191,   192,
     193,   194,   195,   196,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   343,   198,   199,
       0,   200,     0,     0,     0,   201,   202,     0,   203,     0,
       0,   204,   205,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   206,     0,     0,     0,   207,     0,
       0,     0,   208,   209,   198,   199,   210,   200,     0,     0,
       0,   201,   202,     0,   203,     0,     0,   204,   205,     0,
       0,     0,     0,   211,     0,     0,     0,     0,     0,     0,
     206,     0,     0,     0,   207,     0,     0,     0,   208,   209,
       0,     0,   210,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   211
};

static const yytype_int16 yycheck[] =
{
      10,    60,    61,     0,   139,   393,   394,   395,   396,    68,
     412,   305,     5,    23,    24,    76,    29,    29,   171,    51,
      29,   409,   410,   525,    43,   413,    86,   119,   416,   494,
     495,   496,     5,     6,     7,     8,     9,    10,    43,    36,
      13,    43,    35,    43,    43,    86,   350,    86,    43,    43,
      86,    43,    43,    86,    86,    86,    19,    43,    43,    43,
     126,   365,    35,    36,    37,    38,    39,    40,    86,    79,
     102,    52,    52,    29,    19,   121,    21,   132,   124,   130,
      86,     5,    52,   520,   521,   522,   523,   138,    86,   121,
      86,    68,    69,   132,   186,    92,   102,   121,   108,   138,
     537,   538,   539,     5,   541,   542,   103,    70,   545,    72,
     107,    35,   109,    86,   132,   121,   182,   121,    74,    86,
      82,    66,   587,   121,    91,   121,   186,   109,   132,   517,
     532,   533,   178,    35,   138,    60,   133,   525,   526,   527,
     528,   529,   530,   531,   185,   181,   534,   535,    86,   181,
     181,   133,   185,    55,    56,    57,    58,    59,    60,   138,
     662,   121,    64,   182,   601,    67,    68,    69,    70,    71,
      72,   475,   185,   185,   183,   123,   124,   182,   138,   158,
     182,    31,   182,   182,    86,   650,   651,   182,   182,   121,
     182,   182,   185,   186,   488,   348,   182,   182,   182,   180,
     180,   184,   121,    86,   263,   264,   265,   266,   181,   219,
     180,    31,   271,   121,   224,   185,   275,   100,   101,   138,
     186,   123,   124,   189,   126,    46,    47,   103,   130,   131,
     138,   133,    86,   292,   136,   137,    43,    44,    45,    86,
     116,   117,   118,   138,   120,    66,   121,   149,   182,   121,
     125,   153,   186,   100,   101,   157,   158,   138,   153,   161,
     156,   157,   158,   144,   145,    86,    27,    28,    29,    30,
      31,   182,   138,   103,   662,   186,   178,   107,    86,    86,
       5,     6,     7,     8,     9,    10,   130,   131,   103,   133,
     134,   301,   107,   149,   136,   697,   138,   307,   181,   121,
     121,   103,   690,   691,   103,   107,   367,    61,   107,    63,
      35,    36,    37,    38,    39,    40,   103,   144,   720,   146,
     107,    48,    49,    50,   138,   121,   140,   715,   716,    58,
      59,   114,   115,   137,   138,    85,    29,   183,   184,    81,
      82,   476,   185,   186,    29,    73,    12,     6,   358,   121,
     485,   348,   487,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,    86,    86,   116,     0,    52,   379,
     181,    42,   185,   432,    71,   434,   435,    54,   437,    20,
      86,   103,   104,   105,   106,   444,   108,   109,   110,   111,
     112,   184,   190,   182,   116,    86,    86,   185,   183,    66,
      29,    33,    41,   185,   185,   185,   185,    86,   184,   184,
     184,   184,    86,   184,   184,   184,   184,   184,   184,   121,
     180,    86,   121,    86,     1,    86,    86,   138,     5,     6,
       7,     8,     9,    10,    11,    12,    13,    14,    15,    16,
      17,    18,    86,   121,   182,    22,    23,    24,    25,    26,
      27,    28,   121,   161,    31,    32,    86,    34,    35,    36,
      37,    38,    39,    40,    86,    42,   138,    86,    86,   121,
     121,   121,   482,   483,   121,    86,    52,    86,    55,    56,
      57,    58,    59,    29,   182,    29,    29,   121,   184,   184,
      67,    86,   184,    86,    86,   554,   184,   184,   184,    62,
      64,   184,   184,   184,   184,   125,    83,    84,   184,    86,
      87,    88,    89,    90,   184,    92,    93,    94,    95,    96,
      97,    98,    99,   121,    86,    74,   184,   184,   184,   184,
     184,   184,    86,   182,    86,   181,   184,    65,    86,    86,
     183,   186,   184,   121,    86,   122,   186,    86,   186,   186,
     127,   128,   129,   121,   564,   565,   187,   182,   135,   187,
     161,    86,   139,   140,   141,   142,   143,   577,    86,    86,
     147,   148,    86,   150,   151,   152,    86,   154,   155,   184,
     184,    86,   159,   160,   594,   162,   163,   164,   165,   166,
     167,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   186,   179,   180,   186,   184,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,    18,
      70,    86,   188,    22,    23,    24,    25,    26,    27,    28,
     186,   184,    31,    32,   644,    34,    35,    36,    37,    38,
      39,    40,   182,    42,    86,   186,    86,   185,   185,   185,
     185,   185,   185,    86,   186,    43,    55,    56,    57,    58,
      59,   188,    43,   186,   186,   184,   186,    92,    67,   184,
     182,    36,   171,   186,   103,   186,    79,   592,   134,   580,
     696,   718,   232,   377,    83,    84,   431,    86,    87,    88,
      89,    90,   620,    92,    93,    94,    95,    96,    97,    98,
      99,   550,   283,   103,   235,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   122,    -1,   361,    -1,    -1,   127,   128,
     129,    -1,    -1,    -1,    -1,    -1,   135,    -1,    -1,    -1,
     139,   140,   141,   142,   143,    -1,    -1,    -1,   147,   148,
      -1,   150,   151,   152,    -1,   154,   155,    -1,    -1,    -1,
     159,   160,    -1,   162,   163,   164,   165,   166,   167,   168,
     169,   170,   171,   172,   173,   174,   175,   176,   177,    -1,
     179,   180,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    -1,    -1,    -1,    22,
      23,    24,    25,    26,    27,    28,    -1,    -1,    31,    32,
      -1,    34,    35,    36,    37,    38,    39,    40,    -1,    42,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    55,    56,    57,    58,    59,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    67,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      83,    84,    -1,    86,    87,    88,    89,    90,    -1,    92,
      93,    94,    95,    96,    97,    98,    99,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   122,
      -1,    -1,    -1,    -1,   127,   128,   129,    -1,    -1,    -1,
      -1,    -1,   135,    -1,    -1,    -1,   139,   140,   141,   142,
     143,    -1,    -1,    -1,   147,   148,    -1,   150,   151,   152,
      -1,   154,   155,    -1,    -1,    -1,   159,   160,    -1,   162,
     163,   164,   165,   166,   167,   168,   169,   170,   171,   172,
     173,   174,   175,   176,   177,    -1,   179,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,    16,    17,
      18,    -1,    -1,    -1,    22,    23,    24,    25,    26,    27,
      28,    -1,    -1,    31,    32,    -1,    34,    35,    36,    37,
      38,    39,    40,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    55,    56,    57,
      58,    59,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    67,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    83,    84,    -1,    86,    87,
      88,    89,    90,    -1,    92,    93,    94,    95,    96,    97,
      98,    99,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   122,    -1,    -1,    -1,    -1,   127,
     128,   129,    -1,    -1,    -1,    -1,    -1,   135,    -1,    -1,
      -1,   139,   140,   141,   142,   143,    -1,    -1,    -1,   147,
     148,    -1,   150,   151,   152,    -1,   154,   155,    -1,    -1,
      -1,   159,   160,    -1,   162,   163,   164,   165,   166,   167,
     168,   169,   170,   171,   172,   173,   174,   175,   176,   177,
      -1,   179,    55,    56,    57,    58,    59,    60,    -1,    -1,
      -1,    64,    -1,    -1,    67,    68,    69,    70,    71,    72,
      73,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    86,    -1,    -1,    -1,    -1,    55,    56,
      57,    58,    59,    60,    -1,    -1,    -1,    64,    -1,    -1,
      67,    68,    69,    70,    71,    72,    73,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    86,
     123,   124,    -1,   126,    -1,    -1,    -1,   130,   131,    -1,
     133,    -1,    -1,   136,   137,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   149,    -1,    -1,    -1,
     153,    -1,    -1,    -1,   157,   158,   123,   124,   161,   126,
      -1,    -1,    -1,   130,   131,    -1,   133,    -1,    -1,   136,
     137,    -1,    -1,    -1,    -1,   178,    -1,    -1,    -1,    -1,
     183,    -1,   149,    -1,    -1,    -1,   153,    -1,    -1,    -1,
     157,   158,    -1,    -1,   161,    -1,    -1,    -1,    -1,    -1,
      55,    56,    57,    58,    59,    60,    -1,    -1,    -1,    64,
      -1,   178,    67,    68,    69,    70,    71,    72,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    86,    -1,    -1,    -1,    -1,    55,    56,    57,    58,
      59,    60,    -1,    -1,    -1,    64,    -1,    -1,    67,    68,
      69,    70,    71,    72,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    86,   123,   124,
      -1,   126,    -1,    -1,    -1,   130,   131,    -1,   133,    -1,
      -1,   136,   137,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   149,    -1,    -1,    -1,   153,    -1,
      -1,    -1,   157,   158,   123,   124,   161,   126,    -1,    -1,
      -1,   130,   131,    -1,   133,    -1,    -1,   136,   137,    -1,
      -1,    -1,    -1,   178,    -1,    -1,    -1,    -1,    -1,    -1,
     149,    -1,    -1,    -1,   153,    -1,    -1,    -1,   157,   158,
      -1,    -1,   161,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   178
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     1,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    22,    23,    24,    25,
      26,    27,    28,    31,    32,    34,    35,    36,    37,    38,
      39,    40,    42,    55,    56,    57,    58,    59,    67,    83,
      84,    86,    87,    88,    89,    90,    92,    93,    94,    95,
      96,    97,    98,    99,   122,   127,   128,   129,   135,   139,
     140,   141,   142,   143,   147,   148,   150,   151,   152,   154,
     155,   159,   160,   162,   163,   164,   165,   166,   167,   168,
     169,   170,   171,   172,   173,   174,   175,   176,   177,   179,
     180,   192,   193,   194,   195,   196,   197,   198,   199,   200,
     201,   203,   204,   209,   210,   211,   212,   213,   214,   215,
     218,   219,   220,   221,   225,   226,   227,   228,   229,   230,
     231,   232,   233,   234,   235,   236,   237,   248,   249,   250,
     251,   252,   253,   257,   258,   271,   272,   273,   274,   276,
     277,   283,   284,   285,   289,   291,   292,   293,   295,   297,
     298,   299,   300,   302,   303,   304,   305,   306,   308,   309,
     311,   312,   313,   314,   315,   316,   317,   319,   323,   324,
     325,    82,   132,   275,    19,    70,    72,   318,    86,   100,
     101,   181,   254,   255,    55,    56,    57,    58,    59,    60,
      64,    67,    68,    69,    70,    71,    72,    86,   123,   124,
     126,   130,   131,   133,   136,   137,   149,   153,   157,   158,
     161,   178,   238,   329,   330,    19,    21,    66,   202,    31,
     121,   121,   330,   330,    31,    46,    47,    66,    86,   121,
     278,   279,   280,   278,   278,   278,   278,   278,    85,   103,
     104,   105,   106,   107,   108,   109,   110,   111,   112,   113,
     116,   259,    58,   195,    60,    58,    59,   184,    86,    86,
     123,   124,   290,   130,   131,   133,   134,   296,   130,   138,
     294,   136,   138,   287,   121,   158,   287,   287,    86,   132,
     301,   138,   144,   145,   288,    86,   149,   121,   125,   286,
     121,   307,   153,   287,   121,   310,   156,   157,   158,    29,
      74,    29,   222,    29,   224,   224,   238,    73,   222,   222,
      12,     6,   121,    86,   121,   124,   178,    52,     0,   194,
      52,   180,     5,   181,   197,   205,   207,   208,   237,   248,
     249,   250,   251,   252,   325,   181,   206,   197,   248,   249,
     250,   251,   252,    86,   330,   196,   196,   257,   185,   217,
     241,   242,   243,   197,   244,   245,   327,   328,   330,    71,
     282,   328,   244,   328,   217,   241,    54,    20,    86,   190,
      68,    69,   184,    86,    86,    86,   181,   182,   185,   183,
      66,   330,    29,    33,   330,   185,   185,   185,   185,    41,
     279,   275,    86,   184,   184,   184,   184,    86,   103,   104,
     105,   106,   108,   109,   110,   111,   112,   116,   267,   184,
     184,   184,   184,   184,   114,   115,   184,    43,    44,    45,
      86,   260,   180,    61,    63,   320,   321,   322,    86,    86,
     121,   121,   121,   287,   121,   132,   287,   121,   287,   287,
      86,   132,   138,    86,   137,   287,    86,   287,    86,    86,
     288,   121,   182,   287,   121,    86,   161,   330,    86,   138,
     223,   223,   330,    86,    86,   121,   121,   121,   121,    52,
      86,    86,   182,   186,   197,   241,    29,   216,   242,   144,
     146,   326,    73,   183,   330,    29,   326,    29,   224,   184,
      86,   102,   121,   256,   184,   184,   184,    86,   100,   101,
     254,    51,    86,   102,   121,   181,   239,   240,   330,   121,
      48,    49,    50,   281,    86,    86,   121,   184,    86,    91,
     270,   270,   270,   270,   183,   184,   184,   184,   184,   184,
     184,   184,   184,   184,   184,   184,   266,   270,   270,   268,
     270,   268,   270,   184,   184,   270,    86,    86,    62,    64,
     182,   286,   287,   287,   121,   287,   287,    86,   287,   126,
     182,   121,    74,   184,    29,   183,    86,   184,   181,   186,
     186,   244,    86,   185,    86,   185,   330,   330,   244,   185,
     246,   244,   223,    86,   256,   256,   256,   184,    86,    86,
      86,   181,   182,   186,   183,   186,   186,    86,    86,   186,
     186,   269,   270,   266,   266,   266,   266,    86,   269,   270,
     270,   270,   270,   270,   270,   268,   268,   270,   270,    43,
     182,   266,   266,   266,   266,   266,   187,   187,   266,    65,
     322,   287,   121,   121,   182,   161,    86,   330,   330,    86,
     121,    86,    86,    86,    73,   330,    86,   247,   245,   256,
     184,   184,    86,   240,   330,   186,   186,   266,    43,    43,
      43,    43,   184,   267,    43,    43,    43,    43,    43,   103,
     116,   117,   118,   120,   261,   262,    70,   263,    43,    86,
     184,   186,   186,   330,   182,   186,   256,   256,    43,   269,
     185,   185,   185,   185,   185,   188,   182,   185,   188,    86,
      86,   270,   270,   119,   186,   186,    86,   264,   265,    43,
     261,   268,    43,   186,   186,   184,   184,   186,   182,   186,
     189,   270,   270,   264,   268,   186,   186
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

    { result->cur_stmt_type_ = OBPROXY_T_OTHERS; ;}
    break;

  case 50:

    { result->cur_stmt_type_ = OBPROXY_T_CREATE; ;}
    break;

  case 51:

    { result->cur_stmt_type_ = OBPROXY_T_DROP; ;}
    break;

  case 52:

    { result->cur_stmt_type_ = OBPROXY_T_ALTER; ;}
    break;

  case 53:

    { result->cur_stmt_type_ = OBPROXY_T_TRUNCATE; ;}
    break;

  case 54:

    { result->cur_stmt_type_ = OBPROXY_T_RENAME; ;}
    break;

  case 55:

    {;}
    break;

  case 56:

    {;}
    break;

  case 58:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_TABLE; ;}
    break;

  case 59:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 60:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 61:

    {
            SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num));
            result->cur_stmt_type_ = OBPROXY_T_STOP_DDL_TASK;
          ;}
    break;

  case 62:

    {
            SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num));
            result->cur_stmt_type_ = OBPROXY_T_RETRY_DDL_TASK;
          ;}
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

    {;}
    break;

  case 70:

    {;}
    break;

  case 71:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(2) - (2)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 72:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(4) - (4)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 73:

    {
                          ObProxyTextPsParseNode *node = NULL;
                          malloc_parse_node(node);
                          node->str_value_ = (yyvsp[(2) - (2)].str);
                          add_text_ps_node(result->text_ps_parse_info_, node);
                        ;}
    break;

  case 76:

    {
                      result->text_ps_inner_stmt_type_ = OBPROXY_T_TEXT_PS_PREPARE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 77:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (2)].str);
                    ;}
    break;

  case 78:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 79:

    {
            ;}
    break;

  case 80:

    {
            ;}
    break;

  case 81:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 82:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 83:

    { result->cur_stmt_type_ = OBPROXY_T_GRANT; ;}
    break;

  case 84:

    { result->cur_stmt_type_ = OBPROXY_T_REVOKE; ;}
    break;

  case 85:

    { result->cur_stmt_type_ = OBPROXY_T_ANALYZE; ;}
    break;

  case 86:

    { result->cur_stmt_type_ = OBPROXY_T_PURGE; ;}
    break;

  case 87:

    { result->cur_stmt_type_ = OBPROXY_T_FLASHBACK; ;}
    break;

  case 88:

    { result->cur_stmt_type_ = OBPROXY_T_COMMENT; ;}
    break;

  case 89:

    { result->cur_stmt_type_ = OBPROXY_T_AUDIT; ;}
    break;

  case 90:

    { result->cur_stmt_type_ = OBPROXY_T_NOAUDIT; ;}
    break;

  case 93:

    {;}
    break;

  case 94:

    {;}
    break;

  case 95:

    {;}
    break;

  case 101:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_TX_RO; ;}
    break;

  case 105:

    { result->col_name_ = (yyvsp[(3) - (3)].str); ;}
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

    {;}
    break;

  case 112:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY; ;}
    break;

  case 113:

    {;}
    break;

  case 114:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_DATABASE; ;}
    break;

  case 115:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_PROXY_STATUS; ;}
    break;

  case 116:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DATABASES; ;}
    break;

  case 117:

    {;}
    break;

  case 118:

    {;}
    break;

  case 119:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_COLUMNS; ;}
    break;

  case 120:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_INDEX; ;}
    break;

  case 121:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE; ;}
    break;

  case 122:

    {
                      result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
                      result->cur_stmt_type_ = OBPROXY_T_DESC;
                      result->sub_stmt_type_ = OBPROXY_T_SUB_DESC_TABLE;
                  ;}
    break;

  case 123:

    {
            result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
          ;}
    break;

  case 124:

    {
            result->table_info_.table_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.database_name_ = (yyvsp[(4) - (4)].str);
          ;}
    break;

  case 125:

    {
            result->table_info_.database_name_ = (yyvsp[(2) - (4)].str);
            result->table_info_.table_name_ = (yyvsp[(4) - (4)].str);
          ;}
    break;

  case 126:

    {;}
    break;

  case 127:

    { result->table_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 128:

    {;}
    break;

  case 129:

    { result->table_info_.database_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 130:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLES;
                ;}
    break;

  case 131:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_FULL_TABLES;
                ;}
    break;

  case 132:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLE_STATUS;
                      ;}
    break;

  case 133:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DB_VERSION; ;}
    break;

  case 134:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID; ;}
    break;

  case 135:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 136:

    {
                     SET_ICMD_SECOND_STRING((yyvsp[(5) - (5)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 137:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (7)].str));
                     SET_ICMD_SECOND_STRING((yyvsp[(7) - (7)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
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

    {
;}
    break;

  case 307:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 308:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 309:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 310:

    {;}
    break;

  case 311:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 312:

    {;}
    break;

  case 313:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 315:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 316:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 317:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 318:

    {;}
    break;

  case 319:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 320:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 321:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 323:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 324:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 325:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 326:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 327:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 329:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 330:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 331:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 332:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 333:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 334:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 335:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 336:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 337:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 338:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_READ_STALE); ;}
    break;

  case 339:

    {;}
    break;

  case 340:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 341:

    {;}
    break;

  case 342:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 343:

    {;}
    break;

  case 345:

    {;}
    break;

  case 346:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 347:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 348:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 349:

    {;}
    break;

  case 350:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 351:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 352:

    {;}
    break;

  case 353:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 354:

    {;}
    break;

  case 355:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 356:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 357:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 359:

    {;}
    break;

  case 360:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 361:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 362:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 363:

    {;}
    break;

  case 364:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 366:

    {;}
    break;

  case 367:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 368:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 369:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 370:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 371:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 372:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 373:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 374:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 375:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 376:

    {;}
    break;

  case 377:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 378:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 379:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 380:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 381:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 382:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 383:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 386:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 387:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 388:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 389:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 396:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 397:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 399:

    {;}
    break;

  case 400:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 401:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 402:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 403:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 404:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT();
                                                ;}
    break;

  case 405:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 406:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 407:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 408:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 409:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 410:

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

