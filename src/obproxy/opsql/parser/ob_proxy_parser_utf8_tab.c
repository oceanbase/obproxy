
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
     GRANT = 277,
     REVOKE = 278,
     ANALYZE = 279,
     PURGE = 280,
     COMMENT = 281,
     FROM = 282,
     DUAL = 283,
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
     HINT_END = 294,
     COMMENT_BEGIN = 295,
     COMMENT_END = 296,
     ROUTE_TABLE = 297,
     ROUTE_PART_KEY = 298,
     QUERY_TIMEOUT = 299,
     READ_CONSISTENCY = 300,
     WEAK = 301,
     STRONG = 302,
     FROZEN = 303,
     PLACE_HOLDER = 304,
     END_P = 305,
     ERROR = 306,
     WHEN = 307,
     FLASHBACK = 308,
     AUDIT = 309,
     NOAUDIT = 310,
     BEGI = 311,
     START = 312,
     TRANSACTION = 313,
     READ = 314,
     ONLY = 315,
     WITH = 316,
     CONSISTENT = 317,
     SNAPSHOT = 318,
     INDEX = 319,
     XA = 320,
     WARNINGS = 321,
     ERRORS = 322,
     TRACE = 323,
     QUICK = 324,
     COUNT = 325,
     AS = 326,
     WHERE = 327,
     VALUES = 328,
     ORDER = 329,
     GROUP = 330,
     HAVING = 331,
     INTO = 332,
     UNION = 333,
     FOR = 334,
     TX_READ_ONLY = 335,
     SELECT_OBPROXY_ROUTE_ADDR = 336,
     SET_OBPROXY_ROUTE_ADDR = 337,
     NAME_OB_DOT = 338,
     NAME_OB = 339,
     EXPLAIN = 340,
     DESC = 341,
     DESCRIBE = 342,
     NAME_STR = 343,
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
     DBP_COMMENT = 365,
     ROUTE_TAG = 366,
     SYS_TAG = 367,
     TABLE_NAME = 368,
     SCAN_ALL = 369,
     STICKY_SESSION = 370,
     PARALL = 371,
     SHARD_KEY = 372,
     INT_NUM = 373,
     SHOW_PROXYNET = 374,
     THREAD = 375,
     CONNECTION = 376,
     LIMIT = 377,
     OFFSET = 378,
     SHOW_PROCESSLIST = 379,
     SHOW_PROXYSESSION = 380,
     SHOW_GLOBALSESSION = 381,
     ATTRIBUTE = 382,
     VARIABLES = 383,
     ALL = 384,
     STAT = 385,
     SHOW_PROXYCONFIG = 386,
     DIFF = 387,
     USER = 388,
     LIKE = 389,
     SHOW_PROXYSM = 390,
     SHOW_PROXYCLUSTER = 391,
     SHOW_PROXYRESOURCE = 392,
     SHOW_PROXYCONGESTION = 393,
     SHOW_PROXYROUTE = 394,
     PARTITION = 395,
     ROUTINE = 396,
     SHOW_PROXYVIP = 397,
     SHOW_PROXYMEMORY = 398,
     OBJPOOL = 399,
     SHOW_SQLAUDIT = 400,
     SHOW_WARNLOG = 401,
     SHOW_PROXYSTAT = 402,
     REFRESH = 403,
     SHOW_PROXYTRACE = 404,
     SHOW_PROXYINFO = 405,
     BINARY = 406,
     UPGRADE = 407,
     IDC = 408,
     SHOW_ELASTIC_ID = 409,
     SHOW_TOPOLOGY = 410,
     GROUP_NAME = 411,
     SHOW_DB_VERSION = 412,
     SHOW_DATABASES = 413,
     SHOW_TABLES = 414,
     SHOW_FULL_TABLES = 415,
     SELECT_DATABASE = 416,
     SHOW_CREATE_TABLE = 417,
     SELECT_PROXY_VERSION = 418,
     SHOW_COLUMNS = 419,
     SHOW_INDEX = 420,
     ALTER_PROXYCONFIG = 421,
     ALTER_PROXYRESOURCE = 422,
     PING_PROXY = 423,
     KILL_PROXYSESSION = 424,
     KILL_GLOBALSESSION = 425,
     KILL = 426,
     QUERY = 427,
     SHOW_MASTER_STATUS = 428,
     SHOW_BINARY_LOGS = 429,
     SHOW_BINLOG_EVENTS = 430,
     PURGE_BINARY_LOGS = 431,
     RESET_MASTER = 432,
     SHOW_BINLOG_SERVER_FOR_TENANT = 433
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
#define YYFINAL  314
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   1384

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  190
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  138
/* YYNRULES -- Number of rules.  */
#define YYNRULES  435
/* YYNRULES -- Number of states.  */
#define YYNSTATES  715

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   433

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,   188,     2,     2,     2,     2,
     183,   184,   189,     2,   181,     2,   185,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   179,
       2,   182,     2,     2,   180,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   186,     2,   187,     2,     2,     2,     2,
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
     175,   176,   177,   178
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
     117,   119,   121,   123,   124,   126,   128,   131,   133,   135,
     137,   139,   141,   143,   145,   147,   150,   155,   158,   160,
     162,   166,   169,   173,   176,   179,   183,   187,   189,   191,
     193,   195,   197,   199,   201,   203,   205,   208,   210,   212,
     214,   215,   218,   219,   221,   224,   230,   234,   236,   240,
     242,   244,   246,   248,   250,   252,   254,   256,   258,   260,
     262,   264,   266,   268,   271,   274,   278,   284,   288,   294,
     295,   298,   299,   302,   306,   310,   316,   318,   320,   324,
     330,   338,   340,   344,   346,   350,   352,   354,   356,   358,
     360,   362,   368,   370,   374,   380,   381,   383,   387,   389,
     391,   393,   396,   400,   402,   404,   407,   409,   412,   416,
     420,   422,   424,   426,   427,   431,   433,   437,   441,   447,
     450,   453,   458,   461,   464,   468,   470,   475,   482,   487,
     493,   500,   505,   509,   511,   513,   515,   517,   520,   524,
     530,   537,   544,   551,   558,   565,   573,   580,   587,   594,
     601,   610,   619,   626,   627,   630,   633,   636,   638,   642,
     644,   649,   654,   658,   665,   669,   674,   679,   686,   690,
     692,   696,   697,   701,   705,   709,   713,   717,   721,   725,
     729,   733,   737,   741,   747,   751,   752,   754,   755,   757,
     759,   761,   763,   766,   768,   771,   773,   776,   779,   783,
     784,   786,   789,   791,   794,   796,   799,   802,   803,   806,
     811,   813,   818,   824,   826,   831,   836,   842,   843,   845,
     847,   849,   850,   852,   856,   860,   863,   865,   867,   869,
     871,   873,   875,   877,   879,   881,   883,   885,   887,   889,
     891,   893,   895,   897,   899,   901,   903,   905,   907,   909,
     911,   913,   915,   917,   919,   921,   922,   925,   930,   935,
     936,   939,   940,   943,   946,   948,   950,   954,   957,   961,
     966,   968,   971,   972,   975,   979,   982,   985,   988,   989,
     992,   996,   999,  1003,  1006,  1010,  1014,  1019,  1021,  1024,
    1027,  1031,  1034,  1037,  1038,  1040,  1042,  1045,  1048,  1052,
    1055,  1057,  1060,  1062,  1065,  1068,  1071,  1074,  1075,  1077,
    1081,  1087,  1090,  1094,  1097,  1098,  1100,  1103,  1106,  1109,
    1112,  1117,  1123,  1129,  1133,  1135,  1138,  1142,  1146,  1149,
    1152,  1156,  1160,  1161,  1164,  1166,  1170,  1174,  1178,  1179,
    1181,  1183,  1187,  1190,  1194,  1197,  1200,  1202,  1203,  1206,
    1211,  1214,  1216,  1220,  1223,  1228,  1232,  1238,  1240,  1242,
    1244,  1246,  1248,  1250,  1252,  1254,  1256,  1258,  1260,  1262,
    1264,  1266,  1268,  1270,  1272,  1274,  1276,  1278,  1280,  1282,
    1284,  1286,  1288,  1290,  1292,  1294
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     191,     0,    -1,   192,    -1,     1,    -1,   193,    -1,   192,
     193,    -1,   194,    50,    -1,   194,   179,    -1,   194,   179,
      50,    -1,   179,    -1,   179,    50,    -1,    56,   194,   179,
      -1,   195,    -1,   254,   195,    -1,   196,    -1,   245,    -1,
     250,    -1,   246,    -1,   247,    -1,   248,    -1,   197,    -1,
     316,    -1,   280,    -1,   215,    -1,   281,    -1,   320,    -1,
     321,    -1,   228,    -1,   229,    -1,   230,    -1,   231,    -1,
     232,    -1,   233,    -1,   234,    -1,   198,    -1,   207,    -1,
     249,    -1,   282,    -1,   322,    -1,   268,   212,   211,    -1,
     209,   196,    -1,   209,   245,    -1,   209,   247,    -1,   209,
     248,    -1,   209,   246,    -1,   209,   249,    -1,   199,    -1,
     208,    -1,    14,   200,    -1,    15,    -1,    16,    -1,    17,
      -1,    18,    -1,    -1,    19,    -1,    64,    -1,    21,    64,
      -1,   196,    -1,   245,    -1,   246,    -1,   248,    -1,   247,
      -1,   322,    -1,   234,    -1,   249,    -1,   180,    84,    -1,
     202,   181,   180,    84,    -1,   180,    84,    -1,   203,    -1,
     201,    -1,    29,   327,    27,    -1,    30,   327,    -1,    30,
     327,    31,    -1,   205,   204,    -1,   206,   202,    -1,    15,
      29,   327,    -1,    32,    29,   327,    -1,    22,    -1,    23,
      -1,    24,    -1,    25,    -1,    53,    -1,    26,    -1,    54,
      -1,    55,    -1,   210,    -1,   210,    84,    -1,    85,    -1,
      86,    -1,    87,    -1,    -1,    27,   241,    -1,    -1,   238,
      -1,     5,    80,    -1,     5,    80,   212,    27,   241,    -1,
       5,    80,   238,    -1,   163,    -1,   163,    71,   327,    -1,
     213,    -1,   214,    -1,   226,    -1,   227,    -1,   216,    -1,
     224,    -1,   225,    -1,   223,    -1,   161,    -1,   158,    -1,
     221,    -1,   222,    -1,   217,    -1,   218,    -1,   162,   235,
      -1,   210,   327,    -1,   164,    27,    84,    -1,   164,    27,
      84,    27,    84,    -1,   165,    27,    84,    -1,   165,    27,
      84,    27,    84,    -1,    -1,   134,    84,    -1,    -1,    27,
      84,    -1,   159,   220,   219,    -1,   160,   220,   219,    -1,
      11,    19,    20,   220,   219,    -1,   157,    -1,   154,    -1,
     154,    27,    84,    -1,   154,    72,   156,   182,    84,    -1,
     154,    27,    84,    72,   156,   182,    84,    -1,   155,    -1,
     155,    27,    84,    -1,    81,    -1,    82,   182,   118,    -1,
      91,    -1,    92,    -1,    93,    -1,    94,    -1,    95,    -1,
      96,    -1,    13,   235,   183,   236,   184,    -1,   327,    -1,
     327,   185,   327,    -1,   327,   185,   327,   185,   327,    -1,
      -1,   237,    -1,   236,   181,   237,    -1,    84,    -1,   118,
      -1,    99,    -1,   180,    84,    -1,   180,   180,    84,    -1,
      49,    -1,   239,    -1,   238,   239,    -1,   240,    -1,   183,
     184,    -1,   183,   196,   184,    -1,   183,   238,   184,    -1,
     324,    -1,   242,    -1,   196,    -1,    -1,   183,   244,   184,
      -1,    84,    -1,   244,   181,    84,    -1,   271,   325,   323,
      -1,   271,   325,   323,   243,   242,    -1,   273,   241,    -1,
     269,   241,    -1,   270,   279,    27,   241,    -1,   274,   325,
      -1,    12,   251,    -1,   252,   181,   251,    -1,   252,    -1,
     180,    84,   182,   253,    -1,   180,   180,    97,    84,   182,
     253,    -1,    97,    84,   182,   253,    -1,   180,   180,    84,
     182,   253,    -1,   180,   180,    98,    84,   182,   253,    -1,
      98,    84,   182,   253,    -1,    84,   182,   253,    -1,    84,
      -1,   118,    -1,    99,    -1,   255,    -1,   255,   254,    -1,
      40,   256,    41,    -1,    40,   104,   264,   263,    41,    -1,
      40,   101,   182,   267,   263,    41,    -1,    40,   113,   182,
     267,   263,    41,    -1,    40,   100,   182,   267,   263,    41,
      -1,    40,   102,   182,   267,   263,    41,    -1,    40,   103,
     182,   267,   263,    41,    -1,    40,    83,    84,   182,   266,
     263,    41,    -1,    40,   107,   182,   265,   263,    41,    -1,
      40,   108,   182,   265,   263,    41,    -1,    40,   105,   182,
     267,   263,    41,    -1,    40,   106,   182,   267,   263,    41,
      -1,    40,   110,   111,   182,   186,   258,   187,    41,    -1,
      40,   110,   112,   182,   186,   260,   187,    41,    -1,    40,
     109,   182,   267,   263,    41,    -1,    -1,   256,   257,    -1,
      42,    84,    -1,    43,    84,    -1,    84,    -1,   259,   181,
     258,    -1,   259,    -1,   100,   183,   267,   184,    -1,   113,
     183,   267,   184,    -1,   114,   183,   184,    -1,   114,   183,
     116,   182,   267,   184,    -1,   115,   183,   184,    -1,   117,
     183,   261,   184,    -1,    68,   183,   265,   184,    -1,    68,
     183,   265,   188,   265,   184,    -1,   262,   181,   261,    -1,
     262,    -1,    84,   182,   267,    -1,    -1,   263,   181,   264,
      -1,   100,   182,   267,    -1,   101,   182,   267,    -1,   113,
     182,   267,    -1,   102,   182,   267,    -1,   103,   182,   267,
      -1,   107,   182,   265,    -1,   108,   182,   265,    -1,   105,
     182,   267,    -1,   106,   182,   267,    -1,   109,   182,   267,
      -1,    84,   185,    84,   182,   266,    -1,    84,   182,   266,
      -1,    -1,   267,    -1,    -1,   267,    -1,    84,    -1,    88,
      -1,     5,    -1,    33,   275,    -1,     8,    -1,    34,   275,
      -1,     6,    -1,    35,   275,    -1,     7,   272,    -1,    36,
     275,   272,    -1,    -1,   129,    -1,   129,    52,    -1,     9,
      -1,    37,   275,    -1,    10,    -1,    38,   275,    -1,   276,
      39,    -1,    -1,   277,   276,    -1,    44,   183,   118,   184,
      -1,   118,    -1,    45,   183,   278,   184,    -1,    64,   183,
      84,    84,   184,    -1,    84,    -1,    84,   183,   118,   184,
      -1,    84,   183,    84,   184,    -1,    84,   183,    84,    84,
     184,    -1,    -1,    46,    -1,    47,    -1,    48,    -1,    -1,
      69,    -1,    11,   315,    66,    -1,    11,   315,    67,    -1,
      11,    68,    -1,   286,    -1,   288,    -1,   289,    -1,   292,
      -1,   290,    -1,   294,    -1,   295,    -1,   296,    -1,   297,
      -1,   299,    -1,   300,    -1,   301,    -1,   302,    -1,   303,
      -1,   305,    -1,   306,    -1,   308,    -1,   309,    -1,   310,
      -1,   311,    -1,   312,    -1,   313,    -1,   314,    -1,   173,
      -1,   174,    -1,   175,    -1,   176,    -1,   177,    -1,   178,
      -1,    -1,   122,   118,    -1,   122,   118,   181,   118,    -1,
     122,   118,   123,   118,    -1,    -1,   134,    84,    -1,    -1,
     134,    84,    -1,   119,   287,    -1,   120,    -1,   121,    -1,
     121,   118,   283,    -1,   131,   284,    -1,   131,   132,   284,
      -1,   131,   132,   133,   284,    -1,   124,    -1,   126,   291,
      -1,    -1,   127,    84,    -1,   127,   134,    84,    -1,   127,
     129,    -1,   134,    84,    -1,   125,   293,    -1,    -1,   127,
     284,    -1,   127,   118,   284,    -1,   130,   284,    -1,   130,
     118,   284,    -1,   128,   284,    -1,   128,   118,   284,    -1,
     128,   129,   284,    -1,   128,   129,   118,   284,    -1,   135,
      -1,   135,   118,    -1,   136,   284,    -1,   136,   153,   284,
      -1,   137,   284,    -1,   138,   298,    -1,    -1,    84,    -1,
     129,    -1,   129,    84,    -1,   139,   285,    -1,   139,   141,
     285,    -1,   139,   140,    -1,   142,    -1,   142,    84,    -1,
     143,    -1,   143,   144,    -1,   145,   283,    -1,   145,   118,
      -1,   146,   304,    -1,    -1,   118,    -1,   118,   181,   118,
      -1,   118,   181,   118,   181,    84,    -1,   147,   284,    -1,
     147,   148,   284,    -1,   149,   307,    -1,    -1,   118,    -1,
     118,   118,    -1,   150,   151,    -1,   150,   152,    -1,   150,
     153,    -1,   166,    12,    84,   182,    -1,   166,    12,    84,
     182,    84,    -1,   166,    12,    84,   182,   118,    -1,   167,
       6,    84,    -1,   168,    -1,   169,   118,    -1,   169,   118,
     118,    -1,   170,    84,   118,    -1,   170,    84,    -1,   171,
     118,    -1,   171,   121,   118,    -1,   171,   172,   118,    -1,
      -1,    70,   189,    -1,    56,    -1,    57,    58,   317,    -1,
      65,    56,    84,    -1,    65,    57,    84,    -1,    -1,   318,
      -1,   319,    -1,   318,   181,   319,    -1,    59,    60,    -1,
      61,    62,    63,    -1,    89,    84,    -1,    90,    84,    -1,
      84,    -1,    -1,   140,    84,    -1,   140,   183,    84,   184,
      -1,   325,   323,    -1,   327,    -1,   327,   185,   327,    -1,
     327,   327,    -1,   327,   185,   327,   327,    -1,   327,    71,
     327,    -1,   327,   185,   327,    71,   327,    -1,    57,    -1,
      65,    -1,    56,    -1,    58,    -1,    62,    -1,    67,    -1,
      66,    -1,    70,    -1,    69,    -1,    68,    -1,   120,    -1,
     121,    -1,   123,    -1,   127,    -1,   128,    -1,   130,    -1,
     132,    -1,   133,    -1,   144,    -1,   148,    -1,   152,    -1,
     153,    -1,   172,    -1,   156,    -1,    53,    -1,    54,    -1,
      55,    -1,    84,    -1,   326,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   334,   334,   335,   337,   338,   340,   341,   342,   343,
     344,   345,   347,   348,   350,   351,   352,   353,   354,   355,
     356,   357,   358,   359,   360,   361,   362,   363,   364,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   376,
     378,   379,   380,   381,   382,   383,   385,   386,   388,   389,
     390,   391,   392,   394,   395,   396,   397,   399,   400,   401,
     402,   403,   404,   405,   406,   408,   415,   423,   431,   432,
     435,   441,   446,   452,   455,   458,   463,   469,   470,   471,
     472,   473,   474,   475,   476,   478,   479,   481,   482,   483,
     485,   486,   488,   489,   491,   492,   493,   495,   496,   498,
     499,   500,   501,   502,   504,   505,   506,   507,   508,   509,
     510,   511,   512,   513,   514,   521,   526,   533,   538,   545,
     546,   548,   549,   551,   555,   560,   565,   567,   568,   573,
     578,   585,   586,   592,   595,   601,   602,   603,   604,   605,
     606,   609,   611,   615,   620,   628,   631,   636,   641,   646,
     651,   656,   661,   666,   674,   675,   677,   679,   680,   681,
     683,   684,   686,   688,   689,   691,   692,   694,   698,   699,
     700,   701,   702,   707,   709,   710,   712,   716,   720,   724,
     728,   732,   736,   740,   745,   750,   756,   757,   759,   760,
     761,   762,   763,   764,   765,   766,   772,   773,   774,   775,
     776,   777,   778,   779,   780,   782,   783,   784,   786,   787,
     789,   794,   799,   800,   801,   802,   804,   805,   807,   808,
     810,   818,   819,   821,   822,   823,   824,   825,   826,   827,
     828,   829,   830,   831,   837,   839,   840,   842,   843,   845,
     846,   848,   849,   850,   851,   852,   853,   854,   855,   857,
     858,   859,   861,   862,   863,   864,   865,   866,   867,   869,
     870,   871,   872,   877,   878,   879,   880,   882,   883,   884,
     885,   887,   888,   891,   892,   893,   896,   897,   898,   899,
     900,   901,   902,   903,   904,   905,   906,   907,   908,   909,
     910,   911,   912,   913,   914,   915,   916,   917,   918,   920,
     921,   922,   923,   924,   925,   930,   932,   936,   941,   949,
     950,   954,   955,   958,   960,   961,   962,   966,   967,   968,
     972,   974,   976,   977,   978,   979,   980,   983,   985,   986,
     987,   988,   989,   990,   991,   992,   993,   997,   998,  1002,
    1003,  1008,  1011,  1013,  1014,  1015,  1016,  1020,  1021,  1022,
    1026,  1027,  1031,  1032,  1036,  1037,  1040,  1042,  1043,  1044,
    1045,  1049,  1050,  1053,  1055,  1056,  1057,  1061,  1062,  1063,
    1067,  1068,  1069,  1073,  1077,  1081,  1082,  1086,  1087,  1091,
    1092,  1093,  1096,  1097,  1100,  1104,  1105,  1106,  1108,  1109,
    1111,  1112,  1115,  1116,  1119,  1125,  1128,  1130,  1131,  1132,
    1134,  1139,  1142,  1146,  1150,  1155,  1159,  1165,  1166,  1167,
    1168,  1169,  1170,  1171,  1172,  1173,  1174,  1175,  1176,  1177,
    1178,  1179,  1180,  1181,  1182,  1183,  1184,  1185,  1186,  1187,
    1188,  1189,  1190,  1191,  1193,  1194
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
  "RENAME", "TABLE", "STATUS", "UNIQUE", "GRANT", "REVOKE", "ANALYZE",
  "PURGE", "COMMENT", "FROM", "DUAL", "PREPARE", "EXECUTE", "USING",
  "DEALLOCATE", "SELECT_HINT_BEGIN", "UPDATE_HINT_BEGIN",
  "DELETE_HINT_BEGIN", "INSERT_HINT_BEGIN", "REPLACE_HINT_BEGIN",
  "MERGE_HINT_BEGIN", "HINT_END", "COMMENT_BEGIN", "COMMENT_END",
  "ROUTE_TABLE", "ROUTE_PART_KEY", "QUERY_TIMEOUT", "READ_CONSISTENCY",
  "WEAK", "STRONG", "FROZEN", "PLACE_HOLDER", "END_P", "ERROR", "WHEN",
  "FLASHBACK", "AUDIT", "NOAUDIT", "BEGI", "START", "TRANSACTION", "READ",
  "ONLY", "WITH", "CONSISTENT", "SNAPSHOT", "INDEX", "XA", "WARNINGS",
  "ERRORS", "TRACE", "QUICK", "COUNT", "AS", "WHERE", "VALUES", "ORDER",
  "GROUP", "HAVING", "INTO", "UNION", "FOR", "TX_READ_ONLY",
  "SELECT_OBPROXY_ROUTE_ADDR", "SET_OBPROXY_ROUTE_ADDR", "NAME_OB_DOT",
  "NAME_OB", "EXPLAIN", "DESC", "DESCRIBE", "NAME_STR", "USE", "HELP",
  "SET_NAMES", "SET_CHARSET", "SET_PASSWORD", "SET_DEFAULT",
  "SET_OB_READ_CONSISTENCY", "SET_TX_READ_ONLY", "GLOBAL", "SESSION",
  "NUMBER_VAL", "GROUP_ID", "TABLE_ID", "ELASTIC_ID", "TESTLOAD",
  "ODP_COMMENT", "TNT_ID", "DISASTER_STATUS", "TRACE_ID", "RPC_ID",
  "TARGET_DB_SERVER", "DBP_COMMENT", "ROUTE_TAG", "SYS_TAG", "TABLE_NAME",
  "SCAN_ALL", "STICKY_SESSION", "PARALL", "SHARD_KEY", "INT_NUM",
  "SHOW_PROXYNET", "THREAD", "CONNECTION", "LIMIT", "OFFSET",
  "SHOW_PROCESSLIST", "SHOW_PROXYSESSION", "SHOW_GLOBALSESSION",
  "ATTRIBUTE", "VARIABLES", "ALL", "STAT", "SHOW_PROXYCONFIG", "DIFF",
  "USER", "LIKE", "SHOW_PROXYSM", "SHOW_PROXYCLUSTER",
  "SHOW_PROXYRESOURCE", "SHOW_PROXYCONGESTION", "SHOW_PROXYROUTE",
  "PARTITION", "ROUTINE", "SHOW_PROXYVIP", "SHOW_PROXYMEMORY", "OBJPOOL",
  "SHOW_SQLAUDIT", "SHOW_WARNLOG", "SHOW_PROXYSTAT", "REFRESH",
  "SHOW_PROXYTRACE", "SHOW_PROXYINFO", "BINARY", "UPGRADE", "IDC",
  "SHOW_ELASTIC_ID", "SHOW_TOPOLOGY", "GROUP_NAME", "SHOW_DB_VERSION",
  "SHOW_DATABASES", "SHOW_TABLES", "SHOW_FULL_TABLES", "SELECT_DATABASE",
  "SHOW_CREATE_TABLE", "SELECT_PROXY_VERSION", "SHOW_COLUMNS",
  "SHOW_INDEX", "ALTER_PROXYCONFIG", "ALTER_PROXYRESOURCE", "PING_PROXY",
  "KILL_PROXYSESSION", "KILL_GLOBALSESSION", "KILL", "QUERY",
  "SHOW_MASTER_STATUS", "SHOW_BINARY_LOGS", "SHOW_BINLOG_EVENTS",
  "PURGE_BINARY_LOGS", "RESET_MASTER", "SHOW_BINLOG_SERVER_FOR_TENANT",
  "';'", "'@'", "','", "'='", "'('", "')'", "'.'", "'{'", "'}'", "'#'",
  "'*'", "$accept", "root", "sql_stmts", "sql_stmt", "comment_stmt",
  "stmt", "select_stmt", "explain_stmt", "ddl_stmt", "mysql_ddl_stmt",
  "create_dll_expr", "text_ps_from_stmt", "text_ps_execute_using_var_list",
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
     425,   426,   427,   428,   429,   430,   431,   432,   433,    59,
      64,    44,    61,    40,    41,    46,   123,   125,    35,    42
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   190,   191,   191,   192,   192,   193,   193,   193,   193,
     193,   193,   194,   194,   195,   195,   195,   195,   195,   195,
     195,   195,   195,   195,   195,   195,   195,   195,   195,   195,
     195,   195,   195,   195,   195,   195,   195,   195,   195,   196,
     197,   197,   197,   197,   197,   197,   198,   198,   199,   199,
     199,   199,   199,   200,   200,   200,   200,   201,   201,   201,
     201,   201,   201,   201,   201,   202,   202,   203,   204,   204,
     205,   206,   206,   207,   207,   207,   207,   208,   208,   208,
     208,   208,   208,   208,   208,   209,   209,   210,   210,   210,
     211,   211,   212,   212,   213,   213,   213,   214,   214,   215,
     215,   215,   215,   215,   216,   216,   216,   216,   216,   216,
     216,   216,   216,   216,   216,   217,   217,   218,   218,   219,
     219,   220,   220,   221,   221,   222,   223,   224,   224,   224,
     224,   225,   225,   226,   227,   228,   229,   230,   231,   232,
     233,   234,   235,   235,   235,   236,   236,   236,   237,   237,
     237,   237,   237,   237,   238,   238,   239,   240,   240,   240,
     241,   241,   242,   243,   243,   244,   244,   245,   245,   246,
     247,   248,   249,   250,   251,   251,   252,   252,   252,   252,
     252,   252,   252,   253,   253,   253,   254,   254,   255,   255,
     255,   255,   255,   255,   255,   255,   255,   255,   255,   255,
     255,   255,   255,   256,   256,   257,   257,   257,   258,   258,
     259,   259,   259,   259,   259,   259,   260,   260,   261,   261,
     262,   263,   263,   264,   264,   264,   264,   264,   264,   264,
     264,   264,   264,   264,   264,   265,   265,   266,   266,   267,
     267,   268,   268,   269,   269,   270,   270,   271,   271,   272,
     272,   272,   273,   273,   274,   274,   275,   276,   276,   277,
     277,   277,   277,   277,   277,   277,   277,   278,   278,   278,
     278,   279,   279,   280,   280,   280,   281,   281,   281,   281,
     281,   281,   281,   281,   281,   281,   281,   281,   281,   281,
     281,   281,   281,   281,   281,   281,   281,   281,   281,   282,
     282,   282,   282,   282,   282,   283,   283,   283,   283,   284,
     284,   285,   285,   286,   287,   287,   287,   288,   288,   288,
     289,   290,   291,   291,   291,   291,   291,   292,   293,   293,
     293,   293,   293,   293,   293,   293,   293,   294,   294,   295,
     295,   296,   297,   298,   298,   298,   298,   299,   299,   299,
     300,   300,   301,   301,   302,   302,   303,   304,   304,   304,
     304,   305,   305,   306,   307,   307,   307,   308,   308,   308,
     309,   309,   309,   310,   311,   312,   312,   313,   313,   314,
     314,   314,   315,   315,   316,   316,   316,   316,   317,   317,
     318,   318,   319,   319,   320,   321,   322,   323,   323,   323,
     324,   325,   325,   325,   325,   325,   325,   326,   326,   326,
     326,   326,   326,   326,   326,   326,   326,   326,   326,   326,
     326,   326,   326,   326,   326,   326,   326,   326,   326,   326,
     326,   326,   326,   326,   327,   327
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     1,     1,     1,     2,     2,     2,     3,     1,
       2,     3,     1,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     3,
       2,     2,     2,     2,     2,     2,     1,     1,     2,     1,
       1,     1,     1,     0,     1,     1,     2,     1,     1,     1,
       1,     1,     1,     1,     1,     2,     4,     2,     1,     1,
       3,     2,     3,     2,     2,     3,     3,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     2,     1,     1,     1,
       0,     2,     0,     1,     2,     5,     3,     1,     3,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     2,     2,     3,     5,     3,     5,     0,
       2,     0,     2,     3,     3,     5,     1,     1,     3,     5,
       7,     1,     3,     1,     3,     1,     1,     1,     1,     1,
       1,     5,     1,     3,     5,     0,     1,     3,     1,     1,
       1,     2,     3,     1,     1,     2,     1,     2,     3,     3,
       1,     1,     1,     0,     3,     1,     3,     3,     5,     2,
       2,     4,     2,     2,     3,     1,     4,     6,     4,     5,
       6,     4,     3,     1,     1,     1,     1,     2,     3,     5,
       6,     6,     6,     6,     6,     7,     6,     6,     6,     6,
       8,     8,     6,     0,     2,     2,     2,     1,     3,     1,
       4,     4,     3,     6,     3,     4,     4,     6,     3,     1,
       3,     0,     3,     3,     3,     3,     3,     3,     3,     3,
       3,     3,     3,     5,     3,     0,     1,     0,     1,     1,
       1,     1,     2,     1,     2,     1,     2,     2,     3,     0,
       1,     2,     1,     2,     1,     2,     2,     0,     2,     4,
       1,     4,     5,     1,     4,     4,     5,     0,     1,     1,
       1,     0,     1,     3,     3,     2,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     0,     2,     4,     4,     0,
       2,     0,     2,     2,     1,     1,     3,     2,     3,     4,
       1,     2,     0,     2,     3,     2,     2,     2,     0,     2,
       3,     2,     3,     2,     3,     3,     4,     1,     2,     2,
       3,     2,     2,     0,     1,     1,     2,     2,     3,     2,
       1,     2,     1,     2,     2,     2,     2,     0,     1,     3,
       5,     2,     3,     2,     0,     1,     2,     2,     2,     2,
       4,     5,     5,     3,     1,     2,     3,     3,     2,     2,
       3,     3,     0,     2,     1,     3,     3,     3,     0,     1,
       1,     3,     2,     3,     2,     2,     1,     0,     2,     4,
       2,     1,     3,     2,     4,     3,     5,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
       0,     3,   241,   245,   249,   243,   252,   254,   382,     0,
       0,    53,    49,    50,    51,    52,    77,    78,    79,    80,
      82,     0,     0,     0,   257,   257,   257,   257,   257,   257,
     203,    81,    83,    84,   384,     0,     0,   133,     0,   396,
      87,    88,    89,     0,     0,   135,   136,   137,   138,   139,
     140,     0,   320,   328,   322,   309,   337,   309,   309,   343,
     311,   350,   352,   305,   357,   309,   364,     0,   127,   131,
     126,   108,   121,   121,   107,     0,    97,     0,     0,     0,
       0,   374,     0,     0,     0,   299,   300,   301,   302,   303,
     304,     9,     0,     2,     4,     0,    12,    14,    20,    34,
      46,     0,     0,    35,    47,     0,    85,    99,   100,    23,
     103,   111,   112,   109,   110,   106,   104,   105,   101,   102,
      27,    28,    29,    30,    31,    32,    33,    15,    17,    18,
      19,    36,    16,     0,   186,    92,     0,   271,     0,     0,
       0,    22,    24,    37,   276,   277,   278,   280,   279,   281,
     282,   283,   284,   285,   286,   287,   288,   289,   290,   291,
     292,   293,   294,   295,   296,   297,   298,    21,    25,    26,
      38,    94,   250,   247,     0,   275,     0,     0,     0,     0,
       0,     0,   173,   175,   431,   432,   433,   409,   407,   410,
     411,   408,   413,   412,   416,   415,   414,   434,   417,   418,
     419,   420,   421,   422,   423,   424,   425,   426,   427,   428,
     430,   429,     0,   435,   142,    54,     0,    55,    48,     0,
       0,    71,     0,     0,     0,     0,   263,   260,   242,     0,
     257,   244,   246,   249,   253,   255,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     384,     0,   388,     0,     0,     0,   394,   395,   314,   315,
     313,   309,   309,   309,   327,     0,     0,   321,   309,     0,
     317,   338,   309,   339,   341,   344,   345,   342,     0,   349,
     311,   347,   351,   353,   355,     0,   354,   358,   356,   309,
     361,   365,   363,   367,   368,   369,     0,     0,     0,     0,
     119,   119,   113,     0,     0,     0,     0,     0,   375,   378,
     379,     0,     0,    10,     1,     5,     6,     7,   241,     0,
      57,    69,    68,    73,    63,    58,    59,    61,    60,    64,
      62,     0,    74,    40,    41,    44,    42,    43,    45,    86,
     114,    13,   187,     0,    90,    93,   154,   156,   162,   170,
     161,   160,   397,   401,   272,     0,   397,   169,   172,     0,
      96,   251,   121,   383,   273,   274,     0,     0,     0,     0,
       0,     0,   145,     0,    56,    75,    70,    72,    76,     0,
     267,     0,     0,   256,   258,   248,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   221,     0,     0,   235,   235,     0,     0,     0,
       0,   188,     0,     0,   207,   204,    11,     0,     0,   385,
     389,   390,   386,   387,   134,   305,   309,   329,   309,   309,
     333,   309,   331,   323,   325,     0,   326,   309,   318,   310,
     340,   346,   312,   348,   306,     0,   362,   366,   128,     0,
     132,   122,     0,   123,   124,    98,   115,   117,     0,   373,
     376,   377,   380,   381,     8,    67,    65,     0,   157,     0,
       0,     0,    39,   155,     0,   400,     0,     0,   403,     0,
     163,     0,   119,   183,   185,   184,   182,     0,     0,     0,
       0,     0,     0,   174,   153,   148,   150,   149,     0,     0,
     146,   143,     0,   268,   269,   270,     0,     0,     0,     0,
     237,   239,   240,   221,   221,   221,   221,   237,     0,     0,
       0,     0,     0,     0,     0,   235,   235,     0,     0,     0,
     221,   221,   221,   236,   221,   221,     0,     0,   221,   205,
     206,   392,     0,     0,   316,   330,   334,   309,   335,   332,
     324,   319,     0,     0,   359,     0,     0,   120,     0,     0,
     370,     0,   158,   159,    91,   398,     0,   405,   402,   171,
       0,     0,    95,   125,   178,   181,   176,     0,     0,     0,
     151,     0,     0,   141,     0,   259,   261,     0,     0,   265,
     264,   221,   238,     0,     0,     0,     0,   234,     0,   223,
     224,   226,   227,   230,   231,   228,   229,   232,   225,   189,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   393,
     391,   336,   308,   307,     0,     0,   129,   116,   118,   371,
     372,    66,     0,     0,   404,   165,     0,   168,   179,     0,
       0,   152,   147,   144,   262,   266,     0,   192,   190,   193,
     194,   237,   222,   198,   199,   196,   197,   202,     0,     0,
       0,     0,     0,     0,   209,     0,     0,   191,   360,     0,
     399,   406,     0,   164,   177,   180,   195,   233,     0,     0,
       0,     0,     0,     0,     0,   235,     0,   130,   166,     0,
       0,     0,   212,   214,     0,     0,   219,   200,   208,     0,
     201,   210,   211,     0,     0,   215,     0,   216,   235,     0,
     220,   218,     0,   213,   217
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    92,    93,    94,    95,    96,   348,    98,    99,   100,
     218,   321,   332,   322,   323,   101,   102,   103,   104,   105,
     106,   472,   344,   107,   108,   109,   110,   111,   112,   453,
     300,   113,   114,   115,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   212,   499,   500,   345,   346,
     347,   349,   350,   571,   636,   127,   128,   129,   130,   131,
     132,   182,   183,   486,   133,   134,   249,   415,   663,   664,
     666,   695,   696,   529,   402,   532,   591,   533,   135,   136,
     137,   138,   173,   139,   140,   228,   229,   230,   506,   355,
     141,   142,   143,   286,   270,   281,   144,   260,   145,   146,
     147,   267,   148,   264,   149,   150,   151,   152,   277,   153,
     154,   155,   156,   157,   288,   158,   159,   292,   160,   161,
     162,   163,   164,   165,   166,   177,   167,   419,   420,   421,
     168,   169,   170,   475,   351,   352,   213,   353
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -479
static const yytype_int16 yypact[] =
{
     418,  -479,    36,  -479,   -62,  -479,  -479,  -479,    47,   -14,
    1104,    50,    68,  -479,  -479,  -479,  -479,  -479,  -479,  -479,
    -479,  1104,  1104,   116,     8,     8,     8,     8,     8,     8,
     300,  -479,  -479,  -479,   770,   109,    51,  -479,    29,  -479,
    -479,  -479,  -479,   137,   157,  -479,  -479,  -479,  -479,  -479,
    -479,   129,  -479,   221,    54,    71,   210,   -54,   124,    -2,
     -36,   182,   172,    92,   217,   -15,   224,   148,    49,   297,
    -479,  -479,   319,   319,  -479,  1104,   285,   328,   330,   348,
     355,  -479,   254,   291,   -40,  -479,  -479,  -479,  -479,  -479,
    -479,   327,   379,   595,  -479,    -8,  -479,  -479,  -479,  -479,
    -479,    11,   200,  -479,  -479,   360,  1212,  -479,  -479,  -479,
    -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,
    -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,
    -479,  -479,  -479,   944,   341,   201,    95,   316,  1104,    95,
    1104,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,
    -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,
    -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,
    -479,     0,   334,  -479,   367,  -479,   199,   218,   208,   305,
     308,   -11,  -479,   230,  -479,  -479,  -479,  -479,  -479,  -479,
    -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,
    -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,
    -479,  -479,   216,  -479,   227,  -479,   350,  -479,  -479,  1104,
     388,   385,  1104,   234,   235,   237,   238,  -479,  -479,   383,
       8,  -479,  -479,   -62,  -479,  -479,   353,   256,   257,   263,
     264,   212,   267,   275,   277,   278,   279,   198,   280,    46,
    -479,   286,    76,   380,   384,   345,  -479,  -479,  -479,   351,
    -479,   -25,   -44,    38,  -479,   -16,   386,  -479,   207,   392,
    -479,  -479,   124,  -479,  -479,  -479,   393,  -479,   394,  -479,
     346,  -479,  -479,  -479,  -479,   361,  -479,   301,  -479,   124,
    -479,   363,  -479,  -479,  -479,  -479,   400,   329,   402,   403,
     354,   354,  -479,  1104,   405,   406,   407,   409,   376,   377,
    -479,   378,   397,  -479,  -479,  -479,  -479,   447,  -479,   414,
    -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,
    -479,   417,   325,  -479,  -479,  -479,  -479,  -479,  -479,    13,
    -479,  -479,  -479,    10,   489,   201,  -479,  -479,  -479,  -479,
    -479,  -479,   381,   206,  -479,   490,   381,  -479,  -479,   491,
       3,  -479,   319,  -479,  -479,  -479,   120,   337,   338,   340,
     147,   -14,     2,  1104,  -479,  -479,  -479,  -479,  -479,   408,
     284,   439,    -7,  -479,  -479,  -479,   342,   136,   136,   136,
     136,   -27,   343,   347,   349,   352,   356,   357,   358,   359,
     364,   365,  -479,   136,   136,   136,   136,   136,   366,   368,
     136,  -479,   443,   444,  -479,  -479,  -479,   470,   471,  -479,
     370,  -479,  -479,  -479,  -479,   410,   124,  -479,   124,    41,
    -479,   124,  -479,  -479,  -479,   451,  -479,   124,  -479,  -479,
    -479,  -479,  -479,  -479,   -69,   427,  -479,  -479,   464,   387,
    -479,  -479,   468,  -479,  -479,  -479,   532,   535,   389,  -479,
    -479,  -479,  -479,  -479,  -479,  -479,  -479,   390,  -479,   382,
     169,    95,  -479,  -479,   -29,  -479,  1104,  1104,  -479,    95,
      -9,    95,   354,  -479,  -479,  -479,  -479,   120,   120,   120,
     416,   506,   515,  -479,  -479,  -479,  -479,  -479,     7,   110,
    -479,   429,   431,  -479,  -479,  -479,   432,   538,   -53,   442,
     136,  -479,  -479,  -479,  -479,  -479,  -479,   136,   550,   136,
     136,   136,   136,   136,   136,   136,   136,   136,   136,   -13,
    -479,  -479,  -479,  -479,  -479,  -479,   450,   452,  -479,  -479,
    -479,  -479,   574,    76,  -479,  -479,  -479,   124,  -479,  -479,
    -479,  -479,   521,   522,   460,   486,   559,  -479,   560,   561,
      26,   562,  -479,  -479,  -479,  -479,   563,  -479,  1070,  -479,
     569,    70,  -479,  -479,  -479,  -479,  -479,   120,   472,   473,
    -479,   572,     2,  -479,  1104,  -479,  -479,   474,   475,  -479,
    -479,  -479,  -479,    -5,    -4,    -3,    -1,  -479,   479,  -479,
    -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,
     212,     9,    15,    17,    18,    19,   140,   589,    20,  -479,
    -479,  -479,  -479,  -479,   578,   481,  -479,  -479,  -479,  -479,
    -479,  -479,   480,  1104,  -479,  -479,   111,  -479,  -479,   120,
     120,  -479,  -479,  -479,  -479,  -479,    21,  -479,  -479,  -479,
    -479,   136,  -479,  -479,  -479,  -479,  -479,  -479,   482,   483,
     484,   485,   487,   488,   492,   495,   496,  -479,  -479,   585,
    -479,  -479,   587,  -479,  -479,  -479,  -479,  -479,   136,   136,
     -59,   508,   588,   633,   140,   136,   652,  -479,  -479,   510,
     511,   514,  -479,  -479,   516,   513,   518,  -479,  -479,    42,
    -479,  -479,  -479,   136,   136,  -479,   588,  -479,   136,   517,
    -479,  -479,   519,  -479,  -479
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -479,  -479,  -479,   607,   668,   571,     1,  -479,  -479,  -479,
    -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,
    -479,  -479,   534,  -479,  -479,  -479,  -479,  -479,  -479,  -287,
     -60,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,
    -479,  -479,  -479,  -479,   605,   632,  -479,   126,  -146,  -281,
    -479,  -134,   138,  -479,  -479,   128,   131,   141,   151,   164,
    -479,   339,  -479,  -455,   577,  -479,  -479,  -479,    28,  -479,
    -479,    12,  -479,  -227,   103,  -402,  -478,  -381,  -479,  -479,
    -479,  -479,   494,  -479,  -479,   253,   493,  -479,  -479,  -479,
    -479,  -479,  -479,   290,   -55,   436,  -479,  -479,  -479,  -479,
    -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,
    -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,
    -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,  -479,   174,
    -479,  -479,   621,   369,  -479,    97,  -479,   -10
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -435
static const yytype_int16 yytable[] =
{
     214,    97,   273,   274,   534,   357,   513,   514,   515,   516,
     290,   220,   221,   301,   454,   318,   318,     3,     4,     5,
       6,     7,   530,   531,    10,   360,   535,   -92,   609,   538,
     -93,   588,   574,   575,   576,    97,   647,   648,   649,   597,
     650,  -167,   316,    24,    24,    25,    26,    27,    28,    29,
     653,   494,   223,   224,   552,   565,   654,   691,   655,   656,
     657,   667,   676,  -434,   473,   214,   174,   172,   433,   215,
     178,   216,   225,   369,   428,   318,   296,   508,   310,   473,
     269,   311,   275,   179,   180,   429,   495,   411,   412,   413,
     269,   580,   226,   426,    97,    39,   340,   219,   278,   272,
     318,   496,   320,    24,   279,   280,   333,   253,   254,   269,
     629,   509,   553,   434,   217,   175,   171,   176,   435,   269,
     497,   297,   638,   605,   606,   692,   227,   276,    24,   592,
     414,   589,   312,   289,    97,   417,   592,   418,   599,   600,
     601,   602,   603,   604,   630,   222,   607,   608,   184,   185,
     186,   187,   188,   189,   566,   517,   431,   190,   518,   547,
     191,   192,   193,   194,   195,   196,   181,   252,   610,   370,
    -167,   317,   269,   677,   570,   269,   610,   610,   610,   197,
     610,   265,   498,   343,   674,   675,   343,   581,   266,   473,
     610,   319,  -434,   343,   468,   573,   610,   470,   610,   610,
     610,   610,   610,   268,   483,   269,   427,   430,   432,   375,
     284,   255,   378,   438,   285,   198,   199,   440,   200,   484,
     511,   256,   201,   202,   512,   203,   707,   204,   205,   325,
     708,   490,   326,   334,   446,   356,   335,   358,   485,   206,
     658,   257,   327,   207,   491,   492,   336,   208,   209,   258,
     259,   210,   328,   659,   660,   661,   337,   662,   269,   184,
     185,   186,   187,   188,   189,   329,   282,   211,   190,   338,
     592,   191,   192,   193,   194,   195,   196,   476,   231,   232,
     233,   234,   235,   699,   364,   365,   593,   594,   595,   596,
     197,   582,   672,   455,   583,   673,   391,   689,   690,   293,
     294,   295,   482,   611,   612,   613,   712,   614,   615,   408,
     409,   618,   392,   393,   394,   395,   283,   396,   397,   398,
     399,   400,   709,   710,   298,   401,   198,   199,   271,   200,
     503,   504,   505,   201,   202,   287,   203,   564,   204,   205,
     437,   269,   291,   478,   469,   569,   299,   572,   261,   262,
     206,   263,   343,   563,   207,   304,   303,   305,   208,   209,
     306,   307,   210,   501,   646,   318,     3,     4,     5,     6,
       7,   545,   308,   546,   548,   309,   549,   313,   211,   314,
     331,    30,   551,   236,   343,   354,   361,   362,   363,   367,
     366,   477,   368,    24,    25,    26,    27,    28,    29,   372,
     237,   238,   239,   240,   241,   242,   243,   244,   245,   246,
     247,   371,   373,   248,   374,   376,   377,   379,   380,     1,
     381,   382,   383,     2,     3,     4,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,   386,   387,   388,
      16,    17,    18,    19,    20,   389,   390,    21,    22,   403,
      23,    24,    25,    26,    27,    28,    29,   404,    30,   405,
     406,   407,   410,   424,   422,   416,   567,   568,   423,   425,
     436,    31,    32,    33,    34,    35,   439,   441,   442,   444,
     278,   447,   445,    36,   448,   449,   450,   451,   452,   456,
     457,   458,   621,   459,   460,   461,   462,   464,   465,    37,
      38,   466,    39,    40,    41,    42,   467,    43,    44,    45,
      46,    47,    48,    49,    50,   463,   471,   479,   481,   487,
     488,   474,   489,   507,   510,   519,   502,   539,   540,   520,
     541,   521,   285,   542,   522,   550,   555,    51,   523,   524,
     525,   526,    52,    53,    54,   554,   527,   528,   536,    55,
     537,   543,   557,    56,    57,    58,    59,    60,   634,   558,
      61,    62,   559,    63,    64,    65,   562,    66,    67,   556,
     561,   560,    68,    69,   643,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
     578,    85,    86,    87,    88,    89,    90,    91,   577,   579,
       2,     3,     4,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,   584,   585,   586,    16,    17,    18,
      19,    20,   587,   671,    21,    22,   590,    23,    24,    25,
      26,    27,    28,    29,   598,    30,   616,   619,   617,   622,
     623,   624,   625,   626,   627,   628,   631,   632,    31,    32,
      33,    34,    35,   635,   639,   640,   641,   665,   644,   645,
      36,   651,   668,   669,   670,   678,   679,   680,   681,   687,
     682,   688,   694,   684,   697,   683,    37,    38,   685,    39,
      40,    41,    42,   686,    43,    44,    45,    46,    47,    48,
      49,    50,   693,   700,   701,   702,   703,   705,   704,   706,
     315,   713,   251,   714,   341,   359,   324,   302,   642,   637,
     493,   342,   698,   652,    51,   544,   443,   620,   711,    52,
      53,    54,   330,   384,     0,   480,    55,   385,     0,     0,
      56,    57,    58,    59,    60,     0,     0,    61,    62,     0,
      63,    64,    65,     0,    66,    67,     0,     0,     0,    68,
      69,     0,    70,    71,    72,    73,    74,    75,    76,    77,
      78,    79,    80,    81,    82,    83,    84,     0,    85,    86,
      87,    88,    89,    90,    91,     2,     3,     4,     5,     6,
       7,     8,     9,    10,    11,    12,    13,    14,    15,     0,
       0,     0,    16,    17,    18,    19,    20,     0,     0,    21,
      22,     0,    23,    24,    25,    26,    27,    28,    29,     0,
      30,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    31,    32,    33,   250,    35,     0,     0,
       0,     0,     0,     0,     0,    36,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    37,    38,     0,    39,    40,    41,    42,     0,    43,
      44,    45,    46,    47,    48,    49,    50,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    51,
       0,     0,     0,     0,    52,    53,    54,     0,     0,     0,
       0,    55,     0,     0,     0,    56,    57,    58,    59,    60,
       0,     0,    61,    62,     0,    63,    64,    65,     0,    66,
      67,     0,     0,     0,    68,    69,     0,    70,    71,    72,
      73,    74,    75,    76,    77,    78,    79,    80,    81,    82,
      83,    84,     0,    85,    86,    87,    88,    89,    90,     2,
       3,     4,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,     0,     0,     0,    16,    17,    18,    19,
      20,     0,     0,    21,    22,     0,    23,    24,    25,    26,
      27,    28,    29,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    31,    32,    33,
     250,    35,     0,     0,     0,     0,     0,     0,     0,    36,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    37,    38,     0,    39,    40,
      41,    42,     0,    43,    44,    45,    46,    47,    48,    49,
      50,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    51,     0,     0,     0,     0,    52,    53,
      54,     0,     0,     0,     0,    55,     0,     0,     0,    56,
      57,    58,    59,    60,     0,     0,    61,    62,     0,    63,
      64,    65,     0,    66,    67,     0,     0,     0,    68,    69,
       0,    70,    71,    72,    73,    74,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,     0,    85,    86,    87,
      88,    89,    90,   184,   185,   186,   187,   188,   189,     0,
       0,     0,   190,     0,     0,   191,   192,   193,   194,   195,
     196,   633,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   197,     0,     0,   184,   185,   186,
     187,   188,   189,     0,     0,     0,   190,     0,     0,   191,
     192,   193,   194,   195,   196,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   197,     0,
     198,   199,     0,   200,     0,     0,     0,   201,   202,     0,
     203,     0,   204,   205,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   206,     0,     0,     0,   207,     0,
       0,     0,   208,   209,   198,   199,   210,   200,     0,     0,
       0,   201,   202,     0,   203,     0,   204,   205,     0,     0,
       0,     0,   211,     0,     0,     0,     0,     0,   206,     0,
       0,     0,   207,     0,     0,     0,   208,   209,     0,     0,
     210,     0,     0,     0,     0,   184,   185,   186,   187,   188,
     189,     0,     0,     0,   190,     0,   211,   191,   192,   193,
     194,   195,   196,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   339,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   198,   199,     0,   200,     0,     0,     0,   201,
     202,     0,   203,     0,   204,   205,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   206,     0,     0,     0,
     207,     0,     0,     0,   208,   209,     0,     0,   210,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   211
};

static const yytype_int16 yycheck[] =
{
      10,     0,    57,    58,   406,   139,   387,   388,   389,   390,
      65,    21,    22,    73,   301,     5,     5,     6,     7,     8,
       9,    10,   403,   404,    13,   171,   407,    27,    41,   410,
      27,    84,   487,   488,   489,    34,    41,    41,    41,   517,
      41,    50,    50,    33,    33,    34,    35,    36,    37,    38,
      41,    49,    44,    45,   123,    84,    41,   116,    41,    41,
      41,    41,    41,    50,   345,    75,    19,   129,    84,    19,
      84,    21,    64,    84,   118,     5,    27,    84,   118,   360,
     134,   121,    84,    97,    98,   129,    84,    41,    42,    43,
     134,    84,    84,   118,    93,    84,   106,    29,   134,   153,
       5,    99,   101,    33,   140,   141,   105,    56,    57,   134,
      84,   118,   181,   129,    64,    68,    80,    70,   134,   134,
     118,    72,   577,   525,   526,   184,   118,   129,    33,   510,
      84,   184,   172,   148,   133,    59,   517,    61,   519,   520,
     521,   522,   523,   524,   118,    29,   527,   528,    53,    54,
      55,    56,    57,    58,   183,   182,   118,    62,   185,   118,
      65,    66,    67,    68,    69,    70,   180,    58,   181,   180,
     179,   179,   134,   651,   183,   134,   181,   181,   181,    84,
     181,   127,   180,   183,   639,   640,   183,   180,   134,   470,
     181,   180,   179,   183,   184,   482,   181,   343,   181,   181,
     181,   181,   181,   132,    84,   134,   261,   262,   263,   219,
     118,   182,   222,   268,   122,   120,   121,   272,   123,    99,
      84,    84,   127,   128,    88,   130,   184,   132,   133,   101,
     188,    84,   101,   105,   289,   138,   105,   140,   118,   144,
     100,    84,   101,   148,    97,    98,   105,   152,   153,   120,
     121,   156,   101,   113,   114,   115,   105,   117,   134,    53,
      54,    55,    56,    57,    58,   101,    84,   172,    62,   105,
     651,    65,    66,    67,    68,    69,    70,    71,    25,    26,
      27,    28,    29,   685,    66,    67,   513,   514,   515,   516,
      84,   181,   181,   303,   184,   184,    84,   678,   679,   151,
     152,   153,   362,   530,   531,   532,   708,   534,   535,   111,
     112,   538,   100,   101,   102,   103,   144,   105,   106,   107,
     108,   109,   703,   704,    27,   113,   120,   121,   118,   123,
      46,    47,    48,   127,   128,   118,   130,   471,   132,   133,
     133,   134,   118,   353,   343,   479,    27,   481,   127,   128,
     144,   130,   183,   184,   148,    27,    71,    27,   152,   153,
      12,     6,   156,   373,   591,     5,     6,     7,     8,     9,
      10,   426,   118,   428,   429,    84,   431,    50,   172,     0,
     180,    40,   437,    83,   183,    69,    52,    20,   189,    84,
     182,   185,    84,    33,    34,    35,    36,    37,    38,   183,
     100,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   181,   185,   113,    64,    27,    31,   183,   183,     1,
     183,   183,    39,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,    16,    17,    18,    84,   182,   182,
      22,    23,    24,    25,    26,   182,   182,    29,    30,   182,
      32,    33,    34,    35,    36,    37,    38,   182,    40,   182,
     182,   182,   182,   118,    84,   179,   476,   477,    84,   118,
      84,    53,    54,    55,    56,    57,    84,    84,    84,   118,
     134,   118,   181,    65,    84,   156,    84,    84,   134,    84,
      84,    84,   547,    84,   118,   118,   118,    50,    84,    81,
      82,    84,    84,    85,    86,    87,   181,    89,    90,    91,
      92,    93,    94,    95,    96,   118,    27,    27,    27,   182,
     182,   140,   182,    84,   182,   182,   118,    84,    84,   182,
      60,   182,   122,    62,   182,    84,    72,   119,   182,   182,
     182,   182,   124,   125,   126,   118,   182,   182,   182,   131,
     182,   181,    84,   135,   136,   137,   138,   139,   568,    27,
     142,   143,    27,   145,   146,   147,   184,   149,   150,   182,
     180,   182,   154,   155,   584,   157,   158,   159,   160,   161,
     162,   163,   164,   165,   166,   167,   168,   169,   170,   171,
      84,   173,   174,   175,   176,   177,   178,   179,   182,    84,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,   185,   184,   184,    22,    23,    24,
      25,    26,    84,   633,    29,    30,   184,    32,    33,    34,
      35,    36,    37,    38,    84,    40,   186,    63,   186,   118,
     118,   181,   156,    84,    84,    84,    84,    84,    53,    54,
      55,    56,    57,    84,   182,   182,    84,    68,   184,   184,
      65,   182,    84,   182,   184,   183,   183,   183,   183,    84,
     183,    84,    84,   181,    41,   187,    81,    82,   183,    84,
      85,    86,    87,   187,    89,    90,    91,    92,    93,    94,
      95,    96,   184,    41,   184,   184,   182,   184,   182,   181,
      93,   184,    34,   184,   133,   171,   101,    75,   582,   571,
     371,   134,   684,   610,   119,   425,   280,   543,   706,   124,
     125,   126,   101,   230,    -1,   356,   131,   233,    -1,    -1,
     135,   136,   137,   138,   139,    -1,    -1,   142,   143,    -1,
     145,   146,   147,    -1,   149,   150,    -1,    -1,    -1,   154,
     155,    -1,   157,   158,   159,   160,   161,   162,   163,   164,
     165,   166,   167,   168,   169,   170,   171,    -1,   173,   174,
     175,   176,   177,   178,   179,     5,     6,     7,     8,     9,
      10,    11,    12,    13,    14,    15,    16,    17,    18,    -1,
      -1,    -1,    22,    23,    24,    25,    26,    -1,    -1,    29,
      30,    -1,    32,    33,    34,    35,    36,    37,    38,    -1,
      40,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    53,    54,    55,    56,    57,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    65,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    81,    82,    -1,    84,    85,    86,    87,    -1,    89,
      90,    91,    92,    93,    94,    95,    96,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   119,
      -1,    -1,    -1,    -1,   124,   125,   126,    -1,    -1,    -1,
      -1,   131,    -1,    -1,    -1,   135,   136,   137,   138,   139,
      -1,    -1,   142,   143,    -1,   145,   146,   147,    -1,   149,
     150,    -1,    -1,    -1,   154,   155,    -1,   157,   158,   159,
     160,   161,   162,   163,   164,   165,   166,   167,   168,   169,
     170,   171,    -1,   173,   174,   175,   176,   177,   178,     5,
       6,     7,     8,     9,    10,    11,    12,    13,    14,    15,
      16,    17,    18,    -1,    -1,    -1,    22,    23,    24,    25,
      26,    -1,    -1,    29,    30,    -1,    32,    33,    34,    35,
      36,    37,    38,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    53,    54,    55,
      56,    57,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    65,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    81,    82,    -1,    84,    85,
      86,    87,    -1,    89,    90,    91,    92,    93,    94,    95,
      96,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   119,    -1,    -1,    -1,    -1,   124,   125,
     126,    -1,    -1,    -1,    -1,   131,    -1,    -1,    -1,   135,
     136,   137,   138,   139,    -1,    -1,   142,   143,    -1,   145,
     146,   147,    -1,   149,   150,    -1,    -1,    -1,   154,   155,
      -1,   157,   158,   159,   160,   161,   162,   163,   164,   165,
     166,   167,   168,   169,   170,   171,    -1,   173,   174,   175,
     176,   177,   178,    53,    54,    55,    56,    57,    58,    -1,
      -1,    -1,    62,    -1,    -1,    65,    66,    67,    68,    69,
      70,    71,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    84,    -1,    -1,    53,    54,    55,
      56,    57,    58,    -1,    -1,    -1,    62,    -1,    -1,    65,
      66,    67,    68,    69,    70,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    84,    -1,
     120,   121,    -1,   123,    -1,    -1,    -1,   127,   128,    -1,
     130,    -1,   132,   133,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   144,    -1,    -1,    -1,   148,    -1,
      -1,    -1,   152,   153,   120,   121,   156,   123,    -1,    -1,
      -1,   127,   128,    -1,   130,    -1,   132,   133,    -1,    -1,
      -1,    -1,   172,    -1,    -1,    -1,    -1,    -1,   144,    -1,
      -1,    -1,   148,    -1,    -1,    -1,   152,   153,    -1,    -1,
     156,    -1,    -1,    -1,    -1,    53,    54,    55,    56,    57,
      58,    -1,    -1,    -1,    62,    -1,   172,    65,    66,    67,
      68,    69,    70,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    84,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   120,   121,    -1,   123,    -1,    -1,    -1,   127,
     128,    -1,   130,    -1,   132,   133,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   144,    -1,    -1,    -1,
     148,    -1,    -1,    -1,   152,   153,    -1,    -1,   156,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   172
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     1,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    22,    23,    24,    25,
      26,    29,    30,    32,    33,    34,    35,    36,    37,    38,
      40,    53,    54,    55,    56,    57,    65,    81,    82,    84,
      85,    86,    87,    89,    90,    91,    92,    93,    94,    95,
      96,   119,   124,   125,   126,   131,   135,   136,   137,   138,
     139,   142,   143,   145,   146,   147,   149,   150,   154,   155,
     157,   158,   159,   160,   161,   162,   163,   164,   165,   166,
     167,   168,   169,   170,   171,   173,   174,   175,   176,   177,
     178,   179,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   205,   206,   207,   208,   209,   210,   213,   214,   215,
     216,   217,   218,   221,   222,   223,   224,   225,   226,   227,
     228,   229,   230,   231,   232,   233,   234,   245,   246,   247,
     248,   249,   250,   254,   255,   268,   269,   270,   271,   273,
     274,   280,   281,   282,   286,   288,   289,   290,   292,   294,
     295,   296,   297,   299,   300,   301,   302,   303,   305,   306,
     308,   309,   310,   311,   312,   313,   314,   316,   320,   321,
     322,    80,   129,   272,    19,    68,    70,   315,    84,    97,
      98,   180,   251,   252,    53,    54,    55,    56,    57,    58,
      62,    65,    66,    67,    68,    69,    70,    84,   120,   121,
     123,   127,   128,   130,   132,   133,   144,   148,   152,   153,
     156,   172,   235,   326,   327,    19,    21,    64,   200,    29,
     327,   327,    29,    44,    45,    64,    84,   118,   275,   276,
     277,   275,   275,   275,   275,   275,    83,   100,   101,   102,
     103,   104,   105,   106,   107,   108,   109,   110,   113,   256,
      56,   194,    58,    56,    57,   182,    84,    84,   120,   121,
     287,   127,   128,   130,   293,   127,   134,   291,   132,   134,
     284,   118,   153,   284,   284,    84,   129,   298,   134,   140,
     141,   285,    84,   144,   118,   122,   283,   118,   304,   148,
     284,   118,   307,   151,   152,   153,    27,    72,    27,    27,
     220,   220,   235,    71,    27,    27,    12,     6,   118,    84,
     118,   121,   172,    50,     0,   193,    50,   179,     5,   180,
     196,   201,   203,   204,   234,   245,   246,   247,   248,   249,
     322,   180,   202,   196,   245,   246,   247,   248,   249,    84,
     327,   195,   254,   183,   212,   238,   239,   240,   196,   241,
     242,   324,   325,   327,    69,   279,   325,   241,   325,   212,
     238,    52,    20,   189,    66,    67,   182,    84,    84,    84,
     180,   181,   183,   185,    64,   327,    27,    31,   327,   183,
     183,   183,   183,    39,   276,   272,    84,   182,   182,   182,
     182,    84,   100,   101,   102,   103,   105,   106,   107,   108,
     109,   113,   264,   182,   182,   182,   182,   182,   111,   112,
     182,    41,    42,    43,    84,   257,   179,    59,    61,   317,
     318,   319,    84,    84,   118,   118,   118,   284,   118,   129,
     284,   118,   284,    84,   129,   134,    84,   133,   284,    84,
     284,    84,    84,   285,   118,   181,   284,   118,    84,   156,
      84,    84,   134,   219,   219,   327,    84,    84,    84,    84,
     118,   118,   118,   118,    50,    84,    84,   181,   184,   196,
     238,    27,   211,   239,   140,   323,    71,   185,   327,    27,
     323,    27,   220,    84,    99,   118,   253,   182,   182,   182,
      84,    97,    98,   251,    49,    84,    99,   118,   180,   236,
     237,   327,   118,    46,    47,    48,   278,    84,    84,   118,
     182,    84,    88,   267,   267,   267,   267,   182,   185,   182,
     182,   182,   182,   182,   182,   182,   182,   182,   182,   263,
     267,   267,   265,   267,   265,   267,   182,   182,   267,    84,
      84,    60,    62,   181,   283,   284,   284,   118,   284,   284,
      84,   284,   123,   181,   118,    72,   182,    84,    27,    27,
     182,   180,   184,   184,   241,    84,   183,   327,   327,   241,
     183,   243,   241,   219,   253,   253,   253,   182,    84,    84,
      84,   180,   181,   184,   185,   184,   184,    84,    84,   184,
     184,   266,   267,   263,   263,   263,   263,   266,    84,   267,
     267,   267,   267,   267,   267,   265,   265,   267,   267,    41,
     181,   263,   263,   263,   263,   263,   186,   186,   263,    63,
     319,   284,   118,   118,   181,   156,    84,    84,    84,    84,
     118,    84,    84,    71,   327,    84,   244,   242,   253,   182,
     182,    84,   237,   327,   184,   184,   263,    41,    41,    41,
      41,   182,   264,    41,    41,    41,    41,    41,   100,   113,
     114,   115,   117,   258,   259,    68,   260,    41,    84,   182,
     184,   327,   181,   184,   253,   253,    41,   266,   183,   183,
     183,   183,   183,   187,   181,   183,   187,    84,    84,   267,
     267,   116,   184,   184,    84,   261,   262,    41,   258,   265,
      41,   184,   184,   182,   182,   184,   181,   184,   188,   267,
     267,   261,   265,   184,   184
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

  case 54:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_TABLE; ;}
    break;

  case 55:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 56:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; ;}
    break;

  case 57:

    {;}
    break;

  case 58:

    {;}
    break;

  case 59:

    {;}
    break;

  case 60:

    {;}
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

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(2) - (2)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 66:

    {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = (yyvsp[(4) - (4)].str);
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              ;}
    break;

  case 67:

    {
                          ObProxyTextPsParseNode *node = NULL;
                          malloc_parse_node(node);
                          node->str_value_ = (yyvsp[(2) - (2)].str);
                          add_text_ps_node(result->text_ps_parse_info_, node);
                        ;}
    break;

  case 70:

    {
                      result->text_ps_inner_stmt_type_ = OBPROXY_T_TEXT_PS_PREPARE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 71:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (2)].str);
                    ;}
    break;

  case 72:

    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 73:

    {
            ;}
    break;

  case 74:

    {
            ;}
    break;

  case 75:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 76:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = (yyvsp[(3) - (3)].str);
            ;}
    break;

  case 77:

    { result->cur_stmt_type_ = OBPROXY_T_GRANT; ;}
    break;

  case 78:

    { result->cur_stmt_type_ = OBPROXY_T_REVOKE; ;}
    break;

  case 79:

    { result->cur_stmt_type_ = OBPROXY_T_ANALYZE; ;}
    break;

  case 80:

    { result->cur_stmt_type_ = OBPROXY_T_PURGE; ;}
    break;

  case 81:

    { result->cur_stmt_type_ = OBPROXY_T_FLASHBACK; ;}
    break;

  case 82:

    { result->cur_stmt_type_ = OBPROXY_T_COMMENT; ;}
    break;

  case 83:

    { result->cur_stmt_type_ = OBPROXY_T_AUDIT; ;}
    break;

  case 84:

    { result->cur_stmt_type_ = OBPROXY_T_NOAUDIT; ;}
    break;

  case 87:

    {;}
    break;

  case 88:

    {;}
    break;

  case 89:

    {;}
    break;

  case 94:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_TX_RO; ;}
    break;

  case 98:

    { result->col_name_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 99:

    {;}
    break;

  case 100:

    {;}
    break;

  case 101:

    {;}
    break;

  case 102:

    {;}
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

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_DATABASE; ;}
    break;

  case 108:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DATABASES; ;}
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

    {;}
    break;

  case 113:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE; ;}
    break;

  case 114:

    {
                      result->table_info_.table_name_ = (yyvsp[(2) - (2)].str);
                      result->cur_stmt_type_ = OBPROXY_T_DESC;
                      result->sub_stmt_type_ = OBPROXY_T_SUB_DESC_TABLE;
                  ;}
    break;

  case 115:

    {
                    result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_COLUMNS;
                    result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                  ;}
    break;

  case 116:

    {
                    result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_COLUMNS;
                    result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                    result->table_info_.database_name_ = (yyvsp[(5) - (5)].str);
                  ;}
    break;

  case 117:

    {
                 result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_INDEX;
                 result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
               ;}
    break;

  case 118:

    {
                 result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_INDEX;
                 result->table_info_.table_name_ = (yyvsp[(3) - (5)].str);
                 result->table_info_.database_name_ = (yyvsp[(5) - (5)].str);
               ;}
    break;

  case 119:

    {;}
    break;

  case 120:

    { result->table_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 121:

    {;}
    break;

  case 122:

    { result->table_info_.database_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 123:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLES;
                ;}
    break;

  case 124:

    {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_FULL_TABLES;
                ;}
    break;

  case 125:

    {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLE_STATUS;
                      ;}
    break;

  case 126:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DB_VERSION; ;}
    break;

  case 127:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID; ;}
    break;

  case 128:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 129:

    {
                     SET_ICMD_SECOND_STRING((yyvsp[(5) - (5)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 130:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (7)].str));
                     SET_ICMD_SECOND_STRING((yyvsp[(7) - (7)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 ;}
    break;

  case 131:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY; ;}
    break;

  case 132:

    {
                     result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY;
                 ;}
    break;

  case 133:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_ROUTE_ADDR; ;}
    break;

  case 134:

    {
                              result->cur_stmt_type_ = OBPROXY_T_SET_ROUTE_ADDR;
                              result->cmd_info_.integer_[0] = (yyvsp[(3) - (3)].num);
                           ;}
    break;

  case 135:

    {;}
    break;

  case 136:

    {;}
    break;

  case 137:

    {;}
    break;

  case 138:

    {;}
    break;

  case 139:

    {;}
    break;

  case 140:

    {;}
    break;

  case 142:

    {
                   result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                 ;}
    break;

  case 143:

    {
                   result->table_info_.package_name_ = (yyvsp[(1) - (3)].str);
                   result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                 ;}
    break;

  case 144:

    {
                   result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                   result->table_info_.package_name_ = (yyvsp[(3) - (5)].str);
                   result->table_info_.table_name_ = (yyvsp[(5) - (5)].str);
                 ;}
    break;

  case 145:

    {
                result->call_parse_info_.node_count_ = 0;
              ;}
    break;

  case 146:

    {
                result->call_parse_info_.node_count_ = 0;
                add_call_node(result->call_parse_info_, (yyvsp[(1) - (1)].node));
              ;}
    break;

  case 147:

    {
                add_call_node(result->call_parse_info_, (yyvsp[(3) - (3)].node));
              ;}
    break;

  case 148:

    {
            malloc_call_node((yyval.node), CALL_TOKEN_STR_VAL);
            (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 149:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_INT_VAL);
           (yyval.node)->int_value_ = (yyvsp[(1) - (1)].num);
         ;}
    break;

  case 150:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_NUMBER_VAL);
           (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 151:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_USER_VAR);
           (yyval.node)->str_value_ = (yyvsp[(2) - (2)].str);
         ;}
    break;

  case 152:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_SYS_VAR);
           (yyval.node)->str_value_ = (yyvsp[(3) - (3)].str);
         ;}
    break;

  case 153:

    {
           result->placeholder_list_idx_++;
           malloc_call_node((yyval.node), CALL_TOKEN_PLACE_HOLDER);
           (yyval.node)->placeholder_idx_ = result->placeholder_list_idx_ - 1;
         ;}
    break;

  case 167:

    {
                                                                  handle_stmt_end(result);
                                                                  HANDLE_ACCEPT();
                                                                ;}
    break;

  case 172:

    {
                                                 handle_stmt_end(result);
                                                 HANDLE_ACCEPT();
                                               ;}
    break;

  case 176:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_USER);
        ;}
    break;

  case 177:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 178:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 179:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(5) - (5)].var_node), (yyvsp[(3) - (5)].str), SET_VAR_SYS);
        ;}
    break;

  case 180:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 181:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 182:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_SYS);
        ;}
    break;

  case 183:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_STR);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 184:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_INT);
               (yyval.var_node)->int_value_ = (yyvsp[(1) - (1)].num);
             ;}
    break;

  case 185:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_NUMBER);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 188:

    {;}
    break;

  case 189:

    {;}
    break;

  case 190:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 191:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 192:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 193:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 194:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 195:

    {
              malloc_shard_column_node((yyval.shard_node), (yyvsp[(2) - (7)].str), (yyvsp[(3) - (7)].str), DBMESH_TOKEN_STR_VAL);
              (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (7)].str);
              add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
            ;}
    break;

  case 196:

    { result->trace_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 197:

    { result->rpc_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 198:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 199:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 200:

    {;}
    break;

  case 201:

    {;}
    break;

  case 202:

    { result->target_db_server_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 203:

    {;}
    break;

  case 205:

    { result->has_simple_route_info_ = true; result->simple_route_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 206:

    { result->simple_route_info_.part_key_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 210:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.group_idx_str_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 211:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.table_name_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 212:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 213:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 214:

    { result->dbp_route_info_.sticky_session_ = true; ;}
    break;

  case 215:

    {result->dbp_route_info_.has_shard_key_ = true;;}
    break;

  case 216:

    { result->trace_id_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 217:

    { result->trace_id_ = (yyvsp[(3) - (6)].str); result->rpc_id_ = (yyvsp[(5) - (6)].str); ;}
    break;

  case 218:

    {;}
    break;

  case 220:

    {
                   if (result->dbp_route_info_.shard_key_count_ < OBPROXY_MAX_DBP_SHARD_KEY_NUM) {
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].left_str_ = (yyvsp[(1) - (3)].str);
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].right_str_ = (yyvsp[(3) - (3)].str);
                     ++result->dbp_route_info_.shard_key_count_;
                   }
                 ;}
    break;

  case 223:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 224:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 225:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 226:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 227:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 228:

    { result->trace_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 229:

    { result->rpc_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 230:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 231:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 232:

    { result->target_db_server_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 233:

    {
             malloc_shard_column_node((yyval.shard_node), (yyvsp[(1) - (5)].str), (yyvsp[(3) - (5)].str), DBMESH_TOKEN_STR_VAL);
             (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (5)].str);
             add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
           ;}
    break;

  case 234:

    {;}
    break;

  case 235:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 237:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 259:

    { result->query_timeout_ = (yyvsp[(3) - (4)].num); ;}
    break;

  case 260:

    {;}
    break;

  case 262:

    {
      add_hint_index(result->dbmesh_route_info_, (yyvsp[(3) - (5)].str));
      result->dbmesh_route_info_.index_count_++;
    ;}
    break;

  case 263:

    {;}
    break;

  case 264:

    {;}
    break;

  case 265:

    {;}
    break;

  case 266:

    {;}
    break;

  case 267:

    {;}
    break;

  case 268:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_WEAK); ;}
    break;

  case 269:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_STRONG); ;}
    break;

  case 270:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_FROZEN); ;}
    break;

  case 273:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_WARNINGS; ;}
    break;

  case 274:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_ERRORS; ;}
    break;

  case 275:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 299:

    {;}
    break;

  case 300:

    {;}
    break;

  case 301:

    {;}
    break;

  case 302:

    {;}
    break;

  case 303:

    {;}
    break;

  case 304:

    {;}
    break;

  case 305:

    {
;}
    break;

  case 306:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 307:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 308:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 309:

    {;}
    break;

  case 310:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 311:

    {;}
    break;

  case 312:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 314:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 315:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 316:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 317:

    {;}
    break;

  case 318:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 319:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 320:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 322:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 323:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 324:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 325:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 326:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 328:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 329:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 330:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 331:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 332:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 333:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 334:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 335:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 336:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 337:

    {;}
    break;

  case 338:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 339:

    {;}
    break;

  case 340:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 341:

    {;}
    break;

  case 343:

    {;}
    break;

  case 344:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 345:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 346:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 347:

    {;}
    break;

  case 348:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 349:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 350:

    {;}
    break;

  case 351:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 352:

    {;}
    break;

  case 353:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 354:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 355:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 357:

    {;}
    break;

  case 358:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 359:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 360:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 361:

    {;}
    break;

  case 362:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 364:

    {;}
    break;

  case 365:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 366:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 367:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 368:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 369:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 370:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 371:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 372:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 373:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 374:

    {;}
    break;

  case 375:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 376:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 377:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 378:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 379:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 380:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 381:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 384:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 385:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 386:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 387:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 394:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 395:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 397:

    {;}
    break;

  case 398:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 399:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 400:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT();
                                                ;}
    break;

  case 401:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 402:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 403:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 404:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 405:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 406:

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

