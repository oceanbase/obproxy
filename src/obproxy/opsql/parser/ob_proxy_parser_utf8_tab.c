
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
      case OBPROXY_T_SET_AC_0:
        result->stmt_type_ = OBPROXY_T_OTHERS;
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

#define add_text_ps_execute_node(text_ps_execute_parse_info, execute_parse_node) \
do {                                                      \
  if (NULL != text_ps_execute_parse_info.tail_) {\
    text_ps_execute_parse_info.tail_->next_ = execute_parse_node;\
    text_ps_execute_parse_info.tail_ = execute_parse_node;\
  } else {\
    text_ps_execute_parse_info.head_ = execute_parse_node;\
    text_ps_execute_parse_info.tail_ = execute_parse_node;\
  }\
  ++text_ps_execute_parse_info.node_count_;\
} while(0)

#define malloc_execute_parse_node(execute_parse_node) \
do {                                                                                        \
  if (OB_ISNULL(execute_parse_node = ((ObProxyTextPsExecuteParseNode *)obproxy_parse_malloc(sizeof(ObProxyTextPsExecuteParseNode), result->malloc_pool_)))) { \
    YYABORT;                                                                                \
  } else {                                                                                  \
    execute_parse_node->next_ = NULL;                                                       \
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
     SELECT_HINT_BEGIN = 286,
     UPDATE_HINT_BEGIN = 287,
     DELETE_HINT_BEGIN = 288,
     INSERT_HINT_BEGIN = 289,
     REPLACE_HINT_BEGIN = 290,
     MERGE_HINT_BEGIN = 291,
     HINT_END = 292,
     COMMENT_BEGIN = 293,
     COMMENT_END = 294,
     ROUTE_TABLE = 295,
     ROUTE_PART_KEY = 296,
     QUERY_TIMEOUT = 297,
     READ_CONSISTENCY = 298,
     WEAK = 299,
     STRONG = 300,
     FROZEN = 301,
     PLACE_HOLDER = 302,
     END_P = 303,
     ERROR = 304,
     WHEN = 305,
     FLASHBACK = 306,
     AUDIT = 307,
     NOAUDIT = 308,
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
     AUTOCOMMIT_0 = 334,
     SELECT_OBPROXY_ROUTE_ADDR = 335,
     SET_OBPROXY_ROUTE_ADDR = 336,
     NAME_OB_DOT = 337,
     NAME_OB = 338,
     EXPLAIN = 339,
     DESC = 340,
     DESCRIBE = 341,
     NAME_STR = 342,
     USE = 343,
     HELP = 344,
     SET_NAMES = 345,
     SET_CHARSET = 346,
     SET_PASSWORD = 347,
     SET_DEFAULT = 348,
     SET_OB_READ_CONSISTENCY = 349,
     SET_TX_READ_ONLY = 350,
     GLOBAL = 351,
     SESSION = 352,
     NUMBER_VAL = 353,
     GROUP_ID = 354,
     TABLE_ID = 355,
     ELASTIC_ID = 356,
     TESTLOAD = 357,
     ODP_COMMENT = 358,
     TNT_ID = 359,
     DISASTER_STATUS = 360,
     TRACE_ID = 361,
     RPC_ID = 362,
     DBP_COMMENT = 363,
     ROUTE_TAG = 364,
     SYS_TAG = 365,
     TABLE_NAME = 366,
     SCAN_ALL = 367,
     PARALL = 368,
     SHARD_KEY = 369,
     INT_NUM = 370,
     SHOW_PROXYNET = 371,
     THREAD = 372,
     CONNECTION = 373,
     LIMIT = 374,
     OFFSET = 375,
     SHOW_PROCESSLIST = 376,
     SHOW_PROXYSESSION = 377,
     SHOW_GLOBALSESSION = 378,
     ATTRIBUTE = 379,
     VARIABLES = 380,
     ALL = 381,
     STAT = 382,
     SHOW_PROXYCONFIG = 383,
     DIFF = 384,
     USER = 385,
     LIKE = 386,
     SHOW_PROXYSM = 387,
     SHOW_PROXYCLUSTER = 388,
     SHOW_PROXYRESOURCE = 389,
     SHOW_PROXYCONGESTION = 390,
     SHOW_PROXYROUTE = 391,
     PARTITION = 392,
     ROUTINE = 393,
     SHOW_PROXYVIP = 394,
     SHOW_PROXYMEMORY = 395,
     OBJPOOL = 396,
     SHOW_SQLAUDIT = 397,
     SHOW_WARNLOG = 398,
     SHOW_PROXYSTAT = 399,
     REFRESH = 400,
     SHOW_PROXYTRACE = 401,
     SHOW_PROXYINFO = 402,
     BINARY = 403,
     UPGRADE = 404,
     IDC = 405,
     SHOW_TOPOLOGY = 406,
     GROUP_NAME = 407,
     SHOW_DB_VERSION = 408,
     SHOW_DATABASES = 409,
     SHOW_TABLES = 410,
     SELECT_DATABASE = 411,
     SHOW_CREATE_TABLE = 412,
     SELECT_PROXY_VERSION = 413,
     ALTER_PROXYCONFIG = 414,
     ALTER_PROXYRESOURCE = 415,
     PING_PROXY = 416,
     KILL_PROXYSESSION = 417,
     KILL_GLOBALSESSION = 418,
     KILL = 419,
     QUERY = 420
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
#define YYFINAL  305
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   1423

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  177
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  128
/* YYNRULES -- Number of rules.  */
#define YYNRULES  427
/* YYNRULES -- Number of states.  */
#define YYNSTATES  681

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   420

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,   175,     2,     2,     2,     2,
     170,   171,   176,     2,   168,     2,   172,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   166,
       2,   169,     2,     2,   167,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   173,     2,   174,     2,     2,     2,     2,
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
     165
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     5,     7,     9,    12,    15,    18,    22,
      24,    27,    31,    33,    35,    37,    39,    41,    43,    45,
      47,    49,    51,    53,    55,    57,    59,    61,    63,    65,
      67,    69,    71,    73,    75,    77,    79,    81,    85,    88,
      91,    94,    97,   100,   103,   106,   109,   112,   115,   118,
     121,   123,   125,   128,   130,   132,   134,   136,   137,   139,
     141,   144,   146,   148,   150,   152,   154,   156,   158,   160,
     163,   168,   172,   175,   178,   183,   185,   187,   189,   191,
     193,   195,   197,   199,   201,   204,   206,   208,   210,   211,
     214,   215,   217,   220,   226,   230,   232,   236,   239,   243,
     245,   247,   249,   251,   253,   255,   257,   259,   261,   263,
     265,   268,   271,   273,   275,   279,   285,   293,   295,   299,
     301,   303,   305,   307,   309,   311,   317,   319,   323,   329,
     330,   332,   336,   338,   340,   342,   345,   349,   351,   353,
     356,   358,   361,   365,   369,   371,   373,   375,   376,   380,
     382,   386,   390,   396,   399,   402,   407,   410,   413,   417,
     419,   424,   431,   436,   442,   449,   454,   458,   460,   462,
     464,   466,   469,   473,   479,   486,   493,   500,   507,   514,
     522,   529,   536,   543,   550,   559,   568,   569,   572,   575,
     578,   580,   584,   586,   591,   596,   600,   607,   612,   617,
     624,   628,   630,   634,   635,   639,   643,   647,   651,   655,
     659,   663,   667,   671,   675,   681,   685,   686,   688,   689,
     691,   693,   695,   697,   700,   702,   705,   707,   710,   713,
     717,   718,   720,   723,   725,   728,   730,   733,   736,   737,
     740,   745,   747,   752,   758,   760,   765,   770,   776,   777,
     779,   781,   783,   784,   786,   790,   794,   797,   799,   801,
     803,   805,   807,   809,   811,   813,   815,   817,   819,   821,
     823,   825,   827,   829,   831,   833,   835,   837,   839,   841,
     843,   844,   847,   852,   857,   858,   861,   862,   865,   868,
     870,   872,   876,   879,   883,   888,   890,   893,   894,   897,
     901,   904,   907,   910,   911,   914,   918,   921,   925,   928,
     932,   936,   941,   943,   946,   949,   953,   956,   959,   960,
     962,   964,   967,   970,   974,   977,   979,   982,   984,   987,
     990,   993,   996,   997,   999,  1003,  1009,  1012,  1016,  1019,
    1020,  1022,  1025,  1028,  1031,  1034,  1039,  1045,  1051,  1055,
    1057,  1060,  1064,  1068,  1071,  1074,  1078,  1082,  1083,  1086,
    1088,  1092,  1096,  1100,  1101,  1103,  1105,  1109,  1112,  1116,
    1119,  1122,  1124,  1125,  1128,  1133,  1136,  1138,  1142,  1145,
    1150,  1154,  1160,  1162,  1164,  1166,  1168,  1170,  1172,  1174,
    1176,  1178,  1180,  1182,  1184,  1186,  1188,  1190,  1192,  1194,
    1196,  1198,  1200,  1202,  1204,  1206,  1208,  1210,  1212,  1214,
    1216,  1218,  1220,  1222,  1224,  1226,  1228,  1230,  1232,  1234,
    1236,  1238,  1240,  1242,  1244,  1246,  1248,  1250
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     178,     0,    -1,   179,    -1,     1,    -1,   180,    -1,   179,
     180,    -1,   181,    48,    -1,   181,   166,    -1,   181,   166,
      48,    -1,   166,    -1,   166,    48,    -1,    54,   181,   166,
      -1,   182,    -1,   223,    -1,   228,    -1,   224,    -1,   225,
      -1,   226,    -1,   183,    -1,   184,    -1,   293,    -1,   258,
      -1,   200,    -1,   259,    -1,   297,    -1,   298,    -1,   206,
      -1,   207,    -1,   208,    -1,   209,    -1,   210,    -1,   211,
      -1,   212,    -1,   185,    -1,   191,    -1,   227,    -1,   299,
      -1,   246,   196,   195,    -1,   193,   182,    -1,   193,   223,
      -1,   193,   225,    -1,   193,   226,    -1,   193,   224,    -1,
     193,   227,    -1,   232,   182,    -1,   232,   223,    -1,   232,
     225,    -1,   232,   226,    -1,   232,   224,    -1,   232,   227,
      -1,   186,    -1,   192,    -1,    14,   187,    -1,    15,    -1,
      16,    -1,    17,    -1,    18,    -1,    -1,    19,    -1,    62,
      -1,    20,    62,    -1,   182,    -1,   223,    -1,   224,    -1,
     226,    -1,   225,    -1,   299,    -1,   212,    -1,   227,    -1,
     167,    83,    -1,   189,   168,   167,    83,    -1,    28,   304,
      26,    -1,   190,   188,    -1,    29,   304,    -1,    29,   304,
      30,   189,    -1,    21,    -1,    22,    -1,    23,    -1,    24,
      -1,    51,    -1,    25,    -1,    52,    -1,    53,    -1,   194,
      -1,   194,    83,    -1,    84,    -1,    85,    -1,    86,    -1,
      -1,    26,   219,    -1,    -1,   216,    -1,     5,    78,    -1,
       5,    78,   196,    26,   219,    -1,     5,    78,   216,    -1,
     158,    -1,   158,    69,   304,    -1,    12,    79,    -1,    12,
      79,   216,    -1,   197,    -1,   198,    -1,   199,    -1,   204,
      -1,   205,    -1,   201,    -1,   203,    -1,   202,    -1,   156,
      -1,   154,    -1,   155,    -1,   157,   213,    -1,   194,   213,
      -1,   153,    -1,   151,    -1,   151,    26,    83,    -1,   151,
      70,   152,   169,    83,    -1,   151,    26,    83,    70,   152,
     169,    83,    -1,    80,    -1,    81,   169,   115,    -1,    90,
      -1,    91,    -1,    92,    -1,    93,    -1,    94,    -1,    95,
      -1,    13,   213,   170,   214,   171,    -1,   304,    -1,   304,
     172,   304,    -1,   304,   172,   304,   172,   304,    -1,    -1,
     215,    -1,   214,   168,   215,    -1,    83,    -1,   115,    -1,
      98,    -1,   167,    83,    -1,   167,   167,    83,    -1,    47,
      -1,   217,    -1,   216,   217,    -1,   218,    -1,   170,   171,
      -1,   170,   182,   171,    -1,   170,   216,   171,    -1,   301,
      -1,   220,    -1,   182,    -1,    -1,   170,   222,   171,    -1,
      83,    -1,   222,   168,    83,    -1,   249,   302,   300,    -1,
     249,   302,   300,   221,   220,    -1,   251,   219,    -1,   247,
     219,    -1,   248,   257,    26,   219,    -1,   252,   302,    -1,
      12,   229,    -1,   230,   168,   229,    -1,   230,    -1,   167,
      83,   169,   231,    -1,   167,   167,    96,    83,   169,   231,
      -1,    96,    83,   169,   231,    -1,   167,   167,    83,   169,
     231,    -1,   167,   167,    97,    83,   169,   231,    -1,    97,
      83,   169,   231,    -1,    83,   169,   231,    -1,    83,    -1,
     115,    -1,    98,    -1,   233,    -1,   233,   232,    -1,    38,
     234,    39,    -1,    38,   103,   242,   241,    39,    -1,    38,
     100,   169,   245,   241,    39,    -1,    38,   111,   169,   245,
     241,    39,    -1,    38,    99,   169,   245,   241,    39,    -1,
      38,   101,   169,   245,   241,    39,    -1,    38,   102,   169,
     245,   241,    39,    -1,    38,    82,    83,   169,   244,   241,
      39,    -1,    38,   106,   169,   243,   241,    39,    -1,    38,
     107,   169,   243,   241,    39,    -1,    38,   104,   169,   245,
     241,    39,    -1,    38,   105,   169,   245,   241,    39,    -1,
      38,   108,   109,   169,   173,   236,   174,    39,    -1,    38,
     108,   110,   169,   173,   238,   174,    39,    -1,    -1,   234,
     235,    -1,    40,    83,    -1,    41,    83,    -1,    83,    -1,
     237,   168,   236,    -1,   237,    -1,    99,   170,   245,   171,
      -1,   111,   170,   245,   171,    -1,   112,   170,   171,    -1,
     112,   170,   113,   169,   245,   171,    -1,   114,   170,   239,
     171,    -1,    66,   170,   243,   171,    -1,    66,   170,   243,
     175,   243,   171,    -1,   240,   168,   239,    -1,   240,    -1,
      83,   169,   245,    -1,    -1,   241,   168,   242,    -1,    99,
     169,   245,    -1,   100,   169,   245,    -1,   111,   169,   245,
      -1,   101,   169,   245,    -1,   102,   169,   245,    -1,   106,
     169,   243,    -1,   107,   169,   243,    -1,   104,   169,   245,
      -1,   105,   169,   245,    -1,    83,   172,    83,   169,   244,
      -1,    83,   169,   244,    -1,    -1,   245,    -1,    -1,   245,
      -1,    83,    -1,    87,    -1,     5,    -1,    31,   253,    -1,
       8,    -1,    32,   253,    -1,     6,    -1,    33,   253,    -1,
       7,   250,    -1,    34,   253,   250,    -1,    -1,   126,    -1,
     126,    50,    -1,     9,    -1,    35,   253,    -1,    10,    -1,
      36,   253,    -1,   254,    37,    -1,    -1,   255,   254,    -1,
      42,   170,   115,   171,    -1,   115,    -1,    43,   170,   256,
     171,    -1,    62,   170,    83,    83,   171,    -1,    83,    -1,
      83,   170,   115,   171,    -1,    83,   170,    83,   171,    -1,
      83,   170,    83,    83,   171,    -1,    -1,    44,    -1,    45,
      -1,    46,    -1,    -1,    67,    -1,    11,   292,    64,    -1,
      11,   292,    65,    -1,    11,    66,    -1,   263,    -1,   265,
      -1,   266,    -1,   269,    -1,   267,    -1,   271,    -1,   272,
      -1,   273,    -1,   274,    -1,   276,    -1,   277,    -1,   278,
      -1,   279,    -1,   280,    -1,   282,    -1,   283,    -1,   285,
      -1,   286,    -1,   287,    -1,   288,    -1,   289,    -1,   290,
      -1,   291,    -1,    -1,   119,   115,    -1,   119,   115,   168,
     115,    -1,   119,   115,   120,   115,    -1,    -1,   131,    83,
      -1,    -1,   131,    83,    -1,   116,   264,    -1,   117,    -1,
     118,    -1,   118,   115,   260,    -1,   128,   261,    -1,   128,
     129,   261,    -1,   128,   129,   130,   261,    -1,   121,    -1,
     123,   268,    -1,    -1,   124,    83,    -1,   124,   131,    83,
      -1,   124,   126,    -1,   131,    83,    -1,   122,   270,    -1,
      -1,   124,   261,    -1,   124,   115,   261,    -1,   127,   261,
      -1,   127,   115,   261,    -1,   125,   261,    -1,   125,   115,
     261,    -1,   125,   126,   261,    -1,   125,   126,   115,   261,
      -1,   132,    -1,   132,   115,    -1,   133,   261,    -1,   133,
     150,   261,    -1,   134,   261,    -1,   135,   275,    -1,    -1,
      83,    -1,   126,    -1,   126,    83,    -1,   136,   262,    -1,
     136,   138,   262,    -1,   136,   137,    -1,   139,    -1,   139,
      83,    -1,   140,    -1,   140,   141,    -1,   142,   260,    -1,
     142,   115,    -1,   143,   281,    -1,    -1,   115,    -1,   115,
     168,   115,    -1,   115,   168,   115,   168,    83,    -1,   144,
     261,    -1,   144,   145,   261,    -1,   146,   284,    -1,    -1,
     115,    -1,   115,   115,    -1,   147,   148,    -1,   147,   149,
      -1,   147,   150,    -1,   159,    12,    83,   169,    -1,   159,
      12,    83,   169,    83,    -1,   159,    12,    83,   169,   115,
      -1,   160,     6,    83,    -1,   161,    -1,   162,   115,    -1,
     162,   115,   115,    -1,   163,    83,   115,    -1,   163,    83,
      -1,   164,   115,    -1,   164,   118,   115,    -1,   164,   165,
     115,    -1,    -1,    68,   176,    -1,    54,    -1,    55,    56,
     294,    -1,    63,    54,    83,    -1,    63,    55,    83,    -1,
      -1,   295,    -1,   296,    -1,   295,   168,   296,    -1,    57,
      58,    -1,    59,    60,    61,    -1,    88,    83,    -1,    89,
      83,    -1,    83,    -1,    -1,   137,    83,    -1,   137,   170,
      83,   171,    -1,   302,   300,    -1,   304,    -1,   304,   172,
     304,    -1,   304,   304,    -1,   304,   172,   304,   304,    -1,
     304,    69,   304,    -1,   304,   172,   304,    69,   304,    -1,
      55,    -1,    63,    -1,    54,    -1,    56,    -1,    60,    -1,
      65,    -1,    64,    -1,    68,    -1,    67,    -1,    66,    -1,
     117,    -1,   118,    -1,   120,    -1,   124,    -1,   125,    -1,
     127,    -1,   129,    -1,   130,    -1,   141,    -1,   145,    -1,
     149,    -1,   150,    -1,   165,    -1,    99,    -1,   100,    -1,
     101,    -1,   102,    -1,   152,    -1,   103,    -1,   104,    -1,
     105,    -1,   106,    -1,   107,    -1,   108,    -1,   109,    -1,
     110,    -1,   111,    -1,   113,    -1,   112,    -1,   114,    -1,
      62,    -1,    51,    -1,    52,    -1,    53,    -1,    83,    -1,
     303,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   336,   336,   337,   339,   340,   342,   343,   344,   345,
     346,   347,   349,   350,   351,   352,   353,   354,   355,   356,
     357,   358,   359,   360,   361,   362,   363,   364,   365,   366,
     367,   368,   369,   370,   371,   372,   373,   375,   377,   378,
     379,   380,   381,   382,   384,   385,   386,   387,   388,   389,
     391,   392,   394,   395,   396,   397,   398,   400,   401,   402,
     403,   405,   406,   407,   408,   409,   410,   411,   412,   414,
     421,   429,   435,   438,   443,   449,   450,   451,   452,   453,
     454,   455,   456,   458,   459,   461,   462,   463,   465,   466,
     468,   469,   471,   472,   473,   475,   476,   478,   479,   481,
     482,   483,   484,   485,   486,   488,   489,   490,   491,   492,
     493,   494,   500,   502,   503,   508,   513,   520,   523,   529,
     530,   531,   532,   533,   534,   537,   539,   543,   548,   556,
     559,   564,   569,   574,   579,   584,   589,   594,   602,   603,
     605,   607,   608,   609,   611,   612,   614,   616,   617,   619,
     620,   622,   626,   627,   628,   629,   630,   635,   637,   638,
     640,   644,   648,   652,   656,   660,   664,   668,   673,   678,
     684,   685,   687,   688,   689,   690,   691,   692,   693,   694,
     700,   701,   702,   703,   704,   705,   707,   708,   710,   711,
     712,   714,   715,   717,   722,   727,   728,   729,   731,   732,
     734,   735,   737,   745,   746,   748,   749,   750,   751,   752,
     753,   754,   755,   756,   757,   763,   765,   766,   768,   769,
     771,   772,   774,   775,   776,   777,   778,   779,   780,   781,
     783,   784,   785,   787,   788,   789,   790,   791,   792,   793,
     795,   796,   797,   798,   803,   804,   805,   806,   808,   809,
     810,   811,   813,   814,   817,   818,   819,   822,   823,   824,
     825,   826,   827,   828,   829,   830,   831,   832,   833,   834,
     835,   836,   837,   838,   839,   840,   841,   842,   843,   844,
     849,   851,   855,   860,   868,   869,   873,   874,   877,   879,
     880,   881,   885,   886,   887,   891,   893,   895,   896,   897,
     898,   899,   902,   904,   905,   906,   907,   908,   909,   910,
     911,   912,   916,   917,   921,   922,   927,   930,   932,   933,
     934,   935,   939,   940,   941,   945,   946,   950,   951,   955,
     956,   959,   961,   962,   963,   964,   968,   969,   972,   974,
     975,   976,   980,   981,   982,   986,   987,   988,   992,   996,
    1000,  1001,  1005,  1006,  1010,  1011,  1012,  1015,  1016,  1019,
    1023,  1024,  1025,  1027,  1028,  1030,  1031,  1034,  1035,  1038,
    1044,  1047,  1049,  1050,  1051,  1053,  1058,  1061,  1065,  1069,
    1074,  1078,  1084,  1085,  1086,  1087,  1088,  1089,  1090,  1091,
    1092,  1093,  1094,  1095,  1096,  1097,  1098,  1099,  1100,  1101,
    1102,  1103,  1104,  1105,  1106,  1107,  1108,  1109,  1110,  1111,
    1112,  1113,  1114,  1115,  1116,  1117,  1118,  1119,  1120,  1121,
    1122,  1123,  1124,  1125,  1126,  1127,  1129,  1130
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
  "COMMENT", "FROM", "DUAL", "PREPARE", "EXECUTE", "USING",
  "SELECT_HINT_BEGIN", "UPDATE_HINT_BEGIN", "DELETE_HINT_BEGIN",
  "INSERT_HINT_BEGIN", "REPLACE_HINT_BEGIN", "MERGE_HINT_BEGIN",
  "HINT_END", "COMMENT_BEGIN", "COMMENT_END", "ROUTE_TABLE",
  "ROUTE_PART_KEY", "QUERY_TIMEOUT", "READ_CONSISTENCY", "WEAK", "STRONG",
  "FROZEN", "PLACE_HOLDER", "END_P", "ERROR", "WHEN", "FLASHBACK", "AUDIT",
  "NOAUDIT", "BEGI", "START", "TRANSACTION", "READ", "ONLY", "WITH",
  "CONSISTENT", "SNAPSHOT", "INDEX", "XA", "WARNINGS", "ERRORS", "TRACE",
  "QUICK", "COUNT", "AS", "WHERE", "VALUES", "ORDER", "GROUP", "HAVING",
  "INTO", "UNION", "FOR", "TX_READ_ONLY", "AUTOCOMMIT_0",
  "SELECT_OBPROXY_ROUTE_ADDR", "SET_OBPROXY_ROUTE_ADDR", "NAME_OB_DOT",
  "NAME_OB", "EXPLAIN", "DESC", "DESCRIBE", "NAME_STR", "USE", "HELP",
  "SET_NAMES", "SET_CHARSET", "SET_PASSWORD", "SET_DEFAULT",
  "SET_OB_READ_CONSISTENCY", "SET_TX_READ_ONLY", "GLOBAL", "SESSION",
  "NUMBER_VAL", "GROUP_ID", "TABLE_ID", "ELASTIC_ID", "TESTLOAD",
  "ODP_COMMENT", "TNT_ID", "DISASTER_STATUS", "TRACE_ID", "RPC_ID",
  "DBP_COMMENT", "ROUTE_TAG", "SYS_TAG", "TABLE_NAME", "SCAN_ALL",
  "PARALL", "SHARD_KEY", "INT_NUM", "SHOW_PROXYNET", "THREAD",
  "CONNECTION", "LIMIT", "OFFSET", "SHOW_PROCESSLIST", "SHOW_PROXYSESSION",
  "SHOW_GLOBALSESSION", "ATTRIBUTE", "VARIABLES", "ALL", "STAT",
  "SHOW_PROXYCONFIG", "DIFF", "USER", "LIKE", "SHOW_PROXYSM",
  "SHOW_PROXYCLUSTER", "SHOW_PROXYRESOURCE", "SHOW_PROXYCONGESTION",
  "SHOW_PROXYROUTE", "PARTITION", "ROUTINE", "SHOW_PROXYVIP",
  "SHOW_PROXYMEMORY", "OBJPOOL", "SHOW_SQLAUDIT", "SHOW_WARNLOG",
  "SHOW_PROXYSTAT", "REFRESH", "SHOW_PROXYTRACE", "SHOW_PROXYINFO",
  "BINARY", "UPGRADE", "IDC", "SHOW_TOPOLOGY", "GROUP_NAME",
  "SHOW_DB_VERSION", "SHOW_DATABASES", "SHOW_TABLES", "SELECT_DATABASE",
  "SHOW_CREATE_TABLE", "SELECT_PROXY_VERSION", "ALTER_PROXYCONFIG",
  "ALTER_PROXYRESOURCE", "PING_PROXY", "KILL_PROXYSESSION",
  "KILL_GLOBALSESSION", "KILL", "QUERY", "';'", "'@'", "','", "'='", "'('",
  "')'", "'.'", "'{'", "'}'", "'#'", "'*'", "$accept", "root", "sql_stmts",
  "sql_stmt", "stmt", "select_stmt", "explain_stmt", "comment_stmt",
  "ddl_stmt", "mysql_ddl_stmt", "create_dll_expr", "text_ps_from_stmt",
  "text_ps_using_var_list", "text_ps_prepare_stmt", "text_ps_stmt",
  "oracle_ddl_stmt", "explain_or_desc_stmt", "explain_or_desc", "opt_from",
  "select_expr_list", "select_tx_read_only_stmt",
  "select_proxy_version_stmt", "set_autocommit_0_stmt", "hooked_stmt",
  "shard_special_stmt", "show_db_version_stmt", "show_es_id_stmt",
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
  "opt_limit", "opt_like", "opt_large_like", "show_proxynet",
  "opt_show_net", "show_proxyconfig", "show_processlist",
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
     415,   416,   417,   418,   419,   420,    59,    64,    44,    61,
      40,    41,    46,   123,   125,    35,    42
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   177,   178,   178,   179,   179,   180,   180,   180,   180,
     180,   180,   181,   181,   181,   181,   181,   181,   181,   181,
     181,   181,   181,   181,   181,   181,   181,   181,   181,   181,
     181,   181,   181,   181,   181,   181,   181,   182,   183,   183,
     183,   183,   183,   183,   184,   184,   184,   184,   184,   184,
     185,   185,   186,   186,   186,   186,   186,   187,   187,   187,
     187,   188,   188,   188,   188,   188,   188,   188,   188,   189,
     189,   190,   191,   191,   191,   192,   192,   192,   192,   192,
     192,   192,   192,   193,   193,   194,   194,   194,   195,   195,
     196,   196,   197,   197,   197,   198,   198,   199,   199,   200,
     200,   200,   200,   200,   200,   201,   201,   201,   201,   201,
     201,   201,   202,   203,   203,   203,   203,   204,   205,   206,
     207,   208,   209,   210,   211,   212,   213,   213,   213,   214,
     214,   214,   215,   215,   215,   215,   215,   215,   216,   216,
     217,   218,   218,   218,   219,   219,   220,   221,   221,   222,
     222,   223,   223,   224,   225,   226,   227,   228,   229,   229,
     230,   230,   230,   230,   230,   230,   230,   231,   231,   231,
     232,   232,   233,   233,   233,   233,   233,   233,   233,   233,
     233,   233,   233,   233,   233,   233,   234,   234,   235,   235,
     235,   236,   236,   237,   237,   237,   237,   237,   238,   238,
     239,   239,   240,   241,   241,   242,   242,   242,   242,   242,
     242,   242,   242,   242,   242,   242,   243,   243,   244,   244,
     245,   245,   246,   246,   247,   247,   248,   248,   249,   249,
     250,   250,   250,   251,   251,   252,   252,   253,   254,   254,
     255,   255,   255,   255,   255,   255,   255,   255,   256,   256,
     256,   256,   257,   257,   258,   258,   258,   259,   259,   259,
     259,   259,   259,   259,   259,   259,   259,   259,   259,   259,
     259,   259,   259,   259,   259,   259,   259,   259,   259,   259,
     260,   260,   260,   260,   261,   261,   262,   262,   263,   264,
     264,   264,   265,   265,   265,   266,   267,   268,   268,   268,
     268,   268,   269,   270,   270,   270,   270,   270,   270,   270,
     270,   270,   271,   271,   272,   272,   273,   274,   275,   275,
     275,   275,   276,   276,   276,   277,   277,   278,   278,   279,
     279,   280,   281,   281,   281,   281,   282,   282,   283,   284,
     284,   284,   285,   285,   285,   286,   286,   286,   287,   288,
     289,   289,   290,   290,   291,   291,   291,   292,   292,   293,
     293,   293,   293,   294,   294,   295,   295,   296,   296,   297,
     298,   299,   300,   300,   300,   301,   302,   302,   302,   302,
     302,   302,   303,   303,   303,   303,   303,   303,   303,   303,
     303,   303,   303,   303,   303,   303,   303,   303,   303,   303,
     303,   303,   303,   303,   303,   303,   303,   303,   303,   303,
     303,   303,   303,   303,   303,   303,   303,   303,   303,   303,
     303,   303,   303,   303,   303,   303,   304,   304
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     1,     1,     1,     2,     2,     2,     3,     1,
       2,     3,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     3,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       1,     1,     2,     1,     1,     1,     1,     0,     1,     1,
       2,     1,     1,     1,     1,     1,     1,     1,     1,     2,
       4,     3,     2,     2,     4,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     2,     1,     1,     1,     0,     2,
       0,     1,     2,     5,     3,     1,     3,     2,     3,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       2,     2,     1,     1,     3,     5,     7,     1,     3,     1,
       1,     1,     1,     1,     1,     5,     1,     3,     5,     0,
       1,     3,     1,     1,     1,     2,     3,     1,     1,     2,
       1,     2,     3,     3,     1,     1,     1,     0,     3,     1,
       3,     3,     5,     2,     2,     4,     2,     2,     3,     1,
       4,     6,     4,     5,     6,     4,     3,     1,     1,     1,
       1,     2,     3,     5,     6,     6,     6,     6,     6,     7,
       6,     6,     6,     6,     8,     8,     0,     2,     2,     2,
       1,     3,     1,     4,     4,     3,     6,     4,     4,     6,
       3,     1,     3,     0,     3,     3,     3,     3,     3,     3,
       3,     3,     3,     3,     5,     3,     0,     1,     0,     1,
       1,     1,     1,     2,     1,     2,     1,     2,     2,     3,
       0,     1,     2,     1,     2,     1,     2,     2,     0,     2,
       4,     1,     4,     5,     1,     4,     4,     5,     0,     1,
       1,     1,     0,     1,     3,     3,     2,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       0,     2,     4,     4,     0,     2,     0,     2,     2,     1,
       1,     3,     2,     3,     4,     1,     2,     0,     2,     3,
       2,     2,     2,     0,     2,     3,     2,     3,     2,     3,
       3,     4,     1,     2,     2,     3,     2,     2,     0,     1,
       1,     2,     2,     3,     2,     1,     2,     1,     2,     2,
       2,     2,     0,     1,     3,     5,     2,     3,     2,     0,
       1,     2,     2,     2,     2,     4,     5,     5,     3,     1,
       2,     3,     3,     2,     2,     3,     3,     0,     2,     1,
       3,     3,     3,     0,     1,     1,     3,     2,     3,     2,
       2,     1,     0,     2,     4,     2,     1,     3,     2,     4,
       3,     5,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
       0,     3,   222,   226,   230,   224,   233,   235,   357,     0,
       0,    57,    53,    54,    55,    56,    75,    76,    77,    78,
      80,     0,     0,   238,   238,   238,   238,   238,   238,   186,
      79,    81,    82,   359,     0,     0,   117,     0,   371,    85,
      86,    87,     0,     0,   119,   120,   121,   122,   123,   124,
       0,   295,   303,   297,   284,   312,   284,   284,   318,   286,
     325,   327,   280,   332,   284,   339,     0,   113,   112,   108,
     109,   107,     0,    95,     0,     0,   349,     0,     0,     0,
       9,     0,     2,     4,     0,    12,    18,    19,    33,    50,
       0,    34,    51,     0,    83,    99,   100,   101,    22,   104,
     106,   105,   102,   103,    26,    27,    28,    29,    30,    31,
      32,    13,    15,    16,    17,    35,    14,     0,   170,    90,
       0,   252,     0,     0,     0,    21,    23,   257,   258,   259,
     261,   260,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
      20,    24,    25,    36,    92,   231,   228,   256,     0,     0,
      97,     0,     0,     0,     0,   157,   159,   423,   424,   425,
     384,   382,   385,   386,   422,   383,   388,   387,   391,   390,
     389,   426,   405,   406,   407,   408,   410,   411,   412,   413,
     414,   415,   416,   417,   418,   420,   419,   421,   392,   393,
     394,   395,   396,   397,   398,   399,   400,   401,   402,   403,
     409,   404,     0,   427,   126,    58,     0,    59,    52,     0,
      73,     0,     0,     0,   244,   241,   223,     0,   238,   225,
     227,   230,   234,   236,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   359,     0,   363,
       0,     0,     0,   369,   370,   289,   290,   288,   284,   284,
     284,   302,     0,     0,   296,   284,     0,   292,   313,   284,
     314,   316,   319,   320,   317,     0,   324,   286,   322,   326,
     328,   330,     0,   329,   333,   331,   284,   336,   340,   338,
     342,   343,   344,     0,     0,   110,     0,     0,     0,   350,
     353,   354,     0,     0,    10,     1,     5,     6,     7,   222,
      61,    72,    67,    62,    63,    65,    64,    68,    66,    38,
      39,    42,    40,    41,    43,    84,   111,    44,    45,    48,
      46,    47,    49,   171,     0,    88,    91,   138,   140,   146,
     154,   145,   144,   372,   376,   253,     0,   372,   153,   156,
       0,    94,   232,   358,   254,   255,    98,     0,     0,     0,
       0,     0,     0,   129,     0,    60,    71,     0,     0,   248,
       0,     0,   237,   239,   229,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     203,     0,     0,   216,   216,     0,     0,     0,   172,     0,
       0,   190,   187,    11,     0,     0,   360,   364,   365,   361,
     362,   118,   280,   284,   304,   284,   284,   308,   284,   306,
     298,   300,     0,   301,   284,   293,   285,   315,   321,   287,
     323,   281,     0,   337,   341,   114,     0,    96,     0,   348,
     351,   352,   355,   356,     8,   141,     0,     0,     0,    37,
     139,     0,   375,     0,     0,   378,     0,   147,     0,   167,
     169,   168,   166,     0,     0,     0,     0,     0,     0,   158,
     137,   132,   134,   133,     0,     0,   130,   127,     0,    74,
       0,   249,   250,   251,     0,     0,     0,     0,   218,   220,
     221,   203,   203,   203,   203,   218,     0,     0,     0,     0,
       0,     0,     0,   216,   216,     0,     0,   203,   203,   203,
     217,   203,     0,     0,   203,   188,   189,   367,     0,     0,
     291,   305,   309,   284,   310,   307,   299,   294,     0,     0,
     334,     0,     0,   345,   142,   143,    89,   373,     0,   380,
     377,   155,     0,     0,    93,   162,   165,   160,     0,     0,
       0,   135,     0,     0,   125,     0,    69,     0,   240,   242,
       0,     0,   246,   245,   203,   219,     0,     0,     0,     0,
     215,     0,   205,   206,   208,   209,   212,   213,   210,   211,
     207,   173,     0,     0,     0,     0,     0,     0,     0,     0,
     368,   366,   311,   283,   282,     0,     0,   115,   346,   347,
       0,     0,   379,   149,     0,   152,   163,     0,     0,   136,
     131,   128,     0,   243,   247,     0,   176,   174,   177,   178,
     218,   204,   182,   183,   180,   181,     0,     0,     0,     0,
       0,   192,     0,     0,   175,   335,     0,   374,   381,     0,
     148,   161,   164,    70,   179,   214,     0,     0,     0,     0,
       0,     0,   216,     0,   116,   150,     0,     0,     0,   195,
       0,     0,   201,   184,   191,     0,   185,   193,   194,     0,
       0,   197,     0,   198,   216,     0,   202,   200,     0,   196,
     199
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    81,    82,    83,    84,   339,    86,    87,    88,    89,
     218,   311,   479,    90,    91,    92,    93,    94,   449,   335,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   212,   475,   476,   336,
     337,   338,   340,   341,   543,   604,   111,   112,   113,   114,
     115,   116,   165,   166,   462,   117,   118,   246,   402,   630,
     631,   633,   661,   662,   506,   390,   509,   564,   510,   119,
     120,   121,   122,   156,   123,   124,   226,   227,   228,   484,
     346,   125,   126,   283,   267,   278,   127,   257,   128,   129,
     130,   264,   131,   261,   132,   133,   134,   135,   274,   136,
     137,   138,   139,   140,   285,   141,   142,   289,   143,   144,
     145,   146,   147,   148,   149,   159,   150,   406,   407,   408,
     151,   152,   153,   452,   342,   343,   213,   344
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -475
static const yytype_int16 yypact[] =
{
     365,  -475,    28,  -475,   -61,  -475,  -475,  -475,   145,   -39,
    1154,    35,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,
    -475,  1154,  1154,    30,    30,    30,    30,    30,    30,   139,
    -475,  -475,  -475,   821,    15,   201,  -475,   -83,  -475,  -475,
    -475,  -475,    41,    57,  -475,  -475,  -475,  -475,  -475,  -475,
     191,  -475,   170,    55,   135,    42,   -62,    46,   -33,   -11,
      98,    87,    -7,   157,   -63,   162,   150,    26,  -475,  -475,
    -475,  -475,  1154,   241,   303,   312,  -475,   204,   237,   -51,
     273,   322,   541,  -475,     8,  -475,  -475,  -475,  -475,  -475,
     184,  -475,  -475,   227,  1258,  -475,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,  -475,  -475,  -475,   227,   285,   154,
     660,   258,  1154,   660,  1154,  -475,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,   -18,   276,  -475,  -475,   151,    16,
     154,   160,   247,   248,   -36,  -475,   164,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,
    -475,  -475,   163,  -475,   165,  -475,   278,  -475,  -475,   309,
     311,   172,   173,   174,   175,  -475,  -475,   310,    30,  -475,
    -475,   -61,  -475,  -475,   263,   179,   181,   182,   186,   169,
     187,   190,   193,   195,   202,   196,    36,  -475,   218,   244,
     266,   269,   238,  -475,  -475,  -475,   242,  -475,   -30,    52,
     -24,  -475,   -22,   284,  -475,   183,   302,  -475,  -475,    46,
    -475,  -475,  -475,   308,  -475,   319,  -475,   261,  -475,  -475,
    -475,  -475,   253,  -475,   236,  -475,    46,  -475,   280,  -475,
    -475,  -475,  -475,   323,   255,  -475,  1154,   325,   326,   290,
     295,  -475,   296,   297,  -475,  -475,  -475,  -475,   366,  -475,
    -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,  -475,   -25,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,     1,   387,   154,  -475,  -475,  -475,
    -475,  -475,  -475,   286,   935,  -475,   389,   286,  -475,  -475,
     395,    -6,  -475,  -475,  -475,  -475,   154,    60,   256,   257,
     260,    99,   -37,    -4,  1154,  -475,  -475,   265,   307,   104,
     341,    -5,  -475,  -475,  -475,   262,    93,    93,    93,    93,
      79,   267,   268,   270,   271,   272,   283,   292,   293,   294,
    -475,    93,    93,    93,    93,   298,   300,    93,  -475,   344,
     347,  -475,  -475,  -475,   375,   374,  -475,   274,  -475,  -475,
    -475,  -475,   316,    46,  -475,    46,   -16,  -475,    46,  -475,
    -475,  -475,   355,  -475,    46,  -475,  -475,  -475,  -475,  -475,
    -475,   -50,   332,  -475,  -475,   394,   301,  -475,   304,  -475,
    -475,  -475,  -475,  -475,  -475,  -475,   305,   146,   660,  -475,
    -475,   -41,  -475,  1154,  1154,  -475,   660,     3,   660,  -475,
    -475,  -475,  -475,    60,    60,    60,   306,   382,   383,  -475,
    -475,  -475,  -475,  -475,   -34,   114,  -475,   299,   391,   314,
     313,  -475,  -475,  -475,   318,   396,   -48,   320,    93,  -475,
    -475,  -475,  -475,  -475,  -475,    93,   397,    93,    93,    93,
      93,    93,    93,    93,    93,    93,   -17,  -475,  -475,  -475,
    -475,  -475,   317,   321,  -475,  -475,  -475,  -475,   411,   244,
    -475,  -475,  -475,    46,  -475,  -475,  -475,  -475,   362,   363,
     315,   333,   409,     6,  -475,  -475,  -475,  -475,   412,  -475,
    1050,  -475,   413,    69,  -475,  -475,  -475,  -475,    60,   334,
     337,  -475,   419,    -4,  -475,  1154,  -475,   343,  -475,  -475,
     342,   346,  -475,  -475,  -475,  -475,   -15,   -14,   -13,   -12,
    -475,   345,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,
    -475,  -475,   169,    -9,    -8,    -3,    -2,    88,   449,     0,
    -475,  -475,  -475,  -475,  -475,   450,   367,  -475,  -475,  -475,
     361,  1154,  -475,  -475,   125,  -475,  -475,    60,    60,  -475,
    -475,  -475,   451,  -475,  -475,     2,  -475,  -475,  -475,  -475,
      93,  -475,  -475,  -475,  -475,  -475,   368,   369,   370,   371,
     386,   376,   372,   393,  -475,  -475,   452,  -475,  -475,   454,
    -475,  -475,  -475,  -475,  -475,  -475,    93,    93,   -68,   460,
     522,    88,    93,   529,  -475,  -475,   400,   407,   414,  -475,
     415,   410,   417,  -475,  -475,    78,  -475,  -475,  -475,    93,
      93,  -475,   460,  -475,    93,   416,  -475,  -475,   418,  -475,
    -475
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -475,  -475,  -475,   498,   549,     5,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,   432,
    -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,  -475,   500,    11,  -475,    44,  -150,
    -303,  -475,  -120,    45,  -475,  -475,   -27,    95,   108,   137,
     188,  -475,   239,  -475,  -446,   480,  -475,  -475,  -475,   -52,
    -475,  -475,   -72,  -475,  -285,    20,  -387,  -474,  -363,  -475,
    -475,  -475,  -475,   377,  -475,  -475,   264,   378,  -475,  -475,
    -475,  -475,  -475,   197,   -55,   328,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,
    -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,  -475,    84,
    -475,  -475,   517,   281,  -475,   180,  -475,   -10
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -427
static const yytype_int16 yytable[] =
{
     214,   270,   271,   348,   351,    85,   309,   511,   -90,   287,
     356,   219,   220,   491,   492,   493,   494,   545,   546,   547,
     -91,   570,   581,  -426,   616,   617,   618,   619,   507,   508,
     622,   623,    23,   450,   514,   561,   624,   625,    85,   634,
     160,   644,   537,   470,   161,   658,   161,   360,   450,   551,
     272,  -151,   293,   450,   215,   216,   307,   162,   163,   162,
     163,   420,   214,   313,   301,   155,   320,   302,   266,   266,
     528,   249,   221,   222,   309,   398,   399,   400,   486,   471,
     354,   355,   286,   295,   214,   413,   252,    85,   269,   598,
     328,   418,   223,   273,   472,   310,   294,   217,   319,   523,
      23,   266,   606,   659,   421,   326,   154,   266,   281,   422,
     487,   473,   282,   224,   303,   266,   578,   579,   529,   401,
     275,   599,   327,   562,   253,   565,   276,   277,   164,   538,
     164,   361,   565,   552,   572,   573,   574,   575,   576,   577,
     254,  -426,   580,   459,   450,   225,   645,  -426,   481,   482,
     483,   582,   334,   582,   582,   582,   582,   268,   460,   582,
     582,   641,   642,   474,   334,   582,   582,   415,   582,  -151,
     582,   334,   445,   542,   308,   461,   489,   266,   416,   262,
     490,   279,   466,   266,   447,   314,   263,   626,   321,   309,
       3,     4,     5,     6,     7,   467,   468,    10,   315,   627,
     628,   322,   629,   414,   417,   419,   566,   567,   568,   569,
     425,   157,   329,   158,   427,    23,    24,    25,    26,    27,
      28,   234,   583,   584,   585,   330,   586,   316,   280,   589,
     323,   433,   309,     3,     4,     5,     6,     7,   235,   236,
     237,   238,   239,   240,   241,   242,   243,   244,   495,   673,
     245,   496,   380,   674,   331,   250,   251,   565,    23,    24,
      25,    26,    27,    28,   265,   665,   266,    38,   381,   382,
     383,   384,   284,   385,   386,   387,   388,   288,   317,   615,
     389,   324,   553,   656,   657,   554,   437,   678,   229,   230,
     231,   232,   233,   639,   258,   259,   640,   260,   290,   291,
     292,   404,   347,   405,   349,   332,   675,   676,   255,   256,
     296,   395,   396,   424,   266,   297,   334,   535,   298,   299,
     300,   304,   305,    29,   334,   345,   352,   353,   536,   357,
     358,   359,   362,   363,   455,   366,   541,   364,   544,   446,
     365,   367,   368,   369,   370,   371,   375,   372,   376,   409,
     377,   378,   410,   411,   477,   379,   391,   412,   521,   392,
     522,   524,   393,   525,   394,   397,     1,   423,   431,   527,
       2,     3,     4,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,   403,   426,    16,    17,    18,    19,
      20,   428,   275,    21,    22,   434,    23,    24,    25,    26,
      27,    28,   429,    29,   432,   440,   435,   436,   438,   439,
     441,   442,   443,   448,   444,   456,    30,    31,    32,    33,
      34,   458,   480,   451,   485,   463,   464,   515,    35,   465,
     516,   488,   478,   517,   518,   282,   497,   498,   526,   499,
     500,   501,   519,   539,   540,    36,    37,   530,    38,    39,
      40,    41,   502,    42,    43,    44,    45,    46,    47,    48,
      49,   503,   504,   505,   531,   549,   550,   512,   592,   513,
     532,   555,   590,   533,   556,   548,   534,   593,   594,   560,
     571,    50,   557,   595,   558,   596,    51,    52,    53,   559,
     587,   563,   597,    54,   588,   600,   603,    55,    56,    57,
      58,    59,   609,   607,    60,    61,   608,    62,    63,    64,
     612,    65,    66,   613,   620,   632,    67,   614,    68,    69,
      70,    71,    72,    73,    74,    75,    76,    77,    78,    79,
     602,    80,   637,   635,   643,   654,   636,   655,   646,   647,
     648,   649,   652,   660,   651,   611,     2,     3,     4,     5,
       6,     7,     8,     9,    10,    11,    12,    13,    14,    15,
     650,   663,    16,    17,    18,    19,    20,   653,   666,    21,
      22,   667,    23,    24,    25,    26,    27,    28,   668,    29,
     306,   671,   248,   669,   670,   672,   350,   679,   605,   680,
     312,   638,    30,    31,    32,    33,    34,   610,   333,   664,
     677,   469,   621,   591,    35,   430,   373,   318,   374,   520,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    36,    37,     0,    38,    39,    40,    41,   457,    42,
      43,    44,    45,    46,    47,    48,    49,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    50,     0,     0,
       0,     0,    51,    52,    53,   309,     0,     0,     0,    54,
       0,     0,     0,    55,    56,    57,    58,    59,     0,     0,
      60,    61,     0,    62,    63,    64,     0,    65,    66,     0,
       0,    23,    67,     0,    68,    69,    70,    71,    72,    73,
      74,    75,    76,    77,    78,    79,     0,    80,     0,     0,
       0,   167,   168,   169,   170,   171,   172,     0,     0,     0,
     173,     0,   174,   175,   176,   177,   178,   179,   180,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   181,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   182,
     183,   184,   185,   186,   187,   188,   189,   190,   191,   192,
     193,   194,   195,   196,   197,     0,     0,   198,   199,     0,
     200,     0,     0,     0,   201,   202,     0,   203,     0,   204,
     205,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   206,     0,     0,     0,   207,     0,     0,     0,   208,
     209,     0,   210,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   211,     2,     3,     4,     5,
       6,     7,     8,     9,    10,    11,    12,    13,    14,    15,
       0,     0,    16,    17,    18,    19,    20,     0,     0,    21,
      22,     0,    23,    24,    25,    26,    27,    28,     0,    29,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    30,    31,    32,   247,    34,     0,     0,     0,
       0,     0,     0,     0,    35,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    36,    37,     0,    38,    39,    40,    41,     0,    42,
      43,    44,    45,    46,    47,    48,    49,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    50,     0,     0,
       0,     0,    51,    52,    53,     0,     0,     0,     0,    54,
       0,     0,     0,    55,    56,    57,    58,    59,     0,     0,
      60,    61,     0,    62,    63,    64,     0,    65,    66,     0,
       0,     0,    67,     0,    68,    69,    70,    71,    72,    73,
      74,    75,    76,    77,    78,    79,   167,   168,   169,   170,
     171,   172,     0,     0,     0,   173,     0,   174,   175,   176,
     177,   178,   179,   180,   453,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   181,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   182,   183,   184,   185,   186,   187,
     188,   189,   190,   191,   192,   193,   194,   195,   196,   197,
       0,     0,   198,   199,     0,   200,     0,     0,     0,   201,
     202,     0,   203,     0,   204,   205,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   206,     0,     0,     0,
     207,     0,     0,     0,   208,   209,     0,   210,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     211,   167,   168,   169,   170,   171,   172,   454,     0,     0,
     173,     0,   174,   175,   176,   177,   178,   179,   180,   601,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   181,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   182,
     183,   184,   185,   186,   187,   188,   189,   190,   191,   192,
     193,   194,   195,   196,   197,     0,     0,   198,   199,     0,
     200,     0,     0,     0,   201,   202,     0,   203,     0,   204,
     205,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   206,     0,     0,     0,   207,     0,     0,     0,   208,
     209,     0,   210,     0,     0,   167,   168,   169,   170,   171,
     172,     0,     0,     0,   173,   211,   174,   175,   176,   177,
     178,   179,   180,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   181,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   182,   183,   184,   185,   186,   187,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,     0,
       0,   198,   199,     0,   200,     0,     0,     0,   201,   202,
       0,   203,     0,   204,   205,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   206,     0,     0,     0,   207,
       0,     0,     0,   208,   209,     0,   210,     0,     0,   167,
     168,   169,   170,   171,   172,     0,     0,     0,   173,   211,
     174,   175,   176,   177,   178,   179,   180,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   325,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   182,   183,   184,
     185,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,     0,     0,   198,   199,     0,   200,     0,
       0,     0,   201,   202,     0,   203,     0,   204,   205,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   206,
       0,     0,     0,   207,     0,     0,     0,   208,   209,     0,
     210,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   211
};

static const yytype_int16 yycheck[] =
{
      10,    56,    57,   123,   154,     0,     5,   394,    26,    64,
     160,    21,    22,   376,   377,   378,   379,   463,   464,   465,
      26,   495,    39,    48,    39,    39,    39,    39,   391,   392,
      39,    39,    31,   336,   397,    83,    39,    39,    33,    39,
      79,    39,    83,    47,    83,   113,    83,    83,   351,    83,
      83,    48,    26,   356,    19,    20,    48,    96,    97,    96,
      97,    83,    72,    90,   115,   126,    93,   118,   131,   131,
     120,    56,    42,    43,     5,    39,    40,    41,    83,    83,
      64,    65,   145,    72,    94,   115,   169,    82,   150,    83,
     117,   115,    62,   126,    98,    90,    70,    62,    93,   115,
      31,   131,   548,   171,   126,    94,    78,   131,   115,   131,
     115,   115,   119,    83,   165,   131,   503,   504,   168,    83,
     131,   115,   117,   171,    83,   488,   137,   138,   167,   170,
     167,   167,   495,   167,   497,   498,   499,   500,   501,   502,
      83,   166,   505,    83,   447,   115,   620,   172,    44,    45,
      46,   168,   170,   168,   168,   168,   168,   115,    98,   168,
     168,   607,   608,   167,   170,   168,   168,   115,   168,   166,
     168,   170,   171,   170,   166,   115,    83,   131,   126,   124,
      87,    83,    83,   131,   334,    90,   131,    99,    93,     5,
       6,     7,     8,     9,    10,    96,    97,    13,    90,   111,
     112,    93,   114,   258,   259,   260,   491,   492,   493,   494,
     265,    66,   117,    68,   269,    31,    32,    33,    34,    35,
      36,    82,   507,   508,   509,   117,   511,    90,   141,   514,
      93,   286,     5,     6,     7,     8,     9,    10,    99,   100,
     101,   102,   103,   104,   105,   106,   107,   108,   169,   171,
     111,   172,    83,   175,   117,    54,    55,   620,    31,    32,
      33,    34,    35,    36,   129,   652,   131,    83,    99,   100,
     101,   102,   115,   104,   105,   106,   107,   115,    90,   564,
     111,    93,   168,   646,   647,   171,   296,   674,    24,    25,
      26,    27,    28,   168,   124,   125,   171,   127,   148,   149,
     150,    57,   122,    59,   124,   117,   669,   670,   117,   118,
      69,   109,   110,   130,   131,    12,   170,   171,     6,   115,
      83,    48,     0,    38,   170,    67,    50,   176,   448,   169,
      83,    83,   168,   170,   344,    26,   456,   172,   458,   334,
      62,    30,   170,   170,   170,   170,    83,    37,   169,    83,
     169,   169,    83,   115,   364,   169,   169,   115,   413,   169,
     415,   416,   169,   418,   169,   169,     1,    83,   115,   424,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,   166,    83,    21,    22,    23,    24,
      25,    83,   131,    28,    29,   115,    31,    32,    33,    34,
      35,    36,    83,    38,   168,   115,    83,   152,    83,    83,
     115,   115,   115,    26,    48,    26,    51,    52,    53,    54,
      55,    26,   115,   137,    83,   169,   169,    83,    63,   169,
      83,   169,   167,    58,    60,   119,   169,   169,    83,   169,
     169,   169,   168,   453,   454,    80,    81,   115,    83,    84,
      85,    86,   169,    88,    89,    90,    91,    92,    93,    94,
      95,   169,   169,   169,    70,    83,    83,   169,   523,   169,
     169,   172,    61,   169,    83,   169,   171,   115,   115,    83,
      83,   116,   168,   168,   171,   152,   121,   122,   123,   171,
     173,   171,    83,   128,   173,    83,    83,   132,   133,   134,
     135,   136,    83,   169,   139,   140,   169,   142,   143,   144,
     167,   146,   147,   171,   169,    66,   151,   171,   153,   154,
     155,   156,   157,   158,   159,   160,   161,   162,   163,   164,
     540,   166,   171,    83,    83,    83,   169,    83,   170,   170,
     170,   170,   170,    83,   168,   555,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,    18,
     174,    39,    21,    22,    23,    24,    25,   174,    39,    28,
      29,   171,    31,    32,    33,    34,    35,    36,   171,    38,
      82,   171,    33,   169,   169,   168,   154,   171,   543,   171,
      90,   601,    51,    52,    53,    54,    55,   553,   118,   651,
     672,   362,   582,   519,    63,   277,   228,    90,   231,   412,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    80,    81,    -1,    83,    84,    85,    86,   347,    88,
      89,    90,    91,    92,    93,    94,    95,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   116,    -1,    -1,
      -1,    -1,   121,   122,   123,     5,    -1,    -1,    -1,   128,
      -1,    -1,    -1,   132,   133,   134,   135,   136,    -1,    -1,
     139,   140,    -1,   142,   143,   144,    -1,   146,   147,    -1,
      -1,    31,   151,    -1,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,   163,   164,    -1,   166,    -1,    -1,
      -1,    51,    52,    53,    54,    55,    56,    -1,    -1,    -1,
      60,    -1,    62,    63,    64,    65,    66,    67,    68,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    83,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    99,
     100,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,   114,    -1,    -1,   117,   118,    -1,
     120,    -1,    -1,    -1,   124,   125,    -1,   127,    -1,   129,
     130,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   141,    -1,    -1,    -1,   145,    -1,    -1,    -1,   149,
     150,    -1,   152,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   165,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,    18,
      -1,    -1,    21,    22,    23,    24,    25,    -1,    -1,    28,
      29,    -1,    31,    32,    33,    34,    35,    36,    -1,    38,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    51,    52,    53,    54,    55,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    63,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    80,    81,    -1,    83,    84,    85,    86,    -1,    88,
      89,    90,    91,    92,    93,    94,    95,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   116,    -1,    -1,
      -1,    -1,   121,   122,   123,    -1,    -1,    -1,    -1,   128,
      -1,    -1,    -1,   132,   133,   134,   135,   136,    -1,    -1,
     139,   140,    -1,   142,   143,   144,    -1,   146,   147,    -1,
      -1,    -1,   151,    -1,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,   163,   164,    51,    52,    53,    54,
      55,    56,    -1,    -1,    -1,    60,    -1,    62,    63,    64,
      65,    66,    67,    68,    69,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    83,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    99,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   112,   113,   114,
      -1,    -1,   117,   118,    -1,   120,    -1,    -1,    -1,   124,
     125,    -1,   127,    -1,   129,   130,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   141,    -1,    -1,    -1,
     145,    -1,    -1,    -1,   149,   150,    -1,   152,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     165,    51,    52,    53,    54,    55,    56,   172,    -1,    -1,
      60,    -1,    62,    63,    64,    65,    66,    67,    68,    69,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    83,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    99,
     100,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,   114,    -1,    -1,   117,   118,    -1,
     120,    -1,    -1,    -1,   124,   125,    -1,   127,    -1,   129,
     130,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   141,    -1,    -1,    -1,   145,    -1,    -1,    -1,   149,
     150,    -1,   152,    -1,    -1,    51,    52,    53,    54,    55,
      56,    -1,    -1,    -1,    60,   165,    62,    63,    64,    65,
      66,    67,    68,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    83,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    99,   100,   101,   102,   103,   104,   105,
     106,   107,   108,   109,   110,   111,   112,   113,   114,    -1,
      -1,   117,   118,    -1,   120,    -1,    -1,    -1,   124,   125,
      -1,   127,    -1,   129,   130,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   141,    -1,    -1,    -1,   145,
      -1,    -1,    -1,   149,   150,    -1,   152,    -1,    -1,    51,
      52,    53,    54,    55,    56,    -1,    -1,    -1,    60,   165,
      62,    63,    64,    65,    66,    67,    68,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    83,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    99,   100,   101,
     102,   103,   104,   105,   106,   107,   108,   109,   110,   111,
     112,   113,   114,    -1,    -1,   117,   118,    -1,   120,    -1,
      -1,    -1,   124,   125,    -1,   127,    -1,   129,   130,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   141,
      -1,    -1,    -1,   145,    -1,    -1,    -1,   149,   150,    -1,
     152,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   165
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     1,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    21,    22,    23,    24,
      25,    28,    29,    31,    32,    33,    34,    35,    36,    38,
      51,    52,    53,    54,    55,    63,    80,    81,    83,    84,
      85,    86,    88,    89,    90,    91,    92,    93,    94,    95,
     116,   121,   122,   123,   128,   132,   133,   134,   135,   136,
     139,   140,   142,   143,   144,   146,   147,   151,   153,   154,
     155,   156,   157,   158,   159,   160,   161,   162,   163,   164,
     166,   178,   179,   180,   181,   182,   183,   184,   185,   186,
     190,   191,   192,   193,   194,   197,   198,   199,   200,   201,
     202,   203,   204,   205,   206,   207,   208,   209,   210,   211,
     212,   223,   224,   225,   226,   227,   228,   232,   233,   246,
     247,   248,   249,   251,   252,   258,   259,   263,   265,   266,
     267,   269,   271,   272,   273,   274,   276,   277,   278,   279,
     280,   282,   283,   285,   286,   287,   288,   289,   290,   291,
     293,   297,   298,   299,    78,   126,   250,    66,    68,   292,
      79,    83,    96,    97,   167,   229,   230,    51,    52,    53,
      54,    55,    56,    60,    62,    63,    64,    65,    66,    67,
      68,    83,    99,   100,   101,   102,   103,   104,   105,   106,
     107,   108,   109,   110,   111,   112,   113,   114,   117,   118,
     120,   124,   125,   127,   129,   130,   141,   145,   149,   150,
     152,   165,   213,   303,   304,    19,    20,    62,   187,   304,
     304,    42,    43,    62,    83,   115,   253,   254,   255,   253,
     253,   253,   253,   253,    82,    99,   100,   101,   102,   103,
     104,   105,   106,   107,   108,   111,   234,    54,   181,    56,
      54,    55,   169,    83,    83,   117,   118,   264,   124,   125,
     127,   270,   124,   131,   268,   129,   131,   261,   115,   150,
     261,   261,    83,   126,   275,   131,   137,   138,   262,    83,
     141,   115,   119,   260,   115,   281,   145,   261,   115,   284,
     148,   149,   150,    26,    70,   213,    69,    12,     6,   115,
      83,   115,   118,   165,    48,     0,   180,    48,   166,     5,
     182,   188,   212,   223,   224,   225,   226,   227,   299,   182,
     223,   224,   225,   226,   227,    83,   213,   182,   223,   224,
     225,   226,   227,   232,   170,   196,   216,   217,   218,   182,
     219,   220,   301,   302,   304,    67,   257,   302,   219,   302,
     196,   216,    50,   176,    64,    65,   216,   169,    83,    83,
      83,   167,   168,   170,   172,    62,    26,    30,   170,   170,
     170,   170,    37,   254,   250,    83,   169,   169,   169,   169,
      83,    99,   100,   101,   102,   104,   105,   106,   107,   111,
     242,   169,   169,   169,   169,   109,   110,   169,    39,    40,
      41,    83,   235,   166,    57,    59,   294,   295,   296,    83,
      83,   115,   115,   115,   261,   115,   126,   261,   115,   261,
      83,   126,   131,    83,   130,   261,    83,   261,    83,    83,
     262,   115,   168,   261,   115,    83,   152,   304,    83,    83,
     115,   115,   115,   115,    48,   171,   182,   216,    26,   195,
     217,   137,   300,    69,   172,   304,    26,   300,    26,    83,
      98,   115,   231,   169,   169,   169,    83,    96,    97,   229,
      47,    83,    98,   115,   167,   214,   215,   304,   167,   189,
     115,    44,    45,    46,   256,    83,    83,   115,   169,    83,
      87,   245,   245,   245,   245,   169,   172,   169,   169,   169,
     169,   169,   169,   169,   169,   169,   241,   245,   245,   243,
     245,   243,   169,   169,   245,    83,    83,    58,    60,   168,
     260,   261,   261,   115,   261,   261,    83,   261,   120,   168,
     115,    70,   169,   169,   171,   171,   219,    83,   170,   304,
     304,   219,   170,   221,   219,   231,   231,   231,   169,    83,
      83,    83,   167,   168,   171,   172,    83,   168,   171,   171,
      83,    83,   171,   171,   244,   245,   241,   241,   241,   241,
     244,    83,   245,   245,   245,   245,   245,   245,   243,   243,
     245,    39,   168,   241,   241,   241,   241,   173,   173,   241,
      61,   296,   261,   115,   115,   168,   152,    83,    83,   115,
      83,    69,   304,    83,   222,   220,   231,   169,   169,    83,
     215,   304,   167,   171,   171,   241,    39,    39,    39,    39,
     169,   242,    39,    39,    39,    39,    99,   111,   112,   114,
     236,   237,    66,   238,    39,    83,   169,   171,   304,   168,
     171,   231,   231,    83,    39,   244,   170,   170,   170,   170,
     174,   168,   170,   174,    83,    83,   245,   245,   113,   171,
      83,   239,   240,    39,   236,   243,    39,   171,   171,   169,
     169,   171,   168,   171,   175,   245,   245,   239,   243,   171,
     171
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

  case 12:

    {;}
    break;

  case 13:

    {;}
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

    { result->cur_stmt_type_ = OBPROXY_T_OTHERS; ;}
    break;

  case 52:

    { result->cur_stmt_type_ = OBPROXY_T_CREATE; ;}
    break;

  case 53:

    { result->cur_stmt_type_ = OBPROXY_T_DROP; ;}
    break;

  case 54:

    { result->cur_stmt_type_ = OBPROXY_T_ALTER; ;}
    break;

  case 55:

    { result->cur_stmt_type_ = OBPROXY_T_TRUNCATE; ;}
    break;

  case 56:

    { result->cur_stmt_type_ = OBPROXY_T_RENAME; ;}
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
                        ObProxyTextPsExecuteParseNode *node = NULL;
                        malloc_execute_parse_node(node);
                        node->str_value_ = (yyvsp[(2) - (2)].str);
                        add_text_ps_execute_node(result->text_ps_execute_parse_info_, node);
                      ;}
    break;

  case 70:

    {
                        ObProxyTextPsExecuteParseNode *node = NULL;
                        malloc_execute_parse_node(node);
                        node->str_value_ = (yyvsp[(4) - (4)].str);
                        add_text_ps_execute_node(result->text_ps_execute_parse_info_, node);
                      ;}
    break;

  case 71:

    {
                      result->text_ps_inner_stmt_type_ = OBPROXY_T_TEXT_PS_PREPARE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 72:

    {
            ;}
    break;

  case 73:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
              result->text_ps_name_ = (yyvsp[(2) - (2)].str);
            ;}
    break;

  case 74:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
              result->text_ps_name_ = (yyvsp[(2) - (4)].str);
            ;}
    break;

  case 75:

    { result->cur_stmt_type_ = OBPROXY_T_GRANT; ;}
    break;

  case 76:

    { result->cur_stmt_type_ = OBPROXY_T_REVOKE; ;}
    break;

  case 77:

    { result->cur_stmt_type_ = OBPROXY_T_ANALYZE; ;}
    break;

  case 78:

    { result->cur_stmt_type_ = OBPROXY_T_PURGE; ;}
    break;

  case 79:

    { result->cur_stmt_type_ = OBPROXY_T_FLASHBACK; ;}
    break;

  case 80:

    { result->cur_stmt_type_ = OBPROXY_T_COMMENT; ;}
    break;

  case 81:

    { result->cur_stmt_type_ = OBPROXY_T_AUDIT; ;}
    break;

  case 82:

    { result->cur_stmt_type_ = OBPROXY_T_NOAUDIT; ;}
    break;

  case 85:

    {;}
    break;

  case 86:

    {;}
    break;

  case 87:

    {;}
    break;

  case 92:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_TX_RO; ;}
    break;

  case 96:

    { result->col_name_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 97:

    { result->cur_stmt_type_ = OBPROXY_T_SET_AC_0; ;}
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

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLES; ;}
    break;

  case 110:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE; ;}
    break;

  case 111:

    {
                      result->cur_stmt_type_ = OBPROXY_T_DESC;
                      result->sub_stmt_type_ = OBPROXY_T_SUB_DESC_TABLE;
                  ;}
    break;

  case 112:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DB_VERSION; ;}
    break;

  case 113:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY; ;}
    break;

  case 114:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY;
                 ;}
    break;

  case 115:

    {
                     SET_ICMD_SECOND_STRING((yyvsp[(5) - (5)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY;
                 ;}
    break;

  case 116:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (7)].str));
                     SET_ICMD_SECOND_STRING((yyvsp[(7) - (7)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY;
                 ;}
    break;

  case 117:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_ROUTE_ADDR; ;}
    break;

  case 118:

    {
                              result->cur_stmt_type_ = OBPROXY_T_SET_ROUTE_ADDR;
                              result->cmd_info_.integer_[0] = (yyvsp[(3) - (3)].num);
                           ;}
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

  case 126:

    {
                   result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                 ;}
    break;

  case 127:

    {
                   result->table_info_.package_name_ = (yyvsp[(1) - (3)].str);
                   result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                 ;}
    break;

  case 128:

    {
                   result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                   result->table_info_.package_name_ = (yyvsp[(3) - (5)].str);
                   result->table_info_.table_name_ = (yyvsp[(5) - (5)].str);
                 ;}
    break;

  case 129:

    {
                result->call_parse_info_.node_count_ = 0;
              ;}
    break;

  case 130:

    {
                result->call_parse_info_.node_count_ = 0;
                add_call_node(result->call_parse_info_, (yyvsp[(1) - (1)].node));
              ;}
    break;

  case 131:

    {
                add_call_node(result->call_parse_info_, (yyvsp[(3) - (3)].node));
              ;}
    break;

  case 132:

    {
            malloc_call_node((yyval.node), CALL_TOKEN_STR_VAL);
            (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 133:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_INT_VAL);
           (yyval.node)->int_value_ = (yyvsp[(1) - (1)].num);
         ;}
    break;

  case 134:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_NUMBER_VAL);
           (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 135:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_USER_VAR);
           (yyval.node)->str_value_ = (yyvsp[(2) - (2)].str);
         ;}
    break;

  case 136:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_SYS_VAR);
           (yyval.node)->str_value_ = (yyvsp[(3) - (3)].str);
         ;}
    break;

  case 137:

    {
           result->placeholder_list_idx_++;
           malloc_call_node((yyval.node), CALL_TOKEN_PLACE_HOLDER);
           (yyval.node)->placeholder_idx_ = result->placeholder_list_idx_ - 1;
         ;}
    break;

  case 151:

    {
                                                                  handle_stmt_end(result);
                                                                  HANDLE_ACCEPT();
                                                                ;}
    break;

  case 156:

    {
                                                 handle_stmt_end(result);
                                                 HANDLE_ACCEPT();
                                               ;}
    break;

  case 160:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_USER);
        ;}
    break;

  case 161:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 162:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 163:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(5) - (5)].var_node), (yyvsp[(3) - (5)].str), SET_VAR_SYS);
        ;}
    break;

  case 164:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 165:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 166:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_SYS);
        ;}
    break;

  case 167:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_STR);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 168:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_INT);
               (yyval.var_node)->int_value_ = (yyvsp[(1) - (1)].num);
             ;}
    break;

  case 169:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_NUMBER);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 172:

    {;}
    break;

  case 173:

    {;}
    break;

  case 174:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 175:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 176:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 177:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 178:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 179:

    {
              malloc_shard_column_node((yyval.shard_node), (yyvsp[(2) - (7)].str), (yyvsp[(3) - (7)].str), DBMESH_TOKEN_STR_VAL);
              (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (7)].str);
              add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
            ;}
    break;

  case 180:

    { result->trace_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 181:

    { result->rpc_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 182:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 183:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 184:

    {;}
    break;

  case 185:

    {;}
    break;

  case 186:

    {;}
    break;

  case 188:

    { result->has_simple_route_info_ = true; result->simple_route_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 189:

    { result->simple_route_info_.part_key_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 193:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.group_idx_str_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 194:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.table_name_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 195:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 196:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 197:

    {result->dbp_route_info_.has_shard_key_ = true;;}
    break;

  case 198:

    { result->trace_id_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 199:

    { result->trace_id_ = (yyvsp[(3) - (6)].str); result->rpc_id_ = (yyvsp[(5) - (6)].str); ;}
    break;

  case 200:

    {;}
    break;

  case 202:

    {
                   if (result->dbp_route_info_.shard_key_count_ < OBPROXY_MAX_DBP_SHARD_KEY_NUM) {
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].left_str_ = (yyvsp[(1) - (3)].str);
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].right_str_ = (yyvsp[(3) - (3)].str);
                     ++result->dbp_route_info_.shard_key_count_;
                   }
                 ;}
    break;

  case 205:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 206:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 207:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 208:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 209:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 210:

    { result->trace_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 211:

    { result->rpc_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 212:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 213:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 214:

    {
             malloc_shard_column_node((yyval.shard_node), (yyvsp[(1) - (5)].str), (yyvsp[(3) - (5)].str), DBMESH_TOKEN_STR_VAL);
             (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (5)].str);
             add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
           ;}
    break;

  case 215:

    {;}
    break;

  case 216:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 218:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 240:

    { result->query_timeout_ = (yyvsp[(3) - (4)].num); ;}
    break;

  case 241:

    {;}
    break;

  case 243:

    {
      add_hint_index(result->dbmesh_route_info_, (yyvsp[(3) - (5)].str));
      result->dbmesh_route_info_.index_count_++;
    ;}
    break;

  case 244:

    {;}
    break;

  case 245:

    {;}
    break;

  case 246:

    {;}
    break;

  case 247:

    {;}
    break;

  case 248:

    {;}
    break;

  case 249:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_WEAK); ;}
    break;

  case 250:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_STRONG); ;}
    break;

  case 251:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_FROZEN); ;}
    break;

  case 254:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_WARNINGS; ;}
    break;

  case 255:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_ERRORS; ;}
    break;

  case 256:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 280:

    {
;}
    break;

  case 281:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 282:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 283:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 284:

    {;}
    break;

  case 285:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 286:

    {;}
    break;

  case 287:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 289:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 290:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 291:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 292:

    {;}
    break;

  case 293:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 294:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 295:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 297:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 298:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 299:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 300:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 301:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 303:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 304:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 305:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 306:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 307:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 308:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 309:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 310:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 311:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 312:

    {;}
    break;

  case 313:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 314:

    {;}
    break;

  case 315:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 316:

    {;}
    break;

  case 318:

    {;}
    break;

  case 319:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 320:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 321:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 322:

    {;}
    break;

  case 323:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 324:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 325:

    {;}
    break;

  case 326:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 327:

    {;}
    break;

  case 328:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 329:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 330:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 332:

    {;}
    break;

  case 333:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 334:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 335:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 336:

    {;}
    break;

  case 337:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 339:

    {;}
    break;

  case 340:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 341:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 342:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 343:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 344:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 345:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 346:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 347:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 348:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 349:

    {;}
    break;

  case 350:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 351:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 352:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 353:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 354:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 355:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 356:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 359:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 360:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 361:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 362:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 369:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 370:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 372:

    {;}
    break;

  case 373:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 374:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 375:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT();
                                                ;}
    break;

  case 376:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 377:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 378:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 379:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 380:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 381:

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

