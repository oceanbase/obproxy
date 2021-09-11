
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
  result->sub_stmt_type_ = OBPROXY_T_SUB_INVALID;\
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
     GRANT = 274,
     REVOKE = 275,
     ANALYZE = 276,
     PURGE = 277,
     COMMENT = 278,
     FROM = 279,
     DUAL = 280,
     PREPARE = 281,
     EXECUTE = 282,
     USING = 283,
     SELECT_HINT_BEGIN = 284,
     UPDATE_HINT_BEGIN = 285,
     DELETE_HINT_BEGIN = 286,
     INSERT_HINT_BEGIN = 287,
     REPLACE_HINT_BEGIN = 288,
     MERGE_HINT_BEGIN = 289,
     HINT_END = 290,
     COMMENT_BEGIN = 291,
     COMMENT_END = 292,
     ROUTE_TABLE = 293,
     ROUTE_PART_KEY = 294,
     QUERY_TIMEOUT = 295,
     READ_CONSISTENCY = 296,
     WEAK = 297,
     STRONG = 298,
     FROZEN = 299,
     PLACE_HOLDER = 300,
     END_P = 301,
     ERROR = 302,
     WHEN = 303,
     FLASHBACK = 304,
     AUDIT = 305,
     NOAUDIT = 306,
     BEGI = 307,
     START = 308,
     TRANSACTION = 309,
     READ = 310,
     ONLY = 311,
     WITH = 312,
     CONSISTENT = 313,
     SNAPSHOT = 314,
     INDEX = 315,
     XA = 316,
     WARNINGS = 317,
     ERRORS = 318,
     TRACE = 319,
     QUICK = 320,
     COUNT = 321,
     AS = 322,
     WHERE = 323,
     VALUES = 324,
     ORDER = 325,
     GROUP = 326,
     HAVING = 327,
     INTO = 328,
     UNION = 329,
     FOR = 330,
     TX_READ_ONLY = 331,
     AUTOCOMMIT_0 = 332,
     SELECT_OBPROXY_ROUTE_ADDR = 333,
     SET_OBPROXY_ROUTE_ADDR = 334,
     NAME_OB_DOT = 335,
     NAME_OB = 336,
     EXPLAIN = 337,
     DESC = 338,
     DESCRIBE = 339,
     NAME_STR = 340,
     USE = 341,
     HELP = 342,
     SET_NAMES = 343,
     SET_CHARSET = 344,
     SET_PASSWORD = 345,
     SET_DEFAULT = 346,
     SET_OB_READ_CONSISTENCY = 347,
     SET_TX_READ_ONLY = 348,
     GLOBAL = 349,
     SESSION = 350,
     NUMBER_VAL = 351,
     GROUP_ID = 352,
     TABLE_ID = 353,
     ELASTIC_ID = 354,
     TESTLOAD = 355,
     ODP_COMMENT = 356,
     TNT_ID = 357,
     DISASTER_STATUS = 358,
     TRACE_ID = 359,
     RPC_ID = 360,
     DBP_COMMENT = 361,
     ROUTE_TAG = 362,
     SYS_TAG = 363,
     TABLE_NAME = 364,
     SCAN_ALL = 365,
     PARALL = 366,
     SHARD_KEY = 367,
     INT_NUM = 368,
     SHOW_PROXYNET = 369,
     THREAD = 370,
     CONNECTION = 371,
     LIMIT = 372,
     OFFSET = 373,
     SHOW_PROCESSLIST = 374,
     SHOW_PROXYSESSION = 375,
     SHOW_GLOBALSESSION = 376,
     ATTRIBUTE = 377,
     VARIABLES = 378,
     ALL = 379,
     STAT = 380,
     SHOW_PROXYCONFIG = 381,
     DIFF = 382,
     USER = 383,
     LIKE = 384,
     SHOW_PROXYSM = 385,
     SHOW_PROXYCLUSTER = 386,
     SHOW_PROXYRESOURCE = 387,
     SHOW_PROXYCONGESTION = 388,
     SHOW_PROXYROUTE = 389,
     PARTITION = 390,
     ROUTINE = 391,
     SHOW_PROXYVIP = 392,
     SHOW_PROXYMEMORY = 393,
     OBJPOOL = 394,
     SHOW_SQLAUDIT = 395,
     SHOW_WARNLOG = 396,
     SHOW_PROXYSTAT = 397,
     REFRESH = 398,
     SHOW_PROXYTRACE = 399,
     SHOW_PROXYINFO = 400,
     BINARY = 401,
     UPGRADE = 402,
     IDC = 403,
     SHOW_TOPOLOGY = 404,
     GROUP_NAME = 405,
     SHOW_DB_VERSION = 406,
     SHOW_DATABASES = 407,
     SHOW_TABLES = 408,
     SELECT_DATABASE = 409,
     SHOW_CREATE_TABLE = 410,
     ALTER_PROXYCONFIG = 411,
     ALTER_PROXYRESOURCE = 412,
     PING_PROXY = 413,
     KILL_PROXYSESSION = 414,
     KILL_GLOBALSESSION = 415,
     KILL = 416,
     QUERY = 417
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
#define YYFINAL  298
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   1404

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  174
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  126
/* YYNRULES -- Number of rules.  */
#define YYNRULES  420
/* YYNRULES -- Number of states.  */
#define YYNSTATES  672

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   417

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,   172,     2,     2,     2,     2,
     167,   168,   173,     2,   165,     2,   169,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   163,
       2,   166,     2,     2,   164,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   170,     2,   171,     2,     2,     2,     2,
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
     155,   156,   157,   158,   159,   160,   161,   162
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
     121,   123,   125,   127,   129,   131,   133,   135,   137,   139,
     141,   143,   145,   147,   149,   151,   154,   159,   163,   166,
     169,   174,   176,   178,   180,   182,   184,   186,   188,   190,
     192,   195,   197,   199,   201,   202,   205,   206,   208,   211,
     217,   221,   224,   228,   230,   232,   234,   236,   238,   240,
     242,   244,   246,   248,   251,   254,   256,   258,   262,   268,
     276,   278,   282,   284,   286,   288,   290,   292,   294,   300,
     302,   306,   312,   313,   315,   319,   321,   323,   325,   328,
     332,   334,   336,   339,   341,   344,   348,   352,   354,   356,
     358,   359,   363,   365,   369,   373,   379,   382,   385,   390,
     393,   396,   400,   402,   407,   414,   419,   425,   432,   437,
     441,   443,   445,   447,   449,   452,   456,   462,   469,   476,
     483,   490,   497,   505,   512,   519,   526,   533,   542,   551,
     552,   555,   558,   561,   563,   567,   569,   574,   579,   583,
     590,   595,   600,   607,   611,   613,   617,   618,   622,   626,
     630,   634,   638,   642,   646,   650,   654,   658,   664,   668,
     669,   671,   672,   674,   676,   678,   680,   683,   685,   688,
     690,   693,   696,   700,   701,   703,   706,   708,   711,   713,
     716,   719,   720,   723,   728,   730,   735,   741,   743,   748,
     753,   759,   760,   762,   764,   766,   767,   769,   773,   777,
     780,   782,   784,   786,   788,   790,   792,   794,   796,   798,
     800,   802,   804,   806,   808,   810,   812,   814,   816,   818,
     820,   822,   824,   826,   827,   830,   835,   840,   841,   844,
     845,   848,   851,   853,   855,   859,   862,   866,   871,   873,
     876,   877,   880,   884,   887,   890,   893,   894,   897,   901,
     904,   908,   911,   915,   919,   924,   926,   929,   932,   936,
     939,   942,   943,   945,   947,   950,   953,   957,   960,   962,
     965,   967,   970,   973,   976,   979,   980,   982,   986,   992,
     995,   999,  1002,  1003,  1005,  1008,  1011,  1014,  1017,  1022,
    1028,  1034,  1038,  1040,  1043,  1047,  1051,  1054,  1057,  1061,
    1065,  1066,  1069,  1071,  1075,  1079,  1083,  1084,  1086,  1088,
    1092,  1095,  1099,  1102,  1105,  1107,  1108,  1111,  1116,  1119,
    1121,  1125,  1128,  1133,  1137,  1143,  1145,  1147,  1149,  1151,
    1153,  1155,  1157,  1159,  1161,  1163,  1165,  1167,  1169,  1171,
    1173,  1175,  1177,  1179,  1181,  1183,  1185,  1187,  1189,  1191,
    1193,  1195,  1197,  1199,  1201,  1203,  1205,  1207,  1209,  1211,
    1213,  1215,  1217,  1219,  1221,  1223,  1225,  1227,  1229,  1231,
    1233
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     175,     0,    -1,   176,    -1,     1,    -1,   177,    -1,   176,
     177,    -1,   178,    46,    -1,   178,   163,    -1,   178,   163,
      46,    -1,   163,    -1,   163,    46,    -1,    52,   178,   163,
      -1,   179,    -1,   218,    -1,   223,    -1,   219,    -1,   220,
      -1,   221,    -1,   180,    -1,   181,    -1,   288,    -1,   253,
      -1,   195,    -1,   254,    -1,   292,    -1,   293,    -1,   201,
      -1,   202,    -1,   203,    -1,   204,    -1,   205,    -1,   206,
      -1,   207,    -1,   182,    -1,   187,    -1,   222,    -1,   294,
      -1,   241,   192,   191,    -1,   189,   179,    -1,   189,   218,
      -1,   189,   220,    -1,   189,   221,    -1,   189,   219,    -1,
     189,   222,    -1,   227,   179,    -1,   227,   218,    -1,   227,
     220,    -1,   227,   221,    -1,   227,   219,    -1,   227,   222,
      -1,   183,    -1,   188,    -1,    14,    -1,    15,    -1,    16,
      -1,    17,    -1,    18,    -1,   179,    -1,   218,    -1,   219,
      -1,   221,    -1,   220,    -1,   294,    -1,   207,    -1,   222,
      -1,   164,    81,    -1,   185,   165,   164,    81,    -1,    26,
     299,    24,    -1,   186,   184,    -1,    27,   299,    -1,    27,
     299,    28,   185,    -1,    19,    -1,    20,    -1,    21,    -1,
      22,    -1,    49,    -1,    23,    -1,    50,    -1,    51,    -1,
     190,    -1,   190,    81,    -1,    82,    -1,    83,    -1,    84,
      -1,    -1,    24,   214,    -1,    -1,   211,    -1,     5,    76,
      -1,     5,    76,   192,    24,   214,    -1,     5,    76,   211,
      -1,    12,    77,    -1,    12,    77,   211,    -1,   193,    -1,
     194,    -1,   199,    -1,   200,    -1,   196,    -1,   198,    -1,
     197,    -1,   154,    -1,   152,    -1,   153,    -1,   155,   208,
      -1,   190,   208,    -1,   151,    -1,   149,    -1,   149,    24,
      81,    -1,   149,    68,   150,   166,    81,    -1,   149,    24,
      81,    68,   150,   166,    81,    -1,    78,    -1,    79,   166,
     113,    -1,    88,    -1,    89,    -1,    90,    -1,    91,    -1,
      92,    -1,    93,    -1,    13,   208,   167,   209,   168,    -1,
     299,    -1,   299,   169,   299,    -1,   299,   169,   299,   169,
     299,    -1,    -1,   210,    -1,   209,   165,   210,    -1,    81,
      -1,   113,    -1,    96,    -1,   164,    81,    -1,   164,   164,
      81,    -1,    45,    -1,   212,    -1,   211,   212,    -1,   213,
      -1,   167,   168,    -1,   167,   179,   168,    -1,   167,   211,
     168,    -1,   296,    -1,   215,    -1,   179,    -1,    -1,   167,
     217,   168,    -1,    81,    -1,   217,   165,    81,    -1,   244,
     297,   295,    -1,   244,   297,   295,   216,   215,    -1,   246,
     214,    -1,   242,   214,    -1,   243,   252,    24,   214,    -1,
     247,   297,    -1,    12,   224,    -1,   225,   165,   224,    -1,
     225,    -1,   164,    81,   166,   226,    -1,   164,   164,    94,
      81,   166,   226,    -1,    94,    81,   166,   226,    -1,   164,
     164,    81,   166,   226,    -1,   164,   164,    95,    81,   166,
     226,    -1,    95,    81,   166,   226,    -1,    81,   166,   226,
      -1,    81,    -1,   113,    -1,    96,    -1,   228,    -1,   228,
     227,    -1,    36,   229,    37,    -1,    36,   101,   237,   236,
      37,    -1,    36,    98,   166,   240,   236,    37,    -1,    36,
     109,   166,   240,   236,    37,    -1,    36,    97,   166,   240,
     236,    37,    -1,    36,    99,   166,   240,   236,    37,    -1,
      36,   100,   166,   240,   236,    37,    -1,    36,    80,    81,
     166,   239,   236,    37,    -1,    36,   104,   166,   238,   236,
      37,    -1,    36,   105,   166,   238,   236,    37,    -1,    36,
     102,   166,   240,   236,    37,    -1,    36,   103,   166,   240,
     236,    37,    -1,    36,   106,   107,   166,   170,   231,   171,
      37,    -1,    36,   106,   108,   166,   170,   233,   171,    37,
      -1,    -1,   229,   230,    -1,    38,    81,    -1,    39,    81,
      -1,    81,    -1,   232,   165,   231,    -1,   232,    -1,    97,
     167,   240,   168,    -1,   109,   167,   240,   168,    -1,   110,
     167,   168,    -1,   110,   167,   111,   166,   240,   168,    -1,
     112,   167,   234,   168,    -1,    64,   167,   238,   168,    -1,
      64,   167,   238,   172,   238,   168,    -1,   235,   165,   234,
      -1,   235,    -1,    81,   166,   240,    -1,    -1,   236,   165,
     237,    -1,    97,   166,   240,    -1,    98,   166,   240,    -1,
     109,   166,   240,    -1,    99,   166,   240,    -1,   100,   166,
     240,    -1,   104,   166,   238,    -1,   105,   166,   238,    -1,
     102,   166,   240,    -1,   103,   166,   240,    -1,    81,   169,
      81,   166,   239,    -1,    81,   166,   239,    -1,    -1,   240,
      -1,    -1,   240,    -1,    81,    -1,    85,    -1,     5,    -1,
      29,   248,    -1,     8,    -1,    30,   248,    -1,     6,    -1,
      31,   248,    -1,     7,   245,    -1,    32,   248,   245,    -1,
      -1,   124,    -1,   124,    48,    -1,     9,    -1,    33,   248,
      -1,    10,    -1,    34,   248,    -1,   249,    35,    -1,    -1,
     250,   249,    -1,    40,   167,   113,   168,    -1,   113,    -1,
      41,   167,   251,   168,    -1,    60,   167,    81,    81,   168,
      -1,    81,    -1,    81,   167,   113,   168,    -1,    81,   167,
      81,   168,    -1,    81,   167,    81,    81,   168,    -1,    -1,
      42,    -1,    43,    -1,    44,    -1,    -1,    65,    -1,    11,
     287,    62,    -1,    11,   287,    63,    -1,    11,    64,    -1,
     258,    -1,   260,    -1,   261,    -1,   264,    -1,   262,    -1,
     266,    -1,   267,    -1,   268,    -1,   269,    -1,   271,    -1,
     272,    -1,   273,    -1,   274,    -1,   275,    -1,   277,    -1,
     278,    -1,   280,    -1,   281,    -1,   282,    -1,   283,    -1,
     284,    -1,   285,    -1,   286,    -1,    -1,   117,   113,    -1,
     117,   113,   165,   113,    -1,   117,   113,   118,   113,    -1,
      -1,   129,    81,    -1,    -1,   129,    81,    -1,   114,   259,
      -1,   115,    -1,   116,    -1,   116,   113,   255,    -1,   126,
     256,    -1,   126,   127,   256,    -1,   126,   127,   128,   256,
      -1,   119,    -1,   121,   263,    -1,    -1,   122,    81,    -1,
     122,   129,    81,    -1,   122,   124,    -1,   129,    81,    -1,
     120,   265,    -1,    -1,   122,   256,    -1,   122,   113,   256,
      -1,   125,   256,    -1,   125,   113,   256,    -1,   123,   256,
      -1,   123,   113,   256,    -1,   123,   124,   256,    -1,   123,
     124,   113,   256,    -1,   130,    -1,   130,   113,    -1,   131,
     256,    -1,   131,   148,   256,    -1,   132,   256,    -1,   133,
     270,    -1,    -1,    81,    -1,   124,    -1,   124,    81,    -1,
     134,   257,    -1,   134,   136,   257,    -1,   134,   135,    -1,
     137,    -1,   137,    81,    -1,   138,    -1,   138,   139,    -1,
     140,   255,    -1,   140,   113,    -1,   141,   276,    -1,    -1,
     113,    -1,   113,   165,   113,    -1,   113,   165,   113,   165,
      81,    -1,   142,   256,    -1,   142,   143,   256,    -1,   144,
     279,    -1,    -1,   113,    -1,   113,   113,    -1,   145,   146,
      -1,   145,   147,    -1,   145,   148,    -1,   156,    12,    81,
     166,    -1,   156,    12,    81,   166,    81,    -1,   156,    12,
      81,   166,   113,    -1,   157,     6,    81,    -1,   158,    -1,
     159,   113,    -1,   159,   113,   113,    -1,   160,    81,   113,
      -1,   160,    81,    -1,   161,   113,    -1,   161,   116,   113,
      -1,   161,   162,   113,    -1,    -1,    66,   173,    -1,    52,
      -1,    53,    54,   289,    -1,    61,    52,    81,    -1,    61,
      53,    81,    -1,    -1,   290,    -1,   291,    -1,   290,   165,
     291,    -1,    55,    56,    -1,    57,    58,    59,    -1,    86,
      81,    -1,    87,    81,    -1,    81,    -1,    -1,   135,    81,
      -1,   135,   167,    81,   168,    -1,   297,   295,    -1,   299,
      -1,   299,   169,   299,    -1,   299,   299,    -1,   299,   169,
     299,   299,    -1,   299,    67,   299,    -1,   299,   169,   299,
      67,   299,    -1,    53,    -1,    61,    -1,    52,    -1,    54,
      -1,    58,    -1,    63,    -1,    62,    -1,    66,    -1,    65,
      -1,    64,    -1,   115,    -1,   116,    -1,   118,    -1,   122,
      -1,   123,    -1,   125,    -1,   127,    -1,   128,    -1,   139,
      -1,   143,    -1,   147,    -1,   148,    -1,   162,    -1,    97,
      -1,    98,    -1,    99,    -1,   100,    -1,   150,    -1,   101,
      -1,   102,    -1,   103,    -1,   104,    -1,   105,    -1,   106,
      -1,   107,    -1,   108,    -1,   109,    -1,   111,    -1,   110,
      -1,   112,    -1,    60,    -1,    49,    -1,    50,    -1,    51,
      -1,    81,    -1,   298,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   337,   337,   338,   340,   341,   343,   344,   345,   346,
     347,   348,   350,   351,   352,   353,   354,   355,   356,   357,
     358,   359,   360,   361,   362,   363,   364,   365,   366,   367,
     368,   369,   370,   371,   372,   373,   374,   376,   378,   379,
     380,   381,   382,   383,   385,   386,   387,   388,   389,   390,
     392,   393,   395,   396,   397,   398,   399,   401,   402,   403,
     404,   405,   406,   407,   408,   410,   417,   425,   431,   434,
     439,   445,   446,   447,   448,   449,   450,   451,   452,   454,
     455,   457,   458,   459,   461,   462,   464,   465,   467,   468,
     469,   471,   472,   474,   475,   476,   477,   478,   480,   481,
     482,   483,   484,   485,   486,   492,   494,   495,   500,   505,
     512,   515,   521,   522,   523,   524,   525,   526,   529,   531,
     535,   540,   548,   551,   556,   561,   566,   571,   576,   581,
     586,   594,   595,   597,   599,   600,   601,   603,   604,   606,
     608,   609,   611,   612,   614,   618,   619,   620,   621,   622,
     627,   629,   630,   632,   636,   640,   644,   648,   652,   656,
     660,   665,   670,   676,   677,   679,   680,   681,   682,   683,
     684,   685,   686,   692,   693,   694,   695,   696,   697,   699,
     700,   702,   703,   704,   706,   707,   709,   714,   719,   720,
     721,   723,   724,   726,   727,   729,   737,   738,   740,   741,
     742,   743,   744,   745,   746,   747,   748,   749,   755,   757,
     758,   760,   761,   763,   764,   766,   767,   768,   769,   770,
     771,   772,   773,   775,   776,   777,   779,   780,   781,   782,
     783,   784,   785,   787,   788,   789,   790,   795,   796,   797,
     798,   800,   801,   802,   803,   805,   806,   809,   810,   811,
     814,   815,   816,   817,   818,   819,   820,   821,   822,   823,
     824,   825,   826,   827,   828,   829,   830,   831,   832,   833,
     834,   835,   836,   841,   843,   847,   852,   860,   861,   865,
     866,   869,   871,   872,   873,   877,   878,   879,   883,   885,
     887,   888,   889,   890,   891,   894,   896,   897,   898,   899,
     900,   901,   902,   903,   904,   908,   909,   913,   914,   919,
     922,   924,   925,   926,   927,   931,   932,   933,   937,   938,
     942,   943,   947,   948,   951,   953,   954,   955,   956,   960,
     961,   964,   966,   967,   968,   972,   973,   974,   978,   979,
     980,   984,   988,   992,   993,   997,   998,  1002,  1003,  1004,
    1007,  1008,  1011,  1015,  1016,  1017,  1019,  1020,  1022,  1023,
    1026,  1027,  1030,  1036,  1039,  1041,  1042,  1043,  1045,  1050,
    1053,  1057,  1061,  1066,  1070,  1076,  1077,  1078,  1079,  1080,
    1081,  1082,  1083,  1084,  1085,  1086,  1087,  1088,  1089,  1090,
    1091,  1092,  1093,  1094,  1095,  1096,  1097,  1098,  1099,  1100,
    1101,  1102,  1103,  1104,  1105,  1106,  1107,  1108,  1109,  1110,
    1111,  1112,  1113,  1114,  1115,  1116,  1117,  1118,  1119,  1121,
    1122
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
  "RENAME", "GRANT", "REVOKE", "ANALYZE", "PURGE", "COMMENT", "FROM",
  "DUAL", "PREPARE", "EXECUTE", "USING", "SELECT_HINT_BEGIN",
  "UPDATE_HINT_BEGIN", "DELETE_HINT_BEGIN", "INSERT_HINT_BEGIN",
  "REPLACE_HINT_BEGIN", "MERGE_HINT_BEGIN", "HINT_END", "COMMENT_BEGIN",
  "COMMENT_END", "ROUTE_TABLE", "ROUTE_PART_KEY", "QUERY_TIMEOUT",
  "READ_CONSISTENCY", "WEAK", "STRONG", "FROZEN", "PLACE_HOLDER", "END_P",
  "ERROR", "WHEN", "FLASHBACK", "AUDIT", "NOAUDIT", "BEGI", "START",
  "TRANSACTION", "READ", "ONLY", "WITH", "CONSISTENT", "SNAPSHOT", "INDEX",
  "XA", "WARNINGS", "ERRORS", "TRACE", "QUICK", "COUNT", "AS", "WHERE",
  "VALUES", "ORDER", "GROUP", "HAVING", "INTO", "UNION", "FOR",
  "TX_READ_ONLY", "AUTOCOMMIT_0", "SELECT_OBPROXY_ROUTE_ADDR",
  "SET_OBPROXY_ROUTE_ADDR", "NAME_OB_DOT", "NAME_OB", "EXPLAIN", "DESC",
  "DESCRIBE", "NAME_STR", "USE", "HELP", "SET_NAMES", "SET_CHARSET",
  "SET_PASSWORD", "SET_DEFAULT", "SET_OB_READ_CONSISTENCY",
  "SET_TX_READ_ONLY", "GLOBAL", "SESSION", "NUMBER_VAL", "GROUP_ID",
  "TABLE_ID", "ELASTIC_ID", "TESTLOAD", "ODP_COMMENT", "TNT_ID",
  "DISASTER_STATUS", "TRACE_ID", "RPC_ID", "DBP_COMMENT", "ROUTE_TAG",
  "SYS_TAG", "TABLE_NAME", "SCAN_ALL", "PARALL", "SHARD_KEY", "INT_NUM",
  "SHOW_PROXYNET", "THREAD", "CONNECTION", "LIMIT", "OFFSET",
  "SHOW_PROCESSLIST", "SHOW_PROXYSESSION", "SHOW_GLOBALSESSION",
  "ATTRIBUTE", "VARIABLES", "ALL", "STAT", "SHOW_PROXYCONFIG", "DIFF",
  "USER", "LIKE", "SHOW_PROXYSM", "SHOW_PROXYCLUSTER",
  "SHOW_PROXYRESOURCE", "SHOW_PROXYCONGESTION", "SHOW_PROXYROUTE",
  "PARTITION", "ROUTINE", "SHOW_PROXYVIP", "SHOW_PROXYMEMORY", "OBJPOOL",
  "SHOW_SQLAUDIT", "SHOW_WARNLOG", "SHOW_PROXYSTAT", "REFRESH",
  "SHOW_PROXYTRACE", "SHOW_PROXYINFO", "BINARY", "UPGRADE", "IDC",
  "SHOW_TOPOLOGY", "GROUP_NAME", "SHOW_DB_VERSION", "SHOW_DATABASES",
  "SHOW_TABLES", "SELECT_DATABASE", "SHOW_CREATE_TABLE",
  "ALTER_PROXYCONFIG", "ALTER_PROXYRESOURCE", "PING_PROXY",
  "KILL_PROXYSESSION", "KILL_GLOBALSESSION", "KILL", "QUERY", "';'", "'@'",
  "','", "'='", "'('", "')'", "'.'", "'{'", "'}'", "'#'", "'*'", "$accept",
  "root", "sql_stmts", "sql_stmt", "stmt", "select_stmt", "explain_stmt",
  "comment_stmt", "ddl_stmt", "mysql_ddl_stmt", "text_ps_from_stmt",
  "text_ps_using_var_list", "text_ps_prepare_stmt", "text_ps_stmt",
  "oracle_ddl_stmt", "explain_or_desc_stmt", "explain_or_desc", "opt_from",
  "select_expr_list", "select_tx_read_only_stmt", "set_autocommit_0_stmt",
  "hooked_stmt", "shard_special_stmt", "show_db_version_stmt",
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
     415,   416,   417,    59,    64,    44,    61,    40,    41,    46,
     123,   125,    35,    42
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   174,   175,   175,   176,   176,   177,   177,   177,   177,
     177,   177,   178,   178,   178,   178,   178,   178,   178,   178,
     178,   178,   178,   178,   178,   178,   178,   178,   178,   178,
     178,   178,   178,   178,   178,   178,   178,   179,   180,   180,
     180,   180,   180,   180,   181,   181,   181,   181,   181,   181,
     182,   182,   183,   183,   183,   183,   183,   184,   184,   184,
     184,   184,   184,   184,   184,   185,   185,   186,   187,   187,
     187,   188,   188,   188,   188,   188,   188,   188,   188,   189,
     189,   190,   190,   190,   191,   191,   192,   192,   193,   193,
     193,   194,   194,   195,   195,   195,   195,   195,   196,   196,
     196,   196,   196,   196,   196,   197,   198,   198,   198,   198,
     199,   200,   201,   202,   203,   204,   205,   206,   207,   208,
     208,   208,   209,   209,   209,   210,   210,   210,   210,   210,
     210,   211,   211,   212,   213,   213,   213,   214,   214,   215,
     216,   216,   217,   217,   218,   218,   219,   220,   221,   222,
     223,   224,   224,   225,   225,   225,   225,   225,   225,   225,
     226,   226,   226,   227,   227,   228,   228,   228,   228,   228,
     228,   228,   228,   228,   228,   228,   228,   228,   228,   229,
     229,   230,   230,   230,   231,   231,   232,   232,   232,   232,
     232,   233,   233,   234,   234,   235,   236,   236,   237,   237,
     237,   237,   237,   237,   237,   237,   237,   237,   237,   238,
     238,   239,   239,   240,   240,   241,   241,   242,   242,   243,
     243,   244,   244,   245,   245,   245,   246,   246,   247,   247,
     248,   249,   249,   250,   250,   250,   250,   250,   250,   250,
     250,   251,   251,   251,   251,   252,   252,   253,   253,   253,
     254,   254,   254,   254,   254,   254,   254,   254,   254,   254,
     254,   254,   254,   254,   254,   254,   254,   254,   254,   254,
     254,   254,   254,   255,   255,   255,   255,   256,   256,   257,
     257,   258,   259,   259,   259,   260,   260,   260,   261,   262,
     263,   263,   263,   263,   263,   264,   265,   265,   265,   265,
     265,   265,   265,   265,   265,   266,   266,   267,   267,   268,
     269,   270,   270,   270,   270,   271,   271,   271,   272,   272,
     273,   273,   274,   274,   275,   276,   276,   276,   276,   277,
     277,   278,   279,   279,   279,   280,   280,   280,   281,   281,
     281,   282,   283,   284,   284,   285,   285,   286,   286,   286,
     287,   287,   288,   288,   288,   288,   289,   289,   290,   290,
     291,   291,   292,   293,   294,   295,   295,   295,   296,   297,
     297,   297,   297,   297,   297,   298,   298,   298,   298,   298,
     298,   298,   298,   298,   298,   298,   298,   298,   298,   298,
     298,   298,   298,   298,   298,   298,   298,   298,   298,   298,
     298,   298,   298,   298,   298,   298,   298,   298,   298,   298,
     298,   298,   298,   298,   298,   298,   298,   298,   298,   299,
     299
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     1,     1,     1,     2,     2,     2,     3,     1,
       2,     3,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     3,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     2,     4,     3,     2,     2,
       4,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       2,     1,     1,     1,     0,     2,     0,     1,     2,     5,
       3,     2,     3,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     2,     2,     1,     1,     3,     5,     7,
       1,     3,     1,     1,     1,     1,     1,     1,     5,     1,
       3,     5,     0,     1,     3,     1,     1,     1,     2,     3,
       1,     1,     2,     1,     2,     3,     3,     1,     1,     1,
       0,     3,     1,     3,     3,     5,     2,     2,     4,     2,
       2,     3,     1,     4,     6,     4,     5,     6,     4,     3,
       1,     1,     1,     1,     2,     3,     5,     6,     6,     6,
       6,     6,     7,     6,     6,     6,     6,     8,     8,     0,
       2,     2,     2,     1,     3,     1,     4,     4,     3,     6,
       4,     4,     6,     3,     1,     3,     0,     3,     3,     3,
       3,     3,     3,     3,     3,     3,     3,     5,     3,     0,
       1,     0,     1,     1,     1,     1,     2,     1,     2,     1,
       2,     2,     3,     0,     1,     2,     1,     2,     1,     2,
       2,     0,     2,     4,     1,     4,     5,     1,     4,     4,
       5,     0,     1,     1,     1,     0,     1,     3,     3,     2,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     0,     2,     4,     4,     0,     2,     0,
       2,     2,     1,     1,     3,     2,     3,     4,     1,     2,
       0,     2,     3,     2,     2,     2,     0,     2,     3,     2,
       3,     2,     3,     3,     4,     1,     2,     2,     3,     2,
       2,     0,     1,     1,     2,     2,     3,     2,     1,     2,
       1,     2,     2,     2,     2,     0,     1,     3,     5,     2,
       3,     2,     0,     1,     2,     2,     2,     2,     4,     5,
       5,     3,     1,     2,     3,     3,     2,     2,     3,     3,
       0,     2,     1,     3,     3,     3,     0,     1,     1,     3,
       2,     3,     2,     2,     1,     0,     2,     4,     2,     1,
       3,     2,     4,     3,     5,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
       0,     3,   215,   219,   223,   217,   226,   228,   350,     0,
       0,    52,    53,    54,    55,    56,    71,    72,    73,    74,
      76,     0,     0,   231,   231,   231,   231,   231,   231,   179,
      75,    77,    78,   352,     0,     0,   110,     0,   364,    81,
      82,    83,     0,     0,   112,   113,   114,   115,   116,   117,
       0,   288,   296,   290,   277,   305,   277,   277,   311,   279,
     318,   320,   273,   325,   277,   332,     0,   106,   105,   101,
     102,   100,     0,     0,     0,   342,     0,     0,     0,     9,
       0,     2,     4,     0,    12,    18,    19,    33,    50,     0,
      34,    51,     0,    79,    93,    94,    22,    97,    99,    98,
      95,    96,    26,    27,    28,    29,    30,    31,    32,    13,
      15,    16,    17,    35,    14,     0,   163,    86,     0,   245,
       0,     0,     0,    21,    23,   250,   251,   252,   254,   253,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,    20,    24,
      25,    36,    88,   224,   221,   249,     0,     0,    91,     0,
       0,     0,     0,   150,   152,   416,   417,   418,   377,   375,
     378,   379,   415,   376,   381,   380,   384,   383,   382,   419,
     398,   399,   400,   401,   403,   404,   405,   406,   407,   408,
     409,   410,   411,   413,   412,   414,   385,   386,   387,   388,
     389,   390,   391,   392,   393,   394,   395,   396,   402,   397,
       0,   420,   119,     0,    69,     0,     0,     0,   237,   234,
     216,     0,   231,   218,   220,   223,   227,   229,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   352,     0,   356,     0,     0,     0,   362,   363,   282,
     283,   281,   277,   277,   277,   295,     0,     0,   289,   277,
       0,   285,   306,   277,   307,   309,   312,   313,   310,     0,
     317,   279,   315,   319,   321,   323,     0,   322,   326,   324,
     277,   329,   333,   331,   335,   336,   337,     0,     0,   103,
       0,     0,   343,   346,   347,     0,     0,    10,     1,     5,
       6,     7,   215,    57,    68,    63,    58,    59,    61,    60,
      64,    62,    38,    39,    42,    40,    41,    43,    80,   104,
      44,    45,    48,    46,    47,    49,   164,     0,    84,    87,
     131,   133,   139,   147,   138,   137,   365,   369,   246,     0,
     365,   146,   149,     0,    90,   225,   351,   247,   248,    92,
       0,     0,     0,     0,     0,     0,   122,     0,    67,     0,
       0,   241,     0,     0,   230,   232,   222,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   196,     0,     0,   209,   209,     0,     0,     0,
     165,     0,     0,   183,   180,    11,     0,     0,   353,   357,
     358,   354,   355,   111,   273,   277,   297,   277,   277,   301,
     277,   299,   291,   293,     0,   294,   277,   286,   278,   308,
     314,   280,   316,   274,     0,   330,   334,   107,     0,     0,
     341,   344,   345,   348,   349,     8,   134,     0,     0,     0,
      37,   132,     0,   368,     0,     0,   371,     0,   140,     0,
     160,   162,   161,   159,     0,     0,     0,     0,     0,     0,
     151,   130,   125,   127,   126,     0,     0,   123,   120,     0,
      70,     0,   242,   243,   244,     0,     0,     0,     0,   211,
     213,   214,   196,   196,   196,   196,   211,     0,     0,     0,
       0,     0,     0,     0,   209,   209,     0,     0,   196,   196,
     196,   210,   196,     0,     0,   196,   181,   182,   360,     0,
       0,   284,   298,   302,   277,   303,   300,   292,   287,     0,
       0,   327,     0,     0,   338,   135,   136,    85,   366,     0,
     373,   370,   148,     0,     0,    89,   155,   158,   153,     0,
       0,     0,   128,     0,     0,   118,     0,    65,     0,   233,
     235,     0,     0,   239,   238,   196,   212,     0,     0,     0,
       0,   208,     0,   198,   199,   201,   202,   205,   206,   203,
     204,   200,   166,     0,     0,     0,     0,     0,     0,     0,
       0,   361,   359,   304,   276,   275,     0,     0,   108,   339,
     340,     0,     0,   372,   142,     0,   145,   156,     0,     0,
     129,   124,   121,     0,   236,   240,     0,   169,   167,   170,
     171,   211,   197,   175,   176,   173,   174,     0,     0,     0,
       0,     0,   185,     0,     0,   168,   328,     0,   367,   374,
       0,   141,   154,   157,    66,   172,   207,     0,     0,     0,
       0,     0,     0,   209,     0,   109,   143,     0,     0,     0,
     188,     0,     0,   194,   177,   184,     0,   178,   186,   187,
       0,     0,   190,     0,   191,   209,     0,   195,   193,     0,
     189,   192
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    80,    81,    82,    83,   332,    85,    86,    87,    88,
     304,   470,    89,    90,    91,    92,    93,   440,   328,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   210,   466,   467,   329,   330,   331,
     333,   334,   534,   595,   109,   110,   111,   112,   113,   114,
     163,   164,   453,   115,   116,   240,   394,   621,   622,   624,
     652,   653,   497,   382,   500,   555,   501,   117,   118,   119,
     120,   154,   121,   122,   220,   221,   222,   475,   339,   123,
     124,   277,   261,   272,   125,   251,   126,   127,   128,   258,
     129,   255,   130,   131,   132,   133,   268,   134,   135,   136,
     137,   138,   279,   139,   140,   283,   141,   142,   143,   144,
     145,   146,   147,   157,   148,   398,   399,   400,   149,   150,
     151,   443,   335,   336,   211,   337
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -466
static const yytype_int16 yypact[] =
{
     359,  -466,    27,  -466,    23,  -466,  -466,  -466,    61,   -35,
    1139,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  1139,  1139,    36,    36,    36,    36,    36,    36,   135,
    -466,  -466,  -466,   809,    78,    56,  -466,    77,  -466,  -466,
    -466,  -466,    85,    97,  -466,  -466,  -466,  -466,  -466,  -466,
      86,  -466,   166,    54,    28,    83,   -63,   118,   -20,   -17,
     168,   129,   107,   171,    76,   190,   150,    25,  -466,  -466,
    -466,  -466,  1139,   282,   307,  -466,   201,   234,   -62,   270,
     317,   534,  -466,    12,  -466,  -466,  -466,  -466,  -466,   180,
    -466,  -466,   221,  1242,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,  -466,  -466,   221,   283,   151,   651,   255,
    1139,   651,  1139,  -466,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,   -16,   274,  -466,  -466,   152,   212,   151,   157,
     243,   245,   -36,  -466,   163,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,
     169,  -466,   161,   309,   306,   170,   172,   173,   174,  -466,
    -466,   300,    36,  -466,  -466,    23,  -466,  -466,   261,   177,
     178,   179,   182,   167,   183,   185,   188,   191,   200,   193,
      26,  -466,   175,   247,   265,   275,   249,  -466,  -466,  -466,
     250,  -466,    -8,   -45,    87,  -466,   -42,   277,  -466,   181,
     302,  -466,  -466,   118,  -466,  -466,  -466,   303,  -466,   313,
    -466,   258,  -466,  -466,  -466,  -466,   284,  -466,   231,  -466,
     118,  -466,   285,  -466,  -466,  -466,  -466,   318,   251,  -466,
     319,   321,   290,   291,  -466,   292,   293,  -466,  -466,  -466,
    -466,   361,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,     4,  -466,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,     2,   389,   151,
    -466,  -466,  -466,  -466,  -466,  -466,   279,   922,  -466,   391,
     279,  -466,  -466,   392,    -4,  -466,  -466,  -466,  -466,   151,
      30,   252,   256,   259,   192,   -25,    10,  1139,  -466,   260,
     304,   257,   338,    -3,  -466,  -466,  -466,   262,   -14,   -14,
     -14,   -14,   124,   263,   264,   266,   267,   273,   278,   287,
     288,   289,  -466,   -14,   -14,   -14,   -14,   294,   295,   -14,
    -466,   340,   342,  -466,  -466,  -466,   370,   369,  -466,   271,
    -466,  -466,  -466,  -466,   314,   118,  -466,   118,    93,  -466,
     118,  -466,  -466,  -466,   375,  -466,   118,  -466,  -466,  -466,
    -466,  -466,  -466,   -66,   344,  -466,  -466,   390,   296,   297,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,   298,   144,   651,
    -466,  -466,   -54,  -466,  1139,  1139,  -466,   651,   -23,   651,
    -466,  -466,  -466,  -466,    30,    30,    30,   299,   383,   386,
    -466,  -466,  -466,  -466,  -466,   -34,    94,  -466,   301,   387,
     310,   308,  -466,  -466,  -466,   315,   388,   -46,   316,   -14,
    -466,  -466,  -466,  -466,  -466,  -466,   -14,   393,   -14,   -14,
     -14,   -14,   -14,   -14,   -14,   -14,   -14,   -15,  -466,  -466,
    -466,  -466,  -466,   311,   312,  -466,  -466,  -466,  -466,   412,
     247,  -466,  -466,  -466,   118,  -466,  -466,  -466,  -466,   364,
     373,   322,   345,   407,    35,  -466,  -466,  -466,  -466,   413,
    -466,  1036,  -466,   417,    52,  -466,  -466,  -466,  -466,    30,
     336,   339,  -466,   425,    10,  -466,  1139,  -466,   343,  -466,
    -466,   341,   355,  -466,  -466,  -466,  -466,   -13,   -12,   -11,
      -7,  -466,   358,  -466,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,   167,    -5,    -1,     0,     3,   148,   408,
       6,  -466,  -466,  -466,  -466,  -466,   444,   360,  -466,  -466,
    -466,   362,  1139,  -466,  -466,   127,  -466,  -466,    30,    30,
    -466,  -466,  -466,   446,  -466,  -466,     7,  -466,  -466,  -466,
    -466,   -14,  -466,  -466,  -466,  -466,  -466,   365,   366,   367,
     368,   357,   372,   371,   398,  -466,  -466,   448,  -466,  -466,
     450,  -466,  -466,  -466,  -466,  -466,  -466,   -14,   -14,   -70,
     477,   522,   148,   -14,   525,  -466,  -466,   403,   404,   409,
    -466,   410,   405,   414,  -466,  -466,    74,  -466,  -466,  -466,
     -14,   -14,  -466,   477,  -466,   -14,   406,  -466,  -466,   420,
    -466,  -466
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -466,  -466,  -466,   496,   545,     5,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,   428,  -466,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,   492,     8,  -466,    45,  -148,  -296,  -466,
    -118,    57,  -466,  -466,    67,    88,    92,   102,   103,  -466,
     235,  -466,  -437,   476,  -466,  -466,  -466,   -49,  -466,  -466,
     -69,  -466,  -410,    24,  -380,  -465,  -355,  -466,  -466,  -466,
    -466,   374,  -466,  -466,   253,   376,  -466,  -466,  -466,  -466,
    -466,   196,   -55,   325,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,  -466,
    -466,  -466,  -466,  -466,  -466,  -466,  -466,    91,  -466,  -466,
     513,   268,  -466,   101,  -466,   -10
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -420
static const yytype_int16 yytable[] =
{
     212,   264,   265,   341,   344,    84,   502,   302,   -86,   281,
     349,   213,   214,   482,   483,   484,   485,   536,   537,   538,
     -87,   561,   572,  -144,   607,   608,   609,   528,   498,   499,
     610,    23,   613,   441,   505,   552,   614,   615,    84,   412,
     616,   649,   158,   625,   635,   353,   159,   542,   441,   287,
    -419,   294,   519,   441,   295,   461,   159,   302,   300,   160,
     161,   266,   212,   390,   391,   392,   260,   480,   407,   160,
     161,   481,   557,   558,   559,   560,   215,   216,   477,   408,
     289,    23,   413,   212,   260,   263,    84,   414,   574,   575,
     576,   462,   577,   288,   303,   580,   217,   312,   650,   520,
     296,   319,   597,   152,   267,   405,   463,   393,   244,   245,
     478,   450,   269,   529,   569,   570,   589,   218,   270,   271,
     320,   260,   553,   464,   556,   155,   451,   156,   354,   162,
     543,   556,   243,   563,   564,   565,   566,   567,   568,   162,
    -144,   571,   441,   452,   533,   606,   636,   153,   590,   219,
     573,   327,   573,   573,   573,   259,   306,   260,   573,   313,
     573,   632,   633,   327,   573,   573,   247,  -419,   573,   327,
     436,   573,   573,  -419,   465,   301,   256,   307,   248,   438,
     314,   308,   321,   257,   315,   302,     3,     4,     5,     6,
       7,   309,   310,    10,   316,   317,   262,   406,   409,   411,
     410,   249,   250,   322,   417,   260,   514,   323,   419,    23,
      24,    25,    26,    27,    28,   228,   260,   324,   325,   280,
     275,   340,   260,   342,   276,   425,   302,     3,     4,     5,
       6,     7,   229,   230,   231,   232,   233,   234,   235,   236,
     237,   238,   664,   246,   239,   617,   665,   260,   372,   273,
      23,    24,    25,    26,    27,    28,   556,   618,   619,   544,
     620,    38,   545,   656,   373,   374,   375,   376,   274,   377,
     378,   379,   380,   457,   347,   348,   381,   223,   224,   225,
     226,   227,   647,   648,   278,   669,   458,   459,   252,   253,
     486,   254,   630,   487,   290,   631,   284,   285,   286,   472,
     473,   474,   396,   282,   397,   666,   667,   387,   388,   416,
     260,   327,   526,   291,   292,   293,   297,   298,   327,    29,
     338,   527,   345,   350,   351,   346,   352,   446,   355,   532,
     357,   535,   437,   358,   359,   364,   356,   360,   395,   361,
     362,   363,   367,   368,   369,   370,   401,   468,   371,   383,
     512,   384,   513,   515,   385,   516,   402,   386,   415,   389,
       1,   518,   403,   404,     2,     3,     4,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,    16,    17,
      18,    19,    20,   418,   420,    21,    22,   269,    23,    24,
      25,    26,    27,    28,   421,    29,   424,   423,   426,   427,
     429,   428,   430,   431,   432,   433,   434,   435,    30,    31,
      32,    33,    34,   439,   442,   447,   449,   471,   454,   476,
      35,   506,   455,   507,   469,   456,   508,   509,   479,   488,
     489,   276,   490,   491,   530,   531,   510,    36,    37,   492,
      38,    39,    40,    41,   493,    42,    43,    44,    45,    46,
      47,    48,    49,   494,   495,   496,   517,   521,   522,   583,
     503,   504,   523,   524,   540,   539,   525,   541,   547,   551,
     546,   581,   623,    50,   562,   548,   549,   584,    51,    52,
      53,   578,   579,   550,   554,    54,   585,   586,   588,    55,
      56,    57,    58,    59,   591,   587,    60,    61,   594,    62,
      63,    64,   598,    65,    66,   599,   600,   603,    67,   604,
      68,    69,    70,    71,    72,    73,    74,    75,    76,    77,
      78,   593,    79,   605,   611,   626,   627,   634,   641,   645,
     628,   646,   637,   638,   639,   640,   602,   642,   643,     2,
       3,     4,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    19,    20,   651,   654,
      21,    22,   657,    23,    24,    25,    26,    27,    28,   644,
      29,   658,   659,   662,   670,   660,   661,   299,   242,   663,
     343,   305,   629,    30,    31,    32,    33,    34,   671,   601,
     460,   596,   326,   655,   668,    35,   422,   612,   365,   366,
     511,   582,   311,     0,     0,     0,     0,     0,   448,     0,
       0,     0,    36,    37,     0,    38,    39,    40,    41,     0,
      42,    43,    44,    45,    46,    47,    48,    49,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    50,     0,
       0,     0,     0,    51,    52,    53,   302,     0,     0,     0,
      54,     0,     0,     0,    55,    56,    57,    58,    59,     0,
       0,    60,    61,     0,    62,    63,    64,     0,    65,    66,
      23,     0,     0,    67,     0,    68,    69,    70,    71,    72,
      73,    74,    75,    76,    77,    78,     0,    79,     0,     0,
     165,   166,   167,   168,   169,   170,     0,     0,     0,   171,
       0,   172,   173,   174,   175,   176,   177,   178,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   179,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   180,   181,
     182,   183,   184,   185,   186,   187,   188,   189,   190,   191,
     192,   193,   194,   195,     0,     0,   196,   197,     0,   198,
       0,     0,     0,   199,   200,     0,   201,     0,   202,   203,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     204,     0,     0,     0,   205,     0,     0,     0,   206,   207,
       0,   208,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   209,     2,     3,     4,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,    16,    17,
      18,    19,    20,     0,     0,    21,    22,     0,    23,    24,
      25,    26,    27,    28,     0,    29,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    30,    31,
      32,   241,    34,     0,     0,     0,     0,     0,     0,     0,
      35,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    36,    37,     0,
      38,    39,    40,    41,     0,    42,    43,    44,    45,    46,
      47,    48,    49,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    50,     0,     0,     0,     0,    51,    52,
      53,     0,     0,     0,     0,    54,     0,     0,     0,    55,
      56,    57,    58,    59,     0,     0,    60,    61,     0,    62,
      63,    64,     0,    65,    66,     0,     0,     0,    67,     0,
      68,    69,    70,    71,    72,    73,    74,    75,    76,    77,
      78,   165,   166,   167,   168,   169,   170,     0,     0,     0,
     171,     0,   172,   173,   174,   175,   176,   177,   178,   444,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   179,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   180,
     181,   182,   183,   184,   185,   186,   187,   188,   189,   190,
     191,   192,   193,   194,   195,     0,     0,   196,   197,     0,
     198,     0,     0,     0,   199,   200,     0,   201,     0,   202,
     203,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   204,     0,     0,     0,   205,     0,     0,     0,   206,
     207,     0,   208,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   209,   165,   166,   167,   168,   169,
     170,   445,     0,     0,   171,     0,   172,   173,   174,   175,
     176,   177,   178,   592,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   179,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   180,   181,   182,   183,   184,   185,   186,
     187,   188,   189,   190,   191,   192,   193,   194,   195,     0,
       0,   196,   197,     0,   198,     0,     0,     0,   199,   200,
       0,   201,     0,   202,   203,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   204,     0,     0,     0,   205,
       0,     0,     0,   206,   207,     0,   208,     0,   165,   166,
     167,   168,   169,   170,     0,     0,     0,   171,   209,   172,
     173,   174,   175,   176,   177,   178,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     179,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   180,   181,   182,   183,
     184,   185,   186,   187,   188,   189,   190,   191,   192,   193,
     194,   195,     0,     0,   196,   197,     0,   198,     0,     0,
       0,   199,   200,     0,   201,     0,   202,   203,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   204,     0,
       0,     0,   205,     0,     0,     0,   206,   207,     0,   208,
       0,   165,   166,   167,   168,   169,   170,     0,     0,     0,
     171,   209,   172,   173,   174,   175,   176,   177,   178,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   318,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   180,
     181,   182,   183,   184,   185,   186,   187,   188,   189,   190,
     191,   192,   193,   194,   195,     0,     0,   196,   197,     0,
     198,     0,     0,     0,   199,   200,     0,   201,     0,   202,
     203,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   204,     0,     0,     0,   205,     0,     0,     0,   206,
     207,     0,   208,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   209
};

static const yytype_int16 yycheck[] =
{
      10,    56,    57,   121,   152,     0,   386,     5,    24,    64,
     158,    21,    22,   368,   369,   370,   371,   454,   455,   456,
      24,   486,    37,    46,    37,    37,    37,    81,   383,   384,
      37,    29,    37,   329,   389,    81,    37,    37,    33,    81,
      37,   111,    77,    37,    37,    81,    81,    81,   344,    24,
      46,   113,   118,   349,   116,    45,    81,     5,    46,    94,
      95,    81,    72,    37,    38,    39,   129,    81,   113,    94,
      95,    85,   482,   483,   484,   485,    40,    41,    81,   124,
      72,    29,   124,    93,   129,   148,    81,   129,   498,   499,
     500,    81,   502,    68,    89,   505,    60,    92,   168,   165,
     162,    93,   539,    76,   124,   113,    96,    81,    52,    53,
     113,    81,   129,   167,   494,   495,    81,    81,   135,   136,
     115,   129,   168,   113,   479,    64,    96,    66,   164,   164,
     164,   486,    54,   488,   489,   490,   491,   492,   493,   164,
     163,   496,   438,   113,   167,   555,   611,   124,   113,   113,
     165,   167,   165,   165,   165,   127,    89,   129,   165,    92,
     165,   598,   599,   167,   165,   165,    81,   163,   165,   167,
     168,   165,   165,   169,   164,   163,   122,    89,    81,   327,
      92,    89,   115,   129,    92,     5,     6,     7,     8,     9,
      10,    89,    89,    13,    92,    92,   113,   252,   253,   254,
     113,   115,   116,   115,   259,   129,   113,   115,   263,    29,
      30,    31,    32,    33,    34,    80,   129,   115,   115,   143,
     113,   120,   129,   122,   117,   280,     5,     6,     7,     8,
       9,    10,    97,    98,    99,   100,   101,   102,   103,   104,
     105,   106,   168,   166,   109,    97,   172,   129,    81,    81,
      29,    30,    31,    32,    33,    34,   611,   109,   110,   165,
     112,    81,   168,   643,    97,    98,    99,   100,   139,   102,
     103,   104,   105,    81,    62,    63,   109,    24,    25,    26,
      27,    28,   637,   638,   113,   665,    94,    95,   122,   123,
     166,   125,   165,   169,    12,   168,   146,   147,   148,    42,
      43,    44,    55,   113,    57,   660,   661,   107,   108,   128,
     129,   167,   168,     6,   113,    81,    46,     0,   167,    36,
      65,   439,    48,   166,    81,   173,    81,   337,   165,   447,
     169,   449,   327,    24,    28,    35,   167,   167,   163,   167,
     167,   167,    81,   166,   166,   166,    81,   357,   166,   166,
     405,   166,   407,   408,   166,   410,    81,   166,    81,   166,
       1,   416,   113,   113,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,    16,    17,    18,    19,    20,
      21,    22,    23,    81,    81,    26,    27,   129,    29,    30,
      31,    32,    33,    34,    81,    36,   165,   113,   113,    81,
      81,   150,    81,   113,   113,   113,   113,    46,    49,    50,
      51,    52,    53,    24,   135,    24,    24,   113,   166,    81,
      61,    81,   166,    81,   164,   166,    56,    58,   166,   166,
     166,   117,   166,   166,   444,   445,   165,    78,    79,   166,
      81,    82,    83,    84,   166,    86,    87,    88,    89,    90,
      91,    92,    93,   166,   166,   166,    81,   113,    68,   514,
     166,   166,   166,   166,    81,   166,   168,    81,    81,    81,
     169,    59,    64,   114,    81,   165,   168,   113,   119,   120,
     121,   170,   170,   168,   168,   126,   113,   165,    81,   130,
     131,   132,   133,   134,    81,   150,   137,   138,    81,   140,
     141,   142,   166,   144,   145,   166,    81,   164,   149,   168,
     151,   152,   153,   154,   155,   156,   157,   158,   159,   160,
     161,   531,   163,   168,   166,    81,   166,    81,   171,    81,
     168,    81,   167,   167,   167,   167,   546,   165,   167,     5,
       6,     7,     8,     9,    10,    11,    12,    13,    14,    15,
      16,    17,    18,    19,    20,    21,    22,    23,    81,    37,
      26,    27,    37,    29,    30,    31,    32,    33,    34,   171,
      36,   168,   168,   168,   168,   166,   166,    81,    33,   165,
     152,    89,   592,    49,    50,    51,    52,    53,   168,   544,
     355,   534,   116,   642,   663,    61,   271,   573,   222,   225,
     404,   510,    89,    -1,    -1,    -1,    -1,    -1,   340,    -1,
      -1,    -1,    78,    79,    -1,    81,    82,    83,    84,    -1,
      86,    87,    88,    89,    90,    91,    92,    93,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   114,    -1,
      -1,    -1,    -1,   119,   120,   121,     5,    -1,    -1,    -1,
     126,    -1,    -1,    -1,   130,   131,   132,   133,   134,    -1,
      -1,   137,   138,    -1,   140,   141,   142,    -1,   144,   145,
      29,    -1,    -1,   149,    -1,   151,   152,   153,   154,   155,
     156,   157,   158,   159,   160,   161,    -1,   163,    -1,    -1,
      49,    50,    51,    52,    53,    54,    -1,    -1,    -1,    58,
      -1,    60,    61,    62,    63,    64,    65,    66,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    81,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    97,    98,
      99,   100,   101,   102,   103,   104,   105,   106,   107,   108,
     109,   110,   111,   112,    -1,    -1,   115,   116,    -1,   118,
      -1,    -1,    -1,   122,   123,    -1,   125,    -1,   127,   128,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     139,    -1,    -1,    -1,   143,    -1,    -1,    -1,   147,   148,
      -1,   150,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   162,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,    16,    17,    18,    19,    20,
      21,    22,    23,    -1,    -1,    26,    27,    -1,    29,    30,
      31,    32,    33,    34,    -1,    36,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    49,    50,
      51,    52,    53,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      61,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    78,    79,    -1,
      81,    82,    83,    84,    -1,    86,    87,    88,    89,    90,
      91,    92,    93,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   114,    -1,    -1,    -1,    -1,   119,   120,
     121,    -1,    -1,    -1,    -1,   126,    -1,    -1,    -1,   130,
     131,   132,   133,   134,    -1,    -1,   137,   138,    -1,   140,
     141,   142,    -1,   144,   145,    -1,    -1,    -1,   149,    -1,
     151,   152,   153,   154,   155,   156,   157,   158,   159,   160,
     161,    49,    50,    51,    52,    53,    54,    -1,    -1,    -1,
      58,    -1,    60,    61,    62,    63,    64,    65,    66,    67,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    81,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    97,
      98,    99,   100,   101,   102,   103,   104,   105,   106,   107,
     108,   109,   110,   111,   112,    -1,    -1,   115,   116,    -1,
     118,    -1,    -1,    -1,   122,   123,    -1,   125,    -1,   127,
     128,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   139,    -1,    -1,    -1,   143,    -1,    -1,    -1,   147,
     148,    -1,   150,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   162,    49,    50,    51,    52,    53,
      54,   169,    -1,    -1,    58,    -1,    60,    61,    62,    63,
      64,    65,    66,    67,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    81,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    97,    98,    99,   100,   101,   102,   103,
     104,   105,   106,   107,   108,   109,   110,   111,   112,    -1,
      -1,   115,   116,    -1,   118,    -1,    -1,    -1,   122,   123,
      -1,   125,    -1,   127,   128,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   139,    -1,    -1,    -1,   143,
      -1,    -1,    -1,   147,   148,    -1,   150,    -1,    49,    50,
      51,    52,    53,    54,    -1,    -1,    -1,    58,   162,    60,
      61,    62,    63,    64,    65,    66,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      81,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    97,    98,    99,   100,
     101,   102,   103,   104,   105,   106,   107,   108,   109,   110,
     111,   112,    -1,    -1,   115,   116,    -1,   118,    -1,    -1,
      -1,   122,   123,    -1,   125,    -1,   127,   128,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   139,    -1,
      -1,    -1,   143,    -1,    -1,    -1,   147,   148,    -1,   150,
      -1,    49,    50,    51,    52,    53,    54,    -1,    -1,    -1,
      58,   162,    60,    61,    62,    63,    64,    65,    66,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    81,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    97,
      98,    99,   100,   101,   102,   103,   104,   105,   106,   107,
     108,   109,   110,   111,   112,    -1,    -1,   115,   116,    -1,
     118,    -1,    -1,    -1,   122,   123,    -1,   125,    -1,   127,
     128,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   139,    -1,    -1,    -1,   143,    -1,    -1,    -1,   147,
     148,    -1,   150,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   162
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     1,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    19,    20,    21,    22,
      23,    26,    27,    29,    30,    31,    32,    33,    34,    36,
      49,    50,    51,    52,    53,    61,    78,    79,    81,    82,
      83,    84,    86,    87,    88,    89,    90,    91,    92,    93,
     114,   119,   120,   121,   126,   130,   131,   132,   133,   134,
     137,   138,   140,   141,   142,   144,   145,   149,   151,   152,
     153,   154,   155,   156,   157,   158,   159,   160,   161,   163,
     175,   176,   177,   178,   179,   180,   181,   182,   183,   186,
     187,   188,   189,   190,   193,   194,   195,   196,   197,   198,
     199,   200,   201,   202,   203,   204,   205,   206,   207,   218,
     219,   220,   221,   222,   223,   227,   228,   241,   242,   243,
     244,   246,   247,   253,   254,   258,   260,   261,   262,   264,
     266,   267,   268,   269,   271,   272,   273,   274,   275,   277,
     278,   280,   281,   282,   283,   284,   285,   286,   288,   292,
     293,   294,    76,   124,   245,    64,    66,   287,    77,    81,
      94,    95,   164,   224,   225,    49,    50,    51,    52,    53,
      54,    58,    60,    61,    62,    63,    64,    65,    66,    81,
      97,    98,    99,   100,   101,   102,   103,   104,   105,   106,
     107,   108,   109,   110,   111,   112,   115,   116,   118,   122,
     123,   125,   127,   128,   139,   143,   147,   148,   150,   162,
     208,   298,   299,   299,   299,    40,    41,    60,    81,   113,
     248,   249,   250,   248,   248,   248,   248,   248,    80,    97,
      98,    99,   100,   101,   102,   103,   104,   105,   106,   109,
     229,    52,   178,    54,    52,    53,   166,    81,    81,   115,
     116,   259,   122,   123,   125,   265,   122,   129,   263,   127,
     129,   256,   113,   148,   256,   256,    81,   124,   270,   129,
     135,   136,   257,    81,   139,   113,   117,   255,   113,   276,
     143,   256,   113,   279,   146,   147,   148,    24,    68,   208,
      12,     6,   113,    81,   113,   116,   162,    46,     0,   177,
      46,   163,     5,   179,   184,   207,   218,   219,   220,   221,
     222,   294,   179,   218,   219,   220,   221,   222,    81,   208,
     179,   218,   219,   220,   221,   222,   227,   167,   192,   211,
     212,   213,   179,   214,   215,   296,   297,   299,    65,   252,
     297,   214,   297,   192,   211,    48,   173,    62,    63,   211,
     166,    81,    81,    81,   164,   165,   167,   169,    24,    28,
     167,   167,   167,   167,    35,   249,   245,    81,   166,   166,
     166,   166,    81,    97,    98,    99,   100,   102,   103,   104,
     105,   109,   237,   166,   166,   166,   166,   107,   108,   166,
      37,    38,    39,    81,   230,   163,    55,    57,   289,   290,
     291,    81,    81,   113,   113,   113,   256,   113,   124,   256,
     113,   256,    81,   124,   129,    81,   128,   256,    81,   256,
      81,    81,   257,   113,   165,   256,   113,    81,   150,    81,
      81,   113,   113,   113,   113,    46,   168,   179,   211,    24,
     191,   212,   135,   295,    67,   169,   299,    24,   295,    24,
      81,    96,   113,   226,   166,   166,   166,    81,    94,    95,
     224,    45,    81,    96,   113,   164,   209,   210,   299,   164,
     185,   113,    42,    43,    44,   251,    81,    81,   113,   166,
      81,    85,   240,   240,   240,   240,   166,   169,   166,   166,
     166,   166,   166,   166,   166,   166,   166,   236,   240,   240,
     238,   240,   238,   166,   166,   240,    81,    81,    56,    58,
     165,   255,   256,   256,   113,   256,   256,    81,   256,   118,
     165,   113,    68,   166,   166,   168,   168,   214,    81,   167,
     299,   299,   214,   167,   216,   214,   226,   226,   226,   166,
      81,    81,    81,   164,   165,   168,   169,    81,   165,   168,
     168,    81,    81,   168,   168,   239,   240,   236,   236,   236,
     236,   239,    81,   240,   240,   240,   240,   240,   240,   238,
     238,   240,    37,   165,   236,   236,   236,   236,   170,   170,
     236,    59,   291,   256,   113,   113,   165,   150,    81,    81,
     113,    81,    67,   299,    81,   217,   215,   226,   166,   166,
      81,   210,   299,   164,   168,   168,   236,    37,    37,    37,
      37,   166,   237,    37,    37,    37,    37,    97,   109,   110,
     112,   231,   232,    64,   233,    37,    81,   166,   168,   299,
     165,   168,   226,   226,    81,    37,   239,   167,   167,   167,
     167,   171,   165,   167,   171,    81,    81,   240,   240,   111,
     168,    81,   234,   235,    37,   231,   238,    37,   168,   168,
     166,   166,   168,   165,   168,   172,   240,   240,   234,   238,
     168,   168
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
                        ObProxyTextPsExecuteParseNode *node = NULL;
                        malloc_execute_parse_node(node);
                        node->str_value_ = (yyvsp[(2) - (2)].str);
                        add_text_ps_execute_node(result->text_ps_execute_parse_info_, node);
                      ;}
    break;

  case 66:

    {
                        ObProxyTextPsExecuteParseNode *node = NULL;
                        malloc_execute_parse_node(node);
                        node->str_value_ = (yyvsp[(4) - (4)].str);
                        add_text_ps_execute_node(result->text_ps_execute_parse_info_, node);
                      ;}
    break;

  case 67:

    {
                      result->text_ps_inner_stmt_type_ = OBPROXY_T_TEXT_PS_PREPARE;
                      result->text_ps_name_ = (yyvsp[(2) - (3)].str);
                    ;}
    break;

  case 68:

    {
            ;}
    break;

  case 69:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
              result->text_ps_name_ = (yyvsp[(2) - (2)].str);
            ;}
    break;

  case 70:

    {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
              result->text_ps_name_ = (yyvsp[(2) - (4)].str);
            ;}
    break;

  case 71:

    { result->cur_stmt_type_ = OBPROXY_T_GRANT; ;}
    break;

  case 72:

    { result->cur_stmt_type_ = OBPROXY_T_REVOKE; ;}
    break;

  case 73:

    { result->cur_stmt_type_ = OBPROXY_T_ANALYZE; ;}
    break;

  case 74:

    { result->cur_stmt_type_ = OBPROXY_T_PURGE; ;}
    break;

  case 75:

    { result->cur_stmt_type_ = OBPROXY_T_FLASHBACK; ;}
    break;

  case 76:

    { result->cur_stmt_type_ = OBPROXY_T_COMMENT; ;}
    break;

  case 77:

    { result->cur_stmt_type_ = OBPROXY_T_AUDIT; ;}
    break;

  case 78:

    { result->cur_stmt_type_ = OBPROXY_T_NOAUDIT; ;}
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

  case 88:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_TX_RO; ;}
    break;

  case 91:

    { result->cur_stmt_type_ = OBPROXY_T_SET_AC_0; ;}
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

  case 96:

    {;}
    break;

  case 97:

    {;}
    break;

  case 98:

    {;}
    break;

  case 99:

    {;}
    break;

  case 100:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_DATABASE; ;}
    break;

  case 101:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DATABASES; ;}
    break;

  case 102:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLES; ;}
    break;

  case 103:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE; ;}
    break;

  case 104:

    {
                      result->cur_stmt_type_ = OBPROXY_T_DESC;
                      result->sub_stmt_type_ = OBPROXY_T_SUB_DESC_TABLE;
                  ;}
    break;

  case 105:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DB_VERSION; ;}
    break;

  case 106:

    { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY; ;}
    break;

  case 107:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY;
                 ;}
    break;

  case 108:

    {
                     SET_ICMD_SECOND_STRING((yyvsp[(5) - (5)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY;
                 ;}
    break;

  case 109:

    {
                     SET_ICMD_ONE_STRING((yyvsp[(3) - (7)].str));
                     SET_ICMD_SECOND_STRING((yyvsp[(7) - (7)].str));
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY;
                 ;}
    break;

  case 110:

    { result->cur_stmt_type_ = OBPROXY_T_SELECT_ROUTE_ADDR; ;}
    break;

  case 111:

    {
                              result->cur_stmt_type_ = OBPROXY_T_SET_ROUTE_ADDR;
                              result->cmd_info_.integer_[0] = (yyvsp[(3) - (3)].num);
                           ;}
    break;

  case 112:

    {;}
    break;

  case 113:

    {;}
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

  case 119:

    {
                   result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                 ;}
    break;

  case 120:

    {
                   result->table_info_.package_name_ = (yyvsp[(1) - (3)].str);
                   result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                 ;}
    break;

  case 121:

    {
                   result->table_info_.database_name_ = (yyvsp[(1) - (5)].str);
                   result->table_info_.package_name_ = (yyvsp[(3) - (5)].str);
                   result->table_info_.table_name_ = (yyvsp[(5) - (5)].str);
                 ;}
    break;

  case 122:

    {
                result->call_parse_info_.node_count_ = 0;
              ;}
    break;

  case 123:

    {
                result->call_parse_info_.node_count_ = 0;
                add_call_node(result->call_parse_info_, (yyvsp[(1) - (1)].node));
              ;}
    break;

  case 124:

    {
                add_call_node(result->call_parse_info_, (yyvsp[(3) - (3)].node));
              ;}
    break;

  case 125:

    {
            malloc_call_node((yyval.node), CALL_TOKEN_STR_VAL);
            (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 126:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_INT_VAL);
           (yyval.node)->int_value_ = (yyvsp[(1) - (1)].num);
         ;}
    break;

  case 127:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_NUMBER_VAL);
           (yyval.node)->str_value_ = (yyvsp[(1) - (1)].str);
         ;}
    break;

  case 128:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_USER_VAR);
           (yyval.node)->str_value_ = (yyvsp[(2) - (2)].str);
         ;}
    break;

  case 129:

    {
           malloc_call_node((yyval.node), CALL_TOKEN_SYS_VAR);
           (yyval.node)->str_value_ = (yyvsp[(3) - (3)].str);
         ;}
    break;

  case 130:

    {
           result->placeholder_list_idx_++;
           malloc_call_node((yyval.node), CALL_TOKEN_PLACE_HOLDER);
           (yyval.node)->placeholder_idx_ = result->placeholder_list_idx_ - 1;
         ;}
    break;

  case 144:

    {
                                                                  handle_stmt_end(result);
                                                                  HANDLE_ACCEPT();
                                                                ;}
    break;

  case 149:

    {
                                                 handle_stmt_end(result);
                                                 HANDLE_ACCEPT();
                                               ;}
    break;

  case 153:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_USER);
        ;}
    break;

  case 154:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 155:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 156:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(5) - (5)].var_node), (yyvsp[(3) - (5)].str), SET_VAR_SYS);
        ;}
    break;

  case 157:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(6) - (6)].var_node), (yyvsp[(4) - (6)].str), SET_VAR_SYS);
        ;}
    break;

  case 158:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(4) - (4)].var_node), (yyvsp[(2) - (4)].str), SET_VAR_SYS);
        ;}
    break;

  case 159:

    {
          add_set_var_node(result->set_parse_info_, (yyvsp[(3) - (3)].var_node), (yyvsp[(1) - (3)].str), SET_VAR_SYS);
        ;}
    break;

  case 160:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_STR);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 161:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_INT);
               (yyval.var_node)->int_value_ = (yyvsp[(1) - (1)].num);
             ;}
    break;

  case 162:

    {
               malloc_set_var_node((yyval.var_node), SET_VALUE_TYPE_NUMBER);
               (yyval.var_node)->str_value_ = (yyvsp[(1) - (1)].str);
             ;}
    break;

  case 165:

    {;}
    break;

  case 166:

    {;}
    break;

  case 167:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 168:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 169:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 170:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 171:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 172:

    {
              malloc_shard_column_node((yyval.shard_node), (yyvsp[(2) - (7)].str), (yyvsp[(3) - (7)].str), DBMESH_TOKEN_STR_VAL);
              (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (7)].str);
              add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
            ;}
    break;

  case 173:

    { result->trace_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 174:

    { result->rpc_id_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 175:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 176:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(4) - (6)].str); ;}
    break;

  case 177:

    {;}
    break;

  case 178:

    {;}
    break;

  case 179:

    {;}
    break;

  case 181:

    { result->has_simple_route_info_ = true; result->simple_route_info_.table_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 182:

    { result->simple_route_info_.part_key_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 186:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.group_idx_str_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 187:

    {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.table_name_ = (yyvsp[(3) - (4)].str);
            ;}
    break;

  case 188:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 189:

    { result->dbp_route_info_.scan_all_ = true; ;}
    break;

  case 190:

    {result->dbp_route_info_.has_shard_key_ = true;;}
    break;

  case 191:

    { result->trace_id_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 192:

    { result->trace_id_ = (yyvsp[(3) - (6)].str); result->rpc_id_ = (yyvsp[(5) - (6)].str); ;}
    break;

  case 193:

    {;}
    break;

  case 195:

    {
                   if (result->dbp_route_info_.shard_key_count_ < OBPROXY_MAX_DBP_SHARD_KEY_NUM) {
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].left_str_ = (yyvsp[(1) - (3)].str);
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].right_str_ = (yyvsp[(3) - (3)].str);
                     ++result->dbp_route_info_.shard_key_count_;
                   }
                 ;}
    break;

  case 198:

    { result->dbmesh_route_info_.group_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 199:

    { result->dbmesh_route_info_.tb_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 200:

    { result->dbmesh_route_info_.table_name_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 201:

    { result->dbmesh_route_info_.es_idx_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 202:

    { result->dbmesh_route_info_.testload_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 203:

    { result->trace_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 204:

    { result->rpc_id_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 205:

    { result->dbmesh_route_info_.tnt_id_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 206:

    { result->dbmesh_route_info_.disaster_status_str_ = (yyvsp[(3) - (3)].str); ;}
    break;

  case 207:

    {
             malloc_shard_column_node((yyval.shard_node), (yyvsp[(1) - (5)].str), (yyvsp[(3) - (5)].str), DBMESH_TOKEN_STR_VAL);
             (yyval.shard_node)->col_str_value_ = (yyvsp[(5) - (5)].str);
             add_shard_column_node(result->dbmesh_route_info_, (yyval.shard_node));
           ;}
    break;

  case 208:

    {;}
    break;

  case 209:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 211:

    { (yyval.str).str_ = NULL; (yyval.str).str_len_ = 0; ;}
    break;

  case 233:

    { result->query_timeout_ = (yyvsp[(3) - (4)].num); ;}
    break;

  case 234:

    {;}
    break;

  case 236:

    {
      add_hint_index(result->dbmesh_route_info_, (yyvsp[(3) - (5)].str));
      result->dbmesh_route_info_.index_count_++;
    ;}
    break;

  case 237:

    {;}
    break;

  case 238:

    {;}
    break;

  case 239:

    {;}
    break;

  case 240:

    {;}
    break;

  case 241:

    {;}
    break;

  case 242:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_WEAK); ;}
    break;

  case 243:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_STRONG); ;}
    break;

  case 244:

    { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_FROZEN); ;}
    break;

  case 247:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_WARNINGS; ;}
    break;

  case 248:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_ERRORS; ;}
    break;

  case 249:

    { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; ;}
    break;

  case 273:

    {
;}
    break;

  case 274:

    {
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (2)].num);/*row*/
;}
    break;

  case 275:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(2) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(4) - (4)].num);/*row*/
;}
    break;

  case 276:

    {
   result->cmd_info_.integer_[1] = (yyvsp[(4) - (4)].num);/*offset*/
   result->cmd_info_.integer_[2] = (yyvsp[(2) - (4)].num);/*row*/
;}
    break;

  case 277:

    {;}
    break;

  case 278:

    { result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 279:

    {;}
    break;

  case 280:

    { result->cmd_info_.string_[1] = (yyvsp[(2) - (2)].str);;}
    break;

  case 282:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); ;}
    break;

  case 283:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); ;}
    break;

  case 284:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 285:

    {;}
    break;

  case 286:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); ;}
    break;

  case 287:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); ;}
    break;

  case 288:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); ;}
    break;

  case 290:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);;}
    break;

  case 291:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, (yyvsp[(2) - (2)].str));;}
    break;

  case 292:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, (yyvsp[(3) - (3)].str));;}
    break;

  case 293:

    {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);;}
    break;

  case 294:

    {result->cmd_info_.string_[0] = (yyvsp[(2) - (2)].str);;}
    break;

  case 296:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); ;}
    break;

  case 297:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); ;}
    break;

  case 298:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 299:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); ;}
    break;

  case 300:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 301:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); ;}
    break;

  case 302:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, (yyvsp[(2) - (3)].num)); ;}
    break;

  case 303:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); ;}
    break;

  case 304:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, (yyvsp[(3) - (4)].num)); ;}
    break;

  case 305:

    {;}
    break;

  case 306:

    { SET_ICMD_ONE_ID((yyvsp[(2) - (2)].num)); ;}
    break;

  case 307:

    {;}
    break;

  case 308:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 309:

    {;}
    break;

  case 311:

    {;}
    break;

  case 312:

    { SET_ICMD_ONE_STRING((yyvsp[(1) - (1)].str)); ;}
    break;

  case 313:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);;}
    break;

  case 314:

    { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, (yyvsp[(2) - (2)].str));;}
    break;

  case 315:

    {;}
    break;

  case 316:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); ;}
    break;

  case 317:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); ;}
    break;

  case 318:

    {;}
    break;

  case 319:

    { SET_ICMD_ONE_STRING((yyvsp[(2) - (2)].str)); ;}
    break;

  case 320:

    {;}
    break;

  case 321:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); ;}
    break;

  case 322:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); ;}
    break;

  case 323:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 325:

    {;}
    break;

  case 326:

    { SET_ICMD_SECOND_ID((yyvsp[(1) - (1)].num)); ;}
    break;

  case 327:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (3)].num), (yyvsp[(1) - (3)].num)); ;}
    break;

  case 328:

    { SET_ICMD_TWO_ID((yyvsp[(3) - (5)].num), (yyvsp[(1) - (5)].num)); SET_ICMD_ONE_STRING((yyvsp[(5) - (5)].str)); ;}
    break;

  case 329:

    {;}
    break;

  case 330:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); ;}
    break;

  case 332:

    {;}
    break;

  case 333:

    { SET_ICMD_ONE_ID((yyvsp[(1) - (1)].num));  ;}
    break;

  case 334:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, (yyvsp[(1) - (2)].num),(yyvsp[(2) - (2)].num)); ;}
    break;

  case 335:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); ;}
    break;

  case 336:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); ;}
    break;

  case 337:

    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); ;}
    break;

  case 338:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (4)].str)); ;}
    break;

  case 339:

    { SET_ICMD_TWO_STRING((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].str)); ;}
    break;

  case 340:

    { SET_ICMD_CONFIG_INT_VALUE((yyvsp[(3) - (5)].str), (yyvsp[(5) - (5)].num)); ;}
    break;

  case 341:

    { SET_ICMD_ONE_STRING((yyvsp[(3) - (3)].str)); ;}
    break;

  case 342:

    {;}
    break;

  case 343:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 344:

    { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, (yyvsp[(2) - (3)].num), (yyvsp[(3) - (3)].num)); ;}
    break;

  case 345:

    {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, (yyvsp[(2) - (3)].str),(yyvsp[(3) - (3)].num));;}
    break;

  case 346:

    {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, (yyvsp[(2) - (2)].str));;}
    break;

  case 347:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(2) - (2)].num)); ;}
    break;

  case 348:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 349:

    { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, (yyvsp[(3) - (3)].num)); ;}
    break;

  case 352:

    {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              ;}
    break;

  case 353:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 354:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 355:

    { result->cur_stmt_type_ = OBPROXY_T_BEGIN; ;}
    break;

  case 362:

    {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = (yyvsp[(2) - (2)].str);
                          ;}
    break;

  case 363:

    { result->cur_stmt_type_ = OBPROXY_T_HELP; ;}
    break;

  case 365:

    {;}
    break;

  case 366:

    { result->part_name_ = (yyvsp[(2) - (2)].str); ;}
    break;

  case 367:

    { result->part_name_ = (yyvsp[(3) - (4)].str); ;}
    break;

  case 368:

    {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT();
                                                ;}
    break;

  case 369:

    {
                          result->table_info_.table_name_ = (yyvsp[(1) - (1)].str);
                        ;}
    break;

  case 370:

    {
                                      result->table_info_.database_name_ = (yyvsp[(1) - (3)].str);
                                      result->table_info_.table_name_ = (yyvsp[(3) - (3)].str);
                                    ;}
    break;

  case 371:

    {
                                    UPDATE_ALIAS_NAME((yyvsp[(2) - (2)].str));
                                    result->table_info_.table_name_ = (yyvsp[(1) - (2)].str);
                                  ;}
    break;

  case 372:

    {
                                                UPDATE_ALIAS_NAME((yyvsp[(4) - (4)].str));
                                                result->table_info_.database_name_ = (yyvsp[(1) - (4)].str);
                                                result->table_info_.table_name_ = (yyvsp[(3) - (4)].str);
                                              ;}
    break;

  case 373:

    {
                                      UPDATE_ALIAS_NAME((yyvsp[(3) - (3)].str));
                                      result->table_info_.table_name_ = (yyvsp[(1) - (3)].str);
                                    ;}
    break;

  case 374:

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

