
/* A Bison parser, made by GNU Bison 2.4.1.  */

/* Skeleton interface for Bison's Yacc-like parsers in C
   
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
