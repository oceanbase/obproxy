
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
