
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
