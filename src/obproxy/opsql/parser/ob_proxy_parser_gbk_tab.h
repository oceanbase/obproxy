
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
     UNIQUE = 275,
     GRANT = 276,
     REVOKE = 277,
     ANALYZE = 278,
     PURGE = 279,
     COMMENT = 280,
     FROM = 281,
     DUAL = 282,
     JOIN = 283,
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
     LOAD_DATA_HINT_BEGIN = 294,
     HINT_END = 295,
     COMMENT_BEGIN = 296,
     COMMENT_END = 297,
     ROUTE_TABLE = 298,
     ROUTE_PART_KEY = 299,
     PLACE_HOLDER = 300,
     END_P = 301,
     ERROR = 302,
     WHEN = 303,
     TABLEGROUP = 304,
     FLASHBACK = 305,
     AUDIT = 306,
     NOAUDIT = 307,
     STATUS = 308,
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
     GLOBALINDEX = 319,
     WARNINGS = 320,
     ERRORS = 321,
     TRACE = 322,
     QUICK = 323,
     COUNT = 324,
     AS = 325,
     WHERE = 326,
     VALUES = 327,
     ORDER = 328,
     GROUP = 329,
     HAVING = 330,
     INTO = 331,
     UNION = 332,
     FOR = 333,
     TX_READ_ONLY = 334,
     SELECT_OBPROXY_ROUTE_ADDR = 335,
     SET_OBPROXY_ROUTE_ADDR = 336,
     NAME_OB_DOT = 337,
     NAME_OB = 338,
     EXPLAIN = 339,
     EXPLAIN_ROUTE = 340,
     DESC = 341,
     DESCRIBE = 342,
     NAME_STR = 343,
     USER_VARIABLE = 344,
     SYSTEM_VARIABLE = 345,
     LOAD = 346,
     DATA = 347,
     LOCAL = 348,
     INFILE = 349,
     SLAVE = 350,
     RELAYLOG = 351,
     EVENTS = 352,
     HOSTS = 353,
     BINLOG = 354,
     PORT = 355,
     USE = 356,
     HELP = 357,
     SET_NAMES = 358,
     SET_CHARSET = 359,
     SET_PASSWORD = 360,
     SET_DEFAULT = 361,
     SET_OB_READ_CONSISTENCY = 362,
     SET_TX_READ_ONLY = 363,
     GLOBAL = 364,
     SESSION = 365,
     GLOBAL_ALIAS = 366,
     SESSION_ALIAS = 367,
     MASTER = 368,
     LOGS = 369,
     RESET = 370,
     FLUSH = 371,
     SERVER = 372,
     TENANT = 373,
     NUMBER_VAL = 374,
     GROUP_ID = 375,
     TABLE_ID = 376,
     ELASTIC_ID = 377,
     TESTLOAD = 378,
     ODP_COMMENT = 379,
     TNT_ID = 380,
     DISASTER_STATUS = 381,
     TRACE_ID = 382,
     RPC_ID = 383,
     TARGET_DB_SERVER = 384,
     TRACE_LOG = 385,
     DBP_COMMENT = 386,
     ROUTE_TAG = 387,
     SYS_TAG = 388,
     TABLE_NAME = 389,
     SCAN_ALL = 390,
     STICKY_SESSION = 391,
     PARALL = 392,
     SHARD_KEY = 393,
     STOP_DDL_TASK = 394,
     RETRY_DDL_TASK = 395,
     QUERY_TIMEOUT = 396,
     READ_CONSISTENCY = 397,
     WEAK = 398,
     STRONG = 399,
     FROZEN = 400,
     INT_NUM = 401,
     SHOW_PROXYNET = 402,
     THREAD = 403,
     CONNECTION = 404,
     LIMIT = 405,
     OFFSET = 406,
     SHOW_PROCESSLIST = 407,
     SHOW_PROXYSESSION = 408,
     SHOW_GLOBALSESSION = 409,
     ATTRIBUTE = 410,
     VARIABLES = 411,
     ALL = 412,
     STAT = 413,
     READ_STALE = 414,
     SHOW_PROXYCONFIG = 415,
     DIFF = 416,
     USER = 417,
     LIKE = 418,
     SHOW_PROXYSM = 419,
     SHOW_PROXYKV = 420,
     SHOW_PROXYCLUSTER = 421,
     SHOW_PROXYRESOURCE = 422,
     SHOW_PROXYCONGESTION = 423,
     SHOW_PROXYROUTE = 424,
     PARTITION = 425,
     ROUTINE = 426,
     SUBPARTITION = 427,
     SHOW_PROXYVIP = 428,
     SHOW_PROXYMEMORY = 429,
     OBJPOOL = 430,
     SHOW_SQLAUDIT = 431,
     SHOW_WARNLOG = 432,
     SHOW_PROXYSTAT = 433,
     REFRESH = 434,
     SHOW_PROXYTRACE = 435,
     SHOW_PROXYINFO = 436,
     BINARY = 437,
     UPGRADE = 438,
     IDC = 439,
     SHOW_PROXYPS = 440,
     DETAIL = 441,
     SHOW_ELASTIC_ID = 442,
     SHOW_TOPOLOGY = 443,
     GROUP_NAME = 444,
     SHOW_DB_VERSION = 445,
     SHOW_DATABASES = 446,
     SHOW_TABLES = 447,
     SHOW_FULL_TABLES = 448,
     SELECT_DATABASE = 449,
     SELECT_PROXY_STATUS = 450,
     SHOW_CREATE_TABLE = 451,
     SELECT_PROXY_VERSION = 452,
     SHOW_COLUMNS = 453,
     SHOW_INDEX = 454,
     ALTER_PROXYCONFIG = 455,
     ALTER_PROXYRESOURCE = 456,
     PING_PROXY = 457,
     KILL_PROXYSESSION = 458,
     KILL_GLOBALSESSION = 459,
     KILL = 460,
     QUERY = 461,
     BINLOG_VARIABLE = 462,
     BINLOG_USER_VAR = 463,
     BINLOG_SYS_VAR = 464
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
