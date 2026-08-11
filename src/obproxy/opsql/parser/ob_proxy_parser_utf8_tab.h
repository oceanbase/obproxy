
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
     JOIN = 283,
     INNER = 284,
     CROSS = 285,
     FULL = 286,
     LEFT = 287,
     RIGHT = 288,
     OUTER = 289,
     PREPARE = 290,
     EXECUTE = 291,
     USING = 292,
     DEALLOCATE = 293,
     SELECT_HINT_BEGIN = 294,
     UPDATE_HINT_BEGIN = 295,
     DELETE_HINT_BEGIN = 296,
     INSERT_HINT_BEGIN = 297,
     REPLACE_HINT_BEGIN = 298,
     MERGE_HINT_BEGIN = 299,
     LOAD_DATA_HINT_BEGIN = 300,
     HINT_END = 301,
     COMMENT_BEGIN = 302,
     COMMENT_END = 303,
     ROUTE_TABLE = 304,
     ROUTE_PART_KEY = 305,
     PLACE_HOLDER = 306,
     END_P = 307,
     ERROR = 308,
     WHEN = 309,
     TABLEGROUP = 310,
     FLASHBACK = 311,
     AUDIT = 312,
     NOAUDIT = 313,
     STATUS = 314,
     BEGI = 315,
     START = 316,
     TRANSACTION = 317,
     READ = 318,
     ONLY = 319,
     WITH = 320,
     CONSISTENT = 321,
     SNAPSHOT = 322,
     INDEX = 323,
     XA = 324,
     GLOBALINDEX = 325,
     WARNINGS = 326,
     ERRORS = 327,
     TRACE = 328,
     QUICK = 329,
     COUNT = 330,
     AS = 331,
     WHERE = 332,
     VALUES = 333,
     ORDER = 334,
     GROUP = 335,
     HAVING = 336,
     INTO = 337,
     UNION = 338,
     FOR = 339,
     TX_READ_ONLY = 340,
     SELECT_OBPROXY_ROUTE_ADDR = 341,
     SET_OBPROXY_ROUTE_ADDR = 342,
     NAME_OB_DOT = 343,
     NAME_OB = 344,
     EXPLAIN = 345,
     EXPLAIN_ROUTE = 346,
     DESC = 347,
     DESCRIBE = 348,
     NAME_STR = 349,
     USER_VARIABLE = 350,
     SYSTEM_VARIABLE = 351,
     LOAD = 352,
     DATA = 353,
     LOCAL = 354,
     INFILE = 355,
     SLAVE = 356,
     RELAYLOG = 357,
     EVENTS = 358,
     HOSTS = 359,
     BINLOG = 360,
     PORT = 361,
     USE = 362,
     HELP = 363,
     SET_NAMES = 364,
     SET_CHARSET = 365,
     SET_PASSWORD = 366,
     SET_DEFAULT = 367,
     SET_OB_READ_CONSISTENCY = 368,
     SET_TX_READ_ONLY = 369,
     GLOBAL = 370,
     SESSION = 371,
     GLOBAL_ALIAS = 372,
     SESSION_ALIAS = 373,
     MASTER = 374,
     LOGS = 375,
     RESET = 376,
     FLUSH = 377,
     SERVER = 378,
     TENANT = 379,
     NUMBER_VAL = 380,
     GROUP_ID = 381,
     TABLE_ID = 382,
     ELASTIC_ID = 383,
     TESTLOAD = 384,
     ODP_COMMENT = 385,
     TNT_ID = 386,
     DISASTER_STATUS = 387,
     TRACE_ID = 388,
     RPC_ID = 389,
     TARGET_DB_SERVER = 390,
     TRACE_LOG = 391,
     DBP_COMMENT = 392,
     ROUTE_TAG = 393,
     SYS_TAG = 394,
     TABLE_NAME = 395,
     SCAN_ALL = 396,
     STICKY_SESSION = 397,
     PARALL = 398,
     SHARD_KEY = 399,
     STOP_DDL_TASK = 400,
     RETRY_DDL_TASK = 401,
     QUERY_TIMEOUT = 402,
     MAX_EXECUTION_TIME = 403,
     READ_CONSISTENCY = 404,
     WEAK = 405,
     STRONG = 406,
     FROZEN = 407,
     INT_NUM = 408,
     SHOW_PROXYNET = 409,
     THREAD = 410,
     CONNECTION = 411,
     LIMIT = 412,
     OFFSET = 413,
     SHOW_PROCESSLIST = 414,
     SHOW_PROXYSESSION = 415,
     SHOW_GLOBALSESSION = 416,
     ATTRIBUTE = 417,
     VARIABLES = 418,
     ALL = 419,
     STAT = 420,
     READ_STALE = 421,
     SHOW_PROXYCONFIG = 422,
     DIFF = 423,
     USER = 424,
     LIKE = 425,
     SHOW_PROXYSM = 426,
     RPC = 427,
     SHOW_PROXYRPC = 428,
     REQUESTSTAT = 429,
     SHOW_PROXYCLUSTER = 430,
     SHOW_PROXYRESOURCE = 431,
     SHOW_PROXYCONGESTION = 432,
     SHOW_PROXYROUTE = 433,
     PARTITION = 434,
     ROUTINE = 435,
     SUBPARTITION = 436,
     TABLETLS = 437,
     QUERYASYNC = 438,
     RPCCTX = 439,
     SHOW_PROXYVIP = 440,
     SHOW_PROXYMEMORY = 441,
     OBJPOOL = 442,
     SHOW_SQLAUDIT = 443,
     SHOW_WARNLOG = 444,
     SHOW_PROXYSTAT = 445,
     REFRESH = 446,
     SHOW_PROXYTRACE = 447,
     SHOW_PROXYINFO = 448,
     BINARY = 449,
     UPGRADE = 450,
     IDC = 451,
     SHOW_PROXYPS = 452,
     DETAIL = 453,
     SHOW_ELASTIC_ID = 454,
     SHOW_TOPOLOGY = 455,
     GROUP_NAME = 456,
     SHOW_DB_VERSION = 457,
     SHOW_DATABASES = 458,
     SHOW_TABLES = 459,
     SHOW_FULL_TABLES = 460,
     SELECT_DATABASE = 461,
     SELECT_PROXY_STATUS = 462,
     SHOW_CREATE_TABLE = 463,
     SELECT_PROXY_VERSION = 464,
     SHOW_COLUMNS = 465,
     SHOW_INDEX = 466,
     ALTER_PROXYCONFIG = 467,
     ALTER_PROXYRESOURCE = 468,
     PING_PROXY = 469,
     KILL_PROXYSESSION = 470,
     KILL_GLOBALSESSION = 471,
     KILL = 472,
     QUERY = 473,
     BINLOG_VARIABLE = 474,
     BINLOG_USER_VAR = 475,
     BINLOG_SYS_VAR = 476
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
