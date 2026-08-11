
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
     FORCE_MASTER_HINT = 304,
     ROUTE_TABLE = 305,
     ROUTE_PART_KEY = 306,
     PLACE_HOLDER = 307,
     END_P = 308,
     ERROR = 309,
     WHEN = 310,
     TABLEGROUP = 311,
     FLASHBACK = 312,
     AUDIT = 313,
     NOAUDIT = 314,
     STATUS = 315,
     BEGI = 316,
     START = 317,
     TRANSACTION = 318,
     READ = 319,
     ONLY = 320,
     WITH = 321,
     CONSISTENT = 322,
     SNAPSHOT = 323,
     INDEX = 324,
     XA = 325,
     GLOBALINDEX = 326,
     WARNINGS = 327,
     ERRORS = 328,
     TRACE = 329,
     QUICK = 330,
     COUNT = 331,
     AS = 332,
     WHERE = 333,
     VALUES = 334,
     ORDER = 335,
     GROUP = 336,
     HAVING = 337,
     INTO = 338,
     UNION = 339,
     FOR = 340,
     TX_READ_ONLY = 341,
     SELECT_OBPROXY_ROUTE_ADDR = 342,
     SET_OBPROXY_ROUTE_ADDR = 343,
     NAME_OB_DOT = 344,
     NAME_OB = 345,
     EXPLAIN = 346,
     EXPLAIN_ROUTE = 347,
     DESC = 348,
     DESCRIBE = 349,
     NAME_STR = 350,
     USER_VARIABLE = 351,
     SYSTEM_VARIABLE = 352,
     LOAD = 353,
     DATA = 354,
     LOCAL = 355,
     INFILE = 356,
     SLAVE = 357,
     RELAYLOG = 358,
     EVENTS = 359,
     HOSTS = 360,
     BINLOG = 361,
     PORT = 362,
     USE = 363,
     HELP = 364,
     SET_NAMES = 365,
     SET_CHARSET = 366,
     SET_PASSWORD = 367,
     SET_DEFAULT = 368,
     SET_OB_READ_CONSISTENCY = 369,
     SET_TX_READ_ONLY = 370,
     GLOBAL = 371,
     SESSION = 372,
     GLOBAL_ALIAS = 373,
     SESSION_ALIAS = 374,
     MASTER = 375,
     LOGS = 376,
     RESET = 377,
     FLUSH = 378,
     SERVER = 379,
     TENANT = 380,
     NUMBER_VAL = 381,
     GROUP_ID = 382,
     TABLE_ID = 383,
     ELASTIC_ID = 384,
     TESTLOAD = 385,
     ODP_COMMENT = 386,
     TNT_ID = 387,
     DISASTER_STATUS = 388,
     TRACE_ID = 389,
     RPC_ID = 390,
     TARGET_DB_SERVER = 391,
     TRACE_LOG = 392,
     DBP_COMMENT = 393,
     ROUTE_TAG = 394,
     SYS_TAG = 395,
     TABLE_NAME = 396,
     SCAN_ALL = 397,
     STICKY_SESSION = 398,
     PARALL = 399,
     SHARD_KEY = 400,
     STOP_DDL_TASK = 401,
     RETRY_DDL_TASK = 402,
     QUERY_TIMEOUT = 403,
     MAX_EXECUTION_TIME = 404,
     READ_CONSISTENCY = 405,
     WEAK = 406,
     STRONG = 407,
     FROZEN = 408,
     HINT_OPT_PARAM = 409,
     HINT_AP_QUERY_ROUTE_POLICY = 410,
     HINT_AP_QRP_FORCE = 411,
     HINT_AP_QRP_AUTO = 412,
     HINT_AP_QRP_OFF = 413,
     INT_NUM = 414,
     SHOW_PROXYNET = 415,
     THREAD = 416,
     CONNECTION = 417,
     LIMIT = 418,
     OFFSET = 419,
     SHOW_PROCESSLIST = 420,
     SHOW_PROXYSESSION = 421,
     SHOW_GLOBALSESSION = 422,
     ATTRIBUTE = 423,
     VARIABLES = 424,
     ALL = 425,
     STAT = 426,
     READ_STALE = 427,
     SHOW_PROXYCONFIG = 428,
     DIFF = 429,
     USER = 430,
     LIKE = 431,
     SHOW_PROXYSM = 432,
     RPC = 433,
     SHOW_PROXYRPC = 434,
     REQUESTSTAT = 435,
     SHOW_PROXYCLUSTER = 436,
     SHOW_PROXYRESOURCE = 437,
     SHOW_PROXYCONGESTION = 438,
     SHOW_PROXYROUTE = 439,
     PARTITION = 440,
     ROUTINE = 441,
     SUBPARTITION = 442,
     TABLETLS = 443,
     QUERYASYNC = 444,
     RPCCTX = 445,
     SHOW_PROXYVIP = 446,
     SHOW_PROXYMEMORY = 447,
     OBJPOOL = 448,
     SHOW_SQLAUDIT = 449,
     SHOW_WARNLOG = 450,
     SHOW_PROXYSTAT = 451,
     REFRESH = 452,
     SHOW_PROXYTRACE = 453,
     SHOW_PROXYINFO = 454,
     BINARY = 455,
     UPGRADE = 456,
     IDC = 457,
     SHOW_PROXYPS = 458,
     DETAIL = 459,
     SHOW_ELASTIC_ID = 460,
     SHOW_TOPOLOGY = 461,
     GROUP_NAME = 462,
     SHOW_DB_VERSION = 463,
     SHOW_DATABASES = 464,
     SHOW_TABLES = 465,
     SHOW_FULL_TABLES = 466,
     SELECT_DATABASE = 467,
     SELECT_PROXY_STATUS = 468,
     SHOW_CREATE_TABLE = 469,
     SELECT_PROXY_VERSION = 470,
     SHOW_COLUMNS = 471,
     SHOW_INDEX = 472,
     ALTER_PROXYCONFIG = 473,
     ALTER_PROXYRESOURCE = 474,
     PING_PROXY = 475,
     KILL_PROXYSESSION = 476,
     KILL_GLOBALSESSION = 477,
     KILL = 478,
     QUERY = 479,
     BINLOG_VARIABLE = 480,
     BINLOG_USER_VAR = 481,
     BINLOG_SYS_VAR = 482,
     CDC = 483,
     REGISTER = 484,
     UNREGISTER = 485,
     AUTH = 486,
     ACK = 487,
     DESCRIBE_CDC = 488
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
