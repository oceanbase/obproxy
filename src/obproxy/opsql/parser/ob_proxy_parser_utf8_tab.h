
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
     WARNINGS = 318,
     ERRORS = 319,
     TRACE = 320,
     QUICK = 321,
     COUNT = 322,
     AS = 323,
     WHERE = 324,
     VALUES = 325,
     ORDER = 326,
     GROUP = 327,
     HAVING = 328,
     INTO = 329,
     UNION = 330,
     FOR = 331,
     TX_READ_ONLY = 332,
     SELECT_OBPROXY_ROUTE_ADDR = 333,
     SET_OBPROXY_ROUTE_ADDR = 334,
     NAME_OB_DOT = 335,
     NAME_OB = 336,
     EXPLAIN = 337,
     EXPLAIN_ROUTE = 338,
     DESC = 339,
     DESCRIBE = 340,
     NAME_STR = 341,
     LOAD = 342,
     DATA = 343,
     LOCAL = 344,
     INFILE = 345,
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
     TRACE_LOG = 367,
     DBP_COMMENT = 368,
     ROUTE_TAG = 369,
     SYS_TAG = 370,
     TABLE_NAME = 371,
     SCAN_ALL = 372,
     STICKY_SESSION = 373,
     PARALL = 374,
     SHARD_KEY = 375,
     STOP_DDL_TASK = 376,
     RETRY_DDL_TASK = 377,
     QUERY_TIMEOUT = 378,
     READ_CONSISTENCY = 379,
     WEAK = 380,
     STRONG = 381,
     FROZEN = 382,
     INT_NUM = 383,
     SHOW_PROXYNET = 384,
     THREAD = 385,
     CONNECTION = 386,
     LIMIT = 387,
     OFFSET = 388,
     SHOW_PROCESSLIST = 389,
     SHOW_PROXYSESSION = 390,
     SHOW_GLOBALSESSION = 391,
     ATTRIBUTE = 392,
     VARIABLES = 393,
     ALL = 394,
     STAT = 395,
     READ_STALE = 396,
     SHOW_PROXYCONFIG = 397,
     DIFF = 398,
     USER = 399,
     LIKE = 400,
     SHOW_PROXYSM = 401,
     SHOW_PROXYCLUSTER = 402,
     SHOW_PROXYRESOURCE = 403,
     SHOW_PROXYCONGESTION = 404,
     SHOW_PROXYROUTE = 405,
     PARTITION = 406,
     ROUTINE = 407,
     SUBPARTITION = 408,
     SHOW_PROXYVIP = 409,
     SHOW_PROXYMEMORY = 410,
     OBJPOOL = 411,
     SHOW_SQLAUDIT = 412,
     SHOW_WARNLOG = 413,
     SHOW_PROXYSTAT = 414,
     REFRESH = 415,
     SHOW_PROXYTRACE = 416,
     SHOW_PROXYINFO = 417,
     BINARY = 418,
     UPGRADE = 419,
     IDC = 420,
     SHOW_ELASTIC_ID = 421,
     SHOW_TOPOLOGY = 422,
     GROUP_NAME = 423,
     SHOW_DB_VERSION = 424,
     SHOW_DATABASES = 425,
     SHOW_TABLES = 426,
     SHOW_FULL_TABLES = 427,
     SELECT_DATABASE = 428,
     SELECT_PROXY_STATUS = 429,
     SHOW_CREATE_TABLE = 430,
     SELECT_PROXY_VERSION = 431,
     SHOW_COLUMNS = 432,
     SHOW_INDEX = 433,
     ALTER_PROXYCONFIG = 434,
     ALTER_PROXYRESOURCE = 435,
     PING_PROXY = 436,
     KILL_PROXYSESSION = 437,
     KILL_GLOBALSESSION = 438,
     KILL = 439,
     QUERY = 440,
     SHOW_BINLOG_SERVER_FOR_TENANT = 441
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
