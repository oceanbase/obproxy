
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
     PREPARE = 283,
     EXECUTE = 284,
     USING = 285,
     DEALLOCATE = 286,
     COMMENT_BEGIN = 287,
     COMMENT_END = 288,
     ROUTE_TABLE = 289,
     ROUTE_PART_KEY = 290,
     QUERY_TIMEOUT = 291,
     READ_CONSISTENCY = 292,
     WEAK = 293,
     STRONG = 294,
     FROZEN = 295,
     PLACE_HOLDER = 296,
     END_P = 297,
     ERROR = 298,
     WHEN = 299,
     TABLEGROUP = 300,
     FLASHBACK = 301,
     AUDIT = 302,
     NOAUDIT = 303,
     STATUS = 304,
     BEGI = 305,
     START = 306,
     TRANSACTION = 307,
     READ = 308,
     ONLY = 309,
     WITH = 310,
     CONSISTENT = 311,
     SNAPSHOT = 312,
     INDEX = 313,
     XA = 314,
     WARNINGS = 315,
     ERRORS = 316,
     TRACE = 317,
     QUICK = 318,
     COUNT = 319,
     AS = 320,
     WHERE = 321,
     VALUES = 322,
     ORDER = 323,
     GROUP = 324,
     HAVING = 325,
     INTO = 326,
     UNION = 327,
     FOR = 328,
     TX_READ_ONLY = 329,
     SELECT_OBPROXY_ROUTE_ADDR = 330,
     SET_OBPROXY_ROUTE_ADDR = 331,
     NAME_OB_DOT = 332,
     NAME_OB = 333,
     EXPLAIN = 334,
     EXPLAIN_ROUTE = 335,
     DESC = 336,
     DESCRIBE = 337,
     NAME_STR = 338,
     LOAD = 339,
     DATA = 340,
     HINT_BEGIN = 341,
     LOCAL = 342,
     INFILE = 343,
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
     TRACE_LOG = 365,
     DBP_COMMENT = 366,
     ROUTE_TAG = 367,
     SYS_TAG = 368,
     TABLE_NAME = 369,
     SCAN_ALL = 370,
     STICKY_SESSION = 371,
     PARALL = 372,
     SHARD_KEY = 373,
     STOP_DDL_TASK = 374,
     RETRY_DDL_TASK = 375,
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
