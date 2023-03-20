
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
     GRANT = 277,
     REVOKE = 278,
     ANALYZE = 279,
     PURGE = 280,
     COMMENT = 281,
     FROM = 282,
     DUAL = 283,
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
     HINT_END = 294,
     COMMENT_BEGIN = 295,
     COMMENT_END = 296,
     ROUTE_TABLE = 297,
     ROUTE_PART_KEY = 298,
     QUERY_TIMEOUT = 299,
     READ_CONSISTENCY = 300,
     WEAK = 301,
     STRONG = 302,
     FROZEN = 303,
     PLACE_HOLDER = 304,
     END_P = 305,
     ERROR = 306,
     WHEN = 307,
     FLASHBACK = 308,
     AUDIT = 309,
     NOAUDIT = 310,
     BEGI = 311,
     START = 312,
     TRANSACTION = 313,
     READ = 314,
     ONLY = 315,
     WITH = 316,
     CONSISTENT = 317,
     SNAPSHOT = 318,
     INDEX = 319,
     XA = 320,
     WARNINGS = 321,
     ERRORS = 322,
     TRACE = 323,
     QUICK = 324,
     COUNT = 325,
     AS = 326,
     WHERE = 327,
     VALUES = 328,
     ORDER = 329,
     GROUP = 330,
     HAVING = 331,
     INTO = 332,
     UNION = 333,
     FOR = 334,
     TX_READ_ONLY = 335,
     SELECT_OBPROXY_ROUTE_ADDR = 336,
     SET_OBPROXY_ROUTE_ADDR = 337,
     NAME_OB_DOT = 338,
     NAME_OB = 339,
     EXPLAIN = 340,
     DESC = 341,
     DESCRIBE = 342,
     NAME_STR = 343,
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
     DBP_COMMENT = 365,
     ROUTE_TAG = 366,
     SYS_TAG = 367,
     TABLE_NAME = 368,
     SCAN_ALL = 369,
     STICKY_SESSION = 370,
     PARALL = 371,
     SHARD_KEY = 372,
     INT_NUM = 373,
     SHOW_PROXYNET = 374,
     THREAD = 375,
     CONNECTION = 376,
     LIMIT = 377,
     OFFSET = 378,
     SHOW_PROCESSLIST = 379,
     SHOW_PROXYSESSION = 380,
     SHOW_GLOBALSESSION = 381,
     ATTRIBUTE = 382,
     VARIABLES = 383,
     ALL = 384,
     STAT = 385,
     SHOW_PROXYCONFIG = 386,
     DIFF = 387,
     USER = 388,
     LIKE = 389,
     SHOW_PROXYSM = 390,
     SHOW_PROXYCLUSTER = 391,
     SHOW_PROXYRESOURCE = 392,
     SHOW_PROXYCONGESTION = 393,
     SHOW_PROXYROUTE = 394,
     PARTITION = 395,
     ROUTINE = 396,
     SHOW_PROXYVIP = 397,
     SHOW_PROXYMEMORY = 398,
     OBJPOOL = 399,
     SHOW_SQLAUDIT = 400,
     SHOW_WARNLOG = 401,
     SHOW_PROXYSTAT = 402,
     REFRESH = 403,
     SHOW_PROXYTRACE = 404,
     SHOW_PROXYINFO = 405,
     BINARY = 406,
     UPGRADE = 407,
     IDC = 408,
     SHOW_ELASTIC_ID = 409,
     SHOW_TOPOLOGY = 410,
     GROUP_NAME = 411,
     SHOW_DB_VERSION = 412,
     SHOW_DATABASES = 413,
     SHOW_TABLES = 414,
     SHOW_FULL_TABLES = 415,
     SELECT_DATABASE = 416,
     SHOW_CREATE_TABLE = 417,
     SELECT_PROXY_VERSION = 418,
     SHOW_COLUMNS = 419,
     SHOW_INDEX = 420,
     ALTER_PROXYCONFIG = 421,
     ALTER_PROXYRESOURCE = 422,
     PING_PROXY = 423,
     KILL_PROXYSESSION = 424,
     KILL_GLOBALSESSION = 425,
     KILL = 426,
     QUERY = 427,
     SHOW_MASTER_STATUS = 428,
     SHOW_BINARY_LOGS = 429,
     SHOW_BINLOG_EVENTS = 430,
     PURGE_BINARY_LOGS = 431,
     RESET_MASTER = 432,
     SHOW_BINLOG_SERVER_FOR_TENANT = 433
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
