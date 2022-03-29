
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
     SELECT_HINT_BEGIN = 286,
     UPDATE_HINT_BEGIN = 287,
     DELETE_HINT_BEGIN = 288,
     INSERT_HINT_BEGIN = 289,
     REPLACE_HINT_BEGIN = 290,
     MERGE_HINT_BEGIN = 291,
     HINT_END = 292,
     COMMENT_BEGIN = 293,
     COMMENT_END = 294,
     ROUTE_TABLE = 295,
     ROUTE_PART_KEY = 296,
     QUERY_TIMEOUT = 297,
     READ_CONSISTENCY = 298,
     WEAK = 299,
     STRONG = 300,
     FROZEN = 301,
     PLACE_HOLDER = 302,
     END_P = 303,
     ERROR = 304,
     WHEN = 305,
     FLASHBACK = 306,
     AUDIT = 307,
     NOAUDIT = 308,
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
     AUTOCOMMIT_0 = 334,
     SELECT_OBPROXY_ROUTE_ADDR = 335,
     SET_OBPROXY_ROUTE_ADDR = 336,
     NAME_OB_DOT = 337,
     NAME_OB = 338,
     EXPLAIN = 339,
     DESC = 340,
     DESCRIBE = 341,
     NAME_STR = 342,
     USE = 343,
     HELP = 344,
     SET_NAMES = 345,
     SET_CHARSET = 346,
     SET_PASSWORD = 347,
     SET_DEFAULT = 348,
     SET_OB_READ_CONSISTENCY = 349,
     SET_TX_READ_ONLY = 350,
     GLOBAL = 351,
     SESSION = 352,
     NUMBER_VAL = 353,
     GROUP_ID = 354,
     TABLE_ID = 355,
     ELASTIC_ID = 356,
     TESTLOAD = 357,
     ODP_COMMENT = 358,
     TNT_ID = 359,
     DISASTER_STATUS = 360,
     TRACE_ID = 361,
     RPC_ID = 362,
     DBP_COMMENT = 363,
     ROUTE_TAG = 364,
     SYS_TAG = 365,
     TABLE_NAME = 366,
     SCAN_ALL = 367,
     PARALL = 368,
     SHARD_KEY = 369,
     INT_NUM = 370,
     SHOW_PROXYNET = 371,
     THREAD = 372,
     CONNECTION = 373,
     LIMIT = 374,
     OFFSET = 375,
     SHOW_PROCESSLIST = 376,
     SHOW_PROXYSESSION = 377,
     SHOW_GLOBALSESSION = 378,
     ATTRIBUTE = 379,
     VARIABLES = 380,
     ALL = 381,
     STAT = 382,
     SHOW_PROXYCONFIG = 383,
     DIFF = 384,
     USER = 385,
     LIKE = 386,
     SHOW_PROXYSM = 387,
     SHOW_PROXYCLUSTER = 388,
     SHOW_PROXYRESOURCE = 389,
     SHOW_PROXYCONGESTION = 390,
     SHOW_PROXYROUTE = 391,
     PARTITION = 392,
     ROUTINE = 393,
     SHOW_PROXYVIP = 394,
     SHOW_PROXYMEMORY = 395,
     OBJPOOL = 396,
     SHOW_SQLAUDIT = 397,
     SHOW_WARNLOG = 398,
     SHOW_PROXYSTAT = 399,
     REFRESH = 400,
     SHOW_PROXYTRACE = 401,
     SHOW_PROXYINFO = 402,
     BINARY = 403,
     UPGRADE = 404,
     IDC = 405,
     SHOW_TOPOLOGY = 406,
     GROUP_NAME = 407,
     SHOW_DB_VERSION = 408,
     SHOW_DATABASES = 409,
     SHOW_TABLES = 410,
     SELECT_DATABASE = 411,
     SHOW_CREATE_TABLE = 412,
     SELECT_PROXY_VERSION = 413,
     ALTER_PROXYCONFIG = 414,
     ALTER_PROXYRESOURCE = 415,
     PING_PROXY = 416,
     KILL_PROXYSESSION = 417,
     KILL_GLOBALSESSION = 418,
     KILL = 419,
     QUERY = 420
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
