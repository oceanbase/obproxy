
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
     AUTOCOMMIT_0 = 336,
     SELECT_OBPROXY_ROUTE_ADDR = 337,
     SET_OBPROXY_ROUTE_ADDR = 338,
     NAME_OB_DOT = 339,
     NAME_OB = 340,
     EXPLAIN = 341,
     DESC = 342,
     DESCRIBE = 343,
     NAME_STR = 344,
     USE = 345,
     HELP = 346,
     SET_NAMES = 347,
     SET_CHARSET = 348,
     SET_PASSWORD = 349,
     SET_DEFAULT = 350,
     SET_OB_READ_CONSISTENCY = 351,
     SET_TX_READ_ONLY = 352,
     GLOBAL = 353,
     SESSION = 354,
     NUMBER_VAL = 355,
     GROUP_ID = 356,
     TABLE_ID = 357,
     ELASTIC_ID = 358,
     TESTLOAD = 359,
     ODP_COMMENT = 360,
     TNT_ID = 361,
     DISASTER_STATUS = 362,
     TRACE_ID = 363,
     RPC_ID = 364,
     DBP_COMMENT = 365,
     ROUTE_TAG = 366,
     SYS_TAG = 367,
     TABLE_NAME = 368,
     SCAN_ALL = 369,
     PARALL = 370,
     SHARD_KEY = 371,
     INT_NUM = 372,
     SHOW_PROXYNET = 373,
     THREAD = 374,
     CONNECTION = 375,
     LIMIT = 376,
     OFFSET = 377,
     SHOW_PROCESSLIST = 378,
     SHOW_PROXYSESSION = 379,
     SHOW_GLOBALSESSION = 380,
     ATTRIBUTE = 381,
     VARIABLES = 382,
     ALL = 383,
     STAT = 384,
     SHOW_PROXYCONFIG = 385,
     DIFF = 386,
     USER = 387,
     LIKE = 388,
     SHOW_PROXYSM = 389,
     SHOW_PROXYCLUSTER = 390,
     SHOW_PROXYRESOURCE = 391,
     SHOW_PROXYCONGESTION = 392,
     SHOW_PROXYROUTE = 393,
     PARTITION = 394,
     ROUTINE = 395,
     SHOW_PROXYVIP = 396,
     SHOW_PROXYMEMORY = 397,
     OBJPOOL = 398,
     SHOW_SQLAUDIT = 399,
     SHOW_WARNLOG = 400,
     SHOW_PROXYSTAT = 401,
     REFRESH = 402,
     SHOW_PROXYTRACE = 403,
     SHOW_PROXYINFO = 404,
     BINARY = 405,
     UPGRADE = 406,
     IDC = 407,
     SHOW_TOPOLOGY = 408,
     GROUP_NAME = 409,
     SHOW_DB_VERSION = 410,
     SHOW_DATABASES = 411,
     SHOW_TABLES = 412,
     SHOW_FULL_TABLES = 413,
     SELECT_DATABASE = 414,
     SHOW_CREATE_TABLE = 415,
     SELECT_PROXY_VERSION = 416,
     SHOW_COLUMNS = 417,
     SHOW_INDEX = 418,
     ALTER_PROXYCONFIG = 419,
     ALTER_PROXYRESOURCE = 420,
     PING_PROXY = 421,
     KILL_PROXYSESSION = 422,
     KILL_GLOBALSESSION = 423,
     KILL = 424,
     QUERY = 425
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
