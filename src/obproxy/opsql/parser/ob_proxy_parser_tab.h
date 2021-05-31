
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
#ifndef OBPROXYDEBUG
# if defined YYDEBUG
#if YYDEBUG
#   define OBPROXYDEBUG 1
#  else
#   define OBPROXYDEBUG 0
#  endif
# else /* ! defined YYDEBUG */
#  define OBPROXYDEBUG 0
# endif /* ! defined YYDEBUG */
#endif  /* ! defined OBPROXYDEBUG */
#if OBPROXYDEBUG
extern int obproxydebug;
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
     GRANT = 274,
     REVOKE = 275,
     ANALYZE = 276,
     PURGE = 277,
     COMMENT = 278,
     FROM = 279,
     DUAL = 280,
     PREPARE = 281,
     EXECUTE = 282,
     USING = 283,
     SELECT_HINT_BEGIN = 284,
     UPDATE_HINT_BEGIN = 285,
     DELETE_HINT_BEGIN = 286,
     INSERT_HINT_BEGIN = 287,
     REPLACE_HINT_BEGIN = 288,
     MERGE_HINT_BEGIN = 289,
     HINT_END = 290,
     COMMENT_BEGIN = 291,
     COMMENT_END = 292,
     ROUTE_TABLE = 293,
     ROUTE_PART_KEY = 294,
     QUERY_TIMEOUT = 295,
     READ_CONSISTENCY = 296,
     WEAK = 297,
     STRONG = 298,
     FROZEN = 299,
     PLACE_HOLDER = 300,
     END_P = 301,
     ERROR = 302,
     WHEN = 303,
     FLASHBACK = 304,
     AUDIT = 305,
     NOAUDIT = 306,
     BEGI = 307,
     START = 308,
     TRANSACTION = 309,
     READ = 310,
     ONLY = 311,
     WITH = 312,
     CONSISTENT = 313,
     SNAPSHOT = 314,
     INDEX = 315,
     XA = 316,
     WARNINGS = 317,
     ERRORS = 318,
     TRACE = 319,
     QUICK = 320,
     COUNT = 321,
     AS = 322,
     WHERE = 323,
     VALUES = 324,
     ORDER = 325,
     GROUP = 326,
     HAVING = 327,
     INTO = 328,
     UNION = 329,
     FOR = 330,
     TX_READ_ONLY = 331,
     AUTOCOMMIT_0 = 332,
     SELECT_OBPROXY_ROUTE_ADDR = 333,
     SET_OBPROXY_ROUTE_ADDR = 334,
     NAME_OB_DOT = 335,
     NAME_OB = 336,
     EXPLAIN = 337,
     DESC = 338,
     DESCRIBE = 339,
     NAME_STR = 340,
     USE = 341,
     HELP = 342,
     SET_NAMES = 343,
     SET_CHARSET = 344,
     SET_PASSWORD = 345,
     SET_DEFAULT = 346,
     SET_OB_READ_CONSISTENCY = 347,
     SET_TX_READ_ONLY = 348,
     GLOBAL = 349,
     SESSION = 350,
     NUMBER_VAL = 351,
     GROUP_ID = 352,
     TABLE_ID = 353,
     ELASTIC_ID = 354,
     TESTLOAD = 355,
     ODP_COMMENT = 356,
     TNT_ID = 357,
     DISASTER_STATUS = 358,
     TRACE_ID = 359,
     RPC_ID = 360,
     DBP_COMMENT = 361,
     ROUTE_TAG = 362,
     SYS_TAG = 363,
     TABLE_NAME = 364,
     SCAN_ALL = 365,
     PARALL = 366,
     SHARD_KEY = 367,
     INT_NUM = 368,
     SHOW_PROXYNET = 369,
     THREAD = 370,
     CONNECTION = 371,
     LIMIT = 372,
     OFFSET = 373,
     SHOW_PROCESSLIST = 374,
     SHOW_PROXYSESSION = 375,
     SHOW_GLOBALSESSION = 376,
     ATTRIBUTE = 377,
     VARIABLES = 378,
     ALL = 379,
     STAT = 380,
     SHOW_PROXYCONFIG = 381,
     DIFF = 382,
     USER = 383,
     LIKE = 384,
     SHOW_PROXYSM = 385,
     SHOW_PROXYCLUSTER = 386,
     SHOW_PROXYRESOURCE = 387,
     SHOW_PROXYCONGESTION = 388,
     SHOW_PROXYROUTE = 389,
     PARTITION = 390,
     ROUTINE = 391,
     SHOW_PROXYVIP = 392,
     SHOW_PROXYMEMORY = 393,
     OBJPOOL = 394,
     SHOW_SQLAUDIT = 395,
     SHOW_WARNLOG = 396,
     SHOW_PROXYSTAT = 397,
     REFRESH = 398,
     SHOW_PROXYTRACE = 399,
     SHOW_PROXYINFO = 400,
     BINARY = 401,
     UPGRADE = 402,
     IDC = 403,
     SHOW_TOPOLOGY = 404,
     GROUP_NAME = 405,
     SHOW_DB_VERSION = 406,
     SHOW_DATABASES = 407,
     SHOW_TABLES = 408,
     SELECT_DATABASE = 409,
     SHOW_CREATE_TABLE = 410,
     ALTER_PROXYCONFIG = 411,
     ALTER_PROXYRESOURCE = 412,
     PING_PROXY = 413,
     KILL_PROXYSESSION = 414,
     KILL_GLOBALSESSION = 415,
     KILL = 416,
     QUERY = 417
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
