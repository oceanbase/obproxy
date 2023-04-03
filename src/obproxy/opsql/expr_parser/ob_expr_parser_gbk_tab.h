
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


#ifndef YY_OBEXPR_OB_EXPR_PARSER_TAB_H_INCLUDED
# define YY_OBEXPR_OB_EXPR_PARSER_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef OBEXPR_UTF8_DEBUG
# if defined YYDEBUG
#if YYDEBUG
#   define OBEXPR_UTF8_DEBUG 1
#  else
#   define OBEXPR_UTF8_DEBUG 0
#  endif
# else /* ! defined YYDEBUG */
#  define OBEXPR_UTF8_DEBUG 0
# endif /* ! defined YYDEBUG */
#endif  /* ! defined OBEXPR_UTF8_DEBUG */
#if OBEXPR_UTF8_DEBUG
extern int ob_expr_parser_gbk_yydebug;
#endif
/* Tokens.  */
#ifndef OBEXPRTOKENTYPE
# define OBEXPRTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum obexprtokentype {
     DUMMY_SELECT_CLAUSE = 258,
     DUMMY_INSERT_CLAUSE = 259,
     WHERE = 260,
     AS = 261,
     VALUES = 262,
     SET = 263,
     END_WHERE = 264,
     JOIN = 265,
     AND_OP = 266,
     OR_OP = 267,
     IN = 268,
     ON = 269,
     BETWEEN = 270,
     IS = 271,
     TOKEN_NULL = 272,
     NOT = 273,
     COMP_EQ = 274,
     COMP_NSEQ = 275,
     COMP_GE = 276,
     COMP_GT = 277,
     COMP_LE = 278,
     COMP_LT = 279,
     COMP_NE = 280,
     PLACE_HOLDER = 281,
     END_P = 282,
     ERROR = 283,
     IGNORED_WORD = 284,
     NAME_OB = 285,
     STR_VAL = 286,
     ROW_ID = 287,
     REVERSED_EXPR_FUNC = 288,
     INT_VAL = 289,
     POS_PLACE_HOLDER = 290
   };
#endif



#if ! defined OBEXPRSTYPE && ! defined OBEXPRSTYPE_IS_DECLARED
typedef union OBEXPRSTYPE
{


  int64_t              num;
  ObProxyParseString   str;
  ObProxyFunctionType  func;
  ObProxyOperatorType  operator;
  ObProxyTokenNode     *node;
  ObProxyTokenList     *list;
  ObProxyRelationExpr  *relation;



} OBEXPRSTYPE;
# define OBEXPRSTYPE_IS_TRIVIAL 1
# define obexprstype OBEXPRSTYPE /* obsolescent; will be withdrawn */
# define OBEXPRSTYPE_IS_DECLARED 1
#endif



#if ! defined OBEXPRLTYPE && ! defined OBEXPRLTYPE_IS_DECLARED
typedef struct OBEXPRLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} OBEXPRLTYPE;
# define obexprltype OBEXPRLTYPE /* obsolescent; will be withdrawn */
# define OBEXPRLTYPE_IS_DECLARED 1
# define OBEXPRLTYPE_IS_TRIVIAL 1
#endif



#endif
