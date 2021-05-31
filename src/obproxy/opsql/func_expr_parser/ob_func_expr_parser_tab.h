
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


#ifndef YY_OBFUNCEXPR_OB_FUNC_EXPR_PARSER_TAB_H_INCLUDED
# define YY_OBFUNCEXPR_OB_FUNC_EXPR_PARSER_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef OBFUNCEXPRDEBUG
# if defined YYDEBUG
#if YYDEBUG
#   define OBFUNCEXPRDEBUG 1
#  else
#   define OBFUNCEXPRDEBUG 0
#  endif
# else /* ! defined YYDEBUG */
#  define OBFUNCEXPRDEBUG 0
# endif /* ! defined YYDEBUG */
#endif  /* ! defined OBFUNCEXPRDEBUG */
#if OBFUNCEXPRDEBUG
extern int obfuncexprdebug;
#endif
/* Tokens.  */
#ifndef OBFUNCEXPRTOKENTYPE
# define OBFUNCEXPRTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum obfuncexprtokentype {
     DUMMY_FUNCTION_CLAUSE = 258,
     TOKEN_SPECIAL = 259,
     FUNC_SUBSTR = 260,
     FUNC_CONCAT = 261,
     FUNC_HASH = 262,
     FUNC_TOINT = 263,
     FUNC_DIV = 264,
     FUNC_ADD = 265,
     FUNC_SUB = 266,
     FUNC_MUL = 267,
     FUNC_TESTLOAD = 268,
     END_P = 269,
     ERROR = 270,
     IGNORED_WORD = 271,
     NAME_OB = 272,
     STR_VAL = 273,
     NUMBER_VAL = 274,
     INT_VAL = 275
   };
#endif



#if ! defined OBFUNCEXPRSTYPE && ! defined OBFUNCEXPRSTYPE_IS_DECLARED
typedef union OBFUNCEXPRSTYPE
{


  int64_t              num;
  ObProxyParseString   str;
  ObProxyParamNode     *param_node;
  ObFuncExprNode        *func_node;
  ObProxyParamNodeList *list;
  ObProxyExprType function_type;



} OBFUNCEXPRSTYPE;
# define OBFUNCEXPRSTYPE_IS_TRIVIAL 1
# define obfuncexprstype OBFUNCEXPRSTYPE /* obsolescent; will be withdrawn */
# define OBFUNCEXPRSTYPE_IS_DECLARED 1
#endif



#if ! defined OBFUNCEXPRLTYPE && ! defined OBFUNCEXPRLTYPE_IS_DECLARED
typedef struct OBFUNCEXPRLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} OBFUNCEXPRLTYPE;
# define obfuncexprltype OBFUNCEXPRLTYPE /* obsolescent; will be withdrawn */
# define OBFUNCEXPRLTYPE_IS_DECLARED 1
# define OBFUNCEXPRLTYPE_IS_TRIVIAL 1
#endif



#endif
