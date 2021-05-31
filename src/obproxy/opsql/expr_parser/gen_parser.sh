#!/bin/bash
#
# DATE: 2016-02-01
# DESCRIPTION:
#
set +x
CURDIR="$(dirname $(readlink -f "$0"))"
export PATH=${CURDIR}/../../../..//deps/3rd/usr/local/oceanbase/devtools/bin/:/usr/local/bin:$PATH
export BISON_PKGDATADIR=${CURDIR}/../../../../deps/3rd/usr/local/oceanbase/devtools/share/bison/
# run bison
#bison -p obexpr -v -Werror -d ob_expr_parser.y -o ob_expr_parser_tab.c
bison -p obexpr -v -Werror -d ob_expr_parser.y -o ob_expr_parser_tab.c
if [ $? -ne 0 ]
then
    echo Compile error[$?], abort.
    exit 1
fi

# format tab.h
sed "s/YY\([a-zA-Z_]*\)/OBEXPR\1/g" -i ob_expr_parser_tab.h
sed "s/yy\([a-zA-Z_]*\)/obexpr\1/g" -i ob_expr_parser_tab.h
sed "/Tokens/i #ifndef YY_OBEXPR_OB_EXPR_PARSER_TAB_H_INCLUDED\n\
# define YY_OBEXPR_OB_EXPR_PARSER_TAB_H_INCLUDED\n\
/* Debug traces.  */\n\
#ifndef OBEXPRDEBUG\n\
# if defined YYDEBUG\n\
#if YYDEBUG\n\
#   define OBEXPRDEBUG 1\n\
#  else\n\
#   define OBEXPRDEBUG 0\n\
#  endif\n\
# else /* ! defined YYDEBUG */\n\
#  define OBEXPRDEBUG 0\n\
# endif /* ! defined YYDEBUG */\n\
#endif  /* ! defined OBEXPRDEBUG */\n\
#if OBEXPRDEBUG\n\
extern int obexprdebug;\n\
#endif" -i ob_expr_parser_tab.h
echo "#endif" >> ob_expr_parser_tab.h

# formart tab.c
sed "/#define yyparse/i #define YYSTYPE         OBEXPRSTYPE\n#define YYLTYPE         OBEXPRLTYPE" -i ob_expr_parser_tab.c
sed "/Tokens/,/Copy the second/{s/YY\([a-zA-Z_]\)/OBEXPR\1/g}" -i ob_expr_parser_tab.c
sed "/Tokens/,/Copy the second/{s/yy\([a-zA-Z_]\)/obexpr\1/g}" -i ob_expr_parser_tab.c
sed "s/yylex (\&yylval, \&yylloc)/yylex (\&yylval, \&yylloc, YYLEX_PARAM)/g" -i ob_expr_parser_tab.c
sed "/Tokens/i #ifndef YY_OBEXPR_OB_EXPR_PARSER_TAB_H_INCLUDED\n\
# define YY_OBEXPR_OB_EXPR_PARSER_TAB_H_INCLUDED\n\
/* Debug traces.  */\n\
#ifndef OBEXPRDEBUG\n\
# if defined YYDEBUG\n\
#if YYDEBUG\n\
#   define OBEXPRDEBUG 1\n\
#  else\n\
#   define OBEXPRDEBUG 0\n\
#  endif\n\
# else /* ! defined YYDEBUG */\n\
#  define OBEXPRDEBUG 0\n\
# endif /* ! defined YYDEBUG */\n\
#endif  /* ! defined OBEXPRDEBUG */\n\
#if OBEXPRDEBUG\n\
extern int obexprdebug;\n\
#endif" -i ob_expr_parser_tab.c
sed "/Copy the second/i #endif" -i ob_expr_parser_tab.c
sed "/Cause a token to be read/a \
\ \ if (SELECT_STMT_PARSE_MODE == result->parse_mode_) {\n\
    yychar = DUMMY_SELECT_CLAUSE;\n\
  } else if (INSERT_STMT_PARSE_MODE == result->parse_mode_) {\n\
    yychar = DUMMY_INSERT_CLAUSE;\n\
  }\n\
" -i ob_expr_parser_tab.c

# run flex
#flex -P obexpr -Cfea -o ob_expr_parser_lex.c ob_expr_parser.l ob_expr_parser_tab.h
flex -P obexpr -o ob_expr_parser_lex.c ob_expr_parser.l ob_expr_parser_tab.h

# format lex.h
sed "s/YYSTYPE/OBEXPRSTYPE/g" -i ob_expr_parser_lex.h
sed "s/YYLTYPE/OBEXPRLTYPE/g" -i ob_expr_parser_lex.h
sed "/static int yy_top_state (yyscan_t yyscanner );/d" -i ob_expr_parser_lex.c
sed "/static int yy_top_state/,/\}/d" -i ob_expr_parser_lex.c
sed "/\*yy_cp = '\\\0';/d" -i ob_expr_parser_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{/int i/d}" -i ob_expr_parser_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{/for ( i = 0; i < _yybytes_len; ++i )/d}" -i ob_expr_parser_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{s/\tbuf\[i\] = yybytes\[i\]/memcpy(buf, yybytes, _yybytes_len)/g}" -i ob_expr_parser_lex.c
