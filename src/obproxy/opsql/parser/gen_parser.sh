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
#bison -p obproxy -v -Werror -d ob_proxy_parser.y -o ob_proxy_parser_tab.c
bison -p obproxy -v -Werror -d ob_proxy_parser.y -o ob_proxy_parser_tab.c
if [ $? -ne 0 ]
then
    echo Compile error[$?], abort.
    exit 1
fi

# format tab.h
sed "s/YY\([a-zA-Z_]*\)/OBPROXY\1/g" -i ob_proxy_parser_tab.h
sed "s/yy\([a-zA-Z_]*\)/obproxy\1/g" -i ob_proxy_parser_tab.h
sed "/Tokens/i #ifndef YY_OBPROXY_OB_PROXY_PARSER_TAB_H_INCLUDED\n\
# define YY_OBPROXY_OB_PROXY_PARSER_TAB_H_INCLUDED\n\
/* Debug traces.  */\n\
#ifndef OBPROXYDEBUG\n\
# if defined YYDEBUG\n\
#if YYDEBUG\n\
#   define OBPROXYDEBUG 1\n\
#  else\n\
#   define OBPROXYDEBUG 0\n\
#  endif\n\
# else /* ! defined YYDEBUG */\n\
#  define OBPROXYDEBUG 0\n\
# endif /* ! defined YYDEBUG */\n\
#endif  /* ! defined OBPROXYDEBUG */\n\
#if OBPROXYDEBUG\n\
extern int obproxydebug;\n\
#endif" -i ob_proxy_parser_tab.h
echo "#endif" >> ob_proxy_parser_tab.h

# formart tab.c
sed "/#define yyparse/i #define YYSTYPE         OBPROXYSTYPE\n#define YYLTYPE         OBPROXYLTYPE" -i ob_proxy_parser_tab.c
sed "/Tokens/,/Copy the second/{s/YY\([a-zA-Z_]\)/OBPROXY\1/g}" -i ob_proxy_parser_tab.c
sed "/Tokens/,/Copy the second/{s/yy\([a-zA-Z_]\)/obproxy\1/g}" -i ob_proxy_parser_tab.c
sed "s/yylex (\&yylval, \&yylloc)/yylex (\&yylval, \&yylloc, YYLEX_PARAM)/g" -i ob_proxy_parser_tab.c
sed "/Tokens/i #ifndef YY_OBPROXY_OB_PROXY_PARSER_TAB_H_INCLUDED\n\
# define YY_OBPROXY_OB_PROXY_PARSER_TAB_H_INCLUDED\n\
/* Debug traces.  */\n\
#ifndef OBPROXYDEBUG\n\
# if defined YYDEBUG\n\
#if YYDEBUG\n\
#   define OBPROXYDEBUG 1\n\
#  else\n\
#   define OBPROXYDEBUG 0\n\
#  endif\n\
# else /* ! defined YYDEBUG */\n\
#  define OBPROXYDEBUG 0\n\
# endif /* ! defined YYDEBUG */\n\
#endif  /* ! defined OBPROXYDEBUG */\n\
#if OBPROXYDEBUG\n\
extern int obproxydebug;\n\
#endif" -i ob_proxy_parser_tab.c
sed "/Copy the second/i #endif" -i ob_proxy_parser_tab.c

# run flex
#flex -P obproxy -Cfea -o ob_proxy_parser_lex.c ob_proxy_parser.l ob_proxy_parser_tab.h
flex -P obproxy -o ob_proxy_parser_lex.c ob_proxy_parser.l ob_proxy_parser_tab.h

# format lex.h
sed "s/YYSTYPE/OBPROXYSTYPE/g" -i ob_proxy_parser_lex.h
sed "s/YYLTYPE/OBPROXYLTYPE/g" -i ob_proxy_parser_lex.h
sed "/static int yy_top_state (yyscan_t yyscanner );/d" -i ob_proxy_parser_lex.c
sed "/static int yy_top_state/,/\}/d" -i ob_proxy_parser_lex.c
sed "/\*yy_cp = '\\\0';/d" -i ob_proxy_parser_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{/int i/d}" -i ob_proxy_parser_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{/for ( i = 0; i < _yybytes_len; ++i )/d}" -i ob_proxy_parser_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{s/\tbuf\[i\] = yybytes\[i\]/memcpy(buf, yybytes, _yybytes_len)/g}" -i ob_proxy_parser_lex.c
