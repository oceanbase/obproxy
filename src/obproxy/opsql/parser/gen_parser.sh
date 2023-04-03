#!/bin/bash
#
# DATE: 2016-02-01
# DESCRIPTION:
#
set +x
CURDIR="$(dirname $(readlink -f "$0"))"
export PATH=${CURDIR}/../../../..//deps/3rd/usr/local/oceanbase/devtools/bin/:/usr/local/bin:$PATH
export BISON_PKGDATADIR=${CURDIR}/../../../../deps/3rd/usr/local/oceanbase/devtools/share/bison/
# generate oracle utf8 obproxy_parser(support multi_byte_space, multi_byte_comma, multi_byte_left_parenthesis, multi_byte_right_parenthesis)
##1.copy lex and yacc files
cat ob_proxy_parser.y > ob_proxy_parser_utf8.y
cat ob_proxy_parser.l > ob_proxy_parser_utf8.l
##2.replace name
sed  "s/ob_proxy_parser_yy/ob_proxy_parser_utf8_yy/g" -i ob_proxy_parser_utf8.y
sed  "s/ob_proxy_parser_yy/ob_proxy_parser_utf8_yy/g" -i ob_proxy_parser_utf8.l
sed  "s/ob_proxy_parser_lex/ob_proxy_parser_utf8_lex/g" -i ob_proxy_parser_utf8.y
sed  "s/ob_proxy_parser_lex/ob_proxy_parser_utf8_lex/g" -i ob_proxy_parser_utf8.l
sed  "s/ob_proxy_parser_tab/ob_proxy_parser_utf8_tab/g" -i ob_proxy_parser_utf8.l
sed  "s/ob_proxy_parser_fatal_error/ob_proxy_utf8_parser_fatal_error/g" -i ob_proxy_parser_utf8.y
sed  "s/ob_proxy_parser_fatal_error/ob_proxy_utf8_parser_fatal_error/g" -i ob_proxy_parser_utf8.l
sed  "s/obproxy_parse_sql/obproxy_parse_utf8_sql/g" -i ob_proxy_parser_utf8.y
##3.add multi_byte_space, multi_byte_comma, multi_byte_left_parenthesis, multi_byte_right_parenthesis code.
sed  "s/multi_byte_space              \[\\\u3000\]/multi_byte_space              ([\\\xe3\][\\\x80\][\\\x80])/g" -i ob_proxy_parser_utf8.l
sed  "s/multi_byte_comma              \[\\\uff0c\]/multi_byte_comma              ([\\\xef\][\\\xbc\][\\\x8c])/g" -i ob_proxy_parser_utf8.l
sed  "s/multi_byte_left_parenthesis   \[\\\uff08\]/multi_byte_left_parenthesis   ([\\\xef\][\\\xbc\][\\\x88])/g" -i ob_proxy_parser_utf8.l
sed  "s/multi_byte_right_parenthesis  \[\\\uff09\]/multi_byte_right_parenthesis  ([\\\xef\][\\\xbc\][\\\x89])/g" -i ob_proxy_parser_utf8.l
sed 's/space                   \[ \\t\\n\\r\\f\]/space                   (\[ \\t\\n\\r\\f\]|{multi_byte_space})/g' -i ob_proxy_parser_utf8.l
echo "U      [\x80-\xbf]
U_1_1  [\x80]
U_1_2  [\x81-\xbf]
U_1_3  [\x80-\xbb]
U_1_4  [\xbc]
U_1_5  [\xbd-\xbf]
U_1_6  [\x80-\x87]
U_1_7  [\x8a-\x8b]
U_1_8  [\x8d-\xbf]
U_2    [\xc2-\xdf]
U_3    [\xe0-\xe2]
U_3_1  [\xe3]
U_3_2  [\xe4-\xee]
U_3_3  [\xef]
U_4    [\xf0-\xf4]
u_except_space ({U_3_1}{U_1_2}{U}|{U_3_1}{U_1_1}{U_1_2})
u_except_comma_parenthesis ({U_3_3}{U_1_3}{U}|{U_3_3}{U_1_4}{U_1_6}|{U_3_3}{U_1_4}{U_1_7}|{U_3_3}{U_1_4}{U_1_8}|{U_3_3}{U_1_5}{U})
UTF8_CHAR ({U_2}{U}|{U_3}{U}{U}|{u_except_space}|{U_3_2}{U}{U}|{u_except_comma_parenthesis}|{U_4}{U}{U}{U})" > utf8.txt
sed '/following character status will be rewrite by gen_parse.sh according to connection character/d' -i ob_proxy_parser_utf8.l
sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//r utf8.txt' -i ob_proxy_parser_utf8.l
sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//d' -i ob_proxy_parser_utf8.l
sed 's/space            \[ \\t\\n\\r\\f\]/space            (\[ \\t\\n\\r\\f\]|{multi_byte_space})/g' -i ob_proxy_parser_utf8.l
sed 's/multi_byte_connect_char/UTF8_CHAR/g' -i ob_proxy_parser_utf8.l
##4.generate oracle utf8 parser files

# run bison
#bison -p obproxy -v -Werror -d ob_proxy_parser.y -o ob_proxy_parser_tab.c
bison -v -Werror -d ob_proxy_parser_utf8.y -o ob_proxy_parser_utf8_tab.c
if [ $? -ne 0 ]
then
    echo Compile error[$?], abort.
    exit 1
fi

# format tab.h
sed "s/YY\([a-zA-Z_]*\)/OBPROXY\1/g" -i ob_proxy_parser_utf8_tab.h
sed "s/yy\([a-zA-Z_]*\)/obproxy\1/g" -i ob_proxy_parser_utf8_tab.h
sed "/Tokens/i #ifndef YY_OBPROXY_OB_PROXY_PARSER_TAB_H_INCLUDED\n\
# define YY_OBPROXY_OB_PROXY_PARSER_TAB_H_INCLUDED\n\
/* Debug traces.  */\n\
#ifndef OBPROXY_UTF8_DEBUG\n\
# if defined YYDEBUG\n\
#if YYDEBUG\n\
#   define OBPROXY_UTF8_DEBUG 1\n\
#  else\n\
#   define OBPROXY_UTF8_DEBUG 0\n\
#  endif\n\
# else /* ! defined YYDEBUG */\n\
#  define OBPROXY_UTF8_DEBUG 0\n\
# endif /* ! defined YYDEBUG */\n\
#endif  /* ! defined OBPROXY_UTF8_DEBUG */\n\
#if OBPROXY_UTF8_DEBUG\n\
extern int ob_proxy_parser_utf8_yydebug;\n\
#endif" -i ob_proxy_parser_utf8_tab.h
echo "#endif" >> ob_proxy_parser_utf8_tab.h

# formart tab.c
sed "/#define yyparse/i #define YYSTYPE         OBPROXYSTYPE\n#define YYLTYPE         OBPROXYLTYPE" -i ob_proxy_parser_utf8_tab.c
sed "/Tokens/,/Copy the second/{s/YY\([a-zA-Z_]\)/OBPROXY\1/g}" -i ob_proxy_parser_utf8_tab.c
sed "/Tokens/,/Copy the second/{s/yy\([a-zA-Z_]\)/obproxy\1/g}" -i ob_proxy_parser_utf8_tab.c
sed "s/yylex (\&yylval, \&yylloc)/yylex (\&yylval, \&yylloc, YYLEX_PARAM)/g" -i ob_proxy_parser_utf8_tab.c
sed "/Tokens/i #ifndef YY_OBPROXY_OB_PROXY_PARSER_TAB_H_INCLUDED\n\
# define YY_OBPROXY_OB_PROXY_PARSER_TAB_H_INCLUDED\n\
/* Debug traces.  */\n\
#ifndef OBPROXY_UTF8_DEBUG\n\
# if defined YYDEBUG\n\
#if YYDEBUG\n\
#   define OBPROXY_UTF8_DEBUG 1\n\
#  else\n\
#   define OBPROXY_UTF8_DEBUG 0\n\
#  endif\n\
# else /* ! defined YYDEBUG */\n\
#  define OBPROXY_UTF8_DEBUG 0\n\
# endif /* ! defined YYDEBUG */\n\
#endif  /* ! defined OBPROXY_UTF8_DEBUG */\n\
#if OBPROXY_UTF8_DEBUG\n\
extern int ob_proxy_parser_utf8_yydebug;\n\
#endif" -i ob_proxy_parser_utf8_tab.c
sed "/Copy the second/i #endif" -i ob_proxy_parser_utf8_tab.c

# run flex
#flex -P obproxy -Cfea -o ob_proxy_parser_lex.c ob_proxy_parser.l ob_proxy_parser_tab.h
flex -o ob_proxy_parser_utf8_lex.c ob_proxy_parser_utf8.l ob_proxy_parser_utf8_tab.h

# format lex.h
sed "s/YYSTYPE/OBPROXYSTYPE/g" -i ob_proxy_parser_utf8_lex.h
sed "s/YYLTYPE/OBPROXYLTYPE/g" -i ob_proxy_parser_utf8_lex.h
sed "/static int yy_top_state (yyscan_t yyscanner );/d" -i ob_proxy_parser_utf8_lex.c
sed "/static int yy_top_state/,/\}/d" -i ob_proxy_parser_utf8_lex.c
sed "/\*yy_cp = '\\\0';/d" -i ob_proxy_parser_utf8_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{/int i/d}" -i ob_proxy_parser_utf8_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{/for ( i = 0; i < _yybytes_len; ++i )/d}" -i ob_proxy_parser_utf8_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{s/\tbuf\[i\] = yybytes\[i\]/memcpy(buf, yybytes, _yybytes_len)/g}" -i ob_proxy_parser_utf8_lex.c
##5.clean useless files
rm -f ob_proxy_parser_utf8.l
rm -f ob_proxy_parser_utf8.y
rm -f ob_proxy_parser_utf8.output


# generate oracle gbk obproxy_parser(support multi_byte_space, multi_byte_comma, multi_byte_left_parenthesis, multi_byte_right_parenthesis)
##1.copy lex and yacc files
cat ob_proxy_parser.y > ob_proxy_parser_gbk.y
cat ob_proxy_parser.l > ob_proxy_parser_gbk.l
##2.replace name
sed  "s/ob_proxy_parser_yy/ob_proxy_parser_gbk_yy/g" -i ob_proxy_parser_gbk.y
sed  "s/ob_proxy_parser_yy/ob_proxy_parser_gbk_yy/g" -i ob_proxy_parser_gbk.l
sed  "s/ob_proxy_parser_lex/ob_proxy_parser_gbk_lex/g" -i ob_proxy_parser_gbk.y
sed  "s/ob_proxy_parser_lex/ob_proxy_parser_gbk_lex/g" -i ob_proxy_parser_gbk.l
sed  "s/ob_proxy_parser_tab/ob_proxy_parser_gbk_tab/g" -i ob_proxy_parser_gbk.l
sed  "s/ob_proxy_parser_fatal_error/ob_proxy_gbk_parser_fatal_error/g" -i ob_proxy_parser_gbk.y
sed  "s/ob_proxy_parser_fatal_error/ob_proxy_gbk_parser_fatal_error/g" -i ob_proxy_parser_gbk.l
sed  "s/obproxy_parse_sql/obproxy_parse_gbk_sql/g" -i ob_proxy_parser_gbk.y
##3.add multi_byte_space, multi_byte_comma, multi_byte_left_parenthesis, multi_byte_right_parenthesis code.
sed  "s/multi_byte_space              \[\\\u3000\]/multi_byte_space              ([\\\xa1][\\\xa1])/g" -i ob_proxy_parser_gbk.l
sed  "s/multi_byte_comma              \[\\\uff0c\]/multi_byte_comma              ([\\\xa3][\\\xac])/g" -i ob_proxy_parser_gbk.l
sed  "s/multi_byte_left_parenthesis   \[\\\uff08\]/multi_byte_left_parenthesis   ([\\\xa3][\\\xa8])/g" -i ob_proxy_parser_gbk.l
sed  "s/multi_byte_right_parenthesis  \[\\\uff09\]/multi_byte_right_parenthesis  ([\\\xa3][\\\xa9])/g" -i ob_proxy_parser_gbk.l
sed 's/space                   \[ \\t\\n\\r\\f\]/space                   (\[ \\t\\n\\r\\f\]|{multi_byte_space})/g' -i ob_proxy_parser_gbk.l
echo "GB_1   [\x81-\xfe]
GB_1_1 [\x81-\xa0]
GB_1_2 [\xa1]
GB_1_3 [\xa2]
GB_1_4 [\xa3]
GB_1_5 [\xa4-\xf2]
GB_2   [\x40-\xfe]
GB_2_1 [\x40-\xa0]
GB_2_2 [\xa2-\xfe]
GB_2_3 [\x40-\xa7]
GB_2_4 [\xaa-\xab]
GB_2_5 [\xad-\xfe]
GB_3   [\x30-\x39]
g_except_space ({GB_1_2}{GB_2_1}|{GB_1_2}{GB_2_2})
g_except_comma_parenthesis ({GB_1_4}{GB_2_3}|{GB_1_4}{GB_2_4}|{GB_1_4}{GB_2_5})
GB_CHAR ({GB_1_1}{GB_2}|{g_except_space}|{GB_1_3}{GB_2}|{g_except_comma_parenthesis}|{GB_1_5}{GB_2}|{GB_1}{GB_3}{GB_1}{GB_3})" > gbk.txt
sed '/following character status will be rewrite by gen_parse.sh according to connection character/d' -i ob_proxy_parser_gbk.l
sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//r gbk.txt' -i ob_proxy_parser_gbk.l
sed '/multi_byte_connect_char       \/\*According to connection character to set by gen_parse.sh\*\//d' -i ob_proxy_parser_gbk.l
sed 's/space            \[ \\t\\n\\r\\f\]/space            (\[ \\t\\n\\r\\f\]|{multi_byte_space})/g' -i ob_proxy_parser_gbk.l
sed 's/multi_byte_connect_char/GB_CHAR/g' -i ob_proxy_parser_gbk.l
##4.generate oracle gbk parser files

# run bison
#bison -p obproxy -v -Werror -d ob_proxy_parser.y -o ob_proxy_parser_tab.c
bison -v -Werror -d ob_proxy_parser_gbk.y -o ob_proxy_parser_gbk_tab.c
if [ $? -ne 0 ]
then
    echo Compile error[$?], abort.
    exit 1
fi

# format tab.h
sed "s/YY\([a-zA-Z_]*\)/OBPROXY\1/g" -i ob_proxy_parser_gbk_tab.h
sed "s/yy\([a-zA-Z_]*\)/obproxy\1/g" -i ob_proxy_parser_gbk_tab.h
sed "/Tokens/i #ifndef YY_OBPROXY_OB_PROXY_PARSER_TAB_H_INCLUDED\n\
# define YY_OBPROXY_OB_PROXY_PARSER_TAB_H_INCLUDED\n\
/* Debug traces.  */\n\
#ifndef OBPROXY_GBK_DEBUG\n\
# if defined YYDEBUG\n\
#if YYDEBUG\n\
#   define OBPROXY_GBK_DEBUG 1\n\
#  else\n\
#   define OBPROXY_GBK_DEBUG 0\n\
#  endif\n\
# else /* ! defined YYDEBUG */\n\
#  define OBPROXY_GBK_DEBUG 0\n\
# endif /* ! defined YYDEBUG */\n\
#endif  /* ! defined OBPROXY_GBK_DEBUG */\n\
#if OBPROXY_GBK_DEBUG\n\
extern int ob_proxy_parser_gbk_yydebug;\n\
#endif" -i ob_proxy_parser_gbk_tab.h
echo "#endif" >> ob_proxy_parser_gbk_tab.h

# formart tab.c
sed "/#define yyparse/i #define YYSTYPE         OBPROXYSTYPE\n#define YYLTYPE         OBPROXYLTYPE" -i ob_proxy_parser_gbk_tab.c
sed "/Tokens/,/Copy the second/{s/YY\([a-zA-Z_]\)/OBPROXY\1/g}" -i ob_proxy_parser_gbk_tab.c
sed "/Tokens/,/Copy the second/{s/yy\([a-zA-Z_]\)/obproxy\1/g}" -i ob_proxy_parser_gbk_tab.c
sed "s/yylex (\&yylval, \&yylloc)/yylex (\&yylval, \&yylloc, YYLEX_PARAM)/g" -i ob_proxy_parser_gbk_tab.c
sed "/Tokens/i #ifndef YY_OBPROXY_OB_PROXY_PARSER_TAB_H_INCLUDED\n\
# define YY_OBPROXY_OB_PROXY_PARSER_TAB_H_INCLUDED\n\
/* Debug traces.  */\n\
#ifndef OBPROXY_GBK_DEBUG\n\
# if defined YYDEBUG\n\
#if YYDEBUG\n\
#   define OBPROXY_GBK_DEBUG 1\n\
#  else\n\
#   define OBPROXY_GBK_DEBUG 0\n\
#  endif\n\
# else /* ! defined YYDEBUG */\n\
#  define OBPROXY_GBK_DEBUG 0\n\
# endif /* ! defined YYDEBUG */\n\
#endif  /* ! defined OBPROXY_GBK_DEBUG */\n\
#if OBPROXY_GBK_DEBUG\n\
extern int ob_proxy_parser_gbk_yydebug;\n\
#endif" -i ob_proxy_parser_gbk_tab.c
sed "/Copy the second/i #endif" -i ob_proxy_parser_gbk_tab.c

# run flex
#flex -P obproxy -Cfea -o ob_proxy_parser_lex.c ob_proxy_parser.l ob_proxy_parser_tab.h
flex -o ob_proxy_parser_gbk_lex.c ob_proxy_parser_gbk.l ob_proxy_parser_gbk_tab.h

# format lex.h
sed "s/YYSTYPE/OBPROXYSTYPE/g" -i ob_proxy_parser_gbk_lex.h
sed "s/YYLTYPE/OBPROXYLTYPE/g" -i ob_proxy_parser_gbk_lex.h
sed "/static int yy_top_state (yyscan_t yyscanner );/d" -i ob_proxy_parser_gbk_lex.c
sed "/static int yy_top_state/,/\}/d" -i ob_proxy_parser_gbk_lex.c
sed "/\*yy_cp = '\\\0';/d" -i ob_proxy_parser_gbk_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{/int i/d}" -i ob_proxy_parser_gbk_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{/for ( i = 0; i < _yybytes_len; ++i )/d}" -i ob_proxy_parser_gbk_lex.c
sed "/Setup the input buffer state to scan the given bytes/,/}/{s/\tbuf\[i\] = yybytes\[i\]/memcpy(buf, yybytes, _yybytes_len)/g}" -i ob_proxy_parser_gbk_lex.c
##5.clean useless files
rm -f ob_proxy_parser_gbk.l
rm -f ob_proxy_parser_gbk.y
rm -f ob_proxy_parser_gbk.output
