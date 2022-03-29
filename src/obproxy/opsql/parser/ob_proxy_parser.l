%option noyywrap nounput noinput case-insensitive
%option stack noyyalloc noyyrealloc noyyfree
%option reentrant bison-bridge bison-locations
%option prefix="ob_proxy_parser_yy"
%option header-file="ob_proxy_parser_lex.h"
%{
#define YYSTYPE         OBPROXYSTYPE
#define YYLTYPE         OBPROXYLTYPE
#include <stdlib.h>
#include "opsql/ob_proxy_parse_define.h"
#include "opsql/ob_proxy_parse_malloc.h"
#include "opsql/parser/ob_proxy_parse_result.h"
#include "opsql/parser/ob_proxy_parser_tab.h"
extern void ob_proxy_parser_fatal_error(yyconst char *msg, yyscan_t yyscanner);
void update_stmt_type(ObProxyBasicStmtType type, void *yyscanner);
void store_str(char* str, int64_t str_len, char* end_ptr, void *yyscanner);
void store_str_with_quote(ObProxyParseQuoteType type, char* str, int64_t str_len, char* end_ptr, void *yyscanner);

#define YY_FATAL_ERROR(msg) ob_proxy_parser_fatal_error(msg, yyscanner)

#define SET_DML_STMT(stmt_type) \
do {\
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p)) {\
    if (OBPROXY_T_INVALID == p->cur_stmt_type_\
        || OBPROXY_T_BEGIN == p->cur_stmt_type_\
        || OBPROXY_T_SET == p->cur_stmt_type_) {\
      p->cur_stmt_type_ = stmt_type;\
    }\
  }\
} while (0);

#define SET_DDL_STMT(stmt_type) \
do {\
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p)) {\
    if (OBPROXY_T_INVALID == p->cur_stmt_type_\
        || OBPROXY_T_BEGIN == p->cur_stmt_type_) {\
      p->cur_stmt_type_ = stmt_type;\
    }\
  }\
} while (0);

#define SET_BASIC_STMT(stmt_type) \
do {\
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p)) {\
    if (OBPROXY_T_INVALID == p->cur_stmt_type_\
        || OBPROXY_T_BEGIN == p->cur_stmt_type_) {\
      p->cur_stmt_type_ = stmt_type;\
    }\
  }\
} while (0);

#define SET_ICMD_STMT(stmt_type) \
do {\
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p) && (OBPROXY_T_INVALID == p->cur_stmt_type_ || OBPROXY_T_SHOW == p->cur_stmt_type_)) {\
    p->cur_stmt_type_ = stmt_type;\
  }\
  store_str(yytext, yyleng, yytext + yyleng, yyscanner);\
} while (0);

#define SET_GLOBAL_SET_STMT() \
do {\
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p) && (OBPROXY_T_SET == p->cur_stmt_type_)) {\
    p->cur_stmt_type_ = OBPROXY_T_SET_GLOBAL;\
  }\
} while (0);

#define RETURN_NON_RESERVED_KEYWORD(word_type) \
  { store_str(yytext, yyleng, yytext + yyleng, yyscanner); return word_type; }

#define RETURN_NON_RESERVED_KEYWORD_WITH_QUOTE(word_type, quote_type) \
  { store_str_with_quote(quote_type, yytext + 1, yyleng - 2, yytext + yyleng, yyscanner); return word_type; }

#define RETURN_INT_NUM() \
do {\
  errno = 0;\
  yylval->num = strtoll(yytext, NULL, 10);\
  if (0 != errno) {\
    ObProxyParseResult *p = yyget_extra(yyscanner);\
    if (OB_NOTNULL(p) && OBPROXY_T_INVALID < p->cur_stmt_type_ && p->cur_stmt_type_ < OBPROXY_T_ICMD_MAX) {\
      p->cmd_info_.err_type_ = OBPROXY_T_ERR_INVALID_NUM;\
    } else {\
      yylval->num = 0;\
    }\
  }\
  return INT_NUM;\
} while (0);

#define RETURN_COMMENT_INT_NUM() \
do {\
  errno = 0;\
  yylval->num = strtoll(yytext, NULL, 10);\
  if (0 != errno) {\
    store_str(yytext, yyleng, yytext + yyleng, yyscanner);\
    return NAME_OB;\
  }\
  return INT_NUM;\
} while (0);

#define RETURN_NUMBER_VAL() \
  { store_str(yytext, yyleng, yytext + yyleng, yyscanner); return NUMBER_VAL; }

#define RETURN_COL_NAME(word_type) \
do { \
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p)) {\
    if (!p->has_ignored_word_) {\
      if (p->col_name_.str_len_ > 0) {\
        p->has_ignored_word_ = true;\
      } else {\
        p->col_name_.str_ = yytext;\
        p->col_name_.end_ptr_ = yytext + yyleng;\
        p->col_name_.str_len_ = yyleng;\
        return word_type;\
      }\
    }\
  }\
} while (0);

#define RETURN_IGNORED_WORD() \
do { \
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p)) {\
    p->has_ignored_word_ = true;\
    if (IN_TRANS_PARSE_MODE == p->parse_mode_) {\
      return ERROR;\
    }\
  }\
} while (0);

#define RETURN_IGNORED_WORD_FOR_DUAL() \
do { \
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p)) {\
    p->has_ignored_word_ = true;\
    p->is_dual_request_ = true; \
    if (IN_TRANS_PARSE_MODE == p->parse_mode_) {\
      return ERROR;\
    }\
  }\
} while (0);

#define SET_LAST_INSERT_ID() \
do { \
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p)) {\
    p->has_last_insert_id_ = true;\
  }\
} while (0);

#define SET_FOUND_ROWS() \
do { \
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p)) {\
    p->has_found_rows_ = true;\
  }\
} while (0);

#define SET_ROW_COUNT() \
do { \
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p)) {\
    p->has_row_count_ = true;\
  }\
} while (0);

#define SET_HAS_EXPLAIN() \
do { \
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p)) {\
    p->has_explain_ = true;\
  }\
} while (0);

#define RETURN_SHARD_COMMENT_NAME_OB_DOT(word_type) \
do {\
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p) && !p->has_shard_comment_) {\
    p->has_shard_comment_ = true;\
  }\
  store_str(yytext, yyleng - 1, yytext + yyleng - 1, yyscanner);\
  return word_type;\
} while (0);

#define RETURN_SHARD_COMMENT(word_type) \
do {\
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p) && !p->has_shard_comment_) {\
    p->has_shard_comment_ = true;\
  }\
  return word_type;\
} while (0);

#define SET_HAS_ANONYMOUS_BLOCK() \
do { \
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p)) {\
    if (OBPROXY_T_INVALID == p->cur_stmt_type_) {\
      p->has_anonymous_block_ = true;\
    } \
  }\
} while (0);

#define PUSH_STATE(state) \
  { yy_push_state(state, yyscanner); }

#define POP_STATE(state) \
  { yy_pop_state(yyscanner); }

#define PUSH_STATE_IF_NOT_ICMD(state) \
do {\
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p) && OBPROXY_T_INVALID < p->cur_stmt_type_ && p->cur_stmt_type_ < OBPROXY_T_ICMD_MAX) {\
    /*do nothing*/\
  } else {\
    yy_push_state(state, yyscanner);\
  }\
} while (0);

#define RETURN_WITH_CALL_CHECK(key) \
do {\
  ObProxyParseResult *p = yyget_extra(yyscanner);\
  if (OB_NOTNULL(p) && (OBPROXY_T_CALL == p->cur_stmt_type_ || OBPROXY_T_INSERT == p->cur_stmt_type_)) {\
    return key;\
  } else {\
    PUSH_STATE(INITIAL); \
    RETURN_IGNORED_WORD();\
  }\
} while (0);

#define ENTER_QUOTE_STATE(state) \
do {\
  PUSH_STATE(state); \
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra; \
  if (OB_NOTNULL(p)) { \
    p->tmp_buf_ = (yytext + 1); \
    p->tmp_start_ptr_ = yytext; \
    p->tmp_len_ = 0; \
  }\
} while (0);

%}

%x                      hint
%x                      in_c_comment
%x                      in_comment_expr
%x                      in_old_comment_expr
%x                      in_odp_comment_expr
%x                      in_odp_comment_value_expr
%x                      in_odp_comment_value_expr_calc
%x                      in_ignored_comment_expr
%x                      in_dbp_comment_expr
%x                      in_dbp_comment_trace_expr
%x                      in_dbp_comment_trace_value_expr
%x                      in_dbp_comment_trace_value_expr_calc
%x                      comment_sq
%x                      sq
%x                      dq
%x                      bt
%x                      bt_in_expr
%x                      in_expr
%x                      in_anonymous_block
%x                      in_subquery
%x                      in_no_select_query
%x                      prepare
%x                      insert_all_expr
%x                      set_expr
%x                      show_topology

/*following character status will be rewrite by gen_parse.sh according to connection character*/
multi_byte_space              [\u3000]
multi_byte_comma              [\uff0c]
multi_byte_left_parenthesis   [\uff08]
multi_byte_right_parenthesis  [\uff09]
space                   [ \t\n\r\f]
identifer               ([A-Za-z0-9$_]*)
int_num                 [\-\+]?[0-9]+
number                  ([0-9]+E[-+]?[0-9]+)|([0-9]+"."[0-9]*E[-+]?[0-9]+)|("."[0-9]+E[-+]?[0-9]+)|([0-9]+"."[0-9]*)|("."[0-9]+)
comment_identifer       [\-\+]?{identifer}
odp_comment_identifer   ([\-\+]?[A-Za-z0-9$_\-]*)

non_newline             [^\n\r]
sql_comment             ("--"{space}+{non_newline}*)|(#{non_newline}*)
whitespace              ({space}+|{sql_comment})
c_cmt_begin             \/\*
select_hint_begin       (select{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
update_hint_begin       (update{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
delete_hint_begin       (delete{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
insert_hint_begin       (insert{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
replace_hint_begin      (replace{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
merge_hint_begin        (merge{space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))

c_cmt_end               \*+\/

tx_read_only            (@@session{space}*\.{space}*tx_read_only|@@tx_read_only)
autocommit              (@@session{space}*\.{space}*autocommit|@@autocommit|autocommit)
autocommit_0            ({autocommit}{space}*={space}*0)
set_names               (set{space}+names{space}+)
set_charset             (set{space}+charset{space}+|set{space}+character{space}+set{space}+)
set_password            (set{space}+password{space}+)
set_default             (set{space}+default{space}+)
set_ob_read_consistency    (set{space}+(@@session{space}*\.{space}*ob_read_consistency|@@ob_read_consistency|ob_read_consistency){space}*)
set_tx_read_only        (set{space}+(@@session{space}*\.{space}*tx_read_only|@@tx_read_only|tx_read_only){space}*)

quote         '
sqbegin       {quote}
sqend         {quote}
sqdouble      {quote}{quote}
sqcontent     [^\\']+
qescape       [\\](.|\n)
sqnewline     {quote}{whitespace}{quote}

dquote         \"
dqbegin       {dquote}
dqend         {dquote}
dqdouble      {dquote}{dquote}
dqcontent     [^\\"]
dqnewline     {dquote}{whitespace}{dquote}

backtick      `
btbegin       {backtick}
btend         {backtick}
btdouble      {backtick}{backtick}
btcontent     [^`]+

comma         ,
leftbracket   \(
rightbracket  \)

show_proxynet             (show{space}+proxynet)
show_proxyconfig          (show{space}+proxyconfig)
show_proxysession         (show{space}+proxysession)
show_globalsession        (show{space}+globalsession)
show_processlist          (show{space}+processlist)
show_proxysm              (show{space}+proxysm)
show_proxycluster         (show{space}+proxycluster)
show_proxyresource        (show{space}+proxyresource)
show_proxycongestion      (show{space}+proxycongestion)
show_proxyroute           (show{space}+proxyroute)
show_proxyvip             (show{space}+proxyvip)
show_proxymemory          (show{space}+proxymemory)
show_sqlaudit             (show{space}+sqlaudit)
show_warnlog              (show{space}+warnlog)
show_proxystat            (show{space}+proxystat)
show_proxytrace           (show{space}+proxytrace)
show_proxyinfo            (show{space}+proxyinfo)
show_databases            (show{space}+databases)
show_tables               (show{space}+tables)
show_create_table         (show{space}+create{space}+table)
show_topology             (show{space}+elastic_id)
show_db_version           (show{space}+database_version)
select_database           (select{space}+database({leftbracket}|{multi_byte_left_parenthesis})({rightbracket}|{multi_byte_right_parenthesis}))
alter_proxyconfig         (alter{space}+proxyconfig)
alter_proxyresource       (alter{space}+proxyresource)
ping_proxy                (ping{space}+proxy)
kill_proxysession         (kill{space}+proxysession)
kill_globalsession         (kill{space}+globalsession)
select_proxy_version       (select{space}+proxy_version{space}*({leftbracket}|{multi_byte_left_parenthesis})({rightbracket}|{multi_byte_right_parenthesis}))

select_obproxy_route_addr (select{space}+@obproxy_route_addr)
set_obproxy_route_addr    (set{space}+@obproxy_route_addr)
%%

 /* basic dml stmt: */
SELECT           { SET_DML_STMT(OBPROXY_T_SELECT); PUSH_STATE(in_expr); return SELECT; }
DELETE           { SET_DML_STMT(OBPROXY_T_DELETE); PUSH_STATE_IF_NOT_ICMD(in_expr); return DELETE; }
INSERT           { SET_DML_STMT(OBPROXY_T_INSERT); return INSERT; }
UPDATE           { SET_DML_STMT(OBPROXY_T_UPDATE); return UPDATE; }
REPLACE          { SET_DML_STMT(OBPROXY_T_REPLACE); return REPLACE; }
MERGE            { SET_DML_STMT(OBPROXY_T_MERGE); return MERGE; }
SHOW             { SET_BASIC_STMT(OBPROXY_T_SHOW); return SHOW; }
XA               { RETURN_NON_RESERVED_KEYWORD(XA); }
 /* if begin is for starting transaction, will set has_anonymous_block = false in yacc */
BEGIN            { SET_HAS_ANONYMOUS_BLOCK(); RETURN_NON_RESERVED_KEYWORD(BEGI); }
START            { RETURN_NON_RESERVED_KEYWORD(START); }
COMMIT           { SET_BASIC_STMT(OBPROXY_T_COMMIT); RETURN_IGNORED_WORD(); }
ROLLBACK         { SET_BASIC_STMT(OBPROXY_T_ROLLBACK); RETURN_IGNORED_WORD(); }
SET              { SET_BASIC_STMT(OBPROXY_T_SET); PUSH_STATE_IF_NOT_ICMD(set_expr); return SET; }
CALL             { SET_BASIC_STMT(OBPROXY_T_CALL); return CALL; }
DECLARE          { SET_HAS_ANONYMOUS_BLOCK(); PUSH_STATE(in_anonymous_block); }
WHEN             { ObProxyParseResult *p = yyget_extra(yyscanner);
                   if (OB_NOTNULL(p)) {
                     if (OBPROXY_T_INSERT == p->cur_stmt_type_) 
                       PUSH_STATE(insert_all_expr);
                   }
                   return WHEN; }

 /* basic ddl stmt */
CREATE           { return CREATE; }
DROP             { return DROP; }
ALTER            { return ALTER; }
TRUNCATE         { return TRUNCATE; }
RENAME           { return RENAME; }
INDEX            { return INDEX; }
TABLE            { return TABLE; }
UNIQUE           { return UNIQUE; }

 /* ps stmt */
USING            { return USING; }
PREPARE          { PUSH_STATE(prepare); return PREPARE; }
EXECUTE          { return EXECUTE; }

 /* oracle ddl stmt */
GRANT            { return GRANT; }
REVOKE           { return REVOKE; }
ANALYZE          { return ANALYZE; }
PURGE            { return PURGE; }
COMMENT          { return COMMENT; }
FLASHBACK        { RETURN_NON_RESERVED_KEYWORD(FLASHBACK); }
AUDIT            { RETURN_NON_RESERVED_KEYWORD(AUDIT); }
NOAUDIT          { RETURN_NON_RESERVED_KEYWORD(NOAUDIT); }

GROUP            { return GROUP;}
HAVING           { return HAVING;}
ORDER            { return ORDER;}
FOR              { return FOR;}
UNION            { return UNION;}
AS               { return AS; }
WHERE            { return WHERE; }
VALUES           { return VALUES; }
VALUE            { return VALUES; }
EXPLAIN          { SET_HAS_EXPLAIN(); return EXPLAIN; }
DESC             { SET_HAS_EXPLAIN(); return DESC; }
DESCRIBE         { SET_HAS_EXPLAIN(); return DESCRIBE; }
 /*change from non_reserved to reserved according https://dev.mysql.com/doc/refman/5.6/en/keywords.html*/
READ             { return READ; }
WITH             { return WITH; }
USE              { return USE; }
LIMIT            { return LIMIT; }
ALL              { return ALL; }
LIKE             { return LIKE; }
PARTITION        { return PARTITION; }
BINARY           { return BINARY; }
GROUP_NAME       { return GROUP_NAME; }

 /* to make bison easy, do not return these reserved keyword in non strict mode */
IGNORE           { RETURN_IGNORED_WORD(); }
INTO             { RETURN_IGNORED_WORD(); }
LOW_PRIORITY     { RETURN_IGNORED_WORD(); }
DELAYED          { RETURN_IGNORED_WORD(); }
HIGH_PRIORITY    { RETURN_IGNORED_WORD(); }

 /* no reserved keyword , don't forget to add these keyword in .y */
 /*refer: https://dev.mysql.com/doc/refman/5.6/en/keywords.html*/
QUICK            { RETURN_NON_RESERVED_KEYWORD(QUICK); }
COUNT            { RETURN_NON_RESERVED_KEYWORD(COUNT); }
WARNINGS         { RETURN_NON_RESERVED_KEYWORD(WARNINGS); }
ERRORS           { RETURN_NON_RESERVED_KEYWORD(ERRORS); }
TRACE            { RETURN_NON_RESERVED_KEYWORD(TRACE); }
TRANSACTION      { RETURN_NON_RESERVED_KEYWORD(TRANSACTION); }

ONLY             { RETURN_NON_RESERVED_KEYWORD(ONLY); }

CONSISTENT       { RETURN_NON_RESERVED_KEYWORD(CONSISTENT); }
SNAPSHOT         { RETURN_NON_RESERVED_KEYWORD(SNAPSHOT); }

HELP             { RETURN_NON_RESERVED_KEYWORD(HELP); }


 /*set names*/
{set_names}         { SET_BASIC_STMT(OBPROXY_T_SET_NAMES); return SET_NAMES; }
 /*set charset*/
{set_charset}       { SET_BASIC_STMT(OBPROXY_T_SET_CHARSET); return SET_CHARSET; }
 /*set passwd*/
{set_password}      { SET_BASIC_STMT(OBPROXY_T_SET_PASSWORD); return SET_PASSWORD; }
 /*set default*/
{set_default}      { SET_BASIC_STMT(OBPROXY_T_SET_DEFAULT); return SET_DEFAULT; }

{set_ob_read_consistency} { SET_BASIC_STMT(OBPROXY_T_SET_OB_READ_CONSISTENCY); return SET_OB_READ_CONSISTENCY; }
{set_tx_read_only} { SET_BASIC_STMT(OBPROXY_T_SET_TX_READ_ONLY); return SET_TX_READ_ONLY; }

 /*internal cmd*/
 /*show net*/
{show_proxynet}  { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_NET); return SHOW_PROXYNET; }
THREAD           { RETURN_NON_RESERVED_KEYWORD(THREAD); }
CONNECTION       { RETURN_NON_RESERVED_KEYWORD(CONNECTION); }

OFFSET           { RETURN_NON_RESERVED_KEYWORD(OFFSET); }

 /*show session*/
{show_globalsession} { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_GLOBAL_SESSION); return SHOW_GLOBALSESSION; }
{show_proxysession} { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_SESSION); return SHOW_PROXYSESSION; }
{show_processlist}  { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_PROCESSLIST); return SHOW_PROCESSLIST; }
ATTRIBUTE           { RETURN_NON_RESERVED_KEYWORD(ATTRIBUTE); }
VARIABLES           { RETURN_NON_RESERVED_KEYWORD(VARIABLES); }

STAT                { RETURN_NON_RESERVED_KEYWORD(STAT); }

 /*show config*/
{show_proxyconfig}  { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_CONFIG); return SHOW_PROXYCONFIG; }

DIFF                { RETURN_NON_RESERVED_KEYWORD(DIFF); }
USER                { RETURN_NON_RESERVED_KEYWORD(USER); }

 /*show sm*/
{show_proxysm}      { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_SM); return SHOW_PROXYSM; }

 /*show cluster*/
{show_proxycluster} { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_CLUSTER); return SHOW_PROXYCLUSTER; }

 /*show resource*/
{show_proxyresource}    { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_RESOURCE); return SHOW_PROXYRESOURCE; }

 /*show congestion*/
{show_proxycongestion}  { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_CONGESTION); return SHOW_PROXYCONGESTION; }

 /*show route*/
{show_proxyroute}   { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_ROUTE); return SHOW_PROXYROUTE; }

ROUTINE             { RETURN_NON_RESERVED_KEYWORD(ROUTINE); }

 /*show vip*/
{show_proxyvip}     { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_VIP); return SHOW_PROXYVIP; }

 /*show memory*/
{show_proxymemory}  { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_MEMORY); return SHOW_PROXYMEMORY; }
OBJPOOL             { RETURN_NON_RESERVED_KEYWORD(OBJPOOL); }

 /*show sqlaudit*/
{show_sqlaudit}     { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_SQLAUDIT); return SHOW_SQLAUDIT; }

 /*show warnlog*/
{show_warnlog}     { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_WARNLOG); return SHOW_WARNLOG; }

 /*show stat*/
{show_proxystat}    { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_STAT); return SHOW_PROXYSTAT; }
REFRESH             { RETURN_NON_RESERVED_KEYWORD(REFRESH); }

 /*show trace*/
{show_proxytrace}   { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_TRACE); return SHOW_PROXYTRACE; }

 /*show info*/
{show_proxyinfo}    { SET_ICMD_STMT(OBPROXY_T_ICMD_SHOW_INFO); return SHOW_PROXYINFO; }
UPGRADE             { RETURN_NON_RESERVED_KEYWORD(UPGRADE); }
IDC                 { RETURN_NON_RESERVED_KEYWORD(IDC); }

{show_topology}    { SET_BASIC_STMT(OBPROXY_T_SHOW); PUSH_STATE(show_topology); return SHOW_TOPOLOGY; }
<show_topology>"FROM"                        { POP_STATE(); PUSH_STATE(INITIAL); return FROM; }
<show_topology>"WHERE"                       { POP_STATE(); PUSH_STATE(INITIAL); return WHERE; }
<show_topology><<EOF>>                       { return END_P; }
<show_topology>{whitespace}                  { }
<show_topology>;                             { POP_STATE(); PUSH_STATE(INITIAL); return yytext[0]; }
<show_topology>.                             { return ERROR; }

{show_db_version}  { SET_BASIC_STMT(OBPROXY_T_SHOW); return SHOW_DB_VERSION; }
{show_databases}    { SET_BASIC_STMT(OBPROXY_T_SHOW); return SHOW_DATABASES; }
{show_tables}       { SET_BASIC_STMT(OBPROXY_T_SHOW); return SHOW_TABLES; }
{show_create_table} { SET_BASIC_STMT(OBPROXY_T_SHOW); return SHOW_CREATE_TABLE; }
{select_database}   { SET_DML_STMT(OBPROXY_T_SELECT); return SELECT_DATABASE; }
{select_proxy_version}   { SET_DML_STMT(OBPROXY_T_SELECT_PROXY_VERSION); return SELECT_PROXY_VERSION; }

 /*alter config*/
{alter_proxyconfig} { SET_ICMD_STMT(OBPROXY_T_ICMD_ALTER_CONFIG); return ALTER_PROXYCONFIG; }

 /*alter resource*/
{alter_proxyresource} { SET_ICMD_STMT(OBPROXY_T_ICMD_ALTER_RESOURCE); return ALTER_PROXYRESOURCE; }

 /*ping proxy*/
{ping_proxy}        { SET_ICMD_STMT(OBPROXY_T_PING_PROXY); return PING_PROXY; }

 /*kill*/
{kill_proxysession} { SET_ICMD_STMT(OBPROXY_T_ICMD_KILL_SESSION); return KILL_PROXYSESSION; }
{kill_globalsession} { SET_ICMD_STMT(OBPROXY_T_ICMD_KILL_GLOBAL_SESSION); return KILL_GLOBALSESSION; }
KILL                { SET_ICMD_STMT(OBPROXY_T_ICMD_KILL_MYSQL); return KILL; }
QUERY               { RETURN_NON_RESERVED_KEYWORD(QUERY); }

 /* obproxy_route_addr */
{select_obproxy_route_addr} { return SELECT_OBPROXY_ROUTE_ADDR; }
{set_obproxy_route_addr}    { return SET_OBPROXY_ROUTE_ADDR; }

 /* identifer */
FOUND_ROWS       { SET_FOUND_ROWS(); RETURN_NON_RESERVED_KEYWORD(NAME_OB); }
ROW_COUNT        { SET_ROW_COUNT(); RETURN_NON_RESERVED_KEYWORD(NAME_OB); }
LAST_INSERT_ID   { SET_LAST_INSERT_ID(); RETURN_NON_RESERVED_KEYWORD(NAME_OB); }
{comma}|{multi_byte_comma} { return ','; }
{int_num}        { RETURN_INT_NUM(); }
{identifer}      { RETURN_NON_RESERVED_KEYWORD(NAME_OB); }
{sqbegin}{identifer}{sqend} { RETURN_NON_RESERVED_KEYWORD_WITH_QUOTE(NAME_OB, OBPROXY_QUOTE_T_SINGLE); }
{dqbegin}{identifer}{dqend} { RETURN_NON_RESERVED_KEYWORD_WITH_QUOTE(NAME_OB, OBPROXY_QUOTE_T_DOUBLE); }
{btbegin}{identifer}{btend} { RETURN_NON_RESERVED_KEYWORD_WITH_QUOTE(NAME_OB, OBPROXY_QUOTE_T_BACK); }
{number}         { RETURN_NUMBER_VAL(); }
"?"              { return PLACE_HOLDER; }
[.;*-+&~|^/%:!@=] { return yytext[0]; }
{leftbracket}|{rightbracket}   { RETURN_WITH_CALL_CHECK(yytext[0]); }
{multi_byte_left_parenthesis}  { RETURN_WITH_CALL_CHECK('('); }
{multi_byte_right_parenthesis} { RETURN_WITH_CALL_CHECK(')'); }
{whitespace}        { }

 /* hint option */
{select_hint_begin} {
  PUSH_STATE(in_expr);
  PUSH_STATE(hint);
  SET_BASIC_STMT(OBPROXY_T_SELECT);
  return SELECT_HINT_BEGIN;
}
{update_hint_begin} {
  PUSH_STATE(hint);
  SET_BASIC_STMT(OBPROXY_T_UPDATE);
  return UPDATE_HINT_BEGIN;
}
{delete_hint_begin} {
  PUSH_STATE(in_expr);
  PUSH_STATE(hint);
  SET_BASIC_STMT(OBPROXY_T_DELETE);
  return DELETE_HINT_BEGIN;
}
{insert_hint_begin} {
  PUSH_STATE(hint);
  SET_BASIC_STMT(OBPROXY_T_INSERT);
  return INSERT_HINT_BEGIN;
}
{replace_hint_begin} {
  PUSH_STATE(hint);
  SET_BASIC_STMT(OBPROXY_T_REPLACE);
  return REPLACE_HINT_BEGIN;
}
{merge_hint_begin} {
  PUSH_STATE(hint);
  SET_BASIC_STMT(OBPROXY_T_MERGE);
  return MERGE_HINT_BEGIN;
}

<set_expr>{autocommit_0}                { return AUTOCOMMIT_0; }
<set_expr>"global"                      { return GLOBAL; }
<set_expr>"session"                     { return SESSION; }
<set_expr>{int_num}                     { RETURN_INT_NUM(); }
<set_expr>{comma}|{multi_byte_comma}    { return ','; }
<set_expr>{identifer}                   { RETURN_NON_RESERVED_KEYWORD(NAME_OB); }
<set_expr>{number}                      { RETURN_NUMBER_VAL(); }
<set_expr>{whitespace}                  { }
<set_expr>[@=]                         { return yytext[0]; }
<set_expr><<EOF>>                       { return END_P; }
<set_expr>;                             { POP_STATE(); PUSH_STATE(INITIAL); return yytext[0]; }

<set_expr>{sqbegin} {
  PUSH_STATE(sq);
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_buf_ = (yytext + 1);
    p->tmp_start_ptr_ = yytext;
    p->tmp_len_ = 0;
  }
}

<set_expr>{dqbegin} {
  PUSH_STATE(dq);
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_buf_ = (yytext + 1);
    p->tmp_start_ptr_ = yytext;
    p->tmp_len_ = 0;
  }
}

<set_expr>{btbegin} {
  PUSH_STATE(bt);
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (NULL != p) {
    p->tmp_buf_ = (char *)obproxy_parse_malloc(OBPROXY_MAX_NAME_LENGTH, p->malloc_pool_);
    p->tmp_start_ptr_ = yytext;
    p->tmp_len_ = 0;
  }
}
<set_expr>.                             { return ERROR; }

<insert_all_expr>"into"        { POP_STATE(); RETURN_IGNORED_WORD(); }
<insert_all_expr>.             {}

<hint>{multi_byte_left_parenthesis}  { return '('; }
<hint>{multi_byte_right_parenthesis}  { return ')'; }
<hint>"query_timeout"     { return QUERY_TIMEOUT; }
<hint>{int_num}           { RETURN_INT_NUM(); }
<hint>"read_consistency"  { return READ_CONSISTENCY; }
<hint>WEAK                { return WEAK; }
<hint>STRONG              { return STRONG; }
<hint>FROZEN              { return FROZEN; }
<hint>INDEX               { return INDEX; }
<hint>{leftbracket}|{rightbracket}  { return yytext[0]; }
<hint>{identifer}         { RETURN_NON_RESERVED_KEYWORD(NAME_OB); }
<hint>{c_cmt_end}         { POP_STATE(); return HINT_END; }
<hint>.                   {}

 /* comment */
{c_cmt_begin} {
  PUSH_STATE(in_c_comment);
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    if (OBPROXY_T_INVALID == p->cur_stmt_type_) {
      if (NULL == p->comment_begin_) {
        p->comment_begin_ = yytext;
      }
      PUSH_STATE(in_comment_expr);
      return COMMENT_BEGIN;
    }
  }
}

<in_c_comment>{c_cmt_end} { POP_STATE(); }
<in_c_comment>[\n]        {}
<in_c_comment>.           {}

<in_comment_expr>{c_cmt_end} {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->comment_end_ = yytext + 2;
  }
  POP_STATE();
  POP_STATE();
  return COMMENT_END;
}

<in_comment_expr>.           { PUSH_STATE(in_old_comment_expr); }
<in_comment_expr>{space}*"table" {
  PUSH_STATE(in_old_comment_expr);
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->simple_route_info_.table_start_ptr_ = yytext;
    return ROUTE_TABLE;
  }
}

<in_comment_expr>{space}*"part_key" {
  PUSH_STATE(in_old_comment_expr);
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    if (OBPROXY_T_INVALID == p->cur_stmt_type_) {
      p->simple_route_info_.part_key_start_ptr_ = yytext;
      return ROUTE_PART_KEY;
    }
  }
}

<in_old_comment_expr>"table" {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->simple_route_info_.table_start_ptr_ = yytext;
    return ROUTE_TABLE;
  }
}

<in_old_comment_expr>"part_key" {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    if (OBPROXY_T_INVALID == p->cur_stmt_type_) {
      p->simple_route_info_.part_key_start_ptr_ = yytext;
      return ROUTE_PART_KEY;
    }
  }
}

<in_old_comment_expr>{identifer} { RETURN_NON_RESERVED_KEYWORD(NAME_OB); }
<in_old_comment_expr>{sqbegin}{identifer}{sqend} { RETURN_NON_RESERVED_KEYWORD_WITH_QUOTE(NAME_OB, OBPROXY_QUOTE_T_SINGLE); }
<in_old_comment_expr>{dqbegin}{identifer}{dqend} { RETURN_NON_RESERVED_KEYWORD_WITH_QUOTE(NAME_OB, OBPROXY_QUOTE_T_DOUBLE); }
<in_old_comment_expr>{btbegin}{identifer}{btend} { RETURN_NON_RESERVED_KEYWORD_WITH_QUOTE(NAME_OB, OBPROXY_QUOTE_T_BACK); }
<in_old_comment_expr>{c_cmt_end} {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->comment_end_ = yytext + 2;
  }
  POP_STATE();
  POP_STATE();
  POP_STATE();
  return COMMENT_END;
}
<in_old_comment_expr>. {}

<in_comment_expr>"ODP:"                      { PUSH_STATE(in_odp_comment_expr); RETURN_SHARD_COMMENT(ODP_COMMENT); }
<in_comment_expr>{space}*"group_id"          { PUSH_STATE(in_odp_comment_expr); RETURN_SHARD_COMMENT(GROUP_ID); }
<in_comment_expr>{space}*"table_id"          { PUSH_STATE(in_odp_comment_expr); RETURN_SHARD_COMMENT(TABLE_ID); }
<in_comment_expr>{space}*"table_name"        { PUSH_STATE(in_odp_comment_expr); RETURN_SHARD_COMMENT(TABLE_NAME); }
<in_comment_expr>{space}*"elastic_id"        { PUSH_STATE(in_odp_comment_expr); RETURN_SHARD_COMMENT(ELASTIC_ID); }
<in_comment_expr>{space}*"testload"          { PUSH_STATE(in_odp_comment_expr); RETURN_SHARD_COMMENT(TESTLOAD); }
<in_comment_expr>{space}*"disaster_status"   { PUSH_STATE(in_odp_comment_expr); RETURN_SHARD_COMMENT(DISASTER_STATUS); }
<in_comment_expr>{space}*"tnt_id"            { PUSH_STATE(in_odp_comment_expr); RETURN_SHARD_COMMENT(TNT_ID); }
<in_comment_expr>{space}*"trace_id"          { PUSH_STATE(in_odp_comment_expr); RETURN_SHARD_COMMENT(TRACE_ID); }
<in_comment_expr>{space}*{comment_identifer}"." { PUSH_STATE(in_odp_comment_expr); RETURN_SHARD_COMMENT_NAME_OB_DOT(NAME_OB_DOT); }
<in_comment_expr>{space}*"rpc_id"            { PUSH_STATE(in_odp_comment_expr); RETURN_SHARD_COMMENT(RPC_ID); }

<in_odp_comment_expr>"group_id"          { return GROUP_ID; }
<in_odp_comment_expr>"table_id"          { return TABLE_ID; }
<in_odp_comment_expr>"table_name"        { return TABLE_NAME; }
<in_odp_comment_expr>"elastic_id"        { return ELASTIC_ID; }
<in_odp_comment_expr>"testload"          { return TESTLOAD; }
<in_odp_comment_expr>"disaster_status"   { return DISASTER_STATUS; }
<in_odp_comment_expr>"tnt_id"            { return TNT_ID; }
<in_odp_comment_expr>"trace_id"          { return TRACE_ID; }
<in_odp_comment_expr>"rpc_id"            { return RPC_ID; }
<in_odp_comment_expr>{sqbegin}           { ENTER_QUOTE_STATE(comment_sq); }
<in_odp_comment_expr>{comma}|{multi_byte_comma}  { return ','; }
<in_odp_comment_expr>{odp_comment_identifer} { RETURN_NON_RESERVED_KEYWORD(NAME_OB); }
<in_odp_comment_expr>[=]                 { PUSH_STATE(in_odp_comment_value_expr); return yytext[0]; }
<in_odp_comment_expr>[.]                { return yytext[0]; }
<in_odp_comment_expr>{space}             {}
<in_odp_comment_expr>.                   { return ERROR; }
<in_odp_comment_expr>{c_cmt_end} {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->comment_end_ = yytext + 2;
  }
  POP_STATE();
  POP_STATE();
  POP_STATE();
  return COMMENT_END;
}

<in_odp_comment_value_expr>{sqbegin}            { ENTER_QUOTE_STATE(comment_sq); }
<in_odp_comment_value_expr>{comma}|{multi_byte_comma} {
  POP_STATE();
  return ',';
}
<in_odp_comment_value_expr>{c_cmt_end} {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->comment_end_ = yytext + 2;
  }
  POP_STATE();
  POP_STATE();
  POP_STATE();
  POP_STATE();
  return COMMENT_END;
}
<in_odp_comment_value_expr>{space} {}
<in_odp_comment_value_expr>. {
  do {
    PUSH_STATE(in_odp_comment_value_expr_calc)
    ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
    if (OB_NOTNULL(p)) {
      p->tmp_buf_ = yytext;
      p->tmp_start_ptr_ = yytext;
      p->tmp_len_ = 1;
    }
  } while (0);
}

<in_odp_comment_value_expr_calc>{sqbegin}|{comma}|{multi_byte_comma}|{space}|{c_cmt_end} {
  POP_STATE();
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p) && OB_NOTNULL(p->tmp_buf_)) {
    store_str(p->tmp_buf_, p->tmp_len_, p->tmp_start_ptr_ + p->tmp_len_, yyscanner);
    yyless(0);
    return NAME_STR;
  } else {
    RETURN_IGNORED_WORD();
  }
}

<in_odp_comment_value_expr_calc>. {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_len_ += yyleng;
  }
}

<in_comment_expr>"+DBP:"                    { PUSH_STATE(in_dbp_comment_expr); RETURN_SHARD_COMMENT(DBP_COMMENT); }
<in_dbp_comment_expr>"$ROUTE"            { return ROUTE_TAG; }
<in_dbp_comment_expr>"$SYS"              { return SYS_TAG; }
<in_dbp_comment_expr>"SCAN_ALL"          { return SCAN_ALL; }
<in_dbp_comment_expr>"SHARDING_KEY"      { return SHARD_KEY; }
<in_dbp_comment_expr>"table_name"        { return TABLE_NAME;}
<in_dbp_comment_expr>"parallelism"       { return PARALL; }
<in_dbp_comment_expr>"group_id"          { return GROUP_ID; }
<in_dbp_comment_expr>"TRACE"             { PUSH_STATE(in_dbp_comment_trace_expr); return TRACE; }
<in_dbp_comment_expr>{sqbegin}           { ENTER_QUOTE_STATE(comment_sq); }
<in_dbp_comment_expr>[={}]|{leftbracket}|{rightbracket}       { return yytext[0]; }
<in_dbp_comment_expr>{multi_byte_left_parenthesis}            { return '('; }
<in_dbp_comment_expr>{multi_byte_right_parenthesis}           { return ')'; }
<in_dbp_comment_expr>{comma}|{multi_byte_comma}  { return ','; }
<in_dbp_comment_expr>{space}             { }
<in_dbp_comment_expr>{comment_identifer} { RETURN_NON_RESERVED_KEYWORD(NAME_OB); }

<in_dbp_comment_expr>{c_cmt_end} {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->comment_end_ = yytext + 2;
  }
  POP_STATE();
  POP_STATE();
  POP_STATE();
  return COMMENT_END;
}
<in_dbp_comment_expr>.                   {return yytext[0];}
<in_dbp_comment_trace_expr>{leftbracket}|{multi_byte_left_parenthesis} { PUSH_STATE(in_dbp_comment_trace_value_expr); return '('; }
<in_dbp_comment_trace_expr>{space}       {}

<in_dbp_comment_trace_value_expr>{sqbegin}            { ENTER_QUOTE_STATE(comment_sq); }
<in_dbp_comment_trace_value_expr>{rightbracket}|{multi_byte_right_parenthesis}      {
  POP_STATE();
  POP_STATE();
  return ')';
}
<in_dbp_comment_trace_value_expr>[#]            {
  do {
    PUSH_STATE(in_dbp_comment_trace_value_expr_calc)
    ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
    if (OB_NOTNULL(p)) {
      p->tmp_buf_ = yytext;
      p->tmp_start_ptr_ = yytext;
      p->tmp_len_ = 1;
    }
    return yytext[0];
  } while (0);
}
<in_dbp_comment_trace_value_expr>{c_cmt_end} {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->comment_end_ = yytext + 2;
  }
  POP_STATE();
  POP_STATE();
  POP_STATE();
  POP_STATE();
  POP_STATE();
  return COMMENT_END;
}
<in_dbp_comment_trace_value_expr>{space}        {}
<in_dbp_comment_trace_value_expr>. {
  do {
    PUSH_STATE(in_dbp_comment_trace_value_expr_calc)
    ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
    if (OB_NOTNULL(p)) {
      p->tmp_buf_ = yytext;
      p->tmp_start_ptr_ = yytext;
      p->tmp_len_ = 0;
    }
  } while (0);
}

<in_dbp_comment_trace_value_expr_calc>{sqbegin}|[#]|{leftbracket}|{rightbracket}|{multi_byte_left_parenthesis}|{multi_byte_right_parenthesis}|{space}|{c_cmt_end} {
  POP_STATE();
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p) && OB_NOTNULL(p->tmp_buf_)) {
    store_str(p->tmp_buf_, p->tmp_len_, p->tmp_start_ptr_ + p->tmp_len_, yyscanner);
    yyless(0);
    return NAME_STR;
  } else {
    RETURN_IGNORED_WORD();
  }
}

<in_dbp_comment_trace_value_expr_calc>. {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_len_ += yyleng;
  }
}

 /* quote */
{sqbegin} {
  PUSH_STATE(sq);
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_buf_ = (yytext + 1);
    p->tmp_start_ptr_ = yytext;
    p->tmp_len_ = 0;
  }
}

<sq>{sqcontent} {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_len_ += yyleng;
  }
}

<sq>{sqend} {
  POP_STATE();
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)
      && OBPROXY_T_INVALID < p->cur_stmt_type_
      && (p->cur_stmt_type_ < OBPROXY_T_ICMD_MAX
          || OBPROXY_T_CALL == p->cur_stmt_type_
          || OBPROXY_T_SET == p->cur_stmt_type_)
      && OB_NOTNULL(p->tmp_buf_)) {
    store_str_with_quote(OBPROXY_QUOTE_T_SINGLE, p->tmp_buf_, p->tmp_len_, p->tmp_start_ptr_ + p->tmp_len_ + 2, yyscanner);
    return NAME_OB;
  } else {
    RETURN_IGNORED_WORD();
  }
}

<sq>{sqdouble}            {}
<sq>{qescape}             {}
<sq>{sqnewline}           {}
<sq><<EOF>>               { return ERROR; }

 /* comment sq return name_str */
<comment_sq>{sqcontent} {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_len_ += yyleng;
  }
}

<comment_sq>{sqend} {
  POP_STATE();
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p) && OB_NOTNULL(p->tmp_buf_)) {
    store_str(p->tmp_buf_, p->tmp_len_, p->tmp_start_ptr_ + p->tmp_len_ + 2, yyscanner);
    return NAME_STR;;
  } else {
    RETURN_IGNORED_WORD();
  }
}

<comment_sq>{sqdouble}            {}
<comment_sq>{qescape}             {}
<comment_sq>{sqnewline}           {}
<comment_sq><<EOF>>               { return ERROR; }

 /* dquote */
{dqbegin} {
  PUSH_STATE(dq);
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_buf_ = (yytext + 1);
    p->tmp_start_ptr_ = yytext;
    p->tmp_len_ = 0;
  }
}

<dq>{dqcontent} {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)) {
    p->tmp_len_ += yyleng;
  }
}

<dq>{dqend} {
  POP_STATE();
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (OB_NOTNULL(p)
      && OBPROXY_T_INVALID < p->cur_stmt_type_
      && (p->cur_stmt_type_ < OBPROXY_T_ICMD_MAX
          || OBPROXY_T_CALL == p->cur_stmt_type_
          || OBPROXY_T_SET == p->cur_stmt_type_)
      && OB_NOTNULL(p->tmp_buf_)) {
    store_str_with_quote(OBPROXY_QUOTE_T_DOUBLE, p->tmp_buf_, p->tmp_len_, p->tmp_start_ptr_ + p->tmp_len_ + 2, yyscanner);
    return NAME_OB;
  } else {
    RETURN_IGNORED_WORD();
  }
}

<dq>{dqdouble}            {}
<dq>{qescape}             {}
<dq>{dqnewline}           {}
<dq><<EOF>>               { return ERROR; }


 /* backtick */
{btbegin} {
  PUSH_STATE(bt);
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (NULL != p) {
    p->tmp_buf_ = (char *)obproxy_parse_malloc(OBPROXY_MAX_NAME_LENGTH, p->malloc_pool_);
    p->tmp_start_ptr_ = yytext;
    p->tmp_len_ = 0;
  }
}

<bt>{btdouble} {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (NULL != p && NULL != p->tmp_buf_ && p->tmp_len_ + 1 < OBPROXY_MAX_NAME_LENGTH) {
    p->tmp_buf_[p->tmp_len_++] = '`';
  }
}

<bt>{btcontent} {
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (NULL != p && NULL != p->tmp_buf_ && p->tmp_len_ + yyleng < OBPROXY_MAX_NAME_LENGTH) {
    memcpy(p->tmp_buf_ + p->tmp_len_, yytext, yyleng);
    p->tmp_len_ += yyleng;
  }
}

<bt>{btend} {
  POP_STATE();
  ObProxyParseResult *p = (ObProxyParseResult *)yyextra;
  if (NULL != p && NULL != p->tmp_buf_) {
    yylval->str.str_  = obproxy_parse_strndup(p->tmp_buf_, p->tmp_len_, p->malloc_pool_);
    yylval->str.str_len_ = p->tmp_len_;
    yylval->str.end_ptr_ = p->tmp_start_ptr_ + p->tmp_len_ + 2;
    yylval->str.quote_type_ = OBPROXY_QUOTE_T_BACK;
  }
  return NAME_OB;
}

<bt><<EOF>>  {
  return ERROR;
}

<bt_in_expr>{btdouble}  {}
<bt_in_expr>{btcontent} {}
<bt_in_expr>{btend}     { POP_STATE(); RETURN_IGNORED_WORD(); }
<bt_in_expr><<EOF>>     { return ERROR; }

 /* some useful keyword */
<in_expr>"FROM"{whitespace}+"DUAL"     { RETURN_IGNORED_WORD_FOR_DUAL(); }
<in_expr>"FROM"                        { POP_STATE(); PUSH_STATE(INITIAL); return FROM; }
<in_expr>{leftbracket}|{multi_byte_left_parenthesis}                 { PUSH_STATE(in_subquery); return '('; }
<in_expr>{dqbegin}                     { PUSH_STATE(dq); }
<in_expr>{sqbegin}                     { PUSH_STATE(sq); }
<in_expr>{btbegin}                     { PUSH_STATE(bt_in_expr); }
<in_expr>"found_rows"                  { SET_FOUND_ROWS(); RETURN_IGNORED_WORD(); }
<in_expr>"row_count"                   { SET_ROW_COUNT(); RETURN_IGNORED_WORD(); }
<in_expr>"last_insert_id"              { SET_LAST_INSERT_ID(); RETURN_IGNORED_WORD(); }
<in_expr>"global"                      { SET_GLOBAL_SET_STMT(); RETURN_IGNORED_WORD(); }
<in_expr>{tx_read_only}                { RETURN_COL_NAME(TX_READ_ONLY); }
<in_expr>{autocommit_0}                { return AUTOCOMMIT_0; }
<in_expr>{identifer}                   { RETURN_IGNORED_WORD(); }
<in_expr><<EOF>>                       { return END_P; }
<in_expr>{whitespace}                  { }
<in_expr>;                             { POP_STATE(); PUSH_STATE(INITIAL); return yytext[0]; }
<in_expr>.                             { RETURN_IGNORED_WORD(); }

<in_subquery>"SELECT"                  { PUSH_STATE(in_expr); return SELECT; }
<in_subquery>{leftbracket}|{multi_byte_left_parenthesis}             { PUSH_STATE(in_subquery); return '('; }
<in_subquery>{rightbracket}|{multi_byte_right_parenthesis}            { POP_STATE(); return ')'; }
<in_subquery>{dqbegin}                 { PUSH_STATE(dq); }
<in_subquery>{sqbegin}                 { PUSH_STATE(sq); }
<in_subquery><<EOF>>                   { return END_P; }
<in_subquery>{whitespace}              { }
<in_subquery>.                         { PUSH_STATE(in_no_select_query); RETURN_IGNORED_WORD(); }

<in_no_select_query>{leftbracket}|{multi_byte_left_parenthesis}      { PUSH_STATE(in_subquery); return '('; }
<in_no_select_query>{rightbracket}|{multi_byte_right_parenthesis}     { POP_STATE(); POP_STATE(); return ')'; }
<in_no_select_query>{dqbegin}          { PUSH_STATE(dq); }
<in_no_select_query>{sqbegin}          { PUSH_STATE(sq); }
<in_no_select_query>"found_rows"       { SET_FOUND_ROWS(); RETURN_IGNORED_WORD(); }
<in_no_select_query>"row_count"        { SET_ROW_COUNT(); RETURN_IGNORED_WORD(); }
<in_no_select_query>"last_insert_id"   { SET_LAST_INSERT_ID(); RETURN_IGNORED_WORD(); }
<in_no_select_query>{identifer}        { RETURN_IGNORED_WORD(); }
<in_no_select_query><<EOF>>            { return END_P; }
<in_no_select_query>{whitespace}       { }
<in_no_select_query>.                  { RETURN_IGNORED_WORD(); }

<prepare>"FROM"                        { return FROM; }
<prepare>{identifer}                   { RETURN_NON_RESERVED_KEYWORD(NAME_OB); }
<prepare>{dquote}                      { POP_STATE(); }
<prepare>{quote}                       { POP_STATE(); }

<in_anonymous_block>"BEGIN"            { POP_STATE(); return BEGI;}
<in_anonymous_block>.                  {}

<<EOF>>                     { return END_P; }
.                           { RETURN_IGNORED_WORD(); }
%%
inline void *yyalloc(size_t bytes, void *yyscanner)
{
  void *ptr_ret = NULL;
  ObProxyParseResult *p = yyget_extra(yyscanner);
  if (OB_ISNULL(p)) {
    // print err into msg buffer later
  } else {
    ptr_ret = obproxy_parse_malloc(bytes, p->malloc_pool_);
  }
  return ptr_ret;
}

inline void *yyrealloc (void *ptr, size_t bytes, void *yyscanner)
{
  void *ptr_ret = NULL;
  ObProxyParseResult *p = yyget_extra(yyscanner);
  if (OB_ISNULL(p)) {
    // print err into msg buffer later
  } else {
    ptr_ret = obproxy_parse_realloc(ptr, bytes, p->malloc_pool_);
  }
  return ptr_ret;

}

inline void yyfree(void *ptr, void *yyscanner)
{
  // Do nothing -- we leave it to the garbage collector.
  obproxy_parse_free(ptr);
}

inline void update_stmt_type(ObProxyBasicStmtType type, void *yyscanner)
{
  ObProxyParseResult *p = yyget_extra(yyscanner);
  if (OB_ISNULL(p)) {
    // print err into msg buffer later
  } else {
    // we only update the cur_stmt_type_ at the first time
    if (OBPROXY_T_INVALID == p->cur_stmt_type_) {
      p->cur_stmt_type_ = type;
    } else {
      // do nothing
    }
  }
}

inline void store_str(char* str, int64_t str_len, char* end_ptr, void *yyscanner)
{
  YYSTYPE *lval = yyget_lval(yyscanner);
  if (OB_ISNULL(lval)) {
    // do nothing
  } else {
    lval->str.str_ = str;
    lval->str.end_ptr_ = end_ptr;
    lval->str.str_len_ = str_len;
    lval->str.quote_type_ = OBPROXY_QUOTE_T_INVALID;
  }
}

inline void store_str_with_quote(ObProxyParseQuoteType type, char* str, int64_t str_len, char* end_ptr, void *yyscanner)
{
  YYSTYPE *lval = yyget_lval(yyscanner);
  if (OB_ISNULL(lval)) {
    // do nothing
  } else {
    lval->str.str_ = str;
    lval->str.end_ptr_ = end_ptr;
    lval->str.str_len_ = str_len;
    lval->str.quote_type_ = type;
  }
}
