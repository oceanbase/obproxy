%define api.pure
%parse-param {ObProxyParseResult* result}
%name-prefix "ob_proxy_parser_yy"
%locations
%no-lines
%verbose
%{
#include <stdint.h>
#include "opsql/ob_proxy_parse_define.h"
#include "opsql/parser/ob_proxy_parse_result.h"

#define HANDLE_ACCEPT() \
do {\
  if (result->stmt_count_ > 1) {\
    result->stmt_type_ = OBPROXY_T_MULTI_STMT;\
  }\
  if (NULL != result->end_pos_) {\
  } else if (NULL != result->table_info_.table_name_.str_ && result->table_info_.table_name_.str_len_ > 0) {\
    if (NULL != result->part_name_.str_ && result->part_name_.str_len_ > 0) {\
      result->end_pos_ = result->part_name_.end_ptr_;\
    } else if (NULL != result->table_info_.alias_name_.str_ && result->table_info_.alias_name_.str_len_ > 0) {\
      result->end_pos_ = result->table_info_.alias_name_.end_ptr_;\
    } else {\
      result->end_pos_ = result->table_info_.table_name_.end_ptr_;\
    }\
  } else {\
    result->end_pos_ = ob_proxy_parser_yyget_text(result->yyscan_info_);\
  }\
  YYACCEPT;\
} while (0);

static inline void handle_stmt_end(ObProxyParseResult* result)
{
  // no need to judge NULL
  if (result->has_ignored_word_) {
    switch (result->cur_stmt_type_) {
      // these stmt should match exactly,
      // so if we have ignored word we should reset type
      case OBPROXY_T_SELECT_TX_RO:
        result->stmt_type_ = OBPROXY_T_SELECT;
        break;
      case OBPROXY_T_BEGIN:
        result->stmt_type_ = OBPROXY_T_OTHERS;
        break;
      case OBPROXY_T_SHOW_TRACE:
        result->stmt_type_ = OBPROXY_T_OTHERS;
        break;
      case OBPROXY_T_SELECT_ROUTE_ADDR:
        result->stmt_type_ = OBPROXY_T_OTHERS;
        break;
      case OBPROXY_T_SET_ROUTE_ADDR:
        result->stmt_type_ = OBPROXY_T_OTHERS;
        break;
      default:
        result->stmt_type_ = result->cur_stmt_type_;
        break;
    }
  } else {
    result->stmt_type_ = result->cur_stmt_type_;
  }

  if (OBPROXY_T_TEXT_PS_PREPARE == result->text_ps_inner_stmt_type_) {
    ObProxyBasicStmtType tmp_type = result->cur_stmt_type_;
    result->stmt_type_ = OBPROXY_T_TEXT_PS_PREPARE;
    result->text_ps_inner_stmt_type_ = tmp_type;
  }

  result->cur_stmt_type_ = OBPROXY_T_INVALID;
  result->stmt_count_++;
}

#define UPDATE_ALIAS_NAME(name) \
    /* only support select and update with alias name */ \
    /* insert into ... select 语法也需要支持 alias name */ \
    if (NULL != result && (OBPROXY_T_SELECT == result->cur_stmt_type_ || OBPROXY_T_UPDATE == result->cur_stmt_type_ \
                           || OBPROXY_T_INSERT == result->cur_stmt_type_ || OBPROXY_T_MERGE == result->cur_stmt_type_)) { \
      result->table_info_.alias_name_ = name; \
    } \

#define HANDLE_ERROR_ACCEPT() \
do {\
  result->has_ignored_word_ = true;\
  if ((OBPROXY_T_INVALID < result->cur_stmt_type_ && result->cur_stmt_type_ < OBPROXY_T_ICMD_MAX) || (OBPROXY_T_PING_PROXY == result->cur_stmt_type_)) {\
    result->cmd_info_.err_type_ = OBPROXY_T_ERR_PARSE;\
  }\
  handle_stmt_end(result);\
  HANDLE_ACCEPT();\
} while (0);

#define SET_ICMD_SUB_TYPE(sub_type) \
do {\
  result->cmd_info_.sub_type_ = sub_type;\
} while (0);

#define SET_ICMD_SUB_AND_ONE_ID(sub_type, id) \
do {\
  result->cmd_info_.sub_type_ = sub_type;\
  result->cmd_info_.integer_[0] = id;\
} while (0);

#define SET_ICMD_SUB_AND_TWO_ID(sub_type, id, id_two) \
do {\
  result->cmd_info_.sub_type_ = sub_type;\
  result->cmd_info_.integer_[0] = id;\
  result->cmd_info_.integer_[1] = id_two;\
} while (0);

#define SET_ICMD_SUB_AND_ONE_STRING(sub_type, string) \
do {\
  result->cmd_info_.sub_type_ = sub_type;\
  result->cmd_info_.string_[0] = string;\
} while (0);

#define SET_ICMD_ONE_STRING(string) \
do {\
  result->cmd_info_.string_[0] = string;\
} while (0);

#define SET_ICMD_TWO_STRING(string, string_two) \
do {\
  result->cmd_info_.string_[0] = string;\
  result->cmd_info_.string_[1] = string_two;\
} while (0);

#define SET_ICMD_SECOND_STRING(string) \
do {\
  result->cmd_info_.string_[1] = string;\
} while (0);

#define SET_ICMD_CONFIG_INT_VALUE(string, integer) \
do {\
  result->cmd_info_.sub_type_ = OBPROXY_T_SUB_CONFIG_INT_VAULE;\
  result->cmd_info_.string_[0] = string;\
  result->cmd_info_.integer_[0] = integer;\
} while (0);

#define SET_ICMD_TYPE_STRING_INT_VALUE(sub_type, string, integer) \
do {\
  result->cmd_info_.sub_type_ = sub_type;\
  result->cmd_info_.string_[0] = string;\
  result->cmd_info_.integer_[0] = integer;\
} while (0);

#define SET_ICMD_ONE_ID(id) \
do {\
  result->cmd_info_.integer_[0] = id;\
} while (0);

#define SET_ICMD_TWO_ID(id, id_two) \
do {\
  result->cmd_info_.integer_[0] = id;\
  result->cmd_info_.integer_[1] = id_two;\
} while (0);

#define SET_ICMD_SECOND_ID(id) \
do {\
  result->cmd_info_.integer_[1] = id;\
} while (0);

#define SET_READ_CONSISTENCY(read_consistency_type) \
do {\
  if (OBPROXY_READ_CONSISTENCY_INVALID == result->read_consistency_type_) {\
    result->read_consistency_type_ = read_consistency_type;\
  }\
} while (0);

#define add_call_node(call_parse_info, call_node) \
do {                                                      \
  if (NULL != call_parse_info.tail_) {\
    call_parse_info.tail_->next_ = call_node;\
    call_parse_info.tail_ = call_node;\
  } else {\
    call_parse_info.head_ = call_node;\
    call_parse_info.tail_ = call_node;\
  }\
  ++call_parse_info.node_count_;\
} while(0)

#define malloc_call_node(call_node, type) \
do {                                                                                        \
  if (OB_ISNULL(call_node = ((ObProxyCallParseNode *)obproxy_parse_malloc(sizeof(ObProxyCallParseNode), result->malloc_pool_)))) { \
    YYABORT;                                                                                \
  } else {                                                                                  \
    call_node->type_ = type;                                                                \
    call_node->next_ = NULL;                                                                 \
  }                                                                                         \
} while(0)                                                                                  \

#define add_text_ps_node(text_ps_parse_info, parse_node) \
do {                                                      \
  if (NULL != text_ps_parse_info.tail_) {\
    text_ps_parse_info.tail_->next_ = parse_node;\
    text_ps_parse_info.tail_ = parse_node;\
  } else {\
    text_ps_parse_info.head_ = parse_node;\
    text_ps_parse_info.tail_ = parse_node;\
  }\
  ++text_ps_parse_info.node_count_;\
} while(0)

#define malloc_parse_node(parse_node) \
do {                                                                                        \
  if (OB_ISNULL(parse_node = ((ObProxyTextPsParseNode *)obproxy_parse_malloc(sizeof(ObProxyTextPsParseNode), result->malloc_pool_)))) { \
    YYABORT;                                                                                \
  } else {                                                                                  \
    parse_node->next_ = NULL;                                                       \
  }                                                                                         \
} while(0)                                                                                  \

#define malloc_shard_column_node(col_node, tb_name, col_name, col_type) \
do {                                                      \
  if (OB_ISNULL(col_node = ((ObShardColumnNode *)obproxy_parse_malloc(sizeof(ObShardColumnNode), result->malloc_pool_)))) { \
    YYABORT;                                                                                \
  }                                                                                         \
  col_node->tb_name_ = tb_name;\
  col_node->col_name_ = col_name;\
  col_node->type_ = col_type;\
} while(0)

#define add_shard_column_node(route_info, col_node) \
do {                                                \
  col_node->next_ = NULL;\
  if (NULL != route_info.tail_) {\
    route_info.tail_->next_ = col_node;\
    route_info.tail_ = col_node;\
  } else {\
    route_info.head_ = col_node;\
    route_info.tail_ = col_node;\
  }\
  ++route_info.node_count_;\
} while(0)

#define add_hint_index(route_info, index_tb_name)   \
do {                                                \
  if (route_info.index_count_ >=0 && route_info.index_count_ < OBPROXY_MAX_HINT_INDEX_COUNT) {\
    route_info.index_tb_name_[route_info.index_count_] = index_tb_name; \
  }\
} while(0)

#define malloc_set_var_node(var_node, value_type) \
do {                                                      \
  if (OB_ISNULL(var_node = ((ObProxySetVarNode *)obproxy_parse_malloc(sizeof(ObProxySetVarNode), result->malloc_pool_)))) { \
    YYABORT;                                                                                \
  }                                                                                         \
  var_node->value_type_ = value_type;\
  var_node->next_ = NULL;\
} while(0)

#define add_set_var_node(set_info, var_node, name, type) \
do {                                                     \
  var_node->name_ = name; \
  var_node->type_ = type; \
  if (NULL != set_info.tail_) {\
    set_info.tail_->next_ = var_node;\
    set_info.tail_ = var_node;\
  } else {\
    set_info.head_ = var_node;\
    set_info.tail_ = var_node;\
  }\
  ++set_info.node_count_;\
} while(0)

#define SET_BASIC_STMT(stmt_type) \
do {\
  if (OBPROXY_T_INVALID == result->cur_stmt_type_\
      || OBPROXY_T_BEGIN == result->cur_stmt_type_) {\
    result->cur_stmt_type_ = stmt_type;\
  }\
} while (0);

%}

%union
{
  int64_t               num;
  ObProxyParseString    str;
  ObProxyCallParseNode  *node;
  ObShardColumnNode     *shard_node;
  ObProxySetVarNode     *var_node;
};

%{
#include "ob_proxy_parser_lex.h"
#define YYLEX_PARAM result->yyscan_info_
extern void yyerror(YYLTYPE* yylloc, ObProxyParseResult* p, char* s,...);
extern void *obproxy_parse_malloc(const size_t nbyte, void *malloc_pool);
%}

 /* dummy token */
%token DUMMY_WHERE_CLAUSE DUMMY_INSERT_CLAUSE
 /* reserved keyword */
%token SELECT DELETE INSERT UPDATE REPLACE MERGE SHOW SET CALL CREATE DROP ALTER TRUNCATE RENAME TABLE UNIQUE
%token GRANT REVOKE ANALYZE PURGE COMMENT
%token FROM DUAL
%token PREPARE EXECUTE USING DEALLOCATE
%token COMMENT_BEGIN COMMENT_END ROUTE_TABLE ROUTE_PART_KEY QUERY_TIMEOUT READ_CONSISTENCY WEAK STRONG FROZEN PLACE_HOLDER
%token END_P ERROR
%token WHEN
%token TABLEGROUP /*OB 特有的保留关键字*/
 /* non-reserved keyword */
%token<str> FLASHBACK AUDIT NOAUDIT STATUS
%token<str> BEGI START TRANSACTION READ ONLY WITH CONSISTENT SNAPSHOT INDEX XA
%token<str> WARNINGS ERRORS TRACE
%token<str> QUICK COUNT AS WHERE VALUES ORDER GROUP HAVING INTO UNION FOR
%token<str> TX_READ_ONLY SELECT_OBPROXY_ROUTE_ADDR SET_OBPROXY_ROUTE_ADDR
%token<str> NAME_OB_DOT NAME_OB EXPLAIN EXPLAIN_ROUTE DESC DESCRIBE NAME_STR
%token<str> LOAD DATA HINT_BEGIN LOCAL INFILE
%token<str> USE HELP SET_NAMES SET_CHARSET SET_PASSWORD SET_DEFAULT SET_OB_READ_CONSISTENCY SET_TX_READ_ONLY GLOBAL SESSION
%token<str> NUMBER_VAL
%token<str> GROUP_ID TABLE_ID ELASTIC_ID TESTLOAD ODP_COMMENT TNT_ID DISASTER_STATUS TRACE_ID RPC_ID TARGET_DB_SERVER TRACE_LOG
%token<str> DBP_COMMENT ROUTE_TAG SYS_TAG TABLE_NAME SCAN_ALL STICKY_SESSION PARALL SHARD_KEY STOP_DDL_TASK RETRY_DDL_TASK
%token<num> INT_NUM
%type<str> right_string_val tracer_right_string_val name_right_string_val
%type<node> call_expr
%type<shard_node> odp_comment comment_expr
%type<var_node> set_expr set_var_value
 /*internal cmd keyword*/
%token<str> SHOW_PROXYNET THREAD CONNECTION LIMIT OFFSET
%token<str> SHOW_PROCESSLIST SHOW_PROXYSESSION SHOW_GLOBALSESSION ATTRIBUTE VARIABLES ALL STAT READ_STALE
%token<str> SHOW_PROXYCONFIG DIFF USER LIKE
%token<str> SHOW_PROXYSM
%token<str> SHOW_PROXYCLUSTER
%token<str> SHOW_PROXYRESOURCE
%token<str> SHOW_PROXYCONGESTION
%token<str> SHOW_PROXYROUTE PARTITION ROUTINE SUBPARTITION
%token<str> SHOW_PROXYVIP
%token<str> SHOW_PROXYMEMORY OBJPOOL
%token<str> SHOW_SQLAUDIT
%token<str> SHOW_WARNLOG
%token<str> SHOW_PROXYSTAT REFRESH
%token<str> SHOW_PROXYTRACE
%token<str> SHOW_PROXYINFO BINARY UPGRADE IDC
%token<str> SHOW_ELASTIC_ID SHOW_TOPOLOGY GROUP_NAME SHOW_DB_VERSION
%token<str> SHOW_DATABASES SHOW_TABLES SHOW_FULL_TABLES SELECT_DATABASE SELECT_PROXY_STATUS
%token<str> SHOW_CREATE_TABLE SELECT_PROXY_VERSION SHOW_COLUMNS SHOW_INDEX
%token<str> ALTER_PROXYCONFIG
%token<str> ALTER_PROXYRESOURCE
%token<str> PING_PROXY
%token<str> KILL_PROXYSESSION KILL_GLOBALSESSION KILL QUERY
%token<str> SHOW_BINLOG_SERVER_FOR_TENANT

%type<str> table_factor non_reserved_keyword var_name
%start root
%%
root: sql_stmts { HANDLE_ACCEPT(); }
    | error     { HANDLE_ERROR_ACCEPT(); }

sql_stmts: sql_stmt
         | sql_stmts sql_stmt

sql_stmt: comment_stmt END_P     { handle_stmt_end(result); HANDLE_ACCEPT(); }
        | comment_stmt ';'       { handle_stmt_end(result); }
        | comment_stmt ';' END_P { handle_stmt_end(result); HANDLE_ACCEPT(); }
        | ';'            { handle_stmt_end(result); }
        | ';' END_P      { handle_stmt_end(result); HANDLE_ACCEPT(); }
        | BEGI comment_stmt ';'  { handle_stmt_end(result); }

comment_stmt: stmt
            | comment_expr_list stmt

stmt: select_stmt                    {}
    | insert_stmt                    {}
    | set_stmt                       {}
    | replace_stmt                   {}
    | update_stmt                    {}
    | delete_stmt                    {}
    | explain_route_stmt             {}
    | explain_stmt                   {}
    | begin_stmt                     {}
    | show_stmt                      {}
    | hooked_stmt                    {}
    | icmd_stmt                      {}
    | use_db_stmt                    {}
    | help_stmt                      {}
    | set_names_stmt                 {}
    | set_charset_stmt               {}
    | set_password_stmt              {}
    | set_default_stmt               {}
    | set_ob_read_consistency_stmt   {}
    | set_tx_read_only_stmt          {}
    | call_stmt                      {}
    | ddl_stmt                       {}
    | text_ps_stmt                   {}
    | merge_stmt                     {}
    | binlog_stmt                    {}
    | load_data_stmt                 {}
    | other_stmt                     { result->cur_stmt_type_ = OBPROXY_T_OTHERS; }

select_stmt: select_with_opt_hint select_expr_list opt_from
            {
              result->cur_stmt_type_ = OBPROXY_T_SELECT;
            }

opt_replace_ignore: /* empty */
                    /* word 'ignore' will be ignored and treated as empty*/
                   | REPLACE

infile_desc: INFILE NAME_OB opt_replace_ignore TABLE table_factor 
            {
              result->cur_stmt_type_ = OBPROXY_T_LOAD_DATA_INFILE;
            }

local_infile_desc: LOCAL INFILE NAME_OB opt_replace_ignore TABLE table_factor
            {
              result->cur_stmt_type_ = OBPROXY_T_LOAD_DATA_LOCAL_INFILE;
            }

load_infile_desc: local_infile_desc
                | infile_desc

load_data_stmt: load_data_opt_hint load_infile_desc

explain_stmt: explain_or_desc_stmt select_stmt
            | explain_or_desc_stmt insert_stmt
            | explain_or_desc_stmt update_stmt
            | explain_or_desc_stmt delete_stmt
            | explain_or_desc_stmt replace_stmt
            | explain_or_desc_stmt merge_stmt

explain_route_stmt: explain_route stmt

ddl_stmt: mysql_ddl_stmt
        | oracle_ddl_stmt

mysql_ddl_stmt: CREATE create_dll_expr { result->cur_stmt_type_ = OBPROXY_T_CREATE; }
              | DROP   drop_ddl_expr   { result->cur_stmt_type_ = OBPROXY_T_DROP; }
              | ALTER    { result->cur_stmt_type_ = OBPROXY_T_ALTER; }
              | TRUNCATE { result->cur_stmt_type_ = OBPROXY_T_TRUNCATE; }
              | RENAME   { result->cur_stmt_type_ = OBPROXY_T_RENAME; }
              | stop_ddl_task_stmt   {}
              | retry_ddl_task_stmt  {}

create_dll_expr : /* empty */
                | TABLE { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_TABLE; }
                | INDEX { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; }
                | UNIQUE INDEX { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_INDEX; }
                | TABLEGROUP   { result->sub_stmt_type_ = OBPROXY_T_SUB_CREATE_TABLEGROUP; }

drop_ddl_expr : /* empty */
              | TABLEGROUP { result->sub_stmt_type_ = OBPROXY_T_SUB_DROP_TABLEGROUP; }

stop_ddl_task_stmt: STOP_DDL_TASK INT_NUM
          {
            SET_ICMD_ONE_ID($2);
            result->cur_stmt_type_ = OBPROXY_T_STOP_DDL_TASK;
          }

retry_ddl_task_stmt: RETRY_DDL_TASK INT_NUM
          {
            SET_ICMD_ONE_ID($2);
            result->cur_stmt_type_ = OBPROXY_T_RETRY_DDL_TASK;
          }

text_ps_from_stmt: select_stmt {}
                 | insert_stmt {}
                 | replace_stmt {}
                 | delete_stmt {}
                 | update_stmt {}
                 | other_stmt {}
                 | call_stmt {}
                 | merge_stmt {}

text_ps_execute_using_var_list: '@' NAME_OB
                              {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = $2;
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              }
                              | text_ps_execute_using_var_list ',' '@' NAME_OB
                              {
                                ObProxyTextPsParseNode *node = NULL;
                                malloc_parse_node(node);
                                node->str_value_ = $4;
                                add_text_ps_node(result->text_ps_parse_info_, node);
                              }

text_ps_prepare_var_list: '@' NAME_OB
                        {
                          ObProxyTextPsParseNode *node = NULL;
                          malloc_parse_node(node);
                          node->str_value_ = $2;
                          add_text_ps_node(result->text_ps_parse_info_, node);
                        }

text_ps_prepare_args_stmt : text_ps_prepare_var_list
                          | text_ps_from_stmt


text_ps_prepare_stmt: PREPARE var_name FROM
                    {
                      result->text_ps_inner_stmt_type_ = OBPROXY_T_TEXT_PS_PREPARE;
                      result->text_ps_name_ = $2;
                    }

text_ps_execute_stmt: EXECUTE var_name
                    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = $2;
                    }
                    | EXECUTE var_name USING
                    {
                      result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_EXECUTE;
                      result->text_ps_name_ = $2;
                    }

text_ps_stmt: text_ps_prepare_stmt text_ps_prepare_args_stmt
            {
            }
            | text_ps_execute_stmt text_ps_execute_using_var_list
            {
            }
            | DROP PREPARE var_name
            {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = $3;
            }
            | DEALLOCATE PREPARE var_name
            {
              result->cur_stmt_type_ = OBPROXY_T_TEXT_PS_DROP;
              result->text_ps_name_ = $3;
            }

oracle_ddl_stmt: GRANT     { result->cur_stmt_type_ = OBPROXY_T_GRANT; }
               | REVOKE    { result->cur_stmt_type_ = OBPROXY_T_REVOKE; }
               | ANALYZE   { result->cur_stmt_type_ = OBPROXY_T_ANALYZE; }
               | PURGE     { result->cur_stmt_type_ = OBPROXY_T_PURGE; }
               | FLASHBACK { result->cur_stmt_type_ = OBPROXY_T_FLASHBACK; }
               | COMMENT   { result->cur_stmt_type_ = OBPROXY_T_COMMENT; }
               | AUDIT     { result->cur_stmt_type_ = OBPROXY_T_AUDIT; }
               | NOAUDIT   { result->cur_stmt_type_ = OBPROXY_T_NOAUDIT; }

explain_or_desc_stmt: explain_or_desc
                    | explain_or_desc NAME_OB

explain_or_desc: EXPLAIN  {}
               | DESC     {}
               | DESCRIBE {}

explain_route: EXPLAIN_ROUTE

opt_from: /* empty */
        | FROM fromlist

select_expr_list: /* empty */
                | expr_list

select_tx_read_only_stmt: SELECT TX_READ_ONLY { result->cur_stmt_type_ = OBPROXY_T_SELECT_TX_RO; }
                        | SELECT TX_READ_ONLY select_expr_list FROM fromlist
                        | SELECT TX_READ_ONLY expr_list

select_proxy_version_stmt: SELECT_PROXY_VERSION
                         | SELECT_PROXY_VERSION AS var_name { result->col_name_ = $3; }

hooked_stmt: select_tx_read_only_stmt       {}
           | select_proxy_version_stmt      {}
           | select_obproxy_route_addr_stmt {}
           | set_obproxy_route_addr_stmt    {}
           | shard_special_stmt             {}

shard_special_stmt: show_es_id_stmt {}
                  | SHOW_TOPOLOGY db_tb_stmt { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TOPOLOGY; }
                  | show_db_version_stmt {}
                  | SELECT_DATABASE { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_DATABASE; }
                  | SELECT_PROXY_STATUS { result->sub_stmt_type_ = OBPROXY_T_SUB_SELECT_PROXY_STATUS; }
                  | SHOW_DATABASES  { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DATABASES; }
                  | show_tables_stmt {}
                  | show_table_status_stmt {}
                  | SHOW_COLUMNS db_tb_stmt { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_COLUMNS; }
                  | SHOW_INDEX db_tb_stmt { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_INDEX; }
                  | show_create_table_stmt {}
                  | explain_or_desc var_name
                  {
                      result->table_info_.table_name_ = $2;
                      result->cur_stmt_type_ = OBPROXY_T_DESC;
                      result->sub_stmt_type_ = OBPROXY_T_SUB_DESC_TABLE;
                  }

db_tb_stmt: FROM var_name
          {
            result->table_info_.table_name_ = $2;
          }
          | FROM var_name FROM var_name
          {
            result->table_info_.table_name_ = $2;
            result->table_info_.database_name_ = $4;
          }
          | FROM var_name '.' var_name
          {
            result->table_info_.database_name_ = $2;
            result->table_info_.table_name_ = $4;
          }

show_create_table_stmt: SHOW_CREATE_TABLE NAME_OB  
                      {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE;
                        result->table_info_.table_name_ = $2;
                      }
                      | SHOW_CREATE_TABLE NAME_OB '.' NAME_OB 
                      {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_CREATE_TABLE;
                        result->table_info_.database_name_ = $2;
                        result->table_info_.table_name_ = $4;
                      }

opt_show_like: /*empty*/            {}
             | LIKE NAME_OB         { result->table_info_.table_name_ = $2; }

opt_show_from: /*empty*/            {}
             | FROM NAME_OB         { result->table_info_.database_name_ = $2; }

show_tables_stmt: SHOW_TABLES opt_show_from opt_show_like
                {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLES;
                }
                | SHOW_FULL_TABLES opt_show_from opt_show_like
                {
                  result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_FULL_TABLES;
                }

show_table_status_stmt: SHOW TABLE STATUS opt_show_from opt_show_like
                      {
                        result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_TABLE_STATUS;
                      }

show_db_version_stmt: SHOW_DB_VERSION { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_DB_VERSION; }

show_es_id_stmt: SHOW_ELASTIC_ID { result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID; }
                 | SHOW_ELASTIC_ID FROM NAME_OB
                 {
                     SET_ICMD_ONE_STRING($3);
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 }
                 | SHOW_ELASTIC_ID WHERE GROUP_NAME '=' NAME_OB
                 {
                     SET_ICMD_SECOND_STRING($5);
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 }
                 | SHOW_ELASTIC_ID FROM NAME_OB WHERE GROUP_NAME '=' NAME_OB
                 {
                     SET_ICMD_ONE_STRING($3);
                     SET_ICMD_SECOND_STRING($7);
                     result->sub_stmt_type_ = OBPROXY_T_SUB_SHOW_ELASTIC_ID;
                 }

select_obproxy_route_addr_stmt: SELECT_OBPROXY_ROUTE_ADDR
                              { result->cur_stmt_type_ = OBPROXY_T_SELECT_ROUTE_ADDR; }

set_obproxy_route_addr_stmt: SET_OBPROXY_ROUTE_ADDR '=' INT_NUM
                           {
                              result->cur_stmt_type_ = OBPROXY_T_SET_ROUTE_ADDR;
                              result->cmd_info_.integer_[0] = $3;
                           }

set_names_stmt: SET_NAMES       {}
set_charset_stmt: SET_CHARSET   {}
set_password_stmt: SET_PASSWORD {}
set_default_stmt: SET_DEFAULT   {}
set_ob_read_consistency_stmt: SET_OB_READ_CONSISTENCY {}
set_tx_read_only_stmt: SET_TX_READ_ONLY {}


call_stmt: CALL routine_name_stmt '(' call_expr_list ')'

routine_name_stmt: var_name
                 {
                   result->table_info_.table_name_ = $1;
                 }
                 | var_name '.' var_name
                 {
                   result->table_info_.package_name_ = $1;
                   result->table_info_.table_name_ = $3;
                 }
                 | var_name '.' var_name '.' var_name
                 {
                   result->table_info_.database_name_ = $1;
                   result->table_info_.package_name_ = $3;
                   result->table_info_.table_name_ = $5;
                 }

call_expr_list:   /* empty */
              {
                result->call_parse_info_.node_count_ = 0;
              }
              | call_expr
              {
                result->call_parse_info_.node_count_ = 0;
                add_call_node(result->call_parse_info_, $1);
              }
              | call_expr_list ',' call_expr
              {
                add_call_node(result->call_parse_info_, $3);
              }

call_expr: NAME_OB
         {
            malloc_call_node($$, CALL_TOKEN_STR_VAL);
            $$->str_value_ = $1;
         }
         | INT_NUM
         {
           malloc_call_node($$, CALL_TOKEN_INT_VAL);
           $$->int_value_ = $1;
         }
         | NUMBER_VAL
         {
           malloc_call_node($$, CALL_TOKEN_NUMBER_VAL);
           $$->str_value_ = $1;
         }
         | '@' NAME_OB
         {
           malloc_call_node($$, CALL_TOKEN_USER_VAR);
           $$->str_value_ = $2;
         }
         | '@' '@' NAME_OB
         {
           malloc_call_node($$, CALL_TOKEN_SYS_VAR);
           $$->str_value_ = $3;
         }
         | PLACE_HOLDER
         {
           result->placeholder_list_idx_++;
           malloc_call_node($$, CALL_TOKEN_PLACE_HOLDER);
           $$->placeholder_idx_ = result->placeholder_list_idx_ - 1;
         }


expr_list: expr
         | expr_list expr

expr: clause

clause: '(' ')'
      | '(' select_stmt ')'
      | '(' expr_list ')'

fromlist: table_references
        | sub_query

sub_query: select_stmt

opt_column_list: /* empty */
               | '(' column_list ')'

column_list: var_name
           | column_list ',' var_name

insert_stmt: insert_with_opt_hint table_factor partition_factor {
                                                                  handle_stmt_end(result);
                                                                  HANDLE_ACCEPT();
                                                                }
           | insert_with_opt_hint table_factor partition_factor opt_column_list sub_query
replace_stmt: replace_with_opt_hint fromlist
update_stmt: update_with_opt_hint fromlist
delete_stmt: delete_with_opt_hint opt_quick FROM fromlist
merge_stmt: merge_with_opt_hint table_factor {
                                                 handle_stmt_end(result);
                                                 HANDLE_ACCEPT();
                                               }

set_stmt: SET set_expr_list

set_expr_list: set_expr ',' set_expr_list
             | set_expr

set_expr: '@' var_name '=' set_var_value
        {
          add_set_var_node(result->set_parse_info_, $4, $2, SET_VAR_USER);
        }
        | '@' '@' GLOBAL var_name '=' set_var_value
        {
          add_set_var_node(result->set_parse_info_, $6, $4, SET_VAR_SYS);
        }
        | GLOBAL var_name '=' set_var_value
        {
          add_set_var_node(result->set_parse_info_, $4, $2, SET_VAR_SYS);
        }
        | '@' '@' var_name '=' set_var_value
        {
          add_set_var_node(result->set_parse_info_, $5, $3, SET_VAR_SYS);
        }
        | '@' '@' SESSION var_name '=' set_var_value
        {
          add_set_var_node(result->set_parse_info_, $6, $4, SET_VAR_SYS);
        }
        | SESSION var_name '=' set_var_value
        {
          add_set_var_node(result->set_parse_info_, $4, $2, SET_VAR_SYS);
        }
        | var_name '=' set_var_value
        {
          add_set_var_node(result->set_parse_info_, $3, $1, SET_VAR_SYS);
        }
set_var_value: var_name
             {
               malloc_set_var_node($$, SET_VALUE_TYPE_STR);
               $$->str_value_ = $1;
             }
             | INT_NUM
             {
               malloc_set_var_node($$, SET_VALUE_TYPE_INT);
               $$->int_value_ = $1;
             }
             | NUMBER_VAL
             {
               malloc_set_var_node($$, SET_VALUE_TYPE_NUMBER);
               $$->str_value_ = $1;
             }

comment_expr_list: comment_expr
                 | comment_expr comment_expr_list

comment_expr: COMMENT_BEGIN comment_list COMMENT_END {}
            | COMMENT_BEGIN ODP_COMMENT odp_comment odp_comment_list COMMENT_END {}
            | COMMENT_BEGIN TABLE_ID '=' right_string_val odp_comment_list COMMENT_END   { result->dbmesh_route_info_.tb_idx_str_ = $4; }
            | COMMENT_BEGIN TABLE_NAME '=' right_string_val odp_comment_list COMMENT_END   { result->dbmesh_route_info_.table_name_str_ = $4; }
            | COMMENT_BEGIN GROUP_ID '=' right_string_val odp_comment_list COMMENT_END   { result->dbmesh_route_info_.group_idx_str_ = $4; }
            | COMMENT_BEGIN ELASTIC_ID '=' right_string_val  odp_comment_list COMMENT_END { result->dbmesh_route_info_.es_idx_str_ = $4; }
            | COMMENT_BEGIN TESTLOAD '=' right_string_val  odp_comment_list COMMENT_END   { result->dbmesh_route_info_.testload_str_ = $4; }
            | COMMENT_BEGIN NAME_OB_DOT NAME_OB '=' name_right_string_val odp_comment_list COMMENT_END
            {
              malloc_shard_column_node($$, $2, $3, DBMESH_TOKEN_STR_VAL);
              $$->col_str_value_ = $5;
              add_shard_column_node(result->dbmesh_route_info_, $$);
            }
            | COMMENT_BEGIN TRACE_ID '=' tracer_right_string_val odp_comment_list COMMENT_END   { result->trace_id_ = $4; }
            | COMMENT_BEGIN RPC_ID '=' tracer_right_string_val odp_comment_list COMMENT_END { result->rpc_id_ = $4; }
            | COMMENT_BEGIN TNT_ID '=' right_string_val odp_comment_list COMMENT_END { result->dbmesh_route_info_.tnt_id_str_ = $4; }
            | COMMENT_BEGIN DISASTER_STATUS '=' right_string_val odp_comment_list COMMENT_END   { result->dbmesh_route_info_.disaster_status_str_ = $4; }
            | COMMENT_BEGIN DBP_COMMENT ROUTE_TAG '=' '{' dbp_comment_list '}' COMMENT_END  {}
            | COMMENT_BEGIN DBP_COMMENT SYS_TAG '=' '{' dbp_sys_comment '}' COMMENT_END  {}
            | COMMENT_BEGIN TARGET_DB_SERVER '=' right_string_val odp_comment_list COMMENT_END { result->target_db_server_ = $4; }
comment_list: /* empty */ {}
            | comment_list comment

comment: ROUTE_TABLE NAME_OB { result->has_simple_route_info_ = true; result->simple_route_info_.table_name_ = $2; }
       | ROUTE_PART_KEY NAME_OB { result->simple_route_info_.part_key_ = $2; }
       | NAME_OB

dbp_comment_list: dbp_comment ',' dbp_comment_list
                | dbp_comment

dbp_comment: GROUP_ID '(' right_string_val ')'
            {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.group_idx_str_ = $3;
            }
            | TABLE_NAME '(' right_string_val ')'
            {
              result->dbp_route_info_.has_group_info_ = true;
              result->dbp_route_info_.table_name_ = $3;
            }
            | SCAN_ALL '(' ')'           { result->dbp_route_info_.scan_all_ = true; }
            | SCAN_ALL '(' PARALL '=' right_string_val ')' { result->dbp_route_info_.scan_all_ = true; }
            | STICKY_SESSION '(' ')'           { result->dbp_route_info_.sticky_session_ = true; }
            | SHARD_KEY '(' dbp_kv_comment_list ')' {result->dbp_route_info_.has_shard_key_ = true;}

dbp_sys_comment: TRACE '(' tracer_right_string_val')' { result->trace_id_ = $3; }
               | TRACE '(' tracer_right_string_val '#' tracer_right_string_val ')' { result->trace_id_ = $3; result->rpc_id_ = $5; }

dbp_kv_comment_list: dbp_kv_comment ',' dbp_kv_comment_list {}
                   | dbp_kv_comment

dbp_kv_comment : NAME_OB '=' right_string_val {
                   if (result->dbp_route_info_.shard_key_count_ < OBPROXY_MAX_DBP_SHARD_KEY_NUM) {
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].left_str_ = $1;
                     result->dbp_route_info_.shard_key_infos_[result->dbp_route_info_.shard_key_count_].right_str_ = $3;
                     ++result->dbp_route_info_.shard_key_count_;
                   }
                 }

odp_comment_list : /* empty */
                 | odp_comment_list ',' odp_comment

odp_comment: GROUP_ID '=' right_string_val   { result->dbmesh_route_info_.group_idx_str_ = $3; }
           | TABLE_ID '=' right_string_val   { result->dbmesh_route_info_.tb_idx_str_ = $3; }
           | TABLE_NAME '=' right_string_val   { result->dbmesh_route_info_.table_name_str_ = $3; }
           | ELASTIC_ID '=' right_string_val { result->dbmesh_route_info_.es_idx_str_ = $3; }
           | TESTLOAD '=' right_string_val   { result->dbmesh_route_info_.testload_str_ = $3; }
           | TRACE_ID '=' tracer_right_string_val   { result->trace_id_ = $3; }
           | RPC_ID '=' tracer_right_string_val     { result->rpc_id_ = $3; }
           | TNT_ID '=' right_string_val { result->dbmesh_route_info_.tnt_id_str_ = $3; }
           | DISASTER_STATUS '=' right_string_val { result->dbmesh_route_info_.disaster_status_str_ = $3; }
           | TRACE_LOG  { result->has_trace_log_hint_ = true; }
           | TARGET_DB_SERVER '=' right_string_val { result->target_db_server_ = $3; }
           | NAME_OB '.' NAME_OB '=' name_right_string_val
           {
             malloc_shard_column_node($$, $1, $3, DBMESH_TOKEN_STR_VAL);
             $$->col_str_value_ = $5;
             add_shard_column_node(result->dbmesh_route_info_, $$);
           }
           | NAME_OB '=' name_right_string_val {}

tracer_right_string_val: /* empty */ { $$.str_ = NULL; $$.str_len_ = 0; }
                       | right_string_val

name_right_string_val: /* empty */ { $$.str_ = NULL; $$.str_len_ = 0; }
                     | right_string_val

right_string_val: NAME_OB
                | NAME_STR

select_with_opt_hint: SELECT
                    | SELECT hint_list_begin hint_list_with_end { SET_BASIC_STMT(OBPROXY_T_SELECT); }
update_with_opt_hint: UPDATE
                    | UPDATE hint_list_begin hint_list_with_end { SET_BASIC_STMT(OBPROXY_T_UPDATE); }
delete_with_opt_hint: DELETE
                    | DELETE hint_list_begin hint_list_with_end { SET_BASIC_STMT(OBPROXY_T_DELETE); }

insert_with_opt_hint: INSERT insert_all_when
                    | INSERT hint_list_begin hint_list_with_end insert_all_when { SET_BASIC_STMT(OBPROXY_T_INSERT); }

insert_all_when:
               | ALL
               | ALL WHEN
/* load data will use replace as keyword so we need to set cur_stmt_type_ here */
replace_with_opt_hint: REPLACE
                     | REPLACE hint_list_begin hint_list_with_end { SET_BASIC_STMT(OBPROXY_T_REPLACE); }
merge_with_opt_hint: MERGE
                   | MERGE hint_list_begin hint_list_with_end { SET_BASIC_STMT(OBPROXY_T_MERGE); }
load_data_opt_hint: LOAD DATA
                  | LOAD DATA hint_list_begin hint_list_with_end

hint_list_begin : COMMENT_BEGIN HINT_BEGIN
hint_list_with_end: hint_list COMMENT_END
hint_list: /* empty */
         | hint hint_list

hint: QUERY_TIMEOUT '(' INT_NUM ')' { result->query_timeout_ = $3; }
    | INT_NUM {}
    | READ_CONSISTENCY '(' opt_read_consistency ')'
    | INDEX '(' NAME_OB NAME_OB ')'
    {
      add_hint_index(result->dbmesh_route_info_, $3);
      result->dbmesh_route_info_.index_count_++;
    }
    | TRACE_LOG { result->has_trace_log_hint_ = true; }
    | NAME_OB {}
    | NAME_OB '(' INT_NUM ')' {}
    | NAME_OB '(' NAME_OB ')' {}
    | NAME_OB '(' NAME_OB NAME_OB ')' {}
    | NAME_OB '(' NAME_OB INT_NUM ')' {}

opt_read_consistency: /* empty */ {}
                    | WEAK { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_WEAK); }
                    | STRONG { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_STRONG); }
                    | FROZEN { SET_READ_CONSISTENCY(OBPROXY_READ_CONSISTENCY_FROZEN); }

opt_quick: /* empty */
         | QUICK

 /* show stmt */
show_stmt: SHOW opt_count WARNINGS { result->cur_stmt_type_ = OBPROXY_T_SHOW_WARNINGS; }
         | SHOW opt_count ERRORS   { result->cur_stmt_type_ = OBPROXY_T_SHOW_ERRORS; }
         | SHOW TRACE              { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; }
         | SHOW TRACE NAME_OB '=' NAME_OB { result->cur_stmt_type_ = OBPROXY_T_SHOW_TRACE; }

 /* internal cmd stmt */
icmd_stmt: show_proxynet
         | show_proxyconfig
         | show_processlist
         | show_proxysession
         | show_globalsession
         | show_proxysm
         | show_proxycluster
         | show_proxyresource
         | show_proxycongestion
         | show_proxyroute
         | show_proxyvip
         | show_proxymemory
         | show_sqlaudit
         | show_warnlog
         | show_proxystat
         | show_proxytrace
         | show_proxyinfo
         | alter_proxyconfig
         | alter_proxyresource
         | ping_proxy
         | kill_proxysession
         | kill_globalsession
         | kill_mysql

binlog_stmt: SHOW_BINLOG_SERVER_FOR_TENANT {}

 /* limit param stmt*/
opt_limit:
 /*empty*/
{
}
| LIMIT INT_NUM  /*LIMIT rows*/
{
   result->cmd_info_.integer_[2] = $2;/*row*/
}
| LIMIT INT_NUM ',' INT_NUM /*LIMIT offset, rows*/
{
   result->cmd_info_.integer_[1] = $2;/*offset*/
   result->cmd_info_.integer_[2] = $4;/*row*/
}
| LIMIT INT_NUM OFFSET INT_NUM /*LIMIT rows OFFSET offset*/
{
   result->cmd_info_.integer_[1] = $4;/*offset*/
   result->cmd_info_.integer_[2] = $2;/*row*/
}

 /* like param stmt*/
opt_like:
  /*empty*/             {}
| LIKE NAME_OB          { result->cmd_info_.string_[0] = $2;}

 /* large like param stmt*/
opt_large_like:
 /*empty*/              {}
| LIKE NAME_OB          { result->cmd_info_.string_[1] = $2;}

 /*show proxynet grammer*/
show_proxynet: SHOW_PROXYNET opt_show_net
opt_show_net:
  THREAD                        { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_THREAD); }
| CONNECTION                    { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_NET_CONNECTION); }
| CONNECTION INT_NUM opt_limit  { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_NET_CONNECTION, $2); }

 /*show proxyconfig grammer*/
show_proxyconfig:
  SHOW_PROXYCONFIG opt_like           {}
| SHOW_PROXYCONFIG DIFF opt_like      { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF); }
| SHOW_PROXYCONFIG DIFF USER opt_like { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONFIG_DIFF_USER); }

  /*show processlist grammer*/
show_processlist:
  SHOW_PROCESSLIST            { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST); }

show_globalsession: SHOW_GLOBALSESSION opt_show_global_session
opt_show_global_session:
  /*empty*/ {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST);}
  |ATTRIBUTE NAME_OB {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO, $2);}
  |ATTRIBUTE LIKE NAME_OB {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE, $3);}
  |ATTRIBUTE ALL {SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL);}
  |LIKE NAME_OB  {result->cmd_info_.string_[0] = $2;}

 /*show proxysession grammer*/
show_proxysession: SHOW_PROXYSESSION opt_show_session
opt_show_session:
  /*empty*/                       { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_LIST_INTERNAL); }
| ATTRIBUTE opt_like              { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_ATTRIBUTE); }
| ATTRIBUTE INT_NUM opt_like      { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_ATTRIBUTE, $2); }
| STAT opt_like                   { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_STAT); }
| STAT INT_NUM opt_like           { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_STAT, $2); }
| VARIABLES opt_like              { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL); }
| VARIABLES INT_NUM opt_like      { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL, $2); }
| VARIABLES ALL opt_like          { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_VARIABLES_ALL); }
| VARIABLES ALL INT_NUM opt_like  { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SESSION_VARIABLES_ALL, $3); }
| READ_STALE opt_like             { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SESSION_READ_STALE); }

 /*show proxysm grammer*/
show_proxysm:
  SHOW_PROXYSM               {}
| SHOW_PROXYSM INT_NUM       { SET_ICMD_ONE_ID($2); }

 /*show proxycluster grammer*/
show_proxycluster:
  SHOW_PROXYCLUSTER opt_like          {}
| SHOW_PROXYCLUSTER IDC opt_like      { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); }


 /*show proxyresource grammer*/
show_proxyresource:
  SHOW_PROXYRESOURCE opt_like   {}

 /*show proxycongestion grammer*/
show_proxycongestion: SHOW_PROXYCONGESTION opt_show_congestion
opt_show_congestion:
  /* empty */         {}
| NAME_OB             { SET_ICMD_ONE_STRING($1); }
| ALL                 { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_CONGEST_ALL);}
| ALL NAME_OB         { SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_CONGEST_ALL, $2);}

 /*show proxyroute grammer*/
show_proxyroute:
  SHOW_PROXYROUTE opt_large_like  {}
| SHOW_PROXYROUTE ROUTINE   opt_large_like  { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_ROUTINE); }
| SHOW_PROXYROUTE PARTITION                 { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_ROUTE_PARTITION); }

 /*show proxyvip grammer*/
show_proxyvip:
  SHOW_PROXYVIP             {}
| SHOW_PROXYVIP NAME_OB     { SET_ICMD_ONE_STRING($2); }

 /*show proxymemory grammer*/
show_proxymemory:
  SHOW_PROXYMEMORY          {}
| SHOW_PROXYMEMORY INT_NUM  { SET_ICMD_ONE_ID($2); }
| SHOW_PROXYMEMORY OBJPOOL         { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); }
| SHOW_PROXYMEMORY OBJPOOL INT_NUM { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_MEMORY_OBJPOOL); SET_ICMD_ONE_ID($3); }

 /*show sqlaudit grammer*/
show_sqlaudit:
  SHOW_SQLAUDIT opt_limit   { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID); }
| SHOW_SQLAUDIT INT_NUM     { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_SQLAUDIT_SM_ID, $2); }

 /*show warnlog grammer*/
show_warnlog: SHOW_WARNLOG opt_show_warnlog
opt_show_warnlog:
  /*empty*/                       {}
| INT_NUM                         { SET_ICMD_SECOND_ID($1); }
| INT_NUM ',' INT_NUM             { SET_ICMD_TWO_ID($3, $1); }
| INT_NUM ',' INT_NUM ',' NAME_OB { SET_ICMD_TWO_ID($3, $1); SET_ICMD_ONE_STRING($5); }

 /*show proxystat grammer*/
show_proxystat:
  SHOW_PROXYSTAT opt_like         {}
| SHOW_PROXYSTAT REFRESH opt_like { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_STAT_REFRESH); }

 /*show proxytrace grammer*/
show_proxytrace: SHOW_PROXYTRACE opt_show_trace
opt_show_trace:
  /* empty */                 {}
| INT_NUM                     { SET_ICMD_ONE_ID($1);  }
| INT_NUM INT_NUM             { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_TRACE_LIMIT, $1,$2); }

 /*show proxyinfo grammer*/
show_proxyinfo:
  SHOW_PROXYINFO BINARY       { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_BINARY); }
| SHOW_PROXYINFO UPGRADE      { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_UPGRADE); }
| SHOW_PROXYINFO IDC          { SET_ICMD_SUB_TYPE(OBPROXY_T_SUB_INFO_IDC); }

 /*alter proxyconfig grammer*/
alter_proxyconfig:
  ALTER_PROXYCONFIG SET NAME_OB '='           { SET_ICMD_ONE_STRING($3); }
| ALTER_PROXYCONFIG SET NAME_OB '=' NAME_OB   { SET_ICMD_TWO_STRING($3, $5); }
| ALTER_PROXYCONFIG SET NAME_OB '=' INT_NUM   { SET_ICMD_CONFIG_INT_VALUE($3, $5); }

 /*alter proxyresource grammer*/
alter_proxyresource:
  ALTER_PROXYRESOURCE DELETE NAME_OB          { SET_ICMD_ONE_STRING($3); }

 /*ping proxy grammer*/
ping_proxy:
  PING_PROXY              {}

 /*kill proxysession grammer*/
kill_proxysession:
  KILL_PROXYSESSION INT_NUM          { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CS, $2); }
| KILL_PROXYSESSION INT_NUM INT_NUM  { SET_ICMD_SUB_AND_TWO_ID(OBPROXY_T_SUB_KILL_SS, $2, $3); }

/*kill globalsession grammer*/
kill_globalsession:
  KILL_GLOBALSESSION NAME_OB INT_NUM {SET_ICMD_TYPE_STRING_INT_VALUE(OBPROXY_T_SUB_KILL_GLOBAL_SS_ID, $2,$3);}
| KILL_GLOBALSESSION NAME_OB {SET_ICMD_SUB_AND_ONE_STRING(OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY, $2);}

 /*mysql kill grammer*/
kill_mysql:
  KILL INT_NUM             { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, $2); }
| KILL CONNECTION INT_NUM  { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_CONNECTION, $3); }
| KILL QUERY INT_NUM       { SET_ICMD_SUB_AND_ONE_ID(OBPROXY_T_SUB_KILL_QUERY, $3); }


opt_count: /* empty */
         | COUNT '*'

 /* begin stmt */
begin_stmt: BEGI                                              {
                                                                result->has_anonymous_block_ = false ;
                                                                result->cur_stmt_type_ = OBPROXY_T_BEGIN;
                                                              }
          | START TRANSACTION opt_transaction_characteristics { result->cur_stmt_type_ = OBPROXY_T_BEGIN; }
          | XA BEGI NAME_OB  { result->cur_stmt_type_ = OBPROXY_T_BEGIN; }
          | XA START NAME_OB { result->cur_stmt_type_ = OBPROXY_T_BEGIN; }

opt_transaction_characteristics: /* empty */
                               | transaction_characteristics

transaction_characteristics: transaction_characteristic
                           | transaction_characteristics ',' transaction_characteristic

 /* do not parse READ WRITE */
transaction_characteristic: READ ONLY
                          | WITH CONSISTENT SNAPSHOT

 /*use db stmt*/
use_db_stmt: USE var_name  {
                            result->cur_stmt_type_ = OBPROXY_T_USE_DB;
                            result->table_info_.database_name_ = $2;
                          }

/*help stmt*/
help_stmt: HELP NAME_OB  { result->cur_stmt_type_ = OBPROXY_T_HELP; }

 /* other stmt */
other_stmt: NAME_OB

partition_factor: /*empty*/ {}
                | SUBPARTITION var_name { result->part_name_ = $2; }
                | SUBPARTITION '(' var_name ')' { result->part_name_ = $3; }
                | PARTITION var_name { result->part_name_ = $2; }
                | PARTITION '(' var_name ')' { result->part_name_ = $3; }

table_references: table_factor partition_factor {
                                                  handle_stmt_end(result);
                                                  HANDLE_ACCEPT();
                                                }

table_factor: var_name  {
                          result->table_info_.table_name_ = $1;
                        }
            | var_name '.' var_name {
                                      result->table_info_.database_name_ = $1;
                                      result->table_info_.table_name_ = $3;
                                    }
            | var_name var_name   {
                                    UPDATE_ALIAS_NAME($2);
                                    result->table_info_.table_name_ = $1;
                                  }
            | var_name '.' var_name var_name  {
                                                UPDATE_ALIAS_NAME($4);
                                                result->table_info_.database_name_ = $1;
                                                result->table_info_.table_name_ = $3;
                                              }
            | var_name AS var_name  {
                                      UPDATE_ALIAS_NAME($3);
                                      result->table_info_.table_name_ = $1;
                                    }
            | var_name '.' var_name AS var_name {
                                                  UPDATE_ALIAS_NAME($5);
                                                  result->table_info_.database_name_ = $1;
                                                  result->table_info_.table_name_ = $3;
                                                }

non_reserved_keyword: START
                    | XA
                    | BEGI
                    | TRANSACTION
                    | CONSISTENT
                    | ERRORS
                    | WARNINGS
                    | COUNT
                    | QUICK
                    | TRACE
                    | THREAD
                    | CONNECTION
                    | OFFSET
                    | ATTRIBUTE
                    | VARIABLES
                    | STAT
                    | DIFF
                    | USER
                    | OBJPOOL
                    | REFRESH
                    | UPGRADE
                    | IDC
                    | QUERY
                    | GROUP_NAME
                    | FLASHBACK
                    | AUDIT
                    | NOAUDIT
                    | LOCAL
                    | DATA
                    | STATUS

var_name: NAME_OB
        | non_reserved_keyword
%%

void yyerror(YYLTYPE* yylloc, ObProxyParseResult* p, char* s, ...)
{
  // do nothing
}

void ob_proxy_parser_fatal_error(yyconst char *msg, yyscan_t yyscanner)
{
  fprintf(stderr, "FATAL ERROR:%s\n", msg);
  ObProxyParseResult *p = ob_proxy_parser_yyget_extra(yyscanner);
  if (OB_ISNULL(p)) {
    fprintf(stderr, "unexpected null parse result\n");
  } else {
    longjmp(p->jmp_buf_, 1);//the secord param must be non-zero value
  }
}

int obproxy_parse_sql(ObProxyParseResult* p, const char* buf, size_t len)
{
  int ret = OB_SUCCESS;
  //obproxydebug = 1;
  if (OB_ISNULL(p) || OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    // print err msg later
  } else if (OB_FAIL(ob_proxy_parser_yylex_init_extra(p, &(p->yyscan_info_)))) {
    // print err msg later
  } else {
    int val = setjmp(p->jmp_buf_);
    if (val) {
      ret = OB_PARSER_ERR_PARSE_SQL;
    } else {
      ob_proxy_parser_yy_scan_buffer((char *)buf, len, p->yyscan_info_);
      if (OB_FAIL(ob_proxy_parser_yyparse(p))) {
        // print err msg later
      } else {
        // do nothing
      }
    }
  }

  return ret;
}
