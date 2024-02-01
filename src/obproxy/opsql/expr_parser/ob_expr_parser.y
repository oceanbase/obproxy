%define api.pure
%parse-param {ObExprParseResult* result}
%name-prefix "ob_expr_parser_yy"
%locations
%no-lines
%verbose
%{
#include <stdint.h>
#include "opsql/ob_proxy_parse_define.h"
#include "opsql/expr_parser/ob_expr_parse_result.h"
%}

%union
{
  int64_t              num;
  ObProxyParseString   str;
  ObProxyFunctionType  func;
  ObProxyOperatorType  operator;
  ObProxyTokenNode     *node;
  ObProxyTokenList     *list;
  ObProxyRelationExpr  *relation;
};

%{
#include "ob_expr_parser_lex.h"
#define YYLEX_PARAM result->yyscan_info_
extern void yyerror(YYLTYPE* yylloc, ObExprParseResult* p, char* s,...);
extern void *obproxy_parse_malloc(const size_t nbyte, void *malloc_pool);
extern void obproxy_parse_free(void *ptr);

static inline bool is_equal(ObProxyParseString *l, ObProxyParseString *r)
{
  return NULL != l && NULL != r && l->str_len_ == r->str_len_
         && 0 == strncasecmp(l->str_, r->str_, l->str_len_);
}

static inline bool is_parameter_token_type(const ObProxyTokenType type)
{
  bool bret = false;
  switch (type)
  {
  case TOKEN_STR_VAL:
  case TOKEN_INT_VAL:
  case TOKEN_FUNC:
  case TOKEN_NULL:
    bret = true; 
    break;
  default:
    bret = false;
    break;
  }
  return bret;
}

static inline void add_token(ObProxyTokenList *list, ObExprParseResult *result, ObProxyTokenNode *node)
{
  UNUSED(result); // use for perf later
  if (OB_ISNULL(list) || OB_ISNULL(node)) {
    // do nothing
  } else {
    if (TOKEN_COLUMN == node->type_) {
      list->column_node_ = node;
    }
    if (NULL != list->tail_) {
      list->tail_->next_ = node;
      list->tail_ = node;
    }
  }
}

// column in (xxx,xxx,xxx) / column = func(xxx,xxx,xxx)
// we only care about the first parameter of the first case
// and function parameters only support str/int/func/null
static inline void add_token_list(ObProxyTokenList *list, ObProxyTokenList *next_list)
{
  if (OB_ISNULL(list) || OB_ISNULL(next_list)) {
  } else if (NULL != list->tail_
             && NULL  != next_list->head_
             && is_parameter_token_type(list->tail_->type_)
             && is_parameter_token_type(next_list->head_->type_)) {
    list->tail_->next_ = next_list->head_;
    list->tail_ = next_list->head_;
    list->tail_->next_ = NULL;
  }
}

static inline ObProxyFunctionType get_reverse_func(ObProxyFunctionType type)
{
  ObProxyFunctionType ret_type = type;
  switch (type) {
    case F_COMP_GE:
      ret_type = F_COMP_LE;
      break;
    case F_COMP_GT:
      ret_type = F_COMP_LT;
      break;
    case F_COMP_LE:
      ret_type = F_COMP_GE;
      break;
    case F_COMP_LT:
      ret_type = F_COMP_GT;
      break;
    default:
      // do nothing
      break;
  }
  return ret_type;
}

static inline int64_t get_mask(ObProxyFunctionType type, ObProxyPartKeyLevel level)
{
  int64_t mask = 0;
  int64_t flag = NO_BOUND_FLAG;
  switch (type) {
    case F_COMP_EQ:
    case F_COMP_NSEQ:
      flag = BOTH_BOUND_FLAG;
      break;
    case F_COMP_GE:
    case F_COMP_GT:
      flag = LOW_BOUND_FLAG;
      break;
    case F_COMP_LE:
    case F_COMP_LT:
      flag = HIGH_BOUND_FLAG;
      break;
    default:
      break;
  }
  switch (level) {
    case PART_KEY_LEVEL_ONE:
      mask = GET_FIRST_PART_MASK(flag);
      break;
    case PART_KEY_LEVEL_TWO:
      mask = GET_SUB_PART_MASK(flag);
      break;
    case PART_KEY_LEVEL_BOTH:
      mask = GET_FIRST_PART_MASK(flag) | GET_SUB_PART_MASK(flag);
      break;
    default:
      break;
  }
  return mask;
}

static inline void set_part_key_column_idx(ObExprParseResult *result, ObProxyParseString *column_name)
{
  int64_t i = 0;
  for (i = 0; i < result->part_key_info_.key_num_; ++i) {
    if (is_equal(column_name, &result->part_key_info_.part_keys_[i].name_)) {
      result->part_key_info_.part_keys_[i].idx_ = result->column_idx_;
      result->part_key_info_.part_keys_[i].is_exist_in_sql_ = true;
    }
  }
}

static inline void init_part_key_all_match(ObExprParseResult *result)
{
  for (int64_t i = 0; i < result->part_key_info_.key_num_; ++i) {
      result->part_key_info_.part_keys_[i].is_exist_in_sql_ = true;
  }
}

#define store_const_str(str_value, str, str_len)                 \
  do {                                                           \
    str_value.str_ = str;                                        \
    if (str == NULL) {                                           \
      str_value.str_len_ = 0;                                    \
      str_value.end_ptr_ = NULL;                                 \
    } else {                                                     \
      str_value.str_len_ = str_len;                              \
      str_value.end_ptr_ = str_value.str_ + str_value.str_len_;  \
    }                                                            \
  } while (0)

#define malloc_func_node(node, result, operator)                           \
  do {                                                                     \
    if (OB_ISNULL(node = ((ObProxyTokenNode *)obproxy_parse_malloc(        \
                      sizeof(ObProxyTokenNode), result->malloc_pool_)))) { \
      YYABORT;                                                             \
    } else {                                                               \
      node->type_ = TOKEN_FUNC;                                            \
      node->child_ = NULL;                                                 \
      node->next_ = NULL;                                                  \
      node->str_value_.str_len_ = 1;                                       \
      switch (operator) {                                                  \
        case OPT_ADD:                                                      \
          store_const_str(node->str_value_, "+", 1);                       \
          break;                                                           \
        case OPT_MINUS:                                                    \
          store_const_str(node->str_value_, "-", 1);                       \
          break;                                                           \
        case OPT_MUL:                                                      \
          store_const_str(node->str_value_, "*", 1);                       \
          break;                                                           \
        case OPT_DIV:                                                      \
          store_const_str(node->str_value_, "/", 1);                       \
          break;                                                           \
        case OPT_MOD:                                                      \
          store_const_str(node->str_value_, "%", 1);                       \
          break;                                                           \
        case OPT_AND:                                                      \
          store_const_str(node->str_value_, "&", 1);                       \
          break;                                                           \
        case OPT_NOT:                                                      \
          store_const_str(node->str_value_, "!", 1);                       \
          break;                                                           \
        default:                                                           \
          break;                                                           \
      }                                                                    \
    }                                                                      \
  } while (0)

#define malloc_node(node, result, type)                                                       \
  do {                                                                                        \
    if (OB_ISNULL(node = ((ObProxyTokenNode *)obproxy_parse_malloc(sizeof(ObProxyTokenNode),  \
                                                                   result->malloc_pool_)))) { \
      YYABORT;                                                                                \
    } else {                                                                                  \
      node->type_ = type;                                                                     \
      node->child_ = NULL;                                                                    \
      node->next_ = NULL;                                                                     \
    }                                                                                         \
  } while(0)                                                                                  \

#define malloc_list(list, result, node)                                                       \
  do {                                                                                        \
    if (OB_ISNULL(list = ((ObProxyTokenList *)obproxy_parse_malloc(sizeof(ObProxyTokenList),  \
                                                                   result->malloc_pool_)))) { \
      YYABORT;                                                                                \
    } else if (OB_ISNULL(node))  {                                                            \
      list->column_node_ = NULL;                                                              \
    } else {                                                                                  \
      if (TOKEN_COLUMN == node->type_) {                                                      \
        list->column_node_ = node;                                                            \
      } else {                                                                                \
        list->column_node_ = NULL;                                                            \
      }                                                                                       \
      list->head_ = node;                                                                     \
      list->tail_ = node;                                                                     \
    }                                                                                         \
  } while(0)                                                                                  \

#define check_and_add_relation(result, relation)                                                \
  do {                                                                                          \
    if (NULL == relation) {                                                                     \
    } else {                                                                                    \
      if (relation->level_ != PART_KEY_LEVEL_ZERO) {                                            \
        if (result->relation_info_.relation_num_ < OBPROXY_MAX_RELATION_NUM) {                  \
          result->relation_info_.relations_[result->relation_info_.relation_num_++] = relation; \
        } else {                                                                                \
          /* YYACCEPT; */                                                                       \
        }                                                                                       \
      }                                                                                         \
    }                                                                                           \
  } while(0)                                                                                    \

static int64_t get_part_key_idx(ObProxyParseString *db_name,
                                ObProxyParseString *table_name,
                                ObProxyParseString *column_name,
                                ObExprParseResult *result)
{
  int64_t part_key_idx = IDX_NO_PART_KEY_COLUMN;
  
  if (result->part_key_info_.key_num_ > 0) {
    if (NULL != db_name && !is_equal(db_name, &result->table_info_.database_name_)) {
      part_key_idx = IDX_NO_PART_KEY_COLUMN;
    } else if (NULL != table_name
               && !is_equal(table_name, &result->table_info_.table_name_)
               && !is_equal(table_name, &result->table_info_.alias_name_)) {
      part_key_idx = IDX_NO_PART_KEY_COLUMN;
    } else if (NULL != column_name) {
      int64_t i = 0;
      for (i = 0; i < result->part_key_info_.key_num_ && part_key_idx  < 0; ++i) {
        if (is_equal(column_name, &result->part_key_info_.part_keys_[i].name_)) {
          part_key_idx = i;
          break;
        }
      }
    }
  }
  return part_key_idx;
}
static inline void add_relation(ObExprParseResult *result,
                                ObProxyTokenList *left_value,
                                ObProxyFunctionType type,
                                ObProxyTokenList *right_value)
{
  if (result->all_relation_info_.relation_num_ < OBPROXY_MAX_RELATION_NUM) {
    ObProxyRelationExpr *relation = NULL;
    ObProxyTokenList *tmp_left = NULL;
    ObProxyTokenList *tmp_right = NULL;
    ObProxyFunctionType tmp_type = F_NONE;
    ObProxyPartKeyLevel tmp_level = PART_KEY_LEVEL_ZERO;

    if (NULL != left_value->column_node_
        && TOKEN_COLUMN == left_value->column_node_->type_) {
      tmp_left = left_value;
      tmp_right = right_value;
      tmp_type = type;
    } else if (NULL != right_value->column_node_
               && TOKEN_COLUMN == right_value->column_node_->type_) {
      tmp_left = right_value;
      tmp_right = left_value;
      tmp_type = get_reverse_func(type);
    }

    if (NULL == tmp_left || NULL == tmp_right || F_COMP_NE == tmp_type) {
      // will return null
    } else if (OB_ISNULL(relation = ((ObProxyRelationExpr *)obproxy_parse_malloc(
                                          sizeof(ObProxyRelationExpr), result->malloc_pool_)))) {
      // will return null
    } else {
      relation->left_value_ = tmp_left;
      relation->type_ = tmp_type;
      relation->right_value_ = tmp_right;
      relation->level_ = tmp_level;

      result->all_relation_info_.relations_[result->all_relation_info_.relation_num_++] = relation;
    }
  }
}

static inline void set_relation_part_with_column_idx(int64_t idx_in_schema_columns, 
                                                     ObExprParseResult *result, 
                                                     ObProxyPartKeyLevel *level, 
                                                     int64_t *first_part_column_idx, 
                                                     int64_t *second_part_column_idx) 
{
  if (OB_ISNULL(level) && OB_ISNULL(first_part_column_idx) && 
      OB_ISNULL(second_part_column_idx) && OB_ISNULL(result)) {
    // do nothing
  } else {
    *level = PART_KEY_LEVEL_ZERO;
    *first_part_column_idx = 0;
    *second_part_column_idx = 0;
    bool is_level_one = false;
    bool is_level_two = false;
    for (int i = 0; i < result->part_key_info_.key_num_; i++) {
      // make sure the part_key exist in sql to avoid the uninitialized idx_
      if (idx_in_schema_columns == result->part_key_info_.part_keys_[i].idx_ && result->part_key_info_.part_keys_[i].is_exist_in_sql_) {
        if (result->part_key_info_.part_keys_[i].level_ == PART_KEY_LEVEL_ONE) {
          is_level_one = true;
          *first_part_column_idx = result->part_key_info_.part_keys_[i].idx_in_part_columns_;
        } else if (result->part_key_info_.part_keys_[i].level_ == PART_KEY_LEVEL_TWO) {
          is_level_two = true;
          *second_part_column_idx = result->part_key_info_.part_keys_[i].idx_in_part_columns_;
        }
      }
    }
    if (is_level_one) {
      *level = PART_KEY_LEVEL_ONE;
    }
    if (is_level_two) {
      *level = PART_KEY_LEVEL_TWO;
    }
    if (is_level_one && is_level_two) {
      *level = PART_KEY_LEVEL_BOTH;
    }
  }
}

static inline void set_relation_part_with_column_name(ObProxyParseString *column,
                                                      ObExprParseResult *result,
                                                      ObProxyPartKeyLevel *level,
                                                      int64_t *first_part_column_idx,
                                                      int64_t *second_part_column_idx) 
{
  if (OB_ISNULL(column)
      || OB_ISNULL(result)
      || OB_ISNULL(level)
      || OB_ISNULL(first_part_column_idx)
      || OB_ISNULL(second_part_column_idx)) {
    // do nothing
  } else if (result->has_rowid_
             && is_equal_to_rowid(column)) {
    // handle rowid
    *level = PART_KEY_LEVEL_ONE;
    *first_part_column_idx = 0;
    *second_part_column_idx = 0;
  } else {
    *level = PART_KEY_LEVEL_ZERO;
    *first_part_column_idx = 0;
    *second_part_column_idx = 0;
    bool is_level_one = false;
    bool is_level_two = false;
    for (int i = 0; i < result->part_key_info_.key_num_; i++) {
      if (is_equal(&result->part_key_info_.part_keys_[i].name_, column)) {
        if (result->part_key_info_.part_keys_[i].level_ == PART_KEY_LEVEL_ONE) {
          is_level_one = true;
          *first_part_column_idx = result->part_key_info_.part_keys_[i].idx_in_part_columns_;
        } else if (result->part_key_info_.part_keys_[i].level_ == PART_KEY_LEVEL_TWO) {
          is_level_two = true;
          *second_part_column_idx = result->part_key_info_.part_keys_[i].idx_in_part_columns_;
        }
      }
    }
    if (is_level_one) {
      *level = PART_KEY_LEVEL_ONE;
    }
    if (is_level_two) {
      *level = PART_KEY_LEVEL_TWO;
    }
    if (is_level_one && is_level_two) {
      *level = PART_KEY_LEVEL_BOTH;
    }
  }
}


static inline ObProxyRelationExpr *get_relation(ObExprParseResult *result,
                                                ObProxyTokenList *left_value,
                                                ObProxyFunctionType type,
                                                ObProxyTokenList *right_value)
{
  ObProxyRelationExpr *relation = NULL;
  ObProxyTokenList *tmp_left = NULL;
  ObProxyTokenList *tmp_right = NULL;
  ObProxyFunctionType tmp_type = F_NONE;
  int64_t tmp_column_idx_ = -1;
  ObProxyParseString *tmp_column = NULL;

  if (NULL != left_value->column_node_
      && TOKEN_COLUMN == left_value->column_node_->type_
      && left_value->column_node_->part_key_idx_ >= 0) {
    tmp_left = left_value;
    tmp_right = right_value;
    tmp_type = type;
    tmp_column_idx_ = left_value->column_node_->part_key_idx_;
    tmp_column = &left_value->column_node_->column_name_;
  } else if (NULL != right_value->column_node_
             && TOKEN_COLUMN == right_value->column_node_->type_
             && right_value->column_node_->part_key_idx_ >= 0) {
    tmp_left = right_value;
    tmp_right = left_value;
    tmp_type = get_reverse_func(type);
    tmp_column_idx_ = right_value->column_node_->part_key_idx_;
    tmp_column = &right_value->column_node_->column_name_;
  }

  if (NULL == tmp_left || NULL == tmp_right || F_COMP_NE == tmp_type) {
    // will return null
  } else if (OB_ISNULL(relation = ((ObProxyRelationExpr *)obproxy_parse_malloc(
                                        sizeof(ObProxyRelationExpr), result->malloc_pool_)))) {
    // will return null
  } else {
    relation->column_idx_ = tmp_column_idx_;
    relation->left_value_ = tmp_left;
    relation->type_ = tmp_type;
    relation->right_value_ = tmp_right;
    set_relation_part_with_column_name(tmp_column,
                                       result,
                                       &relation->level_,
                                       &relation->first_part_column_idx_,
                                       &relation->second_part_column_idx_);
  }
  return relation;
}

static inline ObProxyRelationExpr *get_values_relation(ObExprParseResult *result,
                                                       ObProxyTokenList *right_value)
{
  ObProxyRelationExpr *relation = NULL;
  if (NULL == right_value) {
  // will return null
  } else {
    int64_t i = 0;
    for (i = 0; i < result->part_key_info_.key_num_; ++i) {
      // make sure the part_key exist in sql to avoid the uninitialized idx_
      if (result->values_list_idx_ == result->part_key_info_.part_keys_[i].idx_ && result->part_key_info_.part_keys_[i].is_exist_in_sql_) {
        if (OB_ISNULL(relation = ((ObProxyRelationExpr *)obproxy_parse_malloc(
                                        sizeof(ObProxyRelationExpr), result->malloc_pool_)))) {
        } else {
          relation->column_idx_ = i;
          relation->type_ = F_COMP_EQ;
          relation->right_value_ = right_value;
          relation->left_value_ = NULL;
          set_relation_part_with_column_idx(result->values_list_idx_, 
                                            result,
                                            &relation->level_, 
                                            &relation->first_part_column_idx_, 
                                            &relation->second_part_column_idx_);        
        }
        break;
      }
    }
  }
  return relation;
}

static inline void add_left_relation_value(ObExprParseResult *result,
                                                       ObProxyTokenList *left_value)
{
  ObProxyRelationExpr *relation = NULL;
  if (NULL == left_value) {
    // will return
  } else if (result->all_relation_info_.relation_num_ >= OBPROXY_MAX_RELATION_NUM) {
    // do nothing
  } else if (OB_ISNULL(relation = ((ObProxyRelationExpr *)obproxy_parse_malloc(
                                        sizeof(ObProxyRelationExpr), result->malloc_pool_)))) {
  } else {
    relation->type_ = F_COMP_EQ;
    relation->left_value_ = left_value;
    relation->right_value_ = NULL;
    result->all_relation_info_.relations_[result->all_relation_info_.relation_num_++] = relation;
  }
}

static inline void add_right_relation_value(ObExprParseResult *result,
                                            ObProxyTokenList *right_value)
{
  if (NULL == right_value) {
    // will return
  } else if (result->all_relation_info_.relation_num_ >= OBPROXY_MAX_RELATION_NUM) {
    // do nohting
  } else if (result->all_relation_info_.right_value_num_ >= result->all_relation_info_.relation_num_) {
    // ignore
  } else if (OB_ISNULL(result->all_relation_info_.relations_[result->all_relation_info_.right_value_num_])) {
  } else {
    ObProxyRelationExpr *relation = result->all_relation_info_.relations_[result->all_relation_info_.right_value_num_++];
    relation->type_ = F_COMP_EQ;
    relation->right_value_ = right_value;
  }
}

static inline ObProxyTokenNode* calc_unary_operator(ObProxyTokenNode *node, ObExprParseResult *result, 
                                       ObProxyOperatorType operator)
{
  ObProxyTokenNode *token_node = node;
  if (node == NULL) {
    // do nothing
  } else if (OPT_MINUS != operator) {
    // only need handle '-' now
    // do nothing
  } else if (TOKEN_INT_VAL == node->type_) {
    // don't worry out of range
    node->int_value_ = - node->int_value_;
  } else if (TOKEN_STR_VAL == node->type_) {
    // convert 'str' -> '-str'
    void *tmp_buf = NULL;
    if (OB_ISNULL(tmp_buf = ((char *)obproxy_parse_malloc(node->str_value_.str_len_ + 1, result->malloc_pool_)))) { 
        //do nothing                                                                                
    } else {
      node->str_value_.str_len_ = node->str_value_.str_len_ + 1;
      if (OPT_ADD == operator) {
        memcpy(tmp_buf, "+", 1);
      } else {
        memcpy(tmp_buf, "-", 1);
      }
      memcpy(tmp_buf + 1, node->str_value_.str_, node->str_value_.str_len_ - 1);
      node->str_value_.str_ = tmp_buf;
      node->str_value_.end_ptr_ = node->str_value_.str_ + node->str_value_.str_len_;
    }
  } else if (TOKEN_FUNC == node->type_) {
    // convert func: [-xxx()] ->  [0 - xxx()]                                                                
    ObProxyTokenNode *dummy_param = NULL;
    ObProxyTokenList *list = NULL;
    // we do nothing in obproxy_parse_free, memory will be free in allocator.resuse() finnaly
    if (OB_ISNULL(token_node = ((ObProxyTokenNode *)obproxy_parse_malloc(sizeof(ObProxyTokenNode), result->malloc_pool_)))) {
      // return NULL
    } else if (OB_ISNULL(dummy_param = ((ObProxyTokenNode *)obproxy_parse_malloc(sizeof(ObProxyTokenNode), result->malloc_pool_)))) {
      token_node = NULL;
      obproxy_parse_free(token_node);
      // return NULL
    } else if (OB_ISNULL(list = ((ObProxyTokenList *)obproxy_parse_malloc(sizeof(ObProxyTokenList), result->malloc_pool_)))) {
      token_node = NULL;
      list = NULL;
      obproxy_parse_free(token_node);
      obproxy_parse_free(list);
      // return NULL
    } else {
      token_node->type_ = TOKEN_FUNC;                                                               
      token_node->child_ = list;                                                                    
      token_node->next_ = NULL;
      store_const_str(token_node->str_value_, "-", 1);
      dummy_param->type_ = TOKEN_INT_VAL;
      dummy_param->child_ = NULL;
      dummy_param->next_ = NULL;
      dummy_param->int_value_ = 0;
      list->head_ = dummy_param;
      list->tail_ = dummy_param;
      list->tail_->next_ = node;
      list->tail_ = node;
    }
  }
  return token_node;
}

%}

 /* dummy node */
%token DUMMY_SELECT_CLAUSE DUMMY_INSERT_CLAUSE
 /* reserved keyword */
%token WHERE AS VALUES SET END_WHERE JOIN BOTH LEADING TRAILING FROM
%token AND_OP OR_OP IN ON BETWEEN IS NULL_VAL NOT
%token COMP_EQ COMP_NSEQ COMP_GE COMP_GT COMP_LE COMP_LT COMP_NE
%token PLACE_HOLDER
%token END_P ERROR IGNORED_WORD
 /* expression priority */
 %left '&'
 %left '+' '-'
 %left '*' '/' '%'
 %nonassoc LOWER_PARENS
 %left '(' 
 %right ')'
 %nonassoc HIGHER_PARENS
 %right '!'
 /* type token */
%token<str> NAME_OB STR_VAL ROW_ID NONE_PARAM_FUNC HEX_VAL TRIM 
%token<num> INT_VAL POS_PLACE_HOLDER
%type<func> comp
%type<node> token opt_column simple_expr trim_type
%type<list> expr token_list in_expr_list column_list func_param_list trim_param_list trim_type_list
%type<relation> bool_pri
%start start
%%
start: DUMMY_SELECT_CLAUSE select_root
     | DUMMY_INSERT_CLAUSE insert_root

select_root: WHERE cond_expr end_flag { YYACCEPT; }
           | join_expr_list end_flag { YYACCEPT; }
           | join_expr_list WHERE cond_expr end_flag { YYACCEPT; }
           | error { YYACCEPT; }

end_flag: END_WHERE
        | ';'
        | END_P

join_expr_list: join_on_expr
              | join_expr_list join_on_expr

join_on_expr: join_expr ON cond_expr

join_expr: JOIN NAME_OB
         | JOIN NAME_OB '.' NAME_OB
         | JOIN NAME_OB NAME_OB
         | JOIN NAME_OB AS NAME_OB
         | JOIN NAME_OB '.' NAME_OB NAME_OB
         | JOIN NAME_OB '.' NAME_OB AS NAME_OB

cond_expr: bool_pri { check_and_add_relation(result, $1); }
         | cond_expr AND_OP bool_pri { check_and_add_relation(result, $3); }
         | '(' cond_expr AND_OP bool_pri ')' { check_and_add_relation(result, $4); }
         | cond_expr OR_OP bool_pri { check_and_add_relation(result, $3); }
         | '(' cond_expr OR_OP bool_pri ')' { check_and_add_relation(result, $4); }

bool_pri: expr comp expr { add_relation(result, $1, $2,$3); $$ = get_relation(result, $1, $2, $3); }
        | '(' expr comp expr ')' { $$ = get_relation(result, $2, $3, $4); add_relation(result, $2, $3,$4); }
        | expr IN '(' in_expr_list ')' { $$ = get_relation(result, $1, F_COMP_EQ, $4); add_relation(result, $1, F_COMP_EQ,$4); }
        | expr BETWEEN expr AND_OP expr
        {
          $$ = get_relation(result, $1, F_COMP_GE, $3);
          check_and_add_relation(result, $$);
          add_relation(result, $1, F_COMP_GE, $3);
          $$ = get_relation(result, $1, F_COMP_LE, $5);
          check_and_add_relation(result, $$);
          add_relation(result, $1, F_COMP_LE, $5);
          $$ = NULL;
        }
        | expr IS NULL_VAL
        {
          ObProxyTokenNode *null_node = NULL;
          ObProxyTokenList *token_list = NULL;
          malloc_node(null_node, result, TOKEN_NULL);
          malloc_list(token_list, result, null_node);
          add_relation(result, $1, F_COMP_EQ, token_list);
          $$ = get_relation(result, $1, F_COMP_EQ, token_list);
        }
        | expr IS NOT NULL_VAL
        {
          ObProxyTokenNode *null_node = NULL;
          ObProxyTokenList *token_list = NULL;
          malloc_node(null_node, result, TOKEN_NULL);
          malloc_list(token_list, result, null_node);
          add_relation(result, $1, F_COMP_NE, token_list);
          $$ = get_relation(result, $1, F_COMP_NE, token_list);
        }

comp: COMP_EQ   { $$ = F_COMP_EQ; }
    | COMP_NSEQ { $$ = F_COMP_NSEQ; }
    | COMP_GE   { $$ = F_COMP_GE; }
    | COMP_GT   { $$ = F_COMP_GT; }
    | COMP_LE   { $$ = F_COMP_LE; }
    | COMP_LT   { $$ = F_COMP_LT; }
    | COMP_NE   { $$ = F_COMP_NE; }

in_expr_list: expr { $$ = $1; }
            | in_expr_list ',' expr { $$ = $1; add_token_list($1, $3); }

expr: token_list { $$ = $1; }

token_list: simple_expr { malloc_list($$, result, $1); }
          | token_list token { add_token($1, result, $2); $$ = $1; } 

simple_expr: token { $$ = $1; }
          | '(' simple_expr ')' %prec HIGHER_PARENS { $$ = $2; }
          | '+' simple_expr %prec '*' { $$ = calc_unary_operator($2, result, OPT_ADD); }
          | '-' simple_expr %prec '*' { $$ = calc_unary_operator($2, result, OPT_MINUS); }
          | '!' simple_expr %prec '!'
          {
            ObProxyTokenList *dummylist = NULL;
            malloc_list(dummylist, result, $2);
            malloc_func_node($$, result, OPT_NOT);
            $$->child_ = dummylist;
          }
          | simple_expr '+' simple_expr %prec '+'
          {
            ObProxyTokenList *dummylist = NULL;
            malloc_list(dummylist, result, $1);
            add_token(dummylist, result, $3);
            malloc_func_node($$, result, OPT_ADD);
            $$->child_ = dummylist;
          }
          | simple_expr '-' simple_expr %prec '-'
          {
            ObProxyTokenList *dummylist = NULL;
            malloc_list(dummylist, result, $1);
            add_token(dummylist, result, $3);
            malloc_func_node($$, result, OPT_MINUS);
            $$->child_ = dummylist;
          }
          | simple_expr '*' simple_expr %prec '*'
          {
            ObProxyTokenList *dummylist = NULL;
            malloc_list(dummylist, result, $1);
            add_token(dummylist, result, $3);
            malloc_func_node($$, result, OPT_MUL);
            $$->child_ = dummylist;
          }
          | simple_expr '/' simple_expr %prec '/'
          {
            ObProxyTokenList *dummylist = NULL;
            malloc_list(dummylist, result, $1);
            add_token(dummylist, result, $3);
            malloc_func_node($$, result, OPT_DIV);
            $$->child_ = dummylist;
          }
          | simple_expr '&' simple_expr %prec '&' 
          {
            ObProxyTokenList *dummylist = NULL;
            malloc_list(dummylist, result, $1);
            add_token(dummylist, result, $3);
            malloc_func_node($$, result, OPT_AND);
            $$->child_ = dummylist;
          }
          | simple_expr '%' simple_expr %prec '%'
          {
            ObProxyTokenList *dummylist = NULL;
            malloc_list(dummylist, result, $1);
            add_token(dummylist, result, $3);
            malloc_func_node($$, result, OPT_MOD);
            $$->child_ = dummylist;
          }

func_param_list: { $$ = NULL; } /* empty */
               | token_list ',' token_list         { add_token_list($1, $3); $$ = $1; }
               | func_param_list ',' token_list    { add_token_list($1, $3); $$ = $1; }

trim_param_list: simple_expr { malloc_list($$, result, $1); }
               | trim_type_list FROM token { add_token($1, result, $3); $$ = $1; }

trim_type_list: trim_type { malloc_list($$, result, $1); }
              | token 
              { 
                ObProxyTokenNode *default_type = NULL;
                malloc_node(default_type, result, TOKEN_INT_VAL);
                default_type->int_value_ = 0;
                malloc_list($$, result, default_type);
                add_token($$, result, $1); 
              }
              | trim_type token
              {
                malloc_list($$, result, $1);
                add_token($$, result, $2);
              }

trim_type: BOTH { malloc_node($$, result, TOKEN_INT_VAL); $$->int_value_ = 0; }
         | LEADING { malloc_node($$, result, TOKEN_INT_VAL); $$->int_value_ = 1; }
         | TRAILING { malloc_node($$, result, TOKEN_INT_VAL); $$->int_value_ = 2; }
               
token:
     ROW_ID
     {
       malloc_node($$, result, TOKEN_COLUMN);
       $$->part_key_idx_ = 0;
       $$->column_name_ = $1;
       result->has_rowid_ = true;
     }
     | HEX_VAL 
     { 
      malloc_node($$, result, TOKEN_HEX_VAL); $$->str_value_ = $1; 
     }
     | NAME_OB
     {
       malloc_node($$, result, TOKEN_COLUMN);
       $$->part_key_idx_ = get_part_key_idx(NULL, NULL, &$1, result);
       $$->column_name_ = $1;
     }
     | NAME_OB '.' NAME_OB
     {
       malloc_node($$, result, TOKEN_COLUMN);
       $$->part_key_idx_ = get_part_key_idx(NULL, &$1, &$3, result);
       $$->column_name_ = $3;
     }
     | NAME_OB '.' NAME_OB '.' NAME_OB
     {
       malloc_node($$, result, TOKEN_COLUMN);
       $$->part_key_idx_ = get_part_key_idx(&$1, &$3, &$5, result);
       $$->column_name_ = $5;
     }
     | NONE_PARAM_FUNC
     {
       malloc_node($$, result, TOKEN_FUNC);
       $$->str_value_ = $1;
     } 
     | NONE_PARAM_FUNC '(' ')'
     {
       malloc_node($$, result, TOKEN_FUNC);
       $$->str_value_ = $1;
     }
     | NONE_PARAM_FUNC '(' token_list ')'
     {
       malloc_node($$, result, TOKEN_FUNC);
       $$->str_value_ = $1;
       $$->child_ = $3;
     }
     | TRIM '(' trim_param_list ')' 
     {
      malloc_node($$, result, TOKEN_FUNC);
      $$->str_value_ = $1;
      $$->child_ = $3;
     }
     | NAME_OB '(' token_list ')'
     {
       malloc_node($$, result, TOKEN_FUNC);
       $$->str_value_ = $1;
       $$->child_ = $3;
     }
     | NAME_OB '(' func_param_list ')'
     {
       malloc_node($$, result, TOKEN_FUNC);
       $$->str_value_ = $1;
	     $$->child_ = $3;
     }
     | INT_VAL { malloc_node($$, result, TOKEN_INT_VAL); $$->int_value_ = $1; }
     | STR_VAL { malloc_node($$, result, TOKEN_STR_VAL); $$->str_value_ = $1; }
     | PLACE_HOLDER
     {
       result->placeholder_list_idx_++;
       malloc_node($$, result, TOKEN_PLACE_HOLDER);
       $$->placeholder_idx_ = result->placeholder_list_idx_ - 1;
     }
     | POS_PLACE_HOLDER
     {
       malloc_node($$, result, TOKEN_PLACE_HOLDER);
       $$->placeholder_idx_ = $1;
     }
     | NULL_VAL
     {
       malloc_node($$, result, TOKEN_NULL);
     }

insert_root: opt_column_list VALUES values_expr_lists end_flag { YYACCEPT; }
           | SET set_expr opt_where_clause end_flag { YYACCEPT; }
           | ON cond_expr end_flag { YYACCEPT; }
           | select_root

values_expr_lists: '(' values_expr_list ')'
                 {
                   result->multi_param_values_++;
                 }
                 | values_expr_lists ',' '(' values_expr_list ')'
                 {
                   result->multi_param_values_++;
                 }

opt_column_list: /* empty */ { init_part_key_all_match(result);}
               | '(' column_list ')'

 /* column_list: NAME_OB { result->column_idx_ = 0; set_part_key_column_idx(result, &$1); }
           | column_list ',' NAME_OB { result->column_idx_++; set_part_key_column_idx(result, &$3); } */

column_list: opt_column {
                          malloc_list($$, result, $1);
                          add_left_relation_value(result, $$);
                        }
           | column_list ',' opt_column {
                                malloc_list($$, result, $3);
                                add_left_relation_value(result, $$);
                              }

opt_column: NAME_OB {
                      set_part_key_column_idx(result, &$1);
                      result->column_idx_++;
                      malloc_node($$, result, TOKEN_COLUMN);
                      $$->column_name_ = $1;
                    }
values_expr_list:expr
                {
                  if (result->multi_param_values_ < 1) {
                    result->values_list_idx_ = 0;
                    result->all_relation_info_.right_value_num_ = 0;
                    ObProxyRelationExpr *relation = get_values_relation(result, $1);
                    check_and_add_relation(result, relation);
                    add_right_relation_value(result, $1);
                  }
                }
                | values_expr_list ',' expr
                {
                  if (result->multi_param_values_ < 1) {
                    result->values_list_idx_++;
                    ObProxyRelationExpr *relation = get_values_relation(result, $3);
                    check_and_add_relation(result, relation);
                    add_right_relation_value(result, $3);
                  }
                }

set_expr: bool_pri { check_and_add_relation(result, $1); }
        | set_expr ',' bool_pri { check_and_add_relation(result, $3); }

opt_where_clause: /* empty */
                | WHERE cond_expr {}
%%
void yyerror(YYLTYPE* yylloc, ObExprParseResult* p, char* s, ...)
{
  // do nothing
}

void ob_expr_parser_fatal_error(yyconst char *msg, yyscan_t yyscanner)
{
  fprintf(stderr, "FATAL ERROR:%s\n", msg);
  ObExprParseResult *p = ob_expr_parser_yyget_extra(yyscanner);
  if (OB_ISNULL(p)) {
    fprintf(stderr, "unexpected null parse result\n");
  } else {
    longjmp(p->jmp_buf_, 1);//the secord param must be non-zero value
  }
}

int ob_expr_parse_sql(ObExprParseResult* p, const char* buf, size_t len)
{
  int ret = OB_SUCCESS;
  //obexprdebug = 1;
  if (OB_ISNULL(p) || OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    // print err msg later
  } else if (OB_FAIL(ob_expr_parser_yylex_init_extra(p, &(p->yyscan_info_)))) {
    // print err msg later
  } else {
    int val = setjmp(p->jmp_buf_);
    if (val) {
      ret = OB_PARSER_ERR_PARSE_SQL;
    } else {
      ob_expr_parser_yy_scan_buffer((char *)buf, len, p->yyscan_info_);
      if (OB_FAIL(ob_expr_parser_yyparse(p))) {
        // print err msg later
      } else {
        // do nothing
      }
    }
  }

  return ret;
}
