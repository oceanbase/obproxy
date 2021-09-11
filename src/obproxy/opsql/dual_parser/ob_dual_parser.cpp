/**
 * Copyright (c) 2021 OceanBase
 * OceanBase Database Proxy(ODP) is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PROXY

#include "opsql/dual_parser/ob_dual_parser.h"
#include "opsql/expr_parser/ob_expr_parser.h"
#include "obutils/ob_proxy_sql_parser.h"


using namespace oceanbase::common;
using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace opsql
{
ObProxyDualParser::ObProxyDualParser()
  : last_pos_(0), cur_pos_(0), result_(NULL), allocator_(NULL)
{
}
ObProxyDualParser::~ObProxyDualParser()
{
  if (allocator_ != NULL) {
    allocator_->reuse();
    allocator_ = NULL;
  }
}
void ObProxyDualParser::reset()
{
  cur_pos_ = 0;
  last_pos_ = 0;
  result_ = NULL;
}
bool ObProxyDualParser::is_valid_result()
{
  if (OB_ISNULL(result_)) {
    return false;
  }
  for (int i = 0; i < result_->select_fields_size_; i++) {
    if (result_->select_fields_[i].seq_name_.case_compare(result_->select_fields_[0].seq_name_) != 0) {
      LOG_DEBUG("seq_name is not euql to 0 seq_name ", K(i), K(result_->select_fields_[i].seq_name_),
               K(result_->select_fields_[0].seq_name_));
      return false;
    } else if (result_->select_fields_[i].seq_field_.case_compare("nextval") == 0) {
      result_->need_value_ = true;
      continue;
    }  else if (result_->select_fields_[i].seq_field_.case_compare("timestamp") == 0) {
      continue;
    } else if (result_->select_fields_[i].seq_field_.case_compare("dbtimestamp") == 0) {
      result_->need_db_timestamp_ = true;
      continue;
    } else if (result_->select_fields_[i].seq_field_.case_compare("tableid") == 0) {
      continue;
    } else if (result_->select_fields_[i].seq_field_.case_compare("groupid") == 0) {
      continue;
    } else if (result_->select_fields_[i].seq_field_.case_compare("eid") == 0
               || result_->select_fields_[i].seq_field_.case_compare("elasticid") == 0) {
      continue;
    } else {
      LOG_DEBUG("select filed is invalid:", K(result_->select_fields_[i].seq_field_));
      return false;
    }
  }
  return true;
}

int ObProxyDualParser::parse_key_word(const char* key, bool allow_semi)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(skip_comments())) {
    LOG_WARN("skip_comments fail", K(sql_ptr_), K(cur_pos_));
    return ret;
  }
  last_pos_ = cur_pos_;
  if (allow_semi) {
    if (OB_FAIL(find_next_semi_or_blanket())) {
      LOG_WARN("find_next_blanket fail", K(sql_ptr_), K(cur_pos_));
      return ret;
    }
  } else {
    if (OB_FAIL(find_next_blanket())) {
      LOG_WARN("find_next_blanket fail", K(sql_ptr_), K(cur_pos_));
      return ret;
    }
  }
  common::ObString str(cur_pos_ - last_pos_, sql_ptr_ + last_pos_);
  LOG_DEBUG("str is ", K(str));
  if (is_match_word(str, key)) {
    return ret;
  } else {
    LOG_WARN("not match word ", K(sql_ptr_), K(cur_pos_), K(key));
    return OB_ERR_UNEXPECTED;
  }
}

inline int ObProxyDualParser::is_valid_find()
{
  if (cur_pos_ >= length_) {
    return OB_ERR_UNEXPECTED;
  }
  return OB_SUCCESS;
}

inline int ObProxyDualParser::find_next_blanket()
{
  for (; cur_pos_ < length_; cur_pos_++) {
    if (is_blanket_char(sql_ptr_[cur_pos_])) {
      break;
    }
  }
  return is_valid_find();
}

inline int ObProxyDualParser::find_next_not_blanket()
{
  for (; cur_pos_ < length_; cur_pos_++) {
    if (!is_blanket_char(sql_ptr_[cur_pos_])) {
      break;
    }
  }
  return is_valid_find();
}

inline int ObProxyDualParser::find_next_dot()
{
  for (; cur_pos_ < length_; cur_pos_++) {
    if (is_dot_char(sql_ptr_[cur_pos_])) {
      break;
    }
  }
  return is_valid_find();
}
inline int ObProxyDualParser::find_next_equal()
{
  for (; cur_pos_ < length_; cur_pos_++) {
    if (is_equal_char(sql_ptr_[cur_pos_])) {
      break;
    }
  }
  return is_valid_find();
}

int ObProxyDualParser::find_next_dot_or_blanket()
{
  for (; cur_pos_ < length_; cur_pos_++) {
    if (is_dot_char(sql_ptr_[cur_pos_]) ||
        is_blanket_char(sql_ptr_[cur_pos_])) {
      break;
    }
  }
  return is_valid_find();
}
int ObProxyDualParser::find_next_comma()
{
  for (; cur_pos_ < length_; cur_pos_++) {
    if (is_comma_char(sql_ptr_[cur_pos_])) {
      break;
    }
  }
  return is_valid_find();
}

int ObProxyDualParser::find_next_semi()
{
  for (; cur_pos_ < length_; cur_pos_++) {
    if (is_semi_char(sql_ptr_[cur_pos_])) {
      break;
    }
  }
  return is_valid_find();
}

int ObProxyDualParser::find_next_slash()
{
  for (; cur_pos_ < length_; cur_pos_++) {
    if (is_slash_char(sql_ptr_[cur_pos_])) {
      break;
    }
  }
  return is_valid_find();
}

int ObProxyDualParser::find_next_comma_or_blanket()
{
  for (; cur_pos_ < length_; cur_pos_++) {
    if (is_comma_char(sql_ptr_[cur_pos_]) ||
        is_blanket_char(sql_ptr_[cur_pos_])) {
      break;
    }
  }
  return is_valid_find();
}

int ObProxyDualParser::find_next_equal_or_blanket()
{
  for (; cur_pos_ < length_; cur_pos_++) {
    if (is_equal_char(sql_ptr_[cur_pos_]) ||
        is_blanket_char(sql_ptr_[cur_pos_])) {
      break;
    }
  }
  return is_valid_find();
}
int ObProxyDualParser::find_next_semi_or_blanket() {
  for (; cur_pos_ < length_; cur_pos_++) {
    if (is_semi_char(sql_ptr_[cur_pos_]) ||
        is_blanket_char(sql_ptr_[cur_pos_])) {
      break;
    }
  }
  return OB_SUCCESS;
  // return is_valid_find();
}

int ObProxyDualParser::skip_comments()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(find_next_not_blanket())) {
    LOG_WARN("find_next_not_blanket failed", K(sql_ptr_), K(cur_pos_));
    return ret;
  }
  while (true) {
    if (is_slash_char(sql_ptr_[cur_pos_])) {
      ++cur_pos_; // skip cur /*
      if (OB_FAIL(find_next_slash())) {
        LOG_WARN("find_next_slash failed", K(sql_ptr_), K(cur_pos_));
        return ret;
      }
      ++cur_pos_; // skip cur */
      if (OB_FAIL(find_next_not_blanket())) {
        LOG_WARN("find_next_not_blanket failed", K(sql_ptr_), K(cur_pos_));
        return ret;
      }
    } else {
      LOG_DEBUG("not slash end now");
      break;
    }
  }
  return ret;
}
int ObProxyDualParser::parse_seqs_and_fields()
{
  int ret = OB_SUCCESS;
  while (true) {
    {
      // code block find seq_name
      LOG_DEBUG("parse_seqs_and_fields 1:", K(cur_pos_), K(last_pos_), K(sql_ptr_));
      // find seq_name
      if (OB_FAIL(skip_comments())) {
        LOG_WARN("skip_comments failed", K(sql_ptr_), K(cur_pos_));
        return ret;
      }
      LOG_DEBUG("parse_seqs_and_fields 2:", K(cur_pos_), K(last_pos_), K(sql_ptr_));
      last_pos_ = cur_pos_;
      if (OB_FAIL(find_next_dot_or_blanket())) {
        LOG_DEBUG("find_next_dot_or_blanket failed", K(sql_ptr_), K(cur_pos_));
        return ret;
      }
      LOG_DEBUG("parse_seqs_and_fields 3:", K(cur_pos_), K(last_pos_), K(sql_ptr_));
      common::ObString seq_name(cur_pos_ - last_pos_, sql_ptr_ + last_pos_);
      LOG_DEBUG("seq_name is ", K(seq_name));
      if (seq_name.case_compare("FROM") == 0) {
        break;
      }
      result_->select_fields_[result_->select_fields_size_].seq_name_ = seq_name;
      if (is_blanket_char(sql_ptr_[cur_pos_])) {
        if (OB_FAIL(find_next_dot())) {
          LOG_WARN("find_next_dot failed", K(sql_ptr_), K(cur_pos_));
          return ret;
        }
      }
    }
    ++cur_pos_;
    {
      // code block find seq_field
      LOG_DEBUG("parse_seqs_and_fields 4:", K(cur_pos_), K(last_pos_), K(sql_ptr_));
      // find dual_field
      if (OB_FAIL(find_next_not_blanket())) {
        LOG_DEBUG("find_next_not_blanket failed", K(sql_ptr_), K(cur_pos_));
        return ret;
      }
      LOG_DEBUG("parse_seqs_and_fields 5:", K(cur_pos_), K(last_pos_), K(sql_ptr_));
      last_pos_ = cur_pos_;
      if (OB_FAIL(find_next_comma_or_blanket())) {
        LOG_DEBUG("find_next_comma_or_blanket failed", K(sql_ptr_), K(cur_pos_));
        return ret;
      }
      LOG_DEBUG("parse_seqs_and_fields 6:", K(cur_pos_), K(last_pos_), K(sql_ptr_));
      common::ObString seq_field(cur_pos_ - last_pos_, sql_ptr_ + last_pos_);
      LOG_DEBUG("seq_field is ", K(seq_field));
      result_->select_fields_[result_->select_fields_size_].seq_field_ = seq_field;
    }
    if (is_blanket_char(sql_ptr_[cur_pos_])) {
      if (OB_FAIL(find_next_not_blanket())) {
        LOG_DEBUG("find_next_not_blanket failed", K(sql_ptr_), K(cur_pos_));
        return ret;
      }
    }
    if (is_comma_char(sql_ptr_[cur_pos_])) {
      ++result_->select_fields_size_;
      ++cur_pos_;
      // comma continue parse seq and name
      continue;
    } else {
      last_pos_ = cur_pos_;
      // not comma have alias
      if (OB_FAIL(find_next_comma_or_blanket())) {
        LOG_DEBUG("find_next_comma_or_blanket", K(sql_ptr_), K(cur_pos_));
        return ret;
      }
      common::ObString seg_str(cur_pos_ - last_pos_, sql_ptr_ + last_pos_);
      LOG_DEBUG("seq_str is ", K(seg_str));
      if (seg_str.case_compare("AS") == 0) {
        if (OB_FAIL(find_next_not_blanket())) {
          LOG_DEBUG("find_next_not_blanket failed", K(sql_ptr_), K(cur_pos_));
          return ret;
        }
        last_pos_ = cur_pos_;
        if (OB_FAIL(find_next_comma_or_blanket())) {
          LOG_DEBUG("find_next_comma_or_blanket", K(sql_ptr_), K(cur_pos_));
          return ret;
        }
        common::ObString alias_str(cur_pos_ - last_pos_, sql_ptr_ + last_pos_);
        result_->select_fields_[result_->select_fields_size_].alias_name_ = alias_str;
      } else if (seg_str.case_compare("FROM") == 0) {
        ++result_->select_fields_size_;
        break;
      } else {
        // not as this is alias
        result_->select_fields_[result_->select_fields_size_].alias_name_ = seg_str;
      }
      if (is_blanket_char(sql_ptr_[cur_pos_])) {
        if (OB_FAIL(find_next_not_blanket())) {
          LOG_DEBUG("find_next_not_blanket failed", K(sql_ptr_), K(cur_pos_));
          return ret;
        }
      }
      if (is_comma_char(sql_ptr_[cur_pos_])) {
        ++cur_pos_;
      }
    }
    ++result_->select_fields_size_;
  }
  return ret;
}
bool ObProxyDualParser::is_parse_end()
{
  if (is_semi_char(sql_ptr_[cur_pos_]) ||
      cur_pos_ >= length_) {
    return true;
  }
  return false;
}
int ObProxyDualParser::parse_where_key_word()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(find_next_not_blanket())) {
    LOG_DEBUG("sql is end not have where");
    return OB_SUCCESS;
  }
  if (is_parse_end()) {
    LOG_DEBUG("sql is end not have where");
    return OB_SUCCESS;
  }
  if (OB_FAIL(parse_key_word("WHERE", false))) {
    LOG_DEBUG("parse WHERE failed");
    return ret;
  }
  return OB_SUCCESS;
}

int ObProxyDualParser::parse_where_fields(ObCollationType connection_collation)
{
  int ret = OB_SUCCESS;
  // OB_PROXY_MAX_CONFIG_STRING_LENGTH is 512
  // Parse need last two should be 0
  if (length_ >= 510) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql is too long", KP(sql_ptr_));
    return ret;
  }
  ObExprParseMode parse_mode = SELECT_STMT_PARSE_MODE;
  ObExprParseResult& expr_result = result_->expr_result_;
  expr_result.target_mask_ = FIRST_PART_MASK; // set for init success
  if (OB_FAIL(ObProxySqlParser::get_parse_allocator(allocator_))) {
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObExprParser expr_parser(*allocator_, parse_mode);
    expr_result.part_key_info_.key_num_ = 0;
    ObString expr_sql(length_ + 2, sql_ptr_);
    expr_sql.ptr()[length_] = 0;
    expr_sql.ptr()[length_ + 1] = 0;

    if (SELECT_STMT_PARSE_MODE == parse_mode) {
      const char *expr_sql_str = expr_sql.ptr();
      const char *pos = NULL;
      if (OB_LIKELY(NULL != expr_sql_str)
          && NULL != (pos = strcasestr(expr_sql_str, "WHERE"))
          && (pos - expr_sql_str) > 1
          && OB_LIKELY((pos - expr_sql_str) < length_)) {
        const int32_t len_before_where = static_cast<int32_t>(pos - expr_sql_str);
        expr_sql += (len_before_where - 1);
      }
    }
    if (OB_FAIL(expr_parser.parse(expr_sql, expr_result, connection_collation))) {
      LOG_DEBUG("parse failed ", K(expr_sql));
    } else {
      LOG_DEBUG("expr_sql", K(expr_sql));
    }
  }
  return ret;
}

int ObProxyDualParser::parse(const common::ObString &sql_string,
                             ObProxyDualParseResult &parse_result,
                             ObCollationType connection_collation)
{
  int ret = OB_SUCCESS;
  reset();
  memset((void*)&parse_result, 0, sizeof(parse_result));
  result_ = &parse_result;
  length_ = sql_string.length();
  sql_ptr_ = sql_string.ptr();
  if (OB_FAIL(parse_key_word("SELECT", false))) {
    LOG_WARN("parse_key_word SELECT failed");
    return ret;
  } else if (OB_FAIL(parse_seqs_and_fields())) {
    LOG_WARN("parse_seqs_and_fields failed");
    // } else if (OB_FAIL(parse_key_word("FROM", false))){
    //   LOG_WARN("parse_key_word FROM failed");
  } else if (OB_FAIL(parse_key_word("DUAL", true))) {
    LOG_WARN("parse_key_word DUAL failed");
  } else if (OB_FAIL(parse_where_key_word())) {
    LOG_DEBUG("parse_where_and_fields failed");
  } else if (OB_FAIL(parse_where_fields(connection_collation))) {
    LOG_DEBUG("parse_where_fields failed");
  }
  return ret;
}

} // end of namespace opsql
} // end of namespace obproxy
} // end of namespace oceanbase
