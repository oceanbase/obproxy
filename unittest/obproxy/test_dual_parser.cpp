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

#define USING_LOG_PREFIX PROXY_EVENT
#include <pthread.h>
#include "opsql/dual_parser/ob_dual_parser.h"
#include "lib/utility/ob_macro_utils.h"
// #define USING_GTEST
#ifdef USING_GTEST
#include <gtest/gtest.h>
#endif



using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::common;
using namespace oceanbase::obproxy;

namespace oceanbase
{
namespace obproxy
{
void show_parser_result(const ObProxyDualParseResult& result) {
	printf("select_fields_ is :\n");
	ObString alias;
	for (int i = 0; i < result.select_fields_size_; i++) {
		if (result.select_fields_[i].alias_name_.ptr() != NULL) {
			alias = result.select_fields_[i].alias_name_;
		} else {
			alias = ObString::make_string("");
		}
		printf("[%d]:seq_name:%.*s, seq_field:%.*s, alis_name:%.*s\n", i,
		       result.select_fields_[i].seq_name_.length(),
		       result.select_fields_[i].seq_name_.ptr(),
		       result.select_fields_[i].seq_field_.length(),
		       result.select_fields_[i].seq_field_.ptr(),
		       alias.length(), alias.ptr());
	}
	const ObExprParseResult& expr_result = result.expr_result_;
	int64_t total_num = expr_result.all_relation_info_.relation_num_;
	for (int64_t i = 0; i < total_num; i ++) {
		ObProxyRelationExpr* relation_expr = expr_result.all_relation_info_.relations_[i];
		if (relation_expr == NULL) {
			printf("Got an empty relation_expr");
			continue;
		}
		if (relation_expr->left_value_ != NULL
		    && relation_expr->left_value_->head_ != NULL
		    && relation_expr->left_value_->head_->type_ == TOKEN_COLUMN) {
			if (relation_expr->left_value_->head_->column_name_.str_ != NULL) {
				ObString tmp_column(relation_expr->left_value_->head_->column_name_.str_len_,
				                    relation_expr->left_value_->head_->column_name_.str_);
				LOG_DEBUG("column_name is ", K(tmp_column));
			} else {
				LOG_WARN("get an empty column_name_");
			}
		} else {
			LOG_WARN("left value is null");
		}
		if (relation_expr->right_value_ != NULL
		    && relation_expr->right_value_->head_ != NULL) {
			if (relation_expr->right_value_->head_->type_ == TOKEN_INT_VAL) {
				LOG_DEBUG("right_value is int", K(relation_expr->right_value_->head_->int_value_));
				// snprintf(sql_result.fields_[sql_result.field_num_].column_value_,
				//            32, "%ld", relation_expr->right_value_->head_->int_value_);
				// sql_result.fields_[sql_result.field_num_].column_value_ = "";
				// sql_result.fields_[sql_result.field_num_].column_value_ = std::string(buf);
			} else if (relation_expr->right_value_->head_->type_ == TOKEN_STR_VAL) {
				ObString tmp_value(relation_expr->right_value_->head_->str_value_.str_len_,
				                   relation_expr->right_value_->head_->str_value_.str_);
				LOG_DEBUG("tmp_value", K(tmp_value));

				// strncpy(sql_result.fields_[sql_result.field_num_].column_value_,
				//           relation_expr->right_value_->head_->str_value_.str_,
				//           relation_expr->right_value_->head_->str_value_.str_len_);
				// sql_result.fields_[sql_result.field_num_].column_value_ = "";
				// std::string(relation_expr->right_value_->head_->str_value_.str_,
				//   relation_expr->right_value_->head_->str_value_.str_len_);
			} else {
				printf("invalid type :");
			}
		} else {
			printf("right value is null\n");
		}
	}
}
#ifdef USING_GTEST
TEST(ObProxyDualParserTest, SIMPLE_DUAL_MULTI_COL_TEST)
{
	ObProxyDualParser parser;
	ObProxyDualParseResult result;
	ObString sql = ObString::make_string("select test_seq.next_val, test_seq.min_value, test_seq.uid  from dual;");
	parser.parse(sql, result, CS_TYPE_UTF8MB4_GENERAL_CI);
	show_parser_result(result);
	ASSERT_FALSE(parser.is_valid_result());
}
TEST(ObProxyDualParserTest, INVALID_COL_TEST)
{
	ObProxyDualParser parser;
	ObProxyDualParseResult result;
	ObString sql = ObString::make_string("select test_seq1.next_val, test_seq2.min_value, test_seq.uid  from dual;");
	parser.parse(sql, result, CS_TYPE_UTF8MB4_GENERAL_CI);
	show_parser_result(result);
	ASSERT_FALSE(parser.is_valid_result());
}
TEST(ObProxyDualParserTest, SIMPLE_DUAL_ONE_COL_TEST)
{
	ObProxyDualParser parser;
	ObProxyDualParseResult result;
	ObString sql = ObString::make_string("select test_sql.nextval from dual");
	parser.parse(sql, result, CS_TYPE_UTF8MB4_GENERAL_CI);
	show_parser_result(result);
	ASSERT_TRUE(parser.is_valid_result());
}
TEST(ObProxyDualParserTest, ONE_WHERE_COL_TEST)
{
	ObProxyDualParser parser;
	ObProxyDualParseResult result;
	ObString sql = ObString::make_string("select test.nextval from dual where sharding_col = '00'");
	parser.parse(sql, result, CS_TYPE_UTF8MB4_GENERAL_CI);
	show_parser_result(result);
	ASSERT_TRUE(parser.is_valid_result());
}
TEST(ObProxyDualParserTest, MULTI_WHERE_COLS_TEST)
{
	ObProxyDualParser parser;
	ObProxyDualParseResult result;
	ObString sql = ObString::make_string("select test_sql.nextval from dual where sharding_col = '123' and name = 'test'");
	parser.parse(sql, result, CS_TYPE_UTF8MB4_GENERAL_CI);
	show_parser_result(result);
	ASSERT_TRUE(parser.is_valid_result());
}
TEST(ObProxyDualParserTest, ALIAS_NAME_TEST)
{
	ObProxyDualParser parser;
	ObProxyDualParseResult result;
	ObString sql = ObString::make_string("select test_seq.nextval next_val, test_seq.min_value min_value, test_seq.uid as uid from dual;");
	parser.parse(sql, result, CS_TYPE_UTF8MB4_GENERAL_CI);
	show_parser_result(result);
	ASSERT_FALSE(parser.is_valid_result());
}
#endif
} // end of namespace obproxy
} // end of namespace oceanbase
int main(int argc, char **argv)
{
	oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
	OB_LOGGER.set_log_level("DEBUG");
#ifndef USING_GTEST
	int ret = OB_SUCCESS;
	if (argc == 2) {
		ObProxyDualParser parser;
		ObProxyDualParseResult result;
		ObString sql = ObString::make_string(argv[1]);
		if (OB_FAIL(parser.parse(sql, result, CS_TYPE_UTF8MB4_GENERAL_CI))) {
			fprintf(stderr, "parse sql failed");
		} else {
			oceanbase::obproxy::show_parser_result(result);
		}
		return 0;
	} else {
		fprintf(stderr, "invalid argu");
	}
	#else
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
	#endif
}
