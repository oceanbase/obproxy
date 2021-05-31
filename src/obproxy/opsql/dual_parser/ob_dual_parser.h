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

#ifndef OB_PROXY_DUAL_PARSER_H_
#define OB_PROXY_DUAL_PARSER_H_
#include "lib/string/ob_string.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "obutils/ob_proxy_sql_parser.h"
namespace oceanbase
{
namespace obproxy
{
namespace opsql
{
class ObProxyDualParser
{
public:
	ObProxyDualParser();
	~ObProxyDualParser();
	int parse(const common::ObString &sql_string,
	          obutils::ObProxyDualParseResult &parse_result);
	bool is_valid_result();
private:
	int skip_comments();
	int parse_key_word(const char* key, bool allow_semi = false);
	int parse_seqs_and_fields();
	int parse_where_key_word();
	int parse_where_fields();

	inline bool is_slash_char(const char c) {
		return c == '/';
	}

	inline bool is_single_quote_char(const char c) {
		return c == '\'';
	}
	inline bool is_blanket_char(const char c) {
		return c == ' ';
	}
	inline bool is_dot_char(const char c) {
		return c == '.';
	}
	inline bool is_comma_char(const char c) {
		return c == ',';
	}

	inline bool is_semi_char(const char c) {
		return c == ';';
	}
	inline bool is_equal_char(const char c) {
		return c == '=';
	}
	inline bool is_parse_end();
	inline bool is_match_word(const common::ObString& str1, const char* str2) {
		return str1.case_compare(str2) == 0;
	}
	void reset();
	inline int is_valid_find() ;
	inline int find_next_blanket();
	inline int find_next_not_blanket();
	inline int find_next_slash();
	inline int find_next_dot();
	inline int find_next_dot_or_blanket();
	inline int find_next_comma();
	inline int find_next_semi();
	inline int find_next_equal();
	inline int find_next_comma_or_blanket();
	inline int find_next_semi_or_blanket();
	inline int find_next_equal_or_blanket();


	int64_t last_pos_;
	int64_t cur_pos_;
	int64_t length_;
	const char* sql_ptr_;
	obutils::ObProxyDualParseResult* result_; // set from parse function do not need to free
	oceanbase::common::ObArenaAllocator* allocator_;
	DISALLOW_COPY_AND_ASSIGN(ObProxyDualParser);
};
} // end of namespace opsql
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OB_PROXY_DUAL_PARSER_H_