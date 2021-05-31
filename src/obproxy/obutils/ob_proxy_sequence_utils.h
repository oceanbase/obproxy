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

#ifndef OBPROXY_SEQUENCE_UTILS_H
#define OBPROXY_SEQUENCE_UTILS_H
#include "lib/ob_define.h"
#include "lib/container/ob_iarray.h"
namespace oceanbase
{
namespace common
{
class ObString;
class ObAddr;
}

namespace obproxy
{
namespace proxy
{
class ObProxySequenceUtils
{
public:
	static int get_sequence_entry_sql(char*sql_buf, const int64_t buf_len,
	                                  const common::ObString& database_name,
	                                  const common::ObString& table_name,
	                                  const common::ObString& seq_name,
	                                  const common::ObString& tnt_id,
	                                  const common::ObString& tnt_col);

	static int insert_sequence_entry_sql(char*sql_buf, const int64_t buf_len,
	                                     const common::ObString& database_name,
	                                     const common::ObString& table_name,
	                                     const common::ObString& seq_name,
	                                     const common::ObString& tnt_id,
	                                     const common::ObString& tnt_col,
	                                     int64_t min_value,
	                                     int64_t max_value,
	                                     int64_t step,
	                                     int64_t value,
	                                     const common::ObString& now_time);

	static int update_sequence_entry_sql(char*sql_buf, const int64_t buf_len,
	                                     const common::ObString& database_name,
	                                     const common::ObString& table_name,
	                                     const common::ObString& seq_name,
	                                     const common::ObString& tnt_id,
	                                     const common::ObString& tnt_col,
	                                     int64_t new_value,
	                                     int64_t old_value);
	static int get_nowtime_string(char* str_buf, int buf_size);

	static const common::ObString& get_default_sequence_table_name();
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbse
#endif //OBPROXY_SEQUENCE_UTILS_H
