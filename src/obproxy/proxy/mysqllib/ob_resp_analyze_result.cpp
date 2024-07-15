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
#include "proxy/mysqllib/ob_resp_analyze_result.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
int64_t ObRespAnalyzeResult::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_trans_completed),
       K_(is_resp_completed),
       K_(ending_type),
       K_(is_partition_hit),
       K_(has_new_sys_var),
       K_(has_proxy_idc_name_user_var),
       K_(is_server_db_reset),
       K_(reserved_ok_len_of_mysql),
       K_(reserved_ok_len_of_compressed),
       K_(connection_id),
       K_(scramble_buf),
       K_(is_resultset_resp),
       K_(server_capabilities_lower_.capability),
       K_(ok_packet_action_type),
       K_(last_ok_pkt_len),
       K_(rewritten_last_ok_pkt_len),
       K_(extra_info));

  if (is_error_resp()) {
    J_COMMA();
    J_KV(K_(error_pkt));
  }
  J_OBJ_END();
  return pos;
}
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
