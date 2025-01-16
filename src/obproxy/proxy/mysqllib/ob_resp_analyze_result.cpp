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
  J_KV(K(transmit_control_.is_decompressed_),
       K(transmit_control_.is_trans_completed_),
       K(transmit_control_.is_resp_completed_),
       K(transmit_control_.reserved_ok_len_of_mysql_),
       K(transmit_control_.reserved_ok_len_of_compressed_),
       K(transmit_control_.is_last_ok_handled_),
       K(transmit_control_.last_ok_pkt_len_),
       K(transmit_control_.rewritten_last_ok_pkt_len_),
       K(transmit_control_.ok_packet_action_type_),
       K(format_.is_auth_switch_req_),
       K(format_.is_resultset_resp_),
       K(format_.is_server_db_reset_),
       K(format_.ending_type_),
       K(sysvar_.is_partition_hit_),
       K(sysvar_.is_last_insert_id_changed_),
       K(sysvar_.has_new_sys_var_),
       K(sysvar_.has_proxy_idc_name_user_var_),
       K(sysvar_.weak_read_hit_replica_),
       K(sysvar_.server_trace_id_),
       K(handshake_.connection_id_),
       K(handshake_.server_capabilities_lower_.capability_),
       K(handshake_.server_capabilities_upper_.capability_),
       K(ob20_.is_server_trans_internal_routing_),
       K(ob20_.extra_info_),
       K(ob20_.flt_));

  if (is_error_resp()) {
    J_COMMA();
    J_KV(K(error_.error_pkt_));
  }
  J_OBJ_END();
  return pos;
}
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
