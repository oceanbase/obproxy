/**
 * Copyright (c) 2022 OceanBase
 * OceanBase Database Proxy(ODP) is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OB_PROXY_PACKET_WRITER_H
#define OB_PROXY_PACKET_WRITER_H

#include "obproxy/proxy/mysqllib/ob_proxy_session_info.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObMysqlClientSession;
} // end of proxy

namespace packet
{
#define MAX_MSG_BUFFER_SIZE (256)

class ObProxyPacketWriter {
public:
  // packet
  static int write_packet(event::ObMIOBuffer &write_buf,
                          proxy::ObMysqlClientSession &client_session,
                          obmysql::ObMySQLPacket &packet);
  static int write_raw_packet(event::ObMIOBuffer &write_buf, proxy::ObMysqlClientSession &client_session,
                              ObString &pkt_str);
  static int write_kv_resultset(event::ObMIOBuffer &write_buf, proxy::ObMysqlClientSession &client_session,
                                uint8_t &seq, const obmysql::ObMySQLField &field, ObObj &field_value,
                                const uint16_t status_flag);
  static int write_ok_packet(event::ObMIOBuffer &write_buf, proxy::ObMysqlClientSession &client_session, uint8_t &seq,
                             const int64_t affected_rows, const obmysql::ObMySQLCapabilityFlags &capability);
  static int write_executor_resp_packet(event::ObMIOBuffer *write_buf, proxy::ObMysqlClientSession *client_session,
                                        uint8_t &seq, engine::ObProxyResultResp *result_resp);
  
  // get thread err buffer
  static int get_err_buf(int err_code, char *&buf);

  template<typename T>
  static int get_user_err_buf(int err_code, char *&buf, T &param)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(err_code == common::OB_SUCCESS)) {
      err_code = common::OB_ERR_UNEXPECTED;
    }

    static __thread char msg_buffer_[MAX_MSG_BUFFER_SIZE];
    MEMSET(msg_buffer_, 0x0, sizeof(msg_buffer_));
    const char *err_msg = common::ob_str_user_error(err_code);
    int32_t length = 0;
    if (OB_ISNULL(err_msg)) {
      length = snprintf(msg_buffer_, sizeof(msg_buffer_), "Unknown user error, err=%d", err_code);
    } else {
      length = snprintf(msg_buffer_, sizeof(msg_buffer_), err_msg, param);
    }
    
    if (OB_UNLIKELY(length < 0) || OB_UNLIKELY(length > MAX_MSG_BUFFER_SIZE)) {
      ret = common::OB_BUF_NOT_ENOUGH;
    } else {
      buf = msg_buffer_;
    }
    return ret;
  }
  
  static int write_error_packet(event::ObMIOBuffer &write_buf, proxy::ObMysqlClientSession *client_session,
                                uint8_t &seq, const int err_code, const char *msg_buf);
  static int write_error_packet(event::ObMIOBuffer &write_buf, proxy::ObMysqlClientSession *client_session,
                                uint8_t &seq, const int err_code, const ObString &msg_buf);

};


} // end of packet
} // end of obproxy
} // end of oceanbase

#endif  // end OB_PROXY_PACKET_WRITER_H
