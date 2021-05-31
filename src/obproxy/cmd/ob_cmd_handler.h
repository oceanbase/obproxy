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

#ifndef OBPROXY_CMD_HANDLER_H
#define OBPROXY_CMD_HANDLER_H

#include "packet/ob_mysql_packet_util.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "common/obsm_row.h"

namespace oceanbase
{
namespace obproxy
{

#define DEBUG_CMD(fmt...) PROXY_CMD_LOG(DEBUG, ##fmt)
#define INFO_CMD(fmt...) PROXY_CMD_LOG(INFO, ##fmt)
#define WARN_CMD(fmt...) PROXY_CMD_LOG(WARN, ##fmt)
#define ERROR_CMD(fmt...) PROXY_CMD_LOG(ERROR, ##fmt)

class ObCmdHandler
{
public:
  ObCmdHandler(event::ObMIOBuffer *buf, uint8_t pkg_seq, int64_t memory_limit);
  virtual ~ObCmdHandler();

  int init(const bool is_query_cmd = true);
  int reset();//clean buf and reset seq
  int fill_external_buf();
  bool is_inited() const { return is_inited_; };
  bool is_buf_empty() const { return original_seq_ == seq_; }

protected:
  int encode_header(const common::ObString *cname, const obmysql::EMySQLFieldType *ctype,
          const int64_t size);
  int encode_row_packet(const common::ObNewRow &row, const bool need_limit_size = true);
  int encode_ok_packet(const int64_t affected_rows, const obmysql::ObMySQLCapabilityFlags &capability);
  int encode_eof_packet();
  int encode_err_packet(const int errcode);

  template<typename T>
  int encode_err_packet(const int errcode, const T &param)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      WARN_CMD("it has not inited", K(ret));
    } else if (OB_FAIL(reset())) { //before encode_err_packet, we need clean buf
      WARN_CMD("fail to do reset", K(errcode), K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet(*internal_buf_, seq_, errcode, param))) {
      WARN_CMD("fail to encode err packet", K(errcode), K(ret));
    } else {
      INFO_CMD("succ to encode err packet", K(errcode));
    }
    return ret;
  }

protected:
  event::ObMIOBuffer *external_buf_;
  event::ObMIOBuffer *internal_buf_;
  event::ObIOBufferReader *internal_reader_;
  int64_t internal_buf_limited_;

  bool is_inited_;
  bool header_encoded_;
  uint8_t seq_;
  const uint8_t original_seq_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCmdHandler);
};

} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_CMD_HANDLER_H
