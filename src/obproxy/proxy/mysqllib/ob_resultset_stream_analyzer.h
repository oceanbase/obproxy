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

#ifndef OBPROXY_RESULTSET_STREAM_ANALYZER_H
#define OBPROXY_RESULTSET_STREAM_ANALYZER_H
#include "lib/ob_define.h"
#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
class ObIOBufferReader;
}

namespace proxy
{
enum ObResultsetStreamStatus
{
  RSS_CONTINUE_STATUS = 0,
  RSS_END_NORMAL_STATUS,
  RSS_END_ERROR_STATUS,
};

class ObResultsetStreamAnalyzer
{
public:
  ObResultsetStreamAnalyzer() { reset(); }
  ~ObResultsetStreamAnalyzer() { reset(); }
  void reset();

  int analyze_resultset_stream(event::ObIOBufferReader *reader,
                               int64_t &to_consume_size,
                               ObResultsetStreamStatus &stream_status);

private:
  // @param start_pos, the pos of pkt header in reader
  // @param pkt_len,
  int get_mysql_resp_meta(event::ObIOBufferReader *reader, const int64_t start_pos,
                          uint64_t &pkt_len, uint8_t &type);

  // handle the last two pkt in resultset protocol, eof + ok or err + ok
  int handle_end_packet(event::ObIOBufferReader *reader, const int64_t read_avail, const int64_t expect_type,
                        int64_t &to_consume_size, ObResultsetStreamStatus &stream_status);

  bool is_received_all_eof() { return MYSQL_RESULTSET_EOF_PECKET_COUNT == eof_pkt_count_; }
  bool is_received_error() { return MYSQL_RESULTSET_ERROR_PECKET_COUNT == error_pkt_count_; }

private:
  static const int64_t MYSQL_RESULTSET_EOF_PECKET_COUNT = 2;
  static const int64_t MYSQL_RESULTSET_ERROR_PECKET_COUNT = 1;

  int64_t eof_pkt_count_;
  int64_t error_pkt_count_;
  int64_t next_read_len_;
  DISALLOW_COPY_AND_ASSIGN(ObResultsetStreamAnalyzer);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_RESULTSET_STREAM_ANALYZER_H */
