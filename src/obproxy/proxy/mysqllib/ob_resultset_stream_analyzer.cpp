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
#include "proxy/mysqllib/ob_resultset_stream_analyzer.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
void ObResultsetStreamAnalyzer::reset()
{
  eof_pkt_count_ = 0;
  next_read_len_ = 0;
  error_pkt_count_= 0;
}

int ObResultsetStreamAnalyzer::analyze_resultset_stream(ObIOBufferReader *reader,
                                                        int64_t &to_consume_size,
                                                        ObResultsetStreamStatus &stream_status)
{
  int ret = OB_SUCCESS;
  int64_t read_avail = 0;
  to_consume_size = 0;
  stream_status = RSS_CONTINUE_STATUS;

  if (OB_ISNULL(reader) || OB_UNLIKELY((read_avail = reader->read_avail()) <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("reader must contain data", K(reader), K(read_avail), K(ret));
  } else if (!is_received_all_eof() && !is_received_error()) {
    bool need_loop = true;
    int64_t min_len = 0;
    uint64_t pkt_len = 0; // not include pkt header len
    uint8_t type = 0;
    while (OB_SUCC(ret) && need_loop && read_avail > 0) {
      if (next_read_len_ > 0) {
        min_len = std::min(read_avail, next_read_len_);
        read_avail -= min_len;
        next_read_len_ -= min_len;
        to_consume_size += min_len;
      }
      if (OB_UNLIKELY(0 != next_read_len_ && 0 != read_avail)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("error, never run here!", K(next_read_len_), K(read_avail), K(ret));
      } else if (read_avail > 0) {
        // if pkt meta has not received completed,
        // break and continue received from net.
        if (read_avail < MYSQL_NET_META_LENGTH) {
          need_loop = false;
        } else if (OB_FAIL(get_mysql_resp_meta(reader, to_consume_size, pkt_len, type))) {
          LOG_WDIAG("fail to get mysql resp meta", K(ret));
        } else if (MYSQL_EOF_PACKET_TYPE == type) {
          ++eof_pkt_count_;
          if (OB_UNLIKELY(eof_pkt_count_ > MYSQL_RESULTSET_EOF_PECKET_COUNT)
              || OB_UNLIKELY(eof_pkt_count_ <= 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("eof_pkt_count is out of range", K(eof_pkt_count_), K(ret));
          } else if (is_received_all_eof()) {
            need_loop = false;
          } else {
            next_read_len_ = pkt_len + MYSQL_NET_HEADER_LENGTH;
          }
        } else if (MYSQL_ERR_PACKET_TYPE == type) {
          ++error_pkt_count_;
          need_loop = false;
        } else if (OB_UNLIKELY(MYSQL_OK_PACKET_TYPE == type && 1 != eof_pkt_count_)) {
          // only type == MYSQL_OK_PACKET_TYPE can not determine it is indeed an
          // ok pakt in ResultSet Protocol;
          // it need both MYSQL_OK_PACKET_TYPE == type and 1 != eof_pkt_count_;
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("error, never run here!", K(type), K(eof_pkt_count_), K(ret));
        } else {
          next_read_len_ = pkt_len + MYSQL_NET_HEADER_LENGTH;
        }
      }
    } // end while
  } // end if (!is_received_all_eof() && !is_received_error())

  // hanldle the last two pkt in resultset protocol, the second eof pkt and ok pkt
  if (OB_SUCC(ret) && is_received_all_eof()) {
    if (OB_FAIL(handle_end_packet(reader, read_avail, MYSQL_EOF_PACKET_TYPE,
                to_consume_size, stream_status))) {
      LOG_WDIAG("fail to handle the last eof pkt  and ok pkt", K(ret));
    }
  }

  // handle the last two pkt in resultset protocol, error pkt and ok pkt
  if (OB_SUCC(ret) && is_received_error()) {
    if (OB_FAIL(handle_end_packet(reader, read_avail, MYSQL_ERR_PACKET_TYPE,
                to_consume_size, stream_status))) {
      LOG_WDIAG("fail to handle the last err pkt and ok pkt", K(ret));
    }
  }

  return ret;
}

inline int ObResultsetStreamAnalyzer::get_mysql_resp_meta(
    ObIOBufferReader *reader,
    const int64_t start_pos,
    uint64_t &pkt_len,
    uint8_t &type)
{
  int ret = OB_SUCCESS;
  pkt_len = 0;
  type = 0;
  if (OB_ISNULL(reader) || OB_UNLIKELY(start_pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(reader), K(start_pos), K(ret));
  } else if (OB_UNLIKELY(start_pos + MYSQL_NET_META_LENGTH > reader->read_avail())) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WDIAG("reader buf is not enough", "buf_len", reader->read_avail(),
             "need_len", start_pos + MYSQL_NET_META_LENGTH, K(ret));
  } else {
    // TODO, later optimization
    char meta_buf[MYSQL_NET_META_LENGTH];
    meta_buf[0] = '\0';
    char *written_pos = reader->copy(meta_buf, MYSQL_NET_META_LENGTH, start_pos);
    if (OB_UNLIKELY(written_pos != meta_buf + MYSQL_NET_META_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("not copy completely", K(written_pos), K(meta_buf),
               "meta_length", MYSQL_NET_META_LENGTH, K(ret));
    } else {
      char *start = meta_buf;
      uint32_t tmp_pkt_len = 0;
      obmysql::ObMySQLUtil::get_uint3(start, tmp_pkt_len);
      pkt_len = tmp_pkt_len;
      type = static_cast<uint8_t>(meta_buf[MYSQL_NET_META_LENGTH - 1]);
    }
  }
  return ret;
}

inline int ObResultsetStreamAnalyzer::handle_end_packet(
                                      ObIOBufferReader *reader,
                                      const int64_t read_avail,
                                      const int64_t expect_type,
                                      int64_t &to_consume_size,
                                      ObResultsetStreamStatus &stream_status)
{
  int ret = OB_SUCCESS;

  if (read_avail > MYSQL_NET_META_LENGTH) {
    uint64_t pkt_len = 0;
    uint8_t pkt_type = 0;
    if (OB_FAIL(get_mysql_resp_meta(reader, to_consume_size, pkt_len, pkt_type))) {
      LOG_WDIAG("fail to get mysql resp meta", K(expect_type), K(ret));
    } else if (OB_UNLIKELY(expect_type != pkt_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected pkt type", K(expect_type), K(pkt_type), K(ret));
    } else if (read_avail > static_cast<int64_t>(pkt_len) + MYSQL_NET_HEADER_LENGTH + MYSQL_NET_META_LENGTH) {
      //get ok pkt meta
      uint64_t ok_pkt_len = 0;
      uint8_t ok_type = 0;
      int64_t start_pos = to_consume_size + pkt_len + MYSQL_NET_HEADER_LENGTH;
      if (OB_FAIL(get_mysql_resp_meta(reader, start_pos, ok_pkt_len, ok_type))) {
        LOG_WDIAG("fail to get mysql resp meta", K(expect_type), K(ret));
      } else if (OB_UNLIKELY(MYSQL_OK_PACKET_TYPE != ok_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected pkt type, expect ok pkt", K(ok_type), K(ret));
      } else {
        int64_t total_len = pkt_len + MYSQL_NET_HEADER_LENGTH + ok_pkt_len + MYSQL_NET_HEADER_LENGTH;
        if (OB_UNLIKELY(read_avail > total_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("invalid packet", K(read_avail), K(ret));
        } else if (read_avail == total_len) {
          to_consume_size += pkt_len + MYSQL_NET_HEADER_LENGTH;
          stream_status = RSS_END_NORMAL_STATUS;
        }
      }
    }
  }

  return ret;
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
