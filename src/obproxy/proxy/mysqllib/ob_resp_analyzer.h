/**
 * Copyright (c) 2024 OceanBase
 * OceanBase Database Proxy(ODP) is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OBPROXY_OB_REPS_ANALYZER_H
#define OBPROXY_OB_REPS_ANALYZER_H
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "proxy/mysqllib/ob_resp_analyze_result.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "utils/ob_zlib_stream_compressor.h"
#include "obproxy/iocore/eventsystem/ob_io_buffer.h"

#include "proxy/mysqllib/ob_oceanbase_20_pkt_analyzer.h"
#include "proxy/mysqllib/ob_compressed_pkt_analyzer.h"
#include "proxy/mysqllib/ob_mysql_pkt_analyzer.h"

#include "proxy/mysqllib/ob_resp_packet_analyze_result.h"
#include "proxy/mysqllib/ob_2_0_protocol_struct.h"
#include "proxy/mysqllib/ob_compression_algorithm.h"
#include "proxy/mysqllib/ob_protocol_diagnosis.h"
#include "obproxy/obutils/ob_proxy_buf.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
static const int64_t OK_PKT_MAX_LEN = 20;
static const int64_t RESP_PKT_MEMORY_BUFFER_SIZE = 32;
static const int64_t DEFAULT_PKT_BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K);

class ObCompressedPktAnalyzer;
class ObRespAnalyzer
{
public:
  ObRespAnalyzer()
    : protocol_(ObProxyProtocol::PROTOCOL_OB20),
      analyze_mode_(ObRespAnalyzeMode::SIMPLE_MODE),
      protocol_mode_(ObMysqlProtocolMode::OCEANBASE_MYSQL_PROTOCOL_MODE),
      req_cmd_(obmysql::OB_MYSQL_COM_SLEEP), last_ob_seq_(0),
      request_id_(0), sess_id_(0), is_inited_(false),
      mysql_pkt_buf_(NULL),
      is_mysql_stream_end_(false),
      is_oceanbase_stream_end_(false),
      is_compressed_stream_end_(false),
      last_mysql_pkt_seq_(0), is_in_multi_pkt_(false),
      ending_type_(ObMysqlRespEndingType::MAX_PACKET_ENDING_TYPE),
      reserved_len_(0), cur_stmt_has_more_result_(false),
      stream_ob20_state_(STREAM_INVALID),
      stream_compressed_state_(STREAM_INVALID),
      stream_mysql_state_(STREAM_INVALID),
      protocol_diagnosis_(NULL) {}
  ~ObRespAnalyzer()
  {
    DEC_SHARED_REF(protocol_diagnosis_);
    dealloc_mysql_pkt_buf();
  }
  // init for compressed mysql and oceanbase 2.0
  int init(ObProxyProtocol protocol,
           obmysql::ObMySQLCmd req_cmd,
           ObMysqlProtocolMode protocol_mode,
           ObRespAnalyzeMode analyze_mode,
           bool is_extra_ok_for_stats,
           bool is_compressed,
           uint8_t last_ob_req,
           uint8_t last_compressed_seq,
           uint32_t request_id,
           uint32_t sess_id,
           bool enable_trans_checksum);
  // init for mysql
  int init(obmysql::ObMySQLCmd req_cmd,
           ObMysqlProtocolMode protocol_mode,
           bool is_extra_ok_for_stats,
           bool is_in_trans,
           bool is_autocommit,
           bool is_binlog_req);
  // analyze one packet first then decide to analyze the rest of packets or not
  int analyze_response(event::ObIOBufferReader &reader, const bool need_receive_completed,
                       ObAnalyzeHeaderResult &result, ObRespAnalyzeResult &resp_result);
  // directly analyze all pkts
  int analyze_response(event::ObIOBufferReader &reader, ObRespAnalyzeResult &resp_result);
  int analyze_one_packet(event::ObIOBufferReader &reader, ObAnalyzeHeaderResult &result);
  OB_INLINE void reset();
  OB_INLINE void reset_for_mysql_tunnel();
public: // getter and setter
  OB_INLINE bool is_inited() const { return is_inited_; }
  OB_INLINE bool is_stream_end();
  OB_INLINE event::ObIOBufferReader* alloc_mysql_pkt_reader() { return mysql_pkt_buf_ == NULL ? NULL : mysql_pkt_buf_->alloc_reader(); }
  OB_INLINE ObProtocolDiagnosis *&get_protocol_diagnosis_ref() { return protocol_diagnosis_; }
  OB_INLINE ObProtocolDiagnosis *get_protocol_diagnosis() { return protocol_diagnosis_; }
  OB_INLINE const ObProtocolDiagnosis *get_protocol_diagnosis() const { return protocol_diagnosis_; }
private:
  int analyze_response_with_length(event::ObIOBufferReader &reader, uint64_t length);
  inline int stream_analyze_packets(const ObString buf, ObRespAnalyzeResult &resp_result);
  int stream_analyze_compressed_oceanbase(const ObString buf, ObRespAnalyzeResult &resp_result);
  int stream_analyze_compressed_mysql(const ObString buf, ObRespAnalyzeResult &resp_result);
  int stream_analyze_oceanbase(const ObString buf, ObRespAnalyzeResult &resp_result);
  int stream_analyze_mysql(const ObString buf, ObRespAnalyzeResult &resp_result);
private:
  // oceanbase 2.0
  int handle_analyze_compressed_oceanbase_payload(const char *buf, const int64_t len, ObRespAnalyzeResult &resp_result);
  int handle_analyze_oceanbase_compressed_hdr(const char *buf, const int64_t analyzed);
  int handle_analyze_ob20_header(ObRespAnalyzeResult &resp_result);
  void handle_analyze_ob20_tailer(ObRespAnalyzeResult &resp_result);
  inline void handle_analyze_ob20_extra_info_header(ObRespAnalyzeResult &resp_result);
  int handle_analyze_ob20_extra_info(const char *buf, const int64_t len, ObRespAnalyzeResult &resp_result);
  int handle_analyze_mysql_payload(const char *buf, const int64_t len, ObRespAnalyzeResult &resp_result);
  // mysql
  int handle_not_analyze_mysql_pkt_type();
  int handle_analyze_mysql_pkt_type(const char *buf);
  int handle_analyze_mysql_end(const char *pkt_end,  ObRespAnalyzeResult *resp_result);
  int handle_analyze_mysql_body(const char *buf, const int64_t len);
  int handle_analyze_last_mysql(ObRespAnalyzeResult &resp_result);
  void handle_analyze_mysql_header(const int64_t analyzed);
  // compressed
  void handle_analyze_compressed_mysql_header();
  int handle_analyze_compressed_mysql_payload(const char *buf, const int64_t len, ObRespAnalyzeResult &resp_result);
  int handle_analyze_compressed_payload(const char *buf, const int64_t len, ObRespAnalyzeResult &resp_result);
private:
  int analyze_all_packets(event::ObIOBufferReader &reader, ObAnalyzeHeaderResult &result, ObRespAnalyzeResult &resp_result);
  int analyze_one_packet_header(event::ObIOBufferReader &reader, ObAnalyzeHeaderResult &result, ObRespAnalyzeResult &resp_result);
  int analyze_one_packet_header(event::ObIOBufferReader &reader, ObAnalyzeHeaderResult &result);
  int analyze_eof_pkt(const char *buf, bool &is_in_trans, bool &is_last_eof_pkt);
  int analyze_ok_pkt(const char *buf, bool &is_in_trans);
  int analyze_hanshake_pkt(ObRespAnalyzeResult *resp_result);
  int analyze_error_pkt(ObRespAnalyzeResult *resp_result);
  int build_packet_content(obutils::ObVariableLenBuffer<FIXED_MEMORY_BUFFER_SIZE> &content_buf);
  int analyze_prepare_ok_pkt();
  int update_ending_type();
  void handle_last_eof(const char *pkt_end, uint32_t pkt_len);
  bool is_last_pkt(const ObAnalyzeHeaderResult &result);
  OB_INLINE bool need_copy_ok_pkt();
  OB_INLINE bool need_reserve_pkt();
  OB_INLINE bool need_analyze_mysql_pkt_type();
  OB_INLINE bool is_oceanbase_mode() const { return OCEANBASE_MYSQL_PROTOCOL_MODE == protocol_mode_ || OCEANBASE_ORACLE_PROTOCOL_MODE == protocol_mode_; }
  OB_INLINE bool is_mysql_mode() const { return STANDARD_MYSQL_PROTOCOL_MODE == protocol_mode_; }
  OB_INLINE bool is_last_pkt();
  OB_INLINE bool is_last_mysql_pkt();
  OB_INLINE bool is_last_oceanbase_pkt();
  OB_INLINE bool is_last_compressed_pkt();
  OB_INLINE bool is_decompress_mode() { return DECOMPRESS_MODE == analyze_mode_; }
  OB_INLINE bool need_analyze_all_packets(bool need_receive_completed, ObRespAnalyzeResult &resp_result);
private:
  int alloc_mysql_pkt_buf();
  inline void dealloc_mysql_pkt_buf();
private:
  ObProxyProtocol protocol_;
  ObRespAnalyzeMode analyze_mode_;
  ObMysqlProtocolMode protocol_mode_;
  obmysql::ObMySQLCmd req_cmd_;
  // compressed_seq will be ignored in oceanbase v2 protocol
  union {
    uint8_t last_ob_seq_;
    uint8_t last_compressed_seq_;
  };
  uint32_t request_id_;
  uint32_t sess_id_;
  struct {
    bool is_compressed_;
    bool is_extra_ok_for_stats_;
    bool is_binlog_req_;
    bool is_in_trans_;
    bool is_autocommit_;
  } params_;
  bool is_inited_;
  event::ObMIOBuffer *mysql_pkt_buf_;
  bool is_mysql_stream_end_; // mark mysql packets stream end
  bool is_oceanbase_stream_end_; // mark oceanbase packets stream end (crc tailer included)
  bool is_compressed_stream_end_; // mark compressed packets stream end (compressed tailer included)
  ObRespPacketAnalyzeResult mysql_resp_result_;
  uint8_t last_mysql_pkt_seq_;
  // mysql packet
  bool is_in_multi_pkt_;
  ObMysqlRespEndingType ending_type_;
  obutils::ObVariableLenBuffer<FIXED_MEMORY_BUFFER_SIZE> reserve_pkt_body_buf_;
  int64_t reserved_len_;
  bool cur_stmt_has_more_result_;
  ObRespPktAnalyzerState stream_ob20_state_;
  ObRespPktAnalyzerState stream_compressed_state_;
  ObRespPktAnalyzerState stream_mysql_state_;
  ObMysqlPktAnalyzer mysql_analyzer_;
  ObCompressedPktAnalyzer compressed_analyzer_;
  ObOceanBase20PktAnalyzer ob20_analyzer_;
  ObZlibStreamCompressor compressor_;
  ObProtocolDiagnosis *protocol_diagnosis_;
};

bool ObRespAnalyzer::is_stream_end()
{
  bool ret = false;
  if (OB_LIKELY(ObProxyProtocol::PROTOCOL_OB20 == protocol_ || ObProxyProtocol::PROTOCOL_CHECKSUM == protocol_)) {
    ret = is_compressed_stream_end_;
  } else if (ObProxyProtocol::PROTOCOL_NORMAL == protocol_) {
    ret = is_mysql_stream_end_;
  }
  return ret;
}
int ObRespAnalyzer::stream_analyze_packets(ObString data, ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(ObProxyProtocol::PROTOCOL_OB20 == protocol_)) {
    ret = stream_analyze_compressed_oceanbase(data, resp_result);
  } else if (ObProxyProtocol::PROTOCOL_CHECKSUM == protocol_) {
    ret = stream_analyze_compressed_mysql(data, resp_result);
  } else if (ObProxyProtocol::PROTOCOL_NORMAL == protocol_) {
    ret = stream_analyze_mysql(data, resp_result);
    resp_result.reserved_ok_len_of_mysql_ = reserved_len_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WDIAG, "unexpect analyze protocol", K(protocol_), K(ret));
  }

  return ret;
}
bool ObRespAnalyzer::need_reserve_pkt()
{
  return (OK_PACKET_ENDING_TYPE == ending_type_
          || (EOF_PACKET_ENDING_TYPE == ending_type_
              && (1 == mysql_resp_result_.get_pkt_cnt(EOF_PACKET_ENDING_TYPE)))
          || ERROR_PACKET_ENDING_TYPE == ending_type_);
}
bool ObRespAnalyzer::need_copy_ok_pkt()
{
  return (OK_PACKET_ENDING_TYPE == ending_type_
          // no need copy the second eof packet, only analyze the first eof packet
          || (EOF_PACKET_ENDING_TYPE == ending_type_ && (0 == mysql_resp_result_.get_pkt_cnt(EOF_PACKET_ENDING_TYPE)))
          || ERROR_PACKET_ENDING_TYPE == ending_type_
          || (HANDSHAKE_PACKET_ENDING_TYPE == ending_type_ && OB_MYSQL_COM_HANDSHAKE == req_cmd_)
          || PREPARE_OK_PACKET_ENDING_TYPE == ending_type_);
}

bool ObRespAnalyzer::is_last_compressed_pkt()
{
  return compressed_analyzer_.get_header().seq_ == last_compressed_seq_;
}
bool ObRespAnalyzer::is_last_oceanbase_pkt()
{
  return ob20_analyzer_.get_header().flag_.is_last_packet();
}
bool ObRespAnalyzer::is_last_mysql_pkt()
{
  return mysql_analyzer_.get_body_to_read() == 0;
}
bool ObRespAnalyzer::is_last_pkt()
{
  bool ret = false;
  if (OB_LIKELY(ObProxyProtocol::PROTOCOL_OB20 == protocol_)) {
    ret = is_last_oceanbase_pkt();
  } else if (ObProxyProtocol::PROTOCOL_CHECKSUM == protocol_) {
    ret = is_last_compressed_pkt();
  } else if (ObProxyProtocol::PROTOCOL_NORMAL == protocol_) {
    ret = is_last_mysql_pkt();
  }
  return ret;
}
bool ObRespAnalyzer::need_analyze_mysql_pkt_type()
{
  return OB_LIKELY(!is_in_multi_pkt_) && OB_LIKELY(OB_MYSQL_COM_STATISTICS != req_cmd_) && OB_LIKELY(mysql_analyzer_.get_pkt_len() > 0);
}

bool ObRespAnalyzer::need_analyze_all_packets(bool need_receive_completed, ObRespAnalyzeResult &resp_result)
{
  bool ret = false;
  if (need_receive_completed) {
    ret = true;
  } else if (OB_LIKELY(ObProxyProtocol::PROTOCOL_NORMAL != protocol_)) {
    ret = is_decompress_mode();
  } else {
    ret = !resp_result.is_resultset_resp() && OB_MYSQL_COM_BINLOG_DUMP != req_cmd_ && OB_MYSQL_COM_BINLOG_DUMP_GTID != req_cmd_;
  }
  return ret;
}
void ObRespAnalyzer::reset()
{
  compressor_.reset();
  ob20_analyzer_.reset();
  reserve_pkt_body_buf_.reset();
  mysql_resp_result_.reset();
  compressed_analyzer_.reset();
  mysql_analyzer_.reset();
  reserved_len_ = 0;
  stream_ob20_state_ = STREAM_INVALID;
  stream_compressed_state_ = STREAM_INVALID;
  stream_mysql_state_ = STREAM_INVALID;
  dealloc_mysql_pkt_buf();

  protocol_ = ObProxyProtocol::PROTOCOL_NORMAL;
  protocol_mode_ = ObMysqlProtocolMode::OCEANBASE_MYSQL_PROTOCOL_MODE;
  analyze_mode_ = ObRespAnalyzeMode::SIMPLE_MODE;
  req_cmd_ = OB_MYSQL_COM_SLEEP;
  ending_type_ = ObMysqlRespEndingType::MAX_PACKET_ENDING_TYPE;

  request_id_ = 0;
  sess_id_ = 0;
  last_mysql_pkt_seq_ = 0;
  last_ob_seq_ = 0;

  params_.is_extra_ok_for_stats_ = false;
  params_.is_compressed_ = false;
  params_.is_binlog_req_ = false;
  params_.is_in_trans_ = false;
  params_.is_autocommit_ = false;

  is_inited_ = false;
  is_mysql_stream_end_ = false;
  is_compressed_stream_end_ = false;
  is_oceanbase_stream_end_ = false;
  is_in_multi_pkt_ = false;
  cur_stmt_has_more_result_ = false;
}
void ObRespAnalyzer::reset_for_mysql_tunnel()
{
  reserve_pkt_body_buf_.reset();
  mysql_resp_result_.reset();
  mysql_analyzer_.reset();
  dealloc_mysql_pkt_buf();
  stream_mysql_state_ = STREAM_MYSQL_HEADER;
  is_in_multi_pkt_ = false;
  is_mysql_stream_end_ = false;

}
void ObRespAnalyzer::dealloc_mysql_pkt_buf()
{
  if (NULL != mysql_pkt_buf_) {
    free_miobuffer(mysql_pkt_buf_);
    mysql_pkt_buf_ = NULL;
  }
}

void ObRespAnalyzer::handle_analyze_ob20_extra_info_header(ObRespAnalyzeResult &resp_result)
{
  if (STREAM_OCEANBASE20_EXTRA_INFO == stream_ob20_state_) {
    resp_result.extra_info_.extra_info_buf_.reset();
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif