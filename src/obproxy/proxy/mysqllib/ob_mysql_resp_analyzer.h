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

#ifndef OBPROXY_MYSQL_RESP_ANALYZER_H
#define OBPROXY_MYSQL_RESP_ANALYZER_H

#include "rpc/obmysql/ob_mysql_packet.h"
#include "obutils/ob_proxy_buf.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
typedef common::ObString ObRespBuffer;
static const int64_t FIXED_MEMORY_BUFFER_SIZE = 32;

enum ObMysqlProtocolMode
{
  UNDEFINED_MYSQL_PROTOCOL_MODE = 0,
  STANDARD_MYSQL_PROTOCOL_MODE,
  OCEANBASE_MYSQL_PROTOCOL_MODE,
};

enum ObMysqlResponseAnalyzerState
{
  READ_HEADER,
  READ_TYPE,
  READ_BODY,
};

class ObBufferReader
{
public:
  explicit ObBufferReader(const ObRespBuffer resp_buf) :  pos_(0), resp_buf_(resp_buf) {}
  ~ObBufferReader() {}
  void reset()
  {
    pos_ = 0;
    resp_buf_.reset();
  }

  bool empty() const { return (resp_buf_.length() <= pos_); }
  int64_t get_remain_len() const { return resp_buf_.length() - pos_; }
  char *get_ptr() { return (resp_buf_.ptr() + pos_); }

  // if buf is NULL, will not copy and just add pos_
  int read(char *buf, const int64_t len);

  int64_t pos_; //point to next char will be read
  ObRespBuffer resp_buf_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBufferReader);
};

// this class is binded to Server Command. when new Server
// Command arrive, this class should be reset.
class ObRespResult
{
public:
  ObRespResult();
  ~ObRespResult() {}
  void reset();
  int is_resp_finished(bool &finished, ObMysqlRespEndingType &ending_type) const;

  void set_cmd(const obmysql::ObMySQLCmd cmd) { cmd_ = cmd; }
  obmysql::ObMySQLCmd get_cmd() const { return cmd_; }

  int64_t get_pkt_cnt(const ObMysqlRespEndingType type) const { return pkt_cnt_[type]; }
  void inc_pkt_cnt(const ObMysqlRespEndingType type) { ++pkt_cnt_[type]; }
  void reset_pkt_cnt() { memset(pkt_cnt_, 0, sizeof(pkt_cnt_)); }

  int32_t get_all_pkt_cnt() const { return all_pkt_cnt_; }
  void inc_all_pkt_cnt() { ++all_pkt_cnt_; }

  int32_t get_expect_pkt_cnt() const { return expect_pkt_cnt_; }
  void set_expect_pkt_cnt(const int32_t expect_pkt_cnt) { expect_pkt_cnt_ = expect_pkt_cnt; }

  bool is_recv_resultset() const { return is_recv_resultset_; }
  void set_recv_resultset(const bool is_recv_resultset) { is_recv_resultset_ = is_recv_resultset; }

  ObMysqlRespType get_resp_type() const { return resp_type_; }
  void set_resp_type(const ObMysqlRespType type) { resp_type_ = type; }

  int64_t get_reserved_len() const { return reserved_len_; }
  void set_reserved_len(const int64_t reserved_len) { reserved_len_ = reserved_len; }

  ObRespTransState get_trans_state() const { return trans_state_; }
  void set_trans_state(const ObRespTransState state) { trans_state_ = state; }

  ObMysqlProtocolMode get_mysql_mode() const { return mysql_mode_; }
  void set_mysql_mode(const ObMysqlProtocolMode mode) { mysql_mode_ = mode; }
  bool is_oceanbase_mode() const { return OCEANBASE_MYSQL_PROTOCOL_MODE == mysql_mode_; }
  bool is_mysql_mode() const { return STANDARD_MYSQL_PROTOCOL_MODE == mysql_mode_; }

  void set_enable_extra_ok_packet_for_stats(const bool enable_extra_ok_packet_for_stats) {
    enable_extra_ok_packet_for_stats_ = enable_extra_ok_packet_for_stats;
  }
  bool is_extra_ok_packet_for_stats_enabled() const {
    return enable_extra_ok_packet_for_stats_;
  }
private:
  bool is_compress_; //use for compress Protocol, not support now
  bool enable_extra_ok_packet_for_stats_;
  obmysql::ObMySQLCmd cmd_;
  ObMysqlProtocolMode mysql_mode_;
  ObMysqlRespType resp_type_;
  ObRespTransState trans_state_;
  int64_t reserved_len_;
  int32_t all_pkt_cnt_;
  int32_t expect_pkt_cnt_;
  bool is_recv_resultset_;
  int8_t pkt_cnt_[OB_MYSQL_RESP_ENDING_TYPE_COUNT];
  DISALLOW_COPY_AND_ASSIGN(ObRespResult);
};

// Mysql Response Packet struct:
//
// |<--------- Packet Header-------->|<----------Packet Body(may be empty)------------->|
// |_________________________________|__________________________________________________|
// |packet len(3Byte)| seq num(1Byte)| packet_type(1Byte)|      others paylaod          |
// |_________________|_______________|___________________|______________________________|
//
// cache incomplete mysql packet header
// ObRespBuffer may divide mysql packet header(4 Byte) into muti parts,
// and we should splice those parts into a complete packet mysql header.
class ObMysqlPacketMetaAnalyzer
{
public:
  ObMysqlPacketMetaAnalyzer() : cur_type_(MAX_PACKET_ENDING_TYPE) {}
  ~ObMysqlPacketMetaAnalyzer() {}

  void reset()
  {
    meta_.reset();
    cur_type_ = MAX_PACKET_ENDING_TYPE;
    meta_buf_.reset();
  }

  ObMysqlPacketMeta &get_meta() { return meta_; }
  ObMysqlRespEndingType get_cur_type() const { return cur_type_; }
  void set_cur_type(ObMysqlRespEndingType type) { cur_type_ = type; }

  bool is_need_copy(ObRespResult &result) const;
  bool is_need_reserve_packet(ObRespResult &result) const;
  int update_cur_type(ObRespResult &result);

  bool is_full_filled() const { return meta_buf_.is_full_filled(); }
  bool empty() const { return (0 == meta_buf_.len()); }
  char *get_insert_pos() { return meta_buf_.pos(); }
  int64_t get_remain_len() const { return meta_buf_.remain(); }
  const char *get_ptr() const { return meta_buf_.ptr(); }
  int64_t get_valid_len() const { return meta_buf_.len(); }
  int add_valid_len(const int64_t inc_len) { return meta_buf_.consume(inc_len); }

private:
  ObMysqlPacketMeta meta_;
  ObMysqlRespEndingType cur_type_;
  obutils::ObFixedLenBuffer<MYSQL_NET_HEADER_LENGTH> meta_buf_;
  DISALLOW_COPY_AND_ASSIGN(ObMysqlPacketMetaAnalyzer);
};

class ObMysqlResp;

// When proxy send one client's sql statement(e.g. select, insert, set @@xxx)
// to observer, proxy will forward observer's response packets back to user
// client. This class is used to determine whether proxy has receive all of the
// response packets.
//
// TIPS: In Text Protocol, only one type of request(OB_MYSQL_COM_QUIT) has no request packet,
//       all others have one or many response packets.
//       In Binary Protocal, only two types of request (OB_MYSQL_COM_STMT_SEND_LONG_DATA,
//       OB_MYSQL_COM_STMT_CLOSE) have no response, others also have one or many response
//       packets.
//
// 1. Prepare Statement Protocol is not supported project phase I;
// 2. SSL transport is not supported;
// 3. Compress Protocol in not supported now, and will be supported
//    later, in coordination with observer, TODO;
// 4. In other words, only Text Protocol is supported now;
//
// This class is bind to a response packet, every time when a
// new response packet need to be analyzed, this class should reset;
//
class ObMysqlRespAnalyzer
{
public:
  ObMysqlRespAnalyzer()  { reset(); }
  ~ObMysqlRespAnalyzer() { reset(); }
  void reset();

  // analyze byte flow, and determine whether response is over
  int analyze_mysql_resp(ObBufferReader &buf_reader, ObRespResult &result, ObMysqlResp *resp);

  ObMysqlProtocolMode get_mysql_mode() const { return mysql_mode_; }
  void set_mysql_mode(const ObMysqlProtocolMode mode) { mysql_mode_ = mode; }
  bool is_oceanbase_mode() const { return OCEANBASE_MYSQL_PROTOCOL_MODE == mysql_mode_; }
  bool is_mysql_mode() const { return STANDARD_MYSQL_PROTOCOL_MODE == mysql_mode_; }
  bool need_wait_more_data() const { return (next_read_len_ > 0); }

private:
  int analyze_prepare_ok_pkt(ObRespResult &result);
  int analyze_ok_pkt(bool &is_in_trans);
  int analyze_eof_pkt(bool &is_in_trans);
  int analyze_error_pkt(ObMysqlResp *resp);
  int analyze_hanshake_pkt(ObMysqlResp *resp);//extract connection id

  int read_pkt_hdr(ObBufferReader &buf_reader);
  int read_pkt_type(ObBufferReader &buf_reader, ObRespResult &result);
  int read_pkt_body(ObBufferReader &buf_reader, ObRespResult &result);
  int analyze_resp_pkt(ObRespResult &result, ObMysqlResp *resp);

  int build_packet_content(obutils::ObVariableLenBuffer<FIXED_MEMORY_BUFFER_SIZE> &content_buf);

private:
  static const int64_t OK_PACKET_MAX_COPY_LEN = 20; // 9 + 9 + 2;
  bool is_in_multi_pkt_;
  bool cur_stmt_has_more_result_;
  ObMysqlResponseAnalyzerState state_;
  ObMysqlProtocolMode mysql_mode_;
  int64_t next_read_len_;
  int64_t reserved_len_;
  ObMysqlPacketMetaAnalyzer meta_analyzer_;
  // packet body(except packet type Byte above), when packet type is OK or
  // EOF, we should copy packet body data.
  obutils::ObVariableLenBuffer<FIXED_MEMORY_BUFFER_SIZE> body_buf_;
  DISALLOW_COPY_AND_ASSIGN(ObMysqlRespAnalyzer);
}; // end of class ObMysqlRespAnalyzer

inline void ObRespResult::reset()
{
  is_compress_ = false;
  enable_extra_ok_packet_for_stats_ = false;
  cmd_ = obmysql::OB_MYSQL_COM_END;
  mysql_mode_ = UNDEFINED_MYSQL_PROTOCOL_MODE;
  resp_type_ = MAX_RESP_TYPE;
  trans_state_ = IN_TRANS_STATE_BY_DEFAULT;
  reserved_len_ = 0;
  all_pkt_cnt_ = 0;
  expect_pkt_cnt_ = 0;
  is_recv_resultset_ = false;
  memset(pkt_cnt_, 0, sizeof(pkt_cnt_));
}

inline void ObMysqlRespAnalyzer::reset()
{
  is_in_multi_pkt_ = false;
  cur_stmt_has_more_result_ = false;
  state_ = READ_HEADER;
  mysql_mode_ = UNDEFINED_MYSQL_PROTOCOL_MODE;
  next_read_len_ = 0;
  reserved_len_ = 0;
  meta_analyzer_.reset();
  body_buf_.reset();
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_MYSQL_RESP_ANALYZER_H */
