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

#ifndef OBPROXY_MYSQL_COMPRESS_OB20_ANALYZER_H
#define OBPROXY_MYSQL_COMPRESS_OB20_ANALYZER_H

#include "proxy/mysqllib/ob_2_0_protocol_utils.h"
#include "proxy/mysqllib/ob_2_0_protocol_struct.h"
#include "proxy/mysqllib/ob_mysql_compress_analyzer.h"

namespace oceanbase
{

namespace common
{
class ObObj;
class FLTObjManage;
}

namespace obproxy
{
namespace proxy
{

enum OB20AnalyzerState
{
  OB20_ANALYZER_EXTRA,
  OB20_ANALYZER_PAYLOAD,
  OB20_ANALYZER_TAIL,
  OB20_ANALYZER_END,
  OB20_ANALYZER_MAX
};

enum OB20ReqAnalyzeState
{
  OB20_REQ_ANALYZE_HEAD,
  OB20_REQ_ANALYZE_EXTRA,
  OB20_REQ_ANALYZE_END,
  OB20_REQ_ANALYZE_MAX
};

class ObMysqlCompressOB20Analyzer : public ObMysqlCompressAnalyzer
{
public:
  ObMysqlCompressOB20Analyzer()
    : ObMysqlCompressAnalyzer(), last_ob20_seq_(0), request_id_(0), sessid_(0), remain_head_checked_len_(0),
      extra_header_len_(0), extra_len_(0), extra_checked_len_(0), payload_checked_len_(0), tail_checked_len_(0),
      ob20_analyzer_state_(OB20_ANALYZER_MAX), crc64_(0), curr_compressed_ob20_header_()
    {}
  virtual ~ObMysqlCompressOB20Analyzer() { reset(); }

  virtual int init(const uint8_t last_seq, const AnalyzeMode mode,
                   const obmysql::ObMySQLCmd mysql_cmd,
                   const ObMysqlProtocolMode mysql_mode,
                   const bool enable_extra_ok_packet_for_stats,
                   const uint8_t last_ob20_seq,
                   const uint32_t request_id,
                   const uint32_t sessid);

  virtual void reset();
  virtual bool is_compressed_payload() const { return curr_compressed_ob20_header_.cp_hdr_.is_compressed_payload(); }

  virtual int analyze_first_response(event::ObIOBufferReader &reader,
                                     const bool need_receive_completed,
                                     ObMysqlCompressedAnalyzeResult &result,
                                     ObMysqlResp &resp);
  virtual int analyze_compress_packet_payload(event::ObIOBufferReader &reader,
                                              ObMysqlCompressedAnalyzeResult &result);
  int decompress_request_packet(common::ObString &req_buf,
                                ObMysqlCompressedOB20AnalyzeResult &ob20_result);
  int do_req_head_decode(const char* &buf_start, int64_t &buf_len);
  int do_req_extra_decode(const char* &buf_start, int64_t &buf_len, ObMysqlCompressedOB20AnalyzeResult &ob20_result);
    
  int64_t to_string(char *buf, const int64_t buf_len) const;

protected:
  virtual int decompress_data(const char *zprt, const int64_t zlen, ObMysqlResp &resp);
  virtual int decode_compressed_header(const common::ObString &compressed_data, int64_t &avail_len);
  virtual int analyze_last_compress_packet(const char *start, const int64_t len,
                                           const bool is_last_data, ObMysqlResp &resp);
  virtual int analyze_one_compressed_packet(event::ObIOBufferReader &reader,
                                            ObMysqlCompressedAnalyzeResult &result);
  virtual bool is_last_packet(const ObMysqlCompressedAnalyzeResult &result);

private:
  int do_header_decode(const char *start);
  int do_header_checksum(const char *header_start);
  int do_extra_info_decode(const char *&payload_start, int64_t &payload_len, ObMysqlResp &resp);
  int do_body_checksum(const char *&payload_start, int64_t &payload_len);
  int do_body_decode(const char *&payload_start, int64_t &payload_len, ObMysqlResp &resp);
  int do_analyzer_end(ObMysqlResp &resp);
  int do_obobj_extra_info_decode(const char *buf, const int64_t len, Ob20ExtraInfo &extra_info,
                                 common::FLTObjManage &flt_manage);
  int do_new_extra_info_decode(const char *buf, const int64_t len, Ob20ExtraInfo &extra_info,
                               common::FLTObjManage &flt_manage);

private:
  uint8_t last_ob20_seq_;
  uint32_t request_id_;
  uint32_t sessid_;
  int64_t remain_head_checked_len_;
  uint32_t extra_header_len_;
  uint32_t extra_len_;
  uint32_t extra_checked_len_;
  uint32_t payload_checked_len_;
  uint32_t tail_checked_len_;
  enum OB20ReqAnalyzeState ob20_req_analyze_state_;
  enum OB20AnalyzerState ob20_analyzer_state_;
  char temp_buf_[OB20_PROTOCOL_EXTRA_INFO_LENGTH];          // temp 4 extra len buffer
  char header_buf_[MYSQL_COMPRESSED_OB20_HEALDER_LENGTH];   // temp 7+24 head buffer
  uint64_t crc64_;
  ObRespResult result_;
  ObMysqlRespAnalyzer analyzer_;
  Ob20ProtocolHeader curr_compressed_ob20_header_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlCompressOB20Analyzer);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_MYSQL_COMPRESS_OB20_ANALYZER_H */
