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

#include "proxy/mysqllib/ob_mysql_compress_ob20_analyzer.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/checksum/ob_crc16.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "lib/utility/ob_2_0_full_link_trace_info.h"
#include "proxy/mysqllib/ob_protocol_diagnosis.h"

using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

static int64_t const MYSQL_BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K);

int ObMysqlCompressOB20Analyzer::init(
        const uint8_t last_seq, const AnalyzeMode mode,
        const obmysql::ObMySQLCmd mysql_cmd,
        const ObMysqlProtocolMode mysql_mode,
        const bool enable_extra_ok_packet_for_stats,
        const uint8_t last_ob20_seq,
        const uint32_t request_id,
        const uint32_t sessid,
        const bool is_analyze_compressed_ob20)
{
  int ret = OB_SUCCESS;

  ObMysqlCompressAnalyzer::init(last_seq, mode, mysql_cmd, mysql_mode, enable_extra_ok_packet_for_stats,
                                last_ob20_seq, request_id, sessid, is_analyze_compressed_ob20);
  last_ob20_seq_ = last_ob20_seq;
  request_id_ = request_id;
  sessid_ = sessid;
  is_analyze_compressed_ob20_ = is_analyze_compressed_ob20;
  LOG_DEBUG("ObMysqlCompressOB20Analyzer init", K(is_analyze_compressed_ob20), K(last_ob20_seq), K(request_id), K(sessid));
  if (is_analyze_compressed_ob20) {
    MEMSET(&compressed_ob20_, 0, sizeof(compressed_ob20_));
  }
  return ret;
}

void ObMysqlCompressOB20Analyzer::reset()
{
  ObMysqlCompressAnalyzer::reset();
  last_ob20_seq_ = 0;
  request_id_ = 0;
  sessid_ = 0;
  remain_head_checked_len_ = 0;
  extra_header_len_ = 0;
  extra_len_ = 0;
  extra_checked_len_ = 0;
  payload_checked_len_ = 0;
  tail_checked_len_ = 0;
  ob20_analyzer_state_ = OB20_ANALYZER_MAX;
  MEMSET(temp_buf_, 0, 4);
  MEMSET(header_buf_, 0, MYSQL_COMPRESSED_OB20_HEALDER_LENGTH);
  crc64_ = 0;
  curr_compressed_ob20_header_.reset();
  if (is_analyze_compressed_ob20_) {
    MEMSET(&compressed_ob20_, 0, sizeof(compressed_ob20_));
  }
  is_analyze_compressed_ob20_ = false;
}

int64_t ObMysqlCompressOB20Analyzer::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(last_ob20_seq),
       K_(request_id),
       K_(sessid),
       K_(remain_head_checked_len),
       K_(extra_header_len),
       K_(extra_len),
       K_(extra_checked_len),
       K_(payload_checked_len),
       K_(tail_checked_len),
       K_(ob20_analyzer_state),
       K_(crc64),
       K_(curr_compressed_ob20_header),
       K_(is_analyze_compressed_ob20));
  J_OBJ_END();
  return pos;
}

inline int ObMysqlCompressOB20Analyzer::do_body_checksum(const char *&payload_start, int64_t &payload_len)
{
  int ret = OB_SUCCESS;
  uint32_t tail_remain_len = static_cast<uint32_t>(OB20_PROTOCOL_TAILER_LENGTH - tail_checked_len_);

  if (tail_remain_len > 0 && payload_len > 0) {
    uint32_t tail_len = static_cast<uint32_t>(payload_len <= tail_remain_len ? payload_len : tail_remain_len);
    MEMCPY(temp_buf_ + tail_checked_len_, payload_start, tail_len);
    tail_checked_len_ += tail_len;
    payload_start += tail_len;
    payload_len -= tail_len;

    if (tail_checked_len_ == OB20_PROTOCOL_TAILER_LENGTH) {
      uint32_t payload_checksum = 0;
      char *temp_buf = temp_buf_;
      ObMySQLUtil::get_uint4(temp_buf, payload_checksum);
      ob20_analyzer_state_ = OB20_ANALYZER_END;


      if (payload_checksum != 0) {
        if (OB_UNLIKELY(crc64_ != payload_checksum))  {
          ret = OB_CHECKSUM_ERROR;
          LOG_EDIAG("body checksum error", K_(crc64), K(payload_checksum), K(ret));
        }
      } else {
        // 0 means skip checksum
        LOG_DEBUG("body checksum is 0", K_(crc64));
      }

    }
  }

  return ret;
}

inline int ObMysqlCompressOB20Analyzer::do_body_decode(const char *&payload_start,
                                                       int64_t &payload_len,
                                                       ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  uint32_t payload_remain_len = static_cast<uint32_t>(curr_compressed_ob20_header_.payload_len_
                                                      - extra_header_len_ - extra_len_ - payload_checked_len_);

  if (payload_remain_len > 0 && payload_len > 0) {
    int64_t filled_len = 0;
    uint32_t body_len = static_cast<uint32_t>(payload_len <= payload_remain_len ? payload_len : payload_remain_len);

    crc64_ = ob_crc64(crc64_, payload_start, body_len); // actual is crc32

    LOG_DEBUG("print body decode", K(payload_len), K(payload_remain_len), K(payload_checked_len_), K(body_len));

    ObString buf;
    buf.assign_ptr(payload_start, body_len);
    ObBufferReader buf_reader(buf);
    if (OB_FAIL(analyzer_.analyze_mysql_resp(buf_reader, result_, &resp, protocol_diagnosis_))) {
      LOG_WDIAG("fail to analyze mysql resp", K(ret));
    } else if (OB_FAIL(mysql_decompress_buffer_->write(payload_start, body_len, filled_len))) {
      LOG_WDIAG("fail to write uncompressed payload", K(payload_len), K(filled_len), K(ret));
    } else if (OB_UNLIKELY(body_len != filled_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to write uncompressed payload", K(payload_len), K(filled_len), K(ret));
    } else {
      if (OB_LIKELY(mysql_decompress_buffer_reader_ != NULL)) {
        // body_len of mysql packets has been analyzed
        mysql_decompress_buffer_reader_->consume(body_len);
      }
      payload_checked_len_ += body_len;
      payload_start += body_len;
      payload_len -= body_len;
      resp.get_analyze_result().reserved_len_of_last_ok_ = result_.get_reserved_len();
      LOG_DEBUG("do payload decode succ", K_(payload_checked_len), K(payload_len));

      if (payload_checked_len_ == curr_compressed_ob20_header_.payload_len_ - extra_header_len_ - extra_len_) {
        ob20_analyzer_state_ = OB20_ANALYZER_TAIL;
        LOG_DEBUG("do payload decode end", K_(crc64), K_(payload_checked_len), K(payload_len));
      }
    }
  }

  return ret;
}

inline int ObMysqlCompressOB20Analyzer::do_extra_info_decode(const char* &payload_start,
                                                             int64_t &payload_len,
                                                             ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  Ob20ExtraInfo &extra_info = resp.get_analyze_result().get_extra_info();

  if (curr_compressed_ob20_header_.flag_.is_extra_info_exist()) {
    //get extra info length
    if (extra_len_ == 0) {
      uint32_t extra_remain_len = static_cast<uint32_t>(OB20_PROTOCOL_EXTRA_INFO_LENGTH - extra_checked_len_);
      uint32_t extra_header_len = static_cast<uint32_t>(payload_len <= extra_remain_len ? payload_len : extra_remain_len);

      crc64_ = ob_crc64(crc64_, payload_start, extra_header_len); // actual is crc32

      MEMCPY(temp_buf_ + extra_checked_len_, payload_start, extra_header_len);
      extra_checked_len_ += extra_header_len;
      payload_start += extra_header_len;
      payload_len -= extra_header_len;
      LOG_DEBUG("do extra info lenth succ", K_(extra_checked_len), K(payload_len));

      if (extra_checked_len_ == OB20_PROTOCOL_EXTRA_INFO_LENGTH) {
        char *temp_buf = temp_buf_;
        ObMySQLUtil::get_uint4(temp_buf, extra_len_);
        extra_header_len_ = OB20_PROTOCOL_EXTRA_INFO_LENGTH;
        extra_checked_len_ = 0;
        LOG_DEBUG("do extra info length end", K_(extra_len), K_(extra_header_len), K(payload_len));

        extra_info.extra_info_buf_.reset();
      }
    }

    //get extra info
    int64_t extra_remain_len = extra_len_ - extra_checked_len_;
    if (OB_SUCC(ret) && extra_len_ > 0 && extra_remain_len > 0 && payload_len > 0) {
      uint32_t extra_len = static_cast<uint32_t>(payload_len <= extra_remain_len ? payload_len : extra_remain_len);
      if (!extra_info.extra_info_buf_.is_inited()) {
        if (OB_FAIL(extra_info.extra_info_buf_.init(extra_len_))) {
          LOG_WDIAG("fail int alloc mem", K(extra_len_), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(extra_info.extra_info_buf_.write(payload_start, extra_len))) {
          LOG_WDIAG("fail to write", K(extra_len), K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        crc64_ = ob_crc64(crc64_, payload_start, extra_len); // actual is crc32

        extra_checked_len_ += extra_len;
        payload_start += extra_len;
        payload_len -= extra_len;

        LOG_DEBUG("do extra info succ", K_(extra_checked_len), K(payload_len));
        if (extra_checked_len_ == extra_len_) {
          if (curr_compressed_ob20_header_.flag_.is_new_extra_info()) {
            if (OB_FAIL(do_new_extra_info_decode(extra_info.extra_info_buf_.ptr(),
                                                 extra_info.extra_info_buf_.len(),
                                                 extra_info,
                                                 resp.get_analyze_result().flt_))) {
              LOG_WDIAG("fail to do resp new extra info decode", K(ret));
            }
          } else {
            if (OB_FAIL(do_obobj_extra_info_decode(extra_info.extra_info_buf_.ptr(),
                                                   extra_info.extra_info_buf_.len(),
                                                   extra_info,
                                                   resp.get_analyze_result().flt_))) {
              LOG_WDIAG("fail to do resp obobj extra info decode", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            ob20_analyzer_state_ = OB20_ANALYZER_PAYLOAD;
            LOG_DEBUG("do extra info end", K_(extra_len), K(payload_len));
          }
        } // if extra info received done
      }
    }
  } else {
    ob20_analyzer_state_ = OB20_ANALYZER_PAYLOAD;
    LOG_DEBUG("no need decode extra");
  }

  return ret;
}

int ObMysqlCompressOB20Analyzer::do_obobj_extra_info_decode(const char *buf,
                                                            const int64_t len,
                                                            Ob20ExtraInfo &extra_info,
                                                            common::FLTObjManage &flt_manage)
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;
  while (OB_SUCC(ret) && pos < len) {
    common::ObObj key;
    common::ObObj value;
    if (OB_FAIL(key.deserialize(buf, len, pos))) {
      LOG_WDIAG("fail to deserialize extra info", K(ret));
    } else if (!key.is_varchar()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid extra info key type", K(ret), K(key));
    } else if (OB_FAIL(value.deserialize(buf, len, pos))) {
      LOG_WDIAG("fail to deserialize extra info", K(ret));
    } else if (!value.is_varchar()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid extra info value type", K(ret), K(key), K(value));
    } else {
      LOG_DEBUG("deserialize extra info", K(key), K(value));

      if (0 == key.get_string().case_compare(OB_V20_PRO_EXTRA_KV_NAME_SYNC_SESSION_INFO)) {
        const char *value_ptr = value.get_string().ptr();
        const int64_t value_len = value.get_string().length();
        if (OB_FAIL(extra_info.add_sess_info_buf(value_ptr, value_len))) {
          LOG_WDIAG("fail to write sess info to buf", K(ret), K(value));
        }
      } else if (0 == key.get_string().case_compare(OB_V20_PRO_EXTRA_KV_NAME_FULL_LINK_TRACE)) {
        int64_t full_pos = 0;
        ObString full_trc = value.get_string();
        const char *full_trc_buf = full_trc.ptr();
        const int64_t full_trc_len = full_trc.length();
        if (OB_FAIL(flt_manage.deserialize(full_trc_buf, full_trc_len, full_pos))) {
          LOG_WDIAG("fail to deserialize FLT", K(ret));
        } else if (full_pos != full_trc_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected pos and length check", K(ret), K(full_pos), K(full_trc));
        } else {
          LOG_DEBUG("succ to deserialize obobj extra info", K(flt_manage));
        }
      } else {
        LOG_DEBUG("attention: do no recognize such an extra info key", K(ret), K(key));
      }
    }
  } // while

  return ret;
}

int ObMysqlCompressOB20Analyzer::do_new_extra_info_decode(const char *buf,
                                                          const int64_t len,
                                                          Ob20ExtraInfo &extra_info,
                                                          common::FLTObjManage &flt_manage)
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;
  while (OB_SUCC(ret) && pos < len) {
    int16_t key_type = OB20_SVR_END;
    int32_t key_len = 0;
    if (OB_FAIL(common::Ob20FullLinkTraceTransUtil::resolve_type_and_len(buf, len, pos, key_type, key_len))) {
      LOG_WDIAG("fail to resolve type and len for new extra info", K(ret));
    } else {
      Ob20NewExtraInfoProtocolKeyType type = static_cast<Ob20NewExtraInfoProtocolKeyType>(key_type);
      if (type == SESS_INFO) {
        if (OB_FAIL(extra_info.add_sess_info_buf(buf+pos, key_len))) {
          LOG_WDIAG("fail to write sess info buf", K(ret), K(key_len));
        }
      } else if (type == FULL_TRC) {
        int64_t full_pos = 0;
        if (OB_FAIL(flt_manage.deserialize(buf + pos, key_len, full_pos))) {
          LOG_WDIAG("fail to deserialize FLT", K(ret));
        } else if (full_pos != key_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected pos and length check", K(ret), K(full_pos));
        } else {
          LOG_DEBUG("succ to deserialize new extra info", K(flt_manage));
        }
      } else {
        LOG_DEBUG("unexpected new extra info type, ignore", K(type), K(key_len));
      }
      pos += key_len;     // pos offset at last
    }
  } // while

  return ret;
}

inline int ObMysqlCompressOB20Analyzer::do_header_checksum(const char *check_header_start, int64_t check_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(check_header_start)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("invalid input value", KP(check_header_start), K(ret));
  } else if (0 == curr_compressed_ob20_header_.header_checksum_) {
    // 0 means skip checksum
  } else {
    // 3. crc16 for header checksum
    uint16_t local_header_checksum = 0;
    local_header_checksum = ob_crc16(0, reinterpret_cast<const uint8_t *>(check_header_start), check_len);
    if (local_header_checksum != curr_compressed_ob20_header_.header_checksum_) {
      ret = OB_CHECKSUM_ERROR;
      LOG_EDIAG("ob 2.0 protocol header checksum error!", K(local_header_checksum),
                K(curr_compressed_ob20_header_.header_checksum_), K(check_len), KP(check_header_start), K(ret));
    }
  }

  return ret;
}

inline int ObMysqlCompressOB20Analyzer::do_header_decode(const char *start)
{
  int ret = OB_SUCCESS;

  curr_compressed_ob20_header_.reset();

  // 1. decode mysql compress header
  if (!is_analyze_compressed_ob20_) {
    uint32_t pktlen = 0;
    uint8_t pktseq = 0;
    uint32_t pktlen_before_compress = 0; // here, must be 0
    ObMySQLUtil::get_uint3(start, pktlen);
    ObMySQLUtil::get_uint1(start, pktseq);
    ObMySQLUtil::get_uint3(start, pktlen_before_compress);
    curr_compressed_ob20_header_.cp_hdr_.compressed_len_ = pktlen;
    curr_compressed_ob20_header_.cp_hdr_.seq_ = pktseq;
    curr_compressed_ob20_header_.cp_hdr_.non_compressed_len_ = pktlen_before_compress;
  }

  // 2. decode proto2.0 header
  ObMySQLUtil::get_uint2(start, curr_compressed_ob20_header_.magic_num_);
  ObMySQLUtil::get_uint2(start, curr_compressed_ob20_header_.version_);
  ObMySQLUtil::get_uint4(start, curr_compressed_ob20_header_.connection_id_);
  ObMySQLUtil::get_uint3(start, curr_compressed_ob20_header_.request_id_);
  ObMySQLUtil::get_uint1(start, curr_compressed_ob20_header_.pkt_seq_);
  ObMySQLUtil::get_uint4(start, curr_compressed_ob20_header_.payload_len_);
  ObMySQLUtil::get_uint4(start, curr_compressed_ob20_header_.flag_.flags_);
  ObMySQLUtil::get_uint2(start, curr_compressed_ob20_header_.reserved_);
  ObMySQLUtil::get_uint2(start, curr_compressed_ob20_header_.header_checksum_);

  LOG_DEBUG("decode proto20 header succ", K(curr_compressed_ob20_header_));

  return ret;
}
int ObMysqlCompressOB20Analyzer::decode_ob20_header(const ObString &compressed_data,
                                                    int64_t &avail_len)
{
  int ret = OB_SUCCESS;
  int64_t origin_len = compressed_data.length();
  const char *start = compressed_data.ptr();
  const char *header_buffer_start = NULL;
  uint8_t next_pkt_seq = (uint8_t)(last_ob20_seq_ + 1);

  // an optimization, if header all in one buf, no need copy
  if ((0 == header_valid_len_) && (avail_len >= OB20_PROTOCOL_HEADER_LENGTH)) {
    header_buffer_start = start + origin_len - avail_len;
    avail_len -= OB20_PROTOCOL_HEADER_LENGTH;
    header_valid_len_ = OB20_PROTOCOL_HEADER_LENGTH;
  } else {
    if (header_valid_len_ < OB20_PROTOCOL_HEADER_LENGTH) {
      int64_t need_len = OB20_PROTOCOL_HEADER_LENGTH - header_valid_len_;
      int64_t copy_len = (avail_len >= need_len) ? (need_len) : (avail_len);
      MEMCPY(header_buf_ + header_valid_len_, (start + origin_len - avail_len), copy_len);
      avail_len -= copy_len;
      header_valid_len_ += copy_len;
      header_buffer_start = header_buf_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("never happen", K_(header_valid_len), K(curr_compressed_ob20_header_),
               K(avail_len), K(is_last_packet_), K(ret));
    }
  }
  char *check_header_start = const_cast<char*>(header_buffer_start);
  int64_t check_len = OB20_PROTOCOL_HEADER_LENGTH - 2;

  if (OB_SUCC(ret) && (header_valid_len_ == OB20_PROTOCOL_HEADER_LENGTH)) {
    // compress packet header received completely, and start to analyze
    if (OB_FAIL(do_header_decode(header_buffer_start))) {
      LOG_WDIAG("fail to analyze compressed packet header", K(ret));
      // 3. crc16 for header checksum
    } else if (OB_FAIL(do_header_checksum(check_header_start, check_len))) {
      LOG_EDIAG("fail to do header checksum", K(curr_compressed_ob20_header_), K(check_len),K(ret));
    } else if (OB_UNLIKELY(OB20_PROTOCOL_MAGIC_NUM != curr_compressed_ob20_header_.magic_num_)) {
      ret = OB_UNKNOWN_PACKET;
      LOG_EDIAG("invalid magic num", K(OB20_PROTOCOL_MAGIC_NUM),
                K(curr_compressed_ob20_header_.magic_num_), K_(sessid), K(ret));
    } else if (OB_UNLIKELY(sessid_ != curr_compressed_ob20_header_.connection_id_)) {
      ret = OB_UNKNOWN_CONNECTION;
      LOG_EDIAG("connection id mismatch", K_(sessid), K_(curr_compressed_ob20_header_.connection_id), K(ret));
    } else if (OB_UNLIKELY(OB20_PROTOCOL_VERSION_VALUE != curr_compressed_ob20_header_.version_)) {
      ret = OB_UNKNOWN_PACKET;
      LOG_EDIAG("invalid version", K(OB20_PROTOCOL_VERSION_VALUE),
                K(curr_compressed_ob20_header_.version_), K_(sessid), K(ret));
    } else if (OB_UNLIKELY(request_id_ != curr_compressed_ob20_header_.request_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("invalid request_id", K_(request_id),
                "current request id", curr_compressed_ob20_header_.request_id_,
                K_(curr_compressed_ob20_header), K(ret));
    } else if (OB_UNLIKELY(curr_compressed_ob20_header_.pkt_seq_ != next_pkt_seq)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("invalid pkg seq", K_(last_ob20_seq),
                "current pkg seq", curr_compressed_ob20_header_.pkt_seq_,
                K_(curr_compressed_ob20_header), K(ret));
    } else {
      if (mode_ == DECOMPRESS_MODE) {
        PROTOCOL_DIAGNOSIS(OCEANBASE20, recv,  protocol_diagnosis_,
                           compressed_ob20_.cur_hdr_.compressed_len_,
                           compressed_ob20_.cur_hdr_.seq_,
                           compressed_ob20_.cur_hdr_.non_compressed_len_,
                           curr_compressed_ob20_header_.payload_len_,
                           curr_compressed_ob20_header_.connection_id_,
                           curr_compressed_ob20_header_.flag_,
                           curr_compressed_ob20_header_.pkt_seq_,
                           curr_compressed_ob20_header_.request_id_);
      }
      if (curr_compressed_ob20_header_.flag_.is_last_packet()) {
        is_last_packet_ = true;
      }

      extra_header_len_ = 0;
      extra_len_ = 0;
      extra_checked_len_ = 0;
      payload_checked_len_ = 0;
      tail_checked_len_ = 0;
      ob20_analyzer_state_ = OB20_ANALYZER_EXTRA;
      crc64_ = 0;
      last_ob20_seq_++;
      //last_seq_ = curr_compressed_ob20_header_.cp_hdr_.seq_; not use by compressed ob20
      last_compressed_pkt_remain_len_ = curr_compressed_ob20_header_.payload_len_ + OB20_PROTOCOL_TAILER_LENGTH;
    }
  }

  return ret;
}

int ObMysqlCompressOB20Analyzer::decode_compress_ob20_header(const ObString &compressed_data,
                                                          int64_t &avail_len)
{
  int ret = OB_SUCCESS;
  int64_t origin_len = compressed_data.length();
  const char *start = compressed_data.ptr();
  const char *header_buffer_start = NULL;
  uint8_t next_pkt_seq = (uint8_t)(last_ob20_seq_ + 1);

  // an optimization, if header all in one buf, no need copy
  if ((0 == header_valid_len_) && (avail_len >= MYSQL_COMPRESSED_OB20_HEALDER_LENGTH)) {
    header_buffer_start = start + origin_len - avail_len;
    avail_len -= MYSQL_COMPRESSED_OB20_HEALDER_LENGTH;
    header_valid_len_ = MYSQL_COMPRESSED_OB20_HEALDER_LENGTH;
  } else {
    if (header_valid_len_ < MYSQL_COMPRESSED_OB20_HEALDER_LENGTH) {
      int64_t need_len = MYSQL_COMPRESSED_OB20_HEALDER_LENGTH - header_valid_len_;
      int64_t copy_len = (avail_len >= need_len) ? (need_len) : (avail_len);
      MEMCPY(header_buf_ + header_valid_len_, (start + origin_len - avail_len), copy_len);
      avail_len -= copy_len;
      header_valid_len_ += copy_len;
      header_buffer_start = header_buf_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("never happen", K_(header_valid_len), K(curr_compressed_ob20_header_),
               K(avail_len), K(is_last_packet_), K(ret));
    }
  }
  char *check_header_start = const_cast<char*>(header_buffer_start);
  int64_t check_len = MYSQL_COMPRESSED_OB20_HEALDER_LENGTH - 2;
  if (is_analyze_compressed_ob20_) {
    check_header_start = const_cast<char*>(header_buffer_start) + MYSQL_COMPRESSED_HEALDER_LENGTH;
    check_len = OB20_PROTOCOL_HEADER_LENGTH - 2;
  }


  if (OB_SUCC(ret) && (header_valid_len_ == MYSQL_COMPRESSED_OB20_HEALDER_LENGTH)) {
    // compress packet header received completely, and start to analyze
    if (OB_FAIL(do_header_decode(header_buffer_start))) {
      LOG_WDIAG("fail to analyze compressed packet header", K(ret));
      // 3. crc16 for header checksum
    } else if (OB_FAIL(do_header_checksum(check_header_start, check_len))) {
      LOG_EDIAG("fail to do header checksum", K(curr_compressed_ob20_header_), K(check_len),K(ret));
    } else if (OB_UNLIKELY(OB20_PROTOCOL_MAGIC_NUM != curr_compressed_ob20_header_.magic_num_)) {
      ret = OB_UNKNOWN_PACKET;
      LOG_EDIAG("invalid magic num", K(OB20_PROTOCOL_MAGIC_NUM),
                K(curr_compressed_ob20_header_.magic_num_), K_(sessid), K(ret));
    } else if (OB_UNLIKELY(sessid_ != curr_compressed_ob20_header_.connection_id_)) {
      ret = OB_UNKNOWN_CONNECTION;
      LOG_EDIAG("connection id mismatch", K_(sessid), K_(curr_compressed_ob20_header_.connection_id), K(ret));
    } else if (0 != curr_compressed_ob20_header_.cp_hdr_.non_compressed_len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("pktlen_before_compress must be 0 here", K(curr_compressed_ob20_header_.cp_hdr_.non_compressed_len_),
                K_(sessid), K(ret));
    } else if (OB_UNLIKELY(OB20_PROTOCOL_VERSION_VALUE != curr_compressed_ob20_header_.version_)) {
      ret = OB_UNKNOWN_PACKET;
      LOG_EDIAG("invalid version", K(OB20_PROTOCOL_VERSION_VALUE),
                K(curr_compressed_ob20_header_.version_), K_(sessid), K(ret));
    } else if (OB_UNLIKELY(curr_compressed_ob20_header_.cp_hdr_.compressed_len_ !=
                           (curr_compressed_ob20_header_.payload_len_ +
                            OB20_PROTOCOL_HEADER_LENGTH +
                            OB20_PROTOCOL_TAILER_LENGTH))) {
      // must only contain one ob20 packet
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("invalid pktlen len",
                K(curr_compressed_ob20_header_.cp_hdr_.compressed_len_),
                K(curr_compressed_ob20_header_.payload_len_), K_(sessid), K(ret));
    } else if (OB_UNLIKELY(request_id_ != curr_compressed_ob20_header_.request_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("invalid request_id", K_(request_id),
                "current request id", curr_compressed_ob20_header_.request_id_,
                K_(curr_compressed_ob20_header), K(ret));
    } else if (OB_UNLIKELY(curr_compressed_ob20_header_.pkt_seq_ != next_pkt_seq)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("invalid pkg seq", K_(last_ob20_seq),
                "current pkg seq", curr_compressed_ob20_header_.pkt_seq_,
                K_(curr_compressed_ob20_header), K(ret));
    } else {
      if (mode_ == DECOMPRESS_MODE) {
        PROTOCOL_DIAGNOSIS(OCEANBASE20, recv, protocol_diagnosis_,
                           curr_compressed_ob20_header_.cp_hdr_.compressed_len_,
                           curr_compressed_ob20_header_.cp_hdr_.seq_,
                           curr_compressed_ob20_header_.cp_hdr_.non_compressed_len_,
                           curr_compressed_ob20_header_.payload_len_,
                           curr_compressed_ob20_header_.connection_id_,
                           curr_compressed_ob20_header_.flag_,
                           curr_compressed_ob20_header_.pkt_seq_,
                           curr_compressed_ob20_header_.request_id_);
      }
      if (curr_compressed_ob20_header_.flag_.is_last_packet()) {
        is_last_packet_ = true;
      }

      extra_header_len_ = 0;
      extra_len_ = 0;
      extra_checked_len_ = 0;
      payload_checked_len_ = 0;
      tail_checked_len_ = 0;
      ob20_analyzer_state_ = OB20_ANALYZER_EXTRA;
      crc64_ = 0;
      last_ob20_seq_++;
      last_seq_ = curr_compressed_ob20_header_.cp_hdr_.seq_;
      last_compressed_pkt_remain_len_ = curr_compressed_ob20_header_.payload_len_ + OB20_PROTOCOL_TAILER_LENGTH;

      LOG_DEBUG("decode compressed header succ",
                K_(curr_compressed_ob20_header));
    }
  }

  return ret;
}

int ObMysqlCompressOB20Analyzer::decode_compressed_header(const ObString &compressed_data,
                                                          int64_t &avail_len)
{
  return is_analyze_compressed_ob20_?
          decode_ob20_header(compressed_data, avail_len) :
          decode_compress_ob20_header(compressed_data, avail_len);
}

int ObMysqlCompressOB20Analyzer::analyze_last_compress_packet(
        const char *start, const int64_t len,
        const bool is_last_data, ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  UNUSED(is_last_data);
  if (OB_ISNULL(start) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(start), K(len), K(ret));
  } else if (OB_FAIL(decompress_data(start, len, resp))) {
    LOG_WDIAG("fail to decompress last mysql packet", K(ret));
  }
  resp.get_analyze_result().is_server_trans_internal_routing_ = curr_compressed_ob20_header_.flag_.is_trans_internal_routing();
  return ret;
}

// streaming analyze ob20 packets
int ObMysqlCompressOB20Analyzer::decompress_data(const char *compressed_data, const int64_t len, ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  const char *payload_start = compressed_data;
  int64_t payload_len = len;

  LOG_DEBUG("ob20 analyzer state", K(ob20_analyzer_state_), K(len));

  while (OB_SUCC(ret) && payload_len > 0) {
    switch (ob20_analyzer_state_) {
      case OB20_ANALYZER_EXTRA: {
        if (OB_FAIL(do_extra_info_decode(payload_start, payload_len, resp))) {
          LOG_EDIAG("do extra info decode failed", K(payload_len), K(len), KPC(this), K(ret));
        }
        break;
      }
      case OB20_ANALYZER_PAYLOAD: {
        if (OB_FAIL(do_body_decode(payload_start, payload_len, resp))) {
          LOG_EDIAG("do body decode failed", K(payload_len), K(len), KPC(this), K(ret));
        }
        break;
      }
      case OB20_ANALYZER_TAIL: {
        if (OB_FAIL(do_body_checksum(payload_start, payload_len))) {
          LOG_EDIAG("do body checksum failed", K(payload_len), K(len), KPC(this), K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("ob20 analyzer state is not right", K(payload_len), K(len), KPC(this), K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && OB20_ANALYZER_END == ob20_analyzer_state_
      && OB_FAIL(analyze_mysql_packet_end(resp))) {
    LOG_EDIAG("do analyze_mysql_packet_end failed", K(payload_len), K(len), KPC(this), K(ret));
  }

  return ret;
}

int ObMysqlCompressOB20Analyzer::analyze_one_compressed_packet(ObIOBufferReader &reader,
                                                               ObMysqlCompressedAnalyzeResult &result)
{
  return ObProto20Utils::analyze_one_compressed_packet(reader,
                                                       dynamic_cast<ObMysqlCompressedOB20AnalyzeResult&>(result),
                                                       is_analyze_compressed_ob20_);
}

bool ObMysqlCompressOB20Analyzer::is_last_packet(const ObMysqlCompressedAnalyzeResult &result)
{
  const ObMysqlCompressedOB20AnalyzeResult& ob20_result = dynamic_cast<const ObMysqlCompressedOB20AnalyzeResult&>(result);
  return ob20_result.ob20_header_.flag_.is_last_packet();
}

/*
  ATTENTION!!!
  called by ObMysqlCompressAnalyzer::analyze_response(event::ObIOBufferReader &reader, ObMysqlResp *resp)
  and ObMysqlCompressAnalyzer::analyze_response will be called by
  [ObPacketAnalyzer::process_response_content] and [ObMysqlResponseCompressTransformPlugin::consume]
*/
int ObMysqlCompressOB20Analyzer::analyze_compressed_response(
  event::ObIOBufferReader &reader,
  ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  ObIOBufferReader *normal_ob20_buf_reader = &reader;
  ObIOBufferReader *decompressed_buf_reader = NULL;

  bool has_data = true;
  if (is_analyze_compressed_ob20_) {
    if (OB_ISNULL(ob20_decompress_buffer_) && OB_ISNULL(ob20_decompress_buffer_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ob20_decompress_buffer_ is NULL", K(ret));
    } else if (OB_FALSE_IT(ob20_decompress_buffer_->reset())) {
    } else if (OB_ISNULL(decompressed_buf_reader = ob20_decompress_buffer_->alloc_reader())) {
      LOG_WDIAG("fail to alloc reader", K(ob20_decompress_buffer_));
    } else if (OB_FAIL(decompress_compressed_ob20(reader, *ob20_decompress_buffer_))) {
      LOG_WDIAG("fail to do decompress_compressed_ob20", K(ret));
    } else {
      normal_ob20_buf_reader = decompressed_buf_reader;
      has_data = normal_ob20_buf_reader->read_avail() > 0;
      resp.get_analyze_result().is_compressed_ob20_pkt_completed_ = (compressed_ob20_.last_remain_len_ == 0);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (normal_ob20_buf_reader == NULL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("normal_ob20_buf_reader is NULL");
  } else if (!has_data) {
    LOG_DEBUG("no data, got compress header or tailer");
  } else if (OB_FAIL(ObMysqlCompressAnalyzer::analyze_compressed_response(*normal_ob20_buf_reader, resp))) {
    LOG_WDIAG("fail to do analyze_compressed_response", K(resp), K(ret));
  }

  if (ob20_decompress_buffer_ != NULL) {
    free_miobuffer(ob20_decompress_buffer_);
    ob20_decompress_buffer_ = NULL;
  }

  if (decompressed_buf_reader != NULL) {
    decompressed_buf_reader->destroy();
    decompressed_buf_reader = NULL;
  }

  return ret;
}

int ObMysqlCompressOB20Analyzer::decompress_compressed_ob20_data_to_buffer(
  const char *zprt,
  const int64_t zlen,
  event::ObMIOBuffer &decompress_buffer)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(zprt) || OB_UNLIKELY(zlen < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(&decompress_buffer), KP(zprt), K(zlen), K(ret));
  } else {
    int64_t filled_len = 0;
    if (compressed_ob20_.cur_hdr_.is_compressed_payload()) {
      char *buf = NULL;
      int64_t len = 0;
      bool stop = false;
      if (OB_FAIL(compressor_.add_decompress_data(zprt, zlen))) {
        LOG_WDIAG("fail to add decompress data", K(ret));
      }
      while (OB_SUCC(ret) && !stop) {
        if (OB_FAIL(decompress_buffer.get_write_avail_buf(buf, len))) {
          LOG_WDIAG("fail to get successive vuf", K(len), K(ret));
        } else if (OB_ISNULL(buf) || (0 == len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("invalid argument", KP(buf), K(len), K(ret));
        } else if (OB_FAIL(compressor_.decompress(buf, len, filled_len))) {
          LOG_WDIAG("fail to decompress", KP(buf), K(len), K(ret));
        } else {
          if (filled_len > 0) {
            decompress_buffer.fill(filled_len);
          }
          if (len > filled_len) { // complete
            stop = true;
          }
        }
      }
    } else {
      //uncompressed payload, just copy
      //TODO:: this can do better, remove_append
      if (OB_FAIL(decompress_buffer.write(zprt, zlen, filled_len))) {
        LOG_WDIAG("fail to write uncompressed payload", K(zlen), K(filled_len), K(ret));
      } else if (OB_UNLIKELY(zlen != filled_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to write uncompressed payload", K(zlen), K(filled_len), K(ret));
      }
    }
  }
  return ret;
}
int ObMysqlCompressOB20Analyzer::decode_compressed_ob20_header(
  const ObString &compressed_data,
  int64_t &avail_len)
{
  int ret = OB_SUCCESS;
  int64_t origin_len = compressed_data.length();
  const char *start = compressed_data.ptr();
  const char *header_buffer_start = NULL;

  // an optimization, if header all in one buf, no need copy
  if ((0 == compressed_ob20_.cur_valid_hdr_len_) &&
      (avail_len >= MYSQL_COMPRESSED_HEALDER_LENGTH)) {
    header_buffer_start = start + origin_len - avail_len;
    avail_len -= MYSQL_COMPRESSED_HEALDER_LENGTH;
    compressed_ob20_.cur_valid_hdr_len_ = MYSQL_COMPRESSED_HEALDER_LENGTH;
  } else {
    if (compressed_ob20_.cur_valid_hdr_len_ < MYSQL_COMPRESSED_HEALDER_LENGTH) {
      int64_t need_len = MYSQL_COMPRESSED_HEALDER_LENGTH - compressed_ob20_.cur_valid_hdr_len_;
      int64_t copy_len = (avail_len >= need_len) ? (need_len) : (avail_len);
      MEMCPY(compressed_ob20_.hdr_buf_ + compressed_ob20_.cur_valid_hdr_len_, (start + origin_len - avail_len), copy_len);
      avail_len -= copy_len;
      compressed_ob20_.cur_valid_hdr_len_ += copy_len;
      header_buffer_start = compressed_ob20_.hdr_buf_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("never happen", K_(compressed_ob20_.cur_valid_hdr_len), K(compressed_ob20_.cur_hdr_),
               K(avail_len),
               K(ret));
      abort();
    }
  }

  if (OB_SUCC(ret) && (compressed_ob20_.cur_valid_hdr_len_ == MYSQL_COMPRESSED_HEALDER_LENGTH)) {
    // compress packet header received completely, and start to analyze
    compressed_ob20_.cur_hdr_.reset();
    if (OB_FAIL(ObMysqlAnalyzerUtils::analyze_compressed_packet_header(
            header_buffer_start, MYSQL_COMPRESSED_HEALDER_LENGTH, compressed_ob20_.cur_hdr_))) {
      LOG_WDIAG("fail to analyze compressed packet header", K(ret));
    } else {
      compressed_ob20_.last_remain_len_ = compressed_ob20_.cur_hdr_.compressed_len_;
      LOG_DEBUG("decode compressed header succ", K_(compressed_ob20_.cur_hdr));
    }
  }

  return ret;
}

int ObMysqlCompressOB20Analyzer::decompress_compressed_ob20(
  const ObString &compressed_data,
  ObMIOBuffer &write_buf)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(compressed_data.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("empty compressed data", K(compressed_data), K(ret));
  } else {
    int64_t origin_len = compressed_data.length();
    int64_t avail_len = origin_len;
    const char *start = compressed_data.ptr();

    while (OB_SUCC(ret) && avail_len > 0) {
      // 1. last compressed packet decompressed completely
      //    so read header to prepare decompressed next compressed packet
      if (0 == compressed_ob20_.last_remain_len_) { // next compressed packet start
        if (OB_FAIL(decode_compressed_ob20_header(compressed_data, avail_len))) {
          LOG_WDIAG("fail to decode compressed header", K(ret));
        }
      // 2. last compressed packet's remain
      //    directly add these data to decompress
      } else if (compressed_ob20_.last_remain_len_ > 0) { // just consume
        const char *buf_start = start + origin_len - avail_len;
        int64_t buf_len = 0;
        // the remaining of the one compressed packet across two blocks
        // so we just decompress the first part
        if (compressed_ob20_.last_remain_len_ > avail_len) {
          buf_len = avail_len;
          compressed_ob20_.last_remain_len_ -= avail_len;
          avail_len = 0;
        // the remaining of the one compressed packet in this block
        // so we finish to decompress this packet
        // and make header_valid_len to 0 to prepare decompress the next one
        } else {
          buf_len = compressed_ob20_.last_remain_len_;
          avail_len -= compressed_ob20_.last_remain_len_;
          compressed_ob20_.last_remain_len_ = 0;
          compressed_ob20_.cur_valid_hdr_len_ = 0; // prepare next compressed packet
        }
        if (OB_FAIL(decompress_compressed_ob20_data_to_buffer(buf_start, buf_len, write_buf))) {
          LOG_WDIAG("fail to do decompress_data_to_buffer", K(buf_start), K(buf_len), K(&write_buf));
        }
      } else { // last_compressed_pkt_remain_len_ < 0
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid last remain len", K_(compressed_ob20_.last_remain_len), K(ret));
      }
    }
  }
  return ret;
}

int ObMysqlCompressOB20Analyzer::decompress_compressed_ob20(
  ObIOBufferReader &reader,
  ObMIOBuffer &write_buf)
{
  int ret = OB_SUCCESS;
  ObIOBufferBlock *block = NULL;
  int64_t offset = 0;
  char *data = NULL;
  int64_t data_size = 0;
  if (NULL != reader.block_) {
    reader.skip_empty_blocks();
    block = reader.block_;
    offset = reader.start_offset_;
    data = block->start() + offset;
    data_size = block->read_avail() - offset;
  }

  if (data_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("the first block data_size in reader is less than 0",
             K(offset), K(block->read_avail()), K(ret));
  }

  ObString resp_buf;
  while (OB_SUCC(ret) && NULL != block && data_size > 0) {
    resp_buf.assign_ptr(data, static_cast<int32_t>(data_size));
    if (OB_FAIL(decompress_compressed_ob20(resp_buf, write_buf))) {
      LOG_WDIAG("fail to analyze compressed response", K(ret));
    } else {
      // on to the next block
      offset = 0;
      block = block->next_;
      if (NULL != block) {
        data = block->start();
        data_size = block->read_avail();
      }
    }
  }
  LOG_DEBUG("finish to do decompress_compressed_ob20 from reader", K(reader.read_avail()));

  return ret;
}

// Two parts:
// 1. analyze first ob20 packet
// 2. analyze all ob20 packet if needed
int ObMysqlCompressOB20Analyzer::analyze_first_response(
  ObIOBufferReader &reader,
  const bool need_receive_completed,
  ObMysqlCompressedAnalyzeResult &result,
  ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  ObIOBufferReader *normal_ob20_buf_reader = &reader;
  ObIOBufferReader *decompressed_buf_reader = NULL;

  bool has_data = true;
  if (is_analyze_compressed_ob20_) {
    if (OB_ISNULL(ob20_decompress_buffer_) && OB_ISNULL(ob20_decompress_buffer_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ob20_decompress_buffer_ is NULL", K(ret));
    } else if (OB_FALSE_IT(ob20_decompress_buffer_->reset())) {
    } else if (OB_ISNULL(decompressed_buf_reader = ob20_decompress_buffer_->alloc_reader())) {
      LOG_WDIAG("fail to alloc reader", K(ob20_decompress_buffer_));
    } else if (OB_FAIL(decompress_compressed_ob20(reader, *ob20_decompress_buffer_))) {
      LOG_WDIAG("fail to do decompress_compressed_ob20", K(ret));
    } else {
      normal_ob20_buf_reader = decompressed_buf_reader;
      has_data = normal_ob20_buf_reader->read_avail() > 0;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(normal_ob20_buf_reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("normal_ob20_buf_reader is NULL");
  } else if (!has_data) {
    LOG_DEBUG("no data, got the compress header or tailer");
  } else if (OB_FAIL(ObMysqlCompressAnalyzer::analyze_first_response(*normal_ob20_buf_reader, need_receive_completed, result, resp))) {
    LOG_WDIAG("analyze first response failed", K(result), K(need_receive_completed), K(ret));
  } else {
    if (is_analyze_compressed_ob20_ && resp.get_analyze_result().is_resp_completed()) {
      if (mode_ == DECOMPRESS_MODE) {
        int64_t written_len = 0;
        int64_t tmp_read_avail = normal_ob20_buf_reader->read_avail();
        // write all decompressed data back to reader.mbuf_
        if (OB_FAIL(reader.consume_all())) {
          LOG_WDIAG("fail to consume all", K(ret));
        } else if (OB_FAIL(reader.mbuf_->write(normal_ob20_buf_reader, tmp_read_avail, written_len))) {
          LOG_WDIAG("fail to write", K(tmp_read_avail), K(ret));
        } else if (OB_UNLIKELY(written_len != tmp_read_avail)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("written_len is not expected", K(written_len), K(tmp_read_avail), K(ret));
        } else {
          LOG_DEBUG("move decompressed compressed ob20 buffer to reader", K(need_receive_completed), K(result));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ANALYZE_DONE == result.status_) {
    if (resp.get_analyze_result().is_resp_completed()) {
      if (resp.get_analyze_result().is_eof_resp()
          || ((OB_MYSQL_COM_STMT_PREPARE == result_.get_cmd() || OB_MYSQL_COM_STMT_PREPARE_EXECUTE == result_.get_cmd())
              && !resp.get_analyze_result().is_error_resp())) {
        resp.get_analyze_result().is_resultset_resp_ = true;
      }
    } else {
      ObMysqlAnalyzeResult mysql_result;
      if (OB_FAIL(ObProto20Utils::analyze_first_mysql_packet(*normal_ob20_buf_reader, dynamic_cast<ObMysqlCompressedOB20AnalyzeResult&>(result), mysql_result))) {
        LOG_WDIAG("fail to analyze packet", K(normal_ob20_buf_reader), K(ret));
      } else {
        // if it is result + eof + error + ok, it may be not....
        // treat multi stmt as result set protocol
        resp.get_analyze_result().is_resultset_resp_ = ((OB_MYSQL_COM_QUERY == result_.get_cmd()
                                                         || OB_MYSQL_COM_STMT_EXECUTE == result_.get_cmd()
                                                         || OB_MYSQL_COM_STMT_FETCH == result_.get_cmd())
                                                        && (OB_MYSQL_COM_STATISTICS != result_.get_cmd())
                                                        && (MYSQL_OK_PACKET_TYPE != mysql_result.meta_.pkt_type_)
                                                        && (MYSQL_ERR_PACKET_TYPE != mysql_result.meta_.pkt_type_)
                                                        && (MYSQL_EOF_PACKET_TYPE != mysql_result.meta_.pkt_type_)
                                                        && (MYSQL_LOCAL_INFILE_TYPE != mysql_result.meta_.pkt_type_))
                                                       || OB_MYSQL_COM_STMT_PREPARE == result_.get_cmd()
                                                       || OB_MYSQL_COM_STMT_PREPARE_EXECUTE == result_.get_cmd()
                                                       || OB_MYSQL_COM_FIELD_LIST == result_.get_cmd();
      }
    }

    LOG_DEBUG("analyze OB20 first response finished", K(result),
              "is_resultset_resp", resp.get_analyze_result().is_resultset_resp_);
  } else if (ANALYZE_CONT == result.status_ && is_analyze_compressed_ob20_) {
    // if ANALYZE_CONT then reader wouldn't call consume
    // and analyze_first_response will be called again with more data
    // so we need to clear the analyzer's buffer to prepare for the next calling
    MEMSET(&compressed_ob20_, 0, sizeof(compressed_ob20_));
  }
  if (ob20_decompress_buffer_ != NULL) {
    free_miobuffer(ob20_decompress_buffer_);
    ob20_decompress_buffer_ = NULL;
    decompressed_buf_reader = NULL;

  }

  return ret;
}

// called by ObMysqlTransact::do_handle_prepare_response
int ObMysqlCompressOB20Analyzer::analyze_first_response(
    ObIOBufferReader &reader,
    ObMysqlCompressedAnalyzeResult &result)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(is_inited_), K(ret));
  } else {
    ObIOBufferReader *normal_ob20_buf_reader = &reader;
    ObIOBufferReader *decompressed_buf_reader = NULL;

    bool has_data = true;
    if (is_analyze_compressed_ob20_) {
      if (OB_ISNULL(ob20_decompress_buffer_) && OB_ISNULL(ob20_decompress_buffer_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("ob20_decompress_buffer_ is NULL", K(ret));
      } else if (OB_FALSE_IT(ob20_decompress_buffer_->reset())) {
      } else if (OB_ISNULL(decompressed_buf_reader = ob20_decompress_buffer_->alloc_reader())) {
        LOG_WDIAG("fail to alloc reader", K(ob20_decompress_buffer_));
      } else if (OB_FAIL(decompress_compressed_ob20(reader, *ob20_decompress_buffer_))) {
        LOG_WDIAG("fail to do decompress_compressed_ob20", K(ret));
      } else {
        normal_ob20_buf_reader = decompressed_buf_reader;
        has_data = normal_ob20_buf_reader->read_avail() > 0;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!has_data) {
      LOG_DEBUG("no data, got the compress header or tailer");
    } else if (OB_FAIL(ObMysqlCompressAnalyzer::analyze_first_response(*normal_ob20_buf_reader, result))) {
      LOG_WARN("fail to do analyze_first_response", K(ret));
    }

    if (ob20_decompress_buffer_ != NULL) {
      free_miobuffer(ob20_decompress_buffer_);
      ob20_decompress_buffer_ = NULL;
      decompressed_buf_reader = NULL;
    }
  }
  return ret;
}

int ObMysqlCompressOB20Analyzer::analyze_compress_packet_payload(ObIOBufferReader &reader,
                                                                 ObMysqlCompressedAnalyzeResult &result)
{
  int ret = OB_SUCCESS;

  ObMysqlCompressedOB20AnalyzeResult &ob20_result = dynamic_cast<ObMysqlCompressedOB20AnalyzeResult&>(result);
  if (!ob20_result.ob20_header_.flag_.is_extra_info_exist()) {
    LOG_DEBUG("no extra info flag in ob20 head");
  } else {
    ObIOBufferBlock *block = NULL;
    int64_t offset = 0;
    char *data = NULL;
    int64_t data_size = 0;
    if (NULL != reader.block_) {
      reader.skip_empty_blocks();
      block = reader.block_;
      offset = reader.start_offset_;
      data = block->start() + offset;
      data_size = block->read_avail() - offset;
    }

    if (data_size <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("the first block data_size in reader is less than 0", K(offset), K(block->read_avail()), K(ret));
    }

    ob20_req_analyze_state_ = OB20_REQ_ANALYZE_HEAD;    // begin
    ObString req_buf;

    while (OB_SUCC(ret) && block != NULL && data_size > 0 && ob20_req_analyze_state_ != OB20_REQ_ANALYZE_END) {
      req_buf.assign_ptr(data, static_cast<int32_t>(data_size));
      if (OB_FAIL(decompress_request_packet(req_buf, ob20_result))) {  //only extra info now
        LOG_WDIAG("fail to decompress request packet", K(ret));
      } else {
        offset = 0;
        block = block->next_;
        if (NULL != block) {
          data = block->start();
          data_size = block->read_avail();
        }
      }
    }
  }

  LOG_DEBUG("analyze ob20 request payload finished", K(ret), K(reader.read_avail()), K(ob20_result));

  return ret;
}

// check the buffer cross different blocks
int ObMysqlCompressOB20Analyzer::decompress_request_packet(ObString &req_buf,
                                                           ObMysqlCompressedOB20AnalyzeResult &ob20_result)
{
  int ret = OB_SUCCESS;

  if (!is_inited_ || req_buf.length() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected argument check", K(ret), K(req_buf));
  } else {
    const char *buf_start = req_buf.ptr();
    int64_t buf_len = req_buf.length();

    while (OB_SUCC(ret) && buf_len > 0 && ob20_req_analyze_state_ != OB20_REQ_ANALYZE_END) {
      switch (ob20_req_analyze_state_) {
        case OB20_REQ_ANALYZE_HEAD: {
          if (OB_FAIL(do_req_head_decode(buf_start, buf_len))) {
            LOG_WDIAG("fail to to req head decode", K(ret));
          }
          break;
        }
        case OB20_REQ_ANALYZE_EXTRA: {
          if (OB_FAIL(do_req_extra_decode(buf_start, buf_len, ob20_result))) {
            LOG_WDIAG("fail to do req extra decode", K(ret));
          }
          break;
        }
        case OB20_REQ_ANALYZE_END: {
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("ob20 analyze decompress req unknown status", K(ret), K(ob20_req_analyze_state_));
        }
      }
    }
  }

  return ret;
}

int ObMysqlCompressOB20Analyzer::do_req_head_decode(const char* &buf_start, int64_t &buf_len)
{
  int ret = OB_SUCCESS;

  if (remain_head_checked_len_ == 0) {
    remain_head_checked_len_ = MYSQL_COMPRESSED_OB20_HEALDER_LENGTH;
  } else if (remain_head_checked_len_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected remain head checked len", K(ret), K(remain_head_checked_len_));
  } else {
    // remain_head_checked_len_ > 0, nothing
  }

  if (OB_SUCC(ret)) {
    if (remain_head_checked_len_ > buf_len) {
      remain_head_checked_len_ -= buf_len;
      buf_start += buf_len;
      buf_len = 0;
    } else {
      buf_start += remain_head_checked_len_;
      buf_len -= remain_head_checked_len_;
      remain_head_checked_len_ = 0;
      ob20_req_analyze_state_ = OB20_REQ_ANALYZE_EXTRA;
    }
  }

  LOG_DEBUG("ob20 do req head decode", K(remain_head_checked_len_), K(buf_len), K(ob20_req_analyze_state_));

  return ret;
}

int ObMysqlCompressOB20Analyzer::do_req_extra_decode(const char *&buf_start,
                                                     int64_t &buf_len,
                                                     ObMysqlCompressedOB20AnalyzeResult &ob20_result)
{
  int ret = OB_SUCCESS;

  if (ob20_result.ob20_header_.flag_.is_extra_info_exist()) {
    // extra len
    if (extra_len_ == 0) {
      uint32_t extra_remain_len = static_cast<uint32_t>(OB20_PROTOCOL_EXTRA_INFO_LENGTH - extra_checked_len_);
      uint32_t extra_header_len = static_cast<uint32_t>(MIN(buf_len, extra_remain_len));

      MEMCPY(temp_buf_ + extra_checked_len_, buf_start, extra_header_len);
      extra_checked_len_ += extra_header_len;
      buf_start += extra_header_len;
      buf_len -= extra_header_len;

      if (extra_checked_len_ == OB20_PROTOCOL_EXTRA_INFO_LENGTH) {
        char *temp_buf = temp_buf_;
        ObMySQLUtil::get_uint4(temp_buf, extra_len_);
        ob20_result.extra_info_.extra_len_ = extra_len_;
        extra_checked_len_ = 0;
        LOG_DEBUG("ob20 do req extra len decode success", K(extra_len_), K(buf_len));
        ob20_result.extra_info_.extra_info_buf_.reset();
      }
    }

    // extra info
    int64_t extra_remain_len = extra_len_ - extra_checked_len_;
    if (extra_len_ > 0 && buf_len > 0 && extra_remain_len > 0) {
      Ob20ExtraInfo &extra_info = ob20_result.extra_info_;
      if (!extra_info.extra_info_buf_.is_inited()
          && OB_FAIL(extra_info.extra_info_buf_.init(extra_len_))) {
        LOG_WDIAG("fail to init buf", K(ret));
      }
      if (OB_SUCC(ret)) {
        uint32_t curr_extra_len = static_cast<uint32_t>(MIN(buf_len, extra_remain_len));
        if (OB_FAIL(extra_info.extra_info_buf_.write(buf_start, curr_extra_len))) {
          LOG_WDIAG("fail to write to buf", K(ret), K(buf_len));
        } else {
          extra_checked_len_ += curr_extra_len;
          buf_start += curr_extra_len;
          buf_len -= curr_extra_len;

          // decode total extra info
          if (extra_checked_len_ == extra_len_) {
            const char *buf = extra_info.extra_info_buf_.ptr();
            const int64_t len = extra_info.extra_info_buf_.len();
            if (ob20_result.ob20_header_.flag_.is_new_extra_info()) {
              if (OB_FAIL(do_new_extra_info_decode(buf, len, extra_info, ob20_result.flt_))) {
                LOG_WDIAG("fail to do new extra info decode", K(ret));
              }
            } else {
              if (OB_FAIL(do_obobj_extra_info_decode(buf, len, extra_info, ob20_result.flt_))) {
                LOG_WDIAG("fail to do req obobj extra info decode", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              ob20_req_analyze_state_ = OB20_REQ_ANALYZE_END;
              LOG_DEBUG("ob20 req analyzer analyzed finished");
            }
          } // decode total extra info
        }
      }
    }
  } else {
    ob20_req_analyze_state_ = OB20_REQ_ANALYZE_END;
    LOG_DEBUG("no extra info flag, set to analyze end");
  }

  return ret;
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
