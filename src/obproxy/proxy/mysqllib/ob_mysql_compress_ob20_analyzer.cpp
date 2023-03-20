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
        const uint32_t sessid)
{
  int ret = OB_SUCCESS;

  ObMysqlCompressAnalyzer::init(last_seq, mode, mysql_cmd, mysql_mode, enable_extra_ok_packet_for_stats,
                                last_ob20_seq, request_id, sessid);
  last_ob20_seq_ = last_ob20_seq;
  request_id_ = request_id;
  sessid_ = sessid;
  LOG_DEBUG("ObMysqlCompressOB20Analyzer init", K(request_id), "request_id_", request_id_);

  result_.set_cmd(mysql_cmd);
  result_.set_mysql_mode(mysql_mode);
  result_.set_enable_extra_ok_packet_for_stats(enable_extra_ok_packet_for_stats);
  analyzer_.set_mysql_mode(mysql_mode);

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
  result_.reset();
  analyzer_.reset();
  curr_compressed_ob20_header_.reset();
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
       K_(curr_compressed_ob20_header));
  J_OBJ_END();
  return pos;
}

int ObMysqlCompressOB20Analyzer::do_analyzer_end(ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  bool is_trans_completed = false;
  bool is_resp_completed = false;
  ObMysqlRespEndingType ending_type = MAX_PACKET_ENDING_TYPE;
  if (!analyzer_.need_wait_more_data()
        && OB_FAIL(result_.is_resp_finished(is_resp_completed, ending_type))) {
      LOG_WARN("fail to check is resp finished", K(ending_type), K(ret));
  } else {
    // this mysql response is complete
    if (is_resp_completed) {
      ObRespTransState state = result_.get_trans_state();
      if (NOT_IN_TRANS_STATE_BY_PARSE == state) {
        is_trans_completed = true;
      }
    }

    // just defense
    if (OB_UNLIKELY(is_trans_completed) && OB_UNLIKELY(!is_resp_completed)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("state error", K(is_trans_completed), K(is_resp_completed), K(ret));
    } else if (OB_UNLIKELY(!is_resp_completed) && OB_UNLIKELY(is_trans_completed)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("state error", K(is_trans_completed), K(is_resp_completed), K(ret));
    }

    LOG_DEBUG("analyze trans response succ", K(is_trans_completed), K(is_resp_completed),
              "mode", result_.get_mysql_mode(), "ObMysqlRespEndingType", ending_type);
    ObRespAnalyzeResult &analyze_result = resp.get_analyze_result();
    if (is_resp_completed) {
      analyze_result.is_trans_completed_ = is_trans_completed;
      analyze_result.is_resp_completed_ = is_resp_completed;
      analyze_result.ending_type_ = ending_type;
      is_stream_finished_ = true;
      LOG_DEBUG("analyze result", K(resp));
    }
  }

  return ret;
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
          LOG_ERROR("body checksum error", K_(crc64), K(payload_checksum), K(ret));
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
    if (OB_FAIL(analyzer_.analyze_mysql_resp(buf_reader, result_, &resp))) {
      LOG_WARN("fail to analyze mysql resp", K(ret));
    } else if (OB_FAIL(out_buffer_->write(payload_start, body_len, filled_len))) {
      LOG_WARN("fail to write uncompressed payload", K(payload_len), K(filled_len), K(ret));
    } else if (OB_UNLIKELY(body_len != filled_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to write uncompressed payload", K(payload_len), K(filled_len), K(ret));
    } else {
      payload_checked_len_ += body_len;
      payload_start += body_len;
      payload_len -= body_len;
      resp.get_analyze_result().reserved_len_for_ob20_ok_ = result_.get_reserved_len();
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
          LOG_WARN("fail int alloc mem", K(extra_len_), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(extra_info.extra_info_buf_.write(payload_start, extra_len))) {
          LOG_WARN("fail to write", K(extra_len), K(ret));
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
              LOG_WARN("fail to do resp new extra info decode", K(ret));
            }
          } else {
            if (OB_FAIL(do_obobj_extra_info_decode(extra_info.extra_info_buf_.ptr(),
                                                   extra_info.extra_info_buf_.len(),
                                                   extra_info,
                                                   resp.get_analyze_result().flt_))) {
              LOG_WARN("fail to do resp obobj extra info decode", K(ret));
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
      LOG_WARN("fail to deserialize extra info", K(ret));
    } else if (!key.is_varchar()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid extra info key type", K(ret), K(key));
    } else if (OB_FAIL(value.deserialize(buf, len, pos))) {
      LOG_WARN("fail to deserialize extra info", K(ret));
    } else if (!value.is_varchar()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid extra info value type", K(ret), K(key), K(value));
    } else {
      LOG_DEBUG("deserialize extra info", K(key), K(value));

      if (0 == key.get_string().case_compare(OB_V20_PRO_EXTRA_KV_NAME_SYNC_SESSION_INFO)) {
        const char *value_ptr = value.get_string().ptr();
        const int64_t value_len = value.get_string().length();
        if (OB_FAIL(extra_info.add_sess_info_buf(value_ptr, value_len))) {
          LOG_WARN("fail to write sess info to buf", K(ret), K(value));
        }
      } else if (0 == key.get_string().case_compare(OB_V20_PRO_EXTRA_KV_NAME_FULL_LINK_TRACE)) {
        int64_t full_pos = 0;
        ObString full_trc = value.get_string();
        const char *full_trc_buf = full_trc.ptr();
        const int64_t full_trc_len = full_trc.length();
        if (OB_FAIL(flt_manage.deserialize(full_trc_buf, full_trc_len, full_pos))) {
          LOG_WARN("fail to deserialize FLT", K(ret));
        } else if (full_pos != full_trc_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected pos and length check", K(ret), K(full_pos), K(full_trc));
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
      LOG_WARN("fail to resolve type and len for new extra info", K(ret));
    } else {
      Ob20NewExtraInfoProtocolKeyType type = static_cast<Ob20NewExtraInfoProtocolKeyType>(key_type);
      if (type == SESS_INFO) {
        if (OB_FAIL(extra_info.add_sess_info_buf(buf+pos, key_len))) {
          LOG_WARN("fail to write sess info buf", K(ret), K(key_len));
        }
      } else if (type == FULL_TRC) {
        int64_t full_pos = 0;
        if (OB_FAIL(flt_manage.deserialize(buf + pos, key_len, full_pos))) {
          LOG_WARN("fail to deserialize FLT", K(ret));
        } else if (full_pos != key_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected pos and length check", K(ret), K(full_pos));
        } else {
          LOG_DEBUG("succ to deserialize new extra info", K(flt_manage));
        }
      } else {
        LOG_WARN("unexpected new extra info type", K(type), K(key_len));
      }
      pos += key_len;     // pos offset at last
    }
  } // while

  return ret;
}

inline int ObMysqlCompressOB20Analyzer::do_header_checksum(const char *header_start)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(header_start)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid input value", KP(header_start), K(ret));
  } else if (0 == curr_compressed_ob20_header_.header_checksum_) {
    // 0 means skip checksum
  } else {
    // mysql compress header len + proto20 header(except 2 byte checksum)
    int64_t check_len = MYSQL_COMPRESSED_OB20_HEALDER_LENGTH - 2;

    // 3. crc16 for header checksum
    uint16_t local_header_checksum = 0;
    local_header_checksum = ob_crc16(0, reinterpret_cast<const uint8_t *>(header_start), check_len);
    if (local_header_checksum != curr_compressed_ob20_header_.header_checksum_) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("ob 2.0 protocol header checksum error!", K(local_header_checksum),
                K(curr_compressed_ob20_header_.header_checksum_), K(check_len), KP(header_start), K(ret));
    }
  }

  return ret;
}

inline int ObMysqlCompressOB20Analyzer::do_header_decode(const char *start)
{
  int ret = OB_SUCCESS;

  curr_compressed_ob20_header_.reset();

  // 1. decode mysql compress header
  uint32_t pktlen = 0;
  uint8_t pktseq = 0;
  uint32_t pktlen_before_compress = 0; // here, must be 0
  ObMySQLUtil::get_uint3(start, pktlen);
  ObMySQLUtil::get_uint1(start, pktseq);
  ObMySQLUtil::get_uint3(start, pktlen_before_compress);
  curr_compressed_ob20_header_.cp_hdr_.compressed_len_ = pktlen;
  curr_compressed_ob20_header_.cp_hdr_.seq_ = pktseq;
  curr_compressed_ob20_header_.cp_hdr_.non_compressed_len_ = pktlen_before_compress;

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

int ObMysqlCompressOB20Analyzer::decode_compressed_header(const ObString &compressed_data,
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
      LOG_WARN("never happen", K_(header_valid_len), K(curr_compressed_ob20_header_),
               K(avail_len), K(is_last_packet_), K(ret));
      abort();
    }
  }

  if (OB_SUCC(ret) && (header_valid_len_ == MYSQL_COMPRESSED_OB20_HEALDER_LENGTH)) {
    // compress packet header received completely, and start to analyze
    if (OB_FAIL(do_header_decode(header_buffer_start))) {
      LOG_WARN("fail to analyze compressed packet header", K(ret));
      // 3. crc16 for header checksum
    } else if (OB_FAIL(do_header_checksum(header_buffer_start))) {
      LOG_ERROR("fail to do header checksum", K(curr_compressed_ob20_header_), K(ret));
    } else if (OB_UNLIKELY(OB20_PROTOCOL_MAGIC_NUM != curr_compressed_ob20_header_.magic_num_)) {
      ret = OB_UNKNOWN_PACKET;
      LOG_ERROR("invalid magic num", K(OB20_PROTOCOL_MAGIC_NUM),
                K(curr_compressed_ob20_header_.magic_num_), K_(sessid), K(ret));
    } else if (OB_UNLIKELY(sessid_ != curr_compressed_ob20_header_.connection_id_)) {
      ret = OB_UNKNOWN_CONNECTION;
      LOG_ERROR("connection id mismatch", K_(sessid), K_(curr_compressed_ob20_header_.connection_id), K(ret));
    } else if (0 != curr_compressed_ob20_header_.cp_hdr_.non_compressed_len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("pktlen_before_compress must be 0 here", K(curr_compressed_ob20_header_.cp_hdr_.non_compressed_len_),
                K_(sessid), K(ret));
    } else if (OB_UNLIKELY(OB20_PROTOCOL_VERSION_VALUE != curr_compressed_ob20_header_.version_)) {
      ret = OB_UNKNOWN_PACKET;
      LOG_ERROR("invalid version", K(OB20_PROTOCOL_VERSION_VALUE),
                K(curr_compressed_ob20_header_.version_), K_(sessid), K(ret));
    } else if (OB_UNLIKELY(curr_compressed_ob20_header_.cp_hdr_.compressed_len_ !=
              (curr_compressed_ob20_header_.payload_len_ + OB20_PROTOCOL_HEADER_LENGTH + OB20_PROTOCOL_TAILER_LENGTH))) {
      // must only contain one ob20 packet
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid pktlen len", K(origin_len), K(curr_compressed_ob20_header_.payload_len_), K_(sessid),
                K(OB20_PROTOCOL_HEADER_LENGTH), K(OB20_PROTOCOL_TAILER_LENGTH), K(ret));
    } else if (OB_UNLIKELY(request_id_ != curr_compressed_ob20_header_.request_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid request_id", K_(request_id),
                "current request id", curr_compressed_ob20_header_.request_id_,
                K_(curr_compressed_ob20_header), K(ret));
    } else if (OB_UNLIKELY(curr_compressed_ob20_header_.pkt_seq_ != next_pkt_seq)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid pkg seq", K_(last_ob20_seq),
                "current pkg seq", curr_compressed_ob20_header_.pkt_seq_,
                K_(curr_compressed_ob20_header), K(ret));
    } else {
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
      remain_len_ = curr_compressed_ob20_header_.payload_len_ + OB20_PROTOCOL_TAILER_LENGTH;

      LOG_DEBUG("decode compressed header succ", K_(curr_compressed_ob20_header));
    }
  }

  return ret;
}

int ObMysqlCompressOB20Analyzer::analyze_last_compress_packet(
        const char *start, const int64_t len,
        const bool is_last_data, ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  UNUSED(is_last_data);
  if (OB_ISNULL(start) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", KP(start), K(len), K(ret));
  } else if (OB_FAIL(decompress_data(start, len, resp))) {
    LOG_WARN("fail to decompress last mysql packet", K(ret));
  }
  resp.get_analyze_result().is_server_trans_internal_routing_ = curr_compressed_ob20_header_.flag_.is_trans_internal_routing();
  return ret;
}

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
          LOG_ERROR("do extra info decode failed", K(payload_len), K(len), KPC(this), K(ret));
        }
        break;
      }
      case OB20_ANALYZER_PAYLOAD: {
        if (OB_FAIL(do_body_decode(payload_start, payload_len, resp))) {
          LOG_ERROR("do body decode failed", K(payload_len), K(len), KPC(this), K(ret));
        }
        break;
      }
      case OB20_ANALYZER_TAIL: {
        if (OB_FAIL(do_body_checksum(payload_start, payload_len))) {
          LOG_ERROR("do body checksum failed", K(payload_len), K(len), KPC(this), K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ob20 analyzer state is not right", K(payload_len), K(len), KPC(this), K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && OB20_ANALYZER_END == ob20_analyzer_state_
      && OB_FAIL(do_analyzer_end(resp))) {
    LOG_ERROR("do do_ananlyzer_end failed", K(payload_len), K(len), KPC(this), K(ret));
  }

  return ret;
}

int ObMysqlCompressOB20Analyzer::analyze_one_compressed_packet(ObIOBufferReader &reader,
                                                               ObMysqlCompressedAnalyzeResult &result)
{
  return ObProto20Utils::analyze_one_compressed_packet(reader, dynamic_cast<ObMysqlCompressedOB20AnalyzeResult&>(result));
}

bool ObMysqlCompressOB20Analyzer::is_last_packet(const ObMysqlCompressedAnalyzeResult &result)
{
  const ObMysqlCompressedOB20AnalyzeResult& ob20_result = dynamic_cast<const ObMysqlCompressedOB20AnalyzeResult&>(result);
  return ob20_result.ob20_header_.flag_.is_last_packet();
}

int ObMysqlCompressOB20Analyzer::analyze_first_response(
    ObIOBufferReader &reader,
    const bool need_receive_completed,
    ObMysqlCompressedAnalyzeResult &result,
    ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMysqlCompressAnalyzer::analyze_first_response(reader, need_receive_completed, result, resp))) {
    LOG_WARN("analyze first response failed", K(result), K(need_receive_completed), K(ret));
  } else if (ANALYZE_DONE == result.status_) {
    if (resp.get_analyze_result().is_resp_completed()) {
      if (resp.get_analyze_result().is_eof_resp()
          || ((OB_MYSQL_COM_STMT_PREPARE == result_.get_cmd() || OB_MYSQL_COM_STMT_PREPARE_EXECUTE == result_.get_cmd())
              && !resp.get_analyze_result().is_error_resp())) {
        resp.get_analyze_result().is_resultset_resp_ = true;
      }
    } else {
      ObMysqlAnalyzeResult mysql_result;
      if (OB_FAIL(ObProto20Utils::analyze_first_mysql_packet(reader, dynamic_cast<ObMysqlCompressedOB20AnalyzeResult&>(result), mysql_result))) {
        LOG_WARN("fail to analyze packet", K(&reader), K(ret));
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
      LOG_WARN("the first block data_size in reader is less than 0", K(offset), K(block->read_avail()), K(ret));
    }

    ob20_req_analyze_state_ = OB20_REQ_ANALYZE_HEAD;    // begin
    ObString req_buf;
    
    while (OB_SUCC(ret) && block != NULL && data_size > 0 && ob20_req_analyze_state_ != OB20_REQ_ANALYZE_END) {
      req_buf.assign_ptr(data, static_cast<int32_t>(data_size));
      if (OB_FAIL(decompress_request_packet(req_buf, ob20_result))) {  //only extra info now
        LOG_WARN("fail to decompress request packet", K(ret));
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
    LOG_WARN("unexpected argument check", K(ret), K(req_buf));
  } else {
    const char *buf_start = req_buf.ptr();
    int64_t buf_len = req_buf.length();
    
    while (OB_SUCC(ret) && buf_len > 0 && ob20_req_analyze_state_ != OB20_REQ_ANALYZE_END) {
      switch (ob20_req_analyze_state_) {
        case OB20_REQ_ANALYZE_HEAD: {
          if (OB_FAIL(do_req_head_decode(buf_start, buf_len))) {
            LOG_WARN("fail to to req head decode", K(ret));
          }
          break;
        }
        case OB20_REQ_ANALYZE_EXTRA: {
          if (OB_FAIL(do_req_extra_decode(buf_start, buf_len, ob20_result))) {
            LOG_WARN("fail to do req extra decode", K(ret));
          }
          break;
        }
        case OB20_REQ_ANALYZE_END: {
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ob20 analyze decompress req unknown status", K(ret), K(ob20_req_analyze_state_));
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
    LOG_WARN("unexpected remain head checked len", K(ret), K(remain_head_checked_len_));
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
        LOG_WARN("fail to init buf", K(ret));
      }
      if (OB_SUCC(ret)) {
        uint32_t curr_extra_len = static_cast<uint32_t>(MIN(buf_len, extra_remain_len));
        if (OB_FAIL(extra_info.extra_info_buf_.write(buf_start, curr_extra_len))) {
          LOG_WARN("fail to write to buf", K(ret), K(buf_len));
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
                LOG_WARN("fail to do new extra info decode", K(ret));
              }
            } else {
              if (OB_FAIL(do_obobj_extra_info_decode(buf, len, extra_info, ob20_result.flt_))) {
                LOG_WARN("fail to do req obobj extra info decode", K(ret));
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
