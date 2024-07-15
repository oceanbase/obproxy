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
#define USING_LOG_PREFIX PROXY
#include "proxy/mysqllib/ob_resp_analyzer_util.h"
#include "proxy/mysqllib/ob_mysql_analyzer_utils.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "utils/ob_zlib_stream_compressor.h"
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace oceanbase::common;

int ObRespAnalyzerUtil::analyze_one_compressed_ob20_packet(
    event::ObIOBufferReader &reader,
    ObAnalyzeHeaderResult &result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMysqlAnalyzerUtils::analyze_one_compressed_packet(reader, result))) {
    LOG_WDIAG("fail to analyze_one_compressed_packet", K(ret));
  } else if (result.status_ == ANALYZE_DONE) {
    event::ObIOBufferReader *tmp_reader = reader.clone();
    if (OB_NOT_NULL(tmp_reader)) {
      tmp_reader->consume(MYSQL_COMPRESSED_HEALDER_LENGTH);
      event::ObIOBufferBlock *block = NULL;
      int64_t offset = 0;
      char *data = NULL;
      int64_t data_size = 0;
      if (NULL != tmp_reader->block_) {
        tmp_reader->skip_empty_blocks();
        block = tmp_reader->block_;
        offset = tmp_reader->start_offset_;
        data = block->start() + offset;
        data_size = block->read_avail() - offset;
      }
      char ob20_hdr_buf[OB20_PROTOCOL_HEADER_LENGTH];
      char *start = ob20_hdr_buf;
      char *end = start + OB20_PROTOCOL_HEADER_LENGTH;
      // decompress
      ObZlibStreamCompressor compressor;
      int64_t filled = 0;
      while (OB_SUCC(ret) && NULL != block && data_size > 0 && start != end) {
        LOG_DEBUG("going to decompress", K(data), K(data_size));
        if (OB_FAIL(compressor.add_decompress_data(data, data_size))) {
          LOG_WDIAG("fail to add_decompress_data", K(data), K(data_size), K(ret));
        } else if (OB_FAIL(compressor.decompress(start, end - start, filled))) {
          LOG_WDIAG("fail to decompress", K(start), K(filled), K(ret));
        } else {
          start += filled;
          block = block->next_;
          if (OB_NOT_NULL(block)) {
            data = block->start();
            data_size = block->read_avail();
          }
        }
      }
      tmp_reader->dealloc();
      tmp_reader = NULL;
      if (start < end) {
        result.status_ = ANALYZE_CONT;
        LOG_DEBUG("need more data to analyze first compressed oceanbase 2.0 header", "need", end - start);
      } else {
        start = ob20_hdr_buf;
        ObMySQLUtil::get_uint2(start, result.ob20_header_.magic_num_);
        ObMySQLUtil::get_uint2(start, result.ob20_header_.version_);
        ObMySQLUtil::get_uint4(start, result.ob20_header_.connection_id_);
        ObMySQLUtil::get_uint3(start, result.ob20_header_.request_id_);
        ObMySQLUtil::get_uint1(start, result.ob20_header_.pkt_seq_);
        ObMySQLUtil::get_uint4(start, result.ob20_header_.payload_len_);
        ObMySQLUtil::get_uint4(start, result.ob20_header_.flag_.flags_);
        ObMySQLUtil::get_uint2(start, result.ob20_header_.reserved_);
        ObMySQLUtil::get_uint2(start, result.ob20_header_.header_checksum_);
        LOG_DEBUG("decode proto20 header succ", K(result.ob20_header_));
        result.status_ = ANALYZE_DONE;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to clone reader", K(tmp_reader), K(ret));
    }
  }
  return ret;
}
int ObRespAnalyzerUtil::receive_next_ok_packet(event::ObIOBufferReader &reader, ObAnalyzeHeaderResult &result)
{
  int ret = OB_SUCCESS;
  event::ObIOBufferReader *tmp_reader = reader.mbuf_->clone_reader(&reader);
  if (OB_ISNULL(tmp_reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to reader", K(tmp_reader), K(ret));
    result.status_ = ANALYZE_ERROR;
  } else {
    ObMysqlAnalyzeResult tmp_result;
    int64_t first_pkt_len = tmp_result.meta_.pkt_len_;
    if (OB_UNLIKELY(tmp_reader->read_avail() < first_pkt_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("buf reader is error", "read_avail", tmp_reader->read_avail(),
               K(first_pkt_len), K(ret));
    } else {
      if (OB_FAIL(tmp_reader->consume(first_pkt_len))) {
        PROXY_TXN_LOG(WDIAG, "fail to consume ", K(first_pkt_len), K(ret));
      } else if (tmp_reader->read_avail() > 0) {
        if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*tmp_reader, tmp_result))) {
          LOG_WDIAG("fail to analyze packet", K(tmp_reader), K(ret));
        }
      } else {
        tmp_result.status_ = ANALYZE_CONT;
      }
    }
    result.mysql_header_ = tmp_result.meta_;
    result.status_ = tmp_result.status_;
    reader.mbuf_->dealloc_reader(tmp_reader);
  }
  return ret;
}

int ObRespAnalyzerUtil::do_obobj_extra_info_decode(
    const char *buf,
    const int64_t len,
    Ob20ExtraInfo &extra_info,
    common::FLTObjManage &flt_manage)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("invalid argument", KP(buf), K(ret));
  }
  int64_t pos = 0;
  while (OB_SUCC(ret) && pos < len) {
    common::ObObj key;
    common::ObObj value;
    if (OB_FAIL(key.deserialize(buf, len, pos))) {
      LOG_WDIAG("fail to deserialize extra info", K(ret));
    } else if (!key.is_varchar()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid extra info key type", K(key), K(ret));
    } else if (OB_FAIL(value.deserialize(buf, len, pos))) {
      LOG_WDIAG("fail to deserialize extra info", K(ret));
    } else if (!value.is_varchar()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid extra info value type", K(key), K(value), K(ret));
    } else {
      LOG_DEBUG("deserialize extra info", K(key), K(value));

      if (0 == key.get_string().case_compare(OB_V20_PRO_EXTRA_KV_NAME_SYNC_SESSION_INFO)) {
        const char *value_ptr = value.get_string().ptr();
        const int64_t value_len = value.get_string().length();
        if (OB_FAIL(extra_info.add_sess_info_buf(value_ptr, value_len))) {
          LOG_WDIAG("fail to write sess info to buf", K(value), K(ret));
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
          LOG_WDIAG("unexpected pos and length check", K(full_pos), K(full_trc), K(ret));
        } else {
          LOG_DEBUG("succ to deserialize obobj extra info", K(flt_manage));
        }
      } else {
        LOG_DEBUG("attention: do no recognize such an extra info key", K(key), K(ret));
      }
    }            
  } // while

  return ret;
}

int ObRespAnalyzerUtil::do_new_extra_info_decode(
    const char *buf,
    const int64_t len,
    Ob20ExtraInfo &extra_info,
    common::FLTObjManage &flt_manage)
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("invalid argument", KP(buf), K(ret));
  }
  while (OB_SUCC(ret) && pos < len) {
    int16_t key_type = OB20_SVR_END;
    int32_t key_len = 0;
    if (OB_FAIL(common::Ob20FullLinkTraceTransUtil::resolve_type_and_len(buf, len, pos, key_type, key_len))) {
      LOG_WDIAG("fail to resolve type and len for new extra info", K(ret));
    } else {
      Ob20NewExtraInfoProtocolKeyType type = static_cast<Ob20NewExtraInfoProtocolKeyType>(key_type);
      if (type == SESS_INFO) {
        if (OB_FAIL(extra_info.add_sess_info_buf(buf+pos, key_len))) {
          LOG_WDIAG("fail to write sess info buf", K(key_len), K(ret));
        }
      } else if (type == FULL_TRC) {
        int64_t full_pos = 0;
        if (OB_FAIL(flt_manage.deserialize(buf + pos, key_len, full_pos))) {
          LOG_WDIAG("fail to deserialize FLT", K(ret));
        } else if (full_pos != key_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected pos and length check", K(full_pos), K(ret));
        } else {
          LOG_DEBUG("succ to deserialize new extra info", K(flt_manage));
        }
      } else if (type == FEEDBACK_PROXY_INFO) {
        extra_info.is_exist_feedback_proxy_info_ = true;
        LOG_DEBUG("get feedback info from observer", K(pos), K(len), K(key_len), K(type));
        if (OB_FAIL(extra_info.decode_feedback_proxy_info(buf + pos, key_len))) {
          LOG_WDIAG("fail to decode feedback info", K(ret), K(key_len));
        }
      } else {
        LOG_DEBUG("unexpected new extra info type, ignore", K(type), K(key_len));
      }
      pos += key_len;     // pos offset at last
    }
  } // while

  return ret;
}

const char *ObRespAnalyzerUtil::get_stream_state_str(ObRespPktAnalyzerState state)
{
  const char *ret = NULL;
  switch (state)
  {
  case STREAM_INVALID:
    ret = "STREAM_INVALID";
    break;

  case STREAM_MYSQL_HEADER:
    ret = "STREAM_MYSQL_HEADER";
    break;

  case STREAM_MYSQL_TYPE:
    ret = "STREAM_MYSQL_TYPE";
    break;

  case STREAM_MYSQL_BODY:
    ret = "STREAM_MYSQL_BODY";
    break;

  case STREAM_MYSQL_END:
    ret = "STREAM_MYSQL_END";
    break;

  case STREAM_COMPRESSED_MYSQL_HEADER:
    ret = "STREAM_COMPRESSED_MYSQL_HEADER";
    break;

  case STREAM_COMPRESSED_MYSQL_PAYLOAD:
    ret = "STREAM_COMPRESSED_MYSQL_PAYLOAD";
    break;

  case STREAM_COMPRESSED_OCEANBASE_PAYLOAD:
    ret = "STREAM_COMPRESSED_OCEANBASE_PAYLOAD";
    break;

  case STREAM_COMPRESSED_END:
    ret = "STREAM_COMPRESSED_END";
    break;

  case STREAM_OCEANBASE20_HEADER:
    ret = "STREAM_OCEANBASE20_HEADER";
    break;

  case STREAM_OCEANBASE20_EXTRA_INFO_HEADER:
    ret = "STREAM_OCEANBASE20_EXTRA_INFO_HEADER";
    break;

  case STREAM_OCEANBASE20_EXTRA_INFO:
    ret = "STREAM_OCEANBASE20_EXTRA_INFO";
    break;

  case STREAM_OCEANBASE20_MYSQL_PAYLOAD:
    ret = "STREAM_OCEANBASE20_MYSQL_PAYLOAD";
    break;

  case STREAM_OCEANBASE20_TAILER:
    ret = "STREAM_OCEANBASE20_TAILER";
    break;

  case STREAM_OCEANBASE20_END:
    ret = "STREAM_OCEANBASE20_END";
    break;

  default:
    ret = "STREAM_UNEXPECTED";
    OB_LOG(WDIAG, "unexpected analyze state", K(state), K(ret));
    break;
  }

  return ret;
}
} // proxy
} // obproxy
} // oceanbase
