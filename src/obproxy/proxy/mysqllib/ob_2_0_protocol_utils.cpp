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
#include "rpc/obmysql/packet/ompk_ok.h"
#include "packet/ob_mysql_packet_reader.h"
#include "proxy/mysqllib/ob_2_0_protocol_utils.h"
#include "proxy/mysqllib/ob_2_0_protocol_struct.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/checksum/ob_crc16.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "proxy/mysqllib/ob_session_field_mgr.h"

using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::packet;
using namespace oceanbase::obmysql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

int ObProto20Utils::analyze_ok_packet_and_get_reroute_info(ObIOBufferReader *reader,
                                                           const int64_t pkt_len,
                                                           const ObMySQLCapabilityFlags &cap,
                                                           ObProxyRerouteInfo &reroute_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(reader) ||  pkt_len < OB_SIMPLE_OK_PKT_LEN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(reader), K(pkt_len), K(ret));
  } else {
    ObMysqlPacketReader pkt_reader;
    OMPKOK src_ok;
    int64_t offset = reader->read_avail() - pkt_len;
    LOG_DEBUG("analyze_extra_ok_packet_and_get_reroute_info", K(reader->read_avail()), K(pkt_len));

    pkt_reader.get_ok_packet(*reader, offset, cap, src_ok);
    const ObIArray<ObStringKV> &sys_var = src_ok.get_user_vars();
    if (!sys_var.empty()) {
      for (int64_t i = 0; i < sys_var.count() && OB_SUCC(ret); ++i) {
        const ObStringKV &str_kv = sys_var.at(i);
        if (ObSessionFieldMgr::is_client_reroute_info_variable(str_kv.key_)) {
          int64_t pos = 0;
          reroute_info.deserialize_struct(str_kv.value_.ptr(), str_kv.value_.length(), pos);
        }
      }
    }
  }

  return ret;
}

int ObProto20Utils::analyze_fisrt_mysql_packet(
    ObIOBufferReader &reader,
    ObMysqlCompressedOB20AnalyzeResult &result,
    ObMysqlAnalyzeResult &mysql_result)
{
  int ret = OB_SUCCESS;
  char mysql_hdr[MYSQL_NET_META_LENGTH];
  uint32_t extra_len = 0;

  if (result.ob20_header_.flag_.is_extra_info_exist()) {
    uint32_t extra_len;
    char extro_info[OB20_PROTOCOL_EXTRA_INFO_LENGTH];
    char *written_pos = reader.copy(extro_info, OB20_PROTOCOL_EXTRA_INFO_LENGTH, MYSQL_COMPRESSED_OB20_HEALDER_LENGTH);

    if (OB_UNLIKELY(written_pos != extro_info + OB20_PROTOCOL_EXTRA_INFO_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not copy completely", KP(written_pos), K(extro_info),
                 "header_length", OB20_PROTOCOL_EXTRA_INFO_LENGTH, K(ret));
    } else {
      char *extro_info_ptr = extro_info;
      ObMySQLUtil::get_uint4(extro_info_ptr, extra_len);
      extra_len += OB20_PROTOCOL_EXTRA_INFO_LENGTH;
    }
  }

  if (OB_SUCC(ret)) {
    char *written_pos = reader.copy(mysql_hdr, MYSQL_NET_META_LENGTH, MYSQL_COMPRESSED_OB20_HEALDER_LENGTH + extra_len);
    if (OB_UNLIKELY(written_pos != mysql_hdr + MYSQL_NET_META_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not copy completely", K(written_pos), K(mysql_hdr),
               "meta_length", MYSQL_NET_META_LENGTH, K(ret));
    } else {
      if (OB_FAIL(ObProxyParserUtils::analyze_mysql_packet_meta(mysql_hdr, MYSQL_NET_META_LENGTH, mysql_result.meta_))) {
        LOG_WARN("fail to analyze mysql packet meta", K(ret));
      }
    }
  }

  return ret;
}

int ObProto20Utils::analyze_one_compressed_packet(
    ObIOBufferReader &reader,
    ObMysqlCompressedOB20AnalyzeResult &result)
{
  int ret = OB_SUCCESS;
  int64_t len = reader.read_avail();

  result.status_ = ANALYZE_CONT;
  // just consider the condition the compressed mysql header cross two buffer block
  if (OB_LIKELY(len >= MYSQL_COMPRESSED_OB20_HEALDER_LENGTH)) {
    int64_t block_len = reader.block_read_avail();
    char *buf_start = reader.start();

    char mysql_hdr[MYSQL_COMPRESSED_OB20_HEALDER_LENGTH];
    if (OB_UNLIKELY(block_len < MYSQL_COMPRESSED_OB20_HEALDER_LENGTH)) {
      char *written_pos = reader.copy(mysql_hdr, MYSQL_COMPRESSED_OB20_HEALDER_LENGTH, 0);
      if (OB_UNLIKELY(written_pos != mysql_hdr + MYSQL_COMPRESSED_OB20_HEALDER_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not copy completely", KP(written_pos), K(mysql_hdr),
                 "header_length", MYSQL_COMPRESSED_OB20_HEALDER_LENGTH, K(ret));
      } else {
        buf_start = mysql_hdr;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(analyze_compressed_packet_header(buf_start, MYSQL_COMPRESSED_OB20_HEALDER_LENGTH, result))) {
        LOG_WARN("fail to analyze compressed packet header", K(ret));
      } else {
        if (len >= (result.header_.compressed_len_ + MYSQL_COMPRESSED_HEALDER_LENGTH)) {
          result.status_ = ANALYZE_DONE;
          result.is_checksum_on_ = result.header_.is_compressed_payload();
          LOG_DEBUG("analyze one compressed packet succ", "data len", len, K(result));
        }
      }
    }
  }
  return ret;
}

int ObProto20Utils::analyze_compressed_packet_header(
    const char *start, const int64_t len,
    ObMysqlCompressedOB20AnalyzeResult &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(start) || len < MYSQL_COMPRESSED_OB20_HEALDER_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", KP(start), K(len), K(ret));
  } else {
    // 1. decode mysql compress header
    uint32_t pktlen = 0;
    uint8_t pktseq = 0;
    uint32_t pktlen_before_compress = 0; // here, must be 0
    ObMySQLUtil::get_uint3(start, pktlen);
    ObMySQLUtil::get_uint1(start, pktseq);
    ObMySQLUtil::get_uint3(start, pktlen_before_compress);

    result.ob20_header_.cp_hdr_.compressed_len_ = pktlen;
    result.ob20_header_.cp_hdr_.seq_ = pktseq;
    result.ob20_header_.cp_hdr_.non_compressed_len_ = pktlen_before_compress;

    result.header_.compressed_len_ = pktlen;
    result.header_.seq_ = pktseq;
    result.header_.non_compressed_len_ = pktlen_before_compress;

    // 2. decode proto2.0 header
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
  }
  return ret;
}

inline int ObProto20Utils::reserve_proto20_hdr(ObMIOBuffer *write_buf, char *&hdr_start)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(write_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("write buf is NULL", K(ret));
  } else {
    //include compress header and ob20 header
    int64_t header_len = MYSQL_COMPRESSED_HEALDER_LENGTH + OB20_PROTOCOL_HEADER_LENGTH;
    if (OB_FAIL(write_buf->reserve_successive_buf(header_len))) {
      LOG_WARN("fail to reserve successive buf", K(write_buf), K(header_len), K(ret));
    } else if (write_buf->block_write_avail() < header_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("after reserve successive buf, must has enough space", K(header_len),
               "block_write_avail", write_buf->block_write_avail(), "current_write_avail",
               write_buf->current_write_avail(), K(ret));
    } else if (OB_ISNULL(hdr_start = write_buf->end())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buf is NULL", K(ret));
      // just fill and reserve
    } else if(OB_FAIL(write_buf->fill(header_len))) {
      LOG_WARN("fail to fill write buf", K(ret));
    }
  }

  return ret;
}

inline int ObProto20Utils::fill_proto20_header(char *hdr_start, const int64_t payload_len,
                                               const uint8_t compressed_seq, const uint8_t packet_seq,
                                               const uint32_t request_id, const uint32_t connid,
                                               const bool is_last_packet, const bool is_need_reroute,
                                               const bool is_extro_info_exist)
{
  int ret = OB_SUCCESS;

  //include compress header and ob20 header
  int64_t header_len = MYSQL_COMPRESSED_HEALDER_LENGTH + OB20_PROTOCOL_HEADER_LENGTH;
  //compress payload
  uint32_t compress_len = static_cast<uint32_t>(OB20_PROTOCOL_HEADER_LENGTH + payload_len + OB20_PROTOCOL_TAILER_LENGTH);
  uint32_t uncompress_len = 0;
  int16_t magic_num = OB20_PROTOCOL_MAGIC_NUM;
  uint16_t version = OB20_PROTOCOL_VERSION_VALUE;
  Ob20ProtocolFlags flag;
  flag.st_flags_.OB_EXTRA_INFO_EXIST = is_extro_info_exist ? 1 : 0;
  flag.st_flags_.OB_IS_LAST_PACKET = is_last_packet ? 1 : 0;
  flag.st_flags_.OB_IS_PROXY_REROUTE = is_need_reroute ? 1 : 0;
  uint16_t reserved = 0;
  uint16_t header_checksum = 0;
  int64_t pos = 0;

  if (OB_UNLIKELY(compress_len > MYSQL_PAYLOAD_MAX_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid compress_len", K(compress_len), K(MYSQL_PAYLOAD_MAX_LENGTH), K(ret));
  } else {
    if (OB_FAIL(ObMySQLUtil::store_int3(hdr_start, header_len, compress_len, pos))) {
      LOG_ERROR("fail to store int3", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(hdr_start, header_len, compressed_seq, pos))) {
      LOG_ERROR("fail to store int1", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int3(hdr_start, header_len, uncompress_len, pos))) {
      LOG_ERROR("fail to store int3", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int2(hdr_start, header_len, magic_num, pos))) {
      LOG_ERROR("fail to store int2", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int2(hdr_start, header_len, version, pos))) {
      LOG_ERROR("fail to store int2", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int4(hdr_start, header_len, connid, pos))) {
      LOG_ERROR("fail to store int4", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int3(hdr_start, header_len, request_id, pos))) {
      LOG_ERROR("fail to store int4", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(hdr_start, header_len, packet_seq, pos))) {
      LOG_ERROR("fail to store int4", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int4(hdr_start, header_len, (uint32_t)(payload_len), pos))) {
      LOG_ERROR("fail to store int4", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int4(hdr_start, header_len, flag.flags_, pos))) {
      LOG_ERROR("fail to store int4", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int2(hdr_start, header_len, reserved, pos))) {
      LOG_ERROR("fail to store int2", K(ret));
    } else {
      // calc header checksum
      header_checksum = ob_crc16(0, reinterpret_cast<uint8_t *>(hdr_start), pos);

      if (OB_FAIL(ObMySQLUtil::store_int2(hdr_start, header_len, header_checksum, pos))) {
        LOG_ERROR("fail to store int2", K(ret));
      } else {
        LOG_DEBUG("fill proto20 header succ", K(compress_len), K(compressed_seq), K(uncompress_len),
                  K(magic_num), K(version), K(connid), K(request_id), K(packet_seq), K(payload_len),
                  K(flag.flags_), K(reserved), K(header_checksum));
      }
    }
  }
  return ret;
}

inline int ObProto20Utils::fill_proto20_extro_info(ObMIOBuffer *write_buf, const ObIArray<ObObJKV> *extro_info,
                                                   int64_t &payload_len, uint64_t &crc64, bool &is_extro_info_exist)
{
  int ret = OB_SUCCESS;

  int64_t max_kv_size = 0;
  int64_t extro_info_len = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < extro_info->count(); i++) {
    const ObObJKV &ob_obj_kv = extro_info->at(i);
    const int64_t key_size = ob_obj_kv.key_.get_serialize_size();
    const int64_t value_size = ob_obj_kv.value_.get_serialize_size();
    extro_info_len += (key_size + value_size);

    if (key_size + value_size > max_kv_size) {
      max_kv_size = key_size + value_size;
    }
  }

  if (OB_LIKELY(extro_info_len > 0)) {
    int64_t pos = 0;
    int64_t written_len = 0;
    char extro_info_len_store[OB20_PROTOCOL_EXTRA_INFO_LENGTH];
    if (OB_FAIL(ObMySQLUtil::store_int4(extro_info_len_store,
                                        OB20_PROTOCOL_EXTRA_INFO_LENGTH,
                                        static_cast<int32_t>(extro_info_len), pos))) {
      LOG_ERROR("fail to store extro_info_len", K(extro_info_len), K(ret));
    } else if (OB_FAIL(write_buf->write(extro_info_len_store, pos, written_len))) {
      LOG_WARN("fail to write extro info len", K(extro_info_len_store), K(ret));
    } else if (OB_UNLIKELY(written_len != pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to write extro info len", K(written_len), K(pos), K(ret));
    } else {
      payload_len += written_len;
      crc64 = ob_crc64(crc64, extro_info_len_store, written_len);
      is_extro_info_exist = true;

      char *obj_buf = NULL;
      if (OB_ISNULL(obj_buf = static_cast<char *>(op_fixed_mem_alloc(max_kv_size)))) {
        LOG_WARN("fail to alloc memory", K(max_kv_size), K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < extro_info->count(); i++) {
          pos = 0;
          const ObObJKV &ob_obj_kv = extro_info->at(i);
          if (OB_FAIL(ob_obj_kv.key_.serialize(obj_buf, max_kv_size, pos))) {
            LOG_WARN("ail to serialize key", K(i), "key", ob_obj_kv.key_, K(ret));
          } else if (OB_FAIL(ob_obj_kv.value_.serialize(obj_buf, max_kv_size, pos))) {
            LOG_WARN("ail to serialize value", K(i), "value", ob_obj_kv.value_, K(ret));
          } else {
            written_len = 0;
            if (OB_FAIL(write_buf->write(obj_buf, pos, written_len))) {
              LOG_WARN("fail to write extro info", K(pos), K(ret));
            } else if (OB_UNLIKELY(written_len != pos)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to write extro info", K(written_len), K(pos), K(ret));
            } else {
              payload_len += written_len;
              crc64 = ob_crc64(crc64, obj_buf, written_len);
            }
          }
        }

        if (OB_NOT_NULL(obj_buf)) {
          op_fixed_mem_free(obj_buf, max_kv_size);
        }
      }
    }
  }

  LOG_DEBUG("fill proto20 extro succ", KPC(extro_info), K(payload_len), K(crc64),
            K(is_extro_info_exist));

  return ret;
}

inline int ObProto20Utils::fill_proto20_payload(ObIOBufferReader *reader, ObMIOBuffer *write_buf,
                                                const int64_t data_len, int64_t &payload_len, uint64_t &crc64)
{
  int ret = OB_SUCCESS;

  int64_t remain_len = data_len;
  payload_len += data_len;

  char *start = NULL;
  int64_t buf_len = 0;
  int64_t block_read_avail = 0;

  while (remain_len > 0 && OB_SUCC(ret)) {
    int64_t written_len = 0;
    start = reader->start();
    block_read_avail = reader->block_read_avail();
    buf_len = (block_read_avail >= remain_len ? remain_len : block_read_avail);
    remain_len -= buf_len;

    crc64 = ob_crc64(crc64, start, buf_len);

    if (OB_FAIL(write_buf->write(start, buf_len, written_len))) {
      LOG_WARN("fail to write uncompress data", K(buf_len), K(ret));
    } else if (OB_UNLIKELY(written_len != buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to write uncompress data", K(written_len), K(buf_len), K(ret));
    } else if (OB_FAIL(reader->consume(buf_len))) {
      LOG_WARN("fail to consume", K(buf_len), K(ret));
    }
  }

  return ret;
}

inline int ObProto20Utils::fill_proto20_tailer(ObMIOBuffer *write_buf, const uint64_t crc64)
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;
  int64_t written_len = 0;

  char crc64_store[OB20_PROTOCOL_TAILER_LENGTH];
  if (OB_FAIL(ObMySQLUtil::store_int4(crc64_store, OB20_PROTOCOL_TAILER_LENGTH,
                                      static_cast<int32_t>(crc64), pos))) {
    LOG_ERROR("fail to store proto20 tailer", K(crc64), K(ret));
  } else if (OB_FAIL(write_buf->write(crc64_store, pos, written_len))) {
    LOG_WARN("fail to write proto20 tailer", K(crc64_store), K(ret));
  } else if (OB_UNLIKELY(written_len != pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to write proto20 tailer", K(written_len), K(pos), K(ret));
  } else {
    LOG_DEBUG("fill proto20 tailer succ", K(crc64));
  }

  return ret;
}

int ObProto20Utils::consume_and_compress_data(ObIOBufferReader *reader, ObMIOBuffer *write_buf,
                                              const int64_t data_len, const uint8_t compressed_seq,
                                              const uint8_t packet_seq, const uint32_t request_id,
                                              const uint32_t connid, const bool is_last_packet,
                                              const bool is_need_reroute,
                                              const ObIArray<ObObJKV> *extro_info)
{
  int ret = OB_SUCCESS;
  uint64_t crc64 = 0;
  char *hdr_start = NULL;
  int64_t payload_len = 0;
  bool is_extro_info_exist = false;

  if (OB_ISNULL(reader) || OB_ISNULL(write_buf) || data_len > reader->read_avail()) {
    ret = OB_INVALID_ARGUMENT;
    int64_t tmp_read_avail = ((NULL == reader) ? 0 : reader->read_avail());
    LOG_ERROR("invalid input value", K(reader), K(write_buf), K(data_len), K(compressed_seq),
              K(packet_seq), K(request_id), K(connid), K(is_last_packet), K(is_need_reroute),
              "read_avail", tmp_read_avail, K(ret));
  } else if (OB_FAIL(reserve_proto20_hdr(write_buf, hdr_start))) {
    LOG_ERROR("fail to reserve proto20 hdr", K(ret));
  } else if (OB_NOT_NULL(extro_info)
             && OB_FAIL(fill_proto20_extro_info(write_buf, extro_info, payload_len, crc64, is_extro_info_exist))) {
    LOG_ERROR("fail to fill proto20 extro info", KPC(extro_info), K(ret));
  } else if (OB_FAIL(fill_proto20_payload(reader, write_buf, data_len, payload_len, crc64))) {
    LOG_ERROR("fail to fill proto20 payload", K(data_len), K(crc64), K(ret));
  } else if (OB_FAIL(fill_proto20_tailer(write_buf, crc64))) {
    LOG_ERROR("fail to fill proto20 tailer", K(crc64), K(ret));
  } else if (OB_FAIL(fill_proto20_header(hdr_start, payload_len, compressed_seq,
                                         packet_seq, request_id, connid,
                                         is_last_packet, is_need_reroute,
                                         is_extro_info_exist))) {
    LOG_ERROR("fail to fill_proto20_header", K(payload_len), K(compressed_seq),
              K(packet_seq), K(request_id), K(connid), K(is_last_packet),
              K(is_need_reroute), K(ret));
  } else {
    LOG_DEBUG("build mysql compress packet with ob20 succ", "origin len", data_len, K(compressed_seq), K(crc64));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
