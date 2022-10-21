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
#include "obproxy/proxy/mysql/ob_mysql_sm.h"


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

int ObProto20Utils::encode_ok_packet(event::ObMIOBuffer &write_buf, const Ob20ProtocolHeaderParam &ob20_head_param,
                                     uint8_t &seq, const int64_t affected_rows,
                                     const obmysql::ObMySQLCapabilityFlags &capability,
                                     const uint16_t status_flag /* 0 */)
{
  int ret = OB_SUCCESS;

  ObMIOBuffer *tmp_mio_buf = NULL;
  ObIOBufferReader *tmp_mio_reader = NULL;
  if (OB_ISNULL(tmp_mio_buf = new_miobuffer(MYSQL_BUFFER_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new miobuffer", K(ret), K(MYSQL_BUFFER_SIZE));
  } else if (OB_ISNULL(tmp_mio_reader = tmp_mio_buf->alloc_reader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc reader", K(ret));
  } else if (OB_FAIL(ObMysqlPacketUtil::encode_ok_packet(*tmp_mio_buf, seq, affected_rows, capability, status_flag))) {
    LOG_WARN("fail to encode ok packet", K(ret));
  } else if (OB_FAIL(ObProto20Utils::consume_and_compress_data(tmp_mio_reader, &write_buf,
                                                               tmp_mio_reader->read_avail(), ob20_head_param))) {
    LOG_WARN("fail to consume and compress ok packet in ob20", K(ret));
  } else {
    // nothing
  }

  if (OB_LIKELY(tmp_mio_buf != NULL)) {
    free_miobuffer(tmp_mio_buf);
    tmp_mio_buf = NULL;
    tmp_mio_reader = NULL;
  }

  return ret;
}

int ObProto20Utils::encode_kv_resultset(ObMIOBuffer &write_buf, const Ob20ProtocolHeaderParam &ob20_head_param,
                                        uint8_t &seq, const ObMySQLField &field, ObObj &field_value,
                                        const uint16_t status_flag)
{
  int ret = OB_SUCCESS;

  event::ObIOBufferReader *tmp_mio_reader = NULL;
  event::ObMIOBuffer *tmp_mio_buf = NULL;
  if (OB_ISNULL(tmp_mio_buf = new_empty_miobuffer(MYSQL_BUFFER_SIZE))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc miobuffer", K(ret));
  } else if (OB_ISNULL(tmp_mio_reader = tmp_mio_buf->alloc_reader())) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc reader", K(ret));
  } else if (OB_FAIL(ObMysqlPacketUtil::encode_kv_resultset(*tmp_mio_buf, seq, field, field_value, status_flag))) {
    LOG_WARN("fail to encode kv resultset", K(ret));
  } else if (OB_FAIL(ObProto20Utils::consume_and_compress_data(tmp_mio_reader, &write_buf,
                                                               tmp_mio_reader->read_avail(), ob20_head_param))) {
    LOG_WARN("fail to consume and compress kv resultset in ob20 packet", K(ret));
  } else {
    LOG_DEBUG("succ to encode kv resultset in ob20 packet");
  }

  if (OB_LIKELY(tmp_mio_buf != NULL)) {
    free_miobuffer(tmp_mio_buf);
    tmp_mio_buf = NULL;
    tmp_mio_reader = NULL;
  }

  return ret;
}

int ObProto20Utils::encode_err_packet(event::ObMIOBuffer &write_buf, const Ob20ProtocolHeaderParam &ob20_head_param,
                                      uint8_t &seq, const int err_code, const ObString &msg_buf)
{
  int ret = OB_SUCCESS;

  event::ObIOBufferReader *tmp_mio_reader = NULL;
  event::ObMIOBuffer *tmp_mio_buf = NULL;
  
  if (OB_ISNULL(tmp_mio_buf = new_empty_miobuffer(MYSQL_BUFFER_SIZE))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc miobuffer", K(ret));
  } else if (OB_ISNULL(tmp_mio_reader = tmp_mio_buf->alloc_reader())) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc reader", K(ret));
  } else if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet(*tmp_mio_buf, seq, err_code, msg_buf))) {
    LOG_WARN("fail to encode err packet buf", K(ret));
  } else if (OB_FAIL(ObProto20Utils::consume_and_compress_data(tmp_mio_reader, &write_buf,
                                                               tmp_mio_reader->read_avail(), ob20_head_param))) {
    LOG_WARN("fail to consume and compress data for err packet in ob20", K(ret));
  } else {
    LOG_DEBUG("succ to encode err packet in ob20 packet");
  }

  if (OB_LIKELY(tmp_mio_buf != NULL)) {
    free_miobuffer(tmp_mio_buf);
    tmp_mio_buf = NULL;
    tmp_mio_reader = NULL;
  }

  return ret;
}

int ObProto20Utils::encode_executor_response_packet(event::ObMIOBuffer *write_buf,
                                                    const Ob20ProtocolHeaderParam &ob20_head_param,
                                                    uint8_t &seq, engine::ObProxyResultResp *result_resp)
{
  int ret = OB_SUCCESS;

  event::ObIOBufferReader *tmp_mio_reader = NULL;
  event::ObMIOBuffer *tmp_mio_buf = NULL;
  
  if (OB_ISNULL(tmp_mio_buf = new_empty_miobuffer(MYSQL_BUFFER_SIZE))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc miobuffer", K(ret));
  } else if (OB_ISNULL(tmp_mio_reader = tmp_mio_buf->alloc_reader())) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc reader", K(ret));
  } else if (OB_FAIL(ObMysqlPacketUtil::encode_executor_response_packet(write_buf, seq, result_resp))) {
    LOG_WARN("fail to encode err packet buf", K(ret));
  } else if (OB_FAIL(ObProto20Utils::consume_and_compress_data(tmp_mio_reader, write_buf,
                                                               tmp_mio_reader->read_avail(), ob20_head_param))) {
    LOG_WARN("fail to consume and compress data for executor response packet in ob20", K(ret));
  } else {
    LOG_DEBUG("succ to executor response in ob20 packet");
  }

  if (OB_LIKELY(tmp_mio_buf != NULL)) {
    free_miobuffer(tmp_mio_buf);
    tmp_mio_buf = NULL;
    tmp_mio_reader = NULL;
  }
  
  return ret;
}

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

int ObProto20Utils::analyze_first_mysql_packet(
    ObIOBufferReader &reader,
    ObMysqlCompressedOB20AnalyzeResult &result,
    ObMysqlAnalyzeResult &mysql_result)
{
  int ret = OB_SUCCESS;
  char mysql_hdr[MYSQL_NET_META_LENGTH];
  uint32_t extra_len = 0;

  if (result.ob20_header_.flag_.is_extra_info_exist()) {
    uint32_t extra_len;
    char extra_info[OB20_PROTOCOL_EXTRA_INFO_LENGTH];
    char *written_pos = reader.copy(extra_info, OB20_PROTOCOL_EXTRA_INFO_LENGTH, MYSQL_COMPRESSED_OB20_HEALDER_LENGTH);

    if (OB_UNLIKELY(written_pos != extra_info + OB20_PROTOCOL_EXTRA_INFO_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not copy completely", KP(written_pos), K(extra_info),
                 "header_length", OB20_PROTOCOL_EXTRA_INFO_LENGTH, K(ret));
    } else {
      char *extra_info_ptr = extra_info;
      ObMySQLUtil::get_uint4(extra_info_ptr, extra_len);
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
                                               const bool is_extra_info_exist, const bool is_new_extra_info)
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
  flag.st_flags_.OB_EXTRA_INFO_EXIST = is_extra_info_exist ? 1 : 0;
  flag.st_flags_.OB_IS_LAST_PACKET = is_last_packet ? 1 : 0;
  flag.st_flags_.OB_IS_PROXY_REROUTE = is_need_reroute ? 1 : 0;
  flag.st_flags_.OB_IS_NEW_EXTRA_INFO = is_new_extra_info ? 1 : 0;
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
                  K(flag.flags_), K(reserved), K(header_checksum), K(is_last_packet), K(is_extra_info_exist),
                  K(is_new_extra_info), K(lbt()));
      }
    }
  }
  return ret;
}

int ObProto20Utils::fill_proto20_extra_info(ObMIOBuffer *write_buf, const ObIArray<ObObJKV> *extra_info,
                                            const bool is_new_extra_info, int64_t &payload_len,
                                            uint64_t &crc64, bool &is_extra_info_exist)
{
  int ret = OB_SUCCESS;

  if (is_new_extra_info) {
    if (OB_FAIL(fill_proto20_new_extra_info(write_buf, extra_info, payload_len, crc64, is_extra_info_exist))) {
      LOG_WARN("fail to fill new extra info");
    }
  } else {
    if (OB_FAIL(fill_proto20_obobj_extra_info(write_buf, extra_info, payload_len, crc64, is_extra_info_exist))) {
      LOG_WARN("fail to fill obobj extra info");
    }
  }

  return ret;
}

int ObProto20Utils::fill_proto20_obobj_extra_info(ObMIOBuffer *write_buf, const ObIArray<ObObJKV> *extra_info,
                                                  int64_t &payload_len, uint64_t &crc64, bool &is_extra_info_exist)
{
  int ret = OB_SUCCESS;

  int64_t max_kv_size = 0;
  int64_t extra_info_len = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < extra_info->count(); i++) {
    const ObObJKV &ob_obj_kv = extra_info->at(i);
    const int64_t key_size = ob_obj_kv.key_.get_serialize_size();
    const int64_t value_size = ob_obj_kv.value_.get_serialize_size();
    extra_info_len += (key_size + value_size);

    if (key_size + value_size > max_kv_size) {
      max_kv_size = key_size + value_size;
    }
  }
  
  if (OB_LIKELY(extra_info_len > 0)) {
    int64_t pos = 0;
    int64_t written_len = 0;
    char extra_info_len_store[OB20_PROTOCOL_EXTRA_INFO_LENGTH];
    if (OB_FAIL(ObMySQLUtil::store_int4(extra_info_len_store,
                                        OB20_PROTOCOL_EXTRA_INFO_LENGTH,
                                        static_cast<int32_t>(extra_info_len), pos))) {
      LOG_ERROR("fail to store extra_info_len", K(extra_info_len), K(ret));
    } else if (OB_FAIL(write_buf->write(extra_info_len_store, pos, written_len))) {
      LOG_WARN("fail to write extra info len", K(extra_info_len_store), K(ret));
    } else if (OB_UNLIKELY(written_len != pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to write extra info len", K(written_len), K(pos), K(ret));
    } else {
      payload_len += written_len;
      crc64 = ob_crc64(crc64, extra_info_len_store, written_len);
      is_extra_info_exist = true;

      char *obj_buf = NULL;
      if (OB_ISNULL(obj_buf = static_cast<char *>(op_fixed_mem_alloc(max_kv_size)))) {
        LOG_WARN("fail to alloc memory", K(max_kv_size), K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < extra_info->count(); i++) {
          pos = 0;
          const ObObJKV &ob_obj_kv = extra_info->at(i);
          if (OB_FAIL(ob_obj_kv.key_.serialize(obj_buf, max_kv_size, pos))) {
            LOG_WARN("ail to serialize key", K(i), "key", ob_obj_kv.key_, K(ret));
          } else if (OB_FAIL(ob_obj_kv.value_.serialize(obj_buf, max_kv_size, pos))) {
            LOG_WARN("ail to serialize value", K(i), "value", ob_obj_kv.value_, K(ret));
          } else {
            written_len = 0;
            if (OB_FAIL(write_buf->write(obj_buf, pos, written_len))) {
              LOG_WARN("fail to write extra info", K(pos), K(ret));
            } else if (OB_UNLIKELY(written_len != pos)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to write extra info", K(written_len), K(pos), K(ret));
            } else {
              payload_len += written_len;
              crc64 = ob_crc64(crc64, obj_buf, written_len);
              LOG_DEBUG("succ to fill obobj extra info item", K(ob_obj_kv.key_), K(written_len));
            }
          }
        }

        if (OB_NOT_NULL(obj_buf)) {
          op_fixed_mem_free(obj_buf, max_kv_size);
        }
      }
    }
  }

  LOG_DEBUG("fill proto20 obobj extra info end", K(ret), KPC(extra_info),
            K(extra_info_len), K(payload_len), K(crc64), K(is_extra_info_exist));

  return ret;
}

int ObProto20Utils::fill_proto20_new_extra_info(ObMIOBuffer *write_buf, const ObIArray<ObObJKV> *extra_info,
                                                int64_t &payload_len, uint64_t &crc64, bool &is_extra_info_exist)
{
  int ret = OB_SUCCESS;

  int64_t extra_info_len = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < extra_info->count(); i++) {
    const ObObJKV &obobj_kv = extra_info->at(i);
    ObString obj_value = obobj_kv.value_.get_varchar();
    extra_info_len += FLT_TYPE_AND_LEN + obj_value.length();
  }
  
  if (extra_info_len > 0) {
    LOG_DEBUG("will fill new extra info", K(extra_info_len));
    int64_t pos = 0;
    int64_t written_len = 0;
    char extra_info_len_store[OB20_PROTOCOL_EXTRA_INFO_LENGTH];
    if (OB_FAIL(ObMySQLUtil::store_int4(extra_info_len_store,
                                        OB20_PROTOCOL_EXTRA_INFO_LENGTH,
                                        static_cast<int32_t>(extra_info_len), pos))) {
      LOG_ERROR("fail to store extra_info_len", K(extra_info_len), K(ret));
    } else if (OB_FAIL(write_buf->write(extra_info_len_store, pos, written_len))) {
      LOG_WARN("fail to write extra info len", K(extra_info_len_store), K(ret));
    } else if (OB_UNLIKELY(written_len != pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to write extra info len", K(written_len), K(pos), K(ret));
    } else {
      payload_len += written_len;
      crc64 = ob_crc64(crc64, extra_info_len_store, written_len);
      is_extra_info_exist = true;

      for (int64_t i = 0; OB_SUCC(ret) && i < extra_info->count(); ++i) {
        Ob20NewExtraInfoProtocolKeyType key_type = OB20_SVR_END;
        const ObObJKV &obobj_kv = extra_info->at(i);
        ObString obj_key = obobj_kv.key_.get_varchar();
        ObString obj_value = obobj_kv.value_.get_varchar();
        if (obj_key.case_compare(OB_V20_PRO_EXTRA_KV_NAME_FULL_LINK_TRACE) == 0) {
          key_type = FULL_TRC;
        } else if (obj_key.case_compare(OB_V20_PRO_EXTRA_KV_NAME_SYNC_SESSION_INFO) == 0) {
          key_type = SESS_INFO;
        } else if (obj_key.case_compare(OB_TRACE_INFO_VAR_NAME) == 0) {
          key_type = TRACE_INFO;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected extra key error", K(ret), K(obj_key), K(obj_value));
        }

        if (OB_SUCC(ret)) {
          // fill type and value
          char new_extra_info_key_len_buf[FLT_TYPE_AND_LEN];
          int64_t t_pos = 0;
          int64_t type_written_len = 0;
          int64_t value_written_len = 0;
          const char *ptr = obj_value.ptr();
          const int32_t len = obj_value.length();
          if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_type_and_len(
                        new_extra_info_key_len_buf, FLT_TYPE_AND_LEN, t_pos, key_type, len))) {
            LOG_WARN("fail to store type and len for new extra info", K(ret), K(key_type), K(obj_value));
          } else if (OB_FAIL(write_buf->write(new_extra_info_key_len_buf, FLT_TYPE_AND_LEN, type_written_len))) {
            LOG_WARN("fail to write type and len to buf", K(ret));
          } else if (OB_UNLIKELY(FLT_TYPE_AND_LEN != type_written_len)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected check written len", K(ret), K(type_written_len));
          } else if (OB_FAIL(write_buf->write(ptr, len, value_written_len))) {
            LOG_WARN("fail to write obj value to buf", K(ret), K(obj_value));
          } else if (OB_UNLIKELY(len != value_written_len)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected check written len", K(ret), K(len), K(value_written_len));
          } else {
            payload_len += (type_written_len + value_written_len);
            crc64 = ob_crc64(crc64, new_extra_info_key_len_buf, type_written_len);
            crc64 = ob_crc64(crc64, ptr, len);
            LOG_DEBUG("succ to fill new extra info item", K(key_type), K(value_written_len));
          }
        }
      } // for
    } // else
  }

  LOG_DEBUG("fill proto20 new extra info end", K(ret), KPC(extra_info),
            K(extra_info_len), K(payload_len), K(crc64), K(is_extra_info_exist));

  return ret;
}

int ObProto20Utils::fill_proto20_payload(ObIOBufferReader *reader, ObMIOBuffer *write_buf,
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

int ObProto20Utils::fill_proto20_tailer(ObMIOBuffer *write_buf, const uint64_t crc64)
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

int ObProto20Utils::consume_and_compress_data(ObIOBufferReader *reader,
                                              ObMIOBuffer *write_buf,
                                              const int64_t data_len,
                                              const Ob20ProtocolHeaderParam &ob20_head_param,
                                              const ObIArray<ObObJKV> *extra_info)
{
  int ret = OB_SUCCESS;
  uint64_t crc64 = 0;
  char *hdr_start = NULL;
  int64_t payload_len = 0;
  bool is_extra_info_exist = false;

  const uint8_t compressed_seq = ob20_head_param.get_compressed_seq();
  const uint8_t packet_seq = ob20_head_param.get_pkt_seq();
  const uint32_t request_id = ob20_head_param.get_request_id();
  const uint32_t conn_id = ob20_head_param.get_connection_id();
  const bool is_last_packet = ob20_head_param.is_last_packet();
  const bool is_need_reroute = ob20_head_param.is_need_reroute();
  const bool is_new_extra_info = ob20_head_param.is_new_extra_info();

  if (OB_ISNULL(reader) || OB_ISNULL(write_buf) || data_len > reader->read_avail()) {
    ret = OB_INVALID_ARGUMENT;
    int64_t tmp_read_avail = ((NULL == reader) ? 0 : reader->read_avail());
    LOG_ERROR("invalid input value", K(reader), K(write_buf), K(data_len), K(compressed_seq),
              K(packet_seq), K(request_id), K(conn_id), K(is_last_packet), K(is_need_reroute),
              "read_avail", tmp_read_avail, K(ret));
  } else if (OB_FAIL(reserve_proto20_hdr(write_buf, hdr_start))) {
    LOG_ERROR("fail to reserve proto20 hdr", K(ret));
  } else if (OB_NOT_NULL(extra_info)
             && OB_FAIL(fill_proto20_extra_info(write_buf, extra_info, is_new_extra_info,
                                                payload_len, crc64, is_extra_info_exist))) {
    LOG_ERROR("fail to fill proto20 extra info", KPC(extra_info), K(ret));
  } else if (OB_FAIL(fill_proto20_payload(reader, write_buf, data_len, payload_len, crc64))) {
    LOG_ERROR("fail to fill proto20 payload", K(data_len), K(crc64), K(ret));
  } else if (OB_FAIL(fill_proto20_tailer(write_buf, crc64))) {
    LOG_ERROR("fail to fill proto20 tailer", K(crc64), K(ret));
  } else if (OB_FAIL(fill_proto20_header(hdr_start, payload_len, compressed_seq,
                                         packet_seq, request_id, conn_id,
                                         is_last_packet, is_need_reroute,
                                         is_extra_info_exist, is_new_extra_info))) {
    LOG_ERROR("fail to fill_proto20_header", K(payload_len), K(compressed_seq),
              K(packet_seq), K(request_id), K(conn_id), K(is_last_packet),
              K(is_need_reroute), K(ret));
  } else {
    LOG_DEBUG("build mysql compress packet with ob20 succ", "origin len", data_len, K(compressed_seq), K(crc64));
  }
  return ret;
}

int ObProxyTraceUtils::build_client_ip(ObIArray<ObObJKV> &extra_info,
                                       char *buf,
                                       int64_t buf_len,
                                       ObMysqlSM *sm,
                                       const bool is_last_packet_or_segment)
{
  int ret = OB_SUCCESS;

  ObMysqlClientSession *client_session = sm->get_client_session();
  if (!client_session->is_proxy_mysql_client_
      && client_session->is_need_send_trace_info()
      && is_last_packet_or_segment) {
    int64_t pos = 0;
    ObAddr client_ip = client_session->get_real_client_addr();
    
    if (OB_FAIL(ObMySQLUtil::store_str_nzt(buf, buf_len, OB_TRACE_INFO_CLIENT_IP, pos))) {
        LOG_WARN("fail to store client addr", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_str_nzt(buf, buf_len, "=", pos))) {
      LOG_WARN("fail to store equals sign", K(ret));
    } else if (OB_UNLIKELY(!client_ip.ip_to_string(buf + STRLEN(buf),
                                                   static_cast<int32_t>(buf_len - pos)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to ip_to_string", K(client_ip), K(ret));
    } else {
      ObObJKV kv;
      kv.key_.set_varchar(OB_TRACE_INFO_VAR_NAME, static_cast<int32_t>(STRLEN(OB_TRACE_INFO_VAR_NAME)));
      kv.key_.set_default_collation_type();
      kv.value_.set_varchar(buf, static_cast<int32_t>(STRLEN(buf)));
      kv.value_.set_default_collation_type();

      if (OB_FAIL(extra_info.push_back(kv))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
        LOG_DEBUG("succ to generate client ip info");
        client_session->set_already_send_trace_info(true);
      }
    }
  }

  return ret;
}
                                       
int ObProxyTraceUtils::build_sync_sess_info(common::ObIArray<ObObJKV> &extra_info,
                                            common::hash::ObHashMap<int16_t, ObString> &sess_info_hash_map,
                                            common::ObSqlString &info_value)
{
  int ret = OB_SUCCESS;
  ObObJKV ob_sess_info;

  ob_sess_info.key_.set_varchar(OB_V20_PRO_EXTRA_KV_NAME_SYNC_SESSION_INFO,
                                static_cast<int32_t>(STRLEN(OB_V20_PRO_EXTRA_KV_NAME_SYNC_SESSION_INFO)));
  ob_sess_info.key_.set_default_collation_type();

  common::hash::ObHashMap<int16_t, ObString>::iterator last = sess_info_hash_map.end();
  common::hash::ObHashMap<int16_t, ObString>::iterator it = sess_info_hash_map.begin();
  for (; it != last; ++it) {
    info_value.append(it->second);
  }
  ob_sess_info.value_.set_varchar(info_value.string());
  ob_sess_info.value_.set_default_collation_type();

  if (OB_FAIL(extra_info.push_back(ob_sess_info))) {
    LOG_WARN("fail to push back", K(ret));
  }
  return ret;
}

int ObProxyTraceUtils::build_extra_info_for_server(ObMysqlSM *sm,
                                                   char *buf,
                                                   int64_t buf_len,
                                                   common::ObIArray<ObObJKV> &extra_info,
                                                   const bool is_last_packet_or_segment)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  FLTSpanInfo &span_info = sm->flt_.span_info_;
  FLTAppInfo &app_info = sm->flt_.app_info_;

  LOG_DEBUG("before build extra for server", K(is_last_packet_or_segment), K(span_info), K(app_info));

  // proxy could send span_info and app_info to server
  if (sm->get_server_session()->is_full_link_trace_supported()
      && is_last_packet_or_segment
      && sm->enable_record_full_link_trace_info()) {
    // set current child span id from OBTRACE to server
    SET_TRACE_BUFFER(sm->flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
    trace::ObSpanCtx *curr_span_ctx = OBTRACE->last_active_span_;
    if (curr_span_ctx != NULL) {
      INIT_SPAN(curr_span_ctx);         // before send to observer, init span first
      trace::UUID &curr_span_id = curr_span_ctx->span_id_;
      span_info.span_id_ = curr_span_id;
      LOG_DEBUG("set the latest span id as current span id", K(curr_span_id));
    } else {
      // unexpected here
      ret = OB_ERR_UNEXPECTED;
      FLTTraceLogInfo &trace_log_info = sm->flt_.trace_log_info_;
      LOG_WARN("unexpected empty current span ctx, plz check!", K(ret), K(span_info), K(trace_log_info));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(span_info.serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize span info for server", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)
      && sm->get_server_session()->is_ob_protocol_v2_supported()
      && app_info.is_valid()) {
    if (OB_FAIL(app_info.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize app info for server", K(ret));
    }
  }

  int32_t total_seri = static_cast<int32_t>(pos);
  if (OB_SUCC(ret) && total_seri > 0) {
    ObObJKV kv;
    kv.key_.set_varchar(OB_V20_PRO_EXTRA_KV_NAME_FULL_LINK_TRACE,
                        static_cast<int32_t>(STRLEN(OB_V20_PRO_EXTRA_KV_NAME_FULL_LINK_TRACE)));
    kv.key_.set_default_collation_type();
    kv.value_.set_varchar(buf, total_seri);
    kv.value_.set_default_collation_type();

    if (OB_FAIL(extra_info.push_back(kv))) {
      LOG_WARN("fail to push back to extra info", K(ret));
    } else {
      if (app_info.is_valid()) {
        app_info.reset();         // only send once if exist
      }
      LOG_DEBUG("succ to serialize extra info for server", K(kv.key_), K(pos), K(total_seri));
    }
  }
  
  return ret;
}

int ObProxyTraceUtils::build_extra_info_for_client(ObMysqlSM *sm,
                                                   char *buf,
                                                   const int64_t len,
                                                   common::ObIArray<ObObJKV> &extra_info)
{
  int ret = OB_SUCCESS;
  
  int64_t pos = 0;
  FLTControlInfo &control_info = sm->flt_.control_info_;
  if (control_info.is_need_send()) {
    if (sm->get_client_session()->is_client_support_full_link_trace()) {
      if (OB_FAIL(control_info.serialize(buf, len, pos))) {
        LOG_WARN("fail to serialize control info", K(ret));
      }
    }
    // do not send control info to client again, keep it
    // the client could support flt or not, so reset here
    control_info.set_need_send(false);        
  }

  if (OB_SUCC(ret) && pos > 0) {
    ObObJKV kv;
    kv.key_.set_varchar(OB_V20_PRO_EXTRA_KV_NAME_FULL_LINK_TRACE,
                        static_cast<int32_t>(STRLEN(OB_V20_PRO_EXTRA_KV_NAME_FULL_LINK_TRACE)));
    kv.key_.set_default_collation_type();
    kv.value_.set_varchar(buf, static_cast<int32_t>(pos));
    kv.value_.set_default_collation_type();

    if (OB_FAIL(extra_info.push_back(kv))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
      LOG_DEBUG("succ to generate extra info back to client", K(pos), K(extra_info));
    }
  }

  return ret;
}

int ObProxyTraceUtils::build_related_extra_info_all(ObIArray<ObObJKV> &extra_info, ObMysqlSM *sm,
                                                    char *ip_buf, const int64_t ip_buf_len,
                                                    char *extra_info_buf, const int64_t extra_info_buf_len,
                                                    ObSqlString &sess_info_value, const bool is_last_packet)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObProxyTraceUtils::build_client_ip(extra_info, ip_buf, ip_buf_len, sm, is_last_packet))) {
    LOG_WARN("fail to build client ip", K(ret));
  } else if (OB_FAIL(ObProxyTraceUtils::build_extra_info_for_server(sm, extra_info_buf, extra_info_buf_len,
                                                                    extra_info, is_last_packet))) {
    LOG_WARN("fail to build extra info for server");
  } else {
    ObMysqlServerSession *server_session = sm->get_server_session();
    ObMysqlClientSession *client_session = sm->get_client_session();
    if (OB_SUCC(ret) && OB_NOT_NULL(client_session) && OB_NOT_NULL(server_session)) {
      ObClientSessionInfo &client_info = client_session->get_session_info();
      ObServerSessionInfo &server_info = server_session->get_session_info();
      if (!client_session->is_proxy_mysql_client_
          && client_info.need_reset_sess_info_vars(server_info)
          && is_last_packet) {
        SessFieldHashMap& sess_info_hash_map = client_info.get_sess_info_map();
        if (OB_FAIL(ObProxyTraceUtils::build_sync_sess_info(extra_info, sess_info_hash_map, sess_info_value))) {
          LOG_WARN("fail to build sync sess info", K(ret));
        }
      }
    }
  }
  
  return ret;
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
