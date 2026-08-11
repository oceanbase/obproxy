/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "proxy/mysqllib/ob_cdc_dump_packet.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/charset/ob_mysql_global.h"
#include "rpc/obmysql/ob_mysql_util.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;

const int64_t ObCdcDumpPacket::CDC_DUMP_FIXED_HEADER_LEN = 55;
const int64_t ObCdcDumpPacket::CDC_DUMP_MAX_FIELD_LEN = 512;
const int64_t ObCdcDumpPacket::CDC_DUMP_MAX_PAYLOAD_LEN = 0xFFFF;

ObCdcDumpPacket::ObCdcDumpPacket()
{
  reset();
}

void ObCdcDumpPacket::reset()
{
  is_parse_completed_ = false;
  total_len_ = 0;
  flags_ = 0;
  channel_id_ = 0;
  commit_version_ = 0;
  txid_ = 0;
  txseq_ = 0;
  tenant_id_ = 0;
  header_len_ = 0;
  client_id_offset_ = 0;
  client_id_len_ = 0;
  token_offset_ = 0;
  token_len_ = 0;
  stream_name_offset_ = 0;
  stream_name_len_ = 0;
  client_id_.reset();
  token_.reset();
  stream_name_.reset();
}

bool ObCdcDumpPacket::is_valid() const
{
  return (header_len_ == CDC_DUMP_FIXED_HEADER_LEN)
         && (total_len_ <= CDC_DUMP_MAX_PAYLOAD_LEN);
}

int ObCdcDumpPacket::assign_var_fields(const char *payload)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(payload) || OB_UNLIKELY(total_len_ < header_len_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid payload for cdc dump var fields", K(total_len_), K_(header_len), K(ret));
  } else if (OB_UNLIKELY(client_id_len_ > CDC_DUMP_MAX_FIELD_LEN
                         || token_len_ > CDC_DUMP_MAX_FIELD_LEN
                         || stream_name_len_ > CDC_DUMP_MAX_FIELD_LEN)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("cdc dump var field too long", K_(client_id_len), K_(token_len),
             K_(stream_name_len), K(ret));
  } else if (OB_UNLIKELY(client_id_offset_ + client_id_len_ > total_len_
                         || token_offset_ + token_len_ > total_len_
                         || stream_name_offset_ + stream_name_len_ > total_len_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("cdc dump var field out of range", K(total_len_), K_(client_id_offset),
             K_(client_id_len), K_(token_offset), K_(token_len),
             K_(stream_name_offset), K_(stream_name_len), K(ret));
  } else if (OB_FAIL(client_id_.rewrite(payload + client_id_offset_, client_id_len_))) {
    LOG_WDIAG("fail to write client_id", K_(client_id_len));
  } else if (OB_FAIL(token_.rewrite(payload + token_offset_, token_len_))) {
    LOG_WDIAG("fail to write token_", K_(token_len));
  } else if (OB_FAIL(stream_name_.rewrite(payload + stream_name_offset_, stream_name_len_))) {
    LOG_WDIAG("fail to write stream_name_", K_(stream_name_len));
  }

  return ret;
}

int64_t ObCdcDumpPacket::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_parse_completed), K_(total_len), K_(flags), K_(channel_id),
       K_(commit_version), K_(txid), K_(txseq), K_(tenant_id),
       K_(header_len), K_(client_id_offset), K_(client_id_len), K_(token_offset), K_(token_len),
       K_(stream_name_offset), K_(stream_name_len), K_(client_id), K_(token), K_(stream_name));
  J_OBJ_END();
  return pos;
}

int ObCdcDumpPacket::parse(const char *payload, const int64_t payload_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(payload) || OB_UNLIKELY(payload_len < ObCdcDumpPacket::CDC_DUMP_FIXED_HEADER_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid cdc dump packet", K(payload_len), "expect_min_len",
             ObCdcDumpPacket::CDC_DUMP_FIXED_HEADER_LEN, K(ret));
  } else if (OB_UNLIKELY(static_cast<uint8_t>(OB_MYSQL_COM_CDC_DUMP) != static_cast<uint8_t>(payload[0]))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("unexpected cdc dump cmd", "cmd", static_cast<int64_t>(payload[0]), K(ret));
  } else {
    const char *pos = payload + MYSQL_NET_TYPE_LENGTH;
    total_len_ = payload_len;
    ObMySQLUtil::get_uint4(pos, flags_);
    ObMySQLUtil::get_int4(pos, channel_id_);
    ObMySQLUtil::get_int8(pos, commit_version_);
    ObMySQLUtil::get_int8(pos, txid_);
    ObMySQLUtil::get_int8(pos, txseq_);
    ObMySQLUtil::get_uint8(pos, tenant_id_);
    ObMySQLUtil::get_uint2(pos, header_len_);
    ObMySQLUtil::get_uint2(pos, client_id_offset_);
    ObMySQLUtil::get_uint2(pos, client_id_len_);
    ObMySQLUtil::get_uint2(pos, token_offset_);
    ObMySQLUtil::get_uint2(pos, token_len_);
    ObMySQLUtil::get_uint2(pos, stream_name_offset_);
    ObMySQLUtil::get_uint2(pos, stream_name_len_);

    if (OB_FAIL(assign_var_fields(payload))) {
      LOG_WDIAG("fail to assign cdc dump var fields", K(ret));
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("cdc dump packet is invalid after parse", K(ret));
    } else {
      is_parse_completed_ = true;
      LOG_DEBUG("succ to parse cdc dump packet", KPC(this));
    }
  }
  return ret;
}

int ObCdcDumpPacket::parse_from_reader(ObIOBufferReader &reader, int64_t pkt_len)
{
  int ret = OB_SUCCESS;

  const int64_t avail = reader.read_avail();
  const int64_t body_offset = MYSQL_NET_HEADER_LENGTH;
  const int64_t body_len = pkt_len - MYSQL_NET_HEADER_LENGTH;
  if (OB_UNLIKELY(avail < MYSQL_NET_HEADER_LENGTH + ObCdcDumpPacket::CDC_DUMP_FIXED_HEADER_LEN
                  || avail < pkt_len)) {
    ret = OB_EAGAIN;
    LOG_WDIAG("cdc dump packet is incomplete", K(avail), K(ret));
  } else if (OB_UNLIKELY(body_len <= 0 || body_len > ObCdcDumpPacket::CDC_DUMP_MAX_PAYLOAD_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid cdc dump body len", K(body_len), K(ret));
  } else {
    char *payload_buf = static_cast<char *>(op_fixed_mem_alloc(body_len));
    if (OB_ISNULL(payload_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc cdc dump payload buf", K(body_len), K(ret));
    } else {

      char *written_pos = reader.copy(payload_buf, body_len, body_offset);
      if (OB_UNLIKELY(written_pos != payload_buf + body_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to copy cdc dump payload", K(body_len), K(written_pos), K(ret));
      } else if (OB_FAIL(parse(payload_buf, body_len))) {
        LOG_WDIAG("fail to parse cdc dump payload", K(body_len), K(ret));
      }
      op_fixed_mem_free(payload_buf, body_len);
      payload_buf = NULL;
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
