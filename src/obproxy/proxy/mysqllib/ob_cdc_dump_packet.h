/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_CDC_DUMP_PACKET_H
#define OBPROXY_CDC_DUMP_PACKET_H

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "share/config/ob_config_helper.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
class ObIOBufferReader;
}
namespace proxy
{

// COM_CDC_DUMP (0x60) MsgService data connection request.
// Payload = 55-byte fixed header + client_id + token + stream_name (little-endian).
class ObCdcDumpPacket
{
  static const int64_t CDC_DUMP_FIXED_HEADER_LEN;
  static const int64_t CDC_DUMP_MAX_FIELD_LEN;
  static const int64_t CDC_DUMP_MAX_PAYLOAD_LEN;
public:
  ObCdcDumpPacket();
  void reset();
  bool is_valid() const;

  int64_t to_string(char *buf, const int64_t buf_len) const;
  int64_t get_channel_id() const { return channel_id_; }
  const common::ObString get_client_id() const { return client_id_; }
  const common::ObString get_token() const { return token_; }
  const common::ObString get_stream_name() const { return stream_name_; }

  int parse(const char *payload, const int64_t payload_len);
  // not support stream analyze, it would be better to call
  // parse_from_reader when reveive a complete mysql packet
  int parse_from_reader(event::ObIOBufferReader &reader, int64_t pkt_len);

private:
  int assign_var_fields(const char *payload);

private:
  bool is_parse_completed_;
  uint64_t total_len_;
  uint32_t flags_;
  int32_t channel_id_;
  int64_t commit_version_;
  int64_t txid_;
  int64_t txseq_;
  uint64_t tenant_id_;
  uint16_t header_len_;
  uint16_t client_id_offset_;
  uint16_t client_id_len_;
  uint16_t token_offset_;
  uint16_t token_len_;
  uint16_t stream_name_offset_;
  uint16_t stream_name_len_;
  common::ObConfigVariableString client_id_;
  common::ObConfigVariableString token_;
  common::ObConfigVariableString stream_name_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_CDC_DUMP_PACKET_H
