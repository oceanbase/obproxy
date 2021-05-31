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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_PACKET_
#define OCEANBASE_RPC_OBRPC_OB_RPC_PACKET_

#include "easy_io_struct.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/checksum/ob_crc64.h"
#include "rpc/ob_packet.h"

namespace oceanbase
{
namespace obrpc
{

enum ObRpcPriority
{
  ORPR_UNDEF = 0,
  ORPR1, ORPR2, ORPR3, ORPR4,
  ORPR5, ORPR6, ORPR7, ORPR8, ORPR9,
  ORPR_DDL = 10,
};

enum ObRpcPacketCode
{
  OB_INVALID_RPC_CODE = 0,

#define PCODE_DEF(name, id) name = id,
#include "rpc/obrpc/ob_rpc_packet_list.h"
#undef PCODE_DEF

  // don't use the packet code larger than or equal to
  // (PACKET_CODE_MASK - 1), because the first (32 - PACKET_CODE_BITS)
  // bits is used for special packet
  OB_PACKET_NUM,
};

class ObRpcPacketSet
{
  enum
  {
#define PCODE_DEF(name, id) name,
#include "rpc/obrpc/ob_rpc_packet_list.h"
#undef PCODE_DEF
    PCODE_COUNT
  };

  ObRpcPacketSet()
  {
#define PCODE_DEF(name, id)                     \
  names_[name] = #name;                         \
  pcode_[name] = obrpc::name;                   \
  index_[obrpc::name] = name;
#include "rpc/obrpc/ob_rpc_packet_list.h"
#undef PCODE_DEF
  }

public:
  int64_t idx_of_pcode(ObRpcPacketCode code) const
  {
    int64_t index = 0;
    if (code >= 0 && code < OB_PACKET_NUM) {
      index = index_[code];
    }
    return index;
  }

  const char *name_of_idx(int64_t idx) const
  {
    const char *name = "Unknown";
    if (idx >= 0 && idx < PCODE_COUNT) {
      name = names_[idx];
    }
    return name;
  }

  ObRpcPacketCode pcode_of_idx(int64_t idx) const
  {
    ObRpcPacketCode pcode = OB_INVALID_RPC_CODE;
    if (idx < PCODE_COUNT) {
      pcode = pcode_[idx];
    }
    return pcode;
  }

  static ObRpcPacketSet &instance()
  {
    return instance_;
  }

public:
  static const int64_t THE_PCODE_COUNT = PCODE_COUNT;

private:
  static ObRpcPacketSet instance_;

  const char *names_[PCODE_COUNT];
  ObRpcPacketCode pcode_[PCODE_COUNT];
  int64_t index_[OB_PACKET_NUM];
};

class ObRpcPacketHeader
{
public:
  static const uint8_t HEADER_SIZE = 72;
  static const uint16_t RESP_FLAG = 1 << 15;
  static const uint16_t STREAM_FLAG = 1 << 14;
  static const uint16_t STREAM_LAST_FLAG = 1 << 13;

  uint64_t checksum_;
  ObRpcPacketCode pcode_;
  uint8_t hlen_;
  uint8_t priority_;
  uint16_t flags_;
  uint64_t tenant_id_;
  uint64_t priv_tenant_id_;
  uint64_t session_id_;
  uint64_t trace_id_[2];  // 128 bits trace id
  uint64_t timeout_;
  int64_t timestamp_;

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(K(checksum_), K(pcode_), K(hlen_), K(priority_),
               K(flags_), K(tenant_id_), K(priv_tenant_id_), K(session_id_),
               K(trace_id_), K(timeout_), K_(timestamp));

  ObRpcPacketHeader() { memset(this, 0, sizeof(*this)); flags_ |= (OB_LOG_LEVEL_NONE & 0x7); }

  int64_t get_encoded_size() const { return HEADER_SIZE; }
};

class ObRpcPacket
    : public rpc::ObPacket
{
  friend class ObPacketQueue;

public:
  static uint32_t global_chid;

public:
  ObRpcPacket();
  virtual ~ObRpcPacket();

  inline void set_checksum(uint64_t checksum);
  inline uint64_t get_checksum() const;
  inline void calc_checksum();
  inline int verify_checksum() const;

  inline uint32_t get_chid() const;
  inline void set_chid(uint32_t chid);

  inline void set_content(const char *content, int64_t len);
  inline const char *get_cdata() const;
  inline uint32_t get_clen() const;

  inline int decode(const char *buf, int64_t len);
  inline int encode(char *buf, int64_t len, int64_t &pos);
  inline int encode_header(char *buf, int64_t len, int64_t &pos);
  inline int64_t get_encoded_size() const;
  inline int64_t get_header_size() const;

  inline void set_resp();
  inline void unset_resp();
  inline bool is_resp() const;

  inline bool is_stream() const;
  inline bool is_stream_next() const;
  inline bool is_stream_last() const;
  inline void set_stream_next();
  inline void set_stream_last();
  inline void unset_stream();

  inline void set_packet_len(int length);

  inline void set_no_free();
  inline int32_t get_packet_stream_flag() const;
  inline ObRpcPacketCode get_pcode() const;
  inline void set_pcode(ObRpcPacketCode packet_code);

  inline int64_t get_session_id() const;
  inline void set_session_id(const int64_t session_id);

  inline void set_timeout(int64_t timeout) { hdr_.timeout_ = timeout; }
  inline int64_t get_timeout() const { return hdr_.timeout_; }

  inline void set_receive_ts(const int64_t receive_ts) {receive_ts_ = receive_ts;}
  inline int64_t get_receive_ts() const { return receive_ts_;}

  inline void set_priority(uint8_t priority);
  inline uint8_t get_priority() const;

  inline void set_trace_id(const uint64_t *trace_id);
  inline const uint64_t *get_trace_id() const;
  inline int8_t get_log_level() const;
  inline void set_log_level(const int8_t level);

  inline void set_tenant_id(const int64_t tenant_id);
  inline int64_t get_tenant_id() const;

  inline void set_priv_tenant_id(const int64_t tenant_id);
  inline int64_t get_priv_tenant_id() const;


  inline void set_timestamp(const int64_t timestamp);
  inline int64_t get_timestamp() const;

  TO_STRING_KV(K(hdr_), K(chid_), K(clen_));

private:
  ObRpcPacketHeader hdr_;
  const char *cdata_;
  uint32_t clen_;
  uint32_t chid_;         // channel id
  int64_t receive_ts_;  // do not serialize it
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcPacket);
};

void ObRpcPacket::set_checksum(uint64_t checksum)
{
  hdr_.checksum_ = checksum;
}

uint64_t ObRpcPacket::get_checksum() const
{
  return hdr_.checksum_;
}

void ObRpcPacket::calc_checksum()
{
  hdr_.checksum_ = common::ob_crc64(cdata_, clen_);
}

int ObRpcPacket::verify_checksum() const
{
  return hdr_.checksum_ == common::ob_crc64(cdata_,
                                            clen_) ? common::OB_SUCCESS : common::OB_CHECKSUM_ERROR;
}

void ObRpcPacket::set_content(const char *content, int64_t len)
{
  cdata_ = content;
  clen_ = static_cast<uint32_t>(len);
}

const char *ObRpcPacket::get_cdata() const
{
  return cdata_;
}

uint32_t ObRpcPacket::get_clen() const
{
  return clen_;
}


int64_t ObRpcPacket::get_header_size() const
{
  return hdr_.get_encoded_size();
}

int ObRpcPacket::decode(const char *buf, int64_t len)
{
  int ret = common::OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(hdr_.deserialize(buf, len, pos))) {
    RPC_OBRPC_LOG(
        ERROR,
        "decode rpc packet header fail",
        K(ret), K(len), K(pos));
  } else if (len < hdr_.hlen_) {
    ret = common::OB_RPC_PACKET_INVALID;
    RPC_OBRPC_LOG(
        WARN,
        "rpc packet invalid", K_(hdr), K(len));
  } else {
    cdata_ = buf + hdr_.hlen_;
    clen_ = static_cast<uint32_t>(len - hdr_.hlen_);
  }
  return ret;
}

int ObRpcPacket::encode(char *buf, int64_t len, int64_t &pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(hdr_.serialize(buf, len, pos))) {
    COMMON_LOG(WARN, "serialize ob packet header fail");
  } else if (clen_ > len - pos) {
    // buffer no enough to serialize packet
    ret = common::OB_BUF_NOT_ENOUGH;
  } else if (clen_ > 0) {
    MEMCPY(buf + pos, cdata_, clen_);
    pos += clen_;
  }
  return ret;
}

int ObRpcPacket::encode_header(char *buf, int64_t len, int64_t &pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(hdr_.serialize(buf, len, pos))) {
    COMMON_LOG(WARN, "serialize ob packet header fail");
  }
  return ret;
}

void ObRpcPacket::set_resp()
{
  hdr_.flags_ |= ObRpcPacketHeader::RESP_FLAG;
}

void ObRpcPacket::unset_resp()
{
  hdr_.flags_ &= static_cast<uint16_t>(~ObRpcPacketHeader::RESP_FLAG);
}

bool ObRpcPacket::is_resp() const
{
  return hdr_.flags_ & ObRpcPacketHeader::RESP_FLAG;
}

bool ObRpcPacket::is_stream() const
{
  return hdr_.flags_ & ObRpcPacketHeader::STREAM_FLAG;
}

bool ObRpcPacket::is_stream_next() const
{
  return is_stream() && !(hdr_.flags_ & ObRpcPacketHeader::STREAM_LAST_FLAG);
}

bool ObRpcPacket::is_stream_last() const
{
  return is_stream() && (hdr_.flags_ & ObRpcPacketHeader::STREAM_LAST_FLAG);
}

void ObRpcPacket::set_stream_next()
{
  hdr_.flags_ &= static_cast<uint16_t>(~ObRpcPacketHeader::STREAM_LAST_FLAG);
  hdr_.flags_ |= ObRpcPacketHeader::STREAM_FLAG;
}

void ObRpcPacket::set_stream_last()
{
  hdr_.flags_ |= ObRpcPacketHeader::STREAM_LAST_FLAG;
  hdr_.flags_ |= ObRpcPacketHeader::STREAM_FLAG;
}

void ObRpcPacket::unset_stream()
{
  hdr_.flags_ &= static_cast<uint16_t>(~ObRpcPacketHeader::STREAM_FLAG);
}

int64_t ObRpcPacket::get_encoded_size() const
{
  return clen_ + hdr_.get_encoded_size();
}

uint32_t ObRpcPacket::get_chid() const
{
  return chid_;
}

void ObRpcPacket::set_chid(uint32_t chid)
{
  chid_ = chid;
}

ObRpcPacketCode ObRpcPacket::get_pcode() const
{
  return hdr_.pcode_;
}

void ObRpcPacket::set_pcode(ObRpcPacketCode pcode)
{
  hdr_.pcode_ = pcode;
}

int64_t ObRpcPacket::get_session_id() const
{
  return hdr_.session_id_;
}

void ObRpcPacket::set_session_id(const int64_t session_id)
{
  hdr_.session_id_ = session_id;
}

void ObRpcPacket::set_priority(uint8_t priority)
{
  hdr_.priority_ = priority;
}

uint8_t ObRpcPacket::get_priority() const
{
  return hdr_.priority_;
}

const uint64_t *ObRpcPacket::get_trace_id() const
{
  return hdr_.trace_id_;
}

int8_t ObRpcPacket::get_log_level() const
{
  return hdr_.flags_ & 0x7;
}

void ObRpcPacket::set_trace_id(const uint64_t *trace_id)
{
  if (trace_id != NULL) {
    hdr_.trace_id_[0] = trace_id[0];
    hdr_.trace_id_[1] = trace_id[1];
  }
}

void ObRpcPacket::set_log_level(const int8_t log_level)
{
  hdr_.flags_ &= static_cast<uint16_t>(~0x7);   //must set zero first
  hdr_.flags_ |= static_cast<uint16_t>(log_level);
}

void ObRpcPacket::set_tenant_id(const int64_t tenant_id)
{
  hdr_.tenant_id_ = tenant_id;
}

int64_t ObRpcPacket::get_tenant_id() const
{
  return hdr_.tenant_id_;
}

void ObRpcPacket::set_priv_tenant_id(const int64_t tenant_id)
{
  hdr_.priv_tenant_id_ = tenant_id;
}

int64_t ObRpcPacket::get_priv_tenant_id() const
{
  return hdr_.priv_tenant_id_;
}

void ObRpcPacket::set_timestamp(const int64_t timestamp)
{
  hdr_.timestamp_ = timestamp;
}

int64_t ObRpcPacket::get_timestamp() const
{
  return hdr_.timestamp_;
}

} // end of namespace rpc
} // end of namespace oceanbase

#endif // OCEANBASE_RPC_OBRPC_OB_RPC_PACKET_
