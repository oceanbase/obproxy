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

#ifndef OBPROXY_OB20_PROTOCOL_STRUCT_H
#define OBPROXY_OB20_PROTOCOL_STRUCT_H

#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "obutils/ob_proxy_buf.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

const char * const OB_V20_PRO_EXTRA_KV_NAME_SYNC_SESSION_INFO = "sess_inf";
const char * const OB_V20_PRO_EXTRA_KV_NAME_FULL_LINK_TRACE = "full_trc";
const char * const OB_TRACE_INFO_VAR_NAME = "ob_trace_info";
const char * const OB_TRACE_INFO_CLIENT_IP = "client_ip";


/*
 * oceanbase 2.0 protocol new extra info key type
 *
 * [0, 999] only for ob driver
 * [1001, 1999] only for obproxy
 * [2001, 65535] common for observer + obclient + obproxy
 */
enum Ob20NewExtraInfoProtocolKeyType {
  OB20_DRIVER_END = 1000,
  OB20_PROXY_END = 2000,
  TRACE_INFO = 2001,
  SESS_INFO = 2002,
  FULL_TRC = 2003,
  OB20_SVR_END,
};

// used for proxy and observer to negotiate new features
union Ob20ProtocolFlags
{
  Ob20ProtocolFlags() : flags_(0) {}
  explicit Ob20ProtocolFlags(uint32_t flag) : flags_(flag) {}

  bool is_extra_info_exist() const { return 1 == st_flags_.OB_EXTRA_INFO_EXIST; }
  bool is_last_packet() const { return 1 == st_flags_.OB_IS_LAST_PACKET; }
  bool is_new_extra_info() const { return 1 == st_flags_.OB_IS_NEW_EXTRA_INFO; }

  uint32_t flags_;
  struct Protocol20Flags
  {
    uint32_t OB_EXTRA_INFO_EXIST:                       1;
    uint32_t OB_IS_LAST_PACKET:                         1;
    uint32_t OB_IS_PROXY_REROUTE:                       1;
    uint32_t OB_IS_NEW_EXTRA_INFO:                      1;
    uint32_t OB_FLAG_RESERVED_NOT_USE:                 28;
  } st_flags_;
};

struct Ob20ExtraInfo
{
public:
  // for session info sync mechanism
  bool is_exist_sess_info_;
  obutils::ObVariableLenBuffer<32> extra_info_buf_;     // save total extra info kv
  obutils::ObVariableLenBuffer<32> sess_info_buf_;      // save only session info
  common::ObString sess_info_;

  // ob20 info
  uint32_t extra_len_;                // extra len in ob20 payload, if the flag.exist_extra_info

public:
  Ob20ExtraInfo() : is_exist_sess_info_(false), sess_info_(), extra_len_(0) {}
  ~Ob20ExtraInfo() {}
  
  void reset() {
    is_exist_sess_info_ = false;
    extra_info_buf_.reset();
    sess_info_buf_.reset();
    sess_info_.reset();
    extra_len_ = 0;
  }
  
  inline bool exist_sess_info() const { return is_exist_sess_info_; }
  inline common::ObString get_sess_info() const { return sess_info_; }
  
  TO_STRING_KV(K_(is_exist_sess_info), K_(sess_info), K_(extra_len));
};

class Ob20ProtocolHeader
{
public:
  ObMysqlCompressedPacketHeader cp_hdr_;

  uint16_t magic_num_;
  uint16_t header_checksum_;
  uint32_t connection_id_;
  uint32_t request_id_;
  uint8_t pkt_seq_;
  uint32_t payload_len_;
  Ob20ProtocolFlags flag_;
  uint16_t version_;
  uint16_t reserved_;

public:
  Ob20ProtocolHeader()
    : cp_hdr_(), magic_num_(0), header_checksum_(0),
    connection_id_(0), request_id_(0), pkt_seq_(0), payload_len_(0),
    flag_(0), version_(0), reserved_(0) {}

  ~Ob20ProtocolHeader() {}

  void reset()
  {
    MEMSET(this, 0, sizeof(Ob20ProtocolHeader));
  }

  TO_STRING_KV("ob 20 protocol header", cp_hdr_,
               K_(magic_num),
               K_(header_checksum),
               K_(connection_id),
               K_(request_id),
               K_(pkt_seq),
               K_(payload_len),
               K_(version),
               K_(flag_.flags),
               K_(reserved));
};

// used for transfer function argument
class Ob20ProtocolHeaderParam {
public:
  Ob20ProtocolHeaderParam() : connection_id_(0), request_id_(0), compressed_seq_(0), pkt_seq_(0),
                              is_last_packet_(false), is_need_reroute_(false), is_new_extra_info_(false) {}
  Ob20ProtocolHeaderParam(uint32_t conn_id, uint32_t req_id, uint8_t compressed_seq, uint8_t pkt_seq,
                          bool is_last_packet, bool is_need_reroute, bool is_new_extra_info)
                          : connection_id_(conn_id), request_id_(req_id), compressed_seq_(compressed_seq),
                            pkt_seq_(pkt_seq), is_last_packet_(is_last_packet), is_need_reroute_(is_need_reroute),
                            is_new_extra_info_(is_new_extra_info) {}
  ~Ob20ProtocolHeaderParam() {}

  Ob20ProtocolHeaderParam(const Ob20ProtocolHeaderParam &param) {
    *this = param;
  }

  Ob20ProtocolHeaderParam &operator=(const Ob20ProtocolHeaderParam &param) {
    if (this != &param) {
      connection_id_ = param.connection_id_;
      request_id_ = param.request_id_;
      compressed_seq_ = param.compressed_seq_;
      pkt_seq_ = param.pkt_seq_;
      is_last_packet_ = param.is_last_packet_;
      is_need_reroute_ = param.is_need_reroute_;
      is_new_extra_info_ = param.is_new_extra_info_;
    }
    return *this;
  }

  uint32_t get_connection_id() const { return connection_id_; }
  uint32_t get_request_id() const { return request_id_; }
  uint8_t get_compressed_seq() const { return compressed_seq_; }
  uint8_t get_pkt_seq() const { return pkt_seq_; }
  bool is_last_packet() const { return is_last_packet_; }
  bool is_need_reroute() const { return is_need_reroute_; }
  bool is_new_extra_info() const { return is_new_extra_info_; }

  void reset()
  {
    MEMSET(this, 0x0, sizeof(Ob20ProtocolHeaderParam));
    is_last_packet_ = false;
    is_need_reroute_ = false;
    is_new_extra_info_ = false;
  }

  TO_STRING_KV(K_(connection_id), K_(request_id), K_(compressed_seq),
               K_(pkt_seq), K_(is_last_packet), K_(is_need_reroute), K_(is_new_extra_info));
private:
  uint32_t connection_id_;
  uint32_t request_id_;
  uint8_t compressed_seq_;
  uint8_t pkt_seq_;
  bool is_last_packet_;
  bool is_need_reroute_;
  bool is_new_extra_info_;
};


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_OB20_PROTOCOL_STRUCT_H */
