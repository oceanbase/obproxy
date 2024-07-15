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
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

const char * const OB_V20_PRO_EXTRA_KV_NAME_SYNC_SESSION_INFO = "sess_inf";
const char * const OB_V20_PRO_EXTRA_KV_NAME_FULL_LINK_TRACE = "full_trc";
const char * const OB_SESSION_INFO_VERI = "sess_ver";
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
  FEEDBACK_PROXY_INFO = 1001,
  OB20_PROXY_END = 2000,
  TRACE_INFO = 2001,
  SESS_INFO = 2002,
  FULL_TRC = 2003,
  SESS_INFO_VERI = 2004,

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
  bool is_trans_internal_routing() const { return 1 == st_flags_.OB_IS_TRANS_INTERNAL_ROUTING; }

  uint32_t flags_;
  struct Protocol20Flags
  {
    uint32_t OB_EXTRA_INFO_EXIST:                       1;
    uint32_t OB_IS_LAST_PACKET:                         1;
    uint32_t OB_IS_PROXY_REROUTE:                       1;
    uint32_t OB_IS_NEW_EXTRA_INFO:                      1;
    uint32_t OB_IS_WEAK_READ:                           1;
    uint32_t OB_IS_TRANS_INTERNAL_ROUTING:              1;
    uint32_t OB_PROXY_SWITCH_ROUTE:                     1;
    uint32_t OB_FLAG_RESERVED_NOT_USE:                 25;
  } st_flags_;
};

enum SessionSyncInfoType {
  //SESSION_SYNC_USER_VAR,                // for user variables
  SESSION_SYNC_APPLICATION_INFO = 0,      // for application info
  SESSION_SYNC_APPLICATION_CONTEXT = 1,   // for app ctx
  SESSION_SYNC_CLIENT_ID = 2,             // for client identifier
  SESSION_SYNC_CONTROL_INFO = 3,          // for full trace link control info
  SESSION_SYNC_SYS_VAR = 4,               // for system variables
  SESSION_SYNC_TXN_STATIC_INFO = 5,       // 5: basic txn info
  SESSION_SYNC_TXN_DYNAMIC_INFO = 6,      // 6: txn dynamic info
  SESSION_SYNC_TXN_PARTICIPANTS_INFO = 7, // 7: txn dynamic info
  SESSION_SYNC_TXN_EXTRA_INFO = 8,        // 8: txn dynamic info
  SESSION_SYNC_MAX_TYPE,
};

enum FeedbackProxyInfoType {
  IS_LOCK_SESSION = 0,
  FEEDBACK_PROXY_MAX_TYPE,
};

// all feedback info from observer in extra info should define here
struct Ob20FeedbackProxyInfo {
  Ob20FeedbackProxyInfo() : is_lock_session_(false) {}
  ~Ob20FeedbackProxyInfo() {}
  bool is_lock_session_;
};


struct Ob20ExtraInfo
{
public:
  Ob20ExtraInfo() : is_exist_sess_info_(false), is_exist_feedback_proxy_info_(false),
                    extra_info_buf_(), sess_info_buf_(),
                    sess_info_count_(0), extra_len_(0), feedback_proxy_info_(), 
                    sess_info_length_(), sess_info_cur_idx_(0), sess_info_offset_(0) {}
  ~Ob20ExtraInfo() {}
  
  void reset() {
    is_exist_sess_info_ = false;
    is_exist_feedback_proxy_info_ = false;
    extra_info_buf_.reset();
    sess_info_buf_.reset();
    sess_info_count_ = 0;
    extra_len_ = 0;

    sess_info_length_.reset();
    sess_info_cur_idx_ = 0;
    sess_info_offset_ = 0;
  }
  bool exist_sess_info() const { return is_exist_sess_info_; }
  bool exist_feedback_proxy_info() const { return is_exist_feedback_proxy_info_; }
  uint32_t get_sess_info_count() { return sess_info_count_; }
  int get_next_sess_info(common::ObString &sess_info);
  void reset_sess_info_iterate_idx() { 
    sess_info_cur_idx_ = 0;
    sess_info_offset_ = 0;
  }
  int add_sess_info_buf(const char *str, const int64_t len);
  int decode_feedback_proxy_info(const char * str, const int64_t len);

  TO_STRING_KV(K_(is_exist_sess_info), K_(sess_info_count), K_(extra_len));
public:
  // for session info sync mechanism
  bool is_exist_sess_info_;
  bool is_exist_feedback_proxy_info_;
  obutils::ObVariableLenBuffer<32> extra_info_buf_;     // save last packet extra info kv
  common::ObSqlString sess_info_buf_;                   // save all sess info 
  uint32_t sess_info_count_;
  uint32_t extra_len_;                                 // extra len in ob20 payload, if the flag.exist_extra_info

  Ob20FeedbackProxyInfo feedback_proxy_info_;

private:
  // sess info iterator info
  common::ObSEArray<uint64_t, 2> sess_info_length_;     
  uint32_t sess_info_cur_idx_;
  uint64_t sess_info_offset_;
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

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_OB20_PROTOCOL_STRUCT_H */
