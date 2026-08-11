/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_MYSQL_RESPONSE_H
#define OBPROXY_MYSQL_RESPONSE_H
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "common/ob_object.h"
#include "rpc/obmysql/packet/ompk_error.h"
#include "rpc/obmysql/packet/ompk_ok.h"
#include "rpc/obmysql/packet/ompk_eof.h"
#include "rpc/obmysql/packet/ompk_handshake.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "proxy/mysqllib/ob_resp_packet_analyze_result.h"
#include "proxy/mysqllib/ob_proxy_session_info_handler.h"
#include "proxy/mysqllib/ob_2_0_protocol_struct.h"
#include "lib/utility/ob_2_0_full_link_trace_info.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
enum ObOKPacketActionType {
  OK_PACKET_ACTION_SEND = 0, // send this packet directly (in multi stmt or mysql mode)
  OK_PACKET_ACTION_REWRITE,  // rewrite this packet, last valid ok packet
  OK_PACKET_ACTION_CONSUME,  // consume this packet, last extra ok packet
};

// ok, error, eof packet's content,
// valid only when response is received completed.
class ObRespAnalyzeResult
{
public:
  ObRespAnalyzeResult()
  {
    reset();
    memset(handshake_.scramble_buf_, 0, sizeof(handshake_.scramble_buf_));
    memset(sysvar_.server_trace_id_buf_, 0, sizeof(sysvar_.server_trace_id_buf_));
    memset(rsa_.public_key_buf_, 0, sizeof(rsa_.public_key_buf_));
  }
  ~ObRespAnalyzeResult()
  {
    error_.error_pkt_buf_.reset();
    ob20_.extra_info_.reset();
    ob20_.flt_.reset();
  }
  void reset();
  void reset_transmit_control();
  inline bool is_decompressed() const { return transmit_control_.is_decompressed_; }
  inline void set_is_decompressed(bool v) { transmit_control_.is_decompressed_ = v; }
  inline bool is_trans_completed() const { return transmit_control_.is_trans_completed_; }
  inline void set_is_trans_completed(bool v) { transmit_control_.is_trans_completed_ = v; }
  inline bool is_resp_completed() const { return transmit_control_.is_resp_completed_; }
  inline void set_is_resp_completed(bool v) { transmit_control_.is_resp_completed_ = v; }
  inline int64_t get_reserved_ok_len_of_mysql() const { return transmit_control_.reserved_ok_len_of_mysql_; }
  inline void set_reserved_ok_len_of_mysql(int64_t len) { transmit_control_.reserved_ok_len_of_mysql_ = len; }
  inline int64_t get_reserved_ok_len_of_compressed() const { return transmit_control_.reserved_ok_len_of_compressed_; }
  inline void set_reserved_ok_len_of_compressed(int64_t len) { transmit_control_.reserved_ok_len_of_compressed_ = len; }
  inline int64_t get_last_ok_pkt_len() const { return transmit_control_.last_ok_pkt_len_; }
  inline void set_last_ok_pkt_len(int64_t len) { transmit_control_.last_ok_pkt_len_ = len; }
  inline int64_t get_rewritten_last_ok_pkt_len() const { return transmit_control_.rewritten_last_ok_pkt_len_; }
  inline void set_rewritten_last_ok_pkt_len(int64_t len) { transmit_control_.rewritten_last_ok_pkt_len_ = len; }
  inline ObOKPacketActionType get_ok_packet_action_type() const { return transmit_control_.ok_packet_action_type_; }
  inline void set_ok_packet_action_type(ObOKPacketActionType action) { transmit_control_.ok_packet_action_type_ = action; }
  inline bool is_last_ok_handled() const { return transmit_control_.is_last_ok_handled_; }
  inline void set_is_last_ok_handled(bool v) { transmit_control_.is_last_ok_handled_ = v; }
  inline bool is_local_infile_0xfb_resp() const { return format_.ending_type_ == LOCAL_INFILE_ENDING_TYPE; }

  inline bool is_error_resp() const { return ERROR_PACKET_ENDING_TYPE == format_.ending_type_; }
  inline bool is_ok_resp() const { return OK_PACKET_ENDING_TYPE == format_.ending_type_; }
  inline bool is_eof_resp() const { return EOF_PACKET_ENDING_TYPE == format_.ending_type_; }
  inline bool is_handshake_pkt() const { return HANDSHAKE_PACKET_ENDING_TYPE == format_.ending_type_; }
  inline void set_ending_type(ObMysqlRespEndingType type) { format_.ending_type_ = type; }
  bool is_resultset_resp() const { return format_.is_resultset_resp_; }
  inline void set_is_resultset_resp(bool v) { format_.is_resultset_resp_ = v; }
  inline bool is_server_db_reset() const { return format_.is_server_db_reset_; }
  inline void set_is_server_db_reset(bool v) { format_.is_server_db_reset_ = v; }
  inline bool is_auth_switch_req() const { return format_.is_auth_switch_req_; }
  inline void set_is_auth_switch_req(bool v) { format_.is_auth_switch_req_ = v; }
  inline bool is_auth_more_data_req() const { return format_.is_auth_more_data_req_; }
  inline void set_is_auth_more_data_req(bool v) { format_.is_auth_more_data_req_ = v; }
  inline bool is_rsa_public_key_resp() const { return format_.is_rsa_public_key_resp_; }
  inline void set_is_rsa_public_key_resp(bool v) { format_.is_rsa_public_key_resp_ = v; }
  inline bool is_fast_auth_succ() const { return format_.is_fast_auth_succ_; }
  inline void set_is_fast_auth_succ(bool v) { format_.is_fast_auth_succ_ = v; }
  inline int64_t get_deferred_ok_offset() const { return transmit_control_.deferred_ok_offset_; }
  inline void set_deferred_ok_offset(int64_t v) { transmit_control_.deferred_ok_offset_ = v; }
  inline int64_t get_deferred_ok_pkt_len() const { return transmit_control_.deferred_ok_pkt_len_; }
  inline void set_deferred_ok_pkt_len(int64_t v) { transmit_control_.deferred_ok_pkt_len_ = v; }
  inline bool is_partition_hit() const { return sysvar_.is_partition_hit_; }
  inline void set_is_partition_hit(bool v) { sysvar_.is_partition_hit_ = v; }
  inline ObWeakReadHitReplica get_weak_read_hit_replica() const { return sysvar_.weak_read_hit_replica_; }
  inline void set_weak_read_hit_replica(ObWeakReadHitReplica replica) { sysvar_.weak_read_hit_replica_ = replica; }
  inline bool is_last_insert_id_changed() const { return sysvar_.is_last_insert_id_changed_; }
  inline void set_is_last_insert_id_changed(bool v) { sysvar_.is_last_insert_id_changed_ = v; }
  inline bool has_new_sys_var() const { return sysvar_.has_new_sys_var_; }
  inline void set_has_new_sys_var(bool v) { sysvar_.has_new_sys_var_ = v; }
  inline bool has_proxy_idc_name_user_var() const { return sysvar_.has_proxy_idc_name_user_var_; }
  inline void set_has_proxy_idc_name_user_var(bool v) { sysvar_.has_proxy_idc_name_user_var_ = v; }
  inline uint32_t get_connection_id() const { return handshake_.connection_id_; }
  inline void set_connection_id(uint32_t id) { handshake_.connection_id_ = id; }
  inline common::ObString get_scramble_string() const { return common::ObString::make_string(handshake_.scramble_buf_); }
  inline char* get_scramble_buf() { return handshake_.scramble_buf_; }
  inline const int64_t get_scramble_buf_len() const { return sizeof(handshake_.scramble_buf_); }
  inline common::ObString get_rsa_public_key() const
  {
    return common::ObString(rsa_.public_key_len_, rsa_.public_key_buf_);
  }
  inline int set_rsa_public_key(const common::ObString &public_key)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(public_key.length() > static_cast<int64_t>(sizeof(rsa_.public_key_buf_) - 1))) {
      ret = common::OB_SIZE_OVERFLOW;
    } else {
      rsa_.public_key_len_ = static_cast<int32_t>(public_key.length());
      if (rsa_.public_key_len_ > 0) {
        MEMCPY(rsa_.public_key_buf_, public_key.ptr(), rsa_.public_key_len_);
      }
      rsa_.public_key_buf_[rsa_.public_key_len_] = '\0';
    }
    return ret;
  }
  inline bool is_server_can_use_compress() const { return (1 == handshake_.server_capabilities_lower_.capability_flag_.OB_SERVER_CAN_USE_COMPRESS); }
  inline bool support_ssl() const { return 1 == handshake_.server_capabilities_lower_.capability_flag_.OB_SERVER_SSL; }
  inline void set_server_cap_lower(uint16_t cap_lower) { handshake_.server_capabilities_lower_.capability_ = cap_lower; }
  inline void set_server_cap_upper(uint16_t cap_upper) { handshake_.server_capabilities_upper_.capability_ = cap_upper; }
  inline uint32_t get_server_capability() const { return ((handshake_.server_capabilities_upper_.capability_ << 16)
                                                         | handshake_.server_capabilities_lower_.capability_); }
  inline bool is_server_trans_internal_routing() const { return ob20_.is_server_trans_internal_routing_; }
  inline void set_is_server_trans_internal_routing(bool v) { ob20_.is_server_trans_internal_routing_ = v; }
  inline obmysql::OMPKError &get_error_pkt() { return error_.error_pkt_; }
  inline const obmysql::OMPKError &get_error_pkt() const { return error_.error_pkt_; }
  inline uint16_t get_error_code() const { return error_.error_pkt_.get_err_code(); }
  inline common::ObString get_error_message() const { return error_.error_pkt_.get_message(); }
  inline obutils::ObVariableLenBuffer<FIXED_MEMORY_BUFFER_SIZE> &get_error_pkt_buf() { return error_.error_pkt_buf_; }
  inline bool is_not_supported_error() const
  {
    return (is_error_resp() && ER_NOT_SUPPORTED_YET == error_.error_pkt_.get_err_code());
  }
  inline bool is_bad_db_error() const
  {
    return (is_error_resp() && ER_BAD_DB_ERROR == error_.error_pkt_.get_err_code());
  }
  inline bool is_unknown_tenant_error() const
  {
    return (is_error_resp() && -common::OB_TENANT_NOT_EXIST == error_.error_pkt_.get_err_code());
  }
  inline bool is_tenant_not_in_server_error() const
  {
    return (is_error_resp() && -common::OB_TENANT_NOT_IN_SERVER == error_.error_pkt_.get_err_code());
  }
  inline bool is_cluster_not_match_error() const
  {
    return (is_error_resp() && -common::OB_CLUSTER_NO_MATCH == error_.error_pkt_.get_err_code());
  }
  inline bool is_server_init_error() const
  {
    return (is_error_resp() && -common::OB_SERVER_IS_INIT == error_.error_pkt_.get_err_code());
  }
  inline bool is_server_stopping_error() const /* OB_SERVER_IS_STOPPING(8002) is same error code with OB_ERR_SEQUENCE_NOT_DEFINE in oracle mode */
  {
    return (is_error_resp() && -common::OB_SERVER_IS_STOPPING == error_.error_pkt_.get_err_code()
              && get_error_message().case_compare(ob_str_user_error(OB_SERVER_IS_STOPPING)) == 0);
  }
  inline bool is_session_entry_exist() const
  {
    return (is_error_resp() && -common::OB_SESSION_ENTRY_EXIST == error_.error_pkt_.get_err_code());
  }
  inline bool is_net_packet_too_large_error() const
  {
    return (is_error_resp() && ER_NET_PACKET_TOO_LARGE == error_.error_pkt_.get_err_code());
  }
  inline bool is_connect_error() const
  {
    return (is_error_resp() && -common::OB_CONNECT_ERROR == error_.error_pkt_.get_err_code());
  }
  inline bool is_readonly_error() const
  {
    return (is_error_resp() && -common::OB_ERR_READ_ONLY == error_.error_pkt_.get_err_code());
  }
  inline bool is_service_name_not_found_error() const
  {
    return (is_error_resp() && -common::OB_SERVICE_NAME_NOT_FOUND == error_.error_pkt_.get_err_code());
  }
  inline bool is_not_primary_tenant() const
  {
    return (is_error_resp() && -common::OB_NOT_PRIMARY_TENANT == error_.error_pkt_.get_err_code());
  }
  inline bool is_reroute_error() const
  {
    return (is_error_resp() && -common::OB_ERR_REROUTE == error_.error_pkt_.get_err_code());
  }
  inline bool is_ora_fatal_error() const
  {
    return (is_error_resp() && -common::OB_ORA_FATAL_ERROR == error_.error_pkt_.get_err_code());
  }
  inline bool is_standby_weak_readonly_error() const
  {
    return (is_error_resp() && -common::OB_STANDBY_WEAK_READ_ONLY == error_.error_pkt_.get_err_code());
  }
  inline bool is_trans_free_route_not_supported_error() const
  {
    return (is_error_resp() && -common::OB_TRANS_FREE_ROUTE_NOT_SUPPORTED == error_.error_pkt_.get_err_code());
  }
  inline bool is_mysql_wrong_arguments_error() const
  {
    return (is_error_resp() && ER_WRONG_ARGUMENTS == error_.error_pkt_.get_err_code());
  }
  inline bool is_internal_error() const
  {
    return (is_error_resp() && -common::OB_INTERNAL_ERROR == error_.error_pkt_.get_err_code());
  }
  inline bool is_client_session_killed_error() const
  {
    return (is_error_resp() && -common::OB_ERR_KILL_CLIENT_SESSION == error_.error_pkt_.get_err_code());
  }
  inline Ob20ExtraInfo &get_extra_info() { return ob20_.extra_info_; }
  inline const Ob20ExtraInfo &get_extra_info() const { return ob20_.extra_info_; }
  inline common::FLTObjManage &get_flt() { return ob20_.flt_; }
  inline const common::FLTObjManage &get_flt() const { return ob20_.flt_; }
  inline const common::ObString& get_server_trace_id() const { return sysvar_.server_trace_id_; }
  inline void set_server_trace_id(const common::ObString &trace_id)
  {
    if (trace_id.empty()) {
      sysvar_.server_trace_id_.reset();
    } else {
      common::ObString::obstr_size_t copy_len = std::min(trace_id.length(),
           common::ObString::obstr_size_t(common::OB_MAX_TRACE_ID_LENGTH));
      if (copy_len >= 0) { // just for defense
        MEMCPY(sysvar_.server_trace_id_buf_, trace_id.ptr(), copy_len);
        sysvar_.server_trace_id_.assign(sysvar_.server_trace_id_buf_, copy_len);
      }
    }
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  struct {
    /* control the flow of transmitting response used by tunnel and plugin */
    bool is_decompressed_:                     1; // whether need tunnel or plugin to decompress
    bool is_trans_completed_:                  1; // whether transaction completed
    bool is_resp_completed_:                   1; // whether whole data of response be analyzed
    bool is_last_ok_handled_:                  1; // whether the last ok pkt be trimmed
    int64_t reserved_ok_len_of_mysql_;            // reserve the lastest one mysql pkt data in the tunnel for triming if neccessary
    int64_t reserved_ok_len_of_compressed_;       // reserve the lastest one mysql pkt data in the ObMysqlResponseCompressTransformPlugin for triming if neccessary
    int64_t last_ok_pkt_len_;                     // the last ok pkt len including mysql header
    int64_t rewritten_last_ok_pkt_len_;           // the last ok pkt len including mysql header after rebuild it
    ObOKPacketActionType ok_packet_action_type_;  // rebuild or trim the last ok pkt
    int64_t deferred_ok_offset_;                  // offset of OK pkt after 0x01 0x03 (fast auth succ), for csha2 defer
    int64_t deferred_ok_pkt_len_;                // length of that OK pkt including mysql header
  } transmit_control_;

  /* format of the response */
  struct {
    bool is_auth_switch_req_:              1; // if resp is auth switch request
    bool is_auth_more_data_req_:           1; // if resp is auth more data request (e.g. caching_sha2_password full auth)
    bool is_rsa_public_key_resp_:          1; // if resp is auth more data carrying RSA public key text
    bool is_fast_auth_succ_:               1; // if first pkt is 0x01 0x03 (fast auth success), expect OK next
    bool is_resultset_resp_:               1; // if resultset then handle_resultset_resp()
    bool is_server_db_reset_:              1; // if db reset(empty db) then disconnect all server session of current client session (ObMysqlTransact::handle_db_reset)
    ObMysqlRespEndingType ending_type_;       // if resp is eof/ok/err/handshake
  } format_;

  /* system variables in response */
  struct {
    bool is_partition_hit_:                     1; // if miss partition then update location cached and print log
    bool is_last_insert_id_changed_:            1; // if changed then call set_lii_server_session() on client_session
    bool has_new_sys_var_:                      1; // if observer respond new sys var then add it to cluster resource (add_sys_var_renew_task)
    bool has_proxy_idc_name_user_var_:          1; // if observer respond the sys var then update the client session ldc
    ObWeakReadHitReplica weak_read_hit_replica_;   // if observer respond the sys var then set
    common::ObString server_trace_id_;             // if observer respond the sys var then set
    char server_trace_id_buf_[common::OB_MAX_TRACE_ID_LENGTH];
  } sysvar_;

  /* properties of handshake packet */
  struct {
    uint32_t connection_id_;                                                    // for handshake pkt
    obmysql::OMPKHandshake::ServerCapabilitiesLower server_capabilities_lower_; // for handshake pkt
    obmysql::OMPKHandshake::ServerCapabilitiesUpper server_capabilities_upper_; // for handshake pkt
    char scramble_buf_[obmysql::OMPKHandshake::SCRAMBLE_TOTAL_SIZE + 1];        // for handshake pkt
  } handshake_;

  struct {
    int32_t public_key_len_;
    char public_key_buf_[2048];
  } rsa_;

  /* save whole error packet */
  struct {
    obmysql::OMPKError error_pkt_;                                              // for error pkt
    obutils::ObVariableLenBuffer<FIXED_MEMORY_BUFFER_SIZE> error_pkt_buf_;      // for error pkt

  } error_;

  /* properties in oceanbase 2.0 */
  struct {
    bool is_server_trans_internal_routing_:    1; // for oceanbase 2.0
    Ob20ExtraInfo extra_info_;                    // for oceanbase 2.0 extra info
    common::FLTObjManage flt_;                    // for oceanbase 2.0 extra info's full-link trace info

  } ob20_;

  DISALLOW_COPY_AND_ASSIGN(ObRespAnalyzeResult);
};

// reset the members that control the flow of tunnel and plugin
inline void ObRespAnalyzeResult::reset_transmit_control()
{
  set_is_decompressed(false);
  set_is_trans_completed(false);
  set_is_resp_completed(false);
  set_reserved_ok_len_of_mysql(0);
  set_reserved_ok_len_of_compressed(0);
  set_is_last_ok_handled(false);
  set_last_ok_pkt_len(0);
  set_rewritten_last_ok_pkt_len(0);
  set_ok_packet_action_type(OK_PACKET_ACTION_SEND);
  set_deferred_ok_offset(0);
  set_deferred_ok_pkt_len(0);
}

inline void ObRespAnalyzeResult::reset()
{
  // transmit control
  reset_transmit_control();

  // format
  set_is_auth_switch_req(false);
  set_is_auth_more_data_req(false);
  set_is_rsa_public_key_resp(false);
  set_is_fast_auth_succ(false);
  set_is_resultset_resp(false);
  set_is_server_db_reset(false);
  set_ending_type(MAX_PACKET_ENDING_TYPE);

  // sys var
  set_is_partition_hit(true);
  set_is_last_insert_id_changed(false);
  set_has_new_sys_var(false);
  set_has_proxy_idc_name_user_var(false);
  set_weak_read_hit_replica(MAX_REPLICA);
  sysvar_.server_trace_id_.reset();
  sysvar_.server_trace_id_buf_[0] = '\0';

  // handshake
  set_connection_id(0);
  set_server_cap_lower(0);
  set_server_cap_upper(0);
  handshake_.scramble_buf_[0] = '\0';

  // rsa
  rsa_.public_key_len_ = 0;
  rsa_.public_key_buf_[0] = '\0';

  // error
  error_.error_pkt_buf_.reset();

  // ob20
  set_is_server_trans_internal_routing(false);
  ob20_.extra_info_.reset();
  ob20_.flt_.reset();
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_MYSQL_RESP_ANALYZE_RESULT_H
