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

#ifndef OB_CONNECTION_DIAGNOSIS_TRACE
#define OB_CONNECTION_DIAGNOSIS_TRACE

#include "iocore/net/ob_inet.h"
#include "lib/string/ob_string.h"
#include "lib/oblog/ob_log_module.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/string/ob_string.h"
#include "lib/ptr/ob_ptr.h"
#include "proxy/mysqllib/ob_protocol_diagnosis.h"

#define MAX_MSG_BUF_LEN 128

#define COLLECT_MSG_BUF(args...)     \
  int64_t buf_len = MAX_MSG_BUF_LEN; \
  int64_t pos = 0;                   \
  char buf[MAX_MSG_BUF_LEN+1];         \
  MEMSET(buf, 0, MAX_MSG_BUF_LEN);   \
  BUF_PRINTF(args);

// connection related trace, record disconnection of client or observer
// if proxy not received first packet and client disconnect, marked as detect request
#define COLLECT_VC_DIAGNOSIS(diag, trace_type, event, code, args...) \
  if (OB_NOT_NULL(diag) && diag->diagnosis_info_ == NULL) {          \
    if (trace_type == OB_CLIENT_VC_TRACE && event == VC_EVENT_EOS && \
        !diag->is_first_packet_received_) {                          \
      diag->set_is_detect_request(true);                             \
    }                                                                \
    if (diag->need_record_diagnosis_log()) {                         \
      COLLECT_MSG_BUF(args);                                         \
      diag->set_trace_type(trace_type);                              \
      diag->record_vc_disconnection(event, code, buf);               \
    }                                                                \
  }

// timeout related trace, record timeout disconnection event
#define COLLECT_TIMEOUT_DIAGNOSIS(diag, trace_type, event, timeout, code,     \
                                  args...)                                    \
  if (OB_NOT_NULL(diag) && diag->need_record_diagnosis_log() &&     \
      diag->diagnosis_info_ == NULL) {                                        \
    COLLECT_MSG_BUF(args);                                                    \
    diag->set_trace_type(trace_type);                                         \
    diag->record_inactivity_timeout_disconnection(event, timeout, code, buf); \
  }

// internal error related trace, record obproxy internal disconnection
#define COLLECT_INTERNAL_DIAGNOSIS(diag, trace_type, code, args...)       \
  if (OB_NOT_NULL(diag) && diag->need_record_diagnosis_log() && \
      diag->diagnosis_info_ == NULL) {                                    \
    COLLECT_MSG_BUF(args);                                                \
    diag->set_trace_type(trace_type);                                     \
    diag->record_obproxy_internal_disconnection(code, buf);               \
  }

// login related trace, record login failed event
#define COLLECT_LOGIN_DIAGNOSIS(diag, trace_type, sql, code, args...)     \
  if (OB_NOT_NULL(diag) && diag->need_record_diagnosis_log() && \
      diag->diagnosis_info_ == NULL) {                                    \
    COLLECT_MSG_BUF(args);                                                \
    diag->set_trace_type(trace_type);                                     \
    diag->record_login_disconnection(sql, code, buf);                     \
  }

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

using namespace common;

enum ObConenctionDiagnosisOption {
  OB_ENABLE_CONNECTION_DIAGNOSIS_LOG = 0,
  OB_ENABLE_KEEP_CONNECTION
};

enum ObConnectionDiagnosisTraceType
{
  // login trace
  OB_LOGIN_DISCONNECT_TRACE,

  // proxy or observer internal error trace
  OB_PROXY_INTERNAL_TRACE,
  OB_SERVER_INTERNAL_TRACE,

  // connection related trace, record disconnection of client or observer
  OB_CLIENT_VC_TRACE,
  OB_SERVER_VC_TRACE,

  // timeout trace
  OB_TIMEOUT_DISCONNECT_TRACE,

  // max type
  OB_MAX_TRACE_TYPE,
};

enum ObInactivityTimeoutEvent
{
  OB_TIMEOUT_UNKNOWN_EVENT = 0,
  OB_CLIENT_DELETE_CLUSTER_RESOURCE,
  OB_CLIENT_INTERNAL_CMD_TIMEOUT,
  OB_CLIENT_CONNECT_TIMEOUT,
  OB_CLIENT_NET_READ_TIMEOUT,
  OB_CLIENT_EXECUTE_PLAN_TIMEOUT,
  OB_CLIENT_WAIT_TIMEOUT,
  OB_CLIENT_NET_WRITE_TIMEOUT,
  OB_SERVER_QUERY_TIMEOUT,
  OB_SERVER_TRX_TIMEOUT,
  OB_SERVER_WAIT_TIMEOUT,
  OB_CLIENT_FORCE_KILL,
  OB_MAX_TIMEOUT_EVENT,
};

class ObConnectionDiagnosisInfo;
class ObProxyInternalDiagnosisInfo;
class ObConnectionDiagnosisTrace : public ObSharedRefCount
{
public:
  ObConnectionDiagnosisTrace()
      : trace_type_(OB_MAX_TRACE_TYPE),
        diagnosis_info_(NULL),
        is_user_client_(true),
        is_first_packet_received_(false),
        is_detect_request_(false),
        is_com_quit_(false),
        is_detect_user_(false),
        protocol_diagnosis_(NULL) {}
  ~ObConnectionDiagnosisTrace() { destroy(); }
  void destroy();
  void reset();
  ObConnectionDiagnosisInfo *get_diagnosis_info() { return diagnosis_info_; }
  void set_diagnosis_info(ObConnectionDiagnosisInfo *info) { diagnosis_info_ = info; }
  template<typename T>
  int create_diagnosis_info(T *&diagnosis_info) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(diagnosis_info)) {
      if (OB_ISNULL(diagnosis_info = op_alloc(T))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_LOG(WDIAG, "fail to allocate mem for connection diagnosis info", K(ret));
      }
    }
    return ret;
  }
  int record_vc_disconnection(int vc_event, int error_code, ObString error_msg = "");
  int record_inactivity_timeout_disconnection(ObInactivityTimeoutEvent event, ObHRTime timeout, int error_code, ObString error_msg = "");
  int record_obproxy_internal_disconnection(int error_code, ObString error_msg = "");
  int record_login_disconnection (const ObString &internal_sql, int error_code, ObString error_msg = "");
  void log_diagnosis_info() const;
  void set_trace_type(ObConnectionDiagnosisTraceType trace_type) { trace_type_ = trace_type; }
  void set_user_client(const bool is_user_client) { is_user_client_ = is_user_client; }
  void set_is_detect_request(const bool is_detect_request) { is_detect_request_ = is_detect_request; }
  void set_first_packet_received(const bool is_first_packet_received) { is_first_packet_received_ = is_first_packet_received; }
  void set_is_detect_user(const bool is_detect_user) { is_detect_user_ = is_detect_user; }
  bool need_record_diagnosis_log() const;
  void set_is_com_quit(const bool is_com_quit) { is_com_quit_ = is_com_quit; }
  virtual void free();
  static bool is_enable_diagnosis_log(int64_t connection_diagnosis_control)
  {
    return ((connection_diagnosis_control) & (1 << OB_ENABLE_CONNECTION_DIAGNOSIS_LOG)) == (1 << OB_ENABLE_CONNECTION_DIAGNOSIS_LOG);
  }
  static bool is_enable_keep_connection(int64_t connection_diagnosis_control) {
    return ((connection_diagnosis_control) & (1 << OB_ENABLE_KEEP_CONNECTION)) == (1 << OB_ENABLE_KEEP_CONNECTION);
  }

public:
  ObConnectionDiagnosisTraceType trace_type_;
  ObConnectionDiagnosisInfo *diagnosis_info_;
  bool is_user_client_;
  bool is_first_packet_received_;
  bool is_detect_request_;
  bool is_com_quit_;
  bool is_detect_user_;
  proxy::ObProtocolDiagnosis *protocol_diagnosis_;
};

class ObConnectionDiagnosisInfo
{
public:
  ObConnectionDiagnosisInfo() { reset(); }
  virtual ~ObConnectionDiagnosisInfo() {}
  virtual void destroy() { reset(); op_free(this); }
  virtual void reset() {
    cs_id_ = 0;
    ss_id_ = 0;
    proxy_session_id_ = 0;
    server_session_id_ = 0;
    MEMSET(client_addr_, '\0', MAX_IP_ADDR_LENGTH);
    MEMSET(server_addr_, '\0', MAX_IP_ADDR_LENGTH);
    MEMSET(proxy_server_addr_, '\0', MAX_IP_ADDR_LENGTH);
    MEMSET(cluster_name_, '\0', OB_PROXY_MAX_CLUSTER_NAME_LENGTH);
    MEMSET(tenant_name_, '\0', OB_MAX_TENANT_NAME_LENGTH);
    MEMSET(user_name_, '\0', OB_MAX_USER_NAME_LENGTH);
    MEMSET(error_msg_, '\0', MAX_MSG_BUF_LEN + 1);
    error_code_ = 0;
    sql_cmd_ = obmysql::ObMySQLCmd::OB_MYSQL_COM_MAX_NUM;
    request_cmd_ = obmysql::ObMySQLCmd::OB_MYSQL_COM_MAX_NUM;
    req_total_time_ = 0;
  }
  virtual int64_t to_string(char *buf, const int64_t buf_len) const;
  void set_error_msg(const ObString msg);
public:
  int64_t cs_id_;
  int64_t ss_id_;
  int64_t proxy_session_id_;
  int64_t server_session_id_;
  char client_addr_[MAX_IP_ADDR_LENGTH];
  char server_addr_[MAX_IP_ADDR_LENGTH];
  char proxy_server_addr_[MAX_IP_ADDR_LENGTH];
  char cluster_name_[OB_PROXY_MAX_CLUSTER_NAME_LENGTH];
  char tenant_name_[OB_MAX_TENANT_NAME_LENGTH];
  char user_name_[OB_MAX_USER_NAME_LENGTH];
  int64_t error_code_;
  char error_msg_[MAX_MSG_BUF_LEN + 1];
  obmysql::ObMySQLCmd sql_cmd_;
  obmysql::ObMySQLCmd request_cmd_;
  ObHRTime req_total_time_;
};

class ObLoginDiagnosisInfo : public ObConnectionDiagnosisInfo
{
public:
  ObLoginDiagnosisInfo() { reset(); };
  ~ObLoginDiagnosisInfo() {};
  void destroy() { reset(); op_free(this); }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  char internal_sql_[OB_SHORT_SQL_LENGTH + 1];
};

class ObVCDiagnosisInfo : public ObConnectionDiagnosisInfo
{
public:
  ObVCDiagnosisInfo() { reset(); }
  ~ObVCDiagnosisInfo() {};
  void destroy() { reset(); op_free(this); }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();

public:
  int vc_event_;
  char user_sql_[OB_SHORT_SQL_LENGTH + 1];
};

class ObInactivityTimeoutDiagnosisInfo : public ObConnectionDiagnosisInfo
{
public:
  ObInactivityTimeoutDiagnosisInfo() { reset(); }
  ~ObInactivityTimeoutDiagnosisInfo() {};
  void destroy() { reset(); op_free(this); }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObInactivityTimeoutEvent inactivity_timeout_event_;
  ObHRTime timeout_;
};

class ObProxyInternalDiagnosisInfo : public ObConnectionDiagnosisInfo
{
public:
  ObProxyInternalDiagnosisInfo() { reset(); }
  ~ObProxyInternalDiagnosisInfo() {};
  void destroy() { reset(); op_free(this); }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  char user_sql_[OB_SHORT_SQL_LENGTH + 1];
};

}
}
}

#endif