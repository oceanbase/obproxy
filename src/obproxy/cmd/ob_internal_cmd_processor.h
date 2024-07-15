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

#ifndef OBPROXY_INTERNAL_CMD_PROCESSOR_H
#define OBPROXY_INTERNAL_CMD_PROCESSOR_H

#include "lib/utility/ob_print_utils.h"
#include "lib/lock/ob_drw_lock.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "opsql/parser/ob_proxy_parse_result.h"

#include "iocore/eventsystem/ob_event.h"
#include "iocore/net/ob_inet.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "proxy/mysqllib/ob_oceanbase_20_header_param.h"

#define  INTERNAL_CMD_EVENTS_SUCCESS   INTERNAL_CMD_EVENTS_START + 1
#define  INTERNAL_CMD_EVENTS_FAILED    INTERNAL_CMD_EVENTS_START + 2

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObProxyReloadConfig;
}
namespace event
{
class ObAction;
class ObEvent;
class ObContinuation;
class ObMIOBuffer;
}

class ObProxySessionPrivInfo;

#define DEBUG_ICMD(fmt...) PROXY_ICMD_LOG(DEBUG, ##fmt)
#define INFO_ICMD(fmt...) PROXY_ICMD_LOG(INFO, ##fmt)
#define WARN_ICMD(fmt...) PROXY_ICMD_LOG(WARN, ##fmt)
#define ERROR_ICMD(fmt...) PROXY_ICMD_LOG(ERROR, ##fmt)
#define WDIAG_ICMD(fmt...) PROXY_ICMD_LOG(WDIAG, ##fmt)
#define EDIAG_ICMD(fmt...) PROXY_ICMD_LOG(EDIAG, ##fmt)

const int64_t PROXY_LIKE_NAME_MAX_SIZE = common::OB_MAX_CONFIG_NAME_LEN;

class ObInternalCmdInfo
{
public:
  ObInternalCmdInfo() { reset(); }
  ~ObInternalCmdInfo() { }

  void reset();

  uint8_t get_pkt_seq() const { return pkt_seq_; }
  obmysql::ObMySQLCapabilityFlags get_capability() const { return capability_; }
  bool is_internal_cmd() const { return is_internal_user_ || (type_ > OBPROXY_T_INVALID && type_ < OBPROXY_T_ICMD_MAX); }
  bool is_error_cmd() const { return OBPROXY_T_ERR_INVALID != err_type_; }
  bool is_error_cmd_need_resp_ok() const { return OBPROXY_T_ERR_NEED_RESP_OK == err_type_; }
  bool is_mysql_compatible_cmd() const
  {
    return (OBPROXY_T_ICMD_SHOW_PROCESSLIST == type_ || OBPROXY_T_ICMD_KILL_MYSQL == type_);
  }
  ObProxyBasicStmtType get_cmd_type() const { return type_; }
  ObProxyBasicStmtSubType get_sub_cmd_type() const { return sub_type_; }
  ObProxyErrorStmtType get_err_cmd_type() const { return err_type_; }
  int64_t get_limit_offset() const { return limit_offset_; }
  int64_t get_limit_rows() const { return limit_rows_; }
  int64_t get_memory_limit() const { return memory_limit_; }
  int64_t get_sm_id() const { return sm_id_; }
  int64_t get_cs_id() const { return cs_id_; }
  int64_t get_first_int() const { return first_int_; }
  int64_t get_thread_id() const { return thread_id_; }
  int64_t get_ss_id() const { return ss_id_; }
  int64_t get_attempt_limit() const { return attempt_limit_; }
  int64_t get_log_id() const { return log_id_; }
  const common::ObString &get_like_string() const { return first_string_; }
  const common::ObString &get_key_string() const { return first_string_; }
  const common::ObString &get_value_string() const { return second_string_; }
  const common::ObString &get_cluster_string() const { return first_string_; }
  const common::ObString &get_large_key_string() const { return second_string_; }

  proxy::ObProxyProtocol get_protocol() const { return protocol_; }
  proxy::Ob20HeaderParam &get_ob20_head_param() { return ob20_param_; }
  const proxy::Ob20HeaderParam &get_ob20_head_param() const { return ob20_param_; }

  void set_protocol(const proxy::ObProxyProtocol protocol) { protocol_ = protocol; }
  void set_ob20_head_param(const proxy::Ob20HeaderParam &param) { ob20_param_ = param; }

  void set_pkt_seq(const uint8_t pkt_seq) { pkt_seq_ = pkt_seq; }
  void set_cmd_type(const ObProxyBasicStmtType type) { type_ = type; }
  void set_sub_cmd_type(const ObProxyBasicStmtSubType type) { sub_type_ = type; }
  void set_err_cmd_type(const ObProxyErrorStmtType type) { err_type_ = type; }
  void set_memory_limit (const int64_t internal_cmd_mem_limit) { memory_limit_ = internal_cmd_mem_limit; }
  void set_capability(const obmysql::ObMySQLCapabilityFlags capability) { capability_ = capability; }
  void set_integers(const int64_t first_int, const int64_t second_int, const int64_t third_int)
  {
    first_int_ = first_int;
    second_int_ = second_int;
    third_int_ = third_int;
  }
  void set_internal_user(bool internal_user) { is_internal_user_ = internal_user;}

  void deep_copy_strings(const common::ObString first_string, const common::ObString second_string)
  {
    int32_t min_len = 0;
    if (!first_string.empty()) {
      min_len = std::min(first_string.length(), static_cast<int32_t>(sizeof(first_str_)));
      MEMCPY(first_str_, first_string.ptr(), min_len);
      first_string_.assign_ptr(first_str_, min_len);
    }
    if (!second_string.empty()) {
      min_len = std::min(second_string.length(), static_cast<int32_t>(sizeof(second_str_)));
      MEMCPY(second_str_, second_string.ptr(), min_len);
      second_string_.assign_ptr(second_str_, min_len);
    }
  }

  int64_t to_string(char* buf, const int64_t buf_len) const;

public:
  ObProxySessionPrivInfo *session_priv_;  

private:
  uint8_t pkt_seq_;
  ObProxyBasicStmtType type_;
  ObProxyBasicStmtSubType sub_type_;
  ObProxyErrorStmtType err_type_;
  obmysql::ObMySQLCapabilityFlags capability_;
  int64_t memory_limit_;
  bool is_internal_user_;

  union {
    int64_t first_int_;
    int64_t sm_id_;//for sm use
    int64_t cs_id_;//for client session use
    int64_t thread_id_;//for net connection use
  };
  union {
    int64_t second_int_;
    int64_t ss_id_;//for kill server session
    int64_t limit_offset_;//for net connection limit use
    int64_t attempt_limit_;//for net connection limit use
    int64_t log_id_;//for show warnlog
  };
  union {
    int64_t third_int_;
    int64_t limit_rows_;//for net connection limit use
  };

  common::ObString first_string_;
  common::ObString second_string_;
  char first_str_[common::OB_MAX_CONFIG_NAME_LEN];
  char second_str_[common::OB_MAX_CONFIG_VALUE_LEN];

  proxy::ObProxyProtocol protocol_;
  proxy::Ob20HeaderParam ob20_param_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObInternalCmdInfo);
};

typedef int (*ObInternalCmdCallbackFunc) (event::ObContinuation *cont, ObInternalCmdInfo &info,
                                          event::ObMIOBuffer *buf, event::ObAction *&action);

struct ObCmdTableInfo
{
  ObCmdTableInfo() : type_(OBPROXY_T_INVALID), func_(NULL) {}
  ~ObCmdTableInfo() {}

  ObProxyBasicStmtType type_;
  ObInternalCmdCallbackFunc func_;
  TO_STRING_KV("type", get_obproxy_stmt_name(type_), K_(func));
};

class ObInternalCmdProcessor
{
public:
  ObInternalCmdProcessor() :is_inited_(false), reload_config_(NULL), internal_cmd_lock_()
  {
    memset(cmd_table_, 0, sizeof(cmd_table_));
  }

  ~ObInternalCmdProcessor() {}

  int init(obutils::ObProxyReloadConfig *reload_config);
  int execute_cmd(event::ObContinuation *cont, ObInternalCmdInfo &info,
                  event::ObMIOBuffer *buf, event::ObAction *&action);
  //register funcs
  int register_cmd(const ObProxyBasicStmtType type, ObInternalCmdCallbackFunc func, bool skip_type_check = false);
  obutils::ObProxyReloadConfig *get_reload_config() { return reload_config_; }

private:
  bool is_inited_;
  obutils::ObProxyReloadConfig *reload_config_; //used for alter config set
  ObCmdTableInfo cmd_table_[OBPROXY_T_MAX];
  common::DRWLock internal_cmd_lock_;
  DISALLOW_COPY_AND_ASSIGN(ObInternalCmdProcessor);
};

ObInternalCmdProcessor &get_global_internal_cmd_processor();

}//end of obproxy
}//end of oceanbase

#endif /* OBPROXY_INTERNAL_CMD_PROCESSOR_H */


