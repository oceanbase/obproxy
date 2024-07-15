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

#ifndef OBPROXY_MYSQL_RESP_ANALYZER_H
#define OBPROXY_MYSQL_RESP_ANALYZER_H

#include "rpc/obmysql/ob_mysql_packet.h"
#include "obutils/ob_proxy_buf.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObRespAnalyzeResult;
class ObProtocolDiagnosis;
typedef common::ObString ObRespBuffer;
static const int64_t FIXED_MEMORY_BUFFER_SIZE = 32;

enum ObMysqlProtocolMode
{
  UNDEFINED_MYSQL_PROTOCOL_MODE = 0,
  STANDARD_MYSQL_PROTOCOL_MODE,
  OCEANBASE_MYSQL_PROTOCOL_MODE,
  OCEANBASE_ORACLE_PROTOCOL_MODE,
};

// this class is binded to Server Command. when new Server
// Command arrive, this class should be reset.`
class ObRespPacketAnalyzeResult
{
public:
  ObRespPacketAnalyzeResult();
  ~ObRespPacketAnalyzeResult() {}
  void reset();
  int is_resp_finished(bool &finished,
                       ObMysqlRespEndingType &ending_type,
                       obmysql::ObMySQLCmd req_cmd,
                       ObMysqlProtocolMode protocol_mode,
                       bool is_extra_ok_for_stats) const;

  inline void set_cmd(const obmysql::ObMySQLCmd cmd) { cmd_ = cmd; }
  obmysql::ObMySQLCmd get_cmd() const { return cmd_; }

  int64_t get_pkt_cnt(const ObMysqlRespEndingType type) const { return pkt_cnt_[type]; }
  void inc_pkt_cnt(const ObMysqlRespEndingType type) { ++pkt_cnt_[type]; }
  void reset_pkt_cnt() { memset(pkt_cnt_, 0, sizeof(pkt_cnt_)); }

  int32_t get_all_pkt_cnt() const { return all_pkt_cnt_; }
  void inc_all_pkt_cnt() { ++all_pkt_cnt_; }

  int32_t get_expect_pkt_cnt() const { return expect_pkt_cnt_; }
  void set_expect_pkt_cnt(const int32_t expect_pkt_cnt) { expect_pkt_cnt_ = expect_pkt_cnt; }

  bool is_recv_resultset() const { return is_recv_resultset_; }
  void set_recv_resultset(const bool is_recv_resultset) { is_recv_resultset_ = is_recv_resultset; }

  ObMysqlRespType get_resp_type() const { return resp_type_; }
  void set_resp_type(const ObMysqlRespType type) { resp_type_ = type; }

  ObRespTransState get_trans_state() const { return trans_state_; }
  void set_trans_state(const ObRespTransState state) { trans_state_ = state; }

  ObMysqlProtocolMode get_mysql_mode() const { return mysql_mode_; }
  inline void set_mysql_mode(const ObMysqlProtocolMode mode) { mysql_mode_ = mode; }
  bool is_oceanbase_mode() const { return OCEANBASE_MYSQL_PROTOCOL_MODE == mysql_mode_ || OCEANBASE_ORACLE_PROTOCOL_MODE == mysql_mode_; }
  bool is_oceanbase_mysql_mode() const { return OCEANBASE_MYSQL_PROTOCOL_MODE == mysql_mode_; }
  bool is_oceanbase_oracle_mode() const { return OCEANBASE_ORACLE_PROTOCOL_MODE == mysql_mode_; }
  bool is_mysql_mode() const { return STANDARD_MYSQL_PROTOCOL_MODE == mysql_mode_; }

  void set_enable_extra_ok_packet_for_stats(const bool enable_extra_ok_packet_for_stats) {
    enable_extra_ok_packet_for_stats_ = enable_extra_ok_packet_for_stats;
  }
  bool is_extra_ok_packet_for_stats_enabled() const {
    return enable_extra_ok_packet_for_stats_;
  }
private:
  bool enable_extra_ok_packet_for_stats_; // deprecated
  obmysql::ObMySQLCmd cmd_; // deprecated
  ObMysqlProtocolMode mysql_mode_; // deprecated
  ObMysqlRespType resp_type_;
  ObRespTransState trans_state_;
  int32_t all_pkt_cnt_;
  int32_t expect_pkt_cnt_;
  bool is_recv_resultset_;
  int8_t pkt_cnt_[OB_MYSQL_RESP_ENDING_TYPE_COUNT];
  DISALLOW_COPY_AND_ASSIGN(ObRespPacketAnalyzeResult);
};
inline void ObRespPacketAnalyzeResult::reset()
{
  enable_extra_ok_packet_for_stats_ = false;
  cmd_ = obmysql::OB_MYSQL_COM_END;
  mysql_mode_ = UNDEFINED_MYSQL_PROTOCOL_MODE;
  resp_type_ = MAX_RESP_TYPE;
  trans_state_ = IN_TRANS_STATE_BY_DEFAULT;
  all_pkt_cnt_ = 0;
  expect_pkt_cnt_ = 0;
  is_recv_resultset_ = false;
  memset(pkt_cnt_, 0, sizeof(pkt_cnt_));
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_MYSQL_RESP_ANALYZER_H */
