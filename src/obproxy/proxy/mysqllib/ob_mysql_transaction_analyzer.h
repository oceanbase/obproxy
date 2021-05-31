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

#ifndef OBPROXY_MYSQL_TRANSACTION_ANALYZER_H
#define OBPROXY_MYSQL_TRANSACTION_ANALYZER_H
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "proxy/mysqllib/ob_mysql_resp_analyzer.h"
#include "proxy/mysqllib/ob_mysql_response.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "proxy/mysqllib/ob_i_mysql_respone_analyzer.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

// Proxy should keep transaction state.
//
// This class is bind to a transaction, every time when a
// transaction start this class instance should reset()
//
// when ObMysqlTransactionAnalyzer is constructed, the trans
// starts by default(is_in_trans_ = true, is_in_resp_ = true)
class ObMysqlTransactionAnalyzer : public ObIMysqlRespAnalyzer
{
public:
  ObMysqlTransactionAnalyzer();
  ~ObMysqlTransactionAnalyzer() {};

  void reset();
  void destroy() { reset(); }
  // analyse trans data, and determine whether the trans and resp is completed
  //
  // possible output:
  // <1>is_trans_completed = true, is_resp_completed = true
  //    the entire transaction is over, and current request(the last
  //    request of the transaction)'s response is over.
  //
  // <2>is_trans_completed = false, is_resp_completed = true
  //    the transaction is not over, but the current request's response is over
  //
  // <3>is_trans_completed = false, is_resp_completed = false
  //    both the transaction and the current response is not over
  //
  int analyze_trans_response(const ObRespBuffer &buff,
                             ObMysqlResp *resp = NULL);

  int analyze_trans_response(event::ObIOBufferReader &reader,
                             ObMysqlResp *resp = NULL);

  virtual int analyze_response(event::ObIOBufferReader &reader, ObMysqlResp *resp = NULL)
  {
    return analyze_trans_response(reader, resp);
  }

  int analyze_response(event::ObIOBufferReader &reader,
                       ObMysqlAnalyzeResult &result,
                       ObMysqlResp *resp,
                       const bool need_receive_complete);

  //before start analyze trans packets, must firstly set_sever_cmd and protocol mode
  void set_server_cmd(const obmysql::ObMySQLCmd cmd, const ObMysqlProtocolMode mode,
                      const bool enable_extra_ok_packet_for_stats,
                      const bool is_current_in_trans);

  //add for ut
  void set_server_cmd(const obmysql::ObMySQLCmd cmd, const ObMysqlProtocolMode mode,
                      const bool enable_extra_ok_packet_for_stats) {
    set_server_cmd(cmd, mode, enable_extra_ok_packet_for_stats, true);
  }

  inline obmysql::ObMySQLCmd get_server_cmd() const { return result_.get_cmd(); }

  inline bool is_trans_completed() const { return !is_in_trans_; }
  inline bool is_resp_completed() const { return !is_in_resp_; }

  // only after call analyze_one_packet_response() and it returns ANALYZE_DONE
  inline bool is_resultset_resp() const { return is_resultset_resp_; }
private:
  bool is_in_trans_;
  bool is_in_resp_;
  bool is_resultset_resp_;
  bool is_current_in_trans_;

  ObRespResult result_;
  ObMysqlRespAnalyzer analyzer_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OB_MYSQL_TRANSACTION_ANALYSER_H */
