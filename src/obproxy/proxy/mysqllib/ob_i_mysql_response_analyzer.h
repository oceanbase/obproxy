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

#ifndef OBPROXY_I_MYSQL_RESPONSE_ANALYZER_H
#define OBPROXY_I_MYSQL_RESPONSE_ANALYZER_H

#include "rpc/obmysql/ob_mysql_packet.h"
#include "obutils/ob_proxy_buf.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
class ObIOBufferReader;
class ObMIOBuffer;
}
namespace proxy
{
class ObProtocolDiagnosis;
class ObMysqlResp;
class ObIMysqlRespAnalyzer
{
public:
  ObIMysqlRespAnalyzer() : protocol_diagnosis_(NULL) {}
  ~ObIMysqlRespAnalyzer();
  virtual int analyze_response(event::ObIOBufferReader &reader, ObMysqlResp *resp = NULL) = 0;
  virtual void reset() = 0;
  ObProtocolDiagnosis *&get_protocol_diagnosis_ref();
  ObProtocolDiagnosis *get_protocol_diagnosis();
  const ObProtocolDiagnosis *get_protocol_diagnosis() const;
protected:
  ObProtocolDiagnosis *protocol_diagnosis_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIMysqlRespAnalyzer);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_I_MYSQL_RESPONSE_ANALYZER_H */
