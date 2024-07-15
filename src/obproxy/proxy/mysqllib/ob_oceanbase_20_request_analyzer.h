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

#ifndef OBPROXY_MYSQL_COMPRESS_OB20_ANALYZER_H
#define OBPROXY_MYSQL_COMPRESS_OB20_ANALYZER_H

#include "proxy/mysqllib/ob_2_0_protocol_utils.h"
#include "proxy/mysqllib/ob_2_0_protocol_struct.h"

namespace oceanbase
{

namespace common
{
class ObObj;
class FLTObjManage;
}

namespace obproxy
{
namespace proxy
{

enum OB20ReqAnalyzeState
{
  OB20_REQ_ANALYZE_HEAD,
  OB20_REQ_ANALYZE_EXTRA,
  OB20_REQ_ANALYZE_END,
  OB20_REQ_ANALYZE_MAX
};

class ObOceanBase20RequestAnalyzer
{
public:
  ObOceanBase20RequestAnalyzer()
    : remain_head_checked_len_(0),
      extra_len_(0),
      extra_checked_len_(0),
      ob20_req_analyze_state_(OB20_REQ_ANALYZE_HEAD) {}
  int analyze_first_request(event::ObIOBufferReader &reader, ObAnalyzeHeaderResult &result,
                            ObProxyMysqlRequest &req, ObMysqlAnalyzeStatus &status,
                            Ob20ExtraInfo &extra_info, FLTObjManage &flt);
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  int analyze_first_request(event::ObIOBufferReader &reader, ObAnalyzeHeaderResult &result,
                            Ob20ExtraInfo &extra_info, FLTObjManage &flt);
  int stream_analyze_request(common::ObString &req_buf, ObAnalyzeHeaderResult &ob20_result,
                             Ob20ExtraInfo &extra_info, FLTObjManage &flt);

  int do_req_head_decode(const char* &buf_start, int64_t &buf_len);
  int do_req_extra_decode(const char *&buf_start, int64_t &buf_len,
                          ObAnalyzeHeaderResult &ob20_result,
                          Ob20ExtraInfo &extra_info, FLTObjManage &flt);
private:
    
  int64_t remain_head_checked_len_;
  uint32_t extra_len_;
  uint32_t extra_checked_len_;
  enum OB20ReqAnalyzeState ob20_req_analyze_state_;
  char temp_buf_[OB20_PROTOCOL_EXTRA_INFO_LENGTH];          // temp 4 extra len buffer
  char header_buf_[MYSQL_COMPRESSED_OB20_HEALDER_LENGTH];   // temp 7+24 head buffer
private:
  DISALLOW_COPY_AND_ASSIGN(ObOceanBase20RequestAnalyzer);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_MYSQL_COMPRESS_OB20_ANALYZER_H */
