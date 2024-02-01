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

#define USING_LOG_PREFIX RPC_OBMYSQL
#include "ompk_prepare_execute_req.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
namespace oceanbase
{
using namespace common;
using namespace obproxy;
using namespace proxy;

namespace obmysql
{

int OMPKPrepareExecuteReq::decode()
{
  int ret = OB_SUCCESS;
  const char *pos = cdata_;
  int64_t data_len = hdr_.len_;
  if (NULL != cdata_) {
    uint64_t query_len = 0;
    // skip cmd
    if (OB_FAIL(ObMysqlPacketUtil::get_uint1(pos, data_len, cmd_))) {
      LOG_WDIAG("fail to get uint1", K(data_len), K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_uint4(pos, data_len, statement_id_))) {
      LOG_WDIAG("fail to get uint4", K(data_len), K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_uint1(pos, data_len, flags_))) {
      LOG_WDIAG("fail to get uint1", K(data_len), K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_uint4(pos, data_len, iteration_count_))) {
      LOG_WDIAG("fail to get uint4", K(data_len), K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_length(pos, data_len, query_len))) {
      LOG_WDIAG("fail to get length", K(data_len), K(ret));
    // skip query
    } else if (FALSE_IT(pos += query_len)) {
    } else if (OB_FAIL(ObMysqlPacketUtil::get_uint4(pos, data_len, param_num_))) {
      LOG_WDIAG("fail to get uint4", K(data_len), K(ret));
    // skip null bitmap
    } else if (param_num_ <= 0) {
      LOG_DEBUG("skip read param related", K(param_num_));
    } else if (FALSE_IT(pos += (param_num_ + 7)/8)) {
    } else if (OB_FAIL(ObMysqlPacketUtil::get_uint1(pos, data_len, new_params_bound_flag_))) {
      LOG_WDIAG("fail to get uint1", K(data_len), K(ret));
    } else if (new_params_bound_flag_ == 1 
               && OB_FAIL(ObMysqlRequestAnalyzer::parse_param_type(param_num_, param_types_, type_infos_, pos, data_len))) {
      LOG_WDIAG("fail to parse param type", K_(param_num), K(pos), K(data_len), K(ret));
    } else { /* do nothing */ }
    // skip the rest of data
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("null input", K(ret), K(cdata_));
  }
  return ret;
}

} //end of obmysql
} //end of oceanbase
