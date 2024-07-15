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

#define USING_LOG_PREFIX PROXY

#include "proxy/mysqllib/ob_oceanbase_20_request_analyzer.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/checksum/ob_crc16.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "lib/utility/ob_2_0_full_link_trace_info.h"
#include "proxy/mysqllib/ob_protocol_diagnosis.h"
#include "proxy/mysqllib/ob_resp_analyzer_util.h"

using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

static int64_t const MYSQL_BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K);
int64_t ObOceanBase20RequestAnalyzer::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(
       K_(remain_head_checked_len),
       K_(extra_len),
       K_(extra_checked_len));
  J_OBJ_END();
  return pos;
}

int ObOceanBase20RequestAnalyzer::analyze_first_request(ObIOBufferReader &reader,
                                                        ObAnalyzeHeaderResult &result,
                                                        ObProxyMysqlRequest &req,
                                                        ObMysqlAnalyzeStatus &status,
                                                        Ob20ExtraInfo &extra_info,
                                                        FLTObjManage &flt)
{
  UNUSED(req);
  
  int ret = OB_SUCCESS;
  result.reset();
  extra_info.reset();
  flt.reset();

  if (OB_FAIL(ObProto20Utils::analyze_one_ob20_packet_header(reader, result, false))) { // 7 + 24 header decode finish
    LOG_WDIAG("fail to analyze one compressed packet", K(ret));
  } else if (ANALYZE_DONE == result.status_) {
    status = result.status_;

    // total ob20 request received complete here, total_len = ob20.head.compressed_len + mysql compress head(3+1+3)
    // get extra info from ob20 payload
    if (OB_FAIL(analyze_first_request(reader, result, extra_info, flt))) {
      LOG_WDIAG("fail to analyze compress packet payload", K(ret));
      status = ANALYZE_ERROR;
    }
  }

  return ret;
}

int ObOceanBase20RequestAnalyzer::analyze_first_request(ObIOBufferReader &reader,
                                                        ObAnalyzeHeaderResult &ob20_result,
                                                        Ob20ExtraInfo &extra_info,
                                                        FLTObjManage &flt)
{
  int ret = OB_SUCCESS;

  if (!ob20_result.ob20_header_.flag_.is_extra_info_exist()) {
    LOG_DEBUG("no extra info flag in ob20 head");
  } else {
    ObIOBufferBlock *block = NULL;
    int64_t offset = 0;
    char *data = NULL;
    int64_t data_size = 0;
    if (NULL != reader.block_) {
      reader.skip_empty_blocks();
      block = reader.block_;
      offset = reader.start_offset_;
      data = block->start() + offset;
      data_size = block->read_avail() - offset;
    }

    if (data_size <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("the first block data_size in reader is less than 0", K(offset), K(block->read_avail()), K(ret));
    }

    ob20_req_analyze_state_ = OB20_REQ_ANALYZE_HEAD;    // begin
    ObString req_buf;
    
    while (OB_SUCC(ret) && block != NULL && data_size > 0 && ob20_req_analyze_state_ != OB20_REQ_ANALYZE_END) {
      req_buf.assign_ptr(data, static_cast<int32_t>(data_size));
      if (OB_FAIL(stream_analyze_request(req_buf, ob20_result, extra_info, flt))) {  //only extra info now
        LOG_WDIAG("fail to decompress request packet", K(ret));
      } else {
        offset = 0;
        block = block->next_;
        if (NULL != block) {
          data = block->start();
          data_size = block->read_avail();
        }
      }
    }
  }

  LOG_DEBUG("analyze ob20 request payload finished", K(ret), K(reader.read_avail()), K(ob20_result));

  return ret;
}

// check the buffer cross different blocks
int ObOceanBase20RequestAnalyzer::stream_analyze_request(ObString &req_buf,
                                                         ObAnalyzeHeaderResult &ob20_result,
                                                         Ob20ExtraInfo &extra_info,
                                                         FLTObjManage &flt)
{
  int ret = OB_SUCCESS;

  if (req_buf.length() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected argument check", K(ret), K(req_buf));
  } else {
    const char *buf_start = req_buf.ptr();
    int64_t buf_len = req_buf.length();
    
    while (OB_SUCC(ret) && buf_len > 0 && ob20_req_analyze_state_ != OB20_REQ_ANALYZE_END) {
      switch (ob20_req_analyze_state_) {
        case OB20_REQ_ANALYZE_HEAD: {
          if (OB_FAIL(do_req_head_decode(buf_start, buf_len))) {
            LOG_WDIAG("fail to to req head decode", K(ret));
          }
          break;
        }
        case OB20_REQ_ANALYZE_EXTRA: {
          if (OB_FAIL(do_req_extra_decode(buf_start, buf_len, ob20_result, extra_info, flt))) {
            LOG_WDIAG("fail to do req extra decode", K(ret));
          }
          break;
        }
        case OB20_REQ_ANALYZE_END: {
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("ob20 analyze decompress req unknown status", K(ret), K(ob20_req_analyze_state_));
        } 
      }
    }
  }

  return ret;
}

int ObOceanBase20RequestAnalyzer::do_req_head_decode(const char* &buf_start, int64_t &buf_len)
{
  int ret = OB_SUCCESS;

  if (remain_head_checked_len_ == 0) {
    remain_head_checked_len_ = MYSQL_COMPRESSED_OB20_HEALDER_LENGTH;
  } else if (remain_head_checked_len_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected remain head checked len", K(ret), K(remain_head_checked_len_));
  } else {
    // remain_head_checked_len_ > 0, nothing
  }

  if (OB_SUCC(ret)) {
    if (remain_head_checked_len_ > buf_len) {
      remain_head_checked_len_ -= buf_len;
      buf_start += buf_len;
      buf_len = 0;
    } else {
      buf_start += remain_head_checked_len_;
      buf_len -= remain_head_checked_len_;
      remain_head_checked_len_ = 0;
      ob20_req_analyze_state_ = OB20_REQ_ANALYZE_EXTRA;
    }
  }

  LOG_DEBUG("ob20 do req head decode", K(remain_head_checked_len_), K(buf_len), K(ob20_req_analyze_state_));

  return ret;
}

int ObOceanBase20RequestAnalyzer::do_req_extra_decode(const char *&buf_start,
                                                     int64_t &buf_len,
                                                     ObAnalyzeHeaderResult &ob20_result,
                                                     Ob20ExtraInfo &extra_info,
                                                     FLTObjManage &flt)
{
  int ret = OB_SUCCESS;

  if (ob20_result.ob20_header_.flag_.is_extra_info_exist()) {
    // extra len
    if (extra_len_ == 0) {
      uint32_t extra_remain_len = static_cast<uint32_t>(OB20_PROTOCOL_EXTRA_INFO_LENGTH - extra_checked_len_);
      uint32_t extra_header_len = static_cast<uint32_t>(MIN(buf_len, extra_remain_len));

      MEMCPY(temp_buf_ + extra_checked_len_, buf_start, extra_header_len);
      extra_checked_len_ += extra_header_len;
      buf_start += extra_header_len;
      buf_len -= extra_header_len;

      if (extra_checked_len_ == OB20_PROTOCOL_EXTRA_INFO_LENGTH) {
        char *temp_buf = temp_buf_;
        ObMySQLUtil::get_uint4(temp_buf, extra_len_);
        extra_info.extra_len_ = extra_len_;
        extra_checked_len_ = 0;
        LOG_DEBUG("ob20 do req extra len decode success", K(extra_len_), K(buf_len));
        extra_info.extra_info_buf_.reset();
      }
    }

    // extra info
    int64_t extra_remain_len = extra_len_ - extra_checked_len_;
    if (extra_len_ > 0 && buf_len > 0 && extra_remain_len > 0) {
      if (!extra_info.extra_info_buf_.is_inited()
          && OB_FAIL(extra_info.extra_info_buf_.init(extra_len_))) {
        LOG_WDIAG("fail to init buf", K(ret));
      }
      if (OB_SUCC(ret)) {
        uint32_t curr_extra_len = static_cast<uint32_t>(MIN(buf_len, extra_remain_len));
        if (OB_FAIL(extra_info.extra_info_buf_.write(buf_start, curr_extra_len))) {
          LOG_WDIAG("fail to write to buf", K(ret), K(buf_len));
        } else {
          extra_checked_len_ += curr_extra_len;
          buf_start += curr_extra_len;
          buf_len -= curr_extra_len;

          // decode total extra info
          if (extra_checked_len_ == extra_len_) {
            const char *buf = extra_info.extra_info_buf_.ptr();
            const int64_t len = extra_info.extra_info_buf_.len();
            if (ob20_result.ob20_header_.flag_.is_new_extra_info()) {
              if (OB_FAIL(ObRespAnalyzerUtil::do_new_extra_info_decode(buf, len, extra_info, flt))) {
                LOG_WDIAG("fail to do new extra info decode", K(ret));
              }
            } else {
              if (OB_FAIL(ObRespAnalyzerUtil::do_obobj_extra_info_decode(buf, len, extra_info, flt))) {
                LOG_WDIAG("fail to do req obobj extra info decode", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              ob20_req_analyze_state_ = OB20_REQ_ANALYZE_END;
              LOG_DEBUG("ob20 req analyzer analyzed finished");
            }
          } // decode total extra info
        }
      }
    }
  } else {
    ob20_req_analyze_state_ = OB20_REQ_ANALYZE_END;
    LOG_DEBUG("no extra info flag, set to analyze end");
  }

  return ret;
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
