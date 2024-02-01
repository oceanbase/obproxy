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

#include "ob_mysql_request_prepare_transform_plugin.h"
#include "proxy/mysqllib/ob_mysql_analyzer_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obmysql;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObMysqlRequestPrepareTransformPlugin *ObMysqlRequestPrepareTransformPlugin::alloc(ObApiTransaction &transaction, int64_t req_pkt_len)
{
  return op_reclaim_alloc_args(ObMysqlRequestPrepareTransformPlugin, transaction, req_pkt_len);
}

ObMysqlRequestPrepareTransformPlugin::ObMysqlRequestPrepareTransformPlugin(ObApiTransaction &transaction, int64_t req_pkt_len)
  : ObTransformationPlugin(transaction, ObTransformationPlugin::REQUEST_TRANSFORMATION),
    ps_pkt_len_(req_pkt_len), copy_offset_(0), ps_or_text_ps_sql_buf_(NULL), local_reader_(NULL)
{
  PROXY_API_LOG(DEBUG, "ObMysqlRequestPrepareTransformPlugin born", K(this));
}

void ObMysqlRequestPrepareTransformPlugin::destroy()
{
  PROXY_API_LOG(DEBUG, "ObMysqlRequestPrepareTransformPlugin destroy", K(this));
  ObTransformationPlugin::destroy();
  if (NULL != ps_or_text_ps_sql_buf_ && ps_pkt_len_ > 0) {
    op_fixed_mem_free(ps_or_text_ps_sql_buf_, ps_pkt_len_);
  }
  ps_or_text_ps_sql_buf_ = NULL;
  copy_offset_ = 0;
  ps_pkt_len_ = 0;
  op_reclaim_free(this);
}

int ObMysqlRequestPrepareTransformPlugin::consume(event::ObIOBufferReader *reader)
{
  PROXY_API_LOG(DEBUG, "ObMysqlRequestPrepareTransformPlugin::consume happen");
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_API_LOG(WDIAG, "invalid argument", K(reader), K(ret));
  } else {
    int64_t produce_size = 0;
    int64_t local_read_avail = 0;

    if (NULL == local_reader_) {
      local_reader_ = reader->clone();
      if (NULL != ps_or_text_ps_sql_buf_) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_API_LOG(EDIAG, "ps_or_text_ps_sql_buf_ must be NULL here", K_(ps_or_text_ps_sql_buf), K(ret));
      } else if (OB_UNLIKELY(ps_pkt_len_ >= MYSQL_PACKET_MAX_LENGTH)) {
        ret = OB_NOT_SUPPORTED;
        PROXY_API_LOG(WDIAG, "we cannot support packet which is larger than 16MB", K_(ps_pkt_len),
            K(MYSQL_PACKET_MAX_LENGTH), K(ret));
      } else if (OB_ISNULL(ps_or_text_ps_sql_buf_ = static_cast<char *>(op_fixed_mem_alloc(ps_pkt_len_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_API_LOG(WDIAG, "fail to alloc mem for ps sql buf", K_(ps_or_text_ps_sql_buf), K_(ps_pkt_len), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      local_read_avail = local_reader_->read_avail();
      if (local_read_avail > 0) {
        if (OB_FAIL(copy_ps_or_text_ps_sql(local_read_avail))) {
          PROXY_API_LOG(WDIAG, "fail to copy ps sql", K(local_read_avail),  K(ret));
        } else if (copy_offset_ >= ps_pkt_len_) {
          if (copy_offset_ > ps_pkt_len_) {
            ret = OB_ERR_UNEXPECTED;
            PROXY_API_LOG(EDIAG, "copied packet length is larger than ps_pkt_len",
                K_(copy_offset), K_(ps_pkt_len), K(ret));
          } else if (OB_MYSQL_COM_STMT_PREPARE == sm_->trans_state_.trans_info_.sql_cmd_ &&
              OB_FAIL(handle_ps_prepare())) {
            PROXY_API_LOG(WDIAG, "fail to handle ps prepare", K(ret));
          } else if (sm_->trans_state_.trans_info_.client_request_.get_parse_result().is_text_ps_prepare_stmt()
              && OB_FAIL(handle_text_ps_prepare())) {
            PROXY_API_LOG(WDIAG, "fail to handle text ps prepare", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (local_read_avail != (produce_size = produce(local_reader_, local_read_avail))) {
            ret = OB_ERR_UNEXPECTED;
            PROXY_API_LOG(WDIAG, "fail to produce", "expected size", local_read_avail,
                "actual size", produce_size, K(ret));
          } else if (OB_FAIL(local_reader_->consume(local_read_avail))) {
            PROXY_API_LOG(WDIAG, "fail to consume local transfer reader", K(local_read_avail), K(ret));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    sm_->trans_state_.inner_errcode_ = ret;
    // if failed set to INTERNAL_ERROR
    sm_->trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
  }

  return ret;
}

int ObMysqlRequestPrepareTransformPlugin::copy_ps_or_text_ps_sql(int64_t copy_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(copy_size + copy_offset_ > ps_pkt_len_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(WDIAG, "invalid packet", K(copy_size), K_(copy_offset), K_(ps_pkt_len), K(ret));
  } else {
    local_reader_->copy(ps_or_text_ps_sql_buf_ + copy_offset_, copy_size, 0);
    copy_offset_ += copy_size;
  }
  return ret;
}

int ObMysqlRequestPrepareTransformPlugin::handle_ps_prepare()
{
  int ret = OB_SUCCESS;
  ObString ps_sql;
  ps_sql.assign_ptr(ps_or_text_ps_sql_buf_ + MYSQL_NET_META_LENGTH, static_cast<int32_t>(ps_pkt_len_ - MYSQL_NET_META_LENGTH));
  if (OB_FAIL(sm_->do_analyze_ps_prepare_request(ps_sql))) {
    PROXY_API_LOG(WDIAG, "fail to do_analyze_ps_prepare_request", K(ps_sql), K(ret));
  }

  return ret;
}

int ObMysqlRequestPrepareTransformPlugin::handle_text_ps_prepare()
{
  int ret = OB_SUCCESS;
  ObString text_ps_sql;
  text_ps_sql.assign_ptr(ps_or_text_ps_sql_buf_ + MYSQL_NET_META_LENGTH, static_cast<int32_t>(ps_pkt_len_ - MYSQL_NET_META_LENGTH));
  if (OB_FAIL(sm_->do_analyze_text_ps_prepare_request(text_ps_sql))) {
    PROXY_API_LOG(WDIAG, "fail to do_analyze_text_ps_prepare_request", K(text_ps_sql), K(ret));
  }

  return ret;
}

void ObMysqlRequestPrepareTransformPlugin::handle_input_complete()
{
  PROXY_API_LOG(DEBUG, "ObMysqlRequestPrepareTransformPlugin::handle_input_complete happen");
  if (NULL != local_reader_) {
    local_reader_->dealloc();
    local_reader_ = NULL;
  }

  set_output_complete();
}

int ObMysqlRequestPrepareTransformPlugin::check_last_data_segment(
    event::ObIOBufferReader &reader,
    bool &is_last_segment)
{
  int ret = OB_SUCCESS;
  const int64_t read_avail = reader.read_avail();
  int64_t ntodo = -1;
  if (OB_FAIL(get_write_ntodo(ntodo))) {
    PROXY_API_LOG(EDIAG, "fail to get write ntodo", K(ret));
  } else if (OB_UNLIKELY(ntodo <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(EDIAG, "get_data_to_read must > 0", K(ntodo), K(ret));
  } else if (OB_UNLIKELY(read_avail > ntodo)) { // just defense
    if (read_avail >= (ntodo + MYSQL_NET_META_LENGTH)) {
      char tmp_buff[MYSQL_NET_META_LENGTH]; // print the next packet's meta
      reader.copy(tmp_buff, MYSQL_NET_META_LENGTH, ntodo);
      int64_t payload_len = uint3korr(tmp_buff);
      int64_t seq = uint1korr(tmp_buff + 3);
      int64_t cmd = static_cast<uint8_t>(tmp_buff[4]);
      PROXY_API_LOG(EDIAG, "next packet meta is", K(payload_len), K(seq), K(cmd));
    }
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(EDIAG, "invalid data, maybe client received more than one mysql packet,"
                  " will disconnect", K(read_avail), K(ntodo), K(ret));
  } else {
    is_last_segment = (read_avail == ntodo);
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
