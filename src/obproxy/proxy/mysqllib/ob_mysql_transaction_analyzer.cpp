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
#include "proxy/mysqllib/ob_mysql_transaction_analyzer.h"
#include "packet/ob_mysql_packet_reader.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::packet;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
typedef ObString ObResultBuffer;

ObMysqlTransactionAnalyzer::ObMysqlTransactionAnalyzer()
    : is_in_trans_(true),
      is_in_resp_(true),
      is_resultset_resp_(false),
      is_current_in_trans_(true)
{
  reset();
}

// Attention!! before start to analyze a trans, reset must be called
void ObMysqlTransactionAnalyzer::reset()
{
  is_in_trans_ = true;
  is_in_resp_ = true;
  is_resultset_resp_ = false;
  is_current_in_trans_ = true;
  result_.reset();
  analyzer_.reset();
}

// Attention!! before start to analyze a request' response, set_server_cmd must be called
void ObMysqlTransactionAnalyzer::set_server_cmd(const ObMySQLCmd cmd,
                                                const ObMysqlProtocolMode mode,
                                                const bool enable_extra_ok_packet_for_stats,
                                                const bool is_current_in_trans,
                                                const bool is_autocommit,
                                                const bool is_binlog_related)
{
  //when set server cmd, result_ must reset
  result_.reset();
  analyzer_.reset();

  is_in_resp_ = true;
  is_in_trans_ = true;
  is_resultset_resp_ = false;
  is_current_in_trans_ = is_current_in_trans;
  result_.set_cmd(cmd);
  result_.set_mysql_mode(mode);
  result_.set_enable_extra_ok_packet_for_stats(enable_extra_ok_packet_for_stats);
  analyzer_.set_mysql_mode(mode);
  analyzer_.set_binlog_relate(is_current_in_trans, is_autocommit, is_binlog_related);
}

int ObMysqlTransactionAnalyzer::analyze_trans_response(const ObResultBuffer &buf,
                                                              ObMysqlResp *resp /*= NULL*/)
{
  int ret = OB_SUCCESS;
  bool is_trans_completed = false;
  bool is_resp_completed = false;
  ObBufferReader buf_reader(buf);
  ObMysqlRespEndingType ending_type = MAX_PACKET_ENDING_TYPE;
  if (OB_UNLIKELY(buf_reader.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result buffer is empty", K(ret));
  } else if ((UNDEFINED_MYSQL_PROTOCOL_MODE == result_.get_mysql_mode())
              || (UNDEFINED_MYSQL_PROTOCOL_MODE == analyzer_.get_mysql_mode())) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql protocol mode was not set", K(ret));
  } else if (OB_FAIL(analyzer_.analyze_mysql_resp(buf_reader, result_, resp))) {
    LOG_WARN("fail to analyze mysql resp", K(ret));
  } else if (!analyzer_.need_wait_more_data()
             && OB_FAIL(result_.is_resp_finished(is_resp_completed, ending_type))) {
    LOG_WARN("fail to check is resp finished", K(ending_type), K(ret));
  } else {
    // this mysql response is complete
    if (is_resp_completed) {
      is_in_resp_ = false;
      ObRespTransState state = result_.get_trans_state();
      if (NOT_IN_TRANS_STATE_BY_PARSE == state) {
        is_in_trans_ = false;
      } else if (IN_TRANS_STATE_BY_PARSE == state) {
        is_in_trans_ = true;
      } else if(result_.is_mysql_mode() && !result_.is_extra_ok_packet_for_stats_enabled()) {
        is_in_trans_ = is_current_in_trans_;
      }
    } else {
      is_in_resp_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    is_trans_completed = !is_in_trans_;
    is_resp_completed = !is_in_resp_;
    // just defense
    if (OB_UNLIKELY(is_trans_completed) && OB_UNLIKELY(!is_resp_completed)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("state error", K(is_trans_completed), K(is_resp_completed), K(ret));
    } else if (OB_UNLIKELY(!is_resp_completed) && OB_UNLIKELY(is_trans_completed)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("state error", K(is_trans_completed), K(is_resp_completed), K(ret));
    }

    LOG_DEBUG("analyze trans response succ", "response data len", buf.length(),
              K(is_trans_completed), K(is_resp_completed), "mode",
              result_.get_mysql_mode(), "ObMysqlRespEndingType", ending_type);

    if (NULL != resp) {
      ObRespAnalyzeResult &analyze_result = resp->get_analyze_result();
      analyze_result.reserved_len_ = result_.get_reserved_len();
      if (is_resp_completed) {
        analyze_result.is_trans_completed_ = is_trans_completed;
        analyze_result.is_resp_completed_ = is_resp_completed;
        analyze_result.ending_type_ = ending_type;
        LOG_DEBUG("analyze result", K(*resp));
      }
    }
  }
  return ret;
}

int ObMysqlTransactionAnalyzer::analyze_trans_response(ObIOBufferReader &reader,
                                                              ObMysqlResp *resp /*= NULL*/)
{
  int ret = OB_SUCCESS;
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
    LOG_WARN("the first block data_size in reader is less than 0",
             K(offset), K(block->read_avail()), K(ret));
  }

  ObRespBuffer resp_buf;
  while (OB_SUCC(ret) && NULL != block && data_size > 0 && !is_resp_completed()) {
    resp_buf.assign_ptr(data, static_cast<int32_t>(data_size));
    if (OB_FAIL(analyze_trans_response(resp_buf, resp))) {
      LOG_WARN("fail to analyze transaction resp", K(ret));
    } else {
      // on to the next block
      offset = 0;
      block = block->next_;
      if (NULL != block) {
        data = block->start();
        data_size = block->read_avail();
        // just for defense
        if (is_resp_completed()) {
          if (OB_UNLIKELY(0 != data_size)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("resp completed but data_size != 0", K(data_size), K(ret));
          }
        }
      }
    }
  }
  LOG_DEBUG("analyze_trans_response finished", "cmd",
            ObProxyParserUtils::get_sql_cmd_name(get_server_cmd()), "data_size",
            reader.read_avail(), K(is_resp_completed()), K(is_trans_completed()),
            KPC(resp));

  return ret;
}

int receive_next_ok_packet(ObIOBufferReader &reader, ObMysqlAnalyzeResult &result) {
  int ret = OB_SUCCESS;
  ObMysqlAnalyzeResult tmp_result;
  ObIOBufferReader *tmp_reader = reader.mbuf_->clone_reader(&reader);
  if (OB_ISNULL(tmp_reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to reader", K(tmp_reader), K(ret));
    result.status_ = ANALYZE_ERROR;
  } else {
    int64_t first_pkt_len = result.meta_.pkt_len_;
    if (OB_UNLIKELY(tmp_reader->read_avail() < first_pkt_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buf reader is error", "read_avail", tmp_reader->read_avail(),
               K(first_pkt_len), K(ret));
    } else {
      if (OB_FAIL(tmp_reader->consume(first_pkt_len))) {
        PROXY_TXN_LOG(WARN, "fail to consume ", K(first_pkt_len), K(ret));
      } else if (tmp_reader->read_avail() > 0) {
        if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*tmp_reader, tmp_result))) {
          LOG_WARN("fail to analyze packet", K(tmp_reader), K(ret));
        }
      } else {
        tmp_result.status_ = ANALYZE_CONT;
      }
    }
    result.status_ = tmp_result.status_;
    reader.mbuf_->dealloc_reader(tmp_reader);
  }
  return ret;
}

int ObMysqlTransactionAnalyzer::analyze_response(ObIOBufferReader &reader,
                                                 ObMysqlAnalyzeResult &result,
                                                 ObMysqlResp *resp,
                                                 const bool need_receive_complete)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(need_receive_complete)) {
    if (OB_FAIL(analyze_trans_response(reader, resp))) {
      LOG_WARN("fail to analyze trans response", K(ret));
      result.status_ = ANALYZE_ERROR;
    } else {
      if (is_resp_completed()) {
        result.status_ = ANALYZE_DONE;
        if (resp->get_analyze_result().is_eof_resp()
            || ((OB_MYSQL_COM_STMT_PREPARE == result_.get_cmd() || OB_MYSQL_COM_STMT_PREPARE_EXECUTE == result_.get_cmd())
                 && !resp->get_analyze_result().is_error_resp())) {
          is_resultset_resp_ = true;
          resp->get_analyze_result().is_resultset_resp_ = true;
        }
      } else {
        result.status_ = ANALYZE_CONT;
      }
    }
  } else {
    ObServerStatusFlags server_status(0);
    if (OB_UNLIKELY(OB_MYSQL_COM_STATISTICS == result_.get_cmd())) {
      // OB_MYSQL_COM_STATISTICS, maybe only have mysql packet header, without packet body
      if (OB_FAIL(ObProxyParserUtils::analyze_one_packet_only_header(reader, result))) {
        LOG_WARN("fail to analyze packet", K(&reader), K(ret));
      }
    } else {
      // 1. first we should confirm ok pkt or error pkt is received completed
      if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(reader, result))) {
        LOG_WARN("fail to analyze packet", K(&reader), K(ret));
      }
    }

    if (ANALYZE_DONE == result.status_) {
      if (OB_LIKELY(analyzer_.is_oceanbase_mode())) {
        if (OB_UNLIKELY(OB_MYSQL_COM_STATISTICS == result_.get_cmd())) {
          if (result_.is_extra_ok_packet_for_stats_enabled()) {
            if (OB_FAIL(receive_next_ok_packet(reader, result))) {
              LOG_WARN("fail to receive next ok packet", K(ret));
            }
          }
        } else {
          if (MYSQL_OK_PACKET_TYPE == result.meta_.pkt_type_
              && OB_MYSQL_COM_STMT_PREPARE != result_.get_cmd() && OB_MYSQL_COM_STMT_PREPARE_EXECUTE != result_.get_cmd()) {
            ObMysqlPacketReader pkt_reader;
            if (OB_FAIL(pkt_reader.get_ok_packet_server_status(reader, server_status))) {
              LOG_WARN("fail to get server status flags", K(server_status.flags_), K(ret));
            } else {
              // do nothing
            }
          } else if (OB_UNLIKELY(MYSQL_ERR_PACKET_TYPE == result.meta_.pkt_type_)) {
            // 2. if the fist packet is an err pkt, it must be followed by an ok packet.
            //    in this case, we should also confirm the second ok packet is received competed.
            if (OB_FAIL(receive_next_ok_packet(reader, result))) {
              LOG_WARN("fail to receive next ok packet", K(ret));
            }
          }
        }
      }

      if (OB_SUCC(ret) && ANALYZE_DONE == result.status_) {
        // if it is result + eof + error + ok, it may be not....
        // treat multi stmt as result set protocol
        is_resultset_resp_ = ((OB_MYSQL_COM_QUERY == result_.get_cmd()
                               || OB_MYSQL_COM_STMT_EXECUTE == result_.get_cmd()
                               || OB_MYSQL_COM_STMT_FETCH == result_.get_cmd())
                              && (OB_MYSQL_COM_STATISTICS != result_.get_cmd())
                              && (server_status.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS
                                  || ((MYSQL_OK_PACKET_TYPE != result.meta_.pkt_type_)
                                       && (MYSQL_ERR_PACKET_TYPE != result.meta_.pkt_type_)
                                       && (MYSQL_EOF_PACKET_TYPE != result.meta_.pkt_type_)
                                       && (MYSQL_LOCAL_INFILE_TYPE != result.meta_.pkt_type_))))
                             || ((OB_MYSQL_COM_STMT_PREPARE == result_.get_cmd()
                                  || OB_MYSQL_COM_STMT_PREPARE_EXECUTE == result_.get_cmd())
                                 && MYSQL_ERR_PACKET_TYPE != result.meta_.pkt_type_)
                             || OB_MYSQL_COM_FIELD_LIST == result_.get_cmd();
        resp->get_analyze_result().is_resultset_resp_ = is_resultset_resp_;
        LOG_DEBUG("after analyze one response packet", "packet_type", result.meta_.pkt_type_,
                  "cmd", result_.get_cmd(),
                  "status_flag", server_status.flags_,
                  K_(is_resultset_resp));
        if ((!is_resultset_resp_ && COM_BINLOG_DUMP != result_.get_cmd() && COM_BINLOG_DUMP_GTID != result_.get_cmd())
              && OB_FAIL(analyze_trans_response(reader, resp))) {
          result.status_ = ANALYZE_ERROR;
        }
      }
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
