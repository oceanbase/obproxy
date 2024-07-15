/**
 * Copyright (c) 2024 OceanBase
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

#include "proxy/mysqllib/ob_resp_analyzer.h"
#include "proxy/mysqllib/ob_compressed_pkt_analyzer.h"
#include "proxy/mysqllib/ob_resp_packet_analyze_result.h"
#include "lib/checksum/ob_crc16.h"
#include "proxy/mysqllib/ob_2_0_protocol_utils.h" // ObProto20Utils::analyze_first_mysql_packet
#include "rpc/obmysql/packet/ompk_prepare.h" // OMPKPrepare
#include "proxy/mysqllib/ob_resp_analyzer_util.h"
#include "obproxy/packet/ob_mysql_packet_reader.h"

using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
using namespace oceanbase::obproxy::packet;
namespace proxy
{

int ObRespAnalyzer::init(
    ObProxyProtocol protocol,
    obmysql::ObMySQLCmd req_cmd,
    ObMysqlProtocolMode protocol_mode,
    ObRespAnalyzeMode analyze_mode,
    bool is_extra_ok_for_stats,
    bool is_compressed,
    uint8_t last_ob_req,
    uint8_t last_compressed_seq,
    uint32_t request_id,
    uint32_t sess_id,
    bool enable_trans_checksum)
{
  reset();
  int ret = OB_SUCCESS;
  protocol_ = protocol;
  req_cmd_ = req_cmd;
  protocol_mode_ = protocol_mode;
  analyze_mode_ = analyze_mode;
  if (protocol == ObProxyProtocol::PROTOCOL_OB20) {
    last_ob_seq_ = last_ob_req;
    request_id_ = request_id;
  } else if (protocol == ObProxyProtocol::PROTOCOL_CHECKSUM) {
    last_compressed_seq_ = last_compressed_seq;
    request_id_ = 0;
  }
  stream_ob20_state_ = STREAM_OCEANBASE20_HEADER;
  stream_compressed_state_ = STREAM_COMPRESSED_MYSQL_HEADER;
  stream_mysql_state_ = STREAM_MYSQL_HEADER;
  sess_id_ = sess_id;
  params_.is_extra_ok_for_stats_ = is_extra_ok_for_stats;
  params_.is_compressed_ = is_compressed;
  ob20_analyzer_.set_enable_transmission_checksum(enable_trans_checksum);
  if (is_decompress_mode() && OB_FAIL(alloc_mysql_pkt_buf())) {
    LOG_WDIAG("fail to init resp analyzer because of failing to alloc_mysql_pkt_buf", K(analyze_mode_), K(ret));
  } else {
    is_inited_ = true;
    LOG_DEBUG("succ to init resp analyzer", K(protocol_), K(req_cmd_), K(protocol_mode_),
              K(analyze_mode_), K(last_ob_seq_), K(last_compressed_seq_),
              K(request_id_), K(sess_id_), K(is_extra_ok_for_stats),
              K(is_compressed), K(enable_trans_checksum));
  }

  return ret;
}

int ObRespAnalyzer::init(
    obmysql::ObMySQLCmd req_cmd,
    ObMysqlProtocolMode protocol_mode,
    bool is_extra_ok_for_stats,
    bool is_in_trans,
    bool is_autocommit,
    bool is_binlog_req)
{
  reset();
  int ret = OB_SUCCESS;
  stream_mysql_state_ = STREAM_MYSQL_HEADER;
  protocol_ = ObProxyProtocol::PROTOCOL_NORMAL;
  req_cmd_ = req_cmd;
  protocol_mode_ = protocol_mode;
  params_.is_extra_ok_for_stats_ = is_extra_ok_for_stats;
  params_.is_in_trans_ = is_in_trans;
  params_.is_autocommit_ = is_autocommit;
  params_.is_binlog_req_ = is_binlog_req;
  is_inited_ = true;
  LOG_DEBUG("succ to init resp analyzer",
            K(protocol_), K(req_cmd_), K(protocol_mode_),
            K(is_extra_ok_for_stats), K(is_in_trans),
           K(is_autocommit), K(is_binlog_req));

  return ret;
}

int ObRespAnalyzer::handle_analyze_last_mysql(ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  bool is_trans_completed = false;
  bool is_resp_completed = false;
  ObMysqlRespEndingType ending_type = MAX_PACKET_ENDING_TYPE;
  if (OB_FAIL(mysql_resp_result_.is_resp_finished(is_resp_completed, ending_type, req_cmd_, protocol_mode_,
                                                  params_.is_extra_ok_for_stats_))) {
      LOG_WDIAG("fail to check is resp finished", K(ending_type), K(ret));
  } else {
    // this mysql response is complete
    if (is_resp_completed) {
      ObRespTransState state = mysql_resp_result_.get_trans_state();
      if (NOT_IN_TRANS_STATE_BY_PARSE == state) {
        is_trans_completed = true;
      } else if (is_mysql_mode() && params_.is_extra_ok_for_stats_) {
        is_trans_completed = !params_.is_in_trans_;
      }
    }

    // just defense
    if (OB_UNLIKELY(is_trans_completed) && OB_UNLIKELY(!is_resp_completed)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("state error", K(is_trans_completed), K(is_resp_completed), K(ret));
    }

    if (is_resp_completed) {
      resp_result.is_trans_completed_ = is_trans_completed;
      resp_result.is_resp_completed_ = is_resp_completed;
      resp_result.ending_type_ = ending_type;
      is_mysql_stream_end_ = true;
      LOG_DEBUG("mark mysql stream end", K(is_mysql_stream_end_), K(is_trans_completed), K(is_resp_completed));
    }
    LOG_DEBUG("succ to handle_analyze_last_mysql",
              K(is_trans_completed), K(is_resp_completed),
              "mode", mysql_resp_result_.get_mysql_mode(),
              "ObMysqlRespEndingType", ending_type);
  }

  return ret;
}

int ObRespAnalyzer::handle_analyze_mysql_end(const char *pkt_end, ObRespAnalyzeResult *resp_result)
{
  int ret = OB_SUCCESS;
  if (STREAM_MYSQL_END == stream_mysql_state_) {
    last_mysql_pkt_seq_ = mysql_analyzer_.get_pkt_seq(); // save last pkt seq
    bool is_ob_mode = is_oceanbase_mode();
    reserved_len_ = 0; // after read one whole packet, we reset reserved_len_
    mysql_resp_result_.inc_all_pkt_cnt();
    int64_t eof_pkt_cnt = mysql_resp_result_.get_pkt_cnt(EOF_PACKET_ENDING_TYPE);
    int64_t err_pkt_cnt = mysql_resp_result_.get_pkt_cnt(ERROR_PACKET_ENDING_TYPE);
    int64_t string_eof_pkt_cnt = mysql_resp_result_.get_pkt_cnt(STRING_EOF_ENDING_TYPE);
    int64_t prepare_ok_pkt_cnt = mysql_resp_result_.get_pkt_cnt(PREPARE_OK_PACKET_ENDING_TYPE);
    ObMysqlRespEndingType pkt_type = ending_type_;
    uint32_t pkt_len = mysql_analyzer_.get_pkt_len();
    // in these case, we will reset pkt cnt:
    // 1. ok packet (has more result)
    // 2. the second eof packet (has more result)
    ObOKPacketActionType ok_packet_action_type = OK_PACKET_ACTION_SEND;
    bool is_in_trans = false;
    switch (pkt_type) {
      case OK_PACKET_ENDING_TYPE : {
        // in any case, we should analyze packet
        if (OB_FAIL(analyze_ok_pkt(pkt_end, is_in_trans))) {
          LOG_WDIAG("fail to analyze_ok_pkt", K(ret));
        } else {
          if (is_in_trans) {
            mysql_resp_result_.set_trans_state(IN_TRANS_STATE_BY_PARSE);
          } else {
            mysql_resp_result_.set_trans_state(NOT_IN_TRANS_STATE_BY_PARSE);
          }
        }
        // judge pacekt action
        if (OB_SUCC(ret)) {
          if (OB_LIKELY(is_ob_mode)) {
            if (cur_stmt_has_more_result_) {
              // in multi stmt, send directly
              ok_packet_action_type = OK_PACKET_ACTION_SEND;
            } else {
              if (OB_MYSQL_COM_FIELD_LIST == req_cmd_ && 1 == eof_pkt_cnt) {
                ok_packet_action_type = OK_PACKET_ACTION_CONSUME;
              /* stmt prepare execute 的 OK 包, 除了 error 包后的 OK 包之外, 都是要透传的 */
              } else if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE == req_cmd_) {
                if (1 == err_pkt_cnt) {
                  ok_packet_action_type = OK_PACKET_ACTION_CONSUME;
                } else {
                  ok_packet_action_type = OK_PACKET_ACTION_REWRITE;
                }
              } else if (OB_MYSQL_COM_STMT_GET_PIECE_DATA == req_cmd_) {
                ok_packet_action_type = OK_PACKET_ACTION_CONSUME;
              } else if (1 == prepare_ok_pkt_cnt) {
                //stmt_prepare extra ok atfter
                ok_packet_action_type = OK_PACKET_ACTION_CONSUME;
              } else if (1 == string_eof_pkt_cnt) {
                ok_packet_action_type = OK_PACKET_ACTION_CONSUME;
              } else if (1 == err_pkt_cnt) {
                // extra ok after err packet
                ok_packet_action_type = OK_PACKET_ACTION_CONSUME;
              } else if (2 == eof_pkt_cnt) {
                // extra ok after result set
                ok_packet_action_type = OK_PACKET_ACTION_CONSUME;
              } else if (1 == eof_pkt_cnt && OB_MYSQL_COM_STMT_EXECUTE == req_cmd_ && mysql_resp_result_.is_recv_resultset()) {
                ok_packet_action_type = OK_PACKET_ACTION_CONSUME;
              } else if (1 == eof_pkt_cnt && OB_MYSQL_COM_STMT_FETCH == req_cmd_) {
                ok_packet_action_type = OK_PACKET_ACTION_CONSUME;
              } else if (0 == err_pkt_cnt || 0 == eof_pkt_cnt) {
                // last ok packet, no err and eof in front
                // NOTE: in multi stmt, we will reset pkt cnt if it isn't the last stmt
                ok_packet_action_type = OK_PACKET_ACTION_REWRITE;
              } else {
                // is not valid in mysql protocol
                ret = OB_ERR_UNEXPECTED;
                LOG_WDIAG("unexpected ok packet", K(err_pkt_cnt), K(eof_pkt_cnt), K(ret));
              }
            }
          } else {
            // in mysql mode send it directly
            ok_packet_action_type = OK_PACKET_ACTION_SEND;
          }
        }

        // do not reserved ok packet
        reserved_len_ = 0;

        // record last_ok_pkt_len
        if (OK_PACKET_ACTION_CONSUME == ok_packet_action_type
            || OK_PACKET_ACTION_REWRITE == ok_packet_action_type) {
          if (OB_NOT_NULL(resp_result)) {
            resp_result->last_ok_pkt_len_ = pkt_len + MYSQL_NET_HEADER_LENGTH;
          }
        } else {
          // do nothing
        }

        if (cur_stmt_has_more_result_) {
          // has more stmt, reset pkt_cnt and record flag
          mysql_resp_result_.reset_pkt_cnt();
        } else {
          mysql_resp_result_.inc_pkt_cnt(OK_PACKET_ENDING_TYPE);
        }
        break;
      }

      case PREPARE_OK_PACKET_ENDING_TYPE : {
        if (OB_FAIL(analyze_prepare_ok_pkt())) {
          LOG_WDIAG("fail to analyze_prepare_ok_pkt", K(ret));
        } else {
          mysql_resp_result_.inc_pkt_cnt(PREPARE_OK_PACKET_ENDING_TYPE);
        }

        break;
      }

      case EOF_PACKET_ENDING_TYPE : {
        /* 普通协议下, 第二个 EOF 就是最后一个 EOF */
        bool is_last_eof_pkt = (1 == eof_pkt_cnt);

        if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE == req_cmd_) {
          uint32_t expect_pkt_cnt = mysql_resp_result_.get_expect_pkt_cnt();
          uint32_t all_pkt_cnt = mysql_resp_result_.get_all_pkt_cnt();
          if (all_pkt_cnt > expect_pkt_cnt) {
            is_last_eof_pkt = true;
            /* 如果是最后一个 EOF 了, 那结果一定接收完了 */
            mysql_resp_result_.set_recv_resultset(true);
          } else {
            is_last_eof_pkt = false;
          }
        } else if (OB_MYSQL_COM_CHANGE_USER == req_cmd_ && resp_result != NULL) {
          resp_result->is_auth_switch_req_ = true;
        }

        if (0 == eof_pkt_cnt) {
          // analyze the first eof packet
          bool is_in_trans = false;
          if (OB_FAIL(analyze_eof_pkt(pkt_end, is_in_trans, is_last_eof_pkt))) {
            LOG_WDIAG("fail to analyze_eof_pkt", K(ret));
          } else {
            if (is_in_trans) {
              mysql_resp_result_.set_trans_state(IN_TRANS_STATE_BY_PARSE);
            } else {
              mysql_resp_result_.set_trans_state(NOT_IN_TRANS_STATE_BY_PARSE);
            }

            if (is_last_eof_pkt) {
              handle_last_eof(pkt_end, pkt_len);
              if (OB_MYSQL_COM_STMT_EXECUTE == req_cmd_) {
                mysql_resp_result_.set_recv_resultset(true);
              }
            }
          }
        } else if (is_last_eof_pkt) {
          handle_last_eof(pkt_end, pkt_len);
        } else if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE != req_cmd_){
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected eof packet", K(err_pkt_cnt), K(eof_pkt_cnt), K(ret));
          ok_packet_action_type = OK_PACKET_ACTION_SEND;
        }

        // if has more result, reset the packet cnt if we received the second eof
        if (cur_stmt_has_more_result_ && is_last_eof_pkt) {
          mysql_resp_result_.reset_pkt_cnt();
        } else {
          mysql_resp_result_.inc_pkt_cnt(EOF_PACKET_ENDING_TYPE);
        }
        break;
      }

      case ERROR_PACKET_ENDING_TYPE : {
        if (OB_FAIL(analyze_error_pkt(resp_result))) {
          LOG_WDIAG("fail to analyze_error_pkt", K(ret));
        } else if (OB_LIKELY(is_ob_mode)) {
          // resultset protocol, after read the error packet, hold it until the extra ok
          // packet is read completed.
          reserved_len_ = pkt_len + MYSQL_NET_HEADER_LENGTH;
        }
        // inc the error packet count
        if (OB_SUCC(ret)) {
          mysql_resp_result_.inc_pkt_cnt(ERROR_PACKET_ENDING_TYPE);
        }
        break;
      }

      case HANDSHAKE_PACKET_ENDING_TYPE: {
        if (OB_MYSQL_COM_HANDSHAKE == req_cmd_) {
          if (OB_FAIL(analyze_hanshake_pkt(resp_result))) {
            LOG_WDIAG("fail to analyze_error_pkt", K(ret));
          } else {
            mysql_resp_result_.inc_pkt_cnt(HANDSHAKE_PACKET_ENDING_TYPE);
          }
        }
        break;
      }

      case STRING_EOF_ENDING_TYPE : {
        if (OB_LIKELY(is_ob_mode) && params_.is_extra_ok_for_stats_) {
          // STRING_EOF_ENDING_TYPE will followed by an ok packet
          reserved_len_ = pkt_len + MYSQL_NET_HEADER_LENGTH;
        }
        mysql_resp_result_.inc_pkt_cnt(STRING_EOF_ENDING_TYPE);
        LOG_DEBUG("string eof ending type packet", "is_ob_mode", is_ob_mode,
                  "is_extra_ok_for_stats",
                  params_.is_extra_ok_for_stats_);
        break;
      }

      case LOCAL_INFILE_ENDING_TYPE: {
        if (OB_UNLIKELY(LOCAL_INFILE_RESP_TYPE == mysql_resp_result_.get_resp_type())) {
          mysql_resp_result_.inc_pkt_cnt(LOCAL_INFILE_ENDING_TYPE);
        }
        break;
      }

      case MAX_PACKET_ENDING_TYPE : { // handle common mysql packet
        // for multi packet, not test now
        if (MYSQL_PACKET_MAX_LENGTH != pkt_len && is_in_multi_pkt_) {
          is_in_multi_pkt_ = false;
          LOG_INFO("now large packet receive complete", K_(is_in_multi_pkt));
        }
        break;
      }

      default : {
        LOG_WDIAG("invalid packet type", K(pkt_type));
        break;
      }
    }

    reserve_pkt_body_buf_.reset();

    if (OB_LIKELY(NULL != resp_result)) {
      resp_result->ok_packet_action_type_ = ok_packet_action_type;
      if (is_last_pkt() && OB_FAIL(handle_analyze_last_mysql(*resp_result))) {
        LOG_WDIAG("fail to handle_analyze_last_mysql", K(ret), K(*resp_result));
      }
    } else {
      // do nothing
    }
    mysql_analyzer_.reset();
    stream_mysql_state_ = STREAM_MYSQL_HEADER;
  }

  return ret;
}

void ObRespAnalyzer::handle_analyze_mysql_header(const int64_t analyzed)
{
  reserved_len_ += analyzed;
  is_in_multi_pkt_ = (mysql_analyzer_.get_pkt_len() == MYSQL_PACKET_MAX_LENGTH);
  LOG_DEBUG("analyze mysql pkt", K(analyzed),
            "len", mysql_analyzer_.get_pkt_len(),
            "seq", mysql_analyzer_.get_pkt_seq(),
            "type/data", mysql_analyzer_.get_pkt_type());
}

int ObRespAnalyzer::handle_analyze_mysql_body(const char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (reserve_pkt_body_buf_.remain() > 0) {
    int64_t copy = std::min(len, reserve_pkt_body_buf_.remain());
    if (OB_FAIL(reserve_pkt_body_buf_.write(buf, copy))) {
      LOG_DEBUG("fail to write", K(ret), K(buf), K(copy), K(len));
    }
  }

  if (OB_SUCC(ret)) {
    if (need_reserve_pkt() || (params_.is_binlog_req_ && EOF_PACKET_ENDING_TYPE == ending_type_)) {
      reserved_len_ += len;
    } else {
      reserved_len_ = 0;
    }
  }

  return ret;
}

int ObRespAnalyzer::handle_not_analyze_mysql_pkt_type()
{
  int ret = OB_SUCCESS;
  PROTOCOL_DIAGNOSIS(SINGLE_MYSQL_WITH_FOLD, recv, protocol_diagnosis_,
                     mysql_analyzer_.get_pkt_len(),
                     mysql_analyzer_.get_pkt_seq(),
                     0,
                     (ending_type_ == MAX_PACKET_ENDING_TYPE) ?
                      ObMysqlPacketRecord::get_fold_type(mysql_resp_result_) : OB_PACKET_FOLD_TYPE_NONE);
  if (OB_UNLIKELY(OB_MYSQL_COM_STATISTICS == req_cmd_)) {
    int64_t string_eof_pkt_cnt = mysql_resp_result_.get_pkt_cnt(STRING_EOF_ENDING_TYPE);
    if (0 == string_eof_pkt_cnt) {
      ending_type_ = STRING_EOF_ENDING_TYPE;
    } else if (1 == string_eof_pkt_cnt) {
      if (params_.is_extra_ok_for_stats_) {
        // will followed by an ok packet
        ending_type_ = OK_PACKET_ENDING_TYPE;
        if (OB_FAIL(reserve_pkt_body_buf_.init(OK_PKT_MAX_LEN))) {
          LOG_WDIAG("fail to alloc mem", K(OK_PKT_MAX_LEN), K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid string_eof_pkt_cnt", K(string_eof_pkt_cnt), K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid string_eof_pkt_cnt", K(string_eof_pkt_cnt), K(ret));
    }
  } else {
    ending_type_ = MAX_PACKET_ENDING_TYPE;
  }

  stream_mysql_state_ = STREAM_MYSQL_BODY;

  return ret;
}

int ObRespAnalyzer::handle_analyze_mysql_pkt_type(const char *buf)
{
  int ret = OB_SUCCESS;
  PROTOCOL_DIAGNOSIS(SINGLE_MYSQL_WITH_FOLD, recv, protocol_diagnosis_,
                     mysql_analyzer_.get_pkt_len(),
                     mysql_analyzer_.get_pkt_seq(),
                     mysql_analyzer_.get_pkt_type(),
                     (ending_type_ == MAX_PACKET_ENDING_TYPE) ?
                      ObMysqlPacketRecord::get_fold_type(mysql_resp_result_) : OB_PACKET_FOLD_TYPE_NONE);
  reserved_len_ += 1;
  // binlog service seq check
  if(OB_UNLIKELY(OB_MYSQL_COM_BINLOG_DUMP == req_cmd_ || OB_MYSQL_COM_BINLOG_DUMP_GTID == req_cmd_)) {
    uint8_t seq = mysql_analyzer_.get_pkt_seq();
    if (last_mysql_pkt_seq_ < 255 && (last_mysql_pkt_seq_ + 1 != seq)) {
      LOG_EDIAG("BINLOG_DUMP seq is not expected", K(last_mysql_pkt_seq_), K(seq));
    } else if (last_mysql_pkt_seq_ == 255 && 0 != seq) {
      LOG_EDIAG("BINLOG_DUMP seq is not expected", K(last_mysql_pkt_seq_), K(seq));
    } else {
      LOG_DEBUG("check binlog dump seq pass", K(last_mysql_pkt_seq_), K(seq));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(update_ending_type())) {
    LOG_WDIAG("fail to update ending type", K(ret));
  } else {
    if (OB_LIKELY(OB_MYSQL_COM_QUERY == req_cmd_ ||
                  OB_MYSQL_COM_PROCESS_INFO == req_cmd_ ||
                  OB_MYSQL_COM_STMT_PREPARE == req_cmd_ ||
                  OB_MYSQL_COM_STMT_FETCH == req_cmd_ ||
                  OB_MYSQL_COM_STMT_EXECUTE == req_cmd_ ||
                  OB_MYSQL_COM_STMT_PREPARE_EXECUTE == req_cmd_ ||
                  OB_MYSQL_COM_STMT_GET_PIECE_DATA == req_cmd_ ||
                  OB_MYSQL_COM_STMT_SEND_PIECE_DATA == req_cmd_) &&
        OB_LIKELY(MAX_RESP_TYPE == mysql_resp_result_.get_resp_type())) {
      if (ERROR_PACKET_ENDING_TYPE == ending_type_ || OK_PACKET_ENDING_TYPE == ending_type_) {
        mysql_resp_result_.set_resp_type(OTHERS_RESP_TYPE);
      } else if (OB_UNLIKELY(LOCAL_INFILE_ENDING_TYPE == ending_type_)) {
        mysql_resp_result_.set_resp_type(LOCAL_INFILE_RESP_TYPE);
      } else {
        mysql_resp_result_.set_resp_type(RESULT_SET_RESP_TYPE);
      }
    }

    if (need_copy_ok_pkt() || (params_.is_binlog_req_ && EOF_PACKET_ENDING_TYPE == ending_type_)) {
      // if ok packet, just copy OK_PACKET_MAX_COPY_LEN(default is 20) len to improve efficiency
      int64_t mem_len = (OK_PACKET_ENDING_TYPE == ending_type_ ? OK_PKT_MAX_LEN : mysql_analyzer_.get_pkt_len());
      if (OB_FAIL(reserve_pkt_body_buf_.init(mem_len))) {
        LOG_WDIAG("fail to alloc mem", K(ret), K(mem_len));
      //解析 handshake/prepare ok 时是交给 OMPKHandshake/OMPKPrepare 类去解析的, 会按照标准格式解析
      //所以这里要把第一个字节也保存下来
      } else if ((HANDSHAKE_PACKET_ENDING_TYPE == ending_type_ || PREPARE_OK_PACKET_ENDING_TYPE == ending_type_)
                 && OB_FAIL(reserve_pkt_body_buf_.write(buf, 1))) {
        LOG_WDIAG("fail to write handshake protocol version", K(ret));
      }
    }
  }
  return ret;
}

void ObRespAnalyzer::handle_analyze_ob20_tailer(ObRespAnalyzeResult &resp_result)
{
  if (stream_ob20_state_ == STREAM_OCEANBASE20_END) {
    LOG_DEBUG("analyze oceanbase 2.0 tailer", "tailer_crc", ob20_analyzer_.get_local_payload_checksum());
    if (is_last_oceanbase_pkt()) {
      if (SIMPLE_MODE == analyze_mode_) {
        resp_result.is_resp_completed_ = true; // 不设置会导致 tunnel 出现问题
        is_mysql_stream_end_ = true;
      }
      is_oceanbase_stream_end_ = true;
      LOG_DEBUG("mark oceanbase stream end",
                K(is_compressed_stream_end_),
                K(is_oceanbase_stream_end_),
                K(is_mysql_stream_end_));
    }
    ob20_analyzer_.reset();
    stream_ob20_state_ = STREAM_OCEANBASE20_HEADER;
  } else {
    LOG_DEBUG("continue to analyze data of oceanbase 2.0 tailer",
              "tailer_to_read", ob20_analyzer_.get_tailer_to_read());
  }
}

void ObRespAnalyzer::handle_analyze_compressed_mysql_header()
{
  if (STREAM_COMPRESSED_MYSQL_PAYLOAD == stream_compressed_state_) {
    const ObMysqlCompressedPacketHeader &hdr = compressed_analyzer_.get_header();
    PROTOCOL_DIAGNOSIS(COMPRESSED_MYSQL, recv, protocol_diagnosis_,
                       hdr.compressed_len_, hdr.seq_, hdr.non_compressed_len_);
  }
}

int ObRespAnalyzer::handle_analyze_compressed_oceanbase_payload(const char *buf, const int64_t len, ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  if (params_.is_compressed_) {
    int64_t filled_len = 0;
    bool stop = false;
    if (OB_FAIL(compressor_.add_decompress_data(buf, len))) {
      LOG_WDIAG("fail to add decompress data", K(ret));
    }
    char ob20_pkt[DEFAULT_PKT_BUFFER_SIZE];
    while (OB_SUCC(ret) && !stop) {
      if (OB_FAIL(compressor_.decompress(ob20_pkt, DEFAULT_PKT_BUFFER_SIZE, filled_len))) {
        LOG_WDIAG("fail to decompress", KP(ob20_pkt), K(DEFAULT_PKT_BUFFER_SIZE), K(filled_len), K(ret));
      } else {
        //if (pkt_buf_len > filled_len) { // complete
        if (DEFAULT_PKT_BUFFER_SIZE > filled_len) { // complete
          stop = true;
        }
        ObString seg(filled_len, ob20_pkt);
        if (OB_FAIL(stream_analyze_oceanbase(seg, resp_result))) {
          LOG_WDIAG("fail to stream_analyze_oceanbase", K(ret), K(seg), K(resp_result));
        } else {
        }
      }
    }
  } else {
    ObString seg(len, buf);
    if (OB_FAIL(stream_analyze_oceanbase(seg, resp_result))) {
      LOG_WDIAG("fail to stream_analyze_oceanbase", K(ret), K(seg), K(resp_result));
    }
  }

  if (stream_compressed_state_ == STREAM_COMPRESSED_END) {
    stream_compressed_state_ = STREAM_COMPRESSED_MYSQL_HEADER;
    // the last compressed packet which contains the last oceanbase packet was analyzed
    if (is_oceanbase_stream_end_) {     // oceanbase crc tailer analyzed
      is_compressed_stream_end_ = true; // mark compressed crc tailer analyzed
      LOG_DEBUG("mark compressed oceanbase stream end",
                K(is_compressed_stream_end_),
                K(is_oceanbase_stream_end_),
                K(is_mysql_stream_end_));
    }
    compressed_analyzer_.reset();
  }

  return ret;
}

int ObRespAnalyzer::handle_analyze_compressed_mysql_payload(const char *buf, const int64_t len, ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  if (is_decompress_mode()) {
    // payload is compressed by zlib or ...
    if (compressed_analyzer_.is_compressed_payload()) {
      if (OB_FAIL(handle_analyze_compressed_payload(buf, len, resp_result))) {
        LOG_WDIAG("fail to handle_analyze_compressed_payload", K(ret));
      }
    // payload is not compressed
    } else {
      if (OB_FAIL(handle_analyze_mysql_payload(buf, len, resp_result))) {
        LOG_WDIAG("fail to handle_analyze_mysql_payload", K(ret));
      }
    }
  } else {
    LOG_DEBUG("simple mode, skip compressed (mysql) payload");
  }
  if (stream_compressed_state_ == STREAM_COMPRESSED_END) {
    // the last compressed packet which contains the last mysql packet was analyzed
    if (is_last_compressed_pkt()) {
      if (SIMPLE_MODE == analyze_mode_) {
        resp_result.is_resp_completed_ = true; // 不设置会导致 tunnel 出现问题
        is_mysql_stream_end_ = true;
      }
      is_compressed_stream_end_ = true; // mark compressed mysql packet stream end
      LOG_DEBUG("mark compressed mysql stream end",
                K(is_compressed_stream_end_),
                K(is_oceanbase_stream_end_),
                K(is_mysql_stream_end_));
    }
    last_compressed_seq_ = compressed_analyzer_.get_header().seq_;
    compressed_analyzer_.reset();
    stream_compressed_state_ = STREAM_COMPRESSED_MYSQL_HEADER;
  } else {
    LOG_DEBUG("continue to analyze data of compressed payload",
              "payload_to_read", compressed_analyzer_.get_payload_to_read(),
              "header", compressed_analyzer_.get_header());
  }
  return ret;
}

// for mysql payload
int ObRespAnalyzer::handle_analyze_mysql_payload(const char *buf, const int64_t len, ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  ObString seg;
  seg.assign_ptr(buf, len);
  int64_t filled = 0;
  if (OB_FAIL(mysql_pkt_buf_->write(buf, len, filled))) {
    LOG_WDIAG("fail to write mysql pkt to buf", K(ret), KP(buf), K(len), K(filled));
  } else if (OB_UNLIKELY(len != filled)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to write mysql packet data", K(len), K(filled), K(ret));
  } else if (OB_FAIL(stream_analyze_mysql(seg, resp_result))) {
    LOG_WDIAG("fail to stream_analyze_mysql", K(ret), K(seg), K(resp_result));
  } else {
    resp_result.reserved_ok_len_of_compressed_ = reserved_len_;
  }

  return ret;
}

// for compressed mysql payload
int ObRespAnalyzer::handle_analyze_compressed_payload(const char *buf, const int64_t len, ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  int64_t filled_len = 0;
  char *pkt_buf = NULL;
  int64_t pkt_buf_len = 0;
  bool stop = false;
  if (OB_FAIL(compressor_.add_decompress_data(buf, len))) {
    LOG_WDIAG("fail to add decompress data", K(ret));
  }
  while (OB_SUCC(ret) && !stop) {
    if (OB_FAIL(mysql_pkt_buf_->get_write_avail_buf(pkt_buf, pkt_buf_len))) {
      LOG_WDIAG("fail to get successive vuf", K(pkt_buf_len), K(ret));
    } else if (OB_ISNULL(pkt_buf) || (0 == pkt_buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid argument", KP(pkt_buf), K(pkt_buf_len), K(ret));
    } else if (OB_FAIL(compressor_.decompress(pkt_buf, pkt_buf_len, filled_len))) {
      LOG_WDIAG("fail to decompress", KP(pkt_buf), K(pkt_buf_len), K(ret));
    } else {
      if (filled_len > 0) {
        mysql_pkt_buf_->fill(filled_len);
      }
      if (pkt_buf_len > filled_len) { // complete
        stop = true;
      }
      ObString seg(filled_len, pkt_buf);
      if (OB_FAIL(stream_analyze_mysql(seg, resp_result))) {
        LOG_WDIAG("fail to stream_analyze_mysql", K(ret), K(seg), K(resp_result));
      } else {
        resp_result.reserved_ok_len_of_compressed_ = reserved_len_;
      }
    }
  }

  return ret;
}

int ObRespAnalyzer::handle_analyze_ob20_extra_info(const char *buf, const int64_t len, ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  const Ob20ProtocolFlags &flags = ob20_analyzer_.get_header().flag_;
  FLTObjManage &flt = resp_result.flt_;
  Ob20ExtraInfo &extra_info = resp_result.get_extra_info();
  // only process extra info in the last oceanbase 2.0 packet
  if (!extra_info.extra_info_buf_.is_inited() &&
      OB_FAIL(extra_info.extra_info_buf_.init(ob20_analyzer_.get_extra_info_len()))) {
    LOG_WDIAG("fail to init extra info buf", K(ret));
  } else if (OB_FAIL(extra_info.extra_info_buf_.write(buf, len))) { // continuous write buf of extra info to buf
    LOG_WDIAG("fail to write extra info buf", KP(buf), K(len));
  } else if (ob20_analyzer_.get_extra_info_to_read() == 0) {
    if (flags.is_new_extra_info()) {
      if (OB_FAIL(ObRespAnalyzerUtil::do_new_extra_info_decode(extra_info.extra_info_buf_.ptr(),
                                                               extra_info.extra_info_buf_.len(),
                                                               extra_info, flt))) {
        LOG_WDIAG("fail to do resp new extra info decode", K(ret));
      }
    } else {
      if (OB_FAIL(ObRespAnalyzerUtil::do_obobj_extra_info_decode(extra_info.extra_info_buf_.ptr(),
                                                                 extra_info.extra_info_buf_.len(),
                                                                 extra_info, flt))) {
        LOG_WDIAG("fail to do resp obobj extra info decode", K(ret));
      }
    }
  }
  LOG_DEBUG("succ/continue to analyze oceanbase 2.0 extra info", K(len));

  return ret;
}

int ObRespAnalyzer::handle_analyze_oceanbase_compressed_hdr(const char *buf, const int64_t analyzed)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("invalid argument", KP(buf), K(ret));
  } else if (stream_compressed_state_ == STREAM_COMPRESSED_MYSQL_PAYLOAD) {
    // in case of nego with compressed oceanbase 2.0 protocol but receiving oceanbase 2.0 protocol
    params_.is_compressed_ = (compressed_analyzer_.get_header().non_compressed_len_ != 0);
    if (!params_.is_compressed_) {
      const char *compressed_mysql_header_buf = NULL;
      if (analyzed == MYSQL_COMPRESSED_HEALDER_LENGTH) {
        compressed_mysql_header_buf = buf;
      } else {
        compressed_mysql_header_buf = compressed_analyzer_.get_header_buf();
      }
      const uint8_t *hdr_buf = reinterpret_cast<const uint8_t *>(compressed_mysql_header_buf);
      ob20_analyzer_.set_local_header_checksum(ob_crc16(0, hdr_buf, MYSQL_COMPRESSED_HEALDER_LENGTH));
      LOG_DEBUG("calc compressed mysql header crc16", K(ob20_analyzer_.get_local_header_checksum()), K(params_.is_compressed_));
    }

    stream_compressed_state_ = STREAM_COMPRESSED_OCEANBASE_PAYLOAD;
    LOG_DEBUG("succ to analyze oceanbase 2.0 compressed mysql header", K(analyzed), K(compressed_analyzer_));
  } else {
    LOG_DEBUG("continue to analyze data of oceanbase 2.0 compressed mysql header", K(analyzed), K(compressed_analyzer_));
  }

  return ret;
}

int ObRespAnalyzer::handle_analyze_ob20_header(ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  // oceanbase 2.0 header has been read completely
  if (stream_ob20_state_ == STREAM_OCEANBASE20_EXTRA_INFO_HEADER ||
      stream_ob20_state_ == STREAM_OCEANBASE20_EXTRA_INFO ||
      stream_ob20_state_ == STREAM_OCEANBASE20_MYSQL_PAYLOAD) {
    const ObOceanBase20PktHeader &header = ob20_analyzer_.get_header();
    if (is_decompress_mode()) {
      const ObMysqlCompressedPacketHeader &comp_hdr = compressed_analyzer_.get_header();
      PROTOCOL_DIAGNOSIS(OCEANBASE20, recv, protocol_diagnosis_,
                         comp_hdr.compressed_len_,
                         comp_hdr.seq_,
                         comp_hdr.non_compressed_len_,
                         header.payload_len_,
                         header.connection_id_,
                         header.flag_,
                         header.pkt_seq_,
                         header.request_id_);
    }
    // magic num
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(OB20_PROTOCOL_MAGIC_NUM != header.magic_num_)) {
      ret = OB_UNKNOWN_PACKET;
      LOG_EDIAG("invalid magic num", K(OB20_PROTOCOL_MAGIC_NUM),
                K(header.magic_num_), K_(sess_id), K(ret));
    } else if (OB_UNLIKELY(sess_id_ != header.connection_id_)) {
      ret = OB_UNKNOWN_CONNECTION;
      LOG_EDIAG("connection id mismatch", K_(sess_id), K_(header.connection_id), K(ret));
    } else if (OB_UNLIKELY(OB20_PROTOCOL_VERSION_VALUE != header.version_)) {
      ret = OB_UNKNOWN_PACKET;
      LOG_EDIAG("invalid version", K(OB20_PROTOCOL_VERSION_VALUE),
                K(header.version_), K_(sess_id), K(ret));
    } else if (OB_UNLIKELY(request_id_ != header.request_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("invalid request_id", K_(request_id),
                "current request id", header.request_id_, K(ret));
    } else if (OB_UNLIKELY(header.pkt_seq_ != static_cast<uint8_t>(last_ob_seq_ + 1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("invalid pkg seq", K_(last_ob_seq),
                "current pkg seq", header.pkt_seq_, K(ret));
    } else {
      last_ob_seq_++;
    }

    if (header.flag_.is_last_packet()) {
      resp_result.is_server_trans_internal_routing_ = header.flag_.is_trans_internal_routing();
    }
    LOG_DEBUG("analyze oceanbase 2.0 header",
              "magic_num", header.magic_num_,
              "conn_id", header.connection_id_,
              "request_id", header.request_id_,
              "flag", header.flag_.flags_,
              "pkt_seq", header.pkt_seq_,
              "hdr_checksum", header.header_checksum_);
  }
  return ret;
}

int ObRespAnalyzer::analyze_one_packet_header(
  event::ObIOBufferReader &reader,
  ObAnalyzeHeaderResult &result)
{
  int ret = OB_SUCCESS;
  // oceanbase 2.0 pkt
  if (OB_LIKELY(ObProxyProtocol::PROTOCOL_OB20 == protocol_)) {
    if (params_.is_compressed_) {
      ret = ObRespAnalyzerUtil::analyze_one_compressed_ob20_packet(reader, result);
    } else {
      ret = ObProto20Utils::analyze_one_ob20_packet_header(reader, result, false);
    }
  // compressed mysql pkt
  } else if (ObProxyProtocol::PROTOCOL_CHECKSUM == protocol_) {
    ret = ObMysqlAnalyzerUtils::analyze_one_compressed_packet(reader, result);
  // mysql pkt
  } else if (ObProxyProtocol::PROTOCOL_NORMAL == protocol_) {
    ret = ObRespAnalyzerUtil::analyze_one_mysql_packet(reader, result, req_cmd_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected protocol", K(protocol_), K(ret));
  }
  return ret;
}

int ObRespAnalyzer::analyze_one_packet_header(
    event::ObIOBufferReader &reader,
    ObAnalyzeHeaderResult &result,
    ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  result.reset();

  if (OB_FAIL(analyze_one_packet_header(reader, result))) {
    LOG_WDIAG("fail to analyze one compressed packet", K(ret));
  } else if (ANALYZE_DONE == result.status_) { // one compressed packet received completely
    // compressed mysql or oceanabse 2.0
    if (ObProxyProtocol::PROTOCOL_NORMAL != protocol_) {
      if (is_last_pkt(result)) {    // only has one compressed packet
        resp_result.is_resultset_resp_ = false;
        analyze_mode_ = DECOMPRESS_MODE;
        if (OB_FAIL(alloc_mysql_pkt_buf())) {
          LOG_WDIAG("fail to alloc_mysql_pkt_buf", K(ret));
        }
      } else {
        resp_result.is_resultset_resp_ = true;
      }
    } else {
      ObServerStatusFlags server_status(0);
      if (OB_LIKELY(is_oceanbase_mode())) {
        if (OB_UNLIKELY(OB_MYSQL_COM_STATISTICS == req_cmd_)) {
          if (params_.is_extra_ok_for_stats_) {
            if (OB_FAIL(ObRespAnalyzerUtil::receive_next_ok_packet(reader, result))) {
              LOG_WDIAG("fail to receive next ok packet", K(ret));
            }
          }
        } else {
          if (MYSQL_OK_PACKET_TYPE == result.mysql_header_.pkt_type_
              && OB_MYSQL_COM_STMT_PREPARE != req_cmd_
              && OB_MYSQL_COM_STMT_PREPARE_EXECUTE != req_cmd_) {
            ObMysqlPacketReader pkt_reader;
            if (OB_FAIL(pkt_reader.get_ok_packet_server_status(reader, server_status))) {
              LOG_WDIAG("fail to get server status flags", K(server_status.flags_), K(ret));
            } else {
              // do nothing
            }
          } else if (OB_UNLIKELY(MYSQL_ERR_PACKET_TYPE == result.mysql_header_.pkt_type_)) {
            // 2. if the fist packet is an err pkt, it must be followed by an ok packet.
            //    in this case, we should also confirm the second ok packet is received competed.
            if (OB_FAIL(ObRespAnalyzerUtil::receive_next_ok_packet(reader, result))) {
              LOG_WDIAG("fail to receive next ok packet", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret) && ANALYZE_DONE == result.status_) {
        // if it is result + eof + error + ok, it may be not....
        // treat multi stmt as result set protocol
        bool is_resultset_resp = ObRespAnalyzerUtil::is_resultset_resp(req_cmd_, result.mysql_header_.pkt_type_, server_status);
        resp_result.is_resultset_resp_ = is_resultset_resp;
        LOG_DEBUG("after analyze one response packet",
                  "packet_type", result.mysql_header_.pkt_type_,
                  "cmd", req_cmd_,
                  "status_flag", server_status.flags_,
                  K(is_resultset_resp));
      }
    }
  }
  return ret;
}

int ObRespAnalyzer::analyze_all_packets(
    event::ObIOBufferReader &reader,
    ObAnalyzeHeaderResult &result,
    ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  // compressed mysql or oceanbase 2.0 pkts
  if (ObProxyProtocol::PROTOCOL_NORMAL != protocol_) {
    ObIOBufferReader *mysql_pkt_reader = NULL;
    if (is_decompress_mode() && OB_ISNULL(mysql_pkt_reader = alloc_mysql_pkt_reader())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to alloc_mysql_pkt_reader", K(mysql_pkt_reader), K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(analyze_response(reader, resp_result))) { // analyze it
        LOG_WDIAG("fail to analyze_response", K(ret));
      } else {
        if (is_stream_end()) {
          result.status_ = ANALYZE_DONE;
          if (is_decompress_mode()) {
            int64_t written_len = 0;
            int64_t tmp_read_avail = mysql_pkt_reader->read_avail();
            // reader.mio_buf -- decompress --> mysql_pkt_reader.mio_buf ----> reader.mio_buf
            //  we have decompress all data from reader to mysql_pkt_reader.mio_buf
            //  then we write all data in mysql_pkt_reader.mio_buf back to reader.mio_buf
            if (OB_FAIL(reader.consume_all())) {
              LOG_WDIAG("fail to consume all", K(ret));
            } else if (OB_FAIL(reader.mbuf_->write(mysql_pkt_reader, tmp_read_avail, written_len))) {
              LOG_WDIAG("fail to write", K(tmp_read_avail), K(ret));
            } else if (OB_UNLIKELY(written_len != tmp_read_avail)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("written_len is not expected", K(written_len), K(tmp_read_avail), K(ret));
            } else {
              resp_result.is_decompressed_ = true;
              LOG_DEBUG("all mysql pkts decompressed", K(result));
            }
          }
        } else {
          result.status_ = ANALYZE_CONT;
          LOG_DEBUG("not receive completed", K(result));
        }
      }
    }
    // in case of memory leak
    if (NULL != mysql_pkt_reader) {
      mysql_pkt_reader->dealloc();
      mysql_pkt_reader = NULL;
    }

    if (OB_SUCC(ret)) {
      if (ANALYZE_DONE == result.status_) {
        if (is_stream_end()) {
          if (resp_result.is_eof_resp()
              || ((OB_MYSQL_COM_STMT_PREPARE == req_cmd_ || OB_MYSQL_COM_STMT_PREPARE_EXECUTE == req_cmd_)
                   && !resp_result.is_error_resp())) {
            resp_result.is_resultset_resp_ = true;
          }
        } else {
          if(OB_FAIL(ObProto20Utils::analyze_first_mysql_packet(reader, result))) {
            LOG_WDIAG("fail to analyze packet", KP(&reader), K(ret));
          } else {
            // if it is result + eof + error + ok, it may be not....
            // treat multi stmt as result set protocol
            uint8_t pkt_type = result.mysql_header_.pkt_type_;
            resp_result.is_resultset_resp_ =
              ((OB_MYSQL_COM_QUERY == req_cmd_
                || OB_MYSQL_COM_STMT_EXECUTE == req_cmd_
                || OB_MYSQL_COM_STMT_FETCH == req_cmd_)
               && OB_MYSQL_COM_STATISTICS != req_cmd_
               && MYSQL_OK_PACKET_TYPE != pkt_type
               && MYSQL_ERR_PACKET_TYPE != pkt_type
               && MYSQL_EOF_PACKET_TYPE != pkt_type
               && MYSQL_LOCAL_INFILE_TYPE != pkt_type)
              || OB_MYSQL_COM_STMT_PREPARE == req_cmd_
              || OB_MYSQL_COM_STMT_PREPARE_EXECUTE == req_cmd_
              || OB_MYSQL_COM_FIELD_LIST == req_cmd_;
          }
          LOG_DEBUG("analyze OB20 first response finished", K(result),
                    "is_resultset_resp", resp_result.is_resultset_resp_);
        }
      }
    }
  // mysql pkts
  } else {
    if (OB_FAIL(analyze_response(reader, resp_result))) {
        LOG_WDIAG("fail to analyze response", K(ret));
        result.status_ = ANALYZE_ERROR;
    } else {
      if (is_stream_end()) {
        result.status_ = ANALYZE_DONE;
        if (resp_result.is_eof_resp() ||
            ((OB_MYSQL_COM_STMT_PREPARE == req_cmd_ || OB_MYSQL_COM_STMT_PREPARE_EXECUTE == req_cmd_) &&
            !resp_result.is_error_resp())) {
          resp_result.is_resultset_resp_ = true;
        }
      } else {
        result.status_ = ANALYZE_CONT;
      }
    }
  }
  return ret;
}

int ObRespAnalyzer::stream_analyze_mysql(const ObString data, ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  int64_t remain = data.length();
  char *start = const_cast<char*>(data.ptr());
  int64_t analyzed = 0;

  while(OB_SUCC(ret) && remain > 0) {
    switch (stream_mysql_state_) {
      case STREAM_MYSQL_HEADER: {
        if (OB_FAIL(mysql_analyzer_.stream_analyze_header(start, remain, analyzed, stream_mysql_state_))) {
          LOG_WDIAG("fail to analyze_header (mysql)", K(start), K(remain), K(analyzed),
                    "stream_mysql_state_", ObRespAnalyzerUtil::get_stream_state_str(stream_mysql_state_), K(ret));
        } else if (OB_FALSE_IT(handle_analyze_mysql_header(analyzed))) {
        } else if (OB_FAIL(handle_analyze_mysql_end(start + analyzed, &resp_result))) { // handle empty pkt (len == 0)
          LOG_WDIAG("fail to handle_analyze_mysql_end", K(ret));
        }
        break;
      }

      case STREAM_MYSQL_TYPE: {
        if (need_analyze_mysql_pkt_type()) {
          if (OB_FAIL(mysql_analyzer_.stream_analyze_type(start, remain, analyzed, stream_mysql_state_))) {
            LOG_WDIAG("fail to analyze_type (mysql)", K(start), K(remain), K(analyzed),
                      "stream_mysql_state_", ObRespAnalyzerUtil::get_stream_state_str(stream_mysql_state_), K(ret));
          } else if (OB_FAIL(handle_analyze_mysql_pkt_type(start))) {
            LOG_WDIAG("fail to handle_analyze_mysql_pkt_type", K(start), K(remain), K(analyzed), K(ret));
          } else if (OB_FAIL(handle_analyze_mysql_end(start + analyzed, &resp_result))) {  // handle pkt (len == 1)
            LOG_WDIAG("fail to handle_analyze_mysql_end", K(resp_result), K(mysql_analyzer_), K(ret));
          }
        } else {
          analyzed = 0; // nothing to analyzed
          if (OB_FAIL(handle_not_analyze_mysql_pkt_type())) {
            LOG_WDIAG("fail to handle_not_analyze_mysql_pkt_type", K(ret));
          } else {
            LOG_DEBUG("succ to skip analyze mysql pkt type", K(analyzed), K(mysql_analyzer_));
          }
        }
        break;
      }

      case STREAM_MYSQL_BODY: {
        if (OB_FAIL(mysql_analyzer_.stream_analyze_body(start, remain, analyzed, stream_mysql_state_))) {
          LOG_WDIAG("fail to analyze_body", K(ret));
        } else if (OB_FAIL(handle_analyze_mysql_body(start, analyzed))) {
          LOG_WDIAG("fail to handle_analyze_mysql_body", K(ret));
        } else if (OB_FAIL(handle_analyze_mysql_end(start + analyzed, &resp_result))) {
          LOG_WDIAG("fail to handle_analyze_mysql_end", K(resp_result), K(mysql_analyzer_), K(ret));
        }
        break;
      }

      default: {
        ret = OB_INNER_STAT_ERROR;
        LOG_WDIAG("unexpected state", K(stream_mysql_state_), K(mysql_analyzer_));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      start += analyzed;
      remain = (remain <= analyzed ? 0 : remain - analyzed);
      LOG_DEBUG("in loop",
                "next_state", ObRespAnalyzerUtil::get_stream_state_str(stream_mysql_state_),
                "header_to_read", mysql_analyzer_.get_header_to_read(),
                "body_to_read", mysql_analyzer_.get_body_to_read(),
                "pkt_len", mysql_analyzer_.get_pkt_len(),
                "pkt_seq", mysql_analyzer_.get_pkt_seq(),
                K(analyzed), K(remain));
    }
  }

  return ret;
}
int ObRespAnalyzer::stream_analyze_compressed_mysql(const ObString data, ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  int64_t remain = data.length();
  char *start = const_cast<char*>(data.ptr());
  int64_t analyzed = 0;

  while (OB_SUCC(ret) && remain > 0) {
    switch (stream_compressed_state_) {
      case STREAM_COMPRESSED_MYSQL_HEADER: {
        if (OB_FAIL(compressed_analyzer_.stream_analyze_header(start, remain, analyzed, stream_compressed_state_))) {
          LOG_WDIAG("fail to analyze_header (compressed mysql)", K(ret));
        } else if (is_decompress_mode()) {
          handle_analyze_compressed_mysql_header();
        }
        break;
      }

      case STREAM_COMPRESSED_MYSQL_PAYLOAD: {
        if (OB_FAIL(compressed_analyzer_.stream_analyze_payload(start, remain, analyzed, stream_compressed_state_))) {
          LOG_WDIAG("fail to analyze_payload (compressed mysql)", K(ret));
        } else if (OB_FAIL(handle_analyze_compressed_mysql_payload(start, analyzed, resp_result))) {
          LOG_WDIAG("fail to handle_analyze_compressed_mysql_payload", K(ret));
        }
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected stream state", K(stream_compressed_state_));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      start += analyzed;
      remain = (remain <= analyzed ? 0 : remain - analyzed);
      LOG_DEBUG("contine to analyze compressed mysql pkt",
                "next_compressed_state", ObRespAnalyzerUtil::get_stream_state_str(stream_compressed_state_),
                 K(analyzed), K(remain));
    }
  }

  return ret;
}
int ObRespAnalyzer::stream_analyze_compressed_oceanbase(const ObString data, ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  int64_t remain = data.length();
  char *start = const_cast<char*>(data.ptr());
  int64_t analyzed = 0;

  while (OB_SUCC(ret) && remain > 0) {
    switch (stream_compressed_state_) {
      case STREAM_COMPRESSED_MYSQL_HEADER: {
        if (OB_FAIL(compressed_analyzer_.stream_analyze_header(start, remain, analyzed, stream_compressed_state_))) {
          LOG_WDIAG("fail to analyze_header (compressed oceanbase)", K(ret));
        } else if (OB_FAIL(handle_analyze_oceanbase_compressed_hdr(start, analyzed))) {
          LOG_WDIAG("fail to handle_analyze_oceanbase_compressed_hdr", K(ret));
        }
        break;
      }

      case STREAM_COMPRESSED_OCEANBASE_PAYLOAD: {
        if (OB_FAIL(compressed_analyzer_.stream_analyze_payload(start, remain, analyzed, stream_compressed_state_))) {
          LOG_WDIAG("fail to analyze_payload (compressed oceanbase)", K(ret));
        } else if (OB_FAIL(handle_analyze_compressed_oceanbase_payload(start, analyzed, resp_result))) {
          LOG_WDIAG("fail to handle_analyze_compressed_oceanbase_payload", K(ret));
        }
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected stream state", K(stream_compressed_state_));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      start += analyzed;
      remain = (remain <= analyzed ? 0 : remain - analyzed);
      LOG_DEBUG("in loop",
                "next_compressed_state", ObRespAnalyzerUtil::get_stream_state_str(stream_compressed_state_),
                "header_to_read", compressed_analyzer_.get_header_to_read(),
                "payload_to_read", compressed_analyzer_.get_payload_to_read(),
                 K(analyzed), K(remain));
    }
  }
  return ret;
}

int ObRespAnalyzer::stream_analyze_oceanbase(const ObString data, ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  int64_t remain = data.length();
  char *start = const_cast<char*>(data.ptr());
  int64_t analyzed = 0;

  while (OB_SUCC(ret) && remain > 0) {
    switch (stream_ob20_state_) {
      case STREAM_OCEANBASE20_HEADER: {
        if (OB_FAIL(ob20_analyzer_.stream_analyze_header(start, remain, analyzed, stream_ob20_state_))) {
          LOG_WDIAG("fail to analyze_header (oceanbase 2.0)", K(ret));
        } else if (OB_FAIL(handle_analyze_ob20_header(resp_result))) {
          LOG_WDIAG("fail to validate_oceanbase20_header", K(ob20_analyzer_), K(ret));
        }
        break;
      }

      case STREAM_OCEANBASE20_EXTRA_INFO_HEADER: {
        if (OB_FAIL(ob20_analyzer_.stream_analyze_extra_info_header(start, remain, analyzed, stream_ob20_state_))) {
          LOG_WDIAG("fail to analyze_extra_info_header", K(ret));
        } else if (is_decompress_mode()) {
          handle_analyze_ob20_extra_info_header(resp_result);
        } else {
          LOG_DEBUG("simple mode, skip handle extra info header");
        }

        break;
      }

      case STREAM_OCEANBASE20_EXTRA_INFO: {
        if (OB_FAIL(ob20_analyzer_.stream_analyze_extra_info(start, remain, analyzed, stream_ob20_state_))) {
          LOG_WDIAG("fail to analyze_extra_info(oceanbase 2.0)", K(ret));
        } else if (is_decompress_mode()) {
          if (OB_FAIL(OB_FAIL(handle_analyze_ob20_extra_info(start, analyzed, resp_result)))) {
            LOG_WDIAG("fail to handle_analyze_ob20_extra_info", K(ret));
          }
        } else {
          LOG_DEBUG("simple mode, skip handle extra info");
        }
        break;
      }

      case STREAM_OCEANBASE20_MYSQL_PAYLOAD: {
        if (OB_FAIL(ob20_analyzer_.stream_analyze_mysql_payload(start, remain, analyzed, stream_ob20_state_))) {
          LOG_WDIAG("fail to analyze_mysql_payload(oceanbase 2.0)", K(ret));
        } else if (is_decompress_mode()) {
          if (OB_FAIL(handle_analyze_mysql_payload(start, analyzed, resp_result))) {
            LOG_WDIAG("fail to handle_analyze_mysql_payload", K(ret));
          }
        } else {
          LOG_DEBUG("simple mode, skip handle mysql payload");
        }
        break;
      }

      case STREAM_OCEANBASE20_TAILER: {
        if (OB_FAIL(ob20_analyzer_.stream_analyze_tailer(start, remain, analyzed, stream_ob20_state_))) {
          LOG_WDIAG("fail to analyze_tailer(oceanbase 2.0)", K(ret));
        } else {
          handle_analyze_ob20_tailer(resp_result);
        }
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected stream state", K(stream_ob20_state_));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      start += analyzed;
      remain = (remain <= analyzed ? 0 : remain - analyzed);
      LOG_DEBUG("in loop",
                "next_ob20_stream_state", ObRespAnalyzerUtil::get_stream_state_str(stream_ob20_state_),
                "header_to_read", ob20_analyzer_.get_header_to_read(),
                "extra_info_to_read", ob20_analyzer_.get_extra_info_to_read(),
                "mysql_payload_to_read", ob20_analyzer_.get_mysql_payload_to_read(),
                "tailer_to_read", ob20_analyzer_.get_tailer_to_read(),
                 K(analyzed), K(remain));
    }
  }

  return ret;
}

int ObRespAnalyzer::analyze_one_packet(event::ObIOBufferReader &reader, ObAnalyzeHeaderResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(is_inited_), K(ret));
  } else if (OB_FAIL(analyze_one_packet_header(reader, result))) {
    LOG_WDIAG("fail to analyze_one_packet_header", K(ret));
  } else if (ANALYZE_DONE == result.status_) { // one compressed packet received completely
    if (OB_FAIL(analyze_response_with_length(reader, result.compressed_mysql_header_.compressed_len_ + MYSQL_COMPRESSED_HEALDER_LENGTH))) { // analyze it
      LOG_WDIAG("fail to analyze response with length", K(ret));
    }
  }

  return ret;
}

/*
  ATTENTION!!!
  1. reader should contain all compressed packets
  2. reader.mio_buf_ shouldn't be modified
*/
int ObRespAnalyzer::analyze_response(event::ObIOBufferReader &reader, ObRespAnalyzeResult &resp_result)
{
  // need to judge is_inited_
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(is_inited_), K(ret));
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
      LOG_WDIAG("the first block data_size in reader is less than 0",
              K(offset), K(block->read_avail()), K(ret));
    }

    ObString resp_buf;
    while (OB_SUCC(ret) && NULL != block && data_size > 0 && !is_stream_end()) {
      resp_buf.assign_ptr(data, static_cast<int32_t>(data_size));
      if (OB_FAIL(stream_analyze_packets(resp_buf, resp_result))) {
        LOG_WDIAG("fail to stream_analyze_packets", K(resp_buf), K(ret));
      } else {
        // on to the next block
        offset = 0;
        block = block->next_;
        if (NULL != block) {
          data = block->start();
          data_size = block->read_avail();
        }
      }
    }
    LOG_DEBUG("analyze response finished", "data_size", reader.read_avail(), K(is_mysql_stream_end_), K(analyze_mode_), K(resp_result));
  }


  return ret;
}

int ObRespAnalyzer::analyze_response_with_length(event::ObIOBufferReader &reader, uint64_t length)
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
    LOG_WDIAG("the first block data_size in reader is less than 0",
             K(offset), K(block->read_avail()), K(ret));
  } else if (data_size > length) {
    data_size = length;
  }

  ObString resp_buf;
  ObRespAnalyzeResult resp_result;
  while (OB_SUCC(ret) && NULL != block && length > 0) {
    resp_buf.assign_ptr(data, static_cast<int32_t>(data_size));
    if (OB_FAIL(stream_analyze_packets(resp_buf, resp_result))) {
      LOG_WDIAG("fail to stream_analyze_packets", K(ret));
    } else {
      length -= data_size;
      if (length > 0) {
        // on to the next block
        offset = 0;
        block = block->next_;
        if (NULL != block) {
          data = block->start();
          data_size = block->read_avail();
          if (data_size > length) {
            data_size = length;
          }
        }
      }
    }
  }
  LOG_DEBUG("analyze response with length finished", K(length), K(resp_result));

  return ret;
}

int ObRespAnalyzer::analyze_response(
    event::ObIOBufferReader &reader, const bool need_receive_completed,
    ObAnalyzeHeaderResult &result, ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(is_inited_), K(ret));
  } else if (OB_FAIL(analyze_one_packet_header(reader, result, resp_result))) {
    LOG_WDIAG("fail to analyze_one_packet_header", K_(analyze_mode), K(result), K(ret));
  } else if (need_analyze_all_packets(need_receive_completed, resp_result)) {
    if (OB_FAIL(analyze_all_packets(reader, result, resp_result))) {
      LOG_WDIAG("fail to analyze_all_packets", K(result), K(resp_result), K(ret));
    } else {
      LOG_DEBUG("succ to analyze all pkts", K(resp_result), K(result));
    }
  }
  return ret;
}

int ObRespAnalyzer::analyze_ok_pkt(const char *pkt_end, bool &is_in_trans)
{
  int ret = OB_SUCCESS;
  // only need to get the OB_SERVER_STATUS_IN_TRANS bit
  int64_t len = reserve_pkt_body_buf_.len();
  const char *ptr = reserve_pkt_body_buf_.ptr();

  const char *pos = ptr;
  uint64_t affected_rows = 0;
  uint64_t last_insert_id = 0;
  ObServerStatusFlags server_status;

  if (OB_FAIL(ObMySQLUtil::get_length(pos, affected_rows))) {
    LOG_WDIAG("get length failed", K(ptr), K(pos), K(affected_rows), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::get_length(pos, last_insert_id))) {
    LOG_WDIAG("get length failed", K(ptr), K(pos), K(last_insert_id), K(ret));
  } else if (FALSE_IT(ObMySQLUtil::get_uint2(pos, server_status.flags_))) {
    // impossible
  } else if (OB_UNLIKELY((pos - ptr) > OK_PKT_MAX_LEN)
             || OB_UNLIKELY((pos - ptr) > len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected ptr pos", K(ptr), K(pos), K(len), K(ret));
  } else {
    if (server_status.status_flags_.OB_SERVER_STATUS_IN_TRANS) {
      is_in_trans = true;
    } else {
      is_in_trans = false;
    }

    if (server_status.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS) {
      cur_stmt_has_more_result_ = true;
    } else {
      cur_stmt_has_more_result_ = false;
    }
    if (params_.is_binlog_req_) {
      if (OB_ISNULL(pkt_end)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_EDIAG("invalid argument", KP(pkt_end), K(ret));
      } else {
        server_status.status_flags_.OB_SERVER_STATUS_IN_TRANS = params_.is_in_trans_;
        server_status.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = params_.is_autocommit_;
        pos = pkt_end - len + (pos - ptr) - 2;
        (*((unsigned short *) (pos))) = server_status.flags_;
      }
    }
  }

  return ret;
}

int ObRespAnalyzer::analyze_prepare_ok_pkt()
{
  int ret = OB_SUCCESS;

  //body_buf not include first type byte[00]
  const char *ptr = reserve_pkt_body_buf_.ptr();

  OMPKPrepare prepare_packet;
  prepare_packet.set_content(ptr, static_cast<uint32_t>(reserve_pkt_body_buf_.len()));
  if (OB_FAIL(prepare_packet.decode())) {
    LOG_WDIAG("decode packet failed", K(ret));
  } else {
    int32_t expect_pkt_cnt = 1;
    if (prepare_packet.get_param_num() > 0) {
      expect_pkt_cnt += (prepare_packet.get_param_num() + 1);
    }

    if (prepare_packet.get_column_num()> 0) {
      expect_pkt_cnt += (prepare_packet.get_column_num() + 1);
    }

    mysql_resp_result_.set_expect_pkt_cnt(expect_pkt_cnt);
    /* 如果没结果集, 就设置为已接收 */
    mysql_resp_result_.set_recv_resultset(0 == prepare_packet.has_result_set());
  }

  return ret;
}

int ObRespAnalyzer::analyze_eof_pkt(const char *pkt_end, bool &is_in_trans, bool &is_last_eof_pkt)
{
  int ret = OB_SUCCESS;
  int64_t len = reserve_pkt_body_buf_.len();
  ObServerStatusFlags server_status;

  if (len < 4) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("fail to build packet content", K(ret));
  } else {
    // skip 2 bytes of warning_count, we don't care it
    server_status.flags_ = uint2korr(reserve_pkt_body_buf_.ptr() + 2);

    if (server_status.status_flags_.OB_SERVER_STATUS_IN_TRANS) {
      is_in_trans = true;
    } else {
      is_in_trans = false;
    }

    if (server_status.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS) {
      cur_stmt_has_more_result_ = true;
    } else {
      cur_stmt_has_more_result_ = false;
    }

    if (OB_MYSQL_COM_STMT_EXECUTE == req_cmd_ && server_status.status_flags_.OB_SERVER_STATUS_CURSOR_EXISTS) {
      is_last_eof_pkt = true;
    }

    if (params_.is_binlog_req_) {
      if (OB_ISNULL(pkt_end)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_EDIAG("invalid argument", KP(pkt_end), K(ret));
      } else {
        server_status.status_flags_.OB_SERVER_STATUS_IN_TRANS = params_.is_in_trans_;
        server_status.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = params_.is_autocommit_;
        char *ptr = const_cast<char*>(pkt_end) - len + 2;
        (*((unsigned short *) (ptr))) = server_status.flags_;
      }
    }
  }
  return ret;
}

int ObRespAnalyzer::analyze_error_pkt(ObRespAnalyzeResult *resp_result)
{
  int ret = OB_SUCCESS;
  if (NULL != resp_result) {
    obutils::ObVariableLenBuffer<FIXED_MEMORY_BUFFER_SIZE> &content_buf
      = resp_result->error_pkt_buf_;
    if (OB_FAIL(build_packet_content(content_buf))) {
      LOG_WDIAG("fail to build packet content", K(ret));
    } else {
      int64_t len = content_buf.len();
      const char *ptr = content_buf.ptr();

      OMPKError &err_pkt = resp_result->get_error_pkt();
      err_pkt.set_content(ptr, static_cast<uint32_t>(len));
      if (OB_FAIL(err_pkt.decode())) {
        LOG_WDIAG("fail to decode error packet", K(ret));
      }
    }
  }

  return ret;
}

int ObRespAnalyzer::build_packet_content(obutils::ObVariableLenBuffer<RESP_PKT_MEMORY_BUFFER_SIZE> &content_buf)
{
  int ret = OB_SUCCESS;
  content_buf.reset();

  int64_t body_len = reserve_pkt_body_buf_.len();
  const char *body_ptr = reserve_pkt_body_buf_.ptr();
  int64_t type_len = 1;

  if (OB_UNLIKELY(MYSQL_NET_TYPE_LENGTH != type_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("type_len is not MYSQL_NET_TYPE_LENGTH ", K(ret));
  } else {
    int64_t mem_len = body_len + type_len;
    if (OB_FAIL(content_buf.init(mem_len))) {
      LOG_WDIAG("fail int alloc mem", K(mem_len), K(ret));
    } else {
      char *pos = content_buf.pos();
      *pos = static_cast<char>(mysql_analyzer_.get_pkt_type());
      if (OB_FAIL(content_buf.consume(type_len))) {
        LOG_WDIAG("failed to consume", K(type_len), K(ret));
      }

      if (OB_SUCC(ret)) {
        pos = content_buf.pos();
        MEMCPY(pos, body_ptr, body_len);
        if (OB_FAIL(content_buf.consume(body_len))) {
          LOG_WDIAG("failed to consume", K(body_len), K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t content_len = content_buf.len();
        LOG_DEBUG("build packet content", K(content_len), K(type_len), K(body_len));
        if (OB_UNLIKELY(content_len != type_len + body_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("content_len is not valid ", K(type_len), K(body_len), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRespAnalyzer::analyze_hanshake_pkt(ObRespAnalyzeResult *resp_result)
{
  int ret = OB_SUCCESS;
  if (NULL != resp_result) {
    // only need to get the OB_SERVER_STATUS_IN_TRANS bit
    const char *ptr = reserve_pkt_body_buf_.ptr();

    // skip ptotocol_version and server version
    // handshake packet content:
    // int<1> protocol_version --> string<EOF> server_version --> int<4> connection_id --> others
    OMPKHandshake packet;
    packet.set_content(ptr, static_cast<uint32_t>(reserve_pkt_body_buf_.len()));
    if (OB_FAIL(packet.decode())) {
      LOG_WDIAG("decode packet failed", K(ret));
    } else {
      resp_result->server_capabilities_lower_.capability_ = packet.get_server_capability_lower();
      resp_result->server_capabilities_upper_.capability_ = packet.get_server_capability_upper();
      resp_result->connection_id_ = packet.get_thread_id();
      int64_t copy_len = 0;
      if (OB_FAIL(packet.get_scramble(resp_result->scramble_buf_,
                                      static_cast<int64_t>(sizeof(resp_result->scramble_buf_)),
                                      copy_len))) {
        LOG_WDIAG("fail to get scramble", K(ret));
      } else if (OB_UNLIKELY(copy_len >= static_cast<int64_t>(sizeof(resp_result->scramble_buf_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("copy_len is too bigger", K(copy_len), K(ret));
      } else {
        resp_result->scramble_buf_[copy_len] = '\0';
      }
    }
    LOG_DEBUG("succ to get connection id and scramble ",
              "connection_id", resp_result->connection_id_,
              "scramble_buf", resp_result->scramble_buf_);
  }

  return ret;
}

int ObRespAnalyzer::alloc_mysql_pkt_buf()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mysql_pkt_buf_)
      && OB_ISNULL(mysql_pkt_buf_ = new_miobuffer(DEFAULT_PKT_BUFFER_SIZE))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to do new_miobuffer", K(mysql_pkt_buf_), K(ret));
  }

  return ret;
}

int ObRespAnalyzer::update_ending_type()
{
  int ret = OB_SUCCESS;
  int64_t eof_pkt_cnt = mysql_resp_result_.get_pkt_cnt(EOF_PACKET_ENDING_TYPE);
  int64_t err_pkt_cnt = mysql_resp_result_.get_pkt_cnt(ERROR_PACKET_ENDING_TYPE);
  int64_t prepare_ok_pkt_cnt = mysql_resp_result_.get_pkt_cnt(PREPARE_OK_PACKET_ENDING_TYPE);
  ending_type_ = MAX_PACKET_ENDING_TYPE;
  switch (mysql_analyzer_.get_pkt_type()) {
    case MYSQL_ERR_PACKET_TYPE:
      ending_type_ = ERROR_PACKET_ENDING_TYPE;
      break;

    case MYSQL_OK_PACKET_TYPE:
      // a mysql packet can not treat as OK packet just only by (0x00 == pkt_type)
      // in ReslutSet protocol.
      // mysql> select *from t4;
      //    +------+
      //    | a    |
      //    +------+
      //    |      |
      //    | 1    |
      //    +------+
      // the first line is empty
      // the second line is 1
      // if we just judge 0x00 == pkt_type, we will make a mistake, the first line
      // paket will treat as ok packet.
      // so in ResultSet Protocol, a packet can be detemined as ok packet by
      // both 0x00 === pkt_type and 1 != has_already_recived_eof_pkt_cnt
      if (OB_MYSQL_COM_BINLOG_DUMP == req_cmd_
          || OB_MYSQL_COM_BINLOG_DUMP_GTID == req_cmd_) {
        break;
      } else if (OB_MYSQL_COM_STMT_PREPARE == req_cmd_) {
        /* Preapre 请求, in OCEANBASE, 可能是 error + ok, 这时的 OK 应该是 OK_PACKET_ENDING_TYPE */
        if (0 == prepare_ok_pkt_cnt && 0 == err_pkt_cnt) {
          ending_type_ = PREPARE_OK_PACKET_ENDING_TYPE;
        } else {
          ending_type_ = OK_PACKET_ENDING_TYPE;
        }
      } else if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE == req_cmd_) {
        if (0 == prepare_ok_pkt_cnt && 0 == err_pkt_cnt) {
          ending_type_ = PREPARE_OK_PACKET_ENDING_TYPE;
        } else if (1 == err_pkt_cnt) {
          ending_type_ = OK_PACKET_ENDING_TYPE;
        } else if (mysql_resp_result_.is_recv_resultset()) {
          ending_type_ = OK_PACKET_ENDING_TYPE;
        }
      } else if (OB_MYSQL_COM_STMT_EXECUTE == req_cmd_) {
        if (1 == err_pkt_cnt) {
          ending_type_ = OK_PACKET_ENDING_TYPE;
        } else if (1 != eof_pkt_cnt) {
          ending_type_ = OK_PACKET_ENDING_TYPE;
        } else if (mysql_resp_result_.is_recv_resultset()) {
          ending_type_ = OK_PACKET_ENDING_TYPE;
        }
      } else if (OB_MYSQL_COM_STMT_FETCH == req_cmd_) {
        if (1 == err_pkt_cnt) {
          ending_type_ = OK_PACKET_ENDING_TYPE;
          // After two EOF packages in Oracle mode is the OK package
        } else if (OCEANBASE_ORACLE_PROTOCOL_MODE == protocol_mode_) {
          if (1 != eof_pkt_cnt) {
            ending_type_ = OK_PACKET_ENDING_TYPE;
          }
          // OB MySQL mode and standard MySQL mode are OK packets after an EOF
        } else if (1 == eof_pkt_cnt) {
          ending_type_ = OK_PACKET_ENDING_TYPE;
        }
      } else if (1 != eof_pkt_cnt) {
        ending_type_ = OK_PACKET_ENDING_TYPE;
        // in OCEANBASE_MYSQL_MODE, if we got an erro in ResultSet Protocol(maybe timeout), the packet
        // received from observer is EOF(first),  ERROR(second) and OK(last),
        // so we should recognize the last ok packet.
      } else if ((1 == eof_pkt_cnt) && (1 == err_pkt_cnt)) {
        ending_type_ = OK_PACKET_ENDING_TYPE;
      } else if (OB_MYSQL_COM_FIELD_LIST == req_cmd_) {
        ending_type_ = OK_PACKET_ENDING_TYPE;
      } else {
        // ending_type_ = MAX_PACKET_ENDING_TYPE;
      }
      break;

    case MYSQL_EOF_PACKET_TYPE:
      if (mysql_analyzer_.get_pkt_len() < MYSQL_MAX_EOF_PACKET_LEN) {
        ending_type_ = EOF_PACKET_ENDING_TYPE;
      } else if (OB_MYSQL_COM_CHANGE_USER == req_cmd_) {
        ending_type_ = EOF_PACKET_ENDING_TYPE;
      }
      break;

    case MYSQL_HANDSHAKE_PACKET_TYPE:
      if ((0 == eof_pkt_cnt) && (0 == err_pkt_cnt)) {
        ending_type_ = HANDSHAKE_PACKET_ENDING_TYPE;
      }
      break;

    case MYSQL_LOCAL_INFILE_TYPE:
      ending_type_ = LOCAL_INFILE_ENDING_TYPE;
      break;

    default:
      ending_type_ = MAX_PACKET_ENDING_TYPE;
      break;
  }
  return ret;
}

void ObRespAnalyzer::handle_last_eof(const char *pkt_end, uint32_t pkt_len)
{
  if (OB_LIKELY(is_oceanbase_mode())) {
    if (cur_stmt_has_more_result_) {
      // in multi stmt, send directly
      reserved_len_ = 0;
    } else {
      // last eof packet, must following by an extra ok packet
      reserved_len_ = pkt_len + MYSQL_NET_HEADER_LENGTH;
    }
  } else {
    // in mysql mode send directly
    reserved_len_ = 0;
    if (params_.is_binlog_req_) {
      int64_t len = reserve_pkt_body_buf_.len();
      ObServerStatusFlags server_status;

      // skip 2 bytes of warning_count, we don't care it
      server_status.flags_ = uint2korr(reserve_pkt_body_buf_.ptr() + 2);

      if (params_.is_binlog_req_) {
        server_status.status_flags_.OB_SERVER_STATUS_IN_TRANS = params_.is_in_trans_;
        server_status.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = params_.is_autocommit_;
        char *ptr = const_cast<char*>(pkt_end) - len + 2;
        (*((unsigned short *) (ptr))) = server_status.flags_;
      }
    }
  }
}

bool ObRespAnalyzer::is_last_pkt(const ObAnalyzeHeaderResult &result)
{
  bool ret = false;
  if (OB_LIKELY(ObProxyProtocol::PROTOCOL_OB20 == protocol_)) {
    int64_t non_compressed_len = result.compressed_mysql_header_.non_compressed_len_;
    int64_t ob20_packet_len = result.ob20_header_.payload_len_ + OB20_PROTOCOL_HEADER_TAILER_LENGTH;
    // compressed oceanbase 2.0
    if (params_.is_compressed_) {
      // one compressed packet contains multiple oceanbase 2.0 packets
      if (non_compressed_len > ob20_packet_len) {
        LOG_EDIAG("never happen: one compressed packet contains multiple oceanbase 2.0 packets",
                  K(non_compressed_len), K(ob20_packet_len), K(result), K(protocol_));
        ret = result.ob20_header_.flag_.is_last_packet();
      // one compressed packet contains one oceanbase 2.0 packet so it's the last
      } else if (non_compressed_len == ob20_packet_len) {
        ret = result.ob20_header_.flag_.is_last_packet();
      // one oceanbase 2.0 packet acrosses multiple compressed packets so it's not the last compressed packet
      } else {
        ret = false;
      }
    // oceanbase 2.0
    } else {
      ret = result.ob20_header_.flag_.is_last_packet();
    }
  } else if (ObProxyProtocol::PROTOCOL_CHECKSUM == protocol_) {
    ret = result.compressed_mysql_header_.seq_ == last_compressed_seq_;
  } else {
    // can not determine whether it's the last packet
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase