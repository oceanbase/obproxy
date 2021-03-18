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
#include "proxy/mysqllib/ob_mysql_resp_analyzer.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/packet/ompk_prepare.h"
#include "proxy/mysqllib/ob_mysql_response.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::obutils;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
//------------------ObBufferReader-------------------
inline int ObBufferReader::read(char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid len", K(len), K(ret));
  } else if (OB_UNLIKELY(len > get_remain_len())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf data not enough", K(len), K(ret));
  } else {
    // if buf is NULL, will not copy and just add pos_
    if (OB_LIKELY(NULL != buf)) {
      MEMCPY(buf, resp_buf_.ptr() + pos_, len);
    }
    pos_ += len;
  }
  return ret;
}

//------------------ObMysqlPacketMetaAnalyzer-------------------
inline int ObMysqlPacketMetaAnalyzer::update_cur_type(ObRespResult &result)
{
  int ret = OB_SUCCESS;
  int64_t eof_pkt_cnt = result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE);
  int64_t err_pkt_cnt = result.get_pkt_cnt(ERROR_PACKET_ENDING_TYPE);
  int64_t prepare_ok_pkt_cnt = result.get_pkt_cnt(PREPARE_OK_PACKET_ENDING_TYPE);

  cur_type_ = MAX_PACKET_ENDING_TYPE;
  switch (meta_.pkt_type_) {
    case MYSQL_ERR_PACKET_TYPE:
      cur_type_ = ERROR_PACKET_ENDING_TYPE;
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
      if (OB_MYSQL_COM_STMT_PREPARE == result.get_cmd()) {
        /* Preapre Request, in OCEANBASE, maybe error + ok. in this case, should set OK_PACKET_ENDING_TYPE */
        if (0 == prepare_ok_pkt_cnt && 0 == err_pkt_cnt) {
          cur_type_ = PREPARE_OK_PACKET_ENDING_TYPE;
        } else {
          cur_type_ = OK_PACKET_ENDING_TYPE;
        }
      } else if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE == result.get_cmd()) {
        if (0 == prepare_ok_pkt_cnt && 0 == err_pkt_cnt) {
          cur_type_ = PREPARE_OK_PACKET_ENDING_TYPE;
        } else if (1 == err_pkt_cnt) {
          cur_type_ = OK_PACKET_ENDING_TYPE;
        } else if (result.is_recv_resultset()) {
          cur_type_ = OK_PACKET_ENDING_TYPE;
        }
      } else if (1 != eof_pkt_cnt) {
        cur_type_ = OK_PACKET_ENDING_TYPE;
        // in OCEANBASE_MYSQL_MODE, if we got an erro in ResultSet Protocol(maybe timeout), the packet
        // received from observer is EOF(first),  ERROR(second) and OK(last),
        // so we should recognize the last ok packet.
      } else if ((1 == eof_pkt_cnt) && (1 == err_pkt_cnt)) {
        cur_type_ = OK_PACKET_ENDING_TYPE;
      } else if (OB_MYSQL_COM_FIELD_LIST == result.get_cmd()) {
        cur_type_ = OK_PACKET_ENDING_TYPE;
      } else {
        // cur_type_ = MAX_PACKET_ENDING_TYPE;
      }
      break;

    case MYSQL_EOF_PACKET_TYPE:
      if (meta_.pkt_len_ < 9) {
        cur_type_ = EOF_PACKET_ENDING_TYPE;
      }
      break;

    case MYSQL_HANDSHAKE_PACKET_TYPE:
      if ((0 == eof_pkt_cnt) && (0 == err_pkt_cnt)) {
        cur_type_ = HANDSHAKE_PACKET_ENDING_TYPE;
      }
      break;

    case MYSQL_LOCAL_INFILE_TYPE:
      cur_type_ = LOCAL_INFILE_ENDING_TYPE;
      break;

    default:
      cur_type_ = MAX_PACKET_ENDING_TYPE;
      break;
  }
  return ret;
}

inline bool ObMysqlPacketMetaAnalyzer::is_need_copy(ObRespResult &result) const
{
  return (OK_PACKET_ENDING_TYPE == cur_type_
          // no need copy the second eof packet, only analyze the first eof packet
          || (EOF_PACKET_ENDING_TYPE == cur_type_
              && (0 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE)))
          || ERROR_PACKET_ENDING_TYPE == cur_type_
          || (HANDSHAKE_PACKET_ENDING_TYPE == cur_type_
              && OB_MYSQL_COM_HANDSHAKE == result.get_cmd())
          || PREPARE_OK_PACKET_ENDING_TYPE == cur_type_);
}

inline bool ObMysqlPacketMetaAnalyzer::is_need_reserve_packet(ObRespResult &result) const
{
  // need reserve the second eof packet, all ok and error packets
  return (OK_PACKET_ENDING_TYPE == cur_type_
          || (EOF_PACKET_ENDING_TYPE == cur_type_
              && (1 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE)))
          || ERROR_PACKET_ENDING_TYPE == cur_type_);
}

//------------------ObRespResult------------------
ObRespResult::ObRespResult()
    : is_compress_(false),
      enable_extra_ok_packet_for_stats_(false),
      cmd_(OB_MYSQL_COM_END),
      mysql_mode_(UNDEFINED_MYSQL_PROTOCOL_MODE),
      resp_type_(MAX_RESP_TYPE),
      trans_state_(IN_TRANS_STATE_BY_DEFAULT),
      reserved_len_(0)
{
  MEMSET(pkt_cnt_, 0, sizeof(pkt_cnt_));
}

int ObRespResult::is_resp_finished(bool &finished, ObMysqlRespEndingType &ending_type) const
{
  int ret = OB_SUCCESS;
  finished = false;
  if (OB_UNLIKELY(pkt_cnt_[OK_PACKET_ENDING_TYPE] < 0)
      || OB_UNLIKELY(pkt_cnt_[OK_PACKET_ENDING_TYPE] > 2)
      || OB_UNLIKELY(pkt_cnt_[EOF_PACKET_ENDING_TYPE] < 0)
      || OB_UNLIKELY(pkt_cnt_[EOF_PACKET_ENDING_TYPE] > 3)
      || OB_UNLIKELY(pkt_cnt_[ERROR_PACKET_ENDING_TYPE] < 0)
      || OB_UNLIKELY(pkt_cnt_[ERROR_PACKET_ENDING_TYPE] > 1)
      || OB_UNLIKELY(OB_MYSQL_COM_END == cmd_)
      || OB_UNLIKELY(UNDEFINED_MYSQL_PROTOCOL_MODE == mysql_mode_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pkt_cnt_), K(cmd_), K(mysql_mode_), K(ret));
  } else {
    LOG_DEBUG("pkt count", "ok pkt count", pkt_cnt_[OK_PACKET_ENDING_TYPE],
              "error pkt count", pkt_cnt_[ERROR_PACKET_ENDING_TYPE],
              "eof pkt count", pkt_cnt_[EOF_PACKET_ENDING_TYPE],
              "string eof pkt count", pkt_cnt_[STRING_EOF_ENDING_TYPE],
              "prepare ok pkt count", pkt_cnt_[PREPARE_OK_PACKET_ENDING_TYPE],
              K_(mysql_mode));
    switch (cmd_) {
      case OB_MYSQL_COM_TIME :
      case OB_MYSQL_COM_CONNECT :
      case OB_MYSQL_COM_DELAYED_INSERT :
      case OB_MYSQL_COM_DAEMON :
      case OB_MYSQL_COM_SLEEP : {
        if (OB_UNLIKELY(is_mysql_mode())) {
          if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = ERROR_PACKET_ENDING_TYPE;
          }
        } else if (OB_LIKELY(is_oceanbase_mode())) {
          if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE] && 1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = ERROR_PACKET_ENDING_TYPE;
          }
        }
        break;
      }
      case OB_MYSQL_COM_PING :
      case OB_MYSQL_COM_QUIT : {
        if (1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
          finished = true;
          ending_type = OK_PACKET_ENDING_TYPE;
        }
        break;
      }
      case OB_MYSQL_COM_CREATE_DB :
      case OB_MYSQL_COM_DROP_DB :
      case OB_MYSQL_COM_REFRESH :
      case OB_MYSQL_COM_PROCESS_KILL :
      case OB_MYSQL_COM_LOGIN:
      case OB_MYSQL_COM_INIT_DB : {
        if (OB_UNLIKELY(is_mysql_mode())) {
          if (1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = OK_PACKET_ENDING_TYPE;
          } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = ERROR_PACKET_ENDING_TYPE;
          }
        } else if (OB_LIKELY(is_oceanbase_mode())) {
          if (1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]
              && 0 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = OK_PACKET_ENDING_TYPE;
          } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]
                     && 1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = ERROR_PACKET_ENDING_TYPE;
          }
        }
        break;
      }
      case OB_MYSQL_COM_PROCESS_INFO :
      case OB_MYSQL_COM_STMT_PREPARE : {
        if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
          finished = true;
          ending_type = ERROR_PACKET_ENDING_TYPE;
        } else if (RESULT_SET_RESP_TYPE == resp_type_ || OTHERS_RESP_TYPE == resp_type_) {
          uint32_t expect_pkt_cnt = expect_pkt_cnt_;
          // in oceanbase, have extra OK packet
          if (OB_LIKELY(is_oceanbase_mode())) {
            expect_pkt_cnt += 1;
          }

          if (all_pkt_cnt_ == expect_pkt_cnt) {
            uint64_t eof_pkt_cnt = pkt_cnt_[EOF_PACKET_ENDING_TYPE];
            finished = true;

            if (2 == eof_pkt_cnt || 1 == eof_pkt_cnt) {
              ending_type = EOF_PACKET_ENDING_TYPE;
            } else if (0 == eof_pkt_cnt) {
              ending_type = PREPARE_OK_PACKET_ENDING_TYPE;
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unknown eof_pkt_cnt", K(eof_pkt_cnt), K(ret));
            }
          }
        } else if (MAX_RESP_TYPE == resp_type_) {
          // have not read resp type, as read data len less than MYSQL_PACKET_HEADER(4)
          finished = false;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("infile not supported now", K(ret));
        }

        break;
      }
      case OB_MYSQL_COM_STMT_FETCH:
      case OB_MYSQL_COM_STMT_EXECUTE:
      case OB_MYSQL_COM_QUERY:
      case OB_MYSQL_COM_STMT_SEND_PIECE_DATA:
      case OB_MYSQL_COM_STMT_GET_PIECE_DATA: {
        if (RESULT_SET_RESP_TYPE == resp_type_ || OTHERS_RESP_TYPE == resp_type_) {
          if (OB_UNLIKELY(is_mysql_mode())) {
            if (2 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = EOF_PACKET_ENDING_TYPE;
            } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = ERROR_PACKET_ENDING_TYPE;
            } else if (1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = OK_PACKET_ENDING_TYPE;
            }
          } else if (OB_LIKELY(is_oceanbase_mode()) && 1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
            if (2 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = EOF_PACKET_ENDING_TYPE;
            } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = ERROR_PACKET_ENDING_TYPE;
            } else {
              finished = true;
              ending_type = OK_PACKET_ENDING_TYPE;
            }
          }
        } else if (MAX_RESP_TYPE == resp_type_) {
          // have not read resp type, as read data len less than MYSQL_PACKET_HEADER(4)
          finished = false;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("infile not supported now", K(ret));
        }
        break;
      }
      case OB_MYSQL_COM_STMT_PREPARE_EXECUTE: {
        if (RESULT_SET_RESP_TYPE == resp_type_ || OTHERS_RESP_TYPE == resp_type_) {
          // only error packet is error resp, other is result set
          if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE] && 0 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = ERROR_PACKET_ENDING_TYPE;
          } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = EOF_PACKET_ENDING_TYPE;
          } else if (1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = EOF_PACKET_ENDING_TYPE;
          } else {
            finished = false;
          }
        } else if (MAX_RESP_TYPE == resp_type_) {
          // have not read resp type, as read data len less than MYSQL_PACKET_HEADER(4)
          finished = false;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("infile not supported now", K(ret));
        }
        break;
      }
      case OB_MYSQL_COM_FIELD_LIST : {
        if (OB_UNLIKELY(is_mysql_mode())) {
          if (1 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = EOF_PACKET_ENDING_TYPE;
          } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = ERROR_PACKET_ENDING_TYPE;
          }
        } else if (OB_LIKELY(is_oceanbase_mode())) {
          if (1 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]
              && 0 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]
              && 1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = EOF_PACKET_ENDING_TYPE;
          } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]
                     && 1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]
                     && 0 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = ERROR_PACKET_ENDING_TYPE;
          }
        }
        break;
      }
      case OB_MYSQL_COM_SHUTDOWN :
      case OB_MYSQL_COM_DEBUG : {
        if (OB_UNLIKELY(is_mysql_mode())) {
          if (1 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = EOF_PACKET_ENDING_TYPE;
          } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = ERROR_PACKET_ENDING_TYPE;
          }
        } else if (OB_LIKELY(is_oceanbase_mode())) {
          if (1 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]
              && 0 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]
              && 0 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = EOF_PACKET_ENDING_TYPE;
          } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]
                     && 1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]
                     && 0 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = ERROR_PACKET_ENDING_TYPE;
          }
        }
        break;
      }
      case OB_MYSQL_COM_STATISTICS : {
        if (OB_UNLIKELY(is_mysql_mode())) {
          if ((1 == pkt_cnt_[STRING_EOF_ENDING_TYPE])) {
            finished = true;
            ending_type = STRING_EOF_ENDING_TYPE;
          }
        } else if (OB_LIKELY(is_oceanbase_mode())) {
          if (is_extra_ok_packet_for_stats_enabled()) {
            if ((1 == pkt_cnt_[STRING_EOF_ENDING_TYPE])
                && (1 == pkt_cnt_[OK_PACKET_ENDING_TYPE])) {
              finished = true;
              ending_type = STRING_EOF_ENDING_TYPE;
            }
          } else {
            if ((1 == pkt_cnt_[STRING_EOF_ENDING_TYPE])) {
              finished = true;
              ending_type = STRING_EOF_ENDING_TYPE;
            }
          }
        }
        break;
      }
      case OB_MYSQL_COM_HANDSHAKE : {
        if (1 == pkt_cnt_[HANDSHAKE_PACKET_ENDING_TYPE]) {
          finished = true;
          ending_type = HANDSHAKE_PACKET_ENDING_TYPE;
        }
        if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
          finished = true;
          ending_type = ERROR_PACKET_ENDING_TYPE;
        }
        break;
      }
      default : {
        if (!is_supported_mysql_cmd(cmd_)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupport mysql server cmd", K_(cmd), K(ret));
          // maybe supported in future version?
          if (OB_UNLIKELY(is_mysql_mode())) {
            if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = ERROR_PACKET_ENDING_TYPE;
            }
          } else if (OB_LIKELY(is_oceanbase_mode())) {
            if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE] && 1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = ERROR_PACKET_ENDING_TYPE;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknown mysql server cmd", K_(cmd), K(ret));
        }
        break;
      }
    }
  }
  return ret;
}

//--------------------ObMysqlRespAnalyzer--------------------
inline int ObMysqlRespAnalyzer::read_pkt_hdr(ObBufferReader &buf_reader)
{
  int ret = OB_SUCCESS;
  int64_t data_len = buf_reader.get_remain_len();
  const char *buf_start = NULL;

  if (OB_LIKELY(meta_analyzer_.empty()) && OB_LIKELY(data_len >= MYSQL_NET_HEADER_LENGTH)) {
    buf_start = buf_reader.get_ptr();
    buf_reader.pos_ += MYSQL_NET_HEADER_LENGTH;
    reserved_len_ += MYSQL_NET_HEADER_LENGTH;
  } else {
    int64_t len = std::min(data_len, meta_analyzer_.get_remain_len());
    if (OB_FAIL(buf_reader.read(meta_analyzer_.get_insert_pos(), len))) {
      LOG_WARN("fail to read resp buffer", K(len), K(ret));
    } else if (OB_FAIL(meta_analyzer_.add_valid_len(len))) {
      LOG_WARN("fail to add valid cnt", K(len), K(ret));
    } else {
      // reserve the header, avoid sending part of packet header in more than one net io
      reserved_len_ += len;
    }

    if (meta_analyzer_.is_full_filled()) {
      buf_start = meta_analyzer_.get_ptr();
    }
  }

  if (OB_LIKELY(NULL != buf_start)) {
    meta_analyzer_.get_meta().pkt_len_ = ob_uint3korr(buf_start);
    meta_analyzer_.get_meta().pkt_seq_ = ob_uint1korr(buf_start + 3);
    state_ = READ_TYPE;
    next_read_len_ = meta_analyzer_.get_meta().pkt_len_;
  }
  return ret;
}

inline int ObMysqlRespAnalyzer::read_pkt_type(ObBufferReader &buf_reader, ObRespResult &result)
{
  int ret = OB_SUCCESS;
  int64_t data_len = buf_reader.get_remain_len();

  // empty response packet, OB_MYSQL_COM_STATISTICS and multi packet can not read type
  if (OB_LIKELY(!is_in_multi_pkt_)
      && OB_LIKELY(OB_MYSQL_COM_STATISTICS != result.get_cmd())
      && OB_LIKELY(meta_analyzer_.get_meta().pkt_len_ > 0)) {

    if (OB_LIKELY(data_len > 0)) {
      meta_analyzer_.get_meta().pkt_type_ = static_cast<uint8_t>(*buf_reader.get_ptr());
      buf_reader.pos_ += 1;
      next_read_len_ -= 1;
      // reserve the header, avoid sending part of packet header in more than one net io
      reserved_len_ += 1;
      state_ = READ_BODY;
      is_in_multi_pkt_ = (MYSQL_PACKET_MAX_LENGTH == meta_analyzer_.get_meta().pkt_len_);
      if (is_in_multi_pkt_) {
        LOG_INFO("now received large packet", "meta", meta_analyzer_.get_meta(), K_(is_in_multi_pkt));
      }

      if (OB_FAIL(meta_analyzer_.update_cur_type(result))) {
        LOG_WARN("fail to update ending type", K(ret));
      } else {
        if (OB_LIKELY(OB_MYSQL_COM_QUERY == result.get_cmd()
                   || OB_MYSQL_COM_PROCESS_INFO == result.get_cmd()
                   || OB_MYSQL_COM_STMT_PREPARE == result.get_cmd()
                   || OB_MYSQL_COM_STMT_FETCH == result.get_cmd()
                   || OB_MYSQL_COM_STMT_EXECUTE == result.get_cmd()
                   || OB_MYSQL_COM_STMT_PREPARE_EXECUTE == result.get_cmd()
                   || OB_MYSQL_COM_STMT_GET_PIECE_DATA == result.get_cmd()
                   || OB_MYSQL_COM_STMT_SEND_PIECE_DATA == result.get_cmd())
            && OB_LIKELY(MAX_RESP_TYPE == result.get_resp_type())) {
          ObMysqlRespEndingType type = meta_analyzer_.get_cur_type();
          if (ERROR_PACKET_ENDING_TYPE == type || OK_PACKET_ENDING_TYPE == type) {
            result.set_resp_type(OTHERS_RESP_TYPE);
          } else if (OB_UNLIKELY(LOCAL_INFILE_ENDING_TYPE == type)) {
            result.set_resp_type(LOCAL_INFILE_RESP_TYPE);
          } else {
            result.set_resp_type(RESULT_SET_RESP_TYPE);
          }
        }

        if (meta_analyzer_.is_need_copy(result)) {
          // if ok packet, just copy OK_PACKET_MAX_COPY_LEN(default is 20) len to improve efficiency
          int64_t mem_len = (OK_PACKET_ENDING_TYPE == meta_analyzer_.get_cur_type()
                             ? OK_PACKET_MAX_COPY_LEN : meta_analyzer_.get_meta().pkt_len_);
          if (OB_FAIL(body_buf_.init(mem_len))) {
            LOG_WARN("fail to alloc mem", K(ret), K(mem_len));
          //need save first byte to paser in OMPKHandshake/OMPKPrepare
          } else if ((HANDSHAKE_PACKET_ENDING_TYPE == meta_analyzer_.get_cur_type()
                      || PREPARE_OK_PACKET_ENDING_TYPE == meta_analyzer_.get_cur_type())
                  && OB_FAIL(body_buf_.write(buf_reader.get_ptr() - 1 , 1))) {
            LOG_WARN("fail to write handshake protocol version", K(ret));
          }
        }
      }
    }
  } else {
    if (OB_UNLIKELY(OB_MYSQL_COM_STATISTICS == result.get_cmd())) {
      int64_t string_eof_pkt_cnt = result.get_pkt_cnt(STRING_EOF_ENDING_TYPE);
      if (0 == string_eof_pkt_cnt) {
        meta_analyzer_.set_cur_type(STRING_EOF_ENDING_TYPE);
      } else if (1 == string_eof_pkt_cnt) {
        if (result.is_extra_ok_packet_for_stats_enabled()) {
          // will followed by an ok packet
          meta_analyzer_.set_cur_type(OK_PACKET_ENDING_TYPE);
          int64_t mem_len = OK_PACKET_MAX_COPY_LEN;
          if (OB_FAIL(body_buf_.init(mem_len))) {
            LOG_WARN("fail to alloc mem", K(mem_len), K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid string_eof_pkt_cnt", K(string_eof_pkt_cnt), K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid string_eof_pkt_cnt", K(string_eof_pkt_cnt), K(ret));
      }
    } else {
      meta_analyzer_.set_cur_type(MAX_PACKET_ENDING_TYPE);
    }
    state_ = READ_BODY;
  }
  return ret;
}

inline int ObMysqlRespAnalyzer::read_pkt_body(ObBufferReader &buf_reader,
                                              ObRespResult &result)
{
  int ret = OB_SUCCESS;
  int64_t data_len = buf_reader.get_remain_len();

  if (next_read_len_ > 0 && data_len > 0){
    int64_t len = std::min(data_len, next_read_len_);

    // only copy OK, the first EOF or ERROR or HANDSHAKE
    if (body_buf_.remain() > 0) {
      int64_t len_left = std::min(len, body_buf_.remain());
      if (OB_FAIL(body_buf_.write(buf_reader.get_ptr(), len_left))) {
        LOG_WARN("fail to write", K(len_left), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (meta_analyzer_.is_need_reserve_packet(result)) {
        // reserve eof, ok, error and handshake packet,
        //avoid sending part of these packet in more than one net io
        reserved_len_ += len;
      } else {
        // it isn't eof, ok, error or handshake packet, needn't reserve
        reserved_len_ = 0;
      }
      buf_reader.pos_ += len;
      next_read_len_ -= len;
    }
  }
  return ret;
}

inline int ObMysqlRespAnalyzer::analyze_resp_pkt(
    ObRespResult &result,
    ObMysqlResp *resp)
{
  int ret = OB_SUCCESS;
  int64_t eof_pkt_cnt = result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE);
  int64_t err_pkt_cnt = result.get_pkt_cnt(ERROR_PACKET_ENDING_TYPE);
  int64_t string_eof_pkt_cnt = result.get_pkt_cnt(STRING_EOF_ENDING_TYPE);
  int64_t prepare_ok_pkt_cnt = result.get_pkt_cnt(PREPARE_OK_PACKET_ENDING_TYPE);
  ObMysqlRespEndingType pkt_type = meta_analyzer_.get_cur_type();
  uint32_t pkt_len = meta_analyzer_.get_meta().pkt_len_;

  // in these case, we will reset pkt cnt:
  // 1. ok packet (has more result)
  // 2. the second eof packet (has more result)
  ObOKPacketActionType ok_packet_action_type = OK_PACKET_ACTION_SEND;
  bool is_in_trans = false;

  switch (pkt_type) {
    case OK_PACKET_ENDING_TYPE : {
      // in any case, we should analyze packet
      if (OB_FAIL(analyze_ok_pkt(is_in_trans))) {
        LOG_WARN("fail to analyze_ok_pkt", K(ret));
      } else {
        if (is_in_trans) {
          result.set_trans_state(IN_TRANS_STATE_BY_PARSE);
        } else {
          result.set_trans_state(NOT_IN_TRANS_STATE_BY_PARSE);
        }
      }

      // judge pacekt action
      if (OB_SUCC(ret)) {
        if (OB_LIKELY(is_oceanbase_mode())) {
          if (cur_stmt_has_more_result_) {
            // in multi stmt, send directly
            ok_packet_action_type = OK_PACKET_ACTION_SEND;
          } else {
            if (OB_MYSQL_COM_FIELD_LIST == result.get_cmd() && 1 == eof_pkt_cnt) {
              ok_packet_action_type = OK_PACKET_ACTION_CONSUME;
            /* stmt prepare execute's OK packet need pass to client, except OK packet after error packet */
            } else if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE == result.get_cmd()) {
              if (1 == err_pkt_cnt) {
                ok_packet_action_type = OK_PACKET_ACTION_CONSUME;
              } else {
                ok_packet_action_type = OK_PACKET_ACTION_REWRITE;
              }
            } else if (COM_STMT_GET_PIECE_DATA == result.get_cmd()) {
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
            } else if (0 == err_pkt_cnt || 0 == eof_pkt_cnt) {
              // last ok packet, no err and eof in front
              // NOTE: in multi stmt, we will reset pkt cnt if it isn't the last stmt
              ok_packet_action_type = OK_PACKET_ACTION_REWRITE;
            } else {
              // is not valid in mysql protocol
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected ok packet", K(err_pkt_cnt), K(eof_pkt_cnt), K(ret));
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
        if (OB_LIKELY(NULL != resp)) {
          resp->get_analyze_result().last_ok_pkt_len_ = pkt_len + MYSQL_NET_HEADER_LENGTH;
        } else {
          // do nothing
        }
      } else {
        // do nothing
      }

      if (cur_stmt_has_more_result_) {
        // has more stmt, reset pkt_cnt and record flag
        result.reset_pkt_cnt();
      } else {
        result.inc_pkt_cnt(OK_PACKET_ENDING_TYPE);
      }
      break;
    }
    case PREPARE_OK_PACKET_ENDING_TYPE : {
      if (OB_FAIL(analyze_prepare_ok_pkt(result))) {
        LOG_WARN("fail to analyze_prepare_ok_pkt", K(ret));
      } else {
        result.inc_pkt_cnt(PREPARE_OK_PACKET_ENDING_TYPE);
      }

      break;
    }
    case EOF_PACKET_ENDING_TYPE : {
      // normal protocol, the second EOF is last EOF
      bool is_last_eof_pkt = (1 == eof_pkt_cnt);

      if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE == result.get_cmd()) {
        uint32_t expect_pkt_cnt = result.get_expect_pkt_cnt();
        uint32_t all_pkt_cnt = result.get_all_pkt_cnt();
        if (all_pkt_cnt > expect_pkt_cnt) {
          is_last_eof_pkt = true;
          // if last EOF, recv all ResultSet
          result.set_recv_resultset(true);
        } else {
          is_last_eof_pkt = false;
        }
      }

      if (0 == eof_pkt_cnt) {
        // analyze the first eof packet
        bool is_in_trans = false;
        if (OB_FAIL(analyze_eof_pkt(is_in_trans))) {
          LOG_WARN("fail to analyze_eof_pkt", K(ret));
        } else {
          if (is_in_trans) {
            result.set_trans_state(IN_TRANS_STATE_BY_PARSE);
          } else {
            result.set_trans_state(NOT_IN_TRANS_STATE_BY_PARSE);
          }
        }
      } else if (is_last_eof_pkt) {
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
        }
      } else if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE != result.get_cmd()){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected eof packet", K(err_pkt_cnt), K(eof_pkt_cnt), K(ret));
        ok_packet_action_type = OK_PACKET_ACTION_SEND;
      }

      // if has more result, reset the packet cnt if we received the second eof
      if (cur_stmt_has_more_result_ && is_last_eof_pkt) {
        result.reset_pkt_cnt();
      } else {
        result.inc_pkt_cnt(EOF_PACKET_ENDING_TYPE);
      }
      break;
    }
    case ERROR_PACKET_ENDING_TYPE : {
      if (OB_FAIL(analyze_error_pkt(resp))) {
        LOG_WARN("fail to analyze_error_pkt", K(ret));
      } else if (OB_LIKELY(is_oceanbase_mode())) {
        // resultset protocol, after read the error packet, hold it until the extra ok
        // packet is read completed.
        reserved_len_ = pkt_len + MYSQL_NET_HEADER_LENGTH;
      }
      // inc the error packet count
      if (OB_SUCC(ret)) {
        result.inc_pkt_cnt(ERROR_PACKET_ENDING_TYPE);
      }
      break;
    }
    case HANDSHAKE_PACKET_ENDING_TYPE: {
      if (OB_MYSQL_COM_HANDSHAKE == result.get_cmd()) {
        if (OB_FAIL(analyze_hanshake_pkt(resp))) {
          LOG_WARN("fail to analyze_error_pkt", K(ret));
        } else {
          result.inc_pkt_cnt(HANDSHAKE_PACKET_ENDING_TYPE);
        }
      }
      break;
    }
    case STRING_EOF_ENDING_TYPE : {
      if (OB_LIKELY(is_oceanbase_mode()) && result.is_extra_ok_packet_for_stats_enabled()) {
        // STRING_EOF_ENDING_TYPE will followed by an ok packet
        reserved_len_ = pkt_len + MYSQL_NET_HEADER_LENGTH;
      }
      result.inc_pkt_cnt(STRING_EOF_ENDING_TYPE);
      LOG_DEBUG("string eof ending type packet", "is_oceanbase_mode", is_oceanbase_mode(),
                "enable_extra_ok_packet_for_stats",
                result.is_extra_ok_packet_for_stats_enabled());
      break;
    }
    case LOCAL_INFILE_ENDING_TYPE: {
      if (OB_UNLIKELY(LOCAL_INFILE_RESP_TYPE == result.get_resp_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("local infile not support now!", K(ret));
        result.inc_pkt_cnt(LOCAL_INFILE_ENDING_TYPE);
      }
      break;
    }
    case MAX_PACKET_ENDING_TYPE : { // handle common mysql packet
      // for multi packet, not test now
      if (MYSQL_PACKET_MAX_LENGTH != pkt_len && is_in_multi_pkt_) {
        is_in_multi_pkt_ = false;
        LOG_INFO("now large packet receive complete",
                 "meta", meta_analyzer_.get_meta(), K_(is_in_multi_pkt));
      }
      break;
    }
    default : {
      LOG_WARN("invalid packet type", K(pkt_type));
      break;
    }
  }

  if (OB_LIKELY(NULL != resp)) {
    resp->get_analyze_result().ok_packet_action_type_ = ok_packet_action_type;
  } else {
    // do nothing
  }

  return ret;
}

int ObMysqlRespAnalyzer::analyze_mysql_resp(
    ObBufferReader &buf_reader,
    ObRespResult &result,
    ObMysqlResp *resp)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(UNDEFINED_MYSQL_PROTOCOL_MODE == mysql_mode_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql mod was not set", K_(mysql_mode), K(ret));
  } else {
    while ((OB_SUCC(ret)) && !buf_reader.empty()) {
      switch (state_) {
        case READ_HEADER:
          if (OB_FAIL(read_pkt_hdr(buf_reader))) {
            LOG_WARN("fail to read packet header", K(ret));
          }
          break;

        case READ_TYPE:
          if (OB_FAIL(read_pkt_type(buf_reader, result))) {
            LOG_WARN("fail to read packet type", K(ret));
          }
          break;

        case READ_BODY:
          if (OB_FAIL(read_pkt_body(buf_reader, result))) {
            LOG_WARN("fail to read packet body", K(ret));
          }
          break;

        default:
          ret = OB_INNER_STAT_ERROR;
          LOG_WARN("unknown state", K_(state), K(ret));
          break;
      }

      if (OB_SUCC(ret)) {
        if ((OB_MYSQL_COM_STATISTICS == result.get_cmd())
            && (0 == next_read_len_)
            && (READ_TYPE == state_)) {
          // OB_MYSQL_COM_STATISTICS's response maybe only has packet header, so at this time,
          // if READ_HEADER completed, then next_read_len_ == 0, and we need
          // continue read type
          LOG_DEBUG("OB_MYSQL_COM_STATISTICS will read packet type");
        } else {
          if (READ_HEADER != state_ && 0 == next_read_len_) { // the mysql packet has read completely
            reserved_len_ = 0; // after read one whole packet, we reset reserved_len_
            result.inc_all_pkt_cnt();
            if (OB_FAIL(analyze_resp_pkt(result, resp))) {
              LOG_WARN("fail to analyze resp packet", K(ret));
            } else {
              meta_analyzer_.reset();
              body_buf_.reset();
              next_read_len_ = 0;
              state_ = READ_HEADER;
              //do not clear is_in_multi_pkt_
            }
          } else if (OB_UNLIKELY(next_read_len_ < 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("next_read_len should not less than 0", K_(next_read_len), K(ret));
          }
        }
      }
    }
    result.set_reserved_len(reserved_len_);
  }
  return ret;
}

inline int ObMysqlRespAnalyzer::build_packet_content(
    ObVariableLenBuffer<FIXED_MEMORY_BUFFER_SIZE> &content_buf)
{
  int ret = OB_SUCCESS;
  content_buf.reset();

  int64_t body_len = body_buf_.len();
  const char *body_ptr = body_buf_.ptr();
  int64_t type_len = 1;

  if (OB_UNLIKELY(MYSQL_NET_TYPE_LENGTH != type_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type_len is not MYSQL_NET_TYPE_LENGTH ", K(ret));
  } else {
    int64_t mem_len = body_len + type_len;
    if (OB_FAIL(content_buf.init(mem_len))) {
      LOG_WARN("fail int alloc mem", K(mem_len), K(ret));
    } else {
      char *pos = content_buf.pos();
      *pos = static_cast<char>(meta_analyzer_.get_meta().pkt_type_);
      if (OB_FAIL(content_buf.consume(type_len))) {
        LOG_WARN("failed to consume", K(type_len), K(ret));
      }

      if (OB_SUCC(ret)) {
        pos = content_buf.pos();
        MEMCPY(pos, body_ptr, body_len);
        if (OB_FAIL(content_buf.consume(body_len))) {
          LOG_WARN("failed to consume", K(body_len), K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t content_len = content_buf.len();
        LOG_DEBUG("build packet content", K(content_len), K(type_len), K(body_len));
        if (OB_UNLIKELY(content_len != type_len + body_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("content_len is not valid ", K(type_len), K(body_len), K(ret));
        }
      }
    }
  }
  return ret;
}

// ref:http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
inline int ObMysqlRespAnalyzer::analyze_ok_pkt(bool &is_in_trans)
{
  int ret = OB_SUCCESS;
  // only need to get the OB_SERVER_STATUS_IN_TRANS bit
  int64_t len = body_buf_.len();
  const char *ptr = body_buf_.ptr();

  const char *pos = ptr;
  uint64_t affected_rows = 0;
  uint64_t last_insert_id = 0;
  ObServerStatusFlags server_status;

  if (OB_FAIL(ObMySQLUtil::get_length(pos, affected_rows))) {
    LOG_WARN("get length failed", K(ptr), K(pos), K(affected_rows), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::get_length(pos, last_insert_id))) {
    LOG_WARN("get length failed", K(ptr), K(pos), K(last_insert_id), K(ret));
  } else if (FALSE_IT(ObMySQLUtil::get_uint2(pos, server_status.flags_))) {
    // impossible
  } else if (OB_UNLIKELY((pos - ptr) > OK_PACKET_MAX_COPY_LEN)
             || OB_UNLIKELY((pos - ptr) > len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ptr pos", K(ptr), K(pos), K(len), K(ret));
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
  }

  return ret;
}

// ref:https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html
inline int ObMysqlRespAnalyzer::analyze_prepare_ok_pkt(ObRespResult &result)
{
  int ret = OB_SUCCESS;

  //body_buf not include first type byte[00]
  const char *ptr = body_buf_.ptr();

  OMPKPrepare prepare_packet;
  prepare_packet.set_content(ptr, static_cast<uint32_t>(body_buf_.len()));
  if (OB_FAIL(prepare_packet.decode())) {
    LOG_WARN("decode packet failed", K(ret));
  } else {
    int32_t expect_pkt_cnt = 1;
    if (prepare_packet.get_param_num() > 0) {
      expect_pkt_cnt += (prepare_packet.get_param_num() + 1);
    }

    if (prepare_packet.get_column_num()> 0) {
      expect_pkt_cnt += (prepare_packet.get_column_num() + 1);
    }

    result.set_expect_pkt_cnt(expect_pkt_cnt);
    // if no resultSet, set true
    result.set_recv_resultset(0 == prepare_packet.has_result_set());
  }

  return ret;
}

// ref:http://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
inline int ObMysqlRespAnalyzer::analyze_eof_pkt(bool &is_in_trans)
{
  int ret = OB_SUCCESS;
  int64_t len = body_buf_.len();
  ObServerStatusFlags server_status;

  if (len < 4) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("fail to build packet content", K(ret));
  } else {
    // skip 2 bytes of warning_count, we don't care it
    server_status.flags_ = ob_uint2korr(body_buf_.ptr() + 2);

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
  }

  return ret;
}

// ref:http://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
inline int ObMysqlRespAnalyzer::analyze_error_pkt(ObMysqlResp *resp)
{
  int ret = OB_SUCCESS;

  if (NULL != resp) {
    ObVariableLenBuffer<FIXED_MEMORY_BUFFER_SIZE> &content_buf
      = resp->get_analyze_result().error_pkt_buf_;
    if (OB_FAIL(build_packet_content(content_buf))) {
      LOG_WARN("fail to build packet content", K(ret));
    } else {
      int64_t len = content_buf.len();
      const char *ptr = content_buf.ptr();

      ObRespAnalyzeResult &analyze_result = resp->get_analyze_result();
      OMPKError &err_pkt = analyze_result.get_error_pkt();
      err_pkt.set_content(ptr, static_cast<uint32_t>(len));
      if (OB_FAIL(err_pkt.decode())) {
        LOG_WARN("fail to decode error packet", K(ret));
      }
    }
  }

  return ret;
}

// ref: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeV10
int ObMysqlRespAnalyzer::analyze_hanshake_pkt(ObMysqlResp *resp)
{
  int ret = OB_SUCCESS;
  if (NULL != resp) {
    // only need to get the OB_SERVER_STATUS_IN_TRANS bit
    const char *ptr = body_buf_.ptr();
    ObRespAnalyzeResult &analyze_result = resp->get_analyze_result();

    // skip ptotocol_version and server version
    // handshake packet content:
    // int<1> protocol_version --> string<EOF> server_version --> int<4> connection_id --> others
    OMPKHandshake packet;
    packet.set_content(ptr, static_cast<uint32_t>(body_buf_.len()));
    if (OB_FAIL(packet.decode())) {
      LOG_WARN("decode packet failed", K(ret));
    } else {
      analyze_result.server_capabilities_lower_.capability_ = packet.get_server_capability_lower();
      analyze_result.server_capabilities_upper_.capability_ = packet.get_server_capability_upper();
      analyze_result.connection_id_ = packet.get_thread_id();
      int64_t copy_len = 0;
      if (OB_FAIL(packet.get_scramble(analyze_result.scramble_buf_,
                                      static_cast<int64_t>(sizeof(analyze_result.scramble_buf_)),
                                      copy_len))) {
        LOG_WARN("fail to get scramble", K(ret));
      } else if (OB_UNLIKELY(copy_len >= static_cast<int64_t>(sizeof(analyze_result.scramble_buf_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("copy_len is too bigger", K(copy_len), K(ret));
      } else {
        analyze_result.scramble_buf_[copy_len] = '\0';
      }
    }
    LOG_DEBUG("succ to get connection id and scramble ",
              "connection_id", analyze_result.connection_id_,
              "scramble_buf", analyze_result.scramble_buf_);
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

