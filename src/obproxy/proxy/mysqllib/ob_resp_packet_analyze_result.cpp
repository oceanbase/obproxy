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
#include "proxy/mysqllib/ob_resp_packet_analyze_result.h"

using namespace oceanbase::obmysql;
using namespace oceanbase::common;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObRespPacketAnalyzeResult::ObRespPacketAnalyzeResult()
    : enable_extra_ok_packet_for_stats_(false),
      cmd_(OB_MYSQL_COM_END),
      mysql_mode_(UNDEFINED_MYSQL_PROTOCOL_MODE),
      resp_type_(MAX_RESP_TYPE),
      trans_state_(IN_TRANS_STATE_BY_DEFAULT),
      all_pkt_cnt_(0),
      expect_pkt_cnt_(0),
      is_recv_resultset_(false)
{
  MEMSET(pkt_cnt_, 0, sizeof(pkt_cnt_));
}
int ObRespPacketAnalyzeResult::is_resp_finished(
    bool &finished, ObMysqlRespEndingType &ending_type,
    obmysql::ObMySQLCmd req_cmd, ObMysqlProtocolMode protocol_mode,
    bool is_extra_ok_for_stats) const
{
  bool is_mysql_mode = STANDARD_MYSQL_PROTOCOL_MODE == protocol_mode;
  bool is_ob_mysql_mode = OCEANBASE_MYSQL_PROTOCOL_MODE == protocol_mode;
  bool is_ob_oracle_mode = OCEANBASE_ORACLE_PROTOCOL_MODE == protocol_mode;
  bool is_oceanbase_mode = is_ob_mysql_mode || is_ob_oracle_mode;
  int ret = OB_SUCCESS;
  finished = false;
  if (OB_UNLIKELY(pkt_cnt_[OK_PACKET_ENDING_TYPE] < 0)
      || OB_UNLIKELY(pkt_cnt_[OK_PACKET_ENDING_TYPE] > 2)
      || OB_UNLIKELY(pkt_cnt_[EOF_PACKET_ENDING_TYPE] < 0)
      || OB_UNLIKELY(pkt_cnt_[EOF_PACKET_ENDING_TYPE] > 3)
      || OB_UNLIKELY(pkt_cnt_[ERROR_PACKET_ENDING_TYPE] < 0)
      || OB_UNLIKELY(pkt_cnt_[ERROR_PACKET_ENDING_TYPE] > 1)
      || OB_UNLIKELY(OB_MYSQL_COM_END == req_cmd)
      || OB_UNLIKELY(UNDEFINED_MYSQL_PROTOCOL_MODE == protocol_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", "ok pkt count", pkt_cnt_[OK_PACKET_ENDING_TYPE],
             "eof pkt count", pkt_cnt_[EOF_PACKET_ENDING_TYPE],
             "err pkt count", pkt_cnt_[ERROR_PACKET_ENDING_TYPE],
             K(req_cmd), K(protocol_mode), K(ret));
  } else {
    LOG_DEBUG("pkt count", "ok pkt count", pkt_cnt_[OK_PACKET_ENDING_TYPE],
              "error pkt count", pkt_cnt_[ERROR_PACKET_ENDING_TYPE],
              "eof pkt count", pkt_cnt_[EOF_PACKET_ENDING_TYPE],
              "string eof pkt count", pkt_cnt_[STRING_EOF_ENDING_TYPE],
              "prepare ok pkt count", pkt_cnt_[PREPARE_OK_PACKET_ENDING_TYPE],
              K(protocol_mode));
    switch (req_cmd) {
      case OB_MYSQL_COM_TIME :
      case OB_MYSQL_COM_CONNECT :
      case OB_MYSQL_COM_DELAYED_INSERT :
      case OB_MYSQL_COM_DAEMON :
      case OB_MYSQL_COM_SLEEP : {
        if (OB_UNLIKELY(is_mysql_mode)) {
          if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = ERROR_PACKET_ENDING_TYPE;
          }
        } else if (OB_LIKELY(is_oceanbase_mode)) {
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
      case OB_MYSQL_COM_STMT_RESET:
      case OB_MYSQL_COM_INIT_DB :
      case OB_MYSQL_COM_CHANGE_USER:
      case OB_MYSQL_COM_AUTH_SWITCH_RESP:
      case OB_MYSQL_COM_RESET_CONNECTION: {
        if (OB_UNLIKELY(is_mysql_mode)) {
          if (1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = OK_PACKET_ENDING_TYPE;
          } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = ERROR_PACKET_ENDING_TYPE;
          }
        } else if (OB_LIKELY(is_oceanbase_mode)) {
          if (1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]
              && 0 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = OK_PACKET_ENDING_TYPE;
          } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]
                     && 1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = ERROR_PACKET_ENDING_TYPE;
          // eof as auth switch response
          } else if (OB_MYSQL_COM_CHANGE_USER == req_cmd && 1 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = EOF_PACKET_ENDING_TYPE;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected mode error", K(ret), K(protocol_mode));
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
          /* 如果连的是 OB, 最后会多一个 OK 包 */
          if (OB_LIKELY(is_oceanbase_mode)) {
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
              LOG_WDIAG("unknown eof_pkt_cnt", K(eof_pkt_cnt), K(ret));
            }
          }
        } else if (MAX_RESP_TYPE == resp_type_) {
          // have not read resp type, as read data len less than MYSQL_PACKET_HEADER(4)
          finished = false;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WDIAG("infile not supported now", K(ret));
        }

        break;
      }
      case OB_MYSQL_COM_STMT_FETCH: {
        if (RESULT_SET_RESP_TYPE == resp_type_ || OTHERS_RESP_TYPE == resp_type_) {
          if (OB_UNLIKELY(is_mysql_mode || is_ob_mysql_mode)) {
            if (1 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = EOF_PACKET_ENDING_TYPE;
            } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = ERROR_PACKET_ENDING_TYPE;
            } else if (1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = OK_PACKET_ENDING_TYPE;
            }
          } else if (OB_LIKELY(is_ob_oracle_mode) && 1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
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
          LOG_WDIAG("infile not supported now", K(ret));
        }
        break;
      }
      case OB_MYSQL_COM_STMT_EXECUTE:
      case OB_MYSQL_COM_QUERY:
      case OB_MYSQL_COM_STMT_SEND_PIECE_DATA:
      case OB_MYSQL_COM_STMT_GET_PIECE_DATA: {
        if (RESULT_SET_RESP_TYPE == resp_type_ || OTHERS_RESP_TYPE == resp_type_) {
          if (OB_UNLIKELY(is_mysql_mode)) {
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
          } else if (OB_LIKELY(is_oceanbase_mode) && 1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
            if (2 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = EOF_PACKET_ENDING_TYPE;
            } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = ERROR_PACKET_ENDING_TYPE;
            } else if (1 == pkt_cnt_[EOF_PACKET_ENDING_TYPE] && OB_MYSQL_COM_STMT_EXECUTE == req_cmd && is_recv_resultset_) {
              finished = true;
              ending_type = EOF_PACKET_ENDING_TYPE;
            } else {
              finished = true;
              ending_type = OK_PACKET_ENDING_TYPE;
            }
          }
        } else if (LOCAL_INFILE_RESP_TYPE == resp_type_) {
          ending_type = LOCAL_INFILE_ENDING_TYPE;
          finished = true;
          LOG_DEBUG("succ to parse [0xFB - filename] packet from MySQL packet");
        } else if (MAX_RESP_TYPE == resp_type_) {
          // have not read resp type, as read data len less than MYSQL_PACKET_HEADER(4)
          finished = false;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WDIAG("not supported now", K(ret), K_(resp_type));
        }
        break;
      }
      case OB_MYSQL_COM_STMT_PREPARE_EXECUTE: {
        if (RESULT_SET_RESP_TYPE == resp_type_ || OTHERS_RESP_TYPE == resp_type_) {
          if (1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
            /* 只有 error 包时, 才是 error resp, 其他时候都认为是 EOF 结尾, 既是一个结果集 */
            if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE] && 0 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = ERROR_PACKET_ENDING_TYPE;
            } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = EOF_PACKET_ENDING_TYPE;
            } else if (1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = EOF_PACKET_ENDING_TYPE;
            }
          } else {
            finished = false;
          }
        } else if (MAX_RESP_TYPE == resp_type_) {
          // have not read resp type, as read data len less than MYSQL_PACKET_HEADER(4)
          finished = false;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WDIAG("infile not supported now", K(ret));
        }
        break;
      }
      case OB_MYSQL_COM_FIELD_LIST : {
        if (OB_UNLIKELY(is_mysql_mode)) {
          if (1 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = EOF_PACKET_ENDING_TYPE;
          } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = ERROR_PACKET_ENDING_TYPE;
          }
        } else if (OB_LIKELY(is_oceanbase_mode)) {
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
        if (OB_UNLIKELY(is_mysql_mode)) {
          if (1 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = EOF_PACKET_ENDING_TYPE;
          } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
            finished = true;
            ending_type = ERROR_PACKET_ENDING_TYPE;
          }
        } else if (OB_LIKELY(is_oceanbase_mode)) {
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
        if (OB_UNLIKELY(is_mysql_mode)) {
          if ((1 == pkt_cnt_[STRING_EOF_ENDING_TYPE])) {
            finished = true;
            ending_type = STRING_EOF_ENDING_TYPE;
          }
        } else if (OB_LIKELY(is_oceanbase_mode)) {
          if (is_extra_ok_for_stats) {
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
      case OB_MYSQL_COM_BINLOG_DUMP:
      case OB_MYSQL_COM_BINLOG_DUMP_GTID : {
        if (1 == pkt_cnt_[EOF_PACKET_ENDING_TYPE]) {
          finished = true;
          ending_type = EOF_PACKET_ENDING_TYPE;
        } else if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
          finished = true;
          ending_type = ERROR_PACKET_ENDING_TYPE;
        }
        break;
      }
      case OB_MYSQL_COM_SET_OPTION:
      case OB_MYSQL_COM_REGISTER_SLAVE: {
        if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
          finished = true;
          ending_type = ERROR_PACKET_ENDING_TYPE;
        } else if (1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
          finished = true;
          ending_type = OK_PACKET_ENDING_TYPE;
        }
        break;
      }
      case OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT: {
        if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
          finished = true;
          ending_type = ERROR_PACKET_ENDING_TYPE;
        } else if (1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
          finished = true;
          ending_type = OK_PACKET_ENDING_TYPE;
        }
        break;
      }
      default : {
        if (!is_supported_mysql_cmd(req_cmd)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WDIAG("unsupport mysql server cmd", K(req_cmd), K(ret));
          // maybe supported in future version?
          if (OB_UNLIKELY(is_mysql_mode)) {
            if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = ERROR_PACKET_ENDING_TYPE;
            }
          } else if (OB_LIKELY(is_oceanbase_mode)) {
            if (1 == pkt_cnt_[ERROR_PACKET_ENDING_TYPE] && 1 == pkt_cnt_[OK_PACKET_ENDING_TYPE]) {
              finished = true;
              ending_type = ERROR_PACKET_ENDING_TYPE;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unknown mysql server cmd", K(req_cmd), K(ret));
        }
        break;
      }
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase