/**
 * Copyright (c) 2023 OceanBase
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
#include "proxy/mysqllib/ob_protocol_diagnosis.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "proxy/mysqllib/ob_resp_packet_analyze_result.h"
namespace oceanbase
{
using namespace common;
using namespace obmysql;
namespace obproxy
{
namespace proxy
{

ObPacketFoldType ObMysqlPacketRecord::get_fold_type(const ObMySQLCmd cmd, const ObRespPacketAnalyzeResult &result) {
  ObPacketFoldType fold_type = OB_PACKET_FOLD_TYPE_NONE;
  if (OB_MYSQL_COM_QUERY == cmd || OB_MYSQL_COM_STMT_EXECUTE == cmd) {
    if (1 < result.get_all_pkt_cnt() &&
        0 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE)) {
      fold_type = OB_PACKET_FOLD_TYPE_COL_DEF;
    } else if (1 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE)) {
      fold_type = OB_PACKET_FOLD_TYPE_ROW;
    } else {
      fold_type = OB_PACKET_FOLD_TYPE_NONE;
    }
  } else if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE == cmd) {
    if (1 < result.get_all_pkt_cnt() &&
        (0 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE) ||
         1 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE))) {
      fold_type = OB_PACKET_FOLD_TYPE_COL_DEF;
    } else if (2 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE)) {
      fold_type = OB_PACKET_FOLD_TYPE_ROW;
    } else {
      fold_type = OB_PACKET_FOLD_TYPE_NONE;
    }
  } else if (OB_MYSQL_COM_STMT_FETCH == cmd) {
    if (OCEANBASE_ORACLE_PROTOCOL_MODE == result.get_mysql_mode()) {
      if (1 < result.get_all_pkt_cnt() &&
          0 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE)) {
        fold_type = OB_PACKET_FOLD_TYPE_COL_DEF;
      } else if (1 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE)) {
        fold_type = OB_PACKET_FOLD_TYPE_ROW;
      } else {
        fold_type = OB_PACKET_FOLD_TYPE_NONE;
      }
    } else {
      if (0 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE)) {
        fold_type = OB_PACKET_FOLD_TYPE_ROW;
      } else {
        fold_type = OB_PACKET_FOLD_TYPE_NONE;
      }
    }
  }

  return fold_type;
}

int64_t ObPacketRecord::to_string(char *buf, const int64_t buf_len) const {
  int64_t pos = 0;
  J_OBJ_START();

  J_KV("\"action\"", get_record_action_str(static_cast<ObPacketRecordAction>(status_)),
       "\"protocol\"", get_record_type_str(static_cast<ObPacketRecordType>(type_)));
  J_COMMA();
  char *tmp = NULL;
  uint32_t compressed_len = 0;
  uint32_t uncompressed_len = 0;
  switch (type_) {
    case OB_PACKET_RECORD_TYPE_MYSQL:
      if (mysql_rec_.fold_type_ == OB_PACKET_FOLD_TYPE_NONE) {
        uint32_t len = 0;
        ObMySQLUtil::get_uint3(tmp = (char*) mysql_rec_.len_, len);
        J_KV("\"len\"", len, "\"seq\"", mysql_rec_.seq_);
        J_COMMA();
        if (status_ == OB_PACKET_RECORD_ACTION_REQ) {
          J_KV("\"cmd\"", ObProxyParserUtils::get_sql_cmd_name(static_cast<ObMySQLCmd>(mysql_rec_.cmd_)));
        } else {
          BUF_PRINTF("\"type\"");
          J_COLON();
          if (mysql_rec_.pkt_type_ == 0xFE) {
            BUF_PRINTF("\"eof\"");
          } else {
            BUF_PRINTF("\"0x%x\"", mysql_rec_.pkt_type_);
          }
        }
      } else {
        uint32_t cnt = 0;
        ObMySQLUtil::get_uint3(tmp = (char*) mysql_rec_.cnt_, cnt);
        J_KV("\"count\"", cnt, "\"seq\"", mysql_rec_.seq_, "\"type\"", get_fold_type_str(mysql_rec_.fold_type_));
      }
      break;

    case OB_PACKET_RECORD_TYPE_COMPRESSED:
      ObMySQLUtil::get_uint3(tmp = (char*) compressed_mysql_rec_.compressed_len_, compressed_len);
      ObMySQLUtil::get_uint3(tmp = (char*) compressed_mysql_rec_.uncompressed_len_, uncompressed_len);
      J_KV("\"compressed_len\"", compressed_len,
           "\"compressed_seq\"", compressed_mysql_rec_.compressed_seq_,
           "\"uncompressed_len\"", uncompressed_len);
      break;

    case OB_PACKET_RECORD_TYPE_OB20:
      ObMySQLUtil::get_uint3(tmp = (char*) ob20_rec_.compressed_len_, compressed_len);
      ObMySQLUtil::get_uint3(tmp = (char*) ob20_rec_.uncompressed_len_, uncompressed_len);
      uint32_t req_id = 0;
      ObMySQLUtil::get_uint3(tmp = (char*) ob20_rec_.request_id_, req_id);
      J_KV("\"compressed_len\"", compressed_len,
           "\"compressed_seq\"", ob20_rec_.compressed_seq_,
           "\"uncompressed_len\"", uncompressed_len,
           "\"payload_len\"", ob20_rec_.payload_len_,
           "\"conn_id\"", ob20_rec_.connection_id_,
           "\"req_id\"", req_id,
           "\"pkt_seq\"", ob20_rec_.pkt_seq_,
           "\"flag\"", ob20_rec_.flag_.flags_);
      break;
  };
  J_OBJ_END();

  return pos;
}

int64_t ObRespForwardDataFlow::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  BUF_PRINTF("\"sm_read\"");
  J_COLON();
  J_ARRAY_START();
  for (int i = 0; i < sm_read_.count(); i++) {
    BUF_PRINTF("%ld", sm_read_[i]);
    if (i + 1 < sm_read_.count()) {
      J_COMMA();
    }
  }
  J_ARRAY_END();
  J_COMMA();

  J_KV("\"sm_read_internal_sync_request_resp\"", sm_read_internal_sync_request_resp_,
       "\"tunnel_init\"", tunnel_init_,
       "\"sm_trim_ok\"", sm_trim_ok_,
       "\"sm_rewrite_ok_delta\"", sm_rewrite_ok_delta_,
       "\"plugin_trim_ok\"", plugin_trim_ok_,
       "\"plugin_rewrite_ok_delta\"", plugin_rewrite_ok_delta_,

       "\"producer_observer_read\"", producer_observer_read_,

       "\"plugin_decompress_read\"", plugin_decompress_read_,
       "\"plugin_decompress_decrease\"", plugin_decompress_decrease_,
       "\"plugin_decompress_write\"", plugin_decompress_write_,

       "\"plugin_cursor_read\"", plugin_cursor_read_,
       "\"plugin_cursor_write\"", plugin_cursor_write_,

       "\"plugin_prepare_read\"", plugin_prepare_read_,
       "\"plugin_prepare_write\"", plugin_prepare_write_,

       "\"plugin_prepare_execute_read\"", plugin_prepare_execute_read_,
       "\"consumer_transform_write\"", consumer_transform_write_,
       "\"plugin_prepare_execute_write\"", plugin_prepare_execute_write_,

       "\"producer_transform_read\"", producer_transform_read_,
       "\"consumer_client_write\"", consumer_client_write_
      );
  J_OBJ_END();

  return pos;
}

int64_t ObReqForwardDataFlow::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  BUF_PRINTF("\"sm_read\"");
  J_COLON();
  J_ARRAY_START();
  for (int i = 0; i < sm_read_.count(); i++) {
    BUF_PRINTF("%ld", sm_read_[i]);
    if (i + 1 < sm_read_.count()) {
      J_COMMA();
    }
  }
  J_ARRAY_END();
  J_COMMA();

  J_KV("\"sm_write\"", sm_write_,
       "\"sm_write_sync_req\"", sm_write_sync_req_,
       "\"tunnel_init\"", tunnel_init_,
       "\"tunnel_next_req_of_send_long\"", tunnel_next_req_of_send_long_,

       "\"producer_client_read\"", producer_client_read_,
       "\"consumer_transform_write\"", consumer_transform_write_,
       "\"plugin_execute_read\"", plugin_execute_read_,
       "\"plugin_execute_write\"", plugin_execute_write_,

       "\"plugin_compress_read\"", plugin_compress_read_,
       "\"plugin_compress_increase\"", plugin_compress_increase_,
       "\"plugin_compress_write\"", plugin_compress_write_,

       "\"plugin_prepare_read\"", plugin_prepare_read_,
       "\"plugin_prepare_write\"", plugin_prepare_write_,

       "\"producer_transform_read\"", producer_transform_read_,
       "\"consumer_observer_write\"", consumer_observer_write_

      );
  J_OBJ_END();

  return pos;
}

int ObProtocolDiagnosis::record_recv_compressed_mysql(const ObCompressedMysqlPacketRecord &compressed_mysql_rec) {
  int ret = OB_SUCCESS;
  ObPacketRecord &rec = records_[cur_idx_];
  rec.compressed_mysql_rec_ = compressed_mysql_rec;
  rec.status_ = OB_PACKET_RECORD_ACTION_RESP;
  rec.type_ = OB_PACKET_RECORD_TYPE_COMPRESSED;
  rec.used_ = OB_PACKET_RECORD_USED;
  cur_idx_++;
  cur_idx_ = cur_idx_ % MAX_PACKET_RECORDS;
  return ret;
}

int ObProtocolDiagnosis::record_recv_mysql(const ObMysqlPacketRecord &mysql_rec) {
  int ret = OB_SUCCESS;
  if (OB_PACKET_FOLD_TYPE_ROW == mysql_rec.fold_type_||
      OB_PACKET_FOLD_TYPE_COL_DEF == mysql_rec.fold_type_) {
    int64_t last_idx = (cur_idx_ + MAX_PACKET_RECORDS - 1) % MAX_PACKET_RECORDS;
    ObPacketRecord &last_rec = records_[last_idx];
    if (last_rec.type_ == OB_PACKET_RECORD_TYPE_MYSQL &&
        last_rec.status_ == OB_PACKET_RECORD_ACTION_RESP &&
        last_rec.mysql_rec_.fold_type_ == mysql_rec.fold_type_) {
      char *tmp = NULL;
      int64_t tmp_pos;
      int32_t v = 0;
      ObMySQLUtil::get_int3(tmp = (char*) last_rec.mysql_rec_.cnt_, v);
      ObMySQLUtil::store_int3((char*) last_rec.mysql_rec_.cnt_, 3, ++v, tmp_pos = 0);
    } else {
      ObPacketRecord &rec = records_[cur_idx_];
      rec.status_ = OB_PACKET_RECORD_ACTION_RESP;
      rec.type_ = OB_PACKET_RECORD_TYPE_MYSQL;
      rec.mysql_rec_ = mysql_rec;
      rec.used_ = OB_PACKET_RECORD_USED;
      int64_t tmp_pos;
      ObMySQLUtil::store_int3((char*) rec.mysql_rec_.cnt_, 3, 1, tmp_pos = 0);
      cur_idx_++;
      cur_idx_ = cur_idx_ % MAX_PACKET_RECORDS;
    }
  } else {
    ObPacketRecord &rec = records_[cur_idx_];
    rec.mysql_rec_ = mysql_rec;
    rec.status_ = OB_PACKET_RECORD_ACTION_RESP;
    rec.type_ = OB_PACKET_RECORD_TYPE_MYSQL;
    rec.used_ = OB_PACKET_RECORD_USED;
    cur_idx_++;
    cur_idx_ = cur_idx_ % MAX_PACKET_RECORDS;
  }
  return ret;
}

int ObProtocolDiagnosis::record_recv_ob20(const Ob20PacketRecord &ob20_rec) {
  int ret = OB_SUCCESS;
  ObPacketRecord &rec = records_[cur_idx_];
  rec.ob20_rec_ = ob20_rec;
  rec.status_ = OB_PACKET_RECORD_ACTION_RESP;
  rec.type_ = OB_PACKET_RECORD_TYPE_OB20;
  rec.used_ = OB_PACKET_RECORD_USED;
  cur_idx_++;
  cur_idx_ = cur_idx_ % MAX_PACKET_RECORDS;
  return ret;
}

int ObProtocolDiagnosis::record_send_ob20(const Ob20PacketRecord &ob20) {
  int ret = OB_SUCCESS;
  ObPacketRecord &rec = records_[cur_idx_];
  rec.ob20_rec_ = ob20;
  rec.status_ = OB_PACKET_RECORD_ACTION_REQ;
  rec.type_ = OB_PACKET_RECORD_TYPE_OB20;
  rec.used_ = OB_PACKET_RECORD_USED;
  cur_idx_++;
  cur_idx_ = cur_idx_ % MAX_PACKET_RECORDS;
  return ret;
}

int ObProtocolDiagnosis::record_send_mysql(const ObMysqlPacketRecord &mysql_rec) {
  int ret = OB_SUCCESS;
  ObPacketRecord &rec = records_[cur_idx_];
  rec.mysql_rec_ = mysql_rec;
  rec.status_ = OB_PACKET_RECORD_ACTION_REQ;
  rec.type_ = OB_PACKET_RECORD_TYPE_MYSQL;
  rec.used_ = OB_PACKET_RECORD_USED;
  cur_idx_++;
  cur_idx_ = cur_idx_ % MAX_PACKET_RECORDS;
  return ret;
}

int ObProtocolDiagnosis::record_send_mysql(event::ObIOBufferReader &mysql_buf_reader, const int64_t buf_len) {
  bool finished = false;
  int64_t tmp_len = 0;
  return mysql_req_analyzer_.is_request_finished(
            mysql_buf_reader, finished, sql_cmd_, buf_len, tmp_len, this);
}

int ObProtocolDiagnosis::record_send_mysql(event::ObIOBufferReader &mysql_buf_reader) {
  bool finished = false;
  int64_t tmp_len = 0;
  return mysql_req_analyzer_.is_request_finished(
            mysql_buf_reader, finished, sql_cmd_, mysql_buf_reader.read_avail(), tmp_len, this);
}

int ObProtocolDiagnosis::record_send_compressed_mysql(const ObCompressedMysqlPacketRecord &compressed_mysql_rec) {
  int ret = OB_SUCCESS;
  ObPacketRecord &rec = records_[cur_idx_];
  rec.compressed_mysql_rec_ = compressed_mysql_rec;
  rec.status_ = OB_PACKET_RECORD_ACTION_REQ;
  rec.type_ = OB_PACKET_RECORD_TYPE_COMPRESSED;
  rec.used_ = OB_PACKET_RECORD_USED;
  cur_idx_++;
  cur_idx_ = cur_idx_ % MAX_PACKET_RECORDS;
  return ret;
}

void ObProtocolDiagnosis::reuse_req_analyzer() {
  mysql_req_analyzer_.reuse();
}

void ObProtocolDiagnosis::reuse_forward_flow() {
  reuse_req_forward_flow();
  reuse_resp_forward_flow();
  reset_extra_ok_exists();
}

void ObProtocolDiagnosis::reuse_req_forward_flow() {
  req_forward_ctrl_flow_.reuse();
  req_forward_data_flow_.reuse();
}

void ObProtocolDiagnosis::reuse_resp_forward_flow() {
  resp_forward_ctrl_flow_.reuse();
  resp_forward_data_flow_.reuse();
}

bool ObProtocolDiagnosis::is_req_forward_flow_exist(ObReqForwardCtrlFlow flow) const {
  bool bret = false;
  for (int i = 0; i < req_forward_ctrl_flow_.count(); i++) {
    if (req_forward_ctrl_flow_[i] == flow) {
      bret = true;
      break;
    }
  }
  return bret;
}

bool ObProtocolDiagnosis::is_resp_forward_flow_exist(ObRespForwardCtrlFlow flow) const {
  bool bret = false;
  for (int i = 0; i < resp_forward_ctrl_flow_.count(); i++) {
    if (resp_forward_ctrl_flow_[i] == flow) {
      bret = true;
      break;
    }
  }
  return bret;
}


bool ObProtocolDiagnosis::is_req_tunnel_finish() const {
  bool bret = false;
  if (is_req_forward_flow_exist(ObReqForwardCtrlFlow::SM_WRITE)
      && !is_req_forward_flow_exist(ObReqForwardCtrlFlow::PRODUCER_CLIENT_READ_FINISH)) {
    bret = true;
  } else if (!is_req_forward_flow_exist(ObReqForwardCtrlFlow::CONSUMER_OBSERVER_WRITE_FINISH)) {
    bret = false;
  } else {
    bret = true;
  }
  return bret;
}

bool ObProtocolDiagnosis::is_resp_tunnel_finish() const {
  bool bret = false;
  if (is_resp_forward_flow_exist(ObRespForwardCtrlFlow::PRODUCER_OBSERVER_READ_FINISH)
      && is_resp_forward_flow_exist(ObRespForwardCtrlFlow::CONSUMER_CLIENT_WRITE_FINISH)) {
    bret = true;
  }
  return bret;
}

DEFINE_IS_PLUGIN_FINISH(req, prepare,
  ObReqForwardCtrlFlow::PLUGIN_PREPARE_WORK,
  ObReqForwardCtrlFlow::PLUGIN_PREPARE_FINISH);

DEFINE_IS_PLUGIN_FINISH(req, execute,
  ObReqForwardCtrlFlow::PLUGIN_EXECUTE_WORK,
  ObReqForwardCtrlFlow::PLUGIN_EXECUTE_FINISH);

DEFINE_IS_PLUGIN_FINISH(req, compress,
  ObReqForwardCtrlFlow::PLUGIN_COMPRESS_WORK,
  ObReqForwardCtrlFlow::PLUGIN_COMPRESS_FINISH);

DEFINE_IS_PLUGIN_FINISH(resp, cursor,
  ObRespForwardCtrlFlow::PLUGIN_CURSOR_WORK,
  ObRespForwardCtrlFlow::PLUGIN_CURSOR_FINISH);

DEFINE_IS_PLUGIN_FINISH(resp, prepare,
  ObRespForwardCtrlFlow::PLUGIN_PREPARE_WORK,
  ObRespForwardCtrlFlow::PLUGIN_PREPARE_FINISH);

DEFINE_IS_PLUGIN_FINISH(resp, prepare_execute,
  ObRespForwardCtrlFlow::PLUGIN_PREPARE_EXECUTE_WORK,
  ObRespForwardCtrlFlow::PLUGIN_PREPARE_EXECUTE_FINISH);

DEFINE_IS_PLUGIN_FINISH(resp, decompress,
  ObRespForwardCtrlFlow::PLUGIN_DECOMPRESS_WORK,
  ObRespForwardCtrlFlow::PLUGIN_DECOMPRESS_FINISH);


int64_t ObProtocolDiagnosis::get_req_sm_read() const {
  int64_t ret = 0;
  int count = req_forward_data_flow_.sm_read_.count();
  if (count > 0) {
    ret = req_forward_data_flow_.sm_read_[count - 1];
  }

  return ret;
}

int64_t ObProtocolDiagnosis::get_resp_sm_read() const {
  int64_t ret = 0;
  int count = resp_forward_data_flow_.sm_read_.count();
  if (count > 0) {
    ret = resp_forward_data_flow_.sm_read_[count - 1];
  }

  return ret;
}

// 因为要处理 push_back 返回值, 所以封装成函数
void ObProtocolDiagnosis::record_resp_forward_ctrl_flow(ObRespForwardCtrlFlow f) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(resp_forward_ctrl_flow_.push_back(f))) {
    PROTOCOL_FORWARD_LOG(EDIAG, "fail to push back resp forward ctrl flow", K(ret), K(f));
  }
}

// 因为要处理 push_back 返回值, 所以封装成函数
void ObProtocolDiagnosis::record_req_forward_ctrl_flow(ObReqForwardCtrlFlow f) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(req_forward_ctrl_flow_.push_back(f))) {
    PROTOCOL_FORWARD_LOG(EDIAG, "fail to push back req forward ctrl flow", K(ret), K(f));
  }
}

bool ObProtocolDiagnosis::diagnose_extra_ok_trim() {
  bool bug_diagnosed = false;
  if (active_diagnosis_.is_extra_ok_exists_) {
    if (!is_resp_forward_flow_exist(ObRespForwardCtrlFlow::SM_TRIM_EXTRA_OK)
        && !(is_resp_forward_flow_exist(ObRespForwardCtrlFlow::PLUGIN_DECOMPRESS_TRIM_EXTRA_OK))) {
      bug_diagnosed = true;
      PROTOCOL_FORWARD_LOG(EDIAG, "potential bug diagnosed! extra ok packet not be trimmed, please check obproxy_diagnosis.log to troubleshoot",
                                  "cmd", ObProxyParserUtils::get_sql_cmd_name(sql_cmd_));
    }
  }

  return bug_diagnosed;
}

int64_t ObProtocolDiagnosis::diagnose_request_forward(char *buf, const int64_t buf_len) const {
  int64_t pos = 0;
  auto& req_data_flow = req_forward_data_flow_;
  if (!is_req_forward_flow_exist(ObReqForwardCtrlFlow::SM_READ)) {
    BUF_PRINTF("[ok] no request data read from net");
  } else if (sql_cmd_ == OB_MYSQL_COM_HANDSHAKE) {
    BUF_PRINTF("[ok] client connect proxy with tcp");
  } else if (is_req_forward_flow_exist(ObReqForwardCtrlFlow::TUNNEL_INIT)) {
    if (!is_req_tunnel_finish()) {
      if (!is_req_plugin_prepare_finish()) {
        BUF_PRINTF("[error] ObMysqlRequestPrepareTransformPlugin not completed, plugin read %ld bytes but write %ld bytes",
                    req_data_flow.plugin_prepare_read_, req_data_flow.plugin_compress_write_);
      } else if (!is_req_plugin_execute_finish()) {
        BUF_PRINTF("[error] ObMysqlRequestExecuteTransformPlugin not completed, plugin read %ld bytes but write %ld bytes",
                    req_data_flow.plugin_execute_read_, req_data_flow.plugin_execute_write_);
      } else if (!is_req_plugin_compress_finish()) {
        BUF_PRINTF("[error] ObMysqlRequestCompressTransformPlugin not completed, plugin read %ld bytes but write %ld bytes",
                    req_data_flow.plugin_compress_read_, req_data_flow.plugin_compress_write_);
      } else {
        BUF_PRINTF("[error] tunnel not completed, tunnel read %ld bytes but write %ld bytes",
                    req_data_flow.producer_client_read_, req_forward_data_flow_.consumer_observer_write_);
      }
    } else {
      BUF_PRINTF("[ok] tunnel completed the request streaming forwarding");
    }
  } else if (!is_req_forward_flow_exist(ObReqForwardCtrlFlow::SM_WRITE)) {
    BUF_PRINTF("[error] request not fowarded to server, sm read %ld bytes but sm write %ld bytes",
               get_req_sm_read(), req_data_flow.sm_write_);
  } else {
    BUF_PRINTF("[ok] sm completed the request forwarding");
  }

  return pos;
}

int64_t ObProtocolDiagnosis::diagnose_response_forward(char *buf, const int64_t buf_len) const {
  int64_t pos = 0;
  auto& resp_data_flow = resp_forward_data_flow_;
  if (sql_cmd_ == OB_MYSQL_COM_HANDSHAKE) {
    if (resp_data_flow.consumer_client_write_ == 0
        || !is_resp_forward_flow_exist(ObRespForwardCtrlFlow::CONSUMER_CLIENT_WRITE_FINISH)) {
      BUF_PRINTF("[error] proxy not respond handshake request to client");
    } else {
      BUF_PRINTF("[ok] proxy respond client with handshake request");
    }
  } else if (!is_resp_forward_flow_exist(ObRespForwardCtrlFlow::SM_READ)) {
    BUF_PRINTF("[ok] no response data read from net");
  } else if (!is_resp_forward_flow_exist(ObRespForwardCtrlFlow::TUNNEL_INIT)) {
    BUF_PRINTF("[error] sm is still reading from net, sm read %ld bytes", get_resp_sm_read());
  } else if (is_resp_forward_flow_exist(ObRespForwardCtrlFlow::PLUGIN_DECOMPRESS_WORK)
             && (is_resp_forward_flow_exist(ObRespForwardCtrlFlow::SM_TRIM_EXTRA_OK)
                 || is_resp_forward_flow_exist(ObRespForwardCtrlFlow::SM_REWRITE_LAST_OK))) {
    BUF_PRINTF("[error] ObMysqlResponseCompressTransformPlugin need trim/rewrite the extra/last ok, sm trim extra ok %ld bytes/sm rewrite last ok delta %ld bytes",
               resp_data_flow.sm_trim_ok_, resp_data_flow.sm_rewrite_ok_delta_);
  } else if (!is_resp_tunnel_finish()) {
    if (!is_resp_plugin_decompress_finish()) {
      BUF_PRINTF("[error] ObMysqlResponseCompressTransformPlugin not completed, plugin read %ld bytes but write %ld bytes",
                 resp_data_flow.plugin_decompress_read_, resp_data_flow.plugin_decompress_write_);
    } else if (!is_resp_plugin_cursor_finish()) {
      BUF_PRINTF("[error] ObMysqlResponseCursorTransformPlugin not completed, plugin read %ld bytes but write %ld bytes",
                 resp_data_flow.plugin_cursor_read_, resp_data_flow.plugin_cursor_write_);
    } else if (!is_resp_plugin_prepare_finish()) {
      BUF_PRINTF("[error] ObMysqlResponsePrepareTransformPlugin not completed, plugin read %ld bytes but write %ld bytes",
                 resp_data_flow.plugin_prepare_read_, resp_data_flow.plugin_prepare_write_);
    } else if (!is_resp_plugin_prepare_execute_finish()) {
      BUF_PRINTF("[error] ObMysqlResponsePrepareExecuteTransformPlugin not completed, plugin read %ld bytes, but write %ld bytes",
                 resp_data_flow.plugin_prepare_execute_read_, resp_data_flow.plugin_prepare_execute_write_);
    } else {
      BUF_PRINTF("[error] tunnel not completed, tunnel read %ld bytes but write %ld bytes",
                 resp_data_flow.producer_observer_read_, resp_data_flow.consumer_client_write_);
    }
  } else if (is_resp_forward_flow_exist(ObRespForwardCtrlFlow::SM_TRIM_EXTRA_OK)
             && is_resp_forward_flow_exist(ObRespForwardCtrlFlow::PLUGIN_DECOMPRESS_TRIM_EXTRA_OK)){
    BUF_PRINTF("[error] double trim the extra ok, sm trim extra ok %ld bytes and plugin trim extra ok %ld bytes",
               resp_data_flow.sm_trim_ok_, resp_data_flow.plugin_trim_ok_);
  } else if (is_resp_forward_flow_exist(ObRespForwardCtrlFlow::SM_REWRITE_LAST_OK)
             && is_resp_forward_flow_exist(ObRespForwardCtrlFlow::PLUGIN_DECOMPRESS_REWRITE_LAST_OK)) {
    BUF_PRINTF("[error] double rewrite the last ok, sm rewrite last ok %ld bytes and plugin rewrite last ok delta %ld bytes",
               resp_data_flow.sm_rewrite_ok_delta_, resp_data_flow.plugin_rewrite_ok_delta_);
  } else {
    BUF_PRINTF("[ok] tunnel completed the response forwarding");
  }

  return pos;
}

int64_t ObProtocolDiagnosis::to_string(char *buf, const int64_t buf_len) const {
  int64_t pos = 0;
  int32_t start_idx = cur_idx_;
  int32_t cnt = 0;
  J_OBJ_START();
  BUF_PRINTF("\"records\"");
  J_COLON();
  J_ARRAY_START();
  while (cnt < MAX_PACKET_RECORDS) {
    const ObPacketRecord rec = records_[start_idx];
    if(rec.used_ == OB_PACKET_RECORD_UNUSED) {
      // record not be used
    } else {
      BUF_PRINTO(rec);
      start_idx = (start_idx + 1) % MAX_PACKET_RECORDS;
      if (cnt + 1 < MAX_PACKET_RECORDS) {
        if (records_[start_idx].used_ != OB_PACKET_RECORD_UNUSED) {
          J_COMMA();
        }
      }
    }
    cnt++;
  }
  J_ARRAY_END();
  J_COMMA();

  BUF_PRINTF("\"control_flow\"");
  J_COLON();
  J_OBJ_START();
  BUF_PRINTF("\"request_forward\"");
  J_COLON();
  J_ARRAY_START();
  for (int i = 0; i < req_forward_ctrl_flow_.count(); i++) {
    BUF_PRINTF("\"%s\"", get_req_forward_ctrl_flow_name(req_forward_ctrl_flow_[i]));
    if (i + 1 < req_forward_ctrl_flow_.count()) {
      J_COMMA();
    }
  }
  J_ARRAY_END();
  J_COMMA();
  BUF_PRINTF("\"response_forward\"");
  J_COLON();
  J_ARRAY_START();
  for (int i = 0; i < resp_forward_ctrl_flow_.count(); i++) {
    BUF_PRINTF("\"%s\"", get_resp_forward_ctrl_flow_name(resp_forward_ctrl_flow_[i]));
    if (i + 1 < resp_forward_ctrl_flow_.count()) {
      J_COMMA();
    }
  }
  J_ARRAY_END();
  J_OBJ_END();
  J_COMMA();

  BUF_PRINTF("\"data_flow\"");
  J_COLON();
  J_OBJ_START();
  BUF_PRINTF("\"request_forward\"");
  J_COLON();
  BUF_PRINTO(req_forward_data_flow_);
  J_COMMA();
  BUF_PRINTF("\"response_foward\"");
  J_COLON();
  BUF_PRINTO(resp_forward_data_flow_);
  J_OBJ_END();
  J_COMMA();


  BUF_PRINTF("\"diagnosis\"");
  J_COLON();
  J_OBJ_START();
  BUF_PRINTF("\"request_foward\"");
  J_COLON();
  BUF_PRINTF("\"");
  pos += diagnose_request_forward(buf + pos, buf_len - pos);
  BUF_PRINTF("\"");
  J_COMMA();
  BUF_PRINTF("\"response_forward\"");
  J_COLON();
  BUF_PRINTF("\"");
  pos += diagnose_response_forward(buf + pos, buf_len - pos);
  BUF_PRINTF("\"");
  J_OBJ_END();

  J_OBJ_END();
  return pos;
}

} // end of proxy
} // end of obproxy
} // end of oceanbase