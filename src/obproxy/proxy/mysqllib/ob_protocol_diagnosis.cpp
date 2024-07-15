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

ObPacketFoldType ObMysqlPacketRecord::get_fold_type(const ObRespPacketAnalyzeResult &result) {
  ObPacketFoldType fold_type = OB_PACKET_FOLD_TYPE_NONE;
  if (OB_MYSQL_COM_QUERY == result.get_cmd() || OB_MYSQL_COM_STMT_EXECUTE == result.get_cmd()) {
    if (1 < result.get_all_pkt_cnt() &&
        0 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE)) {
      fold_type = OB_PACKET_FOLD_TYPE_COL_DEF;
    } else if (1 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE)) {
      fold_type = OB_PACKET_FOLD_TYPE_ROW;
    } else {
      fold_type = OB_PACKET_FOLD_TYPE_NONE;
    }
  } else if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE == result.get_cmd()) {
    if (1 < result.get_all_pkt_cnt() &&
        (0 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE) ||
         1 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE))) {
      fold_type = OB_PACKET_FOLD_TYPE_COL_DEF;
    } else if (2 == result.get_pkt_cnt(EOF_PACKET_ENDING_TYPE)) {
      fold_type = OB_PACKET_FOLD_TYPE_ROW;
    } else {
      fold_type = OB_PACKET_FOLD_TYPE_NONE;
    }
  } else if (OB_MYSQL_COM_STMT_FETCH == result.get_cmd()) {
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

  bool is_send = (status_ == OB_PACKET_RECORD_ACTION_SEND);
  char *tmp = NULL;
  uint32_t compressed_len = 0;
  uint32_t uncompressed_len = 0;
  switch (type_) {
    case OB_PACKET_RECORD_TYPE_MYSQL:
      BUF_PRINTF("mysql");
      J_OBJ_START();
      if (mysql_rec_.fold_type_ == OB_PACKET_FOLD_TYPE_NONE) {
        uint32_t len = 0;
        ObMySQLUtil::get_uint3(tmp = (char*) mysql_rec_.len_, len);
        J_KV("len", len);
        J_COMMA();
        J_KV("seq", mysql_rec_.seq_);
        J_COMMA();
        if (is_send) {
          J_KV("cmd", mysql_rec_.cmd_);
        } else {
          J_KV("type", mysql_rec_.pkt_type_);
        }
      } else {
        uint32_t cnt = 0;
        ObMySQLUtil::get_uint3(tmp = (char*) mysql_rec_.cnt_, cnt);
        J_KV("cnt(len)", cnt, "seq", mysql_rec_.seq_, "type", get_fold_type_str(mysql_rec_.fold_type_));
      }
      J_OBJ_END();
      break;

    case OB_PACKET_RECORD_TYPE_COMPRESSED:
      BUF_PRINTF("compressed mysql");
      J_OBJ_START();
      ObMySQLUtil::get_uint3(tmp = (char*) compressed_mysql_rec_.compressed_len_, compressed_len);
      ObMySQLUtil::get_uint3(tmp = (char*) compressed_mysql_rec_.uncompressed_len_, uncompressed_len);
      J_KV(K(compressed_len), "seq", compressed_mysql_rec_.compressed_seq_, K(uncompressed_len));
      J_OBJ_END();
      break;

    case OB_PACKET_RECORD_TYPE_OB20:
      BUF_PRINTF("oceanbase2.0");
      J_OBJ_START();
      ObMySQLUtil::get_uint3(tmp = (char*) ob20_rec_.compressed_len_, compressed_len);
      ObMySQLUtil::get_uint3(tmp = (char*) ob20_rec_.uncompressed_len_, uncompressed_len);
      uint32_t req_id = 0;
      ObMySQLUtil::get_uint3(tmp = (char*) ob20_rec_.request_id_, req_id);
      J_KV(K(compressed_len),
           "seq", ob20_rec_.compressed_seq_,
           K(uncompressed_len),
           "payload_len", ob20_rec_.payload_len_,
           "conn_id", ob20_rec_.connection_id_,
           K(req_id),
           "pkt_seq", ob20_rec_.pkt_seq_,
           "flag", ob20_rec_.flag_.flags_);
      J_OBJ_END();
      break;
  }

  return pos;
}

int ObProtocolDiagnosis::record_recv_compressed_mysql(const ObCompressedMysqlPacketRecord &compressed_mysql_rec) {
  int ret = OB_SUCCESS;
  ObPacketRecord &rec = records_[cur_idx_];
  rec.compressed_mysql_rec_ = compressed_mysql_rec;
  rec.status_ = OB_PACKET_RECORD_ACTION_RECV;
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
        last_rec.status_ == OB_PACKET_RECORD_ACTION_RECV &&
        last_rec.mysql_rec_.fold_type_ == mysql_rec.fold_type_) {
      char *tmp = NULL;
      int64_t tmp_pos;
      int32_t v = 0;
      ObMySQLUtil::get_int3(tmp = (char*) last_rec.mysql_rec_.cnt_, v);
      ObMySQLUtil::store_int3((char*) last_rec.mysql_rec_.cnt_, 3, ++v, tmp_pos = 0);
    } else {
      ObPacketRecord &rec = records_[cur_idx_];
      rec.status_ = OB_PACKET_RECORD_ACTION_RECV;
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
    rec.status_ = OB_PACKET_RECORD_ACTION_RECV;
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
  rec.status_ = OB_PACKET_RECORD_ACTION_RECV;
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
  rec.status_ = OB_PACKET_RECORD_ACTION_SEND;
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
  rec.status_ = OB_PACKET_RECORD_ACTION_SEND;
  rec.type_ = OB_PACKET_RECORD_TYPE_MYSQL;
  rec.used_ = OB_PACKET_RECORD_USED;
  cur_idx_++;
  cur_idx_ = cur_idx_ % MAX_PACKET_RECORDS;
  return ret;
}

int ObProtocolDiagnosis::record_send_mysql(event::ObIOBufferReader &mysql_buf_reader, const int64_t buf_len) {
  bool finished = false;
  return mysql_req_analyzer_.is_request_finished(
            mysql_buf_reader, finished, sql_cmd_, buf_len, this);
}

int ObProtocolDiagnosis::record_send_mysql(event::ObIOBufferReader &mysql_buf_reader) {
  bool finished = false;
  return mysql_req_analyzer_.is_request_finished(
            mysql_buf_reader, finished, sql_cmd_, mysql_buf_reader.read_avail(), this);
}

int ObProtocolDiagnosis::record_send_compressed_mysql(const ObCompressedMysqlPacketRecord &compressed_mysql_rec) {
  int ret = OB_SUCCESS;
  ObPacketRecord &rec = records_[cur_idx_];
  rec.compressed_mysql_rec_ = compressed_mysql_rec;
  rec.status_ = OB_PACKET_RECORD_ACTION_SEND;
  rec.type_ = OB_PACKET_RECORD_TYPE_COMPRESSED;
  rec.used_ = OB_PACKET_RECORD_USED;
  cur_idx_++;
  cur_idx_ = cur_idx_ % MAX_PACKET_RECORDS;
  return ret;
}

void ObProtocolDiagnosis::reuse_req_analyzer() {
  mysql_req_analyzer_.reuse();
}

/*
  format likes
  [00]recv(+) mysql{len:74, seq:0, type:10}
  [01]recv(+) mysql{len:6369, seq:2, type:0}
  [02]send(-) compressed mysql{compressed_len:48, seq:0, uncompressed_len:37}
  [03] └─mysql{len:33, seq:0, cmd:0}
*/
int64_t ObProtocolDiagnosis::to_string(char *buf, const int64_t buf_len) const {
  int64_t pos = 0;
  int32_t start_idx = cur_idx_;
  bool is_payload = false;
  uint8_t status = OB_PACKET_RECORD_ACTION_SEND;
  int32_t cnt = 0;
  while (cnt < MAX_PACKET_RECORDS) {
    const ObPacketRecord rec = records_[start_idx];
    if(rec.used_ == OB_PACKET_RECORD_UNUSED) {
      // record not be used
    } else {
      BUF_PRINTF("#[%02d]", start_idx);
      // oceanbase 2.0/compressed mysql packets
      if (rec.type_ == OB_PACKET_RECORD_TYPE_OB20 ||
          rec.type_ == OB_PACKET_RECORD_TYPE_COMPRESSED) {
        is_payload = true;
        status = rec.status_;
        BUF_PRINTF("%s ", get_record_status_str(static_cast<ObPacketRecordAction>(rec.status_)));
      // mysql packets
      } else {
        if (status != rec.status_) {
          is_payload = false;
          status = rec.status_;
        }

        if (is_payload) {
          BUF_PRINTF(" └─");
        } else {
          BUF_PRINTF("%s ", get_record_status_str(static_cast<ObPacketRecordAction>(rec.status_)));
        }
      }
      pos += rec.to_string(buf + pos, buf_len - pos);
    }
    start_idx = (start_idx + 1) % MAX_PACKET_RECORDS;
    cnt++;
  }
  if (0 == pos) {
    BUF_PRINTF("no protocol records now");
  }
  return pos;
}

} // end of proxy
} // end of obproxy
} // end of oceanbase