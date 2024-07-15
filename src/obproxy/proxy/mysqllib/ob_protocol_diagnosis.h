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

#ifndef OBPROXY_PROTOCOL_DIAGNOSIS_H
#define OBPROXY_PROTOCOL_DIAGNOSIS_H
#include "lib/ob_define.h"
#include "lib/ptr/ob_ptr.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/oblog/ob_log.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "proxy/mysqllib/ob_2_0_protocol_struct.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "proxy/mysqllib/ob_mysql_request_analyzer.h"

#define MAX_PACKET_RECORDS 20
#define PROTOCOL_DIAGNOSIS_COMPRESSED_MYSQL(protocol_diagnosis, action, compressed_len, seq, uncompressed_len)\
    ObCompressedMysqlPacketRecord compressed_mysql_rec;  \
    int64_t tmp_pos = 0;  \
    ObMySQLUtil::store_int3((char*) compressed_mysql_rec.compressed_len_, 3, compressed_len, tmp_pos = 0);  \
    ObMySQLUtil::store_int3((char*) compressed_mysql_rec.uncompressed_len_, 3, uncompressed_len, tmp_pos = 0);  \
    compressed_mysql_rec.compressed_seq_ = seq;  \
    protocol_diagnosis->record_##action##_compressed_mysql(compressed_mysql_rec);

#define PROTOCOL_DIAGNOSIS_MULTI_MYSQL(protocol_diagnosis, action, reader, buf_len) \
    protocol_diagnosis->record_##action##_mysql(reader, buf_len);

#define PROTOCOL_DIAGNOSIS_SINGLE_MYSQL_WITH_FOLD(protocol_diagnosis, action, len, seq, cmd, fold) \
    ObMysqlPacketRecord mysql_rec;  \
    int64_t tmp_pos = 0;  \
    ObMySQLUtil::store_int3((char*) mysql_rec.len_, 3, len, tmp_pos);  \
    mysql_rec.seq_ = seq; \
    mysql_rec.cmd_ = cmd;  \
    mysql_rec.fold_type_ = fold; \
    protocol_diagnosis->record_##action##_mysql(mysql_rec);

#define PROTOCOL_DIAGNOSIS_SINGLE_MYSQL(protocol_diagnosis, action, len, seq, cmd) \
  PROTOCOL_DIAGNOSIS_SINGLE_MYSQL_WITH_FOLD(protocol_diagnosis, action, len, seq, cmd, OB_PACKET_FOLD_TYPE_NONE)

#define PROTOCOL_DIAGNOSIS_OCEANBASE20(protocol_diagnosis, action, \
                                       compressed_len, compressed_seq, uncompressed_len, \
                                       payload_len, connection_id, flag, pkt_seq, request_id) \
    Ob20PacketRecord ob20_rec;  \
    ob20_rec.compressed_seq_ = compressed_seq;  \
    ob20_rec.payload_len_ = payload_len;  \
    ob20_rec.connection_id_ = connection_id;  \
    ob20_rec.flag_ = flag;  \
    ob20_rec.pkt_seq_ = pkt_seq;  \
    int64_t tmp_pos = 0;  \
    ObMySQLUtil::store_int3((char*) ob20_rec.compressed_len_, 3, compressed_len, tmp_pos = 0);  \
    ObMySQLUtil::store_int3((char*) ob20_rec.uncompressed_len_, 3, uncompressed_len, tmp_pos = 0);  \
    ObMySQLUtil::store_int3((char*) ob20_rec.request_id_, 3, request_id, tmp_pos = 0);  \
    protocol_diagnosis->record_##action##_ob20(ob20_rec);


#define PROTOCOL_DIAGNOSIS(type, action, protocol_diagnosis, args...) \
  if (OB_UNLIKELY(protocol_diagnosis != NULL)) {  \
    PROTOCOL_DIAGNOSIS_##type(protocol_diagnosis, action, args);  \
  }

#define TMP_DISABLE_PROTOCOL_DIAGNOSIS(protocol_diagnosis_ptr)  \
  ObProtocolDiagnosis *tmp = protocol_diagnosis_ptr;  \
  protocol_diagnosis_ptr = NULL;

#define REENABLE_PROTOCOL_DIAGNOSIS(protocol_diagnosis_ptr) \
  protocol_diagnosis_ptr = tmp; \
  tmp = NULL;


namespace oceanbase
{
using namespace common;
namespace obproxy
{
namespace proxy
{
class ObRespPacketAnalyzeResult;
enum ObPacketFoldType {
  OB_PACKET_FOLD_TYPE_NONE,
  OB_PACKET_FOLD_TYPE_ROW,
  OB_PACKET_FOLD_TYPE_COL_DEF,
};
enum ObPacketRecordType {
  OB_PACKET_RECORD_TYPE_MYSQL = 0,
  OB_PACKET_RECORD_TYPE_COMPRESSED,
  OB_PACKET_RECORD_TYPE_OB20
};

enum ObPacketRecordUsed {
  OB_PACKET_RECORD_UNUSED = 0,
  OB_PACKET_RECORD_USED = 1,
};

enum ObPacketRecordAction {
  OB_PACKET_RECORD_ACTION_SEND,
  OB_PACKET_RECORD_ACTION_RECV,
};

const char *get_record_status_str(ObPacketRecordAction status) {
  const char* ret = "";
  switch (status) {
    case OB_PACKET_RECORD_ACTION_RECV:
      ret = "recv(+)";
      break;

    case OB_PACKET_RECORD_ACTION_SEND:
      ret = "send(-)";
      break;
  }
  return ret;
}

const char *get_fold_type_str(ObPacketFoldType type) {
  const char *ret = "";
  switch (type) {
    case OB_PACKET_FOLD_TYPE_NONE:
      ret = "non_fold";
      break;

    case OB_PACKET_FOLD_TYPE_ROW:
      ret = "row";
      break;

    case OB_PACKET_FOLD_TYPE_COL_DEF:
      ret = "col_def";
      break;
  }
  return ret;
}
struct ObMysqlPacketRecord // 5 bytes
{
  union
  {
    uint8_t len_[3]; // normal mysql packet len
    uint8_t cnt_[3]; // row or field use it
  };
  uint8_t seq_;    // first row/field's sequence number
  union
  {
    uint8_t cmd_;		   // for request
    uint8_t pkt_type_; // for response
  };

  ObPacketFoldType fold_type_;

  // fold the consecutive col_def and row into one record
  static ObPacketFoldType get_fold_type(const ObRespPacketAnalyzeResult &result);
};
struct ObCompressedMysqlPacketRecord	// 7 bytes
{
  uint8_t compressed_len_[3];
  uint8_t compressed_seq_;
  uint8_t uncompressed_len_[3];
};
struct Ob20PacketRecord
{
  uint8_t compressed_len_[3];
  uint8_t compressed_seq_;
  uint8_t uncompressed_len_[3];
  uint32_t payload_len_;
  Ob20ProtocolFlags flag_;
  uint32_t connection_id_;
  uint8_t request_id_[3];
  uint8_t pkt_seq_;
};
struct ObPacketRecord
{
public:
  ObPacketRecord() { MEMSET(this, 0, sizeof(ObPacketRecord)); }
  int64_t to_string(char *buf, const int64_t buf_len) const;

  union {
    uint8_t flag_;
    struct {
      uint8_t used_:    1;    // whether is valid record
      uint8_t status_:  1;    // send or receive
      uint8_t type_:    2;    // mysql or compressed mysql or ob20 or compressed ob20
    };
  };

  union
  {
    Ob20PacketRecord               ob20_rec_;
    ObMysqlPacketRecord            mysql_rec_;
    ObCompressedMysqlPacketRecord  compressed_mysql_rec_;
  };
};

class ObProtocolDiagnosis : public ObSharedRefCount
{
public:
  ObProtocolDiagnosis()
    : cur_idx_(0), mysql_req_analyzer_(), sql_cmd_(obmysql::OB_MYSQL_COM_SLEEP)
  {
    MEMSET(records_, 0, sizeof(records_));
  }
  static inline int alloc(ObProtocolDiagnosis *&protocol_diagnosis)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(protocol_diagnosis = op_alloc(ObProtocolDiagnosis))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      protocol_diagnosis->inc_ref();
    }
    return ret;
  }
  virtual void free() { op_free(this); }
  int record_send_ob20(const Ob20PacketRecord &ob20_rec);
  int record_send_compressed_mysql(const ObCompressedMysqlPacketRecord &compressed_mysql_rec);
  int record_send_mysql(const ObMysqlPacketRecord &mysql_rec);
  int record_send_mysql(event::ObIOBufferReader &mysql_buf_reader);
  int record_send_mysql(event::ObIOBufferReader &mysql_buf_reader, const int64_t buf_len);
  int record_recv_ob20(const Ob20PacketRecord &ob20_rec);
  int record_recv_mysql(const ObMysqlPacketRecord &mysql_rec);
  int record_recv_compressed_mysql(const ObCompressedMysqlPacketRecord &compressed_mysql_rec);
  void reuse_req_analyzer();
  inline void set_sql_cmd(obmysql::ObMySQLCmd cmd) { sql_cmd_ = cmd; }
  inline const obmysql::ObMySQLCmd get_sql_cmd() const { return sql_cmd_; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  ObPacketRecord records_[MAX_PACKET_RECORDS];
  uint8_t cur_idx_;
  ObMysqlRequestAnalyzer mysql_req_analyzer_; // to analyze mysql packet from block reader
  obmysql::ObMySQLCmd sql_cmd_;
};

} // end of proxy
} // end of obproxy
} // end of oceanbase

#endif
