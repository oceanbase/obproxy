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

#define DEFINE_IS_PLUGIN_FINISH(req_or_resp, name, start_flow, end_flow) \
bool ObProtocolDiagnosis::is_##req_or_resp##_plugin_##name##_finish() const { \
  bool bret = false;  \
  if (!is_##req_or_resp##_forward_flow_exist(start_flow)) { \
    bret = true;  \
  } else if (!is_##req_or_resp##_forward_flow_exist(end_flow)) { \
    bret = false; \
  } else {  \
    bret = true;  \
  } \
  return  bret; \
}

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
  OB_PACKET_FOLD_TYPE_ROW, // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_row.html
  OB_PACKET_FOLD_TYPE_COL_DEF, // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html
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
  OB_PACKET_RECORD_ACTION_REQ,
  OB_PACKET_RECORD_ACTION_RESP,
};

const char *get_record_type_str(ObPacketRecordType type) {
  const char* ret = "";
  switch (type) {
    case OB_PACKET_RECORD_TYPE_MYSQL:
      ret = "mysql";
      break;

    case OB_PACKET_RECORD_TYPE_COMPRESSED:
      ret = "compressed mysql";
      break;

    case OB_PACKET_RECORD_TYPE_OB20:
      ret = "oceanbase 2.0";
      break;

    default:
      ret = "unknown";
  }
  return ret;
}

const char *get_record_action_str(ObPacketRecordAction action) {
  const char* ret = "";
  switch (action) {
    case OB_PACKET_RECORD_ACTION_RESP:
      ret = "response";
      break;

    case OB_PACKET_RECORD_ACTION_REQ:
      ret = "request";
      break;

    default:
      ret = "unknown";
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
  static ObPacketFoldType get_fold_type(const obmysql::ObMySQLCmd cmd, const ObRespPacketAnalyzeResult &result);
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
  uint32_t extra_info_len_;
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

enum class ObRespForwardCtrlFlow
{
  SM_READ,
  SM_READ_INTERNAL_SYNC_REQ_RESP,
  SM_ANALYZE_DONE,
  SM_ANALYZE_CONT,
  DECOMPRESSED,
  NOT_DECOMPRESSED,
  TUNNEL_INIT,
  PRODUCER_INTERNAL_MSG_FINISH,
  PRODUCER_OBSERVER_READ_FINISH,
  CONSUMER_CLIENT_WRITE_FINISH,
  PRODUCER_TRANSFORM_READ_FINISH,
  CONSUMER_TRANSFORM_WRITE_FINISH,
  PLUGIN_DECOMPRESS_FINISH,
  PLUGIN_DECOMPRESS_WORK,
  PLUGIN_CURSOR_FINISH,
  PLUGIN_CURSOR_WORK,
  PLUGIN_PREPARE_EXECUTE_FINISH,
  PLUGIN_PREPARE_EXECUTE_WORK,
  PLUGIN_PREPARE_FINISH,
  PLUGIN_PREPARE_WORK,
  SM_TRIM_EXTRA_OK,
  SM_REWRITE_LAST_OK,
  PLUGIN_DECOMPRESS_TRIM_EXTRA_OK,
  PLUGIN_DECOMPRESS_REWRITE_LAST_OK,

};

const char* get_resp_forward_ctrl_flow_name(ObRespForwardCtrlFlow type) {
    const char* result = "unknown";

    switch (type) {
        case ObRespForwardCtrlFlow::SM_READ:
            result = "sm_read";
            break;
        case ObRespForwardCtrlFlow::SM_READ_INTERNAL_SYNC_REQ_RESP:
            result = "sm_read_internal_sync_request_resp";
            break;
        case ObRespForwardCtrlFlow::SM_ANALYZE_DONE:
            result = "sm_analyze_done";
            break;
        case ObRespForwardCtrlFlow::SM_ANALYZE_CONT:
            result = "sm_analyze_cont";
            break;
        case ObRespForwardCtrlFlow::DECOMPRESSED:
            result = "decompressed";
            break;
        case ObRespForwardCtrlFlow::NOT_DECOMPRESSED:
            result = "not_decompressed";
            break;
        case ObRespForwardCtrlFlow::TUNNEL_INIT:
            result = "tunnel_init";
            break;
        case ObRespForwardCtrlFlow::PRODUCER_OBSERVER_READ_FINISH:
            result = "producer_observer_read_finish";
            break;
        case ObRespForwardCtrlFlow::PRODUCER_INTERNAL_MSG_FINISH:
            result = "producer_internal_msg_finish";
            break;
        case ObRespForwardCtrlFlow::CONSUMER_CLIENT_WRITE_FINISH:
            result = "consumer_client_write_finish";
            break;
        case ObRespForwardCtrlFlow::PRODUCER_TRANSFORM_READ_FINISH:
            result = "producer_transform_read_finish";
            break;
        case ObRespForwardCtrlFlow::CONSUMER_TRANSFORM_WRITE_FINISH:
            result = "consumer_transform_write_finish";
            break;
        case ObRespForwardCtrlFlow::PLUGIN_DECOMPRESS_FINISH:
            result = "plugin_decompress_finish";
            break;
        case ObRespForwardCtrlFlow::PLUGIN_DECOMPRESS_WORK:
            result = "plugin_decompress_work";
            break;
        case ObRespForwardCtrlFlow::PLUGIN_CURSOR_FINISH:
            result = "plugin_cursor_finish";
            break;
        case ObRespForwardCtrlFlow::PLUGIN_CURSOR_WORK:
            result = "plugin_cursor_work";
            break;
        case ObRespForwardCtrlFlow::PLUGIN_PREPARE_EXECUTE_FINISH:
            result = "plugin_prepare_execute_finish";
            break;
        case ObRespForwardCtrlFlow::PLUGIN_PREPARE_EXECUTE_WORK:
            result = "plugin_prepare_execute_work";
            break;
        case ObRespForwardCtrlFlow::PLUGIN_PREPARE_FINISH:
            result = "plugin_prepare_finish";
            break;
        case ObRespForwardCtrlFlow::PLUGIN_PREPARE_WORK:
            result = "plugin_prepare_work";
            break;
        case ObRespForwardCtrlFlow::SM_TRIM_EXTRA_OK:
            result = "sm_trim_extra_ok";
            break;
        case ObRespForwardCtrlFlow::SM_REWRITE_LAST_OK:
            result = "sm_rewrite_last_ok";
            break;
        case ObRespForwardCtrlFlow::PLUGIN_DECOMPRESS_TRIM_EXTRA_OK:
            result = "plugin_decompress_trim_extra_ok";
            break;
        case ObRespForwardCtrlFlow::PLUGIN_DECOMPRESS_REWRITE_LAST_OK:
            result = "plugin_decompress_rewrite_last_ok";
            break;
        default:
            result = "unknown";
            break;
    }

    return result;
}

enum class ObReqForwardCtrlFlow
{
  SM_READ,
  SM_WRITE,
  SM_WRITE_SYNC_REQ,
  TUNNEL_INIT,
  PRODUCER_CLIENT_READ_FINISH,
  CONSUMER_OBSERVER_WRITE_FINISH,
  PRODUCER_TRANSFORM_READ_FINISH,
  CONSUMER_TRANSFORM_WRITE_FINISH,
  PLUGIN_COMPRESS_FINISH,
  PLUGIN_COMPRESS_WORK,
  PLUGIN_EXECUTE_FINISH,
  PLUGIN_EXECUTE_WORK,
  PLUGIN_PREPARE_FINISH,
  PLUGIN_PREPARE_WORK,
};

const char* get_req_forward_ctrl_flow_name(ObReqForwardCtrlFlow type) {
  const char* result = "unknown";

  switch (type) {
      case ObReqForwardCtrlFlow::SM_READ:
          result = "sm_read";
          break;
      case ObReqForwardCtrlFlow::SM_WRITE:
          result = "sm_write";
          break;
      case ObReqForwardCtrlFlow::SM_WRITE_SYNC_REQ:
          result = "sm_write_sync_req";
          break;
      case ObReqForwardCtrlFlow::TUNNEL_INIT:
          result = "tunnel_init";
          break;
      case ObReqForwardCtrlFlow::PRODUCER_CLIENT_READ_FINISH:
          result = "producer_client_read_finish";
          break;
      case ObReqForwardCtrlFlow::CONSUMER_OBSERVER_WRITE_FINISH:
          result = "consumer_observer_write_finish";
          break;
      case ObReqForwardCtrlFlow::PRODUCER_TRANSFORM_READ_FINISH:
          result = "producer_transform_read_finish";
          break;
      case ObReqForwardCtrlFlow::CONSUMER_TRANSFORM_WRITE_FINISH:
          result = "consumer_transform_write_finish";
          break;
      case ObReqForwardCtrlFlow::PLUGIN_COMPRESS_FINISH:
          result = "plugin_compress_finish";
          break;
      case ObReqForwardCtrlFlow::PLUGIN_COMPRESS_WORK:
          result = "plugin_compress_work";
          break;
      case ObReqForwardCtrlFlow::PLUGIN_EXECUTE_FINISH:
          result = "plugin_execute_finish";
          break;
      case ObReqForwardCtrlFlow::PLUGIN_EXECUTE_WORK:
          result = "plugin_execute_work";
          break;
      case ObReqForwardCtrlFlow::PLUGIN_PREPARE_FINISH:
          result = "plugin_prepare_finish";
          break;
      case ObReqForwardCtrlFlow::PLUGIN_PREPARE_WORK:
          result = "plugin_prepare_work";
          break;
      default:
          result = "unknown";
          break;
  }

  return result;
}

struct ObRespForwardDataFlow
{
  // sm 可能多次从网络读取响应数据
  common::ObSEArray<int64_t, 4>  sm_read_;
  // sm 读取同步语句的响应数据, 这些数据不会被转发给客户端的
  // 所以使用一个值记录上一个同步语句响应报文的数据长度
  // 对应的 ObReqForwardDataFlow::sm_write_sync_resp_
  int64_t sm_read_internal_sync_request_resp_;
  int64_t tunnel_init_;
  int64_t sm_trim_ok_;
  int64_t sm_rewrite_ok_delta_;
  int64_t plugin_trim_ok_;
  int64_t plugin_rewrite_ok_delta_;

  int64_t producer_observer_read_;
  int64_t consumer_transform_write_;
  int64_t producer_transform_read_;
  int64_t consumer_client_write_;

  int64_t plugin_decompress_read_;
  int64_t plugin_decompress_decrease_;
  int64_t plugin_decompress_write_;

  int64_t plugin_cursor_read_;
  int64_t plugin_cursor_write_;

  int64_t plugin_prepare_read_;
  int64_t plugin_prepare_write_;

  int64_t plugin_prepare_execute_read_;
  int64_t plugin_prepare_execute_write_;

  void reuse()
  {
    sm_read_.reuse();
    sm_read_internal_sync_request_resp_ = 0;
    tunnel_init_ = 0;
    sm_trim_ok_ = 0;
    sm_rewrite_ok_delta_ = 0;
    plugin_trim_ok_ = 0;
    plugin_rewrite_ok_delta_ = 0;

    producer_observer_read_ = 0;
    consumer_transform_write_ = 0;
    producer_transform_read_ = 0;
    consumer_client_write_ = 0;

    plugin_decompress_read_ = 0;
    plugin_decompress_decrease_ = 0;
    plugin_decompress_write_ = 0;

    plugin_cursor_read_ = 0;
    plugin_cursor_write_ = 0;

    plugin_prepare_read_ = 0;
    plugin_prepare_write_ = 0;

    plugin_prepare_execute_read_ = 0;
    plugin_prepare_execute_write_ = 0;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
};

struct ObReqForwardDataFlow
{
  // sm 可能多次从网络读取用户请求数据
  common::ObSEArray<int64_t, 2>  sm_read_;
  // sm 写入用户请求数据, 同时包含客户端登录时的 handshake/handshake response/
  int64_t sm_write_;
  // sm 写入用户请求数据前可能会先发送同步语句给 server, 记录可能的上一个同步语句写入数据长度
  // 包含切换路由 handshake response, 同步 database, 同步会话变量, 同步 ps 等
  int64_t sm_write_sync_req_;
  // tunnel 初始化缓冲区数据的大小, 理论上应该与 sm_read_[-1] 值相等
  int64_t tunnel_init_;
  // tunnel 转发 send_long_data 时会将 send_long_data 的下一个请求数据保留不处理
  int64_t tunnel_next_req_of_send_long_;

  int64_t producer_client_read_;
  int64_t consumer_transform_write_;
  int64_t producer_transform_read_;
  int64_t consumer_observer_write_;

  int64_t plugin_execute_read_;
  int64_t plugin_execute_write_;

  int64_t plugin_compress_read_;
  int64_t plugin_compress_increase_;
  int64_t plugin_compress_write_;

  int64_t plugin_prepare_read_;
  int64_t plugin_prepare_write_;

  void reuse()
  {
    sm_read_.reuse();
    sm_write_ = 0;
    sm_write_sync_req_ = 0;
    tunnel_init_ = 0;
    tunnel_next_req_of_send_long_ = 0;

    producer_client_read_ = 0;
    consumer_transform_write_ = 0;
    producer_transform_read_ = 0;
    consumer_observer_write_ = 0;

    plugin_execute_read_ = 0;
    plugin_execute_write_ = 0;

    plugin_compress_read_ = 0;
    plugin_compress_increase_ = 0;
    plugin_compress_write_ = 0;

    plugin_prepare_read_ = 0;
    plugin_prepare_write_ = 0;
  }

  int64_t to_string(char *buf, const int64_t buf_len) const;
};

class ObProtocolDiagnosis : public ObSharedRefCount
{
public:
  ObProtocolDiagnosis()
    : cur_idx_(0), mysql_req_analyzer_(), sql_cmd_(obmysql::OB_MYSQL_COM_SLEEP)
  {
    MEMSET(records_, 0, sizeof(records_));
    reuse_forward_flow();
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
  inline void set_extra_ok_exists() { active_diagnosis_.is_extra_ok_exists_ = true; };
  inline void reset_extra_ok_exists() { active_diagnosis_.is_extra_ok_exists_ = false; };
  inline const obmysql::ObMySQLCmd get_sql_cmd() const { return sql_cmd_; }
  void reuse_forward_flow();
  void reuse_resp_forward_flow();
  void reuse_req_forward_flow();
  void record_resp_forward_ctrl_flow(ObRespForwardCtrlFlow f);
  void record_req_forward_ctrl_flow(ObReqForwardCtrlFlow f);

  // 被动诊断 Request/Response 转发控制流是否存在异常, 存在异常则将异常内容输出到 buf
  int64_t diagnose_request_forward(char *buf, const int64_t buf_len) const;
  int64_t diagnose_response_forward(char *buf, const int64_t buf_len) const;

  int64_t to_string(char *buf, const int64_t buf_len) const;

  // 主动诊断裁剪 extra ok 是否异常
  // 返回是否诊断到了 bug
  bool diagnose_extra_ok_trim();
private:
  bool is_req_forward_plugin_finish() const;
  bool is_resp_forward_plugin_finish() const;
  bool is_req_forward_flow_exist(ObReqForwardCtrlFlow flow) const;
  bool is_resp_forward_flow_exist(ObRespForwardCtrlFlow flow) const;
  bool is_req_tunnel_finish() const;
  bool is_resp_tunnel_finish() const;
  bool is_req_plugin_prepare_finish() const;
  bool is_req_plugin_execute_finish() const;
  bool is_req_plugin_compress_finish() const;
  bool is_resp_plugin_cursor_finish() const;
  bool is_resp_plugin_prepare_finish() const;
  bool is_resp_plugin_prepare_execute_finish() const;
  bool is_resp_plugin_decompress_finish() const;
  int64_t get_req_sm_read() const;
  int64_t get_resp_sm_read() const;

public:
  common::ObSEArray<ObRespForwardCtrlFlow, 8> resp_forward_ctrl_flow_;
  common::ObSEArray<ObReqForwardCtrlFlow, 8> req_forward_ctrl_flow_;
  ObRespForwardDataFlow resp_forward_data_flow_;
  ObReqForwardDataFlow req_forward_data_flow_;

private:
  ObPacketRecord records_[MAX_PACKET_RECORDS];
  uint8_t cur_idx_;
  ObMysqlRequestAnalyzer mysql_req_analyzer_; // to analyze mysql packet from block reader
  obmysql::ObMySQLCmd sql_cmd_;

  // 请求转发完成后主动检查转发过程是否异常的相关数据
  struct {
    bool is_extra_ok_exists_: 1;   // 标记 Response 中是否应该存在 extra ok
  } active_diagnosis_;
};

} // end of proxy
} // end of obproxy
} // end of oceanbase

#endif