/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_mysql_response_cursor_transform_plugin.h"
#include "rpc/obmysql/packet/ompk_resheader.h"
#include "rpc/obmysql/packet/ompk_field.h"
#include "rpc/obmysql/packet/ompk_row.h"
#include "proxy/mysql/ob_cursor_struct.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "proxy/mysqllib/ob_proxy_session_info.h"
#include "common/obsm_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObMysqlResponseCursorTransformPlugin *ObMysqlResponseCursorTransformPlugin::alloc(ObApiTransaction &transaction)
{
  return op_reclaim_alloc_args(ObMysqlResponseCursorTransformPlugin, transaction);
}

ObMysqlResponseCursorTransformPlugin::ObMysqlResponseCursorTransformPlugin(ObApiTransaction &transaction)
  : ObTransformationPlugin(transaction, ObTransformationPlugin::RESPONSE_TRANSFORMATION),
    local_produce_reader_(NULL), local_analyze_reader_(NULL), local_buffer_(NULL), pkt_reader_(),
    resultset_state_(RESULTSET_HEADER), column_num_(0), pkt_count_(0),
    have_cursor_(false), field_types_()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponseCursorTransformPlugin born", K(this));
  // local_buffer_ 的使用目的换一个 ObMIOBuffer 暂存上游过来的数据, 缓急上游 ObMIOBuffer 数据压力
  // 因为 consume() 的参数 reader 对应的 ObMIOBuffer 如果数据量太大将无法扩容, 继续读取数据会造成数据流 Hung
  if (OB_ISNULL(local_buffer_ = new_empty_miobuffer())) {
    PROXY_API_LOG(EDIAG, "fail to alloc memory for local_buffer_");
  } else if (OB_ISNULL(local_analyze_reader_ = local_buffer_->alloc_reader())) {
    PROXY_API_LOG(EDIAG, "fail to alloc reader of local_buffer_");
  } else if (OB_ISNULL(local_produce_reader_ = local_buffer_->alloc_reader())) {
    PROXY_API_LOG(EDIAG, "fail to alloc reader of local_buffer_");
  }
}

void ObMysqlResponseCursorTransformPlugin::free_local_buffer()
{
  if (NULL != local_analyze_reader_) {
    local_analyze_reader_->dealloc();
    local_analyze_reader_ = NULL;
  }

  if (NULL != local_produce_reader_) {
    local_produce_reader_->dealloc();
    local_produce_reader_ = NULL;
  }

  if (NULL != local_buffer_) {
    free_miobuffer(local_buffer_);
    local_buffer_ = NULL;
  }
}

void ObMysqlResponseCursorTransformPlugin::destroy()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponseCursorTransformPlugin destroy", K(this));
  ObTransformationPlugin::destroy();
  free_local_buffer();
  reset();
  op_reclaim_free(this);
}

void ObMysqlResponseCursorTransformPlugin::reset()
{
  resultset_state_ = RESULTSET_HEADER;
  column_num_ = 0;
  pkt_count_ = 0;
  have_cursor_ = false;
  field_types_.reset();
  pkt_reader_.reset();
}

int ObMysqlResponseCursorTransformPlugin::consume(event::ObIOBufferReader *reader)
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponseCursorTransformPlugin::consume happen");
  int ret = OB_SUCCESS;
  event::ObIOBufferReader *produce_reader = NULL;

  if (OB_NOT_NULL(sm_->protocol_diagnosis_) && reader != NULL) {
    sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_cursor_read_ += reader->read_avail();
    PROTOCOL_FORWARD_LOG(TRACE, "plugin_cursor read response",
      "plugin_cursor_read", sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_cursor_read_,
      "read_delta", reader->read_avail());
  }

  if (local_analyze_reader_ == NULL || local_buffer_ == NULL) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(EDIAG, "unexpected null ptr", KP(local_analyze_reader_), KP(local_buffer_), K(ret));
  } else {
    int64_t forward_len = 0;  // 往下游 produce 的数据大小
    if (RESULTSET_END != resultset_state_) {
      int64_t written = 0;
      produce_reader = local_produce_reader_;
      if (OB_FAIL(local_buffer_->write(reader, reader->read_avail(), written))) {  // 并没有真正拷贝内存, 仅克隆 block
        PROXY_API_LOG(EDIAG, "fail to alloc reader of local_buffer_", K(ret));
      } else if (reader->read_avail() != written) {
        PROXY_API_LOG(EDIAG, "fail to write all data to local_buffer_", K(written), K(reader->read_avail()));
      } else {
        ObMysqlAnalyzeResult result;
        while (OB_SUCC(ret) && local_analyze_reader_->read_avail() > 0) {
          if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*local_analyze_reader_, result))) {
            PROXY_API_LOG(EDIAG, "fail to analyze one packet", K(local_analyze_reader_), K(ret));
          } else {
            if (ANALYZE_DONE == result.status_) {
              if (result.is_error_packet()) {
                resultset_state_ = RESULTSET_END;
              }

              switch(resultset_state_) {
              case RESULTSET_HEADER :
                if (OB_FAIL(handle_resultset_header(local_analyze_reader_))) {
                  PROXY_API_LOG(EDIAG, "handle resultset header failed", K(ret));
                }
                break;
              case RESULTSET_FIELD :
                if (OB_UNLIKELY(result.is_eof_packet())) {
                  // just for defence
                  ret = OB_UNKNOWN_PACKET;
                  PROXY_API_LOG(EDIAG, "unknown decode state", K_(column_num), K_(pkt_count), K(result), K(ret));
                } else if (OB_FAIL(handle_resultset_field(local_analyze_reader_))) {
                  PROXY_API_LOG(EDIAG, "handle resultset field", K(ret));
                }
                break;
              case RESULTSET_EOF_FIRST :
                if (OB_UNLIKELY(!result.is_eof_packet())) {
                  PROXY_API_LOG(EDIAG, "excepted EOF packet, but not", "type", result.meta_.pkt_type_, K(ret));
                } else {
                  if (have_cursor_) {
                    resultset_state_ = RESULTSET_ROW;
                  } else {
                    resultset_state_ = RESULTSET_END;
                  }
                }
                break;
              case RESULTSET_ROW :
                if (result.is_eof_packet()) {
                  // 读完所有的 Row Packet 之后不需要再解析了
                  resultset_state_ = RESULTSET_END;
                } else if (OB_FAIL(handle_resultset_row(local_analyze_reader_, sm_, field_types_, have_cursor_, column_num_))) {
                  PROXY_API_LOG(EDIAG, "fail to consume local analyze reader", K(result.meta_.pkt_len_), K(ret));
                }
                break;
              default :
                break;
              }

              if (OB_FAIL(ret)) {
              } else if (RESULTSET_END == resultset_state_) {
                forward_len += local_analyze_reader_->read_avail();
                local_analyze_reader_->consume_all();
                break;
              } else if (OB_FAIL(local_analyze_reader_->consume(result.meta_.pkt_len_))) {
                PROXY_API_LOG(EDIAG, "fail to consume local analyze reader", K(result.meta_.pkt_len_), K(ret));
              } else {
                forward_len += result.meta_.pkt_len_;
              }
            } else {
              break;
            }
          }
        }
      }
    } else {
      // RESULTSET_END 状态, 说明已经不需要读 reader 里面的 packet 内容了
      // 直接将 reader 的数据全部写入下游
      forward_len = reader->read_avail();
      produce_reader = reader;
    }

    int64_t actual_size = 0;
    if (OB_SUCC(ret) && forward_len > 0) {
      if (forward_len != (actual_size = produce(produce_reader, forward_len))) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_API_LOG(EDIAG, "fail to produce", "expected size", forward_len, "actual size", actual_size, K(ret));
      // local_produce_reader_ 中已经被解析的数据写入下游后要及时 consume
      // 因为下次进入这个函数时会有新的数据写入到 local_buffer_
      // reader 中的数据写入下游后不用管,因为在外部会调用 reader->consume_all()
      } else if (produce_reader == local_produce_reader_) {
        if (OB_FAIL(local_produce_reader_->consume(forward_len))) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_API_LOG(EDIAG, "fail to consume data from local_produce_reader", K(ret), K(forward_len));
        }
      }
      if (OB_NOT_NULL(sm_->protocol_diagnosis_)) {
        sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_cursor_write_ += forward_len;
        PROTOCOL_FORWARD_LOG(TRACE, "plugin_cursor write response",
          "plugin_cursor_write", sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_cursor_write_,
          "write_delta", forward_len);
      }
    }
  }

  if (OB_FAIL(ret)) {
    sm_->trans_state_.inner_errcode_ = ret;
    // if failed, set state to INTERNAL_ERROR
    sm_->trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
  }

  return ret;
}

int ObMysqlResponseCursorTransformPlugin::handle_resultset_header(event::ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;
  OMPKResheader resultset_header;

  pkt_reader_.reset();
  if (OB_FAIL(pkt_reader_.get_packet(*reader, resultset_header))) {
    PROXY_API_LOG(EDIAG, "fail to get filed packet from reader", K(ret));
  } else {
    column_num_ = resultset_header.get_field_count();
    if (OB_UNLIKELY(0 == column_num_)) {
      resultset_state_ = RESULTSET_EOF_FIRST;
    } else {
      resultset_state_ = RESULTSET_FIELD;
    }
  }

  return ret;
}

int ObMysqlResponseCursorTransformPlugin::handle_resultset_field(event::ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;

  ObMySQLField field;
  OMPKField field_packet(field);

  pkt_reader_.reset();
  if (OB_UNLIKELY(column_num_ < pkt_count_)) {
    ret = OB_UNKNOWN_PACKET;
    PROXY_API_LOG(WDIAG, "error packet decode state", K_(column_num), K_(pkt_count), K(ret));
  } else if (OB_FAIL(pkt_reader_.get_packet(*reader, field_packet))) {
    PROXY_API_LOG(EDIAG, "fail to get filed packet from reader", K(ret));
  } else {
    if (OB_FAIL(field_types_.push_back(field.type_))) {
      PROXY_API_LOG(EDIAG, "fail to push field type", K(ret), K(field.type_));
    } else {
      pkt_count_++;
      if (OB_MYSQL_TYPE_CURSOR == field.type_) {
        have_cursor_ = true;
      }

      if (pkt_count_ == column_num_) {
        resultset_state_ = RESULTSET_EOF_FIRST;
        pkt_count_ = 0;
      }
    }
  }

  return ret;
}

int ObMysqlResponseCursorTransformPlugin::handle_resultset_row(event::ObIOBufferReader *reader, ObMysqlSM *sm,
                                                               const ObArray<obmysql::EMySQLFieldType> &field_types,
                                                               bool hava_cursor, uint64_t column_num)
{
  int ret = OB_SUCCESS;

  if (hava_cursor) {
    ObNewRow row;
    ObSMRow sm_row(BINARY, row);
    OMPKRow row_packet(sm_row);
    packet::ObMysqlPacketReader pkt_reader;
    if (OB_FAIL(pkt_reader.get_packet(*reader, row_packet))) {
      PROXY_API_LOG(EDIAG, "fail to get filed packet from reader", K(ret));
    } else {
      const char *start = row_packet.get_cdata();
      const char *pos = start;
      int64_t payload_len = row_packet.get_clen();
      const char *bitmap = NULL;
      int64_t bitmap_len = (column_num + 7 + 2) / 8; /* skip null bits */

      pos++;
      payload_len--;

      bitmap = pos;
      pos += bitmap_len;
      payload_len -= bitmap_len;

      for (int64_t i = 0; OB_SUCC(ret) && i < column_num; ++i) {
        /* first 2 bits are reserved */
        ObObj param;
        if (ObSMUtils::update_from_bitmap(param, bitmap, i + 2)) {
          // do nothing
        } else {
          obmysql::EMySQLFieldType type;
          if (OB_FAIL(field_types.at(i, type))) {
            PROXY_API_LOG(WDIAG, "fail to get field_types", K(i), K(ret));
          } else {
            if (OB_MYSQL_TYPE_CURSOR == type) {
              ObMysqlClientSession *client_session = sm->get_client_session();
              ObMysqlServerSession *server_session = sm->get_server_session();
              if (OB_ISNULL(server_session)) {
                server_session = client_session->get_last_server_session();
              }

              uint32_t client_cursor_id = client_session->inc_and_get_cursor_id();
              uint32_t server_cursor_id = 0;
              if (OB_FAIL(ObMysqlPacketUtil::get_uint4(pos, payload_len, server_cursor_id))) {
                PROXY_API_LOG(WDIAG, "fail to get cursor id", K(i), K(ret));
              } else if (OB_FAIL(add_cursor_id_pair(server_session, client_cursor_id, server_cursor_id))) {
                PROXY_API_LOG(WDIAG, "fail to add cursor id parit", K(i), K(client_cursor_id), K(server_cursor_id), K(ret));
              } else if (OB_FAIL(add_cursor_id_addr(client_session, client_cursor_id, server_session->get_netvc()->get_remote_addr()))) {
                PROXY_API_LOG(WDIAG, "fail to add cursor id addr", K(i), K(client_cursor_id), K(ret));
              } else {
                // pos - 4 回到 cursor_id 的起始位置, 然后减 start, 得到偏移
                reader->replace(reinterpret_cast<const char*>(&client_cursor_id), sizeof(client_cursor_id),
                                MYSQL_NET_HEADER_LENGTH + (pos - 4 - start));
              }
            } else if (OB_FAIL(skip_field_value(pos, payload_len, type))) {
              PROXY_API_LOG(WDIAG, "fail to skip field value", K(i), K(ret));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObMysqlResponseCursorTransformPlugin::add_cursor_id_pair(ObMysqlServerSession *server_session, uint32_t client_cursor_id, uint32_t server_cursor_id)
{
  int ret = OB_SUCCESS;

  ObServerSessionInfo &ss_info = server_session->get_session_info();
  ObCursorIdPair *cursor_id_pair = NULL;
  if (OB_FAIL(ObCursorIdPair::alloc_cursor_id_pair(client_cursor_id, server_cursor_id, cursor_id_pair))) {
    PROXY_API_LOG(WDIAG, "fail to alloc cursor id pair", K(client_cursor_id), K(server_cursor_id), K(ret));
  } else if (OB_ISNULL(cursor_id_pair)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(WDIAG, "cursor_id_pair is null", K(cursor_id_pair), K(ret));
  } else if (OB_FAIL(ss_info.add_cursor_id_pair(cursor_id_pair))) {
    PROXY_API_LOG(WDIAG, "fail to add cursor_id_pair", KPC(cursor_id_pair), K(ret));
    cursor_id_pair->destroy();
  }

  return ret;
}

int ObMysqlResponseCursorTransformPlugin::add_cursor_id_addr(ObMysqlClientSession *client_session, uint32_t client_cursor_id, const sockaddr &addr)
{
  int ret = OB_SUCCESS;
  const bool using_service_name = client_session->using_service_name();
  ObString cluster_name;
  ObString tenant_name;
  ObClientSessionInfo &cs_info = client_session->get_session_info();
  ObServiceaNameSessionInfo *service_name_session_info = cs_info.get_service_name_session_info();
  ObCursorIdAddr *cursor_id_addr = NULL;

  if (using_service_name) {
    if (OB_FAIL(cs_info.get_cluster_name(cluster_name))) {
      PROXY_API_LOG(WDIAG, "get cluster name failed", K(ret));
    } else if (OB_FAIL(cs_info.get_tenant_name(tenant_name))) {
      PROXY_API_LOG(WDIAG, "get tenant name failed", K(ret));
    }
  }

  if (OB_FAIL(ObCursorIdAddr::alloc_cursor_id_addr(client_cursor_id, addr, cursor_id_addr))) {
    PROXY_API_LOG(WDIAG, "fail to alloc cursor id addr", K(client_cursor_id), K(ret));
  } else if (OB_ISNULL(cursor_id_addr)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(WDIAG, "cursor_id_addr is null", K(cursor_id_addr), K(ret));
  } else if (using_service_name) {
    if (OB_ISNULL(service_name_session_info)) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_API_LOG(WDIAG, "unexcepted service_name_session_info is null, mayby out of memory", K(ret));
    } else if (OB_FAIL(service_name_session_info->add_cursor_id_tenant_info(client_cursor_id, tenant_name, cluster_name))) {
      PROXY_API_LOG(WDIAG, "fail to set cursor tenant info, will destory cursor_id_addr",
                    K(tenant_name), K(cluster_name), KPC(cursor_id_addr), K(ret));
      cursor_id_addr->destroy();
      cursor_id_addr = NULL;
    }
  }

  if (OB_FAIL(cs_info.add_cursor_id_addr(cursor_id_addr))) {
    PROXY_API_LOG(WDIAG, "fail to add cursor_id_addr", KPC(cursor_id_addr), K(ret));
    cursor_id_addr->destroy();
  }

  return ret;
}

int ObMysqlResponseCursorTransformPlugin::skip_field_value(const char *&data, int64_t &buf_len, EMySQLFieldType field_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_API_LOG(WDIAG, "invalid input value", K(ret));
  } else {
    switch (field_type) {
      case OB_MYSQL_TYPE_NULL:
        break;
      case OB_MYSQL_TYPE_TINY: {
        int8_t value;
        ret = ObMysqlPacketUtil::get_int1(data, buf_len, value);
        break;
      }
      case OB_MYSQL_TYPE_SHORT: {
        int16_t value = 0;
        ret = ObMysqlPacketUtil::get_int2(data, buf_len, value);
        break;
      }
      case OB_MYSQL_TYPE_CURSOR:
      case OB_MYSQL_TYPE_INT24:
      case OB_MYSQL_TYPE_LONG: {
        int32_t value = 0;
        ret = ObMysqlPacketUtil::get_int4(data, buf_len, value);
        break;
      }
      case OB_MYSQL_TYPE_LONGLONG: {
        int64_t value = 0;
        ret = ObMysqlPacketUtil::get_int8(data, buf_len, value);
        break;
      }
      case OB_MYSQL_TYPE_FLOAT: {
        float value = 0;
        ret = ObMysqlPacketUtil::get_float(data, buf_len, value);
        break;
      }
      case OB_MYSQL_TYPE_DOUBLE: {
        double value = 0;
        ret = ObMysqlPacketUtil::get_double(data, buf_len, value);
        break;
      }
      case OB_MYSQL_TYPE_YEAR: {
        int16_t value = 0;
        ret = ObMysqlPacketUtil::get_int2(data, buf_len, value);
        break;
      }
      case OB_MYSQL_TYPE_GEOMETRY:
      case OB_MYSQL_TYPE_JSON:
      case OB_MYSQL_TYPE_BLOB:
      case OB_MYSQL_TYPE_LONG_BLOB:
      case OB_MYSQL_TYPE_MEDIUM_BLOB:
      case OB_MYSQL_TYPE_TINY_BLOB:
      case OB_MYSQL_TYPE_SET:
      case OB_MYSQL_TYPE_ENUM:
      case OB_MYSQL_TYPE_BIT:
      case OB_MYSQL_TYPE_DATE:
      case OB_MYSQL_TYPE_NEWDATE:
      case OB_MYSQL_TYPE_DATETIME:
      case OB_MYSQL_TYPE_TIMESTAMP:
      case OB_MYSQL_TYPE_TIME:
      case OB_MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
      case OB_MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case OB_MYSQL_TYPE_OB_TIMESTAMP_NANO:
      case OB_MYSQL_TYPE_OB_RAW:
      case OB_MYSQL_TYPE_STRING:
      case OB_MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_OB_NCHAR:
      case MYSQL_TYPE_OB_NVARCHAR2:
      case OB_MYSQL_TYPE_VAR_STRING:
      case OB_MYSQL_TYPE_OB_UROWID:
      case OB_MYSQL_TYPE_DECIMAL:
      case OB_MYSQL_TYPE_NEWDECIMAL: {
        uint64_t length = 0;
        if (OB_FAIL(ObMysqlPacketUtil::get_length(data, buf_len, length))) {
          PROXY_API_LOG(WDIAG, "decode varchar field value failed", K(buf_len), K(ret));
        } else if (buf_len < length) {
          ret = OB_SIZE_OVERFLOW;
          PROXY_API_LOG(WDIAG, "data buf size is not enough", K(length), K(buf_len), K(ret));
        } else {
          data += length;
          buf_len -= length;
        }
        break;
      }
      case OB_MYSQL_TYPE_NOT_DEFINED:
      case OB_MYSQL_TYPE_COMPLEX: {
        ret = OB_ERR_ILLEGAL_TYPE;
        PROXY_API_LOG(WDIAG, "illegal mysql type, we will set param with null", K(field_type), K(ret));
        break;
      }
    }
  }
  return ret;
}

void ObMysqlResponseCursorTransformPlugin::handle_input_complete()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponseCursorTransformPlugin::handle_input_complete happen");
  if (OB_NOT_NULL(sm_->protocol_diagnosis_)) {
    sm_->protocol_diagnosis_->record_resp_forward_ctrl_flow(ObRespForwardCtrlFlow::PLUGIN_CURSOR_FINISH);
    PROTOCOL_FORWARD_LOG(TRACE, "plugin_cursor process response finish");
  }

  free_local_buffer();
  set_output_complete();
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
