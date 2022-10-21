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
    local_reader_(NULL), local_analyze_reader_(NULL), pkt_reader_(),
    resultset_state_(RESULTSET_HEADER), column_num_(0), pkt_count_(0),
    hava_cursor_(false), field_types_()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponseCursorTransformPlugin born", K(this));
}

void ObMysqlResponseCursorTransformPlugin::destroy()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponseCursorTransformPlugin destroy", K(this));
  ObTransformationPlugin::destroy();

  reset();

  op_reclaim_free(this);
}

void ObMysqlResponseCursorTransformPlugin::reset()
{
  resultset_state_ = RESULTSET_HEADER;
  column_num_ = 0;
  pkt_count_ = 0;
  hava_cursor_ = false;
  field_types_.reset();
  pkt_reader_.reset();
}

int ObMysqlResponseCursorTransformPlugin::consume(event::ObIOBufferReader *reader)
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponseCursorTransformPlugin::consume happen");
  int ret = OB_SUCCESS;

  int64_t write_size = 0;
  ObMysqlAnalyzeResult result;

  // why use two reader?
  // local_analyze_reader for analyze. after analyze one mysql packet, will move to next mysql packet
  // local_reader for output data to tunnel, need from start pos
  if (NULL == local_reader_) {
    local_reader_ = reader->clone();
    local_analyze_reader_ = local_reader_->clone();
  } else {
    local_reader_->reserved_size_ = reader->reserved_size_;
    local_analyze_reader_->reserved_size_ = reader->reserved_size_;
  }

    while (OB_SUCC(ret) && local_analyze_reader_->read_avail()) {
      if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*local_analyze_reader_, result))) {
        PROXY_API_LOG(ERROR, "fail to analyze one packet", K(local_analyze_reader_), K(ret));
      } else {
        if (ANALYZE_DONE == result.status_) {
          switch(resultset_state_) {
          case RESULTSET_HEADER :
            if (OB_FAIL(handle_resultset_header(local_analyze_reader_))) {
              PROXY_API_LOG(ERROR, "handle resultset header failed", K(ret));
            }
            break;
          case RESULTSET_FIELD :
            if (OB_FAIL(handle_resultset_field(local_analyze_reader_))) {
              PROXY_API_LOG(ERROR, "handle resultset field", K(ret));
            }
            break;
          case RESULTSET_EOF_FIRST :
            if (MYSQL_EOF_PACKET_TYPE != result.meta_.pkt_type_) {
              PROXY_API_LOG(ERROR, "excepted EOF packet, but not", "type", result.meta_.pkt_type_, K(ret));
            } else {
              resultset_state_ = RESULTSET_ROW;
            }
            break;
          case RESULTSET_ROW :
            if (MYSQL_EOF_PACKET_TYPE == result.meta_.pkt_type_) {
              reset();
            } else if (OB_FAIL(handle_resultset_row(local_analyze_reader_, sm_, field_types_, hava_cursor_, column_num_))) {
              PROXY_API_LOG(ERROR, "fail to consume local analyze reader", K(result.meta_.pkt_len_), K(ret));
            }
            break;
          default :
            break;
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(local_analyze_reader_->consume(result.meta_.pkt_len_))) {
              PROXY_API_LOG(ERROR, "fail to consume local analyze reader", K(result.meta_.pkt_len_), K(ret));
            } else {
              write_size += result.meta_.pkt_len_;
            }
          }
        } else {
          break;
        }
      }
    }

  if (OB_SUCC(ret) && write_size > 0) {
    int64_t actual_size = 0;
    if (write_size != (actual_size = produce(local_reader_, write_size))) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_API_LOG(ERROR, "fail to produce", "expected size", write_size,
                    "actual size", actual_size, K(ret));
    } else if (write_size == local_reader_->read_avail() && OB_FAIL(local_analyze_reader_->consume_all())) {
      PROXY_API_LOG(ERROR, "fail to consume all local analyze reader", K(ret));
    } else if (OB_FAIL(local_reader_->consume(write_size))) {
      PROXY_API_LOG(ERROR, "fail to consume local reader", K(write_size), K(ret));
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
    PROXY_API_LOG(ERROR, "fail to get filed packet from reader", K(ret));
  } else {
    column_num_ = resultset_header.get_field_count();
    resultset_state_ = RESULTSET_FIELD;
  }

  return ret;
}

int ObMysqlResponseCursorTransformPlugin::handle_resultset_field(event::ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;

  ObMySQLField field;
  OMPKField field_packet(field);

  pkt_reader_.reset();
  if (OB_FAIL(pkt_reader_.get_packet(*reader, field_packet))) {
    PROXY_API_LOG(ERROR, "fail to get filed packet from reader", K(ret));
  } else {
    pkt_count_++;
    field_types_.push_back(field.type_);
    if (OB_MYSQL_TYPE_CURSOR == field.type_) {
      hava_cursor_ = true;
    }

    if (pkt_count_ == column_num_) {
      resultset_state_ = RESULTSET_EOF_FIRST;
      pkt_count_ = 0;
    }
  }

  return ret;
}

int ObMysqlResponseCursorTransformPlugin::handle_resultset_row(event::ObIOBufferReader *reader, ObMysqlSM *sm,
                                                               ObArray<obmysql::EMySQLFieldType> field_types,
                                                               bool hava_cursor, uint64_t column_num)
{
  int ret = OB_SUCCESS;

  if (hava_cursor) {
    ObNewRow row;
    ObSMRow sm_row(BINARY, row);
    OMPKRow row_packet(sm_row);
    packet::ObMysqlPacketReader pkt_reader;
    if (OB_FAIL(pkt_reader.get_packet(*reader, row_packet))) {
      PROXY_API_LOG(ERROR, "fail to get filed packet from reader", K(ret));
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
          if (OB_MYSQL_TYPE_CURSOR == field_types.at(i)) {
            ObMysqlClientSession *client_session = sm->get_client_session();
            ObMysqlServerSession *server_session = sm->get_server_session();
            if (OB_ISNULL(server_session)) {
              server_session = client_session->get_server_session();
            }

            uint32_t client_cursor_id = client_session->inc_and_get_cursor_id();
            uint32_t server_cursor_id = 0;
            if (OB_FAIL(ObMysqlPacketUtil::get_uint4(pos, payload_len, server_cursor_id))) {
              PROXY_API_LOG(WARN, "fail to get cursor id", K(i), K(ret));
            } else if (OB_FAIL(add_cursor_id_pair(server_session, client_cursor_id, server_cursor_id))) {
              PROXY_API_LOG(WARN, "fail to add cursor id parit", K(i), K(client_cursor_id), K(server_cursor_id), K(ret));
            } else if (OB_FAIL(add_cursor_id_addr(client_session, client_cursor_id, server_session->get_netvc()->get_remote_addr()))) {
              PROXY_API_LOG(WARN, "fail to add cursor id addr", K(i), K(client_cursor_id), K(ret));
            } else {
              reader->replace(reinterpret_cast<const char*>(&client_cursor_id), sizeof(client_cursor_id),
                              MYSQL_NET_HEADER_LENGTH + (pos - 4 - start));
            }
          } else if (OB_FAIL(skip_field_value(pos, payload_len, field_types.at(i)))) {
            PROXY_API_LOG(WARN, "fail to skip field value", K(i), K(ret));
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
    PROXY_API_LOG(WARN, "fail to alloc cursor id pair", K(client_cursor_id), K(server_cursor_id), K(ret));
  } else if (OB_ISNULL(cursor_id_pair)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(WARN, "cursor_id_pair is null", K(cursor_id_pair), K(ret));
  } else if (OB_FAIL(ss_info.add_cursor_id_pair(cursor_id_pair))) {
    PROXY_API_LOG(WARN, "fail to add cursor_id_pair", KPC(cursor_id_pair), K(ret));
    cursor_id_pair->destroy();
  }

  return ret;
}

int ObMysqlResponseCursorTransformPlugin::add_cursor_id_addr(ObMysqlClientSession *client_session, uint32_t client_cursor_id, const sockaddr &addr)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &cs_info = client_session->get_session_info();
  ObCursorIdAddr *cursor_id_addr = NULL;
  if (OB_FAIL(ObCursorIdAddr::alloc_cursor_id_addr(client_cursor_id, addr, cursor_id_addr))) {
    PROXY_API_LOG(WARN, "fail to alloc cursor id addr", K(client_cursor_id), K(ret));
  } else if (OB_ISNULL(cursor_id_addr)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(WARN, "cursor_id_addr is null", K(cursor_id_addr), K(ret));
  } else if (OB_FAIL(cs_info.add_cursor_id_addr(cursor_id_addr))) {
    PROXY_API_LOG(WARN, "fail to add cursor_id_addr", KPC(cursor_id_addr), K(ret));
    cursor_id_addr->destroy();
  }

  return ret;
}

int ObMysqlResponseCursorTransformPlugin::skip_field_value(const char *&data, int64_t &buf_len, EMySQLFieldType field_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_API_LOG(WARN, "invalid input value", K(ret));
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
      case OB_MYSQL_TYPE_VAR_STRING:
      case OB_MYSQL_TYPE_OB_UROWID:
      case OB_MYSQL_TYPE_DECIMAL:
      case OB_MYSQL_TYPE_NEWDECIMAL: {
        uint64_t length = 0;
        if (OB_FAIL(ObMysqlPacketUtil::get_length(data, buf_len, length))) {
          PROXY_API_LOG(WARN, "decode varchar field value failed", K(buf_len), K(ret));
        } else if (buf_len < length) {
          ret = OB_SIZE_OVERFLOW;
          PROXY_API_LOG(WARN, "data buf size is not enough", K(length), K(buf_len), K(ret));
        } else {
          data += length;
          buf_len -= length;
        }
        break;
      }
      case OB_MYSQL_TYPE_NOT_DEFINED:
      case OB_MYSQL_TYPE_COMPLEX: {
        ret = OB_ERR_ILLEGAL_TYPE;
        PROXY_API_LOG(WARN, "illegal mysql type, we will set param with null", K(field_type), K(ret));
        break;
      }
    }
  }
  return ret;
}

void ObMysqlResponseCursorTransformPlugin::handle_input_complete()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponseCursorTransformPlugin::handle_input_complete happen");
  if (NULL != local_reader_) {
    local_reader_->dealloc();
    local_reader_ = NULL;
  }

  if (NULL != local_analyze_reader_) {
    local_analyze_reader_->dealloc();
    local_analyze_reader_ = NULL;
  }

  set_output_complete();
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
