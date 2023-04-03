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

#define USING_LOG_PREFIX PROXY_CMD
#include "cmd/ob_cmd_handler.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "obproxy/proxy/mysqllib/ob_2_0_protocol_utils.h"

using namespace oceanbase::obproxy::event;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
ObCmdHandler::ObCmdHandler(ObMIOBuffer *buf, ObCmdInfo &info)
  : external_buf_(buf),
    internal_buf_(NULL), internal_reader_(NULL), internal_buf_limited_(info.get_memory_limit()),
    protocol_(info.get_protocol()), ob20_param_(info.get_ob20_param()), is_inited_(false), header_encoded_(false),
    seq_(info.get_seq()), original_seq_(info.get_seq())
{
}

ObCmdHandler::~ObCmdHandler()
{
  is_inited_ = false;
  destroy_internal_buf();
  protocol_ = ObProxyProtocol::PROTOCOL_NORMAL;
  ob20_param_.reset();
}

int ObCmdHandler::init(const bool is_query_cmd/*true*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    WARN_CMD("fail to init twice", K(ret));
  } else if (OB_ISNULL(external_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    WARN_CMD("internal argument is NULL", K(external_buf_), K(ret));
  } else if (OB_UNLIKELY(NULL != internal_buf_) || OB_UNLIKELY(NULL != internal_reader_)) {
    ret = OB_INVALID_ARGUMENT;
    WARN_CMD("internal argument is not NULL", K(internal_buf_), K(internal_reader_), K(ret));
  } else if (OB_ISNULL(internal_buf_ = new_empty_miobuffer())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    WARN_CMD("fail to new_empty_miobuffer for internal_buf_", K(ret));
  } else if (OB_ISNULL(internal_reader_ = internal_buf_->alloc_reader())) {
    ret = OB_ENTRY_NOT_EXIST;
    WARN_CMD("fail to alloc_reader for internal_reader_", K(ret));
  } else {
    //WARN::The io buffer will alloced by thread allocator, and free by work thread. it will memory leak.
    //So we need alloc these buf in work thread before use it.
    //For non-query cmd, only one block is enough
    int64_t block_count = 1;
    if (is_query_cmd && internal_buf_limited_ > 0) {
      block_count = internal_buf_limited_ / DEFAULT_LARGE_BUFFER_SIZE;
      if (0 != internal_buf_limited_ % DEFAULT_LARGE_BUFFER_SIZE) {
        block_count += 1;
      }
    }
    if (OB_FAIL(internal_buf_->add_block(block_count))) {
      WARN_CMD("fail to add_block for internal_buf_", K(block_count), K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObCmdHandler::reset()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    WARN_CMD("it has not inited", K(ret));
  } else if (OB_FAIL(internal_reader_->consume(internal_reader_->read_avail()))) {
    WARN_CMD("fail to consume internal buf", K(ret));
  } else {
    internal_buf_->reset();
    seq_ = original_seq_;
    // could not reset protocol and ob20 param, cause we need fill external buf according to protocol
  }
  return ret;
}

void ObCmdHandler::destroy_internal_buf()
{
  if (OB_LIKELY(NULL != internal_reader_)) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(internal_reader_->consume(internal_reader_->read_avail()))) {
      WARN_CMD("fail to consume ", K(ret));
    }
    internal_reader_ = NULL;
  }
  
  if (OB_LIKELY(NULL != internal_buf_)) {
    free_miobuffer(internal_buf_);
    internal_buf_ = NULL;
  }
}

int ObCmdHandler::encode_header(const ObString *cname, const EMySQLFieldType *ctype, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    WARN_CMD("it has not inited", K(ret));
  } else if (OB_ISNULL(cname) || OB_ISNULL(ctype) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObArray<ObMySQLField> fields;
    ObMySQLField field;
    //TODO: ref the observer to_mysql_field(), make the encode better
    field.charsetnr_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
    for (int64_t i = 0; (OB_SUCC(ret) && i < size); ++i) {
      field.cname_ = cname[i];
      field.org_cname_ = cname[i];
      field.type_ = ctype[i];
      if (OB_FAIL(fields.push_back(field))) {
        WARN_CMD("fail to push field into array", K(field), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObMysqlPacketUtil::encode_header(*internal_buf_, seq_, fields))) {
        WARN_CMD("fail to encode header", K(ret));
      } else {
        DEBUG_CMD("succ to encode header", K(fields));
      }
    }
  }
  return ret;
}

int ObCmdHandler::encode_header(const ObProxyColumnSchema *column_schema, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    WARN_ICMD("it has not inited", K(ret));
  } else if (OB_ISNULL(column_schema) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObArray<ObMySQLField> fields;
    ObMySQLField field;
    //TODO: ref the observer to_mysql_field(), make the encode better
    field.charsetnr_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
    for (int64_t i = 0; (OB_SUCC(ret) && i < size); ++i) {
      field.cname_ = column_schema[i].cname_;
      field.org_cname_ = column_schema[i].cname_;
      field.type_ = column_schema[i].ctype_;
      if (OB_FAIL(fields.push_back(field))) {
        WARN_ICMD("fail to push field into array", K(field), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObMysqlPacketUtil::encode_header(*internal_buf_, seq_, fields))) {
        WARN_ICMD("fail to encode header", K(ret));
      } else {
        DEBUG_ICMD("succ to encode header", K(fields));
      }
    }
  }
  return ret;
}

int ObCmdHandler::encode_row_packet(const ObNewRow &row, const bool need_limit_size/*true*/)
{
  int ret = OB_SUCCESS;
  int64_t data_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    WARN_CMD("it has not inited", K(ret));
  } else if (need_limit_size
          && internal_buf_limited_ > 0
          && (data_size = internal_reader_->read_avail()) >= internal_buf_limited_) {
    ret = OB_BUF_NOT_ENOUGH;
    WARN_CMD("internal cmd response size will out of limited size, break it",
              K(data_size), K(internal_buf_limited_), K(ret));
  } else if (OB_FAIL(ObMysqlPacketUtil::encode_row_packet(*internal_buf_, seq_, row))) {
    WARN_CMD("fail to encode row packet", K(row), K(ret));
  } else {
    DEBUG_CMD("succ to encode row packet", K(row));
  }
  return ret;
}

int ObCmdHandler::encode_eof_packet()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    WARN_CMD("it has not inited", K(ret));
  } else if (OB_FAIL(ObMysqlPacketUtil::encode_eof_packet(*internal_buf_, seq_))) {
    WARN_CMD("fail to encode eof packet", K(ret));
  } else {
    DEBUG_CMD("succ to encode eof packet");
  }
  return ret;
}

// write packet in mysql format to internal buf, then trans it to external buf as ob20/mysql
int ObCmdHandler::encode_err_packet(const int errcode)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    WARN_CMD("it has not inited", K(ret));
  } else if (OB_FAIL(reset())) {  // before encode err packet, we need clean buf
    WARN_CMD("fail to do reset", K(errcode), K(ret));
  } else {
    char *err_msg = NULL;
    if (OB_FAIL(packet::ObProxyPacketWriter::get_err_buf(errcode, err_msg))) {
      WARN_CMD("fail to get err buf", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet(*internal_buf_, seq_, errcode, err_msg))) {
      WARN_CMD("fail to encode err packet", K(errcode), K(err_msg), K(ret));
    } else {
      INFO_CMD("succ to encode err packet", K(errcode), K(err_msg));
    }
  }

  return ret;
}

int ObCmdHandler::encode_ok_packet(const int64_t affected_rows,
                                   const obmysql::ObMySQLCapabilityFlags &capability)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    WARN_CMD("it has not inited", K(ret));
  } else if (OB_FAIL(ObMysqlPacketUtil::encode_ok_packet(*internal_buf_, seq_, affected_rows, capability))) {
    WARN_CMD("fail to encode ok packet", K(ret));
  } else {
    INFO_CMD("succ to encode ok packet");
  }
  return ret;
}

bool ObCmdHandler::match_like(const ObString &str_text, const ObString &str_pattern) const
{
  bool ret = false;
  if (str_pattern.empty()) {
    ret = true;
  } else if (str_text.empty()) {
    //return false if text config namme is NULL
  } else {
    ret = ObCharset::wildcmp(CS_TYPE_UTF8MB4_BIN, str_text, str_pattern, 0, '_', '%');
  }
  return ret;
}

int ObCmdHandler::fill_external_buf()
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(protocol_ == ObProxyProtocol::PROTOCOL_OB20)) {
    if (OB_FAIL(ObProto20Utils::consume_and_compress_data(internal_reader_, external_buf_,
                                                          internal_reader_->read_avail(), ob20_param_))) {
      WARN_ICMD("fail to consume and compress to ob20 packet", K(ret), K_(ob20_param));
    } else {
      DEBUG_ICMD("succ to write to client in ob20", K_(ob20_param));
    }
  } else if (protocol_ == ObProxyProtocol::PROTOCOL_NORMAL) {
    // mysql
    int64_t data_size = internal_reader_->read_avail();
    int64_t bytes_written = 0;
    if (OB_FAIL(external_buf_->remove_append(internal_reader_, bytes_written))) {
      ERROR_CMD("Error while remove_append to external_buf_!", "Attempted size", data_size,
                "wrote size", bytes_written, K(ret));
    } else if (OB_UNLIKELY(bytes_written != data_size)) {
      ret = OB_ERR_UNEXPECTED;
      WARN_CMD("unexpected result", "Attempted size", data_size, "wrote size", bytes_written, K(ret));
    } else {
      internal_reader_ = NULL;
      DEBUG_CMD("succ to write to client in mysql", "Attempted size", bytes_written);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("unexpected protocol error", K(ret), K_(protocol));
  }
  
  return ret;
}

} // end of namespace obproxy
} // end of namespace oceanbase
