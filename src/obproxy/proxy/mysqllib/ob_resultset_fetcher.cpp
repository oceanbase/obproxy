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
#include "proxy/mysqllib/ob_resultset_fetcher.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/packet/ompk_error.h"

using namespace oceanbase::obproxy::event;
using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
int64_t ObMysqlField::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(name),
       K_(org_name),
       K_(table),
       K_(org_table),
       K_(db),
       K_(catalog),
       K_(def),
       K_(pos),
       K_(length),
       K_(max_length),
       K_(flags),
       K_(decimals),
       K_(charsetnr));
  J_OBJ_END();
  return pos;
}

int ObResultSetFetcher::init(ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;
  if ((NULL == reader) || (reader->read_avail() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(reader), K(ret));
  } else if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (FALSE_IT(reader_ = reader)) {
    // impossible
  } else if (OB_FAIL(read_field_count())) {
    LOG_WDIAG("fail to read field count", K(ret));
  } else if (OB_FAIL(read_fields())) {
    LOG_WDIAG("fail to read fields", K(ret));
  } else {
    // alloc row
    row_.column_count_ = field_count_;
    int64_t buf_len = ((sizeof(ObString) + sizeof(ObObj)) * field_count_);
    char *buf = static_cast<char *>(allocator_.alloc(buf_len));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem", K(buf_len), K(ret));
    } else {
      row_.columns_ = new (buf) ObString[field_count_];
      row_.objs_ = new (buf + sizeof(ObString) * field_count_) ObObj[field_count_];
      is_inited_ = true;
    }
  }

  return ret;
}

int ObResultSetFetcher::next()
{
  int ret = OB_SUCCESS;
  ObString body;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_FAIL(read_one_packet(body))) {
    LOG_WDIAG("fail to read one packet", K(ret));
  } else if (is_eof_packet(body)) {
    // the second eof pakcet in ResultSet, data read complete
    ret = OB_ITER_END;
  } else {
    uint16_t mysql_err_code = 0;
    bool is_error_pkt = false;
    if (OB_FAIL(judge_error_packet(body, is_error_pkt, mysql_err_code))) {
      LOG_WDIAG("fail to judge error packet", K(ret));
    } else {
      if (is_error_pkt) {
        // the error packet in ResultSet, data read complete
        // just assign mysql_err_code to ret
        ret = mysql_err_code;
      } else {
        if (OB_FAIL(fill_row_data(body))) {
          LOG_WDIAG("fail to fill row data", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObResultSetFetcher::get_int(const char *col_name, int64_t &int_val) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = -1;
  if (NULL == col_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid col_name", K(col_name), K(ret));
  } else if (OB_FAIL(get_col_idx(col_name, col_idx))) {
    LOG_WDIAG("fail to get col idx", K(col_name), K(ret));
  } else if (OB_FAIL(get_int(col_idx, int_val))) {
    LOG_WDIAG("fail to get int", K(col_name), K(col_idx), K(ret));
  }
  return ret;
}

int ObResultSetFetcher::get_uint(const char *col_name, uint64_t &int_val) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = -1;
  if (NULL == col_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid col_name", K(col_name), K(ret));
  } else if (OB_FAIL(get_col_idx(col_name, col_idx))) {
    LOG_WDIAG("fail to get col idx", K(col_name), K(ret));
  } else if (OB_FAIL(get_uint(col_idx, int_val))) {
    LOG_WDIAG("fail to get uint", K(col_name), K(col_idx), K(ret));
  }
  return ret;
}

int ObResultSetFetcher::get_bool(const char *col_name, bool &bool_val) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = -1;
  if (NULL == col_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid col_name", K(col_name), K(ret));
  } else if (OB_FAIL(get_col_idx(col_name, col_idx))) {
    LOG_WDIAG("fail to get col idx", K(col_name), K(ret));
  } else if (OB_FAIL(get_bool(col_idx, bool_val))) {
    LOG_WDIAG("fail to get bool", K(col_name), K(col_idx), K(ret));
  }
  return ret;
}

int ObResultSetFetcher::get_double(const char *col_name, double &double_val) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = -1;
  if (NULL == col_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid col_name", K(col_name), K(ret));
  } else if (OB_FAIL(get_col_idx(col_name, col_idx))) {
    LOG_WDIAG("fail to get col idx", K(col_name), K(ret));
  } else if (OB_FAIL(get_double(col_idx, double_val))) {
    LOG_WDIAG("fail to get double", K(col_name), K(col_idx), K(ret));
  }
  return ret;
}

int ObResultSetFetcher::get_varchar(const char *col_name, common::ObString &varchar_val) const
{
  int ret = OB_SUCCESS;
  int64_t col_idx = -1;
  if (NULL == col_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid col_name", K(col_name), K(ret));
  } else if (OB_FAIL(get_col_idx(col_name, col_idx))) {
    LOG_WDIAG("fail to get col idx", K(col_name), K(ret));
  } else if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WDIAG("fail to get varchar", K(col_name), K(col_idx), K(ret));
  }
  return ret;
}

int ObResultSetFetcher::get_col_idx(const char *col_name, int64_t &idx) const
{
  int ret = OB_SUCCESS;
  if (NULL == col_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid col_name", K(col_name), K(ret));
  } else {
    ObString col_name_str = ObString::make_string(col_name);
    ObMysqlField *field = NULL;
    ret = column_map_->get_refactored(col_name_str, field);
    if (OB_HASH_NOT_EXIST == ret) {
      LOG_WDIAG("fail to get column", K(col_name_str), K(ret));
      ret = OB_ERR_COLUMN_NOT_FOUND;
    } else if (NULL == field) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("mysql field can not be null", K(field), K(ret));
    } else {
      idx = field->pos_;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObResultSetFetcher::get_bool(const int64_t col_idx, bool &bool_val) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WDIAG("fail to get value", K(col_idx), K(ret));
  } else {
    if (1 != varchar_val.length()) {
      ret = OB_INVALID_DATA;
      LOG_WDIAG("invalid bool value", K(varchar_val), K(ret));
    } else {
      if (0 == STRNCMP("0", varchar_val.ptr(), 1)) {
        bool_val = false;
      } else if (0 == STRNCMP("1", varchar_val.ptr(), 1)) {
        bool_val = true;
      } else {
        ret = OB_INVALID_DATA;
        LOG_WDIAG("invalid bool value", K(varchar_val), K(ret));
      }
    }
  }
  return ret;
}

int ObResultSetFetcher::get_int(const int64_t col_idx, int64_t &int_val) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WDIAG("fail to get value", K(col_idx), K(ret));
  } else if (varchar_val.empty()) {
    ret = OB_INVALID_DATA;
    LOG_WDIAG("invalid empty value", K(varchar_val), K(ret));
  } else {
    int64_t ret_val = 0;
    char int_buf[MAX_UINT64_STORE_LEN + 1];
    int64_t len = std::min(varchar_val.length(),
                           static_cast<ObString::obstr_size_t>(MAX_UINT64_STORE_LEN));
    MEMCPY(int_buf, varchar_val.ptr(), len);
    int_buf[len] = '\0';
    char *end_ptr = NULL;
    ret_val = strtoll(int_buf, &end_ptr, 10);
    if (('\0' != *int_buf ) && ('\0' == *end_ptr)) {
      int_val = ret_val;
    } else {
      ret = OB_INVALID_DATA;
      LOG_WDIAG("invalid int value", K(int_val), K(ret));
    }
  }
  return ret;
}

int ObResultSetFetcher::get_uint(const int64_t col_idx, uint64_t &int_val) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WDIAG("fail to get value", K(col_idx), K(ret));
  } else if (varchar_val.empty()) {
    ret = OB_INVALID_DATA;
    LOG_WDIAG("invalid empty value", K(varchar_val), K(ret));
  } else {
    uint64_t ret_val = 0;
    char int_buf[MAX_UINT64_STORE_LEN + 1];
    int64_t len = std::min(varchar_val.length(),
                           static_cast<ObString::obstr_size_t>(MAX_UINT64_STORE_LEN));
    MEMCPY(int_buf, varchar_val.ptr(), len);
    int_buf[len] = '\0';
    char *end_ptr = NULL;
    ret_val = strtoull(int_buf, &end_ptr, 10);
    if (('\0' != *int_buf ) && ('\0' == *end_ptr)) {
      int_val = ret_val;
    } else {
      ret = OB_INVALID_DATA;
      LOG_WDIAG("invalid uint value", K(int_val), K(ret));
    }
  }
  return ret;
}

int ObResultSetFetcher::get_double(const int64_t col_idx, double &double_val) const
{
  int ret = OB_SUCCESS;
  // some type convertion work
  ObString varchar_val;
  if (OB_FAIL(get_varchar(col_idx, varchar_val))) {
    LOG_WDIAG("fail to get value", K(col_idx), K(ret));
  } else if (varchar_val.empty()) {
    ret = OB_INVALID_DATA;
    LOG_WDIAG("invalid empty value", K(varchar_val), K(ret));
  } else {
    double ret_val = 0.0;
    char int_buf[MAX_UINT64_STORE_LEN + 1];
    int64_t len = std::min(varchar_val.length(),
                           static_cast<ObString::obstr_size_t>(MAX_UINT64_STORE_LEN));
    MEMCPY(int_buf, varchar_val.ptr(), len);
    int_buf[len] = '\0';
    char *end_ptr = NULL;
    ret_val = strtod(int_buf, &end_ptr);
    if (('\0' != *int_buf ) && ('\0' == *end_ptr)) {
      double_val = ret_val;
    } else {
      ret = OB_INVALID_DATA;
      LOG_WDIAG("invalid dobule value", K(double_val), K(varchar_val), K(ret));
    }
  }
  return ret;
}

int ObResultSetFetcher::get_varchar(const int64_t col_idx, common::ObString &varchar_val) const
{
  int ret = OB_SUCCESS;
  if (col_idx < 0 || col_idx >= field_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid index", K(col_idx), K_(field_count), K(ret));
  } else if (!row_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to check cur row", K(ret));
  } else {
    varchar_val = row_.columns_[col_idx];
  }

  return ret;
}

int ObResultSetFetcher::get_obj(const int64_t col_idx, common::ObObj &obj) const
{
  int ret = OB_SUCCESS;
  if (col_idx < 0 || col_idx >= field_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid index", K(col_idx), K_(field_count), K(ret));
  } else if (!row_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to check cur row", K(ret));
  } else {
    obj = row_.objs_[col_idx];
  }

  return ret;
}

int ObResultSetFetcher::fill_row_data(ObString &body)
{
  int ret = OB_SUCCESS;
  if (body.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(body), K(ret));
  } else {
    row_.reset_row_content();
    const char *pos = body.ptr();
    char *end = body.ptr() + body.length();
    uint64_t len = 0;
    for (int64_t i = 0; (i < field_count_) && (OB_SUCC(ret)); ++i) {
      len = 0;
      if (OB_FAIL(ObMySQLUtil::get_length(pos, len))) {
        LOG_WDIAG("fill to get length", KP(pos), K(ret));
      } else if (ObMySQLUtil::NULL_ == len) { // NULL or empty
        row_.columns_[i].reset();
        row_.objs_[i].set_null();
      } else if (0 == len) { // NULL or empty
        row_.columns_[i].reset();
        row_.objs_[i].set_varchar(row_.columns_[i]);
      } else {
        row_.columns_[i].assign_ptr(pos, static_cast<ObString::obstr_size_t>(len));
        row_.objs_[i].set_varchar(row_.columns_[i]);
        pos += len;
      }
    }

    if (pos != end) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("pos must be equal end", KP(pos), KP(end), K(body), K(ret));
    }
  }

  return ret;
}

int ObResultSetFetcher::read_field_count()
{
  int ret = OB_SUCCESS;
  ObString body;
  if (OB_FAIL(read_one_packet(body))) {
    LOG_WDIAG("fail to read body", K(ret));
  } else {
    if (is_ok_packet(body)) {
      ret = OB_INVALID_DATA;
      LOG_WDIAG("the first packet of ResultSet can not be OK Packet", K(ret));
    } else {
      bool is_error_pkt = false;
      uint16_t mysql_error_code = 0;
      if (OB_FAIL(judge_error_packet(body, is_error_pkt, mysql_error_code))) {
        LOG_WDIAG("fail to judge error packet", K(ret));
      } else {
        if (is_error_pkt) {
          // the fist packet in Reusltset is error pakcet, data read complete
          // just assign mysql_err_code to ret
          ret = mysql_error_code;
        }
      }
    }
    if (OB_SUCC(ret)) {
      const char *pos = body.ptr();
      uint64_t field_count = 0;
      if (OB_FAIL(ObMySQLUtil::get_length(pos,  field_count))) {
        LOG_WDIAG("fail to get len encode number", K(ret));
      } else if (field_count <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid field count", K_(field_count), K(ret));
      } else {
        field_count_ = static_cast<int64_t>(field_count);
        // alloc ObMysqlField array;
        int64_t buf_len = field_count_ * sizeof(ObMysqlField);
        char *buf = static_cast<char *>(allocator_.alloc(buf_len));
        if (NULL == buf) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc memory", K(buf_len), K(ret));
        } else {
          // it will be free when page_arena_ destruct
          field_ = new (buf) ObMysqlField[field_count_];
        }

        // alloc ObColumnMap
        if (OB_SUCC(ret)) {
          buf_len = ObColumnMap::get_hash_array_mem_size(field_count_);
          buf = static_cast<char *>(allocator_.alloc(buf_len));
          if (NULL == buf) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WDIAG("fail to alloc memory", K(buf_len), K(ret));
          } else {
            // it will be free when page_arena_ destruct
            column_map_ = new (buf) ObColumnMap(buf_len);
          }
        }
      }
    }
  }

  return ret;
}

int ObResultSetFetcher::read_fields()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; (i < field_count_) && (OB_SUCC(ret)); ++i) {
    ObString body;
    if (OB_FAIL(read_one_packet(body))) {
      LOG_WDIAG("fail to read one packet", K(i), K(ret));
    } else if (is_eof_packet(body)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("the packet must be not EOF packet", K(body), K(ret));
    } else if (OB_FAIL(fill_field(body, &field_[i]))) {
      LOG_WDIAG("fail to fill field", K(body), K(ret));
    } else if (OB_FAIL(add_field(i))) {
      LOG_WDIAG("fail to add field", K(i), K(body), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObString body;
    if (OB_FAIL(read_one_packet(body))) {
      LOG_WDIAG("fail to read one packet", K(ret));
    } else if (!is_eof_packet(body)) { // the first EOF Packet in Resultset
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("the packet must be EOF packet", K(body), K(ret));
    }
  }

  return ret;
}

int ObResultSetFetcher::fill_field(ObString &body, ObMysqlField *field)
{
  int ret = OB_SUCCESS;
  if (body.empty() || NULL == field) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(body), K(field), K(ret));
  } else {
    const char *pos = body.ptr();
    char *end = body.ptr() + body.length();
    if (OB_FAIL(assign_string(field->catalog_, pos))) {
      LOG_WDIAG("fail to assign_string(catalog)", KP(pos), K(ret));
    } else if (OB_FAIL(assign_string(field->db_, pos))) {
      LOG_WDIAG("fail to assign_string(db)", KP(pos), K(ret));
    } else if (OB_FAIL(assign_string(field->table_, pos))) {
      LOG_WDIAG("fail to assign_string(table)", KP(pos), K(ret));
    } else if (OB_FAIL(assign_string(field->org_table_, pos))) {
      LOG_WDIAG("fail to assign_string(org_table)", KP(pos), K(ret));
    } else if (OB_FAIL(assign_string(field->name_, pos))) {
      LOG_WDIAG("fail to assign_string(name)", KP(pos), K(ret));
    } else if (OB_FAIL(assign_string(field->org_name_, pos))) {
      LOG_WDIAG("fail to assign_string(org_name)", KP(pos), K(ret));
    } else {
      uint64_t len = 0;
      if (OB_FAIL(ObMySQLUtil::get_length(pos, len))) {
        LOG_WDIAG("fail to get length", KP(pos), K(ret));
      } else {
        uint16_t value = 0;
        ObMySQLUtil::get_uint2(pos, value);
        field->charsetnr_ = value;

        uint32_t length_value = 0;
        ObMySQLUtil::get_uint4(pos, length_value);
        field->length_ = length_value;

        uint8_t type_value = 0;
        ObMySQLUtil::get_uint1(pos, type_value);
        field->type_ = (EMySQLFieldType)(type_value);

        value = 0;
        ObMySQLUtil::get_uint2(pos, value);
        field->flags_ = value;

        uint8_t decimals_value = 0;
        ObMySQLUtil::get_uint1(pos, decimals_value);
        field->decimals_ = decimals_value;
      }
    }

    if (OB_SUCC(ret)) {
      if (pos > end) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("pos must be <= end", KP(pos), KP(end), K(body), K(ret));
      }
    }
  }

  return ret;
}

int ObResultSetFetcher::assign_string(ObString &str, const char *&pos)
{
   uint64_t len = 0;
  int ret = OB_SUCCESS;
  if (NULL == pos) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(pos), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::get_length(pos, len))) {
    LOG_WDIAG("fail to get length", K(ret));
  } else {
    str.assign_ptr(pos, static_cast<uint32_t>(len));
    pos += len;
  }

  return ret;
}

int ObResultSetFetcher::add_field(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= field_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(idx), K(ret));
  } else {
    if (field_[idx].name_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected field", "field", field_[idx], K(ret));
    } else {
      field_[idx].pos_ = idx;
      ret = column_map_->set_refactored(field_[idx].name_, &field_[idx]); // do not overwrite
      if (OB_SUCCESS == ret) {
      } else if (OB_HASH_EXIST == ret) {
        LOG_DEBUG("resultset has the same two column names, covert it to OB_SUCCESS",
                 "name", field_[idx].name_, K(idx));
        ret = OB_SUCCESS;
      } else {
        LOG_WDIAG("fail to set name to hashtable", "name", field_[idx].name_, K(idx), K(ret));
      }
    }
  }
  return ret;
}

bool ObResultSetFetcher::is_eof_packet(const ObString &packet_body) const
{
  bool bret = false;
  if (!packet_body.empty()) {
    int64_t type = static_cast<uint8_t>(packet_body.ptr()[0]);
    if ((MYSQL_EOF_PACKET_TYPE == type) && (packet_body.length() < MYSQL_MAX_EOF_PACKET_LEN)) {
      bret = true;
    }
  }
  return bret;
}

bool ObResultSetFetcher::is_ok_packet(const ObString &packet_body) const
{
  bool bret = false;
  if (!packet_body.empty()) {
    int64_t type = static_cast<uint8_t>(packet_body.ptr()[0]);
    if ((MYSQL_OK_PACKET_TYPE == type) && (packet_body.length() >= 7)) {
      bret = true;
    }
  }
  return bret;
}

int ObResultSetFetcher::judge_error_packet(
    const ObString &packet_body,
    bool &is_err_pkt,
    uint16_t &mysql_error_code) const
{
  int ret = OB_SUCCESS;
  is_err_pkt = false;
  if (packet_body.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("packet_body can not be empty", K(ret));
  } else {
    int64_t type = static_cast<uint8_t>(packet_body.ptr()[0]);
    if (MYSQL_ERR_PACKET_TYPE == type) {
      OMPKError error;
      error.set_content(packet_body.ptr(), packet_body.length());
      if (OB_FAIL(error.decode())) {
        LOG_WDIAG("fail to decode error packet", K(ret));
      } else {
        LOG_WDIAG("get error packet in ResultSet", K(error));
        mysql_error_code = error.get_err_code();
      }
      is_err_pkt = true;
    }
  }

  return ret;
}

int ObResultSetFetcher::read_one_packet(ObString &packet_body)
{
  int ret = OB_SUCCESS;
  int64_t packet_body_len = 0;
  ObString body;
  if (OB_FAIL(read_header(packet_body_len))) {
    LOG_WDIAG("fail to read header", K(ret));
  } else if (OB_FAIL(read_body(packet_body_len, body))) {
    LOG_WDIAG("fail to read body", K(packet_body_len), K(ret));
  } else {
    packet_body = body;
  }

  return ret;
}

int ObResultSetFetcher::read_header(int64_t &packet_body_len)
{
  int ret = OB_SUCCESS;
  ObString header;
  if (OB_FAIL(do_read(MYSQL_NET_HEADER_LENGTH, header))) {
    LOG_WDIAG("fail to do read", K(ret));
  } else {
    uint32_t pkt_body_len = 0;
    const char *pos = header.ptr();
    ObMySQLUtil::get_uint3(pos, pkt_body_len);
    packet_body_len = pkt_body_len;
  }

  return ret;
}

int ObResultSetFetcher::read_body(const int64_t packet_body_len, ObString &packet_body)
{
  int ret = OB_SUCCESS;
  ObString body;
  if (packet_body_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(packet_body_len), K(ret));
  } else if (OB_FAIL(do_read(packet_body_len, body))) {
    LOG_WDIAG("fail to do read", K(packet_body_len), K(ret));
  } else {
    packet_body = body;
  }
  return ret;
}

int ObResultSetFetcher::do_read(const int64_t len, common::ObString &data)
{
  int ret = OB_SUCCESS;
  if (len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(len), K(ret));
  } else {
    data.reset();
    // get first io block
    if (NULL == cur_block_) {
      if (NULL != reader_->block_) {
        reader_->skip_empty_blocks();
        cur_block_ = reader_->block_;
        cur_offset_ = reader_->start_offset_;
      }
    }

    int64_t to_read = len;
    char *data_start = NULL;
    char *alloc_buf = NULL;
    int64_t block_data_avail = 0;
    char *copy_des = NULL;
    char *copy_src = NULL;
    int64_t copy_size = 0;
    while ((NULL != cur_block_) && (to_read > 0) && (OB_SUCC(ret))) {
      block_data_avail = cur_block_->read_avail() - cur_offset_;
      if (block_data_avail < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("block_data_avail must not < 0", K(block_data_avail), K(ret));
      } else if (0 == block_data_avail) {
        // nothing, continue ...
      } else {
        if (block_data_avail >= to_read) {
          if (NULL != alloc_buf) {
            // cross multi IObufferBlocks
            char *copy_des = alloc_buf + len - to_read;
            char *copy_src = cur_block_->start() + cur_offset_;
            MEMCPY(copy_des, copy_src, to_read);
          } else {
            // in signle one IOBufferBlock
            data_start = cur_block_->start() + cur_offset_;
          }
          cur_offset_ += to_read;
          to_read = 0;
        } else {
          // cross multi IObufferBlocks
          if (NULL == alloc_buf) {
            if (NULL == (alloc_buf = static_cast<char *>(allocator_.alloc(to_read)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WDIAG("fail to alloc mem", "alloc size", to_read, K(ret));
            } else {
              LOG_DEBUG("cross multi IObufferBlocks, alloc new buf", "alloc size", to_read);
              data_start = alloc_buf;
            }
          }
          if (OB_SUCC(ret)) {
            copy_des = alloc_buf + len - to_read;
            copy_src = cur_block_->start() + cur_offset_;
            copy_size = block_data_avail;
            MEMCPY(copy_des, copy_src, copy_size);
            cur_offset_ += copy_size;
            to_read -= copy_size;
          }
        }
      }

      int64_t remain = cur_block_->read_avail() - cur_offset_;
      if (0 == remain) {
        // move to next IOBufferBlock
        cur_offset_ = 0;
        cur_block_ = cur_block_->next_;
      } else if (remain < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unknown error", K(remain), K(ret));
      } else if (remain > 0) {
        // nothing, continue
      }
    }

    if (OB_SUCC(ret)) {
      if ((0 == to_read) && (NULL != data_start)) {
        data.assign_ptr(data_start, static_cast<ObString::obstr_size_t>(len));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("read error unexpected", K(to_read), KP(data_start), K(ret));
      }
    }
  }

  return ret;
}

int ObResultSetFetcher::print_info() const
{
  int ret = OB_SUCCESS;
  if (!row_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to check cur row or length");
  } else {
    for (int64_t i = 0; i < row_.column_count_; ++i) {
      _LOG_INFO("cell_idx: %ld, cell_name: %.*s, cell_value:%.*s", i,
                field_[i].name_.length(), field_[i].name_.ptr(),
                row_.columns_[i].length(), row_.columns_[i].ptr());
    }
  }
  return ret;
}

} // end of namespace obproxy
} // end of namespace oceanbase
