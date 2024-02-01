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

#ifndef OBPROXY_RESULTSET_FETCHER_H
#define OBPROXY_RESULTSET_FETCHER_H

#include "rpc/obmysql/ob_mysql_global.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/allocator/page_arena.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "common/ob_object.h"


#define PROXY_EXTRACT_INT_FIELD_MYSQL(result, column_name, field, type) \
  if (OB_SUCCESS == ret) { \
    int64_t int_value = 0; \
    if (OB_FAIL((result).get_int(column_name, int_value))) { \
      PROXY_LOG(WDIAG, "fail to int in row", K(column_name), K(ret)); \
    } else { \
      field = static_cast<type>(int_value); \
    }\
  }

#define PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result, column_name, field) \
  if (OB_SUCCESS == ret) { \
    if (OB_FAIL((result).get_varchar(column_name, field))) { \
      PROXY_LOG(WDIAG, "fail to get varchar in row", K(column_name), K(ret)); \
    } \
  }

#define PROXY_EXTRACT_BOOL_FIELD_MYSQL(result, column_name, field) \
  if (OB_SUCCESS == ret) { \
    if (OB_FAIL((result).get_bool(column_name, field))) { \
      PROXY_LOG(WDIAG, "fail to get bool in row", K(column_name), K(ret)); \
    } \
  }

// limit max length
#define PROXY_EXTRACT_STRBUF_FIELD_MYSQL(result, column_name, field, max_length, real_length) \
  if (OB_SUCCESS == ret) { \
    ObString str_value; \
    if (OB_SUCCESS == (ret = (result).get_varchar(column_name, str_value))) { \
      if(str_value.length() >= max_length) { \
        ret = OB_SIZE_OVERFLOW; \
        PROXY_LOG(WDIAG, "field max length is not enough:", \
                 "max length", max_length, "str length", str_value.length()); \
      } else if (str_value.empty()) {\
        real_length = 0;             \
        field[0] = '\0'; \
      } else { \
        MEMCPY(field, str_value.ptr(), str_value.length()); \
        real_length = str_value.length();                   \
        field[str_value.length()] = '\0'; \
      } \
    } else { \
      PROXY_LOG(WDIAG, "fail to extract strbuf field mysql", K(column_name), K(real_length), K(ret)); \
    } \
  }

// unlimit max length
#define PROXY_EXTRACT_STRBUF_FIELD_MYSQL_UNLIMIT_LENGTH(result, column_name, field, real_length, allocator) \
if (OB_SUCCESS == ret) { \
  ObString str_value; \
  if (OB_SUCCESS == (ret = (result).get_varchar(column_name, str_value))) { \
    if (str_value.empty()) { \
      real_length = 0;             \
      field = NULL; \
    } else if (OB_ISNULL(field = static_cast<char *>(allocator.alloc(str_value.length() + 1)))) { \
      ret = OB_ALLOCATE_MEMORY_FAILED; \
      LOG_WDIAG("fail to allc part key name", K(field), K(str_value.length()), K(ret)); \
    } else { \
      MEMCPY(field, str_value.ptr(), str_value.length()); \
      real_length = str_value.length();                   \
      field[str_value.length()] = '\0'; \
    } \
  } else { \
    PROXY_LOG(WDIAG, "fail to extract strbuf field mysql", K(column_name), K(real_length), K(ret)); \
  } \
}

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace obproxy
{

struct ObMysqlField
{
  ObMysqlField()
    : name_(), org_name_(), table_(), org_table_(), db_(),
      catalog_(), def_(), pos_(0), length_(0), max_length_(0), flags_(0),
      decimals_(0), charsetnr_(0), type_(obmysql::OB_MYSQL_TYPE_NOT_DEFINED),
      extension_(NULL) {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  common::ObString get_name() { return name_; }

  common::ObString name_;                   // Name of column
  common::ObString org_name_;               // Original column name, if an alias
  common::ObString table_;                  // Table of column if column was a field
  common::ObString org_table_;              // Org table name, if table was an alias
  common::ObString db_;                     // Database for table
  common::ObString catalog_;                // Catalog for table
  common::ObString def_;                    // Default value (set by mysql_list_fields)
  int64_t pos_;                             // the pos in field list
  uint64_t length_;                         // Width of column (create length)
  uint64_t max_length_;                     // Max width for selected set
  uint64_t flags_;                          // Div flags
  uint64_t decimals_;                       // Number of decimals in field
  uint64_t charsetnr_;                      // Character set
  obmysql::EMySQLFieldType type_;           // Type of field. See mysql_com.h for types
  void *extension_;

};

struct ObMysqlRow
{
  ObMysqlRow() { reset(); }
  ~ObMysqlRow() { reset(); }
  void reset_row_content();
  bool is_valid() const { return ((NULL != columns_) && (NULL != objs_) && (column_count_ > 0)); }

  common::ObString *columns_;
  common::ObObj *objs_;
  int64_t column_count_;

private:
  void reset();
};

inline void ObMysqlRow::reset_row_content()
{
  if (column_count_ > 0) {
    if (NULL != columns_) {
      memset(columns_, 0, (column_count_ * sizeof(common::ObString)));
    }
    if (NULL != objs_) {
      memset(objs_, 0, (column_count_ * sizeof(common::ObObj)));
    }
  }
}

inline void ObMysqlRow::reset()
{
  columns_ = NULL;
  objs_ = NULL;
  column_count_ = 0;
}

template <class K, class V>
struct ObGetFieldKey
{
  K operator()(const V value) const
  {
    return value->get_name();
  }
};

typedef common::hash::ObPointerHashArray<common::ObString, ObMysqlField*, ObGetFieldKey> ObColumnMap;

class ObResultSetFetcher
{
public:
  ObResultSetFetcher()
    : is_inited_(false), field_(NULL), field_count_(0),
      row_(), reader_(NULL), cur_block_(NULL), cur_offset_(0),
      allocator_(common::ObModIds::OB_PROXY_RESULTSET_FETCHER, PAGE_SIZE_4K),
      column_map_(NULL) {}
  ~ObResultSetFetcher() {}

  int init(event::ObIOBufferReader *reader);
  // move result cursor to next row, until OB_ITER_END return
  int next();

  int64_t get_column_count() const { return field_count_; }
  ObMysqlField *get_field() const { return field_; }

  // get value by col_name
  int get_int(const char *col_name, int64_t &int_val) const;
  int get_uint(const char *col_name, uint64_t &int_val) const;
  int get_bool(const char *col_name, bool &bool_val) const;
  int get_varchar(const char *col_name, common::ObString &varchar_val) const;
  int get_double(const char *col_name, double &double_val) const;

  // get value by col_idx
  int get_int(const int64_t col_idx, int64_t &int_val) const;
  int get_uint(const int64_t col_idx, uint64_t &int_val) const;
  int get_bool(const int64_t col_idx, bool &bool_val) const;
  int get_varchar(const int64_t col_idx, common::ObString &varchar_val) const;
  int get_double(const int64_t col_idx, double &double_val) const;

  int get_obj(const int64_t col_idx, common::ObObj &obj) const;

  // debug function
  int print_info() const;

private:
  int read_field_count();
  int read_fields();
  int fill_field(common::ObString &body, ObMysqlField *field);
  int add_field(const int64_t field_idx);
  bool is_eof_packet(const common::ObString &packet_body) const;
  bool is_ok_packet(const common::ObString &packet_body) const;
  int judge_error_packet(const common::ObString &packet_body,
                         bool &is_err_pkt, uint16_t &mysql_error_code) const;
  int get_col_idx(const char *col_name, int64_t &idx) const;
  int assign_string(common::ObString &str, const char *&pos);

  int fill_row_data(common::ObString &body);
  int read_one_packet(common::ObString &packet_body);
  int read_header(int64_t &packet_body_len);
  int read_body(const int64_t packet_body_len, common::ObString &packet_body);
  int do_read(const int64_t len, common::ObString &data);

private:
  static const int64_t MAX_UINT64_STORE_LEN = 32;
  static const int64_t PAGE_SIZE_4K = (1LL << 12); // 4K

  bool is_inited_;
  ObMysqlField *field_;
  int64_t field_count_;

  ObMysqlRow row_;

  event::ObIOBufferReader *reader_;
  event::ObIOBufferBlock *cur_block_;
  int64_t cur_offset_;
  common::ObArenaAllocator allocator_;
  ObColumnMap *column_map_;
};

} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_RESULTSET_FETCHER_H
