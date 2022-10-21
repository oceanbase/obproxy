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

#ifndef OB_UROWID_H_
#define OB_UROWID_H_

#include "lib/alloc/alloc_assist.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_macro_utils.h"
#include "common/ob_obj_type.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObProxyPartInfo;
} // proxy

namespace opsql
{
class ObExprResolverResult;
} // opsql
} // obproxy

namespace common
{
class ObObj;
class ObIAllocator;
template <class T>
class ObIArray;

struct ObURowIDData
{
  OB_UNIS_VERSION(1);
friend class TestURowID;
private:
  typedef int (*get_pk_val_func)(const uint8_t *, const int64_t, int64_t &, ObObj &);
  typedef int (*set_pk_val_func)(const ObObj &, uint8_t *, const int64_t, int64_t &);
  static get_pk_val_func inner_get_funcs_[ObMaxType];
  static set_pk_val_func inner_set_funcs_[ObMaxType];
public:
  enum ObURowIDDataVersion {
    INVALID_ROWID_VERSION = 0,
    PK_ROWID_VERSION = 1,
    NO_PK_ROWID_VERSION = 2,
    HEAP_TABLE_ROWID_VERSION = 128,       // 0x80
    EXT_HEAP_TABLE_ROWID_VERSION = 160,   // 0xA0
  };
  const static uint8_t OB40_ROWID_VERSION_OFFSET = 3;
  static constexpr int64_t HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE = 10;
  static constexpr int64_t EXT_HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE = 16;

  static constexpr int64_t HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS = 32;
  static constexpr int64_t EXT_HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS = 56;
  
  int64_t rowid_len_;
  /*
   *  logical rowid (pk rowid and no pk rowid)
   *  +-----------+---------+---------+--------------+
   *  | 1 byte    | dba len | 1 bytye | pk_len bytes |
   *  +-----------+---------+---------+--------------+
   *  | dba len   |  dba    | version |  pk_content  |
   *  +-----------+---------+---------+--------------+
   *
   *  heap-organized table rowid
   *  +--------------+-----------+----------------+
   *  |    3 bit     |   37 bit  |     40 bit     |
   *  +--------------+-----------+----------------+
   *  | version >> 5 | tablet_id | auto_increment |
   *  +--------------+-----------+----------------+
   *
   *  extended heap-organized table rowid
   *  +--------------+-----------+----------------+
   *  |    3 bit     |   61 bit  |     64 bit     |
   *  +--------------+-----------+----------------+
   *  | version >> 5 | tablet_id | auto_increment |
   *  +--------------+-----------+----------------+
   */
  const uint8_t *rowid_content_;

  ObURowIDData()
    : rowid_len_(0), rowid_content_(NULL) {}

  ObURowIDData(int64_t len, const uint8_t *content)
    : rowid_len_(len), rowid_content_(content) {}

  inline uint8_t get_guess_dba_len() const
  {
    // 1 byte for guess dba len
    OB_ASSERT(rowid_len_ >= 1);
    return rowid_content_[0];
  }

  inline const uint8_t *get_guess_dba() const
  {
    OB_ASSERT(rowid_len_ >= 1 + get_guess_dba_len());
    return rowid_content_ + 1;
  }

  inline int64_t get_version_offset() const
  {
    return get_guess_dba_len() + 1;
  }

  inline int64_t get_pk_content_offset() const
  {
    return get_version_offset() + 1;
  }

  inline const uint8_t *get_pk_content() const
  {
    return rowid_content_ + get_pk_content_offset();
  }

  int compare(const ObURowIDData &other) const;
  bool operator ==(const ObURowIDData &other) const;
  bool operator !=(const ObURowIDData &other) const;
  bool operator >(const ObURowIDData &other) const;
  bool operator <(const ObURowIDData &other) const;
  bool operator <=(const ObURowIDData &other) const;
  bool operator >=(const ObURowIDData &other) const;

  inline bool is_physical_rowid() const
  {
    uint8_t version = get_version();
    return HEAP_TABLE_ROWID_VERSION == version || EXT_HEAP_TABLE_ROWID_VERSION == version;
  }

  int64_t needed_base64_buffer_size() const;
  int get_base64_str(char *buf, const int64_t buf_len, int64_t &pos) const;
  static int decode2urowid(const char *input, const int64_t input_len,
                           ObIAllocator &alloc, ObURowIDData &urowid_data);

  static bool is_valid_version(int64_t v);
  bool is_valid_urowid() const;

  int set_rowid_content(const ObIArray<ObObj> &pk_vals,
                        const int64_t version,
                        ObIAllocator &allocator,
                        const int64_t dba_len = 0,
                        const uint8_t* guess_dba = NULL);
  int get_pk_vals(ObIArray<ObObj> &pk_vals);
  int get_rowkey_for_heap_organized_table(ObIArray<ObObj> &rowkey);
  int get_obobj_or_partition_id_from_decoded(obproxy::proxy::ObProxyPartInfo &part_info,
                                             obproxy::opsql::ObExprResolverResult &resolve_result,
                                             int64_t &partition_id);

  // if highest bit is 1, lower 7 bits of version filed indicates how many primar key obj
  static bool is_valid_part_gen_col_version(int64_t v)
  {
    bool bret = true; 
    if ((v > 0xFF) || (0 == (v & 0x80))) {
      bret = false;
    }
    return bret;
  }

  // create table t1(c1 int, c2 int, c3 int as (c1 + c2), primary key(c1, c2))
  //  partition by hash(c3);
  // c3 is not primary key, but it will be serialize into base64 string
  // we need to use version to distinguish this.
  static int get_part_gen_col_version(int64_t rowkey_cnt, int64_t &v)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(0x7F < rowkey_cnt)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      v = rowkey_cnt | 0x80;
    }
    return ret;
  }

  uint8_t get_version() const;

  DECLARE_TO_STRING;
private:
  static int64_t needed_urowid_buf_size(const int64_t base64_str_len);
  static int decode_base64_str(const char *input, const int64_t input_len,
                               uint8_t *output, const int64_t output_len, int64_t &pos);

  inline int64_t get_buf_len() const { return rowid_len_; }
  inline ObObjType get_pk_type(int64_t &pos) const
  {
    OB_ASSERT(pos + 1 <= get_buf_len());
    return static_cast<ObObjType>(rowid_content_[pos++]);
  }
  int get_pk_value(ObObjType obj_type, int64_t &offset, ObObj &pk_val) const;

  template<ObObjType type>
  static int inner_get_pk_value(const uint8_t *rowid_buf, const int64_t rowid_buf_len,
                                int64_t &offset, ObObj &pk_val)
  {
    UNUSED(rowid_buf);
    UNUSED(rowid_buf_len);
    UNUSED(offset);
    UNUSED(pk_val);
    return OB_NOT_SUPPORTED;
  }

  template<ObObjType type>
  static int inner_set_pk_value(const ObObj &pk_val, uint8_t *buffer, const int64_t buf_len,
                                int64_t &pos)
  {
    UNUSED(pk_val);
    UNUSED(buffer);
    UNUSED(buf_len);
    UNUSED(pos);
    return OB_NOT_SUPPORTED;
  }

  static int64_t get_obj_size(const ObObj &pk_val);

  inline void reset()
  {
    rowid_len_ = 0;
    rowid_content_ = NULL;
  }

  int parse_heap_organized_table_rowid(uint64_t &tablet_id, uint64_t &auto_inc) const;
  int parse_ext_heap_organized_table_rowid(uint64_t &tablet_id, uint64_t &auto_inc);

  int get_obobj_from_decoded(obproxy::proxy::ObProxyPartInfo &part_info,
                             obproxy::opsql::ObExprResolverResult &resolve_result);
  
};
} // end namespace common
} // end namespace oceanbase

#endif // !OB_UROWID_H_
