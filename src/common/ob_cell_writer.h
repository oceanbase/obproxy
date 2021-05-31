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

#ifndef OCEANBASE_COMMON_OB_CELL_WRITER_H_
#define OCEANBASE_COMMON_OB_CELL_WRITER_H_
#include "ob_obj_type.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
class ObObj;

/**
 * Four storage types used to distinguish compact formats
 * The actual implementation of DENSE_SPARSE and DENSE_DENSE is that rowkey and ordinary column belong to two rows
 */
enum ObCompactStoreType
{
  SPARSE, //Sparse format type, with column_id
  DENSE, //Dense format type, without column_id
  DENSE_SPARSE, //Each row consists of two parts, the first part is rowkey, dense, and the latter part is sparse.
  DENSE_DENSE, //Each row consists of two parts, the first part is rowkey, which is dense, and the latter part of ordinary columns is also sparse
  INVALID_COMPACT_STORE_TYPE
};

class ObCellWriter
{
public:
  struct CellMeta
  {
    uint8_t type_: 6;
    uint8_t attr_: 2;
    CellMeta() : type_(0), attr_(0) {}
  };
  enum ExtendAttr
  {
    EA_END_FLAG = 0,
    EA_OTHER = 1
  };
  enum ExtendValue
  {
    EV_NOP_ROW = 0,
    EV_DEL_ROW = 1,
    EV_NOT_EXIST_ROW = 2,
    EV_MIN_CELL = 3,
    EV_MAX_CELL = 4
  };
public:
  ObCellWriter();
  virtual ~ObCellWriter() {}
  int init(char *buf, int64_t size, const ObCompactStoreType store_type);
  int append(uint64_t column_id, const ObObj &obj, ObObj *clone_obj = NULL);
  int append(const ObObj &obj, ObObj *clone_obj = NULL);
  void reset();
  void reuse() { pos_ = 0; cell_cnt_ = 0; }
  void set_store_type(const ObCompactStoreType store_type) { store_type_ = store_type; }
  int row_nop();
  int row_delete();
  int row_not_exist();
  int row_finish();
  inline int64_t get_cell_cnt() { return cell_cnt_; }
  inline int64_t size() const { return pos_; }
  inline char *get_buf() const { return buf_; }
private:
  class NumberAllocator
  {
  public:
    NumberAllocator(char *buf, const int64_t buf_size, int64_t &pos);
    char *alloc(const int64_t size);
  private:
    char *buf_;
    int64_t buf_size_;
    int64_t &pos_;
  };
private:
  template<class T>
  int append(const T &value);
  inline int get_int_byte(int64_t int_value, int &bytes);
private:
  char *buf_;
  int64_t buf_size_;
  int64_t pos_;
  int64_t cell_cnt_;
  ObCompactStoreType store_type_;
  bool is_inited_;
};
}//end namespace common
}//end namespace oceanbase
#endif
