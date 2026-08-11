/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_ROW_ITERATOR_
#define OCEANBASE_COMMON_ROW_ITERATOR_

#include "common/ob_row.h"

namespace oceanbase
{
namespace common
{

class ObNewRowIterator
{
public:
  ObNewRowIterator() {}
  virtual ~ObNewRowIterator() {}
  /**
   * get the next row and move the cursor
   *
   * @param row [out]
   *
   * @return OB_ITER_END if end of iteration
   */
  virtual int get_next_row(ObNewRow *&row) = 0;
  /// rewind the iterator
  virtual void reset() = 0;
  TO_STRING_EMPTY();
};

/// wrap one row as an iterator
class ObSingleRowIteratorWrapper: public ObNewRowIterator
{
public:
  ObSingleRowIteratorWrapper();
  ObSingleRowIteratorWrapper(ObNewRow *row);
  virtual ~ObSingleRowIteratorWrapper() {}

  void set_row(ObNewRow *row) { row_ = row; }
  virtual int get_next_row(ObNewRow *&row);
  virtual void reset() { iter_end_ = false; }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSingleRowIteratorWrapper);
private:
  // data members
  ObNewRow *row_;
  bool iter_end_;
};

inline ObSingleRowIteratorWrapper::ObSingleRowIteratorWrapper()
    :row_(NULL),
     iter_end_(false)
{}

inline ObSingleRowIteratorWrapper::ObSingleRowIteratorWrapper(ObNewRow *row)
    :row_(row),
     iter_end_(false)
{}

inline int ObSingleRowIteratorWrapper::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_)) {
    ret = OB_NOT_INIT;
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else {
    row = row_;
    iter_end_ = true;
  }
  return ret;
}

class ObOuterRowIterator
{
public:
  ObOuterRowIterator() {}
  virtual ~ObOuterRowIterator() {}
  /**
   * get the next row and move the cursor
   *
   * @param row [out]
   *
   * @return OB_ITER_END if end of iteration
   */
  virtual int get_next_row(ObNewRow &row) = 0;
  /// rewind the iterator
  virtual void reset() = 0;
};

}
}

#endif //OCEANBASE_COMMON_ROW_ITERATOR_
