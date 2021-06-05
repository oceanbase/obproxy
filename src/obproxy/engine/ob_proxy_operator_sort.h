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

#ifndef OBPROXY_OB_PROXY_OPERATOR_SORT_H
#define OBPROXY_OB_PROXY_OPERATOR_SORT_H

#include "ob_proxy_operator.h"
#include "common/ob_row.h"

namespace oceanbase {
namespace obproxy {
namespace engine {


class ObSortColumn;
class ObBaseSort;
class ObRowTrunk;
typedef common::ObSEArray<ObSortColumn *, 4, common::ObIAllocator&> SortColumnArray;
class ObProxySortOp : public ObProxyOperator
{
public:
/*
  ObProxySortOp() : ObProxyOperator(), sort_columns_(), sort_imp_(NULL),
                    added_order_by_objs_(false)
  { set_op_type(PHY_SORT); }
*/
  ObProxySortOp(ObProxyOpInput *input, common::ObIAllocator &allocator)
    : ObProxyOperator(input, allocator), sort_columns_(NULL), sort_imp_(NULL),
      added_order_by_objs_(false)
  {
    set_op_type(PHY_SORT);
  }

  void set_added_order_by_cols(bool added) { added_order_by_objs_ = added; }
  bool get_added_order_by_cols() { return added_order_by_objs_; }

  virtual int add_order_by_obj(ResultRow *row);
  virtual int remove_all_order_by_objs(ResultRows &rows);

  virtual ~ObProxySortOp();

  virtual int init();
  virtual int init_sort_columns();

  virtual int get_next_row();

  virtual void set_sort_impl(ObBaseSort *sort) { sort_imp_ = sort; }
  virtual ObBaseSort* get_sort_impl() { return sort_imp_; }

protected:
  SortColumnArray *sort_columns_;
  ObBaseSort *sort_imp_;
  bool added_order_by_objs_;
};

class ObProxySortInput : public ObProxyOpInput
{
public:
  ObProxySortInput()
    : ObProxyOpInput(),
      order_by_expr_(ObModIds::OB_SE_ARRAY_ENGINE, array_new_alloc_size) {}
  ObProxySortInput(const common::ObSEArray<ObProxyExpr*, 4> &select_exprs,
           const common::ObSEArray<ObProxyExpr*, 4> &order_by_expr)
    : ObProxyOpInput(select_exprs), order_by_expr_(order_by_expr)
  {}

  void set_order_by_expr(const common::ObSEArray<ObProxyExpr*, 4> &select_exprs)
  {
    select_exprs_ = select_exprs;
  }
  common::ObSEArray<ObProxyExpr*, 4>& get_order_by_expr()
  {
    return order_by_expr_;
  }

protected:
  common::ObSEArray<ObProxyExpr*, 4> order_by_expr_;
};

class ObProxyMemSortOp : public ObProxySortOp
{
public:

  ObProxyMemSortOp(ObProxyOpInput *input, common::ObIAllocator &allocator)
    : ObProxySortOp(input, allocator) {
    set_op_type(PHY_MERGE_SORT);
  }

  ~ObProxyMemSortOp() {};
  virtual int get_next_row();
  virtual int handle_response_result(void *src, bool is_final, ObProxyResultResp *&result);

};

class ObProxyTopKOp : public ObProxySortOp
{
public:
  ObProxyTopKOp(ObProxyOpInput *input, common::ObIAllocator &allocator)
    : ObProxySortOp(input, allocator) {
    set_op_type(PHY_TOPK);
  }

  ~ObProxyTopKOp() {};
  virtual int get_next_row();
  virtual int handle_response_result(void *src, bool is_final, ObProxyResultResp *&result);
};

class ObSortColumn : public common::ObColumnInfo
{
public:
  bool is_ascending_;
  ObProxyOrderItem *order_by_expr_;
  ObSortColumn()
      : ObColumnInfo(),
        is_ascending_(true),
        order_by_expr_(NULL)
  {
  }
  ObSortColumn(int64_t index, common::ObCollationType cs_type, bool is_asc)
  {
    index_ = index;
    cs_type_ = cs_type; //not used
    is_ascending_ = is_asc;
    order_by_expr_ = NULL;
  }
  TO_STRING_KV3(N_INDEX_ID, index_,
      N_COLLATION_TYPE, common::ObCharset::collation_name(cs_type_),
      N_ASCENDING, is_ascending_ ? N_ASC : N_DESC);
};

class ObBaseSort
{
public:
  ObBaseSort(SortColumnArray &sort_columns, common::ObIAllocator &allocator_, ResultRows &sort_rows);
  virtual ~ObBaseSort();

  virtual void set_sort_columns(SortColumnArray &sort_columns)
  {
    sort_columns_.assign(sort_columns);
  }

  virtual const common::ObIArray<ObSortColumn*>& get_sort_columns()
  {
    return sort_columns_;
  }

  virtual void set_sort_rows(ResultRows &sort_rows)
  {
    sort_rows_ = sort_rows;
  }

  virtual ResultRows& get_sort_rows()
  {
    return sort_rows_;
  }

  virtual int sort_rows() = 0;
  bool compare_row(ResultRow &row1, ResultRow &row2, int &ret);
  virtual int add_row(ResultRow *row);
  virtual int fetch_final_results(ResultRows &rows);

  inline void swap_index(int64_t *l, int64_t *r);
  inline void swap_row(ResultRow *&l, ResultRow *&r);
  int64_t get_row_count() { return row_count_; }
  int64_t get_topn_cnt() { return topn_cnt_; }
  void set_topn_cnt(int64_t count) { topn_cnt_ = count; }
  bool get_sorted() { return sorted_;}
  void set_sorted(bool sorted) { sorted_ = sorted; }

protected:
  common::ObIAllocator &allocator_;
  SortColumnArray &sort_columns_;
  ResultRows sort_rows_;
  int64_t topn_cnt_;
  int64_t row_count_;
  bool sorted_;
  DISALLOW_COPY_AND_ASSIGN(ObBaseSort);
  int *err_;
  int *sort_err_;
};

class ObMemorySort : public ObBaseSort
{
public:
  ObMemorySort(SortColumnArray &sort_column, common::ObIAllocator &allocator, ResultRows &sort_rows)
    : ObBaseSort(sort_column, allocator, sort_rows) {}
  ~ObMemorySort() {}
  virtual int sort_rows();
  int64_t partition(int64_t low, int64_t height);
  int quick_sort(int64_t low, int64_t height);
};

class ObTopKSort : public ObBaseSort
{
public:
  ObTopKSort(SortColumnArray &sort_column, common::ObIAllocator &allocator,
    ResultRows &sort_rows, ResultRows &rows)
    : ObBaseSort(sort_column, allocator, sort_rows),
                row_count_(0), sort_rows_heap_(rows) {}

  ~ObTopKSort() {}

  virtual int sort_rows();
  virtual int fetch_final_results();
  virtual int sort_rows(ResultRow &row);
  virtual int build_heap();
  virtual int heap_adjust(int64_t p, int64_t len);
  virtual int heap_sort();

protected:
  int64_t row_count_;
  ResultRows &sort_rows_heap_;
};

/*
class ObMergeSort : public ObBaseSort
{
public:
  ObMergeSort() {
    sort_regions_ = 0;
  }

  ObMergeSort(common::ObSEArray<ObRowTrunk*, 4> &merge_sort_rows_array, int64_t regions)
    : merge_sort_rows_array_(merge_sort_rows_array), sort_regions_(regions)
  {
    merge_rows_index_.push_back(static_cast<int64_t>(0));
  }

//  ObMergeSort(common::ObIArray<ObRowTrunk> &merge_sort_rows_array, int64_t regions)
//    :merge_sort_rows_array_(merge_sort_rows_array), sort_regions_(regions) {
//    for (int64_t i = 0; i < sort_regions_; i++) {
//      merge_rows_index_.push_back(0);
//    }
//  }
//
  ~ObMergeSort() {}
  virtual int sort_rows();
  int merge_sort();
protected:
  //common::ObIArray<ObRowTrunk> merge_sort_rows_array_;
  //common:ObIArray<int64_t> merge_rows_index_;
  common::ObSEArray<ObRowTrunk*, 4> merge_sort_rows_array_;

  common::ObSEArray<int64_t, 4> merge_rows_index_;

  int64_t sort_regions_;
};

*/

}
}
}

#endif //OBPROXY_OB_PROXY_OPERATOR_SORT_H
