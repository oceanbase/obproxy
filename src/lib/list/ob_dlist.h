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

#ifndef OCEANBASE_LIB_LIST_OB_DLIST_H_
#define OCEANBASE_LIB_LIST_OB_DLIST_H_

#include "lib/ob_define.h"
#include "lib/tbsys.h"
#include "lib/list/ob_dlink_node.h"

namespace oceanbase
{
namespace common
{
// DLinkNode should be a derived class of ObDLinkBase. See also ObDLinkNode and ObDLinkDerived
template <typename DLinkNode>
class ObDList
{
public:
  typedef DLinkNode node_t;

  ObDList();
  ~ObDList();
  int shadow_copy(const ObDList &list)
  {
    header_ = list.header_;
    size_ = list.size_;
    return common::OB_SUCCESS;
  }

  // get the header
  DLinkNode *get_header() {return static_cast<DLinkNode *>(&header_);}
  const DLinkNode *get_header() const {return static_cast<const DLinkNode *>(&header_);}

  //get the last node
  DLinkNode *get_last() { return header_.prev_; }
  const DLinkNode *get_last() const { return header_.prev_; }

  //get the first node
  DLinkNode *get_first() { return header_.next_; }
  const DLinkNode *get_first() const { return header_.next_; }
  const DLinkNode *get_first_const() const { return header_.next_; }

  // insert the node to the tail
  bool add_last(DLinkNode *e);
  // insert the node to the head
  bool add_first(DLinkNode *e);
  bool increasing_add(DLinkNode *e);
  // move the node to the head
  bool move_to_first(DLinkNode *e);
  // move the node to the tail
  bool move_to_last(DLinkNode *e);

  // remove the node at tail
  DLinkNode *remove_last();
  // remove the node at head
  DLinkNode *remove_first();

  void push_range(ObDList<DLinkNode> &range);
  void pop_range(int32_t num, ObDList<DLinkNode> &range);

  //the list is empty or not
  bool is_empty() const { return header_.next_ == static_cast<const DLinkNode *>(&header_); }
  // get the list size
  int32_t get_size() const { return size_; }

  DLinkNode *remove(DLinkNode *e);

  void clear()
  {
    header_.next_ = static_cast<DLinkNode *>(&header_);
    header_.prev_ = static_cast<DLinkNode *>(&header_);
    size_ = 0;
  }
  void reset()
  { clear(); }
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDList);
private:
  ObDLinkBase<DLinkNode> header_;
  int32_t  size_;
};

template <typename DLinkNode>
ObDList<DLinkNode>::ObDList()
{
  header_.next_ = static_cast<DLinkNode *>(&header_);
  header_.prev_ = static_cast<DLinkNode *>(&header_);
  size_ = 0;
}

template <typename DLinkNode>
ObDList<DLinkNode>::~ObDList()
{
}

template <typename DLinkNode>
bool ObDList<DLinkNode>::add_last(DLinkNode *e)
{
  bool ret = true;
  if (OB_ISNULL(e)) {
    ret = false;
    LIB_LOG(EDIAG, "the poniter is null",K(e));
  } else if (OB_UNLIKELY(e->get_prev() != NULL
             || e->get_next() != NULL)) {
    ret = false;
    LIB_LOG(EDIAG, "link node is not alone",
              K(e->get_prev()), K(e->get_next()), K(e));
  } else {
    header_.add_before(e);
    ++size_;
  }
  return ret;
}

// insert the node to the head
template <typename DLinkNode>
bool ObDList<DLinkNode>::add_first(DLinkNode *e)
{
  bool ret = true;
  if (OB_ISNULL(e)) {
    ret = false;
    LIB_LOG(EDIAG, "the poniter is null",K(e));
  } else if (e->get_prev() != NULL
             || e->get_next() != NULL) {
    ret = false;
    LIB_LOG(EDIAG, "link node is not alone",
              K(e->get_prev()), K(e->get_next()), K(e));
  } else {
    header_.add_after(e);
    ++size_;
  }
  return ret;
}

template <typename DLinkNode>
bool ObDList<DLinkNode>::increasing_add(DLinkNode *e)
{
  bool bret = true;
  if (OB_ISNULL(e)) {
    bret = false;
  } else {
    DLinkNode *cur_node = header_.get_next();
    while (cur_node != &header_) {
      if (*e <= *cur_node) {
        cur_node = cur_node->get_next();
      } else {
        break;
      }
    }
    cur_node->add_before(e);
    ++size_;
  }
  return bret;
}

// move the node to the head
template <typename DLinkNode>
bool ObDList<DLinkNode>::move_to_first(DLinkNode *e)
{
  bool ret = true;
  if (OB_UNLIKELY(e == &header_) || OB_ISNULL(e)) {
    ret = false;
  } else {
    e->unlink();
    --size_;
    ret = add_first(e);
  }
  return ret;
}

// move the node to the tail
template <typename DLinkNode>
bool ObDList<DLinkNode>::move_to_last(DLinkNode *e)
{
  bool ret = true;
  if (OB_UNLIKELY(e == &header_) || OB_ISNULL(e)) {
    ret = false;
  } else {
    e->unlink();
    size_--;
    ret = add_last(e);
  }
  return ret;
}

// remove the node at tail
template <typename DLinkNode>
DLinkNode *ObDList<DLinkNode>::remove_last()
{
  return remove(header_.prev_);
}

// remove the node at head
template <typename DLinkNode>
DLinkNode *ObDList<DLinkNode>::remove_first()
{
  return remove(header_.next_);
}
template <typename DLinkNode>
DLinkNode *ObDList<DLinkNode>::remove(DLinkNode *e)
{
  DLinkNode *ret = e;
  if (OB_UNLIKELY(e == &header_) || OB_ISNULL(e)) {
    ret = NULL;
  } else {
    e->unlink();
    size_--;
  }
  return ret;
}
template <typename DLinkNode>
void ObDList<DLinkNode>::push_range(ObDList<DLinkNode> &range)
{
  if (!range.is_empty()) {
    DLinkNode *first = range.header_.next_;
    DLinkNode *last = range.header_.prev_;
    first->prev_ = NULL;
    last->next_ = NULL;
    this->header_.add_range_after(first, last);
    size_ += range.get_size();
    range.reset();
  }
}
template <typename DLinkNode>
void ObDList<DLinkNode>::pop_range(int32_t num, ObDList<DLinkNode> &range)
{
  DLinkNode *first = this->header_.next_;
  DLinkNode *last = first;
  int count = 0;
  if (count < num && last != &this->header_) {
    ++ count;
  }
  while (count < num && last->next_ != &this->header_) {
    ++ count;
    last = last->next_;
  }
  if (0 < count) {
    if (last->next_ == &this->header_) {
      reset();
    } else {
      header_.next_ = last->next_;
      last->next_->prev_ = static_cast<DLinkNode *>(&header_);
      size_ -= count;
    }
    first->prev_ = NULL;
    last->next_ = NULL;
    range.header_.add_range_after(first, last);
    range.size_ += count;
  }
}

int databuff_printf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt,
                    ...) __attribute__((format(printf, 4, 5)));

template <typename DLinkNode>
int64_t ObDList<DLinkNode>::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "["))) { // json style
//    LIB_LOG(WDIAG, "fail to databuff printf", K(ret));
  }
  for (DLinkNode *it = header_.next_;
       it != &header_ && it != header_.prev_ && OB_SUCCESS == ret;
       it = it->next_) {
    if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, *it))) {
//      LIB_LOG(WDIAG, "fail to databuff printf", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
//      LIB_LOG(WDIAG, "fail to databuff printf", K(ret));
    }
  }
  if (OB_SUCCESS == ret) {
    if (!is_empty()) {
      if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, *header_.prev_))) {
//        LIB_LOG(WDIAG, "fail to databuff printf", K(ret));
      }
    }
    if (OB_SUCCESS == ret) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "]"))) {
//        LIB_LOG(WDIAG, "fail to databuff printf", K(ret));
      }
    }
  }
  return pos;
}
} // end namespace common
} // end namespace oceanbase


#define DLIST_FOREACH_X(curr, dlist, extra_condition)                     \
  for (typeof ((dlist).get_first()) curr = (dlist).get_first();           \
       (extra_condition) && curr != (dlist).get_header() && curr != NULL; \
       curr = curr->get_next())
#define DLIST_FOREACH_NORET(curr, dlist) DLIST_FOREACH_X(curr, dlist, true)
#define DLIST_FOREACH(curr, dlist) DLIST_FOREACH_X(curr, dlist, OB_SUCC(ret))

#define DLIST_FOREACH_REMOVESAFE_X(curr, dlist, extra_condition)        \
  for (typeof ((dlist).get_first()) curr = (dlist).get_first(), save = curr->get_next(); \
       (extra_condition) && curr != (dlist).get_header();               \
       curr = save, save = save->get_next())
#define DLIST_FOREACH_REMOVESAFE_NORET(curr, dlist) DLIST_FOREACH_REMOVESAFE_X(curr, dlist, true)
#define DLIST_FOREACH_REMOVESAFE(curr, dlist) DLIST_FOREACH_REMOVESAFE_X(curr, dlist, OB_SUCC(ret))

#define DLIST_REMOVE_ALL_X(p, dlist, extra_condition)                   \
  for (typeof((dlist).remove_first()) p = (dlist).remove_first();       \
       (extra_condition) && NULL != p && p != (dlist).get_header();     \
       p = (dlist).remove_first())
#define DLIST_REMOVE_ALL_NORET(curr, dlist) DLIST_REMOVE_ALL_X(curr, dlist, true)
#define DLIST_REMOVE_ALL(curr, dlist) DLIST_REMOVE_ALL_X(curr, dlist, OB_SUCC(ret))
#endif  //OCEANBASE_LIB_LIST_OB_DLIST_H_
