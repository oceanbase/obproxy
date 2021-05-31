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

#ifndef OCEANBASE_SRC_LIB_LIST_OB_OBJ_STORE_H_
#define OCEANBASE_SRC_LIB_LIST_OB_OBJ_STORE_H_
#include "lib/ob_define.h"
#include "lib/list/ob_dlist.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace common
{
template <typename T>
class ObObjNode : public ObDLinkBase<ObObjNode<T> >
{
public:
  ObObjNode() : obj_() {}
  explicit ObObjNode(const T &obj) : obj_(obj) {}
  virtual ~ObObjNode() {}

  inline void set_obj(const T &obj) { obj_ = obj; }
  inline const T &get_obj() const { return obj_; }
  inline T &get_obj() { return obj_; }
private:
  T obj_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObObjNode);
};

template <typename T, typename BlockAllocatorT = ModulePageAllocator>
class ObObjStore
{
public:
  explicit ObObjStore(const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_OBJ_STORE));
  ~ObObjStore() { destory(); }

  inline int store_obj(const T &obj);
  inline const ObDList<ObObjNode<T> > &get_obj_list() const { return obj_list_; }
  inline ObDList<ObObjNode<T> > &get_obj_list() { return obj_list_; }
  inline void destory();
private:
  ObDList<ObObjNode<T> > obj_list_;
  BlockAllocatorT allocator_;
};

template <typename T, typename BlockAllocatorT>
ObObjStore<T, BlockAllocatorT>::ObObjStore(const BlockAllocatorT &alloc)
  : allocator_(alloc)
{
}

template <typename T, typename BlockAllocatorT>
inline int ObObjStore<T, BlockAllocatorT>::store_obj(const T &obj)
{
  int ret = OB_SUCCESS;
  ObObjNode<T> *obj_node = NULL;
  void *ptr = allocator_.alloc(sizeof(ObObjNode<T>));
  if (OB_UNLIKELY(NULL == ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "no more memory to create obj node");
  } else {
    obj_node = new(ptr) ObObjNode<T>(obj);
    if (!obj_list_.add_last(obj_node)) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "add obj node to list failed");
    }
  }
  if (ret != OB_SUCCESS && obj_node != NULL) {
    allocator_.free(obj_node);
    obj_node = NULL;
    ptr = NULL;
  }
  return ret;
}

template<typename T, typename BlockAllocatorT>
inline void ObObjStore<T, BlockAllocatorT>::destory()
{
  DLIST_REMOVE_ALL_NORET(node, obj_list_) {
    if (node != NULL) {
      node->~ObObjNode<T>();
      allocator_.free(node);
      node = NULL;
    }
  }
}
}  // namespace common
}  // namespace oceanbase

#endif /* OCEANBASE_SRC_LIB_LIST_OB_OBJ_STORE_H_ */
