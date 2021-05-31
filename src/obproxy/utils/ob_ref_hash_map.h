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

#ifndef OBPROXY_REF_HASH_MAP_H
#define OBPROXY_REF_HASH_MAP_H
#include "lib/ob_define.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/hash/ob_pointer_hashmap.h"

namespace oceanbase
{
namespace obproxy
{

template <class K, class V, template<class K, class V> class GetKey,
          int64_t default_size = common::OB_MALLOC_NORMAL_BLOCK_SIZE,
          class Allocator = common::ModulePageAllocator>
class ObRefHashMap
{
public:
  ObRefHashMap(const common::ObModIds::ObModIdEnum mod_id)
    : is_inited_(false), pointer_map_(mod_id) {}
  virtual ~ObRefHashMap() { destroy(); }

  typedef common::hash::ObPointerHashMap<K, V, GetKey, default_size, Allocator> PointerMap;
  typedef typename PointerMap::iterator EntryIterator;

  int init()
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = common::OB_INIT_TWICE;
      PROXY_LOG(WARN, "init twice", K_(is_inited), K(ret));
    } else if (OB_FAIL(pointer_map_.init())) {
      PROXY_LOG(WARN, "fail to init pointer_map", K(ret));
    } else {
      is_inited_ = true;
    }
    return ret;
  }

  void destroy()
  {
    if (is_inited_) {
      int ret = common::OB_SUCCESS;
      int64_t sub_map_count = get_sub_map_count();
      for (int64_t i = 0; (i < sub_map_count) && OB_SUCC(ret); ++i) {
        for (EntryIterator it = begin(i); (it != end(i)) && OB_SUCC(ret); ++it) {
          if (OB_FAIL(erase(it, i))) { // dec_ref
            PROXY_LOG(WARN, "fail to erase", K(ret));
            ret = common::OB_SUCCESS; // ignore ret
          }
        }
      }
      pointer_map_.destroy();
      is_inited_ = false;
    }
  }

  // add to map, will inc_ref
  int set(V &value)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(value)) {
      ret = common::OB_INVALID_ARGUMENT;
      PROXY_LOG(WARN, "invalid input value", K(value), K(ret));
    } else {
      const K &k = get_key_(value);
      value->inc_ref();
      V over_written_value = NULL;
      if (OB_FAIL(pointer_map_.set_refactored(k, value, over_written_value, 1, 1))) {
        PROXY_LOG(WARN, "fail to set value", K(k), KPC(value), K(ret));
      } else {
        if (NULL != over_written_value) {
          over_written_value->dec_ref(); // free the old one
          over_written_value = NULL;
        }
      }

      if (OB_FAIL(ret)) {
        value->dec_ref();
        value = NULL;
      }
    }
    return ret;
  }

  // will inc_ref before return out
  V get(const K &key)
  {
    V ret_value = NULL;
    V *value = pointer_map_.get(key);
    if (NULL != value && NULL != *value) {
      (*value)->inc_ref();
      ret_value = *value;
    }
    return ret_value;
  }

  // remove from map, and dec_ref;
  int erase(const K &key)
  {
    int ret = common::OB_SUCCESS;
    V value = NULL;
    if (common::OB_SUCCESS == (ret = pointer_map_.erase_refactored(key, value))) {
      if (NULL != value) {
        ret = common::OB_SUCCESS;
        value->dec_ref();
        value = NULL;
      } else {
        ret = common::OB_HASH_NOT_EXIST;
      }
    }
    return ret;
  }

  int erase(EntryIterator &it, const int64_t sub_map_id)
  {
    int ret = common::OB_SUCCESS;
    V saved_value = *it;
    ret = pointer_map_.erase(it, sub_map_id);
    if (OB_SUCC(ret) && (NULL != saved_value)) {
      saved_value->dec_ref();
      saved_value = NULL;
    }
    return ret;
  }

protected:
  int64_t get_sub_map_count() const { return pointer_map_.get_sub_map_count(); }
  EntryIterator begin(const int64_t sub_map_id) { return pointer_map_.begin(sub_map_id); }
  EntryIterator end(const int64_t sub_map_id) { return pointer_map_.end(sub_map_id); }

private:
  bool is_inited_;
  GetKey<K, V> get_key_;
  PointerMap pointer_map_;
  DISALLOW_COPY_AND_ASSIGN(ObRefHashMap);
};

} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_REF_HASH_MAP_H */
