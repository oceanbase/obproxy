/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef  OCEANBASE_LIB_ADAPTIVE_MAP_
#define  OCEANBASE_LIB_ADAPTIVE_MAP_

#include <stdint.h>
#include <utility>

#include "lib/container/ob_fixed_array.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace lib
{

// Usage Introduction:
// 1. it would be better to use this ObAdaptiveMap in condition that there is a certain upperbound of data set
// 2. it will use array in low amout of data for better space efficience.
// 3. it will use hash-map in high amout of data for better time efficience.
template<class _key_type,
         class _value_type,
         int64_t UPPERBOUND = 64,
         class _allocer = oceanbase::common::ObMalloc>
class ObAdaptiveMap
{
public:
  ObAdaptiveMap() : is_init_(false), is_using_array_(false), local_arr_(default_allocer_), local_map_() {}
  ~ObAdaptiveMap() {}

  // memory is alloced and free for local_arr_ and local_map_ by default alloctor , in other words, ob_malloc().
  // memory life cycle is the same with the ObAdaptiveMap
  int init(int64_t expected_upperbound) {
    int ret = common::OB_SUCCESS;

    if (OB_UNLIKELY(is_init_)) {
      ret = common::OB_INIT_TWICE;
      OB_LOG(WDIAG, "double init ", K_(is_init), K(expected_upperbound), K(ret));
    } else if (OB_LIKELY(expected_upperbound > UPPERBOUND)) {
      is_using_array_ = false;
      if (OB_FAIL(local_map_.create(expected_upperbound, common::ObModIds::OB_HASH_BUCKET, common::ObModIds::OB_HASH_NODE))) {
        OB_LOG(WDIAG, "fail to create local_map_", K_(is_init), K(expected_upperbound), K(ret));
      } else {
        is_init_ = true;
      }
    } else {
      if (OB_FAIL(local_arr_.reserve(expected_upperbound))) {
        OB_LOG(WDIAG, "fail to reserve local_arr_", K_(is_init), K(expected_upperbound), K(ret));
      } else {
        is_using_array_ = true;
        is_init_ = true;
      }
    }

    return ret;
  }

  int get(const _key_type& key, _value_type& value) const {
    return find(key, value);
  }
  int find(const _key_type& key, _value_type& value) const {
    int ret = common::OB_SUCCESS;

    if (OB_UNLIKELY(!is_init_)) {
      ret = common::OB_NOT_INIT;
      OB_LOG(WDIAG, "not init", K(ret));
    } else if (is_using_array_) {
      if (OB_FAIL(find_with_array(key, value))) {
        if (OB_LIKELY(ret == common::OB_HASH_NOT_EXIST)) {
          OB_LOG(DEBUG, "fail to find_with_array", K(ret));
        } else {
          OB_LOG(WDIAG, "fail to find_with_array", K(ret));
        }
      }
    } else {
      if (OB_FAIL(find_with_map(key, value))) {
        if (OB_LIKELY(ret == common::OB_HASH_NOT_EXIST)) {
          OB_LOG(DEBUG, "fail to find_with_map", K(ret));
        } else {
          OB_LOG(WDIAG, "fail to find_with_map", K(ret));
        }
      }
    }

    return ret;
  }

  int set(const _key_type& key, _value_type& value, bool overwrite_value = false) {
    int ret = common::OB_SUCCESS;

    if (OB_UNLIKELY(!is_init_)) {
      ret = common::OB_NOT_INIT;
      OB_LOG(WDIAG, "not init", K(ret));
    } else if (is_using_array_) {
      if (OB_FAIL(set_with_array(key, value, overwrite_value))) {
        OB_LOG(WDIAG, "fail to set_with_array", K(ret));
      }
    } else {
      if (OB_FAIL(set_with_map(key, value, overwrite_value))) {
        OB_LOG(WDIAG, "fail to set_with_map", K(ret));
      }
    }

    return ret;
  }

  bool is_exist(const _key_type& key) const {
    int ret = common::OB_SUCCESS;
    bool bret = false;

    if (OB_UNLIKELY(!is_init_)) {
      ret = common::OB_NOT_INIT;
      OB_LOG(WDIAG, "not init", K(ret));
    } else if (is_using_array_) {
      bret = is_exist_with_array(key);
    } else {
      bret = is_exist_with_map(key);
    }

    return bret;
  }

  int64_t count() const {
    int64_t ret = 0;
    if (is_using_array_) {
      ret = local_arr_.count();
    } else {
      ret = local_map_.size();
    }
    return ret;
  }

  int to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(is_init), K_(is_using_array), "arr_count", local_arr_.count(),
         "map_count", local_map_.count() /*, K_(local_arr), K_(local_map)*/);
    J_OBJ_END();
    return pos;
  }

private:
  int find_with_array(const _key_type& key, _value_type& value) const {
    int ret = common::OB_HASH_NOT_EXIST;

    int64_t count = local_arr_.count();
    for (int64_t i = 0; i < count; ++i) {
      if (key == local_arr_.at(i).first) {
        value = local_arr_.at(i).second;
        ret = common::OB_SUCCESS;
        break;
      }
    }

    return ret;
  }

  int set_with_array(const _key_type& key, _value_type& value, bool overwrite_value) {
    int ret = common::OB_SUCCESS;

    if (is_exist_with_array(key)) {
      if (!overwrite_value) {
        ret = common::OB_HASH_EXIST;
        OB_LOG(WDIAG, "element existed", K(ret));
      } else {
        ret = common::OB_HASH_NOT_EXIST;
        int64_t count = local_arr_.count();
        for (int64_t i = 0; i < count; ++i) {
          if (key == local_arr_.at(i).first) {
            local_arr_.at(i).second = value;
            ret = common::OB_SUCCESS;
            break;
          }
        }
      }
    } else if (OB_FAIL(local_arr_.push_back(std::pair<_key_type, _value_type>(key, value)))) {
      OB_LOG(WDIAG, "fail to push back", K(ret));
    }

    return ret;
  }

  bool is_exist_with_array(const _key_type& key) const {
   bool bret = false;

    int64_t count = local_arr_.count();
    for (int64_t i = 0; i < count; ++i) {
      if (key == local_arr_.at(i).first) {
        bret = true;
        break;
      }
    }

    return bret;
  }


  int find_with_map(const _key_type& key, _value_type& value) const {
    return local_map_.get_refactored(key, value);
  }

  int set_with_map(const _key_type& key, _value_type& value, bool overwrite_value) {
    return local_map_.set_refactored(key, value, overwrite_value);
  }

  bool is_exist_with_map(const _key_type& key) const {
    return NULL != local_map_.get(key);
  }

private:
  bool is_init_;
  bool is_using_array_; //false means using map
  _allocer default_allocer_;
  common::ObFixedArray<std::pair<_key_type, _value_type>, _allocer> local_arr_;
  common::hash::ObHashMap<_key_type, _value_type, common::hash::NoPthreadDefendMode> local_map_;
};

}//namespace lib
}//namespace oceanbase
#endif //OCEANBASE_LIB_ADAPTIVE_MAP_
