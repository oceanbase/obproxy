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

#ifndef __OB_MEM_LEAK_CHECKER_H__
#define __OB_MEM_LEAK_CHECKER_H__
#include "lib/hash/ob_hashmap.h"


namespace oceanbase
{
namespace common
{
class ObMemLeakChecker
{
  struct PtrKey
  {
    void *ptr_;
    uint64_t hash() const
    {
      return murmurhash(&ptr_, sizeof(ptr_), 0);
    }
    bool operator==(const PtrKey &other) const
    {
      return (other.ptr_ == this->ptr_);
    }
  };
  struct Info
  {
    uint64_t hash() const
    {
      return murmurhash(bt_, static_cast<int32_t> (strlen(bt_)), 0);
    }
    bool operator==(const Info &other) const
    {
      return (0 == STRNCMP(bt_, other.bt_,  static_cast<int32_t> (strlen(bt_))));
    }

    char bt_[512];
  };

  typedef hash::ObHashMap<PtrKey, Info> mod_alloc_info_t;

public:
  typedef hash::ObHashMap<Info, int64_t> mod_info_map_t;

  ObMemLeakChecker()
      :mod_id_(-1)
  {}
  int init(int64_t mod_id)
  {
    int ret = malloc_info_.create(512, ObModIds::TEST, ObModIds::TEST);
    if (OB_FAIL(ret)) {
      _OB_LOG(ERROR, "failed to create hashmap, err=%d", ret);
    } else {
      mod_id_ = mod_id;
      _OB_LOG(INFO, "leak checker, mod_id=%lu", mod_id);
    }
    return ret;
  }
  void set_mod_id(int64_t mod_id)
  {
    mod_id_ = mod_id;
  }
  int64_t get_mod_id() const { return mod_id_; }
  void reset()
  {
    malloc_info_.reuse();
  }

  void on_alloc(int64_t mod_id, void *ptr)
  {
    if (mod_id == mod_id_) {
      char* bt = lbt();
      Info info;
      snprintf(info.bt_, sizeof(info.bt_), "%s", bt);
      PtrKey ptr_key;
      ptr_key.ptr_ = ptr;
      int ret = 0;

      if (OB_FAIL(malloc_info_.set_refactored(ptr_key, info))) {
        _OB_LOG(WARN, "failed to insert leak checker(ret=%d), ptr=%p bt=%s",
                ret, ptr_key.ptr_, bt);
      }
    }
  }

  void on_free(int64_t mod_id, void*ptr)
  {
    if (mod_id == mod_id_) {
      PtrKey ptr_key;
      ptr_key.ptr_ = ptr;
      char* bt = lbt();
      int ret = 0;
      if (OB_FAIL(malloc_info_.erase_refactored(ptr_key))) {
        _OB_LOG(WARN, "failed to erase leak checker(ret=%d), ptr=%p, bt=%s",
                ret, ptr_key.ptr_, bt);
      }
    }
  }

  int load_leak_info_map(hash::ObHashMap<Info, int64_t> &info_map)
  {
    int ret = OB_SUCCESS;
    mod_alloc_info_t::const_iterator it = malloc_info_.begin();
    for (; it != malloc_info_.end(); ++it) {
      int64_t size = 0;
      //_OB_LOG(INFO, "hash value, bt=%s, hash=%lu", it->second.bt_, it->second.hash());
      ret = info_map.get_refactored(it->second, size);
      if (OB_FAIL(ret) && OB_HASH_NOT_EXIST != ret) {
        _OB_LOG(INFO, "LEAK_CHECKER, ptr=%p bt=%s", it->first.ptr_, it->second.bt_);
      } else {
        if (OB_SUCC(ret)) {
          size += 1;
          if (OB_FAIL(info_map.set_refactored(it->second, size, 1, 0, 1))) {
            _OB_LOG(WARN, "failed to aggregate memory size, ret=%d", ret);
          } else {
            _OB_LOG(DEBUG, "LEAK_CHECKER hash updated");
          }
        } else {
          size = 1;
          if (OB_FAIL(info_map.set_refactored(it->second, size, 1, 0, 0))) {
            _OB_LOG(WARN, "failed to aggregate memory size, ret=%d", ret);
          } else {
            _OB_LOG(DEBUG, "LEAK_CHECKER hash inserted");
          }
       }
      }
    }
    return ret;
  }
  void print()
  {
    hash::ObHashMap<Info, int64_t> tmp_map;
    int ret = tmp_map.create(10000, ObModIds::TEST);
    if (OB_FAIL(ret)) {
      _OB_LOG(ERROR, "failed to create hashmap, err=%d", ret);
    } else if (OB_FAIL(load_leak_info_map(tmp_map))) {
      _OB_LOG(INFO, "failed to collection leak info, ret=%d", ret);
    } else {
      _OB_LOG(INFO, "######## LEAK_CHECKER (mod_id = %ld)########", mod_id_);

      hash::ObHashMap<Info, int64_t>::const_iterator jt = tmp_map.begin();
      for (; jt != tmp_map.end(); ++jt)
      {
        _OB_LOG(INFO, "[LC] bt=%s, count=%ld", jt->first.bt_, jt->second);
      }
      _OB_LOG(INFO, "######## LEAK_CHECKER (END) ########");
    }
  }

private:
  int64_t mod_id_;
  mod_alloc_info_t malloc_info_;
};
}; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_MEM_LEAK_CHECKER_H__ */
