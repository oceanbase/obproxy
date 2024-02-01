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


#include "lib/ob_define.h"
#include "lib/lock/ob_mutex.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/thread_local/ob_tsi_utils.h"


namespace oceanbase
{
namespace common
{
class ObMemLeakChecker
{
  static const int64_t MAX_BACKTRACE_SIZE = 16;
  static const int64_t MEM_INFO_MAP_NUM = common::OB_MAX_CPU_NUM;
  static const int64_t DEFAULT_MAP_SIZE = 3079;
  static const int64_t MAX_PRINT_RECORD = 10;
public:
  static const int64_t MOD_ID_FOR_CHECK = ObModIds::OB_PROXY_MEM_LEAK_CHECK;
  
  ObMemLeakChecker() : init_(false), locks_() {
    MEMSET(check_name_, 0, sizeof(check_name_));
    MEMSET(alloc_info_maps_, 0, sizeof(alloc_info_maps_));
  }
  struct PtrKey
  {
    PtrKey() : ptr_(NULL) {}
    const void *ptr_;
    uint64_t hash() const
    {
      return murmurhash(&ptr_, sizeof(void*), 0);
    }
    bool operator==(const PtrKey &other) const
    {
      return (other.ptr_ == this->ptr_);
    }
  };
  struct Info
  {
    Info() : id_(0), alloc_bytes_(0) {
      MEMSET(bt_, 0, sizeof(bt_));
    }
    uint64_t hash() const
    {
      return murmurhash(bt_, sizeof(bt_), 0);
    }
    bool operator==(const Info &other) const
    {
      return (0 == STRNCMP((const char*)bt_, (const char*)other.bt_, sizeof(bt_)));
    }
    int64_t id_;
    int64_t alloc_bytes_;
    void* bt_[common::ObMemLeakChecker::MAX_BACKTRACE_SIZE];
  };

  struct AllocInfoHelper
  {
    AllocInfoHelper() : alloc_bytes_(0), alloc_times_(0), info_(NULL) {}
    bool operator<(const AllocInfoHelper& other) {
      bool b_ret = false;
      if (alloc_bytes_ < other.alloc_bytes_) {
        b_ret = true;
      } else if (alloc_bytes_ == other.alloc_bytes_) {
        if (alloc_times_ < other.alloc_times_) {
          b_ret = true;
        }
      }
      return b_ret;
    }

    DECLARE_TO_STRING;
    int64_t alloc_bytes_;
    int64_t alloc_times_;
    const Info* info_;
  };

  typedef hash::ObHashMap<Info, std::pair<int64_t, int64_t>, common::hash::NoPthreadDefendMode> mod_info_map_t; // Info -> (alloc_bytes, alloc_times)
  typedef hash::ObHashMap<PtrKey, Info, common::hash::NoPthreadDefendMode> mod_alloc_info_t;

  int reset();
  int reuse();
  int change_check_name(const char* name);
  void on_alloc(int64_t id, const char *name, const void *ptr, int64_t alloc_bytes);
  void on_free(int64_t id, const char *name, const void *ptr);
  int load_backtrace_info_for_id(int64_t id, int64_t max_count, char *const buf, int64_t buf_len, int64_t& ret_len);
  int load_leak_info_map(mod_info_map_t &info_map, const int64_t id);

  void print(int64_t id = -1);
  DECLARE_TO_STRING;

  static int init_all_mem_leak_checker();
private:
  int init(const char* name);
  int get_info_map_for_cur_thread(mod_alloc_info_t*& ret_ptr, int64_t& tid);
  bool need_mem_leak_check(int64_t id, const char *name);


private:
  bool init_;
  char check_name_[common::OB_MAX_CONFIG_VALUE_LEN];

  lib::ObMutex locks_[MEM_INFO_MAP_NUM];
  mod_alloc_info_t* alloc_info_maps_[MEM_INFO_MAP_NUM];
};

ObMemLeakChecker &get_global_mem_leak_checker();
ObMemLeakChecker &get_global_objpool_leak_checker();


}; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_MEM_LEAK_CHECKER_H__ */
