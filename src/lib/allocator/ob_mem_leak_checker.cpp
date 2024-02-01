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

#define USING_LOG_PREFIX LIB

#include "lib/allocator/ob_mem_leak_checker.h"

#include <algorithm>
#include "obutils/ob_proxy_config.h"
#include "lib/utility/ob_backtrace.h"
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"

using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace common
{

int ObMemLeakChecker::init_all_mem_leak_checker()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(get_global_mem_leak_checker().init(obproxy::obutils::get_global_proxy_config().mem_leak_check_mod_name))) {
    LOG_WDIAG("fail to init get_global_mem_leak_checker", K(ret));
  } else if (OB_FAIL(get_global_objpool_leak_checker().init(obproxy::obutils::get_global_proxy_config().mem_leak_check_class_name))) {
    LOG_WDIAG("fail to init get_global_objpool_leak_checker", K(ret));
  }

  return ret;
}

ObMemLeakChecker &get_global_mem_leak_checker()
{
  static ObMemLeakChecker g_mem_leak_checker;
  return g_mem_leak_checker;
}

ObMemLeakChecker &get_global_objpool_leak_checker()
{
  static ObMemLeakChecker g_objpool_leak_checker;
  return g_objpool_leak_checker;
}

int ObMemLeakChecker::init(const char* name)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(init_)) {
    ret = OB_INIT_TWICE;
    LOG_EDIAG("failed to init ObMemLeakChecker, double init", K(ret));
  } else if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("failed to init ObMemLeakChecker", K(ret));
  } else {
    LOG_INFO("init name", "check_name", ObString(check_name_), "new name", ObString(name));
    int size = snprintf(check_name_, sizeof(check_name_), "%s", name);
    if (OB_UNLIKELY(size < 0 || size >= sizeof(check_name_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("failed to init ObMemLeakChecker", K(ret));
    } else {
      init_ = true;
    }
  }

  return ret;
}

int ObMemLeakChecker::change_check_name(const char* name)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!init_)) {
    // nothing
  } else if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("failed to init ObMemLeakChecker", K(ret));
  } else if (('\0' != check_name_[0]) && ('\0' == name[0])) {
    reset();
    LOG_INFO("succ to clear own mem");
  } else {
    LOG_INFO("change name", K_(check_name), K(name));
    const ObString check_name(check_name_);
    
  
    if (0 != check_name.case_compare(name)) {
      reuse();
      int size = snprintf(check_name_, sizeof(check_name_), "%s", name);
      if (OB_UNLIKELY(size < 0 || size >= sizeof(check_name_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("failed to init ObMemLeakChecker", K(ret));
      }
    }
  }

  return ret;
}

bool ObMemLeakChecker::need_mem_leak_check(int64_t id, const char * name)
{
  bool b_ret = false;
  if (id == MOD_ID_FOR_CHECK) {
    // do nothing for MOD_ID_FOR_CHECK, or will there will be a dead cycle
  } else {
    const ObString check_name(check_name_);
    b_ret = (0 == check_name.case_compare("ALL")
             || 0 == check_name.case_compare(name));
  }

  return b_ret;
}

int ObMemLeakChecker::reuse()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  MEMSET(check_name_, 0, sizeof(check_name_));
  for (int64_t i = 0; i < MEM_INFO_MAP_NUM; ++i) {
    if (OB_UNLIKELY(NULL != alloc_info_maps_[i])) {
      lib::ObMutexGuard guard(locks_[i]);
      if (OB_FAIL(alloc_info_maps_[i]->reuse())) {
        tmp_ret = ret;
      }
    }
  }
  ret = tmp_ret;
  return ret;
}

int ObMemLeakChecker::reset()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  MEMSET(check_name_, 0, sizeof(check_name_));
  for (int64_t i = 0; i < MEM_INFO_MAP_NUM; ++i) {
    if (OB_UNLIKELY(NULL != alloc_info_maps_[i])) {
      lib::ObMutexGuard guard(locks_[i]);
      if (OB_FAIL(alloc_info_maps_[i]->destroy())) {
        LOG_EDIAG("failed to destroy mem info map", K(i), K(ret));
        tmp_ret = ret;
        init_ = false;
      } else if (OB_FAIL(alloc_info_maps_[i]->create(DEFAULT_MAP_SIZE, MOD_ID_FOR_CHECK, MOD_ID_FOR_CHECK))) {
        LOG_EDIAG("failed to create mem info map", K(i), K(ret));
        tmp_ret = ret;
        init_ = false;
      }
    }
  }
  ret = tmp_ret;
  return ret;
}

int ObMemLeakChecker::get_info_map_for_cur_thread(mod_alloc_info_t*& ret_ptr, int64_t& id)
{
  int ret = OB_SUCCESS;

  ret_ptr = NULL;
  id = (get_itid() + 1) % MEM_INFO_MAP_NUM; // get_itid() start from -1
  if (OB_LIKELY(NULL != alloc_info_maps_[id])) {
    ret_ptr = alloc_info_maps_[id];
  } else if (OB_ISNULL(ret_ptr = (mod_alloc_info_t*) ob_malloc(sizeof(mod_alloc_info_t), MOD_ID_FOR_CHECK))) {
    LOG_WDIAG("failed to alloc mem for mod_alloc_info_t");
  } else {
    lib::ObMutexGuard guard(locks_[id]);
    if (NULL == alloc_info_maps_[id]) {
      alloc_info_maps_[id] = ret_ptr;
      LOG_DEBUG("succ to alloc mem info map", K(id), K(ret_ptr), "new_ptr", alloc_info_maps_[id], K(lbt()));
      ret_ptr = new (ret_ptr) mod_alloc_info_t();
      if (OB_UNLIKELY(OB_SUCCESS != ret_ptr->create(DEFAULT_MAP_SIZE, MOD_ID_FOR_CHECK, MOD_ID_FOR_CHECK))) {
        LOG_EDIAG("failed to create mem info map", K(id), K(ret_ptr), "new_ptr", alloc_info_maps_[id]);
      }
    } else {
      //is alloc by other thread, release
      ob_free(ret_ptr);
      ret_ptr = alloc_info_maps_[id];
      LOG_DEBUG("succ to avoid multi-thread data race", K(ret_ptr));
    }
  }

  return ret;
}

void ObMemLeakChecker::on_alloc(int64_t id, const char * name, const void *ptr, int64_t alloc_bytes)
{
  int ret = 0;
  mod_alloc_info_t* malloc_info = NULL;
  int64_t tid = 0;
  if (OB_ISNULL(ptr) || !init_) {
    // nothing
  } else if (OB_LIKELY(!need_mem_leak_check(id, name))) {
    // do nothing
  } else if (OB_FAIL(get_info_map_for_cur_thread(malloc_info, tid)
             || OB_ISNULL(malloc_info))) {
    LOG_WDIAG("fail to get cur thread mem info map");
  } else {
    Info info;
    info.id_ = id;
    info.alloc_bytes_ = alloc_bytes;
    (void) common::ptr_lbt(info.bt_, MAX_BACKTRACE_SIZE);
    PtrKey ptr_key;
    ptr_key.ptr_ = ptr;

    lib::ObMutexGuard guard(locks_[tid]);
    if (OB_FAIL(malloc_info->set_refactored(ptr_key, info))) {
      LOG_DEBUG("failed to insert leak checker", K(ret), "ptr", ptr_key.ptr_, K(alloc_bytes),
                "mod_name", get_global_mod_set().get_mod_name(id), K(lbt()));
    }
  }
}

void ObMemLeakChecker::on_free(int64_t id, const char * name, const void *ptr)
{
  int ret = 0;
  mod_alloc_info_t* malloc_info = NULL;
  int64_t tid = 0;
  if (OB_ISNULL(ptr) || !init_) {
    // nothing
  } else if (OB_LIKELY(!need_mem_leak_check(id, name))) {
    // do nothing
  } else if (OB_FAIL(get_info_map_for_cur_thread(malloc_info, tid)
             || OB_ISNULL(malloc_info))) {
    LOG_WDIAG("fail to get cur thread mem info map");
  } else {
    PtrKey ptr_key;
    ptr_key.ptr_ = ptr;
    lib::ObMutexGuard guard(locks_[tid]);
    if (OB_FAIL(malloc_info->erase_refactored(ptr_key))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_EDIAG("failed to erase ptr from leak checker", K(ret), "ptr", ptr_key.ptr_,
                  "mod_name", get_global_mod_set().get_mod_name(id));
      } else {
        // nothing, maybe mod_name is changed
      }
    }
  }
}

int ObMemLeakChecker::load_leak_info_map(mod_info_map_t &info_map, const int64_t id)
{
  int ret = OB_SUCCESS;

  mod_alloc_info_t* malloc_info = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < MEM_INFO_MAP_NUM; ++i) {
    malloc_info = alloc_info_maps_[i];
    if (OB_NOT_NULL(malloc_info)) {
      // malloc_info 在多个线程线程读写，需要使用线程安全的遍历方法
      lib::ObMutexGuard guard(locks_[i]);
      mod_alloc_info_t::const_iterator node_it = malloc_info->begin();
      for (; OB_SUCC(ret) && node_it != malloc_info->end(); ++node_it) {
        if (OB_UNLIKELY(-1 == id || node_it->second.id_ == id)) {
          // 预留 id = -1, 用于实现全量信息导出
          std::pair<int64_t, int64_t> bytes_count_pair;
          if (OB_FAIL(info_map.get_refactored(node_it->second, bytes_count_pair)) 
                      && OB_HASH_NOT_EXIST != ret) {
            LOG_WDIAG("fail to get ptr info from malloc_info", "ptr", node_it->first.ptr_, K(ret));
          } else {
            ret = OB_SUCCESS; // ignore OB_HASH_NOT_EXIST
            bytes_count_pair.first += node_it->second.alloc_bytes_;
            bytes_count_pair.second++;
            if (OB_FAIL(info_map.set_refactored(node_it->second, bytes_count_pair, 1, 0, 0))) {
              LOG_WDIAG("failed to aggregate memory size", K(ret));
            } else {
              // nothing
            }
          }
        }
      }
    }
  }
  
  return ret;
}


int ObMemLeakChecker::load_backtrace_info_for_id(int64_t id, int64_t max_count, char *const buf, int64_t buf_len, int64_t& ret_len)
{
  int ret = OB_SUCCESS;
  ret_len = 0;

  mod_info_map_t tmp_map;
  common::ObSEArray<ObMemLeakChecker::AllocInfoHelper, 20> order_array(MOD_ID_FOR_CHECK, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  if (OB_UNLIKELY(max_count < 0 || OB_ISNULL(buf) || buf_len <= 0)) {
    // 参数错误不返回错误，仅打印日志
    LOG_WDIAG("invalid parameters", K(max_count), K(buf), K(buf_len));
  } else if (OB_UNLIKELY(0 == max_count)) {
    // nothing
  } else if (OB_FAIL(tmp_map.create(DEFAULT_MAP_SIZE, MOD_ID_FOR_CHECK, MOD_ID_FOR_CHECK))) {
    LOG_EDIAG("failed to create hashmap", K(ret));
  } else if (OB_FAIL(load_leak_info_map(tmp_map, id))) {
    LOG_WDIAG("failed to load leak info hashmap", K(ret));
  } else if (OB_FAIL(order_array.prepare_allocate(tmp_map.size()))) {
    LOG_WDIAG("failed to alloc order_array", K(ret));
  } else {

    // tmp_map 只在当前单个线程访问，可以使用线程不安全的遍历方法
    int64_t count = tmp_map.size();
    mod_info_map_t::const_iterator it = tmp_map.begin();
    for (int64_t i = 0; (it != tmp_map.end()) && (i < count); ++it, ++i) {
      order_array.at(i).alloc_bytes_ = it->second.first;
      order_array.at(i).alloc_times_ = it->second.second;
      order_array.at(i).info_ = &(it->first);
    }
    
    std::sort(order_array.begin(), order_array.end());
    int64_t tmp_count = 0;
    int64_t write_len = 0;
    for (int64_t i = order_array.count() - 1;
         OB_SUCC(ret) && (i >= 0) && tmp_count < max_count && buf_len > write_len;
         --i, ++tmp_count) {
      int64_t alloc_bytes = order_array[i].alloc_bytes_;
      int64_t alloc_times = order_array[i].alloc_times_;
      const Info* info = order_array[i].info_;
      int64_t len = 0;
      len = snprintf(buf + write_len, buf_len - write_len, "\n%ld %ld\n", alloc_bytes, alloc_times);
      if (len < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("fail to print backtrace");
      } else {
        write_len += len;
      }

      if (OB_SUCC(ret) && buf_len > write_len) {
        for (int64_t i = 0; OB_SUCC(ret) && i < MAX_BACKTRACE_SIZE && buf_len > write_len; i++) {
          len = snprintf(buf + write_len, buf_len - write_len, "0x%lx ", get_rel_offset((int64_t) (info->bt_[i])));
          if (len < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_EDIAG("fail to print backtrace");
          } else {
            write_len += len;
          }
        }
      } else {
        // nothing
      }
    }

    if (write_len > 0) {
      ret_len = write_len;
    }
  }

  return ret;
}

void ObMemLeakChecker::print(int64_t id /*-1*/)
{
  int ret = 0;

  static const int max_backtrace_len = 8192;
  int64_t backtrace_len = 0;
  char * backtrace_buf = (char*) ob_malloc(max_backtrace_len, ObMemLeakChecker::MOD_ID_FOR_CHECK);
  if (OB_ISNULL(backtrace_buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("backtrace_buf is null", K(backtrace_buf), K(ret));
  // 打印所有 mod, 内存占用前 MAX_PRINT_RECORD 的堆栈
  } else if (OB_FAIL(load_backtrace_info_for_id(id, MAX_PRINT_RECORD, backtrace_buf, max_backtrace_len, backtrace_len))) {
    LOG_WDIAG("fail to load backtrace info", K(ret));
  } else {
    LOG_INFO("######## MEMORY LEAK CHECK (START) ########");
    _OB_LOG(INFO, "%s", backtrace_buf);
    LOG_INFO("######## MEMORY LEAK CHECK (END) ########");
  }

  if (OB_NOT_NULL(backtrace_buf)) {
    ob_free(backtrace_buf);
  }

}

int64_t ObMemLeakChecker::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  ObString check_name = ObString::make_string(check_name_);
  J_OBJ_START();
  J_KV(K_(init), K(check_name));
  J_OBJ_END();
  return pos;
}

int64_t ObMemLeakChecker::AllocInfoHelper::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(alloc_bytes), K_(alloc_times));
  J_OBJ_END();
  return pos;
}

}; // end namespace common
}; // end namespace oceanbase
