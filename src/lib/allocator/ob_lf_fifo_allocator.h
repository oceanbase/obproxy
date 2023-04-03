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

#ifndef  OCEANBASE_COMMON_OB_LF_FIFO_ALLOCATOR_
#define  OCEANBASE_COMMON_OB_LF_FIFO_ALLOCATOR_
#include <algorithm>
#include <pthread.h>
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/utility.h"
#include "lib/core_local/ob_core_local_storage.h"

namespace oceanbase
{
namespace common
{
struct HazardSlot
{
public:
  HazardSlot() : tid_model_(-1), interval_(0), base_(0), next_node_(NULL) {}
  virtual ~HazardSlot() {}
  virtual void purge() = 0;
public:
  inline int64_t get_tid_model() {return tid_model_;}
  inline void set_tid_model(int64_t tid_model) {tid_model_ = tid_model;}
  inline int64_t get_interval() {return interval_;}
  inline void set_interval(int64_t interval) {interval_ = interval;}
  inline int64_t get_base() {return base_;}
  inline void set_base(int64_t base) {base_ = base;}
  inline HazardSlot *get_next_node() {return next_node_;}
  inline void set_next_node(HazardSlot *next_node) {next_node_ = next_node;}
private:
  int64_t tid_model_;
  // interval and base_ is used when purging page,
  // interval equals to cache_page_cnt,
  // base equals to core_num_extension_
  int64_t interval_;
  int64_t base_;
  HazardSlot *next_node_;
};

class ObLfFIFOAllocator : public common::ObIAllocator
{
public:
  /* this function is defined for c driver client compile */
  ObLfFIFOAllocator() {}
  /* this function is defined for c driver client compile */
  virtual ~ObLfFIFOAllocator() {}
public:
  int init(const int64_t page_size,
           const int64_t mod_id,
           const uint64_t tenant_id = OB_SERVER_TENANT_ID,
           const int64_t cache_page_count = DEFAULT_CACHE_PAGE_COUNT,
           const int64_t total_limit = INT64_MAX);
  void destroy();
public:
  /* this function is defined for c driver client compile */
  void *alloc(const int64_t size) { UNUSED(size); return NULL; }
  void free(void *ptr) { UNUSED(ptr); }
  bool is_fragment(void *ptr);
  int64_t allocated() { return allocated_size_; }
  void set_mod_id(const int64_t mod_id) { mem_attr_.mod_id_ = mod_id; }
  void set_total_limit(int64_t total_limit) { total_limit_ = total_limit; }
  int64_t get_direct_alloc_count() const { return direct_alloc_count_; }
private:
  static const int64_t DEFAULT_CACHE_PAGE_COUNT = 8;
  static const int64_t MAX_CPU_NUM = OB_MAX_CPU_NUM;
  static const int64_t MAX_CACHE_PAGE_COUNT = MAX_CPU_NUM * 2;
  static const int64_t HAZARD_PTR_PER_CPU = 8;
  static const int64_t HAZARD_SLOT_FULL_INTERVAL = 1000 * 1000; // 1s
private:
  class HazardPointer
  {
  public:
    HazardPointer();
    virtual ~HazardPointer();
  public:
    void destroy();
  public:
    int acquire(HazardSlot *hazard_node, int64_t core_id, int64_t &slot_idx);
    int release(HazardSlot *hazard_node, int64_t core_id, int64_t slot_idx);
    int retire(HazardSlot *hazard_node, int64_t trigger = 2);
  private:
    class HazardBase
    {
    public:
      HazardBase() : hazard_slot_() {
        for (int64_t i = 0; i < PTR_CNT; ++i) {
          hazard_slot_[i] = NULL;
        }
      }
      ~HazardBase() {}
    public:
      inline int acquire(HazardSlot *hazard_node, int64_t &slot_idx);
      inline int release(HazardSlot *hazard_node, int64_t slot_idx);
      inline bool is_contain(HazardSlot *hazard_node);
      int64_t to_string(char *buffer, const int64_t len) const
      {
        int64_t pos = 0;
        if (NULL != buffer && len >=0 ) {
          for (int64_t i = 0; i < PTR_CNT; ++i) {
            common::databuff_printf(buffer, len, pos, "slot[%ld] = %p", i, hazard_slot_[i]);
          }
        }
        return pos;
      }
    private:
      static const int64_t PTR_CNT = 8;
    private:
      HazardSlot *hazard_slot_[PTR_CNT];
    } CACHE_ALIGNED;

    class RetireList
    {
    public:
      RetireList() : head_(NULL), tail_(NULL), cnt_(0) {}
      ~RetireList() { destroy(); }
    public:
      void destroy();
    public:
      void push(HazardSlot *node);
      void pop();
      HazardSlot *head() {return head_;}
      int64_t count() {return cnt_;}
    private:
      HazardSlot *head_;
      HazardSlot *tail_;
      int64_t cnt_;
    } CACHE_ALIGNED;

    template <typename T>
    class ThreadLocalData
    {
    public:
      ThreadLocalData() {}
      virtual ~ThreadLocalData() {}
    public:
      int acquire(T *&tld) {
        int ret = OB_SUCCESS;
        int64_t tid = get_itid();
        if (tid >= OB_MAX_THREAD_NUM) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          tld = &tld_[tid];
        }
        return ret;
      }
      int get(int64_t tid, T *&tld) {
        int ret = OB_SUCCESS;
        if (tid >= OB_MAX_THREAD_NUM) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          tld = &tld_[tid];
        }
        return ret;
      }
    private:
      T tld_[OB_MAX_THREAD_NUM];
    };

    template <typename T>
    class CoreLocalData
    {
    public:
      CoreLocalData() {}
      virtual ~CoreLocalData() {}
    public:
      int get(int64_t cid, T*&cld) {
        int ret = OB_SUCCESS;
        if (cid >= MAX_CPU_NUM) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          cld = &cld_[cid];
        }
        return ret;
      }
    private:
      T cld_[MAX_CPU_NUM];
    };
  private:
    void do_purge(RetireList *retire_list);
    bool is_contain(int64_t core_id, HazardSlot *node);
  private:
    // thread local retire lists
    ThreadLocalData<RetireList> retire_list_;
    CoreLocalData<HazardBase> hazard_;
  private:
    DISALLOW_COPY_AND_ASSIGN(HazardPointer);
  };

  struct Page : public HazardSlot
  {
  public:
    Page() : pos_(0),
             count_(0),
             page_size_(0) {}
    virtual ~Page() {}
    virtual void purge() {
      LIB_LOG(DEBUG, "a page freed", K(this));
      ob_free(this);
    }
    int64_t pos_ CACHE_ALIGNED;
    int64_t count_ CACHE_ALIGNED;
    int64_t page_size_ CACHE_ALIGNED;
    char buf_[0] CACHE_ALIGNED;
  } CACHE_ALIGNED;
  struct AreaNode
  {
    AreaNode() : header_(NULL) {}
    Page *header_;
  };
  struct SliceHeader
  {
    SliceHeader() : slice_size_(0),
                    page_(NULL) {}
    int64_t slice_size_;
    Page *page_;
  };
private:
  inline int alloc_page_for_header_(Page *&header, Page *cur_page);
  inline void *alloc_page_(int64_t size);
  inline void free_page_(void *page);
  inline int protect_page_(Page *&header, int64_t &idx, Page *&page);
  inline int unprotect_page_(Page *page, int64_t idx);
  inline int retire_page_(Page *page);
  inline void *alloc_(Page *page, const int64_t size, int64_t &end_pos);
  inline int generate_core_id_for_hazard(int64_t &core_id);
  inline int64_t get_core_id(int64_t tid);
  static HazardPointer hazard_pointer_;
private:
  ObCoreLocalStorage<AreaNode *> local_arenas_;
  AreaNode *area_node_array_;
  ObMemAttr mem_attr_;
  int64_t page_size_;
  int64_t cache_page_count_;
  // core_num_extension_ is used to generate hazard core_id, it is a minimum
  // multiple of cache_page_count_ that is larger than or equals to MAX_CPU_NUM;
  int64_t core_num_extension_;
  int64_t direct_alloc_count_;
  int64_t total_limit_;
  int64_t allocated_size_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLfFIFOAllocator);
};

} // namespace common
} // namespace oceanbase

#endif //OCEANBASE_COMMON_OB_LF_FIFO_ALLOCATOR_

