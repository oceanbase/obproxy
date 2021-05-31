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

#ifndef OCEANBASE_COMMON_PAGE_ARENA_H_
#define OCEANBASE_COMMON_PAGE_ARENA_H_

#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_allocator.h"


namespace oceanbase
{
namespace common
{
// convenient function for memory alignment
inline size_t get_align_offset(void *p, const int64_t alignment = sizeof(void *))
{
  size_t size = 0;
  // quick test for power of 2
  size = alignment - (((uint64_t)p) & (alignment - 1));
  return size;
}

struct DefaultPageAllocator: public ObIAllocator
{
  DefaultPageAllocator(int64_t mod_id = ObModIds::OB_PAGE_ARENA,
                       uint64_t tenant_id = OB_SERVER_TENANT_ID)
      : mod_id_(mod_id), tenant_id_(tenant_id) {};
  virtual ~DefaultPageAllocator() {};
  void *alloc(const int64_t sz)
  {
    ObMemAttr malloc_attr;
    malloc_attr.mod_id_ = mod_id_;
    malloc_attr.tenant_id_ = tenant_id_;
    return ob_malloc(sz, malloc_attr);
  }
  void free(void *p) { ob_free(p); }
  void freed(const int64_t sz) {UNUSED(sz); /* mostly for effcient bulk stat reporting */ }
  void set_mod_id(int64_t mod_id) {mod_id_ = mod_id;};
  void set_tenant_id(uint64_t tenant_id) {tenant_id_ = tenant_id;};
  int64_t get_mod_id() const { return mod_id_; };
  void *mod_alloc(const int64_t sz, const int64_t mod_id)
  {
    ObMemAttr malloc_attr;
    malloc_attr.mod_id_ = mod_id;
    malloc_attr.tenant_id_ = tenant_id_;
    return ob_malloc(sz, malloc_attr);
  }
private:
  int64_t mod_id_;
  uint64_t tenant_id_;
};

struct ModulePageAllocator: public ObIAllocator
{
  ModulePageAllocator(int64_t mod_id = ObModIds::OB_MODULE_PAGE_ALLOCATOR,
                      int64_t tenant_id = OB_SERVER_TENANT_ID)
      : mod_id_(mod_id),
      tenant_id_(tenant_id),
      allocator_(NULL)
  {}
  explicit ModulePageAllocator(ObIAllocator &allocator)
      : mod_id_(0),
        tenant_id_(OB_SERVER_TENANT_ID),
        allocator_(&allocator) {}
  virtual ~ModulePageAllocator() {}
  void set_mod_id(int64_t mod_id) { mod_id_ = mod_id; }
  void set_tenant_id(uint64_t tenant_id) {tenant_id_ = tenant_id;};
  int64_t get_mod_id() const { return mod_id_; }
  void *alloc(const int64_t sz)
  {
    ObMemAttr malloc_attr;
    malloc_attr.mod_id_ = mod_id_;
    malloc_attr.tenant_id_ = tenant_id_;
    return (NULL == allocator_) ? ob_malloc(sz, malloc_attr) : allocator_->alloc(sz);
  }
  void free(void *p) { (NULL == allocator_) ? ob_free(p) : allocator_->free(p); p = NULL; }
  void freed(const int64_t sz) {UNUSED(sz); /* mostly for effcient bulk stat reporting */ }
  void set_allocator(ObIAllocator *allocator) { allocator_ = allocator; }
private:
  int64_t mod_id_;
  uint64_t tenant_id_;
  ObIAllocator *allocator_;
};

/**
 * A simple/fast allocator to avoid individual deletes/frees
 * Good for usage patterns that just:
 * load, use and free the entire container repeatedly.
 */
template <typename CharT = char, class PageAllocatorT = DefaultPageAllocator>
class PageArena
{
private: // types
  typedef PageArena<CharT, PageAllocatorT> Self;

  struct Page
  {
    uint64_t magic_;
    Page *next_page_;
    char *alloc_end_;
    const char *page_end_;
    char buf_[0];

    explicit Page(const char *end) : magic_(0x1234abcddbca4321), next_page_(0), page_end_(end)
    {
      alloc_end_ = buf_;
    }

    inline int64_t remain() const { return page_end_ - alloc_end_; }
    inline int64_t used() const { return alloc_end_ - buf_ ; }
    inline int64_t raw_size() const { return page_end_ - buf_ + sizeof(Page); }
    inline int64_t reuse_size() const { return page_end_ - buf_; }

    inline CharT *alloc(int64_t sz)
    {
      CharT *ret = NULL;
      if (sz <= 0) {
        //alloc size is invalid
      } else if (sz <= remain()) {
        char *start = alloc_end_;
        alloc_end_ += sz;
        ret = (CharT *) start;
      }
      return ret;
    }

    inline CharT *alloc_down(int64_t sz)
    {
      page_end_ -= sz;
      return (CharT *)page_end_;
    }

    inline void reuse()
    {
      alloc_end_ = buf_;
    }
  };

public:
  static const int64_t DEFAULT_PAGE_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE - sizeof(Page); // default 8KB
  static const int64_t DEFAULT_BIG_PAGE_SIZE = OB_MALLOC_BIG_BLOCK_SIZE; // default 2M

private: // data
  Page *cur_page_;
  Page *header_;
  Page *tailer_;
  int64_t page_limit_;  // capacity in bytes of an empty page
  int64_t page_size_;   // page size in number of bytes
  int64_t pages_;       // number of pages allocated
  int64_t used_;        // total number of bytes allocated by users
  int64_t total_;       // total number of bytes occupied by pages
  PageAllocatorT page_allocator_;

private: // helpers

  Page *insert_head(Page *page)
  {
    if (OB_ISNULL(page)) {
    } else {
      if (NULL != header_) {
        page->next_page_ = header_;
      }
      header_ = page;
    }
    return page;
  }

  Page *insert_tail(Page *page)
  {
    if (NULL != tailer_) {
      tailer_->next_page_ = page;
    }
    tailer_ = page;

    return page;
  }

  Page *alloc_new_page(const int64_t sz)
  {
    Page *page = NULL;
    void *ptr = page_allocator_.alloc(sz);

    if (NULL != ptr) {
      page  = new(ptr) Page((char *)ptr + sz);

      total_  += sz;
      ++pages_;
    } else {
      _OB_LOG(ERROR, "cannot allocate memory.sz=%ld, pages_=%ld,total_=%ld",
                sz, pages_, total_);
    }

    return page;
  }

  Page *extend_page(const int64_t sz)
  {
    Page *page = cur_page_;
    if (NULL != page) {
      page = page->next_page_;
      if (NULL != page) {
        page->reuse();
      } else {
        page = alloc_new_page(sz);
        if (NULL == page) {
          _OB_LOG(ERROR, "extend_page sz =%ld cannot alloc new page", sz);
        } else {
          insert_tail(page);
        }
      }
    }
    return page;
  }

  inline bool lookup_next_page(const int64_t sz)
  {
    bool ret = false;
    if (NULL != cur_page_
        && NULL != cur_page_->next_page_
        && cur_page_->next_page_->reuse_size() >= sz) {
      cur_page_->next_page_->reuse();
      cur_page_ = cur_page_->next_page_;
      ret = true;
    }
    return ret;
  }

  inline bool ensure_cur_page()
  {
    if (NULL == cur_page_) {
      header_ = cur_page_ = tailer_ = alloc_new_page(page_size_);
      if (NULL != cur_page_) {
        page_limit_ = cur_page_->remain();
      }
    }

    return (NULL != cur_page_);
  }

  inline bool is_normal_overflow(const int64_t sz)
  {
    return sz <= page_limit_;
  }

  inline bool is_large_page(Page *page)
  {
    return NULL == page ? false : page->raw_size() > page_size_;
  }

  CharT *alloc_big(const int64_t sz)
  {
    CharT *ptr = NULL;
    // big enough object to have their own page
    Page *p = alloc_new_page(sz + sizeof(Page));
    if (NULL != p) {
      insert_head(p);
      ptr = p->alloc(sz);
    }
    return ptr;
  }

  void free_large_pages()
  {
    Page **current = &header_;
    while (NULL != *current) {
      Page *entry = *current;
      if (is_large_page(entry)) {
        *current = entry->next_page_;
        pages_ -= 1;
        total_ -= entry->raw_size();
        page_allocator_.free(entry);
        entry = NULL;
      } else {
        tailer_ = *current;
        current = &entry->next_page_;
      }

    }
    if (NULL == header_) {
      tailer_ = NULL;
    }
  }

  Self &assign(Self &rhs)
  {
    if (this != &rhs) {
      free();

      header_ = rhs.header_;
      cur_page_ = rhs.cur_page_;
      tailer_ = rhs.tailer_;

      pages_ = rhs.pages_;
      used_ = rhs.used_;
      total_ = rhs.total_;
      page_size_  = rhs.page_size_;
      page_limit_ = rhs.page_limit_;
      page_allocator_ = rhs.page_allocator_;

    }
    return *this;
  }

public: // API
  /** constructor */
  PageArena(const int64_t page_size = DEFAULT_PAGE_SIZE,
            const PageAllocatorT &alloc = PageAllocatorT())
      : cur_page_(NULL), header_(NULL), tailer_(NULL),
        page_limit_(0), page_size_(page_size),
        pages_(0), used_(0), total_(0), page_allocator_(alloc)
  {
    if (page_size < (int64_t)sizeof(Page)) {
      _OB_LOG(ERROR, "invalid page size(page_size=%ld, page=%ld)", page_size,
              (int64_t)sizeof(Page));
    }
  }
  virtual ~PageArena() { free(); }


  Self &join(Self &rhs)
  {
    if (this != &rhs && rhs.used_ == 0) {
      if (NULL == header_) {
        assign(rhs);
      } else if (NULL != rhs.header_ && NULL != tailer_) {
        tailer_->next_page_ = rhs.header_;
        tailer_ = rhs.tailer_;

        pages_ += rhs.pages_;
        total_ += rhs.total_;
      }
      rhs.reset();
    }
    return *this;
  }

  int64_t page_size() const { return page_size_; }

  void set_mod_id(int64_t mod_id) { page_allocator_.set_mod_id(mod_id); }
  int64_t get_mod_id() const { return page_allocator_.get_mod_id(); }
  void set_tenant_id(uint64_t tenant_id) { page_allocator_.set_tenant_id(tenant_id); }
  /** allocate sz bytes */
  CharT *alloc(const int64_t sz)
  {
    ensure_cur_page();

    // common case
    CharT *ret = NULL;
    if (NULL != cur_page_ && sz > 0) {
      if (sz <= cur_page_->remain()) {
        ret = cur_page_->alloc(sz);
      } else if (is_normal_overflow(sz)) {
        Page *new_page = extend_page(page_size_);
        if (NULL != new_page) {
          cur_page_ = new_page;
        }
        if (NULL != cur_page_) {
          ret = cur_page_->alloc(sz);
        }
      } else if (lookup_next_page(sz)) {
        ret = cur_page_->alloc(sz);
      } else {
        ret = alloc_big(sz);
      }

      if (NULL != ret) {
        used_ += sz;
      }
    }
    return ret;
  }

  template<class T>
  T *new_object()
  {
    T *ret = NULL;
    void *tmp = (void *)alloc_aligned(sizeof(T));
    if (NULL == tmp) {
      _OB_LOG(WARN, "fail to alloc mem for T");
    } else {
      ret = new(tmp)T();
    }
    return ret;
  }

  /** allocate sz bytes */
  CharT *alloc_aligned(const int64_t sz, const int64_t alignment = sizeof(void *))
  {
    ensure_cur_page();

    // common case
    CharT *ret = NULL;
    if (NULL != cur_page_ && sz > 0) {
      int64_t align_offset = get_align_offset(cur_page_->alloc_end_, alignment);
      int64_t adjusted_sz = sz + align_offset;

      if (adjusted_sz <= cur_page_->remain()) {
        ret = cur_page_->alloc(adjusted_sz) + align_offset;
        if (NULL != ret){
          used_ += align_offset;
        }
      } else if (is_normal_overflow(sz)) {
        Page *new_page = extend_page(page_size_);
        if (NULL != new_page) {
          cur_page_ = new_page;
        }
        if (NULL != cur_page_) {
          ret = cur_page_->alloc(sz);
        }
      } else if (lookup_next_page(sz)) {
        if (NULL != cur_page_) {
          ret = cur_page_->alloc(sz);
        }
      } else {
        ret = alloc_big(sz);
      }

      if (NULL != ret) {
        used_ += sz;
      }
    }
    return ret;
  }

  /**
   * allocate from the end of the page.
   * - allow better packing/space saving for certain scenarios
   */
  CharT *alloc_down(const int64_t sz)
  {
    ensure_cur_page();

    // common case
    CharT *ret = NULL;
    if (NULL != cur_page_ && sz > 0) {
      if (sz <= cur_page_->remain()) {
        ret = cur_page_->alloc_down(sz);
      } else if (is_normal_overflow(sz)) {
        Page *new_page = extend_page(page_size_);
        if (NULL != new_page) {
          cur_page_ = new_page;
        }
        if (NULL != cur_page_) {
          ret = cur_page_->alloc_down(sz);
        }
      } else if (lookup_next_page(sz)) {
        ret = cur_page_->alloc_down(sz);
      } else {
        ret = alloc_big(sz);
      }

      if(NULL != ret){
        used_ += sz;
      }
    }
    return ret;
  }

  /** realloc for newsz bytes */
  CharT *realloc(CharT *p, const int64_t oldsz, const int64_t newsz)
  {
    CharT *ret = NULL;
    if (OB_ISNULL(cur_page_)) {
    } else {
      ret = p;
      // if we're the last one on the current page with enough space
      if (p + oldsz == cur_page_->alloc_end_
          && p + newsz  < cur_page_->page_end_) {
        cur_page_->alloc_end_ = (char *)p + newsz;
        ret = p;
      } else {
        ret = alloc(newsz);
        if (NULL != ret) {
          MEMCPY(ret, p, newsz > oldsz ? oldsz : newsz);
        }
      }
    }
    return ret;
  }

  /** duplicate a null terminated string s */
  CharT *dup(const char *s)
  {
    if (NULL == s) { return NULL; }

    int64_t len = strlen(s) + 1;
    CharT *copy = alloc(len);
    if (NULL != copy) {
      MEMCPY(copy, s, len);
    }
    return copy;
  }

  /** duplicate a buffer of size len */
  CharT *dup(const void *s, const int64_t len)
  {
    CharT *copy = NULL;
    if (NULL != s && len > 0) {
      copy = alloc(len);
      if (NULL != copy) {
        MEMCPY(copy, s, len);
      }
    }

    return copy;
  }

  /** free the whole arena */
  void free()
  {
    Page *page = NULL;

    while (NULL != header_) {
      page = header_;
      header_ = header_->next_page_;
      page_allocator_.free(page);
      page = NULL;
    }
    page_allocator_.freed(total_);

    cur_page_ = NULL;
    tailer_ = NULL;
    used_ = 0;
    pages_ = 0;
    total_ = 0;
  }

  /** free the arena and remain one normal page */
  void free_remain_one_page()
  {
    Page *page = NULL;
    Page *remain_page = NULL;
    while (NULL != header_) {
      page = header_;
      if (NULL == remain_page && !is_large_page(page)) {
        remain_page = page;
        header_ = header_->next_page_;
      } else {
        header_ = header_->next_page_;
        page_allocator_.free(page);
      }
      page = NULL;
    }
    header_ = cur_page_ = remain_page;
    if (NULL == cur_page_) {
      page_allocator_.freed(total_);
      total_ = 0;
      pages_ = 0;
    } else {
      cur_page_->next_page_ = NULL;
      page_allocator_.freed(total_ - cur_page_->raw_size());
      cur_page_->reuse();
      total_ = cur_page_->raw_size();
      pages_ = 1;
    }
    tailer_ = cur_page_;
    used_ = 0;
  }
  /**
   * free some of pages. remain memory can be reuse.
   *
   * @param sleep_pages force sleep when pages are freed every time.
   * @param sleep_interval_us sleep interval in microseconds.
   * @param remain_size keep size of memory pages less than %remain_size
   *
   */
  void partial_slow_free(const int64_t sleep_pages,
                         const int64_t sleep_interval_us, const int64_t remain_size = 0)
  {
    Page *page = NULL;

    int64_t current_sleep_pages = 0;

    while (NULL != header_ && (remain_size == 0 || total_ > remain_size)) {
      page = header_;
      header_ = header_->next_page_;

      total_ -= page->raw_size();

      page_allocator_.free(page);

      ++current_sleep_pages;
      --pages_;

      if (sleep_pages > 0 && current_sleep_pages >= sleep_pages) {
        ::usleep(static_cast<useconds_t>(sleep_interval_us));
        current_sleep_pages = 0;
      }
    }

    // reset allocate start point, important.
    // once slow_free called, all memory allocated before
    // CANNOT use anymore.
    cur_page_ = header_;
    if (NULL == header_) { tailer_ = NULL; }
    used_ = 0;
  }

  void free(CharT *ptr)
  {
    UNUSED(ptr);
  }

  void fast_reuse()
  {
    used_ = 0;
    cur_page_ = header_;
    if (NULL != cur_page_) {
      cur_page_->reuse();
    }
  }

  void reuse()
  {
    free_large_pages();
    fast_reuse();
  }

  void dump() const
  {
    Page *page = header_;
    int64_t count = 0;
    while (NULL != page) {
      _OB_LOG(INFO, "DUMP PAGEARENA page[%ld]:rawsize[%ld],used[%ld],remain[%ld]",
                count++, page->raw_size(), page->used(), page->remain());
      page = page->next_page_;
    }
  }

  /** stats accessors */
  int64_t pages() const { return pages_; }
  int64_t used() const { return used_; }
  int64_t total() const { return total_; }
private:
  DISALLOW_COPY_AND_ASSIGN(PageArena);
};

typedef PageArena<> CharArena;
typedef PageArena<unsigned char> ByteArena;
typedef PageArena<char, ModulePageAllocator> ModuleArena;

class ObArenaAllocator: public ObIAllocator
{
public:
  ObArenaAllocator(int64_t mod_id = ObModIds::OB_MODULE_PAGE_ALLOCATOR,
                   const int64_t page_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
                   int64_t tenant_id = OB_SERVER_TENANT_ID)
      : arena_(page_size, ModulePageAllocator(mod_id, tenant_id)) {};
  virtual ~ObArenaAllocator() {};
public:
  virtual void *alloc(const int64_t sz) { return arena_.alloc_aligned(sz); }
  void *alloc(const int64_t size, const ObMemAttr &attr)
  {
    UNUSED(attr);
    return alloc(size);
  }
  virtual void *alloc_aligned(const int64_t sz, const int64_t align)
  { return arena_.alloc_aligned(sz, align); }
  virtual void free(void *ptr) { arena_.free(reinterpret_cast<char *>(ptr)); ptr = NULL; }
  virtual void clear() { arena_.free(); }
  int64_t used() const { return arena_.used(); }
  int64_t total() const { return arena_.total(); }
  void reset() { arena_.free(); }
  void reset_remain_one_page() { arena_.free_remain_one_page(); }
  void reuse() { arena_.reuse(); }
  virtual void set_mod_id(int64_t mod_id) { arena_.set_mod_id(mod_id); }
  virtual int64_t get_mod_id() const { return arena_.get_mod_id(); }
  virtual void set_tenant_id(uint64_t tenant_id) { arena_.set_tenant_id(tenant_id); }
  ModuleArena &get_arena() { return arena_; }
private:
  ModuleArena arena_;
};

} // end namespace common
} // end namespace oceanbase

#endif // end if OCEANBASE_COMMON_PAGE_ARENA_H_
