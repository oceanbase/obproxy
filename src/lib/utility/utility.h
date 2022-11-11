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

#ifndef OCEANBASE_COMMON_UTILITY_H_
#define OCEANBASE_COMMON_UTILITY_H_

#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "lib/tbsys.h"
#include "easy_define.h"
#include "easy_io_struct.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/container/ob_iarray.h"

#define FALSE_IT(stmt) ({ (stmt); false; })
#define OB_FALSE_IT(stmt) ({ (stmt); false; })

#define CPUID_STD_SSE4_2 0x00100000

#define htonll(i) \
  ( \
    (((uint64_t)i & 0x00000000000000ff) << 56) | \
    (((uint64_t)i & 0x000000000000ff00) << 40) | \
    (((uint64_t)i & 0x0000000000ff0000) << 24) | \
    (((uint64_t)i & 0x00000000ff000000) << 8) | \
    (((uint64_t)i & 0x000000ff00000000) >> 8) | \
    (((uint64_t)i & 0x0000ff0000000000) >> 24) | \
    (((uint64_t)i & 0x00ff000000000000) >> 40) | \
    (((uint64_t)i & 0xff00000000000000) >> 56)   \
  )

#define DEFAULT_TIME_FORMAT "%Y-%m-%d %H:%M:%S"

#define ob_bswap_16(v) \
  (uint16_t)( \
    (((uint16_t)v & 0x00ff) << 8) | \
    (((uint16_t)v & 0xff00) >> 8) \
  )

namespace oceanbase
{
namespace common
{

class ObScanner;
class ObRowkey;
class ObVersionRange;
class ObNewRange;
class ObSqlString;

const int64_t LBT_BUFFER_LENGTH = 1024;
char *parray(char *buf, int64_t len, int64_t *array, int size);
char *lbt();
void hex_dump(const void *data, const int32_t size,
              const bool char_type = true, const int32_t log_level = OB_LOG_LEVEL_DEBUG);
int32_t parse_string_to_int_array(const char *line,
                                  const char del, int32_t *array, int32_t &size);
/**
 * parse string like int:32 to ObObj
 */
int64_t lower_align(int64_t input, int64_t align);
int64_t upper_align(int64_t input, int64_t align);
bool is2n(int64_t input);
int64_t next_pow2(const int64_t x);
bool all_zero(const char *buffer, const int64_t size);
bool all_zero_small(const char *buffer, const int64_t size);
const char *get_file_path(const char *file_dir, const char *file_name);
char *str_trim(char *str);
char *ltrim(char *str);
char *rtrim(char *str);
const char *inet_ntoa_r(const uint64_t ipport);
const char *inet_ntoa_r(const uint32_t ip);
const char *inet_ntoa_r(easy_addr_t addr);

const char *time2str(const int64_t time_s, const char *format = DEFAULT_TIME_FORMAT);
const char *obj_time2str(const int64_t time_us);
int escape_range_string(char *buffer, const int64_t length, int64_t &pos, const ObString &in);
int escape_enter_symbol(char *buffer, const int64_t length, int64_t &pos, const char *src);

int sql_append_hex_escape_str(const ObString &str, ObSqlString &sql);
inline int sql_append_hex_escape_str(const char *str, const int64_t len, ObSqlString &sql)
{
  return sql_append_hex_escape_str(ObString(0, static_cast<int32_t>(len), str), sql);
}

int convert_comment_str(char *comment_str);

int mem_chunk_serialize(char *buf, int64_t len, int64_t &pos, const char *data, int64_t data_len);
int mem_chunk_deserialize(const char *buf, int64_t len, int64_t &pos, char *data, int64_t data_len,
                          int64_t &real_len);
int64_t min(const int64_t x, const int64_t y);
int64_t max(const int64_t x, const int64_t y);

int get_double_expand_size(int64_t &new_size, const int64_t limit_size);
/**
 * allocate new memory that twice larger to store %oldp
 * @param oldp: old memory content.
 * @param old_size: old memory size.
 * @param limit_size: expand memory cannot beyond this limit.
 * @param new_size: expanded memory size.
 * @param allocator: memory allocator.
 */
template <typename T, typename Allocator>
int double_expand_storage(T *&oldp, const int64_t old_size,
                          const int64_t limit_size, int64_t &new_size, Allocator &allocator)
{
  int ret = OB_SUCCESS;
  new_size = old_size;
  void *newp = NULL;
  if (OB_SUCCESS != (ret = get_double_expand_size(new_size, limit_size))) {
  } else if (NULL == (newp = allocator.alloc(new_size * sizeof(T)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int64_t i = 0; i < old_size; ++i) {
      reinterpret_cast<T *>(newp)[i] = oldp[i];
    }
    if (NULL != oldp) { allocator.free(reinterpret_cast<char *>(oldp)); }
    oldp = reinterpret_cast<T *>(newp);
  }
  return ret;
}

template <typename T>
int double_expand_storage(T *&oldp, const int64_t old_size,
                          const int64_t limit_size, int64_t &new_size, const int64_t mod_id)
{
  ObMalloc allocator;
  allocator.set_mod_id(mod_id);
  return double_expand_storage(oldp, old_size, limit_size, new_size, allocator);
}

extern bool str_isprint(const char *str, const int64_t length);
extern int replace_str(char *src_str, const int64_t src_str_buf_size,
                       const char *match_str, const char *replace_str);

inline const char *get_peer_ip(easy_request_t *req)
{
  static char mess[8] = "unknown";
  if (OB_LIKELY(NULL != req
                && NULL != req->ms
                && NULL != req->ms->c)) {
    return inet_ntoa_r(req->ms->c->addr);
  } else {
    return mess;
  }
}

inline const char *get_peer_ip(easy_connection_t *c)
{
  static char mess[8] = "unknown";
  if (OB_LIKELY(NULL != c)) {
    return inet_ntoa_r(c->addr);
  } else {
    return mess;
  }
}

inline int get_fd(const easy_request_t *req)
{
  int fd = -1;
  if (OB_LIKELY(NULL != req
                && NULL != req->ms
                && NULL != req->ms->c)) {
    fd = req->ms->c->fd;
  }
  return fd;
}

inline void init_easy_buf(easy_buf_t *buf, char *data, easy_request_t *req, uint64_t size)
{
  if (NULL != buf && NULL != data) {
    buf->pos = data;
    buf->last = data;
    buf->end = data + size;
    buf->cleanup = NULL;
    if (NULL != req && NULL != req->ms) {
      buf->args = req->ms->pool;
    }
    buf->flags = 0;
    easy_list_init(&buf->node);
  }
}

inline easy_addr_t get_easy_addr(easy_request_t *req)
{
  static easy_addr_t empty = {0, 0, {0}, 0};
  if (OB_LIKELY(NULL != req
                && NULL != req->ms
                && NULL != req->ms->c)) {
    return req->ms->c->addr;
  } else {
    return empty;
  }
}

inline int extract_int(const ObString &str, int n, int64_t &pos, int64_t &value)
{
  int ret = OB_SUCCESS;

  if (!str.ptr() || str.length() <= 0 || pos < 0 || pos >= str.length()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const char *cur_ptr = str.ptr() + pos;
    const char *end_ptr = str.ptr() + str.length();
    int scanned = 0;
    int64_t result = 0;
    int64_t cur_value = 0;

    //skip non-numeric character
    while (cur_ptr < end_ptr && (*cur_ptr > '9' || *cur_ptr < '0')) {
      cur_ptr++;
    }
    if (cur_ptr >= end_ptr) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      n = n > 0 ? n : str.length();
      while (cur_ptr < end_ptr && scanned < n && *cur_ptr <= '9' && *cur_ptr >= '0') {
        cur_value = *cur_ptr - '0';
        result = result * 10L + cur_value;
        scanned++;
        cur_ptr++;
      }
      if (scanned <= 0) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        pos = cur_ptr - str.ptr();
        value = result;
      }
    }
  }
  return ret;
}

inline int extract_int_reverse(const ObString &str, int n, int64_t &pos, int64_t &value)
{
  int ret = OB_SUCCESS;

  if (!str.ptr() || str.length() <= 0 || pos < 0 || pos >= str.length()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const char *cur_ptr = str.ptr() + pos;
    const char *end_ptr = str.ptr();
    int scanned = 0;
    int64_t result = 0;
    int64_t multi_unit = 1;
    int64_t cur_value = 0;

    //skip non-numeric character
    while (cur_ptr >= end_ptr && (*cur_ptr > '9' || *cur_ptr < '0')) {
      cur_ptr--;
    }
    if (cur_ptr < end_ptr) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      n = n > 0 ? n : str.length();
      while (cur_ptr >= end_ptr && scanned < n && *cur_ptr <= '9' && *cur_ptr >= '0') {
        cur_value = *cur_ptr - '0';
        result += cur_value * multi_unit;
        multi_unit *= 10L;
        scanned++;
        cur_ptr--;
      }
      if (scanned <= 0) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        value = result;
        pos = cur_ptr - str.ptr();
      }
    }
  }
  return ret;
}

template <typename Allocator>
int deep_copy_ob_string(Allocator &allocator, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  char *ptr = NULL;
  if (NULL == src.ptr() || 0 >= src.length()) {
    dst.assign_ptr(NULL, 0);
  } else if (NULL == (ptr = reinterpret_cast<char *>(allocator.alloc(src.length())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(ptr, src.ptr(), src.length());
    dst.assign(ptr, src.length());
  }
  return ret;
}


struct SeqLockGuard
{
  explicit SeqLockGuard(volatile uint64_t &seq): seq_(seq)
  {
    uint64_t tmp_seq = 0;
    do {
      tmp_seq = seq_;
    } while ((tmp_seq & 1) || !__sync_bool_compare_and_swap(&seq_, tmp_seq, tmp_seq + 1));
  }
  ~SeqLockGuard()
  {
    __sync_synchronize();
    seq_++;
    __sync_synchronize();
  }
  volatile uint64_t &seq_;
};
struct OnceGuard
{
  OnceGuard(volatile uint64_t &seq): seq_(seq), locked_(false)
  {
  }
  ~OnceGuard()
  {
    if (locked_) {
      __sync_synchronize();
      seq_++;
    }
  }
  bool try_lock()
  {
    uint64_t cur_seq = 0;
    locked_ = (0 == ((cur_seq = seq_) & 1)) &&
              __sync_bool_compare_and_swap(&seq_, cur_seq, cur_seq + 1);
    return locked_;
  }
  volatile uint64_t &seq_;
  bool locked_;
};

struct CountReporter
{
  CountReporter(const char *id, int64_t report_mod)
      : id_(id), seq_lock_(0), report_mod_(report_mod),
        count_(0), start_ts_(0),
        last_report_count_(0), last_report_time_(0),
        total_cost_time_(0), last_cost_time_(0)
  {
  }
  ~CountReporter()
  {
    if (last_report_count_ > 0) {
      _OB_LOG(INFO, "%s=%ld", id_, count_);
    }
  }
  bool has_reported() { return last_report_count_ > 0; }
  void inc(const int64_t submit_time)
  {
    int64_t count = __sync_add_and_fetch(&count_, 1);
    int64_t total_cost_time = __sync_add_and_fetch(&total_cost_time_, (::oceanbase::common::ObTimeUtility::current_time() - submit_time));
    if (0 == (count % report_mod_)) {
      SeqLockGuard lock_guard(seq_lock_);
      int64_t cur_ts = ::oceanbase::common::ObTimeUtility::current_time();
      _OB_LOG(ERROR, "%s=%ld:%ld:%ld\n", id_, count,
                1000000 * (count - last_report_count_) / (cur_ts - last_report_time_),
                (total_cost_time - last_cost_time_)/report_mod_);
      last_report_count_ = count;
      last_report_time_ = cur_ts;
      last_cost_time_ = total_cost_time;
    }
  }
  const char *id_;
  uint64_t seq_lock_ CACHE_ALIGNED;
  int64_t report_mod_;
  int64_t count_ CACHE_ALIGNED;
  int64_t start_ts_;
  int64_t last_report_count_;
  int64_t last_report_time_;
  int64_t total_cost_time_;
  int64_t last_cost_time_;
};

inline int64_t get_cpu_num()
{
  return sysconf(_SC_NPROCESSORS_ONLN);
}

inline int64_t get_cpu_id()
{
  return sched_getcpu();
}

// ethernet speed: byte / second.
int get_ethernet_speed(const char *devname, int64_t &speed);
int get_ethernet_speed(const ObString &devname, int64_t &speed);

inline int64_t get_phy_mem_size()
{
  return sysconf(_SC_PAGE_SIZE) * sysconf(_SC_PHYS_PAGES);
}

inline bool is_cpu_support_sse42()
{
  uint32_t data;
  asm("cpuid"
      : "=c"(data)
      : "a"(1)
      :);
  return 0 != (data & CPUID_STD_SSE4_2);
}

///@brief Whether s1 is equal to s2, ignoring case and regarding space between words as one blank.
///If equal, return true. Otherwise, return false.
///
///For example:
///s1:"  show  collation " s2:"show collation", return true
///s1:"sh ow collation" s2:"show collation", return false
///@param [in] s1 input of string1
///@param [in] s1_len length of string1
///@param [in] s2 input of string2
///@param [in] s2_len length of string2
///@return true s1 is equal to s2,ignoring case and space
///@return false s1 is not equal to s2, or input arguments are wrong
bool is_case_space_equal(const char *s1, int64_t s1_len, const char *s2, int64_t s2_len);

///@brief Whether s1 is equal to s2 in no more than N characters, ignoring case and regarding space behind  a word as one blank.
///If equal, return true. Otherwise, return false.
///
///For example:
///s1:" set names hello" s2:"set names *", cmp_len:strlen(s2)-1  return true
///WARN:To deal SQL"set names *", s1:"set names" s2:"set names *", cmp_len:strlen(s2)-1  return false
///@param [in] s1 input of string1
///@param [in] s1_len length of string1
///@param [in] s2 input of string2
///@param [in] s2_len length of string2
///@param [in] cmp_len length to be compared
///@return true s1 is equal to s2 in no more than N characters,ignoring case and space
///@return false s1 is not equal to s2, or input arguments are wrong
bool is_n_case_space_equal(const char *s1, int64_t s1_len, const char *s2, int64_t s2_len,
                           int64_t cmp_len);

///@brief Whether str is equal to wild_str with wild charset.
///  wild_many '%', wild_one '_', wild_prefix '\'
///@param [in] str string to be compared
///@param [in] wild_str string to be compared with wild charset
///@param [in] str_is_pattern whether str is pattern.
//             When grant privileges to a database_name, this should be true;
int wild_compare(const char *str, const char *wild_str, const bool str_is_pattern);

///@brief Same functionality as 'wild_compare' with input of 'const char *'.
int wild_compare(const ObString &str, const ObString &wild_str, const bool str_is_pattern);

///@brief Get the sort value.
///  The string wich is more specific has larger number. Each string has 8 bits. The number 128
///represents string without wild char. Others represent the position of the fist wild char.
///wild char
///param [in] count count of arguments
///param [in] ... strings of needed to compute sort value
uint64_t get_sort(uint count, ...);

///@brief Get the sort value.
///param [in] str string needed to compute sort value
uint64_t get_sort(const ObString &str);

bool prefix_match(const char *prefix, const char *str);
int str_cmp(const void *v1, const void *v2);
////////////////////////////////////////////////////////////////////////////////////////////////////

template <class T,
          uint64_t N_THREAD = 1024>
class ObTSIArray
{
  struct ThreadNode
  {
    T v;
  } CACHE_ALIGNED;
public:
  ObTSIArray() : key_(-1), thread_num_(0)
  {
    int tmp_ret = pthread_key_create(&key_, NULL);
    if (0 != tmp_ret) {
      _OB_LOG(ERROR, "pthread_key_create fail ret=%d", tmp_ret);
    }
  };
  ~ObTSIArray()
  {
    (void)pthread_key_delete(key_);
  };
public:
  T &get_tsi()
  {
    volatile ThreadNode *thread_node = (volatile ThreadNode *)pthread_getspecific(key_);
    if (NULL == thread_node) {
      thread_node = &array_[__sync_fetch_and_add(&thread_num_, 1) % N_THREAD];
      int tmp_ret = pthread_setspecific(key_, (void *)thread_node);
      if (0 != tmp_ret) {
        _OB_LOG(ERROR, "pthread_setspecific fail ret=%d", tmp_ret);
      }
    }
    return (T &)(thread_node->v);
  };
  const T &at(const uint64_t i) const
  {
    return (const T &)array_[i % N_THREAD].v;
  };
  T &at(const uint64_t i)
  {
    return (T &)array_[i % N_THREAD].v;
  };
  uint64_t get_thread_num() const
  {
    return thread_num_;
  };
  uint64_t get_thread_num()
  {
    return thread_num_;
  };
private:
  pthread_key_t key_                    CACHE_ALIGNED;
  volatile uint64_t thread_num_         CACHE_ALIGNED;
  volatile ThreadNode array_[N_THREAD]  CACHE_ALIGNED;
};

inline void bind_self_to_core(uint64_t id)
{
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(id, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}
inline void bind_core()
{
  static uint64_t idx = 0;
  bind_self_to_core(ATOMIC_FAA(&idx, 1));
}
inline void rebind_cpu(const int64_t cpu_start, const int64_t cpu_end, volatile uint64_t &cpu,
                       const char *file, const int line)
{
  static __thread int64_t bind_cpu_start = 0;
  static __thread int64_t bind_cpu_end = 0;
  static __thread int64_t have_bind = false;
  static const int64_t core_num = sysconf(_SC_NPROCESSORS_ONLN);
  if (!have_bind
      || bind_cpu_start != cpu_start
      || bind_cpu_end != cpu_end) {
    if (0 <= cpu_start
        && cpu_start <= cpu_end
        && core_num > cpu_end) {
      //static volatile uint64_t cpu = 0;
      uint64_t cpu2bind = (__sync_fetch_and_add(&cpu, 1) % (cpu_end - cpu_start + 1)) + cpu_start;
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(cpu2bind, &cpuset);
      int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
      _OB_LOG(INFO, "[%s][%d] rebind setaffinity tid=%ld ret=%d cpu=%ld start=%ld end=%ld",
                file, line, GETTID(), ret, cpu2bind, cpu_start, cpu_end);
      bind_cpu_start = cpu_start;
      bind_cpu_end = cpu_end;
      have_bind = true;
    }
  }
}

/*
 * Load file to string. We alloc one more char for C string terminate '\0',
 * so it is safe to use %str.ptr() as C string.
 */
template <typename Allocator>
int load_file_to_string(const char *path, Allocator &allocator, ObString &str)
{
  int rc = OB_SUCCESS;
  struct stat st;
  char *buf = NULL;
  int fd = -1;
  int64_t size = 0;

  if (NULL == path || strlen(path) == 0) {
    rc = OB_INVALID_ARGUMENT;
  } else if ((fd = ::open(path, O_RDONLY)) < 0) {
    _OB_LOG(WARN, "open file %s failed, errno %d", path, errno);
    rc = OB_ERROR;
  } else if (0 != ::fstat(fd, &st)) {
    _OB_LOG(WARN, "fstat %s failed, errno %d", path, errno);
    rc = OB_ERROR;
  } else if (NULL == (buf = allocator.alloc(st.st_size + 1))) {
    rc = OB_ALLOCATE_MEMORY_FAILED;
  } else if ((size = static_cast<int64_t>(::read(fd, buf, st.st_size))) < 0) {
    _OB_LOG(WARN, "read %s failed, errno %d", path, errno);
    rc = OB_ERROR;
  } else {
    buf[size] = '\0';
    str.assign(buf, static_cast<int>(size));
  }
  if (fd >= 0) {
    int tmp_ret = close(fd);
    if (tmp_ret < 0) {
      _OB_LOG(WARN, "close %s failed, errno %d", path, errno);
      rc = (OB_SUCCESS == rc) ? tmp_ret : rc;
    }
  }
  return rc;
}

/**
 * copy C string safely
 *
 * @param dest destination buffer
 * @param dest_buflen destination buffer size
 * @param src source string
 * @param src_len source string length
 *
 * @return error code
 */
inline int ob_cstrcopy(char *dest, int64_t dest_buflen, const char* src, int64_t src_len)
{
  int ret = OB_SUCCESS;
  if (dest_buflen <= src_len) {
    COMMON_LOG(WARN, "buffer not enough", K(dest_buflen), K(src_len));
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    MEMCPY(dest, src, src_len);
    dest[src_len] = '\0';
  }
  return ret;
}

inline int ob_cstrcopy(char *dest, int64_t dest_buflen, const ObString &src_str)
{
  return ob_cstrcopy(dest, dest_buflen, src_str.ptr(), src_str.length());
}

const char* get_default_if();

int start_daemon(const char *pidfile);

int ob_alloc_printf(ObString &result, ObIAllocator &alloc, const char* fmt, va_list ap);
int ob_alloc_printf(ObString &result, ObIAllocator &alloc, const char* fmt, ...) __attribute__((format(printf, 3, 4)));

template <typename T>
bool has_exist_in_array(const ObIArray<T> &array, const T &var)
{
  bool ret = false;
  int64_t num = array.count();
  for (int64_t i = 0; i < num; i++) {
    if (var == array.at(i)) {
      ret = true;
      break;
    }
  }
  return ret;
}

template <typename T>
bool has_exist_in_array(const T *array, const int64_t num, const T &var)
{
  bool ret = false;
  for (int64_t i = 0; i < num; i++) {
    if (var == array[i]) {
      ret = true;
      break;
    }
  }
  return ret;
}

template <typename T>
int add_var_to_array_no_dup(ObIArray<T> &array, const T &var)
{
  int ret = OB_SUCCESS;
  if (has_exist_in_array(array, var)) {
    //do nothing
  } else if (OB_FAIL(array.push_back(var))) {
    LIB_LOG(WARN, "Add var to array error", K(ret));
  }
  return ret;
}

//size:array capacity, num: current count
template <typename T>
int add_var_to_array_no_dup(T *array, const int64_t size, int64_t &num, const T &var)
{
  int ret = OB_SUCCESS;
  if (num > size) {
    ret = OB_SIZE_OVERFLOW;
    LIB_LOG(WARN, "Num >= size", K(ret));
  } else {
    if (has_exist_in_array(array, num, var)) {
      //do nothing
    } else if (num >= size) {
      ret = OB_SIZE_OVERFLOW;
      LIB_LOG(WARN, "Size is not enough", K(ret));
    } else {
      array[num++] = var;
    }
  }
  return ret;
}

template <typename T, int64_t N = 1>
class ObPtrGuard
{
public:
  explicit ObPtrGuard(ObIAllocator &allocator) : ptr_(NULL), allocator_(allocator) {}
  ~ObPtrGuard()
  {
    if (NULL != ptr_) {
      for (int64_t i = 0; i < N; i++) {
        ptr_[i].~T();
      }
      allocator_.free(ptr_);
      ptr_ = NULL;
    }
  }

  int init()
  {
    int ret = common::OB_SUCCESS;
    if (NULL != ptr_) {
      ret = common::OB_INIT_TWICE;
      LIB_LOG(WARN, "already inited", K(ret));
    } else {
      T *mem = static_cast<T *>(allocator_.alloc(sizeof(T) * N));
      if (NULL == mem) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "alloc memory failed", K(ret), "size", sizeof(T) * N);
      } else {
        for (int64_t i = 0; i < N; i++) {
          new (&mem[i]) T();
        }
        ptr_ = mem;
      }
    }
    return ret;
  }

  T *ptr() { return ptr_; }

private:
  T *ptr_;
  ObIAllocator &allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPtrGuard);
};

class ObTimeGuard
{
public:
  explicit ObTimeGuard(const char *owner = "unknown", const int64_t warn_threshold = INT64_MAX)
  {
    start_ts_ = common::ObTimeUtility::current_time();
    last_ts_ = start_ts_;
    click_count_ = 0;
    warn_threshold_ = warn_threshold;
    owner_ = owner;
  }
  void click()
  {
    const int64_t cur_ts = common::ObTimeUtility::current_time();
    if (click_count_ < MAX_CLICK_COUNT) {
      click_[click_count_++] = (int32_t)(cur_ts - last_ts_);
      last_ts_ = cur_ts;
    }
  }
  ~ObTimeGuard()
  {
    if (get_diff() >= warn_threshold_) {
      LIB_LOG(WARN, "time guard use too much time", "this", *this);
    }
  }
  int64_t get_diff() const { return common::ObTimeUtility::current_time() - start_ts_; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  TO_YSON_KV(Y_(start_ts), Y_(last_ts),
             ID(click), common::ObArrayWrap<int32_t>(click_, click_count_));
private:
  static const int64_t MAX_CLICK_COUNT = 256;
private:
  int64_t start_ts_;
  int64_t last_ts_;
  int64_t click_count_;
  int64_t warn_threshold_;
  const char *owner_;
  int32_t click_[MAX_CLICK_COUNT];
};

// Hang th thread.
// To die or to live, it's a problem.
//
// In obproxy we can't Hang thread
inline void right_to_die_or_duty_to_live()
{
  BACKTRACE(ERROR, true, "Trying so hard to die");
  abort();
  // while(1) {
  //   sleep(120);
  // }
  // _OB_LOG(ERROR, "Trying very hard to live");
}

class ObBandwidthThrottle
{
public:
  ObBandwidthThrottle();
  ~ObBandwidthThrottle();
  int init(const int64_t rate, const int64_t buflen);
  int start_task();
  int limit(const int64_t bytes, int64_t &sleep_time);
  void destroy();
private:
  common::ObSpinLock lock_;
  int64_t start_time_;
  int64_t rate_;        // bandwidth limit kbps.
  int64_t threshold_;   // raise limit stat. by reading exceed threshold_
  int64_t lamt_;        // read bytes in one period
  int64_t buf_len_;     // read buffer length every rpc.
  bool inited_;
};

template<bool, typename T> struct __has_assign__;

template <typename T>
struct __has_assign__<true, T>
{
  typedef int (T::*Sign)(const T &);
  typedef char yes[1];
  typedef char no[2];
  template <typename U, U>
  struct type_check;
  template <typename _1> static yes &chk(type_check<Sign, &_1::assign> *);
  template <typename> static no &chk(...);
  static bool const value = sizeof(chk<T>(0)) == sizeof(yes);
};

template <typename T>
struct __has_assign__<false, T>
{
  static bool const value = false;
};

template <typename T>
inline int get_copy_assign_ret_wrap(T &dest, FalseType)
{
  UNUSED(dest);
  return OB_SUCCESS;
}

template <typename T>
inline int get_copy_assign_ret_wrap(T &dest, TrueType)
{
  return dest.get_copy_assign_ret();
}

template <typename T>
inline int copy_assign_wrap(T &dest, const T &src, FalseType)
{
  dest = src;
  return get_copy_assign_ret_wrap(dest, BoolType<HAS_MEMBER(T, get_copy_assign_ret)>());
}

template <typename T>
inline int copy_assign_wrap(T &dest, const T &src, TrueType)
{
  return dest.assign(src);
}

template <typename T>
inline int construct_assign_wrap(T &dest, const T &src, TrueType)
{
  new(&dest) T();
  return dest.assign(src);
}

template <typename T>
inline int construct_assign_wrap(T &dest, const T &src, FalseType)
{
  new(&dest) T(src);
  return get_copy_assign_ret_wrap(dest, BoolType<HAS_MEMBER(T, get_copy_assign_ret)>());
}

// This function is used for copy assignment
// -If T has a member function int assign(const T &), call dest.assign(src),
//   And take the return value of the assign function as the return value;
// -If T is class && without member function int assign(const T &), then:
//    -If T has a member function get_copy_assign_ret(), call dest=src,
//      And get the return value through dest.get_copy_assign_ret() function;
//    -If T has no member function get_copy_assign_ret(), call dest=src,
//      And return OB_SUCCESS.
template <typename T>
inline int copy_assign(T &dest, const T &src)
{
  return copy_assign_wrap(dest, src, BoolType<__has_assign__<__is_class(T), T>::value>());
}

// This function is used for copy construction
// -If T has a member function int assign(const T &) and no get_copy_assign_ret(),
//   Call new(&dest) T() and dest.assign(src), and use the return value of the assign function as the return value;
// -Otherwise, call new(&dest) T(src), and at the same time:
//    -If T has a member function get_copy_assign_ret(), then
//      Obtain the return value through the dest.get_copy_assign_ret() function;
//    -If T has no member function get_copy_assign_ret(), it returns OB_SUCCESS.
template <typename T>
inline int construct_assign(T &dest, const T &src)
{
  return construct_assign_wrap(dest, src,
      BoolType<__has_assign__<__is_class(T), T>::value && !HAS_MEMBER(T, get_copy_assign_ret)>());
}

struct tm *ob_localtime(const time_t *unix_sec, struct tm *result);
void ob_fast_localtime(time_t &cached_unix_sec, struct tm &cached_localtime,
                       const time_t &unix_sec, struct tm *result);

} // end namespace common
} // end namespace oceanbase


#endif //OCEANBASE_COMMON_UTILITY_H_
