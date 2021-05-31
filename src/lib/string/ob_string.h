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

#ifndef OCEANBASE_LIB_OB_STRING_H_
#define OCEANBASE_LIB_OB_STRING_H_

#include <algorithm>
#include <iostream>
#include "lib/hash_func/murmur_hash.h"
#include "lib/ob_define.h"
#include "lib/utility/serialization.h"
#include "lib/utility/serialization.h"
#include "common/data_buffer.h"

namespace oceanbase
{
namespace common
{
/**
 * ObString do not own the buffer's memory
 * @ptr_ : buffer pointer, allocated by user.
 * @buffer_size_ : buffer's capacity
 * @data_length_ : actual data length of %ptr_
 */
class ObString
{
public:
  typedef int32_t obstr_size_t;
public:
  ObString()
      : buffer_size_(0), data_length_(0), ptr_(NULL)
  {
  }

  INLINE_NEED_SERIALIZE_AND_DESERIALIZE;

  /*
  ObString(obstr_size_t size, char *ptr)
    : size_(size), length_(0), ptr_(ptr)
  {
    assert(length_ <= size_);
  }
  */

  /*
   * attach the buf start from ptr, capacity is size, data length is length
   */

  ObString(const obstr_size_t size, const obstr_size_t length, char *ptr)
      : buffer_size_(size), data_length_(length), ptr_(ptr)
  {
    if (OB_ISNULL(ptr_)) {
      buffer_size_ = 0;
      data_length_ = 0;
    }
  }

  ObString(const int64_t length, const char *ptr)
      : buffer_size_(0),
        data_length_(static_cast<obstr_size_t>(length)),
        ptr_(const_cast<char *>(ptr))
  {
    if (OB_ISNULL(ptr_)) {
      data_length_ = 0;
    }
  }

  ObString(const char *ptr)
      : buffer_size_(0),
        data_length_(0),
        ptr_(const_cast<char *>(ptr))
  {
    if (NULL != ptr_) {
      data_length_ = static_cast<obstr_size_t>(strlen(ptr_));
    }
  }

  // copy a char[] into buf and assign myself with it, buf may be used continuously
  inline int clone(const char *rv, const int32_t len, ObDataBuffer &buf, bool add_separator = true)
  {
    int ret = OB_SUCCESS;
    if (len > buf.get_remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      LIB_LOG(WARN, "buffer not enough", K(ret), K(len), "remain", buf.get_remain());
    } else {
      assign_buffer(buf.get_cur_pos(), static_cast<obstr_size_t>(len));
      if (len > 0) {
        const obstr_size_t writed_length = write(rv, len);
        if (writed_length == len) {
          buf.get_position() += writed_length;
        } else {
          ret = OB_ERROR;
          LIB_LOG(WARN, "write string failed", K(ret), K(writed_length), K(len));
        }
      }
      if (OB_SUCC(ret) && add_separator) {
        if (buf.get_remain() > 0) {
          // temporary use this to separate string
          *(buf.get_cur_pos()) = '\0';
          buf.get_position() ++;
        } else {
          ret = OB_BUF_NOT_ENOUGH;
          LIB_LOG(WARN, "buffer not enough", K(ret), "remain", buf.get_remain());
        }
      }
    }
    return ret;
  }

  ObString(const obstr_size_t size, const obstr_size_t length, const char *ptr)
      : buffer_size_(size), data_length_(length), ptr_(const_cast<char *>(ptr))
  {
    if (OB_ISNULL(ptr_)) {
      buffer_size_ = 0;
      data_length_ = 0;
    }
  }

  inline bool empty() const
  {
    return NULL == ptr_ || 0 == data_length_;
  }

  /*
   * attache myself to buf, and then copy rv's data to myself.
   * copy obstring in rv to buf, link with buf
   *
   */

  inline int clone(const ObString &rv, ObDataBuffer &buf)
  {
    int ret = OB_SUCCESS;
    if (rv.length() > buf.get_remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      LIB_LOG(WARN, "buffer not enough", K(ret), "need", rv.length(), "remain", buf.get_remain());
    } else {
      assign_buffer(buf.get_data() + buf.get_position(), static_cast<obstr_size_t>(buf.get_remain()));
      const obstr_size_t writed_length = write(rv.ptr(), rv.length());
      if (writed_length == rv.length()) {
        buf.get_position() += writed_length;
      } else {
        ret = OB_ERROR;
        LIB_LOG(WARN, "write string failed", K(ret), K(writed_length), "size", rv.length());
      }
    }
    return ret;
  }

  // not virtual, i'm not a base class.
  ~ObString()
  {
  }
  // reset
  void reset()
  {
    buffer_size_ = 0;
    data_length_ = 0;
    ptr_ = NULL;
  }
  // ObString 's copy constructor && assignment, use default, copy every member.
  // ObString(const ObString & obstr);
  // ObString & operator=(const ObString& obstr);

  /*
   * write a stream to my buffer,
   * return buffer
   *
   */

  inline obstr_size_t write(const char *bytes, const obstr_size_t length)
  {
    obstr_size_t writed = 0;
    if (OB_ISNULL(bytes) || OB_UNLIKELY(length <= 0)) {
      // do nothing
    } else {
      if (OB_LIKELY(data_length_ + length <= buffer_size_)) {
        MEMCPY(ptr_ + data_length_, bytes, length);
        data_length_ += length;
        writed = length;
      }
    }
    return writed;
  }

  /*
   * DO NOT USE THIS ANY MORE
   */

  inline void assign(char *bytes, const obstr_size_t length)
  {
    ptr_ = bytes;
    buffer_size_ = length;
    data_length_ = length;
    if (OB_ISNULL(ptr_)) {
      buffer_size_ = 0;
      data_length_ = 0;
    }
  }

  /*
   * attach myself to other's buf, so you can read through me, but not write
   */

  inline void assign_ptr(const char *bytes, const obstr_size_t length)
  {
    ptr_ = const_cast<char *>(bytes);
    buffer_size_ = 0;   //this means I do not hold the buf, just a ptr
    data_length_ = length;
    if (OB_ISNULL(ptr_)) {
      data_length_ = 0;
    }
  }

  /*
   * attach myself to a buffer, whoes capacity is size
   */

  inline void assign_buffer(char *buffer, const obstr_size_t size)
  {
    ptr_ = buffer;
    buffer_size_ = size;  //this means I hold the buffer, so you can do write
    data_length_ = 0;
    if (OB_ISNULL(ptr_)) {
      buffer_size_ = 0;
      data_length_ = 0;
    }

  }

  inline obstr_size_t set_length(const obstr_size_t length)
  {
    if (OB_LIKELY(NULL != ptr_) && OB_LIKELY(length <= buffer_size_)) {
      data_length_ = length;
    }
    return data_length_;
  }

  /*
   * the remain size of my buffer
   */

  inline obstr_size_t remain() const
  {
    return OB_LIKELY(buffer_size_ > 0) ? (buffer_size_ - data_length_) : buffer_size_;
  }

  inline obstr_size_t length() const { return data_length_; }
  inline obstr_size_t size() const { return buffer_size_; }
  inline const char *ptr() const { return ptr_; }
  inline char *ptr() { return ptr_; }

  inline uint64_t hash(uint64_t seed = 0) const
  {
    if (OB_LIKELY(NULL != ptr_) && OB_LIKELY(data_length_ > 0)) {
      seed = murmurhash(ptr_, data_length_, seed);
    }

    return seed;
  }

  inline int case_compare(const ObString &obstr) const
  {
    int cmp = 0;
    if (NULL == ptr_) {
      if (NULL != obstr.ptr_) {
        cmp = -1;
      }
    } else if (NULL == obstr.ptr_) {
      cmp = 1;
    } else {
      cmp = strncasecmp(ptr_, obstr.ptr_, std::min(data_length_, obstr.data_length_));
      if (0 == cmp) {
        cmp = data_length_ - obstr.data_length_;
      }
    }
    return cmp;
  }

  inline int case_compare(const char *str) const
  {
    obstr_size_t len = 0;
    if (NULL != str) {
      len = static_cast<obstr_size_t>(strlen(str));
    }
    char *p = const_cast<char *>(str);
    const ObString rv(0, len, p);
    return case_compare(rv);
  }


  inline int compare(const ObString &obstr) const
  {
    int cmp = 0;
    if (ptr_ == obstr.ptr_) {
      cmp = data_length_ - obstr.data_length_;
    } else if (0 == data_length_ && 0 == obstr.data_length_) {
      cmp = 0;
    } else if (0 == (cmp = MEMCMP(ptr_, obstr.ptr_, std::min(data_length_, obstr.data_length_)))) {
      cmp = data_length_ - obstr.data_length_;
    }
    return cmp;
  }

  inline int32_t compare(const char *str) const
  {
    obstr_size_t len = 0;
    if (NULL != str) {
      len = static_cast<obstr_size_t>(strlen(str));
    }
    char *p = const_cast<char *>(str);
    const ObString rv(0, len, p);
    return compare(rv);
  }

  //return: false:not match ; true:match
  inline bool prefix_case_match(const ObString &prefix_str) const
  {
    bool match = false;
    if (ptr_ == prefix_str.ptr_) {
      match = (data_length_ >= prefix_str.data_length_ ? true : false);
    } else if (data_length_ < prefix_str.data_length_) {
      match = false;
    } else if (0 == strncasecmp(ptr_, prefix_str.ptr_, prefix_str.data_length_)) {
      match = true;
    }
    return match;
  }

  //return: false:not match ; true:match
  inline bool prefix_match(const ObString &obstr) const
  {
    bool match = false;
    if (ptr_ == obstr.ptr_) {
      match = data_length_ >= obstr.data_length_ ? true : false;
    } else if (data_length_ < obstr.data_length_) {
      match = false;
    } else if (0 == MEMCMP(ptr_, obstr.ptr_, obstr.data_length_)) {
      match = true;
    }
    return match;
  }

  inline bool prefix_match(const char *str) const
  {
    obstr_size_t len = 0;
    if (NULL != str) {
      len = static_cast<obstr_size_t>(strlen(str));
    }
    char *p = const_cast<char *>(str);
    const ObString rv(0, len, p);
    return prefix_match(rv);
  }

  inline obstr_size_t shrink()
  {

    obstr_size_t rem  = remain();
    if (buffer_size_ > 0) {
      buffer_size_ = data_length_;
    }
    return rem;
  }

  inline bool operator<(const ObString &obstr) const
  {
    return compare(obstr) < 0;
  }

  inline bool operator<=(const ObString &obstr) const
  {
    return compare(obstr) <= 0;
  }

  inline bool operator>(const ObString &obstr) const
  {
    return compare(obstr) > 0;
  }

  inline bool operator>=(const ObString &obstr) const
  {
    return compare(obstr) >= 0;
  }

  inline bool operator==(const ObString &obstr) const
  {
    return compare(obstr) == 0;
  }

  inline bool operator!=(const ObString &obstr) const
  {
    return compare(obstr) != 0;
  }

  inline bool operator<(const char *str) const
  {
    return compare(str) < 0;
  }

  inline bool operator<=(const char *str) const
  {
    return compare(str) <= 0;
  }

  inline bool operator>(const char *str) const
  {
    return compare(str) > 0;
  }

  inline bool operator>=(const char *str) const
  {
    return compare(str) >= 0;
  }

  inline bool operator==(const char *str) const
  {
    return compare(str) == 0;
  }

  inline bool operator!=(const char *str) const
  {
    return compare(str) != 0;
  }

  const ObString trim()
  {
    ObString ret;
    if (NULL != ptr_) {
      obstr_size_t start = 0;
      obstr_size_t end = data_length_;
      while (start < end && *(ptr_ + start) == ' ') {
        start++;
      }
      while (start < end && *(ptr_ + end - 1) == ' ') {
        end--;
      }
      ret.assign_ptr(ptr_ + start, end - start);
    }
    return ret;
  }

  static ObString make_string(const char *cstr)
  {
    return NULL == cstr
        ? ObString()
        : ObString(0, static_cast<obstr_size_t>(strlen(cstr)), const_cast<char *>(cstr));
  }

  static ObString make_empty_string()
  {
    return ObString();
  }

  int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    if (OB_LIKELY(NULL != buf) && OB_LIKELY(len > 0)) {
      if (NULL != ptr()) {
        pos = snprintf(buf, len, "%.*s", length(), ptr());
        if (pos < 0) {
          pos = 0;
        } else if (pos >= len) {
          pos = len - 1;
        }
      }
    }
    return pos;
  }

  // @return The first character in the buffer.
  char operator *() const { return *ptr_; }

  // Discard the first character in the buffer.
  // @return @a this object.
  ObString &operator++()
  {
    if (OB_LIKELY(NULL != ptr_) && OB_LIKELY(data_length_ > 0)) {
      ++ptr_;
      --data_length_;
    }
    return *this;
  }

  // Discard the first @a n characters.
  // @return @a this object.
  ObString &operator +=(const int64_t n)
  {
    if (OB_LIKELY(NULL != ptr_) && OB_LIKELY(data_length_ >= n)) {
      ptr_ += n;
      data_length_ -= static_cast<obstr_size_t>(n);
    } else {
      ptr_ = NULL;
      data_length_ = 0;
    }
    return *this;
  }

  // Check for empty buffer.
  // @return @c true if the buffer has a zero pointer @b or data length.
  bool operator !() const { return !(NULL != ptr_ && data_length_ > 0); }

  // Check for non-empty buffer.
  // @return @c true if the buffer has a non-zero pointer @b and data length.
  typedef bool (ObString::*pseudo_bool)() const;
  operator pseudo_bool() const { return (NULL != ptr_ && data_length_ > 0) ? &ObString::operator! : 0; }

  // Access a character (no bounds check).
  char operator[] (const int64_t n) const { return ptr_[n]; }

  // @return @c true if @a p points at a character in @a this.
  bool contains(const char *p) const { return ptr_ <= p && p < ptr_ + data_length_; }

  // Find a character.
  // @return A pointer to the first occurrence of @a c in @a this
  // or @c NULL if @a c is not found.
  const char *find(char c) const { return static_cast<const char *>(memchr(ptr_, c, data_length_)); }

  // Split the buffer on the character at @a p.
  //
  // The buffer is split in to two parts and the character at @a p
  // is discarded. @a this retains all data @b after @a p. The
  // initial part of the buffer is returned. Neither buffer will
  // contain the character at @a p.
  //
  // This is convenient when tokenizing and @a p points at the token
  // separator.
  //
  // @note If @a *p is in the buffer then @a this is not changed
  // and an empty buffer is returned. This means the caller can
  // simply pass the result of @c find and check for an empty
  // buffer returned to detect no more separators.
  //
  // @return A buffer containing data up to but not including @a p.
  ObString split_on(const char *p) {
    ObString str; // default to empty return.
    if (contains(p)) {
      const int64_t n = p - ptr_;
      str.assign(ptr_, static_cast<obstr_size_t>(n));
      ptr_ = const_cast<char *>(p + 1);
      data_length_ -= static_cast<obstr_size_t>(n + 1);
    }
    return str;
  }

  // Split the buffer on the character @a c.
  //
  // The buffer is split in to two parts and the occurrence of @a c
  // is discarded. @a this retains all data @b after @a c. The
  // initial part of the buffer is returned. Neither buffer will
  // contain the first occurrence of @a c.
  //
  // This is convenient when tokenizing and @a c is the token
  // separator.
  //
  // @note If @a c is not found then @a this is not changed and an
  // empty buffer is returned.
  //
  // @return A buffer containing data up to but not including @a p.
  ObString split_on(const char c) { return split_on(find(c)); }

  // Get a trailing segment of the buffer.
  // @return A buffer that contains all data after @a p.
  ObString after(const char *p) const
  {
    return contains(p) ? ObString((data_length_ - (p - ptr_)) - 1, p + 1) : ObString();
  }

  // Get a trailing segment of the buffer.
  //
  // @return A buffer that contains all data after the first
  // occurrence of @a c.
  ObString after(char c) const { return after(find(c)); }

  // Remove trailing segment.
  //
  // Data at @a p and beyond is removed from the buffer.
  // If @a p is not in the buffer, no change is made.
  //
  // @return @a this.
  ObString &clip(const char *p)
  {
    if (contains(p)) {
      data_length_ = static_cast<obstr_size_t>(p - ptr_);
    }
    return *this;
  }

  int64_t find(const ObString& sub) const
  {
    int64_t pos = -1;
    int64_t sub_len = sub.length();
    if (data_length_ >= sub_len) {
      for (int64_t i = 0; i <= data_length_ - sub_len; i++) {
        if (0 == memcmp(ptr_ + i, sub.ptr(), sub_len)) {
          pos = i;
          break;
        }
      }
    }

    return pos;
  }

  int64_t find(const char* ptr) const {
    ObString str(strlen(ptr), ptr);
    return find(str);
  }

  friend std::ostream &operator<<(std::ostream &os, const ObString &str);  // for google test
private:
  obstr_size_t buffer_size_;
  obstr_size_t data_length_;
  char *ptr_;
};

DEFINE_SERIALIZE(ObString)
{
  int ret = OB_SUCCESS;
  const int64_t serialize_size = get_serialize_size();
  //Null ObString is allowed
  if (OB_ISNULL(buf) || OB_UNLIKELY(serialize_size > buf_len - pos)) {
    ret = OB_SIZE_OVERFLOW;
    LIB_LOG(WARN, "size overflow", K(ret),
        KP(buf), K(serialize_size), "remain", buf_len - pos);
  } else if (OB_FAIL(serialization::encode_vstr(buf, buf_len, pos, ptr_, data_length_))) {
    LIB_LOG(WARN, "string serialize failed", K(ret));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObString)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  const int64_t MINIMAL_NEEDED_SIZE = 2; //at least need two bytes
  if (OB_ISNULL(buf) || OB_UNLIKELY((data_len - pos) < MINIMAL_NEEDED_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), KP(buf), "remain", data_len - pos);
  } else {
    if (0 == buffer_size_) {
      ptr_ = const_cast<char *>(serialization::decode_vstr(buf, data_len, pos, &len));
      if (OB_ISNULL(ptr_)) {
        ret = OB_ERROR;
        LIB_LOG(WARN, "decode NULL string", K(ret));
      }
    } else {
      //copy to ptr_
      const int64_t str_len = serialization::decoded_length_vstr(buf, data_len, pos);
      if (str_len < 0 || buffer_size_ < str_len || (data_len - pos) < str_len) {
        ret = OB_BUF_NOT_ENOUGH;
        LIB_LOG(WARN, "string buffer not enough",
            K(ret), K_(buffer_size), K(str_len), "remain", data_len - pos);
      } else if (NULL == serialization::decode_vstr(buf, data_len, pos, ptr_, buffer_size_, &len)) {
        ret = OB_ERROR;
        LIB_LOG(WARN, "decode string failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      data_length_ = static_cast<obstr_size_t>(len);
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObString)
{
  return serialization::encoded_length_vstr(data_length_);
}

inline std::ostream &operator<<(std::ostream &os, const ObString &str)  // for google test
{
  os << "size=" << str.buffer_size_ << " len=" << str.data_length_;
  return os;
}

template <typename AllocatorT>
int ob_write_string(AllocatorT &allocator, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  void *ptr = NULL;
  if (OB_ISNULL(src.ptr()) || OB_UNLIKELY(0 >= src_len)) {
    dst.assign(NULL, 0);
  } else if (NULL == (ptr = allocator.alloc(src_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "allocate memory failed", K(ret), "size", src_len);
  } else {
    MEMCPY(ptr, src.ptr(), src_len);
    dst.assign_ptr(reinterpret_cast<char *>(ptr), src_len);
  }
  return ret;
}

template <typename AllocatorT>
int ob_simple_low_to_up(AllocatorT &allocator, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  const char *src_ptr = src.ptr();
  char *dst_ptr = NULL;
  void *ptr = NULL;
  char letter='\0';
  if (OB_ISNULL(src_ptr) || OB_UNLIKELY(0 >= src_len)) {
    dst.assign(NULL, 0);
  } else if (NULL == (ptr = allocator.alloc(src_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "allocate memory failed", K(ret), "size", src_len);
  } else {
    dst_ptr = static_cast<char *>(ptr);
    for(ObString::obstr_size_t i = 0; i < src_len; ++i) {
      letter = src_ptr[i];
      if(letter >= 'a' && letter <= 'z'){
        dst_ptr[i] = static_cast<char>(letter - 32);
      } else{
        dst_ptr[i] = letter;
      }
    }
    dst.assign_ptr(dst_ptr, src_len);
  }
  return ret;
}

template <typename AllocatorT>
int ob_sub_str(AllocatorT &allocator, const ObString &src, int32_t start_index, ObString &dst)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  const char *src_ptr = src.ptr();
  char *dst_ptr = NULL;
  void *ptr = NULL;
  if (OB_ISNULL(src_ptr) || OB_UNLIKELY(0 >= src_len)) {
    dst.assign(NULL, 0);
  } else if(start_index < 0 || start_index >= src_len) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), K(start_index), K(src_len));
  } else if (NULL == (ptr = allocator.alloc(src_len - start_index))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "allocate memory failed", K(ret), "size", src_len - start_index);
  } else{
    dst_ptr = static_cast<char *>(ptr);
    MEMCPY(dst_ptr, src.ptr() + start_index, src_len - start_index);
    dst.assign_ptr(dst_ptr, src_len - start_index);
  }
  return ret;
}

template <typename AllocatorT>
int ob_sub_str(AllocatorT &allocator, const ObString &src, int32_t start_index, int32_t end_index, ObString &dst)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  const char *src_ptr = src.ptr();
  char *dst_ptr = NULL;
  void *ptr = NULL;
  if (OB_ISNULL(src_ptr) || OB_UNLIKELY(0 >= src_len)) {
    dst.assign(NULL, 0);
  } else if(start_index < 0 || end_index >= src_len || start_index > end_index) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), K(start_index), K(end_index), K(src_len));
  } else if (NULL == (ptr = allocator.alloc(end_index - start_index + 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "allocate memory failed", K(ret), "size", end_index - start_index + 1);
  } else{
    dst_ptr = static_cast<char *>(ptr);
    MEMCPY(dst_ptr, src.ptr() + start_index, end_index - start_index + 1);
    dst.assign_ptr(dst_ptr, end_index - start_index + 1);
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_LIB_OB_STRING_H_
