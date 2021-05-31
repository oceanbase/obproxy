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

#ifndef OCEANBASE_LIB_OBLOG_OB_WARNING_BUFFER_
#define OCEANBASE_LIB_OBLOG_OB_WARNING_BUFFER_

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <new>

#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
// one global error message buffer and multi global warning buffers
class ObWarningBuffer
{
public:
  ObWarningBuffer() : append_idx_(0), total_warning_count_(0) { }
  ~ObWarningBuffer() {reset();}

  inline void reset(void);

  static inline void set_warn_log_on(const bool is_log_on) {is_log_on_ = is_log_on;}
  static inline bool is_warn_log_on() {return is_log_on_;}

  inline uint32_t get_total_warning_count(void) const {return total_warning_count_;}
  inline uint32_t get_buffer_size(void) const {return BUFFER_SIZE;}
  inline uint32_t get_readable_warning_count(void) const;
  inline uint32_t get_max_warn_len(void) const {return WarningItem::STR_LEN;}

  /*
   * write WARNING into BUFFER
   * if buffer is full, cover the oldest warning
   * if len(str) > STR_LEN, cut it.
   */
  inline void append_warning(const char *str, int errcode);
  inline void append_note(const char *str, int errcode);

  inline void reset_err() {err_.reset();}
  inline void set_error(const char *str, int error_code = OB_MAX_ERROR_CODE) {err_.set(str); err_.set_code(error_code);}
  inline const char *get_err_msg() const {return err_.get();}
  inline int get_err_code() const {return err_.get_code();}

  ObWarningBuffer &operator= (const ObWarningBuffer &other);

  struct WarningItem
  {
    OB_UNIS_VERSION(1);
  public:
    WarningItem() : timestamp_(0),
                    log_level_(ObLogger::USER_WARN),
                    line_no_(0),
                    code_(OB_MAX_ERROR_CODE)
    { msg_[0] = '\0'; }
    ~WarningItem()
    { }

    static const uint32_t STR_LEN = 512;
    char msg_[STR_LEN];
    int64_t timestamp_;
    int log_level_;
    int line_no_;
    int code_;

    inline void reset()
    { msg_[0] = '\0'; code_ = OB_MAX_ERROR_CODE; }
    inline void set(const char *str) {snprintf(msg_, STR_LEN, "%s", str);}
    inline void set_code(int code) { code_ = code; }
    inline void set_log_level(ObLogger::UserMsgLevel level) { log_level_ = level; }
    inline const char *get() const {return static_cast<const char *>(msg_);}
    inline int get_code() const { return code_; }
    WarningItem &operator= (const WarningItem &other);

    TO_STRING_KV(K_(msg), K_(code));
  };

  /*
   * get WarningItem
   * idx range [0, get_readable_warning_count)
   * return NULL if idx is out of range
   */
  inline const ObWarningBuffer::WarningItem *get_warning_item(const uint32_t idx) const;

private:
  // const define
  static const uint32_t BUFFER_SIZE = 64;
  WarningItem item_[BUFFER_SIZE];
  WarningItem err_;
  uint32_t append_idx_;
  uint32_t total_warning_count_;
  static bool is_log_on_;
};

inline void ObWarningBuffer::reset()
{
  append_idx_ = 0;
  total_warning_count_ = 0;
  err_.reset();
}

inline uint32_t ObWarningBuffer::get_readable_warning_count() const
{
  return (total_warning_count_ < get_buffer_size()) ? total_warning_count_ : get_buffer_size();
}

inline const ObWarningBuffer::WarningItem *ObWarningBuffer::get_warning_item(const uint32_t idx) const
{
  const ObWarningBuffer::WarningItem *item = NULL;
  if (idx < get_readable_warning_count()) {
    uint32_t loc = idx;
    if (total_warning_count_ > BUFFER_SIZE) {
      loc = (append_idx_ + idx) % BUFFER_SIZE;
    }
    item = &item_[loc];
  }
  return item;
}

inline void ObWarningBuffer::append_warning(const char *str, int code)
{
  item_[append_idx_].set(str);
  item_[append_idx_].set_code(code);
  item_[append_idx_].set_log_level(ObLogger::USER_WARN);
  append_idx_ = (append_idx_ + 1) % BUFFER_SIZE;
  total_warning_count_++;
}

inline void ObWarningBuffer::append_note(const char *str, int code)
{
  item_[append_idx_].set(str);
  item_[append_idx_].set_code(code);
  item_[append_idx_].set_log_level(ObLogger::USER_NOTE);
  append_idx_ = (append_idx_ + 1) % BUFFER_SIZE;
  total_warning_count_++;
}

////////////////////////////////////////////////////////////////
/// global functions below return thread_local warning buffer
////////////////////////////////////////////////////////////////
ObWarningBuffer *ob_get_tsi_warning_buffer();
const ObString ob_get_tsi_err_msg();
/*
 * processing function of rpc set thread_local warning buffer of session through
 * set_tsi_warning_buffer function generally, but warning buffer in session could not be used in
 * remote task handler, because result_code was used after process function, so use the default
 * in this situation.
 */
void ob_setup_tsi_warning_buffer(ObWarningBuffer *buffer);
void ob_setup_default_tsi_warning_buffer();
// clear warnging buffer of current thread
void ob_reset_tsi_warning_buffer();

}  // end of namespace common
}
#endif //OB_WARNING_BUFFER_H_
