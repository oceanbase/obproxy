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

#include "lib/oblog/ob_warning_buffer.h"

namespace oceanbase
{
namespace common
{
bool ObWarningBuffer::is_log_on_ = false;

OB_SERIALIZE_MEMBER(ObWarningBuffer::WarningItem,
                    msg_,
                    code_,
                    log_level_);

ObWarningBuffer &ObWarningBuffer::operator= (const ObWarningBuffer &other)
{
  if (this != &other) {
    uint32_t n = 0;
    if (total_warning_count_ >= BUFFER_SIZE) {
      n = BUFFER_SIZE;
    } else {
      n = other.append_idx_;
    }
    for (uint32_t i = 0; i < n; ++i) {
      item_[i] = other.item_[i];
    }
    err_ = other.err_;
    append_idx_ = other.append_idx_;
    total_warning_count_ = other.total_warning_count_;
  }
  return *this;
}

ObWarningBuffer::WarningItem &ObWarningBuffer::WarningItem::operator= (const WarningItem &other)
{
  if (this != &other) {
    strcpy(msg_, other.msg_);
    timestamp_ = other.timestamp_;
    log_level_ = other.log_level_;
    line_no_ = other.line_no_;
    code_ = other.code_;
  }
  return *this;
}
////////////////////////////////////////////////////////////////
// private function
static ObWarningBuffer *&__get_tsi_warning_buffer()
{
  static __thread ObWarningBuffer *buffer = NULL;
  return buffer;
}

ObWarningBuffer *ob_get_tsi_warning_buffer()
{
  return __get_tsi_warning_buffer();
}

const ObString ob_get_tsi_err_msg()
{
  ObString ret;
  ObWarningBuffer *wb = ob_get_tsi_warning_buffer();
  if (OB_LIKELY(NULL != wb)) {
    ret = ObString::make_string(wb->get_err_msg());
  }
  return ret;
}

void ob_setup_tsi_warning_buffer(ObWarningBuffer *buffer)
{
  __get_tsi_warning_buffer() = buffer;
}

void ob_setup_default_tsi_warning_buffer()
{
  static __thread ObWarningBuffer *default_wb = NULL;
  if (NULL == default_wb) {
    default_wb = new(std::nothrow) ObWarningBuffer();
  }
  ob_setup_tsi_warning_buffer(default_wb);
}

void ob_reset_tsi_warning_buffer()
{
  ObWarningBuffer *wb = ob_get_tsi_warning_buffer();
  if (OB_LIKELY(NULL != wb)) {
    wb->reset();
  }
}

}
}
