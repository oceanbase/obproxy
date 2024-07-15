/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_SHARE_OB_LS_ID
#define OCEANBASE_SHARE_OB_LS_ID
#include <stdint.h>
#include "lib/ob_define.h"                  // is_sys_tenant/is_meta_tenant/is_user_tenant
#include "lib/container/ob_se_array.h"      // ObSEArray
#include "lib/utility/ob_print_utils.h"     // TO_STRING_KV
namespace oceanbase
{
namespace common 
{
class ObLSID
{
public:
  // constant LS ID
  static const int64_t INVALID_LS_ID = -1;               // INVALID LS
public:
  ObLSID() : id_(INVALID_LS_ID) {}
  ObLSID(const ObLSID &other) : id_(other.id_) {}
  explicit ObLSID(const int64_t id) : id_(id) {}
  ~ObLSID() { reset(); }
public:
  int64_t id() const { return id_; }
  void reset() { id_ = INVALID_LS_ID; }
  // assignment
  ObLSID &operator=(const int64_t id) { id_ = id; return *this; }
  ObLSID &operator=(const ObLSID &other) { id_ = other.id_; return *this; }
  bool is_valid() const { return INVALID_LS_ID != id_; }
  // compare operator
  bool operator == (const ObLSID &other) const { return id_ == other.id_; }
  bool operator >  (const ObLSID &other) const { return id_ > other.id_; }
  bool operator >= (const ObLSID &other) const { return id_ >= other.id_; }
  bool operator != (const ObLSID &other) const { return id_ != other.id_; }
  bool operator <  (const ObLSID &other) const { return id_ < other.id_; }
  bool operator <= (const ObLSID &other) const { return id_ <= other.id_; }
  int compare(const ObLSID &other) const
  {
    if (id_ == other.id_) {
      return 0;
    } else if (id_ < other.id_) {
      return -1;
    } else {
      return 1;
    }
  }
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(id));
private:
  int64_t id_;
};
static const int64_t OB_DEFAULT_LS_COUNT = 3;
typedef common::ObSEArray<ObLSID, OB_DEFAULT_LS_COUNT> ObLSArray;
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_OB_LS_ID