/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_COMMON_OB_TABLET_ID_H_
#define OCEANBASE_COMMON_OB_TABLET_ID_H_
#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/rowid/ob_urowid.h"
#include "lib/container/ob_fixed_array.h"
namespace oceanbase
{
namespace common
{
class ObTabletID
{
public:
  static const uint64_t INVALID_TABLET_ID = 0;
public:
  explicit ObTabletID(const uint64_t id = INVALID_TABLET_ID) : id_(id) {}
  ObTabletID(const ObTabletID &other) : id_(other.id_) {}
  ~ObTabletID() { reset(); }
public:
  uint64_t id() const { return id_; }
  void reset() { id_ = INVALID_TABLET_ID; }
  // assignment
  ObTabletID &operator=(const uint64_t id) { id_ = id; return *this; }
  ObTabletID &operator=(const ObTabletID &other) { id_ = other.id_; return *this; }
  // tablet attribute interface
  bool is_valid() const { return id_ != INVALID_TABLET_ID; }
  // compare operator
  bool operator==(const ObTabletID &other) const { return id_ == other.id_; }
  bool operator!=(const ObTabletID &other) const { return id_ != other.id_; }
  bool operator< (const ObTabletID &other) const { return id_ <  other.id_; }
  bool operator<=(const ObTabletID &other) const { return id_ <= other.id_; }
  bool operator> (const ObTabletID &other) const { return id_ >  other.id_; }
  bool operator>=(const ObTabletID &other) const { return id_ >= other.id_; }
  int compare(const ObTabletID &other) const
  {
    if (id_ == other.id_) {
      return 0;
    } else if (id_ < other.id_) {
      return -1;
    } else {
      return 1;
    }
  }
  int hash(uint64_t &hash_val) const;
  uint64_t hash() const;
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(id));
private:
  uint64_t id_;
};
// static constants for ObTabletID
static const int64_t OB_DEFAULT_TABLET_ID_COUNT = 3;
typedef ObSEArray<ObTabletID, OB_DEFAULT_TABLET_ID_COUNT> ObTabletIDArray;
typedef ObFixedArray<ObTabletID, ObIAllocator> ObTabletIDFixedArray;
} // end namespace common
} // end namespace oceanbase
#endif /* OCEANBASE_COMMON_OB_TABLET_ID_H_ */