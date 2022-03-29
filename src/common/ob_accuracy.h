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

#ifndef OCEANBASE_COMMON_OB_ACCURACY_
#define OCEANBASE_COMMON_OB_ACCURACY_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "common/ob_obj_type.h"

namespace oceanbase
{
namespace common
{

typedef int32_t ObLength;
typedef int16_t ObPrecision;
typedef int16_t ObScale;
class ObAccuracy
{
public:
  ObAccuracy() { reset(); }
  ~ObAccuracy() {}
  explicit ObAccuracy(ObLength length) { set_length(length); }
  ObAccuracy(ObPrecision precision, ObScale scale) { set_precision(precision); set_scale(scale); }
  ObAccuracy(ObLength length, ObPrecision precision, ObScale scale)
  { set_length(length); set_precision(precision); set_scale(scale); }
  ObAccuracy(const ObAccuracy &other) { accuracy_ = other.accuracy_; }
  OB_INLINE void set_accuracy(const ObAccuracy &accuracy) { accuracy_ = accuracy.accuracy_; }
  OB_INLINE void set_accuracy(const int64_t &accuracy) { accuracy_ = accuracy; }
  OB_INLINE void set_length(ObLength length) { length_ = length; }
  OB_INLINE void set_precision(ObPrecision precision) { precision_ = precision; }
  OB_INLINE void set_scale(ObScale scale) { scale_ = scale; }
  
  // get union data
  OB_INLINE int64_t get_accuracy() const { return accuracy_; }
  // get detail data
  OB_INLINE ObLength get_length() const { return length_; }
  OB_INLINE ObPrecision get_precision() const { return precision_; }
  OB_INLINE ObScale get_scale() const { return scale_; }
  OB_INLINE void reset() { accuracy_ = -1; }
public:
  OB_INLINE ObAccuracy &operator =(const ObAccuracy &other)
  {
    if (this != &other) {
      accuracy_ = other.accuracy_;
    }
    return *this;
  }
  OB_INLINE bool operator ==(const ObAccuracy &other) const { return accuracy_ == other.accuracy_; }
  OB_INLINE bool operator !=(const ObAccuracy &other) const { return accuracy_ != other.accuracy_; }
public:
  // why we expose this 3 arrays directly?
  // imagine that we add 'if ... else' statements in ddl_default_accuracy() first,
  // and 'int ret = OB_SUCCESS' and 'return ret' statements too.
  // then the caller must add some 'if (OB_FAIL(...)) ... else LOG_WARN()'.
  // at last we get much more codes which are very, very, very ugly.
  // so I think this is a better way: expose this 3 static const arrays directly.
  static const ObAccuracy DDL_DEFAULT_ACCURACY[ObMaxType];
  static const ObAccuracy MAX_ACCURACY[ObMaxType];
  static const ObAccuracy DML_DEFAULT_ACCURACY[ObMaxType];
public:
  TO_STRING_KV(N_LENGTH, length_,
               N_PRECISION, precision_,
               N_SCALE, scale_);
  NEED_SERIALIZE_AND_DESERIALIZE;

  union
  {
    int64_t accuracy_;
    struct {
      ObLength length_;//count in charater. NOT byte
      ObPrecision precision_;
      ObScale scale_;
    };
  };
};

}
}

#endif /* OCEANBASE_COMMON_OB_ACCURACY_ */
