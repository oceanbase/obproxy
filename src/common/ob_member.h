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

#ifndef OCEANBASE_COMMON_OB_MEMBER_H_
#define OCEANBASE_COMMON_OB_MEMBER_H_

#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
class ObMember
{
public:
  ObMember();
  ObMember(const common::ObAddr &server,
           const int64_t timestamp);
public:
  const common::ObAddr &get_server() const;
  int64_t get_timestamp() const;
  void reset();
  bool is_valid() const;

  friend bool operator==(const ObMember &lhs, const ObMember &rhs);
  friend bool operator<(const ObMember &lhs, const ObMember &rhs);
  ObMember &operator=(const ObMember &rhs);

  TO_STRING_KV(K_(server), K_(timestamp), K_(flag));
  TO_YSON_KV(Y_(server), ID(t), timestamp_, Y_(flag));
  OB_UNIS_VERSION(1);
private:
  common::ObAddr server_;
  int64_t timestamp_;
  int64_t flag_;
};

inline bool operator==(const ObMember &lhs, const ObMember &rhs)
{
  return (lhs.server_ == rhs.server_) && (lhs.timestamp_ == rhs.timestamp_)
         && (lhs.flag_ == rhs.flag_);
}

inline bool operator<(const ObMember &lhs, const ObMember &rhs)
{
  return lhs.server_ < rhs.server_;
}
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_MEMBER_H_
