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

#ifndef OCEANBASE_COMMON_OB_MEMBER_LIST_H_
#define OCEANBASE_COMMON_OB_MEMBER_LIST_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "common/ob_member.h"
namespace oceanbase
{
namespace common
{
class ObMemberList
{
  OB_UNIS_VERSION(1);

public:
  ObMemberList();
  ~ObMemberList();

public:
  void reset();
  bool is_valid() const;
  int add_member(const common::ObMember &member);
  int add_server(const common::ObAddr &server);
  // This function will determine the timestamp corresponding to the server when remove_member
  int remove_member(const common::ObMember &member);
/ // This function will not determine the timestamp corresponding to the server when removing_server
  int remove_server(const common::ObAddr &server);
  int64_t get_member_number() const;
  int get_server_by_index(const int64_t index, common::ObAddr &server) const;
  int get_member_by_index(const int64_t index, common::ObMember &server_ex) const;
  bool contains(const common::ObAddr &server) const;
  int deep_copy(const ObMemberList &member_list);
  ObMemberList &operator=(const ObMemberList &member_list);

  int64_t to_string(char *buf, const int64_t buf_len) const;
  TO_YSON_KV(ID(member), common::ObArrayWrap<common::ObMember>(member_, member_number_));
private:
  int64_t member_number_;
  common::ObMember member_[OB_MAX_MEMBER_NUMBER];
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_MEMBER_LIST_H_
