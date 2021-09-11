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

#ifndef OBPROXY_PART_MGR_H
#define OBPROXY_PART_MGR_H

#include "lib/allocator/page_arena.h"
#include "share/part/ob_part_mgr.h"
#include "proxy/route/ob_route_struct.h"
#include "opsql/expr_resolver/ob_expr_resolver.h"
namespace oceanbase
{
namespace common
{
class ObPartDesc;
}
namespace obproxy
{
class ObResultSetFetcher;
namespace proxy
{

class ObPartNameIdPair
{
public:
  ObPartNameIdPair() : part_id_(-1)
  {
    part_name_buf_[0] = '\0';
  }
  ~ObPartNameIdPair() {}

  int set_part_name(const common::ObString &part_name);
  void set_part_id(int64_t part_id) { part_id_ = part_id; }

  const common::ObString get_part_name() const { return part_name_; }
  int64_t get_part_id() const { return part_id_; }

  int assign(const ObPartNameIdPair &other);
  int64_t to_string(char *buf, const int64_t buf_len) const;
public:
  common::ObString part_name_;

private:
  int64_t part_id_;
  char part_name_buf_[common::OB_MAX_PARTITION_NAME_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObPartNameIdPair);
};

inline int ObPartNameIdPair::set_part_name(const common::ObString &part_name)
{
  int ret = common::OB_SUCCESS;
  if (part_name.length() > common::OB_MAX_PARTITION_NAME_LENGTH) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WARN, "invalid part name", K(part_name), K(ret));
  } else {
    MEMCPY(part_name_buf_, part_name.ptr(), part_name.length());
    part_name_.assign_ptr(part_name_buf_, part_name.length());
  }
  return ret;
}

inline int ObPartNameIdPair::assign(const ObPartNameIdPair &other)
{
  int ret = common::OB_SUCCESS;
  set_part_id(other.get_part_id());
  ret = set_part_name(other.get_part_name());
  return ret;
}

class ObProxyPartMgr
{
public:
  explicit ObProxyPartMgr(common::ObIAllocator &allocator);
  ~ObProxyPartMgr() { }

  int get_part_with_part_name(const common::ObString &part_name,
                              int64_t &part_id_);
  int get_first_part_id_by_idx(const int64_t idx, int64_t &part_id);
  int get_first_part(common::ObNewRange &range,
                     common::ObIAllocator &allocator,
                     common::ObIArray<int64_t> &part_ids);
  int get_sub_part(ObNewRange &range,
                   ObIAllocator &allocator,
                   ObPartDesc *sub_part_desc_ptr,
                   ObIArray<int64_t> &part_ids);

  int get_sub_part_by_random(const int64_t rand_num, 
                             ObPartDesc *sub_part_desc_ptr,
                             ObIArray<int64_t> &part_ids);

  int build_hash_part(const bool is_oracle_mode,
                      const share::schema::ObPartitionLevel part_level,
                      const share::schema::ObPartitionFuncType part_func_type,
                      const int64_t part_num,
                      const int64_t part_space,
                      const bool is_template_table,
                      const ObProxyPartKeyInfo &key_info,
                      ObResultSetFetcher *rs_fetcher = NULL);
  int build_sub_hash_part_with_non_template(const bool is_oracle_mode,
                                            const share::schema::ObPartitionFuncType part_func_type,
                                            const int64_t part_space,
                                            const ObProxyPartKeyInfo &key_info,
                                            ObResultSetFetcher &rs_fetcher);
  int build_key_part(const share::schema::ObPartitionLevel part_level,
                     const share::schema::ObPartitionFuncType part_func_type,
                     const int64_t part_num,
                     const int64_t part_space,
                     const bool is_template_table,
                     const ObProxyPartKeyInfo &key_info,
                     ObResultSetFetcher *rs_fetcher = NULL);
  int build_sub_key_part_with_non_template(const share::schema::ObPartitionFuncType part_func_type,
                                           const int64_t part_space,
                                           const ObProxyPartKeyInfo &key_info,
                                           ObResultSetFetcher &rs_fetcher);
  int build_range_part(const share::schema::ObPartitionLevel part_level,
                       const share::schema::ObPartitionFuncType part_func_type,
                       const int64_t part_num,
                       const bool is_template_table,
                       const ObProxyPartKeyInfo &key_info,
                       ObResultSetFetcher &rs_fetcher);
  int build_sub_range_part_with_non_template(const share::schema::ObPartitionFuncType part_func_type,
                                             const ObProxyPartKeyInfo &key_info,
                                             ObResultSetFetcher &rs_fetcher);
  int build_list_part(const share::schema::ObPartitionLevel part_level,
                      const share::schema::ObPartitionFuncType part_func_type,
                      const int64_t part_num,
                      const bool is_template_table,
                      const ObProxyPartKeyInfo &key_info,
                      ObResultSetFetcher &rs_fetcher);
  int build_sub_list_part_with_non_template(const share::schema::ObPartitionFuncType part_func_type,
                                            const ObProxyPartKeyInfo &key_info,
                                            ObResultSetFetcher &rs_fetcher);

  bool is_first_part_valid() const { return NULL != first_part_desc_; }
  bool is_sub_part_valid() const { return NULL != sub_part_desc_; }
  int get_first_part_num(int64_t &num);
  int get_sub_part_num_by_first_part_id(ObProxyPartInfo &part_info,
                                        const int64_t first_part_id,
                                        int64_t &num);
  int get_sub_part_desc_by_first_part_id(const bool is_template_table,
                                         const int64_t first_part_id,
                                         ObPartDesc *&sub_part_desc_ptr);

  common::ObObjType get_first_part_type() const;
  common::ObObjType get_sub_part_type() const;

  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  common::ObPartDesc *first_part_desc_;
  common::ObPartDesc *sub_part_desc_; // may be we need a array here
  int64_t first_part_num_;
  int64_t *sub_part_num_;
  ObPartNameIdPair* part_pair_array_; // first part name id pair array
  common::ObIAllocator &allocator_;
};

} // namespace proxy
} // namespace obproxy
} // namespace oceanbase
#endif // OBPROXY_PART_MGR_H
