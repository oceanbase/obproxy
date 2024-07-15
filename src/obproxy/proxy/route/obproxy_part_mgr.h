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
class ObPartDescCtx;
}
namespace obproxy
{
class ObResultSetFetcher;
namespace proxy
{
class ObProxyExprCalculator;
class ObServerRoute;

class ObProxyPartMgr
{
private:
  const static int64_t MAX_PART_NAME_LENGTH = common::OB_MAX_PARTITION_NAME_LENGTH;//64
  typedef char PART_NAME_BUF[MAX_PART_NAME_LENGTH];
  typedef hash::ObHashMap<ObString,int64_t,hash::NoPthreadDefendMode> PartNameIdMap;
public:
  explicit ObProxyPartMgr(common::ObIAllocator &allocator);
  ~ObProxyPartMgr() { }
  void destroy();
  
  int get_part_with_part_name(const common::ObString &part_name,
                              int64_t &part_id,
                              ObProxyPartInfo &part_info,
                              ObServerRoute &route,
                              ObProxyExprCalculator &expr_calculator);
  int get_first_part_id_by_random(const int64_t rand_num, 
                                  int64_t &first_part_id,
                                  int64_t &tablet_id);
  int get_first_part(common::ObNewRange &range,
                     common::ObIAllocator &allocator,
                     common::ObIArray<int64_t> &part_ids,
                     common::ObPartDescCtx &ctx,
                     common::ObIArray<int64_t> &tablet_ids,
                     int64_t &part_idx);
  int get_sub_part(common::ObNewRange &range,
                   common::ObIAllocator &allocator,
                   common::ObPartDesc *sub_part_desc_ptr,
                   common::ObIArray<int64_t> &part_ids,
                   common::ObPartDescCtx &ctx,
                   common::ObIArray<int64_t> &tablet_ids,
                   int64_t &sub_part_idx);

  int get_sub_part_by_random(const int64_t rand_num,
                   common::ObPartDescCtx &ctx);
  int get_sub_part_for_obkv(common::ObNewRange &range,
                            common::ObIAllocator &allocator,
                            common::ObPartDesc *sub_part_desc_ptr,
                            common::ObIArray<int64_t> &part_ids,
                            common::ObPartDescCtx &ctx,
                            common::ObIArray<int64_t> &tablet_ids,
                            common::ObIArray<int64_t> &ls_ids);
  int get_first_part_for_obkv(common::ObNewRange &range,
                              common::ObIAllocator &allocator,
                              common::ObIArray<int64_t> &part_ids,
                              common::ObPartDescCtx &ctx,
                              common::ObIArray<int64_t> &tablet_ids,
                              common::ObIArray<int64_t> &ls_ids);
  int get_sub_part_by_random(const int64_t rand_num, 
                             common::ObPartDesc *sub_part_desc_ptr,
                             common::ObIArray<int64_t> &part_ids,
                             common::ObIArray<int64_t> &tablet_ids);

  int build_hash_part(const bool is_oracle_mode,
                      const share::schema::ObPartitionLevel part_level,
                      const share::schema::ObPartitionFuncType part_func_type,
                      const int64_t part_num,
                      const int64_t part_space,
                      const int64_t part_col_num,
                      const bool is_template_table,
                      const ObProxyPartKeyInfo &key_info,
                      ObResultSetFetcher *rs_fetcher,
                      const int64_t cluster_version);
  int build_sub_hash_part_with_non_template(const bool is_oracle_mode,
                                            const share::schema::ObPartitionFuncType part_func_type,
                                            const int64_t part_space,
                                            const int64_t part_col_num,
                                            const ObProxyPartKeyInfo &key_info,
                                            ObResultSetFetcher &rs_fetcher,
                                            const int64_t cluster_version);
  int build_key_part(const share::schema::ObPartitionLevel part_level,
                     const share::schema::ObPartitionFuncType part_func_type,
                     const int64_t part_num,
                     const int64_t part_space,
                     const int64_t part_col_num,
                     const bool is_template_table,
                     const ObProxyPartKeyInfo &key_info,
                     ObResultSetFetcher *rs_fetcher,
                     const int64_t cluster_version);
  int build_sub_key_part_with_non_template(const share::schema::ObPartitionFuncType part_func_type,
                                           const int64_t part_space,
                                           const int64_t part_col_num,
                                           const ObProxyPartKeyInfo &key_info,
                                           ObResultSetFetcher &rs_fetcher,
                                           const int64_t cluster_version);
  int build_range_part(const share::schema::ObPartitionLevel part_level,
                       const share::schema::ObPartitionFuncType part_func_type,
                       const int64_t part_num,
                       const int64_t part_col_num,
                       const bool is_template_table,
                       const ObProxyPartKeyInfo &key_info,
                       ObResultSetFetcher &rs_fetcher,
                       const int64_t cluster_version);
  int build_sub_range_part_with_non_template(const share::schema::ObPartitionFuncType part_func_type,
                                             const int64_t part_col_num,
                                             const ObProxyPartKeyInfo &key_info,
                                             ObResultSetFetcher &rs_fetcher,
                                             const int64_t cluster_verison);
  int build_list_part(const share::schema::ObPartitionLevel part_level,
                      const share::schema::ObPartitionFuncType part_func_type,
                      const int64_t part_num,
                      const int64_t part_col_num,
                      const bool is_template_table,
                      const ObProxyPartKeyInfo &key_info,
                      ObResultSetFetcher &rs_fetcher,
                      const int64_t cluster_version);
  int build_sub_list_part_with_non_template(const share::schema::ObPartitionFuncType part_func_type,
                                            const int64_t part_col_num,
                                            const ObProxyPartKeyInfo &key_info,
                                            ObResultSetFetcher &rs_fetcher,
                                            const int64_t cluster_version);

  bool is_first_part_valid() const { return NULL != first_part_desc_; }
  bool is_sub_part_valid() const { return NULL != sub_part_desc_; }
  int get_first_part_num(int64_t &num);
  int get_sub_part_num_by_first_part_id(ObProxyPartInfo &part_info,
                                        const int64_t first_part_id,
                                        int64_t &num);
  int get_sub_part_desc_by_first_part_id(const bool is_template_table,
                                         const int64_t first_part_id,
                                         ObPartDesc *&sub_part_desc_ptr,
                                         const int64_t cluster_version);

  void set_all_sub_part_num(int64_t all_sub_part_num);
  common::ObObjType get_first_part_type() const;
  common::ObObjType get_sub_part_type() const;
  void set_cluster_version(const int64_t cluster_version) { cluster_version_ = cluster_version; }
  const common::ObPartDesc *get_first_part_desc() { return first_part_desc_; }
  const common::ObPartDesc *get_sub_part_desc() { return sub_part_desc_; }

  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  int init_first_part_map();
  int init_sub_part_map();
  int alloc_name_buf(PART_NAME_BUF *&name_buf, int64_t *&name_len_buf, int64_t *&part_id_buf, const int64_t part_num);
  void free_name_buf(PART_NAME_BUF *name_buf, int64_t *name_len_buf, int64_t *part_id_buf);
  int build_part_name_id_map(const PART_NAME_BUF * const name_buf, 
                             const int64_t * const name_len_buf, 
                             const int64_t * const part_id_buf, 
                             char * target_part_name_buf,
                             const int64_t total_part_name_len,
                             const int64_t part_num, 
                             PartNameIdMap& part_name_id_map);
  int build_temp_sub_part_name_id_map(const PART_NAME_BUF * const name_buf, 
                                      const int64_t * const name_len_buf, 
                                      const int64_t * const part_id_buf, 
                                      const int64_t part_num);
private:
  common::ObPartDesc *first_part_desc_;
  common::ObPartDesc *sub_part_desc_; // may be we need a array here
  int64_t first_part_num_;
  int64_t *sub_part_num_;
  int64_t all_sub_part_num_;
  char * all_first_part_name_buf_; // first part name cancatenation string: "p1|p2|···|pn"
  char * all_sub_part_name_buf_;// sub part name cancatenation string: "p1sp1|p1sp2|···|pnspn"
  int64_t all_first_part_name_length_;
  int64_t all_sub_part_name_length_;
  PartNameIdMap  first_part_name_id_map_; // map K(first part name) to V(first part id)
  PartNameIdMap  sub_part_name_id_map_; // map K(sub part name) to V(physical part id)
  common::ObIAllocator &allocator_;
  int64_t cluster_version_;
};

inline void ObProxyPartMgr::set_all_sub_part_num(int64_t all_sub_part_num)
{
  all_sub_part_num_ = all_sub_part_num;
}
inline int ObProxyPartMgr::init_first_part_map()
{
  return first_part_name_id_map_.create(first_part_num_ * 1, ObModIds::OB_HASH_BUCKET, ObModIds::OB_HASH_NODE);//default load factor = 1.0
}
inline int ObProxyPartMgr::init_sub_part_map()
{
  return sub_part_name_id_map_.create(all_sub_part_num_ * 1, ObModIds::OB_HASH_BUCKET, ObModIds::OB_HASH_NODE);//default load factor = 1.0
}

} // namespace proxy
} // namespace obproxy
} // namespace oceanbase
#endif // OBPROXY_PART_MGR_H
