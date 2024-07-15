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

#ifndef _OB_PART_DESC_H
#define _OB_PART_DESC_H 1

#include "common/ob_range2.h"
#include "lib/container/ob_iarray.h"
#include "lib/timezone/ob_time_convert.h"
#include "share/part/ob_part_mgr_util.h"
#include "obproxy/opsql/expr_parser/ob_expr_parse_result.h"
#include "common/ob_row.h"

namespace oceanbase
{

namespace obproxy
{
namespace proxy
{
class ObClientSessionInfo;
}
}

namespace common
{

class ObPartDescCtx {
public:
  ObPartDescCtx() : session_info_(NULL), need_accurate_(false), cluster_version_(0),
                    need_get_whole_range_(false), calc_first_partition_(false), calc_last_partition_(false) {}
  ObPartDescCtx(obproxy::proxy::ObClientSessionInfo *session_info)
    : session_info_(session_info), need_accurate_(false), cluster_version_(0),
      need_get_whole_range_(false), calc_first_partition_(false), calc_last_partition_(false) {}
  ObPartDescCtx(obproxy::proxy::ObClientSessionInfo *session_info, bool need_accurate, const int64_t cluster_version)
    : session_info_(session_info), need_accurate_(need_accurate), cluster_version_(cluster_version),
      need_get_whole_range_(false), calc_first_partition_(false), calc_last_partition_(false) {}
  ~ObPartDescCtx() {}

  obproxy::proxy::ObClientSessionInfo *get_session_info() { return session_info_; }
  bool need_accurate() const { return need_accurate_; }
  int64_t get_cluster_version() const { return cluster_version_; }
  bool need_get_whole_range() const { return need_get_whole_range_; }
  bool calc_first_partition() const { return calc_first_partition_; }
  bool calc_last_partition() const { return calc_last_partition_; }

  void set_need_get_whole_range(bool need_get_whole_range) { need_get_whole_range_ = need_get_whole_range; }
  void set_calc_first_partition(bool calc_first_partition) { calc_first_partition_ = calc_first_partition; }
  void set_calc_last_partition(bool calc_last_partition) { calc_last_partition_ = calc_last_partition; }

private:
  obproxy::proxy::ObClientSessionInfo *session_info_;

  /* need accurate the part key result or not, currently only support insert stmt, need to support update set=x stmt */
  bool need_accurate_;
  int64_t cluster_version_;
  // for obkv
  // In some cases, it is necessary to calculate all partitions
  bool need_get_whole_range_;
  // When calculating the secondary partition, you need to pay attention to whether it is the first or last partition.
  bool calc_first_partition_;
  bool calc_last_partition_;
};

class ObPartDesc
{
public:
  ObPartDesc() : part_level_(share::schema::PARTITION_LEVEL_ZERO), tablet_id_array_(NULL), ls_id_array_(NULL) {}
  virtual ~ObPartDesc() {}

  /*
   * get partition id according to range
   * @in param  range
   * @in param  allocator: use to type conversion
   *
   * @out param part_ids: list of part id
   *
   */
  virtual int get_part(ObNewRange &range,
                       ObIAllocator &allocator,
                       ObIArray<int64_t> &part_ids,
                       ObPartDescCtx &ctx,
                       ObIArray<int64_t> &tablet_ids,
                       int64_t &part_idx);
  virtual int get_part_by_num(const int64_t num, ObIArray<int64_t> &part_ids,
                              ObIArray<int64_t> &tablet_ids);
  virtual int get_ls_id_by_num(const int64_t num, ObIArray<int64_t> &ls_ids);
  virtual int get_part_for_obkv(ObNewRange &range,
                                ObIAllocator &allocator,
                                ObIArray<int64_t> &part_ids,
                                ObPartDescCtx &ctx,
                                ObIArray<int64_t> &tablet_ids,
                                ObIArray<int64_t> &ls_ids);
  virtual int get_all_part_id_for_obkv(ObIArray<int64_t> &part_ids,
                                       ObIArray<int64_t> &tablet_ids,
                                       ObIArray<int64_t> &ls_ids);
  void set_part_level(share::schema::ObPartitionLevel part_level) { part_level_ = part_level; }
  share::schema::ObPartitionLevel get_part_level() { return part_level_; }
  void set_part_func_type(share::schema::ObPartitionFuncType part_func_type) { part_func_type_ = part_func_type; }
  share::schema::ObPartitionFuncType get_part_func_type() { return part_func_type_; }
  int build_dtc_params(obproxy::proxy::ObClientSessionInfo *session_info,
                       ObObjType obj_type,
                       ObDataTypeCastParams &dtc_params);
  ObIArray<ObAccuracy> &get_accuracies() { return accuracies_; }
  int set_accuracies(int64_t pos, const ObAccuracy &accuracy);

  int cast_obj(ObObj &src_obj,
               ObObj &target_obj,
               ObIAllocator &allocator,
               ObPartDescCtx &ctx,
               ObAccuracy &accuracy);

  int cast_obj(ObObj &src_obj,
               ObObjType obj_type,
               ObCollationType cs_type,
               ObIAllocator &allocator,
               ObPartDescCtx &ctx,
               ObAccuracy &accuracy);
  int cast_obj_for_obkv(ObObj &src_obj,
                        ObObj &target_obj,
                        ObIAllocator &allocator,
                        ObPartDescCtx &ctx,
                        ObAccuracy &accuracy);

  int cast_obj_for_obkv(ObObj &src_obj,
                        ObObjType obj_type,
                        ObCollationType cs_type,
                        ObIAllocator &allocator,
                        ObPartDescCtx &ctx,
                        ObAccuracy &accuracy);
  static int decimal_int_murmur_hash(const ObObj &val, const uint64_t seed, uint64_t &res);

  DECLARE_VIRTUAL_TO_STRING = 0;
  virtual int64_t to_plain_string(char* buf, const int64_t buf_len) const = 0;
  share::schema::ObPartitionLevel part_level_;
  share::schema::ObPartitionFuncType part_func_type_;
  ObSEArray<ObAccuracy, 4> accuracies_;
  int64_t *tablet_id_array_;
  int64_t *ls_id_array_;
};

}
}

#endif /* _OB_PART_DESC_H */


