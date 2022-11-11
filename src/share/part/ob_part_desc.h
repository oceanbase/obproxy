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
  ObPartDescCtx() : session_info_(NULL), need_accurate_(false), cluster_version_(0) {}
  ObPartDescCtx(obproxy::proxy::ObClientSessionInfo *session_info)
    : session_info_(session_info), need_accurate_(false), cluster_version_(0) {}
  ObPartDescCtx(obproxy::proxy::ObClientSessionInfo *session_info, bool need_accurate, const int64_t cluster_version)
    : session_info_(session_info), need_accurate_(need_accurate), cluster_version_(cluster_version) {}
  ~ObPartDescCtx() {}

  obproxy::proxy::ObClientSessionInfo *get_session_info() { return session_info_; }
  bool need_accurate() { return need_accurate_; }
  int64_t get_cluster_version() const { return cluster_version_; }

private:
  obproxy::proxy::ObClientSessionInfo *session_info_;

  /* need accurate the part key result or not, currently only support insert stmt, need to support update set=x stmt */
  bool need_accurate_;
  int64_t cluster_version_;
};

class ObPartDesc
{
public:
  ObPartDesc() : part_level_(share::schema::PARTITION_LEVEL_ZERO), tablet_id_array_(NULL) {}
  virtual ~ObPartDesc() {}

  /*
   * get partition id according to range
   * @in param  range
   * @in param  allocator: use to type conversion
   *
   * @out param part_ids: list of part id
   *
   */
  virtual int get_part(common::ObNewRange &range,
                       common::ObIAllocator &allocator,
                       common::ObIArray<int64_t> &part_ids,
                       ObPartDescCtx &ctx,
                       common::ObIArray<int64_t> &tablet_ids);
  virtual int get_part_by_num(const int64_t num, common::ObIArray<int64_t> &part_ids,
                              common::ObIArray<int64_t> &tablet_ids);
  void set_part_level(share::schema::ObPartitionLevel part_level) { part_level_ = part_level; }
  share::schema::ObPartitionLevel get_part_level() { return part_level_; }
  void set_part_func_type(share::schema::ObPartitionFuncType part_func_type) { part_func_type_ = part_func_type; }
  share::schema::ObPartitionFuncType get_part_func_type() { return part_func_type_; }
  int build_dtc_params(obproxy::proxy::ObClientSessionInfo *session_info,
                       ObObjType obj_type,
                       ObDataTypeCastParams &dtc_params);
  void set_accuracy(const ObProxyPartKeyAccuracy &accuracy);

  DECLARE_VIRTUAL_TO_STRING = 0;
  share::schema::ObPartitionLevel part_level_;
  share::schema::ObPartitionFuncType part_func_type_;
  ObProxyPartKeyAccuracy accuracy_;
  int64_t *tablet_id_array_;
};

}
}

#endif /* _OB_PART_DESC_H */


