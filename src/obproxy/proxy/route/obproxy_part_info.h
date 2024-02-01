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

#ifndef OBPROXY_PART_INFO_H
#define OBPROXY_PART_INFO_H

#include "lib/allocator/page_arena.h"
#include "share/part/ob_part_mgr_util.h"
#include "opsql/expr_resolver/ob_expr_resolver.h"
#include "proxy/route/ob_route_struct.h"
#include "proxy/route/obproxy_part_mgr.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObProxyPartOption
{
public:
  ObProxyPartOption();
  ~ObProxyPartOption() {}
  bool is_range_part(const int64_t cluster_version) const;
  bool is_hash_part(const int64_t cluster_version) const;
  bool is_key_part(const int64_t cluster_version) const;
  bool is_list_part(const int64_t cluster_version) const;

  int64_t to_string(char *buf, const int64_t buf_len) const;

  share::schema::ObPartitionFuncType part_func_type_;
  int32_t part_space_;
  int64_t part_num_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyPartOption);
};

class ObProxyPartInfo
{
public:
  ObProxyPartInfo();
  ~ObProxyPartInfo() { }

  static int alloc(ObProxyPartInfo *&part_info);
  void free();

  bool is_valid() const { return true; } // TODO: check first part and sub part later
  bool has_first_part() const { return share::schema::PARTITION_LEVEL_ONE <= part_level_; }
  bool has_sub_part() const { return share::schema::PARTITION_LEVEL_TWO == part_level_; }
  bool has_generated_key() const { return has_generated_key_; }
  void set_has_generated_key(bool has_generated_key) { has_generated_key_ = has_generated_key; }
  bool is_oracle_mode() const { return is_oracle_mode_; }
  void set_oracle_mode(bool is_oracle_mode) { is_oracle_mode_ = is_oracle_mode; }
  bool has_unknown_part_key() const { return has_unknown_part_key_; }
  void set_unknown_part_key(bool has_unknown_part_key) { has_unknown_part_key_ = has_unknown_part_key; }
  bool is_template_table() const { return is_template_table_; }
  void set_template_table(bool is_template_table) { is_template_table_ = is_template_table; }
   bool is_primary_key_as_part_expr() const { return is_primary_key_as_part_expr_; }
  void set_primary_key_as_part_expr(bool is_pk_as_part_expr) { is_primary_key_as_part_expr_ = is_pk_as_part_expr; }

  share::schema::ObPartitionLevel get_part_level() const { return part_level_; }
  common::ObCollationType get_table_cs_type() const { return table_cs_type_; }
  common::ObIArray<common::ObString> &get_part_columns() { return part_columns_; }
  common::ObIArray<common::ObString> &get_sub_part_columns() { return sub_part_columns_; }
  ObProxyPartOption &get_first_part_option() { return first_part_option_; }
  ObProxyPartOption &get_sub_part_option() { return sub_part_option_; }
  ObProxyPartMgr &get_part_mgr() { return part_mgr_; }
  ObProxyPartKeyInfo &get_part_key_info() { return part_key_info_; }
  common::ObIAllocator &get_allocator() { return allocator_; }

  void set_part_level(const share::schema::ObPartitionLevel level) { part_level_ = level; }
  void set_table_cs_type(const common::ObCollationType cs_type) { table_cs_type_ = cs_type; }
  int64_t get_cluster_version() const { return cluster_version_; }
  void set_cluster_version(const int64_t cluster_version) { cluster_version_ = cluster_version; }

  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyPartInfo);

  bool is_oracle_mode_;
  bool has_generated_key_;
  bool has_unknown_part_key_;
  bool is_template_table_;
  bool is_primary_key_as_part_expr_;
  share::schema::ObPartitionLevel part_level_;
  common::ObCollationType table_cs_type_;

  common::ObSEArray<common::ObString, 2> part_columns_;
  common::ObSEArray<common::ObString, 2> sub_part_columns_;

  common::ObArenaAllocator allocator_;
  ObProxyPartOption first_part_option_;
  ObProxyPartOption sub_part_option_;
  ObProxyPartKeyInfo part_key_info_;
  ObProxyPartMgr part_mgr_;
  int64_t cluster_version_;
};

inline bool ObProxyPartOption::is_range_part(const int64_t cluster_version) const
{
  return share::schema::is_range_part(part_func_type_, cluster_version);
}

inline bool ObProxyPartOption::is_hash_part(const int64_t cluster_version) const
{
  return share::schema::is_hash_part(part_func_type_, cluster_version);
}

inline bool ObProxyPartOption::is_key_part(const int64_t cluster_version) const
{
  return share::schema::is_key_part(part_func_type_, cluster_version);
}

inline bool ObProxyPartOption::is_list_part(const int64_t cluster_version) const
{
  return share::schema::is_list_part(part_func_type_, cluster_version);
}

} // namespace proxy
} // namespace obproxy
} // namespace oceanbase
#endif // OBPROXY_PART_INFO_H
