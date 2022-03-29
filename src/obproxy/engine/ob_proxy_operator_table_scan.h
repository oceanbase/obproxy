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

#ifndef OBPROXY_OB_PROXY_OPERATOR_TABLE_SCAN_H
#define OBPROXY_OB_PROXY_OPERATOR_TABLE_SCAN_H

#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"

#include "ob_proxy_operator.h"
#include "ob_proxy_operator_async_task.h"

namespace oceanbase {
namespace obproxy {
namespace engine {

class ObProxyTableScanOp : public ObProxyOperator
{
public:
  ObProxyTableScanOp(ObProxyOpInput *input, common::ObIAllocator &allocator)
    : ObProxyOperator(input, allocator), sub_sql_count_(0) {
    set_op_type(PHY_TABLE_SCAN);
  }

  virtual ~ObProxyTableScanOp() {};

  virtual int open(event::ObContinuation *cont, event::ObAction *&action, const int64_t timeout_ms = 0);
  virtual int get_next_row();

  virtual ObProxyOperator* get_child(const uint32_t idx);// { return NULL; }

  virtual int handle_result(void *src, bool &is_final, ObProxyResultResp *&result);
  virtual int handle_response_result(void *src, bool &is_final, ObProxyResultResp *&result);

  int64_t get_sub_sql_count() { return sub_sql_count_; }
  void set_sub_sql_count(int64_t count) { sub_sql_count_ = count; }

private:
  int set_index();
  template <typename T>
  int set_index_for_exprs(common::ObIArray<T*> &expr_array);
  int set_index_for_expr(opsql::ObProxyExpr *expr);

protected:
  int64_t sub_sql_count_;
};

class ObProxyTableScanInput : public ObProxyOpInput
{
public:
  ObProxyTableScanInput() : ObProxyOpInput(), request_sql_(),
    table_name_maps_(ObModIds::OB_SE_ARRAY_ENGINE, ENGINE_ARRAY_NEW_ALLOC_SIZE),
    db_key_names_(ObModIds::OB_SE_ARRAY_ENGINE, ENGINE_ARRAY_NEW_ALLOC_SIZE),
    shard_props_(ObModIds::OB_SE_ARRAY_ENGINE, ENGINE_ARRAY_NEW_ALLOC_SIZE),
    calc_exprs_(ObModIds::OB_SE_ARRAY_ENGINE, ENGINE_ARRAY_NEW_ALLOC_SIZE),
    agg_exprs_(ObModIds::OB_SE_ARRAY_ENGINE, ENGINE_ARRAY_NEW_ALLOC_SIZE),
    group_exprs_(ObModIds::OB_SE_ARRAY_ENGINE, ENGINE_ARRAY_NEW_ALLOC_SIZE),
    order_exprs_(ObModIds::OB_SE_ARRAY_ENGINE, ENGINE_ARRAY_NEW_ALLOC_SIZE) {}

  ~ObProxyTableScanInput();

  void set_request_sql(const ObString &request_sql) { request_sql_ = request_sql; }
  ObString &get_request_sql() { return request_sql_; }

  int set_db_key_names(const common::ObIArray<dbconfig::ObShardConnector*> &db_key_names) {
    return db_key_names_.assign(db_key_names);
  }
  common::ObIArray<dbconfig::ObShardConnector*> &get_db_key_names() {
    return db_key_names_;
  }

  int set_shard_props(const common::ObIArray<dbconfig::ObShardProp*> &shard_props) {
    return shard_props_.assign(shard_props);
  }
  common::ObIArray<dbconfig::ObShardProp*> &get_shard_props() {
    return shard_props_;
  }

  int set_table_name_maps(common::ObIArray<hash::ObHashMapWrapper<common::ObString, common::ObString> > &table_name_map_array) {
    return table_name_maps_.assign(table_name_map_array);
  }
  common::ObIArray<hash::ObHashMapWrapper<common::ObString, common::ObString> > &get_table_name_maps() {
    return table_name_maps_;
  }

  int set_calc_exprs(common::ObIArray<opsql::ObProxyExpr*> &calc_exprs) {
    return calc_exprs_.assign(calc_exprs);
  }
  common::ObIArray<opsql::ObProxyExpr*> &get_calc_exprs() {
    return calc_exprs_;
  }

  int set_agg_exprs(common::ObIArray<opsql::ObProxyExpr*> &agg_exprs) {
    return agg_exprs_.assign(agg_exprs);
  }
  common::ObIArray<opsql::ObProxyExpr*> &get_agg_exprs() {
    return agg_exprs_;
  }

  int set_group_exprs(common::ObIArray<opsql::ObProxyGroupItem*> &group_exprs) {
    return group_exprs_.assign(group_exprs);
  }
  common::ObIArray<opsql::ObProxyGroupItem*> &get_group_exprs() {
    return group_exprs_;
  }

  int set_order_exprs(common::ObIArray<opsql::ObProxyOrderItem*> &order_exprs) {
    return order_exprs_.assign(order_exprs);
  }
  common::ObIArray<opsql::ObProxyOrderItem*> &get_order_exprs() {
    return order_exprs_;
  }

private:
  ObString request_sql_;
  ObSEArray<hash::ObHashMapWrapper<common::ObString, common::ObString>, 4> table_name_maps_;
  common::ObSEArray<dbconfig::ObShardConnector*, 4> db_key_names_;
  common::ObSEArray<dbconfig::ObShardProp*, 4> shard_props_;
  common::ObSEArray<opsql::ObProxyExpr*, 4> calc_exprs_;
  common::ObSEArray<opsql::ObProxyExpr*, 4> agg_exprs_;
  common::ObSEArray<opsql::ObProxyGroupItem*, 4> group_exprs_;
  common::ObSEArray<opsql::ObProxyOrderItem*, 4> order_exprs_;
};

}
}
}

#endif //OBPROXY_OB_PROXY_OPERATOR_TABLE_SCAN_H
