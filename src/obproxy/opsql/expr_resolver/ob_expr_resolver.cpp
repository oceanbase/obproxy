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

#define USING_LOG_PREFIX PROXY

#include "opsql/expr_resolver/ob_expr_resolver.h"

#include "opsql/func_expr_resolver/ob_func_expr_resolver.h"
#include "opsql/func_expr_parser/ob_func_expr_parser.h"
#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr_factory.h"
#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr.h"
#include "proxy/route/obproxy_part_info.h"
#include "proxy/route/obproxy_expr_calculator.h"
#include "proxy/mysqllib/ob_proxy_mysql_request.h"
#include "proxy/mysqllib/ob_mysql_request_analyzer.h"
#include "proxy/mysqllib/ob_proxy_session_info.h"
#include "proxy/mysql/ob_prepare_statement_struct.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "utils/ob_proxy_utils.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "common/ob_obj_compare.h"
#include "lib/utility/ob_print_utils.h"
#include "proxy/route/ob_route_diagnosis.h"
#include "common/expression/ob_expr_util.h"

#include "lib/rowid/ob_urowid.h"
#include "lib/data_structure/ob_adaptive_map.h"
#include "lib/data_structure/ob_union_find_set.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::dbconfig;

namespace oceanbase
{
namespace obproxy
{
namespace opsql
{

int64_t ObExprResolverResult::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  for (int64_t i = 0; i < OBPROXY_MAX_PART_LEVEL; ++i) {
    databuff_printf(buf, buf_len, pos, " ranges_[%ld]:", i);

    if (ranges_[i].border_flag_.inclusive_start()) {
      databuff_printf(buf, buf_len, pos, "[");
    } else {
      databuff_printf(buf, buf_len, pos, "(");
    }
    pos += ranges_[i].start_key_.to_plain_string(buf + pos, buf_len - pos);
    databuff_printf(buf, buf_len, pos, " ; ");
    pos += ranges_[i].end_key_.to_plain_string(buf + pos, buf_len - pos);
    if (ranges_[i].border_flag_.inclusive_end()) {
      databuff_printf(buf, buf_len, pos, "]");
    } else {
      databuff_printf(buf, buf_len, pos, ")");
    }

    databuff_printf(buf, buf_len, pos, ",");
  }
  J_OBJ_END();
  return pos;
}


// todo : merge with calc_generated_key_value
int ObExprResolver::calc_generated_key_value_for_obkv(common::ObObj &obj, const ObProxyPartKey &part_key, const obkv::ObTableEntityType entity_type, common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_PROXY_EXPR_TYPE_FUNC_SUBSTR == part_key.func_type_) {
    //  we only support substr now
    ObCollationType collation = obj.get_collation_type();
    // todo : client and proxy can not get the shcema type of user table now
    if (entity_type == obkv::ObTableEntityType::ET_HKV) {
      collation = common::CS_TYPE_BINARY;
    }
    ObString output;
    ObString str = obj.get_string();
    int64_t start_pos = 0;
    int64_t length = 0;
    if (OB_UNLIKELY(OB_ISNULL(part_key.params_[0])
                    || PARAM_COLUMN != part_key.params_[0]->type_
                    || ObStringTC != obj.get_type_class())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("unexpected arg for generated calculation", K(ret), KP(part_key.params_[0]), K(obj));
    } else if (OB_UNLIKELY(OB_ISNULL(part_key.params_[1]))
              || OB_FAIL(ObFuncExprTool::calc_int_value_from_func_parser(part_key.params_[1], start_pos))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("unexpected arg for generated calculation", K(ret), KP(part_key.params_[1]));
    } else if (OB_UNLIKELY(OB_NOT_NULL(part_key.params_[2]))
               && OB_FAIL(ObFuncExprTool::calc_int_value_from_func_parser(part_key.params_[2], length))) {
      // params 2 counld be NULL
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("unexpected arg for generated calculation", K(ret), KP(part_key.params_[2]));
    } else if (obj.get_string().empty()) {
      // do nothing
      LOG_DEBUG("origin str is empty", K(ret));
    } else {
      length = OB_ISNULL(part_key.params_[2]) ? str.length() : length;
      int64_t mb_len = ObCharset::strlen_char(collation, str.ptr(), str.length());
      start_pos = (start_pos >= 0) ? (start_pos - 1) : start_pos + mb_len;
      LOG_DEBUG("calc substr generated key for obkv params:", K(str), K(length), K(start_pos), K(mb_len), K(str.length()));
      if (OB_UNLIKELY(start_pos < 0 || start_pos >= mb_len || length <= 0)) {
        output.assign(NULL, 0);
      } else {
        length = min(length, mb_len - start_pos);
        int64_t offset = ObCharset::charpos(collation, str.ptr(), str.length(), start_pos);
        length = ObCharset::charpos(collation,
                                    str.ptr() + offset,
                                    (offset == 0) ? str.length() : str.length() - offset,
                                    length);
        // length could be equal to str.length()
        if (offset >= str.length() || length > str.length()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected offset and length of str", K(str), K(offset), K(length), K(ret));
        } else {
          output.assign_ptr(str.ptr() + offset, length);
        }
      }
      if (OB_SUCC(ret)) {
        obj.set_string(obj.get_type(), output);
      }
      LOG_DEBUG("calc substr generated key for obkv", K(obj), K(output), K(output.length()));
    }

  } else if (OB_PROXY_EXPR_TYPE_FUNC_SUBSTR_INDEX == part_key.func_type_) {
    ObString output;
    ObString str = obj.get_string();
    ObString delim;
    int64_t count = 0;

    if (OB_UNLIKELY(OB_ISNULL(part_key.params_[0])
                    || PARAM_COLUMN != part_key.params_[0]->type_
                    || ObStringTC != obj.get_type_class())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("unexpected arg for generated calculation", K(ret), KP(part_key.params_[0]), K(obj));
    } else if (OB_UNLIKELY(OB_ISNULL(part_key.params_[1])
                           || OB_FAIL(ObFuncExprTool::calc_str_value_from_func_parser(part_key.params_[1], delim)))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("unexpected arg for generated calculation", K(ret), KP(part_key.params_[1]));
    } else if (OB_UNLIKELY(OB_ISNULL(part_key.params_[2])
                           || OB_FAIL(ObFuncExprTool::calc_int_value_from_func_parser(part_key.params_[2], count)))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("unexpected arg for generated calculation", K(ret), KP(part_key.params_[2]));
    } else if (obj.get_string().empty()
               || delim.empty()
               || count == 0) {
      // these case return empty string
      // 1. src str is empty
      // 2. delim str is empty
      // 3. count is 0
      obj.set_string(obj.get_type(), "");
    } else {
      bool is_reverse = count < 0;
      int32_t *next_arr = NULL;
      int64_t pos = -1;

      LOG_DEBUG("calc substring_index generated key for obkv", K(str), K(delim), K(count));
      if (OB_ISNULL(next_arr = static_cast<int32_t *>(op_fixed_mem_alloc(delim.length() * sizeof(int32_t))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to allocate mem for kmp next arr", K(ret));
      } else if (!is_reverse && OB_FAIL(ObExprUtil::kmp_next(delim.ptr(), delim.length(), next_arr))) {
        LOG_WDIAG("fail to init kmp next arr", K(ret));
      } else if (is_reverse && OB_FAIL(ObExprUtil::kmp_next_reverse(delim.ptr(), delim.length(), next_arr))) {
        LOG_WDIAG("fail to init kmp next arr", K(ret));
      } else {
        if (!is_reverse) {
          if (OB_FAIL(ObExprUtil::kmp(delim.ptr(), delim.length(), str.ptr(), str.length(), count, next_arr, pos))) {
            LOG_WDIAG("fail to calculate substr pos", K(ret));
          } else if (-1 < pos) {
            output.assign(str.ptr(), pos);
          }
        } else if (is_reverse) {
          if (OB_FAIL(ObExprUtil::kmp_reverse(delim.ptr(), delim.length(), str.ptr(), str.length(), count, next_arr, pos))) {
            LOG_WDIAG("fail to calculate substr pos", K(ret));
          } else if (-1 < pos) {
            output.assign(str.ptr() + pos + delim.length(), str.length() - delim.length() - pos);
          }
        }
      }
      if (-1 == pos) {
        output.assign(str.ptr(), str.length());
      }
      if (OB_NOT_NULL(next_arr)) {
        op_fixed_mem_free(next_arr, delim.length() * sizeof(int32_t));
      }
      if (OB_SUCC(ret)) {
        obj.set_string(obj.get_type(), output);
      }
      LOG_DEBUG("calc substring_index generated key for obkv", K(obj));
    }
  } else if (OB_PROXY_EXPR_TYPE_FUNC_ABS == part_key.func_type_) {
    if (OB_UNLIKELY(OB_ISNULL(part_key.params_[0]) || obj.is_null())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("unexpected arg for generated calculation", K(ret), KP(part_key.params_[0]), K(obj));
    } else if (PARAM_INT_VAL == part_key.params_[0]->type_ || ObIntType == obj.get_type()) {
      int64_t val = obj.get_int();
      obj.set_int(abs(val));
      LOG_DEBUG("calc abs generated key for obkv hbase", K(obj));
    } else {
      number::ObNumber res_nmb;
      if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(&allocator, obj, obj)))) {
        LOG_WDIAG("get number obj failed", K(ret), K(obj));
      } else {
        if (obj.get_number().is_negative()) {
          if (OB_FAIL(obj.get_number().negate(res_nmb, allocator))) {
            LOG_WDIAG("calc abs number failed", K(ret), K(obj));
          } else {
            obj.set_number(res_nmb);
          }
        }
      }
      LOG_DEBUG("calc abs generated key for obkv hbase", K(obj));
    }
  } else {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WDIAG("unknown generate function type", K(part_key.func_type_), K(ret));
  }
  return ret;
}

int64_t ObExprResolverV2::init(int64_t part_key_num)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_EDIAG("fail to init", K(part_key_num), K(ret));
  } else if (OB_FAIL(left_bound_arr_.prepare_allocate(part_key_num))) {
    //impossible
    LOG_EDIAG("fail to prepare_allocate left_bound_arr_", K(part_key_num), K(ret));
  } else if (OB_FAIL(equal_obj_arr_.prepare_allocate(part_key_num))) {
    //impossible
    LOG_EDIAG("fail to prepare_allocate equal_obj_arr_", K(part_key_num), K(ret));
  } else if (OB_FAIL(right_bound_arr_.prepare_allocate(part_key_num))) {
    //impossible
    LOG_EDIAG("fail to prepare_allocate right_bound_arr_", K(part_key_num), K(ret));
  } else if (OB_FAIL(left_bound_flag_arr_.prepare_allocate(part_key_num))) {
    //impossible
    LOG_EDIAG("fail to prepare_allocate left_bound_flag_arr_", K(part_key_num), K(ret));
  } else if (OB_FAIL(right_bound_flag_arr_.prepare_allocate(part_key_num))) {
    //impossible
    LOG_EDIAG("fail to prepare_allocate right_bound_flag_arr_", K(part_key_num), K(ret));
  } else {
    for (int64_t i = 0; i < part_key_num; ++i) {
      left_bound_arr_.at(i).set_type(ObUnknownType);
      equal_obj_arr_.at(i).set_type(ObUnknownType);
      right_bound_arr_.at(i).set_type(ObUnknownType);
      left_bound_flag_arr_.at(i) = false;
      right_bound_flag_arr_.at(i) = false;
    }
    part_key_num_ = part_key_num;
    is_inited_ = true;
  }

  return ret;
}

int64_t ObExprResolverV2::do_relation_obj_resolve(const ObExprParseResult& expr_parse_result)
{
  int ret = OB_SUCCESS;

  bool has_rowid = expr_parse_result.has_rowid_;
  const ObProxyRelationInfo& all_relation_info = expr_parse_result.all_relation_info_;
  int64_t relation_num = all_relation_info.relation_num_;
  ObPsIdEntry *ps_id_entry = NULL;
  ObMySQLCmd cmd = client_request_.get_packet_meta().cmd_;

  if (OB_MYSQL_COM_STMT_EXECUTE == cmd || OB_MYSQL_COM_STMT_SEND_LONG_DATA == cmd) {
    // parse execute param value for OB_MYSQL_COM_STMT_EXECUTE
    // try to get param types from OB_MYSQL_COM_STMT_EXECUTE while handling OB_MYSQL_COM_STMT_SEND_LONG_DATA
    ps_id_entry = client_info_.get_ps_id_entry();
    if (OB_ISNULL(ps_id_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("client ps id entry is null", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_FAIL(relation_obj_arr_.prepare_allocate(relation_num))) {
    LOG_EDIAG("fail to prepare_allocate relation_obj_arr_", K(relation_num), K(ret));
  } else if (OB_FAIL(relation_type_arr_.prepare_allocate(relation_num))) {
    LOG_EDIAG("fail to prepare_allocate relation_type_arr_", K(relation_num), K(ret));
  } else {
    // only get const expr here
    for (int64_t i = 0; OB_SUCC(ret) && i < relation_num; ++i) {
      if (OB_ISNULL(all_relation_info.relations_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected relation info pointer", K(ret));
      } else {
        ObProxyRelationExpr& relation = *(all_relation_info.relations_[i]);
        ObString column_name;
        ObObj& target_obj = relation_obj_arr_.at(i);
        target_obj.set_type(ObUnknownType);
        relation_type_arr_.at(i) = relation.type_;
        if (OB_ISNULL(relation.right_value_)
            || OB_ISNULL(relation.right_value_->head_)) {
          LOG_DEBUG("unexpected pointer, maybe unsupported data in right value, ignore", K(ret));
        } else if (OB_UNLIKELY(relation.right_value_->head_->type_ == TOKEN_COLUMN)) {
          // do union later
        } else if (OB_FAIL(resolve_token_list_const_obj(relation, part_info_,
                                    client_request_, client_info_, ps_id_entry, target_obj))) {
          LOG_DEBUG("fail to resolve token list, ignore the ret", K(target_obj), K(ret));  // DEBUG for simplify meaningless and futile log
          ret = OB_SUCCESS;
        } else if (OB_LIKELY(!has_rowid)){
          // nothing, ignore column name
        } else if (OB_ISNULL(relation.left_value_)
                   || OB_ISNULL(relation.left_value_->column_node_)) {
          // ignore, may be insert stmt without column
        } else if (FALSE_IT(column_name.assign_ptr(relation.left_value_->column_node_->column_name_.str_,
                                                   relation.left_value_->column_node_->column_name_.str_len_))) {
          // impossible
        } else if (OB_UNLIKELY(0 == column_name.case_compare(g_ROWID))) {
          if (OB_FAIL(rowid_obj_arr_.push_back(target_obj))) {
            LOG_WDIAG("fail to push back rowID obj" , K(column_name), K(target_obj));
          }
        }
      }
    }
  }

  return ret;
}

int64_t ObExprResolverV2::do_part_key_obj_prepare(const ObProxyRelationInfo& all_relation_info,
                                                  bool is_empty_column_insert_stmt)
{
  int ret = OB_SUCCESS;

  int64_t relation_num = all_relation_info.relation_num_;
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_num; ++i) {
    ObProxyRelationExpr& relation = *(all_relation_info.relations_[i]);
    ObObj& src_obj = relation_obj_arr_.at(i);
    ObString column_name;
    int64_t part_key_idx = -1;
    if (OB_UNLIKELY(src_obj.is_unknown())) {
      // noting
    } else {
      if (is_empty_column_insert_stmt) {
        // no column name in insert stmt
        part_key_idx = relation.part_key_idx_;
      } else if (OB_ISNULL(relation.left_value_)
                  || OB_ISNULL(relation.left_value_->column_node_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected relation info pointer", K(ret));
      } else {
        part_key_idx = relation.left_value_->column_node_->part_key_idx_;
      }

      if (OB_FAIL(ret)) {
        // nothing
      } else if (-1 == part_key_idx) {
        // not part key relation
      } else if (part_key_idx < 0 || part_key_idx >= part_key_num_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected relation info", K(part_key_idx), K(ret));
      } else {
        switch(relation_type_arr_.at(i)) {
          case F_COMP_EQ:
          case F_COMP_NSEQ: {
            equal_obj_arr_.at(part_key_idx) = src_obj;
            break;
          }
          case F_COMP_GE:
            left_bound_flag_arr_.at(part_key_idx) = true;
          case F_COMP_GT: {
            left_bound_arr_.at(part_key_idx) = src_obj;
            break;
          }
          case F_COMP_LE:
            right_bound_flag_arr_.at(part_key_idx) = true;
          case F_COMP_LT: {
            right_bound_arr_.at(part_key_idx) = src_obj;
            break;
          }
          default: {
            // continue
          }
        }
      }
    }
  }

  return ret;
}

int64_t ObExprResolverV2::do_equal_relation_ratiocination(const ObProxyRelationInfo& all_relation_info,
                                                          const ObProxyPartKeyInfo& part_key_info,
                                                          const ObString& origin_table_name,
                                                          const ObString& alias_table_name)
{
  int ret = OB_SUCCESS;

  int64_t relation_num = all_relation_info.relation_num_;
  bool need_ratiocination = false;
  for (int64_t i = 0; i < part_key_num_; ++i) {
    if (equal_obj_arr_.at(i).is_unknown()) {
      need_ratiocination = true;
      break;
    }
  }

  if (OB_LIKELY(!need_ratiocination)) {
    // ignore
  } else {
    // the mem of column_idx_map is individual from allocator_
    lib::ObAdaptiveMap<PartitionColumn, int64_t> column_idx_map;
    lib::ObUnionFindSet union_find_set;
    common::ObFixedArray<PartitionColumn, common::ObIAllocator> column_name_arr;
    ObSEArray<int64_t, OBPROXY_MAX_RELATION_NUM> merge_realtion_idx_arr;
    ObSEArray<ObObj, OBPROXY_MAX_RELATION_NUM> merge_realtion_obj_arr;

    column_name_arr.set_allocator(&allocator_);
    if (OB_FAIL(column_name_arr.reserve(relation_num * 2))) {
      LOG_EDIAG("fail to reserve column_name_arr", K(relation_num), K(ret));
    } else if (OB_FAIL(column_idx_map.init(relation_num * 2))) {
      LOG_WDIAG("fail to create column_idx_map", K(relation_num), K(ret));
    } else if (OB_FAIL(merge_realtion_obj_arr.prepare_allocate(relation_num * 2))) {
      LOG_WDIAG("fail to prepare_allocate merge_realtion_obj_arr", K(relation_num), K(ret));
    } else {
      for (int64_t i = 0; i < relation_num * 2; ++i) {
        merge_realtion_obj_arr.at(i).set_type(ObUnknownType);
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < relation_num; ++i) {
        if (OB_ISNULL(all_relation_info.relations_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected relation info pointer", K(ret));
        } else if (F_COMP_EQ != relation_type_arr_.at(i)
                   && F_COMP_NSEQ != relation_type_arr_.at(i)) {
          // no equal relation, ignore
        } else {
          int64_t idx = column_name_arr.count();
          PartitionColumn tmp_partition_column;
          ObProxyRelationExpr& relation = *(all_relation_info.relations_[i]);
          if (OB_ISNULL(relation.left_value_)
              || OB_ISNULL(relation.left_value_->column_node_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpected relation info value pointer", K(ret));
          } else {
            tmp_partition_column.assgin(*(relation.left_value_->column_node_));
            // should store the idx in column_name_arr  for column in left_value
            if (column_idx_map.is_exist(tmp_partition_column)) {
              if (OB_FAIL(column_idx_map.get(tmp_partition_column, idx))) {
                LOG_WDIAG("fail to get tmp_partition_column from column_idx_map", K(ret));
              }
            } else if (OB_FAIL(column_idx_map.set(tmp_partition_column, idx))) {
              LOG_WDIAG("fail to set tmp_partition_column to column_idx_map", K(ret));
            } else if (OB_FAIL(column_name_arr.push_back(tmp_partition_column))) {
              LOG_WDIAG("fail to push tmp_partition_column", K(ret));
            } else {
              // nothing
            }

            if (OB_SUCC(ret)
                && idx < merge_realtion_obj_arr.count()
                && merge_realtion_obj_arr.at(idx).is_unknown()
                && !relation_obj_arr_.at(i).is_unknown()) {
              merge_realtion_obj_arr.at(idx) = relation_obj_arr_.at(i);
            }
          }

          if (OB_FAIL(ret)) {
            // nothing
          } else if (OB_NOT_NULL(relation.right_value_)
                     && OB_NOT_NULL(relation.right_value_->head_)
                     && OB_NOT_NULL(relation.right_value_->column_node_)
                     && (relation.right_value_->head_->type_ == TOKEN_COLUMN)) {
            idx = column_name_arr.count();
            tmp_partition_column.assgin(*(relation.right_value_->column_node_));
            // should store the idx in column_name_arr for column in right_value
            if (column_idx_map.is_exist(tmp_partition_column)) {
              // no need to get idx
            } else if (OB_FAIL(column_idx_map.set(tmp_partition_column, idx))) {
              LOG_WDIAG("fail to set tmp_partition_column to column_idx_map", K(ret));
            } else if (OB_FAIL(column_name_arr.push_back(tmp_partition_column))) {
              LOG_WDIAG("fail to push tmp_partition_column", K(ret));
            } else {
              // nothing
            }

            if (OB_FAIL(ret)) {
              // nothing
            } else if (OB_FAIL(merge_realtion_idx_arr.push_back(i))) {
              // should store the idx in all_relation_info_
              LOG_WDIAG("fail to push i to merge_realtion_idx_arr", K(ret));
            } else {
              // nothing
            }
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      // nothing
    } else if (OB_FAIL(union_find_set.init(column_name_arr.count(), &allocator_))) {
      LOG_WDIAG("fail to init union_find_set", K(ret));
    } else {
      // do union
      for (int64_t i = 0; OB_SUCC(ret) && i < merge_realtion_idx_arr.count(); ++i) {
        int64_t idx_in_relation = merge_realtion_idx_arr.at(i);
        int64_t idx_left = 0;
        int64_t idx_right = 0;
        PartitionColumn left_column;
        PartitionColumn right_column;
        if (OB_ISNULL(all_relation_info.relations_[idx_in_relation])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected relation info pointer", K(idx_in_relation), K(ret));
        } else {
          ObProxyRelationExpr& relation = *(all_relation_info.relations_[idx_in_relation]);
          if (OB_ISNULL(relation.left_value_)
              || OB_ISNULL(relation.right_value_)
              || OB_ISNULL(relation.left_value_->column_node_)
              || OB_ISNULL(relation.right_value_->column_node_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpected relation info value pointer", K(idx_in_relation), K(ret));
          } else {
            left_column.assgin(*(relation.left_value_->column_node_));
            right_column.assgin(*(relation.right_value_->column_node_));
            if (OB_FAIL(column_idx_map.get(left_column, idx_left))) {
              LOG_WDIAG("unexpected relation info value pointer", K(left_column), K(idx_in_relation), K(ret));
            } else if (OB_FAIL(column_idx_map.get(right_column, idx_right))) {
              LOG_WDIAG("unexpected relation info value pointer", K(right_column), K(idx_in_relation), K(ret));
            } else {
              union_find_set.union_merge(idx_left, idx_right);
            }
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      // nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_name_arr.count(); ++i) {
        int64_t root_idx = -1;
        if (merge_realtion_obj_arr.at(i).is_unknown()) {
          // continue
        } else if (OB_FAIL(union_find_set.find(i, root_idx))) {
          LOG_WDIAG("fail to find from union_find_set", K(root_idx), K(i));
        } else if (root_idx < 0 || root_idx >= column_name_arr.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("fail to get root_idx from union_find_set", K(root_idx), K(i));
        } else if (!merge_realtion_obj_arr.at(root_idx).is_unknown()) {
          // continue
        } else {
          merge_realtion_obj_arr.at(root_idx) = merge_realtion_obj_arr.at(i);
        }
      }
    }

    if (OB_FAIL(ret)) {
      // nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < equal_obj_arr_.count(); ++i) {
        if (!equal_obj_arr_.at(i).is_unknown()) {
          // continue
        } else {
          int64_t idx = -1;
          int64_t root_idx = -1;
          PartitionColumn tmp_partition_column;
          const ObProxyParseString& parse_column_name = part_key_info.part_keys_[i].name_;
          tmp_partition_column.column_name_.assign_ptr(parse_column_name.str_, parse_column_name.str_len_);

          if (!origin_table_name.empty()) {
            tmp_partition_column.table_name_ = origin_table_name;
            if (OB_FAIL(column_idx_map.find(tmp_partition_column, idx))) {
              LOG_DEBUG("didn't find tmp_partition_column from column_idx_map using origin table name", K(tmp_partition_column),
                        K(origin_table_name), K(idx), K(ret));
              tmp_partition_column.table_name_.reset();
              ret = OB_SUCCESS;
            }
          }

          if (-1 != idx) {
            // continue
          } else if (!alias_table_name.empty()) {
            tmp_partition_column.table_name_ = alias_table_name;
            if (OB_FAIL(column_idx_map.find(tmp_partition_column, idx))) {
              LOG_DEBUG("didn't find tmp_partition_column from column_idx_map using alias table name", K(tmp_partition_column),
                        K(alias_table_name), K(idx), K(ret));
              tmp_partition_column.table_name_.reset();
              ret = OB_SUCCESS;
            }
          }

          if (-1 != idx) {
            // continue
          } else if (OB_FAIL(column_idx_map.find(tmp_partition_column, idx))) {
            LOG_DEBUG("fail to retry find tmp_partition_column from column_idx_map", K(tmp_partition_column), K(idx), K(ret));
          }

          if (OB_FAIL(ret)) {
            // wont`t influence other ratiocination
            ret = OB_SUCCESS;
          } else if (OB_UNLIKELY(idx < 0 || idx >= column_idx_map.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("error idx in column_idx_map", K(tmp_partition_column), K(idx), K(ret));
          } else if (OB_FAIL(union_find_set.find(idx, root_idx))) {
            LOG_WDIAG("error idx in column_idx_map", K(tmp_partition_column), K(idx), K(root_idx), K(ret));
          } else if (OB_UNLIKELY(root_idx < 0 || root_idx >= union_find_set.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("error idx in union_find_set", K(tmp_partition_column), K(root_idx), K(ret));
          } else if (merge_realtion_obj_arr.at(root_idx).is_unknown()) {
            // nothing
            LOG_DEBUG("no available equivalence result", K(tmp_partition_column), "column", column_name_arr.at(idx),
                        "obj", merge_realtion_obj_arr.at(idx), "root column", column_name_arr.at(root_idx),
                        "root obj", merge_realtion_obj_arr.at(root_idx), K(idx), K(root_idx), K(ret));
          } else {
            LOG_DEBUG("succ to get equivalence result", K(tmp_partition_column), "column", column_name_arr.at(idx),
                        "obj", merge_realtion_obj_arr.at(idx), "root column", column_name_arr.at(root_idx),
                        "root obj", merge_realtion_obj_arr.at(root_idx), K(idx), K(root_idx), K(ret));
            equal_obj_arr_.at(i) = merge_realtion_obj_arr.at(root_idx);
          }
        }
      }
    }
  } // end of need_ratiocination

  return ret;
}

int64_t ObExprResolverV2::do_relation_simplication()
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < part_key_num_; ++i) {
    if (!equal_obj_arr_.at(i).is_unknown()) {
      // nothing
    } else if (!left_bound_arr_.at(i).is_unknown()
                && !right_bound_arr_.at(i).is_unknown()
                && (left_bound_arr_.at(i) == right_bound_arr_.at(i))) {
      equal_obj_arr_.at(i) = left_bound_arr_.at(i);
    }
  }

  return ret;
}

int64_t ObExprResolverV2::do_rowid_calc(const ObProxyPartKeyInfo& part_key_info,
                                        int64_t &partition_id)
{
  int ret = OB_SUCCESS;

  ObObj src_obj;
  ObSEArray<ObObj, OBPROXY_MAX_PART_KEY_NUM> rowid_result_obj_arr;
  src_obj.set_type(ObUnknownType);

  if (OB_FAIL(rowid_result_obj_arr.reserve(part_key_num_))) {
    //impossible
    LOG_EDIAG("fail to prepare_allocate rowid_result_obj_arr", K(part_key_num_), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowid_obj_arr_.count(); ++i) {
      if (OB_LIKELY(!rowid_obj_arr_.at(i).is_unknown())) {
        src_obj = rowid_obj_arr_.at(i);
        break; // use first valid rowid value
      }
    }

    if (OB_UNLIKELY(src_obj.is_unknown())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to get target_obj for rowID", K(src_obj), K(ret));
    } else if (OB_FAIL(calc_partition_id_with_rowid(src_obj, partition_id, rowid_result_obj_arr))) {
      LOG_WDIAG("fail to calc partition_id with rowid", K(src_obj), K(partition_id), K(ret));
    } else if (OB_INVALID_INDEX != partition_id) {
      // have got partition_id from rowID
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_key_num_; ++i) {
        const ObProxyPartKey &part_key = part_key_info.part_keys_[i];
        ObObj &target_obj = equal_obj_arr_.at(i);
        if (part_key.idx_in_rowid_ < 0
            || part_key.idx_in_rowid_ >= rowid_result_obj_arr.count()) {
          // not in rowid, ignore
        } else if (!target_obj.is_unknown()) {
          // ignore set target_obj
        } else {
          ObObj &rowid_result_obj = rowid_result_obj_arr.at(part_key.idx_in_rowid_);
          ObObjType type = rowid_result_obj.get_type();
          if (ObCharType == type || ObNCharType == type) {
            int32_t val_len = rowid_result_obj.get_val_len();
            const char* obj_str = rowid_result_obj.get_string_ptr();
            while (val_len > 1) {
              if (OB_PADDING_CHAR == *(obj_str + val_len - 1)) {
                --val_len;
              } else {
                break;
              }
            }
            rowid_result_obj.set_string(type, rowid_result_obj.get_string_ptr(), val_len);
          }
          target_obj = rowid_result_obj;
        }
      }
    }
  }

  return ret;
}

int64_t ObExprResolverV2::do_default_value_set(const ObProxyPartKeyInfo& part_key_info)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < part_key_num_; ++i) {
    if (!equal_obj_arr_.at(i).is_unknown()) {
      // no need use default value
    } else if (OB_FAIL(parse_and_resolve_default_value(part_key_info.part_keys_[i].default_value_,
                                          client_info_, equal_obj_arr_.at(i), is_oracle_mode_))) {
      LOG_WDIAG("fail to resovle default value", K(i), K(ret));
    }
  }

  return ret;
}

int64_t ObExprResolverV2::do_generated_key_calc(const ObProxyPartKeyInfo& part_key_info)
{
  int ret = OB_SUCCESS;

  for (int64_t col_idx = 0; col_idx < part_key_num_; ++col_idx) {
    ObObj& src_obj = equal_obj_arr_.at(col_idx);
    int64_t target_idx = -1;
    ObObj target_obj;
    ObProxyExprType generated_func = ObProxyExprType::OB_PROXY_EXPR_TYPE_NONE;
    if (src_obj.is_unknown()) {
      // nothing
    } else if (part_key_info.part_keys_[col_idx].is_generated_) {
      // do nothing, user sql explicitly contains value for generated key, no need to calculate
    } else if (FALSE_IT(target_idx = part_key_info.part_keys_[col_idx].generated_col_idx_)) {
      // will not come here
    } else if (OB_UNLIKELY(0 > target_idx)) {
      LOG_DEBUG("this relation's part key is not used to generate column");
    } else if (OB_UNLIKELY(target_idx >= part_key_info.key_num_)
                || OB_UNLIKELY(!part_key_info.part_keys_[target_idx].is_generated_)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WDIAG("fail to get generated key value, source key is not offered",
                K(col_idx), K(part_key_info.key_num_), K(target_idx), K(ret));
    } else if (!equal_obj_arr_.at(target_idx).is_unknown()) {
      // generate part key has value, ignore now
      LOG_DEBUG("", "exist value", equal_obj_arr_.at(target_idx), K(src_obj));
    } else if (FALSE_IT(target_obj = src_obj)) {
      // impossible
    } else if (OB_FAIL(calc_generated_key_value(target_obj, part_key_info.part_keys_[col_idx],
                                                is_oracle_mode_))) {
      LOG_WDIAG("fail to get generated key value", K(col_idx), K(src_obj), K(target_obj), K(target_idx), K(ret));
    } else {
      generated_func = part_key_info.part_keys_[col_idx].func_type_;
      equal_obj_arr_.at(target_idx) = target_obj;
      LOG_DEBUG("succ to calculate generated key value", K(col_idx), K(src_obj), K(target_obj), K(target_idx), K(ret));
      ROUTE_DIAGNOSIS(route_diagnosis_, RESOLVE_TOKEN, resolve_token, ret, TOKEN_NONE, "", DEFAULT_EXPR_TYPE,
                      generated_func, DEFAULT_EXPR_TYPE, target_obj);
    }
  }

  return ret;
}

int64_t ObExprResolverV2::do_same_part_key_set(const ObProxyPartKeyInfo& part_key_info)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < part_key_num_; ++i) {
    const ObProxyPartKey& part_key_i = part_key_info.part_keys_[i];
    const ObString part_key_name_i(part_key_i.name_.str_len_, part_key_i.name_.str_);
    for (int64_t j = i + 1; j < part_key_num_; ++j) {
      const ObProxyPartKey& part_key_j = part_key_info.part_keys_[j];
      const ObString part_key_name_j(part_key_j.name_.str_len_, part_key_j.name_.str_);
      if (!(0 == part_key_name_i.case_compare(part_key_name_j))) {
        // nothing
      } else {
        equal_obj_arr_.at(j) = equal_obj_arr_.at(i);
        left_bound_arr_.at(j) = left_bound_arr_.at(i);
        right_bound_arr_.at(j) = right_bound_arr_.at(i);
      }
    }
  }

  return ret;
}

int64_t ObExprResolverV2::do_part_key_func_calc(const ObProxyPartKeyInfo& part_key_info,
                                                ObProxyPartInfo &part_info)
{
  int ret = OB_SUCCESS;

  if (part_info.get_part_level() >= share::schema::PARTITION_LEVEL_ONE
      && OB_FAIL(calc_part_key_func_level(part_key_info, first_part_func_result_, PART_KEY_LEVEL_ONE))) {
    LOG_WDIAG("fail to calc first part_key_func", K(ret));
  } else if (part_info.get_part_level() >= share::schema::PARTITION_LEVEL_TWO
             && OB_FAIL(calc_part_key_func_level(part_key_info, sub_part_func_result_, PART_KEY_LEVEL_TWO))){
    LOG_WDIAG("fail to calc sub part_key_func", K(ret));
  }

  return ret;
}

int64_t ObExprResolverV2::calc_part_key_func_level(const ObProxyPartKeyInfo& part_key_info,
                                                   ObIArray<ObObj>& func_result_arr,
                                                   ObProxyPartKeyLevel part_key_func_level)
{
  int ret = OB_SUCCESS;

  SqlFieldResult sql_field_result;
  ObProxyExprType part_key_func_type = ObProxyExprType::OB_PROXY_EXPR_TYPE_NONE;
  ObPartkeyFuncInfo part_key_func_info;

  // 1. only support one part func
  // 2. only support result obj at idx of zero.
  //    - because threre is a bug that the idx_in_part_columns_ of part_key_func is not set.
  //    - so, only support one part func and one part expr for the function of part key func calc.
  // 3. TODO will be optimized by shanbao.yb
  ObObj func_result_obj;
  func_result_obj.set_type(ObUnknownType);
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < part_key_num_; ++col_idx) {
    const ObObj& target_obj = equal_obj_arr_.at(col_idx);
    if (part_key_info.part_keys_[col_idx].level_ != part_key_func_level) {
      // continue
    } else if (OB_UNLIKELY(target_obj.is_unknown())) {
      // continue
    } else if (OB_PROXY_EXPR_TYPE_NONE == part_key_info.part_keys_[col_idx].part_key_func_info_.part_key_func_type_) {
      LOG_DEBUG("not support func of part_key, do nothing");
    } else if (OB_ISNULL(part_key_info.part_keys_[col_idx].part_key_func_info_.func_params_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("func_params_ is NULL for has_part_func_key is true", K(ret));
    } else {
      const ObProxyParseString &p_name = part_key_info.part_keys_[col_idx].name_;
      ObString col_name(p_name.str_len_, p_name.str_);
      if (OB_FAIL(add_obj_to_sql_field(sql_field_result, target_obj, col_name))) {
        LOG_WDIAG("fail to add_obj_to_sql_field", K(col_name), K(target_obj), K(ret));
      } else {
        LOG_DEBUG("succ to push_column_obj", K(col_name));
        part_key_func_info.set_func_params(part_key_info.part_keys_[col_idx].part_key_func_info_.func_params_);
        part_key_func_type = part_key_info.part_keys_[col_idx].part_key_func_info_.part_key_func_type_;
      }
    }
  }

  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_LIKELY(NULL == part_key_func_info.func_params_)) {
    // no part key func for this level
    // nothing
  } else if (OB_FAIL(cal_part_key_func(part_key_func_info, sql_field_result,
                                       client_info_, func_result_obj, is_oracle_mode_))) {
    LOG_WDIAG("fail to cal_part_key_func", K(func_result_obj), K(ret));
  } else if (func_result_obj.is_unknown()) {
    // no valid result ignore
  } else if (OB_FAIL(func_result_arr.push_back(func_result_obj))) {
    LOG_WDIAG("fail to set func result", K(func_result_obj), K(ret));
  } else {
    LOG_DEBUG("succ to set func result", K(func_result_obj));
    ROUTE_DIAGNOSIS(route_diagnosis_, RESOLVE_TOKEN, resolve_token, ret, TOKEN_NONE, "", DEFAULT_EXPR_TYPE,
                    DEFAULT_EXPR_TYPE, part_key_func_type, func_result_obj);
  }

  return ret;
}

int64_t ObExprResolverV2::do_generate_range(const ObProxyPartKeyInfo& part_key_info,
                                            ObProxyPartInfo &part_info)
{
  int ret = OB_SUCCESS;
  const int64_t func_result_idx = 0;
  ObString range, sub_range;
  // to store partition column's border flag as order above like
  // "where c2 > 1 and c1 < 33" => ordered_part_col_border [(exclusive_start, exclusive_end]
  ObSEArray<ObBorderFlag, 2> first_part_columns_border;
  ObSEArray<ObBorderFlag, 2> sub_part_columns_border;

  // init range and border
  if (part_info.get_part_level() >= share::schema::PARTITION_LEVEL_ONE) {
    if (OB_FAIL(ranges_[0].build_row_key(part_info.get_first_part_columns().count(), allocator_))) {
      LOG_WDIAG("fail to init range", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < part_info.get_first_part_columns().count(); i++) {
        if (OB_FAIL(first_part_columns_border.push_back(ObBorderFlag()))) {
          LOG_WDIAG("fail to push border flag", K(i), K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // nothing
  } else if (part_info.get_part_level() == share::schema::PARTITION_LEVEL_TWO) {
    if (OB_FAIL(ranges_[1].build_row_key(part_info.get_sub_part_columns().count(), allocator_))) {
      LOG_WDIAG("fail to init range", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < part_info.get_sub_part_columns().count(); i++) {
        if (OB_FAIL(sub_part_columns_border.push_back(ObBorderFlag()))) {
          LOG_WDIAG("fail to push border flag", K(i), K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_key_num_; ++i) {
      const ObProxyPartKey& part_key_i = part_key_info.part_keys_[i];
      if (OB_UNLIKELY(-1 != part_key_i.generated_col_idx_)) {
        // the part key is generated source, ignore
        // continue;
      } else if (OB_PROXY_EXPR_TYPE_NONE != part_key_i.part_key_func_info_.part_key_func_type_) {
        // part key func param, ignore
        // continue;
      } else {
        const ObObj& equal_obj = equal_obj_arr_.at(i);
        ObObj& left_obj = left_bound_arr_.at(i);
        ObObj& right_obj = right_bound_arr_.at(i);
        bool is_left_inclusive = left_bound_flag_arr_.at(i);
        bool is_right_inclusive = right_bound_flag_arr_.at(i);

        const ObProxyParseString& part_key_name_parse = part_key_info.part_keys_[i].name_;
        const ObString part_key_name(part_key_name_parse.str_len_, part_key_name_parse.str_);
        ObProxyPartKeyLevel part_key_level = part_key_info.part_keys_[i].level_;
        int64_t part_column_idx = part_key_info.part_keys_[i].idx_in_part_columns_;
        int64_t first_part_column_idx = -1;
        int64_t second_part_column_idx = -1;

        if (PART_KEY_LEVEL_ZERO == part_key_level) {
          LOG_WDIAG("unexpected part key, continue calc", K(part_key_level), K(part_key_name));
        } else if (PART_KEY_LEVEL_ONE == part_key_level) {
          first_part_column_idx = part_column_idx;
        } else if (PART_KEY_LEVEL_TWO == part_key_level) {
          second_part_column_idx = part_column_idx;
        } else if (PART_KEY_LEVEL_BOTH == part_key_level) {
          // impossible
        } else {
          LOG_WDIAG("unexpected part key, continue calc", K(part_key_level), K(part_key_name));
        }

        if (!equal_obj.is_unknown()) {
          left_obj = equal_obj;
          right_obj = equal_obj;
          is_left_inclusive = true;
          is_right_inclusive = true;
        } else {
          // nothing
        }

        if (-1 != first_part_column_idx) {
          // first part key
          if (OB_FAIL(place_obj_to_range(ranges_[0], first_part_columns_border,
                                        first_part_column_idx, left_obj, right_obj, is_left_inclusive, is_right_inclusive))) {
            LOG_WDIAG("fail to place_obj_to_range at first", K(first_part_column_idx), K(left_obj), K(right_obj), K(ret));
          }
        } else {
          // not first part key, nothing
        }

        if (OB_FAIL(ret)) {
          // nothing
        } else if (-1 != second_part_column_idx) {
          // sub part key
          if (OB_FAIL(place_obj_to_range(ranges_[1], sub_part_columns_border,
                                        second_part_column_idx, left_obj, right_obj, is_left_inclusive, is_right_inclusive))) {
            LOG_WDIAG("fail to place_obj_to_range at second", K(second_part_column_idx), K(left_obj), K(right_obj), K(ret));
          }
        } else {
          // not sub part key, nothing
        }
      }
    }
  }


  // handle part key
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_LIKELY(first_part_func_result_.count() <= 0)) {
    // ignore
  } else {
    const ObObj& first_part_func_result = first_part_func_result_.at(func_result_idx);
    if (OB_UNLIKELY(first_part_func_result.is_unknown())) {
      // ignore
    } else if (OB_FAIL(place_obj_to_range(ranges_[0], first_part_columns_border,
                                   func_result_idx, first_part_func_result, first_part_func_result, true, true))) {
      LOG_WDIAG("fail to place_obj_to_range at second", K(func_result_idx), K(first_part_func_result), K(ret));
    }
  }

    // handle part key
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_LIKELY(sub_part_func_result_.count() <= 0)) {
    // ignore
  } else {
    const ObObj& sub_part_func_result = sub_part_func_result_.at(func_result_idx);
    if (OB_UNLIKELY(sub_part_func_result.is_unknown())) {
      // ignore
    } else if (OB_FAIL(place_obj_to_range(ranges_[1], sub_part_columns_border,
                                   func_result_idx, sub_part_func_result, sub_part_func_result, true, true))) {
      LOG_WDIAG("fail to place_obj_to_range at second", K(func_result_idx), K(sub_part_func_result), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(preprocess_range(ranges_[0], first_part_columns_border))) {
      LOG_WDIAG("fail to preprocess range, part key level 0", K(ret));
    } else if (part_info.get_part_level() == share::schema::PARTITION_LEVEL_TWO
                && OB_FAIL(preprocess_range(ranges_[1], sub_part_columns_border))) {
      LOG_WDIAG("fail to preprocess range, part key level 1", K(ret));
    } else {}
  }

  ObDiagnosisResolveExpr *resolve_expr = NULL;
  if (OB_NOT_NULL(route_diagnosis_) && route_diagnosis_->is_diagnostic(RESOLVE_EXPR)) {
    resolve_expr = reinterpret_cast<ObDiagnosisResolveExpr*>(route_diagnosis_->get_last_matched_diagnosis_point(RESOLVE_EXPR));
    if (OB_NOT_NULL(resolve_expr)) {
      char range_buf[RESOLVE_EXPR_MAX_LEN] { 0 };
      char sub_range_buf[RESOLVE_EXPR_MAX_LEN] { 0 };
      if (!ranges_[0].empty()) {
        ranges_[0].to_plain_string(range_buf, RESOLVE_EXPR_MAX_LEN);
        range.assign_ptr(range_buf, (ObString::obstr_size_t) strlen(range_buf));
      }
      if (!ranges_[1].empty()) {
        ranges_[1].to_plain_string(sub_range_buf, RESOLVE_EXPR_MAX_LEN);
        sub_range.assign_ptr(sub_range_buf, (ObString::obstr_size_t) strlen(sub_range_buf));
      }
      resolve_expr->ret_ = ret;
      deep_copy_string(resolve_expr->alloc_, range, resolve_expr->part_range_);
      deep_copy_string(resolve_expr->alloc_, sub_range, resolve_expr->sub_part_range_);
    }
  }

  LOG_DEBUG("generate range", "range result", *this);

  return ret;
}

/**
 * @brief The range's border flag is decided by the last valid column.
 *        If use range directly, the range will cover partition which
 *        doesn't contain the data.
 *
 * @param range
 * @param border_flags all columns' border flag
 * @return int
 */
int ObExprResolverV2::preprocess_range(ObNewRange& range,
                                       ObIArray<ObBorderFlag>& border_flags)
{
  int ret = OB_SUCCESS;

  int64_t invalid_idx = range.start_key_.get_obj_cnt();
  for (int64_t i = 0; i < range.start_key_.get_obj_cnt(); i++) {
    // find the last valid col,
    // use the last valid col's border flag as range's border flag
    range.border_flag_.set_data(border_flags.at(i).get_data());

    if (i >= range.start_key_.get_obj_cnt()
        || i >= range.end_key_.get_obj_cnt()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected null pointer");
    } else {
      const ObObj& obj_start = range.start_key_.get_obj_at(i);
      const ObObj& obj_end = range.end_key_.get_obj_at(i);
      ObCompareCtx cmp_ctx(ObMaxType, CS_TYPE_INVALID, true, INVALID_TZ_OFF);
      bool need_cast = false;
      ObObj cmp_result(false);
      if (OB_FAIL(ObObjCmpFuncs::compare(cmp_result, obj_start, obj_end, cmp_ctx, ObCmpOp::CO_EQ, need_cast))) {
        LOG_WDIAG("fail to compare", K(ret));
        invalid_idx = i + 1;
        ret = OB_SUCCESS;
        break;
      } else if (!cmp_result.get_bool()) {
        invalid_idx = i + 1;
        break;
      }
    }
  }

  // set the cols after invalid_idx(included) to (max : min)
  // the default value of a range is (min, max), need set it to (max, min),
  // or the partition id result will be more than expected.
  for (int64_t i = invalid_idx; i < range.start_key_.get_obj_cnt(); i++) {
    ObObj& obj_start = range.start_key_.get_obj_at(i);
    ObObj& obj_end = range.end_key_.get_obj_at(i);
    obj_start.set_max_value();
    obj_end.set_min_value();
  }
  LOG_DEBUG("succ to simplify range", K(range));
  return ret;
}

int ObExprResolverV2::calc_partition_id_with_rowid_str(const char *str,
                                                       const int64_t str_len,
                                                       int64_t &partition_id,
                                                       ObIArray<ObObj>& result_obj,
                                                       int32_t &state,
                                                       int16_t &version)
{
  int ret = OB_SUCCESS;

  ObURowIDData rowid_data;
  if (OB_FAIL(ObURowIDData::decode2urowid(str, str_len, allocator_, rowid_data))) {
    LOG_WDIAG("decode2urowid failed", K(ret));
    state = DECODE_ROWID;
  } else if (OB_FAIL(rowid_data.get_obobj_or_partition_id_from_decoded(partition_id, result_obj))) {
    LOG_WDIAG("fail to get obobj or partition id by rowid data", K(ret));
    state = GET_PART_ID_FROM_DECODED_ROWID;
  } else {
    version = rowid_data.get_version();
  }

  return ret;
}

int ObExprResolverV2::calc_partition_id_with_rowid(const ObObj& src_obj,
                                                   int64_t &partition_id,
                                                   ObIArray<ObObj>& result_obj_arr)
{
  int ret = OB_SUCCESS;
  ObRowIDCalcState state = SUCCESS;
  int16_t version = 0;
  if (!src_obj.is_varchar()) {
    ret = OB_ERR_UNEXPECTED;
    state = RESOLVE_ROWID_TO_OBOBJ;
    LOG_INFO("expected obj type after resolved from execute", K(ret), K(src_obj));
  } else {
    ObString obj_str = src_obj.get_varchar();
    if (OB_FAIL(calc_partition_id_with_rowid_str(obj_str.ptr(), obj_str.length(), partition_id,
                                                 result_obj_arr, (int32_t&) state, version))) {
      LOG_INFO("fail to calc partition id with rowid str within execute", K(ret));
    }
  }

  ROUTE_DIAGNOSIS(route_diagnosis_, CALC_ROWID, calc_rowid, ret, state, version);
  return ret;
}

int64_t ObExprResolverV2::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  for (int64_t i = 0; i < OBPROXY_MAX_PART_LEVEL; ++i) {
    databuff_printf(buf, buf_len, pos, " ranges_[%ld]:", i);

    if (ranges_[i].border_flag_.inclusive_start()) {
      databuff_printf(buf, buf_len, pos, "[");
    } else {
      databuff_printf(buf, buf_len, pos, "(");
    }
    pos += ranges_[i].start_key_.to_plain_string(buf + pos, buf_len - pos);
    databuff_printf(buf, buf_len, pos, " ; ");
    pos += ranges_[i].end_key_.to_plain_string(buf + pos, buf_len - pos);
    if (ranges_[i].border_flag_.inclusive_end()) {
      databuff_printf(buf, buf_len, pos, "]");
    } else {
      databuff_printf(buf, buf_len, pos, ")");
    }

    databuff_printf(buf, buf_len, pos, ",");
  }
  J_OBJ_END();
  return pos;
}

void ObExprResolverV2::set_route_diagnosis(ObRouteDiagnosis *route_diagnosis)
{
  if (OB_NOT_NULL(route_diagnosis_)) {
    route_diagnosis_->dec_ref();
    route_diagnosis_ = NULL;
  }
  if (OB_NOT_NULL(route_diagnosis)) {
    route_diagnosis_ = route_diagnosis;
    route_diagnosis_->inc_ref();
  }
}
/*
 * calculate func token, convert token node to param node to reuse func resolver
 */
int ObExprResolverV2::calc_token_func_obj(ObProxyTokenNode *token,
                                          ObClientSessionInfo *client_session_info,
                                          ObObj &target_obj,
                                          SqlFieldResult *sql_field_result,
                                          const bool is_oracle_mode,
                                          ObProxyExprType &expr_type)
{
  int ret = OB_SUCCESS;
  ObProxyParamNode *param_node = NULL;
  ObProxyExprFactory factory(allocator_);
  ObFuncExprResolverContext ctx(&allocator_, &factory);
  ObFuncExprResolver resolver(ctx);
  ObProxyExpr *expr = NULL;

  if (OB_FAIL(convert_token_node_to_param_node(token, param_node))) {
    LOG_WDIAG("fail to convert func token to param node", K(ret));
  } else if (OB_ISNULL(param_node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to convert func token to param node", K(ret));
  } else if (OB_FAIL(resolver.resolve(param_node, expr))) {
    LOG_DEBUG("proxy expr resolve failed", K(ret)); // DEBUG for simplify meaningless and futile log
  } else {
    ObSEArray<ObObj, 4> result_array;
    ObProxyExprCalcItem calc_item(const_cast<SqlFieldResult *>(sql_field_result));
    ObProxyExprCtx expr_ctx(0, TESTLOAD_NON, false, &allocator_, client_session_info);
    expr_ctx.is_oracle_mode = is_oracle_mode;
    if (OB_FAIL(expr->calc(expr_ctx, calc_item, result_array))) {
      LOG_WDIAG("calc expr result failed", K(ret));
    } else if (OB_FAIL(result_array.at(0, target_obj))) {
      LOG_WDIAG("get expr calc result fail", K(ret));
    }
    expr_type = expr->get_expr_type();
  }

  return ret;
}

int ObExprResolverV2::add_obj_to_sql_field(SqlFieldResult &sql_field_result,
                                           const ObObj &target_obj,
                                           const ObString &col_name)
{
  int ret = OB_SUCCESS;
  SqlField *field = NULL;
  SqlColumnValue col_value;
  if (OB_FAIL(SqlField::alloc_sql_field(field))) {
    LOG_WDIAG("fail to alloc_sql_field for part_key_func", K(ret));
  } else if (false == field->column_name_.set_value(col_name)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to set col_name value, may be out of memory", K(col_name),
              K(ret));
    // 根据obj的类型，设置对应的值
  } else if (OB_FAIL(convert_obj_to_sql_column_value(target_obj, col_value))) {
    LOG_WDIAG("fail to convert obj to sql_column_value", K(col_name),
              K(target_obj), K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(field->column_values_.push_back(col_value))) {
      LOG_WDIAG("fail to push_back col_value", K(col_value), K(ret));
    } else if (OB_FAIL(sql_field_result.fields_.push_back(field))) {
      LOG_WDIAG("fail to push_back field", K(ret));
    } else {
      sql_field_result.field_num_++;
      field = NULL;
    }
  }

  // 如果中间出现失败，需要释放申请的field内存
  if (NULL != field) {
    field->reset();
  }

  return ret;
}

int ObExprResolverV2::convert_token_node_to_param_node(ObProxyTokenNode *token,
                                                       ObProxyParamNode *&param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(token)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret));
  } else {
    void *tmp_buf = NULL;
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObProxyParamNode)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc new param node", K(ret));
    } else {
      param = new(tmp_buf) ObProxyParamNode();
      param->next_ = NULL;
      if (TOKEN_INT_VAL == token->type_) {
        param->int_value_ = token->int_value_;
        param->type_ = PARAM_INT_VAL;
      } else if (TOKEN_STR_VAL == token->type_) {
        param->str_value_ = token->str_value_;
        param->type_ = PARAM_STR_VAL;
      } else if (TOKEN_NULL == token->type_) {
        param->type_ = PARAM_NULL;
      } else if (TOKEN_FUNC == token->type_) {
        if (OB_FAIL(recursive_convert_func_token(token, param))) {
          LOG_WDIAG("convert func token node to param node failed", K(ret));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("unexpected token node type, please check", K(ret), K(param->type_));
      }
    }
  }
  return ret;
}

int ObExprResolverV2::recursive_convert_func_token(ObProxyTokenNode *token,
                                                   ObProxyParamNode *param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(token) || OB_ISNULL(param) || token->type_ != TOKEN_FUNC) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret));
  } else {
    if (OB_ISNULL(token->str_value_.str_) || token->str_value_.str_len_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid func name", K(ret));
    } else {
      ObString func_name(token->str_value_.str_len_, token->str_value_.str_);
      void *tmp_buf = NULL;
      param->type_ = PARAM_FUNC;
      if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObFuncExprNode)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc new func expr node", K(ret));
      } else {
        param->func_expr_node_ = new(tmp_buf) ObFuncExprNode();
        param->func_expr_node_->func_name_ = token->str_value_;
        param->func_expr_node_->child_ = NULL;

        if (OB_ISNULL(token->child_)) {
          // do nothing
        } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObProxyParamNodeList)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc new token list", K(ret));
        } else {
          param->func_expr_node_->child_ = new(tmp_buf) ObProxyParamNodeList();
          ObProxyParamNode head;
          ObProxyParamNode *param_cur = &head;
          ObProxyTokenNode *token_child = token->child_->head_;
          int64_t child_num = 0;
          for (; OB_SUCC(ret) && token_child != NULL; token_child = token_child->next_, param_cur = param_cur->next_) {
            if (OB_FAIL(convert_token_node_to_param_node(token_child, param_cur->next_))) {
              LOG_WDIAG("recursive convert func token failed", K(ret));
            } else {
              child_num++;
            }
          }
          if (OB_SUCC(ret)) {
            param->func_expr_node_->child_->tail_ = param_cur;
            param->func_expr_node_->child_->head_ = head.next_;
            param->func_expr_node_->child_->child_num_ = child_num;
          }
        }
      }
    }
  }
  return ret;
}

int ObExprResolverV2::calc_token_hex_obj(ObProxyTokenNode *token, ObObj &target_obj)
{
  int ret = OB_SUCCESS;
  ObString hex_str_format_val;
  // may temp used
  char* full_hex_str_format_val = NULL;
  if (OB_ISNULL(token)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("hex token or str val is null pointer", K(ret));
  } else if (TOKEN_HEX_VAL != token->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("hex token type dismatch", K(ret));
  } else if (3 > token->str_value_.str_len_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("hex str val is too short", K(ret));
  } else if (OB_ISNULL(token->str_value_.str_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("hex str val is null pointer", K(ret));
  // like x'86adf554' which represents a sequence of memory '86 ad f5 54'
  } else if (token->str_value_.str_[0] == 'x' || token->str_value_.str_[0] == 'X') {
    hex_str_format_val.assign(token->str_value_.str_ + 2, token->str_value_.str_len_ - 3);
  } else if (token->str_value_.str_[0] == '0' && (token->str_value_.str_[1] == 'x' || token->str_value_.str_[1] == 'X')) {
    hex_str_format_val.assign(token->str_value_.str_ + 2, token->str_value_.str_len_ - 2);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail calc token hex obj", K(hex_str_format_val));
  }
  LOG_DEBUG("to calc hex val token str", K(hex_str_format_val));
  if (OB_SUCC(ret)) {
    char* hex_byte_format_buf = NULL;
    if (hex_str_format_val.empty()) {
      target_obj.set_hex_string(hex_str_format_val);
      LOG_WDIAG("hex str val invalid", K(hex_str_format_val));
    } else {
      // 0x12345 => 0x012345
      if (hex_str_format_val.length() % 2 != 0) {
        if (OB_ISNULL(full_hex_str_format_val = static_cast<char*>(allocator_.alloc(hex_str_format_val.length() + 1)))) {
          LOG_WDIAG("fail to alloc buf", K(ret));
        } else {
          full_hex_str_format_val[0] = '0';
          MEMCPY(full_hex_str_format_val + 1, hex_str_format_val.ptr(), hex_str_format_val.length());
          hex_str_format_val.assign_ptr(full_hex_str_format_val, static_cast<ObString::obstr_size_t>(hex_str_format_val.length() + 1));
          LOG_DEBUG("succ to calc hex val full format", K(hex_str_format_val));
        }
      }
      int64_t byte_len = hex_str_format_val.length() / 2;

      if (OB_FAIL(ret)) {
        // nothing
      } else if (OB_ISNULL(hex_byte_format_buf = static_cast<char*>(allocator_.alloc(byte_len)))) {
        LOG_WDIAG("fail to alloc buf", K(ret));
      } else if (hex_str_format_val.length() != static_cast<int64_t>(str_to_hex(hex_str_format_val.ptr(),
                                                             hex_str_format_val.length(),
                                                             hex_byte_format_buf, static_cast<int32_t>(byte_len)))){
        LOG_WDIAG("fail to str to hex byte", K(hex_str_format_val));
      } else {
        ObString hex_byte_val(byte_len, hex_byte_format_buf);
        target_obj.set_hex_string(hex_byte_val);
        LOG_DEBUG("calc hex val byte format", K(hex_byte_val));
      }
    }
    if (OB_NOT_NULL(full_hex_str_format_val)) {
      allocator_.free(full_hex_str_format_val);
    }
  }
  return ret;
}

int ObExprResolverV2::convert_obj_to_sql_column_value(const common::ObObj &src_obj, obutils::SqlColumnValue &dest_val)
{
  // 目前只支持整形/string类型
  int ret = OB_SUCCESS;
  ObObjTypeClass obj_type = src_obj.get_type_class();
  switch (obj_type) {
    case ObNullTC:
      // do nothing
      break;
    // 处理整形相关类型
    case ObIntTC:
    case ObUIntTC:
      dest_val.value_type_ = TOKEN_INT_VAL;
      dest_val.column_int_value_ = src_obj.get_int();
      break;
    case ObDateTimeTC:
    case ObOTimestampTC:
      dest_val.value_type_ = TOKEN_INT_VAL;
      dest_val.column_int_value_ = src_obj.get_datetime();
      break;
    case ObDateTC:
      dest_val.value_type_ = TOKEN_INT_VAL;
      dest_val.column_int_value_ = src_obj.get_date();
      break;
    case ObTimeTC:
      dest_val.value_type_ = TOKEN_INT_VAL;
      dest_val.column_int_value_ = src_obj.get_time();
      break;
    case ObYearTC:
      dest_val.value_type_ = TOKEN_INT_VAL;
      dest_val.column_int_value_ = src_obj.get_year();
      break;
    // 处理字符串相关
    case ObStringTC:
      dest_val.value_type_ = TOKEN_STR_VAL;
      dest_val.column_value_.set_value(src_obj.get_string());
      break;
    default:
      // 其他的暂不支持
      ret = OB_NOT_SUPPORTED;
      LOG_WDIAG("not support convert obj_type to column_value", K(obj_type));
      break;
  }
  return ret;
}

// todo : integrate with new func expr resolver
int ObExprResolverV2::calc_generated_key_value(ObObj &obj, const ObProxyPartKey &part_key, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(OB_PROXY_EXPR_TYPE_FUNC_SUBSTR == part_key.func_type_)) {
    //  we only support substr now
    int64_t start_pos = INT64_MAX;
    int64_t sub_len = INT64_MAX;
    if (NULL != part_key.params_[1] && PARAM_INT_VAL == part_key.params_[1]->type_) {
      start_pos = part_key.params_[1]->int_value_;
    }
    if (NULL != part_key.params_[2] && PARAM_INT_VAL == part_key.params_[2]->type_) {
      sub_len = part_key.params_[2]->int_value_;
    }
    ObString src_val;
    if (obj.is_varchar()) {
      if (OB_FAIL(obj.get_varchar(src_val))) {
        LOG_WDIAG("fail to get varchar value", K(obj), K(ret));
      } else {
        if (start_pos < 0) {
          start_pos = src_val.length() + start_pos + 1;
        }
        if (0 == start_pos && is_oracle_mode) {
          start_pos = 1;
        }
        if (INT64_MAX == sub_len) {
          sub_len = src_val.length() - start_pos + 1;
        }
        if (start_pos > 0 && start_pos <= src_val.length()
            && sub_len > 0 && sub_len <= src_val.length()) {
            obj.set_varchar(src_val.ptr() + start_pos - 1, static_cast<int32_t>(sub_len));
        }
      }
    }
  } else {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WDIAG("unknown generate function type", K(part_key.func_type_), K(ret));
  }
  return ret;
}

// todo : merge with calc_generated_key_value
int ObExprResolverV2::calc_generated_key_value_for_obkv(common::ObObj &obj, const ObProxyPartKey &part_key, const obkv::ObTableEntityType entity_type, common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_PROXY_EXPR_TYPE_FUNC_SUBSTR == part_key.func_type_) {
    //  we only support substr now
    ObCollationType collation = obj.get_collation_type();
    // todo : client and proxy can not get the shcema type of user table now
    if (entity_type == obkv::ObTableEntityType::ET_HKV) {
      collation = common::CS_TYPE_BINARY;
    }
    ObString output;
    ObString str = obj.get_string();
    int64_t start_pos = 0;
    int64_t length = 0;
    if (OB_UNLIKELY(OB_ISNULL(part_key.params_[0])
                    || PARAM_COLUMN != part_key.params_[0]->type_
                    || ObStringTC != obj.get_type_class())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("unexpected arg for generated calculation", K(ret), KP(part_key.params_[0]), K(obj));
    } else if (OB_UNLIKELY(OB_ISNULL(part_key.params_[1]))
              || OB_FAIL(ObFuncExprTool::calc_int_value_from_func_parser(part_key.params_[1], start_pos))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("unexpected arg for generated calculation", K(ret), KP(part_key.params_[1]));
    } else if (OB_UNLIKELY(OB_NOT_NULL(part_key.params_[2]))
               && OB_FAIL(ObFuncExprTool::calc_int_value_from_func_parser(part_key.params_[2], length))) {
      // params 2 counld be NULL
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("unexpected arg for generated calculation", K(ret), KP(part_key.params_[2]));
    } else if (obj.get_string().empty()) {
      // do nothing
      LOG_DEBUG("origin str is empty", K(ret));
    } else {
      length = OB_ISNULL(part_key.params_[2]) ? str.length() : length;
      int64_t mb_len = ObCharset::strlen_char(collation, str.ptr(), str.length());
      start_pos = (start_pos >= 0) ? (start_pos - 1) : start_pos + mb_len;
      LOG_DEBUG("calc substr generated key for obkv params:", K(str), K(length), K(start_pos), K(mb_len), K(str.length()));
      if (OB_UNLIKELY(start_pos < 0 || start_pos >= mb_len || length <= 0)) {
        output.assign(NULL, 0);
      } else {
        length = min(length, mb_len - start_pos);
        int64_t offset = ObCharset::charpos(collation, str.ptr(), str.length(), start_pos);
        length = ObCharset::charpos(collation,
                                    str.ptr() + offset,
                                    (offset == 0) ? str.length() : str.length() - offset,
                                    length);
        // length could be equal to str.length()
        if (offset >= str.length() || length > str.length()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected offset and length of str", K(str), K(offset), K(length), K(ret));
        } else {
          output.assign_ptr(str.ptr() + offset, length);
        }
      }
      if (OB_SUCC(ret)) {
        obj.set_string(obj.get_type(), output);
      }
      LOG_DEBUG("calc substr generated key for obkv", K(obj), K(output), K(output.length()));
    }

  } else if (OB_PROXY_EXPR_TYPE_FUNC_SUBSTR_INDEX == part_key.func_type_) {
    ObString output;
    ObString str = obj.get_string();
    ObString delim;
    int64_t count = 0;

    if (OB_UNLIKELY(OB_ISNULL(part_key.params_[0])
                    || PARAM_COLUMN != part_key.params_[0]->type_
                    || ObStringTC != obj.get_type_class())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("unexpected arg for generated calculation", K(ret), KP(part_key.params_[0]), K(obj));
    } else if (OB_UNLIKELY(OB_ISNULL(part_key.params_[1])
               || OB_FAIL(ObFuncExprTool::calc_str_value_from_func_parser(part_key.params_[1], delim)))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("unexpected arg for generated calculation", K(ret), KP(part_key.params_[1]));
    } else if (OB_UNLIKELY(OB_ISNULL(part_key.params_[2])
               || OB_FAIL(ObFuncExprTool::calc_int_value_from_func_parser(part_key.params_[2], count)))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("unexpected arg for generated calculation", K(ret), KP(part_key.params_[2]));
    } else if (obj.get_string().empty()
               || delim.empty()
               || count == 0) {
      // these case return empty string
      // 1. src str is empty
      // 2. delim str is empty
      // 3. count is 0
      obj.set_string(obj.get_type(), "");
    } else {
      bool is_reverse = count < 0;
      int32_t *next_arr = NULL;
      int64_t pos = -1;

      LOG_DEBUG("calc substring_index generated key for obkv", K(str), K(delim), K(count));
      if (OB_ISNULL(next_arr = static_cast<int32_t *>(op_fixed_mem_alloc(delim.length() * sizeof(int32_t))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to allocate mem for kmp next arr", K(ret));
      } else if (!is_reverse && OB_FAIL(ObExprUtil::kmp_next(delim.ptr(), delim.length(), next_arr))) {
        LOG_WDIAG("fail to init kmp next arr", K(ret));
      } else if (is_reverse && OB_FAIL(ObExprUtil::kmp_next_reverse(delim.ptr(), delim.length(), next_arr))) {
        LOG_WDIAG("fail to init kmp next arr", K(ret));
      } else {
        if (!is_reverse) {
          if (OB_FAIL(ObExprUtil::kmp(delim.ptr(), delim.length(), str.ptr(), str.length(), count, next_arr, pos))) {
            LOG_WDIAG("fail to calculate substr pos", K(ret));
          } else if (-1 < pos) {
            output.assign(str.ptr(), pos);
          }
        } else if (is_reverse) {
          if (OB_FAIL(ObExprUtil::kmp_reverse(delim.ptr(), delim.length(), str.ptr(), str.length(), count, next_arr, pos))) {
            LOG_WDIAG("fail to calculate substr pos", K(ret));
          } else if (-1 < pos) {
            output.assign(str.ptr() + pos + delim.length(), str.length() - delim.length() - pos);
          }
        }
      }
      if (-1 == pos) {
        output.assign(str.ptr(), str.length());
      }
      if (OB_NOT_NULL(next_arr)) {
        op_fixed_mem_free(next_arr, delim.length() * sizeof(int32_t));
      }
      if (OB_SUCC(ret)) {
        obj.set_string(obj.get_type(), output);
      }
      LOG_DEBUG("calc substring_index generated key for obkv", K(obj));
    }
  } else if (OB_PROXY_EXPR_TYPE_FUNC_ABS == part_key.func_type_) {
    if (OB_UNLIKELY(OB_ISNULL(part_key.params_[0])
                    || obj.is_null())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("unexpected arg for generated calculation", K(ret), KP(part_key.params_[0]), K(obj));
    } else if (PARAM_INT_VAL == part_key.params_[0]->type_ || ObIntType == obj.get_type()) {
      int64_t val = obj.get_int();
      obj.set_int(abs(val));
      LOG_DEBUG("calc abs generated key for obkv hbase", K(obj));
    } else {
      number::ObNumber res_nmb;
      if (OB_FAIL((get_obj_for_calc<ObNumberTC, ObNumberType>(&allocator, obj, obj)))) {
        LOG_WDIAG("get number obj failed", K(ret), K(obj));
      } else {
        if (obj.get_number().is_negative()) {
          if (OB_FAIL(obj.get_number().negate(res_nmb, allocator))) {
            LOG_WDIAG("calc abs number failed", K(ret), K(obj));
          } else {
            obj.set_number(res_nmb);
          }
        }
      }
      LOG_DEBUG("calc abs generated key for obkv hbase", K(obj));
    }
  } else {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WDIAG("unknown generate function type", K(part_key.func_type_), K(ret));
  }
  return ret;
}

int ObExprResolverV2::get_obj_with_param(ObObj &target_obj,
                                         ObProxyMysqlRequest *client_request,
                                         ObClientSessionInfo *client_info,
                                         ObProxyPartInfo *part_info,
                                         ObPsIdEntry *ps_id_entry,
                                         const int64_t param_index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_request) || OB_ISNULL(client_info) || OB_UNLIKELY(param_index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(client_request), K(param_index), K(ret));
  } else {
    int64_t execute_param_index = param_index;
    bool need_use_execute_param = false;
    // here parse result means the original parse result for this ps sql or call sql
    ObSqlParseResult &parse_result = client_request->get_parse_result();
    ObProxyCallInfo &call_info = parse_result.call_info_;
    if (parse_result.is_call_stmt() || parse_result.is_text_ps_call_stmt()) {
      if (OB_UNLIKELY(!call_info.is_valid()) || OB_UNLIKELY(param_index >= call_info.params_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid placeholder idx", K(param_index), K(call_info), K(ret));
      } else {
        const ObProxyCallParam* call_param = call_info.params_.at(param_index);
        if (CALL_TOKEN_INT_VAL == call_param->type_) {
          int64_t int_val = 0;
          if (OB_FAIL(get_int_value(call_param->str_value_.config_string_, int_val))) {
            LOG_WDIAG("fail to get int value", K(call_param->str_value_.config_string_), K(ret));
          } else {
            target_obj.set_int(int_val);
          }
        } else if (CALL_TOKEN_STR_VAL == call_param->type_) {
          target_obj.set_varchar(call_param->str_value_.config_string_);
          target_obj.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else if (CALL_TOKEN_PLACE_HOLDER == call_param->type_) {
          need_use_execute_param = true;
          if (OB_FAIL(get_int_value(call_param->str_value_.config_string_, execute_param_index))) {
            LOG_WDIAG("fail to get int value", K(call_param->str_value_.config_string_), K(ret));
          }
        }
      }
    } else {
      need_use_execute_param = true;
    }
    if (OB_SUCC(ret)
        && need_use_execute_param
        && OB_MYSQL_COM_STMT_EXECUTE == client_request->get_packet_meta().cmd_) {
      // for com_stmt_prepare, we have no execute_params, so no need continue, just return
      LOG_DEBUG("will cal obj with value from execute param", K(execute_param_index));
      if (OB_ISNULL(ps_id_entry)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("client ps id entry is null", K(ret), KPC(ps_id_entry));
      } else if (OB_UNLIKELY(execute_param_index >= ps_id_entry->get_param_count())
                 || execute_param_index < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid placeholder idx", K(execute_param_index), KPC(ps_id_entry), K(ret));
      } else if (OB_FAIL(ObMysqlRequestAnalyzer::analyze_execute_param(ps_id_entry->get_param_count(),
                         ps_id_entry->get_ps_sql_meta().get_param_types(), *client_request, execute_param_index, target_obj))) {
        LOG_WDIAG("fail to analyze execute param", K(ret));
      }
    }
    if (OB_SUCC(ret) && need_use_execute_param && OB_MYSQL_COM_STMT_PREPARE == client_request->get_packet_meta().cmd_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_DEBUG("prepare sql with only placeholder, will return fail", K(ret));
    }

    if (OB_SUCC(ret)
        && need_use_execute_param
        && client_request->get_parse_result().is_text_ps_execute_stmt()) {
      LOG_DEBUG("will cal obj with value from ps execute param", K(execute_param_index));
      ObSqlParseResult &parse_result = client_request->get_parse_result();
      ObProxyTextPsInfo execute_info = parse_result.text_ps_info_;
      if (execute_param_index >= execute_info.params_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("param index is large than param count", K(execute_param_index),
                  K(execute_info.params_.count()), K(ret));
      } else {
        ObProxyTextPsParam* param = execute_info.params_.at(execute_param_index);
        ObString user_variable_name = param->str_value_.config_string_;
        string_to_lower_case(user_variable_name.ptr(), user_variable_name.length());
        if (OB_FAIL(static_cast<const ObClientSessionInfo&>(*client_info).get_user_variable_value(user_variable_name, target_obj))) {
          LOG_WDIAG("get user variable failed", K(ret), K(user_variable_name));
        } else {
          ObString user_var;
          int tmp_ret = OB_SUCCESS;
          if (target_obj.is_varchar()) {
            if (OB_SUCCESS != (tmp_ret = target_obj.get_varchar(user_var))) {
              LOG_WDIAG("get varchar failed", K(tmp_ret));
            } else {
              char* ptr = user_var.ptr();
              int32_t len = user_var.length();
              // user var has store ' into value
              if ((user_var[0] == 0x27 && user_var[len-1] == 0x27) ||
                (user_var[0] == 0x22 && user_var[len-1] == 0x22)) {
                target_obj.set_varchar(ptr + 1, len - 2);
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)
        && need_use_execute_param
        && OB_MYSQL_COM_STMT_PREPARE_EXECUTE == client_request->get_packet_meta().cmd_) {
      LOG_DEBUG("will cal obj with value from execute param", K(execute_param_index));
      if (OB_UNLIKELY(execute_param_index < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid placeholder idx", K(execute_param_index), K(ret));
      } else if (OB_FAIL(ObMysqlRequestAnalyzer::analyze_prepare_execute_param(*client_request, execute_param_index, target_obj))) {
        LOG_WDIAG("fail to analyze execute param", K(ret));
      }
    }

    if (OB_SUCC(ret)
        && need_use_execute_param
        && OB_MYSQL_COM_STMT_SEND_LONG_DATA == client_request->get_packet_meta().cmd_) {
      LOG_DEBUG("will calc obj with execute param for send long data");
      if (OB_FAIL(ObMysqlRequestAnalyzer::analyze_send_long_data_param(*client_request, execute_param_index,
                                                                       part_info, ps_id_entry, target_obj))) {
        LOG_DEBUG("fail to analyze send long data param", K(ret));
      }
    }
  }
  return ret;
}

/*
 * func parse num type in these cases
 * 1. return int type for a positive number
 * 2. return func type for a negative number : - 100 -> - ( 100 )
 * 3. return str type for big number: length of number > 17 (don't consider for generated col temporary)
 * 4. we don't consider complex expr calculation for generated col temporary, like substr('aaa', 1, 1 + 2)
 */
int ObFuncExprTool::calc_int_value_from_func_parser(ObProxyParamNode *param_node, int64_t &int_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected func param node", K(ret));
  } else if (param_node->type_ == PARAM_INT_VAL) {
    int_value = param_node->int_value_;
  } else if (param_node->type_ == PARAM_FUNC && param_node->func_expr_node_->func_name_.str_len_ > 0
             && OB_NOT_NULL(param_node->func_expr_node_->func_name_.str_)
             && param_node->func_expr_node_->func_name_.str_[0] == '-') {
    if (OB_UNLIKELY(param_node->func_expr_node_->child_->child_num_ != 2)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid int value param node", K(ret));
    } else {
      int_value = -param_node->func_expr_node_->child_->head_->next_->int_value_;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid int value param node", K(ret));
  }
  LOG_DEBUG("calc int value from func parser", K(int_value));
  return ret;
}


/*
 * func parse str
 * 1. ObServer return generated_col func in these cases,  func parser treat all str to PARAM_COLUMN
 *    a. SUBSTRING_INDEX(K, 'AAAA',2)
 *    b. SUBSTRING_INDEX('K', 'AAAA',2)
 *    c. SUBSTRING_INDEX(`K`, 'AAAA',2)
 *    d. SUBSTRING_INDEX("K", 'AAAA',2)
 */
int ObFuncExprTool::calc_str_value_from_func_parser(ObProxyParamNode *param_node, ObString &str_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected func param node", K(ret));
  } else if (param_node->type_ == PARAM_STR_VAL) {
    str_value = ObString(param_node->str_value_.str_len_, param_node->str_value_.str_);
  } else if (param_node->type_ == PARAM_COLUMN) {
    str_value = ObString(param_node->col_name_.str_len_, param_node->col_name_.str_);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid str value for param node", K(ret));
  }
  LOG_DEBUG("calc str value from func parser", K(str_value));
  return ret;
}


/*
 * calculate const partition key value from sql
 * for normal ps sql, placeholder_idx_ in token node means the pos of '?'
 * for normal pl sql, placeholder_idx_ in token node means the index of call_info.params_
 * for pl sql with ps, placeholder_idx_ in call_info_node_ means the pos of '?'
 * for example: ps sql = call func1(11, ?, 22, ?),
 * the first sql of func1 is select * from t1 where a = :1 and b = :2 and c =:3 and d = :4
 * result:
 * call_info_.params_[1].placeholder_idx_ = 0, call_info_.params_[3].placeholder_idx_ = 1
*/
int ObExprResolverV2::resolve_token_list_const_obj(ObProxyRelationExpr& relation,
                                                   ObProxyPartInfo& part_info,
                                                   ObProxyMysqlRequest& client_request,
                                                   ObClientSessionInfo& client_info,
                                                   ObPsIdEntry *ps_id_entry,
                                                   ObObj &target_obj)
{
  int ret = OB_SUCCESS;

  bool is_diagnostic = OB_NOT_NULL(route_diagnosis_) && route_diagnosis_->is_diagnostic(RESOLVE_TOKEN);
  ObString token_str;
  char int_token_buf[20] { 0 };
  ObProxyTokenType token_type = ObProxyTokenType::TOKEN_NONE;
  ObProxyExprType expr_type = ObProxyExprType::OB_PROXY_EXPR_TYPE_NONE;

  if (OB_ISNULL(relation.right_value_)
      || OB_ISNULL(relation.right_value_->head_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_INFO("token list or head is null", K(relation.right_value_), K(ret));
  } else {
    target_obj.set_type(ObUnknownType);
    ObProxyTokenNode *token = relation.right_value_->head_;
    token_type = token->type_;
    if (TOKEN_STR_VAL == token->type_) {
      if (OB_UNLIKELY(is_diagnostic)) {
        token_str.assign_ptr(token->str_value_.str_, token->str_value_.str_len_);
      }
      target_obj.set_varchar(token->str_value_.str_, token->str_value_.str_len_);
      target_obj.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      if (token->str_value_.str_len_ > 2 &&
          token->str_value_.str_[1] == '\'' &&
          token->str_value_.str_[token->str_value_.str_len_ - 1] == '\'' &&
          lib::is_oracle_mode()) {
        // oracle mode u'xxxx' str pattern treated as unicode(CHARSET_BINARY)
        if (token->str_value_.str_[0] == 'u' || token->str_value_.str_[0] == 'U') {
          target_obj.set_collation_type(ObCharset::get_default_collation_oracle(CHARSET_BINARY));
          LOG_DEBUG("succ to parse u'xxx' pattern and set to binary in oracle mode", K(target_obj));
        } else if (token->str_value_.str_[0] == 'n' || token->str_value_.str_[0] == 'N') {
          LOG_DEBUG("succ to parse n'xxx' pattern in oracle mode", K(target_obj));
        }
        // n/u'string_val' -> string_val
        target_obj.set_varchar(token->str_value_.str_ + 2, token->str_value_.str_len_ - 3);
      }
    } else if (TOKEN_INT_VAL == token->type_) {
      target_obj.set_int(token->int_value_);
      if (OB_UNLIKELY(is_diagnostic)) {
        sprintf(int_token_buf, "%ld",token->int_value_);
        token_str.assign_ptr(int_token_buf, (ObString::obstr_size_t) strlen(int_token_buf));
      }
    } else if (TOKEN_PLACE_HOLDER == token->type_) {
      int64_t param_index = token->placeholder_idx_;
      target_obj.set_type(ObUnknownType);
      if (OB_FAIL(get_obj_with_param(target_obj, &client_request, &client_info,
                                     &part_info, ps_id_entry, param_index))) {
        LOG_DEBUG("fail to get target obj with param", K(ret));
      }
      if (OB_UNLIKELY(is_diagnostic)) {
        token_str.assign_ptr(NULL, 0);
      }
    } else if (TOKEN_FUNC == token->type_) {
      if (OB_FAIL(calc_token_func_obj(token, &client_info, target_obj, NULL /* sql_field_result */, part_info.is_oracle_mode(), expr_type))) {
        LOG_DEBUG("fail to calc token func obj", K(ret)); // DEBUG for simplify meaningless and futile log
      }
      if (OB_UNLIKELY(is_diagnostic)) {
        token_str.assign_ptr(token->str_value_.str_, token->str_value_.str_len_);
      }
    } else if (TOKEN_HEX_VAL == token->type_) {
      if (OB_FAIL(calc_token_hex_obj(token, target_obj))) {
        LOG_WDIAG("fail to calc token hex obj", K(ret));
      }
      if (OB_UNLIKELY(is_diagnostic)) {
        token_str.assign_ptr(token->str_value_.str_, token->str_value_.str_len_);
      }
    } else if (TOKEN_COLUMN == token->type_) {
      if (OB_UNLIKELY(is_diagnostic)) {
        token_str.assign_ptr(token->column_name_.str_, token->column_name_.str_len_);
      }
      LOG_DEBUG("get unexpected column token here, ignore it", K(token_str));
    } else if (TOKEN_NULL == token->type_) {
      target_obj.set_null();
      if (OB_UNLIKELY(is_diagnostic)) {
        token_str = "NULL";
      }
    } else {
      if (OB_UNLIKELY(is_diagnostic)) {
        token_str.assign_ptr(NULL, 0);
      }
      ret = OB_INVALID_ARGUMENT;
    }

    // set target_obj collation
    if (OB_SUCC(ret)) {
      if (ObHexStringType == target_obj.get_type()) {
        LOG_DEBUG("succ to set hex string obj to binary collation", K(target_obj));
      } else if (ObStringTC == target_obj.get_type_class()) {
        // in oracle mode, nchar/nvarchar2 obj use ncharacter_set_connection
        if (lib::is_oracle_mode() && target_obj.get_meta().is_nstring() && CHARSET_INVALID != client_info.get_ncharacter_set_connection()) {
          target_obj.set_collation_type(ObCharset::get_default_collation_oracle(static_cast<ObCharsetType>(client_info.get_ncharacter_set_connection())));
          LOG_DEBUG("succ to set nchar/nvarchar2 obj collation to ncharacter_set_connection", K(target_obj));
        // use collation_connection
        } else if (target_obj.get_collation_type() == ObCharset::get_default_collation(ObCharset::get_default_charset()) ||
                   target_obj.get_collation_type() == CS_TYPE_INVALID) {
          target_obj.set_collation_type(static_cast<common::ObCollationType>(client_info.get_collation_connection()));
          LOG_DEBUG("succ to set string obj collation to connection collation", K(target_obj));
        } else {
          LOG_DEBUG("succ to set string obj collation to specified collation", K(target_obj));
        }
      } else {
        LOG_DEBUG("skip setting non string obj collation", K(target_obj));
      }
    }
  } // end of else

  LOG_DEBUG("succ to route diagnosis resolve token", K(target_obj));
  ROUTE_DIAGNOSIS(route_diagnosis_, RESOLVE_TOKEN, resolve_token, ret, token_type, token_str, expr_type,
                  DEFAULT_EXPR_TYPE, DEFAULT_EXPR_TYPE, target_obj);

  return ret;
}

int ObExprResolverV2::parse_and_resolve_default_value(const ObProxyParseString& default_value,
                                                      const ObClientSessionInfo& client_session_info,
                                                      ObObj& target_obj,
                                                      bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  number::ObNumber nb;
  int64_t tmp_pos = 0;
  if (OB_UNLIKELY(default_value.str_len_ <= 0 )) {
    // noting
  } else if (OB_FAIL(target_obj.deserialize(default_value.str_ , default_value.str_len_, tmp_pos))) {
    LOG_WDIAG("fail to deserialize default value of part key");
  } else {
    LOG_DEBUG("default value deserialize succ" , K(target_obj));
    if (!target_obj.is_varchar() || !is_oracle_mode) {
      // mysql mode return the default value with resolved obj in column's type
      // do nothing
    } else if (target_obj.is_varchar() && is_oracle_mode) {
      // oracle mode return the default as a unresolved varchar type obj
      ObString default_value_expr = target_obj.get_varchar();
      if (default_value_expr.empty()) {
        target_obj.set_varchar(ObString());
      } else if ('\'' == default_value_expr[0]) {
        // match string type
        ObString dst;
        if (2 >= default_value_expr.length()) {
          dst = ObString();
          // remove single quotes
        } else if (OB_FAIL(ob_sub_str(allocator_, default_value_expr, 1, default_value_expr.length() - 2, dst))) {
          LOG_WDIAG("get sub stirng of default value failed", K(ret));
        }
        if (OB_SUCC(ret)) {
          target_obj.set_varchar(dst);
        }
      } else if ( OB_SUCCESS == nb.from(default_value_expr.ptr(), default_value_expr.length(), allocator_)) {
        // match positive number
      } else {
        /*
          original resolver can't resolve this case : -(1+5) -> -((1+5)),
          so treat negative number as expr
          server return negative with brackets, -1 -> -(1)
        */
        // match expr
        ObFuncExprParser parser(allocator_, SHARDING_EXPR_FUNC_PARSE_MODE);
        ObFuncExprParseResult result;

        ObProxyExprFactory factory(allocator_);
        ObFuncExprResolverContext ctx(&allocator_, &factory);
        ObFuncExprResolver resolver(ctx);
        ObProxyExpr *expr = NULL;

        if (OB_FAIL(parser.parse(default_value_expr, result))) {
          LOG_INFO("parse default value expr failed", K(ret));
        } else if (OB_FAIL(resolver.resolve(result.param_node_, expr))) {
          LOG_DEBUG("proxy expr resolve failed", K(ret)); // DEBUG for simplify meaningless and futile log
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected pointer", K(expr), K(ret));
        } else {
          SqlFieldResult sql_field_result;
          ObSEArray<ObObj, 4> result_array;
          ObProxyExprCalcItem calc_item(&sql_field_result);
          ObProxyExprCtx expr_ctx(0, TESTLOAD_NON, false, &allocator_,
                                  const_cast<ObClientSessionInfo*>(&client_session_info));
          expr_ctx.is_oracle_mode = is_oracle_mode;
          if (OB_FAIL(expr->calc(expr_ctx, calc_item, result_array))) {
            LOG_WDIAG("calc expr result failed", K(ret));
          } else if (OB_FAIL(result_array.at(0, target_obj))) {
            LOG_WDIAG("get expr calc result fail", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && ObStringTC == target_obj.get_type_class()) {
    LOG_DEBUG("parse and resolve default value succ", K(target_obj), K(ret));
    target_obj.set_collation_type(static_cast<common::ObCollationType>(client_session_info.get_collation_connection()));
  }

  ROUTE_DIAGNOSIS(route_diagnosis_, RESOLVE_TOKEN, resolve_token, ret, TOKEN_NONE, "", DEFAULT_EXPR_TYPE,
                  DEFAULT_EXPR_TYPE, DEFAULT_EXPR_TYPE, target_obj);
  return ret;
}

int ObExprResolverV2::cal_part_key_func(ObPartkeyFuncInfo &func_info,
                                        const SqlFieldResult& sql_field_result,
                                        const ObClientSessionInfo& client_session_info,
                                        ObObj& target_obj,
                                        bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  ObProxyParamNode *param_node = func_info.func_params_;
  ObProxyExprType expr_type = OB_PROXY_EXPR_TYPE_NONE;

  ObProxyExpr *expr = NULL;
  ObProxyExprFactory factory(allocator_);
  ObFuncExprResolverContext ctx_context(&allocator_, &factory);
  ObFuncExprResolver resolver(ctx_context);

  // 1. 计算建表表达式的分区结果
  if (OB_ISNULL(param_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("unexpected param of func_info is NULL ", K(param_node), K(ret));
  } else if (OB_FAIL(resolver.resolve(param_node, expr))) {
    LOG_WDIAG("fail to resolve", K(ret));
  } else {
    ObSEArray<ObObj, 4> result_array;
    ObProxyExprCalcItem calc_item(const_cast<SqlFieldResult *>(&sql_field_result));
    ObProxyExprCtx expr_ctx(0, TESTLOAD_NON, false, &allocator_, const_cast<ObClientSessionInfo *>(&client_session_info));
    expr_ctx.is_oracle_mode = is_oracle_mode;
    if (OB_FAIL(expr->calc(expr_ctx, calc_item, result_array))) {
      LOG_WDIAG("calc expr result failed", K(ret));
    } else if (OB_FAIL(result_array.at(0, target_obj))) {
      LOG_WDIAG("get expr calc result fail", K(ret));
    } else {
      expr_type = expr->get_expr_type();
      LOG_DEBUG("succ to cal part_key_func", K(get_expr_type_name(expr_type)), K(target_obj));
    }
  }

  return ret;
}

// ObObj::is_unknown measns invalid type, can be ignored
int ObExprResolverV2::place_obj_to_range(ObNewRange& range,
                                         ObIArray<ObBorderFlag>& border_flags,
                                         int64_t idx_in_part_columns,
                                         const ObObj& start_obj,
                                         const ObObj& end_obj,
                                         bool is_start_inclusive,
                                         bool is_end_inclusive)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(range.start_key_.get_obj_ptr())
      || OB_ISNULL(range.end_key_.get_obj_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null pointer");
  } else {
    ObObj& target_start_obj = *((range.start_key_.get_obj_ptr()) + idx_in_part_columns);
    ObObj& target_end_obj = *((range.end_key_.get_obj_ptr()) + idx_in_part_columns);

    if (OB_UNLIKELY(!start_obj.is_unknown())) {
      target_start_obj = start_obj;
      if (is_start_inclusive) {
        border_flags.at(idx_in_part_columns).set_inclusive_start();
      }
    }

    if (OB_UNLIKELY(!end_obj.is_unknown())) {
      target_end_obj = end_obj;
      if (is_end_inclusive) {
        border_flags.at(idx_in_part_columns).set_inclusive_end();
      }
    }
  }

  return ret;
}

} // namespace opsql
} // namespace obproxy
} // end of oceanbase
