/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "obutils/ob_proxy_stmt_ctx.h"
#include "utils/ob_proxy_utils.h"
#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

ObProxyStmtCtx::ObProxyStmtCtx(common::ObIAllocator& allocator): valid_level_(0), table_exprs_maps_(), alias_exprs_maps_(),
                all_table_exprs_map_(), all_table_exprs_array_(), allocator_(allocator)
{
}

ObProxyStmtCtx::~ObProxyStmtCtx()
{
  // just fo reminding free the resource related to the pointers;
  for (int i = 0; i < table_exprs_maps_.count(); ++i) {
    table_exprs_maps_[i]->destroy();
    allocator_.free(table_exprs_maps_[i]);
  }

  for (int i = 0; i < alias_exprs_maps_.count(); ++i) {
    alias_exprs_maps_[i]->destroy();
    allocator_.free(alias_exprs_maps_[i]);
  }


}

// the default value of `pre_allocate_level` is 0
// you can set the value of `pre_allocate_level` if you know the
// exact sub-select level of Abstract Syntatic Tree.
// otherwise, please do not set
int ObProxyStmtCtx::init(int pre_allocate_level)
{
  int ret = OB_SUCCESS;

  valid_level_ = pre_allocate_level;
  if (OB_FAIL(all_table_exprs_map_.create(default_bucket_num, ObModIds::OB_HASH_BUCKET_PROXY_MAP, ObModIds::OB_HASH_BUCKET_PROXY_MAP))) {
    LOG_WDIAG("fail to reserve all_table_exprs_map_", K(ret));
  } else if (OB_FAIL(table_exprs_maps_.prepare_allocate(valid_level_))) {
    LOG_WDIAG("fail to reserve table_exprs_maps", K(ret));
  } else if (OB_FAIL(alias_exprs_maps_.prepare_allocate(valid_level_))) {
    LOG_WDIAG("fail to reserve alias_exprs_maps", K(ret));
  } else {
    for (int64_t i = 0; i < valid_level_; ++i) {
      table_exprs_maps_.at(i) = NULL;
      alias_exprs_maps_.at(i) = NULL;
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < valid_level_; ++i) {
      ExprMap* table_exprs_map = NULL;
      ExprMap* alias_exprs_map = NULL;
      if (OB_ISNULL(table_exprs_map = static_cast<ExprMap*>(allocator_.alloc(sizeof(ExprMap))))
          || OB_ISNULL(alias_exprs_map = static_cast<ExprMap*>(allocator_.alloc(sizeof(ExprMap))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc exprs_map", KP(table_exprs_map), KP(alias_exprs_map), K(ret));
      } else {
        table_exprs_map = new(table_exprs_map) ExprMap();
        alias_exprs_map = new(alias_exprs_map) ExprMap();
      }

      if (OB_FAIL(ret)) {
        // nothing
      } else if (OB_FAIL(table_exprs_map->create(default_bucket_num, ObModIds::OB_HASH_BUCKET_PROXY_MAP, ObModIds::OB_HASH_BUCKET_PROXY_MAP))) {
        LOG_WDIAG("fail to create table_exprs_map", K(i), K(ret));
      } else if (OB_FAIL(alias_exprs_map->create(default_bucket_num, ObModIds::OB_HASH_BUCKET_PROXY_MAP, ObModIds::OB_HASH_BUCKET_PROXY_MAP))) {
        LOG_WDIAG("fail to create alias_exprs_map", K(i), K(ret));
      } else {
        table_exprs_maps_.at(i) = table_exprs_map;
        alias_exprs_maps_.at(i) = alias_exprs_map;
        LOG_DEBUG("succ to create exprs_maps", K(i));
      }
    }
  }

  return ret;
}

// when entering a new level of sub-select, you need call inc_ctx_level
int ObProxyStmtCtx::inc_ctx_level()
{
  int ret = OB_SUCCESS;

  valid_level_++;
  if (OB_UNLIKELY(table_exprs_maps_.count() != alias_exprs_maps_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected map count", "table map count", table_exprs_maps_.count(),
              "alias map count", alias_exprs_maps_.count(), K_(valid_level), K(ret));
  } else if (table_exprs_maps_.count() >= valid_level_) {
    // nothing, reuse old map
    LOG_DEBUG("succ to reuse old map", "table map count", table_exprs_maps_.count(), K_(valid_level));
  } else {
    ExprMap* table_exprs_map = NULL;
    ExprMap* alias_exprs_map = NULL;
    if (OB_ISNULL(table_exprs_map = static_cast<ExprMap*>(allocator_.alloc(sizeof(ExprMap))))
        || OB_ISNULL(alias_exprs_map = static_cast<ExprMap*>(allocator_.alloc(sizeof(ExprMap))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc exprs_map", KP(table_exprs_map), KP(alias_exprs_map), K(ret));
    } else {
      table_exprs_map = new(table_exprs_map) ExprMap();
      alias_exprs_map = new(alias_exprs_map) ExprMap();
    }

    if (OB_FAIL(ret)) {
      // nothing
    } else if (OB_FAIL(table_exprs_map->create(default_bucket_num, ObModIds::OB_HASH_BUCKET_PROXY_MAP, ObModIds::OB_HASH_BUCKET_PROXY_MAP))) {
      LOG_WDIAG("fail to create table_exprs_map", K_(valid_level), K(ret));
    } else if (OB_FAIL(alias_exprs_map->create(default_bucket_num, ObModIds::OB_HASH_BUCKET_PROXY_MAP, ObModIds::OB_HASH_BUCKET_PROXY_MAP))) {
      LOG_WDIAG("fail to create alias_exprs_map", K_(valid_level), K(ret));
    } else if (OB_FAIL(table_exprs_maps_.push_back(table_exprs_map))) {
      LOG_WDIAG("fail to push back table_exprs_map", K_(valid_level), K(ret));
    } else if (OB_FAIL(alias_exprs_maps_.push_back(alias_exprs_map))) {
      LOG_WDIAG("fail to push back alias_exprs_map", K_(valid_level), K(ret));
    } else {
      LOG_DEBUG("succ to add exprs_maps", KP(table_exprs_map), KP(alias_exprs_map));
    }
  }

  return ret;
}

// when leaving a level of sub-select, you need call dec_ctx_level
int ObProxyStmtCtx::dec_ctx_level()
{
  int ret = OB_SUCCESS;
  int last_level = valid_level_ - 1;

  if (OB_UNLIKELY(last_level < 0 || last_level >= table_exprs_maps_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected level", K(last_level), K(ret));
  } else if (OB_FAIL(table_exprs_maps_[last_level]->reuse())) {
    LOG_WDIAG("fail to reuse table_exprs_map", K(last_level), K(ret));
  } else if (OB_FAIL(alias_exprs_maps_[last_level]->reuse())) {
    LOG_WDIAG("fail to reuse table_exprs_map", K(last_level), K(ret));
  }

  valid_level_--;
  return ret;
}

// find a name in Abstract Syntatic Tree in spite of
// whether it's a table name or a alias name
int ObProxyStmtCtx::find_name(const ObString& name, ObProxyExpr*& ret_expr)
{
  int ret = OB_SUCCESS;

  bool find = false;
  int last_level = valid_level_ - 1;

  if (OB_UNLIKELY(NULL != ret_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("wrong argument", K(ret_expr), K(ret));
  } else if (OB_UNLIKELY(last_level < 0
                        || last_level >= table_exprs_maps_.count()
                        || table_exprs_maps_.count() != alias_exprs_maps_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected level", K(last_level), K(ret));
  } else {
    // 1.优先找最新层级的 name
    // 2.同层级，优先找别名
    for (int i = last_level; OB_SUCC(ret) && !find && i >= 0; --i) {
      if (OB_ISNULL(alias_exprs_maps_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected point", K(i), K(ret));
      } else if (OB_FAIL(alias_exprs_maps_[i]->get_refactored(name, ret_expr))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WDIAG("fail to get name from alias_exprs_maps_", K(i), K(name), K(ret_expr), K(ret));
        }
      } else {
        find = true;
        LOG_DEBUG("succ to find expr from alias_exprs_maps_", K(i), K(name), K(ret_expr));
      }

      if (OB_SUCC(ret)) {
        // nothing
      } else if (OB_UNLIKELY(ret != OB_HASH_NOT_EXIST)) {
        // unexpected error
      } else if (OB_ISNULL(table_exprs_maps_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected point", K(i), K(ret));
      } else if (// ret == OB_HASH_NOT_EXIST
                 OB_FAIL(table_exprs_maps_[i]->get_refactored(name, ret_expr))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WDIAG("fail to get name from table_exprs_maps_", K(i), K(name), K(ret_expr), K(ret));
        }
      } else {
        find = true;
        LOG_DEBUG("succ to find expr from table_exprs_maps_", K(i), K(name), K(ret_expr));
      }

      if (OB_UNLIKELY(OB_HASH_NOT_EXIST == ret)) {
        ret = OB_SUCCESS;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!find) {
      ret = OB_HASH_NOT_EXIST;
      LOG_WDIAG("fail to find name from ObProxyStmtCtx", K(name), K_(valid_level), K(ret));
    }
  }

  return ret;
}

// find a name in Abstract Syntatic Tree supposing it's a table name
int ObProxyStmtCtx::find_table_name(const ObString& name, ObProxyExpr*& ret_expr)
{
  int ret = OB_SUCCESS;

  bool find = false;
  int last_level = valid_level_ - 1;

  if (OB_UNLIKELY(NULL != ret_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("wrong argument", K(ret_expr), K(ret));
  } else if (OB_UNLIKELY(last_level < 0 || last_level >= table_exprs_maps_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected level", K(last_level), K(ret));
  } else {
    for (int i = last_level; OB_SUCC(ret) && !find && i >= 0; --i) {
      if (OB_ISNULL(table_exprs_maps_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected point", K(i), K(ret));
      } else if (OB_FAIL(table_exprs_maps_[i]->get_refactored(name, ret_expr))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WDIAG("fail to get name from table_exprs_maps_", K(i), K(name), K(ret_expr), K(ret));
        } else {
          LOG_DEBUG("fail to get name from table_exprs_maps_", K(i), K(name), K(ret_expr), K(ret));
          ret = OB_SUCCESS;
        }
      } else {
        find = true;
        LOG_DEBUG("succ to find expr from table_exprs_maps_", K(i), K(name), K(ret_expr));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!find) {
      // not find may be by design
      ret = OB_HASH_NOT_EXIST;
      LOG_DEBUG("fail to find name from ObProxyStmtCtx", K(name), K_(valid_level), K(ret));
    }
  }

  return ret;
}

// find a name in Abstract Syntatic Tree supposing it's a alias name
int ObProxyStmtCtx::find_alias_name(const ObString& name, ObProxyExpr*& ret_expr)
{
  int ret = OB_SUCCESS;

  bool find = false;
  int last_level = valid_level_ - 1;

  if (OB_UNLIKELY(NULL != ret_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("wrong argument", K(ret_expr), K(ret));
  } else if (OB_UNLIKELY(last_level < 0 || last_level >= table_exprs_maps_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected level", K(last_level), K(ret));
  } else {
    for (int i = last_level; OB_SUCC(ret) && !find && i >= 0; --i) {
      if (OB_ISNULL(alias_exprs_maps_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected point", K(i), K(ret));
      } else if (OB_FAIL(alias_exprs_maps_[i]->get_refactored(name, ret_expr))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WDIAG("fail to get name from alias_exprs_maps_", K(i), K(name), K(ret_expr), K(ret));
        } else {
          LOG_DEBUG("fail to get name from alias_exprs_maps_", K(i), K(name), K(ret_expr), K(ret));
          ret = OB_SUCCESS;
        }
      } else {
        find = true;
        LOG_DEBUG("succ to find expr from alias_exprs_maps_", K(i), K(name), K(ret_expr));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!find) {
      // not find may be by design
      ret = OB_HASH_NOT_EXIST;
      LOG_DEBUG("fail to find name from ObProxyStmtCtx", K(name), K_(valid_level), K(ret));
    }
  }

  return ret;
}

// add a table name in Abstract Syntatic Tree in current level
int ObProxyStmtCtx::add_table_name(const ObString& name, ObProxyExpr* const table_expr)
{
  int ret = OB_SUCCESS;
  int last_level = valid_level_ - 1;

  if (OB_UNLIKELY(last_level < 0 || last_level >= table_exprs_maps_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected level", K(last_level), K(ret));
  } else if (OB_FAIL(table_exprs_maps_[last_level]->set_refactored(name, table_expr))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_DEBUG("table name exists", K(last_level), K(name), K(table_expr), K(ret));
    } else {
      LOG_WDIAG("fail to add table name", K(last_level), K(name), K(table_expr), K(ret));
    }
  } else {
    LOG_DEBUG("succ to set table_expr", K(last_level), K(name), K(table_expr));
  }

  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_FAIL(all_table_exprs_map_.set_refactored(name, table_expr))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_DEBUG("table name exists", K(last_level), K(name), K(table_expr), K(ret));
    } else {
      LOG_WDIAG("fail to add table name", K(last_level), K(name), K(table_expr), K(ret));
    }
  } else if (OB_FAIL(all_table_exprs_array_.push_back(std::make_pair(name, table_expr)))) {
    LOG_WDIAG("fail to push_back table name", K(last_level), K(name), K(table_expr), K(ret));
  }

  return ret;
}

// add a alias name in Abstract Syntatic Tree in current level
int ObProxyStmtCtx::add_alias_name(const ObString& name, ObProxyExpr* const alias_expr)
{
  int ret = OB_SUCCESS;
  int last_level = valid_level_ - 1;

  if (OB_UNLIKELY(last_level < 0 || last_level >= alias_exprs_maps_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected level", K(last_level), K(ret));
  } else if (OB_FAIL(alias_exprs_maps_[last_level]->set_refactored(name, alias_expr))) {
    if (OB_HASH_EXIST == ret) {
      LOG_DEBUG("alias name exists", K(last_level), K(name), K(alias_expr), K(ret));
    } else {
      LOG_WDIAG("fail to add alias name", K(last_level), K(name), K(alias_expr), K(ret));
    }
  } else {
    LOG_DEBUG("succ to set alias_expr", K(last_level), K(name), K(alias_expr));
  }

  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase