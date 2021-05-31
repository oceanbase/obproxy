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

#ifdef OB_STMT_TYPE_DEF
OB_STMT_TYPE_DEF(T_NONE, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SELECT, get_dml_stmt_need_privs)
OB_STMT_TYPE_DEF(T_INSERT, get_dml_stmt_need_privs)
OB_STMT_TYPE_DEF(T_REPLACE, get_dml_stmt_need_privs)
OB_STMT_TYPE_DEF(T_DELETE, get_dml_stmt_need_privs)
OB_STMT_TYPE_DEF(T_UPDATE, get_dml_stmt_need_privs)
OB_STMT_TYPE_DEF(T_EXPLAIN, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_CREATE_TENANT, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_DROP_TENANT, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_LOCK_TENANT, get_lock_tenant_stmt_need_privs)
OB_STMT_TYPE_DEF(T_MODIFY_TENANT, get_modify_tenant_stmt_need_privs)
OB_STMT_TYPE_DEF(T_CHANGE_TENANT, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_CREATE_RESOURCE_POOL, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_DROP_RESOURCE_POOL, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_ALTER_RESOURCE_POOL, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_CREATE_RESOURCE_UNIT, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_ALTER_RESOURCE_UNIT, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_DROP_RESOURCE_UNIT, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_CREATE_TABLE, get_create_table_stmt_need_privs)
OB_STMT_TYPE_DEF(T_DROP_TABLE, get_drop_table_stmt_need_privs)
OB_STMT_TYPE_DEF(T_ALTER_TABLE, get_alter_table_stmt_need_privs)
OB_STMT_TYPE_DEF(T_CREATE_INDEX, get_create_index_stmt_need_privs)
OB_STMT_TYPE_DEF(T_DROP_INDEX, get_drop_index_stmt_need_privs)
OB_STMT_TYPE_DEF(T_CREATE_VIEW, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_ALTER_VIEW, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_DROP_VIEW, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_HELP, no_priv_needed)
OB_STMT_TYPE_DEF(T_SHOW_TABLES, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_DATABASES, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_COLUMNS, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_VARIABLES, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_TABLE_STATUS, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_SCHEMA, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_CREATE_DATABASE, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_CREATE_TABLE, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_CREATE_VIEW, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_PARAMETERS, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_SERVER_STATUS, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_INDEXES, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_WARNINGS, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_ERRORS, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_PROCESSLIST, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_CHARSET, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_COLLATION, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_TABLEGROUPS, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_STATUS, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_TENANT, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_CREATE_TENANT, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_TRACE, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_ENGINES, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_PRIVILEGES, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SHOW_GRANTS, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_CREATE_USER, get_create_user_privs)
OB_STMT_TYPE_DEF(T_DROP_USER, get_create_user_privs)
OB_STMT_TYPE_DEF(T_SET_PASSWORD, get_create_user_privs)
OB_STMT_TYPE_DEF(T_LOCK_USER, get_create_user_privs)
OB_STMT_TYPE_DEF(T_RENAME_USER, get_create_user_privs)
OB_STMT_TYPE_DEF(T_GRANT, get_grant_stmt_need_privs)
OB_STMT_TYPE_DEF(T_REVOKE, get_revoke_stmt_need_privs)
OB_STMT_TYPE_DEF(T_PREPARE, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_VARIABLE_SET, get_variable_set_stmt_need_privs)
OB_STMT_TYPE_DEF(T_EXECUTE, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_DEALLOCATE, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_START_TRANS, no_priv_needed)
OB_STMT_TYPE_DEF(T_END_TRANS, no_priv_needed)
OB_STMT_TYPE_DEF(T_KILL, no_priv_needed)
OB_STMT_TYPE_DEF(T_ALTER_SYSTEM, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_CHANGE_OBI, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SWITCH_MASTER, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_SERVER_ACTION, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_BOOTSTRAP, get_boot_strap_stmt_need_privs)
OB_STMT_TYPE_DEF(T_CS_DISKMAINTAIN, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_TABLET_CMD, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_REPORT_REPLICA, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_SWITCH_ROOTSERVER, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_SWITCH_UPDATESERVER, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_CLUSTER_MANAGER, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_FREEZE, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_FLUSH_PLAN_CACHE, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_DROP_MEMTABLE, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_CLEAR_MEMTABLE, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_PRINT_ROOT_TABLE, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_ADD_UPDATESERVER, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_DELETE_UPDATESERVER, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_CHECK_ROOT_TABLE, err_stmt_type_priv)
OB_STMT_TYPE_DEF(T_CLEAR_ROOT_TABLE, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_REFRESH_SCHEMA, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_CREATE_DATABASE, get_create_database_stmt_need_privs)
OB_STMT_TYPE_DEF(T_USE_DATABASE, no_priv_needed)
OB_STMT_TYPE_DEF(T_ADMIN_SERVER, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_ADMIN_ZONE, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_SWITCH_REPLICA_ROLE, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_DROP_REPLICA, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_MIGRATE_REPLICA, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_RECYCLE_REPLICA, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_ADMIN_MERGE, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_ALTER_DATABASE, get_alter_database_stmt_need_privs)
OB_STMT_TYPE_DEF(T_DROP_DATABASE, get_drop_database_stmt_need_privs)
OB_STMT_TYPE_DEF(T_CREATE_TABLEGROUP, get_create_tablegroup_stmt_need_privs)
OB_STMT_TYPE_DEF(T_DROP_TABLEGROUP, get_drop_tablegroup_stmt_need_privs)
OB_STMT_TYPE_DEF(T_ALTER_TABLEGROUP, get_alter_tablegroup_stmt_need_privs)
OB_STMT_TYPE_DEF(T_TRUNCATE_TABLE, get_truncate_table_stmt_need_privs)
OB_STMT_TYPE_DEF(T_RENAME_TABLE, get_rename_table_stmt_need_privs)
OB_STMT_TYPE_DEF(T_CREATE_TABLE_LIKE, get_create_table_like_stmt_need_privs)
OB_STMT_TYPE_DEF(T_SET_NAMES, no_priv_needed)
OB_STMT_TYPE_DEF(T_CLEAR_LOCATION_CACHE, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_RELOAD_UNIT, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_RELOAD_SERVER, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_RELOAD_ZONE, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_CLEAR_MERGE_ERROR, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_MIGRATE_UNIT, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_UPGRADE_VIRTUAL_SCHEMA, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_RUN_JOB, get_sys_tenant_super_priv)
OB_STMT_TYPE_DEF(T_EMPTY_QUERY, no_priv_needed)
OB_STMT_TYPE_DEF(T_CREATE_OUTLINE, no_priv_needed)
OB_STMT_TYPE_DEF(T_DROP_OUTLINE, no_priv_needed)
OB_STMT_TYPE_DEF(T_SWITCH_RS_ROLE, get_sys_tenant_super_priv)
#endif

#ifndef OCEANBASE_SQL_RESOLVER_OB_STMT_TYPE_
#define OCEANBASE_SQL_RESOLVER_OB_STMT_TYPE_

namespace oceanbase {
namespace sql {
namespace stmt {

enum StmtType
{
#define OB_STMT_TYPE_DEF(stmt_type, priv_check_func) stmt_type,
#include "sql/resolver/ob_stmt_type.h"
#undef OB_STMT_TYPE_DEF
};

}
}
}

#endif /* _OB_STMT_TYPE_H */
