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

#include <gtest/gtest.h>
#define private public
#define protected public
#include "easy_io_struct.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/allocator/ob_malloc.h"
#include "obproxy/ob_proxy_session_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace obproxy
{
struct ObSessionStruct
{
  ObSessionStruct() :mgr_(NULL), client_(NULL), ret_get_(NULL), ret_erase_(NULL) {}
  ObProxySessionMgr *mgr_;
  ObSQLSessionKey key_;
  ObClientSessionInfo *client_;
  int *ret_get_;
  int *ret_erase_;
};

void* func(void *ptr)
{
  ObSessionStruct *st = static_cast<ObSessionStruct *>(ptr);
  *st->ret_get_ = st->mgr_->try_acquire_session(st->key_, st->client_);
  *st->ret_erase_ = st->mgr_->erase(st->key_);
  sleep (1);
  return NULL;
}

//--------test ObClientSessionInfo-----------//
TEST(test_proxy_session_mgr, init_get_and_set)
{
  //test init
  ObProxySessionMgr session_mgr;
  //test init
  ASSERT_EQ(OB_INVALID_ARGUMENT, session_mgr.init(1, 0));
  ASSERT_EQ(OB_SUCCESS, session_mgr.init(2, 2));
  ASSERT_EQ(OB_INIT_TWICE, session_mgr.init(2, 2));
  //test get_session_count
  ASSERT_EQ(0, session_mgr.get_client_session_count());
  ASSERT_EQ(0, session_mgr.get_server_session_count());
  ASSERT_EQ(0, session_mgr.get_total_session_count());

//  ObString user_name;
//  ObString var_name = ObString::make_string("yyy");

  //test alloc
}

TEST(test_proxy_session_mgr, alloc_and_free)
{
  ObProxySessionMgr session_mgr;
  ASSERT_EQ(NULL, session_mgr.alloc_client_session());
  ASSERT_EQ(NULL, session_mgr.alloc_server_session());
  ASSERT_EQ(OB_NOT_INIT, session_mgr.free_client_session(NULL));
  ASSERT_EQ(OB_NOT_INIT, session_mgr.free_server_session(NULL));

  ASSERT_EQ(OB_SUCCESS, session_mgr.init(2, 2));
  ASSERT_EQ(OB_INVALID_ARGUMENT, session_mgr.free_client_session(NULL));
  ASSERT_EQ(OB_INVALID_ARGUMENT, session_mgr.free_server_session(NULL));
  ObClientSessionInfo * client_1 = NULL;
  ObClientSessionInfo * client_2 = NULL;
  ObServerSessionInfo * server_1 = NULL;
  ObServerSessionInfo * server_2 = NULL;
  ASSERT_TRUE(NULL != (client_1 = session_mgr.alloc_client_session()));
  ASSERT_TRUE(NULL != (client_2 = session_mgr.alloc_client_session()));
  ASSERT_TRUE(NULL == session_mgr.alloc_client_session());
  //test alloc
  ASSERT_TRUE(NULL != (server_1 = session_mgr.alloc_server_session()));
  ASSERT_TRUE(NULL != (server_2 = session_mgr.alloc_server_session()));
  ASSERT_TRUE(NULL == session_mgr.alloc_server_session());
  ASSERT_EQ(OB_SUCCESS, session_mgr.free_client_session(client_1));
  ASSERT_EQ(OB_SUCCESS, session_mgr.free_client_session(client_2));
  ASSERT_EQ(OB_SIZE_OVERFLOW, session_mgr.free_client_session(client_2));
  ASSERT_EQ(OB_SUCCESS, session_mgr.free_server_session(server_1));
  ASSERT_EQ(OB_SUCCESS, session_mgr.free_server_session(server_2));
  ASSERT_EQ(OB_SIZE_OVERFLOW, session_mgr.free_server_session(server_2));
}

TEST(test_proxy_session_mgr, acquire_and_erase_and_kill)
{
  ObProxySessionMgr session_mgr;
  ObSQLSessionKey key;
  ObClientSessionInfo *client_1 = NULL;
  ObClientSessionInfo *client_2 = NULL;
  ObClientSessionInfo *client_3 = NULL;
  ASSERT_EQ(OB_NOT_INIT, session_mgr.acquire_session(key, client_1));
  ASSERT_EQ(OB_NOT_INIT, session_mgr.try_acquire_session(key, client_1));
  ASSERT_EQ(OB_NOT_INIT, session_mgr.kill_session(key, false));
  ASSERT_EQ(OB_SUCCESS, session_mgr.init(2, 2));
  ASSERT_TRUE(NULL != (client_1 = session_mgr.alloc_client_session()));
  ASSERT_TRUE(NULL != (client_2 = session_mgr.alloc_client_session()));
  key.set_array_idx(2);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, session_mgr.acquire_session(key, client_3));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, session_mgr.try_acquire_session(key, client_3));
  ASSERT_EQ(OB_SUCCESS, session_mgr.erase(key));
  ASSERT_EQ(OB_SESSION_INVALID, session_mgr.kill_session(key, true));
  ASSERT_EQ(OB_ERR_UNKNOWN_SESSION_ID, session_mgr.kill_session(key, false));
  key.set_array_idx(0);
  ASSERT_EQ(OB_SUCCESS, session_mgr.acquire_session(key, client_3));
  client_3->unlock();

  key.inc_reuse_count();
  ASSERT_EQ(OB_SESSION_INVALID, session_mgr.acquire_session(key, client_3));
  ASSERT_EQ(OB_ERR_UNKNOWN_SESSION_ID, session_mgr.kill_session(key, false));
  ASSERT_EQ(OB_SESSION_INVALID, session_mgr.try_acquire_session(key, client_3));
  ASSERT_EQ(OB_SUCCESS, session_mgr.erase(key));
  ASSERT_EQ(OB_ERR_UNKNOWN_SESSION_ID, session_mgr.kill_session(key, true));
  client_1->set_valid(false);
  ASSERT_EQ(OB_SESSION_INVALID, session_mgr.acquire_session(key, client_3));
  ASSERT_EQ(OB_SESSION_INVALID, session_mgr.try_acquire_session(key, client_3));

  ObSQLSessionKey key_1;
  key_1.set_array_idx(1);
  ASSERT_EQ(OB_SUCCESS, session_mgr.try_acquire_session(key_1, client_3));
  client_3->unlock();
  ASSERT_EQ(OB_SUCCESS, session_mgr.kill_session(key_1, true));
  ASSERT_EQ(OB_SUCCESS, session_mgr.kill_session(key_1, false));
  ASSERT_EQ(OB_SUCCESS, session_mgr.erase(key));
}

TEST(test_proxy_session_mgr, try_acquire)
{
  ObProxySessionMgr session_mgr;
  ObSQLSessionKey key;
  key.set_array_idx(0);
  ObClientSessionInfo * client_1 = NULL;
  ObClientSessionInfo * client_2 = NULL;
  ObClientSessionInfo * client_3 = NULL;
  ObClientSessionInfo * client_4 = NULL;
  ASSERT_EQ(OB_SUCCESS, session_mgr.init(2, 2));
  ASSERT_TRUE(NULL != (client_1 = session_mgr.alloc_client_session()));
  ASSERT_TRUE(NULL != (client_2 = session_mgr.alloc_client_session()));
  pthread_t p1; 
  pthread_t p2; 
  ObSessionStruct st_1;
  ObSessionStruct st_2;
  int ret_1 = OB_SUCCESS;
  int ret_2 = OB_SUCCESS;
  int ret_3 = OB_SUCCESS;
  int ret_4 = OB_SUCCESS;
  st_1.mgr_ = &session_mgr;
  st_1.ret_get_ = &ret_1;
  st_1.ret_erase_ = &ret_2;
  st_1.key_ = key;
  st_1.client_ = client_3;
  st_2.mgr_ = &session_mgr;
  st_2.ret_get_ = &ret_3;
  st_2.ret_erase_ = &ret_4;
  st_2.key_ = key;
  st_2.client_ = client_4;
  int p_ret = 0;
  if (0 != (p_ret = pthread_create(&p1, NULL, func, &st_1))) {
    LOG_ERROR("failed to create p");
  }
  if (0 != (p_ret = pthread_create(&p2, NULL, func, &st_2))) {
    LOG_ERROR("failed to create p");
  }
  pthread_join(p1, NULL);
  pthread_join(p2, NULL);
  ASSERT_EQ(OB_SUCCESS, ret_1);
  ASSERT_EQ(OB_SUCCESS, ret_2);
  ASSERT_EQ(OB_EAGAIN, ret_3);
  ASSERT_EQ(OB_EAGAIN, ret_4);
}

TEST(test_proxy_session_mgr, other_get_funcs)
{
  ObProxySessionMgr session_mgr;
  ObSessionKeyInfo key;
  key.session_key_.set_array_idx(0);
  ObClientSessionInfo * client_1 = NULL;
  ObClientSessionInfo * client_2 = NULL;
  ObServerSessionInfo * server_1 = NULL;
  ObString time_var = ObString::make_string("wait_timeout");
  ObString user_name = ObString::make_string("yyy");
  ASSERT_EQ(NULL, session_mgr.get_variable_value(key, time_var));
  ASSERT_EQ(NULL, session_mgr.get_session_user(key));
  ASSERT_EQ(NULL, session_mgr.get_log_id_level_map(key));
  ASSERT_EQ(NULL, session_mgr.get_session_info(key));
  ASSERT_EQ(OB_SUCCESS, session_mgr.init(2, 2));
  ASSERT_TRUE(NULL != (client_1 = session_mgr.alloc_client_session()));
  ASSERT_TRUE(NULL != (client_2 = session_mgr.alloc_client_session()));
  ASSERT_TRUE(NULL != (server_1 = session_mgr.alloc_server_session()));

  key.session_type_ = OB_MAX_SESSION_TYPE;
  ASSERT_EQ(NULL, session_mgr.get_variable_value(key, time_var));
  ASSERT_EQ(NULL, session_mgr.get_session_user(key));
  ASSERT_EQ(NULL, session_mgr.get_log_id_level_map(key));

  key.session_type_ = OB_CLIENT_SESSION_TYPE;
  ObObj int_value;
  ObObj type;
  type.set_type(ObIntType);
  int_value.set_int(1000);
  int ret = client_1->replace_variable(time_var, int_value);
  ASSERT_EQ(hash::HASH_INSERT_SUCC, ret);
  ASSERT_EQ(OB_SUCCESS, client_1->set_user_name(user_name));
  ret = server_1->replace_variable(time_var, int_value);
  ASSERT_EQ(hash::HASH_INSERT_SUCC, ret);
  ASSERT_EQ(OB_SUCCESS, server_1->set_user_name(user_name));

  ASSERT_TRUE(NULL != session_mgr.get_variable_value(key, time_var));
  ASSERT_TRUE(NULL != session_mgr.get_session_user(key));
  client_1->log_id_level_map_valid_ = true;
  ASSERT_TRUE(NULL != session_mgr.get_log_id_level_map(key));
  key.session_key_.set_array_idx(2);
  ASSERT_EQ(NULL, session_mgr.get_variable_value(key, time_var));
  key.session_type_ = OB_SERVER_SESSION_TYPE;
  ASSERT_EQ(NULL, session_mgr.get_variable_value(key, time_var));
  key.session_key_.set_array_idx(0);
  key.session_key_.inc_reuse_count();
  ASSERT_EQ(NULL, session_mgr.get_variable_value(key, time_var));
  server_1->set_valid(false);
  ASSERT_EQ(NULL, session_mgr.get_variable_value(key, time_var));
  LOG_ERROR("test key to_string:", K(key));
}

}
}


int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("ERROR");
  oceanbase::common::ObLogger::get_logger().set_log_level("ERROR");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
