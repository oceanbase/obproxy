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
#include "proxy/mysql/ob_prepare_statement_struct.h"
#include "lib/ob_errno.h"
#include <pthread.h>
#include <thread>
#define NUM_THREADS 128
#define NUM_SQL 64
using namespace oceanbase::common;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
static char ps_sql[NUM_SQL][40] { 
  "SELECT * FROM T WHERE C1=?\0", 
  "SELECT * FROM T WHERE C2=?\0",
  "SELECT * FROM T WHERE C3=?\0",
  "SELECT * FROM T WHERE C4=?\0",
  "SELECT * FROM T WHERE C5=?\0",
  "SELECT * FROM T WHERE C6=?\0",
  "SELECT * FROM T WHERE C7=?\0",
  "SELECT * FROM T WHERE C8=?\0",
  "SELECT * FROM T WHERE C9=?\0",
  "SELECT * FROM T WHERE C10=?\0",
  "SELECT * FROM T WHERE C11=?\0",
  "SELECT * FROM T WHERE C12=?\0",
  "SELECT * FROM T WHERE C13=?\0",
  "SELECT * FROM T WHERE C14=?\0",
  "SELECT * FROM T WHERE C15=?\0",
  "SELECT * FROM T WHERE C16=?\0",
  "SELECT * FROM T WHERE C17=?\0",
  "SELECT * FROM T WHERE C18=?\0",
  "SELECT * FROM T WHERE C19=?\0",
  "SELECT * FROM T WHERE C20=?\0",
  "SELECT * FROM T WHERE C21=?\0",
  "SELECT * FROM T WHERE C21=?\0",
  "SELECT * FROM T WHERE C22=?\0",
  "SELECT * FROM T WHERE C23=?\0",
  "SELECT * FROM T WHERE C24=?\0",
  "SELECT * FROM T WHERE C25=?\0",
  "SELECT * FROM T WHERE C26=?\0",
  "SELECT * FROM T WHERE C27=?\0",
  "SELECT * FROM T WHERE C28=?\0",
  "SELECT * FROM T WHERE C29=?\0",
  "SELECT * FROM T WHERE C30=?\0",
  "SELECT * FROM T WHERE C31=?\0",
  "SELECT * FROM T WHERE C32=?\0",
  "SELECT * FROM T WHERE C33=?\0",
  "SELECT * FROM T WHERE C34=?\0",
  "SELECT * FROM T WHERE C35=?\0",
  "SELECT * FROM T WHERE C36=?\0",
  "SELECT * FROM T WHERE C37=?\0",
  "SELECT * FROM T WHERE C38=?\0",
  "SELECT * FROM T WHERE C39=?\0",
  "SELECT * FROM T WHERE C40=?\0",
  "SELECT * FROM T WHERE C41=?\0",
  "SELECT * FROM T WHERE C42=?\0",
  "SELECT * FROM T WHERE C43=?\0",
  "SELECT * FROM T WHERE C44=?\0",
  "SELECT * FROM T WHERE C45=?\0",
  "SELECT * FROM T WHERE C46=?\0",
  "SELECT * FROM T WHERE C47=?\0",
  "SELECT * FROM T WHERE C48=?\0",
  "SELECT * FROM T WHERE C49=?\0",
  "SELECT * FROM T WHERE C50=?\0",
  "SELECT * FROM T WHERE C51=?\0",
  "SELECT * FROM T WHERE C52=?\0",
  "SELECT * FROM T WHERE C53=?\0",
  "SELECT * FROM T WHERE C54=?\0",
  "SELECT * FROM T WHERE C55=?\0",
  "SELECT * FROM T WHERE C56=?\0",
  "SELECT * FROM T WHERE C57=?\0",
  "SELECT * FROM T WHERE C58=?\0",
  "SELECT * FROM T WHERE C59=?\0",
  "SELECT * FROM T WHERE C60=?\0",
  "SELECT * FROM T WHERE C61=?\0",
  "SELECT * FROM T WHERE C62=?\0",
  "SELECT * FROM T WHERE C63=?\0",};
void *op(void *arg) {
  UNUSED(arg);
  int ret = OB_SUCCESS;
  int64_t i = 0;
  ObBasePsEntryGlobalCache& ps_entry_cache = get_global_ps_entry_cache();
  while(1) {
    i++;
    ObGlobalPsEntry *global_ps_entry = NULL;
    obutils::ObSqlParseResult result;
    ObString sql(ps_sql[std::rand() % NUM_SQL]);
    if (OB_FAIL(ps_entry_cache.acquire_or_create_ps_entry(sql, result, global_ps_entry))) {
      std::cout << "fail to acquire ps entry: " << sql.ptr() <<  ", ret:" << ret << std::endl;
    }
    if (global_ps_entry != NULL) {
      global_ps_entry->dec_ref();
    }
    if (i % 100000 == 0) {
      std::cout << "now " << i << std::endl;
    }
  }
  return NULL;
}

} // end of proxy
} // end of obproxy
} // end of oceanbase

int main(int argc, char **argv) {
  UNUSED(argc);
  UNUSED(argv);
  pthread_t tids_op[NUM_THREADS];
  for(int i = 0; i < NUM_THREADS; ++i) {
    pthread_create(&tids_op[i], NULL, oceanbase::obproxy::proxy::op, NULL);
  }
  pthread_exit(NULL);
  return 0;
}