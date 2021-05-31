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

#include <malloc.h>
#include "lib/oblog/ob_log.h"
#include "ob_proxy_main.h"
#include "lib/oblog/ob_warning_buffer.h"
using namespace oceanbase::common;
using namespace oceanbase::obproxy;


int main(int argc, char *argv[])
{
  int ret = OB_SUCCESS;

  static const int DEFAULT_MMAP_MAX_VAL = 1024 * 1024 * 1024; // 1G
  mallopt(M_MMAP_MAX, DEFAULT_MMAP_MAX_VAL);
  mallopt(M_ARENA_MAX, 1);// disable malloc multiple arena pool

  setlocale(LC_ALL, "");// just for print mem info, mem size can be segment by ','

  // turn warn log on so that there's a obproxy.log.wf file which records
  // all WARN and ERROR logs in log directory.
  ObWarningBuffer::set_warn_log_on(true);
  // DISABLE_DIA(); disable di_cache stat

  ObProxyMain *proxy_main = ObProxyMain::get_instance();
  if (OB_ISNULL(proxy_main)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to start obproxy", K(proxy_main), K(ret));
  } else if (OB_FAIL(proxy_main->start(argc, argv))) {
    if(OB_UNLIKELY(OB_NOT_RUNNING == ret)) {
      // if use ./obproxy -h or ./obproxy -V
      // will come here, but we need return success
      ret = OB_SUCCESS;
    } else {
      proxy_main->print_usage();
    }
  } else {
    proxy_main->destroy();
  }

  _exit(0);
}
