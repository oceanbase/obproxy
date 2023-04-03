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
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <malloc.h>
#include "ob_proxy_main.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "ob_proxy_init.h"
#include "lib/json/ob_json.h"
#include "obutils/ob_proxy_json_config_info.h"

using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{

OBPROXY_RUN_MODE g_run_mode = RUN_MODE_PROXY;
static bool is_obproxy_inited = false;
DRWLock local_init_lock;

int init_obproxy_client(const ObString &config)
{
  int ret = OB_SUCCESS;
  if (!is_obproxy_inited) {
    DRWLock::WRLockGuard lock(local_init_lock);
    if (!is_obproxy_inited) {
      static const int DEFAULT_MMAP_MAX_VAL = 1024 * 1024 * 1024; // 1G
      mallopt(M_MMAP_MAX, DEFAULT_MMAP_MAX_VAL);
      mallopt(M_ARENA_MAX, 1); // disable malloc multiple arena pool

      setlocale(LC_ALL, "");

      // turn warn log on so that there's a obproxy.log.wf file which records
      // all WARN and ERROR logs in log directory.
      ObWarningBuffer::set_warn_log_on(true);
      // DISABLE_DIA(); disable di_cache stat

      g_run_mode = RUN_MODE_CLIENT;
      int argc = 0;
      char argv[10][4096];
      memset(argv, 0, sizeof(argv));
      MEMCPY(argv[0], "./bin/obproxy", strlen("./bin/obproxy"));
      argc++;
      if (!config.empty()) {
        Parser parser;
        json::Value *json_value = NULL;
        ObArenaAllocator json_allocator(ObModIds::OB_JSON_PARSER);
        if (OB_FAIL(parser.init(&json_allocator))) {
          MPRINT("json parser init failed, ret =%d", ret);
        } else if (OB_FAIL(parser.parse(config.ptr(), config.length(), json_value))) {
          MPRINT("json parse failed, ret =%d", ret);
        } else if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, json::JT_OBJECT))) {
          MPRINT("check config info type failed, ret=%d", ret);
        } else {
          DLIST_FOREACH(p, json_value->get_object()) {
            if (0 == p->name_.case_compare("proxy_config")) {
              if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, json::JT_STRING))) {
                MPRINT("check config info type failed, ret=%d", ret);
              } else {
                MEMCPY(argv[argc++], "-o", strlen("-o"));
                MEMCPY(argv[argc++], p->value_->get_string().ptr(), p->value_->get_string().length());
              }
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        char *tmp_argv[10];
        for (int i = 0; i < argc; i++) {
          tmp_argv[i] = argv[i];
        }
        ObProxyMain *proxy_main = ObProxyMain::get_instance();
        if (OB_ISNULL(proxy_main)) {
          ret = OB_ERR_UNEXPECTED;
          MPRINT("fail to get obproxy main instance, ret=%d", ret);
        } else if (OB_FAIL(proxy_main->start(argc, tmp_argv))) {
          MPRINT("fail to start obproxy, ret=%d", ret);
        } else {
          is_obproxy_inited = true;
          atexit(destroy_obproxy_client);
        }
      }
    }
  }

  return ret;
}

void destroy_obproxy_client()
{
  ObProxyMain *proxy_main = ObProxyMain::get_instance();
  if (OB_NOT_NULL(proxy_main)) {
    proxy_main->destroy();
  }
}

}
}

