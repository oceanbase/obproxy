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
 *
 */

#ifndef OBPROXY_EVENTSYSTEM_H
#define OBPROXY_EVENTSYSTEM_H

#include "iocore/eventsystem/ob_event_processor.h"
#include "iocore/eventsystem/ob_task.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

#define EVENT_SYSTEM_MODULE_MAJOR_VERSION 1
#define EVENT_SYSTEM_MODULE_MINOR_VERSION 0
#define EVENT_SYSTEM_MODULE_VERSION make_module_version(               \
                                    EVENT_SYSTEM_MODULE_MAJOR_VERSION, \
                                    EVENT_SYSTEM_MODULE_MINOR_VERSION, \
                                    PUBLIC_MODULE_HEADER)

int init_event_system(ObModuleVersion version);

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_EVENTSYSTEM_H
