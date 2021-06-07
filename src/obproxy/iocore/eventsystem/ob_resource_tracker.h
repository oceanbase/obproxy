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

#ifndef OBPROXY_RESOURCE_TRACKER_H
#define OBPROXY_RESOURCE_TRACKER_H

#include "utils/ob_proxy_lib.h"
#include "lib/lock/ob_mutex.h"
#include "lib/hash/ob_build_in_hashmap.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

extern volatile bool res_track_memory;   // set this to zero to disable resource tracking

#define __RES_PATH(x)   #x
#define _RES_PATH(x)    __RES_PATH (x)
#define RES_PATH(x)     x __FILE__ ":" _RES_PATH (__LINE__)

// class ObSrcLoc
//
// The ObSrcLoc class wraps up a source code location, including file
// name, function name, and line number, and contains a method to
// format the result into a string buffer.

#define MAKE_LOCATION() oceanbase::obproxy::event::ObSrcLoc(__FILE__, __FUNCTION__, __LINE__)

class ObSrcLoc
{
public:
  ObSrcLoc() : file_(NULL), func_(NULL), line_(0) { }
  ObSrcLoc(const char *file, const char *func, int line) : file_(file), func_(func), line_(line) { }
  ObSrcLoc(const ObSrcLoc &rhs) : file_(rhs.file_), func_(rhs.func_), line_(rhs.line_) { }

  bool is_valid() const { return (NULL != file_ && 0 != line_); }
  ObSrcLoc &operator=(const ObSrcLoc &rhs)
  {
    file_ = rhs.file_;
    func_ = rhs.func_;
    line_ = rhs.line_;
    return *this;
  }
  TO_STRING_KV(K_(file), K_(func), K_(line));

public:
  const char *file_;
  const char *func_;
  int32_t line_;
};


/**
 * Individual resource to keep track of.  A map of these are in the ObMemoryResourceTracker.
 */
class ObMemoryResource
{
public:
  ObMemoryResource() : increment_count_(0), decrement_count_(0), value_(0) { }
  void increment(const int64_t size);
  int64_t get_value() const { return value_; }

public:
  common::ObString location_;
  LINK(ObMemoryResource, link_);

private:
  volatile int64_t increment_count_;
  volatile int64_t decrement_count_;
  volatile int64_t value_;

  DISALLOW_COPY_AND_ASSIGN(ObMemoryResource);
};

/**
 * Generic class to keep track of memory usage
 * Used to keep track of the location in the code that allocated ioBuffer memory
 */
class ObMemoryResourceTracker
{
public:
  ObMemoryResourceTracker() { };
  static int increment(const char *name, const int64_t size);
  static int lookup(const char *location, ObMemoryResource *&Resource);
  static void dump();

  static const int64_t HASH_BUCKET_SIZE = 64;

  // Interface class for client session connection id map.
  struct ObResourceHashing
  {
    typedef const common::ObString &Key;
    typedef ObMemoryResource Value;
    typedef ObDLList(ObMemoryResource, link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->location_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };

  typedef common::hash::ObBuildInHashMap<ObResourceHashing, HASH_BUCKET_SIZE> ObResourceHashMap; // Sessions by client session identity

private:
  static ObResourceHashMap g_resource_map;
  static ObMutex g_resource_lock;

  DISALLOW_COPY_AND_ASSIGN(ObMemoryResourceTracker);
};

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_RESOURCE_TRACKER_H
