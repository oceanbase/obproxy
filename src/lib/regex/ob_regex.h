/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_REGEX_OB_REGEX_
#define OCEANBASE_LIB_REGEX_OB_REGEX_

#include <regex.h>
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
class ObRegex
{
public:
  ObRegex();
  virtual ~ObRegex();
public:
  int init(const char* pattern, int flags);
  int match(const char* text, int flags, bool &is_match);
  void destroy();
  inline const regmatch_t* get_match() const
  {
    return match_;
  }
  inline int64_t get_match_count() const
  {
    return static_cast<int64_t>(nmatch_);
  }
private:
  bool init_;
  regmatch_t* match_;
  regex_t reg_;
  size_t nmatch_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRegex);
};
}
}

#endif //OCEANBASE_LIB_REGEX_OB_REGEX_

