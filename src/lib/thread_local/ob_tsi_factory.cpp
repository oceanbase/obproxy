/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/thread_local/ob_tsi_factory.h"

namespace oceanbase
{
namespace common
{
TSIFactory &get_tsi_fatcory()
{
  static TSIFactory instance;
  return instance;
}

void tsi_factory_init()
{
  get_tsi_fatcory().init();
}

void tsi_factory_destroy()
{
  get_tsi_fatcory().destroy();
}
} // namespace common
} // namespace oceanbase
