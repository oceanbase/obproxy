/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RPC_OBMYSQL

#include "rpc/obmysql/packet/ompk_row.h"

using namespace oceanbase::obmysql;

OMPKRow::OMPKRow(const ObMySQLRow &row)
    : row_(row)
{

}

int OMPKRow::decode()
{
  int ret = OB_SUCCESS;
  //OB_ASSERT(NULL != cdata_);
  if (NULL == cdata_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("null input", K(ret));
  }

  return ret;
}

int OMPKRow::serialize(char *buffer, int64_t len, int64_t &pos) const
{
  return row_.serialize(buffer, len, pos);
}
