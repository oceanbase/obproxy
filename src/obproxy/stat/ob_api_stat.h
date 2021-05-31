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

#ifndef OBPROXY_API_STAT_H
#define OBPROXY_API_STAT_H

#include "utils/ob_proxy_lib.h"
#include "stat/ob_stat_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

/**
 * @brief A ObApiStat is an atomic variable that can be used to store counters,
 * averages, time averages, or summations.
 *
 * Stats are very easy to use, here is a simple example of how you can create
 * a counter and increment its
 * value:
 * @code
 *  ObApiStat stat;
 *  stat.init("stat_name");
 *  stat.inc();
 * @endcode
 *
 * A full example is available in examples/stat_example/.
 */
class ObApiStat
{
public:
  ObApiStat();
  ~ObApiStat();

  /**
   * You must initialize your ObApiStat with a call to this init() method.
   *
   * @param name       The string name of the stat.
   * @param type       The ObRawStatSyncType  of the ObApiStat, this decides how ObProxy
   *                   will treat your inputs. The default value is SYNC_COUNT.
   *
   * @return OB_SUCCESS if the stat was successfully created and OB_ERROR otherwise.
   * @see ObRawStatSyncType
   */
  int init(const char *name, const ObRawStatSyncType type = SYNC_COUNT);

  /**
   * This method allows you to increment a stat by a certain amount.
   *
   * @param amount the amount to increment the stat by the default value is 1.
   */
  void increment(const int64_t amount = 1);

  /**
   * This method allows you to decrement a stat by a certain amount.
   *
   * @param amount the amount to decrement the stat by the default value is 1.
   */
  void decrement(const int64_t amount = 1);

  /**
   * This method returns the current value of the stat.
   *
   * @return The value of the stat.
   */
  int64_t get() const;

  /**
   * This method sets the value of the stat.
   *
   * @param value  the value to set the stat to.
   */
  void set(const int64_t value);

  static int64_t get_next_stat_id();

private:
  int64_t stat_id_; // The internal stat ID
  DISALLOW_COPY_AND_ASSIGN(ObApiStat);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_API_STAT_H
