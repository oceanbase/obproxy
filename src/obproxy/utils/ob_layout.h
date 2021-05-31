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

#ifndef OBPROXY_LAYOUT_H
#define OBPROXY_LAYOUT_H
#include "lib/ob_define.h"
#include "lib/file/file_directory_utils.h"
#include "iocore/eventsystem/ob_buf_allocator.h"

namespace oceanbase
{
namespace obproxy
{
class ObLayout
{
public:
  static const int64_t MAX_PATH_LENGTH = 4096;

  ObLayout();
  ~ObLayout();

  int init(const char *start_cmd);
  const char *get_bin_dir() const { return bin_dir_; }
  const char *get_etc_dir() const { return etc_dir_; }
  const char *get_log_dir() const { return log_dir_; }
  const char *get_conf_dir() const { return conf_dir_; }
  const char *get_control_config_dir() const { return control_config_dir_; }
  const char *get_dbconfig_dir() const { return dbconfig_dir_; }
  const char *get_proxy_root_dir() const { return prefix_; }
  static int merge_file_path(const char *root, const char *file, common::ObIAllocator &allocator, char *&buf);

private:
  int init_bin_dir(const char *cwd, const char *start_cmd);
  int init_dir_prefix(const char *cwd);
  int construct_dirs();
  int construct_single_dir(const char *sub_dir, char *&full_path);
  int extract_actual_path(const char * const full_path, char *&actual_path);

private:
  bool is_inited_;
  char *prefix_;
  char *bin_dir_;
  char *etc_dir_;
  char *log_dir_;
  char *conf_dir_;
  char *control_config_dir_;
  char *dbconfig_dir_;
  event::ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator_;
};

ObLayout &get_global_layout();
}//end of obproxy
}//end of oceanbase

#endif /* OBPROXY_LAYOUT_H */


