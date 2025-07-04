/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DINGOFS_SRC_CLIENT_OPTIONS_OPTION_H_
#define DINGOFS_SRC_CLIENT_OPTIONS_OPTION_H_

#include <gflags/gflags_declare.h>

#include "options/client/vfs/vfs_option.h"
#include "options/client/vfs_legacy/vfs_legacy_option.h"

namespace dingofs {
namespace client {

struct ClientOption {
  vfs::VFSOption vfs_option;
  VFSLegacyOption vfs_legacy_option;
};

void InitClientOption(utils::Configuration* conf, ClientOption* client_option);

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_OPTIONS_OPTION_H_