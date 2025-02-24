/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Dingofs
 * Date: 2022-02-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_METASERVER_STORAGE_UTILS_H_
#define DINGOFS_SRC_METASERVER_STORAGE_UTILS_H_

#include <cstdint>
#include <string>

namespace dingofs {
namespace metaserver {
namespace storage {

bool GetFileSystemSpaces(const std::string& path, uint64_t* capacity,
                         uint64_t* available);

bool GetProcMemory(uint64_t* vmRSS);

}  // namespace storage
}  // namespace metaserver
}  // namespace dingofs

#endif  //  DINGOFS_SRC_METASERVER_STORAGE_UTILS_H_
