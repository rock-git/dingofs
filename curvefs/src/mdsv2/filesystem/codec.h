// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDV2_FILESYSTEM_CODEC_H_
#define DINGOFS_MDV2_FILESYSTEM_CODEC_H_

#include <cstdint>
#include <string>

namespace dingofs {
namespace mdsv2 {

class MetaDataCodec {
 public:
  static std::string EncodeFSKey(const std::string& name);
  static void DecodeFSKey(const std::string& key, std::string& name);

  static std::string EncodeDentryKey(int fs_id, uint64_t inode_id, const std::string& name);
  static void DecodeDentryKey(const std::string& key, int& fs_id, uint64_t& inode_id, std::string& name);

  static std::string EncodeDirInodeKey(int fs_id, uint64_t inode_id);
  static void DecodeDirInodeKey(const std::string& key, int& fs_id, uint64_t& inode_id);

  static std::string EncodeFileInodeKey(int fs_id, uint64_t inode_id);
  static void DecodeFileInodeKey(const std::string& key, int& fs_id, uint64_t& inode_id);
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_CODEC_H_