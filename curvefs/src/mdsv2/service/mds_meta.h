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

#ifndef DINGOFS_MDSV2_SERVICE_MDS_META_H_
#define DINGOFS_MDSV2_SERVICE_MDS_META_H_

#include <cstdint>
#include <string>

namespace dingofs {

namespace mdsv2 {

class MDSMeta {
 public:
  MDSMeta() = default;
  ~MDSMeta() = default;

  enum State {
    kInit = 0,
    kNormal = 1,
    kAbnormal = 2,
  };

  int64_t ID() const { return id_; }
  void SetID(int64_t id) { id_ = id; }

  std::string Host() const { return host_; }
  void SetHost(const std::string& host) { host_ = host; }

  int Port() const { return port_; }
  void SetPort(int port) { port_ = port; }

  State GetState() const { return state_; }
  void SetState(State state) { state_ = state; }

  uint64_t RegisterTimeMs() const { return register_time_ms_; }
  void SetRegisterTimeMs(uint64_t time_ms) { register_time_ms_ = time_ms; }
  uint64_t LastOnlineTimeMs() const { return last_online_time_ms_; }
  void SetLastOnlineTimeMs(uint64_t time_ms) { last_online_time_ms_ = time_ms; }

  std::string ToString() const;

 private:
  int64_t id_{0};

  std::string host_;
  int port_{0};

  State state_;

  uint64_t register_time_ms_;
  uint64_t last_online_time_ms_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_SERVICE_MDS_META_H_