# Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(dummy)
add_subdirectory(v1)
add_subdirectory(v2)

add_library(vfs_meta_system 
    meta_log.cpp
    meta_wrapper.cpp
)

target_link_libraries(vfs_meta_system
    vfs_meta_dummy_lib
    vfs_meta_v2_lib
    absl::str_format
)