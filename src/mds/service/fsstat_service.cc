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

#include "mds/service/fsstat_service.h"

#include <json/value.h>
#include <sys/types.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/http_status_code.h"
#include "brpc/server.h"
#include "butil/iobuf.h"
#include "common/logging.h"
#include "common/version.h"
#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "json/writer.h"
#include "mds/common/codec.h"
#include "mds/common/context.h"
#include "mds/common/helper.h"
#include "mds/common/type.h"
#include "mds/filesystem/filesystem.h"
#include "mds/filesystem/fs_utils.h"
#include "mds/server.h"
#include "mds/service/fsstat_api.h"
#include "mds/service/fsstat_assets.h"
#include "mds/service/fsstat_diagnostics_api.h"
#include "mds/service/fsstat_remaining_api.h"
#include "mds/storage/dingodb_storage.h"
#include "utils/uuid.h"

namespace dingofs {
namespace mds {

DECLARE_uint32(mds_heartbeat_mds_offline_period_time_ms);
DECLARE_uint32(mds_heartbeat_client_offline_period_ms);
DECLARE_uint32(cache_member_heartbeat_offline_timeout_s);
DECLARE_uint32(cache_member_heartbeat_miss_timeout_s);

DECLARE_string(mds_storage_engine);

void FsStatServiceImpl::default_method(::google::protobuf::RpcController* controller, const pb::web::FsStatRequest*,
                                       pb::web::FsStatResponse*, ::google::protobuf::Closure* done) {
  brpc::ClosureGuard const done_guard(done);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  const brpc::Server* server = cntl->server();
  butil::IOBufBuilder os;
  const bool use_html = brpc::UseHTML(cntl->http_request());
  cntl->http_response().set_content_type(use_html ? "text/html" : "text/plain");
  const std::string& path = cntl->http_request().unresolved_path();

  LOG(INFO) << fmt::format("FsStatService path: {}", path);

  std::vector<std::string> params;
  Helper::SplitString(path, '/', params);

  // The React console and its versioned Management API are handled before the
  // legacy renderer. Only explicit browser routes are accepted here; unknown
  // paths continue to the legacy dispatcher and eventually return 404.
  if (HandleFsStatRemainingApi(cntl, params, os) || HandleFsStatDiagnosticsApi(cntl, params, os) ||
      HandleFsStatApi(cntl, params, os) || ServeFsStatConsole(cntl, path, os)) {
    os.move_to(cntl->response_attachment());
    cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
    return;
  }

  if (!HandleLegacy(server, cntl, path, params, os)) {
    cntl->SetFailed("unknown path: " + path);
    cntl->http_response().set_status_code(brpc::HTTP_STATUS_NOT_FOUND);
  }

  os.move_to(cntl->response_attachment());
  cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
}

void FsStatServiceImpl::GetTabInfo(brpc::TabInfoList* tab_list) const {
  brpc::TabInfo* tab = tab_list->add();
  tab->tab_name = "dingofs";
  tab->path = "/FsStatService";
}

}  // namespace mds
}  // namespace dingofs
