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

#include <csignal>
#include <iostream>
#include <string>

#include "backtrace.h"
#include "dlfcn.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "libunwind.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/version.h"
#include "mdsv2/server.h"

DEFINE_string(conf, "./conf/mdsv2.conf", "mdsv2 config path");
DEFINE_string(coor_url, "file://./conf/coor_list", "coor service url, e.g. file://<path> or list://<addr1>,<addr2>");

const int kMaxStacktraceSize = 128;

struct StackTraceInfo {
  char* filename{nullptr};
  int lineno{0};
  char* function{nullptr};
  uintptr_t pc{0};
};

// Passed to backtrace callback function.
struct BacktraceData {
  struct StackTraceInfo* stack_traces{nullptr};
  size_t index{0};
  size_t max{0};
  int fail{0};
};

int BacktraceCallback(void* vdata, uintptr_t pc, const char* filename, int lineno, const char* function) {
  struct BacktraceData* backtrace = (struct BacktraceData*)vdata;
  struct StackTraceInfo* stack_trace;

  if (backtrace->index >= backtrace->max) {
    std::cerr << "stack index beyond max.\n";
    backtrace->fail = 1;
    return 1;
  }

  stack_trace = &backtrace->stack_traces[backtrace->index];

  stack_trace->filename = (filename == nullptr) ? nullptr : strdup(filename);
  stack_trace->lineno = lineno;
  stack_trace->function = (function == nullptr) ? nullptr : strdup(function);
  stack_trace->pc = pc;

  ++backtrace->index;

  return 0;
}

// An error callback passed to backtrace.
void ErrorCallback(void* vdata, const char* msg, int errnum) {
  struct BacktraceData* data = (struct BacktraceData*)vdata;

  std::cerr << msg;
  if (errnum > 0) {
    std::cerr << ": " << strerror(errnum) << "\n";
  }
  data->fail = 1;
}

// The signal handler
static void SignalHandler(int signo) {
  if (signo == SIGTERM) {
    dingofs::mdsv2::Server& server = dingofs::mdsv2::Server::GetInstance();
    server.Stop();

    _exit(0);
  }

  std::cerr << "received signal: " << signo << '\n';
  std::cerr << "stack trace:\n";
  DINGO_LOG(ERROR) << "received signal " << signo;
  DINGO_LOG(ERROR) << "stack trace:";

  struct backtrace_state* state = backtrace_create_state(nullptr, 0, ErrorCallback, nullptr);
  if (state == nullptr) {
    std::cerr << "state is null.\n";
    _exit(1);
  }

  struct StackTraceInfo stack_traces[kMaxStacktraceSize];
  struct BacktraceData data;

  data.stack_traces = &stack_traces[0];
  data.index = 0;
  data.max = kMaxStacktraceSize;
  data.fail = 0;

  if (backtrace_full(state, 0, BacktraceCallback, ErrorCallback, &data) != 0) {
    std::cerr << "backtrace_full fail." << '\n';
    DINGO_LOG(ERROR) << "backtrace_full fail.";
  }

  for (size_t i = 0; i < data.index; ++i) {
    auto& stack_trace = stack_traces[i];
    int status;
    char* nameptr = stack_trace.function;
    char* demangled = abi::__cxa_demangle(stack_trace.function, nullptr, nullptr, &status);
    if (status == 0 && demangled) {
      nameptr = demangled;
    }

    Dl_info info = {};

    std::string error_msg;
    if (!dladdr((void*)stack_trace.pc, &info)) {
      error_msg = butil::string_printf("#%zu source[%s:%d] symbol[%s] pc[0x%0lx]", i, stack_trace.filename,
                                       stack_trace.lineno, nameptr, static_cast<uint64_t>(stack_trace.pc));

    } else {
      error_msg = butil::string_printf(
          "#%zu source[%s:%d] symbol[%s] pc[0x%0lx] fname[%s] fbase[0x%lx] sname[%s] saddr[0x%lx] ", i,
          stack_trace.filename, stack_trace.lineno, nameptr, static_cast<uint64_t>(stack_trace.pc), info.dli_fname,
          (uint64_t)info.dli_fbase, info.dli_sname, (uint64_t)info.dli_saddr);
    }

    DINGO_LOG(ERROR) << error_msg;
    std::cerr << error_msg << '\n';

    if (demangled) {
      free(demangled);
    }
  }

  // call abort() to generate core dump
  if (signal(SIGABRT, SIG_DFL) == SIG_ERR) {
    std::cerr << "setup SIGABRT signal to SIG_DFL fail.\n";
  }

  abort();
}

void SetupSignalHandler() {
  sighandler_t s;
  s = signal(SIGTERM, SignalHandler);
  if (s == SIG_ERR) {
    std::cout << "setup SIGTERM signal fail.\n";
    exit(-1);
  }

  s = signal(SIGSEGV, SignalHandler);
  if (s == SIG_ERR) {
    std::cout << "setup SIGSEGV signal fail.\n";
    exit(-1);
  }

  s = signal(SIGFPE, SignalHandler);
  if (s == SIG_ERR) {
    std::cout << "setup SIGFPE signal fail.\n";
    exit(-1);
  }

  s = signal(SIGBUS, SignalHandler);
  if (s == SIG_ERR) {
    std::cout << "setup SIGBUS signal fail.\n";
    exit(-1);
  }

  s = signal(SIGILL, SignalHandler);
  if (s == SIG_ERR) {
    std::cout << "setup SIGILL signal fail.\n";
    exit(-1);
  }

  s = signal(SIGABRT, SignalHandler);
  if (s == SIG_ERR) {
    std::cout << "setup SIGABRT signal fail.\n";
    exit(-1);
  }

  // ignore SIGPIPE
  s = signal(SIGPIPE, SIG_IGN);
  if (s == SIG_ERR) {
    std::cout << "setup SIGPIPE signal fail.\n";
    exit(-1);
  }
}

bool GeneratePidFile(const std::string& filepath) {
  int64_t pid = dingofs::mdsv2::Helper::GetPid();
  if (pid <= 0) {
    DINGO_LOG(ERROR) << "get pid fail.";
    return false;
  }

  DINGO_LOG(INFO) << "pid file: " << filepath;

  return dingofs::mdsv2::Helper::SaveFile(filepath, std::to_string(pid));
}

int main(int argc, char* argv[]) {
  if (dingofs::mdsv2::Helper::IsExistPath("conf/gflags.conf")) {
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
  }

  google::ParseCommandLineFlags(&argc, &argv, true);

  if (dingofs::mdsv2::FLAGS_show_version) {
    dingofs::mdsv2::DingoShowVerion();

    printf("Usage: %s --conf ./conf/mdsv2.conf --coor_url=[file://./conf/coor_list]\n", argv[0]);
    printf("Example: \n");
    printf("         bin/dingofs_mdsv2\n");
    printf(
        "         bin/dingofs_mdsv2 --conf ./conf/mdsv2.yaml "
        "--coor_url=file://./conf/coor_list\n");
    exit(-1);
  }

  SetupSignalHandler();

  dingofs::mdsv2::Server& server = dingofs::mdsv2::Server::GetInstance();

  CHECK(server.InitConfig(FLAGS_conf)) << fmt::format("init config({}) error.", FLAGS_conf);
  CHECK(server.InitLog()) << "init log error.";
  CHECK(GeneratePidFile(server.GetPidFilePath())) << "generate pid file error.";
  CHECK(server.InitMDSMeta()) << "init mds meta error.";
  CHECK(server.InitCoordinatorClient(FLAGS_coor_url)) << "init coordinator client error.";
  CHECK(server.InitStorage(FLAGS_coor_url)) << "init storage error.";
  CHECK(server.InitQuotaProcessor()) << "init quota processor error.";
  CHECK(server.InitRenamer()) << "init renamer error.";
  CHECK(server.InitMutationMerger()) << "init mutation merger error.";
  CHECK(server.InitFileSystem()) << "init file system set error.";
  CHECK(server.InitWorkerSet()) << "init worker set error.";
  CHECK(server.InitHeartbeat()) << "init heartbeat error.";
  CHECK(server.InitFsInfoSync()) << "init fs info sync error.";
  CHECK(server.InitMDSMonitor()) << "init mds monitor error.";
  CHECK(server.InitCompactChunkProcessor()) << "init compact chunk processor error.";
  CHECK(server.InitCrontab()) << "init crontab error.";

  DINGO_LOG(INFO) << "##################### init finish ######################";

  server.Run();

  server.Stop();

  return 0;
}