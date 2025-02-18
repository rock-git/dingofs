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

#include <cstdint>
#include <string>
#include <utility>

#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mdsv2/coordinator/dummy_coordinator_client.h"
#include "mdsv2/filesystem/filesystem.h"
#include "mdsv2/storage/dummy_storage.h"

namespace dingofs {
namespace mdsv2 {
namespace unit_test {

const int64_t kFsTableId = 1234;
const int64_t kInodeTableId = 2345;

const uint64_t kRootIno = 1;

const int64_t kMdsId = 10000;

static pb::mdsv2::S3Info CreateS3Info() {
  pb::mdsv2::S3Info s3_info;
  s3_info.set_ak("ak");
  s3_info.set_sk("sk");
  s3_info.set_endpoint("http://s3.com");
  s3_info.set_bucketname("bucket");
  s3_info.set_block_size(1024 * 1024);
  s3_info.set_chunk_size(1024 * 1024);
  s3_info.set_object_prefix(1);

  return s3_info;
}

// test FileSystemSet
class FileSystemSetTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto coordinator_client = DummyCoordinatorClient::New();
    ASSERT_TRUE(coordinator_client->Init("")) << "init coordinator client fail.";

    auto fs_id_generator = AutoIncrementIdGenerator::New(coordinator_client, kFsTableId, 20000, 8);
    ASSERT_TRUE(fs_id_generator->Init()) << "init fs id generator fail.";

    auto kv_storage = DummyStorage::New();
    ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";

    MDSMeta mds_meta;
    mds_meta.SetID(kMdsId);
    mds_meta.SetHost("127.0.0.1");
    mds_meta.SetPort(6666);
    mds_meta.SetState(MDSMeta::State::kInit);

    auto mds_meta_map = MDSMetaMap::New();
    fs_set = FileSystemSet::New(coordinator_client, std::move(fs_id_generator), kv_storage, mds_meta, mds_meta_map);
    ASSERT_TRUE(fs_set->Init()) << "init fs set fail.";
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

 public:
  static FileSystemSetPtr fs_set;

  static FileSystemSetPtr FsSet() { return fs_set; }
};

FileSystemSetPtr FileSystemSetTest::fs_set = nullptr;

// test FileSystem
class FileSystemTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto coordinator_client = DummyCoordinatorClient::New();
    ASSERT_TRUE(coordinator_client->Init("")) << "init coordinator client fail.";

    auto fs_id_generator = AutoIncrementIdGenerator::New(coordinator_client, kInodeTableId, 1000000, 8);
    ASSERT_TRUE(fs_id_generator->Init()) << "init fs id generator fail.";

    auto kv_storage = DummyStorage::New();
    ASSERT_TRUE(kv_storage->Init("")) << "init kv storage fail.";

    pb::mdsv2::FsInfo fs_info;
    fs_info.set_fs_id(1);
    fs_info.set_fs_name("test_fs");
    fs_info.set_fs_type(pb::mdsv2::FsType::S3);
    fs_info.set_status(pb::mdsv2::FsStatus::NEW);
    fs_info.set_block_size(1024 * 1024);
    fs_info.set_enable_sum_in_dir(false);
    fs_info.set_owner("dengzh");
    fs_info.set_capacity(1024 * 1024 * 1024);
    fs_info.set_recycle_time_hour(24);
    *fs_info.mutable_extra()->mutable_s3_info() = CreateS3Info();

    fs = FileSystem::New(kMdsId, fs_info, std::move(fs_id_generator), kv_storage);
    auto status = fs->CreateRoot();
    ASSERT_TRUE(status.ok()) << "create root fail, error: " << status.error_str();
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

 public:
  static FileSystemPtr fs;

  static FileSystemPtr Fs() { return fs; }
};

FileSystemPtr FileSystemTest::fs = nullptr;

TEST_F(FileSystemSetTest, CreateFs) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_name = "test_fs_for_create";
  param.block_size = 1024 * 1024;
  param.fs_type = pb::mdsv2::FsType::S3;
  *param.fs_extra.mutable_s3_info() = CreateS3Info();
  param.enable_sum_in_dir = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  int64_t fs_id = 0;
  auto status = fs_set->CreateFs(param, fs_id);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  ASSERT_TRUE(fs_set->GetFileSystem(fs_id) != nullptr) << "get fs fail.";
}

TEST_F(FileSystemSetTest, GetFsInfo) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_name = "test_fs_for_get";
  param.block_size = 1024 * 1024;
  param.fs_type = pb::mdsv2::FsType::S3;
  *param.fs_extra.mutable_s3_info() = CreateS3Info();
  param.enable_sum_in_dir = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  int64_t fs_id = 0;
  auto status = fs_set->CreateFs(param, fs_id);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  pb::mdsv2::FsInfo fs_info;
  status = fs_set->GetFsInfo(param.fs_name, fs_info);
  ASSERT_TRUE(status.ok()) << "get fs info fail, error: " << status.error_str();
  ASSERT_EQ(fs_info.fs_id(), fs_id) << "fs id not equal.";
  ASSERT_EQ(fs_info.fs_name(), param.fs_name) << "fs name not equal.";
  ASSERT_EQ(fs_info.fs_type(), param.fs_type) << "fs type not equal.";
  ASSERT_EQ(fs_info.block_size(), param.block_size) << "block size not equal.";
  ASSERT_EQ(fs_info.enable_sum_in_dir(), param.enable_sum_in_dir) << "enable sum in dir not equal.";
  ASSERT_EQ(fs_info.owner(), param.owner) << "owner not equal.";
  ASSERT_EQ(fs_info.capacity(), param.capacity) << "capacity not equal.";
  ASSERT_EQ(fs_info.recycle_time_hour(), param.recycle_time_hour) << "recycle time hour not equal.";
}

TEST_F(FileSystemSetTest, DeleteFs) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_name = "test_fs_for_delete";
  param.block_size = 1024 * 1024;
  param.fs_type = pb::mdsv2::FsType::S3;
  *param.fs_extra.mutable_s3_info() = CreateS3Info();
  param.enable_sum_in_dir = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  int64_t fs_id = 0;
  auto status = fs_set->CreateFs(param, fs_id);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  status = fs_set->DeleteFs(param.fs_name);
  ASSERT_TRUE(status.ok()) << "delete fs fail, error: " << status.error_str();

  ASSERT_EQ(nullptr, fs_set->GetFileSystem(fs_id));

  pb::mdsv2::FsInfo fs_info;
  status = fs_set->GetFsInfo(param.fs_name, fs_info);
  ASSERT_TRUE(pb::error::ENOT_FOUND == status.error_code()) << "not should found fs, error: " << status.error_str();
}

TEST_F(FileSystemSetTest, MountFs) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_name = "test_fs_for_mount";
  param.block_size = 1024 * 1024;
  param.fs_type = pb::mdsv2::FsType::S3;
  *param.fs_extra.mutable_s3_info() = CreateS3Info();
  param.enable_sum_in_dir = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  int64_t fs_id = 0;
  auto status = fs_set->CreateFs(param, fs_id);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  pb::mdsv2::MountPoint mount_point;
  mount_point.set_hostname("localhost");
  mount_point.set_port(8080);
  mount_point.set_path("/mnt/dingofs");
  mount_point.set_cto(true);
  status = fs_set->MountFs(param.fs_name, mount_point);
  ASSERT_TRUE(status.ok()) << "mount fs fail, error: " << status.error_str();

  pb::mdsv2::FsInfo fs_info;
  status = fs_set->GetFsInfo(param.fs_name, fs_info);
  ASSERT_TRUE(status.ok()) << "get fs info fail, error: " << status.error_str();
  ASSERT_EQ(fs_info.fs_id(), fs_id) << "fs id not equal.";
  ASSERT_EQ(1, fs_info.mount_points_size()) << "mount point size not equal.";
  auto actual_mount_point = fs_info.mount_points(0);
  ASSERT_EQ(mount_point.hostname(), actual_mount_point.hostname()) << "hostname not equal.";
  ASSERT_EQ(mount_point.port(), actual_mount_point.port()) << "port not equal.";
  ASSERT_EQ(mount_point.path(), actual_mount_point.path()) << "path not equal.";
  ASSERT_EQ(mount_point.cto(), actual_mount_point.cto()) << "cto not equal.";
}

TEST_F(FileSystemSetTest, UnMountFs) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_name = "test_fs_for_unmount";
  param.block_size = 1024 * 1024;
  param.fs_type = pb::mdsv2::FsType::S3;
  *param.fs_extra.mutable_s3_info() = CreateS3Info();
  param.enable_sum_in_dir = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  int64_t fs_id = 0;
  auto status = fs_set->CreateFs(param, fs_id);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  pb::mdsv2::MountPoint mount_point;
  mount_point.set_hostname("localhost");
  mount_point.set_port(8080);
  mount_point.set_path("/mnt/dingofs");
  mount_point.set_cto(true);
  status = fs_set->MountFs(param.fs_name, mount_point);
  ASSERT_TRUE(status.ok()) << "mount fs fail, error: " << status.error_str();

  pb::mdsv2::FsInfo fs_info;
  status = fs_set->GetFsInfo(param.fs_name, fs_info);
  ASSERT_TRUE(status.ok()) << "get fs info fail, error: " << status.error_str();
  ASSERT_EQ(fs_info.fs_id(), fs_id) << "fs id not equal.";
  ASSERT_EQ(1, fs_info.mount_points_size()) << "mount point size not equal.";
  auto actual_mount_point = fs_info.mount_points(0);
  ASSERT_EQ(mount_point.hostname(), actual_mount_point.hostname()) << "hostname not equal.";
  ASSERT_EQ(mount_point.port(), actual_mount_point.port()) << "port not equal.";
  ASSERT_EQ(mount_point.path(), actual_mount_point.path()) << "path not equal.";
  ASSERT_EQ(mount_point.cto(), actual_mount_point.cto()) << "cto not equal.";

  status = fs_set->UmountFs(param.fs_name, mount_point);
  ASSERT_TRUE(status.ok()) << "unmount fs fail, error: " << status.error_str();

  {
    pb::mdsv2::FsInfo fs_info;
    status = fs_set->GetFsInfo(param.fs_name, fs_info);
    ASSERT_TRUE(status.ok()) << "get fs info fail, error: " << status.error_str();
    ASSERT_EQ(fs_info.fs_id(), fs_id) << "fs id not equal.";
    ASSERT_EQ(0, fs_info.mount_points_size()) << "mount point size not equal.";
  }
}

TEST_F(FileSystemTest, CreateRoot) {
  auto fs = Fs();

  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  auto partition = partition_cache.Get(kRootIno);
  ASSERT_TRUE(partition != nullptr);

  auto inode = inode_cache.GetInode(kRootIno);
  ASSERT_TRUE(inode != nullptr);
  ASSERT_EQ(kRootIno, inode->Ino());
  ASSERT_EQ(pb::mdsv2::FileType::DIRECTORY, inode->Type());
}

TEST_F(FileSystemTest, MkNod) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  FileSystem::MkNodParam param;
  param.parent_ino = kRootIno;
  param.name = "test_mknod";
  param.mode = 0777;
  param.uid = 1;
  param.gid = 3;
  param.rdev = 1;

  EntryOut entry_out;
  auto status = fs->MkNod(param, entry_out);
  ASSERT_TRUE(status.ok()) << "create file fail, error: " << status.error_str();
  ASSERT_GT(entry_out.inode.ino(), 0) << "ino is invalid.";

  auto partition = partition_cache.Get(param.parent_ino);
  ASSERT_TRUE(partition != nullptr) << "get partition fail.";

  Dentry dentry;
  ASSERT_TRUE(partition->GetChild(param.name, dentry)) << "get child fail.";
  ASSERT_EQ(param.name, dentry.Name()) << "dentry name not equal.";
  ASSERT_EQ(param.parent_ino, dentry.ParentIno()) << "dentry parent ino not equal.";
  ASSERT_TRUE(dentry.Inode() != nullptr) << "inode is nullptr.";

  InodePtr inode = inode_cache.GetInode(entry_out.inode.ino());
  ASSERT_TRUE(inode != nullptr) << "get inode fail.";
  ASSERT_EQ(param.mode, inode->Mode()) << "inode mode not equal.";
  ASSERT_EQ(param.uid, inode->Uid()) << "inode uid not equal.";
  ASSERT_EQ(param.gid, inode->Gid()) << "inode gid not equal.";
  ASSERT_EQ(pb::mdsv2::FileType::FILE, inode->Type()) << "inode type not equal.";
  ASSERT_EQ(0, inode->Length()) << "inode length not equal.";
  ASSERT_EQ(param.rdev, inode->Rdev()) << "inode rdev not equal.";
}

TEST_F(FileSystemTest, MkDir) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  FileSystem::MkDirParam param;
  param.parent_ino = kRootIno;
  param.name = "test_mkdir";
  param.mode = 0777;
  param.uid = 1;
  param.gid = 3;
  param.rdev = 0;

  EntryOut entry_out;
  auto status = fs->MkDir(param, entry_out);
  ASSERT_TRUE(status.ok()) << "create file fail, error: " << status.error_str();
  ASSERT_GT(entry_out.inode.ino(), 0) << "ino is invalid.";

  auto partition = partition_cache.Get(param.parent_ino);
  ASSERT_TRUE(partition != nullptr) << "get partition fail.";

  Dentry dentry;
  ASSERT_TRUE(partition->GetChild(param.name, dentry)) << "get child fail.";
  ASSERT_EQ(param.name, dentry.Name()) << "dentry name not equal.";
  ASSERT_EQ(param.parent_ino, dentry.ParentIno()) << "dentry parent ino not equal.";

  InodePtr inode = inode_cache.GetInode(entry_out.inode.ino());
  ASSERT_TRUE(status.ok()) << "get inode fail, error: " << status.error_str();
  ASSERT_TRUE(inode != nullptr) << "get inode fail.";
  ASSERT_EQ(param.mode, inode->Mode()) << "inode mode not equal.";
  ASSERT_EQ(param.uid, inode->Uid()) << "inode uid not equal.";
  ASSERT_EQ(param.gid, inode->Gid()) << "inode gid not equal.";
  ASSERT_EQ(pb::mdsv2::FileType::DIRECTORY, inode->Type()) << "inode type not equal.";
  ASSERT_EQ(4096, inode->Length()) << "inode length not equal.";
  ASSERT_EQ(param.rdev, inode->Rdev()) << "inode rdev not equal.";
}

TEST_F(FileSystemTest, RmDir) {
  auto fs = Fs();
  auto& partition_cache = fs->GetPartitionCache();
  auto& inode_cache = fs->GetInodeCache();

  FileSystem::MkDirParam param;
  param.parent_ino = kRootIno;
  param.name = "test_mkdir";
  param.mode = 0777;
  param.uid = 1;
  param.gid = 3;
  param.rdev = 0;

  EntryOut entry_out;
  auto status = fs->MkDir(param, entry_out);
  ASSERT_TRUE(status.ok()) << "create file fail, error: " << status.error_str();
  ASSERT_GT(entry_out.inode.ino(), 0) << "ino is invalid.";
  int64_t ino = entry_out.inode.ino();

  auto partition = partition_cache.Get(param.parent_ino);
  ASSERT_TRUE(partition != nullptr) << "get dentry fail.";

  Dentry dentry;
  ASSERT_TRUE(partition->GetChild(param.name, dentry)) << "get child fail.";
  ASSERT_EQ(param.name, dentry.Name()) << "dentry name not equal.";
  ASSERT_EQ(param.parent_ino, dentry.ParentIno()) << "dentry parent ino not equal.";

  InodePtr inode = inode_cache.GetInode(ino);
  ASSERT_TRUE(status.ok()) << "get inode fail, error: " << status.error_str();
  ASSERT_TRUE(inode != nullptr) << "get inode fail.";

  {
    status = fs->RmDir(param.parent_ino, param.name);
    ASSERT_TRUE(status.ok()) << "remove dir fail, error: " << status.error_str();

    auto partition = partition_cache.Get(ino);
    ASSERT_TRUE(partition == nullptr) << "get partition fail.";

    InodePtr inode = inode_cache.GetInode(ino);
    ASSERT_TRUE(inode == nullptr) << "get inode fail.";
  }
}

TEST_F(FileSystemTest, Link) {}

TEST_F(FileSystemTest, UnLink) {}

TEST_F(FileSystemTest, Symlink) {}

TEST_F(FileSystemTest, ReadLink) {}

TEST_F(FileSystemTest, GetDentry) {}

TEST_F(FileSystemTest, GetInode) {}

TEST_F(FileSystemTest, UpdateInode) {}

TEST_F(FileSystemTest, GetXAttr) {}

TEST_F(FileSystemTest, SetXAttr) {}

}  // namespace unit_test
}  // namespace mdsv2
}  // namespace dingofs