/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/CheckpointStoreFactory.h"

#include "logdevice/common/FileBasedVersionedConfigStore.h"
#include "logdevice/common/RSMBasedVersionedConfigStore.h"
#include "logdevice/common/RqliteVersionedConfigStore.h"
#include "logdevice/common/VersionedConfigStore.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/checkpointing/CheckpointStoreImpl.h"

namespace facebook { namespace logdevice {

std::unique_ptr<CheckpointStore>
CheckpointStoreFactory::createFileBasedCheckpointStore(std::string root_path) {
  auto versioned_config_store = std::make_unique<FileBasedVersionedConfigStore>(
      root_path, CheckpointStoreImpl::extractVersion);
  return std::make_unique<CheckpointStoreImpl>(
      std::move(versioned_config_store));
}

std::shared_ptr<CheckpointStore>
CheckpointStoreFactory::createSharedFileBasedCheckpointStore(
    std::string root_path) {
  auto versioned_config_store = std::make_unique<FileBasedVersionedConfigStore>(
      root_path, CheckpointStoreImpl::extractVersion);
  return std::make_shared<CheckpointStoreImpl>(
      std::move(versioned_config_store));
}

std::unique_ptr<CheckpointStore>
CheckpointStoreFactory::createRqliteBasedCheckpointStore(
    std::shared_ptr<Client>& client) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(client.get());
  ld_check(client_impl);

  auto rqlite_config = client_impl->getConfig()->getRqliteConfig();
  auto rqlite_client =
      std::make_unique<RqliteClient>(rqlite_config->getRqliteUri());
  if (!rqlite_client) {
    ld_error("Failed to create a Rqlite client in CheckpointStoreFactory");
    return nullptr;
  }
  auto versioned_config_store = std::make_unique<RqliteVersionedConfigStore>(
      CheckpointStoreImpl::extractVersion, std::move(rqlite_client));

  std::string prefix = folly::sformat(
      "logdevice-{}-checkpoints",
      client_impl->getConfig()->getServerConfig()->getClusterName());
  return std::make_unique<CheckpointStoreImpl>(
      std::move(versioned_config_store), std::move(prefix));
}

std::shared_ptr<CheckpointStore>
CheckpointStoreFactory::createSharedRqliteBasedCheckpointStore(
    std::shared_ptr<Client>& client) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(client.get());
  ld_check(client_impl);

  auto rqlite_config = client_impl->getConfig()->getRqliteConfig();
  auto rqlite_client =
      std::make_unique<RqliteClient>(rqlite_config->getRqliteUri());
  if (!rqlite_client) {
    ld_error("Failed to create a Rqlite client in CheckpointStoreFactory");
    return nullptr;
  }
  auto versioned_config_store = std::make_unique<RqliteVersionedConfigStore>(
      CheckpointStoreImpl::extractVersion, std::move(rqlite_client));

  std::string prefix = folly::sformat(
      "logdevice-{}-checkpoints",
      client_impl->getConfig()->getServerConfig()->getClusterName());
  return std::make_shared<CheckpointStoreImpl>(
      std::move(versioned_config_store), std::move(prefix));
}

std::unique_ptr<CheckpointStore>
CheckpointStoreFactory::createRSMBasedCheckpointStore(
    std::shared_ptr<Client>& client,
    logid_t log_id,
    std::chrono::milliseconds stop_timeout) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(client.get());
  ld_check(client_impl);
  auto versioned_config_store = std::make_unique<RSMBasedVersionedConfigStore>(
      log_id,
      CheckpointStoreImpl::extractVersion,
      &client_impl->getProcessor(),
      stop_timeout);
  return std::make_unique<CheckpointStoreImpl>(
      std::move(versioned_config_store));
}

std::shared_ptr<CheckpointStore>
CheckpointStoreFactory::createSharedRSMBasedCheckpointStore(
    std::shared_ptr<Client>& client,
    logid_t log_id,
    std::chrono::milliseconds stop_timeout) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(client.get());
  ld_check(client_impl);
  auto versioned_config_store = std::make_unique<RSMBasedVersionedConfigStore>(
      log_id,
      CheckpointStoreImpl::extractVersion,
      &client_impl->getProcessor(),
      stop_timeout);
  return std::make_shared<CheckpointStoreImpl>(
      std::move(versioned_config_store));
}

}} // namespace facebook::logdevice
