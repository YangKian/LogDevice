/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/AdminAPIUtils.h"

#include <gtest/gtest.h>

using namespace ::testing;
using namespace apache::thrift;
using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration::nodes;

TEST(AdminAPIUtilsTest, TestNodeMatchesID) {
  auto sd = NodeServiceDiscovery{"server-1",
                                 Sockaddr("127.0.0.1", 4440),
                                 Sockaddr("127.0.0.1", 4441),
                                 /*ssl address*/ folly::none,
                                 /* location */ folly::none,
                                 /* roles */ 0};
  {
    // Simple match by name
    thrift::NodeID id;
    id.set_name("server-1");
    EXPECT_TRUE(nodeMatchesID(node_index_t{0}, sd, id));

    id.set_name("server-2");
    EXPECT_FALSE(nodeMatchesID(node_index_t{0}, sd, id));
  }

  {
    // Simple match by index
    thrift::NodeID id;
    id.set_node_index(node_index_t{12});
    EXPECT_TRUE(nodeMatchesID(node_index_t{12}, sd, id));

    id.set_node_index(node_index_t{2});
    EXPECT_FALSE(nodeMatchesID(node_index_t{12}, sd, id));
  }

  {
    // Simple match by address
    thrift::SocketAddress address;
    address.set_address("127.0.0.1");
    address.set_port(4440);

    thrift::NodeID id;
    id.set_address(address);
    EXPECT_TRUE(nodeMatchesID(node_index_t{12}, sd, id));

    address.set_port(4441);
    id.set_address(address);
    EXPECT_FALSE(nodeMatchesID(node_index_t{12}, sd, id));
  }

  {
    // Match by the name AND index
    thrift::NodeID id;
    id.set_name("server-1");
    id.set_node_index(node_index_t{12});
    EXPECT_TRUE(nodeMatchesID(node_index_t{12}, sd, id));

    // Make sure it's an AND
    id.set_name("server-2");
    EXPECT_FALSE(nodeMatchesID(node_index_t{12}, sd, id));
  }

  {
    // Emtpy ID matches everything
    thrift::NodeID id;
    EXPECT_TRUE(nodeMatchesID(node_index_t{12}, sd, id));
  }
}
