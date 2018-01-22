/*
 * Copyright (c) 2012, 2017 Oracle and/or its affiliates. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v. 2.0, which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the
 * Eclipse Public License v. 2.0 are satisfied: GNU General Public License,
 * version 2 with the GNU Classpath Exception, which is available at
 * https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 */

package org.glassfish.grizzly.thrift.client.zookeeper;

/**
 * The interface using the ZooKeeper for synchronizing thrift server list
 * <p>
 * Example of use:
 * {@code
 * final GrizzlyThriftClientManager.Builder managerBuilder = new GrizzlyThriftClientManager.Builder();
 * // setup zookeeper server
 * final ZooKeeperConfig zkConfig = ZooKeeperConfig.create("thrift-client-manager", DEFAULT_ZOOKEEPER_ADDRESS);
 * zkConfig.setRootPath(ROOT);
 * zkConfig.setConnectTimeoutInMillis(3000);
 * zkConfig.setSessionTimeoutInMillis(30000);
 * zkConfig.setCommitDelayTimeInSecs(2);
 * managerBuilder.zooKeeperConfig(zkConfig);
 * // create a thrift client manager
 * final GrizzlyThriftClientManager manager = managerBuilder.build();
 * final GrizzlyThriftClient.Builder<String, String> thriftClientBuilder = manager.createThriftClientBuilder("user");
 * // setup thrift servers
 * final Set<SocketAddress> thriftServers = new HashSet<SocketAddress>();
 * thriftServers.add(THRIFT_SERVER_ADDRESS1);
 * thriftServers.add(THRIFT_SERVER_ADDRESS2);
 * thriftClientBuilder.servers(thriftServers);
 * // create a user thrift
 * final GrizzlyThriftClient<String, String> thriftClient = thriftClientBuilder.build();
 * // ZooKeeperSupportThriftClient's basic operations
 * if (thriftClient.isZooKeeperSupported()) {
 * final String serverListPath = thriftClient.getZooKeeperServerListPath();
 * final String serverList = thriftClient.getCurrentServerListFromZooKeeper();
 * thriftClient.setCurrentServerListOfZooKeeper("localhost:11211,localhost:11212");
 * }
 * // ...
 * // clean
 * manager.removeThriftClient("user");
 * manager.shutdown();
 * }
 *
 * @author Bongjae Chang
 */
public interface ZooKeeperSupportThriftClient {

    /**
     * Check if this thrift client supports the ZooKeeper for synchronizing the thrift server list
     *
     * @return true if this thrift client supports it
     */
    public boolean isZooKeeperSupported();

    /**
     * Return the path of the thrift server list which has been registered in the ZooKeeper server
     *
     * @return the path of the thrift server list in the ZooKeeper server.
     *         "null" means this thrift client doesn't support the ZooKeeper or this thrift client is not started yet
     */
    public String getZooKeeperServerListPath();

    /**
     * Return the current thrift server list string from the ZooKeeper server
     *
     * @return the current server list string
     */
    public String getCurrentServerListFromZooKeeper();

    /**
     * Set the current thrift server list string with the given {@code thriftServerList}
     * <p>
     * {@code thriftServerList} could be comma separated host:port pairs, each corresponding to a thrift server.
     * e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
     * Be careful that this operation will propagate {@code thriftServerList} to thrift clients which has joinned the same thrift client name(scope)
     * because the thrift server list of ZooKeeper server will be changed.
     *
     * @param thriftServerList the thrift server list string
     * @return true if this thrift server list is set successfully
     */
    public boolean setCurrentServerListOfZooKeeper(final String thriftServerList);

    /**
     * Add the custom {@link BarrierListener}
     *
     * The given {@code listener} will be called after thrift client's default listener will be completed.
     * {@link BarrierListener#onInit} will be called when this thrift client will be registered in the ZooKeeper.
     * {@link BarrierListener#onCommit} will be called when this thrift client's server list will be changed in the ZooKeeper.
     * {@link BarrierListener#onDestroy} will be called when this thrift client will be unregistered in the ZooKeeper.
     *
     * @param listener the custom listener
     */
    public void addZooKeeperListener(final BarrierListener listener);

    /**
     * Remove the custom {@link BarrierListener}
     *
     * The given {@code listener} will be called after thrift client's default listener will be completed.
     * {@link BarrierListener#onInit} will be called when this thrift client will be registered in the ZooKeeper.
     * {@link BarrierListener#onCommit} will be called when this thrift client's server list will be changed in the ZooKeeper.
     * {@link BarrierListener#onDestroy} will be called when this thrift client will be unregistered in the ZooKeeper.
     *
     * @param listener the custom listener which was given by {@link #addZooKeeperListener}
     */
    public void removeZooKeeperListener(final BarrierListener listener);
}
