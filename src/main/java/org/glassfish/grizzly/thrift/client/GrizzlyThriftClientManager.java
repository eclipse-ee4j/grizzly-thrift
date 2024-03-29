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

package org.glassfish.grizzly.thrift.client;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.glassfish.grizzly.Grizzly;
import org.glassfish.grizzly.IOStrategy;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.glassfish.grizzly.strategies.SameThreadIOStrategy;
import org.glassfish.grizzly.thrift.ThriftClientFilter;
import org.glassfish.grizzly.thrift.ThriftFrameFilter;
import org.glassfish.grizzly.thrift.client.zookeeper.ZKClient;
import org.glassfish.grizzly.thrift.client.zookeeper.ZooKeeperConfig;

/**
 * The implementation of the {@link ThriftClientManager} based on Grizzly
 * <p>
 * This thrift client manager has a key(String thrift client
 * name)/value({@link GrizzlyThriftClient} map for retrieving thrift clients. If
 * the specific {@link org.glassfish.grizzly.nio.transport.TCPNIOTransport
 * GrizzlyTransport} is not set at creation time, this will create a main
 * GrizzlyTransport. The
 * {@link org.glassfish.grizzly.nio.transport.TCPNIOTransport GrizzlyTransport}
 * must contain {@link org.glassfish.grizzly.thrift.ThriftFrameFilter} and
 * {@link org.glassfish.grizzly.thrift.ThriftClientFilter}.
 *
 * @author Bongjae Chang
 */
public class GrizzlyThriftClientManager implements ThriftClientManager {

    private static final Logger logger = Grizzly.logger(GrizzlyThriftClientManager.class);

    private final ConcurrentHashMap<String, GrizzlyThriftClient<?>> thriftClients = new ConcurrentHashMap<String, GrizzlyThriftClient<?>>();
    private final TCPNIOTransport transport;
    private final boolean isExternalTransport;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private ZKClient zkClient;
    private final int maxThriftFrameLength;

    private GrizzlyThriftClientManager(final Builder builder) {
        this.maxThriftFrameLength = builder.maxThriftFrameLength;
        TCPNIOTransport transportLocal = builder.transport;
        if (transportLocal == null) {
            isExternalTransport = false;
            final FilterChainBuilder clientFilterChainBuilder = FilterChainBuilder.stateless();
            clientFilterChainBuilder.add(new TransportFilter()).add(new ThriftFrameFilter(builder.maxThriftFrameLength))
                    .add(new ThriftClientFilter());
            final TCPNIOTransportBuilder clientTCPNIOTransportBuilder = TCPNIOTransportBuilder.newInstance();
            transportLocal = clientTCPNIOTransportBuilder.build();
            transportLocal.setProcessor(clientFilterChainBuilder.build());
            transportLocal.setSelectorRunnersCount(builder.selectorRunnersCount);
            transportLocal.setIOStrategy(builder.ioStrategy);
            transportLocal.configureBlocking(builder.blocking);
            if (builder.workerThreadPool != null) {
                transportLocal.setWorkerThreadPool(builder.workerThreadPool);
            }
            try {
                transportLocal.start();
            } catch (IOException ie) {
                if (logger.isLoggable(Level.SEVERE)) {
                    logger.log(Level.SEVERE, "failed to start the transport", ie);
                }
            }
        } else {
            isExternalTransport = true;
        }
        this.transport = transportLocal;
        if (builder.zooKeeperConfig != null) {
            final ZKClient.Builder zkBuilder = new ZKClient.Builder(builder.zooKeeperConfig.getName(),
                    builder.zooKeeperConfig.getZooKeeperServerList());
            zkBuilder.rootPath(builder.zooKeeperConfig.getRootPath());
            zkBuilder.connectTimeoutInMillis(builder.zooKeeperConfig.getConnectTimeoutInMillis());
            zkBuilder.sessionTimeoutInMillis(builder.zooKeeperConfig.getSessionTimeoutInMillis());
            zkBuilder.commitDelayTimeInSecs(builder.zooKeeperConfig.getCommitDelayTimeInSecs());
            this.zkClient = zkBuilder.build();
            try {
                this.zkClient.connect();
            } catch (IOException ie) {
                if (logger.isLoggable(Level.SEVERE)) {
                    logger.log(Level.SEVERE, "failed to connect the zookeeper server. zkClient=" + this.zkClient, ie);
                }
                this.zkClient = null;
            } catch (InterruptedException ie) {
                if (logger.isLoggable(Level.SEVERE)) {
                    logger.log(Level.SEVERE, "failed to connect the zookeeper server. zkClient=" + this.zkClient, ie);
                }
                Thread.currentThread().interrupt();
                this.zkClient = null;
            }
        } else {
            this.zkClient = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T extends TServiceClient> GrizzlyThriftClient.Builder<T> createThriftClientBuilder(final String thriftClientName,
            final TServiceClientFactory<T> thriftClientFactory) {
        return new GrizzlyThriftClient.Builder<T>(thriftClientName, this, transport, thriftClientFactory);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T extends TServiceClient> GrizzlyThriftClient<T> getThriftClient(final String thriftClientName) {
        if (shutdown.get()) {
            return null;
        }
        return thriftClientName != null ? (GrizzlyThriftClient<T>) thriftClients.get(thriftClientName) : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeThriftClient(final String thriftClientName) {
        if (shutdown.get()) {
            return false;
        }
        if (thriftClientName == null)
            return false;
        final GrizzlyThriftClient thriftClient = thriftClients.remove(thriftClientName);
        if (thriftClient == null) {
            return false;
        }
        thriftClient.stop();
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }
        for (ThriftClient thriftClient : thriftClients.values()) {
            thriftClient.stop();
        }
        thriftClients.clear();
        if (!isExternalTransport && transport != null) {
            try {
                transport.shutdownNow();
            } catch (IOException ie) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.log(Level.INFO, "failed to stop the transport", ie);
                }
            }
        }
        if (zkClient != null) {
            zkClient.shutdown();
        }
    }

    /**
     * Add the given {@code thriftClient} to this thrift client manager
     * <p/>
     * If this returns false, the given {@code thriftClient} should be stopped by
     * caller. Currently, this method is called by only
     * {@link org.glassfish.grizzly.thrift.client.GrizzlyThriftClient.Builder#build()}.
     *
     * @param thriftClient a thrift client instance
     * @return true if the thrift client was added
     */
    <T extends TServiceClient> boolean addThriftClient(final GrizzlyThriftClient<T> thriftClient) {
        return !shutdown.get() && thriftClient != null && thriftClients.putIfAbsent(thriftClient.getName(), thriftClient) == null
                && !(shutdown.get() && thriftClients.remove(thriftClient.getName()) == thriftClient);
    }

    ZKClient getZkClient() {
        return zkClient;
    }

    int getMaxThriftFrameLength() {
        return maxThriftFrameLength;
    }

    public static class Builder {

        private TCPNIOTransport transport;

        // grizzly config
        private int selectorRunnersCount = Runtime.getRuntime().availableProcessors() * 2;
        private IOStrategy ioStrategy = SameThreadIOStrategy.getInstance();
        private boolean blocking = false;
        private ExecutorService workerThreadPool;
        private int maxThriftFrameLength = 1024 * 1024; // 1M

        // zookeeper config
        private ZooKeeperConfig zooKeeperConfig;

        /**
         * Set the specific {@link org.glassfish.grizzly.nio.transport.TCPNIOTransport
         * GrizzlyTransport}
         * <p>
         * If this is not set or set to be null,
         * {@link org.glassfish.grizzly.thrift.client.GrizzlyThriftClientManager} will
         * create a default transport. The given {@code transport} must be always
         * started state if it is not null. Default is null.
         *
         * @param transport the specific Grizzly's
         * {@link org.glassfish.grizzly.nio.transport.TCPNIOTransport}
         * @return this builder
         */
        public Builder transport(final TCPNIOTransport transport) {
            this.transport = transport;
            return this;
        }

        /**
         * Set selector threads' count
         * <p>
         * If this thrift client manager will create a default transport, the given
         * selector counts will be passed to
         * {@link org.glassfish.grizzly.nio.transport.TCPNIOTransport}. Default is
         * processors' count * 2.
         *
         * @param selectorRunnersCount selector threads' count
         * @return this builder
         */
        public Builder selectorRunnersCount(final int selectorRunnersCount) {
            this.selectorRunnersCount = selectorRunnersCount;
            return this;
        }

        /**
         * Set the specific IO Strategy of Grizzly
         * <p>
         * If this thrift client manager will create a default transport, the given
         * {@link org.glassfish.grizzly.IOStrategy} will be passed to
         * {@link org.glassfish.grizzly.nio.transport.TCPNIOTransport}. Default is
         * {@link org.glassfish.grizzly.strategies.SameThreadIOStrategy}.
         *
         * @param ioStrategy the specific IO Strategy
         * @return this builder
         */
        public Builder ioStrategy(final IOStrategy ioStrategy) {
            this.ioStrategy = ioStrategy;
            return this;
        }

        /**
         * Enable or disable the blocking mode
         * <p>
         * If this thrift client manager will create a default transport, the given mode
         * will be passed to
         * {@link org.glassfish.grizzly.nio.transport.TCPNIOTransport}. Default is
         * false.
         *
         * @param blocking true means the blocking mode
         * @return this builder
         */
        public Builder blocking(final boolean blocking) {
            this.blocking = blocking;
            return this;
        }

        /**
         * Set the specific worker thread pool
         * <p>
         * If this thrift client manager will create a default transport, the given
         * {@link java.util.concurrent.ExecutorService} will be passed to
         * {@link org.glassfish.grizzly.nio.transport.TCPNIOTransport}. This is only
         * effective if {@link org.glassfish.grizzly.IOStrategy} is not
         * {@link org.glassfish.grizzly.strategies.SameThreadIOStrategy}. Default is
         * null.
         *
         * @param workerThreadPool worker thread pool
         * @return this builder
         */
        public Builder workerThreadPool(final ExecutorService workerThreadPool) {
            this.workerThreadPool = workerThreadPool;
            return this;
        }

        /**
         * Set the {@link ZooKeeperConfig} for synchronizing thrift server list among
         * thrift clients
         *
         * @param zooKeeperConfig zookeeper config. if {@code zooKeeperConfig} is null,
         * the zookeeper is never used.
         * @return this builder
         */
        public Builder zooKeeperConfig(final ZooKeeperConfig zooKeeperConfig) {
            this.zooKeeperConfig = zooKeeperConfig;
            return this;
        }

        /**
         * Set the max length of thrift frame
         *
         * @param maxThriftFrameLength max frame length
         * @return this builder
         */
        public Builder maxThriftFrameLength(final int maxThriftFrameLength) {
            this.maxThriftFrameLength = maxThriftFrameLength;
            return this;
        }

        /**
         * Create a
         * {@link org.glassfish.grizzly.thrift.client.GrizzlyThriftClientManager}
         * instance with this builder's properties
         *
         * @return a thrift client manager
         */
        public GrizzlyThriftClientManager build() {
            return new GrizzlyThriftClientManager(this);
        }
    }
}
