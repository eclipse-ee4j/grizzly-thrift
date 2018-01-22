/*
 * Copyright (c) 2011, 2017 Oracle and/or its affiliates. All rights reserved.
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

package org.glassfish.grizzly.thrift;

import org.apache.thrift.TServiceClient;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.Grizzly;
import org.glassfish.grizzly.attributes.Attribute;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.memory.Buffers;
import org.glassfish.grizzly.thrift.client.GrizzlyThriftClient;
import org.glassfish.grizzly.thrift.client.pool.ObjectPool;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ThriftClientFilter is a client-side filter for Thrift RPC processors.
 * <p>
 * Read-messages will be queued in LinkedBlockingQueue from which TGrizzlyClientTransport will read it.
 * <p>
 * Usages:
 * <pre>
 * {@code
 * final FilterChainBuilder clientFilterChainBuilder = FilterChainBuilder.stateless();
 * clientFilterChainBuilder.add(new TransportFilter()).add(new ThriftFrameFilter()).add(new ThriftClientFilter());
 * <p>
 * final TCPNIOTransport transport = TCPNIOTransportBuilder.newInstance().build();
 * transport.setProcessor(clientFilterChainBuilder.build());
 * transport.start();
 * Future<Connection> future = transport.connect(ip, port);
 * final Connection connection = future.get(10, TimeUnit.SECONDS);
 * <p>
 * final TTransport ttransport = TGrizzlyClientTransport.create(connection);
 * final TProtocol tprotocol = new TBinaryProtocol(ttransport);
 * user-generated.thrift.Client client = new user-generated.thrift.Client(tprotocol);
 * client.ping();
 * // execute more works
 * // ...
 * // release
 * ttransport.close();
 * connection.close();
 * transport.shutdownNow();
 * }
 * </pre>
 *
 * @author Bongjae Chang
 */
public class ThriftClientFilter<T extends TServiceClient> extends BaseFilter {

    private static final Logger logger = Grizzly.logger(ThriftClientFilter.class);

    private final Attribute<ObjectPool<SocketAddress, T>> connectionPoolAttribute =
            Grizzly.DEFAULT_ATTRIBUTE_BUILDER.createAttribute(GrizzlyThriftClient.CONNECTION_POOL_ATTRIBUTE_NAME);
    private final Attribute<T> connectionClientAttribute =
            Grizzly.DEFAULT_ATTRIBUTE_BUILDER.createAttribute(GrizzlyThriftClient.CLIENT_ATTRIBUTE_NAME);
    private final Attribute<BlockingQueue<Buffer>> inputBuffersQueueAttribute =
            Grizzly.DEFAULT_ATTRIBUTE_BUILDER.createAttribute(GrizzlyThriftClient.INPUT_BUFFERS_QUEUE_ATTRIBUTE_NAME);

    static final Buffer POISON = Buffers.EMPTY_BUFFER;

    @Override
    public NextAction handleRead(FilterChainContext ctx) throws IOException {
        final Buffer input = ctx.getMessage();
        if (input == null) {
            throw new IOException("input message could not be null");
        }
        if (!input.hasRemaining()) {
            return ctx.getStopAction();
        }
        final Connection connection = ctx.getConnection();
        if (connection == null) {
            throw new IOException("connection must not be null");
        }
        final BlockingQueue<Buffer> inputBuffersQueue = inputBuffersQueueAttribute.get(connection);
        if (inputBuffersQueue == null) {
            throw new IOException("inputBuffersQueue must not be null");
        }
        inputBuffersQueue.offer(input);
        return ctx.getStopAction();
    }

    @SuppressWarnings("unchecked")
    @Override
    public NextAction handleClose(FilterChainContext ctx) throws IOException {
        final Connection<SocketAddress> connection = ctx.getConnection();
        if (connection != null) {
            final ObjectPool<SocketAddress, T> connectionPool = connectionPoolAttribute.remove(connection);
            final T client = connectionClientAttribute.remove(connection);
            if (connectionPool != null && client != null) {
                try {
                    connectionPool.removeObject(connection.getPeerAddress(), client);
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, "the connection has been removed in pool. connection={0}", connection);
                    }
                } catch (Exception ignore) {
                }
            }
            final BlockingQueue<Buffer> inputBuffersQueue = inputBuffersQueueAttribute.remove(connection);
            if (inputBuffersQueue != null) {
                inputBuffersQueue.offer(POISON);
            }
        }
        return ctx.getInvokeAction();
    }
}
