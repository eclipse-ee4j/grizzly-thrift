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

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.thrift.transport.TTransportException;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.Grizzly;
import org.glassfish.grizzly.GrizzlyFuture;
import org.glassfish.grizzly.Processor;
import org.glassfish.grizzly.attributes.Attribute;
import org.glassfish.grizzly.filterchain.FilterChain;
import org.glassfish.grizzly.memory.MemoryManager;
import org.glassfish.grizzly.thrift.client.GrizzlyThriftClient;
import org.glassfish.grizzly.utils.BufferOutputStream;

/**
 * TGrizzlyClientTransport is the client-side TTransport.
 * <p>
 * BlockingQueue which belongs to ThriftClientFilter has input messages when
 * server's response are arrived. Only TTransport#flush() will be called, output
 * messages will be written. Before flush(), output messages will be stored in
 * buffer.
 *
 * @author Bongjae Chang
 */
public class TGrizzlyClientTransport extends AbstractTGrizzlyTransport {

    private static final long DEFAULT_READ_TIMEOUT_MILLIS = -1L; // never timed out
    private static final long DEFAULT_WRITE_TIMEOUT_MILLIS = -1L; // never timed out

    private Buffer input = null;
    private final Connection connection;
    private final BlockingQueue<Buffer> inputBuffersQueue;
    private final BufferOutputStream outputStream;
    private final long readTimeoutMillis;
    private final long writeTimeoutMillis;

    private final Attribute<BlockingQueue<Buffer>> inputBuffersQueueAttribute = Grizzly.DEFAULT_ATTRIBUTE_BUILDER
            .createAttribute(GrizzlyThriftClient.INPUT_BUFFERS_QUEUE_ATTRIBUTE_NAME);

    private final AtomicBoolean running = new AtomicBoolean();

    public static TGrizzlyClientTransport create(final Connection connection) {
        return create(connection, DEFAULT_READ_TIMEOUT_MILLIS);
    }

    public static TGrizzlyClientTransport create(final Connection connection, final long readTimeoutMillis) {
        return create(connection, readTimeoutMillis, DEFAULT_WRITE_TIMEOUT_MILLIS);
    }

    public static TGrizzlyClientTransport create(final Connection connection, final long readTimeoutMillis, final long writeTimeoutMillis) {
        if (connection == null) {
            throw new IllegalStateException("connection should not be null.");
        }

        final Processor processor = connection.getProcessor();
        if (!(processor instanceof FilterChain)) {
            throw new IllegalStateException("connection's processor has to be a FilterChain.");
        }
        final FilterChain connectionFilterChain = (FilterChain) processor;
        final int idx = connectionFilterChain.indexOfType(ThriftClientFilter.class);
        if (idx == -1) {
            throw new IllegalStateException("connection has to have ThriftClientFilter in the FilterChain.");
        }
        final ThriftClientFilter thriftClientFilter = (ThriftClientFilter) connectionFilterChain.get(idx);
        if (thriftClientFilter == null) {
            throw new IllegalStateException("thriftClientFilter should not be null.");
        }
        return new TGrizzlyClientTransport(connection, readTimeoutMillis, writeTimeoutMillis);
    }

    private TGrizzlyClientTransport(final Connection connection, final long readTimeoutMillis, final long writeTimeoutMillis) {
        this.connection = connection;
        this.inputBuffersQueue = new LinkedTransferQueue<>();
        inputBuffersQueueAttribute.set(connection, this.inputBuffersQueue);
        this.outputStream = new BufferOutputStream(connection.getTransport().getMemoryManager()) {

            @Override
            protected Buffer allocateNewBuffer(final MemoryManager memoryManager, final int size) {
                final Buffer b = memoryManager.allocate(size);
                b.allowBufferDispose(true);
                return b;
            }
        };
        this.readTimeoutMillis = readTimeoutMillis;
        this.writeTimeoutMillis = writeTimeoutMillis;
    }

    @Override
    public boolean isOpen() {
        return connection.isOpen();
    }

    @Override
    public void close() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        final Buffer output = outputStream.getBuffer();
        output.dispose();
        try {
            outputStream.close();
        } catch (IOException ignore) {
        }
        inputBuffersQueueAttribute.remove(connection);
        inputBuffersQueue.clear();
        try {
            final GrizzlyFuture closeFuture = connection.close();
            closeFuture.get(3, TimeUnit.SECONDS);
        } catch (Exception ignore) {
        }
        running.set(false);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flush() throws TTransportException {
        checkConnectionOpen();
        final Buffer output = outputStream.getBuffer();
        output.trim();
        outputStream.reset();
        try {
            final GrizzlyFuture future = connection.write(output);
            if (writeTimeoutMillis > 0) {
                future.get(writeTimeoutMillis, TimeUnit.MILLISECONDS);
            } else {
                future.get();
            }
        } catch (TimeoutException te) {
            throw new TTimedoutException(te);
        } catch (ExecutionException ee) {
            throw new TTransportException(ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public Connection getGrizzlyConnection() {
        return connection;
    }

    @Override
    protected Buffer getInputBuffer() throws TTransportException {
        Buffer localInput = this.input;
        if (localInput == null) {
            localInput = getLocalInput(readTimeoutMillis);
        } else if (localInput.remaining() <= 0) {
            localInput.dispose();
            localInput = getLocalInput(readTimeoutMillis);
        }
        if (localInput == null) {
            throw new TTimedoutException("timed out while reading the input buffer.");
        } else if (localInput == ThriftClientFilter.POISON) {
            throw new TTransportException("client connection was already closed.");
        }
        this.input = localInput;
        return localInput;
    }

    private Buffer getLocalInput(final long readTimeoutMillis) throws TTransportException {
        final Buffer localInput;
        try {
            if (readTimeoutMillis < 0) {
                localInput = inputBuffersQueue.take();
            } else {
                localInput = inputBuffersQueue.poll(readTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TTransportException(ie);
        }
        return localInput;
    }

    @Override
    protected BufferOutputStream getOutputStream() {
        return outputStream;
    }

    private void checkConnectionOpen() throws TTransportException {
        if (!isOpen()) {
            throw new TTransportException("client connection is closed.");
        }
    }
}
