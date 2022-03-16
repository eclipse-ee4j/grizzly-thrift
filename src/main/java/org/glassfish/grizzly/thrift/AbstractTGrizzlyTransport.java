/*
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.utils.BufferOutputStream;

/**
 * Abstract class for implementing thrift's TTransport. By using
 * BufferOutputStream, the output buffer will be increased automatically by
 * given MemoryManager if it doesn't have enough spaces.
 *
 * @author Bongjae Chang
 */
public abstract class AbstractTGrizzlyTransport extends TTransport {

    @Override
    public void open() throws TTransportException {
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
        final Buffer input = getInputBuffer();
        final int readableBytes = input.remaining();
        final int bytesToRead = len > readableBytes ? readableBytes : len;
        input.get(buf, off, bytesToRead);
        return bytesToRead;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        final BufferOutputStream outputStream = getOutputStream();
        if (outputStream != null) {
            try {
                outputStream.write(buf, off, len);
            } catch (IOException ie) {
                throw new TTransportException(ie);
            }
        }
    }

    @Override
    public abstract void flush() throws TTransportException;

    @Override
    public TConfiguration getConfiguration() {
        return null;
    }

    @Override
    public void updateKnownMessageSize(long l) throws TTransportException {

    }

    @Override
    public void checkReadBytesAvailable(long l) throws TTransportException {

    }

    protected abstract Buffer getInputBuffer() throws TTransportException;

    protected abstract BufferOutputStream getOutputStream() throws TTransportException;
}
