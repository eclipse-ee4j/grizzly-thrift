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

import java.net.SocketAddress;

import org.apache.thrift.TServiceClient;

/**
 * The thrift client's interface
 * <p>
 * By {@link #addServer} and {@link #removeServer}, servers can be added and
 * removed dynamically in this thrift client. In other words, the managed server
 * list can be changed in runtime by APIs. By {@link #execute}, user can execute
 * custom service(For more information, please see
 * {@link ThriftClientCallback}'s javadoc).
 *
 * @author Bongjae Chang
 */
public interface ThriftClient<T extends TServiceClient> extends ThriftClientLifecycle {

    public <U> U execute(final ThriftClientCallback<T, U> callback) throws Exception;

    /**
     * Return the name of the thrift client.
     *
     * @return the name of the thrift client.
     */
    public String getName();

    /**
     * Add a specific server in this thrift client
     *
     * @param serverAddress a specific server's {@link java.net.SocketAddress} to be
     * added
     * @return true if the given {@code serverAddress} is added successfully
     */
    public boolean addServer(final SocketAddress serverAddress);

    /**
     * Remove the given server in this thrift client
     *
     * @param serverAddress the specific server's {@link java.net.SocketAddress} to
     * be removed in this thrift client
     */
    public void removeServer(final SocketAddress serverAddress);

    /**
     * Check if this thrift client contains the given server
     *
     * @param serverAddress the specific server's {@link java.net.SocketAddress} to
     * be checked
     * @return true if this thrift client already contains the given
     * {@code serverAddress}
     */
    public boolean isInServerList(final SocketAddress serverAddress);
}
