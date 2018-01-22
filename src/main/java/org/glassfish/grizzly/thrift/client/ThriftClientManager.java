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

import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;

/**
 * The interface for managing thrift clients
 *
 * @author Bongjae Chang
 */
public interface ThriftClientManager {
    /**
     * Creates a new {@link ThriftClientBuilder} for the named thrift client to be managed by this thrift client manager.
     * <p>
     * The returned ThriftClientBuilder is associated with this ThriftClientManager.
     * The ThriftClient will be created, added to this ThriftClientManager and started when
     * {@link ThriftClientBuilder#build()} is called.
     *
     * @param thriftClientName    the name of the thrift client to build. A thrift client name must consist of at least one non-whitespace character.
     * @param thriftClientFactory thrift client factory
     * @return the ThriftClientBuilder for the named thrift client
     */
    public <T extends TServiceClient> ThriftClientBuilder createThriftClientBuilder(final String thriftClientName, final TServiceClientFactory<T> thriftClientFactory);

    /**
     * Looks up a named thrift client.
     *
     * @param thriftClientName the name of the thrift client to look for
     * @return the ThriftClient or null if it does exist
     */
    public <T extends TServiceClient> ThriftClient<T> getThriftClient(final String thriftClientName);

    /**
     * Remove a thrift client from the ThriftClientManager. The thrift client will be stopped.
     *
     * @param thriftClientName the thrift client name
     * @return true if the thrift client was removed
     */
    public boolean removeThriftClient(final String thriftClientName);

    /**
     * Shuts down the ThriftClientManager.
     */
    public void shutdown();
}
