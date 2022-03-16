/*
 * Copyright (c) 2014, 2017 Oracle and/or its affiliates. All rights reserved.
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

package org.glassfish.grizzly.thrift.http;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.http.HttpBaseFilter;
import org.glassfish.grizzly.http.HttpContent;
import org.glassfish.grizzly.http.HttpRequestPacket;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.Protocol;
import org.glassfish.grizzly.http.util.Header;

/**
 * ThriftHttpClientFilter is a client-side filter for Thrift RPC processors over
 * HTTP.
 * <p>
 * Usages:
 *
 * <pre>
 * {@code
 * final FilterChainBuilder clientFilterChainBuilder = FilterChainBuilder.stateless();
 * clientFilterChainBuilder.add(new TransportFilter()).add(new HttpClientFilter()).add(new ThriftHttpClientFilter("/yourUriPath")).add(new ThriftClientFilter());
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
public class ThriftHttpClientFilter extends HttpBaseFilter {

    private static final String THRIFT_HTTP_CONTENT_TYPE = "application/x-thrift";

    private final String uriPath;
    private final Map<String, String> headers = new HashMap<>();

    public ThriftHttpClientFilter(final String uriPath, Map<String, String> headers) {
        this.uriPath = uriPath;

        this.headers.put(Header.Connection.toString(), "keep-alive");
        this.headers.put(Header.Accept.toString(), "*/*");
        this.headers.put(Header.UserAgent.toString(), "grizzly-thrift");

        if (headers != null && !headers.isEmpty()) {
            this.headers.putAll(headers);
        }
    }

    public ThriftHttpClientFilter(final String uriPath) {
        this(uriPath, null);
    }

    @Override
    public NextAction handleRead(FilterChainContext ctx) throws IOException {
        final HttpContent httpContent = ctx.getMessage();
        if (httpContent == null) {
            throw new IOException("httpContent should not be null");
        }
        final Buffer responseBodyBuffer = httpContent.getContent();
        ctx.setMessage(responseBodyBuffer);
        return ctx.getInvokeAction();
    }

    @Override
    public NextAction handleWrite(FilterChainContext ctx) throws IOException {
        final Buffer requestBodyBuffer = ctx.getMessage();
        if (requestBodyBuffer == null) {
            throw new IOException("request body's buffer should not be null");
        }

        final HttpRequestPacket.Builder builder = HttpRequestPacket.builder();
        builder.method(Method.POST);
        builder.protocol(Protocol.HTTP_1_1);
        builder.uri(uriPath);
        final InetSocketAddress peerAddress = (InetSocketAddress) ctx.getConnection().getPeerAddress();
        final String httpHost = peerAddress.getHostName() + ':' + peerAddress.getPort();
        builder.host(httpHost);
        final long contentLength = requestBodyBuffer.remaining();
        if (contentLength >= 0) {
            builder.contentLength(contentLength);
        } else {
            builder.chunked(true);
        }
        builder.contentType(THRIFT_HTTP_CONTENT_TYPE);
        for (Entry<String, String> entry : headers.entrySet()) {
            builder.header(entry.getKey(), entry.getValue());
        }
        final HttpRequestPacket requestPacket = builder.build();

        final HttpContent content = requestPacket.httpContentBuilder().content(requestBodyBuffer).build();
        content.setLast(true);
        ctx.setMessage(content);

        return ctx.getInvokeAction();
    }
}
