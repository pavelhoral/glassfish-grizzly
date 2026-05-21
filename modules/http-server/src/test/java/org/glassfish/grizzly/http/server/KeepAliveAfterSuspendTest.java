/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation.
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

package org.glassfish.grizzly.http.server;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.glassfish.grizzly.strategies.SameThreadIOStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Reproducer for a keep-alive regression that surfaces when {@link Response#suspend()} is used
 * together with {@link SameThreadIOStrategy}.
 *
 * <p>When the body of a request triggers multiple selector READ events and the handler suspends
 * the response and resumes it asynchronously, the IO-event interest bookkeeping in
 * {@code SameThreadIOStrategy.InterestLifeCycleListenerWhenIoEnabled} ends up with OP_READ
 * disabled on the connection but never re-enabled. The next request on the keep-alive connection
 * is therefore never picked up by the selector and the client times out.</p>
 */
public class KeepAliveAfterSuspendTest {

    private static final int PORT = 18901;
    private static final int BODY_SIZE = 20 * 1024;
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);

    private HttpServer httpServer;
    private ExecutorService asyncWorker;

    @Before
    public void before() throws Exception {
        asyncWorker = Executors.newFixedThreadPool(1);
        httpServer = new HttpServer();
        NetworkListener listener = new NetworkListener("grizzly",
                NetworkListener.DEFAULT_NETWORK_HOST, PORT);
        listener.getTransport().setIOStrategy(SameThreadIOStrategy.getInstance());
        listener.getTransport().setReadBufferSize(4096);
        listener.getTransport().setSelectorRunnersCount(1);
        httpServer.addListener(listener);

        httpServer.getServerConfiguration().addHttpHandler(new HttpHandler() {
            @Override
            public void service(final Request request, final Response response) throws Exception {
                // Acquire the input stream on the selector thread, then dispatch for processing
                // to the worker
                final InputStream in = request.getInputStream();
                // Suspend on the selector thread, do all real work on a worker thread, then
                // resume from the worker. This is the async-handler pattern that triggers the
                // SameThreadIOStrategy interest-bookkeeping bug.
                response.suspend();
                asyncWorker.submit(() -> {
                    try {
                        final byte[] buf = new byte[8192];
                        int total = 0;
                        int n;
                        while ((n = in.read(buf)) >= 0) {
                            total += n;
                        }
                        response.setContentType("text/plain");
                        // Reflect back the length of processed request body.
                        byte[] payload = ("bytes=" + total).getBytes("US-ASCII");
                        response.getOutputStream().write(payload);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        response.resume();
                    }
                });
            }
        }, "/echo");
        httpServer.start();
    }

    @After
    public void after() {
        if (httpServer != null) {
            httpServer.shutdownNow();
        }
        if (asyncWorker != null) {
            asyncWorker.shutdownNow();
        }
    }

    /**
     * Sends two HTTP/1.1 requests using JDK's {@link HttpClient}, which pools and reuses the
     * underlying TCP connection across calls. The first POST carries a 20 KiB body that,
     * combined with the 4 KiB read buffer, spans multiple selector READ events. The second
     * request must be served on the same keep-alive connection. When the bug is present the
     * second {@link HttpClient#send} times out because the selector is parked in
     * {@code select()} with OP_READ disabled on the connection.
     */
    @Test
    public void testKeepAliveAfterSuspendedResponse() throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        byte[] body = new byte[BODY_SIZE];
        Arrays.fill(body, (byte) 'A');

        HttpRequest first = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/echo"))
                .timeout(REQUEST_TIMEOUT)
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        HttpResponse<String> firstResponse = client.send(first, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, firstResponse.statusCode());
        // XXX A separate intermittent bug can shorten the body to a multiple of the read buffer
        // (e.g. 16384 instead of 20480); might or might not be connected.
        assertEquals("unexpected response length", "bytes=" + BODY_SIZE, firstResponse.body());

        HttpRequest second = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/echo"))
                .timeout(REQUEST_TIMEOUT)
                .GET()
                .build();

        HttpResponse<String> secondResponse = client.send(second, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, secondResponse.statusCode());
        assertEquals("bytes=0", secondResponse.body());
    }
}
