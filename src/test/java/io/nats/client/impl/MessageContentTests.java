// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client.impl;


import io.nats.client.*;
import io.nats.client.ConnectionListener.Events;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;


public class MessageContentTests {
    @Test
    public void testSimpleString() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), msg.getData());
            });
            d.subscribe("subject");

            String body = "hello world";
            byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
            Future<Message> incoming = nc.request("subject", bodyBytes);
            Message msg = incoming.get(50000, TimeUnit.MILLISECONDS);

            assertNotNull(msg);
            assertEquals(bodyBytes.length, msg.getData().length);
            assertEquals(body, new String(msg.getData(), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testUTF8String() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), msg.getData());
            });
            d.subscribe("subject");

            String body = "??????";
            byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
            Future<Message> incoming = nc.request("subject", bodyBytes);
            Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

            assertNotNull(msg);
            assertEquals(bodyBytes.length, msg.getData().length);
            assertEquals(body, new String(msg.getData(), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testDifferentSizes() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), msg.getData());
            });
            d.subscribe("subject");

            String body = "hello world";
            for (int i=0;i<10;i++) {

                byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
                Future<Message> incoming = nc.request("subject", bodyBytes);
                Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

                assertNotNull(msg);
                assertEquals(bodyBytes.length, msg.getData().length);
                assertEquals(body, new String(msg.getData(), StandardCharsets.UTF_8));

                body = body+body;
            }
        }
    }

    @Test
    public void testZeros() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), msg.getData());
            });
            d.subscribe("subject");

            byte[] data = new byte[17];
            Future<Message> incoming = nc.request("subject", data);
            Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

            assertNotNull(msg);
            assertEquals(data.length, msg.getData().length);
            assertArrayEquals(msg.getData(), data);
        }
    }
    
    @Test
    public void testDisconnectOnMissingLineFeedContent() throws Exception {
        CompletableFuture<Boolean> ready = new CompletableFuture<>();
        NatsServerProtocolMock.Customizer badServer = (ts, r, w) -> {

            // Wait for client to be ready.
            try {
                ready.get();
            } catch (Exception e) {
                return;
            }

            // System.out.println("*** Mock Server @" + ts.getPort() + " sending bad message ...");
            w.write("MSG test 0 4\rtest"); // Missing \n
            w.flush();
        };

        runBadContentTest(badServer, ready);
    }
    
    @Test
    public void testDisconnectOnTooMuchData() throws Exception {
        CompletableFuture<Boolean> ready = new CompletableFuture<>();
        NatsServerProtocolMock.Customizer badServer = (ts, r, w) -> {

            // Wait for client to be ready.
            try {
                ready.get();
            } catch (Exception e) {
                return;
            }

            // System.out.println("*** Mock Server @" + ts.getPort() + " sending bad message ...");
            w.write("MSG test 0 4\r\ntesttesttest"); // data is too long
            w.flush();
        };

        runBadContentTest(badServer, ready);
    }
    
    @Test
    public void testDisconnectOnNoLineFeedAfterData() throws Exception {
        CompletableFuture<Boolean> ready = new CompletableFuture<>();
        NatsServerProtocolMock.Customizer badServer = (ts, r, w) -> {

            // Wait for client to be ready.
            try {
                ready.get();
            } catch (Exception e) {
                return;
            }

            // System.out.println("*** Mock Server @" + ts.getPort() + " sending bad message ...");
            w.write("MSG test 0 4\r\ntest\rPING"); // no \n after data
            w.flush();
        };

        runBadContentTest(badServer, ready);
    }
    
    @Test
    public void testDisconnectOnBadProtocol() throws Exception {
        CompletableFuture<Boolean> ready = new CompletableFuture<>();
        NatsServerProtocolMock.Customizer badServer = (ts, r, w) -> {
            // Wait for client to be ready.
            try {
                ready.get();
            } catch (Exception e) {
                return;
            }

            // System.out.println("*** Mock Server @" + ts.getPort() + " sending bad message ...");
            w.write("BLAM\r\n"); // Bad protocol op
            w.flush();
        };

        runBadContentTest(badServer, ready);
    }

    void runBadContentTest(NatsServerProtocolMock.Customizer badServer, CompletableFuture<Boolean> ready) throws Exception {
        ListenerForTesting listener = new ListenerForTesting();

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(badServer, null)) {
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(0).
                                errorListener(listener).
                                connectionListener(listener).
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

                listener.prepForStatusChange(Events.DISCONNECTED);
                ready.complete(Boolean.TRUE);
                listener.waitForStatusChange(200, TimeUnit.MILLISECONDS);

                assertTrue(listener.getExceptionCount() > 0);
                assertTrue(Connection.Status.DISCONNECTED == nc.getStatus()
                                                    || Connection.Status.CLOSED == nc.getStatus(), "Disconnected Status");
            } finally {
                nc.close();
                assertSame(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }
}