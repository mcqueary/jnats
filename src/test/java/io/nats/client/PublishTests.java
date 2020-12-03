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

package io.nats.client;

import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.NatsConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class PublishTests {
    @Test
    public void throwsIfClosedOnPublish() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.close();
                nc.publish("subject", "replyto", null);
                fail();
            }
        });
    }

    @Test
    public void throwsIfClosedOnFlush() {
        assertThrows(TimeoutException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.close();
                nc.flush(null);
                fail();
            }
        });
    }

    @Test
    public void testThrowsWithoutSubject() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.publish(null, null);
                fail();
            }
        });
    }

    @Test
    public void testThrowsWithoutReplyTo() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.publish("subject", "", null);
                fail();
            }
        });
    }

    @Test
    public void testThrowsIfTooBig() {
        assertThrows(IllegalArgumentException.class, () -> {
            String customInfo = "{\"server_id\":\"myid\",\"max_payload\": 1000}";

            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, customInfo);
                 Connection nc = Nats.connect(ts.getURI())) {
                assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

                byte[] body = new byte[1001];
                nc.publish("subject", null, body);
                fail();
            }
        });
    }

    @Test
    public void testThrowsIfheadersNotSupported() {
        assertThrows(IllegalArgumentException.class, () -> {
            String customInfo = "{\"server_id\":\"test\"}";

            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, customInfo);
                 Connection nc = Nats.connect(ts.getURI())) {
                assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

                nc.publish(new NatsMessage.Builder()
                        .subject("testThrowsIfheadersNotSupported")
                        .headers(new Headers().add("key", "value"))
                        .build());
                fail();
            }
        });
    }

    @Test
    public void testEmptyPublish() throws IOException, InterruptedException,ExecutionException {
        runSimplePublishTest("testsubemptybody", null, null, "");
    }

    @Test
    public void testEmptyByDefaultPublish() throws IOException, InterruptedException,ExecutionException {
        runSimplePublishTest("testsubemptybody", null, null, null);
    }

    @Test
    public void testNoReplyPublish() throws IOException, InterruptedException,ExecutionException {
        runSimplePublishTest("testsub", null, null, "This is the message.");
    }

    @Test
    public void testReplyToInPublish() throws IOException, InterruptedException,ExecutionException {
        runSimplePublishTest("testsubforreply", "replyTo", null, "This is the message to reply to.");
        runSimplePublishTest("testsubforreply", "replyTo", new Headers().add("key", "value"), "This is the message to reply to.");
    }

    private void runSimplePublishTest(String subject, String replyTo, Headers headers, String bodyString)
            throws IOException, InterruptedException,ExecutionException {
        CompletableFuture<Boolean> gotPub = new CompletableFuture<>();
        AtomicReference<String> hdrProto  = new AtomicReference<>("");
        AtomicReference<String> body  = new AtomicReference<>("");
        AtomicReference<String> protocol  = new AtomicReference<>("");

        boolean hPub = headers != null && !headers.isEmpty();
        String proto = hPub ? OP_HPUB : OP_PUB;
        int hdrlen = hPub ? headers.serializedLength() : 0;

        NatsServerProtocolMock.Customizer receiveMessageCustomizer = (ts, r,w) -> {
            String pubLine;
            String headerLine;
            String bodyLine;
            
            System.out.println("*** Mock Server @" + ts.getPort() + " waiting for " + proto + " ...");
            try {
                pubLine = r.readLine();
                if (hPub) {
                    // the version \r\n, each header \r\n, then separator \r\n
                    headerLine = r.readLine() + "\r\n";
                    while (headerLine.length() < hdrlen) {
                        headerLine = headerLine + r.readLine() + "\r\n";
                    }
                }
                else {
                    headerLine = "";
                }
                bodyLine = r.readLine(); // Ignores encoding, but ok for test
            } catch(Exception e) {
                gotPub.cancel(true);
                return;
            }

            if (pubLine.startsWith(proto)) {
                System.out.println("*** Mock Server @" + ts.getPort() + " got " + proto + " ...");
                protocol.set(pubLine);
                hdrProto.set(headerLine);
                body.set(bodyLine);
                gotPub.complete(Boolean.TRUE);
            }
        };

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(receiveMessageCustomizer);
                    Connection nc = Nats.connect(ts.getURI())) {
            byte[] bodyBytes = (bodyString != null) ? bodyString.getBytes(StandardCharsets.UTF_8) : null;

            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

            nc.publish(new NatsMessage.Builder().subject(subject).replyTo(replyTo).headers(headers).dataKeepNull(bodyBytes).build());

            // This is used for the default test
            if (bodyString == null) {
                bodyBytes = EMPTY_BODY;
                bodyString = "";
            }

            assertTrue(gotPub.get(), "Got " + proto + "."); //wait for receipt to close up
            nc.close();
            assertSame(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");

            if (proto.equals(OP_PUB)) {
                String expectedProtocol;
                if (replyTo == null) {
                    expectedProtocol = proto + " " + subject + " " + bodyBytes.length;
                } else {
                    expectedProtocol = proto + " " + subject + " " + replyTo + " " + bodyBytes.length;
                }
                assertEquals(expectedProtocol, protocol.get(), "Protocol matches");
                assertEquals(bodyString, body.get(), "Body matches");
            }
            else {
                String expectedProtocol;
                int hdrLen = headers.serializedLength();
                int totLen = hdrLen + bodyBytes.length;
                if (replyTo == null) {
                    expectedProtocol = proto + " " + subject + " " + hdrLen + " " + totLen;
                } else {
                    expectedProtocol = proto + " " + subject + " " + replyTo + " " + hdrLen + " " + totLen;
                }
                assertEquals(expectedProtocol, protocol.get(), "Protocol matches");
                assertEquals(bodyString, body.get(), "Body matches");
                assertEquals(new String(headers.getSerialized()), hdrProto.get());
            }
        }
    }
}