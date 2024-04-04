// Copyright 2020 The NATS Authors
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
import io.nats.client.api.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.NatsJetStreamConstants.*;
import static io.nats.client.support.NatsKeyValueUtil.KV_OPERATION_HEADER_KEY;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPushAsyncTests extends JetStreamTestBase {

    @Test
    public void testHandlerSub() throws Exception {
        jsServer.run(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            // publish some messages
            jsPublish(js, tsc.subject(), 10);

            // create a dispatcher without a default handler.
            Dispatcher dispatcher = nc.createDispatcher();

            CountDownLatch msgLatch = new CountDownLatch(10);
            AtomicInteger received = new AtomicInteger();

            // create our message handler.
            MessageHandler handler = (Message msg) -> {
                received.incrementAndGet();
                msg.ack();
                msgLatch.countDown();
            };

            // Subscribe using the handler
            js.subscribe(tsc.subject(), dispatcher, handler, false);

            // Wait for messages to arrive using the countdown latch.
            // make sure we don't wait forever
            awaitAndAssert(msgLatch);

            assertEquals(10, received.get());
        });
    }

    @Test
    public void testHandlerAutoAck() throws Exception {
        jsServer.run(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            // publish some messages
            jsPublish(js, tsc.subject(), 10);

            // create a dispatcher without a default handler.
            Dispatcher dispatcher = nc.createDispatcher();

            // 1. auto ack true
            CountDownLatch msgLatch1 = new CountDownLatch(10);
            AtomicInteger received1 = new AtomicInteger();

            // create our message handler, does not ack
            MessageHandler handler1 = (Message msg) -> {
                received1.incrementAndGet();
                msgLatch1.countDown();
            };

            // subscribe using the handler, auto ack true
            PushSubscribeOptions pso1 = PushSubscribeOptions.builder().durable(durable(1)).build();
            JetStreamSubscription sub  = js.subscribe(tsc.subject(), dispatcher, handler1, true, pso1);

            // Wait for messages to arrive using the countdown latch.
            // make sure we don't wait forever
            awaitAndAssert(msgLatch1);

            assertEquals(10, received1.get());

            // check that all the messages were read by the durable
            dispatcher.unsubscribe(sub);
            sub = js.subscribe(tsc.subject(), pso1);
            assertNull(sub.nextMessage(Duration.ofSeconds(1)));

            // 2. auto ack false
            CountDownLatch msgLatch2 = new CountDownLatch(10);
            AtomicInteger received2 = new AtomicInteger();

            // create our message handler, also does not ack
            MessageHandler handler2 = (Message msg) -> {
                received2.incrementAndGet();
                msgLatch2.countDown();
            };

            // subscribe using the handler, auto ack false
            ConsumerConfiguration cc = ConsumerConfiguration.builder().ackWait(Duration.ofMillis(500)).build();
            PushSubscribeOptions pso2 = PushSubscribeOptions.builder().durable(durable(2)).configuration(cc).build();
            sub = js.subscribe(tsc.subject(), dispatcher, handler2, false, pso2);

            // Wait for messages to arrive using the countdown latch.
            // make sure we don't wait forever
            awaitAndAssert(msgLatch2);
            assertEquals(10, received2.get());

            ConsumerInfo ci = sub.getConsumerInfo();
            assertEquals(10, ci.getNumAckPending());

            sleep(1000); // just give it time for the server to realize the messages are not ack'ed

            dispatcher.unsubscribe(sub);
            sub = js.subscribe(tsc.subject(), pso2);
            List<Message> list = readMessagesAck(sub, false);
            assertEquals(10, list.size());
        });
    }

    @Test
    public void testCantNextMessageOnAsyncPushSub() throws Exception {
        jsServer.run(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            JetStreamSubscription sub = js.subscribe(tsc.subject(), nc.createDispatcher(), msg -> {}, false);

            // this should exception, can't next message on an async push sub
            assertThrows(IllegalStateException.class, () -> sub.nextMessage(Duration.ofMillis(1000)));
            assertThrows(IllegalStateException.class, () -> sub.nextMessage(1000));
        });
    }

    @Test
    public void testPushAsyncFlowControl() throws Exception {
        ListenerForTesting listenerForTesting = new ListenerForTesting();
        Options.Builder ob = new Options.Builder().errorListener(listenerForTesting);

        runInJsServer(ob, nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            byte[] data = new byte[8192];

            int MSG_COUNT = 1000;

            // publish some messages
            for (int x = 100_000; x < MSG_COUNT + 100_000; x++) {
                byte[] fill = (""+ x).getBytes();
                System.arraycopy(fill, 0, data, 0, 6);
                js.publish(NatsMessage.builder().subject(tsc.subject()).data(data).build());
            }

            // create a dispatcher without a default handler.
            Dispatcher dispatcher = nc.createDispatcher();

            CountDownLatch msgLatch = new CountDownLatch(MSG_COUNT);
            AtomicInteger count = new AtomicInteger();
            AtomicReference<Set<String>> set = new AtomicReference<>(new HashSet<>());

            // create our message handler.
            MessageHandler handler = (Message msg) -> {
                String id = new String(Arrays.copyOf(msg.getData(), 6));
                if (set.get().add(id)) {
                    count.incrementAndGet();
                }
                sleep(5); // slow the process down to hopefully get flow control more often
                msg.ack();
                msgLatch.countDown();
            };

            ConsumerConfiguration cc = ConsumerConfiguration.builder().flowControl(1000).build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
            js.subscribe(tsc.subject(), dispatcher, handler, false, pso);

            // Wait for messages to arrive using the countdown latch.
            // make sure we don't wait forever
            awaitAndAssert(msgLatch);

            assertEquals(MSG_COUNT, count.get());
            assertFalse(listenerForTesting.getFlowControlProcessedEvents().isEmpty());
        });
    }

    @Test
    public void testDontAutoAckSituations() throws Exception {
        String mockAckReply = "mock-ack-reply.";

        jsServer.run(nc -> {
            // create the stream.
            String stream = stream();
            String subject = subject();
            createMemoryStream(nc, stream, subject, mockAckReply + "*");

            // Create our JetStream context.
            JetStream js = nc.jetStream();

            int pubCount = 5;

            // publish a message
            jsPublish(js, subject, pubCount);

            // create a dispatcher without a default handler.
            Dispatcher dispatcher = nc.createDispatcher();

            AtomicReference<CountDownLatch> msgLatchRef = new AtomicReference<>(new CountDownLatch(pubCount));

            AtomicInteger count = new AtomicInteger();

            // create our message handler, does not ack
            MessageHandler handler = (Message msg) -> {
                NatsJetStreamMessage m = (NatsJetStreamMessage)msg;
                // ack the real ack address for the server's sake.
                // The message and handler code won't know anything about this,
                // so its behavior won't be affected which is what we are testing
                nc.publish(m.replyTo, AckType.AckAck.bodyBytes(-1));

                int f = count.incrementAndGet();
                if (f == 1) {
                    m.replyTo = mockAckReply + "ack";
                    m.ack();
                }
                else if (f == 2) {
                    m.replyTo = mockAckReply + "nak";
                    m.nak();
                }
                else if (f == 3) {
                    m.replyTo = mockAckReply + "term";
                    m.term();
                }
                else if (f == 4) {
                    m.replyTo = mockAckReply + "progress";
                    m.inProgress();
                }
                else {
                    m.replyTo = mockAckReply + "auto";
                }
                msgLatchRef.get().countDown();
            };

            // subscribe using the handler, auto  ack true
            JetStreamSubscription async = js.subscribe(subject, dispatcher, handler, true);

            // Wait for messages to arrive using the countdown latch.
            // make sure we don't wait forever
            awaitAndAssert(msgLatchRef.get());
            assertEquals(0, msgLatchRef.get().getCount());
            dispatcher.unsubscribe(async);

            JetStreamSubscription mockAckReplySub = js.subscribe(mockAckReply + "*");
            Message msg = mockAckReplySub.nextMessage(2000);
            assertEquals(mockAckReply + "ack", msg.getSubject());
            assertEquals("+ACK", new String(msg.getData()));

            msg = mockAckReplySub.nextMessage(500);
            assertEquals(mockAckReply + "nak", msg.getSubject());
            assertEquals("-NAK", new String(msg.getData()));

            msg = mockAckReplySub.nextMessage(500);
            assertEquals(mockAckReply + "term", msg.getSubject());
            assertEquals("+TERM", new String(msg.getData()));

            msg = mockAckReplySub.nextMessage(500);
            assertEquals(mockAckReply + "progress", msg.getSubject());
            assertEquals("+WPI", new String(msg.getData()));

            // because it was in progress which is not a terminal ack, the auto ack acks
            msg = mockAckReplySub.nextMessage(500);
            assertEquals(mockAckReply + "progress", msg.getSubject());
            assertEquals("+ACK", new String(msg.getData()));

            msg = mockAckReplySub.nextMessage(500);
            assertEquals(mockAckReply + "auto", msg.getSubject());
            assertEquals("+ACK", new String(msg.getData()));

            // coverage explicit no ack flag
            msgLatchRef.set(new CountDownLatch(pubCount));
            PushSubscribeOptions pso = ConsumerConfiguration.builder().ackWait(Duration.ofMinutes(2)).buildPushSubscribeOptions();
            async = js.subscribe(subject, dispatcher, handler, false, pso);
            awaitAndAssert(msgLatchRef.get());
            assertEquals(0, msgLatchRef.get().getCount());
            dispatcher.unsubscribe(async);

            // no messages should have been published to our mock reply
            assertNull(mockAckReplySub.nextMessage(1000));

            // coverage explicit AckPolicyNone
            msgLatchRef.set(new CountDownLatch(pubCount));
            pso = ConsumerConfiguration.builder().ackPolicy(AckPolicy.None).buildPushSubscribeOptions();
            async = js.subscribe(subject, dispatcher, handler, true, pso);
            awaitAndAssert(msgLatchRef.get());
            assertEquals(0, msgLatchRef.get().getCount());
            dispatcher.unsubscribe(async);

            // no messages should have been published to our mock reply
            assertNull(mockAckReplySub.nextMessage(1000));
        });
    }

    static class MemStorBugHandler implements MessageHandler {
        public List<Message> messages = new ArrayList<>();

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            messages.add(msg);
        }
    }

    @Test
    public void testMemoryStorageServerBugPR2719() throws Exception {
        String stream = stream();
        String sub = "msbsub.>";
        String key1 = "msbsub.key1";
        String key2 = "msbsub.key2";

        Headers deleteHeaders = new Headers()
            .put(KV_OPERATION_HEADER_KEY, KeyValueOperation.DELETE.name());
        Headers purgeHeaders = new Headers()
            .put(KV_OPERATION_HEADER_KEY, KeyValueOperation.PURGE.name())
            .put(ROLLUP_HDR, ROLLUP_HDR_SUBJECT);

        jsServer.run(nc -> {
            StreamConfiguration sc = StreamConfiguration.builder()
                .name(stream)
                .storageType(StorageType.Memory)
                .subjects(sub)
                .allowRollup(true)
                .denyDelete(true)
                .build();

            nc.jetStreamManagement().addStream(sc);

            JetStream js = nc.jetStream();

            MemStorBugHandler fullHandler = new MemStorBugHandler();
            MemStorBugHandler onlyHandler = new MemStorBugHandler();

            PushSubscribeOptions psoFull = ConsumerConfiguration.builder()
                .ackPolicy(AckPolicy.None)
                .deliverPolicy(DeliverPolicy.LastPerSubject)
                .flowControl(5000)
                .filterSubject(sub)
                .headersOnly(false)
                .buildPushSubscribeOptions();

            PushSubscribeOptions psoOnly = ConsumerConfiguration.builder()
                .ackPolicy(AckPolicy.None)
                .deliverPolicy(DeliverPolicy.LastPerSubject)
                .flowControl(5000)
                .filterSubject(sub)
                .headersOnly(true)
                .buildPushSubscribeOptions();

            Dispatcher d = nc.createDispatcher();
            js.subscribe(sub, d, fullHandler, false, psoFull);
            js.subscribe(sub, d, onlyHandler, false, psoOnly);

            Object[] expecteds = new Object[] {
                "a", "aa", "z", "zz",
                KeyValueOperation.DELETE, KeyValueOperation.DELETE,
                "aaa", "zzz",
                KeyValueOperation.DELETE, KeyValueOperation.DELETE,
                KeyValueOperation.PURGE, KeyValueOperation.PURGE,
            };

            js.publish(NatsMessage.builder().subject(key1).data((String)expecteds[0]).build());
            js.publish(NatsMessage.builder().subject(key1).data((String)expecteds[1]).build());
            js.publish(NatsMessage.builder().subject(key2).data((String)expecteds[2]).build());
            js.publish(NatsMessage.builder().subject(key2).data((String)expecteds[3]).build());

            js.publish(NatsMessage.builder().subject(key1).headers(deleteHeaders).build());
            js.publish(NatsMessage.builder().subject(key2).headers(deleteHeaders).build());

            js.publish(NatsMessage.builder().subject(key1).data((String)expecteds[6]).build());
            js.publish(NatsMessage.builder().subject(key2).data((String)expecteds[7]).build());

            js.publish(NatsMessage.builder().subject(key1).headers(deleteHeaders).build());
            js.publish(NatsMessage.builder().subject(key2).headers(deleteHeaders).build());

            js.publish(NatsMessage.builder().subject(key1).headers(purgeHeaders).build());
            js.publish(NatsMessage.builder().subject(key2).headers(purgeHeaders).build());

            sleep(2000); // give time for the handler to get messages

            validateRegular(expecteds, fullHandler);
            validateHeadersOnly(expecteds, onlyHandler);
        });
    }

    private void validateHeadersOnly(Object[] expecteds, MemStorBugHandler handler) {
        int aix = 0;
        for (Message m : handler.messages) {
            Object expected = expecteds[aix++];
            Headers h = m.getHeaders();
            assertNotNull(h);
            if (expected instanceof String) {
                assertTrue(m.getData() == null || m.getData().length == 0);
                assertEquals("" + ((String)expected).length(), h.getFirst(MSG_SIZE_HDR));
                assertNull(h.getFirst(KV_OPERATION_HEADER_KEY));
                assertNull(h.getFirst(ROLLUP_HDR));
            }
            else {
                assertTrue(m.getData() == null || m.getData().length == 0);
                assertEquals("0", h.getFirst(MSG_SIZE_HDR));
                assertEquals(expected, KeyValueOperation.valueOf(h.getFirst(KV_OPERATION_HEADER_KEY)));
                if (expected == KeyValueOperation.PURGE) {
                    assertEquals(ROLLUP_HDR_SUBJECT, h.getFirst(ROLLUP_HDR));
                }
                else {
                    assertNull(h.getFirst(ROLLUP_HDR));
                }
            }
        }
    }

    private void validateRegular(Object[] expecteds, MemStorBugHandler handler) {
        int aix = 0;
        for (Message m : handler.messages) {
            Object expected = expecteds[aix++];
            if (expected instanceof String) {
                assertEquals(expected, new String(m.getData()));
                assertNull(m.getHeaders());
            }
            else {
                Headers h = m.getHeaders();
                assertNotNull(h);
                assertTrue(m.getData() == null || m.getData().length == 0);
                assertNull(h.getFirst(MSG_SIZE_HDR));
                assertEquals(expected, KeyValueOperation.valueOf(h.getFirst(KV_OPERATION_HEADER_KEY)));
                if (expected == KeyValueOperation.PURGE) {
                    assertEquals(ROLLUP_HDR_SUBJECT, h.getFirst(ROLLUP_HDR));
                }
                else {
                    assertNull(h.getFirst(ROLLUP_HDR));
                }
            }
        }
    }
}
