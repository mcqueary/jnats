// Copyright 2023 The NATS Authors
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
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.BaseConsumeOptions.*;
import static org.junit.jupiter.api.Assertions.*;

public class SimplificationTests extends JetStreamTestBase {

    @Test
    public void testStreamContext() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            assertThrows(JetStreamApiException.class, () -> nc.getStreamContext(stream()));
            assertThrows(JetStreamApiException.class, () -> nc.getStreamContext(stream(), JetStreamOptions.DEFAULT_JS_OPTIONS));
            assertThrows(JetStreamApiException.class, () -> js.getStreamContext(stream()));

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            StreamContext streamContext = nc.getStreamContext(tsc.stream);
            assertEquals(tsc.stream, streamContext.getStreamName());
            _testStreamContext(js, tsc, streamContext);

            tsc = new TestingStreamContainer(jsm);
            streamContext = js.getStreamContext(tsc.stream);
            assertEquals(tsc.stream, streamContext.getStreamName());
            _testStreamContext(js, tsc, streamContext);
        });
    }

    private static void _testStreamContext(JetStream js, TestingStreamContainer tsc, StreamContext streamContext) throws IOException, JetStreamApiException {
        String durable = durable();
        assertThrows(JetStreamApiException.class, () -> streamContext.getConsumerContext(durable));
        assertThrows(JetStreamApiException.class, () -> streamContext.deleteConsumer(durable));

        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(durable).build();
        ConsumerContext consumerContext = streamContext.createOrUpdateConsumer(cc);
        ConsumerInfo ci = consumerContext.getConsumerInfo();
        assertEquals(tsc.stream, ci.getStreamName());
        assertEquals(durable, ci.getName());

        ci = streamContext.getConsumerInfo(durable);
        assertNotNull(ci);
        assertEquals(tsc.stream, ci.getStreamName());
        assertEquals(durable, ci.getName());

        assertEquals(1, streamContext.getConsumerNames().size());

        assertEquals(1, streamContext.getConsumers().size());
        consumerContext = streamContext.getConsumerContext(durable);
        assertNotNull(consumerContext);
        assertEquals(durable, consumerContext.getConsumerName());

        ci = consumerContext.getConsumerInfo();
        assertNotNull(ci);
        assertEquals(tsc.stream, ci.getStreamName());
        assertEquals(durable, ci.getName());

        ci = consumerContext.getCachedConsumerInfo();
        assertNotNull(ci);
        assertEquals(tsc.stream, ci.getStreamName());
        assertEquals(durable, ci.getName());

        streamContext.deleteConsumer(durable);

        assertThrows(JetStreamApiException.class, () -> streamContext.getConsumerContext(durable));
        assertThrows(JetStreamApiException.class, () -> streamContext.deleteConsumer(durable));

        // coverage
        js.publish(tsc.subject(), "one".getBytes());
        js.publish(tsc.subject(), "two".getBytes());
        js.publish(tsc.subject(), "three".getBytes());
        js.publish(tsc.subject(), "four".getBytes());
        js.publish(tsc.subject(), "five".getBytes());
        js.publish(tsc.subject(), "six".getBytes());

        assertTrue(streamContext.deleteMessage(3));
        assertTrue(streamContext.deleteMessage(4, true));

        MessageInfo mi = streamContext.getMessage(1);
        assertEquals(1, mi.getSeq());

        mi = streamContext.getFirstMessage(tsc.subject());
        assertEquals(1, mi.getSeq());

        mi = streamContext.getLastMessage(tsc.subject());
        assertEquals(6, mi.getSeq());

        mi = streamContext.getNextMessage(3, tsc.subject());
        assertEquals(5, mi.getSeq());

        assertNotNull(streamContext.getStreamInfo());
        assertNotNull(streamContext.getStreamInfo(StreamInfoOptions.builder().build()));

        streamContext.purge(PurgeOptions.builder().sequence(5).build());
        assertThrows(JetStreamApiException.class, () -> streamContext.getMessage(1));

        mi = streamContext.getFirstMessage(tsc.subject());
        assertEquals(5, mi.getSeq());

        streamContext.purge();
        assertThrows(JetStreamApiException.class, () -> streamContext.getFirstMessage(tsc.subject()));
    }

    static int FETCH_EPHEMERAL = 1;
    static int FETCH_DURABLE = 2;
    static int FETCH_ORDERED = 3;
    @Test
    public void testFetch() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            TestingStreamContainer tsc = new TestingStreamContainer(nc);
            JetStream js = nc.jetStream();
            for (int x = 1; x <= 20; x++) {
                js.publish(tsc.subject(), ("test-fetch-msg-" + x).getBytes());
            }

            for (int f = FETCH_EPHEMERAL; f <= FETCH_ORDERED; f++) {
                // 1. Different fetch sizes demonstrate expiration behavior

                // 1A. equal number of messages than the fetch size
                _testFetch("1A", nc, tsc, 20, 0, 20, f);

                // 1B. more messages than the fetch size
                _testFetch("1B", nc, tsc, 10, 0, 10, f);

                // 1C. fewer messages than the fetch size
                _testFetch("1C", nc, tsc, 40, 0, 40, f);

                // 1D. simple-consumer-40msgs was created in 1C and has no messages available
                _testFetch("1D", nc, tsc, 40, 0, 40, f);

                // 2. Different max bytes sizes demonstrate expiration behavior
                //    - each test message is approximately 100 bytes

                // 2A. max bytes is reached before message count
                _testFetch("2A", nc, tsc, 0, 750, 20, f);

                // 2B. fetch size is reached before byte count
                _testFetch("2B", nc, tsc, 10, 1500, 10, f);

                if (f > FETCH_EPHEMERAL) {
                    // 2C. fewer bytes than the byte count
                    _testFetch("2C", nc, tsc, 0, 3000, 40, f);
                }
            }
        });
    }

    private static void _testFetch(String label, Connection nc, TestingStreamContainer tsc, int maxMessages, int maxBytes, int testAmount, int fetchType) throws Exception {
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();

        StreamContext ctx = js.getStreamContext(tsc.stream);

        BaseConsumerContext consumerContext;
        if (fetchType == FETCH_ORDERED) {
            consumerContext = ctx.createOrderedConsumer(new OrderedConsumerConfiguration());
            // coverage
        }
        else {
            // Pre define a consumer
            String name = generateConsumerName(maxMessages, maxBytes);
            ConsumerConfiguration.Builder builder = ConsumerConfiguration.builder();
            ConsumerConfiguration cc;
            if (fetchType == FETCH_DURABLE) {
                name = name + "D";
                cc = builder.durable(name).build();
            }
            else {
                name = name + "E";
                cc = builder.name(name).inactiveThreshold(10_000).build();
            }
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            // Consumer[Context]
            consumerContext = ctx.getConsumerContext(name);
        }

        // Custom consume options
        FetchConsumeOptions.Builder builder = FetchConsumeOptions.builder().expiresIn(2000);
        if (maxMessages == 0) {
            builder.maxBytes(maxBytes);
        }
        else if (maxBytes == 0) {
            builder.maxMessages(maxMessages);
        }
        else {
            builder.max(maxBytes, maxMessages);
        }
        FetchConsumeOptions fetchConsumeOptions = builder.build();

        long start = System.currentTimeMillis();

        int rcvd = 0;
        long elapsed;
        // create the consumer then use it
        try (FetchConsumer consumer = consumerContext.fetch(fetchConsumeOptions)) {
            Message msg = consumer.nextMessage();
            while (msg != null) {
                ++rcvd;
                msg.ack();
                msg = consumer.nextMessage();
            }
            elapsed = System.currentTimeMillis() - start;
        }

        switch (label) {
            case "1A":
            case "1B":
            case "2B":
                assertEquals(testAmount, rcvd);
                assertTrue(elapsed < 100);
                break;
            case "1C":
            case "1D":
            case "2C":
                assertTrue(rcvd < testAmount);
                assertTrue(elapsed >= 1500);
                break;
            case "2A":
                assertTrue(rcvd < testAmount);
                assertTrue(elapsed < 100);
                break;
        }
    }

    private static String generateConsumerName(int maxMessages, int maxBytes) {
        return maxBytes == 0
            ? NAME + "-" + maxMessages + "msgs"
            : NAME + "-" + maxBytes + "bytes-" + maxMessages + "msgs";
    }

    @Test
    public void testIterableConsumer() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStream js = nc.jetStream();

            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(tsc.name()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(tsc.stream, tsc.name());

            int stopCount = 500;
            // create the consumer then use it
            try (IterableConsumer consumer = consumerContext.iterate()) {
                _testIterable(js, stopCount, consumer, tsc.subject());
            }

            // coverage
            IterableConsumer consumer = consumerContext.iterate(ConsumeOptions.DEFAULT_CONSUME_OPTIONS);
            consumer.close();
            assertThrows(IllegalArgumentException.class, () -> consumerContext.iterate((ConsumeOptions)null));
        });
    }

    @Test
    public void testOrderedIterableConsumerBasic() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            StreamContext sctx = nc.getStreamContext(tsc.stream);

            int stopCount = 500;
            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubject(tsc.subject());
            OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);
            try (IterableConsumer consumer = occtx.iterate()) {
                _testIterable(js, stopCount, consumer, tsc.subject());
            }
        });
    }

    private static void _testIterable(JetStream js, int stopCount, IterableConsumer consumer, String subject) throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        Thread consumeThread = new Thread(() -> {
            try {
                while (count.get() < stopCount) {
                    Message msg = consumer.nextMessage(1000);
                    if (msg != null) {
                        msg.ack();
                        count.incrementAndGet();
                    }
                }

                Thread.sleep(50); // allows more messages to come across
                consumer.stop();

                Message msg = consumer.nextMessage(1000);
                while (msg != null) {
                    msg.ack();
                    count.incrementAndGet();
                    msg = consumer.nextMessage(1000);
                }
            }
            catch (Exception e) {
                fail(e);
            }
        });
        consumeThread.start();

        Publisher publisher = new Publisher(js, subject, 25);
        Thread pubThread = new Thread(publisher);
        pubThread.start();

        consumeThread.join();
        publisher.stop();
        pubThread.join();

        assertTrue(count.get() > 500);
    }

    @Test
    public void testConsumeWithHandler() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);

            JetStream js = nc.jetStream();
            jsPublish(js, tsc.subject(), 2500);

            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(tsc.name()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(tsc.stream, tsc.name());

            int stopCount = 500;

            CountDownLatch latch = new CountDownLatch(1);
            AtomicInteger atomicCount = new AtomicInteger();
            MessageHandler handler = msg -> {
                msg.ack();
                if (atomicCount.incrementAndGet() == stopCount) {
                    latch.countDown();
                }
            };

            try (MessageConsumer consumer = consumerContext.consume(handler)) {
                latch.await();
                consumer.stop();
                while (!consumer.isFinished()) {
                    //noinspection BusyWait
                    Thread.sleep(10);
                }
                assertTrue(atomicCount.get() > 500);
            }
        });
    }

    @Test
    public void testNext() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            jsPublish(js, tsc.subject(), 4);

            String name = name();

            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(name).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(tsc.stream, name);
            assertThrows(IllegalArgumentException.class, () -> consumerContext.next(1)); // max wait too small
            assertNotNull(consumerContext.next(1000));
            assertNotNull(consumerContext.next(Duration.ofMillis(1000)));
            assertNotNull(consumerContext.next(null));
            assertNotNull(consumerContext.next());
            assertNull(consumerContext.next(1000));

            StreamContext sctx = js.getStreamContext(tsc.stream);
            OrderedConsumerContext occtx = sctx.createOrderedConsumer(new OrderedConsumerConfiguration());
            assertThrows(IllegalArgumentException.class, () -> occtx.next(1)); // max wait too small
            assertNotNull(occtx.next(1000));
            assertNotNull(occtx.next(Duration.ofMillis(1000)));
            assertNotNull(occtx.next(null));
            assertNotNull(occtx.next());
            assertNull(occtx.next(1000));
        });
    }

    @Test
    public void testCoverage() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStream js = nc.jetStream();

            // Pre define a consumer
            jsm.addOrUpdateConsumer(tsc.stream, ConsumerConfiguration.builder().durable(tsc.name(1)).build());
            jsm.addOrUpdateConsumer(tsc.stream, ConsumerConfiguration.builder().durable(tsc.name(2)).build());
            jsm.addOrUpdateConsumer(tsc.stream, ConsumerConfiguration.builder().durable(tsc.name(3)).build());
            jsm.addOrUpdateConsumer(tsc.stream, ConsumerConfiguration.builder().durable(tsc.name(4)).build());

            // Stream[Context]
            StreamContext sctx1 = nc.getStreamContext(tsc.stream);
            nc.getStreamContext(tsc.stream, JetStreamOptions.DEFAULT_JS_OPTIONS);
            js.getStreamContext(tsc.stream);

            // Consumer[Context]
            ConsumerContext cctx1 = nc.getConsumerContext(tsc.stream, tsc.name(1));
            ConsumerContext cctx2 = nc.getConsumerContext(tsc.stream, tsc.name(2), JetStreamOptions.DEFAULT_JS_OPTIONS);
            ConsumerContext cctx3 = js.getConsumerContext(tsc.stream, tsc.name(3));
            ConsumerContext cctx4 = sctx1.getConsumerContext(tsc.name(4));
            ConsumerContext cctx5 = sctx1.createOrUpdateConsumer(ConsumerConfiguration.builder().durable(tsc.name(5)).build());
            ConsumerContext cctx6 = sctx1.createOrUpdateConsumer(ConsumerConfiguration.builder().durable(tsc.name(6)).build());

            after(cctx1.iterate(), tsc.name(1), true);
            after(cctx2.iterate(ConsumeOptions.DEFAULT_CONSUME_OPTIONS), tsc.name(2), true);
            after(cctx3.consume(m -> {}), tsc.name(3), true);
            after(cctx4.consume(ConsumeOptions.DEFAULT_CONSUME_OPTIONS, m -> {}), tsc.name(4), true);
            after(cctx5.fetchMessages(1), tsc.name(5), false);
            after(cctx6.fetchBytes(1000), tsc.name(6), false);
        });
    }

    private void after(MessageConsumer con, String name, boolean doStop) throws Exception {
        ConsumerInfo ci = con.getConsumerInfo();
        assertEquals(name, ci.getName());
        if (doStop) {
            con.stop();
        }
    }

    @Test
    public void testFetchConsumeOptionsBuilder() {
        FetchConsumeOptions fco = FetchConsumeOptions.builder().build();
        assertEquals(DEFAULT_MESSAGE_COUNT, fco.getMaxMessages());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, fco.getExpiresInMillis());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, fco.getThresholdPercent());
        assertEquals(0, fco.getMaxBytes());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS * MAX_IDLE_HEARTBEAT_PERCENT / 100, fco.getIdleHeartbeat());

        fco = FetchConsumeOptions.builder().maxMessages(1000).build();
        assertEquals(1000, fco.getMaxMessages());
        assertEquals(0, fco.getMaxBytes());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, fco.getThresholdPercent());

        fco = FetchConsumeOptions.builder().maxMessages(1000).thresholdPercent(50).build();
        assertEquals(1000, fco.getMaxMessages());
        assertEquals(0, fco.getMaxBytes());
        assertEquals(50, fco.getThresholdPercent());

        fco = FetchConsumeOptions.builder().max(1000, 100).build();
        assertEquals(100, fco.getMaxMessages());
        assertEquals(1000, fco.getMaxBytes());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, fco.getThresholdPercent());

        fco = FetchConsumeOptions.builder().max(1000, 100).thresholdPercent(50).build();
        assertEquals(100, fco.getMaxMessages());
        assertEquals(1000, fco.getMaxBytes());
        assertEquals(50, fco.getThresholdPercent());
    }

    @Test
    public void testConsumeOptionsBuilder() {
        ConsumeOptions co = ConsumeOptions.builder().build();
        assertEquals(DEFAULT_MESSAGE_COUNT, co.getBatchSize());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, co.getExpiresInMillis());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, co.getThresholdPercent());
        assertEquals(0, co.getBatchBytes());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS * MAX_IDLE_HEARTBEAT_PERCENT / 100, co.getIdleHeartbeat());

        co = ConsumeOptions.builder().batchSize(1000).build();
        assertEquals(1000, co.getBatchSize());
        assertEquals(0, co.getBatchBytes());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, co.getThresholdPercent());

        co = ConsumeOptions.builder().batchSize(1000).thresholdPercent(50).build();
        assertEquals(1000, co.getBatchSize());
        assertEquals(0, co.getBatchBytes());
        assertEquals(50, co.getThresholdPercent());

        co = ConsumeOptions.builder().batchBytes(1000).build();
        assertEquals(DEFAULT_MESSAGE_COUNT_WHEN_BYTES, co.getBatchSize());
        assertEquals(1000, co.getBatchBytes());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, co.getThresholdPercent());

        co = ConsumeOptions.builder().thresholdPercent(0).build();
        assertEquals(DEFAULT_THRESHOLD_PERCENT, co.getThresholdPercent());

        co = ConsumeOptions.builder().thresholdPercent(-1).build();
        assertEquals(DEFAULT_THRESHOLD_PERCENT, co.getThresholdPercent());

        co = ConsumeOptions.builder().thresholdPercent(-999).build();
        assertEquals(DEFAULT_THRESHOLD_PERCENT, co.getThresholdPercent());

        co = ConsumeOptions.builder().thresholdPercent(99).build();
        assertEquals(99, co.getThresholdPercent());

        co = ConsumeOptions.builder().thresholdPercent(100).build();
        assertEquals(100, co.getThresholdPercent());

        co = ConsumeOptions.builder().thresholdPercent(101).build();
        assertEquals(100, co.getThresholdPercent());

        co = ConsumeOptions.builder().expiresIn(0).build();
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, co.getExpiresInMillis());

        co = ConsumeOptions.builder().expiresIn(-1).build();
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, co.getExpiresInMillis());

        co = ConsumeOptions.builder().expiresIn(-999).build();
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, co.getExpiresInMillis());

        assertThrows(IllegalArgumentException.class,
            () -> ConsumeOptions.builder().expiresIn(MIN_EXPIRES_MILLS - 1).build());
    }

    // this sim is different from the other sim b/c next has a new sub every message
    public static class PullOrderedNextTestDropSimulator extends PullOrderedMessageManager {
        @SuppressWarnings("ClassEscapesDefinedScope")
        public PullOrderedNextTestDropSimulator(NatsConnection conn, NatsJetStream js, String stream, SubscribeOptions so, ConsumerConfiguration serverCC, boolean queueMode, boolean syncMode) {
            super(conn, js, stream, so, serverCC, syncMode);
        }

        // these have to be static or the test keeps repeating
        static boolean ss2 = true;
        static boolean ss5 = true;

        @Override
        protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
            if (msg.isJetStream()) {
                long ss = msg.metaData().streamSequence();
                if (ss == 2 && ss2) {
                    ss2 = false;
                    return false;
                }
                if (ss == 5 && ss5) {
                    ss5 = false;
                    return false;
                }
            }

            return super.beforeQueueProcessorImpl(msg);
        }
    }

    @Test
    public void testOrderedBehaviorNext() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Get this in place before subscriptions are made
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedNextTestDropSimulator::new;

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            StreamContext sctx = js.getStreamContext(tsc.stream);
            jsPublish(js, tsc.subject(), 101, 6);

            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubject(tsc.subject());
            OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);
            // Loop through the messages to make sure I get stream sequence 1 to 6
            int expectedStreamSeq = 1;
            while (expectedStreamSeq <= 6) {
                Message m = occtx.next(1000);
                if (m != null) {
                    assertEquals(expectedStreamSeq, m.metaData().streamSequence());
                    assertEquals(1, m.metaData().consumerSequence());
                    ++expectedStreamSeq;
                }
            }
        });
    }

    public static class PullOrderedTestDropSimulator extends PullOrderedMessageManager {
        @SuppressWarnings("ClassEscapesDefinedScope")
        public PullOrderedTestDropSimulator(NatsConnection conn, NatsJetStream js, String stream, SubscribeOptions so, ConsumerConfiguration serverCC, boolean queueMode, boolean syncMode) {
            super(conn, js, stream, so, serverCC, syncMode);
        }

        @Override
        protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
            if (msg.isJetStream()
                && msg.metaData().streamSequence() == 2
                && msg.metaData().consumerSequence() == 2)
            {
                return false;
            }

            return super.beforeQueueProcessorImpl(msg);
        }
    }

    @Test
    public void testOrderedBehaviorFetch() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Get this in place before subscriptions are made
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            StreamContext sctx = js.getStreamContext(tsc.stream);
            jsPublish(js, tsc.subject(), 101, 5);
            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubject(tsc.subject());
            OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);
            int expectedStreamSeq = 1;
            FetchConsumeOptions fco = FetchConsumeOptions.builder().maxMessages(6).expiresIn(1000).build();
            try (FetchConsumer fcon = occtx.fetch(fco)) {
                Message m = fcon.nextMessage();
                while (m != null) {
                    assertEquals(expectedStreamSeq++, m.metaData().streamSequence());
                    m = fcon.nextMessage();
                }
                // we know this because the simulator is designed to fail the first time at the second message
                assertEquals(2, expectedStreamSeq);
                // fetch failure will stop the consumer, but make sure it's done b/c with ordered
                // I can't have more than one consuming at a time.
                while (!fcon.isFinished()) {
                    sleep(1);
                }
            }
            // this should finish without error
            try (FetchConsumer fcon = occtx.fetch(fco)) {
                Message m = fcon.nextMessage();
                while (expectedStreamSeq <= 5) {
                    assertEquals(expectedStreamSeq++, m.metaData().streamSequence());
                    m = fcon.nextMessage();
                }
            }
        });
    }

    @Test
    public void testOrderedBehaviorIterable() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Get this in place before subscriptions are made
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            StreamContext sctx = js.getStreamContext(tsc.stream);
            jsPublish(js, tsc.subject(), 101, 5);
            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubject(tsc.subject());
            OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);
            try (IterableConsumer icon = occtx.iterate()) {
                // Loop through the messages to make sure I get stream sequence 1 to 5
                int expectedStreamSeq = 1;
                while (expectedStreamSeq <= 5) {
                    Message m = icon.nextMessage(Duration.ofSeconds(1)); // use duration version here for coverage
                    if (m != null) {
                        assertEquals(expectedStreamSeq++, m.metaData().streamSequence());
                    }
                }
            }
        });
    }

    @Test
    public void testOrderedConsume() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);

            StreamContext sctx = js.getStreamContext(tsc.stream);

            // Get this in place before subscriptions are made
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;

            CountDownLatch msgLatch = new CountDownLatch(6);
            AtomicInteger received = new AtomicInteger();
            AtomicLong[] ssFlags = new AtomicLong[6];
            MessageHandler handler = hmsg -> {
                int i = received.incrementAndGet() - 1;
                ssFlags[i] = new AtomicLong(hmsg.metaData().streamSequence());
                msgLatch.countDown();
            };

            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubject(tsc.subject());
            OrderedConsumerContext octx = sctx.createOrderedConsumer(occ);
            try (MessageConsumer mcon = octx.consume(handler)) {
                jsPublish(js, tsc.subject(), 201, 6);

                // wait for the messages
                awaitAndAssert(msgLatch);

                // Loop through the messages to make sure I get stream sequence 1 to 6
                int expectedStreamSeq = 1;
                while (expectedStreamSeq <= 6) {
                    int idx = expectedStreamSeq - 1;
                    assertEquals(expectedStreamSeq++, ssFlags[idx].get());
                }
            }
        });
    }

    @Test
    public void testOrderedConsumeMultipleSubjects() throws Exception {
        jsServer.run(TestBase::atLeast2_10, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm, 2);
            jsPublish(js, tsc.subject(0), 10);
            jsPublish(js, tsc.subject(1), 5);

            StreamContext sctx = js.getStreamContext(tsc.stream);

            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubjects(tsc.subject(0), tsc.subject(1));
            OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);

            int count0 = 0;
            int count1 = 0;
            try (FetchConsumer fc = occtx.fetch(FetchConsumeOptions.builder().maxMessages(20).expiresIn(2000).build())) {
                Message m = fc.nextMessage();
                while (m != null) {
                    if (m.getSubject().equals(tsc.subject(0))) {
                        count0++;
                    }
                    else {
                        count1++;
                    }
                    m.ack();
                    m = fc.nextMessage();
                }
            }

            assertEquals(10, count0);
            assertEquals(5, count1);
        });
    }

    @Test
    public void testOrderedMultipleWays() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            createMemoryStream(jsm, tsc.stream, tsc.subject());

            StreamContext sctx = js.getStreamContext(tsc.stream);

            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubject(tsc.subject());
            OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);

            // can't do others while doing next
            CountDownLatch latch = new CountDownLatch(1);
            new Thread(() -> {
                try {
                    // make sure there is enough time to call other methods.
                    assertNull(occtx.next(2000));
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                finally {
                    latch.countDown();
                }
            }).start();

            Thread.sleep(100); // make sure there is enough time for the thread to start and get into the next method
            validateCantCallOtherMethods(occtx);

            //noinspection ResultOfMethodCallIgnored
            latch.await(3000, TimeUnit.MILLISECONDS);

            for (int x = 0 ; x < 10_000; x++) {
                js.publish(tsc.subject(), ("multiple" + x).getBytes());
            }

            // can do others now
            Message m = occtx.next(1000);
            assertNotNull(m);
            assertEquals(1, m.metaData().streamSequence());

            // can't do others while doing next
            int seq = 2;
            try (FetchConsumer fc = occtx.fetchMessages(5)) {
                while (seq <= 6) {
                    m = fc.nextMessage();
                    assertNotNull(m);
                    assertEquals(seq, m.metaData().streamSequence());
                    assertFalse(fc.isFinished());
                    validateCantCallOtherMethods(occtx);
                    seq++;
                }

                assertNull(fc.nextMessage());
                assertTrue(fc.isFinished());
                assertNull(fc.nextMessage()); // just some coverage
            }

            // can do others now
            m = occtx.next(1000);
            assertNotNull(m);
            assertEquals(seq++, m.metaData().streamSequence());

            // can't do others while doing iterate
            ConsumeOptions copts = ConsumeOptions.builder().batchSize(10).build();
            try (IterableConsumer ic = occtx.iterate(copts)) {
                ic.stop();
                m = ic.nextMessage(1000);
                while (m != null) {
                    assertEquals(seq, m.metaData().streamSequence());
                    if (!ic.isFinished()) {
                        validateCantCallOtherMethods(occtx);
                    }
                    ++seq;
                    m = ic.nextMessage(1000);
                }
            }

            // can do others now
            m = occtx.next(1000);
            assertNotNull(m);
            assertEquals(seq++, m.metaData().streamSequence());

            int last = Math.min(seq + 10, 9999);
            try (FetchConsumer fc = occtx.fetchMessages(last - seq)) {
                while (seq < last) {
                    fc.stop();
                    m = fc.nextMessage();
                    assertNotNull(m);
                    assertEquals(seq, m.metaData().streamSequence());
                    assertFalse(fc.isFinished());
                    validateCantCallOtherMethods(occtx);
                    seq++;
                }
            }
        });
    }

    private static void validateCantCallOtherMethods(OrderedConsumerContext ctx) {
        assertThrows(IOException.class, () -> ctx.next(1000));
        assertThrows(IOException.class, () -> ctx.fetchMessages(1));
        assertThrows(IOException.class, () -> ctx.consume(null));
    }

    @Test
    public void testOrderedConsumerBuilder() {
        OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration();
        assertEquals(">", occ.getFilterSubject());
        assertNull(occ.getDeliverPolicy());
        assertEquals(ConsumerConfiguration.LONG_UNSET, occ.getStartSequence());
        assertNull(occ.getStartTime());
        assertNull(occ.getReplayPolicy());
        assertNull(occ.getHeadersOnly());

        // nulls
        occ = new OrderedConsumerConfiguration()
            .filterSubject(null)
            .deliverPolicy(null)
            .replayPolicy(null)
            .headersOnly(null);
        assertEquals(">", occ.getFilterSubject());
        assertNull(occ.getDeliverPolicy());
        assertEquals(ConsumerConfiguration.LONG_UNSET, occ.getStartSequence());
        assertNull(occ.getStartTime());
        assertNull(occ.getReplayPolicy());
        assertNull(occ.getHeadersOnly());

        // values that set to default
        occ = new OrderedConsumerConfiguration()
            .filterSubject("")
            .startSequence(-42)
            .headersOnly(false);
        assertEquals(">", occ.getFilterSubject());
        assertNull(occ.getDeliverPolicy());
        assertEquals(ConsumerConfiguration.LONG_UNSET, occ.getStartSequence());
        assertNull(occ.getStartTime());
        assertNull(occ.getReplayPolicy());
        assertNull(occ.getHeadersOnly());

        // values
        ZonedDateTime zdt = ZonedDateTime.now();
        occ = new OrderedConsumerConfiguration()
            .filterSubject("fs")
            .deliverPolicy(DeliverPolicy.All)
            .startSequence(42)
            .startTime(zdt)
            .replayPolicy(ReplayPolicy.Original)
            .headersOnly(true);
        assertEquals("fs", occ.getFilterSubject());
        assertEquals(DeliverPolicy.All, occ.getDeliverPolicy());
        assertEquals(42, occ.getStartSequence());
        assertEquals(zdt, occ.getStartTime());
        assertEquals(ReplayPolicy.Original, occ.getReplayPolicy());
        assertTrue(occ.getHeadersOnly());
    }
}
