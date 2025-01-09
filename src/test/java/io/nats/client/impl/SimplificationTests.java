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
import io.nats.client.support.*;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
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

        mi = streamContext.getLastMessage(tsc.subject());
        assertEquals(6, mi.getSeq());

        mi = streamContext.getNextMessage(3, tsc.subject());
        assertEquals(5, mi.getSeq());

        assertNotNull(streamContext.getStreamInfo());
        assertNotNull(streamContext.getStreamInfo(StreamInfoOptions.builder().build()));

        streamContext.purge(PurgeOptions.builder().sequence(5).build());
        assertThrows(JetStreamApiException.class, () -> streamContext.getMessage(1));
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
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(tsc.consumerName()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(tsc.stream, tsc.consumerName());

            int stopCount = 500;
            // create the consumer then use it
            try (IterableConsumer consumer = consumerContext.iterate()) {
                _testIterableBasic(js, stopCount, consumer, tsc.subject());
            }

            // coverage
            IterableConsumer consumer = consumerContext.iterate(ConsumeOptions.DEFAULT_CONSUME_OPTIONS);
            consumer.close();
            assertThrows(IllegalArgumentException.class, () -> consumerContext.iterate((ConsumeOptions)null));
        });
    }

    @Test
    public void testOrderedConsumerDeliverPolices() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);

            jsPublish(js, tsc.subject(), 101, 3, 100);
            ZonedDateTime startTime = getStartTimeFirstMessage(js, tsc);

            StreamContext sctx = nc.getStreamContext(tsc.stream);

            // test a start time
            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration()
                .filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartTime)
                .startTime(startTime);
            OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);
            try (IterableConsumer consumer = occtx.iterate()) {
                Message m = consumer.nextMessage(1000);
                assertEquals(2, m.metaData().streamSequence());
            }

            // test a start sequence
            occ = new OrderedConsumerConfiguration()
                .filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartSequence)
                .startSequence(2);
            occtx = sctx.createOrderedConsumer(occ);
            try (IterableConsumer consumer = occtx.iterate()) {
                Message m = consumer.nextMessage(1000);
                assertEquals(2, m.metaData().streamSequence());
            }
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
                _testIterableBasic(js, stopCount, consumer, tsc.subject());
            }
        });
    }

    private static void _testIterableBasic(JetStream js, int stopCount, IterableConsumer consumer, String subject) throws InterruptedException {
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
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(tsc.consumerName()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(tsc.stream, tsc.consumerName());

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
            jsm.addOrUpdateConsumer(tsc.stream, ConsumerConfiguration.builder().durable(tsc.consumerName(1)).build());
            jsm.addOrUpdateConsumer(tsc.stream, ConsumerConfiguration.builder().durable(tsc.consumerName(2)).build());
            jsm.addOrUpdateConsumer(tsc.stream, ConsumerConfiguration.builder().durable(tsc.consumerName(3)).build());
            jsm.addOrUpdateConsumer(tsc.stream, ConsumerConfiguration.builder().durable(tsc.consumerName(4)).build());

            // Stream[Context]
            StreamContext sctx1 = nc.getStreamContext(tsc.stream);
            nc.getStreamContext(tsc.stream, JetStreamOptions.DEFAULT_JS_OPTIONS);
            js.getStreamContext(tsc.stream);

            // Consumer[Context]
            ConsumerContext cctx1 = nc.getConsumerContext(tsc.stream, tsc.consumerName(1));
            ConsumerContext cctx2 = nc.getConsumerContext(tsc.stream, tsc.consumerName(2), JetStreamOptions.DEFAULT_JS_OPTIONS);
            ConsumerContext cctx3 = js.getConsumerContext(tsc.stream, tsc.consumerName(3));
            ConsumerContext cctx4 = sctx1.getConsumerContext(tsc.consumerName(4));
            ConsumerContext cctx5 = sctx1.createOrUpdateConsumer(ConsumerConfiguration.builder().durable(tsc.consumerName(5)).build());
            ConsumerContext cctx6 = sctx1.createOrUpdateConsumer(ConsumerConfiguration.builder().durable(tsc.consumerName(6)).build());

            after(cctx1.iterate(), tsc.consumerName(1), true);
            after(cctx2.iterate(ConsumeOptions.DEFAULT_CONSUME_OPTIONS), tsc.consumerName(2), true);
            after(cctx3.consume(m -> {}), tsc.consumerName(3), true);
            after(cctx4.consume(ConsumeOptions.DEFAULT_CONSUME_OPTIONS, m -> {}), tsc.consumerName(4), true);
            after(cctx5.fetchMessages(1), tsc.consumerName(5), false);
            after(cctx6.fetchBytes(1000), tsc.consumerName(6), false);
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
    public void testFetchConsumeOptionsBuilder() throws IOException, ClassNotFoundException {
        FetchConsumeOptions fco = FetchConsumeOptions.builder().build();
        check_default_values(fco);
        check_default_values(roundTripSerialize(fco));

        // COVERAGE
        SerializableFetchConsumeOptions sfco = new SerializableFetchConsumeOptions();
        sfco.setFetchConsumeOptions(fco);
        check_default_values(sfco.getFetchConsumeOptions());

        // COVERAGE
        sfco = new SerializableFetchConsumeOptions(FetchConsumeOptions.builder());
        sfco.setFetchConsumeOptions(fco);
        check_default_values(sfco.getFetchConsumeOptions());

        fco = FetchConsumeOptions.builder().maxMessages(1000).build();
        check_values(fco, 1000, 0, DEFAULT_THRESHOLD_PERCENT);
        check_values(roundTripSerialize(fco), 1000, 0, DEFAULT_THRESHOLD_PERCENT);

        fco = FetchConsumeOptions.builder().maxMessages(1000).thresholdPercent(50).build();
        check_values(fco, 1000, 0, 50);
        check_values(roundTripSerialize(fco), 1000, 0, 50);

        fco = FetchConsumeOptions.builder().max(1000, 100).build();
        check_values(fco, 100, 1000, DEFAULT_THRESHOLD_PERCENT);
        check_values(roundTripSerialize(fco), 100, 1000, DEFAULT_THRESHOLD_PERCENT);

        fco = FetchConsumeOptions.builder().max(1000, 100).thresholdPercent(50).build();
        check_values(fco, 100, 1000, 50);
        check_values(roundTripSerialize(fco), 100, 1000, 50);

        fco = FetchConsumeOptions.builder().group("g").minPending(1).minAckPending(2).build();
        assertEquals("g", fco.getGroup());
        assertEquals(1, fco.getMinPending());
        assertEquals(2, fco.getMinAckPending());

        fco = roundTripSerialize(fco);
        assertEquals("g", fco.getGroup());
        assertEquals(1, fco.getMinPending());
        assertEquals(2, fco.getMinAckPending());
    }

    private static void check_default_values(FetchConsumeOptions fco) {
        assertEquals(DEFAULT_MESSAGE_COUNT, fco.getMaxMessages());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, fco.getExpiresInMillis());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, fco.getThresholdPercent());
        assertEquals(0, fco.getMaxBytes());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS * MAX_IDLE_HEARTBEAT_PERCENT / 100, fco.getIdleHeartbeat());
    }

    private static void check_values(FetchConsumeOptions fco, int maxMessages, int maxBytes, int thresholdPercent) throws IOException, ClassNotFoundException {
        _check_values(fco, maxMessages, maxBytes, thresholdPercent);
        fco = roundTripSerialize(fco);
        _check_values(fco, maxMessages, maxBytes, thresholdPercent);
    }

    private static void _check_values(FetchConsumeOptions fco, int maxMessages, int maxBytes, int thresholdPercent) {
        assertEquals(maxMessages, fco.getMaxMessages());
        assertEquals(maxBytes, fco.getMaxBytes());
        assertEquals(thresholdPercent, fco.getThresholdPercent());
        assertNull(fco.getGroup());
        assertEquals(-1, fco.getMinPending());
        assertEquals(-1, fco.getMinAckPending());
    }

    private static FetchConsumeOptions roundTripSerialize(FetchConsumeOptions fco) throws IOException, ClassNotFoundException {
        SerializableFetchConsumeOptions sfco = new SerializableFetchConsumeOptions(fco);
        sfco = (SerializableFetchConsumeOptions)roundTripSerialize(sfco);
        return sfco.getFetchConsumeOptions();
    }

    @Test
    public void testConsumeOptionsBuilder() throws IOException, ClassNotFoundException {
        ConsumeOptions co = ConsumeOptions.builder().build();
        check_default_values(co);
        check_default_values(roundTripSerialize(co));

        co = ConsumeOptions.builder().batchSize(1000).build();
        check_values(co, 1000, 0, DEFAULT_THRESHOLD_PERCENT);
        check_values(roundTripSerialize(co), 1000, 0, DEFAULT_THRESHOLD_PERCENT);

        co = ConsumeOptions.builder().batchSize(1000).thresholdPercent(50).build();
        check_values(co, 1000, 0, 50);
        check_values(roundTripSerialize(co), 1000, 0, 50);

        co = ConsumeOptions.builder().batchBytes(1000).build();
        check_values(co, DEFAULT_MESSAGE_COUNT_WHEN_BYTES, 1000, DEFAULT_THRESHOLD_PERCENT);
        check_values(roundTripSerialize(co), DEFAULT_MESSAGE_COUNT_WHEN_BYTES, 1000, DEFAULT_THRESHOLD_PERCENT);

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

        co = ConsumeOptions.builder().group("g").minPending(1).minAckPending(2).build();
        assertEquals("g", co.getGroup());
        assertEquals(1, co.getMinPending());
        assertEquals(2, co.getMinAckPending());

        co = roundTripSerialize(co);
        assertEquals("g", co.getGroup());
        assertEquals(1, co.getMinPending());
        assertEquals(2, co.getMinAckPending());
    }

    private static void check_default_values(ConsumeOptions co) throws IOException, ClassNotFoundException {
        assertEquals(DEFAULT_MESSAGE_COUNT, co.getBatchSize());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, co.getExpiresInMillis());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, co.getThresholdPercent());
        assertEquals(0, co.getBatchBytes());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS * MAX_IDLE_HEARTBEAT_PERCENT / 100, co.getIdleHeartbeat());
    }

    private static void check_values(ConsumeOptions co, int batchSize, int batchBytes, int thresholdPercent) throws IOException, ClassNotFoundException {
        assertEquals(batchSize, co.getBatchSize());
        assertEquals(batchBytes, co.getBatchBytes());
        assertEquals(thresholdPercent, co.getThresholdPercent());
        assertNull(co.getGroup());
        assertEquals(-1, co.getMinPending());
        assertEquals(-1, co.getMinAckPending());
    }

    private static ConsumeOptions roundTripSerialize(ConsumeOptions co) throws IOException, ClassNotFoundException {
        SerializableConsumeOptions sco = new SerializableConsumeOptions(co);
        sco = (SerializableConsumeOptions)roundTripSerialize(sco);
        return sco.getConsumeOptions();
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

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            StreamContext sctx = js.getStreamContext(tsc.stream);

            jsPublish(js, tsc.subject(), 101, 6, 100);
            ZonedDateTime startTime = getStartTimeFirstMessage(js, tsc);

            // New pomm factory in place before each subscription is made
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedNextTestDropSimulator::new;
            _testOrderedNext(sctx, 1, new OrderedConsumerConfiguration().filterSubject(tsc.subject()));

            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedNextTestDropSimulator::new;
            _testOrderedNext(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartTime).startTime(startTime));

            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedNextTestDropSimulator::new;
            _testOrderedNext(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(2));
        });
    }

    private static ZonedDateTime getStartTimeFirstMessage(JetStream js, TestingStreamContainer tsc) throws IOException, JetStreamApiException, InterruptedException {
        ZonedDateTime startTime;
        JetStreamSubscription sub = js.subscribe(tsc.subject());
        Message mt = sub.nextMessage(1000);
        startTime = mt.metaData().timestamp().plus(30, ChronoUnit.MILLIS);
        sub.unsubscribe();
        return startTime;
    }

    private static void _testOrderedNext(StreamContext sctx, int expectedStreamSeq, OrderedConsumerConfiguration occ) throws IOException, JetStreamApiException, InterruptedException, JetStreamStatusCheckedException {
        OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);
        // Loop through the messages to make sure I get stream sequence 1 to 6
        while (expectedStreamSeq <= 6) {
            Message m = occtx.next(1000);
            if (m != null) {
                assertEquals(expectedStreamSeq, m.metaData().streamSequence());
                assertEquals(1, m.metaData().consumerSequence());
                ++expectedStreamSeq;
            }
        }
    }

    public static long CS_FOR_SS_3 = 3;
    public static class PullOrderedTestDropSimulator extends PullOrderedMessageManager {
        @SuppressWarnings("ClassEscapesDefinedScope")
        public PullOrderedTestDropSimulator(NatsConnection conn, NatsJetStream js, String stream, SubscribeOptions so, ConsumerConfiguration serverCC, boolean queueMode, boolean syncMode) {
            super(conn, js, stream, so, serverCC, syncMode);
        }

        @Override
        protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
            if (msg.isJetStream()
                && msg.metaData().streamSequence() == 3
                && msg.metaData().consumerSequence() == CS_FOR_SS_3)
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

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            StreamContext sctx = js.getStreamContext(tsc.stream);

            jsPublish(js, tsc.subject(), 101, 6, 100);
            ZonedDateTime startTime = getStartTimeFirstMessage(js, tsc);

            // New pomm factory in place before each subscription is made
            // Set the Consumer Sequence For Stream Sequence 3 statically for ease
            CS_FOR_SS_3 = 3;
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;
            _testOrderedFetch(sctx, 1, new OrderedConsumerConfiguration().filterSubject(tsc.subject()));

            CS_FOR_SS_3 = 2;
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;
            _testOrderedFetch(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartTime).startTime(startTime));

            CS_FOR_SS_3 = 2;
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;
            _testOrderedFetch(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(2));
        });
    }

    private static void _testOrderedFetch(StreamContext sctx, int expectedStreamSeq, OrderedConsumerConfiguration occ) throws Exception {
        OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);
        FetchConsumeOptions fco = FetchConsumeOptions.builder().maxMessages(6).expiresIn(1000).build();
        try (FetchConsumer fcon = occtx.fetch(fco)) {
            Message m = fcon.nextMessage();
            while (m != null) {
                assertEquals(expectedStreamSeq++, m.metaData().streamSequence());
                m = fcon.nextMessage();
            }
            // we know this because the simulator is designed to fail the first time at the third message
            assertEquals(3, expectedStreamSeq);
            // fetch failure will stop the consumer, but make sure it's done b/c with ordered
            // I can't have more than one consuming at a time.
            while (!fcon.isFinished()) {
                sleep(1);
            }
        }
        // this should finish without error
        try (FetchConsumer fcon = occtx.fetch(fco)) {
            Message m = fcon.nextMessage();
            while (expectedStreamSeq <= 6) {
                assertEquals(expectedStreamSeq++, m.metaData().streamSequence());
                m = fcon.nextMessage();
            }
        }
    }

    @Test
    public void testOrderedBehaviorIterable() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            StreamContext sctx = js.getStreamContext(tsc.stream);

            jsPublish(js, tsc.subject(), 101, 6, 100);
            ZonedDateTime startTime = getStartTimeFirstMessage(js, tsc);

            // New pomm factory in place before each subscription is made
            // Set the Consumer Sequence For Stream Sequence 3 statically for ease
            CS_FOR_SS_3 = 3;
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;
            _testOrderedIterate(sctx, 1, new OrderedConsumerConfiguration().filterSubject(tsc.subject()));

            CS_FOR_SS_3 = 2;
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;
            _testOrderedIterate(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartTime).startTime(startTime));

            CS_FOR_SS_3 = 2;
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;
            _testOrderedIterate(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(2));
        });
    }

    private static void _testOrderedIterate(StreamContext sctx, int expectedStreamSeq, OrderedConsumerConfiguration occ) throws Exception {
        OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);
        try (IterableConsumer icon = occtx.iterate()) {
            // Loop through the messages to make sure I get stream sequence 1 to 5
            while (expectedStreamSeq <= 5) {
                Message m = icon.nextMessage(Duration.ofSeconds(1)); // use duration version here for coverage
                if (m != null) {
                    assertEquals(expectedStreamSeq++, m.metaData().streamSequence());
                }
            }
        }
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
    public void testOrderedConsumerBuilder() throws IOException, ClassNotFoundException {
        OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration();
        check_default_values(occ);
        check_default_values(roundTripSerialize(occ));

        // COVERAGE
        check_default_values(new SerializableOrderedConsumerConfiguration().getOrderedConsumerConfiguration());

        // COVERAGE
        SerializableOrderedConsumerConfiguration socc = new SerializableOrderedConsumerConfiguration();
        socc.setOrderedConsumerConfiguration(occ);
        check_default_values(socc.getOrderedConsumerConfiguration());

        // nulls
        occ = new OrderedConsumerConfiguration()
            .filterSubject(null)
            .deliverPolicy(null)
            .replayPolicy(null)
            .headersOnly(null);
        check_default_values(occ);
        check_default_values(roundTripSerialize(occ));

        // values that set to default
        occ = new OrderedConsumerConfiguration()
            .filterSubject("")
            .startSequence(-42)
            .headersOnly(false);
        check_default_values(occ);
        check_default_values(roundTripSerialize(occ));

        // values
        ZonedDateTime zdt = DateTimeUtils.toGmt(ZonedDateTime.now());
        occ = new OrderedConsumerConfiguration()
            .filterSubject("fs")
            .deliverPolicy(DeliverPolicy.All)
            .startSequence(42)
            .startTime(zdt)
            .replayPolicy(ReplayPolicy.Original)
            .headersOnly(true);
        check_values(occ, zdt);
        check_values(roundTripSerialize(occ), zdt);

        // COVERAGE - depends on state from last occ
        socc = new SerializableOrderedConsumerConfiguration();
        socc.setOrderedConsumerConfiguration(occ);
        occ = socc.getOrderedConsumerConfiguration();
        check_values(occ, zdt);
        check_values(roundTripSerialize(occ), zdt);

        // Multiple and COVERAGE
        occ = new OrderedConsumerConfiguration().filterSubjects("fs0", "fs1");
        assertNull(occ.getFilterSubject());
        assertTrue(occ.hasMultipleFilterSubjects());
        assertNotNull(occ.getFilterSubjects());
        assertEquals("fs0", occ.getFilterSubjects().get(0));
        assertEquals("fs1", occ.getFilterSubjects().get(1));
        occ = new OrderedConsumerConfiguration(JsonParser.parse(occ.toJson()));
        assertNull(occ.getFilterSubject());
        assertTrue(occ.hasMultipleFilterSubjects());
        assertNotNull(occ.getFilterSubjects());
        assertEquals("fs0", occ.getFilterSubjects().get(0));
        assertEquals("fs1", occ.getFilterSubjects().get(1));
    }

    private static void check_default_values(OrderedConsumerConfiguration occ) {
        assertEquals(">", occ.getFilterSubject());
        assertNotNull(occ.getFilterSubject());
        assertFalse(occ.hasMultipleFilterSubjects());
        assertNull(occ.getDeliverPolicy());
        assertEquals(ConsumerConfiguration.LONG_UNSET, occ.getStartSequence());
        assertNull(occ.getStartTime());
        assertNull(occ.getReplayPolicy());
        assertNull(occ.getHeadersOnly());
    }

    private static void check_values(OrderedConsumerConfiguration occ, ZonedDateTime zdt) {
        assertEquals("fs", occ.getFilterSubject());
        assertEquals(DeliverPolicy.All, occ.getDeliverPolicy());
        assertEquals(42, occ.getStartSequence());
        assertEquals(zdt, occ.getStartTime());
        assertEquals(ReplayPolicy.Original, occ.getReplayPolicy());
        assertTrue(occ.getHeadersOnly());
    }

    private static OrderedConsumerConfiguration roundTripSerialize(OrderedConsumerConfiguration occ) throws IOException, ClassNotFoundException {
        SerializableOrderedConsumerConfiguration socc = new SerializableOrderedConsumerConfiguration(occ);
        socc = (SerializableOrderedConsumerConfiguration)roundTripSerialize(socc);
        return socc.getOrderedConsumerConfiguration();
    }

    private static Object roundTripSerialize(Serializable s) throws IOException, ClassNotFoundException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(s);
            oos.flush();
            return new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())).readObject();
        }
    }

    @Test
    public void testOverflowFetch() throws Exception {
        ListenerForTesting l = new ListenerForTesting();
        Options.Builder b = Options.builder().errorListener(l);
        jsServer.run(b, TestBase::atLeast2_11, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStream js = nc.jetStream();
            jsPublish(js, tsc.subject(), 100);

            // Testing min ack pending
            String group = variant();
            String consumer = variant();

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .name(consumer)
                .priorityPolicy(PriorityPolicy.Overflow)
                .priorityGroups(group)
                .ackWait(60_000)
                .filterSubjects(tsc.subject()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            ConsumerContext ctxPrime = nc.getConsumerContext(tsc.stream, consumer);
            ConsumerContext ctxOver = nc.getConsumerContext(tsc.stream, consumer);

            FetchConsumeOptions fcoNoMin = FetchConsumeOptions.builder()
                .maxMessages(5)
                .expiresIn(2000)
                .group(group)
                .build();

            FetchConsumeOptions fcoOverA = FetchConsumeOptions.builder()
                .maxMessages(5)
                .expiresIn(2000)
                .group(group)
                .minAckPending(5)
                .build();

            FetchConsumeOptions fcoOverB = FetchConsumeOptions.builder()
                .maxMessages(5)
                .expiresIn(2000)
                .group(group)
                .minAckPending(6)
                .build();

            _overflowFetch(ctxPrime, fcoNoMin, true, 5);
            _overflowFetch(ctxOver, fcoNoMin, true, 5);

            _overflowFetch(ctxPrime, fcoNoMin, false, 5);
            _overflowFetch(ctxOver, fcoOverA, true, 5);
            _overflowFetch(ctxOver, fcoOverB, true, 0);
        });
    }

    private static void _overflowFetch(ConsumerContext cctx, FetchConsumeOptions fco, boolean ack, int expected) throws Exception {
        try (FetchConsumer fc = cctx.fetch(fco)) {
            int count = 0;
            Message m = fc.nextMessage();
            while (m != null) {
                count++;
                if (ack) {
                    m.ack();
                }
                m = fc.nextMessage();
            }
            assertEquals(expected, count);
        }
    }

    @Test
    public void testOverflowIterate() throws Exception {
        ListenerForTesting l = new ListenerForTesting();
        Options.Builder b = Options.builder().errorListener(l);
        runInJsServer(b, TestBase::atLeast2_11, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStream js = nc.jetStream();
            jsPublish(js, tsc.subject(), 100);

            // Testing min ack pending
            String group = variant();
            String consumer = variant();

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .name(consumer)
                .priorityPolicy(PriorityPolicy.Overflow)
                .priorityGroups(group)
                .ackWait(30_000)
                .filterSubjects(tsc.subject()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            ConsumerContext ctxPrime = nc.getConsumerContext(tsc.stream, consumer);
            ConsumerContext ctxOver = nc.getConsumerContext(tsc.stream, consumer);

            ConsumeOptions coPrime = ConsumeOptions.builder()
                .group(group)
                .build();

            ConsumeOptions coOver = ConsumeOptions.builder()
                .group(group)
                .minAckPending(101)
                .build();

            // start the overflow consumer
            AtomicLong primeCount = new AtomicLong();
            AtomicLong overCount = new AtomicLong();
            AtomicLong left = new AtomicLong(100);

            Thread tOver = new Thread(() -> {
                try {
                    IterableConsumer ic = ctxOver.iterate(coOver);
                    while (left.get() > 0 && !Thread.currentThread().isInterrupted()) {
                        Message m = ic.nextMessage(100);
                        if (m != null) {
                            m.ack();
                            overCount.incrementAndGet();
                            left.decrementAndGet();
                        }
                    }
                }
                catch (InterruptedException ignore) {
                }
                catch (Exception e) {
                    fail(e);
                }
            });
            tOver.start();

            Thread tPrime = new Thread(() -> {
                try {
                    IterableConsumer ic = ctxPrime.iterate(coPrime);
                    while (left.get() > 0 && !Thread.currentThread().isInterrupted()) {
                        Message m = ic.nextMessage(100);
                        if (m != null) {
                            m.ack();
                            primeCount.incrementAndGet();
                            left.decrementAndGet();
                        }
                    }
                }
                catch (InterruptedException ignore) {
                }
                catch (Exception e) {
                    fail(e);
                }
            });
            tPrime.start();

            tPrime.join();
            tOver.join();
            assertEquals(100, primeCount.get());
            assertEquals(0, overCount.get());
        });
    }

    @Test
    public void testOverflowConsume() throws Exception {
        ListenerForTesting l = new ListenerForTesting();
        Options.Builder b = Options.builder().errorListener(l);
        runInJsServer(b, TestBase::atLeast2_11, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStream js = nc.jetStream();
            jsPublish(js, tsc.subject(), 1000);

            // Testing min ack pending
            String group = variant();
            String consumer = variant();

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .name(consumer)
                .priorityPolicy(PriorityPolicy.Overflow)
                .priorityGroups(group)
                .ackWait(30_000)
                .filterSubjects(tsc.subject()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            ConsumerContext ctxPrime = nc.getConsumerContext(tsc.stream, consumer);
            ConsumerContext ctxOver = nc.getConsumerContext(tsc.stream, consumer);

            ConsumeOptions coPrime = ConsumeOptions.builder()
                .group(group)
                .build();

            ConsumeOptions coOver = ConsumeOptions.builder()
                .group(group)
                .minAckPending(1001)
                .build();

            // start the overflow consumer
            AtomicLong primeCount = new AtomicLong();
            AtomicLong overCount = new AtomicLong();
            AtomicLong left = new AtomicLong(500);

            MessageHandler overHandler = m -> {
                m.ack();
                overCount.incrementAndGet();
                left.decrementAndGet();
            };

            MessageHandler primeHandler = m -> {
                m.ack();
                primeCount.incrementAndGet();
                left.decrementAndGet();
            };

            MessageConsumer mcOver = ctxOver.consume(coOver, overHandler);
            MessageConsumer mcPrime = ctxPrime.consume(coPrime, primeHandler);

            while (left.get() > 0) {
                sleep(100);
            }
            mcOver.stop();
            mcPrime.stop();

            assertTrue(primeCount.get() > 0);
            assertEquals(0, overCount.get());
        });
    }
}
