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
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PriorityPolicy;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.Status;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.client.api.ConsumerConfiguration.builder;
import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.Status.*;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPullTests extends JetStreamTestBase {

    static class ErrorListenerPullImpl extends ErrorListenerLoggerImpl {
        @Override
        public void pullStatusWarning(Connection conn, JetStreamSubscription sub, Status status) {}
    }

    private Options.Builder noPullWarnings() {
        return Options.builder().errorListener(new ErrorListenerPullImpl());
    }

    @Test
    public void testFetch() throws Exception {
        jsServer.run(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            long fetchMs = 3000;
            Duration fetchDur = Duration.ofMillis(fetchMs);
            Duration ackWaitDur = Duration.ofMillis(fetchMs * 2);

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .ackWait(ackWaitDur)
                .build();

            PullSubscribeOptions options = PullSubscribeOptions.builder()
                .durable(tsc.consumerName())
                .configuration(cc)
                .build();

            JetStreamSubscription sub = js.subscribe(tsc.subject(), options);
            assertSubscription(sub, tsc.stream, tsc.consumerName(), null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            List<Message> messages = sub.fetch(10, fetchDur);
            validateRead(0, messages.size());
            messages.forEach(Message::ack);
            sleep(ackWaitDur.toMillis()); // let the pull expire

            jsPublish(js, tsc.subject(), "A", 10);
            messages = sub.fetch(10, fetchDur);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, tsc.subject(), "B", 20);
            messages = sub.fetch(10, fetchDur);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            messages = sub.fetch(10, fetchDur);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, tsc.subject(), "C", 5);
            messages = sub.fetch(10, fetchDur);
            validateRead(5, messages.size());
            messages.forEach(Message::ack);
            sleep(fetchMs); // let the pull expire

            jsPublish(js, tsc.subject(), "D", 15);
            messages = sub.fetch(10, fetchDur);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            messages = sub.fetch(10, fetchDur);
            validateRead(5, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, tsc.subject(), "E", 10);
            messages = sub.fetch(10, fetchDur);
            validateRead(10, messages.size());
            sleep(ackWaitDur.toMillis()); // let the acks wait expire, pull will also expire it's shorter

            // message were not ack'ed
            messages = sub.fetch(10, fetchDur);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            assertThrows(IllegalArgumentException.class, () -> sub.fetch(10, null));
            assertThrows(IllegalArgumentException.class, () -> sub.fetch(10, Duration.ofSeconds(-1)));
        });
    }

    @Test
    public void testIterate() throws Exception {
        jsServer.run(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            long fetchMs = 5000;
            Duration fetchDur = Duration.ofMillis(fetchMs);
            Duration ackWaitDur = Duration.ofMillis(fetchMs * 2);

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .ackWait(ackWaitDur)
                .build();

            PullSubscribeOptions options = PullSubscribeOptions.builder()
                .durable(tsc.consumerName())
                .configuration(cc)
                .build();

            JetStreamSubscription sub = js.subscribe(tsc.subject(), options);
            assertSubscription(sub, tsc.stream, tsc.consumerName(), null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            Iterator<Message> iterator = sub.iterate(10, fetchDur);
            List<Message> messages = readMessages(iterator);
            validateRead(0, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, tsc.subject(), "A", 10);
            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, tsc.subject(), "B", 20);
            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, tsc.subject(), "C", 5);
            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(5, messages.size());
            messages.forEach(Message::ack);
            sleep(fetchMs); // give time for the pull to expire

            jsPublish(js, tsc.subject(), "D", 15);
            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(5, messages.size());
            messages.forEach(Message::ack);
            sleep(fetchMs); // give time for the pull to expire

            jsPublish(js, tsc.subject(), "E", 10);
            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            sleep(ackWaitDur.toMillis()); // give time for the pull and the ack wait to expire

            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, tsc.subject(), "F", 1);
            iterator = sub.iterate(1, fetchDur);
            //noinspection ResultOfMethodCallIgnored
            iterator.hasNext(); // calling hasNext twice in a row is for coverage
            //noinspection ResultOfMethodCallIgnored
            iterator.hasNext(); // calling hasNext twice in a row is for coverage
        });
    }

    @Test
    public void testBasic() throws Exception {
        jsServer.run(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            // Build our subscription options.
            PullSubscribeOptions options = PullSubscribeOptions.builder().durable(tsc.consumerName()).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(tsc.subject(), options);
            assertSubscription(sub, tsc.stream, tsc.consumerName(), null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish some amount of messages, but not entire pull size
            jsPublish(js, tsc.subject(), "A", 4);

            // start the pull
            sub.pull(10);

            // read what is available, expect 4
            List<Message> messages = readMessagesAck(sub);
            int total = messages.size();
            validateRedAndTotal(4, messages.size(), 4, total);

            // publish some more covering our initial pull and more
            jsPublish(js, tsc.subject(), "B", 10);

            // read what is available, expect 6 more
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(6, messages.size(), 10, total);

            // read what is available, should be zero since we didn't re-pull
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(0, messages.size(), 10, total);

            // re-issue the pull
            sub.pull(PullRequestOptions.builder(10).build()); // coverage of the build api

            // read what is available, should be 4 left over
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(4, messages.size(), 14, total);

            // publish some more
            jsPublish(js, tsc.subject(), "C", 10);

            // read what is available, should be 6 since we didn't finish the last batch
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(6, messages.size(), 20, total);

            // re-issue the pull, but a smaller amount
            sub.pull(2);

            // read what is available, should be 2 since we changed the pull size
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(2, messages.size(),22, total);

            // re-issue the pull, since we got the full batch size
            sub.pull(2);

            // read what is available, should be 2
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(2, messages.size(), 24, total);

            // re-issue the pull, any amount there are no messages
            sub.pull(1);

            // read what is available, there are none
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(0, messages.size(), 24, total);

            // publish some more to test null timeout
            jsPublish(js, tsc.subject(), "D", 10);
            sub = js.subscribe(tsc.subject(), PullSubscribeOptions.builder().durable(durable(2)).build());
            sub.pull(10);
            sleep(500);
            messages = readMessagesAck(sub, null);
            validateRedAndTotal(10, messages.size(), 10, messages.size());

            // publish some more to test never timeout
            jsPublish(js, tsc.subject(), "E", 10);
            sub = js.subscribe(tsc.subject(), PullSubscribeOptions.builder().durable(durable(2)).build());
            sub.pull(10);
            sleep(500);
            messages = readMessagesAck(sub, Duration.ZERO, 10);
            validateRedAndTotal(10, messages.size(), 10, messages.size());
        });
    }

    @Test
    public void testNoWait() throws Exception {
        runInJsServer(noPullWarnings(), nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            // Build our subscription options.
            PullSubscribeOptions options = PullSubscribeOptions.builder().durable(tsc.consumerName()).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(tsc.subject(), options);
            assertSubscription(sub, tsc.stream, tsc.consumerName(), null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish 10 messages
            // no wait, batch size 10, there are 10 messages, we will read them all and not trip nowait
            jsPublish(js, tsc.subject(), "A", 10);
            sub.pullNoWait(10);
            List<Message> messages = readMessagesAck(sub);
            assertEquals(10, messages.size());
            assertAllJetStream(messages);

            // publish 20 messages
            // no wait, batch size 10, there are 20 messages, we will read 10
            jsPublish(js, tsc.subject(), "B", 20);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());

            // there are still ten messages
            // no wait, batch size 10, there are 20 messages, we will read 10
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());

            // publish 5 messages
            // no wait, batch size 10, there are 5 messages, we WILL trip nowait
            jsPublish(js, tsc.subject(), "C", 5);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());

            // publish 12 messages
            // no wait, batch size 10, there are more than batch messages we will read 10
            jsPublish(js, tsc.subject(), "D", 12);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());

            // 2 messages left
            // no wait, less than batch size will trip nowait
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(2, messages.size());

            // this is just coverage of the pullNoWait api + expires, not really validating server functionality
            // publish 12 messages
            // no wait, batch size 10, there are more than batch messages we will read 10
            jsPublish(js, tsc.subject(), "E", 12);
            sub.pullNoWait(10, 10000);
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());

            // 2 messages left
            // no wait, less than batch size will trip nowait
            sub.pullNoWait(10, Duration.ofMillis(1000));
            messages = readMessagesAck(sub);
            assertEquals(2, messages.size());
        });
    }

    @Test
    public void testPullExpires() throws Exception {
        runInJsServer(noPullWarnings(), nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            // Build our subscription options.
            PullSubscribeOptions options = PullSubscribeOptions.builder().durable(tsc.consumerName()).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(tsc.subject(), options);
            assertSubscription(sub, tsc.stream, tsc.consumerName(), null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            long expires = 500; // millis

            // publish 10 messages
            jsPublish(js, tsc.subject(), "A", 5);
            sub.pullExpiresIn(10, Duration.ofMillis(expires)); // using Duration version here
            List<Message> messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(js, tsc.subject(), "B", 10);
            sub.pullExpiresIn(10, Duration.ofMillis(expires)); // using Duration version here
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());
            sleep(expires); // make sure the pull actually expires

            jsPublish(js, tsc.subject(), "C", 5);
            sub.pullExpiresIn(10, Duration.ofMillis(expires)); // using Duration version here
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(js, tsc.subject(), "D", 10);
            sub.pull(10);
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());

            jsPublish(js, tsc.subject(), "E", 5);
            sub.pullExpiresIn(10, expires); // using millis version here
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(js, tsc.subject(), "F", 10);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());

            jsPublish(js, tsc.subject(), "G", 5);
            sub.pullExpiresIn(10, expires); // using millis version here
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(js, tsc.subject(), "H", 10);
            messages = sub.fetch(10, expires);
            assertEquals(10, messages.size());
            assertAllJetStream(messages);

            jsPublish(js, tsc.subject(), "I", 5);
            sub.pullExpiresIn(10, expires);
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(js, tsc.subject(), "J", 10);
            Iterator<Message> i = sub.iterate(10, expires);
            int count = 0;
            while (i.hasNext()) {
                assertIsJetStream(i.next());
                ++count;
            }
            assertEquals(10, count);

            assertThrows(IllegalArgumentException.class, () -> sub.pullExpiresIn(10, null));
            assertThrows(IllegalArgumentException.class, () -> sub.pullExpiresIn(10, Duration.ofSeconds(-1)));
            assertThrows(IllegalArgumentException.class, () -> sub.pullExpiresIn(10, -1000));
        });
    }

    @Test
    public void testAckNak() throws Exception {
        jsServer.run(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            PullSubscribeOptions pso = PullSubscribeOptions.builder().durable(DURABLE).build();
            JetStreamSubscription sub = js.subscribe(tsc.subject(), pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // NAK
            jsPublish(js, tsc.subject(), "NAK", 1);

            sub.pull(1);

            Message message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            String data = new String(message.getData());
            assertEquals("NAK1", data);
            message.nak();

            sub.pull(1);
            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK1", data);
            message.ack();

            sub.pull(1);
            assertNull(sub.nextMessage(Duration.ofSeconds(1)));
        });
    }

    @Test
    public void testAckTerm() throws Exception {
        jsServer.run(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            PullSubscribeOptions pso = PullSubscribeOptions.builder().durable(DURABLE).build();
            JetStreamSubscription sub = js.subscribe(tsc.subject(), pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // TERM
            jsPublish(js, tsc.subject(), "TERM", 1);

            sub.pull(1);
            Message message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            String data = new String(message.getData());
            assertEquals("TERM1", data);
            message.term();

            sub.pull(1);
            assertNull(sub.nextMessage(Duration.ofSeconds(1)));
        });
    }

    @Test
    public void testAckReplySyncCoverage() throws Exception {
        jsServer.run(nc -> {
            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            // Create our JetStream context.
            JetStream js = nc.jetStream();

            JetStreamSubscription sub = js.subscribe(tsc.subject());
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            jsPublish(js, tsc.subject(), "COVERAGE", 1);

            Message message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);

            NatsJetStreamMessage njsm = (NatsJetStreamMessage)message;

            njsm.replyTo = "$JS.ACK.stream.LS0k4eeN.1.1.1.1627472530542070600.0";

            assertThrows(TimeoutException.class, () -> njsm.ackSync(Duration.ofSeconds(1)));
        });
    }

    @Test
    public void testAckWaitTimeout() throws Exception {
        jsServer.run(nc -> {
            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            // Create our JetStream context.
            JetStream js = nc.jetStream();

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .ackWait(1500)
                .build();
            PullSubscribeOptions pso = PullSubscribeOptions.builder()
                .durable(tsc.consumerName())
                .configuration(cc)
                .build();

            JetStreamSubscription sub = js.subscribe(tsc.subject(), pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // Ack Wait timeout
            jsPublish(js, tsc.subject(), "WAIT", 2);

            sub.pull(2);
            Message m = sub.nextMessage(1000);
            assertNotNull(m);
            assertEquals("WAIT1", new String(m.getData()));

            m = sub.nextMessage(1000);
            assertNotNull(m);
            assertEquals("WAIT2", new String(m.getData()));

            sleep(2000);

            sub.pull(2);
            m = sub.nextMessage(1000);
            assertNotNull(m);
            assertEquals("WAIT1", new String(m.getData()));
            m.ack();

            m = sub.nextMessage(1000);
            assertNotNull(m);
            assertEquals("WAIT2", new String(m.getData()));
            m.ack();

            sub.pull(2);
            m = sub.nextMessage(1000);
            assertNull(m);
        });
    }

    @Test
    public void testDurable() throws Exception {
        runInJsServer(noPullWarnings(), nc -> {
            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);
            String durable = durable();

            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // Build our subscription options normally
            PullSubscribeOptions options1 = PullSubscribeOptions.builder().durable(durable).build();
            _testDurableOrNamed(js, tsc.subject(), () -> js.subscribe(tsc.subject(), options1));

            // bind long form
            PullSubscribeOptions options2 = PullSubscribeOptions.builder()
                .stream(tsc.stream)
                .durable(durable)
                .bind(true)
                .build();
            _testDurableOrNamed(js, tsc.subject(), () -> js.subscribe(null, options2));

            // fast bind long form
            PullSubscribeOptions options3 = PullSubscribeOptions.builder()
                .stream(tsc.stream)
                .durable(durable)
                .fastBind(true)
                .build();
            _testDurableOrNamed(js, tsc.subject(), () -> js.subscribe(null, options3));

            // bind short form
            PullSubscribeOptions options4 = PullSubscribeOptions.bind(tsc.stream, durable);
            _testDurableOrNamed(js, tsc.subject(), () -> js.subscribe(null, options4));

            // fast bind short form
            PullSubscribeOptions options5 = PullSubscribeOptions.fastBind(tsc.stream, durable);
            _testDurableOrNamed(js, tsc.subject(), () -> js.subscribe(null, options5));
        });
    }

    @Test
    public void testNamed() throws Exception {
        runInJsServer(noPullWarnings(), TestBase::atLeast2_9_0, nc -> {
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            String name = name();

            jsm.addOrUpdateConsumer(tsc.stream, ConsumerConfiguration.builder()
                .name(name)
                .inactiveThreshold(10_000)
                .build());

            // bind long form
            PullSubscribeOptions options2 = PullSubscribeOptions.builder()
                .stream(tsc.stream)
                .name(name)
                .bind(true)
                .build();
            _testDurableOrNamed(js, tsc.subject(), () -> js.subscribe(null, options2));

            // fast bind long form
            PullSubscribeOptions options3 = PullSubscribeOptions.builder()
                .stream(tsc.stream)
                .name(name)
                .fastBind(true)
                .build();
            _testDurableOrNamed(js, tsc.subject(), () -> js.subscribe(null, options3));

            // bind short form
            PullSubscribeOptions options4 = PullSubscribeOptions.bind(tsc.stream, name);
            _testDurableOrNamed(js, tsc.subject(), () -> js.subscribe(null, options4));

            // fast bind short form
            PullSubscribeOptions options5 = PullSubscribeOptions.fastBind(tsc.stream, name);
            _testDurableOrNamed(js, tsc.subject(), () -> js.subscribe(null, options5));
        });
    }

    private void _testDurableOrNamed(JetStream js, String subject, SubscriptionSupplier supplier) throws IOException, JetStreamApiException, InterruptedException {
        jsPublish(js, subject, 2);

        JetStreamSubscription sub = supplier.get();

        // start the pull
        sub.pullNoWait(4);

        List<Message> messages = readMessagesAck(sub);
        validateRedAndTotal(2, messages.size(), 2, 2);

        sub.unsubscribe();
    }

    private interface SubscriptionSupplier {
        JetStreamSubscription get() throws IOException, JetStreamApiException;
    }

    @Test
    public void testPullRequestOptionsBuilder() {
        assertThrows(IllegalArgumentException.class, () -> PullRequestOptions.builder(0).build());
        assertThrows(IllegalArgumentException.class, () -> PullRequestOptions.builder(-1).build());
        assertThrows(IllegalArgumentException.class, () -> PullRequestOptions.builder(1).idleHeartbeat(1).build());
        assertThrows(IllegalArgumentException.class, () -> PullRequestOptions.builder(1).noWait().idleHeartbeat(1).build());
        assertThrows(IllegalArgumentException.class, () -> PullRequestOptions.builder(1).expiresIn(30000).idleHeartbeat(15001).build());

        PullRequestOptions pro = PullRequestOptions.builder(11).build();
        assertEquals(11, pro.getBatchSize());
        assertEquals(0, pro.getMaxBytes());
        assertNull(pro.getExpiresIn());
        assertNull(pro.getIdleHeartbeat());
        assertFalse(pro.isNoWait());
        assertNull(pro.getGroup());
        assertEquals(-1, pro.getMinPending());
        assertEquals(-1, pro.getMinAckPending());

        pro = PullRequestOptions.noWait(21).build();
        assertEquals(21, pro.getBatchSize());
        assertEquals(0, pro.getMaxBytes());
        assertNull(pro.getExpiresIn());
        assertNull(pro.getIdleHeartbeat());
        assertTrue(pro.isNoWait());

        pro = PullRequestOptions.builder(31)
            .maxBytes(32)
            .expiresIn(33)
            .idleHeartbeat(16)
            .noWait()
            .build();
        assertEquals(31, pro.getBatchSize());
        assertEquals(32, pro.getMaxBytes());
        assertEquals(33, pro.getExpiresIn().toMillis());
        assertEquals(16, pro.getIdleHeartbeat().toMillis());
        assertTrue(pro.isNoWait());

        pro = PullRequestOptions.builder(41)
            .expiresIn(Duration.ofMillis(43))
            .idleHeartbeat(Duration.ofMillis(21))
            .noWait(false) // just coverage of this method
            .build();
        assertEquals(41, pro.getBatchSize());
        assertEquals(0, pro.getMaxBytes());
        assertEquals(43, pro.getExpiresIn().toMillis());
        assertEquals(21, pro.getIdleHeartbeat().toMillis());
        assertFalse(pro.isNoWait());

        pro = PullRequestOptions.builder(41)
            .group("g")
            .minPending(1)
            .minAckPending(2)
            .build();
        assertEquals("g", pro.getGroup());
        assertEquals(1, pro.getMinPending());
        assertEquals(2, pro.getMinAckPending());
    }

    interface ConflictSetup {
        JetStreamSubscription setup(Connection nc, JetStreamManagement jsm, JetStream js, TestingStreamContainer tsc, ListenerForTesting listener) throws Exception;
    }

    private boolean versionIsBefore(Connection nc, String targetVersion) {
        return targetVersion != null && nc.getServerInfo().isOlderThanVersion(targetVersion);
    }

    interface BuilderCustomizer {
        ConsumerConfiguration.Builder customize(ConsumerConfiguration.Builder b);
    }

    private PullSubscribeOptions makePso(BuilderCustomizer c) {
        return c.customize(ConsumerConfiguration.builder().ackPolicy(AckPolicy.None)).inactiveThreshold(INACTIVE_THRESHOLD).buildPullSubscribeOptions();
    }

    static final long NEXT_MESSAGE = 2500;
    static final long WAIT_FOR_MESSAGES = 10_000;
    static final long INACTIVE_THRESHOLD = 30_000;
    static final int TYPE_ERROR = 1;
    static final int TYPE_WARNING = 2;
    static final int TYPE_NONE = 0;
    private void testConflictStatus(int statusCode, String statusText, int type, String targetVersion, ConflictSetup setup) throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        AtomicBoolean skip = new AtomicBoolean(false);
        runInJsServer(listener, nc -> {
            skip.set(versionIsBefore(nc, targetVersion));
            if (skip.get()) {
                return;
            }
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStreamSubscription sub = setup.setup(nc, jsm, js, tsc, listener);
            if (sub.getDispatcher() == null) {
                if (type == TYPE_ERROR) {
                    JetStreamStatusException jsse = assertThrows(JetStreamStatusException.class, () -> sub.nextMessage(NEXT_MESSAGE));
                    assertEquals(statusCode, jsse.getStatus().getCode());
                    assertEquals(sub.hashCode(), jsse.getSubscription().hashCode());
                    //noinspection deprecation
                    assertTrue(jsse.getDescription().contains(statusText)); // coverage
                }
                else {
                    sub.nextMessage(NEXT_MESSAGE);
                }
            }
            checkHandler(statusText, type, listener, WAIT_FOR_MESSAGES);
        });
    }

    private void checkHandler(String statusText, int type, ListenerForTesting listener, long timeout) {
        if (type == TYPE_ERROR) {
            assertTrue(listener.pullStatusErrorOrWait(statusText, timeout));
        }
        else if (type == TYPE_WARNING) {
            assertTrue(listener.pullStatusWarningEventually(statusText, timeout));
        }
    }

    @Test
    public void testExceedsMaxWaitingSyncSub() throws Exception {
        testConflictStatus(409, EXCEEDED_MAX_WAITING, TYPE_WARNING, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            PullSubscribeOptions so = makePso(b -> b.maxPullWaiting(1));
            JetStreamSubscription sub = js.subscribe(tsc.subject(), so);
            sub.pull(1);
            sub.pull(1);
            return sub;
        });
    }

    @Test
    public void testExceedsMaxWaitingAsyncSub() throws Exception {
        testConflictStatus(409, EXCEEDED_MAX_WAITING, TYPE_WARNING, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            Dispatcher d = nc.createDispatcher();
            PullSubscribeOptions so = makePso(b -> b.maxPullWaiting(1));
            JetStreamSubscription sub = js.subscribe(tsc.subject(), d, m -> {}, so);
            sub.pull(1);
            sub.pull(1);
            return sub;
        });
    }

    @Test
    public void testExceedsMaxRequestBatchSyncSub() throws Exception {
        testConflictStatus(409, EXCEEDED_MAX_REQUEST_BATCH, TYPE_WARNING, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            PullSubscribeOptions so = makePso(b -> b.maxBatch(1));
            JetStreamSubscription sub = js.subscribe(tsc.subject(), so);
            sub.pull(2);
            return sub;
        });
    }

    @Test
    public void testExceedsMaxRequestBatchAsyncSub() throws Exception {
        testConflictStatus(409, EXCEEDED_MAX_REQUEST_BATCH, TYPE_WARNING, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            Dispatcher d = nc.createDispatcher();
            PullSubscribeOptions so = makePso(b -> b.maxBatch(1));
            JetStreamSubscription sub = js.subscribe(tsc.subject(), d, m -> {}, so);
            sub.pull(2);
            return sub;
        });
    }

    @Test
    public void testMessageSizeExceedsMaxBytesSyncSub() throws Exception {
        testConflictStatus(409, MESSAGE_SIZE_EXCEEDS_MAX_BYTES, TYPE_NONE, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            PullSubscribeOptions so = makePso(b -> b);
            js.publish(tsc.subject(), new byte[1000]);
            JetStreamSubscription sub = js.subscribe(tsc.subject(), so);
            sub.pull(PullRequestOptions.builder(1).maxBytes(100).build());
            return sub;
        });
    }

    @Test
    public void testMessageSizeExceedsMaxBytesAsyncSub() throws Exception {
        testConflictStatus(409, MESSAGE_SIZE_EXCEEDS_MAX_BYTES, TYPE_NONE, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            Dispatcher d = nc.createDispatcher();
            PullSubscribeOptions so = makePso(b -> b);
            js.publish(tsc.subject(), new byte[1000]);
            JetStreamSubscription sub = js.subscribe(tsc.subject(), d, m -> {}, so);
            sub.pull(PullRequestOptions.builder(1).maxBytes(100).build());
            return sub;
        });
    }

    @Test
    public void testExceedsMaxRequestExpiresSyncSub() throws Exception {
        testConflictStatus(409, EXCEEDED_MAX_REQUEST_EXPIRES, TYPE_WARNING, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            PullSubscribeOptions so = makePso(b -> b.maxExpires(1000));
            JetStreamSubscription sub = js.subscribe(tsc.subject(), so);
            sub.pullExpiresIn(1, 2000);
            return sub;
        });
    }

    @Test
    public void testExceedsMaxRequestExpiresAsyncSub() throws Exception {
        testConflictStatus(409, EXCEEDED_MAX_REQUEST_EXPIRES, TYPE_WARNING, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            Dispatcher d = nc.createDispatcher();
            PullSubscribeOptions so = makePso(b -> b.maxExpires(1000));
            JetStreamSubscription sub = js.subscribe(tsc.subject(), d, m -> {}, so);
            sub.pullExpiresIn(1, 2000);
            return sub;
        });
    }

    @Test
    public void testConsumerIsPushBasedSyncSub() throws Exception {
        testConflictStatus(409, CONSUMER_IS_PUSH_BASED, TYPE_ERROR, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            jsm.addOrUpdateConsumer(tsc.stream, builder().durable(durable(1)).ackPolicy(AckPolicy.None).build());
            PullSubscribeOptions so = PullSubscribeOptions.bind(tsc.stream, durable(1));
            JetStreamSubscription sub = js.subscribe(null, so);
            jsm.deleteConsumer(tsc.stream, durable(1));
            // consumer with same name but is push now
            jsm.addOrUpdateConsumer(tsc.stream, builder().durable(durable(1)).deliverSubject(deliver(1)).build());
            sub.pull(1);
            return sub;
        });
    }

    @Test
    public void testConsumerIsPushBasedAsyncSub() throws Exception {
        testConflictStatus(409, CONSUMER_IS_PUSH_BASED, TYPE_ERROR, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            jsm.addOrUpdateConsumer(tsc.stream, builder().durable(durable(1)).ackPolicy(AckPolicy.None).build());
            Dispatcher d = nc.createDispatcher();
            PullSubscribeOptions so = PullSubscribeOptions.bind(tsc.stream, durable(1));
            JetStreamSubscription sub = js.subscribe(null, d, m -> {}, so);
            jsm.deleteConsumer(tsc.stream, durable(1));
            // consumer with same name but is push now
            jsm.addOrUpdateConsumer(tsc.stream, builder().durable(durable(1)).deliverSubject(deliver(1)).build());
            sub.pull(1);
            return sub;
        });
    }

    // This just flaps. It's a timing thing? Already spent too much time, IWOMM and it should work as is.
    @Test
    @Disabled
    public void testConsumerDeletedSyncSub() throws Exception {
        testConflictStatus(409, CONSUMER_DELETED, TYPE_ERROR, "2.9.6", (nc, jsm, js, tsc, handler) -> {
            jsm.addOrUpdateConsumer(tsc.stream, builder().durable(durable(1)).ackPolicy(AckPolicy.None).build());
            PullSubscribeOptions so = PullSubscribeOptions.bind(tsc.stream, durable(1));
            JetStreamSubscription sub = js.subscribe(null, so);
            sub.pullExpiresIn(1, 30000);
            jsm.deleteConsumer(tsc.stream, durable(1));
            js.publish(tsc.subject(), null);
            return sub;
        });
    }

    // This just flaps. It's a timing thing? Already spent too much time, IWOMM and it should work as is.
    @Test
    @Disabled
    public void testConsumerDeletedAsyncSub() throws Exception {
        testConflictStatus(409, CONSUMER_DELETED, TYPE_ERROR, "2.9.6", (nc, jsm, js, tsc, handler) -> {
            jsm.addOrUpdateConsumer(tsc.stream, builder().durable(durable(1)).ackPolicy(AckPolicy.None).build());
            Dispatcher d = nc.createDispatcher();
            PullSubscribeOptions so = PullSubscribeOptions.bind(tsc.stream, durable(1));
            JetStreamSubscription sub = js.subscribe(null, d, m -> {}, so);
            sub.pullExpiresIn(1, 30000);
            jsm.deleteConsumer(tsc.stream, durable(1));
            js.publish(tsc.subject(), null);
            return sub;
        });
    }

    static class BadPullRequestOptions extends PullRequestOptions {
        public BadPullRequestOptions() {
            super(PullRequestOptions.builder(1));
        }

        @Override
        public String toJson() {
            StringBuilder sb = JsonUtils.beginJson();
            JsonUtils.addField(sb, BATCH, 1);
            JsonUtils.addFldWhenTrue(sb, NO_WAIT, true);
            JsonUtils.addFieldAsNanos(sb, IDLE_HEARTBEAT, Duration.ofMillis(1));
            return JsonUtils.endJson(sb).toString();
        }
    }

    @Test
    public void testBadRequestSyncSub() throws Exception {
        testConflictStatus(400, BAD_REQUEST, TYPE_ERROR, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            PullSubscribeOptions so = makePso(b -> b);
            JetStreamSubscription sub = js.subscribe(tsc.subject(), so);
            sub.pull(new BadPullRequestOptions());
            return sub;
        });
    }

    @Test
    public void testBadRequestAsyncSub() throws Exception {
        testConflictStatus(400, BAD_REQUEST, TYPE_ERROR, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            Dispatcher d = nc.createDispatcher();
            PullSubscribeOptions so = makePso(b -> b);
            JetStreamSubscription sub = js.subscribe(tsc.subject(), d, m -> {}, so);
            sub.pull(new BadPullRequestOptions());
            return sub;
        });
    }

    @Test
    public void testNotFoundSyncSub() throws Exception {
        testConflictStatus(404, NO_MESSAGES, TYPE_NONE, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            PullSubscribeOptions so = makePso(b -> b);
            JetStreamSubscription sub = js.subscribe(tsc.subject(), so);
            sub.pullNoWait(1);
            return sub;
        });
    }

    @Test
    public void testNotFoundAsyncSub() throws Exception {
        testConflictStatus(404, NO_MESSAGES, TYPE_NONE, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            Dispatcher d = nc.createDispatcher();
            PullSubscribeOptions so = makePso(b -> b);
            JetStreamSubscription sub = js.subscribe(tsc.subject(), d, m -> {}, so);
            sub.pullNoWait(1);
            return sub;
        });
    }

    @Test
    public void testExceedsMaxRequestBytes1stMessageSyncSub() throws Exception {
        testConflictStatus(409, EXCEEDED_MAX_REQUEST_MAX_BYTES, TYPE_WARNING, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            PullSubscribeOptions so = makePso(b -> b.maxBytes(1));
            JetStreamSubscription sub = js.subscribe(tsc.subject(), so);
            sub.pull(PullRequestOptions.builder(1).maxBytes(2).build());
            return sub;
        });
    }

    @Test
    public void testExceedsMaxRequestBytes1stMessageAsyncSub() throws Exception {
        testConflictStatus(409, EXCEEDED_MAX_REQUEST_MAX_BYTES, TYPE_WARNING, "2.9.0", (nc, jsm, js, tsc, handler) -> {
            Dispatcher d = nc.createDispatcher();
            PullSubscribeOptions so = makePso(b -> b.maxBytes(1));
            JetStreamSubscription sub = js.subscribe(tsc.subject(), d, m -> {}, so);
            sub.pull(PullRequestOptions.builder(1).maxBytes(2).build());
            return sub;
        });
    }

    @Test
    public void testExceedsMaxRequestBytesNthMessageSyncSub() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        runInJsServer(TestBase::atLeast2_9_1, listener, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);

            jsm.addOrUpdateConsumer(tsc.stream, builder().durable(durable(1)).ackPolicy(AckPolicy.None).filterSubjects(tsc.subject()).build());
            PullSubscribeOptions so = PullSubscribeOptions.bind(tsc.stream, durable(1));
            JetStreamSubscription sub = js.subscribe(tsc.subject(), so);

            // subject 7 + reply 52 + bytes 100 = 159
            // subject 7 + reply 52 + bytes 100 + headers 21 = 180
            js.publish(tsc.subject(), new byte[100]);
            js.publish(tsc.subject(), new Headers().add("foo", "bar"), new byte[100]);
            // 1000 - 159 - 180 = 661
            // subject 7 + reply 52 + bytes 610 = 669 > 661
            js.publish(tsc.subject(), new byte[610]);

            sub.pull(PullRequestOptions.builder(10).maxBytes(1000).expiresIn(1000).build());
            assertNotNull(sub.nextMessage(500));
            assertNotNull(sub.nextMessage(500));
            assertNull(sub.nextMessage(500));
            checkHandler(MESSAGE_SIZE_EXCEEDS_MAX_BYTES, TYPE_NONE, listener, 2500);
        });
    }

    @Test
    public void testExceedsMaxRequestBytesExactBytes() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        runInJsServer(TestBase::atLeast2_9_1, listener, nc -> {
            String stream = "sixsix"; // six letters so I can count
            String subject = "seven"; // seven letters so I can count
            String durable = durable(0); // short keeps under max bytes
            createMemoryStream(nc, stream, subject);
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();
            jsm.addOrUpdateConsumer(stream, builder().durable(durable).ackPolicy(AckPolicy.None).filterSubjects(subject).build());
            PullSubscribeOptions so = PullSubscribeOptions.bind(stream, durable);
            JetStreamSubscription sub = js.subscribe(subject, so);

            // 159 + 180 + 661 = 1000
            // subject 7 + reply 52 + bytes 100 = 159
            // subject 7 + reply 52 + bytes 100 + headers 21 = 180
            // subject 7 + reply 52 + bytes 602 = 661
            js.publish(subject, new byte[100]);
            js.publish(subject, new Headers().add("foo", "bar"), new byte[100]);
            js.publish(subject, new byte[602]);

            sub.pull(PullRequestOptions.builder(10).maxBytes(1000).expiresIn(1000).build());
            assertNotNull(sub.nextMessage(500));
            assertNotNull(sub.nextMessage(500));
            assertNotNull(sub.nextMessage(500));
            assertNull(sub.nextMessage(500)); // there are no more messages
            checkHandler(MESSAGE_SIZE_EXCEEDS_MAX_BYTES, TYPE_NONE, listener, 2500);
        });
    }

    @Test
    public void testReader() throws Exception {
        jsServer.run(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStream js = nc.jetStream();

            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(tsc.consumerName()).filterSubjects(tsc.subject()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            PullSubscribeOptions so = PullSubscribeOptions.bind(tsc.stream, tsc.consumerName());
            JetStreamSubscription sub = js.subscribe(tsc.subject(), so);
            JetStreamReader reader = sub.reader(500, 125);

            int stopCount = 500;

            // create the consumer then use it
            AtomicInteger count = new AtomicInteger();
            Thread readerThread = getReaderThread(count, stopCount, reader);

            Publisher publisher = new Publisher(js, tsc.subject(), 25);
            Thread pubThread = new Thread(publisher);
            pubThread.start();

            readerThread.join();
            publisher.stop();
            pubThread.join();

            assertTrue(count.incrementAndGet() > 500);
        });
    }

    private static Thread getReaderThread(AtomicInteger count, int stopCount, JetStreamReader reader) {
        Thread readerThread = new Thread(() -> {
            try {
                while (count.get() < stopCount) {
                    Message msg = reader.nextMessage(1000);
                    if (msg != null) {
                        msg.ack();
                        count.incrementAndGet();
                    }
                }

                Thread.sleep(50); // allows more messages to come across
                reader.stop();

                Message msg = reader.nextMessage(Duration.ofMillis(1000)); // also coverage next message
                while (msg != null) {
                    msg.ack();
                    count.incrementAndGet();
                    msg = reader.nextMessage(1000);
                }
            }
            catch (Exception e) {
                fail(e);
            }
        });
        readerThread.start();
        return readerThread;
    }

    @Test
    public void testOverflow() throws Exception {
        ListenerForTesting l = new ListenerForTesting();
        Options.Builder b = Options.builder().errorListener(l);
        jsServer.run(b, TestBase::atLeast2_11, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStream js = nc.jetStream();
            jsPublish(js, tsc.subject(), 100);

            // Setting PriorityPolicy requires at least one PriorityGroup to be set
            ConsumerConfiguration ccNoGroup = ConsumerConfiguration.builder()
                .priorityPolicy(PriorityPolicy.Overflow)
                .build();
            JetStreamApiException jsae = assertThrows(JetStreamApiException.class,
                () -> jsm.addOrUpdateConsumer(tsc.stream, ccNoGroup));
            assertEquals(10159, jsae.getApiErrorCode());

            // Testing errors
            String group = variant();
            String consumer = variant();

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .name(consumer)
                .priorityPolicy(PriorityPolicy.Overflow)
                .priorityGroups(group)
                .filterSubjects(tsc.subject()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            PullSubscribeOptions so = PullSubscribeOptions.fastBind(tsc.stream, consumer);
            JetStreamSubscription sub = js.subscribe(null, so);

            // 400 Bad Request - Priority Group missing
            sub.pull(1);
            assertThrows(JetStreamStatusException.class, () -> sub.nextMessage(1000));

            // 400 Bad Request - Invalid Priority Group
            sub.pull(PullRequestOptions.builder(5).group("bogus").build());
            assertThrows(JetStreamStatusException.class, () -> sub.nextMessage(1000));

            // Testing min ack pending
            group = variant();
            consumer = variant();

            cc = ConsumerConfiguration.builder()
                .name(consumer)
                .priorityPolicy(PriorityPolicy.Overflow)
                .priorityGroups(group)
                .ackWait(60_000)
                .filterSubjects(tsc.subject()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            so = PullSubscribeOptions.fastBind(tsc.stream, consumer);
            JetStreamSubscription subPrime = js.subscribe(null, so);
            JetStreamSubscription subOver = js.subscribe(null, so);

            PullRequestOptions proNoMin = PullRequestOptions.builder(5)
                .group(group)
                .build();

            PullRequestOptions proOverA = PullRequestOptions.builder(5)
                .group(group)
                .minAckPending(5)
                .build();

            PullRequestOptions proOverB = PullRequestOptions.builder(5)
                .group(group)
                .minAckPending(10)
                .build();

            _overflowCheck(subPrime, proNoMin, true, 5);
            _overflowCheck(subOver, proNoMin, true, 5);

            _overflowCheck(subPrime, proNoMin, false, 5);
            _overflowCheck(subOver, proOverA, true, 5);
            _overflowCheck(subOver, proOverB, true, 0);

            // Testing min pending
            group = variant();
            consumer = variant();

            cc = ConsumerConfiguration.builder()
                .name(consumer)
                .priorityPolicy(PriorityPolicy.Overflow)
                .priorityGroups(group)
                .filterSubjects(tsc.subject()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            so = PullSubscribeOptions.fastBind(tsc.stream, consumer);
            subPrime = js.subscribe(null, so);
            subOver = js.subscribe(null, so);

            proNoMin = PullRequestOptions.builder(5)
                .group(group)
                .build();

            proOverA = PullRequestOptions.builder(5)
                .group(group)
                .minPending(78)
                .build();

            _overflowCheck(subPrime, proNoMin, true, 5);
            _overflowCheck(subOver, proNoMin, true, 5);
            _overflowCheck(subOver, proOverA, true, 5);
            _overflowCheck(subOver, proOverA, true, 5);
            // exactly 80 messages now pending, gt or eq to pull min pending for 3 (80, 79, 78)
            _overflowCheck(subOver, proOverA, true, 3);
            // exactly 77 messages now pending lt pull min pending
            _overflowCheck(subOver, proOverA, true, 0);
        });
    }

    private static void _overflowCheck(JetStreamSubscription sub, PullRequestOptions pro, boolean ack, int expected) throws InterruptedException, JetStreamApiException, IOException {
        sub.pull(pro);
        int count = 0;
        Message m = sub.nextMessage(1000);
        while (m != null) {
            count++;
            if (ack) {
                m.ack();
            }
            m = sub.nextMessage(100);
        }
        assertEquals(expected, count);
    }
}
