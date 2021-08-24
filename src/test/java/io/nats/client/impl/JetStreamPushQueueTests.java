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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JetStreamPushQueueTests extends JetStreamTestBase {

    @Test
    public void testQueueSub() throws Exception {
        runInJsServer(nc -> {
            if (nc.getServerInfo().isNewerVersionThan("2.3.4")) {
                System.out.println("This test will not work after server version v2.3.4");
                return;
            }

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Setup the subscribers
            // - the PushSubscribeOptions can be re-used since all the subscribers are the same
            // - use a concurrent integer to track all the messages received
            // - have a list of subscribers and threads so I can track them
            PushSubscribeOptions pso = PushSubscribeOptions.builder().durable(DURABLE).build();
            AtomicInteger allReceived = new AtomicInteger();
            List<JsQueueSubscriber> subscribers = new ArrayList<>();
            List<Thread> subThreads = new ArrayList<>();
            for (int id = 1; id <= 3; id++) {
                // setup the subscription
                JetStreamSubscription sub = js.subscribe(SUBJECT, QUEUE, pso);
                // create and track the runnable
                JsQueueSubscriber qs = new JsQueueSubscriber(100, js, sub, allReceived);
                subscribers.add(qs);
                // create, track and start the thread
                Thread t = new Thread(qs);
                subThreads.add(t);
                t.start();
            }
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // create and start the publishing
            Thread pubThread = new Thread(new JsPublisher(js, 100));
            pubThread.start();

            // wait for all threads to finish
            pubThread.join(1000, 0);
            for (Thread t : subThreads) {
                t.join(1000, 0);
            }

            // count
            int count = 0;
            for (JsQueueSubscriber qs : subscribers) {
                int c = qs.thisReceived.get();
                assertTrue(c > 0);
                count += c;
            }

            assertEquals(100, count);
        });
    }

    static class JsPublisher implements Runnable {
        JetStream js;
        int msgCount;

        public JsPublisher(JetStream js, int msgCount) {
            this.js = js;
            this.msgCount = msgCount;
        }

        @Override
        public void run() {
            for (int x = 1; x <= msgCount; x++) {
                try {
                    js.publish(SUBJECT, ("Data # " + x).getBytes(StandardCharsets.US_ASCII));
                } catch (IOException | JetStreamApiException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    static class JsQueueSubscriber implements Runnable {
        int msgCount;
        JetStream js;
        JetStreamSubscription sub;
        AtomicInteger allReceived;
        AtomicInteger thisReceived;

        public JsQueueSubscriber(int msgCount, JetStream js, JetStreamSubscription sub, AtomicInteger allReceived) {
            this.msgCount = msgCount;
            this.js = js;
            this.sub = sub;
            this.allReceived = allReceived;
            this.thisReceived = new AtomicInteger();
        }

        @Override
        public void run() {
            while (allReceived.get() < msgCount) {
                try {
                    Message msg = sub.nextMessage(Duration.ofMillis(500));
                    while (msg != null) {
                        thisReceived.incrementAndGet();
                        allReceived.incrementAndGet();
                        msg.ack();
                        msg = sub.nextMessage(Duration.ofMillis(500));
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
