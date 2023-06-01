// Copyright 2021 The NATS Authors
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

package io.nats.examples.jetstream;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PublishAck;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.examples.jetstream.NatsJsUtils.createStreamExitWhenExists;

/**
 * This example will demonstrate JetStream pull subscribing using a durable consumer
 * and sharing processing of the messages.
 * NOTE: This example has multiple subscriptions for the same consumer.
 * Typically, those subscriptions would be running in their own JVM,
 * or their own machine instance, etc. in order to horizontally scale them.
 * This example just demonstrates the functionality of the subscriptions,
 * not how a real application would typically work.
 * That being said, a typical application may indeed have multiple consumers
 * doing different subject processing and even may do both publishing and subscribing.
 */
public class NatsJsPullSubMultipleWorkers {
    static final String usageString =
        "\nUsage: java -cp <classpath> NatsJsPullSubMultipleWorkers [-s server] [-strm stream] [-sub subject] [-dur durable] [-mcnt msgCount] [-scnt subCount]"
            + "\n\nDefault Values:"
            + "\n   [-strm stream]   psmw-stream"
            + "\n   [-sub subject]   psmw-subject"
            + "\n   [-dur durable]   psmw-durable"
            + "\n   [-mcnt msgCount] 100"
            + "\n   [-scnt subCount] 5"
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Push Subscribe, Durable Consumer, Shared Processing", args, usageString)
                .defaultStream("psmw-stream")
                .defaultSubject("psmw-subject")
                .defaultDurable("psmw-durable")
                .defaultMsgCount(100)
                .defaultSubCount(5)
                .build();

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            // Create a JetStreamManagement context.
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Use the utility to create a stream stored in memory.
            createStreamExitWhenExists(jsm, exArgs.stream, exArgs.subject);

            // Pre-create a durable pull consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable(exArgs.durable)
                .ackPolicy(AckPolicy.All)
                .build();
            jsm.addOrUpdateConsumer(exArgs.stream, cc);

            // Create our JetStream context
            JetStream js = nc.jetStream();

            System.out.println();

            // Setup the subscribers
            // - the PullSubscribeOptions can be re-used since all the subscribers are the same
            // - use a concurrent integer to track all the messages received
            // - have a list of subscribers and threads so I can track them
            PullSubscribeOptions pso = PullSubscribeOptions.bind(exArgs.stream, exArgs.durable);
            AtomicInteger allReceived = new AtomicInteger();
            List<JsPullSubWorker> subscribers = new ArrayList<>();
            List<Thread> subThreads = new ArrayList<>();
            for (int id = 1; id <= exArgs.subCount; id++) {
                // setup the subscription
                JetStreamSubscription sub = js.subscribe(exArgs.subject, pso);
                // create and track the runnable
                JsPullSubWorker qs = new JsPullSubWorker(id, exArgs, js, sub, allReceived);
                subscribers.add(qs);
                // create, track and start the thread
                Thread t = new Thread(qs);
                subThreads.add(t);
                t.start();
            }
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // create and start the publishing
            Thread pubThread = new Thread(new JsPublisher(js, exArgs));
            pubThread.start();

            // wait for all threads to finish
            pubThread.join();
            for (Thread t : subThreads) {
                t.join();
            }

            // report
            for (JsPullSubWorker sub : subscribers) {
                sub.report();
            }

            System.out.println();

            // delete the stream since we are done with it.
            jsm.deleteStream(exArgs.stream);
        }
        catch (RuntimeException e) {
            // Synchronous pull calls, including raw calls, fetch, iterate and reader
            // can throw JetStreamStatusException, although it is rare.
            // It also can happen if a new server version introduces a status the client does not understand.
            // The two current statuses that cause this are:
            // 1. 409 "Consumer Deleted" - The consumer was deleted externally in the middle of a pull request.
            // 2. 409 "Consumer is push based" - The consumer was modified externally and changed into a push consumer
            System.err.println(e);
        }
        catch (JetStreamApiException | IOException | TimeoutException | InterruptedException e) {
            System.err.println(e);
        }
    }

    static class JsPublisher implements Runnable {
        JetStream js;
        ExampleArgs exArgs;

        public JsPublisher(JetStream js, ExampleArgs exArgs) {
            this.js = js;
            this.exArgs = exArgs;
        }

        @Override
        public void run() {
            for (int x = 1; x <= exArgs.msgCount; x++) {
                try {
                    PublishAck pa = js.publish(exArgs.subject, ("Data # " + x).getBytes(StandardCharsets.US_ASCII));
                } catch (IOException | JetStreamApiException e) {
                    // something pretty wrong here
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }
    }

    static class JsPullSubWorker implements Runnable {
        int id;
        int thisReceived;
        List<String> datas;

        ExampleArgs exArgs;
        JetStream js;
        JetStreamSubscription sub;
        AtomicInteger allReceived;

        public JsPullSubWorker(int id, ExampleArgs exArgs, JetStream js, JetStreamSubscription sub, AtomicInteger allReceived) {
            this.id = id;
            thisReceived = 0;
            datas = new ArrayList<>();
            this.exArgs = exArgs;
            this.js = js;
            this.sub = sub;
            this.allReceived = allReceived;
        }

        public void report() {
            System.out.printf("Sub # %d handled %d messages.\n", id, thisReceived);
        }

        @Override
        public void run() {
            while (allReceived.get() < exArgs.msgCount) {
                List<Message> messages = sub.fetch(5, 500);
                while (messages != null && messages.size() > 0) {
                    for (Message msg : messages) {
                        thisReceived++;
                        allReceived.incrementAndGet();
                        String data = new String(msg.getData(), StandardCharsets.US_ASCII);
                        datas.add(data);
                        System.out.printf("QS # %d message # %d %s\n", id, thisReceived, data);
                    }
                    messages.get(messages.size()-1).ack();
                    messages = sub.fetch(10, 500);
                }
            }
            System.out.printf("QS # %d completed.\n", id);
        }
    }
}
