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

package io.nats.examples.jetstream.simple;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.examples.jetstream.simple.Utils.Publisher;
import static io.nats.examples.jetstream.simple.Utils.createOrReplaceStream;

/**
 * This example will demonstrate all 3 simplified consumes running at the same time.
 * It just runs forever until you manually stop it.
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class ThreeDifferentConsumers {
    private static final String STREAM = "three-stream";
    private static final String SUBJECT = "three-subject";
    private static final String MESSAGE_TEXT = "three";
    private static final int REPORT_EVERY = 100;
    private static final int JITTER = 30;

    private static final String SERVER = "nats://localhost:4222";

    public static void main(String[] args) {
        Options options = Options.builder().server(SERVER).build();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // set's up the stream and create a durable consumer
            createOrReplaceStream(jsm, STREAM, SUBJECT);

            String consumerName1 = "next-" + NUID.nextGlobal();
            String consumerName2 = "handle-" + NUID.nextGlobal();
            String consumerName3 = "fetch-" + NUID.nextGlobal();

            // get stream context, create consumers and get the consumer contexts
            StreamContext streamContext;
            ConsumerContext ctx1;
            ConsumerContext ctx2;
            ConsumerContext ctx3;
            ConsumerContext consumerContext;
            try {
                streamContext = nc.streamContext(STREAM);
                streamContext.addConsumer(ConsumerConfiguration.builder().durable(consumerName1).build());
                streamContext.addConsumer(ConsumerConfiguration.builder().durable(consumerName2).build());
                streamContext.addConsumer(ConsumerConfiguration.builder().durable(consumerName3).build());
                ctx1 = js.consumerContext(STREAM, consumerName1);
                ctx2 = js.consumerContext(STREAM, consumerName2);
                ctx3 = js.consumerContext(STREAM, consumerName3);
            }
            catch (IOException e) {
                return; // likely a connection problem
            }
            catch (JetStreamApiException e) {
                return; // the stream or consumer did not exist
            }

            // create the consumer then use it
            IterableConsumer con1;
            try {
                con1 = ctx1.consume();
            }
            catch (JetStreamApiException e) {
                // unlikely, but a subscribe call could technically fail
                // if the client made an invalid api call or the communication
                // was corrupted across the wire
                return;
            }

            Thread con1Thread = new Thread(() -> {
                long mark = System.currentTimeMillis();
                int count = 0;
                long report = randomReportInterval();
                while (true) {
                    try {
                        Message msg = con1.nextMessage(1000);
                        if (msg != null) {
                            msg.ack();
                            ++count;
                            if (System.currentTimeMillis() - mark > report) {
                                System.out.println("Manual  " + count + " messages.");
                                mark = System.currentTimeMillis();
                                report = randomReportInterval();
                            }
                        }
                    }
                    catch (InterruptedException e) {
                        // this should never happen unless the
                        // developer interrupts this thread
                        return;
                    }
                    catch (JetStreamStatusCheckedException e) {
                        // either the consumer was deleted in the middle
                        // of the pull or there is a new status from the
                        // server that this client is not aware of
                        return;
                    }
                }
            });
            con1Thread.start();

            Publisher publisher = new Publisher(js, SUBJECT, MESSAGE_TEXT, JITTER);
            Thread pubThread = new Thread(publisher);
            pubThread.start();

            Thread.sleep(1000); // just makes the consumers be reading different messages
            AtomicInteger atomicCount = new AtomicInteger();
            AtomicLong atomicMark = new AtomicLong(System.currentTimeMillis());
            AtomicLong atomicReport = new AtomicLong(randomReportInterval());

            MessageHandler handler = msg -> {
                msg.ack();
                int count = atomicCount.incrementAndGet();
                if (System.currentTimeMillis() - atomicMark.get() > atomicReport.get()) {
                    System.out.println("Handled " + count + " messages.");
                    atomicMark.set(System.currentTimeMillis());
                    atomicReport.set(randomReportInterval());
                }
            };
            // keep the handler so it stays in scope or if you want to call stop
            MessageConsumer con2;
            try {
                con2 = ctx2.consume(handler);
            }
            catch (JetStreamApiException e) {
                // unlikely, but a subscribe call could technically fail if the client made an invalid api call
                // or the communication was corrupted across the wire
                return;
            }

            Thread.sleep(1000); // just makes the consumers be reading different messages
            Thread con2Thread = new Thread(() -> {
                int count = 0;
                long mark = System.currentTimeMillis();
                long report = randomReportInterval();
                try {
                    while (true) {
                        FetchConsumer fc = ctx3.fetchMessages(REPORT_EVERY);
                        Message msg = fc.nextMessage();
                        while (msg != null) {
                            msg.ack();
                            ++count;
                            if (System.currentTimeMillis() - mark > report) {
                                System.out.println("Fetched " + count + " messages.");
                                mark = System.currentTimeMillis();
                                report = randomReportInterval();
                            }
                            msg = fc.nextMessage();
                        }
                    }
                }
                catch (IOException e) {
                    // probably a connection problem in the middle of next
                    System.err.println("IOException should be handled properly, just exiting here.");
                    System.exit(-1);
                }
                catch (InterruptedException e) {
                    // this should never happen unless the
                    // developer interrupts this thread
                    System.err.println("InterruptedException should be handled properly, just exiting here.");
                    System.exit(-1);
                }
                catch (JetStreamStatusCheckedException e) {
                    // either the consumer was deleted in the middle
                    // of the pull or there is a new status from the
                    // server that this client is not aware of
                    System.err.println("JetStreamStatusCheckedException should be handled properly, just exiting here.");
                    System.exit(-1);
                }
                catch (JetStreamApiException e) {
                    // making the underlying subscription
                    System.err.println("JetStreamApiException should be handled properly, just exiting here.");
                    System.exit(-1);
                }
            });
            con2Thread.start();
            con2Thread.join(); // never ends so program runs until stopped.
        }
        catch (IOException ioe) {
            // problem making the connection
        }
        catch (InterruptedException e) {
            // thread interruption in the body of the example
        }
    }

    private static long randomReportInterval() {
        return 1000 + ThreadLocalRandom.current().nextLong(1000);
    }
}
