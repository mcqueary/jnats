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

import java.io.IOException;

import static io.nats.examples.jetstream.simple.Utils.*;

/**
 * This example will demonstrate simplified manual consume.
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class ConsumeManuallyCallNext {
    private static final String STREAM = "manually-stream";
    private static final String SUBJECT = "manually-subject";
    private static final String CONSUMER_NAME = "manually-consumer";
    private static final String MESSAGE_TEXT = "manually";
    private static final int STOP_COUNT = 500;
    private static final int REPORT_EVERY = 50;
    private static final int JITTER = 20;

    private static final String SERVER = "nats://localhost:4222";

    public static void main(String[] args) {
        Options options = Options.builder().server(SERVER).build();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // set's up the stream and create a durable consumer
            createOrReplaceStream(jsm, STREAM, SUBJECT);
            createConsumer(jsm, STREAM, CONSUMER_NAME);

            // Create the Consumer Context
            ConsumerContext consumerContext;
            try {
                consumerContext = js.getConsumerContext(STREAM, CONSUMER_NAME);
            }
            catch (IOException e) {
                return; // likely a connection problem
            }
            catch (JetStreamApiException e) {
                return; // the stream or consumer did not exist
            }

            // Get the Manual Consumer from the context
            ManualConsumer consumer = consumerContext.consume();

            long start = System.nanoTime();
            Thread consumeThread = new Thread(() -> {
                int count = 0;
                try {
                    System.out.println("Starting main loop.");
                    while (count < STOP_COUNT) {
                        Message msg = consumer.nextMessage(1000);
                        if (msg != null) {
                            msg.ack();
                            if (++count % REPORT_EVERY == 0) {
                                report("Main Loop Running", start, count);
                            }
                        }
                    }
                    report("Main Loop Stopped", start, count);

                    System.out.println("Pausing for effect...allow more messages come across.");
                    Thread.sleep(JITTER * 2); // allows more messages to come across
                    consumer.stop();

                    System.out.println("Starting post-drain loop.");
                    Message msg = consumer.nextMessage(1000);
                    while (msg != null) {
                        msg.ack();
                        report("Post Drain Loop Running", start, ++count);
                        msg = consumer.nextMessage(1000);
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

                report("Done", start, count);
            });
            consumeThread.start();

            Publisher publisher = new Publisher(js, SUBJECT, MESSAGE_TEXT, JITTER);
            Thread pubThread = new Thread(publisher);
            pubThread.start();

            consumeThread.join();
            publisher.stopPublishing();
            pubThread.join();
        }
        catch (IOException ioe) {
            // problem making the connection or
        }
        catch (InterruptedException e) {
            // thread interruption in the body of the example
        }
    }

    private static void report(String label, long start, int count) {
        long ms = (System.nanoTime() - start) / 1_000_000;
        System.out.println(label + ": Received " + count + " messages in " + ms + "ms.");
    }
}
