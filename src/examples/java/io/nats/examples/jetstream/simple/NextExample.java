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

import static io.nats.examples.jetstream.simple.Utils.createOrReplaceStream;

/**
 * This example will demonstrate simplified next
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class NextExample {
    private static final String STREAM = "next-stream";
    private static final String SUBJECT = "next-subject";
    private static final String CONSUMER_NAME = "next-consumer";

    public static String SERVER = "nats://localhost:4222";

    public static void main(String[] args) {
        Options options = Options.builder().server(SERVER).build();
        try (Connection nc = Nats.connect(options)) {
            JetStream js = nc.jetStream();
            createOrReplaceStream(nc.jetStreamManagement(), STREAM, SUBJECT);

            // get stream context, create consumer and get the consumer context
            StreamContext streamContext;
            ConsumerContext consumerContext;
            try {
                streamContext = nc.streamContext(STREAM);
                streamContext.addConsumer(ConsumerConfiguration.builder().durable(CONSUMER_NAME).build());
                consumerContext = streamContext.consumerContext(CONSUMER_NAME);
            }
            catch (JetStreamApiException | IOException e) {
                // JetStreamApiException:
                //      the stream or consumer did not exist
                // IOException:
                //      likely a connection problem
                return;
            }

            int count = 20;
            // Simulate messages coming in
            Thread t = new Thread(() -> {
                int sleep = 2000;
                boolean down = true;
                for (int x = 1; x <= count; x++) {
                    try {
                        Thread.sleep(sleep);
                        if (down) {
                            sleep -= 200;
                            down = sleep > 0;
                        }
                        else {
                            sleep += 200;
                        }
                        js.publish(SUBJECT, ("message-" + x).getBytes());
                    }
                    catch (JetStreamApiException | IOException | InterruptedException e) {
                        // JetStreamApiException:
                        //      The "publish" somehow was rejected by the server.
                        //      Unlikely w/o publish option expectations.
                        // IOException:
                        //      likely a connection problem
                        // InterruptedException:
                        //      developer interrupted this thread?
                        throw new RuntimeException(e);
                    }
                }
            });
            t.start();

            int received = 0;
            while (received < count) {
                long start = System.currentTimeMillis();
                try {
                    Message m = consumerContext.next(1000);
                    long elapsed = System.currentTimeMillis() - start;
                    if (m == null) {
                        System.err.println("Waited " + elapsed + "ms for message, got null");
                    }
                    else {
                        ++received;
                        System.out.println("Waited " + elapsed + "ms for message, got " + new String(m.getData()));
                    }
                }
                catch (JetStreamApiException | JetStreamStatusCheckedException | IOException | InterruptedException e) {
                    // JetStreamApiException:
                    //      api calls under the covers theoretically this could fail, but practically it won't.
                    // JetStreamStatusCheckedException:
                    //      Either the consumer was deleted in the middle
                    //      of the pull or there is a new status from the
                    //      server that this client is not aware of
                    // IOException:
                    //      likely a connection problem
                    // InterruptedException:
                    //      developer interrupted this thread?
                    return;
                }
            }

            t.join();
        }
        catch (IOException | InterruptedException ioe) {
            // IOException:
            //      problem making the connection
            // InterruptedException:
            //      thread interruption in the body of the example
        }
    }
}
