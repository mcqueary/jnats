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

package io.nats.examples.jetstream;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static io.nats.examples.jetstream.NatsJsUtils.createStreamExitWhenExists;
import static io.nats.examples.jetstream.NatsJsUtils.publishInBackground;

/**
 * This example will demonstrate basic use of a pull subscription of:
 * fetch pull: <code>fetch(int batchSize, Duration or millis maxWait)</code>,
 */
public class NatsJsPullSubFetch {
    static final String usageString =
        "\nUsage: java -cp <classpath> NatsJsPullSubFetch [-s server] [-strm stream] [-sub subject] [-dur durable] [-mcnt msgCount]"
            + "\n\nDefault Values:"
            + "\n   [-strm] fetch-stream"
            + "\n   [-sub]  fetch-subject"
            + "\n   [-dur]  fetch-durable-not-required"
            + "\n   [-mcnt] 15"
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Pull Subscription using macro Fetch", args, usageString)
                .defaultStream("fetch-stream")
                .defaultSubject("fetch-subject")
                .defaultDurable("fetch-durable-not-required")
                .defaultMsgCount(15)
                .build();

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {
            // Create a JetStreamManagement context.
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Use the utility to create a stream stored in memory.
            createStreamExitWhenExists(jsm, exArgs.stream, exArgs.subject);

            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // start publishing the messages, don't wait for them to finish, simulating an outside producer
            publishInBackground(js, exArgs.subject, "fetch-message", exArgs.msgCount);

            // Build our consumer configuration and subscription options.
            // make sure the ack wait is sufficient to handle the reading and processing of the batch.
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .ackWait(Duration.ofMillis(2500))
                .build();
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                .durable(exArgs.durable)
                .configuration(cc)
                .build();

            JetStreamSubscription sub = js.subscribe(exArgs.subject, pullOptions);
            nc.flush(Duration.ofSeconds(1));

            int red = 0;
            while (red < exArgs.msgCount) {
                List<Message> list = sub.fetch(10, Duration.ofSeconds(1));
                for (Message m : list) {
                    red++; // process message
                    System.out.println("" + red + ". " + m);
                    m.ack();
                }
            }

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
}
