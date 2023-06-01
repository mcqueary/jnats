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

import static io.nats.examples.ExampleUtils.sleep;
import static io.nats.examples.jetstream.NatsJsUtils.*;

/**
 * This example will demonstrate miscellaneous uses cases of a pull subscription of:
 * fetch pull: <code>fetch(int batchSize, Duration maxWait)</code>,
 * no manual handling of null or status.
 */
public class NatsJsPullSubFetchUseCases {
    static final String usageString =
        "\nUsage: java -cp <classpath> NatsJsPullSubFetchUseCases [-s server] [-strm stream] [-sub subject] [-dur durable] [-mcnt msgCount]"
            + "\n\nDefault Values:"
            + "\n   [-strm] fetch-uc-stream"
            + "\n   [-sub]  fetch-uc-subject"
            + "\n   [-dur]  fetch-uc-durable-not-required"
            + "\n   [-mcnt] 15"
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Pull Subscription using macro Fetch, Use Cases", args, usageString)
                .defaultStream("fetch-uc-stream")
                .defaultSubject("fetch-uc-subject")
                .defaultDurable("fetch-uc-durable-not-required")
                .build();
        
        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {
            // Create a JetStreamManagement context.
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Use the utility to create a stream stored in memory.
            createStreamExitWhenExists(jsm, exArgs.stream, exArgs.subject);

            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // Build our consumer configuration and subscription options.
            // make sure the ack wait is sufficient to handle the reading and processing of the batch.
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .ackWait(Duration.ofMillis(2500))
                    .build();
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .durable(exArgs.durable)
                    .configuration(cc)
                    .build();

            // 0.1 Initialize. subscription
            // 0.2 Flush outgoing communication with/to the server, useful when app is both publishing and subscribing.
            System.out.println("\n----------\n0. Initialize the subscription and pull.");
            JetStreamSubscription sub = js.subscribe(exArgs.subject, pullOptions);

            // 1. Fetch, but there are no messages yet.
            // -  Read the messages, get them all (0)
            System.out.println("----------\n1. There are no messages yet");
            List<Message> messages = sub.fetch(10, Duration.ofSeconds(3));
            report(messages);
            messages.forEach(Message::ack);
            System.out.println("We should have received 0 total messages, we received: " + messages.size());

            // 2. Publish 10 messages
            // -  Fetch messages, get 10
            System.out.println("----------\n2. Publish 10 which satisfies the batch");
            publish(js, exArgs.subject, "A", 10);
            messages = sub.fetch(10, Duration.ofSeconds(3));
            report(messages);
            messages.forEach(Message::ack);
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 3. Publish 20 messages
            // -  Fetch messages, only get 10
            System.out.println("----------\n3. Publish 20 which is larger than the batch size.");
            publish(js, exArgs.subject, "B", 20);
            messages = sub.fetch(10, Duration.ofSeconds(3));
            report(messages);
            messages.forEach(Message::ack);
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 4. There are still messages left from the last
            // -  Fetch messages, get 10
            System.out.println("----------\n4. Get the rest of the publish.");
            messages = sub.fetch(10, Duration.ofSeconds(3));
            report(messages);
            messages.forEach(Message::ack);
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 5. Publish 5 messages
            // -  Fetch messages, get 5
            // -  Since there are less than batch size we only get what the server has.
            System.out.println("----------\n5. Publish 5 which is less than batch size.");
            publish(js, exArgs.subject, "C", 5);
            messages = sub.fetch(10, Duration.ofSeconds(3));
            report(messages);
            messages.forEach(Message::ack);
            System.out.println("We should have received 5 total messages, we received: " + messages.size());

            // 6. Publish 15 messages
            // -  Fetch messages, only get 10
            System.out.println("----------\n6. Publish 15 which is more than the batch size.");
            publish(js, exArgs.subject, "D", 15);
            messages = sub.fetch(10, Duration.ofSeconds(3));
            report(messages);
            messages.forEach(Message::ack);
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 7. There are 5 messages left
            // -  Fetch messages, only get 5
            System.out.println("----------\n7. There are 5 messages left.");
            messages = sub.fetch(10, Duration.ofSeconds(3));
            report(messages);
            messages.forEach(Message::ack);
            System.out.println("We should have received 5 messages, we received: " + messages.size());

            // 8. Read but don't ack.
            // -  Fetch messages, get 10, but either take too long to ack them or don't ack them
            System.out.println("----------\n8. Fetch but don't ack.");
            publish(js, exArgs.subject, "E", 10);
            messages = sub.fetch(10, Duration.ofSeconds(3));
            report(messages);
            System.out.println("We should have received 10 message, we received: " + messages.size());
            sleep(3000); // longer than the ackWait

            // 9. Fetch messages,
            // -  get the 10 messages we didn't ack
            System.out.println("----------\n9. Fetch, get the messages we did not ack.");
            messages = sub.fetch(10, Duration.ofSeconds(3));
            report(messages);
            messages.forEach(Message::ack);
            System.out.println("We should have received 10 message, we received: " + messages.size());

            System.out.println("----------\n");

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
        catch (JetStreamApiException | IOException | InterruptedException e) {
            System.err.println(e);
        }
    }
}
