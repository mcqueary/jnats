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
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static io.nats.examples.jetstream.NatsJsUtils.*;

/**
 * This example will demonstrate miscellaneous uses cases of a pull subscription of:
 * expires in pull: <code>pullExpiresIn(int batchSize, Duration or millis expiresIn)</code>
 */
public class NatsJsPullSubExpiresInUseCases {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsPullSubExpiresInUseCases [-s server] [-strm stream] [-sub subject] [-dur durable]"
                    + "\n\nDefault Values:"
                    + "\n   [-strm] expires-in-uc-ack-stream"
                    + "\n   [-sub]  expires-in-uc-ack-subject"
                    + "\n   [-dur]  expires-in-uc-ack-durable"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Pull Subscription using primitive Batch With Expire, Use Cases", args, usageString)
                .defaultStream("expires-in-uc-ack-stream")
                .defaultSubject("expires-in-uc-ack-subject")
                .defaultDurable("expires-in-uc-ack-durable")
                .build();

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {
            // Create a JetStreamManagement context.
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Use the utility to create a stream stored in memory.
            createStreamExitWhenExists(jsm, exArgs.stream, exArgs.subject);

            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // Build our subscription options.
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .durable(exArgs.durable)
                    .build();

            // 0.1 Initialize. subscription
            // 0.2 Flush outgoing communication with/to the server, useful when app is both publishing and subscribing.
            // 0.3 Start the pull, you don't have to call this again because AckMode.NEXT
            // -  When we ack a batch message the server starts preparing or adding to the next batch.
            System.out.println("\n----------\n0. Initialize the subscription and pull.");
            JetStreamSubscription sub = js.subscribe(exArgs.subject, pullOptions);
            nc.flush(Duration.ofSeconds(1));

            // 1. Publish some that is less than the batch size.
            System.out.println("\n----------\n1. Publish some amount of messages, but not entire batch size.");
            publish(js, exArgs.subject, "A", 6);
            sub.pullExpiresIn(10, Duration.ofMillis(1200));
            List<Message> messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 6 total messages, we received: " + messages.size());

            // 2. Publish some more covering our pull size...
            System.out.println("----------\n2. Publish more than the batch size.");
            sub.pullExpiresIn(10, Duration.ofMillis(1200));
            publish(js, exArgs.subject, "B", 14);
            messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 3. There are still 4 messages from B, but the batch was finished
            // -  won't get any messages until a pull is issued.
            System.out.println("----------\n3. Read without issuing a pull.");
            messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 0 total messages, we received: " + messages.size());

            // 4. re-issue the pull to get the last 4
            System.out.println("----------\n4. Issue the pull to get the last 4.");
            sub.pullExpiresIn(10, Duration.ofMillis(1200));
            messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 4 total messages, we received: " + messages.size());

            // 5. publish a lot of messages
            System.out.println("----------\n5. Publish a lot of messages. The last pull was under the batch size.");
            System.out.println(            "   Issue another pull with batch size less than number of messages.");
            publish(js, exArgs.subject, "C", 25);
            sub.pullExpiresIn(10, Duration.ofMillis(1200));
            messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 6. there are still more messages
            System.out.println("----------\n6. Still more messages. Issue another pull with batch size less than number of messages.");
            sub.pullExpiresIn(10, Duration.ofMillis(1200));
            messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 7. there are still more messages
            System.out.println("----------\n7. Still more messages. Issue another pull with batch size more than number of messages.");
            sub.pullExpiresIn(10, Duration.ofMillis(1200));
            messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 5 total messages, we received: " + messages.size());

            // 8. we got them all
            System.out.println("----------\n8. No messages left.");
            sub.pullExpiresIn(10, Duration.ofMillis(1200));
            messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 0 total messages, we received: " + messages.size());

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
        catch (JetStreamApiException | IOException | TimeoutException | InterruptedException e) {
            System.err.println(e);
        }
    }
}
