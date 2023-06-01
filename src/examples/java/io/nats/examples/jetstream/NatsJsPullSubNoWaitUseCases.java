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
 * batch size and no wait pull: <code>pullNoWait(int batchSize)</code>
 */
public class NatsJsPullSubNoWaitUseCases {
    static final String usageString =
        "\nUsage: java -cp <classpath> NatsJsPullSubNoWaitUseCases [-s server] [-strm stream] [-sub subject] [-dur durable]"
            + "\n\nDefault Values:"
            + "\n   [-strm] nowait-uc-stream"
            + "\n   [-sub]  nowait-uc-subject"
            + "\n   [-dur]  nowait-uc-durable"
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Pull Subscription using primitive No Wait, Use Cases", args, usageString)
                .defaultStream("nowait-uc-stream")
                .defaultSubject("nowait-uc-subject")
                .defaultDurable("nowait-uc-durable")
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
            // 0.2 DO NOT start the pull, no wait works differently than regular pull.
            //     With no wait, we have to start the pull the first time and every time the
            //     batch size is exhausted or no waits out.
            // 0.3 Flush outgoing communication with/to the server, useful when app is both publishing and subscribing.
            System.out.println("\n----------\n0. Initialize the subscription and pull.");
            JetStreamSubscription sub = js.subscribe(exArgs.subject, pullOptions);
            nc.flush(Duration.ofSeconds(1));

            // 1. Start the pull, but there are no messages yet.
            // -  Read the messages
            // -  Since there are less than the batch size, we get them all (0)
            System.out.println("----------\n1. There are no messages yet");
            sub.pullNoWait(10);
            List<Message> messages = readMessagesAck(sub);
            System.out.println("We should have received 0 total messages, we received: " + messages.size());

            // 2. Publish 10 messages
            // -  Start the pull
            // -  Read the messages
            // -  Since there are exactly the batch size we get them all
            System.out.println("----------\n2. Publish 10 which satisfies the batch");
            publish(js, exArgs.subject, "A", 10);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 3. Publish 20 messages
            // -  Start the pull
            // -  Read the messages
            System.out.println("----------\n3. Publish 20 which is larger than the batch size.");
            publish(js, exArgs.subject, "B", 20);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 4. There are still messages left from the last
            // -  Start the pull
            // -  Read the messages
            System.out.println("----------\n4. Get the rest of the publish.");
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 5. Publish 5 messages
            // -  Start the pull
            // -  Read the messages
            System.out.println("----------\n5. Publish 5 which is less than batch size.");
            publish(js, exArgs.subject, "C", 5);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            System.out.println("We should have received 5 total messages, we received: " + messages.size());

            // 6. Publish 14 messages
            // -  Start the pull
            // -  Read the messages
            // -  we do NOT get a nowait status message if there are more or equals messages than the batch
            System.out.println("----------\n6. Publish 14 which is more than the batch size.");
            publish(js, exArgs.subject, "D", 14);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 7. There are 4 messages left
            // -  Start the pull
            // -  Read the messages
            // -  Since there are less than batch size the last message we get will be a status 404 message.
            System.out.println("----------\n7. There are 4 messages left, which is less than the batch size.");
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            System.out.println("We should have received 4 messages, we received: " + messages.size());

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
