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

package io.nats.examples;

import io.nats.client.*;
import io.nats.client.impl.NatsMessage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * This example will demonstrate JetStream async / future publishing.
 *
 * Run Notes:
 * - msg_count < 1 is the same as 1
 * - headers are optional
 *
 * Usage: java NatsJsPubAsync [-s server] [-strm stream] [-sub subject] [-mcnt msgCount] [-m messageWords+] [-h headerKey:headerValue]*
 *   Use tls:// or opentls:// to require tls, via the Default SSLContext
 *   Set the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.
 *   Set the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.
 *   Use the URL for user/pass/token authentication.
 */
public class NatsJsPubAsync {

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultStream("example-stream")
                .defaultSubject("example-subject")
                .defaultMessage("hello")
                .defaultMsgCount(5)
                .build(args);

        String hdrNote = exArgs.hasHeaders() ? ", with " + exArgs.headers.size() + " header(s)" : "";
        System.out.printf("\nPublishing to %s%s. Server is %s\n\n", exArgs.subject, hdrNote, exArgs.server);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {

            // Create a JetStream context.  This hangs off the original connection
            // allowing us to produce data to streams and consume data from
            // JetStream consumers.
            JetStream js = nc.jetStream();

            // See NatsJsManagement for examples on how to create the stream
            NatsJsUtils.createOrUpdateStream(nc, exArgs.stream, exArgs.subject);

            List<CompletableFuture<PublishAck>> futures = new ArrayList<>();

            int stop = exArgs.msgCount < 2 ? 2 : exArgs.msgCount + 1;
            for (int x = 1; x < stop; x++) {
                // make unique message data if you want more than 1 message
                String data = exArgs.msgCount < 2 ? exArgs.message : exArgs.message + "-" + x;

                // create a typical NATS message
                Message msg = NatsMessage.builder()
                        .subject(exArgs.subject)
                        .headers(exArgs.headers)
                        .data(data, StandardCharsets.UTF_8)
                        .build();
                System.out.printf("Published message %s on subject %s.\n", data, exArgs.subject);

                // Publish a message and print the results of the publish acknowledgement.
                // An exception will be thrown if there is a failure.
                futures.add(js.publishAsync(msg));
            }

            for (CompletableFuture<PublishAck> future : futures) {
                System.out.println("Received " + future.get());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
