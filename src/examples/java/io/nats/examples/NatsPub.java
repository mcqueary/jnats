// Copyright 2015-2018 The NATS Authors
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

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.impl.Headers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class NatsPub {

    static final String usageString =
            "\nUsage: java NatsPub [server] <subject> <text message>\n"
            + "\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String args[]) {
        String subject = "foo.baz";
        String message = "payload"; // "ms" + System.currentTimeMillis() + "rand" + Math.abs(new Random().nextInt());
        String server = Options.DEFAULT_URL;

        Headers headers = new Headers().add("key1", "val11").add("key2", "val21", "val22");

//        if (args.length == 3) {
//            server = args[0];
//            subject = args[1];
//            message = args[2];
//        } else if (args.length == 2) {
//            server = Options.DEFAULT_URL;
//            subject = args[0];
//            message = args[1];
//        } else {
//            usage();
//            return;
//        }

        try {
            Connection nc = Nats.connect(ExampleUtils.createExampleOptions(server, false));

            System.out.println();
            System.out.printf("Sending %s on %s, server is %s\n", message, subject, server);
            System.out.println();
            nc.publish(subject, headers, message.getBytes(StandardCharsets.UTF_8));
            nc.flush(Duration.ofSeconds(5));
            nc.close();

        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }

    static void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }
}