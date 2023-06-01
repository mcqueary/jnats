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

import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class Utils {

    public static void createOrReplaceStream(JetStreamManagement jsm, String stream, String subject) {
        // in case the stream was here before, we want a completely new one
        try { jsm.deleteStream(stream); } catch (Exception ignore) {}

        try {
            jsm.addStream(StreamConfiguration.builder()
                .name(stream)
                .storageType(StorageType.Memory)
                .subjects(subject)
                .build());
        }
        catch (Exception e) {
            System.err.println("Fatal error, cannot create stream.");
            System.exit(-1);
        }
    }

    public static void publish(JetStream js, String subject, String messageText, int count) {
        try {
            for (int x = 1; x <= count; x++) {
                js.publish(subject, (messageText + "-" + x).getBytes());
            }
        }
        catch (Exception e) {
            System.err.println("Fatal error, publish failure.");
            System.exit(-1);
        }
    }

    public static class Publisher implements Runnable {
        private final JetStream js;
        private final String subject;
        private final String messageText;
        private final int jitter;
        private final AtomicBoolean keepGoing = new AtomicBoolean(true);
        private int pubNo;

        public Publisher(JetStream js, String subject, String messageText, int jitter) {
            this.js = js;
            this.subject = subject;
            this.messageText = messageText;
            this.jitter = jitter;
        }

        public void stopPublishing() {
            keepGoing.set(false);
        }

        @Override
        public void run() {
            try {
                while (keepGoing.get()) {
                    //noinspection BusyWait
                    Thread.sleep(ThreadLocalRandom.current().nextLong(jitter));
                    js.publish(subject, (messageText + "-" + (++pubNo)).getBytes());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
