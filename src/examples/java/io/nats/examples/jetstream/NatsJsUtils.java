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
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.nats.client.impl.NatsMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NatsJsUtils {

    // ----------------------------------------------------------------------------------------------------
    // STREAM INFO / CREATE / UPDATE
    // ----------------------------------------------------------------------------------------------------
    public static StreamInfo getStreamInfoOrNullWhenNotExist(JetStreamManagement jsm, String streamName) throws IOException, JetStreamApiException {
        try {
            return jsm.getStreamInfo(streamName);
        }
        catch (JetStreamApiException jsae) {
            if (jsae.getErrorCode() == 404) {
                return null;
            }
            throw jsae;
        }
    }

    public static boolean streamExists(JetStreamManagement jsm, String streamName) throws IOException, JetStreamApiException {
        return getStreamInfoOrNullWhenNotExist(jsm, streamName) != null;
    }

    public static boolean streamExists(Connection nc, String streamName) throws IOException, JetStreamApiException {
        return getStreamInfoOrNullWhenNotExist(nc.jetStreamManagement(), streamName) != null;
    }

    public static void exitIfStreamExists(JetStreamManagement jsm, String streamName) throws IOException, JetStreamApiException {
        if (streamExists(jsm, streamName)) {
            System.out.println("\nThe example cannot run since the stream '" + streamName + "' already exists.\n" +
                "It depends on the stream being in a new state. You can either:\n" +
                "  1) Change the stream name in the example.\n  2) Delete the stream.\n  3) Restart the server if the stream is a memory stream.");
            System.exit(-1);
        }
    }

    public static void exitIfStreamNotExists(Connection nc, String streamName) throws IOException, JetStreamApiException {
        if (!streamExists(nc, streamName)) {
            System.out.println("\nThe example cannot run since the stream '" + streamName + "' does not exist.\n" +
                "It depends on the stream existing and having data.");
            System.exit(-1);
        }
    }

    public static StreamInfo createOrReplaceStream(JetStreamManagement jsm, String streamName, StorageType storageType, String... subjects) throws IOException, JetStreamApiException {
        try {
            jsm.deleteStream(streamName);
        }
        catch (Exception ignore) {}
        // Create a stream, here will use a file storage type, and one subject,
        // the passed subject.
        StreamConfiguration sc = StreamConfiguration.builder()
            .name(streamName)
            .storageType(storageType)
            .subjects(subjects)
            .build();

        // Add or use an existing stream.
        StreamInfo si = jsm.addStream(sc);
        System.out.printf("Created stream '%s' with subject(s) %s\n",
            streamName, si.getConfiguration().getSubjects());

        return si;
    }

    public static StreamInfo createStream(JetStreamManagement jsm, String streamName, StorageType storageType, String... subjects) throws IOException, JetStreamApiException {
        // Create a stream, here will use a file storage type, and one subject,
        // the passed subject.
        StreamConfiguration sc = StreamConfiguration.builder()
            .name(streamName)
            .storageType(storageType)
            .subjects(subjects)
            .build();

        // Add or use an existing stream.
        StreamInfo si = jsm.addStream(sc);
        System.out.printf("Created stream '%s' with subject(s) %s\n",
            streamName, si.getConfiguration().getSubjects());

        return si;
    }

    public static StreamInfo createStream(JetStreamManagement jsm, String streamName, String... subjects)
            throws IOException, JetStreamApiException {
        return createStream(jsm, streamName, StorageType.Memory, subjects);
    }

    public static StreamInfo createStream(Connection nc, String stream, String... subjects) throws IOException, JetStreamApiException {
        return createStream(nc.jetStreamManagement(), stream, StorageType.Memory, subjects);
    }

    public static StreamInfo createStreamExitWhenExists(Connection nc, String streamName, String... subjects) throws IOException, JetStreamApiException {
        return createStreamExitWhenExists(nc.jetStreamManagement(), streamName, subjects);
    }

    public static StreamInfo createStreamExitWhenExists(JetStreamManagement jsm, String streamName, String... subjects) throws IOException, JetStreamApiException {
        exitIfStreamExists(jsm, streamName);
        return createStream(jsm, streamName, StorageType.Memory, subjects);
    }

    public static StreamInfo createStreamOrUpdateSubjects(JetStreamManagement jsm, String streamName, StorageType storageType, String... subjects)
            throws IOException, JetStreamApiException {

        StreamInfo si = getStreamInfoOrNullWhenNotExist(jsm, streamName);
        if (si == null) {
            return createStream(jsm, streamName, storageType, subjects);
        }

        // check to see if the configuration has all the subject we want
        StreamConfiguration sc = si.getConfiguration();
        boolean needToUpdate = false;
        for (String sub : subjects) {
            if (!sc.getSubjects().contains(sub)) {
                needToUpdate = true;
                sc.getSubjects().add(sub);
            }
        }

        if (needToUpdate) {
            sc = StreamConfiguration.builder(sc).subjects(sc.getSubjects()).build();
            si = jsm.updateStream(sc);
            System.out.printf("Existing stream '%s' was updated, has subject(s) %s\n",
                streamName, si.getConfiguration().getSubjects());
        }
        else
        {
            System.out.printf("Existing stream '%s' already contained subject(s) %s\n",
                streamName, si.getConfiguration().getSubjects());
        }

        return si;
    }

    public static StreamInfo createStreamOrUpdateSubjects(JetStreamManagement jsm, String streamName, String... subjects)
        throws IOException, JetStreamApiException {
        return createStreamOrUpdateSubjects(jsm, streamName, StorageType.Memory, subjects);
    }

    public static StreamInfo createStreamOrUpdateSubjects(Connection nc, String stream, String... subjects) throws IOException, JetStreamApiException {
        return createStreamOrUpdateSubjects(nc.jetStreamManagement(), stream, StorageType.Memory, subjects);
    }

    // ----------------------------------------------------------------------------------------------------
    // PUBLISH
    // ----------------------------------------------------------------------------------------------------
    public static void publish(Connection nc, String subject, int count) throws IOException, JetStreamApiException {
        publish(nc.jetStream(), subject, "data", count, -1, false);
    }

    public static void publish(JetStream js, String subject, int count) throws IOException, JetStreamApiException {
        publish(js, subject, "data", count, -1, false);
    }

    public static void publish(JetStream js, String subject, int count, int msgSize) throws IOException, JetStreamApiException {
        publish(js, subject, "data", count, msgSize, false);
    }

    public static void publish(JetStream js, String subject, String prefix, int count) throws IOException, JetStreamApiException {
        publish(js, subject, prefix, count, -1, false);
    }

    public static void publish(JetStream js, String subject, String prefix, int count, int msgSize) throws IOException, JetStreamApiException {
        publish(js, subject, prefix, count, msgSize, false);
    }

    public static void publish(JetStream js, String subject, String prefix, int count, boolean verbose) throws IOException, JetStreamApiException {
        publish(js, subject, prefix, count, -1, verbose);
    }

    public static void publish(JetStream js, String subject, String prefix, int count, int msgSize, boolean verbose) throws IOException, JetStreamApiException {
        if (verbose) {
            System.out.print("Publish ->");
        }
        for (int x = 1; x <= count; x++) {
            byte[] data = makeData(prefix, msgSize, verbose, x);
            Message msg =
                NatsMessage.builder()
                    .subject(subject)
                    .data(data)
                    .build();
            js.publish(msg);
        }
        if (verbose) {
            System.out.println(" <-");
        }
    }

    public static byte[] makeData(String prefix, int msgSize, boolean verbose, int x) {
        if (msgSize == 0) {
            return null;
        }

        String text = prefix + "-" + x + ".";
        if (verbose) {
            System.out.print(" " + text);
        }

        byte[] data = text.getBytes(StandardCharsets.US_ASCII);
        if (msgSize > data.length) {
            byte[] larger = new byte[msgSize];
            System.arraycopy(data, 0, larger, 0, data.length);
            data = larger;
        }
        return data;
    }

    public static long extractId(String data) {
        int at1 = data.indexOf("#");
        if (at1 == -1) {
            return -1;
        }
        int at2 = data.indexOf("#", at1 + 1);
        if (at2 == -1) {
            return -1;
        }
        return Long.parseLong(data.substring(at1 + 1, at2));
    }

    public static long extractId(byte[] data) {
        int at1 = -1;
        int at2 = -1;
        for (int x = 0; x < data.length; x++) {
            if (data[x] == (byte)'#') {
               if (at1 == -1) {
                   at1 = x;
               }
               else {
                   at2 = x;
                   break;
               }
            }
        }
        if (at1 == -1 || at2 == -1) {
            return -1;
        }
        return Long.parseLong(new String(data, at1 + 1, at2 - at1 - 1));
    }

    public static long extractId(Message m) {
        return extractId(m.getData());
    }

    public static Thread publishInBackground(JetStream js, String subject, String prefix, int count) {
        return publishInBackground(js, subject, prefix, count, 0);
    }

    public static Thread publishInBackground(JetStream js, String subject, String prefix, int count, long delay) {
        Thread t = new Thread(() -> {
            try {
                if (delay > 0) {
                    Thread.sleep(delay);
                }
                for (int x = 1; x <= count; x++) {
                    String data = prefix + "-" + x;
                    Message msg = NatsMessage.builder()
                            .subject(subject)
                            .data(data.getBytes(StandardCharsets.US_ASCII))
                            .build();
                    js.publish(msg);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        });
        t.start();
        return t;
    }

    // ----------------------------------------------------------------------------------------------------
    // READ MESSAGES
    // ----------------------------------------------------------------------------------------------------
    public static List<Message> readMessagesAck(JetStreamSubscription sub) throws InterruptedException {
        return readMessagesAck(sub, true, Duration.ofSeconds(1));
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub, boolean verbose) throws InterruptedException {
        return readMessagesAck(sub, verbose, Duration.ofSeconds(1));
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub, Duration nextMessageTimeout) throws InterruptedException {
        return readMessagesAck(sub, true, nextMessageTimeout);
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub, boolean verbose, Duration nextMessageTimeout) throws InterruptedException {
        if (verbose) {
            System.out.print("Read/Ack ->");
        }
        List<Message> messages = new ArrayList<>();
        Message msg = sub.nextMessage(nextMessageTimeout);
        while (msg != null) {
            messages.add(msg);
            msg.ack();
            if (verbose) {
                System.out.print(" " + new String(msg.getData()));
            }
            msg = sub.nextMessage(nextMessageTimeout);
        }

        if (verbose) {
            System.out.println(messages.size() == 0 ? " No messages available <-" : " <- ");
        }

        return messages;
    }

    // ----------------------------------------------------------------------------------------------------
    // PRINT
    // ----------------------------------------------------------------------------------------------------
    public static void printStreamInfo(StreamInfo si) {
        printObject(si, "StreamConfiguration", "StreamState", "ClusterInfo", "Mirror", "subjects", "sources");
    }

    public static void printStreamInfoList(List<StreamInfo> list) {
        printObject(list, "!StreamInfo", "StreamConfiguration", "StreamState");
    }

    public static void printConsumerInfo(ConsumerInfo ci) {
        printObject(ci, "ConsumerConfiguration", "Delivered", "AckFloor");
    }

    public static void printConsumerInfoList(List<ConsumerInfo> list) {
        printObject(list, "!ConsumerInfo", "ConsumerConfiguration", "Delivered", "AckFloor");
    }

    public static void printObject(Object o, String... subObjectNames) {
        String s = o.toString();
        for (String sub : subObjectNames) {
            boolean noIndent = sub.startsWith("!");
            String sb = noIndent ? sub.substring(1) : sub;
            String rx1 = ", " + sb;
            String repl1 = (noIndent ? ",\n": ",\n    ") + sb;
            s = s.replace(rx1, repl1);
        }

        System.out.println(s);
    }

    public static String metaString(NatsJetStreamMetaData meta) {
        return "Meta{" +
            "str='" + meta.getStream() + '\'' +
            ", con='" + meta.getConsumer() + '\'' +
            ", delivered=" + meta.deliveredCount() +
            ", strSeq=" + meta.streamSequence() +
            ", conSeq=" + meta.consumerSequence() +
            ", pending=" + meta.pendingCount() +
            '}';
    }

    // ----------------------------------------------------------------------------------------------------
    // REPORT
    // ----------------------------------------------------------------------------------------------------
    public static void report(List<Message> list) {
        System.out.print("Fetch ->");
        for (Message m : list) {
            System.out.print(" " + new String(m.getData()));
        }
        System.out.println(" <- ");
    }

    public static List<Message> report(Iterator<Message> list) {
        List<Message> messages = new ArrayList<>();
        System.out.print("Fetch ->");
        while (list.hasNext()) {
            Message m = list.next();
            messages.add(m);
            System.out.print(" " + new String(m.getData()));
        }
        System.out.println(" <- ");
        return messages;
    }

    // ----------------------------------------------------------------------------------------------------
    // MISC
    // ----------------------------------------------------------------------------------------------------
    public static int countJs(List<Message> messages) {
        int count = 0;
        for (Message m : messages) {
            count++;
        }
        return count;
    }

    public static int count408s(List<Message> messages) {
        int count = 0;
        for (Message m : messages) {
            if (m.isStatusMessage() && m.getStatus().getCode() == 408) {
                count++;
            }
        }
        return count;
    }

    public static void createCleanMemStream(JetStreamManagement jsm, String stream, String... subs) throws IOException, JetStreamApiException {
        try {
            jsm.deleteStream(stream);
        }
        catch (Exception ignore) {}

        StreamConfiguration sc = StreamConfiguration.builder()
            .name(stream)
            .storageType(StorageType.Memory)
            .subjects(subs)
            .build();
        jsm.addStream(sc);
    }

    public static void createCleanFileStream(JetStreamManagement jsm, String stream, String... subs) throws IOException, JetStreamApiException {
        try {
            jsm.deleteStream(stream);
        }
        catch (Exception ignore) {}

        StreamConfiguration sc = StreamConfiguration.builder()
            .name(stream)
            .storageType(StorageType.File)
            .subjects(subs)
            .build();
        jsm.addStream(sc);
    }
}
