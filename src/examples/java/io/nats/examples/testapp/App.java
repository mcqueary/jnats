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

package io.nats.examples.testapp;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.examples.testapp.support.CommandLine;
import io.nats.examples.testapp.support.CommandLineConsumer;
import io.nats.examples.testapp.support.ConsumerType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class App {

    public static String[] MANUAL_ARGS = (
        "--servers nats://192.168.50.99:4222"
//        " --servers nats://localhost:4222"
            + " --stream app-stream"
            + " --subject app-subject"
            + " --subject app-subject"
//            + " --runtime 3600 // 1 hour in seconds
            + " --screen left"
            + " --create"
            + " --publish"
            + " --pubjitter 30"
            + " --simple ordered,100,5000"
            + " --simple durable 100 5000" // space or commas work, the parser figures it out
            + " --push ordered"
            + " --push durable"
            + " --logdir c:\\temp"
    ).split(" ");

    public static void main(String[] args) throws Exception {

        // args = MANUAL_ARGS; // comment out for real command line

        CommandLine cmd = new CommandLine(args);
        Monitor monitor;

        try {
            Output.start(cmd);
            Output.controlMessage("APP", cmd.toString().replace(" --", "    \n--"));
            CountDownLatch waiter = new CountDownLatch(1);

            Publisher publisher = null;
            List<ConnectableConsumer> cons = null;

            if (cmd.create) {
                Output.consoleMessage("APP", cmd.toString().replace(" --", "    \n--"));
                Options options = cmd.makeManagmentOptions();
                try (Connection nc = Nats.connect(options)) {
                    JetStreamManagement jsm = nc.jetStreamManagement();
                    createOrReplaceStream(cmd, jsm);
                }
                catch (Exception e) {
                    Output.consoleMessage("APP", e.getMessage());
                }
            }

            if (!cmd.commandLineConsumers.isEmpty()) {
                cons = new ArrayList<>();
                for (CommandLineConsumer clc : cmd.commandLineConsumers) {
                    ConnectableConsumer con;
                    if (clc.consumerType == ConsumerType.Simple) {
                        con = new SimpleConsumer(cmd, clc.consumerKind, clc.batchSize, clc.expiresIn);
                    }
                    else {
                        con = new PushConsumer(cmd, clc.consumerKind);
                    }
                    Output.consoleMessage("APP", con.label);
                    cons.add(con);
                }
            }

            if (cmd.publish) {
                publisher = new Publisher(cmd, cmd.pubjitter);
                Thread pubThread = new Thread(publisher);
                pubThread.start();
            }

            // just creating the stream?
            if (publisher == null && cons == null) {
                return;
            }

            monitor = new Monitor(cmd, publisher, cons);
            Thread monThread = new Thread(monitor);
            monThread.start();

            long runtime = cmd.runtime < 1 ? Long.MAX_VALUE : cmd.runtime;
            //noinspection ResultOfMethodCallIgnored
            waiter.await(runtime, TimeUnit.MILLISECONDS);
        }
        catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
        }
        finally {
            Output.dumpControl();
            System.exit(0);
        }
    }

    public static void createOrReplaceStream(CommandLine cmd, JetStreamManagement jsm) {
        try {
            jsm.deleteStream(cmd.stream);
        }
        catch (Exception ignore) {}
        try {
            StreamConfiguration sc = StreamConfiguration.builder()
                .name(cmd.stream)
                .storageType(StorageType.File)
                .subjects(cmd.subject)
                .build();
            StreamInfo si = jsm.addStream(sc);
            Output.controlMessage("APP", "Create Stream\n" + Output.formatted(si.getConfiguration()));
        }
        catch (Exception e) {
            Output.consoleMessage("FATAL", "Failed creating stream: '" + cmd.stream + "' " + e);
            System.exit(-1);
        }
    }
}