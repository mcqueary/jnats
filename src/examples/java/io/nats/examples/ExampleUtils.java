// Copyright 2018 The NATS Authors
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
import io.nats.client.impl.ErrorListenerLoggerImpl;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class ExampleUtils {

    public static String getServer(String[] args) {
        if (args.length == 1) {
            return args[0];
        } else if (args.length == 2 && args[0].equals("-s")) {
            return args[1];
        }
        return Options.DEFAULT_URL;
    }

    public static final ConnectionListener EXAMPLE_CONNECTION_LISTENER = (conn, type) -> System.out.println("Status change "+ type);

    public static final ErrorListener EXAMPLE_ERROR_LISTENER = new ErrorListenerLoggerImpl();

    public static Options createExampleOptions(String[] args) {
        String server = getServer(args);
        return createExampleOptions(server, false, EXAMPLE_ERROR_LISTENER, EXAMPLE_CONNECTION_LISTENER);
    }

    public static Options createExampleOptions(String[] args, boolean allowReconnect) {
        String server = getServer(args);
        return createExampleOptions(server, allowReconnect, EXAMPLE_ERROR_LISTENER, EXAMPLE_CONNECTION_LISTENER);
    }

    public static Options createExampleOptions(String server) {
        return createExampleOptions(server, false, EXAMPLE_ERROR_LISTENER, EXAMPLE_CONNECTION_LISTENER);
    }

    public static Options createExampleOptions(String server, ErrorListener el) {
        return createExampleOptions(server, false, el, EXAMPLE_CONNECTION_LISTENER);
    }

    public static Options createExampleOptions(String server, boolean allowReconnect) {
        return createExampleOptions(server, allowReconnect, EXAMPLE_ERROR_LISTENER, EXAMPLE_CONNECTION_LISTENER);
    }

    public static Options createExampleOptions(String server, boolean allowReconnect, ErrorListener el, ConnectionListener cl) {

        if (el == null) { el = new ErrorListener() {}; }

        Options.Builder builder = new Options.Builder()
            .server(server)
            .connectionTimeout(Duration.ofSeconds(5))
            .pingInterval(Duration.ofSeconds(10))
            .reconnectWait(Duration.ofSeconds(1))
            .connectionListener(cl)
            .errorListener(el);

        if (!allowReconnect) {
            builder = builder.noReconnect();
        } else {
            builder = builder.maxReconnects(-1);
        }

        if (System.getenv("NATS_NKEY") != null && System.getenv("NATS_NKEY") != "") {
            AuthHandler handler = null;
            try {
                handler = new ExampleAuthHandler(System.getenv("NATS_NKEY"));
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            builder.authHandler(handler);
        } else if (System.getenv("NATS_CREDS") != null && System.getenv("NATS_CREDS") != "") {
            builder.authHandler(Nats.credentials(System.getenv("NATS_CREDS")));
        }
        return builder.build();
    }

    public static ExampleArgs optionalServer(String[] args, String usageString) {
        ExampleArgs ea = new ExampleArgs(args, null, usageString);
        if (ea.containedUnknown) {
            usage(usageString);
        }
        return ea;
    }

    public static ExampleArgs expectSubjectAndMessage(String[] args, String usageString) {
        ExampleArgs ea = new ExampleArgs(args, ExampleArgs.Trail.MESSAGE, usageString);
        if (ea.containedUnknown || ea.message == null) {
            usage(usageString);
        }
        return ea;
    }

    public static ExampleArgs expectSubjectAndMsgCount(String[] args, String usageString) {
        ExampleArgs ea = new ExampleArgs(args, ExampleArgs.Trail.COUNT, usageString);
        if (ea.containedUnknown || ea.msgCount < 1) {
            usage(usageString);
        }
        return ea;
    }

    public static ExampleArgs expectSubjectQueueAndMsgCount(String[] args, String usageString) {
        ExampleArgs ea = new ExampleArgs(args, ExampleArgs.Trail.COUNT, usageString);
        if (ea.containedUnknown || ea.msgCount < 1) {
            usage(usageString);
        }
        return ea;
    }

    public static void sleep(long millis) {
        try {
            if (millis > 0) {
                Thread.sleep(millis);
            }
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public static void sleepRandom(long millis) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(millis));
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private static void usage(String usageString) {
        System.out.println(usageString);
        System.exit(-1);
    }

    public static String uniqueEnough() {
        return NUID.nextGlobal();
    }

    public static String randomString(int length) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < length) {
            sb.append(Long.toHexString(ThreadLocalRandom.current().nextLong()));
        }
        return sb.substring(0, length);
    }
}
