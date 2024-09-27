// Copyright 2022 The NATS Authors
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

package io.nats.examples.requestMany;

import io.nats.client.*;
import io.nats.requestMany.RequestMany;
import io.nats.requestMany.RmMessage;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The "Default" will get as many responses as it can in
 * the connection options timeout time (default is 2 seconds)
 */
public class RequestManyIterate {
    static final String REQUEST_SUBJECT = "req";
    static final int RESPONDERS = 3;

    public static void main(String[] args) throws Exception {

        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .errorListener(new ErrorListener() {})
            .build();

        try (Connection nc = Nats.connect(options)) {

            long ctMs = nc.getOptions().getConnectionTimeout().toMillis();

            // The default request is to get as many in the default time period
            System.out.println("Default Connection Timeout: " + ctMs);

            // The builder needs your connection...we don't set any custom options though
            RequestMany rm = RequestMany.builder(nc).build();

            // On a fetch, if there was an exceptional reason for the completion,
            // an RmMessage with a status message or exception
            // and will be the last message in the results list.
            // We haven't started any responders yet so this will return with a Status 503
            // The good news is that a 503 (or any status or exception)
            // will short circuit and return quickly.
            long start = System.currentTimeMillis();
            LinkedBlockingQueue<RmMessage> q = rm.iterate(REQUEST_SUBJECT, null);
            long elapsed = System.currentTimeMillis() - start;

            // We should expect exactly 1 message since we know it's no responders.
            RmMessage rmm = q.poll(1000, TimeUnit.MILLISECONDS);
            System.out.println("A. Expect 1 status message. ");
            System.out.println("   " + rm);
            System.out.println("   Count: " + q.size() + ", Elapsed: " + elapsed + " ms");
            System.out.println("   0. statusMessage ? " + rmm.isStatusMessage()
                + ", Message: " + rmm.getStatusMessage());

            // start a responder simulator. Each message it gets it will respond n times
            Dispatcher dispatcher = nc.createDispatcher(m -> {
                for (int x = 0; x < RESPONDERS; x++) {
                    nc.publish(m.getReplyTo(), ("R" + x).getBytes());
                }
            });
            dispatcher.subscribe(REQUEST_SUBJECT);

            // It's okay to reuse the RequestMany object, and
            // since it's connection is its rmm state, it can be used in parallel.
            start = System.currentTimeMillis();
            q = rm.iterate(REQUEST_SUBJECT, null);
            elapsed = System.currentTimeMillis() - start;

            // The default options contain a stall timeout of 1/10 of the connection timeout.
            // So this should return pretty fast, not the entire
            System.out.println("\nB. Expect " + RESPONDERS + " data messages in slightly more than " + (ctMs / 10) + " ms.");
            System.out.println("   " + rm);
            System.out.println("   Count: " + q.size() + ", Elapsed: " + elapsed + " ms");
            rmm = q.poll(1000, TimeUnit.MILLISECONDS);
            while (rmm != null) {
                report(rmm);
                rmm = q.poll(1000, TimeUnit.MILLISECONDS);
            }

            // Maybe you always want to wait the full 1 second.
            // Maybe there could be slow (busy?) responders but you still want to hear from them.
            rm = RequestMany.builder(nc).totalWaitTime(3000).build();

            start = System.currentTimeMillis();
            q = rm.iterate(REQUEST_SUBJECT, null);
            elapsed = System.currentTimeMillis() - start;

            System.out.println("\nC. Expect " + RESPONDERS + " data messages in slightly more than 1000 ms.");
            System.out.println("   " + rm);
            System.out.println("   Count: " + q.size() + ", Elapsed: " + elapsed + " ms");
            rmm = q.poll(1000, TimeUnit.MILLISECONDS);
            while (rmm != null) {
                report(rmm);
                rmm = q.poll(1000, TimeUnit.MILLISECONDS);
            }

            // Maybe you just want the first 2. Also limit the time. No slowpoke responders allowed!
            rm = RequestMany.builder(nc).totalWaitTime(1000).maxResponses(2).build();

            start = System.currentTimeMillis();
            q = rm.iterate(REQUEST_SUBJECT, null);
            elapsed = System.currentTimeMillis() - start;

            System.out.println("\nD. Expect the max of 2 happens quickly.");
            System.out.println("   " + rm);
            System.out.println("   Count: " + q.size() + ", Elapsed: " + elapsed + " ms");
            rmm = q.poll(1000, TimeUnit.MILLISECONDS);
            while (rmm != null) {
                report(rmm);
                rmm = q.poll(1000, TimeUnit.MILLISECONDS);
            }
        }
    }

    private static void report(RmMessage rmm) {
        if (rmm.isDataMessage()) {
            System.out.println("   dataMessage: " + new String(rmm.getMessage().getData()));
        }
        else {
            System.out.println("   isEndOfData: " + rmm.isEndOfData()
                + (rmm.isNormalEndOfData() ? " <normal>" : "")
            );
        }
    }
}
