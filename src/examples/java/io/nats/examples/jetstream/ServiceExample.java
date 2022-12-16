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

package io.nats.examples.jetstream;

import io.nats.client.*;
import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;
import io.nats.service.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.Validator.nullOrEmpty;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class ServiceExample {

    public static final String ECHO_SERVICE = "EchoService";
    public static final String SORT_SERVICE = "SortService";

    public static void main(String[] args) throws IOException {

        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .errorListener(new ErrorListener() {})
            .build();

        StatsDataSupplier sds = new ExampleStatsDataSupplier();
        StatsDataDecoder sdd = new ExampleStatsDataDecoder();

        try (Connection nc = Nats.connect(options)) {
            Service serviceEcho = ServiceCreator.instance()
                .connection(nc)
                .name(ECHO_SERVICE)
                .subject(ECHO_SERVICE)
                .description("An Echo Service")
                .version("0.0.1")
                .schemaRequest("echo schema request string/url")
                .schemaResponse("echo schema response string/url")
                .statsDataHandlers(sds, sdd)
                .serviceMessageHandler(msg -> ServiceMessage.reply(nc, msg, "Echo " + new String(msg.getData())))
                .build();

            System.out.println(getFormatted(serviceEcho));

            Service serviceSort = ServiceCreator.instance()
                .connection(nc)
                .name(SORT_SERVICE)
                .subject(SORT_SERVICE)
                .description("A Sort Service")
                .version("0.0.2")
                .schemaRequest("sort schema request string/url")
                .schemaResponse("sort schema response string/url")
                .serviceMessageHandler(msg -> {
                    byte[] data = msg.getData();
                    Arrays.sort(data);
                    ServiceMessage.reply(nc, msg, "Sort " + new String(data));
                })
                .build();

            System.out.println("\n" + getFormatted(serviceSort));

            // ----------------------------------------------------------------------------------------------------
            // Start the services
            // ----------------------------------------------------------------------------------------------------
            CompletableFuture<Boolean> doneEcho = serviceEcho.startService();
            CompletableFuture<Boolean> doneSort = serviceSort.startService();

            // ----------------------------------------------------------------------------------------------------
            // Call the services
            // ----------------------------------------------------------------------------------------------------
            callService(nc, ECHO_SERVICE);
            callService(nc, SORT_SERVICE);

            // ----------------------------------------------------------------------------------------------------
            // discovery
            // ----------------------------------------------------------------------------------------------------
            Discovery discovery = new Discovery(nc, 1000, 3);

            // ----------------------------------------------------------------------------------------------------
            // ping discover variations
            // ----------------------------------------------------------------------------------------------------
            report("Ping", "All", discovery.ping());

            List<Ping> pings = discovery.ping(ECHO_SERVICE);
            report("Ping", ECHO_SERVICE, pings);

            String echoId = pings.get(0).getServiceId();
            Ping ping = discovery.ping(ECHO_SERVICE, echoId);
            report("Ping", ECHO_SERVICE, echoId, ping);

            pings = discovery.ping(SORT_SERVICE);
            report("Ping", SORT_SERVICE, pings);

            String sortId = pings.get(0).getServiceId();
            ping = discovery.ping(SORT_SERVICE, sortId);
            report("Ping", SORT_SERVICE, sortId, ping);

            // ----------------------------------------------------------------------------------------------------
            // info discover variations
            // ----------------------------------------------------------------------------------------------------
            report("Info", "All", discovery.info());

            List<Info> infos = discovery.info(ECHO_SERVICE);
            report("Info", ECHO_SERVICE, infos);

            Info info = discovery.info(ECHO_SERVICE, echoId);
            report("Info", ECHO_SERVICE, echoId, info);

            infos = discovery.info(SORT_SERVICE);
            report("Info", SORT_SERVICE, infos);

            info = discovery.info(SORT_SERVICE, sortId);
            report("Info", SORT_SERVICE, sortId, info);

            // ----------------------------------------------------------------------------------------------------
            // schema discover variations
            // ----------------------------------------------------------------------------------------------------
            report("Schema", "All", discovery.schema());

            List<SchemaInfo> schemaInfos = discovery.schema(ECHO_SERVICE);
            report("Schema", ECHO_SERVICE, schemaInfos);

            SchemaInfo schemaInfo = discovery.schema(ECHO_SERVICE, echoId);
            report("Schema", ECHO_SERVICE, echoId, schemaInfo);

            schemaInfos = discovery.schema(SORT_SERVICE);
            report("Schema", SORT_SERVICE, schemaInfos);

            schemaInfo = discovery.schema(SORT_SERVICE, sortId);
            report("Schema", SORT_SERVICE, sortId, schemaInfo);

            // ----------------------------------------------------------------------------------------------------
            // stats discover variations
            // ----------------------------------------------------------------------------------------------------
            report("Stats", "All", discovery.stats(sdd));
            report("Stats", ECHO_SERVICE, discovery.stats(ECHO_SERVICE)); // will show echo without data decoder
            report("Stats", SORT_SERVICE, discovery.stats(SORT_SERVICE));

            // ----------------------------------------------------------------------------------------------------
            // stop the service
            // ----------------------------------------------------------------------------------------------------
            serviceEcho.stop();
            serviceSort.stop();
            System.out.println();
            System.out.println("Echo service done ? " + doneEcho.get(1, TimeUnit.SECONDS));
            System.out.println("Sort Service done ? " + doneSort.get(1, TimeUnit.SECONDS));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void report(String action, String serviceName, String serviceId, Object o) {
        System.out.println("\n" + action  + " " + serviceName + " " + serviceId);
        printFormatted(o);
    }

    @SuppressWarnings("rawtypes")
    private static void report(String action, String label, List objects) {
        System.out.println("\n" + action + " " + label + " [" + objects.size() + "]");
        for (Object o : objects) {
            System.out.println(getFormatted(o));
        }
    }

    private static void callService(Connection nc, String serviceName) throws InterruptedException, ExecutionException {
        String request = Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()); // just some random text
        CompletableFuture<Message> reply = nc.request(serviceName, request.getBytes());
        String response = new String(reply.get().getData());
        System.out.println("\nReply from " + serviceName + ". Sent [" + request + "] Received [" + response + "]");
    }

    static class ExampleStatsData implements JsonSerializable {
        public String sData;
        public int iData;

        public ExampleStatsData(String sData, int iData) {
            this.sData = sData;
            this.iData = iData;
        }

        public ExampleStatsData(String json) {
            this.sData = JsonUtils.readString(json, string_pattern("sdata"));
            this.iData = JsonUtils.readInt(json, integer_pattern("idata"), -1);
        }

        @Override
        public String toJson() {
            StringBuilder sb = beginJson();
            JsonUtils.addField(sb, "sdata", sData);
            JsonUtils.addField(sb, "idata", iData);
            return endJson(sb).toString();
        }

        @Override
        public String toString() {
            return "ExampleStatsData" + toJson();
        }
    }

    static class ExampleStatsDataSupplier implements StatsDataSupplier {
        int x = 0;
        @Override
        public JsonSerializable get() {
            ++x;
            return new ExampleStatsData("s-" + hashCode(), x);
        }
    }

    static class ExampleStatsDataDecoder implements StatsDataDecoder {
        @Override
        public JsonSerializable decode(String json) {
            ExampleStatsData esd = new ExampleStatsData(json);
            return nullOrEmpty(esd.sData) ? null : esd;
        }
    }
}
