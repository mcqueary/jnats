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

package io.nats.service;

import io.nats.client.*;
import io.nats.client.impl.Headers;
import io.nats.client.impl.JetStreamTestBase;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import io.nats.service.api.*;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.nats.client.impl.NatsPackageScopeWorkarounds.getDispatchers;
import static io.nats.client.support.ApiConstants.ID;
import static io.nats.client.support.ApiConstants.LAST_ERROR;
import static io.nats.client.support.JsonValueUtils.readString;
import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.service.ServiceReplyUtils.NATS_SERVICE_ERROR;
import static io.nats.service.ServiceReplyUtils.NATS_SERVICE_ERROR_CODE;
import static io.nats.service.ServiceUtil.PING;
import static org.junit.jupiter.api.Assertions.*;

public class ServiceTests extends JetStreamTestBase {
    private static final String ECHO_SERVICE_NAME = "EchoService";
    private static final String SORT_SERVICE_NAME = "SortService";
    private static final String ECHO_SERVICE_SUBJECT = "echo";
    private static final String SORT_SERVICE_SUBJECT = "sort";
    private static final String SORT_SERVICE_ASCENDING_SUBJECT = "ascending";
    private static final String SORT_SERVICE_DESCENDING_SUBJECT = "descending";

    @Test
    public void testService() throws Exception {
        try (NatsTestServer ts = new NatsTestServer())
        {
            try (Connection serviceNc1 = standardConnection(ts.getURI());
                 Connection serviceNc2 = standardConnection(ts.getURI());
                 Connection clientNc = standardConnection(ts.getURI())) {

                // construction
                Dispatcher dShared = serviceNc1.createDispatcher(); // services can share dispatchers if the user wants to

                Supplier<StatsData> sds = new TestStatsDataSupplier();
                Function<String, StatsData> sdd = new TestStatsDataDecoder();

                Service echoService1 = echoServiceCreator(serviceNc1, new EchoHandler(serviceNc1))
                    .userServiceDispatcher(dShared)
                    .statsDataHandlers(sds, sdd)
                    .build();
                String echoServiceId1 = echoService1.getId();
                CompletableFuture<Boolean> echoDone1 = echoService1.startService();

                Service sortService1 = sortServiceCreator(serviceNc1, new SortHandler(serviceNc1))
                    .userDiscoveryDispatcher(dShared).build();
                String sortServiceId1 = sortService1.getId();
                CompletableFuture<Boolean> sortDone1 = sortService1.startService();

                Service echoService2 = echoServiceCreator(serviceNc2, new EchoHandler(serviceNc1))
                    .statsDataHandlers(sds, sdd)
                    .build();
                String echoServiceId2 = echoService2.getId();
                CompletableFuture<Boolean> echoDone2 = echoService2.startService();

                Service sortService2 = sortServiceCreator(serviceNc2, new SortHandler(serviceNc2)).build();
                String sortServiceId2 = sortService2.getId();
                CompletableFuture<Boolean> sortDone2 = sortService2.startService();

                assertNotEquals(echoServiceId1, echoServiceId2);
                assertNotEquals(sortServiceId1, sortServiceId2);

                // service request execution
                int requestCount = 10;
                for (int x = 0; x < requestCount; x++) {
                    verifyServiceExecution(clientNc, ECHO_SERVICE_SUBJECT);
                    verifyServiceExecution(clientNc, SORT_SERVICE_SUBJECT);
                }

                InfoResponse echoInfoResponse = echoService1.getInfoResponse();
                InfoResponse sortInfoResponse = sortService1.getInfoResponse();
                SchemaResponse echoSchemaResponse = echoService1.getSchemaResponse();
                SchemaResponse sortSchemaResponse = sortService1.getSchemaResponse();

                // discovery - wait at most 500 millis for responses, 5 total responses max
                Discovery discovery = new Discovery(clientNc, 500, 5);

                // ping discovery
                InfoVerifier pingValidator = (expectedInfoResponse, o) -> {
                    assertTrue(o instanceof PingResponse);
                    PingResponse p = (PingResponse)o;
                    if (expectedInfoResponse != null) {
                        assertEquals(expectedInfoResponse.getName(), p.getName());
                        assertEquals(PingResponse.TYPE, p.getType());
                        assertEquals(expectedInfoResponse.getVersion(), p.getVersion());
                    }
                    return p.getId();
                };
                verifyDiscovery(null, discovery.ping(), pingValidator, echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
                verifyDiscovery(echoInfoResponse, discovery.ping(ECHO_SERVICE_NAME), pingValidator, echoServiceId1, echoServiceId2);
                verifyDiscovery(sortInfoResponse, discovery.ping(SORT_SERVICE_NAME), pingValidator, sortServiceId1, sortServiceId2);
                verifyDiscovery(echoInfoResponse, discovery.ping(ECHO_SERVICE_NAME, echoServiceId1), pingValidator, echoServiceId1);
                verifyDiscovery(sortInfoResponse, discovery.ping(SORT_SERVICE_NAME, sortServiceId1), pingValidator, sortServiceId1);
                verifyDiscovery(echoInfoResponse, discovery.ping(ECHO_SERVICE_NAME, echoServiceId2), pingValidator, echoServiceId2);
                verifyDiscovery(sortInfoResponse, discovery.ping(SORT_SERVICE_NAME, sortServiceId2), pingValidator, sortServiceId2);

                // info discovery
                InfoVerifier infoValidator = (expectedInfoResponse, o) -> {
                    assertTrue(o instanceof InfoResponse);
                    InfoResponse i = (InfoResponse)o;
                    if (expectedInfoResponse != null) {
                        assertEquals(expectedInfoResponse.getName(), i.getName());
                        assertEquals(InfoResponse.TYPE, i.getType());
                        assertEquals(expectedInfoResponse.getDescription(), i.getDescription());
                        assertEquals(expectedInfoResponse.getVersion(), i.getVersion());
                        assertEquals(expectedInfoResponse.getSubject(), i.getSubject());
                    }
                    return i.getId();
                };
                verifyDiscovery(null, discovery.info(), infoValidator, echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
                verifyDiscovery(echoInfoResponse, discovery.info(ECHO_SERVICE_NAME), infoValidator, echoServiceId1, echoServiceId2);
                verifyDiscovery(sortInfoResponse, discovery.info(SORT_SERVICE_NAME), infoValidator, sortServiceId1, sortServiceId2);
                verifyDiscovery(echoInfoResponse, discovery.info(ECHO_SERVICE_NAME, echoServiceId1), infoValidator, echoServiceId1);
                verifyDiscovery(sortInfoResponse, discovery.info(SORT_SERVICE_NAME, sortServiceId1), infoValidator, sortServiceId1);
                verifyDiscovery(echoInfoResponse, discovery.info(ECHO_SERVICE_NAME, echoServiceId2), infoValidator, echoServiceId2);
                verifyDiscovery(sortInfoResponse, discovery.info(SORT_SERVICE_NAME, sortServiceId2), infoValidator, sortServiceId2);

                // schema discovery
                SchemaInfoVerifier schemaValidator = (expectedSchemaResponse, o) -> {
                    assertTrue(o instanceof SchemaResponse);
                    SchemaResponse sr = (SchemaResponse)o;
                    if (expectedSchemaResponse != null) {
                        assertEquals(SchemaResponse.TYPE, sr.getType());
                        assertEquals(expectedSchemaResponse.getName(), sr.getName());
                        assertEquals(expectedSchemaResponse.getVersion(), sr.getVersion());
                        assertEquals(expectedSchemaResponse.getSchema().getRequest(), sr.getSchema().getRequest());
                        assertEquals(expectedSchemaResponse.getSchema().getResponse(), sr.getSchema().getResponse());
                    }
                    return sr.getServiceId();
                };
                verifyDiscovery(null, discovery.schema(), schemaValidator, echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
                verifyDiscovery(echoSchemaResponse, discovery.schema(ECHO_SERVICE_NAME), schemaValidator, echoServiceId1, echoServiceId2);
                verifyDiscovery(sortSchemaResponse, discovery.schema(SORT_SERVICE_NAME), schemaValidator, sortServiceId1, sortServiceId2);
                verifyDiscovery(echoSchemaResponse, discovery.schema(ECHO_SERVICE_NAME, echoServiceId1), schemaValidator, echoServiceId1);
                verifyDiscovery(sortSchemaResponse, discovery.schema(SORT_SERVICE_NAME, sortServiceId1), schemaValidator, sortServiceId1);
                verifyDiscovery(echoSchemaResponse, discovery.schema(ECHO_SERVICE_NAME, echoServiceId2), schemaValidator, echoServiceId2);
                verifyDiscovery(sortSchemaResponse, discovery.schema(SORT_SERVICE_NAME, sortServiceId2), schemaValidator, sortServiceId2);

                // stats discovery
                discovery = new Discovery(clientNc); // coverage for the simple constructor
                List<StatsResponse> srList = discovery.stats(sdd);
                assertEquals(4, srList.size());
                int responseEcho = 0;
                int responseSort = 0;
                long requestsEcho = 0;
                long requestsSort = 0;
                for (StatsResponse statsResponse : srList) {
                    if (statsResponse.getName().equals(ECHO_SERVICE_NAME)) {
                        responseEcho++;
                        requestsEcho += statsResponse.getNumRequests();
                        assertNotNull(statsResponse.getData());
                        assertTrue(statsResponse.getData() instanceof TestStatsData);
                    }
                    else {
                        responseSort++;
                        requestsSort += statsResponse.getNumRequests();
                    }
                    assertEquals(StatsResponse.TYPE, statsResponse.getType());
                }
                assertEquals(2, responseEcho);
                assertEquals(2, responseSort);
                assertEquals(requestCount, requestsEcho);
                assertEquals(requestCount, requestsSort);

                // stats one specific instance so I can also test reset
                StatsResponse sr = discovery.stats(ECHO_SERVICE_NAME, echoServiceId1);
                assertEquals(echoServiceId1, sr.getServiceId());
                assertEquals(echoInfoResponse.getVersion(), sr.getVersion());

                // reset stats
                echoService1.reset();
                sr = echoService1.getStats();
                assertEquals(0, sr.getNumRequests());
                assertEquals(0, sr.getNumErrors());
                assertEquals(0, sr.getProcessingTime());
                assertEquals(0, sr.getAverageProcessingTime());
                assertNull(sr.getData());

                sr = discovery.stats(ECHO_SERVICE_NAME, echoServiceId1);
                assertEquals(0, sr.getNumRequests());
                assertEquals(0, sr.getNumErrors());
                assertEquals(0, sr.getProcessingTime());
                assertEquals(0, sr.getAverageProcessingTime());

                // shutdown
                Map<String, Dispatcher> dispatchers = getDispatchers(serviceNc1);
                assertEquals(3, dispatchers.size()); // user supplied plus echo discovery plus sort discovery
                dispatchers = getDispatchers(serviceNc2);
                assertEquals(4, dispatchers.size()); // echo service, echo discovery, sort service, sort discovery

                sortService1.stop();
                sortDone1.get();
                dispatchers = getDispatchers(serviceNc1);
                assertEquals(2, dispatchers.size()); // user supplied plus echo discovery
                dispatchers = getDispatchers(serviceNc2);
                assertEquals(4, dispatchers.size()); // echo service, echo discovery, sort service, sort discovery

                echoService1.stop(null); // coverage of public void stop(Throwable t)
                echoDone1.get();
                dispatchers = getDispatchers(serviceNc1);
                assertEquals(1, dispatchers.size()); // user supplied is not managed by the service since it was supplied by the user
                dispatchers = getDispatchers(serviceNc2);
                assertEquals(4, dispatchers.size());  // echo service, echo discovery, sort service, sort discovery

                sortService2.stop(true); // coverage of public void stop(boolean drain)
                sortDone2.get();
                dispatchers = getDispatchers(serviceNc1);
                assertEquals(1, dispatchers.size()); // no change so just user supplied
                dispatchers = getDispatchers(serviceNc2);
                assertEquals(2, dispatchers.size());  // echo service, echo discovery

                echoService2.stop(new Exception()); // coverage
                assertThrows(ExecutionException.class, echoDone2::get);
                dispatchers = getDispatchers(serviceNc1);
                assertEquals(1, dispatchers.size()); // no change so user supplied
                dispatchers = getDispatchers(serviceNc2);
                assertEquals(0, dispatchers.size());  // no user supplied
            }
        }
    }

    private static ServiceBuilder echoServiceCreator(Connection nc, MessageHandler handler) {
        return new ServiceBuilder()
            .connection(nc)
            .name(ECHO_SERVICE_NAME)
            .rootSubject(ECHO_SERVICE_SUBJECT)
            .description("An Echo Service")
            .version("0.0.1")
            .schemaRequest("echo schema request string/url")
            .schemaResponse("echo schema response string/url")
            .rootMessageHandler(handler);
    }

    private static ServiceBuilder sortServiceCreator(Connection nc, MessageHandler handler) {
        return new ServiceBuilder()
            .connection(nc)
            .name(SORT_SERVICE_NAME)
            .rootSubject(SORT_SERVICE_SUBJECT)
            .description("A Sort Service")
            .version("0.0.2")
            .schemaRequest("sort schema request string/url")
            .schemaResponse("sort schema response string/url")
            .rootMessageHandler(handler);
    }

    interface InfoVerifier {
        String verify(InfoResponse expectedInfoResponse, Object o);
    }

    interface SchemaInfoVerifier {
        String verify(SchemaResponse expectedSchemaResponse, Object o);
    }

    private static void verifyDiscovery(InfoResponse expectedInfoResponse, Object object, InfoVerifier iv, String... expectedIds) {
        verifyDiscovery(expectedInfoResponse, Collections.singletonList(object), iv, expectedIds);
    }

    private static void verifyDiscovery(SchemaResponse expectedSchemaResponse, Object object, SchemaInfoVerifier siv, String... expectedIds) {
        verifyDiscovery(expectedSchemaResponse, Collections.singletonList(object), siv, expectedIds);
    }

    @SuppressWarnings("rawtypes")
    private static void verifyDiscovery(InfoResponse expectedInfoResponse, List objects, InfoVerifier iv, String... expectedIds) {
        List<String> expectedList = Arrays.asList(expectedIds);
        assertEquals(expectedList.size(), objects.size());
        for (Object o : objects) {
            String id = iv.verify(expectedInfoResponse, o);
            assertTrue(expectedList.contains(id));
        }
    }

    @SuppressWarnings("rawtypes")
    private static void verifyDiscovery(SchemaResponse expectedSchemaResponse, List objects, SchemaInfoVerifier siv, String... expectedIds) {
        List<String> expectedList = Arrays.asList(expectedIds);
        assertEquals(expectedList.size(), objects.size());
        for (Object o : objects) {
            String id = siv.verify(expectedSchemaResponse, o);
            assertTrue(expectedList.contains(id));
        }
    }

    private static void verifyServiceExecution(Connection nc, String serviceSubject) {
        try {
            String request = Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()); // just some random text
            CompletableFuture<Message> future = nc.request(serviceSubject, request.getBytes());
            Message m = future.get();
            String response = new String(m.getData());
            String expected;
            switch (serviceSubject) {
                case SORT_SERVICE_SUBJECT:
                case SORT_SERVICE_SUBJECT + DOT + SORT_SERVICE_ASCENDING_SUBJECT:
                    expected = sortAscending(request);
                    break;
                case SORT_SERVICE_SUBJECT + DOT + SORT_SERVICE_DESCENDING_SUBJECT:
                    expected = sortDescending(request);
                    break;
                case ECHO_SERVICE_SUBJECT:
                default:
                    expected = echo(request);
            }
            assertEquals(expected, response);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class EchoHandler implements MessageHandler {
        Connection conn;

        public EchoHandler(Connection conn) {
            this.conn = conn;
        }

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            ServiceReplyUtils.reply(conn, msg, echo(msg.getData()), new Headers().put("handlerId", Integer.toString(hashCode())));
        }
    }

    static class SortHandler implements MessageHandler {
        Connection conn;

        public SortHandler(Connection conn) {
            this.conn = conn;
        }

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            ServiceReplyUtils.reply(conn, msg, sortAscending(msg.getData()), new Headers().put("handlerId", Integer.toString(hashCode())));
        }
    }

    private static String echo(String data) {
        return "Echo " + data;
    }

    private static String echo(byte[] data) {
        return echo(new String(data));
    }

    private static String sortAscending(byte[] data) {
        Arrays.sort(data);
        return "Sort Ascending " + new String(data);
    }

    private static String sortAscending(String data) {
        return sortAscending(data.getBytes(StandardCharsets.UTF_8));
    }

    private static String sortDescending(byte[] data) {
        Arrays.sort(data);
        int len = data.length;
        byte[] reverse = new byte[len];
        for (int x = 0; x < len; x++) {
            reverse[x] = data[len - x - 1];
        }
        return "Sort Descending " + new String(reverse);
    }

    private static String sortDescending(String data) {
        return sortDescending(data.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testMultipleEndpoints() throws Exception {
        runInServer(nc -> {
            Service sortService = new ServiceBuilder()
                .connection(nc)
                .name(SORT_SERVICE_NAME)
                .rootSubject(SORT_SERVICE_SUBJECT)
                .description("A Sort Service")
                .version("0.0.2")
                .schemaRequest("sort schema request string/url")
                .schemaResponse("sort schema response string/url")
                .endpoint(SORT_SERVICE_ASCENDING_SUBJECT, msg -> ServiceReplyUtils.reply(nc, msg, sortAscending(msg.getData())))
                .endpoint(SORT_SERVICE_DESCENDING_SUBJECT, msg -> ServiceReplyUtils.reply(nc, msg, sortDescending(msg.getData())))
                .build();
            sortService.startService();
            sleep(200);

            verifyServiceExecution(nc, SORT_SERVICE_SUBJECT + DOT + SORT_SERVICE_ASCENDING_SUBJECT);
            verifyServiceExecution(nc, SORT_SERVICE_SUBJECT + DOT + SORT_SERVICE_DESCENDING_SUBJECT);

            sortService.stop(false);
        });
    }

    @Test
    public void testHandlerException() throws Exception {
        runInServer(nc -> {
            Service devexService = new ServiceBuilder()
                .connection(nc)
                .name("HandlerExceptionService")
                .rootSubject("hesSubject")
                .version("0.0.1")
                .rootMessageHandler(m-> { throw new RuntimeException("handler-problem"); })
                .build();
            devexService.startService();

            CompletableFuture<Message> future = nc.request("hesSubject", null);
            Message m = future.get();
            assertEquals("handler-problem", m.getHeaders().getFirst(NATS_SERVICE_ERROR));
            assertEquals("500", m.getHeaders().getFirst(NATS_SERVICE_ERROR_CODE));
            assertEquals(1, devexService.getStats().getNumRequests());
            assertEquals(1, devexService.getStats().getNumErrors());
            assertEquals("java.lang.RuntimeException: handler-problem", devexService.getStats().getLastError());
        });
    }

    @Test
    public void testServiceCreatorValidation() throws Exception {
        runInServer(nc -> {
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(null, m -> {}).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, null).version("").build());

            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).version(null).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).version(EMPTY).build());

            echoServiceCreator(nc, m -> {}).name(PLAIN);
            echoServiceCreator(nc, m -> {}).name(HAS_DASH);
            echoServiceCreator(nc, m -> {}).name(HAS_UNDER);

            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).name(null).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).name(EMPTY).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).name(HAS_SPACE).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).name(HAS_PRINTABLE).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).name(HAS_DOT).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).name(HAS_STAR).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).name(HAS_GT).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).name(HAS_DOLLAR).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).name(HAS_LOW).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).name(HAS_127).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).name(HAS_FWD_SLASH).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).name(HAS_BACK_SLASH).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).name(HAS_EQUALS).build());
            assertThrows(IllegalArgumentException.class, () -> echoServiceCreator(nc, m -> {}).name(HAS_TIC).build());
        });
    }

    @Test
    public void testToDiscoverySubject() {
        assertEquals("$SRV.PING", ServiceUtil.toDiscoverySubject(PING, null, null));
        assertEquals("$SRV.PING.myservice", ServiceUtil.toDiscoverySubject(PING, "myservice", null));
        assertEquals("$SRV.PING.myservice.123", ServiceUtil.toDiscoverySubject(PING, "myservice", "123"));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testApiCoverage() {
        new PingResponse("id", "name", "version").toString();
        new Endpoint("request", "response").toString();
        new InfoResponse("id", "name", "description", "version", "subject").toString();
        assertNull(Endpoint.optionalInstance(null));
    }

    @Test
    public void testApiJsonInOut() {
        PingResponse pr1 = new PingResponse("{\"name\":\"ServiceName\",\"id\":\"serviceId\"}".getBytes());
        PingResponse pr2 = new PingResponse(pr1.toJson().getBytes());
        assertEquals("ServiceName", pr1.getName());
        assertEquals("serviceId", pr1.getId());
        assertEquals(pr1.getName(), pr2.getName());
        assertEquals(pr1.getId(), pr2.getId());

        InfoResponse ir1 = new InfoResponse("{\"name\":\"ServiceName\",\"id\":\"serviceId\",\"description\":\"desc\",\"version\":\"0.0.1\",\"subject\":\"ServiceSubject\"}".getBytes());
        InfoResponse ir2 = new InfoResponse(ir1.toJson().getBytes());
        assertEquals("ServiceName", ir1.getName());
        assertEquals("serviceId", ir1.getId());
        assertEquals("desc", ir1.getDescription());
        assertEquals("0.0.1", ir1.getVersion());
        assertEquals("ServiceSubject", ir1.getSubject());
        assertEquals(ir1.getName(), ir2.getName());
        assertEquals(ir1.getId(), ir2.getId());
        assertEquals(ir1.getDescription(), ir2.getDescription());
        assertEquals(ir1.getVersion(), ir2.getVersion());
        assertEquals(ir1.getSubject(), ir2.getSubject());

        SchemaResponse sr1 = new SchemaResponse("{\"name\":\"ServiceName\",\"id\":\"serviceId\",\"version\":\"0.0.1\",\"schema\":{\"request\":\"rqst\",\"response\":\"rspns\"}}".getBytes());
        SchemaResponse sr2 = new SchemaResponse(sr1.toJson().getBytes());
        assertEquals("ServiceName", sr1.getName());
        assertEquals("serviceId", sr1.getServiceId());
        assertEquals("0.0.1", sr1.getVersion());
        assertEquals("rqst", sr1.getSchema().getRequest());
        assertEquals("rspns", sr1.getSchema().getResponse());
        assertEquals(sr1.getName(), sr2.getName());
        assertEquals(sr1.getServiceId(), sr2.getServiceId());
        assertEquals(sr1.getVersion(), sr2.getVersion());
        assertEquals(sr1.getSchema().getRequest(), sr2.getSchema().getRequest());
        assertEquals(sr1.getSchema().getResponse(), sr2.getSchema().getResponse());

        sr1 = new SchemaResponse("{\"name\":\"ServiceName\",\"id\":\"serviceId\",\"version\":\"0.0.1\"}".getBytes());
        sr2 = new SchemaResponse(sr1.toJson().getBytes());
        assertEquals("ServiceName", sr1.getName());
        assertEquals("serviceId", sr1.getServiceId());
        assertEquals("0.0.1", sr1.getVersion());
        assertEquals(sr1.getName(), sr2.getName());
        assertEquals(sr1.getServiceId(), sr2.getServiceId());
        assertEquals(sr1.getVersion(), sr2.getVersion());
        assertNull(sr1.getSchema());
        assertNull(sr2.getSchema());

        TestStatsDataDecoder sdd = new TestStatsDataDecoder();
        String statsJson = "{\"name\":\"ServiceName\",\"id\":\"serviceId\",\"version\":\"0.0.1\",\"num_requests\":1,\"num_errors\":2,\"last_error\":\"npe\",\"processing_time\":3,\"average_processing_time\":4,\"data\":{\"id\":\"user id\",\"last_error\":\"user last error\"},\"started\":\"2022-12-20T13:37:18.568000000Z\"}";
        StatsResponse statsResponse1 = new StatsResponse(statsJson.getBytes(), sdd);
        StatsResponse statsResponse2 = new StatsResponse(statsResponse1.toJson().getBytes(), sdd);
        assertEquals("ServiceName", statsResponse1.getName());
        assertEquals("serviceId", statsResponse1.getServiceId());
        assertEquals("0.0.1", statsResponse1.getVersion());
        assertEquals(statsResponse1.getName(), statsResponse2.getName());
        assertEquals(statsResponse1.getServiceId(), statsResponse2.getServiceId());
        assertEquals(statsResponse1.getVersion(), statsResponse2.getVersion());
        assertEquals(1, statsResponse1.getNumRequests());
        assertEquals(1, statsResponse2.getNumRequests());
        assertEquals(2, statsResponse1.getNumErrors());
        assertEquals(2, statsResponse2.getNumErrors());
        assertEquals("npe", statsResponse1.getLastError());
        assertEquals("npe", statsResponse2.getLastError());
        assertEquals(3, statsResponse1.getProcessingTime());
        assertEquals(3, statsResponse2.getProcessingTime());
        assertEquals(4, statsResponse1.getAverageProcessingTime());
        assertEquals(4, statsResponse2.getAverageProcessingTime());
        assertTrue(statsResponse1.getData() instanceof TestStatsData);
        assertTrue(statsResponse2.getData() instanceof TestStatsData);
        TestStatsData data1 = (TestStatsData) statsResponse1.getData();
        TestStatsData data2 = (TestStatsData) statsResponse2.getData();
        assertEquals("user id", data1.id);
        assertEquals("user id", data2.id);
        assertEquals("user last error", data1.lastError);
        assertEquals("user last error", data2.lastError);
        ZonedDateTime zdt = DateTimeUtils.parseDateTime("2022-12-20T13:37:18.568Z");
        assertEquals(zdt, statsResponse1.getStarted());
        assertEquals(zdt, statsResponse2.getStarted());
    }

    static class TestStatsData {
        // using id and  last_error as field names to ensure that the manual parsing works
        public String id;
        public String lastError;

        public TestStatsData(String id, String lastError) {
            this.id = id;
            this.lastError = lastError;
        }

        public TestStatsData(String json) {
            JsonValue jv = JsonParser.parse(json);
            this.id = readString(jv, ID);
            this.lastError = readString(jv, LAST_ERROR);
        }

        public JsonValue toJsonValue() {
            Map<String, JsonValue> map = new HashMap<>();
            map.put(ID, new JsonValue(id));
            map.put(LAST_ERROR, new JsonValue(lastError));
            return new JsonValue(map);
        }

        @Override
        public String toString() {
            return toJsonValue().toString(getClass());
        }
    }

    static class TestStatsDataSupplier implements Supplier<JsonValue> {
        @Override
        public JsonValue get() {
            return new TestStatsData("" + hashCode(), "blah error [" + System.currentTimeMillis() + "]").toJsonValue();
        }
    }
}
