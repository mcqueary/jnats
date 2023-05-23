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
import io.nats.client.impl.MockNatsConnection;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonValue;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.nats.client.impl.NatsPackageScopeWorkarounds.getDispatchers;
import static io.nats.client.support.JsonUtils.toKey;
import static io.nats.client.support.JsonValueUtils.readInteger;
import static io.nats.client.support.JsonValueUtils.readString;
import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.service.Service.SRV_PING;
import static io.nats.service.ServiceMessage.NATS_SERVICE_ERROR;
import static io.nats.service.ServiceMessage.NATS_SERVICE_ERROR_CODE;
import static org.junit.jupiter.api.Assertions.*;

public class ServiceTests extends JetStreamTestBase {
    public static final String SERVICE_NAME_1 = "Service1";
    public static final String SERVICE_NAME_2 = "Service2";
    public static final String ECHO_ENDPOINT_NAME = "EchoEndpoint";
    public static final String ECHO_ENDPOINT_SUBJECT = "echo";
    public static final String SORT_GROUP = "sort";
    public static final String SORT_ENDPOINT_ASCENDING_NAME = "SortEndpointAscending";
    public static final String SORT_ENDPOINT_DESCENDING_NAME = "SortEndpointDescending";
    public static final String SORT_ENDPOINT_ASCENDING_SUBJECT = "ascending";
    public static final String SORT_ENDPOINT_DESCENDING_SUBJECT = "descending";

    @Test
    public void testServiceWorkflow() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            try (Connection serviceNc1 = standardConnection(ts.getURI());
                 Connection serviceNc2 = standardConnection(ts.getURI());
                 Connection clientNc = standardConnection(ts.getURI())) {

                Endpoint endEcho = Endpoint.builder()
                    .name(ECHO_ENDPOINT_NAME)
                    .subject(ECHO_ENDPOINT_SUBJECT)
                    .build();

                Endpoint endSortA = Endpoint.builder()
                    .name(SORT_ENDPOINT_ASCENDING_NAME)
                    .subject(SORT_ENDPOINT_ASCENDING_SUBJECT)
                    .build();

                // constructor coverage
                Endpoint endSortD = new Endpoint(SORT_ENDPOINT_DESCENDING_NAME, SORT_ENDPOINT_DESCENDING_SUBJECT);

                // sort is going to be grouped
                Group sortGroup = new Group(SORT_GROUP);

                ServiceEndpoint seEcho1 = ServiceEndpoint.builder()
                    .endpoint(endEcho)
                    .handler(new EchoHandler(serviceNc1))
                    .statsDataSupplier(ServiceTests::supplyData)
                    .build();

                ServiceEndpoint seSortA1 = ServiceEndpoint.builder()
                    .group(sortGroup)
                    .endpoint(endSortA)
                    .handler(new SortHandlerA(serviceNc1))
                    .build();

                ServiceEndpoint seSortD1 = ServiceEndpoint.builder()
                    .group(sortGroup)
                    .endpoint(endSortD)
                    .handler(new SortHandlerD(serviceNc1))
                    .build();

                ServiceEndpoint seEcho2 = ServiceEndpoint.builder()
                    .endpoint(endEcho)
                    .handler(new EchoHandler(serviceNc2))
                    .statsDataSupplier(ServiceTests::supplyData)
                    .build();

                // build variations
                ServiceEndpoint seSortA2 = ServiceEndpoint.builder()
                    .group(sortGroup)
                    .endpointName(endSortA.getName())
                    .endpointSubject(endSortA.getSubject())
                    .handler(new SortHandlerA(serviceNc2))
                    .build();

                ServiceEndpoint seSortD2 = ServiceEndpoint.builder()
                    .group(sortGroup)
                    .endpointName(endSortD.getName())
                    .endpointSubject(endSortD.getSubject())
                    .handler(new SortHandlerD(serviceNc2))
                    .build();

                Service service1 = new ServiceBuilder()
                    .name(SERVICE_NAME_1)
                    .version("1.0.0")
                    .connection(serviceNc1)
                    .addServiceEndpoint(seEcho1)
                    .addServiceEndpoint(seSortA1)
                    .addServiceEndpoint(seSortD1)
                    .build();
                String serviceId1 = service1.getId();
                CompletableFuture<Boolean> serviceDone1 = service1.startService();

                Service service2 = new ServiceBuilder()
                    .name(SERVICE_NAME_2)
                    .version("1.0.0")
                    .connection(serviceNc2)
                    .addServiceEndpoint(seEcho2)
                    .addServiceEndpoint(seSortA2)
                    .addServiceEndpoint(seSortD2)
                    .build();
                String serviceId2 = service2.getId();
                CompletableFuture<Boolean> serviceDone2 = service2.startService();

                assertNotEquals(serviceId1, serviceId2);

                sleep(1000); // just make sure services are all started, for slow CI machines

                // service request execution
                int requestCount = 10;
                for (int x = 0; x < requestCount; x++) {
                    verifyServiceExecution(clientNc, ECHO_ENDPOINT_NAME, ECHO_ENDPOINT_SUBJECT, null);
                    verifyServiceExecution(clientNc, SORT_ENDPOINT_ASCENDING_NAME, SORT_ENDPOINT_ASCENDING_SUBJECT, sortGroup);
                    verifyServiceExecution(clientNc, SORT_ENDPOINT_DESCENDING_NAME, SORT_ENDPOINT_DESCENDING_SUBJECT, sortGroup);
                }

                PingResponse pingResponse1 = service1.getPingResponse();
                PingResponse pingResponse2 = service2.getPingResponse();
                InfoResponse infoResponse1 = service1.getInfoResponse();
                InfoResponse infoResponse2 = service2.getInfoResponse();
                StatsResponse statsResponse1 = service1.getStatsResponse();
                StatsResponse statsResponse2 = service2.getStatsResponse();
                EndpointResponse[] endpointResponseArray1 = new EndpointResponse[]{
                    service1.getEndpointStats(ECHO_ENDPOINT_NAME),
                    service1.getEndpointStats(SORT_ENDPOINT_ASCENDING_NAME),
                    service1.getEndpointStats(SORT_ENDPOINT_DESCENDING_NAME)
                };
                EndpointResponse[] endpointResponseArray2 = new EndpointResponse[]{
                    service2.getEndpointStats(ECHO_ENDPOINT_NAME),
                    service2.getEndpointStats(SORT_ENDPOINT_ASCENDING_NAME),
                    service2.getEndpointStats(SORT_ENDPOINT_DESCENDING_NAME)
                };
                assertNull(service1.getEndpointStats("notAnEndpoint"));

                assertEquals(serviceId1, pingResponse1.getId());
                assertEquals(serviceId2, pingResponse2.getId());
                assertEquals(serviceId1, infoResponse1.getId());
                assertEquals(serviceId2, infoResponse2.getId());
                assertEquals(serviceId1, statsResponse1.getId());
                assertEquals(serviceId2, statsResponse2.getId());

                // this relies on the fact that I load the endpoints up in the service
                // in the same order and the json list comes back ordered
                // expecting 10 responses across each endpoint between 2 services
                for (int x = 0; x < 3; x++) {
                    assertEquals(requestCount,
                        endpointResponseArray1[x].getNumRequests()
                            + endpointResponseArray2[x].getNumRequests());
                    assertEquals(requestCount,
                        statsResponse1.getEndpointStats().get(x).getNumRequests()
                            + statsResponse2.getEndpointStats().get(x).getNumRequests());
                }

                // discovery - wait at most 500 millis for responses, 5 total responses max
                Discovery discovery = new Discovery(clientNc, 500, 5);

                // ping discovery
                Verifier pingVerifier = (expected, response) -> assertTrue(response instanceof PingResponse);
                verifyDiscovery(discovery.ping(), pingVerifier, pingResponse1, pingResponse2);
                verifyDiscovery(discovery.ping(SERVICE_NAME_1), pingVerifier, pingResponse1);
                verifyDiscovery(discovery.ping(SERVICE_NAME_2), pingVerifier, pingResponse2);
                verifyDiscovery(discovery.ping(SERVICE_NAME_1, serviceId1), pingVerifier, pingResponse1);
                assertNull(discovery.ping(SERVICE_NAME_1, "badId"));
                assertNull(discovery.ping("bad", "badId"));

                // info discovery
                Verifier infoVerifier = (expected, response) -> {
                    assertTrue(response instanceof InfoResponse);
                    InfoResponse exp = (InfoResponse) expected;
                    InfoResponse r = (InfoResponse) response;
                    assertEquals(exp.getDescription(), r.getDescription());
                    assertEquals(exp.getSubjects(), r.getSubjects());
                };
                verifyDiscovery(discovery.info(), infoVerifier, infoResponse1, infoResponse2);
                verifyDiscovery(discovery.info(SERVICE_NAME_1), infoVerifier, infoResponse1);
                verifyDiscovery(discovery.info(SERVICE_NAME_2), infoVerifier, infoResponse2);
                verifyDiscovery(discovery.info(SERVICE_NAME_1, serviceId1), infoVerifier, infoResponse1);
                assertNull(discovery.info(SERVICE_NAME_1, "badId"));
                assertNull(discovery.info("bad", "badId"));

                // stats discovery
                Verifier statsVerifier = (expected, response) -> {
                    assertTrue(response instanceof StatsResponse);
                    StatsResponse exp = (StatsResponse) expected;
                    StatsResponse sr = (StatsResponse) response;
                    assertEquals(exp.getStarted(), sr.getStarted());
                    for (int x = 0; x < 3; x++) {
                        EndpointResponse er = exp.getEndpointStats().get(x);
                        if (!er.getName().equals(ECHO_ENDPOINT_NAME)) {
                            // echo endpoint has data that will vary
                            assertEquals(er, sr.getEndpointStats().get(x));
                        }
                    }
                };
                discovery = new Discovery(clientNc); // coverage for the simple constructor
                verifyDiscovery(discovery.stats(), statsVerifier, statsResponse1, statsResponse2);
                verifyDiscovery(discovery.stats(SERVICE_NAME_1), statsVerifier, statsResponse1);
                verifyDiscovery(discovery.stats(SERVICE_NAME_2), statsVerifier, statsResponse2);
                verifyDiscovery(discovery.stats(SERVICE_NAME_1, serviceId1), statsVerifier, statsResponse1);
                assertNull(discovery.stats(SERVICE_NAME_1, "badId"));
                assertNull(discovery.stats("bad", "badId"));

                // test reset
                ZonedDateTime zdt = DateTimeUtils.gmtNow();
                sleep(1);
                service1.reset();
                StatsResponse sr = service1.getStatsResponse();
                assertTrue(zdt.isBefore(sr.getStarted()));
                for (int x = 0; x < 3; x++) {
                    EndpointResponse er = sr.getEndpointStats().get(x);
                    assertEquals(0, er.getNumRequests());
                    assertEquals(0, er.getNumErrors());
                    assertEquals(0, er.getProcessingTime());
                    assertEquals(0, er.getAverageProcessingTime());
                    assertNull(er.getLastError());
                    if (er.getName().equals(ECHO_ENDPOINT_NAME)) {
                        assertNotNull(er.getData());
                    }
                    else {
                        assertNull(er.getData());
                    }
                    assertTrue(zdt.isBefore(er.getStarted()));
                }

                // shutdown
                service1.stop();
                serviceDone1.get();
                service2.stop(new RuntimeException("Testing stop(Throwable t)"));
                ExecutionException ee = assertThrows(ExecutionException.class, serviceDone2::get);
                assertTrue(ee.getMessage().contains("Testing stop(Throwable t)"));
            }
        }
    }

    interface Verifier {
        void verify(ServiceResponse expected, Object response);
    }

    @SuppressWarnings("unchecked")
    private static void verifyDiscovery(Object oResponse, Verifier v, ServiceResponse... expectedResponses) {
        List<Object> responses = oResponse instanceof List ? (List<Object>) oResponse : Collections.singletonList(oResponse);
        assertEquals(expectedResponses.length, responses.size());
        for (Object response : responses) {
            ServiceResponse sr = (ServiceResponse) response;
            ServiceResponse exp = find(expectedResponses, sr);
            assertNotNull(exp);
            assertEquals(exp.getType(), sr.getType());
            assertEquals(exp.getName(), sr.getName());
            assertEquals(exp.getVersion(), sr.getVersion());
            v.verify(exp, response);
        }
    }

    private static ServiceResponse find(ServiceResponse[] expectedResponses, ServiceResponse response) {
        for (ServiceResponse sr : expectedResponses) {
            if (response.id.equals(sr.id)) {
                return sr;
            }
        }
        return null;
    }

    private static void verifyServiceExecution(Connection nc, String endpointName, String serviceSubject, Group group) {
        try {
            String request = Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()); // just some random text
            String subject = group == null ? serviceSubject : group.getSubject() + DOT + serviceSubject;
            CompletableFuture<Message> future = nc.request(subject, request.getBytes());
            Message m = future.get();
            String response = new String(m.getData());
            switch (endpointName) {
                case ECHO_ENDPOINT_NAME:
                    assertEquals(echo(request), response);
                    break;
                case SORT_ENDPOINT_ASCENDING_NAME:
                    assertEquals(sortA(request), response);
                    break;
                case SORT_ENDPOINT_DESCENDING_NAME:
                    assertEquals(sortD(request), response);
                    break;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class EchoHandler implements ServiceMessageHandler {
        Connection conn;

        public EchoHandler(Connection conn) {
            this.conn = conn;
        }

        @Override
        public void onMessage(ServiceMessage smsg) {
            smsg.respond(conn, echo(smsg.getData()));
        }
    }

    static class SortHandlerA implements ServiceMessageHandler {
        Connection conn;

        public SortHandlerA(Connection conn) {
            this.conn = conn;
        }

        @Override
        public void onMessage(ServiceMessage smsg) {
            smsg.respond(conn, sortA(smsg.getData()));
        }
    }

    static class SortHandlerD implements ServiceMessageHandler {
        Connection conn;

        public SortHandlerD(Connection conn) {
            this.conn = conn;
        }

        @Override
        public void onMessage(ServiceMessage smsg) {
            smsg.respond(conn, sortD(smsg.getData()));
        }
    }

    private static String echo(String data) {
        return "Echo " + data;
    }

    private static String echo(byte[] data) {
        return echo(new String(data));
    }

    private static String sortA(byte[] data) {
        Arrays.sort(data);
        return "Sort Ascending " + new String(data);
    }

    private static String sortA(String data) {
        return sortA(data.getBytes(StandardCharsets.UTF_8));
    }

    private static String sortD(byte[] data) {
        Arrays.sort(data);
        int len = data.length;
        byte[] descending = new byte[len];
        for (int x = 0; x < len; x++) {
            descending[x] = data[len - x - 1];
        }
        return "Sort Descending " + new String(descending);
    }

    private static String sortD(String data) {
        return sortD(data.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testDispatchers() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            try (Connection nc = standardConnection(ts.getURI())) {

                Map<String, Dispatcher> dispatchers = getDispatchers(nc);
                assertEquals(0, dispatchers.size());

                Dispatcher dPing = nc.createDispatcher();
                Dispatcher dInfo = nc.createDispatcher();
                Dispatcher dSchema = nc.createDispatcher();
                Dispatcher dStats = nc.createDispatcher();
                Dispatcher dEnd = nc.createDispatcher();

                dispatchers = getDispatchers(nc);
                assertEquals(5, dispatchers.size());

                ServiceEndpoint se1 = ServiceEndpoint.builder()
                    .endpointName("dispatch")
                    .handler(m -> {
                    })
                    .dispatcher(dEnd)
                    .build();
                Service service = new ServiceBuilder()
                    .connection(nc)
                    .name("testDispatchers")
                    .version("0.0.1")
                    .addServiceEndpoint(se1)
                    .pingDispatcher(dPing)
                    .infoDispatcher(dInfo)
                    .schemaDispatcher(dSchema)
                    .statsDispatcher(dStats)
                    .build();

                CompletableFuture<Boolean> done = service.startService();
                sleep(100); // give the service time to spin up
                service.stop(false); // no need to drain, plus // Coverage
                done.get(100, TimeUnit.MILLISECONDS);

                dispatchers = getDispatchers(nc);
                assertEquals(5, dispatchers.size()); // stop doesn't touch supplied dispatchers

                nc.closeDispatcher(dPing);
                nc.closeDispatcher(dInfo);
                nc.closeDispatcher(dSchema);
                sleep(100); // no rush

                dispatchers = getDispatchers(nc);
                assertEquals(2, dispatchers.size()); // dEnd and dStats
                assertTrue(dispatchers.containsValue(dStats));
                assertTrue(dispatchers.containsValue(dEnd));

                service = new ServiceBuilder()
                    .connection(nc)
                    .name("testDispatchers")
                    .version("0.0.1")
                    .addServiceEndpoint(se1)
                    .statsDispatcher(dStats)
                    .build();

                dispatchers = getDispatchers(nc);
                assertEquals(3, dispatchers.size()); // endpoint, stats, internal discovery

                done = service.startService();
                sleep(100); // give the service time to spin up
                service.stop(); // Coverage
                done.get(100, TimeUnit.MILLISECONDS);

                dispatchers = getDispatchers(nc);
                assertEquals(2, dispatchers.size()); // internal discovery was stopped
                assertTrue(dispatchers.containsValue(dStats));
                assertTrue(dispatchers.containsValue(dEnd));

                nc.closeDispatcher(dStats);
                nc.closeDispatcher(dEnd);
                sleep(100); // no rush

                dispatchers = getDispatchers(nc);
                assertEquals(0, dispatchers.size());

                se1 = ServiceEndpoint.builder()
                    .endpointName("dispatch")
                    .handler(m -> {
                    })
                    .build();

                ServiceEndpoint se2 = ServiceEndpoint.builder()
                    .endpointName("another")
                    .handler(m -> {
                    })
                    .build();

                service = new ServiceBuilder()
                    .connection(nc)
                    .name("testDispatchers")
                    .version("0.0.1")
                    .addServiceEndpoint(se1)
                    .addServiceEndpoint(se2)
                    .build();

                dispatchers = getDispatchers(nc);
                assertEquals(2, dispatchers.size()); // 1 internal discovery and 1 internal endpoints

                done = service.startService();
                sleep(100); // give the service time to spin up
                service.stop(); // Coverage
                done.get(100, TimeUnit.MILLISECONDS);

                dispatchers = getDispatchers(nc);
                assertEquals(0, dispatchers.size()); // service cleans up internal dispatchers
            }
        }
    }

    @Test
    public void testServiceBuilderConstruction() {
        Options options = new Options.Builder().build();
        Connection conn = new MockNatsConnection(options);
        ServiceEndpoint se = ServiceEndpoint.builder()
            .endpoint(new Endpoint(name(0)))
            .handler(m -> {
            })
            .build();

        // minimum valid service
        Service service = Service.builder().connection(conn).name(NAME).version("1.0.0").addServiceEndpoint(se).build();
        assertNotNull(service.toString()); // coverage
        assertNotNull(service.getId());
        assertEquals(NAME, service.getName());
        assertEquals(ServiceBuilder.DEFAULT_DRAIN_TIMEOUT, service.getDrainTimeout());
        assertEquals("1.0.0", service.getVersion());
        assertNull(service.getDescription());

        service = Service.builder().connection(conn).name(NAME).version("1.0.0").addServiceEndpoint(se)
            .description("desc")
            .drainTimeout(Duration.ofSeconds(1))
            .build();
        assertEquals("desc", service.getDescription());
        assertEquals(Duration.ofSeconds(1), service.getDrainTimeout());

        assertThrows(IllegalArgumentException.class, () -> Service.builder().name(null));
        assertThrows(IllegalArgumentException.class, () -> Service.builder().name(EMPTY));
        assertThrows(IllegalArgumentException.class, () -> Service.builder().name(HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> Service.builder().name(HAS_PRINTABLE));
        assertThrows(IllegalArgumentException.class, () -> Service.builder().name(HAS_DOT));
        assertThrows(IllegalArgumentException.class, () -> Service.builder().name(HAS_STAR)); // invalid in the middle
        assertThrows(IllegalArgumentException.class, () -> Service.builder().name(HAS_GT)); // invalid in the middle
        assertThrows(IllegalArgumentException.class, () -> Service.builder().name(HAS_DOLLAR));
        assertThrows(IllegalArgumentException.class, () -> Service.builder().name(HAS_LOW));
        assertThrows(IllegalArgumentException.class, () -> Service.builder().name(HAS_127));
        assertThrows(IllegalArgumentException.class, () -> Service.builder().name(HAS_FWD_SLASH));
        assertThrows(IllegalArgumentException.class, () -> Service.builder().name(HAS_BACK_SLASH));
        assertThrows(IllegalArgumentException.class, () -> Service.builder().name(HAS_EQUALS));
        assertThrows(IllegalArgumentException.class, () -> Service.builder().name(HAS_TIC));

        assertThrows(IllegalArgumentException.class, () -> Service.builder().version(null));
        assertThrows(IllegalArgumentException.class, () -> Service.builder().version(EMPTY));
        assertThrows(IllegalArgumentException.class, () -> Service.builder().version("not-semver"));

        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> Service.builder().name(NAME).version("1.0.0").addServiceEndpoint(se).build());
        assertTrue(iae.getMessage().contains("Connection cannot be null or empty"));

        iae = assertThrows(IllegalArgumentException.class,
            () -> Service.builder().connection(conn).version("1.0.0").addServiceEndpoint(se).build());
        assertTrue(iae.getMessage().contains("Name cannot be null or empty"));

        iae = assertThrows(IllegalArgumentException.class,
            () -> Service.builder().connection(conn).name(NAME).addServiceEndpoint(se).build());
        assertTrue(iae.getMessage().contains("Version cannot be null or empty"));

        iae = assertThrows(IllegalArgumentException.class,
            () -> Service.builder().connection(conn).name(NAME).version("1.0.0").build());
        assertTrue(iae.getMessage().contains("Endpoints cannot be null or empty"));
    }

    @Test
    public void testHandlerException() throws Exception {
        runInServer(nc -> {
            ServiceEndpoint exServiceEndpoint = ServiceEndpoint.builder()
                .endpointName("exEndpoint")
                .endpointSubject("exSubject")
                .handler(m -> {
                    throw new RuntimeException("handler-problem");
                })
                .build();

            Service exService = new ServiceBuilder()
                .connection(nc)
                .name("ExceptionService")
                .version("0.0.1")
                .addServiceEndpoint(exServiceEndpoint)
                .build();
            exService.startService();

            CompletableFuture<Message> future = nc.request("exSubject", null);
            Message m = future.get();
            assertEquals("java.lang.RuntimeException: handler-problem", m.getHeaders().getFirst(NATS_SERVICE_ERROR));
            assertEquals("500", m.getHeaders().getFirst(NATS_SERVICE_ERROR_CODE));
            StatsResponse sr = exService.getStatsResponse();
            EndpointResponse er = sr.getEndpointStats().get(0);
            assertEquals(1, er.getNumRequests());
            assertEquals(1, er.getNumErrors());
            assertEquals("java.lang.RuntimeException: handler-problem", er.getLastError());
        });
    }

    @Test
    public void testServiceMessage() throws Exception {
        runInServer(nc -> {
            AtomicInteger which = new AtomicInteger();
            ServiceEndpoint se = ServiceEndpoint.builder()
                .endpointName("testServiceMessage")
                .handler(m -> {
                    // Coverage // just hitting all the reply variations
                    switch (which.incrementAndGet()) {
                        case 1:
                            m.respond(nc, "1".getBytes());
                            break;
                        case 2:
                            m.respond(nc, "2");
                            break;
                        case 3:
                            m.respond(nc, new JsonValue("3"));
                            break;
                        case 4:
                            m.respond(nc, "4".getBytes(), m.getHeaders());
                            break;
                        case 5:
                            m.respond(nc, "5", m.getHeaders());
                            break;
                        case 6:
                            m.respond(nc, new JsonValue("6"), m.getHeaders());
                            break;
                        case 7:
                            // Coverage, Message Interface
                            assertEquals("testServiceMessage", m.getSubject());
                            assertFalse(m.hasHeaders());
                            assertNull(m.getHeaders());
                            assertFalse(m.isStatusMessage());
                            assertNull(m.getStatus());
                            m.isUtf8mode();
                            assertNotNull(m.getSubscription());
                            assertNotNull(m.getSID());
                            assertNotNull(m.getConnection());
                            assertThrows(IllegalStateException.class, m::metaData);
                            assertNull(m.lastAck());
                            assertDoesNotThrow(() -> m.ackSync(Duration.ofMillis(1)));
                            m.ack();
                            m.nak();
                            m.nakWithDelay(Duration.ofMillis(1));
                            m.nakWithDelay(1);
                            m.term();
                            m.inProgress();
                            assertFalse(m.isJetStream());
                            // the actual reply
                            m.respondStandardError(nc, "error", 500);
                            break;
                    }
                })
                .build();

            Service service = new ServiceBuilder()
                .connection(nc)
                .name("testService")
                .version("0.0.1")
                .addServiceEndpoint(se)
                .build();
            service.startService();

            CompletableFuture<Message> future = nc.request("testServiceMessage", null);
            Message m = future.get();
            assertEquals("1", new String(m.getData()));
            assertFalse(m.hasHeaders());

            future = nc.request("testServiceMessage", null);
            m = future.get();
            assertEquals("2", new String(m.getData()));
            assertFalse(m.hasHeaders());

            future = nc.request("testServiceMessage", null);
            m = future.get();
            assertEquals("\"3\"", new String(m.getData()));
            assertFalse(m.hasHeaders());

            Headers h = new Headers().put("h", "4");
            future = nc.request(NatsMessage.builder().subject("testServiceMessage").headers(h).build());
            m = future.get();
            assertEquals("4", new String(m.getData()));
            assertTrue(m.hasHeaders());
            assertEquals("4", m.getHeaders().getFirst("h"));

            h = new Headers().put("h", "5");
            future = nc.request(NatsMessage.builder().subject("testServiceMessage").headers(h).build());
            m = future.get();
            assertEquals("5", new String(m.getData()));
            assertTrue(m.hasHeaders());
            assertEquals("5", m.getHeaders().getFirst("h"));

            h = new Headers().put("h", "6");
            future = nc.request(NatsMessage.builder().subject("testServiceMessage").headers(h).build());
            m = future.get();
            assertEquals("\"6\"", new String(m.getData()));
            assertTrue(m.hasHeaders());
            assertEquals("6", m.getHeaders().getFirst("h"));

            future = nc.request("testServiceMessage", null);
            m = future.get();
            assertEquals(0, m.getData().length);
            assertTrue(m.hasHeaders());
            assertEquals("error", m.getHeaders().getFirst(NATS_SERVICE_ERROR));
            assertEquals("500", m.getHeaders().getFirst(NATS_SERVICE_ERROR_CODE));
        });
    }

    @Test
    public void testEndpointConstruction() {
        EqualsVerifier.simple().forClass(Endpoint.class).verify();

        Endpoint e = new Endpoint(NAME);
        assertEquals(NAME, e.getName());
        assertEquals(NAME, e.getSubject());
        assertEquals(e, Endpoint.builder().endpoint(e).build());
        assertNull(e.getMetadata());

        e = new Endpoint(NAME, SUBJECT);
        assertEquals(NAME, e.getName());
        assertEquals(SUBJECT, e.getSubject());
        assertEquals(e, Endpoint.builder().endpoint(e).build());

        e = Endpoint.builder()
            .name(NAME).subject(SUBJECT)
            .build();
        assertEquals(NAME, e.getName());
        assertEquals(SUBJECT, e.getSubject());
        assertEquals(e, Endpoint.builder().endpoint(e).build());

        Map<String, String> metadata = new HashMap<>();
        e = Endpoint.builder()
            .name(NAME).subject(SUBJECT)
            .metadata(metadata)
            .build();
        assertEquals(NAME, e.getName());
        assertEquals(SUBJECT, e.getSubject());
        assertNull(e.getMetadata());

        metadata.put("k", "v");
        e = Endpoint.builder()
            .name(NAME).subject(SUBJECT)
            .metadata(metadata)
            .build();
        assertEquals(NAME, e.getName());
        assertEquals(SUBJECT, e.getSubject());
        assertTrue(JsonUtils.mapEquals(metadata, e.getMetadata()));

        // some subject testing
        e = new Endpoint(NAME, "foo.>");
        assertEquals("foo.>", e.getSubject());
        e = new Endpoint(NAME, "foo.*");
        assertEquals("foo.*", e.getSubject());

        assertThrows(IllegalArgumentException.class, () -> Endpoint.builder().build());

        // many names are bad
        assertThrows(IllegalArgumentException.class, () -> new Endpoint((String) null));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(EMPTY));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_PRINTABLE));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_DOT));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_STAR)); // invalid in the middle
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_GT)); // invalid in the middle
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_DOLLAR));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_LOW));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_127));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_FWD_SLASH));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_BACK_SLASH));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_EQUALS));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_TIC));

        // fewer subjects are bad
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(NAME, HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(NAME, HAS_LOW));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(NAME, HAS_127));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(NAME, "foo.>.bar")); // gt is not last segment
    }

    @Test
    public void testEndpointResponseConstruction() {
        JsonValue data = new JsonValue("data");
        EqualsVerifier.simple().forClass(EndpointResponse.class)
            .withPrefabValues(JsonValue.class, data, JsonValue.NULL)
            .verify();
        ZonedDateTime zdt = DateTimeUtils.gmtNow();

        EndpointResponse er = new EndpointResponse("name", "subject", 0, 0, 0, null, null, zdt);
        assertEquals("name", er.getName());
        assertEquals("subject", er.getSubject());
        assertNull(er.getLastError());
        assertNull(er.getData());
        assertEquals(0, er.getNumRequests());
        assertEquals(0, er.getNumErrors());
        assertEquals(0, er.getProcessingTime());
        assertEquals(0, er.getAverageProcessingTime());
        assertEquals(zdt, er.getStarted());

        er = new EndpointResponse("name", "subject", 2, 4, 10, "lastError", data, zdt);
        assertEquals("name", er.getName());
        assertEquals("subject", er.getSubject());
        assertEquals("lastError", er.getLastError());
        assertEquals("\"data\"", er.getData().toString());
        assertEquals(2, er.getNumRequests());
        assertEquals(4, er.getNumErrors());
        assertEquals(10, er.getProcessingTime());
        assertEquals(5, er.getAverageProcessingTime());
        assertEquals(zdt, er.getStarted());

        String j = er.toJson();
        assertTrue(j.startsWith("{"));
        assertTrue(j.contains("\"name\":\"name\""));
        assertTrue(j.contains("\"subject\":\"subject\""));
        assertTrue(j.contains("\"last_error\":\"lastError\""));
        assertTrue(j.contains("\"data\":\"data\""));
        assertTrue(j.contains("\"num_requests\":2"));
        assertTrue(j.contains("\"num_errors\":4"));
        assertTrue(j.contains("\"processing_time\":10"));
        assertTrue(j.contains("\"average_processing_time\":5"));
        assertEquals(toKey(EndpointResponse.class) + j, er.toString());
    }

    @Test
    public void testGroupConstruction() {
        Group g1 = new Group(subject(1));
        Group g2 = new Group(subject(2));
        Group g3 = new Group(subject(3));
        assertEquals(subject(1), g1.getName());
        assertEquals(subject(1), g1.getSubject());
        assertEquals(subject(2), g2.getName());
        assertEquals(subject(2), g2.getSubject());
        assertEquals(subject(3), g3.getName());
        assertEquals(subject(3), g3.getSubject());
        assertNull(g1.getNext());
        assertNull(g2.getNext());
        assertNull(g3.getNext());

        assertEquals(g1, g1.appendGroup(g2));
        assertEquals(subject(2), g1.getNext().getName());
        assertNull(g2.getNext());
        assertEquals(subject(1), g1.getName());
        assertEquals(subject(1) + DOT + subject(2), g1.getSubject());
        assertEquals(subject(2), g2.getName());
        assertEquals(subject(2), g2.getSubject());

        assertEquals(g1, g1.appendGroup(g3));
        assertEquals(subject(2), g1.getNext().getName());
        assertEquals(subject(3), g1.getNext().getNext().getName());
        assertEquals(subject(1), g1.getName());
        assertEquals(subject(1) + DOT + subject(2) + DOT + subject(3), g1.getSubject());

        g1 = new Group("foo.*");
        assertEquals("foo.*", g1.getName());

        assertThrows(IllegalArgumentException.class, () -> new Group(HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> new Group(HAS_LOW));
        assertThrows(IllegalArgumentException.class, () -> new Group(HAS_127));
        assertThrows(IllegalArgumentException.class, () -> new Group("foo.>")); // gt is last segment
        assertThrows(IllegalArgumentException.class, () -> new Group("foo.>.bar")); // gt is not last segment

        EqualsVerifier.simple().forClass(Group.class).withPrefabValues(Group.class, g1, g2).verify();
    }

    @Test
    public void testServiceEndpointConstruction() {
        Group g1 = new Group(subject(1));
        Group g2 = new Group(subject(2)).appendGroup(g1);
        Endpoint e1 = new Endpoint(name(100), subject(100));
        Endpoint e2 = new Endpoint(name(200), subject(200));
        ServiceMessageHandler smh = m -> {
        };
        Supplier<JsonValue> sds = () -> null;

        ServiceEndpoint se = ServiceEndpoint.builder()
            .endpoint(e1)
            .handler(smh)
            .statsDataSupplier(sds)
            .dispatcher(null) // just for some coverage, dispatcher is tested elsewhere
            .build();
        assertNull(se.getGroup());
        assertEquals(e1, se.getEndpoint());
        assertEquals(e1.getName(), se.getName());
        assertEquals(e1.getSubject(), se.getSubject());
        assertEquals(smh, se.getHandler());
        assertEquals(sds, se.getStatsDataSupplier());
        assertNull(se.getDispatcher());

        se = ServiceEndpoint.builder()
            .group(g1)
            .endpoint(e1)
            .handler(smh)
            .build();
        assertEquals(g1, se.getGroup());
        assertEquals(e1, se.getEndpoint());
        assertEquals(e1.getName(), se.getName());
        assertEquals(g1.getSubject() + DOT + e1.getSubject(), se.getSubject());
        assertNull(se.getStatsDataSupplier());
        assertNull(se.getDispatcher());

        se = ServiceEndpoint.builder()
            .group(g2)
            .endpoint(e1)
            .handler(smh)
            .build();
        assertEquals(g2, se.getGroup());
        assertEquals(e1, se.getEndpoint());
        assertEquals(e1.getName(), se.getName());
        assertEquals(g2.getSubject() + DOT + e1.getSubject(), se.getSubject());

        se = ServiceEndpoint.builder()
            .endpoint(e1)
            .endpoint(e2)
            .handler(smh)
            .build();
        assertEquals(e2, se.getEndpoint());
        assertEquals(e2.getName(), se.getName());
        assertEquals(e2.getSubject(), se.getSubject());

        se = ServiceEndpoint.builder()
            .endpoint(e1)
            .endpointName(e2.getName())
            .handler(smh)
            .build();
        assertEquals(e2.getName(), se.getName());
        assertEquals(e1.getSubject(), se.getSubject());

        se = ServiceEndpoint.builder()
            .endpoint(e1)
            .endpointSubject(e2.getSubject())
            .handler(smh)
            .build();
        assertEquals(e1.getName(), se.getName());
        assertEquals(e2.getSubject(), se.getSubject());

        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> ServiceEndpoint.builder().build());
        assertTrue(iae.getMessage().contains("Endpoint"));

        iae = assertThrows(IllegalArgumentException.class,
            () -> ServiceEndpoint.builder().endpoint(e1).build());
        assertTrue(iae.getMessage().contains("Handler"));
    }

    @Test
    public void testUtilToDiscoverySubject() {
        assertEquals("$SRV.PING", Service.toDiscoverySubject(SRV_PING, null, null));
        assertEquals("$SRV.PING.myservice", Service.toDiscoverySubject(SRV_PING, "myservice", null));
        assertEquals("$SRV.PING.myservice.123", Service.toDiscoverySubject(SRV_PING, "myservice", "123"));
    }

    static class TestServiceResponses extends ServiceResponse {
        static String TYPE = "io.nats.micro.v1.test_response";

        public TestServiceResponses(byte[] jsonBytes) {
            this(parseMessage(jsonBytes));
        }

        public TestServiceResponses(JsonValue jv) {
            super(TYPE, jv);
        }
    }

    @Test
    public void testServiceResponsesConstruction() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("k", "v");

        PingResponse pr1 = new PingResponse("id", "name", "0.0.0", metadata);
        PingResponse pr2 = new PingResponse(pr1.toJson().getBytes());
        validateApiInOutPingResponse(pr1);
        validateApiInOutPingResponse(pr2);
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> new TestServiceResponses(pr1.toJsonValue()));
        assertTrue(iae.getMessage().contains("Invalid type"));

        iae = assertThrows(IllegalArgumentException.class, () -> new TestServiceResponses("{[bad json".getBytes()));
        assertTrue(iae.getMessage().contains("Type cannot be null"));

        String json1 = "{\"id\":\"id\",\"name\":\"name\",\"version\":\"0.0.0\"}";
        iae = assertThrows(IllegalArgumentException.class, () -> new TestServiceResponses(json1.getBytes()));
        assertTrue(iae.getMessage().contains("Type cannot be null"));

        String json2 = "{\"name\":\"name\",\"version\":\"0.0.0\",\"type\":\"io.nats.micro.v1.test_response\"}";
        iae = assertThrows(IllegalArgumentException.class, () -> new TestServiceResponses(json2.getBytes()));
        assertTrue(iae.getMessage().contains("Id cannot be null"));

        String json3 = "{\"id\":\"id\",\"version\":\"0.0.0\",\"type\":\"io.nats.micro.v1.test_response\"}";
        iae = assertThrows(IllegalArgumentException.class, () -> new TestServiceResponses(json3.getBytes()));
        assertTrue(iae.getMessage().contains("Name cannot be null"));

        String json4 = "{\"id\":\"id\",\"name\":\"name\",\"type\":\"io.nats.micro.v1.test_response\"}";
        iae = assertThrows(IllegalArgumentException.class, () -> new TestServiceResponses(json4.getBytes()));
        assertTrue(iae.getMessage().contains("Version cannot be null"));

        InfoResponse ir1 = new InfoResponse("id", "name", "0.0.0", metadata, "desc", Arrays.asList("subject1", "subject2"));
        InfoResponse ir2 = new InfoResponse(ir1.toJson().getBytes());
        validateApiInOutInfoResponse(ir1);
        validateApiInOutInfoResponse(ir2);

        List<EndpointResponse> endpoints = new ArrayList<>();
        endpoints.add(new EndpointResponse("endName0", "endSubject0"));
        endpoints.add(new EndpointResponse("endName1", "endSubject1"));

        ZonedDateTime serviceStarted = DateTimeUtils.gmtNow();
        ZonedDateTime[] endStarteds = new ZonedDateTime[2];
        sleep(100);
        endStarteds[0] = DateTimeUtils.gmtNow();
        sleep(100);
        endStarteds[1] = DateTimeUtils.gmtNow();

        List<EndpointResponse> statsList = new ArrayList<>();
        JsonValue[] data = new JsonValue[]{supplyData(), supplyData()};
        statsList.add(new EndpointResponse("endName0", "endSubject0", 1000, 0, 10000, "lastError0", data[0], endStarteds[0]));
        statsList.add(new EndpointResponse("endName1", "endSubject1", 2000, 10, 10000, "lastError1", data[1], endStarteds[1]));

        StatsResponse stat1 = new StatsResponse(pr1, serviceStarted, statsList);
        StatsResponse stat2 = new StatsResponse(stat1.toJson().getBytes());
        validateApiInOutStatsResponse(stat1, serviceStarted, endStarteds, data);
        validateApiInOutStatsResponse(stat2, serviceStarted, endStarteds, data);

        EqualsVerifier.simple().forClass(PingResponse.class).verify();
        EqualsVerifier.simple().forClass(InfoResponse.class).verify();
        EqualsVerifier.simple().forClass(StatsResponse.class)
            .withPrefabValues(EndpointResponse.class, statsList.get(0), statsList.get(1))
            .verify();
    }

    private static void validateApiInOutStatsResponse(StatsResponse stat, ZonedDateTime serviceStarted, ZonedDateTime[] endStarteds, JsonValue[] data) {
        validateApiInOutServiceResponse(stat, StatsResponse.TYPE);
        assertEquals(serviceStarted, stat.getStarted());
        assertEquals(2, stat.getEndpointStats().size());
        for (int x = 0; x < 2; x++) {
            EndpointResponse e = stat.getEndpointStats().get(x);
            assertEquals("endName" + x, e.getName());
            assertEquals("endSubject" + x, e.getSubject());
            long nr = x * 1000 + 1000;
            long errs = x * 10;
            long avg = 10000 / nr;
            assertEquals(nr, e.getNumRequests());
            assertEquals(errs, e.getNumErrors());
            assertEquals(10000, e.getProcessingTime());
            assertEquals(avg, e.getAverageProcessingTime());
            assertEquals("lastError" + x, e.getLastError());
            assertEquals(new TestStatsData(data[x]), new TestStatsData(e.getData()));
            assertEquals(endStarteds[x], e.getStarted());
        }
    }

    private static void validateApiInOutInfoResponse(InfoResponse r) {
        validateApiInOutServiceResponse(r, InfoResponse.TYPE);
        assertEquals("desc", r.getDescription());
        assertEquals(2, r.getSubjects().size());
        assertTrue(r.getSubjects().contains("subject1"));
        assertTrue(r.getSubjects().contains("subject2"));
    }

    private static void validateApiInOutPingResponse(PingResponse r) {
        validateApiInOutServiceResponse(r, PingResponse.TYPE);
    }

    private static void validateApiInOutServiceResponse(ServiceResponse r, String type) {
        assertEquals(type, r.getType());
        assertEquals("id", r.getId());
        assertEquals("name", r.getName());
        assertEquals("0.0.0", r.getVersion());
        assertNotNull(r.getMetadata());
        assertEquals(1, r.getMetadata().size());
        assertEquals("v", r.getMetadata().get("k"));
        assertNull(r.getMetadata().get("x"));
        String j = r.toJson();
        assertTrue(j.startsWith("{"));
        assertTrue(j.contains("\"type\":\"" + type + "\""));
        assertTrue(j.contains("\"name\":\"name\""));
        assertTrue(j.contains("\"id\":\"id\""));
        assertTrue(j.contains("\"version\":\"0.0.0\""));
        assertTrue(j.contains("\"metadata\":{\"k\":\"v\"}"));
        assertEquals(toKey(r.getClass()) + j, r.toString());
    }

    private static int _dataX = -1;

    public static JsonValue supplyData() {
        _dataX++;
        return new TestStatsData("s-" + _dataX, _dataX).toJsonValue();
    }

    static class TestStatsData implements JsonSerializable {
        public String sData;
        public int iData;

        public TestStatsData(String sData, int iData) {
            this.sData = sData;
            this.iData = iData;
        }

        public TestStatsData(JsonValue jv) {
            sData = readString(jv, "sdata");
            iData = readInteger(jv, "idata", -1);
        }

        @Override
        public String toJson() {
            return JsonUtils.toKey(getClass()) + toJsonValue().toJson();
        }

        @Override
        public JsonValue toJsonValue() {
            Map<String, JsonValue> map = new HashMap<>();
            map.put("sdata", new JsonValue(sData));
            map.put("idata", new JsonValue(iData));
            return new JsonValue(map);
        }

        @Override
        public String toString() {
            return toJsonValue().toString(getClass());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestStatsData that = (TestStatsData) o;

            if (iData != that.iData) return false;
            return Objects.equals(sData, that.sData);
        }

        @Override
        public int hashCode() {
            int result = sData != null ? sData.hashCode() : 0;
            result = 31 * result + iData;
            return result;
        }
    }

    static class TestInboxSupplier implements Supplier<String> {
        boolean wasCalled = false;
        @Override
        public String get() {
            wasCalled = true;
            return "CUSTOM_INBOX";
        }
    }

    @Test
    public void testInboxSupplier() throws Exception {
        runInServer(nc -> {
            Discovery discovery = new Discovery(nc, 100, 1);
            TestInboxSupplier supplier = new TestInboxSupplier();
            discovery.setInboxSupplier(supplier);
            try {
                discovery.ping("servicename");
            }
            catch (Exception e) {
                // we know it will throw exception b/c there is no service
                // running, we just care about it make the call
            }
            assertTrue(supplier.wasCalled);
        });
    }
}