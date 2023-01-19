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

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonUtils;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonUtils.toKey;
import static io.nats.service.ServiceUtil.toDiscoverySubject;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class Service {
    private final Connection conn;
    private final Duration drainTimeout;
    private final Map<String, EndpointContext> serviceContexts;
    private final List<EndpointContext> discoveryContexts;
    private final List<Dispatcher> dInternals;
    private final PingResponse pingResponse;
    private final InfoResponse infoResponse;
    private final SchemaResponse schemaResponse;

    private final Object stopLock;
    private ZonedDateTime started;
    private CompletableFuture<Boolean> doneFuture;

    Service(ServiceBuilder b) {
        String id = new io.nats.client.NUID().next();
        conn = b.conn;
        drainTimeout = b.drainTimeout;
        dInternals = new ArrayList<>();
        stopLock = new Object();

        // setup the service contexts
        // ? do we need an internal dispatcher for any user endpoints
        // ? also while we are here, we need to collect the endpoints for the SchemaResponse
        Dispatcher dTemp = null;
        List<String> infoSubjects = new ArrayList<>();
        List<Endpoint> schemaEndpoints = new ArrayList<>();
        serviceContexts = new HashMap<>();
        for (ServiceEndpoint se : b.serviceEndpoints.values()) {
            if (se.getDispatcher() == null) {
                if (dTemp == null) {
                    dTemp = conn.createDispatcher();
                }
                serviceContexts.put(se.getName(), new EndpointContext(conn, dTemp, true, se));
            }
            else {
                serviceContexts.put(se.getName(), new EndpointContext(conn, null, true, se));
            }
            infoSubjects.add(se.getSubject());
            schemaEndpoints.add(se.getEndpoint());
        }
        if (dTemp != null) {
            dInternals.add(dTemp);
        }

        // build static responses
        pingResponse = new PingResponse(id, b.name, b.version);
        infoResponse = new InfoResponse(id, b.name, b.version, b.description, infoSubjects);
        schemaResponse = new SchemaResponse(id, b.name, b.version, b.apiUrl, schemaEndpoints);

        if (b.pingDispatcher == null || b.infoDispatcher == null || b.schemaDispatcher == null || b.statsDispatcher == null) {
            dTemp = conn.createDispatcher();
            dInternals.add(dTemp);
        }
        else {
            dTemp = null;
        }

        discoveryContexts = new ArrayList<>();
        addDiscoveryContexts(ServiceUtil.SRV_PING, pingResponse, b.pingDispatcher, dTemp);
        addDiscoveryContexts(ServiceUtil.SRV_INFO, infoResponse, b.infoDispatcher, dTemp);
        addDiscoveryContexts(ServiceUtil.SRV_SCHEMA, schemaResponse, b.schemaDispatcher, dTemp);
        addStatsContexts(b.statsDispatcher, dTemp);
    }

    private void addDiscoveryContexts(String discoveryName, ServiceResponse sr, Dispatcher dUser, Dispatcher dInternal) {
        final byte[] responseBytes = sr.serialize();
        ServiceMessageHandler handler = smsg -> smsg.reply(conn, responseBytes);
        addDiscoveryContexts(discoveryName, dUser, dInternal, handler);
    }

    private void addStatsContexts(Dispatcher dUser, Dispatcher dInternal) {
        ServiceMessageHandler handler = smsg -> smsg.reply(conn,getStatsResponse().serialize());
        addDiscoveryContexts(ServiceUtil.SRV_STATS, dUser, dInternal, handler);
    }

    private Endpoint internalEndpoint(String discoveryName, String optionalServiceNameSegment, String optionalServiceIdSegment) {
        String subject = toDiscoverySubject(discoveryName, optionalServiceNameSegment, optionalServiceIdSegment);
        return new Endpoint(subject, subject, null, false);
    }

    private void addDiscoveryContexts(String discoveryName, Dispatcher dUser, Dispatcher dInternal, ServiceMessageHandler handler) {
        Endpoint[] endpoints = new Endpoint[] {
            internalEndpoint(discoveryName, null, null),
            internalEndpoint(discoveryName, pingResponse.getName(), null),
            internalEndpoint(discoveryName, pingResponse.getName(), pingResponse.getId())
        };

        for (Endpoint endpoint : endpoints) {
            discoveryContexts.add(
                new EndpointContext(conn, dInternal, false,
                    new ServiceEndpoint(endpoint, handler, dUser)));
        }
    }

    public CompletableFuture<Boolean> startService() {
        doneFuture = new CompletableFuture<>();
        for (EndpointContext ctx : serviceContexts.values()) {
            ctx.start();
        }
        for (EndpointContext ctx : discoveryContexts) {
            ctx.start();
        }
        started = DateTimeUtils.gmtNow();
        return doneFuture;
    }

    @Override
    public String toString() {
        StringBuilder sb = JsonUtils.beginJsonPrefixed(toKey(this.getClass()));
        JsonUtils.addField(sb, ID, pingResponse.getId());
        JsonUtils.addField(sb, NAME, pingResponse.getName());
        JsonUtils.addField(sb, VERSION, infoResponse.getVersion());
        JsonUtils.addField(sb, DESCRIPTION, infoResponse.getDescription());
        JsonUtils.addField(sb, API_URL, schemaResponse.getApiUrl());
        JsonUtils.addJsons(sb, ENDPOINTS, schemaResponse.getEndpoints());
        return endJson(sb).toString();
    }

    public void stop() {
        stop(true, null);
    }

    public void stop(Throwable t) {
        stop(true, t);
    }

    public void stop(boolean drain) {
        stop(drain, null);
    }

    public void stop(boolean drain, Throwable t) {
        synchronized (stopLock) {
            if (!doneFuture.isDone()) {
                if (drain) {
                    List<CompletableFuture<Boolean>> futures = new ArrayList<>();

                    for (Dispatcher d : dInternals) {
                        try {
                            futures.add(d.drain(drainTimeout));
                        }
                        catch (Exception e) { /* nothing I can really do, we are stopping anyway */ }
                    }

                    for (EndpointContext c : serviceContexts.values()) {
                        if (c.isNotInternalDispatcher()) {
                            try {
                                futures.add(c.getSub().drain(drainTimeout));
                            }
                            catch (Exception e) { /* nothing I can really do, we are stopping anyway */ }
                        }
                    }

                    for (EndpointContext c : discoveryContexts) {
                        if (c.isNotInternalDispatcher()) {
                            try {
                                futures.add(c.getSub().drain(drainTimeout));
                            }
                            catch (Exception e) { /* nothing I can really do, we are stopping anyway */ }
                        }
                    }

                    // make sure drain is done before closing dispatcher
                    long drainTimeoutMillis = drainTimeout.toMillis();
                    for (CompletableFuture<Boolean> f : futures) {
                        try {
                            f.get(drainTimeoutMillis, TimeUnit.MILLISECONDS);
                        }
                        catch (Exception ignore) {
                            // don't care if it completes successfully or not, just that it's done.
                        }
                    }
                }

                // close internal dispatchers
                for (Dispatcher d : dInternals) {
                    conn.closeDispatcher(d);
                }

                // ok we are done
                if (t == null) {
                    doneFuture.complete(true);
                }
                else {
                    doneFuture.completeExceptionally(t);
                }
            }
        }
    }

    public void reset() {
        started = DateTimeUtils.gmtNow();
        for (EndpointContext c : discoveryContexts) {
            c.reset();
        }
        for (EndpointContext c : serviceContexts.values()) {
            c.reset();
        }
    }

    public String getId() {
        return pingResponse.getId();
    }

    public PingResponse getPingResponse() {
        return pingResponse;
    }

    public InfoResponse getInfoResponse() {
        return infoResponse;
    }

    public SchemaResponse getSchemaResponse() {
        return schemaResponse;
    }

    public StatsResponse getStatsResponse() {
        List<EndpointStats> endpointStats = new ArrayList<>();
        for (EndpointContext c : serviceContexts.values()) {
            EndpointStats es = c.getEndpointStats();
            endpointStats.add(es);
        }
        return new StatsResponse(pingResponse, started, endpointStats);
    }

    public EndpointStats getEndpointStats(String endpointName) {
        EndpointContext c = serviceContexts.get(endpointName);
        return c == null ? null : c.getEndpointStats();
    }
}
