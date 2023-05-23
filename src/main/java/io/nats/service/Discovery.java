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

package io.nats.service;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Subscription;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.nats.service.Service.*;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class Discovery {
    public static final long DEFAULT_DISCOVERY_MAX_TIME_MILLIS = 5000;
    public static final int DEFAULT_DISCOVERY_MAX_RESULTS = 10;

    private final Connection conn;
    private final long maxTimeMillis;
    private final int maxResults;

    private Supplier<String> inboxSupplier;

    public Discovery(Connection conn) {
        this(conn, -1, -1);
    }

    public Discovery(Connection conn, long maxTimeMillis, int maxResults) {
        this.conn = conn;
        this.maxTimeMillis = maxTimeMillis < 1 ? DEFAULT_DISCOVERY_MAX_TIME_MILLIS : maxTimeMillis;
        this.maxResults = maxResults < 1 ? DEFAULT_DISCOVERY_MAX_RESULTS : maxResults;
        setInboxSupplier(null);
    }

    public void setInboxSupplier(Supplier<String> inboxSupplier) {
        this.inboxSupplier = inboxSupplier == null ? conn::createInbox : inboxSupplier;
    }

    // ----------------------------------------------------------------------------------------------------
    // ping
    // ----------------------------------------------------------------------------------------------------
    public List<PingResponse> ping() {
        return ping(null);
    }

    public List<PingResponse> ping(String serviceName) {
        List<PingResponse> list = new ArrayList<>();
        discoverMany(SRV_PING, serviceName, jsonBytes -> list.add(new PingResponse(jsonBytes)));
        return list;
    }

    public PingResponse ping(String serviceName, String serviceId) {
        byte[] jsonBytes = discoverOne(SRV_PING, serviceName, serviceId);
        return jsonBytes == null ? null : new PingResponse(jsonBytes);
    }

    // ----------------------------------------------------------------------------------------------------
    // info
    // ----------------------------------------------------------------------------------------------------
    public List<InfoResponse> info() {
        return info(null);
    }

    public List<InfoResponse> info(String serviceName) {
        List<InfoResponse> list = new ArrayList<>();
        discoverMany(SRV_INFO, serviceName, jsonBytes -> list.add(new InfoResponse(jsonBytes)));
        return list;
    }

    public InfoResponse info(String serviceName, String serviceId) {
        byte[] jsonBytes = discoverOne(SRV_INFO, serviceName, serviceId);
        return jsonBytes == null ? null : new InfoResponse(jsonBytes);
    }

    // ----------------------------------------------------------------------------------------------------
    // stats
    // ----------------------------------------------------------------------------------------------------
    public List<StatsResponse> stats() {
        return stats(null);
    }

    public List<StatsResponse> stats(String serviceName) {
        List<StatsResponse> list = new ArrayList<>();
        discoverMany(SRV_STATS, serviceName, jsonBytes -> list.add(new StatsResponse(jsonBytes)));
        return list;
    }

    public StatsResponse stats(String serviceName, String serviceId) {
        byte[] jsonBytes = discoverOne(SRV_STATS, serviceName, serviceId);
        return jsonBytes == null ? null : new StatsResponse(jsonBytes);
    }

    // ----------------------------------------------------------------------------------------------------
    // workers
    // ----------------------------------------------------------------------------------------------------
    private byte[] discoverOne(String action, String serviceName, String serviceId) {
        String subject = Service.toDiscoverySubject(action, serviceName, serviceId);
        try {
            Message m = conn.request(subject, null, Duration.ofMillis(maxTimeMillis));
            if (m != null) {
                return m.getData();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return null;
    }

    private void discoverMany(String action, String serviceName, Consumer<byte[]> dataConsumer) {
        Subscription sub = null;
        try {
            String replyTo = inboxSupplier.get();
            sub = conn.subscribe(replyTo);

            String subject = toDiscoverySubject(action, serviceName, null);
            conn.publish(subject, replyTo, null);

            int resultsLeft = maxResults;
            long start = System.currentTimeMillis();
            long timeLeft = maxTimeMillis;
            while (resultsLeft > 0 && timeLeft > 0) {
                Message msg = sub.nextMessage(timeLeft);
                if (msg == null) {
                    return;
                }
                dataConsumer.accept(msg.getData());
                resultsLeft--;
                // try again while we have time
                timeLeft = maxTimeMillis - (System.currentTimeMillis() - start);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            try {
                //noinspection DataFlowIssue
                sub.unsubscribe();
            }
            catch (Exception ignore) {}
        }
    }
}
