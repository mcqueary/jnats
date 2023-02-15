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

package io.nats.client.impl;

import io.nats.client.Options;
import io.nats.client.support.NatsUri;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ServerPoolTests extends TestBase {

    public static final String NATS_ONE = "nats://one";
    public static final String NATS_TWO = "nats://two";
    public static final String NATS_THREE = "nats://three";
    public static final String NATS_FOUR = "nats://four";
    public static final String HOST_THAT_CAN_BE_RESOLVED = "connect.ngs.global";
    public static final String[] bootstrap = new String[]{NATS_ONE, NATS_TWO};
    public static final String[] combined = new String[]{NATS_ONE, NATS_TWO, NATS_THREE, NATS_FOUR};
    public static final List<String> discoveredServers = Arrays.asList(NATS_TWO, NATS_THREE, NATS_FOUR);

    @Test
    public void testPoolOptions() throws URISyntaxException {
        NatsUri lastConnectedServer = new NatsUri(NATS_ONE);

        // testing that the expected show up in the pool
        Options o = new Options.Builder().servers(bootstrap).build();
        NatsServerPool nsp = newNatsServerPool(o, null, discoveredServers);
        validateNslp(nsp, null, false, combined);

        // ... and last connected is moved to the end
        nsp = newNatsServerPool(o, lastConnectedServer, discoveredServers);
        validateNslp(nsp, lastConnectedServer, false, combined);

        // testing that noRandomize maintains order
        o = new Options.Builder().noRandomize().servers(bootstrap).build();
        nsp = newNatsServerPool(o, null, discoveredServers);
        validateNslp(nsp, null, true, combined);

        // ... and still properly moves last connected server to end of list
        nsp = newNatsServerPool(o, lastConnectedServer, discoveredServers);
        validateNslp(nsp, lastConnectedServer, true, combined);

        // testing that ignoreDiscoveredServers ignores discovered servers
        o = new Options.Builder().ignoreDiscoveredServers().servers(bootstrap).build();
        nsp = newNatsServerPool(o, null, discoveredServers);
        validateNslp(nsp, null, false, NATS_ONE, NATS_TWO);
    }

    @Test
    public void testMaxReconnects() throws URISyntaxException {
        NatsUri failed = new NatsUri(NATS_ONE);

        // testing that servers that fail max times and is removed
        Options o = new Options.Builder().server(NATS_ONE).maxReconnects(3).build();
        NatsServerPool nsp = newNatsServerPool(o, null, null);
        for (int x = 0; x < 4; x++) {
            nsp.nextServer();
            validateNslp(nsp, null, false, NATS_ONE);
            nsp.connectFailed(failed);
        }
        assertNull(nsp.nextServer());

        // and that it's put back
        nsp.acceptDiscoveredUrls(Collections.singletonList(NATS_ONE));
        validateNslp(nsp, null, false, NATS_ONE);

        // testing that servers that fail max times and is removed
        o = new Options.Builder().server(NATS_ONE).maxReconnects(0).build();
        nsp = newNatsServerPool(o, null, null);
        nsp.nextServer();
        validateNslp(nsp, null, false, NATS_ONE);
        nsp.connectFailed(failed);
        assertNull(nsp.nextServer());

        // and that it's put back
        nsp.acceptDiscoveredUrls(Collections.singletonList(NATS_ONE));
        validateNslp(nsp, null, false, NATS_ONE);
    }

    @Test
    public void testPruning() throws URISyntaxException {
        // making sure that pruning happens
        Options o = new Options.Builder().servers(bootstrap).maxReconnects(0).build();
        NatsServerPool nsp = newNatsServerPool(o, null, discoveredServers);
        validateNslp(nsp, null, false, combined);
        nsp.acceptDiscoveredUrls(Collections.singletonList(NATS_FOUR));
        validateNslp(nsp, null, false, NATS_ONE, NATS_TWO, NATS_FOUR);
    }

    @Test
    public void testResolvingHostname() throws URISyntaxException {
        // resolving host name is false
        NatsUri ngs = new NatsUri(HOST_THAT_CAN_BE_RESOLVED);
        Options o = new Options.Builder().build();
        NatsServerPool nsp = newNatsServerPool(o, null, null);
        List<String> resolved = nsp.resolveHostToIps(HOST_THAT_CAN_BE_RESOLVED);
        assertNull(resolved);

        // resolving host name is true
        o = new Options.Builder().resolveHostnames().build();
        nsp = newNatsServerPool(o, null, null);
        resolved = nsp.resolveHostToIps(HOST_THAT_CAN_BE_RESOLVED);
        assertNotNull(resolved);
        assertTrue(resolved.size() > 1);
        for (String ip : resolved) {
            NatsUri nuri = ngs.reHost(ip);
            assertTrue(nuri.hostIsIpAddress());
        }
    }

    private static NatsServerPool newNatsServerPool(Options o, NatsUri last, List<String> discoveredServers) {
        NatsServerPool nsp = new NatsServerPool();
        nsp.initialize(o);
        if (last != null) {
            NatsUri next = nsp.nextServer();
            while (!next.equals(last)) {
                next = nsp.nextServer();
            }
            assertEquals(last, next);
            nsp.connectSucceeded(last);
        }
        if (discoveredServers != null) {
            nsp.acceptDiscoveredUrls(discoveredServers);
        }
        return nsp;
    }

    private static List<NatsUri> convertToNuri(Collection<String> urls) {
        final List<NatsUri> nuris = new ArrayList<>();
        for (String s : urls) {
            try {
                nuris.add(new NatsUri(s));
            } catch (URISyntaxException ignore) {
            }
        }
        return nuris;
    }

    private static void validateNslp(NatsServerPool nsp, NatsUri last, boolean notRandom, String... expectedUrls) throws URISyntaxException {
        List<NatsUri> supplied = convertToNuri(nsp.getServerList());
        int expectedSize = expectedUrls.length;
        assertEquals(expectedSize, supplied.size());
        for (int i = 0; i < expectedUrls.length; i++) {
            NatsUri expected = new NatsUri(expectedUrls[i]);
            assertTrue(supplied.contains(expected));
            if (notRandom && last == null) {
                assertEquals(expected, supplied.get(i));
            }
        }
        if (last != null) {
            assertEquals(last, supplied.get(supplied.size() - 1));
        }
    }
}
