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

package io.nats.client.support;

import io.nats.client.Options;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.regex.Pattern;

import static io.nats.client.support.NatsConstants.*;

public class NatsUri {
    private static final int NO_PORT = -1;
    private static final String UNABLE_TO_PARSE = "Unable to parse URI string.";
    private static final String UNSUPPORTED_SCHEME = "Unsupported NATS URI scheme.";
    private static final String URI_E_ALLOW_TRY_PREFIXED = "Illegal character in scheme name at index";
    private static final Pattern IPV4_RE = Pattern.compile("(([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])");
    private static final Pattern IPV6_RE = Pattern.compile("((([0-9a-fA-F]){1,4})\\:){7}([0-9a-fA-F]){1,4}");

    public static NatsUri DEFAULT_NATS_URI = new NatsUri();

    private final URI uri;

    public URI getUri() {
        return uri;
    }

    public String getScheme() {
        return uri.getScheme();
    }

    public String getHost() {
        return uri.getHost();
    }

    public int getPort() {
        return uri.getPort();
    }

    public String getUserInfo() {
        return uri.getUserInfo();
    }

    public boolean isSecure() {
        return SECURE_PROTOCOLS.contains(uri.getScheme().toLowerCase());
    }

    public boolean isWebsocket() {
        return WSS_PROTOCOLS.contains(uri.getScheme().toLowerCase());
    }

    public boolean hostIsIpAddress() {
        return IPV4_RE.matcher(uri.getHost()).matches()
            || IPV6_RE.matcher(uri.getHost()).matches();
    }

    public NatsUri reHost(String newHost) throws URISyntaxException {
        return new NatsUri(uri.toString().replace(uri.getHost(), newHost));
    }

    @Override
    public String toString() {
        return uri.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (this == o) return true;
        if (o instanceof NatsUri) {
            o = ((NatsUri)o).uri;
        }
        return uri.equals(o);
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    public NatsUri() {
        try {
            uri = new URI(Options.DEFAULT_URL);
        } catch (URISyntaxException e) {
            // seriously, this better not happen!
            throw new RuntimeException(e);
        }
    }

    public NatsUri(URI uri) throws URISyntaxException {
        this(uri.toString());
    }

    public NatsUri(String url) throws URISyntaxException {
    /*
        test string --> result of new URI(String)

        [1] provide protocol and try again
        1.2.3.4:4222 --> Illegal character in scheme name at index 0: 1.2.3.4:4222

        [2] throw exception
        proto:// --> Expected authority at index

        [3] null scheme, non-empty path? provide protocol and try again
        host    --> scheme:'null', host:'null', up:'null', port:-1, path:'host'
        1.2.3.4 --> scheme:'null', host:'null', up:'null', port:-1, path:'1.2.3.4'

        [4] has scheme but null host/path, provide protocol and try again
        x:p@host         --> scheme:'x', host:'null', up:'null', port:-1, path:'null'
        x:p@1.2.3.4      --> scheme:'x', host:'null', up:'null', port:-1, path:'null'
        x:4222           --> scheme:'x', host:'null', up:'null', port:-1, path:'null'
        x:p@host:4222    --> scheme:'x', host:'null', up:'null', port:-1, path:'null'
        x:p@1.2.3.4:4222 --> scheme:'x', host:'null', up:'null', port:-1, path:'null'

        [5] has scheme, null host, non-null path, make/throw
        proto://u:p@      --> scheme:'proto', host:'null', up:'null', port:-1, path:''
        proto://:4222     --> scheme:'proto', host:'null', up:'null', port:-1, path:''
        proto://u:p@:4222 --> scheme:'proto', host:'null', up:'null', port:-1, path:''

        [6] has scheme and host just needs port
        proto://host        --> scheme:'proto', host:'host', up:'null', port:-1, path:''
        proto://u:p@host    --> scheme:'proto', host:'host', up:'u:p', port:-1, path:''
        proto://1.2.3.4     --> scheme:'proto', host:'1.2.3.4', up:'null', port:-1, path:''
        proto://u:p@1.2.3.4 --> scheme:'proto', host:'1.2.3.4', up:'u:p', port:-1, path:''

        [7] has scheme, host and port
        proto://host:4222        --> scheme:'proto', host:'host', up:'null', port:4222, path:''
        proto://u:p@host:4222    --> scheme:'proto', host:'host', up:'u:p', port:4222, path:''
        proto://1.2.3.4:4222     --> scheme:'proto', host:'1.2.3.4', up:'null', port:4222, path:''
        proto://u:p@1.2.3.4:4222 --> scheme:'proto', host:'1.2.3.4', up:'u:p', port:4222, path:''
     */

        Helper h = parse(url, true);
        String scheme = h.uri.getScheme();
        String path = h.uri.getPath();
        if (scheme == null) {
            if (path != null) {
                // [3]
                h = tryPrefixed(h);
                scheme = h.uri.getScheme();
                path = h.uri.getPath();
            }
            else {
                // [X] not in the examples so don't know what to do, we are done
                throw new URISyntaxException(url, UNABLE_TO_PARSE);
            }
        }

        String host = h.uri.getHost();
        if (host == null) {
            if (path == null) {
                // [4]
                h = tryPrefixed(h);
                scheme = h.uri.getScheme();
                host = h.uri.getHost();
            }
            else {
                // [5]
                throw new URISyntaxException(url, UNABLE_TO_PARSE);
            }
        }

        if (host == null) {
            // if these aren't here by now, nothing we can do
            throw new URISyntaxException(url, UNABLE_TO_PARSE);
        }

        String lower = scheme.toLowerCase();
        if (!KNOWN_PROTOCOLS.contains(lower)) {
            throw new URISyntaxException(url, UNSUPPORTED_SCHEME);
        }
        if (!lower.equals(scheme)) {
            h.url = h.url.replace(scheme, lower);
        }

        if (h.uri.getPort() == NO_PORT) {
            // [6]
            uri = new URI(h.url + ":" + DEFAULT_PORT);
        }
        else {
            uri = new URI(h.url);
        }
    }

    static class Helper {
        String url;
        URI uri;
    }

    private Helper tryPrefixed(Helper helper) throws URISyntaxException {
        return parse(NATS_PROTOCOL_SLASH_SLASH + helper.url, false);
    }

    private Helper parse(String inUrl, boolean allowTryPrefixed) throws URISyntaxException {
        Helper helper = new Helper();
        try {
            helper.url = inUrl.trim();
            helper.uri = new URI(helper.url);
            return helper;
        }
        catch (URISyntaxException e) {
            if (allowTryPrefixed && e.getMessage().contains(URI_E_ALLOW_TRY_PREFIXED)) {
                // [4]
                return tryPrefixed(helper);
            }
            else {
                // [5]
                throw e;
            }
        }
    }

    public static String join(String delimiter, List<NatsUri> uris) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < uris.size(); i++) {
            if (i > 0) {
                sb.append(delimiter);
            }
            sb.append(uris.get(i).toString());
        }
        return sb.toString();
    }
}
