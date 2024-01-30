// Copyright 2015-2018 The NATS Authors
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

import javax.net.ssl.*;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

import static io.nats.client.support.RandomUtils.SRAND;

public class SSLUtils {

    public static final String DEFAULT_TLS_ALGORITHM = "SunX509";
    public static final String DEFAULT_KEYSTORE_TYPE = "JKS";

    private static TrustManagerDelegate TRUST_MANAGER_DELEGATE;

    public static void setDefaultTrustManagerDelegate(TrustManagerDelegate trustManagerDelegate) {
        SSLUtils.TRUST_MANAGER_DELEGATE = trustManagerDelegate;
    }

    public interface TrustManagerDelegate {
        java.security.cert.X509Certificate[] getAcceptedIssuers();
        void checkClientTrusted(X509Certificate[] certs, String authType);
        void checkServerTrusted(X509Certificate[] certs, String authType);
    }

    private static final TrustManager[] DEFAULT_TRUST_MANAGERS = new TrustManager[] {
        new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return TRUST_MANAGER_DELEGATE == null ? null : TRUST_MANAGER_DELEGATE.getAcceptedIssuers();
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {
                if (TRUST_MANAGER_DELEGATE != null) {
                    TRUST_MANAGER_DELEGATE.checkClientTrusted(certs, authType);
                }
            }

            public void checkServerTrusted(X509Certificate[] certs, String authType) {
                if (TRUST_MANAGER_DELEGATE != null) {
                    TRUST_MANAGER_DELEGATE.checkServerTrusted(certs, authType);
                }
            }
        }
    };

    public static SSLContext createOpenTLSContext() {
        try {
            return createTrustAllTlsContext();
        }
        catch (Exception e) {
            return null;
        }
    }

    public static SSLContext createTrustAllTlsContext() throws GeneralSecurityException {
        SSLContext context = SSLContext.getInstance(Options.DEFAULT_SSL_PROTOCOL);
        context.init(null, DEFAULT_TRUST_MANAGERS, SRAND);
        return context;
    }

    public static KeyStore loadKeystore(String keystorePath, char[] keystorePwd) throws GeneralSecurityException, IOException {
        final KeyStore store = KeyStore.getInstance(DEFAULT_KEYSTORE_TYPE);
        try (BufferedInputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(keystorePath)))) {
            store.load(in, keystorePwd);
        }
        return store;
    }

    public static KeyManager[] createKeyManagers(String keystorePath, char[] keystorePwd) throws GeneralSecurityException, IOException {
        return createKeyManagers(keystorePath, keystorePwd, DEFAULT_TLS_ALGORITHM);
    }

    public static KeyManager[] createKeyManagers(String keystorePath, char[] keystorePwd, String tlsAlgo) throws GeneralSecurityException, IOException {
        if (keystorePath == null) {
            return null;
        }
        KeyStore store = loadKeystore(keystorePath, keystorePwd);
        KeyManagerFactory factory = KeyManagerFactory.getInstance(tlsAlgo);
        factory.init(store, keystorePwd);
        return factory.getKeyManagers();
    }

    public static TrustManager[] createTrustManagers(String truststorePath, char[] truststorePwd) throws GeneralSecurityException, IOException {
        return createTrustManagers(truststorePath, truststorePwd, DEFAULT_TLS_ALGORITHM);
    }

    public static TrustManager[] createTrustManagers(String truststorePath, char[] truststorePwd, String tlsAlgo) throws GeneralSecurityException, IOException {
        if (truststorePath == null) {
            return null;
        }
        KeyStore store = loadKeystore(truststorePath, truststorePwd);
        TrustManagerFactory factory = TrustManagerFactory.getInstance(tlsAlgo);
        factory.init(store);
        return factory.getTrustManagers();
    }

    public static SSLContext createSSLContext(String keystorePath, char[] keystorePwd, String truststorePath, char[] truststorePwd) throws GeneralSecurityException, IOException {
        return createSSLContext(keystorePath, keystorePwd, truststorePath, truststorePwd, DEFAULT_TLS_ALGORITHM);
    }
    public static SSLContext createSSLContext(String keystorePath, char[] keystorePwd, String truststorePath, char[] truststorePwd, String tlsAlgo) throws GeneralSecurityException, IOException {
        SSLContext ctx = SSLContext.getInstance(Options.DEFAULT_SSL_PROTOCOL);
        ctx.init(createKeyManagers(keystorePath, keystorePwd, tlsAlgo), createTrustManagers(truststorePath, truststorePwd, tlsAlgo), SRAND);
        return ctx;
    }
}