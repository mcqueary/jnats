// Copyright 2020-2023 The NATS Authors
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

import io.nats.client.JetStreamApiException;
import io.nats.client.PullRequestOptions;
import io.nats.client.SimpleConsumer;
import io.nats.client.api.ConsumerInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class NatsSimpleConsumerBase implements SimpleConsumer {
    protected NatsJetStreamPullSubscription sub;
    protected PullMessageManager pmm;
    protected final Object subLock;
    protected CompletableFuture<Boolean> drainFuture;
    protected Duration drainTimeout;

    NatsSimpleConsumerBase() {
        subLock = new Object();
    }

    // Synchronized by caller if necessary
    protected void initSub(NatsJetStreamPullSubscription sub) {
        this.sub = sub;
        pmm = (PullMessageManager)sub.manager;
    }

    protected void nscBasePull(PullRequestOptions pro) {
        sub._pull(pro, false);
        drainTimeout = pro.getExpiresIn().plusSeconds(1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        synchronized (subLock) {
            return sub.getConsumerInfo();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Boolean> stop() throws InterruptedException {
        synchronized (subLock) {
            if (drainFuture == null) {
                if (sub.getNatsDispatcher() != null) {
                    drainFuture = sub.getDispatcher().drain(drainTimeout);
                }
                else {
                    drainFuture = sub.drain(drainTimeout);
                }
            }
            return drainFuture;
        }
    }
}
