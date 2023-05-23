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
import io.nats.client.SimpleConsumer;
import io.nats.client.api.ConsumerInfo;

import java.io.IOException;
import java.time.Duration;

class NatsSimpleConsumerBase implements SimpleConsumer {
    protected NatsJetStreamPullSubscription sub;
    protected PullMessageManager pmm;
    protected final Object subLock;
    protected boolean active;

    NatsSimpleConsumerBase() {
        subLock = new Object();
    }

    // Synchronized by caller if necessary
    protected void initSub(NatsJetStreamPullSubscription sub) {
        this.sub = sub;
        pmm = (PullMessageManager)sub.manager;
        active = true;
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
    public boolean isActive() {
        return active;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasPending() {
        return pmm.pendingMessages > 0 || (pmm.trackingBytes && pmm.pendingBytes > 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() throws InterruptedException {
        synchronized (subLock) {
            active = false;
            // drain here but not really worried about whether it finishes properly
            // so intentionally not waiting using the future that drain(...) gives
            if (sub.getNatsDispatcher() != null) {
                sub.getDispatcher().drain(Duration.ofMillis(30000));
            }
            else {
                sub.drain(Duration.ofMillis(30000));
            }
        }
    }

    protected void stopInternal() {
        try {
            stop();
        }
        catch (InterruptedException ignore) {
            // exception on exception nothing really to do
        }
    }
}
