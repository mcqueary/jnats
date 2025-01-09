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

import io.nats.client.*;
import io.nats.client.api.ConsumerInfo;

import java.io.IOException;

class NatsMessageConsumer extends NatsMessageConsumerBase implements PullManagerObserver {
    protected final ConsumeOptions consumeOpts;
    protected final int thresholdMessages;
    protected final long thresholdBytes;
    protected final SimplifiedSubscriptionMaker subscriptionMaker;
    protected final Dispatcher userDispatcher;
    protected final MessageHandler userMessageHandler;

    NatsMessageConsumer(SimplifiedSubscriptionMaker subscriptionMaker,
                        ConsumerInfo cachedConsumerInfo,
                        ConsumeOptions consumeOpts,
                        Dispatcher userDispatcher,
                        final MessageHandler userMessageHandler) throws IOException, JetStreamApiException
    {
        super(cachedConsumerInfo);

        this.subscriptionMaker = subscriptionMaker;
        this.consumeOpts = consumeOpts;
        this.userDispatcher = userDispatcher;
        this.userMessageHandler = userMessageHandler;

        int bm = consumeOpts.getBatchSize();
        long bb = consumeOpts.getBatchBytes();
        int rePullMessages = Math.max(1, bm * consumeOpts.getThresholdPercent() / 100);
        long rePullBytes = bb == 0 ? 0 : Math.max(1, bb * consumeOpts.getThresholdPercent() / 100);
        thresholdMessages = bm - rePullMessages;
        thresholdBytes = bb == 0 ? Integer.MIN_VALUE : bb - rePullBytes;

        doSub();
    }

    @Override
    public void heartbeatError() {
        try {
            // just close the current sub and make another one.
            // this could go on endlessly
            lenientClose();
            doSub();
        }
        catch (JetStreamApiException | IOException e) {
            pmm.resetTracking();
            pmm.initOrResetHeartbeatTimer();
        }
    }

    void doSub() throws JetStreamApiException, IOException {
        MessageHandler mh = userMessageHandler == null ? null : msg -> {
            userMessageHandler.onMessage(msg);
            if (stopped.get() && pmm.noMorePending()) {
                finished.set(true);
            }
        };
        super.initSub(subscriptionMaker.subscribe(mh, userDispatcher, pmm, null));
        repull();
        stopped.set(false);
        finished.set(false);
    }

    @Override
    public void pendingUpdated() {
        if (!stopped.get() && (pmm.pendingMessages <= thresholdMessages || (pmm.trackingBytes && pmm.pendingBytes <= thresholdBytes)))
        {
            repull();
        }
    }

    private void repull() {
        int rePullMessages = Math.max(1, consumeOpts.getBatchSize() - pmm.pendingMessages);
        long rePullBytes = consumeOpts.getBatchBytes() == 0 ? 0 : consumeOpts.getBatchBytes() - pmm.pendingBytes;
        PullRequestOptions pro = PullRequestOptions.builder(rePullMessages)
            .maxBytes(rePullBytes)
            .expiresIn(consumeOpts.getExpiresInMillis())
            .idleHeartbeat(consumeOpts.getIdleHeartbeat())
            .group(consumeOpts.getGroup())
            .minPending(consumeOpts.getMinPending())
            .minAckPending(consumeOpts.getMinAckPending())
            .build();
        sub._pull(pro, false, this);
    }
}
