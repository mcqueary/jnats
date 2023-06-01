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

import java.io.IOException;

class NatsFetchConsumer extends NatsMessageConsumerBase implements FetchConsumer {
    private final long maxWaitNanos;
    private long startNanos;

    public NatsFetchConsumer(NatsConsumerContext.SubscriptionMaker subscriptionMaker, FetchConsumeOptions fetchConsumeOptions) throws IOException, JetStreamApiException {
        initSub(subscriptionMaker.makeSubscription(null));
        maxWaitNanos = fetchConsumeOptions.getExpiresIn() * 1_000_000;
        PullRequestOptions pro = PullRequestOptions.builder(fetchConsumeOptions.getMaxMessages())
            .maxBytes(fetchConsumeOptions.getMaxBytes())
            .expiresIn(fetchConsumeOptions.getExpiresIn())
            .idleHeartbeat(fetchConsumeOptions.getIdleHeartbeat())
            .build();
        sub._pull(pro, false, null);
        startNanos = -1;
    }

    @Override
    public Message nextMessage() throws InterruptedException, JetStreamStatusCheckedException {
        try {
            if (startNanos == -1) {
                startNanos = System.nanoTime();
            }

            long timeLeftMillis = (maxWaitNanos - (System.nanoTime() - startNanos)) / 1_000_000;

            // if the manager thinks it has received everything in the pull, it means
            // that all the messages are already in the internal queue and there is
            // no waiting necessary
            if (timeLeftMillis < 1 | pmm.pendingMessages < 1 || (pmm.trackingBytes && pmm.pendingBytes < 1)) {
                return sub._nextUnmanagedNullOrLteZero(null); // null means don't wait
            }

            return sub.nextMessage(timeLeftMillis);
        }
        catch (JetStreamStatusException e) {
            throw new JetStreamStatusCheckedException(e);
        }
    }
}
