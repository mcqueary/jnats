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
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;

import java.io.IOException;

import static io.nats.client.BaseConsumeOptions.MIN_EXPIRES_MILLS;

class NatsFetchConsumer extends NatsMessageConsumerBase implements FetchConsumer, PullManagerObserver {
    private final boolean isNoWaitNoExpires;
    private final long maxWaitNanos;
    private final String pullSubject;
    private long startNanos;

    NatsFetchConsumer(SimplifiedSubscriptionMaker subscriptionMaker,
                      ConsumerInfo cachedConsumerInfo,
                      FetchConsumeOptions fetchConsumeOptions) throws IOException, JetStreamApiException
    {
        super(cachedConsumerInfo);

        boolean isNoWait = fetchConsumeOptions.isNoWait();
        long expiresInMillis = fetchConsumeOptions.getExpiresInMillis();
        isNoWaitNoExpires = isNoWait && expiresInMillis == ConsumerConfiguration.LONG_UNSET;

        long inactiveThreshold;
        if (expiresInMillis == ConsumerConfiguration.LONG_UNSET) { // can be for noWait
            maxWaitNanos = MIN_EXPIRES_MILLS * 1_000_000;
            inactiveThreshold = MIN_EXPIRES_MILLS; // no need to do the 10% longer
        }
        else {
            maxWaitNanos = expiresInMillis * 1_000_000;
            inactiveThreshold = expiresInMillis * 110 / 100; // 10% longer than the wait
        }

        PullRequestOptions pro = PullRequestOptions.builder(fetchConsumeOptions.getMaxMessages())
            .maxBytes(fetchConsumeOptions.getMaxBytes())
            .expiresIn(expiresInMillis)
            .idleHeartbeat(fetchConsumeOptions.getIdleHeartbeat())
            .noWait(isNoWait)
            .group(fetchConsumeOptions.getGroup())
            .minPending(fetchConsumeOptions.getMinPending())
            .minAckPending(fetchConsumeOptions.getMinAckPending())
            .build();
        initSub(subscriptionMaker.subscribe(null, null, null, inactiveThreshold));
        pullSubject = sub._pull(pro, false, this);
        startNanos = -1;
    }

    @Override
    public void pendingUpdated() {}

    @Override
    public void heartbeatError() {
        stopped.set(true);
        finished.set(true);
    }

    @Override
    public Message nextMessage() throws InterruptedException, JetStreamStatusCheckedException {
        try {
            if (finished.get()) {
                return null;
            }

            // if the manager thinks it has received everything in the pull, it means
            // that all the messages are already in the internal queue and there is
            // no waiting necessary
            if (pmm.noMorePending()) {
                Message m = sub._nextUnmanagedNoWait(pullSubject);
                if (m == null) {
                    // if there are no messages in the internal cache AND there are no more pending,
                    // they all have been read and we can go ahead and finish
                    finished.set(true);
                    lenientClose();
                }
                return m;
            }

            // by not starting the timer until the first call, it gives a little buffer around
            // the next message to account for latency of incoming messages
            if (startNanos == -1) {
                startNanos = System.nanoTime();
            }
            long timeLeftMillis = (maxWaitNanos - (System.nanoTime() - startNanos)) / 1_000_000;

            // if the timer has run out, don't allow waiting
            // this might happen once, but it should already be noMorePending
            if (timeLeftMillis < 1) {
                Message m = sub._nextUnmanagedNoWait(pullSubject); // null means don't wait
                if (m == null) {
                    // no message and no time left, go ahead and finish
                    finished.set(true);
                    lenientClose();
                }
                return m;
            }

            Message m = sub._nextUnmanaged(timeLeftMillis, pullSubject);
            if (m == null && isNoWaitNoExpires) {
                // no message and no wait, go ahead and finish
                finished.set(true);
                lenientClose();
            }
            return m;
        }
        catch (JetStreamStatusException e) {
            throw new JetStreamStatusCheckedException(e);
        }
        catch (IllegalStateException i) {
            // this happens if the consumer is stopped, since it is
            // drained/unsubscribed, so don't pass it on if it's expected
            return null;
        }
    }
}
