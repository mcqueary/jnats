// Copyright 2020 The NATS Authors
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
import io.nats.client.impl.MessageManager.ManageResult;
import io.nats.client.support.NatsJetStreamConstants;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;

/**
 * This is a JetStream specific subscription.
 */
public class NatsJetStreamSubscription extends NatsSubscription implements JetStreamSubscription, NatsJetStreamConstants {

    public static final String SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL = "Subscription type does not support pull.";

    protected final NatsJetStream js;

    protected String stream;
    protected String consumerName;

    protected MessageManager manager;

    NatsJetStreamSubscription(String sid, String subject, String queueName,
                              NatsConnection connection, NatsDispatcher dispatcher,
                              NatsJetStream js,
                              String stream, String consumer,
                              MessageManager manager)
    {
        super(sid, subject, queueName, connection, dispatcher);

        this.js = js;
        this.stream = stream;
        this.consumerName = consumer; // might be null, someone will call setConsumerName

        this.manager = manager;
        manager.startup(this);
    }

    void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    @Override
    public String getConsumerName() {
        return consumerName;
    }

    String getStream() {
        return stream;
    }

    boolean isPullMode() {
        return false;
    }

    MessageManager getManager() { return manager; } // internal, for testing

    @Override
    void invalidate() {
        manager.shutdown();
        super.invalidate();
    }

    @Override
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
        if (timeout == null || timeout.toMillis() <= 0) {
            return _nextUnmanagedNullOrLteZero(timeout);
        }

        return _nextUnmanaged(timeout.toMillis(), null);
    }

    @Override
    public Message nextMessage(long timeoutMillis) throws InterruptedException, IllegalStateException {
        if (timeoutMillis <= 0) {
            return _nextUnmanagedNullOrLteZero(Duration.ZERO);
        }

        return _nextUnmanaged(timeoutMillis, null);
    }

    protected Message _nextUnmanagedNullOrLteZero(Duration timeout) throws InterruptedException {
        // timeout null means don't wait at all, timeout <= 0 means wait forever
        // until we get an actual no (null) message, or we get a message
        // that the managers do not handle or are pull terminal indicator status message
        while (true) {
            Message msg = nextMessageInternal(timeout);
            if (msg == null) {
                return null; // no message currently queued
            }
            if (manager.manage(msg) == ManageResult.MESSAGE) {
                return msg;
            }
        }
    }

    public static final long EXPIRE_ADJUSTMENT = 10;
    public static final long MIN_EXPIRE_MILLIS = 20;

    protected Message _nextUnmanaged(long timeout, String expectedPullId) throws InterruptedException {
        // timeout > 0 process as many messages we can in that time period
        // If we get a message that either manager handles, we try again, but
        // with a shorter timeout based on what we already used up
        long start = System.nanoTime();
        long timeoutNanos = timeout * 1_000_000;
        long timeLeftNanos = timeoutNanos;
        while (timeLeftNanos > 0) {
            Message msg = nextMessageInternal( Duration.ofNanos(timeLeftNanos) );
            if (msg == null) {
                return null; // normal timeout
            }
            switch (manager.manage(msg)) {
                case MESSAGE:
                    return msg;
                case TERMINUS:
                case ERROR:
                    // reply match will be null on pushes and all status are "managed" so ignored in this loop
                    // otherwise (pull) if there is a match, the status applies
                    if (expectedPullId != null && expectedPullId.equals(msg.getSubject())) {
                        return null;
                    }
            }
            // case push managed / pull not match / other ManageResult (i.e. STATUS), try again while we have time
            timeLeftNanos = timeoutNanos - (System.nanoTime() - start);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pull(int batchSize) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pull(PullRequestOptions pullRequestOptions) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize, Duration expiresIn) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize, long expiresInMillis) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, Duration expiresIn) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, long expiresInMillis) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, long maxWaitMillis) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, Duration maxWait) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(int batchSize, Duration maxWait) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(final int batchSize, long maxWaitMillis) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamReader reader(int batchSize, int repullAt) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        return js.lookupConsumerInfo(stream, consumerName);
    }

    @Override
    public String toString() {
        return "NatsJetStreamSubscription{" +
                "consumer='" + consumerName + '\'' +
                ", stream='" + stream + '\'' +
                ", deliver='" + getSubject() + '\'' +
                ", isPullMode=" + isPullMode() +
                '}';
    }
}
