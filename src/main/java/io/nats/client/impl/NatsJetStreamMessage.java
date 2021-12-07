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

import io.nats.client.Connection;
import io.nats.client.impl.NatsMessage.InternalMessage;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static io.nats.client.impl.AckType.*;
import static io.nats.client.support.Validator.validateDurationRequired;

class NatsJetStreamMessage extends InternalMessage {

    private NatsJetStreamMetaData jsMetaData = null;

    NatsJetStreamMessage() {}

    /**
     * {@inheritDoc}
     */
    @Override
    public void ack() {
        ackReply(AckAck);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ackSync(Duration d) throws InterruptedException, TimeoutException {
        if (dontAdr15SkipAck()) {
            validateDurationRequired(d);
            Connection nc = getJetStreamValidatedConnection();
            if (nc.request(replyTo, AckAck.bytes, d) == null) {
                throw new TimeoutException("Ack response timed out.");
            }
            lastAck = AckAck;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void nak() {
        ackReply(AckNak);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void inProgress() {
        ackReply(AckProgress);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void term() {
        ackReply(AckTerm);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NatsJetStreamMetaData metaData() {
        if (this.jsMetaData == null) {
            this.jsMetaData = new NatsJetStreamMetaData(this);
        }
        return this.jsMetaData;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isJetStream() {
        return true; // NatsJetStreamMessage will never be created unless it's actually a JetStream Message
    }

    private void ackReply(AckType ackType) {
        if (dontAdr15SkipAck()) {
            Connection nc = getJetStreamValidatedConnection();
            nc.publish(replyTo, ackType.bytes);
            lastAck = ackType;
        }
    }

    private boolean dontAdr15SkipAck() {
        return lastAck == null || !lastAck.terminal;
    }

    Connection getJetStreamValidatedConnection() {
        if (getSubscription() == null) {
            throw new IllegalStateException("Message is not bound to a subscription.");
        }

        Connection c = getConnection();
        if (c == null) {
            throw new IllegalStateException("Message is not bound to a connection");
        }
        return c;
    }
}
