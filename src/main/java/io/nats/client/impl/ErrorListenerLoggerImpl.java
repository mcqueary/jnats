// Copyright 2021 The NATS Authors
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
import io.nats.client.support.Status;

import java.util.logging.Logger;

public class ErrorListenerLoggerImpl implements ErrorListener {

    private final static Logger LOGGER = Logger.getLogger(ErrorListenerLoggerImpl.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    public void errorOccurred(final Connection conn, final String error) {
        LOGGER.severe(() -> supplyMessage("errorOccurred", conn, null, null, "Error: ", error));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exceptionOccurred(final Connection conn, final Exception exp) {
        LOGGER.severe(() -> supplyMessage("exceptionOccurred", conn, null, null, "Exception: ", exp));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void slowConsumerDetected(final Connection conn, final Consumer consumer) {
        LOGGER.warning(() -> supplyMessage("slowConsumerDetected", conn, consumer, null));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void messageDiscarded(final Connection conn, final Message msg) {
        LOGGER.info(() -> supplyMessage("messageDiscarded", conn, null, null, "Message: ", msg));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void heartbeatAlarm(final Connection conn, final JetStreamSubscription sub,
                               final long lastStreamSequence, final long lastConsumerSequence) {
        LOGGER.severe(() -> supplyMessage("heartbeatAlarm", conn, null, sub, "lastStreamSequence: ", lastStreamSequence, "lastConsumerSequence: ", lastConsumerSequence));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unhandledStatus(final Connection conn, final JetStreamSubscription sub, final Status status) {
        LOGGER.warning(() -> supplyMessage("unhandledStatus", conn, null, sub, "Status:", status));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullStatusWarning(Connection conn, JetStreamSubscription sub, Status status) {
        LOGGER.warning(() -> supplyMessage("pullStatusWarning", conn, null, sub, "Status:", status));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullStatusError(Connection conn, JetStreamSubscription sub, Status status) {
        LOGGER.severe(() -> supplyMessage("pullStatusError", conn, null, sub, "Status:", status));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String id, FlowControlSource source) {
        LOGGER.info(() -> supplyMessage("flowControlProcessed", conn, null, sub, "FlowControlSource:", source));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void socketWriteTimeout(Connection conn) {
        LOGGER.severe(() -> supplyMessage("socketWriteTimeout", conn, null, null));
    }
}
