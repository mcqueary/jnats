/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.Constants.ERR_BAD_SUBSCRIPTION;
import static io.nats.client.Constants.ERR_CONNECTION_CLOSED;
import static io.nats.client.Constants.ERR_MAX_MESSAGES;
import static io.nats.client.Constants.ERR_SLOW_CONSUMER;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class SyncSubscriptionImpl extends SubscriptionImpl implements SyncSubscription {

    protected SyncSubscriptionImpl(ConnectionImpl nc, String subj, String queue, int maxMsgs,
            long maxBytes) {
        super(nc, subj, queue, maxMsgs, maxBytes);
    }

    @Override
    public Message nextMessage() throws IOException {
        Message msg = null;
        try {
            msg = nextMessage(-1);
        } catch (TimeoutException e) {
            // Can't happen
        }
        return msg;
    }

    @Override
    public Message nextMessage(long timeout) throws IOException, TimeoutException {
        return nextMessage(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message nextMessage(long timeout, TimeUnit unit) throws IOException, TimeoutException {
        Message msg = null;
        final ConnectionImpl localConn;
        final Channel<Message> localChannel;
        final long localMax;

        mu.lock();
        if (connClosed) {
            mu.unlock();
            throw new IllegalStateException(ERR_CONNECTION_CLOSED);
        }
        if (mch == null) {
            if ((this.max > 0) && (delivered.get() >= this.max)) {
                mu.unlock();
                throw new IOException(ERR_MAX_MESSAGES);
            } else if (closed) {
                mu.unlock();
                throw new IllegalStateException(ERR_BAD_SUBSCRIPTION);
            }
        }
        if (sc == true) {
            sc = false;
            mu.unlock();
            throw new IOException(ERR_SLOW_CONSUMER);
        }
        localConn = (ConnectionImpl) this.getConnection();
        localChannel = mch;
        localMax = max;
        mu.unlock();

        if (timeout >= 0) {
            try {
                logger.trace("Calling Channel.get({}, {}) for {}", timeout, unit, this.subject);
                msg = localChannel.get(timeout, unit);
            } catch (TimeoutException e) {
                throw e;
            }
        } else {
            msg = localChannel.get();
        }

        if (msg != null) {
            long d = delivered.incrementAndGet();
            // Remove subscription if we have reached max.
            if (d == localMax) {
                localConn.mu.lock();
                localConn.removeSub(this);
                localConn.mu.unlock();
            }
            if ((localMax > 0) && (d > localMax)) {
                throw new IOException(ERR_MAX_MESSAGES);
            }
        }
        return msg;

    }
}
