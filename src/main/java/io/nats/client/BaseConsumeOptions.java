// Copyright 2023 The NATS Authors
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

package io.nats.client;

/**
 * Base Consume Options are provided to customize the way the consume and
 * fetch operate. It is the base class for ConsumeOptions and FetchConsumeOptions.
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class BaseConsumeOptions {
    public static final int DEFAULT_MESSAGE_COUNT = 500;
    public static final int DEFAULT_MESSAGE_COUNT_WHEN_BYTES = 1_000_000;
    public static final int DEFAULT_THRESHOLD_PERCENT = 25;
    public static final long DEFAULT_EXPIRES_IN_MILLIS = 30000;
    public static final long MIN_EXPIRES_MILLS = 1000;
    public static final long MAX_HEARTBEAT_MILLIS = 30000;
    public static final int MAX_IDLE_HEARTBEAT_PERCENT = 50;

    protected final int messages;
    protected final long bytes;
    protected final long expiresIn;
    protected final long idleHeartbeat;
    protected final int thresholdPercent;

    @SuppressWarnings("rawtypes") // Don't need the type of the builder to get its vars
    protected BaseConsumeOptions(Builder b) {
        bytes = b.bytes;
        if (bytes > 0) {
            messages = b.messages == -1 ? DEFAULT_MESSAGE_COUNT_WHEN_BYTES : b.messages;
        }
        else {
            messages = b.messages == -1 ? DEFAULT_MESSAGE_COUNT : b.messages;
        }

        // validation handled in builder
        thresholdPercent = b.thresholdPercent;
        expiresIn = b.expiresIn;

        // calculated
        idleHeartbeat = Math.min(MAX_HEARTBEAT_MILLIS, expiresIn * MAX_IDLE_HEARTBEAT_PERCENT / 100);
    }

    public long getExpiresIn() {
        return expiresIn;
    }

    public long getIdleHeartbeat() {
        return idleHeartbeat;
    }

    public int getThresholdPercent() {
        return thresholdPercent;
    }

    protected static abstract class Builder<B, CO> {
        protected int messages = -1;
        protected long bytes = 0;
        protected int thresholdPercent = DEFAULT_THRESHOLD_PERCENT;
        protected long expiresIn = DEFAULT_EXPIRES_IN_MILLIS;

        protected abstract B getThis();

        protected B messages(int messages) {
            this.messages = messages < 1 ? -1 : messages;
            return getThis();
        }

        protected B bytes(long bytes) {
            this.bytes = bytes < 1 ? 0 : bytes;
            return getThis();
        }

        /**
         * In Fetch, sets the maximum amount of time to wait to reach the batch size or max byte.
         * In Consume, sets the maximum amount of time for an individual pull to be open
         * before issuing a replacement pull.
         * <p>Zero or less will default to {@value BaseConsumeOptions#DEFAULT_EXPIRES_IN_MILLIS},
         * otherwise, cannot be less than {@value BaseConsumeOptions#MIN_EXPIRES_MILLS}</p>
         * @param expiresInMillis the expiration time in milliseconds
         * @return the builder
         */
        public B expiresIn(long expiresInMillis) {
            if (expiresInMillis < 1) {
                expiresIn = DEFAULT_EXPIRES_IN_MILLIS;
            }
            else if (expiresInMillis < MIN_EXPIRES_MILLS) {
                throw new IllegalArgumentException("Expires must be greater than or equal to " + MIN_EXPIRES_MILLS);
            }
            else {
                expiresIn = expiresInMillis;
            }
            return getThis();
        }

        /**
         * Set the threshold percent of max bytes (if max bytes is specified) or messages
         * that will trigger issuing pull requests to keep messages flowing.
         * <p>Only applies to endless consumes.</p>
         * <p>For instance if the batch size is 100 and the re-pull percent is 25,
         * the first pull will be for 100, and then when 25 messages have been received
         * another 75 will be requested, keeping the number of messages in transit always at 100.</p>
         * <p>Must be between 1 and 100 inclusive.
         * Less than 1 will assume the default of {@value BaseConsumeOptions#DEFAULT_THRESHOLD_PERCENT}.
         * Greater than 100 will assume 100. </p>
         * @param thresholdPercent the threshold percent
         * @return the builder
         */
        public B thresholdPercent(int thresholdPercent) {
            this.thresholdPercent = thresholdPercent < 1 ? DEFAULT_THRESHOLD_PERCENT : Math.min(100, thresholdPercent);
            return getThis();
        }

        /**
         * Build the options.
         * @return the built options
         */
        public abstract CO build();
    }
}
