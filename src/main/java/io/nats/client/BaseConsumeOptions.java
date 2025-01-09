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

import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.support.JsonParseException;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonValue;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.JsonValueUtils.readBoolean;
import static io.nats.client.support.JsonValueUtils.readInteger;
import static io.nats.client.support.JsonValueUtils.readLong;
import static io.nats.client.support.JsonValueUtils.*;

/**
 * Base Consume Options are provided to customize the way the consume and
 * fetch operate. It is the base class for ConsumeOptions and FetchConsumeOptions.
 */
public class BaseConsumeOptions implements JsonSerializable {
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
    protected final boolean noWait;
    protected final String group;
    protected final long minPending;
    protected final long minAckPending;

    protected BaseConsumeOptions(Builder<?, ?> b) {
        bytes = b.bytes;
        if (bytes > 0) {
            messages = b.messages < 0 ? DEFAULT_MESSAGE_COUNT_WHEN_BYTES : b.messages;
        }
        else {
            messages = b.messages < 0 ? DEFAULT_MESSAGE_COUNT : b.messages;
        }

        this.group = b.group;
        this.minPending = b.minPending;
        this.minAckPending = b.minAckPending;

        // validation handled in builder
        thresholdPercent = b.thresholdPercent;
        noWait = b.noWait;

        // if it's not noWait, it must have an expiresIn
        // we can't check this in the builder because we can't guarantee order
        // so we always default to LONG_UNSET in the builder and check it here.
        if (b.expiresIn == ConsumerConfiguration.LONG_UNSET && !noWait) {
            expiresIn = DEFAULT_EXPIRES_IN_MILLIS;
        }
        else {
            expiresIn = b.expiresIn;
        }

        // calculated
        idleHeartbeat = Math.min(MAX_HEARTBEAT_MILLIS, expiresIn * MAX_IDLE_HEARTBEAT_PERCENT / 100);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, MESSAGES, messages);
        addField(sb, BYTES, bytes);
        addField(sb, EXPIRES_IN, expiresIn);
        addField(sb, IDLE_HEARTBEAT, idleHeartbeat);
        addField(sb, THRESHOLD_PERCENT, thresholdPercent);
        addFldWhenTrue(sb, NO_WAIT, noWait);
        addField(sb, GROUP, group);
        addField(sb, MIN_PENDING, minPending);
        addField(sb, MIN_ACK_PENDING, minAckPending);
        return endJson(sb).toString();
    }

    public long getExpiresInMillis() {
        return expiresIn;
    }

    public long getIdleHeartbeat() {
        return idleHeartbeat;
    }

    public int getThresholdPercent() {
        return thresholdPercent;
    }

    public boolean isNoWait() {
        return noWait;
    }

    public String getGroup() {
        return group;
    }

    public long getMinPending() {
        return minPending;
    }

    public long getMinAckPending() {
        return minAckPending;
    }

    protected static abstract class Builder<B, CO> {
        protected int messages = -1;
        protected long bytes = 0;
        protected int thresholdPercent = DEFAULT_THRESHOLD_PERCENT;
        protected long expiresIn = DEFAULT_EXPIRES_IN_MILLIS;
        protected boolean noWait = false;
        protected String group;
        protected long minPending = -1;
        protected long minAckPending = -1;

        protected abstract B getThis();

        protected B noWait() {
            return getThis();
        }

        /**
         * Initialize values from the json string.
         * @param json the json string to parse
         * @return the builder
         * @throws JsonParseException if the json is invalid
         */
        public B json(String json) throws JsonParseException {
            return jsonValue(JsonParser.parse(json));
        }

        /**
         * Initialize values from the JsonValue object.
         * @param jsonValue the json value object
         * @return the builder
         */
        public B jsonValue(JsonValue jsonValue) {
            messages(readInteger(jsonValue, MESSAGES, -1));
            bytes(readLong(jsonValue, BYTES, -1));
            expiresIn(readLong(jsonValue, EXPIRES_IN, MIN_EXPIRES_MILLS));
            thresholdPercent(readInteger(jsonValue, THRESHOLD_PERCENT, -1));
            if (readBoolean(jsonValue, NO_WAIT, false)) {
                noWait();
            }
            group(readStringEmptyAsNull(jsonValue, GROUP));
            minPending(readLong(jsonValue, MIN_PENDING, -1));
            minAckPending(readLong(jsonValue, MIN_ACK_PENDING, -1));
            return getThis();
        }

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
            if (expiresInMillis < 1) { // this is way to clear or reset, just a code guard really
                expiresIn = ConsumerConfiguration.LONG_UNSET;
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
         * Sets the group
         * @param group the priority group for this pull
         * @return Builder
         */
        public B group(String group) {
            this.group = group;
            return getThis();
        }

        /**
         * When specified, the consumer will only receive messages when the consumer has at least this many pending messages.
         * @param minPending the min pending
         * @return the builder
         */
        public B minPending(long minPending) {
            this.minPending = minPending < 1 ? -1 : minPending;
            return getThis();
        }

        /**
         * When specified, the consumer will only receive messages when the consumer has at least this many ack pending messages.
         * @param minAckPending the min ack pending
         * @return the builder
         */
        public B minAckPending(long minAckPending) {
            this.minAckPending = minAckPending < 1 ? -1 : minAckPending;
            return getThis();
        }

        /**
         * Build the options.
         * @return the built options
         */
        public abstract CO build();
    }
}
