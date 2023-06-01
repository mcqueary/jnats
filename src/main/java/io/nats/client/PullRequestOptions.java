// Copyright 2022 The NATS Authors
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

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;

import java.time.Duration;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.Validator.validateGtZero;

/**
 * The PullRequestOptions class specifies the options for pull requests
 */
public class PullRequestOptions implements JsonSerializable {

    private final int batchSize;
    private final long maxBytes;
    private final boolean noWait;
    private final Duration expiresIn;
    private final Duration idleHeartbeat;

    public PullRequestOptions(Builder b) {
        this.batchSize = b.batchSize;
        this.maxBytes = b.maxBytes;
        this.noWait = b.noWait;
        this.expiresIn = b.expiresIn;
        this.idleHeartbeat = b.idleHeartbeat;
    }

    @Override
    public String toJson() {
        StringBuilder sb = JsonUtils.beginJson();
        JsonUtils.addField(sb, BATCH, batchSize);
        JsonUtils.addField(sb, MAX_BYTES, maxBytes);
        JsonUtils.addFldWhenTrue(sb, NO_WAIT, noWait);
        JsonUtils.addFieldAsNanos(sb, EXPIRES, expiresIn);
        JsonUtils.addFieldAsNanos(sb, IDLE_HEARTBEAT, idleHeartbeat);
        return JsonUtils.endJson(sb).toString();
    }

    /**
     * Get the batch size option value
     * @return the batch size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Get the max bytes size option value
     * @return the max bytes size
     */
    public long getMaxBytes() {
        return maxBytes;
    }

    /**
     * Get the no wait flag value
     * @return the flag
     */
    public boolean isNoWait() {
        return noWait;
    }

    /**
     * Get the expires in option value
     * @return the expires in duration
     */
    public Duration getExpiresIn() {
        return expiresIn;
    }

    /**
     * Get the idle heartbeat option value
     * @return the idle heartbeat duration
     */
    public Duration getIdleHeartbeat() {
        return idleHeartbeat;
    }

    /**
     * Creates a builder for the pull options, with batch size since it's always required
     * @param batchSize the size of the batch. Must be greater than 0
     * @return a pull options builder
     */
    public static Builder builder(int batchSize) {
        return new Builder().batchSize(batchSize);
    }

    /**
     * Creates a builder for the pull options, setting no wait to true and accepting batch size
     * @param batchSize the size of the batch. Must be greater than 0
     * @return a pull options builder
     */
    public static Builder noWait(int batchSize) {
        return new Builder().batchSize(batchSize).noWait();
    }

    public static class Builder {
        private int batchSize;
        private long maxBytes;
        private boolean noWait;
        private Duration expiresIn;
        private Duration idleHeartbeat;

        /**
         * Set the batch size for the pull
         * @param batchSize the size of the batch. Must be greater than 0
         * @return the builder
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * The maximum bytes for the pull
         * @param maxBytes the maximum bytes
         * @return the builder
         */
        public Builder maxBytes(long maxBytes) {
            this.maxBytes = maxBytes;
            return this;
        }

        /**
         * Set no wait to true
         * @return the builder
         */
        public Builder noWait() {
            this.noWait = true;
            return this;
        }

        /**
         * Set the no wait flag
         * @param noWait the flag
         * @return the builder
         */
        public Builder noWait(boolean noWait) {
            this.noWait = noWait;
            return this;
        }

        /**
         * Set the expires time in millis
         * @param expiresInMillis the millis
         * @return the builder
         */
        public Builder expiresIn(long expiresInMillis) {
            this.expiresIn = Duration.ofMillis(expiresInMillis);
            return this;
        }

        /**
         * Set the expires duration
         * @param expiresIn the duration
         * @return the builder
         */
        public Builder expiresIn(Duration expiresIn) {
            this.expiresIn = expiresIn;
            return this;
        }

        /**
         * Set the idle heartbeat time in millis
         * @param idleHeartbeatMillis the millis
         * @return the builder
         */
        public Builder idleHeartbeat(long idleHeartbeatMillis) {
            this.idleHeartbeat = Duration.ofMillis(idleHeartbeatMillis);
            return this;
        }

        /**
         * Set the idle heartbeat duration
         * @param idleHeartbeat the duration
         * @return the builder
         */
        public Builder idleHeartbeat(Duration idleHeartbeat) {
            this.idleHeartbeat = idleHeartbeat;
            return this;
        }

        /**
         * Build the PullRequestOptions. Validates that the batch size is greater than 0
         * @return the built PullRequestOptions
         */
        public PullRequestOptions build() {
            validateGtZero(batchSize, "Pull batch size");
            return new PullRequestOptions(this);
        }
    }
}
