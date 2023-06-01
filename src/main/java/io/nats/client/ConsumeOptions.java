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
 * Consume Options are provided to customize the consume operation.
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class ConsumeOptions extends BaseConsumeOptions {
    public static ConsumeOptions DEFAULT_CONSUME_OPTIONS = ConsumeOptions.builder().build();

    private ConsumeOptions(Builder b) {
        super(b);
    }

    /**
     * The initial batch size in messages.
     * @return the initial batch size in messages
     */
    public int getBatchSize() {
        return messages;
    }
    /**
     * The initial batch size in bytes.
     * @return the initial batch size in bytes
     */
    public long getBatchBytes() {
        return bytes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder
        extends BaseConsumeOptions.Builder<Builder, ConsumeOptions> {

        protected Builder getThis() { return this; }

        /**
         * Set the initial batch size in messages and remove any previously set {@link #batchBytes(long)} constraint.
         * <p>Less than 1 means default of {@value BaseConsumeOptions#DEFAULT_MESSAGE_COUNT} when bytes are not specified.
         * When bytes are specified, the batch messages size is set to prioritize the batch byte amount.</p>
         * @param batchSize the batch size in messages.
         * @return the builder
         */
        public Builder batchSize(int batchSize) {
            messages(batchSize);
            return bytes(-1);
        }

        /**
         * Set the initial batch size in bytes and remove any previously set batch message constraint.
         * Less than 1 removes any previously set batch byte constraint.
         * <p>When setting bytes to non-zero, the batch messages size is set to prioritize the batch byte size.</p>
         * <p>Also, it is important to set the byte size greater than your largest message payload, plus some amount
         * to account for overhead, otherwise the consume process will stall if there are no messages that fit the criteria.</p>
         * @see Message#consumeByteCount()
         * @param batchBytes the batch size in bytes.
         * @return the builder
         */
        public Builder batchBytes(long batchBytes) {
            messages(-1);
            return bytes(batchBytes);
        }

        /**
         * Build the ConsumeOptions.
         * @return a ConsumeOptions instance
         */
        public ConsumeOptions build() {
            return new ConsumeOptions(this);
        }
    }
}
