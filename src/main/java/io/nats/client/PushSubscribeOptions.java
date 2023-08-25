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

package io.nats.client;

import static io.nats.client.support.Validator.emptyAsNull;

/**
 * The PushSubscribeOptions class specifies the options for subscribing with JetStream enabled servers.
 * Options are set using the {@link PushSubscribeOptions.Builder} or static helper methods.
 */
public class PushSubscribeOptions extends SubscribeOptions {
    public static final PushSubscribeOptions DEFAULT_PUSH_OPTS = PushSubscribeOptions.builder().build();

    private PushSubscribeOptions(Builder builder, String deliverSubject, String deliverGroup,
                                 long pendingMessageLimit, long pendingByteLimit) {
        super(builder, false, deliverSubject, deliverGroup, pendingMessageLimit, pendingByteLimit);
    }

    /**
     * Gets the deliver subject held in the consumer configuration.
     * @return the deliver subject
     */
    public String getDeliverSubject() {
        return consumerConfig.getDeliverSubject();
    }

    /**
     * Gets the deliver group held in the consumer configuration.
     * @return the deliver group
     */
    public String getDeliverGroup() {
        return consumerConfig.getDeliverGroup();
    }

    /**
     * Macro to create a default PushSubscribeOptions except for
     * where you must specify the stream because
     * the subject could apply to both a stream and a mirror.
     * @deprecated
     * This method resolves to {@link #stream(String)} as bind has a different meaning
     * and requires both stream and consumer name
     * @param stream the stream name
     * @return push subscribe options
     */
    @Deprecated
    public static PushSubscribeOptions bind(String stream) {
        return stream(stream);
    }

    /**
     * Macro to create a default PushSubscribeOptions except for
     * where you must specify the stream because
     * the subject could apply to both a stream and a mirror.
     * @param stream the stream name
     * @return push subscribe options
     */
    public static PushSubscribeOptions stream(String stream) {
        return new Builder().stream(stream).build();
    }

    /**
     * Create PushSubscribeOptions for binding to
     * a specific stream and consumer by name.
     * The client validates regular (non-fast)
     * binds to ensure that provided consumer configuration
     * is consistent with the server version and that
     * consumer type (push versus pull) matches the subscription type.
     * and that it matches the subscription type.
     * @param stream the stream name
     * @param name the consumer name
     * @return push subscribe options
     */
    public static PushSubscribeOptions bind(String stream, String name) {
        return new Builder().stream(stream).name(name).bind(true).build();
    }

    /**
     * Create PushSubscribeOptions where you are fast-binding to
     * a specific stream and consumer by name.
     * The client does not validate that the provided consumer configuration
     * is consistent with the server version or that
     * consumer type (push versus pull) matches the subscription type.
     * An inconsistent consumer configuration for instance can result in
     * receiving messages from unexpected subjects.
     * A consumer type mismatch will result in an error from the server.
     * @param stream the stream name
     * @param name the consumer name
     * @return push subscribe options
     */
    public static PushSubscribeOptions fastBind(String stream, String name) {
        return new Builder().stream(stream).name(name).fastBind(true).build();
    }

    /**
     * Macro to start a PushSubscribeOptions builder
     * @return push subscribe options builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * PushSubscribeOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    public static class Builder
            extends SubscribeOptions.Builder<Builder, PushSubscribeOptions> {
        private String deliverSubject;
        private String deliverGroup;
        private long pendingMessageLimit = Consumer.DEFAULT_MAX_MESSAGES;
        private long pendingByteLimit = Consumer.DEFAULT_MAX_BYTES;

        @Override
        protected Builder getThis() {
            return this;
        }

        /**
         * Set the ordered consumer flag
         * @param ordered flag indicating whether this subscription should be ordered
         * @return the builder.
         */
        public Builder ordered(boolean ordered) {
            this.ordered = ordered;
            return this;
        }

        /**
         * Setting this specifies the push model to a delivery subject.
         * Null or empty clears the field.
         * @param deliverSubject the subject to deliver on.
         * @return the builder.
         */
        public Builder deliverSubject(String deliverSubject) {
            this.deliverSubject = emptyAsNull(deliverSubject);
            return this;
        }

        /**
         * Setting this specifies deliver group. Must match queue is both are supplied.
         * Null or empty clears the field.
         * @param deliverGroup the group to queue on
         * @return the builder.
         */
        public Builder deliverGroup(String deliverGroup) {
            this.deliverGroup = emptyAsNull(deliverGroup);
            return this;
        }

        /**
         * Set the maximum number of messages that non-dispatched push subscriptions can hold
         * in the internal (pending) message queue. Defaults to 512 * 1024  (Consumer.DEFAULT_MAX_MESSAGES)
         * @param pendingMessageLimit the number of messages.
         * @return the builder
         */
        public Builder pendingMessageLimit(long pendingMessageLimit) {
            this.pendingMessageLimit = pendingMessageLimit;
            return this;
        }

        /**
         * Set the maximum number of bytes that non-dispatched push subscriptions can hold
         * in the internal (pending) message queue. Defaults to 64 * 1024 * 1024 (Consumer.DEFAULT_MAX_BYTES)
         * @param pendingByteLimit the number of bytes.
         * @return the builder
         */
        public Builder pendingByteLimit(long pendingByteLimit) {
            this.pendingByteLimit = pendingByteLimit;
            return this;
        }

        /**
         * Builds the push subscribe options.
         * @return push subscribe options
         */
        @Override
        public PushSubscribeOptions build() {
            return new PushSubscribeOptions(this, deliverSubject, deliverGroup,
                pendingMessageLimit, pendingByteLimit);
        }
    }
}

