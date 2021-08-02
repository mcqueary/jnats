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

import io.nats.client.api.ConsumerConfiguration;

import static io.nats.client.support.Validator.*;

/**
 * The PushSubscribeOptions class specifies the options for subscribing with JetStream enabled servers.
 * Options are created using the constructors or a {@link Builder}.
 */
public abstract class SubscribeOptions {

    protected final String stream;
    protected final boolean directBind;
    protected final ConsumerConfiguration consumerConfig;

    protected SubscribeOptions(String stream, String durable, String deliverSubject,
                               boolean directBind, boolean pull, ConsumerConfiguration cc) {

        this.stream = validateStreamName(stream, directBind);

        durable = durable == null && cc != null ? cc.getDurable() : durable;
        durable = validateDurable(durable, pull || directBind);

        this.consumerConfig = ConsumerConfiguration.builder(cc)
                .durable(emptyAsNull(durable))
                .deliverSubject(emptyAsNull(deliverSubject))
                .build();

        this.directBind = directBind;
    }

    /**
     * Gets the name of the stream.
     * @return the name of the stream.
     */
    public String getStream() {
        return stream;
    }

    /**
     * Gets the durable consumer name held in the consumer configuration.
     * @return the durable consumer name
     */
    public String getDurable() {
        return consumerConfig.getDurable();
    }

    /**
     * Gets whether this subscription is expected to be direct
     * @return the direct flag
     */
    public boolean isDirectBind() {
        return directBind;
    }

    /**
     * Gets the consumer configuration.
     * @return the consumer configuration.
     */
    public ConsumerConfiguration getConsumerConfiguration() {
        return consumerConfig;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "stream='" + stream + '\'' +
                "directBind=" + directBind +
                ", " + consumerConfig +
                '}';
    }

    /**
     * PushSubscribeOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    protected static abstract class Builder<B, SO> {
        protected String stream;
        protected boolean directBind;
        protected String durable;
        protected ConsumerConfiguration consumerConfig;

        protected abstract B getThis();

        /**
         * Specify the stream to attach to. If not supplied the stream will be looked up by subject.
         * Null or empty clears the field.
         * @param stream the name of the stream
         * @return the builder
         */
        public B stream(String stream) {
            this.stream = stream;
            return getThis();
        }

        /**
         * Specify the to attach in direct mode
         * @return the builder
         */
        public B directBind() {
            this.directBind = true;
            return getThis();
        }

        /**
         * Sets the durable consumer name for the subscriber.
         * Null or empty clears the field.
         * @param durable the durable name
         * @return the builder
         */
        public B durable(String durable) {
            this.durable = durable;
            return getThis();
        }

        /**
         * The consumer configuration. The configuration durable name will be replaced
         * if you supply a consumer name in the builder. The configuration deliver subject
         * will be replaced if you supply a name in the builder.
         * @param configuration the consumer configuration.
         * @return the builder
         */
        public B configuration(ConsumerConfiguration configuration) {
            this.consumerConfig = configuration;
            return getThis();
        }

        /**
         * Builds the subscribe options.
         * @return subscribe options
         */
        public abstract SO build();
    }
}
