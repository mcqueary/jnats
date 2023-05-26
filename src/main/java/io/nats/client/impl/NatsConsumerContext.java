// Copyright 2020-2023 The NATS Authors
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
import io.nats.client.support.Validator;

import java.io.IOException;
import java.time.Duration;

import static io.nats.client.BaseConsumeOptions.*;
import static io.nats.client.ConsumeOptions.DEFAULT_CONSUME_OPTIONS;
import static io.nats.client.impl.NatsJetStreamSubscription.EXPIRE_ADJUSTMENT;

/**
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class NatsConsumerContext implements ConsumerContext {

    private final NatsStreamContext streamContext;
    private final NatsJetStream js;
    private final String consumerName;

    NatsConsumerContext(NatsStreamContext streamContext, ConsumerInfo ci) throws IOException {
        this.streamContext = streamContext;
        js = new NatsJetStream(streamContext.jsm.conn, streamContext.jsm.jso);
        this.consumerName = ci.getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getConsumerName() {
        return consumerName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        return streamContext.jsm.getConsumerInfo(streamContext.streamName, consumerName);
    }

    class SubscriptionMaker {
        Dispatcher dispatcher;

        public NatsJetStreamPullSubscription makeSubscription(MessageHandler messageHandler) throws IOException, JetStreamApiException {
            PullSubscribeOptions pso = PullSubscribeOptions.bind(streamContext.streamName, consumerName);
            if (messageHandler == null) {
                return (NatsJetStreamPullSubscription)js.subscribe(null, pso);
            }

            dispatcher = js.conn.createDispatcher();
            return (NatsJetStreamPullSubscription)js.subscribe(null, dispatcher, messageHandler, pso);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message next() throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        return next(DEFAULT_EXPIRES_IN_MS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message next(Duration maxWait) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        return next(maxWait == null ? DEFAULT_EXPIRES_IN_MS : maxWait.toMillis());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message next(long maxWaitMillis) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        if (maxWaitMillis < MIN_EXPIRES_MILLS) {
            throw new IllegalArgumentException("Max wait must be at least " + MIN_EXPIRES_MILLS + " milliseconds.");
        }

        long expires = maxWaitMillis - EXPIRE_ADJUSTMENT;

        NatsJetStreamPullSubscription sub = new SubscriptionMaker().makeSubscription(null);
        sub._pull(PullRequestOptions.builder(1).expiresIn(expires).build(), false);
        try {
            return sub.nextMessage(maxWaitMillis);
        }
        catch (JetStreamStatusException e) {
            throw new JetStreamStatusCheckedException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FetchConsumer fetchMessages(int maxMessages) throws IOException, JetStreamApiException {
        return fetch(FetchConsumeOptions.builder().maxMessages(maxMessages).build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FetchConsumer fetchBytes(int maxBytes) throws IOException, JetStreamApiException {
        return fetch(FetchConsumeOptions.builder().maxBytes(maxBytes, DEFAULT_MESSAGE_COUNT_WHEN_BYTES).build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FetchConsumer fetch(FetchConsumeOptions fetchConsumeOptions) throws IOException, JetStreamApiException {
        Validator.required(fetchConsumeOptions, "Fetch Consume Options");
        return new NatsFetchConsumer(new SubscriptionMaker(), fetchConsumeOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ManualConsumer consume() throws IOException, JetStreamApiException {
        return new NatsManualConsumer(new SubscriptionMaker(), DEFAULT_CONSUME_OPTIONS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ManualConsumer consume(ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        Validator.required(consumeOptions, "Consume Options");
        return new NatsManualConsumer(new SubscriptionMaker(), consumeOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SimpleConsumer consume(MessageHandler handler) throws IOException, JetStreamApiException {
        Validator.required(handler, "Message Handler");
        return new NatsSimpleConsumer(new SubscriptionMaker(), handler, DEFAULT_CONSUME_OPTIONS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SimpleConsumer consume(MessageHandler handler, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        Validator.required(handler, "Message Handler");
        Validator.required(consumeOptions, "Consume Options");
        return new NatsSimpleConsumer(new SubscriptionMaker(), handler, consumeOptions);
    }
}
