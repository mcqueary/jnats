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
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.support.Validator;

import java.io.IOException;

import static io.nats.client.ConsumeOptions.DEFAULT_OPTIONS;

/**
 * TODO
 */
public class NatsConsumerContext extends NatsStreamContext implements ConsumerContext {

    private final NatsJetStream js;
    private final ConsumerConfiguration userCc;
    private String consumer;

    NatsConsumerContext(NatsStreamContext streamContext, String consumerName, ConsumerConfiguration cc) throws IOException, JetStreamApiException {
        super(streamContext);
        js = new NatsJetStream(jsm.conn, jsm.jso);
        if (consumerName != null) {
            consumer = consumerName;
            userCc = null;
            jsm.getConsumerInfo(stream, consumer);
        }
        else {
            userCc = cc;
        }
    }

    private NatsConsumerContext(NatsConnection connection, JetStreamOptions jsOptions, String streamName,
                                String consumerName, ConsumerConfiguration cc) throws IOException, JetStreamApiException {
        this(new NatsStreamContext(connection, jsOptions, streamName), consumerName, cc);
    }

    NatsConsumerContext(NatsConnection connection, JetStreamOptions jsOptions, String stream, String consumerName) throws IOException, JetStreamApiException {
        this(connection, jsOptions, stream, Validator.required(consumerName, "Consumer Name"), null);
    }

    NatsConsumerContext(NatsConnection connection, JetStreamOptions jsOptions, String stream, ConsumerConfiguration consumerConfiguration) throws IOException, JetStreamApiException {
        this(connection, jsOptions, stream, null, Validator.required(consumerConfiguration, "Consumer Configuration"));
    }

    public String getName() {
        return consumer;
    }

    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        return jsm.getConsumerInfo(stream, consumer);
    }

    private ConsumeOptions orDefault(ConsumeOptions consumeOptions) {
        return consumeOptions == null ? DEFAULT_OPTIONS : consumeOptions;
    }

    private NatsJetStreamPullSubscription makeSubscription() throws IOException, JetStreamApiException {
        PullSubscribeOptions pso;
        if (consumer == null) {
            pso = PullSubscribeOptions.builder().stream(stream).configuration(userCc).build();
        }
        else {
            pso = PullSubscribeOptions.bind(stream, consumer);
        }
        return (NatsJetStreamPullSubscription)js.subscribe(null, pso);
    }

    /* inner */ class NatsFetchConsumer extends NatsMessageConsumer implements FetchConsumer {
        final long expiration;
        public NatsFetchConsumer(ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
            super(consumeOptions);
            setSub(makeSubscription());
            sub.pull(PullRequestOptions
                .builder(consumeOptions.getBatchSize())
                .expiresIn(consumeOptions.getExpiresInMillis())
                .idleHeartbeat(consumeOptions.getIdleHeartbeatMillis())
                .build()
            );
            expiration = System.currentTimeMillis() + consumeOptions.getExpiresInMillis() - 50;
        }

        @Override
        public Message nextMessage() throws InterruptedException {
            if (pmm.pendingMessages < 1 || (pmm.trackingBytes && pmm.pendingBytes < 1)) {
                return null;
            }
            long timeLeft = expiration - System.currentTimeMillis();
            if (timeLeft > 0) {
                return sub.nextMessage(timeLeft);
            }
            return null;
        }
    }

    @Override
    public FetchConsumer fetch(int count) throws IOException, JetStreamApiException {
        return fetch(ConsumeOptions.builder().batchSize(count).build());
    }

    @Override
    public FetchConsumer fetch(ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        Validator.required(consumeOptions, "Consume Options");
        PullSubscribeOptions pso = PullSubscribeOptions.bind(stream, consumer);
        NatsJetStreamPullSubscription sub = (NatsJetStreamPullSubscription)js.subscribe(null, pso);
        return new NatsFetchConsumer(orDefault(consumeOptions));
    }

    @Override
    public MessageConsumer consume() throws IOException, JetStreamApiException {
        return consume((ConsumeOptions)null);
    }

    @Override
    public MessageConsumer consume(ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        return null;
    }

    @Override
    public MessageConsumer consume(MessageHandler handler) throws IOException, JetStreamApiException {
        return null;
    }


    @Override
    public MessageConsumer consume(MessageHandler handler, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        return null;
    }
}
