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
import io.nats.client.api.*;

import java.io.IOException;
import java.util.List;

/**
 * Implementation of Stream Context
 */
class NatsStreamContext implements StreamContext {
    final String streamName;
    final NatsJetStream js;
    final NatsJetStreamManagement jsm;

    // for when this is constructed from the NatsJetStream itself
    NatsStreamContext(String streamName, NatsJetStream js, NatsConnection connection, JetStreamOptions jsOptions) throws IOException, JetStreamApiException {
        this.streamName = streamName;
        this.js = js == null ? new NatsJetStream(connection, jsOptions) : js;
        jsm = new NatsJetStreamManagement(connection, jsOptions);
        jsm.getStreamInfo(streamName); // this is just verifying that the stream exists
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getStreamName() {
        return streamName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo getStreamInfo() throws IOException, JetStreamApiException {
        return jsm.getStreamInfo(streamName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo getStreamInfo(StreamInfoOptions options) throws IOException, JetStreamApiException {
        return jsm.getStreamInfo(streamName, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PurgeResponse purge() throws IOException, JetStreamApiException {
        return jsm.purgeStream(streamName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PurgeResponse purge(PurgeOptions options) throws IOException, JetStreamApiException {
        return jsm.purgeStream(streamName, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerContext getConsumerContext(String consumerName) throws IOException, JetStreamApiException {
        return new NatsConsumerContext(this, jsm.getConsumerInfo(streamName, consumerName), null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerContext createOrUpdateConsumer(ConsumerConfiguration config) throws IOException, JetStreamApiException {
        return new NatsConsumerContext(this, jsm.addOrUpdateConsumer(streamName, config), null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OrderedConsumerContext createOrderedConsumer(OrderedConsumerConfiguration config) throws IOException, JetStreamApiException {
        return new NatsOrderedConsumerContext(this, config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteConsumer(String consumerName) throws IOException, JetStreamApiException {
        return jsm.deleteConsumer(streamName, consumerName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo(String consumerName) throws IOException, JetStreamApiException {
        return jsm.getConsumerInfo(streamName, consumerName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getConsumerNames() throws IOException, JetStreamApiException {
        return jsm.getConsumerNames(streamName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ConsumerInfo> getConsumers() throws IOException, JetStreamApiException {
        return jsm.getConsumers(streamName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageInfo getMessage(long seq) throws IOException, JetStreamApiException {
        return jsm.getMessage(streamName, seq);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageInfo getLastMessage(String subject) throws IOException, JetStreamApiException {
        return jsm.getLastMessage(streamName, subject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageInfo getFirstMessage(String subject) throws IOException, JetStreamApiException {
        return jsm.getFirstMessage(streamName, subject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageInfo getNextMessage(long seq, String subject) throws IOException, JetStreamApiException {
        return jsm.getNextMessage(streamName, seq, subject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteMessage(long seq) throws IOException, JetStreamApiException {
        return jsm.deleteMessage(streamName, seq);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteMessage(long seq, boolean erase) throws IOException, JetStreamApiException {
        return jsm.deleteMessage(streamName, seq, erase);
    }
}
