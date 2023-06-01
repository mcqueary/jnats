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

package io.nats.client;

import io.nats.client.api.ConsumerInfo;

import java.io.IOException;
import java.time.Duration;

/**
 * The Consumer Context provides a convenient interface around a defined JetStream Consumer
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public interface ConsumerContext {
    /**
     * Gets the consumer name that was used to create the context.
     * @return the consumer name
     */
    String getConsumerName();

    /**
     * Gets information about the consumer behind this subscription.
     * @return consumer information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException;

    /**
     * Gets information about the consumer behind this subscription.
     * This returns the last read version of Consumer Info, which could technically be out of date.
     * @return consumer information
     */
    ConsumerInfo getCachedConsumerInfo();

    /**
     * Read the next message with max wait set to {@value BaseConsumeOptions#DEFAULT_EXPIRES_IN_MILLIS} ms
     * @return the next message or null if the max wait expires
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws InterruptedException if one is thrown, in order to propagate it up
     * @throws JetStreamStatusCheckedException an exception representing a status that requires attention,
     *         such as the consumer was deleted on the server in the middle of use.
     * @throws JetStreamApiException the request had an error related to the data
     */
    Message next() throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException;

    /**
     * Read the next message with provide max wait
     * @param maxWait duration of max wait
     * @return the next message or null if the max wait expires
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws InterruptedException if one is thrown, in order to propagate it up
     * @throws JetStreamStatusCheckedException an exception representing a status that requires attention,
     *         such as the consumer was deleted on the server in the middle of use.
     * @throws JetStreamApiException the request had an error related to the data
     */
    Message next(Duration maxWait) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException;

    /**
     * Read the next message with provide max wait
     * @param maxWaitMillis the max wait value in milliseconds
     * @return the next message or null if the max wait expires
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws InterruptedException if one is thrown, in order to propagate it up
     * @throws JetStreamStatusCheckedException an exception representing a status that requires attention,
     *         such as the consumer was deleted on the server in the middle of use.
     * @throws JetStreamApiException the request had an error related to the data
     */
    Message next(long maxWaitMillis) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException;

    /**
     * Create a one use Fetch Consumer using all defaults other than the number of messages. See {@link FetchConsumer}
     * @param maxMessages the maximum number of message to consume
     * @return the FetchConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    FetchConsumer fetchMessages(int maxMessages) throws IOException, JetStreamApiException;

    /**
     * Create a one use Fetch Consumer using all defaults other than the number of bytes. See {@link FetchConsumer}
     * @param maxBytes the maximum number of bytes to consume
     * @return the FetchConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    FetchConsumer fetchBytes(int maxBytes) throws IOException, JetStreamApiException;

    /**
     * Create a one use Fetch Consumer with complete custom consume options. See {@link FetchConsumer}
     * @param fetchConsumeOptions the custom fetch consume options. See {@link FetchConsumeOptions}
     * @return the FetchConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    FetchConsumer fetch(FetchConsumeOptions fetchConsumeOptions) throws IOException, JetStreamApiException;

    /**
     * Create a long-running IterableConsumer with default ConsumeOptions. See {@link ConsumeOptions}
     * IterableConsumer require the developer call nextMessage. See {@link IterableConsumer}
     * @return the IterableConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    IterableConsumer consume() throws IOException, JetStreamApiException;

    /**
     * Create a long-running IterableConsumer with custom ConsumeOptions. See {@link IterableConsumer} and {@link ConsumeOptions}
     * IterableConsumer requires the developer call nextMessage.
     * @param consumeOptions the custom consume options
     * @return the IterableConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    IterableConsumer consume(ConsumeOptions consumeOptions) throws IOException, JetStreamApiException;

    /**
     * Create a long-running MessageConsumer with default ConsumeOptions. See {@link MessageConsumer} and  {@link ConsumeOptions}
     * @param handler the MessageHandler used for receiving messages.
     * @return the MessageConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    MessageConsumer consume(MessageHandler handler) throws IOException, JetStreamApiException;

    /**
     * Create a long-running MessageConsumer with custom ConsumeOptions. See {@link MessageConsumer} and  {@link ConsumeOptions}
     * @param handler the MessageHandler used for receiving messages.
     * @param consumeOptions the custom consume options
     * @return the MessageConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    MessageConsumer consume(MessageHandler handler, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException;
}
