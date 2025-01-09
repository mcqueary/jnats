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

import io.nats.client.api.*;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * JetStream Management context for creation and access to streams and consumers in NATS.
 * <p> Using JetStream Management is the <b>recommended</b> way of managing Jetstream resources.
 * <p> Basic usage examples can be found in {@link JetStream JetStream}
 */
public interface JetStreamManagement {

    /**
     * Gets the account statistics for the logged in account.
     * @return account statistics
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    AccountStatistics getAccountStatistics() throws IOException, JetStreamApiException;

    /**
     * Loads or creates a stream.
     * @param config the stream configuration to use.
     * @return stream information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the configuration is missing or invalid
     */
    StreamInfo addStream(StreamConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Updates an existing stream.
     * @param config the stream configuration to use.
     * @return stream information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the configuration is missing or invalid
     */
    StreamInfo updateStream(StreamConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Deletes an existing stream.
     * @param streamName the stream name to use.
     * @return true if the delete succeeded. Usually throws a JetStreamApiException otherwise
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    boolean deleteStream(String streamName) throws IOException, JetStreamApiException;

    /**
     * Gets the info for an existing stream.
     * Does not retrieve any optional data.
     * See the overloaded version that accepts StreamInfoOptions
     * @param streamName the stream name to use.
     * @return stream information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    StreamInfo getStreamInfo(String streamName) throws IOException, JetStreamApiException;

    /**
     * Gets the info for an existing stream, and include subject or deleted details
     * as defined by StreamInfoOptions.
     * @param streamName the stream name to use.
     * @param options the stream info options. If null, request will not return any optional data.
     * @return stream information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    StreamInfo getStreamInfo(String streamName, StreamInfoOptions options) throws IOException, JetStreamApiException;

    /**
     * Purge stream messages
     * @param streamName the stream name to use.
     * @return PurgeResponse the purge response
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PurgeResponse purgeStream(String streamName) throws IOException, JetStreamApiException;

    /**
     * Purge messages for a specific subject
     * @param streamName the stream name to use.
     * @param options the purge options
     * @return PurgeResponse the purge response
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PurgeResponse purgeStream(String streamName, PurgeOptions options) throws IOException, JetStreamApiException;

    /**
     * Loads or creates a consumer.
     * @param streamName name of the stream
     * @param config the consumer configuration to use.
     * @return consumer information.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ConsumerInfo addOrUpdateConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Creates a consumer. Must not already exist.
     * @param streamName name of the stream
     * @param config the consumer configuration to use.
     * @return consumer information.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data such as the consumer already exists
     */
    ConsumerInfo createConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Updates an existing consumer. Must already exist.
     * @param streamName name of the stream
     * @param config the consumer configuration to use.
     * @return consumer information.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data such as the consumer does not already exist
     */
    ConsumerInfo updateConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Deletes a consumer.
     * @param streamName name of the stream
     * @param consumerName the name of the consumer.
     * @return true if the delete succeeded
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data, for instance the consumer does not exist.
     */
    boolean deleteConsumer(String streamName, String consumerName) throws IOException, JetStreamApiException;

    /**
     * Pauses a consumer.
     * @param streamName name of the stream
     * @param consumerName the name of the consumer.
     * @param pauseUntil consumer is paused until this time.
     * @return ConsumerPauseResponse the pause response
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data, for instance the consumer does not exist.
     */
    ConsumerPauseResponse pauseConsumer(String streamName, String consumerName, ZonedDateTime pauseUntil) throws IOException, JetStreamApiException;

    /**
     * Resumes a paused consumer.
     * @param streamName name of the stream
     * @param consumerName the name of the consumer.
     * @return true if the resume succeeded
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data, for instance the consumer does not exist.
     */
    boolean resumeConsumer(String streamName, String consumerName) throws IOException, JetStreamApiException;

    /**
     * Gets the info for an existing consumer.
     * @param streamName name of the stream
     * @param consumerName the name of the consumer.
     * @return consumer information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ConsumerInfo getConsumerInfo(String streamName, String consumerName) throws IOException, JetStreamApiException;

    /**
     * Return a list of consumers by name
     * @param streamName the name of the stream.
     * @return The list of names
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<String> getConsumerNames(String streamName) throws IOException, JetStreamApiException;

    /**
     * Return a list of ConsumerInfo objects.
     * @param streamName the name of the stream.
     * @return The list of ConsumerInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<ConsumerInfo> getConsumers(String streamName) throws IOException, JetStreamApiException;

    /**
     * Get the names of all streams.
     * @return The list of names
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<String> getStreamNames() throws IOException, JetStreamApiException;

    /**
     * Get a list of stream names that have subjects matching the subject filter.
     *
     * @param subjectFilter the subject. Wildcards are allowed.
     * @return The list of stream names matching the subject filter. May be empty, will not be null.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<String> getStreamNames(String subjectFilter) throws IOException, JetStreamApiException;

    /**
     * Return a list of StreamInfo objects.
     * @return The list of StreamInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<StreamInfo> getStreams() throws IOException, JetStreamApiException;

    /**
     * Return a list of StreamInfo objects that have subjects matching the filter.
     * @param subjectFilter the filter to limit the streams by subjects. Wildcards allowed.
     * @return The list of StreamInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<StreamInfo> getStreams(String subjectFilter) throws IOException, JetStreamApiException;

    /**
     * Get MessageInfo for the message with the exact sequence in the stream.
     * @param streamName the name of the stream.
     * @param seq the sequence number of the message
     * @return The MessageInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    MessageInfo getMessage(String streamName, long seq) throws IOException, JetStreamApiException;

    /**
     * Get MessageInfo for the message matching the {@link MessageGetRequest}.
     * @param streamName the name of the stream.
     * @param messageGetRequest the {@link MessageGetRequest} to get a message
     * @return The MessageInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    MessageInfo getMessage(String streamName, MessageGetRequest messageGetRequest) throws IOException, JetStreamApiException;

    /**
     * Get MessageInfo for the last message of the subject.
     * @param streamName the name of the stream.
     * @param subject the subject to get the last message for.
     * @return The MessageInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    MessageInfo getLastMessage(String streamName, String subject) throws IOException, JetStreamApiException;

    /**
     * Get MessageInfo for the first message of the subject.
     * @param streamName the name of the stream.
     * @param subject the subject to get the first message for.
     * @return The MessageInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    MessageInfo getFirstMessage(String streamName, String subject) throws IOException, JetStreamApiException;

    /**
     * Get MessageInfo for the first message created at or after the start time.
     * <p>
     * This API 1) is currently EXPERIMENTAL and is subject to change. 2) Works on Server 2.11 or later
     * @param streamName the name of the stream.
     * @param startTime the start time to get the first message for.
     * @return The MessageInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    MessageInfo getFirstMessage(String streamName, ZonedDateTime startTime) throws IOException, JetStreamApiException;

    /**
     * Get MessageInfo for the first message created at or after the start time matching the subject.
     * <p>
     * This API 1) is currently EXPERIMENTAL and is subject to change. 2) Works on Server 2.11 or later
     * @param streamName the name of the stream.
     * @param startTime the start time to get the first message for.
     * @param subject the subject to get the first message for.
     * @return The MessageInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    MessageInfo getFirstMessage(String streamName, ZonedDateTime startTime, String subject) throws IOException, JetStreamApiException;

    /**
     * Get MessageInfo for the message of the message sequence
     * is equal to or greater the requested sequence for the subject.
     * @param streamName the name of the stream.
     * @param seq the first possible sequence number of the message
     * @param subject the subject to get the next message for.
     * @return The MessageInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    MessageInfo getNextMessage(String streamName, long seq, String subject) throws IOException, JetStreamApiException;

    /**
     * Request a batch of messages using a {@link MessageBatchGetRequest}.
     * <p>
     * This API 1) is currently EXPERIMENTAL and is subject to change. 2) Works on Server 2.11 or later
     * @param streamName the name of the stream
     * @param messageBatchGetRequest the request details
     * @return a list containing {@link MessageInfo}
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<MessageInfo> fetchMessageBatch(String streamName, MessageBatchGetRequest messageBatchGetRequest) throws IOException, JetStreamApiException;

    /**
     * Request a batch of messages using a {@link MessageBatchGetRequest}.
     * <p>
     * This API 1) is currently EXPERIMENTAL and is subject to change. 2) Works on Server 2.11 or later
     * @param streamName the name of the stream
     * @param messageBatchGetRequest the request details
     * @return a queue used to asynchronously receive {@link MessageInfo}
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    LinkedBlockingQueue<MessageInfo> queueMessageBatch(String streamName, MessageBatchGetRequest messageBatchGetRequest) throws IOException, JetStreamApiException;

    /**
     * Request a batch of messages using a {@link MessageBatchGetRequest}.
     * <p>
     * This API 1) is currently EXPERIMENTAL and is subject to change. 2) Works on Server 2.11 or later
     * @param streamName             the name of the stream
     * @param messageBatchGetRequest the request details
     * @param handler                the handler used for receiving {@link MessageInfo}
     * @return true if all messages were received and properly terminated with a server EOB
     * @throws IOException           covers various communication issues with the NATS
     *                               server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    boolean requestMessageBatch(String streamName, MessageBatchGetRequest messageBatchGetRequest, MessageInfoHandler handler) throws IOException, JetStreamApiException;

    /**
     * Deletes a message, overwriting the message data with garbage
     * This can be considered an expensive (time-consuming) operation, but is more secure.
     * @param streamName name of the stream
     * @param seq the sequence number of the message
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return true if the delete succeeded
     */
    boolean deleteMessage(String streamName, long seq) throws IOException, JetStreamApiException;

    /**
     * Deletes a message, optionally erasing the content of the message.
     * @param streamName name of the stream
     * @param seq the sequence number of the message
     * @param erase whether to erase the message (overwriting with garbage) or only mark it as erased.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return true if the delete succeeded
     */
    boolean deleteMessage(String streamName, long seq, boolean erase) throws IOException, JetStreamApiException;

    /**
     * Gets a context for publishing and subscribing to subjects backed by Jetstream streams
     * and consumers, using the same connection and JetStreamOptions as the management.
     * @return a JetStream instance.
     */
    JetStream jetStream();
}
