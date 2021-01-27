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

import io.nats.client.impl.JetStreamApiException;

import java.io.IOException;

/**
 * JetStream Management context for creation and access to streams and consumers in NATS.
 */
public interface JetStreamManagement {

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
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    void deleteStream(String streamName) throws IOException, JetStreamApiException;

    /**
     * Gets the info for an existing stream.
     * @param streamName the stream name to use.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return stream information
     */
    StreamInfo streamInfo(String streamName) throws IOException, JetStreamApiException;

    /**
     * Purge stream messages
     * @param streamName the stream name to use.
     * @return stream information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    StreamInfo purgeStream(String streamName) throws IOException, JetStreamApiException;

    /**
     * Loads or creates a consumer.
     * @param streamName name of the stream
     * @param config the consumer configuration to use.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return consumer information.
     */
    ConsumerInfo addConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Deletes a consumer.
     * @param streamName name of the stream
     * @param consumer the name of the consumer.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    void deleteConsumer(String streamName, String consumer) throws IOException, JetStreamApiException;

    /**
     * Return pages of ConsumerInfo objects
     * @param streamName the name of the consumer.
     * @return The consumer lister
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ConsumerLister getConsumers(String streamName) throws IOException, JetStreamApiException;
}
