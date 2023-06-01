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

import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * JetStream context for creation and access to streams and consumers in NATS.
 */
public interface JetStream {

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The default publish options will be used.
     * The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * js.publish("destination", "message".getBytes("UTF-8"))
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     *
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The acknowledgement of the publish
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(String subject, byte[] body) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The default publish options will be used.
     * The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * Headers h = new Headers().put("foo", "bar");
     * js.publish("destination", h, "message".getBytes("UTF-8"))
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
     * @param subject the subject to send the message to
     * @param headers Optional headers to publish with the message.
     * @param body the message body
     * @return The acknowledgement of the publish
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(String subject, Headers headers, byte[] body) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * js.publish("destination", "message".getBytes("UTF-8"), publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
     * @param subject the subject to send the message to
     * @param body the message body
     * @param options publisher options
     * @return The acknowledgement of the publish
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(String subject, byte[] body, PublishOptions options) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * Headers h = new Headers().put("foo", "bar");
     * js.publish("destination", h, "message".getBytes("UTF-8"), publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
     * @param subject the subject to send the message to
     * @param headers Optional headers to publish with the message.
     * @param body the message body
     * @param options publisher options
     * @return The acknowledgement of the publish
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(String subject, Headers headers, byte[] body, PublishOptions options) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The default publish options will be used.
     * The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * js.publish(message)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     *
     * <p>The Message object allows you to set a replyTo, but in publish requests,
     * the replyTo is reserved for internal use as the address for the
     * server to respond to the client with the PublishAck.</p>
     *
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
     * @param message the message to send
     * @return The acknowledgement of the publish
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(Message message) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * js.publish(message, publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     *
     * <p>The Message object allows you to set a replyTo, but in publish requests,
     * the replyTo is reserved for internal use as the address for the
     * server to respond to the client with the PublishAck.</p>
     *
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
     * @param message the message to send
     * @param options publisher options
     * @return The acknowledgement of the publish
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(Message message, PublishOptions options) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject but does not wait for a response from
     * Jetstream. The default publish options will be used.
     * The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * CompletableFuture&lt;PublishAck&gt; future =
     *     js.publishAsync("destination", "message".getBytes("UTF-8"),)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     * The future me be completed with an exception, either
     * an IOException covers various communication issues with the NATS server such as timeout or interruption
     * - or - a JetStreamApiException the request had an error related to the data
     *
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The future
     */
    CompletableFuture<PublishAck> publishAsync(String subject, byte[] body);

    /**
     * Send a message to the specified subject but does not wait for a response from
     * Jetstream. The default publish options will be used.
     * The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * Headers h = new Headers().put("foo", "bar");
     * CompletableFuture&lt;PublishAck&gt; future =
     *     js.publishAsync("destination", h, "message".getBytes("UTF-8"),)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     * The future me be completed with an exception, either
     * an IOException covers various communication issues with the NATS server such as timeout or interruption
     * - or - a JetStreamApiException the request had an error related to the data
     *
     * @param subject the subject to send the message to
     * @param headers Optional headers to publish with the message.
     * @param body the message body
     * @return The future
     */
    CompletableFuture<PublishAck> publishAsync(String subject, Headers headers, byte[] body);

    /**
     * Send a message to the specified subject but does not wait for a response from
     * Jetstream. The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * CompletableFuture&lt;PublishAck&gt; future =
     *     js.publishAsync("destination", "message".getBytes("UTF-8"), publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     * The future me be completed with an exception, either
     * an IOException covers various communication issues with the NATS server such as timeout or interruption
     * - or - a JetStreamApiException the request had an error related to the data
     *
     * @param subject the subject to send the message to
     * @param body the message body
     * @param options publisher options
     * @return The future
     */
    CompletableFuture<PublishAck> publishAsync(String subject, byte[] body, PublishOptions options);

    /**
     * Send a message to the specified subject but does not wait for a response from
     * Jetstream. The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * Headers h = new Headers().put("foo", "bar");
     * CompletableFuture&lt;PublishAck&gt; future =
     *     js.publishAsync("destination", h, "message".getBytes("UTF-8"), publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     * The future me be completed with an exception, either
     * an IOException covers various communication issues with the NATS server such as timeout or interruption
     * - or - a JetStreamApiException the request had an error related to the data
     *
     * @param subject the subject to send the message to
     * @param headers Optional headers to publish with the message.
     * @param body the message body
     * @param options publisher options
     * @return The future
     */
    CompletableFuture<PublishAck> publishAsync(String subject, Headers headers, byte[] body, PublishOptions options);

    /**
     * Send a message to the specified subject but does not wait for a response from
     * Jetstream. The default publish options will be used.
     * The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * CompletableFuture&lt;PublishAck&gt; future = js.publishAsync(message)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     * The future me be completed with an exception, either
     * an IOException covers various communication issues with the NATS server such as timeout or interruption
     * - or - a JetStreamApiException the request had an error related to the data
     *
     * <p>The Message object allows you to set a replyTo, but in publish requests,
     * the replyTo is reserved for internal use as the address for the
     * server to respond to the client with the PublishAck.</p>
     *
     * @param message the message to send
     * @return The future
     */
    CompletableFuture<PublishAck> publishAsync(Message message);

    /**
     * Send a message to the specified subject but does not wait for a response from
     * Jetstream. The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * CompletableFuture&lt;PublishAck&gt; future = js.publishAsync(message, publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     * The future me be completed with an exception, either
     * an IOException covers various communication issues with the NATS server such as timeout or interruption
     * - or - a JetStreamApiException the request had an error related to the data
     *
     * <p>The Message object allows you to set a replyTo, but in publish requests,
     * the replyTo is reserved for internal use as the address for the
     * server to respond to the client with the PublishAck.</p>
     *
     * @param message the message to publish
     * @param options publisher options
     * @return The future
     */
    CompletableFuture<PublishAck> publishAsync(Message message, PublishOptions options);

    /**
     * Create a synchronous subscription to the specified subject with default options.
     *
     * <p>Use the {@link io.nats.client.Subscription#nextMessage(Duration)}
     * method to read messages for this subscription.
     *
     * <p>See {@link io.nats.client.Connection#createDispatcher(MessageHandler) createDispatcher} for
     * information about creating an asynchronous subscription with callbacks.
     *
     * @param subject the subject to subscribe to
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subject) throws IOException, JetStreamApiException;

    /**
     * Create a synchronous subscription to the specified subject.
     *
     * <p>Use the {@link io.nats.client.Subscription#nextMessage(Duration)}
     * method to read messages for this subscription.
     *
     * <p>See {@link io.nats.client.Connection#createDispatcher(MessageHandler) createDispatcher} for
     * information about creating an asynchronous subscription with callbacks.
     *
     * @param subject the subject to subscribe to
     * @param options optional subscription options
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subject, PushSubscribeOptions options) throws IOException, JetStreamApiException;

    /**
     * Create a synchronous subscription to the specified subject.
     *
     * <p>Use the {@link io.nats.client.Subscription#nextMessage(Duration) nextMessage}
     * method to read messages for this subscription.
     *
     * <p>See {@link io.nats.client.Connection#createDispatcher(MessageHandler) createDispatcher} for
     * information about creating an asynchronous subscription with callbacks.
     *
     * @param subject the subject to subscribe to
     * @param queue the optional queue group to join
     * @param options optional subscription options
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subject, String queue, PushSubscribeOptions options) throws IOException, JetStreamApiException;

    /**
     * Create an asynchronous subscription to the specified subject under the control of the
     * specified dispatcher. Since a MessageHandler is also required, the Dispatcher will
     * not prevent duplicate subscriptions from being made.
     *
     * @param subject The subject to subscribe to
     * @param dispatcher The dispatcher to handle this subscription
     * @param handler The target for the messages
     * @param autoAck Whether to auto ack
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException;

    /**
     * Create an asynchronous subscription to the specified subject under the control of the
     * specified dispatcher. Since a MessageHandler is also required, the Dispatcher will
     * not prevent duplicate subscriptions from being made.
     *
     * @param subject The subject to subscribe to.
     * @param dispatcher The dispatcher to handle this subscription
     * @param handler The target for the messages
     * @param autoAck Whether to auto ack
     * @param options The options for this subscription.
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException;

    /**
     * Create an asynchronous subscription to the specified subject under the control of the
     * specified dispatcher. Since a MessageHandler is also required, the Dispatcher will
     * not prevent duplicate subscriptions from being made.
     *
     * @param subject The subject to subscribe to.
     * @param queue the optional queue group to join
     * @param dispatcher The dispatcher to handle this subscription
     * @param handler The target for the messages
     * @param autoAck Whether to auto ack
     * @param options optional subscription options
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subject, String queue, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException;

    /**
     * Create a subscription to the specified subject in the mode of pull, with additional options
     *
     * @param subject The subject to subscribe to
     * @param options pull subscription options
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subject, PullSubscribeOptions options) throws IOException, JetStreamApiException;

    /**
     * Create an asynchronous subscription to the specified subject in the mode of pull, with additional options
     *
     * @param subject The subject to subscribe to
     * @param dispatcher The dispatcher to handle this subscription
     * @param handler The target for the messages
     * @param options pull subscription options
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, PullSubscribeOptions options) throws IOException, JetStreamApiException;

    /**
     * Create a stream context for a specific named stream. Verifies that the stream exists.
     * EXPERIMENTAL API SUBJECT TO CHANGE
     * @param streamName the name of the stream
     * @return a StreamContext object
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    StreamContext streamContext(String streamName) throws IOException, JetStreamApiException;

    /**
     * Create a consumer context for a specific named stream and specific named consumer.
     * Verifies that the stream and consumer exist.
     * EXPERIMENTAL API SUBJECT TO CHANGE
     * @param streamName the name of the stream
     * @param consumerName the name of the consumer
     * @return a ConsumerContext object
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ConsumerContext consumerContext(String streamName, String consumerName) throws IOException, JetStreamApiException;
}
