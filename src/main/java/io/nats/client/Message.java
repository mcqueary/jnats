// Copyright 2015-2018 The NATS Authors
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

import io.nats.client.impl.AckType;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.nats.client.support.Status;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * The NATS library uses a Message object to encapsulate incoming messages. Applications
 * publish and send requests with raw strings and byte[] but incoming messages can have a few
 * values, so they need a wrapper.
 *
 * <p>The byte[] returned by {@link #getData() getData()} is not shared with any library code
 * and is safe to manipulate.
 */
public interface Message {

	/**
	 * @return the subject that this message was sent to
	 */
	String getSubject();

	/**
	 * @return the subject the application is expected to send a reply message on
	 */
	String getReplyTo();

	/**
	 * @return true if there are headers
	 */
	boolean hasHeaders();

	/**
	 * @return the headers object the message
	 */
	Headers getHeaders();

	/**
	 * @return true if there is status
	 */
	boolean isStatusMessage();

	/**
	 * @return the status object message
	 */
	Status getStatus();

	/**
	 * @return the data from the message
	 */
	byte[] getData();

	/**
	 * @return if is utf8Mode
	 */
	boolean isUtf8mode();

	/**
	 * @return the Subscription associated with this message, may be owned by a Dispatcher
	 */
	Subscription getSubscription();

	/**
	 * @return the id associated with the subscription, used by the connection when processing an incoming
	 * message from the server
	 */
	String getSID();

	/**
	 * @return the connection which can be used for publishing, will be null if the subscription is null
	 */
	Connection getConnection();

	/**
	 * Gets the metadata associated with a JetStream message.
	 * @return metadata or null if the message is not a JetStream message.
	 */
	NatsJetStreamMetaData metaData();

	/**
	 * the last ack that was done with this message
	 * @return the last ack or null
	 */
	AckType lastAck();

	/**
	 * ack acknowledges a JetStream messages received from a Consumer, indicating the message
	 * should not be received again later.
	 */
	void ack();

	/**
	 * ack acknowledges a JetStream messages received from a Consumer, indicating the message
	 * should not be received again later.  Duration.ZERO does not confirm the acknowledgement.
	 * @param timeout the duration to wait for an ack confirmation
     * @throws TimeoutException if a timeout was specified and the NATS server does not return a response
     * @throws InterruptedException if the thread is interrupted
	 */
	void ackSync(Duration timeout) throws TimeoutException, InterruptedException;

	/**
	 * nak acknowledges a JetStream message has been received but indicates that the message
	 * is not completely processed and should be sent again later.
	 */
	void nak();

	/**
	 * nak acknowledges a JetStream message has been received but indicates that the message
	 * is not completely processed and should be sent again later, after at least the delay amount.
	 * @param nakDelay tell the server how long to delay before processing the ack
	 */
	void nakWithDelay(Duration nakDelay);

	/**
	 * nak acknowledges a JetStream message has been received but indicates that the message
	 * is not completely processed and should be sent again later, after at least the delay amount.
	 * @param nakDelayMillis tell the server how long to delay before processing the ack
	 */
	void nakWithDelay(long nakDelayMillis);

	/**
	 * term instructs the server to stop redelivery of this message without acknowledging it as
	 * successfully processed.
	 */
	void term();

	/**
	 *  Indicates that this message is being worked on and reset redelivery timer in the server.
	 */
	void inProgress();

	/**
	 * Checks if a message is from JetStream or is a standard message.
	 * @return true if the message is from JetStream.
	 */
	boolean isJetStream();

	/**
	 * The number of bytes the server counts for the message when calculating byte counts.
	 * Only applies to JetStream messages received from the server.
	 * @return the consumption byte count or -1 if the message implementation does not support this method
	 */
	long consumeByteCount();
}
