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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * A Consumer in the NATS library is an object that represents an incoming queue of
 * messages. There are two types of consumers {@link Dispatcher} and {@link Subscription}.
 * This interface unifies the responsibilities that are focused on dealing with slow consumers.
 * The {@link Dispatcher} and {@link Subscription} deal with the mechanics of their specialized roles.
 * 
 * <p>A slow consumer is defined as a consumer that is not handling messages as quickly as they are arriving. For example,
 * if the application code doesn't call {@link Subscription#nextMessage(java.time.Duration) nextMessage()}
 * often enough on a Subscription.
 * 
 * <p>By default the library will allow a consumer to be a bit slow, at times, by caching messages for it in a queue.
 * The size of this queue is determined by {@link #setPendingLimits(long, long) setPendingLimits()}. When a consumer
 * maxes out its queue size, either by message count or bytes, the library will start to drop messages.
 */
public interface Consumer {

    /**
     * The default number of messages a consumer will hold before it starts to drop them.
     */
    public static final long DEFAULT_MAX_MESSAGES = 512 * 1024;

    /**
     * The default number of bytes a consumer will hold before it starts to drop messages.
     */
    public static final long DEFAULT_MAX_BYTES = 64 * 1024 * 1024;

    /**
     * Set limits on the maximum number of messages, or maximum size of messages this consumer
     * will hold before it starts to drop new messages waiting for the application to drain the queue.
     * 
     * <p>Messages are dropped as they encounter a full queue, which is to say, new messages are dropped rather than
     * old messages. If a queue is 10 deep and fills up, the 11th message is dropped.
     * 
     * <p>Sizes are checked after the fact, so if the max bytes is too small for the first pending
     * message to arrive, it will still be stored, only the next message will fail.
     * 
     * <p>Setting a value to anything less than or equal to 0 will disable this check.
     * 
     * @param maxMessages the maximum message count to hold, defaults to {@value #DEFAULT_MAX_MESSAGES}
     * @param maxBytes the maximum bytes to hold, defaults to {@value #DEFAULT_MAX_BYTES}
     */
    public void setPendingLimits(long maxMessages, long maxBytes);

    /**
     * @return the pending message limit set by {@link #setPendingLimits(long, long) setPendingLimits}
     */
    public long getPendingMessageLimit();

    /**
     * @return the pending byte limit set by {@link #setPendingLimits(long, long) setPendingLimits}
     */
    public long getPendingByteLimit();

    /**
     * @return the number of messages waiting to be delivered/popped, {@link #setPendingLimits(long, long) setPendingLimits}
     */
    public long getPendingMessageCount();

    /**
     * @return the cumulative size of the messages waiting to be delivered/popped, {@link #setPendingLimits(long, long) setPendingLimits}
     */
    public long getPendingByteCount();

    /**
     * @return the total number of messages delivered to this consumer, for all time
     */
    public long getDeliveredCount();

    /**
     * @return the number of messages dropped from this consumer, since the last call to {@link #clearDroppedCount() clearDroppedCount}.
     */
    public long getDroppedCount();

    /**
     * Reset the drop count to 0.
     */
    public void clearDroppedCount();

    /**
     * @return whether this consumer is still processing messages.
     * For a subscription the answer is false after unsubscribe. For a dispatcher, false after stop.
     */
    public boolean isActive();

    /**
     * Drain tells the consumer to process in flight, or cached messages, but stop receiving new ones. The library will
     * flush the unsubscribe call(s) insuring that any publish calls made by this client are included. When all messages
     * are processed the consumer effectively becomes unsubscribed.
     * 
     * A future is used to allow this call to be treated as synchronous or asynchronous as
     * needed by the application.
     * 
     * @param timeout The time to wait for the drain to succeed, pass 0 to wait
     *                    forever. Drain involves moving messages to and from the server
     *                    so a very short timeout is not recommended.
     * @return A future that can be used to check if the drain has completed
     * @throws InterruptedException if the thread is interrupted
     */
    public CompletableFuture<Boolean> drain(Duration timeout) throws InterruptedException;
}