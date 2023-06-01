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

package io.nats.client.impl;

import io.nats.client.Consumer;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

abstract class NatsConsumer implements Consumer {

    NatsConnection connection;
    private final AtomicLong maxMessages;
    private final AtomicLong maxBytes;
    private final AtomicLong droppedMessages;
    private final AtomicLong messagesDelivered;
    private final AtomicBoolean slow;
    private final AtomicReference<CompletableFuture<Boolean>> drainingFuture;

    NatsConsumer(NatsConnection conn) {
        this.connection = conn;
        this.maxMessages = new AtomicLong(Consumer.DEFAULT_MAX_MESSAGES);
        this.maxBytes = new AtomicLong(Consumer.DEFAULT_MAX_BYTES);
        this.droppedMessages = new AtomicLong();
        this.messagesDelivered = new AtomicLong(0);
        this.slow = new AtomicBoolean(false);
        this.drainingFuture = new AtomicReference<>();
    }

    /**
     * Set limits on the maximum number of messages, or maximum size of messages
     * this consumer will hold before it starts to drop new messages waiting.
     * <p>
     * Messages are dropped as they encounter a full queue, which is to say, new
     * messages are dropped rather than old messages. If a queue is 10 deep and
     * fills up, the 11th message is dropped.
     * <p>
     * Any value less than or equal to zero means unlimited and will be stored as 0.
     * @param maxMessages the maximum message count to hold, defaults to
     *                    {@value #DEFAULT_MAX_MESSAGES}.
     * @param maxBytes    the maximum bytes to hold, defaults to
     *                    {@value #DEFAULT_MAX_BYTES}.
     */
    public void setPendingLimits(long maxMessages, long maxBytes) {
        this.maxMessages.set(maxMessages <= 0 ? 0 : maxMessages);
        this.maxBytes.set(maxBytes <= 0 ? 0 : maxBytes);
    }

    /**
     * @return the pending message limit set by {@link #setPendingLimits(long, long)
     *         setPendingLimits}.
     */
    public long getPendingMessageLimit() {
        return this.maxMessages.get();
    }

    /**
     * @return the pending byte limit set by {@link #setPendingLimits(long, long)
     *         setPendingLimits}.
     */
    public long getPendingByteLimit() {
        return this.maxBytes.get();
    }

    /**
     * @return the number of messages waiting to be delivered/popped,
     *         {@link #setPendingLimits(long, long) setPendingLimits}.
     */
    public long getPendingMessageCount() {
        return this.getMessageQueue() != null ? this.getMessageQueue().length() : 0;
    }

    /**
     * @return the cumulative size of the messages waiting to be delivered/popped,
     *         {@link #setPendingLimits(long, long) setPendingLimits}.
     */
    public long getPendingByteCount() {
        return this.getMessageQueue() != null ? this.getMessageQueue().sizeInBytes() : 0;
    }

    /**
     * @return the total number of messages delivered to this consumer, for all
     *         time.
     */
    public long getDeliveredCount() {
        return this.messagesDelivered.get();
    }

    void incrementDeliveredCount() {
        this.messagesDelivered.incrementAndGet();
    }

    void incrementDroppedCount() {
        this.droppedMessages.incrementAndGet();
    }

    /**
     * @return the number of messages dropped from this consumer, since the last
     *         call to {@link @clearDroppedCount}.
     */
    public long getDroppedCount() {
        return this.droppedMessages.get();
    }

    /**
     * Reset the drop count to 0.
     */
    public void clearDroppedCount() {
        this.droppedMessages.set(0);
    }

    void markSlow() {
        this.slow.set(true);
    }

    void markNotSlow() {
        this.slow.set(false);
    }

    boolean isMarkedSlow() {
        return this.slow.get();
    }

    boolean hasReachedPendingLimits() {
        long ml = maxMessages.get();
        if (ml > 0 && getPendingMessageCount() >= ml) {
            return true;
        }
        long bl = maxBytes.get();
        return bl > 0 && getPendingByteCount() >= bl;
    }

    void markDraining(CompletableFuture<Boolean> future) {
        this.drainingFuture.set(future);
    }

    void markUnsubedForDrain() {
        if (this.getMessageQueue() != null) {
            this.getMessageQueue().drain();
        }
    }

    CompletableFuture<Boolean> getDrainingFuture() {
        return this.drainingFuture.get();
    }

    boolean isDraining() {
        return this.drainingFuture.get() != null;
    }

    boolean isDrained() {
        return isDraining() && this.getPendingMessageCount() == 0;
    }

    /**
    * Drain tells the consumer to process in flight, or cached messages, but stop receiving new ones. The library will
    * flush the unsubscribe call(s) insuring that any publish calls made by this client are included. When all messages
    * are processed the consumer effectively becomes unsubscribed.
    * 
    * @param timeout The time to wait for the drain to succeed, pass 0 to wait
    *                    forever. Drain involves moving messages to and from the server
    *                    so a very short timeout is not recommended.
    * @return A future that can be used to check if the drain has completed
    * @throws InterruptedException if the thread is interrupted
    */
   public CompletableFuture<Boolean> drain(Duration timeout) throws InterruptedException {
       if (!this.isActive() || this.connection==null) {
           throw new IllegalStateException("Consumer is closed");
       }

       if (isDraining()) {
           return this.getDrainingFuture();
       }

       Instant start = Instant.now();
       final CompletableFuture<Boolean> tracker = new CompletableFuture<>();
       this.markDraining(tracker);
       this.sendUnsubForDrain();

       try {
            this.connection.flush(timeout); // Flush and wait up to the timeout
       } catch (TimeoutException e) {
           this.connection.processException(e);
       }

       this.markUnsubedForDrain();

        // Wait for the timeout or the pending count to go to 0, skipped if conn is
        // draining
        connection.getExecutor().submit(() -> {
            try {
                Instant now = Instant.now();

                while (timeout == null || timeout.equals(Duration.ZERO)
                        || Duration.between(start, now).compareTo(timeout) < 0) {
                    if (this.isDrained()) {
                        break;
                    }

                    Thread.sleep(1); // Sleep 1 milli

                    now = Instant.now();
                }

                this.cleanUpAfterDrain();
            } catch (InterruptedException e) {
                this.connection.processException(e);
            } finally {
                tracker.complete(this.isDrained());
            }
       });

       return getDrainingFuture();
   }

    /**
     * @return whether or not this consumer is still processing messages. For a
     *         subscription the answer is false after unsubscribe. For a dispatcher,
     *         false after stop.
     */
    public abstract boolean isActive();

    abstract MessageQueue getMessageQueue();

    /**
     * Called during drain to tell the consumer to send appropriate unsub requests
     * to the connection.
     * 
     * A subscription will unsub itself, while a dispatcher will unsub all of its
     * subscriptions.
     */
    abstract void sendUnsubForDrain();

    /**
     * Abstract method, called by the connection when the drain is complete.
     */
    abstract void cleanUpAfterDrain();
}