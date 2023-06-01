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

import java.time.Duration;

public interface IterableConsumer extends MessageConsumer {
    /**
     * Read the next message. Return null if the calls times out.
     * Use a timeout of 0 to wait indefinitely. This could still be interrupted if
     * the subscription is unsubscribed or the client connection is closed.
     * @param timeout the maximum time to wait
     * @return the next message for this subscriber or null if there is a timeout
     * @throws InterruptedException if one is thrown, in order to propagate it up
     * @throws JetStreamStatusCheckedException an exception representing a status that requires attention,
     *         such as the consumer was deleted on the server in the middle of use.
     */
    Message nextMessage(Duration timeout) throws InterruptedException, JetStreamStatusCheckedException;

    /**
     * Read the next message. Return null if the calls times out.
     * Use a timeout of 0 to wait indefinitely. This could still be interrupted if
     * the subscription is unsubscribed or the client connection is closed.
     * @param timeoutMillis the maximum time to wait
     * @return the next message for this subscriber or null if there is a timeout
     * @throws InterruptedException if one is thrown, in order to propagate it up
     * @throws JetStreamStatusCheckedException an exception representing a status that requires attention,
     *         such as the consumer was deleted on the server in the middle of use.
     */
    Message nextMessage(long timeoutMillis) throws InterruptedException, JetStreamStatusCheckedException;
}
