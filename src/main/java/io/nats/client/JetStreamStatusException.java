// Copyright 2021 The NATS Authors
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

import io.nats.client.support.Status;

/**
 * JetStreamStatusException is used to indicate an unknown status message was received.
 */
public class JetStreamStatusException extends IllegalStateException {
    public static final String DEFAULT_DESCRIPTION = "Unknown or unprocessed status message";

    private final JetStreamSubscription sub;
    private final Status status;

    /**
     * Construct JetStreamStatusException for a subscription and a status and a custom message
     * @param sub     the subscription
     * @param status  the status
     * @param message the exception message
     */
    public JetStreamStatusException(JetStreamSubscription sub, Status status, String message) {
        super(message);
        this.sub = sub;
        this.status = status;
    }

    private static String guardedStatusMessage(Status status) {
        return status == null ? DEFAULT_DESCRIPTION : status.getMessageWithCode();
    }

    /**
     * Construct JetStreamStatusException for a subscription and a status
     * @param sub the subscription
     * @param status the status
     */
    public JetStreamStatusException(JetStreamSubscription sub, Status status) {
        this(sub, status, guardedStatusMessage(status));
    }

    /**
     * Construct JetStreamStatusException for a status
     * @param status the status
     */
    public JetStreamStatusException(Status status) {
        this(null, status, guardedStatusMessage(status));
    }

    /**
     * Get the subscription this issue occurred on
     * @return the subscription
     */
    public JetStreamSubscription getSubscription() {
        return sub;
    }

    /**
     * Get the description
     * @return the description
     */
    @Deprecated
    public String getDescription() {
        return getMessage();
    }

    /**
     * Get the full status object
     *
     * @return the status
     */
    public Status getStatus() {
        return status;
    }
}