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

package io.nats.client.impl;

import io.nats.client.JetStreamStatusException;
import io.nats.client.Message;
import io.nats.client.SubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.support.Status;

import static io.nats.client.ErrorListener.FlowControlSource;
import static io.nats.client.ErrorListener.FlowControlSource.FLOW_CONTROL;
import static io.nats.client.ErrorListener.FlowControlSource.HEARTBEAT;
import static io.nats.client.impl.MessageManager.ManageResult.*;
import static io.nats.client.support.NatsJetStreamConstants.CONSUMER_STALLED_HDR;

class PushMessageManager extends MessageManager {

    protected final NatsJetStream js;
    protected final String stream;
    protected final ConsumerConfiguration originalCc;

    protected final boolean queueMode;
    protected final boolean fc;
    protected String lastFcSubject;

    protected PushMessageManager(NatsConnection conn,
                       NatsJetStream js,
                       String stream,
                       SubscribeOptions so,
                       ConsumerConfiguration originalCc,
                       boolean queueMode,
                       boolean syncMode)
    {
        super(conn, so, syncMode);
        this.js = js;
        this.stream = stream;
        this.originalCc = originalCc;
        this.queueMode = queueMode;

        if (queueMode) {
            fc = false;
        }
        else {
            configureIdleHeartbeat(originalCc.getIdleHeartbeat(), so.getMessageAlarmTime());
            fc = hb && originalCc.isFlowControl(); // can't have fc w/o heartbeat
        }
    }

    protected boolean isQueueMode()     { return queueMode; }
    protected boolean isFc()            { return fc; }
    protected String getLastFcSubject() { return lastFcSubject; }

    @Override
    protected void startup(NatsJetStreamSubscription sub) {
        super.startup(sub);
        sub.setBeforeQueueProcessor(this::beforeQueueProcessorImpl);
        if (hb) {
            initOrResetHeartbeatTimer();
        }
    }

    @Override
    protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
        if (hb) {
            messageReceived(); // only need to track when heartbeats are expected
            Status status = msg.getStatus();
            if (status != null) {
                // only plain heartbeats do not get queued
                if (status.isHeartbeat()) {
                    return hasFcSubject(msg); // true if not a plain hb
                }
            }
        }
        return true;
    }

    protected boolean hasFcSubject(Message msg) {
        return msg.getHeaders() != null && msg.getHeaders().containsKey(CONSUMER_STALLED_HDR);
    }

    protected String extractFcSubject(Message msg) {
        return msg.getHeaders() == null ? null : msg.getHeaders().getFirst(CONSUMER_STALLED_HDR);
    }

    @Override
    protected ManageResult manage(Message msg) {
        if (msg.getStatus() == null) {
            trackJsMessage(msg);
            return MESSAGE;
        }

        return manageStatus(msg);
    }

    protected ManageResult manageStatus(Message msg) {
        // this checks fc, hb and unknown
        // only process fc and hb if those flags are set
        // otherwise they are simply known statuses
        Status status = msg.getStatus();
        if (fc) {
            boolean isFcNotHb = status.isFlowControl();
            String fcSubject = isFcNotHb ? msg.getReplyTo() : extractFcSubject(msg);
            if (fcSubject != null) {
                processFlowControl(fcSubject, isFcNotHb ? FLOW_CONTROL : HEARTBEAT);
                return STATUS;
            }
        }

        conn.executeCallback((c, el) -> el.unhandledStatus(c, sub, status));
        if (syncMode) {
            throw new JetStreamStatusException(sub, status,
                "Unknown or unprocessed status message: " + status.getMessageWithCode());
        }
        return ERROR;
    }

    private void processFlowControl(String fcSubject, FlowControlSource source) {
        // we may get multiple fc/hb messages with the same reply
        // only need to post to that subject once
        if (fcSubject != null && !fcSubject.equals(lastFcSubject)) {
            conn.publishInternal(fcSubject, null, null, null);
            lastFcSubject = fcSubject; // set after publish in case the pub fails
            conn.executeCallback((c, el) -> el.flowControlProcessed(c, sub, fcSubject, source));
        }
    }
}
