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

import io.nats.client.Message;
import io.nats.client.SubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;

import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.impl.MessageManager.ManageResult.MESSAGE;
import static io.nats.client.impl.MessageManager.ManageResult.STATUS;

class OrderedMessageManager extends PushMessageManager {

    protected long expectedExternalConsumerSeq;
    protected final AtomicReference<String> targetSid;

    protected OrderedMessageManager(NatsConnection conn,
                          NatsJetStream js,
                          String stream,
                          SubscribeOptions so,
                          ConsumerConfiguration originalCc,
                          boolean queueMode,
                          boolean syncMode) {
        super(conn, js, stream, so, originalCc, queueMode, syncMode);
        expectedExternalConsumerSeq = 1; // always starts at 1
        targetSid = new AtomicReference<>();
    }

    @Override
    protected void startup(NatsJetStreamSubscription sub) {
        super.startup(sub);
        targetSid.set(sub.getSID());
    }

    @Override
    protected ManageResult manage(Message msg) {
        if (!msg.getSID().equals(targetSid.get())) {
            return STATUS; // wrong sid is throwaway from previous consumer that errored
        }

        if (msg.isJetStream()) {
            long receivedConsumerSeq = msg.metaData().consumerSequence();
            if (expectedExternalConsumerSeq != receivedConsumerSeq) {
                handleErrorCondition();
                return STATUS;
            }
            trackJsMessage(msg);
            expectedExternalConsumerSeq++;
            return MESSAGE;
        }

        return manageStatus(msg);
    }

    private void handleErrorCondition() {
        try {
            targetSid.set(null);
            expectedExternalConsumerSeq = 1; // consumer always starts with consumer sequence 1

            // 1. shutdown the manager, for instance stops heartbeat timers
            shutdown();

            // 2. re-subscribe. This means kill the sub then make a new one
            //    New sub needs a new deliverSubject
            String newDeliverSubject = sub.connection.createInbox();
            sub.reSubscribe(newDeliverSubject);
            targetSid.set(sub.getSID());

            // 3. make a new consumer using the same deliver subject but
            //    with a new starting point
            ConsumerConfiguration userCC = ConsumerConfiguration.builder(originalCc)
                .deliverPolicy(DeliverPolicy.ByStartSequence)
                .deliverSubject(newDeliverSubject)
                .startSequence(Math.max(1, lastStreamSeq + 1))
                .startTime(null) // clear start time in case it was originally set
                .build();
            js._createConsumerUnsubscribeOnException(stream, userCC, sub);

            // 4. restart the manager.
            startup(sub);
        }
        catch (Exception e) {
            IllegalStateException ise = new IllegalStateException("Ordered subscription fatal error.", e);
            js.conn.processException(ise);
            if (syncMode) {
                throw ise;
            }
        }
    }
}
