// Copyright 2023 The NATS Authors
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

package io.nats.examples.testapp;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.examples.testapp.support.CommandLine;
import io.nats.examples.testapp.support.ConsumerKind;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ConnectableConsumer {

    protected final Connection nc;
    protected final JetStream js;
    protected final UiConnectionListener connectionListener;
    protected final UiErrorListener errorListener;
    protected final AtomicLong lastReceivedSequence;
    protected final MessageHandler handler;
    protected final ConsumerKind consumerKind;

    protected final CommandLine cmd;
    protected String initials;
    protected String name;
    protected String durableName;
    protected String label;

    public ConnectableConsumer(CommandLine cmd, String initials, ConsumerKind consumerKind) throws IOException, InterruptedException, JetStreamApiException {
        this.cmd = cmd;
        lastReceivedSequence = new AtomicLong(0);
        this.consumerKind = consumerKind;
        switch (consumerKind) {
            case Durable:
                durableName = initials + "-dur-" + new NUID().nextSequence();
                break;
            case Ephemeral:
                durableName = null;
                break;
            case Ordered:
                durableName = null;
                break;
        }
        this.initials = initials;
        updateNameAndLabel(durableName == null
            ? initials + "-con-" + new NUID().nextSequence()
            : durableName);

        connectionListener = new UiConnectionListener(label);
        errorListener = new UiErrorListener(label);

        Options options = new Options.Builder()
            .servers(cmd.servers)
            .connectionListener(connectionListener)
            .errorListener(errorListener)
            .maxReconnects(-1)
            .build();

        nc = Nats.connect(options);
        js = nc.jetStream();

        handler = m -> {
            m.ack();
            long seq = m.metaData().streamSequence();
            lastReceivedSequence.set(seq);
            Ui.workMessage(label, "Last Received Seq: " + seq);
        };
    }

    protected void updateNameAndLabel(String updatedName) {
        name = updatedName;
        if (updatedName == null) {
            label = consumerKind.name();
        }
        else {
            label = name + " (" + consumerKind.name() + ")";
        }
    }

    public long getLastReceivedSequence() {
        return lastReceivedSequence.get();
    }

    protected ConsumerConfiguration.Builder newCreateConsumer() {
        return recreateConsumer(0);
    }

    private ConsumerConfiguration.Builder recreateConsumer(long last) {
        return ConsumerConfiguration.builder()
            .name(consumerKind == ConsumerKind.Ordered ? null : name)
            .durable(durableName)
            .deliverPolicy(last == 0 ? DeliverPolicy.All : DeliverPolicy.ByStartSequence)
            .startSequence(last == 0 ? -1 : last + 1)
            .filterSubject(cmd.subject);
    }
}
