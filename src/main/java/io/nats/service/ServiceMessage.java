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

package io.nats.service;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.client.impl.AckType;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.JsonSerializable;
import io.nats.client.support.Status;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class ServiceMessage implements Message {

    public static final String NATS_SERVICE_ERROR = "Nats-Service-Error";
    public static final String NATS_SERVICE_ERROR_CODE = "Nats-Service-Error-Code";

    private final Message message;

    ServiceMessage(Message message) {
        this.message = message;
    }

    public void respond(Connection conn, byte[] response) {
        conn.publish(message.getReplyTo(), response);
    }

    public void respond(Connection conn, String response) {
        conn.publish(message.getReplyTo(), response.getBytes(StandardCharsets.UTF_8));
    }

    public void respond(Connection conn, JsonSerializable response) {
        conn.publish(message.getReplyTo(), response.serialize());
    }

    public void respond(Connection conn, byte[] response, Headers headers) {
        conn.publish(NatsMessage.builder().subject(message.getReplyTo()).data(response).headers(headers).build());
    }

    public void respond(Connection conn, String response, Headers headers) {
        conn.publish(NatsMessage.builder().subject(message.getReplyTo()).data(response).headers(headers).build());
    }

    public void respond(Connection conn, JsonSerializable response, Headers headers) {
        conn.publish(NatsMessage.builder().subject(message.getReplyTo()).data(response.serialize()).headers(headers).build());
    }

    public void respondStandardError(Connection conn, String errorMessage, int errorCode) {
        conn.publish(NatsMessage.builder()
            .subject(message.getReplyTo())
            .headers(new Headers()
                .put(NATS_SERVICE_ERROR, errorMessage)
                .put(NATS_SERVICE_ERROR_CODE, "" + errorCode))
            .build());
    }

    @Override
    public String getSubject() {
        return message.getSubject();
    }

    @Override
    public String getReplyTo() {
        return message.getReplyTo();
    }

    @Override
    public boolean hasHeaders() {
        return message.hasHeaders();
    }

    @Override
    public Headers getHeaders() {
        return message.getHeaders();
    }

    @Override
    public boolean isStatusMessage() {
        return message.isStatusMessage();
    }

    @Override
    public Status getStatus() {
        return message.getStatus();
    }

    @Override
    public byte[] getData() {
        return message.getData();
    }

    @Override
    public boolean isUtf8mode() {
        return message.isUtf8mode();
    }

    @Override
    public Subscription getSubscription() {
        return message.getSubscription();
    }

    @Override
    public String getSID() {
        return message.getSID();
    }

    @Override
    public Connection getConnection() {
        return message.getConnection();
    }

    @Override
    public NatsJetStreamMetaData metaData() {
        return message.metaData();
    }

    @Override
    public AckType lastAck() {
        return message.lastAck();
    }

    @Override
    public void ack() {
        message.ack();
    }

    @Override
    public void ackSync(Duration timeout) throws TimeoutException, InterruptedException {
        message.ackSync(timeout);
    }

    @Override
    public void nak() {
        message.nak();
    }

    @Override
    public void nakWithDelay(Duration nakDelay) {
        message.nakWithDelay(nakDelay);
    }

    @Override
    public void nakWithDelay(long nakDelayMillis) {
        message.nakWithDelay(nakDelayMillis);
    }

    @Override
    public void term() {
        message.term();
    }

    @Override
    public void inProgress() {
        message.inProgress();
    }

    @Override
    public boolean isJetStream() {
        return message.isJetStream();
    }

    @Override
    public long consumeByteCount() {
        return message.consumeByteCount();
    }
}
