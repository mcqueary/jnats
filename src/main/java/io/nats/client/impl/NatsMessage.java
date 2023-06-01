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

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.client.support.ByteArrayBuilder;
import io.nats.client.support.Status;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static io.nats.client.support.NatsConstants.*;
import static io.nats.client.support.Validator.validateReplyTo;
import static io.nats.client.support.Validator.validateSubject;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

public class NatsMessage implements Message {

    protected static final String NOT_A_JET_STREAM_MESSAGE = "Message is not a JetStream message";

    protected String subject;
    protected String replyTo;
    protected byte[] data;
    protected boolean utf8mode;
    protected Headers headers;

    // incoming specific : subject, replyTo, data and these fields
    protected String sid;
    protected int controlLineLength;

    // protocol specific : just this field
    protected ByteArrayBuilder protocolBab;

    // housekeeping
    protected int sizeInBytes = -1;
    protected int headerLen = 0;
    protected int dataLen;

    protected NatsSubscription subscription;

    NatsMessage next; // for linked list

    protected AckType lastAck;

    // ----------------------------------------------------------------------------------------------------
    // Constructors - Prefer to use Builder
    // ----------------------------------------------------------------------------------------------------
    protected NatsMessage() {
        this((byte[])null);
    }

    protected NatsMessage(byte[] data) {
        this.data = data == null ? EMPTY_BODY : data;
        dataLen = this.data.length;
    }

    @Deprecated // Plans are to remove allowing utf8-mode
    public NatsMessage(String subject, String replyTo, byte[] data, boolean utf8mode) {
        this(subject, replyTo, null, data);
        this.utf8mode = utf8mode;
    }

    @Deprecated // Plans are to remove allowing utf8-mode
    public NatsMessage(String subject, String replyTo, Headers headers, byte[] data, boolean utf8mode) {
        this(subject, replyTo, headers, data);
        this.utf8mode = utf8mode;
    }

    public NatsMessage(String subject, String replyTo, byte[] data) {
        this(subject, replyTo, null, data);
    }

    public NatsMessage(String subject, String replyTo, Headers headers, byte[] data) {
        this(data);
        this.subject = validateSubject(subject, true);
        this.replyTo = validateReplyTo(replyTo, false);
        this.headers = headers;
        this.utf8mode = false;
        finishConstruct();
    }

    public NatsMessage(Message message) {
        this(message.getData());
        this.subject = message.getSubject();
        this.replyTo = message.getReplyTo();
        this.headers = message.getHeaders();
        this.utf8mode = message.isUtf8mode();
        finishConstruct();
    }

    protected void finishConstruct() {
        int replyToLen = replyTo == null ? 0 : replyTo.length();

        if (headers != null && !headers.isEmpty()) {
            headerLen = headers.serializedLength();
        }
        else {
            headerLen = 0;
        }
        int headerAndDataLen = headerLen + dataLen;

        // initialize the builder with a reasonable length, preventing resize in 99.9% of the cases
        // 32 for misc + subject length doubled in case of utf8 mode + replyToLen + totLen (headerLen + dataLen)
        ByteArrayBuilder bab = new ByteArrayBuilder(32 + (subject.length() * 2) + replyToLen + headerAndDataLen);

        // protocol come first
        if (headerLen > 0) {
            bab.append(HPUB_SP_BYTES, 0, HPUB_SP_BYTES_LEN);
        }
        else {
            bab.append(PUB_SP_BYTES, 0, PUB_SP_BYTES_LEN);
        }

        // next comes the subject
        bab.append(subject.getBytes(UTF_8)).append(SP);

        // reply to if it's there
        if (replyToLen > 0) {
            bab.append(replyTo.getBytes(UTF_8)).append(SP);
        }

        // header length if there are headers
        if (headerLen > 0) {
            bab.append(Integer.toString(headerLen).getBytes(US_ASCII)).append(SP);
        }

        // payload length
        bab.append(Integer.toString(headerAndDataLen).getBytes(US_ASCII));

        protocolBab = bab;
        controlLineLength = protocolBab.length() + 2; // One CRLF. This is just how controlLineLength is defined.
        sizeInBytes = controlLineLength + headerAndDataLen + 2; // The 2nd CRLFs
    }

    // ----------------------------------------------------------------------------------------------------
    // Client and Message Internal Methods
    // ----------------------------------------------------------------------------------------------------
    long getSizeInBytes() {
        return sizeInBytes;
    }

    boolean isProtocol() {
        return false; // overridden in NatsMessage.ProtocolMessage
    }

    byte[] getProtocolBytes() {
        return protocolBab.toByteArray();
    }

    int getControlLineLength() {
        return controlLineLength;
    }

    Headers getOrCreateHeaders() {
        if (headers == null) {
            headers = new Headers();
        }
        return headers;
    }

    void setSubscription(NatsSubscription sub) {
        subscription = sub;
    }

    NatsSubscription getNatsSubscription() {
        return subscription;
    }

    // ----------------------------------------------------------------------------------------------------
    // Public Interface Methods
    // ----------------------------------------------------------------------------------------------------
    /**
     * {@inheritDoc}
     */
    @Override
    public String getSID() {
        return sid;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection() {
        return subscription == null ? null : subscription.connection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSubject() {
        return subject;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getReplyTo() {
        return replyTo;
    }

    /**
     * @param destPosition the position index in destination byte array to start
     * @param dest the byte array to write to
     * @return the length of the header
     */
    int copyNotEmptyHeaders(int destPosition, byte[] dest) {
        if (headers != null && !headers.isEmpty()) {
            return headers.serializeToArray(destPosition, dest);
        }
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasHeaders() {
        return headers != null && !headers.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Headers getHeaders() {
        return headers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isStatusMessage() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Status getStatus() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getData() {
        return data;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isUtf8mode() {
        return utf8mode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Subscription getSubscription() {
        return subscription;
    }

    @Override
    public AckType lastAck() {
        return lastAck;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ack() {
        // do nothing. faster. saves checking whether a message is jetstream or not
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ackSync(Duration d) throws InterruptedException, TimeoutException {
        // do nothing. faster. saves checking whether a message is jetstream or not
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void nak() {
        // do nothing. faster. saves checking whether a message is jetstream or not
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void nakWithDelay(Duration nakDelay) {
        // do nothing. faster. saves checking whether a message is jetstream or not
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void nakWithDelay(long nakDelayMillis) {
        // do nothing. faster. saves checking whether a message is jetstream or not
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void inProgress() {
        // do nothing. faster. saves checking whether a message is jetstream or not
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void term() {
        // do nothing. faster. saves checking whether a message is jetstream or not
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NatsJetStreamMetaData metaData() {
        throw new IllegalStateException(NOT_A_JET_STREAM_MESSAGE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isJetStream() {
        return false;  // overridden in NatsJetStreamMessage
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long consumeByteCount() {
        return subject == null ? 0 : subject.length()
            + headerLen
            + dataLen
            + (replyTo == null ? 0 : replyTo.length());
    }

    @Override
    public String toString() {
        if (subject == null) {
            return "NatsMessage | " + protocolBytesToString();
        }
        return "NatsMessage |" + subject + "|" + replyToString() + "|" + dataToString() + "|";
    }

    String toDetailString() {
        return "NatsMessage:" +
                "\n  subject='" + subject + '\'' +
                "\n  replyTo='" + replyToString() + '\'' +
                "\n  data=" + dataToString() +
                "\n  utf8mode=" + utf8mode +
                "\n  headers=" + headersToString() +
                "\n  sid='" + sid + '\'' +
                "\n  protocolBytes=" + protocolBytesToString() +
                "\n  sizeInBytes=" + sizeInBytes +
                "\n  headerLen=" + headerLen +
                "\n  dataLen=" + dataLen +
                "\n  subscription=" + subscription +
                "\n  next=" + nextToString();

    }

    private String headersToString() {
        return hasHeaders() ? new String(headers.getSerialized(), US_ASCII).replace("\r", "+").replace("\n", "+") : "";
    }

    private String dataToString() {

        if (data.length == 0) {
            return "<no data>";
        }
        String s = new String(data, UTF_8);
        int at = s.indexOf("io.nats.jetstream.api");
        if (at == -1) {
            return s.length() > 27 ? s.substring(0, 27) + "..." : s;
        }
        int at2 = s.indexOf('"', at);
        return s.substring(at, at2);
    }

    private String replyToString() {
        return replyTo == null ? "<no reply>" : replyTo;
    }

    private String protocolBytesToString() {
        return protocolBab == null ? null : protocolBab.toString();
    }

    private String nextToString() {
        return next == null ? "No" : "Yes";
    }

    // ----------------------------------------------------------------------------------------------------
    // Standard Builder
    // ----------------------------------------------------------------------------------------------------
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The builder is for building normal publish/request messages,
     * as an option for client use developers instead of the normal constructor
     */
    public static class Builder {
        private String subject;
        private String replyTo;
        private Headers headers;
        private byte[] data;
        private boolean utf8mode;

        /**
         * Set the subject
         *
         * @param subject the subject
         * @return the builder
         */
        public Builder subject(final String subject) {
            this.subject = subject;
            return this;
        }

        /**
         * Set the reply to
         *
         * @param replyTo the reply to
         * @return the builder
         */
        public Builder replyTo(final String replyTo) {
            this.replyTo = replyTo;
            return this;
        }

        /**
         * Set the headers
         *
         * @param headers the headers
         * @return the builder
         */
        public Builder headers(final Headers headers) {
            this.headers = headers;
            return this;
        }

        /**
         * Set the data from a string converting using the
         * charset StandardCharsets.UTF_8
         *
         * @param data    the data string
         * @return the builder
         */
        public Builder data(final String data) {
            if (data != null) {
                this.data = data.getBytes(StandardCharsets.UTF_8);
            }
            return this;
        }

        /**
         * Set the data from a string
         *
         * @param data    the data string
         * @param charset the charset, for example {@code StandardCharsets.UTF_8}
         * @return the builder
         */
        public Builder data(final String data, final Charset charset) {
            this.data = data.getBytes(charset);
            return this;
        }

        /**
         * Set the data from a byte array. null data changed to empty byte array
         *
         * @param data the data
         * @return the builder
         */
        public Builder data(final byte[] data) {
            this.data = data;
            return this;
        }

        /**
         * Set if the subject should be treated as utf
         * @deprecated Code is just always treating as utf8
         * @param utf8mode true if utf8 mode for subject
         * @return the builder
         */
        @Deprecated
        public Builder utf8mode(final boolean utf8mode) {
            this.utf8mode = utf8mode;
            return this;
        }

        /**
         * Build the {@code NatsMessage} object
         *
         * @return the {@code NatsMessage}
         */
        public NatsMessage build() {
            return new NatsMessage(subject, replyTo, headers, data, utf8mode);
        }
    }
}
