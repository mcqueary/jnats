// Copyright 2020 The NATS Authors
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

package io.nats.client.api;

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.support.*;

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.addRawJson;
import static io.nats.client.support.JsonValueUtils.*;
import static io.nats.client.support.NatsJetStreamConstants.*;

/**
 * The MessageInfo class contains information about a JetStream message.
 */
public class MessageInfo extends ApiResponse<MessageInfo> {

    private final String subject;
    private final long seq;
    private final byte[] data;
    private final ZonedDateTime time;
    private final Headers headers;
    private final String stream;
    private final long lastSeq;
    private final long numPending;
    private final Status status;

    /**
     * Create a Message Info
     * @deprecated This signature was public for unit testing but is no longer used.
     * @param msg the message
     */
    @Deprecated
    public MessageInfo(Message msg) {
        this(msg, null, null, false);
    }

    /**
     * Create a Message Info
     * @param msg the message
     * @param streamName the stream name if known
     * @param parseDirect true if the object is being created from a direct api call instead of get message
     */
    public MessageInfo(Message msg, String streamName, boolean parseDirect) {
        this(msg, null, streamName, parseDirect);
    }

    /**
     * Create a Message Info
     * @param status     the status
     * @param streamName the stream name if known
     */
    public MessageInfo(Status status, String streamName) {
        this(null, status, streamName, false);
    }

    private MessageInfo(Message msg, Status status, String streamName, boolean parseDirect) {
        super(parseDirect ? null : msg);

        // working vars because the object vars are final
        String _subject = null;
        long _seq = -1;
        byte[] _data = null;
        ZonedDateTime _time = null;
        Headers _headers = null;
        String _stream = null;
        long _lastSeq = -1;
        long _numPending = -1;
        Status _status = null;

        if (status != null) {
            _status = status;
            _stream = streamName;
        }
        else if (parseDirect) {
            Headers msgHeaders = msg.getHeaders();
            _subject = msgHeaders.getLast(NATS_SUBJECT);
            _data = msg.getData();
            _seq = Long.parseLong(msgHeaders.getLast(NATS_SEQUENCE));
            _time = DateTimeUtils.parseDateTime(msgHeaders.getLast(NATS_TIMESTAMP));
            _stream = msgHeaders.getLast(NATS_STREAM);
            String tempLastSeq = msgHeaders.getLast(NATS_LAST_SEQUENCE);
            if (tempLastSeq != null) {
                _lastSeq = JsonUtils.safeParseLong(tempLastSeq, -1);
            }
            String tempNumPending = msgHeaders.getLast(NATS_NUM_PENDING);
            if (tempNumPending != null) {
                _numPending = Long.parseLong(tempNumPending) - 1;
            }

            // these are control headers, not real headers so don't give them to the user. Must be done last
            _headers = new Headers(msgHeaders, true, MESSAGE_INFO_HEADERS);
        }
        else if (!hasError()){
            JsonValue mjv = readValue(jv, MESSAGE);
            _subject = readString(mjv, SUBJECT);
            _data = readBase64(mjv, DATA);
            _seq = readLong(mjv, SEQ, 0);
            _time = readDate(mjv, TIME);
            byte[] hdrBytes = readBase64(mjv, HDRS);
            _headers = hdrBytes == null ? null : new IncomingHeadersProcessor(hdrBytes).getHeaders();
            _stream = streamName;
        }

        this.subject = _subject;
        this.data = _data;
        this.seq = _seq;
        this.time = _time;
        this.headers = _headers;
        this.stream = _stream;
        this.lastSeq = _lastSeq;
        this.numPending = _numPending;
        this.status = _status;
    }

    /**
     * Get the message subject
     * @return the subject
     */
    public String getSubject() {
        return subject;
    }

    /**
     * Get the message sequence
     * @return the sequence number
     */
    public long getSeq() {
        return seq;
    }

    /**
     * Get the message data
     * @return the data bytes
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Get the time the message was received
     * @return the time
     */
    public ZonedDateTime getTime() {
        return time;
    }

    /**
     * Get the headers
     * @return the headers object or null if there were no headers
     */
    public Headers getHeaders() {
        return headers;
    }

    /**
     * Get the name of the stream. Not always set.
     * @return the stream name or null if the name is not known.
     */
    public String getStream() {
        return stream;
    }

    /**
     * Get the sequence number of the last message in the stream. Not always set.
     * @return the last sequence or -1 if the value is not known.
     */
    public long getLastSeq() {
        return lastSeq;
    }

    /**
     * Amount of pending messages that can be requested with a subsequent batch request.
     * @return number of pending messages
     */
    public long getNumPending() {
        return numPending;
    }

    /**
     * Get the Status object. Null if this MessageInfo is not a Status.
     * @return the status object
     */
    public Status getStatus() {
        return status;
    }

    /**
     * Whether this MessageInfo is a regular message
     * @return true if the MessageInfo is a regular message
     */
    public boolean isMessage() {
        return status == null && !hasError();
    }

    /**
     * Whether this MessageInfo is a status message
     * @return true if this MessageInfo is a status message
     */
    public boolean isStatus() {
        return status != null;
    }

    /**
     * Whether this MessageInfo is a status message and is a direct EOB status
     * @return true if this MessageInfo is a status message and is a direct EOB status
     */
    public boolean isEobStatus() {
        return status != null && status.isEob();
    }

    /**
     * Whether this MessageInfo is a status message and is an error status
     * @return true if this MessageInfo is a status message and is an error status
     */
    public boolean isErrorStatus() {
        return status != null && !status.isEob();
    }

    @Override
    public String toString() {
        StringBuilder sb = JsonUtils.beginJsonPrefixed("\"MessageInfo\":");
        if (status != null) {
            JsonUtils.addField(sb, "status_code", status.getCode());
            JsonUtils.addField(sb, "status_message", status.getMessage());
        }
        else if (hasError()) {
            JsonUtils.addField(sb, ERROR, getError());
        }
        else {
            JsonUtils.addField(sb, SEQ, seq);
            JsonUtils.addField(sb, LAST_SEQ, lastSeq);
            JsonUtils.addFieldWhenGteMinusOne(sb, NUM_PENDING, numPending);
            JsonUtils.addField(sb, STREAM, stream);
            JsonUtils.addField(sb, SUBJECT, subject);
            JsonUtils.addField(sb, TIME, time);
            if (data == null) {
                addRawJson(sb, DATA, "null");
            }
            else {
                JsonUtils.addField(sb, "data_length", data.length);
            }
            JsonUtils.addField(sb, HDRS, headers);
        }
        return JsonUtils.endJson(sb).toString();
    }
}
