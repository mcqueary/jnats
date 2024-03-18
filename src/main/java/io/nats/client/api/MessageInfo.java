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
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.IncomingHeadersProcessor;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonValue;

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.addRawJson;
import static io.nats.client.support.JsonValueUtils.*;
import static io.nats.client.support.NatsJetStreamConstants.*;

/**
 * The MessageInfo class contains information about a JetStream message.
 */
public class MessageInfo extends ApiResponse<MessageInfo> {

    private final boolean direct;
    private final String subject;
    private final long seq;
    private final byte[] data;
    private final ZonedDateTime time;
    private final Headers headers;
    private final String stream;
    private final long lastSeq;

    /**
     * Create a Message Info
     * @deprecated This signature was public for unit testing but is no longer used.
     * @param msg the message
     */
    @Deprecated
    public MessageInfo(Message msg) {
        this(msg, null, false);
    }

    /**
     * Create a Message Info
     * This signature is public for testing purposes and is not intended to be used externally.
     * @param msg the message
     * @param streamName the stream name if known
     * @param direct true if the object is being created from a get direct api call instead of the standard get message
     */
    public MessageInfo(Message msg, String streamName, boolean direct) {
        super(direct ? null : msg);

        this.direct = direct;

        if (direct) {
            Headers msgHeaders = msg.getHeaders();
            this.subject = msgHeaders.getLast(NATS_SUBJECT);
            this.data = msg.getData();
            seq = Long.parseLong(msgHeaders.getLast(NATS_SEQUENCE));
            time = DateTimeUtils.parseDateTime(msgHeaders.getLast(NATS_TIMESTAMP));
            stream = msgHeaders.getLast(NATS_STREAM);
            String temp = msgHeaders.getLast(NATS_LAST_SEQUENCE);
            if (temp == null) {
                lastSeq = -1;
            }
            else {
                lastSeq = JsonUtils.safeParseLong(temp, -1);
            }
            // these are control headers, not real headers so don't give them to the user.
            headers = new Headers(msgHeaders, true, MESSAGE_INFO_HEADERS);
        }
        else if (hasError()) {
            subject = null;
            data = null;
            seq = -1;
            time = null;
            headers = null;
            stream = null;
            lastSeq = -1;
        }
        else {
            JsonValue mjv = readValue(jv, MESSAGE);
            subject = readString(mjv, SUBJECT);
            data = readBase64(mjv, DATA);
            seq = readLong(mjv, SEQ, 0);
            time = readDate(mjv, TIME);
            byte[] hdrBytes = readBase64(mjv, HDRS);
            headers = hdrBytes == null ? null : new IncomingHeadersProcessor(hdrBytes).getHeaders();
            stream = streamName;
            lastSeq = -1;
        }
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

    @Override
    public String toString() {
        StringBuilder sb = JsonUtils.beginJsonPrefixed("\"MessageInfo\":");
        JsonUtils.addField(sb, "direct", direct);
        JsonUtils.addField(sb, "error", getError());
        JsonUtils.addField(sb, SUBJECT, subject);
        JsonUtils.addField(sb, SEQ, seq);
        if (data == null) {
            addRawJson(sb, DATA, "null");
        }
        else {
            JsonUtils.addField(sb, "data_length", data.length);
        }
        JsonUtils.addField(sb, TIME, time);
        JsonUtils.addField(sb, STREAM, stream);
        JsonUtils.addField(sb, "last_seq", lastSeq);
        JsonUtils.addField(sb, SUBJECT, subject);
        JsonUtils.addField(sb, HDRS, headers);
        return JsonUtils.endJson(sb).toString();
    }
}
