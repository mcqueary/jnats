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

package io.nats.client.jetstream;

import io.nats.client.support.JsonUtils;

import java.time.ZonedDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamState {
    private long msgs;
    private long bytes;
    private long firstSeq;
    private ZonedDateTime firstTime;
    private long lastSeq;
    private ZonedDateTime lastTime;
    private long consumers;

    private static final String msgsField = "messages";
    private static final String bytesField = "bytes";
    private static final String firstSeqField = "first_seq";
    private static final String firstTimeField = "first_ts";
    private static final String lastSeqField = "last_seq";
    private static final String lastTimeField = "last_ts";
    private static final String consumersField = "consumer_count";

    private static final Pattern msgsRE = JsonUtils.buildPattern(msgsField, JsonUtils.FieldType.jsonNumber);
    private static final Pattern bytesRE = JsonUtils.buildPattern(bytesField, JsonUtils.FieldType.jsonNumber);
    private static final Pattern firstSeqRE = JsonUtils.buildPattern(firstSeqField, JsonUtils.FieldType.jsonNumber);
    private static final Pattern firstTimeRE = JsonUtils.buildPattern(firstTimeField, JsonUtils.FieldType.jsonString);
    private static final Pattern lastSeqRE = JsonUtils.buildPattern(lastSeqField, JsonUtils.FieldType.jsonNumber);
    private static final Pattern lastTimeRE = JsonUtils.buildPattern(lastTimeField, JsonUtils.FieldType.jsonString);
    private static final Pattern consumersRE = JsonUtils.buildPattern(consumersField, JsonUtils.FieldType.jsonNumber);

    public StreamState(String json) {
        Matcher m = msgsRE.matcher(json);
        if (m.find()) {
            this.msgs = Long.parseLong(m.group(1));
        }

        m = bytesRE.matcher(json);
        if (m.find()) {
            this.bytes = Long.parseLong(m.group(1));
        }

        m = firstSeqRE.matcher(json);
        if (m.find()) {
            this.firstSeq = Long.parseLong(m.group(1));
        }

        m = firstTimeRE.matcher(json);
        if (m.find()) {
            this.firstTime = JsonUtils.parseDateTime(m.group(1));
        }

        m = lastSeqRE.matcher(json);
        if (m.find()) {
            this.lastSeq = Long.parseLong(m.group(1));
        }

        m = lastTimeRE.matcher(json);
        if (m.find()) {
            this.lastTime = JsonUtils.parseDateTime(m.group(1));
        }

        m = consumersRE.matcher(json);
        if (m.find()) {
            this.consumers = Long.parseLong(m.group(1));
        }
    }

    /**
     * Gets the message count of the stream.
     *
     * @return the message count
     */
    public long getMsgCount() {
        return msgs;
    }

    /**
     * Gets the byte count of the stream.
     *
     * @return the byte count
     */
    public long getByteCount() {
        return bytes;
    }

    /**
     * Gets the first sequence number of the stream.
     *
     * @return a sequence number
     */
    public long getFirstSequence() {
        return firstSeq;
    }

    /**
     * Gets the time stamp of the first message in the stream
     *
     * @return the first time
     */
    public ZonedDateTime getFirstTime() {
        return firstTime;
    }

    /**
     * Gets the last sequence of a message in the stream
     *
     * @return a sequence number
     */
    public long getLastSequence() {
        return lastSeq;
    }

    /**
     * Gets the time stamp of the last message in the stream
     *
     * @return the first time
     */
    public ZonedDateTime getLastTime() {
        return lastTime;
    }

    /**
     * Gets the number of consumers attached to the stream.
     *
     * @return the consumer count
     */
    public long getConsumerCount() {
        return consumers;
    }
}
