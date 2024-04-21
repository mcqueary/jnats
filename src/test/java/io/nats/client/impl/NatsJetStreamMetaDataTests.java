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

package io.nats.client.impl;

import io.nats.client.Message;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class NatsJetStreamMetaDataTests extends JetStreamTestBase {

    @Test
    public void testMetaData() {
        Message msg = getTestMessage(TestMetaV0);
        NatsJetStreamMetaData meta = msg.metaData(); // first time, coverage lazy check is null
        assertNotNull(msg.metaData()); // 2nd time, coverage lazy check is not null
        assertNotNull(meta.toString()); // COVERAGE toString

        validateMeta(false, false, getTestMessage(TestMetaV0).metaData());
        validateMeta(true, false, getTestMessage(TestMetaV1).metaData());
        validateMeta(true, true, getTestMessage(TestMetaV2).metaData());
        validateMeta(true, true, getTestMessage(TestMetaVFuture).metaData());

        assertThrows(IllegalArgumentException.class, () -> getTestMessage(InvalidMetaLt8Tokens).metaData());
        assertThrows(IllegalArgumentException.class, () -> getTestMessage(InvalidMeta10Tokens).metaData());
        assertThrows(IllegalArgumentException.class, () -> getTestMessage(InvalidMetaData).metaData());

        // InvalidMetaNoAck is actually not even a JS message
        assertThrows(IllegalStateException.class, () -> getTestMessage(InvalidMetaNoAck).metaData());

        // since I can't make a JS message directly, do it indirectly
        NatsMessage nm = getTestMessage(InvalidMetaLt8Tokens);
        nm.replyTo = InvalidMetaNoAck;
        assertThrows(IllegalArgumentException.class, nm::metaData);
    }

    private void validateMeta(boolean hasPending, boolean hasDomainHashToken, NatsJetStreamMetaData meta) {
        assertEquals("test-stream", meta.getStream());
        assertEquals("test-consumer", meta.getConsumer());
        assertEquals(1, meta.deliveredCount());
        assertEquals(2, meta.streamSequence());
        assertEquals(3, meta.consumerSequence());

        ZonedDateTime localTs = meta.timestamp();
        assertEquals(2020, localTs.getYear());
        assertEquals(6, localTs.getMinute());
        assertEquals(113260000, localTs.getNano());
        
        ZonedDateTime utcTs = localTs.withZoneSameInstant(ZoneId.of("UTC"));
        assertEquals(0, utcTs.getHour());

        assertEquals(hasPending ? 4L : -1L, meta.pendingCount());

        if (hasDomainHashToken) {
            assertEquals("v2Domain", meta.getDomain());
            assertEquals("v2Hash", meta.getAccountHash());
        }
        else {
            assertNull(meta.getDomain());
            assertNull(meta.getAccountHash());
        }
    }

    @Test
    public void testInvalidMetaDataConstruction() {
        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(NatsMessage.builder().subject("test").build()));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(getTestMessage("$JS.ACK.not.enough.parts")));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(getTestMessage(TestMetaV0 + ".too.many.parts")));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(getTestMessage("$JS.ZZZ.enough.parts.though.need.three.more")));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(new NatsMessage("sub", null, new byte[0])));
    }
}
