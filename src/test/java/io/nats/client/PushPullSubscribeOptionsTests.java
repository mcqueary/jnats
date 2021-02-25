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

package io.nats.client;

import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class PushPullSubscribeOptionsTests extends TestBase {

    @Test
    public void testPushAffirmative() {
        PushSubscribeOptions.Builder builder = PushSubscribeOptions.builder();

        PushSubscribeOptions so = builder.build();

        // starts out all null which is fine
        assertNull(so.getStream());
        assertNull(so.getDurable());
        assertNull(so.getDeliverSubject());

        so = builder.stream(STREAM).durable(DURABLE).deliverSubject(DELIVER).build();

        assertEquals(STREAM, so.getStream());
        assertEquals(DURABLE, so.getDurable());
        assertEquals(DELIVER, so.getDeliverSubject());

        // demonstrate that you can clear the builder
        so = builder.stream(null).deliverSubject(null).durable(null).build();
        assertNull(so.getStream());
        assertNull(so.getDurable());
        assertNull(so.getDeliverSubject());

        assertNotNull(so.toString()); // COVERAGE
    }

    @Test
    public void testPullAffirmative() {
        PullSubscribeOptions.Builder builder = PullSubscribeOptions.builder()
                .stream(STREAM)
                .durable(DURABLE);

        PullSubscribeOptions so = builder.build();
        assertEquals(STREAM, so.getStream());
        assertEquals(DURABLE, so.getDurable());

        assertNotNull(so.toString()); // COVERAGE
    }

    @Test
    public void testPushFieldValidation() {
        PushSubscribeOptions.Builder builder = PushSubscribeOptions.builder();
        assertThrows(IllegalArgumentException.class, () -> builder.stream(HAS_DOT).build());
        assertThrows(IllegalArgumentException.class, () -> builder.durable(HAS_DOT).build());

        ConsumerConfiguration ccBadDur = ConsumerConfiguration.builder().durable(HAS_DOT).build();
        assertThrows(IllegalArgumentException.class, () -> builder.configuration(ccBadDur).build());

        // durable directly
        PushSubscribeOptions.builder().durable(DURABLE).build();

        // in configuration
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(DURABLE).build();
        PushSubscribeOptions.builder().configuration(cc).build();
    }

    @Test
    public void testPullValidation() {
        PullSubscribeOptions.Builder builder1 = PullSubscribeOptions.builder();
        assertThrows(IllegalArgumentException.class, () -> builder1.stream(HAS_DOT).build());
        assertThrows(IllegalArgumentException.class, () -> builder1.durable(HAS_DOT).build());

        ConsumerConfiguration ccBadDur = ConsumerConfiguration.builder().durable(HAS_DOT).build();
        assertThrows(IllegalArgumentException.class, () -> builder1.configuration(ccBadDur).build());

        // durable required direct or in configuration
        PullSubscribeOptions.Builder builder2 = PullSubscribeOptions.builder();

        assertThrows(IllegalArgumentException.class, builder2::build);

        ConsumerConfiguration ccNoDur = ConsumerConfiguration.builder().build();
        assertThrows(IllegalArgumentException.class, () -> builder2.configuration(ccNoDur).build());

        // durable directly
        PullSubscribeOptions.builder().durable(DURABLE).build();

        // in configuration
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(DURABLE).build();
        PullSubscribeOptions.builder().configuration(cc).build();
    }
}
