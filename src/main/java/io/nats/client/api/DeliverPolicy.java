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

import java.util.HashMap;
import java.util.Map;

/**
 * The delivery policy for this consumer.
 */
public enum DeliverPolicy {
    All("all"),
    Last("last"),
    New("new"),
    ByStartSequence("by_start_sequence"),
    ByStartTime("by_start_time"),
    LastPerSubject("last_per_subject");

    private String policy;

    DeliverPolicy(String p) {
        policy = p;
    }

    @Override
    public String toString() {
        return policy;
    }

    private static final Map<String, DeliverPolicy> strEnumHash = new HashMap<>();

    static {
        for (DeliverPolicy env : DeliverPolicy.values()) {
            strEnumHash.put(env.toString(), env);
        }
    }

    public static DeliverPolicy get(String value) {
        return strEnumHash.get(value);
    }
}
