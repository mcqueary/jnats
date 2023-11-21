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

package io.nats.compatibility;

public enum Kind {
    COMMAND("command"),
    RESULT("result");

    public final String name;

    Kind(String name) {
        this.name = name;
    }

    public static Kind instance(String text) {
        for (Kind os : Kind.values()) {
            if (os.name.equals(text)) {
                return os;
            }
        }
        System.err.println("Unknown consumerKind: " + text);
        System.exit(-7);
        return null;
    }

    @Override
    public String toString() {
        return name;
    }
}
