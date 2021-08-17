// Copyright 2021 The NATS Authors
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

/**
 * Use the KeyValueEntryHandler interface to define the listener
 * for KvEntry. Each Dispatcher can have a single message handler, although the
 * handler can use the incoming message's subject to branch for the actual work.
 */
public interface KvEntryHandler {

    /**
     * Called to deliver an entry to the handler.
     *
     * @param kve the received Message
     */
    void handle(KvEntry kve);
}
