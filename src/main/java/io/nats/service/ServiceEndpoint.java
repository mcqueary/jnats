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

import io.nats.client.Dispatcher;
import io.nats.client.support.JsonValue;
import io.nats.client.support.Validator;

import java.util.function.Supplier;

import static io.nats.client.support.NatsConstants.DOT;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class ServiceEndpoint {
    private final Group group;
    private final Endpoint endpoint;
    private final ServiceMessageHandler handler;
    private final Dispatcher dispatcher;
    private final Supplier<JsonValue> statsDataSupplier;

    private ServiceEndpoint(Builder b, Endpoint e) {
        this.group = b.group;
        this.endpoint = e;
        this.handler = b.handler;
        this.dispatcher = b.dispatcher;
        this.statsDataSupplier = b.statsDataSupplier;
    }

    // internal use constructor
    ServiceEndpoint(Endpoint endpoint, ServiceMessageHandler handler, Dispatcher dispatcher) {
        this.group = null;
        this.endpoint = endpoint;
        this.handler = handler;
        this.dispatcher = dispatcher;
        this.statsDataSupplier = null;
    }

    public String getName() {
        return endpoint.getName();
    }

    public String getSubject() {
        return group == null ? endpoint.getSubject() : group.getSubject() + DOT + endpoint.getSubject();
    }

    public Group getGroup() {
        return group;
    }

    public Endpoint getEndpoint() {
        return endpoint;
    }

    public ServiceMessageHandler getHandler() {
        return handler;
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    public Supplier<JsonValue> getStatsDataSupplier() {
        return statsDataSupplier;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Group group;
        private ServiceMessageHandler handler;
        private Dispatcher dispatcher;
        private Supplier<JsonValue> statsDataSupplier;
        private final Endpoint.Builder endpointBuilder = Endpoint.builder();

        public Builder group(Group group) {
            this.group = group;
            return this;
        }

        public Builder endpoint(Endpoint endpoint) {
            endpointBuilder.endpoint(endpoint);
            return this;
        }

        public Builder endpointName(String name) {
            endpointBuilder.name(name);
            return this;
        }

        public Builder endpointSubject(String subject) {
            endpointBuilder.subject(subject);
            return this;
        }

        public Builder handler(ServiceMessageHandler handler) {
            this.handler = handler;
            return this;
        }

        public Builder dispatcher(Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
            return this;
        }

        public Builder statsDataSupplier(Supplier<JsonValue> statsDataSupplier) {
            this.statsDataSupplier = statsDataSupplier;
            return this;
        }

        public ServiceEndpoint build() {
            Endpoint endpoint = endpointBuilder.build();
            Validator.required(handler, "Message Handler");
            return new ServiceEndpoint(this, endpoint);
        }
    }
}
