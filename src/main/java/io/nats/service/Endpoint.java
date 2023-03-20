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

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonValue;

import java.util.Map;
import java.util.Objects;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonValueUtils.*;
import static io.nats.client.support.Validator.validateIsRestrictedTerm;
import static io.nats.client.support.Validator.validateSubject;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class Endpoint implements JsonSerializable {
    private final String name;
    private final String subject;
    private final Schema schema;
    private final Map<String, String> metadata;

    public Endpoint(String name, String subject, Schema schema) {
        this(name, subject, schema, null, true);
    }

    public Endpoint(String name) {
        this(name, null, null, null, true);
    }

    public Endpoint(String name, String subject) {
        this(name, subject, null, null, true);
    }

    public Endpoint(String name, String subject, String schemaRequest, String schemaResponse) {
        this(name, subject, Schema.optionalInstance(schemaRequest, schemaResponse), null, true);
    }

    // internal use constructors
    Endpoint(String name, String subject, Schema schema, Map<String, String> metadata, boolean validate) {
        if (validate) {
            this.name = validateIsRestrictedTerm(name, "Endpoint Name", true);
            if (subject == null) {
                this.subject = this.name;
            }
            else {
                this.subject = validateSubject(subject, "Endpoint Subject", false, false);
            }
        }
        else {
            this.name = name;
            this.subject = subject;
        }
        this.schema = schema;
        this.metadata = metadata == null || metadata.size() == 0 ? null : metadata;
    }

    Endpoint(JsonValue vEndpoint) {
        name = readString(vEndpoint, NAME);
        subject = readString(vEndpoint, SUBJECT);
        schema = Schema.optionalInstance(readValue(vEndpoint, SCHEMA));
        metadata = readStringStringMap(vEndpoint, METADATA);
    }

    Endpoint(Builder b) {
        this(b.name, b.subject, Schema.optionalInstance(b.schemaRequest, b.schemaResponse), b.metadata, true);
    }

    @Override
    public String toJson() {
        StringBuilder sb = JsonUtils.beginJson();
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, SUBJECT, subject);
        JsonUtils.addField(sb, SCHEMA, schema);
        JsonUtils.addField(sb, METADATA, metadata);
        return endJson(sb).toString();
    }

    @Override
    public String toString() {
        return JsonUtils.toKey(getClass()) + toJson();
    }

    public String getName() {
        return name;
    }

    public String getSubject() {
        return subject;
    }

    public Schema getSchema() {
        return schema;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private String subject;
        private String schemaRequest;
        private String schemaResponse;
        private Map<String, String> metadata;

        public Builder endpoint(Endpoint endpoint) {
            name = endpoint.getName();
            subject = endpoint.getSubject();
            Schema s = endpoint.getSchema();
            if (s == null) {
                schemaRequest = null;
                schemaResponse = null;
            }
            else {
                schemaRequest = s.getRequest();
                schemaResponse = s.getResponse();
            }
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder subject(String subject) {
            this.subject = subject;
            return this;
        }

        public Builder schemaRequest(String schemaRequest) {
            this.schemaRequest = schemaRequest;
            return this;
        }

        public Builder schemaResponse(String schemaResponse) {
            this.schemaResponse = schemaResponse;
            return this;
        }

        public Builder schema(Schema schema) {
            if (schema == null) {
                schemaRequest = null;
                schemaResponse = null;
            }
            else {
                schemaRequest = schema.getRequest();
                schemaResponse = schema.getResponse();
            }
            return this;
        }

        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Endpoint build() {
            return new Endpoint(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Endpoint that = (Endpoint) o;

        if (!Objects.equals(name, that.name)) return false;
        if (!Objects.equals(subject, that.subject)) return false;
        if (!Objects.equals(schema, that.schema)) return false;
        return JsonUtils.mapEquals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (subject != null ? subject.hashCode() : 0);
        result = 31 * result + (schema != null ? schema.hashCode() : 0);
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        return result;
    }
}
