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
import io.nats.client.support.JsonValueUtils;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonValueUtils.*;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class EndpointResponse implements JsonSerializable {
    private final String name;
    private final String subject;
    private final long numRequests;
    private final long numErrors;
    private final long processingTime;
    private final long averageProcessingTime;
    private final String lastError;
    private final JsonValue data;
    private final ZonedDateTime started;

    static List<EndpointResponse> listOf(JsonValue vEndpointStats) {
        return JsonValueUtils.listOf(vEndpointStats, EndpointResponse::new);
    }

    // This is for stats
    EndpointResponse(String name, String subject, long numRequests, long numErrors, long processingTime, String lastError, JsonValue data, ZonedDateTime started) {
        this.name = name;
        this.subject = subject;
        this.numRequests = numRequests;
        this.numErrors = numErrors;
        this.processingTime = processingTime;
        this.averageProcessingTime = numRequests < 1 ? 0 : processingTime / numRequests;
        this.lastError = lastError;
        this.data = data;
        this.started = started;
    }

    // This is for schema
    EndpointResponse(String name, String subject) {
        this.name = name;
        this.subject = subject;
        this.numRequests = 0;
        this.numErrors = 0;
        this.processingTime = 0;
        this.averageProcessingTime = 0;
        this.lastError = null;
        this.data = null;
        this.started = null;
    }

    EndpointResponse(JsonValue vEndpointResponse) {
        name = readString(vEndpointResponse, NAME);
        subject = readString(vEndpointResponse, SUBJECT);
        numRequests = readLong(vEndpointResponse, NUM_REQUESTS, 0);
        numErrors = readLong(vEndpointResponse, NUM_ERRORS, 0);
        processingTime = readLong(vEndpointResponse, PROCESSING_TIME, 0);
        averageProcessingTime = readLong(vEndpointResponse, AVERAGE_PROCESSING_TIME, 0);
        lastError = readString(vEndpointResponse, LAST_ERROR);
        data = readValue(vEndpointResponse, DATA);
        started = readDate(vEndpointResponse, STARTED);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, SUBJECT, subject);
        JsonUtils.addFieldWhenGtZero(sb, NUM_REQUESTS, numRequests);
        JsonUtils.addFieldWhenGtZero(sb, NUM_ERRORS, numErrors);
        JsonUtils.addFieldWhenGtZero(sb, PROCESSING_TIME, processingTime);
        JsonUtils.addFieldWhenGtZero(sb, AVERAGE_PROCESSING_TIME, averageProcessingTime);
        JsonUtils.addField(sb, LAST_ERROR, lastError);
        JsonUtils.addField(sb, DATA, data);
        JsonUtils.addField(sb, STARTED, started);
        return endJson(sb).toString();
    }

    public String getName() {
        return name;
    }

    public String getSubject() {
        return subject;
    }

    public long getNumRequests() {
        return numRequests;
    }

    public long getNumErrors() {
        return numErrors;
    }

    public long getProcessingTime() {
        return processingTime;
    }

    public long getAverageProcessingTime() {
        return averageProcessingTime;
    }

    public String getLastError() {
        return lastError;
    }

    public JsonValue getData() {
        return data;
    }

    public ZonedDateTime getStarted() {
        return started;
    }

    @Override
    public String toString() {
        return JsonUtils.toKey(getClass()) + toJson();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EndpointResponse that = (EndpointResponse) o;

        if (numRequests != that.numRequests) return false;
        if (numErrors != that.numErrors) return false;
        if (processingTime != that.processingTime) return false;
        if (averageProcessingTime != that.averageProcessingTime) return false;
        if (!Objects.equals(name, that.name)) return false;
        if (!Objects.equals(subject, that.subject)) return false;
        if (!Objects.equals(lastError, that.lastError)) return false;
        if (!Objects.equals(data, that.data)) return false;
        return Objects.equals(started, that.started);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (subject != null ? subject.hashCode() : 0);
        result = 31 * result + (int) (numRequests ^ (numRequests >>> 32));
        result = 31 * result + (int) (numErrors ^ (numErrors >>> 32));
        result = 31 * result + (int) (processingTime ^ (processingTime >>> 32));
        result = 31 * result + (int) (averageProcessingTime ^ (averageProcessingTime >>> 32));
        result = 31 * result + (lastError != null ? lastError.hashCode() : 0);
        result = 31 * result + (data != null ? data.hashCode() : 0);
        result = 31 * result + (started != null ? started.hashCode() : 0);
        return result;
    }
}
