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

import io.nats.client.PullSubscribeOptions;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.support.*;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonValueUtils.*;
import static io.nats.client.support.NatsJetStreamClientError.JsConsumerNameDurableMismatch;
import static io.nats.client.support.Validator.*;

/**
 * The ConsumerConfiguration class specifies the configuration for creating a JetStream consumer on the client and
 * if necessary the server.
 * Options are created using a ConsumerConfiguration.Builder.
 * <p>ConsumerConfiguration is intended to be used with  {@link io.nats.client.JetStreamManagement#createConsumer(String, ConsumerConfiguration) JetStreamManagement.createConsumer()}.
 * <P> By default this will create a <b>pull consumer</b> unless {@link ConsumerConfiguration.Builder#deliverSubject(String) ConsumerConfiguration.Builder.deliverSubject(String) } is set.
 */
public class ConsumerConfiguration implements JsonSerializable {
    @Deprecated
    public static final Duration DURATION_MIN = Duration.ofNanos(1);

    public static final DeliverPolicy DEFAULT_DELIVER_POLICY = DeliverPolicy.All;
    public static final AckPolicy DEFAULT_ACK_POLICY = AckPolicy.Explicit;
    public static final ReplayPolicy DEFAULT_REPLAY_POLICY = ReplayPolicy.Instant;
    public static final PriorityPolicy DEFAULT_PRIORITY_POLICY = PriorityPolicy.None;

    public static final Duration DURATION_UNSET = Duration.ZERO;
    public static final Duration MIN_IDLE_HEARTBEAT = Duration.ofMillis(100);

    public static final int INTEGER_UNSET = -1;
    public static final long LONG_UNSET = -1;
    public static final long ULONG_UNSET = 0;
    public static final long DURATION_UNSET_LONG = 0;
    public static final long DURATION_MIN_LONG = 1;
    public static final int STANDARD_MIN = 0;
    public static final int MAX_DELIVER_MIN = 1;

    public static final long MIN_IDLE_HEARTBEAT_NANOS = MIN_IDLE_HEARTBEAT.toNanos();
    public static final long MIN_IDLE_HEARTBEAT_MILLIS = MIN_IDLE_HEARTBEAT.toMillis();

    protected final DeliverPolicy deliverPolicy;
    protected final AckPolicy ackPolicy;
    protected final ReplayPolicy replayPolicy;
    protected final String description;
    protected final String durable;
    protected final String name;
    protected final String deliverSubject;
    protected final String deliverGroup;
    protected final String sampleFrequency;
    protected final ZonedDateTime startTime;
    protected final Duration ackWait;
    protected final Duration idleHeartbeat;
    protected final Duration maxExpires;
    protected final Duration inactiveThreshold;
    protected final Long startSeq; // server side this is unsigned
    protected final Integer maxDeliver;
    protected final Long rateLimit; // server side this is unsigned
    protected final Integer maxAckPending;
    protected final Integer maxPullWaiting;
    protected final Integer maxBatch;
    protected final Integer maxBytes;
    protected final Integer numReplicas;
    protected final ZonedDateTime pauseUntil;
    protected final Boolean flowControl;
    protected final Boolean headersOnly;
    protected final Boolean memStorage;
    protected final List<Duration> backoff;
    protected final Map<String, String> metadata;
    protected final List<String> filterSubjects;
    protected final List<String> priorityGroups;
    protected final PriorityPolicy priorityPolicy;

    protected ConsumerConfiguration(ConsumerConfiguration cc) {
        this.deliverPolicy = cc.deliverPolicy;
        this.ackPolicy = cc.ackPolicy;
        this.replayPolicy = cc.replayPolicy;
        this.description = cc.description;
        this.durable = cc.durable;
        this.name = cc.name;
        this.deliverSubject = cc.deliverSubject;
        this.deliverGroup = cc.deliverGroup;
        this.sampleFrequency = cc.sampleFrequency;
        this.startTime = cc.startTime;
        this.ackWait = cc.ackWait;
        this.idleHeartbeat = cc.idleHeartbeat;
        this.maxExpires = cc.maxExpires;
        this.inactiveThreshold = cc.inactiveThreshold;
        this.startSeq = cc.startSeq;
        this.maxDeliver = cc.maxDeliver;
        this.rateLimit = cc.rateLimit;
        this.maxAckPending = cc.maxAckPending;
        this.maxPullWaiting = cc.maxPullWaiting;
        this.maxBatch = cc.maxBatch;
        this.maxBytes = cc.maxBytes;
        this.numReplicas = cc.numReplicas;
        this.pauseUntil = cc.pauseUntil;
        this.flowControl = cc.flowControl;
        this.headersOnly = cc.headersOnly;
        this.memStorage = cc.memStorage;
        this.backoff = cc.backoff == null ? null : new ArrayList<>(cc.backoff);
        this.metadata = cc.metadata == null ? null : new HashMap<>(cc.metadata);
        this.filterSubjects = cc.filterSubjects == null ? null : new ArrayList<>(cc.filterSubjects);
        this.priorityGroups = cc.priorityGroups == null ? null : new ArrayList<>(cc.priorityGroups);
        this.priorityPolicy = cc.priorityPolicy;
    }

    // For the builder
    protected ConsumerConfiguration(Builder b)
    {
        this.deliverPolicy = b.deliverPolicy;
        this.ackPolicy = b.ackPolicy;
        this.replayPolicy = b.replayPolicy;

        this.description = b.description;
        this.durable = b.durable;
        this.name = b.name;
        this.startTime = b.startTime;
        this.ackWait = b.ackWait;
        this.sampleFrequency = b.sampleFrequency;
        this.deliverSubject = b.deliverSubject;
        this.deliverGroup = b.deliverGroup;
        this.idleHeartbeat = b.idleHeartbeat;
        this.maxExpires = b.maxExpires;
        this.inactiveThreshold = b.inactiveThreshold;

        this.startSeq = b.startSeq;
        this.maxDeliver = b.maxDeliver;
        this.rateLimit = b.rateLimit;
        this.maxAckPending = b.maxAckPending;
        this.maxPullWaiting = b.maxPullWaiting;
        this.maxBatch = b.maxBatch;
        this.maxBytes = b.maxBytes;
        this.numReplicas = b.numReplicas;
        this.pauseUntil = b.pauseUntil;

        this.flowControl = b.flowControl;
        this.headersOnly = b.headersOnly;
        this.memStorage = b.memStorage;

        this.backoff = b.backoff;
        this.metadata = b.metadata;
        this.filterSubjects = b.filterSubjects;

        this.priorityGroups = b.priorityGroups;
        this.priorityPolicy = b.priorityPolicy;
    }

    /**
     * Returns a JSON representation of this consumer configuration.
     * @return json consumer configuration json string
     */
    @Override
	public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, DESCRIPTION, description);
        JsonUtils.addField(sb, DURABLE_NAME, durable);
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, DELIVER_SUBJECT, deliverSubject);
        JsonUtils.addField(sb, DELIVER_GROUP, deliverGroup);
        JsonUtils.addField(sb, DELIVER_POLICY, GetOrDefault(deliverPolicy).toString());
        JsonUtils.addFieldWhenGtZero(sb, OPT_START_SEQ, startSeq);
        JsonUtils.addField(sb, OPT_START_TIME, startTime);
        JsonUtils.addField(sb, ACK_POLICY, GetOrDefault(ackPolicy).toString());
        JsonUtils.addFieldAsNanos(sb, ACK_WAIT, ackWait);
        JsonUtils.addFieldWhenGtZero(sb, MAX_DELIVER, maxDeliver);
        JsonUtils.addField(sb, MAX_ACK_PENDING, maxAckPending);
        JsonUtils.addField(sb, REPLAY_POLICY, GetOrDefault(replayPolicy).toString());
        JsonUtils.addField(sb, SAMPLE_FREQ, sampleFrequency);
        JsonUtils.addFieldWhenGtZero(sb, RATE_LIMIT_BPS, rateLimit);
        JsonUtils.addFieldAsNanos(sb, IDLE_HEARTBEAT, idleHeartbeat);
        JsonUtils.addFldWhenTrue(sb, FLOW_CONTROL, flowControl);
        JsonUtils.addField(sb, ApiConstants.MAX_WAITING, maxPullWaiting);
        JsonUtils.addFldWhenTrue(sb, HEADERS_ONLY, headersOnly);
        JsonUtils.addField(sb, MAX_BATCH, maxBatch);
        JsonUtils.addField(sb, MAX_BYTES, maxBytes);
        JsonUtils.addFieldAsNanos(sb, MAX_EXPIRES, maxExpires);
        JsonUtils.addFieldAsNanos(sb, INACTIVE_THRESHOLD, inactiveThreshold);
        JsonUtils.addDurations(sb, BACKOFF, backoff);
        JsonUtils.addField(sb, NUM_REPLICAS, numReplicas);
        JsonUtils.addField(sb, PAUSE_UNTIL, pauseUntil);
        JsonUtils.addField(sb, MEM_STORAGE, memStorage);
        JsonUtils.addField(sb, METADATA, metadata);
        if (filterSubjects != null) {
            if (filterSubjects.size() > 1) {
                JsonUtils.addStrings(sb, FILTER_SUBJECTS, filterSubjects);
            }
            else if (filterSubjects.size() == 1) {
                JsonUtils.addField(sb, FILTER_SUBJECT, filterSubjects.get(0));
            }
        }
        JsonUtils.addStrings(sb, PRIORITY_GROUPS, priorityGroups);
        if (priorityPolicy != null && priorityPolicy != DEFAULT_PRIORITY_POLICY) {
            JsonUtils.addField(sb, PRIORITY_POLICY, priorityPolicy.toString());
        }
        return endJson(sb).toString();
    }

    /**
     * Gets the name of the description of this consumer configuration.
     * @return name of the description.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Gets the name of the durable name for this consumer configuration.
     * @return name of the durable.
     */
    public String getDurable() {
        return durable;
    }

    /**
     * Gets the name of the consumer name for this consumer configuration.
     * @return name of the consumer.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the deliver subject of this consumer configuration.
     * @return the deliver subject.
     */
    public String getDeliverSubject() {
        return deliverSubject;
    }

    /**
     * Gets the deliver group of this consumer configuration.
     * @return the deliver group.
     */
    public String getDeliverGroup() {
        return deliverGroup;
    }

    /**
     * Gets the deliver policy of this consumer configuration.
     * @return the deliver policy.
     */
    public DeliverPolicy getDeliverPolicy() {
        return GetOrDefault(deliverPolicy);
    }

    /**
     * Gets the start sequence of this consumer configuration.
     * @return the start sequence.
     */
    public long getStartSequence() {
        return getOrUnsetUlong(startSeq);
    }

    /**
     * Gets the start time of this consumer configuration.
     * @return the start time.
     */
    public ZonedDateTime getStartTime() {
        return startTime;
    }

    /**
     * Gets the acknowledgment policy of this consumer configuration.
     * @return the acknowledgment policy.
     */
    public AckPolicy getAckPolicy() {
        return GetOrDefault(ackPolicy);
    }

    /**
     * Gets the acknowledgment wait of this consumer configuration.
     * @return the acknowledgment wait duration.
     */
    public Duration getAckWait() {
        return ackWait;
    }

    /**
     * Gets the max delivery amount of this consumer configuration.
     * @return the max delivery amount.
     */
    public long getMaxDeliver() {
        return getOrUnset(maxDeliver);
    }

    /**
     * Gets the filter subject of this consumer configuration.
     * With the introduction of multiple filter subjects, this method will
     * return null if there are not exactly one filter subjects
     * @return the first filter subject.
     */
    public String getFilterSubject() {
        return filterSubjects == null || filterSubjects.size() != 1 ? null : filterSubjects.get(0);
    }

    /**
     * Gets the filter subjects as a list. May be null, otherwise won't be empty
     * @return the list
     */
    public List<String> getFilterSubjects() {
        return filterSubjects;
    }

    /**
     * Gets the priority groups as a list. May be null, otherwise won't be empty
     * @return the list
     */
    public List<String> getPriorityGroups() {
        return priorityGroups;
    }

    /**
     * Whether there are multiple filter subjects for this consumer configuration.
     * @return true if there are multiple filter subjects
     */
    public boolean hasMultipleFilterSubjects() {
        return filterSubjects != null && filterSubjects.size() > 1;
    }

    /**
     * Gets the replay policy of this consumer configuration.
     * @return the replay policy.
     */
    public ReplayPolicy getReplayPolicy() {
        return GetOrDefault(replayPolicy);
    }

    /**
     * Gets the rate limit for this consumer configuration.
     * @return the rate limit in bits per second
     */
    public long getRateLimit() {
        return getOrUnsetUlong(rateLimit);
    }

    /**
     * Gets the maximum ack pending configuration.
     * @return maximum ack pending.
     */
    public long getMaxAckPending() {
        return getOrUnset(maxAckPending);
    }

    /**
     * Gets the sample frequency.
     * @return sampleFrequency.
     */
    public String getSampleFrequency() {
        return sampleFrequency;
    }


    /**
     * Gets the idle heart beat wait time
     * @return the idle heart beat wait duration.
     */
    public Duration getIdleHeartbeat() {
        return idleHeartbeat;
    }

    /**
     * Get the flow control flag indicating whether it's on or off
     * @return the flow control mode
     */
    public boolean isFlowControl() {
        // The way the builder and json reading works it's never false if it's not null
        // but this way I can make code coverage happy and not assume.
        return Boolean.TRUE.equals(flowControl);
    }

    /**
     * Get the number of pulls that can be outstanding on a pull consumer
     * @return the max pull waiting
     */
    public long getMaxPullWaiting() {
        return getOrUnset(maxPullWaiting);
    }

    /**
     * Get the header only flag indicating whether it's on or off
     * @return the flow control mode
     */
    public boolean isHeadersOnly() {
        return headersOnly != null && headersOnly;
    }

    /**
     * Get the mem storage flag whether it's on or off.
     * @return the mem storage mode
     */
    public boolean isMemStorage() {
        return memStorage != null && memStorage;
    }

    /**
     * Get the max batch size for the server to allow on pull requests.
     * @return the max batch size
     */
    public long getMaxBatch() {
        return getOrUnset(maxBatch);
    }

    /**
     * Get the max bytes size for the server to allow on pull requests.
     * @return the max byte size
     */
    public long getMaxBytes() {
        return getOrUnset(maxBytes);
    }

    /**
     * Get the max amount of expire time for the server to allow on pull requests.
     * @return the max expire
     */
    public Duration getMaxExpires() {
        return maxExpires;
    }

    /**
     * Get the amount of time before the consumer is deemed inactive.
     * @return the inactive threshold
     */
    public Duration getInactiveThreshold() {
        return inactiveThreshold;
    }

    /**
     * Get the backoff list; may be empty, will never be null.
     * @return the list
     */
    public List<Duration> getBackoff() {
        return backoff == null ? Collections.emptyList() : backoff;
    }

    /**
     * Metadata for the consumer; may be empty, will never be null.
     * @return the metadata map
     */
    public Map<String, String> getMetadata() {
        return metadata == null ? Collections.emptyMap() : metadata;
    }

    /**
     * Get the number of consumer replicas.
     * @return the replicas count
     */
    public int getNumReplicas() { return getOrUnset(numReplicas); }

    /**
     * Get the time until the consumer is paused.
     * @return paused until time
     */
    public ZonedDateTime getPauseUntil() {
        return pauseUntil;
    }

    /**
     * Gets the priority policy of this consumer configuration.
     * @return the priority policy.
     */
    public PriorityPolicy getPriorityPolicy() {
        return GetOrDefault(priorityPolicy);
    }

    /**
     * Gets whether deliver policy of this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean deliverPolicyWasSet() {
        return deliverPolicy != null;
    }

    /**
     * Gets whether ack policy for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean ackPolicyWasSet() {
        return ackPolicy != null;
    }

    /**
     * Gets whether replay policy for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean replayPolicyWasSet() {
        return replayPolicy != null;
    }

    /**
     * Gets whether start sequence for this consumer configuration was set or left unset
     * @return true if the start sequence was set by the user
     */
    public boolean startSeqWasSet() {
        return startSeq != null;
    }

    /**
     * Gets whether max deliver for this consumer configuration was set or left unset
     * @return true if max deliver was set by the user
     */
    public boolean maxDeliverWasSet() {
        return maxDeliver != null;
    }

    /**
     * Gets whether rate limit for this consumer configuration was set or left unset
     * @return true if rate limit was set by the user
     */
    public boolean rateLimitWasSet() {
        return rateLimit != null;
    }

    /**
     * Gets whether max ack pending for this consumer configuration was set or left unset
     * @return true if mac ack pending was set by the user
     */
    public boolean maxAckPendingWasSet() {
        return maxAckPending != null;
    }

    /**
     * Gets whether max pull waiting for this consumer configuration was set or left unset
     * @return true if max pull waiting was set by the user
     */
    public boolean maxPullWaitingWasSet() {
        return maxPullWaiting != null;
    }

    /**
     * Gets whether max batch for this consumer configuration was set or left unset
     * @return true if max batch was set by the user
     */
    public boolean maxBatchWasSet() {
        return maxBatch != null;
    }

    /**
     * Gets whether max bytes for this consumer configuration was set or left unset
     * @return true if max bytes was set by the user
     */
    public boolean maxBytesWasSet() {
        return maxBytes != null;
    }

    /**
     * Gets whether flow control for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean flowControlWasSet() {
        return flowControl != null;
    }

    /**
     * Gets whether headers only for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean headersOnlyWasSet() {
        return headersOnly != null;
    }

    /**
     * Gets whether mem storage for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean memStorageWasSet() {
        return memStorage != null;
    }

    /**
     * Gets whether num replicas for this consumer configuration was set or left unset
     * @return true if num replicas was set by the user
     */
    public boolean numReplicasWasSet() {
        return numReplicas != null;
    }

    /**
     * Gets whether backoff for this consumer configuration was set or left unset
     * @return true if num backoff was set by the user
     */
    public boolean backoffWasSet() {
        return backoff != null;
    }

    /**
     * Gets whether metadata for this consumer configuration was set or left unset
     * @return true if num metadata was set by the user
     */
    public boolean metadataWasSet() {
        return metadata != null;
    }

    /**
     * Gets whether priority policy for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean priorityPolicyWasSet() {
        return priorityPolicy != null;
    }

    /**
     * Creates a builder for the options.
     * @return a publish options builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder for the options.
     * @param cc the consumer configuration
     * @return a publish options builder
     */
    public static Builder builder(ConsumerConfiguration cc) {
        return cc == null ? new Builder() : new Builder(cc);
    }

    /**
     * ConsumerConfiguration is created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     *
     * <p>{@code new ConsumerConfiguration.Builder().build()} will create a default ConsumerConfiguration.
     *
     */
    public static class Builder {
        private DeliverPolicy deliverPolicy;
        private AckPolicy ackPolicy;
        private ReplayPolicy replayPolicy;

        private String description;
        private String durable;
        private String name;
        private String deliverSubject;
        private String deliverGroup;
        private String sampleFrequency;

        private ZonedDateTime startTime;
        private Duration ackWait;
        private Duration idleHeartbeat;
        private Duration maxExpires;
        private Duration inactiveThreshold;

        private Long startSeq;
        private Integer maxDeliver;
        private Long rateLimit;
        private Integer maxAckPending;
        private Integer maxPullWaiting;
        private Integer maxBatch;
        private Integer maxBytes;
        private Integer numReplicas;
        private ZonedDateTime pauseUntil;

        private Boolean flowControl;
        private Boolean headersOnly;
        private Boolean memStorage;

        private List<Duration> backoff;
        private Map<String, String> metadata;
        private List<String> filterSubjects;

        private List<String> priorityGroups;
        private PriorityPolicy priorityPolicy;

        /**
         * Construct the builder
         */
        public Builder() {}

        /**
         * Construct the builder and initialize values with the existing ConsumerConfiguration
         * @param cc the consumer configuration to clone
         */
        public Builder(ConsumerConfiguration cc) {
            if (cc != null) {
                this.deliverPolicy = cc.deliverPolicy;
                this.ackPolicy = cc.ackPolicy;
                this.replayPolicy = cc.replayPolicy;

                this.description = cc.description;
                this.durable = cc.durable;
                this.name = cc.name;
                this.deliverSubject = cc.deliverSubject;
                this.deliverGroup = cc.deliverGroup;
                this.sampleFrequency = cc.sampleFrequency;

                this.startTime = cc.startTime;
                this.ackWait = cc.ackWait;
                this.idleHeartbeat = cc.idleHeartbeat;
                this.maxExpires = cc.maxExpires;
                this.inactiveThreshold = cc.inactiveThreshold;

                this.startSeq = cc.startSeq;
                this.maxDeliver = cc.maxDeliver;
                this.rateLimit = cc.rateLimit;
                this.maxAckPending = cc.maxAckPending;
                this.maxPullWaiting = cc.maxPullWaiting;
                this.maxBatch = cc.maxBatch;
                this.maxBytes = cc.maxBytes;
                this.numReplicas = cc.numReplicas;
                this.pauseUntil = cc.pauseUntil;

                this.flowControl = cc.flowControl;
                this.headersOnly = cc.headersOnly;
                this.memStorage = cc.memStorage;

                if (cc.backoff != null) {
                    this.backoff = new ArrayList<>(cc.backoff);
                }
                if (cc.metadata != null) {
                    this.metadata = new HashMap<>(cc.metadata);
                }
                if (cc.filterSubjects != null) {
                    this.filterSubjects = new ArrayList<>(cc.filterSubjects);
                }

                if (cc.priorityGroups != null) {
                    this.priorityGroups = new ArrayList<>(cc.priorityGroups);
                }
                this.priorityPolicy = cc.priorityPolicy;
            }
        }

        /**
         * Initialize values from the json string.
         * @param json the json string to parse
         * @return the builder
         * @throws JsonParseException if the json is invalid
         */
        public Builder json(String json) throws JsonParseException {
            return jsonValue(JsonParser.parse(json));
        }

        /**
         * Initialize values from the JsonValue object.
         * @param jsonValue the json value object
         * @return the builder
         */
        public Builder jsonValue(JsonValue jsonValue) {
            deliverPolicy(DeliverPolicy.get(readString(jsonValue, DELIVER_POLICY)));
            ackPolicy(AckPolicy.get(readString(jsonValue, ACK_POLICY)));

            replayPolicy(ReplayPolicy.get(readString(jsonValue, REPLAY_POLICY)));

            description(readString(jsonValue, DESCRIPTION));
            durable(readString(jsonValue, DURABLE_NAME));
            name(readString(jsonValue, NAME));
            deliverSubject(readString(jsonValue, DELIVER_SUBJECT));
            deliverGroup(readString(jsonValue, DELIVER_GROUP));
            sampleFrequency(readString(jsonValue, SAMPLE_FREQ));
            startTime(readDate(jsonValue, OPT_START_TIME));
            ackWait(readNanos(jsonValue, ACK_WAIT));
            maxExpires(readNanos(jsonValue, MAX_EXPIRES));
            inactiveThreshold(readNanos(jsonValue, INACTIVE_THRESHOLD));

            startSequence(readLong(jsonValue, OPT_START_SEQ));
            maxDeliver(readLong(jsonValue, MAX_DELIVER, INTEGER_UNSET));
            rateLimit(readLong(jsonValue, RATE_LIMIT_BPS));
            maxAckPending(readLong(jsonValue, MAX_ACK_PENDING));
            maxPullWaiting(readLong(jsonValue, MAX_WAITING));
            maxBatch(readLong(jsonValue, MAX_BATCH));
            maxBytes(readLong(jsonValue, MAX_BYTES));

            Integer r = readInteger(jsonValue, NUM_REPLICAS);
            if (r != null) {
                if (r == 0) {
                    numReplicas = 0;
                }
                else {
                    numReplicas(r);
                }
            }

            pauseUntil(readDate(jsonValue, PAUSE_UNTIL));

            Duration idleHeartbeat = readNanos(jsonValue, IDLE_HEARTBEAT);
            if (idleHeartbeat != null) {
                if (readBoolean(jsonValue, FLOW_CONTROL, false)) {
                    flowControl(idleHeartbeat);
                }
                else {
                    idleHeartbeat(idleHeartbeat);
                }
            }

            headersOnly(readBoolean(jsonValue, HEADERS_ONLY, null));
            memStorage(readBoolean(jsonValue, MEM_STORAGE, null));

            //noinspection DataFlowIssue readNanosList with false ensures not null;
            backoff(readNanosList(jsonValue, BACKOFF, false).toArray(new Duration[0]));

            metadata(readStringStringMap(jsonValue, METADATA));

            String fs = emptyAsNull(readString(jsonValue, FILTER_SUBJECT));
            if (fs == null) {
                filterSubjects(readOptionalStringList(jsonValue, FILTER_SUBJECTS));
            }
            else {
                filterSubject(fs);
            }

            priorityGroups(readOptionalStringList(jsonValue, PRIORITY_GROUPS));
            priorityPolicy(PriorityPolicy.get(readString(jsonValue, PRIORITY_POLICY)));

            return this;
        }

        /**
         * Sets the description
         * @param description the description
         * @return the builder
         */
        public Builder description(String description) {
            this.description = emptyAsNull(description);
            return this;
        }

        /**
         * Sets the name of the durable consumer.
         * Null or empty clears the field.
         * @param durable name of the durable consumer.
         * @return the builder
         */
        public Builder durable(String durable) {
            this.durable = validateDurable(durable, false);
            return this;
        }

        /**
         * Sets the name of the consumer.
         * Null or empty clears the field.
         * @param name name of the consumer.
         * @return the builder
         */
        public Builder name(String name) {
            this.name = validateConsumerName(name, false);
            return this;
        }

        /**
         * Sets the delivery policy of the ConsumerConfiguration.
         * @param policy the delivery policy.
         * @return Builder
         */
        public Builder deliverPolicy(DeliverPolicy policy) {
            this.deliverPolicy = policy;
            return this;
        }

        /**
         * Sets the subject to deliver messages to.
         * <p> By setting the deliverySubject this configuration will create a <b>push consumer</b>. When left empty or set to NULL a pull consumer will be created.
         * @param subject the subject.
         * @return the builder
         */
        public Builder deliverSubject(String subject) {
            this.deliverSubject = emptyAsNull(subject);
            return this;
        }

        /**
         * Sets the group to deliver messages to.
         * @param group the delivery group.
         * @return the builder
         */
        public Builder deliverGroup(String group) {
            this.deliverGroup = emptyAsNull(group);
            return this;
        }

        /**
         * Sets the start sequence of the ConsumerConfiguration or null to unset / clear.
         * @param sequence the start sequence
         * @return Builder
         */
        public Builder startSequence(Long sequence) {
            this.startSeq = normalizeUlong(sequence);
            return this;
        }

        /**
         * Sets the start sequence of the ConsumerConfiguration.
         * @param sequence the start sequence
         * @return Builder
         */
        public Builder startSequence(long sequence) {
            this.startSeq = normalizeUlong(sequence);
            return this;
        }

        /**
         * Sets the start time of the ConsumerConfiguration.
         * @param startTime the start time
         * @return Builder
         */
        public Builder startTime(ZonedDateTime startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Sets the acknowledgement policy of the ConsumerConfiguration.
         * @param policy the acknowledgement policy.
         * @return Builder
         */
        public Builder ackPolicy(AckPolicy policy) {
            this.ackPolicy = policy;
            return this;
        }

        /**
         * Sets the acknowledgement wait duration of the ConsumerConfiguration.
         * @param timeout the wait timeout
         * @return Builder
         */
        public Builder ackWait(Duration timeout) {
            this.ackWait = normalize(timeout);
            return this;
        }

        /**
         * Sets the acknowledgement wait duration of the ConsumerConfiguration.
         * @param timeoutMillis the wait timeout in milliseconds
         * @return Builder
         */
        public Builder ackWait(long timeoutMillis) {
            this.ackWait = normalizeDuration(timeoutMillis);
            return this;
        }

        /**
         * Sets the maximum delivery amount of the ConsumerConfiguration or null to unset / clear.
         * @param maxDeliver the maximum delivery amount
         * @return Builder
         */
        public Builder maxDeliver(Long maxDeliver) {
            this.maxDeliver = normalize(maxDeliver, MAX_DELIVER_MIN);
            return this;
        }

        /**
         * Sets the maximum delivery amount of the ConsumerConfiguration.
         * @param maxDeliver the maximum delivery amount
         * @return Builder
         */
        public Builder maxDeliver(long maxDeliver) {
            this.maxDeliver = normalize(maxDeliver, MAX_DELIVER_MIN);
            return this;
        }

        /**
         * Sets the filter subject of the ConsumerConfiguration.
         * Replaces any other filter subjects set in the builder
         * @param filterSubject the filter subject
         * @return Builder
         */
        public Builder filterSubject(String filterSubject) {
            if (nullOrEmpty(filterSubject)) {
                this.filterSubjects = null;
            }
            else {
                this.filterSubjects = Collections.singletonList(filterSubject);
            }
            return this;
        }

        /**
         * Sets the filter subjects of the ConsumerConfiguration.
         * Replaces any other filter subjects set in the builder
         * @param filterSubjects one or more filter subjects
         * @return Builder
         */
        public Builder filterSubjects(String... filterSubjects) {
            return filterSubjects(Arrays.asList(filterSubjects));
        }

        /**
         * Sets the filter subjects of the ConsumerConfiguration.
         * Replaces any other filter subjects set in the builder
         * @param filterSubjects the list of filter subjects
         * @return Builder
         */
        public Builder filterSubjects(List<String> filterSubjects) {
            this.filterSubjects = new ArrayList<>();
            if (filterSubjects != null) {
                for (String fs : filterSubjects) {
                    if (!nullOrEmpty(fs)) {
                        this.filterSubjects.add(fs);
                    }
                }
            }
            if (this.filterSubjects.isEmpty()) {
                this.filterSubjects = null;
            }
            return this;
        }

        /**
         * Sets the replay policy of the ConsumerConfiguration.
         * @param policy the replay policy.
         * @return Builder
         */
        public Builder replayPolicy(ReplayPolicy policy) {
            this.replayPolicy = policy;
            return this;
        }

        /**
         * Sets the sample frequency of the ConsumerConfiguration.
         * @param frequency the frequency
         * @return Builder
         */
        public Builder sampleFrequency(String frequency) {
            this.sampleFrequency = emptyAsNull(frequency);
            return this;
        }

        /**
         * Set the rate limit of the ConsumerConfiguration or null to unset / clear.
         * @param bitsPerSecond bits per second to deliver
         * @return Builder
         */
        public Builder rateLimit(Long bitsPerSecond) {
            this.rateLimit = normalizeUlong(bitsPerSecond);
            return this;
        }

        /**
         * Set the rate limit of the ConsumerConfiguration.
         * @param bitsPerSecond bits per second to deliver
         * @return Builder
         */
        public Builder rateLimit(long bitsPerSecond) {
            this.rateLimit = normalizeUlong(bitsPerSecond);
            return this;
        }

        /**
         * Sets the maximum ack pending or null to unset / clear.
         * @param maxAckPending maximum pending acknowledgements.
         * @return Builder
         */
        public Builder maxAckPending(Long maxAckPending) {
            this.maxAckPending = normalize(maxAckPending, STANDARD_MIN);
            return this;
        }

        /**
         * Sets the maximum ack pending.
         * @param maxAckPending maximum pending acknowledgements.
         * @return Builder
         */
        public Builder maxAckPending(long maxAckPending) {
            this.maxAckPending = normalize(maxAckPending, STANDARD_MIN);
            return this;
        }

        /**
         * sets the idle heart beat wait time
         * @param idleHeartbeat the idle heart beat duration
         * @return Builder
         */
        public Builder idleHeartbeat(Duration idleHeartbeat) {
            if (idleHeartbeat == null) {
                this.idleHeartbeat = null;
            }
            else {
                long nanos = idleHeartbeat.toNanos();
                if (nanos <= DURATION_UNSET_LONG) {
                    this.idleHeartbeat = DURATION_UNSET;
                }
                else if (nanos < MIN_IDLE_HEARTBEAT_NANOS) {
                    throw new IllegalArgumentException("Duration must be greater than or equal to " + MIN_IDLE_HEARTBEAT_NANOS + " nanos.");
                }
                else {
                    this.idleHeartbeat = idleHeartbeat;
                }
            }
            return this;
        }

        /**
         * sets the idle heart beat wait time
         * @param idleHeartbeatMillis the idle heart beat duration in milliseconds
         * @return Builder
         */
        public Builder idleHeartbeat(long idleHeartbeatMillis) {
            if (idleHeartbeatMillis <= DURATION_UNSET_LONG) {
                this.idleHeartbeat = DURATION_UNSET;
            }
            else if (idleHeartbeatMillis < MIN_IDLE_HEARTBEAT_MILLIS) {
                throw new IllegalArgumentException("Duration must be greater than or equal to " + MIN_IDLE_HEARTBEAT_MILLIS + " milliseconds.");
            }
            else {
                this.idleHeartbeat = Duration.ofMillis(idleHeartbeatMillis);
            }
            return this;
        }

        /**
         * set the flow control on and set the idle heartbeat
         * @param idleHeartbeat the idle heart beat duration
         * @return Builder
         */
        public Builder flowControl(Duration idleHeartbeat) {
            this.flowControl = true;
            return idleHeartbeat(idleHeartbeat);
        }

        /**
         * set the flow control on and set the idle heartbeat
         * @param idleHeartbeatMillis the idle heart beat duration in milliseconds
         * @return Builder
         */
        public Builder flowControl(long idleHeartbeatMillis) {
            this.flowControl = true;
            return idleHeartbeat(idleHeartbeatMillis);
        }

        /**
         * sets the max amount of expire time for the server to allow on pull requests.
         * @param maxExpires the max expire duration
         * @return Builder
         */
        public Builder maxExpires(Duration maxExpires) {
            this.maxExpires = normalize(maxExpires);
            return this;
        }

        /**
         * sets the max amount of expire time for the server to allow on pull requests.
         * @param maxExpires the max expire duration in milliseconds
         * @return Builder
         */
        public Builder maxExpires(long maxExpires) {
            this.maxExpires = normalizeDuration(maxExpires);
            return this;
        }

        /**
         * sets the amount of time before the consumer is deemed inactive.
         * @param inactiveThreshold the threshold duration
         * @return Builder
         */
        public Builder inactiveThreshold(Duration inactiveThreshold) {
            this.inactiveThreshold = normalize(inactiveThreshold);
            return this;
        }

        /**
         * sets the amount of time before the consumer is deemed inactive.
         * @param inactiveThreshold the threshold duration in milliseconds
         * @return Builder
         */
        public Builder inactiveThreshold(long inactiveThreshold) {
            this.inactiveThreshold = normalizeDuration(inactiveThreshold);
            return this;
        }

        /**
         * sets the max pull waiting, the number of pulls that can be outstanding on a pull consumer, pulls received after this is reached are ignored.
         * Use null to unset / clear.
         * @param maxPullWaiting the max pull waiting
         * @return Builder
         */
        public Builder maxPullWaiting(Long maxPullWaiting) {
            this.maxPullWaiting = normalize(maxPullWaiting, STANDARD_MIN);
            return this;
        }

        /**
         * sets the max pull waiting, the number of pulls that can be outstanding on a pull consumer, pulls received after this is reached are ignored.
         * @param maxPullWaiting the max pull waiting
         * @return Builder
         */
        public Builder maxPullWaiting(long maxPullWaiting) {
            this.maxPullWaiting = normalize(maxPullWaiting, STANDARD_MIN);
            return this;
        }

        /**
         * sets the max batch size for the server to allow on pull requests.
         * @param maxBatch the max batch size
         * @return Builder
         */
        public Builder maxBatch(Long maxBatch) {
            this.maxBatch = normalize(maxBatch, STANDARD_MIN);
            return this;
        }

        /**
         * sets the max batch size for the server to allow on pull requests.
         * @param maxBatch the max batch size
         * @return Builder
         */
        public Builder maxBatch(long maxBatch) {
            this.maxBatch = normalize(maxBatch, STANDARD_MIN);
            return this;
        }

        /**
         * sets the max bytes size for the server to allow on pull requests.
         * @param maxBytes the max bytes size
         * @return Builder
         */
        public Builder maxBytes(Long maxBytes) {
            this.maxBytes = normalize(maxBytes, STANDARD_MIN);
            return this;
        }

        /**
         * sets the max bytes size for the server to allow on pull requests.
         * @param maxBytes the max bytes size
         * @return Builder
         */
        public Builder maxBytes(long maxBytes) {
            this.maxBytes = normalize(maxBytes, STANDARD_MIN);
            return this;
        }

        /**
         * set the number of replicas for the consumer. When set do not inherit the
         * replica count from the stream but specifically set it to this amount.
         * @param numReplicas number of replicas for the consumer
         * @return Builder
         */
        public Builder numReplicas(Integer numReplicas) {
            this.numReplicas = numReplicas == null ? null : validateNumberOfReplicas(numReplicas);
            return this;
        }

        /**
         * Sets the time to pause the consumer until.
         * @param pauseUntil the time to pause
         * @return Builder
         */
        public Builder pauseUntil(ZonedDateTime pauseUntil) {
            this.pauseUntil = pauseUntil;
            return this;
        }

        /**
         * set the headers only flag saying to deliver only the headers of
         * messages in the stream and not the bodies
         * @param headersOnly the flag
         * @return Builder
         */
        public Builder headersOnly(Boolean headersOnly) {
            this.headersOnly = headersOnly;
            return this;
        }

        /**
         * set the mem storage flag to force the consumer state to be kept
         * in memory rather than inherit the setting from the stream
         * @param memStorage the flag
         * @return Builder
         */
        public Builder memStorage(Boolean memStorage) {
            this.memStorage = memStorage;
            return this;
        }

        /**
         * Set the list of backoff. Will override ackwait setting.
         * @see <a href="https://docs.nats.io/using-nats/developer/develop_jetstream/consumers#delivery-reliability">Delivery Reliability</a>
         * @param backoffs zero or more backoff durations or an array of backoffs
         * @return Builder
         */
        public Builder backoff(Duration... backoffs) {
            if (backoffs == null || (backoffs.length == 1 && backoffs[0] == null))
            {
                backoff = null;
            }
            else
            {
                backoff = new ArrayList<>();
                for (Duration d : backoffs)
                {
                    if (d != null)
                    {
                        if (d.toNanos() < DURATION_MIN_LONG)
                        {
                            throw new IllegalArgumentException("Backoff cannot be less than " + DURATION_MIN_LONG);
                        }
                        backoff.add(d);
                    }
                }
            }
            return this;
        }

        /**
         * Set the list of backoff. Will override ackwait setting.
         * @see <a href="https://docs.nats.io/using-nats/developer/develop_jetstream/consumers#delivery-reliability">Delivery Reliability</a>
         * @param backoffsMillis zero or more backoff in millis or an array of backoffsMillis
         * @return Builder
         */
        public Builder backoff(long... backoffsMillis) {
            if (backoffsMillis == null) {
                backoff = null;
            }
            else {
                backoff = new ArrayList<>();
                for (long ms : backoffsMillis) {
                    if (ms < DURATION_MIN_LONG) {
                        throw new IllegalArgumentException("Backoff cannot be less than " + DURATION_MIN_LONG);
                    }
                    this.backoff.add(Duration.ofMillis(ms));
                }
            }
            return this;
        }

        /**
         * Sets the metadata for the configuration
         * @param metadata the metadata map
         * @return Builder
         */
        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata == null || metadata.isEmpty() ? null : new HashMap<>(metadata);
            return this;
        }

        /**
         * Sets the priority groups of the ConsumerConfiguration.
         * Replaces any other priority groups set in the builder
         * @param priorityGroups one or more priority groups
         * @return Builder
         */
        public Builder priorityGroups(String... priorityGroups) {
            return priorityGroups(Arrays.asList(priorityGroups));
        }

        /**
         * Sets the priority groups of the ConsumerConfiguration.
         * Replaces any other priority groups set in the builder
         * @param priorityGroups the list of priority groups
         * @return Builder
         */
        public Builder priorityGroups(List<String> priorityGroups) {
            this.priorityGroups = new ArrayList<>();
            if (priorityGroups != null) {
                for (String pg : priorityGroups) {
                    if (!nullOrEmpty(pg)) {
                        this.priorityGroups.add(pg);
                    }
                }
            }
            if (this.priorityGroups.isEmpty()) {
                this.priorityGroups = null;
            }
            return this;
        }

        /**
         * Sets the priority policy of the ConsumerConfiguration.
         * @param policy the priority policy.
         * @return Builder
         */
        public Builder priorityPolicy(PriorityPolicy policy) {
            this.priorityPolicy = policy;
            return this;
        }

        /**
         * Builds the ConsumerConfiguration
         * @return The consumer configuration.
         */
        public ConsumerConfiguration build() {
            validateMustMatchIfBothSupplied(name, durable, JsConsumerNameDurableMismatch);
            return new ConsumerConfiguration(this);
        }

        /**
         * Builds the PushSubscribeOptions with this configuration
         * @return The PushSubscribeOptions.
         */
        public PushSubscribeOptions buildPushSubscribeOptions() {
            return PushSubscribeOptions.builder().configuration(build()).build();
        }

        /**
         * Builds the PushSubscribeOptions with this configuration.
         * Providing the stream is a hint for the subscription process that
         * saves a call to the server. Assumes the stream is the correct stream
         * for the subject filter, otherwise the server will return an error
         * which the subscription call will raise to the user.
         * @param stream the stream for this consumer
         * @return The PushSubscribeOptions.
         */
        public PushSubscribeOptions buildPushSubscribeOptions(String stream) {
            return PushSubscribeOptions.builder().configuration(build()).stream(stream).build();
        }

        /**
         * Builds the PullSubscribeOptions with this configuration
         * @return The PullSubscribeOptions.
         */
        public PullSubscribeOptions buildPullSubscribeOptions() {
            return PullSubscribeOptions.builder().configuration(build()).build();
        }

        /**
         * Builds the PullSubscribeOptions with this configuration
         * Providing the stream is a hint for the subscription process that
         * saves a call to the server. Assumes the stream is the correct stream
         * for the subject filter, otherwise the server will return an error
         * which the subscription call will raise to the user.
         * @param stream the stream for this consumer
         * @return The PullSubscribeOptions.
         */
        public PullSubscribeOptions buildPullSubscribeOptions(String stream) {
            return PullSubscribeOptions.builder().configuration(build()).stream(stream).build();
        }
    }

    @Override
    public String toString() {
        return "ConsumerConfiguration " + toJson();
    }

    protected static int getOrUnset(Integer val)
    {
        return val == null ? INTEGER_UNSET : val;
    }

    protected static long getOrUnsetUlong(Long val)
    {
        return val == null || val < 0 ? ULONG_UNSET : val;
    }

    protected static Duration getOrUnset(Duration val)
    {
        return val == null ? DURATION_UNSET : val;
    }

    protected static Integer normalize(Long l, int min) {
        if (l == null) {
            return null;
        }

        if (l < min) {
            return INTEGER_UNSET;
        }

        if (l > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        return l.intValue();
    }

    protected static Long normalizeUlong(Long u)
    {
        return u == null ? null : u <= ULONG_UNSET ? ULONG_UNSET : u;
    }

    protected static Duration normalize(Duration d)
    {
        return d == null ? null : d.toNanos() <= DURATION_UNSET_LONG ? DURATION_UNSET : d;
    }

    protected static Duration normalizeDuration(long millis)
    {
        return millis <= DURATION_UNSET_LONG ? DURATION_UNSET : Duration.ofMillis(millis);
    }

    protected static DeliverPolicy GetOrDefault(DeliverPolicy p) { return p == null ? DEFAULT_DELIVER_POLICY : p; }
    protected static AckPolicy GetOrDefault(AckPolicy p) { return p == null ? DEFAULT_ACK_POLICY : p; }
    protected static ReplayPolicy GetOrDefault(ReplayPolicy p) { return p == null ? DEFAULT_REPLAY_POLICY : p; }
    protected static PriorityPolicy GetOrDefault(PriorityPolicy p) { return p == null ? DEFAULT_PRIORITY_POLICY : p; }
}
