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

package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.nats.client.PushSubscribeOptions.DEFAULT_PUSH_OPTS;
import static io.nats.client.impl.MessageManager.ManageResult;
import static io.nats.client.support.NatsJetStreamClientError.*;
import static io.nats.client.support.NatsRequestCompletableFuture.CancelAction;
import static io.nats.client.support.Validator.*;

public class NatsJetStream extends NatsJetStreamImpl implements JetStream {

    public NatsJetStream(NatsConnection connection, JetStreamOptions jsOptions) throws IOException {
        super(connection, jsOptions);
    }

    // ----------------------------------------------------------------------------------------------------
    // Publish
    // ----------------------------------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(String subject, byte[] body) throws IOException, JetStreamApiException {
        return publishSyncInternal(subject, null, body, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(String subject, Headers headers, byte[] body) throws IOException, JetStreamApiException {
        return publishSyncInternal(subject, headers, body, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(String subject, byte[] body, PublishOptions options) throws IOException, JetStreamApiException {
        return publishSyncInternal(subject, null, body, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(String subject, Headers headers, byte[] body, PublishOptions options) throws IOException, JetStreamApiException {
        return publishSyncInternal(subject, headers, body, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(Message message) throws IOException, JetStreamApiException {
        validateNotNull(message, "Message");
        return publishSyncInternal(message.getSubject(), message.getHeaders(), message.getData(), null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(Message message, PublishOptions options) throws IOException, JetStreamApiException {
        validateNotNull(message, "Message");
        return publishSyncInternal(message.getSubject(), message.getHeaders(), message.getData(), options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(String subject, byte[] body) {
        return publishAsyncInternal(subject, null, body, null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(String subject, Headers headers, byte[] body) {
        return publishAsyncInternal(subject, headers, body, null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(String subject, byte[] body, PublishOptions options) {
        return publishAsyncInternal(subject, null, body, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(String subject, Headers headers, byte[] body, PublishOptions options) {
        return publishAsyncInternal(subject, headers, body, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(Message message) {
        validateNotNull(message, "Message");
        return publishAsyncInternal(message.getSubject(), message.getHeaders(), message.getData(), null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(Message message, PublishOptions options) {
        validateNotNull(message, "Message");
        return publishAsyncInternal(message.getSubject(), message.getHeaders(), message.getData(), options, null);
    }

    private PublishAck publishSyncInternal(String subject, Headers headers, byte[] data, PublishOptions options) throws IOException, JetStreamApiException {
        Headers merged = mergePublishOptions(headers, options);

        if (jso.isPublishNoAck()) {
            conn.publishInternal(subject, null, merged, data);
            return null;
        }

        Duration timeout = options == null ? jso.getRequestTimeout() : options.getStreamTimeout();

        Message resp = makeInternalRequestResponseRequired(subject, merged, data, timeout, CancelAction.COMPLETE);
        return processPublishResponse(resp, options);
    }

    private CompletableFuture<PublishAck> publishAsyncInternal(String subject, Headers headers, byte[] data, PublishOptions options, Duration knownTimeout) {
        Headers merged = mergePublishOptions(headers, options);

        if (jso.isPublishNoAck()) {
            conn.publishInternal(subject, null, merged, data);
            return null;
        }

        CompletableFuture<Message> future = conn.requestFutureInternal(subject, merged, data, knownTimeout, CancelAction.COMPLETE);

        return future.thenCompose(resp -> {
            try {
                responseRequired(resp);
                return CompletableFuture.completedFuture(processPublishResponse(resp, options));
            } catch (IOException | JetStreamApiException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private PublishAck processPublishResponse(Message resp, PublishOptions options) throws IOException, JetStreamApiException {
        if (resp.isStatusMessage()) {
            throw new IOException("Error Publishing: " + resp.getStatus().getMessageWithCode());
        }

        PublishAck ack = new PublishAck(resp);
        String ackStream = ack.getStream();
        String pubStream = options == null ? null : options.getStream();
        // stream specified in options but different from ack should not happen but...
        if (pubStream != null && !pubStream.equals(ackStream)) {
            throw new IOException("Expected ack from stream " + pubStream + ", received from: " + ackStream);
        }
        return ack;
    }

    private Headers mergePublishOptions(Headers headers, PublishOptions opts) {
        // never touch the user's original headers
        Headers merged = headers == null ? null : new Headers(headers);

        if (opts != null) {
            merged = mergeNum(merged, EXPECTED_LAST_SEQ_HDR, opts.getExpectedLastSequence());
            merged = mergeNum(merged, EXPECTED_LAST_SUB_SEQ_HDR, opts.getExpectedLastSubjectSequence());
            merged = mergeString(merged, EXPECTED_LAST_MSG_ID_HDR, opts.getExpectedLastMsgId());
            merged = mergeString(merged, EXPECTED_STREAM_HDR, opts.getExpectedStream());
            merged = mergeString(merged, MSG_ID_HDR, opts.getMessageId());
        }

        return merged;
    }

    private Headers mergeNum(Headers h, String key, long value) {
        return value > -1 ? _mergeNum(h, key, Long.toString(value)): h;
    }

    private Headers mergeString(Headers h, String key, String value) {
        return Validator.nullOrEmpty(value) ? h : _mergeNum(h, key, value);
    }

    private Headers _mergeNum(Headers h, String key, String value) {
        if (h == null) {
            h = new Headers();
        }
        return h.add(key, value);
    }

    // ----------------------------------------------------------------------------------------------------
    // Subscribe
    // ----------------------------------------------------------------------------------------------------
    interface MessageManagerFactory {
        MessageManager createMessageManager(
            NatsConnection conn, NatsJetStream js, String stream,
            SubscribeOptions so, ConsumerConfiguration cc, boolean queueMode, boolean syncMode);
    }

    MessageManagerFactory _pushMessageManagerFactory = PushMessageManager::new;
    MessageManagerFactory _pushOrderedMessageManagerFactory = OrderedMessageManager::new;
    MessageManagerFactory _pullMessageManagerFactory =
        (mmConn, mmJs, mmStream, mmSo, mmCc, mmQueueMode, mmSyncMode) -> new PullMessageManager(mmConn, mmSo, mmSyncMode);

    JetStreamSubscription createSubscription(String subject,
                                             String queueName,
                                             NatsDispatcher dispatcher,
                                             MessageHandler userHandler,
                                             boolean isAutoAck,
                                             PushSubscribeOptions pushSubscribeOptions,
                                             PullSubscribeOptions pullSubscribeOptions
    ) throws IOException, JetStreamApiException {

        // 1. Prepare for all the validation
        boolean isPullMode = pullSubscribeOptions != null;

        SubscribeOptions so;
        String stream;
        String qgroup;
        ConsumerConfiguration userCC;

        if (isPullMode) {
            so = pullSubscribeOptions; // options must have already been checked to be non-null
            stream = pullSubscribeOptions.getStream();

            userCC = so.getConsumerConfiguration();

            qgroup = null; // just to make compiler happy both paths set variable
            validateNotSupplied(userCC.getDeliverGroup(), JsSubPullCantHaveDeliverGroup);
            validateNotSupplied(userCC.getDeliverSubject(), JsSubPullCantHaveDeliverSubject);
        }
        else {
            so = pushSubscribeOptions == null ? DEFAULT_PUSH_OPTS : pushSubscribeOptions;
            stream = so.getStream(); // might be null, that's ok (see directBind)

            userCC = so.getConsumerConfiguration();

            if (userCC.maxPullWaitingWasSet()) { throw JsSubPushCantHaveMaxPullWaiting.instance(); }
            if (userCC.maxBatchWasSet())       { throw JsSubPushCantHaveMaxBatch.instance(); }
            if (userCC.maxBytesWasSet())       { throw JsSubPushCantHaveMaxBytes.instance(); }

            // figure out the queue name
            qgroup = validateMustMatchIfBothSupplied(userCC.getDeliverGroup(), queueName, JsSubQueueDeliverGroupMismatch);
            if (so.isOrdered() && qgroup != null) {
                throw JsSubOrderedNotAllowOnQueues.instance();
            }

            if (dispatcher != null &&
                (so.getPendingMessageLimit() != Consumer.DEFAULT_MAX_MESSAGES ||
                    so.getPendingByteLimit() != Consumer.DEFAULT_MAX_BYTES))
            {
                throw JsSubPushAsyncCantSetPending.instance();
            }
        }

        // 2A. Flow Control / heartbeat not always valid
        if (userCC.getIdleHeartbeat() != null && userCC.getIdleHeartbeat().toMillis() > 0) {
            if (isPullMode) {
                throw JsSubFcHbNotValidPull.instance();
            }
            if (qgroup != null) {
                throw JsSubFcHbNotValidQueue.instance();
            }
        }

        // 2B. Did they tell me what stream? No? look it up.
        final String fnlStream;
        if (stream == null) {
            fnlStream = lookupStreamBySubject(subject);
            if (fnlStream == null) {
                throw JsSubNoMatchingStreamForSubject.instance();
            }
        }
        else {
            fnlStream = stream;
        }

        ConsumerConfiguration serverCC = null;
        String consumerName = userCC.getDurable();
        String inboxDeliver = userCC.getDeliverSubject();

        // 3. Does this consumer already exist?
        if (consumerName != null) {
            ConsumerInfo serverInfo = lookupConsumerInfo(fnlStream, consumerName);

            if (serverInfo != null) { // the consumer for that durable already exists
                serverCC = serverInfo.getConsumerConfiguration();

                // check to see if the user sent a different version than the server has
                // because modifications are not allowed during create subscription
                ConsumerConfigurationComparer userCCC = new ConsumerConfigurationComparer(userCC);
                List<String> changes = userCCC.getChanges(serverCC);
                if (changes.size() > 0) {
                    throw JsSubExistingConsumerCannotBeModified.instance("Changed fields: " + changes);
                }

                // deliver subject must be null/empty for pull, defined for push
                if (isPullMode) {
                    if (!nullOrEmpty(serverCC.getDeliverSubject())) {
                        throw JsSubConsumerAlreadyConfiguredAsPush.instance();
                    }
                }
                else if (nullOrEmpty(serverCC.getDeliverSubject())) {
                    throw JsSubConsumerAlreadyConfiguredAsPull.instance();
                }

                if (serverCC.getDeliverGroup() == null) {
                    // lookedUp was null, means existing consumer is not a queue consumer
                    if (qgroup == null) {
                        // ok fine, no queue requested and the existing consumer is also not a queue consumer
                        // we must check if the consumer is in use though
                        if (serverInfo.isPushBound()) {
                            throw JsSubConsumerAlreadyBound.instance();
                        }
                    }
                    else { // else they requested a queue but this durable was not configured as queue
                        throw JsSubExistingConsumerNotQueue.instance();
                    }
                }
                else if (qgroup == null) {
                    throw JsSubExistingConsumerIsQueue.instance();
                }
                else if (!serverCC.getDeliverGroup().equals(qgroup)) {
                    throw JsSubExistingQueueDoesNotMatchRequestedQueue.instance();
                }

                // durable already exists, make sure the filter subject matches
                if (nullOrEmpty(subject)) { // allowed if they had given both stream and durable
                    subject = userCC.getFilterSubject();
                }
                else if (!isFilterMatch(subject, serverCC.getFilterSubject(), fnlStream)) {
                    throw JsSubSubjectDoesNotMatchFilter.instance();
                }

                inboxDeliver = serverCC.getDeliverSubject(); // use the deliver subject as the inbox. It may be null, that's ok, we'll fix that later
            }
            else if (so.isBind()) {
                throw JsSubConsumerNotFoundRequiredInBind.instance();
            }
        }

        // 4. If no deliver subject (inbox) provided or found, make an inbox.
        final String fnlInboxDeliver;
        if (isPullMode) {
            fnlInboxDeliver = conn.createInbox() + ".*";
        }
        else if (inboxDeliver == null) {
            fnlInboxDeliver = conn.createInbox();
        }
        else {
            fnlInboxDeliver = inboxDeliver;
        }

        // 5. If consumer does not exist, create and settle on the config. Name will have to wait
        //    If the consumer exists, I know what the settled info is
        final String settledConsumerName;
        final ConsumerConfiguration settledServerCC;
        if (serverCC == null) {
            ConsumerConfiguration.Builder ccBuilder = ConsumerConfiguration.builder(userCC);

            // Pull mode doesn't maintain a deliver subject. It's actually an error if we send it.
            if (!isPullMode) {
                ccBuilder.deliverSubject(fnlInboxDeliver);
            }

            if (userCC.getFilterSubject() == null) {
                ccBuilder.filterSubject(subject);
            }

            ccBuilder.deliverGroup(qgroup);

            settledServerCC = ccBuilder.build();
            settledConsumerName = null;
        }
        else {
            settledServerCC = serverCC;
            settledConsumerName = consumerName;
        }

        // 6. create the subscription. lambda needs final or effectively final vars
        final MessageManager mm;
        final NatsSubscriptionFactory subFactory;
        if (isPullMode) {
            mm = _pullMessageManagerFactory.createMessageManager(conn, this, fnlStream, so, settledServerCC, false, dispatcher == null);
            subFactory = (sid, lSubject, lQgroup, lConn, lDispatcher)
                -> new NatsJetStreamPullSubscription(sid, lSubject, lConn, lDispatcher, this, fnlStream, settledConsumerName, mm);
        }
        else {
            MessageManagerFactory mmFactory = so.isOrdered() ? _pushOrderedMessageManagerFactory : _pushMessageManagerFactory;
            mm = mmFactory.createMessageManager(conn, this, fnlStream, so, settledServerCC, false, dispatcher == null);
            subFactory = (sid, lSubject, lQgroup, lConn, lDispatcher) -> {
                NatsJetStreamSubscription nsub = new NatsJetStreamSubscription(sid, lSubject, lQgroup, lConn, lDispatcher,
                    this, fnlStream, settledConsumerName, mm);
                if (lDispatcher == null) {
                    nsub.setPendingLimits(so.getPendingMessageLimit(), so.getPendingByteLimit());
                }
                return nsub;
            };
        }
        NatsJetStreamSubscription sub;
        if (dispatcher == null) {
            sub = (NatsJetStreamSubscription) conn.createSubscription(fnlInboxDeliver, qgroup, null, subFactory);
        }
        else {
            AsyncMessageHandler handler = new AsyncMessageHandler(mm, userHandler, isAutoAck, settledServerCC);
            sub = (NatsJetStreamSubscription) dispatcher.subscribeImplJetStream(fnlInboxDeliver, qgroup, handler, subFactory);
        }

        // 7. The consumer might need to be created, do it here
        if (settledConsumerName == null) {
            _createConsumerUnsubscribeOnException(fnlStream, settledServerCC, sub);
        }

        return sub;
    }

    static class ConsumerConfigurationComparer extends ConsumerConfiguration {
        public ConsumerConfigurationComparer(ConsumerConfiguration cc) {
            super(cc);
        }

        public List<String> getChanges(ConsumerConfiguration serverCc) {
            ConsumerConfigurationComparer serverCcc = new ConsumerConfigurationComparer(serverCc);
            List<String> changes = new ArrayList<>();

            if (deliverPolicy != null && deliverPolicy != serverCcc.getDeliverPolicy()) { changes.add("deliverPolicy"); };
            if (ackPolicy != null && ackPolicy != serverCcc.getAckPolicy()) { changes.add("ackPolicy"); };
            if (replayPolicy != null && replayPolicy != serverCcc.getReplayPolicy()) { changes.add("replayPolicy"); };

            if (flowControl != null && flowControl != serverCcc.isFlowControl()) { changes.add("flowControl"); };
            if (headersOnly != null && headersOnly != serverCcc.isHeadersOnly()) { changes.add("headersOnly"); };
            if (memStorage != null && memStorage != serverCcc.isMemStorage()) { changes.add("memStorage"); };

            if (startSeq != null && !startSeq.equals(serverCcc.getStartSequence())) { changes.add("startSequence"); };
            if (rateLimit != null && !rateLimit.equals(serverCcc.getStartSequence())) { changes.add("rateLimit"); };

            if (maxDeliver != null && !maxDeliver.equals(serverCcc.getMaxDeliver())) { changes.add("maxDeliver"); };
            if (maxAckPending != null && !maxAckPending.equals(serverCcc.getMaxAckPending())) { changes.add("maxAckPending"); };
            if (maxPullWaiting != null && !maxPullWaiting.equals(serverCcc.getMaxPullWaiting())) { changes.add("maxPullWaiting"); };
            if (maxBatch != null && !maxBatch.equals(serverCcc.getMaxBatch())) { changes.add("maxBatch"); };
            if (maxBytes != null && !maxBytes.equals(serverCcc.getMaxBytes())) { changes.add("maxBytes"); };
            if (numReplicas != null && !numReplicas.equals(serverCcc.numReplicas)) { changes.add("numReplicas"); };

            if (ackWait != null && !ackWait.equals(getOrUnset(serverCcc.ackWait))) { changes.add("ackWait"); };
            if (idleHeartbeat != null && !idleHeartbeat.equals(getOrUnset(serverCcc.idleHeartbeat))) { changes.add("idleHeartbeat"); };
            if (maxExpires != null && !maxExpires.equals(getOrUnset(serverCcc.maxExpires))) { changes.add("maxExpires"); };
            if (inactiveThreshold != null && !inactiveThreshold.equals(getOrUnset(serverCcc.inactiveThreshold))) { changes.add("inactiveThreshold"); };

            if (startTime != null && !startTime.equals(serverCcc.startTime)) { changes.add("startTime"); };

            if (filterSubject != null && !filterSubject.equals(serverCcc.filterSubject)) { changes.add("filterSubject"); };
            if (description != null && !description.equals(serverCcc.description)) { changes.add("description"); };
            if (sampleFrequency != null && !sampleFrequency.equals(serverCcc.sampleFrequency)) { changes.add("sampleFrequency"); };
            if (deliverSubject != null && !deliverSubject.equals(serverCcc.deliverSubject)) { changes.add("deliverSubject"); };
            if (deliverGroup != null && !deliverGroup.equals(serverCcc.deliverGroup)) { changes.add("deliverGroup"); };

            if (backoff != null && !listsAreEqual(backoff, serverCcc.backoff, true)) { changes.add("backoff"); };
            if (metadata != null && !mapsAreEqual(metadata, serverCcc.metadata, true)) { changes.add("metadata"); };

            // do not need to check Durable because the original is retrieved by the durable name

            return changes;
        }
    }

    static class AsyncMessageHandler implements MessageHandler {
        MessageManager manager;
        MessageHandler userHandler;
        boolean autoAck;

        public AsyncMessageHandler(MessageManager manager, MessageHandler userHandler, boolean isAutoAck, ConsumerConfiguration settledServerCC) {
            this.manager = manager;
            this.userHandler = userHandler;
            autoAck = isAutoAck && settledServerCC.getAckPolicy() != AckPolicy.None;
        }

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            if (manager.manage(msg) == ManageResult.MESSAGE) {
                userHandler.onMessage(msg);
                if (autoAck) {
                    msg.ack();
                }
            }
        }
    }

    private boolean isFilterMatch(String subscribeSubject, String filterSubject, String stream) throws IOException, JetStreamApiException {

        // subscribeSubject guaranteed to not be null
        // filterSubject may be null or empty or have value

        if (subscribeSubject.equals(filterSubject)) {
            return true;
        }

        if (nullOrEmpty(filterSubject) || filterSubject.equals(">")) {
            // lookup stream subject returns null if there is not exactly one subject
            String streamSubject = lookupStreamSubject(stream);
            return subscribeSubject.equals(streamSubject);
        }

        return false;
    }

    private String lookupStreamSubject(String stream) throws IOException, JetStreamApiException {
        StreamInfo si = _getStreamInfo(stream, null);
        List<String> streamSubjects = si.getConfiguration().getSubjects();
        return streamSubjects.size() == 1 ? streamSubjects.get(0) : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject) throws IOException, JetStreamApiException {
        validateSubject(subject, true);
        return createSubscription(subject, null, null, null, false, null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, isSubjectRequired(options));
        return createSubscription(subject, null, null, null, false, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, String queue, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, isSubjectRequired(options));
        queue = emptyAsNull(validateQueueName(queue, false));
        return createSubscription(subject, queue, null, null, false, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException {
        validateSubject(subject, true);
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, autoAck, null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, isSubjectRequired(options));
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, autoAck, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, String queue, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, isSubjectRequired(options));
        queue = emptyAsNull(validateQueueName(queue, false));
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subject, queue, (NatsDispatcher) dispatcher, handler, autoAck, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, PullSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, isSubjectRequired(options));
        validateNotNull(options, "Pull Subscribe Options");
        return createSubscription(subject, null, null, null, false, null, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, PullSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, isSubjectRequired(options));
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        validateNotNull(options, "Pull Subscribe Options");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, false, null, options);
    }

    private boolean isSubjectRequired(SubscribeOptions options) {
        return options == null || !options.isBind();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamContext streamContext(String streamName) throws IOException, JetStreamApiException {
        Validator.validateStreamName(streamName, true);
        return getNatsStreamContext(streamName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerContext consumerContext(String streamName, String consumerName) throws IOException, JetStreamApiException {
        Validator.validateStreamName(streamName, true);
        Validator.required(consumerName, "Consumer Name");
        return getNatsStreamContext(streamName).consumerContext(consumerName);
    }

    private NatsStreamContext getNatsStreamContext(String streamName) throws IOException, JetStreamApiException {
        return new NatsStreamContext(conn, jso, streamName);
    }
}
