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

    NatsJetStream(NatsJetStreamImpl impl) {
        super(impl);
    }
    // ----------------------------------------------------------------------------------------------------
    // Publish
    // ----------------------------------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(String subject, byte[] body) throws IOException, JetStreamApiException {
        return publishSyncInternal(subject, null, body, null, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(String subject, Headers headers, byte[] body) throws IOException, JetStreamApiException {
        return publishSyncInternal(subject, headers, body, null, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(String subject, byte[] body, PublishOptions options) throws IOException, JetStreamApiException {
        return publishSyncInternal(subject, null, body, options, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(String subject, Headers headers, byte[] body, PublishOptions options) throws IOException, JetStreamApiException {
        return publishSyncInternal(subject, headers, body, options, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(Message message) throws IOException, JetStreamApiException {
        validateNotNull(message, "Message");
        return publishSyncInternal(message.getSubject(), message.getHeaders(), message.getData(), null, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(Message message, PublishOptions options) throws IOException, JetStreamApiException {
        validateNotNull(message, "Message");
        return publishSyncInternal(message.getSubject(), message.getHeaders(), message.getData(), options, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(String subject, byte[] body) {
        return publishAsyncInternal(subject, null, body, null, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(String subject, Headers headers, byte[] body) {
        return publishAsyncInternal(subject, headers, body, null, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(String subject, byte[] body, PublishOptions options) {
        return publishAsyncInternal(subject, null, body, options, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(String subject, Headers headers, byte[] body, PublishOptions options) {
        return publishAsyncInternal(subject, headers, body, options, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(Message message) {
        validateNotNull(message, "Message");
        return publishAsyncInternal(message.getSubject(), message.getHeaders(), message.getData(), null, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(Message message, PublishOptions options) {
        validateNotNull(message, "Message");
        return publishAsyncInternal(message.getSubject(), message.getHeaders(), message.getData(), options, false);
    }

    private PublishAck publishSyncInternal(String subject, Headers headers, byte[] data, PublishOptions options, boolean validateSubjectAndReplyTo) throws IOException, JetStreamApiException {
        Headers merged = mergePublishOptions(headers, options);

        if (jso.isPublishNoAck()) {
            conn.publishInternal(subject, null, merged, data, validateSubjectAndReplyTo, false);
            return null;
        }

        Message resp = makeInternalRequestResponseRequired(subject, merged, data, getTimeout(), CancelAction.COMPLETE, validateSubjectAndReplyTo, conn.forceFlushOnRequest);
        return processPublishResponse(resp, options);
    }

    private CompletableFuture<PublishAck> publishAsyncInternal(String subject, Headers headers, byte[] data, PublishOptions options, boolean validateSubjectAndReplyTo) {
        Headers merged = mergePublishOptions(headers, options);

        if (jso.isPublishNoAck()) {
            conn.publishInternal(subject, null, merged, data, validateSubjectAndReplyTo, false);
            return null;
        }

        CompletableFuture<Message> future = conn.requestFutureInternal(subject, merged, data, null, CancelAction.COMPLETE, validateSubjectAndReplyTo, conn.forceFlushOnRequest);

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
    MessageManagerFactory _pullOrderedMessageManagerFactory =
        (mmConn, mmJs, mmStream, mmSo, mmCc, mmQueueMode, mmSyncMode) -> new PullOrderedMessageManager(mmConn, mmJs, mmStream, mmSo, mmCc, mmSyncMode);

    JetStreamSubscription createSubscription(String userSubscribeSubject,
                                             PushSubscribeOptions pushSubscribeOptions,
                                             PullSubscribeOptions pullSubscribeOptions,
                                             String queueName,
                                             NatsDispatcher dispatcher,
                                             MessageHandler userHandler,
                                             boolean isAutoAck,
                                             PullMessageManager pmmInstance) throws IOException, JetStreamApiException {

        // Parameter notes. For those relating to the callers, you can see all the callers further down in this source file.
        //    - pull subscribe callers guarantee that pullSubscribeOptions is not null
        //    - qgroup is always null with pull callers
        //    - callers only ever provide one of the subscribe options

        // 1. Initial prep and validation
        boolean isPullMode = pullSubscribeOptions != null;

        SubscribeOptions so;
        String stream;
        ConsumerConfiguration userCC;
        String settledDeliverGroup = null; // push might set this

        if (isPullMode) {
            so = pullSubscribeOptions; // options must have already been checked to be non-null
            stream = pullSubscribeOptions.getStream();

            userCC = so.getConsumerConfiguration();

            validateNotSupplied(userCC.getDeliverGroup(), JsSubPullCantHaveDeliverGroup);
            validateNotSupplied(userCC.getDeliverSubject(), JsSubPullCantHaveDeliverSubject);
        }
        else {
            so = pushSubscribeOptions == null ? DEFAULT_PUSH_OPTS : pushSubscribeOptions;
            stream = so.getStream();

            userCC = so.getConsumerConfiguration();

            if (userCC.maxPullWaitingWasSet()) { throw JsSubPushCantHaveMaxPullWaiting.instance(); }
            if (userCC.maxBatchWasSet())       { throw JsSubPushCantHaveMaxBatch.instance(); }
            if (userCC.maxBytesWasSet())       { throw JsSubPushCantHaveMaxBytes.instance(); }

            // figure out the queue name
            settledDeliverGroup = validateMustMatchIfBothSupplied(userCC.getDeliverGroup(), queueName, JsSubQueueDeliverGroupMismatch);
            if (so.isOrdered() && settledDeliverGroup != null) {
                throw JsSubOrderedNotAllowOnQueues.instance();
            }

            if (dispatcher != null &&
                (so.getPendingMessageLimit() != Consumer.DEFAULT_MAX_MESSAGES ||
                    so.getPendingByteLimit() != Consumer.DEFAULT_MAX_BYTES))
            {
                throw JsSubPushAsyncCantSetPending.instance();
            }
        }

        // 1B. Flow Control / heartbeat not always valid
        if (userCC.getIdleHeartbeat() != null && userCC.getIdleHeartbeat().toMillis() > 0) {
            if (isPullMode) {
                throw JsSubFcHbNotValidPull.instance();
            }
            if (settledDeliverGroup != null) {
                throw JsSubFcHbNotValidQueue.instance();
            }
        }

        // 2. figure out user provided subjects and prepare the settledFilterSubjects
        userSubscribeSubject = emptyAsNull(userSubscribeSubject);
        List<String> settledFilterSubjects = new ArrayList<>();
        if (userCC.getFilterSubjects() == null) { // empty filterSubjects gives null
            // userCC.filterSubjects empty, populate settledFilterSubjects w/userSubscribeSubject if possible
            if (userSubscribeSubject != null) {
                settledFilterSubjects.add(userSubscribeSubject);
            }
        }
        else {
            // userCC.filterSubjects not empty, validate them
            settledFilterSubjects.addAll(userCC.getFilterSubjects());
            // If userSubscribeSubject is provided it must be one of the filter subjects.
            if (userSubscribeSubject != null && !settledFilterSubjects.contains(userSubscribeSubject)) {
                throw JsSubSubjectDoesNotMatchFilter.instance();
            }
        }

        // 3. Did they tell me what stream? No? look it up.
        final String settledStream;
        if (stream == null) {
            if (settledFilterSubjects.isEmpty()) {
                throw JsSubSubjectNeededToLookupStream.instance();
            }
            settledStream = lookupStreamBySubject(settledFilterSubjects.get(0));
            if (settledStream == null) {
                throw JsSubNoMatchingStreamForSubject.instance();
            }
        }
        else {
            settledStream = stream;
        }

        ConsumerConfiguration serverCC = null;
        String consumerName = userCC.getDurable();
        if (consumerName == null) {
            consumerName = userCC.getName();
        }
        String inboxDeliver = userCC.getDeliverSubject();

        // 4. Does this consumer already exist? FastBind bypasses the lookup;
        //    the dev better know what they are doing...
        if (!so.isFastBind() && consumerName != null) {
            ConsumerInfo serverInfo = lookupConsumerInfo(settledStream, consumerName);

            if (serverInfo != null) { // the consumer for that durable already exists
                serverCC = serverInfo.getConsumerConfiguration();

                // check to see if the user sent a different version than the server has
                // because modifications are not allowed during create subscription
                ConsumerConfigurationComparer userCCC = new ConsumerConfigurationComparer(userCC);
                List<String> changes = userCCC.getChanges(serverCC);
                if (!changes.isEmpty()) {
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
                    if (settledDeliverGroup == null) {
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
                else if (settledDeliverGroup == null) {
                    throw JsSubExistingConsumerIsQueue.instance();
                }
                else if (!serverCC.getDeliverGroup().equals(settledDeliverGroup)) {
                    throw JsSubExistingQueueDoesNotMatchRequestedQueue.instance();
                }

                // consumer already exists, make sure the filter subject matches
                // subscribeSubject, if supplied came from the user directly
                // or in the userCC or might not have been in either place
                if (settledFilterSubjects.isEmpty()) {
                    // still also might be null, which the server treats as >
                    if (serverCC.getFilterSubjects() != null) {
                        settledFilterSubjects = serverCC.getFilterSubjects();
                    }
                }
                else if (!listsAreEquivalent(settledFilterSubjects, serverCC.getFilterSubjects())) {
                    throw JsSubSubjectDoesNotMatchFilter.instance();
                }

                inboxDeliver = serverCC.getDeliverSubject(); // use the deliver subject as the inbox. It may be null, that's ok, we'll fix that later
            }
            else if (so.isBind()) {
                throw JsSubConsumerNotFoundRequiredInBind.instance();
            }
        }

        // 5. If pull or no deliver subject (inbox) provided or found, make an inbox.
        final String settledInboxDeliver;
        if (isPullMode) {
            settledInboxDeliver = conn.createInbox() + ".*";
        }
        else if (inboxDeliver == null) {
            settledInboxDeliver = conn.createInbox();
        }
        else {
            settledInboxDeliver = inboxDeliver;
        }

        // 6. If consumer does not exist, create and settle on the config. Name will have to wait
        //    If the consumer exists, I know what the settled info is
        final ConsumerConfiguration settledCC;
        final String settledConsumerName;
        if (so.isFastBind() || serverCC != null) {
            settledCC = serverCC;
            settledConsumerName = so.getName(); // will never be null in this case
        }
        else {
            ConsumerConfiguration.Builder ccBuilder = ConsumerConfiguration.builder(userCC);

            // Pull mode doesn't maintain a deliver subject. It's actually an error if we send it.
            if (!isPullMode) {
                ccBuilder.deliverSubject(settledInboxDeliver);
            }

            // userCC.filterSubjects might have originally been empty
            // but there might have been a userSubscribeSubject,
            // so this makes sure it's resolved either way
            ccBuilder.filterSubjects(settledFilterSubjects);

            ccBuilder.deliverGroup(settledDeliverGroup);

            settledCC = ccBuilder.build();
            settledConsumerName = null; // the server will give us a name
        }

        // 7. create the subscription. lambda needs final or effectively final vars
        final MessageManager mm;
        final NatsSubscriptionFactory subFactory;
        if (isPullMode) {
            if (pmmInstance == null) {
                MessageManagerFactory mmFactory = so.isOrdered() ? _pullOrderedMessageManagerFactory : _pullMessageManagerFactory;
                mm = mmFactory.createMessageManager(conn, this, settledStream, so, settledCC, false, dispatcher == null);
            }
            else {
                mm = pmmInstance;
            }
            subFactory = (sid, lSubject, lQgroup, lConn, lDispatcher)
                -> new NatsJetStreamPullSubscription(sid, lSubject, lConn, lDispatcher, this, settledStream, settledConsumerName, mm);
        }
        else {
            MessageManagerFactory mmFactory = so.isOrdered() ? _pushOrderedMessageManagerFactory : _pushMessageManagerFactory;
            mm = mmFactory.createMessageManager(conn, this, settledStream, so, settledCC, settledDeliverGroup != null, dispatcher == null);
            subFactory = (sid, lSubject, lQgroup, lConn, lDispatcher) -> {
                NatsJetStreamSubscription nsub = new NatsJetStreamSubscription(sid, lSubject, lQgroup, lConn, lDispatcher,
                    this, settledStream, settledConsumerName, mm);
                if (lDispatcher == null) {
                    nsub.setPendingLimits(so.getPendingMessageLimit(), so.getPendingByteLimit());
                }
                return nsub;
            };
        }
        NatsJetStreamSubscription sub;
        if (dispatcher == null) {
            sub = (NatsJetStreamSubscription) conn.createSubscription(settledInboxDeliver, settledDeliverGroup, null, subFactory);
        }
        else {
            AsyncMessageHandler handler = new AsyncMessageHandler(mm, userHandler, isAutoAck, settledCC);
            sub = (NatsJetStreamSubscription) dispatcher.subscribeImplJetStream(settledInboxDeliver, settledDeliverGroup, handler, subFactory);
        }

        // 8. The consumer might need to be created, do it here
        if (settledConsumerName == null) {
            _createConsumerUnsubscribeOnException(settledStream, settledCC, sub);
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

            if (deliverPolicy != null && deliverPolicy != serverCcc.getDeliverPolicy()) { changes.add("deliverPolicy"); }
            if (ackPolicy != null && ackPolicy != serverCcc.getAckPolicy()) { changes.add("ackPolicy"); }
            if (replayPolicy != null && replayPolicy != serverCcc.getReplayPolicy()) { changes.add("replayPolicy"); }

            if (flowControl != null && flowControl != serverCcc.isFlowControl()) { changes.add("flowControl"); }
            if (headersOnly != null && headersOnly != serverCcc.isHeadersOnly()) { changes.add("headersOnly"); }
            if (memStorage != null && memStorage != serverCcc.isMemStorage()) { changes.add("memStorage"); }

            if (startSeq != null && !startSeq.equals(serverCcc.getStartSequence())) { changes.add("startSequence"); }
            if (rateLimit != null && !rateLimit.equals(serverCcc.getRateLimit())) { changes.add("rateLimit"); }

            if (maxDeliver != null && maxDeliver != serverCcc.getMaxDeliver()) { changes.add("maxDeliver"); }
            if (maxAckPending != null && maxAckPending != serverCcc.getMaxAckPending()) { changes.add("maxAckPending"); }
            if (maxPullWaiting != null && maxPullWaiting != serverCcc.getMaxPullWaiting()) { changes.add("maxPullWaiting"); }
            if (maxBatch != null && maxBatch != serverCcc.getMaxBatch()) { changes.add("maxBatch"); }
            if (maxBytes != null && maxBytes != serverCcc.getMaxBytes()) { changes.add("maxBytes"); }
            if (numReplicas != null && !numReplicas.equals(serverCcc.numReplicas)) { changes.add("numReplicas"); }
            if (pauseUntil != null && !pauseUntil.equals(serverCcc.pauseUntil)) { changes.add("pauseUntil"); }

            if (ackWait != null && !ackWait.equals(getOrUnset(serverCcc.ackWait))) { changes.add("ackWait"); }
            if (idleHeartbeat != null && !idleHeartbeat.equals(getOrUnset(serverCcc.idleHeartbeat))) { changes.add("idleHeartbeat"); }
            if (maxExpires != null && !maxExpires.equals(getOrUnset(serverCcc.maxExpires))) { changes.add("maxExpires"); }
            if (inactiveThreshold != null && !inactiveThreshold.equals(getOrUnset(serverCcc.inactiveThreshold))) { changes.add("inactiveThreshold"); }

            if (startTime != null && !startTime.equals(serverCcc.startTime)) { changes.add("startTime"); }

            if (description != null && !description.equals(serverCcc.description)) { changes.add("description"); }
            if (sampleFrequency != null && !sampleFrequency.equals(serverCcc.sampleFrequency)) { changes.add("sampleFrequency"); }
            if (deliverSubject != null && !deliverSubject.equals(serverCcc.deliverSubject)) { changes.add("deliverSubject"); }
            if (deliverGroup != null && !deliverGroup.equals(serverCcc.deliverGroup)) { changes.add("deliverGroup"); }

            if (backoff != null && !listsAreEquivalent(backoff, serverCcc.backoff)) { changes.add("backoff"); }
            if (metadata != null && !mapsAreEquivalent(metadata, serverCcc.metadata)) { changes.add("metadata"); }
            if (filterSubjects != null && !listsAreEquivalent(filterSubjects, serverCcc.filterSubjects)) { changes.add("filterSubjects"); }

            if (priorityGroups != null && !listsAreEquivalent(priorityGroups, serverCcc.priorityGroups)) { changes.add("priorityGroups"); }
            if (priorityPolicy != null && priorityPolicy != serverCcc.getPriorityPolicy()) { changes.add("priorityPolicy"); }

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

    private String lookupStreamSubject(String stream) throws IOException, JetStreamApiException {
        StreamInfo si = _getStreamInfo(stream, null);
        List<String> streamSubjects = si.getConfiguration().getSubjects();
        return streamSubjects.size() == 1 ? streamSubjects.get(0) : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subscribeSubject) throws IOException, JetStreamApiException {
        subscribeSubject = validateSubject(subscribeSubject, true);
        return createSubscription(subscribeSubject, null, null, null, null, null, false, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subscribeSubject, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        subscribeSubject = validateSubject(subscribeSubject, false);
        return createSubscription(subscribeSubject, options, null, null, null, null, false, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subscribeSubject, String queue, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        subscribeSubject = validateSubject(subscribeSubject, false);
        validateQueueName(queue, false);
        return createSubscription(subscribeSubject, options, null, queue, null, null, false, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subscribeSubject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException {
        subscribeSubject = validateSubject(subscribeSubject, false);
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subscribeSubject, null, null, null, (NatsDispatcher) dispatcher, handler, autoAck, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subscribeSubject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        subscribeSubject = validateSubject(subscribeSubject, false);
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subscribeSubject, options, null, null, (NatsDispatcher) dispatcher, handler, autoAck, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subscribeSubject, String queue, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        subscribeSubject = validateSubject(subscribeSubject, false);
        validateQueueName(queue, false);
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subscribeSubject, options, null, queue, (NatsDispatcher) dispatcher, handler, autoAck, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subscribeSubject, PullSubscribeOptions options) throws IOException, JetStreamApiException {
        subscribeSubject = validateSubject(subscribeSubject, false);
        validateNotNull(options, "Pull Subscribe Options");
        return createSubscription(subscribeSubject, null, options, null, null, null, false, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subscribeSubject, Dispatcher dispatcher, MessageHandler handler, PullSubscribeOptions options) throws IOException, JetStreamApiException {
        subscribeSubject = validateSubject(subscribeSubject, false);
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        validateNotNull(options, "Pull Subscribe Options");
        return createSubscription(subscribeSubject, null, options, null, (NatsDispatcher) dispatcher, handler, false, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamContext getStreamContext(String streamName) throws IOException, JetStreamApiException {
        Validator.validateStreamName(streamName, true);
        return getNatsStreamContext(streamName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerContext getConsumerContext(String streamName, String consumerName) throws IOException, JetStreamApiException {
        Validator.validateStreamName(streamName, true);
        Validator.required(consumerName, "Consumer Name");
        return getNatsStreamContext(streamName).getConsumerContext(consumerName);
    }

    private NatsStreamContext getNatsStreamContext(String streamName) throws IOException, JetStreamApiException {
        return new NatsStreamContext(streamName, this, conn, jso);
    }
}
