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
import io.nats.client.api.Error;
import io.nats.client.api.*;
import io.nats.client.support.Status;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static io.nats.client.support.NatsJetStreamClientError.JsAllowDirectRequired;
import static io.nats.client.support.NatsJetStreamClientError.JsDirectBatchGet211NotAvailable;
import static io.nats.client.support.Validator.*;

public class NatsJetStreamManagement extends NatsJetStreamImpl implements JetStreamManagement {
    private NatsJetStream js; // this is lazy init'ed

    public NatsJetStreamManagement(NatsConnection connection, JetStreamOptions jsOptions) throws IOException {
        super(connection, jsOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AccountStatistics getAccountStatistics() throws IOException, JetStreamApiException {
        Message resp = makeRequestResponseRequired(JSAPI_ACCOUNT_INFO, null, getTimeout());
        return new AccountStatistics(resp).throwOnHasError();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo addStream(StreamConfiguration config) throws IOException, JetStreamApiException {
        return addOrUpdateStream(config, JSAPI_STREAM_CREATE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo updateStream(StreamConfiguration config) throws IOException, JetStreamApiException {
        return addOrUpdateStream(config, JSAPI_STREAM_UPDATE);
    }

    private StreamInfo addOrUpdateStream(StreamConfiguration config, String template) throws IOException, JetStreamApiException {
        validateNotNull(config, "Configuration");
        String streamName = config.getName();
        if (nullOrEmpty(streamName)) {
            throw new IllegalArgumentException("Configuration must have a valid stream name");
        }

        String subj = String.format(template, streamName);
        Message resp = makeRequestResponseRequired(subj, config.toJson().getBytes(StandardCharsets.UTF_8), getTimeout());
        return createAndCacheStreamInfoThrowOnError(streamName, resp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteStream(String streamName) throws IOException, JetStreamApiException {
        validateNotNull(streamName, "Stream Name");
        String subj = String.format(JSAPI_STREAM_DELETE, streamName);
        Message resp = makeRequestResponseRequired(subj, null, getTimeout());
        return new SuccessApiResponse(resp).throwOnHasError().getSuccess();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo getStreamInfo(String streamName) throws IOException, JetStreamApiException {
        validateNotNull(streamName, "Stream Name");
        return _getStreamInfo(streamName, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo getStreamInfo(String streamName, StreamInfoOptions options) throws IOException, JetStreamApiException {
        validateNotNull(streamName, "Stream Name");
        return _getStreamInfo(streamName, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PurgeResponse purgeStream(String streamName) throws IOException, JetStreamApiException {
        validateNotNull(streamName, "Stream Name");
        String subj = String.format(JSAPI_STREAM_PURGE, streamName);
        Message resp = makeRequestResponseRequired(subj, null, getTimeout());
        return new PurgeResponse(resp).throwOnHasError();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PurgeResponse purgeStream(String streamName, PurgeOptions options) throws IOException, JetStreamApiException {
        validateNotNull(streamName, "Stream Name");
        validateNotNull(options, "Purge Options");
        String subj = String.format(JSAPI_STREAM_PURGE, streamName);
        byte[] body = options.toJson().getBytes(StandardCharsets.UTF_8);
        Message resp = makeRequestResponseRequired(subj, body, getTimeout());
        return new PurgeResponse(resp).throwOnHasError();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo addOrUpdateConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException {
        validateStreamName(streamName, true);
        validateNotNull(config, "Config");
        return _createConsumer(streamName, config, ConsumerCreateRequest.Action.CreateOrUpdate);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo createConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException {
        validateStreamName(streamName, true);
        validateNotNull(config, "Config");
        return _createConsumer(streamName, config, ConsumerCreateRequest.Action.Create);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo updateConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException {
        validateStreamName(streamName, true);
        validateNotNull(config, "Config");
        return _createConsumer(streamName, config, ConsumerCreateRequest.Action.Update);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteConsumer(String streamName, String consumerName) throws IOException, JetStreamApiException {
        validateNotNull(streamName, "Stream Name");
        validateNotNull(consumerName, "Consumer Name");
        String subj = String.format(JSAPI_CONSUMER_DELETE, streamName, consumerName);
        Message resp = makeRequestResponseRequired(subj, null, getTimeout());
        return new SuccessApiResponse(resp).throwOnHasError().getSuccess();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerPauseResponse pauseConsumer(String streamName, String consumerName, ZonedDateTime pauseUntil) throws IOException, JetStreamApiException {
        validateNotNull(streamName, "Stream Name");
        validateNotNull(consumerName, "Consumer Name");
        String subj = String.format(JSAPI_CONSUMER_PAUSE, streamName, consumerName);
        ConsumerPauseRequest pauseRequest = new ConsumerPauseRequest(pauseUntil);
        Message resp = makeRequestResponseRequired(subj, pauseRequest.serialize(), getTimeout());
        return new ConsumerPauseResponse(resp).throwOnHasError();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean resumeConsumer(String streamName, String consumerName) throws IOException, JetStreamApiException {
        validateNotNull(streamName, "Stream Name");
        validateNotNull(consumerName, "Consumer Name");
        String subj = String.format(JSAPI_CONSUMER_PAUSE, streamName, consumerName);
        Message resp = makeRequestResponseRequired(subj, null, getTimeout());
        ConsumerPauseResponse response = new ConsumerPauseResponse(resp).throwOnHasError();
        return !response.isPaused();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo(String streamName, String consumerName) throws IOException, JetStreamApiException {
        return super._getConsumerInfo(streamName, consumerName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getConsumerNames(String streamName) throws IOException, JetStreamApiException {
        return getConsumerNames(streamName, null);
    }

    // TODO FUTURE resurface this api publicly when server supports
    // @Override
    private List<String> getConsumerNames(String streamName, String filter) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_NAMES, streamName);
        ConsumerNamesReader cnr = new ConsumerNamesReader();
        while (cnr.hasMore()) {
            Message resp = makeRequestResponseRequired(subj, cnr.nextJson(filter), getTimeout());
            cnr.process(resp);
        }
        return cnr.getStrings();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ConsumerInfo> getConsumers(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_LIST, streamName);
        ConsumerListReader clg = new ConsumerListReader();
        while (clg.hasMore()) {
            Message resp = makeRequestResponseRequired(subj, clg.nextJson(), getTimeout());
            clg.process(resp);
        }
        return clg.getConsumers();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getStreamNames() throws IOException, JetStreamApiException {
        return _getStreamNames(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getStreamNames(String subjectFilter) throws IOException, JetStreamApiException {
        return _getStreamNames(subjectFilter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<StreamInfo> getStreams() throws IOException, JetStreamApiException {
        return getStreams(null);
    }

    @Override
    public List<StreamInfo> getStreams(String subjectFilter) throws IOException, JetStreamApiException {
        StreamListReader slr = new StreamListReader();
        while (slr.hasMore()) {
            Message resp = makeRequestResponseRequired(JSAPI_STREAM_LIST, slr.nextJson(subjectFilter), getTimeout());
            slr.process(resp);
        }
        return cacheStreamInfo(slr.getStreams());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageInfo getMessage(String streamName, long seq) throws IOException, JetStreamApiException {
        return _getMessage(streamName, MessageGetRequest.forSequence(seq));
    }

    @Override
    public MessageInfo getMessage(String streamName, MessageGetRequest messageGetRequest) throws IOException, JetStreamApiException {
        return _getMessage(streamName, messageGetRequest);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageInfo getLastMessage(String streamName, String subject) throws IOException, JetStreamApiException {
        return _getMessage(streamName, MessageGetRequest.lastForSubject(subject));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageInfo getFirstMessage(String streamName, String subject) throws IOException, JetStreamApiException {
        return _getMessage(streamName, MessageGetRequest.firstForSubject(subject));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageInfo getFirstMessage(String streamName, ZonedDateTime startTime) throws IOException, JetStreamApiException {
        return _getMessage(streamName, MessageGetRequest.firstForStartTime(startTime));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageInfo getFirstMessage(String streamName, ZonedDateTime startTime, String subject) throws IOException, JetStreamApiException {
        return _getMessage(streamName, MessageGetRequest.firstForStartTimeAndSubject(startTime, subject));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageInfo getNextMessage(String streamName, long seq, String subject) throws IOException, JetStreamApiException {
        return _getMessage(streamName, MessageGetRequest.nextForSubject(seq, subject));
    }

    private MessageInfo _getMessage(String streamName, MessageGetRequest messageGetRequest) throws IOException, JetStreamApiException {
        validateNotNull(messageGetRequest, "Message Get Request");
        CachedStreamInfo csi = getCachedStreamInfo(streamName);
        if (csi.allowDirect) {
            String subject;
            byte[] payload;
            if (messageGetRequest.isLastBySubject()) {
                subject = String.format(JSAPI_DIRECT_GET_LAST, streamName, messageGetRequest.getLastBySubject());
                payload = null;
            }
            else{
                subject = String.format(JSAPI_DIRECT_GET, streamName);
                payload = messageGetRequest.serialize();
            }
            Message resp = makeRequestResponseRequired(subject, payload, getTimeout());
            if (resp.isStatusMessage()) {
                throw new JetStreamApiException(Error.convert(resp.getStatus()));
            }
            return new MessageInfo(resp, streamName, true);
        }
        else {
            String getSubject = String.format(JSAPI_MSG_GET, streamName);
            Message resp = makeRequestResponseRequired(getSubject, messageGetRequest.serialize(), getTimeout());
            return new MessageInfo(resp, streamName, false).throwOnHasError();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<MessageInfo> fetchMessageBatch(String streamName, MessageBatchGetRequest messageBatchGetRequest) throws IOException, JetStreamApiException {
        validateMessageBatchGetRequest(streamName, messageBatchGetRequest);
        final List<MessageInfo> results = new ArrayList<>();
        _requestMessageBatch(streamName, messageBatchGetRequest, false, mi -> {
            if (mi.isErrorStatus()) {
                results.clear();
            }
            results.add(mi);
        });
        return results;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LinkedBlockingQueue<MessageInfo> queueMessageBatch(String streamName, MessageBatchGetRequest messageBatchGetRequest) throws IOException, JetStreamApiException {
        validateMessageBatchGetRequest(streamName, messageBatchGetRequest);
        final LinkedBlockingQueue<MessageInfo> q = new LinkedBlockingQueue<>();
        conn.getOptions().getExecutor().submit(
            () -> _requestMessageBatch(streamName, messageBatchGetRequest, true, q::add));
        return q;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requestMessageBatch(String streamName, MessageBatchGetRequest messageBatchGetRequest, MessageInfoHandler handler) throws IOException, JetStreamApiException {
        validateMessageBatchGetRequest(streamName, messageBatchGetRequest);
        return _requestMessageBatch(streamName, messageBatchGetRequest, true, handler);
    }

    private boolean _requestMessageBatch(String streamName, MessageBatchGetRequest mbgr, boolean sendEob, MessageInfoHandler handler) {
        Subscription sub = null;

        try {
            String replyTo = conn.createInbox();
            sub = conn.subscribe(replyTo);

            String subject = prependPrefix(String.format(JSAPI_DIRECT_GET, streamName));
            conn.publish(subject, replyTo, mbgr.serialize());

            while (true) {
                Message msg = sub.nextMessage(getTimeout());
                Status errorOrNonEob = null;
                if (msg == null) {
                    errorOrNonEob = Status.TIMEOUT_OR_NO_MESSAGES;
                }
                else if (msg.isStatusMessage()) {
                    if (msg.getStatus().isEob()) {
                        return true;  // will send eob in finally if caller asked
                    }
                    errorOrNonEob = msg.getStatus();
                }

                if (errorOrNonEob != null) {
                    // All error or non eob statuses, always send, but it is the last message to the caller
                    sendEob = false;
                    handler.onMessageInfo(new MessageInfo(errorOrNonEob, streamName));
                    return false; // should not time out before eob
                }

                MessageInfo messageInfo = new MessageInfo(msg, streamName, true);
                handler.onMessageInfo(messageInfo);
            }
        }
        catch (InterruptedException e) {
            // sub.nextMessage was fetching one message
            // and data is not completely read
            // so it seems like this is an error condition
            Thread.currentThread().interrupt();
            sendEob = false;
            return false;
        } finally {
            if (sendEob) {
                try {
                    handler.onMessageInfo(new MessageInfo(Status.EOB, streamName));
                }
                catch (RuntimeException ignore) { /* user handler runtime error */ }
            }
            try {
                //noinspection DataFlowIssue
                sub.unsubscribe();
            } catch (RuntimeException ignore) { /* don't want this to fail here */ }
        }
    }

    private void validateMessageBatchGetRequest(String streamName, MessageBatchGetRequest messageBatchGetRequest) throws IOException, JetStreamApiException {
        validateNotNull(messageBatchGetRequest, "Message Batch Get Request");

        if (!directBatchGet211Available) {
            throw JsDirectBatchGet211NotAvailable.instance();
        }

        CachedStreamInfo csi = getCachedStreamInfo(streamName);
        if (!csi.allowDirect) {
            throw JsAllowDirectRequired.instance();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteMessage(String streamName, long seq) throws IOException, JetStreamApiException {
        return deleteMessage(streamName, seq, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteMessage(String streamName, long seq, boolean erase) throws IOException, JetStreamApiException {
        validateNotNull(streamName, "Stream Name");
        String subj = String.format(JSAPI_MSG_DELETE, streamName);
        MessageDeleteRequest mdr = new MessageDeleteRequest(seq, erase);
        Message resp = makeRequestResponseRequired(subj, mdr.serialize(), getTimeout());
        return new SuccessApiResponse(resp).throwOnHasError().getSuccess();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStream jetStream() {
        if (js == null) {
            js = new NatsJetStream(this);
        }
        return js;
    }
}
