package io.nats.client.support;

public interface NatsJetStreamConstants {
    /**
     * The maximum pull size
     */
    int MAX_PULL_SIZE = 256;

    String PREFIX_DOLLAR_JS_DOT = "$JS.";
    String PREFIX_API_DOT = "API.";
    String DEFAULT_API_PREFIX = PREFIX_DOLLAR_JS_DOT + PREFIX_API_DOT;
    String JS_ACK_SUBJECT_PREFIX = PREFIX_DOLLAR_JS_DOT + "ACK.";

    // JSAPI_ACCOUNT_INFO is for obtaining general information about JetStream.
    String JSAPI_ACCOUNT_INFO = "INFO";

    // JSAPI_CONSUMER_CREATE is used to create consumers.
    String JSAPI_CONSUMER_CREATE = "CONSUMER.CREATE.%s";

    // JSAPI_DURABLE_CREATE is used to create durable consumers.
    String JSAPI_DURABLE_CREATE = "CONSUMER.DURABLE.CREATE.%s.%s";

    // JSAPI_CONSUMER_INFO is used to create consumers.
    String JSAPI_CONSUMER_INFO = "CONSUMER.INFO.%s.%s";

    // JSAPI_CONSUMER_MSG_NEXT is the prefix for the request next message(s) for a consumer in worker/pull mode.
    String JSAPI_CONSUMER_MSG_NEXT = "CONSUMER.MSG.NEXT.%s.%s";

    // JSAPI_CONSUMER_DELETE is used to delete consumers.
    String JSAPI_CONSUMER_DELETE = "CONSUMER.DELETE.%s.%s";

    // JSAPI_CONSUMER_NAMES is used to return a list of consumer names
    String JSAPI_CONSUMER_NAMES = "CONSUMER.NAMES.%s";

    // JSAPI_CONSUMER_LIST is used to return all detailed consumer information
    String JSAPI_CONSUMER_LIST = "CONSUMER.LIST.%s";

    // JSAPI_STREAM_CREATE is the endpoint to create new streams.
    String JSAPI_STREAM_CREATE = "STREAM.CREATE.%s";

    // JSAPI_STREAM_INFO is the endpoint to get information on a stream.
    String JSAPI_STREAM_INFO = "STREAM.INFO.%s";

    // JSAPI_STREAM_UPDATE is the endpoint to update existing streams.
    String JSAPI_STREAM_UPDATE = "STREAM.UPDATE.%s";

    // JSAPI_STREAM_DELETE is the endpoint to delete streams.
    String JSAPI_STREAM_DELETE = "STREAM.DELETE.%s";

    // JSAPI_STREAM_PURGE is the endpoint to purge streams.
    String JSAPI_STREAM_PURGE = "STREAM.PURGE.%s";

    // JSAPI_STREAM_NAMES is the endpoint that will return a list of stream names
    String JSAPI_STREAM_NAMES = "STREAM.NAMES";

    // JSAPI_STREAM_LIST is the endpoint that will return all detailed stream information
    String JSAPI_STREAM_LIST = "STREAM.LIST";

    // JSAPI_MSG_GET is the endpoint to get a message.
    String JSAPI_MSG_GET = "STREAM.MSG.GET.%s";

    // JSAPI_MSG_DELETE is the endpoint to remove a message.
    String JSAPI_MSG_DELETE = "STREAM.MSG.DELETE.%s";

    String MSG_ID_HDR = "Nats-Msg-Id";
    String EXPECTED_STREAM_HDR = "Nats-Expected-Stream";
    String EXPECTED_LAST_SEQ_HDR = "Nats-Expected-Last-Sequence";
    String EXPECTED_LAST_MSG_ID_HDR = "Nats-Expected-Last-Msg-Id";
    String EXPECTED_LAST_SUB_SEQ_HDR = "Nats-Expected-Last-Subject-Sequence";

    int JS_CONSUMER_NOT_FOUND_ERR = 10014;
    int JS_NO_MESSAGE_FOUND_ERR = 10037;
}
