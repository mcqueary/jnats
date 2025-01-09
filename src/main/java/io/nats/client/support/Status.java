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

package io.nats.client.support;

import java.util.HashMap;
import java.util.Map;

public class Status {

    public static final String FLOW_CONTROL_TEXT = "FlowControl Request";
    public static final String HEARTBEAT_TEXT = "Idle Heartbeat";
    public static final String NO_RESPONDERS_TEXT = "No Responders Available For Request";
    public static final String EOB_TEXT = "EOB";
    public static final int FLOW_OR_HEARTBEAT_STATUS_CODE = 100;
    public static final int NO_RESPONDERS_CODE = 503;
    public static final int BAD_REQUEST_CODE = 400;
    public static final int NOT_FOUND_CODE = 404;
    public static final int BAD_JS_REQUEST_CODE = 408;
    public static final int REQUEST_TIMEOUT_CODE = BAD_JS_REQUEST_CODE; // only left in for b/w compat
    public static final int CONFLICT_CODE = 409;
    public static final int EOB_CODE = 204;

    public static String BAD_REQUEST                    = "Bad Request"; // 400
    public static String NO_MESSAGES                    = "No Messages"; // 404
    public static String CONSUMER_DELETED               = "Consumer Deleted"; // 409
    public static String CONSUMER_IS_PUSH_BASED         = "Consumer is push based"; // 409

    public static String MESSAGE_SIZE_EXCEEDS_MAX_BYTES = "Message Size Exceeds MaxBytes"; // 409
    public static String EXCEEDED_MAX_WAITING           = "Exceeded MaxWaiting"; // 409
    public static String EXCEEDED_MAX_REQUEST_BATCH     = "Exceeded MaxRequestBatch"; // 409
    public static String EXCEEDED_MAX_REQUEST_EXPIRES   = "Exceeded MaxRequestExpires"; // 409
    public static String EXCEEDED_MAX_REQUEST_MAX_BYTES = "Exceeded MaxRequestMaxBytes"; // 409

    public static String BATCH_COMPLETED                = "Batch Completed"; // 409 informational
    public static String SERVER_SHUTDOWN                = "Server Shutdown"; // 409 informational with headers
    public static String LEADERSHIP_CHANGE              = "Leadership Change"; // 409

    public static final Status EOB = new Status(EOB_CODE, EOB_TEXT);
    public static final Status TIMEOUT_OR_NO_MESSAGES = new Status(NOT_FOUND_CODE, "Timeout or No Messages");

    private final int code;
    private final String message;

    private static final Map<Integer, String> CODE_TO_TEXT;

    static {
        CODE_TO_TEXT = new HashMap<>();
        CODE_TO_TEXT.put(NO_RESPONDERS_CODE, NO_RESPONDERS_TEXT);
    }

    public Status(int code, String message) {
        this.code = code;
        this.message = message == null ? makeMessage(code) : message ;
    }

    public Status(Token codeToken, Token messageToken) {
        this(extractCode(codeToken), extractMessage(messageToken));
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public String getMessageWithCode() {
        return code + " " + message;
    }

    private static String extractMessage(Token messageToken) {
        return messageToken.hasValue() ? messageToken.getValue() : null;
    }

    private static int extractCode(Token codeToken) {
        try {
            return Integer.parseInt(codeToken.getValue());
        }
        catch (Exception e) {
            throw new IllegalArgumentException(NatsConstants.INVALID_HEADER_STATUS_CODE);
        }
    }

    private String makeMessage(int code) {
        String message = CODE_TO_TEXT.get(code);
        return message == null ? "Server Status Message: " + code : message;
    }

    @Override
    public String toString() {
        return "Status{" +
                "code=" + code +
                ", message='" + message + '\'' +
                '}';
    }

    public boolean isFlowControl() {
        return code == FLOW_OR_HEARTBEAT_STATUS_CODE && message.equals(FLOW_CONTROL_TEXT);
    }

    public boolean isHeartbeat() {
        return code == FLOW_OR_HEARTBEAT_STATUS_CODE && message.equals(HEARTBEAT_TEXT);
    }

    public boolean isNoResponders() {
        return code == NO_RESPONDERS_CODE && message.equals(NO_RESPONDERS_TEXT);
    }

    public boolean isEob() {
        return code == EOB_CODE && message.equals(EOB_TEXT);
    }
}
