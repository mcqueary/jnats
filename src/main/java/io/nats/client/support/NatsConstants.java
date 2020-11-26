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

import static java.nio.charset.StandardCharsets.US_ASCII;

public interface NatsConstants {
    String VERSION = "NATS/1.0";
    String SPACE = " ";
    String EMPTY = "";
    String CRLF = "\r\n";

    byte TAB = '\t';
    byte SP = ' ';
    byte COLON = ':';
    byte CR = '\r';
    byte LF = '\n';

    byte[] EMPTY_BODY = new byte[0];
    byte[] VERSION_BYTES = VERSION.getBytes(US_ASCII);
    byte[] VERSION_BYTES_PLUS_CRLF = (VERSION + "\r\n").getBytes(US_ASCII);
    byte[] COLON_BYTES = ":".getBytes(US_ASCII);
    byte[] CRLF_BYTES = CRLF.getBytes(US_ASCII);
    int VERSION_BYTES_LEN = VERSION_BYTES.length;
    int VERSION_BYTES_PLUS_CRLF_LEN = VERSION_BYTES_PLUS_CRLF.length;

    String OP_CONNECT = "CONNECT";
    String OP_INFO = "INFO";
    String OP_SUB = "SUB";
    String OP_PUB = "PUB";
    String OP_HPUB = "HPUB";
    String OP_UNSUB = "UNSUB";
    String OP_MSG = "MSG";
    String OP_HMSG = "HMSG";
    String OP_PING = "PING";
    String OP_PONG = "PONG";
    String OP_OK = "+OK";
    String OP_ERR = "-ERR";
    String UNKNOWN_OP = "UNKNOWN";

    byte[] OP_PING_BYTES = OP_PING.getBytes();
    byte[] OP_PONG_BYTES = OP_PONG.getBytes();

    byte[] PUB_SP_BYTES = (OP_PUB + SPACE).getBytes(US_ASCII);
    byte[] HPUB_SP_BYTES = (OP_HPUB + SPACE).getBytes(US_ASCII);
    byte[] CONNECT_SP_BYTES = (OP_CONNECT + SPACE).getBytes();
    byte[] SUB_SP_BYTES = (OP_SUB + SPACE).getBytes();
    byte[] UNSUB_SP_BYTES = (OP_UNSUB + SPACE).getBytes();

    int OP_CONNECT_SP_LEN = CONNECT_SP_BYTES.length;
    int OP_SUB_SP_LEN = SUB_SP_BYTES.length;
    int OP_UNSUB_SP_LEN = UNSUB_SP_BYTES.length;

    int MAX_PROTOCOL_RECEIVE_OP_LENGTH = 4;

    String INVALID_HEADER_VERSION = "Invalid header version";
    String INVALID_HEADER_COMPOSITION = "Invalid header composition";
    String INVALID_HEADER_STATUS_CODE = "Invalid header status code";
    String SERIALIZED_HEADER_CANNOT_BE_NULL_OR_EMPTY = "Serialized header cannot be null or empty.";

    int STATUS_NO_RESPONDER = 503;
}
