// Copyright 2015-2018 The NATS Authors
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

import io.nats.client.utils.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public final class JsonUtilsTests {

    @Test
    public void testParseStringArray() {
        List<String> a = JsonUtils.getStringList("fieldName", "...\"fieldName\": [\n      ],...");
        assertNotNull(a);
        assertEquals(0, a.size());

        a = JsonUtils.getStringList("fieldName", "...\"fieldName\": [\n      \"value0\"\n    ],...");
        assertNotNull(a);
        assertEquals(1, a.size());
        assertEquals("value0", a.get(0));

        a = JsonUtils.getStringList("fieldName", "...\"fieldName\": [\r\n      \"value0\",\r      \"value1\"\n    ],...");
        assertNotNull(a);
        assertEquals(2, a.size());
        assertEquals("value0", a.get(0));
        assertEquals("value1", a.get(1));
    }

    @Test
    public void testGetJSONObject() {
        // object is there
        String json = "{\"object\": {\"field\": \"val\"}, \"other\":{}}";
        String object = JsonUtils.getJsonObject("object", json);
        assertEquals("{\"field\": \"val\"}", object);

        // object isn't
        json = "{\"other\":{}}";
        object = JsonUtils.getJsonObject("object", json);
        assertEquals(JsonUtils.EMPTY_JSON, object);

        // object there but incomplete
        json = "{\"object\": {\"field\": \"val\"";
        object = JsonUtils.getJsonObject("object", json);
        assertEquals(JsonUtils.EMPTY_JSON, object);

        json = dataAsString("StreamInfo.json");
        object = JsonUtils.getJsonObject("cluster", json);
        assertFalse(object.contains("placementclstr"));
        assertFalse(object.contains("tags"));
        assertFalse(object.contains("messages"));
        assertTrue(object.contains("clustername"));
        assertTrue(object.contains("replicas"));
    }

    @Test
    public void testGetObjectArray() {
        String json = ResourceUtils.dataAsString("ConsumerListResponse.json");
        List<String> list = JsonUtils.getObjectList("consumers", json);
        assertEquals(2, list.size());
    }

    @Test
    public void testBeginEnd() {
        StringBuilder sb = JsonUtils.beginJson();
        JsonUtils.addField(sb, "name", "value");
        JsonUtils.endJson(sb);
        assertEquals("{\"name\":\"value\"}", sb.toString());

        sb = JsonUtils.beginFormattedJson();
        JsonUtils.addField(sb, "name", "value");
        JsonUtils.endFormattedJson(sb);
        assertEquals("{\n    \"name\":\"value\"\n}", sb.toString());

        sb = JsonUtils.beginJsonPrefixed(null);
        assertEquals("{", sb.toString());

        sb = JsonUtils.beginJsonPrefixed("pre");
        assertEquals("pre {", sb.toString());
    }

    @Test
    public void testAddFlds() {
        StringBuilder sb = new StringBuilder();

        JsonUtils.addField(sb, "n/a", (String)null);
        assertEquals(0, sb.length());

        JsonUtils.addField(sb, "n/a", "");
        assertEquals(0, sb.length());

        JsonUtils.addStrings(sb, "n/a", (String[])null);
        assertEquals(0, sb.length());

        JsonUtils.addStrings(sb, "n/a", new String[0]);
        assertEquals(0, sb.length());

        JsonUtils.addStrings(sb, "n/a", (List<String>)null);
        assertEquals(0, sb.length());

        JsonUtils.addField(sb, "n/a", (JsonSerializable)null);
        assertEquals(0, sb.length());

        JsonUtils.addJsons(sb, "n/a", new ArrayList<>());
        assertEquals(0, sb.length());

        JsonUtils.addJsons(sb, "n/a", null);
        assertEquals(0, sb.length());

        JsonUtils.addFldWhenTrue(sb, "n/a", null);
        assertEquals(0, sb.length());

        JsonUtils.addFldWhenTrue(sb, "n/a", false);
        assertEquals(0, sb.length());

        JsonUtils.addField(sb, "lminusone", -1);
        assertEquals(0, sb.length());

        JsonUtils.addFieldAsUnsigned(sb, "uzero", 0);
        assertEquals(10, sb.length());

        JsonUtils.addStrings(sb, "foo", new String[]{"bar"});
        assertEquals(24, sb.length());

        JsonUtils.addField(sb, "zero", 0);
        assertEquals(33, sb.length());

        JsonUtils.addField(sb, "lone", 1);
        assertEquals(42, sb.length());

        JsonUtils.addField(sb, "lmax", Long.MAX_VALUE);
        assertEquals(69, sb.length());
    }

    @Test
    public void testParseDateTime() {
        assertEquals(1611186068, DateTimeUtils.parseDateTime("2021-01-20T23:41:08.579594Z").toEpochSecond());
        assertEquals(1612293508, DateTimeUtils.parseDateTime("2021-02-02T11:18:28.347722551-08:00").toEpochSecond());
        assertEquals(-62135596800L, DateTimeUtils.parseDateTime("anything-not-valid").toEpochSecond());
    }

    @Test
    public void testInteger() {
        Pattern RE = JsonUtils.buildPattern("num", JsonUtils.FieldType.jsonInteger);
        assertEquals(-1, JsonUtils.readInt("\"num\":-1", RE, 0));
        assertEquals(12345678, JsonUtils.readInt("\"num\":12345678", RE, 0));
        assertEquals(2147483647, JsonUtils.readInt("\"num\":2147483647", RE, 0));
    }

    @Test
    public void testLong() {
        Pattern RE = JsonUtils.buildPattern("num", JsonUtils.FieldType.jsonInteger);
        assertEquals(-1, JsonUtils.readLong("\"num\":-1", RE, 0));
        assertEquals(12345678, JsonUtils.readLong("\"num\":12345678", RE, 0));
        assertEquals(9223372036854775807L, JsonUtils.readLong("\"num\":9223372036854775807", RE, 0));

        AtomicLong al = new AtomicLong();
        JsonUtils.readLong("\"num\":999", RE, al::set);
        assertEquals(999, al.get());

        JsonUtils.readLong("\"num\":invalid", RE, al::set);
        assertEquals(999, al.get());

        JsonUtils.readLong("\"num\":18446744073709551615", RE, al::set);
        assertEquals(-1, al.get());
    }

    @Test
    public void testUnsignedLong() {
        Pattern RE = JsonUtils.buildPattern("num", JsonUtils.FieldType.jsonInteger);
        assertEquals("18446744073709551615",
                Long.toUnsignedString(JsonUtils.readUnsignedLong("\"num\":18446744073709551615", RE, 0)));
        assertEquals("18446744073709551614",
                Long.toUnsignedString(JsonUtils.readUnsignedLong("\"num\":18446744073709551614", RE, 0)));
        assertEquals(0, JsonUtils.parseUnsignedLong("18446744073709551616", 0));
        assertEquals(0, JsonUtils.parseUnsignedLong("invalid", 0));
        assertEquals(0, JsonUtils.parseUnsignedLong("-1", 0));
        assertEquals(0, JsonUtils.parseUnsignedLong("0", 1));
        assertEquals(1, JsonUtils.parseUnsignedLong("1", 0));

        // TODO when converting to Ulong
        /*
        Pattern RE = JsonUtils.buildPattern("num", JsonUtils.FieldType.jsonInteger);
        assertEquals(new Ulong("18446744073709551615"),
                JsonUtils.readUnsignedLong("\"num\":18446744073709551615", RE, Ulong.ZERO));
        assertEquals(new Ulong("18446744073709551614"),
                JsonUtils.readUnsignedLong("\"num\":18446744073709551614", RE, Ulong.ZERO));
        assertEquals(Ulong.ZERO, JsonUtils.readUnsignedLong("\"num\":0", RE, Ulong.MAX_VALUE));
        assertEquals(Ulong.ZERO, JsonUtils.readUnsignedLong("\"num\":-1", RE, Ulong.ZERO));
        assertEquals(Ulong.ZERO, JsonUtils.readUnsignedLong("\"num\":invalid", RE, Ulong.ZERO));
        assertNull(JsonUtils.parseUnsignedLong("18446744073709551616", null));
        assertNull(JsonUtils.parseUnsignedLong("invalid", null));
        assertNull(JsonUtils.parseUnsignedLong("-1", null));
        assertNull(JsonUtils.parseUnsignedLong("1.2", null));
        assertEquals(Ulong.ZERO, JsonUtils.parseUnsignedLong("0", null));
        assertEquals(Ulong.ONE, JsonUtils.parseUnsignedLong("1", null));
        assertEquals(new Ulong("18446744073709551614"), JsonUtils.parseUnsignedLong("18446744073709551614", null));
        assertEquals(new Ulong("18446744073709551615"), JsonUtils.parseUnsignedLong("18446744073709551615", null));
        */
    }

    @Test
    public void testReads() {
        String json = "\"yes\": true, \"no\": false";
        Pattern YES_RE = JsonUtils.buildPattern("yes", JsonUtils.FieldType.jsonBoolean);
        Pattern NO_RE = JsonUtils.buildPattern("no", JsonUtils.FieldType.jsonBoolean);
        Pattern MISSING_RE = JsonUtils.buildPattern("x", JsonUtils.FieldType.jsonBoolean);

        assertTrue(JsonUtils.readBoolean(json, YES_RE));
        assertFalse(JsonUtils.readBoolean(json, NO_RE));
        assertFalse((JsonUtils.readBoolean(json, MISSING_RE)));
    }

    @Test
    public void testDecode() {
        assertEquals("blah ", JsonUtils.decode("blah\\u0020"));
        assertEquals("blah\"", JsonUtils.decode("blah\\u0022"));
        assertEquals("blah'", JsonUtils.decode("blah\\u0027"));
        assertEquals("blah=", JsonUtils.decode("blah\\u003d"));
        assertEquals("blah=", JsonUtils.decode("blah\\u003D"));
        assertEquals("blah<", JsonUtils.decode("blah\\u003c"));
        assertEquals("blah<", JsonUtils.decode("blah\\u003C"));
        assertEquals("blah>", JsonUtils.decode("blah\\u003e"));
        assertEquals("blah>", JsonUtils.decode("blah\\u003E"));
        assertEquals("blah`", JsonUtils.decode("blah\\u0060"));
        assertEquals("blah\\", JsonUtils.decode("blah\\\\"));
        assertEquals("blah\b", JsonUtils.decode("blah\\b"));
        assertEquals("blah\f", JsonUtils.decode("blah\\f"));
        assertEquals("blah\n", JsonUtils.decode("blah\\n"));
        assertEquals("blah\r", JsonUtils.decode("blah\\r"));
        assertEquals("blah\t", JsonUtils.decode("blah\\t"));
        assertEquals("blah\\", JsonUtils.decode("blah\\x"));
        assertEquals("blah\\", JsonUtils.decode("blah\\"));
    }
}
