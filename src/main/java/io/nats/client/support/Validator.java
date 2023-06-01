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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsJetStreamConstants.MAX_HISTORY_PER_KEY;

public abstract class Validator {
    private Validator() {
    } /* ensures cannot be constructed */

    public static String validateSubject(String s, boolean required) {
        return validateSubject(s, "Subject", required, false);
    }

    public static String validateSubject(String subject, String label, boolean required, boolean cantEndWithGt) {
        if (emptyAsNull(subject) == null) {
            if (required) {
                throw new IllegalArgumentException(label + " cannot be null or empty.");
            }
            return null;
        }
        String[] segments = subject.split("\\.");
        for (int x = 0; x < segments.length; x++) {
            String segment = segments[x];
            if (segment.equals(">")) {
                if (cantEndWithGt || x != segments.length - 1) { // if it can end with gt, gt must be last segment
                    throw new IllegalArgumentException(label + " cannot contain '>'");
                }
            }
            else if (!segment.equals("*") && notPrintable(segment)) {
                    throw new IllegalArgumentException(label + " must be printable characters only.");
            }
        }
        return subject;
    }

    public static String validateReplyTo(String s, boolean required) {
        return validatePrintableExceptWildGt(s, "Reply To", required);
    }

    public static String validateQueueName(String s, boolean required) {
        return validatePrintableExceptWildDotGt(s, "Queue", required);
    }

    public static String validateStreamName(String s, boolean required) {
        return validatePrintableExceptWildDotGtSlashes(s, "Stream", required);
    }

    public static String validateDurable(String s, boolean required) {
        return validatePrintableExceptWildDotGtSlashes(s, "Durable", required);
    }

    public static String validateConsumerName(String s, boolean required) {
        return validatePrintableExceptWildDotGtSlashes(s, "Name", required);
    }

    public static String validatePrefixOrDomain(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (s.startsWith(DOT)) {
                throw new IllegalArgumentException(label + " cannot start with `.` [" + s + "]");
            }
            if (notPrintableOrHasWildGt(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range and cannot include `*`, `>` [" + s + "]");
            }
            return s;
        });
    }

    public static String validateKvKeyWildcardAllowedRequired(String s) {
        return validateWildcardKvKey(s, "Key", true);
    }

    public static String validateNonWildcardKvKeyRequired(String s) {
        return validateNonWildcardKvKey(s, "Key", true);
    }

    public static void validateNotSupplied(String s, NatsJetStreamClientError err) {
        if (!nullOrEmpty(s)) {
            throw err.instance();
        }
    }

    public static String validateMustMatchIfBothSupplied(String s1, String s2, NatsJetStreamClientError err) {
        // s1   | s2   || result
        // ---- | ---- || --------------
        // null | null || valid, null s2
        // null | y    || valid, y s2
        // x    | null || valid, x s1
        // x    | x    || valid, x s1
        // x    | y    || invalid
        s1 = emptyAsNull(s1);
        s2 = emptyAsNull(s2);
        if (s1 == null) {
            return s2; // s2 can be either null or y
        }

        // x / null or x / x
        if (s2 == null || s1.equals(s2)) {
            return s1;
        }

        throw err.instance();
    }

    interface Check {
        String check();
    }

    public static String required(String s, String label) {
        if (emptyAsNull(s) == null) {
            throw new IllegalArgumentException(label + " cannot be null or empty.");
        }
        return s;
    }

    public static <T> T required(T o, String label) {
        if (o == null) {
            throw new IllegalArgumentException(label + " cannot be null or empty.");
        }
        return o;
    }

    public static void required(List<?> l, String label) {
        if (l == null || l.isEmpty()) {
            throw new IllegalArgumentException(label + " cannot be null or empty.");
        }
    }

    public static void required(Map<?, ?> m, String label) {
        if (m == null || m.isEmpty()) {
            throw new IllegalArgumentException(label + " cannot be null or empty.");
        }
    }

    public static String _validate(String s, boolean required, String label, Check check) {
        if (emptyAsNull(s) == null) {
            if (required) {
                throw new IllegalArgumentException(label + " cannot be null or empty.");
            }
            return null;
        }
        return check.check();
    }

    public static String validateMaxLength(String s, int maxLength, boolean required, String label) {
        return _validate(s, required, label, () -> {
            int len = s.getBytes(StandardCharsets.UTF_8).length;
            if (len > maxLength) {
                throw new IllegalArgumentException(label + " cannot be longer than " + maxLength + " bytes but was " + len + " bytes");
            }
            return s;
        });
    }

    public static String validatePrintable(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notPrintable(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range [" + s + "]");
            }
            return s;
        });
    }

    public static String validatePrintableExceptWildDotGt(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notPrintableOrHasWildGtDot(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range and cannot include `*`, `.` or `>` [" + s + "]");
            }
            return s;
        });
    }

    public static String validatePrintableExceptWildDotGtSlashes(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notPrintableOrHasWildGtDotSlashes(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range and cannot include `*`, `.`, `>`, `\\` or  `/` [" + s + "]");
            }
            return s;
        });
    }

    public static String validatePrintableExceptWildGt(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notPrintableOrHasWildGt(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range and cannot include `*` or `>` [" + s + "]");
            }
            return s;
        });
    }

    public static String validateIsRestrictedTerm(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notRestrictedTerm(s)) {
                throw new IllegalArgumentException(label + " must only contain A-Z, a-z, 0-9, `-` or `_` [" + s + "]");
            }
            return s;
        });
    }

    public static String validateBucketName(String s, boolean required) {
        return validateIsRestrictedTerm(s, "Bucket Name", required);
    }

    public static String validateWildcardKvKey(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notWildcardKvKey(s)) {
                throw new IllegalArgumentException(label + " must only contain A-Z, a-z, 0-9, `*`, `-`, `_`, `/`, `=`, `>` or `.` and cannot start with `.` [" + s + "]");
            }
            return s;
        });
    }

    public static String validateNonWildcardKvKey(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notNonWildcardKvKey(s)) {
                throw new IllegalArgumentException(label + " must only contain A-Z, a-z, 0-9, `-`, `_`, `/`, `=` or `.` and cannot start with `.` [" + s + "]");
            }
            return s;
        });
    }

    public static long validateMaxConsumers(long max) {
        return validateGtZeroOrMinus1(max, "Max Consumers");
    }

    public static long validateMaxMessages(long max) {
        return validateGtZeroOrMinus1(max, "Max Messages");
    }

    public static long validateMaxMessagesPerSubject(long max) {
        return validateGtZeroOrMinus1(max, "Max Messages Per Subject");
    }

    public static int validateMaxHistory(int max) {
        if (max < 1 || max > MAX_HISTORY_PER_KEY) {
            throw new IllegalArgumentException("Max History must be from 1 to " + MAX_HISTORY_PER_KEY + " inclusive.");
        }
        return max;
    }

    public static long validateMaxBytes(long max) {
        return validateGtZeroOrMinus1(max, "Max Bytes");
    }

    public static long validateMaxBucketBytes(long max) {
        return validateGtZeroOrMinus1(max, "Max Bucket Bytes"); // max bucket bytes is a kv alias to max bytes
    }

    public static long validateMaxMessageSize(long max) {
        return validateGtZeroOrMinus1(max, "Max Message Size");
    }

    public static long validateMaxValueSize(long max) {
        return validateGtZeroOrMinus1(max, "Max Value Size"); // max value size is a kv alias to max message size
    }

    public static int validateNumberOfReplicas(int replicas) {
        if (replicas < 1 || replicas > 5) {
            throw new IllegalArgumentException("Replicas must be from 1 to 5 inclusive.");
        }
        return replicas;
    }

    public static Duration validateDurationRequired(Duration d) {
        if (d == null || d.isZero() || d.isNegative()) {
            throw new IllegalArgumentException("Duration required and must be greater than 0.");
        }
        return d;
    }

    public static Duration validateDurationNotRequiredGtOrEqZero(Duration d, Duration ifNull) {
        if (d == null) {
            return ifNull;
        }
        if (d.isNegative()) {
            throw new IllegalArgumentException("Duration must be greater than or equal to 0.");
        }
        return d;
    }

    public static Duration validateDurationNotRequiredGtOrEqZero(long millis) {
        if (millis < 0) {
            throw new IllegalArgumentException("Duration must be greater than or equal to 0.");
        }
        return Duration.ofMillis(millis);
    }

    public static String validateNotNull(String s, String fieldName) {
        if (s == null) {
            throw new IllegalArgumentException(fieldName + " cannot be null");
        }
        return s;
    }

    public static Object validateNotNull(Object o, String fieldName) {
        if (o == null) {
            throw new IllegalArgumentException(fieldName + " cannot be null");
        }
        return o;
    }

    public static int validateGtZero(int i, String label) {
        if (i < 1) {
            throw new IllegalArgumentException(label + " must be greater than zero");
        }
        return i;
    }

    public static long validateGtZeroOrMinus1(long l, String label) {
        if (zeroOrLtMinus1(l)) {
            throw new IllegalArgumentException(label + " must be greater than zero or -1 for unlimited");
        }
        return l;
    }

    public static long validateGtEqMinus1(long l, String label) {
        if (l < -1) {
            throw new IllegalArgumentException(label + " must be greater than zero or -1 for unlimited");
        }
        return l;
    }

    public static long validateNotNegative(long l, String label) {
        if (l < 0) {
            throw new IllegalArgumentException(label + " cannot be negative");
        }
        return l;
    }

    public static boolean isGtEqZero(long l) {
        return l >= 0;
    }

    public static long validateGtEqZero(long l, String label) {
        if (l < 0) {
            throw new IllegalArgumentException(label + " must be greater than or equal to zero");
        }
        return l;
    }

    // ----------------------------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------------------------
    public static boolean nullOrEmpty(String s) {
        return s == null || s.trim().length() == 0;
    }

    public static boolean notPrintable(String s) {
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c < 33 || c > 126) {
                return true;
            }
        }
        return false;
    }

    public static boolean notPrintableOrHasChars(String s, char[] charsToNotHave) {
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c < 33 || c > 126) {
                return true;
            }
            for (char cx : charsToNotHave) {
                if (c == cx) {
                    return true;
                }
            }
        }
        return false;
    }

    // restricted-term  = (A-Z, a-z, 0-9, dash 45, underscore 95)+
    public static boolean notRestrictedTerm(String s) {
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c < '0') { // before 0
                if (c == '-') { // only dash is accepted
                    continue;
                }
                return true; // "not"
            }
            if (c < ':') {
                continue; // means it's 0 - 9
            }
            if (c < 'A') {
                return true; // between 9 and A is "not restricted"
            }
            if (c < '[') {
                continue; // means it's A - Z
            }
            if (c < 'a') { // before a
                if (c == '_') { // only underscore is accepted
                    continue;
                }
                return true; // "not"
            }
            if (c > 'z') { // 122 is z, characters after of them are "not restricted"
                return true;
            }
        }
        return false;
    }

    // limited-term = (A-Z, a-z, 0-9, dash 45, dot 46, fwd-slash 47, equals 61, underscore 95)+
    // kv-key-name = limited-term (dot limited-term)*
    public static boolean notNonWildcardKvKey(String s) {
        if (s.charAt(0) == '.') {
            return true; // can't start with dot
        }
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c < '0') { // before 0
                if (c == '-' || c == '.' || c == '/') { // only dash dot and fwd slash are accepted
                    continue;
                }
                return true; // "not"
            }
            if (c < ':') {
                continue; // means it's 0 - 9
            }
            if (c < 'A') {
                if (c == '=') { // equals is accepted
                    continue;
                }
                return true; // between 9 and A is "not limited"
            }
            if (c < '[') {
                continue; // means it's A - Z
            }
            if (c < 'a') { // before a
                if (c == '_') { // only underscore is accepted
                    continue;
                }
                return true; // "not"
            }
            if (c > 'z') { // 122 is z, characters after of them are "not limited"
                return true;
            }
        }
        return false;
    }

    // (A-Z, a-z, 0-9, star 42, dash 45, dot 46, fwd-slash 47, equals 61, gt 62, underscore 95)+
    public static boolean notWildcardKvKey(String s) {
        if (s.charAt(0) == '.') {
            return true; // can't start with dot
        }
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c < '0') { // before 0
                if (c == '*' || c == '-' || c == '.' || c == '/') { // only star dash dot and fwd slash are accepted
                    continue;
                }
                return true; // "not"
            }
            if (c < ':') {
                continue; // means it's 0 - 9
            }
            if (c < 'A') {
                if (c == '=' || c == '>') { // equals, gt is accepted
                    continue;
                }
                return true; // between 9 and A is "not limited"
            }
            if (c < '[') {
                continue; // means it's A - Z
            }
            if (c < 'a') { // before a
                if (c == '_') { // only underscore is accepted
                    continue;
                }
                return true; // "not"
            }
            if (c > 'z') { // 122 is z, characters after of them are "not limited"
                return true;
            }
        }
        return false;
    }

    static final char[] WILD_GT = {'*', '>'};
    static final char[] WILD_GT_DOT = {'*', '>', '.'};
    static final char[] WILD_GT_DOT_SLASHES = {'*', '>', '.', '\\', '/'};

    private static boolean notPrintableOrHasWildGt(String s) {
        return notPrintableOrHasChars(s, WILD_GT);
    }

    private static boolean notPrintableOrHasWildGtDot(String s) {
        return notPrintableOrHasChars(s, WILD_GT_DOT);
    }

    private static boolean notPrintableOrHasWildGtDotSlashes(String s) {
        return notPrintableOrHasChars(s, WILD_GT_DOT_SLASHES);
    }

    public static String emptyAsNull(String s) {
        return nullOrEmpty(s) ? null : s;
    }

    public static String emptyOrNullAs(String s, String ifEmpty) {
        return nullOrEmpty(s) ? ifEmpty : s;
    }

    public static boolean zeroOrLtMinus1(long l) {
        return l == 0 || l < -1;
    }

    public static Duration ensureNotNullAndNotLessThanMin(Duration provided, Duration minimum, Duration dflt)
    {
        return provided == null || provided.toNanos() < minimum.toNanos() ? dflt : provided;
    }

    public static Duration ensureDurationNotLessThanMin(long providedMillis, Duration minimum, Duration dflt)
    {
        return ensureNotNullAndNotLessThanMin(Duration.ofMillis(providedMillis), minimum, dflt);
    }

    public static String ensureEndsWithDot(String s) {
        return s == null || s.endsWith(DOT) ? s : s + DOT;
    }

    static final Pattern SEMVER_PATTERN = Pattern.compile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$");

    public static String validateSemVer(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (!isSemVer(s)) {
                throw new IllegalArgumentException(label + " must be a valid SemVer");
            }
            return s;
        });
    }

    public static boolean isSemVer(String s) {
        return SEMVER_PATTERN.matcher(s).find();
    }

    public static <T> boolean listsAreEqual(List<T> l1, List<T> l2, boolean nullSecondEqualsEmptyFirst)
    {
        if (l1 == null)
        {
            return l2 == null;
        }

        if (l2 == null)
        {
            return nullSecondEqualsEmptyFirst && l1.size() == 0;
        }

        return l1.equals(l2);
    }


    public static boolean mapsAreEqual(Map<String, String> m1, Map<String, String> m2, boolean nullSecondEqualsEmptyFirst)
    {
        if (m1 == null)
        {
            return m2 == null;
        }

        if (m2 == null)
        {
            return nullSecondEqualsEmptyFirst && m1.size() == 0;
        }

        if (m1.size() != m2.size()) {
            return false;
        }

        for (Map.Entry<String, String> entry : m1.entrySet())
        {
            if (!entry.getValue().equals(m2.get(entry.getKey()))) {
                return false;
            }
        }

        return true;
    }

}
