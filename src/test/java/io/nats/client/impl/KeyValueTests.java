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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.nats.client.support.NatsKeyValueUtil.streamName;
import static io.nats.client.support.NatsKeyValueUtil.streamSubject;
import static org.junit.jupiter.api.Assertions.*;

public class KeyValueTests extends JetStreamTestBase {

    @Test
    public void testWorkflow() throws Exception {
        long now = ZonedDateTime.now().toEpochSecond();

        String byteKey = "byteKey";
        String stringKey = "stringKey";
        String longKey = "longKey";
        String notFoundKey = "notFound";
        String byteValue1 = "Byte Value 1";
        String byteValue2 = "Byte Value 2";
        String stringValue1 = "String Value 1";
        String stringValue2 = "String Value 2";

        runInJsServer(nc -> {
            // get the kv management context
            KeyValueManagement kvm = nc.keyValueManagement(JetStreamOptions.DEFAULT_JS_OPTIONS); // use options here for coverage

            // create the bucket
            KeyValueConfiguration kvc = KeyValueConfiguration.builder()
                    .name(BUCKET)
                    .maxHistoryPerKey(3)
                    .storageType(StorageType.Memory)
                    .build();

            KeyValueStatus status = kvm.create(kvc);

            kvc = status.getConfiguration();
            assertEquals(BUCKET, status.getBucketName());
            assertEquals(BUCKET, kvc.getBucketName());
            assertEquals(streamName(BUCKET), kvc.getBackingConfig().getName());
            assertEquals(-1, kvc.getMaxValues());
            assertEquals(3, status.getMaxHistoryPerKey());
            assertEquals(3, kvc.getMaxHistoryPerKey());
            assertEquals(-1, kvc.getMaxBucketSize());
            assertEquals(-1, kvc.getMaxValueBytes());
            assertEquals(Duration.ZERO, status.getTtl());
            assertEquals(Duration.ZERO, kvc.getTtl());
            assertEquals(StorageType.Memory, kvc.getStorageType());
            assertEquals(1, kvc.getReplicas());
            assertEquals(0, status.getEntryCount());
            assertEquals("JetStream", status.getBackingStore());

            // get the kv context for the specific bucket
            KeyValue kv = nc.keyValue(BUCKET, JetStreamOptions.DEFAULT_JS_OPTIONS); // use options here for coverage

            // Put some keys. Each key is put in a subject in the bucket (stream)
            // The put returns the sequence number in the bucket (stream)
            assertEquals(1, kv.put(byteKey, byteValue1.getBytes()));
            assertEquals(2, kv.put(stringKey, stringValue1));
            assertEquals(3, kv.put(longKey, 1));

            // retrieve the values. all types are stored as bytes
            // so you can always get the bytes directly
            assertEquals(byteValue1, new String(kv.get(byteKey).getValue()));
            assertEquals(stringValue1, new String(kv.get(stringKey).getValue()));
            assertEquals("1", new String(kv.get(longKey).getValue()));

            // if you know the value is not binary and can safely be read
            // as a UTF-8 string, the getStringValue method is ok to use
            assertEquals(byteValue1, kv.get(byteKey).getValueAsString());
            assertEquals(stringValue1, kv.get(stringKey).getValueAsString());
            assertEquals("1", kv.get(longKey).getValueAsString());

            // if you know the value is a long, you can use
            // the getLongValue method
            // if it's not a number a NumberFormatException is thrown
            assertEquals(1, kv.get(longKey).getValueAsLong());
            assertThrows(NumberFormatException.class, () -> kv.get(stringKey).getValueAsLong());

            // going to manually track history for verification later
            List<KeyValueEntry> byteHistory = new ArrayList<>();
            List<KeyValueEntry> stringHistory = new ArrayList<>();
            List<KeyValueEntry> longHistory = new ArrayList<>();

            // entry gives detail about latest entry of the key
            byteHistory.add(
                    assertEntry(BUCKET, byteKey, KeyValueOperation.PUT, 1, byteValue1, now, kv.get(byteKey)));

            stringHistory.add(
                    assertEntry(BUCKET, stringKey, KeyValueOperation.PUT, 2, stringValue1, now, kv.get(stringKey)));

            longHistory.add(
                    assertEntry(BUCKET, longKey, KeyValueOperation.PUT, 3, "1", now, kv.get(longKey)));

            // history gives detail about the key
            assertHistory(byteHistory, kv.history(byteKey));
            assertHistory(stringHistory, kv.history(stringKey));
            assertHistory(longHistory, kv.history(longKey));

            // let's check the bucket info
            status = kvm.getBucketInfo(BUCKET);
            assertEquals(3, status.getEntryCount());
            assertEquals(3, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // delete a key. Its entry will still exist, but it's value is null
            kv.delete(byteKey);

            byteHistory.add(
                    assertEntry(BUCKET, byteKey, KeyValueOperation.DELETE, 4, null, now, kv.get(byteKey)));
            assertHistory(byteHistory, kv.history(byteKey));

            // hashCode coverage
            assertEquals(byteHistory.get(0).hashCode(), byteHistory.get(0).hashCode());
            assertNotEquals(byteHistory.get(0).hashCode(), byteHistory.get(1).hashCode());

            // let's check the bucket info
            status = kvm.getBucketInfo(BUCKET);
            assertEquals(4, status.getEntryCount());
            assertEquals(4, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // if the key has been deleted
            // all varieties of get will return null
            assertNull(kv.get(byteKey).getValue());
            assertNull(kv.get(byteKey).getValueAsString());
            assertNull(kv.get(byteKey).getValueAsLong());

            // if the key does not exist (no history) there is no entry
            assertNull(kv.get(notFoundKey));

            // Update values. You can even update a deleted key
            assertEquals(5, kv.put(byteKey, byteValue2.getBytes()));
            assertEquals(6, kv.put(stringKey, stringValue2));
            assertEquals(7, kv.put(longKey, 2));

            // values after updates
            assertEquals(byteValue2, new String(kv.get(byteKey).getValue()));
            assertEquals(stringValue2, kv.get(stringKey).getValueAsString());
            assertEquals(2, kv.get(longKey).getValueAsLong());

            // entry and history after update
            byteHistory.add(
                    assertEntry(BUCKET, byteKey, KeyValueOperation.PUT, 5, byteValue2, now, kv.get(byteKey)));
            assertHistory(byteHistory, kv.history(byteKey));

            stringHistory.add(
                    assertEntry(BUCKET, stringKey, KeyValueOperation.PUT, 6, stringValue2, now, kv.get(stringKey)));
            assertHistory(stringHistory, kv.history(stringKey));

            longHistory.add(
                    assertEntry(BUCKET, longKey, KeyValueOperation.PUT, 7, "2", now, kv.get(longKey)));
            assertHistory(longHistory, kv.history(longKey));

            // let's check the bucket info
            status = kvm.getBucketInfo(BUCKET);
            assertEquals(7, status.getEntryCount());
            assertEquals(7, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // make sure it only keeps the correct amount of history
            assertEquals(8, kv.put(longKey, 3));
            assertEquals(3, kv.get(longKey).getValueAsLong());

            longHistory.add(
                    assertEntry(BUCKET, longKey, KeyValueOperation.PUT, 8, "3", now, kv.get(longKey)));
            assertHistory(longHistory, kv.history(longKey));

            status = kvm.getBucketInfo(BUCKET);
            assertEquals(8, status.getEntryCount());
            assertEquals(8, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // this would be the 4th entry for the longKey
            // sp the total records will stay the same
            assertEquals(9, kv.put(longKey, 4));
            assertEquals(4, kv.get(longKey).getValueAsLong());

            // history only retains 3 records
            longHistory.remove(0);
            longHistory.add(
                    assertEntry(BUCKET, longKey, KeyValueOperation.PUT, 9, "4", now, kv.get(longKey)));
            assertHistory(longHistory, kv.history(longKey));

            // record count does not increase
            status = kvm.getBucketInfo(BUCKET);
            assertEquals(8, status.getEntryCount());
            assertEquals(9, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // should have exactly these 3 keys
            assertKeys(kv.keys(), byteKey, stringKey, longKey);

            // purge
            kv.purge(longKey);
            longHistory.clear();
            longHistory.add(
                assertEntry(BUCKET, longKey, KeyValueOperation.PURGE, 10, null, now, kv.get(longKey)));
            assertHistory(longHistory, kv.history(longKey));

            status = kvm.getBucketInfo(BUCKET);
            assertEquals(6, status.getEntryCount()); // includes 1 purge
            assertEquals(10, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // only 2 keys now
            assertKeys(kv.keys(), byteKey, stringKey);

            kv.purge(byteKey);
            byteHistory.clear();
            byteHistory.add(
                assertEntry(BUCKET, byteKey, KeyValueOperation.PURGE, 11, null, now, kv.get(byteKey)));
            assertHistory(byteHistory, kv.history(byteKey));

            status = kvm.getBucketInfo(BUCKET);
            assertEquals(4, status.getEntryCount()); // includes 2 purges
            assertEquals(11, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // only 1 key now
            assertKeys(kv.keys(), stringKey);

            kv.purge(stringKey);
            stringHistory.clear();
            stringHistory.add(
                assertEntry(BUCKET, stringKey, KeyValueOperation.PURGE, 12, null, now, kv.get(stringKey)));
            assertHistory(stringHistory, kv.history(stringKey));

            status = kvm.getBucketInfo(BUCKET);
            assertEquals(3, status.getEntryCount()); // 3 purges
            assertEquals(12, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // no more keys left
            assertKeys(kv.keys());

            // clear things
            kv.purgeDeletes();
            status = kvm.getBucketInfo(BUCKET);
            assertEquals(0, status.getEntryCount()); // purges are all gone
            assertEquals(12, status.getBackingStreamInfo().getStreamState().getLastSequence());

            longHistory.clear();
            assertHistory(longHistory, kv.history(longKey));

            stringHistory.clear();
            assertHistory(stringHistory, kv.history(stringKey));

            // put some more
            assertEquals(13, kv.put(longKey, 110));
            longHistory.add(
                    assertEntry(BUCKET, longKey, KeyValueOperation.PUT, 13, "110", now, kv.get(longKey)));

            assertEquals(14, kv.put(longKey, 111));
            longHistory.add(
                    assertEntry(BUCKET, longKey, KeyValueOperation.PUT, 14, "111", now, kv.get(longKey)));

            assertEquals(15, kv.put(longKey, 112));
            longHistory.add(
                    assertEntry(BUCKET, longKey, KeyValueOperation.PUT, 15, "112", now, kv.get(longKey)));

            assertEquals(16, kv.put(stringKey, stringValue1));
            stringHistory.add(
                    assertEntry(BUCKET, stringKey, KeyValueOperation.PUT, 16, stringValue1, now, kv.get(stringKey)));

            assertEquals(17, kv.put(stringKey, stringValue2));
            stringHistory.add(
                    assertEntry(BUCKET, stringKey, KeyValueOperation.PUT, 17, stringValue2, now, kv.get(stringKey)));

            assertHistory(longHistory, kv.history(longKey));
            assertHistory(stringHistory, kv.history(stringKey));

            status = kvm.getBucketInfo(BUCKET);
            assertEquals(5, status.getEntryCount());
            assertEquals(17, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // delete the bucket
            kvm.delete(BUCKET);
            assertThrows(JetStreamApiException.class, () -> kvm.delete(BUCKET));
            assertThrows(JetStreamApiException.class, () -> kvm.getBucketInfo(BUCKET));

            assertEquals(0, kvm.getBucketsNames().size());
        });
    }

    @Test
    public void testKeys() throws Exception {
        runInJsServer(nc -> {
            KeyValueManagement kvm = nc.keyValueManagement();

            // create bucket 1
            kvm.create(KeyValueConfiguration.builder()
                .name(BUCKET)
                .storageType(StorageType.Memory)
                .build());

            KeyValue kv = nc.keyValue(BUCKET);
            for (int x = 1; x <= 10; x++) {
                kv.put("k" + x, x);
            }

            List<String> keys = kv.keys();
            assertEquals(10, keys.size());

            kv.delete("k1");
            kv.delete("k3");
            kv.delete("k5");
            kv.purge("k7");
            kv.purge("k9");

            keys = kv.keys();
            assertEquals(5, keys.size());

            for (int x = 2; x <= 10; x += 2) {
                assertTrue(keys.contains("k" + x));
            }
        });
    }

    @Test
    public void testHistoryDeletePurge() throws Exception {
        runInJsServer(nc -> {
            KeyValueManagement kvm = nc.keyValueManagement();

            // create bucket
            kvm.create(KeyValueConfiguration.builder()
                .name(BUCKET)
                .storageType(StorageType.Memory)
                .maxHistoryPerKey(64)
                .build());

            KeyValue kv = nc.keyValue(BUCKET);
            kv.put(KEY, "a");
            kv.put(KEY, "b");
            kv.put(KEY, "c");
            List<KeyValueEntry> list = kv.history(KEY);
            assertEquals(3, list.size());

            kv.delete(KEY);
            list = kv.history(KEY);
            assertEquals(4, list.size());

            kv.purge(KEY);
            list = kv.history(KEY);
            assertEquals(1, list.size());
        });
    }

    @Test
    public void testPurgeDeletes() throws Exception {
        runInJsServer(nc -> {
            KeyValueManagement kvm = nc.keyValueManagement();

            // create bucket
            kvm.create(KeyValueConfiguration.builder()
                .name(BUCKET)
                .storageType(StorageType.Memory)
                .maxHistoryPerKey(64)
                .build());

            KeyValue kv = nc.keyValue(BUCKET);
            kv.put(key(1), "a");
            kv.delete(key(1));
            kv.put(key(2), "b");
            kv.put(key(3), "c");
            kv.put(key(4), "d");
            kv.purge(key(4));

            JetStream js = nc.jetStream();

            JetStreamSubscription sub = js.subscribe(streamSubject(BUCKET));

            Message m = sub.nextMessage(1000);
            assertEquals("a", new String(m.getData()));

            m = sub.nextMessage(1000);
            assertEquals(0, m.getData().length);

            m = sub.nextMessage(1000);
            assertEquals("b", new String(m.getData()));

            m = sub.nextMessage(1000);
            assertEquals("c", new String(m.getData()));

            m = sub.nextMessage(1000);
            assertEquals(0, m.getData().length);

            sub.unsubscribe();

            kv.purgeDeletes();
            sub = js.subscribe(streamSubject(BUCKET));

            m = sub.nextMessage(1000);
            assertEquals("b", new String(m.getData()));

            m = sub.nextMessage(1000);
            assertEquals("c", new String(m.getData()));

            sub.unsubscribe();
        });
    }

    @Test
    public void testCreateAndUpdate() throws Exception {
        runInJsServer(nc -> {
            KeyValueManagement kvm = nc.keyValueManagement();

            // create bucket
            kvm.create(KeyValueConfiguration.builder()
                .name(BUCKET)
                .storageType(StorageType.Memory)
                .maxHistoryPerKey(64)
                .build());

            KeyValue kv = nc.keyValue(BUCKET);

            // 1. allowed to create something that does not exist
            long rev1 = kv.create(KEY, "a".getBytes());

            // 2. allowed to update with proper revision
            kv.update(KEY, "ab".getBytes(), rev1);

            // 3. not allowed to update with wrong revision
            assertThrows(JetStreamApiException.class, () -> kv.update(KEY, "zzz".getBytes(), rev1));

            // 4. not allowed to create a key that exists
            assertThrows(JetStreamApiException.class, () -> kv.create(KEY, "zzz".getBytes()));

            // 5. not allowed to update a key that does not exist
            assertThrows(JetStreamApiException.class, () -> kv.update(KEY, "zzz".getBytes(), 1));

            // 6. allowed to create a key that is deleted
            kv.delete(KEY);
            kv.create(KEY, "abc".getBytes());

            // 7. allowed to update a key that is deleted, as long as you have it's revision
            kv.delete(KEY);
            List<KeyValueEntry> hist = kv.history(KEY);
            kv.update(KEY, "abcd".getBytes(), hist.get(hist.size()-1).getRevision());
        });
    }

    @Test
    public void testManageGetBucketNames() throws Exception {
        runInJsServer(nc -> {
            KeyValueManagement kvm = nc.keyValueManagement();

            // create bucket 1
            kvm.create(KeyValueConfiguration.builder()
                    .name(bucket(1))
                    .storageType(StorageType.Memory)
                    .build());

            // create bucket 2
            kvm.create(KeyValueConfiguration.builder()
                    .name(bucket(2))
                    .storageType(StorageType.Memory)
                    .build());

            createMemoryStream(nc, stream(1));
            createMemoryStream(nc, stream(2));

            List<String> buckets = kvm.getBucketsNames();
            assertEquals(2, buckets.size());
            assertTrue(buckets.contains(bucket(1)));
            assertTrue(buckets.contains(bucket(2)));
        });
    }

    private void assertKeys(List<String> apiKeys, String... manualKeys) {
        assertEquals(manualKeys.length, apiKeys.size());
        for (String k : manualKeys) {
            assertTrue(apiKeys.contains(k));
        }
    }

    private void assertHistory(List<KeyValueEntry> manualHistory, List<KeyValueEntry> apiHistory) {
        assertEquals(apiHistory.size(), manualHistory.size());
        for (int x = 0; x < apiHistory.size(); x++) {
            assertKvEquals(apiHistory.get(x), manualHistory.get(x));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private KeyValueEntry assertEntry(String bucket, String key, KeyValueOperation op, long seq, String value, long now, KeyValueEntry entry) {
        assertEquals(bucket, entry.getBucket());
        assertEquals(key, entry.getKey());
        assertEquals(op, entry.getOperation());
        assertEquals(seq, entry.getRevision());
        assertEquals(0, entry.getDelta());
        if (op == KeyValueOperation.PUT) {
            assertEquals(value, new String(entry.getValue()));
        }
        else {
            assertNull(entry.getValue());
        }
        assertTrue(now <= entry.getCreated().toEpochSecond());

        // coverage
        assertNotNull(entry.toString());
        return entry;
    }

    private void assertKvEquals(KeyValueEntry kv1, KeyValueEntry kv2) {
        assertEquals(kv1.getOperation(), kv2.getOperation());
        assertEquals(kv1.getRevision(), kv2.getRevision());
        assertEquals(kv1.getBucket(), kv2.getBucket());
        assertEquals(kv1.getKey(), kv2.getKey());
        assertTrue(Arrays.equals(kv1.getValue(), kv2.getValue()));
        long es1 = kv1.getCreated().toEpochSecond();
        long es2 = kv2.getCreated().toEpochSecond();
        assertEquals(es1, es2);
    }

    static class TestKeyValueWatcher implements KeyValueWatcher {
        public List<KeyValueEntry> entries = new ArrayList<>();
        KeyValue.WatchOption[] watchOptions;
        public boolean metaOnly;
        public int endOfDataReceived;
        public boolean noisy;

        public TestKeyValueWatcher(KeyValue.WatchOption... watchOptions) {
            this.watchOptions = watchOptions;
            for (KeyValue.WatchOption ro : watchOptions) {
                if (ro == KeyValue.WatchOption.META_ONLY) {
                    metaOnly = true;
                    break;
                }
            }
        }

        public TestKeyValueWatcher noisy() {
            this.noisy = true;
            return this;
        }

        @Override
        public void watch(KeyValueEntry kve) {
            entries.add(kve);
            if (noisy) { System.out.println("WATCH " + kve); }
        }

        @Override
        public void endOfData() {
            endOfDataReceived++;
            if (noisy) { System.out.println("EOD " + endOfDataReceived); }
        }
    }

    @Test
    public void testWatch() throws Exception {
        String keyNull = "key.nl";
        String key1 = "key.1";
        String key2 = "key.2";

        runInJsServer(nc -> {
            KeyValueManagement kvm = nc.keyValueManagement();

            kvm.create(KeyValueConfiguration.builder()
                .name(BUCKET)
                .maxHistoryPerKey(10)
                .storageType(StorageType.Memory)
                .build());

            KeyValue kv = nc.keyValue(BUCKET);

            TestKeyValueWatcher key1FullWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher key1MetaWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY);
            TestKeyValueWatcher key1StartNewWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY);
            TestKeyValueWatcher key1StartAllWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY);
            TestKeyValueWatcher key1AfterWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY);
            TestKeyValueWatcher key1AfterIgDelWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY, KeyValue.WatchOption.IGNORE_DELETE);
            TestKeyValueWatcher key1AfterStartNewWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY, KeyValue.WatchOption.UPDATES_ONLY);
            TestKeyValueWatcher key1AfterStartFirstWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY, KeyValue.WatchOption.INCLUDE_HISTORY);
            TestKeyValueWatcher key2FullWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher key2MetaWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY);
            TestKeyValueWatcher key2AfterWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY);
            TestKeyValueWatcher key2AfterStartNewWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY, KeyValue.WatchOption.UPDATES_ONLY);
            TestKeyValueWatcher key2AfterStartFirstWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY, KeyValue.WatchOption.INCLUDE_HISTORY);
            TestKeyValueWatcher allAllFullWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher allAllMetaWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY);
            TestKeyValueWatcher allIgDelFullWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.IGNORE_DELETE);
            TestKeyValueWatcher allIgDelMetaWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY, KeyValue.WatchOption.IGNORE_DELETE);
            TestKeyValueWatcher starFullWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher starMetaWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY);
            TestKeyValueWatcher gtFullWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher gtMetaWatcher = new TestKeyValueWatcher(KeyValue.WatchOption.META_ONLY);

            List<NatsKeyValueWatchSubscription> subs = new ArrayList<>();

            // subs created before data will only have 1 endOfData call
            subs.add(kv.watch(key1, key1FullWatcher, key1FullWatcher.watchOptions));
            subs.add(kv.watch(key1, key1MetaWatcher, key1MetaWatcher.watchOptions));
            subs.add(kv.watch(key1, key1StartNewWatcher, key1StartNewWatcher.watchOptions));
            subs.add(kv.watch(key1, key1StartAllWatcher, key1StartAllWatcher.watchOptions));
            subs.add(kv.watch(key2, key2FullWatcher, key2FullWatcher.watchOptions));
            subs.add(kv.watch(key2, key2MetaWatcher, key2MetaWatcher.watchOptions));
            subs.add(kv.watchAll(allAllFullWatcher, allAllFullWatcher.watchOptions));
            subs.add(kv.watchAll(allAllMetaWatcher, allAllMetaWatcher.watchOptions));
            subs.add(kv.watchAll(allIgDelFullWatcher, allIgDelFullWatcher.watchOptions));
            subs.add(kv.watchAll(allIgDelMetaWatcher, allIgDelMetaWatcher.watchOptions));
            subs.add(kv.watch("key.*", starFullWatcher, starFullWatcher.watchOptions));
            subs.add(kv.watch("key.*", starMetaWatcher, starMetaWatcher.watchOptions));
            subs.add(kv.watch("key.>", gtFullWatcher, gtFullWatcher.watchOptions));
            subs.add(kv.watch("key.>", gtMetaWatcher, gtMetaWatcher.watchOptions));

            kv.put(key1, "a");
            kv.put(key1, "aa");
            kv.put(key2, "z");
            kv.put(key2, "zz");

            kv.delete(key1);
            kv.delete(key2);
            kv.put(key1, "aaa");
            kv.put(key2, "zzz");
            kv.delete(key1);

            kv.purge(key1);
            kv.put(keyNull, (byte[])null);

            sleep(100); // give time for all the data to be setup

            subs.add(kv.watch(key1, key1AfterWatcher, key1AfterWatcher.watchOptions));
            subs.add(kv.watch(key1, key1AfterIgDelWatcher, key1AfterIgDelWatcher.watchOptions));
            subs.add(kv.watch(key1, key1AfterStartNewWatcher, key1AfterStartNewWatcher.watchOptions));
            subs.add(kv.watch(key1, key1AfterStartFirstWatcher, key1AfterStartFirstWatcher.watchOptions));
            subs.add(kv.watch(key2, key2AfterWatcher, key2AfterWatcher.watchOptions));
            subs.add(kv.watch(key2, key2AfterStartNewWatcher, key2AfterStartNewWatcher.watchOptions));
            subs.add(kv.watch(key2, key2AfterStartFirstWatcher, key2AfterStartFirstWatcher.watchOptions));

            sleep(2000); // give time for the watches to get messages

            Object[] key1AllExpecteds = new Object[] {
                "a", "aa", KeyValueOperation.DELETE, "aaa", KeyValueOperation.DELETE, KeyValueOperation.PURGE
            };

            Object[] noExpecteds = new Object[0];
            Object[] purgeOnlyExpecteds = new Object[] { KeyValueOperation.PURGE };

            Object[] key2AllExpecteds = new Object[] {
                "z", "zz", KeyValueOperation.DELETE, "zzz"
            };

            Object[] key2AfterExpecteds = new Object[] { "zzz" };

            Object[] allExpecteds = new Object[] {
                "a", "aa", "z", "zz",
                KeyValueOperation.DELETE, KeyValueOperation.DELETE,
                "aaa", "zzz",
                KeyValueOperation.DELETE, KeyValueOperation.PURGE,
                null
            };

            Object[] allPutsExpecteds = new Object[] {
                "a", "aa", "z", "zz", "aaa", "zzz", null
            };

            // unsubscribe so the watchers don't get any more messages
            for (NatsKeyValueWatchSubscription sub : subs) {
                sub.unsubscribe();
            }

            // put some more data which should not be seen by watches
            kv.put(key1, "aaaa");
            kv.put(key2, "zzzz");

            validateWatcher(key1AllExpecteds, key1FullWatcher);
            validateWatcher(key1AllExpecteds, key1MetaWatcher);
            validateWatcher(key1AllExpecteds, key1StartNewWatcher);
            validateWatcher(key1AllExpecteds, key1StartAllWatcher);
            validateWatcher(purgeOnlyExpecteds, key1AfterWatcher);
            validateWatcher(noExpecteds, key1AfterIgDelWatcher);
            validateWatcher(noExpecteds, key1AfterStartNewWatcher);
            validateWatcher(purgeOnlyExpecteds, key1AfterStartFirstWatcher);

            validateWatcher(key2AllExpecteds, key2FullWatcher);
            validateWatcher(key2AllExpecteds, key2MetaWatcher);
            validateWatcher(key2AfterExpecteds, key2AfterWatcher);
            validateWatcher(noExpecteds, key2AfterStartNewWatcher);
            validateWatcher(key2AllExpecteds, key2AfterStartFirstWatcher);

            validateWatcher(allExpecteds, allAllFullWatcher);
            validateWatcher(allExpecteds, allAllMetaWatcher);
            validateWatcher(allPutsExpecteds, allIgDelFullWatcher);
            validateWatcher(allPutsExpecteds, allIgDelMetaWatcher);

            validateWatcher(allExpecteds, starFullWatcher);
            validateWatcher(allExpecteds, starMetaWatcher);
            validateWatcher(allExpecteds, gtFullWatcher);
            validateWatcher(allExpecteds, gtMetaWatcher);
        });
    }

    private void validateWatcher(Object[] expectedKves, TestKeyValueWatcher watcher) {
        assertEquals(expectedKves.length, watcher.entries.size());
        assertEquals(1, watcher.endOfDataReceived);

        int aix = 0;
        ZonedDateTime lastCreated = ZonedDateTime.of(2000, 4, 1, 0, 0, 0, 0, ZoneId.systemDefault());
        long lastRevision = -1;

        for (KeyValueEntry kve : watcher.entries) {

            assertTrue(kve.getCreated().isAfter(lastCreated) || kve.getCreated().isEqual(lastCreated));
            lastCreated = kve.getCreated();

            assertTrue(lastRevision < kve.getRevision());
            lastRevision = kve.getRevision();

            Object expected = expectedKves[aix++];
            if (expected == null) {
                assertSame(KeyValueOperation.PUT, kve.getOperation());
                assertTrue(kve.getValue() == null || kve.getValue().length == 0);
                assertEquals(0, kve.getDataLen());
            }
            else if (expected instanceof String) {
                assertSame(KeyValueOperation.PUT, kve.getOperation());
                String s = (String) expected;
                if (watcher.metaOnly) {
                    assertTrue(kve.getValue() == null || kve.getValue().length == 0);
                    assertEquals(s.length(), kve.getDataLen());
                }
                else {
                    assertNotNull(kve.getValue());
                    assertEquals(s.length(), kve.getDataLen());
                    assertEquals(s, kve.getValueAsString());
                }
            }
            else {
                assertTrue(kve.getValue() == null || kve.getValue().length == 0);
                assertEquals(0, kve.getDataLen());
                assertSame(expected, kve.getOperation());
            }
        }
    }
}