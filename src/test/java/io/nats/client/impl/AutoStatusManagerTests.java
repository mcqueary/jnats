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
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.support.IncomingHeadersProcessor;
import io.nats.client.support.Status;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.nats.client.support.NatsJetStreamConstants.CONSUMER_STALLED_HDR;
import static io.nats.client.support.Status.*;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("SameParameterValue")
public class AutoStatusManagerTests extends JetStreamTestBase {

    @Test
    public void testConstruction() throws Exception {
        runInJsServer(nc -> {
            NatsJetStreamSubscription sub = mockSub(nc);

            _pushConstruction(nc, true, true, push_hb_fc(), sub);
            _pushConstruction(nc, true, false, push_hb_xfc(), sub);

            _pushConstruction(nc, false, false, push_xhb_xfc(), sub);
        });
    }

    private void _pushConstruction(Connection conn, boolean hb, boolean fc, SubscribeOptions so, NatsJetStreamSubscription sub) {
        PushAutoStatusManager manager = getManager(conn, so, sub, true, false);
        assertTrue(manager.isSyncMode());
        assertFalse(manager.isQueueMode());
        assertEquals(hb, manager.isHb());
        assertEquals(fc, manager.isFc());

        manager = getManager(conn, so, sub, true, true);
        assertTrue(manager.isSyncMode());
        assertTrue(manager.isQueueMode());
        assertFalse(manager.isHb());
        assertFalse(manager.isFc());
    }

    @Test
    public void test_status_handle_pushSync() throws Exception {
        runInJsServer(nc -> {
            NatsJetStreamSubscription sub = mockSub(nc);
            _status_handle_pushSync(nc, sub, push_hb_fc());
            _status_handle_pushSync(nc, sub, push_hb_xfc());
            _status_handle_pushSync(nc, sub, push_xhb_xfc());
        });
    }

    private void _status_handle_pushSync(Connection conn, NatsJetStreamSubscription sub, SubscribeOptions so) {
        PushAutoStatusManager manager = getManager(conn, so, sub, true, false);
        assertFalse(manager.manage(getTestJsMessage(1)));
        assertTrue(manager.manage(getFlowControl(1)));
        assertTrue(manager.manage(getFcHeartbeat(1)));
        _status_handle_throws(sub, manager, get404());
        _status_handle_throws(sub, manager, get408());
        _status_handle_throws(sub, manager, getUnkStatus());
    }

    @Test
    public void test_status_handle_pull() throws Exception {
        runInJsServer(nc -> {
            NatsJetStreamSubscription sub = mockSub(nc);
            PullAutoStatusManager manager = new PullAutoStatusManager();
            manager.setSub(sub);
            assertFalse(manager.manage(getTestJsMessage(1)));
            assertTrue(manager.manage(get404()));
            assertTrue(manager.manage(get408()));
            _status_handle_throws(sub, manager, getFlowControl(1));
            _status_handle_throws(sub, manager, getFcHeartbeat(1));
            _status_handle_throws(sub, manager, getUnkStatus());
        });
    }

    private void _status_handle_throws(NatsJetStreamSubscription sub, AutoStatusManager asm, Message m) {
        JetStreamStatusException jsse = assertThrows(JetStreamStatusException.class, () -> asm.manage(m));
        assertSame(sub, jsse.getSubscription());
        assertSame(m.getStatus(), jsse.getStatus());
    }

    @Test
    public void test_status_handle_pushAsync() throws Exception {
        AsmEl el = new AsmEl();
        runInJsServer(optsWithEl(el), nc -> {
            NatsJetStreamSubscription sub = mockSub(nc);
            _status_handle_pushAsync(el, nc, sub, push_hb_fc());
            _status_handle_pushAsync(el, nc, sub, push_hb_xfc());
            _status_handle_pushAsync(el, nc, sub, push_xhb_xfc());
        });
    }

    private void _status_handle_pushAsync(AsmEl el, Connection conn, NatsJetStreamSubscription sub, SubscribeOptions so) {
        PushAutoStatusManager manager = getManager(conn, so, sub, false, false);
        el.reset();
        assertFalse(manager.manage(getTestJsMessage(1)));
        assertTrue(manager.manage(getFlowControl(1)));
        assertTrue(manager.manage(getFcHeartbeat(1)));

        Message m = get404();
        assertTrue(manager.manage(m));
        assertSame(sub, el.sub);
        assertSame(m.getStatus(), el.status);

        m = get408();
        assertTrue(manager.manage(m));
        assertSame(sub, el.sub);
        assertSame(m.getStatus(), el.status);

        m = getUnkStatus();
        assertTrue(manager.manage(m));
        assertSame(sub, el.sub);
        assertSame(m.getStatus(), el.status);
    }

    @Test
    public void test_push_fc() {
        SubscribeOptions so = push_hb_fc();
        MockPublishInternal mc = new MockPublishInternal();
        PushAutoStatusManager asm = new PushAutoStatusManager(mc, so, so.getConsumerConfiguration(), false, true);
        assertNull(asm.getLastFcSubject());
        asm.manage(getFlowControl(1));
        assertEquals(getFcSubject(1), asm.getLastFcSubject());
        assertEquals(getFcSubject(1), mc.fcSubject);
        assertEquals(1, mc.pubCount);

        asm.manage(getFlowControl(1)); // duplicate should not call publish
        assertEquals(getFcSubject(1), asm.getLastFcSubject());
        assertEquals(getFcSubject(1), mc.fcSubject);
        assertEquals(1, mc.pubCount);

        asm.manage(getFlowControl(2)); // duplicate should not call publish
        assertEquals(getFcSubject(2), asm.getLastFcSubject());
        assertEquals(getFcSubject(2), mc.fcSubject);
        assertEquals(2, mc.pubCount);

        asm.manage(getFcHeartbeat(2)); // duplicate should not call publish
        assertEquals(getFcSubject(2), asm.getLastFcSubject());
        assertEquals(getFcSubject(2), mc.fcSubject);
        assertEquals(2, mc.pubCount);

        asm.manage(getFcHeartbeat(3));
        assertEquals(getFcSubject(3), asm.getLastFcSubject());
        assertEquals(getFcSubject(3), mc.fcSubject);
        assertEquals(3, mc.pubCount);
    }

    @Test
    public void test_push_xfc() {
        _push_xfc(push_hb_xfc());
        _push_xfc(push_xhb_xfc());
    }

    private void _push_xfc(SubscribeOptions so) {
        MockPublishInternal mc = new MockPublishInternal();
        PushAutoStatusManager asm = new PushAutoStatusManager(mc, so, so.getConsumerConfiguration(), false, true);
        assertNull(asm.getLastFcSubject());

        asm.manage(getFlowControl(1));
        assertNull(asm.getLastFcSubject());
        assertNull(mc.fcSubject);
        assertEquals(0, mc.pubCount);

        asm.manage(getHeartbeat());
        assertNull(asm.getLastFcSubject());
        assertNull(mc.fcSubject);
        assertEquals(0, mc.pubCount);
    }

    @Test
    public void test_received_time() throws Exception {
        runInJsServer(nc -> {
            createDefaultTestStream(nc);
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            _received_time_yes(push_hb_fc(), js);
            _received_time_yes(push_hb_xfc(), js);
            _received_time_no(js, jsm, js.subscribe(SUBJECT, push_xhb_xfc()));
        });
    }

    private void _received_time_yes(PushSubscribeOptions so, JetStream js) throws Exception {
        long before = System.currentTimeMillis();
        NatsJetStreamSubscription sub = (NatsJetStreamSubscription) js.subscribe(SUBJECT, so);

        // during the sleep, the heartbeat is delivered and is checked
        // by the heartbeat listener and recorded as received
        sleep(1050); // slightly longer than the idle heartbeat

        long preTime = ((PushAutoStatusManager)sub.getAsm()).getLastMsgReceived();
        assertTrue(preTime > before);
        sub.unsubscribe();
    }

    private void _received_time_no(JetStream js, JetStreamManagement jsm, JetStreamSubscription sub) throws IOException, JetStreamApiException, InterruptedException {
        js.publish(SUBJECT, dataBytes(0));
        sub.nextMessage(1000);
        NatsJetStreamSubscription nsub = (NatsJetStreamSubscription)sub;
        PushAutoStatusManager pasm = (PushAutoStatusManager)nsub.getAsm();
        assertEquals(0, pasm.getLastMsgReceived());
        jsm.purgeStream(STREAM);
        sub.unsubscribe();
    }

    @Test
    public void test_hb_yes_settings() throws Exception {
        runInJsServer(nc -> {
            NatsJetStreamSubscription sub = mockSub(nc);

            ConsumerConfiguration cc = ConsumerConfiguration.builder().idleHeartbeat(1000).build();

            // MessageAlarmTime default
            PushSubscribeOptions so = new PushSubscribeOptions.Builder().configuration(cc).build();
            PushAutoStatusManager manager = getManager(nc, so, sub);
            assertEquals(1000, manager.getIdleHeartbeatSetting());
            assertEquals(3000, manager.getAlarmPeriodSetting());

            // MessageAlarmTime < idleHeartbeat
            so = new PushSubscribeOptions.Builder().configuration(cc).messageAlarmTime(999).build();
            manager = getManager(nc, so, sub);
            assertEquals(1000, manager.getIdleHeartbeatSetting());
            assertEquals(3000, manager.getAlarmPeriodSetting());

            // MessageAlarmTime == idleHeartbeat
            so = new PushSubscribeOptions.Builder().configuration(cc).messageAlarmTime(1000).build();
            manager = getManager(nc, so, sub);
            assertEquals(1000, manager.getIdleHeartbeatSetting());
            assertEquals(1000, manager.getAlarmPeriodSetting());

            // MessageAlarmTime > idleHeartbeat
            so = new PushSubscribeOptions.Builder().configuration(cc).messageAlarmTime(2000).build();
            manager = getManager(nc, so, sub);
            assertEquals(1000, manager.getIdleHeartbeatSetting());
            assertEquals(2000, manager.getAlarmPeriodSetting());
        });
    }

    @Test
    public void test_hb_no_settings() throws Exception {
        runInJsServer(nc -> {
            NatsJetStreamSubscription sub = mockSub(nc);
            SubscribeOptions so = push_xhb_xfc();
            PushAutoStatusManager manager = getManager(nc, so, sub);
            assertEquals(0, manager.getIdleHeartbeatSetting());
            assertEquals(0, manager.getAlarmPeriodSetting());
        });
    }

    private ConsumerConfiguration cc_fc_hb() {
        return ConsumerConfiguration.builder().flowControl(1000).build();
    }

    private ConsumerConfiguration cc_xfc_hb() {
        return ConsumerConfiguration.builder().idleHeartbeat(1000).build();
    }

    private ConsumerConfiguration cc_xfc_xhb() {
        return ConsumerConfiguration.builder().build();
    }

    private PushSubscribeOptions push_hb_fc() {
        return new PushSubscribeOptions.Builder().configuration(cc_fc_hb()).build();
    }

    private PushSubscribeOptions push_hb_xfc() {
        return new PushSubscribeOptions.Builder().configuration(cc_xfc_hb()).build();
    }

    private PushSubscribeOptions push_xhb_xfc() {
        return new PushSubscribeOptions.Builder().configuration(cc_xfc_xhb()).build();
    }

    private PushAutoStatusManager getManager(Connection conn, SubscribeOptions so, NatsJetStreamSubscription sub) {
        return getManager(conn, so, sub, true, false);
    }

    private PushAutoStatusManager getManager(Connection conn, SubscribeOptions so, NatsJetStreamSubscription sub, boolean syncMode, boolean queueMode) {
        PushAutoStatusManager asm = new PushAutoStatusManager((NatsConnection)conn, so, so.getConsumerConfiguration(), queueMode, syncMode);
        asm.setSub(sub);
        return asm;
    }

    private NatsMessage getFlowControl(int replyToId) {
        NatsMessage.InternalMessageFactory imf = new NatsMessage.InternalMessageFactory("sid", "subj", getFcSubject(replyToId), 0, false);
        imf.setHeaders(new IncomingHeadersProcessor(("NATS/1.0 " + FLOW_OR_HEARTBEAT_STATUS_CODE + " " + FLOW_CONTROL_TEXT + "\r\n").getBytes()));
        return imf.getMessage();
    }

    private String getFcSubject(int id) {
        return "fcSubject." + id;
    }

    private NatsMessage getFcHeartbeat(int replyToId) {
        NatsMessage.InternalMessageFactory imf = new NatsMessage.InternalMessageFactory("sid", "subj", null, 0, false);
        String s = "NATS/1.0 " + FLOW_OR_HEARTBEAT_STATUS_CODE + " " + HEARTBEAT_TEXT + "\r\n" + CONSUMER_STALLED_HDR + ":" + getFcSubject(replyToId) + "\r\n\r\n";
        imf.setHeaders(new IncomingHeadersProcessor(s.getBytes()));
        return imf.getMessage();
    }

    private NatsMessage getHeartbeat() {
        NatsMessage.InternalMessageFactory imf = new NatsMessage.InternalMessageFactory("sid", "subj", null, 0, false);
        String s = "NATS/1.0 " + FLOW_OR_HEARTBEAT_STATUS_CODE + " " + HEARTBEAT_TEXT + "\r\n";
        imf.setHeaders(new IncomingHeadersProcessor(s.getBytes()));
        return imf.getMessage();
    }

    private NatsMessage get404() {
        return getStatus(404, "not found");
    }

    private NatsMessage get408() {
        return getStatus(408, "expired");
    }

    private NatsMessage getUnkStatus() {
        return getStatus(999, "blah blah");
    }

    private NatsMessage getStatus(int code, String message) {
        NatsMessage.InternalMessageFactory imf = new NatsMessage.InternalMessageFactory("sid", "subj", null, 0, false);
        imf.setHeaders(new IncomingHeadersProcessor(("NATS/1.0 " + code + " " + message + "\r\n").getBytes()));
        return imf.getMessage();
    }

    static class AsmEl implements ErrorListener {
        JetStreamSubscription sub;
        long lastStreamSequence = -1;
        long lastConsumerSequence = -1;
        long expectedConsumerSeq = -1;
        long receivedConsumerSeq = -1;
        Status status;

        public void reset() {
            sub = null;
            expectedConsumerSeq = -1;
            receivedConsumerSeq = -1;
            status = null;
        }

        @Override
        public void errorOccurred(Connection conn, String error) {}

        @Override
        public void exceptionOccurred(Connection conn, Exception exp) {}

        @Override
        public void slowConsumerDetected(Connection conn, Consumer consumer) {}

        @Override
        public void unhandledStatus(Connection conn, JetStreamSubscription sub, Status status) {
            this.sub = sub;
            this.status = status;
        }
    }

    static class MockPublishInternal extends NatsConnection {
        int pubCount;
        String fcSubject;

        public MockPublishInternal() {
            this(new Options.Builder().build());
        }

        public MockPublishInternal(Options options) {
            super(options);
        }

        @Override
        void publishInternal(String subject, String replyTo, Headers headers, byte[] data, boolean utf8mode) {
            fcSubject = subject;
            ++pubCount;
        }
    }

    private NatsJetStreamSubscription mockSub(Connection nc) throws IOException, JetStreamApiException {
        createDefaultTestStream(nc);
        JetStream js = nc.jetStream();
        return (NatsJetStreamSubscription) js.subscribe(SUBJECT);
    }
}
