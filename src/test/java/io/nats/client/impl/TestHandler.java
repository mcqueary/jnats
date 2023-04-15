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

package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.support.Status;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class TestHandler implements ErrorListener, ConnectionListener {
    private final ReentrantLock lock = new ReentrantLock();

    private final AtomicInteger count = new AtomicInteger();
    private final AtomicInteger exceptionCount = new AtomicInteger();
    private final HashMap<Events,AtomicInteger> eventCounts = new HashMap<>();
    private final HashMap<String,AtomicInteger> errorCounts = new HashMap<>();

    private Connection lastEventConnection;

    private CompletableFuture<Boolean> statusChanged;
    private CompletableFuture<Boolean> slowSubscriber;
    private CompletableFuture<Boolean> errorWaitFuture;
    private CompletableFuture<HeartbeatAlarmEvent> heartbeatAlarmEventWaitFuture;
    private CompletableFuture<StatusEvent> pullStatusWarningWaitFuture;
    private CompletableFuture<StatusEvent> pullStatusErrorWaitFuture;
    private Events eventToWaitFor;
    private String errorToWaitFor;

    private final List<Consumer> slowConsumers = new ArrayList<>();
    private final List<Message> discardedMessages = new ArrayList<>();
    private final List<StatusEvent> unhandledStatuses = new ArrayList<>();
    private final List<StatusEvent> pullStatusWarnings = new ArrayList<>();
    private final List<StatusEvent> pullStatusErrors = new ArrayList<>();
    private final List<HeartbeatAlarmEvent> heartbeatAlarms = new ArrayList<>();
    private final List<FlowControlProcessedEvent> flowControlProcessedEvents = new ArrayList<>();

    private final boolean printExceptions;
    private final boolean verbose;

    public TestHandler() {
        this(false, false);
    }

    public TestHandler(boolean printExceptions, boolean verbose) {
        this.printExceptions = printExceptions;
        this.verbose = verbose;
    }

    public void reset() {
        count.set(0);
        exceptionCount.set(0);
        eventCounts.clear();
        errorCounts.clear();
        lastEventConnection = null;
        statusChanged = null;
        slowSubscriber = null;
        errorWaitFuture = null;
        heartbeatAlarmEventWaitFuture = null;
        pullStatusWarningWaitFuture = null;
        pullStatusErrorWaitFuture = null;
        eventToWaitFor = null;
        errorToWaitFor = null;
        slowConsumers.clear();
        discardedMessages.clear();
        unhandledStatuses.clear();
        pullStatusWarnings.clear();
        pullStatusErrors.clear();
        heartbeatAlarms.clear();
        flowControlProcessedEvents.clear();
    }

    private boolean waitForBooleanFuture(CompletableFuture<Boolean> future, long timeout, TimeUnit units) {
        try {
            return future.get(timeout, units);
        } catch (TimeoutException | ExecutionException | InterruptedException e) {
            if (printExceptions) {
                e.printStackTrace();
            }
            return false;
        }
    }

    private <T> T waitForFuture(CompletableFuture<T> future, long waitInMillis) {
        try {
            return future.get(waitInMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | ExecutionException | InterruptedException e) {
            if (printExceptions) {
                e.printStackTrace();
            }
            return null;
        }
    }

    public void prepForStatusChange(Events waitFor) {
        lock.lock();
        try {
            statusChanged = new CompletableFuture<>();
            eventToWaitFor = waitFor;
            if (verbose) {
                report("prepForStatusChange",  waitFor);
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean waitForStatusChange(long timeout, TimeUnit units) {
        return waitForBooleanFuture(statusChanged, timeout, units);
    }

    public void exceptionOccurred(Connection conn, Exception exp) {
        lastEventConnection = conn;
        count.incrementAndGet();
        exceptionCount.incrementAndGet();

        if (exp != null) {
            if (verbose) {
                report("exceptionOccurred",  exp);
            }
            else if (printExceptions) {
                exp.printStackTrace();
            }
        }
    }

    public void prepForError(String waitFor) {
        lock.lock();
        try {
            errorWaitFuture = new CompletableFuture<>();
            errorToWaitFor = waitFor;
            if (verbose) {
                report("prepForError",  waitFor);
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean waitForError(long timeout, TimeUnit units) {
        return waitForBooleanFuture(errorWaitFuture, timeout, units);
    }

    public void errorOccurred(Connection conn, String type) {
        lastEventConnection = conn;
        count.incrementAndGet();

        lock.lock();
        try {
            AtomicInteger counter = errorCounts.get(type);
            if (counter == null) {
                counter = new AtomicInteger();
                errorCounts.put(type, counter);
            }
            counter.incrementAndGet();
            if (errorWaitFuture != null && type.equals(errorToWaitFor)) {
                errorWaitFuture.complete(Boolean.TRUE);
            }
            if (verbose) {
                report("errorOccurred",  type);
            }
        } finally {
            lock.unlock();
        }
    }

    public void messageDiscarded(Connection conn, Message msg) {
        lastEventConnection = conn;
        count.incrementAndGet();

        lock.lock();
        try {
            discardedMessages.add(msg);
            if (verbose) {
                report("messageDiscarded",  msg);
            }
        } finally {
            lock.unlock();
        }
    }

    public void connectionEvent(Connection conn, Events type) {
        lastEventConnection = conn;
        count.incrementAndGet();

        lock.lock();
        try {
            AtomicInteger counter = eventCounts.get(type);
            if (counter == null) {
                counter = new AtomicInteger();
                eventCounts.put(type, counter);
            }
            counter.incrementAndGet();
            if (statusChanged != null && type == eventToWaitFor) {
                statusChanged.complete(Boolean.TRUE);
            }
            if (verbose) {
                report("connectionEvent",  type);
            }
        } finally {
            lock.unlock();
        }
    }

    public Future<Boolean> waitForSlow() {
        slowSubscriber = new CompletableFuture<>();
        return slowSubscriber;
    }

    public void slowConsumerDetected(Connection conn, Consumer consumer) {
        count.incrementAndGet();

        lock.lock();
        try {
            slowConsumers.add(consumer);
            if (slowSubscriber != null) {
                slowSubscriber.complete(true);
            }
            if (verbose) {
                String msg;
                if (consumer instanceof NatsSubscription) {
                    NatsSubscription nats = (NatsSubscription)consumer;
                    msg = "Subscription " + nats.getSID() + " for " + nats.getSubject();
                }
                else if (consumer instanceof NatsDispatcher) {
                    NatsDispatcher nats = (NatsDispatcher)consumer;
                    msg = "Dispatcher " + nats.getId();
                }
                else {
                    msg = consumer.toString();
                }
                report("slowConsumerDetected",  msg);
            }
        } finally {
            lock.unlock();
        }
    }

    private void report(String func, Object message) {
        System.out.println("" + System.currentTimeMillis() + " [TestHelper." + func + "] " + message);
    }

    public List<Consumer> getSlowConsumers() {
        return slowConsumers;
    }

    public List<Message> getDiscardedMessages() {
        return discardedMessages;
    }

    public List<StatusEvent> getUnhandledStatuses() {
        return unhandledStatuses;
    }

    public List<StatusEvent> getPullStatusWarnings() {
        return pullStatusWarnings;
    }

    public List<StatusEvent> getPullStatusErrors() {
        return pullStatusErrors;
    }

    public List<HeartbeatAlarmEvent> getHeartbeatAlarms() {
        return heartbeatAlarms;
    }

    public List<FlowControlProcessedEvent> getFlowControlProcessedEvents() {
        return flowControlProcessedEvents;
    }

    public int getCount() {
        return count.get();
    }

    public int getExceptionCount() {
        return exceptionCount.get();
    }

    public int getEventCount(Events type) {
        int retVal = 0;
        lock.lock();
        try {
            AtomicInteger counter = eventCounts.get(type);
            if (counter != null) {
                retVal = counter.get();
            }
        } finally {
            lock.unlock();
        }
        return retVal;
    }

    public int getErrorCount(String type) {
        int retVal = 0;
        lock.lock();
        try {
            AtomicInteger counter = errorCounts.get(type);
            if (counter != null) {
                retVal = counter.get();
            }
        } finally {
            lock.unlock();
        }
        return retVal;
    }

    public void dumpErrorCountsToStdOut() {
        lock.lock();
        try {
            System.out.println("#### Test Handler Error Counts ####");
            for (String key : errorCounts.keySet()) {
                int count = errorCounts.get(key).get();
                System.out.println(key+": "+count);
            }
        } finally {
            lock.unlock();
        }
    }

    public Connection getLastEventConnection() {
        return lastEventConnection;
    }

    @Override
    public void unhandledStatus(Connection conn, JetStreamSubscription sub, Status status) {
        unhandledStatuses.add(new StatusEvent(sub, status));
    }

    public void prepForPullStatusWarning() {
        lock.lock();
        try {
            pullStatusWarningWaitFuture = new CompletableFuture<>();
        }
        finally {
            lock.unlock();
        }
    }

    public StatusEvent waitForPullStatusWarning(long waitInMillis) {
        return waitForFuture(pullStatusWarningWaitFuture, waitInMillis);
    }

    @Override
    public void pullStatusWarning(Connection conn, JetStreamSubscription sub, Status status) {
        lock.lock();
        try {
            StatusEvent event = new StatusEvent(sub, status);
            pullStatusWarnings.add(event);
            if (pullStatusWarningWaitFuture != null) {
                pullStatusWarningWaitFuture.complete(event);
            }
        } finally {
            lock.unlock();
        }
    }

    public void prepForPullStatusError() {
        lock.lock();
        try {
            pullStatusErrorWaitFuture = new CompletableFuture<>();
        }
        finally {
            lock.unlock();
        }
    }

    public StatusEvent waitForPullStatusError(long waitInMillis) {
        return waitForFuture(pullStatusErrorWaitFuture, waitInMillis);
    }

    @Override
    public void pullStatusError(Connection conn, JetStreamSubscription sub, Status status) {
        lock.lock();
        try {
            StatusEvent event = new StatusEvent(sub, status);
            pullStatusErrors.add(event);
            if (pullStatusErrorWaitFuture != null) {
                pullStatusErrorWaitFuture.complete(event);
            }
        } finally {
            lock.unlock();
        }
    }

    public void prepForHeartbeatAlarm() {
        lock.lock();
        try {
            heartbeatAlarmEventWaitFuture = new CompletableFuture<>();
        }
        finally {
            lock.unlock();
        }
    }

    public HeartbeatAlarmEvent waitForHeartbeatAlarm(long waitInMillis) {
        return waitForFuture(heartbeatAlarmEventWaitFuture, waitInMillis);
    }

    @Override
    public void heartbeatAlarm(Connection conn, JetStreamSubscription sub, long lastStreamSequence, long lastConsumerSequence) {
        lock.lock();
        try {
            HeartbeatAlarmEvent event = new HeartbeatAlarmEvent(sub, lastStreamSequence, lastConsumerSequence);
            heartbeatAlarms.add(event);
            if (heartbeatAlarmEventWaitFuture != null) {
                heartbeatAlarmEventWaitFuture.complete(event);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String subject, FlowControlSource source) {
        flowControlProcessedEvents.add(new FlowControlProcessedEvent(sub, subject, source));
    }

    public static class StatusEvent {
        String sid;
        Status status;

        public StatusEvent(JetStreamSubscription sub, Status status) {
            this.sid = extractSid(sub);
            this.status = status;
        }

        @Override
        public String toString() {
            return "StatusEvent{" +
                "sid='" + sid + '\'' +
                ", status=" + status +
                '}';
        }
    }

    public static class HeartbeatAlarmEvent {
        String sid;
        long lastStreamSequence;
        long lastConsumerSequence;

        public HeartbeatAlarmEvent(JetStreamSubscription sub, long lastStreamSequence, long lastConsumerSequence) {
            this.sid = extractSid(sub);
            this.lastStreamSequence = lastStreamSequence;
            this.lastConsumerSequence = lastConsumerSequence;
        }

        @Override
        public String toString() {
            return "HeartbeatAlarmEvent{" +
                "sid='" + sid + '\'' +
                ", lastStreamSequence=" + lastStreamSequence +
                ", lastConsumerSequence=" + lastConsumerSequence +
                '}';
        }
    }

    public static class FlowControlProcessedEvent {
        String sid;
        String subject;
        FlowControlSource source;

        public FlowControlProcessedEvent(JetStreamSubscription sub, String subject, FlowControlSource source) {
            this.sid = extractSid(sub);
            this.subject = subject;
            this.source = source;
        }

        @Override
        public String toString() {
            return "FlowControlEvent{" +
                "sid='" + sid + '\'' +
                ", subject='" + subject + '\'' +
                ", source=" + source +
                '}';
        }
    }

    private static String extractSid(JetStreamSubscription sub) {
        return ((NatsJetStreamSubscription)sub).getSID();
    }
}