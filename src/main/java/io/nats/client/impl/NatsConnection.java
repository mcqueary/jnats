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
import io.nats.client.ConnectionListener.Events;
import io.nats.client.api.ServerInfo;
import io.nats.client.support.ByteArrayBuilder;
import io.nats.client.support.NatsRequestCompletableFuture;
import io.nats.client.support.NatsUri;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.nats.client.support.NatsConstants.*;
import static io.nats.client.support.NatsRequestCompletableFuture.CancelAction;
import static io.nats.client.support.Validator.validateNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

class NatsConnection implements Connection {

    private final Options options;

    private final NatsStatistics statistics;

    private boolean connecting; // you can only connect in one thread
    private boolean disconnecting; // you can only disconnect in one thread
    private boolean closing; // respect a close call regardless
    private Exception exceptionDuringConnectChange; // an exception occurred in another thread while disconnecting or
                                                    // connecting

    private Status status;
    private final ReentrantLock statusLock;
    private final Condition statusChanged;

    private CompletableFuture<DataPort> dataPortFuture;
    private DataPort dataPort;
    private NatsUri currentServer;
    private CompletableFuture<Boolean> reconnectWaiter;
    private final HashMap<NatsUri, String> serverAuthErrors;

    private final NatsConnectionReader reader;
    private final NatsConnectionWriter writer;

    private final AtomicReference<ServerInfo> serverInfo;

    private final Map<String, NatsSubscription> subscribers;
    private final Map<String, NatsDispatcher> dispatchers; // use a concurrent map so we get more consistent iteration
                                                     // behavior
    private final Collection<ConnectionListener> connectionListeners; 
    private final Map<String, NatsRequestCompletableFuture> responsesAwaiting;
    private final Map<String, NatsRequestCompletableFuture> responsesRespondedTo;
    private final ConcurrentLinkedDeque<CompletableFuture<Boolean>> pongQueue;

    private final String mainInbox;
    private final AtomicReference<NatsDispatcher> inboxDispatcher;
    private Timer timer;

    private final AtomicBoolean needPing;

    private final AtomicLong nextSid;
    private final NUID nuid;

    private final AtomicReference<String> connectError;
    private final AtomicReference<String> lastError;
    private final AtomicReference<CompletableFuture<Boolean>> draining;
    private final AtomicBoolean blockPublishForDrain;

    private final ExecutorService callbackRunner;

    private final ExecutorService executor;
    private final ExecutorService connectExecutor;
    private final boolean advancedTracking;

    private final ServerPool serverPool;
    private final CancelAction cancelAction;

    NatsConnection(Options options) {
        boolean trace = options.isTraceConnection();
        timeTrace(trace, "creating connection object");

        this.options = options;

        advancedTracking = options.isTrackAdvancedStats();
        this.statistics = new NatsStatistics(advancedTracking);

        this.statusLock = new ReentrantLock();
        this.statusChanged = this.statusLock.newCondition();
        this.status = Status.DISCONNECTED;
        this.reconnectWaiter = new CompletableFuture<>();
        this.reconnectWaiter.complete(Boolean.TRUE);

        this.connectionListeners = ConcurrentHashMap.newKeySet();
        if (options.getConnectionListener() != null) {
            addConnectionListener(options.getConnectionListener());
        }

        this.dispatchers = new ConcurrentHashMap<>();
        this.subscribers = new ConcurrentHashMap<>();
        this.responsesAwaiting = new ConcurrentHashMap<>();
        this.responsesRespondedTo = new ConcurrentHashMap<>();

        this.serverAuthErrors = new HashMap<>();

        this.nextSid = new AtomicLong(1);
        timeTrace(trace, "creating NUID");
        this.nuid = new NUID();
        this.mainInbox = createInbox() + ".*";

        this.lastError = new AtomicReference<>();
        this.connectError = new AtomicReference<>();

        this.serverInfo = new AtomicReference<>();
        this.inboxDispatcher = new AtomicReference<>();
        this.pongQueue = new ConcurrentLinkedDeque<>();
        this.draining = new AtomicReference<>();
        this.blockPublishForDrain = new AtomicBoolean();

        timeTrace(trace, "creating executors");
        this.callbackRunner = Executors.newSingleThreadExecutor();
        this.executor = options.getExecutor();
        this.connectExecutor = Executors.newSingleThreadExecutor();

        timeTrace(trace, "creating reader and writer");
        this.reader = new NatsConnectionReader(this);
        this.writer = new NatsConnectionWriter(this);

        this.needPing = new AtomicBoolean(true);

        serverPool = options.getServerPool() == null ? new NatsServerPool() : options.getServerPool();
        serverPool.initialize(options);

        cancelAction = options.isReportNoResponders() ? CancelAction.REPORT : CancelAction.CANCEL;

        timeTrace(trace, "connection object created");
    }

    // Connect is only called after creation
    void connect(boolean reconnectOnConnect) throws InterruptedException, IOException {
        if (options.getServers().size() == 0) {
            throw new IllegalArgumentException("No servers provided in options");
        }

        boolean trace = options.isTraceConnection();
        long start = System.nanoTime();

        this.lastError.set("");

        timeTrace(trace, "starting connect loop");

        Set<NatsUri> failList = new HashSet<>();
        boolean keepGoing = true;
        NatsUri first = null;
        NatsUri cur;
        while (keepGoing && (cur = serverPool.peekNextServer()) != null) {
            if (first == null) {
                first = cur;
            }
            else if (cur.equals(first)) {
                break;  // connect only goes through loop once
            }
            serverPool.nextServer(); // b/c we only peeked.

            // let server pool resolve hostnames, then loop through resolved
            List<NatsUri> resolvedList = resolveHost(cur);
            while (resolvedList.size() > 0) {
                if (isClosed()) {
                    keepGoing = false;
                    break;
                }
                connectError.set(""); // new on each attempt

                timeTrace(trace, "setting status to connecting");
                updateStatus(Status.CONNECTING);

                timeTrace(trace, "trying to connect to %s", cur);
                NatsUri resolved = resolvedList.remove(0);
                tryToConnect(cur, resolved, System.nanoTime());

                if (isConnected()) {
                    serverPool.connectSucceeded(cur);
                    keepGoing = false;
                    break;
                }

                timeTrace(trace, "setting status to disconnected");
                updateStatus(Status.DISCONNECTED);

                failList.add(cur);
                serverPool.connectFailed(cur);

                String err = connectError.get();

                if (this.isAuthenticationError(err)) {
                    this.serverAuthErrors.put(resolved, err);
                }
            }
        }

        if (!isConnected() && !isClosed()) {
            if (reconnectOnConnect) {
                timeTrace(trace, "trying to reconnect on connect");
                reconnect();
            }
            else {
                timeTrace(trace, "connection failed, closing to cleanup");
                close();

                String err = connectError.get();
                if (this.isAuthenticationError(err)) {
                    String msg = "Authentication error connecting to NATS server: " + err;
                    throw new AuthenticationException(msg);
                } else {
                    String msg = "Unable to connect to NATS servers: " + failList;
                    throw new IOException(msg);
                }
            }
        }
        else if (trace) {
            long end = System.nanoTime();
            double seconds = ((double) (end - start)) / 1_000_000_000.0;
            timeTrace(trace, "connect complete in %.3f seconds", seconds);
        }
    }

    // Reconnect can only be called when the connection is disconnected
    void reconnect() throws InterruptedException {
        if (isClosed()) {
            return;
        }

        if (options.getMaxReconnect() == 0) {
            this.close();
            return;
        }

        writer.setReconnectMode(true);

        if (!isConnected() && !isClosed() && !this.isClosing()) {
            boolean keepGoing = true;
            int timesAllServersSeen = -1;
            NatsUri first = null;
            NatsUri cur;
            while (keepGoing && (cur = serverPool.nextServer()) != null) {
                if (++timesAllServersSeen == 0) {
                    first = cur;
                }
                else if (first.equals(cur)) {
                    // went around the pool an entire time
                    invokeReconnectDelayHandler(timesAllServersSeen);
                }

                // let server list provider resolve hostnames
                // then loop through resolved
                List<NatsUri> resolvedList = resolveHost(cur);
                while (keepGoing && resolvedList.size() > 0) {
                    if (isClosed()) {
                        keepGoing = false;
                    }
                    else {
                        connectError.set(""); // reset on each loop
                        if (isDisconnectingOrClosed() || this.isClosing()) {
                            keepGoing = false;
                        }
                        else {
                            updateStatus(Status.RECONNECTING);

                            NatsUri resolved = resolvedList.remove(0);
                            tryToConnect(cur, resolved, System.nanoTime());

                            if (isConnected()) {
                                serverPool.connectSucceeded(cur);
                                statistics.incrementReconnects();
                                keepGoing = false;
                            }
                            else {
                                serverPool.connectFailed(cur);
                                String err = connectError.get();
                                if (this.isAuthenticationError(err)) {
                                    if (err.equals(this.serverAuthErrors.get(resolved))) {
                                        keepGoing = false; // double auth error
                                    }
                                    else {
                                        serverAuthErrors.put(resolved, err);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } // end-main-loop

        if (!isConnected()) {
            this.close();
            return;
        }

        this.subscribers.forEach((sid, sub) -> {
            if (sub.getDispatcher() == null && !sub.isDraining()) {
                sendSubscriptionMessage(sub.getSID(), sub.getSubject(), sub.getQueueName(), true);
            }
        });

        this.dispatchers.forEach((nuid, d) -> {
            if (!d.isDraining()) {
                d.resendSubscriptions();
            }
        });

        try {
            this.flush(this.options.getConnectionTimeout());
        } catch (Exception exp) {
            this.processException(exp);
        }

        // When the flush returns we are done sending internal messages, so we can
        // switch to the
        // non-reconnect queue
        this.writer.setReconnectMode(false);

        processConnectionEvent(Events.RESUBSCRIBED);
    }

    void timeTrace(boolean trace, String format, Object... args) {
        if (trace) {
            _trace(String.format(format, args));
        }
    }

    void timeTrace(boolean trace, String message) {
        if (trace) {
            _trace(message);
        }
    }

    private void _trace(String message) {
        String timeStr = DateTimeFormatter.ISO_TIME.format(LocalDateTime.now());
        System.out.println("[" + timeStr + "] connect trace: " + message);
    }

    long timeCheck(boolean trace, long endNanos, String message) throws TimeoutException {
        long now = System.nanoTime();
        long remaining = endNanos - now;

        if (trace) {
            double seconds = ((double)remaining) / 1_000_000_000.0;
            _trace( message + String.format(", %.3f (s) remaining", seconds) );
        }

        if (remaining < 0) {
            throw new TimeoutException("connection timed out");
        }

        return remaining;
    }

    // is called from reconnect and connect
    // will wait for any previous attempt to complete, using the reader.stop and
    // writer.stop
    void tryToConnect(NatsUri cur, NatsUri resolved, long now) {
        currentServer = null;

        try {
            Duration connectTimeout = options.getConnectionTimeout();
            boolean trace = options.isTraceConnection();
            long end = now + connectTimeout.toNanos();
            timeCheck(trace, end, "starting connection attempt");

            statusLock.lock();
            try {
                if (this.connecting) {
                    return;
                }
                this.connecting = true;
                statusChanged.signalAll();
            } finally {
                statusLock.unlock();
            }

            // Create a new future for the dataport, the reader/writer will use this
            // to wait for the connect/failure.
            this.dataPortFuture = new CompletableFuture<>();

            // Make sure the reader and writer are stopped
            long timeoutNanos = timeCheck(trace, end, "waiting for reader");
            this.reader.stop().get(timeoutNanos, TimeUnit.NANOSECONDS);
            timeoutNanos = timeCheck(trace, end, "waiting for writer");
            this.writer.stop().get(timeoutNanos, TimeUnit.NANOSECONDS);

            timeCheck(trace, end, "cleaning pong queue");
            cleanUpPongQueue();

            timeoutNanos = timeCheck(trace, end, "connecting data port");
            DataPort newDataPort = this.options.buildDataPort();
            newDataPort.connect(resolved.toString(), this, timeoutNanos);

            // Notify any threads waiting on the sockets
            this.dataPort = newDataPort;
            this.dataPortFuture.complete(this.dataPort);

            // Wait for the INFO message manually
            // all other traffic will use the reader and writer
            Callable<Object> connectTask = () -> {
                readInitialInfo();
                checkVersionRequirements();
                long start = System.nanoTime();
                upgradeToSecureIfNeeded(resolved);
                if (trace && options.isTLSRequired()) {
                    // If the time appears too long it might be related to
                    // https://github.com/nats-io/nats.java#linux-platform-note
                    timeTrace(true, "TLS upgrade took: %.3f (s)",
                            ((double) (System.nanoTime() - start)) / 1_000_000_000.0);
                }
                return null;
            };

            timeoutNanos = timeCheck(trace, end, "reading info, version and upgrading to secure if necessary");
            Future<Object> future = this.connectExecutor.submit(connectTask);
            try {
                future.get(timeoutNanos, TimeUnit.NANOSECONDS);
            } finally {
                future.cancel(true);
            }

            // start the reader and writer after we secured the connection, if necessary
            timeCheck(trace, end, "starting reader");
            this.reader.start(this.dataPortFuture);
            timeCheck(trace, end, "starting writer");
            this.writer.start(this.dataPortFuture);

            timeCheck(trace, end, "sending connect message");
            this.sendConnect(resolved);

            timeoutNanos = timeCheck(trace, end, "sending initial ping");
            Future<Boolean> pongFuture = sendPing();

            if (pongFuture != null) {
                pongFuture.get(timeoutNanos, TimeUnit.NANOSECONDS);
            }

            if (this.timer == null) {
                timeCheck(trace, end, "starting ping and cleanup timers");
                this.timer = new Timer("Nats Connection Timer");

                long pingMillis = this.options.getPingInterval().toMillis();

                if (pingMillis > 0) {
                    this.timer.schedule(new TimerTask() {
                        public void run() {
                            if (isConnected()) {
                                softPing(); // The timer always uses the standard queue
                            }
                        }
                    }, pingMillis, pingMillis);
                }

                long cleanMillis = this.options.getRequestCleanupInterval().toMillis();

                if (cleanMillis > 0) {
                    this.timer.schedule(new TimerTask() {
                        public void run() {
                            cleanResponses(false);
                        }
                    }, cleanMillis, cleanMillis);
                }
            }

            // Set connected status
            timeCheck(trace, end, "updating status to connected");
            statusLock.lock();
            try {
                this.connecting = false;

                if (this.exceptionDuringConnectChange != null) {
                    throw this.exceptionDuringConnectChange;
                }

                this.currentServer = cur;
                this.serverAuthErrors.remove(resolved); // reset on successful connection
                updateStatus(Status.CONNECTED); // will signal status change, we also signal in finally
            } finally {
                statusLock.unlock();
            }
            timeTrace(trace, "status updated");
        } catch (RuntimeException exp) { // runtime exceptions, like illegalArgs
            processException(exp);
            throw exp;
        } catch (Exception exp) { // every thing else
            processException(exp);
            try {
                this.closeSocket(false);
            } catch (InterruptedException e) {
                processException(e);
            }
        } finally {
            statusLock.lock();
            try {
                this.connecting = false;
                statusChanged.signalAll();
            } finally {
                statusLock.unlock();
            }
        }
    }

    void checkVersionRequirements() throws IOException {
        Options opts = getOptions();
        ServerInfo info = getInfo();

        if (opts.isNoEcho() && info.getProtocolVersion() < 1) {
            throw new IOException("Server does not support no echo.");
        }
    }

    void upgradeToSecureIfNeeded(NatsUri nuri) throws IOException {
        Options opts = getOptions();
        ServerInfo info = getInfo();

        boolean isTLSRequired = opts.isTLSRequired();
        if (isTLSRequired && nuri.isWebsocket()) {
            // We are already communicating over "https" websocket, so
            // do NOT try to upgrade to secure.
            isTLSRequired = false;
        }
        if (isTLSRequired && !info.isTLSRequired()) {
            throw new IOException("SSL connection wanted by client.");
        } else if (!isTLSRequired && info.isTLSRequired()) {
            throw new IOException("SSL required by server.");
        }

        if (isTLSRequired) {
            this.dataPort.upgradeToSecure();
        }
    }

    // Called from reader/writer thread
    void handleCommunicationIssue(Exception io) {
        // If we are connecting or disconnecting, note exception and leave
        statusLock.lock();
        try {
            if (this.connecting || this.disconnecting || this.status == Status.CLOSED || this.isDraining()) {
                this.exceptionDuringConnectChange = io;
                return;
            }
        } finally {
            statusLock.unlock();
        }

        processException(io);

        // Spawn a thread so we don't have timing issues with
        // waiting on read/write threads
        executor.submit(() -> {
            try {
                this.closeSocket(true);
            } catch (InterruptedException e) {
                processException(e);
            }
        });
    }

    // Close socket is called when another connect attempt is possible
    // Close is called when the connection should shutdown, period
    void closeSocket(boolean tryReconnectIfConnected) throws InterruptedException {
        boolean wasConnected;

        statusLock.lock();
        try {
            if (isDisconnectingOrClosed()) {
                waitForDisconnectOrClose(this.options.getConnectionTimeout());
                return;
            }
            this.disconnecting = true;
            this.exceptionDuringConnectChange = null;
            wasConnected = (this.status == Status.CONNECTED);
            statusChanged.signalAll();
        } finally {
            statusLock.unlock();
        }

        closeSocketImpl();

        statusLock.lock();
        try {
            updateStatus(Status.DISCONNECTED);
            this.exceptionDuringConnectChange = null; // Ignore IOExceptions during closeSocketImpl()
            this.disconnecting = false;
            statusChanged.signalAll();
        } finally {
            statusLock.unlock();
        }

        if (isClosing()) { // Bit of a misname, but closing means we are in the close method or were asked
                           // to be
            close();
        } else if (wasConnected && tryReconnectIfConnected) {
            reconnect();
        }
    }

    // Close socket is called when another connect attempt is possible
    // Close is called when the connection should shutdown, period
    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws InterruptedException {
        this.close(true);
    }

    void close(boolean checkDrainStatus) throws InterruptedException {
        statusLock.lock();
        try {
            if (checkDrainStatus && this.isDraining()) {
                waitForDisconnectOrClose(this.options.getConnectionTimeout());
                return;
            }

            this.closing = true;// We were asked to close, so do it
            if (isDisconnectingOrClosed()) {
                waitForDisconnectOrClose(this.options.getConnectionTimeout());
                return;
            } else {
                this.disconnecting = true;
                this.exceptionDuringConnectChange = null;
                statusChanged.signalAll();
            }
        } finally {
            statusLock.unlock();
        }

        // Stop the reconnect wait timer after we stop the writer/reader (only if we are
        // really closing, not on errors)
        if (this.reconnectWaiter != null) {
            this.reconnectWaiter.cancel(true);
        }

        closeSocketImpl();

        this.dispatchers.forEach((nuid, d) -> d.stop(false));

        this.subscribers.forEach((sid, sub) -> sub.invalidate());

        this.dispatchers.clear();
        this.subscribers.clear();

        if (timer != null) {
            timer.cancel();
            timer = null;
        }

        cleanResponses(true);

        cleanUpPongQueue();

        statusLock.lock();
        try {
            updateStatus(Status.CLOSED); // will signal, we also signal when we stop disconnecting

            /*
             * if (exceptionDuringConnectChange != null) {
             * processException(exceptionDuringConnectChange); exceptionDuringConnectChange
             * = null; }
             */
        } finally {
            statusLock.unlock();
        }

        // Stop the error handling and connect executors
        callbackRunner.shutdown();
        try {
            callbackRunner.awaitTermination(this.options.getConnectionTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } finally {
            callbackRunner.shutdownNow();
        }

        // There's no need to wait for running tasks since we're told to close
        connectExecutor.shutdownNow();

        statusLock.lock();
        try {
            this.disconnecting = false;
            statusChanged.signalAll();
        } finally {
            statusLock.unlock();
        }
    }

    // Should only be called from closeSocket or close
    void closeSocketImpl() {
        this.currentServer = null;

        // Signal both to stop.
        final Future<Boolean> readStop = this.reader.stop();
        final Future<Boolean> writeStop = this.writer.stop();

        // Now wait until they both stop before closing the socket.
        try {
            readStop.get(1, TimeUnit.SECONDS);
        } catch (Exception ex) {
            //
        }
        try {
            writeStop.get(1, TimeUnit.SECONDS);
        } catch (Exception ex) {
            //
        }

        this.dataPortFuture.cancel(true);

        // Close the current socket and cancel anyone waiting for it
        try {
            if (this.dataPort != null) {
                this.dataPort.close();
            }

        } catch (IOException ex) {
            processException(ex);
        }
        cleanUpPongQueue();

        try {
            this.reader.stop().get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            processException(ex);
        }
        try {
            this.writer.stop().get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            processException(ex);
        }

    }

    void cleanUpPongQueue() {
        Future<Boolean> b;
        while ((b = pongQueue.poll()) != null) {
            try {
                b.cancel(true);
            } catch (CancellationException e) {
                if (!b.isDone() && !b.isCancelled()) {
                    processException(e);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(String subject, byte[] body) {
        publishInternal(subject, null, null, body);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(String subject, Headers headers, byte[] body) {
        publishInternal(subject, null, headers, body);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(String subject, String replyTo, byte[] body) {
        publishInternal(subject, replyTo, null, body);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(String subject, String replyTo, Headers headers, byte[] body) {
        publishInternal(subject, replyTo, headers, body);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(Message message) {
        validateNotNull(message, "Message");
        publishInternal(message.getSubject(), message.getReplyTo(), message.getHeaders(), message.getData());
    }

    void publishInternal(String subject, String replyTo, Headers headers, byte[] data) {
        checkIfNeedsHeaderSupport(headers);
        checkPayloadSize(data);

        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        } else if (blockPublishForDrain.get()) {
            throw new IllegalStateException("Connection is Draining"); // Ok to publish while waiting on subs
        }

        NatsMessage nm = new NatsMessage(subject, replyTo, new Headers(headers), data);

        Connection.Status stat = this.status;
        if ((stat == Status.RECONNECTING || stat == Status.DISCONNECTED)
                && !this.writer.canQueueDuringReconnect(nm)) {
            throw new IllegalStateException(
                    "Unable to queue any more messages during reconnect, max buffer is " + options.getReconnectBufferSize());
        }
        queueOutgoing(nm);
    }

    private void checkIfNeedsHeaderSupport(Headers headers) {
        if (headers != null && !headers.isEmpty() && !serverInfo.get().isHeadersSupported()) {
            throw new IllegalArgumentException(
                    "Headers are not supported by the server, version: " + serverInfo.get().getVersion());
        }
    }

    private void checkPayloadSize(byte[] body) {
        if (options.clientSideLimitChecks() && body != null && body.length > this.getMaxPayload() && this.getMaxPayload() > 0) {
            throw new IllegalArgumentException(
                    "Message payload size exceed server configuration " + body.length + " vs " + this.getMaxPayload());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Subscription subscribe(String subject) {

        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        Pattern pattern = Pattern.compile("\\s");
        Matcher matcher = pattern.matcher(subject);

        if (matcher.find()) {
            throw new IllegalArgumentException("Subject cannot contain whitespace");
        }

        return createSubscription(subject, null, null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Subscription subscribe(String subject, String queueName) {

        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        Pattern pattern = Pattern.compile("\\s");
        Matcher smatcher = pattern.matcher(subject);

        if (smatcher.find()) {
            throw new IllegalArgumentException("Subject cannot contain whitespace");
        }

        if (queueName == null || queueName.length() == 0) {
            throw new IllegalArgumentException("QueueName is required in subscribe");
        }

        Matcher qmatcher = pattern.matcher(queueName);

        if (qmatcher.find()) {
            throw new IllegalArgumentException("Queue names cannot contain whitespace");
        }

        return createSubscription(subject, queueName, null, null);
    }

    void invalidate(NatsSubscription sub) {
        remove(sub);
        sub.invalidate();
    }

    void remove(NatsSubscription sub) {
        CharSequence sid = sub.getSID();
        subscribers.remove(sid);

        if (sub.getNatsDispatcher() != null) {
            sub.getNatsDispatcher().remove(sub);
        }
    }

    void unsubscribe(NatsSubscription sub, int after) {
        if (isClosed()) { // last chance, usually sub will catch this
            throw new IllegalStateException("Connection is Closed");
        }

        if (after <= 0) {
            this.invalidate(sub); // Will clean it up
        } else {
            sub.setUnsubLimit(after);

            if (sub.reachedUnsubLimit()) {
                sub.invalidate();
            }
        }

        if (!isConnected()) {
            return; // We will set up sub on reconnect or ignore
        }

        sendUnsub(sub, after);
    }

    void sendUnsub(NatsSubscription sub, int after) {
        ByteArrayBuilder bab =
            new ByteArrayBuilder().append(UNSUB_SP_BYTES).append(sub.getSID());
        if (after > 0) {
            bab.append(SP).append(after);
        }
        queueInternalOutgoing(new ProtocolMessage(bab));
    }

    // Assumes the null/empty checks were handled elsewhere
    NatsSubscription createSubscription(String subject, String queueName, NatsDispatcher dispatcher, NatsSubscriptionFactory factory) {
        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        } else if (isDraining() && (dispatcher == null || dispatcher != this.inboxDispatcher.get())) {
            throw new IllegalStateException("Connection is Draining");
        }

        NatsSubscription sub;
        String sid = getNextSid();

        if (factory == null) {
            sub = new NatsSubscription(sid, subject, queueName, this, dispatcher);
        }
        else {
            sub = factory.createNatsSubscription(sid, subject, queueName, this, dispatcher);
        }
        subscribers.put(sid, sub);

        sendSubscriptionMessage(sid, subject, queueName, false);
        return sub;
    }

    String getNextSid() {
        return Long.toString(nextSid.getAndIncrement());
    }

    String reSubscribe(NatsSubscription sub, String subject, String queueName) {
        String sid = getNextSid();
        sendSubscriptionMessage(sid, subject, queueName, false);
        subscribers.put(sid, sub);
        return sid;
    }

    void sendSubscriptionMessage(String sid, String subject, String queueName, boolean treatAsInternal) {
        if (!isConnected()) {
            return; // We will set up sub on reconnect or ignore
        }

        ByteArrayBuilder bab = new ByteArrayBuilder(UTF_8).append(SUB_SP_BYTES).append(subject);
        if (queueName != null) {
            bab.append(SP).append(queueName);
        }
        bab.append(SP).append(sid);

        NatsMessage subMsg = new ProtocolMessage(bab);

        if (treatAsInternal) {
            queueInternalOutgoing(subMsg);
        } else {
            queueOutgoing(subMsg);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createInbox() {
        return options.getInboxPrefix() + nuid.next();
    }

    int getRespInboxLength() {
        return options.getInboxPrefix().length() + 22 + 1; // 22 for nuid, 1 for .
    }

    String createResponseInbox(String inbox) {
        // Substring gets rid of the * [trailing]
        return inbox.substring(0, getRespInboxLength()) + nuid.next();
    }

    // If the inbox is long enough, pull out the end part, otherwise, just use the
    // full thing
    String getResponseToken(String responseInbox) {
        int len = getRespInboxLength();
        if (responseInbox.length() <= len) {
            return responseInbox;
        }
        return responseInbox.substring(len);
    }

    void cleanResponses(boolean closing) {
        ArrayList<String> toRemove = new ArrayList<>();

        responsesAwaiting.forEach((key, future) -> {
            boolean remove = false;
            if (future.hasExceededTimeout()) {
                remove = true;
                future.cancelTimedOut();
            }
            else if (closing) {
                remove = true;
                future.cancelClosing();
            }
            else if (future.isDone()) {
                // done should have already been removed, not sure if
                // this even needs checking, but it won't hurt
                remove = true;
            }

            if (remove) {
                toRemove.add(key);
                statistics.decrementOutstandingRequests();
            }
        });

        for (String token : toRemove) {
            responsesAwaiting.remove(token);
        }

        if (advancedTracking) {
            toRemove.clear(); // just reuse this
            responsesRespondedTo.forEach((key, future) -> {
                if (future.hasExceededTimeout()) {
                    toRemove.add(key);
                }
            });

            for (String token : toRemove) {
                responsesRespondedTo.remove(token);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message request(String subject, byte[] body, Duration timeout) throws InterruptedException {
        return requestInternal(subject, null, body, timeout, cancelAction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message request(String subject, Headers headers, byte[] body, Duration timeout) throws InterruptedException {
        return requestInternal(subject, headers, body, timeout, cancelAction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message request(Message message, Duration timeout) throws InterruptedException {
        validateNotNull(message, "Message");
        return requestInternal(message.getSubject(), message.getHeaders(), message.getData(), timeout, cancelAction);
    }

    Message requestInternal(String subject, Headers headers, byte[] data, Duration timeout, CancelAction cancelAction) throws InterruptedException {
        CompletableFuture<Message> incoming = requestFutureInternal(subject, headers, data, timeout, cancelAction);
        try {
            return incoming.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException | ExecutionException | CancellationException e) {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Message> request(String subject, byte[] body) {
        return requestFutureInternal(subject, null, body, null, cancelAction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Message> request(String subject, Headers headers, byte[] body) {
        return requestFutureInternal(subject, headers, body, null, cancelAction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Message> requestWithTimeout(String subject, byte[] body, Duration timeout) {
        return requestFutureInternal(subject, null, body, timeout, cancelAction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Message> requestWithTimeout(String subject, Headers headers, byte[] body, Duration timeout) {
        return requestFutureInternal(subject, headers, body, timeout, cancelAction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Message> requestWithTimeout(Message message, Duration timeout) {
        validateNotNull(message, "Message");
        return requestFutureInternal(message.getSubject(), message.getHeaders(), message.getData(), timeout, cancelAction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Message> request(Message message) {
        validateNotNull(message, "Message");
        return requestFutureInternal(message.getSubject(), message.getHeaders(), message.getData(), null, cancelAction);
    }

    CompletableFuture<Message> requestFutureInternal(String subject, Headers headers, byte[] data, Duration futureTimeout, CancelAction cancelAction) {
        checkPayloadSize(data);

        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        } else if (isDraining()) {
            throw new IllegalStateException("Connection is Draining");
        }

        if (inboxDispatcher.get() == null) {
            NatsDispatcher d = new NatsDispatcher(this, this::deliverReply);

            if (inboxDispatcher.compareAndSet(null, d)) {
                String id = this.nuid.next();
                this.dispatchers.put(id, d);
                d.start(id);
                d.subscribe(this.mainInbox);
            }
        }

        boolean oldStyle = options.isOldRequestStyle();
        String responseInbox = oldStyle ? createInbox() : createResponseInbox(this.mainInbox);
        String responseToken = getResponseToken(responseInbox);
        NatsRequestCompletableFuture future =
            new NatsRequestCompletableFuture(cancelAction,
                futureTimeout == null ? options.getRequestCleanupInterval() : futureTimeout);

        if (!oldStyle) {
            responsesAwaiting.put(responseToken, future);
        }
        statistics.incrementOutstandingRequests();

        if (oldStyle) {
            NatsDispatcher dispatcher = this.inboxDispatcher.get();
            NatsSubscription sub = dispatcher.subscribeReturningSubscription(responseInbox);
            dispatcher.unsubscribe(responseInbox, 1);
            // Unsubscribe when future is cancelled:
            future.whenComplete((msg, exception) -> {
                if (exception instanceof CancellationException) {
                    dispatcher.unsubscribe(responseInbox);
                }
            });
            responsesAwaiting.put(sub.getSID(), future);
        }

        publishInternal(subject, responseInbox, headers, data);
        writer.flushBuffer();
        statistics.incrementRequestsSent();

        return future;
    }

    void deliverReply(Message msg) {
        boolean oldStyle = options.isOldRequestStyle();
        String subject = msg.getSubject();
        String token = getResponseToken(subject);
        String key = oldStyle ? msg.getSID() : token;
        NatsRequestCompletableFuture f = responsesAwaiting.remove(key);
        if (f != null) {
            if (advancedTracking) {
                responsesRespondedTo.put(key, f);
            }
            statistics.decrementOutstandingRequests();
            if (msg.isStatusMessage() && msg.getStatus().getCode() == 503) {
                switch (f.getCancelAction()) {
                    case COMPLETE:
                        f.complete(msg);
                        break;
                    case REPORT:
                        f.completeExceptionally(new JetStreamStatusException(msg.getStatus()));
                        break;
                    case CANCEL:
                    default:
                        f.cancel(true);
                }
            }
            else {
                f.complete(msg);
            }
            statistics.incrementRepliesReceived();
        }
        else if (!oldStyle && !subject.startsWith(mainInbox)) {
            if (advancedTracking && responsesRespondedTo.get(key) != null) {
                statistics.incrementDuplicateRepliesReceived();
            }
            else {
                statistics.incrementOrphanRepliesReceived();
            }
        }
    }

    public Dispatcher createDispatcher() {
        return createDispatcher(null);
    }

    public Dispatcher createDispatcher(MessageHandler handler) {
        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        } else if (isDraining()) {
            throw new IllegalStateException("Connection is Draining");
        }

        NatsDispatcher dispatcher = new NatsDispatcher(this, handler);
        String id = this.nuid.next();
        this.dispatchers.put(id, dispatcher);
        dispatcher.start(id);
        return dispatcher;
    }

    public void closeDispatcher(Dispatcher d) {
        if (isClosed()) {
            throw new IllegalStateException("Connection is Closed");
        } else if (!(d instanceof NatsDispatcher)) {
            throw new IllegalArgumentException("Connection can only manage its own dispatchers");
        }

        NatsDispatcher nd = ((NatsDispatcher) d);

        if (nd.isDraining()) {
            return; // No op while draining
        }

        if (!this.dispatchers.containsKey(nd.getId())) {
            throw new IllegalArgumentException("Dispatcher is already closed.");
        }

        cleanupDispatcher(nd);
    }

    void cleanupDispatcher(NatsDispatcher nd) {
        nd.stop(true);
        this.dispatchers.remove(nd.getId());
    }

    Map<String, Dispatcher> getDispatchers() {
        return Collections.unmodifiableMap(dispatchers);
    }

    public void addConnectionListener(ConnectionListener connectionListener) {
        connectionListeners.add(connectionListener);
    }

    public void removeConnectionListener(ConnectionListener connectionListener) {
        connectionListeners.remove(connectionListener);
    }

    public void flush(Duration timeout) throws TimeoutException, InterruptedException {

        Instant start = Instant.now();
        waitForConnectOrClose(timeout);

        if (isClosed()) {
            throw new TimeoutException("Attempted to flush while closed");
        }

        if (timeout == null) {
            timeout = Duration.ZERO;
        }

        Instant now = Instant.now();
        Duration waitTime = Duration.between(start, now);

        if (!timeout.equals(Duration.ZERO) && waitTime.compareTo(timeout) >= 0) {
            throw new TimeoutException("Timeout out waiting for connection before flush.");
        }

        try {
            Future<Boolean> waitForIt = sendPing();

            if (waitForIt == null) { // error in the send ping code
                return;
            }

            long nanos = timeout.toNanos();

            if (nanos > 0) {

                nanos -= waitTime.toNanos();

                if (nanos <= 0) {
                    nanos = 1; // let the future timeout if it isn't resolved
                }

                waitForIt.get(nanos, TimeUnit.NANOSECONDS);
            } else {
                waitForIt.get();
            }

            this.statistics.incrementFlushCounter();
        } catch (ExecutionException | CancellationException e) {
            throw new TimeoutException(e.toString());
        }
    }

    void sendConnect(NatsUri nuri) throws IOException {
        try {
            ServerInfo info = this.serverInfo.get();
            // This is changed - we used to use info.isAuthRequired(), but are changing it to
            // better match older versions of the server. It may change again in the future.
            CharBuffer connectOptions = options.buildProtocolConnectOptionsString(
                nuri.toString(), true, info.getNonce());
            ByteArrayBuilder bab =
                new ByteArrayBuilder(OP_CONNECT_SP_LEN + connectOptions.limit(), UTF_8)
                    .append(CONNECT_SP_BYTES).append(connectOptions);
            queueInternalOutgoing(new ProtocolMessage(bab));
        } catch (Exception exp) {
            throw new IOException("Error sending connect string", exp);
        }
    }

    CompletableFuture<Boolean> sendPing() {
        return this.sendPing(true);
    }

    CompletableFuture<Boolean> softPing() {
        return this.sendPing(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Duration RTT() throws IOException {
        if (!isConnectedOrConnecting()) {
            throw new IOException("Must be connected to do RTT.");
        }

        long timeout = options.getConnectionTimeout().toMillis();
        CompletableFuture<Boolean> pongFuture = new CompletableFuture<>();
        pongQueue.add(pongFuture);
        try {
            long time = System.nanoTime();
            writer.queueInternalMessage(new ProtocolMessage(OP_PING_BYTES));
            pongFuture.get(timeout, TimeUnit.MILLISECONDS);
            return Duration.ofNanos(System.nanoTime() - time);
        }
        catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }
        catch (InterruptedException | TimeoutException e) {
            throw new IOException(e);
        }
    }

    // Send a ping request and push a pong future on the queue.
    // futures are completed in order, keep this one if a thread wants to wait
    // for a specific pong. Note, if no pong returns the wait will not return
    // without setting a timeout.
    CompletableFuture<Boolean> sendPing(boolean treatAsInternal) {
        int max = this.options.getMaxPingsOut();

        if (!isConnectedOrConnecting()) {
            CompletableFuture<Boolean> retVal = new CompletableFuture<>();
            retVal.complete(Boolean.FALSE);
            return retVal;
        }

        if (!treatAsInternal && !this.needPing.get()) {
            CompletableFuture<Boolean> retVal = new CompletableFuture<>();
            retVal.complete(Boolean.TRUE);
            this.needPing.set(true);
            return retVal;
        }

        if (max > 0 && pongQueue.size() + 1 > max) {
            handleCommunicationIssue(new IllegalStateException("Max outgoing Ping count exceeded."));
            return null;
        }

        CompletableFuture<Boolean> pongFuture = new CompletableFuture<>();
        pongQueue.add(pongFuture);

        if (treatAsInternal) {
            queueInternalOutgoing(new ProtocolMessage(PING_PROTO));
        } else {
            queueOutgoing(new ProtocolMessage(PING_PROTO));
        }

        this.needPing.set(true);
        this.statistics.incrementPingCount();
        return pongFuture;
    }

    // This is a minor speed / memory enhancement.
    // We can't reuse the same instance of any NatsMessage b/c of the "NatsMessage next" state
    // But it is safe to share the data bytes and the size since those fields are just being read
    // This constructor "ProtocolMessage(ProtocolMessage pm)" shares the data and size
    // reducing allocation of data for something that is often created and used
    // These static instances are the once that are used for copying, sendPing and sendPong
    private static final ProtocolMessage PING_PROTO = new ProtocolMessage(OP_PING_BYTES);
    private static final ProtocolMessage PONG_PROTO = new ProtocolMessage(OP_PONG_BYTES);

    void sendPong() {
        queueInternalOutgoing(new ProtocolMessage(PONG_PROTO));
    }

    // Called by the reader
    void handlePong() {
        CompletableFuture<Boolean> pongFuture = pongQueue.pollFirst();
        if (pongFuture != null) {
            pongFuture.complete(Boolean.TRUE);
        }
    }

    void readInitialInfo() throws IOException {
        byte[] readBuffer = new byte[options.getBufferSize()];
        ByteBuffer protocolBuffer = ByteBuffer.allocate(options.getBufferSize());
        boolean gotCRLF = false;
        boolean gotCR = false;

        while (!gotCRLF) {
            int read = this.dataPort.read(readBuffer, 0, readBuffer.length);

            if (read < 0) {
                break;
            }

            int i = 0;
            while (i < read) {
                byte b = readBuffer[i++];

                if (gotCR) {
                    if (b != LF) {
                        throw new IOException("Missed LF after CR waiting for INFO.");
                    } else if (i < read) {
                        throw new IOException("Read past initial info message.");
                    }

                    gotCRLF = true;
                    break;
                }

                if (b == CR) {
                    gotCR = true;
                } else {
                    if (!protocolBuffer.hasRemaining()) {
                        protocolBuffer = enlargeBuffer(protocolBuffer, 0); // just double it
                    }
                    protocolBuffer.put(b);
                }
            }
        }

        if (!gotCRLF) {
            throw new IOException("Failed to read initial info message.");
        }

        protocolBuffer.flip();

        String infoJson = UTF_8.decode(protocolBuffer).toString();
        infoJson = infoJson.trim();
        String[] msg = infoJson.split("\\s");
        String op = msg[0].toUpperCase();

        if (!OP_INFO.equals(op)) {
            throw new IOException("Received non-info initial message.");
        }

        handleInfo(infoJson);
    }

    void handleInfo(String infoJson) {
        ServerInfo serverInfo = new ServerInfo(infoJson);
        this.serverInfo.set(serverInfo);

        List<String> urls = this.serverInfo.get().getConnectURLs();
        if (urls != null && urls.size() > 0) {
            if (serverPool.acceptDiscoveredUrls(urls)) {
                processConnectionEvent(Events.DISCOVERED_SERVERS);
            }
        }

        if (serverInfo.isLameDuckMode()) {
            processConnectionEvent(Events.LAME_DUCK);
        }
    }

    void queueOutgoing(NatsMessage msg) {
        if (msg.getControlLineLength() > this.options.getMaxControlLine()) {
            throw new IllegalArgumentException("Control line is too long");
        }
        if (!writer.queue(msg)) {
            options.getErrorListener().messageDiscarded(this, msg);
        }
    }

    void queueInternalOutgoing(NatsMessage msg) {
        if (msg.getControlLineLength() > this.options.getMaxControlLine()) {
            throw new IllegalArgumentException("Control line is too long");
        }
        this.writer.queueInternalMessage(msg);
    }

    void deliverMessage(NatsMessage msg) {
        this.needPing.set(false);
        this.statistics.incrementInMsgs();
        this.statistics.incrementInBytes(msg.getSizeInBytes());

        NatsSubscription sub = subscribers.get(msg.getSID());

        if (sub != null) {
            msg.setSubscription(sub);

            NatsDispatcher d = sub.getNatsDispatcher();
            NatsConsumer c = (d == null) ? sub : d;
            MessageQueue q = ((d == null) ? sub.getMessageQueue() : d.getMessageQueue());

            if (c.hasReachedPendingLimits()) {
                // Drop the message and count it
                this.statistics.incrementDroppedCount();
                c.incrementDroppedCount();

                // Notify the first time
                if (!c.isMarkedSlow()) {
                    c.markSlow();
                    processSlowConsumer(c);
                }
            } else if (q != null) {
                c.markNotSlow();

                // beforeQueueProcessor returns true if the message is allowed to be queued
                if (sub.getBeforeQueueProcessor().apply(msg)) {
                    q.push(msg);
                }
            }

        }
//        else {
//            // Drop messages we don't have a subscriber for (could be extras on an
//            // auto-unsub for example)
//        }
    }

    void processOK() {
        this.statistics.incrementOkCount();
    }

    void processSlowConsumer(Consumer consumer) {
        if (!this.callbackRunner.isShutdown()) {
            try {
                this.callbackRunner.execute(() -> {
                    try {
                        options.getErrorListener().slowConsumerDetected(this, consumer);
                    } catch (Exception ex) {
                        this.statistics.incrementExceptionCount();
                    }
                });
            } catch (RejectedExecutionException re) {
                // Timing with shutdown, let it go
            }
        }
    }

    void processException(Exception exp) {
        this.statistics.incrementExceptionCount();

        if (!this.callbackRunner.isShutdown()) {
            try {
                this.callbackRunner.execute(() -> {
                    try {
                        options.getErrorListener().exceptionOccurred(this, exp);
                    } catch (Exception ex) {
                        this.statistics.incrementExceptionCount();
                    }
                });
            } catch (RejectedExecutionException re) {
                // Timing with shutdown, let it go
            }
        }
    }

    void processError(String errorText) {
        this.statistics.incrementErrCount();

        this.lastError.set(errorText);
        this.connectError.set(errorText); // even if this isn't during connection, save it just in case

        // If we are connected && we get an authentication error, save it
        if (this.isConnected() && this.isAuthenticationError(errorText) && currentServer != null) {
            this.serverAuthErrors.put(currentServer, errorText);
        }

        if (!this.callbackRunner.isShutdown()) {
            try {
                this.callbackRunner.execute(() -> {
                    try {
                        options.getErrorListener().errorOccurred(this, errorText);
                    } catch (Exception ex) {
                        this.statistics.incrementExceptionCount();
                    }
                });
            } catch (RejectedExecutionException re) {
                // Timing with shutdown, let it go
            }
        }
    }

    interface ErrorListenerCaller {
        void call(Connection conn, ErrorListener el);
    }

    void executeCallback(ErrorListenerCaller elc) {
        if (!this.callbackRunner.isShutdown()) {
            try {
                this.callbackRunner.execute(() -> elc.call(this, options.getErrorListener()));
            } catch (RejectedExecutionException re) {
                // Timing with shutdown, let it go
            }
        }
    }

    void processConnectionEvent(Events type) {
        if (!this.callbackRunner.isShutdown()) {
            try {
                for (ConnectionListener listener : connectionListeners) {
                    this.callbackRunner.execute(() -> {
                        try {
                            listener.connectionEvent(this, type);
                        } catch (Exception ex) {
                            this.statistics.incrementExceptionCount();
                        }
                    });
                }
            } catch (RejectedExecutionException re) {
                // Timing with shutdown, let it go
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ServerInfo getServerInfo() {
        return getInfo();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetAddress getClientInetAddress() {
        try {
            return InetAddress.getByName(getInfo().getClientIp());
        }
        catch (Exception e) {
            return null;
        }
    }

    ServerInfo getInfo() {
        return this.serverInfo.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Options getOptions() {
        return this.options;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statistics getStatistics() {
        return this.statistics;
    }

    NatsStatistics getNatsStatistics() {
        return this.statistics;
    }

    DataPort getDataPort() {
        return this.dataPort;
    }

    // Used for testing
    int getConsumerCount() {
        return this.subscribers.size() + this.dispatchers.size();
    }

    public long getMaxPayload() {
        ServerInfo info = this.serverInfo.get();

        if (info == null) {
            return -1;
        }

        return info.getMaxPayload();
    }

    /**
     * Return the list of known server urls, including additional servers discovered
     * after a connection has been established.
     * @return this connection's list of known server URLs
     */
    public Collection<String> getServers() {
        return serverPool.getServerList();
    }

    protected List<NatsUri> resolveHost(NatsUri nuri) {
        // 1. If the host is not an ip address, let the pool resolve it.
        List<NatsUri> results = new ArrayList<>();
        if (!nuri.hostIsIpAddress()) {
            List<String> ips = serverPool.resolveHostToIps(nuri.getHost());
            if (ips != null) {
                for (String ip : ips) {
                    try {
                        results.add(nuri.reHost(ip));
                    } catch (URISyntaxException u) {
                        // ??? should never happen
                    }
                }
            }
        }

        // 2. If there were no results,
        //    - host was an ip address or
        //    - pool returned nothing or
        //    - resolving failed...
        //    so the list just becomes the original host.
        if (results.size() == 0) {
            results.add(nuri);
        }
        return results;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getConnectedUrl() {
        return currentServer == null ? null : currentServer.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Status getStatus() {
        return this.status;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getLastError() {
        return this.lastError.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearLastError() {
        this.lastError.set("");
    }

    ExecutorService getExecutor() {
        return executor;
    }

    void updateStatus(Status newStatus) {
        Status oldStatus = this.status;

        statusLock.lock();
        try {
            if (oldStatus == Status.CLOSED || newStatus == oldStatus) {
                return;
            }
            this.status = newStatus;
        } finally {
            statusChanged.signalAll();
            statusLock.unlock();
        }

        if (this.status == Status.DISCONNECTED) {
            processConnectionEvent(Events.DISCONNECTED);
        } else if (this.status == Status.CLOSED) {
            processConnectionEvent(Events.CLOSED);
        } else if (oldStatus == Status.RECONNECTING && this.status == Status.CONNECTED) {
            processConnectionEvent(Events.RECONNECTED);
        } else if (this.status == Status.CONNECTED) {
            processConnectionEvent(Events.CONNECTED);
        }
    }

    boolean isClosing() {
        return this.closing;
    }

    boolean isClosed() {
        return this.status == Status.CLOSED;
    }

    boolean isConnected() {
        return this.status == Status.CONNECTED;
    }

    boolean isConnectedOrConnecting() {
        statusLock.lock();
        try {
            return this.status == Status.CONNECTED || this.connecting;
        } finally {
            statusLock.unlock();
        }
    }

    boolean isDisconnectingOrClosed() {
        statusLock.lock();
        try {
            return this.status == Status.CLOSED || this.disconnecting;
        } finally {
            statusLock.unlock();
        }
    }

    boolean isDisconnecting() {
        statusLock.lock();
        try {
            return this.disconnecting;
        } finally {
            statusLock.unlock();
        }
    }

    void waitForDisconnectOrClose(Duration timeout) throws InterruptedException {
        waitFor(timeout, (Void) -> this.isDisconnecting() && !this.isClosed() );
    }

    void waitForConnectOrClose(Duration timeout) throws InterruptedException {
        waitFor(timeout, (Void) -> !this.isConnected() && !this.isClosed());
    }

    void waitFor(Duration timeout, Predicate<Void> test) throws InterruptedException {
        statusLock.lock();
        try {
            long currentWaitNanos = (timeout != null) ? timeout.toNanos() : -1;
            long start = System.nanoTime();
            while (currentWaitNanos >= 0 && test.test(null)) {
                if (currentWaitNanos > 0) {
                    statusChanged.await(currentWaitNanos, TimeUnit.NANOSECONDS);
                    long now = System.nanoTime();
                    currentWaitNanos = currentWaitNanos - (now - start);
                    start = now;

                    if (currentWaitNanos <= 0) {
                        break;
                    }
                } else {
                    statusChanged.await();
                }
            }
        } finally {
            statusLock.unlock();
        }
    }

    void invokeReconnectDelayHandler(long totalTries) {
        long currentWaitNanos = 0;

        ReconnectDelayHandler handler = options.getReconnectDelayHandler();
        if (handler == null) {
            Duration dur = options.getReconnectWait();
            if (dur != null) {
                currentWaitNanos = dur.toNanos();
                dur = serverPool.hasSecureServer() ? options.getReconnectJitterTls() : options.getReconnectJitter();
                if (dur != null) {
                    currentWaitNanos += ThreadLocalRandom.current().nextLong(dur.toNanos());
                }
            }
        }
        else {
            Duration waitTime = handler.getWaitTime(totalTries);
            if (waitTime != null) {
                currentWaitNanos = waitTime.toNanos();
            }
        }

        this.reconnectWaiter = new CompletableFuture<>();

        long start = System.nanoTime();
        while (currentWaitNanos > 0 && !isDisconnectingOrClosed() && !isConnected() && !this.reconnectWaiter.isDone()) {
            try {
                this.reconnectWaiter.get(currentWaitNanos, TimeUnit.NANOSECONDS);
            } catch (Exception exp) {
                // ignore, try to loop again
            }
            long now = System.nanoTime();
            currentWaitNanos = currentWaitNanos - (now - start);
            start = now;
        }

        this.reconnectWaiter.complete(Boolean.TRUE);
    }

    ByteBuffer enlargeBuffer(ByteBuffer buffer, int atLeast) {
        int current = buffer.capacity();
        int newSize = Math.max(current * 2, atLeast);
        ByteBuffer newBuffer = ByteBuffer.allocate(newSize);
        buffer.flip();
        newBuffer.put(buffer);
        return newBuffer;
    }

    // For testing
    NatsConnectionReader getReader() {
        return this.reader;
    }

    // For testing
    NatsConnectionWriter getWriter() {
        return this.writer;
    }

    // For testing
    Future<DataPort> getDataPortFuture() {
        return this.dataPortFuture;
    }

    boolean isDraining() {
        return this.draining.get() != null;
    }

    boolean isDrained() {
        CompletableFuture<Boolean> tracker = this.draining.get();

        try {
            if (tracker != null && tracker.getNow(false)) {
                return true;
            }
        } catch (Exception e) {
            // These indicate the tracker was cancelled/timed out
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Boolean> drain(Duration timeout) throws TimeoutException, InterruptedException {

        if (isClosing() || isClosed()) {
            throw new IllegalStateException("A connection can't be drained during close.");
        }

        this.statusLock.lock();
        try {
            if (isDraining()) {
                return this.draining.get();
            }
            this.draining.set(new CompletableFuture<>());
        } finally {
            this.statusLock.unlock();
        }

        final CompletableFuture<Boolean> tracker = this.draining.get();
        Instant start = Instant.now();

        // Don't include subscribers with dispatchers
        HashSet<NatsSubscription> pureSubscribers = new HashSet<>();
        pureSubscribers.addAll(this.subscribers.values());
        pureSubscribers.removeIf((s) -> s.getDispatcher() != null);

        final HashSet<NatsConsumer> consumers = new HashSet<>();
        consumers.addAll(pureSubscribers);
        consumers.addAll(this.dispatchers.values());

        NatsDispatcher inboxer = this.inboxDispatcher.get();

        if (inboxer != null) {
            consumers.add(inboxer);
        }

        // Stop the consumers NOW so that when this method returns they are blocked
        consumers.forEach((cons) -> {
            cons.markDraining(tracker);
            cons.sendUnsubForDrain();
        });

        try {
            this.flush(timeout); // Flush and wait up to the timeout, if this fails, let the caller know
        } catch (Exception e) {
            this.close(false);
            throw e;
        }

        consumers.forEach(NatsConsumer::markUnsubedForDrain);

        // Wait for the timeout or the pending count to go to 0
        executor.submit(() -> {
            try {
                Instant now = Instant.now();

                while (timeout == null || timeout.equals(Duration.ZERO)
                        || Duration.between(start, now).compareTo(timeout) < 0) {
                    consumers.removeIf(NatsConsumer::isDrained);

                    if (consumers.size() == 0) {
                        break;
                    }

                    Thread.sleep(1); // Sleep 1 milli

                    now = Instant.now();
                }

                // Stop publishing
                this.blockPublishForDrain.set(true);

                // One last flush
                if (timeout == null || timeout.equals(Duration.ZERO)) {
                    this.flush(Duration.ZERO);
                } else {
                    now = Instant.now();

                    Duration passed = Duration.between(start, now);
                    Duration newTimeout = timeout.minus(passed);

                    if (newTimeout.toNanos() > 0) {
                        this.flush(newTimeout);
                    }
                }

                this.close(false); // close the connection after the last flush
                tracker.complete(consumers.size() == 0);
            } catch (TimeoutException | InterruptedException e) {
                this.processException(e);
            } finally {
                try {
                    this.close(false);// close the connection after the last flush
                } catch (InterruptedException e) {
                    this.processException(e);
                }
                tracker.complete(false);
            }
        });

        return tracker;
    }

    boolean isAuthenticationError(String err) {
        if (err == null) {
            return false;
        }
        err = err.toLowerCase();
        return err.startsWith("user authentication") || err.contains("authorization violation");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flushBuffer() throws IOException {
        if (!isConnected()) {
            throw new IllegalStateException("Connection is not active.");
        }
        writer.flushBuffer();
    }

    void lenientFlushBuffer()  {
        try {
            writer.flushBuffer();
        }
        catch (Exception e) {
            // ignore
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamContext streamContext(String streamName) throws IOException, JetStreamApiException {
        Validator.validateStreamName(streamName, true);
        ensureNotClosing();
        return new NatsStreamContext(this, null, streamName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamContext streamContext(String streamName, JetStreamOptions options) throws IOException, JetStreamApiException {
        Validator.validateStreamName(streamName, true);
        ensureNotClosing();
        return new NatsStreamContext(this, options, streamName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerContext consumerContext(String streamName, String consumerName) throws IOException, JetStreamApiException {
        return streamContext(streamName).consumerContext(consumerName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerContext consumerContext(String streamName, String consumerName, JetStreamOptions options) throws IOException, JetStreamApiException {
        return streamContext(streamName, options).consumerContext(consumerName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStream jetStream() throws IOException {
        ensureNotClosing();
        return new NatsJetStream(this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStream jetStream(JetStreamOptions options) throws IOException {
        ensureNotClosing();
        return new NatsJetStream(this, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamManagement jetStreamManagement() throws IOException {
        ensureNotClosing();
        return new NatsJetStreamManagement(this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamManagement jetStreamManagement(JetStreamOptions options) throws IOException {
        ensureNotClosing();
        return new NatsJetStreamManagement(this, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValue keyValue(String bucketName) throws IOException {
        Validator.validateBucketName(bucketName, true);
        ensureNotClosing();
        return new NatsKeyValue(this, bucketName, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValue keyValue(String bucketName, KeyValueOptions options) throws IOException {
        Validator.validateBucketName(bucketName, true);
        ensureNotClosing();
        return new NatsKeyValue(this, bucketName, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueManagement keyValueManagement() throws IOException {
        ensureNotClosing();
        return new NatsKeyValueManagement(this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueManagement keyValueManagement(KeyValueOptions options) throws IOException {
        ensureNotClosing();
        return new NatsKeyValueManagement(this, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectStore objectStore(String bucketName) throws IOException {
        Validator.validateBucketName(bucketName, true);
        ensureNotClosing();
        return new NatsObjectStore(this, bucketName, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectStore objectStore(String bucketName, ObjectStoreOptions options) throws IOException {
        Validator.validateBucketName(bucketName, true);
        ensureNotClosing();
        return new NatsObjectStore(this, bucketName, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectStoreManagement objectStoreManagement() throws IOException {
        ensureNotClosing();
        return new NatsObjectStoreManagement(this, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectStoreManagement objectStoreManagement(ObjectStoreOptions options) throws IOException {
        ensureNotClosing();
        return new NatsObjectStoreManagement(this, options);
    }

    private void ensureNotClosing() throws IOException {
        if (isClosing() || isClosed()) {
            throw new IOException("A JetStream context can't be established during close.");
        }
    }
}
