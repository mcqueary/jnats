// Copyright 2021-2022 The NATS Authors
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

import io.nats.client.JetStreamReader;
import io.nats.client.Message;
import io.nats.client.PullRequestOptions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class NatsJetStreamPullSubscription extends NatsJetStreamSubscription {

    private final AtomicLong pullIdIncrementer;

    NatsJetStreamPullSubscription(String sid, String subject,
                                  NatsConnection connection, NatsDispatcher dispatcher,
                                  NatsJetStream js,
                                  String stream, String consumer,
                                  MessageManager manager) {
        super(sid, subject, null, connection, dispatcher, js, stream, consumer, manager);
        pullIdIncrementer = new AtomicLong();
    }

    @Override
    boolean isPullMode() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pull(int batchSize) {
        _pull(PullRequestOptions.builder(batchSize).build(), true, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pull(PullRequestOptions pullRequestOptions) {
        _pull(pullRequestOptions, true, null);
    }

    protected String _pull(PullRequestOptions pullRequestOptions, boolean raiseStatusWarnings, TrackPendingListener trackPendingListener) {
        String publishSubject = js.prependPrefix(String.format(JSAPI_CONSUMER_MSG_NEXT, stream, consumerName));
        String pullId = getSubject().replace("*", Long.toString(this.pullIdIncrementer.incrementAndGet()));
        manager.startPullRequest(pullId, pullRequestOptions, raiseStatusWarnings, trackPendingListener);
        connection.publish(publishSubject, pullId, pullRequestOptions.serialize());
        connection.lenientFlushBuffer();
        return pullId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize) {
        _pull(PullRequestOptions.noWait(batchSize).build(), true, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize, Duration expiresIn) {
        durationGtZeroRequired(expiresIn, "NoWait Expires In");
        _pull(PullRequestOptions.noWait(batchSize).expiresIn(expiresIn).build(), true, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize, long expiresInMillis) {
        durationGtZeroRequired(expiresInMillis, "NoWait Expires In");
        _pull(PullRequestOptions.noWait(batchSize).expiresIn(expiresInMillis).build(), true, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, Duration expiresIn) {
        durationGtZeroRequired(expiresIn, "Expires In");
        _pull(PullRequestOptions.builder(batchSize).expiresIn(expiresIn).build(), true, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, long expiresInMillis) {
        durationGtZeroRequired(expiresInMillis, "Expires In");
        _pull(PullRequestOptions.builder(batchSize).expiresIn(expiresInMillis).build(), true, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, long maxWaitMillis) {
        durationGtZeroRequired(maxWaitMillis, "Fetch");
        return _fetch(batchSize, maxWaitMillis);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, Duration maxWait) {
        durationGtZeroRequired(maxWait, "Fetch");
        return _fetch(batchSize, maxWait.toMillis());
    }

    private List<Message> _fetch(int batchSize, long maxWaitMillis) {
        List<Message> messages = drainAlreadyBuffered(batchSize);

        int batchLeft = batchSize - messages.size();
        if (batchLeft == 0) {
            return messages;
        }

        try {
            long start = System.nanoTime();

            Duration expires = Duration.ofMillis(
                maxWaitMillis > MIN_EXPIRE_MILLIS ? maxWaitMillis - EXPIRE_ADJUSTMENT : maxWaitMillis);
            String pullId = _pull(PullRequestOptions.builder(batchLeft).expiresIn(expires).build(), false, null);

            // timeout > 0 process as many messages we can in that time period
            // If we get a message that either manager handles, we try again, but
            // with a shorter timeout based on what we already used up
            long maxWaitNanos = maxWaitMillis * 1_000_000;
            long timeLeftNanos = maxWaitNanos;
            while (batchLeft > 0 && timeLeftNanos > 0) {
                Message msg = nextMessageInternal( Duration.ofNanos(timeLeftNanos) );
                if (msg == null) {
                    return messages; // normal timeout
                }
                switch (manager.manage(msg)) {
                    case MESSAGE:
                        messages.add(msg);
                        batchLeft--;
                        break;
                    case TERMINUS:
                    case ERROR:
                        // if there is a match, the status applies
                        if (pullId.equals(msg.getSubject())) {
                            return messages;
                        }
                }
                // case pull not match / other ManageResult (i.e. STATUS), try again while we have time
                timeLeftNanos = maxWaitNanos - (System.nanoTime() - start);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return messages;
    }

    private List<Message> drainAlreadyBuffered(int batchSize) {
        List<Message> messages = new ArrayList<>(batchSize);
        try {
            while (true) {
                Message msg = nextMessageInternal(null);
                if (msg == null) {
                    return messages; // no more message currently queued
                }
                if (manager.manage(msg) == MessageManager.ManageResult.MESSAGE) {
                    messages.add(msg);
                    if (messages.size() == batchSize) {
                        return messages;
                    }
                }
                // since this is buffered, no non-message applies, try again
            }
        }
        catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        }
        return messages;
    }

    private void durationGtZeroRequired(Duration duration, String label) {
        if (duration == null || duration.toMillis() <= 0) {
            throw new IllegalArgumentException(label + " wait duration must be supplied and greater than 0.");
        }
    }

    private void durationGtZeroRequired(long millis, String label) {
        if (millis <= 0) {
            throw new IllegalArgumentException(label + " wait duration must be supplied and greater than 0.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(int batchSize, Duration maxWait) {
        durationGtZeroRequired(maxWait, "Iterate");
        return _iterate(batchSize, maxWait.toMillis());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(final int batchSize, long maxWaitMillis) {
        durationGtZeroRequired(maxWaitMillis, "Iterate");
        return _iterate(batchSize, maxWaitMillis);
    }

    private Iterator<Message> _iterate(final int batchSize, long maxWaitMillis) {
        final List<Message> buffered = drainAlreadyBuffered(batchSize);

        // if there was a full batch buffered, no need to pull, just iterate over the list you already have
        int batchLeft = batchSize - buffered.size();
        if (batchLeft == 0) {
            return new Iterator<Message>() {
                @Override
                public boolean hasNext() {
                    return buffered.size() > 0;
                }

                @Override
                public Message next() {
                    return buffered.remove(0);
                }
            };
        }

        // if there were some messages buffered, reduce the raw pull batch size
        String pullId = _pull(PullRequestOptions.builder(batchLeft).expiresIn(maxWaitMillis).build(), false, null);

        final long timeout = maxWaitMillis;

        // the iterator is also more complicated
        return new Iterator<Message>() {
            int received = 0;
            boolean done = false;
            Message msg = null;

            @Override
            public boolean hasNext() {
                try {
                    if (msg != null) {
                        return true;
                    }

                    if (done) {
                        return false;
                    }

                    if (buffered.size() == 0) {
                        msg = _nextUnmanaged(timeout, pullId);
                        if (msg == null) {
                            done = true;
                            return false;
                        }
                    }
                    else {
                        msg = buffered.remove(0);
                    }

                    done = ++received == batchSize;
                    return true;
                }
                catch (InterruptedException e) {
                    msg = null;
                    done = true;
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            @Override
            public Message next() {
                Message next = msg;
                msg = null;
                return next;
            }
        };
    }

    static class JetStreamReaderImpl implements JetStreamReader {
        private final NatsJetStreamPullSubscription sub;
        private final int batchSize;
        private final int repullAt;
        private int currentBatchRed;
        private boolean keepGoing = true;

        public JetStreamReaderImpl(final NatsJetStreamPullSubscription sub, final int batchSize, final int repullAt) {
            this.sub = sub;
            this.batchSize = batchSize;
            this.repullAt = Math.max(1, Math.min(batchSize, repullAt));
            currentBatchRed = 0;
            sub.pull(batchSize);
        }

        @Override
        public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
            return track(sub.nextMessage(timeout));
        }

        @Override
        public Message nextMessage(long timeoutMillis) throws InterruptedException, IllegalStateException {
            return track(sub.nextMessage(timeoutMillis));
        }

        private Message track(Message msg) {
            if (msg != null) {
                if (++currentBatchRed == repullAt) {
                    if (keepGoing) {
                        sub.pull(batchSize);
                    }
                }
                if (currentBatchRed == batchSize) {
                    currentBatchRed = 0;
                }
            }
            return msg;
        }

        @Override
        public void stop() {
            keepGoing = false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamReader reader(final int batchSize, final int repullAt) {
        return new JetStreamReaderImpl(this, batchSize, repullAt);
    }
}
