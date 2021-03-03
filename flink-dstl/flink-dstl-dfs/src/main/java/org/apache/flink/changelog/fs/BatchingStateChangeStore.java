/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.changelog.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.lang.Thread.holdsLock;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.rethrow;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StateChangeStore} that waits for some configured amount of time before passing the
 * accumulated state changes to the actual store.
 */
@ThreadSafe
class BatchingStateChangeStore implements StateChangeStore {
    private static final Logger LOG = LoggerFactory.getLogger(BatchingStateChangeStore.class);

    private final ScheduledExecutorService scheduler;
    private final long scheduleDelayMs;

    @GuardedBy("scheduled")
    private final Queue<StateChangeSet> scheduled;

    @GuardedBy("scheduled")
    private ScheduledFuture<?> scheduledFuture;

    private final RetryPolicy retryPolicy;
    private volatile Throwable error;
    private final int sizeThreshold;
    private final RetryingExecutor retryingExecutor;
    private final StateChangeStore delegate;

    BatchingStateChangeStore(
            long persistDelayMs,
            int sizeThreshold,
            RetryPolicy retryPolicy,
            StateChangeStore delegate) {
        this(
                persistDelayMs,
                sizeThreshold,
                retryPolicy,
                delegate,
                SchedulerFactory.create(1, "ChangelogRetryScheduler", LOG),
                new RetryingExecutor());
    }

    BatchingStateChangeStore(
            long persistDelayMs,
            int sizeThreshold,
            RetryPolicy retryPolicy,
            StateChangeStore delegate,
            ScheduledExecutorService scheduler,
            RetryingExecutor retryingExecutor) {
        this.scheduleDelayMs = persistDelayMs;
        this.scheduled = new LinkedList<>();
        this.scheduler = scheduler;
        this.retryPolicy = retryPolicy;
        this.retryingExecutor = retryingExecutor;
        this.sizeThreshold = sizeThreshold;
        this.delegate = delegate;
    }

    @Override
    public void save(Collection<StateChangeSet> changeSets) {
        if (error != null) {
            LOG.debug("don't persist {} changesets, already failed", changeSets.size());
            changeSets.forEach(cs -> cs.setFailed(error));
            return;
        }
        LOG.debug("persist {} changeSets", changeSets.size());
        try {
            synchronized (scheduled) {
                scheduled.addAll(changeSets);
                scheduleUploadIfNeeded();
            }
        } catch (Exception e) {
            changeSets.forEach(cs -> cs.setFailed(e));
            rethrow(e);
        }
    }

    private void scheduleUploadIfNeeded() {
        checkState(holdsLock(scheduled));
        if (scheduleDelayMs == 0 || scheduled.size() >= sizeThreshold) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                scheduledFuture = null;
            }
            drainAndSave();
        } else if (scheduledFuture == null) {
            scheduledFuture = scheduler.schedule(this::drainAndSave, scheduleDelayMs, MILLISECONDS);
        }
    }

    private void drainAndSave() {
        Collection<StateChangeSet> changeSets;
        synchronized (scheduled) {
            changeSets = new ArrayList<>(scheduled);
            scheduled.clear();
            scheduledFuture = null;
        }
        try {
            if (error != null) {
                changeSets.forEach(changeSet -> changeSet.setFailed(error));
                return;
            }
            retryingExecutor.execute(retryPolicy, () -> delegate.save(changeSets));
        } catch (Throwable t) {
            changeSets.forEach(changeSet -> changeSet.setFailed(t));
            if (findThrowable(t, IOException.class).isPresent()) {
                LOG.warn("Caught IO exception while uploading", t);
            } else {
                error = t;
                rethrow(t);
            }
        }
    }

    @Override
    public void close() throws Exception {
        LOG.debug("close");
        scheduler.shutdownNow();
        if (!scheduler.awaitTermination(1, SECONDS)) {
            LOG.warn("Unable to cleanly shutdown scheduler in 1s");
        }
        ArrayList<StateChangeSet> drained;
        synchronized (scheduled) {
            drained = new ArrayList<>(scheduled);
            scheduled.clear();
        }
        drained.forEach(StateChangeSet::setCancelled);
        retryingExecutor.close();
        delegate.close();
    }
}
