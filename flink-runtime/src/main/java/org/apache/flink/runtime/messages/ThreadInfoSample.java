/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.messages;

import java.io.Serializable;
import java.lang.management.LockInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

/**
 * A serializable wrapper around {@link java.lang.management.ThreadInfo} that excludes {@link
 * java.lang.management.LockInfo}-based non-serializable fields.
 */
public class ThreadInfoSample implements Serializable {

    private final String threadName;
    private final long threadId;
    private final long blockedTime;
    private final long blockedCount;
    private final long waitedTime;
    private final long waitedCount;
    private final String lockName;
    private final long lockOwnerId;
    private final String lockOwnerName;
    private final boolean inNative;
    private final boolean suspended;
    private final Thread.State threadState;
    private final StackTraceElement[] stackTrace;

    private ThreadInfoSample(
            String threadName,
            long threadId,
            long blockedTime,
            long blockedCount,
            long waitedTime,
            long waitedCount,
            String lockName,
            long lockOwnerId,
            String lockOwnerName,
            boolean inNative,
            boolean suspended,
            Thread.State threadState,
            StackTraceElement[] stackTrace) {
        this.threadName = threadName;
        this.threadId = threadId;
        this.blockedTime = blockedTime;
        this.blockedCount = blockedCount;
        this.waitedTime = waitedTime;
        this.waitedCount = waitedCount;
        this.lockName = lockName;
        this.lockOwnerId = lockOwnerId;
        this.lockOwnerName = lockOwnerName;
        this.inNative = inNative;
        this.suspended = suspended;
        this.threadState = threadState;
        this.stackTrace = stackTrace;
    }

    /**
     * Constructs a {@link ThreadInfoSample} from {@link ThreadInfo}.
     *
     * @param threadInfo {@link ThreadInfo} where the data will be copied from.
     * @return new {@link ThreadInfoSample}
     */
    public static ThreadInfoSample from(ThreadInfo threadInfo) {
        if (threadInfo != null) {
            return new ThreadInfoSample(
                    threadInfo.getThreadName(),
                    threadInfo.getThreadId(),
                    threadInfo.getBlockedTime(),
                    threadInfo.getBlockedCount(),
                    threadInfo.getWaitedTime(),
                    threadInfo.getWaitedCount(),
                    threadInfo.getLockName(),
                    threadInfo.getLockOwnerId(),
                    threadInfo.getLockOwnerName(),
                    threadInfo.isInNative(),
                    threadInfo.isSuspended(),
                    threadInfo.getThreadState(),
                    threadInfo.getStackTrace());
        } else {
            return null;
        }
    }

    /**
     * Returns the ID of the thread associated with this {@code ThreadInfo}.
     *
     * @return the ID of the associated thread.
     */
    public long getThreadId() {
        return threadId;
    }

    /**
     * Returns the name of the thread associated with this {@code ThreadInfo}.
     *
     * @return the name of the associated thread.
     */
    public String getThreadName() {
        return threadName;
    }

    /**
     * Returns the state of the thread associated with this {@code ThreadInfo}.
     *
     * @return {@code Thread.State} of the associated thread.
     */
    public Thread.State getThreadState() {
        return threadState;
    }

    /**
     * Returns the approximate accumulated elapsed time (in milliseconds) that the thread associated
     * with this {@code ThreadInfo} has blocked to enter or reenter a monitor since thread
     * contention monitoring is enabled. I.e. the total accumulated time the thread has been in the
     * {@link java.lang.Thread.State#BLOCKED BLOCKED} state since thread contention monitoring was
     * last enabled. This method returns {@code -1} if thread contention monitoring is disabled.
     *
     * <p>The Java virtual machine may measure the time with a high resolution timer. This statistic
     * is reset when the thread contention monitoring is reenabled.
     *
     * @return the approximate accumulated elapsed time in milliseconds that a thread entered the
     *     {@code BLOCKED} state; {@code -1} if thread contention monitoring is disabled.
     * @throws java.lang.UnsupportedOperationException if the Java virtual machine does not support
     *     this operation.
     * @see ThreadMXBean#isThreadContentionMonitoringSupported
     * @see ThreadMXBean#setThreadContentionMonitoringEnabled
     */
    public long getBlockedTime() {
        return blockedTime;
    }

    /**
     * Returns the total number of times that the thread associated with this {@code ThreadInfo}
     * blocked to enter or reenter a monitor. I.e. the number of times a thread has been in the
     * {@link java.lang.Thread.State#BLOCKED BLOCKED} state.
     *
     * @return the total number of times that the thread entered the {@code BLOCKED} state.
     */
    public long getBlockedCount() {
        return blockedCount;
    }

    /**
     * Returns the approximate accumulated elapsed time (in milliseconds) that the thread associated
     * with this {@code ThreadInfo} has waited for notification since thread contention monitoring
     * is enabled. I.e. the total accumulated time the thread has been in the {@link
     * java.lang.Thread.State#WAITING WAITING} or {@link java.lang.Thread.State#TIMED_WAITING
     * TIMED_WAITING} state since thread contention monitoring is enabled. This method returns
     * {@code -1} if thread contention monitoring is disabled.
     *
     * <p>The Java virtual machine may measure the time with a high resolution timer. This statistic
     * is reset when the thread contention monitoring is reenabled.
     *
     * @return the approximate accumulated elapsed time in milliseconds that a thread has been in
     *     the {@code WAITING} or {@code TIMED_WAITING} state; {@code -1} if thread contention
     *     monitoring is disabled.
     * @throws java.lang.UnsupportedOperationException if the Java virtual machine does not support
     *     this operation.
     * @see ThreadMXBean#isThreadContentionMonitoringSupported
     * @see ThreadMXBean#setThreadContentionMonitoringEnabled
     */
    public long getWaitedTime() {
        return waitedTime;
    }

    /**
     * Returns the total number of times that the thread associated with this {@code ThreadInfo}
     * waited for notification. I.e. the number of times that a thread has been in the {@link
     * java.lang.Thread.State#WAITING WAITING} or {@link java.lang.Thread.State#TIMED_WAITING
     * TIMED_WAITING} state.
     *
     * @return the total number of times that the thread was in the {@code WAITING} or {@code
     *     TIMED_WAITING} state.
     */
    public long getWaitedCount() {
        return waitedCount;
    }

    /**
     * Returns the {@link LockInfo#toString string representation} of an object for which the thread
     * associated with this {@code ThreadInfo} is blocked waiting. This method is equivalent to
     * calling:
     *
     * <blockquote>
     *
     * <pre>
     * getLockInfo().toString()
     * </pre>
     *
     * </blockquote>
     *
     * <p>This method will return {@code null} if this thread is not blocked waiting for any object
     * or if the object is not owned by any thread.
     *
     * @return the string representation of the object on which the thread is blocked if any; {@code
     *     null} otherwise.
     */
    public String getLockName() {
        return lockName;
    }

    /**
     * Returns the ID of the thread which owns the object for which the thread associated with this
     * {@code ThreadInfo} is blocked waiting. This method will return {@code -1} if this thread is
     * not blocked waiting for any object or if the object is not owned by any thread.
     *
     * @return the thread ID of the owner thread of the object this thread is blocked on; {@code -1}
     *     if this thread is not blocked or if the object is not owned by any thread.
     */
    public long getLockOwnerId() {
        return lockOwnerId;
    }

    /**
     * Returns the name of the thread which owns the object for which the thread associated with
     * this {@code ThreadInfo} is blocked waiting. This method will return {@code null} if this
     * thread is not blocked waiting for any object or if the object is not owned by any thread.
     *
     * @return the name of the thread that owns the object this thread is blocked on; {@code null}
     *     if this thread is not blocked or if the object is not owned by any thread.
     */
    public String getLockOwnerName() {
        return lockOwnerName;
    }

    /**
     * Returns the stack trace of the thread associated with this {@code ThreadInfo}. If no stack
     * trace was requested for this thread info, this method will return a zero-length array. If the
     * returned array is of non-zero length then the first element of the array represents the top
     * of the stack, which is the most recent method invocation in the sequence. The last element of
     * the array represents the bottom of the stack, which is the least recent method invocation in
     * the sequence.
     *
     * <p>Some Java virtual machines may, under some circumstances, omit one or more stack frames
     * from the stack trace. In the extreme case, a virtual machine that has no stack trace
     * information concerning the thread associated with this {@code ThreadInfo} is permitted to
     * return a zero-length array from this method.
     *
     * @return an array of {@code StackTraceElement} objects of the thread.
     */
    public StackTraceElement[] getStackTrace() {
        return stackTrace.clone();
    }

    /**
     * Tests if the thread associated with this {@code ThreadInfo} is suspended. This method returns
     * {@code true} if {@link Thread#suspend} has been called.
     *
     * @return {@code true} if the thread is suspended; {@code false} otherwise.
     */
    public boolean isSuspended() {
        return suspended;
    }

    /**
     * Tests if the thread associated with this {@code ThreadInfo} is executing native code via the
     * Java Native Interface (JNI). The JNI native code does not include the virtual machine support
     * code or the compiled native code generated by the virtual machine.
     *
     * @return {@code true} if the thread is executing native code; {@code false} otherwise.
     */
    public boolean isInNative() {
        return inNative;
    }
}
