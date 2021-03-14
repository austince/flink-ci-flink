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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A fixed-size {@link MemorySegment} pool used by sort-merge blocking shuffle for file read. */
@Internal
public class BatchReadBufferPool {

    private static final Logger LOG = LoggerFactory.getLogger(BatchReadBufferPool.class);

    /** The minimum total memory size in byte of this batch read buffer pool. */
    public static final int MIN_TOTAL_BYTES = 32 * 1024 * 1024;

    /**
     * The memory size in byte can be allocated from this batch read buffer pool for a single
     * request (8M is for better sequential read).
     */
    public static final int NUM_BYTES_TO_REQUEST = 8 * 1024 * 1024;

    /**
     * The maximum memory size in byte can be allocated from this batch read buffer pool for a
     * single requester.
     */
    public static final int MAX_REQUESTED_BYTES = MIN_TOTAL_BYTES;

    /** The number of total buffers in this batch read buffer pool. */
    private final int numTotalBuffers;

    /** Size of each buffer in byte in this batch read buffer pool. */
    private final int bufferSize;

    /**
     * The number of buffers to be allocated from this batch read buffer pool for a single request.
     */
    private final int numBuffersToRequest;

    /**
     * The maximum number of buffers can be allocated from this batch read buffer pool for a single
     * requester.
     */
    private final int maxRequestedBuffers;

    /** All available buffers can be allocated from this batch read buffer pool. */
    @GuardedBy("buffers")
    private final Queue<MemorySegment> buffers = new ArrayDeque<>();

    /** Account for all the buffers requested per requester. */
    @GuardedBy("buffers")
    private final Map<Object, Counter> numBuffersAllocated = new HashMap<>();

    /** Whether this buffer pool has been destroyed or not. */
    @GuardedBy("buffers")
    private boolean destroyed;

    /** Whether this buffer pool has been initialized or not. */
    @GuardedBy("buffers")
    private boolean initialized;

    public BatchReadBufferPool(long totalBytes, int bufferSize) {
        checkArgument(
                totalBytes >= MIN_TOTAL_BYTES,
                String.format(
                        "The configured memory size must be no smaller than 16M for %s.",
                        TaskManagerOptions.NETWORK_BATCH_READ_MEMORY_SIZE.key()));
        checkArgument(
                totalBytes >= bufferSize,
                String.format(
                        "The configured value for %s must be no smaller than that for %s.",
                        TaskManagerOptions.NETWORK_BATCH_READ_MEMORY_SIZE.key(),
                        TaskManagerOptions.MEMORY_SEGMENT_SIZE.key()));

        this.numTotalBuffers = MathUtils.checkedDownCast(totalBytes / bufferSize);
        this.bufferSize = bufferSize;
        this.numBuffersToRequest = Math.max(1, NUM_BYTES_TO_REQUEST / bufferSize);
        this.maxRequestedBuffers = Math.max(1, MAX_REQUESTED_BYTES / bufferSize);
    }

    public int getNumBuffersToRequest() {
        return numBuffersToRequest;
    }

    public int getMaxRequestedBuffers() {
        return maxRequestedBuffers;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getNumTotalBuffers() {
        return numTotalBuffers;
    }

    public int getAvailableBuffers() {
        synchronized (buffers) {
            return buffers.size();
        }
    }

    /** Initializes this batch read buffer pool which allocates all the buffers. */
    public void initialize() {
        LOG.info(
                "Initializing batch read buffer pool: numBuffers={}, bufferSize={}.",
                numTotalBuffers,
                bufferSize);

        synchronized (buffers) {
            if (initialized || destroyed) {
                return;
            }
            initialized = true;

            try {
                for (int i = 0; i < numTotalBuffers; ++i) {
                    buffers.add(MemorySegmentFactory.allocateUnpooledOffHeapMemory(bufferSize));
                }
            } catch (OutOfMemoryError outOfMemoryError) {
                int allocated = buffers.size();
                buffers.clear();
                throw new OutOfMemoryError(
                        String.format(
                                "Can't initialize batch read buffer pool for sort-merge blocking "
                                        + "shuffle (bytes allocated: %d, bytes still needed: %d)."
                                        + " Please increase %s to avoid this exception. The size "
                                        + "of memory allocated is configured by %s. The occurrence "
                                        + "exception means that some other part of your application"
                                        + " has consumed too many direct memory.",
                                allocated * bufferSize,
                                (numTotalBuffers - allocated) * bufferSize,
                                TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key(),
                                TaskManagerOptions.NETWORK_BATCH_READ_MEMORY_SIZE.key()));
            }
        }
    }

    /**
     * Requests a collection of buffers (determined by {@link #numBuffersToRequest}) from this batch
     * read buffer pool. Exception will be thrown if no enough buffers can be allocated in the given
     * timeout and the corresponding requester is not holding any allocated buffers currently.
     */
    public List<MemorySegment> requestBuffers(Object owner, long timeoutMillis) throws Exception {
        checkArgument(owner != null, "Owner must be not null.");

        List<MemorySegment> allocated = new ArrayList<>(numBuffersToRequest);
        synchronized (buffers) {
            checkState(!destroyed, "Buffer pool is already destroyed.");

            if (!initialized) {
                initialize();
            }

            long startTime = System.currentTimeMillis();
            Counter counter = numBuffersAllocated.get(owner);
            while (buffers.size() < numBuffersToRequest
                    || (counter != null
                            && counter.get() + numBuffersToRequest > maxRequestedBuffers)) {
                checkState(!destroyed, "Buffer pool is already destroyed.");

                buffers.wait(timeoutMillis);
                counter = numBuffersAllocated.get(owner);

                if (counter == null && System.currentTimeMillis() - startTime >= timeoutMillis) {
                    throw new TimeoutException(
                            String.format(
                                    "Can't allocate enough buffers in the given timeout, which means"
                                            + " there is a fierce contention for read buffers, please"
                                            + " increase %s.",
                                    TaskManagerOptions.NETWORK_BATCH_READ_MEMORY_SIZE.key()));
                }
            }

            while (allocated.size() < numBuffersToRequest) {
                allocated.add(buffers.poll());
            }

            if (counter == null) {
                counter = new Counter();
                numBuffersAllocated.put(owner, counter);
            }
            counter.increase(allocated.size());
        }
        return allocated;
    }

    /**
     * Recycles the target buffer to this batch read buffer pool. This method should never throw any
     * exception.
     */
    public void recycle(MemorySegment segment, Object owner) {
        checkArgument(segment != null, "Buffer must be not null.");
        checkArgument(owner != null, "Owner must be not null.");

        synchronized (buffers) {
            checkState(initialized, "Recycling a buffer before initialization.");

            if (destroyed) {
                return;
            }

            decreaseCounter(1, owner);
            buffers.add(segment);

            if (buffers.size() >= numBuffersToRequest) {
                buffers.notifyAll();
            }
        }
    }

    /**
     * Recycles a collection of buffers to this batch read buffer pool. This method should never
     * throw any exception.
     */
    public void recycle(Collection<MemorySegment> segments, Object owner) {
        checkArgument(segments != null, "Buffer list must be not null.");
        checkArgument(owner != null, "Owner must be not null.");

        if (segments.isEmpty()) {
            return;
        }

        synchronized (buffers) {
            checkState(initialized, "Recycling a buffer before initialization.");

            if (destroyed) {
                segments.clear();
                return;
            }

            decreaseCounter(segments.size(), owner);
            buffers.addAll(segments);

            if (buffers.size() >= numBuffersToRequest) {
                buffers.notifyAll();
            }
        }
    }

    private void decreaseCounter(int delta, Object owner) {
        assert Thread.holdsLock(buffers);

        Counter counter = checkNotNull(numBuffersAllocated.get(owner));
        counter.decrease(delta);
        if (counter.get() == 0) {
            numBuffersAllocated.remove(owner);
        }
    }

    /**
     * Destroys this batch read buffer pool and after which, no buffer can be allocated any more.
     */
    public void destroy() {
        synchronized (buffers) {
            destroyed = true;

            buffers.clear();
            numBuffersAllocated.clear();

            buffers.notifyAll();
        }
    }

    public boolean isDestroyed() {
        synchronized (buffers) {
            return destroyed;
        }
    }

    private static final class Counter {

        private int counter = 0;

        int get() {
            return counter;
        }

        void increase(int delta) {
            counter += delta;
        }

        void decrease(int delta) {
            counter -= delta;
        }
    }
}
