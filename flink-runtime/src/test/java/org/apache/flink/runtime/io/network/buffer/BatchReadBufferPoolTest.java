/*
 * Copyright 2012 The Netty Project
 * Copy from netty 4.1.32.Final
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.MathUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BatchReadBufferPool}. */
public class BatchReadBufferPoolTest {

    @Rule public Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    @Test(expected = IllegalArgumentException.class)
    public void testPoolSizeTooSmall() {
        new BatchReadBufferPool(BatchReadBufferPool.MIN_TOTAL_BYTES - 1, 1024);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPoolSizeTooLarge() {
        new BatchReadBufferPool(Long.MAX_VALUE, 1024);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPoolSizeSmallerThanBufferSize() {
        new BatchReadBufferPool(
                BatchReadBufferPool.MIN_TOTAL_BYTES, BatchReadBufferPool.MIN_TOTAL_BYTES + 1);
    }

    @Test
    public void testBufferCalculation() {
        int poolSize = 2 * BatchReadBufferPool.MIN_TOTAL_BYTES;
        for (int bufferSize = 32 * 1024; bufferSize <= poolSize; bufferSize += 1024) {
            BatchReadBufferPool bufferPool = new BatchReadBufferPool(poolSize, bufferSize);

            assertEquals(
                    MathUtils.checkedDownCast(poolSize / bufferSize),
                    bufferPool.getNumTotalBuffers());
            assertEquals(
                    Math.max(1, BatchReadBufferPool.NUM_BYTES_TO_REQUEST / bufferSize),
                    bufferPool.getNumBuffersToRequest());
            assertEquals(
                    Math.max(1, BatchReadBufferPool.MAX_REQUESTED_BYTES / bufferSize),
                    bufferPool.getMaxRequestedBuffers());
        }
    }

    @Test
    public void testRequestBuffers() throws Exception {
        int poolSize = BatchReadBufferPool.MIN_TOTAL_BYTES;
        int bufferSize = 32 * 1024;
        BatchReadBufferPool bufferPool = new BatchReadBufferPool(poolSize, bufferSize);
        List<MemorySegment> buffersAllocated = new ArrayList<>();

        try {
            buffersAllocated.addAll(bufferPool.requestBuffers(this, 10000));
            assertEquals(bufferPool.getNumBuffersToRequest(), buffersAllocated.size());
        } finally {
            bufferPool.recycle(buffersAllocated, this);
            assertEquals(bufferPool.getNumTotalBuffers(), bufferPool.getAvailableBuffers());
            bufferPool.destroy();
        }
    }

    @Test(expected = TimeoutException.class)
    public void testRequestBuffersTimeout() throws Exception {
        int poolSize = BatchReadBufferPool.MIN_TOTAL_BYTES;
        int bufferSize = 32 * 1024;
        BatchReadBufferPool bufferPool = new BatchReadBufferPool(poolSize, bufferSize);
        List<MemorySegment> buffersAllocated = new ArrayList<>();

        try {
            for (int i = 0; i < 4; ++i) {
                buffersAllocated.addAll(bufferPool.requestBuffers(this, 1000));
            }
            bufferPool.requestBuffers(new Object(), 1000);
        } finally {
            bufferPool.recycle(buffersAllocated, this);
            assertEquals(bufferPool.getNumTotalBuffers(), bufferPool.getAvailableBuffers());
            bufferPool.destroy();
        }
    }

    @Test
    public void testNoBufferTimeoutIfHoldingBuffers() throws Exception {
        int poolSize = BatchReadBufferPool.MIN_TOTAL_BYTES;
        int bufferSize = 32 * 1024;
        Object owner = new Object();

        AtomicReference<Throwable> timeoutException = new AtomicReference<>();
        BatchReadBufferPool bufferPool = new BatchReadBufferPool(poolSize, bufferSize);
        List<MemorySegment> buffersAllocated = Collections.synchronizedList(new ArrayList<>());

        try {
            Thread requestThread =
                    new Thread(
                            () -> {
                                try {
                                    for (int i = 0; i < 4; ++i) {
                                        buffersAllocated.addAll(
                                                bufferPool.requestBuffers(owner, 100));
                                    }
                                    bufferPool.requestBuffers(owner, 100);
                                } catch (TimeoutException exception) {
                                    timeoutException.set(exception);
                                } catch (Throwable ignored) {
                                }
                            });
            requestThread.start();

            Thread.sleep(1000);
            requestThread.interrupt();
            requestThread.join();

            assertNull(timeoutException.get());
        } finally {
            bufferPool.recycle(buffersAllocated, owner);
            assertEquals(bufferPool.getNumTotalBuffers(), bufferPool.getAvailableBuffers());
            bufferPool.destroy();
        }
    }

    @Test
    public void testBufferFulfilledByRecycledBuffers() throws Exception {
        int poolSize = BatchReadBufferPool.MIN_TOTAL_BYTES;
        int bufferSize = 32 * 1024;
        int numRequestThreads = 2;

        AtomicReference<Throwable> exception = new AtomicReference<>();
        BatchReadBufferPool bufferPool = new BatchReadBufferPool(poolSize, bufferSize);
        Map<Object, List<MemorySegment>> allocatedBuffers = new ConcurrentHashMap<>();

        try {
            Object[] owners = new Object[] {new Object(), new Object(), new Object(), new Object()};
            for (int i = 0; i < 4; ++i) {
                allocatedBuffers.put(owners[i], bufferPool.requestBuffers(owners[i], 1000));
            }
            assertEquals(0, bufferPool.getAvailableBuffers());

            Thread[] requestThreads = new Thread[numRequestThreads];
            for (int i = 0; i < numRequestThreads; ++i) {
                requestThreads[i] =
                        new Thread(
                                () -> {
                                    try {
                                        Object owner = new Object();
                                        allocatedBuffers.put(
                                                owner,
                                                bufferPool.requestBuffers(
                                                        owner, Integer.MAX_VALUE));
                                    } catch (Throwable throwable) {
                                        exception.set(throwable);
                                    }
                                });
                requestThreads[i].start();
            }

            // recycle one by one
            for (MemorySegment segment : allocatedBuffers.remove(owners[0])) {
                bufferPool.recycle(segment, owners[0]);
            }

            // bulk recycle
            bufferPool.recycle(allocatedBuffers.remove(owners[1]), owners[1]);

            for (Thread requestThread : requestThreads) {
                requestThread.join();
            }

            assertNull(exception.get());
            assertEquals(0, bufferPool.getAvailableBuffers());
            assertEquals(4, allocatedBuffers.size());
        } finally {
            for (Object owner : allocatedBuffers.keySet()) {
                bufferPool.recycle(allocatedBuffers.remove(owner), owner);
            }
            assertEquals(bufferPool.getNumTotalBuffers(), bufferPool.getAvailableBuffers());
            bufferPool.destroy();
        }
    }

    @Test
    public void testMultipleThreadRequestAndRecycle() throws Exception {
        int poolSize = BatchReadBufferPool.MIN_TOTAL_BYTES;
        int bufferSize = 32 * 1024;
        int numRequestThreads = 10;
        AtomicReference<Throwable> exception = new AtomicReference<>();
        BatchReadBufferPool bufferPool = new BatchReadBufferPool(poolSize, bufferSize);

        try {
            Thread[] requestThreads = new Thread[numRequestThreads];
            for (int i = 0; i < numRequestThreads; ++i) {
                requestThreads[i] =
                        new Thread(
                                () -> {
                                    try {
                                        Object owner = new Object();
                                        for (int j = 0; j < 100; ++j) {
                                            List<MemorySegment> allocatedBuffers =
                                                    bufferPool.requestBuffers(
                                                            owner, Integer.MAX_VALUE);
                                            Thread.sleep(10);
                                            if (j % 2 == 0) {
                                                bufferPool.recycle(allocatedBuffers, owner);
                                            } else {
                                                for (MemorySegment segment : allocatedBuffers) {
                                                    bufferPool.recycle(segment, owner);
                                                }
                                            }
                                        }
                                    } catch (Throwable throwable) {
                                        exception.set(throwable);
                                    }
                                });
                requestThreads[i].start();
            }

            for (Thread requestThread : requestThreads) {
                requestThread.join();
            }

            assertNull(exception.get());
            assertEquals(bufferPool.getNumTotalBuffers(), bufferPool.getAvailableBuffers());
        } finally {
            bufferPool.destroy();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testDestroy() throws Exception {
        int poolSize = BatchReadBufferPool.MIN_TOTAL_BYTES;
        int bufferSize = 32 * 1024;
        BatchReadBufferPool bufferPool = new BatchReadBufferPool(poolSize, bufferSize);
        bufferPool.initialize();

        assertFalse(bufferPool.isDestroyed());
        assertEquals(bufferPool.getNumTotalBuffers(), bufferPool.getAvailableBuffers());

        List<MemorySegment> buffersAllocated = bufferPool.requestBuffers(this, 1000);
        assertEquals(
                bufferPool.getNumTotalBuffers() - buffersAllocated.size(),
                bufferPool.getAvailableBuffers());

        bufferPool.destroy();
        assertTrue(bufferPool.isDestroyed());
        assertEquals(0, bufferPool.getAvailableBuffers());

        bufferPool.recycle(buffersAllocated, this);
        assertEquals(0, bufferPool.getAvailableBuffers());

        bufferPool.requestBuffers(this, 1000);
    }

    @Test
    public void testDestroyWhileBlockingRequest() throws Exception {
        int poolSize = BatchReadBufferPool.MIN_TOTAL_BYTES;
        int bufferSize = 32 * 1024;
        Object owner = new Object();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        BatchReadBufferPool bufferPool = new BatchReadBufferPool(poolSize, bufferSize);
        Thread requestThread =
                new Thread(
                        () -> {
                            try {
                                while (true) {
                                    bufferPool.requestBuffers(owner, Integer.MAX_VALUE);
                                }
                            } catch (Throwable throwable) {
                                exception.set(throwable);
                            }
                        });
        requestThread.start();

        Thread.sleep(1000);
        bufferPool.destroy();
        requestThread.join();

        assertTrue(exception.get() instanceof IllegalStateException);
    }
}
