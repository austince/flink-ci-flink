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

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import static org.apache.flink.changelog.fs.StateChangeSet.Status.MATERIALIZED;
import static org.apache.flink.shaded.guava18.com.google.common.collect.Iterables.getOnlyElement;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** {@link FsStateChangelogWriter} test. */
public class FsStateChangelogWriterTest {
    private static final int KEY_GROUP = 0;
    private final Random random = new Random();

    @Test
    public void testAppend() throws IOException {
        withWriter(
                (writer, store) -> {
                    writer.append(KEY_GROUP, getBytes());
                    writer.append(KEY_GROUP, getBytes());
                    writer.append(KEY_GROUP, getBytes());
                    assertTrue("shouldn't persist", store.getSaved().isEmpty());
                });
    }

    @Test
    public void testPersist() throws IOException {
        withWriter(
                (writer, store) -> {
                    byte[] bytes = getBytes();
                    writer.persist(append(writer, bytes));
                    assertSubmittedOnly(store, bytes);
                });
    }

    @Test
    public void testPersistAgain() throws IOException {
        withWriter(
                (writer, store) -> {
                    SequenceNumber sqn = append(writer, getBytes());
                    writer.persist(sqn);
                    store.finalizeUpload(true);
                    store.reset();
                    writer.persist(sqn);
                    assertTrue(store.getSaved().isEmpty());
                });
    }

    @Test
    public void testPersistAgainBeforeCompletion() throws IOException {
        withWriter(
                (writer, store) -> {
                    byte[] bytes = getBytes();
                    SequenceNumber sqn = append(writer, bytes);
                    store.finalizeUpload(false); // don't confirm
                    store.reset();
                    writer.persist(sqn);
                    assertSubmittedOnly(store, bytes);
                });
    }

    @Test
    public void testPersistNewlyAppended() throws IOException {
        withWriter(
                (writer, store) -> {
                    SequenceNumber sqn = append(writer, getBytes());
                    writer.persist(sqn);
                    store.finalizeUpload(true);
                    store.reset();
                    byte[] bytes = getBytes();
                    sqn = append(writer, bytes);
                    writer.persist(sqn);
                    assertSubmittedOnly(store, bytes);
                });
    }

    /** Emulates checkpoint abortion followed by a new checkpoint. */
    @Test
    public void testPersistAfterReset() throws IOException {
        withWriter(
                (writer, store) -> {
                    byte[] bytes = getBytes();
                    SequenceNumber sqn = append(writer, bytes);
                    store.finalizeUpload(false);
                    writer.reset(sqn, SequenceNumber.of(Long.MAX_VALUE));
                    store.reset();
                    writer.persist(sqn);
                    assertSubmittedOnly(store, bytes);
                });
    }

    @Test
    public void testPersistAfterFailure() throws IOException {
        withWriter(
                (writer, store) -> {
                    byte[] bytes = getBytes();
                    SequenceNumber sqn = append(writer, bytes);
                    store.failUpload();
                    store.reset();
                    writer.persist(sqn);
                    assertSubmittedOnly(store, bytes);
                });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTruncate() throws IOException {
        withWriter(
                (writer, store) -> {
                    SequenceNumber sqn = append(writer, getBytes());
                    writer.truncate(sqn.next());
                    store.getSaved().forEach(cs -> assertEquals(MATERIALIZED, cs.getStatus()));
                    writer.persist(sqn);
                });
    }

    private void withWriter(
            BiConsumerWithException<FsStateChangelogWriter, TestingStateChangeStore, IOException>
                    test)
            throws IOException {
        TestingStateChangeStore store = new TestingStateChangeStore();
        try (FsStateChangelogWriter writer =
                new FsStateChangelogWriter(
                        UUID.randomUUID(), KeyGroupRange.of(KEY_GROUP, KEY_GROUP), store)) {
            test.accept(writer, store);
        }
    }

    private void assertSubmittedOnly(TestingStateChangeStore store, byte[] bytes) {
        assertArrayEquals(
                bytes, getOnlyElement(getOnlyElement(store.getSaved()).getChanges()).getChange());
    }

    private SequenceNumber append(FsStateChangelogWriter writer, byte[] bytes) {
        SequenceNumber sqn = writer.lastAppendedSequenceNumber().next();
        writer.append(KEY_GROUP, bytes);
        return sqn;
    }

    private byte[] getBytes() {
        byte[] bytes = new byte[10];
        random.nextBytes(bytes);
        return bytes;
    }
}
