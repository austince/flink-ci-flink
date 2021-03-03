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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.changelog.fs.FsStateChangelogOptions.BASE_PATH;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.BATCH_ENABLED;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.PERSIST_DELAY_MS;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.PERSIST_SIZE_THRESHOLD;

// todo in MVP or later: implement CheckpointStreamFactory / CheckpointStorageWorkerView - based
// store
// Considerations:
// 0. need for checkpointId in the current API to resolve the location
//   option a: pass checkpointId (race condition?)
//   option b: pass location (race condition?)
//   option c: add FsCheckpointStorageAccess.createSharedStateStream
// 1. different settings for materialized/changelog (e.g. timeouts)
// 2. re-use closeAndGetHandle
// 3. re-use in-memory handles (.metadata)
// 4. handle in-memory handles duplication
interface StateChangeStore extends AutoCloseable {

    void save(Collection<StateChangeSet> changeSets) throws IOException;

    static StateChangeStore fromConfig(ReadableConfig config) throws IOException {
        StateChangeFsStore store = createSimpleFsStore(new Path(config.get(BASE_PATH)));
        if (config.get(BATCH_ENABLED)) {
            return createBatchingStore(
                    config.get(PERSIST_DELAY_MS),
                    config.get(PERSIST_SIZE_THRESHOLD).getBytes(),
                    RetryPolicy.fromConfig(config),
                    store);
        } else {
            return store;
        }
    }

    static StateChangeFsStore createSimpleFsStore(Path basePath) throws IOException {
        return new StateChangeFsStore(basePath, basePath.getFileSystem());
    }

    static StateChangeStore createBatchingStore(
            long persistDelayMs,
            long persistSizeThreshold,
            RetryPolicy retryPolicy,
            StateChangeFsStore delegate) {
        return new BatchingStateChangeStore(
                persistDelayMs, persistSizeThreshold, retryPolicy, delegate);
    }
}
