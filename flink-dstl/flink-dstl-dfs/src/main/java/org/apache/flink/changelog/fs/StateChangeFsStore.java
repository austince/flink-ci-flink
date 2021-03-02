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

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.core.fs.FileSystem.WriteMode.NO_OVERWRITE;

class StateChangeFsStore implements StateChangeStore {
    private static final Logger LOG = LoggerFactory.getLogger(StateChangeFsStore.class);

    private final Path basePath;
    private final FileSystem fileSystem;
    private final StateChangeFormat format;

    public StateChangeFsStore(Path basePath, FileSystem fileSystem) {
        this.basePath = basePath;
        this.fileSystem = fileSystem;
        this.format = new StateChangeFormat();
    }

    @Override
    public void save(Collection<StateChangeSet> changeSets) throws IOException {
        final String fileName = generateFileName();
        LOG.debug("upload {} to {}", changeSets, fileName);
        Path path = new Path(basePath, fileName);
        try {
            try (FSDataOutputStream os = fileSystem.create(path, NO_OVERWRITE)) {
                upload(changeSets, path, os);
            }
        } catch (IOException e) {
            changeSets.forEach(cs -> cs.setFailed(e));
            handleError(path, e);
        }
    }

    private void handleError(Path path, IOException e) throws IOException {
        try {
            fileSystem.delete(path, true);
        } catch (IOException cleanupError) {
            LOG.warn("unable to delete after failure: " + path, cleanupError);
            e.addSuppressed(cleanupError);
        }
        throw e;
    }

    private void upload(Collection<StateChangeSet> changeSets, Path path, FSDataOutputStream os)
            throws IOException {
        final Map<StateChangeSet, Long> offsets = format.write(os, changeSets);
        if (offsets.isEmpty()) {
            LOG.info(
                    "nothing to upload (cancelled concurrently), cancelling upload {} {}",
                    changeSets.size(),
                    path);
            fileSystem.delete(path, true);
        } else {
            final long size = os.getPos();
            os.close();
            final StreamStateHandle handle = new FileStateHandle(path, size);
            offsets.forEach(
                    (changes, offset) ->
                            changes.setUploaded(
                                    new StoreResult(handle, offset, changes.getSequenceNumber())));
        }
    }

    private String generateFileName() {
        return UUID.randomUUID().toString();
    }

    @Override
    public void close() {}
}
