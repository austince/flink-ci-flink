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

package org.apache.flink.formats.json;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

import org.apache.commons.compress.utils.BoundedInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * An abstract {@link FileInputFormat} is responsible to calculate the start and end offset when read records
 * from a {@link org.apache.flink.core.fs.FileInputSplit} with json format.
 */
public abstract class AbstractJsonFileInputFormat<T> extends FileInputFormat<T> {

	protected transient InputStream jsonInputStream;

	public AbstractJsonFileInputFormat(Path[] filePaths) {
		setFilePaths(filePaths);
	}

	@Override
	public boolean supportsMultiPaths() {
		return true;
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);

		jsonInputStream = stream;

		long jsonStart = splitStart;
		if (splitStart != 0) {
			jsonStart = findNextLineStartOffset();
		}

		if (splitLength != READ_WHOLE_SPLIT_FLAG) {
			stream.seek(splitStart + splitLength);
			long nextLineStartOffset = findNextLineStartOffset();
			stream.seek(jsonStart);
			jsonInputStream = new BoundedInputStream(stream, nextLineStartOffset - jsonStart);
		} else {
			stream.seek(jsonStart);
		}
	}

	/**
	 * Find next legal line separator to return next offset (first byte offset of next line).
	 *
	 * <p>NOTE: Because of the particularity of UTF-8 encoding, we can determine the number of bytes
	 * of this character only by comparing the first byte, so we do not need to traverse M*N in comparison.
	 */
	private long findNextLineStartOffset() throws IOException {
		byte b;
		while ((b = (byte) stream.read()) != -1) {
			if (b == '\r' || b == '\n') {
				long pos = stream.getPos();

				// deal with "\r\n", next one maybe '\n', so we need skip it.
				if (b == '\r' && (byte) stream.read() == '\n') {
					return stream.getPos();
				} else {
					return pos;
				}
			}
		}
		return stream.getPos();
	}
}
