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

package org.apache.flink.connector.file.sink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.connector.file.sink.committer.FileCommitter;
import org.apache.flink.connector.file.sink.writer.DefaultFileWriterBucketFactory;
import org.apache.flink.connector.file.sink.writer.FileWriter;
import org.apache.flink.connector.file.sink.writer.FileWriterBucketFactory;
import org.apache.flink.connector.file.sink.writer.FileWriterBucketState;
import org.apache.flink.connector.file.sink.writer.FileWriterBucketStateSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.BulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.RowWiseBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A unified sink for both streaming and blocking mode, based on the new Sink API.
 */
@Experimental
public class FileSink<IN, BucketID>
		implements Sink<IN, FileSinkCommittable, FileWriterBucketState<BucketID>, Void> {

	private final BucketsBuilder<IN, BucketID, ? extends BucketsBuilder<IN, BucketID, ?>> bucketsBuilder;

	private FileSink(BucketsBuilder<IN, BucketID, ? extends BucketsBuilder<IN, BucketID, ?>> bucketsBuilder) {
		this.bucketsBuilder = checkNotNull(bucketsBuilder);
	}

	@Override
	public Writer<IN, FileSinkCommittable, FileWriterBucketState<BucketID>> createWriter(
			InitContext context,
			List<FileWriterBucketState<BucketID>> states) throws IOException {
		FileWriter<IN, BucketID> writer = bucketsBuilder.createWriter(context);
		writer.initializeState(states);
		return writer;
	}

	@Override
	public Optional<SimpleVersionedSerializer<FileWriterBucketState<BucketID>>> getWriterStateSerializer()
			throws IOException {
		return Optional.of(bucketsBuilder.getWriterStateSerializer());
	}

	@Override
	public Optional<Committer<FileSinkCommittable>> createCommitter() throws IOException {
		return Optional.of(bucketsBuilder.createCommitter());
	}

	@Override
	public Optional<SimpleVersionedSerializer<FileSinkCommittable>> getCommittableSerializer()
			throws IOException {
		return Optional.of(bucketsBuilder.getCommittableSerializer());
	}

	@Override
	public Optional<GlobalCommitter<FileSinkCommittable, Void>> createGlobalCommitter() {
		return Optional.empty();
	}

	@Override
	public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
		return Optional.empty();
	}

	public static <IN> DefaultRowFormatBuilder<IN> forRowFormat(
			final Path basePath, final Encoder<IN> encoder) {
		return new DefaultRowFormatBuilder<>(basePath, encoder, new DateTimeBucketAssigner<>());
	}

	/**
	 * The base abstract class for the {@link RowFormatBuilder} and {@link BulkFormatBuilder}.
	 */
	@Internal
	private abstract static class BucketsBuilder<IN, BucketID, T extends BucketsBuilder<IN, BucketID, T>>
			implements Serializable {

		private static final long serialVersionUID = 1L;

		public static final long DEFAULT_BUCKET_CHECK_INTERVAL = 60L * 1000L;

		@SuppressWarnings("unchecked")
		protected T self() {
			return (T) this;
		}

		@Internal
		protected abstract FileWriter<IN, BucketID> createWriter(final InitContext context) throws IOException;

		@Internal
		protected abstract FileCommitter createCommitter() throws IOException;

		@Internal
		protected abstract SimpleVersionedSerializer<FileWriterBucketState<BucketID>> getWriterStateSerializer() throws IOException;

		@Internal
		protected abstract SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer() throws IOException;
	}

	/**
	 * A builder for configuring the sink for row-wise encoding formats.
	 */
	public static class RowFormatBuilder<IN, BucketID, T extends RowFormatBuilder<IN, BucketID, T>>
			extends BucketsBuilder<IN, BucketID, T> {

		private static final long serialVersionUID = 1L;

		private final Path basePath;

		private final Encoder<IN> encoder;

		private BucketAssigner<IN, BucketID> bucketAssigner;

		private RollingPolicy<IN, BucketID> rollingPolicy;

		private FileWriterBucketFactory<IN, BucketID> bucketFactory;

		private OutputFileConfig outputFileConfig;

		protected RowFormatBuilder(
				Path basePath,
				Encoder<IN> encoder,
				BucketAssigner<IN, BucketID> bucketAssigner) {
			this(
					basePath,
					encoder,
					bucketAssigner,
					DefaultRollingPolicy.builder().build(),
					new DefaultFileWriterBucketFactory<>(),
					OutputFileConfig.builder().build());
		}

		protected RowFormatBuilder(
				Path basePath,
				Encoder<IN> encoder,
				BucketAssigner<IN, BucketID> assigner,
				RollingPolicy<IN, BucketID> policy,
				FileWriterBucketFactory<IN, BucketID> bucketFactory,
				OutputFileConfig outputFileConfig) {
			this.basePath = checkNotNull(basePath);
			this.encoder = checkNotNull(encoder);
			this.bucketAssigner = checkNotNull(assigner);
			this.rollingPolicy = checkNotNull(policy);
			this.bucketFactory = checkNotNull(bucketFactory);
			this.outputFileConfig = checkNotNull(outputFileConfig);
		}

		public T withBucketAssigner(final BucketAssigner<IN, BucketID> assigner) {
			this.bucketAssigner = checkNotNull(assigner);
			return self();
		}

		public T withRollingPolicy(final RollingPolicy<IN, BucketID> policy) {
			this.rollingPolicy = checkNotNull(policy);
			return self();
		}

		public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
			this.outputFileConfig = outputFileConfig;
			return self();
		}

		public RowFormatBuilder<IN, BucketID, ? extends RowFormatBuilder<IN, BucketID, ?>> withNewBucketAssignerAndPolicy(
				BucketAssigner<IN, BucketID> assigner,
				RollingPolicy<IN, BucketID> policy) {
			Preconditions.checkState(
					bucketFactory.getClass() == DefaultFileWriterBucketFactory.class,
					"newBuilderWithBucketAssignerAndPolicy() cannot be called "
							+ "after specifying a customized bucket factory");
			return new RowFormatBuilder<>(
					basePath,
					encoder,
					checkNotNull(assigner),
					checkNotNull(policy),
					bucketFactory,
					outputFileConfig);
		}

		@VisibleForTesting
		T withBucketFactory(final FileWriterBucketFactory<IN, BucketID> factory) {
			this.bucketFactory = Preconditions.checkNotNull(factory);
			return self();
		}

		/** Creates the actual sink. */
		public FileSink<IN, BucketID> build() {
			return new FileSink<>(this);
		}

		@Override
		public FileWriter<IN, BucketID> createWriter(InitContext context) throws IOException {
			return new FileWriter<>(
					basePath,
					bucketAssigner,
					bucketFactory,
					createBucketWriter(),
					rollingPolicy,
					outputFileConfig);
		}

		@Override
		public FileCommitter createCommitter() throws IOException {
			return new FileCommitter(createBucketWriter());
		}

		@Override
		public SimpleVersionedSerializer<FileWriterBucketState<BucketID>> getWriterStateSerializer() throws IOException {
			return new FileWriterBucketStateSerializer<>(
					createBucketWriter()
							.getProperties()
							.getInProgressFileRecoverableSerializer(),
					bucketAssigner.getSerializer());
		}

		@Override
		public SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer() throws IOException {
			BucketWriter<IN, BucketID> bucketWriter = createBucketWriter();

			return new FileSinkCommittableSerializer(
					bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
					bucketWriter.getProperties().getInProgressFileRecoverableSerializer());
		}

		private BucketWriter<IN, BucketID> createBucketWriter() throws IOException {
			return new RowWiseBucketWriter<>(
					FileSystem.get(basePath.toUri()).createRecoverableWriter(),
					encoder);
		}
	}

	/**
	 * Builder for the vanilla {@link FileSink} using a row format.
	 */
	public static final class DefaultRowFormatBuilder<IN> extends RowFormatBuilder<IN, String, DefaultRowFormatBuilder<IN>> {
		private static final long serialVersionUID = -8503344257202146718L;

		private DefaultRowFormatBuilder(
				Path basePath,
				Encoder<IN> encoder,
				BucketAssigner<IN, String> bucketAssigner) {
			super(basePath, encoder, bucketAssigner);
		}
	}

	/**
	 * A builder for configuring the sink for bulk-encoding formats, e.g. Parquet/ORC.
	 */
	@PublicEvolving
	public static class BulkFormatBuilder<IN, BucketID, T extends BulkFormatBuilder<IN, BucketID, T>>
			extends BucketsBuilder<IN, BucketID, T> {

		private static final long serialVersionUID = 1L;

		private final Path basePath;

		private BulkWriter.Factory<IN> writerFactory;

		private BucketAssigner<IN, BucketID> bucketAssigner;

		private CheckpointRollingPolicy<IN, BucketID> rollingPolicy;

		private FileWriterBucketFactory<IN, BucketID> bucketFactory;

		private OutputFileConfig outputFileConfig;

		protected BulkFormatBuilder(
				Path basePath,
				BulkWriter.Factory<IN> writerFactory,
				BucketAssigner<IN, BucketID> assigner) {
			this(
					basePath,
					writerFactory,
					assigner,
					OnCheckpointRollingPolicy.build(),
					new DefaultFileWriterBucketFactory<>(),
					OutputFileConfig.builder().build());
		}

		protected BulkFormatBuilder(
				Path basePath,
				BulkWriter.Factory<IN> writerFactory,
				BucketAssigner<IN, BucketID> assigner,
				CheckpointRollingPolicy<IN, BucketID> policy,
				FileWriterBucketFactory<IN, BucketID> bucketFactory,
				OutputFileConfig outputFileConfig) {
			this.basePath = Preconditions.checkNotNull(basePath);
			this.writerFactory = writerFactory;
			this.bucketAssigner = Preconditions.checkNotNull(assigner);
			this.rollingPolicy = Preconditions.checkNotNull(policy);
			this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
			this.outputFileConfig = Preconditions.checkNotNull(outputFileConfig);
		}

		public T withBucketAssigner(BucketAssigner<IN, BucketID> assigner) {
			this.bucketAssigner = Preconditions.checkNotNull(assigner);
			return self();
		}

		public T withRollingPolicy(CheckpointRollingPolicy<IN, BucketID> rollingPolicy) {
			this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy);
			return self();
		}

		@VisibleForTesting
		T withBucketFactory(final FileWriterBucketFactory<IN, BucketID> factory) {
			this.bucketFactory = Preconditions.checkNotNull(factory);
			return self();
		}

		public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
			this.outputFileConfig = outputFileConfig;
			return self();
		}

		public BulkFormatBuilder<IN, BucketID, ? extends BulkFormatBuilder<IN, BucketID, ?>> withNewBucketAssigner(
				BucketAssigner<IN, BucketID> assigner) {
			Preconditions.checkState(
					bucketFactory.getClass() == DefaultFileWriterBucketFactory.class,
					"newBuilderWithBucketAssigner() cannot be called "
							+ "after specifying a customized bucket factory");
			return new BulkFormatBuilder<>(
					basePath,
					writerFactory,
					Preconditions.checkNotNull(assigner),
					rollingPolicy,
					bucketFactory,
					outputFileConfig);
		}

		/** Creates the actual sink. */
		public FileSink<IN, BucketID> build() {
			return new FileSink<>(this);
		}

		@Override
		public FileWriter<IN, BucketID> createWriter(InitContext context) throws IOException {
			return new FileWriter<>(
					basePath,
					bucketAssigner,
					bucketFactory,
					createBucketWriter(),
					rollingPolicy,
					outputFileConfig);
		}

		@Override
		public FileCommitter createCommitter() throws IOException {
			return new FileCommitter(createBucketWriter());
		}

		@Override
		public SimpleVersionedSerializer<FileWriterBucketState<BucketID>> getWriterStateSerializer() throws IOException {
			return new FileWriterBucketStateSerializer<>(
					createBucketWriter()
							.getProperties()
							.getInProgressFileRecoverableSerializer(),
					bucketAssigner.getSerializer());
		}

		@Override
		public SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer() throws IOException {
			BucketWriter<IN, BucketID> bucketWriter = createBucketWriter();

			return new FileSinkCommittableSerializer(
					bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
					bucketWriter.getProperties().getInProgressFileRecoverableSerializer());
		}

		private BucketWriter<IN, BucketID> createBucketWriter() throws IOException {
			return new BulkBucketWriter<>(
					FileSystem.get(basePath.toUri()).createRecoverableWriter(),
					writerFactory);
		}
	}

	/**
	 * Builder for the vanilla {@link FileSink} using a bulk format.
	 *
	 * @param <IN> record type
	 */
	public static final class DefaultBulkFormatBuilder<IN> extends BulkFormatBuilder<IN, String, DefaultBulkFormatBuilder<IN>> {

		private static final long serialVersionUID = 7493169281036370228L;

		private DefaultBulkFormatBuilder(
				Path basePath,
				BulkWriter.Factory<IN> writerFactory,
				BucketAssigner<IN, String> assigner) {
			super(basePath, writerFactory, assigner);
		}
	}
}
