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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * WatermarkStrategies is a simply way to build a {@link WatermarkStrategy} by configuring
 * common strategies.
 */
@PublicEvolving
public final class WatermarkStrategies<T> {

	/**
	 * The {@link TimestampAssigner} to use. This can be {@code null} for cases where records come
	 * out of a source with valid timestamps, for example from Kafka.
	 */
	@Nullable
	private TimestampAssignerSupplier<T> timestampAssignerSupplier = null;

	/** The base strategy for watermark generation. Starting point, is always set. */
	private final WatermarkStrategy<T> baseStrategy;

	/** Optional idle timeout for watermarks. */
	@Nullable
	private Duration idleTimeout;

	private WatermarkStrategies(WatermarkStrategy<T> baseStrategy) {
		this.baseStrategy = baseStrategy;
	}

	// ------------------------------------------------------------------------
	//  builder methods
	// ------------------------------------------------------------------------

	/**
	 * Add an idle timeout to the watermark strategy.
	 * If no records flow in a partition of a stream for that amount of time, then that partition
	 * is considered "idle" and will not hold back the progress of watermarks in downstream operators.
	 *
	 * <p>Idleness can be important if some partitions have little data and might not have events during
	 * some periods. Without idleness, these streams can stall the overall event time progress of the
	 * application.
	 */
	public WatermarkStrategies<T> withIdleness(Duration idleTimeout) {
		checkNotNull(idleTimeout, "idleTimeout");
		checkArgument(!(idleTimeout.isZero() || idleTimeout.isNegative()), "idleTimeout must be greater than zero");
		this.idleTimeout = idleTimeout;
		return this;
	}

	/**
	 * Adds the given {@link TimestampAssigner} (via a {@link TimestampAssignerSupplier}) to this
	 * {@link WatermarkStrategies}.
	 *
	 * <p>You can use this when a {@link TimestampAssigner} needs additional context, for example
	 * access to the metrics system.
	 *
	 * <pre>
	 * {@code WatermarkStrategy<Object> wmStrategy = WatermarkStrategies
	 *   .forMonotonousTimestamps()
	 *   .withTimestampAssigner((ctx) -> new MetricsReportingAssigner(ctx))
	 *   .build();
	 * }</pre>
	 */
	public WatermarkStrategies<T> withTimestampAssigner(TimestampAssignerSupplier<T> timestampAssigner) {
		checkNotNull(timestampAssigner, "timestampAssigner");
		this.timestampAssignerSupplier = timestampAssigner;
		return this;
	}

	/**
	 * Adds the given {@link TimestampAssigner} to this {@link WatermarkStrategies}.
	 *
	 * <p>You can use this in case you want to specify a {@link TimestampAssigner} via a lambda
	 * function.
	 *
	 * <pre>
	 * {@code WatermarkStrategy<CustomObject> wmStrategy = WatermarkStrategies
	 *   .<CustomObject>forMonotonousTimestamps()
	 *   .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
	 *   .build();
	 * }</pre>
	 */
	public WatermarkStrategies<T> withTimestampAssigner(SerializableTimestampAssigner<T> timestampAssigner) {
		checkNotNull(timestampAssigner, "timestampAssigner");
		this.timestampAssignerSupplier = TimestampAssignerSupplier.of(timestampAssigner);
		return this;
	}

	/**
	 * Build the watermark strategy.
	 */
	public WatermarkStrategy<T> build() {
		WatermarkStrategy<T> strategy = this.baseStrategy;

		if (idleTimeout != null) {
			strategy = new WithIdlenessStrategy<>(strategy, idleTimeout);
		}

		if (timestampAssignerSupplier != null) {
			strategy = new WithTimestampAssigner<>(strategy, timestampAssignerSupplier);
		}

		return strategy;
	}

	// ------------------------------------------------------------------------
	//  builder entry points
	// ------------------------------------------------------------------------

	/**
	 * Starts building a watermark strategy for situations with monotonously ascending
	 * timestamps.
	 *
	 * <p>The watermarks are generated periodically and tightly follow the latest
	 * timestamp in the data. The delay introduced by this strategy is mainly the periodic
	 * interval in which the watermarks are generated.
	 *
	 * @see AscendingTimestampsWatermarks
	 */
	public static <T> WatermarkStrategies<T> forMonotonousTimestamps() {
		return new WatermarkStrategies<T>(AscendingTimestampsWatermarks::new);
	}

	/**
	 * Starts building a watermark strategy for situations where records are out of order, but
	 * you can place an upper bound on how far the events are out of order.
	 * An out-of-order bound B means that once the an event with timestamp T was encountered, no
	 * events older than {@code T - B} will follow any more.
	 *
	 * <p>The watermarks are generated periodically. The delay introduced by this watermark strategy
	 * is the periodic interval length, plus the out of orderness bound.
	 *
	 * @see BoundedOutOfOrdernessWatermarks
	 */
	public static <T> WatermarkStrategies<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) {
		return new WatermarkStrategies<>(() -> new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness));
	}

	/**
	 * Starts building a watermark strategy based on an existing {@code WatermarkStrategy}.
	 */
	public static <T> WatermarkStrategies<T> forStrategy(WatermarkStrategy<T> strategy) {
		return new WatermarkStrategies<>(strategy);
	}

	/**
	 * Starts building a watermark strategy based on an existing {@code WatermarkGenerator}.
	 */
	public static <T, X extends WatermarkGenerator<T> & Serializable> WatermarkStrategies<T> forGenerator(X generator) {
		return new WatermarkStrategies<>(new FromSerializedGeneratorStrategy<>(generator));
	}

	// ------------------------------------------------------------------------

	private static final class FromSerializedGeneratorStrategy<T> implements WatermarkStrategy<T> {
		private static final long serialVersionUID = 1L;

		private final WatermarkGenerator<T> generator;

		private FromSerializedGeneratorStrategy(WatermarkGenerator<T> generator) {
			this.generator = generator;
		}

		@Override
		public WatermarkGenerator<T> createWatermarkGenerator() {
			try {
				byte[] serialized = InstantiationUtil.serializeObject(generator);
				return InstantiationUtil.deserializeObject(serialized, generator.getClass().getClassLoader());
			}
			catch (Exception e) {
				throw new FlinkRuntimeException("Cannot clone watermark generator via serialization");
			}
		}
	}

	/**
	 * A {@link WatermarkStrategy} that overrides the {@link TimestampAssigner} of the given base
	 * {@link WatermarkStrategy}.
	 */
	private static final class WithTimestampAssigner<T> implements WatermarkStrategy<T> {
		private static final long serialVersionUID = 1L;

		private final WatermarkStrategy<T> baseStrategy;
		private final TimestampAssignerSupplier<T> timestampAssigner;

		private WithTimestampAssigner(WatermarkStrategy<T> baseStrategy, TimestampAssignerSupplier<T> timestampAssigner) {
			this.baseStrategy = baseStrategy;
			this.timestampAssigner = timestampAssigner;
		}

		@Override
		public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
			return timestampAssigner.createTimestampAssigner(context);
		}

		@Override
		public WatermarkGenerator<T> createWatermarkGenerator() {
			return baseStrategy.createWatermarkGenerator();
		}
	}

	private static final class WithIdlenessStrategy<T> implements WatermarkStrategy<T> {
		private static final long serialVersionUID = 1L;

		private final WatermarkStrategy<T> baseStrategy;
		private final Duration idlenessTimeout;

		private WithIdlenessStrategy(WatermarkStrategy<T> baseStrategy, Duration idlenessTimeout) {
			this.baseStrategy = baseStrategy;
			this.idlenessTimeout = idlenessTimeout;
		}

		@Override
		public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
			return baseStrategy.createTimestampAssigner(context);
		}

		@Override
		public WatermarkGenerator<T> createWatermarkGenerator() {
			return new WatermarksWithIdleness<>(baseStrategy.createWatermarkGenerator(), idlenessTimeout);
		}
	}
}
