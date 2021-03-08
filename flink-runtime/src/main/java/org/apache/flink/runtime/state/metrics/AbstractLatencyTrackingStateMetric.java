/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricGroup;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Snapshot;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Abstract class of latency tracking state metric which counts and histogram the state metric. */
class AbstractLatencyTrackingStateMetric implements AutoCloseable {
    protected static final String STATE_CLEAR_LATENCY = "stateClearLatency";
    private final MetricGroup metricGroup;
    private final int sampleInterval;
    private final Map<String, Histogram> histogramMetrics;
    private final Map<String, Counter> countersPerMetric;
    private final Supplier<com.codahale.metrics.Histogram> histogramSupplier;

    AbstractLatencyTrackingStateMetric(
            String stateName, MetricGroup metricGroup, int sampleInterval, long slidingWindow) {
        this.metricGroup = metricGroup.addGroup(stateName);
        this.sampleInterval = sampleInterval;
        this.histogramMetrics = new HashMap<>();
        this.countersPerMetric = new HashMap<>();
        this.histogramSupplier =
                () ->
                        new com.codahale.metrics.Histogram(
                                new SlidingTimeWindowReservoir(slidingWindow, TimeUnit.SECONDS));
    }

    protected boolean checkCounter(final String metricName) {
        return countersPerMetric
                .computeIfAbsent(metricName, (k) -> new Counter(sampleInterval))
                .checkAndUpdateCounter();
    }

    protected boolean checkClearCounter() {
        return checkCounter(STATE_CLEAR_LATENCY);
    }

    protected void updateHistogram(final String metricName, final long durationNanoTime) {
        this.histogramMetrics
                .computeIfAbsent(
                        metricName,
                        (k) -> {
                            HistogramWrapper histogram =
                                    new HistogramWrapper(histogramSupplier.get());
                            metricGroup.histogram(metricName, histogram);
                            return histogram;
                        })
                .update(durationNanoTime);
    }

    protected void updateClearLatency(long duration) {
        updateHistogram(STATE_CLEAR_LATENCY, duration);
    }

    @VisibleForTesting
    Map<String, Counter> getCountersPerMetric() {
        return countersPerMetric;
    }

    @Override
    public void close() throws Exception {
        histogramMetrics.clear();
        countersPerMetric.clear();
    }

    static class HistogramWrapper implements Histogram {
        private final com.codahale.metrics.Histogram histogram;

        public HistogramWrapper(com.codahale.metrics.Histogram histogram) {
            this.histogram = histogram;
        }

        @Override
        public void update(long value) {
            histogram.update(value);
        }

        @Override
        public long getCount() {
            return histogram.getCount();
        }

        @Override
        public HistogramStatistics getStatistics() {
            return new SnapshotHistogramStatistics(this.histogram.getSnapshot());
        }
    }

    private static class SnapshotHistogramStatistics extends HistogramStatistics {

        private final Snapshot snapshot;

        SnapshotHistogramStatistics(com.codahale.metrics.Snapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public double getQuantile(double quantile) {
            return snapshot.getValue(quantile);
        }

        @Override
        public long[] getValues() {
            return snapshot.getValues();
        }

        @Override
        public int size() {
            return snapshot.size();
        }

        @Override
        public double getMean() {
            return snapshot.getMean();
        }

        @Override
        public double getStdDev() {
            return snapshot.getStdDev();
        }

        @Override
        public long getMax() {
            return snapshot.getMax();
        }

        @Override
        public long getMin() {
            return snapshot.getMin();
        }
    }

    protected static class Counter {
        private final int metricSampledInterval;
        private int counter;

        Counter(int metricSampledInterval) {
            this.metricSampledInterval = metricSampledInterval;
            this.counter = 0;
        }

        private int updateMetricsSampledCounter(int counter) {
            return (counter + 1 < metricSampledInterval) ? counter + 1 : 0;
        }

        boolean checkAndUpdateCounter() {
            boolean result = counter == 0;
            this.counter = updateMetricsSampledCounter(counter);
            return result;
        }

        @VisibleForTesting
        int getCounter() {
            return counter;
        }
    }
}
