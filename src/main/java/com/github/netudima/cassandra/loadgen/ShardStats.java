package com.github.netudima.cassandra.loadgen;

import org.HdrHistogram.Recorder;

/**
 * Latency and counters for one shard. Only the shard's event loop writes here,
 * so the counters need no atomics; the collector thread only reads them.
 *
 * <p>The single writer does not come from setting one netty thread per shard.
 * Completions can arrive on a sibling event loop, and timeouts arrive on the
 * driver's admin thread, so {@code LoadGenerator} moves every completion onto
 * the shard's own loop before it reaches this class.
 *
 * <p>{@link Recorder} handles the one real handoff: the collector swaps the
 * active histogram out while the loop keeps recording into the new one.
 */
public final class ShardStats {

    /** Microseconds. */
    private final Recorder latency = new Recorder(3);

    private volatile long completed;
    private volatile long errors;

    private volatile String firstError;

    // ---------------------------------------------------------- shard loop only
    public void recordLatency(long micros) {
        latency.recordValue(micros);
    }

    public void recordCompleted() {
        completed++;
    }

    public void recordError(Throwable t) {
        errors++;
        if (firstError == null) {
            firstError = t.toString();
        }
    }

    // ------------------------------------------------------------ collector side
    Recorder latencyRecorder() {
        return latency;
    }

    long completed() {
        return completed;
    }

    long errors() {
        return errors;
    }

    String firstError() {
        return firstError;
    }

    /** Call only between phases, with the shard loop stopped. */
    void reset() {
        latency.reset();
        completed = 0;
        errors = 0;
        firstError = null;
    }
}
