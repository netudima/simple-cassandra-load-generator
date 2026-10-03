package com.github.netudima.cassandra.loadgen;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Measurement and reporting, in cassandra-stress's output format: the same
 * interval lines and a {@code Results:} block terminated by {@code END}.
 *
 * <p>cassandra-stress also prints GC columns, which it reads from the server over
 * JMX. This tool has no JMX connection and only ever saw its own heap, so those
 * columns are left out instead of being filled with client numbers under the same
 * headings. Server GC belongs in the node's gc.log.
 */
public final class Stats {

    private static final String HEADER =
            "type                                               total ops,    op/s,    pk/s,   row/s,"
          + "    mean,     med,     .95,     .99,    .999,     max,   time,   stderr, errors";

    // Nothing on the request path writes to this object; recording is per shard
    // (see ShardStats). The collector thread samples every shard once a second.
    private final List<ShardStats> shards = new CopyOnWriteArrayList<>();
    // Recycled interval histograms, one slot per shard. A slot starts null: Recorder
    // only accepts a replacement it handed out itself, so the first sample must call
    // getIntervalHistogram() with no argument and later ones pass back its result.
    private final List<Histogram> reuseLatency = new CopyOnWriteArrayList<>();

    private final Histogram latencyTotal = new Histogram(3);

    private final List<Double> intervalRates = new ArrayList<>();
    private volatile boolean recording = false;
    private volatile boolean running = false;
    private Thread progressThread;

    // -------------------------------------------------------------- recording
    /** One per shard, handed to the loop that will own it. */
    public ShardStats newShard() {
        ShardStats st = new ShardStats();
        shards.add(st);
        reuseLatency.add(null);
        return st;
    }

    public boolean isRecording() {
        return recording;
    }

    private long sumCompleted() {
        long n = 0;
        for (ShardStats st : shards) {
            n += st.completed();
        }
        return n;
    }

    private long sumErrors() {
        long n = 0;
        for (ShardStats st : shards) {
            n += st.errors();
        }
        return n;
    }

    /**
     * Drain one interval from every shard into {@code into}, reusing a histogram per
     * shard so a per-second sample allocates nothing.
     *
     * <p>getIntervalHistogram() CONSUMES what the recorder holds, so the caller has
     * to fold the result into a running total; otherwise the final percentiles cover
     * only the last interval.
     */
    private void drain(Histogram into) {
        into.reset();
        for (int i = 0; i < shards.size(); i++) {
            Recorder r = shards.get(i).latencyRecorder();
            Histogram prev = reuseLatency.get(i);
            Histogram h = prev == null ? r.getIntervalHistogram() : r.getIntervalHistogram(prev);
            reuseLatency.set(i, h);
            into.add(h);
        }
    }

    public long completedCount() {
        return sumCompleted();
    }

    /** Clears everything, so a warmup phase leaves no trace in the reported run. */
    public void reset() {
        for (ShardStats st : shards) {
            st.reset();
        }
        // The recorders were reset underneath them, so the recycled instances are no
        // longer the ones those recorders handed out.
        for (int i = 0; i < reuseLatency.size(); i++) {
            reuseLatency.set(i, null);
        }
        synchronized (latencyTotal) {
            latencyTotal.reset();
        }
        intervalRates.clear();
    }

    // ------------------------------------------------------------- progress
    private int rowsPerOp = 1;

    public void startProgress(double intervalSec, long startNanos, int rowsPerOp) {
        this.rowsPerOp = rowsPerOp;
        recording = true;
        running = true;
        System.out.println(HEADER);
        progressThread = new Thread(() -> progressLoop(intervalSec, startNanos), "progress");
        progressThread.setDaemon(true);
        progressThread.start();
    }

    public void stopProgress() throws InterruptedException {
        running = false;
        if (progressThread != null) {
            progressThread.interrupt();
            progressThread.join();
            progressThread = null;
        }
    }

    public void setRecording(boolean on) {
        recording = on;
    }

    private void progressLoop(double intervalSec, long startNanos) {
        long lastOps = 0;
        try {
            while (running) {
                Thread.sleep((long) (intervalSec * 1000));
                Histogram h = new Histogram(3);
                drain(h);
                synchronized (latencyTotal) {
                    latencyTotal.add(h);
                }

                long ops = sumCompleted() - sumErrors();
                double elapsed = (System.nanoTime() - startNanos) / 1e9;
                double rateNow = (ops - lastOps) / intervalSec;
                lastOps = ops;
                intervalRates.add(rateNow);

                System.out.printf(Locale.ROOT,
                        "%-50s%10d,%8d,%8d,%8d,%8.1f,%8.1f,%8.1f,%8.1f,%8.1f,%8.1f,%7.1f,%9.5f,"
                        + "%7d%n",
                        "total,", ops, (long) rateNow, (long) rateNow, (long) (rateNow * rowsPerOp),
                        ms(h.getMean()), ms(h.getValueAtPercentile(50)), ms(h.getValueAtPercentile(95)),
                        ms(h.getValueAtPercentile(99)), ms(h.getValueAtPercentile(99.9)),
                        ms(h.getMaxValue()), elapsed, relStdErr(intervalRates), sumErrors());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // --------------------------------------------------------------- results
    public void printResults(long elapsedMs, Config cfg, int rowsPerOp) {
        Histogram total = new Histogram(3);
        synchronized (latencyTotal) {
            total.add(latencyTotal);
            Histogram tail = new Histogram(3);
            drain(tail);
            total.add(tail);                                // tail since the last progress tick
        }

        long ops = sumCompleted() - sumErrors();
        long rateOps = elapsedMs == 0 ? 0 : (long) (ops / (elapsedMs / 1000.0));

        System.out.println();
        System.out.println("Offered load (generator side - NOT server latency):");
        System.out.printf(Locale.ROOT, "  requested rate          : closed loop (in-flight bound only)%n");
        System.out.printf(Locale.ROOT, "  achieved rate           : %s op/s%n", Config.fmt(rateOps));
        System.out.printf(Locale.ROOT, "  concurrency             : %s in flight over %d shards (%s each, independent)%n",
                Config.fmt(cfg.inFlight), cfg.shards, Config.fmt(cfg.inFlight / cfg.shards));
        System.out.println();

        System.out.println("Results:");
        kv("Op rate", Config.fmt(rateOps) + " op/s", "insert: " + Config.fmt(rateOps) + " op/s");
        kv("Partition rate", Config.fmt(rateOps) + " pk/s", "insert: " + Config.fmt(rateOps) + " pk/s");
        // A batch writes several rows per operation, so row rate is not op rate.
        long rateRows = rateOps * rowsPerOp;
        kv("Row rate", Config.fmt(rateRows) + " row/s", "insert: " + Config.fmt(rateRows) + " row/s");
        latencyLine("Latency mean", ms(total.getMean()));
        latencyLine("Latency median", ms(total.getValueAtPercentile(50)));
        latencyLine("Latency 95th percentile", ms(total.getValueAtPercentile(95)));
        latencyLine("Latency 99th percentile", ms(total.getValueAtPercentile(99)));
        latencyLine("Latency 99.9th percentile", ms(total.getValueAtPercentile(99.9)));
        latencyLine("Latency max", ms(total.getMaxValue()));
        kv("Total partitions", Config.fmt(ops), "insert: " + Config.fmt(ops));
        if (rowsPerOp > 1) {
            kv("Total rows", Config.fmt(ops * rowsPerOp), "insert: " + Config.fmt(ops * rowsPerOp));
        }
        kv("Total errors", Config.fmt(sumErrors()), "insert: " + Config.fmt(sumErrors()));
        System.out.printf(Locale.ROOT, "%-26s: %s%n", "Total operation time", hms(elapsedMs));
        System.out.println();
        System.out.println("END");

        List<String> firstErrors = new ArrayList<>();
        for (ShardStats st : shards) {
            if (st.firstError() != null && firstErrors.size() < 5) {
                firstErrors.add(st.firstError());
            }
        }
        if (!firstErrors.isEmpty()) {
            System.out.println();
            System.out.println("First errors:");
            firstErrors.forEach(e -> System.out.println("  " + e));
        }
    }

    public boolean hadErrors() {
        return sumErrors() > 0;
    }

    // ----------------------------------------------------------------- utils
    private static void kv(String label, String value, String detail) {
        System.out.printf(Locale.ROOT, "%-26s: %s [%s]%n", label, value, detail);
    }

    private static void latencyLine(String label, double msValue) {
        System.out.printf(Locale.ROOT, "%-26s: %6.1f ms [insert: %.1f ms]%n", label, msValue, msValue);
    }

    private static double ms(double micros) {
        return micros / 1000.0;
    }

    private static double ms(long micros) {
        return micros / 1000.0;
    }

    private static String hms(long millis) {
        long s = millis / 1000;
        return String.format(Locale.ROOT, "%02d:%02d:%02d", s / 3600, (s % 3600) / 60, s % 60);
    }

    /** Relative standard error of the interval rates - the column cassandra-stress prints. */
    private static double relStdErr(List<Double> rates) {
        if (rates.size() < 2) {
            return 0;
        }
        double mean = rates.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        if (mean == 0) {
            return 0;
        }
        double var = rates.stream().mapToDouble(r -> (r - mean) * (r - mean)).sum() / (rates.size() - 1);
        return Math.sqrt(var / rates.size()) / mean;
    }
}
