package com.github.netudima.cassandra.loadgen;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Partition keys: {@code "test"} plus a zero-padded id, rendered into a buffer
 * the calling thread owns. No String is created, and nothing is allocated per
 * request - {@code BoundStatementBuilder.set} encodes eagerly, so the driver
 * never retains the array.
 *
 * <p>Sequential order is per shard: each shard walks its own contiguous slice of
 * the key space, so there is no shared counter on the request path.
 */
public final class Keys {

    private static final char[] PREFIX = "test".toCharArray();

    private final long partitions;
    private final int digits;
    private final boolean sequential;

    public Keys(long partitions, boolean sequential) {
        this.partitions = partitions;
        this.digits = Math.max(1, String.valueOf(Math.max(1, partitions - 1)).length());
        this.sequential = sequential;
    }

    public int keyLength() {
        return PREFIX.length + digits;
    }

    public boolean isSequential() {
        return sequential;
    }

    /** One per shard, never shared. */
    public Cursor newCursor(int threadIndex, int threadCount) {
        long slice = Math.max(1, partitions / Math.max(1, threadCount));
        return new Cursor(threadIndex * slice, slice);
    }

    public final class Cursor {
        private final long start;
        private final long slice;
        private long offset;

        private Cursor(long start, long slice) {
            this.start = start;
            this.slice = slice;
        }

        /** Render the next key into {@code buf}, which must be {@link #keyLength()} long. */
        public void render(char[] buf, ThreadLocalRandom rnd) {
            long id = sequential
                    ? (start + (offset++ % slice)) % partitions
                    : rnd.nextLong(partitions);
            System.arraycopy(PREFIX, 0, buf, 0, PREFIX.length);
            for (int i = buf.length - 1; i >= PREFIX.length; i--) {
                buf[i] = (char) ('0' + (int) (id % 10));
                id /= 10;
            }
        }
    }

    /** Clustering keys: the row's index within its partition, zero padded. */
    public static void renderClustering(char[] buf, int row) {
        int v = row;
        for (int i = buf.length - 1; i >= 0; i--) {
            buf[i] = (char) ('0' + (v % 10));
            v /= 10;
        }
    }
}
