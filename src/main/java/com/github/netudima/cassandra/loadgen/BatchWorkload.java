package com.github.netudima.cassandra.loadgen;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

import java.util.concurrent.ThreadLocalRandom;

/**
 * The batch table and its distribution - {@code rowsPerPartition} clustering rows
 * per partition, five 10-character values per row - written either as one
 * UNLOGGED batch per operation (batch10) or one
 * row per operation (singlerow10).
 *
 * <p>Both write the same rows to the same partitions and differ only in rows per
 * request. In single-row mode the clustering index walks 0..n-1 and the partition
 * advances when it wraps, so n consecutive requests fill a partition instead of
 * one batch. The data ends up identical.
 */
public final class BatchWorkload implements Workload {

    private final String keyspace;
    private final String table;
    private final int rowsPerPartition;
    private final int rowsPerOperation;
    private final boolean batched;
    private final int valuesPerRow;
    private final int valueSize;
    private final int clusteringSize;
    private final ValuePool values;
    private final Keys keys;

    public BatchWorkload(Config cfg, boolean batched) {
        this.keyspace = cfg.keyspace != null ? cfg.keyspace : "stress";
        this.table = cfg.table != null ? cfg.table : "stress_table";
        this.rowsPerPartition = cfg.batchRows;
        this.batched = batched;
        this.rowsPerOperation = batched ? cfg.batchRows : 1;
        this.valuesPerRow = cfg.cells > 0 ? cfg.cells : 5;
        this.valueSize = cfg.cellSize > 0 ? cfg.cellSize : 10;
        this.clusteringSize = 10;
        this.keys = new Keys(cfg.partitions, cfg.sequentialKeys(true));
        this.values = new ValuePool(cfg.valuePool, this.valueSize);
    }

    @Override public String name() { return batched ? "batch" + rowsPerPartition : "singlerow" + rowsPerPartition; }
    @Override public String keyspace() { return keyspace; }
    @Override public String table() { return table; }
    @Override public int rowsPerOp() { return rowsPerOperation; }

    public boolean isBatched() { return batched; }
    public int rowsPerPartition() { return rowsPerPartition; }
    public int valuesPerRow() { return valuesPerRow; }
    public int valueSize() { return valueSize; }
    public Keys keys() { return keys; }
    public ValuePool values() { return values; }

    @Override
    public String createTableCql() {
        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(keyspace).append('.').append(table)
                .append(" (part_key text, clust_key text");
        for (int i = 1; i <= valuesPerRow; i++) {
            ddl.append(", value").append(i).append(" text");
        }
        ddl.append(", PRIMARY KEY ((part_key), clust_key))")
           .append(" WITH CLUSTERING ORDER BY (clust_key DESC)");
        return ddl.toString();
    }

    @Override
    public String insertCql() {
        StringBuilder cols = new StringBuilder("part_key, clust_key");
        StringBuilder marks = new StringBuilder("?, ?");
        for (int i = 1; i <= valuesPerRow; i++) {
            cols.append(", value").append(i);
            marks.append(", ?");
        }
        return "INSERT INTO " + keyspace + "." + table + " (" + cols + ") VALUES (" + marks + ")";
    }

    @Override
    public Binder newBinder(PreparedStatement prepared, int shardIndex, int shardCount) {
        return new BatchBinder(prepared, shardIndex, shardCount);
    }

    private final class BatchBinder implements Binder {
        private final PreparedStatement prepared;
        private final char[] partBuf = new char[keys.keyLength()];
        private final char[] clustBuf = new char[clusteringSize];
        private final Keys.Cursor cursor;
        /** Starts "past the end" so the first request opens a fresh partition. */
        private int rowInPartition = Integer.MAX_VALUE;

        private BatchBinder(PreparedStatement prepared, int shardIndex, int shardCount) {
            this.prepared = prepared;
            this.cursor = keys.newCursor(shardIndex, shardCount);
        }

        @Override
        public Statement<?> next(ThreadLocalRandom rnd) {
            if (!batched) {
                // One row per request; the partition advances once its rows are done.
                if (rowInPartition >= rowsPerPartition) {
                    cursor.render(partBuf, rnd);
                    rowInPartition = 0;
                }
                return bindRow(rowInPartition++, rnd).build();
            }
            cursor.render(partBuf, rnd);
            BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
            for (int row = 0; row < rowsPerOperation; row++) {
                batch.addStatement(bindRow(row, rnd).build());
            }
            return batch.build();
        }

        private BoundStatementBuilder bindRow(int row, ThreadLocalRandom rnd) {
            Keys.renderClustering(clustBuf, row);
            BoundStatementBuilder b = prepared.boundStatementBuilder();
            b = b.set(0, partBuf, CharArrayCodec.INSTANCE);
            b = b.set(1, clustBuf, CharArrayCodec.INSTANCE);
            for (int v = 0; v < valuesPerRow; v++) {
                b = b.setBytesUnsafe(2 + v, values.value(rnd));
            }
            return b;
        }
    }
}
