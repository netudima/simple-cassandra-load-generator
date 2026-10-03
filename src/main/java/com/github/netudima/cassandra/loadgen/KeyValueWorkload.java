package com.github.netudima.cassandra.loadgen;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Single-row partitions: {@code key text PRIMARY KEY} plus N text cells, one
 * prepared INSERT per operation. Covers kv1x150, kv5x30 and kv5x60 - they differ
 * only in cell count and cell size.
 *
 * <p>Keys default to random order, matching easy-stress's KeyValue generator.
 */
public final class KeyValueWorkload implements Workload {

    private final String name;
    private final String keyspace;
    private final String table;
    private final int cells;
    private final int cellSize;
    private final ValuePool values;
    private final Keys keys;

    public KeyValueWorkload(Config cfg, String name, String defaultKeyspace, String defaultTable,
                            int defaultCells, int defaultCellSize) {
        this.name = name;
        this.keyspace = cfg.keyspace != null ? cfg.keyspace : defaultKeyspace;
        this.table = cfg.table != null ? cfg.table : defaultTable;
        this.cells = cfg.cells > 0 ? cfg.cells : defaultCells;
        this.cellSize = cfg.cellSize > 0 ? cfg.cellSize : defaultCellSize;
        this.keys = new Keys(cfg.partitions, cfg.sequentialKeys(false));
        this.values = new ValuePool(cfg.valuePool, this.cellSize);
    }

    @Override public String name() { return name; }
    @Override public String keyspace() { return keyspace; }
    @Override public String table() { return table; }
    @Override public int rowsPerOp() { return 1; }

    public int cells() { return cells; }
    public int cellSize() { return cellSize; }
    public Keys keys() { return keys; }
    public ValuePool values() { return values; }

    private String columnName(int i) {
        return cells == 1 ? "value" : "value" + i;
    }

    @Override
    public String createTableCql() {
        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(keyspace).append('.').append(table).append(" (key text PRIMARY KEY");
        for (int i = 1; i <= cells; i++) {
            ddl.append(", ").append(columnName(i)).append(" text");
        }
        return ddl.append(')').toString();
    }

    @Override
    public String insertCql() {
        StringBuilder cols = new StringBuilder("key");
        StringBuilder marks = new StringBuilder("?");
        for (int i = 1; i <= cells; i++) {
            cols.append(", ").append(columnName(i));
            marks.append(", ?");
        }
        return "INSERT INTO " + keyspace + "." + table + " (" + cols + ") VALUES (" + marks + ")";
    }

    @Override
    public Binder newBinder(PreparedStatement prepared, int shardIndex, int shardCount) {
        return new KvBinder(prepared, shardIndex, shardCount);
    }

    private final class KvBinder implements Binder {
        private final PreparedStatement prepared;
        private final char[] keyBuf = new char[keys.keyLength()];
        private final Keys.Cursor cursor;

        private KvBinder(PreparedStatement prepared, int shardIndex, int shardCount) {
            this.prepared = prepared;
            this.cursor = keys.newCursor(shardIndex, shardCount);
        }

        @Override
        public Statement<?> next(ThreadLocalRandom rnd) {
            cursor.render(keyBuf, rnd);
            BoundStatementBuilder b = prepared.boundStatementBuilder();
            // Key through the codec - it changes every request, so it must be encoded.
            b = b.set(0, keyBuf, CharArrayCodec.INSTANCE);
            // Values are already in wire form.
            for (int c = 1; c <= cells; c++) {
                b = b.setBytesUnsafe(c, values.value(rnd));
            }
            return b.build();
        }
    }
}
