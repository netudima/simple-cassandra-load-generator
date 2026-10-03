package com.github.netudima.cassandra.loadgen;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

/**
 * What a request writes. Each scenario uses the same schema as its
 * cassandra-stress or easy-stress counterpart, so the numbers line up:
 *
 * <pre>
 *   batch10      stress.stress_table           10-row UNLOGGED batch per
 *                                              operation, 5 value cells of 10
 *                                              chars per row
 *   singlerow10  stress.stress_table           the same rows in the same
 *                                              partitions, one row per request
 *
 *   kv1x150   keyvalue_stress.keyvalue         one row, one 150-byte value -
 *                                              easy-stress KeyValue
 *   kv5x30    keyvalue5_stress.keyvalue5x30    one row, five 30-byte values
 *   kv5x60    keyvalue5_stress.keyvalue5       one row, five 60-byte values
 * </pre>
 *
 * <p>kv1x150 and kv5x30 both carry 150 bytes per row, split across a different
 * number of cells. kv5x60 doubles the bytes at the same cell count.
 */
public interface Workload {

    String name();

    String keyspace();

    String table();

    String createTableCql();

    String insertCql();

    /** Rows written per operation. A batch writes more than one. */
    int rowsPerOp();

    /**
     * One binder per shard, owning that shard's reusable buffers. The index and
     * count are passed in so each shard's slice of the key space follows from its
     * identity, with no counter shared between shards.
     */
    Binder newBinder(PreparedStatement prepared, int shardIndex, int shardCount);

    /** Builds the next statement. Called only from the thread that created it. */
    interface Binder {
        Statement<?> next(ThreadLocalRandom rnd);
    }

    static Workload of(Config cfg) {
        String n = cfg.workload.toLowerCase(Locale.ROOT);
        return switch (n) {
            case "batch10" -> new BatchWorkload(cfg, true);
            case "singlerow10" -> new BatchWorkload(cfg, false);
            case "kv1x150" -> new KeyValueWorkload(cfg, "kv1x150", "keyvalue_stress", "keyvalue", 1, 150);
            case "kv5x30"  -> new KeyValueWorkload(cfg, "kv5x30", "keyvalue5_stress", "keyvalue5x30", 5, 30);
            case "kv5x60"  -> new KeyValueWorkload(cfg, "kv5x60", "keyvalue5_stress", "keyvalue5", 5, 60);
            default -> throw new IllegalArgumentException(
                    "unknown --workload '" + cfg.workload
                            + "', expected one of: batch10, singlerow10, kv1x150, kv5x30, kv5x60");
        };
    }
}
