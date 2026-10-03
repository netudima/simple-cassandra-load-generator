package com.github.netudima.cassandra.loadgen;

import java.util.Locale;

/** Command line options. */
public final class Config {

    public String host;
    public int port = 9042;
    public String datacenter = "datacenter1";
    public String user = "cassandra";
    public String password = "cassandra";
    /** null = take the workload's own default, so each scenario lands on the
     *  same table its cassandra-stress counterpart used. */
    public String keyspace = null;
    public String table = null;
    public String workload = "kv5x60";
    /** 0 = workload default; set only to override a scenario's shape. */
    public int batchRows = 10;
    public String consistency = "LOCAL_QUORUM";

    public long ops = -1;
    public long durationSec = -1;
    public long warmupSec = 0;

    public int inFlight = 3840;
    // One driver session per shard, so one connection pool and one event loop each,
    // and that loop both issues and completes. --netty-io and --connections are
    // PER SHARD.
    public int shards = 5;
    public int nettyIo = 1;
    public int connections = 2;
    public int maxRequestsPerConnection = 8192;
    public int requestTimeoutSec = 30;
    public String transport = "epoll";

    public long partitions = 10_000_000L;
    public int cells = 0;      // 0 = workload default
    public int cellSize = 0;   // 0 = workload default
    /** null = workload default (sequential for batch10, random for the kv scenarios). */
    public Boolean keyOrderSequential = null;
    public int valuePool = 128 * 1024;

    public double progressSec = 1.0;

    public boolean createSchema = false;
    public boolean truncate = false;
    public int rf = 1;

    public static Config parse(String[] args) {
        Config c = new Config();
        for (int i = 0; i < args.length; i++) {
            String k = args[i];
            switch (k) {
                case "--host"        -> c.host = value(args, ++i, k);
                case "--port"        -> c.port = Integer.parseInt(value(args, ++i, k));
                case "--datacenter"  -> c.datacenter = value(args, ++i, k);
                case "--user"        -> c.user = value(args, ++i, k);
                case "--password"    -> c.password = value(args, ++i, k);
                case "--workload"    -> c.workload = value(args, ++i, k);
                case "--batch-rows"  -> c.batchRows = Integer.parseInt(value(args, ++i, k));
                case "--key-order"   -> {
                    String v = value(args, ++i, k).toLowerCase(Locale.ROOT);
                    if (!v.equals("sequential") && !v.equals("random")) {
                        usage("--key-order must be sequential or random, got: " + v);
                    }
                    c.keyOrderSequential = v.equals("sequential");
                }
                case "--keyspace"    -> c.keyspace = value(args, ++i, k);
                case "--table"       -> c.table = value(args, ++i, k);
                case "--cl"          -> c.consistency = value(args, ++i, k);
                case "--ops"         -> c.ops = number(value(args, ++i, k));
                case "--duration"    -> c.durationSec = number(value(args, ++i, k));
                case "--warmup"      -> c.warmupSec = number(value(args, ++i, k));
                case "--inflight"    -> c.inFlight = (int) number(value(args, ++i, k));
                case "--shards"      -> c.shards = Integer.parseInt(value(args, ++i, k));
                case "--netty-io"    -> c.nettyIo = Integer.parseInt(value(args, ++i, k));
                case "--connections" -> c.connections = Integer.parseInt(value(args, ++i, k));
                case "--max-requests-per-connection" ->
                        c.maxRequestsPerConnection = Integer.parseInt(value(args, ++i, k));
                case "--request-timeout" -> c.requestTimeoutSec = Integer.parseInt(value(args, ++i, k));
                case "--transport"   -> c.transport = value(args, ++i, k).toLowerCase(Locale.ROOT);
                case "--partitions"  -> c.partitions = number(value(args, ++i, k));
                case "--cells"       -> c.cells = Integer.parseInt(value(args, ++i, k));
                case "--cell-size"   -> c.cellSize = Integer.parseInt(value(args, ++i, k));
                case "--value-pool"  -> c.valuePool = (int) number(value(args, ++i, k));
                case "--progress"    -> c.progressSec = Double.parseDouble(value(args, ++i, k));
                case "--create-schema" -> c.createSchema = true;
                case "--truncate"    -> c.truncate = true;
                case "--rf"          -> c.rf = Integer.parseInt(value(args, ++i, k));
                case "-h", "--help"  -> usage(null);
                default -> usage("unknown option: " + k);
            }
        }
        if (c.host == null) {
            usage("--host is required");
        }
        if (c.ops < 0 && c.durationSec < 0) {
            usage("one of --ops or --duration is required");
        }
        if (c.shards < 1) {
            usage("--shards must be >= 1");
        }
        if (c.inFlight < c.shards) {
            // The window is split across shards; a shard given a zero-sized window
            // would never issue anything and the phase would never finish.
            usage("--inflight (" + c.inFlight + ") must be >= --shards (" + c.shards + ")");
        }
        if (c.cells < 0) {
            usage("--cells must be >= 1");
        }
        if (!c.transport.equals("epoll") && !c.transport.equals("nio")) {
            usage("--transport must be epoll or nio, got: " + c.transport);
        }
        return c;
    }

    private static String value(String[] a, int i, String k) {
        if (i >= a.length) {
            usage(k + " requires a value");
        }
        return a[i];
    }

    /** Accepts the suffixes the stress tools use: 15m, 500k, 2b. */
    static long number(String raw) {
        String s = raw.trim().toLowerCase(Locale.ROOT);
        long mult = 1;
        if (s.endsWith("k")) {
            mult = 1_000L;
            s = s.substring(0, s.length() - 1);
        } else if (s.endsWith("m")) {
            mult = 1_000_000L;
            s = s.substring(0, s.length() - 1);
        } else if (s.endsWith("b")) {
            mult = 1_000_000_000L;
            s = s.substring(0, s.length() - 1);
        }
        return Long.parseLong(s) * mult;
    }

    /** Whether keys walk in order, given the workload's default. */
    public boolean sequentialKeys(boolean workloadDefault) {
        return keyOrderSequential != null ? keyOrderSequential : workloadDefault;
    }

    public void print(int keyLength, Workload workload, String dataDescription) {
        System.out.println("simple-cassandra-load-generator settings:");
        System.out.printf("  node            : %s:%d (dc %s)%n", host, port, datacenter);
        System.out.printf("  workload        : %s (%d row%s per operation)%n",
                workload.name(), workload.rowsPerOp(), workload.rowsPerOp() == 1 ? "" : "s");
        System.out.printf("  target          : %s.%s, cl=%s%n",
                workload.keyspace(), workload.table(), consistency);
        System.out.printf("  insert          : %s%n", workload.insertCql());
        System.out.printf("  data            : %s%n", dataDescription);
        System.out.printf("  keys            : %d chars over %s partitions%n", keyLength, fmt(partitions));
        System.out.printf("  values          : %s pre-encoded per cell (bound as bytes, zero alloc)%n",
                fmt(valuePool));
        System.out.printf("  rate            : closed loop - as fast as %s in flight allows%n",
                fmt(inFlight));
        System.out.printf("  bound           : %s%n", ops > 0 ? fmt(ops) + " ops"
                : durationSec > 0 ? durationSec + "s" : "none");
        System.out.printf("  sharding        : %d shard%s, each 1 session / %d netty io / %d connection%s%n",
                shards, shards == 1 ? "" : "s", nettyIo, connections, connections == 1 ? "" : "s");
        System.out.printf("  concurrency     : %s in flight total, %s per shard%n",
                fmt(inFlight), fmt(inFlight / shards));
        System.out.printf("  transport       : %s%n", EpollTransport.describe(transport.equals("epoll")));
        System.out.printf("  jvm             : %s, %d processors%n",
                System.getProperty("java.version"), Runtime.getRuntime().availableProcessors());
        System.out.println();
    }

    static String fmt(long v) {
        return String.format(Locale.ROOT, "%,d", v);
    }

    public static void usage(String err) {
        if (err != null) {
            System.err.println("ERROR: " + err);
        }
        System.err.println("""
            Usage: run.sh --host H (--ops N | --duration S) [options]

            Writes single-row partitions with N text cells using async requests
            issued from the driver's own netty event loops, and reports in
            cassandra-stress's format. Closed loop: --inflight sets the offered
            concurrency and the achieved rate is the result.

            Workload (pick one; each uses the same schema as its cassandra-stress counterpart)
              --workload W         batch10 | singlerow10 | kv1x150 | kv5x30 | kv5x60
                                   (default kv5x60)
                                     batch10  stress.stress_table, 10-row UNLOGGED batch,
                                              5 values x 10 chars per row, sequential keys
                                     singlerow10
                                              same table, same rows, one row per
                                              request instead of a batch
                                     kv1x150  keyvalue_stress.keyvalue, 1 row, 1 x 150 bytes
                                     kv5x30   keyvalue5_stress.keyvalue5x30, 1 row, 5 x 30
                                     kv5x60   keyvalue5_stress.keyvalue5, 1 row, 5 x 60
              --key-order O        sequential | random. Defaults per workload:
                                   sequential for batch10 (matching its profile),
                                   random for the kv scenarios (matching easy-stress).
              --batch-rows N       rows per batch for batch10 (10)
              --cells N / --cell-size N   override a scenario's shape

            Connection
              --host H             contact point (required)
              --port P             9042
              --datacenter DC      local datacenter (datacenter1)
              --user / --password  cassandra / cassandra
              --keyspace / --table override the workload's defaults
              --cl CL              LOCAL_QUORUM

            Load
              --ops N              total operations (accepts 30m, 500k)
              --duration S         run for S seconds instead
              --warmup S           unrecorded warmup seconds (0)

            Concurrency (this is what replaces cassandra-stress's 500 threads)
              --inflight N         max outstanding requests in TOTAL, split evenly
                                   across shards (3840 = 768 per shard, which is
                                   where the sweep plateaued; halving it roughly
                                   halves latency for ~3% less throughput)
              --shards N           independent pipelines: a driver session, netty
                                   event loop and connection pool each (5). That
                                   loop issues and completes its own requests, so
                                   there is no handoff on the request path.
                                   --netty-io and --connections are per shard;
                                   --inflight is the total, split across shards.
              --netty-io N         driver event loop threads PER SHARD (1). Above
                                   1 the driver spreads a shard's connections over
                                   several loops, which costs a cross-thread hop
                                   per completion for no gain.
              --connections N      connections per node, PER SHARD (2); 4 per shard
                                   measured worse on throughput and p99 alike
              --max-requests-per-connection N   (8192)
              --request-timeout S  per-request timeout, seconds (30)
              --transport T        epoll (default) or nio. epoll fails fast if the
                                   native library is missing; it does not fall
                                   back silently.

            Data
              --partitions N       key space size (10m)
              --value-pool N       distinct pre-encoded values per cell (131072)

            Schema and reporting
              --create-schema      CREATE KEYSPACE/TABLE IF NOT EXISTS first
              --rf N               replication factor for --create-schema (1)
              --truncate           TRUNCATE the table before the run
              --progress S         progress line interval, seconds (1)
            """);
        System.exit(err == null ? 0 : 2);
    }
}
