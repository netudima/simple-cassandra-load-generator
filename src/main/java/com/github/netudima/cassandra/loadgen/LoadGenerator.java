package com.github.netudima.cassandra.loadgen;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.datastax.oss.driver.internal.core.context.InternalDriverContext;

import io.netty.channel.EventLoop;

public final class LoadGenerator {

    private final Config cfg;
    private final Stats stats = new Stats();
    private final Workload workload;
    private volatile boolean running = true;

    private LoadGenerator(Config cfg) {
        this.cfg = cfg;
        this.workload = Workload.of(cfg);
    }

    public static void main(String[] args) throws Exception {
        Config cfg = Config.parse(args);
        new LoadGenerator(cfg).run();
    }

    /**
     * One independent pipeline: its own driver session, and so its own netty event
     * loop and connection pool, plus its own in-flight count and binder. Nothing on
     * the request path is shared between shards.
     */
    private static final class Shard implements AutoCloseable {
        final int id;
        final CqlSession session;
        final PreparedStatement prepared;
        final int capacity;
        final ShardStats stats;
        /**
         * The loop this shard issues and completes requests on. With --netty-io 1 it
         * is the session's only loop and no request is ever handed between threads.
         * A bigger group still works, but the driver then spreads this shard's
         * connections over several loops and every completion has to come back here.
         */
        final EventLoop loop;
        Workload.Binder binder;

        // Written only on `loop`, so the plain ++/-- are safe; volatile just so the
        // drain can watch it from the main thread. completeOnLoop records before it
        // decrements, which means a drain that sees 0 also sees every stat written.
        // Keep that order or reset() between phases can race a late completion.
        volatile int inFlight;
        long issued;
        boolean pumping;

        Shard(int id, CqlSession session, PreparedStatement prepared, int capacity, ShardStats stats) {
            this.id = id;
            this.session = session;
            this.prepared = prepared;
            this.capacity = capacity;
            this.stats = stats;
            this.loop = ((InternalDriverContext) session.getContext())
                    .getNettyOptions().ioEventLoopGroup().next();
        }

        @Override
        public void close() {
            session.close();
        }
    }

    private void run() throws Exception {
        boolean useEpoll = cfg.transport.equals("epoll");
        if (useEpoll && !EpollTransport.isAvailable()) {
            System.err.println("ERROR: --transport epoll requested but netty's native epoll is unavailable");
            System.err.println("       cause: " + EpollTransport.unavailabilityCause());
            System.err.println("       (needs netty-transport-native-epoll for this platform on the classpath)");
            System.exit(2);
        }

        List<Shard> shards = new ArrayList<>(cfg.shards);
        try {
            // The in-flight window is the TOTAL across shards, so --inflight keeps
            // meaning the same thing whatever the shard count.
            for (int i = 0; i < cfg.shards; i++) {
                CqlSession session = newSession(useEpoll);
                if (i == 0) {
                    if (cfg.createSchema) {
                        createSchema(session);
                    }
                    if (cfg.truncate) {
                        System.out.printf("Truncating %s.%s%n", workload.keyspace(), workload.table());
                        session.execute("TRUNCATE " + workload.keyspace() + "." + workload.table());
                    }
                }
                int capacity = cfg.inFlight / cfg.shards + (i < cfg.inFlight % cfg.shards ? 1 : 0);
                shards.add(new Shard(i, session, session.prepare(workload.insertCql()), capacity, stats.newShard()));
            }

            cfg.print(keyLength(), workload, describeData());
            System.out.printf("  transport (live): %s%n%n", EpollTransport.actualTransport(shards.get(0).session));

            if (cfg.warmupSec > 0) {
                System.out.printf("Warming up for %ds (not recorded)%n", cfg.warmupSec);
                stats.setRecording(false);
                phase(shards, cfg.warmupSec, -1, false);
                stats.reset();
            }

            long startMs = System.currentTimeMillis();
            phase(shards, cfg.durationSec, cfg.ops, true);
            long elapsedMs = System.currentTimeMillis() - startMs;

            stats.printResults(elapsedMs, cfg, workload.rowsPerOp());
        } finally {
            for (Shard s : shards) {
                s.close();
            }
        }
        if (stats.hadErrors()) {
            System.exit(1);
        }
    }

    /** A session per shard: each builds its own netty group and connection pool. */
    private CqlSession newSession(boolean useEpoll) {
        DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                .withString(DefaultDriverOption.REQUEST_CONSISTENCY, cfg.consistency)
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
                        Duration.ofSeconds(cfg.requestTimeoutSec))
                .withBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, true)
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, cfg.connections)
                .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, cfg.maxRequestsPerConnection)
                .withInt(DefaultDriverOption.NETTY_IO_SIZE, cfg.nettyIo)
                // Schema metadata triggers a refresh per DDL and is not used here.
                .withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, false)
                .build();
        CqlSessionBuilder builder = useEpoll ? EpollTransport.sessionBuilder() : CqlSession.builder();
        return builder
                .addContactPoint(new InetSocketAddress(cfg.host, cfg.port))
                .withLocalDatacenter(cfg.datacenter)
                .withAuthCredentials(cfg.user, cfg.password)
                .withConfigLoader(loader)
                // Registered so decode/format work too; the hot path passes the
                // codec explicitly and never looks it up.
                .addTypeCodecs(CharArrayCodec.INSTANCE)
                .build();
    }

    /** One phase: kick each shard's loop into pumping, wait it out, then drain. */
    private void phase(List<Shard> shards, long seconds, long ops, boolean record) throws Exception {
        running = true;

        long start = System.nanoTime();
        long endNanos = seconds > 0 ? start + seconds * 1_000_000_000L : Long.MAX_VALUE;
        long opsPerShard = ops > 0 ? (ops + shards.size() - 1) / shards.size() : -1;

        if (record) {
            stats.setRecording(true);
            stats.startProgress(cfg.progressSec, start, workload.rowsPerOp());
        }

        for (Shard shard : shards) {
            shard.binder = workload.newBinder(shard.prepared, shard.id, shards.size());
            shard.inFlight = 0;
            shard.issued = 0;
        }

        pumpPhase(shards, endNanos, opsPerShard, record);

        if (record) {
            stats.stopProgress();
        }
    }

    /**
     * Start every shard pumping on its own loop, wait out the phase, then let the
     * outstanding requests land.
     *
     * <p>A full window needs no park or spin: the loop simply stops issuing and goes
     * back to epoll_wait, and the next response wakes it.
     */
    private void pumpPhase(List<Shard> shards, long endNanos, long opsPerShard, boolean record)
            throws InterruptedException {
        for (Shard shard : shards) {
            shard.loop.execute(() -> pump(shard, endNanos, opsPerShard, record));
        }
        while (running && System.nanoTime() < endNanos) {
            if (opsPerShard > 0) {
                boolean allDone = true;
                for (Shard s : shards) {
                    if (s.issued < opsPerShard) {
                        allDone = false;
                        break;
                    }
                }
                if (allDone) {
                    break;
                }
            }
            Thread.sleep(5);
        }
        running = false;
        for (Shard s : shards) {
            while (s.inFlight > 0) {
                Thread.sleep(1);
            }
        }
    }

    /**
     * Fill this shard's in-flight window. Always runs on the shard's loop.
     *
     * <p>A request that fails fast completes inline inside {@code executeAsync} and
     * calls straight back in here. The {@code pumping} flag catches that re-entry;
     * without it each such request would add a stack frame.
     */
    private void pump(Shard s, long endNanos, long opsPerShard, boolean record) {
        if (s.pumping) {
            return;
        }
        s.pumping = true;
        try {
            while (running && s.inFlight < s.capacity) {
                if (opsPerShard > 0 && s.issued >= opsPerShard) {
                    break;
                }
                if (System.nanoTime() >= endNanos) {
                    break;
                }
                s.inFlight++;
                s.issued++;
                issueOnLoop(s, endNanos, opsPerShard, record);
            }
        } finally {
            s.pumping = false;
        }
    }

    private void issueOnLoop(Shard s, long endNanos, long opsPerShard, boolean record) {
        Statement<?> statement = s.binder.next(ThreadLocalRandom.current());
        long t0 = System.nanoTime();
        s.session.executeAsync(statement).whenComplete((rs, err) -> {
            // Can complete on a sibling event loop, or on the driver's admin thread
            // for a timeout, so move it onto this shard's loop to keep one writer.
            // inEventLoop() tests the caller against THIS loop, not netty in general,
            // so a sibling loop takes the execute() branch.
            if (s.loop.inEventLoop()) {
                completeOnLoop(s, err, t0, endNanos, opsPerShard, record);
            } else {
                s.loop.execute(() -> completeOnLoop(s, err, t0, endNanos, opsPerShard, record));
            }
        });
    }

    private void completeOnLoop(Shard s, Throwable err, long t0,
                                long endNanos, long opsPerShard, boolean record) {
        if (err != null) {
            s.stats.recordError(err);
        } else if (record) {
            s.stats.recordLatency((System.nanoTime() - t0) / 1000);
        }
        s.stats.recordCompleted();
        s.inFlight--;
        pump(s, endNanos, opsPerShard, record);
    }

    private void createSchema(CqlSession session) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + workload.keyspace()
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': "
                + cfg.rf + "}");
        session.execute(workload.createTableCql());
        System.out.printf("Schema ensured: %s.%s%n", workload.keyspace(), workload.table());
    }

    private int keyLength() {
        return workload instanceof BatchWorkload b ? b.keys().keyLength()
                : ((KeyValueWorkload) workload).keys().keyLength();
    }

    private String describeData() {
        if (workload instanceof BatchWorkload b) {
            // rows/partition is the data shape, rows/request is how it is written.
            // They differ for singlerow10.
            return String.format("%d rows/partition, %d values x %d chars each, %s per request, %s keys",
                    b.rowsPerPartition(), b.valuesPerRow(), b.valueSize(),
                    b.isBatched() ? b.rowsPerOp() + "-row batch" : "1 row",
                    b.keys().isSequential() ? "sequential" : "random");
        }
        KeyValueWorkload kv = (KeyValueWorkload) workload;
        return String.format("1 row/partition, %d value%s x %d chars (%d bytes/row), %s keys",
                kv.cells(), kv.cells() == 1 ? "" : "s", kv.cellSize(), kv.cells() * kv.cellSize(),
                kv.keys().isSequential() ? "sequential" : "random");
    }
}
