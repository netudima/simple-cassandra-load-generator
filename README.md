# simple-cassandra-load-generator

Async, closed-loop write load for Cassandra, reporting in cassandra-stress's
output format.

Why it exists. cassandra-stress needs around 500 threads to drive a node hard,
and at that width its own `SeedManager` lock becomes the limit: profiling a
single-row insert workload showed 99% of client wall time parked on one
`ReentrantReadWriteLock`, with both machines otherwise idle.

cassandra-easy-stress costs far less than that, but it hands every operation from
a generator thread to a runner thread through an `ArrayBlockingQueue` built in
fair mode (`RequestQueue.kt`).

This generator runs a few independent shards on the driver's own event loops, so
nothing mutable is shared on the request path and no operation is handed between
threads.

## Scope and limitations

This is not a competitor to cassandra-stress or cassandra-easy-stress, and it is
not a general benchmarking tool. Those cover reads, mixed workloads, user-defined
profiles, multi-node clusters and much else. This one writes five fixed scenarios.

What it is for: comparing write throughput and server CPU cost between Cassandra
builds and configurations, with a client cheap enough that it is not the limit.
That is the only question it is built to answer.

**Do not read the latency numbers as a latency measurement.** The run is closed
loop with a fixed in-flight window, which means it is subject to coordinated
omission. When the server stalls, the generator stops issuing, so the requests
that a real arrival process would have queued during the stall are never sent and
never recorded. The histogram therefore describes only the requests that went
out, and its percentiles look better than what a client with an independent
arrival rate would see.

The latency figures are still useful in one narrow way: in a closed loop they
move inversely with throughput at a fixed window, so they help explain a
throughput change. For actual latency or SLA work, use a tool that paces to an
open-loop arrival rate and corrects for omission.

## Build (JDK 25)

```bash
mvn -q clean package
```

Produces in `target/`:

| artifact | contents |
|---|---|
| `simple-cassandra-load-generator.jar` | code only, ~47KB. Manifest `Class-Path` points at `lib/`. |
| `lib/` | the dependencies, ~15MB |
| `simple-cassandra-load-generator-1.0-SNAPSHOT-dist.zip` | jar + `lib/` + `bin/run.sh` |

The jar does not embed its dependencies, so a code change means re-uploading
47KB instead of 15MB and `lib/` can stay where it is on the load host.

## Run

```bash
unzip simple-cassandra-load-generator-1.0-SNAPSHOT-dist.zip
JAVA_HOME=/path/to/jdk25 simple-cassandra-load-generator/bin/run.sh \
    --host 10.0.0.1 --create-schema --duration 60 --workload kv1x150
```

`run.sh` uses `JAVA_HOME` if set, otherwise the `java` on `PATH`.

### Workloads

`--workload` selects one. Each uses the same schema as an existing
cassandra-stress or easy-stress scenario, so results can be put beside runs of
those tools.

| workload | table | shape |
|---|---|---|
| `batch10` | `stress.stress_table` | 10-row UNLOGGED batch per request, 5 values x 10 chars per row, sequential keys |
| `singlerow10` | `stress.stress_table` | the same rows in the same partitions, one row per request |
| `kv1x150` | `keyvalue_stress.keyvalue` | single-row partition, 1 value of 150 bytes |
| `kv5x30` | `keyvalue5_stress.keyvalue5x30` | single-row partition, 5 values of 30 bytes |
| `kv5x60` | `keyvalue5_stress.keyvalue5` | single-row partition, 5 values of 60 bytes |

`batch10` and `singlerow10` write identical data and differ only in rows per
request. `kv1x150` and `kv5x30` both carry 150 bytes per row, split across a
different number of cells, which separates cell count from payload size.

Key order defaults per workload: sequential for the `stress.stress_table` ones,
whose cassandra-stress profile uses `population: seq(...)`, and random for the
key-value ones, matching easy-stress. `--key-order` overrides it. Sequential is
per shard: each shard walks its own slice of the key space, so there is no
shared counter on the request path.

`bin/run.sh --help` lists every option. The main ones:

- `--inflight N` — total outstanding requests, split evenly across shards. This
  takes the place of cassandra-stress's thread count, and it is the only
  throttle: the run is closed loop, so the rate is an outcome, not a setting.
- `--shards N` — independent pipelines, one driver session each.
- `--netty-io N` / `--connections N` — event loops and connections per shard.
- `--ops N` / `--duration S` — how much load. `--warmup S` is excluded from the
  statistics.
- `--cells N` / `--cell-size N` / `--partitions N` — the shape of the data.

Defaults come from a sweep on a 16-core node: 5 shards, 768 in flight each
(`--inflight 3840`), 1 event loop and 2 connections per shard. Past 768 per shard
throughput flattens while latency keeps rising, and 4 connections per shard
measured worse on both throughput and p99.

## Cost on the client

All rows below write the same workload — single-row partitions with one 150-byte
value — against the same Cassandra node running trunk, at `LOCAL_QUORUM` with
RF 1. "client CPU/op" is the load node's busy CPU divided by the achieved rate.

| tool | client config | op/s | client cores | client CPU/op |
|---|---|---|---|---|
| cassandra-stress | 500 threads, `maxPending=256` | 747,187 | 19.6 | 26.3 us |
| cassandra-easy-stress | 4 threads, `--rate 800k` | 591,752 | 8.2 | 13.8 us |
| this tool, earlier dispatcher model | 4096 in flight, 4 dispatchers, 8 loops | 930,347 | 5.3 | 5.7 us |
| this tool, current | 3840 in flight, 5 shards, 1 loop each | 915,253 | 2.6 | 2.8 us |

So offering ~750k writes/s costs about 20 cores through cassandra-stress and
about 2 cores here. On a 16-core load node this tool reaches ~915k op/s while
using 16% of the machine, which leaves the server as the thing being measured
instead of the client.

This table compares client cost on one write scenario. It is not a verdict on
those tools, which do far more than this one does.

## Design notes

**One thread owns a request end to end.** Each shard has its own driver session,
and so its own connection pool and event loop, and that loop both issues and
completes requests. In-flight is a plain counter only that loop touches: when the
window is full the loop stops issuing and goes back to `epoll_wait`, where the
next response wakes it, so a full window needs no park or spin. Latency and
counters are per shard for the same reason, which is what makes the non-atomic
increments on them correct.

The single writer does not depend on how many netty threads a shard has. A
completion can land on a sibling event loop, and timeouts arrive on the driver's
admin thread, so the callback checks `shard.loop.inEventLoop()` and re-dispatches
onto the shard's own loop when it does not match. `--netty-io 1` is a
performance setting, not a correctness one: it is where that check always passes
and no request is handed between threads.

**Closed loop.** There is no rate pacing and no scheduler. The offered load is
`--inflight` and the achieved rate is whatever the server sustains, so a
reported rate is always one that was sustained.

**No Strings on the request path.** Values are pre-built `char[]` held in a pool
and bound through `CharArrayCodec`, a `TypeCodec<char[]>` that writes CQL `text`
directly. `setString` would allocate three objects per cell — the String, the
`byte[]` from `getBytes(UTF_8)`, and the `ByteBuffer` around it — which at five
cells and 900k op/s is about 13 million allocations per second. Each value is
bound with the codec passed explicitly, so there is no `Object[]` and no codec
lookup per value.

**Latency is service time**, from driver hand-off to response. In a closed loop
it is tied to throughput by Little's law (`inflight = rate x latency`), so read
the two together: a server that gets slower per operation shows up as higher
latency at a lower rate, with the in-flight window unchanged. These numbers are
not a latency measurement — see the coordinated omission note under Scope.

## Output

Interval lines and a `Results:` block terminated by `END`, in cassandra-stress's
format. Two differences from that tool:

- An extra `Offered load` block above `Results:`, recording the in-flight window
  the run used.
- No GC columns. cassandra-stress fills those from the server over JMX. This tool
  has no JMX connection and can only see its own heap, so it leaves them out
  instead of printing client numbers under headings that mean server numbers. For
  server GC, read the node's gc.log.
