# How Apache Flink (Java) works — mapped to this repo

## 1. The cluster

```
 you ── flink run job.jar ──▶  JobManager (1 per cluster)
                               ├─ Dispatcher        accepts jobs, serves REST/UI (:8081)
                               ├─ ResourceManager   tracks TaskManagers & their slots
                               └─ JobMaster (1/job) builds the ExecutionGraph, schedules tasks,
                                                    triggers checkpoints, handles failover
                                   │ deploys tasks
                     ┌─────────────┴─────────────┐
                 TaskManager A                TaskManager B     (JVMs that do the work)
                 [slot][slot]                 [slot][slot]      slot = unit of parallelism/memory
```

* `main()` of your jar runs on the **client** (here: inside the jobmanager container via `flink run`).
  It does **not** process data; it only *builds a dataflow graph* and calls `env.execute()`, which
  submits that graph. Anything read in `main()` (e.g. `AppConfig.fromEnv()`) is captured and serialized
  into the operators shipped to TaskManagers.
* **Session cluster** (this template): long-running cluster, many jobs submitted to it.
  **Application mode**: cluster per job, `main()` runs on the JobManager (typical on K8s/YARN).

## 2. From your code to running tasks

1. **StreamGraph** – your `DataStream` calls (`source → keyBy → window → sink`).
2. **JobGraph** – operators that can run in the same thread are **chained** (e.g. source + map) to avoid
   serialization/network hops. A chain breaks at `keyBy` (a *shuffle*), at parallelism changes, or when
   you call `.disableChaining()`.
3. **ExecutionGraph** – each operator expanded into `parallelism` **subtasks**, assigned to slots.

**Parallelism**: `-p 2` means each operator has 2 parallel subtasks. `keyBy(userId)` hash-partitions by key
into *key groups* so all events of one user always land on the same subtask — that is what makes per-key
state and windows correct. `maxParallelism` (default 128) fixes the number of key groups and therefore
the upper bound for rescaling a job from a savepoint.

## 3. Time

* **Event time** = timestamp inside the record (`Event.ts`). **Processing time** = wall clock of the machine.
* A **watermark(t)** says "I believe no more events with timestamp ≤ t will arrive". Windows fire when the
  watermark passes their end. Here: `forBoundedOutOfOrderness(2s)` = watermark trails max-seen-ts by 2s;
  `withIdleness(10s)` stops an idle source subtask from holding all watermarks back.
* Events later than the watermark (and allowed lateness) are dropped from the window (or sent to a side output).

## 4. State, checkpoints, savepoints — the heart of Flink

* **Keyed state** (per key: counters, windows' accumulators, …) lives in a **state backend**:
  `hashmap` = JVM heap (fast, bounded by memory) or **RocksDB** = off-heap on local disk (huge state, slower).
* **Checkpoint** = automatic, periodic, *consistent snapshot of all operator state + source positions*.
  Mechanism (Chandy–Lamport variant): JobMaster injects a **barrier** at the sources; the barrier flows with
  the data; when an operator has seen the barrier on all inputs it snapshots its state and forwards it.
  When every operator acked, the checkpoint is complete. On failure Flink restarts the job from the last
  completed checkpoint: sources rewind to their saved position, state is restored.
  Config: `enableCheckpointing(10s, EXACTLY_ONCE)` in `Job.java`.
* **Savepoint** = the same snapshot, but **manually triggered, self-contained, and kept** — meant for
  planned operations: upgrade code, change parallelism, move cluster. Our scripts always
  `stop-with-savepoint` (take savepoint, then stop) and resubmit with `-s <savepoint>`.
* **Operator uids**: state in a savepoint is matched to operators by `.uid("…")`. Without explicit uids,
  Flink auto-generates ids from graph shape, so a harmless refactor can make a savepoint unrestorable.
  Every stateful/stateful-adjacent operator in `Job.java` has a `uid`. **Never rename a uid casually.**
* Checkpoints are **retained on cancel** (`RETAIN_ON_CANCELLATION` in compose), so even after a crash you can
  restore: `scripts/up.sh --from-checkpoint`.

## 5. Delivery guarantees and the two sinks

* Flink's **exactly-once** is about *state*: after recovery, state reflects each input record exactly once.
* End-to-end exactly-once additionally needs a **transactional/2PC sink** (Kafka transactions, XA) or an
  **idempotent sink**. Batched JDBC sinks are **at-least-once**: records after the last checkpoint may be
  re-sent after a failure.
* So this template makes both sinks idempotent on a *stable key*:

| Sink | Mechanism | Key |
|---|---|---|
| Postgres `user_stats` | `INSERT … ON CONFLICT DO UPDATE` | `(user_id, window_start)` |
| ClickHouse `events_raw` | `ReplacingMergeTree` | `event_id` (deterministic, derived from the source sequence number) |

  ClickHouse deduplicates during background merges, so use `FINAL` or `uniqExact(event_id)` when you need
  exact numbers *right now* (`scripts/verify.sh` shows how many dupes are still unmerged).
* Why the generator derives `event_id` from the sequence index: after a restore the source replays the same
  indexes → same ids → duplicates collapse. With Kafka use `topic-partition-offset` or a business key.

## 6. Failure handling

* **Restart strategy** (`fixedDelayRestart(20, 10s)`): on a task failure the whole job (or a failover region) is
  cancelled and redeployed from the last checkpoint, up to N times, then the job goes `FAILED`.
* **Checkpoint failures** are tolerated up to 3 in a row (`setTolerableCheckpointFailureNumber`).
* **Backpressure**: if a sink is slow (e.g. ClickHouse merging), upstream operators automatically slow down
  through bounded network buffers; the source ends up consuming less. Watch it in the UI → job → *Backpressure*.
* A hung/dead **TaskManager** is detected by heartbeat timeout (60s here); its slots are re-requested.

## 7. Dependencies & packaging gotchas (see `pom.xml`)

* `flink-streaming-java`, `flink-clients` are `provided` — the cluster already ships them. Bundling them
  causes classloader conflicts.
* Connectors/drivers (`flink-connector-jdbc`, `postgresql`, `clickhouse-jdbc`) are **bundled** via the shade plugin.
  `ServicesResourceTransformer` is required so `META-INF/services/java.sql.Driver` files are merged.
* POJO rules: public class, public no-arg constructor, public fields (or getters/setters). Otherwise Flink
  falls back to Kryo (slow, and a state-evolution hazard).
* Java 17 on the cluster ⇒ compile with `release 17`.

## 8. Where each concept lives in the code

| Concept | File |
|---|---|
| Env, checkpointing, restart strategy, graph wiring, uids | `src/main/java/com/example/flinkjob/Job.java` |
| Source + deterministic ids | `source/EventGenerator.java` |
| Watermarks, `keyBy`, tumbling window | `Job.java` |
| Incremental aggregation (`AggregateFunction` + `ProcessWindowFunction`) | `transform/UserStatAggregation.java` |
| Idempotent batched sinks | `sink/Sinks.java` |
| Config from env | `config/AppConfig.java` |
| Cluster config, state dirs, retained checkpoints | `docker-compose.yml` (`FLINK_PROPERTIES`) |

## 9. Extending the template

* **Real source**: add `flink-connector-kafka`, replace the `DataGeneratorSource` with `KafkaSource`
  (offsets are stored in checkpoints → replay is exact).
* **New stateful operator**: extend `KeyedProcessFunction`, use `ValueState`/`MapState`, add a `uid`.
* **Bigger state**: `state.backend.type: rocksdb` + `state.backend.incremental: true`
  (+ `flink-statebackend-rocksdb` is already in the flink image).
* **Production deployment**: move checkpoint/savepoint dirs to S3/HDFS (`state.checkpoints.dir: s3://…`),
  run JM in HA (K8s/ZooKeeper), run in Application mode, add external uptime alerting on the Flink REST `/jobs/overview`.
* **Schema changes**: ClickHouse `ALTER TABLE … ADD COLUMN` first, then deploy code (`scripts/rebuild.sh`).
