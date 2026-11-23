# Flink → Postgres + ClickHouse template

Apache Flink 1.20 (Java 17) job template with two sinks and **savepoint-based safe restart / rebuild** scripts.
Everything runs in Docker; no local JDK or Maven needed.

```
generator source ─┬─▶ ClickHouse  flink.events_raw     (all raw events, ReplacingMergeTree)
                  └─▶ keyBy(user) → 10s window → Postgres user_stats (upsert)
```

New to Flink? Read **[docs/HOW_FLINK_WORKS.md](docs/HOW_FLINK_WORKS.md)** — architecture, state/checkpoints,
delivery guarantees, and where each concept lives in this code.

## Quick start

```bash
make up          # builds jar if missing, starts postgres/clickhouse/flink, submits job   (UI: http://127.0.0.1:8081)
make status      # containers, job state, row counts in both DBs
make verify      # run twice: counts must grow
```

## Operations

| Command | What it does | Data safety |
|---|---|---|
| `make up` | converge stack, submit job; restores the pending savepoint if one exists | idempotent |
| `make restart` | savepoint → stop → resubmit from savepoint | no loss, no gap |
| `make restart-cluster` | same + restart flink containers | DBs untouched |
| `make rebuild` | build new jar **while old job runs**, then savepoint → swap → restore | build fail ⇒ nothing changes |
| `scripts/rebuild.sh --image` | also rebuild Flink image | |
| `make savepoint` | savepoint without stopping | |
| `make down` | savepoint + graceful stop, **keeps volumes** | resume with `make up` |
| `make purge` | delete everything incl. volumes | typed confirmation required |
| `scripts/up.sh --from-checkpoint` | recover after crash/unclean stop from newest retained checkpoint | at-least-once, sinks dedupe |
| `scripts/restart.sh --force` | job wedged, savepoint impossible: cancel + restore from checkpoint | |

Safety properties of the scripts (`scripts/lib.sh`):

* The job is only ever stopped via **stop-with-savepoint**; the path is written to `.state/pending-savepoint`
  *before* anything else happens and removed only after the new job is confirmed `RUNNING`. If a resubmit fails,
  run `scripts/up.sh` to resume from it.
* If a savepoint can't be taken, scripts **refuse to stop** the job (no silent data-loss path); `--force` is explicit.
* Every REST call/wait loop has a timeout; one lifecycle script at a time (lock in `.state/lock`).
* Compose uses **non-blocking, size-capped** `json-file` logging, so a log sink can never wedge a container,
  and ports bind to `127.0.0.1` only.
* Savepoints/checkpoints/DB data live in named volumes that only `purge` removes.

## Layout

```
pom.xml                       shaded fat jar (Flink deps `provided`)
src/main/java/.../Job.java    pipeline wiring, checkpointing, uids
docker-compose.yml            postgres, clickhouse, jobmanager, taskmanager
docker/*/init/*.sql           schemas (run once on empty volumes)
scripts/                      up, down, restart, rebuild, savepoint, status, verify, build, lib
.env.example                  copy → .env (auto-created); ports, memory, job knobs
```

## Customising

1. Replace `EventGenerator` / `DataGeneratorSource` in `Job.java` with your real source (e.g. Kafka).
2. Change models + `Sinks.java` SQL + the two init SQL files. Keep sinks **idempotent on a stable key**.
3. Keep `.uid()` values stable between releases, otherwise savepoints stop restoring
   (`ALLOW_NON_RESTORED_STATE=1 scripts/rebuild.sh` to knowingly drop state of removed operators).
4. Rename the job only together with `JOB_NAME` in `scripts/lib.sh` and `Job.java`.

## Notes / limits

* Init SQL only runs on an **empty** DB volume. For schema changes on existing data use `ALTER TABLE` manually,
  then `make rebuild`.
* The `purge` path deletes ClickHouse and Postgres data too — there is no other script that removes volumes.
* Memory: ~1 GB JM + ~1.7 GB TM + DBs. Lower `JM_MEMORY`/`TM_MEMORY` in `.env` for small machines.
* Local state is `file://` on docker volumes; for production switch checkpoint/savepoint dirs to object storage.
