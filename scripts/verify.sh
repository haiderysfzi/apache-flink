#!/usr/bin/env bash
# Row counts in both sinks. Run twice a few seconds apart: numbers should grow while the job runs.
source "$(dirname "$0")/lib.sh"
require_docker

echo "== postgres: user_stats (windowed aggregates, upserted) =="
dc exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
  "select 'rows='||count(*)||'  windows='||count(distinct window_start)||'  latest_window='||coalesce(max(window_start)::text,'-') from user_stats" \
  || echo "postgres unreachable"

echo "== clickhouse: events_raw (raw events, deduped on merge) =="
dc exec -T clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -q \
  "select count() as rows_total, uniqExact(event_id) as unique_ids, count()-uniqExact(event_id) as not_yet_merged_dupes from flink.events_raw FORMAT Vertical" \
  || echo "clickhouse unreachable"
