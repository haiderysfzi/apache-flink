.PHONY: help build up down restart restart-cluster rebuild savepoint status verify logs psql ch purge

help:            ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-16s %s\n", $$1, $$2}'

build:           ## build artifacts/job.jar in Docker (job keeps running)
	scripts/build.sh
up:              ## start everything, submit job (restores pending savepoint if any)
	scripts/up.sh
down:            ## savepoint + graceful stop, data kept
	scripts/down.sh
restart:         ## safe job restart via savepoint
	scripts/restart.sh
restart-cluster: ## safe restart incl. flink containers
	scripts/restart.sh --cluster
rebuild:         ## build new code, redeploy with state preserved
	scripts/rebuild.sh
savepoint:       ## savepoint without stopping
	scripts/savepoint.sh
status:          ## containers, job, sink counts
	scripts/status.sh
verify:          ## sink row counts
	scripts/verify.sh
logs:            ## tail flink logs
	docker compose logs -f --tail=100 jobmanager taskmanager
psql:            ## psql shell
	docker compose exec postgres psql -U $${POSTGRES_USER:-flink} -d $${POSTGRES_DB:-flink}
ch:              ## clickhouse-client shell
	docker compose exec clickhouse clickhouse-client --user $${CLICKHOUSE_USER:-flink} --password $${CLICKHOUSE_PASSWORD:-flink}
purge:           ## DELETE ALL DATA (asks for confirmation)
	scripts/down.sh --purge
