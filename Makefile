.PHONY: up down build logs ps test unit chaos health export restore burst idle demo clean

TOXI ?= http://localhost:8474

build:            ## build the worker and dashboard images
	docker compose build

up: build         ## start the whole stack (deps, 3 workers, loadgen, dashboard)
	docker compose up -d
	@echo
	@echo "  dashboard   http://localhost:9000"
	@echo "  billing     http://localhost:8081/health"
	@echo "  notify      http://localhost:8082/health"
	@echo "  reconcile   http://localhost:8083/health"
	@echo "  rabbitmq    http://localhost:15672  (app / canary-mq-4b7ce02d)"
	@echo

down:             ## stop everything and remove volumes
	docker compose down -v --remove-orphans

ps:
	docker compose ps

logs:
	docker compose logs -f billing notify reconcile

test:             ## full suite inside a container on the compose network
	docker compose --profile test run --rm tests

unit:             ## L0 only: no containers, must pass with docker stopped
	PYTHONPATH=src python3 -m pytest tests/unit -q

health:           ## print every worker's aggregate status
	@for p in 8081 8082 8083; do \
	  printf "  :%s  " $$p; \
	  curl -s --noproxy '*' http://localhost:$$p/health \
	    | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["service"], d["status"], "| checks:", {k: v["internal_status"] for k,v in d["checks"].items()})' \
	    2>/dev/null || echo "unreachable"; \
	done

export:           ## OTLP exporter counters from the billing worker
	@curl -s --noproxy '*' http://localhost:8081/health 	  | python -c "import json,sys; print(json.dumps(json.load(sys.stdin).get('export', {'export': 'not configured'}), indent=2))"

# ---- fault injection ------------------------------------------------------ #
# Each target injects one fault against the running fleet. Watch the dashboard.

chaos-db-down:    ## close the postgres port
	@curl -s --noproxy '*' -X POST $(TOXI)/proxies/postgres -d '{"enabled":false}' >/dev/null && echo "postgres proxy DISABLED"

chaos-db-blackhole: ## packets dropped, socket stays open (firewall DROP)
	@curl -s --noproxy '*' -X POST $(TOXI)/proxies/postgres/toxics \
	  -d '{"name":"blackhole","type":"timeout","stream":"downstream","toxicity":1.0,"attributes":{"timeout":0}}' >/dev/null \
	  && echo "postgres BLACK HOLE (this is the hard one)"

chaos-db-slow:    ## 400ms latency, below the check timeout
	@curl -s --noproxy '*' -X POST $(TOXI)/proxies/postgres/toxics \
	  -d '{"name":"slow","type":"latency","stream":"downstream","toxicity":1.0,"attributes":{"latency":400,"jitter":0}}' >/dev/null \
	  && echo "postgres +400ms"

chaos-redis-down: ## cache path only; locks path stays up
	@curl -s --noproxy '*' -X POST $(TOXI)/proxies/redis-cache -d '{"enabled":false}' >/dev/null && echo "redis-cache proxy DISABLED"

chaos-mq-down:    ## close the broker port
	@curl -s --noproxy '*' -X POST $(TOXI)/proxies/rabbitmq -d '{"enabled":false}' >/dev/null && echo "rabbitmq proxy DISABLED"

restore:          ## clear every toxic and re-enable every proxy
	@curl -s --noproxy '*' -X POST $(TOXI)/reset >/dev/null && echo "all proxies restored"

# ---- load control --------------------------------------------------------- #

idle:             ## pause the load generator (the quiet-queue test)
	@curl -s --noproxy '*' -X POST http://localhost:8090/ -d '{"rate":0}' >/dev/null && echo "load paused - workers must stay OK"

burst:            ## dump 2000 messages onto billing.in
	@curl -s --noproxy '*' -X POST http://localhost:8090/ -d '{"burst":2000}' >/dev/null && echo "2000 messages queued"

normal:           ## resume steady load
	@curl -s --noproxy '*' -X POST http://localhost:8090/ -d '{"rate":8}' >/dev/null && echo "load resumed at 8/s"

clean: down
	docker compose rm -f 2>/dev/null || true
