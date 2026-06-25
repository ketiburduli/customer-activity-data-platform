# Convenience wrapper around docker compose. `make help` lists targets.
# Everything here also works as plain `docker compose ...` if you prefer.

CH_USER ?= dpe
CH_PASS ?= dpe
CH_DB   ?= warehouse

.PHONY: help up down ps logs ch query reset kafka-up kafka-down produce

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: ## Start ClickHouse (default; no Kafka)
	docker compose up -d clickhouse
	@echo "ClickHouse: HTTP http://localhost:8123  |  native localhost:9000  |  db=$(CH_DB) user=$(CH_USER) pass=$(CH_PASS)"

down: ## Stop and remove containers + volumes
	docker compose --profile kafka down -v

ps: ## Show running services
	docker compose ps

logs: ## Tail ClickHouse logs
	docker compose logs -f clickhouse

ch: ## Open a clickhouse-client shell on the warehouse db
	docker compose exec clickhouse clickhouse-client --user $(CH_USER) --password $(CH_PASS) -d $(CH_DB)

query: ## Run a one-off query, e.g. make query Q="SELECT 1"
	docker compose exec clickhouse clickhouse-client --user $(CH_USER) --password $(CH_PASS) -d $(CH_DB) --query "$(Q)"

reset: ## Wipe ClickHouse data and restart fresh
	docker compose down -v
	docker compose up -d clickhouse

# ---- optional streaming path -------------------------------------------------
kafka-up: ## Start Kafka alongside ClickHouse
	docker compose --profile kafka up -d
	@echo "Kafka broker: localhost:9092"

kafka-down: ## Stop Kafka (leaves ClickHouse running)
	docker compose rm -sf kafka producer

produce: ## Replay the sample files onto Kafka topics (one-shot)
	docker compose --profile kafka run --rm producer
