.PHONY: up down reset logs status shell-trino shell-postgres init-schema

# Start all services
up:
	docker compose up -d

# Stop all services (keep data)
down:
	docker compose down

# Stop and wipe all data volumes (full reset)
reset:
	docker compose down -v

# Tail logs for all services (Ctrl+C to stop)
logs:
	docker compose logs -f

# Show status of all containers
status:
	docker compose ps

# Open Trino CLI
shell-trino:
	docker exec -it trino trino

# Open PostgreSQL shell
shell-postgres:
	docker exec -it postgres psql -U $${POSTGRES_USER} -d $${POSTGRES_DB}

# Create Iceberg schema and tables (run once after first 'make up')
init-schema:
	docker exec -i trino trino < conf/trino/schema.sql
