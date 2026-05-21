.PHONY: dev prod down logs clean test

# Environment variables
COMPOSE_DEV = docker compose -f docker-compose.dev.yml
COMPOSE_PROD = docker compose -f docker-compose.prod.yml

dev:
	@echo "Starting Development Environment with Hot Reload (Air) [Go 1.26]..."
	$(COMPOSE_DEV) up --build

prod:
	@echo "Starting Production Environment [Go 1.26]..."
	$(COMPOSE_PROD) up --build

down:
	@echo "Stopping all environments..."
	$(COMPOSE_DEV) down
	$(COMPOSE_PROD) down

logs:
	$(COMPOSE_DEV) logs -f

test:
	@echo "Running tests inside dev container..."
	$(COMPOSE_DEV) run --rm file-processor go test -v ./...

clean:
	@echo "Cleaning up..."
	rm -rf tmp build-errors.log
	$(COMPOSE_DEV) down -v
	$(COMPOSE_PROD) down -v
