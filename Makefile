# 🔨 One-time build (first-time users)
build:
	docker-compose build

# 🚀 Launch MLflow UI in background
ui:
	docker-compose up -d mlflow-ui
	@echo "MLflow UI running at http://localhost:5001"

# 🧪 Run CLI pipeline modes
bulk:
	docker-compose run nyc-taxi --mode bulk

test:
	docker-compose run nyc-taxi --mode test

incremental:
	docker-compose run nyc-taxi --mode incremental --year $(YEAR) --month $(MONTH)

# 🛑 Shutdown and cleanup
stop:
	docker-compose down

clean:
	docker-compose down -v
clean-orphans:
	docker-compose down --remove-orphans
