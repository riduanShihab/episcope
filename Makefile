# Makefile for EpiScope
# Usage: make all

.PHONY: all generate pipeline analytics serve dashboard test clean

all: generate pipeline analytics serve
	@echo "✓ Full pipeline complete. Run 'make dashboard' to view."

generate:
	python generator/generate.py --sf 0.25 --seed 42
	python generator/fetch_weather.py

pipeline:
	python pipeline/bronze/ingest.py
	python pipeline/silver/transform.py
	python pipeline/gold/aggregate.py

analytics:
	python analytics/detection/detect.py
	python analytics/forecast/forecast.py
	python analytics/optimise/optimise.py
	python analytics/calendar/calendar.py

serve:
	python serving/export.py

dashboard:
	cd dashboard && python -m http.server 8080

test:
	python -m pytest tests/ -v

clean:
	rm -rf data/bronze data/silver data/gold data/serving data/raw
	rm -rf spark-warehouse metastore_db derby.log

full-scale:
	python run_all.py --sf 1.0
