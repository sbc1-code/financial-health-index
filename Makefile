.PHONY: setup ingest clean-data seed dashboard test all

setup:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

ingest:
	python -m src.ingest

clean-data:
	python -m src.clean

seed:
	python -m src.seed

dashboard:
	streamlit run dashboard.py

test:
	pytest tests/ -v

all: ingest clean-data seed dashboard
