CONFIG ?= configs/commoncrawl_collection.yaml
YEAR ?=
TRACK ?= corpus
BATCH ?= 1

.PHONY: paper

env:
	@echo "Activate env: conda activate msc-nlp"

sanity:
	python -m src.cli --help
	python -c "import datatrove, warcio, yaml, pandas, tldextract; print('Imports OK')"

lint:
	ruff check .

format:
	ruff format .

paper:
	cd paper && latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex

collection_select_crawls:
	python -m src.cli cc-collection-select-crawls --config $(CONFIG)

collection_preflight:
	python -m src.cli cc-collection-preflight --config $(CONFIG)

collection_stop_index_server:
	python -m src.cli cc-collection-stop-index-server --config $(CONFIG)

collection_migrate_interim_layout:
	python -m src.cli cc-collection-migrate-interim-layout --config $(CONFIG) --dry-run

collection_migrate_interim_layout_apply:
	python -m src.cli cc-collection-migrate-interim-layout --config $(CONFIG) --apply

collection_year:
	@if [ -z "$(YEAR)" ]; then echo "Set YEAR=YYYY"; exit 1; fi
	python -m src.cli cc-collection-run-year --config $(CONFIG) --year $(YEAR) --track $(TRACK) --batch $(BATCH)

trend_year:
	$(MAKE) collection_year YEAR=$(YEAR) TRACK=trend BATCH=1 CONFIG=$(CONFIG)

corpus_year:
	$(MAKE) collection_year YEAR=$(YEAR) TRACK=corpus BATCH=1 CONFIG=$(CONFIG)

corpus_expand:
	$(MAKE) collection_year YEAR=$(YEAR) TRACK=corpus BATCH=$(BATCH) CONFIG=$(CONFIG)

corpus_build_processed:
	python -m src.cli cc-collection-build-corpus --config $(CONFIG) --upload

trend:
	python -m src.cli cc-collection-run --config $(CONFIG) --track trend

trend_build_processed:
	python -m src.cli cc-collection-build-trend --config $(CONFIG) --upload

corpus:
	python -m src.cli cc-collection-run --config $(CONFIG) --track corpus
