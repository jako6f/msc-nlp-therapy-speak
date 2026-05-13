CONFIG ?= configs/commoncrawl_collection.yaml
YEAR ?=
TRACK ?= corpus
BATCH ?= 1

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
	cd paper && latexmk -pdf main.tex || true

collection_select_crawls:
	python -m src.cli cc-collection-select-crawls --config $(CONFIG)

collection_preflight:
	python -m src.cli cc-collection-preflight --config $(CONFIG)

collection_stop_index_server:
	python -m src.cli cc-collection-stop-index-server --config $(CONFIG)

collection_sample:
	@if [ -z "$(YEAR)" ]; then echo "Set YEAR=YYYY"; exit 1; fi
	python -m src.cli cc-collection-sample-wet --config $(CONFIG) --year $(YEAR) --track $(TRACK) --batch $(BATCH)

collection_download:
	@if [ -z "$(YEAR)" ]; then echo "Set YEAR=YYYY"; exit 1; fi
	python -m src.cli cc-collection-download-wet --config $(CONFIG) --year $(YEAR) --track $(TRACK) --batch $(BATCH)

collection_acquire:
	$(MAKE) collection_sample YEAR=$(YEAR) TRACK=$(TRACK) BATCH=$(BATCH) CONFIG=$(CONFIG)
	$(MAKE) collection_download YEAR=$(YEAR) TRACK=$(TRACK) BATCH=$(BATCH) CONFIG=$(CONFIG)

collection_scan:
	@if [ -z "$(YEAR)" ]; then echo "Set YEAR=YYYY"; exit 1; fi
	python -m src.cli cc-collection-scan --config $(CONFIG) --year $(YEAR) --track $(TRACK) --batch $(BATCH)

collection_export_urls:
	@if [ -z "$(YEAR)" ]; then echo "Set YEAR=YYYY"; exit 1; fi
	python -m src.cli cc-collection-export-urls --config $(CONFIG) --year $(YEAR) --track $(TRACK) --batch $(BATCH)

collection_upload_urls:
	@if [ -z "$(YEAR)" ]; then echo "Set YEAR=YYYY"; exit 1; fi
	python -m src.cli cc-collection-upload-urls --config $(CONFIG) --year $(YEAR) --track $(TRACK) --batch $(BATCH)

collection_install_indexes:
	@if [ -z "$(URL_EXPORT_URI)" ]; then echo "Set URL_EXPORT_URI=s3://..."; exit 1; fi
	python -m src.cli cc-collection-install-indexes --config $(CONFIG) --url-export-uri $(URL_EXPORT_URI)

collection_start_index_server:
	python -m src.cli cc-collection-start-index-server --config $(CONFIG)

collection_resolve:
	@if [ -z "$(YEAR)" ]; then echo "Set YEAR=YYYY"; exit 1; fi
	@if [ -z "$(URL_EXPORT_URI)" ]; then echo "Set URL_EXPORT_URI=s3://..."; exit 1; fi
	@if [ -z "$(RESOLVE_OUTPUT_PREFIX)" ]; then echo "Set RESOLVE_OUTPUT_PREFIX=s3://..."; exit 1; fi
	python -m src.cli cc-collection-resolve --config $(CONFIG) --year $(YEAR) --track $(TRACK) --batch $(BATCH) --url-export-uri $(URL_EXPORT_URI) --s3-output-prefix $(RESOLVE_OUTPUT_PREFIX)

collection_extract:
	@if [ -z "$(YEAR)" ]; then echo "Set YEAR=YYYY"; exit 1; fi
	@if [ -z "$(POINTER_CACHE_URI)" ]; then echo "Set POINTER_CACHE_URI=s3://..."; exit 1; fi
	@if [ -z "$(WARC_OUTPUT_PREFIX)" ]; then echo "Set WARC_OUTPUT_PREFIX=s3://..."; exit 1; fi
	python -m src.cli cc-collection-extract --config $(CONFIG) --year $(YEAR) --track $(TRACK) --batch $(BATCH) --pointer-cache-uri $(POINTER_CACHE_URI) --s3-output-prefix $(WARC_OUTPUT_PREFIX)

collection_quality:
	@if [ -z "$(YEAR)" ]; then echo "Set YEAR=YYYY"; exit 1; fi
	python -m src.cli cc-collection-quality --config $(CONFIG) --year $(YEAR) --track $(TRACK) --batch $(BATCH)

collection_build_processed:
	@if [ "$(TRACK)" = "trend" ]; then \
		python -m src.cli cc-collection-build-trend --config $(CONFIG); \
	elif [ "$(TRACK)" = "corpus" ]; then \
		python -m src.cli cc-collection-build-corpus --config $(CONFIG); \
	else \
		python -m src.cli cc-collection-build-trend --config $(CONFIG); \
		python -m src.cli cc-collection-build-corpus --config $(CONFIG); \
	fi

collection_year:
	@if [ -z "$(YEAR)" ]; then echo "Set YEAR=YYYY"; exit 1; fi
	python -m src.cli cc-collection-run-year --config $(CONFIG) --year $(YEAR) --track $(TRACK) --batch $(BATCH)

trend_year:
	$(MAKE) collection_year YEAR=$(YEAR) TRACK=trend BATCH=1 CONFIG=$(CONFIG)

corpus_year:
	$(MAKE) collection_year YEAR=$(YEAR) TRACK=corpus BATCH=1 CONFIG=$(CONFIG)

corpus_expand:
	$(MAKE) collection_year YEAR=$(YEAR) TRACK=corpus BATCH=$(BATCH) CONFIG=$(CONFIG)

trend:
	python -m src.cli cc-collection-run --config $(CONFIG) --track trend

corpus:
	python -m src.cli cc-collection-run --config $(CONFIG) --track corpus
