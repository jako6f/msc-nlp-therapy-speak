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

# Historical Stage 1b freeze reproduction (`configs/stage1b_freeze.yaml`)
cc_stage1b_freeze_sample:
	python -m src.cli cc-sample --config configs/stage1b_freeze.yaml

cc_stage1b_freeze_download:
	python -m src.cli cc-download --config configs/stage1b_freeze.yaml

cc_stage1b_freeze_acquire:
	$(MAKE) cc_stage1b_freeze_sample
	$(MAKE) cc_stage1b_freeze_download

cc_stage1b_freeze_scan:
	python -m src.cli cc-scan --config configs/stage1b_freeze.yaml

cc_stage1b_freeze_validate:
	python -m src.cli cc-validate --config configs/stage1b_freeze.yaml

cc_stage1b_freeze_process:
	$(MAKE) cc_stage1b_freeze_scan
	$(MAKE) cc_stage1b_freeze_validate

cc_stage1b_freeze_run:
	$(MAKE) cc_stage1b_freeze_acquire
	$(MAKE) cc_stage1b_freeze_process

# Active Stage 1c freeze workflow (`configs/stage1c_freeze.yaml`)
cc_stage1c_freeze_sample:
	python -m src.cli cc-sample --config configs/stage1c_freeze.yaml

cc_stage1c_freeze_download:
	python -m src.cli cc-download --config configs/stage1c_freeze.yaml

cc_stage1c_freeze_acquire:
	$(MAKE) cc_stage1c_freeze_sample
	$(MAKE) cc_stage1c_freeze_download

cc_stage1c_freeze_scan:
	python -m src.cli cc-scan --config configs/stage1c_freeze.yaml

cc_stage1c_freeze_validate:
	python -m src.cli cc-validate --config configs/stage1c_freeze.yaml

cc_stage1c_freeze_process:
	$(MAKE) cc_stage1c_freeze_scan
	$(MAKE) cc_stage1c_freeze_validate

cc_stage1c_freeze_run:
	$(MAKE) cc_stage1c_freeze_acquire
	$(MAKE) cc_stage1c_freeze_process

# Active Stage 1d freeze workflow (`configs/stage1d_freeze.yaml`)
cc_stage1d_freeze_sample:
	python -m src.cli cc-sample --config configs/stage1d_freeze.yaml

cc_stage1d_freeze_download:
	python -m src.cli cc-download --config configs/stage1d_freeze.yaml

cc_stage1d_freeze_acquire:
	$(MAKE) cc_stage1d_freeze_sample
	$(MAKE) cc_stage1d_freeze_download

cc_stage1d_freeze_scan:
	python -m src.cli cc-scan --config configs/stage1d_freeze.yaml

cc_stage1d_freeze_validate:
	python -m src.cli cc-validate --config configs/stage1d_freeze.yaml

cc_stage1d_freeze_export_urls:
	python -m src.cli cc-stage1d-export-urls --config configs/stage1d_freeze.yaml

cc_stage1d_freeze_upload_urls:
	python -m src.cli cc-stage1d-upload-urls --config configs/stage1d_freeze.yaml

cc_stage1d_freeze_install_indexes_remote:
	@if [ -z "$(URL_EXPORT_URI)" ]; then \
		echo "Set URL_EXPORT_URI=s3://... before running cc_stage1d_freeze_install_indexes_remote"; \
		exit 1; \
	fi
	python -m src.cli cc-stage1d-install-indexes-remote \
		--config configs/stage1d_freeze.yaml \
		--url-export-uri $(URL_EXPORT_URI)

cc_stage1d_freeze_start_index_server:
	python -m src.cli cc-stage1d-start-index-server --config configs/stage1d_freeze.yaml

cc_stage1d_freeze_resolve:
	@if [ -z "$(URL_EXPORT_URI)" ]; then \
		echo "Set URL_EXPORT_URI=s3://... before running cc_stage1d_freeze_resolve"; \
		exit 1; \
	fi
	@if [ -z "$(RESOLVE_OUTPUT_PREFIX)" ]; then \
		echo "Set RESOLVE_OUTPUT_PREFIX=s3://... before running cc_stage1d_freeze_resolve"; \
		exit 1; \
	fi
	python -m src.cli cc-stage1d-resolve-remote \
		--config configs/stage1d_freeze.yaml \
		--url-export-uri $(URL_EXPORT_URI) \
		--s3-output-prefix $(RESOLVE_OUTPUT_PREFIX)

cc_stage1d_freeze_extract:
	@if [ -z "$(POINTER_CACHE_URI)" ]; then \
		echo "Set POINTER_CACHE_URI=s3://... before running cc_stage1d_freeze_extract"; \
		exit 1; \
	fi
	@if [ -z "$(WARC_OUTPUT_PREFIX)" ]; then \
		echo "Set WARC_OUTPUT_PREFIX=s3://... before running cc_stage1d_freeze_extract"; \
		exit 1; \
	fi
	python -m src.cli cc-stage1d-extract-remote \
		--config configs/stage1d_freeze.yaml \
		--pointer-cache-uri $(POINTER_CACHE_URI) \
		--s3-output-prefix $(WARC_OUTPUT_PREFIX)

cc_stage1d_freeze_filter_en_dedup:
	python -m src.cli cc-filter-en-dedup --config configs/stage1d_freeze.yaml

cc_stage1d_freeze_process:
	$(MAKE) cc_stage1d_freeze_scan
	$(MAKE) cc_stage1d_freeze_validate
	$(MAKE) cc_stage1d_freeze_export_urls
	$(MAKE) cc_stage1d_freeze_upload_urls

cc_stage1d_freeze_run:
	$(MAKE) cc_stage1d_freeze_acquire
	$(MAKE) cc_stage1d_freeze_process

# Frozen Stage 1e corpus-tightening workflow (`configs/stage1e_freeze.yaml`)
cc_stage1e_freeze_scan:
	python -m src.cli cc-scan --config configs/stage1e_freeze.yaml

cc_stage1e_freeze_validate:
	python -m src.cli cc-validate --config configs/stage1e_freeze.yaml

cc_stage1e_freeze_export_urls:
	python -m src.cli cc-stage1e-export-urls --config configs/stage1e_freeze.yaml

cc_stage1e_freeze_upload_urls:
	python -m src.cli cc-stage1e-upload-urls --config configs/stage1e_freeze.yaml

cc_stage1e_freeze_install_indexes_remote:
	@if [ -z "$(URL_EXPORT_URI)" ]; then \
		echo "Set URL_EXPORT_URI=s3://... before running cc_stage1e_freeze_install_indexes_remote"; \
		exit 1; \
	fi
	python -m src.cli cc-stage1e-install-indexes-remote \
		--config configs/stage1e_freeze.yaml \
		--url-export-uri $(URL_EXPORT_URI)

cc_stage1e_freeze_start_index_server:
	python -m src.cli cc-stage1e-start-index-server --config configs/stage1e_freeze.yaml

cc_stage1e_freeze_resolve:
	@if [ -z "$(URL_EXPORT_URI)" ]; then \
		echo "Set URL_EXPORT_URI=s3://... before running cc_stage1e_freeze_resolve"; \
		exit 1; \
	fi
	@if [ -z "$(RESOLVE_OUTPUT_PREFIX)" ]; then \
		echo "Set RESOLVE_OUTPUT_PREFIX=s3://... before running cc_stage1e_freeze_resolve"; \
		exit 1; \
	fi
	python -m src.cli cc-stage1e-resolve-remote \
		--config configs/stage1e_freeze.yaml \
		--url-export-uri $(URL_EXPORT_URI) \
		--s3-output-prefix $(RESOLVE_OUTPUT_PREFIX)

cc_stage1e_freeze_extract:
	@if [ -z "$(POINTER_CACHE_URI)" ]; then \
		echo "Set POINTER_CACHE_URI=s3://... before running cc_stage1e_freeze_extract"; \
		exit 1; \
	fi
	@if [ -z "$(WARC_OUTPUT_PREFIX)" ]; then \
		echo "Set WARC_OUTPUT_PREFIX=s3://... before running cc_stage1e_freeze_extract"; \
		exit 1; \
	fi
	python -m src.cli cc-stage1e-extract-remote \
		--config configs/stage1e_freeze.yaml \
		--pointer-cache-uri $(POINTER_CACHE_URI) \
		--s3-output-prefix $(WARC_OUTPUT_PREFIX)

cc_stage1e_freeze_document_quality:
	python -m src.cli cc-stage1e-document-quality --config configs/stage1e_freeze.yaml

cc_stage1e_freeze_process:
	$(MAKE) cc_stage1e_freeze_scan
	$(MAKE) cc_stage1e_freeze_validate
	$(MAKE) cc_stage1e_freeze_export_urls
	$(MAKE) cc_stage1e_freeze_upload_urls
