env:
	@echo "Activate env: conda activate msc-nlp"

sanity:
	python -m src.cli --help
	python -c "import warcio, yaml, pandas, tldextract; print('Imports OK')"

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
