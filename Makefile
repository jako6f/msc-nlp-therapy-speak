env:
	@echo "Activate env: conda activate msc-nlp"

sanity:
	python -m src.cli --help
	python -c "import warcio, yaml, pandas, tldextract; print('Imports OK')"

lint:
	ruff check .

format:
	ruff format .

test:
	pytest -q

paper:
	cd paper && latexmk -pdf main.tex || true

cc_pilot_acquire:
	python -m src.cli cc-sample --config configs/pilot.yaml
	python -m src.cli cc-download --config configs/pilot.yaml

cc_pilot_scan:
	python -m src.cli cc-scan --config configs/pilot.yaml

cc_pilot:
	make cc_pilot_acquire
	make cc_pilot_scan
