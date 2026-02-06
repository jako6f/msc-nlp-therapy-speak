env:
	@echo "Activate env: conda activate nlp-therapy"

sanity:
	python -m src.cli --config configs/pilot.yaml
	python -c "import warcio, yaml, pandas, tldextract; print('Imports OK')"

lint:
	ruff check .

format:
	ruff format .

test:
	pytest -q

paper:
	cd paper && latexmk -pdf main.tex || true
