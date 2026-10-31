PY ?= python3
.PHONY: test sample demo failure-demo benchmark all
test:          ; $(PY) -m unittest discover -s tests -v
sample:        ; $(PY) -m loadlab generate --out data/sample --edge-cases data/edge_cases
demo:          ; $(PY) -m loadlab demo --fresh && $(PY) -m loadlab report --db output/demo.db
failure-demo:  ; bash scripts/failure_demo.sh
benchmark:     ; $(PY) -m loadlab benchmark --orders 100000 --days 30 --out output
all: test sample demo failure-demo benchmark
