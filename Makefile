.PHONY: install test demo evaluate outbox-demo outbox-evaluate
install:
	python -m pip install -e .
test:
	python -m unittest discover -s verification -v
demo:
	python -m resilient_runtime demo --output artifacts/demo.json
evaluate:
	python -m resilient_runtime evaluate --output artifacts/evaluation.json
outbox-demo:
	python -m resilient_runtime outbox-demo --output artifacts/outbox-demo.json
outbox-evaluate:
	python -m resilient_runtime outbox-evaluate --output artifacts/outbox-evaluation.json
