clean:
	find . -name "*.pyc" | xargs rm
	rm -rf exporter/tests/__pycache__/

test:
	py.test
