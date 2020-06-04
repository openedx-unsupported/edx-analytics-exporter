clean:
	find . -name "*.pyc" | xargs rm
	rm -rf exporter/tests/__pycache__/

test:
	pytest


upgrade: export CUSTOM_COMPILE_COMMAND=make upgrade
upgrade: ## update the requirements/*.txt files with the latest packages satisfying requirements/*.in
	pip install -qr requirements/pip-tools.txt
	pip-compile --upgrade -o requirements/pip-tools.txt requirements/pip-tools.in
	pip-compile --upgrade -o requirements/base.txt requirements/base.in
	pip-compile --upgrade -o requirements/test.txt requirements/test.in
	pip-compile --upgrade -o requirements/github_requirements.txt requirements/github.in
	pip-compile --upgrade -o requirements/tox.txt requirements/tox.in
