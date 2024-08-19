lock:
	pip-compile -v -o requirements.txt --upgrade --no-emit-index-url \
		--resolver backtracking --generate-hashes --extra dev \
		pyproject.toml

validate:
	ruff check .
	black --check .

format:
	black .
	ruff check --fix .
