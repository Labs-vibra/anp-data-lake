init_venv:
	python3 -m venv .venv
	@echo "Virtual environment created. Activate it with 'source .venv/bin/activate'"

install_deps:
	pip install -r requirements.txt