init_venv:
	python3 -m venv .venv
	@echo "Virtual environment created. Activate it with 'source .venv/bin/activate'"

install_deps:
	pip install -r requirements.txt

test-docker-image:
	cp ~/gcp.secrets.json src/$(FOLDER)
	docker build -t extracao src/$(FOLDER)
	rm src/$(FOLDER)/gcp.secrets.json
