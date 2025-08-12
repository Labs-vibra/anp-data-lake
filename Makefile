PROJECT_ID=ext-ecole-biomassa-468317
ARTIFACT_REPO=ar-juridico-process-anp-datalake
COMPOSE_BUCKET_NAME=us-central1-composer-jur-an-2f010f0a-bucket

init_venv:
	python3 -m venv .venv
	@echo "Virtual environment created. Activate it with 'source .venv/bin/activate'"

install_deps:
	pip install -r requirements.txt

test-docker-image:
	cp ~/gcp.secrets.json src/$(FOLDER)
	docker build -t extracao src/$(FOLDER)
	rm src/$(FOLDER)/gcp.secrets.json

gcp-login:
	gcloud auth application-default login --no-launch-browser

configure-docker-gcp:
	gcloud auth configure-docker us-central1-docker.pkg.dev
	gcloud config set project $(PROJECT_ID)

upload-docker:
	docker build --platform linux/amd64 -t us-central1-docker.pkg.dev/$(PROJECT_ID)/${ARTIFACT_REPO}/run-extracao-logistica:latest src/logistica
	docker push us-central1-docker.pkg.dev/$(PROJECT_ID)/${ARTIFACT_REPO}/run-extracao-logistica:latest

upload-dags:
	gsutil cp -r airflow/dags/* gs://$(COMPOSE_BUCKET_NAME)/dags/
