SHELL := /bin/bash
include .env

init_venv:
	python3 -m venv .venv
	@echo "Virtual environment created. Activate it with 'source .venv/bin/activate'"

install_deps:
	pip install -r requirements.txt

test-docker-image:
	cp ~/gcp.secrets.json src/$(FOLDER)
	docker build -t extracao src/$(FOLDER)
	rm src/$(FOLDER)/gcp.secrets.json

run-test-docker-image: test-docker-image
	docker run --env-file ./.env extracao:latest

gcp-login:
	gcloud auth application-default login --no-launch-browser

configure-docker-gcp:
	gcloud auth configure-docker us-central1-docker.pkg.dev
	gcloud config set project $(GOOGLE_CLOUD_PROJECT)

upload-docker:
	python3 ./scripts/upload-docker-images.py

upload-dags:
	gsutil cp -r airflow/dags/* gs://$(COMPOSE_BUCKET_NAME)/dags/

upload-files:
	gsutil cp -r db/queries/* gs://$(GOOGLE_BUCKET_NAME)/sql/


upload-db:
	python3 ./scripts/upload_db.py

run-terraform-prod:
	cd terraform && terraform apply -var="is_prod=true" && cd ..

run-terraform-dev:
	cd terraform && terraform apply && cd ..

validate-terraform:
	cd terraform && \
	terraform validate && \
	terraform fmt && \
	cd ..

create-artifact-registry-prod:
	cd terraform && terraform apply -target=google_artifact_registry_repository.anp_repo_etl -var="is_prod=true" && cd ..

create-artifact-registry:
	cd terraform && terraform apply -target=google_artifact_registry_repository.anp_repo_etl && cd ..

