PROJECT_ID ?= ext-ecole-biomassa
ARTIFACT_REPO=ar-juridico-process-anp-datalake
COMPOSE_BUCKET_NAME=us-central1-composer-ecole--8a87d5fc-bucket

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
	python3 ./scripts/upload-docker-images.py

upload-dags:
	gsutil cp -r airflow/dags/* gs://$(COMPOSE_BUCKET_NAME)/dags/

upload-db:
	python3 ./scripts/upload_db.py

run-terraform-prod:
	cd terraform && terraform apply -var="is_prod=true" && cd ..

run-terraform-dev:
	cd terraform && terraform apply && cd ..

create-artifact-registry-prod:
	cd terraform && terraform apply -target=google_artifact_registry_repository.anp_repo_etl -var="is_prod=true" && cd ..

create-artifact-registry:
	cd terraform && terraform apply -target=google_artifact_registry_repository.anp_repo_etl && cd ..

