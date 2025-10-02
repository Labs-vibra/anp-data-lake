BUCKET_NAME=vibra-dtan-jur-anp-input
COMPOSE_BUCKET_NAME=us-central1-composer-ecole--8a87d5fc-bucket

check_env:
	@if [ -z "$$VIRTUAL_ENV" ]; then \
		echo "Error: Please activate your virtual environment with 'source venv/bin/activate'."; \
		exit 1; \
	fi

deploy_pmqc: check_env
	@echo "Deploying PMQC application..."
	make upload-one-docker-image IMAGE=run-raw-pmqc-job
	cd terraform && terraform apply -target=google_cloud_run_v2_job.raw_pmqc_job -var="is_prod=true" && cd ..
	@echo "uploading files to GCS..."
	gsutil cp ./db/schemas/raw/ddl_pmqc.sql gs://$(BUCKET_NAME)/sql/schemas/raw
	gsutil cp ./db/schemas/trusted/ddl_pmqc.sql gs://$(BUCKET_NAME)/sql/schemas/trusted
	gsutil cp ./db/queries/trusted/dml_td_pmqc.sql gs://$(BUCKET_NAME)/sql/queries/trusted
	@ echo "uploading dags to GCS..."
	gsutil cp airflow/dags/pmqc.py gs://$(COMPOSE_BUCKET_NAME)/dags/

	@echo "PMQC application deployed successfully."


