
upload-docker:
	python3 ./scripts/upload-docker-images.py

upload-dags:
	gsutil cp -r airflow/dags/* gs://$(COMPOSE_BUCKET_NAME)/dags/

upload-files:
	gsutil cp -r db/queries/* gs://$(BUCKET_NAME)/sql/

upload-db:
	python3 ./scripts/upload_db.py
