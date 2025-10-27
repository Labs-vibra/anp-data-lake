
upload-docker:
	python3 ./scripts/upload-docker-images.py

upload-dags:
	gsutil cp -r airflow/dags/* gs://us-central1-composer-ecole--8a87d5fc-bucket/dags/

upload-files:
	gsutil cp -r db/queries/* gs://$(BUCKET_NAME)/sql/

upload-db:
	python3 ./scripts/upload_db.py

upload-market-share:
	gsutil cp airflow/dags/market_share.py gs://$(COMPOSE_BUCKET_NAME)/dags/
