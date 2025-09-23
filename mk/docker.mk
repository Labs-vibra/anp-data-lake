test-docker-image:
	cp ~/gcp.secrets.json src/$(FOLDER)
	docker build --no-cache -t extracao src/$(FOLDER)
	rm src/$(FOLDER)/gcp.secrets.json

run-test-docker-image: test-docker-image
	docker run --env-file ./.env extracao:latest

deploy-sprint-3-images:
	make upload-one-docker-image IMAGE=run-extracao-market-share-job
	make upload-one-docker-image IMAGE=run-raw-distribuidor-atual
	make upload-one-docker-image IMAGE=run-raw-importacao-distribuidores
	make upload-one-docker-image IMAGE=run-raw-liquidos-historico-entregas
	make upload-one-docker-image IMAGE=run-raw-vendas-atual
	make upload-one-docker-image IMAGE=run-raw-entregas-fornecedor-atual
	make upload-one-docker-image IMAGE=run-raw-liquidos-historico-vendas
	make upload-one-docker-image IMAGE=run-extracao-aposentadoria-cbios-job

build-airflow-image:
	docker build --no-cache -t composer-airflow-image ./airflow/image

configure-docker-gcp:
	gcloud auth configure-docker us-central1-docker.pkg.dev
	gcloud config set project $(GOOGLE_CLOUD_PROJECT)

upload-one-docker-image:
	python3 ./scripts/upload-single-docker-image.py $(IMAGE)
