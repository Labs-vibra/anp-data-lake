# BUCKET_NAME=vibra-dtan-jur-anp-input
BUCKET_NAME=labs-vibra-data-bucket
COMPOSE_BUCKET_NAME=vibra-dtan-jur-anp-input-dev-2

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


deploy_consulta_bases_distribuicao_trr_autorizados: check_env
	@echo "Deploying Consulta de Bases de Distribuição e TRR Autorizados application..."
	make upload-one-docker-image IMAGE=run-consulta-bases-de-distribuicao-e-trr-autorizados-job
	cd terraform && terraform apply -target=google_cloud_run_v2_job.extracao_consulta_bases_de_distribuicao_e_trr_autorizados -var="is_prod=true" && cd ..
	@echo "uploading files to GCS..."
	gsutil cp ./db/schemas/raw/ddl_consulta_bases_distribuicao_trr_autorizados.sql gs://$(BUCKET_NAME)/sql/schemas/raw
	gsutil cp ./db/schemas/trusted/ddl_consulta_bases_distribuicao_trr_autorizados.sql gs://$(BUCKET_NAME)/sql/schemas/trusted
	gsutil cp ./db/queries/trusted/dml_td_consulta_bases_distribuicao_trr_autorizados.sql gs://$(BUCKET_NAME)/sql/queries/trusted
	@ echo "uploading dags to GCS..."
	gsutil cp airflow/dags/consulta_bases_de_distribuicao_e_trr_autorizados.py gs://$(COMPOSE_BUCKET_NAME)/dags/

	@echo "Consulta de Bases de Distribuição e TRR Autorizados application deployed successfully."

deploy_postos_revendedores: check_env
	@echo "Deploying Postos Revendedores application..."
	make upload-one-docker-image IMAGE=run-postos-revendedores-job
	cd terraform && terraform apply -target=google_cloud_run_v2_job.raw_postos_revendedores_job -var="is_prod=true" && cd ..
	@echo "uploading files to GCS..."
	gsutil cp ./db/schemas/raw/ddl_postos_revendedores.sql gs://$(BUCKET_NAME)/sql/schemas/raw
	gsutil cp ./db/schemas/trusted/ddl_postos_revendedores.sql gs://$(BUCKET_NAME)/sql/schemas/trusted
	gsutil cp ./db/queries/trusted/dml_td_postos_revendedores.sql gs://$(BUCKET_NAME)/sql/queries/trusted
	@ echo "uploading dags to GCS..."
	gsutil cp airflow/dags/postos-revendedores.py gs://$(COMPOSE_BUCKET_NAME)/dags/

	@echo "Postos Revendedores application deployed successfully."

deploy_vendas_combustiveis_segmento: check_env
	@echo "Deploying Vendas de Combustíveis por Segmento application..."
	make upload-one-docker-image IMAGE=run-extracao-vendas-comb-segmento-job
	cd terraform && terraform apply -target=google_cloud_run_v2_job.extracao_vendas-comb-segmento_job -var="is_prod=true" && cd ..
	@echo "uploading files to GCS..."
	gsutil cp ./db/schemas/raw/ddl_vendas_comb_segmento.sql gs://$(BUCKET_NAME)/sql/schemas/raw
	gsutil cp ./db/schemas/trusted/ddl_vendas_comb_segmento.sql gs://$(BUCKET_NAME)/sql/schemas/trusted
	gsutil cp ./db/queries/trusted/dml_td_vendas_comb_segmento.sql gs://$(BUCKET_NAME)/sql/queries/trusted
	@ echo "uploading dags to GCS..."
	gsutil cp airflow/dags/vendas-comb-segmento.py gs://$(COMPOSE_BUCKET_NAME)/dags/

	@echo "Vendas de Combustíveis por Segmento application deployed successfully."

deploy_dados_fiscalizacao_do_abastecimento: check_env
	@echo "Deploying Dados de Fiscalização do Abastecimento application..."
	make upload-one-docker-image IMAGE=run-raw-dados-fiscalizacao-do-abastecimento-job
	cd terraform && terraform apply -target=google_cloud_run_v2_job.raw_dados_fiscalizacao_do_abastecimento_job -var="is_prod=true" && cd ..
	@echo "uploading files to GCS..."
	gsutil cp ./db/schemas/raw/ddl_dados_fiscalizacao_do_abastecimento.sql gs://$(BUCKET_NAME)/sql/schemas/raw
	gsutil cp ./db/schemas/trusted/ddl_dados_fiscalizacao_abastecimento.sql gs://$(BUCKET_NAME)/sql/schemas/trusted
	gsutil cp ./db/queries/trusted/dml_td_dados_fiscalizacao_abastecimento.sql gs://$(BUCKET_NAME)/sql/queries/trusted
	@ echo "uploading dags to GCS..."
	gsutil cp airflow/dags/dados_fiscalizacao_do_abastecimento.py gs://$(COMPOSE_BUCKET_NAME)/dags/

	@echo "Dados de Fiscalização do Abastecimento application deployed successfully."

deploy_contratos_cessao: check_env
	@echo "Deploying Contratos de Cessão de Espaço ou Carregamento application..."
	make upload-one-docker-image IMAGE=run-extracao-contratos-cessao-job
	cd terraform && terraform apply -target=google_cloud_run_v2_job.extracao_contratos_cessao_job -var="is_prod=true" && cd ..
	@echo "uploading files to GCS..."
	gsutil cp ./db/schemas/raw/ddl_contratos_cessao.sql gs://$(BUCKET_NAME)/sql/schemas/raw
	gsutil cp ./db/schemas/trusted/ddl_contratos_cessao.sql gs://$(BUCKET_NAME)/sql/schemas/trusted
	gsutil cp ./db/queries/trusted/dml_td_contratos_cessao.sql gs://$(BUCKET_NAME)/sql/queries/trusted
	@ echo "uploading dags to GCS..."
	gsutil cp airflow/dags/contratos_cessao.py gs://$(COMPOSE_BUCKET_NAME)/dags/

	@echo "Contratos de Cessão de Espaço ou Carregamento application deployed successfully."

#### ------ Faltando ------ ####

# Produção de Biocombstíveis - Biodiesel (Barris)
# Contratos de cessão de espaço ou carregamento
# Trusted Distribuidores de Combustíveis Líquidos Autorizados ao Exercício da Atividade
# [PRIO]Tancagem do Abastecimento Nacional de Combustíveis - 2022 a 2025 (+ Parceiro)
# Multas aplicadas decorrentes de autuações em ações de fiscalização - 2019 a 2024
