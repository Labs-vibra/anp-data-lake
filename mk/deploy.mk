BUCKET_NAME=vibra-dtan-jur-anp-input
COMPOSE_BUCKET_NAME=vibra-dtan-jur-anp-input-dev

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
	make upload-one-docker-image IMAGE=run_extracao_consulta_bases_de_distribuicao_e_trr_autorizados
	make upload-one-docker-image IMAGE=run-raw-consulta-bases-de-distribuicao-e-trr-autorizados-job
	cd terraform && terraform plan -var="is_prod=true" && cd ..
	cd terraform && terraform apply -target=google_cloud_run_v2_job.extracao_consulta_bases_de_distribuicao_e_trr_autorizados -var="is_prod=true" && cd ..
	cd terraform && terraform plan -var="is_prod=true" && cd ..
	cd terraform && terraform apply -target=google_cloud_run_v2_job.raw_consulta_bases_de_distribuicao_e_trr_autorizados -var="is_prod=true" && cd ..
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

producao_de_biocombustiveis_biodiesel_barris: check_env
	@echo "Deploying Produção de Biocombustíveis - Biodiesel em Barris application..."
	make upload-one-docker-image IMAGE=run-producao-de-biocombustiveis-biodiesel-em-barris-job
	cd terraform && terraform apply -target=google_cloud_run_v2_job.producao_de_biocombustiveis_biodiesel_em_barris_job -var="is_prod=true" && cd ..
	@echo "uploading files to GCS..."
	gsutil cp ./db/schemas/raw/ddl_producao_de_biocombustiveis_biodiesel_em_barris.sql gs://$(BUCKET_NAME)/sql/schemas/raw
	gsutil cp ./db/schemas/trusted/ddl_producao_de_biocombustiveis_biodiesel_em_barris.sql gs://$(BUCKET_NAME)/sql/schemas/trusted
	gsutil cp ./db/queries/trusted/dml_td_producao_de_biocombustiveis_biodiesel_em_barris.sql gs://$(BUCKET_NAME)/sql/queries/trusted
	@ echo "uploading dags to GCS..."
	gsutil cp airflow/dags/producao_de_biocombustiveis_biodiesel_em_barris.py gs://$(COMPOSE_BUCKET_NAME)/dags/

	@echo "Produção de Biocombustíveis - Biodiesel em Barris application deployed successfully."


contratos_de_cessao_de_espaço_ou_carregamento: check_env
	@echo "Deploying Contratos de Cessão de Espaço ou Carregamento application..."
	make upload-one-docker-image IMAGE=run-contratos-de-cessao-de-espaco-ou-carregamento-job
	cd terraform && terraform apply -target=google_cloud_run_v2_job.contratos_de_cessao_de_espaco_ou_carregamento_job -var="is_prod=true" && cd ..
	@echo "uploading files to GCS..."
	gsutil cp ./db/schemas/raw/ddl_contratos_de_cessao_de_espaco_ou_carregamento.sql gs://$(BUCKET_NAME)/sql/schemas/raw
	gsutil cp ./db/schemas/trusted/ddl_contratos_de_cessao_de_espaco_ou_carregamento.sql gs://$(BUCKET_NAME)/sql/schemas/trusted
	gsutil cp ./db/queries/trusted/dml_td_contratos_de_cessao_de_espaco_ou_carregamento.sql gs://$(BUCKET_NAME)/sql/queries/trusted
	@ echo "uploading dags to GCS..."
	gsutil cp airflow/dags/contratos_de_cessao_de_espaco_ou_carregamento.py gs://$(COMPOSE_BUCKET_NAME)/dags/

	@echo "Contratos de Cessão de Espaço ou Carregamento application deployed successfully."

distribuidores_de_combustíveis_liquidos_autorizados_ao_exercicio_da_atividade: check_env
	@echo "Deploying Distribuidores de Combustíveis Líquidos Autorizados ao Exercício da Atividade application..."
	make upload-one-docker-image IMAGE=run-distribuidores-de-combustiveis-liquidos-autorizados-ao-exercicio-da-atividade-job
	cd terraform && terraform apply -target=google_cloud_run_v2_job.distribuidores_de_combustiveis_liquidos_autorizados_ao_exercicio_da_atividade_job -var="is_prod=true" && cd ..
	@echo "uploading files to GCS..."
	gsutil cp ./db/schemas/raw/ddl_distribuidores_de_combustiveis_liquidos_autorizados_ao_exercicio_da_atividade.sql gs://$(BUCKET_NAME)/sql/schemas/raw
	gsutil cp ./db/schemas/trusted/ddl_distribuidores_de_combustiveis_liquidos_autorizados_ao_exercicio_da_atividade.sql gs://$(BUCKET_NAME)/sql/schemas/trusted
	gsutil cp ./db/queries/trusted/dml_td_distribuidores_de_combustiveis_liquidos_autorizados_ao_exercicio_da_atividade.sql gs://$(BUCKET_NAME)/sql/queries/trusted
	@ echo "uploading dags to GCS..."
	gsutil cp airflow/dags/distribuidores_de_combustiveis_liquidos_autorizados_ao_exercicio_da_atividade.py gs://$(COMPOSE_BUCKET_NAME)/dags/

	@echo "Distribuidores de Combustíveis Líquidos Autorizados ao Exercício da Atividade application deployed successfully."


tancagem_do_abastecimento_nacional_de_combustíveis_2022_2025: check_env
	@echo "Deploying Tancagem do Abastecimento Nacional de Combustíveis 2022 a 2025 application..."
	make upload-one-docker-image IMAGE=run-tancagem-do-abastecimento-nacional-de-combustiveis-2022-2025-job
	cd terraform && terraform apply -target=google_cloud_run_v2_job.tancagem_do_abastecimento_nacional_de_combustiveis_2022_2025_job -var="is_prod=true" && cd ..
	@echo "uploading files to GCS..."
	gsutil cp ./db/schemas/raw/ddl_tancagem_do_abastecimento_nacional_de_combustiveis_2022_2025.sql gs://$(BUCKET_NAME)/sql/schemas/raw
	gsutil cp ./db/schemas/trusted/ddl_tancagem_do_abastecimento_nacional_de_combustiveis_2022_2025.sql gs://$(BUCKET_NAME)/sql/schemas/trusted
	gsutil cp ./db/queries/trusted/dml_td_tancagem_do_abastecimento_nacional_de_combustiveis_2022_2025.sql gs://$(BUCKET_NAME)/sql/queries/trusted
	@ echo "uploading dags to GCS..."
	gsutil cp airflow/dags/tancagem_do_abastecimento_nacional_de_combustiveis_2022_2025.py gs://$(COMPOSE_BUCKET_NAME)/dags/

	@echo "Tancagem do Abastecimento Nacional de Combustíveis 2022 a 2025 application deployed successfully."

deploy_multas_aplicadas_acoes_fiscalizacao: check_env
	@echo "Deploying Multas Aplicadas e Ações de Fiscalização application..."
	@echo "Uploading Extração Docker image..."
	make upload-one-docker-image IMAGE=cr-juridico-raw-multas-aplicadas-acoes-fiscalizacao-job
	@echo "Uploading Raw Docker image..."
	make upload-one-docker-image IMAGE=cr-juridico-raw-multas-aplicadas-job
	@echo "Applying Terraform configuration..."
	cd terraform && terraform apply -target=module.multas_aplicadas_acoes_fiscalizacao -var="is_prod=true" && cd ..
	@echo "Uploading SQL files to GCS..."
	gsutil cp ./db/schemas/raw/ddl_multas_aplicadas_acoes_fiscalizacao.sql gs://$(BUCKET_NAME)/sql/schemas/raw
	gsutil cp ./db/schemas/trusted/ddl_multas_aplicadas_acoes_fiscalizacao.sql gs://$(BUCKET_NAME)/sql/schemas/trusted
	gsutil cp ./db/queries/trusted/dml_td_multas_aplicadas_acoes_fiscalizacao.sql gs://$(BUCKET_NAME)/sql/queries/trusted
	@echo "Uploading DAG to GCS..."
	gsutil cp airflow/dags/multas_aplicadas_acoes_fiscalizacao.py gs://$(COMPOSE_BUCKET_NAME)/dags/
	@echo "Multas Aplicadas e Ações de Fiscalização application deployed successfully."
