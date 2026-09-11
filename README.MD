# anp-data-lake — Estrutura do projeto

Data lake de dados públicos da ANP rodando em GCP. Branch: `develop`.

## Como o projeto se organiza

O repositório é dividido por **responsabilidade técnica**, não por dataset. Cada
dataset da ANP aparece repetido em cinco lugares diferentes, um para cada etapa
do pipeline:

```
src/<dataset>/         → código da extração
terraform/modules/<x>/ → o Cloud Run Job que roda esse código
airflow/dags/<x>.py    → quem agenda e dispara o job
db/schemas/ e queries/ → as tabelas e transformações desse dado
mk/deploy.mk           → o target que faz o deploy de tudo isso junto
```

O fluxo entre eles: o script em `src/` vira imagem Docker → o Terraform cria um
Cloud Run Job apontando para essa imagem → a DAG do Airflow dispara o job e
depois roda o SQL que popula a camada seguinte no BigQuery.

Camadas do BigQuery: `rw_ext_anp` (raw) → `td_ext_anp` (trusted) → `rf_ext_anp` (refined).

---

# 1. `scripts/`

Quatro utilitários Python de build e deploy. Não fazem parte do ETL — são
chamados pelos targets do Makefile.

| Arquivo | Papel |
|---|---|
| `docker_images_config.py` | Catálogo central das imagens. Único lugar que mapeia nome da imagem → caminho do Dockerfile. Os outros scripts de docker importam daqui. |
| `upload-single-docker-image.py` | Builda e publica **uma** imagem no Artifact Registry. É o mais usado — todo deploy chama ele. |
| `upload-docker-images.py` | Mesma coisa, mas **todas** as imagens do catálogo, em paralelo. |
| `upload_db.py` | Executa os DDLs de `db/schemas/` no BigQuery. Aceita um arquivo específico ou roda uma lista pré-definida. |

A organização segue um padrão simples: **um arquivo de configuração + scripts
que consomem essa configuração**. Adicionar uma imagem nova significa registrá-la
no catálogo; os dois scripts de upload passam a enxergá-la automaticamente.

## Como usar

### Pré-requisitos

Os scripts esperam ser executados **a partir da raiz do repositório** — os
caminhos do catálogo (`./src/pmqc`, etc.) são relativos ao diretório atual, não
ao diretório do script.

```bash
# venv ativo (os targets do Makefile abortam sem isso)
source .venv/bin/activate
make install_deps

# autenticação no GCP
gcloud auth login
gcloud auth application-default login    # necessário para o upload_db.py
make configure-docker-gcp                # necessário para os scripts de docker
```

O `.env` na raiz define `GOOGLE_CLOUD_PROJECT` e `ARTIFACT_REPO`. Sem ele, os
scripts caem nos valores padrão do catálogo.

### `upload-single-docker-image.py`

```bash
# listar todas as imagens disponíveis, com label e caminho
python3 ./scripts/upload-single-docker-image.py --help

# buildar e publicar uma imagem
python3 ./scripts/upload-single-docker-image.py run-raw-pmqc-job

# equivalente, pelo Makefile
make upload-one-docker-image IMAGE=run-raw-pmqc-job
```

O argumento é sempre o campo `name` do catálogo, nunca o caminho da pasta nem o
label. Se o nome não existir, o script imprime a lista completa e sai com erro.
A saída do `docker build` aparece em tempo real.

### `upload-docker-images.py`

```bash
python3 ./scripts/upload-docker-images.py
# ou
make upload-docker
```

Não recebe argumentos: processa todo o catálogo de uma vez, em paralelo, com uma
linha de progresso por imagem. Como a saída do Docker é suprimida, uma falha
aparece só como erro genérico — nesse caso, rode a imagem isolada com o script
anterior para ver o log completo.

### `upload_db.py`

```bash
# executar um DDL específico (uso recomendado no dia a dia)
python3 ./scripts/upload_db.py db/schemas/raw/ddl_pmqc.sql
python3 ./scripts/upload_db.py db/schemas/trusted/ddl_pmqc.sql

# rodar o lote pré-definido dentro do script
make upload-db
```

Sem argumento, o script cria primeiro os datasets (arquivos `ddl_datasets*`) e
depois executa uma lista fixa declarada no topo do arquivo — que hoje cobre
poucos datasets. Para um DDL novo, passe o caminho como argumento em vez de
depender do lote.

Cada arquivo roda isolado: se um falhar, os demais continuam.

### Fluxo típico de um dataset novo

```bash
# 1. registrar a imagem no catálogo (editar docker_images_config.py)
# 2. publicar a imagem
make upload-one-docker-image IMAGE=run-raw-meu-dataset-job

# 3. criar as tabelas
python3 ./scripts/upload_db.py db/schemas/raw/ddl_meu_dataset.sql
python3 ./scripts/upload_db.py db/schemas/trusted/ddl_meu_dataset.sql

# 4. criar o Cloud Run Job (depois de declarar o módulo no terraform)
make run-terraform-prod

# 5. publicar DML e DAG
make upload-files
make upload-dags
```

Na prática, os passos 2 a 5 são o que os targets `deploy_<dataset>` do
`mk/deploy.mk` encapsulam — quando existe um target pronto, use ele.

---

# 2. `terraform/`

Provisiona o Artifact Registry e **um Cloud Run Job por processo de extração**.
Buckets, datasets do BigQuery e o Composer não são gerenciados aqui.

```
terraform/
├── provider.tf    # provider google + backend remoto no GCS
├── variables.tf   # variáveis e locals do projeto
├── main.tf        # artifact registry + chamada dos 14 módulos
├── outputs.tf     # (vazio)
└── modules/       # um diretório por domínio de dados
```

**Estrutura em dois níveis.** A raiz não declara jobs — ela só cria o Artifact
Registry e instancia os módulos. Todos os 14 módulos recebem exatamente os
mesmos três parâmetros (`region`, `jobs_image_base_url`, `image_version`), então
as chamadas em `main.tf` são idênticas entre si.

**Módulos são agrupados por domínio, não por job.** Um módulo pode conter de 1 a
7 jobs — `pmqc` tem um só, `metas_individuais_cbios` tem sete (um por ano de
2019 a 2025), `market_share` e `logistica` agrupam as várias etapas do mesmo
tema. Todo módulo tem apenas dois arquivos: `main.tf` com os jobs e um
`variables.tf` que é igual em todos.

**Os jobs são declarações mínimas.** Cada recurso define só nome, região e
imagem. Não há service account, limites de CPU/memória, timeout ou variáveis de
ambiente — isso tudo é injetado em runtime pela DAG do Airflow. São 33 recursos
declarados no total.

**Convenção de nomes:** `cr-juridico-{extracao|rw}-{dataset}-job-dev`, onde
`extracao` baixa o arquivo bruto e `rw`/`raw` carrega na camada raw. Esse nome é
a chave que as DAGs usam para disparar o job, então mudá-lo quebra o Airflow.

---

# 3. `Makefile` + `mk/`

O `Makefile` da raiz é só um agregador de 8 linhas: inclui o `.env` e os cinco
módulos de `mk/`. Toda a lógica está nos arquivos incluídos, separados por
finalidade:

| Arquivo | Escopo |
|---|---|
| `environment.mk` | Setup local — criar venv, instalar dependências |
| `docker.mk` | Build de imagens, autenticação no Artifact Registry, teste local de uma extração |
| `uploads.mk` | Envio de artefatos ao GCS — imagens, DAGs, SQLs, DDLs |
| `terraform.mk` | Wrappers finos sobre `terraform apply`, `validate` e `fmt` |
| `deploy.mk` | Deploy end-to-end, um target por dataset |

**`deploy.mk` é o centro da operação.** Cada dataset tem um target `deploy_<nome>`
que executa sempre as mesmas quatro etapas, nessa ordem:

1. builda e publica a imagem Docker
2. aplica o Cloud Run Job correspondente via Terraform
3. copia os SQLs (DDL raw, DDL trusted, DML trusted) para o bucket GCS
4. copia a DAG para o bucket do Composer

A etapa 3 é o que amarra o repositório ao Airflow: as DAGs baixam o SQL do GCS
em tempo de execução. Alterar um DML localmente não tem efeito nenhum sem rodar
o deploy.

Todos os targets de deploy dependem de `check_env`, um guard que aborta se não
houver virtualenv ativo.

**Não há CI.** Todo deploy é manual, rodado da máquina do desenvolvedor.

---

# 4. Deploy manual e criação do zero

Esta seção descreve a ordem em que as peças precisam existir. A regra geral é
que **cada passo depende do anterior já estar no ar**: o Terraform não consegue
criar um Cloud Run Job apontando para uma imagem que ainda não foi publicada, e
a DAG não consegue rodar um job que ainda não existe.

## 4.1 Bootstrap — uma vez por projeto

Antes de qualquer `terraform init` é preciso que o bucket do state exista. Ele é
o único recurso que o Terraform não pode criar para si mesmo:

```bash
gsutil mb -p ext-ecole-biomassa -l us-central1 gs://vibra-dtan-jur-anp-input
gsutil mb -p ext-ecole-biomassa -l us-central1 gs://vibra-dtan-jur-anp-input-dev
```

O primeiro bucket guarda o state do Terraform **e** os arquivos SQL; o segundo é
o bucket de DAGs do Composer.

> Nota: nenhum dos dois buckets é gerenciado pelo Terraform hoje. O
> `provider.tf` apenas os consome como backend. Se quiser trazê-los para o IaC,
> o caminho é declarar `google_storage_bucket` na raiz e importar com
> `terraform import` — nunca recriar, sob risco de perder o state.

Com os buckets no lugar:

```bash
cd terraform
terraform init          # conecta no backend GCS
cd ..
```

## 4.2 Passo 1 — Artifact Registry

O registry precisa existir antes de qualquer `docker push`. Ele é o único
recurso declarado na raiz do Terraform:

```bash
make create-artifact-registry-prod
# equivale a:
# cd terraform && terraform apply -target=google_artifact_registry_repository.anp_repo_etl -var="is_prod=true"
```

Em seguida, autentique o Docker local contra ele (uma vez por máquina):

```bash
make configure-docker-gcp
```

## 4.3 Passo 2 — Publicar a imagem

Registre a imagem no catálogo (`scripts/docker_images_config.py`), acrescentando
uma entrada ao grupo correspondente **e** garantindo que esse grupo esteja
incluído na lista `DOCKER_IMAGES` — vários grupos estão declarados mas ficaram
de fora dela, e nesse caso o script não encontra a imagem.

```python
MEU_DATASET_IMAGES = [
    {"label": "Extração Meu Dataset",
     "name": "run-raw-meu-dataset-job",
     "path": "./src/meu_dataset"},
]

DOCKER_IMAGES = [
    ...
    *MEU_DATASET_IMAGES,
]
```

Então publique:

```bash
make upload-one-docker-image IMAGE=run-raw-meu-dataset-job
```

Vale conferir no Artifact Registry antes de seguir — se a imagem não estiver lá,
o job do próximo passo sobe quebrado.

## 4.4 Passo 3 — Declarar o Cloud Run Job no Terraform

Crie o módulo em `terraform/modules/meu_dataset/` com os dois arquivos de sempre.

`variables.tf` é cópia idêntica de qualquer outro módulo:

```hcl
variable "region"              { type = string }
variable "jobs_image_base_url" { type = string }
variable "image_version"       { type = string }
```

`main.tf` declara um job por imagem:

```hcl
resource "google_cloud_run_v2_job" "raw_meu_dataset_job" {
  name     = "cr-juridico-rw-meu-dataset-job-dev"
  location = var.region

  template {
    template {
      containers {
        image = "${var.jobs_image_base_url}/run-raw-meu-dataset-job:${var.image_version}"
      }
    }
  }
}
```

O valor de `name` é a chave que a DAG vai usar para disparar o job — anote,
porque os dois precisam bater exatamente.

Registre o módulo em `terraform/main.tf`:

```hcl
module "meu_dataset" {
  source              = "./modules/meu_dataset"
  region              = var.region
  jobs_image_base_url = local.jobs_image_base_url
  image_version       = var.image_version
}
```

Valide antes de aplicar:

```bash
make validate-terraform    # roda validate + fmt
```

## 4.5 Passo 4 — Aplicar o job

Como o módulo é novo, é preciso reinicializar para o Terraform baixá-lo:

```bash
cd terraform
terraform init             # necessário sempre que se adiciona um módulo
terraform plan -var="is_prod=true"
cd ..
```

Aplicando só esse módulo:

```bash
cd terraform && terraform apply -target=module.meu_dataset -var="is_prod=true" && cd ..
```

Ou tudo de uma vez:

```bash
make run-terraform-prod
```

> **Atenção ao endereço do `-target`.** Como todos os jobs vivem dentro de
> módulos, o endereço correto é `module.<nome_do_modulo>`. Vários targets
> antigos do `mk/deploy.mk` ainda usam a forma de raiz
> (`-target=google_cloud_run_v2_job.<recurso>`), herdada de antes da
> modularização. Esse endereço não resolve, e o Terraform aplica **nada** sem
> falhar — o deploy parece ter dado certo. Ao criar um target novo, use sempre
> a forma com `module.`.

## 4.6 Passo 5 — Publicar SQL e DAG

Os dois últimos artefatos não vivem no repositório em runtime: são lidos do GCS
pelo Airflow durante a execução.

```bash
# DDLs — criam as tabelas no BigQuery
python3 ./scripts/upload_db.py db/schemas/raw/ddl_meu_dataset.sql
python3 ./scripts/upload_db.py db/schemas/trusted/ddl_meu_dataset.sql

# os mesmos DDLs versionados no bucket
gsutil cp ./db/schemas/raw/ddl_meu_dataset.sql     gs://vibra-dtan-jur-anp-input/sql/schemas/raw
gsutil cp ./db/schemas/trusted/ddl_meu_dataset.sql gs://vibra-dtan-jur-anp-input/sql/schemas/trusted

# DML de transformação — é o arquivo que a DAG baixa e executa
gsutil cp ./db/queries/trusted/dml_td_meu_dataset.sql gs://vibra-dtan-jur-anp-input/sql/queries/trusted

# DAG
gsutil cp airflow/dags/meu_dataset.py gs://vibra-dtan-jur-anp-input-dev/dags/
```

O Composer leva alguns minutos para detectar a DAG nova. Confira no Airflow se
ela apareceu sem erro de import antes de dar o passo por concluído.

## 4.7 Passo 6 — Automatizar o deploy

Com tudo funcionando, crie o target em `mk/deploy.mk` seguindo o padrão dos
demais, para que os próximos deploys sejam um comando só:

```makefile
deploy_meu_dataset: check_env
	make upload-one-docker-image IMAGE=run-raw-meu-dataset-job
	cd terraform && terraform apply -target=module.meu_dataset -var="is_prod=true" && cd ..
	gsutil cp ./db/schemas/raw/ddl_meu_dataset.sql        gs://$(BUCKET_NAME)/sql/schemas/raw
	gsutil cp ./db/schemas/trusted/ddl_meu_dataset.sql    gs://$(BUCKET_NAME)/sql/schemas/trusted
	gsutil cp ./db/queries/trusted/dml_td_meu_dataset.sql gs://$(BUCKET_NAME)/sql/queries/trusted
	gsutil cp airflow/dags/meu_dataset.py                 gs://$(COMPOSE_BUCKET_NAME)/dags/
```

Confira os nomes de arquivo com atenção — vários targets existentes apontam para
SQLs e DAGs cujo nome divergiu do real (hífen vs. underscore, singular vs.
plural) e falham no `gsutil cp`.

## 4.8 Deploy do dia a dia

Depois da primeira criação, o que muda define o que precisa rodar:

| O que mudou | O que rodar |
|---|---|
| Só o código em `src/` | `make upload-one-docker-image IMAGE=<nome>` — a tag é `latest`, o job pega a imagem nova na próxima execução |
| Só um DML em `db/queries/` | `gsutil cp` do arquivo para o bucket |
| Estrutura de tabela em `db/schemas/` | `upload_db.py` + `gsutil cp` |
| Só a DAG | `gsutil cp` para o bucket do Composer |
| Configuração do job no Terraform | `terraform apply -target=module.<nome>` |
| Qualquer combinação | `make deploy_<dataset>` |

Como as imagens usam tag `latest` e não versionamento explícito, republicar a
imagem é suficiente para atualizar o comportamento do job — não é preciso
reaplicar o Terraform. O efeito colateral é que não há rollback de imagem: para
voltar atrás, o caminho é republicar a versão anterior do código.

---

# 5. Demais diretórios

## `src/`
Código das extrações — um subdiretório por dataset da ANP (15 no total). Cada um
é autocontido: tem seu próprio `Dockerfile`, `requirements.txt` e `constants.py`
com as URLs e mapeamentos daquele dataset. Datasets mais complexos se subdividem
em `extracao/` (baixa o bruto para o GCS) e `raw/` (carrega no BigQuery). Cada
pasta vira uma imagem Docker e um Cloud Run Job.

## `airflow/`
Orquestração. `dags/` tem uma DAG por dataset, todas construídas sobre helpers
compartilhados em `dags/utils/` que abstraem "dispare este Cloud Run Job" e
"rode este SQL no BigQuery". `image/` guarda o Dockerfile da imagem customizada
do Composer. `logs/` são logs de execução local commitados por engano.

## `db/`
O SQL do data lake, versionado e espelhando as camadas do BigQuery.
`schemas/` tem os DDLs separados em `raw/`, `trusted/` e `refined/`; `queries/`
tem os DMLs de transformação entre camadas. Esses arquivos não rodam do repo —
são publicados no GCS e lidos pelo Airflow em runtime.

## `.github/`
Templates de issue (incluindo um específico para novos pipelines de ETL) e dois
workflows de Terraform (`plan` e `apply`) que estão inteiramente comentados.

## Raiz
`docker-compose.yml` levanta o ambiente local (Airflow standalone + Postgres +
emulador de BigQuery). `requirements.txt` cobre as dependências dos scripts e do
desenvolvimento. O `.gitignore` ignora credenciais, `.env`, state do Terraform —
e também `README.md`, o que explica a ausência de documentação no repositório.
