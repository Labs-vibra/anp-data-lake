import os

BUCKET_NAME = os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")
FILES_BUCKET_DESTINATION = "anp/multas_aplicadas_acoes_fiscalizacao"

BQ_DATASET = "rw_ext_anp"
BQ_TABLE_NAME = "multas_aplicadas_acoes_fiscalizacao"

BQ_TABLE = f"{BQ_DATASET}.{BQ_TABLE_NAME}"