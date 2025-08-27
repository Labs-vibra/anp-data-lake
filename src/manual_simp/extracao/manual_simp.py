import logging
from utils import (
    process_zip_and_upload_to_gcp,
    upload_bytes_to_bucket,
    download_file
)

from constants import (
    BUCKET_NAME,
    MANUAL_SIMP_URL,
    MANUAL_SIMP_ZIP_BUCKET_PATH,
    MANUAL_SIMP_EXTRACTION_BUCKET_PATH
)


logging.basicConfig(level=logging.INFO)

def extract_ext_anp_manual_simp():
    """
    Realiza a extração de arquivos da Tabela do i-SIMP:
    - Faz o download do ZIP e envia ao bucket
    - Extrai arquivos XLSX específicos e envia ao bucket
    """
    logging.info("Iniciando extração: Manual SIMP ANP...")
    logging.info(f"BUCKET_NAME {BUCKET_NAME}")
    logging.info(f"Mandando para o bucket de extração {MANUAL_SIMP_EXTRACTION_BUCKET_PATH}")

    zip_bytes = download_file(MANUAL_SIMP_URL)
    logging.info("Download concluído.")

    process_zip_and_upload_to_gcp(zip_bytes, MANUAL_SIMP_EXTRACTION_BUCKET_PATH)
    zip_bytes.seek(0)
    upload_bytes_to_bucket(zip_bytes, MANUAL_SIMP_ZIP_BUCKET_PATH)

    logging.info("Extração e upload de Manual SIMP concluída.")

if __name__ == "__main__":
    extract_ext_anp_manual_simp()
