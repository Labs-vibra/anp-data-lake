import os
import requests
from io import BytesIO
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from constants import URL_BASE, BUCKET_PATH
from utils import download_file, fetch_html, find_all_csv_links, normalize_filename, upload_bytes_to_bucket

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def upload_csv_to_gcp(link: str):
    """
    Faz download de um CSV da ANP e envia para o GCP usando upload_bytes_to_bucket,
    normalizando o nome do arquivo para pmqc_YYYY_MM.csv.
    """
    try:
        original_filename = link.split("/")[-1]

        # Determina o ano
        if original_filename[:4].isdigit():
            year = original_filename[:4]
        elif link.split("/")[-2].isdigit():
            year = link.split("/")[-2]
        else:
            year = "unknown"

        filename = normalize_filename(original_filename, year)
        bucket_file_path = f"{BUCKET_PATH}{filename}"

        logger.info(f"Baixando arquivo: {link}")
        response = requests.get(link, timeout=600, stream=True, verify=False)
        response.raise_for_status()

        file_bytes = BytesIO(response.content)
        upload_bytes_to_bucket(file_bytes, bucket_file_path)
        logger.info(f"Arquivo enviado para GCP: {bucket_file_path}")

    except Exception as e:
        logger.error(f"Erro ao processar {link}: {e}")

def download_pmqc_data(parallel: bool = True, max_workers: int = 5):
    """
    Busca todos os CSVs de PMQC na página da ANP, baixa e envia para o GCP.
    Se parallel=True, os uploads são feitos em threads paralelas.
    """
    try:
        logger.info("Buscando HTML da página de PMQC...")
        soup = fetch_html(URL_BASE)

        csv_links = find_all_csv_links(soup)
        if not csv_links:
            logger.error("Nenhum link CSV encontrado na página.")
            return False

        logger.info(f"{len(csv_links)} arquivos CSV encontrados.")

        if parallel:
            # Executa uploads em paralelo
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(upload_csv_to_gcp, link): link for link in csv_links}
                for future in as_completed(futures):
                    # Apenas para capturar exceções que não foram logadas
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Erro em thread para {futures[future]}: {e}")
        else:
            # Executa sequencialmente
            for link in csv_links:
                upload_csv_to_gcp(link)

        logger.info("Todos os CSVs processados e enviados para o GCP com sucesso!")
        return True

    except Exception as e:
        logger.error(f"Erro ao baixar ou enviar os arquivos: {e}")
        return False

def main():
    """
    Função principal que executa a extração dos dados de market share.
    """
    logger.info("=== Iniciando extração de dados de Market Share da ANP ===")

    success = download_pmqc_data(parallel=True, max_workers=10)  # aumenta a velocidade com 10 threads

    if success:
        logger.info("=== Processo finalizado com sucesso! ===")
        exit(0)
    else:
        logger.error("=== Processo finalizado com erro! ===")
        exit(1)

if __name__ == "__main__":
    main()
