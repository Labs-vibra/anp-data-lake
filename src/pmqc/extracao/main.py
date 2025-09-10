import requests
from io import BytesIO
import logging

from constants import ZIP_URL, BUCKET_PATH
from utils import process_zip_and_upload_to_gcp

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def download_and_extract_pmqc_data():
    """
    Baixa o arquivo ZIP da URL da ANP e extrai os arquivos para o Cloud Storage.

#     Returns:
#         bool: True se o processo foi bem-sucedido, False caso contrário
#     """
    try:
        logger.info(f"Iniciando download do arquivo ZIP da URL: {ZIP_URL}")

        # Fazer o download do arquivo ZIP
        response = requests.get(ZIP_URL, timeout=300)  # 5 minutos de timeout
        response.raise_for_status()  # Levanta exceção se houver erro HTTP

        logger.info(f"Download concluído. Tamanho do arquivo: {len(response.content)} bytes")

        # Converter o conteúdo para BytesIO
        zip_bytes = BytesIO(response.content)

        logger.info(f"Processando arquivos ZIP e enviando para: {BUCKET_PATH}")

        # Processar o ZIP e fazer upload dos arquivos
        process_zip_and_upload_to_gcp(zip_bytes, BUCKET_PATH)

        logger.info("Processo de extração e upload concluído com sucesso!")
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao baixar o arquivo: {e}")
        return False
    except Exception as e:
        logger.error(f"Erro durante o processamento: {e}")
        return False

def main():
    """
    Função principal que executa a extração dos dados de PMQC.
    """
    logger.info("=== Iniciando extração de dados de PMQC da ANP ===")

    success = download_and_extract_pmqc_data()

    if success:
        logger.info("=== Processo finalizado com sucesso! ===")
        exit(0)
    else:
        logger.error("=== Processo finalizado com erro! ===")
        exit(1)

if __name__ == "__main__":
    main()
