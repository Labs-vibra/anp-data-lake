from io import BytesIO
import zipfile
import logging
from utils import download_file, fetch_html, find_link_by_text, process_zip_and_upload_to_gcp, upload_bytes_to_bucket
from constants import LOGISTICS_URL, LOGISTICS_ZIP_BUCKET_PATH, LOGISTICS_EXTRACTION_BUCKET_PATH

logging.basicConfig(level=logging.INFO)

def extract_ext_anp_logistics(url: str = LOGISTICS_URL):
    """
    Realiza a extração de arquivos de logística da ANP:
    - Baixa o HTML da página
    - Encontra o link para o arquivo ZIP
    - Faz o download do ZIP e envia ao bucket
    - Extrai arquivos CSV específicos e envia ao bucket

    Args:
        url (str): URL da página onde o link para o arquivo ZIP será buscado.
                   Default é LOGISTICS_URL.
    """
    logging.info("Iniciando extração: Logística ANP...")

    soup = fetch_html(url)
    data_info = find_link_by_text(soup, "Veja também a base dados do painel")

    if not data_info:
        raise RuntimeError("Link para download dos dados não encontrado.")

    logging.info(f"Link para dados encontrado: {data_info['link']}")
    logging.info(f"Data da última atualização: {data_info['updated_date']}")

    zip_bytes = download_file(data_info['link'])
    upload_bytes_to_bucket(zip_bytes, LOGISTICS_ZIP_BUCKET_PATH)

    process_zip_and_upload_to_gcp(zip_bytes, LOGISTICS_EXTRACTION_BUCKET_PATH)

    logging.info("Extração e upload de Logística concluída.")

if __name__ == "__main__":
    extract_ext_anp_logistics()
