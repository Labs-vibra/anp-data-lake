from io import BytesIO
import zipfile
from src.services.gcp.gcs import upload_bytes_to_bucket
from src.utils.scrapping_utils import download_file, fetch_html, find_link_by_text

URL = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/paineis-dinamicos-da-anp/"
    "paineis-dinamicos-do-abastecimento/painel-dinamico-da-logistica-do-abastecimento-nacional-de-combustiveis"
)

target_files = [
    "DADOS ABERTOS - LOGISTICA 01 - ABASTECIMENTO NACIONAL DE COMBUST÷VEIS",
    "DADOS ABERTOS - LOGISTICA 02 - VENDAS NO MERCADO BRASILEIRO DE COMBUST÷VEIS",
    "DADOS ABERTOS - LOGISTICA 03 - VENDAS CONG╥NERES DE DISTRIBUIDORES.csv",
]

def rw_ext_anp_logistics(
    url: str = URL,
    target_files_list: list[str] = target_files,
):
    print("Iniciando extração: Logística ANP...")

    soup = fetch_html(url)

    data_info = find_link_by_text(soup, "Veja também a base dados do painel")
    if not data_info:
        raise Exception("Link para download dos dados não encontrado.")
    print(f"Link para dados encontrado: {data_info['link']}")
    print(f"Data da última atualização: {data_info['updated_date']}")

    zip_bytes = download_file(data_info['link'])

    with zipfile.ZipFile(zip_bytes) as zf:
        print("\nEnviando arquivos diretamente para o bucket...")
        for file_info in zf.infolist():
            file_name_upper = file_info.filename.upper()
            if ("LOGISTICA" in file_name_upper and
                ("01" in file_name_upper or "02" in file_name_upper or "03" in file_name_upper) and
                file_name_upper.endswith('.CSV')):
                with zf.open(file_info) as source_file:
                    file_bytes = BytesIO(source_file.read())
                    bucket_path = f"extractions/{file_info.filename}"
                    upload_bytes_to_bucket(file_bytes, bucket_path)
                    print(f"Arquivo enviado para o bucket: {bucket_path}")

    print("Extração e upload de Logística concluída.")
