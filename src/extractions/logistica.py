import os
import zipfile
from services.constants import PATHS
from services.utils import (
    fetch_html,
    find_link_by_text,
    download_file,
    save_zip_file,
)

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

    os.makedirs(PATHS["EXTRACTION_DIR"], exist_ok=True)
    zip_file_path = os.path.join(PATHS["EXTRACTION_DIR"], "logistics.zip")
    save_zip_file(zip_bytes, zip_file_path)
    with zipfile.ZipFile(zip_file_path, 'r') as zf:

        print("\nExtraindo arquivos...")
        for file_info in zf.infolist():
            file_name_upper = file_info.filename.upper()
            if ("LOGISTICA" in file_name_upper and
                ("01" in file_name_upper or "02" in file_name_upper or "03" in file_name_upper) and
                file_name_upper.endswith('.CSV')):
                output_file_path = os.path.join(PATHS["EXTRACTION_DIR"], file_info.filename)
                with zf.open(file_info) as source_file, open(output_file_path, 'wb') as dest_file:
                    dest_file.write(source_file.read())
                print(f"Arquivo extraído: {output_file_path}")

    print("Extração de Logística concluída.")
