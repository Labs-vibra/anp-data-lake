import os

URL = (
	"https://www.gov.br/anp/pt-br/assuntos/renovabio/metas/2019/metas-individuais-compulsorias-2019.xlsx"
)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

EXTRACTION_DIR = os.path.join(BASE_DIR, "extracted")

os.makedirs(EXTRACTION_DIR, exist_ok=True)