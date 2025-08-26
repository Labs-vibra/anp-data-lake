import os
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME= os.getenv("BUCKET_NAME", "vibra-dtan-jur-anp-input")

MANUAL_SIMP_URL = "https://csa.anp.gov.br/downloads/manuais-isimp/Geracao-de-tabelas-ISIMP.zip"

MANUAL_SIMP_ZIP_BUCKET_PATH = "anp/simp/manual_simp.zip"

MANUAL_SIMP_EXTRACTION_BUCKET_PATH = "anp/simp/"

MANUAL_SIMP_XLSX_FILENAME_KEYWORD = "MANUAL_SIMP"

MANUAL_SIMP_XLSX_ALLOWED_NUMBERS = [
	"001","002", "003",
	"004", "005", "006",
	"007", "008", "009",
	"010", "011", "012",
	"013", "014", "016",
	"017", "018", "020",
	"021", "022", "023"
]

MANUAL_SIMP_XLSX_EXTENSION = ".XLSX"
