from dotenv import load_dotenv
from src.extractions.logistica.logistica import extract_ext_anp_logistics

load_dotenv()

def execute_logistics_pipeline():
    extract_ext_anp_logistics()

execute_logistics_pipeline()
