from dotenv import load_dotenv
# from src.extractions.logistica.logistica import extract_ext_anp_logistics
from metas_individuais_cbios.cbios_2022 import rw_ext_anp_cbios_2022

load_dotenv()

def execute_logistics_pipeline():
    # extract_ext_anp_logistics()
    rw_ext_anp_cbios_2022()

execute_logistics_pipeline()
