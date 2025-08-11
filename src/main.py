from dotenv import load_dotenv
from extractions.logistica import rw_ext_anp_logistics

load_dotenv()

def execute_logistics_pipeline():
    rw_ext_anp_logistics()

execute_logistics_pipeline()
