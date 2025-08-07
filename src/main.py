from flask import Flask, Response
from dotenv import load_dotenv
import json

from extractions.logistica import rw_ext_anp_logistics
from db.raw.rw_ext_anp_cbios_2019 import rw_ext_anp_cbios_2019

load_dotenv()
app = Flask(__name__)

def execute_logistics_pipeline():
    # rw_ext_anp_logistics()
    rw_ext_anp_cbios_2019()

@app.route("/")
def entrypoint():
    try:
        execute_logistics_pipeline()
        response = json.dumps({
            "ok": True,
            "message": "Raw logistics executed successfully",
        })
        return Response(response, mimetype="application/json")
    except Exception as e:
        response = json.dumps({
            "ok": False,
            "message": str(e),
        })
        return Response(response, mimetype="application/json", status=500)

if __name__ == '__main__':
    execute_logistics_pipeline()
    exit(0)
