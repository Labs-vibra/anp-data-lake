from flask import Flask, Response
from dotenv import load_dotenv
import json

from db.extractions import rw_ext_anp_logistics

load_dotenv()
app = Flask(__name__)

def execute_raw_pipeline():
    rw_ext_anp_logistics()

@app.route("/")
def entrypoint():
    try:
        execute_raw_pipeline()
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
    execute_raw_pipeline()
    exit(0)
