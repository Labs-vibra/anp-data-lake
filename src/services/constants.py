import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

EXTRACTION_DIR = os.path.join(BASE_DIR, "extracted")

os.makedirs(EXTRACTION_DIR, exist_ok=True)

PATHS = {
    "BASE_DIR": BASE_DIR,
    "EXTRACTION_DIR": EXTRACTION_DIR,
}
