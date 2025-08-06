import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

RAW_DIR = os.path.join(BASE_DIR, "extracted")

os.makedirs(RAW_DIR, exist_ok=True)

PATHS = {
    "BASE_DIR": BASE_DIR,
    "RAW_DIR": RAW_DIR,
}
