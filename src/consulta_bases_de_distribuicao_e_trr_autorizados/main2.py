import os
import time
import io
import logging
import pandas as pd
from PIL import Image
import pytesseract
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from constants import BASE_URL, PROJECT_ID, BQ_DATASET, TABLE_NAME, COLUMNS
from utils import normalize_column
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

DOWNLOAD_FOLDER = "/tmp/anp_downloads"
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# --- CHROME CONFIG ---
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")

prefs = {
    "download.default_directory": DOWNLOAD_FOLDER,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
}
chrome_options.add_experimental_option("prefs", prefs)

service = ChromeService(executable_path=ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)
wait = WebDriverWait(driver, 20)

# --- CAPTCHA ---
def solve_captcha():
    """Solve ANP page CAPTCHA using OCR"""
    try:
        captcha_div = wait.until(EC.presence_of_element_located((By.ID, "anp_p7_captcha_1")))
        captcha_images = captcha_div.find_elements(By.TAG_NAME, "img")
        if len(captcha_images) != 5:
            return None

        text = ""
        for img in captcha_images:
            img_bytes = img.screenshot_as_png
            image = Image.open(io.BytesIO(img_bytes)).convert("L")
            config = r"--oem 3 --psm 8 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            char = pytesseract.image_to_string(image, config=config).strip().upper()
            if char:
                text += char[0]

        return text if len(text) == 5 else None
    except:
        return None

def fill_captcha(captcha_text):
    field = driver.find_element(By.ID, "P7_CAPTCHA_1")
    field.clear()
    field.send_keys(captcha_text)

def check_captcha_error():
    try:
        error_div = driver.find_element(By.ID, "t_Alert_Notification")
        return error_div.is_displayed()
    except:
        return False

# --- DOWNLOAD ---
def download_data(base_value, base_name):
    """Download a specific base and return DataFrame"""
    try:
        select_element = wait.until(EC.presence_of_element_located((By.ID, "P7_TIPO_BASE")))
        Select(select_element).select_by_value(base_value)

        for attempt in range(50):
            captcha_text = solve_captcha()
            if not captcha_text:
                continue
            fill_captcha(captcha_text)

            download_button = driver.find_element(By.ID, "B627337254132141370")
            download_button.click()
            time.sleep(3)

            if not check_captcha_error():
                logging.info(f"✅ CAPTCHA accepted ({base_name})")
                break
            else:
                logging.info("❌ CAPTCHA rejected, retrying...")
                driver.refresh()
                time.sleep(5)
        else:
            logging.error(f"Captcha failed for {base_name}")
            return None

        downloaded_file = None
        for _ in range(60):
            for file in os.listdir(DOWNLOAD_FOLDER):
                if file.endswith(".xlsx"):
                    path = os.path.join(DOWNLOAD_FOLDER, file)
                    if not path.endswith(".crdownload"):
                        downloaded_file = path
                        break
            if downloaded_file:
                break
            time.sleep(2)

        if not downloaded_file:
            logging.error(f"❌ File not found for {base_name}")
            return None

        df = pd.read_excel(downloaded_file, engine="openpyxl")
        df["TYPE"] = base_name
        os.remove(downloaded_file)
        return df

    except Exception as e:
        logging.error(f"Error downloading {base_name}: {e}")
        return None

# --- BIGQUERY ---
def insert_dataframe_to_bq(df: pd.DataFrame):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME}"

    df = df[COLUMNS]  # Select only the desired columns
    df.columns = [normalize_column(c) for c in df.columns]

    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Wait for completion
    logging.info(f"✅ Data loaded into BigQuery: {table_id}, {df.shape[0]} rows")

# --- MAIN ---
def main():
    driver.get(BASE_URL)
    time.sleep(5)

    bases = {
        "1": "BASE DISTRIBUICAO",
        "2": "TRR",
        "3": "IMPORTACAO",
        "4": "EXPORTACAO",
        "5": "ARMAZENAGEM",
        "6": "OUTROS"
    }

    dfs = []
    for value, name in bases.items():
        logging.info(f"🔽 Downloading {name}...")
        df = download_data(value, name)
        if df is not None:
            dfs.append(df)

    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
        logging.info(f"📊 Final DataFrame ready with {final_df.shape[0]} rows")
        insert_dataframe_to_bq(final_df)
    else:
        logging.error("❌ No DataFrames downloaded.")

    driver.quit()

if __name__ == "__main__":
    main()

