from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep
import traceback
import pandas as pd
import os
import time
from google.cloud import bigquery

from constants import B3_URL, CBIOS_COL_MAPPING, TABLE_ID

def await_file_download(download_dir, timeout=120):
    start_time = time.time()
    downloaded_file = None

    while time.time() - start_time < timeout:
        files = os.listdir(download_dir)
        for file in files:
            if file.endswith(".csv"):
                downloaded_file = os.path.join(download_dir, file)
                break
        if downloaded_file:
            return downloaded_file
        time.sleep(1)

    raise Exception("File download timed out.")

def configure_driver():
    print("Configuring WebDriver...")
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", {
        "download.default_directory": "/tmp",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    options.add_argument("--headless")  # Enable headless mode
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=service, options=options)
    print("WebDriver configured successfully.")
    return driver

def scrape_cbio_data(year = "2023"):
    print("Starting data scrape...")
    # Setup Chrome WebDriver
    driver = configure_driver()
    try:
        driver.get(B3_URL)
        ELEMENT_ID = "div_idInformation"

        sleep(5)  # Additional wait to ensure all elements are loaded
        element = driver.find_element(By.ID, ELEMENT_ID)
        element.click()
        options_element_a = driver.find_element(By.ID, "4")
        options_element_a.click()
        button_element = driver.find_element(By.CSS_SELECTOR, '[b3button]')
        button_element.click()
        RADIO_CSS_SELECTOR = 'input[type="radio"][value="yearly"]'
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, RADIO_CSS_SELECTOR)))
        driver.find_element(By.CSS_SELECTOR, RADIO_CSS_SELECTOR).click()
        select_year = driver.find_element(By.ID, "year")
        select_year.click()
        parent_element = select_year.find_element(By.XPATH, "../..")
        year_element = parent_element.find_element(By.XPATH, f"//a[@role='option' and contains(text(), '{year}')]")
        year_element.click()
        driver.find_elements(By.CSS_SELECTOR, '[b3button][icon="download"]')[1].click()

        downloaded_file = await_file_download("/tmp", timeout=120)
        return downloaded_file
    except Exception as e:
        print("An error occurred:")
        traceback.print_exc()
    finally:
        # Close the browser
        driver.quit()
        print('Finished')

def cbios_retirement_extract(date_start=None, date_end=None):
    if year_to_extract is None:
        year_to_extract = os.getenv("YEAR_TO_EXTRACT", "2023")
    downloaded_file = scrape_cbio_data(year_to_extract)
    if downloaded_file:
        print(f"File downloaded to: {downloaded_file}")
        df = pd.read_csv(downloaded_file, sep=';', encoding='latin1', dtype=str, index_col=False).fillna('')
        df = df.rename(columns=CBIOS_COL_MAPPING)

        bq_client = bigquery.Client()
        job = bq_client.load_table_from_dataframe(df, TABLE_ID)
        job.result()
        print(f"Loaded {job.output_rows} rows into {TABLE_ID}.")


if __name__ == "__main__":
    cbios_retirement_extract()

