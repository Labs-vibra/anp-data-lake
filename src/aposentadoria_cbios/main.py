from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep
import traceback
import pandas as pd
import os
import time
import subprocess


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
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", {
        "download.default_directory": "/tmp",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def scrape_cbio_data():
    # Setup Chrome WebDriver
    driver = configure_driver()
    try:
        url = "https://sistemaswebb3-balcao.b3.com.br/historicalSeriesPage/?language=pt-BR"
        driver.get(url)
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
        year_element = parent_element.find_element(By.XPATH, "//a[@role='option' and contains(text(), '2023')]")
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

def cbios_retirement_extract():
    downloaded_file = scrape_cbio_data()
    if downloaded_file:
        print(f"File downloaded to: {downloaded_file}")
        # Inspect the file before loading
        with open(downloaded_file, 'rb') as f:
            print("File content:")
            for _ in range(5):
                print(f.readline())

        # Try reading the CSV with different configurations
        df = pd.read_csv(downloaded_file, sep=';', encoding='latin1', skipinitialspace=True)
        df = df.rename(columns={
            'Data': 'data',
            'Quantidade (Parte Obrigada)': 'quantidade_parte_obrigada',
            'Quantidade (Parte Não Obrigada)': 'quantidade_parte_nao_obrigada',
            'Totalização': 'totalizacao'
        })  # Clean column names

        print("DataFrame preview:")
        print(df.columns)
        print(df["data"])



if __name__ == "__main__":
    cbios_retirement_extract()

