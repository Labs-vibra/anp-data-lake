import logging
from os import wait
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from constants import BASE_URL, SELECT_ID
from captcha_utils import resolver_captcha, preencher_captcha, verificar_erro_captcha
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait, Select

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Mudar logs!!!!!

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
    # options.add_argument("--headless")  # Enable headless mode
    # options.add_argument("--disable-gpu")
    # options.add_argument("--no-sandbox")
    # options.add_argument("--disable-dev-shm-usage")
    logging.info("Configurando modo headless (sem janela visível)...")
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ignore-ssl-errors")
    options.add_argument("--ignore-certificate-errors-spki-list")
    options.add_argument("--user-data-dir=/tmp/chrome-user-data")
    options.add_argument("--data-path=/tmp/chrome-user-data")
    options.add_argument("--homedir=/tmp")
    options.add_argument("--disk-cache-dir=/tmp/chrome-cache")
    options.add_argument("--remote-debugging-port=9222")

    driver = webdriver.Chrome(service=service, options=options)
    print("WebDriver configured successfully.")
    return driver

def process_select_values(select_object, select_values):
    for value in select_values:
        try:
            print(f"Processando valor: {value}")
            select_object.select_by_value(value)
            max_retries = 30
            retries = 0
            while retries < max_retries:
                captcha_code = resolver_captcha(wait)
                print(f"CAPTCHA resolvido: {captcha_code}")
                if captcha_code:
                    preencher_captcha(captcha_code, driver)
                    if not verificar_erro_captcha(driver):
                        logging.info(f"CAPTCHA rejeitado para o valor '{value}'. Tentando novamente...")
                        break  # Tenta resolver o CAPTCHA novamente para o mesmo valor
                retries += 1
                # Aqui você pode adicionar o código para extrair os dados após o CAPTCHA ser aceito
                logging.info(f"Dados extraídos com sucesso para o valor '{value}'")
            else:
                logging.info(f"Falha ao resolver CAPTCHA para o valor '{value}'. Pulando este valor.")

        except Exception as e:
            logging.info(f"Erro ao processar o valor '{value}': {e}")
print("Starting data scrape...")

# Setup Chrome WebDriver
driver = configure_driver()
try:
    driver.get(BASE_URL)
    print("Abrindo!")
    wait = WebDriverWait(driver, 20)

except Exception as error:
    print("Deu erro!")


try:
    # Aguarda até que o elemento select (dropdown) esteja presente na página
    select_element = wait.until(EC.presence_of_element_located((By.ID, SELECT_ID)))
    select_object = Select(select_element)

    select_values = [option.get_attribute("value") for option in select_object.options if option.get_attribute("value") != '']
    print(f"Valores encontrados no select: {select_values}")
    process_select_values(select_object, select_values)

except Exception as e:
    logging.info(f"Erro ao tentar preencher o campo 'Delivery': {e}")

