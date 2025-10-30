import os
import time
import pandas as pd
from io import BytesIO
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from PIL import Image
import pytesseract
import io
from constants import ANP_URL, BUCKET_PATH
import logging
from utils import (
    upload_bytes_to_bucket,
    read_excel_from_bucket,
    format_columns_for_bq,
    insert_data_into_bigquery
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

PASTA_DOWNLOAD_RAM = "/dev/shm/anp_downloads"
if not os.path.exists("/dev/shm"):
    PASTA_DOWNLOAD_RAM = "/tmp/anp_downloads"

os.makedirs(PASTA_DOWNLOAD_RAM, exist_ok=True)
logging.info(f"📁 Download configurado para RAM: {PASTA_DOWNLOAD_RAM}")

# Configurações do Chrome
chrome_options = webdriver.ChromeOptions()

# Configurações condicionais para modo headless
logging.info("Configurando modo headless (sem janela visível)...")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-plugins")
chrome_options.add_argument("--disable-web-security")
chrome_options.add_argument("--allow-running-insecure-content")
chrome_options.add_argument("--ignore-certificate-errors")
chrome_options.add_argument("--ignore-ssl-errors")
chrome_options.add_argument("--ignore-certificate-errors-spki-list")
chrome_options.add_argument("--user-data-dir=/tmp/chrome-user-data")
chrome_options.add_argument("--data-path=/tmp/chrome-user-data")
chrome_options.add_argument("--homedir=/tmp")
chrome_options.add_argument("--disk-cache-dir=/tmp/chrome-cache")
chrome_options.add_argument("--remote-debugging-port=9222")

prefs = {
    "download.default_directory": PASTA_DOWNLOAD_RAM,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
chrome_options.add_experimental_option("prefs", prefs)
logging.info(f"🔽 Chrome configurado para baixar na RAM: {PASTA_DOWNLOAD_RAM}")

# --- INICIANDO O NAVEGADOR ---

logging.info("Iniciando o navegador em modo headless (sem janela visível)...")

try:
    # Tentar usar ChromeDriverManager
    service = ChromeService(executable_path=ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
except Exception as e:
    logging.warning(f"Falha ao usar ChromeDriverManager: {e}")
    # Fallback: tentar usar chromedriver do sistema
    try:
        driver = webdriver.Chrome(options=chrome_options)
    except Exception as e2:
        logging.error(f"Falha ao inicializar Chrome: {e2}")
        raise

driver.get(ANP_URL)
driver.implicitly_wait(10)
logging.info("Página carregada com sucesso!")

# Configurar WebDriverWait globalmente
wait = WebDriverWait(driver, 20)

# --- FUNÇÕES DE CAPTCHA ---

def resolver_captcha():
    """Resolve o CAPTCHA da página ANP extraindo as 5 imagens e usando OCR"""
    try:
        logging.info("Aguardando o CAPTCHA carregar...")
        captcha_div = wait.until(EC.presence_of_element_located((By.ID, "anp_p7_captcha_1")))
        captcha_images = captcha_div.find_elements(By.TAG_NAME, "img")

        if len(captcha_images) != 5:
            logging.info(f"Erro: Esperado 5 imagens do CAPTCHA, encontrado {len(captcha_images)}")
            return None

        logging.info("Extraindo e processando as 5 imagens do CAPTCHA...")
        captcha_text = ""

        for i, img_element in enumerate(captcha_images):
            try:
                img_screenshot = img_element.screenshot_as_png
                image = Image.open(io.BytesIO(img_screenshot))
                image = image.convert('L')

                custom_config = r'--oem 3 --psm 8 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                char = pytesseract.image_to_string(image, config=custom_config).strip().upper()

                if char:
                    captcha_text += char[0]
                else:
                    return None

            except Exception as e:
                logging.info(f"Erro ao processar imagem {i+1}: {e}")
                return None

        if len(captcha_text) == 5:
            return captcha_text
        else:
            logging.info(f"Erro: CAPTCHA incompleto - apenas {len(captcha_text)} caracteres reconhecidos")
            return None

    except Exception as e:
        logging.info(f"Erro ao resolver CAPTCHA: {e}")
        return None

def preencher_captcha(captcha_text):
    """Preenche o campo do CAPTCHA com o texto resolvido"""
    try:
        captcha_input = driver.find_element(By.ID, "P7_CAPTCHA_1")
        captcha_input.clear()
        captcha_input.send_keys(captcha_text)
        logging.info(f"Campo CAPTCHA preenchido com: '{captcha_text}'")
        return True
    except Exception as e:
        logging.info(f"Erro ao preencher campo CAPTCHA: {e}")
        return False

def verificar_erro_captcha():
    """Verifica se apareceu a mensagem de erro do CAPTCHA"""
    try:
        possible_errors = [
            "O campo imagem está inválido",
            "Ocorreu 1 erro",
            "1 error has occurred"
        ]
        error_div = driver.find_element(By.ID, "t_Alert_Notification")
        if error_div.is_displayed():
            error_text = error_div.text
            if any(err in error_text for err in possible_errors):
                logging.info("❌ CAPTCHA foi rejeitado pelo servidor!")
                return True
        return False
    except:
        return False

# --- FUNÇÃO PRINCIPAL DE DOWNLOAD ---

def fazer_download_delivery(valor_delivery, nome_delivery):
    """
    Faz o download dos dados para um valor específico de delivery
    Args:
        valor_delivery: '1' para SIM, '0' para NÃO
        nome_delivery: 'SIM' ou 'NAO' para nomenclatura
    Returns:
        DataFrame com os dados ou None se falhar
    """
    try:
        # Aguarda até que o elemento select (dropdown) esteja presente na página
        select_element = wait.until(EC.presence_of_element_located((By.ID, "P7_DELIVERY")))
        select_object = Select(select_element)
        select_object.select_by_value(valor_delivery)

    except Exception as e:
        logging.info(f"Erro ao tentar preencher o campo 'Delivery' com {nome_delivery}: {e}")
        return None

    # Tentar resolver o CAPTCHA com até 100 tentativas
    max_tentativas = 100
    captcha_resolvido = False

    for tentativa in range(1, max_tentativas + 1):
        logging.info(f"Tentativa {tentativa} de resolver CAPTCHA para {nome_delivery}")

        # 1. Resolve o CAPTCHA
        captcha_text = resolver_captcha()

        if captcha_text and preencher_captcha(captcha_text):
            # 2. Aperta o botão para Exportar com Tancagem
            try:
                logging.info("🚀 Clicando no botão 'Exportar com Tancagem'...")
                download_button = driver.find_element(By.ID, "B627337254132141370")
                download_button.click()

                logging.info("⏱️ Aguardando validação do CAPTCHA pelo servidor...")
                time.sleep(3)

                # 3. Verifica se a div de erro aparece
                if verificar_erro_captcha():
                    # 4. Se há erro, fecha a div e continua tentativas
                    logging.info("❌ CAPTCHA rejeitado pelo servidor. Fechando alerta...")
                    try:
                        close_button = driver.find_element(By.CSS_SELECTOR, ".t-Icon.icon-close")
                        close_button.click()
                        logging.info("Alerta de erro fechado")
                    except Exception as e:
                        logging.info(f"Erro ao fechar alerta: {e}")
                else:
                    # 5. Se não há erro, CAPTCHA aceito
                    logging.info("✅ CAPTCHA aceito pelo servidor!")
                    captcha_resolvido = True
                    break

            except Exception as e:
                logging.info(f"Erro ao clicar no botão de exportar: {e}")

        # Se chegou aqui, a tentativa falhou - precisa refresh do CAPTCHA
        if tentativa < max_tentativas:
            logging.info("🔄 Fazendo refresh do CAPTCHA...")
            try:
                refresh_button = driver.find_element(By.ID, "spn_captchaanp_refresh_anp_p7_captcha_1")
                refresh_button.click()
                time.sleep(3)
            except Exception as e:
                driver.refresh()
                time.sleep(5)

                try:
                    select_element = wait.until(EC.presence_of_element_located((By.ID, "P7_DELIVERY")))
                    select_object = Select(select_element)
                    select_object.select_by_value(valor_delivery)
                except Exception as e:
                    logging.info(f"Erro ao reselecionar campo delivery: {e}")
        else:
            logging.info("Número máximo de tentativas excedido.")

    if not captcha_resolvido:
        logging.info(f"❌ Não foi possível resolver o CAPTCHA para {nome_delivery} após {max_tentativas} tentativas.")
        return None

    logging.info(f"🎯 CAPTCHA resolvido com sucesso para {nome_delivery}! Aguardando download...")

    # Sistema de verificação de arquivo (robusto)
    if nome_delivery == 'SIM':
        nomes_possiveis = ["exportação.xlsx", "export.xlsx"]  # Sem (1) é mais provável
    else:  # NAO
        nomes_possiveis = ["exportação (1).xlsx", "exportação.xlsx", "export.xlsx"]  # (1) é mais provável

    logging.info(f"🔍 Aguardando arquivo para {nome_delivery}. Nomes possíveis: {nomes_possiveis}")

    tempo_espera = 120
    tempo_inicial = time.time()
    arquivo_baixado = None

    while time.time() - tempo_inicial < tempo_espera:
        for nome_arquivo in nomes_possiveis:
            caminho_arquivo = os.path.join(PASTA_DOWNLOAD_RAM, nome_arquivo)
            if os.path.exists(caminho_arquivo):
                if not os.path.exists(caminho_arquivo + ".crdownload"):
                    if time.time() - os.path.getctime(caminho_arquivo) < 300:
                        arquivo_baixado = caminho_arquivo
                        logging.info(f"✅ Arquivo encontrado: {arquivo_baixado}")
                        break

        if arquivo_baixado:
            break

        time.sleep(1)

    if not arquivo_baixado:
        logging.info("📁 Arquivo específico não encontrado. Procurando arquivos .xlsx recentes...")

        arquivos_xlsx = []
        for arquivo in os.listdir(PASTA_DOWNLOAD_RAM):
            if arquivo.endswith('.xlsx'):
                caminho_completo = os.path.join(PASTA_DOWNLOAD_RAM, arquivo)
                # Verifica se foi criado nos últimos 3 minutos
                if time.time() - os.path.getctime(caminho_completo) < 180:
                    arquivos_xlsx.append(caminho_completo)

        if arquivos_xlsx:
            arquivo_baixado = max(arquivos_xlsx, key=os.path.getctime)
            logging.info(f"✅ Arquivo recente encontrado: {arquivo_baixado}")
        else:
            logging.info(f"❌ Nenhum arquivo .xlsx encontrado para {nome_delivery}.")
            return None

    nome_no_bucket = f"{BUCKET_PATH}exportacao_delivery_{nome_delivery.lower()}.xlsx"

    logging.info("☁️ Upload para GCP")
    upload_sucesso = upload_bytes_to_bucket(arquivo_baixado, nome_no_bucket)

    try:
        os.remove(arquivo_baixado)
        logging.info(f"🗑️ Arquivo removido da RAM: {os.path.basename(arquivo_baixado)}")
    except Exception as e:
        logging.info(f"⚠️ Erro ao limpar RAM: {e}")

    if not upload_sucesso:
        logging.info(f"❌ Falha ao fazer upload para bucket. Usando arquivo local.")
        try:
            df = pd.read_excel(arquivo_baixado, engine='openpyxl')
            df['DELIVERY'] = nome_delivery
            logging.info(f"✅ DataFrame {nome_delivery} criado com sucesso (local)!")
            logging.info(f"📊 O DataFrame {nome_delivery} possui {df.shape[0]} linhas e {df.shape[1]} colunas.")

            try:
                os.remove(arquivo_baixado)
                logging.info(f"🗑️ Arquivo local removido: {arquivo_baixado}")
            except:
                pass

            return df
        except Exception as e:
            logging.info(f"❌ Erro ao ler o arquivo XLSX local para {nome_delivery}: {e}")
            return None

    # Ler o arquivo do bucket
    logging.info("📖 Lendo arquivo Excel do bucket GCP...")
    df = read_excel_from_bucket(nome_no_bucket, nome_delivery)

    if df is not None:
        logging.info(f"✅ DataFrame {nome_delivery} criado com sucesso!")
        logging.info(f"📊 O DataFrame {nome_delivery} possui {df.shape[0]} linhas e {df.shape[1]} colunas.")

        try:
            os.remove(arquivo_baixado)
            logging.info(f"🗑️ Arquivo local removido: {arquivo_baixado}")
        except Exception as e:
            logging.info(f"⚠️ Não foi possível remover arquivo local: {e}")

        return df
    else:
        logging.info(f"❌ Erro ao processar arquivo do bucket para {nome_delivery}")
        return None

# --- PROCESSAMENTO PRINCIPAL ---

try:
    dataframes = []

    logging.info("INICIANDO DOWNLOAD PARA DELIVERY SIM")

    df_sim = fazer_download_delivery('1', 'SIM')
    if df_sim is not None:
        dataframes.append(df_sim)
        logging.info(f"✅ Dados DELIVERY SIM obtidos: {df_sim.shape[0]} linhas")
    else:
        logging.info("❌ Falha ao obter dados para DELIVERY SIM")

    logging.info("INICIANDO DOWNLOAD PARA DELIVERY NÃO")

    logging.info("🆕 Abrindo nova aba para DELIVERY NÃO...")
    driver.execute_script("window.open('');")
    driver.switch_to.window(driver.window_handles[1])
    driver.get(ANP_URL)
    time.sleep(5)

    df_nao = fazer_download_delivery('0', 'NAO')
    if df_nao is not None:
        dataframes.append(df_nao)
        logging.info(f"✅ Dados DELIVERY NÃO obtidos: {df_nao.shape[0]} linhas")
    else:
        logging.info("❌ Falha ao obter dados para DELIVERY NÃO")

    if len(dataframes) == 0:
        logging.info("❌ Nenhum DataFrame foi criado com sucesso.")
    elif len(dataframes) == 1:
        logging.info("⚠️  Apenas um DataFrame foi criado com sucesso.")
        df_final = dataframes[0]
    else:
        logging.info("JUNTANDO OS DATAFRAMES")

        df_final = pd.concat(dataframes, ignore_index=True)
        logging.info(f"✅ DataFrames unidos com sucesso!")
        logging.info(f"📊 DataFrame final: {df_final.shape[0]} linhas e {df_final.shape[1]} colunas")

    # --- RESULTADO FINAL ---

    if len(dataframes) > 0:
        logging.info("RESULTADO FINAL")

        logging.info(f"📊 DataFrame final possui {df_final.shape[0]} linhas e {df_final.shape[1]} colunas.")

        if 'DELIVERY' in df_final.columns:
            logging.info("\n📈 Distribuição por tipo de delivery:")
            logging.info(df_final['DELIVERY'].value_counts())

        logging.info("\n⚙️ Processamento BigQuery...")
        df_formatado = format_columns_for_bq(df_final)
        logging.info(f"📋 Colunas formatadas: {list(df_formatado.columns)}")
        insert_data_into_bigquery(df_formatado)

except Exception as e:
    logging.info(f"❌ Erro durante execução: {e}")

finally:
    logging.info("PROCESSO FINALIZADO")

    try:
        if len(driver.window_handles) > 1:
            for handle in driver.window_handles[1:]:
                driver.switch_to.window(handle)
                driver.close()
            driver.switch_to.window(driver.window_handles[0])

        driver.quit()
        logging.info("🚪 Navegador fechado.")
    except Exception as e:
        logging.info(f"❌ Erro ao fechar navegador: {e}")

    try:
        import shutil
        if os.path.exists(PASTA_DOWNLOAD_RAM):
            shutil.rmtree(PASTA_DOWNLOAD_RAM)
    except Exception as e:
        logging.info(f"⚠️ Erro ao limpar pasta RAM: {e}")
