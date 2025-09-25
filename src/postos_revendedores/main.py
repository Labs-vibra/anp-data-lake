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
from utils import (
    upload_bytes_to_bucket,
    read_excel_from_bucket,
    format_columns_for_bq,
    insert_data_into_bigquery
)

# Configurações do script
MODO_HEADLESS = True  # Mude para False se querer ver o navegador funcionando
PASTA_DOWNLOAD = os.getcwd()  # Pasta onde os arquivos serão baixados

# Configurações do Chrome
chrome_options = webdriver.ChromeOptions()

# Configurações condicionais para modo headless
if MODO_HEADLESS:
    print("Configurando modo headless (sem janela visível)...")
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
else:
    print("Configurando modo com janela visível...")

prefs = {
    "download.default_directory": PASTA_DOWNLOAD,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
chrome_options.add_experimental_option("prefs", prefs)

# --- INICIANDO O NAVEGADOR ---

if MODO_HEADLESS:
    print("Iniciando o navegador em modo headless (sem janela visível)...")
else:
    print("Iniciando o navegador com janela visível...")

service = ChromeService(executable_path=ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# Abre a URL
driver.get(ANP_URL)
driver.implicitly_wait(10) 
print("Página carregada com sucesso!")

# Configurar WebDriverWait globalmente
wait = WebDriverWait(driver, 20)

# --- FUNÇÕES DE CAPTCHA ---

def resolver_captcha():
    """Resolve o CAPTCHA da página ANP extraindo as 5 imagens e usando OCR"""
    try:
        print("Aguardando o CAPTCHA carregar...")
        captcha_div = wait.until(EC.presence_of_element_located((By.ID, "anp_p7_captcha_1")))
        captcha_images = captcha_div.find_elements(By.TAG_NAME, "img")
        
        if len(captcha_images) != 5:
            print(f"Erro: Esperado 5 imagens do CAPTCHA, encontrado {len(captcha_images)}")
            return None
            
        print("Extraindo e processando as 5 imagens do CAPTCHA...")
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
                    print(f"Imagem {i+1}: '{char[0]}'")
                else:
                    print(f"Imagem {i+1}: Não foi possível reconhecer o caractere")
                    return None
                    
            except Exception as e:
                print(f"Erro ao processar imagem {i+1}: {e}")
                return None
        
        if len(captcha_text) == 5:
            print(f"CAPTCHA resolvido: '{captcha_text}'")
            return captcha_text
        else:
            print(f"Erro: CAPTCHA incompleto - apenas {len(captcha_text)} caracteres reconhecidos")
            return None
            
    except Exception as e:
        print(f"Erro ao resolver CAPTCHA: {e}")
        return None

def preencher_captcha(captcha_text):
    """Preenche o campo do CAPTCHA com o texto resolvido"""
    try:
        captcha_input = driver.find_element(By.ID, "P7_CAPTCHA_1")
        captcha_input.clear()
        captcha_input.send_keys(captcha_text)
        print(f"Campo CAPTCHA preenchido com: '{captcha_text}'")
        return True
    except Exception as e:
        print(f"Erro ao preencher campo CAPTCHA: {e}")
        return False

def verificar_erro_captcha():
    """Verifica se apareceu a mensagem de erro do CAPTCHA"""
    try:
        error_div = driver.find_element(By.ID, "t_Alert_Notification")
        if error_div.is_displayed():
            error_text = error_div.text
            if "O campo imagem está inválido" in error_text or "Ocorreu 1 erro" in error_text:
                print("❌ CAPTCHA foi rejeitado pelo servidor!")
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
        print(f"\n=== PROCESSANDO DELIVERY {nome_delivery} ===")
        print(f"Selecionando '{nome_delivery}' no campo de delivery...")
        
        # Aguarda até que o elemento select (dropdown) esteja presente na página
        select_element = wait.until(EC.presence_of_element_located((By.ID, "P7_DELIVERY")))
        select_object = Select(select_element)
        select_object.select_by_value(valor_delivery)
        
        print(f"Campo delivery preenchido com '{nome_delivery}' com sucesso!")

    except Exception as e:
        print(f"Erro ao tentar preencher o campo 'Delivery' com {nome_delivery}: {e}")
        return None

    # Tentar resolver o CAPTCHA com até 100 tentativas
    max_tentativas = 100
    captcha_resolvido = False

    for tentativa in range(1, max_tentativas + 1):
        print(f"\n--- Tentativa {tentativa} de resolver CAPTCHA para {nome_delivery} ---")
        
        # 1. Resolve o CAPTCHA
        captcha_text = resolver_captcha()
        
        if captcha_text and preencher_captcha(captcha_text):
            # 2. Preenche campo CAPTCHA (já feito no if acima)
            
            # 3. Aperta o botão para Exportar com Tancagem
            try:
                print("🚀 Clicando no botão 'Exportar com Tancagem'...")
                download_button = driver.find_element(By.ID, "B627337254132141370")
                download_button.click()
                
                # 4. Aguarda 3 segundos
                print("⏱️ Aguardando validação do CAPTCHA pelo servidor...")
                time.sleep(3)
                
                # 5. Verifica se a div de erro aparece
                if verificar_erro_captcha():
                    # 6. Se há erro, fecha a div e continua tentativas
                    print("❌ CAPTCHA rejeitado pelo servidor. Fechando alerta...")
                    try:
                        close_button = driver.find_element(By.CSS_SELECTOR, ".t-Icon.icon-close")
                        close_button.click()
                        print("Alerta de erro fechado")
                    except Exception as e:
                        print(f"Erro ao fechar alerta: {e}")
                    # Continua para a próxima tentativa
                else:
                    # 7. Se não há erro, CAPTCHA aceito
                    print("✅ CAPTCHA aceito pelo servidor!")
                    captcha_resolvido = True
                    break
                    
            except Exception as e:
                print(f"Erro ao clicar no botão de exportar: {e}")
        
        # Se chegou aqui, a tentativa falhou - precisa refresh do CAPTCHA
        if tentativa < max_tentativas:
            print("🔄 Fazendo refresh do CAPTCHA...")
            try:
                refresh_button = driver.find_element(By.ID, "spn_captchaanp_refresh_anp_p7_captcha_1")
                refresh_button.click()
                print("Clicou no botão de refresh do CAPTCHA")
                time.sleep(3)  # Aguarda o novo CAPTCHA carregar
            except Exception as e:
                print(f"Erro ao clicar no botão de refresh do CAPTCHA: {e}")
                print("Tentando recarregar a página como fallback...")
                driver.refresh()
                time.sleep(5)
                
                # Refaz a seleção do campo delivery
                try:
                    select_element = wait.until(EC.presence_of_element_located((By.ID, "P7_DELIVERY")))
                    select_object = Select(select_element)
                    select_object.select_by_value(valor_delivery)
                except Exception as e:
                    print(f"Erro ao reselecionar campo delivery: {e}")
        else:
            print("Número máximo de tentativas excedido.")

    if not captcha_resolvido:
        print(f"❌ Não foi possível resolver o CAPTCHA para {nome_delivery} após {max_tentativas} tentativas.")
        return None

    print(f"🎯 CAPTCHA resolvido com sucesso para {nome_delivery}! Aguardando download...")

    # Sistema de verificação de arquivo (robusto)
    if nome_delivery == 'SIM':
        nomes_possiveis = ["exportação.xlsx"]
    else:  # NAO
        nomes_possiveis = ["exportação (1).xlsx", "exportação.xlsx"]  # (1) é mais provável
    
    print(f"🔍 Aguardando arquivo para {nome_delivery}. Nomes possíveis: {nomes_possiveis}")

    # Aguarda até 120 segundos pelo arquivo (tempo aumentado)
    tempo_espera = 120
    tempo_inicial = time.time()
    arquivo_baixado = None

    while time.time() - tempo_inicial < tempo_espera:
        # Verifica cada nome possível
        for nome_arquivo in nomes_possiveis:
            caminho_arquivo = os.path.join(PASTA_DOWNLOAD, nome_arquivo)
            if os.path.exists(caminho_arquivo):
                # Verifica se o arquivo não está sendo baixado ainda (não tem .crdownload)
                if not os.path.exists(caminho_arquivo + ".crdownload"):
                    # Verifica se foi criado nos últimos 5 minutos (arquivo recente)
                    if time.time() - os.path.getctime(caminho_arquivo) < 300:
                        arquivo_baixado = caminho_arquivo
                        print(f"✅ Arquivo encontrado: {arquivo_baixado}")
                        break
        
        if arquivo_baixado:
            break
            
        time.sleep(1)  # Verifica a cada 1 segundo

    if not arquivo_baixado:
        # Fallback: procura qualquer arquivo .xlsx muito recente (últimos 3 minutos)
        print("📁 Arquivo específico não encontrado. Procurando arquivos .xlsx recentes...")
        
        arquivos_xlsx = []
        for arquivo in os.listdir(PASTA_DOWNLOAD):
            if arquivo.endswith('.xlsx'):
                caminho_completo = os.path.join(PASTA_DOWNLOAD, arquivo)
                # Verifica se foi criado nos últimos 3 minutos
                if time.time() - os.path.getctime(caminho_completo) < 180:
                    arquivos_xlsx.append(caminho_completo)
        
        if arquivos_xlsx:
            # Pega o mais recente
            arquivo_baixado = max(arquivos_xlsx, key=os.path.getctime)
            print(f"✅ Arquivo recente encontrado: {arquivo_baixado}")
        else:
            print(f"❌ Nenhum arquivo .xlsx encontrado para {nome_delivery}.")
            return None
    
    # Fazer upload do arquivo para o bucket GCP
    nome_no_bucket = f"{BUCKET_PATH}/exportacao_delivery_{nome_delivery.lower()}.xlsx"
    
    print(f"☁️ Fazendo upload do arquivo para o bucket GCP...")
    upload_sucesso = upload_bytes_to_bucket(arquivo_baixado, nome_no_bucket)
    
    if not upload_sucesso:
        print(f"❌ Falha ao fazer upload para bucket. Usando arquivo local.")
        # Continua com processamento local como fallback
        try:
            df = pd.read_excel(arquivo_baixado, engine='openpyxl')
            df['DELIVERY'] = nome_delivery
            print(f"✅ DataFrame {nome_delivery} criado com sucesso (local)!")
            print(f"📊 O DataFrame {nome_delivery} possui {df.shape[0]} linhas e {df.shape[1]} colunas.")
            
            # Remove arquivo local
            try:
                os.remove(arquivo_baixado)
                print(f"🗑️ Arquivo local removido: {arquivo_baixado}")
            except:
                pass
            
            return df
        except Exception as e:
            print(f"❌ Erro ao ler o arquivo XLSX local para {nome_delivery}: {e}")
            return None
    
    # Ler o arquivo diretamente do bucket
    print(f"📖 Lendo arquivo Excel diretamente do bucket GCP...")
    df = read_excel_from_bucket(nome_no_bucket, nome_delivery)
    
    if df is not None:
        print(f"✅ DataFrame {nome_delivery} criado com sucesso!")
        print(f"📊 O DataFrame {nome_delivery} possui {df.shape[0]} linhas e {df.shape[1]} colunas.")
        
        # Remove o arquivo local após upload bem-sucedido
        try:
            os.remove(arquivo_baixado)
            print(f"🗑️ Arquivo local removido: {arquivo_baixado}")
        except Exception as e:
            print(f"⚠️ Não foi possível remover arquivo local: {e}")
        
        return df
    else:
        print(f"❌ Erro ao processar arquivo do bucket para {nome_delivery}")
        return None

# --- PROCESSAMENTO PRINCIPAL ---

try:
    # Lista para armazenar os DataFrames
    dataframes = []

    # 1. Download para Delivery SIM
    print("\n" + "="*60)
    print("INICIANDO DOWNLOAD PARA DELIVERY SIM")
    print("="*60)

    df_sim = fazer_download_delivery('1', 'SIM')
    if df_sim is not None:
        dataframes.append(df_sim)
        print(f"✅ Dados DELIVERY SIM obtidos: {df_sim.shape[0]} linhas")
    else:
        print("❌ Falha ao obter dados para DELIVERY SIM")

    # 2. Download para Delivery NÃO em nova aba
    print("\n" + "="*60)
    print("INICIANDO DOWNLOAD PARA DELIVERY NÃO EM NOVA ABA")
    print("="*60)

    # Abre uma nova aba para o segundo download
    print("🆕 Abrindo nova aba para DELIVERY NÃO...")
    driver.execute_script("window.open('');")
    driver.switch_to.window(driver.window_handles[1])  # Muda para a nova aba
    driver.get(ANP_URL)  # Carrega a URL na nova aba
    time.sleep(5)  # Aguarda a página carregar completamente
    print("✅ Nova aba aberta e carregada com sucesso!")

    df_nao = fazer_download_delivery('0', 'NAO')
    if df_nao is not None:
        dataframes.append(df_nao)
        print(f"✅ Dados DELIVERY NÃO obtidos: {df_nao.shape[0]} linhas")
    else:
        print("❌ Falha ao obter dados para DELIVERY NÃO")

    # --- JUNTANDO OS DATAFRAMES ---

    if len(dataframes) == 0:
        print("❌ Nenhum DataFrame foi criado com sucesso.")
    elif len(dataframes) == 1:
        print("⚠️  Apenas um DataFrame foi criado com sucesso.")
        df_final = dataframes[0]
    else:
        print("\n" + "="*60)
        print("JUNTANDO OS DATAFRAMES")
        print("="*60)
        
        # Junta os DataFrames
        df_final = pd.concat(dataframes, ignore_index=True)
        print(f"✅ DataFrames unidos com sucesso!")
        print(f"📊 DataFrame final: {df_final.shape[0]} linhas e {df_final.shape[1]} colunas")

    # --- RESULTADO FINAL ---

    if len(dataframes) > 0:
        print("\n" + "="*60)
        print("RESULTADO FINAL")
        print("="*60)

        print(f"📊 DataFrame final possui {df_final.shape[0]} linhas e {df_final.shape[1]} colunas.")

        # Mostra distribuição por tipo de delivery
        if 'DELIVERY' in df_final.columns:
            print("\n📈 Distribuição por tipo de delivery:")
            print(df_final['DELIVERY'].value_counts())

        print("\n⚙️ Processamento BigQuery...")
        df_formatado = format_columns_for_bq(df_final)
        print(f"📋 Colunas formatadas: {list(df_formatado.columns)}")
        insert_data_into_bigquery(df_formatado)

except Exception as e:
    print(f"❌ Erro durante execução: {e}")
    
finally:
    # --- FINALIZAÇÃO ---
    print("\n" + "="*60)
    print("PROCESSO FINALIZADO")
    print("="*60)

    # Fecha todas as abas adicionais antes de fechar o navegador
    try:
        if len(driver.window_handles) > 1:
            print("🗂️ Fechando abas adicionais...")
            for handle in driver.window_handles[1:]:
                driver.switch_to.window(handle)
                driver.close()
            driver.switch_to.window(driver.window_handles[0])  # Volta para a primeira aba

        driver.quit()
        print("🚪 Navegador fechado.")
    except Exception as e:
        print(f"❌ Erro ao fechar navegador: {e}")
