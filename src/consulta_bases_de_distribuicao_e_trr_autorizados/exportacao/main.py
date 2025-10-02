import os
import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from captcha_utils import resolver_captcha, preencher_captcha, verificar_erro_captcha
from constants import BASE_URL, SELECT_ID, CONSULT_BUTTON_ID, PASTA_DOWNLOAD_RAM, BUCKET_PATH, BUCKET_NAME
from utils import upload_bytes_to_bucket, limpar_pasta_download, configurar_downloads_chrome

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Criar pasta de download na RAM se não existir
os.makedirs(PASTA_DOWNLOAD_RAM, exist_ok=True)
logging.info(f"📁 Download configurado para RAM: {PASTA_DOWNLOAD_RAM}")

# Configuração do Chrome com downloads
chrome_options = configurar_downloads_chrome(PASTA_DOWNLOAD_RAM)

# Comentar o --headless para ver o processo acontecer
# chrome_options.add_argument("--headless")

# Inicializar o driver
service = ChromeService(executable_path=ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)
wait = WebDriverWait(driver, 20)

def aguardar_download_completo(valor_select, timeout=120):
    """
    Aguarda o download ser concluído verificando o arquivo 'exportação.xlsx' na pasta RAM.
    ASSUME: Pasta sempre vazia no início, arquivo sempre chamado 'exportação.xlsx'
    Args:
        valor_select: Valor do select para identificar o download
        timeout: Tempo limite em segundos
    Returns:
        str: Caminho do arquivo baixado ou None se falhou
    """
    inicio = time.time()
    arquivo_esperado = "exportação.xlsx"
    caminho_arquivo = os.path.join(PASTA_DOWNLOAD_RAM, arquivo_esperado)
    
    logging.info(f"⏳ Aguardando download para o valor '{valor_select}'...")
    logging.info(f"📂 Monitorando pasta: {PASTA_DOWNLOAD_RAM}")
    logging.info(f"📄 Arquivo esperado: {arquivo_esperado}")
    
    # Verifica pasta inicial
    if os.path.exists(PASTA_DOWNLOAD_RAM):
        arquivos_iniciais = os.listdir(PASTA_DOWNLOAD_RAM)
        logging.info(f"📋 Arquivos iniciais na pasta: {arquivos_iniciais if arquivos_iniciais else 'pasta vazia'}")
    else:
        logging.warning(f"⚠️ Pasta de download não existe: {PASTA_DOWNLOAD_RAM}")
        return None
    
    ultimo_tamanho = None
    contagem_estavel = 0
    
    while time.time() - inicio < timeout:
        try:
            # Verifica se o arquivo esperado existe
            if os.path.exists(caminho_arquivo) and os.path.isfile(caminho_arquivo):
                tamanho_atual = os.path.getsize(caminho_arquivo)
                
                # Primeira detecção do arquivo
                if ultimo_tamanho is None:
                    logging.info(f"📥 Arquivo '{arquivo_esperado}' detectado! Tamanho: {tamanho_atual} bytes")
                    ultimo_tamanho = tamanho_atual
                    contagem_estavel = 0
                # Verifica se o tamanho está estável
                elif tamanho_atual == ultimo_tamanho:
                    contagem_estavel += 1
                    
                    # Arquivo estável por 3 verificações = download completo
                    if contagem_estavel >= 3:
                        logging.info(f"✅ Download concluído para '{valor_select}': {arquivo_esperado}")
                        logging.info(f"📁 Caminho completo: {caminho_arquivo}")
                        logging.info(f"📊 Tamanho final: {tamanho_atual} bytes")
                        return caminho_arquivo
                # Arquivo ainda está crescendo
                else:
                    logging.info(f"📊 Arquivo '{arquivo_esperado}' crescendo: {tamanho_atual} bytes")
                    ultimo_tamanho = tamanho_atual
                    contagem_estavel = 0
            else:
                # Arquivo ainda não apareceu
                if ultimo_tamanho is not None:
                    # Arquivo existia mas sumiu (estranho)
                    logging.warning(f"⚠️ Arquivo '{arquivo_esperado}' desapareceu!")
                    ultimo_tamanho = None
                    contagem_estavel = 0
            
            time.sleep(0.5)  # Verifica a cada 0.5 segundos
            
        except Exception as e:
            logging.warning(f"⚠️ Erro ao verificar download: {e}")
            time.sleep(1)
    
    # Timeout - verifica uma última vez se o arquivo existe
    logging.warning(f"⏰ Timeout aguardando download para '{valor_select}'")
    
    if os.path.exists(caminho_arquivo) and os.path.isfile(caminho_arquivo):
        tamanho = os.path.getsize(caminho_arquivo)
        logging.info(f"⚠️ Mas o arquivo '{arquivo_esperado}' existe com {tamanho} bytes")
        logging.info(f"✅ Usando arquivo mesmo após timeout")
        return caminho_arquivo
    
    # Lista o que há na pasta para debug
    if os.path.exists(PASTA_DOWNLOAD_RAM):
        arquivos_finais = os.listdir(PASTA_DOWNLOAD_RAM)
        logging.info(f"📋 Arquivos na pasta após timeout: {arquivos_finais if arquivos_finais else 'pasta vazia'}")
    
    logging.error(f"❌ Arquivo '{arquivo_esperado}' não foi encontrado para '{valor_select}'")
    return None

def processar_arquivo_baixado(arquivo_path, valor_select):
    """
    Processa o arquivo baixado e envia para o bucket.
    Args:
        arquivo_path: Caminho do arquivo baixado
        valor_select: Valor do select usado
    Returns:
        bool: True se processado com sucesso, False caso contrário
    """
    try:
        logging.info(f"📦 Processando arquivo baixado para o valor '{valor_select}'...")
        logging.info(f"📁 Arquivo local: {arquivo_path}")
        logging.info(f"📊 Tamanho do arquivo: {os.path.getsize(arquivo_path)} bytes")
        
        # Determina a extensão do arquivo
        _, extensao = os.path.splitext(arquivo_path)
        
        # Valida extensão
        extensoes_validas = ['.csv', '.xls', '.xlsx', '.zip', '.pdf']
        if extensao.lower() not in extensoes_validas:
            logging.warning(f"⚠️ Extensão '{extensao}' não é válida. Extensões aceitas: {extensoes_validas}")
            logging.info(f"� Tentando processar mesmo assim...")
        
        # Define nome no bucket
        nome_no_bucket = f"exportacao_{valor_select}{extensao}"
        caminho_completo_bucket = f"{BUCKET_PATH}{nome_no_bucket}"
        
        logging.info(f"☁️ Bucket destino: gs://{BUCKET_NAME}/{caminho_completo_bucket}")
        logging.info(f"📤 Enviando arquivo para o bucket...")
        
        # Upload usando a função do utils.py
        sucesso = upload_bytes_to_bucket(
            arquivo_bytes=arquivo_path,  # Passa o caminho do arquivo
            nome_no_bucket=caminho_completo_bucket,
            bucket_name=BUCKET_NAME
        )
        
        if sucesso:
            logging.info(f"✅ Arquivo enviado com sucesso para gs://{BUCKET_NAME}/{caminho_completo_bucket}")
            
            # Remove o arquivo da pasta RAM
            try:
                os.remove(arquivo_path)
                logging.info(f"🗑️ Arquivo removido da RAM: {arquivo_path}")
            except Exception as e:
                logging.warning(f"⚠️ Erro ao remover arquivo da RAM: {e}")
            
            return True
        else:
            logging.error(f"❌ Falha ao enviar arquivo para o bucket")
            return False
            
    except Exception as e:
        logging.error(f"❌ Erro ao processar arquivo baixado: {e}")
        return False

def process_select_values(select_object, select_values):
    """
    Processa cada valor do select, resolvendo o CAPTCHA e fazendo download dos arquivos
    """
    global driver, wait
    
    for value in select_values:
        try:
            logging.info(f"🎯 Processando valor: {value}")
            
            # Limpa pasta de download antes de processar
            logging.info(f"🧹 Limpando pasta de download: {PASTA_DOWNLOAD_RAM}")
            limpar_pasta_download(PASTA_DOWNLOAD_RAM)
            
            # Verifica se pasta está vazia após limpeza
            if os.path.exists(PASTA_DOWNLOAD_RAM):
                arquivos_restantes = os.listdir(PASTA_DOWNLOAD_RAM)
                if arquivos_restantes:
                    logging.warning(f"⚠️ Ainda há arquivos na pasta após limpeza: {arquivos_restantes}")
                else:
                    logging.info(f"✅ Pasta de download limpa com sucesso")
            
            # Verifica se o driver ainda está ativo
            if not driver_ativo():
                logging.info("🔄 Navegador fechou. Recriando...")
                recriar_navegador()
                select_element = wait.until(EC.presence_of_element_located((By.ID, SELECT_ID)))
                select_object = Select(select_element)
            
            # 1. Seleciona o valor no select
            select_object.select_by_value(value)
            logging.info(f"Valor '{value}' selecionado")
            time.sleep(1)
            
            # 2. Tentar resolver o CAPTCHA com retry
            max_retries = 100
            captcha_resolvido = False
            
            for tentativa in range(1, max_retries + 1):
                logging.info(f"Tentativa {tentativa}/{max_retries} de resolver CAPTCHA para '{value}'")
                
                if not driver_ativo():
                    logging.info("🔄 Driver não ativo. Recriando...")
                    recriar_navegador()
                    select_element = wait.until(EC.presence_of_element_located((By.ID, SELECT_ID)))
                    select_object = Select(select_element)
                    select_object.select_by_value(value)
                
                try:
                    # Aguarda o CAPTCHA estar completamente carregado
                    time.sleep(1)
                    
                    # 1. Resolve o CAPTCHA
                    captcha_text = resolver_captcha(wait)
                    
                    if not captcha_text:
                        logging.info("❌ Falha ao extrair texto do CAPTCHA. Tentando refresh...")
                        if not fazer_refresh_captcha():
                            break
                        continue
                    
                    # 2. Preenche o CAPTCHA
                    if not preencher_captcha(captcha_text, driver):
                        logging.info("❌ Falha ao preencher CAPTCHA. Tentando refresh...")
                        if not fazer_refresh_captcha():
                            break
                        continue
                    
                    # 3. Clica no botão de consultar
                    button_found = clicar_botao_consultar()
                    
                    if not button_found:
                        logging.info("❌ Botão de consultar não encontrado")
                        if not fazer_refresh_captcha():
                            break
                        continue
                    
                    # 4. Aguarda validação do CAPTCHA pelo servidor
                    logging.info("⏱️ Aguardando validação do CAPTCHA pelo servidor...")
                    time.sleep(3)
                    
                    # 5. Verifica se houve erro no CAPTCHA
                    if verificar_erro_captcha(driver):
                        logging.info("❌ CAPTCHA rejeitado pelo servidor. Tentando novamente...")
                        
                        # Fecha o alerta de erro
                        fechar_alerta_erro()
                        
                        # Faz refresh do CAPTCHA
                        if not fazer_refresh_captcha():
                            break
                        
                        # Re-seleciona o valor
                        if driver_ativo():
                            try:
                                select_element = wait.until(EC.presence_of_element_located((By.ID, SELECT_ID)))
                                select_object = Select(select_element)
                                select_object.select_by_value(value)
                            except Exception as e:
                                logging.error(f"Erro ao re-selecionar valor: {e}")
                                break
                        
                        continue
                    
                    else:
                        # 6. CAPTCHA aceito - download será iniciado automaticamente
                        logging.info(f"✅ CAPTCHA aceito para o valor '{value}'!")
                        captcha_resolvido = True
                        
                        # Log de debug para verificar configuração do Chrome
                        logging.info(f"🔍 Verificando configuração de downloads do Chrome...")
                        try:
                            download_dir = driver.execute_script("return navigator.webdriver")
                            logging.info(f"🤖 WebDriver detectado: {download_dir}")
                        except Exception as e:
                            logging.info(f"⚠️ Erro ao verificar WebDriver: {e}")
                        
                        # 7. Aguarda o download do arquivo
                        arquivo_baixado = aguardar_download_completo(value)
                        
                        if arquivo_baixado:
                            # 8. Processa o arquivo: upload para bucket e remove da RAM
                            sucesso_upload = processar_arquivo_baixado(arquivo_baixado, value)
                            
                            if sucesso_upload:
                                logging.info(f"✅ Processamento completo para o valor '{value}'!")
                            else:
                                logging.error(f"❌ Falha no upload para o valor '{value}'")
                        else:
                            logging.error(f"❌ Download não concluído para o valor '{value}'")
                        
                        # 9. Volta para a página inicial para próximo valor
                        logging.info("🔄 Voltando para página inicial...")
                        driver.get(BASE_URL)
                        time.sleep(2)
                        
                        # Recarrega select para próxima iteração
                        select_element = wait.until(EC.presence_of_element_located((By.ID, SELECT_ID)))
                        select_object = Select(select_element)
                        
                        break
                        
                except Exception as e:
                    logging.error(f"Erro na tentativa {tentativa}: {e}")
                    if not fazer_refresh_captcha():
                        break
                    continue
            
            if not captcha_resolvido:
                logging.error(f"❌ Falha ao resolver CAPTCHA para o valor '{value}' após {max_retries} tentativas.")
            
        except Exception as e:
            logging.error(f"❌ Erro geral ao processar o valor '{value}': {e}")

def clicar_botao_consultar():
    """Tenta clicar no botão de consultar usando diferentes métodos"""
    try:
        # Método 1: Por ID (usando o ID correto fornecido)
        button_ids = [CONSULT_BUTTON_ID, "P25_CONSULT", "P25_CONSULTAR", "B25_CONSULT"]
        for button_id in button_ids:
            try:
                export_button = driver.find_element(By.ID, button_id)
                if export_button.is_displayed() and export_button.is_enabled():
                    logging.info(f"✅ Botão encontrado com ID: {button_id}")
                    driver.execute_script("arguments[0].click();", export_button)
                    return True
            except Exception:
                continue
        
        # Método 2: Por texto
        try:
            export_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Consultar') or contains(text(), 'CONSULTAR') or contains(@value, 'Consultar')]")
            logging.info("✅ Botão encontrado por texto")
            driver.execute_script("arguments[0].click();", export_button)
            return True
        except Exception:
            pass
        
        # Método 3: Por tipo de input
        try:
            export_button = driver.find_element(By.XPATH, "//input[@type='submit' or @type='button'][contains(@value, 'Consultar') or contains(@id, 'CONSULT')]")
            logging.info("✅ Botão encontrado por input type")
            driver.execute_script("arguments[0].click();", export_button)
            return True
        except Exception:
            pass
        
        logging.info("❌ Nenhum botão de consultar encontrado")
        return False
        
    except Exception as e:
        logging.error(f"Erro ao clicar no botão: {e}")
        return False

def recriar_navegador():
    """Recria o navegador se ele fechou inesperadamente"""
    global driver, wait
    try:
        logging.info("🔄 Recriando navegador...")
        
        # Fecha o driver atual se ainda existir
        try:
            driver.quit()
        except:
            pass
        
        # Recria o driver com configurações de download
        chrome_options = configurar_downloads_chrome(PASTA_DOWNLOAD_RAM)
        service = ChromeService(executable_path=ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        wait = WebDriverWait(driver, 20)
        
        # Navega para a página
        driver.get(BASE_URL)
        time.sleep(5)
        
        logging.info("✅ Navegador recriado com sucesso")
        
    except Exception as e:
        logging.error(f"Erro ao recriar navegador: {e}")
        raise

def fazer_refresh_captcha():
    """Função auxiliar para fazer refresh do CAPTCHA de forma segura"""
    try:
        if not driver_ativo():
            return False
            
        refresh_button = driver.find_element(By.ID, "spn_captchaanp_refresh_anp_p25_captcha")
        refresh_button.click()
        time.sleep(2)
        return True
    except Exception as e:
        try:
            if driver_ativo():
                driver.refresh()
                time.sleep(3)
                return True
        except Exception as e2:
            logging.error(f"Erro ao fazer refresh da página: {e2}")
        return False

def driver_ativo():
    """Verifica se o driver ainda está ativo"""
    try:
        driver.current_url
        return True
    except Exception:
        return False

def fechar_alerta_erro():
    """Função auxiliar para fechar alertas de erro"""
    try:
        close_button = driver.find_element(By.CSS_SELECTOR, ".t-Icon.icon-close")
        close_button.click()
        logging.info("Alerta de erro fechado")
        time.sleep(2)
    except Exception as e:
        logging.info(f"Erro ao fechar alerta: {e}")
        try:
            close_button = driver.find_element(By.CSS_SELECTOR, "button[title='Close']")
            close_button.click()
            time.sleep(2)
        except:
            pass

if __name__ == "__main__":
    try:
        # Navegar para a página
        driver.get(BASE_URL)
        logging.info("Página carregada com sucesso!")
        
        # Aguardar o select estar disponível
        select_element = wait.until(EC.presence_of_element_located((By.ID, SELECT_ID)))
        select_object = Select(select_element)
        
        # Obter todos os valores disponíveis no select
        select_values = [option.get_attribute("value") for option in select_object.options if option.get_attribute("value") != '']
        logging.info(f"Valores encontrados no select: {select_values}")
        
        # Processar cada valor do select
        process_select_values(select_object, select_values)
        
        logging.info("=== Processo finalizado com sucesso! ===")
        logging.info(f"📁 Arquivos enviados para o bucket: {BUCKET_PATH}")
        
    except Exception as e:
        logging.error(f"Erro durante a execução: {e}")
        logging.error("=== Processo finalizado com erro! ===")
    
    finally:
        # Limpa pasta de download
        limpar_pasta_download(PASTA_DOWNLOAD_RAM)
        
        try:
            driver.quit()
            logging.info("🚪 Navegador fechado.")
        except:
            pass

