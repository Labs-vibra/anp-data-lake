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
from constants import BASE_URL, SELECT_ID, CONSULT_BUTTON_ID

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Configuração do Chrome
chrome_options = webdriver.ChromeOptions()

# Comentar o --headless para ver o processo acontecer
# chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-plugins")

# Inicializar o driver
service = ChromeService(executable_path=ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)
wait = WebDriverWait(driver, 20)

def process_select_values(select_object, select_values):
    """
    Processa cada valor do select, resolvendo o CAPTCHA para cada um
    """
    global driver, wait  # Para poder recriar se necessário
    
    for value in select_values:
        try:
            logging.info(f"🎯 Processando valor: {value}")
            
            # Verifica se o driver ainda está ativo
            if not driver_ativo():
                logging.info("🔄 Navegador fechou. Recriando...")
                recriar_navegador()
                select_element = wait.until(EC.presence_of_element_located((By.ID, SELECT_ID)))
                select_object = Select(select_element)
            
            # 1. Seleciona o valor no select
            select_object.select_by_value(value)
            logging.info(f"Valor '{value}' selecionado")
            time.sleep(1)  # Tempo reduzido
            
            # 2. Tentar resolver o CAPTCHA com retry
            max_retries = 100  # Aumentando para 100 tentativas conforme solicitado
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
                    # Aguarda o CAPTCHA estar completamente carregado (reduzido)
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
                    
                    # 3. Clica no botão de consultar (usando JavaScript para ser mais confiável)
                    button_found = clicar_botao_consultar()
                    
                    if not button_found:
                        logging.info("❌ Botão de consultar não encontrado")
                        if not fazer_refresh_captcha():
                            break
                        continue
                    
                    # 4. Aguarda um tempo para o servidor processar (reduzido)
                    logging.info("⏱️ Aguardando validação do CAPTCHA pelo servidor...")
                    time.sleep(2)
                    
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
                        # 6. CAPTCHA aceito - sucesso!
                        logging.info(f"✅ CAPTCHA aceito para o valor '{value}'!")
                        captcha_resolvido = True
                        
                        # Aqui você pode adicionar o código para extrair os dados após o CAPTCHA ser aceito
                        logging.info(f"📊 Processando dados para o valor '{value}'...")
                        
                        # Aguardar dados carregarem (tempo reduzido)
                        time.sleep(2)
                        
                        # Adicione aqui sua lógica de download/extração de dados
                        # Por exemplo:
                        # dados = extrair_dados()
                        # salvar_dados(dados, value)
                        
                        # Voltar para a página inicial para próximo valor
                        logging.info("🔄 Voltando para página inicial...")
                        driver.get(BASE_URL)
                        time.sleep(2)  # Tempo reduzido
                        
                        # Recarregar select para próxima iteração
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
                    # Usa JavaScript para clicar (mais confiável)
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
        
        # Recria o driver
        from webdriver_manager.chrome import ChromeDriverManager
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        # chrome_options.add_argument("--headless")  # Removido para debug
        
        service = Service(ChromeDriverManager().install())
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
        # Verifica se o navegador ainda está ativo
        if not driver_ativo():
            return False
            
        refresh_button = driver.find_element(By.ID, "spn_captchaanp_refresh_anp_p25_captcha")
        refresh_button.click()
        time.sleep(2)  # Tempo reduzido
        return True
    except Exception as e:
        try:
            if driver_ativo():
                driver.refresh()
                time.sleep(3)  # Tempo reduzido
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
        # Tenta outras formas de fechar
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
        
    except Exception as e:
        logging.error(f"Erro durante a execução: {e}")
        logging.error("=== Processo finalizado com erro! ===")
    
    finally:
        try:
            driver.quit()
            logging.info("🚪 Navegador fechado.")
        except:
            pass

