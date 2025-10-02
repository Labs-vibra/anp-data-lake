import io
import logging
import time
from PIL import Image
import pytesseract
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

def resolver_captcha(window_wait):
    """
    Resolve o CAPTCHA da página ANP extraindo as 5 imagens e usando OCR.
    """
    try:
        logging.info("Aguardando o CAPTCHA carregar...")
        
        # Aguarda o div do CAPTCHA
        captcha_div = window_wait.until(EC.presence_of_element_located((By.ID, "anp_p25_captcha")))
        
        # Pequena pausa para garantir carregamento das imagens
        time.sleep(1)
        
        captcha_images = captcha_div.find_elements(By.TAG_NAME, "img")

        if len(captcha_images) != 5:
            logging.info(f"Erro: Esperado 5 imagens do CAPTCHA, encontrado {len(captcha_images)}")
            return None

        logging.info("Extraindo e processando as 5 imagens do CAPTCHA...")
        captcha_text = ""
        
        for i, img_element in enumerate(captcha_images):
            try:
                # Captura screenshot da imagem
                img_screenshot = img_element.screenshot_as_png
                image = Image.open(io.BytesIO(img_screenshot))
                
                # Converte para grayscale
                image = image.convert('L')
                
                custom_config = r'--oem 3 --psm 8 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                char = pytesseract.image_to_string(image, config=custom_config).strip().upper()

                if char and len(char) > 0:
                    # Pega apenas o primeiro caractere reconhecido
                    captcha_text += char[0]
                else:
                    logging.info(f"Erro: Nenhum caractere reconhecido na imagem {i+1}")
                    return None

            except Exception as e:
                logging.info(f"Erro ao processar imagem {i+1}: {e}")
                return None

        if len(captcha_text) == 5:
            logging.info(f"✅ CAPTCHA extraído: {captcha_text}")
            return captcha_text
        else:
            logging.info(f"Erro: CAPTCHA incompleto - apenas {len(captcha_text)} caracteres reconhecidos")
            return None

    except Exception as e:
        logging.info(f"Erro ao resolver CAPTCHA: {e}")
        return None


def preencher_captcha(captcha_text, driver):
    """Preenche o campo do CAPTCHA com o texto resolvido"""
    try:
        captcha_input = driver.find_element(By.ID, "P25_CAPTCHA")
        captcha_input.clear()
        captcha_input.send_keys(captcha_text)
        logging.info(f"Campo CAPTCHA preenchido com: '{captcha_text}'")
        return True
    except Exception as e:
        logging.info(f"Erro ao preencher campo CAPTCHA: {e}")
        return False


def verificar_erro_captcha(driver):
    """Verifica se apareceu a mensagem de erro do CAPTCHA"""
    try:
        # Aguarda um pouco para a div de erro aparecer
        time.sleep(2)
        
        error_div = driver.find_element(By.ID, "t_Alert_Notification")
        if error_div.is_displayed():
            error_text = error_div.text
            logging.info(f"Mensagem de erro encontrada: {error_text}")
            
            # Verifica se é erro relacionado ao CAPTCHA
            if ("O campo imagem está inválido" in error_text or 
                "Ocorreu 1 erro" in error_text or
                "captcha" in error_text.lower() or
                "imagem" in error_text.lower()):
                logging.info("❌ CAPTCHA foi rejeitado pelo servidor!")
                return True
        return False
    except Exception as e:
        # Se não encontrou a div de erro, assume que não há erro
        logging.debug(f"Nenhum erro de CAPTCHA detectado: {e}")
        return False


def fechar_alerta_erro(driver):
    """Fecha o alerta de erro se estiver visível"""
    try:
        close_button = driver.find_element(By.CSS_SELECTOR, ".t-Icon.icon-close")
        if close_button.is_displayed():
            close_button.click()
            logging.info("✅ Alerta de erro fechado")
            time.sleep(1)
            return True
    except Exception as e:
        logging.info(f"Erro ao fechar alerta: {e}")
        return False


def refresh_captcha(driver):
    """Faz refresh do CAPTCHA"""
    try:
        refresh_button = driver.find_element(By.ID, "spn_captchaanp_refresh_anp_p25_captcha")
        refresh_button.click()
        logging.info("🔄 CAPTCHA refreshed")
        time.sleep(3)
        return True
    except Exception as e:
        logging.info(f"Erro ao fazer refresh do CAPTCHA: {e}")
        return False
