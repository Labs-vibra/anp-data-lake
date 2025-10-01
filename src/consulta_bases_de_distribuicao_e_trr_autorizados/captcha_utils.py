import io
import logging
from os import wait
from time import sleep
from PIL import Image
import pytesseract
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from constants import BASE_URL, SELECT_ID
from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.support.ui import WebDriverWait, Select

def resolver_captcha(window_wait):
    """Resolve o CAPTCHA da página ANP extraindo as 5 imagens e usando OCR"""
    try:
        logging.info("Aguardando o CAPTCHA carregar...")
        captcha_div = window_wait.until(EC.presence_of_element_located((By.ID, "anp_p25_captcha")))
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
        error_div = driver.find_element(By.ID, "t_Alert_Notification")
        if error_div.is_displayed():
            error_text = error_div.text
            if "O campo imagem está inválido" in error_text or "Ocorreu 1 erro" in error_text:
                logging.info("CAPTCHA foi rejeitado pelo servidor!")
                return True
        return False
    except:
        return False
