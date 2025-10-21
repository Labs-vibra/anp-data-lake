import io
import logging
import time
from PIL import Image, ImageEnhance, ImageFilter
import pytesseract
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import numpy as np

def preprocessar_imagem(image):
    """
    Aplica pré-processamento na imagem para melhorar a precisão do OCR.
    """
    try:
        # Converte para grayscale
        image = image.convert('L')
        
        # Aumenta o contraste
        enhancer = ImageEnhance.Contrast(image)
        image = enhancer.enhance(2.0)
        
        # Aumenta a nitidez
        enhancer = ImageEnhance.Sharpness(image)
        image = enhancer.enhance(2.0)
        
        # Aplica threshold (binarização) para melhorar contraste
        # Converte para array numpy
        img_array = np.array(image)
        
        # Aplica threshold adaptativo
        threshold = 128
        img_array = np.where(img_array > threshold, 255, 0).astype(np.uint8)
        
        # Converte de volta para Image
        image = Image.fromarray(img_array)
        
        # Redimensiona a imagem para melhorar OCR (aumenta tamanho)
        width, height = image.size
        image = image.resize((width * 3, height * 3), Image.LANCZOS)
        
        return image
    except Exception as e:
        logging.warning(f"Erro no pré-processamento: {e}. Usando imagem original.")
        return image

def resolver_captcha(window_wait):
    """
    Resolve o CAPTCHA da página ANP extraindo as 5 imagens e usando OCR.
    Versão otimizada com pré-processamento de imagem.
    """
    try:
        logging.info("Aguardando o CAPTCHA carregar...")
        
        # Aguarda o div do CAPTCHA
        captcha_div = window_wait.until(EC.presence_of_element_located((By.ID, "anp_p25_captcha")))
        
        # Pequena pausa para garantir carregamento das imagens
        time.sleep(1.5)
        
        captcha_images = captcha_div.find_elements(By.TAG_NAME, "img")

        if len(captcha_images) != 5:
            logging.info(f"❌ Erro: Esperado 5 imagens do CAPTCHA, encontrado {len(captcha_images)}")
            return None

        logging.info("🔍 Extraindo e processando as 5 imagens do CAPTCHA...")
        captcha_text = ""
        
        for i, img_element in enumerate(captcha_images):
            try:
                # Captura screenshot da imagem
                img_screenshot = img_element.screenshot_as_png
                image = Image.open(io.BytesIO(img_screenshot))
                
                # Aplica pré-processamento otimizado
                image = preprocessar_imagem(image)
                
                # Configuração otimizada do Tesseract
                # --oem 3: Usar LSTM + modo legado (melhor precisão)
                # --psm 10: Tratar imagem como um único caractere
                # --psm 8: Alternativa - trata como uma única palavra
                custom_config = r'--oem 3 --psm 10 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                
                char = pytesseract.image_to_string(image, config=custom_config).strip().upper()
                
                # Remove espaços e caracteres especiais
                char = ''.join(c for c in char if c.isalnum())

                if char and len(char) > 0:
                    # Pega apenas o primeiro caractere reconhecido
                    captcha_text += char[0]
                    logging.info(f"  📝 Imagem {i+1}/5: '{char[0]}'")
                else:
                    logging.info(f"❌ Erro: Nenhum caractere reconhecido na imagem {i+1}")
                    
                    # Tenta com PSM alternativo
                    custom_config_alt = r'--oem 3 --psm 8 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                    char = pytesseract.image_to_string(image, config=custom_config_alt).strip().upper()
                    char = ''.join(c for c in char if c.isalnum())
                    
                    if char and len(char) > 0:
                        captcha_text += char[0]
                        logging.info(f"  📝 Imagem {i+1}/5: '{char[0]}' (modo alternativo)")
                    else:
                        logging.info(f"❌ Falha definitiva na imagem {i+1}")
                        return None

            except Exception as e:
                logging.info(f"❌ Erro ao processar imagem {i+1}: {e}")
                return None

        if len(captcha_text) == 5:
            logging.info(f"✅ CAPTCHA extraído com sucesso: '{captcha_text}'")
            return captcha_text
        else:
            logging.info(f"❌ Erro: CAPTCHA incompleto - apenas {len(captcha_text)} caracteres reconhecidos")
            return None

    except Exception as e:
        logging.error(f"❌ Erro ao resolver CAPTCHA: {e}")
        return None


def preencher_captcha(captcha_text, driver):
    """Preenche o campo do CAPTCHA com o texto resolvido"""
    try:
        captcha_input = driver.find_element(By.ID, "P25_CAPTCHA")
        captcha_input.clear()
        time.sleep(0.5)
        captcha_input.send_keys(captcha_text)
        logging.info(f"✅ Campo CAPTCHA preenchido com: '{captcha_text}'")
        return True
    except Exception as e:
        logging.error(f"❌ Erro ao preencher campo CAPTCHA: {e}")
        return False


def verificar_erro_captcha(driver):
    """Verifica se apareceu a mensagem de erro do CAPTCHA"""
    try:
        # Aguarda um pouco para a div de erro aparecer
        time.sleep(2.5)
        
        # Tenta encontrar a div de erro
        error_div = driver.find_element(By.ID, "t_Alert_Notification")
        if error_div.is_displayed():
            error_text = error_div.text
            logging.info(f"⚠️ Mensagem encontrada: {error_text[:100]}...")
            
            # Verifica se é erro relacionado ao CAPTCHA
            erro_captcha_keywords = [
                "O campo imagem está inválido",
                "campo imagem",
                "Ocorreu 1 erro",
                "captcha",
                "imagem",
                "inválido"
            ]
            
            error_text_lower = error_text.lower()
            for keyword in erro_captcha_keywords:
                if keyword.lower() in error_text_lower:
                    logging.info(f"❌ CAPTCHA foi rejeitado pelo servidor! (keyword: '{keyword}')")
                    return True
            
            logging.info(f"ℹ️ Mensagem de erro não relacionada ao CAPTCHA")
        return False
        
    except Exception as e:
        # Se não encontrou a div de erro, assume que não há erro
        logging.debug(f"✅ Nenhum erro de CAPTCHA detectado (exception: {e})")
        return False


def fechar_alerta_erro(driver):
    """Fecha o alerta de erro se estiver visível"""
    try:
        # Tenta vários seletores diferentes
        seletores = [
            ".t-Icon.icon-close",
            "button.t-Button.t-Button--icon.t-Button--hot",
            "button[title='Close']",
            ".t-Alert-wrap button",
            "#t_Alert_Notification button"
        ]
        
        for seletor in seletores:
            try:
                close_button = driver.find_element(By.CSS_SELECTOR, seletor)
                if close_button.is_displayed():
                    close_button.click()
                    logging.info(f"✅ Alerta de erro fechado (seletor: {seletor})")
                    time.sleep(1)
                    return True
            except:
                continue
        
        logging.warning("⚠️ Não foi possível fechar o alerta de erro")
        return False
        
    except Exception as e:
        logging.warning(f"⚠️ Erro ao fechar alerta: {e}")
        return False


def refresh_captcha(driver):
    """Faz refresh do CAPTCHA"""
    try:
        # Tenta encontrar o botão de refresh do CAPTCHA
        refresh_button_ids = [
            "spn_captchaanp_refresh_anp_p25_captcha",
            "anp_p25_captcha_refresh",
            "captcha_refresh"
        ]
        
        for button_id in refresh_button_ids:
            try:
                refresh_button = driver.find_element(By.ID, button_id)
                refresh_button.click()
                logging.info(f"🔄 CAPTCHA refreshed (ID: {button_id})")
                time.sleep(2.5)
                return True
            except:
                continue
        
        # Se não encontrou o botão, tenta por CSS
        try:
            refresh_button = driver.find_element(By.CSS_SELECTOR, "[id*='captcha'][id*='refresh']")
            refresh_button.click()
            logging.info("🔄 CAPTCHA refreshed (CSS selector)")
            time.sleep(2.5)
            return True
        except:
            pass
        
        logging.warning("⚠️ Botão de refresh do CAPTCHA não encontrado")
        return False
        
    except Exception as e:
        logging.error(f"❌ Erro ao fazer refresh do CAPTCHA: {e}")
        return False
