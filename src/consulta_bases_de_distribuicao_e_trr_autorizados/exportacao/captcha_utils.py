import io
import logging
import time
from PIL import Image, ImageEnhance, ImageFilter
import pytesseract
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

def resolver_captcha(window_wait):
    """Resolve o CAPTCHA da página ANP extraindo as 5 imagens e usando OCR melhorado"""
    try:
        # Aguarda o CAPTCHA carregar (tempo reduzido)
        time.sleep(1)
        
        # Verifica se o div do CAPTCHA existe e está visível
        captcha_div = window_wait.until(EC.visibility_of_element_located((By.ID, "anp_p25_captcha")))
        
        # Aguarda um pouco para garantir que as imagens estão carregadas (reduzido)
        time.sleep(1)
        
        captcha_images = captcha_div.find_elements(By.TAG_NAME, "img")

        if len(captcha_images) != 5:
            return None

        captcha_text = ""
        
        for i, img_element in enumerate(captcha_images):
            try:
                # Verifica se a imagem está visível
                if not img_element.is_displayed():
                    return None
                
                img_screenshot = img_element.screenshot_as_png
                image = Image.open(io.BytesIO(img_screenshot))
                
                # Melhorar a imagem para OCR mais preciso
                image = melhorar_imagem_para_ocr(image)

                # Múltiplas tentativas de OCR com diferentes configurações
                char = extrair_caractere_com_multiplas_tentativas(image)

                if char:
                    captcha_text += char
                else:
                    return None

            except Exception as e:
                return None

        if len(captcha_text) == 5:
            logging.info(f"✅ CAPTCHA: {captcha_text}")
            return captcha_text
        else:
            return None

    except Exception as e:
        return None


def melhorar_imagem_para_ocr(image):
    """Melhora a qualidade da imagem para OCR mais preciso"""
    from PIL import ImageEnhance, ImageFilter
    
    # Converte para grayscale
    image = image.convert('L')
    
    # Redimensiona a imagem (aumenta o tamanho para melhor OCR)
    width, height = image.size
    image = image.resize((width * 3, height * 3), Image.LANCZOS)
    
    # Aumenta contraste
    enhancer = ImageEnhance.Contrast(image)
    image = enhancer.enhance(2.0)
    
    # Aumenta brilho
    enhancer = ImageEnhance.Brightness(image)
    image = enhancer.enhance(1.2)
    
    # Aplica filtro para nitidez
    image = image.filter(ImageFilter.SHARPEN)
    
    # Binarização - converte para preto e branco puro
    threshold = 128
    image = image.point(lambda p: p > threshold and 255)
    
    return image


def extrair_caractere_com_multiplas_tentativas(image):
    """Tenta extrair caractere usando múltiplas configurações de OCR"""
    
    # Configurações diferentes para tentar
    configs = [
        r'--oem 3 --psm 8 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ',
        r'--oem 3 --psm 7 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ',
        r'--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ',
        r'--oem 1 --psm 8 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ',
    ]
    
    for config in configs:
        try:
            char = pytesseract.image_to_string(image, config=config).strip().upper()
            if char and len(char) > 0:
                # Aplica correções de caracteres comuns
                char = corrigir_caracteres_confusos(char[0])
                return char
        except Exception:
            continue
    
    return None


def corrigir_caracteres_confusos(char):
    """Corrige caracteres que são frequentemente confundidos pelo OCR"""
    
    # Mapeamento de correções comuns
    correções = {
        'O': '0',  # O maiúsculo -> zero
        'I': '1',  # I maiúsculo -> um  
        'l': '1',  # l minúsculo -> um
        'S': '5',  # S às vezes confundido com 5
        'G': '6',  # G às vezes confundido com 6
        'B': '8',  # B às vezes confundido com 8
    }
    
    # Aplica correção se necessário
    return correções.get(char, char)

def preencher_captcha(captcha_text, driver):
    """Preenche o campo do CAPTCHA com o texto resolvido"""
    try:
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        
        wait = WebDriverWait(driver, 5)  # Tempo reduzido
        captcha_input = wait.until(EC.element_to_be_clickable((By.ID, "P25_CAPTCHA")))
        
        # Limpa e preenche rapidamente
        captcha_input.click()
        captcha_input.clear()
        captcha_input.send_keys(captcha_text)
        
        # Verifica se foi preenchido corretamente
        valor_atual = captcha_input.get_attribute("value")
        if valor_atual == captcha_text:
            return True
        else:
            return False
            
    except Exception as e:
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
        # Ajuste o ID do botão de refresh conforme necessário
        refresh_button = driver.find_element(By.ID, "spn_captchaanp_refresh_anp_p25_captcha")
        refresh_button.click()
        logging.info("🔄 CAPTCHA refreshed")
        time.sleep(3)
        return True
    except Exception as e:
        logging.info(f"Erro ao fazer refresh do CAPTCHA: {e}")
        return False
