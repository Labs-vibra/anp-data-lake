from io import BytesIO
import requests
from bs4 import BeautifulSoup
from constants import BUCKET_NAME, FILES_BUCKET_DESTINATION, FILES_TO_EXTRACT_URL, SCRAPPING_HTML_XPATH
from utils import process_file_name, upload_bytes_to_bucket

def extrair_links():
    response = requests.get(FILES_TO_EXTRACT_URL)
    soup = BeautifulSoup(response.content, "html.parser")
    links = []
    for entry in soup.select(SCRAPPING_HTML_XPATH):
        href = entry.get("href")[:-5]
        file_extension = href.split('.')[-1]
        if href not in links and file_extension == "csv":
            links.append(href)
    return links

def download_file_bytes(url):
    response = requests.get(url)
    response.raise_for_status()
    return BytesIO(response.content)

def process_pipeline():
    print("Starting the extraction and upload process...")
    todos_links = extrair_links()
    for link in todos_links:
        file_content = download_file_bytes(link)
        print(f"Downloaded {file_content.getbuffer().nbytes} bytes from {link}")
        file_name = link.split("/")[-1]
        destination_path = f"{FILES_BUCKET_DESTINATION}/{process_file_name(file_name)}"
        print(f"Uploading to {destination_path} in bucket {BUCKET_NAME}")
        upload_bytes_to_bucket(file_content, destination_path)
        print(f"Uploaded to {destination_path} in bucket {BUCKET_NAME}")
    print("Process completed.")

if __name__ == "__main__":
    process_pipeline()