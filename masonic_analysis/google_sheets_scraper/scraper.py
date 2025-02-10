import csv
import json
import os
import gspread
import requests
from geojson import FeatureCollection, Feature, Point
from bs4 import BeautifulSoup
from langdetect import detect, LangDetectException
from concurrent.futures import ThreadPoolExecutor
from google.oauth2.service_account import Credentials
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Función para crear sesión HTTP con reintentos
def get_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def read_google_sheets():
    """Lee y devuelve los datos del Google Sheet."""
    creds = Credentials.from_service_account_info(json.loads(os.environ["GOOGLE_CREDENTIALS"]))
    gc = gspread.authorize(creds)
    sheet = gc.open_by_url("https://docs.google.com/spreadsheets/d/1dBQlNHrD6Ww69CcW9x47VIMd4FAay_7-paIlYEy0H8U/edit#gid=0")
    worksheet = sheet.sheet1
    data = worksheet.get_all_records()
    return data

def scrape_alert(url):
    """Procesa una URL y extrae datos usando BeautifulSoup."""
    session = get_session()
    try:
        response = session.get(url.strip(), timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')

        title = soup.title.get_text(strip=True) if soup.title else "Sin título"
        summary = soup.find('p').get_text(strip=True) if soup.find('p') else ""
        language = detect(summary) if summary else "Desconocido"
        parsed_url = urlparse(url)
        publisher = parsed_url.netloc if parsed_url.netloc else "Desconocido"
        category = "Artículo" if "article" in parsed_url.path.lower() else "Noticia"
        coords = (-99.1332, 19.4326)  # Coordenadas de ejemplo; puedes usar tu sistema de geocodificación.

        return {
            "title": title, "summary": summary, "link": url, "publisher": publisher,
            "category": category, "language": language, "coords": coords
        }
    except Exception as e:
        print(f"Error procesando {url}: {str(e)}")
        return None

def read_alerts_geojson():
    """Lee las alertas de Google desde el archivo GeoJSON existente."""
    alerts_file = 'masonic_alerts.geojson'
    if os.path.exists(alerts_file):
        with open(alerts_file, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        return [feature["properties"] for feature in geojson_data.get("features", [])]
    return []

def unify_data(google_sheets_data, alerts_data, wikipedia_data):
    """Unifica las tres capas de datos y elimina duplicados."""
    combined_data = google_sheets_data + alerts_data + wikipedia_data

    # Eliminar duplicados basados en el campo `link`
    seen_links = set()
    unique_data = []
    for item in combined_data:
        link = item.get("link")
        if link and link not in seen_links:
            seen_links.add(link)
            unique_data.append(item)
    return unique_data

def write_csv(filename, data):
    """Escribe los datos unificados en un archivo CSV."""
    headers = ["title", "summary", "link", "publisher", "category", "language", "coords"]
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

def write_geojson(filename, data):
    """Escribe los datos en un archivo GeoJSON."""
    features = [Feature(geometry=Point(item["coords"]), properties=item) for item in data if item.get("coords")]
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(FeatureCollection(features), f, ensure_ascii=False, indent=2)

def main():
    # Leer los datos de Google Sheets
    google_sheets_data = read_google_sheets()

    # Leer las alertas de Google desde GeoJSON existente
    alerts_data = read_alerts_geojson()

    # Simulamos el scraping de datos de Wikipedia
    wikipedia_data = [
        {"title": "Historia de la Masonería", "summary": "Artículo sobre la historia...", "link": "https://wikipedia.org/masonería",
         "publisher": "Wikipedia", "category": "Artículo", "language": "es", "coords": (-0.1278, 51.5074)}
    ]

    # Unificación y eliminación de duplicados
    unified_data = unify_data(google_sheets_data, alerts_data, wikipedia_data)

    # Escribir los archivos finales
    write_csv('output_data.csv', unified_data)
    write_geojson('output_data.geojson', unified_data)

    print("Archivos CSV y GeoJSON generados correctamente.")

if __name__ == "__main__":
    main()
